# Elastic-Agent — 项目指南

> **重要：Claude 必须自主维护本文件。** 架构或约定变化时更新，保持简洁。

## 架构要点

- **任务执行两条路径**（worker/runtime.py `_handle_execute`）：
  - subprocess（默认）：Manager 经 AgentType 构造 `claude -p ... --output-format stream-json` 命令行，worker spawn 后逐行转发 stdout
  - **PTY 模式**（可选）：`ExecuteMessage.agent_params` 非空且 worker 装了 [claude-pty](https://github.com/zjw49246/Claude-Code-PTY) 时，worker 用 `ElasticPTYBackend`（worker/pty_backend.py，继承 claude_pty 的 BasePTYBackend）把 Claude Code 宿主在持久 PTY 会话里；`command` 始终随消息下发作为 fallback
- **PTY 事件回传**：带 raw_json 的事件原行透传为 stdout NDJSON（Manager 解析链不变）；交互模式 JSONL 无 result 行，worker 在 turn 结束合成一条（带 session_id，无 cost_usd）；限流/错误 turn → 非零 exit code → 既有凭证轮换照常触发
- **orphan / autonomous 守卫**（pty_backend.py `on_event`）：claude-pty 的 PTYEvent 带 `orphan`（冷恢复时重放的上一 turn JSONL）/ `autonomous`（后台子 agent turn）标记。二者都**不参与前台 turn 的记账**——不设 session_id、不标 turn-fatal 错误、不计入 `_saw_result`/`_saw_claude_output`（`turn_scoped` 门）；orphan 事件还直接从转发流丢弃（避免 Manager 重复落盘 / 把旧 result 当本 turn 结果）。否则 resume 会把旧 api_error 重新标致命 → 刚成功的 turn 被误报 failed（recover-then-failed，借鉴 CCM task #729/#87）
- **瞬时过载同号重试**（P2，`worker/pty_backend.py`）：Anthropic 基础设施侧的临时 429/过载（`overloaded_error` / "temporarily limiting requests (not your usage limit)"）**换号无用**。`core/rate_limit.py` 的 `is_transient_overload`（与 `is_rate_limited`/`is_auth_failure` 互斥、后者优先）识别后，`classify_turn_error` 归为 `transient_overload`；on_exit 不报失败，而是退避（指数+jitter，`transient_retry_delay`）后**同 session `--resume` 重试**（`_schedule_transient_retry`/`_run_transient_retry`，最多 `transient_retry_max=5` 次），耗尽才真失败。额度用尽/认证失败仍走既有 QuotaMonitor 轮换。`rate_limit_event_is_actionable` 备用（避免被 `status=allowed` 例行 ping 误触发轮换）
- **开关**：Manager 侧 `TaskRouter(use_pty=True)` + bootstrap `include_pty=True`；worker 侧无需配置
- **凭证轮换**：原地换凭证（同 config_dir 写新 token）；`CREDENTIAL_LOGIN` 后 worker 调 `recycle_config_dir` 回收该 config_dir 的所有 PTY 会话（温热会话仍持旧账号，不可热复用），下次 EXECUTE 冷恢复读新凭证
- **OAuth token 刷新无 aiohttp 依赖**（`core/claude_oauth.py` `refresh_access_token`）：未注入 `http_client` 时走 **stdlib urllib**（`_post_form_urllib` + `asyncio.to_thread`），不再 `import aiohttp`——worker 框架环境未声明 aiohttp，旧 fallback 会 `ModuleNotFoundError` 致 QuotaChecker 每次刷 token 崩、额度检测/换号失灵
- **账号自动登录**（`worker/login/`，vendored 自 CCM `auto_login.py`+`cdp_login.py`）：登录逻辑=CCM 版，纯 Chrome CDP 直调 OAuth authorize（不用 Playwright/mitmproxy），多后端接码（多数域走 171mail API，mail.com 家族走 mail relay / mail.com Web，按邮箱域名自动选，`resolve_provider`）。**worker 本地跑**：Manager 只持 email+接码token（`AccountDefinition`）并下发；`ClaudeOAuthProvider.login()` 是薄壳，`worker_host` 有值时经 SSH 在 worker 上跑 `python -m elastic_agent.worker.login.auto_login`（起 Xvfb:99 + Chrome），无值时进程内直跑——凭证只落 worker、绝不经 Manager。`OAuthConfig→LoginResult` 契约不变，`CredentialLoginService/Step` 编排层不动。CCM 特定硬编码已参数化（`CLAUDE_MAILCATCHER_URL`/`CLAUDE_171MAIL_URL`/`CLAUDE_SETTINGS_EXTRA_DIRS`）
- **worker 自治登录**（P3，协议 `ACCOUNT_LOGIN`/`ACCOUNT_LOGIN_RESULT`）：Manager 只下发账号身份（`account_id`/`email`/`email_token`/`config_dir`/`provider`），worker `_handle_account_login` 本地跑 `perform_login`（`worker_host=None`，进程内 Chrome/CDP）→ 成功则 `_warmup_config_dir`（`claude -p` 预热 GrowthBook+验证凭证）+ `recycle_config_dir` + 挂 QuotaChecker slot → 回 `ACCOUNT_LOGIN_RESULT`。区别于既有 `CREDENTIAL_LOGIN`（下发已登好的 token）。凭证只在 worker 生成、不回传。（既有 SSH 驱动登录路径仍保留）

## 两类任务 & 批量编排

- **两类任务模型**（关键区分）：
  - **Mode A — Elastic 托管 agent**（PTY 路径，上文）：任务=一个 prompt，Elastic 宿主 Claude、逐 turn 记账、逐 turn 换号。
  - **Mode B — 不透明长命令**：任务=一条任意 shell 命令（如 `uv run ai4sci-bench run …`），它自己开 sandbox、自己内部消费账号——Elastic **看不到**里面的 turn。此时 Elastic 只做：装环境（bootstrap）+ worker 本地登号 + 跑命令 + 盯输出 + 收结果 + 弹性缩扩。`ExecuteMessage.command` 本就是任意 argv（`create_subprocess_exec`），Mode B 天然支持。
- **声明式 JobSpec**（`core/job_spec.py`）：任务即数据。`setup`(repo+commands)/`run`(command+env+cwd，shell 模式包 `bash -lc`)/`account`(mode/per_worker/config_dir)/`rotation`/`fanout`(workers/shard_by/name_prefix/**instance_type/region/disk_gb/spot**)/`collect`/`completion`。模板 `{{shard_index}}`/`{{num_shards}}`/`{{hostname}}` 由 Manager 渲染；`$(hostname -s)`/`$VAR` 留给 worker shell。**per-job 机器覆盖**（`fanout.instance_type/region/disk_gb/spot`）经 orchestrator→`ManagerFleetDriver`→`manager.scale_out`→`InstanceConfig`（`disk_gb`=0→provider 默认，写 `root_disk_size_gb`；AMI/subnet/SG/key 仍取 Manager config）。
- **cwd 契约**（`JobSpec.resolved_cwd`）：repo clone 到 `setup.target_dir`（= repo 根），**setup 命令（`harness_code_step` 里 `cd target_dir`）和 run 命令都从这里跑**——用户照本地 `git clone && cd repo && <cmd>` 写即可。`run.cwd` 精化：`.`/空=repo 根，相对路径=其子目录，绝对路径原样用。
- **代码分发**（`setup.deliver`）：`worker_clone`（worker 自己 clone，公开 repo；私有需把 token 推到 worker）或 **`manager_rsync`（私有 repo 推荐）**——`core/code_sync.py` `ManagerCodeSync` 在 **Manager 本地** clone（token 只在此、`ensure_clone` 后 scrub 掉），再 `rsync --exclude .git` 到 worker（**token/`.git`/remote 都不上 worker**，防权限泛滥），最后 SSH 在 worker 上 `cd target_dir && <setup.commands>`。`compile_bootstrap_steps` 在 `manager_rsync` 下跳过 worker 的 harness-code 步；provision hook 负责 clone+rsync+setup。token 取 `ELASTIC_AGENT_GIT_TOKEN`（Manager 进程 env）。**setup 命令以 job 用户跑、不 sudo**（`SSHExecutor(..., use_sudo=False)`）——必须与 run 命令同用户/同 HOME，否则 `curl uv/install.sh|sh`+`uv sync` 装到 root 的 HOME、run 以 ssh_user 跑就 `uv: No such file`（见 PROGRESS 2026-07-16）。
- **两条接入路**统一成 `Harness`（`harness/generic.py` `resolve_harness`）：`harness_ref` 空→`GenericJobHarness(spec)`（声明式编译 bootstrap/凭证槽/execute）；有值→导入上传的 `Harness` 子类（`module:Class` 或 `/path.py:Class`）。
- **BatchOrchestrator**（`core/batch_orchestrator.py`）：`launch(spec)` = scale_out(N) → 每 worker 并发 provision→login→run_command；`on_worker_exhausted`/`on_worker_exit` 驱动换号与完成。经 `FleetDriver` Protocol 解耦（真实现 `core/manager_fleet_driver.py`）。
- **增量收集 & 结果落 S3**（`collect.interval_seconds`）：`>0` 时 `_dispatch` 起 per-worker 周期任务，运行中每隔 N 秒 `driver.collect`（rsync worker `collect.paths` → Manager `collected/<job_id>/`），配合 `S3ResultUploader.run_periodic`（每 `ELASTIC_AGENT_RESULTS_S3_INTERVAL` 秒镜像 `collected_root`→`s3://<bucket>/<prefix>/<job_id>/`）→ **边跑边把已完成任务推上 S3**；长跑/撞额度也有已完成部分。`on_worker_exit` 停周期任务 + **DONE/FAILED 都做一次 final collect**（失败/额度耗尽的部分结果也不丢）。`interval_seconds=0`=只在完成时收集（旧行为）。**run 退出判定**（`worker/runtime.py _wait_process_exit`）：轮询 `proc.returncode`（SIGCHLD 立即置位）而非 `proc.wait()`——后者要等 stdout/stderr EOF，`--sandbox os` 的 docker 容器会长期占管道 → 卡住 → 永不发 PROCESS_EXIT → Manager 卡 RUNNING、collect 不触发。
- **实盘装配**（`core/batch_hooks.py` `wire_batch`，`Manager.batch` 默认走它）：provision=等实例 running→`BootstrapHandler` SSH 跑 `compile_bootstrap_steps`→等 WS 连上；login=`AccountAllocator`（内存分配账号身份，换号时把旧号 retire 不再选；**account 在 job 终态即释放**——`_maybe_finish` 无条件 `release_worker` 回池，且 bring-up 全失败/rotation 耗尽 decline 两条路也补调 `_maybe_finish`，`release_worker` 幂等。修单例 orchestrator 下"只在 scale-in 释放"致单账号第二个 job 撞 `no available account` 的 starvation）+ `LoginCoordinator`（发 `ACCOUNT_LOGIN`、经 event_bus 等 `ACCOUNT_LOGIN_RESULT`）；并把 worker 的 `RUN_EXHAUSTED`/`PROCESS_EXIT` 经 event_bus 路由回 orchestrator（`handle_exhausted`/`handle_exit`）。manager_url 取 `ELASTIC_AGENT_MANAGER_URL` 或 `config.server`；ssh key 按 provider.type 取。`configure_batch(...)` 可覆盖 hooks。**换号竞态**：中断退出的旧 run 的 `PROCESS_EXIT` 带旧 task_id，`on_worker_exit` 用 task_id 匹配当前 run 丢弃陈旧退出（ROTATING 相位守卫挡不住重派已转 RUNNING 的竞态）。
- **Mode B 换号（策略 a）**：`ExecuteMessage.watch_exhaustion` 开启时 worker `_read_stream` 用 `core/rate_limit.py` 扫 stdout/stderr，撞限流即 `_signal_exhaustion`（发 `RunExhaustedMessage` + SIGINT）；Manager 侧 `on_worker_exhausted` 换号 + 用 `rotation.resume_args` `--resume` 重启，`max_rotations` 上限；打断退出（ROTATING）不算失败。
- **每机多号**（`account.per_worker>1`）：GenericJobHarness 按 per_worker 产出多个独立 config_dir 槽（`<base>-slot-N`）；`_bring_up` 逐槽 `driver.login`（`AccountAllocator` 每次给不同账号，已分配的号在本 job 内不再发出→耗尽号不会被重选）。换号优先切到**下一个预登录槽**（`active_slot++`，免登录、快），本地池耗尽才 `-rot-N` 现登新号。`WorkerRun.config_dirs/account_ids/account_emails` + `active_slot` 跟踪；`ctx.config_dir` 随活跃槽注入 `CLAUDE_CONFIG_DIR`。
- **Docker 沙箱支持**（`setup.needs_docker`）：run 命令用 Docker（如 ai4sci-bench `--sandbox os`）时置 true → `compile_bootstrap_steps` 在 runtime 部署**前**插 `docker_install_step`（装 docker.io + `usermod -aG docker <ssh_user>` + `enable --now docker`）。**必须在 runtime systemd 起来前**跑：systemd 起服务时才解析 User 的补充组，usermod 先跑 runtime(及其子 run 命令)才拿得到 docker socket。`run_as` 经 `compile_bootstrap_steps(..., run_as=ssh_user)` 从 `batch_hooks` 传入。
- **S3 数据集分发**（`setup.s3_datasets: [{uri,dest}]`）：**worker 直连 S3 拉**（不再经 Manager 中转）。前提 `fanout` provider 配了 `worker_instance_profile`（AWS：`AWSProviderConfig.worker_instance_profile`，`create_instance` 给 `run_instances` 加 `IamInstanceProfile`；Manager 角色需 `iam:PassRole`）→ worker EC2 挂 IAM 角色（`elastic-agent-worker`/S3 权限）。provision hook 在 run 前：装 awscli（缺才装）→ 逐个 `aws s3 sync <uri>/ <dest>`（尾 `/`=前缀递归）或 `aws s3 cp`（单对象）在 worker 上跑。**GitHub 代码分发不变**（仍 manager_rsync，token 只在 Manager）。旧的 `code_sync.py` `_download_s3`/`ManagerCodeSync.stage_s3`（Manager 下载→rsync）保留为工具函数但 provision 不再用。凭证仍不下发（worker 用实例角色，非静态 key）。
- **Worker 日志/管理 API**（`api/routes/nodes.py`）：`GET /api/nodes/{id}/logs?lines=&unit=ea-runtime`（Manager SSH 到 worker 跑 `journalctl -u <unit>`，含 bootstrap/登号/run 转发的 stdout，免手动 SSH 排障）；既有 `/api/nodes`（列表/详情/status）、`/api/scale-out`、`/api/scale-in`、`/nodes/{id}/drain`、`DELETE /nodes/{id}`（终止实例）。
- **前端**（`api/routes/`）：`/api/accounts`（账号池 CRUD，`core/account_store.py` JSON 存储，与 `accounts.json` 同 schema）；`/api/jobs`（提交/列表/详情）+ `/api/jobs/harness`（上传 Harness 代码→`harness_ref`）；`/batch` Batch Console 单页（账号面板 + 任务表单[声明式+上传代码，含 disk_gb/spot/needs_docker/s3_datasets] + 实时任务监控）。凭证绝不入前端/Manager。

## 依赖链（重要）

```
Claude-Code-PTY (claude-pty)  ←  elastic-agent[pty]  ←  下游 harness（audio_book_echo_agent 等）
```

- claude-pty 通过 `[project.optional-dependencies] pty` + `[tool.uv.sources]` 声明，**版本 pin 在 uv.lock**
- **机制保证（优先）**：`scripts/refresh_deps.sh` 自动对比已装 claude-pty commit 与 PTY main HEAD，落后即刷新 lock + 重装——部署/启动流程必须包含它（推荐挂 systemd `ExecStartPre`），`git pull` 本仓库后跑一次即与上游一致
- 手动级联（脚本不可用时）：`uv lock --upgrade-package claude-pty && uv sync`，提交 uv.lock 并 push
- **本仓库更新后**，提醒/级联下游 harness：`uv lock --upgrade-package elastic-agent && uv sync`
- **任务生命周期补充**：领取任务时（步骤 1）先检查上游是否有新版本（`uv lock --upgrade-package claude-pty --dry-run` 或对比 PTY main HEAD 与 lock 中 pin 的 rev）；若本次改动涉及 PTY 接口适配，必须同步 bump lock
- worker 侧 claude-pty 由 bootstrap `pty_install_step` 安装——下游应传入与其 lock 一致的 pinned URL（`pty_package="git+https://github.com/zjw49246/Claude-Code-PTY@<rev>"`）

## Git 信息

- Remote: https://github.com/zjw49246/Elastic-Agent.git
- 默认分支: main

## 任务生命周期

你收到任务后，按以下 9 步流程自主完成：

1. **领取任务** — 你已被分配任务，阅读本文件和项目代码理解上下文
2. **创建工作区**:
   - `git fetch origin`（如有 remote）
   - `git worktree add -b task-<简短描述> .claude-manager/worktrees/task-<简短描述> origin/main`
   - 进入 worktree 目录工作（后续所有操作在 worktree 中）
   - 如果 worktree 创建失败，直接在当前分支工作
3. **实现功能** — 编写代码，确保可运行
4. **提交代码** — `git add` + `git commit`，commit message 简洁描述改动
5. **Merge + 测试**:
   - `git fetch origin && git merge origin/main`（集成最新代码，如有 remote）
   - 运行测试（如有测试命令）
6. **自动合并到 main**（如有 remote）:
   - `git fetch origin main`
   - `git rebase origin/main`，如果冲突则自行 resolve
   - 如果成功：`git checkout main && git merge <task-branch> && git push origin main`
   - 如果这一步有任何失败，退回到步骤 5 重试
   - （纯本地项目跳过本步）
7. **标记完成** — 更新文档（必须在清理之前，防止进程被杀时状态丢失）
8. **清理** — 回到项目根目录:
   - `git worktree remove .claude-manager/worktrees/<worktree名>`
   - `git branch -D <task-branch>`
   - 如有 remote: `git push origin --delete <task-branch>`
9. **经验沉淀** — 在 PROGRESS.md 记录经验教训（可选）

### 冲突处理

rebase 发生冲突时：
1. 查看冲突文件: `git diff --name-only --diff-filter=U`
2. 逐个解决冲突
3. `git add <resolved-files> && git rebase --continue`
4. 如果无法解决: `git rebase --abort`，退回步骤 5

### 状态判断

- 通过 `git remote -v` 判断是否有 remote
- 有 remote → 必须完成步骤 6（merge + push）
- 无 remote → 跳过步骤 5 的 fetch、步骤 6 和步骤 8 的远程分支删除

## 文件维护规则

> **以下文件都由 Claude Code 自主维护，每次功能变更后必须同步更新。**

- **CLAUDE.md**（本文件）：架构、约定、关键路径变化时更新，只改变化的部分，保持简洁
- **README.md**：面向用户的文档，功能、使用流程变化时同步更新，保持与实际代码一致
- **TEST.md**：测试指南，新增功能时同步添加测试用例和文档
- **PROGRESS.md**：见下方「经验教训沉淀」

## 测试规范

**开发时必须主动使用测试，不是事后补充！**

- **改代码前**：先跑测试，确认基线全绿
- **改代码后**：再跑一遍确认无回归
- **新增功能**：同步新增测试用例，更新 TEST.md
- **修 bug**：先写复现 bug 的测试（红），修复后确认变绿

## 经验教训沉淀

每次遇到问题或完成重要改动后，要在 PROGRESS.md 中记录：
- 遇到了什么问题
- 如何解决的
- 以后如何避免
- **必须附上 git commit ID**

**同样的问题不要犯两次！**

## 注意事项

- 在 worktree 中工作时，不要切换到其他分支
- 完成任务后确保代码可运行、测试通过
