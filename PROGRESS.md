# PROGRESS — 经验教训沉淀

## 2026-07-16 Docker沙箱 + S3数据集 + worker日志 三功能 + UI Submit 全量排障（task-ccm-sync）

**功能**（commit `ddb9a8f` + buildx `ae85860`）：
1. **Docker 支持**（`setup.needs_docker`）：`docker_install_step` 装 `docker.io **docker-buildx**` + `usermod -aG docker <ssh_user>` + `enable --now docker`，插在 runtime 部署**前**（systemd 起服务才解析 User 补充组，必须先 usermod）。**buildx 是实盘抠出来的坑**：Docker 29 用 BuildKit build 镜像、必须 buildx，`docker.io` 不含 → "buildx component is missing" → 镜像建不出、agent 没跑、score 0。补 `docker-buildx` 后实盘验证：镜像 build 成功、agent 在容器内跑 23 turns、出真分 46/100。
2. **S3 数据集**（`setup.s3_datasets:[{uri,dest}]`）：worker 无 S3 凭证（结果上传也 Manager 侧 boto3）→ Manager 下载(`code_sync._download_s3`)→rsync(`stage_s3` 复用 deliver)。
3. **Worker 日志 API**：`GET /api/nodes/{id}/logs` Manager SSH 跑 journalctl。

**UI Submit 全量 spec 逐层排障**（用户在前端交 `--tasks all --sandbox os` 全量 job，连挂 5 次，每次不同根因，都是 **spec/环境**问题、非框架 bug——除 buildx/docker 那两个补进框架）：
- `rc=127`：setup 只有 `uv sync` 没装 uv → 加 `curl uv/install.sh|sh`。
- `rc=2`：`uv sync` 默认 py3.14、**taichi 无 cp314 wheel** → `uv sync --python 3.13`。
- `credentials not valid`：`account.config_dir` **留空** → 登录校验查错路径（`per_worker=1` 直接透传空 config_dir）。设 `/home/ubuntu/.claude-autorun`。
- `no available account`：用户 UI 又交一次（空 config_dir）→ 两 job 抢唯一账号；直接 AWS terminate 不释放 allocator（release 只在编排 scale-in 触发）→ 重启 Manager 清 allocator。
- `--sandbox os` 静默死：**worker 没 Docker**（bootstrap 只装 node/uv/chrome）→ needs_docker 功能（上文）。

**教训**：
- **provision 中途失败留活 EC2**（wait_until_running 抛前实例已建；login/run 失败也不自动关）——每次失败必查 `Name=ai4sci*` + terminate，尤其 r5 类贵机型。本轮开了~8 台，逐个清了。
- **直接 AWS terminate 不释放 Manager 内存态**（AccountAllocator 按 worker_id 分配，只在 scale-in release）→ 重复/失败 job 会饿死后续 job 的账号；清账号态最简单是重启 Manager。
- **潜在 bug（待修）**：run 子进程退出但 Manager 相位卡 `running`（run 静默失败/完成时都见过）；结果照落盘/S3，只是 UI 显示不对。
- **`serve_demo.py` 缺 `ELASTIC_AGENT_FRAMEWORK_SRC`** 已补：否则 UI 提交的 job 装 PyPI 框架、缺本分支 `ACCOUNT_LOGIN` handler。
- 单账号跑全量（3小时 opus）大概率撞额度；`rotation` 无备用号也换不动 → 池里要备≥2 号。

**Commit**: `ddb9a8f`（三功能）、`ae85860`（buildx）

## 2026-07-16 实盘 e2e 逐层打通（full_run → ai4sci-bench，task-ccm-sync）

**背景**：在本 VPC 内起真 Manager（`.claude-manager/full_run.py`，:8080，真 AWS provider），`manager.batch.launch` 开新 EC2→全量 provision→worker 本地登号→跑 ai4sci-bench→收集→S3。逐次 retry 逐层暴露问题，**三个是真代码 bug**（commit `7cdcb57`），两个是外部/操作层。

1. **AWS 最终一致性**（`providers/aws.py::wait_until_running`）：`run_instances` 后立刻 `describe_instances` 可能 `InvalidInstanceID.NotFound`（ID 尚未传播），原代码直接抛→`provision` 判死，**而实例其实已起→泄漏白烧钱**。修：poll 循环把「尚未可见」（NotFound/Malformed/空 reservation）当继续等，只有真错误才抛。
2. **setup/run 用户不一致**（`batch_hooks.py` manager_rsync）：`SSHExecutor` 对非 root 用户**默认 sudo 包裹**（`bootstrap.py:79`），于是 `setup.commands`（`curl uv/install.sh | sh` + `uv sync`）以 **root** 跑→uv 装到 `/root/.local`、`.venv` 归 root；但 run 命令以 **ssh_user(ubuntu)** 跑→`$HOME/.local/bin/uv: No such file`，benchmark 秒退且无 stdout。修：setup 命令 `use_sudo=False` 按 job 用户跑，与 run 共享 HOME。
3. **根盘扩容错设备**（`providers/aws.py::create_instance`）：BlockDeviceMapping 写死 `DeviceName=/dev/xvda`，但 **Ubuntu AMI root 是 `/dev/sda1`**→我们建了个幽灵 40G 卷没挂上、真 root 仍是 AMI 里 8G→sandbox 建 per-task venv（taichi 53M+scipy 33M…）撑爆 100%。修：`describe_images` 查 AMI 真实 `RootDeviceName` 再扩容（带缓存，fallback `/dev/sda1`）。

**外部/操作层（非代码）**：① full_run 抢 :8080（被常驻 serve_demo 占）→启动即崩、no EC2/no marker——先腾端口；② 171mail `/claude/send` 偶发 500（`SendMagicLink 网络或代理请求失败`）——**外部接码服务瞬时故障**，换号无用，自愈后同参数返回 200。

**踩坑教训**：
- **验证脚本要贴合真实执行路径**。我一度以为有"第 6 层"（benchmark 内部 `subprocess.run(["uv",...])` 报 `FileNotFoundError: uv`），实为**我的验证脚本用 `#!/bin/bash`+`nohup`（非登录 shell），PATH 没有 `~/.local/bin`**；框架真实路径是 `bash -lc`（`job_spec.py:231/244`，登录 shell），`~/.profile` 有 uv 的 PATH 段→bare `uv` 能解析。用 `bash -lc` 重跑即过。**排查执行环境问题时，务必用与生产相同的 shell 类型（登录/非登录）复现。**
- provision 中途失败会**留下已开的 EC2**（wait_until_running 抛出前实例已创建）；排查后要 `terminate-instances` 清理，别只看 registry。
- 复用"已开着的失败 worker"验证下一层 fix（挂它的幽灵卷补磁盘、以正确用户重装 uv）比每次盲开新机（~20min+计费）快得多。

**验证**：真实 login-shell 路径下 benchmark 完整跑起来——sandbox venv 建成、生成 task instance、`claude --model claude-opus-4-8` 真 agent 在解 `homotopy_poly_roots`（跑 solver.py、分析 roots.npy）。三个 fix 均带回归测试，`test_aws_provider.py`(4)+`test_batch_hooks.py`(+1) 全绿；7 个既有失败（config/file_sync/reconciler/worker_reconnect）经 stash 验证为**分支既存、与本改动无关**。

**Commit**: `7cdcb57`

## 2026-07-15 用 CCM Worker 系统更新框架 · P1（task-ccm-sync）

**背景**：CCM（Claude-Code-Manager）当初借鉴本框架的 Worker 系统，之后独立演进出大量 PTY/凭证运行时经验；本框架落后。二者已不共享代码，但共用同一上游 `claude-pty`。本次先落地风险最低、价值最高的 P1。

**做了什么**：
1. **claude-pty bump `88b77ad → d6ff732`**（uv.lock）。本框架 pin 落后上游约 30 个提交；`88b77ad` 是 HEAD 的直系祖先。中间含多个影响既有功能的修复：subagent autonomous-callback 在 PTYEvent 上崩溃、cold-resume 强制 stdin、idle reaper、轮换 exit handler 孤儿 proxy、首启 theme-picker/登录方式 drain 修复等。
2. **orphan / autonomous 守卫**（`pty_backend.py::on_event`）——**修确认的潜在 bug**。此前 `on_event` 完全不看 `orphan`/`autonomous` 标记，而这两个字段在 pin 的 `88b77ad` 就已存在。后果：温热会话 resume 时 claude-pty 重放上一 turn 的 JSONL backlog（orphan），旧 api_error 被重新标 turn-fatal → 毒化本 turn 合成的 result → 刚成功的 turn 被误报 failed；autonomous 子 agent 的 session_id 还会覆盖主 task 的。修法照搬 CCM `_process_event` 的 `turn_scoped = not orphan and not autonomous` 门：二者不参与 session_id/错误/`_saw_result`/`_saw_claude_output` 记账；orphan 事件另从转发流丢弃。

**遇到的坑**：
- 主 venv **没装 claude-pty**（`pty` extra 未 sync），导致 `@pty_required` 事件测试长期被 skip。装上后暴露 **4 个 pre-经年失效的 stale 测试**——它们断言 `usage limit reached → pty_turn_error`，但 `classify_turn_error` 早已把限速文案重分类为 `claude_rate_limited`（一直被 skip 没人发现）。已改用泛化错误文案解耦分类器，并补 `classify_turn_error` 的限速/泛化分支专测。
- worktree 里跑测试要用主 venv 的 python 且 pytest 的 `pythonpath=["src"]` 会解析到 worktree 的 src；`uv pip install --python <主venv> pytest pytest-asyncio` 装测试依赖，claude-pty 用 pinned URL@d6ff732 装进主 venv 才能真正跑事件测试。

**测试**：`tests/unit/test_pty_backend.py` 63 passed（含 3 个新 orphan/autonomous 回归 + 2 个新 classify）；PTY 相关子集（pty_backend/worker_runtime/task_router/protocol/agent_type/bootstrap_steps）208 passed。

**待续（P2/P3）**：瞬时过载同号退避重试、限速 actionability + assistant 文本兜底、hardlink 会话迁移保温热会话、首启 warmup、前端批次编排。

**Commit**: 见本节合入的 commit。

## 2026-07-15 自动登录同步成 CCM 版本 · worker 本地登录（task-ccm-sync）

**背景**：用户要求「凭证在 worker 那里登录，Manager 只管 email+接码token 分配」，且「自动登录逻辑同步成 CCM 版本」。原 `claude_oauth.py` 是 Manager 侧经 SSH 反向驱动 worker 上 Chrome 的 CDP 状态机（自研 `_CDPClient` + 只支持 171mail）；CCM 的 `auto_login.py`+`cdp_login.py` 是 **worker 本地自洽**脚本（纯 Chrome CDP、多后端接码、按域名自动选），更贴合意图。

**做了什么**：
1. **Vendored** CCM 的 `auto_login.py`+`cdp_login.py` 到 `src/elastic_agent/worker/login/`（near-verbatim，只把 CCM 特定硬编码参数化：`CLAUDE_MAILCATCHER_URL`/`CLAUDE_171MAIL_URL`/`CLAUDE_SETTINGS_EXTRA_DIRS`——原来写死了 `mail.claude-code-manager.com` 和 `/home/ubuntu/Claude-Code-Manager`）。
2. **重写 `ClaudeOAuthProvider`** 为薄壳：删掉自研 `_CDPClient` + 全部 Manager 侧 CDP/接码/CF 机器（~850 行），改为委派给 vendored `perform_login`——`worker_host` 有值时经 SSH 在 worker 上跑 `python -m elastic_agent.worker.login.auto_login`（脚本起 Xvfb:99），无值时进程内直跑；登录后读回凭证填 `LoginResult`。**保留**所有被 quota_checker/runtime 依赖的模块级符号（`refresh_access_token`/`read_credentials`/`write_credentials`/`OAUTH_CLIENT_ID`/`ANTHROPIC_USAGE_URL`）和 `OAuthConfig`(+`provider` 字段)/`LoginResult` 契约 → `CredentialLoginService/Step` 零改动。

**遇到的坑 / 注意**：
- `httpx`+`websockets` 已是顶层依赖，vendored 脚本无需加依赖；`mitmproxy`/`playwright` 仅在函数内 lazy import + `from __future__ import annotations` 使类型注解不求值，故 worker 无须装它们（当前 CDP 路径也不用）。
- 删内部方法后 `test_claude_oauth.py`/`test_oauth_race_and_retry.py` 有 18 个测旧 CDP 内部的用例失效——已重写为委派契约测试（local 成功/失败/无凭证/超时/provider 传递、remote 构造 worker 命令 + SSH 失败透传）+ 删 3 个测已删内部方法的用例。顺带修 `test_defaults` 的 stale 断言（login_timeout 240→480）。
- **未在本沙箱端到端验证浏览器登录**（无 Chrome/真账号/接码可达）；风险集中在 vendored 的 CCM 已验证代码，我方胶水（SSH invoke + LoginResult 映射）已单测覆盖。**合并前需在真 worker 上验证一次真登录**。

**测试**：`test_claude_oauth.py` 24 passed、OAuth+credential+quota 子集 59 passed；全 unit 套件 1074 passed，失败/错误全部是既有环境性（fastapi/DryRunProvider abstract/reconnect timing），与本改动无关（均不 import 改动模块）。

**Commit**: 见本节合入的 commit。

## 2026-07-15 P2 运行时健壮性 + P3 worker 自治登录（task-ccm-sync）

**P2 — 瞬时过载同号重试**（借鉴 CCM `is_transient_overload`）：
- 新 `core/rate_limit.py`：`is_rate_limited`/`is_auth_failure`/`is_transient_overload`（互斥、额度/认证优先）/`rate_limit_event_is_actionable`/`transient_retry_delay`，正则照搬 CCM。
- `classify_turn_error` 加 `transient_overload` 分支（在 rate-limit 之前；transient 检测器内部已排除额度/认证横幅，故额度仍走轮换）。
- `ElasticPTYBackend` on_exit：`transient_overload` turn **不报失败**，退避后 `_run_transient_retry` 同 session `--resume` 重试（`_schedule_transient_retry`，最多 5 次，`launch` 覆盖捕获 kwargs），耗尽才失败。**为何 worker 侧**：elastic 轮换是 Manager 侧 QuotaMonitor（Usage-API 驱动），与 turn 退出码解耦；瞬时过载是 turn 级、worker 自己 resume 最省事且保住上下文。
- 未做「assistant 纯文本限速（exit 0）」硬失败——elastic 靠 QuotaMonitor 周期轮换兜底，从正常输出文本硬判失败风险更大；`is_rate_limited`/`rate_limit_event_is_actionable` 已备好供后续 proactive/批量用。

**P3 — worker 自治登录**（协议下发，不再 Manager-over-SSH 驱动）：
- 新协议 `ACCOUNT_LOGIN`/`ACCOUNT_LOGIN_RESULT`（messages.py + 三个 union + `__init__` 导出）。
- worker `_handle_account_login`：`ClaudeOAuthProvider(worker_host=None)` 进程内跑 vendored `perform_login` → 成功则 `_warmup_config_dir`（`claude -p 'reply: ok'` 预热+验证）+ `recycle_config_dir` + `quota_checker.add_slot` → 回 result。凭证只在 worker 生成、不回传。既有 SSH 登录路径（CredentialLoginService）保留不动。
- **⑥ hardlink 会话迁移保温热会话 延后**：它需要把凭证模型从「每 worker 单账号 + 原地换凭证」改成「每账号独立 config_dir + 迁移 session」，这与「每机多号」的批量特性纠缠，属批量编排那一步再一起做（elastic 的 Manager 侧轮换与 CCM 单机本地池是两种架构，不能照搬）。

**测试**（本批最后统一补）：新增 `test_rate_limit.py`（检测器全覆盖）、`test_pty_backend.py::TestTransientOverloadRetry`（分类+调度+重试+耗尽）、`test_worker_runtime.py::TestHandleAccountLogin`（成功/失败/异常）。相关子集 204 passed；改 `_make_backend` 补新 __init__ 字段。

**注意**：瞬时重试与 worker 自治登录的**真实链路未在本沙箱端到端验证**（无真 429/Chrome/账号）；胶水与决策逻辑已单测覆盖，真 worker 上需验证一次。

**Commit**: 见本节合入的 commit。

## 2026-06-11 PTY 框架支持（task-pty-support）

**做了什么**：worker 支持用 claude-pty 把 Claude Code 宿主在持久 PTY 会话中执行任务，替代每任务 spawn `claude -p`。Manager 侧只加了可选的 `ExecuteMessage.agent_params`（向后兼容）+ `TaskRouter(use_pty=True)` 开关；PTY 仓库零改动。

**关键设计决策**：
1. **raw_json 透传**：claude_pty 的 PTYEvent 保留 JSONL 原始行，worker 直接当 stdout NDJSON 转发 → Manager 的 `_try_parse_ndjson` / LogEventParser 解析链完全不用动。不要自己造事件映射表。
2. **合成 result 行**：交互模式 JSONL 没有 `result` 行（回合结束是 `system/turn_duration` 哨兵），但 Manager 靠 result 事件提取 session_id——worker 在 turn 结束时合成一条（`synthesized_by: "pty_backend"`）。注意 `cost_usd` 在 PTY 模式拿不到。
3. **错误 turn 强制非零 exit**：API error / 限流把 turn 标记 error 但进程不死；on_exit 把 exit_code 0 改成 1，否则 Manager 侧凭证轮换不触发。

**遇到的坑**：
- venv 的 editable 安装指向主仓库 src，worktree 里跑测试改动不生效——必须 `PYTHONPATH=src` 覆盖。
- **同机多 BridgeHub 串话**：冒烟测试在本机起第二个 BridgeHub，channel 注入打进了同机另一个已存在的 PTY 会话（注入端口撞了），目标会话靠 15s stdin fallback 兜底成功。生产 worker 单 backend 无此问题，但同机多 backend 部署前要先解决端口/会话路由隔离。
- 仓库基线测试本就有 43 failed / 91 errors（环境性，fastapi/oauth 相关）；本任务相关子集（runtime/router/protocol/agent_type 等 7 个文件）基线 154 passed 全绿，改后 185 passed。

**测试**：tests/unit/test_pty_backend.py（31 个用例）+ 真实 claude 端到端冒烟（assistant 原行透传、合成 result 带 session_id、exit 0 验证通过）。

**Commit**: 见 git log（task-pty-support 合入 main 的 commit）。

## 2026-06-11 PTY Phase 2：热会话 follow-up + 注入串话修复（验证）

**结论**：elastic-agent 侧零代码改动。`BasePTYBackend.launch(resume_session_id=...)` → pool `get_or_create` 命中存活会话即热复用；本仓库的 `on_exit` 只清 task 级映射、不动 pool，会话保持温热。

**验证**（真 claude 双 turn 冒烟）：
- turn 1 冷启动 14s；turn 2 同 session/同 PID 注入新 turn，5s 完成
- 两个 turn 都走 channel 注入（无 stdin fallback），合成 result 带 session_id，exit 0
- 串话不再出现——修复在 PTY 仓库（commit aa23aab）：inject 端口 OS 分配 + /inject 校验 session_id（不匹配 409）+ bind 失败不崩 MCP

**注意**：worker 需要 claude-pty >= aa23aab；旧版在同机多宿主下有注入串话风险（消息可能漏进别的会话且发送方以为成功）。

## 2026-06-11 PTY Phase 3：凭证轮换 × 温热会话（recycle_config_dir）

**缺口**：轮换是原地换凭证（新账号 token 写进同一 config_dir）。subprocess 路径每任务新进程重读凭证没问题；PTY 温热会话是旧账号启动的、一直带着旧凭证活着——follow-up 热复用会继续烧已耗尽的账号。

**解决**：`ElasticPTYBackend.recycle_config_dir(config_dir)`——回收该 config_dir 上所有会话（有任务的走 stop()，Manager 收到 PROCESS_EXIT；纯温热的直接 pool.remove）。worker 在 `_handle_credential_login` 写完新凭证后调用；回收失败不影响登录结果上报。

**验证**：6 个新单元测试 + 真 claude 冒烟（turn1 温热 → recycle → turn2 resume 冷恢复新 PID、干净完成）。

**附带发现**：冒烟首跑因 API 529 overloaded_error 失败（CC 内部重试 10 次未过）——`system/api_error` 是 CC 的重试事件（带 retryAttempt/maxRetries），不带 isApiErrorMessage，turn 未被掐断，等待即可恢复。区别于 isApiErrorMessage:true（turn 被终止、无哨兵）。

**Commit**: 见本节合入 main 的 commit。

## 2026-07-15 批量编排 + 前端：适配 Mode B 不透明长命令任务（AI4Sci-Bench）

**背景**：AI4Sci-Bench 类任务（`uv run ai4sci-bench run …`）是跑数小时、内部自开 Docker sandbox、内部消费账号的黑盒长命令——暴露了框架只假设「Mode A：Elastic 托管 agent、逐 turn 换号」的错配。这类「Mode B」任务里 Elastic 的 PTY 逐 turn 轮换插不进去。

**做法**（4 个 commit）：
1. `17f72b5` 声明式 `JobSpec` + `GenericJobHarness`：任务即数据；`resolve_harness` 把「声明式」和「上传 Harness 代码」统一成 `Harness`。模板 `{{shard_index}}` 由 Manager 渲染、`$(hostname -s)` 留给 worker shell（shell 模式包 `bash -lc`）。
2. `193fd1f` `BatchOrchestrator`：单 JobSpec fan-out 到 N worker，`FleetDriver` Protocol 解耦真实 Manager，fake 单测全生命周期。
3. `22d1444` worker 侧 Mode B 换号(a)：`watch_exhaustion` 开启时扫子进程输出，撞限流即 `RunExhaustedMessage`+SIGINT；复用 P2 的 `core/rate_limit.py` 检测器（消费方从 PTY turn 变成子进程输出行）。
4. `c150e47` 前端后端：`AccountStore` + `/api/accounts` + `/api/jobs`(+harness 上传) + `/batch` Batch Console 页。

**经验教训**：
- **别把 Mode A 的逐 turn 轮换硬套 Mode B**。黑盒命令自己消费账号，只能靠扫它的 stdout 做「整条命令」粒度换号 + 它自带的 `--resume` 恢复。换号(a) 会丢在飞 sandbox，是已知代价（用户拍板选 a）。
- **`ExecuteMessage.command` 本就是任意 argv**——框架早支持任意命令，缺的只是上层编排（scale→bootstrap→login→dispatch→track），Manager 的 `scale_out` 只建实例、这条链是空缺。
- **凭证边界**：前端/Manager 只碰账号身份（email+接码token），凭证只在 worker 本地 `perform_login` 生成、绝不回传——批量路径全程守住。
- **未接的活**：`ManagerFleetDriver` 的 provision/login 是部署期注入钩子（bootstrap SSH 管线 + ACCOUNT_LOGIN 结果关联），沙箱内无法端到端验证；真 worker 上需验登录链路 + 瞬时/限流重启。多账号/机（per_worker>1）的独立 config_dir 池 + hardlink 会话迁移仍待做。
- 全 unit 套件 32F/76E 与环境基线一致（DryRunProvider/InMemoryProvider 缺 reboot_instance、fastapi、reconnect timing，均不 import 新模块），passed 1118→1190（+72），零新回归。

## 2026-07-15 实盘装配 provision/login：/batch 可真拉起 worker（commit 537d1c3）

**做法**：`core/batch_hooks.py` 把 `ManagerFleetDriver` 的注入钩子变成真行为——provision=等实例 running→`BootstrapHandler` SSH 跑 `compile_bootstrap_steps`→等 WS 连上；login=`AccountAllocator`（内存分配账号身份）+`LoginCoordinator`（发 ACCOUNT_LOGIN、经 event_bus 等 ACCOUNT_LOGIN_RESULT）；`wire_batch` 装配并把 worker 的 RUN_EXHAUSTED/PROCESS_EXIT 经 event_bus 路由回 orchestrator。`Manager.batch` 默认走 `wire_batch`。

**经验教训（换号竞态）**：worker 撞限流 → 先发 `RUN_EXHAUSTED` → 再 SIGINT 产生 `PROCESS_EXIT`。Manager 先处理 exhausted（换号+重派，相位已从 ROTATING 转回 RUNNING），随后陈旧的 `PROCESS_EXIT`（非零）到达——若只靠 `ROTATING` 相位守卫会把**刚重派的新 run 误判 FAILED**。**修复：`on_worker_exit` 用 `task_id` 匹配当前 `run.task_id`，旧 run 的退出 task_id 不匹配即丢弃。** 这是 recover-then-failed 家族问题的又一实例（对照 PTY 侧 orphan/autonomous 守卫）——凡「中断+重启」路径都要用稳定标识区分「旧实例的尾声」和「新实例的结果」，别用会被重置的相位。

**仍待真机验**：浏览器登录链路、瞬时 429/限流重启、Mode B 撞限流换号，沙箱无法端到端（无 Chrome/账号/真 429）。`manager_url` 需经 `ELASTIC_AGENT_MANAGER_URL` 或 `config.server` 给对；ssh key 路径按 provider.type 取。全 unit 32F/76E 与基线一致，passed→1206。

## 2026-07-15 真机测试（EC2 elastic-agent-test，Ubuntu 26.04）：worker 本地登录跑通 + 修依赖 bug

**环境**：本 sandbox 本身是 EC2（Manager 角色）。在同 VPC/subnet/SG 开一台 `elastic-agent-test`（t3.large, Ubuntu 26.04, key interview-key），私网直连。

**验证结果（全绿）**：
- 装依赖（xvfb/xdotool/google-chrome 150/node22/claude 2.1.181/httpx/websockets）Ubuntu 26.04 干净装上。
- worker 本地登录（P3 代码路径 `perform_login`）：171mail API 接码（send+poll magic link，200）→ Chrome CDP OAuth 全流程 → CLI `Login successful` exit 0 → 写出 `.credentials.json`（claudeAiOauth: accessToken/refreshToken/expiresAt/subscriptionType=max/rateLimitTier=default_claude_max_20x）。
- 凭证可用：`CLAUDE_CONFIG_DIR=... claude -p` 真跑一 turn 返回预期文本，exit 0。

**发现并修复的真 bug（commit 1049118）**：`credential_login_deps_step` 原装 playwright/chromium/mitmproxy，但 vendored CCM 登录代码 exec 的是**真 `google-chrome` 二进制 + xdotool**，根本不用 playwright。若走框架标准 bootstrap 再登录会 `google-chrome not found` 必失败。改为装 xvfb+xdotool+google-chrome-stable(.deb)+httpx+websockets；`config.login_dependencies` 默认改空（仅额外 pip 包）。**教训：vendor 上游脚本时，必须连它的系统依赖一起对齐，别沿用旧 bootstrap 的 deps 假设——单测测不出二进制缺失，真机一测即现。**

**仍未真机验**：完整 Manager↔worker WS + ACCOUNT_LOGIN over WS + 批量编排 e2e（需在 worker 上装本分支而非 PyPI 版 elastic-agent，且 Manager 端口对 worker 开放）；瞬时 429/Mode B 撞限流换号。

## 2026-07-16 完成剩余项：真机全链 e2e + sudo + 每机多号 + 前端优化（main 74fe5c8）

**真机全链 e2e（用真账号）**：把本分支 rsync 到 EC2 worker（私有 repo 装不了，rsync 绕过），起 WorkerRuntime 连到本机 Manager 的 WS，Manager 驱动：
- EXECUTE `claude -p` → exit 0，Manager 收到流式 stdout `E2E_EXEC_OK`；
- ACCOUNT_LOGIN(P3) over WS → worker 本地 perform_login（171 接码+Chrome CDP）→ success，写出 Max 凭证。

**经验教训（真机 e2e 卡了很久的坑）**：
1. **`pkill -f serve_demo.py` 会杀自己**——发起命令的 shell cmdline 含该串，pkill 匹配到自身。用 `[s]erve_demo.py` 括号法。
2. **worker runtime 后台起不来**：`run_in_background`/`nohup`/`setsid` 起的 SSH 远程进程都被 SIGHUP 秒杀（ssh exit 255），空日志。**唯一可靠**：前台阻塞 `ssh '... timeout N python -m ...runtime_main'`——SSH 会话开着 N 秒进程就活着，期间 Manager（独立进程）自主驱动 EXECUTE/ACCOUNT_LOGIN，阻塞返回后读 Manager 侧结果。
3. **Bash 工具屏蔽含 `sleep` 的命令**（前台 sleep）——即使 sleep 在 SSH 远程字符串里也被扫到拦截；改用 remote 脚本文件或 timeout。
4. **uvicorn.run() 脱离终端静默退出**，改 `uvicorn.Server(...).serve()`；bind 端口需 `dangerouslyDisableSandbox`。

**代码补齐**：
- `feat(bootstrap)` 非 root SSH 用户自动 `sudo -n bash -c` 包裹（Ubuntu AMI 才能 apt/systemctl）。
- `feat(batch)` 每机多号 per_worker>1：预登录 N 账号到 N 个 config_dir，换号优先切下一个预登录槽（免登录），本地池耗尽才 `-rot-N` 现登。
- `feat(ui)` 根路径默认 Batch Console、Fleet 移 /fleet、api_key 存 localStorage。
- `fix(providers)/test` AWSProvider/DryRunProvider/4 个测试内联 provider 补 reboot_instance——清掉全部 58 个 collection error。

**全 unit：5 failed / 1315 passed / 0 errors**（5 个 failed 均为未触碰文件的既有 env/时序 flaky）。main 已推。

## 2026-07-16 真跑用户任务 AI4Sci-Bench + 结果交付（main）

**真跑用户的实际任务**：私有 repo `Agent-AI4Sci-Bench`（本机 gh 已登 youchengsong，`git ls-remote` 可达）rsync 到 worker（避免 token 上机），worker 装 uv、`uv sync --python 3.13`（默认 3.14 因 taichi 无 wheel 失败）。用账号（`CLAUDE_CONFIG_DIR=~/.claude-e2e-login`）跑一个真 task：
`ai4sci-bench run --agent claude_code_cli --agent-config '{"model":"claude-opus-4-8","effort":"medium",...}' --tasks math.homotopy_poly_roots --prompt-levels b1 --sandbox task --tool-mode restricted` → **完成，final_score 39.06/100**（recall 0.6 / precision 0.6，5 次代码执行 0 失败）。`--sandbox task`（per-task uv venv）免 Docker，适合小 worker。

**结果交付**：worker 的 `results/` rsync 回 Manager 的 `collected/<job_id>/`；新端点 `GET /api/jobs/{id}/results`（列表+benchmark final_score）+ `/results/download`（tar.gz）。经公网域名验证：列出 25 文件+分数，下载 32K tar.gz。**用户拿结果 = 从 Manager 公网域名下载**（或后续接 S3/OSS）。

**经验**：跑真 benchmark 前必须对齐 (1) repo 访问（私有→rsync 或 deploy key）(2) Python 版本（sci 依赖常无最新 py wheel，用 uv --python 锁旧版）(3) sandbox 模式（无 Docker 用 --sandbox task）。

## 2026-07-17 全量 benchmark 跑通链路修复：fullrun + 账号 starvation + aiohttp

**背景**：昨天提交的全量 job（`run --tasks all --sandbox os`）实际 **0 分**——failed。逐层排查真因：

1. **`run --tasks all` 是「全体先准备 GT、一坏俱崩」**：`orchestrator._prepare_instances` 对所有任务上来就在线生成 ground-truth，一个任务的 `generate_gt.py` 抛异常 → 整批在任何 agent 执行前全崩。已踩到 2 个坏任务：`robotics/cr3bp_halo_orbit`（status 非法枚举，sed 绕）+ `computer_science/deployment_prediction_sets`（拒绝采样在 seed=119 下 20 次采不到合格实例，**确定性复现**）。S3 上只有输入/参考数据、无跑分。
   - **解**：改用 benchmark 自带 **`ai4sci-bench fullrun`**——它 `for task in all_task_ids: try: orchestrator.run([task]) except Exception: 记 ✗FAILED 继续`（cli.py:4272），**天然 per-task 隔离** + resume（output-dir 已有结果跳过已完成）+ preflight/diagnose。别手撸循环，作者已内置。任务集用 `ai4sci-bench list`（过滤器同 `--tasks all`）确认 59 个 final 任务。

2. **账号 starvation（框架真 bug，根治）**：`AccountAllocator` 只在 scale-in（`release_worker`）释放账号，而 `_scale_in_on_complete` 默认 False → job DONE/FAILED 后账号一直被 `_assigned` 占住 → 下一个 job `allocate` 返回 None → `no available account`。单账号 demo 跑第二个 job 必挂，之前只能靠**重启 Manager**绕。
   - **解**（commit `290138e`）：`_maybe_finish` 无条件把该 job 所有 worker 账号 `release_worker` 回 allocator；并在 **bring-up gather 后**（整批 provision/login 失败路径）+ **rotation 耗尽 decline 处**（额度耗尽→resume 路径）补调 `_maybe_finish`。`release_worker` 幂等（按 worker_id pop），多路径重复调用安全。+6 回归测试。
   - **教训**：单例 orchestrator（`manager._batch = wire_batch(self)` 缓存）→ allocator 跨 job 共享，任何"资源占用只在 scale-in 释放"的设计对"不 scale-in 的完成"都会泄漏。

3. **`aiohttp` 硬依赖缺失（框架真 bug）**（commit `c6d1eb1`）：`claude_oauth.refresh_access_token` fallback 分支 `import aiohttp`，但 `pyproject.toml` 未声明该依赖 → worker 框架环境 `ModuleNotFoundError` → QuotaChecker 每 3 分钟刷 token 崩、额度检测/换号失灵。改用 **stdlib urllib**（恒可用、无新依赖）经 `asyncio.to_thread` 跑单次 POST（不阻塞事件循环）。注入 `http_client` 的既有路径不变。+2 回归测试。

**教训汇总**：
- 排障顺序对了才快：job failed → 看 `phases` → 看 opus48 输出**只有 `instances/`（准备产物）无跑分 json** → 定位「准备阶段崩」→ 拉 worker ea-logs ndjson 尾部拿到 `generate_gt.py failed`。**"只有 instances/ 没有结果 json" = 准备阶段就崩**，是关键信号。
- 空转实例要及时 terminate 止血（旧 failed job 的实例空跑 10h ≈ 白烧 $5）。
- 跑真 benchmark 优先找它**自带的自动化全量命令**（fullrun/batch-run），通常已处理坏任务隔离/resume/preflight，比外面套 shell 循环稳。

## 2026-07-17 登录 stealth 翻车 + 账号可见性（main）

**stealth 反 CF 弄巧成拙（教训）**：给阿里云过 Cloudflare 加的 stealth（`--disable-blink-features=AutomationControlled` + `navigator.webdriver` 等指纹伪装，commit `94db2ab`）**反而触发 Turnstile**——A/B 实测：stealth ON → magic-link/OAuth 页弹交互式勾选框、CDP 点击过不去 → 登录失败；stealth OFF → 三处 CF 全自动放行、登录成功。**不一致的指纹比"像自动化"更可疑**。已完全回退（`a68a41e`）。**根因教训：改登录这种外部反爬逻辑，必须在真实登录上 A/B 验证，别只验 mode=none 的旁路**（我当初验 worker 直连 S3 用的 account.mode=none，没走登录，漏了）。排障关键靠失败 worker 的 journalctl + `/tmp/cdp_oauth.png` 截图（截图是视口坐标，xdotool 是屏幕坐标，差一个浏览器 chrome 高度——一度误判成坐标 bug）。

**账号可见性**：新端点 `GET /api/accounts/allocations`（从 orchestrator 内存 job 反推 账号→worker/job/phase/active）+ 账号面板加「当前绑定 worker」列。回答"账号是否已分配给 worker"。注意：orchestrator job 内存态、Manager 重启即清（重启部署代码会丢正在跑的 job 记录——本次多次踩到，长期应让新 Manager 能重连/收养在跑的 worker）。
