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
- **账号资料与秘密边界**（`worker/login/`，Codex 流程参考 CCM）：账号以 `agent_type`（`claude|codex`）区分；Manager 将 email、邮箱查询 token 和 OpenAI password 保存在权限 0600 的账号文件中。Codex 账号必须至少有 `email_token` 或 `password` 之一（可同时配置）；REST 永不回显秘密，只返回 `has_email_token`/`has_password`。`ACCOUNT_LOGIN` 跨机传输必须使用 `wss://`（仅可信测试网可显式设 `ELASTIC_AGENT_ALLOW_INSECURE_ACCOUNT_LOGIN=1`）。
- **worker-local 自动登录**：Claude 沿用 Chrome CDP 登录、`claude auth status` 精确 email 校验和 `claude -p` 预热。Codex 在同一 worker 启动 `codex login`，用 Xvfb 下的系统 Chrome + Playwright 完成 OpenAI OAuth，并保留系统 Chrome 原生 UA（禁止固定伪装成与实际 binary 不一致的旧版本）：有 password 时走密码页，仅有 email token 时切换 OpenAI email-code/one-time-code/login-code 入口并自动取码；页面不提供该入口时明确要求补 password。Codex 浏览器预算由 `account.login_timeout_seconds` 经 `ACCOUNT_LOGIN` 下发（默认 900 秒、范围 60–1200），Manager 端到端等待 3600 秒，为 OTP、身份校验、smoke test 与清理留余量；超时错误只回传脱敏页面状态。可见 anti-bot challenge 最多等待 120 秒，仍未清除就明确提示核对账号绑定 EIP，不空等完整预算。生成的 `CODEX_HOME/auth.json` 必须是 ChatGPT OAuth、包含 access token，且 id_token email 大小写不敏感地精确匹配所选账号，随后 `codex exec` smoke test 必须成功。登录失败、Manager 超时或任务取消会通过关联的 `ACCOUNT_LOGIN_CANCEL` 停止 CLI/浏览器进程组并恢复旧 `auth.json`，worker 完成回滚后才发 `ACCOUNT_LOGIN_CANCELLED`；OAuth 凭证始终只留在 worker。单槽空 `config_dir` 由 worker 按实际运行用户解析 `~/.codex`；Codex 多槽/换号必须显式给可写的绝对 `config_dir`，禁止猜 `/root`。
- **Codex OTP / 邮箱边界**：Codex 的 `email_token` 是接码/邮箱查询 token，不是 OpenAI token、OpenAI password 或通用 IMAP 密码；password-only 遇 OTP、或自动取码失败时，worker 发 `ACCOUNT_LOGIN_OTP_REQUIRED`，Manager 通过 `GET /api/accounts/login-attempts` 暴露 challenge，管理员用 `POST /api/accounts/login-attempts/{login_request_id}/otp` 提交 6 位码，验证码转发后不持久化。邮箱查询前永久把进程内 `httpx`/`httpcore` 日志提升到 WARNING，防止带 query token 的完整请求 URL 落入 worker journal（不做临时恢复，避免并发泄漏竞态）。当前 Codex 取码支持 171mail，以及 163/mail.com/onet/gazeta 域名对应的 MailCatcher 后端；没有通用 IMAP。

## 两类任务 & 批量编排

- **两类任务模型**（关键区分）：
  - **Mode A — Elastic 托管 agent**（PTY 路径，上文）：任务=一个 prompt，Elastic 宿主 Claude、逐 turn 记账、逐 turn 换号。
  - **Mode B — 不透明长命令**：任务=一条任意 shell 命令（如 `uv run ai4sci-bench run …`），它自己开 sandbox、自己内部消费账号——Elastic **看不到**里面的 turn。此时 Elastic 只做：装环境（bootstrap）+ worker 本地登号 + 跑命令 + 盯输出 + 收结果 + 弹性缩扩。`ExecuteMessage.command` 本就是任意 argv（`create_subprocess_exec`），Mode B 天然支持。worker 会保留 stdin pipe 供 `SEND_INPUT` 交互；无人值守且会等 stdin EOF 的 CLI（例如直接跑 `codex exec`）必须在 shell 命令中显式加 `</dev/null`。
- **声明式 JobSpec**（`core/job_spec.py`）：任务即数据，所有外部 section 均 `extra="forbid"`，拼错字段直接 422。`environment.profile` 选择版本化、不可变的通用环境（默认 `ubuntu-agent-v1`；Docker 版 `ubuntu-agent-docker-v1`），Job 增量写在 `setup`/`run`。`setup.commands` 旧字符串列表继续兼容；新 `setup.steps` 支持 `name/command/env/cwd/timeout/retries` 且固定以 Job 用户执行。代码可用 `ref`（branch/tag）并用完整 `resolved_commit` 校验精确 checkout。`run.timeout` 缺省/旧 0 归一为 24h，`ttl_seconds` 默认 48h，二者最大 30 天且 TTL 不得短于 run。其余 section：`account`(**agent_type**/mode/per_worker/group/config_dir/**binding/ids**)/`rotation`/`fanout`(workers/shard_by/name_prefix/**instance_type/region/disk_gb/spot**)/`collect`/`completion`。`agent_type="codex"` 不支持 `manager_distribute`，运行时注入 `CODEX_HOME`；Claude 注入 `CLAUDE_CONFIG_DIR`。模板 `{{shard_index}}`/`{{num_shards}}`/`{{hostname}}` 由 Manager 渲染；`$(hostname -s)`/`$VAR` 留给 worker shell。**per-job 机器覆盖**经 orchestrator→driver→Manager，但 Job region 当前必须与 Manager provider region 一致。
- **cwd / 运行用户契约**（`JobSpec.resolved_cwd` + `compile_job_setup_steps`）：repo clone 到 `setup.target_dir`（= repo 根），setup 和 run 默认都从这里跑；相对 cwd 是其子目录，绝对路径原样用。系统初始化仍以 root 执行，Job-owned setup 固定降权到 provider 配置的 runtime/SSH 用户；`manager_rsync` 也逐 step 保留 env/timeout/retry 并以同一用户执行。
- **代码分发**（`setup.deliver`）：`setup.repo` 只接受无 HTTP userinfo/query/fragment 的远程 http(s)/ssh/git/scp-style URL，拒绝本地路径。`worker_clone` 仅用于 worker 无需 Manager 凭证即可 clone 的仓库，永不隐式下发 `ELASTIC_AGENT_GIT_TOKEN`；私库必须用 **`manager_rsync`**——`core/code_sync.py` `ManagerCodeSync` 在 Manager 本地 clone（token 只在此、`ensure_clone` 后 scrub 掉），再 `rsync --exclude .git` 到 worker（token/`.git`/remote 都不上 worker），最后 SSH 逐个执行结构化 setup step。repo 为空时 `manager_rsync` 仍会创建 target_dir 并执行 setup。setup 固定以 job 用户跑、不 sudo，必须与 run 同用户/HOME。
- **两条接入路**统一成 `Harness`（`harness/generic.py` `resolve_harness`）：`harness_ref` 空→`GenericJobHarness(spec)`（声明式编译 bootstrap/凭证槽/execute）；有值→导入上传的 `Harness` 子类。Harness 是 Manager 任意代码执行边界，API 上传和 `harness_ref` 默认禁用，只有可信部署显式设 `ELASTIC_AGENT_ENABLE_HARNESS_UPLOAD=1` 才开放；不可信 submitter 只允许声明式 JobSpec。
- **账号→EIP 持久绑定**（`core/account_binding.py` + `core/binding_manager.py`）：稳定身份键是 `account_id`（email 仅快照），持久资源只有 AWS EIP；绑定/租约 journal 原子 fsync 且权限固定为 0600（旧文件加载时收紧）。每个 Job worker 使用独占、持久化的 `AccountLease`（job/slot/generation/instance/worker/state），出现 worker 却没有 instance id 的 journal 直接按损坏 fail closed。`reserve` 原子占用账号并按需分配 EIP，`attach_instance` 拒绝抢占已挂到其他实例的地址（AWS `AllowReassociation=False`），`release` 先在 store 单锁内原子完成调用方 lease/account/job/slot/generation/worker/instance 快照比较 + `RELEASING` intent，intent 后身份字段冻结，再可重试地解绑 EIP→确认终止实例→清理并立即移除 Manager task/Node/WS status→**保留 EIP**；allocator claim 也须精确匹配 claim/owner/account 后才释放。缺失/非 `RELEASED`/任一身份冲突一律保留 Node 与账号 claim。只有显式 `decommission` 才永久释放 EIP。
- **EIP Job 生命周期**（`core/batch_orchestrator.py` + `core/batch_hooks.py`）：普通 Job 仍是 `scale_out(N) → provision → login → run`；`account.binding="eip"` 则反转为 `选择/claim 账号 → reserve EIP lease → 创建临时 EC2 → attach EIP → provision → worker-local fresh login → run → final collect → detach EIP → terminate EC2/root EBS → release lease/account claim`。整单各 shard 的 EIP reservation 并行执行但全部收敛后才创建任何 EC2；任一失败/取消会等待其他云调用结束并回滚成功租约。EIP bootstrap 在任何登录/任务流量前禁用 IPv6，且强制把当前 Manager 正在运行的 `elastic_agent` 包 rsync 到 fresh worker 并从源码启动；此安全路径不接受 `ELASTIC_AGENT_FRAMEWORK_SRC` 降级覆盖，会停旧 runtime、断开现有 socket 并要求新服务重连，不能回退到可能缺 request correlation、精确 email 校验和 warmup 判定的旧 PyPI worker。EIP Job 还拒绝 `run.env` 覆盖 `HOME` 或所选 agent 对应的凭证环境变量（Claude=`CLAUDE_CONFIG_DIR`，Codex=`CODEX_HOME`），显式 config dir 始终由已校验账号槽强制注入。终态先停周期收集，再等待 final collect（默认最多 3 次、总计 300 秒）；失败会标记 Job failed，但仍销毁收费实例。各失败分支也补偿清理临时实例，EIP 留给下个 Job。当前仅支持 AWS，Job region 必须与 Manager/EIP region 相同，`per_worker=1`，不支持原机 `on_exhaust_restart_resume` 换号；`account.ids` 可显式按 worker 选号（数量须等于 fanout），留空则按 `group` 自动分配。
- **AWS EIP/ENI 权限边界**：创建 EIP worker 时给 primary ENI 同步完整 ownership tags；`DisassociateAddress` 同时要求 EIP 与 ENI 都是 `ManagedBy=elastic-agent`，`ReleaseAddress` 仍只授权 EIP，禁止用无条件 `network-interface/*` 解决解绑权限。
- **EIP 不等于登录态**：固定 EIP 只稳定出口公网 IP，不保存 `auth.json`、浏览器 profile 或设备指纹；每台新 EC2 仍由 worker 本地重新登录；生成的 Claude/Codex OAuth 凭证都不回传 Manager。AWS 默认每 Region 仅 5 个 EIP 配额且公网 IPv4 按小时收费（空闲/使用中均计费），扩号前需申请配额并核算长期 EIP 成本。
- **Job preflight、耐久性与容量护栏**：`POST /api/jobs/plan` 纯读校验并返回不含 env value 的执行预览（profile/source/setup/run/fanout/results/warnings），不写 spec、不 claim 账号、不调用云；真实 submit/resubmit 在首个副作用前复用同一 preflight，拒绝跨 Manager region、超 provider 最大容量、账号静态不足、无实例角色的 S3 dataset、实例类型不在 `ELASTIC_AGENT_ALLOWED_INSTANCE_TYPES`（默认仅 provider 默认值）或 `workers*ttl` 超 `ELASTIC_AGENT_MAX_JOB_WORKER_HOURS`（默认 1440）。通过后才将 JobSpec 原子落到 0600 的 `specs/<job_id>.json` 并 fsync，journal 记录 `prepared→launching→running→succeeded|failed|cancelled`；Idempotency-Key 以 spec hash 绑定确定性 job id，重试不会重复创建。重启对中断 Job 先 final collect 再销毁遗留实例。Schema 硬上限：`fanout.workers≤100`、`account.per_worker≤32`、`max_rotations≤100`、`disk_gb≤2048`、run/Job TTL≤30 天、collect interval≤86400 秒；AWS/Aliyun `max_instances` 默认 30。
- **BatchOrchestrator**（`core/batch_orchestrator.py`）：两种资源路径均经 `FleetDriver` Protocol 解耦（真实现 `core/manager_fleet_driver.py`）；`on_worker_exhausted`/`on_worker_exit` 驱动普通模式换号与任务完成，EIP 模式禁止在原实例切账号。
- **增量收集 & 结果落 S3**（`collect.paths` + `interval_seconds`）：只收显式 paths；空列表是 no-op（Batch UI 默认填 `results`，API/SDK 必须显式给），stdout/stderr 只是日志，不会自动成为 S3 对象。`interval_seconds>0` 周期收集，成功/失败/取消都会 awaited final collect。每 shard 独立落 `<prefix>/<job>/workers/shard-00000/<path>/` 并写 `_elastic_agent/collection.json`，避免同名覆盖。AWS worker profile+bucket 时 worker 用实例角色直推；否则 rsync `--safe-links` 回 Manager 后由 `S3ResultUploader` 上传；无桶仅留本地。上传按内容 SHA-256 判变，跳过 symlink/special file；结果 API S3 优先，校验 key/path，score 读取和 tar download 有对象数/总大小/单文件上限且对象在 LIST→GET 间变化时 fail closed。run 退出靠 `proc.returncode` 而非等管道 EOF，保证 final collect 可触发。
- **可靠终态、取消与清理**：`PROCESS_EXIT`/`RUN_EXHAUSTED` 带 UUID event_id，worker 在 0600 fsync outbox 中按产生顺序保存，Manager 处理成功才 ACK；重连先 replay 再 STATUS，退出中 task 也进入快照，避免误判。同一 event_id 的 in-flight replay 只由一个 handler 执行，其余等待：成功后去重 ACK，失败/取消则不标 processed，由等待者接手或主动断开 WS 触发 worker 重连重放。若终态 handler 已在清理中移除/替换并关闭当前 WS，Manager 保留 processed event_id 但不向旧连接强发 ACK；短暂重连的 replay 由新连接 ACK，活动连接的真实发送错误仍向上抛出。取消会关联取消 bootstrap/login，已 dispatch 的命令先 TERM、超时 KILL、等可靠 exit，再 final collect 和销毁；普通 Job 默认也是临时 EC2，终态 force terminate 后从 task/Node registry 移除（不为每个 shard 永久积累 TERMINATED 记录），EIP Job detach/terminate 后保留 EIP。云创建/registry/WS/collect/终止各失败路径均有幂等后台补偿，Manager shutdown/restart 也扫描带 controller/job tag 的遗留实例。
- **Reconciler 终态守卫**：云 API 会在 EC2 已终止后继续返回历史行；registry 不存在时，terminated **无 lease** orphan 直接忽略。带 lease 的 terminated row 只有在 durable lease 对 lease/instance/account/job 精确匹配，且 `RELEASED` 的 EIP detach、instance terminal readback、必要 worker cleanup、`released_at` 全部提交后才视为历史并忽略；缺 lease、读取异常、非终态、任一身份/完成标记不匹配都继续收养并触发 durable cleanup。raw orphan cleanup 前还必须确认该 instance 未被另一 active lease claim；冲突时 quarantine/保留 registry，禁止 detach/terminate。
- **Job secrets**：`run.secret_env` 只存 `aws-secretsmanager://...` / `aws-ssm://...` 引用，实际值在 dispatch 前即时解析且 API/plan 不回显。跨机传输必须 `wss://`；仅 localhost 或显式 `ELASTIC_AGENT_ALLOW_INSECURE_SECRET_ENV=1` 的可信测试网允许 ws，且不安全传输会在读取 AWS secret 前被拒绝。
- **实盘装配**（`core/batch_hooks.py` `wire_batch`，`Manager.batch` 默认走它）：provision=等实例 running→`BootstrapHandler` SSH 跑 `compile_bootstrap_steps`→等 WS 连上；login=`AccountAllocator`（内存分配账号身份，换号时把旧号 retire 不再选；**account 在 job 终态即释放**——`_maybe_finish` 无条件 `release_worker` 回池，且 bring-up 全失败/rotation 耗尽 decline 两条路也补调 `_maybe_finish`，`release_worker` 幂等。若普通 worker 登录取消后 60 秒内未确认清理、或登录中断线，账号进入进程内 quarantine，即使 claim 释放也不再分配，直到外部确认清理后显式清除；EIP Job 由销毁临时实例提供隔离，不走此 quarantine）+ `LoginCoordinator`（发 `ACCOUNT_LOGIN`、经 event_bus 等 `ACCOUNT_LOGIN_RESULT`；Codex 人工验证码以 `ACCOUNT_LOGIN_OTP_REQUIRED` challenge 关联 request/account/worker，并把 API 提交的 `ACCOUNT_LOGIN_OTP` 精确转发；总等待 3600 秒，断线立即失败，超时/调用方取消会发关联 cancel 并等待清理 ACK）；并把 worker 的 `RUN_EXHAUSTED`/`PROCESS_EXIT` 经 event_bus 路由回 orchestrator（`handle_exhausted`/`handle_exit`）。manager_url 取 `ELASTIC_AGENT_MANAGER_URL` 或 `config.server`；ssh key 按 provider.type 取。`configure_batch(...)` 可覆盖 hooks。**换号竞态**：中断退出的旧 run 的 `PROCESS_EXIT` 带旧 task_id，`on_worker_exit` 用 task_id 匹配当前 run 丢弃陈旧退出（ROTATING 相位守卫挡不住重派已转 RUNNING 的竞态）。
- **Mode B 换号（策略 a，非 EIP 模式）**：`ExecuteMessage.watch_exhaustion` 开启时 worker `_read_stream` 用 `core/rate_limit.py` 扫 stdout/stderr，撞限流即 `_signal_exhaustion`（发 `RunExhaustedMessage` + SIGINT）；Manager 侧 `on_worker_exhausted` 换号 + 用 `rotation.resume_args` `--resume` 重启，`max_rotations` 上限；打断退出（ROTATING）不算失败。
- **每机多号（非 EIP 模式）**（`account.per_worker>1`）：GenericJobHarness 按 per_worker 产出多个独立 config_dir 槽（`<base>-slot-N`）；`_bring_up` 逐槽 `driver.login`（`AccountAllocator` 每次给不同 agent 类型的账号，已分配的号在本 job 内不再发出→耗尽号不会被重选）。换号优先切到**下一个预登录槽**（`active_slot++`，免登录、快），本地池耗尽才 `-rot-N` 现登新号。`WorkerRun.config_dirs/account_ids/account_emails` + `active_slot` 跟踪；`ctx.config_dir` 随活跃槽按 `agent_type` 注入 `CLAUDE_CONFIG_DIR` 或 `CODEX_HOME`。
- **Codex worker bootstrap**：`agent_install_step(agent_type="codex")` 安装固定版本 `@openai/codex@0.144.6`；worker-local 登录依赖系统 Google Chrome、Xvfb 和 Playwright。Codex Job 跳过 Claude 专用的 claude-pty 安装/健康检查，并强制下发当前 Manager worker 源码，避免旧协议把 Codex 请求误作 Claude 登录；source runtime 以 `ELASTIC_AGENT_AGENT_TYPE=codex` 检查/上报 Codex CLI 健康度，不要求机器另装 Claude。
- **Manager 生产启动边界**：AWS 生产入口固定为 `deploy/aws_manager.py` + 版本化 `deploy/aws/elastic-agent-manager.service`。秘密与非秘密云配置分别由必需的 `/etc/elastic-agent-manager.env`、`/etc/elastic-agent-manager.aws.env` 注入；unit 禁用 IMDSv1、屏蔽 env/shared-file/web-identity/container 等替代凭证源，Launcher 用 STS 精确校验专用 Manager role。release/HOME 只读，仅生产 state 目录可写（含 webhook dead-letter），并启用 `NoNewPrivileges`、`PrivateTmp` 和 kernel/control-group 防护；state 必须预建，health `ExecStartPost` 才完成 systemd readiness。AMI/role provenance 失败时在 Manager 生命周期启动前 fail closed；`TimeoutStopSec=1200` 为 final collect + EIP detach/terminate 留足收敛窗口。
- **Docker 沙箱支持**（`setup.needs_docker`）：run 命令用 Docker（如 ai4sci-bench `--sandbox os`）时置 true → `compile_bootstrap_steps` 在 runtime 部署**前**插 `docker_install_step`（装 docker.io + `usermod -aG docker <ssh_user>` + `enable --now docker`）。**必须在 runtime systemd 起来前**跑：systemd 起服务时才解析 User 的补充组，usermod 先跑 runtime(及其子 run 命令)才拿得到 docker socket。`run_as` 经 `compile_bootstrap_steps(..., run_as=ssh_user)` 从 `batch_hooks` 传入。
- **Golden worker AMI 快路**：`scripts/build_golden_ami.sh` 从固定 Canonical Ubuntu AMI 构建加密、无凭证的 standard+Docker union image（Docker 默认停用），写 `/etc/elastic-agent/image-manifest.json` schema 1 并安装 `/usr/local/bin/elastic-agent-image-verify`。system/agent/login/Docker/runtime Python/固定 commit claude-pty 只有在 manifest 与实际 dpkg/command/CLI/import/direct_url commit **全部精确匹配**时跳过联网安装；marker 缺失/损坏、漂移或验证异常都完整回退既有 apt/npm/pip。不得 bake OAuth/auth.json、浏览器 profile、AWS 凭证、Job 代码/数据、Manager URL/token、当前框架源码或运行中的 worker unit；详见 `docs/operations/golden-worker-ami.md`。
- **AWS 私网管理面**：Manager 发起的 bootstrap、源码分发、worker-local 登录、日志读取与结果收集统一经 `core/network.py::worker_management_host` 选址；AWS 必须 private_ip-first（EIP 只作 worker 稳定出站身份），其他 provider 保持 public-first，地址缺失时才回落。这样 Worker SG 可仅允许 Manager SG 的 22，不把 SSH 暴露公网。
- **AWS 生产入口与最小权限**：生产 Manager 使用版本控制的 `deploy/aws_manager.py`，部署参数只读 0600 EnvironmentFile，API/Git 秘密无 CLI fallback；启动前 fail-closed 校验 AMI provenance、加密、架构、ENA、IMDSv2 与 golden tags。`deploy/aws/` 保存专用 Manager inline policy、worker 结果桶 write-only policy 和 cutover/rollback runbook；不得收紧多机共用的旧 `Manager` role，必须给 Elastic Manager 单独 instance profile。per-job 新实例类型或 S3 dataset prefix 必须先显式扩白名单并重跑 Analyzer/simulator。
- **S3 数据集分发**（`setup.s3_datasets: [{uri,dest}]`）：**worker 直连 S3 拉**（不再经 Manager 中转）。前提 `fanout` provider 配了 `worker_instance_profile`（AWS：`AWSProviderConfig.worker_instance_profile`，`create_instance` 给 `run_instances` 加 `IamInstanceProfile`；Manager 角色需 `iam:PassRole`）→ worker EC2 挂 IAM 角色（`elastic-agent-worker`/S3 权限）。provision hook 在 run 前：装 awscli（缺才装）→ 逐个 `aws s3 sync <uri>/ <dest>`（尾 `/`=前缀递归）或 `aws s3 cp`（单对象）在 worker 上跑。**GitHub 代码分发不变**（仍 manager_rsync，token 只在 Manager）。旧的 `code_sync.py` `_download_s3`/`ManagerCodeSync.stage_s3`（Manager 下载→rsync）保留为工具函数但 provision 不再用。凭证仍不下发（worker 用实例角色，非静态 key）。
- **Worker 日志/管理 API**（`api/routes/nodes.py`）：`GET /api/nodes/{id}/logs?lines=&unit=ea-runtime`（Manager SSH 到 worker 跑 `journalctl -u <unit>`，含 bootstrap/登号/run 转发的 stdout，免手动 SSH 排障）；既有 `/api/nodes`（列表/详情/status）、`/api/scale-out`、`/api/scale-in`、`/nodes/{id}/drain`、`DELETE /nodes/{id}`（终止实例）。
- **前端与绑定管理 API**（`api/routes/`）：`/api/accounts` 管 Claude/Codex 账号池；OpenAI password 与邮箱查询 token 写入 Manager 的 0600 文件，API 只返回存在标志；空秘密默认保留旧值，显式 clear 才清除。API key 仅接受 Bearer/X-API-Key header，不接受 query；UI 只存 sessionStorage 并清理旧 query。OTP challenge 严格关联 Manager/worker，验证码不持久化。绑定 decommission 双确认且是唯一释放 EIP 入口，active claim/lease 时拒绝；账号 CRUD/ensure 与 Job claim 串行。Batch UI 在 AWS Manager 上默认选择 EIP 模式并展示持久 EIP；显式 `binding=none` 仍受支持，但 plan 会警告它绕过账号 EIP、使用临时公网出口。`/api/jobs` 提供 plan/submit/idempotency/cancel/results，Harness 上传默认关闭。

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
