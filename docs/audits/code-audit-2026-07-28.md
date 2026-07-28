# Elastic-Agent 代码审计记录（2026-07-28）

## 1. 审计基线

- 审计 Commit：`8dc4228`（审计期间从 `979d765` 增量复核至最新
  `origin/main`，包含 per-worker S3 dataset 改动）
- 审计分支：`task-code-audit`
- 审计方式：只读代码审查、完整单元/集成测试、静态检查、故障注入和本地最小复现
- 未执行：真实云资源创建、真实账号登录、真实 Provider API 调用、生产服务重启或配置修改

自动检查结果：

| 检查 | 结果 |
| --- | --- |
| `uv run pytest -q` | `2356 passed, 12 skipped, 87 warnings` |
| 近期 Agent API、Worker、Batch、UI 专项测试及覆盖率 | `375 passed`，所选模块语句覆盖率约 `77%` |
| 最新 S3 分片数据集增量专项测试 | `303 passed` |
| `uv run python -m compileall -q src` | 通过 |
| `uv lock --upgrade-package claude-pty --dry-run` | 无 lockfile 变化 |
| `uv run --extra dev ruff check .` | 失败，共 `341` 项 |

严重度定义：

- **高**：可导致秘密泄漏、越权读取、Worker/Job 长时间不收敛、持续云计费、Manager/Worker 资源耗尽，或核心公开功能实际不可用。
- **中**：会造成错误路由、错误记账、幂等性破坏、结果不一致、可恢复的资源泄漏，或明确的管理功能缺口。
- **低**：影响受限的输入兼容性或文档/API 契约不一致，不扩大权限且有直接绕行方式。

本记录只列入已通过代码路径或最小复现确认的问题。纯推测和已经被反证的问题未列入。

## 2. 问题总览

| ID | 严重度 | 状态 | 摘要 |
| --- | --- | --- | --- |
| EA-AUD-001 | 高 | Open | LocalBackend 文件 API 可路径穿越，读取 Manager 本地文件 |
| EA-AUD-002 | 高 | Open | Claude OAuth 凭据会以 `0644` 写入或恢复 |
| EA-AUD-003 | 高 | Open | Claude 登录把 mailbox token、验证码和 OAuth code/state 写入 Worker journal |
| EA-AUD-004 | 高 | Open | Claude 登录取消/异常不事务回滚，仍可能错误确认清理完成 |
| EA-AUD-005 | 高 | Open | 可靠终态事件持久化失败被吞掉，Job 可永久停在运行态 |
| EA-AUD-006 | 高 | Open | Agent API tombstone 写失败阻断 PROCESS_EXIT/RUN_EXHAUSTED 收敛 |
| EA-AUD-007 | 高 | Open | stdout/stderr 单行超过 64 KiB 后停止排水，任务可假死或被误杀 |
| EA-AUD-008 | 高 | Open | Agent API Key 仍按 OAuth 账号全局独占，不能多 Worker 共享 |
| EA-AUD-009 | 高 | Open | 结果 score 解析可被 Job 输出放大为 Manager 内存/I/O DoS |
| EA-AUD-010 | 高 | Open | S3 流式下载排队取消会泄漏 pipe FD，并使用无界 executor 队列 |
| EA-AUD-011 | 高 | Open | 普通多 shard Job 的单 Worker 失败后不会立即销毁对应实例 |
| EA-AUD-012 | 高 | Open | Batch 接受 `manager_distribute`，实际仍执行 Worker 本地登录 |
| EA-AUD-013 | 高 | Open | Worker 日志发送队列无界，断线/慢连接时可 OOM |
| EA-AUD-014 | 中 | Open | 预构建结果包断连后遗留临时文件，且无全局磁盘预算 |
| EA-AUD-015 | 中 | Open | 幂等重放先执行当前 preflight，可能拒绝已经成功的同 Key 重试 |
| EA-AUD-016 | 中 | Open | 本地结果下载的“快照一致性”存在 LIST→OPEN TOCTOU |
| EA-AUD-017 | 中 | Open | 前端在 non-EIP 模式下不能指定具体账号/API Key |
| EA-AUD-018 | 中 | Open | Claude 忽略 `account.login_timeout_seconds`，固定使用 480 秒 |
| EA-AUD-019 | 中 | Open | PTY autonomous result 会覆盖前台 session 并重复计费 |
| EA-AUD-020 | 中 | Open | 同名 Harness 上传失败会删除旧有效插件，并可改写历史 Job 代码 |
| EA-AUD-021 | 中 | Open | Agent API 账号删除端点无条件返回 409 |
| EA-AUD-022 | 中 | Open | OAuth 账号字段和请求体缺少硬大小/字符边界 |
| EA-AUD-023 | 高 | Open | 空 hostname 会把单对象 S3 dataset 退化为整桶 prefix 同步 |
| EA-AUD-024 | 中 | Open | dataset 丢失 Worker context 时静默回退 shard 0 |
| EA-AUD-025 | 中 | Open | dataset 目标路径允许空格，但建父目录命令会错误拆词 |
| EA-AUD-026 | 低 | Open | dataset URI 不兼容模板引擎支持的空白占位符语法 |

## 3. 高严重度问题

### EA-AUD-001 — LocalBackend 文件 API 可路径穿越

**位置**

- `src/elastic_agent/api/routes/files.py:98-115`
- `src/elastic_agent/worker/file_sync.py:297-321`

**问题**

`GET /api/external/files/{task_id}/{path:path}` 直接构造
`tasks/{task_id}/{path}`。`LocalBackend` 再使用
`self._base_dir / oss_key` 访问文件，二者都没有拒绝 `..`，也没有验证最终路径仍位于
`base_dir` 下。

**已确认复现**

在 LocalBackend 根目录建立 `tasks/x/`，并在根目录外建立 `secret.txt` 后：

```text
read_file("tasks/x/../../../secret.txt") -> 根目录外 secret.txt 的内容
```

使用合法 Bearer 调用 percent-encoded `../` 的 HTTP 路径也可得到 `200`。

**影响**

启用 LocalBackend 的 Manager 上，持有 API 凭据的调用方可读取 Manager 运行用户可读的任意文件，包括同用户环境文件、状态文件或 `/proc/self/*`。LocalBackend 的
`upload_file`/`upload_bytes` 也有相同的路径越界原语，内部错误调用可越界写入。

**建议**

1. API 层同时校验 `task_id` 和 `path`，拒绝绝对路径、反斜杠、NUL 和任何 `..` 分段。
2. LocalBackend 每个读写方法都应独立执行 `resolve`/`relative_to(base)` containment 校验，不能只依赖路由。
3. 增加 percent-encoded traversal、符号链接祖先和不存在目标路径的回归测试。

### EA-AUD-002 — Claude OAuth 凭据权限不安全

**位置**

- `src/elastic_agent/core/claude_oauth.py:309-318`
- `src/elastic_agent/worker/login/auto_login.py:764-781`
- `src/elastic_agent/worker/login/auto_login.py:793-816`
- `src/elastic_agent/core/bootstrap_steps.py:246-257`

**问题**

`write_credentials()` 使用普通 `mkdir`、固定名 `.credentials.tmp`、
`write_text` 和 `replace`，未设置目录/文件权限，也未 fsync。Claude 登录失败恢复旧文件时只保存
bytes，不保存原 mode，并通过 `write_bytes` 重新创建。

Worker systemd unit 未配置 `UMask=0077`，因此行为取决于进程 umask。

**已确认复现**

在 `umask 022` 下：

```text
config_dir mode          = 0755
.credentials.json mode   = 0644
```

另一个复现从 mode `0600` 的旧 `.credentials.json` 开始，强制邮件服务失败后，恢复文件变为
`0644`（当前审计环境 umask 为 `0002` 时实测为 `0664`）。

**影响**

Claude access token 和 refresh token 可被同机其他 OS 用户读取。固定临时文件名还引入并发写入和预置 symlink 风险。

**建议**

统一使用已有的 `secure_state_directory`/`atomic_write_private`：

- 目录固定 `0700`；
- 文件固定 `0600`；
- 唯一临时文件、`O_NOFOLLOW`/inode 检查；
- 文件和父目录 fsync；
- 回滚同时恢复内容和 mode。

### EA-AUD-003 — Claude 登录秘密进入 ea-runtime journal

**位置**

- `src/elastic_agent/worker/login/auto_login.py:49`
- `src/elastic_agent/worker/login/auto_login.py:144-205`
- `src/elastic_agent/worker/login/cdp_login.py:179-181`
- `src/elastic_agent/worker/login/cdp_login.py:233,290,336,374,389,412`
- `src/elastic_agent/api/routes/nodes.py:180-209`

**问题**

Claude 登录模块启用 INFO 日志，但未像 Codex 登录一样永久提高
`httpx`/`httpcore` 的日志级别。mailbox 查询 token 被放在 GET query 中，因此 httpx 会记录完整 URL。

主 CDP 登录路径还通过 `print()` 输出：

- magic-link 后的 URL 和页面正文（可能含验证码）；
- authorize API 返回体；
- network response body；
- navigation/current URL。

authorize 响应和 URL 可直接包含 OAuth `code` 与 `state`。

**已确认复现**

- MockTransport 调用 mailbox 轮询后，日志中可观察到完整 `?token=<canary>`。
- 构造带 `redirect_uri?...code=<canary>&state=<canary>` 的 CDP authorize 返回值后，stdout 同时出现 code 和 state。

该登录在 WorkerRuntime 进程内运行，stdout 进入 `ea-runtime` systemd journal；节点日志 API 又会读取该 journal。

**影响**

长效 mailbox token、一次性验证码和 OAuth 授权材料会进入日志采集面，并在实例销毁前可由日志管理员读取。

**建议**

1. 在任何 HTTP 客户端创建前，与 Codex 一样永久把 `httpx`/`httpcore` 提升到 WARNING。
2. 删除 CDP 中所有页面正文、响应 body 和完整 URL 输出；只记录固定状态枚举和长度。
3. 增加 canary 测试，断言日志、异常文本和节点日志响应中均不出现 token/code/state。

### EA-AUD-004 — Claude 登录取消并非事务性清理

**位置**

- `src/elastic_agent/worker/login/auto_login.py:774-816`
- `src/elastic_agent/worker/login/cdp_login.py:185-187`
- `src/elastic_agent/worker/login/cdp_login.py:444-445`
- `src/elastic_agent/worker/runtime.py:1879-1978`
- `src/elastic_agent/worker/runtime.py:2041-2073`

**问题**

`perform_login()` 在登录开始时即 unlink 旧 `.claude.json` 和
`.credentials.json`，但只在两个显式失败分支恢复。`CancelledError`、意外异常以及 Runtime 后续的
email identity/warmup 失败均不完整回滚。

CDP 路径通过同步 `Popen` 启动 `claude auth login`，最终清理只 kill/wait Chrome，不终止或等待
Claude CLI；Chrome 也不是独立 process group，且在进入主 `try/finally` 前已有可取消 await。

Runtime 只要登录 Task 已进入 cancelled/done 状态，就可能返回
`ACCOUNT_LOGIN_CANCELLED(cleanup_complete=True)`，并没有验证凭据和所有子进程已恢复/退出。

**影响**

- 原本可用的凭据被删除或替换；
- 被取消的 CLI 仍可能随后写入凭据；
- Manager 误以为隔离完成，释放账号 claim 或不 quarantine；
- 后续任务可能使用错误账号、半写凭据或残留浏览器进程。

**建议**

在 Runtime 层为整个 Claude 登录建立事务：

1. 以唯一私有备份保存所有会修改的凭据和 mode；
2. identity 校验和 warmup 成功后才 commit；
3. 所有异常和取消都在 shielded finally 中 rollback；
4. Chrome/CLI/Xvfb 均使用已跟踪的 process group，确认完全退出后才发送 cleanup ACK。

### EA-AUD-005 — 可靠终态事件持久化失败被吞掉

**位置**

- `src/elastic_agent/worker/runtime.py:2287-2338`
- `src/elastic_agent/core/batch_hooks.py:2080-2086`
- `src/elastic_agent/core/batch_orchestrator.py:689-719`

**问题**

`_send_event()` 先把 `PROCESS_EXIT`/`RUN_EXHAUSTED` 放入内存 map，再持久化
outbox。若持久化因 ENOSPC、EROFS 或 I/O 错误失败，外层 `except Exception` 只记录日志：

- 不把事件放入 send queue；
- 不重试；
- 不抛出给调用方；
- 内存 map 仍让 STATUS 报告 pending exit。

Manager 将 pending exit 与 active process 合并，因此不会触发“任务已丢失”的 fallback。

**已确认复现**

将 `_persist_reliable_events` 注入 `OSError(ENOSPC)` 后：

```text
event remains in memory = true
send queue size         = 0
pending task ids        = ["canary"]
```

**影响**

连接保持正常时不会发生 outbox replay，Job 可永久停在 RUNNING，final collect 和实例销毁不执行；Worker 重启后未 fsync 的事件又会彻底丢失。

**建议**

持久化失败必须成为显式的 runtime-fatal/reconnect 条件，或进入独立的有界重试状态；绝不能把未 durable、未 queue 的事件继续广告成可正常交付。增加 ENOSPC/EROFS/restart 故障注入测试。

### EA-AUD-006 — Agent API tombstone 失败阻断生命周期

**位置**

- `src/elastic_agent/core/batch_hooks.py:1989-2078`
- `src/elastic_agent/manager/manager.py:2844-2853`

**问题**

处理 API auth/quota failure 时，`_on_exhausted` 和 `_on_exit` 在调用
`defer_exhausted`、日志归档和 `orch.handle_exit` 之前，先 await
`mark_runtime_unavailable`/`mark_runtime_quota_unavailable`。

这些本地状态写入没有 `try/finally` 隔离。磁盘满、只读或 store 损坏时，EventBus subscriber 抛错，
Manager 不 ACK 可靠事件；每次 replay 都在同一点失败。

**影响**

一个用于“暂时禁用 Key”的辅助写入错误，会阻断更重要的 Job 终态、结果收集、账号释放和 EC2 销毁。

**建议**

tombstone 更新应 fail-closed 地阻止该 Key 再分配，但生命周期处理必须放在
`finally` 中继续。失败状态可写入独立内存 quarantine/告警，不能阻止 terminal ACK。

### EA-AUD-007 — 超长单行输出停止 pipe 排水

**位置**

- `src/elastic_agent/worker/runtime.py:684-689`
- `src/elastic_agent/worker/runtime.py:985-1015`

**问题**

`create_subprocess_exec` 使用 asyncio StreamReader 默认 64 KiB limit，
`_read_stream` 使用 `readline()`。单行超过 limit 会抛
`ValueError`/`LimitOverrunError`，随后被宽泛异常处理静默 break；该 stdout/stderr pipe 从此无人读取。

**已确认复现**

- 输出一行 100,000 bytes：进程 exit 0，但对应 LOG 数为 0。
- 输出一行 1,000,000 bytes：writer 被 pipe backpressure 卡住，2 秒 timeout 后被 SIGINT，最终 exit `-2`。

Claude/Codex NDJSON 的大 tool/result/trace 单帧可以自然超过 64 KiB。

**影响**

正常任务可能无声丢 trace、假死到最长 30 天 timeout，或被错误标为超时失败并持续占用实例。

**建议**

设置明确的有界 frame limit，并在超限时持续分块 drain、记录截断标记；任何解析/大小错误都不能停止 pipe 排水。终端 LOG API 的 64 KiB 行上限应只影响存储/展示，不应影响子进程读取。

### EA-AUD-008 — Agent API Key 不能多 Worker 共享

**位置**

- `src/elastic_agent/core/batch_hooks.py:92-94`
- `src/elastic_agent/core/batch_hooks.py:317-339`
- `src/elastic_agent/api/routes/jobs.py:527-552`
- `src/elastic_agent/core/job_spec.py:568-579`
- `src/elastic_agent/core/job_spec.py:746-756`

**问题**

Allocator 对 OAuth 和 Agent API 统一使用
`_claim_by_account: account_id -> one claim_id`。候选选择统一排除已 claim 的 ID，第二个 Worker 无法取得同一 Key。

preflight 又把每把 API Key 只计作一个 slot；显式 `account.ids` 会先去重，再要求账号数恰好等于
`workers * per_worker`。

**已确认复现**

- 一个可用 Apex/CloudRouter Key，顺序 reserve 给 `w1` 成功，给 `w2` 返回 `None`。
- `fanout.workers=2` 且池内只有一把兼容 Key 时，plan/submit 返回 422。
- 显式填写同一 Key 两次会被 schema 的 unique-account 校验拒绝。

**影响**

当前行为与“OAuth 独占、API Key 可多 Worker 共用”的资源模型不一致；Apex 返回的 concurrency 也未被用作可共享容量。

**建议**

仅允许 `binding=none && auth_kind=agent_api` 多 claim：

- OAuth 和所有 EIP lease 继续独占；
- account → claim 改为集合/refcount；
- mutation/delete 在存在任意引用时继续拒绝；
- preflight 识别 API 可复用容量；
- 显式重复 ID 只允许已证明为 Agent API 的 non-EIP 配置；
- 增加同 Job、多 Job并发共享，以及任一 release 不误清其他 claim 的测试。

### EA-AUD-009 — score 解析可放大为 Manager DoS

**位置**

- `src/elastic_agent/api/routes/jobs.py:55-58`
- `src/elastic_agent/api/routes/jobs.py:1096-1187`
- `src/elastic_agent/api/routes/jobs.py:1349-1392`
- `src/elastic_agent/api/routes/ui.py:2624-2660`
- `src/elastic_agent/api/routes/ui.py:2914-2927`

**问题**

本地 `_results_for` 最多遍历 100,000 个文件，并可对每个候选读取/解析 2 MB JSON，却没有应用已有的
`RESULT_SCORE_MAX_ATTEMPTS=500`，也没有 aggregate byte budget。

S3 路径虽限制 500 次，但 `task_id`、`prompt_level`、`status` 和
`final_score` 没有标量类型或长度上限。每个 2 MB JSON 都可把大字符串加入 `scores`。

UI 对结果接口自动轮询，使问题无需用户手动重复触发。

**影响**

- 本地理论读取量约 200 GB；
- S3 响应可接近 1 GB；
- Job 控制的结果文件可耗尽 Manager 内存、CPU 和 I/O。

**建议**

本地和 S3 使用同一 attempts 上限、aggregate read/serialized-response budget；只接受类型正确且长度受限的标量，限制 score 条目数，并让 UI 对超限结果停止高频轮询。

### EA-AUD-010 — S3 流式下载排队取消泄漏 FD

**位置**

- `src/elastic_agent/api/routes/jobs.py:62-70`
- `src/elastic_agent/api/routes/jobs.py:1715-1764`

**问题**

流式下载使用 4 线程专用 executor，但没有 admission semaphore。每个请求在获得线程前先
`os.pipe()`，随后把裸 `write_fd` 放进无界 executor 队列。

若四个 producer 被慢/挂 S3 占满，后续请求排队后取消时，generator finally 只取消 control 和关闭
read transport；queued callable 尚未启动，没有对象负责关闭 write FD。

**已确认复现**

占满四个 executor worker 后，创建并取消 20 个流式 generator：

```text
before_fds        = 8
while_queued_fds  = 28
after release     = 8
```

每个排队取消请求泄漏一个 FD，直到线程池最终执行对应 callable。

**影响**

慢 S3 或恶意并发下载可同时造成 FD exhaustion、executor work queue 增长和大量迟到的无用任务。

**建议**

在 `os.pipe()` 前获取有界 async semaphore/admission；取消等待时不创建任何 FD。创建后明确转移 FD ownership，并为 queued-before-start cancellation 增加回归测试和请求上限。

### EA-AUD-011 — 单 Worker 失败后普通实例不立即销毁

**位置**

- `src/elastic_agent/core/batch_orchestrator.py:608-631`
- `src/elastic_agent/core/batch_orchestrator.py:1965-1984`
- `src/elastic_agent/core/batch_orchestrator.py:2328-2365`

**问题**

普通 non-EIP 多 shard Job 中，`cancel_worker()` 只把该 run 标为 FAILED 并 final collect。
`_maybe_finish()` 在其他 shard 未终态时直接返回；普通 `scale_in(list(job.runs))` 只在整单全部终态后执行。

`cancel_worker()` 对 non-EIP 还会直接返回 `True`，即使该 Worker 尚未被单独终止。

**影响**

一个永久断线、单独取消或提前失败的 Worker，其 EC2、残留进程和已委托凭据可继续存在并计费，直到最长运行 shard 结束或 Job TTL（最长 30 天）。

**建议**

为普通 Worker 实现 exact per-run stop → final collect → terminate-confirm → claim release；若当前 driver 无法安全逐个清理，则单 shard 永久丢失应升级为整 Job cancel。

### EA-AUD-012 — `manager_distribute` 名义支持、实际未实现

**位置**

- `src/elastic_agent/api/routes/ui.py:889-895`
- `src/elastic_agent/core/job_spec.py:509-517`
- `src/elastic_agent/harness/generic.py:228-230`
- `src/elastic_agent/core/batch_orchestrator.py:1462-1538`
- `src/elastic_agent/core/batch_hooks.py:1451-1570`
- `src/elastic_agent/worker/runtime.py:1819-1905`

**问题**

UI 和 Schema 允许 Claude 使用 `account.mode="manager_distribute"`，注释承诺下发
`CREDENTIAL_LOGIN`。实际生产 Batch login hook 不区分 mode，仍通过 `LoginCoordinator` 发
`ACCOUNT_LOGIN`，Worker 执行本地 Chrome/CDP OAuth。

真正的 `CREDENTIAL_LOGIN` handler 存在，但生产 Batch 没有调用路径。与此同时 bootstrap 只在
`worker_local_login` 安装浏览器登录依赖。

**影响**

用户选择“Manager 下发凭据”后通常在 logging_in 阶段失败；即使镜像偶然带齐依赖，也会悄悄执行与声明不同的 Worker 本地登录。

**建议**

短期在 Schema/preflight/UI 禁用该模式；若保留，则实现独立的凭据来源、`CREDENTIAL_LOGIN` coordinator、传输保护、轮换和测试，不能复用 ACCOUNT_LOGIN。

### EA-AUD-013 — Worker LOG send queue 无界

**位置**

- `src/elastic_agent/worker/runtime.py:257`
- `src/elastic_agent/worker/runtime.py:395-413`
- `src/elastic_agent/worker/runtime.py:1010-1015`
- `src/elastic_agent/worker/runtime.py:2325-2333`

**问题**

`_send_queue` 是无 maxsize 的 `asyncio.Queue`。每一行 stdout/stderr 都序列化后 `put`；Worker
断线重连期间没有 sender，慢连接时 sender 也可能长期落后，但生产者完全没有条目数或字节级背压。

**影响**

高输出 Job 在网络故障时会把完整 LOG 帧持续堆在内存中，最终 OOM Worker runtime；终态、收集和实例回收也随之受影响。

**建议**

使用有界、按总 bytes 计量的队列。普通 LOG 可在本地日志作为 authoritative copy 后截断/合并/丢弃并计数；终态事件继续走独立 durable outbox，不能与可丢日志共用可靠性策略。

### EA-AUD-023 — 空 hostname 会把单对象 S3 dataset 放大为整桶同步

**位置**

- `src/elastic_agent/core/job_spec.py:708-713`
- `src/elastic_agent/core/job_spec.py:831-848`
- `src/elastic_agent/core/batch_orchestrator.py:1081-1101`
- `src/elastic_agent/core/batch_hooks.py:1432-1440`

**问题**

普通 fanout 的 hostname 查询失败时，Orchestrator 只记录 warning，合法地保留空
`WorkerContext.hostname` 并继续 provision。dataset 渲染也允许模板结果为空，下载逻辑又只依据最终
URI 是否以 `/` 结尾，在 `cp` 与 recursive `sync` 之间切换。

因此本意为单对象的：

```text
s3://private-data/{{hostname}}
```

会渲染为 `s3://private-data/`，随后执行整桶
`aws s3 sync`。JobSpec 的提交期校验也使用一个 hostname 为空的 synthetic context，却仍接受该
结果。

**已确认复现**

```text
render_s3_datasets(WorkerContext(hostname=""))[0].uri
    -> s3://private-data/
```

**影响**

一次临时 hostname 查询故障可让全部 Worker 下载同一完整 bucket/prefix，造成流量、S3 请求、磁盘和
启动时间放大；若实例角色本来可读多个数据分片，还会把超出该 shard 需要的数据落到 Worker。

**建议**

dataset 使用到的上下文变量不得解析为空。特别是 URI key 从非空对象退化为 bucket/prefix，或渲染后
改变 `cp`/`sync` 类型时必须 fail closed。增加 hostname lookup failure 与空模板值的回归测试。

## 4. 中严重度问题

### EA-AUD-014 — 预构建结果包存在临时文件和磁盘预算泄漏

**位置**

- `src/elastic_agent/api/routes/jobs.py:55-58`
- `src/elastic_agent/api/routes/jobs.py:1511-1524`
- `src/elastic_agent/api/routes/jobs.py:1818-1882`

**问题**

strict S3 下载和 local fallback 先在 `/tmp/elastic-agent-results-*.tar.gz`
构建完整包，再通过 `FileResponse(background=unlink)` 清理。Starlette 在 ASGI body send
中途取消/抛错时不会执行 background task。

**已确认复现**

让 ASGI `send` 在第一个非空 body chunk 抛 `CancelledError` 后：

```text
exists_after_cancel = true
```

单请求允许最多 10 GiB；`asyncio.to_thread(_build_*)` 又没有全局并发或 spool byte budget。多个客户端/多个 Job 可并发占满常见 40 GiB Manager 根盘。

**建议**

用带 `finally unlink` 的自定义 response/stream；启动时清理带年龄阈值的 stale temp；增加全局构建 semaphore、已预留/实际 spool bytes 预算，超限返回 429/507。最好让 local 路径也使用有界流式打包。

### EA-AUD-015 — 幂等重放受当前 preflight 阻断

**位置**

- `src/elastic_agent/api/routes/jobs.py:664-727`
- `src/elastic_agent/api/routes/ui.py:2009-2019`

**问题**

`submit_job()` 在解析 Idempotency-Key、查 live Job 或 durable journal 之前，先执行
`_preflight_job()`。

首次提交已成功但响应丢失后，如果账号被禁用、模型/usage 改变、实例 allowlist 收紧或当前容量不足，同 Key + 同 spec 不会返回原 Job，而会先得到 422。

UI 的 Key 又只保存在页面内存，刷新后丢失。

**影响**

违反“同 Key 重试不重复创建”的契约，并诱导调用方换 Key 再提交，产生重复收费 Job。

**建议**

在 submit lock 内先规范化 Key、定位 existing job/journal 并精确比较 spec；已 launch/terminal 的 exact replay 应直接返回。只有新提交和仍为 `prepared`、尚未产生副作用的恢复才执行适当 preflight。UI pending Key 存入 sessionStorage。

### EA-AUD-016 — 本地结果快照存在 TOCTOU

**位置**

- `src/elastic_agent/api/routes/jobs.py:1096-1160`
- `src/elastic_agent/api/routes/jobs.py:1767-1811`

**问题**

`_local_regular_files()` 枚举时保存 lstat，但 `_build_local_archive()` 循环丢弃该 stat，重新打开同路径后只确认“仍是 regular file”。score 读取也未将 fd fstat 与 listing stat 比较。

文件在 LIST→OPEN 间被原子 replace 后，归档会接受新 inode，而不是按注释 fail closed。

**影响**

周期 rsync/collect 与下载并发时可返回混合代次结果，score 列表和下载包也可能不是同一 snapshot。

**建议**

比较 open fd 的 `st_dev/st_ino/st_size/st_mtime_ns` 与捕获值，读取后验证 exact EOF；更稳妥的是按 collection generation 加快照锁或内容寻址 manifest。

### EA-AUD-017 — non-EIP 前端不能精确选择账号

**位置**

- `src/elastic_agent/api/routes/ui.py:913-916`
- `src/elastic_agent/api/routes/ui.py:1791-1801`
- `src/elastic_agent/api/routes/ui.py:1895-1905`
- `src/elastic_agent/core/job_spec.py:746-757`
- `src/elastic_agent/core/batch_orchestrator.py:1562-1583`

**问题**

UI 仅在 `binding=eip` 时启用账号 picker，`buildJobSpec()` 也只在 EIP 时发送
`account.ids`；其他模式无条件发送空数组。

Backend 明确支持 non-EIP 的精确 worker/slot 映射。

**影响**

普通临时出口 Job 无法锁定具体 CloudRouter、Apex 或 OAuth 账号。同组多 Key 调试、配额隔离和 Provider 选择可能跑到另一身份。

**建议**

non-EIP 也启用 picker；显式 ids 数应匹配 `workers * per_worker`，EIP 保持匹配
`workers`。与 EA-AUD-008 一起设计 API Key 重复 ID 规则。

### EA-AUD-018 — Claude 登录超时配置被忽略

**位置**

- `src/elastic_agent/core/job_spec.py:525-532`
- `src/elastic_agent/core/batch_hooks.py:806-812`
- `src/elastic_agent/worker/runtime.py:1896-1903`
- `src/elastic_agent/core/claude_oauth.py:69-77`
- `src/elastic_agent/core/claude_oauth.py:165-173`

**问题**

Job 的 `login_timeout_seconds` 正确进入 `AccountLoginMessage`，但 Claude Runtime 构造
`OAuthConfig` 时未传该字段，因此始终使用默认 480 秒。Codex 路径会正确传入消息值。

**已确认复现**

消息请求 `1100` 秒，Claude provider 实际收到 `480` 秒。

**影响**

配置 900/1200 秒的慢验证码/CF 登录会被提前终止；配置 60 秒时又可能多等 7 分钟。

**建议**

构造 Claude `OAuthConfig` 时显式传 `msg.login_timeout_seconds`，并增加 60、默认值和 1200 三个边界测试。

### EA-AUD-019 — PTY autonomous result 污染前台记账

**位置**

- `src/elastic_agent/worker/pty_backend.py:529-596`
- `src/elastic_agent/core/log_event_parser.py:86-118`

**问题**

PTY backend 对 autonomous 事件用 `turn_scoped=False`，避免 Worker 本地 session/error 记账，但仍把
autonomous `raw_json` 原样作为 LOG 转发。原始 Claude JSON 不含 `autonomous` 标记。

Manager `LogEventParser` 对任何 parsed `type=result` 都更新前台 TaskSession 并累加成本。

**已确认复现**

转发：

```json
{"type":"result","session_id":"subagent-session","cost_usd":12.5}
```

后，前台 task 的 `session_id` 变为 `subagent-session`，总成本增加 `12.5`；Manager 无法得知它来自 autonomous turn。

**影响**

外部 API/归档显示错误 session，子 Agent 成本可能重复计入前台；Job terminal 本身仍由 Worker 本地 session 控制。

**建议**

丢弃 autonomous result 的 Manager-accounting 语义，或在 LOG 协议增加不可由 raw payload 伪造的 scope metadata，并让 Manager 仅对 foreground result 记账。

### EA-AUD-020 — Harness 同名上传会破坏旧版本

**位置**

- `src/elastic_agent/api/routes/jobs.py:1913-1947`

**问题**

启用可信 Harness 上传后，接口先用 `atomic_write_private(dest, content)` 覆盖同名文件，再 import/验证。新内容无效时执行 `dest.unlink()`，旧有效插件不会恢复。

即使新内容有效，既有持久化 JobSpec 的 `harness_ref` 仍指向同一路径，历史 Job 的执行代码被静默改写。

**影响**

一次错误上传可删除在用插件；同名成功上传破坏历史可复现性。该功能默认关闭，因此严重度降为中。

**建议**

先在唯一私有临时路径验证，再按内容 hash 发布不可变版本；禁止覆盖已有路径。JobSpec 持久化具体内容版本，而不是可变文件名。

### EA-AUD-021 — Agent API 账号无法删除

**位置**

- `src/elastic_agent/api/routes/agent_api_accounts.py:242-255`
- `src/elastic_agent/core/agent_api.py:1783-1840`
- `src/elastic_agent/api/routes/ui.py:1293-1297`

**问题**

REST DELETE 在账号存在时无条件返回 409，和实际 claim、lease、Worker 状态无关。错误文案要求“terminate all delegated Workers”，但完成该动作也不会改变结果。

core store 已有 durable `remove()`，UI 却只提供 Refresh，没有退休/删除入口。

**影响**

错误录入、已撤销或不再使用的 Key 永久留在账号列表和 Manager 状态目录；管理员无法完成凭据退休流程。

**建议**

实现引用感知的删除：mutation guard、无 active claims/leases、确认所有相关 Worker 已销毁或 scrub 后调用 durable remove。若近期不实现，应移除误导性的 DELETE 路由并明确标为未支持。

### EA-AUD-022 — OAuth 账号字段缺少硬边界

**位置**

- `src/elastic_agent/api/routes/accounts.py:42-59`
- `src/elastic_agent/core/credential_pool.py:33-68`
- `src/elastic_agent/core/account_store.py:66-113`

**问题**

`id`、`email`、`group` 只做 trim/non-empty；`email_token` 和 `password` 没有 byte 上限。没有统一 request body 上限。

合法 Bearer 可提交超大字符串或在 identity 中加入内部控制字符。AccountStore 每次 CRUD 都会 deep-copy、重新序列化并 fsync 整个 `accounts.json`。

**影响**

单次请求即可造成明显内存/磁盘放大，后续 list/update/startup 会反复付出成本；异常 ID 还会进入日志、Job 元数据和 cloud tags。

**建议**

为 ID 设字符集，为 email/group/name 设长度与控制字符限制，为 secrets 设 UTF-8 byte 上限，并在 ASGI/反向代理层设置全局 body limit。边界应与 Agent API store 的 16 KiB key 上限一样在 API 和持久层双重执行。

### EA-AUD-024 — dataset 丢失 Worker context 时静默回退 shard 0

**位置**

- `src/elastic_agent/core/batch_hooks.py:1423-1432`
- `src/elastic_agent/core/batch_orchestrator.py:504-511`
- `PROGRESS.md:562-563`

**问题**

provision hook 通过 Manager 的私有 `_batch` 反查 Worker context。如果 orchestrator 被自定义集成替换、
Manager 尚未暴露 `_batch`、worker index 丢失或 lookup 返回 `None`，代码不会失败，而是无条件使用
`spec.worker_contexts()[0]`。

这与项目自己记录的“fanout 资源不能在 provision 阶段退回首 worker 的静态 context”契约相反。

**已确认复现**

在无 `_batch` 的 FakeManager 上，对一个 `workers=2`、URI 含
`shard-{{shard_id}}` 的 Job 分别 provision `w0`/`w1`，捕获到的两条下载命令都指向
`shard-00000`。

**影响**

多 Worker Job 可正常通过 bootstrap 并开始执行，但所有 Worker 使用同一输入，造成重复计算和缺失
shard；错误不会在控制面显式暴露。

**建议**

把 `WorkerContext` 作为 provision 接口的显式参数传递。短期至少在
`fanout.workers > 1` 或 dataset 含模板时，对 context lookup 失败直接 fail closed，不能回退 shard
0。

### EA-AUD-025 — dataset 目标路径含空格时建父目录错误拆词

**位置**

- `src/elastic_agent/core/job_spec.py:254-264`
- `src/elastic_agent/core/batch_hooks.py:1438-1440`

**问题**

`S3Dataset.dest` 允许空格和 shell glob 字符。单对象下载虽然用 `_shell_quote` 保护了最终目标，但父目录
命令是：

```text
mkdir -p $(dirname '<dest>')
```

命令替换位于未引用上下文，`dirname` 的输出会再次发生 shell word splitting 和 pathname
expansion。

**已确认复现**

目标 `/tmp/.../shard data/input.jsonl` 的同构命令没有创建预期的
`/tmp/.../shard data`，而是分别创建 `/tmp/.../shard` 和当前目录下的 `data`。后续
`aws s3 cp` 因真实父目录不存在而失败。

**影响**

Schema 接受的合法绝对路径在 Worker 上稳定失败，并可能在 setup cwd 中留下意外目录。

**建议**

Manager 直接用 `PurePosixPath(ds.dest).parent` 计算父目录，再对完整父目录做一次
`_shell_quote`；或用位置参数传值，避免未引用的 command substitution。增加空格、`*`、`[` 和单引号
路径测试。

## 5. 低严重度问题

### EA-AUD-026 — dataset URI 不兼容空白占位符语法

**位置**

- `src/elastic_agent/core/job_spec.py:157-173`
- `src/elastic_agent/core/job_spec.py:241-252`
- `src/elastic_agent/api/routes/ui.py:1871-1879`
- `src/elastic_agent/api/routes/ui.py:1910-1917`
- `README.md:147-156`

**问题**

通用模板引擎明确接受 `{{ shard_id }}`，README 又说明“相同模板”适用于 dataset URI/dest；但
`S3Dataset.valid_s3_uri` 在渲染前拒绝 URI 中的任何 whitespace。Batch UI 也按空格切分一整行，
进一步把该 URI 拆坏。

**已确认复现**

构造：

```text
s3://private-data/shard-{{ shard_id }}.jsonl
```

会在 JobSpec 创建时得到 `S3 dataset uri must be a safe ...`，尚未进入模板渲染。

**影响**

API 与文档模板语法不一致；用户必须改写成无空白的 `{{shard_id}}` 才能提交，没有权限扩大。

**建议**

选择并固定一个契约：要么文档明确 dataset 只支持无空白形式并给出专门错误，要么先识别/保护模板
token，再验证渲染后的 URI；UI 不应使用无法区分模板内部空白的简单 split。

## 6. 工程质量和测试缺口

这些项目没有单独计入上述 26 个 Bug，但会降低回归发现能力：

1. Ruff 当前有 `341` 项错误：
   - `F401` 127 项；
   - `E501` 125 项；
   - `I001` 28 项；
   - 另有 `F841` 8 项、`F541` 9 项、`F821` 1 项等。
2. 全仓 `F`/`E9` 类共有 `145` 项（其中 `F401` 127 项）；按 Ruff
   常用致命集合 `E9,F63,F7,F82` 检查仍有 1 项 `F821`，当前没有干净的静态质量门。
3. `87` 条测试 warning 主要来自 websockets legacy/deprecation；依赖升级后存在破坏风险。
4. 近期模块的专项语句覆盖率约 `77%`，缺失区域集中在错误恢复、取消、存储失败和路由异常分支；本次多数高风险问题正位于这些 fault paths。

建议先增加不依赖真实云的故障注入测试，再把 Ruff 分阶段收敛为 CI gate。不能因为完整测试全绿就认为生命周期和安全边界已经覆盖。

## 7. 已核对但未发现新增高风险问题的区域

- Job、logs、results 路由统一带 Bearer API dependency，未发现未认证读取绕过。
- Agent API key 的普通 REST response 仍保持 write-only，未发现直接回显。
- Agent API 配置发送前的 login hook 会统一检查远端 WebSocket 为 `wss://`；未发现 API key 明文跨公网 WS 的独立绕过。
- Codex 登录的 auth.json 事务回滚、OTP request/account/worker 关联和敏感 HTTP 日志压制未发现同类缺陷。
- BindingManager 的正常 EIP detach → instance terminate → lease release 路径，以及通用 controller-tag orphan scan，未发现新的可复现身份越权或漏清理问题。
- Agent API projection 的固定 endpoint、私有 key-helper、marker/inode 校验未发现直接 key 回显或任意路径删除问题。

## 8. 建议修复顺序

1. 先处理秘密与越权面：EA-AUD-001～004。
2. 再处理会阻止终态、持续计费或资源放大的问题：EA-AUD-005～007、010～011、
   013～014、023。
3. 实现正确的 API Key 共享模型，并同步 preflight、JobSpec 和 UI：EA-AUD-008、017、021。
4. 修正公开但错误的产品契约：EA-AUD-012、015、018。
5. 最后处理结果一致性、PTY 记账、Harness、输入边界和 dataset 边界：
   EA-AUD-009、016、019～020、022、024～026。

每项修复都应先提交当前最小复现作为失败测试，再实施代码修改；涉及终态、删除、EIP 或凭据的修复应继续使用故障注入验证取消、重启、ENOSPC 和并发场景。
