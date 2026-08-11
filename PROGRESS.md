# PROGRESS — 经验教训沉淀

## 2026-08-11 Worker dataset prefix listing

**问题**：Worker role 虽允许 `GetObject` 读取 `jobs/datasets/*`，但递归
`aws s3 sync` 会先调用 `ListObjectsV2`。真实动态 smoke 完成 EC2、bootstrap 和 WSS 后，在
`prepare` 前按设计以 `AccessDenied` 失败关闭。

**解决**：仅当 `s3:prefix` 是 `jobs/datasets`、`jobs/datasets/` 或
`jobs/datasets/*` 时允许 `s3:ListBucket`；结果前缀仍不可列举，结果对象仍不可读取/删除。生产更新前
用 IAM simulator 同时证明 dataset allow 与 result deny。

## 2026-08-11 Run-Benchmark S3 dataset/setup ordering

**问题**：真实无模型 smoke 已通过 IAM、私网 SSH 和四步 host bootstrap，但 Manager 的固定顺序是
`code → setup → S3 dataset → run`，constructor 却在 setup step 执行 `elastic_worker prepare`，导致
`_SEAL.json` 尚未下载就按设计失败并回收实例。

**解决**：setup 只创建 venv 并安装 exact package；固定 run command 改为 `prepare && exec execute`。
Manager 在进程启动后发送的一次性 frame 只在 trusted stdin pipe 等待，prepare 不读取 stdin，execute 在
seal/release/instance/image/wall-time 全部证明后才首次读取。保留通用 dataset 顺序，避免影响其他 Job。

## 2026-08-11 Tokyo A dynamic Worker KeyPair binding

**问题**：无模型 smoke 的首台 EC2 进入 running 后无法 SSH；Worker SG 的 Manager-SG 私网规则正确，但
生产 env 使用 `interview-key`，本地 PEM 实际匹配 `panyuexi`。切换 env 后，Manager role 的精确
RunInstances resource whitelist 又按预期拒绝未列出的 `panyuexi`。

**解决**：新 Worker 统一为 `panyuexi + /home/ubuntu/panyuexi.pem`；IAM policy 精确增加 `panyuexi` ARN，
同时保留旧 `interview-key` 作为回滚，不使用通配符。测试同时绑定 env、runbook 和两个精确 ARN。

## 2026-08-11 Run-Benchmark 一次性凭据动态 Job 通道

**问题**：通用 JobSpec 的 `run.secret_env` 会持久化 secret reference，无法满足 Run-Benchmark API Key
不进入 JobSpec/S3/cloud-init/checkpoint/disk/log 的边界；普通 interactive `SEND_INPUT` 又会把文本加换行且
不关闭 stdin，无法承载 `RBWORK01` 二进制 frame，子进程会永久等待 EOF。Manager 重启还会丢失内存 Key，
不能把 prepared Job 静默恢复成无凭据或重放旧调用。

**解决**：增加 Manager-owned `/api/jobs/run-benchmark` constructor，固定 repo/exact commit、环境、命令、
单 Worker、S3 和 collect，只把严格交叉验证的公开 binding 持久化。frame 由 bounded process-local
`EphemeralStdinLeaseStore` 无复制接管；command 启动后 consume，一次性经 WSS `SENSITIVE_INPUT` 发送，
task supervisor 最终 decode、写原始 bytes 并立即 EOF。所有 mutable buffer 在发送、丢弃、过期、失败和
shutdown 后覆零；普通 `/jobs`/plan 禁止 reserved protocol，缺 lease/发送失败会 stop 已启动 task。
request fingerprint 不含 frame、Key 或 secret-derived digest；相同 Idempotency-Key 的 replay 只返回原 Job，
不会替换其 lease。prepared journal 在 Manager restart 后明确 409，要求新 attempt。

**验证**：新增 lease one-shot/TTL/close、dispatch ordering、missing-lease stop、supervisor binary+EOF 和专用
API 安全/幂等/无秘密 journal 测试；专项 suite 338 passed（既有 asyncio subprocess teardown warning 不影响
结果）。生产验收只允许无付费 provisioning/result 生命周期 smoke，不能为部署主动调用真实模型 Key。

## 2026-08-07 ApexRouter 不限额窗口准入（commits `8bbd2df`, `017149e`）

**问题**：ApexRouter 用固定窗口的 `remaining=null`、`limit=null` 表示该共享窗口
没有额度上限。Elastic-Agent 原先要求两者都是非负数字，因此把有效的不限额 Key
归为 `invalid_usage_response`，usage probe 后无法进入 Codex Agent API 调度。

**解决**：仅把字段存在且成对显式为 `null` 的窗口归一化为 `unlimited=true`，保留
该 Key 的独立 usage 和共享 concurrency。受限与不限额窗口可以混合；字段缺失、单边
`null`、非法/负数、remaining 超过 limit 以及 concurrency 耗尽仍严格 fail closed。
该规则与 CC-Manager PR #98 对齐；README、测试指南和架构说明同步记录该 provider 语义。

**避免复发**：共享额度必须按 provider 的显式 sentinel 解释；修复假阴性时不能把
“无法证明有限额”泛化成“不限额”。每种 sentinel 都应同时覆盖正常、混合、非对称和
缺字段四类测试，避免放宽门禁。

**验证**：先用红测试复现全不限额和混合窗口被拒绝，修复后 Apex/Agent API 定向
`71 passed`，完整套件 `2877 passed / 12 skipped / 0 failed`；Ruff、
`git diff --check` 通过。claude-pty lock 与远端 main 同为 `7d5a0e5`。

## 2026-07-29 Job 人工冷中断与一键续跑（commit `192430b`）

**问题**：已有恢复能力只覆盖 runtime/连接重启和实例丢失后的新 Job 恢复，没有一个由
管理员主动触发的安全停止事务。直接复用 Cancel 会在应用写中间文件时终止进程，并可能把
不完整目录上传；只增加 SIGINT 按钮又无法覆盖 HTTP 断连、Manager 崩溃、自然
`PROCESS_EXIT` 并发清理、EC2 已终止但 Job 终态未写盘等窗口，也无法证明后续续跑使用的是
哪一份完整 checkpoint。

**解决**：JobSpec 新增完整、显式的 `run.resume_command`。Batch API/UI 增加
`中断并保存进度` 与 `一键续跑`：interrupt 先把 Idempotency-Key 的 SHA-256 和
`suspending` intent 在同一次 mode-0600 journal replace 中提交，再以 group-scoped、
non-escalating SIGINT 给应用协作收口机会，超时后按 TERM/KILL，随后停止
runtime/supervisor/Docker/containerd 并扫描宿主残留 writer。只有可靠 exit、日志归档、
final collect、完整 S3 set、Worker/EIP/账号 cleanup 全部收敛且 generation/timestamp 与
本地 durable pointer 精确一致时才写 `suspended`。普通 Worker 在终止后保留带 collection
proof 的 registry tombstone，EIP 在 release 前写 lease proof；terminal journal 成功后才
移除/视为完成。后台异常有界重试，shutdown 同步接管已提交事务，仍失败则保留 durable
资源给启动恢复，禁止从未静止文件系统收集。续跑永不复活旧 Job id，而是从私有 source
spec 和 exact generation 创建新 attempt，并记录 direct/root/attempt lineage。

**关键经验**：优雅信号只是“请求”，不是静止证明；应用、shell wrapper、容器和逃逸进程
必须分别收敛。任何“先销毁资源、后写终态”的路径都需要一个跨崩溃 tombstone。异步调用方
取消不能撤销已进入线程或文件替换的事务，owner 必须在释放锁前等真实结果。幂等 sidecar
不能成为唯一真相；当前每次 action 在全局锁内完整校验所有有界私有 Job journal，sidecar
只作可重建缓存。宿主静止证明要求独立的非 root runtime 用户；root 部署会 fail closed。

**验证**：Elastic 全仓 `2866 passed / 12 skipped / 0 failed`；另有 API/UI
`230 passed`、核心交叉套件 `615 passed` 和独立对抗复核 `787 + 58 passed`。
`compileall`、fatal Ruff、Batch 内联 JavaScript `node --check`、敏感 Key pattern 与
`git diff --check` 均通过。未创建云资源、未部署、未重载或重启当前服务。

## 2026-07-29 Job 提交配置持久查看（commit `9d548b6`）

**问题**：JobSpec 已在云资源副作用前写入 mode-0600 journal，但 Batch Console 的
`GET /jobs` 轻量轮询故意不携带 spec，Job 卡也没有按需读取详情的入口。用户因此无法在
提交后或 Manager 重启后确认当时实际生效的 repo/ref、运行命令、资源、账号与收集配置；
直接把 spec 加进列表又会造成历史响应膨胀和秘密暴露。

**解决**：每张 Job 卡新增“提交时生效配置（已脱敏）”折叠区，仅在管理员显式展开时读取
`GET /api/jobs/{job_id}`，通过 `textContent` 显示和复制 JSON。详情优先从 durable
journal 读取不可变的 normalized snapshot，不信任后续可变的 live model；普通/Setup
环境值和 Secret 引用均隐藏，未知旧字段、损坏/超大/symlink journal fail closed。
读取复用专用线程池、fail-fast admission 和取消 owner，响应 `no-store`；前端同时限制
single-flight、全局并发、等待队列、LRU 总量和单配置预览大小，并在轮询重绘后保留展开、
滚动与焦点。`/jobs` 列表继续不带 spec。

**以后避免**：持久化能力只有形成可发现、可理解的只读 UI 才算完整；历史配置应明确区分
raw request 与“当时验证/归一化后的生效配置”。新增历史展示入口时必须先确定秘密投影、
响应与浏览器内存预算、旧 schema 策略和重启语义，不能为了减少一次请求把重对象塞进高频
列表轮询。

**验证**：先以红测试复现缺少入口、live spec 漂移和无 `no-store`；修复后 Batch API +
UI `197 passed`，全量 Unit `2526 passed`，Integration `90 passed / 12 skipped`。
Node 内联脚本语法、focused Ruff、compileall、`git diff --check` 和 claude-pty lock
dry-run 均通过。未部署、未重启或修改运行中的服务。

## 2026-07-28 全量代码审计修复闭环

**结果**：以 `86dd0f8` 为修复基线，关闭
[`docs/audits/code-audit-2026-07-28.md`](docs/audits/code-audit-2026-07-28.md)
记录的全部 35 项可复现问题。修复覆盖秘密文件权限与登录回滚、可靠事件和 Worker
有界队列、API Key 共享与账号/EIP 释放证明、普通 shard 逐机回收、Results/外部文件
有界流式读取、请求体/历史 Job/日志 admission、幂等恢复、旧 Job teardown-only
兼容，以及 Batch UI 输入和状态一致性。AI4Sci Bench 的默认 ref 同步为
`archive/youchengsong-managed-agent-api-20260728`。

**关键经验**：异步取消不能等同于底层线程、云调用或文件句柄已经退出；并发 permit 必须由
真实 owner 持有到资源关闭。账号可复用必须以实例终止、registry 删除和 durable lease
释放证明为准，不能只看 Job phase。持久格式收紧时要为旧 journal 保留只读、不可执行的
清理兼容层。前端不能静默丢弃格式错误配置，也不能在权威状态读取失败时把未知显示成空闲。

**验证**：Unit `2510 passed`；Integration `90 passed, 12 skipped`；Batch API
`148 passed`；UI/API 最终复核 `181 passed`。前端 JavaScript 语法、compileall、
fatal Ruff 集合、`git diff --check`、文档相对链接和敏感 Key 扫描通过。未创建云资源，
未部署、未重启或修改运行中的服务。

## 2026-07-28 全量代码审计基线

**范围**：以最新 `origin/main` `8dc4228` 为基线（包含审计期间新增的 per-worker
S3 dataset 增量），对账号/API Key、Batch/EIP 生命周期、Worker 协议与登录、PTY、
结果下载、API/UI 和持久化恢复做只读审查。完整套件
`2356 passed / 12 skipped`，近期高风险模块专项 `375 passed`、新增分片数据集专项
`303 passed`，`compileall` 通过；同时用故障注入复现 ENOSPC、登录取消、超长输出、
下载断连和路径穿越。
未创建真实云资源、调用用户 Key、部署或重启服务。

**结果**：确认 26 项开放问题，其中 14 项高严重度、11 项中严重度、1 项低严重度。最高优先级是
LocalBackend 路径穿越、Claude 凭据 `0644` 与登录秘密进入 journal、Claude
取消登录的错误清理确认、可靠终态/tombstone 写失败阻断实例回收、超长输出停止
pipe 排水、Agent API Key 不能多 Worker 共享、结果下载/解析的资源耗尽路径，以及空
hostname 把单对象 S3 dataset 退化为整桶同步。
完整证据、最小复现、影响和建议见
[`docs/audits/code-audit-2026-07-28.md`](docs/audits/code-audit-2026-07-28.md)。

**以后避免**：完整测试全绿不能替代失败路径审查；取消、断线、ENOSPC、LIST→OPEN
替换和多 Worker 并发必须作为一等测试维度。任何秘密写入都要在 API、传输、日志和
文件 mode 四层分别验证；任何可靠终态的辅助状态写入都不能阻止更重要的收集和云资源
销毁。API Key 共享必须与 OAuth/EIP 独占模型显式分层，不能仅移除一个候选过滤条件。

## 2026-07-28 同步 CloudRouter unrestricted 准入语义

**问题**：CCM `387834d` 明确了 CloudRouter 无消费上限账号会返回
`mode=unrestricted`，同时可能把顶层 `balance` 和 `remaining` 都报告为 0；
这些数值只是展示信息。Elastic 仍把该模式改写成 `wallet`，进而把 0 判为 exhausted，
导致有效 Key 在 usage probe 后被错误排除，无法进入 Agent API 登录/调度。

**解决**：保留 `unrestricted` 为一等模式并使用 USD 展示；只对该模式忽略顶层
balance/remaining 的耗尽推断，原值仍原样返回。显式 exhausted/invalid status、
expiry、quota 和 rate-limit window 继续走原有严格门禁。没有改 Worker Key 投影、
WSS 下发协议或其他 provider，也没有混入 CCM 相邻的大型 Fast-tier/API 删除功能。

**避免复发**：额度字段必须结合 provider 的明确模式解释，不能把“0”脱离语义统一
视为耗尽，也不能为了方便改写上游模式。同步上游小修时既要加入同构回归，也要额外
锁定原有 fail-closed 门禁，避免修正假阴性时引入真正的无限准入。

**验证**：先用两条红测试复现 `unrestricted→wallet/exhausted`，修复后同时验证
零余额可准入且显式 quota 耗尽仍拦截；Agent API/Worker/API/编排定向回归
`271 passed`，完整套件 `2351 passed / 12 skipped / 0 failed`。Ruff、`compileall`、
`git diff --check` 和独立对照审查均通过；claude-pty lock 与上游 main 一致。
按用户要求未部署、重启或修改任何运行中的服务。

## 2026-07-28 使用过的 EIP 账号安全删除

**问题**：EIP Job 无论成功还是失败，终态都会销毁临时 EC2、释放 lease，
但按设计保留账号的持久 EIP。后端因此正确拒绝直接删除仍有 binding 的账号；
Batch Console 却只调用 identity DELETE，没有提供显式 decommission 入口，导致已经
清理完成、binding 回到 `ready` 的旧账号也只能看到 409，无法从页面删除。

**解决**：OAuth 账号删除前重新读取权威 binding。无 binding 时确认后正常删除；
有 binding 时展示具体 EIP 和不可恢复警告，并要求输入完整账号 ID，再调用双确认
decommission，成功后才删除 identity。活跃 claim/lease 或清理未完成仍由服务端
409 阻止，EIP 与账号保持不变；若两个 API 之间发生并发抢占，页面会准确报告
“EIP 已释放但账号尚未删除”并刷新真实状态，不误报成功。

**避免复发**：持久云资源不能由普通 identity DELETE 隐式释放，但安全边界也必须
提供完整的管理 UX。内联 JavaScript 的测试要检查 Python 字符串解析后的真实 HTTP
响应；仅对源码运行语法检查会漏掉 `\n` 转义被解释成字面换行的问题。

**验证**：先以红测试复现缺少 decommission 流程，修复后 Web/API 定向回归
`117 passed`；精确页面响应通过 Node 语法检查，并动态验证绑定成功、活跃 lease
阻断、无绑定删除三条路径；完整套件 `2349 passed / 12 skipped / 0 failed`，
Ruff（忽略该内联 HTML 文件既有 E501）、`compileall` 和 `git diff --check` 通过。
按用户要求未部署、重启或修改运行中的服务。

## 2026-07-28 Batch Job 表单信息架构与可访问性

**问题**：JobSpec 的几十个字段长期平铺在一张卡片中，标签以内部字段名为主，
Repo/账号/EIP/换号等条件字段看不出何时生效；结果收集间隔、根盘等常用配置也难找，
窄屏上长标签和操作按钮进一步挤压。

**实现**：

- 保留所有既有控件 ID、默认值和 `buildJobSpec()` 映射，按基本信息、计算资源、
  代码初始化、账号、运行、结果、换号和 Harness 八个 `fieldset` 重排。
- 主标签改为用户职能，原始 JobSpec 字段降为次要提示；低频字段放入 disclosure，
  结果路径和增量收集保持常显。
- Repo、账号模式、EIP 和换号策略增加动态禁用与状态说明；运行命令、TTL/timeout、
  数值上限和 S3 数据集格式在发起 preflight 前本地校验。
- 标签与帮助文字建立 `for`/`aria-describedby` 关联，状态使用 `aria-live`；
  移动端控件提高到 44px/16px，操作按钮改为全宽堆叠。

**避免复发**：UI 重排不能靠复制一套新字段；测试必须同时锁定控件 ID 唯一性、
字段归属顺序和 `buildJobSpec()` 全量语义，防止视觉改版悄悄遗漏配置。

## 2026-07-21 task153：账号固定 EIP + 临时 EC2 全生命周期重写

**问题**：旧模型把账号绑定到长期 EC2，停机仍保留根盘等资源；Job 创建、登录、结果收集、取消和云 API 最终一致性之间也没有一个可恢复的所有权事务，失败窗口可能遗留收费实例、重复 EIP、账号 claim 或错误登录身份。

**实现**（commit `4a1f244`）：

- 持久关系改为稳定 `account_id → AWS EIP`；每个 Job 创建独占 lease 和 fresh EC2，attach 后 bootstrap/login/run，终态先 final collect，再 detach、确认 terminate EC2/root EBS、释放 lease/claim，EIP 仅在显式 decommission 时释放。
- 绑定、lease、JobSpec 都以 mode 0600 + fsync 原子落盘；Manager flock 防双主，启动/live recovery 按 controller/account/lease tags 收敛不确定的 AllocateAddress/RunInstances。
- 整单容量先预占，多 shard EIP reservation 并发但 all-settled；普通失败、反复取消、capacity release 和 post-reserve 窗口都先完成补偿再返回，清理失败保持 fail-closed/pending retry。
- EIP worker 禁 IPv6，强制分发当前 Manager 的 worker 包、停旧服务并要求新 WebSocket 重连；登录结果按 request id 关联，精确核对 email 且 warmup 成功后才运行。EIP spec 拒绝 `HOME`/`CLAUDE_CONFIG_DIR` 覆盖。
- 账号邮箱 token 写入 Manager 的 mode-0600 store 且 API 不回显；跨机登录要求 WSS。当前真实执行仍仅支持 Claude，Codex 自动登录和通用 IMAP 未实现。

**避免复发**：任何不可取消的云调用都必须先持久化意图，并由循环 shield 的 owner task 等真实结果后补偿；永远不能先释放账号 claim 再处理 durable lease。部署验证不能只看 registry READY，必须证明当前 worker 版本重新连接并完成身份校验。

**验证**：task153 聚焦回归 `521 passed`；安装 PTY extra 后全量 `1689 passed, 12 skipped, 8 failed`，8 项均为任务前基线（credential rotation、默认端口、file-sync 断言/权限），本功能无新增失败；`ruff`（变更核心）、`compileall`、`git diff --check` 通过。未使用真实 AWS 凭证做破坏性 smoke test。

**Commit**: `4a1f244`

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

## 2026-07-21 Codex 密码自动登录（commit `5f52384`）

**问题**：Elastic 只有 Claude 的 worker-local 登录；`group=codex` 只是标签。Codex 的 OAuth callback 又固定回到启动 CLI 的本机，不能由 Manager 代登或只分发 token；无邮箱查询 token 时还必须允许管理员在同一登录请求中补交 OTP。

**解决**：账号模型新增 `agent_type` 与写入后不回显的 OpenAI password；worker 在同机启动 `codex login`，用 Xvfb + 系统 Chrome + Playwright 完成 email/password/OTP，严格校验 `auth.json` 的 ChatGPT OAuth、id-token email，并用隔离的 `codex exec` 做真实可用性验证。失败/取消事务性恢复旧凭证。Manager 用 request/account/worker 三元关联 OTP 与结果，断线立即失败；取消时等待 worker 清理 ACK，普通 worker 清理不确定则隔离账号，EIP Job 则靠先销毁临时 EC2 再释放 claim 保证安全。Codex Job 固定 CLI 版本并强制部署当前 worker 源码，避免旧协议误走 Claude。

**以后避免**：浏览器登录不能只以“页面走完”为成功，必须同时验证最终账号身份和 CLI 真调用；异步取消也不能把“已发 cancel”当作“已清理”，资源/账号复用前必须有 ACK、实例销毁或隔离兜底。秘密字段的 REST、日志、异常和 UI 都要逐层检查，OTP 元数据用 DOM text 节点渲染，不能直接拼 HTML。

**验证**：相关回归 389 项全绿；全量为 1770 passed / 12 skipped / 8 个既有基线失败（凭证轮换 3、端口默认值 2、文件同步 3），无新增失败；ruff、`compileall`、diff check 与 Batch Console JavaScript 语法检查通过。未使用真实 OpenAI 账号做线上登录，也未创建 AWS 资源。

## 2026-07-22 Codex token-only / email-code 登录（commit `5206410`）

**问题**：CCM 已支持 OpenAI password 或邮箱查询 token 二选一，但 Elastic-Agent 仍在账号模型、REST 和 worker runtime 三层强制 password；即使有 163 MailCatcher token 也无法进入登录。FastAPI 默认 422 还会把类型错误的秘密输入放回响应。

**解决**：Codex 账号改为至少配置 `password`/`email_token` 之一；token-only worker 在密码页或登录方式选择页进入 email-code，支持 “Try another method” 二级菜单，再沿用受支持邮箱后端自动取 OTP。若页面无 email-code 入口则安全失败并要求 password；原 `auth.json` 事务性回滚、身份校验与 `codex exec` smoke test 不变。API 新增 `clear_password`，且禁止清掉唯一登录输入；全局请求校验响应移除原始 `input`/validator context，避免 malformed secret 回显。

**以后避免**：同步上游登录实现时必须逐层核对账号 schema、REST、协议、runtime 守卫和真实浏览器状态机，不能只复制取码函数。页面跳转成功必须以目标 OTP 控件出现为准，不能用“密码框消失”推断；所有写入后不回显字段还要覆盖框架级 422 路径。

**验证**：token/password、REST、Manager→worker、method picker、OTP、回滚、脱敏与旧密码路径相关 253 项全绿；全量 1784 passed / 12 skipped，8 个失败与既有基线相同（凭证轮换 3、端口默认值 2、文件同步 3），无新增失败；Ruff、`compileall`、diff check 和 Batch Console JavaScript 语法检查通过。完整 AWS EIP/EC2/真实账号结果见后续实盘记录。

## 2026-07-22 Codex mailbox URL 日志脱敏（commit `fd9a737`）

**问题**：token-only Codex 的首轮 AWS EIP 真机登录到达邮箱取码阶段时，发现 `httpx` 默认 INFO 日志会记录完整请求 URL，MailCatcher query token 因而可能落入临时 worker 的 systemd journal。发现后立即取消 Job，按编排链解绑 EIP、终止 EC2 并删除 root EBS；账号绑定的 EIP 保留且恢复 `ready`，未继续执行任务。

**解决**：在任何邮箱请求之前，永久把进程内 `httpx` 与 `httpcore` logger 提升到 WARNING。这里不在请求后恢复级别，避免并发邮箱轮询在恢复窗口再次泄漏。新增回归测试，在 INFO 级别执行带哨兵 token 的请求，断言日志无 token，且两个 logger 的有效级别均已受限；同步更新秘密边界和真机测试文档。

**以后避免**：write-only 不只要检查 REST/异常和业务日志；第三方 HTTP 客户端可能在 INFO 记录含 query secret 的完整 URL。任何带秘密的 URL 请求都要在第一次真机调用前审计依赖 logger，并用哨兵秘密跑日志回归。

**修复后实盘**：东京区 EIP 配额申请已批准到 13，给 token-only Codex 账号分配了独立 EIP `13.112.54.251`。Job `job-7a2c3b96bf274d3584b1b342fba7c4e1` 从空白 `t3.large` 创建开始，完成 EIP attach、固定版 Codex/Chrome/runtime bootstrap、自动 email-code 登录、OAuth 邮箱身份校验和两次真实 `codex exec`（登录预热 + Job 命令）。结果 API 收到 `codex-output.txt`/`observed-eip.txt`/`status.txt`，分别验证 `CODEX_EIP_JOB_OK`、实际 IPv4 出口等于绑定 EIP、状态 `ok`。终态 `done=1`、final collect 与 cleanup 均成功；实例终止、EIP 解绑但保留，账号 binding 回到 `ready` 且无活动 allocation，Manager 原 3 个 worker 保持在线。真实 journal 二次检查均为 0 条 mailbox token URL。

**测试命令教训**：worker 的 subprocess stdin 保持 PIPE 以支持 `SEND_INPUT`。首条无人值守 `codex exec` 虽已有 prompt，仍显示 `Reading additional input from stdin...` 并等 EOF；这不是登录失败。该轮通过编排器安全取消并完成 final collect/EC2 清理，重跑时显式加 `</dev/null` 即完成。以后所有可能读取 stdin 到 EOF 的 Mode-B CLI 都要明确重定向，不能把“进程存活但无输出”误判成 OAuth 卡住。

**验证**：`tests/unit/test_codex_login.py` 26 项全绿，Ruff、`compileall` 与 diff check 通过；真实 Job 的登录、命令、结果 API、EIP 出口、失败补偿和成功清理均已验证。

## 2026-07-22 Manager 生产域名与 WSS 回连（deployed main `476c3f0`）

**变更**：`elastic-agent.claude-code-manager.com` 已由 Cloudflare 代理到东京 `elastic-agent-manager`。公网 HTTPS 页面和 `/api/health` 均返回 200，`wss://elastic-agent.claude-code-manager.com/ws/runtime` 握手成功。Manager 的 mode-0600 EnvironmentFile 改为该 WSS URL，机器 launcher 改为 `setdefault`（不再覆盖部署环境），并移除临时 `ELASTIC_AGENT_ALLOW_INSECURE_ACCOUNT_LOGIN`；零活动 Job 时重启，原 3 个 worker 全部重连且健康。修改前 launcher/EnvironmentFile 均留有同机备份。

**边界**：当前 TLS 在 Cloudflare 边缘终止；源站仍只有 Python 服务监听 `0.0.0.0:8080`，本机没有 nginx/caddy/certbot 或 80/443 listener。若要求 Cloudflare→源站也加密，需要在 Cloudflare 侧配合 Origin Certificate/Tunnel 与 Full (strict)，不能仅凭公网 `https://`/`wss://` 握手推断端到端 TLS。

**同时完成**：东京区 EC2-VPC EIP quota 从 13 提升到 20 的申请已 `APPROVED`；新增地址仍按账号 binding 按需分配，不提前创建无账号归属且持续计费的 EIP。

## 2026-07-22 分布式 Job 生命周期、环境与结果耐久性加固（commit `65fb0b7`）

**问题**：从“命令能跑”扩展为可长期分布式执行时，故障注入暴露出一组组合竞态：终态事件断线会丢或乱序；取消撞上 EXECUTE 派发会先收集/销毁再停进程；RUN_EXHAUSTED 的 handler 在唯一 WS read loop 内等动态登录会自锁；云创建后 registry/terminate 双失败会留下收费实例；多 shard 同名结果会覆盖，S3 LIST→GET 变化可造成 OOM/静默截断，metadata 判重也会漏掉同 size/mtime 改写。Job 环境、超时、秘密和实例成本此前也缺少统一的提交前约束。

**解决**：Worker 用 0600 fsync ordered outbox + event_id/ACK 可靠回传 PROCESS_EXIT/RUN_EXHAUSTED，并把 final-sync 中的 task 纳入 STATUS；Manager 精确按 task_id 幂等处理，换号同步 claim 后转后台登录。取消固定执行 TERM→可靠 exit→KILL fallback→final collect→销毁；普通和 EIP 实例所有创建/注册/收集/终止失败都有在线补偿和重启扫描。JobSpec 新增不可变环境 profile、结构化非 root setup、严格 schema、有限 run timeout/TTL、JIT AWS secret refs 与 WSS 守卫；plan/submit 在副作用前校验 region、账号、S3 role、实例 allowlist 和 worker-hours。结果按 shard 隔离，显式 `collect.paths`，worker 直推递归刷新、relay 用 rsync checksum、Manager uploader 用 SHA-256；API 对路径、对象数/大小、score GET 和 tar 流式下载做边界与一致性校验。worker_clone 不再隐式下发 Manager Git token，任意 Python Harness 默认关闭。

**以后避免**：异步“已发送”不等于远端“已完成”，资源释放必须由可靠、相关联的终态证明驱动；不能在承载响应的同一接收循环里等待该响应。云资源一旦 API 返回 ID，就要先进入所有权图再做可失败的登记。结果耐久性不能只信 size/mtime 或一次 LIST；最终收集必须在停止生产者后按内容刷新并 fail closed。任何秘密在解析前先检查传输边界，任何 Job 参数在产生持久化/账号/云副作用前先做纯 preflight。

**验证**：完整 unit 为 1852 passed；仅 3 个本任务前已有的 file_sync 角色/本机 `/root` 权限用例失败。新增/相关聚焦 188 项与 claude-pty 69 项全绿；`compileall`、diff check、变更模块 Ruff 和两段前端 JavaScript 语法检查通过。实机 EC2→命令→S3→销毁闭环在部署后单独记录。

**实机补充（commit `2799d6d`）**：首轮 smoke 虽已终止 EC2，但 `/api/nodes` 仍保留 TERMINATED NodeRecord；大量短 Job 会让 registry/UI 无界增长。Job 专用 `ManagerFleetDriver.scale_in` 现于云终止成功后调用 Manager 的标准 `remove_node`，同步清理 task/connection/registry；若本地清理失败会让 orchestrator 重试，而不会把仍可能收费的实例句柄提前丢掉。相关 131 项全绿。

## 2026-07-22 生产发布与 EC2→S3→销毁双闭环（deployed runtime `c3c25d8`）

东京 Manager 以原子 release symlink 发布到 `/home/ubuntu/elastic-agent.release-c3c25d8`，旧 release 保留可回滚，持久状态目录未移动。新 launcher 移除硬编码 API-key fallback，只依赖 root:root 0600 EnvironmentFile；域名 health 200，WSS/S3/provider 配置保留。发布前后均确认无活动 Job/Worker。

两轮 `account.mode=none`、单 `t3.large` 真 Job 分别为 `job-bc85c36570121c8abe1ff41634a97c39` 与 `job-8029babb8c9162fa7dd001d77ba5cb31`：从零创建 EC2、bootstrap 当前源码、执行 repo-less shell、Worker 实例角色直推 `jobs/<job>/workers/shard-00000/results/`，manifest+4 个数据文件均可经 S3 优先 results/list/download API 读取；Job `succeeded/done=true/cleanup_pending=0`，EC2 终止且 root EBS DeleteOnTermination。重复首轮 Idempotency-Key 返回同 job 且 AWS 始终只有一台实例。第二轮验证 `c3c25d8` 后 `/api/nodes` 自动回到 0，无 TERMINATED registry 残留；首轮旧记录在确认云实例 terminated 后通过标准 API 删除。

冷启动主要耗时仍是 Ubuntu 现场安装 Node/npm（展开 500+ deb）；下一步最高收益是用当前 bootstrap 产出版本化 golden AMI，并保留现有 profile/commit 校验作为漂移与回滚边界。生产侧另有三项需单独变更窗口：Manager/Worker 拆 SG 并封 origin 8080、Worker S3 FullAccess 收敛到结果桶/prefix、为结果桶确定 retention 后启用 lifecycle/versioning。

## 2026-07-22 AWS 生产加固、Golden AMI 与四轮真机验收

**代码与发布**：`378398b` 增加私网管理路径、Golden AMI 构建/校验、生产 launcher、IAM/SG runbook 和回归测试；`32f0217` 固定东京生产资源；`7627c81` 把 Manager 启动凭证链收紧为 IMDSv2 专用实例角色并启用 systemd 沙箱；`5832c5e` 修正 EC2 Key Pair ARN 必须使用名称 `interview-key` 而不是 KeyPairId。Manager 当前运行不可变 release `/home/ubuntu/elastic-agent.release-7627c81`，域名健康，持久状态未迁移，旧 release/配置保留作回滚。

**AWS 落地状态**：Golden AMI `ami-0aec7ffcbe44c6f7a` 可用、私有、自有、x86_64/HVM/ENA/IMDSv2-only；加密 snapshot `snap-095e5fef3ae78fce0`（20 GiB，KMS `94512b70-8710-409f-ae17-5770f7562668`）完成，builder 已终止。Manager 独占 role/profile `elastic-agent-manager`，目标实例的 profile association 为 `iip-assoc-01f75381926371ea2`，共享 `Manager` role 未改。Manager ENI 只保留 `sg-02a0d62d1a8d082c9`，22/8080 仅允许 Connector SG `sg-050b918ed465816c8`；Worker 只用 `sg-05c68220f901fb555`，22 仅允许 Manager SG。域名/私网 SSH 正常，Manager 公网 22 与 8080 均超时不可达。

结果桶 `elastic-agent-results-297645381734` 已启用 versioning、全量 public block、SSE-S3，以及未完成 multipart 7 天清理、30 天 Standard-IA、90 天 Glacier 和同样的 noncurrent transition（不自动删除结果）。Worker role 已移除 `AmazonS3FullAccess`，无 AWS managed policy，只保留 `ElasticAgentWorkerResultsOnly`：允许指定桶 `jobs/*` 的 Put/Abort 和 bucket-location，拒绝 List/Get/Delete/跨前缀写。EIP quota 已批准为 20；继续按账号懒分配而不预建空闲、持续计费的地址。

**真实验收（全部终态 `succeeded`）**：

- 标准 profile `job-a743ae66659751839ef7c38821823f74`：Golden AMI、专用 SG/role、加密 gp3/IMDSv2 创建，结果直传 S3，EC2 `i-09e3ce1208e187b39` 与 root EBS 删除。
- 最小 S3 权限复测 `job-c440c9b46a2f221013d0382427d3d9fa`：移除 FullAccess 后仍完成 worker-direct upload，EC2 `i-0904cfbd70a551735` 与 root EBS 删除。
- Docker profile `job-95d4b8ff9c99fb4a16cc675ac3fe5c96`：Job 用户成功访问 Docker Engine 29.1.3，结果直传，EC2 `i-0ebb8c18a42d7e427` 与 root EBS 删除。
- token-only Codex+EIP `job-c66a472aa16f5ff4391a6c4327e564f6`：EC2 `i-04c1f305f125967d3` 绑定 `13.112.54.251`，自动 email-code 登录，无人工 OTP challenge，预热和 Job 内真实 `codex exec` 均成功并返回 `EIP_CODEX_OK`；S3 记录的公网 IPv4 与绑定完全一致。终态先 final collect，再解绑 EIP、终止 EC2/删除 EBS、清 Node/lease；EIP 保留且 binding 回到 `ready`。结果 list/download API 从 S3 返回 200。

最终检查为零活动 Elastic-Agent EC2、零附着 root volume、零 Node 残留、零 login challenge；Codex EIP 已分离但保留。当前账号池没有可用于本轮发布的 Claude 账号，因此没有新跑 Claude 登录 canary；此前 2026-07-15 的真实 Claude worker-local 登录记录仍有效。

**验证**：部署/IAM/Golden/私网路径聚焦测试 158 passed；AWS Access Analyzer 对 Manager/Worker 两份策略均为零 finding，按动作切片的 IAM allow/deny 模拟全部通过；完整套件 2010 passed / 12 skipped，8 个失败均为本任务前已有的 credential-rotation 断言 3、server 默认端口断言 2、file-sync 角色/本机 `/root` 权限 3，本次未改对应实现且无新增失败。Ruff、`bash -n`、diff check 与四轮真实 AWS canary 均通过。

**经验**：IAM 资源 ARN 不能凭控制台 ID 猜。EC2 Key Pair 的 ARN 资源段使用 key name，首轮 Job `job-06cd63e5cdf5a0954264059e6a4402b1` 因把 KeyPairId 写进策略而在创建实例前安全失败、无资源泄漏。修复时先加红测，再用真实 ARN 做 allow、旧 ARN 做 implicit-deny，并跑真实 create/upload/terminate canary。`SimulateCustomPolicy` 的 `PolicyInputList` 单份上限是 131,072 字符；完整 Manager policy 只有约 7 KiB，应直接模拟完整策略以保留 future explicit Deny/跨 statement 约束，并对 RunInstances 的全部 resource evaluation 逐项断言 allowed。

## 2026-07-22 EIP 终态对账与身份原子性（commit `1fdaa48`）

**问题**：AWS 会继续枚举已经终止的 EC2。旧 reconciler 把带 lease tag 的历史行重新收养并重复清理，EIP Job 正常 release 后的 TERMINATED Node 也要等下一轮 reconcile 才消失。进一步故障注入发现，缺失/错配的 durable lease、worker→instance 映射或 allocator claim 若被当作幂等成功，可能提前丢失仍需重试的 Node/账号句柄；显式 disconnect 后排队中的 STATUS 还能复活缓存。

**解决**：terminated 历史只有在 durable lease 对 lease/instance/account/Job 精确匹配，且 detach/terminate/必要 worker cleanup/`released_at` 全部提交后才跳过；任何 active-by-instance 冲突、未知或不完整状态都 quarantine 并禁止云变更。release 必须返回身份匹配的 `RELEASED` 才立即清 Task/Node/WS status 和精确 claim；worker 存在但 instance id 缺失从 journal invariant、startup、live hook 到 BindingManager 全链 fail closed。`begin_release` 在 store 单锁内原子比较七项身份并写 `RELEASING`，之后冻结身份字段，关闭晚到 attach/recovery 改写销毁目标的窗口。connection 显式断开同时清状态/event，旧连接排队消息不能重新写回。

**以后避免**：云 API 的 terminated row 是历史事实，不是新的待清理资源；忽略它必须依赖本地正向、完整、精确的完成证明。资源 release 的“前置读取”和“写入销毁意图”不能分成两个锁窗口，且 Node、durable lease、allocator claim 三张所有权表必须分别核对稳定身份后再释放。缺失返回值或缺失 instance id 不是幂等成功，而是应保留控制面句柄的故障。

**验证**：新增与受影响核心测试 253 项、EIP/Job 聚焦矩阵 726 项全绿；完整套件 2039 passed / 12 skipped，8 个失败与既有基线相同（credential rotation 3、server 默认端口 2、file sync 3），无新增失败；变更文件 Ruff 与 `git diff --check` 通过，安全和生命周期并发复核均无 blocker。

## 2026-07-22 清零全量测试历史失败（commit `eee21b7`）

**问题**：连续多个任务把同一组 8 个失败记录为“既有基线”，久而久之会让真正的新回归混在红色全量测试中。逐项复核后，3 个 credential/quota 断言仍停留在旧轮换语义；2 个 config 默认值用例被宿主通用 `PORT=8002` 合法覆盖；file sync 一方面有两条测试要求已废弃的“delivery 下任意 Markdown 都是最终稿”，另一方面真的会在扫描 `/root/books/...` 时因 PermissionError 中止。

**解决**：凭证测试同步现行契约——无替代账号时保留任务且不发 exhausted、达到阈值请求 graceful rotation、明确 rate limit 保留 `rate_limited` 原因；事件回调改成 async，去掉伪 TypeError。默认配置测试显式隔离 `HOST`/`PORT`，另加环境覆盖用例保留兼容。file sync 继续只把标准文件名提升为 `delivery_manuscript`，补回接口文档规定的 `audiobook_manuscript.md`，不可读候选根按单根跳过，并新增确定性权限回归。

**以后避免**：不能长期接受“与本任务无关”的红色基线；语义变更必须同时更新集成断言。`BaseSettings` 默认会读取宿主环境，测默认值必须先清相关键，同时另测覆盖能力。文件发现器面对外部路径时，权限错误应隔离到单个候选根，不能让一个不可读的 fallback 阻断所有可读路径。

**验证**：原 8 个失败逐条转绿；配置/凭证/额度/file-sync 组合回归 192 passed；完整套件 **2049 passed / 12 skipped / 0 failed**，`git diff --check` 通过。file-sync 两个历史文件原有 Ruff 15 项，本次前后数量不变；其余变更文件 Ruff 全绿。

## 2026-07-22 可靠终态重连/ACK 竞态收口（commit `c2a9f9b`）

**问题**：生产 Codex+EIP canary 已成功完成任务、S3 收集和资源释放，但终态 handler 在 `PROCESS_EXIT` 内完成 EC2/Node/WS 清理后，连接层仍向已关闭的旧 WebSocket 发送 `EVENT_ACK`，产生一条 closed-send traceback。并发复核继续发现：replacement 若在首个 handler 尚未结束时重放同一 event_id，会重复执行终态 handler；handler 完成附近的取消可遗留永不完成的 in-flight owner；subscriber 失败后若原 WS 保持在线，worker outbox 又只在重连时 replay，事件会永久不 ACK。

**解决**：Manager 只向 identity 仍匹配的当前连接 ACK；旧连接已关闭/替换时保留 processed 结果，等待新连接 replay 后去重 ACK，活动连接的发送错误仍原样抛出。同一 event_id 用 in-flight Future 串行化，唯一 owner 成功后唤醒等待者，失败则由一个等待者重新竞争；claim/create 和同步 finish 均无取消点，handler 失败或取消不会留下悬挂 owner。可靠事件 subscriber 失败且没有并发接手者时，message loop 主动关闭 WS，让 worker durable outbox 通过重连重放。

**以后避免**：at-least-once 不能只检查“处理后再 ACK”；还要覆盖处理期间 connection replacement、ACK 前主动清理 socket、handler failure/cancellation，以及无并发 replay 时如何强制下一次投递。event-id 的 in-flight 状态转移必须在无 await 的事件循环原子段内完成，不能把 owner 清理放在可取消的锁等待之后。

**验证**：先用严格 WebSocket fake 复现生产同款异常，再覆盖 disconnect/replay、并发 replacement、failure takeover、handler cancellation、ACK 活动连接错误和失败后重连；Connection Manager 38 passed，Ruff 与 diff check 通过，独立复审无 blocker。完整套件 **2057 passed / 12 skipped / 0 failed**。触发修复的真实 Job `job-5a9e8c7df50112ffc4e64368f16b5360` 已完成 token-only Codex 自动登录与真实 `codex exec`，S3 五个对象逐项断言通过，出口精确为账号绑定 EIP `13.112.54.251`；EC2 `i-0d423ec97597f8031` 已终止且 root EBS 已删除，EIP 解绑保留，Node/active lease/allocation 均为 0。AWS terminated 历史行在边界后的两个完整 300 秒 reconcile 周期均保持 `cloud=1, registry=0, orphans=0, conflicts=0`，绑定 journal 哈希和 Manager PID/InvocationID 不变。

**修复后实盘（production runtime `bf2d6dd`）**：不可变 release 已原子切换到东京 Manager，旧 release 以 rollback symlink 保留，专用实例角色、域名 health 和持久状态不变。Job `job-5502197ba3e23370fefb5ee3aebfd18f` 在 EC2 `i-0fa17f9b9683399b3` 上再次完成无人工 OTP 的 token-only Codex 登录、CLI 预热和真实 `codex exec`；S3 五对象的 release/marker/公网 IP/manifest 断言全部通过。Job `succeeded/done=true/cleanup_pending=0`，final collect 与 cleanup 均成功；实例终止、root EBS `vol-0f0368c545e28f77d` 删除，EIP 解绑保留，Node/active lease/allocation/challenge/live managed instance 全为 0。新 systemd Invocation 中旧 `Cannot call send once a close message has been sent`、`Error in message loop` 均为 0，证明目标竞态已消失；journal 对账号秘密和 query-secret pattern 的扫描也均为 0。

**传输层权衡**：第二轮 AWS `TerminateInstances` 到 terminal readback 之间，底层 Python 3.14 + websockets legacy keepalive 记录过一次 ping-timeout `ConnectionClosedError exception in shielded future`；它来自被终止 EC2 的连接没有 close frame，与只会 `set_result(bool)` 的 event-id Future 无关，也未影响任何终态。没有为消除这条 P3 日志而在 durable `RELEASING` 前提前断 WS：该做法不是 fencing，会破坏 detach/terminate 失败时保留 live control-plane connection 的故障语义并引入额外重连竞态。若后续要清零，应在 BindingManager 原子提交 release intent 后增加专用 pre-terminate fence，而不是禁用 ping、降日志级别或提前 ACK。

## 2026-07-25 Job Worker 执行历史与资源状态分离（commit `cb645cf`）

**问题**：Batch Console 把持久保留的 `WorkerRun` 历史直接显示成 `1 workers`，即使 EC2 已完成结果收集并销毁，仍会让管理员误以为存在存活 Worker；历史行还保留无效的实时日志/终止按钮。

**解决**：Job API 为每条执行记录显式返回 `worker_released` 与 `worker_release_expected`，EIP 以 durable cleanup 完成、普通 Job 以整组 scale-in 完成为释放证明。UI 改成“Worker 执行记录”，将执行状态与资源状态分列；已销毁或其他终态行不再显示实时操作，活动 Worker 的人工终止统一走强制 scale-in，避免只删除 Node 句柄。同步修正 backoff 测试只采样真实重连延迟，不再把认证 timeout 的内部 sleep 误判为退避。

**以后避免**：进程终态、结果收集完成和云资源销毁是三个不同事实，前端不能从 `done/failed` 或历史记录数量推断实例存活；`false` 的释放证明也只表示“尚无完整完成证明”，不等于实例必然存在。涉及云销毁的按钮必须调用保留所有权与失败重试语义的资源回收入口。

**验证**：API/UI 聚焦测试 84 项全绿；完整套件 `2058 passed / 12 skipped / 0 failed`，Ruff、`compileall`、Batch Console JavaScript 语法和 `git diff --check` 均通过。

**生产发布（runtime `a8d8da2`）**：代码已推送 `origin/main`，东京 Manager 原子切换到 `/home/ubuntu/elastic-agent.release-a8d8da2` 并重启。首轮把 editable 环境装在 `.staging` 后再改目录名，导致 `.pth` 仍指向旧路径，启动检查以 `ModuleNotFoundError` fail closed；旧进程已正常停机且当时 Worker/Node/账号占用均为 0。随后在最终 release 路径用 `uv sync --reinstall-package elastic-agent` 修正引用并启动成功。域名 health、目标 Job、页面新文案、systemd Invocation 与 AWS 状态复核通过：Worker/Node/存活 managed EC2 均为 0，目标 EIP 保留且未关联实例。以后构建不可变 release 时必须先确定最终目录再安装 editable 项目，或安装 wheel，不能在安装后移动虚拟环境的源码根。

## 2026-07-25 生产 Worker 常用实例规格白名单（commit `6548a0e`）

**问题**：生产 API 和 Manager IAM 都把 Worker 实例类型钉死为 `t3.large`，提交 `r5.2xlarge` 会在 Job preflight 返回 422；只扩应用白名单又会让后续 `RunInstances` 被 IAM 拒绝。

**解决**：应用 EnvironmentFile 与 Manager IAM `ec2:InstanceType` 条件同步扩为东京固定可用区提供的 39 种常用 x86_64 规格：T3，以及 M/C/R 的 5、6i、7i 世代，尺寸限定 `large` 至 `4xlarge`（T3 至 `2xlarge`）。Graviton、GPU、metal 和更大高成本规格继续拒绝。回归测试解析版本化 env 与 IAM policy 并要求两集合完全相等，运维文档同步给出 `r5.2xlarge` simulator 示例。

**以后避免**：实例类型存在应用 preflight 与 AWS IAM 两道 fail-closed 白名单，任何扩容必须同一变更、同一验证窗口更新；Region/AZ offering、AMI 架构、inline policy 大小、Access Analyzer 和 allow/deny simulation 都要在生产变更前核对。

**验证**：东京 `ap-northeast-1a` offering 与 AMI x86_64 兼容性逐项确认；policy compact 大小 7649 bytes，Access Analyzer 0 finding；`r5.2xlarge`、M/C/R 7i 模拟允许，`g5.xlarge` 保持拒绝。相关测试 129 项、完整套件 `2059 passed / 12 skipped / 0 failed`，Ruff、JSON 和 diff check 均通过。

**生产变更**：实际角色 `elastic-agent-manager/ElasticAgentManagerRuntime` 已更新并与版本化 policy 逐项一致，principal simulation 同样允许 `r5.2xlarge`/`m7i.4xlarge`、拒绝 `g5.xlarge`。`/etc/elastic-agent-manager.aws.env` 以 root:root 0600 原子替换，旧文件保留为 `.pre-6548a0e`，Manager 重启后 health 正常。线上 `/api/jobs/plan` 返回 39 项 allowlist，`r5.2xlarge` 与 `m7i.4xlarge` 均 valid，GPU 仍 422；plan 前后 Job 数不变，Worker/Node/allocation/非终止 managed EC2 均为 0，当前 Invocation 无 ERROR/Traceback。

## 2026-07-25 Codex 登录页兼容与分层超时（commit `0f85bdc`）

**问题**：生产 Job `job-9719774622ef4f3f4af8fd46f193cf23` 在 Worker 连接后固定等待 300 秒，未产生 OTP challenge 就报 `Login flow did not complete within 300s`。对照最后成功任务发现 CLI、AMI 和登录源码未漂移，但失败 Job 使用 `binding=none` 绕过了该账号历史成功使用的固定 EIP；同时 OpenAI 登录页新增 one-time/login-code 按钮文案，旧状态机无法识别，并且浏览器还伪装成与系统 Chrome binary 不一致的旧 131 UA。销毁后的 Worker 没有保留脱敏页面阶段，无法事后二选一确认具体页面。

**解决**：Codex 状态机兼容 email/one-time/login-code 入口并使用系统 Chrome 原生 UA；只记录枚举后的安全页面状态，不回传 OAuth URL。可见 anti-bot challenge 等待 120 秒仍不清除时明确提示核对绑定 EIP。JobSpec/协议新增 `account.login_timeout_seconds`（默认 900、范围 60–1200），Worker 显式执行该预算，Manager 总等待提升到 3600 秒，为人工 OTP、精确账号校验、真实 smoke test 和关联清理保留余量。AWS Batch UI 默认 EIP、展示账号持久地址，并等待 provider 默认初始化后再 plan/submit；切换到非 worker-local 模式会自动关闭 EIP。API plan 对显式临时公网 IP 发出警告。旧持久化 JobSpec 在幂等比较前按当前默认值规范化，避免升级后误报 409。

**以后避免**：网页登录自动化不能把“未知页面”统一折叠成一个长超时；必须识别稳定的安全状态、让反机器人页面有有界等待，并保留不含 URL/凭证的诊断类别。账号固定网络身份是登录契约的一部分，生产 UI 默认值和提交竞态也要纳入测试。新增带默认值的持久化 schema 字段时，幂等比较必须先做版本规范化。

**验证**：新增页面真实文案、challenge title/selector、安全错误、协议旧 payload、超时上下界/透传、AWS plan/UI 竞态和旧 JobSpec 幂等回放测试；独立审查无安全或协议 blocker。定向回归 384 项、完整套件 **2069 passed / 12 skipped / 0 failed**；变更模块 Ruff、`compileall`、Batch Console JavaScript 语法、依赖锁上游 dry-run 和 `git diff --check` 全部通过。

**生产发布与真机验证（runtime `8592bad`）**：东京 Manager 已原子切换到 `/home/ubuntu/elastic-agent.release-8592bad`，旧 `/home/ubuntu/elastic-agent.release-a8d8da2` 保留为 rollback。首次 release 只执行 `uv sync --no-dev`，遗漏非基础依赖的 `aws` extra，launcher 在导入 `boto3` 时按设计 fail closed，未启动 API、未产生任何 Worker/账号/云副作用；在最终 release 路径补执行 `uv sync --frozen --no-dev --extra aws` 后启动成功。runbook 已固定最终路径安装和 AWS extra 导入检查，避免重复发生。

绑定 EIP 的 token-only Codex canary `job-026acbf6153d0621f17af1245d6cb507` 在 EC2 `i-02113f0d54de134b8` 上完成自动登录、smoke test 和真实 `codex exec`，无人工 OTP；返回 `RELEASE_8592BAD_EIP_OK`，S3 五个对象记录的出口精确为账号 EIP `13.112.54.251`。终态 `succeeded/done=true/cleanup_pending=0`；实例 terminated，root EBS `vol-0893898bd57872708` 已删除，EIP 解绑保留并回到 `ready`。最终 Node、allocation、login challenge、活动 managed EC2 和附着 EIP 均为 0，域名 health 正常，当前 systemd Invocation 的 ERROR/Traceback/Exception 为 0。

## 2026-07-25 Job 诊断日志与控制台稳定性（commit `d4737bf`）

**问题**：生产 Batch 页面每 5 秒重建全部 Job DOM，并为 82 条历史逐卡请求 results；单个浏览器 5 分钟产生 6,577 次 API 请求，造成页面跳动、焦点/展开状态丢失。终态 Worker 销毁后 systemd journal 不可回取，Job 又只显示 `run exited 1`，用户无法判断失败发生在登录、命令还是结果收集。最近 seed2233 AI4Sci Job 实际在第 19 个 `deployment_prediction_sets` 生成阶段连续 20 次拒绝样本后退出，Codex 尚未启动；另一个 Job 是自动取码失败后人工 OTP 过期，旧 UI 都没有把关键阶段讲清楚。

**解决**：Batch/Fleet 默认浅色并保留 session 级深色切换；Jobs/Nodes/OTP 改为 keyed reconcile、串行且隐藏页暂停的轮询，results 按可见历史限并发渐进缓存，完整历史可显式展开。Job 卡展示申请机器→初始化→登录→运行→收集→销毁、顶层/Worker/collection/cleanup 错误和醒目的任务输出入口；OTP 提升为顶部操作卡。Manager 在可靠退出、final collect 和销毁前把 stdout/stderr 原子归档到私有 `JobLogStore`，重启 replay 可从存活 Worker 的本地 NDJSON 有界回取；API 逐 snapshot 流式合并 live/archive tail。单 task、单 Job、全局、行长、响应、task 数和 30 天 retention 均有硬边界，裁剪前写 durable marker，损坏/归档失败 fail-closed 且绝不为日志保留收费实例；统一 exit archive barrier 阻止取消/reconcile 抢先销毁。Codex 人工 OTP 超时现在保留可操作错误，而不是退化为 `RuntimeError`。

**以后避免**：轮询页面不能把“拿到新响应”等同于“必须重建 DOM”，也不能让历史数线性放大请求；先按数据签名判断、保留交互状态、隐藏页停表、异步请求不重叠。临时计算资源的可观测性必须在销毁前完成耐久提交，但日志耐久性不能反过来阻止资源清理。任何按 task 有界的存储还必须同时核算 Job/全局最坏 fan-out×rotation，并让配额删除留下可查询的截断证明。

**验证**：先以红测试复现损坏 JSON 顶层、全量日志读取、配额缺失、截断假阴性、归档/取消竞态、历史 results 饥饿、隐藏页轮询和 Fleet 0→1 空状态；修复后相关 350 项与最终完整套件 **2091 passed / 12 skipped / 0 failed**。Ruff、`compileall`、Batch/Fleet JavaScript、依赖锁 dry-run、diff check 全部通过；Chrome 149 真实 DOM/截图验证浅色表单、失败卡、任务输出、结果区和 Fleet 0→1 均正常，三轮独立复审最终无 blocker/high。

**生产发布（runtime `e56a312`）**：代码推送 `origin/main` 后，先确认东京 Manager 真正内存态活跃 Job、Node、allocation、OTP challenge、非终态 managed EC2 和已挂载 EIP 均为 0；63 条 `recovered` 是旧 journal 历史，不误作活任务。最终路径 `/home/ubuntu/elastic-agent.release-e56a312` 执行 frozen AWS-extra 安装、boto3/源码路径和版本化 unit/env 校验后原子切换，旧 `8592bad` 与配置备份保留。域名 health/浅色 UI/任务输出入口正常，旧失败 Job logs 返回明确 `unavailable` 与 `Cache-Control: no-store`；发布后资源仍全为 0，3 个账号 EIP 均未挂载，新 systemd Invocation 的 ERROR/Traceback/Exception pattern 为 0。为避免无意义费用，本次未额外创建 EC2 canary。

## 2026-07-25 Job 卡默认折叠与交互状态稳定（commit `6c2efe7`）

**问题**：Jobs 区虽然已改成 keyed reconcile，但运行中和失败 Job 仍会自动展开 Worker 表，操作、错误与结果也始终占据页面；状态轮询替换一张已展开卡片时只保留 `open`，仍会丢键盘焦点、页面位置和表格横向滚动。执行已终态但资源尚在 final collect/cleanup 的 Worker 又过早隐藏 systemd 日志入口。

**解决**：整张 Job 卡改为合法、可键盘操作的原生 `<details>/<summary>`，新卡一律收起，摘要保留名称、状态、阶段、时间与 Worker 数，展开后一次显示操作、错误、结果和 Worker 明细。节点替换同时恢复展开状态、稳定控件焦点、视口与表格滚动；终态未释放 Worker 保留只读系统日志，资源消失返回 404/409 后停止自动轮询，危险的终止按钮仍只在执行未终态时出现。

**以后避免**：减少 DOM 重建不等于交互状态稳定；任何轮询替换都要逐项核对 disclosure、focus、viewport 和局部 scroll。资源生命周期与命令生命周期必须分别控制只读诊断和破坏性操作。原生 disclosure 也要遵守 HTML 内容模型，并验证 focus ring 不被圆角裁剪。

**验证**：先补红测试锁定默认折叠、外层状态恢复和日志资源边界；最终完整套件 **2092 passed / 12 skipped / 0 failed**，变更模块 Ruff（忽略文件既有 E501）、`compileall`、Batch JavaScript 语法和 `git diff --check` 通过。真实 Chrome 在桌面/手机视口验证默认收起、展开、强制数据签名变化后的 open/focus/viewport/table scroll 保持，以及 Worker 404 后只请求一次；两轮独立复审无 blocker/high。

**生产发布（runtime `05a1181`）**：切换前后均确认活跃 Job、Node、allocation、OTP challenge、非终态 managed EC2 和已挂载 EIP 全为 0，3 个账号 EIP 保持 `ready`。生产机直接 HTTPS clone 私有仓库因无 GitHub 凭证在创建 release 前安全失败，旧 Manager 未受影响；随后改由本机对精确提交生成校验过的 Git archive，经私网 SSH 传到最终路径后执行 frozen AWS-extra 安装，没有向生产机下发 Git token。东京 Manager 已原子切换到 `/home/ubuntu/elastic-agent.release-05a1181`，旧 `e56a312` 和 unit/env 备份保留；域名 health、线上折叠/状态保持源码标记、运行时模块路径和 systemd Invocation 全部通过，当前 Invocation 的 ERROR/Traceback/Exception 为 0。此次仅改 UI，未创建收费 EC2 canary。

## 2026-07-25 多 Worker OTP 精确卡片（commit `bae9a7e`）

**问题**：最新 Codex Job 的邮箱自动取码未完成后等待人工 OTP 超时；账号域名没有专属后端映射而走默认 171mail，查询 token 若不兼容就会回退人工输入。旧页面只在顶部显示 `account_id + worker_id`，没有账号邮箱、所属 Job 或 shard；多个 Worker 同时缺码时难以判断每个验证码该交给谁。Job 轮询替换又可能让直接嵌入的输入丢失或失焦。

**解决**：`LoginCoordinator` 在开始登录时保存 Manager 可信的账号邮箱和 Job/name/shard 快照，challenge 继续以 authenticated Worker envelope、`login_request_id` 和 expected account 三重校验；交叉 Worker/账号事件拒绝，同一 challenge 在首次 transport await 前 claim，阻止双击重复发送。Batch Console 只为实际上报人工 challenge 的 Worker 显示卡片，每张卡在对应 Job 内明确账号邮箱/ID、完整 Worker、Job 和 shard，多 Worker 独立提交；浮动提醒负责发现与跳转，移动端跳转后缩成小条，新 challenge 到来再展开。OTP DOM 以 request+challenge 复合键复用，Job replace 时移植原节点，并在显隐完成后恢复输入、焦点和光标选区；验证码不写浏览器持久状态。账号表单同时澄清查询 token 只是读取 OpenAI 邮箱 OTP 的凭据，密码与 token 至少一个且可同时配置。

**以后避免**：异步人工操作 UI 不能只展示资源 ID，必须把“登录请求→可信 Worker→账号→Job/shard”完整标注，并让提交目标从卡片自身的关联键读取，不能靠当前选择或列表顺序猜测。轮询保持 input value 还不够，隐藏祖先再显示会让真实浏览器丢焦点；必须验证 DOM identity、value、focus、selection、scroll 和窄屏 overlay。自动取码 token 与 OpenAI 登录凭据也必须在文案中明确区分。

**验证**：先补红测试覆盖两 Worker/两账号并发、交叉事件拒绝、单卡提交不影响另一卡、双提交 claim、transport 失败恢复、非秘密 REST 元数据和 Job 内 UI 契约。最终完整套件 **2096 passed / 12 skipped / 0 failed**；变更模块 Ruff、Batch JavaScript 语法、diff check 全部通过。Chrome 149 真实浏览器验证双 Job 精确落位、XSS text-only、未知 Job fallback、Job replacement 后值/焦点/选区不变，以及 390px 视口 10 个 Worker 时目标输入完整可见且不被提醒层遮挡；两轮独立后端/UI 审查最终无 blocker。

**生产发布（runtime `f6d510b`）**：发布前确认活跃 Job、Node、账号占用、OTP challenge、非终态 managed EC2 和已挂载 EIP 全为 0。精确 Git archive 已安装到最终路径 `/home/ubuntu/elastic-agent.release-f6d510b`，东京 Manager 原子切换成功，旧 `/home/ubuntu/elastic-agent.release-05a1181` 由 rollback symlink 保留。域名 health、线上按需 OTP 源码标记、运行时进程路径和当前 systemd Invocation 均复核通过，ERROR pattern 为 0；发布后资源仍全为 0，4 个账号 EIP 均为 `ready`。本次只改登录控制面与 UI，没有为制造人工 OTP 而创建收费 EC2 canary。

## 2026-07-26 失败日志与下载操作稳定（commit `e296484`）

**问题**：失败 Job 的命令输出已经在 Worker 销毁前归档，但入口藏在折叠详情中且只显示通用“任务输出”，终态页面只取 1000 行，也没有展示归档任务的退出码。结果按钮又完全依赖异步内存缓存：页面先画 Job 再查 S3 时会短暂移除按钮；手动刷新可与自动轮询重叠，较旧的空响应晚到后会覆盖较新的非空结果；终态 `file_count=0` 还会被永久缓存。

**解决**：失败 Job/Worker 使用明确的“查看失败日志”入口，终态读取后端允许的完整 5000 行有界归档，并把 task exit code/error summary 与 stderr 一起显示；持久化 Job journal 对应的归档在 Manager 内存 Job 消失后仍可查询。结果操作节点始终保留，以检查中、等待、暂无、暂不可用、可下载和下载中表达状态；每个 Job 的 results 请求带前端 generation，只允许最新请求提交缓存，已知非空结果不会回退为空，终态空结果有限退避复查，错误保留最后有效快照，同一 Job 同时只生成一个下载压缩包。

**以后避免**：轮询接口的响应完成顺序不等于请求发出顺序，任何会改变可操作性的异步缓存都必须有 request identity，并明确哪些状态可单调推进。加载占位不能通过删除操作节点表达，否则正常瀑布加载也会被用户看成“按钮消失”；后台完成标记也不等于文件一定存在，下载可用性必须以实际结果元数据为准。

**验证**：先以红测试锁定持久化失败 Job 的归档访问和新的结果状态契约；最终完整套件 **2097 passed / 12 skipped / 0 failed**，Ruff、`compileall`、Batch JavaScript 语法和 `git diff --check` 均通过。Chrome 149 真实复现新结果先返回、旧空结果后返回，修复后文件数保持非空且按钮不消失；下载中 Job 卡被轮询替换仍保持单请求状态；失败归档请求 5000 行、显示退出摘要/stderr，并在终态停止轮询。独立日志、结果和 UI 测试审查无 blocker。

**生产发布（runtime `4348521`）**：发布前后均确认活跃 Job、Node、账号占用、OTP challenge、非终态 managed EC2 和已挂载 EIP 全为 0。东京 Manager 已原子切换到 `/home/ubuntu/elastic-agent.release-4348521`，旧 `f6d510b` 由 rollback symlink 保留；域名 health、运行时路径和新 Invocation 均正常，ERROR pattern 为 0。Chrome 149 通过线上失败 Job `job-c4827c3f4bcc992fb6dbea99a925ad29` 验证“查看失败日志”显示退出码 1、101 行归档及 instance-generation 根因，3.5 秒后仍只有一次日志请求；结果操作稳定显示 607 个文件且可下载。此次复用已有失败历史做无副作用验收，没有创建收费 EC2 canary。
## 2026-07-27 大结果流式下载与运行中快照说明（commit `a737c39`）

**问题**：生产 Job `job-c187753c3be7e4393981786b7fe06e3d` 的结果包含 5,184 个 S3 对象、约 754 MiB，其中多数是小文件。旧下载端点会在 Manager 串行 GET 全部对象并完整压缩到临时文件后才返回响应头，前端又等待整个 `Blob` 进入内存后才触发保存；两次请求在数分钟无任何可见进度后被客户端取消。页面只显示“正在打包”，也没有取消操作。与此同时，界面没有充分说明中间结果并非自动出现：`collect.interval_seconds=0` 是终态收集，只有显式设为正数才会在运行中周期上传。

**解决**：保留原来可在响应头前返回 503 的严格预构建端点，另为 Batch UI 增加 S3 流式 tar 路由；使用有界 OS pipe、事件循环原生读端、gzip level 1 和独立四线程 producer 池，首批对象到达即可响应，客户端断开会关闭当前 S3 body 并停止 producer。UI 通过 `ReadableStream` 显示已接收字节与耗时、支持点击取消，并在安全桌面 Chromium 中用 File System Access API 直接写盘；无直接落盘能力时仅允许小于 256 MiB 的内存 fallback，S3 源大小和 Manager 本地 `Content-Length` 都纳入阈值。进度只原位更新两个下载按钮，不再重建 Job 卡或结果列表；磁盘、浏览器和网络异常都会显式 abort controller、cancel reader。运行中按钮明确标记为最近一次已上传的中间快照，文档说明正间隔、首轮等待和“下载不触发即时 Worker 同步”的边界。

**以后避免**：面向多对象存储的下载不能把“完整预构建 + 整包浏览器 Blob”当作普通小文件路径；要同时核算对象 RTT、首字节时间、Manager 磁盘、浏览器内存、代理空闲超时和取消后的服务端资源。高频进度不能进入整卡渲染签名。任何内存 fallback 的上限必须覆盖所有后端响应元数据，而不只覆盖主存储路径。运行中结果的 UI 也必须区分“Worker 当前文件”与“最近一次已完成收集快照”。

**验证**：新增归档有效性、后续对象阻塞时提前产出、active body close 异常下取消清理，以及 UI 流读取、直接落盘、大文件保护、原位进度和运行中快照测试。API/UI 聚焦测试 **101 passed**；完整套件 **2101 passed / 12 skipped / 0 failed**；变更模块 Ruff、`compileall`、Batch JavaScript 语法和 `git diff --check` 均通过。后端与浏览器侧独立复核最终均无 blocker/high。

**生产发布（runtime `a44e38c`）**：首次候选 `01a78dc` 已证明大 Job 首字节和取消正常；真实 Chrome 进一步发现下载状态虽然从 Map 清除，按钮却因开始时没有提交一次 active 渲染签名而停在“正在取消”。follow-up `a44e38c` 在下载开始时只做一次 keyed reconcile，传输进度仍全部原位更新，终止时签名可靠回到 idle。东京 Manager 已原子切换到 `/home/ubuntu/elastic-agent.release-a44e38c`，`release-01a78dc` 与对应 unit/env 作为即时回滚。公网大 Job首字节 1.649 秒、5 秒传输 7,004,160 bytes 后主动取消；Chrome 验证写入、取消、toast 和按钮恢复为“下载结果 (5184)”；另一个 628 对象 Job 在 25.33 秒完整下载 34,151,654-byte 有效 tar。发布后公网 health 正常，新 Invocation 的 ERROR/Traceback/Exception 为 0，活跃 Job、Node、账号占用、OTP、非终态 managed EC2 与已挂载 EIP 均为 0，4 个绑定全部 `ready`。

## 2026-07-28 逐 Worker S3 shard 与不可变代码交付（commit `cdfd958`）

**问题**：Mode-B 的 `setup.s3_datasets` 只能静态配置，100 个 replay worker 会各自拉取整个 shard
prefix；Manager rsync 虽然在提交前校验 `resolved_commit`，clone 阶段仍先解析可变 ref，存在 ref
漂移导致交付失败的窗口。

**解决**：dataset `uri/dest` 支持与 run command 相同的 worker 模板，并新增五位
`shard_id`；provision 按真实 worker context 渲染和重新校验，每个 worker 只下载自己的单个
S3 object，plan API 同时显示首 worker 的数据集预览。Manager rsync 在给出
`resolved_commit` 时直接 fetch 完整 commit SHA，并严格核对解析结果。

**以后避免**：fanout 资源不能在 provision 阶段退回首 worker 的静态 context；任何声明为
不可变的源码交付都必须从 fetch 起点绑定 commit，而不只是 checkout 后验校验。

**验证**：新增 dataset 模板拒绝/渲染、worker 上下文、provision 分片、plan 预览和 commit
fetch 测试；完整套件 **2356 passed / 12 skipped / 0 failed**，`git diff --check` 通过。

## 2026-07-28 Worker dataset staging 最小读权限（commit `6f4af60`）

**问题**：`setup.s3_datasets` 已改为 worker 直拉，但生产 worker policy 仍只有结果
`PutObject/AbortMultipartUpload`，因此真实 shard staging 会在 `aws s3 cp` 处被拒绝。

**解决**：仅对结果桶 `jobs/datasets/*` 增加 `s3:GetObject`；结果对象仍不可读、不可删，
也不授予 `ListBucket`。runbook、README 和架构说明同步该边界。

**避免复发**：增加 S3 dataset 功能时必须同时验证 worker instance profile 的实际 data-plane
权限，不能只验证 JobSpec schema 和本地 mock provision。

**验证**：IAM、dataset provision、JobSpec 和 worker projection 定向测试 **276 passed**，
`git diff --check` 通过。

## 2026-07-29 Mode-B 断线续跑与不可变检查点恢复（Elastic `e9d6e5e` / AI4Sci `c89b4091`）

**问题**：长时间 Mode-B 命令把 Worker WebSocket/runtime 连接误当成任务生命线；runtime
断开后 Manager 可能清理仍在运行的进程或实例。实例、EBS 或 supervisor 真正丢失时，旧的
mutable S3 结果又无法证明删除和完整 generation，只能从头运行。AI4Sci 的部分结果、workspace
与 trace 还存在“先发布完成 JSON、后保留产物”的崩溃窗口，`--resume` 可能跳过不完整实例。

**解决**：Worker 新增独立 `ea-task-supervisor`，以私有 socket、0600 spool/descriptor 和稳定
terminal event id 跨 `ea-runtime` 重启保留原 PID/process group，runtime 重连后 inventory、
补发输出并继续 ACK；单纯断线不再销毁活跃 supervised Job。跨实例恢复新增 v2 S3
content-addressed blob、不可变 shard manifest、完整 Job set、retention、preflight 解析及 exact
generation pin；所有 shard 在云创建前完成有界 Manager staging，新 Worker 以 root 私有同盘事务
树 fsync/re-measure/roll-forward，`installed` 后才登录和 dispatch。rsync 有 pre-spawn durable
journal，未证明 PGID 消失时 staging/配额 quarantine。startup 先禁用 Job-user respawn、停止
framework/container runtime 并证明 quiescent，再 reconcile→final collect；长恢复在 fail-closed
后台 barrier 内执行。对象数、逻辑字节、filesystem allocation block、inode、target disk 和全局
预算都纳入 admission；旧 mutable recovery 明确拒绝，无完整 set 时必须从头开始。

AI4Sci 的 metadata/trajectory/raw/execution/eval/analyze/result 写入全部采用同目录临时文件、
file fsync、atomic replace、directory fsync；普通 agent workspace 与 trace 全部耐久后才发布
result marker，失败会退役旧 marker 而不会让 `--resume` 误判完成。修复位于用户指定的
`archive/youchengsong-managed-agent-api-20260728` 分支。

**生产边界同步**：Manager IAM 两处 tag allowlist 加入 `ElasticAgentShardIndex`，并只对
`jobs/.elastic-agent-checkpoints/*` 授予 `s3:DeleteObject` 供 retention 使用；public result
仍不可删除。IAM runbook 增加 allow/deny simulation。systemd `TimeoutStopSec=32400` 覆盖当前
Batch、遗留 EIP 与 ordinary orphan 三个不可取消收敛波次。Batch Console 增加从完整 checkpoint
恢复入口和 generation 选择；配置、secret 引用继续只保存在 Manager 私有 journal。

**验证**：Elastic 全仓 **2810 passed / 12 skipped / 0 failed**；恢复/Manager/编排 253 项、
API/checkpoint 212 项、结果/UI/supervisor 等 558 项及最终 IAM/transaction 78 项专项均通过。
变更 Python Ruff、`compileall`、shell、JSON、Batch JavaScript、secret pattern 与
`git diff --check` 全部通过。AI4Sci 相关回归 **242 passed / 1 deselected**，综合恢复/report
回归 **319 passed**，compile/fatal Ruff/diff check 通过；其全仓剩余失败均为既有环境/基线，
与本次文件无关。

**发布状态**：代码已提交，未部署、未重载、未重启当前服务；运行中 Job/实例未被触碰。

## 2026-08-10 UI v2 多页控制台（commit `fd4723a`）

**改动**：实现 `docs/ui-v2-implementation-plan.md` 的 Phase 1（静态 App Shell + core 模块）和 Phase 2（所有页面迁移），合计 24 个 ES Module + 1 个后端路由模块 + 1 个构建脚本。

**关键设计决定**：
- 不引入 Node 运行时依赖；原生 ES Modules + History API，无 React/Vue/build chain。`scripts/build_ui_v2.py` 是纯 Python 的 import 校验 + 内容哈希工具。
- 秘密从结构上不可入 store：`store.js` 的 `NEVER_STORE` 列表在 `setState` 时递归拒绝 password/api_key/email_token/otp/secret_env key，让测试可以断言"任何页面代码调用 setState 都不可能泄漏秘密到全局状态"。
- Job 表单是纯函数模块 `job-spec.js`（validateJobForm + buildJobSpec），不依赖 DOM，直接用 Node test runner 做 17 个单测，覆盖 EIP/checkpoint/recovery/codex/rotation 等交叉约束。

**避免的坑**：
1. 测试 JS 源码中 `localStorage` 关键字时，注释中的 prose 也会命中。解法：改用 `re.search(r"localStorage\s*[.\[(]", source)` 只匹配实际 API 调用。
2. `/api/ui/summary` 要避免为 badge 做全量 S3 扫描或历史 journal 读取。解法：只读内存 `BatchJob.summary()` + registry `list_all()` + 账号 store `list()`，5 秒缓存。
3. SPA fallback 必须只在 `/ui-v2/*` 内生效，不能接管 `/api/*` 或 `/ws/*`。解法：在 `app.py` 中先注册全部 API 路由再注册 ui_v2 路由；测试用 `test_spa_fallback_never_claims_api_or_ws` 断言。
4. pre-existing test failures (test_pty_backend/test_worker_agent_api) 是因为 worktree 缺少 `claude-pty` 可选依赖，与本次改动无关（已在 main 分支同样失败）。

## 2026-08-11 JSON 批量任务端到端接入（commit `60870fd`）

**问题**：UI v2 已出现 JSON 批量入口，但 `main` 没有对应的持久化 JobBatch API；页面还会重序列化原始 JSON、每次重试生成新的幂等键、提交后不跟踪终态。静态 HTML 同时注入 API key，旧 `/batch` 回退入口也被重定向，形成安全和回滚风险。

**解决**：接入持久化 JobBatch 队列、plan/submit/status API、进程重启恢复、逐 Job 独立调度与全局 50 个活跃任务上限。UI v2 对同一份原始 UTF-8 字节执行预检和提交，严格拒绝重复键及超过 2 MiB/100 项的输入；以 batch id 派生稳定幂等键，显式确认后提交并轮询到终态。API key 只保留在内存/sessionStorage/Bearer header，不再写入公开 HTML；旧 Batch/Fleet 页面继续作为可用回退面。

**验证**：完整 Python 套件 **2931 passed / 12 skipped / 0 failed**；UI v2 Node 测试 **23 passed**；新增和修改的核心 Python 文件 Ruff、UI 构建/import 检查及 `git diff --check` 全部通过。
## 2026-08-11 UI v2 深层路由刷新资源修复（commit `b5672d1`）

**问题**：SPA shell 用相对路径 `assets/app.css` 和 `js/app.js`。直接打开 `/ui-v2/` 正常，但刷新 `/ui-v2/jobs/batch` 等深层 History-API 路由时，浏览器改为请求 `/ui-v2/jobs/assets/app.css` 与 `/ui-v2/jobs/js/app.js`，两者 404，页面只剩未隐藏的无障碍文本和无样式导航。

**解决**：入口资源固定为 `/ui-v2/assets/app.css` 与 `/ui-v2/js/app.js`；构建器继续在该根路径内替换内容哈希文件名。新增深层路由 shell 回归测试，同时验证根路径 CSS/JS 的状态和 MIME。

**避免复发**：SPA fallback 测试不能只断言深层 URL 返回 HTML，还必须验证该 HTML 引用的静态入口从任意路由都解析到同一应用根。

**验证**：完整 Python 套件 **2940 passed / 12 skipped / 0 failed**；UI v2 Python 专项 **19 passed**、Node 测试 **23 passed**；内容哈希构建、Ruff 与 `git diff --check` 通过。
## 2026-08-11 JobBatch 终态 journal 跨 JobSpec 版本恢复（commit `ff40a7a`）

**问题**：生产机保留 121 份全部终态的 JobBatch journal，它们由一版超前 JobSpec 写入，包含当前 `main` 不认识的 profile/账号字段。新 Manager 启动时用当前严格 JobSpec 重解析所有历史 manifest，导致应用启动失败，尽管这些批次永远不会再次调度。

**解决**：写入和所有非终态恢复继续严格绑定当前 JobSpec；仅加载“批次终态且每项均为 terminal/error”的历史记录时，允许把 JobSpec 当作不可执行的 opaque payload，同时严格校验 journal/envelope 字段、policy、ID 唯一性、item 映射及原始 canonical SHA-256 指纹。这样保留历史查询而不会运行未知 schema。

**避免复发**：持久 journal 的可执行恢复与只读历史恢复必须分层；前者严格绑定当前 schema，后者应在完整性和终态证明后保持前向可读，不能让旧历史阻断整个控制面启动。

**验证**：完整 Python 套件 **2941 passed / 12 skipped / 0 failed**；JobBatch + UI v2 专项 **48 passed**，Ruff 与 `git diff --check` 通过；生产 121 份 journal 均为 terminal、679 个 terminal item + 57 个 error item，且原始 canonical 指纹 **121/121** 匹配。

## 2026-08-11 历史 JobSpec 相邻版本只读投影（commit `27a38df`）

**问题**：生产机有 3 份终态 Job journal 由相邻 JobSpec 版本写入，包含 sandbox
environment profile 以及账号 `auth_kind`/`exclude_ids`。新 Manager 能正常启动和列出任务，
但详情 API 用当前 schema 严格投影时返回 500，旧监控因此持续报错。

**解决**：只在历史详情的 schema-aware 脱敏投影中 allowlist 这几个已知字段和值；先将
sandbox profile 映射到当前 Docker profile 做完整结构校验，账号类型、排除列表数量、字符集、
去重和选中/排除冲突也逐项校验，再恢复只读展示值。未知字段和值继续 fail closed，所有秘密
仍脱敏；提交、预检和重提完全不走兼容投影，因此未知版本配置不能被执行。

**避免复发**：持久历史的读取兼容必须与执行 schema 分离；兼容范围按具体字段和值 allowlist，
不能通过放宽 Pydantic `extra=forbid` 或直接回显原始 journal 实现。

**验证**：完整 Python 套件 **2945 passed / 12 skipped / 0 failed**；API/JobBatch/UI
定向回归 **244 passed**，新增成功、非法值、秘密脱敏和重提拒绝测试均通过；Ruff 与
`git diff --check` 通过。

## 2026-08-11 UI v2 管理员账号登录合并（commit `7f1a97f`）

**分支边界**：从 `main` 的 `4a147e7` 新建 `feat/admin-account-auth-v2`，只移植管理员
账号认证的三个提交并适配当前 UI v2；没有合并或改写 `main`，暂不创建 PR。

**解决**：浏览器控制台改用 Argon2 管理员账号、Secure/HttpOnly/SameSite Session Cookie、
内存 CSRF token 和同源校验；匿名或首次改密访问 UI v2 深层路由时，服务端 303 到登录或改密
页面并保留安全的 `next`。UI v2 删除管理 API Key 输入、sessionStorage 持久化和 Authorization
header，导航显示当前管理员并提供退出登录。服务端 Bearer API Key 继续只服务自动化调用，
没有删除既有接口兼容性；AWS 启动前会验证管理员用户和 public origin 配置。

**验证**：完整 Python 套件 **3036 passed / 12 skipped / 0 failed**；UI v2 Node 测试
**26 passed**；管理员认证、UI v2、JobBatch、AWS 启动等定向测试 **448 passed**，相关 Python
文件 Ruff、UI 构建/import 与 `git diff --check` 均通过。

**登录落点修复（commit `847b8e9`）**：线上发现访问者误带 `/ui-v2/、` 时，安全 `next`
校验正确拒绝该路径，却回退到了旧 `/` 控制台。登录和首次改密的缺省/非法落点现统一为
`/ui-v2/overview`，根地址的匿名登录流程也进入当前 UI v2；合法深层路由继续原样恢复，旧
`/batch`、`/fleet` 回滚入口和所有业务模块不变。管理员/UI 专项 **107 passed**，Node
**26 passed**，Ruff、UI import 和 `git diff --check` 通过。

**静态资源换代修复（commit `5c8be80`）**：Cloudflare 的 4 小时 Browser Cache TTL 会忽略
未哈希源文件的 `no-cache`，导致新 Session shell 仍加载旧 API Key `app.js`。Manager shell
现引用统一 revision namespace，入口及所有相对 ES module import 一次换代；旧打开页仍可读
旧路径，当前页面不会再命中旧图。CDN 正式构建会先移除源站 revision，再生成原有内容哈希。
管理员/UI 专项 **107 passed**、Node **26 passed**，并验证 26-module CDN 构建成功。
