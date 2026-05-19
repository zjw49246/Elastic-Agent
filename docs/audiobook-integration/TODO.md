# Audiobook x Elastic-Agent 全量 TODO

> 本文件汇总四个仓库的全部待办事项。每个 TODO 标注所属仓库和优先级。
>
> **仓库缩写：**
> - **[EA]** = [Elastic-Agent](https://github.com/zjw49246/Elastic-Agent) — 通用弹性计算框架
> - **[ABS]** = [audio_book_echo_agent](https://github.com/zjw49246/audio_book_echo_agent) — Audiobook Agent Service
> - **[ABE]** = [audio_book_echo_editor](https://github.com/zjw49246/audio_book_echo_editor) — 现有做书前后端
> - **[插件]** = audiobook-nonfiction（需新建 GitHub 仓库）— Claude Code 有声书插件

---

## 0. 仓库初始化 — [audio_book_echo_agent](https://github.com/zjw49246/audio_book_echo_agent)

- [ ] **S-001** [ABS] 清理现有 audio_book_echo_agent 仓库的旧代码（backend/、frontend/、worker/、tests/、infra/、README.md、SOLUTION.md、TEST.md、TODO.md、claude-auto-account-switching.md、pyproject.toml、uv.lock），保留 CLAUDE.md 和 .gitignore
- [ ] **S-002** [ABS] 新建项目脚手架（pyproject.toml、目录结构、CI 基础），通过 `uv add git+https://github.com/zjw49246/Elastic-Agent.git` 引入框架
- [ ] **S-003** [ABS] 更新 CLAUDE.md / 编写 README.md，引用本方案文档

---

## 1. Elastic-Agent 框架 — [Elastic-Agent](https://github.com/zjw49246/Elastic-Agent)

### P0 — 必须完成

- [ ] **T-001** [EA] 项目脚手架搭建（pyproject.toml、目录结构、CI 基础）  `01 §9`
- [ ] **T-002** [EA] CloudProvider 抽象基类 + Instance/InstanceConfig 数据模型  `01 §3.1`
- [ ] **T-049** [EA] Harness 抽象基类 + 相关接口定义（WorkerCapacity, FileSyncConfig, SyncMapping, WorkerLifecycle, BootstrapStep, ScalingSignal）  `05 §5.2`
- [ ] **T-003** [EA] 阿里云 ECS Provider（alibabacloud SDK V2.0 直连）  `01 §3.1`
- [ ] **T-004** [EA] AWS EC2 Provider（boto3 SDK 直连）  `01 §3.1`
- [ ] **T-005** [EA] 阿里云前置准备文档 + 验证脚本（VPC/VSwitch/安全组/密钥对，控制台手动创建）  `01 §4`
- [ ] **T-006** [EA] AWS 前置准备文档 + 验证脚本（VPC/Subnet/SG/KeyPair，控制台手动创建）  `01 §4`
- [ ] **T-007** [EA] Worker Runtime 服务端（进程执行、日志双写落盘、文件操作）  `01 §3.2`
- [ ] **T-008** [EA] Worker Runtime 客户端（Manager 侧远程调用抽象）  `01 §3.2`
- [ ] **T-009** [EA] Manager ↔ Worker 通信协议（WebSocket 反向连接 + 消息类型）  `01 §3.2`
- [ ] **T-010** [EA] Manager ↔ Worker 认证（per-Worker Bearer Token）  `01 §3.6`
- [ ] **T-011** [EA] NodeRegistry（节点状态持久化，JSON 文件 + 线程安全锁）  `01 §3.5`
- [ ] **T-012** [EA] 云端标签对账（启动时 + 周期性扫描，清理孤儿实例）  `01 §3.5`
- [ ] **T-013** [EA] 外部服务 API — 实时轨迹流（WebSocket + SSE 双通道）  `01 §3.3`
- [ ] **T-014** [EA] 外部服务 API — 文件变更通知（inotify → WebSocket 事件推送）  `01 §3.3`
- [ ] **T-015** [EA] 外部服务 API — 认证（API Key Bearer Token）  `01 §3.3`
- [ ] **T-016** [EA] Manager FastAPI 服务骨架 + 节点管理 REST API  `01 §2.1`
- [ ] **T-017** [EA] Claude Code AgentType（安装命令、启动命令、健康检查探针）  `01 §3.5`
- [ ] **T-030** [EA] FileSyncManager — Worker 侧文件主动同步到 OSS/S3  `01 §3.4`
- [ ] **T-031** [EA] FileSyncManager — Harness 配置接口  `01 §3.4`
- [ ] **T-032** [EA] FileSyncManager — Worker 侧云存储凭证注入  `01 §3.4`
- [ ] **T-033** [EA] 外部服务 API — 文件内容从云存储读取  `01 §3.3`
- [ ] **T-034** [EA] TaskSyncMapper — Worker 侧动态同步映射  `01 §3.4`
- [ ] **T-035** [EA] REGISTER/UNREGISTER_SYNC_MAPPING 协议消息  `01 §3.2`
- [ ] **T-036** [EA] FileSyncManager 上传错误处理（重试、分片、缓冲）  `01 §3.4`
- [ ] **T-037** [EA] FILE_SYNCED 事件类型  `01 §3.2`
- [ ] **T-038** [EA] Worker 进程日志落盘（stdout/stderr 双写到本地 NDJSON 文件）  `01 §3.2`
- [ ] **T-039** [EA] Manager 结构化操作日志（JSON Lines，全部关键操作）  `01 §3.8`
- [ ] **T-040** [EA] LOG 事件结构化解析（NDJSON → typed event）  `01 §3.3`

**凭证管理（自动登录 + 额度监控 + 自动轮换）：**

- [ ] **T-026** [EA] CredentialPool 账号池管理（accounts.json 加载、pool_status.json、分组、分配/回收）  `01 §3.6`
- [ ] **T-041** [EA] ClaudeOAuthProvider 自动登录（14 步 OAuth：171mail + Playwright + mitmproxy）  `01 §3.6`
- [ ] **T-042** [EA] Worker Bootstrap 登录步骤（为每个 Slot 执行自动登录，串行，失败回滚）  `01 §3.6`
- [ ] **T-043** [EA] Worker 侧额度监控（每 60s 调用 usage API，Token 续期，QUOTA_STATUS 上报）  `01 §3.6`
- [ ] **T-044** [EA] Manager 侧 QuotaMonitor（汇聚额度数据，阈值检测，QUOTA_WARNING 事件）  `01 §3.6`
- [ ] **T-045** [EA] 自动轮换（等待任务完成 → 分配新账号 → 登录/分发 → 恢复槽位）  `01 §3.6`
- [ ] **T-046** [EA] 冷却恢复（5h 窗口到期后自动标记 available）  `01 §3.6`
- [ ] **T-047** [EA] CREDENTIAL_LOGIN / QUOTA_STATUS / CREDENTIAL_ROTATE 协议消息  `01 §3.6`
- [ ] **T-048** [EA] 账号-Worker 绑定（IP 亲和性：互斥分配、亲和复用、max_accounts_per_worker 限制）  `01 §3.6`

### P1 — 应该完成

- [ ] **T-018** [EA] Bootstrap Pipeline（可插拔步骤、per-step 超时、失败策略枚举）  `01 §3.5`
- [ ] **T-019 ~ T-022** [EA] 内置 Bootstrap 步骤（系统初始化/Agent 安装/Runtime 部署/Harness 代码）  `01 §3.5`
- [ ] **T-023** [EA] Bootstrap 失败处理（terminate-retry / retry-from-failed / leave-for-debug）  `01 §3.5`
- [ ] **T-024** [EA] Worker 多层健康检查（L1 VM + L2 Runtime + L3 Agent 进程）  `01 §3.5`
- [ ] **T-025** [EA] 优雅缩容 Drain  `01 §3.5`
- [ ] **T-028** [EA] 手动扩缩容 API（scale_out / scale_in / remove_node）  `01 §6`
- [ ] **T-029** [EA] 基础 Web UI（节点列表、状态卡片、手动操作）  `01 §6`

### 测试 — 单元测试

- [ ] **T-100** [EA] CloudProvider mock：create/terminate/list/wait_until_running 接口行为
- [ ] **T-101** [EA] NodeRegistry CRUD + 并发安全 + JSON 持久化 + 崩溃恢复
- [ ] **T-102** [EA] Protocol 消息序列化/反序列化（全部消息类型）
- [ ] **T-103** [EA] Bootstrap 状态机：步骤成功/失败/超时/重试/回滚
- [ ] **T-104** [EA] Drain 状态机：draining→等待完成→终止、超时强制终止
- [ ] **T-105** [EA] CloudReconciler：孤儿检测、幽灵清理、状态不一致修复
- [ ] **T-106** [EA] 轨迹缓冲：per-task 写入/读取/溢出/释放
- [ ] **T-107** [EA] EventBus：fan-out 分发、subscribe/unsubscribe、事件过滤
- [ ] **T-108** [EA] CredentialPool：分配/回收/轮换/额度检查/分组（high_quota/standard）
- [ ] **T-109** [EA] Config 加载：config.yaml 解析 + 环境变量覆盖 + Pydantic 校验
- [ ] **T-119** [EA] FileSyncManager：防抖逻辑 + 同步清单生成 + 大小文件分流
- [ ] **T-121** [EA] Worker 日志落盘：正常退出/崩溃退出场景下文件完整性
- [ ] **T-123** [EA] TaskSyncMapper：注册/注销映射、路径匹配、多任务并存
- [ ] **T-124** [EA] LOG 事件结构化解析：各 type 正确提取 parsed 字段、非 JSON 行容错
- [ ] **T-125** [EA] Manager 操作日志：日志格式、轮转、各操作类别正确记录
- [ ] **T-126** [EA] Worker 断线重连：指数退避、日志缓冲、重连后回放
- [ ] **T-127** [EA] 外部 API 认证：有效/无效/过期 API Key
- [ ] **T-128** [EA] Spot/抢占式实例处理：回收事件检测 + 状态更新
- [ ] **T-132** [EA] CredentialPool：accounts.json 加载 / 分组 / 分配 / 回收 / pool_status 持久化
- [ ] **T-133** [EA] QuotaMonitor：阈值检测 / QUOTA_WARNING 事件 / 冷却恢复
- [ ] **T-134** [EA] 自动轮换逻辑：查找替代账号 / 等待任务完成 / 凭证切换 / 所有账号耗尽处理
- [ ] **T-137** [EA] 账号-Worker 绑定：互斥分配 / 亲和复用（Step 1-3）/ Worker 下线清理 / max_accounts_per_worker

### 测试 — 集成测试

- [ ] **T-110** [EA] Manager ↔ Worker WS 通信：连接/认证/双向消息/断线重连
- [ ] **T-111** [EA] 阿里云全生命周期：创建 → Bootstrap → 就绪 → 执行 → 终止
- [ ] **T-112** [EA] AWS 全生命周期：创建 → Bootstrap → 就绪 → 执行 → 终止
- [ ] **T-113** [EA] Bootstrap E2E：全步骤执行 + 单步失败重试 + 凭证回收
- [ ] **T-114** [EA] 外部 API E2E：轨迹流订阅 + 文件读取 + 认证
- [ ] **T-115** [EA] 扩容 → 执行命令 → 获取输出 → 缩容 全链路
- [ ] **T-116** [EA] 阿里云前置资源验证：安全组规则 + VSwitch 连通性 + 密钥对 SSH 可达
- [ ] **T-117** [EA] AWS 前置资源验证：Security Group + Subnet + Key Pair
- [ ] **T-118** [EA] DryRunProvider 空跑验证
- [ ] **T-120** [EA] Worker 文件变更 → FileSyncManager → OSS/S3 → 外部 API 读取
- [ ] **T-122** [EA] Worker 日志落盘 → FileSyncManager → OSS → 历史查询
- [ ] **T-129** [EA] Manager 崩溃恢复：重启 → NodeRegistry 重建 → Worker 重连 → 状态一致
- [ ] **T-130** [EA] 凭证轮换 E2E：额度耗尽 → 自动换号 → 进程使用新凭证
- [ ] **T-131** [EA] 多 Worker 并发：5+ Worker 同时连接 + 并发执行命令
- [ ] **T-135** [EA] 自动登录 E2E：171mail + Playwright + mitmproxy → credentials.json 生成
- [ ] **T-136** [EA] 额度监控 E2E：Worker 上报 → Manager 汇聚 → 阈值告警 → 触发轮换

### 测试工具（`elastic_agent.testing` 导出，供下游仓库使用）

- [ ] **T-200** [EA] DryRunProvider 实现（模拟云 API，内存实例生命周期）  `06 §2.1`
- [ ] **T-201** [EA] MockWorker 实现（模拟 Worker Runtime WS 连接，可配置 NDJSON 输出）  `06 §2.1`
- [ ] **T-202** [EA] MockOAuthServer 实现（模拟 171mail + Anthropic OAuth + Usage API）  `06 §2.1`
- [ ] **T-203** [EA] MockOSS 实现（本地文件系统模拟 OSS PutObject/GetObject）  `06 §2.1`
- [ ] **T-204** [EA] create_test_manager() 工厂函数（一键创建带全部 Mock 的测试 Manager）  `06 §2.1`
- [ ] **T-205** [EA] LocalWorkerProcess（本地启动真实 Worker Runtime 进程用于 L3+ 测试）  `06 §5.2`
- [ ] **T-206** [EA] 测试级别框架（conftest.py: 环境变量驱动的 provider/worker/oss fixture 切换）  `06 §5.2`
- [ ] **T-207** [EA] pytest marker 注册（level0 ~ level6，CI 按凭证可用性选择执行范围）  `06 §8`
- [ ] **T-208** [EA] `elastic_agent.testing` 包导出（__init__.py 统一导出全部 Mock 工具）  `06 §2.3`

---

## 2. Audiobook Agent Service — [audio_book_echo_agent](https://github.com/zjw49246/audio_book_echo_agent)

### 仓库初始化

- [ ] **A-000** [ABS] 清理旧代码（删除 audio_book_echo_agent 仓库中旧文件：backend/、frontend/、worker/、tests/、infra/、README.md、SOLUTION.md、TEST.md、TODO.md、claude-auto-account-switching.md、pyproject.toml、uv.lock，保留 CLAUDE.md、.git/ 和 .gitignore）
- [ ] **A-001** [ABS] 项目脚手架（新建 pyproject.toml（依赖 elastic-agent），src/ 目录结构，CI 配置）
- [ ] **A-002** [ABS] 配置模型（AudiobookServiceConfig Pydantic 模型，环境变量 + config.yaml 支持）

### 核心模块

- [ ] **A-003** [ABS] FastAPI 应用入口（main.py: 组装 ElasticAgentManager + 挂载 Audiobook 路由）  `02 §0.2`
- [ ] **A-010** [ABS] AudiobookHarness — 实现 Harness 接口  `02 §4.1`
- [ ] **A-011** [ABS] Bootstrap 步骤定义（Node.js/Claude Code/凭证/插件/Runtime）  `02 §4.1`
- [ ] **A-012** [ABS] BookQueue — 做书请求排队 + 优先级调度  `02 §3.2`
- ~~**A-013**~~ 已移至框架: EA T-050 (TaskRegistry)
- ~~**A-014**~~ 已移至框架: EA T-051 (TaskScheduler)
- ~~**A-015**~~ 已移至框架: EA T-052 (TaskRouter)
- [ ] **A-016** [ABS] TaskSyncMapper 映射推送 — 注册/注销同步映射到 Worker  `02 §3.4`
- ~~**A-017**~~ 已移至框架: EA T-053 (WebhookEmitter)
- [ ] **A-018** [ABS] 进度超时检测 — 30 分钟无 LOG/FILE 事件标记 stalled  `02 §5.7`
- [ ] **A-019** [ABS] Session ID 多源提取（stream-json parsed / 目录扫描 / state.json）  `02 §5.8`
- [ ] **A-020** [ABS] Retry/Continue 编排（OSS 恢复 workspace、清理 Phase 产物、重跑）  `02 §5`
- [ ] **A-021** [ABS] Worker 目录生命周期管理（磁盘监控 + 过期清理 + 手动清理 API）  `02 §5.6`

### API 端点

- [ ] **A-030** [ABS] POST /api/tasks/produce — 提交做书请求  `05 §2.2`
- [ ] **A-031** [ABS] GET /api/tasks/{id}/status — 查询任务状态  `05 §2.3`
- [ ] **A-032** [ABS] POST /api/tasks/{id}/cancel — 取消任务  `05 §2.4`
- [ ] **A-033** [ABS] POST /api/tasks/{id}/continue — 续跑任务  `05 §2.5`
- [ ] **A-034** [ABS] POST /api/tasks/{id}/retry — 重试任务  `05 §2.6`
- [ ] **A-035** [ABS] POST /api/tasks/{id}/chat — 发送修改指令  `05 §2.7`
- [ ] **A-036** [ABS] GET /api/tasks/{id}/chat/live — 实时聊天轮询（从 OSS logs 增量读取）  `05 §2.8`
- [ ] **A-037** [ABS] GET /api/tasks/{id}/chat/history — 聊天历史（从 OSS logs 解析）  `05 §4.5`
- [ ] **A-038** [ABS] POST /api/tasks/{id}/files/sync — 强制文件同步  `05 §2.9`
- [ ] **A-039** [ABS] GET /api/workers — Worker 列表 + 槽位状态  `05 §2.10`

### 配置

- [ ] **A-050** [ABS] Audiobook Agent Service config.yaml 定义（见本文件 §5）
- [ ] **A-051** [ABS] 敏感配置环境变量定义（见本文件 §5）

### 测试 — 单元测试

- [ ] **A-100** [ABS] BookQueue：入队/出队/优先级排序/持久化/空队列
- ~~**A-101**~~ 已移至框架: EA T-138 (TaskRegistry 测试)
- ~~**A-102**~~ 已移至框架: EA T-139 (TaskScheduler 测试)
- [ ] **A-103** [ABS] WebhookEmitter：重试延迟策略/死信队列/幂等 event_id/HMAC 签名
- [ ] **A-104** [ABS] Session ID 多源提取：stream-json parsed / 目录扫描 / state.json 回退
- [ ] **A-105** [ABS] 并发修改互斥：同一 task 二次修改返回 409
- ~~**A-106**~~ 已移至框架: EA T-140 (TaskRouter 测试)
- [ ] **A-107** [ABS] 进度超时检测：正常任务不告警 / 超时任务标记 stalled / 自动 SIGINT
- [ ] **A-108** [ABS] Retry/Continue 编排：Phase 清理逻辑 / OSS workspace 恢复 / state.json 回退
- [ ] **A-109** [ABS] 凭证隔离：CLAUDE_CONFIG_DIR 按 slot 分配 / 修改流程重注册 sync mapping
- [ ] **A-114** [ABS] 配置加载：config.yaml 解析 + 环境变量覆盖 + 必填校验
- [ ] **A-115** [ABS] Phase 检测：state.json phase 数字 → Webhook phase 字符串映射
- [ ] **A-116** [ABS] Worker 目录清理：磁盘阈值触发 / 不活跃天数归档 / 手动清理 API

### 测试 — 集成测试

- [ ] **A-110** [ABS] 单 Worker 端到端做书（DryRunProvider）：produce → 执行 → 完成 → Webhook
- [ ] **A-111** [ABS] 修改模式：produce → 完成 → chat → --resume → 文件同步 → Webhook
- [ ] **A-112** [ABS] 多 Worker 队列分发：3 Worker + 5 任务 → 负载均衡 → 全部完成
- [ ] **A-113** [ABS] Webhook 发送 + 重试：正常发送 / 目标 503 重试 / 死信队列
- [ ] **A-117** [ABS] Audiobook Agent Service 崩溃恢复：重启 → 框架 TaskRegistry 自动恢复 → 任务继续
- [ ] **A-118** [ABS] API 端点完整测试：10 个端点全覆盖（produce/status/cancel/continue/retry/chat/chat-live/history/files-sync/workers）
- [ ] **A-119** [ABS] 从指定 Phase 重试 E2E：retry from_phase=3 → 清理 → /continue-book → 完成
- [ ] **A-120** [ABS] 修改流程 sync mapping 生命周期：production → unregister → edit → re-register → edit complete → unregister

### 测试工具（`audiobook_agent_service.testing` 导出）

- [ ] **A-200** [ABS] MockClaudeOutput 实现（预录 NDJSON 序列：成功/失败/卡住/修改等场景）  `06 §3.1`
- [ ] **A-201** [ABS] WebhookCatcher 实现（捕获 Webhook 事件，支持 wait_for_event + assert_sequence）  `06 §3.1`

---

## 3. audio_book_echo_editor — [audio_book_echo_editor](https://github.com/zjw49246/audio_book_echo_editor)

### 数据模型

- [ ] **B-001** [ABE] Task 表增加 script_generation_backend 字段 + 迁移  `03 §4.1`
- [ ] **B-002** [ABE] 新增 elastic_book_runs 表 + 迁移  `03 §4.2`
- [ ] **B-003** [ABE] 新增 elastic_book_run_events 表 + 迁移  `03 §4.3`

### 后端服务

- [ ] **B-010** [ABE] ElasticAgentClient — HTTP 客户端封装  `03 §5.8`
- [ ] **B-011** [ABE] ElasticBookProductionService — 组装请求 + 提交 Elastic  `03 §5.8`
- [ ] **B-012** [ABE] WebhookService — 验签、幂等、状态更新、回灌 AgentOutput  `03 §5.7`
- [ ] **B-013** [ABE] OssFileService — 读取 manifest、最终稿、预签名 URL  `03 §5.6`
- [ ] **B-014** [ABE] AgentOutput 回灌 — elastic_audiobook + final_proofreading 双写  `03 §4.4`
- [ ] **B-015** [ABE] 轮询补偿 — 定时扫描 running 状态的 elastic_book_runs  `03 §5.10.5`

### API 端点

- [ ] **B-020** [ABE] GET /api/tasks/script-generation-backends  `03 §5.1`
- [ ] **B-021** [ABE] POST /api/tasks/ — 增加 script_generation_backend 字段  `03 §5.2`
- [ ] **B-022** [ABE] POST /api/tasks/batch — 增加 script_generation_backend 字段  `03 §5.2`
- [ ] **B-023** [ABE] GET /api/tasks/{id}/script-production — 统一状态查询  `03 §5.3`
- [ ] **B-024** [ABE] POST /api/tasks/{id}/script-production/cancel  `03 §5.4`
- [ ] **B-025** [ABE] POST /api/tasks/{id}/script-production/continue  `03 §5.4`
- [ ] **B-026** [ABE] POST /api/tasks/{id}/script-production/retry  `03 §5.4`
- [ ] **B-027** [ABE] POST /api/tasks/{id}/script-production/chat  `03 §5.5`
- [ ] **B-028** [ABE] GET /api/tasks/{id}/script-production/chat/history  `03 §5.5`
- [ ] **B-029** [ABE] GET /api/tasks/{id}/script-production/chat/live — 聊天轮询（从 OSS 增量读取）  `03 §5.11`
- [ ] **B-030** [ABE] GET /api/tasks/{id}/script-production/files  `03 §5.6`
- [ ] **B-031** [ABE] GET /api/tasks/{id}/script-production/files/{path}  `03 §5.6`
- [ ] **B-032** [ABE] GET /api/tasks/{id}/script-production/manuscript  `03 §5.6`
- [ ] **B-033** [ABE] POST /api/elastic-agent/webhook — 接收回调  `03 §5.7`
- [ ] **B-034** [ABE] 创建任务后按 backend 分叉调度  `03 §6.1`

### 前端

- [ ] **B-040** [ABE] 创建任务弹窗 — 增加跑书方式选择  `03 §7.1`
- [ ] **B-041** [ABE] TaskDetail — 按 backend 展示不同 UI  `03 §7.3`
- [ ] **B-042** [ABE] Elastic phase 时间线 + 进度条  `03 §7.3`
- [ ] **B-043** [ABE] Elastic 文件列表 + 预览  `03 §7.3`
- [ ] **B-044** [ABE] Elastic chat 修改界面  `03 §7.3`
- [ ] **B-045** [ABE] 任务列表 — 增加 backend 筛选  `03 §7.4`
- [ ] **B-046** [ABE] API SDK 增加 Elastic 相关方法  `03 §7.2`

### 配置

- [ ] **B-050** [ABE] backend config.py 增加 ELASTIC_AGENT_* 配置  `03 §8`

### 测试 — 单元测试

- [ ] **B-100** [ABE] ElasticAgentClient：produce/cancel/retry/continue/chat/status 全接口 mock 测试
- [ ] **B-101** [ABE] WebhookService 验签：有效签名通过 / 无效签名拒绝 / 过期时间戳拒绝
- [ ] **B-102** [ABE] WebhookService 幂等：同一 event_id 重复处理返回 200 不重复写入
- [ ] **B-103** [ABE] WebhookService 状态映射：每种 event_type → Task.status / script_status / current_step 正确映射
- [ ] **B-104** [ABE] WebhookService sequence 排序：乱序事件忽略 / gap 检测触发补偿
- [ ] **B-105** [ABE] OssFileService manifest 解析：正常解析 / 空 manifest / 字段缺失容错
- [ ] **B-106** [ABE] OssFileService 最终稿选择：delivery > compliant > final 优先级 / role 匹配 / path 回退
- [ ] **B-107** [ABE] OssFileService 预签名 URL 生成：path 必须在 manifest 中（防越权）
- [ ] **B-108** [ABE] AgentOutput 回灌：elastic_audiobook + final_proofreading 双写 / 重复回灌 update 不 insert
- [ ] **B-109** [ABE] ElasticBookProductionService：从 Task+Book 组装请求体 / book_slug 生成 / 大文本走 OSS URI
- [ ] **B-110** [ABE] 创建任务分叉：legacy_ai_service → TaskService / elastic_agent → ElasticBookProductionService
- [ ] **B-111** [ABE] 轮询补偿：running 状态 > 5min 无事件 → 触发状态查询 → 更新本地状态
- [ ] **B-112** [ABE] 状态映射完整性：Elastic queued/dispatching/running/completed/failed/cancelled → Task 状态正确映射
- [ ] **B-113** [ABE] TaskService 防御：elastic_agent 任务误入 legacy pipeline 抛异常

### 测试 — 集成测试

- [ ] **B-120** [ABE] Webhook 全流程：收到 queued→started→phase.changed→completed → 状态逐步更新 → AgentOutput 写入
- [ ] **B-121** [ABE] 修改 Webhook 流程：edit.started → edit.completed → AgentOutput 更新
- [ ] **B-122** [ABE] OSS 文件读取 E2E：manifest 解析 → 文件内容读取 → 预签名 URL → 内容校验
- [ ] **B-123** [ABE] 创建 Elastic 任务 E2E：POST /tasks → ElasticBookProductionService → elastic_book_runs 记录创建
- [ ] **B-124** [ABE] script-production API 全覆盖：status/cancel/continue/retry/chat/files/manuscript 端点
- [ ] **B-125** [ABE] 数据库迁移测试：新增字段 + 新增表 + 现有数据不受影响

### 测试 — 前端

- [ ] **B-130** [ABE] 创建任务弹窗：跑书方式切换 → 配置项联动显示/隐藏
- [ ] **B-131** [ABE] TaskDetail 双引擎切换：legacy 展示 Agent 输出 / Elastic 展示 phase+chat+files
- [ ] **B-132** [ABE] Elastic chat 界面：发送消息 → 显示回复 → 修改完成
- [ ] **B-133** [ABE] 任务列表筛选：按 script_generation_backend 过滤

### 测试工具（`tests/mocks/`）

- [ ] **B-200** [ABE] MockAgentService 实现（模拟 ABS 全部 API 端点，可配置返回值）  `06 §4.1`
- [ ] **B-201** [ABE] WebhookSimulator 实现（向 ABE 发送带签名的模拟 Webhook 事件）  `06 §4.1`
- [ ] **B-202** [ABE] MockOSSReader 实现（预置 manifest + 文件内容，模拟 OSS 读取）  `06 §4.1`

---

## 4. 跨仓库集成测试

- [ ] **I-001** [EA + ABS] Audiobook Agent Service 使用 Elastic-Agent 框架创建 Worker + Bootstrap
- [ ] **I-002** [ABS + ABE] audio_book_echo_editor 提交做书 → Audiobook Agent Service 执行 → Webhook 回调
- [ ] **I-003** [EA + ABS + ABE] 全链路：前端提交 → Elastic 做书 → OSS 同步 → Webhook → 回灌 AgentOutput → 审核
- [ ] **I-004** [ABS + ABE] 修改流程：chat → --resume → 文件同步 → Webhook → 更新 AgentOutput
- [ ] **I-005** [ABS] 多 Worker 负载均衡 + 队列分发
- [ ] **I-006** [ABS] Worker 故障 → 告警 → 手动恢复

---

## 4.5 audiobook-nonfiction 插件（需新建 GitHub 仓库）

> audiobook-nonfiction 是 Claude Code 的 Skill 插件，实现 10 Phase 有声书生产流水线。
> 当前代码尚未开源，需要创建独立的 GitHub 仓库，并根据 Elastic-Agent 方案做适配。

- [ ] **P-000** [插件] 创建 GitHub 仓库 `audiobook-nonfiction`，初始化项目结构（CLAUDE.md、README.md）
- [ ] **P-001** [插件] state.json 增加 session_id 字段（做书完成时写入，作为 session_id 提取的冗余来源）  `04 §3.12`
- [ ] **P-002** [插件] delivery/ 目录结构标准化（确保文件名为 audiobook_manuscript.md / audiobook_delivery.zip）  `04 §3.12`
- [ ] **P-003** [插件] 确认 /audiobook 命令参数与 Worker Runtime EXECUTE 命令格式兼容  `04 §3.12`
- [ ] **P-004** [插件] state.json phase 字段值与方案中的 phase 枚举一致（0~9 + DELIVERED）  `05 §4.4`

---

## 5. 配置变量完整清单

### 可配置数量速查表

| 参数 | 默认值 | 配置位置 | 仓库 | 说明 |
|---|---|---|---|---|
| 每 Worker 生产槽位 | 1 | ABS `config.yaml` → `audiobook.max_production_slots` | ABS | 同时做几本新书 |
| 每 Worker 修改槽位 | 3 | ABS `config.yaml` → `audiobook.max_edit_slots` | ABS | 同时修改几本书 |
| 每 Worker 最大账号数 | 4 | EA `config.yaml` → `credentials.max_accounts_per_worker` | EA | 同时登录的 Claude 账号数 |
| 全局最大实例数 | 30 | EA `config.yaml` → `provider.aliyun.max_instances` | EA | 最多创建多少台 Worker |
| 额度告警阈值 | 85% | EA `config.yaml` → `credentials.quota_threshold` | EA | 5h 窗口使用率 |
| 做书超时 | 7200s | ABS `config.yaml` → `audiobook.claude_code.production_timeout` | ABS | 单本书最长时间 |
| 修改超时 | 1800s | ABS `config.yaml` → `audiobook.claude_code.edit_timeout` | ABS | 单次修改最长时间 |
| 进度超时 | 1800s | ABS `config.yaml` → `audiobook.progress_timeout` | ABS | 无输出多久判定卡死 |
| 缩容等待超时 | 3600s | EA `config.yaml` → `drain.timeout` | EA | 缩容前等当前任务 |
| 账号间检查延迟 | 170-190s | EA `config.yaml` → `credentials.quota_check_delay_min/max` | EA | 单账号周期≈N×180s |
| 单步 Bootstrap 超时 | 300s | EA `config.yaml` → `bootstrap.default_step_timeout` | EA | |
| 自动登录超时 | 240s | EA `config.yaml` → `credentials.login_timeout` | EA | 单次 OAuth 流程 |

### 5.1 Elastic-Agent 框架 [EA]

**配置文件 `config.yaml`：**

```yaml
server:
  host: "0.0.0.0"
  port: 8000

provider:
  type: "aliyun"                           # "aliyun" | "aws" | "dryrun"
  aliyun:
    region_id: "cn-hangzhou"
    image_id: "m-bp1xxxx"                  # 自定义镜像 ID
    instance_type: "ecs.c6.large"
    security_group_id: ""                  # 阿里云控制台创建
    vswitch_id: ""                         # 阿里云控制台创建
    key_pair_name: "elastic-agent-key"     # 阿里云控制台创建
    ssh_key_path: "~/.ssh/elastic-agent-aliyun.pem"
    max_instances: 30
    spot_enabled: false                    # 是否使用抢占式实例
  aws:
    region: "ap-northeast-1"
    ami_id: "ami-xxxxx"                    # 自定义 AMI ID
    default_instance_type: "t3.large"
    security_group_ids: []                 # AWS 控制台创建
    subnet_id: ""                          # AWS 控制台创建
    key_pair_name: "elastic-agent-key"     # AWS 控制台创建
    ssh_key_path: "~/.ssh/elastic-agent-aws.pem"
    max_instances: 30

worker:
  ssh_user: "root"                         # 阿里云 root, AWS ubuntu
  runtime_port: 8080
  heartbeat_interval: 30                   # 秒
  unhealthy_threshold: 3                   # 连续 N 次心跳超时标记 unhealthy

bootstrap:
  default_step_timeout: 300                # 单步默认超时（秒）
  max_retries: 2                           # Bootstrap 失败最大重试次数
  failure_strategy: "terminate_and_retry"  # terminate_and_retry | retry_from_failed | leave_for_debug

credentials:
  accounts_file: "~/.elastic-agent/accounts.json"     # 账号池定义
  pool_status_file: "~/.elastic-agent/pool_status.json" # 运行时状态（框架自动维护）
  quota_threshold: 0.85                    # 5h 额度使用率告警阈值
  quota_check_delay_min: 170               # 账号间检查最小延迟（秒，防反检测）
  quota_check_delay_max: 190               # 账号间检查最大延迟（秒，随机化）
  weekly_reserve_per_day: 0                # 7d 每天预留百分比（0=不启用）
  rotation_strategy: "least_used_first"    # 轮换策略：least_used_first | round_robin
  login_timeout: 240                       # 单次自动登录超时（秒）
  max_accounts_per_worker: 4               # 每 Worker 最大同时登录账号数
  login_dependencies:                      # Worker 上自动登录的依赖（Bootstrap 时安装）
    - playwright
    - playwright-stealth
    - mitmproxy
    - chrome

external_api:
  enabled: true
  trace_buffer_size: 5000                  # per-task 实时缓冲条数

logging:
  operations_log: "~/.elastic-agent/operations.log"
  log_level: "INFO"                        # DEBUG | INFO | WARNING | ERROR
  rotation: "daily"
  retention_days: 30
  worker_process_log_dir: "logs/"          # Worker 进程日志（相对于任务工作目录）

monitor:
  health_check_interval: 30                # 秒
  reconcile_interval: 300                  # 云端对账间隔（秒）

drain:
  timeout: 3600                            # 缩容等待超时（秒），Audiobook 生产可达 2h

registry:
  path: "~/.elastic-agent/registry.json"
```

**敏感配置（仅通过环境变量，不写入配置文件）：**

| 环境变量 | 说明 | 示例 |
|---|---|---|
| `ALICLOUD_ACCESS_KEY_ID` | 阿里云 RAM 子账号 AccessKey ID | `LTAI5t...` |
| `ALICLOUD_ACCESS_KEY_SECRET` | 阿里云 RAM 子账号 AccessKey Secret | `HBYwH...` |
| `AWS_ACCESS_KEY_ID` | AWS IAM 用户 Access Key | `AKIA...` |
| `AWS_SECRET_ACCESS_KEY` | AWS IAM 用户 Secret Key | `wJalr...` |
| `AWS_SESSION_TOKEN` | 可选，使用 STS 临时凭证时 | |
| `ELASTIC_AGENT_EXTERNAL_API_KEYS` | 外部 API 认证密钥（逗号分隔多个） | `key-frontend,key-monitoring` |

### 5.2 Audiobook Agent Service [ABS]

**配置文件 `config.yaml`：**

```yaml
# 引入 Elastic-Agent 框架配置
elastic_agent:
  config_path: "./elastic-agent-config.yaml"    # 或内联框架配置

# Audiobook 业务配置
audiobook:
  max_production_slots: 1                       # 每 Worker 最大生产槽位
  max_edit_slots: 3                             # 每 Worker 最大修改槽位
  progress_timeout: 1800                        # 进度超时（秒），默认 30 分钟
  # task_registry 由框架自动管理，路径在 EA config.yaml 的 registry.path 配置
  book_queue_path: "~/.elastic-agent/book_queue.json"

  claude_code:
    command: "claude"
    default_flags:
      - "--dangerously-skip-permissions"
      - "--output-format"
      - "stream-json"
    production_timeout: 7200                    # 生产超时（秒），默认 2 小时
    edit_timeout: 1800                          # 修改超时（秒），默认 30 分钟

  credential_isolation:
    production_config_dir: "/root/.claude-prod"
    edit_config_dir_template: "/root/.claude-edit-{slot_index}"

  workspace_cleanup:
    disk_warning_threshold: 0.80                # 磁盘使用率告警
    disk_cleanup_threshold: 0.90                # 磁盘使用率触发清理
    inactive_days_before_archive: 7             # 不活跃天数后归档到 OSS

webhook:
  targets: []                                   # 运行时通过 produce 请求的 callback 字段动态注册
  retry_delays: [1, 5, 30, 300, 1800]          # 重试延迟（秒）
  send_timeout: 10                              # 发送超时（秒）
  dead_letter_log: "~/.elastic-agent/webhook_dead_letters.json"

oss:
  default_bucket: ""                            # 默认 OSS bucket（可被 produce 请求覆盖）
  endpoint: ""                                  # 阿里云 OSS endpoint
```

**敏感配置（仅通过环境变量）：**

| 环境变量 | 说明 | 示例 |
|---|---|---|
| `ABS_OSS_ACCESS_KEY_ID` | OSS 写入凭证 | `LTAI5t...` |
| `ABS_OSS_ACCESS_KEY_SECRET` | OSS 写入凭证 | `HBYwH...` |
| `ABS_WEBHOOK_SECRETS` | Webhook 验签密钥映射（JSON） | `{"default": "hmac-secret-xxx"}` |

### 5.3 audio_book_echo_editor [ABE]

**后端环境变量（在 backend/app/config.py 中定义）：**

| 环境变量 | 默认值 | 说明 |
|---|---|---|
| `ELASTIC_AGENT_ENABLED` | `false` | 是否启用 Elastic-Agent 引擎 |
| `ELASTIC_AGENT_MANAGER_URL` | (必填) | Audiobook Agent Service 地址 |
| `ELASTIC_AGENT_API_KEY` | (必填) | 调用 Agent Service 的 Bearer Token |
| `ELASTIC_AGENT_WEBHOOK_SECRET` | (必填) | 验证 Webhook 签名的密钥 |
| `ELASTIC_AGENT_DEFAULT_PERSONA` | `nonfiction_default` | 默认 Audiobook persona |
| `ELASTIC_AGENT_DEFAULT_TARGET_PCT` | `12` | 默认压缩比例 |
| `ELASTIC_AGENT_REQUEST_TIMEOUT_SECONDS` | `30` | 调用 Agent Service 超时 |
| `ELASTIC_AGENT_OSS_BUCKET` | (必填) | Elastic 产物 OSS bucket |
| `ELASTIC_AGENT_OSS_PREFIX` | `elastic-agent/` | OSS 路径前缀（不含 tasks/） |
| `ELASTIC_AGENT_OSS_ENDPOINT` | (必填) | 阿里云 OSS endpoint（如 `oss-cn-shanghai.aliyuncs.com`） |
| `ELASTIC_AGENT_POLL_INTERVAL_SECONDS` | `300` | Webhook 补偿轮询间隔 |

> 注：audio_book_echo_editor 读取 OSS 文件时复用项目已有的 OSS 凭证（同一阿里云账号），如果 bucket 不同则需额外配置 `ELASTIC_AGENT_OSS_ACCESS_KEY_ID` / `SECRET`。

### 5.4 Worker 侧配置（Bootstrap 时自动注入）

以下配置由 Elastic-Agent 框架在 Bootstrap 过程中自动写入 Worker，不需要手动配置：

| 配置项 | 写入位置 | 来源 |
|---|---|---|
| Worker Runtime 连接地址 | `/etc/elastic-agent/runtime.yaml` | Manager 地址 + per-Worker token |
| Claude Code 凭证 | `/root/.claude-prod/.credentials.json` 等 | CredentialPool 分配 |
| OSS 写入凭证 | 环境变量 `OSS_ACCESS_KEY_ID` / `SECRET` | Bootstrap 注入 |
| audiobook-nonfiction 插件 | `~/.claude/commands/` | Bootstrap 步骤安装 |
| 应用凭证（Git key 等） | Harness 声明的路径 | get_app_credentials() |

---

## 6. 开发阶段依赖

```
Phase A (Week 1-2):  EA T-001~T-006, T-011, T-012
Phase B (Week 2-3):  EA T-007~T-010, T-016
                     ABS A-000, A-001, A-002          ← ABS 仓库清理 + 脚手架
Phase C (Week 3-4):  EA T-030~T-040, T-013~T-015
                     ABS A-010~A-016                  ← 需要框架 Phase B 完成
Phase D (Week 4-5):  EA T-017~T-023, T-026, T-041~T-048
                     ABS A-017~A-021, A-030~A-039     ← 需要框架 Phase C 完成
                     ABE B-001~B-003, B-050           ← 数据模型可先行
Phase E (Week 5-6):  EA T-024~T-029
                     ABE B-010~B-034                  ← 需要 ABS API 稳定
Phase F (Week 6-7):  ABE B-040~B-046                  ← 前端
                     跨仓库集成测试 I-001~I-006
Phase G (Week 7-8):  EA T-100~T-137, T-200~T-208
                     ABS A-100~A-120, A-200~A-201
                     ABE B-100~B-133, B-200~B-202
                     全量测试 + 修复
```
