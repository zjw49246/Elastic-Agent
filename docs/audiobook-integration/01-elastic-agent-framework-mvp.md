# Elastic-Agent MVP 详细实现计划（Audiobook Integration 更新版）

> **本文档是 [archive/mvp-plan.md](archive/mvp-plan.md) 的更新版本，针对 Audiobook Harness 集成场景做了修正。**
> 修正内容来自 [04-gap-analysis.md](04-gap-analysis.md) 中识别的 Gap，包括：
> - FileSyncManager 动态映射（TaskSyncMapper）
> - 新增通信协议消息类型（REGISTER_SYNC_MAPPING / UNREGISTER_SYNC_MAPPING / FILE_SYNCED）
> - OSS 上传错误处理策略
> - _sync_manifest.json 格式变更
> - 多 Slot 凭证隔离
> - Harness 级状态持久化
>
> 未修改的部分与原文档保持一致。
>
> **核心策略：** 阿里云优先、SDK 直连管理实例、IaC 管理基础网络（阿里云 Terraform / AWS CDK）、外部服务 API 暴露实时数据。

---

## TODO 清单

### P0 — 必须完成

- [ ] **T-001** 项目脚手架搭建（pyproject.toml、目录结构、CI 基础）
- [ ] **T-002** CloudProvider 抽象基类 + Instance/InstanceConfig 数据模型
- [ ] **T-003** 阿里云 ECS Provider（alibabacloud SDK V2.0 直连）
- [ ] **T-004** AWS EC2 Provider（boto3 SDK 直连）
- [ ] **T-005** IaC — 阿里云基础网络（Terraform: VPC/VSwitch/安全组/密钥对/NAT）
- [ ] **T-006** IaC — AWS 基础网络（CDK Python: VPC/Subnet/SG/KeyPair/NAT）
- [ ] **T-007** Worker Runtime 服务端（Worker 侧：进程执行、日志流、文件操作）
- [ ] **T-008** Worker Runtime 客户端（Manager 侧：远程调用抽象）
- [ ] **T-009** Manager ↔ Worker 通信协议（WebSocket 反向连接 + 消息类型）
- [ ] **T-010** Manager ↔ Worker 认证（per-Worker Bearer Token）
- [ ] **T-011** NodeRegistry（节点状态持久化，JSON 文件 + 线程安全锁）
- [ ] **T-012** 云端标签对账（启动时 + 周期性扫描，清理孤儿实例）
- [ ] **T-013** 外部服务 API — 实时轨迹流（WebSocket + SSE 双通道）
- [ ] **T-014** 外部服务 API — 文件变更通知（inotify → WebSocket 事件推送）
- [ ] **T-015** 外部服务 API — 认证（API Key Bearer Token）
- [ ] **T-030** FileSyncManager — Worker 侧文件主动同步到 OSS/S3（inotify 监听 + 分层防抖 + 同步清单）
- [ ] **T-031** FileSyncManager — Harness 配置接口（`get_file_sync_config()`: 监听路径、同步目标、防抖策略）
- [ ] **T-032** FileSyncManager — Worker 侧云存储凭证注入（Bootstrap 时配置 OSS/S3 access）
- [ ] **T-033** 外部服务 API — 文件内容从云存储读取（代理 OSS/S3 或返回预签名 URL，附带 synced_at 元数据）
- [ ] **T-034** TaskSyncMapper — Worker 侧动态同步映射（接收 Manager 下发的 per-task 映射，替代静态 path_mapping）
- [ ] **T-035** REGISTER_SYNC_MAPPING / UNREGISTER_SYNC_MAPPING 协议消息（Manager→Worker 动态注册/注销同步映射）
- [ ] **T-036** FileSyncManager 上传错误处理（重试、分片上传、本地缓冲）
- [ ] **T-037** FILE_SYNCED 事件类型（区别于 FILE_CHANGE，确认文件已同步到 OSS）
- [ ] **T-038** Worker 进程日志落盘（stdout/stderr → 本地 NDJSON 文件 + LOG 事件双写）
- [ ] **T-039** Manager 结构化操作日志（JSON Lines 格式，覆盖扩缩容/Bootstrap/对账/凭证/Webhook 全部关键操作）
- [ ] **T-040** LOG 事件结构化解析（解析 Claude Code NDJSON 为 typed event，支持按 type 过滤和 token 统计）
- [ ] **T-016** Manager FastAPI 服务骨架 + 节点管理 REST API
- [ ] **T-017** Claude Code AgentType（安装命令、启动命令、健康检查探针）

### P1 — 应该完成

- [ ] **T-018** Bootstrap Pipeline（可插拔步骤、per-step 超时、失败策略枚举）
- [ ] **T-019 ~ T-022** 内置 Bootstrap 步骤（系统初始化 / Agent 安装 / Runtime 部署 / Harness 代码）
- [ ] **T-023** Bootstrap 失败处理（terminate-retry / retry-from-failed / leave-for-debug）
- [ ] **T-024** Worker 多层健康检查（L1 VM + L2 Runtime + L3 Agent 进程）
- [ ] **T-025** 优雅缩容 Drain（draining 标记 → 等待完成 → 回收凭证 → 终止）
- [ ] **T-026** 凭证分发（API Key 方式，Bootstrap 注入环境变量 / .credentials.json）
- [ ] **T-027** 基础额度监控（轮询 Worker 侧 quota 状态，阈值告警）
- [ ] **T-028** 手动扩缩容 API（scale_out / scale_in / remove_node）
- [ ] **T-029** 基础 Web UI（节点列表、状态卡片、手动操作）

### 测试

- [ ] **T-100 ~ T-109** 单元测试（Provider mock / Registry CRUD / Protocol 序列化 / Bootstrap 状态机 / Drain 状态机 / 对账逻辑 / 轨迹流过滤 / 文件监听）
- [ ] **T-110 ~ T-115** 集成测试（Manager↔Worker WS 通信 / 阿里云全生命周期 / AWS 全生命周期 / Bootstrap E2E / 外部 API E2E / 扩容→执行→缩容全链路）
- [ ] **T-116** IaC 测试 — 阿里云 Terraform plan+apply+destroy
- [ ] **T-117** IaC 测试 — AWS CDK synth+deploy+destroy
- [ ] **T-118** DryRunProvider 空跑验证
- [ ] **T-119** 单元测试：FileSyncManager 防抖逻辑 + 同步清单生成
- [ ] **T-120** 集成测试：Worker 文件变更 → OSS/S3 同步 → 外部 API 读取一致性
- [ ] **T-121** 单元测试：日志落盘完整性（进程正常退出 + 崩溃退出场景）
- [ ] **T-122** 集成测试：Worker 日志落盘 → FileSyncManager → OSS → 外部 API 读取一致性

---

## 1. 设计原则与约束

### 1.1 核心原则

| 原则 | 说明 | 对设计的影响 |
|------|------|-------------|
| **SDK 直连** | 实例生命周期由 Python 代码通过云 SDK 管理，不引入 ASG/Lambda 等额外云服务 | 所有编排逻辑集中在 Manager 进程内，调试路径短 |
| **控制面单进程** | MVP 阶段 Manager 是单个 FastAPI 进程 | 不需要分布式协调，但需要考虑崩溃恢复（操作日志 + 标签对账） |
| **数据面随动** | 日志/文件传输通道复用 Worker Runtime 的 WebSocket 连接 | 不引入消息队列（SQS/RabbitMQ），减少外部依赖 |
| **Harness 无感** | 框架提供完整的 Worker Runtime 和外部 API，Harness 只定义 Bootstrap 步骤和事件回调 | Harness 不需要自建 Worker 侧服务 |
| **故障收敛** | 任何中间状态都能通过「标签对账 + 注册表重建」恢复到一致 | 不需要分布式事务 |

### 1.2 MVP 接受的已知限制

| 限制 | 影响范围 | 何时解决 |
|------|---------|---------|
| Manager 单点 | Manager 挂了无法创建/销毁 Worker，但已有 Worker 继续运行 | Phase 6（高可用） |
| SSH 密钥共享 | 所有 Worker 共用一个密钥对 | Phase 2（SSM/云助手） |
| 文件系统状态 | 注册表 JSON 文件在 50+ 实例时可能瓶颈 | Phase 6（PostgreSQL） |
| 手动扩缩容 | 需要通过 API/UI 触发 | Phase 2（规则引擎） |
| 无 mTLS | Manager↔Worker 通信靠 Token 而非证书 | Phase 6 |

---

## 2. 系统架构

### 2.1 运行时拓扑

```
                     ┌─────────── 外部服务 / 前端 ──────────┐
                     │  WS /api/external/traces/stream      │
                     │  GET /api/external/files/{path}       │
                     │  GET /api/external/cluster/status     │
                     └───────────────┬───────────────────────┘
                                     │ HTTPS (API Key 认证)
                                     ▼
┌────────────────────────────────────────────────────────────────────┐
│                        Manager 节点                                │
│                                                                    │
│  ┌─ FastAPI 进程 ─────────────────────────────────────────────┐   │
│  │                                                             │   │
│  │  ┌───────────────┐    ┌───────────────┐   ┌─────────────┐  │   │
│  │  │ 节点管理 API   │    │ 外部服务 API  │   │ Harness     │  │   │
│  │  │ /api/nodes/*  │    │ /api/external │   │ 回调        │  │   │
│  │  └───────┬───────┘    └───────┬───────┘   └──────┬──────┘  │   │
│  │          │                    │                   │         │   │
│  │  ┌───────▼────────────────────▼───────────────────▼──────┐  │   │
│  │  │                    事件总线 (EventBus)                  │  │   │
│  │  │  NODE_READY · WORKER_UNHEALTHY · CREDENTIAL_ROTATED   │  │   │
│  │  │  LOG · FILE_CHANGE · PROCESS_EXIT · HEARTBEAT         │  │   │
│  │  └───────┬────────────────────────────────────────────────┘  │   │
│  │          │                                                   │   │
│  │  ┌───────▼───────────────────────────────────────────────┐   │   │
│  │  │              ElasticAgentManager                       │   │   │
│  │  │                                                       │   │   │
│  │  │  CloudProvider   NodeRegistry    BootstrapPipeline     │   │   │
│  │  │  (SDK 直连)      (JSON 文件)     (可插拔步骤)          │   │   │
│  │  │                                                       │   │   │
│  │  │  CredentialPool  HealthChecker   DrainManager          │   │   │
│  │  │  (账号池)        (多层探针)      (优雅缩容)            │   │   │
│  │  │                                                       │   │   │
│  │  │  CloudReconciler                                      │   │   │
│  │  │  (标签对账)                                            │   │   │
│  │  └───────┬───────────────────────────────────────────────┘   │   │
│  └──────────┼──────────────────────────────────────────────────┘   │
└─────────────┼─────────────────────────────────────────────────────┘
              │
              │ WebSocket 反向连接（Worker 主动连 Manager）
              │ 每条连接承载：命令下发 + 日志回传 + 文件操作 + 心跳
              │
    ┌─────────┼──────────┬──────────────────┐
    │         │          │                  │
┌───▼───┐ ┌──▼────┐ ┌───▼───┐         ┌───▼───┐
│Worker │ │Worker │ │Worker │  ...     │Worker │
│  #1   │ │  #2   │ │  #3   │         │  #N   │
│       │ │       │ │       │         │       │
│ WR ◄──┤ │ WR    │ │ WR    │         │ WR    │  WR = Worker Runtime
│ ┌───┐ │ │       │ │       │         │       │
│ │CC │ │ │       │ │       │         │       │  CC = Claude Code
│ └───┘ │ │       │ │       │         │       │      (或其他 Agent)
│ ┌───┐ │ │       │ │       │         │       │
│ │H  │ │ │       │ │       │         │       │  H  = Harness 代码
│ └───┘ │ │       │ │       │         │       │
└───────┘ └───────┘ └───────┘         └───────┘
 阿里云ECS   阿里云ECS   AWS EC2          ...
```

### 2.2 数据流

系统中有四条主要数据流，全部复用同一条 WebSocket 连接：

```
数据流 ①：命令下发（低频，关键路径）
  Manager → Worker
  Execute / Stop / ReadFile / WatchFiles / HealthCheck

数据流 ②：日志回传（高频，大流量）
  Worker → Manager → EventBus → 外部 API (WebSocket/SSE) → 前端
  每行 Claude Code stdout/stderr (NDJSON) 产生一个 LogEvent
  Worker Runtime 同时将原始输出写入本地日志文件（per-task NDJSON）
  本地日志文件通过 FileSyncManager 持久化到 OSS/S3
  实时流用于前端展示，持久化日志用于历史查询和排障

数据流 ③：文件同步（持续，中等流量）
  Worker FileSyncManager: inotify → 分层防抖 → 增量上传 OSS/S3
  Worker → Manager: FILE_CHANGE 事件 (通知前端有新文件)
  Worker → Manager: FILE_SYNCED 事件 (确认文件已同步到 OSS/S3)
  外部读取: 直接从 OSS/S3 读取（不走 Worker）
  外部订阅: Manager → FILE_CHANGE 事件流 → 前端刷新文件列表

数据流 ④：心跳与状态（低频，关键路径）
  Worker → Manager: Heartbeat（30s 间隔）
  Manager → Worker: HealthCheck（主动探测）
  Manager ← 云 SDK: DescribeInstances（60s 间隔，标签对账用）
```

**为什么不用消息队列（SQS/RabbitMQ）？** MVP 阶段 Worker 数量 <50，单条 WebSocket 连接的吞吐足够。消息队列引入外部依赖和运维成本，且 WebSocket 的延迟远低于 SQS（毫秒 vs 百毫秒）。当 Worker 超过 50 台时，再考虑引入 NATS/Redis Stream 做数据面分离。

### 2.3 组件交互时序

#### 扩容全流程

```
  Harness/API          Manager              CloudProvider       Worker Runtime
      │                    │                      │                    │
      │  scale_out(N=1)    │                      │                    │
      ├───────────────────▶│                      │                    │
      │                    │  create_instance()   │                    │
      │                    ├─────────────────────▶│                    │
      │                    │  instance_id         │                    │
      │                    │◀─────────────────────┤                    │
      │                    │                      │                    │
      │                    │  wait_until_running() │                    │
      │                    ├─────────────────────▶│ (轮询 5s 间隔)     │
      │                    │  Instance(RUNNING)   │                    │
      │                    │◀─────────────────────┤                    │
      │                    │                      │                    │
      │                    │── SSH Bootstrap ──────────────────────────▶│
      │                    │   1. 系统初始化                            │
      │                    │   2. Agent 安装                           │
      │                    │   3. 凭证注入                             │
      │                    │   4. Worker Runtime 启动                  │
      │                    │                      │                    │
      │                    │◀═══ WS 反向连接（Worker 主动）═════════════╡
      │                    │   认证: {token: "per-worker-secret"}     │
      │                    │                      │                    │
      │                    │── NodeRegistry.add() │                    │
      │                    │── EventBus.emit(NODE_READY)               │
      │  [nodes]           │                      │                    │
      │◀───────────────────┤                      │                    │
```

#### 崩溃恢复流程

```
  HealthChecker         Manager              CloudProvider       New Worker
      │                    │                      │                    │
      │ 连续 3 次心跳超时   │                      │                    │
      ├───────────────────▶│                      │                    │
      │                    │── emit(WORKER_UNHEALTHY)                  │
      │                    │                      │                    │
      │                    │── 回收凭证到池子      │                    │
      │                    │── Registry.update(unhealthy)              │
      │                    │── terminate_instance()│                    │
      │                    ├─────────────────────▶│                    │
      │                    │                      │                    │
      │                    │── Harness._on_worker_unhealthy()          │
      │                    │   → 决定是否需要替换   │                    │
      │                    │                      │                    │
      │                    │── scale_out(1, recovery=True)             │
      │                    ├─────────────────────▶│  (新实例)          │
      │                    │                      │───────────────────▶│
      │                    │                      │                    │
      │                    │── Bootstrap (恢复工作目录 + /continue-book)│
```

---

## 3. 核心子系统设计

### 3.1 多云资源管理层

**设计目标：** 一个 `CloudProvider` 接口抹平阿里云 / AWS 的差异，上层代码完全不感知云厂商。

**接口契约：**

```python
class CloudProvider(ABC):
    async def create_instance(config: InstanceConfig) -> Instance
    async def start_instance(instance_id: str) -> None
    async def stop_instance(instance_id: str) -> None
    async def terminate_instance(instance_id: str) -> None
    async def list_instances(filters: dict | None) -> list[Instance]
    async def get_instance(instance_id: str) -> Instance
    async def wait_until_running(instance_id: str, timeout: int) -> Instance
```

**关键设计决策：**

| 决策 | 选择 | 理由 |
|------|------|------|
| 同步 vs 异步 SDK | 同步 SDK 包在 `asyncio.to_thread()` 中 | 阿里云 SDK V2 的 async 支持不稳定，boto3 完全同步；包一层比引入两套 client 简单 |
| 实例标识 | `{platform}:{native_id}`（如 `aliyun:i-bp1xxx`、`aws:i-0abc`） | 平台前缀避免跨云 ID 碰撞，同时保留原生 ID 的可调试性（云控制台可直接搜索） |
| 标签约定 | 所有框架实例必须打 `ManagedBy=elastic-agent` | 对账的基础；不可省略 |
| Spot/抢占式 | `InstanceConfig.spot: bool` 统一字段 | 阿里云 SpotStrategy / AWS SpotInstanceType 在 Provider 内部映射 |
| 错误重试 | Provider 内部不重试，由 Manager 层决定重试策略 | Provider 是纯粹的 SDK 封装，策略逻辑上推 |

**阿里云 vs AWS 实现差异（Provider 内部封装）：**

| 操作 | 阿里云 ECS | AWS EC2 | Provider 如何抹平 |
|------|-----------|---------|------------------|
| 创建实例 | `RunInstances` | `run_instances` | 统一返回 `Instance` |
| 销毁实例 | 需先 Stop 再 `DeleteInstance(Force=True)` | `terminate_instances` 直接释放 | Provider 内部处理两步 |
| 公网 IP | `PublicIpAddress` 或 `EipAddress` 两个位置 | `PublicIpAddress` | `_to_instance()` 统一提取 |
| 默认用户 | `root` | `ubuntu` | 配置项 `ssh_user` |
| Spot | `SpotStrategy: SpotAsPriceGo` | `InstanceMarketOptions.MarketType: spot` | `config.spot` 布尔值 |

### 3.2 Worker Runtime 通信层

**这是框架最核心的子系统。** 它定义了 Manager 和 Worker 之间的所有交互方式。

#### 连接模型：Worker 主动连接 Manager

```
启动时序:
  1. Manager 监听 WS 端点 ws://0.0.0.0:8000/ws/runtime
  2. Bootstrap 最后一步在 Worker 上启动 Runtime 服务
  3. Worker Runtime 主动连接 Manager（反向连接）
  4. 发送认证消息 {"type": "auth", "token": "per-worker-secret"}
  5. Manager 验证 token → 绑定连接到 NodeRegistry 中的节点
  6. 进入双向消息循环

为什么 Worker 主动连 Manager（而不是 Manager 连 Worker）？
  - Worker 在 VPC 私有子网内，不需要开入站端口 → 安全
  - Manager IP 固定（或域名），Worker IP 动态 → 连接方向自然
  - 断线重连逻辑在 Worker 侧，更简单（Manager 不需要维护重连队列）
```

#### 消息类型体系

```
Manager → Worker (命令):
  EXECUTE      启动子进程         {task_id, command[], cwd, env{}, timeout?}
  STOP         停止子进程         {task_id, signal?}
  READ_FILE    读取文件           {request_id, path, encoding?}
  WATCH_FILES  监听文件变化       {request_id, paths[], events[]}
  UNWATCH      取消监听           {request_id}
  HEALTH_CHECK 主动探测           {}
  UPLOAD_FILE  上传文件到 Worker  {path, content_base64, mode?}
  MESSAGE      反向消息（用户→Agent） {task_id, payload}

  [NEW] REGISTER_SYNC_MAPPING    注册同步映射（per-task）
        {task_id, book_slug, oss_prefix, watch_paths[], session_path_hash}
        说明: Manager 在分配任务到 Worker 时下发，告诉 FileSyncManager
              该任务的文件应该同步到哪个 OSS 路径。替代静态 path_mapping。
        示例: {
          "task_id": "task-abc123",
          "book_slug": "erta-ale",
          "oss_prefix": "oss://audiobook-prod/tasks/task-abc123/",
          "watch_paths": ["/root/.work/erta-ale/", "/root/.claude/projects/"],
          "session_path_hash": "a1b2c3d4"
        }

  [NEW] UNREGISTER_SYNC_MAPPING  注销同步映射（任务结束时）
        {task_id}
        说明: 任务完成或 Worker 释放时，Manager 通知 FileSyncManager
              停止监听该任务的文件变更并清理映射。

Worker → Manager (事件):
  LOG          日志行             {task_id, stream, data, timestamp, parsed?}
                                  stream: "stdout" | "stderr"
                                  data: 原始输出行（字符串）
                                  parsed: 可选的结构化解析结果（见下方说明）
  PROCESS_EXIT 进程退出           {task_id, exit_code, timestamp}
  FILE_CONTENT 文件内容响应       {request_id, path, content}
  FILE_CHANGE  文件变更事件       {path, event, content?, timestamp}
  STATUS       状态上报           {cpu%, mem%, disk%, active_processes[]}
  HEARTBEAT    心跳               {uptime_seconds}
  ERROR        错误上报           {error_type, message, recoverable}

  [NEW] FILE_SYNCED  文件同步完成确认（区别于 FILE_CHANGE）
        {task_id, path, oss_key, synced_at, md5}
        说明: 文件已成功上传到 OSS/S3 后发送。FILE_CHANGE 表示文件在本地变了，
              FILE_SYNCED 表示文件已经持久化到云存储。外部 API 收到 FILE_SYNCED
              后才能保证从 OSS 读到最新版本。
```

#### 连接生命周期与断线恢复

```
状态机:
  CONNECTING → AUTHENTICATING → CONNECTED → DISCONNECTED
                                    ↑              │
                                    └── (自动重连) ──┘

断线恢复策略:
  Worker 侧:
    - 指数退避重连: 1s → 2s → 4s → 8s → ... → 60s (上限)
    - 重连期间缓冲日志到本地文件（防止丢失）
    - 重连成功后回放缓冲日志

  Manager 侧:
    - 连接断开 → 标记节点为 "disconnected"
    - 超过 3 × 心跳间隔(90s) 仍未重连 → 标记 "unhealthy"
    - 连续 3 次 unhealthy → 触发 WORKER_UNHEALTHY 事件
```

#### 进程管理模型

```
Worker Runtime 管理的进程:
  ┌────────────────────────────────────────────────┐
  │ Worker Runtime (FastAPI + WS client)           │
  │                                                │
  │  processes: dict[task_id, Process]              │
  │                                                │
  │  每个进程:                                      │
  │    ├── asyncio.create_subprocess_exec           │
  │    ├── stdout → 逐行读取 → 双写:               │
  │    │     ├── LOG 事件发送到 Manager（实时流）    │
  │    │     └── 追加写入本地日志文件（持久化）      │
  │    ├── stderr → 逐行读取 → 双写（同上）         │
  │    └── 退出 → PROCESS_EXIT 事件                │
  │              → 关闭日志文件                     │
  │              → 触发 FileSyncManager 立即同步    │
  │                日志文件到 OSS（跳过防抖）       │
  │                                                │
  │  日志文件路径:                                  │
  │    由 TaskSyncMapper 的映射决定，默认:          │
  │    {task_work_dir}/logs/production.ndjson        │
  │                                                │
  │  文件监听:                                      │
  │    ├── watchdog Observer (inotify)              │
  │    └── 变更 → FILE_CHANGE 事件                  │
  └────────────────────────────────────────────────┘

停止进程的信号序列:
  SIGINT → 等待 10s → SIGTERM → 等待 5s → SIGKILL
  (与 Claude Code CLI 的优雅退出协议一致)
```

### 3.3 外部服务 API 层

**设计目标：** 外部服务（前端 UI、监控系统、第三方集成）通过 Manager 获取实时 Agent 轨迹和 Worker 文件，不需要直接访问 Worker。

#### 数据流路径

```
实时轨迹流:
  Worker Claude Code stdout
    → Worker Runtime 逐行读取
    → LOG 消息 via WS
    → Manager EventBus
    → 外部 API 轨迹流端点 (WebSocket / SSE)
    → 外部消费者

文件访问:
  外部请求 GET /api/external/files/{node_id}/{path}
    → Manager 查找 node_id 对应的 WS 连接
    → 发送 READ_FILE 命令到 Worker
    → Worker 读取本地文件
    → FILE_CONTENT 响应 via WS
    → Manager 返回给外部

文件变更监听:
  外部订阅 WS /api/external/files/{node_id}/watch
    → Manager 转发 WATCH_FILES 命令到 Worker
    → Worker inotify 监听
    → FILE_CHANGE 事件 via WS
    → Manager 转发到外部 WebSocket
```

#### 轨迹存储：实时缓冲 + 持久化双层

```
                  ┌─────────────────────────┐
                  │   实时轨迹缓冲 (内存)     │
                  │                         │
  LOG 事件 ──────▶│  环形缓冲 (per-task)    │──────▶ 实时订阅者 (WebSocket/SSE)
                  │  容量: 5000 条/task      │
                  │                         │
                  │  查询接口:               │──────▶ 近期查询 (REST GET)
                  │  by node_id             │
                  │  by task_id             │
                  │  by time range          │
                  └─────────────────────────┘

                  ┌─────────────────────────┐
                  │   持久化轨迹 (OSS/S3)    │
                  │                         │
  Worker 日志文件 ─▶│  logs/production.ndjson │──────▶ 历史查询 / 排障 / 回放
  (FileSyncManager) │  per-task 独立文件      │
                  │                         │
                  └─────────────────────────┘

两层职责分工:
  - 实时缓冲: 服务当前活跃的 WebSocket/SSE 订阅者和近期 REST 查询
    容量改为 per-task（非 per-worker），每个任务独立 5000 条
    任务结束后缓冲可释放（历史查询走 OSS）
  - 持久化日志: Worker Runtime 在读取进程输出时同步写入本地文件
    FileSyncManager 将文件同步到 OSS/S3
    任务完成后触发立即同步（跳过防抖），确保日志完整
    支持完整的历史查询、聊天记录回放、运维排障
```

#### LOG 事件结构化解析

Worker Runtime 在发送 LOG 事件时，如果 data 是合法 JSON 且包含 `type` 字段（Claude Code stream-json 格式），则同时附带 `parsed` 字段：

```
LOG 事件（增强）:
  {
    "task_id": "123",
    "stream": "stdout",
    "data": "{\"type\":\"assistant\",\"content\":\"开始 Phase 1...\"}",
    "timestamp": "2026-05-17T10:01:23Z",
    "parsed": {
      "type": "assistant",
      "subtype": null,
      "cost_usd": null,
      "session_id": null
    }
  }

parsed 字段的 type 枚举（来自 Claude Code stream-json）:
  system       系统消息（模型、配置信息）
  assistant    Claude 输出文本
  user         用户输入（-p 参数）
  tool_use     工具调用（Read/Edit/Bash 等）
  tool_result  工具执行结果
  result       最终结果（包含 session_id, cost_usd, context_usage）

框架层面的处理:
  - 实时缓冲支持按 parsed.type 过滤（如只返回 assistant + result）
  - result 类型的事件额外提取 session_id 和 cost_usd 到 PROCESS_EXIT
  - 非 JSON 行或无 type 字段的行：parsed = null（不影响转发）
  - stderr 输出: parsed 始终为 null（stderr 不是 NDJSON 格式）
```

#### 认证模型

```
外部 API 认证 (MVP):
  所有外部 API 请求必须携带 API Key:
    - URL 参数: ?api_key=xxx
    - 或 Header: Authorization: Bearer xxx

  API Key 在 Manager 配置文件中定义:
    external_api:
      api_keys:
        - "key-for-frontend"
        - "key-for-monitoring"

  WebSocket 连接在首条消息中认证:
    {"type": "auth", "api_key": "xxx"}

后续演进:
  Phase 2: OAuth 2.0 Client Credentials（支持 scope 控制）
  Phase 6: JWT + RBAC（细粒度权限）
```

### 3.4 FileSyncManager — Worker 文件主动同步

**为什么需要这个组件：** MVP 的原始设计是"外部按需从 Worker 读文件"。但 Audiobook Harness 的架构要求所有文件内容查询走云存储（OSS/S3），不走 Worker。这意味着文件必须**主动**从 Worker 同步到云存储，而不是被动等外部来拉。崩溃恢复（从 OSS 恢复 workspace）和跨 Worker session 迁移也依赖这个机制。

#### 动态映射架构（TaskSyncMapper）

> **[更新] 原设计中 FileSyncManager 依赖静态的 `get_file_sync_config()` 返回 `path_mapping`。**
> 在 Audiobook Harness 场景下，同一个 Worker 可能同时（或先后）处理多个 task，每个 task 的同步目标路径不同。
> 静态映射无法满足需求，改为 **动态映射** 设计。

```
FileSyncManager 运行在每个 Worker 上（Worker Runtime 的子组件）:

  ┌─────────────────────────────────────────────────────────┐
  │ FileSyncManager                                         │
  │                                                         │
  │  ┌─────────────────────────────────────────────────┐    │
  │  │ TaskSyncMapper（动态映射子组件）                   │    │
  │  │                                                 │    │
  │  │  职责:                                          │    │
  │  │    - 接收 REGISTER_SYNC_MAPPING 消息 → 注册映射  │    │
  │  │    - 接收 UNREGISTER_SYNC_MAPPING 消息 → 注销映射│    │
  │  │    - 维护 active_mappings: dict[task_id, SyncMapping]│
  │  │    - 根据文件路径匹配到对应的 task_id + oss_prefix│    │
  │  │                                                 │    │
  │  │  数据结构:                                       │    │
  │  │    SyncMapping {                                │    │
  │  │      task_id: str                               │    │
  │  │      book_slug: str                             │    │
  │  │      oss_prefix: str    # "oss://bucket/tasks/X/"│   │
  │  │      watch_paths: list[str]                     │    │
  │  │      session_path_hash: str                     │    │
  │  │      registered_at: datetime                    │    │
  │  │    }                                            │    │
  │  │                                                 │    │
  │  │  路径匹配逻辑:                                   │    │
  │  │    file_changed("/root/.work/erta-ale/ch01.md") │    │
  │  │    → 遍历 active_mappings                       │    │
  │  │    → 找到 watch_paths 包含 "/root/.work/erta-ale/"│   │
  │  │    → 返回 (task_id, oss_prefix)                 │    │
  │  │    → 上传到 oss_prefix + 相对路径               │    │
  │  └─────────────────────────────────────────────────┘    │
  │                                                         │
  │  模板规则（来自 Harness get_file_sync_config()）:         │
  │    debounce_tiers: {"state.json": 0.5, "manuscript_*": 2, "*": 5}│
  │    exclude_patterns: ["*.tmp", "*.swp", "__pycache__/"]  │
  │    说明: Harness 只定义防抖策略和排除规则等模板配置，     │
  │          实际的 watch_paths 和 oss_prefix 由 Manager      │
  │          通过 REGISTER_SYNC_MAPPING 动态下发。            │
  │                                                         │
  │  行为:                                                   │
  │    1. 收到 REGISTER_SYNC_MAPPING → 添加 inotify watch    │
  │    2. inotify 事件 → TaskSyncMapper 匹配 task_id         │
  │    3. 匹配防抖等级 → 计时器                               │
  │    4. 计时器到期 → 上传到 oss_prefix + 相对路径           │
  │    5. 上传完成 → 更新 _sync_manifest.json                │
  │    6. 发送 FILE_SYNCED 事件到 Manager（确认已同步）       │
  │    7. 同时发送 FILE_CHANGE 事件到 Manager（通知前端）     │
  │    8. 收到 UNREGISTER_SYNC_MAPPING → 移除 inotify watch  │
  │       → 做最终同步（确保最后的文件变更不丢）→ 清理映射    │
  └─────────────────────────────────────────────────────────┘

  外部读取路径:
    外部 API GET /api/tasks/{task_id}/files/{path}
      → Manager 从 OSS/S3 读取（不走 Worker）
      → 响应包含 synced_at（调用者知道数据新鲜度）

  云存储凭证:
    Bootstrap 时注入 OSS AccessKey 或 AWS IAM Role
    Worker Runtime 用这些凭证调用 PutObject

  与原有 external_api/files 的关系:
    T-014（文件变更通知）: Worker → Manager WS 事件 → 前端（只传事件，不传内容）
    T-033（文件内容读取）: 外部 → Manager → OSS/S3 → 返回内容（不走 Worker）
    T-037（文件同步确认）: Worker → Manager FILE_SYNCED 事件 → 外部知道 OSS 已更新
    FileSyncManager 是以上三者的上游 — 它保证云存储里的文件是最新的
```

#### OSS 上传错误处理策略

```
上传策略按文件大小分层:

  小文件 (<10MB):
    - 使用 PutObject 单次上传
    - 失败重试: 最多 3 次，指数退避 (1s → 2s → 4s)
    - 3 次全部失败 → 进入本地缓冲队列

  大文件 (>=10MB):
    - 使用 Multipart Upload，每 part 5MB
    - 单个 part 失败重试: 最多 3 次
    - 整体失败 → AbortMultipartUpload + 进入本地缓冲队列

  OSS 不可用时 (网络故障 / 服务降级):
    - 所有待上传文件缓冲到本地队列
    - 后台线程每 30s 尝试重新上传队列中的文件
    - 缓冲队列持久化到磁盘（防止 Runtime 重启丢失）
    - 队列大小上限: 1000 个文件或 500MB（先到者触发告警）

  关键文件 vs 普通文件的区分:
    关键文件 (state.json, manifest.json, _sync_manifest.json):
      - 同步上传 — 等待 OSS 确认后才继续主流程
      - 失败时阻塞并立即报 ERROR 事件到 Manager
      - 理由: 这些文件是崩溃恢复的基础，必须保证持久化

    普通文件 (manuscript_*.md, *.txt 等):
      - 异步上传 — 不阻塞主流程
      - 失败时进入缓冲队列，不影响 Agent 执行
      - 理由: 内容文件短暂延迟可接受，不应因 OSS 抖动阻塞创作
```

#### _sync_manifest.json 格式

> **[更新] 原设计使用 dict 格式（以文件路径为 key）。改为 array 格式，增加 content_type 和 role 字段，便于前端展示和恢复逻辑使用。**

```json
{
  "task_id": "task-abc123",
  "worker_id": "aliyun:i-bp1xxx",
  "status": "syncing",
  "updated_at": "2026-05-18T12:00:00Z",
  "files": [
    {
      "path": "/root/.work/erta-ale/manuscript_ch01.md",
      "oss_key": "tasks/task-abc123/manuscript_ch01.md",
      "size": 15234,
      "md5": "d41d8cd98f00b204e9800998ecf8427e",
      "content_type": "text/markdown",
      "role": "manuscript",
      "synced_at": "2026-05-18T11:59:55Z"
    },
    {
      "path": "/root/.work/erta-ale/state.json",
      "oss_key": "tasks/task-abc123/state.json",
      "size": 2048,
      "md5": "a1b2c3d4e5f6...",
      "content_type": "application/json",
      "role": "state",
      "synced_at": "2026-05-18T12:00:00Z"
    }
  ]
}
```

字段说明：
- `status`: `"syncing"` | `"idle"` | `"error"` — 当前同步状态
- `files[].role`: `"manuscript"` | `"state"` | `"metadata"` | `"audio"` | `"other"` — 文件角色，由 Harness 的 debounce_tiers 模式匹配推断
- `files[].content_type`: MIME 类型，用于 OSS 存储和外部 API 响应

### 3.5 节点生命周期管理

#### 节点状态机

```
                create_instance()
                      │
                      ▼
  ┌──────────┐   ┌──────────┐   Bootstrap 成功   ┌──────────┐
  │          │   │          ├───────────────────▶│          │
  │ CREATING ├──▶│BOOTSTRAP-│                    │  READY   │
  │          │   │   ING    │   Bootstrap 失败   │  (idle)  │
  └──────────┘   │          ├───────┐            │          │
                 └──────────┘       │            └────┬─────┘
                                    ▼                 │
                              ┌──────────┐      分配任务
                              │  FAILED  │           │
                              │(待清理)   │           ▼
                              └──────────┘      ┌──────────┐
                                                │  BUSY    │
                                                │(执行中)   │
                                                └────┬─────┘
                                                     │
                                               任务完成/失败
                                                     │
                             ┌───────────────────────┤
                             │                       │
                             ▼                       ▼
                       ┌──────────┐           ┌──────────┐
                       │  READY   │           │ DRAINING │
                       │ (再次空闲)│           │(等待完成) │
                       └──────────┘           └────┬─────┘
                                                   │
                                              超时或完成
                                                   │
                                                   ▼
                                             ┌──────────┐
                                             │TERMINATED│
                                             └──────────┘

异常路径:
  任意状态 → 心跳超时 → UNHEALTHY → (Harness 决定) → TERMINATED 或替换
  任意状态 → 云端对账发现已消失 → 从注册表移除
```

#### 云端标签对账

```
对账解决的核心问题:
  Manager 在 scale_out() 执行到一半崩溃 → EC2 已创建但注册表未写入 → 孤儿实例

对账触发时机:
  1. Manager 启动时（必须）
  2. 每 5 分钟周期性（兜底）

对账算法:
  cloud_instances = provider.list_instances(ManagedBy=elastic-agent)
  registered_ids = registry.list_all_ids()

  孤儿 = cloud_instances - registered_ids
    → 策略: 纳入管理（add to registry）或清理（terminate）
    → MVP 默认: 纳入管理，等人工确认后清理

  幽灵 = registered_ids - cloud_instances
    → 直接从注册表移除（云端已经不存在了）

  状态不一致 = 两侧都有但状态不同
    → 以云端为准（cloud is source of truth）
```

#### Bootstrap 失败处理

```
失败策略枚举:
  TERMINATE_AND_RETRY  销毁实例 → 重新创建 → 重新 Bootstrap (默认)
  RETRY_FROM_FAILED    在同一实例上从失败步骤重试
  LEAVE_FOR_DEBUG      保留实例供人工排查（仅开发环境）

凭证安全:
  Bootstrap 失败时，如果凭证已注入 Worker → 必须回收凭证到池子
  否则凭证"被锁定"在一台不可用的 Worker 上

最大重试次数: 2
  超过后 → 标记 FAILED + 发送告警 + 不再自动重试
  防止: 云资源持续创建-销毁循环（烧钱）
```

### 3.6 凭证与安全

#### 两层凭证模型

```
Layer 1: Agent 凭证（框架管理）
  Claude Code 的登录态（OAuth refresh token）
  存储: credentials.json（Manager 本地）
  分发: Bootstrap 时写入 Worker 的 ~/.claude/.credentials.json
  回收: 节点终止或 Drain 时回收到池子
  轮换: 额度耗尽时自动换号（Phase 2 实现）

Layer 2: 应用凭证（Harness 声明，框架传递）
  Git SSH key、WandB API key、HuggingFace token 等
  Harness 通过 get_app_credentials() 声明需要哪些
  Bootstrap 时从 Manager 安全传递到 Worker（环境变量或文件）
```

#### 多 Slot Worker 的凭证隔离

> **[更新] 原设计假设每个 Worker 只运行一个 Agent 进程。Audiobook Harness 的双 Slot 模型（production + edit）要求同一台机器上并行运行多个 Claude Code 进程，每个进程需要独立的凭证。**

```
多 Slot 凭证隔离策略:

  Primary Slot (production):
    - 使用高配额凭证（主账号）
    - CLAUDE_CONFIG_DIR=/root/.claude-prod/
    - 长时间运行，承担主要创作工作

  Secondary Slot(s) (edit):
    - 使用普通配额凭证（副账号）
    - CLAUDE_CONFIG_DIR=/root/.claude-edit-{n}/
    - 短时间运行，按需启动

  隔离机制:
    - 每个 Claude Code 进程通过不同的 CLAUDE_CONFIG_DIR 隔离
    - 凭证文件写入各自的 config dir: {CLAUDE_CONFIG_DIR}/.credentials.json
    - Bootstrap 时 Manager 为每个 Slot 分配独立凭证
    - 回收时按 Slot 分别回收

  凭证分配策略:
    - Primary slot → 从 CredentialPool 的 "high_quota" 分组分配
    - Edit slot → 从 CredentialPool 的 "standard" 分组分配
    - 如果 standard 分组耗尽 → 可以临时使用 high_quota（但需要告警）
```

#### Manager ↔ Worker 认证

```
Bootstrap 时:
  1. Manager 生成 per-Worker 随机 token: secrets.token_urlsafe(32)
  2. 通过 SSH 写入 Worker 的配置文件
  3. 同时写入 NodeRegistry

运行时:
  Worker Runtime 连接 Manager WS 端点时:
    → 首条消息: {"type": "auth", "token": "<per-worker-token>"}
    → Manager 在 NodeRegistry 中查找匹配的 token
    → 匹配 → 绑定连接; 不匹配 → 关闭连接

安全边界:
  - Manager ↔ Worker 走 VPC 内网（不经过公网）
  - Worker Runtime 只监听内网 IP
  - Worker 不开放任何入站端口（WS 是 Worker → Manager 方向）
```

### 3.7 事件系统

```
事件系统是 Manager 内部的核心通信机制:

EventBus
  ├── 生产者: Worker Runtime 连接 (LOG, HEARTBEAT, PROCESS_EXIT, ...)
  ├── 生产者: HealthChecker (WORKER_UNHEALTHY)
  ├── 生产者: ElasticAgentManager (NODE_CREATING, NODE_READY, ...)
  ├── 生产者: CredentialPool (CREDENTIAL_ROTATED, CREDENTIAL_EXHAUSTED)
  ├── 生产者: OperationLogger (SCALE_OUT, BOOTSTRAP_STEP, RECONCILE, ...)
  │
  ├── 消费者: 外部 API traces 端点 (订阅 LOG 事件)
  ├── 消费者: 外部 API files 端点 (订阅 FILE_CHANGE 事件)
  ├── 消费者: Harness 事件回调 (订阅框架事件)
  ├── 消费者: HealthChecker (订阅 HEARTBEAT 超时)
  ├── 消费者: 轨迹缓存 (订阅 LOG 事件 → 写入环形缓冲)
  └── 消费者: OperationLogger (所有事件 → 结构化 JSON Lines 日志文件)

实现:
  MVP 用 asyncio.Queue 的 fan-out 模式:
    - EventBus 维护 subscribers: dict[event_type, list[asyncio.Queue]]
    - emit() → 复制事件到所有匹配的 Queue
    - subscribe() → 返回 AsyncIterator 从 Queue 读取

  不使用 Redis Pub/Sub 或 Kafka:
    - MVP 是单进程，内存 Queue 足够
    - 延迟更低（纳秒 vs 毫秒）
    - 无外部依赖
```

### 3.8 结构化操作日志

框架所有关键操作写入结构化日志文件，用于运维排障和审计。

```
日志文件: ~/.elastic-agent/operations.log (JSON Lines 格式，按日轮转)

每条日志包含:
  {
    "ts": "2026-05-17T10:01:23Z",
    "level": "INFO",
    "component": "bootstrap",
    "action": "step_completed",
    "node_id": "aliyun:i-bp1xxx",
    "details": {"step": "install-claude-code", "duration_ms": 12345},
    "trace_id": "op-abc123"
  }

覆盖的操作类别:

  扩缩容:
    scale_out_started     {count, trigger}
    scale_out_completed   {nodes[], duration_ms}
    scale_in_started      {node_id, reason}
    instance_created      {node_id, instance_type, region}
    instance_terminated   {node_id, reason}

  Bootstrap:
    bootstrap_started     {node_id, steps[]}
    step_started          {node_id, step_name}
    step_completed        {node_id, step_name, duration_ms}
    step_failed           {node_id, step_name, error, stderr}
    bootstrap_completed   {node_id, total_duration_ms}
    bootstrap_failed      {node_id, failed_step, strategy}

  健康检查:
    health_check_passed   {node_id, level}
    health_check_failed   {node_id, level, consecutive_failures}
    worker_marked_unhealthy {node_id, reason}

  凭证:
    credential_assigned   {node_id, credential_id, slot_type}
    credential_rotated    {node_id, old_id, new_id, reason}
    credential_recovered  {node_id, credential_id}
    quota_warning         {credential_id, usage_pct}

  对账:
    reconcile_started     {}
    orphan_found          {instance_id, action}
    ghost_found           {node_id, action}
    reconcile_completed   {orphans, ghosts, duration_ms}

  Webhook:
    webhook_sent          {event_type, task_id, target_url, status_code}
    webhook_retry         {event_type, task_id, attempt, next_retry_at}
    webhook_failed        {event_type, task_id, attempts, error}

  Worker 连接:
    worker_connected      {node_id, reconnect: bool}
    worker_disconnected   {node_id, reason}

日志轮转: 按日轮转，保留 30 天。
Harness 可通过 self.logger 写入同一日志流。
```

---

## 4. IaC 策略

### 4.1 双工具策略

| 云 | IaC 工具 | 理由 |
|---|---------|------|
| **阿里云** | **Terraform** (alicloud provider) | 阿里云没有 CDK 等价物，Terraform 是唯一成熟选择 |
| **AWS** | **CDK Python** | 项目全栈 Python，CDK 与主代码共享类型系统；后续 ASG/Lambda/SSM 集成 CDK 远优于 Terraform |

### 4.2 IaC 管什么、不管什么

| 资源类型 | 管理方式 | 理由 |
|---------|---------|------|
| VPC / 子网 / 路由表 | IaC (Terraform / CDK) | 一次性创建，环境间一致性要求高 |
| 安全组及规则 | IaC | 安全策略需要版本控制和审计 |
| 密钥对 | IaC | 一次性创建 |
| NAT Gateway + EIP | IaC | 出站 IP 固定是 IP 亲和性的基础 |
| IAM/RAM 角色 | IaC | 最小权限原则，需要审计 |
| **ECS/EC2 实例 (Worker)** | **SDK 直连** | 动态生命周期，按需创建/销毁 |
| **EIP（弹性 IP）** | **SDK 直连** | 跟随实例动态分配/回收 |

### 4.3 项目结构

```
infra/
├── aliyun/                        # Terraform (HCL)
│   ├── modules/
│   │   └── networking/
│   │       ├── main.tf            # VPC, VSwitch, 安全组, NAT, 密钥对
│   │       ├── variables.tf
│   │       └── outputs.tf         # → vpc_id, vswitch_ids, sg_id, key_name
│   └── environments/
│       └── cn-hangzhou/
│           ├── main.tf            # provider "alicloud" + module 调用
│           ├── terraform.tfvars   # region, cidr, 实例化参数
│           └── backend.tf         # OSS remote state
│
└── aws/                           # CDK Python
    ├── app.py                     # CDK App 入口
    ├── stacks/
    │   └── networking_stack.py    # VPC, Subnet, SG, NAT, KeyPair
    ├── cdk.json
    └── requirements.txt
```

### 4.4 CDK Python 设计要点

```python
# infra/aws/stacks/networking_stack.py

from aws_cdk import Stack, CfnOutput
from aws_cdk import aws_ec2 as ec2
from constructs import Construct

class ElasticAgentNetworkingStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs):
        super().__init__(scope, id, **kwargs)

        # CDK L2 Construct — 一行创建 VPC + 公私有子网 + NAT + 路由表
        self.vpc = ec2.Vpc(self, "ElasticAgentVpc",
            max_azs=2,
            nat_gateways=1,
            subnet_configuration=[
                ec2.SubnetConfiguration(name="worker-private",
                    subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS),
                ec2.SubnetConfiguration(name="public",
                    subnet_type=ec2.SubnetType.PUBLIC),
            ],
        )
        # 对比 Terraform: 需要分别定义 VPC + 2 Subnet + IGW + NAT + 4 RouteTable + 4 RouteTableAssociation

        self.worker_sg = ec2.SecurityGroup(self, "WorkerSG",
            vpc=self.vpc,
            description="Elastic-Agent Workers",
            allow_all_outbound=True,
        )
        self.worker_sg.add_ingress_rule(
            ec2.Peer.ipv4(self.vpc.vpc_cidr_block),
            ec2.Port.tcp(8080),
            "Worker Runtime from VPC",
        )

        # 输出供 Manager 配置使用
        CfnOutput(self, "VpcId", value=self.vpc.vpc_id)
        CfnOutput(self, "PrivateSubnetIds",
            value=",".join([s.subnet_id for s in self.vpc.private_subnets]))
        CfnOutput(self, "SecurityGroupId", value=self.worker_sg.security_group_id)
```

**CDK vs Terraform 对比（AWS 侧）：**

| 维度 | CDK Python | Terraform HCL |
|------|-----------|---------------|
| VPC + NAT + 路由 | 1 个 `ec2.Vpc()` 调用 | ~15 个 resource block |
| 类型安全 | Python 类型检查，IDE 补全 | HCL 无类型，靠 `terraform validate` |
| 测试 | `pytest` + `assertions` 模块 | `terraform plan` 输出解析 |
| 与主代码集成 | 共享 Pydantic 模型、常量 | 完全隔离 |
| 后续扩展 | Lambda/SSM/ASG 用 L2 construct 很自然 | 需要大量 resource block |

---

## 5. 错误处理与恢复策略

### 5.1 故障分类

| 层级 | 故障 | 检测方式 | 恢复策略 |
|------|------|---------|---------|
| **云基础设施** | 实例意外终止 / Spot 回收 | 云端对账 + 心跳超时 | 标签对账清理 + Harness 决定是否替换 |
| **网络** | WS 连接断开 | 心跳超时 | Worker 自动重连（指数退避） |
| **Worker Runtime** | Runtime 进程崩溃 | 心跳消失 | Manager 标记 unhealthy → Harness 回调 |
| **Agent 进程** | Claude Code 崩溃 | PROCESS_EXIT(非零) | Harness 决定：重启 / 恢复 / 放弃 |
| **Manager** | Manager 进程崩溃 | 无（单点） | 重启后标签对账 + 注册表重建 + Harness 状态恢复 |
| **Bootstrap** | 初始化步骤失败 | 步骤返回错误 | 按失败策略处理（见 3.4） |

### 5.2 Manager 崩溃恢复

```
Manager 崩溃后重启:
  1. 读取 NodeRegistry (JSON 文件，崩溃前最后一次写入)
  2. 云端标签对账:
     - 发现孤儿实例 → 纳入管理
     - 发现幽灵节点 → 从注册表移除
  3. 等待 Worker Runtime 重连:
     - Worker 侧有指数退避重连逻辑
     - 所有活跃 Worker 会在 1-60s 内重连
  4. 重建内存状态:
     - EventBus 重新初始化
     - HealthChecker 重新启动（从 0 开始计数）
  5. [NEW] 恢复 Harness 级状态:
     - Harness 自身的持久化状态也需要恢复（不仅仅是 NodeRegistry）
     - 例如 Audiobook Harness 的 SessionRegistry（session↔Worker 映射、
       session 状态、当前 task 进度等）需要从持久化存储重建
     - 恢复顺序: NodeRegistry → Harness 状态 → 等待 Worker 重连 → 对账

  Harness 状态持久化要求:
     - Harness 必须实现 persist_state() / restore_state() 接口
     - 持久化目标: 与 NodeRegistry 相同的本地 JSON 文件
     - 写入时机: 每次状态变更后同步写入（与 NodeRegistry 一致）
     - 恢复时机: Manager 启动时，在 NodeRegistry 恢复之后、
       Worker 重连之前调用
     - 如果 Harness 状态文件损坏或缺失: 降级为"从 Worker 侧重建"
       （Worker 重连时上报当前 task 信息，Harness 据此重建状态）

不需要 WAL/预写日志:
  标签对账是最终一致性保证
  最坏情况: 孤儿实例运行到下一个对账周期 (5 分钟) 被发现
  经济影响: 5 分钟 × 1 台 ecs.c6.large ≈ ¥0.065 — 可接受
```

### 5.3 操作幂等性

```
create_instance:
  阿里云 RunInstances API 不支持 ClientToken 幂等 → 可能重复创建
  保障: 标签对账兜底

terminate_instance:
  阿里云 DeleteInstance(Force=True) 幂等 — 已终止的实例再次调用不报错
  AWS terminate_instances 同理

Bootstrap:
  非幂等 — 部分步骤重复执行会出错（如重复 git clone）
  保障: 失败策略默认 TERMINATE_AND_RETRY（销毁重来）
```

---

## 6. 部署拓扑

### 6.1 最小部署（MVP）

```
┌───────────────────────────────────────┐
│  Manager 机器（本地 Mac / 云服务器）    │
│                                       │
│  uvicorn elastic_agent.manager:app    │
│  监听: 0.0.0.0:8000                  │
│                                       │
│  ~/.elastic-agent/                    │
│    ├── config.yaml                    │
│    ├── registry.json                  │
│    ├── credentials.json               │
│    └── ssh keys                       │
└───────────────┬───────────────────────┘
                │
     VPC 内网 (阿里云) 或公网 (本地 Mac)
                │
        ┌───────┼───────┐
   Worker #1  Worker #2  ...
```

### 6.2 Manager 部署选项

| 方案 | 适用场景 | 优缺点 |
|------|---------|--------|
| 本地 Mac/PC | 开发调试 | 方便但 Manager 关机 Worker 断联 |
| 同区域云服务器 | 生产 | Manager 与 Worker 同 VPC，延迟低、安全 |
| Cloudflare Tunnel | 本地 + 远程 Worker | Manager 在本地但可被 Worker 连接 |

---

## 7. 实现顺序与依赖

### 7.1 分阶段路线

```
Phase A (Week 1-2): 能创建和销毁云实例
  T-002 数据模型
  T-003 阿里云 Provider
  T-004 AWS Provider
  T-005 Terraform 阿里云网络
  T-006 CDK AWS 网络
  T-011 NodeRegistry
  T-012 云端对账
  ── 里程碑: 能通过 Python 代码创建/销毁阿里云 ECS + AWS EC2 ──

Phase B (Week 2-3): Manager 能控制 Worker
  T-009 通信协议
  T-007 Worker Runtime 服务端
  T-008 Worker Runtime 客户端
  T-010 认证
  T-016 Manager FastAPI 服务
  ── 里程碑: Manager 能通过 WS 在 Worker 上远程执行命令并看到输出 ──

Phase C (Week 3-4): 文件同步 + 外部 API
  T-030 FileSyncManager (Worker → OSS/S3 主动同步)
  T-031 Harness file sync config 接口
  T-032 Worker 云存储凭证注入
  T-034 TaskSyncMapper (动态同步映射)
  T-035 REGISTER_SYNC_MAPPING / UNREGISTER_SYNC_MAPPING 协议消息
  T-036 FileSyncManager 上传错误处理
  T-037 FILE_SYNCED 事件类型
  T-013 外部 API 轨迹流
  T-014 外部 API 文件变更通知
  T-033 外部 API 文件内容从云存储读取
  T-015 外部 API 认证
  ── 里程碑: Worker 文件自动同步到 OSS; 前端从 OSS 读文件 + 通过 WS 看实时输出 ──

Phase D (Week 4-5): Bootstrap 自动化
  T-017 Claude Code AgentType
  T-018 Bootstrap Pipeline
  T-019~T-022 内置步骤
  T-023 失败处理
  T-026 凭证分发
  ── 里程碑: scale_out() 一个调用完成从创建到就绪的全流程 ──

Phase E (Week 5-6): 稳定性
  T-024 健康检查
  T-025 Drain 机制
  T-027 额度监控
  T-028 手动扩缩容 API
  T-029 基础 Web UI
  ── 里程碑: MVP 可用 ──

Phase F (Week 6-7): 测试
  T-100~T-120 全部测试
  ── 里程碑: 测试覆盖，可交付 ──
```

### 7.2 关键依赖链

```
T-002 (数据模型) ──→ T-003/T-004 (Provider) ──→ T-011 (Registry) ──→ T-012 (对账)
                                                                           │
T-009 (协议) ──→ T-007/T-008 (Runtime) ──→ T-010 (认证) ──→ T-016 (Manager)
                       │                                          │
                       ├──→ T-030/T-031 (FileSyncManager) ──→ T-034 (TaskSyncMapper)
                       │         │                                │
                       │         ├──→ T-035 (动态映射协议)         │
                       │         ├──→ T-036 (上传错误处理)         │
                       │         └──→ T-037 (FILE_SYNCED) ──→ T-033 (云存储文件读取)
                       │
                       └──→ T-013/T-014 (外部 API 轨迹/通知)
                                                                   │
T-017 (AgentType) ──→ T-018 (Bootstrap) ──→ T-019~T-022 (步骤) ──→ T-028 (扩缩容 API)
                                   │                │
                                                └──→ T-025 (Drain)
```

---

## 8. 测试策略

### 8.1 测试金字塔

```
                    ┌─────────────┐
                    │  E2E 全链路  │  1-2 个: 扩容→执行→缩容
                    │  (真实云)    │  跑一次 ~10min, ¥2
                    ├─────────────┤
                 ┌──┤  集成测试    │  5-6 个: WS 通信, 云生命周期, Bootstrap
                 │  │  (真实云)    │  各 ~3min
                 │  ├─────────────┤
              ┌──┤  │  单元测试    │  30+ 个: Provider mock, 状态机, 协议解析
              │  │  │  (纯内存)    │  全部 <5s
              └──┴──┴─────────────┘
```

### 8.2 测试隔离策略

| 层 | 如何隔离外部依赖 |
|---|-----------------|
| 单元测试 | Mock 云 SDK 响应 + DryRunProvider + 内存 EventBus |
| 集成测试 | 真实云（env var 控制跳过）+ 测试后 cleanup |
| E2E | 真实云 + 真实 Worker Runtime + 真实 Claude Code (可选 mock) |

### 8.3 DryRunProvider

不消耗任何云资源的 Provider 实现，记录所有操作到内存列表。用于：
- 验证 Manager 编排逻辑（scale_out/scale_in 流程）
- CI 中不需要云凭证的快速验证
- Harness 开发时的本地调试

---

## 9. 项目结构

```
elastic-agent/
├── src/elastic_agent/
│   ├── core/
│   │   ├── providers/          # CloudProvider 抽象 + 阿里云/AWS/DryRun 实现
│   │   ├── agents/             # AgentType 抽象 + Claude Code 实现
│   │   ├── credentials/        # CredentialPool + Provider 接口
│   │   ├── runtime/            # 通信协议 + Worker 客户端/服务端
│   │   ├── bootstrap/          # Pipeline + 失败策略 + 内置步骤
│   │   ├── registry/           # NodeRegistry (JSON 存储)
│   │   ├── monitor/            # 健康检查 + 额度监控 + 云端对账 + 事件总线
│   │   ├── scheduler/          # Drain 机制
│   │   ├── external_api/       # 轨迹流 + 文件传输 + 认证 + FastAPI Router
│   │   ├── logging/            # 结构化操作日志 + 日志轮转
│   │   └── security/           # Manager↔Worker 认证
│   ├── manager/                # FastAPI 应用 + ElasticAgentManager + 配置模型
│   ├── worker/                 # Runtime 服务入口 + 进程管理 + 日志落盘 + FileSyncManager
│   └── cli/                    # 命令行入口
├── dashboard/                  # React + Vite + Ant Design 前端
├── infra/
│   ├── aliyun/                 # Terraform (HCL)
│   └── aws/                    # CDK (Python)
├── tests/
│   ├── unit/
│   └── integration/
├── examples/                   # Harness 示例
├── pyproject.toml
└── README.md
```

---

## 10. 配置管理

### 10.1 配置层级

```
优先级从高到低:
  环境变量    ELASTIC_AGENT_PROVIDER_TYPE=aliyun
  配置文件    config.yaml
  代码默认值  ManagerConfig() Pydantic defaults

敏感值（AccessKey、API Key）只通过环境变量传入，不写入配置文件。
```

### 10.2 配置文件结构

```yaml
# config.yaml
server:
  host: "0.0.0.0"                 # Manager FastAPI 监听地址
  port: 8000                      # Manager FastAPI 监听端口

provider:
  type: "aliyun"                  # "aliyun" | "aws" | "dryrun"
  aliyun:
    region_id: "cn-hangzhou"
    image_id: "m-bp1xxxx"         # 自定义镜像 ID
    instance_type: "ecs.c6.large"
    security_group_id: ""         # Terraform output
    vswitch_id: ""                # Terraform output
    key_pair_name: "elastic-agent-key"
    ssh_key_path: "~/.ssh/elastic-agent-aliyun.pem"
    max_instances: 30
    spot_enabled: false           # 是否使用抢占式实例
  aws:
    region: "ap-northeast-1"
    ami_id: "ami-xxxxx"           # 自定义 AMI ID
    default_instance_type: "t3.large"
    security_group_ids: []        # CDK output
    subnet_id: ""                 # CDK output
    key_pair_name: "elastic-agent-key"
    ssh_key_path: "~/.ssh/elastic-agent-aws.pem"
    max_instances: 30

worker:
  ssh_user: "root"                # 阿里云 root, AWS ubuntu
  runtime_port: 8080
  heartbeat_interval: 30          # 秒
  unhealthy_threshold: 3          # 连续 N 次心跳超时标记 unhealthy

bootstrap:
  default_step_timeout: 300       # 单步默认超时（秒）
  max_retries: 2                  # Bootstrap 失败最大重试次数
  failure_strategy: "terminate_and_retry"

credentials:
  pool_file: "~/.elastic-agent/credentials.json"
  quota_threshold: 0.85           # 额度使用率告警阈值
  check_interval: 60              # 额度检查间隔（秒）

external_api:
  enabled: true
  trace_buffer_size: 5000         # per-task 实时缓冲条数

logging:
  operations_log: "~/.elastic-agent/operations.log"
  log_level: "INFO"               # DEBUG | INFO | WARNING | ERROR
  rotation: "daily"
  retention_days: 30
  worker_process_log_dir: "logs/" # Worker 进程日志目录（相对于任务工作目录）

monitor:
  health_check_interval: 30       # 秒
  reconcile_interval: 300         # 云端对账间隔（秒）

drain:
  timeout: 3600                   # 缩容等待超时（秒）

registry:
  path: "~/.elastic-agent/registry.json"
```

**敏感配置（仅通过环境变量传入，不写入配置文件）：**

| 环境变量 | 说明 |
|---|---|
| `ALICLOUD_ACCESS_KEY_ID` | 阿里云 RAM 子账号 AccessKey ID |
| `ALICLOUD_ACCESS_KEY_SECRET` | 阿里云 RAM 子账号 AccessKey Secret |
| `AWS_ACCESS_KEY_ID` | AWS IAM 用户 Access Key |
| `AWS_SECRET_ACCESS_KEY` | AWS IAM 用户 Secret Key |
| `AWS_SESSION_TOKEN` | 可选，STS 临时凭证 |
| `ELASTIC_AGENT_EXTERNAL_API_KEYS` | 外部 API 认证密钥（逗号分隔） |

### 10.3 IaC 输出集成

```bash
# 阿里云: Terraform 输出 → 配置
cd infra/aliyun/environments/cn-hangzhou
terraform apply
export SECURITY_GROUP_ID=$(terraform output -raw security_group_id)
export VSWITCH_ID=$(terraform output -raw vswitch_ids | jq -r '.[0]')

# AWS: CDK 输出 → 配置
cd infra/aws
cdk deploy
export SECURITY_GROUP_ID=$(aws cloudformation describe-stacks \
  --stack-name ElasticAgentNetworking \
  --query 'Stacks[0].Outputs[?OutputKey==`SecurityGroupId`].OutputValue' \
  --output text)
```

---

## 11. 技术选型

| 技术 | 选择 | 理由 |
|------|------|------|
| 语言 | Python 3.11+ | 与三个 Harness 统一，async 生态成熟 |
| 后端 | FastAPI | 原生 async + WebSocket + OpenAPI 文档 |
| 阿里云 SDK | alibabacloud_ecs20140526 (V2) | 类型提示，async 友好 |
| AWS SDK | boto3 | 标准，无替代 |
| IaC (阿里云) | **Terraform** | 阿里云唯一成熟 IaC 选择 |
| IaC (AWS) | **CDK Python** | 与项目同语言，L2 construct 高效，后续 Lambda/SSM 集成优势大 |
| 前端 | React + Vite + Ant Design | 与 CCM 前端统一 |
| 测试 | pytest + pytest-asyncio | 标准 |
| SSH | asyncssh | 原生 async |
| WebSocket | FastAPI native + websockets | 标准 |
| 文件监听 | watchdog | 跨平台 inotify/kqueue 封装 |
| 包管理 | uv | 快速安装，锁文件可靠 |
