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
> **核心策略：** 阿里云优先、SDK 直连管理实例、基础网络手动创建（控制台/CLI）、外部服务 API 暴露实时数据。

---

## TODO 清单

### P0 — 必须完成

- [ ] **T-001** 项目脚手架搭建（pyproject.toml、目录结构、CI 基础）
- [ ] **T-002** CloudProvider 抽象基类 + Instance/InstanceConfig 数据模型
- [ ] **T-003** 阿里云 ECS Provider（alibabacloud SDK V2.0 直连）
- [ ] **T-004** AWS EC2 Provider（boto3 SDK 直连）
- [ ] **T-005** 阿里云基础网络 — 控制台手动创建 VPC/VSwitch/安全组/密钥对
- [ ] **T-006** AWS 基础网络（如需）— 控制台手动创建 VPC/Subnet/SG/KeyPair
- [ ] **T-007** Worker Runtime 服务端（Worker 侧：进程执行、日志流、文件操作）
- [ ] **T-008** Worker Runtime 客户端（Manager 侧：远程调用抽象）
- [ ] **T-009** Manager ↔ Worker 通信协议（WebSocket 反向连接 + 消息类型）
- [ ] **T-010** Manager ↔ Worker 认证（per-Worker Bearer Token）
- [ ] **T-011** NodeRegistry（节点状态持久化，JSON 文件 + 线程安全锁）
- [ ] **T-012** 云端标签对账（启动时 + 周期性扫描，清理孤儿实例）
- [ ] **T-013** 内部轨迹流 — EventBus LOG 事件分发（Harness 回调、phase 检测、轨迹缓存）
- [ ] **T-014** 文件同步通知 — FILE_SYNCED → Webhook 推送到 Harness 回调 URL
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
- [ ] **T-017** Claude Code AgentType（安装命令、启动命令、NDJSON 解析、session_id 提取、--resume 命令组装、健康检查探针）
- [ ] **T-050** [EA] TaskRegistry — task→worker 映射，JSON 持久化，崩溃恢复，Worker 下线清理  `01 §3.9`
- [ ] **T-051** [EA] TaskScheduler — 容量感知分发（WorkerCapacity 检查，Harness 可扩展）  `01 §3.9`
- [ ] **T-052** [EA] TaskRouter — 后续命令路由到 Worker（含 --resume 自动组装）  `01 §3.9`
- [ ] **T-053** [EA] WebhookEmitter — 事件回调 + HMAC 签名 + 重试 + 死信队列  `01 §3.9`

### P1 — 应该完成

- [ ] **T-018** Bootstrap Pipeline（可插拔步骤、per-step 超时、失败策略枚举）
- [ ] **T-019 ~ T-022** 内置 Bootstrap 步骤（系统初始化 / Agent 安装 / Runtime 部署 / Harness 代码）
- [ ] **T-023** Bootstrap 失败处理（terminate-retry / retry-from-failed / leave-for-debug）
- [ ] **T-024** Worker 多层健康检查（L1 VM + L2 Runtime + L3 Agent 进程）
- [ ] **T-025** 优雅缩容 Drain（draining 标记 → 等待完成 → 回收凭证 → 终止）
- [ ] **T-026** CredentialPool 账号池管理（accounts.json 加载、pool_status.json 持久化、分组、分配/回收）
- [ ] **T-041** ClaudeOAuthProvider 自动登录（Worker 侧 14 步 OAuth 流程：171mail + Playwright + mitmproxy）
- [ ] **T-042** Worker Bootstrap 登录步骤（为每个分配的 Slot 执行自动登录，串行执行，失败回滚）
- [ ] **T-043** Worker 侧额度监控（每 60s 调用 usage API，Token 续期，QUOTA_STATUS 事件上报）
- [ ] **T-044** Manager 侧 QuotaMonitor（汇聚 Worker 额度数据，阈值检测，QUOTA_WARNING 事件）
- [ ] **T-045** 自动轮换（额度耗尽 → 等待当前任务完成 → 分配新账号 → 登录/分发凭证 → 恢复槽位）
- [ ] **T-046** 冷却恢复（5h 窗口到期后自动标记账号为 available）
- [ ] **T-047** CREDENTIAL_LOGIN / QUOTA_STATUS / CREDENTIAL_ROTATE 协议消息
- [ ] **T-028** 手动扩缩容 API（scale_out / scale_in / remove_node）
- [ ] **T-029** 基础 Web UI（节点列表、状态卡片、手动操作）

### 测试

- [ ] **T-100 ~ T-109** 单元测试（Provider mock / Registry CRUD / Protocol 序列化 / Bootstrap 状态机 / Drain 状态机 / 对账逻辑 / 轨迹流过滤 / 文件监听）
- [ ] **T-110 ~ T-115** 集成测试（Manager↔Worker WS 通信 / 阿里云全生命周期 / AWS 全生命周期 / Bootstrap E2E / 外部 API E2E / 扩容→执行→缩容全链路）
- [ ] **T-116** 基础网络验证 — 阿里云资源连通性测试
- [ ] **T-117** 基础网络验证 — AWS 资源连通性测试（如需）
- [ ] **T-118** DryRunProvider 空跑验证
- [ ] **T-119** 单元测试：FileSyncManager 防抖逻辑 + 同步清单生成
- [ ] **T-120** 集成测试：Worker 文件变更 → OSS/S3 同步 → 外部 API 读取一致性
- [ ] **T-121** 单元测试：日志落盘完整性（进程正常退出 + 崩溃退出场景）
- [ ] **T-122** 集成测试：Worker 日志落盘 → FileSyncManager → OSS → 外部 API 读取一致性
- [ ] **T-138** [EA] TaskRegistry：CRUD + 持久化 + 崩溃恢复 + Worker 下线清理
- [ ] **T-139** [EA] TaskScheduler：容量检查 + 多 Worker 选择 + 无空闲返回 None
- [ ] **T-140** [EA] TaskRouter：路由到正确 Worker + --resume 自动组装 + Worker 离线错误
- [ ] **T-141** [EA] WebhookEmitter：HMAC 签名 + 重试延迟 + 死信队列 + 幂等

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
  Worker → Manager → EventBus → 内部消费（phase 检测、进度监控）
  每行 Claude Code stdout/stderr (NDJSON) 产生一个 LogEvent
  Worker Runtime 同时将原始输出写入本地日志文件（per-task NDJSON）
  本地日志文件通过 FileSyncManager 持久化到 OSS/S3（数据流 ③）
  外部访问日志走 OSS（前端/ABE 从 OSS 读取 NDJSON 文件）

数据流 ③：文件同步（持续，中等流量）
  Worker FileSyncManager: inotify → 分层防抖 → 增量上传 OSS/S3
  同步范围: workspace 文件 + NDJSON 日志文件（统一由 FileSyncManager 管理）
  Worker → Manager: FILE_CHANGE 事件 (内部使用，Harness 逻辑)
  Worker → Manager: FILE_SYNCED 事件 (确认文件已同步到 OSS/S3)
  外部读取: 直接从 OSS/S3 读取（不走 Worker）
  外部通知: FILE_SYNCED 事件 → Webhook → ABE，ABE 从 OSS 读取最新文件

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

**设计目标：** 外部服务（ABE 前端、监控系统、第三方集成）通过 Manager REST API 查询集群状态和文件元数据，通过 OSS 读取文件内容和日志数据，通过 Webhook 接收文件同步通知。实时轨迹流仅供框架内部使用（Harness 回调、phase 检测）。

#### 数据流路径

```
实时轨迹流（框架内部）:
  Worker Claude Code stdout
    → Worker Runtime 逐行读取
    → LOG 消息 via WS
    → Manager EventBus
    → 内部消费者（Harness 事件回调、轨迹缓存、phase 检测）
  外部消费者（ABE 前端等）通过轮询 OSS 上的 NDJSON 日志文件获取聊天数据

文件访问:
  外部请求 GET /api/external/files/{task_id}/{path}
    → Manager 从 OSS/S3 读取（不走 Worker）
    → 响应包含 synced_at（调用者知道数据新鲜度）

文件同步通知:
  Worker FileSyncManager 上传完成
    → FILE_SYNCED 事件 via WS → Manager
    → Webhook 推送 task.file.synced → ABE
    → ABE 从 OSS 读取最新文件
```

#### 轨迹存储：实时缓冲 + 持久化双层

```
                  ┌─────────────────────────┐
                  │   实时轨迹缓冲 (内存)     │
                  │                         │
  LOG 事件 ──────▶│  环形缓冲 (per-task)    │──────▶ 内部实时订阅者 (Harness 回调)
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
  - 实时缓冲: 服务框架内部的实时订阅者（Harness 回调、phase 检测）和近期 REST 查询
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
  REST 请求必须携带 API Key:
    - URL 参数: ?api_key=xxx
    - 或 Header: Authorization: Bearer xxx

  API Key 在 Manager 配置文件中定义:
    external_api:
      api_keys:
        - "key-for-frontend"
        - "key-for-monitoring"

  Webhook 回调认证:
    Harness 注册回调 URL 时提供 secret
    Manager 推送时用 HMAC-SHA256 签名 payload

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
  │    7. 同时发送 FILE_CHANGE 事件到 Manager（内部使用，Harness 逻辑）     │
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
    T-014（文件变更通知）: Worker → Manager FILE_CHANGE 事件（内部，Harness 逻辑用）
    T-033（文件内容读取）: 外部 → Manager → OSS/S3 → 返回内容（不走 Worker）
    T-037（文件同步确认）: Worker → Manager FILE_SYNCED → Webhook → ABE 从 OSS 读取
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
Layer 1: Agent 凭证（框架管理，全自动）
  Claude Max 订阅账号的 OAuth token（accessToken + refreshToken）
  框架负责: 自动登录获取 → 分发到 Worker → 额度监控 → 自动轮换 → 回收
  存储: accounts.json（Manager 本地账号池定义）
  运行时状态: pool_status.json（各账号额度、可用性、分配状态）

Layer 2: 应用凭证（Harness 声明，框架传递）
  Git SSH key、WandB API key、HuggingFace token 等
  Harness 通过 get_app_credentials() 声明需要哪些
  Bootstrap 时从 Manager 安全传递到 Worker（环境变量或文件）
```

#### 账号池

```
账号池配置 (~/.elastic-agent/accounts.json):
{
  "accounts": [
    {
      "id": "prod-1",
      "email": "user1@171mail.com",
      "email_token": "sk-mailapi-abc...",   // 171mail API token，用于自动登录
      "group": "high_quota",                // 账号分组
      "enabled": true
    },
    {
      "id": "prod-2",
      "email": "user2@171mail.com",
      "email_token": "sk-mailapi-def...",
      "group": "high_quota",
      "enabled": true
    },
    {
      "id": "edit-1",
      "email": "user3@171mail.com",
      "email_token": "sk-mailapi-ghi...",
      "group": "standard",
      "enabled": true
    }
  ],
  "groups": {
    "high_quota": {"description": "主账号，用于生产槽位（Opus 密集）"},
    "standard":   {"description": "副账号，用于修改槽位（Sonnet 轻量）"}
  },
  "weekly_reserve_per_day": 0              // 7d 每天预留百分比（0=不启用）
}

设计说明:
  email_token 直接放在 accounts.json 中（与 agent-ml-research 不同）。
  agent-ml-research 将 email_tokens 放在独立的 email_tokens.json 文件。
  合并到一个文件更简单，减少配置文件数量。敏感性相同（accounts.json 本身就是敏感文件）。

运行时状态 (~/.elastic-agent/pool_status.json, 框架自动维护):
{
  "last_updated": "2026-05-19T12:34:56Z",
  "accounts": {
    "prod-1": {
      "email": "user1@171mail.com",
      "group": "high_quota",
      "assigned_to": "aliyun:i-bp1xxx",    // 当前分配给哪个 Worker
      "slot_type": "production",            // 分配给哪种槽位
      "config_dir": "/root/.claude-prod",   // Worker 上的凭证目录
      "five_hour": {"utilization": 45, "resets_at": "2026-05-19T17:34:56Z"},
      "seven_day": {"utilization": 62, "resets_at": "2026-05-26T00:00:00Z"},
      "available": true,                      // 综合判定，见下方规则
      "login_status": "logged_in",          // logged_in / expired / login_failed / not_logged_in
      "stale": false,                       // Token 读取或 refresh 失败
      "error": null,                        // 当前错误码（auth_expired / rate_limited / ...）
      "backoff_until": null,                // 额度 API 限流退避到期时间
      "last_login_at": "2026-05-18T10:00:00Z",
      "last_quota_check": "2026-05-19T12:34:50Z",
      "last_used": "2026-05-19T12:00:00Z"  // 上次被选中使用的时间（用于轮换排序）
    }
  }
}
```

#### 自动登录（OAuth 14 步流程）

```
框架内置 ClaudeOAuthProvider，在 Worker 上执行自动登录。
（设计参考 agent-ml-research 的 account_login.py）

触发时机:
  1. Bootstrap 时: 为每个分配的凭证执行登录
  2. Token 过期时: refreshToken 失效，需要重新登录
  3. 手动触发: 运维 API 触发重新登录

登录流程（在 Worker 上执行）:
  Manager 发送 CREDENTIAL_LOGIN 命令到 Worker Runtime:
    {account_id, email, email_token, config_dir}

  Worker Runtime 执行登录脚本:
    1. 文件锁防并发（per-account）
    2. 调用 171mail API 触发发送 magic link:
       POST https://b.171mail.com/api/v1/claude/send {email}
       → 返回 deviceId + clientSha
    3. 轮询 171mail 收件箱获取 magic link:
       GET https://b.171mail.com/api/v1/getClaudeMessage?token={email_token}
       → 每 2s 轮询，超时 90s
    4. 验证 magic link:
       POST https://b.171mail.com/api/v1/claude/verify {link, deviceId, clientSha, email}
       → 返回 Anthropic session cookie + sessionKey
    5. 启动 mitmproxy（修正 Claude CLI OAuth redirect_uri bug）
    6. 启动 Claude CLI: claude auth login --email {email}
       环境变量: CLAUDE_CONFIG_DIR={config_dir}, HTTPS_PROXY=mitm
    7. 从 CLI stdout 提取 OAuth authorize URL
    8. Playwright 打开 Chrome（需要 Xvfb）:
       a. 注入 session cookie
       b. 身份验证: GET https://claude.ai/api/account 确认 email 匹配
       c. 导航到 OAuth URL
       d. 等待 Cloudflare 验证通过
       e. 点击 "Authorize" 按钮
       f. 捕获 callback URL 中的 code + state
    9. 将 code+state 发送到 CLI 的 localhost 回调端口
   10. CLI 完成 token 交换，写入 {config_dir}/.credentials.json:
       {claudeAiOauth: {accessToken, refreshToken, expiresAt}}
   11. 验证: claude auth status 确认登录成功
   12. 清理: 关闭 Playwright、停止 mitmproxy、释放文件锁

  Worker Runtime 上报 CREDENTIAL_LOGIN_RESULT:
    {account_id, ok, error?, expires_at?}

Worker 依赖（Bootstrap 时安装）:
  - Python 3.11+ (Worker Runtime 自带)
  - Playwright + playwright-stealth + Chrome
  - Xvfb (Linux headed Chrome 需要)
  - mitmproxy (mitmdump)
  - Node.js + Claude Code CLI

登录耗时: 约 30-60 秒/账号
并发: 同一 Worker 上串行登录（文件锁），不同 Worker 可并行
```

#### 额度监控

```
两级额度监控:

Level 1: Worker 侧（周期性检查）
  Worker Runtime 对本机所有活跃凭证轮流检查:
    检查间隔: 每个账号之间随机延迟 170-190s（防反检测，参考 agent-ml-research）
    单个账号的实际检查周期 ≈ N_accounts × 180s（如 4 个账号 → 每 12 分钟检查一次）

  每个账号的检查步骤:
    1. 读取 {config_dir}/.credentials.json 获取 accessToken
    2. Token 续期（如果 now_ms >= expiresAt - 5min）:
       POST https://console.anthropic.com/v1/oauth/token
       Content-Type: application/x-www-form-urlencoded
       {grant_type=refresh_token, refresh_token=..., client_id=9d1c250a-...}
       → 成功: 更新 accessToken + expiresAt，写回 .credentials.json
       → 失败: 降级处理:
         a. 先尝试用过期 token 继续（Anthropic 有短暂宽限期）
         b. 如果额度 API 也返回 auth error → 标记 stale=true, error="refresh_failed"
         c. 上报 Manager → Manager 触发重新登录（自动执行 14 步 OAuth）
         d. 重新登录也失败 → 标记 login_status="login_failed" → 该账号退出分配池
    3. 调用额度 API:
       GET https://api.anthropic.com/api/oauth/usage
       Headers: Authorization: Bearer {accessToken}
               anthropic-beta: oauth-2025-04-20
    4. 解析响应:
       {
         "five_hour":  {"utilization": 45, "resets_at": "2026-05-19T17:34:56Z"},
         "seven_day":  {"utilization": 62, "resets_at": "2026-05-26T00:00:00Z"}
       }
    5. 发送 QUOTA_STATUS 事件到 Manager:
       {credential_id, five_hour_pct, seven_day_pct, five_hour_resets_at,
        seven_day_resets_at, available}

  额度 API 限流处理:
    返回 rate_limit_error → 使用本地缓存值（30min 内有效）+ 标记 stale
    指数退避: 180s → 360s → ... → 2880s（上限）
    额度 API 限流不影响 Claude Code 正常使用（两个独立的 API）

Level 2: Manager 侧（汇聚 + 决策）
  QuotaMonitor 收集所有 Worker 的 QUOTA_STATUS 事件:
    1. 更新 pool_status.json 中的额度数据
    2. 5h 窗口阈值检测:
       five_hour_pct >= quota_threshold (85%) → 触发 QUOTA_WARNING 事件
       five_hour_pct >= 95% 或 API 返回 rate_limit → 触发轮换
    3. 7d 窗口预算管理:
       配置: weekly_reserve_per_day（每天预留百分比，默认 0 = 不启用）
       动态阈值 = 100 - (剩余天数 × weekly_reserve_per_day)
       seven_day_pct >= 动态阈值 → 标记 available=false + 发出 QUOTA_WARNING
       用途: 防止密集做书在几天内耗尽整周额度
    4. Harness 可订阅 QUOTA_WARNING 发送告警（飞书/Webhook）
```

#### 自动轮换（切号）

```
触发条件:
  - 额度 >= 95%（接近硬限制）
  - Claude Code 进程返回 rate_limit 错误
  - Token 过期且 refresh 失败（需要重新登录）

轮换流程:
  1. QuotaMonitor 检测到账号 "prod-1" 在 Worker W1 上额度耗尽
  2. CredentialPool 查找替代账号:
     过滤: 同 group + enabled + available (five_hour < threshold) + 未被分配
     排序: five_hour_pct 升序（最空闲优先）
  3. 如果找到替代账号 "prod-2":
     a. 等待当前任务完成（不中断执行中的 Claude Code 进程）
     b. 检查 "prod-2" 的登录状态:
        - 已登录且 token 有效 → 直接分发凭证文件到 Worker
        - token 过期 → 先 refresh token
        - 未登录 → 在 Worker 上执行自动登录流程
     c. 将新凭证写入 Worker 的 config_dir（覆盖旧凭证文件）
     d. 更新 pool_status.json:
        "prod-1": assigned_to=null, available=false（冷却中，5h 窗口后自动恢复）
        "prod-2": assigned_to=W1, slot_type=production
     e. 标记槽位可用 → 恢复接受新任务
     f. 操作日志: credential_rotated {node_id, old_id: prod-1, new_id: prod-2, reason: quota_exceeded}
  4. 如果没有替代账号（所有账号都耗尽）:
     → 发送 CREDENTIAL_EXHAUSTED 事件
     → 该 Worker 的对应槽位暂停接受任务
     → Harness 回调: 可选告警运维、排队等待、尝试重新登录已冷却账号

冷却恢复:
  被标记 available=false 的账号，当 resets_at 时间到达时:
  → 额度 API 下一次检查发现 five_hour_pct 降到阈值以下
  → 自动标记 available=true → 可再次被分配

不中断原则:
  轮换永远不中断正在执行的 Claude Code 进程
  等待当前任务完成（PROCESS_EXIT）后再切换凭证
  如果任务很长（如 Audiobook 生产 2h），则在任务完成前就标记"下次任务用新凭证"
```

#### 账号-Worker 绑定（IP 亲和性）

```
核心规则:
  1. 同一个账号在同一时刻只能登录在一台 Worker 上（绝对互斥）
  2. 如果账号上次登录的 Worker 仍然在线，优先在该 Worker 上继续使用
  3. 每台 Worker 同时登录的账号数有上限（可配置）

数据模型 (pool_status.json 中):
  "prod-1": {
    ...
    "assigned_to": "aliyun:i-bp1xxx",     // 当前绑定的 Worker（null = 未绑定）
    "last_worker": "aliyun:i-bp1xxx",     // 上次登录的 Worker（即使已回收也记录）
    "last_worker_alive": true              // Manager 定期刷新（从 NodeRegistry 判断）
  }

分配算法:

  allocate(group, target_worker) → account_id | None:

    Step 1: 已绑定该 Worker 的同 group 账号（优先复用）
      candidates = [a for a in accounts
                    if a.group == group
                    and a.assigned_to == target_worker
                    and a.available]
      if candidates: return candidates[0]

    Step 2: 上次在该 Worker 登录过、当前未被分配的账号（亲和复用，免登录）
      candidates = [a for a in accounts
                    if a.group == group
                    and a.assigned_to is None
                    and a.last_worker == target_worker
                    and a.available]
      if candidates: return sort_by_last_used(candidates)[0]

    Step 3: 从未分配的账号中选（需要登录）
      candidates = [a for a in accounts
                    if a.group == group
                    and a.assigned_to is None
                    and a.available]
      if candidates: return sort_by_last_used(candidates)[0]

    Step 4: 全部已分配或不可用
      return None  → 排队等待或告警

  互斥保证:
    - assigned_to 是排他字段: 一个账号同时只能 assigned_to 一个 Worker
    - 分配前检查: if account.assigned_to is not None and account.assigned_to != target_worker → 跳过
    - Manager 单进程: pool_status.json 的读写在同一进程内，不需要分布式锁

  每 Worker 最大账号数限制:
    分配前检查: count(accounts where assigned_to == target_worker) < max_accounts_per_worker
    超过 → 拒绝分配 → 等待该 Worker 上的账号被回收

  轮换时的亲和性:
    账号额度耗尽需要轮换时:
      1. 回收旧账号: assigned_to = null（但保留 last_worker）
      2. 分配新账号: 走上述 allocate() 流程
      3. 如果 Step 2 命中 → 新账号上次就在这个 Worker 上 → 不需要重新登录（token 还在本地）
      4. 如果 Step 3 命中 → 需要在 Worker 上执行自动登录

  Worker 下线时:
    清理: 所有 assigned_to == 该 Worker 的账号 → assigned_to = null
    保留: last_worker 不清理（Worker 重新上线时可以亲和复用）
```

#### 多 Slot Worker 的凭证隔离

```
多 Slot 凭证隔离策略:

  Primary Slot (production):
    - 从 "high_quota" 分组分配
    - CLAUDE_CONFIG_DIR=/root/.claude-prod/
    - 长时间运行，承担主要创作工作

  Secondary Slot(s) (edit):
    - 从 "standard" 分组分配
    - CLAUDE_CONFIG_DIR=/root/.claude-edit-{n}/
    - 短时间运行，按需启动

  隔离机制:
    - 每个 Claude Code 进程通过不同的 CLAUDE_CONFIG_DIR 使用独立凭证
    - Bootstrap 时: Manager 为每个 Slot 分配账号 → 在 Worker 上逐个登录
    - 额度独立: 各 Slot 的账号额度互不影响
    - 回收时: 按 Slot 分别回收到对应 group

  凭证分配策略:
    - Primary slot → 从 "high_quota" 分组的可用账号中选择
    - Edit slot → 从 "standard" 分组的可用账号中选择
    - 如果 "standard" 耗尽 → 临时使用 "high_quota"（发出告警）
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
  ├── 消费者: 轨迹流内部端点 (订阅 LOG 事件 → Harness 回调、phase 检测)
  ├── 消费者: Webhook 发送器 (订阅 FILE_SYNCED 事件 → 推送到 Harness 注册的回调 URL)
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

### 3.9 任务管理

框架内置通用的任务跟踪、调度、路由和通知能力，Harness 不需要自己实现这些。

#### TaskRegistry

```
任务注册表，管理 task→worker 的映射关系。

  数据模型 (per-task):
    task_id:      "123"
    worker_id:    "aliyun:i-bp1xxx"
    session_id:   "claude-session-abc"     (Claude Code AgentType 自动提取)
    status:       "running" | "completed" | "failed"
    metadata:     {}                        (Harness 自定义字段，如 book_slug)
    created_at:   "2026-05-17T10:00:00Z"
    updated_at:   "2026-05-17T11:45:00Z"

  持久化: ~/.elastic-agent/task_registry.json (每次更新写入)
  崩溃恢复: Manager 重启时读取 → 对比 NodeRegistry 在线 Worker → 清理已下线的任务
  Worker 下线: 该 Worker 上的所有任务标记为 "worker_lost"

  API:
    manager.task_registry.register(task_id, worker_id, metadata)
    manager.task_registry.get(task_id) → TaskRecord | None
    manager.task_registry.update(task_id, session_id=..., status=...)
    manager.task_registry.unregister(task_id)
    manager.task_registry.list_by_worker(worker_id) → list[TaskRecord]
```

#### TaskScheduler

```
容量感知的任务分发。根据 WorkerCapacity 找到有空闲容量的 Worker。

  调度算法:
    1. 遍历所有 READY 状态的 Worker
    2. 对比 TaskRegistry 中该 Worker 的活跃任务数 vs WorkerCapacity.max_concurrent_tasks
    3. 返回有空闲容量的 Worker（多个时选任务最少的）
    4. 无空闲 Worker → 返回 None（调用方决定排队或拒绝）

  Harness 可扩展:
    AudiobookHarness 扩展 WorkerCapacity 为 production_slots + edit_slots
    → TaskScheduler 的容量检查自动适配子类的字段

  API:
    manager.task_scheduler.find_available_worker(capacity_requirement=None) → worker_id | None
```

#### TaskRouter

```
后续命令路由：将命令发送到任务所在的 Worker。

  路由逻辑:
    1. 从 TaskRegistry 查找 task_id → worker_id
    2. 检查 Worker 是否在线（NodeRegistry）
    3. 通过 Worker Runtime WS 连接发送 EXECUTE 命令
    4. 返回进程 task_id（用于跟踪输出）

  --resume 支持:
    TaskRouter 结合 Claude Code AgentType 自动组装 --resume 命令:
    if record.session_id:
      command = agent_type.build_command(prompt=message, session_id=record.session_id)
    else:
      command = agent_type.build_command(prompt=message)

  错误处理:
    task_id 不存在 → NotFoundError
    Worker 离线 → WorkerOfflineError
    Worker 容量满 → CapacityFullError

  API:
    manager.task_router.send_command(task_id, command, env=None) → process_task_id
    manager.task_router.send_followup(task_id, message) → process_task_id  (自动 --resume)
```

#### WebhookEmitter

```
向外部系统推送事件通知。

  注册:
    Harness 在启动时或通过 produce 请求动态注册回调 URL:
    manager.webhook_emitter.register(target_id, url, secret)

  发送:
    manager.webhook_emitter.emit(event_type, task_id, data)
    → 构造 JSON body
    → HMAC-SHA256 签名 (X-Elastic-Agent-Signature)
    → POST 到注册的 URL
    → 非 2xx → 重试 (1s, 5s, 30s, 5min, 30min)
    → 5 次失败 → 写入死信队列

  死信队列: ~/.elastic-agent/webhook_dead_letters.json
  操作日志: 每次发送/重试/失败都写入 operations.log

  API:
    manager.webhook_emitter.register(target_id, url, secret)
    manager.webhook_emitter.emit(event_type, task_id, data)
    manager.webhook_emitter.replay_dead_letters()
```

---

## 4. 前置准备

MVP 不使用 Terraform 或 CDK。基础网络资源通过阿里云控制台或 CLI 一次性手动创建。

### 4.1 阿里云资源准备

在阿里云控制台中准备以下资源，将 ID 填入 config.yaml：

| 资源 | 说明 | 配置项 |
|---|---|---|
| VPC | 已有 VPC 即可，不需要新建 | — |
| VSwitch | Manager 和 Worker 在同一 VSwitch | `provider.aliyun.vswitch_id` |
| 安全组 | 允许 VPC 内 8080 端口（Worker Runtime）+ 22 端口（SSH Bootstrap） | `provider.aliyun.security_group_id` |
| 密钥对 | SSH 登录 Worker 用 | `provider.aliyun.key_pair_name` + `ssh_key_path` |
| 自定义镜像（可选） | 预装 Ubuntu + Python 3.11 + Node.js 20 可加速 Bootstrap | `provider.aliyun.image_id` |

Worker 使用公网 IP 出站（创建 ECS 时分配），不需要 NAT Gateway。

### 4.2 AWS 资源准备（如需）

同理，在 AWS 控制台准备 VPC/Subnet/Security Group/Key Pair，填入 config.yaml 的 `provider.aws` 部分。

### 4.3 后续演进

当环境数量增多或需要版本化管理时，可引入 Terraform（阿里云）或 CDK（AWS）管理上述资源。MVP 阶段手动创建即可。

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
  T-005 阿里云基础网络（控制台创建）
  T-006 AWS 基础网络（控制台创建，如需）
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
  T-013 内部轨迹流 (EventBus LOG 分发)
  T-014 文件同步通知 (FILE_SYNCED → Webhook)
  T-033 外部 API 文件内容从云存储读取
  T-015 外部 API 认证
  ── 里程碑: Worker 文件自动同步到 OSS; 外部通过 OSS 读文件 + Webhook 接收通知 ──

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
                       └──→ T-013/T-014 (内部轨迹流/Webhook 通知)
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
│   │   ├── task/               # TaskRegistry + TaskScheduler + TaskRouter
│   │   ├── webhook/            # WebhookEmitter + 签名 + 重试
│   │   ├── scheduler/          # Drain 机制
│   │   ├── external_api/       # 轨迹流 + 文件传输 + 认证 + FastAPI Router
│   │   ├── logging/            # 结构化操作日志 + 日志轮转
│   │   └── security/           # Manager↔Worker 认证
│   ├── manager/                # FastAPI 应用 + ElasticAgentManager + 配置模型
│   ├── worker/                 # Runtime 服务入口 + 进程管理 + 日志落盘 + FileSyncManager
│   └── cli/                    # 命令行入口
├── dashboard/                  # React + Vite + Ant Design 前端
├── scripts/                    # 部署辅助脚本（可选）
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
    security_group_id: ""         # 阿里云控制台创建
    vswitch_id: ""                # 阿里云控制台创建
    key_pair_name: "elastic-agent-key"
    ssh_key_path: "~/.ssh/elastic-agent-aliyun.pem"
    max_instances: 30
    spot_enabled: false           # 是否使用抢占式实例
  aws:
    region: "ap-northeast-1"
    ami_id: "ami-xxxxx"           # 自定义 AMI ID
    default_instance_type: "t3.large"
    security_group_ids: []        # AWS 控制台创建
    subnet_id: ""                 # AWS 控制台创建
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
  accounts_file: "~/.elastic-agent/accounts.json"       # 账号池定义
  pool_status_file: "~/.elastic-agent/pool_status.json" # 运行时状态（自动维护）
  quota_threshold: 0.85           # 5h 额度使用率告警阈值
  quota_check_delay_min: 170      # 账号间检查最小延迟（秒，防反检测）
  quota_check_delay_max: 190      # 账号间检查最大延迟（秒，随机化）
  weekly_reserve_per_day: 0       # 7d 每天预留百分比（0 = 不启用 7d 预算管理）
  rotation_strategy: "least_used_first"  # 轮换策略
  login_timeout: 240              # 单次自动登录超时（秒）
  max_accounts_per_worker: 4      # 每 Worker 最大同时登录账号数

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

### 10.3 获取资源 ID

在阿里云控制台创建资源后，将 ID 填入 config.yaml：
- VSwitch ID: 阿里云控制台 → VPC → 交换机 → 复制 ID
- 安全组 ID: 阿里云控制台 → ECS → 安全组 → 复制 ID
- 密钥对名称: 阿里云控制台 → ECS → 密钥对 → 复制名称

---

## 11. 技术选型

| 技术 | 选择 | 理由 |
|------|------|------|
| 语言 | Python 3.11+ | 与三个 Harness 统一，async 生态成熟 |
| 后端 | FastAPI | 原生 async + WebSocket + OpenAPI 文档 |
| 阿里云 SDK | alibabacloud_ecs20140526 (V2) | 类型提示，async 友好 |
| AWS SDK | boto3 | 标准，无替代（MVP 阿里云优先，AWS 后续） |
| 前端 | React + Vite + Ant Design | 与 CCM 前端统一 |
| 测试 | pytest + pytest-asyncio | 标准 |
| SSH | asyncssh | 原生 async |
| WebSocket | FastAPI native + websockets | 标准 |
| 文件监听 | watchdog | 跨平台 inotify/kqueue 封装 |
| 包管理 | uv | 快速安装，锁文件可靠 |
| 浏览器自动化 | Playwright + playwright-stealth | 自动登录 OAuth 流程 |
| HTTP 代理 | mitmproxy (mitmdump) | 修正 CLI OAuth redirect_uri |

---

## 12. 凭证管理体系详解

> 本节是 §3.6 的实现参考，提供 Claude Code 账号全生命周期管理的完整技术细节。
> 设计参考 [agent-ml-research](https://github.com/caoxiaoyuyuyuyuyu/agent-ml-research) 的凭证管理系统。

### 12.1 原理概述

Claude Code CLI 使用 Claude Max 订阅账号（而非 API Key）运行。每个账号通过 OAuth 流程获取 access/refresh token 对，存储在本地 `.credentials.json` 文件中。Claude Max 有 **5 小时滑动窗口**的 token 使用量限制——高并发场景下单账号很快耗尽。

框架的凭证管理体系解决以下问题：

```
  账号登录         额度监控          自动轮换          多槽位隔离
    │                │                │                │
    ▼                ▼                ▼                ▼
171mail 魔法链接   Anthropic        等当前任务完成    每个 Claude Code
  + Playwright     usage API        → 换一个          进程使用独立的
  + mitmproxy      5h/7d 窗口       额度充足的        CLAUDE_CONFIG_DIR
  = OAuth token    → 阈值告警        账号              = 互不干扰
```

完整的凭证生命周期：

```
运维配置 accounts.json（email + 171mail token）
    │
    ▼
Bootstrap: Manager 为每个 Slot 从 CredentialPool 分配账号
    │
    ▼
Worker 上执行自动登录: 171mail → OAuth → .credentials.json
    │
    ▼
正常运行: Claude Code 使用 token 调用 API
    │
    ├── Worker 每 60-90s 检查额度 ─→ QUOTA_STATUS 上报 Manager
    │       │
    │       ├── five_hour < 85% → 正常
    │       ├── five_hour >= 85% → QUOTA_WARNING（告警）
    │       └── five_hour >= 95% → 触发轮换
    │
    ├── Token 即将过期（< 5min）→ 自动 refresh → 续期
    │       │
    │       └── refresh 失败 → 重新执行自动登录
    │
    └── 任务完成/Worker 销毁 → 凭证回收到池子
```

### 12.2 OAuth 登录流程技术细节

自动登录在 Worker 上执行（因为 credentials.json 需要在 Worker 本地文件系统）。

#### 为什么需要 mitmproxy

Claude CLI 2.1.x 的 `auth login` 命令存在一个 OAuth bug：它在 token 交换请求中使用 `redirect_uri: http://localhost:{port}/callback`，但 Anthropic OAuth 服务端期望的是 `https://platform.claude.com/oauth/code/callback`。mitmproxy 拦截这个 POST 请求并修正 `redirect_uri`，同时去掉 `code` 参数中的 `#state` 后缀。

```
mitmproxy addon 做了两件事:
  1. POST /v1/oauth/token 请求体中:
     redirect_uri: http://localhost:N/callback
       → 改为: https://platform.claude.com/oauth/code/callback
  2. code 参数:
     abc123#state → abc123（去掉 #state 后缀）
```

mitmproxy 只在登录时使用（约 30-60 秒），登录完成后关闭。正常的 Claude Code 运行不经过 mitmproxy。

#### 关键 API 端点

| 用途 | URL | 方法 | 说明 |
|---|---|---|---|
| 触发发送 magic link | `https://b.171mail.com/api/v1/claude/send` | POST | 入参: `{email}` |
| 轮询收件箱 | `https://b.171mail.com/api/v1/getClaudeMessage?token={email_token}` | GET | 每 2s，超时 90s |
| 验证 magic link | `https://b.171mail.com/api/v1/claude/verify` | POST | 入参: `{link, info: {deviceId, clientSha, email}}`，返回 cookie + sessionKey |
| 身份验证 | `https://claude.ai/api/account` | GET | 用 171mail 返回的 cookie，确认 email 匹配 |
| OAuth 授权 | `https://claude.com/cai/oauth/authorize?...` | GET (浏览器) | CLI stdout 输出此 URL |
| Token 交换 | `https://console.anthropic.com/v1/oauth/token` | POST | CLI 内部调用，mitmproxy 修正 redirect_uri |
| Token 续期 | `https://console.anthropic.com/v1/oauth/token` | POST | `{grant_type: refresh_token, refresh_token, client_id}` |
| 额度查询 | `https://api.anthropic.com/api/oauth/usage` | GET | `Authorization: Bearer {accessToken}`, `anthropic-beta: oauth-2025-04-20` |

#### OAuth Client ID

所有 token 交换和续期请求使用同一个 Client ID：

```
9d1c250a-e61b-44d9-88ed-5944d1962f5e
```

这是 Claude CLI 硬编码的 OAuth Client ID（公开值，非密钥）。

#### .credentials.json 格式

```json
{
  "claudeAiOauth": {
    "accessToken": "sk-ant-oat01-...",
    "refreshToken": "sk-ant-ort01-...",
    "expiresAt": 1716129296000
  }
}
```

- `expiresAt`: Unix 毫秒时间戳
- Token 续期时机: `now_ms >= expiresAt - 5 * 60 * 1000`（过期前 5 分钟）
- 续期后 `expiresAt = (now + expires_in) * 1000`
- 续期请求可能返回新的 `refresh_token`（token rotation），也可能返回原值
- Content-Type: `application/x-www-form-urlencoded`（不是 JSON）

#### Token 过期的完整降级链

```
accessToken 即将过期
  │
  ▼
尝试 refresh（用 refreshToken 换新 accessToken）
  ├── 成功 → 更新 token → 继续正常运行（用户无感知）
  │
  └── 失败（refreshToken 也失效）
        │
        ▼
      降级尝试: 用过期的 accessToken 继续
        ├── 仍然可用（Anthropic 有短暂宽限期）→ 继续运行 + 标记 stale
        │
        └── 不可用（API 返回 auth error）
              │
              ▼
            Worker 上报 → Manager 触发重新登录（自动执行 14 步 OAuth）
              ├── 登录成功 → 获得新 token → 写入 .credentials.json → 恢复
              │
              └── 登录也失败 → 标记 login_status="login_failed"
                    → 该账号退出分配池 → 触发轮换到其他账号
                    → CREDENTIAL_EXHAUSTED 事件（如果无可用替代）
```

与 agent-ml-research 的对比:
  agent-ml-research 也有自动重新登录能力（Phase 13）:
    watchdog 标记 refresh_failed → pool_state_watcher 发出 auth_expired 事件
    → Manager Agent（AI）收到通知 → 调用 manager_claude_account_login MCP 工具
    → 在 Worker 上执行 perform_login() 重新登录
  但这个能力默认关闭（claude_account_login capability = False），需要显式启用。
  
  我们的框架: 将自动重新登录作为框架内置行为（非 AI 决策），无需额外启用。
  区别在于: agent-ml-research 由 AI Agent 判断是否重新登录;
           我们的框架由硬编码逻辑自动触发。两种方式都可行。

### 12.3 额度监控技术细节

#### Usage API 响应格式

```json
{
  "five_hour": {
    "utilization": 45,
    "resets_at": "2026-05-19T17:34:56Z"
  },
  "seven_day": {
    "utilization": 62,
    "resets_at": "2026-05-26T00:00:00Z"
  }
}
```

- `utilization`: 0-100 的整数百分比
- `resets_at`: 当前窗口到期时间（ISO 8601）

#### 账号可用性判定

一个账号 `available=true` 当且仅当以下**全部满足**：

| 条件 | 说明 |
|---|---|
| `enabled=true` | 账号未被运维禁用 |
| `login_status=logged_in` | 登录成功，token 有效 |
| `stale=false` | Token 读取/refresh 无错误 |
| `five_hour.utilization < 85%` | 5h 窗口未超阈值 |
| `seven_day` 未超动态阈值 | 7d 窗口预算未耗尽（如果启用 weekly_reserve） |
| `backoff_until=null` 或已过期 | 不在 API 限流退避期 |

任何一个条件不满足 → `available=false`，不会被 CredentialPool 选中分配。

#### 额度 API 限流处理

Usage API 本身有请求频率限制。被限流时：

```
Worker 侧处理:
  1. API 返回 rate_limit_error
  2. 使用本地缓存的上次成功查询结果（如果 < 30min）
  3. 标记 stale=true, error="usage_api_rate_limited"
  4. available 保持 true（额度 API 限流 ≠ Claude 聊天 API 限流）
  5. 指数退避: 下次检查延迟 180s → 360s → ... → 2880s
  6. 退避期间跳过该账号的额度检查
  7. 退避到期后恢复正常检查频率
```

### 12.4 轮换（切号）技术细节

#### 选号算法

轮换时复用 §3.6 的 `allocate(group, target_worker)` 算法（三步亲和查找）。额外排除当前正在被替换的账号：

```python
def select_replacement(pool, current_id, group, target_worker):
    # 先回收旧账号的绑定（但保留 last_worker）
    pool.release(current_id)  # assigned_to = null
    # 用亲和算法分配新账号
    return pool.allocate(group, target_worker, exclude=[current_id])
```

如果 Step 2 命中（新账号上次就在这个 Worker 上），token 可能还在本地，不需要重新登录。

#### 轮换时的凭证传递

新账号可能处于三种登录状态：

| 状态 | 处理 |
|---|---|
| 已登录，token 有效 | SCP `.credentials.json` 到 Worker 的 config_dir |
| 已登录，token 过期 | 先 refresh token → 成功则 SCP → 失败则重新登录 |
| 未登录 | 在 Worker 上执行完整的自动登录流程 |

#### 冷却与恢复

```
账号被标记 unavailable（额度耗尽）后:
  不立即回收 — 它可能还在 Worker 上供正在运行的进程使用
  
  恢复流程:
    Worker 侧额度检查发现 five_hour.utilization 降到阈值以下
    （通常在 resets_at 到期后 60-90s 内检测到）
    → QUOTA_STATUS 上报 {available: true}
    → Manager 更新 pool_status.json
    → 该账号重新进入可分配池
```
