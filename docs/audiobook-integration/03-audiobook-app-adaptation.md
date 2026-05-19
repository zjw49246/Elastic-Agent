# 跑书双引擎选择与 Elastic-Agent 接入方案

> **本文件是 `docs/跑书双引擎选择与Elastic-Agent接入方案.md` 的更新版本，移入 `docs/audiobook-integration/` 系列。**
> 主要更新：Audiobook Agent Service 定位澄清（独立 repo，引用 Elastic-Agent 框架作为库）、stream-token 直连方案、webhook sequence 排序、manifest 统一数组格式、book_slug 生成规则、轮询补偿机制、CANCELLED 状态建议等。

## 1. 背景

当前项目的讲书稿生成链路是：

```text
前端创建 Task
  -> backend /tasks
  -> TaskService.execute_task
  -> PipelineService 编排多个 Agent 步骤
  -> PipelineAgentClient 写入 AIRequestQueue
  -> ai_service /api/agents/{agent}/execute
  -> ai_service 回调 backend /api/ai-request/callback
  -> AgentOutput / Chunk / ModelCall 落库
  -> Task.script_status 推进到 PENDING_REVIEW
```

Elastic-Agent 文档中的 Audiobook 方案是另一种模式：

```text
提交整本书
  -> Audiobook Agent Service 入队
  -> 分配 Worker
  -> Worker 运行 Claude Code /audiobook
  -> 实时输出 chat/log
  -> 工作目录和 session 文件同步到 OSS/S3
  -> 完成后保留 session_id
  -> 后续修改通过 --resume 回到同一个会话
```

因此后续不建议把 Elastic-Agent 硬塞进现有 `AIRequestQueue`。更合适的方式是引入"跑书双引擎"：

| 跑书方式 | 标识 | 适用场景 | 是否保留现有链路 |
| --- | --- | --- | --- |
| 现有 AI Service Pipeline | `legacy_ai_service` | 当前稳定生产、按 Agent 细粒度调参、按步骤重跑 | 保留 |
| Elastic-Agent Audiobook | `elastic_agent` | 整本书长任务、Claude Code 会话、实时文件、后续 chat 修改 | 新增适配层 |

## 2. 目标

1. 人工创建或批量创建跑书任务时，可以选择跑书方式。
2. 默认仍使用现有 `legacy_ai_service`，避免影响线上生产。
3. Elastic-Agent 只负责"讲书稿生成阶段"，生成完成后回灌到现有任务体系。
4. 后续音频生成、BGM、目录、审核、推送等流程继续复用当前系统。
5. 前端在任务详情页根据跑书方式展示不同的生产过程。

## 3. 总体设计

```text
                    用户选择跑书方式
                           |
             +-------------+-------------+
             |                           |
 legacy_ai_service                elastic_agent
             |                           |
 TaskService + PipelineService    ElasticBookProductionService
             |                           |
 AIRequestQueue + ai_service      Audiobook Agent Service
             |                           |
 AgentOutput / Chunk / ModelCall  elastic_book_runs / OSS files / events
             |                           |
             +-------------+-------------+
                           |
                统一回灌最终讲书稿
                           |
        AgentOutput(final_proofreading 或兼容输出)
                           |
           现有审核 / TTS / BGM / 成品流程
```

> **说明**：Audiobook Agent Service 是独立 repo 中的服务，使用 Elastic-Agent 框架作为库来管理 Worker 和任务调度。本项目后端通过 HTTP API 和 webhook 与 Audiobook Agent Service 交互，不直接依赖 Elastic-Agent 框架内部。

关键原则：

| 原则 | 说明 |
| --- | --- |
| 不复用 `mode` 字段 | `Task.mode` 当前表示自动/手动流程，不应混入跑书引擎含义 |
| 不改造 `AIRequestQueue` 承载 Elastic 长会话 | `AIRequestQueue` 继续负责短请求并发与回调 |
| `Task` 仍是业务主任务 | Elastic run 是 Task 的一个外部执行记录 |
| 完成后回灌 `AgentOutput` | 下游 TTS/BGM 不需要理解 Elastic 内部文件结构 |
| Elastic 文件读取走新接口 | chat、workspace、session、manifest 等能力仅 Elastic 任务可用 |

### 3.1 本项目新增目录结构

为了不把 Elastic-Agent 适配逻辑混进现有 `TaskService`、`PipelineService`、`PipelineAgentClient`，建议新增一个独立服务目录。Webhook 的入口也放在本项目后端 API 层里，不单独作为一套系统。

```text
backend/
  app/
    api/
      tasks.py                         # 现有任务接口：增加跑书方式选择、script-production 查询/控制接口
      elastic_agent.py                 # 新增：Elastic-Agent 回调入口
                                        # POST /api/elastic-agent/webhook 放在这里
    services/
      task/
        task_service.py                # 现有 legacy 跑书服务，保持原职责
      pipeline/
        pipeline_agent_client.py       # 现有 AIRequestQueue 调用链，保持原职责
      elastic_agent/
        __init__.py
        schemas.py                     # Elastic 相关 Pydantic schema
        client.py                      # 调用 Audiobook Agent Service 的 HTTP client
        book_production_service.py     # 创建 Elastic 跑书任务、组装 Task + Book 入参
        webhook_service.py             # 处理 webhook：验签、幂等、状态更新、触发回灌
        oss_file_service.py            # 读取 OSS manifest、最终稿、导出包、预签名 URL
    models/
      models.py                        # 增加 Task.script_generation_backend、ElasticBookRun 等模型
    schemas/
      schemas.py                       # TaskCreate / BatchTaskCreate / TaskResponse 增加跑书方式字段
    config.py                          # 增加 ELASTIC_AGENT_* 配置
  alembic/
    versions/
      xxxx_add_elastic_book_runs.py    # 新增迁移

frontend/
  src/
    services/
      api.ts                           # tasksAPI 增加 script-production 相关方法
    pages/
      TaskDetail.tsx                   # 根据 backend 展示 legacy 或 Elastic 状态/文件/chat
      BookCollection.tsx               # 创建任务时增加跑书方式选择
```

目录里 webhook 的位置：

```text
接口入口：backend/app/api/elastic_agent.py
业务处理：backend/app/services/elastic_agent/webhook_service.py
对外路径：POST /api/elastic-agent/webhook
```

这个 webhook 只接收 Audiobook Agent Service 主动推送的轻量事件，例如状态、phase、session_id、OSS key；最终稿正文仍由 `oss_file_service.py` 从 OSS 读取。

新增文件职责：

| 文件 | 职责 |
| --- | --- |
| `backend/app/api/elastic_agent.py` | 定义 `POST /api/elastic-agent/webhook`，接收 Audiobook Agent Service 回调，并调用 `webhook_service.py` |
| `backend/app/services/elastic_agent/schemas.py` | 定义本项目内部使用的 schema，例如 `ElasticAgentOptions`、`ElasticProduceBookRequest`、`ElasticAgentWebhookEvent` |
| `backend/app/services/elastic_agent/client.py` | 封装本项目调用 Audiobook Agent Service 的 HTTP 客户端，例如 `produce_book()`、`cancel_task()`、`retry_task()`、`send_chat()` |
| `backend/app/services/elastic_agent/book_production_service.py` | Elastic 跑书主业务服务。负责从 `Task + Book` 组装入参、提交 Audiobook Agent Service、创建/更新 `elastic_book_runs` |
| `backend/app/services/elastic_agent/webhook_service.py` | 处理 Audiobook Agent Service 回调。负责验签、幂等、保存事件、更新状态、完成后触发 OSS 读取和 AgentOutput 回灌 |
| `backend/app/services/elastic_agent/oss_file_service.py` | 封装 Elastic 产物读取。负责读取 `_sync_manifest.json`、读取最终稿、生成预签名 URL、校验 path 是否在 manifest 中 |
| `backend/alembic/versions/xxxx_add_elastic_book_runs.py` | 数据库迁移。增加 `Task.script_generation_backend`、`elastic_book_runs`、可选的 `elastic_book_run_events` |

现有文件的改动点：

| 文件 | 改动 |
| --- | --- |
| `backend/app/models/models.py` | 增加 `ScriptGenerationBackend`、`Task.script_generation_backend`、`ElasticBookRun`、可选 `ElasticBookRunEvent` |
| `backend/app/schemas/schemas.py` | `TaskCreate`、`BatchTaskCreate`、`TaskResponse` 增加 `script_generation_backend` 和 `elastic_agent_options` |
| `backend/app/api/tasks.py` | 创建任务后按 `script_generation_backend` 分叉：legacy 走 `TaskService`，Elastic 走 `ElasticBookProductionService`；新增 `/script-production` 查询和控制接口 |
| `backend/main.py` | include 新增的 `elastic_agent` router |
| `backend/app/config.py` | 增加 `ELASTIC_AGENT_MANAGER_URL`、`ELASTIC_AGENT_API_KEY`、`ELASTIC_AGENT_WEBHOOK_SECRET`、`ELASTIC_AGENT_OSS_BUCKET` 等配置 |
| `backend/app/services/task/task_service.py` | 保持 legacy 职责，只增加防御，避免 Elastic 任务误入现有 Pipeline |
| `frontend/src/services/api.ts` | `tasksAPI` 增加 `getScriptProduction`、`getScriptProductionFiles`、`sendScriptProductionChat` 等方法 |
| `frontend/src/pages/TaskDetail.tsx` | 根据 `task.script_generation_backend` 展示 legacy Agent 输出或 Elastic 状态/文件/chat |

职责边界：

```text
TaskService / PipelineService / PipelineAgentClient
  只负责 legacy_ai_service

ElasticBookProductionService / ElasticAgentClient / ElasticAgentOssFileService
  只负责 elastic_agent

AgentOutput(final_proofreading)
  是两条跑书链路共同汇合点
```

## 4. 数据模型改造

### 4.1 Task 增加跑书方式字段

在 `backend/app/models/models.py` 的 `Task` 表增加：

```python
class ScriptGenerationBackend(str, enum.Enum):
    LEGACY_AI_SERVICE = "legacy_ai_service"
    ELASTIC_AGENT = "elastic_agent"

class Task(Base):
    ...
    script_generation_backend = Column(
        Enum(ScriptGenerationBackend),
        nullable=False,
        default=ScriptGenerationBackend.LEGACY_AI_SERVICE,
        server_default=ScriptGenerationBackend.LEGACY_AI_SERVICE.value,
        index=True,
        comment="讲书稿生成后端：legacy_ai_service / elastic_agent",
    )
```

如果希望减少 enum 迁移风险，也可以先用 `String(50)`，应用层校验枚举值。

### 4.2 新增 Elastic 跑书记录表

新增表 `elastic_book_runs`：

```text
id
task_id                         关联 tasks.id，唯一或多次重跑时非唯一
external_task_id                Audiobook Agent Service 侧任务 ID，推荐直接使用 task_id 字符串
status                          queued / dispatching / running / completed / failed / cancelled
phase                           Elastic 当前 Phase，例如 phase_03_outline
progress_pct                    0-100
worker_id                       当前绑定 Worker
session_id                      Claude Code session_id，用于 --resume
book_slug                       插件内部 slug
persona                         例如 nonfiction_default
target_pct                      压缩比例，例如 12
source_oss_uri                  原文备份地址
manifest_oss_uri                _sync_manifest.json 地址
manuscript_oss_uri              最终讲稿地址
export_oss_uri                  delivery zip 地址
last_event_at
error_message
raw_status_payload              Elastic 状态原始 JSON
created_at
updated_at
```

推荐索引：

```text
idx_elastic_book_runs_task_id
idx_elastic_book_runs_status
idx_elastic_book_runs_external_task_id
```

### 4.3 新增 Elastic 事件表

新增表 `elastic_book_run_events`：

```text
id
run_id
task_id
event_type                      task.production.started / task.phase.changed / task.file.synced ...
payload                         原始事件 JSON
created_at
```

用途：

1. 支持任务详情页展示 Elastic 事件时间线。
2. 支持 webhook 幂等处理和排障。
3. Audiobook Agent Service 短暂不可用时可以按事件重放修复状态。

### 4.4 AgentOutput 兼容策略

Elastic 完成后需要把最终讲书稿写回当前系统。建议同时写两类输出：

| 输出 | 用途 |
| --- | --- |
| `agent_name = "elastic_audiobook"` | 保留 Elastic 原始结果、manifest、文件路径、session_id |
| `agent_name = "final_proofreading"` | 兼容现有 TTS/BGM/审核读取最终讲稿的逻辑 |

兼容输出示例：

```python
AgentOutput(
    task_id=task_id,
    agent_name="final_proofreading",
    step_index=FINAL_PROOFREADING,
    output_data={
        "content": final_manuscript,
        "source": "elastic_agent",
        "elastic_run_id": run.id,
    },
)
```

`ModelCall` 可写一条占位记录，标记 `model_name="elastic_agent"`，token 暂为空，后续如果 Elastic 能回传 token usage 再补充。

## 5. 后端接口改造

### 5.1 跑书方式配置接口

新增：

```text
GET /api/tasks/script-generation-backends
```

返回示例：

```json
{
  "default_backend": "legacy_ai_service",
  "items": [
    {
      "backend": "legacy_ai_service",
      "name": "现有 AI Service Pipeline",
      "enabled": true,
      "description": "使用当前多 Agent 流水线，支持模型配置、Prompt 版本和按步骤重跑"
    },
    {
      "backend": "elastic_agent",
      "name": "Elastic-Agent Audiobook",
      "enabled": true,
      "description": "使用 Elastic-Agent Worker 跑整本书，支持实时 chat、文件同步和后续修改",
      "disabled_reason": null
    }
  ]
}
```

前端用这个接口决定是否展示 Elastic 选项。后端根据 `ELASTIC_AGENT_ENABLED`、`ELASTIC_AGENT_MANAGER_URL`、`ELASTIC_AGENT_API_KEY` 判断可用性。

### 5.2 创建任务接口增加字段

修改：

```text
POST /api/tasks/
POST /api/tasks/batch
POST /api/tasks/auto-script-generation
```

`TaskCreate` 增加：

```python
script_generation_backend: Literal["legacy_ai_service", "elastic_agent"] = "legacy_ai_service"
elastic_agent_options: Optional[ElasticAgentOptions] = None
```

`ElasticAgentOptions`：

```python
class ElasticAgentOptions(BaseModel):
    persona: Optional[str] = "nonfiction_default"
    target_pct: Optional[int] = 12
    priority: Optional[int] = 0
    force_recreate_workspace: Optional[bool] = False
```

请求示例：

```json
{
  "book_id": 123,
  "mode": "AUTO",
  "speaker_style_id": 1,
  "script_generation_backend": "elastic_agent",
  "elastic_agent_options": {
    "persona": "nonfiction_default",
    "target_pct": 12,
    "priority": 0
  }
}
```

兼容要求：

1. 不传 `script_generation_backend` 时按 `legacy_ai_service` 处理。
2. `legacy_ai_service` 继续使用 `agent_config`、`prompt_version_config`、`enable_sensitive_filter`。
3. `elastic_agent` 可以保存 `agent_config` 但不使用，前端应隐藏或弱化这些配置项。
4. 批量创建时所有任务使用同一个 backend；如需混合，后续再加高级批量配置。

### 5.3 统一讲书稿生产状态接口

新增：

```text
GET /api/tasks/{task_id}/script-production
```

返回统一状态，前端不需要自己判断大量字段：

```json
{
  "task_id": 123,
  "backend": "elastic_agent",
  "status": "running",
  "script_status": "GENERATING",
  "current_step": "elastic_phase_03_outline",
  "phase": "phase_03_outline",
  "progress_pct": 35,
  "queue_position": null,
  "worker_id": "worker-2",
  "session_id": "claude-session-id",
  "can_cancel": true,
  "can_continue": false,
  "can_retry": false,
  "can_chat": false,
  "manuscript_ready": false,
  "files_ready": true,
  "updated_at": "2026-05-18T10:20:00Z"
}
```

legacy 返回示例：

```json
{
  "task_id": 123,
  "backend": "legacy_ai_service",
  "status": "running",
  "script_status": "GENERATING",
  "current_step": "text_condensation",
  "phase": null,
  "progress_pct": null,
  "queue_position": null,
  "worker_id": null,
  "session_id": null,
  "can_cancel": true,
  "can_continue": true,
  "can_retry": true,
  "can_chat": false,
  "manuscript_ready": false,
  "files_ready": false,
  "updated_at": "2026-05-18T10:20:00Z"
}
```

### 5.4 统一生产控制接口

新增一组"按 backend 分发"的接口：

```text
POST /api/tasks/{task_id}/script-production/cancel
POST /api/tasks/{task_id}/script-production/continue
POST /api/tasks/{task_id}/script-production/retry
```

`retry` 请求：

```json
{
  "from_phase": 3,
  "reason": "重新从大纲阶段生成"
}
```

分发规则：

| backend | cancel | continue | retry |
| --- | --- | --- | --- |
| `legacy_ai_service` | 映射到现有 abort/cancel 逻辑 | 映射到现有 `/tasks/{id}/continue` | 映射到现有 retry/create-from-step/re-execute 逻辑 |
| `elastic_agent` | 调 Audiobook Agent Service cancel | 调 Audiobook Agent Service continue | 调 Audiobook Agent Service retry |

现有接口可以保留，新增统一接口主要给前端新 UI 使用。

### 5.5 Elastic chat 接口

仅 Elastic 任务支持：

```text
POST /api/tasks/{task_id}/script-production/chat
GET  /api/tasks/{task_id}/script-production/chat/history
WS   /api/tasks/{task_id}/script-production/chat/stream
```

发送修改请求：

```json
{
  "message": "请把第三章开头改得更适合口播，降低学术感",
  "idempotency_key": "uuid"
}
```

可能返回：

```json
{
  "success": true,
  "message": "修改请求已提交",
  "edit_run_id": "edit_abc123"
}
```

错误规则：

| 场景 | HTTP 状态 | 说明 |
| --- | --- | --- |
| 非 Elastic 任务 | 409 | 当前跑书方式不支持 chat 修改 |
| 任务未完成无 session | 409 | 生产完成后才支持修改 |
| Worker 离线 | 503 | MVP 不做跨 Worker session 迁移 |
| 修改槽位满 | 429 | 返回预计可重试时间 |

#### 5.5.1 Stream-token 直连方案（方案 B）

为避免 chat stream 经过本项目后端做双重 WebSocket 代理（本项目 WS <-> Audiobook Agent Service WS），提供 stream-token 方案让前端直接连接 Audiobook Agent Service 的 WebSocket：

新增接口：

```text
GET /api/tasks/{task_id}/script-production/stream-config
```

返回：

```json
{
  "ws_url": "wss://audiobook-agent.example.com/ws/tasks/123/chat/stream",
  "token": "eyJhbGciOiJIUzI1NiIs...",
  "expires_at": "2026-05-18T10:35:00Z"
}
```

Token 规则：

| 属性 | 说明 |
| --- | --- |
| 类型 | 短期 JWT |
| 有效期 | 5 分钟（前端连接后 WS 保活不受影响） |
| 绑定 | `task_id`、`user_id`，不可跨任务使用 |
| 签名 | 由本项目后端使用 `ELASTIC_AGENT_STREAM_SECRET` 签名 |
| 传递方式 | 前端在 WS 连接时通过 query param `?token=xxx` 或首帧 auth message 传递 |

前端使用流程：

1. 前端调用 `GET /api/tasks/{task_id}/script-production/stream-config` 获取 `ws_url` + `token`。
2. 前端直接建立 WebSocket 连接到 Audiobook Agent Service：`new WebSocket(ws_url + '?token=' + token)`。
3. Token 过期前需要重新获取（前端在 `expires_at` 前提前刷新）。
4. 本项目后端不参与 WS 数据中转，避免双 WS 代理的延迟和复杂度。

> **注意**：此方案要求前端能直接访问 Audiobook Agent Service 的 WS 端口。如果网络拓扑不允许（例如 Audiobook Agent Service 在内网），则回退到 5.5 中的 `WS /api/tasks/{task_id}/script-production/chat/stream` 由本项目后端代理。

### 5.6 Elastic 文件接口

仅 Elastic 任务支持：

```text
GET /api/tasks/{task_id}/script-production/files
GET /api/tasks/{task_id}/script-production/files/{path:path}
GET /api/tasks/{task_id}/script-production/files/{path:path}/url
GET /api/tasks/{task_id}/script-production/manuscript
GET /api/tasks/{task_id}/script-production/export
```

文件列表从 OSS `_sync_manifest.json` 读取，不直接读 Worker。

文件内容响应示例：

```json
{
  "path": "workspace/manuscript_final.md",
  "content": "...",
  "content_type": "text/markdown",
  "synced_at": "2026-05-18T10:20:00Z",
  "is_latest": true,
  "source": "oss"
}
```

支持参数：

```text
force_sync=true
```

当 `force_sync=true` 且 Worker 在线时，后端通知 Audiobook Agent Service 立即同步目标文件，再从 OSS 读取。

### 5.7 Elastic webhook 接口

新增：

```text
POST /api/elastic-agent/webhook
```

请求头：

```text
X-Elastic-Agent-Signature: hmac-sha256
X-Elastic-Agent-Event-Id: uuid
```

事件体：

```json
{
  "event_id": "evt_001",
  "event_type": "task.phase.changed",
  "task_id": "123",
  "external_task_id": "123",
  "sequence": 7,
  "status": "running",
  "phase": "phase_03_outline",
  "progress_pct": 35,
  "worker_id": "worker-2",
  "session_id": null,
  "payload": {}
}
```

> **新增 `sequence` 字段**：单调递增整数，用于事件排序和丢失检测。每个 `task_id` 的 sequence 独立计数，从 1 开始。

sequence 处理规则：

| 规则 | 说明 |
| --- | --- |
| 乱序忽略 | `new_sequence <= last_processed_sequence` 时直接返回 200（不处理），避免旧事件覆盖新状态 |
| 间隙检测 | `new_sequence > last_processed_sequence + 1` 时记录 gap 告警，触发一次 status 轮询补偿 |
| 持久化 | `elastic_book_runs.last_event_sequence` 持久化当前已处理的最大 sequence |

需要处理的事件：

| 事件 | 后端处理 |
| --- | --- |
| `task.production.queued` | `Task.script_status=GENERATING`，创建/更新 `elastic_book_runs` |
| `task.production.started` | 记录 worker，`current_step=elastic_started` |
| `task.phase.changed` | 更新 `phase/progress_pct/current_step` |
| `task.file.synced` | 更新 manifest/manuscript/export 地址 |
| `task.session.updated` | 更新 `session_id` |
| `task.production.completed` | 拉取最终稿，写入 `AgentOutput`，`Task.status=REVIEWING`，`script_status=PENDING_REVIEW` |
| `task.production.failed` | `Task.status=FAILED`，`script_status=GENERATION_FAILED` |
| `task.production.cancelled` | `Task.status=FAILED`，`script_status=GENERATION_FAILED`，错误信息标记为用户取消 |
| `worker.unhealthy` | 记录事件和告警，不自动迁移 session |

幂等要求：

1. `event_id` 已处理则直接返回成功。
2. `sequence` 乱序则直接返回成功（不处理）。
3. 完成事件重复到达时，不重复创建 `AgentOutput`，改为 update。
4. webhook 不应长时间阻塞，下载大文件可放后台任务。

### 5.8 Audiobook Agent Service 客户端封装

新增目录：

```text
backend/app/services/elastic_agent/
  __init__.py
  client.py
  book_production_service.py
  webhook_service.py
  schemas.py
```

`ElasticAgentClient` 封装后端到 Audiobook Agent Service 的调用：

```python
class ElasticAgentClient:
    async def produce_book(...)
    async def get_task_status(...)
    async def cancel_task(...)
    async def continue_task(...)
    async def retry_task(...)
    async def send_chat(...)
    async def list_files(...)
    async def read_file(...)
    async def get_manuscript(...)
    async def get_workers(...)
    async def get_stream_config(...)
```

`ElasticBookProductionService` 负责：

1. 从 `Book.original_content` 或 `original_content_path` 获取原文。
2. 生成 `book_slug`。
3. 调用 Audiobook Agent Service `/api/tasks/produce`。
4. 创建 `elastic_book_runs`。
5. webhook 完成时回灌 `AgentOutput`。

### 5.9 我们与 Audiobook Agent Service 的接口契约

这部分是后端和 Audiobook Agent Service 的核心边界。整体链路是：

```text
我们的前端
  -> 我们的 backend 创建 Task
  -> 我们的 backend 调 Audiobook Agent Service 提交做书
  -> Audiobook Agent Service 分发 Worker 执行
  -> Worker 把 workspace / final manuscript / session 同步到 OSS
  -> Audiobook Agent Service webhook 通知我们的 backend
  -> 我们的 backend 从 OSS 读取最终稿和 manifest
  -> 回灌 AgentOutput，进入现有审核、TTS、BGM 流程
```

#### 5.9.1 调用认证

我们调用 Audiobook Agent Service 时使用 Bearer Token：

```text
Authorization: Bearer ${ELASTIC_AGENT_API_KEY}
Content-Type: application/json
```

Audiobook Agent Service 回调我们时使用 HMAC 验签：

```text
X-Elastic-Agent-Event-Id: evt_xxx
X-Elastic-Agent-Signature: sha256=...
```

#### 5.9.2 提交做书任务

我们需要调用 Audiobook Agent Service：

```text
POST {ELASTIC_AGENT_MANAGER_URL}/api/tasks/produce
```

我们传给 Audiobook Agent Service 的信息：

```json
{
  "task_id": "123",
  "book_id": 456,
  "book_slug": "thinking-fast-and-slow-123",
  "book_name": "思考，快与慢",
  "author": "丹尼尔·卡尼曼",
  "isbn": "9787508648336",
  "genre": "nonfiction",
  "language": "zh-CN",
  "persona": "nonfiction_default",
  "target_pct": 12,
  "priority": 0,
  "raw_text": "整本书原文文本。如果太大，可以改传 raw_text_oss_uri。",
  "raw_text_oss_uri": null,
  "metadata": {
    "subtitle": null,
    "publisher": "中信出版社",
    "publish_year": "2012",
    "summary": "书籍简介",
    "catalog": "原书目录",
    "source_system": "audio_book_echo_editor"
  },
  "callback": {
    "url": "https://our-backend.example.com/api/elastic-agent/webhook",
    "secret_id": "default"
  },
  "oss": {
    "bucket": "audio-book-echo-editor-sh-oss",
    "prefix": "elastic-agent/tasks/123/"
  },
  "options": {
    "force_recreate_workspace": false,
    "stream_json": true,
    "sync_files": true,
    "keep_session": true
  }
}
```

字段说明：

| 字段 | 必填 | 说明 |
| --- | --- | --- |
| `task_id` | 是 | 我们系统的 `tasks.id`，推荐用字符串传递，Audiobook Agent Service 内部也以它作为主键 |
| `book_id` | 是 | 我们系统的 `books.id` |
| `book_slug` | 是 | Worker 本地目录名，只允许小写英文、数字、短横线 |
| `book_name` | 是 | 书名 |
| `author` | 否 | 作者，多个作者可用中文顿号或 JSON metadata 传 |
| `isbn` | 否 | 便于排障和重复任务定位 |
| `genre` | 是 | `fiction` / `nonfiction` |
| `language` | 否 | 原文语言 |
| `persona` | 是 | Audiobook 插件 persona，例如 `nonfiction_default` |
| `target_pct` | 是 | 压缩比例，默认 12 |
| `raw_text` | 二选一 | 原文内容。小书可以直接传，大书建议传 OSS 地址 |
| `raw_text_oss_uri` | 二选一 | 原文 OSS 地址，Elastic Worker 自行下载 |
| `metadata` | 否 | 书籍补充信息 |
| `callback.url` | 是 | Audiobook Agent Service 事件回调到我们的地址 |
| `oss.bucket` | 是 | Audiobook Agent Service 同步产物的 bucket |
| `oss.prefix` | 是 | 该任务独立目录，必须包含 `task_id` |
| `options.keep_session` | 是 | 是否保留 Claude session，用于后续修改 |

Audiobook Agent Service 返回：

```json
{
  "success": true,
  "external_task_id": "123",
  "status": "queued",
  "queue_position": 4,
  "oss_prefix": "elastic-agent/tasks/123/",
  "message": "Task accepted"
}
```

我们收到返回后：

1. 写入 `elastic_book_runs.external_task_id`。
2. 写入 `elastic_book_runs.status = queued`。
3. 写入 `elastic_book_runs.manifest_oss_uri = oss://bucket/elastic-agent/tasks/123/_sync_manifest.json`。
4. `Task.script_status = GENERATING`。
5. `Task.current_step = elastic_queued`。

#### 5.9.3 查询 Elastic 任务状态

正常状态更新主要靠 webhook。为了兜底和页面刷新，可以保留轮询接口：

```text
GET {ELASTIC_AGENT_MANAGER_URL}/api/tasks/{external_task_id}/status
```

返回：

```json
{
  "external_task_id": "123",
  "task_id": "123",
  "status": "running",
  "phase": "phase_03_outline",
  "progress_pct": 35,
  "worker_id": "worker-2",
  "session_id": null,
  "queue_position": null,
  "oss": {
    "bucket": "audio-book-echo-editor-sh-oss",
    "prefix": "elastic-agent/tasks/123/",
    "manifest_key": "elastic-agent/tasks/123/_sync_manifest.json",
    "manuscript_key": null
  },
  "updated_at": "2026-05-18T10:20:00Z",
  "error_message": null
}
```

我们使用这个接口做两件事：

1. webhook 丢失时补偿状态。
2. 前端打开任务详情页时刷新一次最新状态。

#### 5.9.4 取消、续跑、重试

取消：

```text
POST {ELASTIC_AGENT_MANAGER_URL}/api/tasks/{external_task_id}/cancel
```

请求：

```json
{
  "reason": "user_cancelled",
  "operator": {
    "user_id": 1,
    "username": "admin"
  }
}
```

续跑：

```text
POST {ELASTIC_AGENT_MANAGER_URL}/api/tasks/{external_task_id}/continue
```

请求：

```json
{
  "from_latest_state": true,
  "operator": {
    "user_id": 1,
    "username": "admin"
  }
}
```

从指定 Phase 重试：

```text
POST {ELASTIC_AGENT_MANAGER_URL}/api/tasks/{external_task_id}/retry
```

请求：

```json
{
  "from_phase": 3,
  "reason": "重新生成大纲",
  "operator": {
    "user_id": 1,
    "username": "admin"
  }
}
```

Audiobook Agent Service 返回统一结构：

```json
{
  "success": true,
  "status": "queued",
  "external_task_id": "123",
  "message": "Retry accepted"
}
```

#### 5.9.5 发送修改指令

完成后的书，如果 `session_id` 已存在，我们调用：

```text
POST {ELASTIC_AGENT_MANAGER_URL}/api/tasks/{external_task_id}/chat
```

请求：

```json
{
  "message": "请把第三章开头改得更适合口播，减少学术论文感。",
  "idempotency_key": "uuid",
  "operator": {
    "user_id": 1,
    "username": "admin"
  }
}
```

Audiobook Agent Service 内部会根据 `external_task_id -> session_id -> worker_id` 路由到对应 Worker，执行 `claude --resume`。

返回：

```json
{
  "success": true,
  "edit_run_id": "edit_abc123",
  "status": "running",
  "message": "Edit request accepted"
}
```

修改完成后，Audiobook Agent Service 仍通过 webhook 通知我们，并把新的最终稿同步到同一个 OSS 任务目录。我们需要重新读取 manifest 和最终稿，更新 `AgentOutput(final_proofreading)`。

#### 5.9.6 Audiobook Agent Service 写入 OSS 的目录约定

Audiobook Agent Service 不把大文件通过 webhook 回传，只把文件同步到 OSS。推荐目录：

```text
oss://{bucket}/elastic-agent/tasks/{task_id}/
  _sync_manifest.json
  source/
    raw_text.md
    metadata.json
  workspace/
    state.json
    manuscript_final.md
    manuscript_compliant.md
    outline.md
    sections/
    drafts/
    styled/
    reports/
  delivery/
    audiobook_manuscript.md
    audiobook_delivery.zip
  session/
    session.jsonl
    .claude.json
  logs/
    production.ndjson
    edits/
      edit_abc123.ndjson
```

最重要的文件：

| 文件 | 我们用途 |
| --- | --- |
| `_sync_manifest.json` | 文件索引，判断哪些文件已同步、最新版在哪里 |
| `workspace/state.json` | Elastic/Audiobook 当前 Phase 和状态机 |
| `workspace/manuscript_final.md` | 默认最终稿候选 |
| `workspace/manuscript_compliant.md` | 如果经过合规处理，优先使用 |
| `delivery/audiobook_manuscript.md` | 最终交付稿，优先级最高 |
| `delivery/audiobook_delivery.zip` | 打包下载 |
| `session/session.jsonl` | 后续 chat/history/resume 需要 |
| `logs/production.ndjson` | 排障和前端日志回放 |

最终稿选择优先级：

```text
delivery/audiobook_manuscript.md
  > workspace/manuscript_compliant.md
  > workspace/manuscript_final.md
```

#### 5.9.7 Manifest 格式约定

Audiobook Agent Service 每次同步文件后更新：

```text
oss://{bucket}/elastic-agent/tasks/{task_id}/_sync_manifest.json
```

统一数组格式：

```json
{
  "task_id": "123",
  "worker_id": "aliyun:i-bp1xxx",
  "status": "completed",
  "updated_at": "2026-05-18T10:20:00Z",
  "files": [
    {
      "path": "workspace/manuscript_final.md",
      "oss_key": "elastic-agent/tasks/123/workspace/manuscript_final.md",
      "size": 123456,
      "md5": "d41d8cd98f00b204e9800998ecf8427e",
      "content_type": "text/markdown",
      "role": "manuscript_final",
      "synced_at": "2026-05-18T10:20:00Z"
    },
    {
      "path": "delivery/audiobook_manuscript.md",
      "oss_key": "elastic-agent/tasks/123/delivery/audiobook_manuscript.md",
      "size": 120000,
      "md5": "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6",
      "content_type": "text/markdown",
      "role": "delivery_manuscript",
      "synced_at": "2026-05-18T10:25:00Z"
    }
  ]
}
```

字段说明：

| 字段 | 说明 |
| --- | --- |
| `task_id` | 任务 ID |
| `worker_id` | 当前执行 Worker 的标识，格式 `provider:instance_id` |
| `status` | manifest 对应的任务状态 |
| `updated_at` | manifest 最后更新时间 |
| `files[].path` | 文件在 workspace 中的相对路径 |
| `files[].oss_key` | 文件在 OSS bucket 中的完整 key |
| `files[].size` | 文件大小（字节） |
| `files[].md5` | 文件 MD5 校验值 |
| `files[].content_type` | MIME 类型 |
| `files[].role` | 文件角色标识，如 `manuscript_final`、`delivery_manuscript`、`outline`、`session` 等 |
| `files[].synced_at` | 该文件最后同步到 OSS 的时间 |

我们后端读取 OSS 时只信任 manifest 中的 `oss_key`，不让前端直接传任意 OSS key，避免越权读取。

#### 5.9.8 Webhook 只传状态和 OSS 指针

Audiobook Agent Service webhook 不传大文本，只传状态、路径和元数据：

```json
{
  "event_id": "evt_001",
  "event_type": "task.production.completed",
  "task_id": "123",
  "external_task_id": "123",
  "sequence": 42,
  "status": "completed",
  "phase": "completed",
  "progress_pct": 100,
  "worker_id": "worker-2",
  "session_id": "claude-session-id",
  "oss": {
    "bucket": "audio-book-echo-editor-sh-oss",
    "prefix": "elastic-agent/tasks/123/",
    "manifest_key": "elastic-agent/tasks/123/_sync_manifest.json",
    "manuscript_key": "elastic-agent/tasks/123/delivery/audiobook_manuscript.md",
    "export_key": "elastic-agent/tasks/123/delivery/audiobook_delivery.zip"
  },
  "metrics": {
    "duration_seconds": 5400,
    "input_tokens": null,
    "output_tokens": null
  },
  "occurred_at": "2026-05-18T10:30:00Z"
}
```

我们收到完成事件后的处理：

1. 验签。
2. 按 `event_id` 做幂等。
3. 按 `sequence` 做乱序检测（`sequence <= last_processed_sequence` 则忽略）。
4. 更新 `elastic_book_runs`（含 `last_event_sequence`）。
5. 读取 manifest。
6. 按最终稿优先级从 OSS 下载 markdown。
7. 写入或更新 `AgentOutput(agent_name="elastic_audiobook")`。
8. 写入或更新 `AgentOutput(agent_name="final_proofreading")`。
9. 设置 `Task.status=REVIEWING`、`Task.script_status=PENDING_REVIEW`、`Task.current_step=elastic_completed`。

#### 5.9.9 我们对外提供给前端的接口仍由自己后端代理

前端不要直接访问 Audiobook Agent Service，也不要直接访问 OSS。前端只调用我们的后端：

```text
GET /api/tasks/{task_id}/script-production
GET /api/tasks/{task_id}/script-production/files
GET /api/tasks/{task_id}/script-production/files/{path:path}
GET /api/tasks/{task_id}/script-production/manuscript
POST /api/tasks/{task_id}/script-production/chat
GET /api/tasks/{task_id}/script-production/stream-config
```

> **例外**：`stream-config` 返回的 `ws_url` 允许前端直接连接 Audiobook Agent Service WebSocket（见 5.5.1 和 5.11）。

我们的后端负责：

1. 校验用户是否有权限访问 Task。
2. 根据 `task_id` 找到 `elastic_book_runs`。
3. 从 manifest 解析合法文件。
4. 从 OSS 读取内容或生成短期预签名 URL。
5. 必要时调用 Audiobook Agent Service 做 `force_sync`、chat、retry、cancel。

### 5.10 本项目侧接口与工程改造清单

本方案的主要改造对象仍然是本项目。Audiobook Agent Service 可以理解为一个外部执行服务，本项目新增一层适配，把它接入当前 `tasks`、`AgentOutput`、审核、TTS、BGM 流程。

#### 5.10.1 本项目新增/修改的后端接口

以下接口都挂在当前后端 `settings.API_PREFIX` 下，命名保持现有 `/api/tasks` 体系。

| 接口 | 方法 | 作用 | 主要调用方 |
| --- | --- | --- | --- |
| `/api/tasks/script-generation-backends` | GET | 获取当前用户可选跑书方式 | 前端创建任务弹窗 |
| `/api/tasks/` | POST | 创建单个任务，增加 `script_generation_backend` | 前端 |
| `/api/tasks/batch` | POST | 批量创建任务，增加 `script_generation_backend` | 前端 |
| `/api/tasks/{task_id}/script-production` | GET | 查询讲书稿生产统一状态 | 任务详情页轮询 |
| `/api/tasks/{task_id}/script-production/files` | GET | Elastic 任务文件列表，从 OSS manifest 读取 | 任务详情页 |
| `/api/tasks/{task_id}/script-production/files/{path:path}` | GET | Elastic 任务文件内容，从 OSS 读取 | 任务详情页 |
| `/api/tasks/{task_id}/script-production/files/{path:path}/url` | GET | 生成文件短期预签名 URL | 下载大文件 |
| `/api/tasks/{task_id}/script-production/manuscript` | GET | 读取最终讲书稿 | 任务详情页预览 |
| `/api/tasks/{task_id}/script-production/export` | GET | 下载 Elastic delivery zip | 人工导出 |
| `/api/tasks/{task_id}/script-production/chat` | POST | 对已完成 Elastic 任务发送修改指令 | 任务详情页 |
| `/api/tasks/{task_id}/script-production/chat/history` | GET | 查看修改/生产历史 | 任务详情页 |
| `/api/tasks/{task_id}/script-production/stream-config` | GET | 获取 WS 直连 token（见 5.11） | 前端 chat stream |
| `/api/tasks/{task_id}/script-production/cancel` | POST | 取消当前生产 | 任务详情页 |
| `/api/tasks/{task_id}/script-production/continue` | POST | 续跑失败或中断任务 | 任务详情页 |
| `/api/tasks/{task_id}/script-production/retry` | POST | 从指定 Elastic phase 重试 | 任务详情页 |
| `/api/elastic-agent/webhook` | POST | 接收 Audiobook Agent Service 状态事件 | Audiobook Agent Service |

说明：

1. 前端仍然只调用本项目后端接口。
2. 本项目后端再决定是读本地数据库、调 Audiobook Agent Service，还是读 OSS。
3. 现有 `/api/ai-request/*` 不需要承担 Elastic 任务状态。

#### 5.10.2 创建任务请求体改造

当前 `TaskCreate` 建议增加：

```python
class ElasticAgentOptions(BaseModel):
    persona: Optional[str] = "nonfiction_default"
    target_pct: Optional[int] = 12
    priority: Optional[int] = 0
    force_recreate_workspace: Optional[bool] = False

class TaskCreate(BaseModel):
    book_id: int
    mode: str
    speaker_style_id: Optional[int] = None
    agent_config: Optional[Dict[str, str]] = None
    prompt_version_config: Optional[Dict[str, int]] = None
    enable_sensitive_filter: Optional[bool] = True
    script_generation_backend: Literal["legacy_ai_service", "elastic_agent"] = "legacy_ai_service"
    elastic_agent_options: Optional[ElasticAgentOptions] = None
```

前端创建 Elastic 任务时传：

```json
{
  "book_id": 123,
  "mode": "AUTO",
  "speaker_style_id": 1,
  "script_generation_backend": "elastic_agent",
  "elastic_agent_options": {
    "persona": "nonfiction_default",
    "target_pct": 12,
    "priority": 0,
    "force_recreate_workspace": false
  }
}
```

前端创建现有 Pipeline 任务时仍传：

```json
{
  "book_id": 123,
  "mode": "AUTO",
  "speaker_style_id": 1,
  "script_generation_backend": "legacy_ai_service",
  "agent_config": {
    "text_condensation": "qwen3-max",
    "draft_generation": "qwen3-max"
  },
  "prompt_version_config": {},
  "enable_sensitive_filter": true
}
```

#### 5.10.3 本项目调用 Audiobook Agent Service 的内部入参

本项目不需要把前端请求原样转给 Audiobook Agent Service。应由 `ElasticBookProductionService` 从数据库组装完整入参：

| 来源 | 字段 | 说明 |
| --- | --- | --- |
| `Task.id` | `task_id` | Elastic 外部主键 |
| `Task.book_id` | `book_id` | 关联书籍 |
| `Book.title` | `book_name` | 书名 |
| `Book.author_list_std` / `author_list` | `author` | 作者 |
| `Book.isbn` | `isbn` | ISBN |
| `Book.genre` | `genre` | `fiction` / `nonfiction` |
| `Book.language` | `language` | 语言 |
| `Book.original_content` | `raw_text` | 原文。大文本也可先上传 OSS 再传 `raw_text_oss_uri` |
| `Book.summary` | `metadata.summary` | 简介 |
| `Book.catalog` | `metadata.catalog` | 目录 |
| `elastic_agent_options.persona` | `persona` | 做书 persona |
| `elastic_agent_options.target_pct` | `target_pct` | 压缩比例 |
| 后端配置 | `callback.url` | `/api/elastic-agent/webhook` |
| 后端配置 | `oss.bucket` | OSS bucket |
| 后端生成 | `oss.prefix` | `elastic-agent/tasks/{task_id}/` |

##### book_slug 生成规则

`book_slug` 用于 Worker 本地目录名，必须满足正则 `^[a-z0-9-]{1,60}$`。

生成规则：

| 步骤 | 说明 | 示例 |
| --- | --- | --- |
| 1. 提取标题关键词 | 中文标题取拼音首字母，英文标题取小写单词 | "思考，快与慢" -> `skykm`；"Thinking Fast and Slow" -> `thinking-fast-and-slow` |
| 2. 截断 | 最大 30 字符 | `thinking-fast-and-slow` (22 chars, OK) |
| 3. 拼接 task_id | `{slug}-{task_id}` | `thinking-fast-and-slow-123` |
| 4. 清理非法字符 | 只保留 `[a-z0-9-]`，连续短横线合并 | -- |
| 5. Fallback | 如果标题无法提取有效字符（特殊符号、emoji 等），使用 `book-{task_id}` | `book-123` |

示例：

```python
def build_book_slug(title: str, task_id: int) -> str:
    """
    生成 book_slug，满足 ^[a-z0-9-]{1,60}$
    """
    slug = extract_slug_from_title(title)  # 拼音首字母或英文小写
    slug = slug[:30]                        # 截断
    slug = re.sub(r'[^a-z0-9-]', '', slug)  # 清理非法字符
    slug = re.sub(r'-+', '-', slug).strip('-')  # 合并连续短横线
    if not slug:
        slug = "book"
    return f"{slug}-{task_id}"
```

`ElasticBookProductionService.start_task()` 内部大致流程：

```python
async def start_task(task_id: int, options: ElasticAgentOptions) -> None:
    task = await load_task_with_book(task_id)
    raw_text = await resolve_book_original_text(task.book)
    book_slug = build_book_slug(task.book.title, task.id)

    request = ElasticProduceBookRequest(
        task_id=str(task.id),
        book_id=task.book_id,
        book_slug=book_slug,
        book_name=task.book.title,
        author=resolve_author(task.book),
        isbn=task.book.isbn,
        genre=task.book.genre,
        language=task.book.language,
        persona=options.persona,
        target_pct=options.target_pct,
        raw_text=raw_text,
        metadata=build_book_metadata(task.book),
        callback_url=f"{settings.BACKEND_URL}/api/elastic-agent/webhook",
        oss_bucket=settings.ELASTIC_AGENT_OSS_BUCKET,
        oss_prefix=f"{settings.ELASTIC_AGENT_OSS_PREFIX}tasks/{task.id}/",
    )

    result = await elastic_agent_client.produce_book(request)
    await create_or_update_elastic_book_run(task, request, result)
```

#### 5.10.4 本项目读 OSS 的方式

本项目不依赖 Audiobook Agent Service 返回正文。正文统一从 OSS 读取：

1. `elastic_book_runs.manifest_oss_uri` 找到 `_sync_manifest.json`。
2. 后端读取 manifest。
3. 根据 `role` 或固定优先级找到最终稿。
4. 从 OSS 下载 markdown。
5. 写回 `AgentOutput`。

建议新增内部服务：

```text
backend/app/services/elastic_agent/oss_file_service.py
```

核心方法：

```python
class ElasticAgentOssFileService:
    async def load_manifest(task_id: int) -> dict
    async def list_files(task_id: int) -> list[dict]
    async def read_file(task_id: int, path: str) -> dict
    async def get_presigned_url(task_id: int, path: str) -> str
    async def read_final_manuscript(task_id: int) -> str
```

`read_final_manuscript()` 的选择优先级：

```text
role=delivery_manuscript
  > role=manuscript_compliant
  > role=manuscript_final
  > path=delivery/audiobook_manuscript.md
  > path=workspace/manuscript_compliant.md
  > path=workspace/manuscript_final.md
```

#### 5.10.5 本项目 webhook 处理流程

新增：

```text
backend/app/api/elastic_agent.py
backend/app/services/elastic_agent/webhook_service.py
```

`POST /api/elastic-agent/webhook` 收到事件后：

```python
async def handle_webhook(event: ElasticAgentWebhookEvent) -> None:
    verify_signature(event)
    if await is_event_processed(event.event_id):
        return

    # sequence 乱序检测
    run = await get_elastic_book_run(event.task_id)
    if event.sequence <= run.last_event_sequence:
        return  # 旧事件，忽略

    await save_event(event)
    run = await update_elastic_book_run(event)  # 含 last_event_sequence 更新
    await update_task_status_by_event(event)

    if event.event_type in ("task.production.completed", "task.edit.completed"):
        final_manuscript = await oss_file_service.read_final_manuscript(run.task_id)
        await upsert_elastic_agent_output(run.task_id, event, final_manuscript)
        await upsert_final_proofreading_output(run.task_id, final_manuscript)
        await mark_task_pending_review(run.task_id)
```

##### 轮询补偿机制

为防止 webhook 丢失或延迟，增加轮询补偿：

| 机制 | 说明 |
| --- | --- |
| 定时轮询 | 后端每 5 分钟轮询 Audiobook Agent Service，查询所有 `status=running` 的 `elastic_book_runs` 的最新状态 |
| 页面刷新 | 前端打开任务详情页时，后端触发一次 status 刷新（调用 Audiobook Agent Service `GET /api/tasks/{id}/status`） |
| sequence gap 检测 | webhook 收到的 `sequence` 与 `last_event_sequence` 存在间隙时，主动轮询一次最新状态，补偿漏掉的事件 |

定时轮询实现建议：

```python
async def poll_running_elastic_tasks():
    """每 5 分钟执行一次，查询所有 running 状态的 elastic_book_runs"""
    running_runs = await get_elastic_book_runs_by_status("running")
    for run in running_runs:
        try:
            status = await elastic_agent_client.get_task_status(run.external_task_id)
            await reconcile_status(run, status)
        except Exception as e:
            logger.warning(f"Poll failed for task {run.task_id}: {e}")
```

状态更新规则：

| webhook 事件 | 本项目动作 |
| --- | --- |
| `task.production.queued` | `Task.script_status=GENERATING`，`current_step=elastic_queued` |
| `task.production.started` | 记录 `worker_id`，`current_step=elastic_started` |
| `task.phase.changed` | 更新 `elastic_book_runs.phase/progress_pct`，`Task.current_step=elastic_{phase}` |
| `task.file.synced` | 更新 manifest 和文件同步时间 |
| `task.session.updated` | 更新 `session_id` |
| `task.production.completed` | 从 OSS 读最终稿，回灌 `AgentOutput`，进入待审核 |
| `task.edit.completed` | 从 OSS 读新最终稿，覆盖更新 `AgentOutput` |
| `task.production.failed` | `Task.status=FAILED`，`script_status=GENERATION_FAILED` |
| `task.production.cancelled` | `Task.status=FAILED`，错误信息写"用户取消" |

#### 5.10.6 本项目回灌 AgentOutput 的格式

为了兼容现有任务详情、审核、TTS、BGM，Elastic 完成后必须写 `final_proofreading` 输出。

建议写两条：

```python
elastic_output = AgentOutput(
    task_id=task_id,
    agent_name="elastic_audiobook",
    step_index=FINAL_PROOFREADING,
    output_data={
        "source": "elastic_agent",
        "content": final_manuscript,
        "elastic_run_id": run.id,
        "external_task_id": run.external_task_id,
        "session_id": run.session_id,
        "manifest_oss_uri": run.manifest_oss_uri,
        "manuscript_oss_uri": run.manuscript_oss_uri,
    },
)

compatible_output = AgentOutput(
    task_id=task_id,
    agent_name="final_proofreading",
    step_index=FINAL_PROOFREADING,
    output_data={
        "content": final_manuscript,
        "source": "elastic_agent",
    },
)
```

如果现有 `getScriptContent` 读取的是字符串或其他 key，需要在回灌时按现有解析逻辑适配，原则是：**不要让 TTS/BGM 侧感知 Elastic-Agent 的存在**。

#### 5.10.7 前端 API 命名建议

`frontend/src/services/api.ts` 中保持 `tasksAPI` 聚合：

```typescript
getScriptGenerationBackends: () =>
  api.get('/tasks/script-generation-backends'),

getScriptProduction: (taskId: number) =>
  api.get(`/tasks/${taskId}/script-production`),

getScriptProductionFiles: (taskId: number) =>
  api.get(`/tasks/${taskId}/script-production/files`),

getScriptProductionFile: (taskId: number, path: string) =>
  api.get(`/tasks/${taskId}/script-production/files/${encodeURIComponent(path)}`),

getScriptProductionManuscript: (taskId: number) =>
  api.get(`/tasks/${taskId}/script-production/manuscript`),

sendScriptProductionChat: (taskId: number, message: string) =>
  api.post(`/tasks/${taskId}/script-production/chat`, { message }),

getScriptProductionStreamConfig: (taskId: number) =>
  api.get(`/tasks/${taskId}/script-production/stream-config`),

cancelScriptProduction: (taskId: number) =>
  api.post(`/tasks/${taskId}/script-production/cancel`),

continueScriptProduction: (taskId: number) =>
  api.post(`/tasks/${taskId}/script-production/continue`),

retryScriptProduction: (taskId: number, data: { from_phase?: number; reason?: string }) =>
  api.post(`/tasks/${taskId}/script-production/retry`, data),
```

前端展示判断：

```typescript
if (task.script_generation_backend === 'elastic_agent') {
  // 展示 Elastic phase、文件、chat、session
} else {
  // 展示现有 Agent 输出、模型调用、prompt、步骤重跑
}
```

### 5.11 Stream config 端点：前端直连 WS

新增接口，允许前端直接建立 WebSocket 连接到 Audiobook Agent Service 的 chat stream，避免本项目后端做双重 WS 代理。

```text
GET /api/tasks/{task_id}/script-production/stream-config
```

请求前提：

| 条件 | 说明 |
| --- | --- |
| 任务必须是 `elastic_agent` | 非 Elastic 任务返回 409 |
| 任务必须有 `session_id` | 未完成的任务返回 409 |
| Worker 必须在线 | Worker 离线返回 503 |

返回：

```json
{
  "ws_url": "wss://audiobook-agent.example.com/ws/tasks/123/chat/stream",
  "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "expires_at": "2026-05-18T10:35:00Z"
}
```

Token payload 结构：

```json
{
  "task_id": "123",
  "user_id": 1,
  "session_id": "claude-session-id",
  "iat": 1747568100,
  "exp": 1747568400
}
```

后端实现要点：

```python
async def get_stream_config(task_id: int, current_user: User) -> dict:
    task = await load_task(task_id)
    assert_elastic_task(task)

    run = await get_latest_elastic_book_run(task_id)
    if not run.session_id:
        raise HTTPException(409, "任务尚未生成 session，无法建立 stream")

    # 向 Audiobook Agent Service 请求 stream config
    config = await elastic_agent_client.get_stream_config(
        external_task_id=run.external_task_id,
        user_id=current_user.id,
    )
    return config
```

前端使用：

```typescript
const streamConfig = await tasksAPI.getScriptProductionStreamConfig(taskId);
const ws = new WebSocket(`${streamConfig.ws_url}?token=${streamConfig.token}`);

// token 过期前刷新
const refreshTimer = setTimeout(async () => {
  const newConfig = await tasksAPI.getScriptProductionStreamConfig(taskId);
  // 用新 token 重连或发 auth refresh 帧
}, (new Date(streamConfig.expires_at).getTime() - Date.now()) - 30000);
```

> **Fallback**：如果 Audiobook Agent Service 不支持 stream-config 或网络拓扑不允许前端直连，本项目后端仍提供 `WS /api/tasks/{task_id}/script-production/chat/stream` 作为代理方案。

## 6. 后端执行逻辑改造点

### 6.1 创建任务后的分发

当前 `backend/app/api/tasks.py` 创建任务后直接：

```python
asyncio.create_task(task_service.execute_task(db_task.id))
```

改为：

```python
if db_task.script_generation_backend == ScriptGenerationBackend.LEGACY_AI_SERVICE:
    asyncio.create_task(task_service.execute_task(db_task.id))
else:
    elastic_service = ElasticBookProductionService()
    asyncio.create_task(elastic_service.start_task(db_task.id, task_create.elastic_agent_options))
```

批量创建同理。

### 6.2 TaskService 保持 legacy 职责

`TaskService`、`PipelineService`、`PipelineAgentClient` 不需要因为 Elastic 做大改。它们继续代表 `legacy_ai_service`。

建议只在 `TaskService.execute_task` 开头增加防御：

```python
if task.script_generation_backend != ScriptGenerationBackend.LEGACY_AI_SERVICE:
    raise ValueError("TaskService only supports legacy_ai_service tasks")
```

避免 Elastic 任务误入现有 Pipeline。

### 6.3 状态映射

| Elastic status | Task.status | Task.script_status | Task.current_step |
| --- | --- | --- | --- |
| `queued` | `RUNNING` | `GENERATING` | `elastic_queued` |
| `dispatching` | `RUNNING` | `GENERATING` | `elastic_dispatching` |
| `running` | `RUNNING` | `GENERATING` | `elastic_{phase}` |
| `completed` | `REVIEWING` | `PENDING_REVIEW` | `elastic_completed` |
| `failed` | `FAILED` | `GENERATION_FAILED` | `elastic_failed` |
| `cancelled` | `FAILED` | `GENERATION_FAILED` | `elastic_cancelled` |

`TaskStatus` 当前没有 `CANCELLED`，所以 MVP 取消先映射为 `FAILED`，并在 `script_error_message` 写明"用户取消"。

#### 6.3.1 CANCELLED 状态建议

建议后续考虑在 `TaskStatus` 枚举中新增 `CANCELLED`：

| 方案 | 说明 |
| --- | --- |
| **方案 A：新增 `CANCELLED`** | 在 `TaskStatus` 枚举中增加 `CANCELLED`，`cancelled` 直接映射到 `Task.status=CANCELLED`。好处是语义清晰，前端可以区分展示"失败"和"取消"。代价是需要迁移和下游（列表、统计、审核）适配。 |
| **方案 B：继续用 `FAILED` + 前缀（MVP）** | `cancelled` 映射为 `Task.status=FAILED`，`script_error_message` 以 `[用户取消]` 为前缀。前端通过检测前缀区分展示。好处是零迁移。代价是语义不精确，统计时需要额外过滤。 |

MVP 推荐方案 B。前端检测逻辑：

```typescript
const isCancelled = task.status === 'FAILED' 
  && task.script_error_message?.startsWith('[用户取消]');

if (isCancelled) {
  // 展示"已取消"样式，而非"失败"样式
} else if (task.status === 'FAILED') {
  // 展示"失败"样式
}
```

后续如果取消场景变多（例如超时自动取消、管理员批量取消），建议正式新增 `CANCELLED` 状态。

## 7. 前端改造

### 7.1 创建任务弹窗增加跑书方式选择

位置：

```text
frontend/src/pages/BookCollection.tsx
frontend/src/pages/TaskDetail.tsx
frontend/src/services/api.ts
```

新增控件：

```text
跑书方式
[现有 AI Service Pipeline] [Elastic-Agent Audiobook]
```

选择 `legacy_ai_service` 时展示：

1. Agent 模型配置。
2. Prompt 版本配置。
3. 敏感词过滤开关。

选择 `elastic_agent` 时展示：

1. Persona。
2. Target pct。
3. Elastic-Agent 可用状态。
4. 提示"完成后仍进入当前审核/TTS/BGM流程"。

不建议复用 `mode` 控件，因为 `mode=AUTO/MANUAL` 和跑书引擎是两个维度。

### 7.2 API SDK 增加方法

`frontend/src/services/api.ts` 增加：

```typescript
getScriptGenerationBackends: () =>
  api.get('/tasks/script-generation-backends'),

getScriptProduction: (taskId: number) =>
  api.get(`/tasks/${taskId}/script-production`),

cancelScriptProduction: (taskId: number) =>
  api.post(`/tasks/${taskId}/script-production/cancel`),

continueScriptProduction: (taskId: number) =>
  api.post(`/tasks/${taskId}/script-production/continue`),

retryScriptProduction: (taskId: number, data: { from_phase?: number; reason?: string }) =>
  api.post(`/tasks/${taskId}/script-production/retry`, data),

sendScriptProductionChat: (taskId: number, message: string) =>
  api.post(`/tasks/${taskId}/script-production/chat`, { message }),

getScriptProductionStreamConfig: (taskId: number) =>
  api.get(`/tasks/${taskId}/script-production/stream-config`),

getScriptProductionFiles: (taskId: number) =>
  api.get(`/tasks/${taskId}/script-production/files`),

getScriptProductionFile: (taskId: number, path: string) =>
  api.get(`/tasks/${taskId}/script-production/files/${encodeURIComponent(path)}`),

getScriptProductionManuscript: (taskId: number) =>
  api.get(`/tasks/${taskId}/script-production/manuscript`),
```

### 7.3 TaskDetail 按 backend 展示

`TaskDetail` 读取 `task.script_generation_backend`：

| backend | 展示 |
| --- | --- |
| `legacy_ai_service` | 当前 Agent 输出、步骤重跑、模型调用、Prompt 查看 |
| `elastic_agent` | Elastic phase 时间线、实时 chat、文件列表、最终稿预览、session 信息 |

`BookProductionProgress` 需要识别 `elastic_*` 的 `current_step`，至少展示：

```text
排队中 -> Worker 已分配 -> 生产中 -> 文件同步中 -> 待审核
```

后续可以细化为 Elastic 的 10 Phase。

### 7.4 列表筛选

任务列表增加筛选：

```text
script_generation_backend=legacy_ai_service | elastic_agent
```

后端 `GET /tasks`、`GET /tasks/with-colors` 增加 query 参数：

```text
script_generation_backend?: string
```

## 8. 配置项

后端新增环境变量：

```text
# 功能开关
ELASTIC_AGENT_ENABLED=false

# Audiobook Agent Service 连接
ELASTIC_AGENT_MANAGER_URL=                     # 如 http://10.0.1.100:8000
ELASTIC_AGENT_API_KEY=                         # Bearer Token
ELASTIC_AGENT_WEBHOOK_SECRET=                  # Webhook HMAC 验签密钥
ELASTIC_AGENT_STREAM_SECRET=                   # 前端 WS 直连 JWT 密钥（与 ABS 共享）

# 默认做书参数
ELASTIC_AGENT_DEFAULT_PERSONA=nonfiction_default
ELASTIC_AGENT_DEFAULT_TARGET_PCT=12

# 超时与轮询
ELASTIC_AGENT_REQUEST_TIMEOUT_SECONDS=30       # 调用 Agent Service 超时
ELASTIC_AGENT_POLL_INTERVAL_SECONDS=300        # Webhook 补偿轮询间隔

# OSS 配置（读取 Elastic 产物）
ELASTIC_AGENT_OSS_BUCKET=                      # 如 audio-book-echo-editor-sh-oss
ELASTIC_AGENT_OSS_PREFIX=elastic-agent/        # 不含 tasks/（produce 请求中拼接）
ELASTIC_AGENT_OSS_ENDPOINT=                    # 如 oss-cn-shanghai.aliyuncs.com
# OSS 读取凭证：复用项目已有的阿里云 OSS 配置；如果 bucket 不同需额外配置：
# ELASTIC_AGENT_OSS_ACCESS_KEY_ID=
# ELASTIC_AGENT_OSS_ACCESS_KEY_SECRET=
```

前端不直接配置 Audiobook Agent Service URL，统一走后端代理（WS stream-config 例外）。

## 9. 权限与安全

1. 普通用户能否使用 `elastic_agent` 应由后端控制，可复用用户角色或新增用户能力字段。
2. `GET /tasks/script-generation-backends` 应根据当前用户返回可用选项。
3. Elastic webhook 必须验签，避免外部伪造任务完成事件。
4. 文件接口必须校验当前用户是否有权限访问该 `task_id`。
5. chat 修改接口必须校验任务归属，并记录操作用户。
6. stream-config 返回的 JWT token 必须绑定 `task_id` 和 `user_id`，Audiobook Agent Service 侧需要验证。

## 10. 接口变化与输入输出总表

本节只描述接口和数据流，不描述分阶段计划。

### 10.1 前端调用本项目后端的接口

前端只调用本项目后端，不直接调用 Audiobook Agent Service，也不直接访问 OSS（WS stream-config 直连例外）。

| 前端动作 | 本项目接口 | 方法 | 入参 | 返回 | 后端数据来源 |
| --- | --- | --- | --- | --- | --- |
| 获取可选跑书方式 | `/api/tasks/script-generation-backends` | GET | 无 | `legacy_ai_service` / `elastic_agent` 可用状态 | 配置 + 用户权限 |
| 创建单个跑书任务 | `/api/tasks/` | POST | `book_id`、`mode`、`script_generation_backend`、`elastic_agent_options` | `TaskCreateResponse` | 写 `tasks`，必要时提交 Elastic |
| 批量创建跑书任务 | `/api/tasks/batch` | POST | `book_ids`、`quantity_per_book`、`script_generation_backend`、`elastic_agent_options` | `BatchTaskCreateResponse` | 写 `tasks`，逐个提交 Elastic |
| 查询讲书稿生产状态 | `/api/tasks/{task_id}/script-production` | GET | `task_id` | backend、status、phase、progress、session、文件状态 | `tasks` + `elastic_book_runs` |
| 取消生产 | `/api/tasks/{task_id}/script-production/cancel` | POST | `reason` 可选 | 操作结果 | legacy 映射现有 abort；Elastic 调 Audiobook Agent Service cancel |
| 续跑生产 | `/api/tasks/{task_id}/script-production/continue` | POST | `from_latest_state` 可选 | 操作结果 | legacy 映射现有 continue；Elastic 调 Audiobook Agent Service continue |
| 重试生产 | `/api/tasks/{task_id}/script-production/retry` | POST | `from_phase`、`reason` | 操作结果 | legacy 映射现有 retry；Elastic 调 Audiobook Agent Service retry |
| 查看 Elastic 文件列表 | `/api/tasks/{task_id}/script-production/files` | GET | `task_id` | 文件列表、大小、同步时间、role | OSS `_sync_manifest.json` |
| 查看 Elastic 单文件内容 | `/api/tasks/{task_id}/script-production/files/{path:path}` | GET | `path`、`force_sync` 可选 | 文件内容、content_type、synced_at | OSS object |
| 获取 Elastic 文件下载地址 | `/api/tasks/{task_id}/script-production/files/{path:path}/url` | GET | `path` | 短期预签名 URL | OSS object |
| 获取最终讲书稿 | `/api/tasks/{task_id}/script-production/manuscript` | GET | `task_id` | markdown 文本和来源文件 | OSS manifest + OSS object |
| 导出交付包 | `/api/tasks/{task_id}/script-production/export` | GET | `task_id` | zip 文件或预签名 URL | OSS `delivery/audiobook_delivery.zip` |
| 发送修改指令 | `/api/tasks/{task_id}/script-production/chat` | POST | `message`、`idempotency_key` 可选 | `edit_run_id`、状态 | Audiobook Agent Service chat |
| 查看聊天历史 | `/api/tasks/{task_id}/script-production/chat/history` | GET | `task_id` | 历史消息 | OSS `logs/*.ndjson` |
| 获取 WS 直连配置 | `/api/tasks/{task_id}/script-production/stream-config` | GET | `task_id` | `ws_url`、`token`、`expires_at` | Audiobook Agent Service stream-config |

### 10.2 创建任务接口的字段变化

`POST /api/tasks/` 和 `POST /api/tasks/batch` 增加以下字段：

```json
{
  "script_generation_backend": "legacy_ai_service",
  "elastic_agent_options": {
    "persona": "nonfiction_default",
    "target_pct": 12,
    "priority": 0,
    "force_recreate_workspace": false
  }
}
```

字段说明：

| 字段 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `script_generation_backend` | string | `legacy_ai_service` | 跑书方式：`legacy_ai_service` 或 `elastic_agent` |
| `elastic_agent_options.persona` | string | `nonfiction_default` | 传给 Audiobook Agent Service 的 Audiobook persona |
| `elastic_agent_options.target_pct` | int | `12` | 目标压缩比例 |
| `elastic_agent_options.priority` | int | `0` | Elastic 队列优先级 |
| `elastic_agent_options.force_recreate_workspace` | bool | `false` | 是否强制重建 workspace |

legacy 任务仍使用：

```json
{
  "agent_config": {},
  "prompt_version_config": {},
  "enable_sensitive_filter": true
}
```

Elastic 任务可以不使用 `agent_config`、`prompt_version_config`，前端应在选择 `elastic_agent` 时隐藏或弱化这些配置。

### 10.3 本项目提交给 Audiobook Agent Service 的输入

本项目后端从 `tasks` 和 `books` 组装请求，然后调用 Audiobook Agent Service：

```text
POST {ELASTIC_AGENT_MANAGER_URL}/api/tasks/produce
```

本项目传给 Audiobook Agent Service 的请求体：

```json
{
  "task_id": "123",
  "book_id": 456,
  "book_slug": "thinking-fast-and-slow-123",
  "book_name": "思考，快与慢",
  "author": "丹尼尔·卡尼曼",
  "isbn": "9787508648336",
  "genre": "nonfiction",
  "language": "zh-CN",
  "persona": "nonfiction_default",
  "target_pct": 12,
  "priority": 0,
  "raw_text": "整本书原文文本",
  "raw_text_oss_uri": null,
  "metadata": {
    "subtitle": null,
    "publisher": "中信出版社",
    "publish_year": "2012",
    "summary": "书籍简介",
    "catalog": "原书目录",
    "source_system": "audio_book_echo_editor"
  },
  "callback": {
    "url": "https://our-backend.example.com/api/elastic-agent/webhook",
    "secret_id": "default"
  },
  "oss": {
    "bucket": "audio-book-echo-editor-sh-oss",
    "prefix": "elastic-agent/tasks/123/"
  },
  "options": {
    "force_recreate_workspace": false,
    "stream_json": true,
    "sync_files": true,
    "keep_session": true
  }
}
```

字段来源：

| Elastic 入参 | 本项目来源 | 说明 |
| --- | --- | --- |
| `task_id` | `Task.id` | 用我们的任务 ID 作为 Elastic 外部主键 |
| `book_id` | `Task.book_id` | 关联书籍 |
| `book_slug` | 后端生成 | 本地目录名，如 `{title_slug}-{task_id}`（见 5.10.3 生成规则） |
| `book_name` | `Book.title` | 书名 |
| `author` | `Book.author_list_std` / `author_list` | 作者 |
| `isbn` | `Book.isbn` | ISBN |
| `genre` | `Book.genre` | `fiction` / `nonfiction` |
| `language` | `Book.language` | 语言 |
| `persona` | `elastic_agent_options.persona` | 做书 persona |
| `target_pct` | `elastic_agent_options.target_pct` | 压缩比例 |
| `raw_text` | `Book.original_content` 或解析文件 | 原文内容 |
| `raw_text_oss_uri` | 本项目上传原文后生成 | 大文本可传 OSS 地址 |
| `metadata.summary` | `Book.summary` | 简介 |
| `metadata.catalog` | `Book.catalog` | 原书目录 |
| `callback.url` | `settings.BACKEND_URL` | 本项目 webhook 地址 |
| `oss.bucket` | `settings.ELASTIC_AGENT_OSS_BUCKET` | Elastic 产物写入的 bucket |
| `oss.prefix` | 后端生成 | `elastic-agent/tasks/{task_id}/` |

Audiobook Agent Service 接收成功后返回：

```json
{
  "success": true,
  "external_task_id": "123",
  "status": "queued",
  "queue_position": 4,
  "oss_prefix": "elastic-agent/tasks/123/",
  "message": "Task accepted"
}
```

本项目收到后写入：

```text
elastic_book_runs.external_task_id
elastic_book_runs.status
elastic_book_runs.queue_position
elastic_book_runs.manifest_oss_uri
Task.script_status = GENERATING
Task.current_step = elastic_queued
```

### 10.4 本项目调用 Audiobook Agent Service 的其他接口

| 本项目动作 | Audiobook Agent Service 接口 | 方法 | 本项目传什么 | Audiobook Agent Service 返回什么 |
| --- | --- | --- | --- | --- |
| 查询状态兜底 | `/api/tasks/{external_task_id}/status` | GET | `external_task_id` | status、phase、progress、worker、session、OSS 指针 |
| 取消生产 | `/api/tasks/{external_task_id}/cancel` | POST | reason、operator | success、status、message |
| 续跑生产 | `/api/tasks/{external_task_id}/continue` | POST | from_latest_state、operator | success、status、message |
| 重试生产 | `/api/tasks/{external_task_id}/retry` | POST | from_phase、reason、operator | success、status、message |
| 发送修改指令 | `/api/tasks/{external_task_id}/chat` | POST | message、idempotency_key、operator | edit_run_id、status |
| 强制同步文件 | `/api/tasks/{external_task_id}/files/sync` | POST | path | synced_at、manifest_key |
| 获取 stream 配置 | `/api/tasks/{external_task_id}/stream-config` | GET | user_id | ws_url、token、expires_at |

说明：

1. 正常状态更新优先靠 webhook。
2. `GET status` 只作为页面刷新和 webhook 丢失时的兜底。
3. `files/sync` 仅在前端传 `force_sync=true` 且 Worker 在线时使用。
4. `stream-config` 用于前端直连 WS，避免双重代理。

### 10.5 Audiobook Agent Service 输出如何获取

Audiobook Agent Service 不通过接口直接返回大文本。输出统一写 OSS，并通过 webhook 或 status 返回 OSS 指针。

OSS 目录约定：

```text
oss://{bucket}/elastic-agent/tasks/{task_id}/
  _sync_manifest.json
  source/
    raw_text.md
    metadata.json
  workspace/
    state.json
    manuscript_final.md
    manuscript_compliant.md
    outline.md
    sections/
    drafts/
    styled/
    reports/
  delivery/
    audiobook_manuscript.md
    audiobook_delivery.zip
  session/
    session.jsonl
    .claude.json
  logs/
    production.ndjson
    edits/
      edit_abc123.ndjson
```

本项目获取输出的规则：

| 输出类型 | 前端调用本项目接口 | 本项目读取来源 | 说明 |
| --- | --- | --- | --- |
| 生产状态 | `/api/tasks/{task_id}/script-production` | `tasks` + `elastic_book_runs` | 不读大文件 |
| 文件列表 | `/api/tasks/{task_id}/script-production/files` | OSS `_sync_manifest.json` | 返回 path、role、size、synced_at |
| 单文件内容 | `/api/tasks/{task_id}/script-production/files/{path}` | manifest 中对应 OSS object | path 必须存在于 manifest |
| 最终讲书稿 | `/api/tasks/{task_id}/script-production/manuscript` | manifest + OSS object | 按优先级选择最终稿 |
| 导出包 | `/api/tasks/{task_id}/script-production/export` | `delivery/audiobook_delivery.zip` | 返回文件或预签名 URL |
| 聊天历史 | `/api/tasks/{task_id}/script-production/chat/history` | `logs/*.ndjson`（Worker Runtime 双写的持久化日志） | 按 parsed.type 过滤 assistant/result 消息后返回 |

最终稿选择优先级：

```text
delivery/audiobook_manuscript.md
  > workspace/manuscript_compliant.md
  > workspace/manuscript_final.md
```

完成 webhook 到达后，本项目还会主动读取最终稿并回灌：

```text
OSS 最终稿
  -> AgentOutput(agent_name="elastic_audiobook")
  -> AgentOutput(agent_name="final_proofreading")
  -> Task.status=REVIEWING
  -> Task.script_status=PENDING_REVIEW
```

### 10.6 Audiobook Agent Service 回调本项目的接口

Audiobook Agent Service 调用本项目：

```text
POST /api/elastic-agent/webhook
```

事件只传状态和 OSS 指针，不传正文：

```json
{
  "event_id": "evt_001",
  "event_type": "task.production.completed",
  "task_id": "123",
  "external_task_id": "123",
  "sequence": 42,
  "status": "completed",
  "phase": "completed",
  "progress_pct": 100,
  "worker_id": "worker-2",
  "session_id": "claude-session-id",
  "oss": {
    "bucket": "audio-book-echo-editor-sh-oss",
    "prefix": "elastic-agent/tasks/123/",
    "manifest_key": "elastic-agent/tasks/123/_sync_manifest.json",
    "manuscript_key": "elastic-agent/tasks/123/delivery/audiobook_manuscript.md",
    "export_key": "elastic-agent/tasks/123/delivery/audiobook_delivery.zip"
  },
  "metrics": {
    "duration_seconds": 5400,
    "input_tokens": null,
    "output_tokens": null
  },
  "occurred_at": "2026-05-18T10:30:00Z"
}
```

本项目处理规则：

| webhook 事件 | 本项目处理 |
| --- | --- |
| `task.production.queued` | 更新 `elastic_book_runs.status=queued`，`Task.current_step=elastic_queued` |
| `task.production.started` | 记录 `worker_id`，`Task.current_step=elastic_started` |
| `task.phase.changed` | 更新 `phase/progress_pct`，`Task.current_step=elastic_{phase}` |
| `task.file.synced` | 更新 manifest / manuscript / export OSS 指针 |
| `task.session.updated` | 更新 `session_id` |
| `task.production.completed` | 从 OSS 读最终稿，回灌 `AgentOutput`，任务进入待审核 |
| `task.edit.completed` | 从 OSS 读新最终稿，覆盖更新 `AgentOutput` |
| `task.production.failed` | 标记任务失败，记录错误 |
| `task.production.cancelled` | 标记任务失败，错误信息写 `[用户取消]` 前缀 |

## 11. 需要改动的主要文件

后端：

```text
backend/app/models/models.py
backend/app/schemas/schemas.py
backend/app/api/tasks.py
backend/main.py
backend/app/config.py
backend/app/services/task/task_service.py
backend/app/services/elastic_agent/client.py
backend/app/services/elastic_agent/book_production_service.py
backend/app/services/elastic_agent/webhook_service.py
backend/app/services/elastic_agent/oss_file_service.py
backend/app/services/elastic_agent/schemas.py
backend/alembic/versions/xxxx_add_script_generation_backend_and_elastic_runs.py
```

新增接口：

```text
GET  /api/tasks/{task_id}/script-production/stream-config    # 前端直连 WS token
POST /api/elastic-agent/webhook                              # Audiobook Agent Service 回调
```

前端：

```text
frontend/src/services/api.ts
frontend/src/pages/BookCollection.tsx
frontend/src/pages/TaskDetail.tsx
frontend/src/pages/TaskDetail/components/BookProductionProgress.tsx
frontend/src/stores/taskDetailStore.ts
frontend/src/stores/taskListStore.ts
```

## 12. 风险点

| 风险 | 应对 |
| --- | --- |
| Elastic 产物格式和现有 `AgentOutput` 不一致 | 完成回灌时做格式适配，保证 `final_proofreading` 输出可被 TTS 读取 |
| `session_id` 丢失导致不能修改 | webhook 和文件同步都保存 session，`elastic_book_runs.session_id` 必填校验 |
| Worker 离线后无法 resume | MVP 返回 503，不做跨 Worker 迁移 |
| Elastic 文件同步延迟 | 文件接口返回 `synced_at`，必要时支持 `force_sync=true` |
| 线上误选未稳定 backend | 默认关闭 Elastic，按用户或管理员权限灰度开放 |
| 取消任务语义不一致 | MVP 映射为 `FAILED + [用户取消]` 前缀，后续可新增 `CANCELLED` 状态 |
| Webhook 丢失导致状态不一致 | 5 分钟定时轮询 + 页面打开时刷新 + sequence gap 检测触发补偿 |
| Stream-token 安全 | JWT 短期有效（5 分钟）、绑定 task_id + user_id、Audiobook Agent Service 侧验证 |
