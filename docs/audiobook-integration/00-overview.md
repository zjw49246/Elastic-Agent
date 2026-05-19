# Audiobook x Elastic-Agent 集成方案总览

> 本文档是 Audiobook 有声书稿生产系统接入 Elastic-Agent 弹性计算框架的**顶层设计总览**。
>
> 三个独立仓库将按本文档描述的架构和接口契约分别开发。

---

## 1. 项目背景

### 1.1 现状

**Audiobook 有声书稿生产系统**（audio_book_echo_editor）是一个企业级讲书系统，当前包含：
- 10 步 Agent Pipeline（文本缩水 → 故事线概览 → 框架 → 分块 → 扰动 → 循环生稿 → 开头结尾 → 问题排查 → 终审 → 评估）
- 30+ 个 AI Agent 协同
- 完整的音频处理流程（TTS + BGM + 混音）
- 审核工作流（初审 + 终审）
- 前端管理界面

**痛点：** 当前 Pipeline 基于 `AIRequestQueue` + `ai_service` 的短请求模式，每个 Agent 是独立 API 调用。无法支持：
- Claude Code 长会话（1-2 小时/本书）
- 会话级上下文保持和后续修改（`--resume`）
- 文件同步和聊天记录持久化
- 多 Worker 并行做书

### 1.2 目标

引入 Elastic-Agent 框架，在不影响现有 Pipeline 稳定运行的前提下，新增"Elastic-Agent 跑书引擎"：
- 整本书交给一个 Claude Code 会话，端到端自动完成
- 聊天记录 + 文件同步到 OSS，前端轮询获取
- 完成后保留会话，支持随时修改
- 多 Worker 并行，队列调度
- 完成后回灌现有任务系统，复用审核/TTS/BGM 流程

---

## 2. 三仓库架构

### 2.1 仓库划分

| 仓库 | 职责 | 技术栈 | 部署形态 |
|------|------|--------|---------|
| **Elastic-Agent** | 通用弹性计算框架：云资源管理、Worker Runtime、通信协议、External API、FileSyncManager | Python 3.11+, FastAPI | GitHub 包（`uv add git+https://github.com/zjw49246/Elastic-Agent.git`） |
| **Audiobook Agent Service** (新建) | 基于 Elastic-Agent 的有声书生产服务：AudiobookHarness、BookQueue、SessionRegistry、SlotScheduler、ChatRelay、Audiobook 专用 API | Python 3.11+, FastAPI | 独立部署的 Manager 服务 |
| **audio_book_echo_editor** | 现有做书前后端：双引擎适配层、Elastic 客户端、Webhook 处理、OSS 文件读取、前端 UI | Python (FastAPI) + React/TS | 现有部署，增量改造 |

### 2.2 依赖关系

```
┌────────────────────────────────┐
│    audio_book_echo_editor      │
│    (现有做书前后端)              │
│                                │
│  调用 Audiobook Agent Service  │
│  接收 Webhook 回调             │
│  从 OSS 读取产物               │
└───────────┬────────────────────┘
            │ HTTPS + Webhook
            ▼
┌────────────────────────────────┐
│   Audiobook Agent Service      │
│   (独立部署的 Manager 服务)     │
│                                │
│  import elastic_agent          │──── uv add git+https://github.com/zjw49246/Elastic-Agent.git
│  实现 AudiobookHarness         │
│  暴露 Audiobook 专用 API       │
│  推送 Webhook 到 audio_book    │
└───────────┬────────────────────┘
            │ WebSocket (Worker 反向连接)
            ▼
┌────────────────────────────────┐
│   Workers (阿里云 ECS / AWS)   │
│                                │
│  Worker Runtime (来自框架)     │
│  Claude Code CLI               │
│  audiobook-nonfiction 插件     │
│  FileSyncManager → OSS/S3      │
└────────────────────────────────┘
```

### 2.3 Elastic-Agent 框架作为 Library

Elastic-Agent 不是一个独立部署的服务，而是一个 **Python 包**（`elastic-agent`），提供：

```python
# Audiobook Agent Service 中的使用方式
from elastic_agent.core.providers import AliyunProvider, AWSProvider
from elastic_agent.core.runtime import WorkerRuntimeServer, WorkerRuntimeClient
from elastic_agent.core.registry import NodeRegistry
from elastic_agent.core.bootstrap import BootstrapPipeline
from elastic_agent.core.monitor import HealthChecker, CloudReconciler, EventBus
from elastic_agent.core.external_api import create_external_api_router
from elastic_agent.core.security import TokenAuthenticator
from elastic_agent.worker import WorkerRuntime, FileSyncManager
from elastic_agent.manager import ElasticAgentManager
from elastic_agent.harness import Harness, HarnessConfig

# Audiobook Agent Service 实现自己的 Harness
class AudiobookHarness(Harness):
    ...

# 组装并启动 Manager
manager = ElasticAgentManager(
    harness=AudiobookHarness(config),
    provider=AliyunProvider(aliyun_config),
    ...
)
app = manager.create_app()  # FastAPI app
# 然后挂载 Audiobook 专用路由
app.include_router(audiobook_api_router)
```

这种设计意味着：
- Elastic-Agent 框架是可复用的（其他 Harness 如 ML Research、CCM 也可以用）
- Audiobook Agent Service 拥有完整的部署和配置控制权
- Audiobook 专用逻辑（BookQueue、SessionRegistry 等）不污染框架

---

## 3. 各仓库职责边界

### 3.1 Elastic-Agent 框架 (Library)

| 模块 | 职责 | 不做什么 |
|------|------|---------|
| CloudProvider | 抹平阿里云/AWS 差异，实例 CRUD | 不决定何时创建/销毁 |
| Worker Runtime | Manager↔Worker 双向通信，进程管理，日志双写（LOG 事件+本地 NDJSON 落盘），文件操作 | 不理解任务业务语义 |
| NodeRegistry | 节点状态持久化 | 不知道"槽位"概念 |
| Bootstrap Pipeline | 可插拔初始化步骤 | 不内置 audiobook 插件安装 |
| HealthChecker | L1/L2/L3 健康检查 | 不定义"卡住"的业务含义 |
| CloudReconciler | 标签对账，孤儿清理 | — |
| EventBus | 内部事件分发 | 不定义业务事件 |
| External API | REST 文件访问、轨迹查询、集群状态 | 不暴露 task/chat/session 接口 |
| FileSyncManager | Worker 文件 → OSS/S3 同步，防抖，清单 | 不知道 book_slug → task_id 映射 |
| Harness 接口 | 定义 `Harness` 基类和回调契约 | 不提供具体实现 |
| 前置准备 | 阿里云/AWS 控制台创建 VPC/安全组/密钥对 | 不管实例创建 |

### 3.2 Audiobook Agent Service (Application)

| 模块 | 职责 | 数据存储 |
|------|------|---------|
| AudiobookHarness | 实现 Harness 接口，定义 Bootstrap 步骤、事件回调、文件同步配置 | — |
| BookQueue | 做书请求排队，优先级调度 | 内存 + JSON 持久化 |
| SessionRegistry | task_id → (worker_id, session_id, book_slug) 映射 | JSON 持久化（崩溃可恢复） |
| SlotScheduler | 查找空闲 Worker、生产/修改槽位管理 | 从 Worker Runtime 状态获取 |
| ChatRelay | 用户修改指令 → Worker `--resume`，聊天记录通过 OSS 日志提供 | — |
| TaskSyncMapper | 维护 Worker 上 book_slug ↔ task_id ↔ OSS prefix 的映射，推送给 Worker | 同步到 Worker Runtime |
| Webhook Emitter | 向 audio_book_echo_editor 推送事件 | 回调 URL 配置 |
| Audiobook API | `/api/tasks/produce`、`/api/tasks/{id}/chat`、`/api/tasks/{id}/status` 等 | — |
| Retry/Continue Logic | 从 OSS 恢复 workspace、清理 Phase 产物、重跑 | 操作 OSS + Worker 文件系统 |

### 3.3 audio_book_echo_editor (Existing App Adaptation)

| 模块 | 职责 |
|------|------|
| ScriptGenerationBackend 枚举 | Task 表新增跑书方式字段 |
| ElasticBookRun 模型 | 记录 Elastic 执行状态、OSS 指针、session_id |
| ElasticAgentClient | HTTP 客户端，调用 Audiobook Agent Service API |
| ElasticBookProductionService | 从 Task+Book 组装请求，提交 Elastic，管理生命周期 |
| WebhookService | 接收 Audiobook Agent Service 回调，更新状态，触发回灌 |
| OssFileService | 从 OSS 读取 manifest、最终稿、文件列表 |
| AgentOutput 回灌 | 将 Elastic 产物写入 `final_proofreading` AgentOutput，对接现有 TTS/BGM |
| 前端双引擎 UI | 创建任务时选择引擎，任务详情按引擎展示不同 UI |

---

## 4. 端到端数据流

### 4.1 新书生产流程

```
用户在前端选书 → 选择 "Elastic-Agent" 引擎 → 提交
        │
        ▼
audio_book_echo_editor 后端:
  1. 创建 Task (script_generation_backend=elastic_agent)
  2. 从 Book 表取原文和元数据
  3. 创建 elastic_book_runs 记录（status=pending，便于跟踪后续状态）
  4. 调用 Audiobook Agent Service: POST /api/tasks/produce
        │
        ▼
Audiobook Agent Service (Manager):
  1. BookQueue 入队
  2. SlotScheduler 查找空闲 Worker (production_slot 有空位)
  3. 通过 Worker Runtime 写入原始文本到 Worker
  4. 同时上传原始文本到 OSS (tasks/{task_id}/source/)
  5. TaskSyncMapper 注册映射: book_slug ↔ task_id ↔ OSS prefix
  6. 推送映射到 Worker Runtime
  7. 启动 Claude Code: /audiobook raw_text.md ...
  8. Webhook → audio_book: task.production.started
        │
        ▼
Worker 上 Claude Code 执行 (1-2 小时):
  - stdout NDJSON → Worker Runtime 双写:
    ├ LOG 事件 → Manager EventBus (内部: Phase 检测、调度)
    └ NDJSON 日志文件 → FileSyncManager → OSS → 前端轮询 chat/history
  - 文件变更 → FileSyncManager → OSS (根据 TaskSyncMapper 的映射)
  - 文件同步完成 → FILE_SYNCED 事件 → Manager → Webhook → audio_book
  - Phase 切换 → state.json 变更 → Webhook → audio_book: task.phase.changed
        │
        ▼
Claude Code 完成 (PROCESS_EXIT):
  1. Worker Runtime 从 result 事件提取 session_id
  2. Manager: SessionRegistry 注册, 释放生产槽位
  3. Webhook → audio_book: task.production.completed (附带 OSS 指针)
        │
        ▼
audio_book_echo_editor 后端收到完成 Webhook:
  1. 从 OSS 读取 _sync_manifest.json
  2. 按优先级选择最终稿 (delivery > compliant > final)
  3. 下载最终稿 markdown
  4. 写入 AgentOutput(agent_name="elastic_audiobook")
  5. 写入 AgentOutput(agent_name="final_proofreading") ← 兼容层
  6. Task.status=REVIEWING, script_status=PENDING_REVIEW
        │
        ▼
进入现有审核 → TTS → BGM → 成品流程 (与 legacy 完全一致)
```

### 4.2 修改流程

```
用户在前端进入已完成的 Elastic 任务聊天界面 → 发送修改指令
        │
        ▼
audio_book_echo_editor 后端:
  POST /api/tasks/{task_id}/script-production/chat
  → ElasticAgentClient.send_chat(external_task_id, message)
        │
        ▼
Audiobook Agent Service:
  1. SessionRegistry 查找: task_id → worker_id, session_id
  2. 检查 Worker edit_slots 是否有空
  3. Worker Runtime: claude -p "修改指令" --resume {session_id} ...
  4. 修改过程: NDJSON → 日志文件 → FileSyncManager → OSS → 前端轮询
  5. 文件变更 → OSS 同步
  6. 完成 → Webhook → audio_book: task.edit.completed
        │
        ▼
audio_book_echo_editor 后端收到修改完成 Webhook:
  1. 重新从 OSS 读取最终稿
  2. 覆盖更新 AgentOutput(final_proofreading)
  3. 可选: 重新触发审核流程
```

### 4.3 实时数据流

```
                    ┌── 聊天流 ─────────────────┐
                    │                           │
Worker Claude Code  │  Audiobook Agent Service  │  audio_book 后端     前端
  stdout NDJSON ────┼→ Worker Runtime 双写:      │
                    │  ├ LOG 事件 → Manager      │
                    │  │  EventBus (内部用途:     │
                    │  │  Phase 检测、调度等)      │
                    │  └ NDJSON 日志文件 ─────────┼→ FileSyncManager → OSS
                    │    logs/{task_id}.ndjson   │
                    │        │                  │
                    │        ▼                  │
                    │  ABE 后端 REST 读取 OSS ───┼──→ 前端轮询 chat/history
                    │                           │
                    ├── 文件同步 ────────────────┤
                    │                           │
  文件变更 ─────────┼→ FileSyncManager → OSS    │
  FILE_SYNCED 事件 ─┼→ EventBus → Webhook ──────┼→ 更新 manifest ─→ 文件列表刷新
                    │                           │
                    ├── 状态更新 ────────────────┤
                    │                           │
  state.json 变更 ──┼→ OSS 同步 + Webhook ──────┼→ 更新 phase ────→ 进度条更新
                    └───────────────────────────┘
```

---

## 5. 关键设计决策

### 5.1 为什么 Elastic-Agent 是 Library 而非 Service

| 方案 | 优点 | 缺点 |
|------|------|------|
| **Library (选定)** | Audiobook Service 完全控制部署和配置；框架可被多个 Harness 复用；无进程间通信开销 | 框架升级需要重新部署 Audiobook Service |
| 独立 Service + 插件 | 框架独立升级 | 需要定义插件协议；Harness 与 Manager 之间需要 IPC/RPC |

### 5.2 为什么不把 Audiobook 逻辑直接放在 Elastic-Agent 仓库

- Elastic-Agent 是通用框架，不应包含业务逻辑
- 其他 Harness（ML Research、CCM）有各自不同的需求
- 独立仓库便于独立迭代和部署

### 5.3 为什么 audio_book_echo_editor 不直接调用 Elastic-Agent 框架

- audio_book_echo_editor 有自己的技术栈和部署方式
- 它只需要 HTTP API 调用 + Webhook 回调，不需要引入框架依赖
- Audiobook Agent Service 作为中间层，封装了所有框架细节

### 5.4 task_id 统一策略

**整个链路使用 audio_book_echo_editor 的 `Task.id` 作为唯一主键**（转为字符串传递）：

```
audio_book 的 Task.id = "123"
  → Audiobook Agent Service 的 external_task_id = "123"
  → OSS 路径: tasks/123/
  → Webhook 中的 task_id = "123"
  → SessionRegistry 的 key = "123"
```

不引入额外的 ID 转换层。Elastic-Agent 框架内部的 `task_id` 也直接使用这个值。

### 5.5 OSS 路径统一

所有文件同步到同一个 OSS bucket，路径统一格式：

```
oss://{ELASTIC_AGENT_OSS_BUCKET}/{ELASTIC_AGENT_OSS_PREFIX}tasks/{task_id}/
  ├── _sync_manifest.json
  ├── source/
  │   ├── raw_text.md
  │   └── metadata.json
  ├── workspace/
  │   ├── state.json
  │   ├── manuscript_final.md
  │   └── ...
  ├── session/
  │   ├── session.jsonl
  │   └── .claude.json
  ├── delivery/
  │   ├── audiobook_manuscript.md
  │   └── audiobook_delivery.zip
  └── logs/
      ├── production.ndjson
      └── edits/
```

- `ELASTIC_AGENT_OSS_BUCKET` 和 `ELASTIC_AGENT_OSS_PREFIX` 由 audio_book_echo_editor 在提交做书请求时指定
- 提交 API 中 `oss.prefix` 已包含 `tasks/{task_id}/`（如 `elastic-agent/tasks/123/`），Audiobook Agent Service 直接使用，不再拼接
- Audiobook Agent Service 按此路径配置 FileSyncManager
- audio_book_echo_editor 按此路径读取文件

---

## 6. 开发阶段与依赖

### 6.1 总体时间线

```
                 Week 1-2    Week 3-4    Week 5-6    Week 7-8    Week 9-10
Elastic-Agent:   [Phase A ----][Phase B ----][Phase C ----][Phase D ----][Phase E ----]
                 云资源管理     Worker通信    文件同步+API   Bootstrap     稳定性+测试

Audiobook Svc:               [Phase 1 --------][Phase 2 --------][Phase 3 --------]
                              Harness+基础API   修改模式+Queue    多Worker+Webhook

audio_book:                                    [Phase α --------][Phase β --------]
                                                数据模型+API      前端+集成测试
```

### 6.2 关键依赖

```
Elastic-Agent Phase A (云资源) ────┐
                                   ├──→ Audiobook Svc Phase 1 (需要框架基础)
Elastic-Agent Phase B (通信) ──────┘
                                            │
Elastic-Agent Phase C (文件同步+API) ───────┤
                                            ├──→ audio_book Phase α (需要 API 契约稳定)
Audiobook Svc Phase 1 (基础API) ────────────┘
                                            │
Audiobook Svc Phase 2 (修改模式) ──→ audio_book Phase β (前端集成)
```

### 6.3 可并行开发的部分

| 仓库 | 可独立开发 | 需等待上游 |
|------|-----------|-----------|
| Elastic-Agent | 全部核心模块（Phase A-E） | 无 |
| Audiobook Svc | Harness 逻辑、API 设计、单元测试（用 DryRunProvider） | 集成测试需要框架 Phase B 完成 |
| audio_book | 数据模型迁移、ElasticAgentClient mock 开发、前端 UI | Webhook 集成需要 Audiobook Svc Phase 1 |

---

## 7. _sync_manifest.json 统一格式

三个仓库共享同一份 manifest 格式规范：

```json
{
  "task_id": "123",
  "worker_id": "aliyun:i-bp1xxx",
  "status": "syncing",
  "updated_at": "2026-05-17T14:30:03Z",
  "files": [
    {
      "path": "workspace/state.json",
      "oss_key": "elastic-agent/tasks/123/workspace/state.json",
      "size": 1234,
      "md5": "abc123",
      "content_type": "application/json",
      "role": "state",
      "synced_at": "2026-05-17T14:30:01Z"
    },
    {
      "path": "workspace/manuscript_final.md",
      "oss_key": "elastic-agent/tasks/123/workspace/manuscript_final.md",
      "size": 58201,
      "md5": "def456",
      "content_type": "text/markdown",
      "role": "manuscript_final",
      "synced_at": "2026-05-17T14:30:03Z"
    },
    {
      "path": "delivery/audiobook_manuscript.md",
      "oss_key": "elastic-agent/tasks/123/delivery/audiobook_manuscript.md",
      "size": 56000,
      "md5": "ghi789",
      "content_type": "text/markdown",
      "role": "delivery_manuscript",
      "synced_at": "2026-05-17T14:30:05Z"
    }
  ]
}
```

**格式要点：**
- `files` 使用数组格式（非 dict），支持 `role` 字段便于语义查询
- `oss_key` 是完整的 OSS 对象路径（不包含 bucket）
- `role` 字段值：`state`、`source`、`source_metadata`、`manuscript_final`、`manuscript_compliant`、`delivery_manuscript`、`delivery_intro`、`delivery_export`、`session`、`session_config`、`log_production`、`log_edit`、`workspace_file`（完整定义见 [05-interface-contracts.md §4.2](05-interface-contracts.md)）
- `md5` 用于增量同步判断和完整性校验

---

## 8. Webhook 事件统一格式

Audiobook Agent Service → audio_book_echo_editor 的所有 Webhook 事件共享结构：

```json
{
  "event_id": "evt_20260517_001",
  "event_type": "task.production.completed",
  "task_id": "123",
  "sequence": 7,
  "timestamp": "2026-05-17T14:30:00Z",
  "data": {
    "status": "completed",
    "phase": "completed",
    "progress_pct": 100,
    "worker_id": "aliyun:i-bp1xxx",
    "session_id": "claude-session-abc123",
    "oss": {
      "bucket": "audio-book-echo-editor-sh-oss",
      "prefix": "elastic-agent/tasks/123/",
      "manifest_key": "elastic-agent/tasks/123/_sync_manifest.json",
      "manuscript_key": "elastic-agent/tasks/123/delivery/audiobook_manuscript.md"
    },
    "metrics": {
      "duration_seconds": 5400,
      "phases_completed": 10
    }
  }
}
```

**事件类型完整列表：**

| 事件 | 触发时机 | data 中包含 |
|------|---------|------------|
| `task.production.queued` | 做书请求入队 | queue_position |
| `task.production.started` | Worker 开始执行 | worker_id |
| `task.phase.changed` | Phase 切换 | phase, progress_pct |
| `task.file.synced` | 关键文件同步完成 | synced_files[] |
| `task.session.registered` | 做书完成，session 注册 | session_id |
| `task.production.completed` | 做书成功完成 | oss, metrics, session_id |
| `task.production.failed` | 做书失败 | error_type, error_message, last_phase |
| `task.production.cancelled` | 做书被取消 | cancelled_by |
| `task.edit.started` | 修改开始 | edit_run_id |
| `task.edit.completed` | 修改完成 | edit_run_id, oss |
| `task.edit.failed` | 修改失败 | edit_run_id, error_message |
| `worker.unhealthy` | Worker 异常 | worker_id, affected_tasks[] |

---

## 9. 各仓库详细方案文档索引

| 文档 | 内容 | 主要受众 |
|------|------|---------|
| [01-elastic-agent-framework-mvp.md](01-elastic-agent-framework-mvp.md) | Elastic-Agent 框架 MVP 实现计划 | Elastic-Agent 开发者 |
| [02-audiobook-agent-service.md](02-audiobook-agent-service.md) | Audiobook Agent Service 方案（Harness 实现、API、调度） | Audiobook Agent Service 开发者 |
| [03-audiobook-app-adaptation.md](03-audiobook-app-adaptation.md) | audio_book_echo_editor 适配方案（双引擎、数据模型、前端） | audio_book_echo_editor 开发者 |
| [04-gap-analysis.md](04-gap-analysis.md) | 方案缺陷分析与补充方案 | 全员 |
| [05-interface-contracts.md](05-interface-contracts.md) | 三方接口契约（API、事件、数据格式） | 全员 |
| [06-testing-isolation.md](06-testing-isolation.md) | 独立测试与 Mock 策略（三仓库隔离测试 + 拼接方案） | 全员 |
| [TODO.md](TODO.md) | 三仓库全量 TODO + 配置变量清单 + 开发阶段依赖 | 全员 |

---

## 10. 风险总览

| 风险 | 严重度 | 缓解措施 | 详见 |
|------|--------|---------|------|
| Claude Code `--resume` 可靠性未经大规模验证 | 高 | Phase 1 即验证；备用方案: `/continue-book` 从 state.json 恢复 | 04-gap-analysis §3.5 |
| FileSyncManager 多任务路径映射复杂度 | 高 | TaskSyncMapper 组件独立设计，映射表持久化 | 04-gap-analysis §3.1 |
| SessionRegistry 内存丢失导致修改不可路由 | 高 | JSON 持久化 + 启动时从 manifest 重建 | 04-gap-analysis §3.3 |
| 单 Worker 4 并发 Claude 账号额度耗尽 | 中 | CredentialPool 按槽位预分配 + 额度监控 | 04-gap-analysis §3.6 |
| Webhook 丢失导致 audio_book 状态不更新 | 中 | 轮询兜底 + 事件重放 | 04-gap-analysis §3.9 |
| 修改流程文件不同步（sync mapping 已注销） | 中 | 修改前重新 REGISTER，完成后 flush + UNREGISTER | 04-gap-analysis §3.19 |
| 同一任务并发修改导致 session 损坏 | 高 | session.status 互斥检查 + 前端按钮防重 | 04-gap-analysis §3.20 |
| OSS 同步延迟导致前端读到旧数据 | 低 | 三种新鲜度策略（事件驱动/直接查询/强制刷新） | 02-audiobook-agent-service §5.3 |
