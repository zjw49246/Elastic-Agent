# 三方接口契约

> 本文档定义 audio_book_echo_editor、Audiobook Agent Service、Elastic-Agent 框架三方之间的接口契约。
>
> **约定：** 接口变更必须向后兼容（新增字段可选），破坏性变更需同步通知所有消费方。

---

## 1. 接口总览

```
audio_book_echo_editor          Audiobook Agent Service          Worker (Elastic-Agent 框架)
     (调用方)                        (服务方+调用方)                    (被管理方)
        │                                │                                │
        ├── HTTP API ──────────────────▶│                                │
        │   (做书/修改/取消/重试/状态)    │                                │
        │                                │                                │
        │◀── Webhook ──────────────────│                                │
        │   (状态/完成/失败事件)          │                                │
        │                                │                                │
        │   OSS 读取 ◀─── OSS 写入 ◀────│────── FileSyncManager ─────────│
        │   (manifest/文件/聊天日志)      │                                │
        │                                │                                │
        ├── REST ──────────────────────▶│                                │
        │   (聊天轮询 chat/live)         ├── WebSocket (Runtime) ─────▶│
        │                                │   (命令/日志/心跳/文件事件)     │
        └────────────────────────────────┘                                │
```

---

## 2. audio_book_echo_editor → Audiobook Agent Service

### 2.1 认证

所有请求携带 Bearer Token：
```
Authorization: Bearer {ELASTIC_AGENT_API_KEY}
Content-Type: application/json
```

### 2.2 提交做书任务

```
POST /api/tasks/produce
```

**请求体：**

```json
{
  "task_id": "123",
  "book_id": 456,
  "book_slug": "thinking-fast-123",
  "book_name": "思考，快与慢",
  "author": "丹尼尔·卡尼曼",
  "isbn": "9787508648336",
  "genre": "nonfiction",
  "language": "zh-CN",
  "persona": "nonfiction_default",
  "target_pct": 12,
  "priority": 0,
  "raw_text": "整本书原文文本...",
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

**字段规范：**

| 字段 | 类型 | 必填 | 约束 | 说明 |
|------|------|------|------|------|
| task_id | string | 是 | 全局唯一 | audio_book 的 Task.id 转字符串 |
| book_id | int | 是 | | audio_book 的 Book.id |
| book_slug | string | 是 | `^[a-z0-9-]{1,60}$` | Worker 本地目录名 |
| book_name | string | 是 | | 书名（中文/英文） |
| author | string | 否 | | 作者 |
| isbn | string | 否 | | ISBN |
| genre | string | 是 | `fiction` \| `nonfiction` | 书籍类型 |
| language | string | 否 | BCP-47 | 语言代码（默认 zh-CN） |
| persona | string | 是 | | audiobook 插件 persona |
| target_pct | int | 是 | 1-100 | 压缩比例 |
| priority | int | 否 | 默认 0 | 队列优先级，越大越优先 |
| raw_text | string | 与 raw_text_oss_uri 二选一 | | 原文内容 |
| raw_text_oss_uri | string | 与 raw_text 二选一 | oss:// 格式 | 大文本通过 OSS 传递 |
| metadata | object | 否 | | 补充元数据 |
| callback.url | string | 是 | HTTPS URL | Webhook 回调地址 |
| callback.secret_id | string | 是 | | 验签密钥标识 |
| oss.bucket | string | 是 | | 产物写入的 OSS bucket |
| oss.prefix | string | 是 | 末尾含 /，已包含 `tasks/{task_id}/` | 产物 OSS 路径前缀（如 `elastic-agent/tasks/123/`） |
| options.keep_session | bool | 是 | | 是否保留 session 供后续修改 |

**响应（200）：**

```json
{
  "success": true,
  "task_id": "123",
  "status": "queued",
  "queue_position": 4,
  "oss_prefix": "elastic-agent/tasks/123/",
  "message": "Task accepted"
}
```

**错误响应：**

| HTTP | 场景 | 响应 |
|------|------|------|
| 400 | 缺少必填字段 | `{"error": "missing_field", "field": "task_id"}` |
| 409 | task_id 已存在 | `{"error": "task_exists", "task_id": "123", "current_status": "running"}` |
| 503 | 服务不可用 | `{"error": "service_unavailable"}` |

### 2.3 查询任务状态

```
GET /api/tasks/{task_id}/status
```

**响应（200）：**

```json
{
  "task_id": "123",
  "status": "running",
  "phase": "phase_04_draft",
  "progress_pct": 45,
  "worker_id": "aliyun:i-bp1xxx",
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

**status 枚举：**

| 值 | 含义 |
|---|------|
| `queued` | 排队中 |
| `dispatching` | 正在分配 Worker |
| `running` | 执行中 |
| `completed` | 成功完成 |
| `failed` | 失败 |
| `cancelled` | 已取消 |

### 2.4 取消任务

```
POST /api/tasks/{task_id}/cancel
```

```json
{
  "reason": "user_cancelled",
  "operator": {"user_id": 1, "username": "admin"}
}
```

**响应（200）：** `{"success": true, "status": "cancelled"}`

**错误：** 404 任务不存在 / 409 任务已完成不可取消

### 2.5 续跑任务

```
POST /api/tasks/{task_id}/continue
```

```json
{
  "from_latest_state": true,
  "operator": {"user_id": 1, "username": "admin"}
}
```

**响应（200）：** `{"success": true, "status": "queued"}`

### 2.6 重试任务

```
POST /api/tasks/{task_id}/retry
```

```json
{
  "from_phase": 3,
  "reason": "重新生成大纲",
  "operator": {"user_id": 1, "username": "admin"}
}
```

`from_phase=0` 表示全部重来。

**响应（200）：** `{"success": true, "status": "queued"}`

### 2.7 发送修改指令

```
POST /api/tasks/{task_id}/chat
```

```json
{
  "message": "请把第三章开头改得更适合口播",
  "idempotency_key": "uuid-xxx"
}
```

**响应（200）：**

```json
{
  "success": true,
  "edit_run_id": "edit_abc123",
  "status": "running"
}
```

**错误：**

| HTTP | 场景 |
|------|------|
| 404 | 任务不存在 |
| 409 | 任务未完成（无 session），或该任务已有修改正在进行中 |
| 429 | 修改槽位已满（含 `retry_after` 秒数） |
| 503 | Worker 离线 |

### 2.8 实时聊天轮询

```
GET /api/tasks/{task_id}/chat/live?offset={byte_offset}
```

从 OSS 的 `logs/production.ndjson` 增量读取新行。

**参数：**
- `offset`: 上次读取到的字节偏移（首次传 0）

**响应（200）：**

```json
{
  "lines": [
    {"data": "{\"type\":\"assistant\",...}", "parsed": {"type": "assistant"}, "offset": 1234}
  ],
  "next_offset": 5678,
  "has_more": false,
  "synced_at": "2026-05-18T10:20:05Z"
}
```

前端每 2-3 秒轮询一次。收到 `task.file.synced` webhook 后可立即轮询。

### 2.9 强制文件同步

```
POST /api/tasks/{task_id}/files/sync
```

```json
{
  "path": "workspace/manuscript_final.md"
}
```

**响应（200）：**

```json
{
  "synced_at": "2026-05-18T10:20:05Z",
  "manifest_key": "elastic-agent/tasks/123/_sync_manifest.json"
}
```

### 2.10 列出 Workers

```
GET /api/workers
```

**响应（200）：**

```json
{
  "workers": [
    {
      "worker_id": "aliyun:i-bp1xxx",
      "status": "ready",
      "production_slots": {"used": 1, "max": 1},
      "edit_slots": {"used": 2, "max": 3},
      "active_tasks": ["123", "456"],
      "completed_tasks_count": 15,
      "uptime_hours": 72.5
    }
  ]
}
```

---

## 3. Audiobook Agent Service → audio_book_echo_editor (Webhook)

### 3.1 Webhook 协议

**认证：** HMAC-SHA256 签名

```
POST {callback.url}
Headers:
  Content-Type: application/json
  X-Elastic-Agent-Event-Id: evt_20260518_001
  X-Elastic-Agent-Signature: sha256=<HMAC-SHA256(secret, body)>
  X-Elastic-Agent-Timestamp: 1716048000
```

**验签算法：**

```python
import hmac, hashlib
expected = hmac.new(
    webhook_secret.encode(),
    request.body,
    hashlib.sha256
).hexdigest()
actual = request.headers["X-Elastic-Agent-Signature"].removeprefix("sha256=")
if not hmac.compare_digest(expected, actual):
    return 401
```

**时间戳校验：** 如果 `abs(now - X-Elastic-Agent-Timestamp) > 300s` → 拒绝（防重放）

### 3.2 事件格式

```json
{
  "event_id": "evt_20260518_001",
  "event_type": "task.production.completed",
  "task_id": "123",
  "sequence": 7,
  "timestamp": "2026-05-18T10:30:00Z",
  "data": {}
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| event_id | string | 全局唯一事件 ID，用于幂等 |
| event_type | string | 事件类型 |
| task_id | string | 关联的任务 ID |
| sequence | int | 递增序号，用于排序和丢失检测 |
| timestamp | string | ISO 8601 时间戳 |
| data | object | 事件类型特定的数据（见下方） |

### 3.3 事件类型详情

#### task.production.queued

```json
{
  "event_type": "task.production.queued",
  "data": {
    "status": "queued",
    "queue_position": 4
  }
}
```

#### task.production.started

```json
{
  "event_type": "task.production.started",
  "data": {
    "status": "running",
    "worker_id": "aliyun:i-bp1xxx"
  }
}
```

#### task.phase.changed

```json
{
  "event_type": "task.phase.changed",
  "data": {
    "status": "running",
    "phase": "phase_04_draft",
    "progress_pct": 45,
    "worker_id": "aliyun:i-bp1xxx"
  }
}
```

**phase 枚举值：**

| phase | 含义 | progress_pct |
|-------|------|-------------|
| `phase_00_init` | 初始化 | 0 |
| `phase_01_decomposition` | 书籍解构 | 10 |
| `phase_02_blueprint` | 战略蓝图 | 20 |
| `phase_03_slicing` | 源文切片 | 30 |
| `phase_04_draft` | 主体生产 | 40 |
| `phase_05_persona` | 人格融合 | 55 |
| `phase_06_opening_closing` | 开头结尾 | 65 |
| `phase_07_audit` | 审核循环 | 75 |
| `phase_08_compliance` | 合规处理 | 85 |
| `phase_08_5_intro` | 简介生成 | 90 |
| `phase_09_delivery` | 交付打包 | 95 |
| `completed` | 全部完成 | 100 |

#### task.file.synced

```json
{
  "event_type": "task.file.synced",
  "data": {
    "synced_files": [
      {
        "path": "workspace/manuscript_final.md",
        "oss_key": "elastic-agent/tasks/123/workspace/manuscript_final.md",
        "role": "manuscript_final",
        "synced_at": "2026-05-18T10:25:00Z"
      }
    ],
    "manifest_key": "elastic-agent/tasks/123/_sync_manifest.json"
  }
}
```

#### task.session.registered

```json
{
  "event_type": "task.session.registered",
  "data": {
    "session_id": "claude-session-abc123",
    "worker_id": "aliyun:i-bp1xxx"
  }
}
```

#### task.production.completed

```json
{
  "event_type": "task.production.completed",
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
      "manuscript_key": "elastic-agent/tasks/123/delivery/audiobook_manuscript.md",
      "export_key": "elastic-agent/tasks/123/delivery/audiobook_delivery.zip"
    },
    "metrics": {
      "duration_seconds": 5400,
      "phases_completed": 10
    }
  }
}
```

#### task.production.failed

```json
{
  "event_type": "task.production.failed",
  "data": {
    "status": "failed",
    "error_type": "process_crash",
    "error_message": "Claude Code exited with code 1",
    "last_phase": "phase_04_draft",
    "worker_id": "aliyun:i-bp1xxx",
    "oss": {
      "manifest_key": "elastic-agent/tasks/123/_sync_manifest.json"
    }
  }
}
```

**error_type 枚举：**

| 值 | 含义 |
|---|------|
| `process_crash` | Claude Code 进程异常退出 |
| `process_timeout` | 执行超时 |
| `progress_stalled` | 长时间无进展 |
| `worker_unhealthy` | Worker 异常 |
| `bootstrap_failed` | Worker 初始化失败 |
| `oss_sync_failed` | OSS 同步失败 |
| `credential_exhausted` | 凭证额度耗尽 |
| `internal_error` | 内部错误 |

#### task.production.cancelled

```json
{
  "event_type": "task.production.cancelled",
  "data": {
    "status": "cancelled",
    "cancelled_by": {"user_id": 1, "username": "admin"},
    "last_phase": "phase_04_draft"
  }
}
```

#### task.edit.started

```json
{
  "event_type": "task.edit.started",
  "data": {
    "edit_run_id": "edit_abc123",
    "worker_id": "aliyun:i-bp1xxx"
  }
}
```

#### task.edit.completed

```json
{
  "event_type": "task.edit.completed",
  "data": {
    "edit_run_id": "edit_abc123",
    "oss": {
      "manifest_key": "elastic-agent/tasks/123/_sync_manifest.json",
      "manuscript_key": "elastic-agent/tasks/123/delivery/audiobook_manuscript.md"
    },
    "session_id": "claude-session-def456"
  }
}
```

#### task.edit.failed

```json
{
  "event_type": "task.edit.failed",
  "data": {
    "edit_run_id": "edit_abc123",
    "error_type": "process_crash",
    "error_message": "Claude Code exited with code 1"
  }
}
```

#### worker.unhealthy

```json
{
  "event_type": "worker.unhealthy",
  "data": {
    "worker_id": "aliyun:i-bp1xxx",
    "affected_tasks": ["123", "456"],
    "reason": "heartbeat_timeout"
  }
}
```

### 3.4 Webhook 重试策略

Audiobook Agent Service 在发送 Webhook 失败时：

| 重试次数 | 延迟 | 总经过时间 |
|---------|------|-----------|
| 1 | 1s | 1s |
| 2 | 5s | 6s |
| 3 | 30s | 36s |
| 4 | 5min | ~5.5min |
| 5 | 30min | ~35min |
| 失败 | 写入死信队列 | — |

"失败"定义：非 2xx 响应或连接超时（10s）。

### 3.5 幂等要求

audio_book_echo_editor 必须保证：
- 同一 `event_id` 重复处理不会导致数据重复（使用 `event_id` 做幂等去重）
- 同一事件第二次到达时返回 200（不返回 409 或 500）
- `sequence` 乱序到达时，如果 `new_sequence <= last_processed_sequence` → 安全忽略或仅记录不处理

---

## 4. OSS 数据格式契约

### 4.1 目录结构

```
oss://{bucket}/{oss.prefix}/
  (oss.prefix 已包含 tasks/{task_id}/，如 elastic-agent/tasks/123/)
├── _sync_manifest.json              # 同步清单（索引文件）
├── source/
│   ├── raw_text.md                  # 原始文本
│   └── metadata.json                # 元数据
├── workspace/
│   ├── state.json                   # 插件状态机
│   ├── compressed.md                # Phase 1 压缩版
│   ├── blueprint.md                 # Phase 2 蓝图
│   ├── quality_targets.json
│   ├── sections/
│   │   └── section_*.txt            # Phase 3 切片
│   ├── drafts/
│   │   └── draft_*.md               # Phase 4 底稿
│   ├── styled/
│   │   └── styled_*.md              # Phase 5 风格化
│   ├── manuscript_v1.md             # Phase 6 初版
│   ├── iter_*/
│   │   └── audit_*.json             # Phase 7 审核
│   ├── manuscript_final.md          # Phase 7 终版
│   ├── manuscript_compliant.md      # Phase 8 合规版
│   ├── intro_final.md               # Phase 8.5 简介
│   └── metrics.json
├── delivery/
│   ├── audiobook_manuscript.md      # 最终交付稿
│   ├── audiobook_intro.md           # 简介
│   └── audiobook_delivery.zip       # 打包
├── session/
│   ├── session.jsonl                # Claude Code 对话历史
│   └── .claude.json                 # 项目配置
└── logs/
    ├── production.ndjson            # Worker Runtime 自动写入的生产过程完整日志
    └── edits/
        └── {edit_run_id}.ndjson     # Worker Runtime 自动写入的修改过程日志
```

### 4.2 _sync_manifest.json 格式

```json
{
  "task_id": "123",
  "worker_id": "aliyun:i-bp1xxx",
  "status": "completed",
  "updated_at": "2026-05-18T11:45:00Z",
  "files": [
    {
      "path": "workspace/state.json",
      "oss_key": "elastic-agent/tasks/123/workspace/state.json",
      "size": 2048,
      "md5": "a1b2c3d4e5f6",
      "content_type": "application/json",
      "role": "state",
      "synced_at": "2026-05-18T11:45:00Z"
    }
  ]
}
```

**字段规范：**

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| task_id | string | 是 | 任务 ID |
| worker_id | string | 是 | 当前 Worker ID |
| status | string | 是 | `syncing` \| `completed` \| `error` |
| updated_at | string | 是 | 最后更新时间 (ISO 8601) |
| files | array | 是 | 文件清单 |
| files[].path | string | 是 | 相对于 task 根目录的路径 |
| files[].oss_key | string | 是 | OSS 对象完整路径（不含 bucket） |
| files[].size | int | 是 | 文件大小 (bytes) |
| files[].md5 | string | 是 | 文件 MD5 |
| files[].content_type | string | 是 | MIME 类型 |
| files[].role | string | 否 | 语义标签（见下方枚举） |
| files[].synced_at | string | 是 | 该文件最后同步时间 |

**role 枚举：**

| role | 含义 | 典型文件 |
|------|------|---------|
| `state` | 插件状态机 | workspace/state.json |
| `source` | 原始文本 | source/raw_text.md |
| `source_metadata` | 元数据 | source/metadata.json |
| `manuscript_final` | 终版讲稿 | workspace/manuscript_final.md |
| `manuscript_compliant` | 合规版讲稿 | workspace/manuscript_compliant.md |
| `delivery_manuscript` | 交付讲稿（最高优先级） | delivery/audiobook_manuscript.md |
| `delivery_intro` | 交付简介 | delivery/audiobook_intro.md |
| `delivery_export` | 打包下载 | delivery/audiobook_delivery.zip |
| `session` | Claude Code 对话历史 | session/session.jsonl |
| `session_config` | Claude Code 项目配置 | session/.claude.json |
| `log_production` | 生产过程完整 NDJSON 日志（Worker Runtime 双写） | logs/production.ndjson |
| `log_edit` | 修改过程完整 NDJSON 日志（Worker Runtime 双写） | logs/edits/*.ndjson |
| `workspace_file` | 其他工作文件 | workspace/**/* |

### 4.3 最终稿选择优先级

audio_book_echo_editor 从 manifest 中选择最终稿的规则：

```
优先级从高到低:
  1. role=delivery_manuscript  (delivery/audiobook_manuscript.md)
  2. role=manuscript_compliant (workspace/manuscript_compliant.md)
  3. role=manuscript_final     (workspace/manuscript_final.md)

如果 manifest 中无 role 字段（兼容旧数据）:
  按 path 匹配:
  1. delivery/audiobook_manuscript.md
  2. workspace/manuscript_compliant.md
  3. workspace/manuscript_final.md
```

### 4.4 state.json 格式（audiobook 插件输出）

```json
{
  "book_slug": "outliers",
  "phase": 9,
  "state": "DELIVERED",
  "session_id": "abc123-def456",
  "started_at": "2026-05-17T10:00:00Z",
  "finished_at": "2026-05-17T11:45:00Z",
  "phases_completed": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
  "current_issues": [],
  "decisions": {},
  "metrics": {
    "total_tokens": 45000000,
    "cost_estimate_usd": 3.50,
    "duration_seconds": 6300
  }
}
```

Audiobook Agent Service 解析 `phase` 字段（**数字**）来确定当前进度，映射到 Webhook 的 `phase` 枚举（**字符串**）：

| state.json phase (数字) | Webhook phase (字符串) | 含义 |
|---|---|---|
| 0 | `phase_00_init` | 初始化 |
| 1 | `phase_01_decomposition` | 书籍解构 |
| 2 | `phase_02_blueprint` | 战略蓝图 |
| 3 | `phase_03_slicing` | 源文切片 |
| 4 | `phase_04_draft` | 主体生产 |
| 5 | `phase_05_persona` | 人格融合 |
| 6 | `phase_06_opening_closing` | 开头结尾 |
| 7 | `phase_07_audit` | 审核循环 |
| 8 | `phase_08_compliance` | 合规处理 |
| 8.5 | `phase_08_5_intro` | 简介生成 |
| 9 | `phase_09_delivery` | 交付打包 |
| state=DELIVERED | `completed` | 全部完成 |

此映射由 Audiobook Agent Service 负责，audio_book_echo_editor 只接收字符串枚举。

### 4.5 chat/history 响应格式

`GET /api/tasks/{task_id}/chat/history` 从 OSS 的 `logs/production.ndjson`（或 `logs/edits/{edit_run_id}.ndjson`）解析后返回。

**响应格式：**

```json
{
  "task_id": "123",
  "messages": [
    {
      "role": "user",
      "content": "/audiobook /root/books/outliers/raw_text.md nonfiction_default target_pct=12",
      "timestamp": "2026-05-17T10:00:01Z"
    },
    {
      "role": "assistant",
      "content": "开始 Phase 1: 书籍解构...",
      "timestamp": "2026-05-17T10:00:05Z"
    },
    {
      "role": "tool_use",
      "tool_name": "Read",
      "tool_input": {"file_path": "/root/books/outliers/raw_text.md"},
      "timestamp": "2026-05-17T10:00:06Z"
    },
    {
      "role": "assistant",
      "content": "Phase 1 完成，已生成 compressed.md...",
      "timestamp": "2026-05-17T10:05:30Z"
    }
  ],
  "total_messages": 1234,
  "source": "logs/production.ndjson",
  "has_more": true,
  "next_offset": 50
}
```

**解析规则：**

从 NDJSON 日志中逐行解析，按 `parsed.type` 字段过滤和映射：

| NDJSON type | 响应中的 role | 包含字段 |
|---|---|---|
| `user` | `user` | content |
| `assistant` | `assistant` | content |
| `tool_use` | `tool_use` | tool_name, tool_input |
| `tool_result` | `tool_result` | tool_name, output (截断到 500 字符) |
| `result` | `result` | session_id, cost_usd, duration_seconds |
| `system` | (跳过) | — |

**分页参数：**
- `offset`: 起始消息索引（默认 0）
- `limit`: 返回消息数（默认 50，最大 200）
- `types`: 过滤消息类型，逗号分隔（如 `types=assistant,result`，默认返回全部）

---

## 5. Elastic-Agent 框架内部契约

### 5.1 通信协议扩展

为支持 Audiobook 场景，框架通信协议新增以下消息类型：

#### Manager → Worker

| 类型 | 字段 | 说明 |
|------|------|------|
| `REGISTER_SYNC_MAPPING` | `{task_id, book_slug, oss_prefix, watch_paths[], session_path_hash}` | 注册文件同步映射 |
| `UNREGISTER_SYNC_MAPPING` | `{task_id}` | 取消同步映射 |

#### Worker → Manager

| 类型 | 字段 | 说明 |
|------|------|------|
| `FILE_SYNCED` | `{task_id, path, oss_key, synced_at, md5}` | 文件同步完成通知，用于触发外部 Webhook（`FILE_CHANGE` 仅框架内部使用，不对外暴露） |
| `LOG` | `{task_id, stream, data, timestamp, parsed?}` | parsed 为可选的 NDJSON 结构化解析结果 |

`parsed` 字段（当 `data` 是合法 Claude Code stream-json 时自动填充）：

```json
{
  "type": "assistant" | "user" | "tool_use" | "tool_result" | "result" | "system",
  "cost_usd": float | null,
  "session_id": string | null
}
```

### 5.2 Harness 接口完整定义

> **注意：** 以下是框架 Harness 基类的**完整**接口，综合了 Audiobook、ML Research、CCM、Idea Review 四个 Harness 的需求。所有方法都有合理的默认实现，Harness 只需 override 自己需要的。

```python
class Harness(ABC):

    # ── 基础配置（所有 Harness 都会用到） ──

    def get_worker_lifecycle(self) -> WorkerLifecycle:
        """Worker 生命周期模型：PERSISTENT（常驻）或 EPHEMERAL（按需创建/销毁）"""
        return WorkerLifecycle.PERSISTENT

    @abstractmethod
    def get_bootstrap_steps(self) -> list[BootstrapStep]:
        """Bootstrap 初始化步骤列表。每个 Harness 必须实现。"""
        ...

    def get_event_handlers(self) -> dict[FrameworkEvent, Callable]:
        """订阅框架事件的回调函数映射"""
        return {}

    def get_worker_capacity(self) -> WorkerCapacity:
        """Worker 并发容量。Audiobook 用 AudiobookWorkerCapacity 子类扩展。"""
        return WorkerCapacity(max_concurrent_tasks=1)

    # ── 代码部署 ──

    def get_repo_url(self) -> str | None:
        """Worker 上需要 clone 的代码仓库 URL。返回 None 表示不需要 clone。"""
        return None

    def get_service_definitions(self) -> list[ServiceDefinition]:
        """Worker 上需要注册的常驻服务（systemd unit）。
        返回空列表表示无常驻服务（batch processing 模式，如 Idea Review）。"""
        return []

    # ── 凭证 ──

    def get_app_credentials(self) -> list[str]:
        """应用凭证名称列表（Git key、WandB token、API key 等）。
        框架在 Bootstrap 时将这些凭证安全注入 Worker 环境。"""
        return []

    # ── 健康检查 ──

    def get_health_check(self) -> dict | None:
        """自定义 L3 应用级健康检查。返回 None 使用默认（进程存活检查）。
        示例: {"type": "http", "url": "http://localhost:8080/status", "interval": 30}
        示例: {"type": "command", "command": "python3 -c 'import sdk; print(ok)'", "interval": 60}
        """
        return None

    # ── 扩缩容 ──

    def get_scaling_signal(self) -> ScalingSignal | None:
        """扩缩容信号。框架的 ScalingEngine（Phase 2）根据此信号自动决策。
        返回 None 表示不参与自动扩缩容（仅手动操作）。"""
        return None

    # ── 文件同步（opt-in，默认关闭） ──

    def get_file_sync_config(self) -> FileSyncConfig:
        """文件同步模板配置。enabled=False 时整个 FileSyncManager 不激活。"""
        return FileSyncConfig(enabled=False)

    def get_task_sync_mapping(self, task_context: dict) -> SyncMapping:
        """给定任务上下文，返回该任务的同步映射。仅在 file sync 启用时需要。"""
        raise NotImplementedError

    # ── 任务生命周期回调 ──

    async def on_task_completed(self, task_id: str, result: dict) -> None:
        """任务完成回调。Harness 可在此注册 session、发送 Webhook、写回外部 API 等。"""
        pass

    async def on_task_failed(self, task_id: str, error: dict) -> None:
        """任务失败回调。"""
        pass
```

### 5.3 WorkerCapacity 模型

```python
@dataclass
class WorkerCapacity:
    max_concurrent_tasks: int = 1

@dataclass
class AudiobookWorkerCapacity(WorkerCapacity):
    max_production_slots: int = 1
    max_edit_slots: int = 3
```

框架理解通用的 `max_concurrent_tasks`，Audiobook Agent Service 在此基础上实现 `production_slots` + `edit_slots` 的细分逻辑。

---

## 6. 版本与兼容性

### 6.1 API 版本策略

MVP 阶段不做 URL 版本（如 `/v1/`）。后续如需破坏性变更，通过 URL 版本区分。

### 6.2 向后兼容规则

- **新增字段：** 响应新增字段不算破坏（消费方忽略未知字段）
- **新增事件类型：** 不算破坏（消费方忽略未知事件类型）
- **移除字段/事件类型：** 破坏性变更，需双方协商
- **修改字段语义：** 破坏性变更

### 6.3 超时与限制

| 参数 | 值 | 说明 |
|------|---|------|
| HTTP 请求超时 | 30s | audio_book → Agent Service |
| Webhook 发送超时 | 10s | Agent Service → audio_book |
| Worker Runtime 心跳 | 30s | Worker ↔ Manager（框架内部） |
| raw_text 最大大小 | 10MB | 超过则用 raw_text_oss_uri |
| Webhook 最大重试 | 5 次 | 约 35 分钟内 |
| chat message 最大长度 | 10000 字符 | 单条修改指令 |
