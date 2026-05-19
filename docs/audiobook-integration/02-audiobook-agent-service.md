# Audiobook Agent Service 方案

> 本文档描述 **Audiobook Agent Service** 的完整设计方案。Audiobook Agent Service 是一个**独立仓库/独立部署的应用**，通过 `import elastic_agent` 引入 Elastic-Agent 框架作为 Library，实现有声书稿全自动化生产系统的 Manager 进程。
>
> 本文档基于 audiobook-nonfiction v1.1.1 的真实代码分析，以及 [00-overview.md](00-overview.md) 中定义的多仓库架构。

---

## 0. 定位与架构概述

### 0.1 Audiobook Agent Service 是什么

Audiobook Agent Service 是一个 **独立的 Python 应用**（独立仓库、独立部署），它：

- 通过 `uv add git+https://github.com/zjw49246/Elastic-Agent.git` 引入 Elastic-Agent 框架
- 实现 `AudiobookHarness(Harness)` 接口，定义有声书生产的业务逻辑
- 作为 **Manager 进程** 运行，管理多台 Worker 上的 Claude Code 会话
- 暴露有声书专用 API（做书、修改、状态查询等），供 audio_book_echo_editor 调用
- 推送 Webhook 事件到 audio_book_echo_editor

### 0.2 与 Elastic-Agent 框架的关系

```python
# Audiobook Agent Service 的 main.py 示意
from elastic_agent.manager import ElasticAgentManager
from elastic_agent.core.providers import AliyunProvider
from elastic_agent.harness import Harness

class AudiobookHarness(Harness):
    """有声书专用 Harness — 只包含业务逻辑"""
    ...

# Manager 内部自动组装全部框架组件
# (TaskRegistry, TaskScheduler, TaskRouter, WebhookEmitter,
#  CredentialPool, Worker Runtime, FileSyncManager, etc.)
manager = ElasticAgentManager(
    harness=AudiobookHarness(config),
    provider=AliyunProvider(aliyun_config),
)
app = manager.create_app()

# 挂载少量 Audiobook 专用路由（produce、retry from phase 等）
from audiobook_agent_service.api import audiobook_router
app.include_router(audiobook_router)
```

### 0.3 不包含什么

以下不属于 Audiobook Agent Service 的职责：
- Elastic-Agent 框架的通用能力（CloudProvider、Worker Runtime、NodeRegistry 等）—— 由框架包提供
- audio_book_echo_editor 的前端/后端适配 —— 由 audio_book_echo_editor 仓库自行实现
- audiobook-nonfiction 插件本身 —— 独立仓库，安装在 Worker 上

---

## 目录

1. [Audiobook 项目真实架构解析](#1-audiobook-项目真实架构解析)
2. [整体业务流程](#2-整体业务流程)
3. [基于框架的分布式架构设计](#3-基于框架的分布式架构设计)
4. [Harness 接口实现](#4-harness-接口实现)
5. [核心技术挑战与方案](#5-核心技术挑战与方案)
6. [分步实施方案](#6-分步实施方案)
7. [对 Elastic-Agent 框架的需求](#7-对-elastic-agent-框架的需求)

---

## 1. Audiobook 项目真实架构解析

### 1.1 项目定位

Audiobook 是一个 **Claude Code 插件（Skill）**，将非虚构书籍转换为 TTS-ready 的有声书讲稿（默认压缩到原文 9-17%）。它实现了一个 **10 Phase 全自动化生产流水线**，由 Main Agent 编排 22 个专用子 Agent，配合 9 个 Python 工具脚本。

**关键架构事实：**
- 它是 Claude Code 的 Skill 插件，运行在 Claude Code CLI 会话中
- 所有编排由 Claude Code 的 Main Agent 完成，不需要自建后端
- 状态管理基于文件系统 — `.work/{book_slug}/state.json`
- 子 Agent 通过 `Agent({subagent_type: "audiobook-xxx"})` 调用
- 单会话单书 — 一个 Claude Code 会话从头到尾处理一本书
- `/continue-book {book_slug}` 支持从任意中断点恢复
- 会话完成后，`session_id` 可用于 `--resume` 继续对话（后续修改）

### 1.2 10 Phase 生产流水线

| Phase | 名称 | 关键子 Agent | 产出 | 耗时 |
|-------|------|-------------|------|------|
| 0 | 初始化 | — | `state.json` | <1min |
| 1 | 书籍解构 | text-compressor(Sonnet*N), book-facts-checker | `compressed.md`, `book_meta.json` | 10-20min |
| 2 | 战略蓝图 | narration-framework-designer(**Opus**) | `blueprint.md`, `quality_targets.json` | 10-15min |
| 3 | 源文切片 | anchor-fixer(如需) | `sections/section_*.txt` | 2-5min |
| 4 | 主体生产 | draft-writer(**Opus***N) | `drafts/draft_*.md` | 20-40min |
| 5 | 人格融合 | persona-fusion(Sonnet*N) | `styled/styled_*.md` | 10-20min |
| 6 | 开头结尾 | opening-closing-editor(**Opus**) | `manuscript_v1.md` | 5-10min |
| 7 | 审核循环 | 7 auditors(Sonnet*7并行) + fixer(**Opus**), 最多 3 轮 | `manuscript_final.md` | 15-30min |
| 7.5 | 终审 | Main Agent 通读全文 | 决策记录 | 5min |
| 8 | 合规处理 | compliance-processor(**Opus**) | `manuscript_compliant.md` | 5-10min |
| 8.5 | 简介生成 | intro-generator + 3 auditor(并行) + fixer | `intro_final.md` | 5-10min |
| 9 | 交付打包 | — | `delivery/` | <1min |

**总计：** 1-2 小时/本书，50-80 次子 Agent 调用，30-80M token

### 1.3 工作目录结构

```
.work/{book_slug}/
├── state.json                    # 状态机（Phase/决策/已知问题/时间戳）
├── raw_text.md                   # Phase 1 原文
├── compressed.md                 # Phase 1 压缩版
├── book_meta.json, book_facts.json
├── blueprint.md, quality_targets.json
├── sections/section_*.txt        # Phase 3 切片
├── drafts/draft_*.md             # Phase 4 底稿
├── styled/styled_*.md            # Phase 5 风格化
├── manuscript_v1.md              # Phase 6 初版
├── iter_*/audit_*.json           # Phase 7 审核迭代
├── manuscript_final.md           # Phase 7 终版
├── manuscript_compliant.md       # Phase 8 合规版（如有）
├── intro_final.md                # Phase 8.5 简介
├── delivery/                     # Phase 9 交付
└── metrics.json                  # 成本/token 追踪
```

---

## 2. 整体业务流程

### 2.1 用户视角的完整流程

```
用户在做书前端选书 → 提交做书请求
        │
        ▼
Audiobook Agent Service 将请求入队 → 分发到有空闲生产槽位的 Worker
        │
        ▼
Worker 上 Claude Code 执行 /audiobook（1-2 小时）
  - 做书过程中：chat 实时流到前端，文件实时同步到 OSS/S3
  - 前端展示：实时聊天框 + Phase 进度条 + 文件目录增量
        │
        ▼
做书完成 → 会话保留在 Worker 上 → 前端展示最终讲稿
        │
        ▼
用户可以随时进入任意已完成的会话，发送修改指令
  - 修改请求 → Audiobook Agent Service 路由到对应 Worker → --resume 恢复会话
  - 修改过程中：chat 继续实时流到前端
  - 同一 Worker 上最多 3 个修改会话并行
```

### 2.2 两种工作模式

| 维度 | 生产模式（/audiobook） | 修改模式（--resume） |
|------|---------------------|---------------------|
| 触发 | 用户提交新书 | 用户在已完成的 chat 中发修改指令 |
| 耗时 | 1-2 小时 | 数分钟到十几分钟 |
| 资源 | 重（Opus 密集调用） | 轻（通常只用 fixer 或局部重写） |
| 并发 | 每 Worker 最多 **1** 个 | 每 Worker 最多 **3** 个（可配置） |
| 会话 | 新建 session | 复用已有 session_id |
| 独占性 | 占用生产槽位 | 占用修改槽位（与生产槽位独立） |

### 2.3 Worker 槽位模型

```
Worker (常驻，手动开启)
│
├── 生产槽位 (max_production_slots = 1)
│   └── [占用] Claude Code: /audiobook 《异类》  ← 1-2h 重负载
│
├── 修改槽位 (max_edit_slots = 3)
│   ├── [占用] Claude Code: --resume session_A "请修改第三章开头..."
│   ├── [占用] Claude Code: --resume session_B "调整尺度表达..."
│   └── [空闲]
│
└── 已完成会话池 (无上限)
    ├── session_C (《枪炮、病菌与钢铁》) — 可随时 --resume
    ├── session_D (《思考，快与慢》) — 可随时 --resume
    └── ...
```

**两种槽位独立计数。** 一台 Worker 可以同时跑 1 个新书生产 + 3 个修改会话 = 4 个 Claude Code 进程。

**并发参数可配置：**

```yaml
worker:
  max_production_slots: 1    # 同时做几本新书（默认 1）
  max_edit_slots: 3          # 同时修改几本已完成的书（默认 3）
```

---

## 3. 基于框架的分布式架构设计

### 3.1 整体架构

```
┌──────────────────────────────────────────────────────────────────────┐
│                    做书前后端（外部服务）                               │
│  提交做书请求 · 实时聊天框 · Phase 进度 · 文件浏览 · 发修改指令         │
└───────────────────────────┬──────────────────────────────────────────┘
                            │ HTTPS + Webhook
                            ▼
┌──────────────────────────────────────────────────────────────────────┐
│                    Audiobook Agent Service (Manager 进程)             │
│                                                                      │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │               Audiobook 业务层 (本仓库实现, 薄业务层)            │  │
│  │                                                                │  │
│  │  BookQueue        Phase 检测        Audiobook API              │  │
│  │  做书请求队列      Phase 进度解析     有声书专用路由              │  │
│  └────────────────────────┬───────────────────────────────────────┘  │
│                           │                                          │
│  ┌────────────────────────▼───────────────────────────────────────┐  │
│  │               Elastic-Agent 框架层 (import elastic_agent)      │  │
│  │                                                                │  │
│  │  CloudProvider    NodeRegistry     ExternalAPI(轨迹/文件)      │  │
│  │  CredentialPool   HealthChecker    FileSyncManager             │  │
│  │  EventBus         Bootstrap        Worker Runtime Server       │  │
│  │  TaskRegistry     TaskScheduler    TaskRouter                  │  │
│  │  task→worker 映射  槽位调度          双向消息路由                │  │
│  │  WebhookEmitter                                                │  │
│  │  事件推送                                                       │  │
│  └────────────────────────┬───────────────────────────────────────┘  │
└───────────────────────────┼──────────────────────────────────────────┘
                            │ Worker Runtime (WebSocket)
              ┌─────────────┼─────────────┐
              │             │             │
         ┌────▼────┐  ┌────▼────┐  ┌─────▼───┐
         │Worker 1 │  │Worker 2 │  │Worker N │
         │(常驻)   │  │(常驻)   │  │(常驻)   │
         │         │  │         │  │         │
         │生产[1/1]│  │生产[0/1]│  │生产[1/1]│
         │修改[2/3]│  │修改[0/3]│  │修改[1/3]│
         │         │  │         │  │         │
         │sessions:│  │sessions:│  │sessions:│
         │ A,B,C,D │  │ (空)    │  │ E,F     │
         └─────────┘  └─────────┘  └─────────┘
          手动开启      手动开启      手动开启
```

### 3.2 核心数据结构

#### Session Registry

每个做书会话通过框架的 TaskRegistry 注册，用于路由修改请求到正确的 Worker。

```
TaskRegistry (框架提供, Manager 侧):
  task_id → {
    worker_id:   "worker-1"
    session_id:  "abc123-def456"       # Claude Code 的 session ID
    book_slug:   "outliers"            # 插件内部使用的 slug（.work/{book_slug}/）
    status:      "producing" | "idle" | "editing"
    cwd:         "/root/.work/outliers"
    created_at:  "2026-05-17T10:00:00Z"
    finished_at: "2026-05-17T11:45:00Z" | null
  }
```

**持久化要求：**

```
存储路径: ~/.elastic-agent/task_registry.json (框架自动管理)
写入策略: 每次 register / update 后立即写入（操作频率低，不需要防抖）
格式: {task_id: {worker_id, session_id, book_slug, status, cwd, created_at, finished_at}}

启动恢复流程:
  1. 读取 task_registry.json
  2. 对比 NodeRegistry 中的在线 Worker → 清理已不存在 Worker 的 session
  3. 对存活 Worker 的 session → 标记状态为 "idle"（重启期间任务可能已完成或终止）
  4. 可选验证: 对比 OSS _sync_manifest.json → 确认 session 数据完整性

兜底重建:
  如果 task_registry.json 丢失或损坏:
    → 扫描所有 task 的 OSS _sync_manifest.json
    → 从 manifest 的 worker_id + session 文件重建映射
    → 这个过程较慢（需要 ListObjects），仅作为最后手段
```

#### Worker 槽位状态

```
WorkerSlotState (per Worker, 由 Worker Runtime 上报):
  production_slots: { used: 1, max: 1 }
  edit_slots:       { used: 2, max: 3 }
  active_sessions:  [
    { task_id: "task-001", book_slug: "outliers", session_id: "abc", mode: "producing", pid: 12345 },
    { task_id: "task-002", book_slug: "sapiens",  session_id: "def", mode: "editing",   pid: 12350 },
    { task_id: "task-003", book_slug: "thinking", session_id: "ghi", mode: "editing",   pid: 12355 },
  ]
  completed_sessions: [
    { task_id: "task-004", book_slug: "guns", session_id: "jkl", finished_at: "..." },
    ...
  ]
```

### 3.3 工作流程

#### 新书生产

```
做书前端提交: {task_id: "task-001", book_slug: "outliers", raw_text: "...(原始文本)...", target_pct: 12, book_name?: "异类", author?: "马尔科姆·格拉德威尔"}
  │
  ▼
Manager BookQueue 入队
  │
  ▼
TaskScheduler 查找有空闲生产槽位的 Worker:
  Worker 1: production_slots 1/1 ← 满
  Worker 2: production_slots 0/1 ← 空闲 ✓
  │
  ▼
通过 Worker 2 的 Runtime 执行:
  1. 将原始文本写入 Worker 本地文件
  2. Manager 发送 REGISTER_SYNC_MAPPING 到 Worker:
     {task_id, book_slug, oss_prefix, watch_paths, session_path_hash}
  3. 启动 Claude Code:
     claude -p "/audiobook /root/books/outliers/raw_text.md nonfiction_default target_pct=12" \
       --dangerously-skip-permissions --output-format stream-json
  │
  ▼
Worker Runtime 流式回传 Claude Code NDJSON 输出:
  → Manager EventBus
  → 外部 API → 做书前端 (实时聊天框)
  │
  ▼
同时: Worker Runtime 的 FileSyncManager 监听文件变更:
  → 通过 TaskSyncMapper 确定 OSS 目标路径
  → 新文件/修改 → 按防抖策略上传到 OSS
  → 上传完成 → FILE_SYNCED 事件 → Manager → 外部服务
  │
  ▼
Claude Code 输出 phase=9, state=DELIVERED, session_id=abc123
  → process_exit 事件
  │
  ▼
Manager:
  TaskRegistry 注册: {task-001 → worker-2, book_slug: outliers, session_id: abc123, status: idle}
  Worker 2 的 production_slots: 1/1 → 0/1 (释放生产槽位)
  通知前端: 做书完成
```

#### 后续修改

```
用户在前端进入 "outliers" 的聊天，发送: "请修改第三章的开头，换一种更吸引人的方式"
  │
  ▼
做书前端 → Audiobook Agent Service API:
  POST /api/tasks/{task_id}/chat
  { "message": "请修改第三章的开头，换一种更吸引人的方式" }
  │
  ▼
Manager TaskRouter:
  1. TaskRegistry 查找: task_id → worker-2, book_slug=outliers, session_id=abc123
  2. 检查 Worker 2 的 edit_slots: 2/3 → 有空位 ✓
  3. 通过 Worker 2 的 Runtime 执行:
     claude -p "请修改第三章的开头，换一种更吸引人的方式" \
       --resume abc123 \
       --dangerously-skip-permissions --output-format stream-json \
       --cwd /root/.work/outliers
  │
  ▼
Worker Runtime:
  → 占用一个修改槽位 (edit_slots: 3/3)
  → Claude Code 通过 --resume 恢复上下文
  → 执行修改 (读取 manuscript_final.md → Edit → 更新文件)
  → NDJSON 输出 → Manager → 前端 (修改过程的聊天流)
  → 文件变更 → OSS 同步 → 前端文件更新
  │
  ▼
修改完成 (process_exit):
  → 释放修改槽位 (edit_slots: 2/3)
  → 更新 session_id (Claude Code 每次 resume 可能产生新 session_id)
  → TaskRegistry 更新
  → 前端显示修改结果
```

#### 槽位满时的行为

```
场景: Worker 2 的修改槽位已满 (3/3)，用户对 Worker 2 上的另一本书发修改请求

Manager TaskRouter:
  1. 查找 session → Worker 2
  2. 检查 edit_slots: 3/3 → 满
  3. 返回 429: "该 Worker 修改槽位已满，请稍后重试"
  4. 前端显示排队提示

不会跨 Worker 路由（MVP）:
  session 文件绑定在特定 Worker 上，MVP 阶段不支持跨 Worker 迁移（见 3.4 节说明）
```

### 3.4 备份范围与云存储结构

#### 需要备份的完整范围

对于一个可能被销毁的 Worker，需要备份到 OSS/S3 的内容：

| 内容 | 路径 | 说明 | 不备份的后果 |
|------|------|------|-------------|
| **工作目录** | `.work/{book_slug}/` | 全部中间产物和最终讲稿 | 做书成果全部丢失 |
| **Session 文件** | `~/.claude/projects/{path_hash}/*.jsonl` | Claude Code 对话历史 | 无法 `--resume`，丧失修改能力 |
| **项目配置** | `~/.claude/projects/{path_hash}/.claude.json` | Claude Code 项目级设置 | `--resume` 时可能行为异常 |
| **源文本** | `/root/books/{slug}/raw_text.md` | 书籍原始文本（外部服务提供） | 新 Worker 恢复时无源文件 |

**不需要备份的**（Bootstrap 可重建）：Claude Code 二进制、audiobook-nonfiction 插件、Python 依赖、Node.js、系统配置。**凭证**由 CredentialPool 管理，不通过 OSS 备份。

#### 每本书独立的 OSS 目录结构

```
oss://audiobook-production/
├── tasks/
│   ├── {task_id}/                          # 每个任务一个独立目录（task_id 由外部服务提供）
│   │   ├── source/
│   │   │   ├── raw_text.md                 # 原始文本（提交时即存一份）
│   │   │   └── metadata.json               # 书名、作者等元数据（可选字段，后续扩展）
│   │   ├── workspace/                      # .work/{book_slug}/ 的镜像
│   │   │   ├── state.json
│   │   │   ├── compressed.md
│   │   │   ├── blueprint.md
│   │   │   ├── manuscript_final.md
│   │   │   ├── delivery/
│   │   │   │   ├── manuscript.md
│   │   │   │   └── intro.md
│   │   │   └── ...
│   │   ├── session/                        # Claude Code session 文件
│   │   │   ├── session.jsonl               # 主对话历史
│   │   │   └── .claude.json                # 项目配置
│   │   ├── logs/
│   │   │   ├── production.ndjson            # 生产过程完整 NDJSON 日志（Worker Runtime 自动写入）
│   │   │   └── edits/
│   │   │       └── {edit_run_id}.ndjson     # 修改过程 NDJSON 日志
│   │   └── _sync_manifest.json             # 同步元数据（见下方）
│   ├── {task_id_2}/
│   │   └── ...
│   └── {task_id_3}/
│       └── ...
```

**关键设计：以 task_id 为 key，不以 worker_id 或 book_slug 为 key。** task_id 由外部做书服务提供，是全局唯一的任务标识。同一本书可以有多个 task_id（例如不同参数的重试）。原因：一个任务的 session 可能因为 Worker 故障而迁移到新 Worker，如果按 worker_id 组织，迁移后路径就变了；而 book_slug 是插件内部概念，不适合作为外部 API 的主键。

#### 同步机制与 TaskSyncMapper

**动态映射架构：**

一台 Worker 上可能同时有多本书（1 个生产 + 多个修改），每本书的 `book_slug -> task_id -> OSS prefix` 映射不同。因此文件同步不能使用静态配置，需要 **动态映射**。

```
Manager 侧 (Audiobook Agent Service):
  新任务分配到 Worker 时:
    1. TaskRegistry 注册 task_id → (worker_id, book_slug)
    2. 调用 Harness.get_task_sync_mapping(task_context) 获取映射规则
    3. 发送 REGISTER_SYNC_MAPPING 消息到 Worker:
       {task_id, book_slug, oss_prefix, mappings, session_path_hash}

Worker 侧 (TaskSyncMapper 组件, 运行在 Worker Runtime 内部):
  维护映射表:
    /root/.work/outliers/        → oss://{bucket}/{prefix}/tasks/123/workspace/
    ~/.claude/projects/abc123/   → oss://{bucket}/{prefix}/tasks/123/session/

  FileSyncManager 使用此映射:
    文件变更 /root/.work/outliers/state.json
      → TaskSyncMapper 匹配: /root/.work/outliers/ → tasks/123/workspace/
      → 上传到: {oss_prefix}/tasks/123/workspace/state.json

  任务完成时:
    Manager 发送 UNREGISTER_SYNC_MAPPING {task_id}
    → Worker 移除映射（但保留已完成任务的目录，供 --resume 使用）
    → 停止该任务的活跃文件同步（已上传的保留）
```

**防抖策略：**

```
inotify 监听 → 文件变更 → 按优先级分层防抖:
  关键文件 (state.json):           0.5s 后上传
  中等文件 (manuscript_*, audit_*): 2s 防抖后上传
  大文件 (raw_text.md, compressed): 5s 防抖后上传

上传方式:
  OSS PutObject / S3 PutObject — 单文件原子上传
  上传完成后更新 _sync_manifest.json
```

**_sync_manifest.json 格式（统一使用 array 格式）：**

```json
{
  "task_id": "123",
  "worker_id": "aliyun:i-bp1xxx",
  "status": "syncing",
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
    },
    {
      "path": "workspace/manuscript_final.md",
      "oss_key": "elastic-agent/tasks/123/workspace/manuscript_final.md",
      "size": 58201,
      "md5": "def456789abc",
      "content_type": "text/markdown",
      "role": "manuscript_final",
      "synced_at": "2026-05-18T11:45:02Z"
    },
    {
      "path": "session/session.jsonl",
      "oss_key": "elastic-agent/tasks/123/session/session.jsonl",
      "size": 203456,
      "md5": "ghi789abcdef",
      "content_type": "application/x-ndjson",
      "role": "session",
      "synced_at": "2026-05-18T11:45:03Z"
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
| files[].role | string | 否 | 语义标签 |
| files[].synced_at | string | 是 | 该文件最后同步时间 |

**通知通道（与同步独立）：**

```
文件变更同时触发 FILE_SYNCED 事件 → Manager → 外部服务（Webhook）
用途: 前端收到通知后刷新文件列表（知道有新文件了）
文件内容从 OSS 下载
```

#### 内容查询：统一从云存储读取

**所有文件内容查询都从 OSS/S3 读取，不走 Worker。** 理由：

| 从 Worker 读 | 从云存储读 |
|-------------|-----------|
| Worker 离线 = 读不到 | 永远可用 |
| 增加 Worker 负担 | Worker 零开销 |
| 需要知道 book->worker 映射 | 只需要 task_id |
| Worker Runtime WS 通道拥挤 | CDN 加速，大文件友好 |

**新鲜度保证机制：**

从 OSS 读文件需要回答两个问题：**读到的是完整的吗？** 和 **读到的是最新的吗？**

```
问题 1：完整性 — OSS PutObject 是原子操作
  文件要么不存在（未上传），要么是完整的（上传完成）
  不存在"读到上传了一半的文件"的情况
  → 只要文件在 OSS 上存在，内容就是完整的

问题 2：最新性 — 通过事件通知 + 同步清单解决

  时间线:
    T=0s   Claude Code 写入 manuscript_final.md (Worker 本地)
    T=0s   inotify 触发，FileSyncManager 启动防抖计时器 (2s)
    T=2s   防抖到期，开始 PutObject 上传
    T=3s   上传完成，更新 _sync_manifest.json
    T=3s   发送 FILE_SYNCED 事件 → Manager → Webhook 通知外部服务

  在 T=0~3s 之间查询 OSS → 拿到的是旧版本（或文件不存在）
  在 T=3s 之后查询 OSS → 拿到的是最新版本

  外部服务如何知道"现在 OSS 上是最新的"？

  方式 A（推荐）：订阅 FILE_SYNCED Webhook
    注册 Webhook → 收到 task.file.synced 事件
    事件包含 {task_id, path, synced_at} → 此时去 OSS 读该文件，保证是最新的
    适用于: 前端实时展示（收到通知才刷新 UI）

  方式 B：直接查询 + 接受延迟
    GET /api/tasks/{task_id}/files/{path}
    响应包含 synced_at 时间戳 → 调用者知道这个版本是什么时候同步的
    最大延迟 = 防抖窗口(0.5~5s) + 上传时间(~1s) ≈ 1.5~6s
    适用于: 一次性查询（不需要精确到秒的最新性）

  方式 C：强制刷新
    GET /api/tasks/{task_id}/files/{path}?force_sync=true
    → Manager 通知 Worker 立即上传该文件（跳过防抖）→ 等上传完成 → 返回
    延迟增加 ~1-3s，但保证返回的是 Worker 上此刻的最新内容
    适用于: 需要确认最新的关键操作（如审核确认）
```

**_sync_manifest.json 的作用：**

```
每次同步批次完成后更新（先上传文件，最后上传 manifest）:

用途:
  - 列出某个 task 的全部可用文件: 读 manifest 即可，不需要 ListObjects
  - 判断文件是否存在: manifest 里有就存在
  - 判断文件新鲜度: 对比 synced_at 与当前时间
  - 崩溃恢复时: 根据 manifest 恢复完整的文件集
```

#### Session 备份与跨 Worker 迁移

Session 文件备份到 OSS 后，理论上可以在另一台 Worker 上恢复并 `--resume`。

> **MVP 不做跨 Worker Session 迁移。** Session 绑定创建它的 Worker，Worker 离线则该 session 不可用。
>
> 理由：
> 1. Claude Code session .jsonl 可能包含绝对路径引用，跨机器需要路径修补，可靠性未验证
> 2. MVP 阶段 Worker 是手动管理的常驻实例，离线是低频异常事件
> 3. 迁移涉及下载+上传+路径校验+TaskRegistry 更新，链路复杂度高
>
> 数据基础已具备（session 已备份到 OSS），后续 Phase 需要时可启用迁移功能。

### 3.5 Chat 双向中继

```
做书前端 ←→ Audiobook Agent Service ←→ Worker Claude Code

上行 (Claude Code → 前端):
  Claude Code stdout (stream-json NDJSON)
    → Worker Runtime 逐行读取，双写:
        1. LOG 事件 via WS → Manager EventBus（内部监控）
        2. 本地 NDJSON 日志文件（持久化，用于历史查询和排障）
           生产模式 → logs/production.ndjson
           修改模式 → logs/edits/{edit_run_id}.ndjson
    → 日志文件随 FileSyncManager 同步到 OSS
    → 同步完成触发 FILE_SYNCED → 前端收到通知后轮询 chat/live 拉取新内容

  chat/live 和 chat/history 均从 OSS 上持久化的 logs/*.ndjson 文件读取，
  而非内存中的 trace buffer，因此即使 Manager 重启也不丢失历史记录。

下行 (前端 → Claude Code):
  目前 Claude Code -p (prompt mode) 是单次输入
  修改模式下: 每次修改请求启动一个新的 --resume 进程
  → 用户输入作为 -p 参数传入
  → 不是"在运行中的进程里注入新输入"，而是"启动新进程恢复上下文"

  这意味着:
    生产模式: 不接受中途输入（10 Phase 自动跑完）
    修改模式: 每条用户指令 = 一次 --resume 调用 = 一个短生命进程

  合规决策 M6 的特殊处理:
    Phase 8 需要用户选择（合规版 vs 原始版）
    → state.json 中 state="NEEDS_HUMAN"
    → 前端检测后显示选择 UI
    → 用户选择后写入 .work/{slug}/user_decision.json
    → /continue-book 读取该文件继续
```

### 3.6 外部服务 API 完整设计

外部服务（做书前后端）通过以下 API 与 Audiobook Agent Service 交互。按功能域分组：

#### 做书生命周期

| 端点 | 方法 | 说明 |
|------|------|------|
| `/api/tasks/produce` | `POST` | 提交一本或多本做书请求（入队），请求体包含原始文本 + 可选元数据 |
| `/api/tasks/{task_id}/retry` | `POST` | 重试：`{from_phase: 3}` 从指定 Phase 重新开始，或 `{from_phase: 0}` 全部重来 |
| `/api/tasks/{task_id}/continue` | `POST` | 断点续跑失败的任务（等价于 /continue-book） |
| `/api/tasks/{task_id}/cancel` | `POST` | 取消正在进行的做书（发 SIGINT 给 Claude Code） |
| `/api/tasks/{task_id}/status` | `GET` | 返回当前 Phase、state、进度百分比、Worker ID |
| `/api/tasks/queue` | `GET` | 查看做书队列（排队中 + 进行中 + 已完成 + 失败） |

**重试的设计要点：**

```
POST /api/tasks/{task_id}/retry
  Body: { "from_phase": 3 }

处理流程（全部由 Audiobook Agent Service 编排）:
  1. 检查 task 当前状态（是否允许 retry）
  2. 如果有活跃进程 → 先停止
  3. 确定目标 Worker:
     a. 优先使用原 Worker（session 文件在本地）
     b. 原 Worker 不可用 → 分配新 Worker → 从 OSS 恢复 workspace
  4. 通过 Worker Runtime 执行清理:
     删除 Phase N 及之后的产物文件（sections/, drafts/, styled/, manuscript_*, ...）
     修改 state.json: phase=N, state 回退到对应状态
  5. 启动 Claude Code: /continue-book {slug} → 从 Phase N 开始重跑

  from_phase=0 (全部重来):
    清空整个 workspace + session → 从 OSS 取源文本 → 重新 /audiobook
```

#### Chat / 修改

| 端点 | 方法 | 说明 |
|------|------|------|
| `/api/tasks/{task_id}/chat` | `POST` | 发送修改指令，路由到 session 所在 Worker |
| `/api/tasks/{task_id}/chat/live` | `GET` | 实时聊天轮询（从 OSS logs 增量读取，offset 分页） |
| `/api/tasks/{task_id}/chat/history` | `GET` | 获取历史聊天记录（从 OSS 的 logs/*.ndjson 解析，按 parsed.type 过滤 assistant/result 消息） |

**Chat live 轮询的统一设计：**

```
GET /api/tasks/{task_id}/chat/live?offset={byte_offset}

  从 OSS 的 logs/production.ndjson 增量读取:
    - 如果正在生产 → 返回 production.ndjson 的新行
    - 如果正在修改 → 返回 edits/{edit_run_id}.ndjson 的新行
    - 如果空闲 → 返回空（next_offset 不变）

  前端每 2-3 秒轮询。收到 task.file.synced webhook 后可立即轮询。
  路由由 Manager 内部完成: task_id → 确定当前活跃的日志文件 → 从 OSS 读取
```

#### 内容查询（统一从 OSS 读取）

| 端点 | 方法 | 说明 |
|------|------|------|
| `/api/tasks/{task_id}/files` | `GET` | 列出该书的全部文件（从 _sync_manifest.json） |
| `/api/tasks/{task_id}/files/{path}` | `GET` | 读取指定文件内容（从 OSS 代理或返回预签名 URL） |
| `/api/tasks/{task_id}/files/{path}/url` | `GET` | 返回 OSS 预签名 URL（大文件直接下载） |
| `/api/tasks/{task_id}/state` | `GET` | 快捷方式：读取 state.json（等价于 files/workspace/state.json） |
| `/api/tasks/{task_id}/manuscript` | `GET` | 快捷方式：读取最终讲稿（自动选择 compliant 或 final 版本） |
| `/api/tasks/{task_id}/export` | `GET` | 打包下载：delivery/ 目录 + intro + state.json -> zip |

**所有内容都从 OSS 读取**，响应包含同步时间信息：

```json
{
  "content": "...",
  "synced_at": "2026-05-17T14:30:00Z",
  "sync_lag_max_seconds": 5
}
```

#### 文件变更通知

文件变更通过 Webhook（`task.file.synced`）通知外部服务，不再提供独立的 WebSocket 端点。

```
Webhook 事件格式 (task.file.synced):
  {
    "event": "task.file.synced",
    "task_id": "...",
    "path": "workspace/manuscript_final.md",
    "size": 58201,
    "synced_at": "2026-05-17T14:30:05Z"
  }

前端收到 Webhook 后:
  → 刷新文件列表 UI
  → 如果是 state.json 变更 → 更新 Phase 进度条
  → 如果是 manuscript_* → 可选自动刷新讲稿预览
  → 如果是 logs/*.ndjson → 触发 chat/live 轮询拉取新聊天内容
```

#### Worker 管理

| 端点 | 方法 | 说明 |
|------|------|------|
| `/api/workers` | `GET` | 列出所有 Worker 及槽位状态 |
| `/api/workers/scale-out` | `POST` | 手动扩容 `{count: 1}` |
| `/api/workers/{id}` | `DELETE` | 手动缩容（需无活跃会话） |
| `/api/workers/{id}/sessions` | `GET` | 列出该 Worker 上的所有 session |

#### 事件通知（Webhook）

| 端点 | 方法 | 说明 |
|------|------|------|
| `/api/webhooks` | `POST` | 注册 Webhook URL |
| `/api/webhooks` | `GET` | 列出已注册的 Webhook |
| `/api/webhooks/{id}` | `DELETE` | 删除 Webhook |

```
Webhook 事件类型:
  task.production.started    做书开始（分配到 Worker）
  task.production.phase      Phase 切换（附带 phase 编号）
  task.production.completed  做书完成（附带 delivery 路径）
  task.production.failed     做书失败（附带 failure type + report 路径）
  task.edit.completed        修改完成
  worker.unhealthy           Worker 异常
  worker.added               新 Worker 上线
```

**为什么需要 Webhook？** 后端服务（如通知系统、计费系统、批量管理）需要异步事件驱动，轮询不合适。前端也依赖 Webhook 通知来触发 chat/live 轮询和文件刷新。

#### 全局状态

| 端点 | 方法 | 说明 |
|------|------|------|
| `/api/dashboard` | `GET` | 总览：队列长度、进行中/已完成/失败数、Worker 数、总成本 |
| `/api/tasks` | `GET` | 所有任务的列表 + 状态摘要（支持分页、过滤） |

#### API 设计原则

1. **以 task_id 为主键**，不暴露 worker_id、node_id 和 book_slug 给外部。路由是 Manager 内部事务。
2. **读操作走 OSS**，写操作（做书/修改/重试）走 Worker。
3. **实时聊天用 chat/live 轮询**（数据走 OSS），查询用 REST，异步通知用 Webhook。
4. **快捷方式 API**（`/state`、`/manuscript`）减少外部服务理解内部文件结构的负担。

---

## 4. Harness 接口实现

### 4.1 AudiobookHarness 定义

```python
class AudiobookHarness(Harness):
    """有声书稿生产系统的 Elastic-Agent Harness"""

    def __init__(self, config: dict):
        self.config = config
        self.book_queue = BookQueue()
        # TaskRegistry/TaskScheduler/TaskRouter/WebhookEmitter 由框架自动创建
        # 通过 self.manager.task_registry 等访问

    def get_worker_lifecycle(self) -> WorkerLifecycle:
        return WorkerLifecycle.PERSISTENT  # 常驻，手动开启/关闭

    def get_worker_capacity(self) -> AudiobookWorkerCapacity:
        return AudiobookWorkerCapacity(
            max_production_slots=self.config.get("max_production_slots", 1),
            max_edit_slots=self.config.get("max_edit_slots", 3),
        )

    def get_bootstrap_steps(self) -> list[BootstrapStep]:
        return [
            InstallNodeJSStep(),
            InstallClaudeCodeStep(),
            InjectCredentialStep(),
            InstallAudiobookPluginStep(),
            StartWorkerRuntimeStep(),
            # 注意: 不在 Bootstrap 中启动 Claude Code 会话
            # 会话由 BookQueue 调度后按需启动
            # 原始文本在做书请求到达时写入 Worker，不在 Bootstrap 中处理
        ]

    def get_file_sync_config(self) -> FileSyncConfig:
        """返回文件同步模板配置。
        注意: 这里只返回通用的监听路径和防抖策略，
        不包含 {task_id}/{book_slug} 等动态占位符。
        实际的 task_id → OSS prefix 映射由 Manager 通过
        REGISTER_SYNC_MAPPING 推送到 Worker 的 TaskSyncMapper。
        """
        return FileSyncConfig(
            watch_base_paths=[
                "/root/.work/",          # workspace + logs/ 都在此目录下
                "~/.claude/projects/",   # session 文件
            ],
            debounce_tiers={
                "state.json": 0.5,       # 关键文件 — 几乎实时
                "manuscript_*": 2,        # 讲稿 — 2s 防抖
                "audit_*": 2,             # 审核报告 — 2s 防抖
                "*": 5,                   # 其他 — 5s 防抖
            },
            write_manifest=True,          # 每次同步后更新 _sync_manifest.json
        )

    def get_task_sync_mapping(self, task_context: dict) -> SyncMapping:
        """Per-task 动态映射，Manager 在分配任务到 Worker 时调用。
        返回的映射规则会通过 REGISTER_SYNC_MAPPING 推送到 Worker。
        """
        return SyncMapping(
            task_id=task_context["task_id"],
            mappings=[
                {"/root/.work/{book_slug}/": "tasks/{task_id}/workspace/"},
                {"~/.claude/projects/{path_hash}/": "tasks/{task_id}/session/"},
            ],
            oss_prefix=task_context["oss_prefix"],
        )

    def get_event_handlers(self) -> dict:
        return {
            FrameworkEvent.NODE_READY: self._on_node_ready,
            FrameworkEvent.PROCESS_EXIT: self._on_process_exit,
            FrameworkEvent.WORKER_UNHEALTHY: self._on_worker_unhealthy,
        }

    async def _on_node_ready(self, data: dict):
        """Worker 就绪 — 开始消费做书队列"""
        worker_id = data["node_id"]
        await self._dispatch_pending_books(worker_id)

    async def _on_process_exit(self, data: dict):
        """Claude Code 进程退出 — 更新会话状态，释放槽位，立即同步 session"""
        task_id = data["task_id"]
        session_info = self.manager.task_registry.get_by_task(task_id)
        if not session_info:
            return

        # 提取 session_id（多源策略，见 §5.8）
        session_id = await self._extract_session_id(data, session_info)

        if session_info.mode == "producing":
            # 做书完成 → 注册会话 → 释放生产槽位 → 尝试消费队列
            session_info.status = "idle"
            session_info.session_id = session_id
            session_info.finished_at = datetime.utcnow()
            # 立即刷新 session 目录到 OSS（跳过防抖）
            await self._flush_session_to_oss(session_info)
            await self._dispatch_pending_books(session_info.worker_id)
        elif session_info.mode == "editing":
            # 修改完成 → 释放修改槽位
            session_info.status = "idle"
            session_info.session_id = session_id  # --resume 可能产生新 id
            await self._flush_session_to_oss(session_info)

    async def _dispatch_pending_books(self, worker_id: str):
        """尝试将队列中的书分发到有空闲生产槽位的 Worker"""
        worker_state = await self.get_worker_slot_state(worker_id)
        if worker_state.production_slots.used < worker_state.production_slots.max:
            book = self.book_queue.dequeue()
            if book:
                await self._start_production(worker_id, book)

    async def _start_production(self, worker_id: str, book: BookRequest):
        """在指定 Worker 上启动做书"""
        runtime = self.manager.get_runtime_client(worker_id)
        # 将原始文本写入 Worker（同时存一份到 OSS）
        text_dir = f"/root/books/{book.slug}"
        await runtime.execute(["mkdir", "-p", text_dir])
        await runtime.write_file(f"{text_dir}/raw_text.md", book.raw_text)
        if book.metadata:
            await runtime.write_file(f"{text_dir}/metadata.json", json.dumps(book.metadata))
        await self._sync_source_to_oss(book)

        # 注册同步映射到 Worker
        sync_mapping = self.get_task_sync_mapping({
            "task_id": book.task_id,
            "book_slug": book.slug,
            "oss_prefix": book.oss_prefix,
        })
        await runtime.send_message("REGISTER_SYNC_MAPPING", sync_mapping.to_dict())

        # 启动 Claude Code（使用生产凭证）
        task_id = await runtime.execute(
            command=["claude", "-p",
                f"/audiobook {text_dir}/raw_text.md {book.persona} target_pct={book.target_pct}",
                "--dangerously-skip-permissions", "--output-format", "stream-json"],
            cwd="/root",
            env={"CLAUDE_CONFIG_DIR": "/root/.claude-prod/"},
        )
        # 注册会话
        self.manager.task_registry.register(
            book.task_id, worker_id, task_id,
            book_slug=book.slug, mode="producing"
        )

    async def handle_edit_request(self, task_id: str, message: str):
        """处理修改请求 — 路由到正确 Worker 的 --resume"""
        session = self.manager.task_registry.get(task_id)
        if not session:
            raise NotFoundError(f"Session for {task_id} not found")

        worker_state = await self.get_worker_slot_state(session.worker_id)
        if worker_state.edit_slots.used >= worker_state.edit_slots.max:
            raise CapacityError(f"Worker {session.worker_id} edit slots full")

        # 分配修改凭证
        edit_credential = self.credential_pool.allocate_edit_credential(session.worker_id)

        runtime = self.manager.get_runtime_client(session.worker_id)
        task_id = await runtime.execute(
            command=["claude", "-p", message,
                "--resume", session.session_id,
                "--dangerously-skip-permissions", "--output-format", "stream-json"],
            cwd=session.cwd,
            env={"CLAUDE_CONFIG_DIR": edit_credential.config_dir},
        )
        session.status = "editing"
        session.mode = "editing"
        return task_id
```

### 4.2 Manager 侧 API 扩展

```python
# Audiobook 特有的 API（挂载在 Manager FastAPI 上）

@app.post("/api/tasks/produce")
async def produce_book(request: ProduceBookRequest):
    """提交做书请求"""
    harness.book_queue.enqueue(BookRequest(
        task_id=request.task_id,             # 外部服务提供的任务 ID
        slug=request.book_slug,              # 插件内部使用的 slug
        raw_text=request.raw_text,           # 原始文本内容
        target_pct=request.target_pct,
        metadata=request.metadata,           # 可选: {book_name, author, ...}
    ))
    # 尝试立即分发（如果有空闲 Worker）
    for worker_id in registry.list_ready_workers():
        await harness._dispatch_pending_books(worker_id)
    return {"status": "queued", "task_id": request.task_id}

@app.post("/api/tasks/{task_id}/chat")
async def send_edit_message(task_id: str, request: ChatRequest):
    """向已完成的会话发送修改指令"""
    edit_task_id = await harness.handle_edit_request(task_id, request.message)
    return {"status": "started", "task_id": edit_task_id}

@app.get("/api/sessions")
async def list_sessions():
    """列出所有注册的会话"""
    return manager.task_registry.list_all()

@app.get("/api/workers")
async def list_workers():
    """列出所有 Worker 及其槽位状态"""
    workers = []
    for node in registry.list_all():
        slot_state = await harness.get_worker_slot_state(node["instance_id"])
        workers.append({**node, "slots": slot_state})
    return workers

@app.delete("/api/tasks/{task_id}/workspace")
async def cleanup_task_workspace(task_id: str):
    """手动清理 Worker 上的任务工作目录"""
    session = manager.task_registry.get(task_id)
    if not session:
        raise HTTPException(404, "Task not found")
    if session.status in ("producing", "editing"):
        raise HTTPException(409, "Task has active process, cannot cleanup")
    # 确认 OSS 备份完整
    manifest = await harness.verify_oss_backup(task_id)
    if not manifest:
        raise HTTPException(412, "OSS backup incomplete, cannot cleanup")
    # 清理 Worker 本地文件
    runtime = harness.manager.get_runtime_client(session.worker_id)
    await runtime.execute(["rm", "-rf", session.cwd])
    session.status = "archived"
    manager.task_registry.update(session)
    return {"status": "archived", "task_id": task_id}
```

---

## 5. 核心技术挑战与方案

### 5.1 Session 路由与 Hybrid --resume 策略

**挑战：** 修改请求必须路由到 session 所在的 Worker。session 文件存在 Worker 本地文件系统中，无法跨 Worker 访问。同时 `--resume` 可能因各种原因失败。

**Session 路由方案：**

```
TaskRegistry 是路由的核心:
  1. 做书完成时注册: task_id → (worker_id, session_id, book_slug)
  2. 修改请求到来时: 查 task_id → 得到 worker_id → 发到该 Worker
  3. session_id 更新: --resume 后 Claude Code 可能返回新 session_id → 更新注册表

路由失败场景:
  - Worker 不在线 → 返回错误"Worker 离线"（不自动迁移）
  - Worker 修改槽位满 → 返回"请稍后重试"
  - session 不存在 → 返回"会话未找到"

MVP 不做跨 Worker session 迁移（详见 3.4 节）:
  session 绑定 Worker，Worker 离线 = 该 session 不可用
  数据基础已备（session 备份到 OSS），后续可启用迁移
```

**Hybrid --resume 策略：**

`--resume` 可能因 session 文件损坏、Claude Code 版本更新等原因失败。因此采用 hybrid 策略：

```
修改请求到达:
  │
  ▼
Step 1: 尝试 --resume
  claude -p "{message}" --resume {session_id} --dangerously-skip-permissions ...
  │
  ├── 成功 → 使用 --resume（保留完整对话上下文）
  │
  └── 失败（进程退出码非 0 或超时 30s 无输出）
      │
      ▼
Step 2: 降级到 /continue-book + 新会话 + workspace 上下文
  claude -p "/continue-book {book_slug}\n\n然后执行以下修改:\n{message}" \
    --dangerously-skip-permissions --output-format stream-json \
    --cwd /root/.work/{book_slug}

  这种方式:
    ✓ 不依赖 session 文件
    ✓ /continue-book 从 state.json 恢复完整状态
    ✓ 所有工作文件在 cwd 中，Claude Code 可以直接读取
    ✗ 丧失之前的对话历史（无法引用"上次你说的那个..."）
    ✗ 需要重新加载文件上下文（token 消耗增加）
  │
  ▼
更新 TaskRegistry:
  新的 session_id 替换旧的（后续修改使用新 session）
```

### 5.2 从 Claude Code 输出中提取 session_id

**挑战：** 做书完成后需要拿到 `session_id` 用于后续 `--resume`。session_id 在 Claude Code 的 stream-json 输出的 `result` 事件中。

**方案：**

```
Claude Code stream-json 输出格式 (NDJSON, 逐行):
  {"type": "system", ...}
  {"type": "assistant", "content": "开始 Phase 1...", ...}
  {"type": "tool_use", ...}
  ...
  {"type": "result", "session_id": "abc123", "cost_usd": 2.34, ...}

Worker Runtime 在读取每行 stdout 时:
  1. 正常转发为 LOG 事件（给外部 API 消费）
  2. 如果行是 JSON 且 type="result" → 提取 session_id
  3. 在 PROCESS_EXIT 事件中附带 session_id

Manager 收到 PROCESS_EXIT:
  → 通过多源策略提取 session_id（见 §5.8）
  → 更新 TaskRegistry 中的 session_id
  → 释放槽位
```

### 5.3 文件查询一致性

**挑战：** 外部服务从 OSS 读取文件时，如何确保读到的是最新版本？

**方案：** 详见 3.4 节「内容查询：统一从云存储读取」中的新鲜度保证机制。核心要点：

- **完整性**由 OSS PutObject 原子性保证 — 能读到就是完整的
- **最新性**通过三种方式获取：
  - **事件驱动**（推荐）：订阅 FILE_SYNCED 事件，收到通知后读 OSS，保证是最新的
  - **直接查询**：接受最大 ~6s 延迟，响应包含 `synced_at` 标注版本时间
  - **强制刷新**：`?force_sync=true`，等 Worker 立即上传后返回，保证当前最新

### 5.4 并发控制与凭证隔离

**挑战：** 同一台 Worker 上可能有 1 个生产 + 3 个修改 = 4 个 Claude Code 进程同时运行。需要确保资源不冲突，尤其是凭证额度不互相干扰。

**方案：**

```
资源隔离:
  每个 Claude Code 进程:
    - 独立的 cwd (.work/{book_slug}/)
    - 独立的 session 文件
    - 独立的凭证目录（见下方凭证隔离方案）
    - 共享系统资源 (CPU/内存)

凭证隔离 (CredentialPool 按槽位分配):

  Worker 上的凭证目录布局:
    /root/.claude-prod/               # 生产凭证（主凭证，高额度 Claude Max 账号）
    /root/.claude-edit-1/             # 修改凭证 1
    /root/.claude-edit-2/             # 修改凭证 2
    /root/.claude-edit-3/             # 修改凭证 3

  每个目录包含独立的 .credentials.json

  Claude Code 进程启动时通过环境变量指定凭证:
    生产模式: CLAUDE_CONFIG_DIR=/root/.claude-prod/
    修改模式: CLAUDE_CONFIG_DIR=/root/.claude-edit-N/

  CredentialPool 分配逻辑:
    分配 Worker 时:
      → 分配 1 个高额度凭证作为 primary_credential → /root/.claude-prod/
      → 分配 N 个普通凭证作为 secondary_credentials → /root/.claude-edit-{1..N}/
    启动修改任务时:
      → 从 secondary_credentials 中选一个空闲的
      → 如果全部被占用 → 返回 429（不是因为槽位满，是因为凭证不够）

  内存/CPU:
    生产模式 (Opus 密集): ~2-4 GB 内存
    修改模式 (通常 Sonnet): ~1-2 GB 内存
    推荐 Worker 实例: ecs.c6.2xlarge (8C/16G) 或以上
    4 个并发时总内存 ~8-12 GB

Worker Runtime 的槽位管理:
  processes: dict[task_id, ProcessInfo]
  ProcessInfo:
    mode: "producing" | "editing"
    book_slug: str
    session_id: str | None
    pid: int
    credential_dir: str

  收到 EXECUTE 请求时:
    if mode == "producing" and production_count >= max_production_slots:
      → 拒绝 (返回 CAPACITY_FULL 错误)
    if mode == "editing" and edit_count >= max_edit_slots:
      → 拒绝

  进程退出时:
    → 释放对应槽位
    → 释放凭证回 CredentialPool
    → 上报 PROCESS_EXIT + session_id
```

### 5.5 Worker 手动管理

**挑战：** Worker 不自动创建/销毁，由运维手动 `scale_out()`。但 Worker 的 Bootstrap、凭证注入、健康检查仍需自动化。

**方案：**

```
手动扩容:
  运维 / 管理界面调用:
    POST /api/nodes/scale-out?count=1
  → Elastic-Agent 创建 ECS 实例 → Bootstrap → 注册

不自动销毁:
  Worker 上所有会话完成后 → 进入 idle 状态
  → 不触发 Drain / terminate
  → 保持运行，等待新的做书任务或修改请求
  → 只有运维手动调用 DELETE /api/nodes/{node_id} 才会销毁

手动缩容:
  运维调用: DELETE /api/nodes/{node_id}
  → 检查有无活跃会话 (producing/editing)
  → 如果有 → 拒绝 (返回 409 Conflict)
  → 如果没有 → 回收凭证 → 终止实例 → 注销所有 session
```

### 5.6 Worker 目录生命周期

**挑战：** Worker 常驻运行，不断接收新书生产任务。每本书在 `/root/.work/{book_slug}/` 创建工作目录，session 文件可能很大（数百 MB/本书），长期运行的 Worker 磁盘可能被历史数据填满。

**方案：**

```
磁盘使用监控:
  Worker Runtime 定期检查 (cron, 每 10 分钟):
    总使用率 > 80%: 上报 WARNING 到 Manager（Manager 可推送告警）
    总使用率 > 90%: 触发自动清理流程

自动清理策略:
  按 last_access_time（最后一次 --resume 或文件变更的时间）排序:
    最近 7 天内有活动的 → 保留
    超过 7 天未活动且 OSS 备份完整的 →
      1. 验证 _sync_manifest.json 完整性（对比本地文件 MD5 和 manifest 记录）
      2. 归档（压缩 + 上传）未同步的文件到 OSS
      3. 删除本地 workspace: rm -rf /root/.work/{book_slug}/
      4. 保留 session 目录元数据（用于后续可能的恢复）
      5. TaskRegistry 标记 status="archived"
    正在被 FileSyncManager 活跃同步的 → 不清理

手动清理 API:
  DELETE /api/tasks/{task_id}/workspace
    → 检查无活跃进程（producing/editing 状态不允许清理）
    → 确认 OSS 备份完整（对比 manifest）
    → 清理 Worker 本地文件
    → TaskRegistry 标记为 "archived"
    → 返回 {"status": "archived", "task_id": task_id}

Session 文件压缩:
  已完成的 session .jsonl 不需要随时读取:
    完成后 24 小时 → gzip 压缩本地副本（减少磁盘占用 ~70%）
    --resume 时 → 先检查是否压缩 → 如有则解压后使用
    OSS 上始终保留未压缩版本（便于直接读取和 Worker 恢复）
```

### 5.7 进度超时检测

**挑战：** Claude Code 进程可能存活但实际卡死（例如子 Agent 无限循环、等待不会到来的输入），L3 健康检查只检查"进程是否活着"，无法发现业务级卡住。

**方案：**

```
Audiobook Agent Service 维护每个 task 的 last_progress_time:

  更新时机:
    收到 LOG 事件 → 更新 last_progress_time
    收到 FILE_CHANGE 事件 → 更新 last_progress_time
    收到 FILE_SYNCED 事件 → 更新 last_progress_time

  定时检查 (每 5 分钟扫描所有 running 状态的 task):
    if now - last_progress_time > PROGRESS_TIMEOUT:
      → 标记任务为 "stalled"
      → 发送告警到运维（通过 Webhook 或内部通知）
      → 可选自动处理:
          1. 发送 SIGINT 给 Claude Code 进程
          2. 等待 30s 优雅退出
          3. 如果未退出 → SIGKILL
          4. PROCESS_EXIT → 标记 task 为 failed，error_type="progress_stalled"
          5. Webhook → audio_book_echo_editor: task.production.failed

  超时阈值 (可配置):
    PROGRESS_TIMEOUT = 30 * 60  # 默认 30 分钟
    生产模式下某些 Phase 可能合理无输出（如 Phase 4 Opus 写长文），
    但 30 分钟完全无任何 LOG 或 FILE_CHANGE 极不正常

  注意:
    这是 Audiobook Agent Service 的业务逻辑，不是框架级功能
    不同 Harness 的"卡住"定义不同，由各 Harness 自行实现
```

### 5.8 Session ID 多源提取

**挑战：** 做书完成后需要拿到 `session_id` 用于后续 `--resume`。但如果 Claude Code 进程 crash（SIGKILL），不会输出 result 行，session_id 丢失。

**方案（防御性多源提取）：**

```
session_id 提取优先级:

  Primary: 从 stream-json 的 result 事件提取（正常路径）
    Worker Runtime 在读取 stdout 时对每行 NDJSON 做结构化解析（parsed 字段）:
      如果 parsed.type="result" → 提取 parsed.session_id
      在 PROCESS_EXIT 事件中附带 session_id
    这是最可靠的提取路径，因为 result 事件是 Claude Code 的正常结束信号
    适用: Claude Code 正常退出的情况

  Secondary: 扫描 ~/.claude/projects/{path_hash}/ 目录
    Worker Runtime 在 PROCESS_EXIT 后:
      扫描 ~/.claude/projects/{path_hash}/ 下的 .jsonl 文件
      按 mtime 排序，取最新的文件名
      文件名格式通常包含 session identifier
    适用: Claude Code crash 但文件已写入的情况

  Tertiary: 从 state.json 读取（需要 audiobook 插件配合）
    audiobook 插件在完成时写入 state.json:
      {"session_id": "abc123", "phase": 9, "state": "DELIVERED", ...}
    Audiobook Agent Service 从 OSS 读取 state.json → 提取 session_id
    适用: stream-json 和文件扫描都失败，但 state.json 已同步到 OSS

  Fallback: 所有方式都失败
    → session_id = None
    → TaskRegistry 中标记 session_id_missing = true
    → 修改请求到来时 → 使用 hybrid 策略的 Step 2（/continue-book）

PROCESS_EXIT 时的关键动作:
  立即触发 session 目录同步到 OSS（跳过防抖）:
    Worker Runtime 收到 PROCESS_EXIT →
      FileSyncManager.flush(path="~/.claude/projects/{path_hash}/")
    → 即使 session_id 提取失败，session 文件已在 OSS，后续可手动恢复
```

---

## 6. 分步实施方案

> Audiobook Agent Service 的开发分为三个 Phase，依赖 Elastic-Agent 框架的 Phase A-C 完成基础能力。

### Phase 1：单 Worker 端到端

**目标：** 一台 Worker 上完成从提交到交付的完整做书流程。

1. 实现 `AudiobookHarness` 基础接口（bootstrap, lifecycle, capacity）
2. 实现 `BookQueue`（内存队列 + JSON 持久化）
3. 实现 `TaskRegistry`（内存 + JSON 持久化 + 启动恢复）
4. 实现 `TaskSyncMapper`（动态同步映射，REGISTER/UNREGISTER 消息）
5. 实现做书 API: `POST /api/tasks/produce`
6. 实现状态查询 API: `GET /api/tasks/{task_id}/status`
7. 手动 `scale_out(1)` 创建一台 Worker → 提交一本书 → 验证全流程
8. 验证实时聊天流（Claude Code -> Manager -> 外部 API）
9. 验证文件同步（.work/ -> OSS，通过 TaskSyncMapper 路由）
10. 做书完成后验证 session 注册和 session_id 提取

**依赖：** Elastic-Agent 框架 Phase A (云资源管理) + Phase B (Worker Runtime 通信)

### Phase 2：修改模式

**目标：** 对已完成的书发送修改请求，支持 --resume 恢复对话。

1. 实现修改 API: `POST /api/tasks/{task_id}/chat`（使用框架 TaskRouter 路由到正确 Worker）
3. 实现 hybrid --resume 策略（先 resume，失败降级到 /continue-book）
4. 实现凭证隔离（CredentialPool 按槽位分配，独立 CLAUDE_CONFIG_DIR）
5. 实现 session_id 多源提取（Primary/Secondary/Tertiary）
6. 实现进度超时检测（30 分钟无进展 -> stalled）
7. 验证并发修改（同时修改 2-3 本）
8. 验证 session_id 更新（--resume 后新 id 写入 TaskRegistry）

**依赖：** Phase 1 完成

### Phase 3：多 Worker + 队列 + Webhook

**目标：** 多台 Worker 并行做书，队列调度，Webhook 通知外部服务。

1. 配置框架 TaskScheduler（扩展 AudiobookWorkerCapacity 的生产/修改槽位逻辑）
2. 配置框架 WebhookEmitter（注册 audio_book_echo_editor 的回调 URL）
3. 实现 Worker 目录生命周期管理（磁盘监控、自动清理、手动清理 API）
4. 实现重试/续跑: `POST /api/tasks/{task_id}/retry`, `POST /api/tasks/{task_id}/continue`
5. 手动扩容到 3 台 Worker → 提交多本书 → 验证队列分发
6. 验证跨 Worker 的 session 路由（修改请求路由到正确 Worker）
7. 验证槽位满时的排队/拒绝行为
8. 验证 Webhook 投递（完成/失败/Phase 切换事件）
9. 集成 audio_book_echo_editor 前端

**依赖：** Phase 2 完成 + Elastic-Agent 框架 Phase C (文件同步 + External API)

---

## 7. Elastic-Agent 框架提供的能力

### 7.1 Audiobook 使用的框架能力

| 需求 | 说明 | 普适性 |
|------|------|--------|
| **多槽位并发模型** | 同一 Worker 上区分"生产槽位"和"修改槽位"，各自可配置上限 | 通用 — 任何需要混合重/轻任务的场景 |
| **Session 路由** | 修改请求路由到 session 所在的 Worker | 通用 — 有状态工作负载的亲和性路由 |
| **Session 持久化** | 做书完成后会话不销毁，支持随时 --resume | 通用 — 任何需要多轮交互的 Agent |
| **双向 Chat 中继** | 外部 -> Manager -> Worker -> Claude Code (--resume) | 通用 — 人工审批、交互式 Agent |
| **文件实时同步到云存储** | .work/ + session 文件 -> OSS/S3，分层防抖 + 同步清单 | 通用 — 需要外部实时查看 Agent 产物 |
| **从云存储统一读取** | 内容查询走 OSS 不走 Worker，附带一致性元数据 | 通用 — 解耦读路径和 Worker 生命周期 |
| **TaskSyncMapper / 动态同步映射** | Worker 上多任务 -> 多 OSS 路径的动态映射，由 Manager 推送 | 通用 — 任何单 Worker 多任务场景 |
| **FILE_SYNCED 事件类型** | 文件同步完成后的通知（含 oss_key、synced_at 等），外部服务统一使用此事件 | 通用 — 外部服务需要知道何时可从 OSS 读取 |
| **Harness 级状态持久化钩子** | TaskRegistry 等 Harness 数据的持久化支持 | 通用 — Manager 崩溃恢复 |
| **Per-task 凭证隔离支持** | EXECUTE 时指定 CLAUDE_CONFIG_DIR 环境变量 | 通用 — 多账号并发场景 |
| **Webhook 事件通知** | 做书完成/失败/Phase 切换 -> 推送到注册的 URL | 通用 — 后端系统异步事件驱动 |
| **任务重试/续跑** | 从指定 Phase 重新开始 或 断点续跑失败任务 | 通用 — 长时间任务的容错 |
| **常驻 Worker** | 手动扩容/缩容，不自动销毁 | 通用 — 稳定工作负载场景 |
| **文件写入到 Worker** | 运行时将原始文本等输入写入 Worker 文件系统 | 通用 — 任何需要输入数据的任务 |
| **Worker 进程日志落盘** | Worker Runtime 进程输出双写（LOG 事件 + 本地文件），持久化到 OSS，支持历史查询和排障 | 通用 — 所有 Harness 都需要历史日志 |
| **LOG 事件结构化解析** | 解析 Claude Code NDJSON 输出为 typed event（assistant/tool_use/result...），支持过滤和统计 | 通用 — 需要从输出中提取 session_id、cost 等 |
| **跨 Worker Session 迁移** | session + workspace 备份到 OSS 后可在新 Worker 恢复 | 通用 — **MVP 不做**，数据基础已备 |

### 7.2 与其他 Harness 的交叉验证

| 框架能力 | agent-ml-research | CCM | Audiobook | 结论 |
|---------|------------------|-----|-----------|------|
| Worker Runtime | 替换 SSH | 替换本地子进程 | 启动 Claude Code 会话 | **框架核心** |
| 外部 API（轨迹） | 飞书消费 | 前端日志 | 做书聊天流 | **框架核心** |
| 外部 API（文件） | 研究产物 | 项目文件 | 讲稿 + 审核报告 + OSS 同步 | **框架核心** |
| 有状态亲和性 | 项目绑定 | session resume | **session 路由到 Worker** | **框架核心** |
| 多槽位并发 | — | max_concurrent=5 | **生产1 + 修改3** | **框架核心** |
| 优雅缩容 | 长时间训练 | 30min 任务 | 手动缩容需检查活跃会话 | **框架核心** |
| 双层凭证 | WandB/HF | Git key | Claude 账号 | **框架核心** |
| 双向 Chat | 飞书指令 | Plan 审批 | **修改指令 + 合规决策** | **框架核心** |
| 文件同步到 OSS/S3 | — | — | **.work/ + session 实时同步** | **新增** |
| TaskSyncMapper 动态映射 | — | — | **多书多任务路径映射** | **新增** |
| FILE_SYNCED 事件 | — | — | **文件同步完成通知** | **新增** |
| 云存储统一读取 | — | — | **内容查询走 OSS** | **新增** |
| Webhook 通知 | 飞书 | — | **完成/失败/Phase 通知** | **新增** |
| 任务重试/续跑 | — | 重试 | **from_phase + continue** | **新增** |
| Per-task 凭证隔离 | — | — | **CLAUDE_CONFIG_DIR 环境变量** | **新增** |
| 跨 Worker 迁移 | — | — | 数据基础已备，**MVP 不做** | 后续 |
| 常驻 Worker | 已有 | 已有 | 已有 | 已有 |

### 7.3 成本估算

| 资源 | 单价 | 说明 |
|------|------|------|
| 阿里云 ecs.c6.2xlarge (8C/16G) | ￥1.56/h On-Demand | 支持 1 生产 + 3 修改并发 |
| Claude Max 订阅 | 已有 | 30-80M token/本新书 |
| OSS 存储 | ￥0.12/GB/月 | 每本书 ~100-200MB 工作文件 |
| OSS 请求 | ￥0.01/万次 | 文件同步 API 调用 |

常驻 Worker 的月成本: ￥1.56 * 24 * 30 = **￥1,123/台/月** (On-Demand)。如果夜间不需要可以手动 stop 降成本。
