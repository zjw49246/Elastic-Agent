# Harness 应用示例：有声书稿全自动化生产系统接入 Elastic-Agent

> 本文档以有声书稿全自动化生产系统（以下简称 Audiobook）为例，说明一个 **Claude Code 插件** 如何作为 Harness 接入 Elastic-Agent 弹性计算框架，实现多书并行生产、会话持久化、后续修改路由。
>
> 本文档基于 audiobook-nonfiction v1.1.1 的真实代码分析。

---

## 目录

1. [Audiobook 项目真实架构解析](#1-audiobook-项目真实架构解析)
2. [整体业务流程](#2-整体业务流程)
3. [基于框架的分布式架构设计](#3-基于框架的分布式架构设计)
4. [Harness 接口实现](#4-harness-接口实现)
5. [核心技术挑战与方案](#5-核心技术挑战与方案)
6. [分步实施方案](#6-分步实施方案)
7. [Audiobook 对框架提出的需求](#7-audiobook-对框架提出的需求)

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
| 1 | 书籍解构 | text-compressor(Sonnet×N), book-facts-checker | `compressed.md`, `book_meta.json` | 10-20min |
| 2 | 战略蓝图 | narration-framework-designer(**Opus**) | `blueprint.md`, `quality_targets.json` | 10-15min |
| 3 | 源文切片 | anchor-fixer(如需) | `sections/section_*.txt` | 2-5min |
| 4 | 主体生产 | draft-writer(**Opus**×N) | `drafts/draft_*.md` | 20-40min |
| 5 | 人格融合 | persona-fusion(Sonnet×N) | `styled/styled_*.md` | 10-20min |
| 6 | 开头结尾 | opening-closing-editor(**Opus**) | `manuscript_v1.md` | 5-10min |
| 7 | 审核循环 | 7 auditors(Sonnet×7并行) + fixer(**Opus**), 最多 3 轮 | `manuscript_final.md` | 15-30min |
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
Elastic-Agent 将请求入队 → 分发到有空闲生产槽位的 Worker
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
  - 修改请求 → Elastic-Agent 路由到对应 Worker → --resume 恢复会话
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
                            │ HTTPS + WebSocket
                            ▼
┌──────────────────────────────────────────────────────────────────────┐
│                       Elastic-Agent Manager                          │
│                                                                      │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │                    Audiobook Harness 层                        │  │
│  │                                                                │  │
│  │  BookQueue        SessionRegistry      ChatRelay              │  │
│  │  做书请求队列      session→worker 映射   双向消息中继            │  │
│  │                                                                │  │
│  │  SlotScheduler                                                │  │
│  │  槽位调度:                                                     │  │
│  │    生产请求 → 找有空生产槽位的 Worker                           │  │
│  │    修改请求 → 找 session 所在的 Worker + 检查修改槽位            │  │
│  └────────────────────────┬───────────────────────────────────────┘  │
│                           │                                          │
│  ┌────────────────────────▼───────────────────────────────────────┐  │
│  │                    Elastic-Agent 框架层                         │  │
│  │                                                                │  │
│  │  CloudProvider    NodeRegistry     ExternalAPI(轨迹/文件)      │  │
│  │  CredentialPool   HealthChecker    FileSyncManager             │  │
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

每个做书会话在 Elastic-Agent 中注册，用于路由修改请求到正确的 Worker。

```
SessionRegistry (Manager 侧):
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
SlotScheduler 查找有空闲生产槽位的 Worker:
  Worker 1: production_slots 1/1 ← 满
  Worker 2: production_slots 0/1 ← 空闲 ✓
  │
  ▼
通过 Worker 2 的 Runtime 执行:
  1. 将原始文本写入 Worker 本地文件
  2. 启动 Claude Code:
     claude -p "/audiobook /root/books/outliers/raw_text.md nonfiction_default target_pct=12" \
       --dangerously-skip-permissions --output-format stream-json
  │
  ▼
Worker Runtime 流式回传 Claude Code NDJSON 输出:
  → Manager EventBus
  → 外部 API → 做书前端 (实时聊天框)
  │
  ▼
同时: Worker Runtime 监听 .work/outliers/ 文件变更:
  → 新文件/修改 → 立即同步到 OSS/S3
  → 外部 API FILE_CHANGE 事件 → 前端文件目录刷新
  │
  ▼
Claude Code 输出 phase=9, state=DELIVERED, session_id=abc123
  → process_exit 事件
  │
  ▼
Manager:
  SessionRegistry 注册: {task-001 → worker-2, book_slug: outliers, session_id: abc123, status: idle}
  Worker 2 的 production_slots: 1/1 → 0/1 (释放生产槽位)
  通知前端: 做书完成
```

#### 后续修改

```
用户在前端进入 "outliers" 的聊天，发送: "请修改第三章的开头，换一种更吸引人的方式"
  │
  ▼
做书前端 → Elastic-Agent API:
  POST /api/tasks/{task_id}/chat
  { "message": "请修改第三章的开头，换一种更吸引人的方式" }
  │
  ▼
Manager ChatRelay:
  1. SessionRegistry 查找: task_id → worker-2, book_slug=outliers, session_id=abc123
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
  → SessionRegistry 更新
  → 前端显示修改结果
```

#### 槽位满时的行为

```
场景: Worker 2 的修改槽位已满 (3/3)，用户对 Worker 2 上的另一本书发修改请求

Manager ChatRelay:
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
| **Session 文件** | `~/.claude/projects/{path-hash}/*.jsonl` | Claude Code 对话历史 | 无法 `--resume`，丧失修改能力 |
| **项目配置** | `~/.claude/projects/{path-hash}/.claude.json` | Claude Code 项目级设置 | `--resume` 时可能行为异常 |
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
│   │   └── _sync_manifest.json             # 同步元数据（见下方）
│   ├── {task_id_2}/
│   │   └── ...
│   └── {task_id_3}/
│       └── ...
```

**关键设计：以 task_id 为 key，不以 worker_id 或 book_slug 为 key。** task_id 由外部做书服务提供，是全局唯一的任务标识。同一本书可以有多个 task_id（例如不同参数的重试）。原因：一个任务的 session 可能因为 Worker 故障而迁移到新 Worker，如果按 worker_id 组织，迁移后路径就变了；而 book_slug 是插件内部概念，不适合作为外部 API 的主键。

#### 同步机制

```
Worker Runtime 的 FileSyncManager:

  监听范围:
    /root/.work/{book_slug}/           → oss://bucket/tasks/{task_id}/workspace/
    ~/.claude/projects/{path-hash}/    → oss://bucket/tasks/{task_id}/session/
    /root/books/{slug}/raw_text.md     → oss://bucket/tasks/{task_id}/source/

  触发策略:
    inotify 监听 → 文件变更 → 按优先级分层防抖:
      关键文件 (state.json):           0.5s 后上传
      中等文件 (manuscript_*, audit_*): 2s 防抖后上传
      大文件 (raw_text.md, compressed): 5s 防抖后上传

  上传方式:
    OSS PutObject / S3 PutObject — 单文件原子上传
    上传完成后更新 _sync_manifest.json

  同步元数据 (_sync_manifest.json):
    {
      "last_sync_at": "2026-05-17T14:30:00Z",
      "worker_id": "worker-2",
      "files": {
        "workspace/state.json":        {"size": 1234, "md5": "abc...", "synced_at": "..."},
        "workspace/manuscript_final.md": {"size": 58201, "md5": "def...", "synced_at": "..."},
        "session/session.jsonl":       {"size": 203456, "md5": "ghi...", "synced_at": "..."},
        ...
      }
    }

  通知通道 (与同步独立):
    文件变更同时触发 FILE_CHANGE 事件 via WebSocket → Manager → 外部服务
    用途: 前端实时刷新文件列表（知道有新文件了）
    文件内容从 OSS 下载（不走 WebSocket）
```

#### 内容查询：统一从云存储读取

**所有文件内容查询都从 OSS/S3 读取，不走 Worker。** 理由：

| 从 Worker 读 | 从云存储读 |
|-------------|-----------|
| Worker 离线 = 读不到 | 永远可用 |
| 增加 Worker 负担 | Worker 零开销 |
| 需要知道 book→worker 映射 | 只需要 task_id |
| Worker Runtime WS 通道拥挤 | CDN 加速，大文件友好 |

**新鲜度保证机制：**

从 OSS 读文件需要回答两个问题：**读到的是完整的吗？** 和 **读到的是最新的吗？**

```
问题 1：完整性 — OSS PutObject 是原子操作
  文件要么不存在（未上传），要么是完整的（上传完成）
  不存在"读到上传了一半的文件"的情况
  → 只要文件在 OSS 上存在，内容就是完整的 ✓

问题 2：最新性 — 通过事件通知 + 同步清单解决

  时间线:
    T=0s   Claude Code 写入 manuscript_final.md (Worker 本地)
    T=0s   inotify 触发，FileSyncManager 启动防抖计时器 (2s)
    T=2s   防抖到期，开始 PutObject 上传
    T=3s   上传完成，更新 _sync_manifest.json
    T=3s   发送 FILE_SYNCED 事件 → Manager → 外部 WebSocket

  在 T=0~3s 之间查询 OSS → 拿到的是旧版本（或文件不存在）
  在 T=3s 之后查询 OSS → 拿到的是最新版本

  外部服务如何知道"现在 OSS 上是最新的"？

  方式 A（推荐）：订阅事件
    WS /api/tasks/{task_id}/files/watch → 收到 FILE_SYNCED 事件
    事件包含 {path, synced_at} → 此时去 OSS 读该文件，保证是最新的
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
{
  "last_sync_at": "2026-05-17T14:30:03Z",
  "worker_id": "aliyun:i-bp1xxx",
  "files": {
    "workspace/state.json":        {"md5": "abc...", "size": 1234, "synced_at": "14:30:01"},
    "workspace/manuscript_final.md": {"md5": "def...", "size": 58201, "synced_at": "14:30:03"},
    ...
  }
}

用途:
  - 列出某个 task 的全部可用文件: 读 manifest 即可，不需要 ListObjects
  - 判断文件是否存在: manifest 里有就存在
  - 判断文件新鲜度: 对比 synced_at 与当前时间
  - 崩溃恢复时: 根据 manifest 恢复完整的文件集
```

#### Session 备份与跨 Worker 迁移

Session 文件备份到 OSS 后，理论上可以在另一台 Worker 上恢复并 `--resume`。

> **⚠ MVP 不做跨 Worker Session 迁移。** Session 绑定创建它的 Worker，Worker 离线则该 session 不可用。
>
> 理由：
> 1. Claude Code session .jsonl 可能包含绝对路径引用，跨机器需要路径修补，可靠性未验证
> 2. MVP 阶段 Worker 是手动管理的常驻实例，离线是低频异常事件
> 3. 迁移涉及下载+上传+路径校验+SessionRegistry 更新，链路复杂度高
>
> 数据基础已具备（session 已备份到 OSS），后续 Phase 需要时可启用迁移功能。

### 3.5 Chat 双向中继

```
做书前端 ←→ Elastic-Agent Manager ←→ Worker Claude Code

上行 (Claude Code → 前端):
  Claude Code stdout (stream-json NDJSON)
    → Worker Runtime 逐行读取 → LOG 消息 via WS
    → Manager EventBus → 外部 API 轨迹流
    → 做书前端 WebSocket → 渲染为聊天气泡

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

外部服务（做书前后端）通过以下 API 与 Elastic-Agent 交互。按功能域分组：

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

处理流程:
  1. 从 OSS 下载 outliers 的 workspace（获取到 Phase 2 的产物）
  2. 删除 Phase 3 及之后的产物文件（sections/, drafts/, styled/, manuscript_*, ...）
  3. 修改 state.json: phase=3, state 回退到对应状态
  4. 上传修改后的 workspace 到空闲 Worker
  5. 启动 Claude Code: /continue-book outliers → 从 Phase 3 开始重跑

  from_phase=0 (全部重来):
    清空整个 workspace + session → 从 OSS 取源文本 → 重新 /audiobook
```

#### Chat / 修改

| 端点 | 方法 | 说明 |
|------|------|------|
| `/api/tasks/{task_id}/chat` | `POST` | 发送修改指令，路由到 session 所在 Worker |
| `/api/tasks/{task_id}/chat/stream` | `WS` | 订阅某本书的实时聊天流（生产 or 修改过程） |
| `/api/tasks/{task_id}/chat/history` | `GET` | 获取历史聊天记录（从 OSS 的 session .jsonl 解析） |

**Chat stream 的统一设计：**

```
WS /api/tasks/{task_id}/chat/stream

  连接后推送该书的所有 Claude Code 输出:
    - 如果正在生产 → 推送生产过程的 NDJSON
    - 如果正在修改 → 推送修改过程的 NDJSON
    - 如果空闲 → 保持连接，等下次操作时自动推送

  注意: 外部服务不需要知道 node_id 或 worker_id
  路由由 Manager 内部完成:
    task_id → SessionRegistry → worker_id → Worker Runtime WS → 转发
```

#### 内容查询（统一从 OSS 读取）

| 端点 | 方法 | 说明 |
|------|------|------|
| `/api/tasks/{task_id}/files` | `GET` | 列出该书的全部文件（从 _sync_manifest.json） |
| `/api/tasks/{task_id}/files/{path}` | `GET` | 读取指定文件内容（从 OSS 代理或返回预签名 URL） |
| `/api/tasks/{task_id}/files/{path}/url` | `GET` | 返回 OSS 预签名 URL（大文件直接下载） |
| `/api/tasks/{task_id}/state` | `GET` | 快捷方式：读取 state.json（等价于 files/workspace/state.json） |
| `/api/tasks/{task_id}/manuscript` | `GET` | 快捷方式：读取最终讲稿（自动选择 compliant 或 final 版本） |
| `/api/tasks/{task_id}/export` | `GET` | 打包下载：delivery/ 目录 + intro + state.json → zip |

**所有内容都从 OSS 读取**，响应包含同步时间信息：

```json
{
  "content": "...",
  "synced_at": "2026-05-17T14:30:00Z",
  "sync_lag_max_seconds": 5
}
```

#### 文件变更通知

| 端点 | 方法 | 说明 |
|------|------|------|
| `/api/tasks/{task_id}/files/watch` | `WS` | 订阅该书的文件变更事件 |

```
事件格式:
  {
    "event": "created" | "modified",
    "path": "workspace/manuscript_final.md",
    "size": 58201,
    "synced_at": "2026-05-17T14:30:05Z"
  }

前端收到后:
  → 刷新文件列表 UI
  → 如果是 state.json 变更 → 更新 Phase 进度条
  → 如果是 manuscript_* → 可选自动刷新讲稿预览
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

**为什么需要 Webhook？** 前端可以用 WebSocket 获取实时流，但后端服务（如通知系统、计费系统、批量管理）需要异步事件驱动，轮询不合适。

#### 全局状态

| 端点 | 方法 | 说明 |
|------|------|------|
| `/api/dashboard` | `GET` | 总览：队列长度、进行中/已完成/失败数、Worker 数、总成本 |
| `/api/tasks` | `GET` | 所有任务的列表 + 状态摘要（支持分页、过滤） |

#### API 设计原则

1. **以 task_id 为主键**，不暴露 worker_id、node_id 和 book_slug 给外部。路由是 Manager 内部事务。
2. **读操作走 OSS**，写操作（做书/修改/重试）走 Worker。
3. **实时流用 WebSocket**，查询用 REST，异步通知用 Webhook。三种模式覆盖所有消费场景。
4. **快捷方式 API**（`/state`、`/manuscript`）减少外部服务理解内部文件结构的负担。

---

## 4. Harness 接口实现

### 4.1 AudiobookHarness 定义

```python
class AudiobookHarness(Harness):
    """有声书稿生产系统的 Elastic-Agent Harness"""

    def __init__(self, config: dict):
        self.config = config
        self.session_registry = SessionRegistry()
        self.book_queue = BookQueue()

    def get_worker_lifecycle(self) -> WorkerLifecycle:
        return WorkerLifecycle.PERSISTENT  # 常驻，手动开启/关闭

    def get_worker_capacity(self) -> WorkerCapacity:
        return WorkerCapacity(
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
        return FileSyncConfig(
            watch_paths=[
                "/root/.work/",                          # 工作目录（所有书）
                "~/.claude/projects/",                   # Session 文件 + 项目配置
            ],
            sync_target="oss://audiobook-production/",
            sync_on_change=True,
            path_mapping={
                # Worker 本地路径 → OSS 路径的映射规则
                # {task_id} 由 FileSyncManager 通过 SessionRegistry 映射
                "/root/.work/{book_slug}/":          "tasks/{task_id}/workspace/",
                "~/.claude/projects/{path_hash}/":   "tasks/{task_id}/session/",
            },
            debounce_tiers={
                "state.json": 0.5,                   # 关键文件 — 几乎实时
                "manuscript_*": 2,                   # 讲稿 — 2s 防抖
                "audit_*": 2,                        # 审核报告 — 2s 防抖
                "*": 5,                              # 其他 — 5s 防抖
            },
            write_manifest=True,                     # 每次同步后更新 _sync_manifest.json
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
        """Claude Code 进程退出 — 更新会话状态，释放槽位"""
        task_id = data["task_id"]
        session_info = self.session_registry.get_by_task(task_id)
        if not session_info:
            return

        if session_info.mode == "producing":
            # 做书完成 → 注册会话 → 释放生产槽位 → 尝试消费队列
            session_info.status = "idle"
            session_info.session_id = data.get("session_id")  # 从输出中提取
            session_info.finished_at = datetime.utcnow()
            await self._dispatch_pending_books(session_info.worker_id)
        elif session_info.mode == "editing":
            # 修改完成 → 释放修改槽位
            session_info.status = "idle"
            # 更新 session_id（--resume 可能产生新 id）

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
        # 启动 Claude Code
        task_id = await runtime.execute(
            command=["claude", "-p",
                f"/audiobook {text_dir}/raw_text.md {book.persona} target_pct={book.target_pct}",
                "--dangerously-skip-permissions", "--output-format", "stream-json"],
            cwd="/root",
        )
        # 注册会话
        self.session_registry.register(book.task_id, worker_id, task_id, book_slug=book.slug, mode="producing")

    async def handle_edit_request(self, task_id: str, message: str):
        """处理修改请求 — 路由到正确 Worker 的 --resume"""
        session = self.session_registry.get(task_id)
        if not session:
            raise NotFoundError(f"Session for {task_id} not found")

        worker_state = await self.get_worker_slot_state(session.worker_id)
        if worker_state.edit_slots.used >= worker_state.edit_slots.max:
            raise CapacityError(f"Worker {session.worker_id} edit slots full")

        runtime = self.manager.get_runtime_client(session.worker_id)
        task_id = await runtime.execute(
            command=["claude", "-p", message,
                "--resume", session.session_id,
                "--dangerously-skip-permissions", "--output-format", "stream-json"],
            cwd=session.cwd,
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
    return harness.session_registry.list_all()

@app.get("/api/workers")
async def list_workers():
    """列出所有 Worker 及其槽位状态"""
    workers = []
    for node in registry.list_all():
        slot_state = await harness.get_worker_slot_state(node["instance_id"])
        workers.append({**node, "slots": slot_state})
    return workers
```

---

## 5. 核心技术挑战与方案

### 5.1 Session 路由

**挑战：** 修改请求必须路由到 session 所在的 Worker。session 文件存在 Worker 本地文件系统中，无法跨 Worker 访问。

**方案：**

```
SessionRegistry 是路由的核心:
  1. 做书完成时注册: task_id → (worker_id, session_id, book_slug)
  2. 修改请求到来时: 查 task_id → 得到 worker_id → 发到该 Worker
  3. session_id 更新: --resume 后 Claude Code 可能返回新 session_id → 更新注册表

路由失败场景:
  - Worker 不在线 → 返回错误"Worker 离线"（不自动迁移）
  - Worker 修改槽位满 → 返回"请稍后重试"
  - session 不存在 → 返回"会话未找到"

⚠ MVP 不做跨 Worker session 迁移（详见 3.4 节）:
  session 绑定 Worker，Worker 离线 = 该 session 不可用
  数据基础已备（session 备份到 OSS），后续可启用迁移
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
  → 更新 SessionRegistry 中的 session_id
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

### 5.4 并发控制

**挑战：** 同一台 Worker 上可能有 1 个生产 + 3 个修改 = 4 个 Claude Code 进程同时运行。需要确保资源不冲突。

**方案：**

```
资源隔离:
  每个 Claude Code 进程:
    - 独立的 cwd (.work/{book_slug}/)
    - 独立的 session 文件
    - 共享 Claude Code 凭证（同一个 ~/.claude/.credentials.json）
    - 共享系统资源 (CPU/内存)

  凭证冲突:
    同一个 Claude Max 账号跑 4 个并发进程 → 额度消耗 4x
    → Worker 应该绑定足够的额度（4 个会话同时跑时 token 消耗飙升）
    → CredentialPool 分配时需要考虑 Worker 的总槽位数

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

  收到 EXECUTE 请求时:
    if mode == "producing" and production_count >= max_production_slots:
      → 拒绝 (返回 CAPACITY_FULL 错误)
    if mode == "editing" and edit_count >= max_edit_slots:
      → 拒绝

  进程退出时:
    → 释放对应槽位
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

---

## 6. 分步实施方案

### Phase 0：基础设施准备

1. Terraform 部署阿里云 VPC/安全组/密钥对
2. 制作 AMI（预装 Ubuntu + Python 3.11 + Node.js 20 + Claude Code CLI + audiobook-nonfiction 插件）
3. 配置 OSS Bucket（存储工作目录同步文件）
4. 准备 Claude Max 账号池

### Phase 1：单 Worker 端到端

1. 手动 `scale_out(1)` 创建一台 Worker
2. 通过 API 提交一本书 → 验证做书全流程
3. 验证实时聊天流（Claude Code → Manager → 外部 API）
4. 验证文件同步（.work/ → OSS）
5. 做书完成后验证 session 注册

### Phase 2：修改模式

1. 对已完成的书发送修改请求
2. 验证 session 路由 → 正确 Worker → --resume
3. 验证修改过程的聊天流
4. 验证并发修改（同时修改 2-3 本）

### Phase 3：多 Worker + 队列

1. 手动扩容到 3 台 Worker
2. 提交多本书 → 验证队列分发
3. 验证跨 Worker 的 session 路由
4. 验证槽位满时的排队/拒绝行为

### Phase 4：前端集成

1. 做书前端接入 API
2. 实时聊天框渲染
3. Phase 进度条（轮询 state.json）
4. 文件浏览器（OSS 文件列表）
5. 修改指令发送 UI

---

## 7. Audiobook 对框架提出的需求

### 7.1 Audiobook 特有但普适的需求

| 需求 | 说明 | 普适性 |
|------|------|--------|
| **多槽位并发模型** | 同一 Worker 上区分"生产槽位"和"修改槽位"，各自可配置上限 | 通用 — 任何需要混合重/轻任务的场景 |
| **Session 路由** | 修改请求路由到 session 所在的 Worker | 通用 — 有状态工作负载的亲和性路由 |
| **Session 持久化** | 做书完成后会话不销毁，支持随时 --resume | 通用 — 任何需要多轮交互的 Agent |
| **双向 Chat 中继** | 外部 → Manager → Worker → Claude Code (--resume) | 通用 — 人工审批、交互式 Agent |
| **文件实时同步到云存储** | .work/ + session 文件 → OSS/S3，分层防抖 + 同步清单 | 通用 — 需要外部实时查看 Agent 产物 |
| **从云存储统一读取** | 内容查询走 OSS 不走 Worker，附带一致性元数据 | 通用 — 解耦读路径和 Worker 生命周期 |
| **Webhook 事件通知** | 做书完成/失败/Phase 切换 → 推送到注册的 URL | 通用 — 后端系统异步事件驱动 |
| **任务重试/续跑** | 从指定 Phase 重新开始 或 断点续跑失败任务 | 通用 — 长时间任务的容错 |
| **常驻 Worker** | 手动扩容/缩容，不自动销毁 | 通用 — 稳定工作负载场景 |
| **文件写入到 Worker** | 运行时将原始文本等输入写入 Worker 文件系统 | 通用 — 任何需要输入数据的任务 |
| **跨 Worker Session 迁移** | session + workspace 备份到 OSS 后可在新 Worker 恢复 | 通用 — **MVP 不做**，数据基础已备 |

### 7.2 与其他 Harness 的交叉验证

| 框架能力 | agent-ml-research | CCM | Audiobook | 结论 |
|---------|------------------|-----|-----------|------|
| Worker Runtime | ✅ 替换 SSH | ✅ 替换本地子进程 | ✅ 启动 Claude Code 会话 | **框架核心** |
| 外部 API（轨迹） | ✅ 飞书消费 | ✅ 前端日志 | ✅ 做书聊天流 | **框架核心** |
| 外部 API（文件） | ✅ 研究产物 | ✅ 项目文件 | ✅ 讲稿 + 审核报告 + OSS 同步 | **框架核心** |
| 有状态亲和性 | ✅ 项目绑定 | ✅ session resume | ✅ **session 路由到 Worker** | **框架核心** |
| 多槽位并发 | — | ✅ max_concurrent=5 | ✅ **生产1 + 修改3** | **框架核心** |
| 优雅缩容 | ✅ 长时间训练 | ✅ 30min 任务 | ✅ 手动缩容需检查活跃会话 | **框架核心** |
| 双层凭证 | ✅ WandB/HF | ✅ Git key | ✅ Claude 账号 | **框架核心** |
| 双向 Chat | ✅ 飞书指令 | ✅ Plan 审批 | ✅ **修改指令 + 合规决策** | **框架核心** |
| 文件同步到 OSS/S3 | — | — | ✅ **.work/ + session 实时同步** | **新增** |
| 云存储统一读取 | — | — | ✅ **内容查询走 OSS** | **新增** |
| Webhook 通知 | ✅ 飞书 | — | ✅ **完成/失败/Phase 通知** | **新增** |
| 任务重试/续跑 | — | ✅ 重试 | ✅ **from_phase + continue** | **新增** |
| 跨 Worker 迁移 | — | — | ⬚ 数据基础已备，**MVP 不做** | 后续 |
| 常驻 Worker | ✅ | ✅ | ✅ | 已有 |

### 7.3 成本估算

| 资源 | 单价 | 说明 |
|------|------|------|
| 阿里云 ecs.c6.2xlarge (8C/16G) | ¥1.56/h On-Demand | 支持 1 生产 + 3 修改并发 |
| Claude Max 订阅 | 已有 | 30-80M token/本新书 |
| OSS 存储 | ¥0.12/GB/月 | 每本书 ~100-200MB 工作文件 |
| OSS 请求 | ¥0.01/万次 | 文件同步 API 调用 |

常驻 Worker 的月成本: ¥1.56 × 24 × 30 = **¥1,123/台/月** (On-Demand)。如果夜间不需要可以手动 stop 降成本。
