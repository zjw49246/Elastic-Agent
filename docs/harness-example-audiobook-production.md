# Harness 应用示例：有声书稿全自动化生产系统接入 Elastic-Agent

> 本文档以有声书稿全自动化生产系统（以下简称 Audiobook）为例，说明一个 **Claude Code 插件** 如何作为 Harness 接入 Elastic-Agent 弹性计算框架，实现多书并行生产、弹性扩缩容、崩溃恢复。
>
> 本文档基于 audiobook-nonfiction v1.1.1 的真实代码分析。
>
> 与 [agent-ml-research](harness-example-agent-ml-research.md)（替换自建基础设施）和 [CCM](harness-example-claude-code-manager.md)（扩展单机应用）不同，Audiobook 代表第三种接入模式：**将 Claude Code 插件提升为可弹性扩展的分布式生产系统**。

---

## 目录

1. [Audiobook 项目真实架构解析](#1-audiobook-项目真实架构解析)
2. [为什么需要 Elastic-Agent](#2-为什么需要-elastic-agent)
3. [基于框架的分布式架构设计](#3-基于框架的分布式架构设计)
4. [Harness 接口实现](#4-harness-接口实现)
5. [核心技术挑战与方案](#5-核心技术挑战与方案)
6. [分步实施方案](#6-分步实施方案)
7. [Audiobook 对框架提出的需求](#7-audiobook-对框架提出的需求)

---

## 1. Audiobook 项目真实架构解析

### 1.1 项目定位

Audiobook 是一个 **Claude Code 插件（Skill）**，将非虚构书籍转换为 TTS-ready 的有声书讲稿（默认压缩到原文 9-17%）。它实现了一个 **10 Phase 全自动化生产流水线**，由 Main Agent 编排 22 个专用子 Agent，配合 9 个 Python 工具脚本，产出可直接录音的讲稿。

**关键架构事实：**
- **不是一个后端服务** — 它是 Claude Code 的 Skill 插件，运行在 Claude Code CLI 会话中
- **不需要自建后端** — 所有编排由 Claude Code 的 Main Agent 完成
- **状态管理是文件系统** — `.work/{book_slug}/state.json` 追踪全部状态
- **子 Agent 是 Claude Code 子代理** — 通过 `Agent({subagent_type: "audiobook-xxx"})` 调用
- **单会话单书** — 一个 Claude Code 会话从头到尾处理一本书

### 1.2 插件结构

```
audiobook-nonfiction/
├── .claude-plugin/
│   ├── plugin.json              # 插件元数据（v1.1.1）
│   └── marketplace.json         # 本地 marketplace 注册
├── agents/                      # 22 个专用子 Agent 定义
│   ├── audiobook-text-compressor.md
│   ├── audiobook-story-structure-analyst.md
│   ├── audiobook-anchor-creator.md
│   ├── audiobook-narration-framework-designer.md   # Opus
│   ├── audiobook-draft-writer.md                   # Opus
│   ├── audiobook-progress-analyst.md
│   ├── audiobook-persona-fusion.md
│   ├── audiobook-opening-closing-editor.md         # Opus
│   ├── audiobook-book-facts-checker.md             # +WebSearch
│   ├── audiobook-intro-generator.md
│   ├── audiobook-auditor-fidelity.md               # 7 审核员
│   ├── audiobook-auditor-narrative-quality.md
│   ├── audiobook-auditor-safety.md
│   ├── audiobook-auditor-logic.md
│   ├── audiobook-auditor-repetition.md
│   ├── audiobook-auditor-style.md
│   ├── audiobook-auditor-standard.md
│   ├── audiobook-fixer.md                          # Opus
│   ├── audiobook-compliance-processor.md           # Opus
│   ├── audiobook-anchor-fixer.md
│   ├── audiobook-fidelity-prechecker.md
│   └── audiobook-intro-generator.md
├── skills/audiobook-nonfiction/
│   ├── SKILL.md                 # Main Agent 系统指令
│   ├── README.md                # 使用说明
│   ├── shared_values.md         # 质量共识（4 维度 + 6 原则）
│   ├── commands/
│   │   ├── audiobook.md         # /audiobook 命令定义
│   │   └── continue-book.md     # /continue-book 命令定义
│   ├── phases/                  # 10 Phase 流水线定义
│   │   ├── phase_00_init.md
│   │   ├── phase_01_decomposition.md
│   │   ├── phase_02_blueprint.md
│   │   ├── phase_03_splitting.md
│   │   ├── phase_04_production_loop.md
│   │   ├── phase_05_persona_fusion.md
│   │   ├── phase_06_opening_closing.md
│   │   ├── phase_07_audit_loop.md
│   │   ├── phase_075_final_review.md
│   │   ├── phase_08_compliance.md
│   │   ├── phase_085_intro.md
│   │   └── phase_09_delivery.md
│   ├── schemas/                 # 数据结构定义
│   │   ├── state.md             # state.json 状态机
│   │   ├── quality_targets.md   # 质量硬指标
│   │   ├── audit_report.md      # 审核报告格式
│   │   └── self_eval.md         # 子 Agent 自评格式
│   ├── decisions/               # 6 个决策点指南
│   │   ├── M1_blueprint_review.md
│   │   ├── M2_section_review.md
│   │   ├── M4_final_review.md
│   │   ├── M5_failure_report.md
│   │   └── M6_compliance_assessment.md
│   ├── personas/                # 叙述人格
│   │   └── nonfiction_default/
│   │       ├── framework.md
│   │       └── style.md
│   └── scripts/                 # 9 个 Python 工具
│       ├── doc_extractor.py     # PDF/EPUB/DOCX → 纯文本
│       ├── chunk_splitter.py    # 固定分块（~20k 字符）
│       ├── cjk_counter.py       # CJK 汉字计数
│       ├── word_count_calculator.py
│       ├── section_splitter.py  # 锚点切分
│       ├── compression_merger.py
│       ├── metrics_collector.py # 成本/token 追踪
│       ├── validate_and_repair_json.py  # 4 层 JSON 修复
│       └── audit_severity_diff.py
```

### 1.3 10 Phase 生产流水线

| Phase | 名称 | 关键子 Agent | 产出 | 耗时 |
|-------|------|-------------|------|------|
| 0 | 初始化 | — | `state.json` | <1min |
| 1 | 书籍解构 | text-compressor(Sonnet×N), book-facts-checker(Sonnet+WebSearch) | `raw_text.md`, `compressed.md`, `book_meta.json`, `book_facts.json` | 10-20min |
| 2 | 战略蓝图 | story-structure-analyst, anchor-creator, narration-framework-designer(**Opus**) | `blueprint.md`, `quality_targets.json`, `anchors.json` | 10-15min |
| 3 | 源文切片 | anchor-fixer(如需) | `sections/section_*.txt` | 2-5min |
| 4 | 主体生产 | progress-analyst(Sonnet×N), draft-writer(**Opus**×N) | `drafts/draft_*.md` | 20-40min |
| 5 | 人格融合 | persona-fusion(Sonnet×N) | `styled/styled_*.md` | 10-20min |
| 6 | 开头结尾 | opening-closing-editor(**Opus**) | `manuscript_v1.md` | 5-10min |
| 7 | 审核循环 | 7 auditors(Sonnet×7并行) + fixer(**Opus**), 最多 3 轮 | `manuscript_final.md` | 15-30min |
| 7.5 | 终审 | Main Agent 通读全文 | 决策记录 | 5min |
| 8 | 合规处理 | compliance-processor(**Opus**) | `manuscript_compliant.md` | 5-10min |
| 8.5 | 简介生成 | intro-generator + 3 auditor(并行) + fixer | `intro_final.md` | 5-10min |
| 9 | 交付打包 | — | `delivery/` | <1min |

**总计：** 1-2 小时/本书，50-80 次子 Agent 调用，30-80M token，约 $1.5-4 USD

### 1.4 状态机 (`state.json`)

```json
{
  "book_slug": "outliers",
  "state": "AUDITING",
  "phase": "7",
  "phase_iter": 2,
  "order": {
    "book_path": "/path/to/book.pdf",
    "target_word_count_pct": 12,
    "target_word_count_chars": null
  },
  "timestamps": {
    "INIT": "2026-04-20T10:00:00Z",
    "COMPRESSING": "2026-04-20T10:01:30Z"
  },
  "decisions": [
    {"at": "...", "decision_point": "M1", "phase": "2", "verdict": "accept"}
  ],
  "known_issues": [],
  "error_history": [],
  "rate_limit_events": [],
  "failure": null
}
```

**关键设计：** state.json 提供了完整的断点续做能力 — `/continue-book {book_slug}` 可以从任意中断的 Phase 恢复。

### 1.5 质量保证体系

**4 维度质量共识：**
1. **忠实性** — 论点/论据/尺度/立场四层对齐
2. **篇幅** — CJK 字数在 T × [0.70, 1.30] 硬指标内
3. **风格** — 教养都市人自然聊天语调
4. **合规** — PRC 法律 + 核心价值观

**3 类故障分类：**
- **Type A**：问题不可修复（≥5 次尝试），标记 Known Issue，跳过继续
- **Type B**：全书停滞（3 轮审核无改善），生成因果报告，上报人工
- **Type C**：不可恢复崩溃（Agent 崩溃、限频无法恢复），立即上报

### 1.6 工作目录结构

```
.work/{book_slug}/
├── state.json                    # 状态机
├── raw_text.md                   # Phase 1 原文
├── chunks/, compressed_chunks/   # Phase 1 分块
├── compressed.md                 # Phase 1 压缩版
├── book_meta.json                # Phase 1 元数据
├── book_facts.json               # Phase 1 事实核查
├── blueprint.md, blueprint_summary.md  # Phase 2 蓝图
├── quality_targets.json          # Phase 2 质量硬指标
├── anchors.json, story_structure.md    # Phase 2
├── sections/section_*.txt        # Phase 3 切片
├── drafts/draft_*.md             # Phase 4 底稿
├── styled/styled_*.md            # Phase 5 风格化
├── manuscript_v1.md              # Phase 6 初版
├── iter_*/                       # Phase 7 审核迭代
│   ├── audit_*.json              # 7 维度审核报告
│   └── manuscript_v*.md          # 修复版本
├── manuscript_final.md           # Phase 7 终版
├── manuscript_compliant.md       # Phase 8 合规版
├── intro_final.md                # Phase 8.5 简介
├── delivery/                     # Phase 9 交付
│   ├── manuscript.md
│   ├── intro.md
│   └── archive/
└── metrics.json                  # 成本/token 追踪
```

### 1.7 当前局限性

| 局限 | 说明 |
|------|------|
| **单机单书** | 一个 Claude Code 会话只能处理一本书，无法并行 |
| **无弹性扩展** | 需要手动启动 EC2、安装插件、启动 Claude Code |
| **单账号** | 一个 Claude Code 会话使用一个账号，额度用完整个流水线停滞 |
| **无崩溃恢复基础设施** | state.json 支持 `/continue-book`，但需要人工操作 |
| **无外部监控** | 流水线进度、审核状态只能在 Claude Code 会话内查看 |
| **无多书队列** | 多本书需要手动逐个启动 |
| **凭证手动管理** | Claude Code 登录态需要手动设置 |

---

## 2. 为什么需要 Elastic-Agent

### 2.1 核心需求

```
当前（单机单书）:                   目标（弹性多书并行）:

┌─────────────┐                     ┌─────────────┐
│ 单台机器     │                     │ Manager     │
│             │                     │ 调度 + 监控  │
│ Claude Code │                     │ + 外部 API  │
│ 1 个会话    │                     └──────┬──────┘
│ 1 本书      │                            │
│ 1 个账号    │               ┌────────────┼────────────┐
│             │               │            │            │
│ 手动操作    │          ┌────▼───┐   ┌────▼───┐   ┌────▼───┐
└─────────────┘          │Worker 1│   │Worker 2│   │Worker N│
                         │阿里云  │   │阿里云  │   │阿里云  │
                         │账号 A  │   │账号 B  │   │账号 C  │
                         │《异类》 │   │《枪炮》 │   │《思考》 │
                         │Phase 4 │   │Phase 7 │   │Phase 1 │
                         └────────┘   └────────┘   └────────┘
                         用完即毁       用完即毁       用完即毁
```

| 问题 | 单机 | Elastic-Agent |
|------|------|---------------|
| 并行度 | 1 本/时 | N 本并行（按需扩容） |
| 额度 | 单账号，30-80M token/书容易触限 | 多账号分布不同 Worker，自动换号 |
| 可用性 | Claude Code 崩溃 = 停工 | Worker 崩溃 → 新 Worker + `/continue-book` 自动恢复 |
| 监控 | 只能在终端看 | 外部 API 实时获取 Phase 进度、审核状态、讲稿文件 |
| 成本 | 固定开销 | 临时 Worker 用完即毁，空闲零成本 |
| 运维 | 手动安装插件、登录账号 | Bootstrap 自动化全部 |

### 2.2 Audiobook 特有的需求

与 agent-ml-research 和 CCM 相比，Audiobook 有几个特殊需求：

| 需求 | 说明 | 其他 Harness 是否有 |
|------|------|-------------------|
| **临时 Worker** | 一本书一台机器，完成后销毁 | agent-ml: 常驻; CCM: 常驻 |
| **断点续做** | Worker 崩溃后新 Worker 从 state.json 恢复 | 部分 — CCM 有 session resume |
| **Phase 进度监控** | 外部服务需要知道当前在哪个 Phase | 部分 — CCM 有任务状态 |
| **工作目录同步** | `.work/` 目录需要持久化到 S3/OSS 以支持崩溃恢复 | 部分 |
| **长时间任务** | 1-2 小时/书，Drain 超时需要足够长 | agent-ml: 小时级; CCM: 分钟级 |

---

## 3. 基于框架的分布式架构设计

### 3.1 整体架构

```
┌─────────────────────────────────────────────────────────────────┐
│                         Manager 节点                            │
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │             Audiobook Dispatcher（业务层）                │   │
│  │                                                          │   │
│  │  ┌───────────────┐  ┌──────────┐  ┌──────────────────┐  │   │
│  │  │ BookQueue      │  │TaskState │  │ Web UI           │  │   │
│  │  │(做书请求队列)  │  │(全局状态)│  │ (进度/聊天/文件) │  │   │
│  │  └───────┬───────┘  └──────────┘  └──────────────────┘  │   │
│  └──────────┼───────────────────────────────────────────────┘   │
│             │ 调用框架 API                                       │
│  ┌──────────▼───────────────────────────────────────────────┐   │
│  │             Elastic-Agent 框架层                          │   │
│  │                                                          │   │
│  │  AliyunEcsProvider    CredentialPool    BootstrapPipeline │   │
│  │  NodeRegistry         HealthChecker     DrainManager      │   │
│  │  ExternalAPI(轨迹流)  ExternalAPI(文件)  CloudReconciler  │   │
│  └──────────────────────────┬───────────────────────────────┘   │
└─────────────────────────────┼───────────────────────────────────┘
                              │ Worker Runtime Protocol
                ┌─────────────┼─────────────┐
                │             │             │
           ┌────▼────┐  ┌────▼────┐  ┌─────▼───┐
           │Worker 1 │  │Worker 2 │  │Worker N │
           │阿里云ECS│  │阿里云ECS│  │阿里云ECS│
           │┌───────┐│  │┌───────┐│  │┌───────┐│
           ││Worker ││  ││Worker ││  ││Worker ││
           ││Runtime││  ││Runtime││  ││Runtime ││  ← 框架内置
           │└───┬───┘│  │└───┬───┘│  │└───┬───┘│
           │┌───▼───┐│  │┌───▼───┐│  │┌───▼───┐│
           ││Claude ││  ││Claude ││  ││Claude ││
           ││Code   ││  ││Code   ││  ││Code   ││
           ││+ 插件 ││  ││+ 插件 ││  ││+ 插件 ││  ← audiobook-nonfiction
           │└───────┘│  │└───────┘│  │└───────┘│
           │  《异类》 │  │  《枪炮》 │  │  《思考》 │
           └─────────┘  └─────────┘  └─────────┘
           用完即毁       用完即毁       用完即毁
```

### 3.2 工作流程

#### 扩容（收到做书请求）

```
前端提交做书请求（book_path, book_slug, persona, target_pct）
  │
  ▼
Manager BookQueue 入队
  │
  ▼
Elastic-Agent scale_out(count=1, task_metadata={book_slug, book_path, ...})
  │
  ▼
AliyunEcsProvider 创建临时 ECS 实例
  → 等待 running
  → 从 CredentialPool 选择账号
  → Bootstrap Pipeline 执行:
      1. 安装 Node.js + Claude Code CLI
      2. 注入 Claude Code 凭证（refresh token 写入 ~/.claude/.credentials.json）
      3. 安装 audiobook-nonfiction 插件
      4. 安装 Python 依赖（pypdf, ebooklib, python-docx）
      5. 上传书籍 PDF 到 Worker
      6. 启动 Worker Runtime
      7. 通过 Worker Runtime 启动 Claude Code 会话:
         claude -p "/audiobook {book_path} {persona} target_pct={N}" \
           --dangerously-skip-permissions --output-format stream-json
  │
  ▼
Worker Runtime 流式回传 Claude Code 的 NDJSON 输出到 Manager
  → Manager 事件总线分发
  → 外部 API 实时推送到前端（Phase 进度、Agent 轨迹、审核结果）
  │
  ▼
Worker 上的 .work/{book_slug}/ 目录定期同步到 OSS（框架文件监听 + 同步）
  → 外部服务可通过 /api/external/files/{node_id}/... 实时获取中间产物
```

#### 崩溃恢复

```
HealthChecker 检测到 Worker 心跳超时
  │
  ▼
从 OSS 获取最新的 .work/{book_slug}/ 快照
  │
  ▼
Elastic-Agent scale_out(count=1, task_metadata={book_slug, recovery: true})
  │
  ▼
Bootstrap Pipeline:
  ... (同上 1-6)
  7. 从 OSS 恢复 .work/ 目录到新 Worker
  8. 启动 Claude Code: claude -p "/continue-book {book_slug}" \
       --dangerously-skip-permissions --output-format stream-json
  │
  ▼
Claude Code 读取 state.json，从中断的 Phase 恢复
```

#### 任务完成 / 缩容

```
Claude Code 输出 phase=9, state=DELIVERED
  │
  ▼
Worker Runtime 检测到进程退出 (exit_code=0)
  │
  ▼
Manager 收到 process_exit 事件
  → 从 Worker 下载 delivery/ 目录最终产物
  → 持久化到 OSS
  → 回收 Claude Code 凭证到 CredentialPool
  → 销毁 ECS 实例（临时 Worker 模式）
  → 从 NodeRegistry 移除
  │
  ▼
BookQueue 标记任务完成
前端展示：讲稿下载链接、简介、质量报告
```

### 3.3 外部服务 API 的使用

Audiobook 的前端和监控系统通过框架外部 API 获取实时数据：

| 需求 | 外部 API 端点 | 数据来源 |
|------|-------------|---------|
| Phase 进度 | `GET /api/external/files/{node_id}/.work/{slug}/state.json` | state.json 文件 |
| 实时 Agent 轨迹 | `WS /api/external/traces/{node_id}/stream` | Claude Code stream-json 输出 |
| 审核报告 | `GET /api/external/files/{node_id}/.work/{slug}/iter_*/audit_*.json` | 审核 JSON 文件 |
| 讲稿预览 | `GET /api/external/files/{node_id}/.work/{slug}/manuscript_*.md` | 讲稿文件 |
| 成本追踪 | `GET /api/external/files/{node_id}/.work/{slug}/metrics.json` | metrics.json |
| 文件变更监听 | `WS /api/external/files/{node_id}/watch` | inotify 监听 .work/ |
| 蓝图审阅 | `GET /api/external/files/{node_id}/.work/{slug}/blueprint.md` | 蓝图文件 |
| 质量指标 | `GET /api/external/files/{node_id}/.work/{slug}/quality_targets.json` | 质量硬指标 |

**前端轮询 state.json 可以构建 Phase 进度条：**

```javascript
// 前端伪代码
const state = await fetch(`/api/external/files/${nodeId}/.work/${slug}/state.json`);
const phaseNames = ["初始化","解构","蓝图","切片","生产","风格","开头结尾","审核","合规","简介","交付"];
progressBar.update(state.phase, phaseNames[state.phase]);
if (state.phase === "7") {
  progressBar.detail(`审核第 ${state.phase_iter}/3 轮`);
}
```

---

## 4. Harness 接口实现

### 4.1 AudiobookHarness 定义

```python
from elastic_agent import (
    Harness, BootstrapStep, ServiceDefinition,
    ScalingSignal, FrameworkEvent, WorkerLifecycle,
)

class AudiobookHarness(Harness):
    """有声书稿生产系统的 Elastic-Agent Harness"""

    def __init__(self, config: dict):
        self.config = config

    def get_worker_lifecycle(self) -> WorkerLifecycle:
        return WorkerLifecycle.EPHEMERAL  # 用完即毁

    def get_repo_url(self) -> str | None:
        return None  # 插件通过 Bootstrap 安装，不需要 clone 代码仓库

    def get_bootstrap_steps(self) -> list[BootstrapStep]:
        return [
            InstallNodeJSStep(),            # Node.js 20.x
            InstallClaudeCodeStep(),         # npm install -g @anthropic-ai/claude-code
            InjectCredentialStep(),          # 写入 ~/.claude/.credentials.json
            InstallAudiobookPluginStep(),    # 解压插件 + claude plugin install
            InstallPythonDepsStep(),         # pypdf, ebooklib, python-docx
            UploadBookPDFStep(),             # SCP/OSS 上传书籍文件到 Worker
            StartWorkerRuntimeStep(),        # 启动框架 Worker Runtime
            LaunchClaudeCodeSessionStep(),   # 启动 Claude Code 会话执行 /audiobook
        ]

    def get_service_definitions(self) -> list[ServiceDefinition]:
        return []  # 无常驻服务，Claude Code 进程由 Bootstrap 最后一步启动

    def get_app_credentials(self) -> list[str]:
        return []  # Audiobook 不需要额外的应用凭证（Git key 等）

    def get_scaling_signal(self) -> ScalingSignal:
        pending_books = self._count_pending_books()
        active_workers = self._count_active_workers()
        return ScalingSignal(
            pending_tasks=pending_books,
            idle_workers=0,  # 临时 Worker 没有空闲概念
            busy_workers=active_workers,
        )

    def get_state_directories(self) -> list[str]:
        """声明需要持久化的目录（框架定期同步到 OSS）"""
        return ["/home/root/.work/"]

    def get_drain_timeout(self) -> int:
        return 7200  # 2 小时（一本书最长耗时）

    def get_event_handlers(self) -> dict:
        return {
            FrameworkEvent.NODE_READY: self._on_node_ready,
            FrameworkEvent.WORKER_UNHEALTHY: self._on_worker_unhealthy,
            FrameworkEvent.NODE_TERMINATING: self._on_node_terminating,
        }

    async def _on_node_ready(self, data: dict):
        """Worker 就绪后更新任务状态"""
        book_slug = data.get("task_metadata", {}).get("book_slug")
        self.book_queue.update_status(book_slug, "running", worker_id=data["node_id"])

    async def _on_worker_unhealthy(self, data: dict):
        """Worker 异常 — 触发崩溃恢复"""
        node_id = data["node_id"]
        book_slug = self.book_queue.get_book_by_worker(node_id)
        if book_slug:
            # 标记需要恢复
            self.book_queue.update_status(book_slug, "recovering")
            # 框架会自动终止旧 Worker
            # 启动新 Worker 并恢复
            await self.manager.scale_out(
                count=1,
                task_metadata={
                    "book_slug": book_slug,
                    "recovery": True,
                    "oss_state_path": f"oss://audiobook-state/{book_slug}/",
                },
            )

    async def _on_node_terminating(self, data: dict):
        """Worker 终止前 — 确保工作目录已同步到 OSS"""
        # 框架的文件同步机制应该已经处理了，这里做最终确认
        pass
```

### 4.2 Bootstrap 步骤实现

```python
class InstallAudiobookPluginStep(BootstrapStep):
    name = "install-audiobook-plugin"
    timeout = 120

    async def execute(self, ctx):
        # 上传插件压缩包到 Worker
        plugin_archive = ctx.config["audiobook_plugin_path"]
        await ctx.runtime.upload_file(plugin_archive, "/tmp/audiobook-nonfiction.tar.gz")
        # 解压 + 安装
        await ctx.runtime.execute(
            ["bash", "-c",
             "cd /opt && tar xzf /tmp/audiobook-nonfiction.tar.gz && "
             "cd audiobook-nonfiction && "
             "claude plugin marketplace add ./ && "
             "claude plugin install audiobook-nonfiction@audiobook-local"],
            timeout=60,
        )

class InstallPythonDepsStep(BootstrapStep):
    name = "install-python-deps"
    timeout = 120

    async def execute(self, ctx):
        await ctx.runtime.execute(
            ["pip3", "install", "pypdf", "ebooklib", "python-docx"],
            timeout=90,
        )

class UploadBookPDFStep(BootstrapStep):
    name = "upload-book-pdf"
    timeout = 60

    async def execute(self, ctx):
        book_path = ctx.task_metadata["book_path"]
        remote_path = f"/home/root/books/{ctx.task_metadata['book_slug']}.pdf"
        await ctx.runtime.upload_file(book_path, remote_path)
        ctx.metadata["remote_book_path"] = remote_path

class LaunchClaudeCodeSessionStep(BootstrapStep):
    name = "launch-claude-code"
    timeout = 30

    async def execute(self, ctx):
        meta = ctx.task_metadata
        book_slug = meta["book_slug"]
        remote_book_path = ctx.metadata.get("remote_book_path",
            f"/home/root/books/{book_slug}.pdf")
        persona = meta.get("persona", "nonfiction_default")
        target = meta.get("target_word_count_pct", 13)

        if meta.get("recovery"):
            # 崩溃恢复模式 — 先从 OSS 恢复工作目录
            oss_path = meta["oss_state_path"]
            await ctx.runtime.execute(
                ["bash", "-c", f"ossutil cp -r {oss_path} /home/root/.work/{book_slug}/"],
                timeout=120,
            )
            prompt = f"/continue-book {book_slug}"
        else:
            prompt = f"/audiobook {remote_book_path} {persona} target_pct={target}"

        # 启动 Claude Code 会话（非阻塞 — Worker Runtime 管理进程生命周期）
        await ctx.runtime.execute(
            ["claude", "-p", prompt,
             "--dangerously-skip-permissions",
             "--output-format", "stream-json",
             "--verbose"],
            cwd="/home/root",
            timeout=None,  # 无超时 — 1-2 小时任务
        )
```

### 4.3 Manager 侧集成

```python
from elastic_agent import ElasticAgentManager, AliyunEcsProvider, CredentialPool
from audiobook_harness import AudiobookHarness

# 阿里云优先
provider = AliyunEcsProvider(
    region_id="cn-hangzhou",
    image_id="m-bp1xxxx",                 # 预装 Ubuntu + Python + Node.js
    instance_type="ecs.c6.xlarge",         # 4C/8G（Opus 子 Agent 需要更多内存）
    security_group_id="sg-bp1xxxx",        # Terraform output
    vswitch_id="vsw-bp1xxxx",             # Terraform output
    key_pair_name="elastic-agent-key",
    spot_strategy="SpotAsPriceGo",         # 抢占式实例节省 70-85%
)

manager = ElasticAgentManager(
    provider=provider,
    credential_pool=CredentialPool("claude_accounts.json"),
    harness=AudiobookHarness({
        "audiobook_plugin_path": "/opt/audiobook-nonfiction.tar.gz",
        "oss_bucket": "audiobook-production",
    }),
)

# 收到做书请求时
async def handle_book_request(book_slug: str, book_path: str, target_pct: int = 13):
    nodes = await manager.scale_out(
        count=1,
        instance_config=InstanceConfig(
            name=f"audiobook-{book_slug}",
            spot=True,  # 抢占式实例
        ),
        task_metadata={
            "book_slug": book_slug,
            "book_path": book_path,
            "target_word_count_pct": target_pct,
        },
    )
    return nodes[0].id

# 前端获取实时数据
# WS: /api/external/traces/{node_id}/stream          → Agent 轨迹
# GET: /api/external/files/{node_id}/.work/*/state.json → Phase 进度
# GET: /api/external/files/{node_id}/.work/*/delivery/  → 最终产物
```

---

## 5. 核心技术挑战与方案

### 5.1 Claude Code 插件安装自动化

**挑战：** audiobook-nonfiction 是 Claude Code 插件，需要通过 `claude plugin install` 安装。Bootstrap 必须正确处理插件的 marketplace 注册和安装。

**方案：**
1. 将插件压缩包预置在 AMI/镜像中（减少 Bootstrap 时间）
2. 或通过 SCP/OSS 上传到 Worker 后本地安装
3. 插件安装后需要**重启 Claude Code 会话**才能生效

### 5.2 工作目录持久化与崩溃恢复

**挑战：** `.work/{book_slug}/` 目录是全部中间产物的唯一存储。如果 Worker（临时 ECS 实例）崩溃或被 Spot 回收，这些文件丢失则无法恢复。

**方案：**
```
Worker 运行中:
  框架 Worker Runtime 使用 inotify 监听 .work/ 目录
    → 文件变更时增量同步到 OSS (aliyun) / S3 (aws)
    → 同步间隔: 变更后 3-5 秒（防抖）
    → 关键文件（state.json）变更立即同步

Worker 崩溃后:
  Manager HealthChecker 检测心跳超时
    → 创建新 Worker
    → Bootstrap: ossutil cp -r oss://.../{book_slug}/ /home/root/.work/{book_slug}/
    → 启动 Claude Code: /continue-book {book_slug}
    → state.json 告诉 Claude Code 从哪个 Phase 恢复
```

**state.json 是恢复的核心** — 它记录了 Phase、迭代次数、决策历史、已知问题。`/continue-book` 命令读取 state.json 后跳过已完成的 Phase。

### 5.3 Claude Code 账号额度管理

**挑战：** 单本书消耗 30-80M token，Claude Max 订阅有 5 小时滑动窗口限制。高并发生产时账号池必须足够大。

**方案：**
- CredentialPool 在 Bootstrap 时选择最空闲账号
- 每个 Worker 独占一个账号（一对一绑定）
- 如果做书过程中额度耗尽：
  1. Claude Code 会自动等待限频恢复（rate_limit_events 记录在 state.json）
  2. 如果等待超时 → Worker Runtime 检测到进程异常 → 触发崩溃恢复流程
  3. 新 Worker 分配新账号 + `/continue-book` 恢复

### 5.4 从 Agent 轨迹中提取 Phase 进度

**挑战：** 外部服务需要知道当前书在哪个 Phase。Claude Code 的 stream-json 输出是原始的 NDJSON 事件，不包含 Phase 信息。

**方案（两个层次）：**

1. **文件监听 state.json**（推荐）— 框架外部 API 监听 state.json 文件变更，每次 state 更新时推送给外部：
   ```
   WS /api/external/files/{node_id}/watch → 监听 .work/*/state.json
   ```

2. **解析 Agent 轨迹流**（辅助）— 从 stream-json 中匹配 Phase 切换关键词。但这需要 Harness 自定义解析逻辑，不如直接读 state.json 可靠。

### 5.5 Spot 实例中断处理

**挑战：** 阿里云抢占式实例可能被回收（2 分钟通知），做书流水线 1-2 小时，中断概率不低。

**方案：**
- 抢占式实例被回收前阿里云发送中断通知
- 框架 Worker Runtime 接收中断信号后：
  1. 立即将当前 `.work/` 目录全量同步到 OSS
  2. 记录中断点到 state.json
  3. Manager 自动创建新 Worker（On-Demand 或新 Spot）+ `/continue-book` 恢复

### 5.6 用户交互（合规决策 M6）

**挑战：** Phase 8 有一个用户决策点 M6 — 用户需要选择使用合规版还是原始版。当前设计中这个决策在 Claude Code 会话内完成，分布式后如何让外部用户参与？

**方案：**
- 框架反向消息通道（Manager → Worker）
- 前端检测到 state.json 中 `state: "NEEDS_HUMAN"` 时显示决策 UI
- 用户选择后通过 Manager API 发送消息到 Worker
- Worker Runtime 将消息写入一个文件（如 `.work/{slug}/user_decision.json`）
- Claude Code 的 `/continue-book` 读取该文件继续

---

## 6. 分步实施方案

> **前置条件：** 按 [MVP 计划](mvp-plan.md) 完成 Terraform 网络部署和框架核心模块开发。

### Phase 0：基础设施准备

1. Terraform 部署阿里云 VPC/安全组/密钥对
2. 制作 AMI（预装 Ubuntu + Python 3.11 + Node.js 20 + Claude Code CLI + audiobook-nonfiction 插件）
3. 配置 OSS Bucket（存储工作目录快照和最终交付物）
4. 准备 Claude Max 账号池

### Phase 1：单书端到端验证

1. 手动创建一台阿里云 ECS
2. 安装插件 + 登录 Claude Code + 执行 `/audiobook`
3. 验证全 10 Phase 流水线可以在阿里云 ECS 上完整运行
4. 验证 `/continue-book` 可以从中断恢复

### Phase 2：框架集成

1. 实现 AudiobookHarness（Bootstrap 步骤、事件处理）
2. 通过 Elastic-Agent 创建临时 Worker → 自动安装插件 → 自动执行做书
3. 验证外部 API：通过 `/api/external/files/` 实时获取 state.json 和讲稿
4. 验证崩溃恢复：手动 kill Worker → 新 Worker 自动恢复

### Phase 3：多书并行 + 前端

1. 实现 BookQueue 队列管理（多本书排队/并行）
2. 前端：提交做书请求 UI
3. 前端：Phase 进度条（轮询 state.json）
4. 前端：Agent 轨迹实时流（WebSocket 消费外部 API）
5. 前端：讲稿预览/下载

### Phase 4：生产化

1. 额度监控 + 自动换号
2. Spot 实例中断处理
3. 成本追踪仪表盘（每本书的 ECS 成本 + token 成本）
4. 用户决策点 M6 的外部交互
5. 批量做书支持

---

## 7. Audiobook 对框架提出的需求

### 7.1 Audiobook 特有但普适的需求

| 需求 | 说明 | 普适性 |
|------|------|--------|
| **临时 Worker 模式** | 一个任务一台 Worker，完成后销毁 | 通用 — batch job、CI runner 都需要 |
| **task_metadata 注入** | Bootstrap 需要知道具体任务参数（book_slug、book_path） | 通用 — 任何按需启动 Worker 的场景 |
| **工作目录持久化** | 定期同步指定目录到 OSS/S3 | 通用 — ML checkpoint、中间结果 |
| **崩溃恢复** | 新 Worker 从 OSS 恢复状态 + 续做 | 通用 — 长时间任务的可靠性保证 |
| **反向消息通道** | Manager → Worker 传递用户决策 | 通用 — 人工审批、交互式 Agent |
| **Spot 中断处理** | 抢占式实例回收时优雅保存状态 | 通用 — 使用 Spot 的所有场景 |
| **文件上传到 Worker** | Bootstrap 时上传书籍 PDF 到 Worker | 通用 — 任何需要输入文件的任务 |

### 7.2 与其他 Harness 的交叉验证

| 框架能力 | agent-ml-research | CCM | Audiobook | 结论 |
|---------|------------------|-----|-----------|------|
| Worker Runtime | ✅ 替换 SSH | ✅ 替换本地子进程 | ✅ 启动 Claude Code 会话 | **框架核心** |
| 日志流式传输 | ✅ 飞书告警 | ✅ WebSocket 前端 | ✅ stream-json → 外部 API | **框架核心** |
| 外部 API（轨迹） | ✅ 飞书消费 | ✅ 前端日志 | ✅ Phase 进度 + Agent 轨迹 | **框架核心** |
| 外部 API（文件） | ✅ 研究产物 | ✅ 项目文件 | ✅ state.json + 讲稿 + 审核报告 | **框架核心** |
| 有状态亲和性 | ✅ 项目绑定 | ✅ session resume | ❌ 临时 Worker 不需要 | 部分通用 |
| 优雅缩容 | ✅ 长时间训练 | ✅ 30min 任务 | ✅ 2h 做书（Drain 7200s） | **框架核心** |
| 双层凭证 | ✅ WandB/HF | ✅ Git key | ✅ Claude 账号 | **框架核心** |
| 扩缩容信号 | ✅ 项目数 | ✅ 队列深度 | ✅ 待做书数 | **框架核心** |
| **临时 Worker** | ❌ 常驻 | ❌ 常驻 | ✅ 用完即毁 | **新增** |
| **工作目录持久化** | ❌ | ❌ | ✅ .work/ → OSS | **新增** |
| **崩溃恢复** | ❌ | ❌ | ✅ OSS → 新 Worker | **新增** |
| **task_metadata** | ❌ | ❌ | ✅ book_slug/path | **新增** |
| **反向消息** | ✅ 飞书指令 | ✅ Plan 审批 | ✅ 合规决策 M6 | **框架核心** |
| **Terraform IaC** | ✅ | ✅ | ✅ | **框架提供模板** |

### 7.3 成本估算

| 资源 | 单价 | 单本书用量 | 单本书成本 |
|------|------|----------|----------|
| 阿里云 ecs.c6.xlarge On-Demand | ¥0.78/h | 2h | ¥1.56 |
| 阿里云 ecs.c6.xlarge Spot | ~¥0.12/h | 2h | ¥0.24 |
| Claude Max 订阅（已有） | — | 30-80M token | ~$1.5-4 |
| OSS 存储 | ¥0.12/GB/月 | ~100MB | ¥0.012 |
| **总计 (Spot)** | | | **~¥0.25 + $2.75 ≈ ¥20/本** |

使用 Spot 实例后，**基础设施成本几乎可以忽略**，主要成本是 Claude API token 消耗。
