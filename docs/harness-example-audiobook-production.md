# Harness 应用示例：有声书稿全自动化生产系统接入 Elastic-Agent

> 本文档以有声书稿全自动化生产系统（以下简称 Audiobook）为例，说明一个 **从零设计、尚未实现** 的分布式系统如何以 Elastic-Agent Harness 为基础进行架构设计。
>
> 与 [agent-ml-research Harness 文档](harness-example-agent-ml-research.md) 中的 **替换** 已有基础设施不同，也与 [CCM Harness 文档](harness-example-claude-code-manager.md) 中的 **扩展** 单机应用不同，Audiobook 是一个绿地项目——它的基础设施层可以直接基于 Elastic-Agent 框架设计，避免先自建再迁移的浪费。
>
> 本案例代表了第三种典型接入模式：**绿地构建**。

---

## 目录

1. [Audiobook 项目解析](#1-audiobook-项目解析)
2. [为什么 Audiobook 需要 Elastic-Agent](#2-为什么-audiobook-需要-elastic-agent)
3. [基于框架的架构设计](#3-基于框架的架构设计)
4. [模块映射：自建 vs 框架](#4-模块映射自建-vs-框架)
5. [Harness 接口实现](#5-harness-接口实现)
6. [分步实施方案](#6-分步实施方案)
7. [技术细节与挑战](#7-技术细节与挑战)
8. [Audiobook 对框架提出的需求](#8-audiobook-对框架提出的需求)

---

## 1. Audiobook 项目解析

### 1.1 项目定位

Audiobook 是一个 **端到端有声书稿自动化生产平台**，用户在前端提交一本书的 PDF，系统自动完成从解构、蓝图设计、底稿生成、风格润色、审核迭代到合规交付的全部 10 个 Phase，最终交付可用于录音的有声书稿。

核心能力：
- 前端提交做书请求 → 后端按需启动 EC2 Worker → 自动登录 Claude Code → 安装做书插件 → 执行 10 Phase 做书流水线
- 每台 EC2 上运行 1-2 个 Claude Code 账号，额度满自动切号（hardlink session + `--resume` 保持上下文）
- **实时双向聊天**：EC2 上 Claude Code 的每条消息实时流到前端聊天框，用户可以发消息回去（如合规决策、手动干预）
- 多任务并行：多台 EC2 同时做不同书，前端可以切换查看任意一台的聊天流
- 中间文件实时同步到 S3，前端可随时查看
- EC2 崩溃自动恢复：从 S3 恢复工作目录 + `/continue-book` 从断点继续
- 任务完成后 EC2 自动销毁

### 1.2 系统架构

Audiobook 是 **前端 → 后端 Dispatcher → EC2 Worker** 三层架构：

```
┌────────────────────────────────────────────────────┐
│                  前端 (Web UI)                       │
│  Vue/React SPA                                      │
│  - 提交做书请求（书名、PDF、参数）                     │
│  - 聊天框：镜像 EC2 Claude Code 对话 + 用户回复       │
│  - 多任务切换、Phase 进度条、中间文件浏览             │
└──────────────────────┬─────────────────────────────┘
                       │ HTTP + WebSocket
                       ▼
┌────────────────────────────────────────────────────┐
│              后端 Dispatcher (FastAPI)               │
│  常驻服务，管理机上运行                                │
│                                                     │
│  ┌────────────┐ ┌────────────┐ ┌────────────────┐  │
│  │TaskManager │ │EC2 Manager │ │ StreamHub      │  │
│  │SQLite 任务  │ │boto3 生命期│ │SQS→WS 中继     │  │
│  └────────────┘ └────────────┘ └────────────────┘  │
│  ┌────────────┐ ┌────────────┐ ┌────────────────┐  │
│  │ChatService │ │PoolMonitor │ │HealthChecker   │  │
│  │双向聊天管理 │ │号池状态聚合│ │崩溃检测+恢复    │  │
│  └────────────┘ └────────────┘ └────────────────┘  │
└──────────────────────┬─────────────────────────────┘
                       │ SQS 双向 + S3
          ┌────────────┼────────────┐
          │            │            │
     ┌────▼───┐   ┌────▼───┐   ┌────▼───┐
     │EC2 #1  │   │EC2 #2  │   │EC2 #N  │
     │        │   │        │   │        │
     │Worker  │   │Worker  │   │Worker  │
     │Agent   │   │Agent   │   │Agent   │
     │        │   │        │   │        │
     │Claude  │   │Claude  │   │Claude  │
     │Code    │   │Code    │   │Code    │
     │1-2 账号 │   │1-2 账号 │   │1-2 账号 │
     │        │   │        │   │        │
     │watchdog│   │watchdog│   │watchdog│
     └────────┘   └────────┘   └────────┘
     用完即毁       用完即毁       用完即毁
```

### 1.3 核心模块

#### 后端 Dispatcher

| 模块 | 职责 |
|------|------|
| `TaskManager` | 任务 CRUD、状态机管理（queued → launching → running → completed） |
| `EC2Manager` | boto3 启动/监控/销毁 EC2，构建 user_data，管理 Launch Template |
| `StreamHub` | 消费上行 SQS（EC2 → 后端），写 DB + 推 WebSocket |
| `ChatService` | 管理 WebSocket 双向连接、下行消息投递（前端 → EC2） |
| `PoolMonitorService` | 消费号池事件，聚合入 DB，告警通知 |
| `HealthChecker` | 心跳超时检测 → EC2 崩溃恢复 |

#### EC2 Worker

| 模块 | 职责 |
|------|------|
| `WorkerAgent` | 主控进程：选号 → 启动 Claude Code → stream-json 解析 → 限流检测 → 切号 → resume |
| `StreamReporter` | 0.5s 攒批 + 即时 flush，SQS `send_message_batch` 上报聊天消息 |
| `FileSyncer` | 每 3s 扫描 `.work/` 目录变更，增量上传 S3 |
| `PoolStateWatcher` | 每 30s diff watchdog 状态文件，检测账号状态转换，SQS 上报事件 |
| `claude-pool-watchdog` | Shell 脚本（AMI 预装），每 60s 调用 Anthropic Usage API 采集额度 |

### 1.4 数据流通道

| 通道 | 方向 | 用途 |
|------|------|------|
| `audiobook-upstream` SQS | EC2 → 后端 | 聊天消息、Phase 变化、心跳、号池事件（全局共享） |
| `audiobook-down-{task_id}` SQS | 后端 → EC2 | 用户消息、控制命令（按任务动态创建/销毁） |
| S3 `tasks/{task_id}/work/` | EC2 → 后端 → 前端 | 中间文件实时同步 |
| WebSocket `/ws/tasks/{id}` | 后端 ↔ 前端 | 实时聊天双向通信 |

### 1.5 技术栈

| 层级 | 技术 |
|------|------|
| 语言 | Python 3.11+（后端 + Worker）、TypeScript（前端） |
| 后端 | FastAPI、SQLAlchemy (async)、aiosqlite |
| 前端 | Vue 3 + Vite + TypeScript |
| AWS | EC2、S3、SQS（Standard）、Secrets Manager、Launch Template、AMI |
| 浏览器自动化 | Playwright + playwright-stealth + mitmproxy（自动登录） |
| 进程管理 | user_data 脚本、PGID 追踪、start_new_session |
| 实时通信 | SQS + WebSocket |
| 持久化 | SQLite（聊天消息、任务、账号状态）、S3（文件） |

### 1.6 关键特性

| 特性 | 实现方式 |
|------|---------|
| **无感账号切换** | 限流检测 → `claude-pool select` → `session_linker.py` 硬链接 `.jsonl` → `--resume` 继续 |
| **实时聊天镜像** | tempfile tail-read → StreamReporter 0.5s 攒批 → SQS → StreamHub 1s 轮询 → WebSocket → 前端 |
| **双向交互** | 前端 → WebSocket → ChatService → 下行 SQS → Worker `poll_user_commands()` → `--resume` + 用户消息 |
| **多任务切换** | 前端 `useChatManager` → 切换时 REST 加载历史 + WebSocket 订阅实时流 |
| **文件实时同步** | FileSyncer 3s 扫描 → S3 上传 → SQS 通知 → 前端文件列表刷新 |
| **崩溃恢复** | HealthChecker 心跳超时 → 新 EC2 → S3 恢复 `.work/` → `/continue-book` 从断点继续 |
| **三层号池监控** | watchdog 60s 采集 → PoolStateWatcher 30s diff → PoolMonitorService 入库+告警 |

### 1.7 与 agent-ml-research 和 CCM 的对比

| 维度 | agent-ml-research | CCM | Audiobook |
|------|-------------------|-----|-----------|
| 架构 | 已有 Manager-Worker | 单机单体 | 绿地三层架构 |
| Worker 生命周期 | 常驻 EC2 | 本地子进程 | **临时 EC2，用完即毁** |
| 通信方式 | SSH (paramiko) | 本地 stdout | **SQS 双向 + S3** |
| 账号管理 | 完整（OAuth 自动登录 + 额度监控） | 无 | 复用 audiobook-pool（OAuth + watchdog） |
| 接入模式 | **替换**自建基础设施 | **扩展**到分布式 | **绿地构建** |
| 任务粒度 | 多阶段研究（小时级） | 短任务（分钟级） | **10-Phase 做书（1-2 小时）** |
| 用户交互 | 飞书 Bot 双向 | Web UI 单向 | **Web UI 双向聊天** |
| 崩溃恢复 | 无 | 无 | **自动检测 + S3 恢复 + /continue-book** |

---

## 2. 为什么 Audiobook 需要 Elastic-Agent

### 2.1 自建基础设施的代价

如果 Audiobook 不使用 Elastic-Agent，需要自建以下基础设施：

| 需要自建的能力 | 预估代码量 | 复杂度 |
|---------------|----------|--------|
| EC2 生命周期管理（boto3 + 状态轮询 + 安全网） | ~400 行 | 中 |
| Bootstrap Pipeline（user_data 生成 + 错误处理） | ~300 行 | 中 |
| 凭证安全分发（Secrets Manager → EC2） | ~200 行 | 低 |
| 号池管理（账号分配 + 额度监控 + 轮换） | ~500 行 | 高 |
| 崩溃恢复（心跳检测 + 新 EC2 + S3 恢复） | ~300 行 | 高 |
| 扩缩容逻辑（并发任务 → EC2 数量） | ~200 行 | 中 |
| **合计** | **~1900 行** | |

这 ~1900 行基础设施代码与做书的核心业务完全无关，且每个新项目都需要重复实现。

### 2.2 Elastic-Agent 的价值

```
不使用框架:                              使用框架:
┌───────────────────────┐              ┌───────────────────────┐
│ Audiobook 后端         │              │ Audiobook 后端         │
│                       │              │                       │
│ ┌─────────────────┐   │              │ ┌─────────────────┐   │
│ │ 做书业务逻辑     │   │              │ │ 做书业务逻辑     │   │
│ │ (TaskManager,   │   │              │ │ (TaskManager,   │   │
│ │  StreamHub,     │   │              │ │  StreamHub,     │   │
│ │  ChatService)   │   │              │ │  ChatService)   │   │
│ └─────────────────┘   │              │ └─────────────────┘   │
│ ┌─────────────────┐   │              │ ┌─────────────────┐   │
│ │ 自建基础设施     │   │              │ │ Elastic-Agent    │   │
│ │ (~1900 行)      │   │  ═替换为═▶   │ │ (~200 行 Harness │   │
│ │ EC2 管理        │   │              │ │  + 框架 API 调用) │   │
│ │ 凭证管理        │   │              │ │                  │   │
│ │ 额度监控        │   │              │ │ + 获得:          │   │
│ │ 崩溃恢复        │   │              │ │   IP 亲和性      │   │
│ │ 扩缩容          │   │              │ │   优雅缩容       │   │
│ └─────────────────┘   │              │ │   健康检查       │   │
│                       │              │ │   自动扩缩容     │   │
└───────────────────────┘              │ └─────────────────┘   │
                                       └───────────────────────┘
```

| 问题 | 自建 | Elastic-Agent |
|------|------|---------------|
| EC2 管理 | 手写 boto3 + 状态轮询 + 安全网 | 框架 CloudProvider 标准接口 |
| 凭证安全 | 手写 Secrets Manager 集成 | 框架 CredentialPool + 安全传递通道 |
| 额度监控 | 三层自建（watchdog → watcher → monitor） | 框架内置 QuotaMonitor |
| 崩溃恢复 | 手写心跳检测 + 恢复流程 | 框架 Worker Runtime 内置 L2/L3 健康检查 |
| 扩缩容 | 手写规则 | 框架 ScalingEngine + ScalingSignal |
| IP 亲和 | 无 | 框架内置，减少账号风控风险 |
| 优雅缩容 | 无（直接 terminate，做书中途被杀） | 框架 Drain 机制 |
| 代码量 | ~1900 行基础设施 | ~200 行 Harness 定义 |

---

## 3. 基于框架的架构设计

### 3.1 使用框架前后对比

```
使用框架前 (SOLUTION.md 的设计):          使用框架后:
┌─────────────────────┐                 ┌─────────────────────┐
│ 后端 Dispatcher      │                 │ 后端 Dispatcher      │
│                     │                 │                     │
│ TaskManager         │                 │ TaskManager         │
│ StreamHub           │                 │ StreamHub           │
│ ChatService         │                 │ ChatService         │
│ EC2Manager (自建)   │  ══替换══▶      │                     │
│ HealthChecker (自建)│                 │ Elastic-Agent       │
│ PoolMonitor (自建)  │                 │ Framework           │
│                     │                 │ (CloudProvider +    │
│ user_data 脚本生成  │                 │  CredentialPool +   │
│ 心跳超时检测        │                 │  QuotaMonitor +     │
│ 恢复流程编排        │                 │  BootstrapPipeline) │
└─────────┬───────────┘                 └─────────┬───────────┘
          │ SQS + SSH                             │ Worker Runtime
     ┌────▼────┐                             ┌────▼────┐
     │EC2      │                             │Worker   │
     │         │                             │┌──────┐ │
     │user_data│                             ││W.R.  │ │  ← 框架 Runtime
     │脚本启动  │                             │└──┬───┘ │
     │         │                             │   │     │
     │Worker   │                             │Worker   │
     │Agent    │                             │Agent    │  ← 业务代码
     └─────────┘                             └─────────┘
```

### 3.2 保留与替换的边界

| 模块 | 操作 | 理由 |
|------|------|------|
| `EC2Manager` | **替换** → `elastic_agent.CloudProvider` | 框架标准 EC2 管理 |
| `user_data.py` 脚本生成 | **替换** → Harness Bootstrap Pipeline | 框架有失败处理 + 回滚 |
| `HealthChecker` | **替换** → 框架 Worker Runtime 健康检查 | 框架内置 L2/L3 |
| `PoolMonitorService` | **替换** → 框架 QuotaMonitor | 框架统一监控 |
| 安全网 (EventBridge) | **替换** → 框架孤儿实例检测 | 框架内置 |
| `StreamHub` | **保留** | 做书特有的聊天中继逻辑 |
| `ChatService` | **保留** | 双向聊天是业务特有 |
| `TaskManager` | **保留** | 做书任务管理是业务特有 |
| `WorkerAgent` | **保留** | 做书核心编排（stream-json 解析、Phase 检测、账号切换） |
| `StreamReporter` | **保留** | 0.5s 攒批 SQS 上报是业务优化 |
| `FileSyncer` | **保留** | 3s 文件同步是业务需求 |
| `PoolStateWatcher` | **适配** | 保留事件检测逻辑，上报方式适配框架 Worker Runtime |
| `claude-pool-watchdog` | **保留** | AMI 预装的 Shell 脚本，本地采集不变 |
| 前端聊天系统 | **保留** | 做书特有的 UI |

### 3.3 框架化后的架构

```
┌─────────────────────────────────────────────────────────────┐
│                     Manager 节点（后端服务器）                 │
│                                                             │
│  ┌───────────────────────────────────────────────────────┐  │
│  │            Audiobook 业务层（保留）                      │  │
│  │  TaskManager · StreamHub · ChatService · Vue 前端      │  │
│  │  SQS 消息协议 · WebSocket 双向聊天 · S3 文件同步       │  │
│  └───────────────────────┬───────────────────────────────┘  │
│                          │ 调用框架 API                      │
│  ┌───────────────────────▼───────────────────────────────┐  │
│  │             Elastic-Agent 框架层                        │  │
│  │                                                       │  │
│  │  CloudProvider  CredentialPool   QuotaMonitor          │  │
│  │  (boto3 EC2)    (Claude 账号池)   (额度监控)           │  │
│  │                                                       │  │
│  │  NodeRegistry   BootstrapPipeline  WorkerRuntime       │  │
│  │  (节点注册)     (初始化管道)       (远程执行+日志)     │  │
│  │                                                       │  │
│  │  IPAffinity     DrainPolicy       ScalingEngine        │  │
│  │  (IP 亲和)      (优雅缩容)        (扩缩容规则)        │  │
│  └───────────────────────┬───────────────────────────────┘  │
└──────────────────────────┼──────────────────────────────────┘
                           │ Worker Runtime Protocol
             ┌─────────────┼─────────────┐
             │             │             │
        ┌────▼────┐   ┌────▼────┐   ┌────▼────┐
        │Worker 1 │   │Worker 2 │   │Worker N │
        │┌───────┐│   │┌───────┐│   │┌───────┐│
        ││Worker ││   ││Worker ││   ││Worker ││
        ││Runtime││   ││Runtime││   ││Runtime││  ← 框架提供
        │└───┬───┘│   │└───┬───┘│   │└───┬───┘│
        │┌───▼───┐│   │┌───▼───┐│   │┌───▼───┐│
        ││Worker ││   ││Worker ││   ││Worker ││
        ││Agent  ││   ││Agent  ││   ││Agent  ││  ← 做书业务代码
        │└───────┘│   │└───────┘│   │└───────┘│
        │┌───────┐│   │┌───────┐│   │┌───────┐│
        ││Claude ││   ││Claude ││   ││Claude ││
        ││Code   ││   ││Code   ││   ││Code   ││
        │└───────┘│   │└───────┘│   │└───────┘│
        └─────────┘   └─────────┘   └─────────┘
        用完即毁        用完即毁        用完即毁
```

---

## 4. 模块映射：自建 vs 框架

### 4.1 EC2 生命周期管理

```python
# ── 自建方案（SOLUTION.md 中的设计） ──

# backend/services/ec2_manager.py
class EC2Manager:
    async def launch_worker(self, task: Task) -> str:
        # 手动构建 user_data
        user_data = build_user_data(task, downstream_url=downstream_url)
        resp = self.ec2.run_instances(
            LaunchTemplate={"LaunchTemplateName": "audiobook-worker-template"},
            UserData=base64.b64encode(user_data.encode()).decode(),
            # ...
        )
        return resp['Instances'][0]['InstanceId']

    async def poll_workers(self):
        # 手动每 30 秒轮询 EC2 状态
        ...

# ── 使用框架后 ──

from elastic_agent import AWSEc2Provider, InstanceConfig

provider = AWSEc2Provider(
    region="us-east-1",
    ami_id="ami-audiobook-worker-v1",
    default_instance_type="m5.xlarge",
    key_pair_name="audiobook-workers",
    security_group_ids=["sg-xxxxx"],
    subnet_id="subnet-xxxxx",
    instance_initiated_shutdown="terminate",
)

# 框架自动处理：状态轮询、孤儿检测、安全网
instance = await provider.create_instance(InstanceConfig(
    name=f"audiobook-{task.id}",
    tags={"TaskId": task.id, "Project": "audiobook"},
))
```

### 4.2 Bootstrap Pipeline

```python
# ── 自建方案：一个巨大的 user_data bash 脚本 ──

# 约 150 行 bash，包含 6 个阶段：
# 阶段 1: 拉取凭证 (Secrets Manager)
# 阶段 2: 自动登录所有账号 (Playwright)
# 阶段 3: 启动 Watchdog
# 阶段 4: 下载 PDF + 准备环境
# 阶段 5: 启动 Worker Agent
# 阶段 6: 上传产物 + 自毁
# 无失败回滚，某步失败整个脚本中断

# ── 使用框架后：Harness Bootstrap 步骤（见第 5 节） ──
# 每步有 execute + rollback，框架处理失败恢复
```

### 4.3 凭证与号池管理

```python
# ── 自建方案 ──

# 1. user_data 中从 Secrets Manager 手动拉取
ACCOUNTS_JSON=$(aws secretsmanager get-secret-value ...)
# 2. 手动调用 account_login.py 登录每个账号
# 3. watchdog shell 脚本本地监控
# 4. PoolStateWatcher Python 协程 SQS 上报
# 5. PoolMonitorService 后端消费入库告警
# 三层架构，~500 行代码

# ── 使用框架后 ──

from elastic_agent import CredentialPool, ClaudeOAuthProvider

pool = CredentialPool(
    provider=ClaudeOAuthProvider(
        email_tokens=load_email_tokens(),
        # Playwright 登录逻辑从 account_login.py 适配而来
    ),
    affinity_policy="prefer_same_ip",
    quota_threshold=0.85,
    rotation_strategy="least_used_first",
)

# 框架自动处理：
# - 安全凭证传递到 Worker
# - 额度监控（内置 watchdog 等价功能）
# - 告警阈值触发事件
```

### 4.4 崩溃恢复

```python
# ── 自建方案 ──

# backend/services/health_checker.py
class HealthChecker:
    HEARTBEAT_TIMEOUT = 120  # 2 分钟
    async def check_loop(self):
        # 每 30 秒检查心跳超时
        # 二次确认 EC2 状态
        # 手动编排恢复流程：
        #   terminate 旧 EC2 → 删旧 SQS → 启新 EC2(recovery_mode) → 更新 DB
    async def recover_task(self, task):
        # ~50 行恢复编排代码

# ── 使用框架后 ──

# 框架 Worker Runtime 内置：
# - L2 健康检查：进程存活
# - L3 健康检查：业务心跳（Worker Agent 上报）
# - 自动重启/重建流程
# Harness 只需实现 on_node_unhealthy 事件处理器
```

---

## 5. Harness 接口实现

### 5.1 AudiobookHarness 定义

```python
from elastic_agent import (
    Harness, BootstrapStep, ServiceDefinition,
    ScalingSignal, FrameworkEvent, EventHandler,
)

class AudiobookHarness(Harness):
    """有声书稿全自动化生产系统的 Elastic-Agent Harness 实现"""

    def __init__(self, config: dict):
        self.config = config

    def get_repo_url(self) -> str | None:
        # Worker Agent 代码已在 AMI 中预装，无需 clone
        # 如需动态部署最新版本可返回 repo URL
        return None

    def get_bootstrap_steps(self) -> list[BootstrapStep]:
        return [
            FetchCredentialsStep(),         # 从 Secrets Manager 拉取账号凭证
            SetupAccountConfigDirsStep(),   # 为每个账号创建 config 目录
            AutoLoginAccountsStep(),        # Playwright 自动登录所有账号
            StartWatchdogStep(),            # 启动 claude-pool-watchdog
            DownloadBookPDFStep(),          # 从 S3 下载书稿 PDF
            PrepareWorkspaceStep(),         # 准备工作目录 + 确认插件可用
            StartWorkerAgentStep(),         # 启动 worker_agent.py（systemd）
        ]

    def get_service_definitions(self) -> list[ServiceDefinition]:
        return [
            ServiceDefinition(
                name="claude-pool-watchdog",
                command="claude-pool-watchdog -l /tmp/watchdog.log",
                restart_policy="always",
            ),
            ServiceDefinition(
                name="audiobook-worker",
                command=(
                    "python3 /home/ubuntu/worker/worker_agent.py "
                    "--task-id {task_id} "
                    "--book-path /home/ubuntu/workspace/book.pdf "
                    "--book-slug {book_slug} "
                    "--book-title '{book_title}' "
                    "--sqs-upstream-url {sqs_upstream_url} "
                    "--sqs-downstream-url {sqs_downstream_url} "
                    "--s3-work-prefix {s3_work_prefix} "
                    "--s3-result-prefix {s3_result_prefix}"
                ),
                restart_policy="on-failure",
                env={
                    "TASK_ID": "{task_id}",
                    "BOOK_SLUG": "{book_slug}",
                },
                working_directory="/home/ubuntu/workspace",
            ),
        ]

    def get_app_credentials(self) -> list[str]:
        return [
            "audiobook_pool_accounts",    # Claude 账号配置 JSON
            "audiobook_pool_email_tokens", # 171mail API tokens
        ]

    def get_health_check(self) -> dict:
        return {
            "type": "heartbeat",
            "source": "sqs",                  # Worker 通过 SQS 上报心跳
            "interval": 30,
            "timeout": 120,                   # 2 分钟无心跳判定异常
        }

    def get_scaling_signal(self) -> ScalingSignal:
        pending = self._count_pending_tasks()
        running = self._count_running_workers()
        return ScalingSignal(
            pending_tasks=pending,
            idle_workers=0,            # Audiobook Worker 用完即毁，无空闲
            busy_workers=running,
        )

    def get_drain_policy(self) -> dict:
        return {
            "grace_period": 7200,      # 做书任务最长 2 小时
            "save_state": True,        # 缩容前上传工作目录到 S3
        }

    def get_event_handlers(self) -> dict:
        return {
            FrameworkEvent.NODE_READY: self._on_node_ready,
            FrameworkEvent.NODE_UNHEALTHY: self._on_node_unhealthy,
            FrameworkEvent.NODE_DRAIN_START: self._on_drain_start,
            FrameworkEvent.CREDENTIAL_EXHAUSTED: self._on_credential_exhausted,
            FrameworkEvent.QUOTA_WARNING: self._on_quota_warning,
            FrameworkEvent.WORKER_COMPLETED: self._on_worker_completed,
        }

    # ── 事件处理器 ──

    async def _on_node_ready(self, data: dict):
        """Worker 就绪后，更新任务状态"""
        task_id = data.get("task_metadata", {}).get("task_id")
        if task_id:
            await self.db.execute(
                "UPDATE tasks SET status='running', instance_ip=? WHERE id=?",
                [data["private_ip"], task_id],
            )

    async def _on_node_unhealthy(self, data: dict):
        """Worker 异常，触发恢复流程"""
        task_id = data.get("task_metadata", {}).get("task_id")
        if not task_id:
            return

        # 通知前端
        await self.stream_hub.broadcast(task_id, {
            "msg_type": "system",
            "content": {"text": "EC2 实例异常，正在自动恢复..."},
        })

        # 框架自动处理：terminate 旧实例 → 启新实例 → 重新 Bootstrap
        # Harness 只需告诉框架如何恢复业务状态：
        return {
            "recovery_action": "restart_with_state",
            "state_restore": {
                "s3_prefix": f"s3://audiobook-production/tasks/{task_id}/work/",
                "target_path": f"/home/ubuntu/workspace/.work/{data.get('book_slug', '')}/",
            },
            "resume_command": f"/continue-book {data.get('book_slug', '')}",
        }

    async def _on_drain_start(self, data: dict):
        """优雅缩容：通知 Worker 保存进度"""
        task_id = data.get("task_metadata", {}).get("task_id")
        if task_id:
            # 通过下行 SQS 发送 pause 命令
            await self.sqs_send_downstream(task_id, {
                "command": "pause",
                "payload": {"reason": "scaling_in"},
            })

    async def _on_credential_exhausted(self, data: dict):
        """所有账号额度耗尽"""
        await self.alert_sender.send({
            "title": "号池告警：所有账号额度已耗尽",
            "detail": data,
        })

    async def _on_quota_warning(self, data: dict):
        """额度告警"""
        account_id = data.get("account_id", "")
        pct = data.get("five_hour_pct", 0)
        await self.alert_sender.send({
            "title": f"号池告警: {account_id} 额度 {pct:.0f}%",
            "detail": data,
        })

    async def _on_worker_completed(self, data: dict):
        """Worker 正常完成，清理资源"""
        task_id = data.get("task_metadata", {}).get("task_id")
        if task_id:
            await self.db.execute(
                "UPDATE tasks SET status='completed', completed_at=? WHERE id=?",
                [datetime.utcnow(), task_id],
            )
            # 删除 per-task 下行 SQS 队列
            downstream_url = await self.db.fetchval(
                "SELECT downstream_queue_url FROM tasks WHERE id=?", [task_id]
            )
            if downstream_url:
                self.sqs.delete_queue(QueueUrl=downstream_url)

    # ── 辅助方法 ──

    def _count_pending_tasks(self) -> int:
        return self.db.fetchval(
            "SELECT COUNT(*) FROM tasks WHERE status = 'queued'"
        ) or 0

    def _count_running_workers(self) -> int:
        return self.db.fetchval(
            "SELECT COUNT(*) FROM tasks WHERE status IN ('running', 'switching')"
        ) or 0
```

### 5.2 各 Bootstrap 步骤实现

```python
class FetchCredentialsStep(BootstrapStep):
    name = "fetch-credentials"

    async def execute(self, ctx):
        """从 Secrets Manager 拉取账号凭证到 Worker"""
        accounts_json = await ctx.secrets.get("audiobook_pool_accounts")
        email_tokens = await ctx.secrets.get("audiobook_pool_email_tokens")

        await ctx.ssh.run("mkdir -p ~/.claude-pool && chmod 700 ~/.claude-pool")
        await ctx.ssh.write_file("~/.claude-pool/accounts.json", accounts_json)
        await ctx.ssh.write_file("~/.claude-pool/email_tokens.json", email_tokens)
        await ctx.ssh.run("chmod 600 ~/.claude-pool/email_tokens.json")

    async def rollback(self, ctx):
        await ctx.ssh.run("rm -rf ~/.claude-pool")


class SetupAccountConfigDirsStep(BootstrapStep):
    name = "setup-account-config-dirs"

    async def execute(self, ctx):
        """为每个账号创建 CLAUDE_CONFIG_DIR"""
        await ctx.ssh.run("""
            python3 -c "
import json, os, pathlib
with open(os.path.expanduser('~/.claude-pool/accounts.json')) as f:
    cfg = json.load(f)
for acc in cfg['accounts']:
    d = os.path.expandvars(acc['config_dir'])
    pathlib.Path(d).mkdir(parents=True, exist_ok=True)
"
        """)


class AutoLoginAccountsStep(BootstrapStep):
    name = "auto-login-accounts"

    async def execute(self, ctx):
        """Playwright 自动登录所有 Claude Code 账号"""
        result = await ctx.ssh.run(
            "python3 /home/ubuntu/.claude-pool/lib/batch_login.py "
            "--accounts ~/.claude-pool/accounts.json "
            "--tokens ~/.claude-pool/email_tokens.json",
            timeout=300,  # 登录可能需要较长时间
        )
        if result.returncode != 0:
            ctx.log(f"部分账号登录失败: {result.stderr}")
            # 不中断 — 只要有一个账号登录成功就行

    async def rollback(self, ctx):
        # 登录失败时无需回滚，凭证已在上一步被清理
        pass


class StartWatchdogStep(BootstrapStep):
    name = "start-watchdog"

    async def execute(self, ctx):
        """启动 claude-pool-watchdog 额度监控"""
        await ctx.ssh.run("nohup claude-pool-watchdog -l /tmp/watchdog.log &")
        # 等待 watchdog 首次写出状态文件
        await ctx.ssh.run(
            "for i in $(seq 1 10); do "
            "  [ -f /tmp/claude_pool_status.json ] && break; "
            "  sleep 1; "
            "done"
        )


class DownloadBookPDFStep(BootstrapStep):
    name = "download-book-pdf"

    async def execute(self, ctx):
        """从 S3 下载书稿 PDF"""
        book_s3_path = ctx.task_metadata["book_s3_path"]
        await ctx.ssh.run(f"mkdir -p /home/ubuntu/workspace")
        await ctx.ssh.run(
            f"aws s3 cp {book_s3_path} /home/ubuntu/workspace/book.pdf",
            timeout=120,
        )

    async def rollback(self, ctx):
        await ctx.ssh.run("rm -f /home/ubuntu/workspace/book.pdf")


class PrepareWorkspaceStep(BootstrapStep):
    name = "prepare-workspace"

    async def execute(self, ctx):
        """准备工作目录，确认做书插件可用"""
        book_slug = ctx.task_metadata["book_slug"]

        # 确认 audiobook-nonfiction 插件已安装
        result = await ctx.ssh.run(
            "ls /home/ubuntu/audiobook-nonfiction/skills/audiobook-nonfiction/SKILL.md"
        )
        if result.returncode != 0:
            raise RuntimeError("audiobook-nonfiction 插件未安装，AMI 可能有问题")

        # 如果是恢复模式，从 S3 恢复工作目录
        if ctx.task_metadata.get("recovery_mode"):
            s3_work_prefix = ctx.task_metadata["s3_work_prefix"]
            await ctx.ssh.run(
                f"mkdir -p /home/ubuntu/workspace/.work/{book_slug} && "
                f"aws s3 sync {s3_work_prefix}/ "
                f"/home/ubuntu/workspace/.work/{book_slug}/ --quiet",
                timeout=300,
            )


class StartWorkerAgentStep(BootstrapStep):
    name = "start-worker-agent"

    async def execute(self, ctx):
        """启动 Worker Agent 主控进程"""
        meta = ctx.task_metadata
        cmd = (
            f"python3 /home/ubuntu/worker/worker_agent.py "
            f"--task-id {meta['task_id']} "
            f"--book-path /home/ubuntu/workspace/book.pdf "
            f"--book-slug {meta['book_slug']} "
            f"--book-title '{meta['book_title']}' "
            f"--sqs-upstream-url {meta['sqs_upstream_url']} "
            f"--sqs-downstream-url {meta['sqs_downstream_url']} "
            f"--s3-work-prefix {meta['s3_work_prefix']} "
            f"--s3-result-prefix {meta['s3_result_prefix']}"
        )

        if meta.get("target_word_count_pct"):
            cmd += f" --target-word-count-pct {meta['target_word_count_pct']}"
        if meta.get("persona", "nonfiction_default") != "nonfiction_default":
            cmd += f" --persona {meta['persona']}"

        # 用 systemd 启动（框架 ServiceDefinition 也可以处理这个）
        unit = f"""
[Unit]
Description=Audiobook Worker Agent
After=network.target

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/home/ubuntu/workspace
ExecStart={cmd}
Restart=on-failure
RestartSec=5
Environment=DISPLAY=

[Install]
WantedBy=multi-user.target
"""
        await ctx.ssh.write_file(
            "/etc/systemd/system/audiobook-worker.service", unit
        )
        await ctx.ssh.run(
            "sudo systemctl daemon-reload && sudo systemctl enable --now audiobook-worker"
        )
```

### 5.3 Manager 侧集成

```python
# backend/main.py — 初始化 Elastic-Agent

from elastic_agent import ElasticAgentManager, AWSEc2Provider, CredentialPool
from audiobook_harness import AudiobookHarness

elastic = ElasticAgentManager(
    provider=AWSEc2Provider(
        region="us-east-1",
        ami_id="ami-audiobook-worker-v1",
        instance_type="m5.xlarge",
        key_pair_name="audiobook-workers",
        security_group_ids=["sg-xxxxx"],
        subnet_id="subnet-xxxxx",
        iam_instance_profile="audiobook-worker-role",
        instance_initiated_shutdown="terminate",
    ),
    credential_pool=CredentialPool(
        provider=ClaudeOAuthProvider(email_tokens=load_email_tokens()),
        affinity_policy="prefer_same_ip",
        quota_threshold=0.85,
    ),
    harness=AudiobookHarness(config),
)

# 任务 API（业务逻辑不变，只是 EC2 管理委托给框架）

@app.post("/api/tasks")
async def create_task(req: TaskCreateRequest):
    # 1. 上传 PDF 到 S3
    # 2. 写 DB
    # 3. 创建 per-task 下行 SQS 队列
    downstream_url = sqs.create_queue(
        QueueName=f"audiobook-down-{task.id}",
        Attributes={"MessageRetentionPeriod": "3600"},
    )["QueueUrl"]
    
    # 4. 通过框架启动 Worker（替换原来的 ec2_manager.launch_worker）
    node = await elastic.scale_out(
        count=1,
        task_metadata={
            "task_id": task.id,
            "book_slug": req.book_slug,
            "book_title": req.book_title,
            "book_s3_path": s3_path,
            "sqs_upstream_url": UPSTREAM_SQS_URL,
            "sqs_downstream_url": downstream_url,
            "s3_work_prefix": f"s3://audiobook-production/tasks/{task.id}/work",
            "s3_result_prefix": f"s3://audiobook-production/tasks/{task.id}/result",
            "target_word_count_pct": req.target_word_count_pct,
            "persona": req.persona,
        },
    )
    
    # 5. 更新 DB
    await db.execute(
        "UPDATE tasks SET status='launching', instance_id=?, downstream_queue_url=? WHERE id=?",
        [node[0].id, downstream_url, task.id],
    )
    return {"task_id": task.id}
```

---

## 6. 分步实施方案

### Phase 0：环境准备（1-2 天）

1. 制作 AMI：预装 Claude Code + Playwright + mitmproxy + audiobook-pool + audiobook-nonfiction
2. 创建 AWS 资源：S3 Bucket、SQS 上行队列、Secrets Manager、IAM Role、Security Group
3. 在 Elastic-Agent 框架中实现 `AudiobookHarness` 基本骨架
4. 手动测试：通过框架创建一台 EC2，验证 AMI 和 Bootstrap 步骤

### Phase 1：Worker Agent 核心（3-5 天）

1. 实现 `WorkerAgent`（Claude 子进程管理 + stream-json 解析 + Phase 检测）
2. 实现 `StreamReporter`（0.5s 攒批 SQS 上报）
3. 实现 `FileSyncer`（3s 扫描 + S3 上传）
4. 实现账号切换逻辑（rate limit 检测 + hardlink + resume）
5. 端到端测试：手动启动 EC2 → 跑通一本书 → 实时消息可在 SQS 看到

### Phase 2：后端 Dispatcher（3-5 天）

1. FastAPI 框架 + SQLite 数据库 + 数据模型
2. 任务 CRUD API + Elastic-Agent 集成
3. `StreamHub`（消费上行 SQS → 写 DB → WebSocket 推送）
4. `ChatService`（WebSocket 双向连接 + 下行 SQS 投递）
5. per-task 下行 SQS 队列创建/销毁
6. 端到端测试：通过 API 创建任务 → Worker 自动启动 → 消息实时到达后端

### Phase 3：前端 UI（2-3 天）

1. 任务提交页面
2. `useChatManager`：多任务切换 + WebSocket 实时聊天
3. `ChatStream.vue`：聊天气泡渲染（文本 + 工具卡片 + 系统消息）
4. `PhaseProgress.vue`：10 Phase 进度条
5. `FileExplorer.vue`：中间文件浏览器
6. 用户消息发送功能

### Phase 4：高级功能（2-3 天）

1. 崩溃恢复：框架 `NODE_UNHEALTHY` → 新 EC2 → S3 恢复 → `/continue-book`
2. 三层号池监控：watchdog → PoolStateWatcher → PoolMonitorService
3. 扩缩容规则：pending 任务数 > 0 → 扩容，任务完成 → 自动销毁
4. IP 亲和性：同一账号优先分配到之前使用过的 IP
5. 前端号池面板

### Phase 5：集成测试与优化（2-3 天）

1. 端到端全流程：前端提交 → EC2 做书 → 实时聊天 → 完成
2. 测试账号切换（模拟限流）
3. 测试崩溃恢复（kill EC2）
4. 测试用户双向交互（Phase 8 合规决策）
5. 延迟优化验证（目标：Claude 输出 → 前端气泡 ≤1.7 秒）

**总计：约 12-18 天**（与自建方案 11-18 天相近，但获得了 IP 亲和、优雅缩容、标准健康检查等额外能力）

---

## 7. 技术细节与挑战

### 7.1 双向聊天与 Claude Code `--print` 模式的矛盾

**挑战**：Claude Code `--print` 模式是单次执行，不接受运行中注入输入。用户在前端发的消息无法"中断"正在运行的 Claude。

**解决方案**：Worker 在每次 Claude Code 运行结束后，检查下行 SQS 是否有用户消息。有的话用 `--resume` + 用户消息作为新 prompt 启动新一轮。

```python
# WorkerAgent 主循环伪代码
while True:
    exit_code, session_id = run_claude_with_streaming(prompt)
    
    if is_rate_limited():
        switch_account()
        prompt = "/continue-book {slug}"
        continue
    
    user_commands = poll_downstream_sqs()
    if user_commands:
        prompt = user_commands[-1]["text"]
        continue  # --resume + 用户消息
    
    if is_waiting_for_user():
        msg = wait_for_user_message(timeout=300)
        if msg:
            prompt = msg
            continue
    
    break  # 正常完成
```

**延迟分析**：
- Claude 已停下等用户：~1-3 秒（SQS 传输 + Worker 轮询）
- Claude 还在运行中：等当前轮次结束（前端可看到 Claude 在忙，用户有预期）

**框架可提供的帮助**：Worker Runtime 如果支持"信号注入"接口（不杀进程，只是标记有新消息），Worker Agent 可以在 Claude 自然停顿点（如工具调用之间）检查消息，减少等待时间。

### 7.2 SQS 消息排序

**挑战**：SQS Standard Queue 不保证消息顺序。聊天气泡乱序会导致前端显示混乱。

**解决方案**：Worker 端维护 `seq` 单调递增计数器，前端按 `seq` 排序。成本比 SQS FIFO 低得多（FIFO 限制 300 TPS 且贵 2x）。

**框架可提供的帮助**：框架的日志传输通道如果内置序号机制，Harness 无需自己维护 `seq`。

### 7.3 临时 EC2 的通信挑战

**挑战**：与 agent-ml-research 的常驻 EC2 不同，Audiobook Worker 是临时的（用完即毁）。无法用 SSH 轮询状态，无法部署常驻 HTTP 服务。

**解决方案**：全部用 SQS 推送模式。Worker 主动上报，后端被动消费。

**与 agent-ml-research 的关键区别**：
- agent-ml-research：Manager 主动 SSH 拉取状态（pull）
- Audiobook：Worker 通过 SQS 主动推送（push）
- 框架 Worker Runtime 可以统一抽象这两种模式

### 7.4 跨 EC2 崩溃恢复的限制

**挑战**：新 EC2 没有旧 EC2 的 session.jsonl 文件，`--resume` 无法恢复完整对话上下文。

**解决方案**：接受限制，用 `/continue-book` 从 `state.json` + 中间文件恢复。Claude 需要重新理解上下文（约浪费 5-10 分钟），但比从头重做（1-2 小时）好得多。

**三层恢复保障**：
1. **S3 文件**：FileSyncer 每 3 秒同步，最多丢 3 秒内的文件变更
2. **state.json**：做书 Skill 自己维护的断点信息
3. **session_id**：存在 DB 中，但跨 EC2 时 .jsonl 文件丢失

**框架可提供的帮助**：如果框架支持 Worker 状态持久化到 S3（包括 session.jsonl），跨 EC2 恢复时可以完整恢复对话上下文。

### 7.5 AMI 依赖复杂度

**挑战**：AMI 需要预装大量依赖——Claude Code CLI + Playwright + Chromium + mitmproxy + audiobook-pool + audiobook-nonfiction 插件。AMI 更新频繁（插件版本变化、Claude Code 升级）。

**解决方案**：
- 基础 AMI：系统依赖 + Playwright + Chromium（变化少）
- Bootstrap 时：动态安装最新 Claude Code + 动态部署最新 Worker 代码
- 做书插件通过 git pull 获取最新版本

**框架可提供的帮助**：Bootstrap Pipeline 的分层缓存——基础 AMI 只装系统依赖，框架在 Bootstrap 时安装应用依赖，支持缓存到 AMI snapshot 加速后续启动。

### 7.6 低延迟实时聊天的攒批策略

**挑战**：逐条发 SQS 延迟低但成本高（每条 ~50ms × 高频消息 = 大量请求）；全量攒批延迟高但便宜。

**解决方案**：StreamReporter 混合策略：
- 普通消息：0.5s 攒批窗口，`send_message_batch` 最多 10 条/次
- 高优先级消息（phase_change / account_switch / error / user_input）：立即 flush

端到端延迟：Claude 输出 → 前端气泡 **0.7-1.7 秒**（0.1s tail-read + 0.5s 攒批 + 0.05s SQS + 1s 后端轮询 + 0.01s WebSocket）。

### 7.7 改造量评估

| 模块 | 工作量 | 说明 |
|------|--------|------|
| `AudiobookHarness` | 新开发 ~300 行 | 7 个 Bootstrap 步骤 + 事件处理器 |
| `WorkerAgent` | 新开发 ~500 行 | 核心做书编排，不依赖框架 |
| `StreamReporter` | 新开发 ~100 行 | SQS 攒批上报 |
| `FileSyncer` | 新开发 ~80 行 | S3 文件同步 |
| `StreamHub` | 新开发 ~150 行 | SQS → DB → WebSocket |
| `ChatService` | 新开发 ~120 行 | 双向聊天管理 |
| `TaskManager` | 新开发 ~100 行 | 任务 CRUD |
| 前端 | 新开发 ~500 行 | 聊天框 + 进度 + 文件浏览 |
| **EC2Manager** | **不需要** | 框架 CloudProvider 替代 |
| **HealthChecker** | **不需要** | 框架 Worker Runtime 替代 |
| **PoolMonitorService** | **不需要** | 框架 QuotaMonitor 替代 |
| **安全网脚本** | **不需要** | 框架孤儿检测替代 |
| **总计** | ~1850 行业务代码 | 省去 ~1900 行基础设施代码 |

---

## 8. Audiobook 对框架提出的需求

### 8.1 Audiobook 特有但值得框架支持的

| 需求 | 说明 | 普适性判断 |
|------|------|-----------|
| **task_metadata 传递** | Bootstrap 步骤需要知道具体任务参数（book_slug、SQS URL 等） | 通用 — 任何按需启动的 Worker 都需要知道"为什么被启动" |
| **per-task 资源创建** | 每个任务需要专属下行 SQS 队列 | 部分通用 — 需要反向通信通道的 Harness 都会遇到 |
| **崩溃恢复的状态还原** | 新 Worker 需要从 S3 恢复上一个 Worker 的工作目录 | 通用 — 任何有状态工作负载的崩溃恢复都需要 |
| **用完即毁的 Worker 模式** | Worker 完成一个任务后立即 terminate，不是常驻服务 | 通用 — batch job 模式 vs long-running 模式 |
| **SQS 推送模式** | 临时 Worker 无法被 Manager pull，只能主动 push | 通用 — 与常驻 SSH/HTTP 的互补模式 |

### 8.2 与 agent-ml-research 和 CCM 的需求交叉验证

| 需求 | agent-ml-research | CCM | Audiobook | 结论 |
|------|-------------------|-----|-----------|------|
| Worker Runtime | ✅ 替换 SSH | ✅ 替换本地子进程 | ✅ 替换 user_data 脚本 | **框架核心** |
| 日志流式传输 | ✅ 飞书告警 | ✅ WebSocket 前端 | ✅ SQS → WebSocket | **框架核心** |
| 有状态亲和性 | ✅ 项目绑定实例 | ✅ session resume | ✅ 账号-IP 亲和 | **框架核心** |
| 优雅缩容 | ✅ 长时间训练 | ✅ 30min 任务 | ✅ 1-2h 做书 | **框架核心** |
| 双层凭证 | ✅ WandB/HF/Feishu | ✅ Git key | ✅ Claude 账号 + 171mail tokens | **框架核心** |
| 扩缩容信号 | ✅ 活跃项目数 | ✅ 任务队列深度 | ✅ 待处理任务数 | **框架核心** |
| 工作区同步 | ✅ git clone/rsync | ✅ Project clone | ✅ S3 sync（崩溃恢复） | **框架核心** |
| Bootstrap 超时 | ✅ uv sync 600s | - | ✅ Playwright 登录 300s | 框架支持 |
| 事件 Webhook | ✅ 飞书 | - | ✅ 钉钉/飞书告警 | 框架支持 |
| task_metadata | - | - | ✅ 任务参数注入 Worker | 框架支持 |
| 用完即毁模式 | - | - | ✅ 临时 Worker | 框架支持 |
| 崩溃状态恢复 | - | - | ✅ S3 → 新 Worker | 框架支持 |

### 8.3 Audiobook 提出的新需求（前两个案例未覆盖）

#### (1) 临时 Worker（Ephemeral Worker）模式

agent-ml-research 和 CCM 的 Worker 都是"创建后常驻、复用多个任务"。Audiobook 的 Worker 是"一个任务一台 EC2，完成后自动销毁"。

**框架应该支持两种 Worker 生命周期模式：**
- `persistent`：创建后常驻，接受多个任务分发
- `ephemeral`：按任务创建，完成后自动销毁

Ephemeral 模式对框架的影响：
- NodeRegistry 需要支持自动注销
- ScalingEngine 的逻辑不同（不是"扩容到 N 台"，而是"每个任务一台"）
- 健康检查的超时阈值更短（Worker 不应该存活超过预期任务时长）

#### (2) 任务级元数据注入

Bootstrap 步骤需要知道具体的任务参数（book_slug、SQS URL、S3 路径等），这些参数在 Worker 创建时才确定。

**框架应该支持 `task_metadata` 传递**：Harness 在调用 `scale_out()` 时传入 metadata dict，框架在 Bootstrap 时通过 `ctx.task_metadata` 暴露给每个步骤。

#### (3) 崩溃恢复的状态还原

Worker 崩溃后，新 Worker 需要从外部存储（S3）恢复前一个 Worker 的工作状态。

**框架应该提供标准的状态快照/恢复接口：**
- Worker Runtime 定期快照指定目录到 S3
- 崩溃恢复时自动还原到新 Worker
- Harness 通过 `get_state_directories()` 声明需要持久化的目录

```python
def get_state_directories(self) -> list[str]:
    return [
        "/home/ubuntu/workspace/.work/{book_slug}/",  # 做书中间文件
    ]
```

#### (4) 反向消息通道（Manager → Worker）

Audiobook 需要从后端向 Worker 发送用户消息和控制命令。当前设计用 per-task SQS 队列实现。

**框架应该内置 Manager → Worker 的消息通道**，而不是让每个 Harness 自己建 SQS 队列。Worker Runtime 已经有 Manager → Worker 的连接（Bootstrap 用的就是这个），可以复用。

### 8.4 三个案例验证的框架核心需求完整度

通过 agent-ml-research（替换）、CCM（扩展）、Audiobook（绿地构建）三个不同接入模式的案例，确认以下 7 项能力是 Elastic-Agent 框架的核心需求：

1. **Worker Runtime**（远程执行 + 日志传输）
2. **日志流式传输**（Worker → Manager → 前端）
3. **有状态亲和性**（session / 项目 / IP 绑定）
4. **优雅缩容**（Drain 机制，不中断长时间任务）
5. **双层凭证管理**（Agent 凭证 + 应用凭证）
6. **扩缩容信号接口**（Harness 上报，框架决策）
7. **工作区同步**（git clone / rsync / S3 sync）

Audiobook 额外提出的 4 项新需求（临时 Worker、task_metadata、崩溃恢复状态还原、反向消息通道）进一步完善了框架设计。详见主文档 [elastic-agent-analysis.md](elastic-agent-analysis.md) 的框架设计完整性审查。
