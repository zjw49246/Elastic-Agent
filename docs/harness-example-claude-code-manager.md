# Harness 应用示例：Claude Code Manager 接入 Elastic-Agent

> 本文档以 [Claude Code Manager](https://github.com/zjw49246/Claude-Code-Manager)（以下简称 CCM）为例，说明一个具体项目如何作为 Harness 接入 Elastic-Agent 弹性计算框架。

---

## 目录

1. [Claude Code Manager 项目解析](#1-claude-code-manager-项目解析)
2. [为什么 CCM 需要 Elastic-Agent](#2-为什么-ccm-需要-elastic-agent)
3. [集成架构设计](#3-集成架构设计)
4. [需要改造的关键点](#4-需要改造的关键点)
5. [Harness 接口实现](#5-harness-接口实现)
6. [分步实施方案](#6-分步实施方案)
7. [技术细节与挑战](#7-技术细节与挑战)

---

## 1. Claude Code Manager 项目解析

### 1.1 项目定位

CCM 是一个 **Web 端的 Claude Code 多任务编排系统**，解决的核心问题是：Claude Code CLI 是一个单进程交互工具，CCM 将其转变为一个可以同时调度多个 Claude Code 实例并行处理软件开发任务的平台。

核心能力：
- 管理一个 Claude Code 子进程工作池（默认最多 5 个并发实例）
- 基于优先级的任务队列 + 自动分发
- WebSocket 实时流式输出每个 Worker 的执行日志
- 支持多轮对话（通过 `--resume` 继续已完成任务的会话）
- 管理项目 Git 仓库、凭证、环境变量
- 提供 Web UI（含 Android App）用于远程管理

### 1.2 系统架构

CCM 是一个 **单机单体应用**，所有组件运行在一台机器上：

```
┌────────────────────────────────────────────────────┐
│                    单台机器                         │
│                                                    │
│  ┌─────────────────────────────────────────────┐   │
│  │          FastAPI 后端 + React 前端            │   │
│  │                                             │   │
│  │  ┌───────────────┐  ┌───────────────────┐   │   │
│  │  │GlobalDispatcher│  │  InstanceManager  │   │   │
│  │  │(调度引擎)      │  │  (进程管理)       │   │   │
│  │  └───────┬───────┘  └────────┬──────────┘   │   │
│  │          │                   │              │   │
│  │  ┌───────▼───────┐  ┌───────▼──────────┐   │   │
│  │  │  TaskQueue    │  │ WebSocket 广播   │   │   │
│  │  │  (SQLite)     │  │ (实时日志流)     │   │   │
│  │  └───────────────┘  └──────────────────┘   │   │
│  └─────────────────────────────────────────────┘   │
│                                                    │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐           │
│  │Claude CLI│ │Claude CLI│ │Claude CLI│  ...       │
│  │进程 #1   │ │进程 #2   │ │进程 #3   │           │
│  │(子进程)  │ │(子进程)  │ │(子进程)  │           │
│  └──────────┘ └──────────┘ └──────────┘           │
└────────────────────────────────────────────────────┘
```

### 1.3 核心模块详解

#### GlobalDispatcher — 调度引擎

中心调度器，每 2 秒轮询一次：

1. 检查是否有空闲的 Instance（Claude Code 进程槽位）
2. 如果空闲 Instance 数量不足 `MAX_CONCURRENT_INSTANCES`，自动创建新 Instance DB 记录
3. 从任务队列中按优先级取出匹配的任务（按 model 匹配）
4. 启动 Claude Code 子进程执行任务

任务生命周期：

```
pending → in_progress → executing → completed
                           │
                           ▼
                     (exit code != 0)
                           │
              retry_count < max_retries?
              /                         \
            yes                         no
             │                           │
          pending                      failed
          (重试)
```

特殊状态：
- `plan_review` — Plan 模式下等待人工审批
- `cancelled` — 用户取消

#### InstanceManager — 进程管理

管理 Claude Code 子进程的生命周期：

```python
# 启动 Claude Code CLI 子进程
cmd = ["claude", "-p", prompt,
       "--dangerously-skip-permissions",
       "--output-format", "stream-json",
       "--verbose"]

# 可选参数
if session_id:  cmd += ["--resume", session_id]
if model:       cmd += ["--model", model]
if effort:      cmd += ["--effort", effort]

proc = await asyncio.create_subprocess_exec(
    *cmd, cwd=project_local_path,
    stdout=asyncio.subprocess.PIPE,
    stderr=asyncio.subprocess.PIPE,
    env=env_with_git_credentials,
)
```

输出消费：
- 逐行读取 stdout（NDJSON 格式）
- StreamParser 解析为结构化事件（system/assistant/user/result/tool_use/tool_result）
- 每个事件存入 `log_entries` 表
- 通过 WebSocket 广播到前端
- 从 `result` 事件中提取 `session_id`、`cost_usd`、`context_usage`

停止流程：SIGINT → 等待 10s → SIGTERM → 等待 5s → SIGKILL

#### TaskQueue — 任务队列

- 优先级数字越小越高（P0 > P1）
- 出队时按 model 匹配（精确匹配优先，然后是未指定 model 的通用任务）
- 支持按 status、project_id、starred、archived 过滤
- 支持分页

#### 任务模式

| 模式 | 说明 |
|------|------|
| `auto` | 标准模式：下发 prompt → Claude Code 自主完成 → 返回结果 |
| `plan` | 计划模式：Claude Code 先产出只读分析计划 → 进入 `plan_review` 等待审批 → 批准后执行 |
| `loop` | 循环模式：Claude Code 反复执行，每次读取 todo 文件，写入信号文件决定继续/停止/中止 |

#### Git 凭证注入

Dispatcher 通过环境变量向子进程注入 Git 凭证：

```python
env["GIT_AUTHOR_NAME"] = git_config.author_name
env["GIT_COMMITTER_NAME"] = git_config.author_name
env["GIT_AUTHOR_EMAIL"] = git_config.author_email
env["GIT_SSH_COMMAND"] = f"ssh -i {ssh_key_path} -o StrictHostKeyChecking=no"
# 或 HTTPS:
env["GIT_ASKPASS"] = temp_script_path  # 临时脚本输出 token
env["GIT_CONFIG_GLOBAL"] = "/dev/null"
env["GIT_CONFIG_NOSYSTEM"] = "1"
```

凭证优先级：项目级 > 全局设置 > 实例级

### 1.4 数据模型

| 表 | 用途 | 关键字段 |
|---|------|---------|
| `tasks` | 任务队列 | id, title, description, status, priority, project_id, mode, session_id, last_cwd, model, effort_level, retry_count |
| `instances` | Claude Code 进程槽 | id, name, status, pid, current_task_id, model, total_cost_usd |
| `projects` | Git 仓库 | id, name, git_url, local_path, git 凭证, tags, env_files |
| `log_entries` | 执行日志 | id, instance_id, task_id, event_type, role, content, tool_name/input/output |
| `global_settings` | 全局 Git 配置 | 单例 (id=1) |
| `secrets` | 敏感数据 | name, content |
| `tags` | 项目标签 | name, color |

### 1.5 技术栈

| 层级 | 技术 |
|------|------|
| 后端 | Python 3.11+, FastAPI, SQLAlchemy (async), Alembic |
| 前端 | React 19, Vite 7, Tailwind CSS v4, TypeScript 5.9, Capacitor (Android) |
| 数据库 | SQLite (默认) / PostgreSQL / MySQL |
| 实时通信 | WebSocket (FastAPI native) |
| CLI 交互 | asyncio.create_subprocess_exec → Claude Code CLI |
| 部署 | uvicorn + systemd，可选 Cloudflare Tunnel |

### 1.6 配置（环境变量）

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `AUTH_TOKEN` | (必填) | API 认证 Bearer Token |
| `PORT` | 8000 | 服务端口 |
| `DATABASE_URL` | `sqlite+aiosqlite:///./claude_manager.db` | 数据库连接 |
| `WORKSPACE_DIR` | `~/Projects` | 项目 clone 目录 |
| `MAX_CONCURRENT_INSTANCES` | 5 | 最大 Claude Code 并发数 |
| `AUTO_START_DISPATCHER` | true | 启动时自动开始调度 |
| `TASK_TIMEOUT_SECONDS` | 1800 | 单任务超时（30 分钟） |

### 1.7 当前局限性

| 局限 | 说明 |
|------|------|
| **单机运行** | 所有 Claude Code 进程都在同一台机器上，无法分布到多台机器 |
| **单账号** | 所有 Claude Code 进程共享同一个登录态，受限于单账号的额度上限 |
| **无弹性扩展** | `MAX_CONCURRENT_INSTANCES` 是固定上限，无法根据负载动态调整 |
| **资源竞争** | 多个 Claude Code 进程争抢同一机器的 CPU/内存 |
| **无账号管理** | 不管理 Claude Code 的登录、额度、账号轮换 |
| **IP 风控风险** | 如果在单机登录多个账号，所有账号共享同一 IP |

---

## 2. 为什么 CCM 需要 Elastic-Agent

### 2.1 单机瓶颈

CCM 当前的单机架构面临几个根本性限制：

**资源瓶颈：** Claude Code CLI 是资源密集型进程，单个实例建议 2 vCPU + 4 GB 内存。在 `t3.large`（2 vCPU / 8 GB）上跑 5 个并发实例已接近极限。如果需要 10-50 个并发任务，单机无法承载。

**额度瓶颈：** Claude Max 订阅有 5 小时滑动窗口的使用量限制。单账号在高并发下很快耗尽额度。需要多账号轮换，但在单机上多账号登录又有风控风险。

**可用性：** 单台机器故障意味着整个系统不可用。

### 2.2 Elastic-Agent 如何解决

```
接入前（CCM 单机）:                   接入后（CCM + Elastic-Agent）:

┌─────────────┐                     ┌─────────────┐
│ 单台机器     │                     │ Manager     │
│             │                     │ CCM 后端    │
│ CCM 后端    │                     │ + Elastic-  │
│ + 5个Claude │                     │   Agent     │
│   Code 进程 │                     └──────┬──────┘
│             │                            │
│ 1个账号     │               ┌────────────┼────────────┐
│ 受限额度    │               │            │            │
└─────────────┘          ┌────▼───┐   ┌────▼───┐   ┌────▼───┐
                         │Worker 1│   │Worker 2│   │Worker N│
                         │EC2     │   │EC2     │   │EC2     │
                         │账号 A  │   │账号 B  │   │账号 C  │
                         │Claude  │   │Claude  │   │Claude  │
                         │ Code   │   │ Code   │   │ Code   │
                         └────────┘   └────────┘   └────────┘
                         可动态扩缩容     每机1账号      自动换号
```

| 问题 | 单机 CCM | CCM + Elastic-Agent |
|------|---------|---------------------|
| 并发数 | 受限于单机资源（5 个） | 可弹性扩展到 N 台机器 |
| 额度 | 单账号，容易耗尽 | 多账号分布在不同机器，自动轮换 |
| 风控 | 多账号同 IP，易封号 | 每台机器 1 个账号，不同 IP |
| 可用性 | 单点故障 | Worker 故障不影响 Manager |
| 成本 | 固定开销 | 按需扩缩，空闲时缩容 |

---

## 3. 集成架构设计

### 3.1 整体架构

```
┌─────────────────────────────────────────────────────────────────┐
│                         Manager 节点                            │
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │                    CCM 后端 (FastAPI)                     │   │
│  │                                                          │   │
│  │  ┌───────────────┐  ┌──────────┐  ┌──────────────────┐  │   │
│  │  │GlobalDispatcher│  │TaskQueue │  │ WebSocket 广播   │  │   │
│  │  │(改造: 远程分发)│  │(不变)    │  │ (改造: 转发远程) │  │   │
│  │  └───────┬───────┘  └──────────┘  └──────────────────┘  │   │
│  │          │                                               │   │
│  │  ┌───────▼───────────────────────────────────────────┐   │   │
│  │  │         InstanceManager (改造: 远程执行)           │   │   │
│  │  │  本地子进程 → SSH/API 远程执行                     │   │   │
│  │  └───────────────────────┬───────────────────────────┘   │   │
│  └──────────────────────────┼───────────────────────────────┘   │
│                             │                                   │
│  ┌──────────────────────────▼───────────────────────────────┐   │
│  │                  Elastic-Agent 模块                       │   │
│  │                                                          │   │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌─────────┐  │   │
│  │  │Node      │  │Credential│  │Quota     │  │Bootstrap│  │   │
│  │  │Provider  │  │Pool      │  │Monitor   │  │Pipeline │  │   │
│  │  │(boto3)   │  │(账号池)  │  │(额度监控)│  │(初始化) │  │   │
│  │  └──────────┘  └──────────┘  └──────────┘  └─────────┘  │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌──────────────────────┐                                       │
│  │  React 前端           │                                       │
│  │  (扩展: 节点管理面板) │                                       │
│  └──────────────────────┘                                       │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                 ┌─────────────┼─────────────┐
                 │             │             │
            ┌────▼────┐  ┌────▼────┐  ┌─────▼───┐
            │Worker 1 │  │Worker 2 │  │Worker N │
            │         │  │         │  │         │
            │Claude   │  │Claude   │  │Claude   │
            │Code CLI │  │Code CLI │  │Code CLI │
            │         │  │         │  │         │
            │Agent    │  │Agent    │  │Agent    │
            │Service  │  │Service  │  │Service  │
            │(汇报+   │  │(汇报+   │  │(汇报+   │
            │ 执行)   │  │ 执行)   │  │ 执行)   │
            └─────────┘  └─────────┘  └─────────┘
```

### 3.2 职责划分

| 组件 | 职责 | 运行位置 |
|------|------|---------|
| CCM 后端 (FastAPI) | 任务管理、调度、Web UI、WebSocket | Manager 节点 |
| Elastic-Agent 模块 | 节点扩缩容、凭证管理、额度监控 | Manager 节点 |
| Worker Agent Service | 接收命令、执行 Claude Code、上报状态和日志 | Worker 节点 |
| Claude Code CLI | 实际执行任务 | Worker 节点 |

### 3.3 交互流程

#### 扩容流程

```
用户在 CCM 前端点击"增加 Worker"（或自动触发）
  │
  ▼
CCM 后端调用 Elastic-Agent 扩容 API
  │
  ▼
Elastic-Agent:
  1. boto3.run_instances() 创建 EC2
  2. 等待 running
  3. 从凭证池选择账号（优先之前在该 IP 用过的）
  4. SSH 执行 bootstrap:
     a. 安装 Claude Code CLI（npm install -g @anthropic-ai/claude-code）
     b. 自动登录 Claude Code（分发凭证）
     c. 部署 Worker Agent Service
     d. 配置 Git 凭证（从 CCM 项目配置同步）
     e. 启动 Worker Agent Service（systemd）
  5. 注册节点到 Elastic-Agent 注册表
  │
  ▼
CCM 后端创建对应的 Instance 记录（status=idle, 绑定 worker_node_id）
  │
  ▼
GlobalDispatcher 下一轮轮询发现新的 idle Instance → 开始分发任务
```

#### 任务执行流程

```
CCM GlobalDispatcher 发现 idle Instance + pending Task
  │
  ▼
（改造前）InstanceManager.launch() → asyncio.create_subprocess_exec()
（改造后）InstanceManager.launch() → Worker Agent Service HTTP API
  │
  ▼
Worker Agent Service 收到执行请求:
  {
    "prompt": "实现用户注册功能...",
    "project_path": "/workspace/my-project",
    "model": "claude-opus-4-6",
    "session_id": null,  // 或者续接的 session_id
    "env": { "GIT_AUTHOR_NAME": "...", ... }
  }
  │
  ▼
Worker Agent Service 在本机启动 Claude Code 子进程:
  claude -p [prompt] --dangerously-skip-permissions \
    --output-format stream-json --verbose
  │
  ▼
Worker Agent Service:
  - 逐行读取 stdout（NDJSON）
  - 通过 WebSocket / HTTP 流式上报到 Manager
  - Manager 的 CCM 后端接收后：
    a. 存入 log_entries 表
    b. WebSocket 广播到前端
    c. 提取 session_id / cost / context_usage
  │
  ▼
Claude Code 进程结束
  │
  ▼
Worker Agent Service 上报完成状态 + exit code
  │
  ▼
CCM 后端根据 exit code 判断成功/失败/重试
```

#### 额度耗尽自动换号

```
Elastic-Agent 额度监控发现 Worker 1 的账号额度 < 15%
  │
  ▼
从凭证池选择新账号（优先之前在 Worker 1 IP 用过的）
  │
  ▼
SSH 到 Worker 1:
  1. 等待当前 Claude Code 任务完成（不中断执行中的任务）
  2. 登出旧账号
  3. 登入新账号
  4. 回收旧账号到凭证池
  │
  ▼
Worker 1 继续工作，使用新账号的额度
```

---

## 4. 需要改造的关键点

### 4.1 CCM 侧需要改造的代码

CCM 的核心逻辑（任务队列、优先级调度、UI、API）**不需要改动**，只需要改造 "执行层"：

| 模块 | 当前实现 | 改造方向 |
|------|---------|---------|
| `InstanceManager.launch()` | `asyncio.create_subprocess_exec()` 本地子进程 | 通过 HTTP/SSH 调用远程 Worker Agent Service |
| `InstanceManager._consume_output()` | 直接读本地 stdout | 从远程 Worker 的 WebSocket/HTTP 流接收日志 |
| `InstanceManager.stop()` | SIGINT/SIGTERM/SIGKILL 本地进程 | 调用远程 Worker Agent Service 的停止 API |
| `GlobalDispatcher._ensure_instances()` | 创建 DB 记录 | 创建 DB 记录 + 调用 Elastic-Agent 扩容 |
| `Instance` 数据模型 | pid (本地进程 ID) | 增加 worker_node_id, worker_ip 字段 |
| `Project` 工作区 | 本地 `WORKSPACE_DIR` | 需要同步到远程 Worker |

关键原则：**CCM 的任务管理、调度逻辑、前端 UI 保持不变，只替换底层的执行通道。**

### 4.2 新增 Worker Agent Service

需要在每个 Worker 节点上部署一个轻量服务，承担原来 InstanceManager 在本地做的事情：

```python
# Worker Agent Service — 运行在每个 Worker 节点上
# 轻量 FastAPI 服务

@app.post("/execute")
async def execute_task(request: ExecuteRequest):
    """启动 Claude Code 子进程执行任务"""
    proc = await asyncio.create_subprocess_exec(
        "claude", "-p", request.prompt,
        "--dangerously-skip-permissions",
        "--output-format", "stream-json",
        "--verbose",
        *([" --resume", request.session_id] if request.session_id else []),
        *([" --model", request.model] if request.model else []),
        cwd=request.project_path,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env={**os.environ, **request.env},
    )
    # 启动后台消费 stdout，流式上报到 Manager
    asyncio.create_task(stream_output_to_manager(proc, request.task_id))
    return {"pid": proc.pid, "status": "started"}

@app.post("/stop")
async def stop_task(request: StopRequest):
    """停止正在执行的 Claude Code 进程"""
    # SIGINT → wait → SIGTERM → wait → SIGKILL
    ...

@app.get("/status")
async def get_status():
    """返回 Worker 状态：空闲/忙碌、当前任务、Claude Code 版本、额度"""
    ...

@app.websocket("/ws/logs/{task_id}")
async def stream_logs(websocket, task_id: str):
    """实时流式传输 Claude Code 的 NDJSON 输出到 Manager"""
    ...
```

### 4.3 项目代码同步

CCM 的 Project 有 `local_path`（本地路径），Claude Code 需要在项目目录中工作。远程 Worker 也需要这个项目代码。

方案：

| 方案 | 说明 | 适用场景 |
|------|------|---------|
| **Git clone（推荐）** | Worker 启动时从 Git remote clone 项目代码 | 有 remote 的项目 |
| **rsync 同步** | Manager 通过 rsync 把项目目录推到 Worker | 纯本地项目 |
| **共享存储 (EFS)** | Manager 和所有 Worker 挂载同一 EFS | 需要实时同步的场景 |

推荐 MVP 方案：**Git clone**。任务执行前，Worker Agent Service 确保项目代码是最新的：

```python
async def ensure_project(project: ProjectConfig):
    if not os.path.exists(project.local_path):
        await run(f"git clone {project.git_url} {project.local_path}")
    else:
        await run(f"git -C {project.local_path} fetch origin")
        await run(f"git -C {project.local_path} reset --hard origin/main")
```

### 4.4 Session 亲和性

CCM 使用 `--resume session_id` 进行多轮对话。Claude Code 的 session 文件存储在本地（相对于 cwd），因此：

- **续接任务必须分发到同一个 Worker**（session 文件在那台机器上）
- Instance 数据模型中增加 `worker_node_id` 后，调度器可以确保续接请求路由到正确的 Worker

```python
# GlobalDispatcher 改造 — 续接任务的亲和调度
def _dispatch_task(self, task: Task):
    if task.session_id and task.last_worker_node_id:
        # 续接任务：必须路由到之前的 Worker
        instance = self._find_instance_on_worker(task.last_worker_node_id)
        if instance and instance.status == "idle":
            return instance
        # Worker 不可用时：session 无法续接，以新 session 开始
        task.session_id = None

    # 新任务：按 model 匹配任意空闲 Instance
    return self._find_idle_instance(model=task.model)
```

---

## 5. Harness 接口实现

### 5.1 CCM 的 Elastic-Agent Harness 定义

```python
from elastic_agent import Harness, BootstrapStep, ServiceDefinition

class CCMHarness(Harness):
    """Claude Code Manager 的 Elastic-Agent Harness 实现"""

    def __init__(self, ccm_config: dict):
        self.config = ccm_config

    def get_repo_url(self) -> str | None:
        # Worker Agent Service 的代码仓库
        # 如果 Worker Agent Service 内置在 Elastic-Agent 中则返回 None
        return "https://github.com/zjw49246/Claude-Code-Manager.git"

    def get_bootstrap_steps(self) -> list[BootstrapStep]:
        return [
            InstallNodeStep(),              # 安装 Node.js (Claude Code 依赖)
            InstallClaudeCodeStep(),        # npm install -g @anthropic-ai/claude-code
            InstallPythonDepsStep(),        # Worker Agent Service 的 Python 依赖
            DeployWorkerAgentStep(),        # 部署 Worker Agent Service 代码
            SetupWorkspaceStep(),           # 创建 /workspace 目录
            ConfigureSystemdStep(),         # 注册 systemd 服务
        ]

    def get_service_definitions(self) -> list[ServiceDefinition]:
        return [
            ServiceDefinition(
                name="ccm-worker-agent",
                command="uvicorn worker_agent.main:app --host 0.0.0.0 --port 8080",
                restart_policy="always",
                env={
                    "MANAGER_URL": self.config["manager_url"],
                    "WORKER_AUTH_TOKEN": self.config["worker_auth_token"],
                },
            ),
        ]

    def get_health_check(self) -> dict:
        return {
            "type": "http",
            "url": "http://localhost:8080/status",
            "interval": 30,
            "timeout": 5,
        }
```

### 5.2 各 Bootstrap 步骤的具体实现

```python
class InstallNodeStep(BootstrapStep):
    name = "install-nodejs"

    async def execute(self, ctx):
        await ctx.ssh.run("curl -fsSL https://deb.nodesource.com/setup_20.x | sudo bash -")
        await ctx.ssh.run("sudo apt-get install -y nodejs")

class InstallClaudeCodeStep(BootstrapStep):
    name = "install-claude-code"

    async def execute(self, ctx):
        await ctx.ssh.run("npm install -g @anthropic-ai/claude-code")
        # 验证安装
        result = await ctx.ssh.run("claude --version")
        ctx.log(f"Claude Code version: {result.stdout}")

class DeployWorkerAgentStep(BootstrapStep):
    name = "deploy-worker-agent"

    async def execute(self, ctx):
        # 方案 A：从 CCM 仓库的 worker_agent/ 子目录部署
        await ctx.ssh.run(
            f"git clone --depth 1 {ctx.harness.get_repo_url()} /opt/ccm"
        )
        await ctx.ssh.run("cd /opt/ccm && pip install -r worker_agent/requirements.txt")

        # 方案 B：pip install 独立的 worker-agent 包
        # await ctx.ssh.run("pip install ccm-worker-agent")

class SetupWorkspaceStep(BootstrapStep):
    name = "setup-workspace"

    async def execute(self, ctx):
        await ctx.ssh.run("mkdir -p /workspace")
        await ctx.ssh.run("chown ubuntu:ubuntu /workspace")

class ConfigureSystemdStep(BootstrapStep):
    name = "configure-systemd"

    async def execute(self, ctx):
        unit = """
[Unit]
Description=CCM Worker Agent Service
After=network.target

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/opt/ccm
ExecStart=/usr/local/bin/uvicorn worker_agent.main:app --host 0.0.0.0 --port 8080
Restart=always
RestartSec=5
Environment=MANAGER_URL={manager_url}
Environment=WORKER_AUTH_TOKEN={token}

[Install]
WantedBy=multi-user.target
""".format(
            manager_url=ctx.config["manager_url"],
            token=ctx.config["worker_auth_token"],
        )
        await ctx.ssh.write_file("/etc/systemd/system/ccm-worker-agent.service", unit)
        await ctx.ssh.run("sudo systemctl daemon-reload")
        await ctx.ssh.run("sudo systemctl enable --now ccm-worker-agent")
```

### 5.3 Manager 侧 CCM 与 Elastic-Agent 的集成

```python
# backend/main.py 中增加 Elastic-Agent 集成
from elastic_agent import ElasticAgentManager, AWSEc2Provider, CredentialPool
from ccm_harness import CCMHarness

# 初始化 Elastic-Agent
elastic = ElasticAgentManager(
    provider=AWSEc2Provider(
        region="ap-northeast-1",
        ami_id="ami-xxxxx",           # 预装 Ubuntu 的 AMI
        instance_type="t3.large",
        key_pair_name="ccm-workers",
        security_group_ids=["sg-xxxxx"],
        subnet_id="subnet-xxxxx",
    ),
    credential_pool=CredentialPool("credentials.json"),
    harness=CCMHarness({
        "manager_url": "http://manager-private-ip:8000",
        "worker_auth_token": "secret-token",
    }),
)

# FastAPI 路由 — 节点管理
@app.post("/api/elastic/scale-out")
async def scale_out(count: int = 1):
    nodes = await elastic.scale_out(count)
    # 为每个新 Worker 创建 CCM Instance 记录
    for node in nodes:
        instance = Instance(
            name=f"worker-{node.id[:8]}",
            status="idle",
            model="default",
            worker_node_id=node.id,
            worker_ip=node.private_ip,
        )
        db.add(instance)
    return {"nodes": [n.id for n in nodes]}

@app.post("/api/elastic/scale-in")
async def scale_in(count: int = 1):
    # 选择空闲的 Worker 缩容
    idle_instances = await get_idle_instances_with_workers()
    to_remove = idle_instances[:count]
    for inst in to_remove:
        await elastic.remove_node(inst.worker_node_id)
        await db.delete(inst)
    return {"removed": len(to_remove)}

@app.get("/api/elastic/nodes")
async def list_nodes():
    return await elastic.list_nodes()

@app.get("/api/elastic/quota")
async def get_quota():
    return await elastic.get_quota_status()
```

---

## 6. 分步实施方案

### Phase 0：准备（不改 CCM 代码）

1. 在 Elastic-Agent 框架中实现基础的 `AWSEc2Provider`
2. 实现 `CredentialPool`（JSON 文件存储账号和 OAuth tokens）
3. 实现 `BootstrapPipeline`（SSH 连接 + 命令执行）
4. 手动测试：用 Elastic-Agent 创建一台 EC2，SSH 进去装好 Claude Code，手动验证可用

### Phase 1：Worker Agent Service

1. 开发独立的 Worker Agent Service（轻量 FastAPI）
   - `/execute` — 启动 Claude Code 子进程
   - `/stop` — 停止进程
   - `/status` — 上报状态
   - `/ws/logs/{task_id}` — 流式日志
2. 实现 CCMHarness Bootstrap 步骤
3. 端到端测试：Elastic-Agent 创建 EC2 → Bootstrap 部署 Worker Agent → 手动调用 API 执行一个 Claude Code 任务

### Phase 2：CCM 后端改造

1. `Instance` 模型增加 `worker_node_id`、`worker_ip` 字段
2. `InstanceManager` 新增 `RemoteInstanceManager`，实现通过 HTTP 调用远程 Worker
3. `GlobalDispatcher` 增加对远程 Instance 的支持
4. 增加节点管理 API（`/api/elastic/`）
5. 端到端测试：通过 CCM Web UI 创建任务 → 自动分发到远程 Worker → 实时看到日志流

### Phase 3：前端扩展

1. 新增"节点管理"面板：
   - 查看所有 Worker 节点的状态（IP、账号、额度、CPU/内存）
   - 手动增减 Worker 数量
   - 查看每个 Worker 上运行的任务
2. Instance 列表增加 Worker 信息列（哪台机器、什么 IP）
3. 额度监控面板：每个账号的 5h/7d 使用率可视化

### Phase 4：智能化

1. 自动扩缩容规则：
   - pending 任务数 > idle Worker 数 → 扩容
   - 所有 Worker 空闲超过 N 分钟 → 缩容
2. 额度耗尽自动换号
3. Session 亲和性调度
4. 数据备份/恢复（缩容时保存 /workspace，扩容时恢复）

---

## 7. 技术细节与挑战

### 7.1 日志流式传输

CCM 当前直接读本地子进程的 stdout。改为远程后，需要一个可靠的实时日志传输通道。

**推荐方案：Worker → Manager WebSocket 反向连接**

```
Worker Agent Service                     Manager (CCM 后端)
       │                                        │
       │  WebSocket 连接 (Worker 主动连接)        │
       ├───────────────────────────────────────▶│
       │                                        │
       │  Claude Code stdout (NDJSON 逐行转发)   │
       │◀──(本地读取)──Claude CLI                │
       ├───────────────────────────────────────▶│
       │                                        │
       │                          存入 log_entries
       │                          WebSocket 广播到前端
```

Worker 主动连接 Manager 的好处：
- Worker 在私有子网内，Manager 不需要知道 Worker 的 IP
- 不需要在 Worker 上开入站端口
- 连接断开后 Worker 自动重连
- 复用 CCM 已有的 WebSocket 广播机制

### 7.2 Git 凭证传递

CCM 通过环境变量注入 Git 凭证。远程执行时需要将凭证安全地传递到 Worker：

```
Manager 在调用 Worker 执行任务时：
  1. 从 CCM 数据库读取 project 的 git 凭证
  2. 通过 HTTPS（加密传输）发送到 Worker Agent Service 的 /execute API
  3. Worker Agent Service 将凭证设为 Claude Code 子进程的环境变量
  4. 任务结束后，凭证随进程环境一起销毁
```

**安全注意事项：**
- Manager ↔ Worker 通信必须走 VPC 内网或 HTTPS
- Git SSH key 通过 `/execute` API 传递时应加密
- Worker Agent Service 应验证请求来源（Bearer Token）

### 7.3 多项目工作区管理

CCM 的多个 Project 可能被分发到不同 Worker。每个 Worker 需要维护自己的项目工作区：

```
Worker 1:
  /workspace/
    project-a/     ← git clone from remote
    project-b/     ← git clone from remote

Worker 2:
  /workspace/
    project-a/     ← 独立的 clone（不与 Worker 1 冲突）
    project-c/     ← git clone from remote
```

每个 Worker 上的 project clone 是独立的。当任务分发到 Worker 时：
1. 检查 `/workspace/{project_name}` 是否存在
2. 不存在 → `git clone`
3. 存在 → `git fetch && git reset --hard origin/main`（确保最新）

### 7.4 Claude Code 账号登录

Elastic-Agent 的凭证分发负责在 Bootstrap 时自动登录 Claude Code：

```bash
# Bootstrap 步骤中的 Claude Code 登录
# 方案 A: OAuth token 直接写入 (如果有 refresh token)
mkdir -p ~/.claude
cat > ~/.claude/.credentials.json << 'EOF'
{
  "claudeAiOauth": {
    "accessToken": "...",
    "refreshToken": "...",
    "expiresAt": "..."
  }
}
EOF

# 方案 B: claude auth login 自动化流程
# 沿用 agent-ml-research 的 Playwright 自动化登录
```

### 7.5 前端改造示意

新增"节点管理"标签页：

```
┌──────────────────────────────────────────────────────────┐
│  Dashboard │ Tasks │ Projects │ Nodes │ Secrets │ Files  │
├──────────────────────────────────────────────────────────┤
│                                                          │
│  Worker Nodes (3/5 running)          [+ Add Worker] [-]  │
│                                                          │
│  ┌────────────────┐ ┌────────────────┐ ┌──────────────┐  │
│  │ Worker 1       │ │ Worker 2       │ │ Worker 3     │  │
│  │ ● Running      │ │ ● Running      │ │ ○ Idle       │  │
│  │                │ │                │ │              │  │
│  │ IP: 10.0.1.12  │ │ IP: 10.0.1.13  │ │ IP: 10.0.2.5 │  │
│  │ Account: acc-1 │ │ Account: acc-2 │ │ Account: acc-3│  │
│  │ Quota: ██░░ 62%│ │ Quota: ████ 89%│ │ Quota: █░░░ 25%│ │
│  │                │ │ ⚠ 额度不足     │ │              │  │
│  │ Task: #142     │ │ Task: #138     │ │ Task: (idle) │  │
│  │ Cost: $2.34    │ │ Cost: $5.12    │ │ Cost: $0.00  │  │
│  └────────────────┘ └────────────────┘ └──────────────┘  │
│                                                          │
│  Account Pool (6 accounts, 3 active)                     │
│  ┌──────────────────────────────────────────────────────┐ │
│  │ acc-1  ██████░░ 62%  → Worker 1  (IP: 1.2.3.4)     │ │
│  │ acc-2  ████████░ 89%  → Worker 2  (IP: 5.6.7.8)     │ │
│  │ acc-3  ██░░░░░░ 25%  → Worker 3  (IP: 9.10.11.12)   │ │
│  │ acc-4  ░░░░░░░░  0%  (idle, last IP: 1.2.3.4)       │ │
│  │ acc-5  ░░░░░░░░  0%  (idle, last IP: 5.6.7.8)       │ │
│  │ acc-6  ████████ 95%  (cooling, last IP: 9.10.11.12)  │ │
│  └──────────────────────────────────────────────────────┘ │
│                                                          │
└──────────────────────────────────────────────────────────┘
```

### 7.6 改造量评估

| 模块 | 改动量 | 说明 |
|------|--------|------|
| CCM 后端 `InstanceManager` | 中 | 新增 `RemoteInstanceManager`，原有 `LocalInstanceManager` 保留用于单机模式 |
| CCM 后端 `GlobalDispatcher` | 小 | 增加 session 亲和性调度逻辑 |
| CCM 后端 API | 小 | 新增 `/api/elastic/` 节点管理路由 |
| CCM 后端 数据模型 | 小 | Instance 表增加 2 个字段 |
| CCM 前端 | 中 | 新增 Nodes 页面和账号池面板 |
| Worker Agent Service | 新开发 | 约 300-500 行 Python，轻量 FastAPI |
| CCM Harness | 新开发 | 约 200 行 Python，Bootstrap 步骤定义 |
| Elastic-Agent 核心 | 独立开发 | Provider + CredentialPool + Bootstrap + Monitor |

### 7.7 兼容性：保留单机模式

改造后的 CCM 应该同时支持两种模式：

```python
# backend/config.py
class Settings(BaseSettings):
    # 新增配置
    EXECUTION_MODE: str = "local"  # "local" | "elastic"
    ELASTIC_AGENT_CONFIG: str | None = None

# backend/services/instance_manager.py
def create_instance_manager(settings: Settings) -> InstanceManager:
    if settings.EXECUTION_MODE == "elastic":
        return RemoteInstanceManager(elastic_config=settings.ELASTIC_AGENT_CONFIG)
    else:
        return LocalInstanceManager()  # 原有逻辑，本地子进程
```

这样在没有配置 Elastic-Agent 的情况下，CCM 仍然可以作为单机工具使用，保持向后兼容。
