# Harness 应用示例：agent-ml-research 接入 Elastic-Agent

> 本文档以 [agent-ml-research](https://github.com/caoxiaoyuyuyuyuyu/agent-ml-research) 为例，说明一个已有自建基础设施的项目如何迁移到 Elastic-Agent 弹性计算框架。
>
> 与 [CCM Harness 文档](harness-example-claude-code-manager.md) 中 CCM 从零接入不同，agent-ml-research 已经自己实现了一套 EC2 管理 + 账号管理的基础设施代码。本文档的重点是 **替换**，而不是新建。

---

## 目录

1. [agent-ml-research 项目解析](#1-agent-ml-research-项目解析)
2. [当前基础设施的痛点](#2-当前基础设施的痛点)
3. [迁移架构设计](#3-迁移架构设计)
4. [模块替换映射](#4-模块替换映射)
5. [Harness 接口实现](#5-harness-接口实现)
6. [分步迁移方案](#6-分步迁移方案)
7. [技术细节与挑战](#7-技术细节与挑战)
8. [迁移对框架提出的需求](#8-迁移对框架提出的需求)

---

## 1. agent-ml-research 项目解析

### 1.1 项目定位

agent-ml-research 是一个 **多 Agent AI 研究自动化平台**，实现了从论文 idea 生成、实验执行、论文撰写到人工审核的全流程自动化。

核心能力：
- 管理多个 EC2 Worker 节点，每个节点运行独立的 `agent-ml server` 进程
- 每个 Worker 上运行 1-2 个 Claude Code 会话，通过 Claude Max 订阅账号执行任务
- Manager 节点提供中心化管控：EC2 编排、账号管理、额度监控、飞书集成
- 全自动的 ML 实验流水线：idea → 代码 → 训练 → 评估 → 论文

### 1.2 系统架构

agent-ml-research **已经是 Manager-Worker 架构**，与 Elastic-Agent 的目标架构高度相似：

```
┌────────────────────────────────────────────────────┐
│              Manager 节点（本地 Mac 或 EC2）         │
│                                                    │
│  ┌──────────┐  ┌──────────┐  ┌──────────────────┐  │
│  │ FastAPI  │  │ Vue      │  │ 飞书 Bot         │  │
│  │ 后端     │  │ Dashboard │ │ (WebSocket 长连接)│  │
│  └────┬─────┘  └──────────┘  └──────────────────┘  │
│       │                                            │
│  ┌────▼────────────────────────────────────────┐   │
│  │  自建基础设施层                              │   │
│  │  ┌──────────┐  ┌──────────┐  ┌────────────┐ │   │
│  │  │ec2/      │  │pool/     │  │quota_      │ │   │
│  │  │provider  │  │account   │  │watchdog    │ │   │
│  │  │bootstrap │  │login     │  │            │ │   │
│  │  │registry  │  │          │  │            │ │   │
│  │  │poller    │  │          │  │            │ │   │
│  │  └──────────┘  └──────────┘  └────────────┘ │   │
│  └─────────────────────┬───────────────────────┘   │
└────────────────────────┼───────────────────────────┘
                         │ SSH (paramiko)
           ┌─────────────┼─────────────┐
           │             │             │
      ┌────▼───┐    ┌────▼───┐    ┌────▼───┐
      │BE #1   │    │BE #2   │    │BE #N   │
      │EC2     │    │EC2     │    │EC2     │
      │        │    │        │    │        │
      │agent-ml│    │agent-ml│    │agent-ml│
      │server  │    │server  │    │server  │
      │        │    │        │    │        │
      │Claude  │    │Claude  │    │Claude  │
      │Code x2 │    │Code x2 │    │Code x2 │
      │        │    │        │    │        │
      │watchdog│    │watchdog│    │watchdog│
      └────────┘    └────────┘    └────────┘
```

### 1.3 自建基础设施层核心模块

#### EC2 管理（`manager/ec2/`）

| 文件 | 职责 |
|------|------|
| `provider.py` | boto3 封装：`run_instances`、`start`、`stop`、`terminate`、EIP 管理 |
| `bootstrap.py` | SSH 10 步初始化：deploy key → git clone → uv sync → npm build → Xvfb → systemd |
| `registry.py` | YAML 文件持久化的项目-实例映射，线程安全互斥锁 |
| `poller.py` | 每 60 秒轮询 `describe_instances`，同步状态/IP 到注册表 |
| `sync.py` | 代码部署：git pull（主路径）/ rsync（备选） |
| `register_backend.py` | 将 EC2 注册为 Manager 的后端：生成 auth token、SCP backend.yaml、热重载配置 |

#### 账号管理（`manager/pool/` + `core/tools/account_login.py`）

| 功能 | 实现 |
|------|------|
| 自动登录 | Playwright + mitmproxy 的 14 步 OAuth 流程，绕过 Cloudflare Turnstile |
| 账号池 | `~/.claude-pool/accounts.json`，每个 EC2 维护独立的账号注册表 |
| 账号选择 | `pool_select()`：按 5h 使用率升序排序，优先可用账号 |
| 邮箱服务 | 171mail API：发送魔法链接邮件、轮询收件、验证链接 |

#### 额度监控（`scripts/claude-pool-watchdog.sh`）

- 约 900 行 Bash 脚本，systemd 运行在每个 EC2 上
- 调用 `api.anthropic.com/api/oauth/usage` 获取使用量
- 跟踪 5 小时 + 7 天滚动窗口
- 使用率 >= 85% 时标记账号不可用
- 反检测措施：Fisher-Yates 随机轮询、随机延迟、指数退避

#### Manager 级额度监控（`manager/quota_watchdog.py`）

- 每 60 秒轮询所有 BE 的 `/manager/claude-pool`
- 检测使用率 > 85% → 飞书告警卡片（30 分钟冷却）
- 唤醒 Manager Agent 执行运维操作

### 1.4 技术栈

| 层级 | 技术 |
|------|------|
| 语言 | Python 3.11+（核心）、TypeScript（Dashboard）、Bash（watchdog）、Node.js（CDP 脚本） |
| 后端 | FastAPI（25+ 路由）、MCP（FastMCP） |
| 前端 | React + Ant Design（主 Dashboard）、Vue + Vite（Manager Dashboard） |
| AWS | EC2、S3（备份）、VPC、Security Groups、Elastic IPs |
| 浏览器自动化 | Playwright + playwright-stealth、Chrome DevTools Protocol |
| 远程执行 | SSH + tmux（paramiko） |
| 进程管理 | systemd（agent-ml.service + claude-pool-watchdog.service） |
| 持久化 | YAML + JSON（无数据库） |

### 1.5 与 CCM 的关键差异

| 维度 | agent-ml-research | Claude Code Manager |
|------|-------------------|---------------------|
| 架构 | 已是 Manager-Worker 分布式 | 单机单体 |
| EC2 管理 | 自建（boto3 + SSH） | 无（本地子进程） |
| 账号管理 | 完整（OAuth 自动登录 + 额度监控） | 无 |
| 迁移策略 | **替换**自建基础设施层 | **新增**分布式能力 |
| 改造重点 | 剥离 `manager/ec2/` 和 `manager/pool/` | 新增 RemoteInstanceManager |

---

## 2. 当前基础设施的痛点

agent-ml-research 虽然已经有了分布式架构，但自建基础设施存在以下问题（详见主文档第 1.5 节）：

| 痛点 | 说明 | Elastic-Agent 如何解决 |
|------|------|----------------------|
| **无自动扩缩容** | 实例创建靠手动 API 或 Manager Agent | 框架提供规则引擎 + ScalingSignal 自动化 |
| **Manager 单点故障** | Manager 挂了所有编排停止 | 框架后续支持高可用部署 |
| **YAML 文件状态** | 50+ 实例后性能下降 | 框架后续支持数据库后端 |
| **SSH key 共享** | 一个 key 泄露影响全集群 | 框架内置 Worker Runtime，消除 SSH 依赖 |
| **凭证明文传输** | SCP 传输凭证文件 | 框架提供安全凭证传递通道 |
| **无 IP 亲和性** | 账号出现在多个 IP 上，增加风控风险 | 框架内置 IP 亲和性调度 |
| **Bootstrap 脆弱** | 10 步流程无失败回滚策略 | 框架 Bootstrap Pipeline 有失败处理 + 凭证回收 |
| **Cloudflare 依赖** | headless=False 绕过 Turnstile，随时可能失效 | 框架提供多种 CredentialProvider 实现 |
| **171mail 依赖** | 第三方邮件服务宕机则无法登录 | 框架凭证管理解耦登录方式 |
| **无应用级健康检查** | VM running 不等于服务正常 | 框架 Worker Runtime 内置 L2/L3 健康检查 |

迁移到 Elastic-Agent 的核心价值：**将约 2000 行自建基础设施代码替换为框架调用**，同时获得 IP 亲和性、优雅缩容、健康检查等自建版本缺失的能力。

---

## 3. 迁移架构设计

### 3.1 迁移前后对比

```
迁移前:                                   迁移后:
┌──────────────────────┐                 ┌──────────────────────┐
│ Manager              │                 │ Manager              │
│ ┌──────────────────┐ │                 │ ┌──────────────────┐ │
│ │ agent-ml 业务    │ │                 │ │ agent-ml 业务    │ │
│ │ (保留)           │ │                 │ │ (保留)           │ │
│ └──────────────────┘ │                 │ └──────────────────┘ │
│ ┌──────────────────┐ │                 │ ┌──────────────────┐ │
│ │ 自建 EC2 管理    │ │  ══替换为══▶    │ │ Elastic-Agent    │ │
│ │ (ec2/, pool/,    │ │                 │ │ 框架             │ │
│ │  watchdog)       │ │                 │ │ (通用能力)       │ │
│ │ ~2000 行代码     │ │                 │ │                  │ │
│ └──────────────────┘ │                 │ └──────────────────┘ │
└──────────┬───────────┘                 └──────────┬───────────┘
           │ SSH                                    │ Worker Runtime
      ┌────▼────┐                              ┌────▼────┐
      │ BE EC2  │                              │ Worker  │
      │         │                              │ ┌─────┐ │
      │agent-ml │                              │ │WR   │ │ ← 框架 Runtime
      │server   │                              │ └──┬──┘ │
      │         │                              │ agent-ml│
      │watchdog │                              │ server  │
      └─────────┘                              └─────────┘
```

### 3.2 保留与替换的边界

| 模块 | 操作 | 理由 |
|------|------|------|
| `manager/ec2/provider.py` | **替换** → `elastic_agent.CloudProvider` | 框架标准接口 |
| `manager/ec2/bootstrap.py` | **替换** → Harness Bootstrap 步骤 | 框架 Pipeline 有失败处理 |
| `manager/ec2/registry.py` | **替换** → `elastic_agent.NodeRegistry` | 框架统一注册表 |
| `manager/ec2/poller.py` | **替换** → 框架云端标签对账 + Worker 健康检查 | 框架内置 |
| `manager/ec2/sync.py` | **替换** → 框架工作区同步 | 框架标准能力 |
| `manager/ec2/register_backend.py` | **替换** → 框架 Bootstrap 最后一步 | 注册 Worker 为 Manager 后端 |
| `manager/api/ec2.py` | **替换** → 框架节点管理 API | 框架统一 API |
| `manager/pool/` | **替换** → `elastic_agent.CredentialPool` | 框架凭证管理 |
| `core/tools/account_login.py` | **适配** → `ClaudeOAuthProvider` 实现 | 登录逻辑保留，适配框架接口 |
| `scripts/claude-pool-watchdog.sh` | **替换** → 框架 Worker Runtime 内置额度监控 | 框架统一监控 |
| `manager/quota_watchdog.py` | **替换** → 框架 Manager 级额度监控 | 框架统一告警 |
| `manager/feishu_bot_pool.py` | **保留** | agent-ml 业务特有 |
| `core/session_server.py` | **保留** | agent-ml 业务逻辑 |
| `core/claude_client.py` | **适配** | 账号选择逻辑适配框架凭证池 |
| `manager/bot/dispatcher.py` | **保留** | 飞书消息路由，业务特有 |
| `manager/clients/backend_client.py` | **适配** | 通过框架 Worker Runtime 通信 |
| FastAPI 路由、Dashboard、飞书集成 | **保留** | agent-ml 业务层 |

### 3.3 迁移后的架构

```
┌─────────────────────────────────────────────────────────────┐
│                      Manager 节点                            │
│                                                             │
│  ┌───────────────────────────────────────────────────────┐  │
│  │             agent-ml 业务层（保留）                     │  │
│  │  FastAPI · Vue Dashboard · 飞书 Bot · 研究流水线       │  │
│  └───────────────────────┬───────────────────────────────┘  │
│                          │ 调用框架 API                      │
│  ┌───────────────────────▼───────────────────────────────┐  │
│  │             Elastic-Agent 框架层                        │  │
│  │                                                       │  │
│  │  CloudProvider  CredentialPool   QuotaMonitor          │  │
│  │  (boto3)        (账号池+轮换)    (额度监控)            │  │
│  │                                                       │  │
│  │  NodeRegistry   BootstrapPipeline  WorkerRuntime       │  │
│  │  (节点注册)     (初始化管道)       (日志传输)          │  │
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
        ││agent- ││   ││agent- ││   ││agent- ││
        ││ml     ││   ││ml     ││   ││ml     ││  ← 业务代码
        ││server ││   ││server ││   ││server ││
        │└───────┘│   │└───────┘│   │└───────┘│
        │┌───────┐│   │┌───────┐│   │┌───────┐│
        ││Claude ││   ││Claude ││   ││Claude ││
        ││Code   ││   ││Code   ││   ││Code   ││
        │└───────┘│   │└───────┘│   │└───────┘│
        └─────────┘   └─────────┘   └─────────┘
```

---

## 4. 模块替换映射

### 4.1 EC2 Provider 替换

```python
# ── 替换前：agent-ml-research 自建 ──

# manager/ec2/provider.py
class Ec2Provider:
    def __init__(self, config: Ec2Config):
        self.ec2 = boto3.client('ec2', region_name=config.region)

    def run_instance(self, project: str, instance_type: str) -> dict:
        resp = self.ec2.run_instances(
            ImageId=self.ami_id,
            InstanceType=instance_type,
            KeyName=config.key_pair_name,
            # ... 自定义 Tag、Security Group 等
        )
        return resp['Instances'][0]

# ── 替换后：使用 Elastic-Agent 框架 ──

from elastic_agent import AWSEc2Provider, InstanceConfig

provider = AWSEc2Provider(
    region="ap-northeast-1",
    ami_id="ami-xxxxx",
    default_instance_type="t3.large",
    key_pair_name="auto-research-ec2-key-pair",
    security_group_ids=["sg-xxxxx"],
    subnet_id="subnet-xxxxx",
)

instance = await provider.create_instance(InstanceConfig(
    name=f"Prod-{project}",
    tags={"Project": project, "ManagedBy": "elastic-agent"},
))
```

### 4.2 Bootstrap 替换

```python
# ── 替换前：agent-ml-research 的 10 步 bootstrap ──

# manager/ec2/bootstrap.py
async def bootstrap_instance(ssh, project, config):
    await setup_git_deploy_key(ssh, config)       # Step 1
    await deploy_code(ssh, config)                 # Step 2
    await install_uv(ssh)                          # Step 3
    await install_dependencies(ssh)                # Step 4
    await install_phase13_deps(ssh)                # Step 5
    await build_dashboard(ssh)                     # Step 6
    await ensure_xvfb(ssh)                         # Step 7
    await create_workspace(ssh, project)           # Step 8
    await configure_credentials(ssh, config)       # Step 9
    await install_systemd_units(ssh, project)      # Step 10

# ── 替换后：Harness Bootstrap 步骤（见第 5 节） ──
```

### 4.3 账号管理替换

```python
# ── 替换前：agent-ml-research 自建 ──

# core/tools/account_login.py — 14 步 Playwright 登录
# scripts/claude-pool-watchdog.sh — 900 行额度监控
# manager/pool/__init__.py — SSH 远程账号管理
# manager/quota_watchdog.py — Manager 级告警

# ── 替换后：框架凭证管理 ──

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
```

### 4.4 注册表 + 轮询替换

```python
# ── 替换前 ──

# manager/ec2/registry.py — YAML 文件 + 互斥锁
# manager/ec2/poller.py — 每 60s describe_instances

# ── 替换后 ──

# 框架 NodeRegistry 自动处理：
# - 节点状态持久化
# - 云端标签对账（启动时 + 定期）
# - Worker 健康检查（L2/L3）
# - 孤儿实例检测
```

---

## 5. Harness 接口实现

### 5.1 AgentMLResearchHarness 定义

```python
from elastic_agent import (
    Harness, BootstrapStep, ServiceDefinition,
    ScalingSignal, FrameworkEvent, EventHandler,
)

class AgentMLResearchHarness(Harness):
    """agent-ml-research 的 Elastic-Agent Harness 实现"""

    def __init__(self, config: dict):
        self.config = config

    def get_repo_url(self) -> str:
        return "https://github.com/caoxiaoyuyuyuyuyu/agent-ml-research.git"

    def get_bootstrap_steps(self) -> list[BootstrapStep]:
        return [
            SetupGitDeployKeyStep(),        # 原 bootstrap Step 1
            DeployCodeStep(),               # 原 bootstrap Step 2（git clone）
            InstallUVStep(),                # 原 bootstrap Step 3
            InstallDependenciesStep(),      # 原 bootstrap Step 4（uv sync --all-extras）
            InstallPlaywrightStep(),        # 原 bootstrap Step 5
            BuildDashboardStep(),           # 原 bootstrap Step 6（npm ci && npm run build）
            SetupXvfbStep(),                # 原 bootstrap Step 7
            CreateWorkspaceStep(),          # 原 bootstrap Step 8
            ConfigureCredentialsStep(),     # 原 bootstrap Step 9（WandB, HF, Feishu）
            RegisterAsBackendStep(),        # 原 bootstrap Step 10 + register_backend
        ]

    def get_service_definitions(self) -> list[ServiceDefinition]:
        return [
            ServiceDefinition(
                name="agent-ml",
                command="agent-ml server --public --dash-port 8420",
                restart_policy="always",
                env={
                    "AGENT_ML_WORKSPACE": "/home/ubuntu/.agent-ml-research-{project}",
                },
            ),
        ]

    def get_app_credentials(self) -> list[str]:
        return [
            "git_deploy_key",       # GitHub deploy key
            "wandb_api_key",        # Weights & Biases
            "hf_token",             # HuggingFace
            "feishu_app_id",        # 飞书 Bot
            "feishu_app_secret",
        ]

    def get_scaling_signal(self) -> ScalingSignal:
        # agent-ml 的扩缩容信号：活跃项目数量
        active_projects = self._count_active_projects()
        idle_workers = self._count_idle_workers()
        return ScalingSignal(
            pending_tasks=active_projects - idle_workers,
            idle_workers=idle_workers,
            busy_workers=self._count_busy_workers(),
        )

    def get_event_handlers(self) -> dict:
        return {
            FrameworkEvent.NODE_READY: self._on_node_ready,
            FrameworkEvent.NODE_DRAIN_START: self._on_drain_start,
            FrameworkEvent.CREDENTIAL_EXHAUSTED: self._on_credential_exhausted,
            FrameworkEvent.QUOTA_WARNING: self._on_quota_warning,
        }

    async def _on_node_ready(self, data: dict):
        """节点就绪后，注册为 Manager 的后端"""
        # 从 register_backend.py 适配
        await register_ec2_as_backend(
            manager_config=self.config["manager_yaml"],
            worker_ip=data["private_ip"],
            worker_id=data["node_id"],
        )

    async def _on_drain_start(self, data: dict):
        """缩容前，停止节点上的研究任务"""
        await stop_active_research(data["node_id"])

    async def _on_credential_exhausted(self, data: dict):
        """额度耗尽时，通过飞书通知运维"""
        await send_feishu_alert(
            f"Account {data['account_id']} exhausted on Worker {data['node_id']}"
        )

    async def _on_quota_warning(self, data: dict):
        """额度告警，发送飞书卡片"""
        await send_feishu_quota_card(data)
```

### 5.2 各 Bootstrap 步骤实现

```python
class SetupGitDeployKeyStep(BootstrapStep):
    name = "setup-git-deploy-key"

    async def execute(self, ctx):
        # 原 bootstrap.py Step 1
        await ctx.ssh.upload_file(ctx.config["git_deploy_key_path"], "~/.ssh/github_deploy")
        await ctx.ssh.run("chmod 600 ~/.ssh/github_deploy")
        await ctx.ssh.run("""
            cat >> ~/.ssh/config << 'EOF'
Host github.com
    IdentityFile ~/.ssh/github_deploy
    StrictHostKeyChecking no
EOF
        """)
        await ctx.ssh.run("ssh-keyscan github.com >> ~/.ssh/known_hosts 2>/dev/null")

class DeployCodeStep(BootstrapStep):
    name = "deploy-code"

    async def execute(self, ctx):
        repo_url = ctx.harness.get_repo_url()
        branch = ctx.config.get("repo_branch", "main")
        result = await ctx.ssh.run(
            f"git clone -b {branch} {repo_url} /home/ubuntu/agent-ml-research",
            timeout=300,
        )
        if result.returncode != 0:
            # 备选：rsync
            await ctx.ssh.rsync(
                src=ctx.config["local_repo_path"],
                dst="/home/ubuntu/agent-ml-research",
                exclude=[".venv/", ".git/", "node_modules/"],
            )

    async def rollback(self, ctx):
        await ctx.ssh.run("rm -rf /home/ubuntu/agent-ml-research")

class InstallUVStep(BootstrapStep):
    name = "install-uv"

    async def execute(self, ctx):
        await ctx.ssh.run("curl -LsSf https://astral.sh/uv/install.sh | sh")

class InstallDependenciesStep(BootstrapStep):
    name = "install-dependencies"

    async def execute(self, ctx):
        await ctx.ssh.run(
            "cd /home/ubuntu/agent-ml-research && uv sync --all-extras",
            timeout=600,
        )

class InstallPlaywrightStep(BootstrapStep):
    name = "install-playwright"

    async def execute(self, ctx):
        await ctx.ssh.run(
            "cd /home/ubuntu/agent-ml-research && "
            "uv pip install playwright playwright-stealth mitmproxy"
        )

class BuildDashboardStep(BootstrapStep):
    name = "build-dashboard"

    async def execute(self, ctx):
        await ctx.ssh.run(
            "cd /home/ubuntu/agent-ml-research/dashboard && npm ci && npm run build",
            timeout=300,
        )

class SetupXvfbStep(BootstrapStep):
    name = "setup-xvfb"

    async def execute(self, ctx):
        xvfb_unit = """
[Unit]
Description=Xvfb virtual framebuffer
After=network.target

[Service]
ExecStart=/usr/bin/Xvfb :99 -screen 0 1365x900x24
Restart=always

[Install]
WantedBy=multi-user.target
"""
        await ctx.ssh.write_file("/etc/systemd/system/xvfb.service", xvfb_unit)
        await ctx.ssh.run("sudo systemctl daemon-reload && sudo systemctl enable --now xvfb")

class CreateWorkspaceStep(BootstrapStep):
    name = "create-workspace"

    async def execute(self, ctx):
        project = ctx.config["project_name"]
        await ctx.ssh.run(f"mkdir -p /home/ubuntu/.agent-ml-research-{project}")

class ConfigureCredentialsStep(BootstrapStep):
    name = "configure-credentials"

    async def execute(self, ctx):
        # WandB
        if wandb_key := ctx.app_credentials.get("wandb_api_key"):
            await ctx.ssh.run(f"wandb login {wandb_key}")
        # HuggingFace
        if hf_token := ctx.app_credentials.get("hf_token"):
            await ctx.ssh.run(f"huggingface-cli login --token {hf_token}")
        # Feishu config
        await ctx.ssh.upload_file(ctx.config["feishu_yaml_path"], "~/.agent-ml-research/feishu.yaml")
        # Project config
        await ctx.ssh.upload_file(ctx.config["project_yaml_path"], "~/.agent-ml-research/project.yaml")

class RegisterAsBackendStep(BootstrapStep):
    name = "register-as-backend"

    async def execute(self, ctx):
        # 生成 64-hex auth token
        import secrets
        token = secrets.token_hex(32)
        # 构建 backend.yaml
        backend_config = {
            "manager_url": ctx.config["manager_url"],
            "auth_token": token,
            "capabilities": ctx.config.get("capabilities", ["research"]),
        }
        import yaml
        await ctx.ssh.write_file(
            f"/home/ubuntu/.agent-ml-research-{ctx.config['project_name']}/config/backend.yaml",
            yaml.dump(backend_config),
        )
        # 通知 Manager 热重载
        ctx.metadata["backend_token"] = token
```

### 5.3 Manager 侧集成

```python
# manager/service.py 中替换自建基础设施为框架调用

from elastic_agent import ElasticAgentManager, AWSEc2Provider, CredentialPool
from aml_harness import AgentMLResearchHarness

# 替换前:
# from manager.ec2.provider import Ec2Provider
# from manager.ec2.bootstrap import bootstrap_instance
# from manager.ec2.registry import Ec2Registry
# from manager.ec2.poller import start_poll_loop

# 替换后:
elastic = ElasticAgentManager(
    provider=AWSEc2Provider(config),
    credential_pool=CredentialPool(provider=ClaudeOAuthProvider(email_tokens)),
    harness=AgentMLResearchHarness(config),
)

# 原有的 API 路由适配
# POST /api/ec2/instances → elastic.scale_out()
# POST /api/ec2/instances/{project}/terminate → elastic.remove_node()
# GET /api/ec2/instances → elastic.list_nodes()
# GET /api/ec2/status → elastic.get_cluster_status()
```

---

## 6. 分步迁移方案

### Phase 0：并行运行（零风险验证）

1. 在 agent-ml-research 中引入 Elastic-Agent 作为依赖
2. 新增 `/api/elastic/` 路由，与原有 `/api/ec2/` 并行
3. 通过 Elastic-Agent 创建一台测试 EC2，验证 Bootstrap 全流程
4. 对比自建版本和框架版本的行为差异

**关键：** 不改动原有代码，新旧共存。

### Phase 1：替换 EC2 管理

1. 将 `manager/ec2/provider.py` 替换为 `AWSEc2Provider`
2. 将 `manager/ec2/bootstrap.py` 替换为 Harness Bootstrap Pipeline
3. 将 `manager/ec2/registry.py` 替换为框架 `NodeRegistry`
4. 将 `manager/ec2/poller.py` 替换为框架云端对账 + 健康检查
5. 保留原有的 API 路由签名，内部实现替换为框架调用
6. 端到端测试：通过 Dashboard 创建/停止/终止实例

### Phase 2：替换账号管理

1. 将 `manager/pool/` 替换为框架 `CredentialPool`
2. 将 `core/tools/account_login.py` 适配为 `ClaudeOAuthProvider`
3. 将 `scripts/claude-pool-watchdog.sh` 替换为框架额度监控
4. 将 `manager/quota_watchdog.py` 替换为框架 Manager 级告警
5. 适配 `core/claude_client.py` 的 `pool_select()` 逻辑
6. 端到端测试：额度耗尽 → 自动换号 → 飞书告警

### Phase 3：接入高级功能

1. 启用 IP 亲和性调度
2. 启用优雅缩容（Drain 机制）
3. 配置 ScalingSignal + 规则引擎实现自动扩缩容
4. 接入框架事件系统（NODE_READY → 注册后端，QUOTA_WARNING → 飞书通知）

### Phase 4：清理

1. 删除 `manager/ec2/` 目录
2. 删除 `manager/pool/` 目录
3. 删除 `scripts/claude-pool-watchdog.sh`
4. 删除 `manager/quota_watchdog.py`
5. 更新文档和测试

预计替换约 2000 行自建基础设施代码，替换为约 300 行 Harness 定义 + 框架 API 调用。

---

## 7. 技术细节与挑战

### 7.1 Playwright 登录逻辑的适配

agent-ml-research 的 `account_login.py` 包含 14 步 OAuth 自动登录流程。这套逻辑需要适配为框架的 `ClaudeOAuthProvider` 接口：

```python
class ClaudeOAuthProvider(CredentialProvider):
    """从 account_login.py 适配而来"""

    async def login(self, account, instance):
        # 复用原有的 14 步登录逻辑
        # 但改为通过框架 Worker Runtime 在目标机器上执行
        # 而不是从 Manager SSH 过去执行
        await self.runtime.execute(
            instance_id=instance.id,
            cmd=["python", "-m", "aml_harness.oauth_login",
                 "--email", account.email,
                 "--token", account.email_token],
        )

    async def check_quota(self, credential):
        # 复用 watchdog 的 usage API 调用逻辑
        usage = await self._call_usage_api(credential.access_token)
        return QuotaStatus(
            five_hour_pct=usage["five_hour"],
            seven_day_pct=usage["seven_day"],
            available=usage["five_hour"] < self.threshold,
        )
```

**挑战：** 登录流程需要 Xvfb + Chrome + Playwright，这些必须在目标 Worker 上可用。Bootstrap 时需要确保这些依赖已安装。

### 7.2 飞书集成的保留

飞书 Bot 是 agent-ml-research 的核心交互方式（用户通过飞书聊天下达研究指令）。这是纯业务逻辑，不进入框架。

但框架的事件系统可以与飞书集成：
- `QUOTA_WARNING` → 飞书告警卡片（替换原来的 `quota_watchdog.py`）
- `NODE_READY` → 飞书通知"新节点已上线"
- `WORKER_UNHEALTHY` → 飞书告警"节点异常"

### 7.3 飞书 Bot Pool 的适配

原系统有一个飞书 Bot 池（`feishu_bot_pool.yaml`），每个项目分配一个 Bot。这是业务特有的资源池模式，与 Agent 凭证池类似但不能混用。

方案：作为应用凭证管理，Harness 内部维护 Bot 池逻辑，框架只负责将 Bot 配置安全传递到 Worker。

### 7.4 Backend 注册机制

原系统通过 `register_backend.py` 将 EC2 注册为 Manager 的后端（写入 `manager.yaml`、SCP `backend.yaml`、热重载配置）。

迁移后，这个逻辑放在 Harness 的 `RegisterAsBackendStep` 和 `on_node_ready` 事件处理器中：
1. Bootstrap 最后一步生成 auth token 并写入 Worker
2. `NODE_READY` 事件触发后，Harness 将 Worker 信息写入 Manager 配置并热重载

### 7.5 Worker 上的会话管理

agent-ml-research 的 `core/claude_client.py` 使用 session 硬链接技术 — 当账号池轮换时，将旧账号的 session `.jsonl` 文件硬链接到新账号的 config_dir，使 Claude 可以 `--resume` 继续对话。

这属于业务逻辑，保留在 Harness 中。但框架的 Worker 亲和性路由可以帮助：
- 设置 `AffinityPolicy.PREFERRED`，确保同一项目尽量在同一 Worker 上执行
- 即使凭证轮换，session 文件仍在同一 Worker 上

### 7.6 迁移量评估

| 模块 | 变化 | 工作量 |
|------|------|--------|
| `manager/ec2/`（6 文件） | 删除，替换为框架调用 | 中（需要 API 适配） |
| `manager/pool/`（3 文件） | 删除，替换为框架凭证管理 | 中（OAuth 适配） |
| `manager/quota_watchdog.py` | 删除，替换为框架事件系统 | 小 |
| `scripts/claude-pool-watchdog.sh` | 删除，框架内置 | 小 |
| `manager/api/ec2.py` | 重写，内部调用框架 | 小（API 签名不变） |
| `core/claude_client.py` | 适配 pool_select | 小 |
| `AgentMLResearchHarness` | 新增 | 中（~300 行） |
| 总计 | 删除 ~2000 行，新增 ~300 行 | 2-3 周 |

---

## 8. 迁移对框架提出的需求

### 8.1 agent-ml-research 特有但值得框架支持的

| 需求 | 说明 | 普适性判断 |
|------|------|-----------|
| **Bootstrap 步骤的超时控制** | `uv sync` 需要 600 秒超时，`npm ci` 需要 300 秒 | 通用 — 不同步骤耗时差异大 |
| **Bootstrap 备选路径** | git clone 失败时 fallback 到 rsync | 通用 — 网络不稳定时需要 |
| **Backend 热注册** | 新 Worker 上线后需要热加载到 Manager 配置 | 通用 — 任何 Manager-Worker 架构都需要 |
| **飞书/Slack/Webhook 告警通道** | 框架事件需要通知到外部系统 | 通用 — Webhook 回调即可 |

### 8.2 与 CCM 需求的交叉验证

| 需求 | agent-ml-research | CCM | 结论 |
|------|-------------------|-----|------|
| Worker Runtime | ✅ 替换 SSH | ✅ 替换本地子进程 | **框架核心** |
| 日志流式传输 | ✅ 飞书告警 | ✅ WebSocket 前端 | **框架核心** |
| 有状态亲和性 | ✅ 项目绑定实例 | ✅ session resume | **框架核心** |
| 优雅缩容 | ✅ 长时间训练 | ✅ 30min 任务 | **框架核心** |
| 双层凭证 | ✅ WandB/HF/Feishu | ✅ Git key | **框架核心** |
| 扩缩容信号 | ✅ 活跃项目数 | ✅ 任务队列深度 | **框架核心** |
| 工作区同步 | ✅ git clone/rsync | ✅ Project clone | **框架核心** |
| Bootstrap 超时 | ✅ | - | 框架支持 |
| Bootstrap 备选路径 | ✅ | - | 框架支持 |
| 事件 Webhook | ✅ 飞书 | - | 框架支持 |

两个 Harness 的核心需求完全一致，进一步确认框架设计的完整性。详见主文档 [elastic-agent-analysis.md](elastic-agent-analysis.md) 第 10 节。
