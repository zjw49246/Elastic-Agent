# Elastic-Agent 弹性计算框架 — 详细调研分析文档

> 本文档是对 AI Agent 弹性计算框架的全面调研分析，涵盖参考项目解析、技术方案对比、架构设计建议。

---

## 目录

1. [参考项目 agent-ml-research 深度解析](#1-参考项目-agent-ml-research-深度解析)
2. [框架需求总结与核心抽象](#2-框架需求总结与核心抽象)
3. [AWS 原生服务分析](#3-aws-原生服务分析)
4. [阿里云对等服务分析](#4-阿里云对等服务分析)
5. [开源方案分析](#5-开源方案分析)
6. [EC2 vs 容器 vs Serverless 对比](#6-ec2-vs-容器-vs-serverless-对比)
7. [纯 EC2 方案的缺陷与不足](#7-纯-ec2-方案的缺陷与不足)
8. [推荐架构方案](#8-推荐架构方案)
9. [MVP 实现计划](#9-mvp-实现计划)
10. [框架设计完整性审查](#10-框架设计完整性审查)
11. [Audiobook Harness 案例补充验证](#11-audiobook-harness-案例补充验证)
12. [未来演进路线](#12-未来演进路线)

---

## 1. 参考项目 agent-ml-research 深度解析

### 1.1 系统概览

agent-ml-research 是一个多 Agent AI 研究自动化平台，采用双层架构：

- **Manager 节点**（单实例）：中心控制面，负责 EC2 编排、账号管理、任务分发
- **Backend Worker 节点**（N 个 EC2 实例，每个研究项目一个）：每个运行独立的 `agent-ml server` 进程和 Claude Code 会话

### 1.2 EC2 节点供应架构

#### 核心文件

| 文件 | 职责 |
|------|------|
| `manager/ec2/provider.py` | boto3 轻量封装（describe、run、start、stop、terminate） |
| `manager/ec2/bootstrap.py` | SSH 方式的启动后配置（10 步流程） |
| `manager/ec2/registry.py` | 项目到实例的映射（YAML 持久化、线程安全互斥锁） |
| `manager/ec2/poller.py` | 后台状态同步（60 秒轮询 AWS） |
| `manager/ec2/sync.py` | 代码同步（git pull / rsync） |
| `manager/ec2/register_backend.py` | 将 EC2 注册为 Manager 后端 |
| `manager/api/ec2.py` | REST API 端点 |
| `manager/config.py` | Pydantic 配置模型 |

#### EC2 配置模型

```python
class Ec2Config(BaseModel):
    enabled: bool = False
    region: str = "ap-northeast-1"
    ami_id: str = ""
    ami_name: str = "aml-train"        # 预装 Chrome、Xvfb、Python、uv 的自定义 AMI
    default_instance_type: str = "t3.large"
    security_group_ids: list[str] = []
    key_pair_name: str = "auto-research-ec2-key-pair"
    ssh_key_path: str = "~/.ssh/auto-research-ec2-key-pair.pem"
    ssh_user: str = "ubuntu"
    max_instances: int = 30             # 硬上限
    repo_url: str = ""
    repo_branch: str = "main"
    git_deploy_key_path: str = "~/.ssh/github_deploy"
    wandb_api_key: str = ""
    hf_token: str = ""
```

#### 实例创建流程

1. API 调用 `POST /api/ec2/instances`，传入项目名
2. AMI 解析：如果未指定 `ami_id`，按 `ami_name`（"aml-train"）查找自有 AMI
3. EC2 启动：`run_instances`，Tags 包括 `Name=Prod-{project}`、`Project={project}`、`ManagedBy=agent-ml-manager`
4. 注册表写入：写入 `~/.agent-ml-manager/ec2_registry.yaml`（原子写入，线程安全）
5. 后台 Bootstrap（异步任务）：等待实例 running 状态后通过 SSH 初始化

#### Bootstrap 10 步流程

从 Manager 节点通过 SSH 连入新 EC2 后依次执行：

| 步骤 | 内容 |
|------|------|
| 1 | 配置 Git deploy key → SCP 到 `~/.ssh/github_deploy`，配置 SSH config |
| 2 | 部署代码 → 主路径 `git clone`，备选 `rsync`（排除 `.venv/`, `.git/`, `node_modules/`） |
| 3 | 安装 uv → `curl -LsSf https://astral.sh/uv/install.sh` |
| 4 | 安装依赖 → `uv sync --all-extras`（600 秒超时） |
| 5 | 安装 Phase 13 依赖 → playwright、playwright-stealth、mitmproxy |
| 6 | 构建 Dashboard → `npm ci && npm run build` |
| 7 | 确保 Xvfb → 安装虚拟 X 显示的 systemd unit（无头 Chrome 需要） |
| 8 | 创建工作区 → `/home/ubuntu/.agent-ml-research-{project}/` |
| 9 | 配置凭证 → WandB、HuggingFace token、feishu.yaml、project.yaml SCP 传输 |
| 10 | 安装 systemd 服务 → `agent-ml.service`（主服务）+ `claude-pool-watchdog.service`（额度监控） |

#### 后台轮询器

`poll_loop()` 每 60 秒运行一次：
- 调用 `describe_instances` 获取所有非终止实例
- 同步状态、公网 IP、内网 IP 变化到注册表
- 标记已消失实例为 `terminated`

#### 生命周期 API

| 端点 | 操作 |
|------|------|
| `POST /api/ec2/instances` | 创建 + 自动 bootstrap |
| `POST /api/ec2/instances/{project}/start` | 启动已停止实例 |
| `POST /api/ec2/instances/{project}/stop` | 停止运行中实例 |
| `POST /api/ec2/instances/{project}/terminate` | 终止实例 |
| `POST /api/ec2/instances/{project}/sync` | Git pull 或 SCP 覆盖 |
| `POST /api/ec2/instances/{project}/bootstrap` | 重新执行 bootstrap |
| `POST /api/ec2/instances/{project}/restart-services` | 重启 agent-ml + watchdog |
| `GET /api/ec2/instances` | 列出所有实例 |
| `GET /api/ec2/status` | 聚合状态统计 |

### 1.3 Claude Code 账号管理

这是整个系统中最复杂的子系统。每个 EC2 需要 1-2 个 Claude Max 订阅账号才能运行 Claude Code。

#### 自动登录流程（Playwright 实现）

`perform_login()` 函数执行 14 步流程：

1. 查找邮箱对应的 171mail token
2. 获取账号级文件锁（防止并发登录）
3. 解锁 macOS keychain（供 Claude CLI 写入 token）
4. 启动本地 mitmproxy（修补 Claude CLI 2.1.x OAuth redirect_uri bug）
5. `POST /claude/send` 到 171mail API — 触发魔法链接邮件
6. 轮询 `/getClaudeMessage` 获取魔法链接（90 秒超时）
7. `POST /claude/verify` 到 171mail — 获取 sessionKey + cookies
8. 启动 `claude auth login` 子进程（带 `HTTPS_PROXY=mitm`）
9. 从 CLI stdout 解析 OAuth URL
10. 启动 Playwright（系统 Chrome + stealth 插件，headless=false 以绕过 Cloudflare）：
    - 在 claude.ai 上设置 session cookies
    - 通过 `/api/account` 进行身份验证
    - 导航到 OAuth URL
    - 等待通过 Cloudflare challenge
    - 点击 "Authorize" 按钮
    - 从回调重定向中捕获 code+state
11. 将 code+state 传递给 CLI 的 localhost 监听器
12. 等待 CLI 退出，验证 `claude auth status`
13. 如果 token 在 `.credentials.json` 中（keychain 被锁定），迁移到 keychain
14. 清理：kill mitm、浏览器、工作目录、释放锁

**关键设计决策：**
- 使用 `headless=False` + 系统 Chrome 绕过 Cloudflare Turnstile 检测
- Linux 上使用 `DISPLAY=:99` + Xvfb（虚拟帧缓冲）
- mitmproxy 修补 Claude CLI 2.1.x 特定 OAuth bug
- 每账号文件锁 + 10 分钟过期检测

#### 账号池架构 (`~/.claude-pool/`)

每个 EC2 维护：

```
~/.claude-pool/
  accounts.json          # 账号注册表
  locks/                 # 每账号文件锁
  keychain-pwd           # macOS keychain 密码

~/.claude-account-{id}/  # 每账号配置目录
  .credentials.json      # OAuth tokens（refresh + access）
  .claude.json           # Claude 配置
```

**accounts.json 结构：**

```json
{
  "five_hour_threshold": 85,
  "weekly_reserve_per_day": 6,
  "check_interval": 60,
  "pool_status_file": "/tmp/claude_pool_status.json",
  "accounts": [
    {
      "id": "project-1",
      "config_dir": "/home/ubuntu/.claude-account-project-1",
      "role": "automation",
      "email": "account1@email.com",
      "enabled": true
    }
  ]
}
```

#### 账号选择逻辑

`pool_select()` 函数选择最佳账号：

1. 调用 `claude-pool select` CLI 工具
2. 失败时使用 `_pool_select_fallback()`:
   - 读取 watchdog 状态文件
   - 过滤：启用、未排除、无认证错误、5 小时使用率 < 85%
   - 优先选择 `available=true` 的账号
   - 按 5 小时使用率升序排序（最空闲优先）
   - 备选：使用 7 天限额但 5 小时正常的账号

**会话硬链接**：当池切换到新账号时，将 session `.jsonl` 文件从旧 config_dir 硬链接到新 config_dir，使 Claude 可以 `--resume` 对话。同邮箱账号可共享会话。

#### 额度监控看门狗 (`claude-pool-watchdog.sh`)

约 900 行 Bash 脚本，以 systemd 服务运行在每个 EC2 上：

**功能：**
- 每个周期读取 `accounts.json`
- 对每个启用的账号：
  - 从 keychain / `.credentials.json` 读取 OAuth tokens
  - 调用 `https://api.anthropic.com/api/oauth/usage` 获取使用量
  - 处理 access token 过期时的刷新
  - 跟踪 5 小时和 7 天滚动窗口使用率
- 如果 5 小时使用率 >= 阈值（85%）：
  - 标记账号为不可用
  - Kill 交互式账号的后台 Claude 进程
- 将汇总状态写入 `/tmp/claude_pool_status.json`

**反检测措施：**
- 随机打乱账号轮询顺序（Fisher-Yates 洗牌）
- 账号轮询间随机延迟（30-60 秒）
- 周期间随机休眠
- 使用量 API 限频时指数退避（初始 180 秒，最大 2880 秒）

#### Manager 级额度看门狗 (`quota_watchdog.py`)

Manager 上运行的 Python asyncio 任务：
- 每 60 秒轮询所有 BE 的 `/manager/claude-pool` 端点
- 检测使用率超过 85% 的账号
- 发送飞书告警卡片（每账号 30 分钟冷却）
- 唤醒 Manager Agent 执行运维操作

### 1.4 技术栈

| 层级 | 技术 |
|------|------|
| 语言 | Python 3.11+（核心）、TypeScript（Dashboard）、Bash（watchdog）、Node.js（CDP 脚本） |
| AI 引擎 | Claude Code CLI 子进程（Opus/Sonnet） |
| 工具协议 | MCP（Model Context Protocol）via FastMCP |
| 后端框架 | FastAPI（25+ 路由） |
| 前端 | React + TypeScript + Vite + Ant Design（主 Dashboard）、Vue + Vite（Manager Dashboard） |
| AWS 服务 | EC2（计算）、S3（备份）、VPC、Security Groups、Elastic IPs |
| 浏览器自动化 | Playwright + playwright-stealth、Chrome DevTools Protocol |
| 代理 | mitmproxy（OAuth bug 修复） |
| 远程执行 | SSH + tmux（paramiko） |
| 包管理 | uv（astral-sh） |
| 进程管理 | systemd |
| 持久化 | YAML + JSON（文件系统，无数据库） |
| 虚拟显示 | Xvfb + noVNC + x11vnc |

### 1.5 现有实现的局限性

| 问题 | 说明 |
|------|------|
| 无自动扩缩容 | 有 `max_instances` 上限（30），但无基于负载的自动 scale-up/down，实例创建靠手动 API 或 Manager Agent |
| Manager 单点故障 | Manager 是单进程，宕机则所有 EC2 编排停止（BE 可继续独立运行） |
| 无数据库 | 全部状态是 YAML/JSON 文件 + 文件锁，当前规模（~20 实例）可用，但无法扩展到数百实例 |
| 内存受限 | EC2 `t3.medium`（4GB）在并发运行多个 Claude CLI 进程时容易 OOM |
| 171mail 依赖 | 登录流程完全依赖 `171mail.com` 第三方服务，宕机则无法新登录 |
| Cloudflare 脆弱性 | 使用 `headless=False` + 真实 Chrome 绕过 Turnstile，Cloudflare 更新检测则会失效 |
| SSH key 共享 | 所有 EC2 共用一个密钥对，密钥泄露影响整个集群 |
| 无静态加密 | Token 明文存储在 YAML/JSON 中（仅靠文件权限保护） |
| IP 管理缺失 | 没有账号到 IP 的亲和性绑定，无法控制一个账号出现在多少个 IP 上 |
| 硬编码坐标 | 旧版登录脚本用固定像素坐标点击按钮，依赖特定分辨率 |

### 1.6 可抽象提取的模块

从 agent-ml-research 中可以抽象为框架的核心模块：

| 模块 | 原项目对应 | 抽象后职责 |
|------|-----------|-----------|
| 节点供应 (Node Provisioning) | `ec2/provider.py` | 多云节点创建/启动/停止/终止 |
| 节点初始化 (Bootstrap) | `ec2/bootstrap.py` | 可插拔的初始化步骤管道 |
| 节点注册 (Registry) | `ec2/registry.py` | 节点状态持久化和查询 |
| 节点监控 (Poller) | `ec2/poller.py` | 云端状态同步 |
| 凭证管理 (Credential Manager) | `pool/`, `account_login.py` | 账号池、分发、轮换 |
| 额度监控 (Quota Monitor) | `watchdog.sh`, `quota_watchdog.py` | Agent 额度检查和告警 |
| 代码部署 (Code Sync) | `ec2/sync.py` | Harness 代码部署到 Worker |

---

## 2. 框架需求总结与核心抽象

### 2.1 框架定位

**Elastic-Agent 是一个 AI Agent 弹性计算基座框架**，提供：

1. **节点自动扩缩容** — 在 AWS / 阿里云上自动管理 Worker 节点
2. **Worker 执行运行时** — 在 Worker 上接收命令、执行进程、实时回传日志的标准运行时
3. **Agent 账号自动分发** — Claude Code / Codex 等 Agent 账号的自动登录、额度监控、智能轮换
4. **双层凭证管理** — Agent 凭证（账号登录态）+ 应用凭证（Git key、API key 等）
5. **智能分发与风控** — 账号-IP 亲和性 + 有状态工作负载的 Worker 亲和性路由
6. **优雅生命周期** — Drain 机制（缩容时等待任务完成）、数据备份/恢复
7. **任务感知扩缩容** — Harness 上报扩缩容信号，框架规则引擎自动决策
8. **Harness 插件化** — 用户可在节点启动时部署自己的 Agent 服务代码

> **设计原则：** 以上 1-7 是所有 Harness 都会需要的通用能力，由框架统一提供。Harness 只需要关注业务逻辑（任务调度策略、Agent 输出解析、UI 定制等），不需要重复解决基础设施问题。

### 2.2 核心架构

```
┌───────────────────────────────────────────────────────────────────┐
│                         Manager 节点                              │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌────────┐           │
│  │ 前端 UI  │  │ 后端 API │  │ 调度引擎 │  │ Router │           │
│  └──────────┘  └──────────┘  └──────────┘  └────────┘           │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌────────────────┐   │
│  │ 凭证管理 │  │ 额度监控 │  │ 节点注册 │  │ 日志/事件汇聚  │   │
│  └──────────┘  └──────────┘  └──────────┘  └────────────────┘   │
└──────────────────────┬──────────────────────────────────────────┘
                       │ Worker Runtime Protocol
                       │ (WebSocket 反向连接 / HTTP API)
     ┌─────────────────┼─────────────────┐
     │                 │                 │
┌────▼────┐       ┌────▼────┐       ┌────▼────┐
│Worker 1 │       │Worker 2 │       │Worker N │
│EC2/ECS  │       │EC2/ECS  │       │EC2/ECS  │
│         │       │         │       │         │
│┌───────┐│       │┌───────┐│       │┌───────┐│
││Worker ││       ││Worker ││       ││Worker ││
││Runtime││       ││Runtime││       ││Runtime ││  ← 框架提供的标准运行时
│└───┬───┘│       │└───┬───┘│       │└───┬───┘│
│    │    │       │    │    │       │    │    │
│┌───▼───┐│       │┌───▼───┐│       │┌───▼───┐│
││Claude ││       ││Claude ││       ││Codex  ││
││ Code  ││       ││ Code  ││       ││       ││
│└───────┘│       │└───────┘│       │└───────┘│
│┌───────┐│       │┌───────┐│       │┌───────┐│
││Harness││       ││Harness││       ││Harness││  ← 用户自定义
│└───────┘│       │└───────┘│       │└───────┘│
└─────────┘       └─────────┘       └─────────┘
```

### 2.3 可插拔接口设计

框架需要提供以下扩展点：

```
CloudProvider 接口                    ← 云资源管理
  ├── AWSProvider（EC2）
  ├── AWSContainerProvider（ECS/Fargate）
  ├── AliyunProvider（阿里云 ECS）
  └── ... 未来扩展

AgentType 接口                        ← Agent CLI 工具适配
  ├── ClaudeCodeAgent
  ├── CodexAgent
  └── ... 未来扩展

BootstrapPipeline 接口                ← 节点初始化
  ├── 基础系统初始化
  ├── Agent 安装与配置
  ├── Harness 代码部署               ← 用户自定义
  └── 服务启动

CredentialProvider 接口               ← Agent 账号管理
  ├── ClaudeOAuthProvider（171mail 登录流程）
  ├── APIKeyProvider（简单 API Key 分发）
  └── ... 未来扩展

WorkerRuntime 接口（框架内置）          ← 远程执行运行时
  ├── execute(command, env, cwd) → stream[LogEvent]
  ├── stop(pid)
  ├── status() → WorkerStatus
  └── health() → HealthReport

LogTransport 接口（框架内置）           ← Worker → Manager 日志通道
  ├── WebSocket 反向连接（Worker 主动连接 Manager）
  └── HTTP Long Polling（备选）

AffinityPolicy 接口                   ← 有状态工作负载路由
  ├── NONE — 任意 Worker
  ├── PREFERRED — 优先同一 Worker
  └── REQUIRED — 必须同一 Worker

ScalingSignal 接口                    ← Harness 上报扩缩容信号
  ├── pending_tasks / idle_workers
  ├── avg_wait_time / avg_task_duration
  └── 自定义指标

ApplicationCredential 接口            ← 应用级凭证（区别于 Agent 凭证）
  ├── Git SSH Key / HTTPS Token
  ├── API Keys（WandB, HuggingFace, ...）
  └── 环境变量 (.env files)
```

### 2.4 框架 vs Harness 职责边界

| 职责 | 框架提供 | Harness 实现 |
|------|---------|-------------|
| 节点创建/销毁/监控 | ✅ | |
| Worker Runtime（远程执行 + 日志流） | ✅ | |
| Agent 凭证管理（登录、额度、轮换） | ✅ | |
| 应用凭证安全传递 | ✅ | |
| IP 亲和性调度 | ✅ | |
| Worker 亲和性路由 | ✅ | |
| 优雅缩容（Drain 机制） | ✅ | |
| 扩缩容规则引擎 | ✅ | |
| 工作区同步（git clone/rsync） | ✅ | |
| Bootstrap 步骤执行 | ✅ | ✅ 定义具体步骤 |
| 扩缩容信号上报 | ✅ 接收+决策 | ✅ 产生信号 |
| 任务调度策略 | | ✅ |
| Agent 输出解析 | | ✅ |
| 业务 API 和 UI | | ✅ |
| 任务队列管理 | | ✅ |

---

## 3. AWS 原生服务分析

### 3.1 EC2 Auto Scaling Groups (ASG)

**工作原理：** ASG 根据定义的策略（目标追踪、阶梯扩展、预测性扩展）自动调整 EC2 实例数量。

**优点：**
- 完整 Linux 环境 — Claude Code 和 Codex CLI 获得真实 OS 和所有依赖
- 混合实例策略支持 On-Demand（基线）+ Spot（突发），Spot 节省最高 90%
- **生命周期钩子**可在实例启动/终止时触发 Lambda，非常适合凭证注入和数据备份
- 预测性扩展使用 ML 预测提前预供应
- 多 AZ 部署容错

**缺点：**
- 扩展延迟：新实例最少 40-60 秒（AMI 启动 + user-data），深度优化 AMI 可降至约 5 秒
- 有状态负载复杂：EBS 卷锁定在 AZ，不能跨 AZ 跟随实例
- 无原生 per-instance EIP 分配 — 需要 Lambda + 生命周期钩子的变通方案
- AMI 维护开销：预装 Claude Code/Codex 的自定义 AMI 需要定期更新

**IP 地址管理方案：**

| 方案 | 说明 | 适用场景 |
|------|------|---------|
| NAT Gateway | 私有子网实例通过固定 EIP 的 NAT Gateway 出站，所有实例共享同一 IP | 简单场景，不需要 per-account IP 隔离 |
| 多 NAT Gateway | 每个账号组一个 NAT Gateway + 对应子网，路由表控制 Worker 走哪个 NAT | **推荐方案**：实现"同一账号 → 同一 IP"的亲和性 |
| Per-instance EIP | Lambda 通过生命周期钩子从 EIP 池分配 EIP 到每个实例 | 最灵活但最复杂，受 EIP 配额限制（默认 5/region） |

### 3.2 ECS (Elastic Container Service)

#### Fargate 启动类型

| 维度 | 说明 |
|------|------|
| 运维 | Serverless — 无需管理底层基础设施 |
| 启动时间 | 冷启动 20-60 秒（SOCI 懒加载可优化至 < 5 秒） |
| 资源上限 | 最大 4 vCPU / 30 GB 内存（Claude Code 推荐 2 vCPU / 4 GB 足够） |
| SSH | 不支持 SSH 到底层主机 |
| 存储 | 无持久本地存储 — 需用 EFS |
| IP | 私有子网 + NAT Gateway → 稳定出站 IP；公有子网 → 动态公网 IP |
| 成本 | 按 vCPU + 内存 + 时长计费，无 Spot 选项 |

#### EC2 启动类型

| 维度 | 说明 |
|------|------|
| 运维 | 需管理底层 EC2 实例（AMI、补丁、容量） |
| 灵活性 | 完全控制实例类型、GPU、本地存储 |
| 调试 | SSH/SSM 可用 |
| 成本 | 大规模更经济（RI / Savings Plans），支持 Spot |
| IP | 同 EC2 ASG 的 NAT Gateway 方案 |

**结论：** Fargate 运维更简单，但 EC2 启动类型在成本控制和灵活性上更好，特别适合长时间运行的 Agent 进程。

### 3.3 EKS (Elastic Kubernetes Service)

| 维度 | 说明 |
|------|------|
| API | 完整 Kubernetes API — 最大灵活性和可移植性 |
| 成本 | $0.10/小时集群费用（$73/月）+ 计算成本 |
| 学习曲线 | 较陡但生态最大 |
| 自动扩展 | 原生支持 KEDA（事件驱动扩展），可基于队列深度、自定义指标扩展 Pod |
| 多云 | 同样的 manifest 可在 ACK（阿里云）、GKE、AKS 上运行 |

### 3.4 AWS Systems Manager (SSM)

高度相关的节点管理服务：

- **State Manager：** 自动配置 ASG 中实例的期望状态（安装 agent、配置凭证）
- **Parameter Store：** 存储配置值，免费层支持最多 10,000 个参数
- **Run Command：** 无需 SSH 在实例上执行命令，适合启动时凭证分发
- **Session Manager：** 无需开放入站端口的安全 shell 访问
- SSM 本身不额外收费

### 3.5 AWS Secrets Manager

| 维度 | 说明 |
|------|------|
| 费用 | $0.40/secret/月 + $0.05/10,000 API 调用 |
| 自动轮换 | 通过 Lambda 实现 4 步生命周期（create → set → test → finish） |
| 集成 | 原生支持 ECS、EKS、Lambda、EC2 |
| 用途 | 存储 Claude/Codex API keys、账号凭证；额度耗尽时通过 Lambda 自动轮换 |

### 3.6 Lambda + Step Functions 编排

Lambda 非常适合作为 Manager 节点的"大脑"：

- ASG 生命周期钩子处理器（扩出/缩入事件）
- 凭证分配逻辑（匹配账号到 IP、跟踪额度）
- 缩入时触发数据备份（EBS 快照、同步到 S3）
- Step Functions 处理复杂工作流：供应 → 配置 → 分配凭证 → 启动 Agent → 监控 → 备份 → 终止
- 按调用付费（$0.20/百万请求）

### 3.7 CloudFormation / CDK

| 工具 | 说明 |
|------|------|
| CloudFormation | 声明式 YAML/JSON 模板；漂移检测；适合标准化部署 |
| CDK（推荐） | 用 Python/TypeScript 编写基础设施；更高级抽象；可用标准框架测试 |

---

## 4. 阿里云对等服务分析

### 4.1 服务对照表

| AWS 服务 | 阿里云对等 | 说明 |
|---------|-----------|------|
| EC2 | ECS（弹性计算服务） | 核心 VM 服务，支持自定义镜像 |
| EC2 Auto Scaling | ESS（弹性伸缩） | ESS 本身免费，只收计算费用；支持定时/动态/手动扩展 |
| ECS (Container) | 容器服务 + ECI | ECI 是 Fargate 等价物：serverless 容器，秒级启动 |
| EKS | ACK（容器服务 Kubernetes 版） | 托管 K8s，支持 ECI 虚拟节点的 serverless Pod |
| Secrets Manager | KMS + 凭据管家 | KMS 支持 HSM（FIPS 140-2 Level 3），凭据管家管理生命周期 |
| Systems Manager | 云助手 | 无需 SSH 的远程命令执行 |
| S3 | OSS（对象存储） | 备份和数据持久化 |
| CloudFormation | ROS（资源编排） | 声明式资源管理 |

### 4.2 阿里云 ECS SDK 直连方案（MVP 首选）

> **阿里云是 MVP 阶段的优先实现目标。** 项目的首要部署环境是中国大陆，阿里云是必选项。

#### SDK 选择

阿里云 ECS 的 Python SDK 已升级为 V2.0 版本（`alibabacloud_ecs20140526`），相比旧版 `aliyunsdkecs`：

| 维度 | 旧版 SDK | 新版 SDK V2.0 |
|------|---------|--------------|
| 包名 | `aliyunsdkecs` | `alibabacloud_ecs20140526` |
| 认证 | AccessKey 硬编码 | 支持 STS Token、ECS RAM Role、OIDC 等多种方式 |
| 异步 | 不支持 | 原生支持 `async/await` |
| 类型提示 | 无 | 完整的 Type Hints |
| 维护状态 | 仅 bugfix | 活跃开发 |

#### 核心 API 映射

| Elastic-Agent 操作 | 阿里云 API | 说明 |
|-------------------|-----------|------|
| `create_instance()` | `RunInstances` | 创建 ECS 实例，支持 InstanceName、Tags、SecurityGroupId |
| `start_instance()` | `StartInstance` | 启动已停止实例 |
| `stop_instance()` | `StopInstance` | 停止实例（`ForceStop=True` 强制停止） |
| `terminate_instance()` | `DeleteInstance` | 释放实例（`Force=True` 强制释放） |
| `describe_instances()` | `DescribeInstances` | 查询实例列表，支持 Tag 过滤 |
| IP 管理 | `AllocateEipAddress` + `AssociateEipAddress` | 弹性公网 IP 分配与绑定 |
| 安全组 | `AuthorizeSecurityGroup` | 入站规则管理 |

#### 阿里云 ECS 配置模型

```python
class AliyunEcsConfig(BaseModel):
    region_id: str = "cn-hangzhou"
    image_id: str = ""                          # 自定义镜像 ID
    instance_type: str = "ecs.c6.large"         # 2 vCPU / 4 GiB
    security_group_id: str = ""
    vswitch_id: str = ""                        # VPC 子网
    key_pair_name: str = "elastic-agent-key"
    ssh_key_path: str = "~/.ssh/elastic-agent.pem"
    ssh_user: str = "root"                      # 阿里云 ECS 默认 root
    max_instances: int = 30
    internet_charge_type: str = "PayByTraffic"  # 按流量计费
    internet_max_bandwidth_out: int = 100       # Mbps
    system_disk_category: str = "cloud_essd"    # ESSD 云盘
    system_disk_size: int = 40                  # GB
    spot_strategy: str = "NoSpot"               # NoSpot / SpotAsPriceGo / SpotWithPriceLimit
    spot_price_limit: float = 0.0               # 抢占式实例最高价
```

#### 阿里云 vs AWS 关键差异

| 维度 | AWS EC2 | 阿里云 ECS | 对 Provider 实现的影响 |
|------|---------|-----------|----------------------|
| 默认用户 | `ubuntu` | `root`（自定义镜像可改） | `ssh_user` 配置项 |
| 公网 IP | 创建时分配或绑 EIP | 创建时分配或绑 EIP | API 路径不同，逻辑一致 |
| 安全组 | 支持入站/出站规则 | 同上，但默认规则不同 | 需要在 Bootstrap 前确认规则 |
| 标签 | `Tags` 在 `run_instances` 中 | `Tag` 在 `RunInstances` 中 | 字段名和结构略有不同 |
| 实例回收 | terminate 立即释放 | `DeleteInstance` 需要先 stop | Provider 封装处理 |
| Spot 实例 | 通过 `SpotInstanceType` | 通过 `SpotStrategy` | 参数映射不同 |
| SSH 密钥 | Key Pair（PEM） | Key Pair（PEM），也支持密码 | 一致 |
| 镜像 | AMI（区分 region） | 自定义镜像（区分 region） | 一致 |
| 内网 DNS | VPC 内自动解析 | VPC 内自动解析 | 一致 |

#### 实例类型推荐

| 用途 | 阿里云实例类型 | 规格 | 按量价格（cn-hangzhou） | 适用场景 |
|------|-------------|------|----------------------|---------|
| 轻量 Agent | ecs.c6.large | 2C / 4G | ~¥0.39/h | 单 Claude Code 进程 |
| 标准 Agent | ecs.c6.xlarge | 4C / 8G | ~¥0.78/h | 多任务并发 |
| 高性能 Agent | ecs.c6.2xlarge | 8C / 16G | ~¥1.56/h | ML 研究等重负载 |
| 抢占式（推荐） | ecs.c6.large (Spot) | 2C / 4G | ~¥0.06-0.12/h | 可中断场景，节省 70-85% |

### 4.3 Infrastructure as Code (IaC) 策略

> **结论：MVP 阶段引入 Terraform 管理基础网络资源（VPC/安全组/密钥对），实例生命周期仍用 SDK 直连。**

#### 为什么需要 IaC

SDK 直连管理 **实例生命周期**（创建/启动/停止/销毁）非常合适，但 **基础网络资源**（VPC、子网、安全组、NAT Gateway、密钥对）是一次性创建、长期存在的，手动管理容易出错且不可重现。

| 资源类型 | 管理方式 | 理由 |
|---------|---------|------|
| VPC、子网 (VSwitch)、路由表 | **Terraform** | 一次性创建，环境间一致性要求高 |
| 安全组及规则 | **Terraform** | 安全策略需要版本控制和审计 |
| 密钥对 | **Terraform** | 一次性创建 |
| NAT Gateway（IP 亲和性） | **Terraform** | 多 NAT 场景拓扑复杂 |
| ECS 实例 (Worker) | **SDK 直连** | 动态生命周期，按需创建/销毁 |
| EIP（弹性 IP） | **SDK 直连** | 跟随实例动态分配/回收 |

#### 双工具方案：阿里云 Terraform + AWS CDK Python

```
infra/
├── aliyun/                         # Terraform (HCL) — 阿里云没有 CDK 等价物
│   ├── modules/networking/         # VPC, VSwitch, 安全组, NAT, 密钥对
│   └── environments/cn-hangzhou/   # provider "alicloud", terraform.tfvars, backend(OSS)
│
└── aws/                            # CDK Python — 与项目同语言，L2 Construct 高效
    ├── app.py                      # CDK App 入口
    ├── stacks/networking_stack.py  # VPC + NAT + SG 一个 ec2.Vpc() 调用搞定
    └── cdk.json
```

**为什么 AWS 不用 Terraform？** CDK Python 的 `ec2.Vpc(max_azs=2, nat_gateways=1)` 一行代码等价于 Terraform 的 ~15 个 resource block。项目全栈 Python，CDK 与主代码共享类型系统。后续 Phase 需要 Lambda/SSM/ASG 时 CDK 的 L2 Construct 优势更大。

#### AWS CDK 的定位

AWS CDK Python 与项目全栈 Python 一致，L2 Construct 的抽象层级远高于 Terraform（VPC + NAT + 路由表一个调用 vs 15 个 resource block），且后续 Lambda/SSM/ASG 集成 CDK 有碾压性优势。阿里云没有 CDK 等价物，因此采用 **双工具策略：阿里云 Terraform + AWS CDK Python**。

| 工具 | 阿里云 | AWS | MVP 推荐 |
|------|--------|-----|---------|
| Terraform | ✅ alicloud provider | ✅ aws provider | ✅ **阿里云侧使用** |
| AWS CDK Python | ❌ 不支持 | ✅ 原生最佳 | ✅ **AWS 侧使用** |
| Pulumi | ✅ alicloud provider | ✅ aws provider | 备选 |

### 4.4 关键考虑

- **阿里云是 MVP 首选**：项目的首要部署环境是中国大陆，阿里云是必须优先实现的云厂商
- 双云策略（阿里云国内 + AWS 全球）时，Terraform 提供统一的 IaC 抽象层
- 阿里云 ESS 直接支持生命周期钩子，可触发自定义操作
- 阿里云云助手（类似 SSM）可在后续替代 SSH，MVP 阶段先用 SSH

---

## 5. 开源方案分析

### 5.1 基础设施即代码 (IaC)

| 工具 | 优势 | 劣势 | 推荐度 |
|------|------|------|--------|
| **Terraform** | 3000+ provider；HCL 声明式，广泛使用；成熟的模块注册表 | 状态管理需要小心（remote state）；HCL 逻辑表达能力有限 | ★★★★ |
| **Pulumi** | 用 Python/TS 写 IaC；适合复杂逻辑（如凭证路由、IP 分配）；完整测试支持 | 生态小于 Terraform | ★★★★★ |
| **CDK** | AWS 原生最高级抽象；Python/TS；可测试 | 仅 AWS | ★★★ |

**推荐：** 如果需要多云（AWS + 阿里云），选 Pulumi 或 Terraform。如果只做 AWS，CDK 也是很好的选择。

### 5.2 容器编排与自动扩展

| 工具 | 定位 | 适用场景 | 复杂度 |
|------|------|---------|--------|
| **KEDA** | K8s 事件驱动扩展器 | 基于队列深度、自定义指标扩展 Pod；支持 scale-to-zero | 中（需要 K8s） |
| **Knative** | K8s 上的 serverless 容器 | HTTP 触发，自动 scale-to-zero；不适合长时运行的 Agent 进程 | 中 |
| **HashiCorp Nomad** | 简单编排器 | 单二进制文件，远比 K8s 简单；支持容器+原生二进制；内置 GPU 调度 | 低 |
| **Ray** | AI 计算引擎 | OpenAI 在用；自动扩展节点；对本用例来说过于重量级 | 高 |

### 5.3 AI Agent 框架

| 框架 | 说明 | 相关性 |
|------|------|--------|
| AutoGen v0.4 | 异步事件驱动架构，支持分布式执行 | 中 — Agent 编排层，非基础设施层 |
| LangGraph | 有状态多 Agent 编排（24.8k stars） | 中 — 同上 |
| CrewAI | 最快原型化多 Agent 系统 | 低 — 侧重 Agent 逻辑而非基础设施 |

### 5.4 相关 Docker 镜像

| 镜像 | 说明 |
|------|------|
| `ghcr.io/anthropics/claude-code` | 官方 Claude Code Docker 镜像（多架构，版本标签） |
| `openai/codex-universal` | Codex CLI 参考镜像（Ubuntu 24.04 基础） |
| `claude-contained` | 开源项目 — 在 Docker 中运行 Claude Code/Codex |

### 5.5 结论：有没有现成的？

**没有现成的完整解决方案。** 目前不存在一个框架能同时满足：
1. AI Agent CLI 工具的弹性扩缩容
2. 账号凭证的自动分发和轮换
3. IP 亲和性风控
4. Harness 插件化

最接近的是 Kubernetes + KEDA + External Secrets Operator 的组合，但仍需要大量定制开发。自建框架是合理的选择。

---

## 6. EC2 vs 容器 vs Serverless 对比

### 6.1 全面对比表

| 维度 | EC2（裸机） | ECS on EC2 | ECS Fargate | EKS | Lambda |
|------|------------|-----------|-------------|-----|--------|
| 启动时间 | 40-60s（优化 AMI 5s） | 10-30s（热主机） | 20-60s（SOCI 5s） | 10-30s（热节点） | N/A（15min 上限） |
| 完整 Linux 环境 | 是 | 是（容器内） | 是（容器内） | 是（容器内） | 否 |
| Claude Code 支持 | 原生 | Docker 镜像 | Docker 镜像 | Docker 镜像 | **不适合** |
| IP 稳定性 | EIP 或 NAT GW | NAT GW | NAT GW | NAT GW | 无法控制 |
| 节点隔离 | 完全 VM 隔离 | 容器隔离 | 容器 + microVM | 容器隔离 | 函数隔离 |
| 状态管理 | EBS + EFS | EBS + EFS | 仅 EFS | EBS + EFS + PV | 无状态 |
| 最大资源 | 448 vCPU / 24TB | 受实例限制 | 4 vCPU / 30GB | 受实例限制 | 10GB / 15min |
| 稳态成本 | 最低（RI/SP） | 低 | 中 | 中 + $73/月 | 按调用 |
| 突发成本 | Spot 节省 | Spot 节省 | 无 Spot | Spot + Karpenter | 按调用 |
| 运维负担 | 高 | 中 | 低 | 高 | 低 |
| 多云可移植 | 否 | 否 | 否 | 是（K8s 可移植） | 否 |

### 6.2 关键结论

1. **Lambda 不适合** — 15 分钟执行限制、无持久 shell，无法运行 Claude Code
2. **Fargate 适合中小规模** — 官方 Docker 镜像可直接使用（2 vCPU / 4 GB 推荐），运维最少
3. **EC2 适合大规模** — 最大控制权但运维负担最重
4. **EKS 适合多云** — 灵活性与可移植性最佳平衡，但学习曲线陡

---

## 7. 纯 EC2 方案的缺陷与不足

### 7.1 冷启动延迟

| 场景 | 延迟 | 说明 |
|------|------|------|
| 默认启动 | 40-60 秒 | AMI 启动 + cloud-init + user-data |
| 优化 AMI | ~5 秒 | 预装所有依赖、移除 cloud-init、快速启动 EBS 快照 |
| Warm Pool | 亚秒级 | 预热的已停止实例池，仅 EBS 存储费用 |

参考 agent-ml-research 的 bootstrap，完整初始化（含 `uv sync`、`npm ci`、Playwright 安装）需要 **5-10 分钟**。这在 EC2 方案下每次扩容都需要承受。

**优化方案：** 全部预装进 AMI → 启动后仅需 SCP 配置 + 启动服务，可缩短到 1-2 分钟。但 AMI 更新维护成本增加。

### 7.2 IP 地址管理复杂

| 问题 | 说明 |
|------|------|
| EIP 配额 | 默认每 region 5 个（可申请提升但有上限） |
| EIP 费用 | 未关联实例时 $0.005/小时；关联后免费 |
| NAT Gateway 费用 | $0.045/小时 + $0.045/GB 数据处理 ≈ $32.40/月/Gateway |
| 多 NAT Gateway 复杂度 | 每个 NAT Gateway 需独立子网 + 路由表，配置和管理成本随账号组增长 |

### 7.3 成本问题

| 实例类型 | On-Demand | Spot (约) | 月成本 (24/7) |
|---------|-----------|-----------|------------|
| t3.small (2c/2G) | $0.0116/h | $0.0035/h | $8.35 / $2.52 |
| t3.medium (2c/4G) | $0.0232/h | $0.007/h | $16.70 / $5.04 |
| t3.large (2c/8G) | $0.0464/h | $0.014/h | $33.41 / $10.08 |

- 空闲实例的持续费用（即使无任务）
- Spot 实例可节省 70% 但有中断风险
- Warm Pool（已停止实例）仅 EBS 费用 (~$0.10/GB/月)

### 7.4 AMI 管理开销

- 自定义 AMI 需要预装 Claude Code、Codex、Node.js、Python、git 等
- AMI 需要定期更新（Claude Code 版本升级、安全补丁）
- 推荐使用 EC2 Image Builder 自动化 AMI 流水线（构建 → 测试 → 分发）

### 7.5 安全性问题

| 问题 | 说明 | 推荐方案 |
|------|------|---------|
| SSH key 管理 | 所有 EC2 共用密钥对，泄露影响全集群 | **消除 SSH**：使用 SSM Session Manager |
| 入站端口 | SSH 需要 22 端口开放 | SSM 不需要任何入站端口 |
| 凭证传输 | SCP 传输凭证文件，中间人风险 | Secrets Manager + IAM Role |
| 审计追踪 | SSH 操作难以审计 | SSM 操作全记录在 CloudTrail |

### 7.6 状态管理困难

- EBS 卷绑定 AZ，实例在其他 AZ 重建后无法直接挂载
- 缩容时数据备份需要额外逻辑（生命周期钩子 → Lambda → S3）
- 扩容时数据恢复也需要额外逻辑（从 S3 拉取 → 解压到 EBS）

### 7.7 扩展性瓶颈

- 文件系统状态（YAML/JSON）在高并发下性能下降
- Manager 单点 + SSH 管理在实例数超过 50 时效率显著下降
- 无服务发现机制，Manager 需要维护静态的后端列表

### 7.8 更好的替代方案？

| 方案 | 优势 | 劣势 | 推荐场景 |
|------|------|------|---------|
| **ECS on EC2** | 容器隔离 + EC2 成本优势 + SSH/SSM 可用 | 需管理底层 EC2 | **MVP 首选** — 兼顾灵活性和可控性 |
| **ECS Fargate** | 零运维、快速启动 | 无 Spot、无 SSH、仅 EFS | 小规模、低成本敏感场景 |
| **EKS + KEDA** | 多云可移植、事件驱动扩展、scale-to-zero | $73/月集群费 + K8s 运维复杂度 | 多云需求、大规模、团队有 K8s 经验 |
| **Nomad** | 极简部署（单二进制）、支持原生进程 | 生态小、社区小 | 简单场景、不想学 K8s |

---

## 8. 推荐架构方案

### 8.1 MVP 方案：SDK 直连管理（最简方案）

**核心思路：** 直接用云厂商 SDK（阿里云 `alibabacloud_ecs20140526` / AWS `boto3`）管理实例，不引入 ASG、Lambda、Secrets Manager 等额外云服务。这正是 agent-ml-research 已验证可行的方案 — 一个 Python 进程 + SDK 就能跑起来。

**阿里云优先：** MVP 阶段优先实现阿里云 ECS Provider，AWS EC2 Provider 同步实现但作为第二优先级。基础网络资源（VPC、安全组等）通过 Terraform 管理。

```
┌──────────────────────────────────────────────────┐
│              Manager 节点 (EC2 / 本地机器)        │
│                                                  │
│  ┌──────────┐  ┌──────────┐  ┌────────────────┐  │
│  │ FastAPI  │  │ React UI │  │  调度引擎       │  │
│  │ 后端 API │  │ 前端     │  │  (Python 进程) │  │
│  └────┬─────┘  └──────────┘  └───────┬────────┘  │
│       │                              │           │
│  ┌────▼──────────────────────────────▼────────┐  │
│  │              核心管理模块                   │  │
│  │  ┌────────────┐  ┌───────────┐             │  │
│  │  │阿里云SDK / │  │ 凭证池    │             │  │
│  │  │boto3 直连  │  │ (JSON/DB) │             │  │
│  │  └────────────┘  └───────────┘             │  │
│  │  ┌────────────┐  ┌───────────┐             │  │
│  │  │节点注册表  │  │ 额度监控  │             │  │
│  │  │(YAML/DB)  │  │ (轮询)    │             │  │
│  │  └────────────┘  └───────────┘             │  │
│  │  ┌────────────┐  ┌───────────┐             │  │
│  │  │外部服务API │  │ Terraform │             │  │
│  │  │(轨迹/文件)│  │ (基础资源)│             │  │
│  │  └────────────┘  └───────────┘             │  │
│  └────────────────────────────────────────────┘  │
└──────────────────────┬───────────────────────────┘
                       │ SSH / Worker Runtime
         ┌─────────────┼─────────────┐
         │             │             │
    ┌────▼───┐    ┌────▼───┐    ┌────▼───┐
    │Worker 1│    │Worker 2│    │Worker N│
    │阿里云  │    │阿里云  │    │AWS EC2 │
    │        │    │        │    │        │
    │Claude  │    │Claude  │    │Codex   │
    │ Code   │    │ Code   │    │        │
    │        │    │        │    │        │
    │Harness │    │Harness │    │Harness │
    └────────┘    └────────┘    └────────┘
```

#### 工作流程

```
扩容:
  Manager API 收到请求
    → aliyun SDK RunInstances() / boto3.run_instances() 创建实例
    → 等待 running 状态
    → SSH 连入执行 bootstrap 脚本
        → 安装 Agent (Claude Code / Codex)
        → SCP 分发凭证文件
        → git clone 部署 Harness 代码
        → systemd 启动服务
    → 写入节点注册表
    → 开始心跳监控

缩容:
  Manager API 收到请求
    → （可选）SSH 触发数据备份到 S3 / OSS
    → aliyun SDK DeleteInstance() / boto3.terminate_instances() 释放实例
    → 回收凭证到池子
    → 从注册表中移除

额度监控:
  Manager 后台轮询
    → SSH / HTTP 调用每个 Worker 的 watchdog 状态
    → 发现额度不足 → 从凭证池取新号
    → SCP 新凭证到 Worker → 重启 Agent 服务
    → 回收旧号到池子
```

#### 核心组件实现

| 功能 | 实现方式 | 说明 |
|------|---------|------|
| 创建/销毁节点 | `alibabacloud_ecs.RunInstances()` / `DeleteInstance()` | **阿里云优先** |
| | `boto3.run_instances()` / `terminate_instances()` | AWS 同步实现 |
| 节点初始化 | SSH + bootstrap 脚本 | 装 Agent、部署代码、分发凭证 |
| 状态监控 | 后台轮询 `describe_instances()` + Worker 心跳 | 60 秒间隔，同步 IP / 状态变化 |
| 凭证存储 | 本地 JSON 文件（MVP）→ 数据库（后续） | 凭证池 + 账号-IP 映射 |
| 凭证分发 | SCP 推送凭证文件到 Worker | 简单直接 |
| 额度监控 | Worker 侧 watchdog 脚本 + Manager 轮询 | 沿用 agent-ml-research 的 watchdog |
| 扩缩容触发 | Manager 后端 API 手动触发 | 未来加规则引擎实现自动化 |
| Harness 部署 | SSH + `git clone` 用户 repo | Worker 启动后自动拉取 |
| 节点注册表 | YAML 文件（MVP）→ 数据库（后续） | 记录实例 ID、IP、状态、绑定的凭证 |

#### 与 AWS 原生服务方案的对比

| 维度 | SDK 直连（本方案） | ASG + Lambda + SSM + Secrets Manager |
|------|-------------------|--------------------------------------|
| **实现复杂度** | 低 — 一个 Python 进程 + SDK | 高 — 需理解 ASG 生命周期钩子、Lambda 部署、SSM 配置 |
| **调试便捷性** | 高 — 所有逻辑在一个进程，直接看日志 | 低 — 逻辑分散在 Lambda、CloudWatch Logs 等多处 |
| **自定义灵活度** | 高 — IP 亲和性、智能分发直接写 Python | 中 — 需要绕 AWS 事件机制和 Lambda 限制 |
| **多云统一性** | 高 — 阿里云 / AWS 各实现一个 Provider，上层逻辑共享 | 低 — ASG、Lambda、SSM 都是 AWS 专属 |
| **额外云服务费用** | 无 — 只付 ECS/EC2 计算费用 | 有 — Secrets Manager ($0.40/secret/月)、NAT Gateway ($32/月)、DynamoDB |
| **开发速度** | 快 — agent-ml-research 已有参考实现可复用 | 慢 — 需要学习和配置多个 AWS 服务 |
| **IaC 集成** | Terraform 管理基础网络，SDK 管理实例 | 全托管但灵活度低 |
| **高可用** | 弱 — Manager 单点故障 | 中 — Lambda 本身高可用，但整体仍需 Manager |
| **安全性** | 中 — SSH key 共享、凭证明文传输 | 高 — SSM 无需 SSH、Secrets Manager 加密存储 |
| **可扩展性** | 中 — 文件系统状态 50+ 实例后可能瓶颈 | 高 — DynamoDB / Secrets Manager 原生可扩展 |

**结论：** MVP 阶段选 SDK 直连，**先跑起来再迭代**。Manager 单点故障、SSH key 共享、文件状态这些问题在 50 个实例以内完全可以接受。当规模增长时，再逐步引入云原生服务增强。

#### SDK 直连方案的代码结构

```python
# 最简 Provider 接口 — 直接封装云厂商 SDK
class CloudProvider(ABC):
    """云服务商接口"""
    async def create_instance(self, config: InstanceConfig) -> Instance: ...
    async def start_instance(self, instance_id: str) -> None: ...
    async def stop_instance(self, instance_id: str) -> None: ...
    async def terminate_instance(self, instance_id: str) -> None: ...
    async def describe_instances(self, filters: dict) -> list[Instance]: ...

# AWS EC2 实现 — 直接用 boto3
class AWSEc2Provider(CloudProvider):
    def __init__(self, region: str, **kwargs):
        self.ec2 = boto3.client('ec2', region_name=region)

    async def create_instance(self, config):
        resp = self.ec2.run_instances(
            ImageId=config.ami_id,
            InstanceType=config.instance_type,
            MinCount=1, MaxCount=1,
            KeyName=config.key_pair_name,
            SecurityGroupIds=config.security_group_ids,
            SubnetId=config.subnet_id,
            TagSpecifications=[{
                'ResourceType': 'instance',
                'Tags': [
                    {'Key': 'Name', 'Value': f'Worker-{config.name}'},
                    {'Key': 'ManagedBy', 'Value': 'elastic-agent'},
                ]
            }],
        )
        return Instance.from_aws(resp['Instances'][0])

# 阿里云 ECS 实现 — 封装 alibabacloud SDK
class AliyunEcsProvider(CloudProvider):
    def __init__(self, region: str, **kwargs):
        self.client = EcsClient(config)

    async def create_instance(self, config):
        request = CreateInstanceRequest()
        request.set_ImageId(config.image_id)
        request.set_InstanceType(config.instance_type)
        # ...
        resp = self.client.do_action_with_exception(request)
        return Instance.from_aliyun(resp)
```

```python
# Manager 核心逻辑 — 一个 Python 进程搞定全部
class ElasticAgentManager:
    def __init__(self, provider: CloudProvider, credential_pool: CredentialPool):
        self.provider = provider
        self.credentials = credential_pool
        self.registry = NodeRegistry()  # YAML/JSON 文件

    async def scale_out(self, count: int = 1):
        for _ in range(count):
            # 1. SDK 直接创建实例
            instance = await self.provider.create_instance(self.config)

            # 2. 等待 running
            await self._wait_until_running(instance.id)

            # 3. 从凭证池选一个号（优先之前在这个 IP 用过的）
            cred = self.credentials.select(prefer_ip=instance.public_ip)

            # 4. SSH bootstrap: 装 agent + 分发凭证 + 部署 harness
            await self._bootstrap(instance, cred)

            # 5. 注册到本地注册表
            self.registry.add(instance, cred)

    async def scale_in(self, count: int = 1):
        victims = self.registry.select_for_removal(count)
        for instance in victims:
            # 1. 回收凭证
            self.credentials.release(instance.credential_id)
            # 2. (可选) 备份数据
            await self._backup_data(instance)
            # 3. SDK 直接终止实例
            await self.provider.terminate_instance(instance.id)
            # 4. 从注册表移除
            self.registry.remove(instance.id)
```

#### MVP 阶段接受的已知限制

| 限制 | 影响 | 后续解决方案 |
|------|------|-------------|
| Manager 单点故障 | Manager 挂了无法管理节点，但 Worker 继续运行 | 后续加高可用（多副本 + 负载均衡） |
| SSH key 共享 | 一个 key 泄露影响所有节点 | 后续引入 SSM Session Manager |
| 凭证明文传输 | SCP 传输凭证文件有中间人风险 | 后续引入 Secrets Manager |
| 文件系统状态 | YAML/JSON 在 50+ 实例时性能下降 | 后续引入数据库（PostgreSQL / SQLite） |
| 手动扩缩容 | 需要通过 API/UI 手动触发 | 后续加规则引擎自动化 |
| IP 亲和性简单实现 | 仅基于历史记录的软绑定 | 后续引入多 NAT Gateway 硬绑定 |

### 8.2 进阶方案：SDK 直连 + 云原生服务增强

在 MVP 基础上，针对痛点逐步引入云原生服务：

| 痛点 | 引入的服务 | 说明 |
|------|-----------|------|
| SSH key 安全 | SSM Session Manager / Run Command | 消除 SSH，无需开放 22 端口 |
| 凭证安全 | Secrets Manager / 阿里云凭据管家 | 加密存储 + 自动轮换 |
| IP 硬绑定 | 多 NAT Gateway + 子网路由 | 每个账号组固定出站 IP |
| 状态可扩展 | DynamoDB / RDS | 替代 YAML/JSON 文件 |
| 数据备份 | EBS 快照 + S3 / OSS | 缩容时自动触发 |

核心管理逻辑仍然是 SDK 直连（boto3 / aliyun SDK），只是把周边能力替换为托管服务。这样可以逐步增强而不需要推翻 MVP 的架构。

### 8.3 容器化方案：ECS on EC2

在进阶方案基础上将 Worker 容器化：

- Worker 使用官方 `ghcr.io/anthropics/claude-code` Docker 镜像为基础
- ECS Task Definition 中注入凭证（引用 Secrets Manager）
- 容器内运行 Claude Code + 用户 Harness
- 底层 EC2 由 ASG 管理，支持 Spot
- 更好的资源隔离和快速部署

### 8.4 远期方案：EKS + KEDA 多云

当规模增长或需要多云时：

- EKS on AWS + ACK on 阿里云
- KEDA 基于任务队列深度自动扩展 Agent Pod
- External Secrets Operator 同步 Secrets Manager / 阿里云 KMS
- 多 NodeGroup + 不同子网实现 IP 亲和性
- Helm Chart 打包，一键部署

### 8.5 路由功能设计

Manager 作为服务入口（Router）：

```
外部请求 → Manager (API Gateway / Nginx)
              │
              ├── 业务逻辑处理（传统后端）
              │
              └── AI Agent 请求路由
                    │
                    ├── 负载均衡（最少连接 / 加权轮询）
                    ├── 亲和性路由（特定任务 → 特定 Worker）
                    ├── 健康检查（排除不健康 Worker）
                    └── 限流（防止单 Worker 过载）
```

实现方式：
- MVP：FastAPI 内置路由 + httpx 转发
- 进阶：Nginx/Envoy 反向代理 + 动态上游配置
- 远期：API Gateway + Service Mesh (Istio)

---

## 9. MVP 实现计划

### 9.1 模块划分

```
elastic-agent/
├── docs/                           # 文档
├── src/
│   ├── core/                       # 核心抽象
│   │   ├── providers/              # 云服务商接口
│   │   │   ├── base.py             # CloudProvider 抽象基类
│   │   │   ├── aliyun_ecs.py       # 阿里云 ECS 实现（MVP 首选）
│   │   │   └── aws_ec2.py          # AWS EC2 实现
│   │   ├── agents/                 # Agent 类型接口
│   │   │   ├── base.py             # AgentType 抽象基类
│   │   │   ├── claude_code.py      # Claude Code 实现
│   │   │   └── codex.py            # Codex 实现（未来）
│   │   ├── credentials/            # 凭证管理
│   │   │   ├── agent_credentials.py    # Agent 凭证池（账号登录态）
│   │   │   ├── app_credentials.py      # 应用凭证（Git key, API key 等）
│   │   │   ├── provider.py             # CredentialProvider 抽象基类
│   │   │   ├── claude_oauth.py         # Claude OAuth 登录
│   │   │   └── api_key.py              # 简单 API Key 分发
│   │   ├── runtime/                # Worker 执行运行时（框架核心）
│   │   │   ├── worker_runtime.py   # Worker 侧运行时（接收命令、启动进程、流式日志）
│   │   │   ├── log_transport.py    # Worker→Manager 日志传输通道
│   │   │   └── protocol.py         # Manager↔Worker 通信协议定义
│   │   ├── bootstrap/              # 节点初始化
│   │   │   ├── pipeline.py         # 可插拔初始化管道
│   │   │   └── steps/              # 各初始化步骤
│   │   ├── scheduler/              # 扩缩容调度
│   │   │   ├── engine.py           # 调度引擎
│   │   │   ├── rules.py            # 规则引擎（基于规则的扩缩容）
│   │   │   ├── signals.py          # Harness 上报的扩缩容信号
│   │   │   └── drain.py            # 优雅缩容 Drain 机制
│   │   ├── affinity/               # 亲和性调度
│   │   │   ├── ip_affinity.py      # 账号-IP 亲和性
│   │   │   └── worker_affinity.py  # 有状态工作负载的 Worker 亲和性
│   │   ├── workspace/              # 工作区管理
│   │   │   ├── sync.py             # 项目代码同步（git clone / rsync）
│   │   │   └── backup.py           # 缩容时数据备份、扩容时恢复
│   │   ├── monitor/                # 监控
│   │   │   ├── quota.py            # 额度监控
│   │   │   ├── health.py           # Worker 应用级健康检查
│   │   │   ├── metrics.py          # 指标收集（资源、成本、任务）
│   │   │   └── events.py           # 事件总线（node_added, drain_start, ...）
│   │   ├── router/                 # 请求路由
│   │   │   └── dispatcher.py       # 请求分发到 Worker
│   │   ├── registry/               # 节点注册
│   │   │   └── store.py            # 节点状态存储
│   │   ├── external_api/           # 外部服务 API（轨迹/文件传输）
│   │   │   ├── traces.py           # Agent 轨迹/chat 记录实时流
│   │   │   ├── files.py            # Worker 文件实时传输
│   │   │   └── auth.py             # 外部 API 认证（API Key / OAuth）
│   │   └── security/               # 安全
│   │       ├── auth.py             # Manager↔Worker 认证
│   │       └── transport.py        # 传输加密
│   ├── manager/                    # Manager 节点
│   │   ├── api/                    # REST API
│   │   ├── service.py              # FastAPI 应用
│   │   └── config.py               # 配置模型
│   ├── worker/                     # Worker 节点
│   │   ├── runtime_server.py       # Worker Runtime HTTP/WS 服务
│   │   ├── process_manager.py      # 本地进程管理（启动/停止/监控）
│   │   ├── watchdog.py             # 额度监控看门狗
│   │   └── reporter.py             # 状态/日志上报
│   └── cli/                        # CLI 工具
│       └── main.py                 # 命令行入口
├── dashboard/                      # 前端 UI
│   └── (React + Vite + Ant Design)
├── scripts/                        # 部署脚本
│   ├── watchdog.sh                 # Worker 侧 watchdog
│   └── bootstrap.sh                # Worker 初始化脚本
├── infra/                          # IaC
│   ├── aliyun/                     # Terraform（阿里云）
│   │   ├── modules/networking/     # VPC、子网、安全组
│   │   └── environments/cn-hangzhou/
│   └── aws/                        # CDK Python（AWS）
│       ├── stacks/networking_stack.py
│       └── app.py
├── examples/                       # 示例 Harness
│   ├── claude-code-manager/        # CCM 的 Harness 示例
│   └── agent-ml-research/          # agent-ml-research 的 Harness 示例
├── pyproject.toml
└── README.md
```

### 9.2 核心接口定义（草案）

```python
# ── 云资源管理 ──

class CloudProvider(ABC):
    async def create_instance(self, config: InstanceConfig) -> Instance: ...
    async def start_instance(self, instance_id: str) -> None: ...
    async def stop_instance(self, instance_id: str) -> None: ...
    async def terminate_instance(self, instance_id: str) -> None: ...
    async def list_instances(self, filters: dict) -> list[Instance]: ...
    async def get_instance(self, instance_id: str) -> Instance: ...

# ── Agent 类型 ──

class AgentType(ABC):
    def get_install_commands(self) -> list[str]: ...
    def get_start_commands(self) -> list[str]: ...
    def get_health_check(self) -> HealthCheck: ...
    def get_quota_check(self, credential: Credential) -> QuotaStatus: ...

# ── 凭证管理（双层） ──

class CredentialProvider(ABC):
    """Agent 凭证 — Claude Code/Codex 的账号登录态"""
    async def login(self, account: Account, instance: Instance) -> Credential: ...
    async def check_quota(self, credential: Credential) -> QuotaStatus: ...
    async def refresh(self, credential: Credential) -> Credential: ...
    async def revoke(self, credential: Credential) -> None: ...

class AppCredentialStore(ABC):
    """应用凭证 — Git key、API key 等 Harness 业务所需"""
    async def get(self, name: str) -> str: ...
    async def set(self, name: str, value: str) -> None: ...
    async def inject_to_env(self, names: list[str]) -> dict[str, str]: ...

# ── Worker 执行运行时（框架核心） ──

class WorkerRuntime(ABC):
    """运行在每个 Worker 上，接收命令、执行进程、流式回传日志"""
    async def execute(self, cmd: list[str], cwd: str, env: dict,
                      timeout: int | None = None) -> AsyncIterator[LogEvent]: ...
    async def stop(self, pid: int) -> None: ...
    async def status(self) -> WorkerStatus: ...
    async def health(self) -> HealthReport: ...

class LogEvent:
    timestamp: datetime
    stream: Literal["stdout", "stderr"]
    data: str               # 原始行内容
    worker_id: str
    task_id: str | None

# ── 亲和性调度 ──

class AffinityPolicy(Enum):
    NONE = "none"           # 任意 Worker
    PREFERRED = "preferred" # 优先同一 Worker，不可用时允许其他
    REQUIRED = "required"   # 必须同一 Worker，不可用时等待或报错

class AffinityKey:
    """标识一个亲和性绑定"""
    key: str                # 如 session_id、project_id
    worker_id: str          # 绑定到的 Worker
    policy: AffinityPolicy

# ── 优雅生命周期 ──

class DrainPolicy:
    timeout: int = 1800             # 最长等待时间（秒），超时强制终止
    backup_before_terminate: bool = True
    notify_harness: bool = True     # 触发 Harness 的 on_drain 回调

# ── 扩缩容信号 ──

class ScalingSignal:
    """Harness 上报的扩缩容信号"""
    pending_tasks: int = 0
    idle_workers: int = 0
    busy_workers: int = 0
    avg_wait_time_seconds: float = 0
    custom_metrics: dict[str, float] = {}

# ── 节点初始化 ──

class BootstrapStep(ABC):
    name: str
    async def execute(self, ctx: BootstrapContext) -> StepResult: ...
    async def rollback(self, ctx: BootstrapContext) -> None: ...

# ── 事件系统 ──

class FrameworkEvent(Enum):
    NODE_CREATING = "node_creating"
    NODE_READY = "node_ready"
    NODE_DRAIN_START = "node_drain_start"
    NODE_TERMINATING = "node_terminating"
    CREDENTIAL_ROTATED = "credential_rotated"
    CREDENTIAL_EXHAUSTED = "credential_exhausted"
    QUOTA_WARNING = "quota_warning"
    BOOTSTRAP_FAILED = "bootstrap_failed"
    WORKER_UNHEALTHY = "worker_unhealthy"

class EventHandler(ABC):
    async def handle(self, event: FrameworkEvent, data: dict) -> None: ...

# ── 外部服务 API（框架内置） ──

class TraceEvent:
    """Agent 轨迹事件（一条 stream-json 输出行）"""
    timestamp: datetime
    worker_id: str
    task_id: str | None
    session_id: str | None
    event_type: str             # "assistant", "tool_use", "tool_result", "result", ...
    content: dict               # 原始事件内容
    metadata: dict              # 扩展字段

class ExternalAPI(ABC):
    """框架向外部服务暴露的标准 API"""
    async def stream_traces(self, node_id: str) -> AsyncIterator[TraceEvent]: ...
    async def get_traces(self, node_id: str, since: datetime, limit: int) -> list[TraceEvent]: ...
    async def read_file(self, node_id: str, path: str) -> bytes: ...
    async def watch_files(self, node_id: str, paths: list[str]) -> AsyncIterator[FileChange]: ...

class FileChange:
    path: str
    event: Literal["modified", "created", "deleted"]
    content: str | None         # 文本文件内容，二进制文件为 None
    timestamp: datetime
    worker_id: str

# ── Harness 接口 — 用户实现 ──

class Harness(ABC):
    def get_repo_url(self) -> str | None: ...
    def get_bootstrap_steps(self) -> list[BootstrapStep]: ...
    def get_service_definitions(self) -> list[ServiceDefinition]: ...
    def get_app_credentials(self) -> list[str]: ...         # 需要注入的应用凭证名
    def get_scaling_signal(self) -> ScalingSignal: ...       # 上报扩缩容信号
    def get_event_handlers(self) -> dict[FrameworkEvent, EventHandler]: ...  # 事件回调
```

### 9.3 智能分发与风控实现

**IP 亲和性调度算法：**

```python
def select_subnet_for_account(account_id: str) -> str:
    """选择账号应该分配到的子网（决定出站 IP）"""
    # 1. 查询历史记录：这个账号之前分配在哪个子网？
    history = db.get_account_history(account_id)
    if history and history.last_subnet:
        # 优先分配到相同子网（相同出站 IP）
        if has_capacity(history.last_subnet):
            return history.last_subnet

    # 2. 如果之前的子网没有容量，选择使用最少 IP 的子网
    subnets = get_available_subnets()
    return min(subnets, key=lambda s: s.unique_account_count)
```

**凭证轮换策略：**

```python
def select_credential_for_instance(instance_id: str) -> Credential:
    """为实例选择最佳凭证"""
    instance = registry.get(instance_id)

    # 1. 优先选择之前在这个 IP 上用过的账号
    previous = credential_store.get_by_ip(instance.ip)
    for cred in previous:
        if cred.quota_remaining > THRESHOLD:
            return cred

    # 2. 选择额度最多且 IP 记录最少的账号
    available = credential_store.get_available()
    return min(available, key=lambda c: (c.ip_count, -c.quota_remaining))
```

### 9.4 应用示例

框架设计基于两个真实项目验证，每个项目有独立的详细 Harness 集成文档：

| 项目 | 接入类型 | 详细文档 |
|------|---------|---------|
| **agent-ml-research** | 替换自建基础设施（~2000 行 → ~300 行） | [harness-example-agent-ml-research.md](harness-example-agent-ml-research.md) |
| **Claude Code Manager** | 从单机扩展到分布式 | [harness-example-claude-code-manager.md](harness-example-claude-code-manager.md) |

两个项目代表了两种典型的接入模式：

- **agent-ml-research**：已有自建 EC2 管理 + 账号管理代码，需要**替换**为框架调用。迁移后删除 ~2000 行自建基础设施代码，同时获得 IP 亲和性、优雅缩容、健康检查等自建版本缺失的能力。

- **Claude Code Manager**：单机单体应用，需要**新增**分布式能力。改造重点是将本地子进程执行切换为远程 Worker Runtime 调用，框架提供全部基础设施层。

两个 Harness 的核心需求（Worker Runtime、日志传输、有状态亲和性、优雅缩容、双层凭证、扩缩容信号）完全一致，验证了框架核心设计的普适性。

**框架调用示例：**

```python
from elastic_agent import ElasticAgentManager, AliyunEcsProvider, CredentialPool

# 阿里云优先
manager = ElasticAgentManager(
    provider=AliyunEcsProvider(config),  # 或 AWSEc2Provider(config)
    credential_pool=CredentialPool(provider=ClaudeOAuthProvider(email_tokens)),
    harness=AgentMLResearchHarness(app_config),  # 或 CCMHarness(app_config)
)

await manager.scale_out(count=3)            # 扩容
await manager.scale_in(count=1)             # 缩容（自动 Drain）
status = await manager.get_cluster_status() # 状态总览

# 外部服务可通过 API 获取实时数据
traces = await manager.external_api.get_traces(node_id="worker-1")
async for event in manager.external_api.stream_traces(node_id="worker-1"):
    print(event)  # 实时 Agent 轨迹
```

---

## 10. 框架设计完整性审查

> 基于对 [agent-ml-research](harness-example-agent-ml-research.md)、[Claude Code Manager](harness-example-claude-code-manager.md) 和 [Audiobook Production](harness-example-audiobook-production.md) 三个实际 Harness 案例的分析，以及框架自身架构的审视，识别出以下设计缺口。每项缺口都在多个案例中得到了验证。

### 10.1 已识别的关键缺口

#### (1) Manager 崩溃恢复与操作幂等性

**问题：** 如果 Manager 在 `scale_out()` 过程中崩溃 — EC2 已创建但注册表未更新 — 会产生"孤儿实例"：云上有机器在跑，但 Manager 不知道它的存在。

**影响：** 孤儿实例持续计费但无人管理。更严重的是，上面可能绑定了一个凭证，但凭证池不知道它已被使用，可能导致同一凭证分配给两台机器。

**设计方案：**

| 机制 | 说明 |
|------|------|
| 操作日志 (Operation Log) | 每次扩缩容操作写入预写日志（WAL），完成后标记 done。Manager 重启时扫描未完成的操作并恢复 |
| 云端标签对账 (Tag Reconciliation) | 所有框架创建的实例都打 `ManagedBy=elastic-agent` 标签。Manager 启动时用 `describe_instances` 扫描，与注册表对比，发现孤儿实例后纳入管理或清理 |
| 幂等操作 | `create_instance` 带幂等性 token，重复调用不会重复创建 |

agent-ml-research 的 Poller 已经做了类似的事情（每 60 秒同步云端状态），但缺少预写日志和幂等保证。框架应该将这个提升为核心能力。

#### (2) Worker 应用级健康检查

**问题：** 当前设计只通过 `describe_instances()` 检查 VM 状态（running/stopped/terminated）。但"VM 在运行"不等于"Claude Code 在正常工作"。进程可能：
- Claude Code 子进程崩溃但 Worker Runtime 还活着
- Worker 磁盘满导致无法写入
- OOM 但进程未被彻底 kill
- 网络不通但实例状态仍显示 running

**设计方案：** 框架的 Worker Runtime 应该内置多层健康检查：

| 层级 | 检查内容 | 方式 |
|------|---------|------|
| L1 - 基础设施 | VM 是否在运行 | `describe_instances()`（已有） |
| L2 - Worker Runtime | Worker Runtime 服务是否响应 | HTTP `GET /health`（框架内置） |
| L3 - Agent 进程 | Claude Code / Codex 进程是否存活 | Worker Runtime 检查子进程状态（框架内置） |
| L4 - 应用 | Harness 业务是否正常 | Harness 自定义健康检查（Harness 实现） |

连续 N 次 L2/L3 检查失败 → 标记 Worker 为 unhealthy → 触发 `WORKER_UNHEALTHY` 事件 → 自动重启服务或重建节点。

#### (3) Bootstrap 失败处理

**问题：** Bootstrap 是一个 10+ 步骤的管道。如果在第 7 步失败了怎么办？当前设计有 per-step rollback，但缺少全局策略。

**设计方案：**

```python
class BootstrapFailurePolicy(Enum):
    TERMINATE_AND_RETRY = "terminate_and_retry"  # 销毁实例，重新创建（默认）
    RETRY_FROM_FAILED = "retry_from_failed"      # 在同一实例上从失败步骤重试
    LEAVE_FOR_DEBUG = "leave_for_debug"           # 保留实例供人工排查
```

框架应该：
- 记录每个步骤的执行状态（成功/失败/跳过）到注册表
- 失败时按策略处理：默认 terminate + retry（因为半初始化的实例是危险的）
- 凭证如果已分配但 bootstrap 失败 → 自动回收凭证到池子
- 最大重试次数限制（防止无限创建+销毁循环）

#### (4) Manager ↔ Worker 安全模型

**问题：** 当前设计没有明确 Manager 和 Worker 之间如何认证。Worker Runtime 暴露了 `/execute` API — 如果没有认证，任何人都可以在 Worker 上执行任意命令。

**设计方案：**

| 方案 | 复杂度 | 安全性 | 推荐 |
|------|--------|--------|------|
| 共享 Secret (Bearer Token) | 低 | 中 | MVP ✅ |
| 每 Worker 独立 Token | 中 | 高 | Phase 2 |
| Mutual TLS (mTLS) | 高 | 最高 | Phase 5+ |
| IAM Role (AWS 原生) | 中 | 高 | Phase 2（AWS only） |

MVP：Bootstrap 时生成随机 token 写入 Worker 配置，Manager 记录在注册表中。所有 API 调用携带 Bearer Token。

此外：
- Manager → Worker 通信走 VPC 内网（不经过公网）
- Worker 不需要开放任何公网入站端口
- Worker Runtime 仅监听内网 IP（`--host 10.x.x.x`）

#### (5) 并发模型：每 Worker 多少任务？

**问题：** 当前设计隐含"一个 Worker 一个任务"。但实际上 CCM 在单机上就跑 5 个并发 Claude Code 进程。框架需要明确每 Worker 的并发模型。

**设计方案：**

```python
class WorkerCapacity:
    max_concurrent_tasks: int = 1    # 默认单任务
    cpu_per_task: float = 2.0        # 每任务预留 CPU
    memory_per_task_mb: int = 4096   # 每任务预留内存
```

- 默认 1 个 Worker 1 个任务（最简单、最安全、风控最好 — 每个账号独占一台机器）
- 可配置为 1:N（多任务共享 Worker），但 Harness 需要自己管理资源隔离
- 框架提供资源感知：Worker Runtime 上报可用 CPU/内存，调度器据此决定是否还能分配新任务

#### (6) 成本追踪

**问题：** 当前设计没有成本相关的能力。Elastic-Agent 管理的是云资源，成本是用户最关心的指标之一。

**设计方案：** 框架应追踪两层成本：

| 层级 | 来源 | 方式 |
|------|------|------|
| 基础设施成本 | EC2/ECS 实例运行时间 | 框架根据 instance_type + 运行时间估算（或调用 Cost Explorer API） |
| Agent 使用成本 | Claude Code / Codex API 调用 | Harness 上报（如 CCM 从 `result` 事件中提取 `cost_usd`） |

框架提供统一的成本视图：
- 每 Worker 的基础设施成本
- 每 Worker 的 Agent 使用成本
- 总成本和趋势
- 预算告警（月度预算超 80% 时提醒）

#### (7) Worker 软件更新

**问题：** Claude Code CLI 会更新版本，Harness 代码也会迭代。如何在不重建整个节点的情况下更新 Worker 上的软件？

**设计方案：**

| 策略 | 说明 | 适用场景 |
|------|------|---------|
| 滚动重建 | 创建新 Worker（新版本）→ drain 旧 Worker → 终止旧 Worker | 大版本更新、AMI 变更 |
| 热更新 | SSH/SSM 在运行中的 Worker 上执行更新命令 | 小版本更新、配置变更 |
| Harness 代码同步 | `git pull` 拉取最新 Harness 代码 + 重启服务 | Harness 逻辑更新 |

框架应该提供 `update_workers(strategy)` API，Harness 可以选择策略。

#### (8) 数据面与控制面分离

**问题：** 当前设计中 Manager 同时承担：
- **控制面**：节点管理、凭证管理、扩缩容决策
- **数据面**：日志转发、请求路由、状态汇聚

如果日志量大（50 个 Worker 同时流式输出），数据面的负载可能拖垮控制面。

**设计方案：** 在 MVP 阶段可以不分离（单进程足够），但架构上应该做好分离准备：

```
控制面（低频、关键）              数据面（高频、大流量）
  ├── 节点 CRUD                    ├── 日志流式传输
  ├── 凭证分配/轮换                ├── Worker 状态上报
  ├── 扩缩容决策                   ├── 请求路由转发
  └── Bootstrap 编排               └── 心跳检测
```

实现方式：
- MVP：同一个 FastAPI 进程，不同的 Router 前缀（`/api/control/` vs `/api/data/`）
- 后续：可以拆分为两个独立服务，控制面高可用部署，数据面水平扩展

#### (9) 多 Harness 支持

**问题：** 当前设计假设一个 Elastic-Agent 部署只跑一种 Harness。但实际场景中可能需要不同 Worker 跑不同的 Harness（比如一些 Worker 跑 CCM，另一些跑 agent-ml-research）。

**设计方案：**

- MVP：一个部署一种 Harness（足够大部分场景）
- 后续：支持 Worker 标签（`harness=ccm` / `harness=aml`），调度器按标签路由
- Worker 的 AMI / Docker 镜像可以不同，每种 Harness 对应一个 AMI

#### (10) 可测试性

**问题：** 框架涉及云资源操作（创建 EC2、SSH、凭证分发），如果每次测试都需要真实云资源，开发效率极低且费钱。

**设计方案：**

```python
class LocalProvider(CloudProvider):
    """本地模拟 Provider — 用 Docker 容器模拟 EC2 实例"""
    async def create_instance(self, config):
        container = docker.run("ubuntu:22.04", detach=True)
        return Instance(id=container.id, ip=container.ip, ...)

class DryRunProvider(CloudProvider):
    """空跑 Provider — 只记录操作日志，不实际创建资源"""
    async def create_instance(self, config):
        self.log.append(("create", config))
        return Instance(id=f"dry-{uuid4()}", ip="127.0.0.1", ...)
```

框架应该提供：
- `LocalProvider` — 用 Docker 容器替代 EC2，本地快速迭代
- `DryRunProvider` — 只验证流程不消耗资源
- 每个模块的单元测试 mock 接口

#### (11) 外部服务 API — 实时轨迹/Chat 记录 + 文件传输

**问题：** 外部服务（如前端 Dashboard、监控系统、第三方集成）需要通过 Manager 获取：
1. **实时的 Agent 轨迹/Chat 记录** — Claude Code 的 stream-json 输出、工具调用记录、对话历史
2. **Worker 上本地文本文件的实时传输** — Agent 执行过程中产生的日志文件、代码文件、配置文件等

当前设计中，Worker → Manager 的日志传输是内部通道，没有对外暴露标准化 API。外部服务无法接入。

**影响：** 每个 Harness 如果需要对外提供实时数据，都需要自建 API 层（CCM 自建了 WebSocket 广播，Audiobook 自建了 SQS→WebSocket 桥接），这部分工作应该由框架统一提供。

**设计方案：**

```python
# ── 外部服务 API 接口 ──

# 1. 实时轨迹流（WebSocket / SSE）
@app.websocket("/api/external/traces/{node_id}/stream")
async def stream_traces(ws: WebSocket, node_id: str, api_key: str):
    """实时推送指定 Worker 的 Agent 轨迹"""
    # 认证
    verify_external_api_key(api_key)
    # 从内部事件总线订阅该 Worker 的日志事件
    async for event in event_bus.subscribe(node_id):
        await ws.send_json({
            "timestamp": event.timestamp.isoformat(),
            "type": event.type,           # "assistant", "tool_use", "tool_result", "result"
            "content": event.content,
            "metadata": {
                "worker_id": node_id,
                "task_id": event.task_id,
                "session_id": event.session_id,
            }
        })

# 2. 历史轨迹查询（REST）
@app.get("/api/external/traces/{node_id}")
async def get_traces(node_id: str, since: datetime = None, limit: int = 100):
    """查询指定 Worker 的历史轨迹记录"""
    return trace_store.query(node_id=node_id, since=since, limit=limit)

# 3. 文件实时传输（REST + WebSocket）
@app.get("/api/external/files/{node_id}/{file_path:path}")
async def get_file(node_id: str, file_path: str):
    """通过 Manager 从 Worker 下载指定文件"""
    content = await worker_runtime.read_file(node_id, file_path)
    return StreamingResponse(content)

@app.websocket("/api/external/files/{node_id}/watch")
async def watch_files(ws: WebSocket, node_id: str, paths: list[str]):
    """监听 Worker 上指定文件的变化，实时推送更新内容"""
    async for change in worker_runtime.watch_files(node_id, paths):
        await ws.send_json({
            "path": change.path,
            "event": change.event,        # "modified", "created", "deleted"
            "content": change.content,     # 文件内容（文本文件）或 None
            "timestamp": change.timestamp.isoformat(),
        })

# 4. 集群级实时流（全部 Worker 的轨迹聚合）
@app.websocket("/api/external/traces/all/stream")
async def stream_all_traces(ws: WebSocket, api_key: str):
    """推送所有 Worker 的 Agent 轨迹（用于全局监控）"""
    ...
```

**Worker Runtime 侧需要支持的能力：**

| 能力 | 说明 | 实现方式 |
|------|------|---------|
| 轨迹缓存 | Worker Runtime 缓存最近 N 条轨迹记录 | 内存环形缓冲 + 可选持久化 |
| 文件读取 | Manager 可远程读取 Worker 上的任意文件 | Worker Runtime `/files/read` API |
| 文件监听 | Manager 可订阅 Worker 上指定文件的变化 | Worker Runtime 用 `inotify`/`watchdog` 监听 + WebSocket 推送 |
| 增量传输 | 大文件只传输变化部分 | tail-follow 模式（适用于日志文件） |

**外部 API 认证：**

| 方案 | 复杂度 | 安全性 | 推荐 |
|------|--------|--------|------|
| API Key (Bearer Token) | 低 | 中 | MVP ✅ |
| OAuth 2.0 Client Credentials | 中 | 高 | Phase 2 |
| JWT + RBAC | 高 | 最高 | Phase 3+ |

**普适性判断：** 通用。CCM 需要 WebSocket 实时日志给前端、agent-ml-research 需要飞书告警、Audiobook 需要 SQS→WebSocket 桥接 — 这些本质上都是"外部服务消费 Agent 轨迹"。文件传输需求同样普遍 — Agent 在 Worker 上生成的代码、报告、日志都需要被外部访问。

### 10.2 优先级排序

| 优先级 | 缺口 | 理由 |
|--------|------|------|
| **P0 - MVP 必须** | **阿里云 ECS Provider** | **项目首要部署环境是中国大陆** |
| **P0 - MVP 必须** | AWS EC2 Provider | 同步实现，验证多云抽象 |
| **P0 - MVP 必须** | Worker Runtime + 日志传输 | 没有这个框架无法执行任何远程任务 |
| **P0 - MVP 必须** | Manager ↔ Worker 认证 | 安全底线 |
| **P0 - MVP 必须** | 云端标签对账 | 防止孤儿实例持续烧钱 |
| **P0 - MVP 必须** | **外部服务 API — 轨迹流** | 外部服务需实时获取 Agent 轨迹/chat 记录 |
| **P0 - MVP 必须** | **外部服务 API — 文件传输** | 外部服务需实时获取 Worker 上的文件 |
| **P0 - MVP 必须** | **IaC 基础网络（Terraform + CDK）** | 阿里云 Terraform + AWS CDK Python |
| **P1 - MVP 应该** | Bootstrap 失败处理 | 否则失败后需要手动清理 |
| **P1 - MVP 应该** | Worker 应用级健康检查 (L2+L3) | 否则 Worker 假死无法发现 |
| **P1 - MVP 应该** | 优雅缩容 (Drain) | 否则缩容会中断正在执行的任务 |
| **P2 - 后续迭代** | 双层凭证管理 | MVP 可先用环境变量传递 |
| **P2 - 后续迭代** | 亲和性调度 | MVP 可先不支持 session 续接 |
| **P2 - 后续迭代** | 扩缩容信号 + 规则引擎 | MVP 手动扩缩容 |
| **P2 - 后续迭代** | 成本追踪 | MVP 阶段实例少，手动看账单 |
| **P2 - 后续迭代** | 操作幂等性 + 预写日志 | MVP 阶段实例少，出问题手动修 |
| **P3 - 长期** | 数据面/控制面分离 | 50+ Worker 后再考虑 |
| **P3 - 长期** | 多 Harness 支持 | 大部分用户单 Harness 足够 |
| **P3 - 长期** | Worker 软件热更新 | MVP 阶段重建 Worker 可接受 |
| **P3 - 长期** | 可测试性 (LocalProvider) | MVP 可以直接用真实 ECS/EC2 测试 |

---

## 11. Audiobook Harness 案例补充验证

> **更新说明**：本节基于新增的第三个 Harness 案例 — [有声书稿全自动化生产系统](harness-example-audiobook-production.md)（以下简称 Audiobook）的分析结果。Audiobook 代表了第三种典型接入模式：**绿地构建**（前两个案例分别代表"替换自建基础设施"和"扩展单机应用"）。通过这个新案例，进一步验证了已有的框架核心需求，并识别出 4 项前两个案例未覆盖的新需求。

### 11.1 Audiobook 项目概要

Audiobook 是一个端到端有声书稿自动化生产平台：前端提交做书请求 → 后端按需启动临时 EC2 → 自动登录 Claude Code → 执行 10 Phase 做书流水线 → 实时双向聊天 → 完成后 EC2 自动销毁。

关键差异点（与前两个案例相比）：

| 维度 | agent-ml-research | CCM | Audiobook |
|------|-------------------|-----|-----------|
| Worker 生命周期 | 常驻 | 本地子进程 | **临时（用完即毁）** |
| 通信模式 | SSH pull | 本地 stdout | **SQS push** |
| 接入模式 | 替换 | 扩展 | **绿地构建** |
| 任务时长 | 小时级 | 分钟级 | **1-2 小时（10 Phase）** |
| 用户交互 | 飞书 Bot | Web 单向 | **Web 双向聊天** |
| 崩溃恢复 | 无 | 无 | **自动检测 + S3 恢复** |

详见 [harness-example-audiobook-production.md](harness-example-audiobook-production.md)。

### 11.2 对已有核心需求的验证

第 10 节识别的 10 个设计缺口，在 Audiobook 案例中再次得到验证：

| 设计缺口 | Audiobook 中的体现 | 验证结论 |
|----------|-------------------|---------|
| (1) Manager 崩溃恢复 | 如果后端在 `scale_out()` 时崩溃，EC2 已启动但 DB 未更新 → 孤儿 Worker 持续烧钱 | ✅ 需要 |
| (2) Worker 应用级健康检查 | Worker 心跳超时检测（`HealthChecker`，2 分钟阈值）是 Audiobook 崩溃恢复的基础 | ✅ **强烈需要** — Audiobook 自建了 HealthChecker 来弥补框架缺失 |
| (3) Bootstrap 失败处理 | 7 步 Bootstrap（凭证拉取 → 登录 → 启动 Worker），登录步骤依赖外部服务（171mail + Anthropic），失败概率较高 | ✅ 需要 |
| (4) Manager ↔ Worker 认证 | EC2 Worker 通过 SQS 通信（AWS IAM 已提供身份），但 SQS 消息体无额外认证 | ✅ 需要 |
| (5) 并发模型 | 每台 EC2 只跑 1 个任务（1-2 个 Claude 账号），严格的 1:1 模型 | ✅ 确认默认单任务模型合理 |
| (6) 成本追踪 | m5.xlarge $0.192/h × 2h = $0.38/本书，Spot 可降 60-70% | ✅ 需要 |
| (7) Worker 软件更新 | AMI 预装 + Bootstrap 动态部署，双层策略 | ✅ 需要 |
| (8) 数据面/控制面分离 | StreamHub 高频消费 SQS（每秒多次轮询），如果与 TaskManager 同进程，高负载下可能相互影响 | ✅ 需要（尤其多任务并行时） |
| (9) 多 Harness 支持 | 单一 Harness，但验证了"不同 Harness 的 Worker 生命周期模式不同"的需求 | ✅ 间接验证 |
| (10) 可测试性 | 做书任务 1-2 小时，真实 EC2 测试成本高 | ✅ 需要 |

### 11.3 Audiobook 提出的 4 项新需求

以下需求在 agent-ml-research 和 CCM 案例中未出现，由 Audiobook 首次提出：

#### (11) 临时 Worker（Ephemeral Worker）模式

**问题：** agent-ml-research 和 CCM 的 Worker 都是"创建后常驻、复用多个任务"。Audiobook 的 Worker 是"一个任务一台 EC2，完成后立即 terminate"。当前框架设计假设 Worker 是持久的。

**影响：**
- `NodeRegistry` 需要支持自动注销（Worker 自毁后自动移除）
- `ScalingEngine` 的逻辑不同 — 不是"维持 N 台 Worker"，而是"每个 pending 任务启动一台"
- 健康检查超时需要更智能 — Worker 不应该存活超过预期任务时长（做书 2 小时，超过 3 小时应告警）

**设计建议：**

```python
class WorkerLifecycle(Enum):
    PERSISTENT = "persistent"     # 常驻，接受多个任务（agent-ml-research, CCM）
    EPHEMERAL = "ephemeral"       # 临时，一个任务一台 Worker（Audiobook）

class AudiobookHarness(Harness):
    def get_worker_lifecycle(self) -> WorkerLifecycle:
        return WorkerLifecycle.EPHEMERAL
```

**普适性判断：** 通用。batch job 类场景（一次性数据处理、CI/CD runner、ML 训练）都需要 ephemeral 模式。与 persistent 模式互补，框架应同时支持。

#### (12) 任务级元数据注入（task_metadata）

**问题：** Audiobook 的 Bootstrap 步骤需要知道具体任务参数 — book_slug、SQS 下行队列 URL、S3 路径等。这些参数在 Worker 创建时才确定，Bootstrap 步骤需要访问它们。

**影响：** 当前 Harness Bootstrap 步骤只能通过 `ctx.config`（全局配置）和 `ctx.app_credentials`（凭证）获取信息。缺少 per-task 的元数据注入机制。

**设计建议：**

```python
# scale_out 时传入 task_metadata
node = await elastic.scale_out(
    count=1,
    task_metadata={
        "task_id": "task-20260512-001",
        "book_slug": "sapiens",
        "sqs_downstream_url": "https://sqs.../audiobook-down-task-20260512-001",
        "s3_work_prefix": "s3://audiobook-production/tasks/task-20260512-001/work",
    },
)

# Bootstrap 步骤中通过 ctx.task_metadata 访问
class DownloadBookPDFStep(BootstrapStep):
    async def execute(self, ctx):
        s3_path = ctx.task_metadata["book_s3_path"]
        await ctx.ssh.run(f"aws s3 cp {s3_path} /workspace/book.pdf")
```

**普适性判断：** 通用。任何按需启动 Worker 的 Harness 都需要告诉 Worker "为什么被启动"。agent-ml-research 通过 SSH 命令参数传递项目名，CCM 通过 HTTP `/execute` API 传递任务信息 — 本质是同一个需求。

#### (13) 崩溃恢复的状态还原

**问题：** Audiobook Worker 崩溃后，新 Worker 需要从 S3 恢复上一个 Worker 的工作目录（`.work/` 中的中间文件），然后用 `/continue-book` 从断点继续。当前框架只处理"新建 Worker"，不处理"恢复旧 Worker 的状态"。

**影响：** Audiobook 自建了 `HealthChecker` + 恢复流程（~300 行代码），这些逻辑与业务无关，应该由框架提供。

**设计建议：**

```python
class AudiobookHarness(Harness):
    def get_state_directories(self) -> list[str]:
        """声明需要持久化的目录（框架自动周期性快照到 S3）"""
        return [
            "/home/ubuntu/workspace/.work/{book_slug}/",
        ]

    def get_recovery_config(self) -> dict:
        return {
            "auto_recover": True,
            "state_restore": "s3_sync",       # 从 S3 恢复工作目录
            "resume_command": "/continue-book {book_slug}",
            "max_recovery_count": 3,
        }
```

框架应该：
1. Worker Runtime 定期将声明的目录快照到 S3（与 Audiobook 的 `FileSyncer` 3s 同步类似，但由框架统一提供）
2. 检测 Worker 异常后，自动创建新 Worker → 恢复状态 → 触发 `on_recovery` 事件
3. Harness 在事件中决定如何"续做"（如 Audiobook 用 `/continue-book`）

**普适性判断：** 通用。任何长时间运行且有中间状态的工作负载都需要崩溃恢复 — ML 训练（checkpoint 恢复）、CI/CD（重试 from 失败步骤）、数据处理（增量恢复）。

#### (14) 反向消息通道（Manager → Worker）

**问题：** Audiobook 需要从后端向 Worker 发送用户消息和控制命令（暂停/取消/合规决策）。当前设计中框架只有 Manager → Worker 的 Bootstrap 通道（SSH），没有持续运行时的消息通道。

**影响：** Audiobook 自建了 per-task 下行 SQS 队列（`audiobook-down-{task_id}`），包括创建、注入 URL、轮询消费、任务结束后删除 — 约 100 行代码。

**设计建议：**

框架 Worker Runtime 已经有 Manager → Worker 的连接（Bootstrap 用的就是这个通道）。应该在运行时也保持这个通道，提供标准 API：

```python
# Manager 侧
await elastic.send_to_worker(node_id, {
    "type": "user_message",
    "payload": {"text": "选 B 合规版"},
})

# Worker 侧（Worker Runtime 自动接收，转发给 Harness 注册的 handler）
class AudiobookHarness(Harness):
    async def on_message_from_manager(self, node_id: str, message: dict):
        # 写入本地队列，WorkerAgent 轮询消费
        self.message_queue.put(message)
```

**普适性判断：** 通用。agent-ml-research 通过飞书 Bot 接收用户指令并转发到 Worker（SSH 执行）；CCM 支持 Plan 模式的人工审批 — 这些都是 Manager → Worker 的反向消息需求。框架统一提供后，各 Harness 无需自建通道。

### 11.4 更新后的优先级排序

将 4 项新需求纳入整体优先级：

| 优先级 | 缺口 | 来源 | 理由 |
|--------|------|------|------|
| **P0** | **阿里云 ECS Provider** | 新增 | 项目首要部署环境是中国大陆 |
| **P0** | AWS EC2 Provider | 10.1(原) | 同步实现，验证多云抽象 |
| **P0** | Worker Runtime + 日志传输 | 10.1(原) | 框架核心 |
| **P0** | Manager ↔ Worker 认证 | 10.1(原) | 安全底线 |
| **P0** | 云端标签对账 | 10.1(原) | 防孤儿实例 |
| **P0** | **外部服务 API（轨迹/文件）** | **10.1(新)** | 外部服务需实时获取 Agent 数据 |
| **P0** | **Terraform 基础网络** | 新增 | VPC/安全组需 IaC 管理 |
| **P1** | Bootstrap 失败处理 | 10.1(原) | 否则需手动清理 |
| **P1** | Worker 应用级健康检查 | 10.1(原) | Audiobook 已自建 HealthChecker 弥补 |
| **P1** | 优雅缩容 (Drain) | 10.1(原) | 1-2 小时做书任务不能被中断 |
| **P1** | **临时 Worker 模式** | **11.3(新)** | Audiobook 和 batch job 场景的基础 |
| **P1** | **task_metadata 注入** | **11.3(新)** | 所有按需启动 Worker 的 Harness 都需要 |
| **P2** | 双层凭证管理 | 10.1(原) | Audiobook 需要 Claude 账号 + 171mail tokens |
| **P2** | 亲和性调度 | 10.1(原) | Audiobook 的账号-IP 亲和性 |
| **P2** | 扩缩容信号 + 规则引擎 | 10.1(原) | Audiobook 用 pending 任务数作为信号 |
| **P2** | **崩溃恢复状态还原** | **11.3(新)** | Audiobook 已自建 ~300 行恢复代码 |
| **P2** | **反向消息通道** | **11.3(新)** | Audiobook 双向聊天、CCM Plan 审批都需要 |
| **P2** | 成本追踪 | 10.1(原) | Audiobook $0.38/本，Spot 可降 60-70% |
| **P2** | 操作幂等性 + 预写日志 | 10.1(原) | MVP 阶段实例少可手动修 |
| **P3** | 数据面/控制面分离 | 10.1(原) | 50+ Worker 后考虑 |
| **P3** | 多 Harness 支持 | 10.1(原) | 大部分用户单 Harness 足够 |
| **P3** | Worker 软件热更新 | 10.1(原) | MVP 阶段重建可接受 |
| **P3** | 可测试性 | 10.1(原) | MVP 用真实 EC2 |

### 11.5 三案例交叉验证汇总

三个 Harness 案例验证的框架核心能力完整度：

| 框架能力 | agent-ml-research (替换) | CCM (扩展) | Audiobook (绿地) | 结论 |
|---------|------------------------|-----------|-----------------|------|
| Worker Runtime | ✅ 替换 SSH | ✅ 替换本地子进程 | ✅ 替换 user_data 脚本 | **框架核心** |
| 日志流式传输 | ✅ 飞书告警 | ✅ WebSocket 前端 | ✅ SQS → WebSocket | **框架核心** |
| 有状态亲和性 | ✅ 项目绑定实例 | ✅ session resume | ✅ 账号-IP 亲和 | **框架核心** |
| 优雅缩容 | ✅ 长时间训练 | ✅ 30min 任务 | ✅ 1-2h 做书 | **框架核心** |
| 双层凭证 | ✅ WandB/HF/Feishu | ✅ Git key | ✅ Claude 账号 + 171mail | **框架核心** |
| 扩缩容信号 | ✅ 活跃项目数 | ✅ 任务队列深度 | ✅ 待处理任务数 | **框架核心** |
| 工作区同步 | ✅ git clone/rsync | ✅ Project clone | ✅ S3 sync（崩溃恢复） | **框架核心** |
| Bootstrap 超时 | ✅ uv sync 600s | - | ✅ Playwright 登录 300s | 框架支持 |
| 事件 Webhook | ✅ 飞书 | - | ✅ 钉钉/飞书告警 | 框架支持 |
| **临时 Worker** | - | - | ✅ 用完即毁 | **新增 — 框架支持** |
| **task_metadata** | - | - | ✅ 任务参数注入 | **新增 — 框架支持** |
| **崩溃恢复** | - | - | ✅ S3 → 新 Worker | **新增 — 框架支持** |
| **反向消息通道** | - | - | ✅ 用户消息下发 | **新增 — 框架支持** |
| **外部服务 API（轨迹）** | ✅ 飞书告警消费 | ✅ WebSocket 前端 | ✅ SQS→WebSocket | **新增 — 框架核心** |
| **外部服务 API（文件）** | ✅ 代码同步 | ✅ 项目文件访问 | ✅ 做书产物下载 | **新增 — 框架核心** |

**结论**：前 7 项核心能力在三个完全不同的案例中均出现，充分确认为框架必备。Audiobook 新增的 4 项需求填补了"临时 Worker"、"任务级参数"、"崩溃恢复"、"反向通信"四个之前未覆盖的维度。外部服务 API（轨迹流 + 文件传输）在三个案例中都有体现，确认为框架核心能力。

---

## 12. 未来演进路线

### Phase 1：MVP（当前目标）

> **详细 MVP 实现计划见独立文档 [mvp-plan.md](mvp-plan.md)**

- [x] 调研分析（本文档）
- [ ] 核心抽象接口定义
- [ ] **阿里云 ECS Provider 实现**（alibabacloud SDK 直连，**MVP 首选**）
- [ ] AWS EC2 Provider 实现（boto3 SDK 直连，同步实现）
- [ ] **IaC 基础网络（Terraform + CDK）**（阿里云 Terraform / AWS CDK Python）
- [ ] **Worker Runtime 实现**（远程执行 + 日志流式传输）
- [ ] **Manager ↔ Worker 认证**（共享 Secret）
- [ ] **外部服务 API — 实时轨迹流**（WebSocket/SSE 推送 Agent 轨迹/chat 记录）
- [ ] **外部服务 API — 文件传输**（Worker 文件实时读取/监听）
- [ ] Claude Code Agent 实现
- [ ] 基础 Bootstrap Pipeline + **失败处理**
- [ ] **云端标签对账**（防止孤儿实例）
- [ ] **Worker 应用级健康检查**（L2 + L3）
- [ ] **优雅缩容（Drain 机制）**
- [ ] 简单的手动扩缩容 API
- [ ] 基础 Web UI（节点列表、手动操作）
- [ ] 凭证分发（API Key 方式）
- [ ] 基础额度监控

### Phase 2：智能化 + 安全增强

- [ ] 双层凭证管理（Agent 凭证 + 应用凭证）
- [ ] Claude OAuth 自动登录
- [ ] IP 亲和性调度
- [ ] **Worker 亲和性路由**（有状态工作负载）
- [ ] **扩缩容信号接口 + 规则引擎**
- [ ] 额度耗尽自动换号
- [ ] 用完/过期账号自动回收
- [ ] **操作幂等性 + 预写日志**
- [ ] **成本追踪**（基础设施 + Agent 使用）
- [ ] 每 Worker 独立 Token 认证
- [ ] 外部 API OAuth 2.0 认证

### Phase 3：多 Agent + Harness 生态

- [ ] Codex Agent 支持
- [ ] 通用 Agent 接口
- [ ] Harness 插件系统 + 事件回调
- [ ] 请求路由（Router 功能）
- [ ] **工作区同步**（git clone / rsync）
- [ ] **Worker 软件热更新**
- [ ] 完善的 Dashboard
- [ ] **多 Harness 支持**（Worker 标签路由）

### Phase 4：容器化

- [ ] 阿里云 ECI / AWS ECS Fargate 支持
- [ ] Docker 镜像构建流水线
- [ ] Kubernetes (ACK/EKS) 支持
- [ ] KEDA 事件驱动扩展
- [ ] Helm Chart 打包

### Phase 6：生产级

- [ ] Manager 高可用（多副本 + 负载均衡）
- [ ] **数据面/控制面分离**
- [ ] 数据库后端（PostgreSQL）
- [ ] 缩容时数据自动备份到 S3
- [ ] 扩容时数据自动恢复
- [ ] 基于 Agent 的智能扩缩容（AI 决策）
- [ ] 成本优化建议
- [ ] 完整可观测性（Prometheus + Grafana）
- [ ] **可测试性**（LocalProvider + DryRunProvider）
- [ ] Mutual TLS (mTLS) 认证

---

## 附录 A：参考资料

### AWS 服务文档
- [EC2 Auto Scaling](https://docs.aws.amazon.com/autoscaling/ec2/userguide/)
- [ECS](https://docs.aws.amazon.com/ecs/latest/developerguide/)
- [EKS](https://docs.aws.amazon.com/eks/latest/userguide/)
- [Systems Manager](https://docs.aws.amazon.com/systems-manager/latest/userguide/)
- [Secrets Manager](https://docs.aws.amazon.com/secretsmanager/latest/userguide/)
- [Lambda Lifecycle Hooks](https://docs.aws.amazon.com/autoscaling/ec2/userguide/lifecycle-hooks.html)

### 阿里云服务文档
- [ESS 弹性伸缩](https://www.alibabacloud.com/help/en/auto-scaling/)
- [ACK 容器服务](https://www.alibabacloud.com/help/en/ack/)
- [ECI 弹性容器实例](https://www.alibabacloud.com/help/en/eci/)

### 开源项目
- [KEDA](https://keda.sh/) — Kubernetes 事件驱动扩展
- [Terraform](https://www.terraform.io/) — 多云 IaC
- [Pulumi](https://www.pulumi.com/) — 编程语言 IaC
- [HashiCorp Nomad](https://www.nomadproject.io/) — 简单编排器
- [Claude Code Docker](https://github.com/anthropics/claude-code) — 官方 Docker 镜像

### 参考项目与 Harness 文档
- [agent-ml-research](https://github.com/caoxiaoyuyuyuyuyu/agent-ml-research) — 已有自建基础设施的参考项目 → [Harness 集成文档](harness-example-agent-ml-research.md)
- [Claude Code Manager](https://github.com/zjw49246/Claude-Code-Manager) — 单机 Agent 编排系统 → [Harness 集成文档](harness-example-claude-code-manager.md)
- [有声书稿全自动化生产系统](../SOLUTION.md) — 绿地构建的临时 EC2 做书系统 → [Harness 集成文档](harness-example-audiobook-production.md)
