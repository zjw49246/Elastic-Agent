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
10. [未来演进路线](#10-未来演进路线)

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
2. **Agent 账号自动分发** — Claude Code / Codex 等 Agent 账号的自动登录、额度监控、智能轮换
3. **智能分发与风控** — 账号-IP 亲和性绑定，最小化 IP 暴露
4. **Harness 插件化** — 用户可在节点启动时部署自己的 Agent 服务代码

### 2.2 核心架构

```
┌─────────────────────────────────────────────────────────┐
│                    Manager 节点                         │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌────────┐  │
│  │ 前端 UI  │  │ 后端 API │  │ 调度引擎 │  │ Router │  │
│  └──────────┘  └──────────┘  └──────────┘  └────────┘  │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐              │
│  │ 凭证管理 │  │ 额度监控 │  │ 节点注册 │              │
│  └──────────┘  └──────────┘  └──────────┘              │
└─────────────────────┬───────────────────────────────────┘
                      │ API / SSH
    ┌─────────────────┼─────────────────┐
    │                 │                 │
┌───▼───┐        ┌───▼───┐        ┌───▼───┐
│Worker1│        │Worker2│        │WorkerN│
│EC2/ECS│        │EC2/ECS│        │EC2/ECS│
│       │        │       │        │       │
│Claude │        │Claude │        │Codex  │
│ Code  │        │ Code  │        │       │
│       │        │       │        │       │
│Harness│        │Harness│        │Harness│  ← 用户自定义的 Agent 服务
└───────┘        └───────┘        └───────┘
```

### 2.3 可插拔接口设计

框架需要提供以下扩展点：

```
CloudProvider 接口
  ├── AWSProvider（EC2）
  ├── AWSContainerProvider（ECS/Fargate）
  ├── AliyunProvider（阿里云 ECS）
  └── ... 未来扩展

AgentType 接口
  ├── ClaudeCodeAgent
  ├── CodexAgent
  └── ... 未来扩展

BootstrapPipeline 接口
  ├── 基础系统初始化
  ├── Agent 安装与配置
  ├── Harness 代码部署        ← 用户自定义
  └── 服务启动

CredentialProvider 接口
  ├── ClaudeOAuthProvider（171mail 登录流程）
  ├── APIKeyProvider（简单 API Key 分发）
  └── ... 未来扩展
```

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

### 4.2 关键考虑

- 阿里云在中国大陆地区是首选，如果需要国内节点部署则必须使用
- 双云策略（AWS 全球 + 阿里云国内）时，Terraform 或 Pulumi 提供抽象层
- 阿里云 ESS 直接支持生命周期钩子，可触发自定义操作

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

### 8.1 MVP 方案：EC2 + Lambda 编排 + SSM

最简单可行的方案，基于纯 EC2 但利用 AWS 原生服务解决 agent-ml-research 的痛点：

```
                        ┌───────────────────────────┐
                        │     Manager EC2 实例       │
                        │  ┌─────────┐ ┌──────────┐ │
                        │  │FastAPI  │ │ React UI │ │
                        │  │后端     │ │ 前端     │ │
                        │  └────┬────┘ └──────────┘ │
                        │       │                   │
                        │  ┌────▼────────────────┐  │
                        │  │ 调度引擎             │  │
                        │  │ - ASG 管理           │  │
                        │  │ - 凭证分发           │  │
                        │  │ - 额度监控           │  │
                        │  │ - IP 亲和调度        │  │
                        │  └────┬────────────────┘  │
                        └───────┼────────────────────┘
                                │
            ┌───────────────────┼───────────────────┐
            │           SSM Run Command             │
            │                                       │
   ┌────────▼────────┐              ┌───────────────▼──┐
   │   NAT GW #1     │              │   NAT GW #2      │
   │   EIP: 1.2.3.4  │              │   EIP: 5.6.7.8   │
   └────────┬────────┘              └───────────────┬──┘
            │                                       │
   ┌────────▼────────┐              ┌───────────────▼──┐
   │ Private Subnet A│              │ Private Subnet B │
   │                 │              │                  │
   │ ┌─────────────┐ │              │ ┌──────────────┐ │
   │ │ Worker EC2  │ │              │ │ Worker EC2   │ │
   │ │ Account #1  │ │              │ │ Account #2   │ │
   │ │ Claude Code │ │              │ │ Claude Code  │ │
   │ │ + Harness   │ │              │ │ + Harness    │ │
   │ └─────────────┘ │              │ └──────────────┘ │
   └──────────────────┘              └──────────────────┘
```

**核心组件：**

| 组件 | 实现 | 说明 |
|------|------|------|
| Manager | FastAPI + React | 单 EC2 实例运行，提供 Web UI 和 API |
| Worker 管理 | boto3 + ASG | 通过 ASG 实现扩缩容，生命周期钩子触发凭证注入 |
| 凭证存储 | Secrets Manager | $0.40/secret/月，自动轮换，审计追踪 |
| 凭证分发 | SSM Run Command | 无需 SSH，实例启动后自动拉取凭证 |
| IP 亲和性 | 多 NAT Gateway + DynamoDB | DynamoDB 记录 account → subnet 映射 |
| 额度监控 | Worker 侧 watchdog + Manager 轮询 | 沿用 agent-ml-research 的 watchdog 思路 |
| 数据备份 | EBS 快照 + S3 | ASG 缩容时生命周期钩子触发 |
| Harness 部署 | user-data + git clone | Worker 启动时自动拉取用户的 Harness 代码 |
| IaC | Pulumi (Python) | 多云准备，Python 生态与框架核心语言一致 |

### 8.2 进阶方案：ECS on EC2 + 容器化

在 MVP 基础上将 Worker 容器化：

- Worker 使用官方 `ghcr.io/anthropics/claude-code` Docker 镜像为基础
- ECS Task Definition 中注入凭证（引用 Secrets Manager）
- 容器内运行 Claude Code + 用户 Harness
- 底层 EC2 由 ASG 管理，支持 Spot
- 更好的资源隔离和快速部署

### 8.3 远期方案：EKS + KEDA 多云

当规模增长或需要多云时：

- EKS on AWS + ACK on 阿里云
- KEDA 基于任务队列深度自动扩展 Agent Pod
- External Secrets Operator 同步 Secrets Manager / 阿里云 KMS
- 多 NodeGroup + 不同子网实现 IP 亲和性
- Helm Chart 打包，一键部署

### 8.4 路由功能设计

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
│   │   │   ├── aws_ec2.py          # AWS EC2 实现
│   │   │   └── aliyun_ecs.py       # 阿里云 ECS 实现（未来）
│   │   ├── agents/                 # Agent 类型接口
│   │   │   ├── base.py             # AgentType 抽象基类
│   │   │   ├── claude_code.py      # Claude Code 实现
│   │   │   └── codex.py            # Codex 实现（未来）
│   │   ├── credentials/            # 凭证管理
│   │   │   ├── base.py             # CredentialProvider 抽象基类
│   │   │   ├── manager.py          # 凭证池管理器
│   │   │   ├── claude_oauth.py     # Claude OAuth 登录
│   │   │   └── api_key.py          # 简单 API Key 分发
│   │   ├── bootstrap/              # 节点初始化
│   │   │   ├── pipeline.py         # 可插拔初始化管道
│   │   │   └── steps/              # 各初始化步骤
│   │   ├── scheduler/              # 扩缩容调度
│   │   │   ├── engine.py           # 调度引擎
│   │   │   ├── rules.py            # 规则引擎（基于规则的扩缩容）
│   │   │   └── policies.py         # 扩缩容策略
│   │   ├── monitor/                # 监控
│   │   │   ├── quota.py            # 额度监控
│   │   │   ├── health.py           # 健康检查
│   │   │   └── metrics.py          # 指标收集
│   │   ├── router/                 # 请求路由
│   │   │   └── dispatcher.py       # 请求分发到 Worker
│   │   └── registry/               # 节点注册
│   │       └── store.py            # 节点状态存储
│   ├── manager/                    # Manager 节点
│   │   ├── api/                    # REST API
│   │   ├── service.py              # FastAPI 应用
│   │   └── config.py               # 配置模型
│   ├── worker/                     # Worker 节点
│   │   ├── agent.py                # Worker 侧 Agent 生命周期
│   │   ├── watchdog.py             # 额度监控看门狗
│   │   └── reporter.py             # 状态上报
│   └── cli/                        # CLI 工具
│       └── main.py                 # 命令行入口
├── dashboard/                      # 前端 UI
│   └── (React + Vite + Ant Design)
├── scripts/                        # 部署脚本
│   ├── watchdog.sh                 # Worker 侧 watchdog
│   └── bootstrap.sh                # Worker 初始化脚本
├── infra/                          # IaC（Pulumi/CDK）
│   └── __main__.py                 # 基础设施定义
├── examples/                       # 示例 Harness
│   └── agent-ml-research/          # agent-ml-research 的 Harness 示例
├── pyproject.toml
└── README.md
```

### 9.2 核心接口定义（草案）

```python
# CloudProvider 接口
class CloudProvider(ABC):
    async def create_instance(self, config: InstanceConfig) -> Instance: ...
    async def start_instance(self, instance_id: str) -> None: ...
    async def stop_instance(self, instance_id: str) -> None: ...
    async def terminate_instance(self, instance_id: str) -> None: ...
    async def list_instances(self, filters: dict) -> list[Instance]: ...
    async def get_instance(self, instance_id: str) -> Instance: ...
    async def execute_command(self, instance_id: str, command: str) -> CommandResult: ...

# AgentType 接口
class AgentType(ABC):
    def get_install_commands(self) -> list[str]: ...
    def get_start_commands(self) -> list[str]: ...
    def get_health_check(self) -> HealthCheck: ...
    def get_quota_check(self, credential: Credential) -> QuotaStatus: ...

# CredentialProvider 接口
class CredentialProvider(ABC):
    async def login(self, account: Account, instance: Instance) -> Credential: ...
    async def check_quota(self, credential: Credential) -> QuotaStatus: ...
    async def refresh(self, credential: Credential) -> Credential: ...
    async def revoke(self, credential: Credential) -> None: ...

# BootstrapStep 接口
class BootstrapStep(ABC):
    name: str
    async def execute(self, ctx: BootstrapContext) -> StepResult: ...
    async def rollback(self, ctx: BootstrapContext) -> None: ...

# Harness 接口 — 用户实现
class Harness(ABC):
    def get_repo_url(self) -> str: ...
    def get_bootstrap_steps(self) -> list[BootstrapStep]: ...
    def get_service_definitions(self) -> list[ServiceDefinition]: ...
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

### 9.4 外接场景示例：agent-ml-research

当框架完成后，agent-ml-research 可以这样接入：

```python
# examples/agent-ml-research/harness.py
class AgentMLResearchHarness(Harness):
    def get_repo_url(self):
        return "https://github.com/caoxiaoyuyuyuyuyu/agent-ml-research.git"

    def get_bootstrap_steps(self):
        return [
            InstallUVStep(),
            UVSyncStep(extras=["all"]),
            InstallPlaywrightStep(),
            BuildDashboardStep(),
            SetupXvfbStep(),
            ConfigureFeishuStep(),
            ConfigureWandBStep(),
        ]

    def get_service_definitions(self):
        return [
            ServiceDefinition(
                name="agent-ml",
                command="agent-ml server --public --dash-port 8420",
                restart_policy="always",
            ),
        ]
```

原来 agent-ml-research 中的 EC2 管理代码可以完全替换为 Elastic-Agent 框架调用：

```python
# 替换前：agent-ml-research 自己的 EC2 管理
from manager.ec2.provider import Ec2Provider
from manager.ec2.bootstrap import bootstrap_instance

# 替换后：使用 Elastic-Agent 框架
from elastic_agent import ElasticAgentManager

manager = ElasticAgentManager(
    cloud_provider=AWSEc2Provider(config),
    agent_type=ClaudeCodeAgent(),
    credential_provider=ClaudeOAuthProvider(email_tokens),
    harness=AgentMLResearchHarness(),
)

# 扩容
await manager.scale_out(count=3)

# 缩容
await manager.scale_in(count=1)

# 查看状态
status = await manager.get_cluster_status()
```

---

## 10. 未来演进路线

### Phase 1：MVP（当前目标）

- [x] 调研分析（本文档）
- [ ] 核心抽象接口定义
- [ ] AWS EC2 Provider 实现
- [ ] Claude Code Agent 实现
- [ ] 基础 Bootstrap Pipeline
- [ ] 简单的手动扩缩容 API
- [ ] 基础 Web UI（节点列表、手动操作）
- [ ] 凭证分发（API Key 方式）
- [ ] 基础额度监控

### Phase 2：智能化

- [ ] Claude OAuth 自动登录
- [ ] IP 亲和性调度
- [ ] 基于规则的自动扩缩容
- [ ] 额度耗尽自动换号
- [ ] 用完/过期账号自动回收
- [ ] 节点健康检查和自动恢复

### Phase 3：多 Agent 支持

- [ ] Codex Agent 支持
- [ ] 通用 Agent 接口
- [ ] Harness 插件系统
- [ ] 请求路由（Router 功能）
- [ ] 完善的 Dashboard

### Phase 4：多云

- [ ] 阿里云 ECS Provider
- [ ] Pulumi IaC 模板
- [ ] 跨云统一管理 UI
- [ ] 跨云凭证同步

### Phase 5：容器化

- [ ] ECS/Fargate 支持
- [ ] Docker 镜像构建流水线
- [ ] Kubernetes (EKS/ACK) 支持
- [ ] KEDA 事件驱动扩展
- [ ] Helm Chart 打包

### Phase 6：生产级

- [ ] Manager 高可用（多副本 + 负载均衡）
- [ ] 数据库后端（PostgreSQL）
- [ ] 缩容时数据自动备份到 S3
- [ ] 扩容时数据自动恢复
- [ ] 基于 Agent 的智能扩缩容（AI 决策）
- [ ] 成本优化建议
- [ ] 完整可观测性（Prometheus + Grafana）

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

### 参考项目
- [agent-ml-research](https://github.com/caoxiaoyuyuyuyuyu/agent-ml-research) — 本框架的核心参考实现
