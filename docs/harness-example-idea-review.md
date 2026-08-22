# Harness 应用示例：idea-review-agent 接入 Elastic-Agent

> 本文档以 [idea-review-agent](https://github.com/your-repo/idea-review-agent)（以下简称 IRA）为例，说明一个 **token 消耗密集型** 项目如何接入 Elastic-Agent 弹性计算框架，获得弹性资源分配和自动凭证管理能力。
>
> IRA 的核心挑战是：**单个 idea 评审消耗大量 token（20-60 分钟持续推理），极易触发 Claude Max 的 5h 滑动窗口限流；同时 idea 数量波动大（一批可能 5 个，也可能 100 个），固定机器数既浪费又不够用**。Elastic-Agent 在此场景下的核心价值是：
>
> 1. **弹性机器分配**：根据 pending idea 数量自动决定开几台 EC2，处理完自动关闭
> 2. **自动登号切号**：每台机器自动登录 Claude 账号，撞 limit 时自动切换到下一个可用账号，全程无人干预

---

## 目录

1. [idea-review-agent 项目解析](#1-idea-review-agent-项目解析)
2. [当前运维流程的痛点](#2-当前运维流程的痛点)
3. [迁移架构设计](#3-迁移架构设计)
4. [模块替换映射](#4-模块替换映射)
5. [Harness 接口实现](#5-harness-接口实现)
6. [分步迁移方案](#6-分步迁移方案)
7. [技术细节与挑战](#7-技术细节与挑战)
8. [迁移对框架提出的需求](#8-迁移对框架提出的需求)

---

## 1. idea-review-agent 项目解析

### 1.1 项目定位

IRA 是一个 **自动化研究 proposal 评审系统**，用于评估 ML 研究论文被 EMNLP 2026 主会接收的概率。输入一篇 Markdown 格式的 proposal，经过三阶段评审流程，输出包含录用概率、优化建议、最终 proposal 的结构化 JSON。

核心能力：
- 三 Agent 协作评审：Excavator（发掘亮点）+ Rescuer（找缺陷）+ Terminal（整合决策）
- 入口/出口双重三重共识（Triple-Consensus）验证
- 迭代优化循环（最多 5 轮），含收敛检测和止损机制
- 分布式执行：4 台 EC2 并行处理，每台 2 个 Claude Max 账号

### 1.2 系统架构

IRA 当前是一个 **无基础设施的手动分布式系统**：没有 Manager 节点，没有自建的 EC2 管理代码，没有账号管理代码。所有分布式能力靠人手动操作。

```
┌─────────────────────────────────────────────────────────────┐
│                    本地机器（人工操作台）                      │
│                                                             │
│  tools/fetch_ideas.py ──→ dispatch/ideas/*.json             │
│                             │                               │
│                    人手动 scp 分发到 4 台服务器                │
│                             │                               │
│  check_remote_progress.sh ←─┘  人手动 ssh 监控               │
│                                                             │
│  tools/writeback_results.py ←── 人手动 scp 收集结果           │
└─────────────────────────────────────────────────────────────┘
                              │
              人手动 ssh + scp（无自动化）
                              │
        ┌─────────────────────┼─────────────────────┐
        │                     │                     │
   ┌────▼────┐           ┌────▼────┐           ┌────▼────┐
   │ EC2 s1  │           │ EC2 s2  │           │ EC2 s3  │   ...s4
   │         │           │         │           │         │
   │dispatch/│           │dispatch/│           │dispatch/│
   │run_full │           │run_full │           │run_full │
   │_pipeline│           │_pipeline│           │_pipeline│
   │.py      │           │.py      │           │.py      │
   │         │           │         │           │         │
   │account-1│           │account-2│           │account-1│
   │account-2│           │account-1│           │account-2│
   │(手动切) │           │(手动切) │           │(手动切) │
   └─────────┘           └─────────┘           └─────────┘
```

### 1.3 三阶段评审流程

#### 阶段 1：入口三重共识（Entry Triple-Consensus）

```
Python 代码: batch_triage.py

asyncio.gather(                              ← Python 显式创建 3 个并行 session
    _single_triage(content, 1, key),         ← 每个调用 query()，spawn CLI 子进程
    _single_triage(content, 2, key),
    _single_triage(content, 3, key),
)

3 个 triage 输出 → Python 字符串拼接 → _meta_judge() → 综合判定

判定逻辑（Python if/else）:
  X_mid < 22% 或 存在致命维度 → KILL_triage（终止）
  否则 → 进入阶段 2
```

#### 阶段 2：迭代优化循环

```
Python 代码: reviewer.py

for round in range(1, MAX_ROUNDS + 1):       ← Python for 循环控制轮数
    asyncio.create_task(excavator_query)      ← Python 创建并行任务
    asyncio.create_task(rescuer_query)        ← 两个 Worker 同时跑
    await gather(excavator, rescuer)          ← Python 等待两者完成

    terminal_query(resume=session_id)         ← 唯一持久 session（跨轮累积上下文）

    # Python 判定收敛/止损:
    if rescuer 发现致命缺陷:                   ← Python 解析 rescuer 输出
        break  # KILL_stoploss
    if 连续 2 轮概率下降 且 < 22%:             ← Python 比较数值
        break  # KILL_stoploss
    if excavator 和 rescuer 都收敛:            ← Python 检查标志位
        break  # 正常收敛
```

#### 阶段 3：出口三重共识（Exit Triple-Consensus）

```
Python 代码: dispatch/run_full_pipeline.py

（与阶段 1 相同的 3+1 流程，但输入是 final_proposal 而非原始 proposal）

判定逻辑（Python if/else）:
  Z_mid >= 30% → PASS
  否则 → FAILED_below_30
```

### 1.4 Session 管理全景

| Session | 创建位置 | 是否复用 | 生命周期 |
|---------|---------|---------|---------|
| Triage-1（入口） | `batch_triage.py:_single_triage()` | 否 | 单次 query |
| Triage-2（入口） | 同上 | 否 | 单次 query |
| Triage-3（入口） | 同上 | 否 | 单次 query |
| MetaJudge（入口） | `batch_triage.py:_meta_judge()` | 否 | 单次 query |
| Excavator（每轮） | `reviewer.py:_call_worker()` | 否，每轮新建 | 单次 query |
| Rescuer（每轮） | `reviewer.py:_call_worker()` | 否，每轮新建 | 单次 query |
| Terminal（跨轮） | `reviewer.py:_call_terminal()` | **是**，`resume=session_id` | 整个迭代循环 |
| Triage-1（出口） | `run_full_pipeline.py:_single_triage()` | 否 | 单次 query |
| Triage-2（出口） | 同上 | 否 | 单次 query |
| Triage-3（出口） | 同上 | 否 | 单次 query |
| MetaJudge（出口） | `run_full_pipeline.py:_meta_judge()` | 否 | 单次 query |
| Terminal Final（Role C） | `reviewer.py:_call_terminal()` | **是**，续接迭代 session | 单次 query |

一个完整的 idea 评审最多创建 **3（入口）+ 1（MetaJudge）+ 5×2（Worker）+ 5+1（Terminal 跨轮+Final）+ 3（出口）+ 1（MetaJudge）= 最多 24 个 CLI 子进程**。

### 1.5 并发控制

```python
# config.py
MAX_CONCURRENCY = 2              # 全局信号量，限制同时运行的 CLI 子进程数
_semaphore = asyncio.Semaphore(MAX_CONCURRENCY)

# 每次 query() 调用前 acquire，完成后 release
# 效果：整台机器上最多同时有 2 个 Claude CLI 进程在跑
```

### 1.6 技术栈

| 层级 | 技术 |
|------|------|
| 语言 | Python 3.11+ |
| AI 调用 | claude-agent-sdk（封装 CLI 子进程） |
| 并发 | asyncio（gather, create_task, Semaphore） |
| 模型 | claude-opus-4-6（Claude Max 订阅） |
| 数据 | JSON 文件（无数据库） |
| 远程执行 | 手动 SSH + scp（无自动化） |
| 监控 | Bash 脚本 `check_remote_progress.sh` |
| API 集成 | requests 直连 Manager API（硬编码 URL + Token） |

### 1.7 与 CCM 和 agent-ml-research 的关键差异

| 维度 | IRA（idea-review） | CCM | agent-ml-research |
|------|---------------|-----|-------------------|
| 架构 | 无基础设施，纯手动分布式 | 单机单体 | 自建 Manager-Worker |
| EC2 管理 | 人手动 SSH | 无（本地进程） | 自建（boto3 + SSH） |
| 账号管理 | 人手动 cp 凭证文件 | 无 | 自建（Playwright + watchdog） |
| 任务分发 | 人手动 scp JSON 文件 | 自动调度（GlobalDispatcher） | 飞书 Bot + API |
| 监控 | Bash 脚本 + 人手动 SSH | WebSocket 实时日志 | 飞书告警 + Dashboard |
| 迁移策略 | **获得**弹性资源 + 自动凭证管理 | **新增**分布式能力 | **替换**自建基础设施 |
| 改造重点 | 接入框架获得弹性机器分配和自动切号 | 新增 RemoteInstanceManager | 剥离 `manager/ec2/` 和 `pool/` |

---

## 2. 为什么 IRA 需要 Elastic-Agent

### 2.1 核心矛盾：token 消耗密集 × idea 数量波动

IRA 面临的根本问题不是"运维麻烦"，而是 **资源需求与供给的动态不匹配**：

**token 消耗端**：单个 idea 评审需要 20-60 分钟持续推理（最多 24 个 CLI 子进程串/并行），一个 Claude Max 账号在 5h 滑动窗口内很容易撞 limit。实测 Batch 3 中，2 个账号交替使用仍然频繁限流。

**idea 数量端**：一批可能进来 5 个 idea，也可能 100 个。固定 4 台服务器——5 个 idea 时 3 台空转浪费钱，100 个 idea 时 4 台跑不完。

**当前的"解决方案"是人**：人看着哪台服务器限流了，ssh 进去手动切账号；人估算 idea 数量，决定开几台服务器。这不可扩展——idea 数量翻倍，人的操作量也翻倍。

### 2.2 Elastic-Agent 解决什么

| 问题 | 当前（人工） | 接入 Elastic-Agent 后 |
|------|------------|---------------------|
| **开几台机器？** | 固定 4 台，人拍脑袋 | 根据 pending idea 数量自动扩缩：5 个 idea 开 2 台，100 个开 10 台，跑完缩到 0 |
| **撞 limit 怎么办？** | 人 ssh 进去 `cp` 切凭证文件，可能 2 小时才发现 | CredentialPool 实时监控 5h 窗口使用率，>85% 自动切到下一个可用账号，秒级响应 |
| **账号怎么登录？** | 预先手动 cp 凭证到每台服务器 | Bootstrap 时自动分发凭证，新开的机器自动登录 |
| **idea 怎么分配？** | 人决定哪些 idea 发到哪台服务器 | 框架调度引擎自动分发到空闲 Worker，动态负载均衡 |
| **跑完了怎么关机？** | 经常忘了关，EC2 白跑 | 扩缩容引擎检测到全部空闲 → 自动缩容到 0，成本归零 |
| **一台机器挂了？** | 4 小时后才发现，手动重启 | Worker Runtime 心跳检测，挂了自动将未完成 idea 重新调度到其他 Worker |

### 2.3 典型场景对比

**场景：53 个 Opus idea 进来**

```
当前（人工）:
  1. 人拍脑袋决定 4 台服务器够不够
  2. 人把 53 个 idea 手动分成 4 份（13/14/13/13），scp 到 4 台服务器
  3. 4 台服务器各自跑，s2 分到最多，其他 3 台跑完空等
  4. s2 跑到第 8 个时限流，人 2 小时后才发现，ssh 进去切号
  5. 全部跑完后 4 台 EC2 继续开着，人忘了关
  → 耗时：约 6 小时（含 2 小时限流空等）+ 人工操作 30 分钟

接入 Elastic-Agent 后:
  1. 53 个 pending idea 被 poll_tasks() 拉取
  2. ScalingEngine 判断需要 6 台 Worker（每台 ~9 个 idea），自动开 EC2
  3. Bootstrap 自动部署代码 + 登录 Claude 账号
  4. 框架动态分发 idea，谁先完成谁接下一个（不会出现 3 台等 1 台）
  5. 某台 Worker 的账号使用率 >85% → 自动切到 account-2，不中断
  6. 53 个全部完成 → ScalingEngine 缩容到 0 台，EC2 成本归零
  → 耗时：约 3 小时（无限流空等）+ 零人工操作
```

### 2.4 附带解决的运维问题

弹性资源分配和自动凭证管理是核心价值。以下问题因为机器由框架管理而 **附带解决**，不需要额外设计：

| 原来的人工操作 | 为什么不需要了 |
|---|---|
| scp 代码到服务器 | 框架 Bootstrap 自动 git clone |
| scp 收集结果 | Harness 回调直接写 API |
| ssh 启动 pipeline | Worker Runtime 自动执行 |
| bash 脚本监控进度 | 框架内置健康检查 |
| 崩溃后手动判断哪些 idea 已完成 | 框架维护任务状态，未完成的自动重新调度 |

---

## 3. 迁移架构设计

### 3.1 迁移前后对比

```
迁移前（固定资源 + 人工管理）:              迁移后（弹性资源 + 自动凭证管理）:

  idea 数量: 53                            idea 数量: 53
  机器数量: 固定 4 台（人拍脑袋）            机器数量: 自动 6 台（ScalingEngine 计算）
  账号管理: 人 ssh 进去 cp 切号             账号管理: CredentialPool 自动轮换
  分发策略: 人手动分成 4 份                  分发策略: 动态调度，谁空闲谁接
       │                                       │
  ┌────┼────┐                          ┌────────┼────────┐
  │    │    │                          │    │   │   │    │
  s1   s2   s3  s4                   W1   W2  W3  W4  W5  W6
  13个 14个 13个 13个                 动态分配，负载均衡
  └─ s2 跑最久，                      └─ 谁先完成谁接下一个
     其他 3 台空等                        无空等
  └─ 限流靠人发现                      └─ 限流自动切号（秒级）
  └─ 跑完忘关机                        └─ 跑完自动缩容到 0
```

### 3.2 保留与替换的边界

| 模块 | 操作 | 理由 |
|------|------|------|
| `reviewer.py` | **保留** | 核心业务逻辑：三 Agent 迭代评审 |
| `batch_triage.py` | **保留** | 核心业务逻辑：三重共识 triage |
| `config.py` | **保留** | 业务参数（阈值、模型、轮数） |
| `parsers.py` | **保留** | 业务逻辑：解析 Agent 输出 |
| `convergence.py` | **保留** | 业务逻辑：收敛/止损判定 |
| `io_utils.py` | **保留** | 业务逻辑：MD 读写、JSON 验证 |
| `prompts/` | **保留** | 业务逻辑：所有 prompt 定义 |
| `dispatch/run_full_pipeline.py` | **适配** | 三阶段编排保留，任务获取/结果写回适配为 Harness 接口 |
| `tools/fetch_ideas.py` | **替换** → Harness `poll_tasks()` | 框架调度 |
| `tools/writeback_results.py` | **替换** → Harness `on_task_complete()` | 框架事件回调 |
| `check_remote_progress.sh` | **替换** → 框架健康检查 + 监控 | 框架内置 |
| 手动 scp 分发 idea | **替换** → 框架 Worker Runtime 远程执行 | 框架内置 |
| 手动 scp 收集结果 | **替换** → Harness 结果回调直写 API | 框架内置 |
| 手动 cp 切账号 | **替换** → 框架 CredentialPool 自动轮换 | 框架内置 |
| 手动 SSH 启动/重启 | **替换** → 框架 Bootstrap + 远程执行 | 框架内置 |

### 3.3 迁移后的架构

```
┌─────────────────────────────────────────────────────────────────┐
│                        Manager 节点                              │
│                                                                  │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │                 IRA Harness（业务编排）                      │  │
│  │                                                            │  │
│  │  poll_tasks()         on_task_complete()    get_scaling()   │  │
│  │  从 Manager API       validate → write →    pending_ideas   │  │
│  │  拉取 pending idea    verify 写回 API       - idle_workers  │  │
│  └────────────────────────┬───────────────────────────────────┘  │
│                           │ 调用框架 API                         │
│  ┌────────────────────────▼───────────────────────────────────┐  │
│  │                  Elastic-Agent 框架层                        │  │
│  │                                                            │  │
│  │  CloudProvider   CredentialPool    QuotaMonitor             │  │
│  │  (boto3 EC2)     (2 accounts ×N)  (5h 窗口监控)            │  │
│  │                                                            │  │
│  │  NodeRegistry    BootstrapPipeline   ScalingEngine          │  │
│  │  (Worker 注册)   (venv + sdk + code) (按需扩缩)            │  │
│  └────────────────────────┬───────────────────────────────────┘  │
└───────────────────────────┼────────────────────────────────────┘
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
         ││run_   ││   ││run_   ││   ││run_   ││
         ││full_  ││   ││full_  ││   ││full_  ││  ← 业务代码
         ││pipe.. ││   ││pipe.. ││   ││pipe.. ││
         │└───┬───┘│   │└───┬───┘│   │└───┬───┘│
         │┌───▼───┐│   │┌───▼───┐│   │┌───▼───┐│
         ││Claude ││   ││Claude ││   ││Claude ││
         ││CLI ×2 ││   ││CLI ×2 ││   ││CLI ×2 ││  ← Semaphore(2)
         │└───────┘│   │└───────┘│   │└───────┘│
         └─────────┘   └─────────┘   └─────────┘
```

---

## 4. 模块替换映射

### 4.1 任务获取：fetch_ideas.py → Harness poll_tasks()

```python
# ── 替换前：手动执行 fetch_ideas.py ──

# 人在本地机器运行:
# python tools/fetch_ideas.py -o dispatch/ideas/ --status pending
# 然后手动 scp 到各服务器

# tools/fetch_ideas.py 核心逻辑:
resp = requests.get(
    f"{BASE}/api/ideas",
    headers={"Authorization": f"Bearer {TOKEN}"},
    params={"source_backend": f"be-idea-gen-0509-{suffix}"}
)
ideas = [i for i in resp.json() if i["status"] == "pending"]
for idea in ideas:
    Path(f"dispatch/ideas/{idea['id']}_{idea['source_backend']}.json").write_text(
        json.dumps(idea)
    )

# ── 替换后：Harness 自动拉取 ──

class IdeaReviewHarness(Harness):
    async def poll_tasks(self) -> list[Task]:
        resp = await self.http.get(
            f"{self.config['manager_api']}/api/ideas",
            headers={"Authorization": f"Bearer {self.config['api_token']}"},
            params={"status": "pending"},
        )
        ideas = resp.json()
        return [
            Task(
                id=f"{idea['id']}_{idea['source_backend']}",
                payload={
                    "idea_id": idea["id"],
                    "source_backend": idea["source_backend"],
                    "title": idea["title"],
                    "content": idea["content"],
                },
                priority=0,  # 所有 idea 同优先级
            )
            for idea in ideas
            if idea.get("status") == "pending"
               and not idea.get("human_review_note")
        ]
```

### 4.2 任务执行：手动 SSH 启动 → Worker Runtime 远程执行

```python
# ── 替换前：人手动 SSH 到服务器启动 ──

# ssh -i $KEY ubuntu@ec2-13-158-17-206...
# cd ~/idea_review
# IDEA_DIR=dispatch/ideas OUTPUT_DIR=dispatch/results \
#   nohup python3 dispatch/run_full_pipeline.py > dispatch/run_s1.log 2>&1 &

# ── 替换后：框架 Worker Runtime 自动执行 ──

# Harness 定义执行命令:
def get_task_command(self, task: Task) -> list[str]:
    return [
        "python3", "dispatch/run_single_idea.py",
        "--idea-json", json.dumps(task.payload),
        "--output-dir", "/home/ubuntu/idea_review/results",
    ]
```

### 4.3 账号管理：手动 cp → CredentialPool 自动轮换

```python
# ── 替换前：人手动切换账号 ──

# 发现限流后 SSH 进去:
# cp ~/.claude-account-2/.credentials.json ~/.claude/.credentials.json
# 然后重启 pipeline

# ── 替换后：框架自动轮换 ──

from elastic_agent import CredentialPool, StaticCredentialProvider

pool = CredentialPool(
    provider=StaticCredentialProvider(
        credentials=[
            {"email": "account-1@xxx", "credentials_path": "account-1.json"},
            {"email": "account-2@xxx", "credentials_path": "account-2.json"},
        ]
    ),
    affinity_policy="prefer_same_ip",
    quota_threshold=0.85,
    rotation_strategy="least_used_first",
)

# 框架自动:
# 1. Bootstrap 时分发 account-1 凭证到 Worker
# 2. QuotaMonitor 检测到 5h 使用率 > 85%
# 3. 等当前 idea 评审完成（不中断）
# 4. 自动切换到 account-2
# 5. 继续处理下一个 idea
```

### 4.4 结果写回：手动 scp + writeback → Harness 自动回调

```python
# ── 替换前：人手动收集 + 写回 ──

# scp -i $KEY -r ubuntu@s1:~/idea_review/dispatch/results/ results/s1_collected/
# python tools/writeback_results.py results/s1_collected/ --dry-run
# python tools/writeback_results.py results/s1_collected/ --set-review-result
# ...重复 4 次

# ── 替换后：Harness 事件回调自动写回 ──

class IdeaReviewHarness(Harness):
    async def on_task_complete(self, task: Task, result: TaskResult):
        """每个 idea 评审完成后自动触发"""
        result_data = json.loads(result.stdout)
        note = self._format_review_note(result_data)
        idea_id = task.payload["idea_id"]
        source_backend = task.payload["source_backend"]

        # 1. 验证
        self._validate_review_note(note)

        # 2. 写入 API
        await self.http.post(
            f"{self.config['manager_api']}/api/ideas/{idea_id}/human-review-note",
            json={"note": note, "by": "孙震", "source_backend": source_backend},
        )

        # 3. 设置 review result
        verdict = result_data.get("verdict", "")
        if verdict == "PASS":
            review_result = "passed"
        elif verdict.startswith("KILL") or verdict.startswith("FAILED"):
            review_result = "failed"
        else:
            return  # ERROR 情况不设置

        await self.http.post(
            f"{self.config['manager_api']}/api/ideas/{idea_id}/human-review",
            json={"result": review_result, "by": "孙震", "source_backend": source_backend},
        )

        # 4. 验证写入
        resp = await self.http.get(
            f"{self.config['manager_api']}/api/ideas/{idea_id}",
            params={"source_backend": source_backend},
        )
        if "**Verdict**" not in resp.json().get("human_review_note", ""):
            raise WritebackVerificationError(f"idea {idea_id} writeback verification failed")
```

### 4.5 监控：check_remote_progress.sh → 框架健康检查

```python
# ── 替换前：900 行 Bash 脚本 ──

# check_remote_progress.sh:
# for server in s1 s2 s3 s4; do
#     ssh -i $KEY ubuntu@$server "pgrep -af 'run_full_pipeline'"
#     ssh -i $KEY ubuntu@$server "grep -c '✓ 已写入' $LOG"
#     ssh -i $KEY ubuntu@$server "tail -3 $LOG"
# done

# ── 替换后：框架内置 ──

# 框架自动:
# - Worker Runtime 每 30s 上报心跳 + 当前任务状态
# - QuotaMonitor 每 60s 检查账号使用率
# - Manager 汇聚所有 Worker 状态到 Dashboard
# - 异常触发事件 → Harness 回调（可接飞书/Slack）
```

---

## 5. Harness 接口实现

### 5.1 IdeaReviewHarness 完整定义

```python
from elastic_agent import (
    Harness, BootstrapStep, ServiceDefinition,
    ScalingSignal, FrameworkEvent, Task, TaskResult,
)

class IdeaReviewHarness(Harness):
    """idea-review-agent 的 Elastic-Agent Harness 实现"""

    def __init__(self, config: dict):
        self.config = config
        # config 包含:
        # - manager_api: Manager API URL
        # - api_token: Manager API Bearer Token
        # - reviewer_name: "孙震"
        # - repo_url: idea review 代码仓库
        # - repo_branch: "main"

    # ── 代码部署 ──

    def get_repo_url(self) -> str:
        return self.config["repo_url"]

    def get_bootstrap_steps(self) -> list[BootstrapStep]:
        return [
            InstallPythonStep(),                # apt install python3.11 + pip
            CloneRepoStep(),                    # git clone idea-review 代码
            CreateVenvStep(),                   # python -m venv .venv
            InstallDependenciesStep(),          # pip install claude-agent-sdk
            SetupWorkspaceStep(),               # mkdir -p results/ dispatch/
        ]

    def get_service_definitions(self) -> list[ServiceDefinition]:
        # IRA 不需要常驻服务 — 每个 idea 是独立的 CLI 调用
        # Worker Runtime 按需启动 pipeline 进程即可
        return []

    # ── 任务获取 ──

    async def poll_tasks(self) -> list[Task]:
        """每 60 秒轮询 Manager API，拉取 pending idea"""
        resp = await self.http.get(
            f"{self.config['manager_api']}/api/ideas",
            headers={"Authorization": f"Bearer {self.config['api_token']}"},
        )
        ideas = resp.json()
        tasks = []
        for idea in ideas:
            if idea.get("status") != "pending":
                continue
            if idea.get("human_review_note"):
                continue  # 已有评审记录，跳过
            tasks.append(Task(
                id=f"{idea['id']}_{idea['source_backend']}",
                payload={
                    "idea_id": idea["id"],
                    "source_backend": idea["source_backend"],
                    "title": idea["title"],
                    "content": idea["content"],
                },
            ))
        return tasks

    # ── 任务执行 ──

    def get_task_command(self, task: Task) -> dict:
        """告诉 Worker Runtime 如何执行这个 idea 的评审"""
        return {
            "command": [
                "python3", "-m", "dispatch.run_single_idea",
                "--idea-json-stdin",
            ],
            "stdin": json.dumps(task.payload),
            "cwd": "/home/ubuntu/idea_review",
            "env": {
                "OUTPUT_DIR": "/home/ubuntu/idea_review/results",
            },
            "timeout": 7200,  # 单个 idea 最长 2 小时
        }

    # ── 任务完成回调 ──

    async def on_task_complete(self, task: Task, result: TaskResult):
        """idea 评审完成，自动写回 API"""
        if result.exit_code != 0:
            await self._on_task_failed(task, result)
            return

        result_data = json.loads(result.output_file)
        note = self._format_review_note(result_data)
        idea_id = task.payload["idea_id"]
        source_backend = task.payload["source_backend"]

        # validate → write → verify（三步写入，沿用 v2 pipeline 的安全策略）
        self._validate_review_note(note)

        await self.http.post(
            f"{self.config['manager_api']}/api/ideas/{idea_id}/human-review-note",
            json={"note": note, "by": self.config["reviewer_name"],
                  "source_backend": source_backend},
        )

        verdict = result_data.get("verdict", "")
        if verdict == "PASS":
            await self._set_review_result(idea_id, "passed", source_backend)
        elif verdict.startswith(("KILL", "FAILED")):
            await self._set_review_result(idea_id, "failed", source_backend)

        await self._verify_writeback(idea_id, source_backend)

    async def on_task_failed(self, task: Task, result: TaskResult):
        """任务失败，记录错误但不写回 API"""
        self.log.error(
            f"idea {task.id} failed: exit_code={result.exit_code}, "
            f"stderr={result.stderr[-500:]}"
        )
        # 不写回 API → idea 保持 pending → 下次 poll_tasks 会重新拉取

    # ── 扩缩容信号 ──

    def get_scaling_signal(self) -> ScalingSignal:
        pending = self._count_pending_ideas()
        idle = self._count_idle_workers()
        busy = self._count_busy_workers()
        return ScalingSignal(
            pending_tasks=pending,
            idle_workers=idle,
            busy_workers=busy,
        )

    # ── 事件处理 ──

    def get_event_handlers(self) -> dict:
        return {
            FrameworkEvent.CREDENTIAL_EXHAUSTED: self._on_credential_exhausted,
            FrameworkEvent.WORKER_UNHEALTHY: self._on_worker_unhealthy,
            FrameworkEvent.NODE_DRAIN_START: self._on_drain_start,
        }

    async def _on_credential_exhausted(self, data: dict):
        """账号额度耗尽 — 框架自动轮换，这里只记录日志"""
        self.log.warning(
            f"Account {data['account_id']} exhausted on Worker {data['node_id']}, "
            f"framework auto-rotating to next account"
        )

    async def _on_worker_unhealthy(self, data: dict):
        """Worker 不健康 — 检查是否有正在执行的 idea"""
        self.log.error(f"Worker {data['node_id']} unhealthy: {data['reason']}")
        # 框架会自动将 Worker 上的 pending task 重新调度到其他 Worker

    async def _on_drain_start(self, data: dict):
        """缩容前 — 等待当前 idea 评审完成"""
        self.log.info(f"Draining Worker {data['node_id']}, waiting for current idea to finish")
        # 框架的 Drain 机制会等待当前任务完成后再终止实例

    # ── 应用凭证 ──

    def get_app_credentials(self) -> list[str]:
        return [
            "manager_api_token",     # Manager API Bearer Token
        ]

    # ── 健康检查 ──

    def get_health_check(self) -> dict:
        return {
            "type": "command",
            "command": "python3 -c 'import claude_agent_sdk; print(\"ok\")'",
            "interval": 60,
            "timeout": 10,
        }

    # ── 内部方法 ──

    def _format_review_note(self, result: dict) -> str:
        """沿用 run_full_pipeline.py 的格式化逻辑"""
        verdict = result.get("verdict", "UNKNOWN")
        prob = result.get("概率对比", {})
        x = prob.get("X_初始_triple_consensus", "?")
        y = prob.get("Y_循环终审自评", "?")
        z = prob.get("Z_独立出口_triple_consensus", "?")
        delta = prob.get("真实提升_Z减X", "?")

        note = f"**Verdict**: {verdict}\n\n"
        note += f"**概率**: X={x}(入口) → Y={y}(终审自评) → Z={z}(出口独立)\n"
        note += f"**真实提升**: {delta}\n\n"

        if advice := result.get("优化方案"):
            note += f"**优化方案**:\n{advice[:2000]}\n"

        return note

    async def _set_review_result(self, idea_id, result_value, source_backend):
        await self.http.post(
            f"{self.config['manager_api']}/api/ideas/{idea_id}/human-review",
            json={"result": result_value, "by": self.config["reviewer_name"],
                  "source_backend": source_backend},
        )

    async def _verify_writeback(self, idea_id, source_backend):
        resp = await self.http.get(
            f"{self.config['manager_api']}/api/ideas/{idea_id}",
            params={"source_backend": source_backend},
        )
        if "**Verdict**" not in resp.json().get("human_review_note", ""):
            raise WritebackVerificationError(f"idea {idea_id} writeback failed verification")
```

### 5.2 各 Bootstrap 步骤实现

```python
class InstallPythonStep(BootstrapStep):
    name = "install-python"

    async def execute(self, ctx):
        await ctx.ssh.run("sudo apt-get update && sudo apt-get install -y python3.11 python3.11-venv python3-pip")

class CloneRepoStep(BootstrapStep):
    name = "clone-repo"

    async def execute(self, ctx):
        repo_url = ctx.harness.get_repo_url()
        branch = ctx.config.get("repo_branch", "main")
        result = await ctx.ssh.run(
            f"git clone -b {branch} {repo_url} /home/ubuntu/idea_review",
            timeout=120,
        )
        if result.returncode != 0:
            # 备选：从 Manager rsync
            await ctx.ssh.rsync(
                src=ctx.config["local_repo_path"],
                dst="/home/ubuntu/idea_review",
                exclude=[".venv/", "__pycache__/", "results/", "dispatch/ideas/"],
            )

    async def rollback(self, ctx):
        await ctx.ssh.run("rm -rf /home/ubuntu/idea_review")

class CreateVenvStep(BootstrapStep):
    name = "create-venv"

    async def execute(self, ctx):
        await ctx.ssh.run(
            "cd /home/ubuntu/idea_review && python3.11 -m venv .venv",
            timeout=60,
        )

class InstallDependenciesStep(BootstrapStep):
    name = "install-dependencies"

    async def execute(self, ctx):
        await ctx.ssh.run(
            "cd /home/ubuntu/idea_review && "
            ".venv/bin/pip install -r requirements.txt",
            timeout=300,
        )
        # requirements.txt 只有一个依赖: claude-agent-sdk
        # 验证安装
        result = await ctx.ssh.run(
            ".venv/bin/python -c 'import claude_agent_sdk; print(\"ok\")'",
        )
        if "ok" not in result.stdout:
            raise BootstrapError("claude-agent-sdk installation failed")

class SetupWorkspaceStep(BootstrapStep):
    name = "setup-workspace"

    async def execute(self, ctx):
        await ctx.ssh.run(
            "mkdir -p /home/ubuntu/idea_review/results "
            "/home/ubuntu/idea_review/dispatch/ideas"
        )
```

### 5.3 需要新增的适配模块：run_single_idea.py

当前 `run_full_pipeline.py` 是批量处理（遍历 `dispatch/ideas/` 目录下所有 JSON）。接入框架后需要一个 **单 idea 执行入口**，接收单个 idea 的 JSON 输入，输出单个结果 JSON。

```python
# dispatch/run_single_idea.py — 新增文件（~80 行）
# 从 run_full_pipeline.py 提取单 idea 处理逻辑

import sys, json, asyncio
from pathlib import Path

# 复用现有模块
from batch_triage import run_entry_triage
from reviewer import ProposalReviewer
from dispatch.run_full_pipeline import (
    run_exit_triage, format_result, format_review_note,
    _safe_write_to_api,
)

async def run_single_idea(idea: dict, output_dir: str) -> dict:
    """处理单个 idea 的完整三阶段 pipeline"""
    key = f"{idea['id']}_{idea['source_backend']}"
    title = idea["title"]
    content = idea["content"]
    trace_dir = Path(output_dir) / f"trace_{key}"
    trace_dir.mkdir(parents=True, exist_ok=True)

    # 阶段 1: Entry Triage
    entry_result = await run_entry_triage(content, key)
    if entry_result["verdict"].startswith("KILL"):
        result = format_result(idea, entry_result, stage="triage_only")
        _write_result(result, output_dir, key)
        return result

    # 阶段 2: Iterative Loop
    reviewer = ProposalReviewer(verbose=True, trace_dir=str(trace_dir))
    stage2_result = await reviewer.run(
        original_title=title,
        original_md=content,
        skip_triage=True,
    )

    # 提取 final_proposal
    final_proposal = _extract_final_proposal(trace_dir)

    # 阶段 3: Exit Triage
    exit_result = await run_exit_triage(final_proposal, key)

    # 组装最终结果
    result = format_result(idea, entry_result, stage2_result, exit_result)
    _write_result(result, output_dir, key)

    # 输出到 stdout（供 Worker Runtime 捕获）
    print(json.dumps({"status": "completed", "result_file": f"result_{key}.json"}))
    return result

def _write_result(result, output_dir, key):
    path = Path(output_dir) / f"result_{key}.json"
    path.write_text(json.dumps(result, ensure_ascii=False, indent=2))

def _extract_final_proposal(trace_dir):
    trace_path = trace_dir / "trace.json"
    trace = json.loads(trace_path.read_text())
    return trace["steps"][-1]["parsed"]["pure_proposal"]

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--idea-json-stdin", action="store_true")
    parser.add_argument("--output-dir", default="results")
    args = parser.parse_args()

    if args.idea_json_stdin:
        idea = json.loads(sys.stdin.read())
    result = asyncio.run(run_single_idea(idea, args.output_dir))
    sys.exit(0 if result.get("verdict") != "ERROR" else 1)
```

---

## 6. 分步迁移方案

### Phase 0：准备（不改现有代码，零风险）

1. 从 `run_full_pipeline.py` 提取 `run_single_idea.py`（单 idea 入口）
2. 本地测试 `run_single_idea.py`：`echo '{"id":..., "content":...}' | python3 dispatch/run_single_idea.py --idea-json-stdin`
3. 确认与现有 `run_full_pipeline.py` 产出相同结果

**验证标准**：同一个 idea 的 verdict 和概率区间一致。

### Phase 1：单 Worker 验证

1. 用 Elastic-Agent 创建 1 台 EC2（复用现有 AMI 或手动创建）
2. 执行 Bootstrap Pipeline：安装 Python → clone 代码 → 创建 venv → 安装依赖
3. 手动分发 1 个 idea 的凭证到 Worker
4. 通过 Worker Runtime 远程执行 `run_single_idea.py`
5. 验证结果 JSON 正确、API 写回成功

**验证标准**：从 `poll_tasks()` 到 `on_task_complete()` 全链路跑通。

### Phase 2：多 Worker + 自动调度

1. 扩展到 4 台 Worker（对标当前手动 4 台）
2. 启用 Harness `poll_tasks()` 自动拉取 pending idea
3. 启用框架调度引擎自动分发 idea 到 idle Worker
4. 启用 `on_task_complete()` 自动写回 API
5. 批量测试：投入 10 个 idea，验证自动分发 + 并行处理 + 结果写回

**验证标准**：零人工操作完成 10 个 idea 的完整评审。

### Phase 3：凭证管理 + 自动轮换

1. 配置 CredentialPool（每台 Worker 2 个账号）
2. 启用 QuotaMonitor（5h 窗口，85% 阈值）
3. 测试自动轮换：故意跑满一个账号，验证自动切到 account-2
4. 验证 IP 亲和性：同一 IP 上尽量复用同一账号

**验证标准**：限流场景下无人工干预，自动完成账号切换并继续处理。

### Phase 4：弹性扩缩容

1. 配置 ScalingSignal + 规则引擎：
   - pending idea > idle Worker → 扩容
   - 所有 Worker 空闲 > 30 分钟 → 缩容
2. 启用 Drain 机制（缩容前等待当前 idea 完成）
3. 测试：投入 50 个 idea，观察自动扩容到 N 台 → 处理完 → 自动缩容到 0

**验证标准**：全自动扩缩容，处理完毕后 EC2 成本归零。

### Phase 5：清理

1. 删除 `check_remote_progress.sh`
2. 删除 `tools/fetch_ideas.py`（功能已在 Harness `poll_tasks()` 中）
3. 删除 `tools/writeback_results.py`（功能已在 Harness `on_task_complete()` 中）
4. `run_full_pipeline.py` 保留为备用批量入口（可选删除）
5. 更新 CLAUDE.md 文档

---

## 7. 技术细节与挑战

### 7.1 单 idea 执行时间长

一个 idea 的完整三阶段评审耗时约 **20-60 分钟**（取决于轮数和 Claude 响应速度）。这意味着：

- **Drain 超时**必须设置足够长（至少 90 分钟）
- **Worker Runtime 心跳**必须区分"在跑"和"卡死"：进程活着但 60 分钟无 stdout 输出 → 可能卡住
- **Semaphore(2)**：每台 Worker 同时只跑 2 个 CLI 子进程，但一个 idea 内部会交替创建多个 session，不会超过 2 个并发

```
时间线（单 idea）:

0min   入口 Triage: 3 个并行 session → 串行 MetaJudge
       ├─ [CLI-1] Triage-1  ──(2min)──┐
       ├─ [CLI-2] Triage-2  ──(2min)──┤  Semaphore(2): 同时跑 2 个
       └─ [等待]  Triage-3  ──────────┘  → [CLI-1 释放] → Triage-3 (2min)
                                         → MetaJudge (1min)

7min   迭代 Round 1:
       ├─ [CLI-1] Excavator ──(5min)──┐
       └─ [CLI-2] Rescuer   ──(5min)──┘  并行
       → [CLI-1] Terminal   ──(3min)──    串行

15min  迭代 Round 2: (同上)
...
45min  出口 Triage: (同入口)
50min  Terminal Final: (1min)
```

### 7.2 Terminal Session 的跨轮持久性

Terminal Agent 使用 `resume=session_id` 跨轮累积上下文。这意味着：

- **Terminal 的所有轮次必须在同一台 Worker 上执行**（session 文件绑定本地磁盘）
- 但这不是问题，因为整个 idea 的评审是一个原子任务 — 不会跨 Worker 分片
- Elastic-Agent 的调度粒度是 **整个 idea**，不是单个轮次

```python
# reviewer.py:257-281 — Terminal session 持久性
async def _call_terminal(self, prompt, ...):
    opts = ClaudeAgentOptions(model=MODEL_NAME)
    if self._terminal_session_id:
        opts.resume = self._terminal_session_id   # 跨轮复用
    result = await query(prompt=prompt, options=opts)
    self._terminal_session_id = result.session_id  # 保存 session_id
    return result
```

### 7.3 API Token 安全

当前 Manager API Token 硬编码在多个文件中：

```python
# 当前（不安全）:
TOKEN = "7c93bcbe6c87e55f28f38a0701bcbde5897e38d49e4334dac137e0801b16cf42"
```

迁移后，Token 通过框架的应用凭证通道安全传递：

```python
# 迁移后:
# Harness 声明需要 manager_api_token
# 框架在 Bootstrap 时将 Token 注入 Worker 环境变量
# run_single_idea.py 从环境变量读取:
TOKEN = os.environ["MANAGER_API_TOKEN"]
```

### 7.4 代码同步

当前修改代码后需要手动 scp 到 4 台服务器。迁移后：

**方案 A（推荐）：Git clone**
- Bootstrap 时 git clone 最新代码
- 如果需要更新：框架重新 Bootstrap（创建新 Worker，销毁旧 Worker）
- 适合代码不频繁变更的场景（IRA 的代码已经稳定）

**方案 B：Rolling update**
- 框架提供代码更新 API → Worker Runtime 执行 `git pull`
- 不需要重建 Worker
- 适合代码频繁变更的开发阶段

### 7.5 结果文件存储

当前结果存在各 Worker 的 `dispatch/results/` 目录。迁移后有两种策略：

| 策略 | 说明 | 适用场景 |
|------|------|---------|
| **API 直写（推荐）** | `on_task_complete()` 直接写回 Manager API，不需要在 Worker 持久化结果 | 生产环境 |
| **Worker 本地 + 上传** | 结果存 Worker 本地，完成后上传到 S3 或 Manager | 需要保留完整 trace 数据时 |

推荐方案：API 直写 + trace 文件上传到 S3 备份。

```python
async def on_task_complete(self, task, result):
    # 1. 写回 API（核心结果）
    await self._writeback_to_api(task, result)

    # 2. 上传 trace 到 S3（调试/审计用，可选）
    trace_dir = f"/home/ubuntu/idea_review/results/trace_{task.id}"
    await self.s3.upload_directory(trace_dir, f"traces/{task.id}/")
```

### 7.6 与现有批量工具的兼容

迁移后，`main.py`（单机单 idea）仍然可以独立使用，不依赖框架：

```bash
# 本地开发/调试，不经过 Elastic-Agent:
python main.py samples/proposal.md -o results/result.json

# 通过 Elastic-Agent 分布式执行:
# 只需要 pending idea 在 Manager API 中 → 框架自动拉取 → 自动分发 → 自动执行 → 自动写回
```

### 7.7 改造量评估

| 模块 | 变化 | 工作量 |
|------|------|--------|
| `dispatch/run_single_idea.py` | **新增**：从 run_full_pipeline.py 提取单 idea 入口 | 小（~80 行） |
| `IdeaReviewHarness` | **新增**：Harness 接口实现 | 中（~250 行） |
| Bootstrap 步骤 | **新增**：5 步 Bootstrap 定义 | 小（~80 行） |
| `reviewer.py` | **不变** | 0 |
| `batch_triage.py` | **不变** | 0 |
| `config.py` | **微调**：API Token 从硬编码改为环境变量 | 极小 |
| `prompts/*` | **不变** | 0 |
| `check_remote_progress.sh` | **删除** | 0 |
| `tools/fetch_ideas.py` | **删除**（功能移入 Harness） | 0 |
| `tools/writeback_results.py` | **删除**（功能移入 Harness） | 0 |
| **总计** | 新增 ~410 行，删除 3 个工具文件 | 1-2 周 |

---

## 8. 迁移对框架提出的需求

### 8.1 IRA 特有但值得框架支持的

| 需求 | 说明 | 普适性判断 |
|------|------|-----------|
| **长任务超时** | 单个 idea 评审 20-60 分钟，Drain 超时需 90 分钟 | 通用 — 任何 ML 训练/长推理任务 |
| **无常驻服务** | IRA 不需要 systemd 服务，每个任务是独立 CLI 调用 | 通用 — batch processing 模式 |
| **stdin 任务传递** | 通过 stdin 传递任务 JSON，避免文件分发 | 通用 — 轻量任务传递 |
| **输出文件上传** | 任务完成后上传 trace 文件到 S3 | 通用 — 任何有中间产物的任务 |

### 8.2 与 CCM 和 agent-ml-research 需求的交叉验证

| 需求 | IRA（idea-review） | CCM | agent-ml-research | 结论 |
|------|---------------|-----|-------------------|------|
| Worker Runtime | ✅ 远程执行 pipeline | ✅ 远程 Claude Code | ✅ 替换 SSH | **框架核心** |
| 日志流式传输 | ✅ 评审进度 | ✅ WebSocket 前端 | ✅ 飞书告警 | **框架核心** |
| 有状态亲和性 | ✅ Terminal session | ✅ session resume | ✅ 项目绑定 | **框架核心** |
| 优雅缩容 | ✅ 20-60min 任务 | ✅ 30min 任务 | ✅ 长时间训练 | **框架核心** |
| 双层凭证 | ✅ API Token | ✅ Git key | ✅ WandB/HF/Feishu | **框架核心** |
| 扩缩容信号 | ✅ pending idea 数 | ✅ 任务队列深度 | ✅ 项目数量 | **框架核心** |
| 工作区同步 | ✅ git clone 代码 | ✅ Project clone | ✅ 代码部署 | **框架核心** |
| 长任务超时 | ✅ 90min drain | ✅ 30min | ✅ 数小时训练 | 框架支持 |
| 无常驻服务模式 | ✅ | - | - | 框架支持 |
| 输出文件上传 | ✅ trace 文件 | - | ✅ 训练产物 | 框架支持 |

三个 Harness 的核心需求完全一致，IRA 新增了"无常驻服务模式"和"长任务超时"两个需求，进一步丰富了框架的能力矩阵。

### 8.3 IRA 作为第三个 Harness 的独特价值

IRA 代表了一类 **token 消耗密集型 Agent 应用**：单次任务推理时间长（20-60 分钟）、频繁撞 limit、任务量波动大。这类项目对 Elastic-Agent 的两个核心能力有刚需：

1. **弹性机器分配**：任务量波动大（5 个 idea vs 100 个 idea），固定机器数既浪费又不够用。框架根据 pending 任务数自动开/关 EC2，处理完缩容到 0
2. **自动凭证轮换**：单个任务 token 消耗高，5h 窗口内必然撞 limit。框架实时监控使用率，自动切到下一个可用账号，秒级响应而非人工 2 小时后才发现
3. **验证框架完整性**：IRA 没有任何自建基础设施代码，如果框架能让这样一个项目直接获得弹性计算和凭证管理能力，说明框架的抽象层设计是自足的

三个 Harness 代表了三种不同的接入动机：

```
token 密集 + 无基础设施 ─── 已有自建基础设施 ─── 单机需要扩展
        IRA                    agent-ml              CCM
        │                        │                    │
   需要弹性资源              需要替换自建代码        需要分布式能力
   + 自动切号                + 获得高级功能          + 凭证管理
   (~410 行 Harness)       (~2000→300 行)         (~500+200 行)
```
