# 独立测试与 Mock 策略

> 三个仓库可以完全独立开发和测试。本文档定义每个仓库的隔离测试策略、Mock 组件、以及最后拼接的集成测试方案。

---

## 1. 隔离原则

```
EA (框架)               ABS (Agent Service)          ABE (做书前后端)
  无上游依赖               依赖 EA                      依赖 ABS
  │                        │                            │
  ├ DryRunProvider         ├ DryRunProvider (来自 EA)    ├ MockAgentService
  ├ MockWorker             ├ MockWorker (来自 EA)        ├ MockOSS
  ├ MockOAuthServer        ├ MockClaudeOutput            ├ WebhookSimulator
  └ MockOSS               └ WebhookCatcher              └ (不需要真 Worker)
```

每个仓库的测试只需要自己的代码 + Mock 依赖，不需要启动其他仓库的服务。

---

## 2. Elastic-Agent 框架 [EA] 独立测试

EA 无上游依赖，全部外部交互都可以 Mock。

### 2.1 Mock 组件

#### DryRunProvider（已有 T-118）

模拟云 API，不消耗真实资源：

```python
class DryRunProvider(CloudProvider):
    """内存中模拟实例生命周期，记录所有操作。"""

    def __init__(self):
        self.instances: dict[str, Instance] = {}
        self.operations: list[dict] = []

    async def create_instance(self, config):
        inst = Instance(id=f"dryrun-{uuid4()[:8]}", status="running", ...)
        self.instances[inst.id] = inst
        self.operations.append({"action": "create", "id": inst.id})
        return inst

    async def terminate_instance(self, instance_id):
        del self.instances[instance_id]
        self.operations.append({"action": "terminate", "id": instance_id})

    # ... list_instances, wait_until_running 等同理
```

用途：所有不涉及真实云的测试——Manager 编排逻辑、Bootstrap 状态机、对账算法、扩缩容 API。

#### MockWorker

模拟一个 Worker Runtime 进程，通过 WebSocket 连接 Manager：

```python
class MockWorker:
    """连接到 Manager WS 端点，响应命令，发送模拟事件。"""

    def __init__(self, manager_url, token):
        self.ws = None
        self.token = token
        self.manager_url = manager_url

    async def connect(self):
        self.ws = await websockets.connect(f"{self.manager_url}/ws/runtime")
        await self.ws.send(json.dumps({"type": "auth", "token": self.token}))

    async def handle_execute(self, msg):
        """收到 EXECUTE → 发送模拟 LOG 事件 → 发送 PROCESS_EXIT"""
        task_id = msg["task_id"]
        # 模拟 Claude Code NDJSON 输出
        for line in self.script.get(task_id, DEFAULT_NDJSON_SEQUENCE):
            await self.ws.send(json.dumps({
                "type": "LOG", "task_id": task_id,
                "stream": "stdout", "data": line,
                "timestamp": datetime.utcnow().isoformat(),
            }))
            await asyncio.sleep(0.01)
        await self.ws.send(json.dumps({
            "type": "PROCESS_EXIT", "task_id": task_id,
            "exit_code": 0, "timestamp": datetime.utcnow().isoformat(),
        }))

    async def send_heartbeat(self):
        while self.ws:
            await self.ws.send(json.dumps({"type": "HEARTBEAT", "uptime_seconds": 123}))
            await asyncio.sleep(30)
```

可配置行为：
- `script`: 预录的 NDJSON 输出序列（按 task_id 区分）
- `exit_code`: 模拟成功/失败
- `delay`: 模拟执行耗时
- `disconnect_after`: 模拟断线

用途：测试 Manager ↔ Worker 通信、日志传输、进程管理、健康检查、断线重连。

#### MockOAuthServer

模拟 171mail + Anthropic OAuth 端点：

```python
class MockOAuthServer:
    """本地 HTTP 服务器，模拟登录流程中的所有外部 API。"""

    # 171mail 端点
    POST /api/v1/claude/send       → {"deviceId": "mock-device", "clientSha": "mock-sha"}
    GET  /api/v1/getClaudeMessage  → {"code": "mock-magic-link"}
    POST /api/v1/claude/verify     → {"cookie": "mock-cookie", "sessionKey": "mock-key"}

    # Anthropic 端点
    GET  /api/account              → {"email_address": "{expected_email}"}
    POST /v1/oauth/token           → {"access_token": "mock-at", "refresh_token": "mock-rt",
                                       "expires_in": 3600}
    GET  /api/oauth/usage          → {"five_hour": {"utilization": N, "resets_at": "..."},
                                       "seven_day": {"utilization": M, "resets_at": "..."}}
```

通过修改返回值测试不同场景：
- 正常登录成功
- 171mail 超时（poll 不返回 code）
- OAuth 授权失败
- Token refresh 失败
- 额度 API 限流（返回 429）
- 额度 85%/95% 阈值

用途：测试 ClaudeOAuthProvider、QuotaMonitor、自动轮换。不需要真 Playwright/Chrome/mitmproxy。

> 注：登录流程的 Playwright + mitmproxy 部分需要单独的集成测试（T-135），mock 无法覆盖浏览器行为。MockOAuthServer 用于测试登录流程之外的 token 管理逻辑。

#### MockOSS

模拟 OSS/S3 存储，用本地文件系统：

```python
class MockOSS:
    """本地文件系统模拟 OSS。"""

    def __init__(self, root_dir: Path):
        self.root = root_dir

    async def put_object(self, key, content):
        path = self.root / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)

    async def get_object(self, key):
        return (self.root / key).read_bytes()

    async def list_objects(self, prefix):
        return [str(p.relative_to(self.root)) for p in self.root.glob(f"{prefix}*")]
```

用途：测试 FileSyncManager 上传逻辑、manifest 生成、文件读取 API。

### 2.2 EA 测试矩阵

| 测试层 | Mock 依赖 | 覆盖范围 |
|---|---|---|
| 单元测试 | 纯内存（无 Mock 服务） | 数据模型、状态机、算法、序列化 |
| 组件测试 | DryRunProvider + MockWorker | Manager 编排、WS 通信、Bootstrap、健康检查 |
| 凭证测试 | MockOAuthServer | Token 管理、额度监控、自动轮换、登录结果处理 |
| 文件同步测试 | MockOSS + MockWorker | FileSyncManager、TaskSyncMapper、manifest |
| 集成测试（真实云） | 真实阿里云/AWS（CI 可选跳过） | 端到端生命周期 |

### 2.3 EA 需要提供给下游的测试工具

EA 作为 library，应该导出这些 Mock 供 ABS 使用：

```python
from elastic_agent.testing import (
    DryRunProvider,     # 模拟云
    MockWorker,         # 模拟 Worker
    MockOAuthServer,    # 模拟登录 API
    MockOSS,            # 模拟 OSS
    create_test_manager,  # 一键创建带 Mock 的 Manager 实例
)
```

---

## 3. Audiobook Agent Service [ABS] 独立测试

ABS 依赖 EA 框架，但不依赖真实云和真实 Worker。使用 EA 导出的 Mock 工具。

### 3.1 Mock 组件

#### 使用 EA 的 Mock（直接 import）

```python
from elastic_agent.testing import DryRunProvider, MockWorker, MockOSS, create_test_manager

# 创建测试用 Manager（内置 DryRunProvider + MockWorker）
manager = create_test_manager(
    provider=DryRunProvider(),
    worker_count=3,  # 自动创建 3 个 MockWorker 并连接
)
```

#### MockClaudeOutput

预录的 Claude Code NDJSON 输出序列，模拟不同做书场景：

```python
class MockClaudeOutput:
    """生成模拟的 Claude Code stream-json NDJSON 输出。"""

    @staticmethod
    def production_success(book_slug="test-book", phases=10) -> list[str]:
        """模拟一本书完整生产成功的输出序列。"""
        lines = [
            '{"type":"system","model":"claude-opus-4-6"}',
        ]
        for phase in range(phases):
            lines.append(f'{{"type":"assistant","content":"开始 Phase {phase}..."}}')
            lines.append(f'{{"type":"tool_use","name":"Write","input":{{"path":"state.json"}}}}')
        lines.append(f'{{"type":"result","session_id":"mock-session-{uuid4()[:8]}","cost_usd":2.5}}')
        return lines

    @staticmethod
    def production_failure_at_phase(phase: int) -> list[str]: ...

    @staticmethod
    def edit_success() -> list[str]: ...

    @staticmethod
    def stalled(lines_before_stall: int) -> list[str]: ...
```

用途：配置 MockWorker 的输出行为，覆盖生产成功/失败/卡住/修改等场景。

#### WebhookCatcher

捕获 ABS 发出的 Webhook，用于断言：

```python
class WebhookCatcher:
    """本地 HTTP 服务器，接收并记录 Webhook 事件。"""

    def __init__(self, port=9999):
        self.events: list[dict] = []
        self.app = FastAPI()

        @self.app.post("/webhook")
        async def catch(request: Request):
            body = await request.json()
            self.events.append(body)
            return {"ok": True}

    def wait_for_event(self, event_type, timeout=10) -> dict:
        """等待指定类型的事件到达。"""
        ...

    def assert_event_sequence(self, expected_types: list[str]):
        """断言收到的事件类型序列。"""
        actual = [e["event_type"] for e in self.events]
        assert actual == expected_types
```

用途：验证 ABS 的 Webhook 发送是否正确——事件类型、顺序、签名、幂等。

### 3.2 ABS 测试矩阵

| 测试层 | Mock 依赖 | 覆盖范围 |
|---|---|---|
| 单元测试 | 纯内存 | BookQueue、SessionRegistry、SlotScheduler、Phase 映射 |
| 组件测试 | DryRunProvider + MockWorker + MockClaudeOutput | Harness 编排、做书/修改全流程、进度检测 |
| API 测试 | 同上 + httpx TestClient | 10 个 API 端点的请求/响应/错误码 |
| Webhook 测试 | 同上 + WebhookCatcher | 事件发送、重试、签名、幂等 |
| 文件测试 | MockOSS | manifest 读写、最终稿选择、chat/history 解析 |

### 3.3 ABS 典型测试场景

```python
async def test_production_happy_path():
    """完整做书流程：produce → 执行 → 完成 → session 注册 → webhook。"""
    manager = create_test_manager(worker_count=1)
    harness = AudiobookHarness(test_config)
    catcher = WebhookCatcher()

    # 配置 MockWorker 返回成功的 NDJSON 序列
    manager.mock_workers[0].set_script("task-1", MockClaudeOutput.production_success())

    # 提交做书
    resp = await client.post("/api/tasks/produce", json={
        "task_id": "task-1", "book_slug": "test", "raw_text": "...",
        "callback": {"url": catcher.url, "secret_id": "test"},
        ...
    })
    assert resp.json()["status"] == "queued"

    # 等待执行完成
    await catcher.wait_for_event("task.production.completed", timeout=5)

    # 验证
    assert harness.session_registry.get("task-1").session_id == "mock-session-xxx"
    catcher.assert_event_sequence([
        "task.production.queued",
        "task.production.started",
        "task.phase.changed",  # 可能多个
        "task.session.registered",
        "task.production.completed",
    ])
```

---

## 4. audio_book_echo_editor [ABE] 独立测试

ABE 依赖 ABS 的 API 和 Webhook，但不需要启动真实的 ABS。

### 4.1 Mock 组件

#### MockAgentService

模拟 ABS 的全部 API 端点，可配置返回值：

```python
class MockAgentService:
    """模拟 Audiobook Agent Service 的 FastAPI 应用。"""

    def __init__(self):
        self.app = FastAPI()
        self.tasks: dict[str, dict] = {}

        @self.app.post("/api/tasks/produce")
        async def produce(request: Request):
            body = await request.json()
            self.tasks[body["task_id"]] = {"status": "queued", "phase": None}
            return {"success": True, "task_id": body["task_id"],
                    "status": "queued", "queue_position": 0}

        @self.app.get("/api/tasks/{task_id}/status")
        async def status(task_id: str):
            task = self.tasks.get(task_id, {})
            return {"task_id": task_id, "status": task.get("status", "unknown"), ...}

        @self.app.post("/api/tasks/{task_id}/cancel")
        async def cancel(task_id: str):
            self.tasks[task_id]["status"] = "cancelled"
            return {"success": True, "status": "cancelled"}

        @self.app.post("/api/tasks/{task_id}/chat")
        async def chat(task_id: str, request: Request):
            return {"success": True, "edit_run_id": f"edit-{uuid4()[:8]}", "status": "running"}

        # ... 其余端点同理

    def simulate_progress(self, task_id, phases):
        """模拟任务经过多个 Phase。"""
        ...

    def simulate_completion(self, task_id):
        """模拟任务完成。"""
        self.tasks[task_id]["status"] = "completed"
```

用途：测试 ElasticAgentClient 的全部方法、ElasticBookProductionService 的组装逻辑。

#### WebhookSimulator

主动向 ABE 的 webhook 端点发送模拟事件：

```python
class WebhookSimulator:
    """向 audio_book_echo_editor 发送模拟 Webhook 事件。"""

    def __init__(self, target_url, secret):
        self.target_url = target_url
        self.secret = secret
        self.sequence = 0

    async def send_event(self, event_type, task_id, data):
        self.sequence += 1
        body = json.dumps({
            "event_id": f"evt-{uuid4()[:8]}",
            "event_type": event_type,
            "task_id": task_id,
            "sequence": self.sequence,
            "timestamp": datetime.utcnow().isoformat(),
            "data": data,
        })
        signature = hmac.new(self.secret.encode(), body.encode(), hashlib.sha256).hexdigest()
        await httpx.AsyncClient().post(self.target_url, content=body, headers={
            "X-Elastic-Agent-Signature": f"sha256={signature}",
            "X-Elastic-Agent-Event-Id": f"evt-{uuid4()[:8]}",
            "X-Elastic-Agent-Timestamp": str(int(time.time())),
        })

    async def simulate_full_production(self, task_id):
        """模拟完整做书流程的 webhook 序列。"""
        await self.send_event("task.production.queued", task_id, {"status": "queued"})
        await self.send_event("task.production.started", task_id, {"status": "running", "worker_id": "mock-w1"})
        for phase, pct in [("phase_01_decomposition", 10), ("phase_04_draft", 40), ("phase_09_delivery", 95)]:
            await self.send_event("task.phase.changed", task_id, {"phase": phase, "progress_pct": pct})
        await self.send_event("task.production.completed", task_id, {
            "status": "completed", "session_id": "mock-session",
            "oss": {"manifest_key": f"elastic-agent/tasks/{task_id}/_sync_manifest.json", ...},
        })
```

用途：测试 WebhookService 的验签、幂等、状态映射、AgentOutput 回灌。

#### MockOSS（ABE 版）

ABE 只读 OSS（不写），所以 Mock 更简单：

```python
class MockOSSReader:
    """提供预置的 manifest 和文件内容。"""

    def __init__(self):
        self.files: dict[str, bytes] = {}

    def put(self, key, content):
        self.files[key] = content.encode() if isinstance(content, str) else content

    def setup_completed_task(self, task_id):
        """预置一个已完成任务的全部 OSS 文件。"""
        prefix = f"elastic-agent/tasks/{task_id}"
        self.put(f"{prefix}/_sync_manifest.json", json.dumps({
            "task_id": task_id, "status": "completed", "files": [
                {"path": "delivery/audiobook_manuscript.md", "oss_key": f"{prefix}/delivery/audiobook_manuscript.md",
                 "role": "delivery_manuscript", "size": 50000, "md5": "abc", "synced_at": "..."},
                {"path": "workspace/manuscript_final.md", ...},
            ],
        }))
        self.put(f"{prefix}/delivery/audiobook_manuscript.md", "# 最终讲稿内容...")
```

用途：测试 OssFileService 的 manifest 解析、最终稿优先级选择、文件读取。

### 4.2 ABE 测试矩阵

| 测试层 | Mock 依赖 | 覆盖范围 |
|---|---|---|
| 单元测试 | 纯内存 + mock 函数 | 状态映射、slug 生成、AgentOutput 格式 |
| Client 测试 | MockAgentService (httpx) | ElasticAgentClient 全部方法 |
| Webhook 测试 | WebhookSimulator | 验签、幂等、sequence、状态更新、回灌 |
| OSS 测试 | MockOSSReader | manifest 解析、最终稿选择、预签名 URL |
| API 测试 | 全部 Mock + TestClient | script-production 全部端点 |
| 前端测试 | Mock API 响应 | 组件渲染、交互 |

---

## 5. 渐进式测试级别

除了全 Mock 和全真实两个极端，每个依赖可以独立地在 Mock / Real 之间切换。通过环境变量选择级别，同一套测试代码适配不同环境。

### 5.1 六个可替换的依赖

| 依赖 | Mock 实现 | Real 实现 | 切换条件 |
|---|---|---|---|
| 云 API | DryRunProvider | AliyunProvider / AWSProvider | 有云凭证 |
| Worker Runtime | MockWorker (进程内 WS) | 真实 Worker Runtime (本地进程或远程 VM) | 有 Worker 二进制 |
| Claude Code CLI | MockClaudeOutput (预录 NDJSON) | 真实 `claude` CLI | 有 Claude 账号 |
| OSS/S3 | MockOSS (本地文件系统) | 真实阿里云 OSS / AWS S3 | 有 OSS 凭证 |
| Agent Service | MockAgentService | 真实 ABS 实例 | ABS 已部署 |
| ABE 环境 | WebhookCatcher | ABE 测试环境 | ABE 已部署 |

### 5.2 EA 框架的测试级别

| 级别 | 云 | Worker | Claude | OSS | 验证什么 | 耗时 | 成本 |
|---|---|---|---|---|---|---|---|
| **L0 Unit** | — | — | — | — | 数据模型、算法、序列化 | 秒 | 0 |
| **L1 Component** | DryRun | Mock | Mock | Mock | Manager 编排、WS 通信、Bootstrap 状态机 | 秒 | 0 |
| **L2 Cloud** | **真实** | Mock | Mock | Mock | 云 API 调用正确性（创建/销毁/标签） | 分钟 | ~¥0.1 |
| **L3 Worker** | **真实** | **真实**(本地) | Mock | Mock | Bootstrap 全流程、Worker Runtime 连接 | 分钟 | ~¥0.5 |
| **L4 Agent** | **真实** | **真实** | **真实** | Mock | Claude Code 执行、日志双写、session_id 提取 | 10min+ | Claude token |
| **L5 Storage** | **真实** | **真实** | Mock | **真实** | FileSyncManager → OSS、manifest 生成 | 分钟 | ~¥0.01 |
| **L6 Full** | **真实** | **真实** | **真实** | **真实** | 端到端：创建 VM → 做书 → OSS 同步 | 1h+ | ¥5+ |

```python
# conftest.py — 根据环境变量选择级别
import os, pytest

TEST_LEVEL = int(os.environ.get("EA_TEST_LEVEL", "1"))

@pytest.fixture
def provider():
    if TEST_LEVEL >= 2 and os.environ.get("ALICLOUD_ACCESS_KEY_ID"):
        return AliyunProvider(real_config)
    return DryRunProvider()

@pytest.fixture
def worker(provider):
    if TEST_LEVEL >= 3:
        return LocalWorkerProcess()   # 本地启动真实 Worker Runtime
    return MockWorker()

@pytest.fixture
def claude_runner():
    if TEST_LEVEL >= 4 and shutil.which("claude"):
        return RealClaudeRunner()     # 真实 CLI
    return MockClaudeOutput.production_success()

@pytest.fixture
def oss():
    if TEST_LEVEL >= 5 and os.environ.get("ABS_OSS_ACCESS_KEY_ID"):
        return RealOSSClient(real_oss_config)
    return MockOSS(tmp_path)
```

L2 的价值：验证云 SDK 调用是否正确（参数、标签、安全组），不需要等 Bootstrap。创建实例后立即销毁，成本极低。

L3 的价值：验证 SSH 连接、Bootstrap 命令执行、Worker Runtime 启动。不消耗 Claude token。Worker Runtime 在本地进程中运行（不需要真实 VM）：

```python
class LocalWorkerProcess:
    """在本地启动真实的 Worker Runtime 进程，连接到测试 Manager。"""

    async def start(self, manager_url, token):
        self.proc = await asyncio.create_subprocess_exec(
            "python", "-m", "elastic_agent.worker",
            "--manager-url", manager_url,
            "--token", token,
        )

    async def stop(self):
        self.proc.terminate()
```

L4 的价值：验证真实 Claude Code CLI 的行为——NDJSON 输出格式、session_id 提取、`--resume` 是否可用。这是 Mock 无法替代的，因为 Claude Code 的输出格式可能随版本变化。

### 5.3 ABS Agent Service 的测试级别

| 级别 | EA 框架 | Worker | Claude | OSS | Webhook 目标 | 验证什么 |
|---|---|---|---|---|---|---|
| **L0 Unit** | — | — | — | — | — | 队列、注册表、调度算法 |
| **L1 Component** | DryRun + MockWorker | Mock | Mock | Mock | WebhookCatcher | Harness 编排、API |
| **L2 Real Worker** | DryRun | **真实**(本地) | Mock | Mock | WebhookCatcher | Worker Runtime 集成 |
| **L3 Real Claude** | DryRun | **真实** | **真实** | Mock | WebhookCatcher | 真实做书流程（无真实 VM） |
| **L4 Real OSS** | DryRun | **真实** | **真实** | **真实** | WebhookCatcher | 文件同步到 OSS |
| **L5 Real Cloud** | **真实云** | **真实** | **真实** | **真实** | WebhookCatcher | 全真实基础设施 |
| **L6 Full** | **真实云** | **真实** | **真实** | **真实** | **ABE 测试环境** | 含 ABE 的全链路 |

L3 是一个关键级别：在本地机器上用 DryRunProvider（不创建真实 VM），但启动真实的 Worker Runtime 进程 + 真实的 Claude Code CLI，执行一本短书的生产。验证整个做书流程的业务逻辑，不需要云账号。

### 5.4 ABE 做书前后端的测试级别

| 级别 | Agent Service | OSS | 验证什么 |
|---|---|---|---|
| **L0 Unit** | — | — | 模型、映射、slug 生成 |
| **L1 Mock** | MockAgentService | MockOSSReader | Client、Webhook、API 全覆盖 |
| **L2 Real ABS** | **真实 ABS**(L1 模式) | MockOSSReader | 跨服务 HTTP 调用 |
| **L3 Real OSS** | **真实 ABS** | **真实 OSS** | 文件读取端到端 |
| **L4 Full** | **真实 ABS**(L5/L6 模式) | **真实 OSS** | ABE → ABS → Worker → Claude → OSS → Webhook → ABE |

L2 的价值：ABE 后端连接到一个真实运行的 ABS 实例（ABS 自己用 DryRun + MockWorker），验证 HTTP 接口契约是否对齐。不需要云账号、不需要 Claude。

---

## 6. 三仓库拼接的集成测试

独立测试通过后，逐步拼接：

### 6.1 拼接矩阵

```
          ┌─ ABE ─┐   ┌── ABS ──┐   ┌── EA ──┐   ┌─ 外部 ─┐
          │       │   │         │   │        │   │        │
拼接 1:   │ Mock  │ → │  Real   │ → │ DryRun │   │ Mock   │
          │       │   │ L1 模式 │   │+MockWk │   │        │
          │       │   │         │   │        │   │        │
拼接 2:   │ Mock  │ → │  Real   │ → │ DryRun │   │ Real   │
          │       │   │ L3 模式 │   │+RealWk │   │ Claude │
          │       │   │         │   │+RealCC │   │        │
          │       │   │         │   │        │   │        │
拼接 3:   │ Real  │ → │  Real   │ → │ DryRun │   │ Mock   │
          │ 测试环境│   │ L1 模式 │   │+MockWk │   │ Claude │
          │       │   │         │   │        │   │        │
拼接 4:   │ Real  │ → │  Real   │ → │ Real   │   │ Real   │
          │ 测试环境│   │ L6 模式 │   │ 阿里云  │   │ 全部   │
          └───────┘   └─────────┘   └────────┘   └────────┘
```

### 6.2 拼接测试用例

| ID | 拼接 | EA 模式 | ABS 模式 | ABE | Claude | 测试内容 |
|---|---|---|---|---|---|---|
| I-001 | 1 | DryRun+MockWk | L1 | Mock | Mock | ABS produce → MockWorker → Webhook |
| I-002 | 2 | DryRun+RealWk | L3 | Mock | **Real** | 真实 Claude Code 做书 → session_id |
| I-003 | 2 | DryRun+RealWk | L4 | Mock | **Real** | 做书 + 文件同步到**真实 OSS** |
| I-004 | 3 | DryRun+MockWk | L1 | **Real 测试环境** | Mock | ABE 创建任务 → ABS → Webhook → ABE 回灌 |
| I-005 | 3 | DryRun+MockWk | L1 | **Real 测试环境** | Mock | ABE chat 修改 → ABS → Webhook → ABE 更新 |
| I-006 | 4 | **真实阿里云** | L6 | **Real 测试环境** | **Real** | 全链路：ABE → ABS → 阿里云 ECS → Claude → OSS → Webhook |

---

## 7. Mock 工具的代码归属

| Mock 组件 | 归属仓库 | 导出方式 | 消费方 |
|---|---|---|---|
| DryRunProvider | EA | `elastic_agent.testing` | EA + ABS |
| MockWorker | EA | `elastic_agent.testing` | EA + ABS |
| LocalWorkerProcess | EA | `elastic_agent.testing` | EA + ABS（L3+ 级别） |
| MockOAuthServer | EA | `elastic_agent.testing` | EA |
| MockOSS | EA | `elastic_agent.testing` | EA + ABS |
| create_test_manager | EA | `elastic_agent.testing` | ABS |
| MockClaudeOutput | ABS | `audiobook_agent_service.testing` | ABS |
| WebhookCatcher | ABS | `audiobook_agent_service.testing` | ABS |
| MockAgentService | ABE | `tests/mocks/` | ABE |
| WebhookSimulator | ABE | `tests/mocks/` | ABE |
| MockOSSReader | ABE | `tests/mocks/` | ABE |

---

## 8. CI 配置

```yaml
# 每个仓库的 CI 独立运行
# 通过环境变量 *_TEST_LEVEL 和凭证环境变量控制测试级别

# EA CI
test:
  script:
    - EA_TEST_LEVEL=0 pytest tests/ -m "level0"              # 必跑：纯单元测试
    - EA_TEST_LEVEL=1 pytest tests/ -m "level0 or level1"    # 必跑：+ Mock 组件测试
    - |
      if [ -n "$ALICLOUD_ACCESS_KEY_ID" ]; then
        EA_TEST_LEVEL=2 pytest tests/ -m "level2"            # 可选：真实云 API（创建+立即销毁）
      fi
    - |
      if [ -n "$CLAUDE_CODE_AVAILABLE" ]; then
        EA_TEST_LEVEL=4 pytest tests/ -m "level4"            # 可选：真实 Claude Code
      fi

# ABS CI
test:
  script:
    - pip install elastic-agent                               # 安装框架（含 testing 模块）
    - ABS_TEST_LEVEL=0 pytest tests/ -m "level0"             # 必跑：纯单元测试
    - ABS_TEST_LEVEL=1 pytest tests/ -m "level0 or level1"   # 必跑：+ DryRun 组件测试
    - |
      if [ -n "$CLAUDE_CODE_AVAILABLE" ]; then
        ABS_TEST_LEVEL=3 pytest tests/ -m "level3"           # 可选：DryRun + 真实 Claude
      fi
    - |
      if [ -n "$ABS_OSS_ACCESS_KEY_ID" ]; then
        ABS_TEST_LEVEL=4 pytest tests/ -m "level4"           # 可选：+ 真实 OSS
      fi

# ABE CI
test:
  script:
    - ABE_TEST_LEVEL=0 pytest tests/ -m "level0"             # 必跑：纯单元测试
    - ABE_TEST_LEVEL=1 pytest tests/ -m "level0 or level1"   # 必跑：+ Mock ABS 测试
    - |
      if [ -n "$ABS_TEST_URL" ]; then
        ABE_TEST_LEVEL=2 pytest tests/ -m "level2"           # 可选：连接真实 ABS (L1 模式)
      fi
    - npm run test                                            # 前端测试
```

每次 push 必跑 L0 + L1（全 Mock，秒级完成，零成本）。L2+ 按凭证可用性自动启用。

### 本地开发时的常用命令

```bash
# 快速验证（全 Mock，几秒完成）
EA_TEST_LEVEL=1 pytest tests/

# 验证云 API 调用（需要阿里云凭证，几分钟，~¥0.1）
EA_TEST_LEVEL=2 pytest tests/ -m "level2"

# 验证真实 Claude Code（需要 Claude 账号，10+ 分钟）
EA_TEST_LEVEL=4 pytest tests/ -m "level4"

# ABS 连接 ABE 测试环境
ABS_TEST_LEVEL=6 ABE_WEBHOOK_URL=https://test.audiobook.example.com/api/elastic-agent/webhook pytest tests/ -m "level6"
```
