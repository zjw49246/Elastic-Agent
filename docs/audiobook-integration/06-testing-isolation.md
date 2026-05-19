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

## 5. 三仓库拼接的集成测试

独立测试通过后，用真实组件替换 Mock 逐步拼接：

### 5.1 拼接阶段

```
阶段 1: EA + ABS（不含 ABE）
  EA 真实 Manager + DryRunProvider + MockWorker
  ABS 真实 Harness + 真实 API
  验证: produce → MockWorker 执行 → session 注册 → Webhook 发出

阶段 2: EA + ABS + 真实 Worker（不含 ABE）
  EA 真实 Manager + DryRunProvider + 真实 Worker Runtime（本地进程）
  ABS 真实 Harness
  验证: produce → 真实 Worker 上 Claude Code 执行 → 文件同步到 MockOSS

阶段 3: EA + ABS + ABE（全链路，DryRun 云）
  ABE 真实后端 → ABS 真实服务 → EA DryRunProvider + MockWorker
  验证: 前端创建任务 → Elastic 做书 → Webhook → 回灌 → 审核流程

阶段 4: 全真实（含真实云）
  ABE → ABS → EA + 真实阿里云 ECS + 真实 Worker + 真实 OSS
  验证: 端到端全链路
```

### 5.2 拼接测试用例

| ID | 阶段 | 测试内容 |
|---|---|---|
| I-001 | 1 | ABS 使用 EA 创建 MockWorker + Bootstrap |
| I-002 | 1 | ABS produce → MockWorker 执行 → Webhook 到 WebhookCatcher |
| I-003 | 2 | 真实 Worker Runtime 连接 + 执行 Claude Code（本地） |
| I-004 | 3 | ABE 创建 Elastic 任务 → ABS 执行 → Webhook → ABE 回灌 |
| I-005 | 3 | ABE 发送修改 → ABS --resume → Webhook → ABE 更新 |
| I-006 | 4 | 真实阿里云 ECS + 真实做书 + 真实 OSS + 真实 Webhook |

---

## 6. Mock 工具的代码归属

| Mock 组件 | 归属仓库 | 导出方式 | 消费方 |
|---|---|---|---|
| DryRunProvider | EA | `elastic_agent.testing` | EA 自己 + ABS |
| MockWorker | EA | `elastic_agent.testing` | EA 自己 + ABS |
| MockOAuthServer | EA | `elastic_agent.testing` | EA 自己 |
| MockOSS | EA | `elastic_agent.testing` | EA 自己 + ABS |
| create_test_manager | EA | `elastic_agent.testing` | ABS |
| MockClaudeOutput | ABS | `audiobook_agent_service.testing` | ABS 自己 |
| WebhookCatcher | ABS | `audiobook_agent_service.testing` | ABS 自己 |
| MockAgentService | ABE | `tests/mocks/` | ABE 自己 |
| WebhookSimulator | ABE | `tests/mocks/` | ABE 自己 |
| MockOSSReader | ABE | `tests/mocks/` | ABE 自己 |

---

## 7. CI 配置建议

```yaml
# 每个仓库的 CI 独立运行，不依赖其他仓库的服务

# EA CI
test:
  - pytest tests/unit/          # 纯内存，无外部依赖
  - pytest tests/component/     # DryRunProvider + MockWorker
  - pytest tests/credential/    # MockOAuthServer
  - pytest tests/filesync/      # MockOSS
  # 以下仅在 CI 有云凭证时执行
  - pytest tests/integration/ -m "cloud" --skip-if-no-creds

# ABS CI
test:
  - pip install elastic-agent   # 安装框架（含 testing 模块）
  - pytest tests/unit/
  - pytest tests/component/     # 使用 EA 的 DryRunProvider + MockWorker
  - pytest tests/api/           # httpx TestClient
  - pytest tests/webhook/       # WebhookCatcher

# ABE CI
test:
  - pytest tests/unit/
  - pytest tests/elastic/       # MockAgentService + WebhookSimulator + MockOSSReader
  - pytest tests/api/
  - npm run test                # 前端测试
```
