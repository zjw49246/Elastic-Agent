"""Tests for Manager-side WorkerConnectionManager (T-008)."""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from elastic_agent.core.protocols.messages import (
    AuthMessage,
    AuthResultMessage,
    ExecuteMessage,
    EventAckMessage,
    HeartbeatMessage,
    LogMessage,
    ProcessExitMessage,
    StatusMessage,
    StopMessage,
    parse_message,
)
from elastic_agent.core.registry import NodeRecord, NodeRegistry, NodeStatus
from elastic_agent.manager.connection import (
    WorkerConnection,
    WorkerConnectionManager,
    WorkerNotConnectedError,
)


class FakeWebSocket:
    """Minimal WebSocket mock for testing."""

    def __init__(self, incoming: list[str] | None = None):
        self.incoming = list(incoming or [])
        self.sent: list[str] = []
        self.accepted = False
        self.closed = False
        self.client = ("127.0.0.1", 12345)
        self._recv_index = 0

    async def accept(self):
        self.accepted = True

    async def send_text(self, data: str):
        self.sent.append(data)

    async def receive_text(self) -> str:
        if self._recv_index < len(self.incoming):
            msg = self.incoming[self._recv_index]
            self._recv_index += 1
            return msg
        raise StopIteration("No more messages")

    async def close(self):
        self.closed = True


@pytest.fixture
def registry(tmp_path):
    reg = NodeRegistry(tmp_path / "test_registry.json")
    return reg


@pytest.fixture
def manager(registry):
    return WorkerConnectionManager(registry)


class TestWorkerConnection:
    @pytest.mark.asyncio
    async def test_send_message(self):
        ws = FakeWebSocket()
        conn = WorkerConnection("worker-1", ws)

        msg = HeartbeatMessage(uptime_seconds=100)
        await conn.send(msg)

        assert len(ws.sent) == 1
        parsed = json.loads(ws.sent[0])
        assert parsed["type"] == "HEARTBEAT"
        assert parsed["uptime_seconds"] == 100

    @pytest.mark.asyncio
    async def test_last_message_at_updated(self):
        ws = FakeWebSocket()
        conn = WorkerConnection("worker-1", ws)
        before = conn.last_message_at

        await asyncio.sleep(0.01)
        await conn.send(HeartbeatMessage(uptime_seconds=1))

        assert conn.last_message_at >= before


class TestAuthentication:
    @pytest.mark.asyncio
    async def test_successful_auth(self, registry, manager):
        await registry.add(NodeRecord(
            node_id="worker-1",
            instance_id="i-123",
            platform="aliyun",
            auth_token="secret-token",
        ))

        auth_msg = AuthMessage(token="secret-token", worker_id="worker-1")
        ws = FakeWebSocket(incoming=[auth_msg.model_dump_json()])

        worker_id = await manager._authenticate(ws)

        assert worker_id == "worker-1"
        result = json.loads(ws.sent[0])
        assert result["success"] is True
        assert result["worker_id"] == "worker-1"

    @pytest.mark.asyncio
    async def test_invalid_token_rejected(self, registry, manager):
        await registry.add(NodeRecord(
            node_id="worker-1",
            instance_id="i-123",
            platform="aliyun",
            auth_token="correct-token",
        ))

        auth_msg = AuthMessage(token="wrong-token")
        ws = FakeWebSocket(incoming=[auth_msg.model_dump_json()])

        worker_id = await manager._authenticate(ws)

        assert worker_id is None
        result = json.loads(ws.sent[0])
        assert result["success"] is False

    @pytest.mark.asyncio
    async def test_wrong_worker_id_rejected(self, registry, manager):
        await registry.add(NodeRecord(
            node_id="worker-1",
            instance_id="i-123",
            platform="aliyun",
            auth_token="secret-token",
        ))

        auth_msg = AuthMessage(token="secret-token", worker_id="worker-999")
        ws = FakeWebSocket(incoming=[auth_msg.model_dump_json()])

        worker_id = await manager._authenticate(ws)

        assert worker_id is None

    @pytest.mark.asyncio
    async def test_non_auth_message_rejected(self, registry, manager):
        hb_msg = HeartbeatMessage(uptime_seconds=0)
        ws = FakeWebSocket(incoming=[hb_msg.model_dump_json()])

        worker_id = await manager._authenticate(ws)
        assert worker_id is None

    @pytest.mark.asyncio
    async def test_invalid_json_rejected(self, registry, manager):
        ws = FakeWebSocket(incoming=["not valid json"])

        worker_id = await manager._authenticate(ws)
        assert worker_id is None


class TestConnectionManager:
    @pytest.mark.asyncio
    async def test_connected_workers_empty(self, manager):
        assert manager.connected_workers == []

    @pytest.mark.asyncio
    async def test_is_connected(self, manager):
        assert not manager.is_connected("worker-1")

        ws = FakeWebSocket()
        conn = WorkerConnection("worker-1", ws)
        manager._connections["worker-1"] = conn

        assert manager.is_connected("worker-1")
        assert "worker-1" in manager.connected_workers

    @pytest.mark.asyncio
    async def test_get_connection(self, manager):
        assert manager.get_connection("worker-1") is None

        ws = FakeWebSocket()
        conn = WorkerConnection("worker-1", ws)
        manager._connections["worker-1"] = conn

        assert manager.get_connection("worker-1") is conn

    @pytest.mark.asyncio
    async def test_reliable_worker_event_is_acked_and_deduplicated(self, manager):
        ws = FakeWebSocket()
        conn = WorkerConnection("worker-1", ws)
        received = []

        async def on_message(worker_id, msg):
            received.append((worker_id, msg.task_id))

        manager.on_message = on_message
        event = ProcessExitMessage(task_id="task-1", exit_code=0)

        assert await manager._deliver_worker_message(conn, event) is True
        assert await manager._deliver_worker_message(conn, event) is True

        assert received == [("worker-1", "task-1")]
        acks = [parse_message(raw) for raw in ws.sent]
        assert len(acks) == 2
        assert all(isinstance(ack, EventAckMessage) for ack in acks)
        assert all(ack.event_id == event.event_id for ack in acks)


class TestSendCommand:
    @pytest.mark.asyncio
    async def test_send_to_connected_worker(self, manager):
        ws = FakeWebSocket()
        conn = WorkerConnection("worker-1", ws)
        manager._connections["worker-1"] = conn

        await manager.send_command("worker-1", HeartbeatMessage(uptime_seconds=42))

        assert len(ws.sent) == 1
        parsed = json.loads(ws.sent[0])
        assert parsed["type"] == "HEARTBEAT"

    @pytest.mark.asyncio
    async def test_send_to_disconnected_worker_raises(self, manager):
        with pytest.raises(WorkerNotConnectedError):
            await manager.send_command("worker-99", HeartbeatMessage(uptime_seconds=0))

    @pytest.mark.asyncio
    async def test_execute_command(self, manager):
        ws = FakeWebSocket()
        conn = WorkerConnection("worker-1", ws)
        manager._connections["worker-1"] = conn

        await manager.execute(
            "worker-1",
            task_id="task-1",
            command=["python", "-c", "print('hi')"],
            cwd="/tmp",
            env={"FOO": "bar"},
            timeout=300,
        )

        parsed = json.loads(ws.sent[0])
        assert parsed["type"] == "EXECUTE"
        assert parsed["task_id"] == "task-1"
        assert parsed["command"] == ["python", "-c", "print('hi')"]
        assert parsed["cwd"] == "/tmp"
        assert parsed["env"] == {"FOO": "bar"}
        assert parsed["timeout"] == 300

    @pytest.mark.asyncio
    async def test_stop_process_command(self, manager):
        ws = FakeWebSocket()
        conn = WorkerConnection("worker-1", ws)
        manager._connections["worker-1"] = conn

        await manager.stop_process("worker-1", "task-1", "SIGINT")

        parsed = json.loads(ws.sent[0])
        assert parsed["type"] == "STOP"
        assert parsed["task_id"] == "task-1"
        assert parsed["signal"] == "SIGINT"

    @pytest.mark.asyncio
    async def test_read_file_command(self, manager):
        ws = FakeWebSocket()
        conn = WorkerConnection("worker-1", ws)
        manager._connections["worker-1"] = conn

        await manager.read_file("worker-1", "req-1", "/tmp/data.txt")

        parsed = json.loads(ws.sent[0])
        assert parsed["type"] == "READ_FILE"
        assert parsed["path"] == "/tmp/data.txt"

    @pytest.mark.asyncio
    async def test_upload_file_command(self, manager):
        ws = FakeWebSocket()
        conn = WorkerConnection("worker-1", ws)
        manager._connections["worker-1"] = conn

        await manager.upload_file("worker-1", "/tmp/file.txt", "aGVsbG8=", "0755")

        parsed = json.loads(ws.sent[0])
        assert parsed["type"] == "UPLOAD_FILE"
        assert parsed["path"] == "/tmp/file.txt"
        assert parsed["content_base64"] == "aGVsbG8="
        assert parsed["mode"] == "0755"

    @pytest.mark.asyncio
    async def test_health_check_command(self, manager):
        ws = FakeWebSocket()
        conn = WorkerConnection("worker-1", ws)
        manager._connections["worker-1"] = conn

        await manager.health_check("worker-1")

        parsed = json.loads(ws.sent[0])
        assert parsed["type"] == "HEALTH_CHECK"

    @pytest.mark.asyncio
    async def test_status_message_updates_runtime_ready_cache(self, manager):
        ws = FakeWebSocket([
            StatusMessage(
                cpu=1.0,
                mem=2.0,
                disk=3.0,
                runtime_ready=True,
                claude_cli_ok=True,
                claude_version="2.1.181 (Claude Code)",
            ).model_dump_json()
        ])
        conn = WorkerConnection("worker-1", ws)

        with pytest.raises(RuntimeError, match="coroutine raised StopIteration"):
            await manager._message_loop(conn)

        assert manager.is_worker_runtime_ready("worker-1") is True
        status = manager.get_worker_status("worker-1")
        assert status is not None
        assert status["claude_version"] == "2.1.181 (Claude Code)"

    @pytest.mark.asyncio
    async def test_send_input_command(self, manager):
        ws = FakeWebSocket()
        conn = WorkerConnection("worker-1", ws)
        manager._connections["worker-1"] = conn

        await manager.send_input("worker-1", "task-1", "user message")

        parsed = json.loads(ws.sent[0])
        assert parsed["type"] == "MESSAGE"
        assert parsed["task_id"] == "task-1"
        assert parsed["payload"] == "user message"

    @pytest.mark.asyncio
    async def test_register_sync_mapping_command(self, manager):
        ws = FakeWebSocket()
        conn = WorkerConnection("worker-1", ws)
        manager._connections["worker-1"] = conn

        await manager.register_sync_mapping(
            "worker-1",
            task_id="task-1",
            book_slug="my-book",
            oss_prefix="oss://bucket/tasks/task-1/",
            watch_paths=["/root/.work/my-book/"],
            session_path_hash="abc123",
        )

        parsed = json.loads(ws.sent[0])
        assert parsed["type"] == "REGISTER_SYNC_MAPPING"
        assert parsed["task_id"] == "task-1"
        assert parsed["book_slug"] == "my-book"

    @pytest.mark.asyncio
    async def test_unregister_sync_mapping_command(self, manager):
        ws = FakeWebSocket()
        conn = WorkerConnection("worker-1", ws)
        manager._connections["worker-1"] = conn

        await manager.unregister_sync_mapping("worker-1", "task-1")

        parsed = json.loads(ws.sent[0])
        assert parsed["type"] == "UNREGISTER_SYNC_MAPPING"
        assert parsed["task_id"] == "task-1"


class TestBroadcast:
    @pytest.mark.asyncio
    async def test_broadcast_to_all_workers(self, manager):
        ws1 = FakeWebSocket()
        ws2 = FakeWebSocket()
        manager._connections["w1"] = WorkerConnection("w1", ws1)
        manager._connections["w2"] = WorkerConnection("w2", ws2)

        results = await manager.broadcast(HeartbeatMessage(uptime_seconds=10))

        assert results == {"w1": True, "w2": True}
        assert len(ws1.sent) == 1
        assert len(ws2.sent) == 1


class TestDisconnect:
    @pytest.mark.asyncio
    async def test_disconnect_worker(self, manager):
        ws = FakeWebSocket()
        conn = WorkerConnection("worker-1", ws)
        manager._connections["worker-1"] = conn

        await manager.disconnect_worker("worker-1")

        assert not manager.is_connected("worker-1")
        assert ws.closed

    @pytest.mark.asyncio
    async def test_disconnect_nonexistent_worker(self, manager):
        await manager.disconnect_worker("nonexistent")


class TestVerifyToken:
    @pytest.mark.asyncio
    async def test_token_match_without_claimed_id(self, registry, manager):
        await registry.add(NodeRecord(
            node_id="worker-1",
            instance_id="i-123",
            platform="aliyun",
            auth_token="secret",
        ))

        result = await manager._verify_token("secret", None)
        assert result == "worker-1"

    @pytest.mark.asyncio
    async def test_token_match_with_correct_claimed_id(self, registry, manager):
        await registry.add(NodeRecord(
            node_id="worker-1",
            instance_id="i-123",
            platform="aliyun",
            auth_token="secret",
        ))

        result = await manager._verify_token("secret", "worker-1")
        assert result == "worker-1"

    @pytest.mark.asyncio
    async def test_token_match_wrong_claimed_id(self, registry, manager):
        await registry.add(NodeRecord(
            node_id="worker-1",
            instance_id="i-123",
            platform="aliyun",
            auth_token="secret",
        ))

        result = await manager._verify_token("secret", "worker-2")
        assert result is None

    @pytest.mark.asyncio
    async def test_no_matching_token(self, registry, manager):
        await registry.add(NodeRecord(
            node_id="worker-1",
            instance_id="i-123",
            platform="aliyun",
            auth_token="real-token",
        ))

        result = await manager._verify_token("fake-token", None)
        assert result is None
