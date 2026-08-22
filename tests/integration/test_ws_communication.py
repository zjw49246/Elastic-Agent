"""T-110: Manager ↔ Worker WS communication integration tests.

Tests: connection, authentication, bidirectional messaging, disconnection/reconnection.
"""

from __future__ import annotations

import asyncio
import json

import pytest

from elastic_agent.core.registry import NodeStatus
from elastic_agent.testing import MockWorker

from .conftest import connect_mock_worker


@pytest.mark.level1
class TestWSConnection:
    """WebSocket connection and authentication."""

    @pytest.mark.asyncio
    async def test_worker_connects_and_authenticates(self, running_server):
        srv = running_server
        nodes = await srv["manager"].scale_out(count=1)
        node = nodes[0]

        worker = await connect_mock_worker(srv["ws_url"], node.auth_token, node.node_id)
        try:
            assert worker.is_connected
            assert srv["manager"].connection_manager.is_connected(node.node_id)
            status = await srv["manager"].registry.get(node.node_id)
            assert status.status == NodeStatus.READY
        finally:
            await worker.disconnect()

    @pytest.mark.asyncio
    async def test_invalid_token_rejected(self, running_server):
        srv = running_server
        await srv["manager"].scale_out(count=1)

        with pytest.raises(ConnectionRefusedError, match="auth failed"):
            await connect_mock_worker(srv["ws_url"], "invalid-token-xxx")

    @pytest.mark.asyncio
    async def test_multiple_workers_connect(self, running_server):
        srv = running_server
        nodes = await srv["manager"].scale_out(count=3)
        workers = []
        try:
            for node in nodes:
                w = await connect_mock_worker(srv["ws_url"], node.auth_token, node.node_id)
                workers.append(w)

            connected = srv["manager"].connection_manager.connected_workers
            assert len(connected) == 3
            for node in nodes:
                assert node.node_id in connected
        finally:
            for w in workers:
                await w.disconnect()


@pytest.mark.level1
class TestBidirectionalMessaging:
    """Bidirectional message exchange over WebSocket."""

    @pytest.mark.asyncio
    async def test_execute_command_sends_logs_and_exit(self, running_server):
        srv = running_server
        nodes = await srv["manager"].scale_out(count=1)
        node = nodes[0]

        events_received: list[dict] = []
        sub_id = srv["manager"].event_bus.subscribe(
            "*", lambda et, wid, d: events_received.append({"type": et, "worker": wid, **d})
        )

        worker = await connect_mock_worker(srv["ws_url"], node.auth_token, node.node_id)
        worker.set_script("task-exec-1", [
            '{"type":"system","subtype":"init","model":"claude-opus-4-6"}',
            '{"type":"result","session_id":"s-exec-1","cost_usd":1.0}',
        ])

        try:
            await srv["manager"].connection_manager.execute(
                worker_id=node.node_id,
                task_id="task-exec-1",
                command=["echo", "hello"],
            )
            await asyncio.sleep(0.3)

            log_events = [e for e in events_received if e["type"] == "LOG"]
            exit_events = [e for e in events_received if e["type"] == "PROCESS_EXIT"]
            assert len(log_events) >= 2
            assert len(exit_events) == 1
            assert exit_events[0].get("exit_code") == 0
        finally:
            srv["manager"].event_bus.unsubscribe(sub_id)
            await worker.disconnect()

    @pytest.mark.asyncio
    async def test_health_check_returns_status(self, running_server):
        srv = running_server
        nodes = await srv["manager"].scale_out(count=1)
        node = nodes[0]

        events: list[dict] = []
        sub_id = srv["manager"].event_bus.subscribe(
            "STATUS", lambda et, wid, d: events.append(d)
        )

        worker = await connect_mock_worker(srv["ws_url"], node.auth_token, node.node_id)
        try:
            await srv["manager"].connection_manager.health_check(node.node_id)
            await asyncio.sleep(0.2)

            assert len(events) == 1
            assert "cpu" in events[0]
            assert "mem" in events[0]
        finally:
            srv["manager"].event_bus.unsubscribe(sub_id)
            await worker.disconnect()

    @pytest.mark.asyncio
    async def test_heartbeat_updates_registry(self, running_server):
        srv = running_server
        nodes = await srv["manager"].scale_out(count=1)
        node = nodes[0]

        worker = await connect_mock_worker(srv["ws_url"], node.auth_token, node.node_id)
        try:
            before = await srv["manager"].registry.get(node.node_id)
            old_hb = before.last_heartbeat

            await worker.send_heartbeat(uptime=42)
            await asyncio.sleep(0.2)

            after = await srv["manager"].registry.get(node.node_id)
            assert after.last_heartbeat >= old_hb
        finally:
            await worker.disconnect()

    @pytest.mark.asyncio
    async def test_worker_sends_custom_message(self, running_server):
        srv = running_server
        nodes = await srv["manager"].scale_out(count=1)
        node = nodes[0]

        quota_events: list[dict] = []
        sub_id = srv["manager"].event_bus.subscribe(
            "QUOTA_STATUS", lambda et, wid, d: quota_events.append(d)
        )

        worker = await connect_mock_worker(srv["ws_url"], node.auth_token, node.node_id)
        try:
            await worker.send_quota_status("task-q1", "acct-1", 50.0, 20.0)
            await asyncio.sleep(0.2)

            assert len(quota_events) == 1
            assert quota_events[0]["account_id"] == "acct-1"
            assert quota_events[0]["five_hour_pct"] == 50.0
        finally:
            srv["manager"].event_bus.unsubscribe(sub_id)
            await worker.disconnect()


@pytest.mark.level1
class TestDisconnectionAndReconnection:
    """Worker disconnect and reconnect scenarios."""

    @pytest.mark.asyncio
    async def test_disconnect_fires_event_and_updates_state(self, running_server):
        srv = running_server
        nodes = await srv["manager"].scale_out(count=1)
        node = nodes[0]

        disconnect_events: list[str] = []
        sub_id = srv["manager"].event_bus.subscribe(
            "WORKER_DISCONNECTED", lambda et, wid, d: disconnect_events.append(wid)
        )

        worker = await connect_mock_worker(srv["ws_url"], node.auth_token, node.node_id)
        assert srv["manager"].connection_manager.is_connected(node.node_id)

        await worker.disconnect()
        await asyncio.sleep(0.3)

        assert not srv["manager"].connection_manager.is_connected(node.node_id)
        assert node.node_id in disconnect_events

        srv["manager"].event_bus.unsubscribe(sub_id)

    @pytest.mark.asyncio
    async def test_worker_reconnects_after_disconnect(self, running_server):
        srv = running_server
        nodes = await srv["manager"].scale_out(count=1)
        node = nodes[0]

        worker1 = await connect_mock_worker(srv["ws_url"], node.auth_token, node.node_id)
        await worker1.disconnect()
        await asyncio.sleep(0.2)

        assert not srv["manager"].connection_manager.is_connected(node.node_id)

        worker2 = await connect_mock_worker(srv["ws_url"], node.auth_token, node.node_id)
        try:
            assert worker2.is_connected
            assert srv["manager"].connection_manager.is_connected(node.node_id)
        finally:
            await worker2.disconnect()

    @pytest.mark.asyncio
    async def test_disconnect_cleans_up_tasks(self, running_server):
        srv = running_server
        nodes = await srv["manager"].scale_out(count=1)
        node = nodes[0]

        await srv["manager"].task_registry.register("task-dc-1", node.node_id, {"test": True})

        worker = await connect_mock_worker(srv["ws_url"], node.auth_token, node.node_id)
        await worker.disconnect()
        await asyncio.sleep(0.3)

        task = await srv["manager"].task_registry.get("task-dc-1")
        assert task.status.value == "worker_lost"

    @pytest.mark.asyncio
    async def test_replace_existing_connection(self, running_server):
        srv = running_server
        nodes = await srv["manager"].scale_out(count=1)
        node = nodes[0]

        worker1 = await connect_mock_worker(srv["ws_url"], node.auth_token, node.node_id)
        worker2 = await connect_mock_worker(srv["ws_url"], node.auth_token, node.node_id)
        try:
            assert srv["manager"].connection_manager.is_connected(node.node_id)
        finally:
            await worker2.disconnect()
            try:
                await worker1.disconnect()
            except Exception:
                pass

    @pytest.mark.asyncio
    async def test_auto_disconnect_after_n_messages(self, running_server):
        srv = running_server
        nodes = await srv["manager"].scale_out(count=1)
        node = nodes[0]

        worker = await connect_mock_worker(
            srv["ws_url"], node.auth_token, node.node_id,
            disconnect_after=2,
        )

        await srv["manager"].connection_manager.health_check(node.node_id)
        await srv["manager"].connection_manager.health_check(node.node_id)
        await asyncio.sleep(0.5)

        assert not worker.is_connected
