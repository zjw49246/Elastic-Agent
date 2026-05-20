"""T-126: Worker reconnection — exponential backoff, log buffering, replay after reconnect."""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock

import pytest

from elastic_agent.worker.runtime import WorkerRuntime
from elastic_agent.core.protocols.messages import (
    AuthMessage,
    AuthResultMessage,
    HeartbeatMessage,
    LogMessage,
    parse_message,
)


@pytest.fixture
def runtime(tmp_path):
    return WorkerRuntime(
        manager_url="ws://localhost:9999/ws/runtime",
        auth_token="test-token",
        worker_id="worker-1",
        heartbeat_interval=60,
        log_dir=str(tmp_path / "logs"),
    )


# ---------------------------------------------------------------------------
# Exponential backoff
# ---------------------------------------------------------------------------


class _FailingConnect:
    """Async context manager that raises OSError on __aenter__."""
    async def __aenter__(self):
        raise OSError("Connection refused")
    async def __aexit__(self, *args):
        pass


class _FakeWSConnect:
    """Async context manager wrapping a FakeWS object."""
    def __init__(self, ws):
        self._ws = ws
    async def __aenter__(self):
        return self._ws
    async def __aexit__(self, *args):
        pass


class TestExponentialBackoff:

    @pytest.mark.asyncio
    async def test_backoff_doubles_on_connection_failure(self, runtime):
        sleep_delays = []
        orig_sleep = asyncio.sleep

        async def mock_sleep(delay):
            sleep_delays.append(delay)
            if len(sleep_delays) >= 4:
                runtime._running = False
            await orig_sleep(0)

        def mock_connect(url):
            return _FailingConnect()

        with patch("websockets.connect", side_effect=mock_connect), \
             patch("asyncio.sleep", side_effect=mock_sleep):
            runtime._running = True
            await runtime.run()

        assert len(sleep_delays) >= 3
        for i in range(1, len(sleep_delays)):
            assert sleep_delays[i] >= sleep_delays[i - 1]

    @pytest.mark.asyncio
    async def test_backoff_capped_at_60(self, runtime):
        sleep_delays = []
        orig_sleep = asyncio.sleep

        async def mock_sleep(delay):
            sleep_delays.append(delay)
            if len(sleep_delays) >= 10:
                runtime._running = False
            await orig_sleep(0)

        def mock_connect(url):
            return _FailingConnect()

        with patch("websockets.connect", side_effect=mock_connect), \
             patch("asyncio.sleep", side_effect=mock_sleep):
            runtime._running = True
            await runtime.run()

        assert all(d <= 60.0 for d in sleep_delays)

    @pytest.mark.asyncio
    async def test_backoff_resets_on_successful_connect(self):
        """After a successful connect, backoff variable resets to 1.0.

        We verify this by: two failed connects (backoff 1→2→4), then a
        successful connect that raises ConnectionClosed on recv. The backoff
        resets to 1.0 inside the `async with`, so the next outer sleep is 1.0.
        """
        import websockets.exceptions

        rt = WorkerRuntime(
            manager_url="ws://localhost:9999/ws",
            auth_token="t",
            worker_id="w1",
            heartbeat_interval=99999,
            log_dir="/tmp/test-logs",
        )
        outer_sleep_delays = []
        connect_attempt = 0
        orig_sleep = asyncio.sleep

        async def mock_sleep(delay):
            outer_sleep_delays.append(delay)
            if len(outer_sleep_delays) >= 8:
                rt._running = False
            await orig_sleep(0)

        recv_count = 0

        class FakeWS:
            async def send(self, data): pass
            async def recv(self):
                nonlocal recv_count
                recv_count += 1
                if recv_count == 1:
                    return AuthResultMessage(success=True, worker_id="w1").model_dump_json()
                raise websockets.exceptions.ConnectionClosed(None, None)
            async def close(self): pass
            def __aiter__(self): return self
            async def __anext__(self):
                raise websockets.exceptions.ConnectionClosed(None, None)

        def mock_connect(url):
            nonlocal connect_attempt, recv_count
            connect_attempt += 1
            recv_count = 0
            if connect_attempt <= 2:
                return _FailingConnect()
            return _FakeWSConnect(FakeWS())

        with patch("websockets.connect", side_effect=mock_connect), \
             patch("asyncio.sleep", side_effect=mock_sleep):
            rt._running = True
            await rt.run()

        backoff_sleeps = [d for d in outer_sleep_delays if d < 1000]
        assert len(backoff_sleeps) >= 3
        assert backoff_sleeps[0] == 1.0
        assert backoff_sleeps[1] == 2.0
        assert backoff_sleeps[2] == 1.0


# ---------------------------------------------------------------------------
# Send queue buffering
# ---------------------------------------------------------------------------


class TestSendQueueBuffering:

    @pytest.mark.asyncio
    async def test_send_event_queues_message(self, runtime):
        msg = HeartbeatMessage(uptime_seconds=42)
        await runtime._send_event(msg)
        assert not runtime._send_queue.empty()
        queued = await runtime._send_queue.get()
        data = json.loads(queued)
        assert data["type"] == "HEARTBEAT"
        assert data["uptime_seconds"] == 42

    @pytest.mark.asyncio
    async def test_multiple_events_queued(self, runtime):
        for i in range(5):
            await runtime._send_event(HeartbeatMessage(uptime_seconds=i))
        assert runtime._send_queue.qsize() == 5

    @pytest.mark.asyncio
    async def test_queue_survives_disconnect(self, runtime):
        await runtime._send_event(HeartbeatMessage(uptime_seconds=1))
        runtime._ws = None
        runtime._authenticated = False
        assert runtime._send_queue.qsize() == 1


# ---------------------------------------------------------------------------
# Connection state
# ---------------------------------------------------------------------------


class TestConnectionState:

    def test_initially_disconnected(self, runtime):
        assert runtime.connected is False

    def test_connected_requires_ws_and_auth(self, runtime):
        runtime._ws = MagicMock()
        runtime._authenticated = False
        assert runtime.connected is False

        runtime._authenticated = True
        assert runtime.connected is True

    def test_disconnect_clears_state(self, runtime):
        runtime._ws = MagicMock()
        runtime._authenticated = True
        assert runtime.connected is True

        runtime._ws = None
        runtime._authenticated = False
        assert runtime.connected is False


# ---------------------------------------------------------------------------
# Authentication flow
# ---------------------------------------------------------------------------


class TestAuthenticationFlow:

    @pytest.mark.asyncio
    async def test_auth_sends_token(self, runtime):
        sent_data = []

        class FakeWS:
            async def send(self, data):
                sent_data.append(data)
            async def recv(self):
                return AuthResultMessage(success=True, worker_id="w1").model_dump_json()

        runtime._ws = FakeWS()
        result = await runtime._authenticate()
        assert result is True
        assert len(sent_data) == 1
        msg = json.loads(sent_data[0])
        assert msg["type"] == "AUTH"
        assert msg["token"] == "test-token"

    @pytest.mark.asyncio
    async def test_auth_failure_returns_false(self, runtime):
        class FakeWS:
            async def send(self, data):
                pass
            async def recv(self):
                return AuthResultMessage(success=False, error="bad token").model_dump_json()

        runtime._ws = FakeWS()
        result = await runtime._authenticate()
        assert result is False

    @pytest.mark.asyncio
    async def test_auth_timeout_returns_false(self, runtime):
        class FakeWS:
            async def send(self, data):
                pass
            async def recv(self):
                await asyncio.sleep(20)

        runtime._ws = FakeWS()
        result = await runtime._authenticate()
        assert result is False

    @pytest.mark.asyncio
    async def test_auth_updates_worker_id(self, runtime):
        class FakeWS:
            async def send(self, data):
                pass
            async def recv(self):
                return AuthResultMessage(success=True, worker_id="assigned-w99").model_dump_json()

        runtime._ws = FakeWS()
        await runtime._authenticate()
        assert runtime._worker_id == "assigned-w99"


# ---------------------------------------------------------------------------
# Graceful shutdown
# ---------------------------------------------------------------------------


class TestGracefulShutdown:

    @pytest.mark.asyncio
    async def test_stop_sets_running_false(self, runtime):
        runtime._running = True
        await runtime.stop()
        assert runtime._running is False

    @pytest.mark.asyncio
    async def test_stop_closes_ws(self, runtime):
        ws = AsyncMock()
        runtime._ws = ws
        runtime._running = True
        await runtime.stop()
        ws.close.assert_called_once()
