"""MockWorker — simulates a Worker Runtime via WebSocket.

T-201: Connects to Manager WS endpoint, authenticates, handles commands,
and sends configurable NDJSON output sequences.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone

from websockets.asyncio.client import connect as ws_connect

logger = logging.getLogger(__name__)


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


DEFAULT_NDJSON_SEQUENCE = [
    '{"type":"system","subtype":"init","model":"claude-opus-4-6","tools":[],"mcp_servers":[]}',
    '{"type":"assistant","subtype":"text","content":"Working on the task..."}',
    '{"type":"result","subtype":"success","session_id":"mock-session-001","cost_usd":0.50,"duration_ms":5000,"num_turns":3}',
]


class MockWorker:
    """Simulates a Worker Runtime that connects to Manager via WebSocket.

    Usage::

        worker = MockWorker("ws://localhost:8000/ws/runtime", token="abc123")
        await worker.connect()

        # Pre-configure output for a specific task
        worker.set_script("task-1", [
            '{"type":"system","model":"claude-opus-4-6"}',
            '{"type":"result","session_id":"s-1","cost_usd":1.0}',
        ])

        # Worker will auto-respond to EXECUTE with LOG events + PROCESS_EXIT
        # ...
        await worker.disconnect()

    Configurable behaviors:
        - script: Pre-recorded NDJSON output per task_id
        - exit_code: Default exit code for PROCESS_EXIT
        - delay: Per-line delay when sending LOG events
        - disconnect_after: Disconnect after N messages received
        - heartbeat_interval: Seconds between HEARTBEAT messages (0 to disable)
    """

    def __init__(
        self,
        manager_url: str,
        token: str,
        *,
        worker_id: str | None = None,
        exit_code: int = 0,
        delay: float = 0.01,
        disconnect_after: int | None = None,
        heartbeat_interval: float = 30.0,
        auto_respond: bool = True,
    ) -> None:
        self.manager_url = manager_url
        self.token = token
        self.worker_id = worker_id
        self.exit_code = exit_code
        self.delay = delay
        self.disconnect_after = disconnect_after
        self.heartbeat_interval = heartbeat_interval
        self.auto_respond = auto_respond

        self._ws = None
        self._scripts: dict[str, list[str]] = {}
        self._exit_codes: dict[str, int] = {}
        self._authenticated = False
        self._assigned_worker_id: str | None = None
        self._heartbeat_task: asyncio.Task | None = None
        self._receive_task: asyncio.Task | None = None
        self._running = False
        self._messages_received: list[dict] = []
        self._messages_sent: list[dict] = []
        self._msg_count = 0

    @property
    def is_connected(self) -> bool:
        return self._ws is not None and self._authenticated

    @property
    def messages_received(self) -> list[dict]:
        return list(self._messages_received)

    @property
    def messages_sent(self) -> list[dict]:
        return list(self._messages_sent)

    def set_script(self, task_id: str, lines: list[str], exit_code: int | None = None) -> None:
        """Set the NDJSON output sequence for a specific task."""
        self._scripts[task_id] = lines
        if exit_code is not None:
            self._exit_codes[task_id] = exit_code

    def set_default_script(self, lines: list[str]) -> None:
        """Set the default NDJSON output for tasks without a specific script."""
        self._scripts["__default__"] = lines

    async def connect(self) -> None:
        """Connect to Manager, authenticate, and start background loops."""
        self._ws = await ws_connect(self.manager_url)
        self._running = True

        auth_msg = {
            "type": "AUTH",
            "token": self.token,
            "worker_id": self.worker_id,
            "timestamp": _utcnow_iso(),
        }
        await self._ws.send(json.dumps(auth_msg))

        raw = await self._ws.recv()
        result = json.loads(raw)
        if result.get("type") == "AUTH_RESULT" and result.get("success"):
            self._authenticated = True
            self._assigned_worker_id = result.get("worker_id")
            logger.info("MockWorker authenticated as %s", self._assigned_worker_id)
        else:
            error = result.get("error", "unknown")
            self._running = False
            await self._ws.close()
            self._ws = None
            raise ConnectionRefusedError(f"MockWorker auth failed: {error}")

        if self.heartbeat_interval > 0:
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        self._receive_task = asyncio.create_task(self._receive_loop())

    async def disconnect(self) -> None:
        """Gracefully disconnect from Manager."""
        self._running = False
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
            self._heartbeat_task = None
        if self._receive_task:
            self._receive_task.cancel()
            try:
                await self._receive_task
            except asyncio.CancelledError:
                pass
            self._receive_task = None
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None
        self._authenticated = False

    async def send_raw(self, data: dict) -> None:
        """Send a raw JSON message to Manager."""
        if self._ws is None:
            raise RuntimeError("Not connected")
        if "timestamp" not in data:
            data["timestamp"] = _utcnow_iso()
        text = json.dumps(data)
        await self._ws.send(text)
        self._messages_sent.append(data)

    async def send_log(self, task_id: str, stream: str, data: str, parsed: dict | None = None) -> None:
        msg: dict = {
            "type": "LOG",
            "task_id": task_id,
            "stream": stream,
            "data": data,
        }
        if parsed is not None:
            msg["parsed"] = parsed
        await self.send_raw(msg)

    async def send_process_exit(self, task_id: str, exit_code: int = 0) -> None:
        await self.send_raw({
            "type": "PROCESS_EXIT",
            "task_id": task_id,
            "exit_code": exit_code,
        })

    async def send_heartbeat(self, uptime: int = 123) -> None:
        await self.send_raw({
            "type": "HEARTBEAT",
            "uptime_seconds": uptime,
        })

    async def send_quota_status(
        self,
        task_id: str,
        account_id: str,
        five_hour_pct: float = 0.0,
        seven_day_pct: float = 0.0,
    ) -> None:
        await self.send_raw({
            "type": "QUOTA_STATUS",
            "task_id": task_id,
            "account_id": account_id,
            "usage_percent": five_hour_pct,
            "five_hour_pct": five_hour_pct,
            "seven_day_pct": seven_day_pct,
            "available": True,
        })

    async def send_credential_login_result(
        self, account_id: str, slot_index: int, success: bool, error: str | None = None
    ) -> None:
        await self.send_raw({
            "type": "CREDENTIAL_LOGIN_RESULT",
            "account_id": account_id,
            "slot_index": slot_index,
            "success": success,
            "error": error,
        })

    async def _heartbeat_loop(self) -> None:
        uptime = 0
        while self._running:
            await asyncio.sleep(self.heartbeat_interval)
            if not self._running:
                break
            uptime += int(self.heartbeat_interval)
            try:
                await self.send_heartbeat(uptime)
            except Exception:
                break

    async def _receive_loop(self) -> None:
        try:
            while self._running and self._ws:
                try:
                    raw = await self._ws.recv()
                except Exception:
                    break

                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    continue

                self._messages_received.append(msg)
                self._msg_count += 1

                if self.disconnect_after and self._msg_count >= self.disconnect_after:
                    logger.info("MockWorker disconnecting after %d messages", self._msg_count)
                    await self.disconnect()
                    return

                if self.auto_respond:
                    msg_type = msg.get("type", "")
                    if msg_type == "EXECUTE":
                        asyncio.create_task(self._handle_execute(msg))
                    elif msg_type == "HEALTH_CHECK":
                        await self.send_raw({
                            "type": "STATUS",
                            "cpu": 25.0,
                            "mem": 40.0,
                            "disk": 30.0,
                            "active_processes": [],
                        })
                    elif msg_type == "CREDENTIAL_LOGIN":
                        account_id = msg.get("credentials", {}).get("account_id", "unknown")
                        slot_index = msg.get("slot_index", 0)
                        await self.send_credential_login_result(account_id, slot_index, True)
        except asyncio.CancelledError:
            pass

    async def _handle_execute(self, msg: dict) -> None:
        """Respond to EXECUTE by sending LOG events and PROCESS_EXIT."""
        task_id = msg.get("task_id", "unknown")
        script = self._scripts.get(task_id) or self._scripts.get("__default__") or DEFAULT_NDJSON_SEQUENCE
        exit_code = self._exit_codes.get(task_id, self.exit_code)

        for line in script:
            if not self._running:
                return
            parsed = None
            try:
                parsed = json.loads(line)
            except (json.JSONDecodeError, TypeError):
                pass
            await self.send_log(task_id, "stdout", line, parsed=parsed)
            if self.delay > 0:
                await asyncio.sleep(self.delay)

        await self.send_process_exit(task_id, exit_code)
