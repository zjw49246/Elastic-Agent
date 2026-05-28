"""Manager-side Worker connection management — WebSocket server + per-worker client API.

T-008: Worker Runtime client (Manager side).

Responsibilities:
- Accept incoming WebSocket connections from Workers
- Authenticate Workers using per-Worker Bearer tokens via NodeRegistry
- Maintain a mapping of worker_id → active WebSocket connection
- Provide high-level API for sending commands and routing events
- Track connection state (connected/disconnected/authenticating)
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Callable, Awaitable
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

from starlette.websockets import WebSocket, WebSocketDisconnect

from elastic_agent.core.auth import verify_token_constant_time
from elastic_agent.core.protocols.messages import (
    AuthMessage,
    AuthResultMessage,
    ExecuteMessage,
    HealthCheckMessage,
    Message,
    ReadFileMessage,
    ForceSyncMessage,
    RegisterSyncMappingMessage,
    SendInputMessage,
    StopMessage,
    UnregisterSyncMappingMessage,
    UploadFileMessage,
    WatchFilesMessage,
    UnwatchMessage,
    parse_message,
)
from elastic_agent.core.registry import NodeRecord, NodeRegistry, NodeStatus

logger = logging.getLogger(__name__)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class WorkerConnection:
    """Represents a single authenticated Worker WebSocket connection."""

    def __init__(self, worker_id: str, ws: WebSocket) -> None:
        self.worker_id = worker_id
        self.ws = ws
        self.connected_at = _utcnow()
        self.last_message_at = _utcnow()
        self._send_lock = asyncio.Lock()

    async def send(self, msg: Message) -> None:
        async with self._send_lock:
            await self.ws.send_text(msg.model_dump_json())
        self.last_message_at = _utcnow()

    async def send_json(self, data: str) -> None:
        async with self._send_lock:
            await self.ws.send_text(data)
        self.last_message_at = _utcnow()


MessageHandler = Callable[[str, Message], Awaitable[None]]


class WorkerConnectionManager:
    """Manages all Worker WebSocket connections on the Manager side.

    Usage:
        manager = WorkerConnectionManager(registry)
        manager.on_message = my_handler  # called for every Worker event

        # In FastAPI:
        @app.websocket("/ws/runtime")
        async def ws_endpoint(websocket: WebSocket):
            await manager.handle_connection(websocket)

        # Send commands:
        await manager.send_command(worker_id, ExecuteMessage(...))
    """

    def __init__(self, registry: NodeRegistry) -> None:
        self._registry = registry
        self._connections: dict[str, WorkerConnection] = {}
        self._lock = asyncio.Lock()
        self.on_message: MessageHandler | None = None
        self.on_connect: Callable[[str], Awaitable[None]] | None = None
        self.on_disconnect: Callable[[str], Awaitable[None]] | None = None

    @property
    def connected_workers(self) -> list[str]:
        return list(self._connections.keys())

    def get_connection(self, worker_id: str) -> WorkerConnection | None:
        return self._connections.get(worker_id)

    def is_connected(self, worker_id: str) -> bool:
        return worker_id in self._connections

    async def handle_connection(self, ws: WebSocket) -> None:
        """Handle an incoming WebSocket connection from a Worker.

        This coroutine runs for the lifetime of the connection:
        1. Accept the WS upgrade
        2. Wait for AUTH message
        3. Validate token against NodeRegistry
        4. Enter message loop until disconnect
        """
        await ws.accept()

        worker_id = await self._authenticate(ws)
        if worker_id is None:
            return

        conn = WorkerConnection(worker_id, ws)
        async with self._lock:
            old = self._connections.pop(worker_id, None)
            if old:
                logger.info("Replacing existing connection for worker %s", worker_id)
            self._connections[worker_id] = conn

        await self._registry.update(
            worker_id,
            status=NodeStatus.READY,
            connected_at=_utcnow(),
            last_heartbeat=_utcnow(),
        )

        if self.on_connect:
            try:
                await self.on_connect(worker_id)
            except Exception:
                logger.exception("on_connect callback failed for %s", worker_id)

        logger.info("Worker %s connected", worker_id)

        try:
            await self._message_loop(conn)
        except WebSocketDisconnect:
            logger.info("Worker %s disconnected", worker_id)
        except Exception:
            logger.exception("Error in message loop for worker %s", worker_id)
        finally:
            async with self._lock:
                if self._connections.get(worker_id) is conn:
                    del self._connections[worker_id]

            if self.on_disconnect:
                try:
                    await self.on_disconnect(worker_id)
                except Exception:
                    logger.exception("on_disconnect callback failed for %s", worker_id)

    async def _authenticate(self, ws: WebSocket) -> str | None:
        try:
            raw = await asyncio.wait_for(ws.receive_text(), timeout=10.0)
        except asyncio.TimeoutError:
            logger.warning("Auth timeout from %s", ws.client)
            await self._reject(ws, "Authentication timeout")
            return None
        except WebSocketDisconnect:
            return None

        try:
            msg = parse_message(raw)
        except Exception:
            await self._reject(ws, "Invalid auth message")
            return None

        if not isinstance(msg, AuthMessage):
            await self._reject(ws, f"Expected AUTH message, got {msg.type}")
            return None

        worker_id = await self._verify_token(msg.token, msg.worker_id)
        if worker_id is None:
            await self._reject(ws, "Invalid token")
            return None

        result = AuthResultMessage(success=True, worker_id=worker_id)
        await ws.send_text(result.model_dump_json())
        return worker_id

    async def _verify_token(self, token: str, claimed_worker_id: str | None) -> str | None:
        nodes = await self._registry.list_all()
        for node in nodes:
            if node.auth_token and verify_token_constant_time(token, node.auth_token):
                if claimed_worker_id and claimed_worker_id != node.node_id:
                    continue
                return node.node_id
        return None

    @staticmethod
    async def _reject(ws: WebSocket, error: str) -> None:
        try:
            result = AuthResultMessage(success=False, error=error)
            await ws.send_text(result.model_dump_json())
            await ws.close()
        except Exception:
            pass

    async def _message_loop(self, conn: WorkerConnection) -> None:
        while True:
            raw = await conn.ws.receive_text()
            conn.last_message_at = _utcnow()

            try:
                msg = parse_message(raw)
            except Exception:
                logger.warning("Failed to parse message from worker %s", conn.worker_id)
                continue

            if msg.type == "HEARTBEAT":
                await self._registry.update(conn.worker_id, last_heartbeat=_utcnow())

            if self.on_message:
                try:
                    await self.on_message(conn.worker_id, msg)
                except Exception:
                    logger.exception(
                        "on_message handler failed for worker=%s type=%s",
                        conn.worker_id,
                        msg.type,
                    )

    # ---- High-level command API ----

    async def send_command(self, worker_id: str, msg: Message) -> None:
        """Send a command message to a specific Worker."""
        conn = self._connections.get(worker_id)
        if conn is None:
            raise WorkerNotConnectedError(f"Worker {worker_id} is not connected")
        await conn.send(msg)

    async def execute(
        self,
        worker_id: str,
        task_id: str,
        command: list[str],
        cwd: str = ".",
        env: dict[str, str] | None = None,
        timeout: int | None = None,
    ) -> None:
        await self.send_command(worker_id, ExecuteMessage(
            task_id=task_id,
            command=command,
            cwd=cwd,
            env=env or {},
            timeout=timeout,
        ))

    async def stop_process(self, worker_id: str, task_id: str, sig: str = "SIGTERM") -> None:
        await self.send_command(worker_id, StopMessage(task_id=task_id, signal=sig))

    async def read_file(self, worker_id: str, request_id: str, path: str, encoding: str = "utf-8") -> None:
        await self.send_command(worker_id, ReadFileMessage(
            request_id=request_id, path=path, encoding=encoding,
        ))

    async def upload_file(
        self, worker_id: str, path: str, content_base64: str, mode: str = "0644"
    ) -> None:
        await self.send_command(worker_id, UploadFileMessage(
            path=path, content_base64=content_base64, mode=mode,
        ))

    async def health_check(self, worker_id: str) -> None:
        await self.send_command(worker_id, HealthCheckMessage())

    async def send_input(self, worker_id: str, task_id: str, payload: str) -> None:
        await self.send_command(worker_id, SendInputMessage(task_id=task_id, payload=payload))

    async def register_sync_mapping(
        self,
        worker_id: str,
        task_id: str,
        book_slug: str,
        oss_prefix: str,
        watch_paths: list[str],
        session_path_hash: str = "",
    ) -> None:
        await self.send_command(worker_id, RegisterSyncMappingMessage(
            task_id=task_id,
            book_slug=book_slug,
            oss_prefix=oss_prefix,
            watch_paths=watch_paths,
            session_path_hash=session_path_hash,
        ))

    async def unregister_sync_mapping(self, worker_id: str, task_id: str) -> None:
        await self.send_command(worker_id, UnregisterSyncMappingMessage(task_id=task_id))

    async def force_sync(self, worker_id: str, task_id: str) -> None:
        await self.send_command(worker_id, ForceSyncMessage(task_id=task_id))

    async def broadcast(self, msg: Message) -> dict[str, bool]:
        """Send a message to all connected Workers. Returns {worker_id: success}."""
        results: dict[str, bool] = {}
        for worker_id, conn in list(self._connections.items()):
            try:
                await conn.send(msg)
                results[worker_id] = True
            except Exception:
                results[worker_id] = False
        return results

    async def disconnect_worker(self, worker_id: str) -> None:
        conn = self._connections.pop(worker_id, None)
        if conn:
            try:
                await conn.ws.close()
            except Exception:
                pass


class WorkerNotConnectedError(Exception):
    pass
