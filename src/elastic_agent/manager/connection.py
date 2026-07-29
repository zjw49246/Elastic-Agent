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
from collections import OrderedDict
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from typing import Any

from starlette.websockets import WebSocket, WebSocketDisconnect

from elastic_agent.core.auth import verify_token_constant_time
from elastic_agent.core.protocols.messages import (
    AuthMessage,
    AuthResultMessage,
    EventAckMessage,
    ExecuteMessage,
    ForceSyncMessage,
    HealthCheckMessage,
    Message,
    ReadFileMessage,
    RegisterSyncMappingMessage,
    SendInputMessage,
    StatusMessage,
    StopMessage,
    UnregisterSyncMappingMessage,
    UploadFileMessage,
    parse_message,
)
from elastic_agent.core.registry import NodeRegistry, NodeStatus

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
        self._worker_status: dict[str, dict[str, Any]] = {}
        self._worker_status_events: dict[str, asyncio.Event] = {}
        self._processed_event_ids: OrderedDict[str, None] = OrderedDict()
        self._inflight_event_ids: dict[str, asyncio.Future[bool]] = {}
        self._processed_event_limit = 10_000

    @property
    def connected_workers(self) -> list[str]:
        return list(self._connections.keys())

    def get_connection(self, worker_id: str) -> WorkerConnection | None:
        return self._connections.get(worker_id)

    def is_connected(self, worker_id: str) -> bool:
        return worker_id in self._connections

    def get_worker_status(self, worker_id: str) -> dict[str, Any] | None:
        return self._worker_status.get(worker_id)

    def is_worker_runtime_ready(self, worker_id: str) -> bool:
        status = self.get_worker_status(worker_id)
        return bool(
            status
            and status.get("runtime_ready", False)
            and status.get("process_inventory_complete", True)
        )

    async def wait_until_worker_ready(self, worker_id: str, timeout: float = 15.0) -> bool:
        """Wait for CLI health and a complete independent-task inventory."""
        if not self.is_connected(worker_id):
            return False

        event = self._worker_status_events.setdefault(worker_id, asyncio.Event())
        event.clear()
        try:
            await self.health_check(worker_id)
        except Exception:
            logger.exception("Failed to request health check from worker %s", worker_id)
            return False

        deadline = asyncio.get_running_loop().time() + timeout
        while True:
            if self.is_worker_runtime_ready(worker_id):
                return True
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                return False
            try:
                await asyncio.wait_for(event.wait(), timeout=remaining)
            except asyncio.TimeoutError:
                return False
            event.clear()

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
        old_conn: WorkerConnection | None = None
        async with self._lock:
            old_conn = self._connections.pop(worker_id, None)
            if old_conn:
                logger.info("Replacing existing connection for worker %s", worker_id)
            self._connections[worker_id] = conn
            self._worker_status_events.setdefault(worker_id, asyncio.Event())

        if old_conn:
            try:
                await old_conn.ws.close()
            except Exception:
                logger.debug("Failed to close stale connection for worker %s", worker_id)

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
            should_notify_disconnect = False
            async with self._lock:
                if self._connections.get(worker_id) is conn:
                    del self._connections[worker_id]
                    self._worker_status.pop(worker_id, None)
                    self._worker_status_events.pop(worker_id, None)
                    should_notify_disconnect = True
                else:
                    logger.info(
                        "Ignoring stale disconnect for replaced worker connection %s",
                        worker_id,
                    )

            if should_notify_disconnect and self.on_disconnect:
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
            current_conn = self._connections.get(conn.worker_id)
            if current_conn is not conn:
                logger.info("Stopping stale message loop for worker %s", conn.worker_id)
                break

            try:
                msg = parse_message(raw)
            except Exception:
                logger.warning("Failed to parse message from worker %s", conn.worker_id)
                continue

            if msg.type == "HEARTBEAT":
                await self._registry.update(conn.worker_id, last_heartbeat=_utcnow())
            if isinstance(msg, StatusMessage):
                self._worker_status[conn.worker_id] = msg.model_dump(mode="json")
                self._worker_status_events.setdefault(conn.worker_id, asyncio.Event()).set()

            delivered = await self._deliver_worker_message(conn, msg)
            event_id = str(getattr(msg, "event_id", "") or "")
            if event_id and not delivered:
                # Workers replay durable events only after reconnecting.  A
                # subscriber failure must therefore end this connection; just
                # continuing to read would strand the unacknowledged event in
                # the worker outbox forever on an otherwise healthy socket.
                logger.warning(
                    "Closing worker connection %s after reliable event %s failed",
                    conn.worker_id,
                    event_id,
                )
                try:
                    await conn.ws.close()
                except Exception:
                    logger.debug(
                        "Failed to close worker %s after reliable event failure",
                        conn.worker_id,
                    )
                break
            if self._connections.get(conn.worker_id) is not conn:
                logger.info(
                    "Stopping closed message loop for worker %s",
                    conn.worker_id,
                )
                break

    async def _ack_reliable_event(
        self, conn: WorkerConnection, event_id: str
    ) -> bool:
        """ACK on the same live connection, or let a reconnect replay it.

        A terminal event handler can intentionally remove and close ``conn``
        while collecting results and destroying its Worker.  Sending on that
        stale socket would raise after the event was already handled.  Keep the
        event in the deduplication set and ACK a later replay instead.  The
        second identity check covers a concurrent close between the first
        check and ``send`` without hiding failures on an active connection.
        """
        if self._connections.get(conn.worker_id) is not conn:
            logger.debug(
                "Skipping reliable event ACK on closed worker connection %s",
                conn.worker_id,
            )
            return False
        try:
            await conn.send(EventAckMessage(event_id=event_id))
        except Exception:
            if self._connections.get(conn.worker_id) is not conn:
                logger.debug(
                    "Worker connection %s closed while sending reliable event ACK",
                    conn.worker_id,
                )
                return False
            raise
        return True

    async def _claim_reliable_event(
        self, event_id: str
    ) -> asyncio.Future[bool] | None:
        """Become the event handler, or wait for the current handler.

        A replacement connection can replay an event while the old connection
        is still inside its handler.  The shared future serializes that window:
        success turns the replay into a duplicate, while failure lets one
        waiter claim the event and retry it.
        """
        while True:
            # No await occurs between the lookup and insertion.  Asyncio runs
            # this as one atomic event-loop segment, so a cancellation cannot
            # leave a half-created owner behind.
            if event_id in self._processed_event_ids:
                self._processed_event_ids.move_to_end(event_id)
                return None
            pending = self._inflight_event_ids.get(event_id)
            if pending is None:
                pending = asyncio.get_running_loop().create_future()
                self._inflight_event_ids[event_id] = pending
                return pending
            if await asyncio.shield(pending):
                return None

    def _finish_reliable_event(
        self,
        event_id: str,
        owner: asyncio.Future[bool],
        *,
        success: bool,
    ) -> None:
        """Publish one handler outcome without introducing a cancel point."""
        if self._inflight_event_ids.get(event_id) is not owner:
            raise RuntimeError(
                f"reliable event {event_id!r} lost its in-flight owner"
            )
        if success:
            self._processed_event_ids[event_id] = None
            self._processed_event_ids.move_to_end(event_id)
            while len(self._processed_event_ids) > self._processed_event_limit:
                self._processed_event_ids.popitem(last=False)
        del self._inflight_event_ids[event_id]
        owner.set_result(success)

    async def _deliver_worker_message(
        self, conn: WorkerConnection, msg: Message
    ) -> bool:
        """Deliver one worker event and ACK reliable events after processing.

        PROCESS_EXIT and RUN_EXHAUSTED are replayed by workers until ACKed.  The
        bounded event-id set prevents a reconnect replay from running terminal
        lifecycle handlers twice while still ACKing duplicates so the worker
        can durably discard them.
        """
        event_id = str(getattr(msg, "event_id", "") or "")
        event_owner: asyncio.Future[bool] | None = None
        if event_id:
            event_owner = await self._claim_reliable_event(event_id)
            if event_owner is None:
                await self._ack_reliable_event(conn, event_id)
                return True

        try:
            if self.on_message:
                await self.on_message(conn.worker_id, msg)
        except Exception:
            logger.exception(
                "on_message handler failed for worker=%s type=%s",
                conn.worker_id,
                msg.type,
            )
            if event_owner is not None:
                self._finish_reliable_event(
                    event_id, event_owner, success=False
                )
            return False
        except BaseException:
            if event_owner is not None:
                self._finish_reliable_event(
                    event_id, event_owner, success=False
                )
            raise

        if event_owner is not None:
            self._finish_reliable_event(
                event_id, event_owner, success=True
            )
        if event_id:
            await self._ack_reliable_event(conn, event_id)
        return True

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
        agent_params: dict | None = None,
        job_id: str | None = None,
        watch_exhaustion: bool = False,
    ) -> None:
        await self.send_command(worker_id, ExecuteMessage(
            task_id=task_id,
            command=command,
            cwd=cwd,
            env=env or {},
            timeout=timeout,
            agent_params=agent_params,
            job_id=job_id,
            watch_exhaustion=watch_exhaustion,
        ))

    async def stop_process(self, worker_id: str, task_id: str, sig: str = "SIGTERM") -> None:
        await self.send_command(worker_id, StopMessage(task_id=task_id, signal=sig))

    async def read_file(self, worker_id: str, request_id: str, path: str, encoding: str = "utf-8") -> None:
        await self.send_command(worker_id, ReadFileMessage(
            request_id=request_id, path=path, encoding=encoding,
        ))

    async def upload_file(
        self,
        worker_id: str,
        path: str,
        content_base64: str,
        mode: str = "0644",
        write_mode: str = "overwrite",
    ) -> None:
        await self.send_command(worker_id, UploadFileMessage(
            path=path, content_base64=content_base64, mode=mode, write_mode=write_mode,
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

    async def force_sync(
        self,
        worker_id: str,
        task_id: str,
        *,
        request_id: str | None = None,
        book_slug: str | None = None,
        cwd: str | None = None,
        oss_prefix: str | None = None,
        watch_paths: list[str] | None = None,
        transient: bool = False,
    ) -> None:
        await self.send_command(worker_id, ForceSyncMessage(
            task_id=task_id,
            request_id=request_id,
            book_slug=book_slug,
            cwd=cwd,
            oss_prefix=oss_prefix,
            watch_paths=watch_paths,
            transient=transient,
        ))

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
        async with self._lock:
            conn = self._connections.pop(worker_id, None)
            self._worker_status.pop(worker_id, None)
            status_event = self._worker_status_events.pop(worker_id, None)
            if status_event is not None:
                status_event.set()
        if conn:
            try:
                await conn.ws.close()
            except Exception:
                pass


class WorkerNotConnectedError(Exception):
    pass
