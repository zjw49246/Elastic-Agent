"""Worker Runtime server — runs on each Worker, connects to Manager via WebSocket.

Responsibilities:
- Reverse WebSocket connection to Manager (Worker initiates)
- Subprocess execution with stdout/stderr dual-write (WS + local log file)
- File read, file watch (inotify), file upload
- Heartbeat, health status reporting
- Exponential-backoff reconnection on disconnect
"""

from __future__ import annotations

import asyncio
import codecs
import json
import logging
import os
import platform
import re
import shlex
import shutil
import signal
import subprocess
import time
import uuid
from collections import deque
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import websockets
import websockets.exceptions

from elastic_agent.core.protocols.messages import (
    AccountLoginCancelledMessage,
    AccountLoginCancelMessage,
    AccountLoginMessage,
    AccountLoginOtpMessage,
    AccountLoginOtpRequiredMessage,
    AccountLoginResultMessage,
    AgentApiConfigureMessage,
    AgentApiConfigureResultMessage,
    AuthMessage,
    AuthResultMessage,
    CredentialLoginMessage,
    CredentialLoginResultMessage,
    ErrorMessage,
    EventAckMessage,
    ExecuteMessage,
    FileChangeMessage,
    FileContentMessage,
    FileSyncedMessage,
    HealthCheckMessage,
    HeartbeatMessage,
    LogMessage,
    Message,
    ProcessExitMessage,
    QuotaStatusMessage,
    ReadFileMessage,
    RegisterSyncMappingMessage,
    RunExhaustedMessage,
    SendInputMessage,
    StatusMessage,
    StopMessage,
    UnregisterSyncMappingMessage,
    UnwatchMessage,
    UploadFileMessage,
    WatchFilesMessage,
    parse_message,
)
from elastic_agent.core.rate_limit import (
    is_apexrouter_auth_failure,
    is_apexrouter_hard_limit,
    is_apexrouter_transient,
    is_auth_failure,
    is_cloudrouter_auth_failure,
    is_cloudrouter_hard_limit,
    is_cloudrouter_transient,
    is_rate_limited,
)
from elastic_agent.core.secure_store import (
    atomic_write_private,
    secure_state_directory,
    tighten_state_file,
)

logger = logging.getLogger(__name__)

# After the run process exits, how long to keep draining stdout/stderr before
# giving up — a lingering child (e.g. a docker container from `--sandbox os`)
# can hold the pipe open so it never EOFs. Bounded so the exit is always reported.
_EXIT_DRAIN_TIMEOUT = 10.0
# Physical stdout/stderr lines are not trusted.  StreamReader.readline() stops
# draining after its internal 64 KiB limit; split larger records into bounded
# frames so a child can never deadlock on a full pipe.
_MAX_LOG_FRAME_BYTES = 64 * 1024
# Local NDJSON remains authoritative during a slow/disconnected Manager link.
# Bound best-effort transports by both entries and serialized wire bytes;
# lifecycle/control messages keep their existing reliable path.
_MAX_PENDING_CONTROL_FRAMES = 256
_MAX_CONTROL_TRANSPORT_FRAME_BYTES = 1024 * 1024
_MAX_PENDING_CONTROL_BYTES = 8 * 1024 * 1024
_MAX_PENDING_LOG_FRAMES = 256
_MAX_LOG_TRANSPORT_FRAME_BYTES = 256 * 1024
_MAX_PENDING_LOG_BYTES = 8 * 1024 * 1024
_MAX_PENDING_DATA_FRAMES = 64
_MAX_DATA_TRANSPORT_FRAME_BYTES = 1024 * 1024
_MAX_PENDING_DATA_BYTES = 8 * 1024 * 1024
_MAX_COMPLETED_LOGIN_RECORDS = 256
_SUPERVISED_OFFSETS_VERSION = 1
_MAX_SUPERVISED_OFFSETS = 4096
_AGENT_API_ERROR_PRIORITY = {
    "agent_api_error": 1,
    "agent_api_transient_error": 2,
    "agent_api_rate_limited": 3,
    "agent_api_auth_failure": 4,
}
_AGENT_API_PROVIDER_LABELS = {
    "cloudrouter": "CloudRouter",
    "apex": "ApexRouter",
}


class ReliableEventPersistenceError(RuntimeError):
    """A terminal event could not be made durable on this worker."""


class AccountLoginCleanupError(RuntimeError):
    """Credential/process rollback could not be proven complete."""


class _BoundedFrameQueue:
    """Same-loop FIFO with strict serialized-byte and frame-count budgets."""

    def __init__(
        self,
        *,
        max_frames: int,
        max_bytes: int,
        max_frame_bytes: int,
    ) -> None:
        self._max_frames = max_frames
        self._max_bytes = max_bytes
        self._max_frame_bytes = max_frame_bytes
        self._frames: deque[tuple[str, int]] = deque()
        self._wire_bytes = 0
        self._not_empty = asyncio.Event()

    @property
    def wire_bytes(self) -> int:
        return self._wire_bytes

    @property
    def max_bytes(self) -> int:
        return self._max_bytes

    @property
    def max_frame_bytes(self) -> int:
        return self._max_frame_bytes

    def qsize(self) -> int:
        return len(self._frames)

    def empty(self) -> bool:
        return not self._frames

    def put_latest(
        self,
        data: str,
        *,
        reserved_bytes: int = 0,
        reserved_frames: int = 0,
    ) -> tuple[bool, int, int]:
        """Append newest data, evicting oldest queued frames within the budget.

        Returns ``(accepted, dropped_frames, dropped_bytes)``.  In-flight/retry
        reservations keep the total pending wire footprint strict even while a
        WebSocket send is suspended or has failed.
        """

        frame_bytes = len(data.encode("utf-8"))
        available_bytes = max(0, self._max_bytes - reserved_bytes)
        available_frames = max(0, self._max_frames - reserved_frames)
        if (
            frame_bytes > self._max_frame_bytes
            or frame_bytes > available_bytes
            or available_frames == 0
        ):
            return False, 0, 0

        dropped_frames = 0
        dropped_bytes = 0
        while self._frames and (
            len(self._frames) >= available_frames
            or self._wire_bytes + frame_bytes > available_bytes
        ):
            _, removed_bytes = self._frames.popleft()
            self._wire_bytes -= removed_bytes
            dropped_frames += 1
            dropped_bytes += removed_bytes
        if (
            len(self._frames) >= available_frames
            or self._wire_bytes + frame_bytes > available_bytes
        ):
            return False, dropped_frames, dropped_bytes
        self._frames.append((data, frame_bytes))
        self._wire_bytes += frame_bytes
        self._not_empty.set()
        return True, dropped_frames, dropped_bytes

    def get_frame_nowait(self) -> tuple[str, int]:
        if not self._frames:
            raise asyncio.QueueEmpty
        data, frame_bytes = self._frames.popleft()
        self._wire_bytes -= frame_bytes
        if not self._frames:
            self._not_empty.clear()
        return data, frame_bytes

    def get_nowait(self) -> str:
        data, _ = self.get_frame_nowait()
        return data

    async def put(self, data: str) -> None:
        """Compatibility helper for internal tests and legacy producers.

        Production transport code uses ``put_latest`` with retry/in-flight
        reservations.  This helper remains strictly bounded and raises when a
        single frame cannot fit.
        """

        accepted, _, _ = self.put_latest(data)
        if not accepted:
            raise asyncio.QueueFull

    async def get(self) -> str:
        while True:
            try:
                return self.get_nowait()
            except asyncio.QueueEmpty:
                await self._not_empty.wait()


def _classify_agent_api_provider_error(
    provider: str,
    line: str,
) -> tuple[str, str] | None:
    """Classify a structured failure with provider-specific retry semantics."""

    if provider == "cloudrouter":
        auth_failure = is_cloudrouter_auth_failure(line)
        hard_limit = is_cloudrouter_hard_limit(line)
        transient = is_cloudrouter_transient(line)
    elif provider == "apex":
        auth_failure = is_apexrouter_auth_failure(line)
        hard_limit = is_apexrouter_hard_limit(line)
        transient = is_apexrouter_transient(line)
    else:
        return None

    label = _AGENT_API_PROVIDER_LABELS[provider]
    if auth_failure:
        return (
            "agent_api_auth_failure",
            f"{label} rejected the delegated API key",
        )
    if hard_limit:
        suffix = (
            "key quota or rate limit was reached"
            if provider == "cloudrouter"
            else "key quota or credit limit was reached"
        )
        return ("agent_api_rate_limited", f"{label} {suffix}")
    if transient:
        suffix = (
            "temporarily rate limited the request"
            if provider == "cloudrouter"
            else "request failed transiently"
        )
        return ("agent_api_transient_error", f"{label} {suffix}")
    return None


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class _WorkerLoginOtpReader:
    """Bridge a Codex browser OTP field to a correlated Manager command."""

    def __init__(self, runtime: "WorkerRuntime", message: AccountLoginMessage) -> None:
        self._runtime = runtime
        self._message = message
        self._challenge_id = ""
        self._expires_at = 0
        self._queue: asyncio.Queue[str] = asyncio.Queue(maxsize=1)

    async def read_code(
        self,
        *,
        attempt_id: str,
        timeout_s: int,
        logs: list[str],
    ) -> str:
        if attempt_id != self._message.login_request_id:
            raise RuntimeError("login attempt id does not match worker request")
        self._challenge_id = uuid.uuid4().hex
        self._expires_at = int(time.time() + timeout_s)
        await self._runtime._send_event(AccountLoginOtpRequiredMessage(
            login_request_id=self._message.login_request_id,
            account_id=self._message.account_id,
            challenge_id=self._challenge_id,
            expires_at=self._expires_at,
        ))
        logs.append("Waiting for a user-supplied email verification code")
        try:
            return await asyncio.wait_for(self._queue.get(), timeout=timeout_s)
        except asyncio.TimeoutError as exc:
            # ``codex_login._redacted_error`` deliberately preserves only its
            # own safe exception type.  Use that boundary so the Job reports
            # an actionable OTP timeout instead of the opaque
            # ``automation failed (RuntimeError)``.
            from elastic_agent.worker.login.codex_login import CodexLoginError

            raise CodexLoginError(
                "Timed out waiting for a user-supplied verification code"
            ) from exc
        finally:
            self._challenge_id = ""
            self._expires_at = 0

    def submit(self, message: AccountLoginOtpMessage) -> bool:
        """Accept exactly one live six-digit response without retaining it."""
        if (
            message.login_request_id != self._message.login_request_id
            or message.account_id != self._message.account_id
            or not self._challenge_id
            or message.challenge_id != self._challenge_id
            or self._expires_at <= int(time.time())
            or not re.fullmatch(r"\d{6}", message.code)
            or self._queue.full()
        ):
            return False
        self._queue.put_nowait(message.code)
        return True


class WorkerRuntime:
    """Worker-side runtime that connects to Manager and executes commands."""

    def __init__(
        self,
        manager_url: str,
        auth_token: str,
        worker_id: str | None = None,
        heartbeat_interval: int = 30,
        log_dir: str = "logs",
        task_supervisor_socket: str | None = None,
    ) -> None:
        self._manager_url = manager_url
        self._auth_token = auth_token
        self._worker_id = worker_id
        self._heartbeat_interval = heartbeat_interval
        self._log_dir = Path(log_dir)
        self._event_outbox_path = self._log_dir / "event_outbox.json"
        self._supervised_offsets_path = (
            self._log_dir / "supervised_offsets.json"
        )
        self._event_outbox_lock = asyncio.Lock()
        self._reliable_events = self._load_reliable_events()
        # A WebSocket send can fail after dequeue.  Keep that exact frame ahead
        # of later STATUS/heartbeat frames across reconnects instead of putting
        # it at the tail and reordering lifecycle state.
        self._retry_send: str | None = None
        self._retry_send_kind = "control"
        self._retry_send_bytes = 0
        self._inflight_send_kind: str | None = None
        self._inflight_send_bytes = 0

        self._ws: Any = None
        self._authenticated = False
        self._running = False
        self._start_time = time.monotonic()

        self._processes: dict[str, asyncio.subprocess.Process] = {}
        self._process_tasks: dict[str, asyncio.Task] = {}
        self._stdin_pipes: dict[str, asyncio.StreamWriter | None] = {}
        # Every Mode-B command is a POSIX session leader.  Keep its process
        # group until terminal hand-off so STOP, exhaustion, timeout, and a
        # naturally exiting shell all clean up descendants before credentials
        # can be delegated to another run.
        self._process_groups: dict[str, int] = {}
        self._process_group_locks: dict[str, asyncio.Lock] = {}
        # Mode-B commands can be delegated to the independent
        # ea-task-supervisor service.  Unlike ``_processes``, these children are
        # not in ea-runtime's systemd cgroup and survive a runtime replacement.
        # The new runtime inventories the private socket before advertising a
        # complete STATUS snapshot.
        supervisor_socket = (
            task_supervisor_socket
            if task_supervisor_socket is not None
            else os.environ.get("ELASTIC_AGENT_TASK_SUPERVISOR_SOCKET", "")
        ).strip()
        self._task_supervisor: Any = None
        if supervisor_socket:
            from elastic_agent.worker.task_supervisor import (
                TaskSupervisorClient,
            )

            self._task_supervisor = TaskSupervisorClient(supervisor_socket)
        self._supervised_tasks: dict[str, Any] = {}
        self._supervised_monitor_tasks: dict[str, asyncio.Task] = {}
        self._supervised_offsets = self._load_supervised_offsets()
        self._supervisor_event_task_ids: dict[str, str] = {}
        self._process_inventory_complete = self._task_supervisor is None
        self._process_inventory_error: str | None = None
        # A process is removed from ``_processes`` before potentially slow final
        # file sync and durable PROCESS_EXIT persistence.  Keep that transition
        # visible to STATUS reconciliation so the Manager cannot mistake the
        # narrow hand-off window for a lost terminal event.
        self._exiting_task_ids: set[str] = set()

        # Mode-B exhaustion watch: task_id -> job_id for runs whose opaque
        # command should be scanned for rate-limit banners; _exhaustion_fired
        # dedupes so we signal + interrupt only once per run.
        self._exhaustion_watch: dict[str, str] = {}
        self._exhaustion_fired: set[str] = set()
        # Only tasks launched from a validated managed API projection receive
        # provider-specific environment hardening and semantic error handling.
        self._agent_api_tasks: dict[str, Any] = {}
        self._agent_api_task_errors: dict[str, tuple[str, str]] = {}

        self._send_queue = _BoundedFrameQueue(
            max_frames=_MAX_PENDING_CONTROL_FRAMES,
            max_bytes=_MAX_PENDING_CONTROL_BYTES,
            max_frame_bytes=_MAX_CONTROL_TRANSPORT_FRAME_BYTES,
        )
        self._log_send_queue = _BoundedFrameQueue(
            max_frames=_MAX_PENDING_LOG_FRAMES,
            max_bytes=_MAX_PENDING_LOG_BYTES,
            max_frame_bytes=_MAX_LOG_TRANSPORT_FRAME_BYTES,
        )
        self._data_send_queue = _BoundedFrameQueue(
            max_frames=_MAX_PENDING_DATA_FRAMES,
            max_bytes=_MAX_PENDING_DATA_BYTES,
            max_frame_bytes=_MAX_DATA_TRANSPORT_FRAME_BYTES,
        )
        self._send_queue_ready = asyncio.Event()
        self._dropped_log_frames = 0
        self._truncated_log_frames = 0
        self._dropped_data_frames = 0
        self._dropped_control_frames = 0
        self._prefer_data_frame = True
        self._reconnect_event = asyncio.Event()

        self._file_sync_manager: Any = None
        self._quota_checker: Any = None

        # PTY-hosted execution (claude-pty); created lazily on first use.
        self._pty_backend: Any = None
        self._pty_timeouts: dict[str, asyncio.Task] = {}
        self._pty_session_ids: dict[str, str] = {}

        # ACCOUNT_LOGIN runs in a background task so the receiver can accept a
        # correlated ACCOUNT_LOGIN_OTP while the same Codex browser/PKCE flow
        # remains alive. A lock preserves the old one-login-at-a-time behavior
        # for Chrome profiles and credential directories.
        self._account_login_lock = asyncio.Lock()
        self._account_login_tasks: dict[str, asyncio.Task] = {}
        self._account_login_accounts: dict[str, str] = {}
        self._account_login_otp_readers: dict[str, _WorkerLoginOtpReader] = {}
        # A late ACCOUNT_LOGIN_CANCEL may arrive after the task's done callback
        # removed it.  Retain a bounded exact request/account proof that all
        # transactional cleanup completed before acknowledging it.
        self._account_login_cleanup_confirmed: dict[str, str] = {}
        # A credential update whose rollback cannot be proven must never feed
        # either a warm PTY or a subprocess. Runtime restart clears this set
        # only after all in-memory PTY sessions have also been destroyed.
        self._unsafe_credential_config_dirs: set[str] = set()

    @property
    def connected(self) -> bool:
        return self._ws is not None and self._authenticated

    @property
    def active_processes(self) -> list[str]:
        tasks = list(self._processes.keys())
        tasks.extend(
            task_id
            for task_id, descriptor in self._supervised_tasks.items()
            if descriptor.state == "running"
        )
        if self._pty_backend is not None:
            tasks.extend(self._pty_backend.active_tasks)
        return tasks

    async def run(self) -> None:
        """Main loop: connect, authenticate, handle messages. Reconnect on failure."""
        self._running = True
        self._start_time = time.monotonic()
        backoff = 1.0

        while self._running:
            if (
                self._task_supervisor is not None
                and not self._process_inventory_complete
            ):
                await self._recover_supervised_task_inventory()
            try:
                async with websockets.connect(self._manager_url) as ws:
                    self._ws = ws
                    backoff = 1.0

                    if not await self._authenticate():
                        logger.error("Authentication failed, retrying after backoff")
                        self._ws = None
                        await asyncio.sleep(backoff)
                        backoff = min(backoff * 2, 60.0)
                        continue

                    self._authenticated = True
                    logger.info("Connected and authenticated to Manager at %s", self._manager_url)

                    await self._start_quota_checker()
                    await self._replay_reliable_events()

                    heartbeat_task = asyncio.create_task(self._heartbeat_loop())
                    sender_task = asyncio.create_task(self._sender_loop())
                    receiver_task = asyncio.create_task(self._receiver_loop())
                    await self._send_status()

                    try:
                        done, pending = await asyncio.wait(
                            [heartbeat_task, sender_task, receiver_task],
                            return_when=asyncio.FIRST_COMPLETED,
                        )
                        for t in done:
                            if t.exception():
                                logger.error("Task failed: %s", t.exception())
                    finally:
                        for t in [heartbeat_task, sender_task, receiver_task]:
                            t.cancel()
                            try:
                                await t
                            except asyncio.CancelledError:
                                pass

            except (
                websockets.exceptions.ConnectionClosed,
                websockets.exceptions.WebSocketException,
                OSError,
            ) as exc:
                logger.warning("Connection lost: %s. Reconnecting in %.0fs", exc, backoff)
            except asyncio.CancelledError:
                break
            finally:
                self._ws = None
                self._authenticated = False

            if self._running:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60.0)

    async def stop(self) -> None:
        """Gracefully shut down: stop all processes, close connection."""
        self._running = False
        if self._quota_checker:
            await self._quota_checker.stop()
        if self._file_sync_manager:
            await self._file_sync_manager.stop()
        for task_id in list(self._processes.keys()):
            await self._stop_process(task_id, "SIGTERM")
        # Runtime replacement intentionally detaches from supervised Mode-B
        # tasks. STOP/timeout/exhaustion use explicit supervisor RPCs; merely
        # stopping the reconnectable control plane must never kill the Job.
        supervised_monitors = list(self._supervised_monitor_tasks.values())
        for task in supervised_monitors:
            task.cancel()
        if supervised_monitors:
            await asyncio.gather(
                *supervised_monitors,
                return_exceptions=True,
            )
        self._supervised_monitor_tasks.clear()
        if self._task_supervisor is not None:
            self._process_inventory_complete = False
        login_tasks = list(self._account_login_tasks.values())
        for task in login_tasks:
            task.cancel()
        if login_tasks:
            await asyncio.gather(*login_tasks, return_exceptions=True)
        self._account_login_tasks.clear()
        self._account_login_accounts.clear()
        self._account_login_otp_readers.clear()
        self._account_login_cleanup_confirmed.clear()
        if self._pty_backend is not None:
            for timer in self._pty_timeouts.values():
                timer.cancel()
            self._pty_timeouts.clear()
            await self._pty_backend.shutdown()
            self._pty_backend = None
        if self._ws:
            await self._ws.close()

    async def _authenticate(self) -> bool:
        auth_msg = AuthMessage(token=self._auth_token, worker_id=self._worker_id)
        await self._ws.send(auth_msg.model_dump_json())

        try:
            raw = await asyncio.wait_for(self._ws.recv(), timeout=10.0)
        except asyncio.TimeoutError:
            logger.error("Auth response timeout")
            return False

        msg = parse_message(raw)
        if isinstance(msg, AuthResultMessage):
            if msg.success:
                if msg.worker_id:
                    self._worker_id = msg.worker_id
                return True
            logger.error("Auth rejected: %s", msg.error)
        return False

    async def _sender_loop(self) -> None:
        while self._running and self._ws:
            data = self._retry_send
            if data is not None:
                kind = self._retry_send_kind
                frame_bytes = self._retry_send_bytes
                self._retry_send = None
                self._retry_send_kind = "control"
                self._retry_send_bytes = 0
            else:
                data, kind, frame_bytes = await self._next_queued_frame()
            self._inflight_send_kind = kind
            self._inflight_send_bytes = frame_bytes
            try:
                await self._ws.send(data)
            except BaseException:
                # The queue item used to be lost as soon as get() returned.  A
                # disconnect/cancel at the send boundary must remain first;
                # reliable terminal events are additionally replayed from the
                # fsynced outbox and are safe to deliver more than once.
                self._retry_send = data
                self._retry_send_kind = kind
                self._retry_send_bytes = frame_bytes
                raise
            finally:
                self._inflight_send_kind = None
                self._inflight_send_bytes = 0

    async def _next_queued_frame(self) -> tuple[str, str, int]:
        """Return control first, then fairly alternate bounded data and LOG."""

        while True:
            try:
                data, frame_bytes = self._send_queue.get_frame_nowait()
                return data, "control", frame_bytes
            except asyncio.QueueEmpty:
                pass
            queues = (
                (("data", self._data_send_queue), ("log", self._log_send_queue))
                if self._prefer_data_frame
                else (("log", self._log_send_queue), ("data", self._data_send_queue))
            )
            for kind, queue in queues:
                try:
                    data, frame_bytes = queue.get_frame_nowait()
                except asyncio.QueueEmpty:
                    continue
                self._prefer_data_frame = kind != "data"
                return data, kind, frame_bytes

            # Clear first, then re-check both queues to close the producer race:
            # a producer between clear() and wait() sets the event again.
            self._send_queue_ready.clear()
            if (
                not self._send_queue.empty()
                or not self._data_send_queue.empty()
                or not self._log_send_queue.empty()
            ):
                continue
            await self._send_queue_ready.wait()

    async def _receiver_loop(self) -> None:
        async for raw in self._ws:
            try:
                msg = parse_message(raw)
            except Exception:
                # Raw control messages can contain write-only mailbox tokens or
                # credential material. Never echo malformed payloads to logs.
                logger.warning(
                    "Failed to parse worker control message (%d bytes)",
                    len(raw),
                )
                continue
            await self._dispatch(msg)

    async def _dispatch(self, msg: Message) -> None:
        handlers = {
            "EXECUTE": self._handle_execute,
            "STOP": self._handle_stop,
            "READ_FILE": self._handle_read_file,
            "HEALTH_CHECK": self._handle_health_check,
            "UPLOAD_FILE": self._handle_upload_file,
            "MESSAGE": self._handle_message,
            "WATCH_FILES": self._handle_watch_files,
            "UNWATCH": self._handle_unwatch,
            "REGISTER_SYNC_MAPPING": self._handle_register_sync_mapping,
            "UNREGISTER_SYNC_MAPPING": self._handle_unregister_sync_mapping,
            "FORCE_SYNC": self._handle_force_sync,
            "CREDENTIAL_LOGIN": self._handle_credential_login,
            "ACCOUNT_LOGIN": self._handle_account_login,
            "ACCOUNT_LOGIN_OTP": self._handle_account_login_otp,
            "ACCOUNT_LOGIN_CANCEL": self._handle_account_login_cancel,
            "AGENT_API_CONFIGURE": self._handle_agent_api_configure,
            "EVENT_ACK": self._handle_event_ack,
        }
        handler = handlers.get(msg.type)
        if handler:
            try:
                if isinstance(msg, AccountLoginMessage):
                    request_id = msg.login_request_id
                    existing = self._account_login_tasks.get(request_id)
                    if existing is not None and not existing.done():
                        await self._send_event(AccountLoginResultMessage(
                            login_request_id=request_id,
                            account_id=msg.account_id,
                            slot_index=msg.slot_index,
                            success=False,
                            error="duplicate account login request",
                            cleanup_complete=False,
                        ))
                        return
                    self._account_login_cleanup_confirmed.pop(request_id, None)
                    task = asyncio.create_task(
                        self._run_account_login_task(msg)
                    )
                    self._account_login_tasks[request_id] = task
                    self._account_login_accounts[request_id] = msg.account_id

                    def _forget_login(completed: asyncio.Task) -> None:
                        if self._account_login_tasks.get(request_id) is completed:
                            self._account_login_tasks.pop(request_id, None)
                            self._account_login_accounts.pop(request_id, None)
                        if completed.cancelled():
                            return
                        error = completed.exception()
                        if error is not None:
                            # Retrieve the exception so asyncio does not emit an
                            # unstructured "Task exception was never retrieved"
                            # warning.  Do not include exception text: a third-
                            # party browser error could contain login inputs.
                            logger.error(
                                "Account login task %s failed unexpectedly (%s)",
                                request_id,
                                type(error).__name__,
                            )

                    task.add_done_callback(_forget_login)
                    return
                await handler(msg)
            except AccountLoginCleanupError as exc:
                # Credential/login exceptions may chain third-party messages
                # containing tokens or browser URLs. Log only the class and
                # return a protocol-specific, deliberately generic failure.
                logger.error(
                    "Cleanup could not be verified while handling %s (%s)",
                    msg.type,
                    type(exc).__name__,
                )
                if isinstance(msg, CredentialLoginMessage):
                    await self._send_event(CredentialLoginResultMessage(
                        account_id=msg.credentials.get(
                            "account_id", f"slot-{msg.slot_index}"
                        ),
                        slot_index=msg.slot_index,
                        success=False,
                        error=(
                            "Credential update failed and rollback could not "
                            "be verified"
                        ),
                    ))
                else:
                    await self._send_event(ErrorMessage(
                        error_type="handler_cleanup_error",
                        message=(
                            f"Cleanup could not be verified for {msg.type}"
                        ),
                        recoverable=False,
                    ))
            except Exception as exc:
                logger.error(
                    "Error handling %s message (%s)",
                    msg.type,
                    type(exc).__name__,
                )
                await self._send_event(ErrorMessage(
                    error_type="handler_error",
                    message=f"Failed to handle {msg.type}",
                    recoverable=True,
                ))
        else:
            logger.debug("Unhandled message type: %s", msg.type)

    # ---- Command handlers ----

    async def _handle_agent_api_configure(
        self,
        msg: AgentApiConfigureMessage,
    ) -> None:
        """Atomically install a correlated, worker-local API credential."""

        from elastic_agent.worker.agent_api import configure_agent_api

        try:
            actual_home = await asyncio.to_thread(
                configure_agent_api,
                provider=msg.provider,
                agent_type=msg.agent_type,
                config_dir=msg.config_dir or None,
                api_key=msg.api_key,
                account_id=msg.account_id,
                models=msg.models,
            )
        except Exception as exc:  # noqa: BLE001
            # Neither third-party exceptions nor validation values are safe to
            # reflect: the request contains a write-only key.
            logger.warning(
                "Agent API configuration failed (%s)",
                type(exc).__name__,
            )
            await self._send_event(AgentApiConfigureResultMessage(
                request_id=msg.request_id,
                account_id=msg.account_id,
                provider=msg.provider,
                agent_type=msg.agent_type,
                success=False,
                error="Agent API configuration failed",
                config_dir="",
            ))
            return

        await self._send_event(AgentApiConfigureResultMessage(
            request_id=msg.request_id,
            account_id=msg.account_id,
            provider=msg.provider,
            agent_type=msg.agent_type,
            success=True,
            config_dir=actual_home,
        ))

    async def _handle_execute(self, msg: ExecuteMessage) -> None:
        task_id = msg.task_id
        if (
            task_id in self._processes
            or task_id in self._supervised_tasks
            or (
                self._pty_backend is not None
                and self._pty_backend.has_task(task_id)
            )
        ):
            await self._send_event(ErrorMessage(
                error_type="duplicate_task",
                message=f"Process already running for task {task_id}",
                recoverable=True,
            ))
            return

        selected_config_dir = str(
            (msg.agent_params or {}).get("config_dir")
            or msg.env.get("CLAUDE_CONFIG_DIR")
            or ""
        )
        uses_claude_credentials = (
            msg.agent_params is not None
            or "CLAUDE_CONFIG_DIR" in msg.env
            or bool(msg.command and Path(msg.command[0]).name == "claude")
        )
        if uses_claude_credentials:
            from elastic_agent.core.claude_oauth import normalize_local_config_dir

            selected_config_dir = normalize_local_config_dir(
                selected_config_dir
            )
        if (
            selected_config_dir
            and selected_config_dir in self._unsafe_credential_config_dirs
        ):
            await self._send_process_exit(ProcessExitMessage(
                task_id=task_id,
                exit_code=-1,
                error_type="credential_slot_unsafe",
                error_message=(
                    "Credential slot rollback could not be verified; "
                    "worker restart or successful reconfiguration is required"
                ),
            ))
            return

        if msg.agent_params:
            if await self._try_execute_pty(msg):
                return
            logger.warning(
                "agent_params present but claude-pty unavailable; "
                "falling back to subprocess for task %s", task_id,
            )

        log_path = self._log_dir / f"{task_id}.ndjson"
        log_path.parent.mkdir(parents=True, exist_ok=True)

        env = {**os.environ, **msg.env}
        from elastic_agent.worker.agent_api import (
            ELASTIC_AGENT_API_PROJECTION_ROOT_ENV,
        )

        # This is a reserved, trusted Worker-to-container-runner hand-off.  A
        # regular/OAuth Job must never be able to turn it into an arbitrary
        # host bind mount.
        env.pop(ELASTIC_AGENT_API_PROJECTION_ROOT_ENV, None)
        command = list(msg.command)
        try:
            projection = self._agent_api_projection_for_execute(msg)
            if projection is not None:
                from elastic_agent.worker.agent_api import (
                    AGENT_API_CODEX_AUTH_ENV_KEYS,
                    CLOUDROUTER_CLAUDE_BASE_URL,
                    CLOUDROUTER_CLAUDE_BINARY_ENV,
                    apply_agent_api_runtime_env,
                    claude_shim_directory_for_home,
                    claude_wrapper_for_home,
                    codex_base_url_for_provider,
                )

                apply_agent_api_runtime_env(env, projection)
                self._agent_api_tasks[task_id] = projection
                env.pop(CLOUDROUTER_CLAUDE_BINARY_ENV, None)
                if (
                    projection.agent_type == "claude"
                    and command
                    and Path(command[0]).name == "claude"
                ):
                    original_binary = (
                        command[0]
                        if Path(command[0]).is_absolute()
                        else shutil.which(
                            command[0],
                            path=env.get("PATH"),
                        )
                    )
                    if not original_binary:
                        raise RuntimeError("Claude CLI is unavailable")
                    env[CLOUDROUTER_CLAUDE_BINARY_ENV] = original_binary
                    command[0] = claude_wrapper_for_home(projection.home)
                elif (
                    len(command) >= 3
                    and Path(command[0]).name in {"bash", "sh", "zsh"}
                    and command[1] in {"-c", "-lc", "-cl"}
                ):
                    auth_and_route_keys = {
                        "ANTHROPIC_AUTH_TOKEN",
                        "ANTHROPIC_API_KEY",
                        "CLAUDE_CODE_OAUTH_TOKEN",
                        "OPENAI_BASE_URL",
                        "CODEX_BASE_URL",
                        *AGENT_API_CODEX_AUTH_ENV_KEYS,
                    }
                    exports = [
                        "unset " + " ".join(sorted(auth_and_route_keys))
                    ]
                    credential_name = (
                        "CLAUDE_CONFIG_DIR"
                        if projection.agent_type == "claude"
                        else "CODEX_HOME"
                    )
                    exports.append(
                        f"export {credential_name}={shlex.quote(str(projection.home))}"
                    )
                    exports.append(
                        "export "
                        f"{ELASTIC_AGENT_API_PROJECTION_ROOT_ENV}="
                        f"{shlex.quote(str(projection.root))}"
                    )
                    if projection.agent_type == "claude":
                        original_binary = shutil.which(
                            "claude",
                            path=env.get("PATH"),
                        )
                        if not original_binary:
                            raise RuntimeError("Claude CLI is unavailable")
                        env[CLOUDROUTER_CLAUDE_BINARY_ENV] = original_binary
                        exports.extend([
                            "export ANTHROPIC_BASE_URL="
                            f"{shlex.quote(CLOUDROUTER_CLAUDE_BASE_URL)}",
                            "export PATH="
                            f"{shlex.quote(claude_shim_directory_for_home(projection.home))}:$PATH",
                        ])
                    else:
                        exports.append(
                            "export OPENAI_BASE_URL="
                            f"{shlex.quote(codex_base_url_for_provider(projection.provider))}"
                        )
                    command[2] = "; ".join([*exports, command[2]])
        except Exception as exc:  # noqa: BLE001
            self._agent_api_tasks.pop(task_id, None)
            self._agent_api_task_errors.pop(task_id, None)
            logger.warning(
                "Refusing unsafe Agent API execution for task %s (%s)",
                task_id,
                type(exc).__name__,
            )
            await self._send_process_exit(ProcessExitMessage(
                task_id=task_id,
                exit_code=-1,
                error_type="agent_api_configuration_error",
                error_message="Managed Agent API configuration is invalid",
            ))
            return
        cwd = msg.cwd if msg.cwd else None

        if self._task_supervisor is not None:
            from elastic_agent.worker.task_supervisor import (
                SupervisedTaskLaunch,
                TaskSupervisorError,
            )

            try:
                descriptor = await self._task_supervisor.launch(
                    SupervisedTaskLaunch(
                        task_id=task_id,
                        command=command,
                        cwd=cwd or os.getcwd(),
                        env=env,
                        timeout_seconds=msg.timeout,
                        job_id=getattr(msg, "job_id", None) or "",
                        watch_exhaustion=bool(
                            getattr(msg, "watch_exhaustion", False)
                        ),
                        agent_api_provider=(
                            projection.provider
                            if projection is not None
                            else None
                        ),
                        agent_type=(
                            projection.agent_type
                            if projection is not None
                            else None
                        ),
                    )
                )
            except TaskSupervisorError:
                # A response can be lost after the supervisor has committed the
                # launch. Inventory before reporting failure, otherwise a retry
                # could create a duplicate side-effecting command.
                descriptor = None
                try:
                    inventory = await self._task_supervisor.list_tasks()
                    descriptor = next(
                        (
                            item
                            for item in inventory
                            if item.task_id == task_id
                        ),
                        None,
                    )
                except TaskSupervisorError:
                    pass
                if descriptor is None:
                    self._agent_api_tasks.pop(task_id, None)
                    await self._send_event(ErrorMessage(
                        error_type="execute_failed",
                        message=(
                            "Independent task supervisor could not start "
                            "the process"
                        ),
                        recoverable=True,
                    ))
                    await self._send_process_exit(ProcessExitMessage(
                        task_id=task_id,
                        exit_code=-1,
                        error_type="execute_failed",
                        error_message=(
                            "Independent task supervisor could not start "
                            "the process"
                        ),
                    ))
                    return
            self._register_supervised_task(descriptor)
            logger.info(
                "Started supervised process for task %s (pid=%d)",
                task_id,
                descriptor.pid,
            )
            return

        spawn_kwargs: dict[str, Any] = {}
        if os.name == "posix":
            spawn_kwargs["start_new_session"] = True
        try:
            proc = await asyncio.create_subprocess_exec(
                *command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                stdin=asyncio.subprocess.PIPE,
                env=env,
                cwd=cwd,
                **spawn_kwargs,
            )
        except Exception as exc:
            self._agent_api_tasks.pop(task_id, None)
            await self._send_event(ErrorMessage(
                error_type="execute_failed",
                message=f"Failed to start process: {exc}",
                recoverable=True,
            ))
            await self._send_process_exit(ProcessExitMessage(
                task_id=task_id,
                exit_code=-1,
                error_type="execute_failed",
                error_message=f"Failed to start process: {exc}",
            ))
            return

        self._processes[task_id] = proc
        if os.name == "posix":
            # start_new_session=True guarantees pid == sid == pgid.
            self._process_groups[task_id] = proc.pid
            self._process_group_locks[task_id] = asyncio.Lock()
        self._stdin_pipes[task_id] = proc.stdin
        if getattr(msg, "watch_exhaustion", False):
            self._exhaustion_watch[task_id] = getattr(msg, "job_id", None) or ""
            self._exhaustion_fired.discard(task_id)
        logger.info("Started process for task %s (pid=%d)", task_id, proc.pid)

        task = asyncio.create_task(self._monitor_process(task_id, proc, log_path, msg.timeout))
        self._process_tasks[task_id] = task

    async def _recover_supervised_task_inventory(self) -> bool:
        """Inventory the independent runner before STATUS reconciliation.

        A failed/partial scan is advertised as incomplete.  The Manager must
        retain its run instead of interpreting an empty first snapshot as
        proof that the task disappeared.
        """

        if self._task_supervisor is None:
            self._process_inventory_complete = True
            self._process_inventory_error = None
            return True
        self._process_inventory_complete = False
        self._process_inventory_error = None
        try:
            descriptors = await self._task_supervisor.list_tasks()
            task_ids = [descriptor.task_id for descriptor in descriptors]
            if len(task_ids) != len(set(task_ids)):
                raise RuntimeError("duplicate supervisor inventory")
            for descriptor in descriptors:
                if (
                    descriptor.task_id in self._processes
                    or (
                        self._pty_backend is not None
                        and self._pty_backend.has_task(descriptor.task_id)
                    )
                ):
                    raise RuntimeError("conflicting task ownership")
            inventory_ids = {
                descriptor.task_id for descriptor in descriptors
            }
            offsets_changed = False
            for stale_task_id in (
                set(self._supervised_tasks) - inventory_ids
            ):
                stale_monitor = self._supervised_monitor_tasks.pop(
                    stale_task_id,
                    None,
                )
                if stale_monitor is not None:
                    stale_monitor.cancel()
                self._supervised_tasks.pop(stale_task_id, None)
                if self._supervised_offsets.pop(stale_task_id, None) is not None:
                    offsets_changed = True
                self._exhaustion_watch.pop(stale_task_id, None)
                self._exhaustion_fired.discard(stale_task_id)
                self._agent_api_tasks.pop(stale_task_id, None)
            for descriptor in descriptors:
                self._register_supervised_task(descriptor)
            retained_exit_task_ids = (
                self._pending_outbox_process_exit_task_ids()
            )
            for stale_task_id in (
                set(self._supervised_offsets)
                - inventory_ids
                - retained_exit_task_ids
            ):
                self._supervised_offsets.pop(stale_task_id, None)
                offsets_changed = True
            if offsets_changed:
                self._persist_supervised_offsets()
        except Exception as exc:
            logger.warning(
                "Independent task inventory is incomplete (%s)",
                type(exc).__name__,
            )
            self._process_inventory_error = (
                "independent task inventory is unavailable"
            )
            return False
        self._process_inventory_complete = True
        self._process_inventory_error = None
        logger.info(
            "Recovered %d task(s) from independent supervisor",
            len(descriptors),
        )
        return True

    def _register_supervised_task(self, descriptor: Any) -> None:
        """Attach one descriptor exactly once without owning its process."""

        task_id = descriptor.task_id
        previous = self._supervised_tasks.get(task_id)
        self._supervised_tasks[task_id] = descriptor
        self._supervised_offsets.setdefault(task_id, 0)
        if descriptor.watch_exhaustion:
            self._exhaustion_watch[task_id] = descriptor.job_id
        pending = descriptor.pending_exhaustion
        if pending is not None:
            self._exhaustion_fired.add(task_id)
            self._supervisor_event_task_ids[pending["event_id"]] = task_id
        self._supervisor_event_task_ids[
            descriptor.terminal_event_id
        ] = task_id
        if (
            descriptor.agent_api_provider is not None
            and task_id not in self._agent_api_tasks
        ):
            # Only classification metadata is reconstructed. API keys, command,
            # environment and credential paths never enter the descriptor.
            self._agent_api_tasks[task_id] = SimpleNamespace(
                provider=descriptor.agent_api_provider,
                agent_type=descriptor.agent_type,
            )
        monitor = self._supervised_monitor_tasks.get(task_id)
        if monitor is None or monitor.done():
            monitor = asyncio.create_task(
                self._monitor_supervised_task(task_id)
            )
            self._supervised_monitor_tasks[task_id] = monitor
        elif previous is not None and previous != descriptor:
            # The active monitor reads the current descriptor from the map.
            logger.debug("Refreshed supervised task %s metadata", task_id)

    async def _monitor_supervised_task(self, task_id: str) -> None:
        """Replay spool records and bridge one stable terminal event."""

        try:
            descriptor = self._supervised_tasks[task_id]
            if (
                descriptor.pending_exhaustion is not None
                and descriptor.state == "running"
            ):
                # A previous runtime may have durably marked exhaustion and
                # died before signaling. Re-establish the no-concurrent-resume
                # fence before replaying RUN_EXHAUSTED.
                await self._stop_process(task_id, "SIGINT")

            while task_id in self._supervised_tasks:
                offset = self._supervised_offsets.get(task_id, 0)
                snapshot = await self._task_supervisor.poll(
                    task_id,
                    offset=offset,
                )
                for record in snapshot.records:
                    stream = record.get("stream")
                    data = record.get("data")
                    if (
                        stream not in {"stdout", "stderr"}
                        or not isinstance(data, str)
                    ):
                        raise RuntimeError("invalid supervised spool record")
                    await self._handle_process_log_line(
                        task_id,
                        stream,
                        data,
                    )
                if snapshot.next_offset != offset:
                    self._supervised_offsets[task_id] = snapshot.next_offset
                    try:
                        self._persist_supervised_offsets()
                    except Exception:
                        # The durable cursor remains at ``offset``.  Keep memory
                        # aligned with that truth so the next inventory retries
                        # this whole batch instead of silently skipping frames.
                        self._supervised_offsets[task_id] = offset
                        raise
                if snapshot.terminal is not None:
                    await self._finish_supervised_task(
                        task_id,
                        snapshot.terminal,
                    )
                    return
                await asyncio.sleep(0.05 if snapshot.records else 0.5)
        except asyncio.CancelledError:
            # Runtime replacement detaches; the supervisor continues owning the
            # process, pipes, timeout and terminal record.
            raise
        except Exception as exc:
            self._process_inventory_complete = False
            self._process_inventory_error = (
                "independent task monitor is unavailable"
            )
            logger.error(
                "Lost supervised task monitor for %s (%s)",
                task_id,
                type(exc).__name__,
            )
        finally:
            current = self._supervised_monitor_tasks.get(task_id)
            if current is asyncio.current_task():
                self._supervised_monitor_tasks.pop(task_id, None)

    async def _finish_supervised_task(
        self,
        task_id: str,
        terminal: dict[str, Any],
    ) -> None:
        descriptor = self._supervised_tasks.get(task_id)
        if descriptor is None:
            return
        if (
            terminal.get("task_id") != task_id
            or terminal.get("event_id") != descriptor.terminal_event_id
            or not isinstance(terminal.get("exit_code"), int)
        ):
            raise RuntimeError("supervisor terminal identity mismatch")

        # Rotation must be committed ahead of the stale PROCESS_EXIT, matching
        # the direct-subprocess ordering contract.
        pending = descriptor.pending_exhaustion
        if pending is not None:
            exhausted = RunExhaustedMessage(
                task_id=task_id,
                job_id=descriptor.job_id,
                worker_id=self._worker_id or "unknown",
                reason=pending["reason"],
                event_id=pending["event_id"],
            )
            self._supervisor_event_task_ids[exhausted.event_id] = task_id
            await self._send_event(exhausted)

        semantic_error = self._agent_api_task_errors.pop(task_id, None)
        exit_code = int(terminal["exit_code"])
        if semantic_error is not None and exit_code == 0:
            exit_code = 1
        error_type = semantic_error[0] if semantic_error else terminal.get(
            "error_type"
        )
        error_message = (
            semantic_error[1]
            if semantic_error
            else terminal.get("error_message")
        )

        await self._mark_task_exiting(task_id)
        self._supervised_tasks.pop(task_id, None)
        self._exhaustion_watch.pop(task_id, None)
        self._exhaustion_fired.discard(task_id)
        self._agent_api_tasks.pop(task_id, None)
        if self._file_sync_manager:
            try:
                synced = await self._file_sync_manager.force_sync(task_id)
                logger.info(
                    "Force-synced %d files for supervised task %s on exit",
                    synced,
                    task_id,
                )
            except Exception:
                logger.exception(
                    "Failed to force-sync files for supervised task %s",
                    task_id,
                )
        event_id = str(terminal["event_id"])
        self._supervisor_event_task_ids[event_id] = task_id
        await self._send_process_exit(ProcessExitMessage(
            task_id=task_id,
            exit_code=exit_code,
            error_type=error_type,
            error_message=error_message,
            event_id=event_id,
        ))

    async def _wait_supervised_terminal(
        self,
        task_id: str,
        timeout: float,
    ) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                descriptor = next(
                    (
                        item
                        for item in await self._task_supervisor.list_tasks()
                        if item.task_id == task_id
                    ),
                    None,
                )
            except Exception:
                return False
            if descriptor is None:
                return False
            self._supervised_tasks[task_id] = descriptor
            if descriptor.state == "terminal":
                return True
            await asyncio.sleep(0.1)
        return False

    @staticmethod
    async def _wait_process_exit(proc: asyncio.subprocess.Process, timeout: float | None) -> bool:
        """Wait until the process terminates (returncode set); return False on
        timeout. Polls returncode instead of ``proc.wait()``, which on asyncio
        blocks until stdout/stderr EOF — a lingering grandchild (docker container
        from ``--sandbox os``) can hold those pipes open past process exit."""
        deadline = (time.monotonic() + timeout) if timeout else None
        while proc.returncode is None:
            if deadline is not None and time.monotonic() >= deadline:
                return False
            await asyncio.sleep(0.25)
        return True

    async def _monitor_process(
        self,
        task_id: str,
        proc: asyncio.subprocess.Process,
        log_path: Path,
        timeout: int | None,
    ) -> None:
        # Never let a logging failure swallow the process exit: if the local log
        # can't be opened (e.g. a mis-permissioned log dir), carry on without it
        # rather than crashing _monitor_process before ProcessExitMessage is sent
        # (which would strand the Manager's run phase at RUNNING).
        try:
            log_file = open(log_path, "a", encoding="utf-8")
        except Exception:
            logger.warning("Task %s: cannot open log file %s — continuing without local log",
                           task_id, log_path)
            log_file = None
        try:
            stdout_task = asyncio.create_task(
                self._read_stream(task_id, proc.stdout, "stdout", log_file)
            )
            stderr_task = asyncio.create_task(
                self._read_stream(task_id, proc.stderr, "stderr", log_file)
            )

            # Wait for the process to actually terminate. We poll returncode
            # rather than await proc.wait(): asyncio's wait() doesn't return
            # until the stdout/stderr pipes hit EOF, and a lingering child (e.g.
            # a docker container/dockerd from `--sandbox os`) can hold those
            # pipes open long — even indefinitely — after the process itself has
            # exited. Relying on wait() would strand this coroutine so the exit
            # is never reported, the Manager's run phase stays RUNNING forever,
            # and collect + S3-upload never fire. returncode is set promptly by
            # the child watcher (SIGCHLD) regardless of pipe state.
            if not await self._wait_process_exit(proc, timeout):
                logger.warning("Task %s timed out after %ds, sending SIGINT", task_id, timeout)
                await self._stop_process(task_id, "SIGINT")
                if not await self._wait_process_exit(proc, 15):
                    await self._stop_process(task_id, "SIGKILL")
                    await self._wait_process_exit(proc, 5)

            # A shell/CLI may exit while a background child keeps the inherited
            # pipes and delegated key alive.  End the whole task-owned process
            # group before draining output or publishing PROCESS_EXIT.
            await asyncio.shield(
                self._ensure_process_group_stopped(
                    task_id,
                    proc,
                    initial_signal=signal.SIGTERM,
                )
            )

            # Best-effort drain of any buffered output, bounded for the same
            # reason (the pipe may be held open past exit).
            for _stream_task in (stdout_task, stderr_task):
                try:
                    await asyncio.wait_for(_stream_task, timeout=_EXIT_DRAIN_TIMEOUT)
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    _stream_task.cancel()

        finally:
            # Also cover exceptions/cancellation before the normal cleanup
            # point.  The helper is serialized and idempotent.
            await asyncio.shield(
                self._ensure_process_group_stopped(
                    task_id,
                    proc,
                    initial_signal=signal.SIGTERM,
                )
            )
            if log_file is not None:
                log_file.close()
            exit_code = proc.returncode if proc.returncode is not None else -1
            semantic_error = self._agent_api_task_errors.pop(task_id, None)
            if semantic_error is not None and exit_code == 0:
                # Both CLIs can report a structurally failed provider turn while
                # exiting cleanly. Process health must not mark that Job done.
                exit_code = 1
            await self._mark_task_exiting(task_id)
            self._processes.pop(task_id, None)
            self._process_tasks.pop(task_id, None)
            self._stdin_pipes.pop(task_id, None)
            self._process_groups.pop(task_id, None)
            self._process_group_locks.pop(task_id, None)
            self._exhaustion_watch.pop(task_id, None)
            self._exhaustion_fired.discard(task_id)
            self._agent_api_tasks.pop(task_id, None)
            logger.info("Process for task %s exited with code %d", task_id, exit_code)

            if self._file_sync_manager:
                try:
                    synced = await self._file_sync_manager.force_sync(task_id)
                    logger.info("Force-synced %d files for task %s on process exit", synced, task_id)
                except Exception:
                    logger.exception("Failed to force-sync files for task %s on exit", task_id)

            await self._send_process_exit(
                ProcessExitMessage(
                    task_id=task_id,
                    exit_code=exit_code,
                    error_type=semantic_error[0] if semantic_error else None,
                    error_message=semantic_error[1] if semantic_error else None,
                )
            )

    # ---- PTY-hosted execution (claude-pty) ----

    async def _try_execute_pty(self, msg: ExecuteMessage) -> bool:
        """Run the task in a PTY session. Returns False if claude-pty is missing."""
        from elastic_agent.worker.pty_backend import PTY_AVAILABLE

        if not PTY_AVAILABLE:
            return False

        if self._pty_backend is None:
            from elastic_agent.worker.pty_backend import ElasticPTYBackend
            self._pty_backend = ElasticPTYBackend(self, log_dir=self._log_dir)

        params = msg.agent_params or {}
        task_id = msg.task_id
        config_dir = params.get("config_dir") or msg.env.get("CLAUDE_CONFIG_DIR")
        env_overrides = {k: v for k, v in msg.env.items() if k != "CLAUDE_CONFIG_DIR"}
        from elastic_agent.worker.agent_api import (
            ELASTIC_AGENT_API_PROJECTION_ROOT_ENV,
        )

        env_overrides.pop(ELASTIC_AGENT_API_PROJECTION_ROOT_ENV, None)

        # Workers run as root; claude refuses --dangerously-skip-permissions
        # under root unless it believes it's sandboxed. Cloud workers are
        # single-purpose VMs, so this is the intended unattended setup.
        if hasattr(os, "geteuid") and os.geteuid() == 0:
            env_overrides.setdefault("IS_SANDBOX", "1")

        try:
            projection = self._agent_api_projection_for_execute(msg)
            if projection is not None:
                if projection.agent_type != "claude":
                    raise RuntimeError("PTY requires a Claude Agent API home")
                from elastic_agent.worker.agent_api import (
                    apply_agent_api_runtime_env,
                )

                apply_agent_api_runtime_env(env_overrides, projection)
                self._agent_api_tasks[task_id] = projection
                # Never let an unvalidated agent_params directory shadow the
                # managed home selected from CLAUDE_CONFIG_DIR.  The backend
                # uses config_dir both for its pool identity and to select the
                # final credential wrapper.
                config_dir = str(projection.home)
            session_id = await self._pty_backend.launch(
                key=task_id,
                prompt=params.get("prompt", ""),
                cwd=msg.cwd or os.getcwd(),
                resume_session_id=params.get("resume_session_id"),
                model=params.get("model"),
                config_dir=config_dir,
                env_overrides=env_overrides or None,
                # The turn must be allowed to run as long as the task itself
                # instead of relying on claude-pty's shorter default.
                response_timeout=params.get("response_timeout") or msg.timeout,
            )
        except Exception as exc:
            self._agent_api_tasks.pop(task_id, None)
            logger.exception("PTY launch failed for task %s", task_id)
            await self._send_event(ErrorMessage(
                error_type="execute_failed",
                message=f"Failed to start PTY session: {exc}",
                recoverable=True,
            ))
            await self._send_process_exit(ProcessExitMessage(
                task_id=task_id,
                exit_code=-1,
                error_type="execute_failed",
                error_message=f"Failed to start PTY session: {exc}",
            ))
            return True

        logger.info("Started PTY session %s for task %s", session_id, task_id)
        if session_id:
            self._pty_session_ids[task_id] = session_id

        if msg.timeout:
            # Hard backstop only: claude-pty's own response_timeout (set to
            # the same task timeout above) fires first and ends the turn
            # gracefully; the watchdog covers a wedged session.
            self._pty_timeouts[task_id] = asyncio.create_task(
                self._pty_timeout_watch(task_id, msg.timeout + 60)
            )
        return True

    async def _pty_timeout_watch(self, task_id: str, timeout: int) -> None:
        await asyncio.sleep(timeout)
        logger.warning("PTY task %s timed out after %ds, stopping session", task_id, timeout)
        try:
            message = (
                f"Worker runtime timed out after {timeout}s and interrupted "
                "the Claude process"
            )
            if hasattr(self._pty_backend, "mark_task_error"):
                self._pty_backend.mark_task_error(task_id, "runtime_timeout", message)
            await self._pty_backend.stop(task_id)
        except Exception:
            logger.exception("Failed to stop timed-out PTY task %s", task_id)

    async def _on_pty_exit(
        self,
        task_id: str,
        exit_code: int,
        *,
        session_id: str | None = None,
        error_type: str | None = None,
        error_message: str | None = None,
    ) -> None:
        """Called by ElasticPTYBackend when a PTY turn/session finishes."""
        await self._mark_task_exiting(task_id)
        timer = self._pty_timeouts.pop(task_id, None)
        if timer and not timer.done():
            timer.cancel()
        session_id = session_id or self._pty_session_ids.pop(task_id, None)
        self._pty_session_ids.pop(task_id, None)
        self._agent_api_tasks.pop(task_id, None)

        logger.info("PTY task %s finished with exit code %d", task_id, exit_code)

        if self._file_sync_manager:
            try:
                synced = await self._file_sync_manager.force_sync(task_id)
                logger.info("Force-synced %d files for PTY task %s on exit", synced, task_id)
            except Exception:
                logger.exception("Failed to force-sync files for PTY task %s", task_id)

        await self._send_process_exit(ProcessExitMessage(
            task_id=task_id,
            exit_code=exit_code,
            session_id=session_id,
            error_type=error_type,
            error_message=error_message,
        ))

    @staticmethod
    async def _iter_stream_frames(stream: asyncio.StreamReader):
        """Drain arbitrary-length physical lines as bounded byte frames."""

        pending = bytearray()
        while True:
            try:
                chunk = await stream.read(_MAX_LOG_FRAME_BYTES)
            except Exception:
                logger.warning("Worker output stream read failed", exc_info=True)
                break
            if not chunk:
                break
            pending.extend(chunk)
            while pending:
                # Search only inside the byte budget.  A short OS read followed
                # by a second read containing a later newline must not let the
                # newline branch emit an over-sized combined frame.
                newline = pending.find(b"\n", 0, _MAX_LOG_FRAME_BYTES)
                if newline >= 0:
                    end = newline + 1
                elif len(pending) >= _MAX_LOG_FRAME_BYTES:
                    end = _MAX_LOG_FRAME_BYTES
                else:
                    break
                yield bytes(pending[:end])
                del pending[:end]
        if pending:
            yield bytes(pending)

    @classmethod
    async def _iter_stream_text_frames(cls, stream: asyncio.StreamReader):
        """Decode bounded byte frames without corrupting split UTF-8 codepoints."""

        decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
        async for frame in cls._iter_stream_frames(stream):
            physical_line_end = frame.endswith(b"\n")
            text = decoder.decode(frame, final=physical_line_end)
            if physical_line_end:
                decoder.reset()
            yield text
        tail = decoder.decode(b"", final=True)
        if tail:
            yield tail

    async def _read_stream(
        self,
        task_id: str,
        stream: asyncio.StreamReader | None,
        stream_name: str,
        log_file: Any,
    ) -> None:
        if stream is None:
            return

        async for text_frame in self._iter_stream_text_frames(stream):
            line = text_frame.rstrip("\n")
            if not line:
                continue

            parsed = self._try_parse_ndjson(line) if stream_name == "stdout" else None

            log_entry = {
                "task_id": task_id,
                "stream": stream_name,
                "data": line,
                "timestamp": _utcnow().isoformat(),
                "parsed": parsed,
            }
            if log_file is not None:
                log_file.write(json.dumps(log_entry) + "\n")
                log_file.flush()

            await self._handle_process_log_line(
                task_id,
                stream_name,
                line,
                parsed=parsed,
            )

    async def _handle_process_log_line(
        self,
        task_id: str,
        stream_name: str,
        line: str,
        *,
        parsed: dict | None = None,
    ) -> None:
        """Apply identical logging/error/rotation semantics to either runner."""

        if parsed is None and stream_name == "stdout":
            parsed = self._try_parse_ndjson(line)
        await self._send_event(LogMessage(
            task_id=task_id,
            stream=stream_name,
            data=line,
            parsed=parsed,
        ))

        projection = self._agent_api_tasks.get(task_id)
        provider_error = None
        if projection is not None:
            if self._agent_api_terminal_success(line):
                previous = self._agent_api_task_errors.get(task_id)
                # Codex emits reconnecting ``type=error`` frames for retryable
                # 500/502 responses before a later successful turn.completed.
                if previous is not None and previous[0] in {
                    "agent_api_error",
                    "agent_api_transient_error",
                }:
                    self._agent_api_task_errors.pop(task_id, None)
            provider_error = _classify_agent_api_provider_error(
                projection.provider,
                line,
            )
            semantic_error = provider_error or self._agent_api_fatal_error(
                line,
                provider=projection.provider,
                agent_type=projection.agent_type,
            )
            if semantic_error is not None:
                previous = self._agent_api_task_errors.get(task_id)
                if (
                    previous is None
                    or _AGENT_API_ERROR_PRIORITY.get(semantic_error[0], 0)
                    > _AGENT_API_ERROR_PRIORITY.get(previous[0], 0)
                ):
                    self._agent_api_task_errors[task_id] = semantic_error

        # Mode-B rotation (a): the opaque command consumes the account
        # internally, so rotation occurs only after its process group is gone.
        exhaustion_reason = None
        if (
            task_id in self._exhaustion_watch
            and task_id not in self._exhaustion_fired
        ):
            if (
                provider_error is not None
                and provider_error[0] == "agent_api_auth_failure"
            ):
                exhaustion_reason = "agent_api_auth_failure"
            elif (
                provider_error is not None
                and provider_error[0] == "agent_api_rate_limited"
            ):
                exhaustion_reason = "agent_api_rate_limited"
            elif (
                provider_error is None
                and (
                    is_rate_limited(line)
                    or is_auth_failure(line)
                )
            ):
                exhaustion_reason = "rate_limit"
        if exhaustion_reason is not None:
            await self._signal_exhaustion(
                task_id,
                reason=exhaustion_reason,
            )

    async def _signal_exhaustion(
        self,
        task_id: str,
        *,
        reason: str = "rate_limit",
    ) -> None:
        """Emit RunExhaustedMessage once and interrupt the run so the
        orchestrator can rotate the account and resume."""
        if task_id in self._exhaustion_fired:
            return
        self._exhaustion_fired.add(task_id)
        job_id = self._exhaustion_watch.get(task_id, "")
        exhausted_event = RunExhaustedMessage(
            task_id=task_id,
            job_id=job_id,
            worker_id=self._worker_id or "unknown",
            reason=reason,
        )
        supervised = task_id in self._supervised_tasks
        if supervised:
            try:
                await self._task_supervisor.mark_exhaustion(
                    task_id,
                    reason=reason,
                    event_id=exhausted_event.event_id,
                )
            except Exception as exc:
                # Rotation without a durable ownership fence could dispatch a
                # second command after runtime replacement.  Do not consume the
                # spool batch or permanently suppress detection: inventory will
                # retry the same frame once the supervisor is reachable.
                self._exhaustion_fired.discard(task_id)
                self._process_inventory_complete = False
                self._process_inventory_error = (
                    "independent exhaustion fence is unavailable"
                )
                logger.error(
                    "Could not persist exhaustion for task %s (%s)",
                    task_id,
                    type(exc).__name__,
                )
                raise RuntimeError(
                    "could not persist supervised exhaustion fence"
                ) from exc
            descriptor = self._supervised_tasks[task_id]
            pending = {
                "event_id": exhausted_event.event_id,
                "reason": reason,
            }
            self._supervised_tasks[task_id] = replace(
                descriptor,
                pending_exhaustion=pending,
            )
            self._supervisor_event_task_ids[
                exhausted_event.event_id
            ] = task_id
        logger.warning(
            "Task %s tripped exhaustion detector; interrupting before rotation", task_id
        )
        # Do not let the Manager dispatch a resumed command while the old
        # command is still consuming the same credential/config directory.
        # _monitor_process drains this stream and queues PROCESS_EXIT only after
        # this RUN_EXHAUSTED event, so task-id guards can discard that stale exit.
        proc = self._processes.get(task_id)
        await self._stop_process(task_id, "SIGINT")
        if supervised:
            if not await self._wait_supervised_terminal(task_id, 20):
                logger.error(
                    "Task %s did not stop after exhaustion; refusing rotation",
                    task_id,
                )
                self._process_inventory_complete = False
                self._process_inventory_error = (
                    "exhausted supervised task has not stopped"
                )
                raise RuntimeError(
                    "exhausted supervised task has not reached terminal state"
                )
        if proc is not None and proc.returncode is None:
            if not await self._wait_process_exit(proc, 15):
                logger.warning(
                    "Task %s ignored SIGINT after exhaustion; sending SIGKILL",
                    task_id,
                )
                await self._stop_process(task_id, "SIGKILL")
                if not await self._wait_process_exit(proc, 5):
                    # Rotating now would run two commands concurrently against
                    # the same credential/config directory.  Let the eventual
                    # PROCESS_EXIT fail the run instead of dispatching a resume
                    # while ownership of the old process is still ambiguous.
                    logger.error(
                        "Task %s did not exit after exhaustion; refusing rotation",
                        task_id,
                    )
                    return
        await self._send_event(exhausted_event)

    @staticmethod
    def _try_parse_ndjson(line: str) -> dict | None:
        try:
            obj = json.loads(line)
            if isinstance(obj, dict) and "type" in obj:
                return {
                    "type": obj["type"],
                    "subtype": obj.get("subtype"),
                    "cost_usd": obj.get("cost_usd"),
                    "session_id": obj.get("session_id"),
                }
        except (TypeError, ValueError, RecursionError):
            pass
        return None

    @staticmethod
    def _agent_api_fatal_error(
        line: str,
        *,
        provider: str = "cloudrouter",
        agent_type: str | None = None,
    ) -> tuple[str, str] | None:
        """Recognize terminal provider failures without matching tool errors."""

        try:
            event = json.loads(line)
        except (TypeError, ValueError, RecursionError):
            return None
        if not isinstance(event, dict):
            return None
        event_type = str(event.get("type") or "")
        provider_label = _AGENT_API_PROVIDER_LABELS.get(
            provider,
            "Agent API provider",
        )
        if event.get("isApiErrorMessage") and event_type in {
            "assistant",
            "message",
            "result",
        }:
            return (
                "agent_api_error",
                f"{provider_label} Claude API request failed",
            )
        if event_type == "turn.failed":
            return (
                "agent_api_error",
                f"{provider_label} Codex turn failed",
            )
        if event_type == "result" and (
            event.get("is_error")
            or str(event.get("subtype") or "").lower() in {
                "error",
                "api_error",
            }
        ):
            return (
                "agent_api_error",
                f"{provider_label} {(agent_type or 'Claude').title()} turn failed",
            )
        if event_type == "session_crashed":
            return (
                "agent_api_error",
                f"{provider_label} agent turn failed",
            )
        return None

    @staticmethod
    def _agent_api_terminal_success(line: str) -> bool:
        """Recognize a provider turn that conclusively completed successfully."""

        try:
            event = json.loads(line)
        except (TypeError, ValueError, RecursionError):
            return False
        if not isinstance(event, dict):
            return False
        event_type = str(event.get("type") or "")
        if event_type == "turn.completed":
            return True
        if event_type != "result":
            return False
        return (
            not bool(event.get("is_error"))
            and not bool(event.get("isApiErrorMessage"))
            and str(event.get("subtype") or "").lower()
            not in {"error", "api_error"}
        )

    @staticmethod
    def _agent_api_projection_for_execute(msg: ExecuteMessage):
        """Return a validated projection, or None for an ordinary OAuth task."""

        from elastic_agent.worker.agent_api import agent_api_marker_for_home

        params = msg.agent_params or {}
        candidates = [
            (params.get("config_dir"), "claude"),
            (msg.env.get("CLAUDE_CONFIG_DIR"), "claude"),
            (msg.env.get("CODEX_HOME"), "codex"),
        ]
        projection = None
        for candidate, expected_agent_type in candidates:
            if not candidate:
                continue
            # Managed projections always live below this worker-owned namespace.
            # Avoid probing ordinary OAuth homes (which can legitimately be
            # inaccessible in local tests or mixed-user installations).
            if ".elastic-agent-api" not in Path(candidate).expanduser().parts:
                continue
            current = agent_api_marker_for_home(candidate)
            if current is None:
                raise RuntimeError(
                    "Agent API projection marker is missing",
                )
            if current.agent_type != expected_agent_type:
                raise RuntimeError(
                    "Agent API projection has the wrong agent type",
                )
            if projection is not None and current != projection:
                raise RuntimeError("conflicting Agent API homes")
            projection = current
        return projection

    async def _handle_stop(self, msg: StopMessage) -> None:
        sig_name = msg.signal or "SIGTERM"
        if self._pty_backend is not None and self._pty_backend.has_task(msg.task_id):
            # PTY sessions interrupt via Esc + teardown; signals don't apply.
            try:
                await self._pty_backend.stop(msg.task_id)
            except Exception:
                logger.exception("Failed to stop PTY task %s", msg.task_id)
            return
        await self._stop_process(
            msg.task_id,
            sig_name,
            scope=msg.scope,
            escalate=msg.escalate,
        )

    async def _stop_process(
        self,
        task_id: str,
        sig_name: str,
        *,
        scope: str = "group",
        escalate: bool = True,
    ) -> None:
        if task_id in self._supervised_tasks:
            try:
                await self._task_supervisor.signal(
                    task_id,
                    signal_name=sig_name,
                    scope=scope,
                    escalate=escalate,
                )
            except Exception as exc:
                logger.error(
                    "Failed to signal supervised task %s (%s)",
                    task_id,
                    type(exc).__name__,
                )
            return
        proc = self._processes.get(task_id)
        if proc is None:
            return

        sig_map = {
            "SIGINT": signal.SIGINT,
            "SIGTERM": signal.SIGTERM,
            "SIGKILL": signal.SIGKILL,
        }
        sig = sig_map.get(sig_name, signal.SIGTERM)

        if task_id in self._process_groups:
            await self._ensure_process_group_stopped(
                task_id,
                proc,
                initial_signal=sig,
                scope=scope,
                escalate=escalate,
            )
            return

        # Non-POSIX fallback. Poll returncode rather than proc.wait(), because
        # descendants can retain inherited stdout/stderr handles.
        if proc.returncode is None:
            try:
                proc.send_signal(sig)
                logger.info(
                    "Sent %s to task %s (pid=%d)",
                    sig_name,
                    task_id,
                    proc.pid,
                )
            except ProcessLookupError:
                return
        if not escalate:
            return
        if sig == signal.SIGKILL or await self._wait_process_exit(proc, 10):
            return
        try:
            proc.send_signal(signal.SIGTERM)
        except ProcessLookupError:
            return
        if await self._wait_process_exit(proc, 5):
            return
        try:
            proc.kill()
        except ProcessLookupError:
            pass

    @staticmethod
    def _process_group_exists(pgid: int) -> bool:
        if os.name != "posix" or pgid <= 1:
            return False
        try:
            os.killpg(pgid, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            # A task group is always created by this runtime under the same
            # user. Treat an ownership mismatch as alive but refuse to signal.
            return True
        return True

    async def _wait_process_group_exit(
        self,
        pgid: int,
        timeout: float,
    ) -> bool:
        deadline = time.monotonic() + max(0.0, timeout)
        while self._process_group_exists(pgid):
            if time.monotonic() >= deadline:
                return False
            await asyncio.sleep(0.05)
        return True

    async def _ensure_process_group_stopped(
        self,
        task_id: str,
        proc: asyncio.subprocess.Process,
        *,
        initial_signal: signal.Signals,
        scope: str = "group",
        escalate: bool = True,
    ) -> None:
        """Terminate exactly the POSIX group created for ``task_id``.

        The stored pgid must equal this process object's pid, which is guaranteed
        only because spawn used ``start_new_session=True``.  A live leader is
        checked against the kernel before signaling; once it exits, an extant
        process group retains that pgid and therefore prevents pid/pgid reuse.
        """

        pgid = self._process_groups.get(task_id)
        lock = self._process_group_locks.get(task_id)
        if (
            os.name != "posix"
            or pgid is None
            or pgid <= 1
            or pgid != proc.pid
            or lock is None
        ):
            return

        async with lock:
            if not self._process_group_exists(pgid):
                return
            leader_verified = False
            if proc.returncode is None:
                try:
                    if os.getpgid(proc.pid) != pgid:
                        logger.error(
                            "Refusing to signal mismatched process group %d "
                            "for task %s",
                            pgid,
                            task_id,
                        )
                        return
                    leader_verified = True
                except ProcessLookupError:
                    # The leader exited between returncode observation and the
                    # check. Any remaining members still reserve this pgid.
                    if scope == "process":
                        return
            elif scope == "process":
                # Never send a process-scoped signal to a numeric PID after its
                # owned Process object became terminal; the PID may be reused
                # while descendants still retain the original process group.
                return

            async def send(
                sig: signal.Signals,
                target: str = "group",
            ) -> bool:
                try:
                    if target == "process":
                        if not leader_verified:
                            return False
                        os.kill(proc.pid, sig)
                    else:
                        os.killpg(pgid, sig)
                    logger.info(
                        "Sent %s to task %s %s %d",
                        sig.name,
                        task_id,
                        target,
                        pgid,
                    )
                    return True
                except ProcessLookupError:
                    return False
                except PermissionError:
                    logger.error(
                        "Refusing process group %d for task %s: ownership "
                        "verification failed",
                        pgid,
                        task_id,
                    )
                    return False

            if not await send(initial_signal, scope):
                return
            if not escalate:
                return
            first_grace = 5.0 if initial_signal == signal.SIGKILL else 10.0
            if await self._wait_process_group_exit(pgid, first_grace):
                return
            if initial_signal not in {signal.SIGTERM, signal.SIGKILL}:
                if not await send(signal.SIGTERM):
                    return
                if await self._wait_process_group_exit(pgid, 5.0):
                    return
            if initial_signal != signal.SIGKILL:
                await send(signal.SIGKILL)
                await self._wait_process_group_exit(pgid, 5.0)

    async def _handle_read_file(self, msg: ReadFileMessage) -> None:
        try:
            content = Path(msg.path).read_text(encoding=msg.encoding)
            await self._send_event(FileContentMessage(
                request_id=msg.request_id,
                path=msg.path,
                content=content,
            ))
        except Exception as exc:
            await self._send_event(ErrorMessage(
                error_type="read_file_failed",
                message=f"Failed to read {msg.path}: {exc}",
                recoverable=True,
            ))

    async def _handle_health_check(self, _msg: HealthCheckMessage) -> None:
        if (
            self._task_supervisor is not None
            and not self._process_inventory_complete
        ):
            await self._recover_supervised_task_inventory()
        await self._send_status()

    def _check_claude_cli(self) -> dict[str, Any]:
        path = shutil.which("claude")
        if not path:
            return {
                "ok": False,
                "path": None,
                "version": None,
                "error": "claude command not found in PATH",
            }

        try:
            result = subprocess.run(
                [path, "--version"],
                check=False,
                capture_output=True,
                text=True,
                timeout=10,
            )
        except Exception as exc:
            return {
                "ok": False,
                "path": path,
                "version": None,
                "error": f"claude --version failed: {exc}",
            }

        output = (result.stdout or result.stderr or "").strip()
        if result.returncode != 0:
            return {
                "ok": False,
                "path": path,
                "version": output or None,
                "error": f"claude --version exited {result.returncode}",
            }
        if "native binary not installed" in output.lower():
            return {
                "ok": False,
                "path": path,
                "version": output or None,
                "error": "Claude Code native binary is not installed",
            }
        return {
            "ok": True,
            "path": path,
            "version": output or None,
            "error": None,
        }

    def _check_codex_cli(self) -> dict[str, Any]:
        path = shutil.which("codex")
        if not path:
            return {
                "ok": False,
                "path": None,
                "version": None,
                "error": "codex command not found in PATH",
            }

        try:
            result = subprocess.run(
                [path, "--version"],
                check=False,
                capture_output=True,
                text=True,
                timeout=10,
            )
        except Exception as exc:
            return {
                "ok": False,
                "path": path,
                "version": None,
                "error": f"codex --version failed: {type(exc).__name__}",
            }

        output = (result.stdout or result.stderr or "").strip()
        if result.returncode != 0:
            return {
                "ok": False,
                "path": path,
                "version": output or None,
                "error": f"codex --version exited {result.returncode}",
            }
        return {
            "ok": True,
            "path": path,
            "version": output or None,
            "error": None,
        }

    async def _send_status(self) -> None:
        cpu = mem = disk = 0.0
        try:
            total, used, _free = shutil.disk_usage("/")
            disk = used / total * 100 if total else 0
        except Exception:
            pass

        try:
            load = os.getloadavg()
            cpu_count = os.cpu_count() or 1
            cpu = load[0] / cpu_count * 100
        except Exception:
            pass

        try:
            if platform.system() == "Linux":
                meminfo = Path("/proc/meminfo").read_text()
                lines_map = {}
                for mline in meminfo.splitlines():
                    parts = mline.split(":")
                    if len(parts) == 2:
                        lines_map[parts[0].strip()] = int(parts[1].strip().split()[0])
                total_mem = lines_map.get("MemTotal", 1)
                available = lines_map.get("MemAvailable", total_mem)
                mem = (1 - available / total_mem) * 100 if total_mem else 0
        except Exception:
            pass

        expected_agent = os.environ.get(
            "ELASTIC_AGENT_AGENT_TYPE", "claude"
        ).strip().lower()
        if expected_agent == "codex":
            codex = self._check_codex_cli()
            claude = {"ok": False, "path": None, "version": None}
            selected = codex
        else:
            expected_agent = "claude"
            claude = self._check_claude_cli()
            codex = {"ok": False, "path": None, "version": None}
            selected = claude
        runtime_ready = bool(selected["ok"])

        pending_process_exits = await self._pending_process_exit_task_ids()
        await self._send_event(StatusMessage(
            cpu=round(cpu, 1),
            mem=round(mem, 1),
            disk=round(disk, 1),
            active_processes=self.active_processes,
            pending_process_exits=pending_process_exits,
            process_inventory_complete=self._process_inventory_complete,
            process_inventory_error=self._process_inventory_error,
            runtime_ready=runtime_ready,
            runtime_error=None if runtime_ready else selected["error"],
            agent_type=expected_agent,
            claude_cli_ok=bool(claude["ok"]),
            claude_version=claude["version"],
            claude_path=claude["path"],
            codex_cli_ok=bool(codex["ok"]),
            codex_version=codex["version"],
            codex_path=codex["path"],
        ))

    async def _handle_upload_file(self, msg: UploadFileMessage) -> None:
        import base64
        try:
            path = Path(msg.path)
            path.parent.mkdir(parents=True, exist_ok=True)
            data = base64.b64decode(msg.content_base64)
            if msg.write_mode == "append":
                with path.open("ab") as fh:
                    fh.write(data)
            else:
                path.write_bytes(data)
            os.chmod(path, int(msg.mode, 8))
        except Exception as exc:
            await self._send_event(ErrorMessage(
                error_type="upload_file_failed",
                message=f"Failed to write {msg.path}: {exc}",
                recoverable=True,
            ))

    async def _handle_message(self, msg: SendInputMessage) -> None:
        if msg.task_id in self._supervised_tasks:
            try:
                await self._task_supervisor.write_stdin(
                    msg.task_id,
                    msg.payload,
                )
            except Exception:
                await self._send_event(ErrorMessage(
                    error_type="stdin_write_failed",
                    message=(
                        "Failed to write to independent task stdin for "
                        f"{msg.task_id}"
                    ),
                    recoverable=True,
                ))
            return
        stdin = self._stdin_pipes.get(msg.task_id)
        if stdin is None:
            await self._send_event(ErrorMessage(
                error_type="no_process",
                message=f"No running process for task {msg.task_id}",
                recoverable=True,
            ))
            return
        try:
            stdin.write((msg.payload + "\n").encode())
            await stdin.drain()
        except Exception as exc:
            await self._send_event(ErrorMessage(
                error_type="stdin_write_failed",
                message=f"Failed to write to stdin for task {msg.task_id}: {exc}",
                recoverable=True,
            ))

    async def _handle_watch_files(self, msg: WatchFilesMessage) -> None:
        logger.debug("WATCH_FILES received (request_id=%s) — delegated to FileSyncManager", msg.request_id)

    async def _handle_unwatch(self, msg: UnwatchMessage) -> None:
        logger.debug("UNWATCH received (request_id=%s) — delegated to FileSyncManager", msg.request_id)

    async def _handle_register_sync_mapping(self, msg: RegisterSyncMappingMessage) -> None:
        from elastic_agent.worker.file_sync import FileSyncManager, SyncMappingEntry, create_storage_backend

        if self._file_sync_manager is None:
            storage = create_storage_backend()
            self._file_sync_manager = FileSyncManager(
                worker_id=self._worker_id or "unknown",
                storage=storage,
                on_file_synced=self._on_file_synced,
                on_file_changed=self._on_file_changed,
            )
            await self._file_sync_manager.start()

        entry = SyncMappingEntry(
            task_id=msg.task_id,
            book_slug=msg.book_slug,
            oss_prefix=msg.oss_prefix,
            watch_paths=msg.watch_paths,
            session_path_hash=msg.session_path_hash,
        )
        self._file_sync_manager.register_mapping(entry)

    async def _handle_unregister_sync_mapping(self, msg: UnregisterSyncMappingMessage) -> None:
        if self._file_sync_manager:
            await self._file_sync_manager.unregister_mapping(msg.task_id)

    async def _handle_force_sync(self, msg) -> None:
        from elastic_agent.core.protocols.messages import ForceSyncResultMessage
        from elastic_agent.worker.file_sync import FileSyncManager, SyncMappingEntry, create_storage_backend

        task_id = msg.task_id
        if not self._file_sync_manager:
            if not (getattr(msg, "oss_prefix", None) and (
                getattr(msg, "watch_paths", None) or getattr(msg, "cwd", None) or getattr(msg, "book_slug", None)
            )):
                await self._send_event(ForceSyncResultMessage(
                    task_id=task_id,
                    request_id=getattr(msg, "request_id", None),
                    files_synced=0,
                    success=False,
                    error="FileSyncManager not initialized",
                ))
                return
            storage = create_storage_backend()
            self._file_sync_manager = FileSyncManager(
                worker_id=self._worker_id or "unknown",
                storage=storage,
                on_file_synced=self._on_file_synced,
                on_file_changed=self._on_file_changed,
            )
            await self._file_sync_manager.start()

        mapping = self._file_sync_manager._mappings.get(task_id)
        transient = False
        if mapping is None:
            watch_paths = list(getattr(msg, "watch_paths", None) or [])
            cwd = getattr(msg, "cwd", None)
            book_slug = getattr(msg, "book_slug", None) or ""
            oss_prefix = getattr(msg, "oss_prefix", None) or ""
            if not watch_paths:
                if cwd:
                    watch_paths.append(cwd)
                elif book_slug:
                    watch_paths.append(f"/root/books/{book_slug}/")
            if not (book_slug and oss_prefix and watch_paths):
                await self._send_event(ForceSyncResultMessage(
                    task_id=task_id,
                    request_id=getattr(msg, "request_id", None),
                    files_synced=0,
                    success=False,
                    error=f"No sync mapping registered for task {task_id}",
                ))
                return
            mapping = SyncMappingEntry(
                task_id=task_id,
                book_slug=book_slug,
                oss_prefix=oss_prefix,
                watch_paths=watch_paths,
                session_path_hash="",
            )
            transient = bool(getattr(msg, "transient", False))

        scan = self._file_sync_manager.scan_task_artifacts(
            task_id,
            mapping=mapping,
            book_slug=getattr(msg, "book_slug", None),
            cwd=getattr(msg, "cwd", None),
            watch_paths=getattr(msg, "watch_paths", None),
        )
        manifest_key = f"{mapping.oss_prefix.rstrip('/')}/_sync_manifest.json"
        if not scan.delivery_found:
            await self._send_event(ForceSyncResultMessage(
                task_id=task_id,
                request_id=getattr(msg, "request_id", None),
                files_synced=0,
                files_attempted=0,
                success=False,
                error="delivery_not_found",
                delivery_found=False,
                manifest_key=manifest_key,
            ))
            return

        try:
            count = await self._file_sync_manager.force_sync_mapping(mapping, transient=transient)
            logger.info("Force-synced %d files for task %s", count, task_id)
            await self._send_event(ForceSyncResultMessage(
                task_id=task_id,
                request_id=getattr(msg, "request_id", None),
                files_synced=count,
                files_attempted=count,
                success=True,
                delivery_found=True,
                delivery_path=scan.delivery_path,
                manifest_key=manifest_key,
                manuscript_path=scan.manuscript_path,
            ))
        except Exception as exc:
            logger.exception("Force sync failed for task %s", task_id)
            await self._send_event(ForceSyncResultMessage(
                task_id=task_id,
                request_id=getattr(msg, "request_id", None),
                files_synced=0,
                files_attempted=0,
                success=False,
                error=str(exc),
                delivery_found=scan.delivery_found,
                delivery_path=scan.delivery_path,
                manifest_key=manifest_key,
                manuscript_path=scan.manuscript_path,
            ))

    async def _on_file_synced(
        self, task_id: str, path: str, oss_key: str, synced_at: str, md5: str
    ) -> None:
        await self._send_event(FileSyncedMessage(
            task_id=task_id,
            path=path,
            oss_key=oss_key,
            synced_at=_utcnow(),
            md5=md5,
        ))

    async def _on_file_changed(self, task_id: str, path: str, event_type: str) -> None:
        await self._send_event(FileChangeMessage(
            path=path,
            event=event_type,
        ))

    # ---- Quota checking ----

    async def _start_quota_checker(self) -> None:
        from elastic_agent.worker.quota_checker import QuotaChecker

        initial_slots = self._discover_credential_slots()
        self._quota_checker = QuotaChecker(
            active_slots=initial_slots,
            on_quota_status=self._on_quota_status,
        )
        await self._quota_checker.start()

    def _discover_credential_slots(self) -> list[dict[str, str]]:
        """Scan well-known credential dirs for existing valid credentials."""
        from elastic_agent.core.claude_oauth import read_credentials

        slots: list[dict[str, str]] = []
        candidate_dirs = [
            "/root/.claude-prod",
            *[f"/root/.claude-edit-{i}" for i in range(1, 10)],
        ]
        for config_dir in candidate_dirs:
            if not Path(config_dir).is_dir():
                continue
            creds = read_credentials(config_dir)
            if creds and creds.get("accessToken"):
                account_id_file = Path(config_dir) / ".account_id"
                if account_id_file.exists():
                    account_id = account_id_file.read_text().strip()
                else:
                    account_id = Path(config_dir).name
                slots.append({"account_id": account_id, "config_dir": config_dir})
                logger.info("QuotaChecker: discovered slot %s at %s", account_id, config_dir)
        return slots

    async def _on_quota_status(self, status: dict) -> None:
        await self._send_event(QuotaStatusMessage(
            task_id="",
            account_id=status.get("account_id", ""),
            usage_percent=max(status.get("five_hour_pct", 0), status.get("seven_day_pct", 0)),
            five_hour_pct=status.get("five_hour_pct", 0.0),
            seven_day_pct=status.get("seven_day_pct", 0.0),
            available=status.get("available", True),
        ))

    async def _handle_credential_login(self, msg: CredentialLoginMessage) -> None:
        from elastic_agent.core.claude_oauth import (
            normalize_local_config_dir,
            restore_claude_credentials,
            snapshot_claude_credentials,
            write_credentials,
        )

        config_dir = normalize_local_config_dir(msg.config_dir)
        credentials = msg.credentials
        account_id = credentials.get("account_id", f"slot-{msg.slot_index}")

        try:
            credential_snapshot = snapshot_claude_credentials(config_dir)
        except Exception as exc:
            self._unsafe_credential_config_dirs.add(config_dir)
            logger.error(
                "Could not secure credential slot %s before update (%s)",
                config_dir,
                type(exc).__name__,
            )
            await self._send_event(CredentialLoginResultMessage(
                account_id=account_id,
                slot_index=msg.slot_index,
                success=False,
                error="Credential slot validation failed",
            ))
            return

        def rollback_error() -> BaseException | None:
            try:
                restore_claude_credentials(credential_snapshot)
            except BaseException as exc:
                self._unsafe_credential_config_dirs.add(config_dir)
                logger.error(
                    "Credential rollback could not be verified for %s (%s)",
                    config_dir,
                    type(exc).__name__,
                )
                return exc
            return None

        try:
            write_credentials(config_dir, credentials)
        except BaseException as exc:
            restore_error = rollback_error()
            if not isinstance(exc, Exception):
                if restore_error is not None:
                    raise AccountLoginCleanupError(
                        "Credential rollback could not be verified"
                    ) from restore_error
                raise
            await self._send_event(CredentialLoginResultMessage(
                account_id=account_id,
                slot_index=msg.slot_index,
                success=False,
                error=(
                    "Credential update failed and rollback could not be verified"
                    if restore_error is not None
                    else "Credential update failed; previous credentials restored"
                ),
            ))
            return
        logger.info("Wrote credentials for %s to %s", account_id, config_dir)

        # Credential swap is in-place: warm PTY sessions on this config_dir
        # still run under the OLD account and must not be hot-reused.
        if self._pty_backend is not None:
            try:
                await self._pty_backend.recycle_config_dir(config_dir)
            except BaseException as exc:
                restore_error = rollback_error()
                if not isinstance(exc, Exception):
                    if restore_error is not None:
                        raise AccountLoginCleanupError(
                            "Credential rollback could not be verified"
                        ) from restore_error
                    raise
                logger.error(
                    "Failed to recycle PTY sessions for %s (%s)",
                    config_dir,
                    type(exc).__name__,
                )
                await self._send_event(CredentialLoginResultMessage(
                    account_id=account_id,
                    slot_index=msg.slot_index,
                    success=False,
                    error=(
                        "Credential activation failed and rollback could not "
                        "be verified"
                        if restore_error is not None
                        else "Credential activation failed; previous "
                        "credentials restored"
                    ),
                ))
                return

        self._unsafe_credential_config_dirs.discard(config_dir)
        if self._quota_checker:
            self._quota_checker.add_slot(account_id, config_dir)

        await self._send_event(CredentialLoginResultMessage(
            account_id=account_id,
            slot_index=msg.slot_index,
            success=True,
        ))

    def _confirm_account_login_cleanup(self, msg: AccountLoginMessage) -> None:
        """Record exact proof that no delegated credential/process remains."""

        self._account_login_cleanup_confirmed[msg.login_request_id] = msg.account_id
        while (
            len(self._account_login_cleanup_confirmed)
            > _MAX_COMPLETED_LOGIN_RECORDS
        ):
            oldest = next(iter(self._account_login_cleanup_confirmed))
            self._account_login_cleanup_confirmed.pop(oldest, None)

    async def _handle_account_login(self, msg: AccountLoginMessage) -> None:
        """Run one worker-local login while keeping OTP commands receivable."""
        entered_login = False
        try:
            async with self._account_login_lock:
                entered_login = True
                if msg.agent_type == "codex":
                    await self._handle_codex_account_login(msg)
                else:
                    await self._handle_claude_account_login(msg)
        except asyncio.CancelledError:
            if not entered_login:
                # Cancellation while waiting behind another login touched no
                # browser, process, or credential and is therefore clean.
                self._confirm_account_login_cleanup(msg)
            raise

    async def _run_account_login_task(self, msg: AccountLoginMessage) -> None:
        """Convert every unexpected background failure into a safe result."""
        try:
            await self._handle_account_login(msg)
        except asyncio.CancelledError:
            raise
        except AccountLoginCleanupError as exc:
            if msg.agent_type == "claude":
                from elastic_agent.core.claude_oauth import (
                    normalize_local_config_dir,
                )

                self._unsafe_credential_config_dirs.add(
                    normalize_local_config_dir(msg.config_dir)
                )
            logger.error(
                "Account login cleanup could not be verified for request %s (%s)",
                msg.login_request_id,
                type(exc).__name__,
            )
            await self._send_event(AccountLoginResultMessage(
                login_request_id=msg.login_request_id,
                account_id=msg.account_id,
                slot_index=msg.slot_index,
                success=False,
                error="account login cleanup could not be verified",
                cleanup_complete=False,
            ))
        except Exception as exc:
            # Browser/subprocess exception strings may contain a URL or input
            # value. Keep both the log and correlated result deliberately
            # generic while ensuring the Manager does not wait for its timeout.
            logger.error(
                "Unexpected account login task failure for request %s (%s)",
                msg.login_request_id,
                type(exc).__name__,
            )
            await self._send_event(AccountLoginResultMessage(
                login_request_id=msg.login_request_id,
                account_id=msg.account_id,
                slot_index=msg.slot_index,
                success=False,
                error="account login failed unexpectedly",
                cleanup_complete=(
                    self._account_login_cleanup_confirmed.get(
                        msg.login_request_id
                    )
                    == msg.account_id
                ),
            ))

    async def _handle_claude_account_login(
        self, msg: AccountLoginMessage,
    ) -> None:
        """Worker-autonomous login: the Manager sends the account identity +
        接码 token; the worker runs the vendored login flow locally (Chrome/CDP
        on this machine) and the credentials are written here, never sent up.
        """
        from elastic_agent.core.claude_oauth import (
            ClaudeLoginCleanupError,
            ClaudeOAuthProvider,
            LoginResult,
            OAuthConfig,
            normalize_local_config_dir,
            restore_claude_credentials,
            snapshot_claude_credentials,
        )

        provider = ClaudeOAuthProvider()
        config_dir = normalize_local_config_dir(msg.config_dir)
        # worker_host=None → run the vendored perform_login in-process on this
        # worker (this IS the machine that owns the config_dir).
        config = OAuthConfig(
            account_id=msg.account_id,
            email=msg.email,
            email_token=msg.email_token,
            config_dir=config_dir,
            provider=msg.provider,
            worker_host=None,
            login_timeout=msg.login_timeout_seconds,
        )
        credential_snapshot = snapshot_claude_credentials(config_dir)
        committed = False
        cancelled_error: asyncio.CancelledError | None = None
        try:
            result = await provider.login(config)
            if result.success:
                logger.info(
                    "Account %s logged in on this worker (%s)",
                    msg.account_id,
                    config_dir,
                )
                identity_ok = await self._verify_config_identity(
                    config_dir, msg.email
                )
                if not identity_ok:
                    result.success = False
                    result.error = (
                        "Claude credentials are valid for a different or unknown "
                        "email than the selected account"
                    )
                # Warm the account so the first real PTY turn doesn't stall on
                # GrowthBook/onboarding, and verify the credentials are usable.
                warmup_ok = (
                    await self._warmup_config_dir(config_dir)
                    if result.success
                    else False
                )
                if result.success and not warmup_ok:
                    result.success = False
                    result.error = (
                        "Claude login produced credentials, but the credential "
                        "validation command failed"
                    )
                committed = result.success
        except asyncio.CancelledError as exc:
            cancelled_error = exc
        except ClaudeLoginCleanupError as exc:
            raise AccountLoginCleanupError(
                "Claude login process cleanup could not be verified"
            ) from exc
        except Exception as exc:
            logger.error(
                "Claude account login failed for %s (%s)",
                msg.account_id,
                type(exc).__name__,
            )
            result = LoginResult(
                success=False,
                account_id=msg.account_id,
                error="Claude login automation failed unexpectedly",
            )
        finally:
            if not committed:
                try:
                    restore_claude_credentials(credential_snapshot)
                except Exception as exc:
                    raise AccountLoginCleanupError(
                        "Claude credential rollback could not be verified"
                    ) from exc

        if cancelled_error is not None:
            self._confirm_account_login_cleanup(msg)
            raise cancelled_error

        # New credentials on this config_dir: warm PTY sessions there ran
        # under a different account and must not be hot-reused.
        if result.success and self._pty_backend is not None:
            try:
                await self._pty_backend.recycle_config_dir(config_dir)
            except Exception as exc:
                # A warm PTY still carries the previous account even though the
                # credential files now contain the newly selected account.
                # Reporting success here would let the next task run under the
                # wrong identity. Fail closed so an ordinary Worker/account is
                # quarantined (or an EIP Worker is destroyed).
                raise AccountLoginCleanupError(
                    "Claude PTY session cleanup could not be verified"
                ) from exc
        if result.success and self._quota_checker:
            self._quota_checker.add_slot(msg.account_id, config_dir)
        if result.success:
            self._unsafe_credential_config_dirs.discard(config_dir)
        if not result.success:
            self._confirm_account_login_cleanup(msg)

        await self._send_event(AccountLoginResultMessage(
            login_request_id=msg.login_request_id,
            account_id=msg.account_id,
            slot_index=msg.slot_index,
            success=result.success,
            error=result.error,
            cleanup_complete=True if not result.success else None,
        ))

    async def _handle_codex_account_login(
        self, msg: AccountLoginMessage,
    ) -> None:
        """Codex OAuth login in this worker's CODEX_HOME."""
        from elastic_agent.worker.login.codex_login import codex_login

        raw_home = Path(msg.config_dir).expanduser() if msg.config_dir else None
        codex_home = (
            raw_home
            if raw_home is not None and raw_home.is_absolute()
            else Path.home() / ".codex"
        )
        if not msg.password and not msg.email_token.strip():
            self._confirm_account_login_cleanup(msg)
            await self._send_event(AccountLoginResultMessage(
                login_request_id=msg.login_request_id,
                account_id=msg.account_id,
                slot_index=msg.slot_index,
                success=False,
                error="Codex login requires an email token or OpenAI password",
                cleanup_complete=True,
            ))
            return

        otp_reader = _WorkerLoginOtpReader(self, msg)
        self._account_login_otp_readers[msg.login_request_id] = otp_reader
        try:
            result = await codex_login(
                email=msg.email,
                password=msg.password,
                token_171=msg.email_token,
                codex_home=str(codex_home),
                mail_provider=msg.provider,
                attempt_id=msg.login_request_id,
                manual_otp_reader=otp_reader,
                timeout=msg.login_timeout_seconds,
            )
        except asyncio.CancelledError:
            # codex_login only lets cancellation escape after its transactional
            # finally has stopped the CLI/browser and restored auth.json.
            self._confirm_account_login_cleanup(msg)
            raise
        except Exception as exc:
            logger.error(
                "Codex account login failed for %s (%s)",
                msg.account_id,
                type(exc).__name__,
            )
            await self._send_event(AccountLoginResultMessage(
                login_request_id=msg.login_request_id,
                account_id=msg.account_id,
                slot_index=msg.slot_index,
                success=False,
                error="Codex login automation failed unexpectedly",
                cleanup_complete=False,
            ))
            return
        finally:
            self._account_login_otp_readers.pop(msg.login_request_id, None)

        success = bool(result.get("ok"))
        if success:
            logger.info(
                "Codex account %s logged in and validated on this worker (%s)",
                msg.account_id,
                codex_home,
            )
        else:
            self._confirm_account_login_cleanup(msg)
        failure_kind = result.get("failure_kind")
        if (
            success
            or failure_kind not in {"hard_quota", "auth_failure"}
        ):
            failure_kind = None
        await self._send_event(AccountLoginResultMessage(
            login_request_id=msg.login_request_id,
            account_id=msg.account_id,
            slot_index=msg.slot_index,
            success=success,
            error=None if success else str(result.get("error") or "Codex login failed"),
            failure_kind=failure_kind,
            cleanup_complete=True if not success else None,
        ))

    async def _handle_account_login_otp(
        self, msg: AccountLoginOtpMessage,
    ) -> None:
        """Deliver a correlated OTP to a live Codex browser without logging it."""
        reader = self._account_login_otp_readers.get(msg.login_request_id)
        if reader is None or not reader.submit(msg):
            await self._send_event(ErrorMessage(
                error_type="invalid_account_login_otp",
                message="Login verification challenge is stale or mismatched",
                recoverable=True,
            ))

    async def _handle_account_login_cancel(
        self, msg: AccountLoginCancelMessage,
    ) -> None:
        """Cancel one login and acknowledge only after its cleanup finishes."""
        task = self._account_login_tasks.get(msg.login_request_id)
        account_id = self._account_login_accounts.get(msg.login_request_id)
        cleanup_complete = (
            self._account_login_cleanup_confirmed.get(msg.login_request_id)
            == msg.account_id
        )
        if task is not None and not task.done() and account_id == msg.account_id:
            logger.info(
                "Cancelling account login request %s (%s)",
                msg.login_request_id,
                msg.reason,
            )
            task.cancel()
            try:
                await asyncio.wait_for(asyncio.shield(task), timeout=60.0)
            except asyncio.CancelledError:
                # A finished child raises CancelledError through ``shield``.
                # If the receiver itself was cancelled on WS shutdown, do not
                # claim that the still-running child's cleanup is complete.
                if not task.done():
                    raise
            except asyncio.TimeoutError:
                cleanup_complete = False
            except Exception:
                # A completed task has run its transactional rollback even if
                # its handler failed. The wrapper has already redacted/logged it.
                pass
            cleanup_complete = (
                task.done()
                and self._account_login_cleanup_confirmed.get(
                    msg.login_request_id
                )
                == msg.account_id
            )

        await self._send_event(AccountLoginCancelledMessage(
            login_request_id=msg.login_request_id,
            account_id=msg.account_id,
            cleanup_complete=cleanup_complete,
        ))

    @staticmethod
    async def _stop_process_group(
        proc: asyncio.subprocess.Process,
        *,
        grace_seconds: float = 5.0,
    ) -> None:
        """Terminate and reap an isolated validation subprocess group."""
        if proc.returncode is not None:
            return
        try:
            os.killpg(proc.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            await asyncio.wait_for(proc.wait(), timeout=grace_seconds)
            return
        except asyncio.TimeoutError:
            pass
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        await proc.wait()

    async def _verify_config_identity(
        self,
        config_dir: str,
        expected_email: str,
        *,
        timeout: float = 30.0,
    ) -> bool:
        """Require Claude's authenticated email to match the selected account."""
        env = {**os.environ, "CLAUDE_CONFIG_DIR": config_dir}
        if hasattr(os, "geteuid") and os.geteuid() == 0:
            env.setdefault("IS_SANDBOX", "1")
        proc: asyncio.subprocess.Process | None = None
        try:
            proc = await asyncio.create_subprocess_exec(
                "claude", "auth", "status",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
                env=env,
                start_new_session=True,
            )
            stdout, _ = await asyncio.wait_for(
                proc.communicate(), timeout=timeout
            )
            if proc.returncode != 0:
                return False
            output = stdout.decode(errors="replace").strip()
            logged_in = False
            actual_email: str | None = None
            try:
                status = json.loads(output)
                logged_in = bool(status.get("loggedIn"))
                email = status.get("email")
                if isinstance(email, str):
                    actual_email = email.strip()
            except json.JSONDecodeError:
                logged_in = (
                    "Logged in" in output or '"loggedIn": true' in output
                )
                match = re.search(
                    r"[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}", output
                )
                if match:
                    actual_email = match.group(0)
            matches = bool(
                logged_in
                and actual_email
                and actual_email.casefold()
                == expected_email.strip().casefold()
            )
            if not matches:
                logger.warning(
                    "Claude auth identity mismatch for %s: expected %s, got %s",
                    config_dir,
                    expected_email,
                    actual_email or "<unknown>",
                )
            return matches
        except asyncio.CancelledError:
            if proc is not None:
                await self._stop_process_group(proc)
            raise
        except Exception:
            if proc is not None:
                await self._stop_process_group(proc)
            logger.exception("Could not verify Claude auth identity for %s", config_dir)
            return False

    async def _warmup_config_dir(
        self, config_dir: str, *, timeout: float = 60.0
    ) -> bool:
        """Run and verify ``claude -p`` for a newly logged-in config dir.

        A fresh account's first turn otherwise pays for GrowthBook cache
        population + onboarding; a short headless run primes it and confirms
        the credentials actually work. A non-zero exit/timeout is a login
        failure, not a successful warmup. Timed-out process groups are always
        terminated and reaped so they cannot survive into the Job.
        """
        env = {**os.environ, "CLAUDE_CONFIG_DIR": config_dir}
        if hasattr(os, "geteuid") and os.geteuid() == 0:
            env.setdefault("IS_SANDBOX", "1")
        proc: asyncio.subprocess.Process | None = None

        try:
            proc = await asyncio.create_subprocess_exec(
                "claude", "-p", "reply: ok", "--dangerously-skip-permissions",
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
                env=env,
                start_new_session=True,
            )
            return_code = await asyncio.wait_for(proc.wait(), timeout=timeout)
            if return_code != 0:
                logger.warning(
                    "Credential validation for %s exited with code %s",
                    config_dir,
                    return_code,
                )
                return False
            return True
        except asyncio.CancelledError:
            if proc is not None:
                await self._stop_process_group(proc)
            raise
        except Exception:
            if proc is not None:
                await self._stop_process_group(proc)
            logger.exception(
                "Credential validation for %s did not complete cleanly",
                config_dir,
            )
            return False

    # ---- Heartbeat ----

    async def _heartbeat_loop(self) -> None:
        while self._running and self._ws:
            uptime = int(time.monotonic() - self._start_time)
            await self._send_event(HeartbeatMessage(uptime_seconds=uptime))
            if (
                self._task_supervisor is not None
                and not self._process_inventory_complete
            ):
                await self._recover_supervised_task_inventory()
                await self._send_status()
            await asyncio.sleep(self._heartbeat_interval)

    # ---- Send helper ----

    def _load_supervised_offsets(self) -> dict[str, int]:
        """Load the durable supervisor spool cursor.

        Replaying from zero is safe but can take hours for a multi-gigabyte
        short-line spool and delays terminal collection.  A corrupt cursor is
        therefore ignored (safe replay) rather than trusted to skip output.
        """

        try:
            if not self._supervised_offsets_path.is_file():
                return {}
            tighten_state_file(self._supervised_offsets_path)
            payload = json.loads(
                self._supervised_offsets_path.read_text(encoding="utf-8")
            )
            offsets = payload.get("offsets") if isinstance(payload, dict) else None
            if (
                set(payload) != {"version", "offsets"}
                or payload["version"] != _SUPERVISED_OFFSETS_VERSION
                or not isinstance(offsets, dict)
                or len(offsets) > _MAX_SUPERVISED_OFFSETS
                or any(
                    not isinstance(task_id, str)
                    or not task_id
                    or len(task_id) > 256
                    or not isinstance(offset, int)
                    or isinstance(offset, bool)
                    or offset < 0
                    or offset > (1 << 63) - 1
                    for task_id, offset in offsets.items()
                )
            ):
                raise ValueError("invalid supervised offset schema")
            return dict(offsets)
        except Exception:
            logger.exception(
                "Failed to load durable supervised-task offsets; "
                "replaying spools from the beginning"
            )
            return {}

    def _persist_supervised_offsets(self) -> None:
        secure_state_directory(self._log_dir)
        atomic_write_private(
            self._supervised_offsets_path,
            json.dumps(
                {
                    "version": _SUPERVISED_OFFSETS_VERSION,
                    "offsets": self._supervised_offsets,
                },
                sort_keys=True,
                separators=(",", ":"),
            ),
        )

    def _pending_outbox_process_exit_task_ids(self) -> set[str]:
        task_ids: set[str] = set()
        for data in self._reliable_events.values():
            try:
                payload = json.loads(data)
            except (json.JSONDecodeError, TypeError):
                continue
            if (
                payload.get("type") == "PROCESS_EXIT"
                and isinstance(payload.get("task_id"), str)
            ):
                task_ids.add(payload["task_id"])
        return task_ids

    def _load_reliable_events(self) -> dict[str, str]:
        """Load terminal events that were not acknowledged before a restart."""
        try:
            if not self._event_outbox_path.is_file():
                return {}
            payload = json.loads(self._event_outbox_path.read_text(encoding="utf-8"))
            events = payload.get("events", []) if isinstance(payload, dict) else []
            ordered: dict[str, str] = {}
            if isinstance(events, list):
                # v2 stores an explicit sequence.  Dict key sorting must never
                # be allowed to reorder RUN_EXHAUSTED and its later PROCESS_EXIT.
                for item in events:
                    if not isinstance(item, dict):
                        continue
                    event_id = item.get("event_id")
                    data = item.get("data")
                    if event_id and isinstance(data, str):
                        ordered[str(event_id)] = data
                return ordered
            if isinstance(events, dict):
                # v1 compatibility: retain the order present in the JSON object.
                # Older files may already have been key-sorted, but accepting
                # them prevents an upgrade from discarding terminal events.
                for event_id, data in events.items():
                    if event_id and isinstance(data, str):
                        ordered[str(event_id)] = data
                return ordered
            raise ValueError("event outbox events must be an ordered list or object")
        except Exception:
            logger.exception("Failed to load durable worker event outbox")
            return {}

    def _persist_reliable_events(self) -> None:
        secure_state_directory(self._log_dir)
        atomic_write_private(
            self._event_outbox_path,
            json.dumps(
                {
                    "version": 2,
                    "events": [
                        {"event_id": event_id, "data": data}
                        for event_id, data in self._reliable_events.items()
                    ],
                },
                sort_keys=True,
                separators=(",", ":"),
            ),
        )

    async def _replay_reliable_events(self) -> None:
        """Send fsynced terminal events before any queued STATUS on reconnect.

        Authentication has completed and the Manager can buffer ACK frames even
        though the receiver task starts immediately after this method returns.
        Direct sends preserve the required terminal-event-before-snapshot order;
        the durable outbox remains authoritative until those ACKs are handled.
        """
        async with self._event_outbox_lock:
            pending = list(self._reliable_events.values())
        for data in pending:
            if self._ws is None:
                return
            await self._ws.send(data)

    async def _pending_process_exit_task_ids(self) -> list[str]:
        """Snapshot exiting/unacknowledged task ids for STATUS reconciliation."""
        async with self._event_outbox_lock:
            pending = list(self._reliable_events.values())
            task_ids: set[str] = set(self._exiting_task_ids)
        # A replacement runtime can inventory a supervisor terminal before it
        # has replayed a large spool and persisted PROCESS_EXIT into its own
        # outbox. Keep that transition visible to the Manager.
        task_ids.update(
            task_id
            for task_id, descriptor in self._supervised_tasks.items()
            if descriptor.state == "terminal"
        )
        for data in pending:
            try:
                payload = json.loads(data)
            except (json.JSONDecodeError, TypeError):
                continue
            if payload.get("type") == "PROCESS_EXIT" and payload.get("task_id"):
                task_ids.add(str(payload["task_id"]))
        return sorted(task_ids)

    async def _mark_task_exiting(self, task_id: str) -> None:
        async with self._event_outbox_lock:
            self._exiting_task_ids.add(task_id)

    async def _send_process_exit(self, msg: ProcessExitMessage) -> None:
        """Bridge active-process removal to a durable PROCESS_EXIT atomically.

        ``_send_event`` fsyncs the event while holding ``_event_outbox_lock``.
        We clear the transient marker only after that durable map contains this
        exact event, so every STATUS snapshot sees either ``exiting`` or an
        unacknowledged PROCESS_EXIT (and briefly both), never neither.
        """
        await self._mark_task_exiting(msg.task_id)
        try:
            await self._send_event(msg)
        except BaseException:
            # The event is neither durable nor deliverable.  Do not advertise a
            # phantom pending exit forever; reconnect STATUS must let the
            # Manager's lost-task reconciliation take over.
            async with self._event_outbox_lock:
                self._exiting_task_ids.discard(msg.task_id)
            raise
        async with self._event_outbox_lock:
            if msg.event_id in self._reliable_events:
                self._exiting_task_ids.discard(msg.task_id)

    async def _handle_event_ack(self, msg: EventAckMessage) -> None:
        # Keep the runtime outbox authoritative until the independent
        # supervisor has also durably accepted the ACK.  If its Unix socket is
        # temporarily unavailable, replaying the same Manager-deduped event is
        # the retry mechanism; dropping our outbox entry first would leak the
        # supervisor terminal forever in an otherwise healthy runtime.
        async with self._event_outbox_lock:
            removed = self._reliable_events.get(msg.event_id)

        supervisor_task_id = self._supervisor_event_task_ids.get(
            msg.event_id
        )
        if (
            self._task_supervisor is not None
            and supervisor_task_id is not None
        ):
            try:
                await self._task_supervisor.ack_event(msg.event_id)
            except Exception as exc:
                self._process_inventory_complete = False
                self._process_inventory_error = (
                    "independent task acknowledgement is pending"
                )
                logger.warning(
                    "Could not acknowledge supervisor event; retaining "
                    "runtime outbox for retry (%s)",
                    type(exc).__name__,
                )
                await self._force_reconnect_for_outbox_failure()
                return
            task_id = self._supervisor_event_task_ids.pop(
                msg.event_id,
                None,
            )
            descriptor = self._supervised_tasks.get(task_id or "")
            if (
                descriptor is not None
                and descriptor.pending_exhaustion is not None
                and descriptor.pending_exhaustion["event_id"] == msg.event_id
            ):
                self._supervised_tasks[task_id] = replace(
                    descriptor,
                    pending_exhaustion=None,
                )

        persist_error: Exception | None = None
        if removed is not None:
            async with self._event_outbox_lock:
                previous_events = dict(self._reliable_events)
                current = self._reliable_events.get(msg.event_id)
                if current is not None:
                    removed = self._reliable_events.pop(msg.event_id)
                    try:
                        self._persist_reliable_events()
                    except Exception as exc:
                        # The on-disk outbox still contains the event. Restore
                        # memory to the same truth so replay remains possible.
                        self._reliable_events = previous_events
                        persist_error = exc
        if persist_error is not None:
            await self._force_reconnect_for_outbox_failure()
            raise ReliableEventPersistenceError(
                "failed to durably acknowledge terminal event"
            ) from persist_error

        if removed is not None:
            try:
                payload = json.loads(removed)
            except (json.JSONDecodeError, TypeError):
                payload = {}
            task_id = payload.get("task_id")
            if (
                payload.get("type") == "PROCESS_EXIT"
                and isinstance(task_id, str)
                and task_id in self._supervised_offsets
            ):
                previous_offset = self._supervised_offsets.pop(task_id)
                try:
                    self._persist_supervised_offsets()
                except Exception:
                    # A stale cursor cannot resurrect an ACKed supervisor task,
                    # but keep memory aligned with the still-durable file and
                    # let the next complete inventory prune it.
                    self._supervised_offsets[task_id] = previous_offset
                    logger.warning(
                        "Could not remove acknowledged supervisor cursor",
                        exc_info=True,
                    )

    def _transport_reservation(self, kind: str) -> tuple[int, int]:
        reserved_bytes = 0
        reserved_frames = 0
        if self._retry_send is not None and self._retry_send_kind == kind:
            reserved_bytes += self._retry_send_bytes
            reserved_frames += 1
        if self._inflight_send_kind == kind:
            reserved_bytes += self._inflight_send_bytes
            reserved_frames += 1
        return reserved_bytes, reserved_frames

    @staticmethod
    def _drop_count_should_log(before: int, after: int) -> bool:
        return before == 0 or before.bit_length() != after.bit_length()

    def _record_transport_drops(self, kind: str, count: int) -> None:
        if count <= 0:
            return
        if kind == "log":
            before = self._dropped_log_frames
            self._dropped_log_frames += count
            after = self._dropped_log_frames
            if self._drop_count_should_log(before, after):
                logger.warning(
                    "Dropped %d Worker LOG transport frames because the Manager "
                    "link is slow or disconnected; local task logs remain intact",
                    after,
                )
            return
        if kind == "control":
            before = self._dropped_control_frames
            self._dropped_control_frames += count
            after = self._dropped_control_frames
            if self._drop_count_should_log(before, after):
                logger.warning(
                    "Dropped %d Worker control transport frames because the "
                    "Manager link is slow or disconnected; durable terminal "
                    "events remain in the worker outbox",
                    after,
                )
            return
        before = self._dropped_data_frames
        self._dropped_data_frames += count
        after = self._dropped_data_frames
        if self._drop_count_should_log(before, after):
            logger.warning(
                "Dropped %d Worker file-data transport frames because the "
                "Manager link is slow or disconnected",
                after,
            )

    async def _send_event(self, msg: Message) -> None:
        data = msg.model_dump_json()
        event_id = str(getattr(msg, "event_id", "") or "")
        if event_id:
            persist_error: Exception | None = None
            async with self._event_outbox_lock:
                previous = self._reliable_events.get(event_id)
                self._reliable_events[event_id] = data
                try:
                    self._persist_reliable_events()
                except Exception as exc:
                    if previous is None:
                        self._reliable_events.pop(event_id, None)
                    else:
                        self._reliable_events[event_id] = previous
                    persist_error = exc
            if persist_error is not None:
                logger.critical(
                    "Failed to persist reliable %s event (%s); forcing reconnect",
                    msg.type,
                    type(persist_error).__name__,
                )
                await self._force_reconnect_for_outbox_failure()
                raise ReliableEventPersistenceError(
                    f"failed to persist reliable {msg.type} event"
                ) from persist_error

        if isinstance(msg, LogMessage):
            serialized_bytes = len(data.encode("utf-8"))
            if serialized_bytes > _MAX_LOG_TRANSPORT_FRAME_BYTES:
                # The caller has already appended the byte-exact raw event to
                # the worker-local NDJSON.  Send only an explicit bounded marker
                # so a giant PTY/tool frame cannot multiply across the backlog.
                msg = LogMessage(
                    task_id=msg.task_id,
                    stream=msg.stream,
                    data=(
                        "[elastic-agent transport truncated: full raw frame is "
                        "available in the worker-local task log]"
                    ),
                    parsed={
                        "type": "elastic_transport_truncated",
                        "original_serialized_bytes": serialized_bytes,
                    },
                )
                data = msg.model_dump_json()
                self._truncated_log_frames += 1
                if self._truncated_log_frames & (
                    self._truncated_log_frames - 1
                ) == 0:
                    logger.warning(
                        "Truncated %d oversized Worker LOG transport frames; "
                        "local raw task logs remain intact",
                        self._truncated_log_frames,
                    )
            reserved_bytes, reserved_frames = self._transport_reservation("log")
            accepted, dropped, _ = self._log_send_queue.put_latest(
                data,
                reserved_bytes=reserved_bytes,
                reserved_frames=reserved_frames,
            )
            self._record_transport_drops(
                "log",
                dropped + (0 if accepted else 1),
            )
            if accepted:
                self._send_queue_ready.set()
            return

        if isinstance(msg, (FileChangeMessage, FileContentMessage)):
            serialized_bytes = len(data.encode("utf-8"))
            if serialized_bytes > _MAX_DATA_TRANSPORT_FRAME_BYTES:
                # File notifications/content are best-effort.  Do not turn
                # every omitted payload into an unbounded control message:
                # during a disconnected Manager link an attacker could flood
                # oversized events and exhaust worker memory through the
                # otherwise reliable/control queue.  The scalar drop counter
                # and its power-of-two local warnings are the bounded signal.
                self._record_transport_drops("data", 1)
                return
            reserved_bytes, reserved_frames = self._transport_reservation("data")
            accepted, dropped, _ = self._data_send_queue.put_latest(
                data,
                reserved_bytes=reserved_bytes,
                reserved_frames=reserved_frames,
            )
            self._record_transport_drops(
                "data",
                dropped + (0 if accepted else 1),
            )
            if accepted:
                self._send_queue_ready.set()
            return

        reserved_bytes, reserved_frames = self._transport_reservation("control")
        accepted, dropped, _ = self._send_queue.put_latest(
            data,
            reserved_bytes=reserved_bytes,
            reserved_frames=reserved_frames,
        )
        self._record_transport_drops(
            "control",
            dropped + (0 if accepted else 1),
        )
        if accepted:
            self._send_queue_ready.set()
        if dropped or (event_id and not accepted):
            # An evicted control frame may itself be a durable terminal event.
            # Reconnect so the fsynced outbox is replayed before the bounded
            # queues; best-effort control frames need no recovery.
            await self._force_reconnect_for_outbox_failure()

    async def _force_reconnect_for_outbox_failure(self) -> None:
        """Close the active socket so Manager receives a fresh STATUS snapshot."""

        self._reconnect_event.set()
        ws = self._ws
        if ws is None:
            return
        try:
            await ws.close(code=1011, reason="worker durable outbox failure")
        except TypeError:
            # Minimal/fake websocket implementations may accept no close args.
            await ws.close()
        except Exception:
            logger.exception("Failed to close websocket after outbox failure")
