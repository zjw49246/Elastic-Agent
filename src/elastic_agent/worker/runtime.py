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
import json
import logging
import os
import platform
import re
import shutil
import signal
import subprocess
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
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
from elastic_agent.core.rate_limit import is_auth_failure, is_rate_limited
from elastic_agent.core.secure_store import atomic_write_private, secure_state_directory

logger = logging.getLogger(__name__)

# After the run process exits, how long to keep draining stdout/stderr before
# giving up — a lingering child (e.g. a docker container from `--sandbox os`)
# can hold the pipe open so it never EOFs. Bounded so the exit is always reported.
_EXIT_DRAIN_TIMEOUT = 10.0


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
            raise RuntimeError(
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
    ) -> None:
        self._manager_url = manager_url
        self._auth_token = auth_token
        self._worker_id = worker_id
        self._heartbeat_interval = heartbeat_interval
        self._log_dir = Path(log_dir)
        self._event_outbox_path = self._log_dir / "event_outbox.json"
        self._event_outbox_lock = asyncio.Lock()
        self._reliable_events = self._load_reliable_events()
        # A WebSocket send can fail after dequeue.  Keep that exact frame ahead
        # of later STATUS/heartbeat frames across reconnects instead of putting
        # it at the tail and reordering lifecycle state.
        self._retry_send: str | None = None

        self._ws: Any = None
        self._authenticated = False
        self._running = False
        self._start_time = time.monotonic()

        self._processes: dict[str, asyncio.subprocess.Process] = {}
        self._process_tasks: dict[str, asyncio.Task] = {}
        self._stdin_pipes: dict[str, asyncio.StreamWriter | None] = {}
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

        self._send_queue: asyncio.Queue[str] = asyncio.Queue()
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

    @property
    def connected(self) -> bool:
        return self._ws is not None and self._authenticated

    @property
    def active_processes(self) -> list[str]:
        tasks = list(self._processes.keys())
        if self._pty_backend is not None:
            tasks.extend(self._pty_backend.active_tasks)
        return tasks

    async def run(self) -> None:
        """Main loop: connect, authenticate, handle messages. Reconnect on failure."""
        self._running = True
        self._start_time = time.monotonic()
        backoff = 1.0

        while self._running:
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
        login_tasks = list(self._account_login_tasks.values())
        for task in login_tasks:
            task.cancel()
        if login_tasks:
            await asyncio.gather(*login_tasks, return_exceptions=True)
        self._account_login_tasks.clear()
        self._account_login_accounts.clear()
        self._account_login_otp_readers.clear()
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
            if data is None:
                data = await self._send_queue.get()
            sent = False
            try:
                await self._ws.send(data)
                sent = True
                if self._retry_send == data:
                    self._retry_send = None
            except BaseException:
                # The queue item used to be lost as soon as get() returned.  A
                # disconnect/cancel at the send boundary must remain first;
                # reliable terminal events are additionally replayed from the
                # fsynced outbox and are safe to deliver more than once.
                if not sent:
                    self._retry_send = data
                raise

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
                        ))
                        return
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
            except Exception:
                logger.exception("Error handling %s message", msg.type)
                await self._send_event(ErrorMessage(
                    error_type="handler_error",
                    message=f"Failed to handle {msg.type}",
                    recoverable=True,
                ))
        else:
            logger.debug("Unhandled message type: %s", msg.type)

    # ---- Command handlers ----

    async def _handle_execute(self, msg: ExecuteMessage) -> None:
        task_id = msg.task_id
        if task_id in self._processes or (
            self._pty_backend is not None and self._pty_backend.has_task(task_id)
        ):
            await self._send_event(ErrorMessage(
                error_type="duplicate_task",
                message=f"Process already running for task {task_id}",
                recoverable=True,
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
        cwd = msg.cwd if msg.cwd else None

        try:
            proc = await asyncio.create_subprocess_exec(
                *msg.command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                stdin=asyncio.subprocess.PIPE,
                env=env,
                cwd=cwd,
            )
        except Exception as exc:
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
        self._stdin_pipes[task_id] = proc.stdin
        if getattr(msg, "watch_exhaustion", False):
            self._exhaustion_watch[task_id] = getattr(msg, "job_id", None) or ""
            self._exhaustion_fired.discard(task_id)
        logger.info("Started process for task %s (pid=%d)", task_id, proc.pid)

        task = asyncio.create_task(self._monitor_process(task_id, proc, log_path, msg.timeout))
        self._process_tasks[task_id] = task

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
                    proc.kill()
                    await self._wait_process_exit(proc, 5)

            # Best-effort drain of any buffered output, bounded for the same
            # reason (the pipe may be held open past exit).
            for _stream_task in (stdout_task, stderr_task):
                try:
                    await asyncio.wait_for(_stream_task, timeout=_EXIT_DRAIN_TIMEOUT)
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    _stream_task.cancel()

        finally:
            if log_file is not None:
                log_file.close()
            exit_code = proc.returncode if proc.returncode is not None else -1
            await self._mark_task_exiting(task_id)
            self._processes.pop(task_id, None)
            self._process_tasks.pop(task_id, None)
            self._stdin_pipes.pop(task_id, None)
            self._exhaustion_watch.pop(task_id, None)
            self._exhaustion_fired.discard(task_id)
            logger.info("Process for task %s exited with code %d", task_id, exit_code)

            if self._file_sync_manager:
                try:
                    synced = await self._file_sync_manager.force_sync(task_id)
                    logger.info("Force-synced %d files for task %s on process exit", synced, task_id)
                except Exception:
                    logger.exception("Failed to force-sync files for task %s on exit", task_id)

            await self._send_process_exit(
                ProcessExitMessage(task_id=task_id, exit_code=exit_code)
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

        # Workers run as root; claude refuses --dangerously-skip-permissions
        # under root unless it believes it's sandboxed. Cloud workers are
        # single-purpose VMs, so this is the intended unattended setup.
        if hasattr(os, "geteuid") and os.geteuid() == 0:
            env_overrides.setdefault("IS_SANDBOX", "1")

        try:
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

    async def _read_stream(
        self,
        task_id: str,
        stream: asyncio.StreamReader | None,
        stream_name: str,
        log_file: Any,
    ) -> None:
        if stream is None:
            return

        while True:
            try:
                line_bytes = await stream.readline()
            except Exception:
                break
            if not line_bytes:
                break

            line = line_bytes.decode("utf-8", errors="replace").rstrip("\n")
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

            await self._send_event(LogMessage(
                task_id=task_id,
                stream=stream_name,
                data=line,
                parsed=parsed,
            ))

            # Mode-B rotation (a): the opaque command consumes the Claude account
            # internally, so we can't rotate per turn — instead we watch its
            # output and, on the first exhaustion banner, interrupt + signal the
            # Manager to swap accounts and restart with --resume.
            if (
                task_id in self._exhaustion_watch
                and task_id not in self._exhaustion_fired
                and (is_rate_limited(line) or is_auth_failure(line))
            ):
                await self._signal_exhaustion(task_id)

    async def _signal_exhaustion(self, task_id: str) -> None:
        """Emit RunExhaustedMessage once and interrupt the run so the
        orchestrator can rotate the account and resume."""
        if task_id in self._exhaustion_fired:
            return
        self._exhaustion_fired.add(task_id)
        job_id = self._exhaustion_watch.get(task_id, "")
        logger.warning(
            "Task %s tripped exhaustion detector; interrupting before rotation", task_id
        )
        # Do not let the Manager dispatch a resumed command while the old
        # command is still consuming the same credential/config directory.
        # _monitor_process drains this stream and queues PROCESS_EXIT only after
        # this RUN_EXHAUSTED event, so task-id guards can discard that stale exit.
        proc = self._processes.get(task_id)
        await self._stop_process(task_id, "SIGINT")
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
        await self._send_event(RunExhaustedMessage(
            task_id=task_id,
            job_id=job_id,
            worker_id=self._worker_id or "unknown",
            reason="rate_limit",
        ))

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
        except (json.JSONDecodeError, TypeError):
            pass
        return None

    async def _handle_stop(self, msg: StopMessage) -> None:
        sig_name = msg.signal or "SIGTERM"
        if self._pty_backend is not None and self._pty_backend.has_task(msg.task_id):
            # PTY sessions interrupt via Esc + teardown; signals don't apply.
            try:
                await self._pty_backend.stop(msg.task_id)
            except Exception:
                logger.exception("Failed to stop PTY task %s", msg.task_id)
            return
        await self._stop_process(msg.task_id, sig_name)

    async def _stop_process(self, task_id: str, sig_name: str) -> None:
        proc = self._processes.get(task_id)
        if proc is None or proc.returncode is not None:
            return

        sig_map = {
            "SIGINT": signal.SIGINT,
            "SIGTERM": signal.SIGTERM,
            "SIGKILL": signal.SIGKILL,
        }
        sig = sig_map.get(sig_name, signal.SIGTERM)

        try:
            proc.send_signal(sig)
            logger.info("Sent %s to task %s (pid=%d)", sig_name, task_id, proc.pid)
        except ProcessLookupError:
            pass

        if sig != signal.SIGKILL:
            try:
                await asyncio.wait_for(proc.wait(), timeout=10.0)
                return
            except asyncio.TimeoutError:
                pass

            try:
                proc.send_signal(signal.SIGTERM)
                await asyncio.wait_for(proc.wait(), timeout=5.0)
                return
            except (asyncio.TimeoutError, ProcessLookupError):
                pass

            try:
                proc.kill()
            except ProcessLookupError:
                pass

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
        from elastic_agent.core.claude_oauth import write_credentials

        config_dir = msg.config_dir
        credentials = msg.credentials
        account_id = credentials.get("account_id", f"slot-{msg.slot_index}")

        write_credentials(config_dir, credentials)
        logger.info("Wrote credentials for %s to %s", account_id, config_dir)

        # Credential swap is in-place: warm PTY sessions on this config_dir
        # still run under the OLD account and must not be hot-reused.
        if self._pty_backend is not None:
            try:
                await self._pty_backend.recycle_config_dir(config_dir)
            except Exception:
                logger.exception(
                    "Failed to recycle PTY sessions for %s", config_dir
                )

        if self._quota_checker:
            self._quota_checker.add_slot(account_id, config_dir)

        await self._send_event(CredentialLoginResultMessage(
            account_id=account_id,
            slot_index=msg.slot_index,
            success=True,
        ))

    async def _handle_account_login(self, msg: AccountLoginMessage) -> None:
        """Run one worker-local login while keeping OTP commands receivable."""
        async with self._account_login_lock:
            if msg.agent_type == "codex":
                await self._handle_codex_account_login(msg)
            else:
                await self._handle_claude_account_login(msg)

    async def _run_account_login_task(self, msg: AccountLoginMessage) -> None:
        """Convert every unexpected background failure into a safe result."""
        try:
            await self._handle_account_login(msg)
        except asyncio.CancelledError:
            raise
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
            ))

    async def _handle_claude_account_login(
        self, msg: AccountLoginMessage,
    ) -> None:
        """Worker-autonomous login: the Manager sends the account identity +
        接码 token; the worker runs the vendored login flow locally (Chrome/CDP
        on this machine) and the credentials are written here, never sent up.
        """
        from elastic_agent.core.claude_oauth import (
            ClaudeOAuthProvider,
            OAuthConfig,
            normalize_local_config_dir,
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
        )
        try:
            result = await provider.login(config)
        except Exception as exc:
            logger.exception("Account login failed for %s", msg.account_id)
            await self._send_event(AccountLoginResultMessage(
                login_request_id=msg.login_request_id,
                account_id=msg.account_id, slot_index=msg.slot_index,
                success=False, error=str(exc),
            ))
            return

        if result.success:
            logger.info("Account %s logged in on this worker (%s)",
                        msg.account_id, config_dir)
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
            # New credentials on this config_dir: warm PTY sessions there ran
            # under a different account and must not be hot-reused.
            if result.success and self._pty_backend is not None:
                try:
                    await self._pty_backend.recycle_config_dir(config_dir)
                except Exception:
                    logger.exception(
                        "Failed to recycle PTY sessions for %s", config_dir
                    )
            if result.success and self._quota_checker:
                self._quota_checker.add_slot(msg.account_id, config_dir)

        await self._send_event(AccountLoginResultMessage(
            login_request_id=msg.login_request_id,
            account_id=msg.account_id,
            slot_index=msg.slot_index,
            success=result.success,
            error=result.error,
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
            await self._send_event(AccountLoginResultMessage(
                login_request_id=msg.login_request_id,
                account_id=msg.account_id,
                slot_index=msg.slot_index,
                success=False,
                error="Codex login requires an email token or OpenAI password",
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
        await self._send_event(AccountLoginResultMessage(
            login_request_id=msg.login_request_id,
            account_id=msg.account_id,
            slot_index=msg.slot_index,
            success=success,
            error=None if success else str(result.get("error") or "Codex login failed"),
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
        cleanup_complete = account_id in (None, msg.account_id)
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
            await asyncio.sleep(self._heartbeat_interval)

    # ---- Send helper ----

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
        await self._send_event(msg)
        async with self._event_outbox_lock:
            if msg.event_id in self._reliable_events:
                self._exiting_task_ids.discard(msg.task_id)

    async def _handle_event_ack(self, msg: EventAckMessage) -> None:
        async with self._event_outbox_lock:
            if self._reliable_events.pop(msg.event_id, None) is None:
                return
            self._persist_reliable_events()

    async def _send_event(self, msg: Message) -> None:
        try:
            data = msg.model_dump_json()
            event_id = getattr(msg, "event_id", "")
            if event_id:
                async with self._event_outbox_lock:
                    self._reliable_events[str(event_id)] = data
                    self._persist_reliable_events()
            await self._send_queue.put(data)
        except Exception:
            # Critical events must never be silently discarded.  The process
            # monitor cannot retry an exit later, so surface persistence/queue
            # failures loudly while preserving the local process log.
            logger.exception("Failed to queue message of type %s", msg.type)
