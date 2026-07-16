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
import signal
import shutil
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import websockets
import websockets.exceptions

from elastic_agent.core.protocols.messages import (
    AccountLoginMessage,
    AccountLoginResultMessage,
    AuthMessage,
    AuthResultMessage,
    CredentialLoginMessage,
    CredentialLoginResultMessage,
    ErrorMessage,
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
    UploadFileMessage,
    WatchFilesMessage,
    UnwatchMessage,
    parse_message,
)

from elastic_agent.core.rate_limit import is_auth_failure, is_rate_limited

logger = logging.getLogger(__name__)

# After the run process exits, how long to keep draining stdout/stderr before
# giving up — a lingering child (e.g. a docker container from `--sandbox os`)
# can hold the pipe open so it never EOFs. Bounded so the exit is always reported.
_EXIT_DRAIN_TIMEOUT = 10.0


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


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

        self._ws: Any = None
        self._authenticated = False
        self._running = False
        self._start_time = time.monotonic()

        self._processes: dict[str, asyncio.subprocess.Process] = {}
        self._process_tasks: dict[str, asyncio.Task] = {}
        self._stdin_pipes: dict[str, asyncio.StreamWriter | None] = {}

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
            data = await self._send_queue.get()
            try:
                await self._ws.send(data)
            except websockets.exceptions.ConnectionClosed:
                raise

    async def _receiver_loop(self) -> None:
        async for raw in self._ws:
            try:
                msg = parse_message(raw)
            except Exception:
                logger.warning("Failed to parse message: %s", raw[:200] if isinstance(raw, str) else raw[:200])
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
        }
        handler = handlers.get(msg.type)
        if handler:
            try:
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
            await self._send_event(ProcessExitMessage(
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
        log_file = open(log_path, "a", encoding="utf-8")
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
            log_file.close()
            exit_code = proc.returncode if proc.returncode is not None else -1
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

            await self._send_event(ProcessExitMessage(task_id=task_id, exit_code=exit_code))

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
            await self._send_event(ProcessExitMessage(
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

        await self._send_event(ProcessExitMessage(
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
            "Task %s tripped exhaustion detector; signaling manager + interrupting", task_id
        )
        await self._send_event(RunExhaustedMessage(
            task_id=task_id,
            job_id=job_id,
            worker_id=self._worker_id or "unknown",
            reason="rate_limit",
        ))
        await self._stop_process(task_id, "SIGINT")

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

        claude = self._check_claude_cli()
        runtime_ready = bool(claude["ok"])

        await self._send_event(StatusMessage(
            cpu=round(cpu, 1),
            mem=round(mem, 1),
            disk=round(disk, 1),
            active_processes=self.active_processes,
            runtime_ready=runtime_ready,
            runtime_error=None if runtime_ready else claude["error"],
            claude_cli_ok=bool(claude["ok"]),
            claude_version=claude["version"],
            claude_path=claude["path"],
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
        """Worker-autonomous login: the Manager sends the account identity +
        接码 token; the worker runs the vendored login flow locally (Chrome/CDP
        on this machine) and the credentials are written here, never sent up.
        """
        from elastic_agent.core.claude_oauth import ClaudeOAuthProvider, OAuthConfig

        provider = ClaudeOAuthProvider()
        # worker_host=None → run the vendored perform_login in-process on this
        # worker (this IS the machine that owns the config_dir).
        config = OAuthConfig(
            account_id=msg.account_id,
            email=msg.email,
            email_token=msg.email_token,
            config_dir=msg.config_dir,
            provider=msg.provider,
            worker_host=None,
        )
        try:
            result = await provider.login(config)
        except Exception as exc:
            logger.exception("Account login failed for %s", msg.account_id)
            await self._send_event(AccountLoginResultMessage(
                account_id=msg.account_id, slot_index=msg.slot_index,
                success=False, error=str(exc),
            ))
            return

        if result.success:
            logger.info("Account %s logged in on this worker (%s)",
                        msg.account_id, msg.config_dir)
            # Warm the account so the first real PTY turn doesn't stall on
            # GrowthBook/onboarding, and verify the credentials are usable.
            await self._warmup_config_dir(msg.config_dir)
            # New credentials on this config_dir: warm PTY sessions there ran
            # under a different account and must not be hot-reused.
            if self._pty_backend is not None:
                try:
                    await self._pty_backend.recycle_config_dir(msg.config_dir)
                except Exception:
                    logger.exception(
                        "Failed to recycle PTY sessions for %s", msg.config_dir
                    )
            if self._quota_checker:
                self._quota_checker.add_slot(msg.account_id, msg.config_dir)

        await self._send_event(AccountLoginResultMessage(
            account_id=msg.account_id,
            slot_index=msg.slot_index,
            success=result.success,
            error=result.error,
        ))

    async def _warmup_config_dir(self, config_dir: str) -> None:
        """Best-effort `claude -p` warmup for a config_dir after login.

        A fresh account's first turn otherwise pays for GrowthBook cache
        population + onboarding; a short headless run primes it and confirms
        the credentials actually work. Failures are non-fatal.
        """
        env = {**os.environ, "CLAUDE_CONFIG_DIR": config_dir}
        if hasattr(os, "geteuid") and os.geteuid() == 0:
            env.setdefault("IS_SANDBOX", "1")
        try:
            proc = await asyncio.create_subprocess_exec(
                "claude", "-p", "reply: ok", "--dangerously-skip-permissions",
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
                env=env,
            )
            await asyncio.wait_for(proc.wait(), timeout=60)
        except Exception:
            logger.debug("Warmup run for %s did not complete cleanly", config_dir)

    # ---- Heartbeat ----

    async def _heartbeat_loop(self) -> None:
        while self._running and self._ws:
            uptime = int(time.monotonic() - self._start_time)
            await self._send_event(HeartbeatMessage(uptime_seconds=uptime))
            await asyncio.sleep(self._heartbeat_interval)

    # ---- Send helper ----

    async def _send_event(self, msg: Message) -> None:
        try:
            await self._send_queue.put(msg.model_dump_json())
        except Exception:
            logger.debug("Failed to queue message of type %s", msg.type)
