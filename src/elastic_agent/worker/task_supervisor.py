"""Independent, durable execution plane for opaque Mode-B commands.

``ea-runtime`` is a reconnectable control plane and must not be the parent of a
multi-hour Job.  This module runs as a separate systemd service, owns each
command's process group/stdin/stdout/stderr/timeout, and exposes a private Unix
socket.  A replacement runtime can inventory tasks, replay their 0600 spool,
and publish a terminal record with the same event id.

Security boundary:

* command and environment are accepted only in the one-shot ``launch`` RPC and
  are never written to descriptors or terminal records;
* the socket and every state/log file are owner-only;
* errors returned over RPC are deliberately generic, because spawn exceptions
  may contain argv or environment-derived paths.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import binascii
import codecs
import errno
import hashlib
import json
import logging
import os
import re
import signal
import stat
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

from elastic_agent.core.secure_store import (
    atomic_write_private,
    fsync_directory,
    secure_state_directory,
    tighten_state_file,
)

logger = logging.getLogger(__name__)

_SCHEMA_VERSION = 1
_MAX_RPC_BYTES = 32 * 1024 * 1024
_MAX_LOG_FRAME_BYTES = 64 * 1024
_MAX_SPOOL_RECORD_BYTES = _MAX_LOG_FRAME_BYTES * 8 + 4096
_DEFAULT_MAX_SPOOL_BYTES = 2 * 1024 * 1024 * 1024
_TERMINAL_RESERVE_BYTES = 256 * 1024
_TERMINAL_COMMIT_ATTEMPTS = 5
_MAX_POLL_RECORDS = 16
_MAX_POLL_BYTES = 1024 * 1024
_MAX_ACKED_TASK_TOMBSTONES = 4096
_EXIT_DRAIN_TIMEOUT = 10.0
_TASK_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:@+-]{0,255}$")
_SIGNALS = {
    "SIGINT": signal.SIGINT,
    "SIGTERM": signal.SIGTERM,
    "SIGKILL": signal.SIGKILL,
}


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class TaskSupervisorError(RuntimeError):
    """A private supervisor operation failed without exposing launch inputs."""


@dataclass(frozen=True)
class SupervisedTaskLaunch:
    """Ephemeral launch inputs.

    ``command`` and ``env`` intentionally have no corresponding fields in
    :class:`SupervisedTaskDescriptor`.
    """

    task_id: str
    command: list[str]
    cwd: str
    env: dict[str, str]
    timeout_seconds: int | None
    job_id: str = ""
    watch_exhaustion: bool = False
    agent_api_provider: str | None = None
    agent_type: str | None = None


@dataclass(frozen=True)
class SupervisedTaskDescriptor:
    schema_version: int
    task_id: str
    state: Literal["running", "terminal"]
    pid: int
    pgid: int
    pid_start_ticks: int
    started_at: str
    deadline_at: float | None
    timeout_seconds: int | None
    job_id: str
    watch_exhaustion: bool
    agent_api_provider: str | None
    agent_type: str | None
    terminal_event_id: str
    pending_exhaustion: dict[str, str] | None = None

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "SupervisedTaskDescriptor":
        allowed = {field.name for field in cls.__dataclass_fields__.values()}
        if set(value) != allowed:
            raise TaskSupervisorError("invalid task descriptor schema")
        descriptor = cls(**value)
        if (
            not isinstance(descriptor.schema_version, int)
            or isinstance(descriptor.schema_version, bool)
            or descriptor.schema_version != _SCHEMA_VERSION
            or not _valid_task_id(descriptor.task_id)
            or descriptor.state not in {"running", "terminal"}
            or not isinstance(descriptor.pid, int)
            or isinstance(descriptor.pid, bool)
            or descriptor.pid <= 1
            or not isinstance(descriptor.pgid, int)
            or descriptor.pgid != descriptor.pid
            or not isinstance(descriptor.pid_start_ticks, int)
            or descriptor.pid_start_ticks < 0
            or (
                os.name == "posix"
                and descriptor.pid_start_ticks == 0
            )
            or not isinstance(descriptor.started_at, str)
            or (
                descriptor.deadline_at is not None
                and not isinstance(descriptor.deadline_at, (int, float))
            )
            or (
                descriptor.timeout_seconds is not None
                and (
                    not isinstance(descriptor.timeout_seconds, int)
                    or isinstance(descriptor.timeout_seconds, bool)
                    or descriptor.timeout_seconds <= 0
                )
            )
            or not isinstance(descriptor.job_id, str)
            or len(descriptor.job_id) > 256
            or not isinstance(descriptor.watch_exhaustion, bool)
            or descriptor.agent_api_provider not in {None, "cloudrouter", "apex"}
            or descriptor.agent_type not in {None, "claude", "codex"}
            or not isinstance(descriptor.terminal_event_id, str)
            or not re.fullmatch(
                r"[A-Za-z0-9_-]{8,128}",
                descriptor.terminal_event_id,
            )
        ):
            raise TaskSupervisorError("invalid task descriptor")
        pending = descriptor.pending_exhaustion
        if pending is not None and (
            set(pending) != {"event_id", "reason"}
            or not pending["event_id"]
            or not pending["reason"]
        ):
            raise TaskSupervisorError("invalid pending exhaustion record")
        return descriptor


@dataclass(frozen=True)
class TaskPollSnapshot:
    records: list[dict[str, Any]]
    next_offset: int
    terminal: dict[str, Any] | None


@dataclass
class _ServerTask:
    descriptor: SupervisedTaskDescriptor
    directory: Path
    descriptor_path: Path
    terminal_path: Path
    terminal_reserve_path: Path
    spool_path: Path
    process: asyncio.subprocess.Process | None = None
    monitor_task: asyncio.Task[None] | None = None
    stdin_lock: asyncio.Lock | None = None
    signal_lock: asyncio.Lock | None = None
    spool_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    terminal: dict[str, Any] | None = None
    last_spool_fsync: float = 0.0
    deadline_monotonic: float | None = None


def _valid_task_id(task_id: str) -> bool:
    return bool(_TASK_ID_RE.fullmatch(task_id))


def _task_key(task_id: str) -> str:
    return hashlib.sha256(task_id.encode("utf-8")).hexdigest()


def _secure_directory(path: Path) -> Path:
    if path.is_symlink():
        raise TaskSupervisorError("private directory must not be a symlink")
    if path.exists() and not path.is_dir():
        raise TaskSupervisorError("private directory path is invalid")
    return secure_state_directory(path)


def _pid_start_ticks(pid: int) -> int:
    """Read Linux start ticks, used to reject PID reuse before signaling."""

    try:
        raw = Path(f"/proc/{pid}/stat").read_text()
        # ``comm`` is parenthesized and may itself contain spaces. Fields after
        # the final ")" start at kernel stat field 3; starttime is field 22.
        stat_fields = raw.rsplit(")", 1)[1].strip().split()
        return int(stat_fields[19])
    except (OSError, ValueError, IndexError):
        return 0


def _json_line(value: dict[str, Any]) -> bytes:
    return (
        json.dumps(value, separators=(",", ":"), ensure_ascii=False) + "\n"
    ).encode("utf-8")


def _write_all(fd: int, data: bytes) -> None:
    remaining = memoryview(data)
    while remaining:
        written = os.write(fd, remaining)
        if written <= 0:
            raise OSError("short task spool write")
        remaining = remaining[written:]


class TaskSupervisorClient:
    """Short-lived RPC client used by each replacement WorkerRuntime."""

    def __init__(self, socket_path: str | Path) -> None:
        self.socket_path = Path(socket_path)

    async def _request(self, payload: dict[str, Any]) -> dict[str, Any]:
        try:
            reader, writer = await asyncio.open_unix_connection(
                str(self.socket_path),
                limit=_MAX_RPC_BYTES + 1,
            )
        except (OSError, ValueError) as exc:
            raise TaskSupervisorError("task supervisor is unavailable") from exc
        try:
            encoded = _json_line(payload)
            if len(encoded) > _MAX_RPC_BYTES:
                raise TaskSupervisorError("task supervisor request is too large")
            writer.write(encoded)
            await writer.drain()
            raw = await reader.readline()
            if not raw or len(raw) > _MAX_RPC_BYTES:
                raise TaskSupervisorError("invalid task supervisor response")
            response = json.loads(raw)
            if not isinstance(response, dict) or not response.get("ok"):
                raise TaskSupervisorError("task supervisor request failed")
            return response
        except (json.JSONDecodeError, UnicodeError) as exc:
            raise TaskSupervisorError("invalid task supervisor response") from exc
        finally:
            writer.close()
            await writer.wait_closed()

    async def launch(
        self,
        launch: SupervisedTaskLaunch,
    ) -> SupervisedTaskDescriptor:
        response = await self._request({
            "op": "launch",
            "launch": asdict(launch),
        })
        return SupervisedTaskDescriptor.from_dict(response["task"])

    async def list_tasks(self) -> list[SupervisedTaskDescriptor]:
        response = await self._request({"op": "list"})
        raw_tasks = response.get("tasks")
        if not isinstance(raw_tasks, list):
            raise TaskSupervisorError("invalid task inventory")
        return [
            SupervisedTaskDescriptor.from_dict(item)
            for item in raw_tasks
        ]

    async def poll(self, task_id: str, *, offset: int) -> TaskPollSnapshot:
        response = await self._request({
            "op": "poll",
            "task_id": task_id,
            "offset": offset,
        })
        records = response.get("records")
        next_offset = response.get("next_offset")
        terminal = response.get("terminal")
        if (
            not isinstance(records, list)
            or not isinstance(next_offset, int)
            or next_offset < 0
            or (terminal is not None and not isinstance(terminal, dict))
        ):
            raise TaskSupervisorError("invalid task poll response")
        return TaskPollSnapshot(
            records=records,
            next_offset=next_offset,
            terminal=terminal,
        )

    async def signal(
        self,
        task_id: str,
        *,
        signal_name: str,
        scope: str = "group",
        escalate: bool = True,
    ) -> bool:
        response = await self._request({
            "op": "signal",
            "task_id": task_id,
            "signal": signal_name,
            "scope": scope,
            "escalate": escalate,
        })
        return bool(response.get("signalled"))

    async def write_stdin(self, task_id: str, payload: str) -> None:
        await self._request({
            "op": "stdin",
            "task_id": task_id,
            "payload": payload,
        })

    async def write_stdin_base64_once(
        self, task_id: str, payload_base64: str,
    ) -> None:
        """Write one opaque binary frame and close the task's stdin.

        The base64 envelope keeps secret-bearing bytes out of JSON decoding and
        log formatting paths.  The supervisor decodes only at the final pipe
        boundary and closes stdin so a one-shot reader cannot wait forever.
        """

        await self._request({
            "op": "stdin_base64_once",
            "task_id": task_id,
            "payload_base64": payload_base64,
        })

    async def mark_exhaustion(
        self,
        task_id: str,
        *,
        reason: str,
        event_id: str,
    ) -> None:
        await self._request({
            "op": "mark_exhaustion",
            "task_id": task_id,
            "reason": reason,
            "event_id": event_id,
        })

    async def ack_event(self, event_id: str) -> None:
        await self._request({"op": "ack_event", "event_id": event_id})


class TaskSupervisorServer:
    """Own Mode-B children independently from ``ea-runtime.service``."""

    def __init__(
        self,
        *,
        socket_path: str | Path,
        state_dir: str | Path,
        log_dir: str | Path,
        max_spool_bytes: int = _DEFAULT_MAX_SPOOL_BYTES,
    ) -> None:
        if max_spool_bytes <= _MAX_SPOOL_RECORD_BYTES:
            raise ValueError("max_spool_bytes is too small")
        self.socket_path = Path(socket_path)
        self.state_dir = Path(state_dir)
        self.log_dir = Path(log_dir)
        self.max_spool_bytes = max_spool_bytes
        self._server: asyncio.AbstractServer | None = None
        self._tasks: dict[str, _ServerTask] = {}
        self._tasks_lock = asyncio.Lock()
        self._tombstone_path = self.state_dir / "acked_tasks.json"
        self._completed_task_ids: dict[str, None] = {}
        self._fatal_error: str | None = None
        self._fatal_event = asyncio.Event()

    @property
    def fatal_event(self) -> asyncio.Event:
        return self._fatal_event

    @property
    def fatal_error(self) -> str | None:
        return self._fatal_error

    @property
    def running_task_ids(self) -> list[str]:
        return sorted(
            task_id
            for task_id, task in self._tasks.items()
            if task.descriptor.state == "running"
        )

    async def start(self) -> None:
        if self._server is not None:
            return
        _secure_directory(self.state_dir)
        _secure_directory(self.log_dir)
        _secure_directory(self.socket_path.parent)
        if self.socket_path.is_symlink():
            raise TaskSupervisorError("supervisor socket must not be a symlink")
        if self.socket_path.exists():
            mode = self.socket_path.lstat().st_mode
            if not stat.S_ISSOCK(mode):
                raise TaskSupervisorError("supervisor socket path is unsafe")
            try:
                _reader, writer = await asyncio.wait_for(
                    asyncio.open_unix_connection(str(self.socket_path)),
                    timeout=1.0,
                )
            except (ConnectionRefusedError, FileNotFoundError):
                # No listener owns this filesystem entry. It is safe to remove
                # only after the connection probe, before examining task state.
                pass
            else:
                writer.close()
                await writer.wait_closed()
                raise TaskSupervisorError(
                    "another task supervisor is already active"
                )
            self.socket_path.unlink()
        self._load_task_tombstones()
        await self._load_persisted_tasks()
        self._server = await asyncio.start_unix_server(
            self._handle_client,
            path=str(self.socket_path),
            limit=_MAX_RPC_BYTES + 1,
        )
        os.chmod(self.socket_path, 0o600)
        fsync_directory(self.socket_path.parent)

    async def stop(self, *, terminate_tasks: bool) -> None:
        server = self._server
        self._server = None
        if server is not None:
            server.close()
            await server.wait_closed()
        if terminate_tasks:
            running = [
                task
                for task in self._tasks.values()
                if task.descriptor.state == "running"
            ]
            for task in running:
                await self._stop_task_group(task, signal.SIGKILL)
            monitors = [
                task.monitor_task
                for task in running
                if task.monitor_task is not None
            ]
            if monitors:
                await asyncio.gather(*monitors, return_exceptions=True)
        self.socket_path.unlink(missing_ok=True)

    async def _load_persisted_tasks(self) -> None:
        for directory in sorted(self.state_dir.iterdir()):
            if not directory.is_dir() or directory.is_symlink():
                continue
            _secure_directory(directory)
            descriptor_path = directory / "descriptor.json"
            terminal_path = directory / "terminal.json"
            terminal_reserve_path = directory / "terminal.reserve"
            if not descriptor_path.exists():
                # A daemon can die after reserving terminal-state space but
                # before the process/descriptor commit.  No command can be
                # adopted without a descriptor, so clean only this known
                # private artifact and leave any unexpected entry untouched.
                if terminal_reserve_path.exists():
                    tighten_state_file(terminal_reserve_path)
                    terminal_reserve_path.unlink()
                    fsync_directory(directory)
                try:
                    directory.rmdir()
                except OSError:
                    pass
                continue
            tighten_state_file(descriptor_path)
            try:
                raw_descriptor = json.loads(descriptor_path.read_text())
                descriptor = SupervisedTaskDescriptor.from_dict(
                    raw_descriptor,
                )
            except Exception as exc:
                raise TaskSupervisorError(
                    "invalid persisted task descriptor"
                ) from exc
            if directory.name != _task_key(descriptor.task_id):
                raise TaskSupervisorError("task descriptor directory mismatch")
            if descriptor.task_id in self._completed_task_ids:
                # The tombstone is the durable ACK commit point.  A crash can
                # occur before the acknowledged descriptor is unlinked; never
                # resurrect it and replay an already-accepted terminal event.
                for path in (
                    terminal_path,
                    terminal_reserve_path,
                    descriptor_path,
                ):
                    if path.exists():
                        tighten_state_file(path)
                        path.unlink()
                try:
                    directory.rmdir()
                except OSError:
                    pass
                fsync_directory(self.state_dir)
                continue
            terminal = None
            if terminal_path.exists():
                tighten_state_file(terminal_path)
                try:
                    terminal = json.loads(terminal_path.read_text())
                except Exception as exc:
                    raise TaskSupervisorError(
                        "invalid persisted terminal record"
                    ) from exc
                self._validate_terminal(terminal, descriptor)
            recovered_terminal_transition = False
            if descriptor.state == "running" and terminal is not None:
                # terminal.json is the commit point and is written first. A
                # daemon crash before descriptor.json is advanced must recover
                # the terminal, not reject the service or signal a dead PID.
                descriptor = SupervisedTaskDescriptor(
                    **{
                        **asdict(descriptor),
                        "state": "terminal",
                    }
                )
                recovered_terminal_transition = True
            if descriptor.state == "terminal" and terminal is None:
                raise TaskSupervisorError(
                    "task descriptor and terminal state disagree"
                )
            task = _ServerTask(
                descriptor=descriptor,
                directory=directory,
                descriptor_path=descriptor_path,
                terminal_path=terminal_path,
                terminal_reserve_path=terminal_reserve_path,
                spool_path=self.log_dir / f"{descriptor.task_id}.ndjson",
                terminal=terminal,
                stdin_lock=asyncio.Lock(),
                signal_lock=asyncio.Lock(),
                deadline_monotonic=None,
            )
            if recovered_terminal_transition:
                self._persist_descriptor(task)
            self._tasks[descriptor.task_id] = task
            if descriptor.state == "running":
                self._ensure_terminal_reserve(task)
                # The daemon is the only process that can retain waitpid and
                # pipe ownership. A daemon restart cannot safely adopt a stale
                # PID, so terminate only an exact Linux start-time match and
                # publish a stable fail-closed terminal record.
                await self._terminate_stale_group(task)
                await self._commit_terminal(
                    task,
                    exit_code=-1,
                    error_type="task_supervisor_restarted",
                    error_message=(
                        "Task supervisor restarted and could not retain "
                        "process ownership"
                    ),
                )

    def _load_task_tombstones(self) -> None:
        if not self._tombstone_path.exists():
            self._completed_task_ids = {}
            return
        tighten_state_file(self._tombstone_path)
        try:
            payload = json.loads(self._tombstone_path.read_text())
            task_ids = payload.get("task_ids")
            if (
                not isinstance(payload, dict)
                or payload.get("schema_version") != _SCHEMA_VERSION
                or not isinstance(task_ids, list)
                or len(task_ids) > _MAX_ACKED_TASK_TOMBSTONES
                or any(
                    not isinstance(task_id, str)
                    or not _valid_task_id(task_id)
                    for task_id in task_ids
                )
                or len(task_ids) != len(set(task_ids))
            ):
                raise ValueError("invalid task tombstone schema")
        except Exception as exc:
            raise TaskSupervisorError(
                "invalid acknowledged-task tombstones"
            ) from exc
        self._completed_task_ids = dict.fromkeys(task_ids)

    def _persist_task_tombstones(self) -> None:
        atomic_write_private(
            self._tombstone_path,
            json.dumps(
                {
                    "schema_version": _SCHEMA_VERSION,
                    "task_ids": list(self._completed_task_ids),
                },
                sort_keys=True,
                separators=(",", ":"),
            ),
        )

    @staticmethod
    def _validate_terminal(
        terminal: Any,
        descriptor: SupervisedTaskDescriptor,
    ) -> None:
        if (
            not isinstance(terminal, dict)
            or set(terminal) != {
                "schema_version",
                "task_id",
                "event_id",
                "exit_code",
                "error_type",
                "error_message",
                "finished_at",
            }
            or terminal["schema_version"] != _SCHEMA_VERSION
            or terminal["task_id"] != descriptor.task_id
            or terminal["event_id"] != descriptor.terminal_event_id
            or not isinstance(terminal["exit_code"], int)
            or isinstance(terminal["exit_code"], bool)
            or (
                terminal["error_type"] is not None
                and not isinstance(terminal["error_type"], str)
            )
            or (
                terminal["error_message"] is not None
                and not isinstance(terminal["error_message"], str)
            )
            or not isinstance(terminal["finished_at"], str)
        ):
            raise TaskSupervisorError("invalid terminal record")

    async def _handle_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        try:
            raw = await reader.readline()
            if not raw or len(raw) > _MAX_RPC_BYTES:
                raise TaskSupervisorError("invalid request")
            request = json.loads(raw)
            if not isinstance(request, dict):
                raise TaskSupervisorError("invalid request")
            response = await self._dispatch(request)
        except Exception as exc:
            # Do not include exception text: launch inputs can contain secrets.
            logger.warning(
                "Task supervisor RPC failed (%s)",
                type(exc).__name__,
            )
            response = {"ok": False, "error": "request failed"}
        try:
            encoded = _json_line(response)
            if len(encoded) > _MAX_RPC_BYTES:
                encoded = _json_line({
                    "ok": False,
                    "error": "response too large",
                })
            writer.write(encoded)
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()

    async def _dispatch(self, request: dict[str, Any]) -> dict[str, Any]:
        if self._fatal_error is not None:
            raise TaskSupervisorError("task supervisor state is unavailable")
        op = request.get("op")
        if op == "launch":
            launch = self._parse_launch(request.get("launch"))
            task = await self._launch(launch)
            return {"ok": True, "task": asdict(task.descriptor)}
        if op == "list":
            async with self._tasks_lock:
                tasks = [
                    asdict(task.descriptor)
                    for task in sorted(
                        self._tasks.values(),
                        key=lambda item: item.descriptor.task_id,
                    )
                ]
            return {"ok": True, "tasks": tasks}
        if op == "poll":
            task_id = self._parse_task_id(request.get("task_id"))
            offset = request.get("offset")
            if not isinstance(offset, int) or offset < 0:
                raise TaskSupervisorError("invalid spool offset")
            return {"ok": True, **await self._poll(task_id, offset)}
        if op == "signal":
            task_id = self._parse_task_id(request.get("task_id"))
            signal_name = request.get("signal")
            if signal_name not in _SIGNALS:
                raise TaskSupervisorError("invalid signal")
            scope = request.get("scope", "group")
            escalate = request.get("escalate", True)
            if (
                scope not in {"process", "group"}
                or not isinstance(escalate, bool)
            ):
                raise TaskSupervisorError("invalid signal policy")
            task = await self._get_task(task_id)
            signalled = await self._stop_task_group(
                task,
                _SIGNALS[signal_name],
                scope=scope,
                escalate=escalate,
            )
            return {"ok": True, "signalled": signalled}
        if op == "stdin":
            task_id = self._parse_task_id(request.get("task_id"))
            payload = request.get("payload")
            if not isinstance(payload, str) or len(payload.encode()) > 1024 * 1024:
                raise TaskSupervisorError("invalid stdin payload")
            await self._write_stdin(task_id, payload)
            return {"ok": True}
        if op == "stdin_base64_once":
            task_id = self._parse_task_id(request.get("task_id"))
            payload_base64 = request.get("payload_base64")
            if (
                not isinstance(payload_base64, str)
                or not payload_base64
                or len(payload_base64) > 400_000
            ):
                raise TaskSupervisorError("invalid binary stdin payload")
            try:
                decoded = bytearray(
                    base64.b64decode(payload_base64, validate=True)
                )
            except (binascii.Error, ValueError) as exc:
                raise TaskSupervisorError(
                    "invalid binary stdin payload"
                ) from exc
            try:
                if not decoded or len(decoded) > 256 * 1024:
                    raise TaskSupervisorError("invalid binary stdin payload")
                await self._write_stdin_once(task_id, decoded)
            finally:
                for index in range(len(decoded)):
                    decoded[index] = 0
            return {"ok": True}
        if op == "mark_exhaustion":
            task_id = self._parse_task_id(request.get("task_id"))
            reason = request.get("reason")
            event_id = request.get("event_id")
            if (
                not isinstance(reason, str)
                or not reason
                or len(reason) > 128
                or not isinstance(event_id, str)
                or not re.fullmatch(r"[A-Za-z0-9_-]{8,128}", event_id)
            ):
                raise TaskSupervisorError("invalid exhaustion record")
            await self._mark_exhaustion(task_id, reason, event_id)
            return {"ok": True}
        if op == "ack_event":
            event_id = request.get("event_id")
            if not isinstance(event_id, str) or not event_id:
                raise TaskSupervisorError("invalid event id")
            await self._ack_event(event_id)
            return {"ok": True}
        raise TaskSupervisorError("unknown operation")

    @staticmethod
    def _parse_task_id(value: Any) -> str:
        if not isinstance(value, str) or not _valid_task_id(value):
            raise TaskSupervisorError("invalid task id")
        return value

    def _parse_launch(self, value: Any) -> SupervisedTaskLaunch:
        if not isinstance(value, dict):
            raise TaskSupervisorError("invalid launch")
        expected = {
            "task_id",
            "command",
            "cwd",
            "env",
            "timeout_seconds",
            "job_id",
            "watch_exhaustion",
            "agent_api_provider",
            "agent_type",
        }
        if set(value) != expected:
            raise TaskSupervisorError("invalid launch schema")
        task_id = self._parse_task_id(value["task_id"])
        command = value["command"]
        env = value["env"]
        timeout = value["timeout_seconds"]
        if (
            not isinstance(command, list)
            or not command
            or len(command) > 1024
            or any(
                not isinstance(item, str)
                or "\0" in item
                or len(item.encode()) > 128 * 1024
                for item in command
            )
        ):
            raise TaskSupervisorError("invalid command")
        if (
            not isinstance(env, dict)
            or len(env) > 4096
            or any(
                not isinstance(key, str)
                or not isinstance(item, str)
                or "\0" in key
                or "\0" in item
                for key, item in env.items()
            )
        ):
            raise TaskSupervisorError("invalid environment")
        if (
            not isinstance(value["cwd"], str)
            or not value["cwd"]
            or "\0" in value["cwd"]
        ):
            raise TaskSupervisorError("invalid cwd")
        if (
            timeout is not None
            and (
                not isinstance(timeout, int)
                or isinstance(timeout, bool)
                or timeout <= 0
                or timeout > 30 * 24 * 60 * 60
            )
        ):
            raise TaskSupervisorError("invalid timeout")
        if (
            not isinstance(value["job_id"], str)
            or len(value["job_id"]) > 256
            or not isinstance(value["watch_exhaustion"], bool)
            or value["agent_api_provider"] not in {
                None,
                "cloudrouter",
                "apex",
            }
            or value["agent_type"] not in {None, "claude", "codex"}
        ):
            raise TaskSupervisorError("invalid launch metadata")
        return SupervisedTaskLaunch(
            task_id=task_id,
            command=list(command),
            cwd=value["cwd"],
            env=dict(env),
            timeout_seconds=timeout,
            job_id=value["job_id"],
            watch_exhaustion=value["watch_exhaustion"],
            agent_api_provider=value["agent_api_provider"],
            agent_type=value["agent_type"],
        )

    async def _launch(self, launch: SupervisedTaskLaunch) -> _ServerTask:
        async with self._tasks_lock:
            if (
                launch.task_id in self._tasks
                or launch.task_id in self._completed_task_ids
            ):
                raise TaskSupervisorError("duplicate task")
            directory = _secure_directory(
                self.state_dir / _task_key(launch.task_id)
            )
            descriptor_path = directory / "descriptor.json"
            terminal_path = directory / "terminal.json"
            terminal_reserve_path = directory / "terminal.reserve"
            spool_path = self.log_dir / f"{launch.task_id}.ndjson"
            self._create_terminal_reserve(terminal_reserve_path)
            try:
                process = await asyncio.create_subprocess_exec(
                    *launch.command,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                    stdin=asyncio.subprocess.PIPE,
                    env=launch.env,
                    cwd=launch.cwd,
                    start_new_session=True,
                )
            except Exception as exc:
                self._discard_terminal_reserve(
                    terminal_reserve_path,
                    directory,
                )
                raise TaskSupervisorError("process launch failed") from exc
            start_ticks = _pid_start_ticks(process.pid)
            if os.name == "posix" and start_ticks == 0:
                await self._kill_unregistered_process(process)
                self._discard_terminal_reserve(
                    terminal_reserve_path,
                    directory,
                )
                raise TaskSupervisorError(
                    "could not bind process identity"
                )
            descriptor = SupervisedTaskDescriptor(
                schema_version=_SCHEMA_VERSION,
                task_id=launch.task_id,
                state="running",
                pid=process.pid,
                pgid=process.pid,
                pid_start_ticks=start_ticks,
                started_at=_utcnow_iso(),
                deadline_at=(
                    time.time() + launch.timeout_seconds
                    if launch.timeout_seconds is not None
                    else None
                ),
                timeout_seconds=launch.timeout_seconds,
                job_id=launch.job_id,
                watch_exhaustion=launch.watch_exhaustion,
                agent_api_provider=launch.agent_api_provider,
                agent_type=launch.agent_type,
                terminal_event_id=uuid.uuid4().hex,
                pending_exhaustion=None,
            )
            task = _ServerTask(
                descriptor=descriptor,
                directory=directory,
                descriptor_path=descriptor_path,
                terminal_path=terminal_path,
                terminal_reserve_path=terminal_reserve_path,
                spool_path=spool_path,
                process=process,
                stdin_lock=asyncio.Lock(),
                signal_lock=asyncio.Lock(),
                deadline_monotonic=(
                    time.monotonic() + launch.timeout_seconds
                    if launch.timeout_seconds is not None
                    else None
                ),
            )
            try:
                self._persist_descriptor(task)
            except Exception:
                await self._kill_unregistered_process(process)
                self._discard_terminal_reserve(
                    terminal_reserve_path,
                    directory,
                )
                raise
            self._tasks[launch.task_id] = task
            task.monitor_task = asyncio.create_task(self._monitor(task))
            task.monitor_task.add_done_callback(self._monitor_done)
            return task

    @staticmethod
    def _create_terminal_reserve(path: Path) -> None:
        """Durably reserve blocks needed to publish terminal metadata.

        The spool and state directories normally share one worker filesystem.
        Releasing this allocation immediately before terminal.json is written
        keeps a noisy task or result tree from consuming the final metadata
        blocks and stranding a dead process as ``running``.
        """

        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        fd = os.open(path, flags, 0o600)
        try:
            os.fchmod(fd, 0o600)
            allocated = False
            if hasattr(os, "posix_fallocate"):
                try:
                    os.posix_fallocate(fd, 0, _TERMINAL_RESERVE_BYTES)
                    allocated = True
                except OSError as exc:
                    if exc.errno not in {
                        errno.EINVAL,
                        errno.ENOSYS,
                        errno.EOPNOTSUPP,
                    }:
                        raise
            if not allocated:
                remaining = _TERMINAL_RESERVE_BYTES
                block = b"\0" * min(64 * 1024, remaining)
                while remaining:
                    chunk = block[:remaining]
                    _write_all(fd, chunk)
                    remaining -= len(chunk)
            os.fsync(fd)
            os.close(fd)
            fd = -1
            fsync_directory(path.parent)
        except Exception:
            if fd >= 0:
                os.close(fd)
            path.unlink(missing_ok=True)
            raise

    @classmethod
    def _discard_terminal_reserve(
        cls,
        path: Path,
        directory: Path,
    ) -> None:
        if path.exists():
            tighten_state_file(path)
            path.unlink()
            fsync_directory(directory)
        try:
            directory.rmdir()
        except OSError:
            pass

    def _ensure_terminal_reserve(self, task: _ServerTask) -> None:
        path = task.terminal_reserve_path
        if path.exists():
            tighten_state_file(path)
            if path.stat().st_size < _TERMINAL_RESERVE_BYTES:
                path.unlink()
                fsync_directory(task.directory)
            else:
                return
        self._create_terminal_reserve(path)

    @staticmethod
    def _release_terminal_reserve(task: _ServerTask) -> None:
        path = task.terminal_reserve_path
        if not path.exists():
            return
        tighten_state_file(path)
        path.unlink()
        fsync_directory(task.directory)

    def _monitor_done(self, monitor: asyncio.Task[None]) -> None:
        if monitor.cancelled():
            return
        try:
            error = monitor.exception()
        except asyncio.CancelledError:
            return
        if error is None:
            return
        logger.critical(
            "Task supervisor cannot publish durable terminal state (%s)",
            type(error).__name__,
        )
        self._fatal_error = "durable terminal state is unavailable"
        self._fatal_event.set()

    @staticmethod
    async def _kill_unregistered_process(
        process: asyncio.subprocess.Process,
    ) -> None:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            pass
        try:
            await asyncio.wait_for(process.wait(), timeout=5)
        except (asyncio.TimeoutError, ProcessLookupError):
            pass

    def _persist_descriptor(self, task: _ServerTask) -> None:
        atomic_write_private(
            task.descriptor_path,
            json.dumps(
                asdict(task.descriptor),
                sort_keys=True,
                separators=(",", ":"),
            ),
        )

    async def _monitor(self, task: _ServerTask) -> None:
        process = task.process
        if process is None:
            return
        stdout_task = asyncio.create_task(
            self._spool_stream(task, process.stdout, "stdout")
        )
        stderr_task = asyncio.create_task(
            self._spool_stream(task, process.stderr, "stderr")
        )
        timeout_error = False
        monitor_error = False
        try:
            while process.returncode is None:
                for stream_task in (stdout_task, stderr_task):
                    if stream_task.done():
                        stream_error = stream_task.exception()
                        if stream_error is not None:
                            raise stream_error
                deadline = task.deadline_monotonic
                if deadline is not None and time.monotonic() >= deadline:
                    timeout_error = True
                    await self._stop_task_group(task, signal.SIGINT)
                    break
                await asyncio.sleep(0.1)
            if process.returncode is None:
                await self._wait_returncode(process, 20)
            await self._stop_task_group(task, signal.SIGTERM)
            for stream_task in (stdout_task, stderr_task):
                try:
                    await asyncio.wait_for(
                        stream_task,
                        timeout=_EXIT_DRAIN_TIMEOUT,
                    )
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    stream_task.cancel()
                    await asyncio.gather(stream_task, return_exceptions=True)
        except asyncio.CancelledError:
            await self._stop_task_group(task, signal.SIGKILL)
            raise
        except Exception:
            monitor_error = True
            logger.exception(
                "Supervised task %s monitor failed",
                task.descriptor.task_id,
            )
            await self._stop_task_group(task, signal.SIGKILL)
        finally:
            for stream_task in (stdout_task, stderr_task):
                if not stream_task.done():
                    stream_task.cancel()
            await asyncio.gather(
                stdout_task,
                stderr_task,
                return_exceptions=True,
            )
            try:
                self._fsync_spool(task)
            except Exception:
                monitor_error = True
                logger.error(
                    "Could not fsync supervised task %s spool",
                    task.descriptor.task_id,
                )
            exit_code = (
                process.returncode
                if process.returncode is not None
                else -1
            )
            if timeout_error:
                error_type = "runtime_timeout"
                error_message = (
                    "Worker task supervisor timeout interrupted the process"
                )
            elif monitor_error:
                error_type = "task_supervisor_error"
                error_message = (
                    "Worker task supervisor could not monitor the process"
                )
            else:
                error_type = None
                error_message = None
            await self._commit_terminal(
                task,
                exit_code=exit_code,
                error_type=error_type,
                error_message=error_message,
            )

    @staticmethod
    async def _wait_returncode(
        process: asyncio.subprocess.Process,
        timeout: float,
    ) -> bool:
        deadline = time.monotonic() + timeout
        while process.returncode is None:
            if time.monotonic() >= deadline:
                return False
            await asyncio.sleep(0.05)
        return True

    @staticmethod
    async def _iter_stream_frames(
        stream: asyncio.StreamReader | None,
    ):
        if stream is None:
            return
        pending = bytearray()
        decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
        while True:
            chunk = await stream.read(_MAX_LOG_FRAME_BYTES)
            if not chunk:
                break
            pending.extend(chunk)
            while pending:
                newline = pending.find(b"\n", 0, _MAX_LOG_FRAME_BYTES)
                if newline >= 0:
                    end = newline + 1
                elif len(pending) >= _MAX_LOG_FRAME_BYTES:
                    end = _MAX_LOG_FRAME_BYTES
                else:
                    break
                frame = bytes(pending[:end])
                del pending[:end]
                physical_line_end = frame.endswith(b"\n")
                text = decoder.decode(frame, final=physical_line_end)
                if physical_line_end:
                    decoder.reset()
                yield text.rstrip("\n")
        if pending:
            yield decoder.decode(bytes(pending), final=True)
        else:
            tail = decoder.decode(b"", final=True)
            if tail:
                yield tail

    async def _spool_stream(
        self,
        task: _ServerTask,
        stream: asyncio.StreamReader | None,
        stream_name: str,
    ) -> None:
        async for line in self._iter_stream_frames(stream):
            if not line:
                continue
            record = {
                "task_id": task.descriptor.task_id,
                "stream": stream_name,
                "data": line,
                "timestamp": _utcnow_iso(),
                "parsed": None,
            }
            encoded = _json_line(record)
            async with task.spool_lock:
                open_flags = os.O_WRONLY | os.O_CREAT | os.O_APPEND
                if hasattr(os, "O_NOFOLLOW"):
                    open_flags |= os.O_NOFOLLOW
                fd = os.open(
                    task.spool_path,
                    open_flags,
                    0o600,
                )
                try:
                    os.fchmod(fd, 0o600)
                    spool_stat = os.fstat(fd)
                    if not stat.S_ISREG(spool_stat.st_mode):
                        raise TaskSupervisorError("task spool is not regular")
                    if spool_stat.st_size + len(encoded) > self.max_spool_bytes:
                        raise TaskSupervisorError(
                            "task output spool limit exceeded"
                        )
                    try:
                        _write_all(fd, encoded)
                        now = time.monotonic()
                        if now - task.last_spool_fsync >= 1.0:
                            os.fsync(fd)
                            task.last_spool_fsync = now
                    except Exception:
                        # os.write may have appended only part of the JSON line
                        # before ENOSPC/EIO.  A trailing partial line prevents
                        # poll() from ever reaching EOF and therefore suppresses
                        # the terminal event forever.  Roll the record back
                        # while both stream writers remain serialized.
                        try:
                            os.ftruncate(fd, spool_stat.st_size)
                            os.fsync(fd)
                        except Exception as rollback_error:
                            raise TaskSupervisorError(
                                "task spool rollback failed"
                            ) from rollback_error
                        raise
                finally:
                    os.close(fd)

    @staticmethod
    def _fsync_spool(task: _ServerTask) -> None:
        if not task.spool_path.exists():
            return
        tighten_state_file(task.spool_path)
        fd = os.open(task.spool_path, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    async def _commit_terminal(
        self,
        task: _ServerTask,
        *,
        exit_code: int,
        error_type: str | None,
        error_message: str | None,
    ) -> None:
        terminal = {
            "schema_version": _SCHEMA_VERSION,
            "task_id": task.descriptor.task_id,
            "event_id": task.descriptor.terminal_event_id,
            "exit_code": int(exit_code),
            "error_type": error_type,
            "error_message": error_message,
            "finished_at": _utcnow_iso(),
        }
        last_error: Exception | None = None
        for attempt in range(_TERMINAL_COMMIT_ATTEMPTS):
            try:
                self._release_terminal_reserve(task)
                if task.terminal is None:
                    atomic_write_private(
                        task.terminal_path,
                        json.dumps(
                            terminal,
                            sort_keys=True,
                            separators=(",", ":"),
                        ),
                    )
                    task.terminal = terminal
                if task.descriptor.state != "terminal":
                    task.descriptor = SupervisedTaskDescriptor(
                        **{
                            **asdict(task.descriptor),
                            "state": "terminal",
                        }
                    )
                    self._persist_descriptor(task)
                task.process = None
                return
            except Exception as exc:
                last_error = exc
                logger.error(
                    "Could not commit terminal state for task %s "
                    "(attempt %d/%d, %s)",
                    task.descriptor.task_id,
                    attempt + 1,
                    _TERMINAL_COMMIT_ATTEMPTS,
                    type(exc).__name__,
                )
                if attempt + 1 < _TERMINAL_COMMIT_ATTEMPTS:
                    await asyncio.sleep(0.05 * (2**attempt))
        raise TaskSupervisorError(
            "durable terminal state could not be committed"
        ) from last_error

    async def _get_task(self, task_id: str) -> _ServerTask:
        async with self._tasks_lock:
            task = self._tasks.get(task_id)
        if task is None:
            raise TaskSupervisorError("task does not exist")
        return task

    async def _poll(self, task_id: str, offset: int) -> dict[str, Any]:
        task = await self._get_task(task_id)
        records: list[dict[str, Any]] = []
        next_offset = offset
        at_eof = True
        if task.spool_path.is_symlink():
            # The monitor will terminate the task with task_supervisor_error.
            # Never follow the attacker-controlled target merely to serve a
            # poll while that terminal transition is being committed.
            if offset != 0:
                raise TaskSupervisorError("invalid spool offset")
        elif task.spool_path.exists():
            tighten_state_file(task.spool_path)
            size = task.spool_path.stat().st_size
            if offset > size:
                raise TaskSupervisorError("spool offset exceeds file size")
            consumed = 0
            with task.spool_path.open("rb") as stream:
                stream.seek(offset)
                for _ in range(_MAX_POLL_RECORDS):
                    raw = stream.readline(_MAX_SPOOL_RECORD_BYTES)
                    if not raw:
                        break
                    if not raw.endswith(b"\n"):
                        if task.terminal is not None:
                            # No writer exists after terminal commit.  A prior
                            # ENOSPC/EIO may have prevented even the rollback
                            # truncate; discard only this uncommitted tail so it
                            # cannot suppress the durable terminal forever.
                            stream.seek(0, os.SEEK_END)
                        else:
                            # A live writer has not committed the complete
                            # record yet; retry from the same byte next poll.
                            stream.seek(-len(raw), os.SEEK_CUR)
                        break
                    consumed += len(raw)
                    if consumed > _MAX_POLL_BYTES:
                        stream.seek(-len(raw), os.SEEK_CUR)
                        break
                    value = json.loads(raw)
                    if not isinstance(value, dict):
                        raise TaskSupervisorError("invalid spool record")
                    records.append(value)
                next_offset = stream.tell()
                at_eof = next_offset >= size
        terminal = (
            dict(task.terminal)
            if task.terminal is not None and at_eof
            else None
        )
        return {
            "records": records,
            "next_offset": next_offset,
            "terminal": terminal,
        }

    async def _write_stdin(self, task_id: str, payload: str) -> None:
        task = await self._get_task(task_id)
        process = task.process
        if (
            task.descriptor.state != "running"
            or process is None
            or process.stdin is None
            or process.returncode is not None
        ):
            raise TaskSupervisorError("task stdin is unavailable")
        lock = task.stdin_lock or asyncio.Lock()
        task.stdin_lock = lock
        async with lock:
            process.stdin.write((payload + "\n").encode())
            await process.stdin.drain()

    async def _write_stdin_once(
        self, task_id: str, payload: bytearray,
    ) -> None:
        task = await self._get_task(task_id)
        process = task.process
        if (
            task.descriptor.state != "running"
            or process is None
            or process.stdin is None
            or process.returncode is not None
        ):
            raise TaskSupervisorError("task stdin is unavailable")
        lock = task.stdin_lock or asyncio.Lock()
        task.stdin_lock = lock
        async with lock:
            process.stdin.write(payload)
            await process.stdin.drain()
            process.stdin.close()
            wait_closed = getattr(process.stdin, "wait_closed", None)
            if callable(wait_closed):
                await wait_closed()

    async def _mark_exhaustion(
        self,
        task_id: str,
        reason: str,
        event_id: str,
    ) -> None:
        task = await self._get_task(task_id)
        current = task.descriptor.pending_exhaustion
        pending = {"event_id": event_id, "reason": reason}
        if current is not None and current != pending:
            raise TaskSupervisorError("conflicting exhaustion event")
        if current is None:
            task.descriptor = SupervisedTaskDescriptor(
                **{
                    **asdict(task.descriptor),
                    "pending_exhaustion": pending,
                }
            )
            self._persist_descriptor(task)

    async def _ack_event(self, event_id: str) -> None:
        async with self._tasks_lock:
            tasks = list(self._tasks.values())
        for task in tasks:
            pending = task.descriptor.pending_exhaustion
            if pending is not None and pending["event_id"] == event_id:
                task.descriptor = SupervisedTaskDescriptor(
                    **{
                        **asdict(task.descriptor),
                        "pending_exhaustion": None,
                    }
                )
                self._persist_descriptor(task)
                return
            if (
                task.terminal is not None
                and task.terminal["event_id"] == event_id
            ):
                if task.descriptor.pending_exhaustion is not None:
                    raise TaskSupervisorError(
                        "prior exhaustion event is not acknowledged"
                    )
                async with self._tasks_lock:
                    current = self._tasks.get(task.descriptor.task_id)
                    if current is not task:
                        return
                    previous_tombstones = dict(
                        self._completed_task_ids
                    )
                    self._completed_task_ids[
                        task.descriptor.task_id
                    ] = None
                    while (
                        len(self._completed_task_ids)
                        > _MAX_ACKED_TASK_TOMBSTONES
                    ):
                        oldest = next(iter(self._completed_task_ids))
                        del self._completed_task_ids[oldest]
                    try:
                        self._persist_task_tombstones()
                    except Exception:
                        self._completed_task_ids = previous_tombstones
                        raise
                    del self._tasks[task.descriptor.task_id]
                task.terminal_path.unlink(missing_ok=True)
                task.descriptor_path.unlink(missing_ok=True)
                try:
                    task.directory.rmdir()
                except OSError:
                    pass
                fsync_directory(self.state_dir)
                return

    @staticmethod
    def _group_exists(pgid: int) -> bool:
        if os.name != "posix" or pgid <= 1:
            return False
        try:
            os.killpg(pgid, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
        return True

    async def _wait_group_exit(self, pgid: int, timeout: float) -> bool:
        deadline = time.monotonic() + timeout
        while self._group_exists(pgid):
            if time.monotonic() >= deadline:
                return False
            await asyncio.sleep(0.05)
        return True

    async def _stop_task_group(
        self,
        task: _ServerTask,
        initial_signal: signal.Signals,
        *,
        scope: str = "group",
        escalate: bool = True,
    ) -> bool:
        process = task.process
        if (
            task.descriptor.state != "running"
            or process is None
            or task.descriptor.pgid != process.pid
        ):
            return False
        lock = task.signal_lock or asyncio.Lock()
        task.signal_lock = lock
        async with lock:
            pgid = task.descriptor.pgid
            if not self._group_exists(pgid):
                return False
            if scope == "process" and process.returncode is not None:
                # The leader PID may already have been recycled while escaped
                # descendants keep the original pgid alive.
                return False
            if (
                process.returncode is None
                and task.descriptor.pid_start_ticks
                and _pid_start_ticks(process.pid)
                != task.descriptor.pid_start_ticks
            ):
                logger.error(
                    "Refusing reused process id for task %s",
                    task.descriptor.task_id,
                )
                return False

            async def send(sig: signal.Signals, target: str = "group") -> bool:
                try:
                    if target == "process":
                        os.kill(process.pid, sig)
                    else:
                        os.killpg(pgid, sig)
                    return True
                except (ProcessLookupError, PermissionError):
                    return False

            if not await send(initial_signal, scope):
                return False
            if not escalate:
                return True
            first_grace = 5.0 if initial_signal == signal.SIGKILL else 10.0
            if await self._wait_group_exit(pgid, first_grace):
                return True
            if initial_signal not in {signal.SIGTERM, signal.SIGKILL}:
                if await send(signal.SIGTERM):
                    if await self._wait_group_exit(pgid, 5.0):
                        return True
            if initial_signal != signal.SIGKILL:
                await send(signal.SIGKILL)
                await self._wait_group_exit(pgid, 5.0)
            return not self._group_exists(pgid)

    async def _terminate_stale_group(self, task: _ServerTask) -> None:
        descriptor = task.descriptor
        if (
            descriptor.pid_start_ticks
            and _pid_start_ticks(descriptor.pid) == descriptor.pid_start_ticks
        ):
            try:
                os.killpg(descriptor.pgid, signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                return
            await self._wait_group_exit(descriptor.pgid, 5.0)


async def _serve(args: argparse.Namespace) -> None:
    server = TaskSupervisorServer(
        socket_path=args.socket,
        state_dir=args.state_dir,
        log_dir=args.log_dir,
        max_spool_bytes=args.max_spool_bytes,
    )
    await server.start()
    stopping = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stopping.set)
        except NotImplementedError:
            pass
    stopping_task = asyncio.create_task(stopping.wait())
    fatal_task = asyncio.create_task(server.fatal_event.wait())
    done, pending = await asyncio.wait(
        {stopping_task, fatal_task},
        return_when=asyncio.FIRST_COMPLETED,
    )
    fatal = fatal_task in done and server.fatal_error is not None
    for waiter in pending:
        waiter.cancel()
    await asyncio.gather(*pending, return_exceptions=True)
    await server.stop(terminate_tasks=True)
    if fatal:
        raise TaskSupervisorError(
            server.fatal_error or "task supervisor state is unavailable"
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Elastic-Agent independent Mode-B task supervisor",
    )
    parser.add_argument("--socket", required=True)
    parser.add_argument("--state-dir", required=True)
    parser.add_argument("--log-dir", required=True)
    parser.add_argument(
        "--max-spool-bytes",
        type=int,
        default=_DEFAULT_MAX_SPOOL_BYTES,
        help="maximum durable stdout/stderr spool bytes per task",
    )
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    asyncio.run(_serve(args))


if __name__ == "__main__":
    main()
