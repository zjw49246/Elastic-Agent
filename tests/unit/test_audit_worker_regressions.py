"""Regression coverage for the 2026-07-28 Worker/login audit findings."""

from __future__ import annotations

import asyncio
import base64
import io
import json
import logging
import os
import subprocess
import sys
import time
import types
from pathlib import Path
from unittest.mock import AsyncMock, patch

import httpx
import pytest

import elastic_agent.worker.runtime as worker_runtime
from elastic_agent.core.claude_oauth import (
    ClaudeLoginCleanupError,
    LoginResult,
    OAuthConfig,
    _safe_login_error,
    restore_claude_credentials,
    secure_claude_credentials,
    snapshot_claude_credentials,
    write_credentials,
)
from elastic_agent.core.log_event_parser import LogEventParser
from elastic_agent.core.protocols.messages import (
    AccountLoginCancelledMessage,
    AccountLoginCancelMessage,
    AccountLoginMessage,
    AccountLoginResultMessage,
    CredentialLoginMessage,
    ErrorMessage,
    ExecuteMessage,
    FileChangeMessage,
    FileContentMessage,
    HeartbeatMessage,
    LogMessage,
    ProcessExitMessage,
)
from elastic_agent.worker.login import auto_login
from elastic_agent.worker.login.cdp_login import (
    _cleanup_tracked_processes,
    _create_login_debug_directory,
    _terminate_process_group_sync,
    cdp_login,
    cdp_screenshot,
)
from elastic_agent.worker.runtime import (
    _MAX_DATA_TRANSPORT_FRAME_BYTES,
    _MAX_LOG_FRAME_BYTES,
    _MAX_LOG_TRANSPORT_FRAME_BYTES,
    _MAX_PENDING_CONTROL_BYTES,
    _MAX_PENDING_CONTROL_FRAMES,
    _MAX_PENDING_DATA_BYTES,
    _MAX_PENDING_DATA_FRAMES,
    _MAX_PENDING_LOG_BYTES,
    _MAX_PENDING_LOG_FRAMES,
    ReliableEventPersistenceError,
    WorkerRuntime,
)


def _runtime(tmp_path: Path) -> WorkerRuntime:
    return WorkerRuntime(
        manager_url="ws://localhost/ws/runtime",
        auth_token="token",
        worker_id="worker-1",
        log_dir=str(tmp_path / "logs"),
    )


def _pid_is_live(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    status = Path(f"/proc/{pid}/stat")
    if status.exists():
        try:
            return status.read_text().split()[2] != "Z"
        except (OSError, IndexError):
            pass
    return True


def test_claude_credentials_are_private_under_permissive_umask(
    tmp_path: Path,
) -> None:
    config_dir = tmp_path / "claude"
    previous = os.umask(0o022)
    try:
        write_credentials(str(config_dir), {"accessToken": "secret"})
    finally:
        os.umask(previous)

    assert config_dir.stat().st_mode & 0o777 == 0o700
    assert (config_dir / ".credentials.json").stat().st_mode & 0o777 == 0o600
    assert not list(config_dir.glob(".*.tmp"))


@pytest.mark.parametrize("broken", [False, True])
def test_claude_credential_snapshot_rejects_symlinks(
    tmp_path: Path,
    broken: bool,
) -> None:
    config_dir = tmp_path / "claude"
    config_dir.mkdir()
    target = tmp_path / "outside"
    if not broken:
        target.write_text("outside")
    (config_dir / ".credentials.json").symlink_to(target)

    with pytest.raises(RuntimeError, match="not a regular file"):
        snapshot_claude_credentials(str(config_dir))


def test_restore_removed_credentials_fsyncs_parent(tmp_path: Path) -> None:
    config_dir = tmp_path / "claude"
    config_dir.mkdir()
    snapshot = snapshot_claude_credentials(str(config_dir))
    write_credentials(str(config_dir), {"accessToken": "new"})

    with patch("elastic_agent.core.claude_oauth.fsync_directory") as fsync:
        restore_claude_credentials(snapshot)

    assert not (config_dir / ".credentials.json").exists()
    fsync.assert_called_once_with(config_dir)


def test_secure_claude_credentials_rejects_broken_symlink(tmp_path: Path) -> None:
    config_dir = tmp_path / "claude"
    config_dir.mkdir()
    (config_dir / ".credentials.json").symlink_to(tmp_path / "missing")

    with pytest.raises(RuntimeError, match="must not be a symlink"):
        secure_claude_credentials(str(config_dir))


@pytest.mark.asyncio
async def test_cancelled_claude_login_restores_private_credentials(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_dir = tmp_path / "claude"
    config_dir.mkdir(mode=0o700)
    old_credential = b'{"claudeAiOauth":{"accessToken":"old"}}'
    old_profile = b'{"oauthAccount":{"emailAddress":"old@example.com"}}'
    (config_dir / ".credentials.json").write_bytes(old_credential)
    (config_dir / ".claude.json").write_bytes(old_profile)
    os.chmod(config_dir / ".credentials.json", 0o600)
    os.chmod(config_dir / ".claude.json", 0o600)

    async def cancelled_login(**_kwargs):
        (config_dir / ".credentials.json").write_text('{"claudeAiOauth":{"accessToken":"new"}}')
        (config_dir / ".claude.json").write_text('{"oauthAccount":{"emailAddress":"new@example.com"}}')
        raise asyncio.CancelledError

    monkeypatch.setitem(
        sys.modules,
        "cdp_login",
        types.SimpleNamespace(cdp_login=cancelled_login),
    )

    with pytest.raises(asyncio.CancelledError):
        await auto_login.perform_login(
            email="new@example.com",
            token_171="mail-secret",
            config_dir=str(config_dir),
            provider="mailcom",
        )

    assert (config_dir / ".credentials.json").read_bytes() == old_credential
    assert (config_dir / ".claude.json").read_bytes() == old_profile
    assert (config_dir / ".credentials.json").stat().st_mode & 0o777 == 0o600
    assert (config_dir / ".claude.json").stat().st_mode & 0o777 == 0o600


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure_kind",
    ["ordinary", "cancelled", "process_cleanup"],
)
async def test_login_rollback_failure_is_always_cleanup_uncertain(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure_kind: str,
) -> None:
    config_dir = tmp_path / "claude"
    config_dir.mkdir(mode=0o700)
    (config_dir / ".credentials.json").write_text('{"old": true}')
    os.chmod(config_dir / ".credentials.json", 0o600)
    if failure_kind == "process_cleanup":
        original: BaseException = ClaudeLoginCleanupError(
            "process cleanup could not be verified"
        )
    elif failure_kind == "cancelled":
        original = asyncio.CancelledError()
    else:
        original = RuntimeError("browser failed")

    async def failed_login(**_kwargs):
        raise original

    rollback_error = RuntimeError("rollback failed")

    def failed_restore(_snapshot):
        raise rollback_error

    monkeypatch.setitem(
        sys.modules,
        "cdp_login",
        types.SimpleNamespace(cdp_login=failed_login),
    )
    monkeypatch.setattr(
        "elastic_agent.core.claude_oauth.restore_claude_credentials",
        failed_restore,
    )

    with pytest.raises(ClaudeLoginCleanupError) as raised:
        await auto_login.perform_login(
            email="new@example.com",
            token_171="mail-secret",
            config_dir=str(config_dir),
            provider="mailcom",
        )

    if failure_kind == "process_cleanup":
        # Keep the process-cleanup failure as the primary exception while also
        # retaining the rollback failure as context and an explicit note.
        assert raised.value is original
        assert "process cleanup" in str(raised.value)
        assert any(
            "credential rollback also failed" in note
            for note in getattr(raised.value, "__notes__", ())
        )
        assert raised.value.__context__ is rollback_error
    else:
        assert "credential rollback" in str(raised.value)
        assert raised.value.__cause__ is rollback_error
        assert rollback_error.__context__ is original


@pytest.mark.asyncio
async def test_mailbox_query_token_is_not_emitted_by_httpx(
    caplog: pytest.LogCaptureFixture,
) -> None:
    canary = "mailbox-token-canary"
    subject_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    def respond(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "data": {
                    "subject": f"Claude | {subject_time}",
                    "code": "https://claude.ai/magic-link/redacted",
                }
            },
        )

    caplog.set_level(logging.INFO)
    # caplog changes the root level, not the deliberately permanent httpx
    # logger WARNING boundary installed by auto_login.
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        await auto_login._poll_magic_link(
            client,
            canary,
            time.time() - 1,
            1,
        )

    assert canary not in caplog.text
    assert logging.getLogger("httpx").level >= logging.WARNING
    assert logging.getLogger("httpcore").level >= logging.WARNING


def test_claude_login_error_redacts_urls_and_known_inputs() -> None:
    canary = "mailbox-token-canary"
    error = _safe_login_error(
        f"failed https://example.invalid/callback?code=oauth-code&state=oauth-state token={canary}",
        secrets=(canary,),
    )

    assert "https://" not in error
    assert "oauth-code" not in error
    assert "oauth-state" not in error
    assert canary not in error


def test_login_process_group_cleanup_reaps_descendants(tmp_path: Path) -> None:
    child_pid_file = tmp_path / "child.pid"
    script = (
        "import pathlib,subprocess,sys,time;"
        "p=subprocess.Popen([sys.executable,'-c','import time;time.sleep(60)']);"
        f"pathlib.Path({str(child_pid_file)!r}).write_text(str(p.pid));"
        "time.sleep(60)"
    )
    parent = subprocess.Popen(
        [sys.executable, "-c", script],
        start_new_session=True,
    )
    try:
        deadline = time.monotonic() + 5
        while not child_pid_file.exists() and time.monotonic() < deadline:
            time.sleep(0.02)
        child_pid = int(child_pid_file.read_text())

        _terminate_process_group_sync(parent, grace_seconds=1)

        assert not _pid_is_live(parent.pid)
        assert not _pid_is_live(child_pid)
    finally:
        if parent.poll() is None:
            os.killpg(parent.pid, 9)
            parent.wait(timeout=5)


@pytest.mark.asyncio
async def test_login_cleanup_attempts_every_process_group(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    processes = [object(), object(), object()]
    attempted = []

    def terminate(process):
        attempted.append(process)
        if process is processes[-1]:
            raise RuntimeError("stuck process group")

    monkeypatch.setattr(
        "elastic_agent.worker.login.cdp_login._terminate_process_group_sync",
        terminate,
    )

    with pytest.raises(ClaudeLoginCleanupError, match="1 Claude login process"):
        await _cleanup_tracked_processes(processes)

    assert attempted == list(reversed(processes))


@pytest.mark.asyncio
async def test_repeated_cancellation_cannot_escape_cdp_process_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    login_started = asyncio.Event()
    cleanup_started = asyncio.Event()
    release_cleanup = asyncio.Event()

    async def blocked_login(*_args, **_kwargs):
        login_started.set()
        await asyncio.Future()

    async def blocked_cleanup(_processes):
        cleanup_started.set()
        await release_cleanup.wait()

    monkeypatch.setattr(
        "elastic_agent.worker.login.cdp_login._cdp_login_impl",
        blocked_login,
    )
    monkeypatch.setattr(
        "elastic_agent.worker.login.cdp_login._cleanup_tracked_processes",
        blocked_cleanup,
    )
    task = asyncio.create_task(cdp_login(
        email="user@example.com",
        token="mail-token",
        config_dir="/tmp/unused-claude-test",
    ))
    await login_started.wait()

    # The first cancellation leaves the login body; later cancellations arrive
    # while its shielded process-group cleanup is still in progress.
    task.cancel()
    await cleanup_started.wait()
    task.cancel()
    await asyncio.sleep(0)
    task.cancel()
    await asyncio.sleep(0)

    assert task.done() is False
    release_cleanup.set()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_provider_does_not_convert_cleanup_failure_to_login_result() -> None:
    from elastic_agent.core.claude_oauth import ClaudeOAuthProvider

    provider = ClaudeOAuthProvider()
    provider._login_local = AsyncMock(
        side_effect=ClaudeLoginCleanupError("cleanup failed")
    )

    with pytest.raises(ClaudeLoginCleanupError):
        await provider.login(
            OAuthConfig(
                account_id="account-1",
                email="user@example.com",
                email_token="mail-token",
                config_dir="/tmp/claude-cleanup-test",
            )
        )


class _ScreenshotWebSocket:
    def __init__(self, payload: bytes = b"private-png") -> None:
        self._message_id = 0
        self._payload = payload

    async def send(self, data: str) -> None:
        self._message_id = int(json.loads(data)["id"])

    async def recv(self) -> str:
        return json.dumps({
            "id": self._message_id,
            "result": {
                "data": base64.b64encode(self._payload).decode("ascii"),
            },
        })


@pytest.mark.asyncio
async def test_debug_screenshot_is_private_and_refuses_symlink(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ELASTIC_AGENT_LOGIN_DEBUG_SCREENSHOTS", "1")
    debug_dir = tmp_path / "debug"
    debug_dir.mkdir(mode=0o755)
    screenshot = debug_dir / "shot.png"

    await cdp_screenshot(_ScreenshotWebSocket(), screenshot)

    assert debug_dir.stat().st_mode & 0o777 == 0o700
    assert screenshot.stat().st_mode & 0o777 == 0o600
    victim = tmp_path / "victim"
    victim.write_bytes(b"keep-me")
    symlink = debug_dir / "link.png"
    symlink.symlink_to(victim)
    with pytest.raises(RuntimeError, match="already exists"):
        await cdp_screenshot(_ScreenshotWebSocket(b"overwrite"), symlink)
    assert victim.read_bytes() == b"keep-me"


def test_debug_directories_are_private_and_unpredictable() -> None:
    first = _create_login_debug_directory()
    second = _create_login_debug_directory()
    try:
        assert first != second
        assert first.stat().st_mode & 0o777 == 0o700
        assert second.stat().st_mode & 0o777 == 0o700
    finally:
        first.rmdir()
        second.rmdir()


@pytest.mark.asyncio
async def test_reliable_outbox_failure_is_not_advertised_or_swallowed(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path)
    runtime._persist_reliable_events = lambda: (_ for _ in ()).throw(OSError("disk full"))
    runtime._ws = AsyncMock()
    message = ProcessExitMessage(task_id="task-1", exit_code=1)

    with pytest.raises(ReliableEventPersistenceError):
        await runtime._send_process_exit(message)

    assert message.event_id not in runtime._reliable_events
    assert "task-1" not in runtime._exiting_task_ids
    assert runtime._send_queue.empty()
    runtime._ws.close.assert_awaited()


@pytest.mark.asyncio
async def test_oversized_output_line_keeps_draining_following_output(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path)
    sent: list[LogMessage] = []

    async def capture(message):
        if isinstance(message, LogMessage):
            sent.append(message)

    runtime._send_event = capture
    reader = asyncio.StreamReader()
    reader.feed_data(b"x" * 100_000 + b"\nafter-long-line\n")
    reader.feed_eof()

    await runtime._read_stream("task-1", reader, "stdout", None)

    assert len(sent) >= 3
    assert max(len(item.data.encode()) for item in sent) <= 64 * 1024
    assert sent[-1].data == "after-long-line"


class _ChunkedReader:
    def __init__(self, chunks: list[bytes]) -> None:
        self._chunks = [*chunks, b""]

    async def read(self, _size: int) -> bytes:
        return self._chunks.pop(0)


@pytest.mark.asyncio
async def test_stream_frame_limit_survives_short_reads_before_newline() -> None:
    reader = _ChunkedReader([
        b"a" * 40_000,
        b"b" * 40_000 + b"\n",
    ])

    frames = [
        frame
        async for frame in WorkerRuntime._iter_stream_frames(reader)
    ]

    assert [len(frame) for frame in frames] == [64 * 1024, 14_465]
    assert max(map(len, frames)) <= 64 * 1024


@pytest.mark.asyncio
async def test_stream_frames_preserve_utf8_split_at_byte_boundary(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path)
    sent: list[LogMessage] = []

    async def capture(message):
        if isinstance(message, LogMessage):
            sent.append(message)

    runtime._send_event = capture
    expected = "a" * (64 * 1024 - 1) + "中"
    reader = _ChunkedReader([
        expected.encode("utf-8")[: 64 * 1024],
        expected.encode("utf-8")[64 * 1024 :] + b"\n",
    ])

    await runtime._read_stream("task-1", reader, "stdout", None)

    assert "".join(message.data for message in sent) == expected
    assert "\N{REPLACEMENT CHARACTER}" not in "".join(
        message.data for message in sent
    )


@pytest.mark.asyncio
async def test_log_transport_queue_is_bounded_without_dropping_control(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path)
    for index in range(_MAX_PENDING_LOG_FRAMES + 17):
        await runtime._send_event(
            LogMessage(
                task_id="task-1",
                stream="stdout",
                data=f"line-{index}",
            )
        )
    await runtime._send_event(HeartbeatMessage(uptime_seconds=1))

    assert runtime._log_send_queue.qsize() == _MAX_PENDING_LOG_FRAMES
    assert runtime._dropped_log_frames == 17
    control = json.loads(await runtime._send_queue.get())
    assert control["type"] == "HEARTBEAT"


@pytest.mark.asyncio
async def test_log_transport_has_strict_serialized_byte_budget(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path)
    payload = "x" * 120_000

    for index in range(100):
        await runtime._send_event(LogMessage(
            task_id=f"task-{index}",
            stream="stdout",
            data=payload,
        ))

    assert runtime._log_send_queue.wire_bytes <= _MAX_PENDING_LOG_BYTES
    assert runtime._log_send_queue.qsize() < 100
    assert runtime._dropped_log_frames > 0


@pytest.mark.asyncio
async def test_control_transport_has_strict_serialized_byte_budget(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path)
    payload = "x" * 700_000

    for index in range(_MAX_PENDING_CONTROL_FRAMES + 50):
        await runtime._send_event(ErrorMessage(
            error_type=f"test-{index}",
            message=payload,
        ))

    assert runtime._send_queue.wire_bytes <= _MAX_PENDING_CONTROL_BYTES
    assert runtime._send_queue.qsize() <= _MAX_PENDING_CONTROL_FRAMES
    assert runtime._dropped_control_frames > 0


@pytest.mark.asyncio
async def test_control_and_data_retry_reservations_keep_total_bytes_strict(
    tmp_path: Path,
) -> None:
    control_runtime = _runtime(tmp_path / "control")
    control = ErrorMessage(
        error_type="retry",
        message="c" * 700_000,
    ).model_dump_json()
    control_runtime._retry_send = control
    control_runtime._retry_send_kind = "control"
    control_runtime._retry_send_bytes = len(control.encode("utf-8"))
    for index in range(20):
        await control_runtime._send_event(ErrorMessage(
            error_type=f"later-{index}",
            message="c" * 700_000,
        ))

    assert (
        control_runtime._send_queue.wire_bytes
        + control_runtime._retry_send_bytes
        <= _MAX_PENDING_CONTROL_BYTES
    )

    data_runtime = _runtime(tmp_path / "data")
    data = FileContentMessage(
        request_id="retry",
        path="/tmp/result",
        content="d" * 700_000,
    ).model_dump_json()
    data_runtime._retry_send = data
    data_runtime._retry_send_kind = "data"
    data_runtime._retry_send_bytes = len(data.encode("utf-8"))
    for index in range(20):
        await data_runtime._send_event(FileContentMessage(
            request_id=f"later-{index}",
            path="/tmp/result",
            content="d" * 700_000,
        ))

    assert (
        data_runtime._data_send_queue.wire_bytes
        + data_runtime._retry_send_bytes
        <= _MAX_PENDING_DATA_BYTES
    )


@pytest.mark.asyncio
async def test_oversized_log_transport_marker_preserves_local_raw_trace(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path)
    raw_line = "\x00" * _MAX_LOG_FRAME_BYTES
    reader = asyncio.StreamReader()
    reader.feed_data(raw_line.encode("utf-8") + b"\n")
    reader.feed_eof()
    local_log = io.StringIO()

    await runtime._read_stream(
        "task-raw",
        reader,
        "stdout",
        local_log,
    )

    local_entry = json.loads(local_log.getvalue())
    assert local_entry["data"] == raw_line
    transport = json.loads(runtime._log_send_queue.get_nowait())
    assert transport["parsed"]["type"] == "elastic_transport_truncated"
    assert (
        transport["parsed"]["original_serialized_bytes"]
        > _MAX_LOG_TRANSPORT_FRAME_BYTES
    )
    assert "full raw frame" in transport["data"]


@pytest.mark.asyncio
async def test_file_data_queue_is_bounded_and_control_stays_first(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path)
    for index in range(_MAX_PENDING_DATA_FRAMES + 11):
        await runtime._send_event(FileChangeMessage(
            path=f"result-{index}.json",
            event="modified",
        ))
    await runtime._send_event(HeartbeatMessage(uptime_seconds=1))

    assert runtime._data_send_queue.qsize() == _MAX_PENDING_DATA_FRAMES
    assert runtime._data_send_queue.wire_bytes <= _MAX_PENDING_DATA_BYTES
    assert runtime._dropped_data_frames == 11
    frame, kind, _ = await runtime._next_queued_frame()
    assert kind == "control"
    assert json.loads(frame)["type"] == "HEARTBEAT"


@pytest.mark.asyncio
async def test_oversized_file_content_flood_cannot_grow_control_queue(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    runtime = _runtime(tmp_path)
    monkeypatch.setattr(
        worker_runtime,
        "_MAX_DATA_TRANSPORT_FRAME_BYTES",
        1024,
    )
    message = FileContentMessage(
        request_id="request-1",
        path="/tmp/result",
        content="x" * 2048,
    )

    with caplog.at_level(logging.WARNING):
        for _ in range(4096):
            await runtime._send_event(message)

    # Oversized best-effort data is represented only by one scalar counter and
    # logarithmically sampled local warnings.  It never gets promoted into the
    # unbounded lifecycle/control queue.
    assert runtime._data_send_queue.empty()
    assert runtime._send_queue.empty()
    assert runtime._dropped_data_frames == 4096
    warnings = [
        record for record in caplog.records
        if "Dropped" in record.message
        and "file-data transport frames" in record.message
    ]
    assert len(warnings) == 13


@pytest.mark.asyncio
async def test_reliable_terminal_precedes_saturated_best_effort_queues(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path)
    for index in range(_MAX_PENDING_LOG_FRAMES):
        await runtime._send_event(LogMessage(
            task_id="task-1",
            stream="stdout",
            data=f"log-{index}",
        ))
    for index in range(_MAX_PENDING_DATA_FRAMES):
        await runtime._send_event(FileChangeMessage(
            path=f"result-{index}",
            event="modified",
        ))
    terminal = ProcessExitMessage(task_id="task-1", exit_code=0)
    await runtime._send_event(terminal)

    frame, kind, _ = await runtime._next_queued_frame()
    assert kind == "control"
    assert json.loads(frame)["event_id"] == terminal.event_id


@pytest.mark.asyncio
async def test_cancelled_log_send_is_retried_within_byte_budget(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path)
    await runtime._send_event(LogMessage(
        task_id="task-1",
        stream="stdout",
        data="retry-me",
    ))
    send_started = asyncio.Event()

    class BlockingWebSocket:
        async def send(self, _data):
            send_started.set()
            await asyncio.Future()

    runtime._running = True
    runtime._ws = BlockingWebSocket()
    sender = asyncio.create_task(runtime._sender_loop())
    await send_started.wait()
    sender.cancel()
    with pytest.raises(asyncio.CancelledError):
        await sender

    assert runtime._retry_send is not None
    assert runtime._retry_send_kind == "log"
    for index in range(_MAX_PENDING_LOG_FRAMES + 20):
        await runtime._send_event(LogMessage(
            task_id="task-1",
            stream="stdout",
            data=f"later-{index}",
        ))
    assert (
        runtime._log_send_queue.wire_bytes + runtime._retry_send_bytes
        <= _MAX_PENDING_LOG_BYTES
    )
    assert (
        runtime._log_send_queue.qsize() + 1
        <= _MAX_PENDING_LOG_FRAMES
    )

    retried: list[str] = []

    class WorkingWebSocket:
        async def send(self, data):
            retried.append(data)
            runtime._running = False

    runtime._running = True
    runtime._ws = WorkingWebSocket()
    await runtime._sender_loop()
    assert json.loads(retried[0])["data"] == "retry-me"


@pytest.mark.asyncio
async def test_claude_login_receives_requested_browser_timeout(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path)
    runtime._send_event = AsyncMock()
    runtime._verify_config_identity = AsyncMock(return_value=True)
    runtime._warmup_config_dir = AsyncMock(return_value=True)
    seen_configs = []

    async def succeed(_provider, config):
        seen_configs.append(config)
        from elastic_agent.core.claude_oauth import LoginResult

        return LoginResult(success=True, account_id="account-1")

    message = AccountLoginMessage(
        login_request_id="login-1",
        account_id="account-1",
        email="user@example.com",
        email_token="mail-token",
        agent_type="claude",
        config_dir=str(tmp_path / "claude"),
        provider="171mail",
        login_timeout_seconds=1100,
    )
    with patch(
        "elastic_agent.core.claude_oauth.ClaudeOAuthProvider.login",
        new=succeed,
    ):
        await runtime._handle_claude_account_login(message)

    assert seen_configs[0].login_timeout == 1100


def _login_message(
    tmp_path: Path,
    *,
    agent_type: str = "claude",
) -> AccountLoginMessage:
    return AccountLoginMessage(
        login_request_id=f"{agent_type}-login-1",
        account_id=f"{agent_type}-account-1",
        email="user@example.com",
        email_token="mail-token",
        password="openai-password" if agent_type == "codex" else "",
        agent_type=agent_type,
        config_dir=str(tmp_path / agent_type),
        provider="171mail",
    )


def _cancel_message(msg: AccountLoginMessage) -> AccountLoginCancelMessage:
    return AccountLoginCancelMessage(
        login_request_id=msg.login_request_id,
        account_id=msg.account_id,
        reason="manager_timeout",
    )


def _cancel_ack(sent: list[object]) -> AccountLoginCancelledMessage:
    return next(
        message
        for message in reversed(sent)
        if isinstance(message, AccountLoginCancelledMessage)
    )


def _login_result(sent: list[object]) -> AccountLoginResultMessage:
    return next(
        message
        for message in reversed(sent)
        if isinstance(message, AccountLoginResultMessage)
    )


@pytest.mark.asyncio
async def test_successful_claude_login_late_cancel_is_not_cleanup_complete(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path)
    sent: list[object] = []
    runtime._send_event = lambda message: sent.append(message) or asyncio.sleep(0)
    runtime._verify_config_identity = AsyncMock(return_value=True)
    runtime._warmup_config_dir = AsyncMock(return_value=True)
    msg = _login_message(tmp_path)

    with patch(
        "elastic_agent.core.claude_oauth.ClaudeOAuthProvider.login",
        new=AsyncMock(return_value=LoginResult(
            success=True,
            account_id=msg.account_id,
        )),
    ):
        await runtime._run_account_login_task(msg)
    await runtime._handle_account_login_cancel(_cancel_message(msg))

    assert msg.login_request_id not in runtime._account_login_cleanup_confirmed
    assert _login_result(sent).cleanup_complete is None
    assert _cancel_ack(sent).cleanup_complete is False


@pytest.mark.asyncio
async def test_failed_claude_login_late_cancel_is_cleanup_complete(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path)
    sent: list[object] = []
    runtime._send_event = lambda message: sent.append(message) or asyncio.sleep(0)
    msg = _login_message(tmp_path)

    with patch(
        "elastic_agent.core.claude_oauth.ClaudeOAuthProvider.login",
        new=AsyncMock(return_value=LoginResult(
            success=False,
            account_id=msg.account_id,
            error="login rejected",
        )),
    ):
        await runtime._run_account_login_task(msg)
    await runtime._handle_account_login_cancel(_cancel_message(msg))

    assert _login_result(sent).cleanup_complete is True
    assert _cancel_ack(sent).cleanup_complete is True


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("login_ok", "cleanup_complete"),
    [(True, False), (False, True)],
)
async def test_codex_late_cancel_reflects_committed_or_rolled_back_state(
    tmp_path: Path,
    login_ok: bool,
    cleanup_complete: bool,
) -> None:
    runtime = _runtime(tmp_path)
    sent: list[object] = []
    runtime._send_event = lambda message: sent.append(message) or asyncio.sleep(0)
    msg = _login_message(tmp_path, agent_type="codex")

    with patch(
        "elastic_agent.worker.login.codex_login.codex_login",
        new=AsyncMock(return_value={
            "ok": login_ok,
            "error": None if login_ok else "login rejected",
        }),
    ):
        await runtime._run_account_login_task(msg)
    await runtime._handle_account_login_cancel(_cancel_message(msg))

    assert _login_result(sent).cleanup_complete is (
        None if login_ok else True
    )
    assert _cancel_ack(sent).cleanup_complete is cleanup_complete


@pytest.mark.asyncio
@pytest.mark.parametrize("agent_type", ["claude", "codex"])
async def test_cancelled_login_confirms_transactional_cleanup(
    tmp_path: Path,
    agent_type: str,
) -> None:
    runtime = _runtime(tmp_path)
    sent: list[object] = []
    runtime._send_event = lambda message: sent.append(message) or asyncio.sleep(0)
    msg = _login_message(tmp_path, agent_type=agent_type)
    target = (
        "elastic_agent.core.claude_oauth.ClaudeOAuthProvider.login"
        if agent_type == "claude"
        else "elastic_agent.worker.login.codex_login.codex_login"
    )

    with patch(target, new=AsyncMock(side_effect=asyncio.CancelledError)):
        with pytest.raises(asyncio.CancelledError):
            await runtime._run_account_login_task(msg)
    await runtime._handle_account_login_cancel(_cancel_message(msg))

    assert _cancel_ack(sent).cleanup_complete is True


@pytest.mark.asyncio
async def test_cancel_while_waiting_for_login_lock_is_cleanup_complete(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path)
    sent: list[object] = []
    runtime._send_event = lambda message: sent.append(message) or asyncio.sleep(0)
    msg = _login_message(tmp_path)
    await runtime._account_login_lock.acquire()
    task = asyncio.create_task(runtime._run_account_login_task(msg))
    try:
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    finally:
        runtime._account_login_lock.release()

    await runtime._handle_account_login_cancel(_cancel_message(msg))
    assert _cancel_ack(sent).cleanup_complete is True


@pytest.mark.asyncio
async def test_claude_process_cleanup_failure_withholds_cancel_ack(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path)
    sent: list[object] = []
    runtime._send_event = lambda message: sent.append(message) or asyncio.sleep(0)
    msg = _login_message(tmp_path)

    with patch(
        "elastic_agent.core.claude_oauth.ClaudeOAuthProvider.login",
        new=AsyncMock(side_effect=ClaudeLoginCleanupError("stuck process")),
    ):
        await runtime._run_account_login_task(msg)
    await runtime._handle_account_login_cancel(_cancel_message(msg))

    assert _login_result(sent).cleanup_complete is False
    assert _cancel_ack(sent).cleanup_complete is False


@pytest.mark.asyncio
async def test_claude_pty_recycle_failure_reports_cleanup_uncertain(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path)
    sent: list[object] = []
    runtime._send_event = lambda message: sent.append(message) or asyncio.sleep(0)
    runtime._verify_config_identity = AsyncMock(return_value=True)
    runtime._warmup_config_dir = AsyncMock(return_value=True)
    runtime._pty_backend = types.SimpleNamespace(
        recycle_config_dir=AsyncMock(side_effect=RuntimeError("PTY stuck"))
    )
    runtime._quota_checker = types.SimpleNamespace(add_slot=AsyncMock())
    msg = _login_message(tmp_path)

    with patch(
        "elastic_agent.core.claude_oauth.ClaudeOAuthProvider.login",
        new=AsyncMock(return_value=LoginResult(
            success=True,
            account_id=msg.account_id,
        )),
    ):
        await runtime._run_account_login_task(msg)

    result = _login_result(sent)
    assert result.success is False
    assert result.cleanup_complete is False
    assert result.error == "account login cleanup could not be verified"
    assert msg.config_dir in runtime._unsafe_credential_config_dirs
    runtime._quota_checker.add_slot.assert_not_called()


@pytest.mark.asyncio
async def test_credential_login_cancellation_restores_previous_credentials(
    tmp_path: Path,
) -> None:
    from elastic_agent.core.claude_oauth import read_credentials

    runtime = _runtime(tmp_path)
    config_dir = str(tmp_path / "claude-slot")
    write_credentials(config_dir, {
        "account_id": "old-account",
        "accessToken": "old-token",
    })
    recycle_started = asyncio.Event()

    async def blocked_recycle(_config_dir):
        recycle_started.set()
        await asyncio.Future()

    runtime._pty_backend = types.SimpleNamespace(
        recycle_config_dir=blocked_recycle
    )
    runtime._send_event = AsyncMock()
    runtime._quota_checker = types.SimpleNamespace(add_slot=AsyncMock())
    task = asyncio.create_task(runtime._handle_credential_login(
        CredentialLoginMessage(
            task_id="",
            slot_index=1,
            credentials={
                "account_id": "new-account",
                "accessToken": "new-token",
            },
            config_dir=config_dir,
        )
    ))
    await recycle_started.wait()

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert read_credentials(config_dir)["accessToken"] == "old-token"
    assert config_dir not in runtime._unsafe_credential_config_dirs
    runtime._send_event.assert_not_awaited()
    runtime._quota_checker.add_slot.assert_not_called()


@pytest.mark.asyncio
async def test_credential_login_double_failure_blocks_slot_and_redacts_result(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path)
    config_dir = str(tmp_path / "claude-slot")
    write_credentials(config_dir, {
        "account_id": "old-account",
        "accessToken": "old-token",
    })
    runtime._pty_backend = types.SimpleNamespace(
        recycle_config_dir=AsyncMock(
            side_effect=RuntimeError("new-token recycle secret")
        )
    )
    runtime._send_event = AsyncMock()
    runtime._quota_checker = types.SimpleNamespace(add_slot=AsyncMock())

    with patch(
        "elastic_agent.core.claude_oauth.restore_claude_credentials",
        side_effect=RuntimeError("old-token rollback secret"),
    ):
        await runtime._handle_credential_login(CredentialLoginMessage(
            task_id="",
            slot_index=1,
            credentials={
                "account_id": "new-account",
                "accessToken": "new-token",
            },
            config_dir=config_dir,
        ))

    result = runtime._send_event.await_args.args[0]
    assert result.success is False
    assert result.error == (
        "Credential activation failed and rollback could not be verified"
    )
    assert "token" not in result.error.lower()
    assert config_dir in runtime._unsafe_credential_config_dirs
    runtime._quota_checker.add_slot.assert_not_called()

    runtime._pty_backend = None
    runtime._send_process_exit = AsyncMock()
    await runtime._handle_execute(ExecuteMessage(
        task_id="must-not-run",
        command=["claude", "-p", "hello"],
        cwd=str(tmp_path),
        env={"CLAUDE_CONFIG_DIR": config_dir},
    ))
    terminal = runtime._send_process_exit.await_args.args[0]
    assert terminal.error_type == "credential_slot_unsafe"


@pytest.mark.asyncio
async def test_credential_login_cancel_and_rollback_double_failure_is_uncertain(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    runtime = _runtime(tmp_path)
    config_dir = str(tmp_path / "claude-slot")
    runtime._pty_backend = types.SimpleNamespace(
        recycle_config_dir=AsyncMock(side_effect=asyncio.CancelledError)
    )
    runtime._send_event = AsyncMock()

    message = CredentialLoginMessage(
        task_id="",
        slot_index=1,
        credentials={
            "account_id": "new-account",
            "accessToken": "new-token",
        },
        config_dir=config_dir,
    )
    with (
        caplog.at_level(logging.ERROR),
        patch(
            "elastic_agent.core.claude_oauth.restore_claude_credentials",
            side_effect=RuntimeError("rollback-secret-canary"),
        ),
    ):
        await runtime._dispatch(message)

    result = runtime._send_event.await_args.args[0]
    assert result.success is False
    assert result.error == (
        "Credential update failed and rollback could not be verified"
    )
    assert config_dir in runtime._unsafe_credential_config_dirs
    assert "new-token" not in caplog.text
    assert "rollback-secret-canary" not in caplog.text


@pytest.mark.asyncio
async def test_failed_post_login_identity_check_rolls_back_credentials(
    tmp_path: Path,
) -> None:
    from elastic_agent.core.claude_oauth import LoginResult

    runtime = _runtime(tmp_path)
    runtime._send_event = AsyncMock()
    runtime._verify_config_identity = AsyncMock(return_value=False)
    runtime._warmup_config_dir = AsyncMock()
    config_dir = tmp_path / "claude"
    write_credentials(str(config_dir), {"accessToken": "old"})

    async def install_wrong_identity(_provider, _config):
        write_credentials(str(config_dir), {"accessToken": "wrong-account"})
        return LoginResult(success=True, account_id="account-1")

    message = AccountLoginMessage(
        login_request_id="login-1",
        account_id="account-1",
        email="expected@example.com",
        email_token="mail-token",
        agent_type="claude",
        config_dir=str(config_dir),
        provider="171mail",
    )
    with patch(
        "elastic_agent.core.claude_oauth.ClaudeOAuthProvider.login",
        new=install_wrong_identity,
    ):
        await runtime._handle_claude_account_login(message)

    saved = json.loads((config_dir / ".credentials.json").read_text())
    assert saved["claudeAiOauth"]["accessToken"] == "old"
    assert (config_dir / ".credentials.json").stat().st_mode & 0o777 == 0o600


@pytest.mark.asyncio
async def test_unknown_login_cancel_does_not_claim_cleanup_complete(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path)
    sent = []
    runtime._send_event = lambda message: sent.append(message) or asyncio.sleep(0)

    await runtime._handle_account_login_cancel(
        AccountLoginCancelMessage(
            login_request_id="unknown-request",
            account_id="account-1",
            reason="manager_timeout",
        )
    )

    acknowledgement = next(message for message in sent if isinstance(message, AccountLoginCancelledMessage))
    assert acknowledgement.cleanup_complete is False


def test_autonomous_result_is_buffered_but_not_accounted() -> None:
    parser = LogEventParser()
    foreground = {
        "task_id": "task-1",
        "stream": "stdout",
        "data": '{"type":"result"}',
        "parsed": {
            "type": "result",
            "session_id": "foreground-session",
            "cost_usd": 1.0,
        },
    }
    autonomous = {
        "task_id": "task-1",
        "stream": "stdout",
        "data": '{"type":"result"}',
        "parsed": None,
    }

    parser.process_log_event("worker-1", foreground)
    assert parser.process_log_event("worker-1", autonomous) is None

    session = parser.get_task_session("task-1")
    assert session is not None
    assert session.session_id == "foreground-session"
    assert session.total_cost_usd == 1.0
    assert parser.buffer_size("task-1") == 2
