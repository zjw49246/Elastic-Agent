"""Durable Mode-B task-supervisor regression tests.

The supervisor is deliberately a separate process/service from ea-runtime.
These tests exercise the same Unix-socket boundary used in production: losing
one runtime client must not terminate the child, lose its output spool, or
change the stable terminal event id.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path

import pytest

import elastic_agent.worker.task_supervisor as task_supervisor_module
from elastic_agent.worker.runtime import WorkerRuntime
from elastic_agent.worker.task_supervisor import (
    SupervisedTaskLaunch,
    TaskSupervisorClient,
    TaskSupervisorError,
    TaskSupervisorServer,
)


async def _wait_for_terminal(
    client: TaskSupervisorClient,
    task_id: str,
    *,
    timeout: float = 10,
) -> tuple[list[dict], dict]:
    offset = 0
    records: list[dict] = []
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        snapshot = await client.poll(task_id, offset=offset)
        offset = snapshot.next_offset
        records.extend(snapshot.records)
        if snapshot.terminal is not None:
            return records, snapshot.terminal
        await asyncio.sleep(0.02)
    raise TimeoutError(f"task {task_id} did not become terminal")


@pytest.fixture
async def supervisor(tmp_path):
    socket_path = tmp_path / "run" / "control.sock"
    state_dir = tmp_path / "state"
    log_dir = tmp_path / "logs"
    server = TaskSupervisorServer(
        socket_path=socket_path,
        state_dir=state_dir,
        log_dir=log_dir,
    )
    await server.start()
    client = TaskSupervisorClient(socket_path)
    try:
        yield server, client, state_dir, log_dir
    finally:
        await server.stop(terminate_tasks=True)


@pytest.mark.asyncio
async def test_client_loss_does_not_stop_task_and_new_client_adopts(supervisor):
    server, first, _state_dir, _log_dir = supervisor
    await first.launch(SupervisedTaskLaunch(
        task_id="job:runtime-restart",
        command=[
            sys.executable,
            "-u",
            "-c",
            "import time; print('before-restart'); time.sleep(.4); print('after-restart')",
        ],
        cwd=os.getcwd(),
        env=dict(os.environ),
        timeout_seconds=10,
        job_id="job-1",
    ))

    # A runtime uses short-lived RPC connections. Dropping that client object
    # models ea-runtime.service being replaced; the independent server remains
    # the child's parent and owns stdout/stderr/waitpid.
    del first
    await asyncio.sleep(0.1)
    assert server.running_task_ids == ["job:runtime-restart"]

    restarted = TaskSupervisorClient(server.socket_path)
    inventory = await restarted.list_tasks()
    assert [item.task_id for item in inventory] == ["job:runtime-restart"]
    assert inventory[0].state == "running"

    records, terminal = await _wait_for_terminal(
        restarted, "job:runtime-restart",
    )
    assert [record["data"] for record in records] == [
        "before-restart",
        "after-restart",
    ]
    assert terminal["exit_code"] == 0


@pytest.mark.asyncio
async def test_second_supervisor_refuses_split_brain_without_killing_task(
    supervisor,
):
    server, client, state_dir, log_dir = supervisor
    descriptor = await client.launch(SupervisedTaskLaunch(
        task_id="split-brain",
        command=[sys.executable, "-c", "import time; time.sleep(60)"],
        cwd=os.getcwd(),
        env=dict(os.environ),
        timeout_seconds=120,
    ))
    second = TaskSupervisorServer(
        socket_path=server.socket_path,
        state_dir=state_dir,
        log_dir=log_dir,
    )

    with pytest.raises(
        TaskSupervisorError,
        match="already active",
    ):
        await second.start()

    assert server.running_task_ids == ["split-brain"]
    os.kill(descriptor.pid, 0)


@pytest.mark.asyncio
async def test_terminal_event_id_is_stable_until_ack(supervisor):
    _server, client, _state_dir, _log_dir = supervisor
    descriptor = await client.launch(SupervisedTaskLaunch(
        task_id="terminal-replay",
        command=[sys.executable, "-c", "print('done')"],
        cwd=os.getcwd(),
        env=dict(os.environ),
        timeout_seconds=10,
    ))
    _, first_terminal = await _wait_for_terminal(client, descriptor.task_id)
    _, replayed_terminal = await _wait_for_terminal(
        TaskSupervisorClient(client.socket_path),
        descriptor.task_id,
    )
    assert replayed_terminal["event_id"] == first_terminal["event_id"]

    await client.ack_event(first_terminal["event_id"])
    assert await client.list_tasks() == []
    with pytest.raises(TaskSupervisorError):
        await client.launch(SupervisedTaskLaunch(
            task_id="terminal-replay",
            command=[sys.executable, "-c", "raise AssertionError('duplicate')"],
            cwd=os.getcwd(),
            env=dict(os.environ),
            timeout_seconds=10,
        ))


@pytest.mark.asyncio
async def test_unacked_terminal_survives_supervisor_daemon_restart(
    supervisor,
):
    server, client, state_dir, log_dir = supervisor
    await client.launch(SupervisedTaskLaunch(
        task_id="daemon-terminal-replay",
        command=[sys.executable, "-c", "raise SystemExit(7)"],
        cwd=os.getcwd(),
        env=dict(os.environ),
        timeout_seconds=10,
    ))
    _, terminal = await _wait_for_terminal(
        client,
        "daemon-terminal-replay",
    )
    await server.stop(terminate_tasks=True)
    # Simulate a daemon crash after terminal.json was fsynced but before the
    # descriptor's state transition was published.
    descriptor_path = next(state_dir.rglob("descriptor.json"))
    descriptor_data = json.loads(descriptor_path.read_text())
    descriptor_data["state"] = "running"
    descriptor_path.write_text(json.dumps(descriptor_data))
    os.chmod(descriptor_path, 0o600)

    restarted_server = TaskSupervisorServer(
        socket_path=server.socket_path,
        state_dir=state_dir,
        log_dir=log_dir,
    )
    await restarted_server.start()
    try:
        restarted = TaskSupervisorClient(server.socket_path)
        inventory = await restarted.list_tasks()
        assert len(inventory) == 1
        assert inventory[0].state == "terminal"
        _, replay = await _wait_for_terminal(
            restarted,
            "daemon-terminal-replay",
        )
        assert replay["event_id"] == terminal["event_id"]
        assert replay["exit_code"] == 7
    finally:
        await restarted_server.stop(terminate_tasks=True)


@pytest.mark.asyncio
async def test_descriptor_and_terminal_never_persist_command_or_environment(
    supervisor,
):
    _server, client, state_dir, log_dir = supervisor
    secret = "write-only-secret-7f64e2"
    command_marker = "argv-must-not-be-in-descriptor-51d6"
    descriptor = await client.launch(SupervisedTaskLaunch(
        task_id="private-state",
        command=[
            sys.executable,
            "-c",
            f"import time; print('ok'); time.sleep(.1) # {command_marker}",
        ],
        cwd=os.getcwd(),
        env={**os.environ, "SECRET_ENV": secret},
        timeout_seconds=10,
        job_id="job-private",
        watch_exhaustion=True,
        agent_api_provider="apex",
        agent_type="codex",
    ))
    await _wait_for_terminal(client, descriptor.task_id)

    persisted = "\n".join(
        path.read_text()
        for path in state_dir.rglob("*.json")
    )
    assert secret not in persisted
    assert command_marker not in persisted
    assert '"env"' not in persisted
    assert '"command"' not in persisted
    assert state_dir.stat().st_mode & 0o777 == 0o700
    for path in state_dir.rglob("*.json"):
        assert path.stat().st_mode & 0o777 == 0o600
    assert (log_dir / "private-state.ndjson").stat().st_mode & 0o777 == 0o600


@pytest.mark.asyncio
async def test_timeout_terminates_whole_process_group(supervisor, tmp_path):
    _server, client, _state_dir, _log_dir = supervisor
    child_pid = tmp_path / "child.pid"
    script = (
        "import pathlib,subprocess,time;"
        "p=subprocess.Popen(['sleep','60']);"
        f"pathlib.Path({str(child_pid)!r}).write_text(str(p.pid));"
        "time.sleep(60)"
    )
    await client.launch(SupervisedTaskLaunch(
        task_id="timeout-group",
        command=[sys.executable, "-c", script],
        cwd=str(tmp_path),
        env=dict(os.environ),
        timeout_seconds=1,
    ))
    _, terminal = await _wait_for_terminal(client, "timeout-group", timeout=10)
    assert terminal["error_type"] == "runtime_timeout"
    pid = int(child_pid.read_text())
    with pytest.raises(ProcessLookupError):
        os.kill(pid, 0)


@pytest.mark.asyncio
async def test_natural_leader_exit_cleans_background_descendant_before_terminal(
    supervisor,
    tmp_path,
):
    _server, client, _state_dir, _log_dir = supervisor
    child_pid = tmp_path / "background.pid"
    script = (
        "import pathlib,subprocess;"
        "p=subprocess.Popen(['sleep','60']);"
        f"pathlib.Path({str(child_pid)!r}).write_text(str(p.pid))"
    )
    await client.launch(SupervisedTaskLaunch(
        task_id="background-cleanup",
        command=[sys.executable, "-c", script],
        cwd=str(tmp_path),
        env=dict(os.environ),
        timeout_seconds=10,
    ))
    _, terminal = await _wait_for_terminal(
        client,
        "background-cleanup",
    )
    assert terminal["exit_code"] == 0
    pid = int(child_pid.read_text())
    with pytest.raises(ProcessLookupError):
        os.kill(pid, 0)


@pytest.mark.asyncio
async def test_escaped_64k_frame_does_not_block_terminal_poll(supervisor):
    _server, client, _state_dir, _log_dir = supervisor
    await client.launch(SupervisedTaskLaunch(
        task_id="large-escaped-frame",
        command=[
            sys.executable,
            "-c",
            "import sys;sys.stdout.buffer.write(b'\\0'*65536+b'\\n')",
        ],
        cwd=os.getcwd(),
        env=dict(os.environ),
        timeout_seconds=10,
    ))

    records, terminal = await _wait_for_terminal(
        client,
        "large-escaped-frame",
    )

    assert terminal["exit_code"] == 0
    assert len(records) == 1
    assert len(records[0]["data"]) == 65536


@pytest.mark.asyncio
async def test_stdin_and_explicit_stop_survive_socket_boundary(supervisor):
    _server, client, _state_dir, _log_dir = supervisor
    await client.launch(SupervisedTaskLaunch(
        task_id="interactive",
        command=[
            sys.executable,
            "-u",
            "-c",
            "import sys,time; print(sys.stdin.readline().strip()); time.sleep(60)",
        ],
        cwd=os.getcwd(),
        env=dict(os.environ),
        timeout_seconds=30,
    ))
    await client.write_stdin("interactive", "hello")
    await asyncio.sleep(0.1)
    assert await client.signal("interactive", signal_name="SIGTERM")
    records, terminal = await _wait_for_terminal(client, "interactive")
    assert any(record["data"] == "hello" for record in records)
    assert terminal["exit_code"] != 0


@pytest.mark.asyncio
async def test_binary_stdin_is_decoded_exactly_and_closed(supervisor):
    import base64
    import hashlib

    _server, client, _state_dir, _log_dir = supervisor
    payload = b"RBWORK01\x00\x01\xffsecret-frame"
    await client.launch(SupervisedTaskLaunch(
        task_id="binary-stdin",
        command=[
            sys.executable,
            "-u",
            "-c",
            "import hashlib,sys; print(hashlib.sha256(sys.stdin.buffer.read()).hexdigest())",
        ],
        cwd=os.getcwd(),
        env=dict(os.environ),
        timeout_seconds=10,
    ))
    await client.write_stdin_base64_once(
        "binary-stdin", base64.b64encode(payload).decode("ascii")
    )

    records, terminal = await _wait_for_terminal(client, "binary-stdin")

    assert terminal["exit_code"] == 0
    assert any(
        record["data"] == hashlib.sha256(payload).hexdigest()
        for record in records
    )


@pytest.mark.asyncio
@pytest.mark.skipif(os.name != "posix", reason="POSIX signalling only")
async def test_cooperative_process_signal_does_not_escalate_or_hit_child(
    supervisor,
    tmp_path,
):
    _server, client, _state_dir, _log_dir = supervisor
    marker = tmp_path / "leader-signalled"
    child_pid_path = tmp_path / "child.pid"
    code = f"""
import pathlib
import signal
import subprocess
import sys
import time

marker = pathlib.Path({str(marker)!r})
child_path = pathlib.Path({str(child_pid_path)!r})
signal.signal(signal.SIGINT, lambda *_: marker.write_text("SIGINT"))
child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
child_path.write_text(str(child.pid))
while True:
    time.sleep(0.1)
"""
    await client.launch(SupervisedTaskLaunch(
        task_id="cooperative-stop",
        command=[sys.executable, "-u", "-c", code],
        cwd=os.getcwd(),
        env=dict(os.environ),
        timeout_seconds=120,
    ))
    async with asyncio.timeout(5):
        while not child_pid_path.exists():
            await asyncio.sleep(0.02)

    started = asyncio.get_running_loop().time()
    assert await client.signal(
        "cooperative-stop",
        signal_name="SIGINT",
        scope="process",
        escalate=False,
    )
    assert asyncio.get_running_loop().time() - started < 1
    async with asyncio.timeout(5):
        while not marker.exists():
            await asyncio.sleep(0.02)
    child_pid = int(child_pid_path.read_text())
    os.kill(child_pid, 0)
    assert "cooperative-stop" in (
        descriptor.task_id for descriptor in await client.list_tasks()
    )

    assert await client.signal(
        "cooperative-stop",
        signal_name="SIGKILL",
        scope="group",
        escalate=False,
    )
    _records, terminal = await _wait_for_terminal(
        client,
        "cooperative-stop",
    )
    assert terminal["exit_code"] != 0


@pytest.mark.asyncio
async def test_pending_exhaustion_is_durable_and_acknowledged_separately(
    supervisor,
):
    _server, client, _state_dir, _log_dir = supervisor
    await client.launch(SupervisedTaskLaunch(
        task_id="exhausted",
        command=[sys.executable, "-c", "import time; time.sleep(60)"],
        cwd=os.getcwd(),
        env=dict(os.environ),
        timeout_seconds=30,
        job_id="job-exhausted",
        watch_exhaustion=True,
    ))
    await client.mark_exhaustion(
        "exhausted",
        reason="rate_limit",
        event_id="stable-exhaustion-event",
    )
    inventory = await client.list_tasks()
    assert inventory[0].pending_exhaustion == {
        "event_id": "stable-exhaustion-event",
        "reason": "rate_limit",
    }
    await client.ack_event("stable-exhaustion-event")
    inventory = await client.list_tasks()
    assert inventory[0].pending_exhaustion is None


@pytest.mark.asyncio
async def test_worker_runtime_restart_replays_spool_and_adopts_same_pid(
    supervisor,
    tmp_path,
):
    server, _client, _state_dir, _log_dir = supervisor
    runtime_logs = tmp_path / "runtime-logs"
    first = WorkerRuntime(
        manager_url="ws://unused",
        auth_token="token",
        worker_id="worker-1",
        log_dir=str(runtime_logs),
        task_supervisor_socket=str(server.socket_path),
    )
    first._running = True
    assert await first._recover_supervised_task_inventory()

    from elastic_agent.core.protocols.messages import ExecuteMessage

    await first._handle_execute(ExecuteMessage(
        task_id="runtime-adoption",
        command=[
            sys.executable,
            "-u",
            "-c",
            "import time; print('checkpoint'); time.sleep(.5); print('finished')",
        ],
        cwd=str(tmp_path),
        timeout=10,
        job_id="job-adoption",
    ))
    original_pid = first._supervised_tasks["runtime-adoption"].pid
    await asyncio.sleep(0.15)
    await first.stop()
    assert server.running_task_ids == ["runtime-adoption"]

    restarted = WorkerRuntime(
        manager_url="ws://unused",
        auth_token="token",
        worker_id="worker-1",
        log_dir=str(runtime_logs),
        task_supervisor_socket=str(server.socket_path),
    )
    restarted._running = True
    assert await restarted._recover_supervised_task_inventory()
    assert restarted.active_processes == ["runtime-adoption"]
    assert restarted._supervised_tasks["runtime-adoption"].pid == original_pid

    monitor = restarted._supervised_monitor_tasks["runtime-adoption"]
    await asyncio.wait_for(monitor, timeout=10)
    terminal_events = [
        json.loads(data)
        for data in restarted._reliable_events.values()
        if json.loads(data)["type"] == "PROCESS_EXIT"
    ]
    assert len(terminal_events) == 1
    assert terminal_events[0]["exit_code"] == 0
    assert terminal_events[0]["task_id"] == "runtime-adoption"

    from elastic_agent.core.protocols.messages import EventAckMessage

    await restarted._handle_event_ack(EventAckMessage(
        event_id=terminal_events[0]["event_id"],
    ))
    assert await restarted._task_supervisor.list_tasks() == []
    await restarted.stop()


@pytest.mark.asyncio
async def test_runtime_supervised_exhaustion_precedes_stable_process_exit(
    supervisor,
    tmp_path,
):
    server, _client, _state_dir, _log_dir = supervisor
    runtime = WorkerRuntime(
        manager_url="ws://unused",
        auth_token="token",
        worker_id="worker-1",
        log_dir=str(tmp_path / "runtime"),
        task_supervisor_socket=str(server.socket_path),
    )
    runtime._running = True
    await runtime._recover_supervised_task_inventory()

    from elastic_agent.core.protocols.messages import ExecuteMessage

    await runtime._handle_execute(ExecuteMessage(
        task_id="supervised-exhaustion",
        command=[
            sys.executable,
            "-u",
            "-c",
            "import time; print('You hit your usage limit'); time.sleep(60)",
        ],
        cwd=str(tmp_path),
        timeout=30,
        job_id="job-exhausted",
        watch_exhaustion=True,
    ))
    await asyncio.wait_for(
        runtime._supervised_monitor_tasks["supervised-exhaustion"],
        timeout=25,
    )
    persisted = [
        json.loads(data)
        for data in runtime._reliable_events.values()
    ]
    assert [item["type"] for item in persisted] == [
        "RUN_EXHAUSTED",
        "PROCESS_EXIT",
    ]
    descriptor = (await runtime._task_supervisor.list_tasks())[0]
    assert (
        descriptor.pending_exhaustion["event_id"]
        == persisted[0]["event_id"]
    )
    assert descriptor.terminal_event_id == persisted[1]["event_id"]
    await runtime.stop()


@pytest.mark.asyncio
async def test_adopted_agent_api_task_retains_structured_failure_semantics(
    supervisor,
    tmp_path,
):
    server, client, _state_dir, _log_dir = supervisor
    frame = {
        "type": "turn.failed",
        "error": {"status": 403, "message": "Forbidden"},
    }
    await client.launch(SupervisedTaskLaunch(
        task_id="adopted-apex-auth",
        command=[
            sys.executable,
            "-c",
            f"import json;print(json.dumps({frame!r}))",
        ],
        cwd=str(tmp_path),
        env=dict(os.environ),
        timeout_seconds=10,
        job_id="job-apex",
        agent_api_provider="apex",
        agent_type="codex",
    ))
    runtime = WorkerRuntime(
        manager_url="ws://unused",
        auth_token="token",
        worker_id="worker-1",
        log_dir=str(tmp_path / "runtime"),
        task_supervisor_socket=str(server.socket_path),
    )
    runtime._running = True
    await runtime._recover_supervised_task_inventory()
    await asyncio.wait_for(
        runtime._supervised_monitor_tasks["adopted-apex-auth"],
        timeout=10,
    )

    exit_event = next(
        json.loads(data)
        for data in runtime._reliable_events.values()
        if json.loads(data)["type"] == "PROCESS_EXIT"
    )
    assert exit_event["exit_code"] == 1
    assert exit_event["error_type"] == "agent_api_auth_failure"
    assert exit_event["error_message"] == (
        "ApexRouter rejected the delegated API key"
    )
    await runtime.stop()


@pytest.mark.asyncio
async def test_inventoried_terminal_is_pending_before_runtime_outbox_bridge(
    supervisor,
    tmp_path,
):
    server, client, _state_dir, _log_dir = supervisor
    await client.launch(SupervisedTaskLaunch(
        task_id="terminal-handoff",
        command=[sys.executable, "-c", "raise SystemExit(0)"],
        cwd=str(tmp_path),
        env=dict(os.environ),
        timeout_seconds=10,
    ))
    await _wait_for_terminal(client, "terminal-handoff")
    descriptor = (await client.list_tasks())[0]
    assert descriptor.state == "terminal"

    runtime = WorkerRuntime(
        manager_url="ws://unused",
        auth_token="token",
        worker_id="worker-1",
        log_dir=str(tmp_path / "runtime"),
        task_supervisor_socket=str(server.socket_path),
    )
    runtime._supervised_tasks[descriptor.task_id] = descriptor

    assert await runtime._pending_process_exit_task_ids() == [
        "terminal-handoff"
    ]


@pytest.mark.asyncio
@pytest.mark.skipif(
    not hasattr(os, "O_NOFOLLOW"),
    reason="requires no-follow file opens",
)
async def test_spool_symlink_is_rejected_without_overwriting_target(
    supervisor,
    tmp_path,
):
    _server, client, _state_dir, log_dir = supervisor
    victim = tmp_path / "victim"
    victim.write_text("untouched")
    (log_dir / "unsafe-spool.ndjson").symlink_to(victim)

    await client.launch(SupervisedTaskLaunch(
        task_id="unsafe-spool",
        command=[sys.executable, "-c", "print('must not follow')"],
        cwd=str(tmp_path),
        env=dict(os.environ),
        timeout_seconds=10,
    ))
    _, terminal = await _wait_for_terminal(client, "unsafe-spool")

    assert terminal["error_type"] == "task_supervisor_error"
    assert victim.read_text() == "untouched"


@pytest.mark.asyncio
async def test_spool_limit_stops_noisy_task_without_hanging(tmp_path):
    socket_path = tmp_path / "run" / "control.sock"
    server = TaskSupervisorServer(
        socket_path=socket_path,
        state_dir=tmp_path / "state",
        log_dir=tmp_path / "logs",
        max_spool_bytes=600_000,
    )
    await server.start()
    client = TaskSupervisorClient(socket_path)
    try:
        await client.launch(SupervisedTaskLaunch(
            task_id="bounded-spool",
            command=[
                sys.executable,
                "-u",
                "-c",
                "import sys\nwhile True: sys.stdout.write('x' * 65535 + '\\n')",
            ],
            cwd=str(tmp_path),
            env=dict(os.environ),
            timeout_seconds=60,
        ))
        _, terminal = await _wait_for_terminal(
            client, "bounded-spool", timeout=10,
        )
        assert terminal["error_type"] == "task_supervisor_error"
        assert (tmp_path / "logs" / "bounded-spool.ndjson").stat().st_size <= 600_000
    finally:
        await server.stop(terminate_tasks=True)


@pytest.mark.asyncio
async def test_partial_spool_write_is_rolled_back_before_terminal(
    supervisor,
    tmp_path,
    monkeypatch,
):
    _server, client, _state_dir, log_dir = supervisor
    original_write_all = task_supervisor_module._write_all
    failed = False

    def partial_write(fd, data):
        nonlocal failed
        if not failed and data.startswith(b'{"task_id"'):
            failed = True
            os.write(fd, data[: max(1, len(data) // 2)])
            raise OSError("injected partial spool write")
        return original_write_all(fd, data)

    monkeypatch.setattr(task_supervisor_module, "_write_all", partial_write)
    await client.launch(SupervisedTaskLaunch(
        task_id="partial-spool",
        command=[
            sys.executable,
            "-u",
            "-c",
            "import time; print('partial-record'); time.sleep(60)",
        ],
        cwd=str(tmp_path),
        env=dict(os.environ),
        timeout_seconds=120,
    ))

    records, terminal = await _wait_for_terminal(
        client,
        "partial-spool",
    )
    assert records == []
    assert terminal["error_type"] == "task_supervisor_error"
    raw = (log_dir / "partial-spool.ndjson").read_bytes()
    assert raw == b""


@pytest.mark.asyncio
async def test_unrecoverable_partial_spool_tail_cannot_hide_terminal(
    supervisor,
    tmp_path,
    monkeypatch,
):
    _server, client, _state_dir, log_dir = supervisor
    original_write_all = task_supervisor_module._write_all
    failed = False

    def partial_write(fd, data):
        nonlocal failed
        if not failed and data.startswith(b'{"task_id"'):
            failed = True
            os.write(fd, data[: max(1, len(data) // 2)])
            raise OSError("injected partial spool write")
        return original_write_all(fd, data)

    def fail_rollback(_fd, _length):
        raise OSError("injected truncate failure")

    monkeypatch.setattr(task_supervisor_module, "_write_all", partial_write)
    monkeypatch.setattr(task_supervisor_module.os, "ftruncate", fail_rollback)
    await client.launch(SupervisedTaskLaunch(
        task_id="partial-spool-no-rollback",
        command=[
            sys.executable,
            "-u",
            "-c",
            "import time; print('partial-record'); time.sleep(60)",
        ],
        cwd=str(tmp_path),
        env=dict(os.environ),
        timeout_seconds=120,
    ))

    records, terminal = await _wait_for_terminal(
        client,
        "partial-spool-no-rollback",
    )
    assert records == []
    assert terminal["error_type"] == "task_supervisor_error"
    raw = (log_dir / "partial-spool-no-rollback.ndjson").read_bytes()
    assert raw and not raw.endswith(b"\n")


@pytest.mark.asyncio
async def test_terminal_commit_retries_using_reserved_space(
    supervisor,
    tmp_path,
    monkeypatch,
):
    server, client, state_dir, _log_dir = supervisor
    original_atomic_write = task_supervisor_module.atomic_write_private
    terminal_attempts = 0

    def flaky_atomic_write(path, data):
        nonlocal terminal_attempts
        if Path(path).name == "terminal.json":
            terminal_attempts += 1
            if terminal_attempts < 3:
                raise OSError("injected terminal ENOSPC")
        return original_atomic_write(path, data)

    monkeypatch.setattr(
        task_supervisor_module,
        "atomic_write_private",
        flaky_atomic_write,
    )
    await client.launch(SupervisedTaskLaunch(
        task_id="terminal-retry",
        command=[sys.executable, "-c", "raise SystemExit(4)"],
        cwd=str(tmp_path),
        env=dict(os.environ),
        timeout_seconds=10,
    ))

    _, terminal = await _wait_for_terminal(client, "terminal-retry")
    assert terminal["exit_code"] == 4
    assert terminal_attempts == 3
    assert server.fatal_error is None
    task_dir = next(
        path for path in state_dir.iterdir() if path.is_dir()
    )
    assert not (task_dir / "terminal.reserve").exists()


@pytest.mark.asyncio
async def test_permanent_terminal_commit_failure_marks_supervisor_fatal(
    supervisor,
    tmp_path,
    monkeypatch,
):
    server, client, _state_dir, _log_dir = supervisor
    original_atomic_write = task_supervisor_module.atomic_write_private

    def fail_terminal(path, data):
        if Path(path).name == "terminal.json":
            raise OSError("injected permanent terminal failure")
        return original_atomic_write(path, data)

    monkeypatch.setattr(
        task_supervisor_module,
        "atomic_write_private",
        fail_terminal,
    )
    await client.launch(SupervisedTaskLaunch(
        task_id="terminal-fatal",
        command=[sys.executable, "-c", "raise SystemExit(0)"],
        cwd=str(tmp_path),
        env=dict(os.environ),
        timeout_seconds=10,
    ))

    await asyncio.wait_for(server.fatal_event.wait(), timeout=5)
    assert server.fatal_error == "durable terminal state is unavailable"
    with pytest.raises(TaskSupervisorError):
        await client.list_tasks()


@pytest.mark.asyncio
async def test_runtime_cursor_advances_only_after_complete_batch_and_reloads(
    supervisor,
    tmp_path,
):
    server, client, _state_dir, _log_dir = supervisor
    await client.launch(SupervisedTaskLaunch(
        task_id="durable-cursor",
        command=[
            sys.executable,
            "-u",
            "-c",
            "print('first'); print('second')",
        ],
        cwd=str(tmp_path),
        env=dict(os.environ),
        timeout_seconds=10,
    ))
    runtime_log_dir = tmp_path / "runtime-cursor"
    runtime = WorkerRuntime(
        manager_url="ws://unused",
        auth_token="token",
        worker_id="worker-1",
        log_dir=str(runtime_log_dir),
        task_supervisor_socket=str(server.socket_path),
    )
    original_handler = runtime._handle_process_log_line

    async def fail_first_batch(*_args, **_kwargs):
        raise RuntimeError("injected relay failure")

    runtime._handle_process_log_line = fail_first_batch
    assert await runtime._recover_supervised_task_inventory()
    first_monitor = runtime._supervised_monitor_tasks["durable-cursor"]
    await asyncio.wait_for(first_monitor, timeout=5)
    assert runtime._supervised_offsets["durable-cursor"] == 0

    runtime._handle_process_log_line = original_handler
    assert await runtime._recover_supervised_task_inventory()
    second_monitor = runtime._supervised_monitor_tasks["durable-cursor"]
    await asyncio.wait_for(second_monitor, timeout=5)
    committed_offset = runtime._supervised_offsets["durable-cursor"]
    assert committed_offset > 0

    restarted = WorkerRuntime(
        manager_url="ws://unused",
        auth_token="token",
        worker_id="worker-1",
        log_dir=str(runtime_log_dir),
        task_supervisor_socket=str(server.socket_path),
    )
    assert restarted._supervised_offsets["durable-cursor"] == committed_offset
    await runtime.stop()
    await restarted.stop()


@pytest.mark.asyncio
async def test_supervisor_ack_failure_retains_outbox_until_retry(
    supervisor,
    tmp_path,
):
    server, _client, _state_dir, _log_dir = supervisor
    runtime = WorkerRuntime(
        manager_url="ws://unused",
        auth_token="token",
        worker_id="worker-1",
        log_dir=str(tmp_path / "runtime-ack"),
        task_supervisor_socket=str(server.socket_path),
    )
    runtime._running = True
    await runtime._recover_supervised_task_inventory()
    from elastic_agent.core.protocols.messages import (
        EventAckMessage,
        ExecuteMessage,
    )

    await runtime._handle_execute(ExecuteMessage(
        task_id="ack-retry",
        command=[sys.executable, "-c", "print('done')"],
        cwd=str(tmp_path),
        timeout=10,
    ))
    await asyncio.wait_for(
        runtime._supervised_monitor_tasks["ack-retry"],
        timeout=5,
    )
    event_id = next(
        event_id
        for event_id, data in runtime._reliable_events.items()
        if json.loads(data)["type"] == "PROCESS_EXIT"
    )
    supervisor_client = runtime._task_supervisor
    original_ack = runtime._task_supervisor.ack_event
    attempts = 0

    async def flaky_ack(candidate):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise TaskSupervisorError("injected ACK outage")
        await original_ack(candidate)

    runtime._task_supervisor.ack_event = flaky_ack
    await runtime._handle_event_ack(EventAckMessage(event_id=event_id))
    assert event_id in runtime._reliable_events
    assert not runtime._process_inventory_complete
    assert len(await supervisor_client.list_tasks()) == 1

    await runtime._handle_event_ack(EventAckMessage(event_id=event_id))
    assert event_id not in runtime._reliable_events
    assert "ack-retry" not in runtime._supervised_offsets
    assert await supervisor_client.list_tasks() == []
    await runtime.stop()


@pytest.mark.asyncio
async def test_exhaustion_fence_failure_replays_same_spool_frame(
    supervisor,
    tmp_path,
):
    server, client, _state_dir, _log_dir = supervisor
    await client.launch(SupervisedTaskLaunch(
        task_id="exhaustion-fence-retry",
        command=[
            sys.executable,
            "-u",
            "-c",
            "import time; print('You hit your usage limit'); time.sleep(60)",
        ],
        cwd=str(tmp_path),
        env=dict(os.environ),
        timeout_seconds=120,
        job_id="job-exhaustion-retry",
        watch_exhaustion=True,
    ))
    runtime = WorkerRuntime(
        manager_url="ws://unused",
        auth_token="token",
        worker_id="worker-1",
        log_dir=str(tmp_path / "runtime-exhaustion"),
        task_supervisor_socket=str(server.socket_path),
    )
    original_mark = runtime._task_supervisor.mark_exhaustion

    async def fail_mark(*_args, **_kwargs):
        raise TaskSupervisorError("injected exhaustion-fence outage")

    runtime._task_supervisor.mark_exhaustion = fail_mark
    assert await runtime._recover_supervised_task_inventory()
    first_monitor = runtime._supervised_monitor_tasks[
        "exhaustion-fence-retry"
    ]
    await asyncio.wait_for(first_monitor, timeout=5)
    assert runtime._supervised_offsets["exhaustion-fence-retry"] == 0
    assert "exhaustion-fence-retry" not in runtime._exhaustion_fired

    runtime._task_supervisor.mark_exhaustion = original_mark
    assert await runtime._recover_supervised_task_inventory()
    second_monitor = runtime._supervised_monitor_tasks[
        "exhaustion-fence-retry"
    ]
    await asyncio.wait_for(second_monitor, timeout=25)
    persisted_types = [
        json.loads(data)["type"]
        for data in runtime._reliable_events.values()
    ]
    assert persisted_types == ["RUN_EXHAUSTED", "PROCESS_EXIT"]
    await runtime.stop()


@pytest.mark.asyncio
async def test_ack_tombstone_wins_crash_before_descriptor_cleanup(
    supervisor,
    tmp_path,
):
    server, client, state_dir, log_dir = supervisor
    await client.launch(SupervisedTaskLaunch(
        task_id="acked-crash-window",
        command=[sys.executable, "-c", "raise SystemExit(0)"],
        cwd=str(tmp_path),
        env=dict(os.environ),
        timeout_seconds=10,
    ))
    await _wait_for_terminal(client, "acked-crash-window")
    server._completed_task_ids["acked-crash-window"] = None
    server._persist_task_tombstones()
    await server.stop(terminate_tasks=True)

    restarted_server = TaskSupervisorServer(
        socket_path=server.socket_path,
        state_dir=state_dir,
        log_dir=log_dir,
    )
    await restarted_server.start()
    try:
        restarted = TaskSupervisorClient(server.socket_path)
        assert await restarted.list_tasks() == []
        with pytest.raises(TaskSupervisorError):
            await restarted.launch(SupervisedTaskLaunch(
                task_id="acked-crash-window",
                command=[sys.executable, "-c", "raise SystemExit(99)"],
                cwd=str(tmp_path),
                env=dict(os.environ),
                timeout_seconds=10,
            ))
    finally:
        await restarted_server.stop(terminate_tasks=True)
