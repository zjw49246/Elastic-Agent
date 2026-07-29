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

import pytest

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
