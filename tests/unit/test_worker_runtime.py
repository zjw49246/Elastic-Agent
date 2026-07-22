"""Tests for Worker Runtime server (T-007)."""

from __future__ import annotations

import asyncio
import json
import os
import signal
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from elastic_agent.worker.runtime import WorkerRuntime


@pytest.fixture
def runtime(tmp_path):
    rt = WorkerRuntime(
        manager_url="ws://localhost:9999/ws/runtime",
        auth_token="test-token-123",
        worker_id="test-worker-1",
        heartbeat_interval=60,
        log_dir=str(tmp_path / "logs"),
    )
    return rt


class TestWorkerRuntimeInit:
    def test_initial_state(self, runtime):
        assert not runtime.connected
        assert runtime.active_processes == []
        assert runtime._worker_id == "test-worker-1"
        assert runtime._auth_token == "test-token-123"
        assert runtime._exiting_task_ids == set()

    def test_custom_log_dir(self, tmp_path):
        custom = tmp_path / "custom-logs"
        rt = WorkerRuntime(
            manager_url="ws://localhost:9999/ws/runtime",
            auth_token="tok",
            log_dir=str(custom),
        )
        assert rt._log_dir == custom

    @pytest.mark.asyncio
    async def test_reliable_exit_event_survives_restart_until_ack(self, tmp_path):
        from elastic_agent.core.protocols.messages import EventAckMessage, ProcessExitMessage

        log_dir = tmp_path / "durable-logs"
        first = WorkerRuntime(
            manager_url="ws://localhost/ws",
            auth_token="token",
            worker_id="worker-1",
            log_dir=str(log_dir),
        )
        event = ProcessExitMessage(task_id="task-1", exit_code=0)
        await first._send_event(event)

        assert event.event_id in first._reliable_events
        assert first._event_outbox_path.stat().st_mode & 0o777 == 0o600

        restarted = WorkerRuntime(
            manager_url="ws://localhost/ws",
            auth_token="token",
            worker_id="worker-1",
            log_dir=str(log_dir),
        )
        assert event.event_id in restarted._reliable_events

        await restarted._handle_event_ack(EventAckMessage(event_id=event.event_id))
        assert event.event_id not in restarted._reliable_events

        after_ack = WorkerRuntime(
            manager_url="ws://localhost/ws",
            auth_token="token",
            worker_id="worker-1",
            log_dir=str(log_dir),
        )
        assert after_ack._reliable_events == {}

    @pytest.mark.asyncio
    async def test_reliable_restart_replay_preserves_event_sequence(self, tmp_path):
        from elastic_agent.core.protocols.messages import (
            ProcessExitMessage,
            RunExhaustedMessage,
        )

        log_dir = tmp_path / "ordered-outbox"
        first = WorkerRuntime(
            manager_url="ws://localhost/ws",
            auth_token="token",
            worker_id="worker-1",
            log_dir=str(log_dir),
        )
        # Deliberately choose ids whose lexical order is opposite their event
        # order.  The v1 object + sort_keys format replayed these backwards.
        exhausted = RunExhaustedMessage(
            task_id="task-1",
            job_id="job-1",
            worker_id="worker-1",
            event_id="z-first",
        )
        exited = ProcessExitMessage(
            task_id="task-1", exit_code=130, event_id="a-second",
        )
        await first._send_event(exhausted)
        await first._send_event(exited)

        persisted = json.loads(first._event_outbox_path.read_text())
        assert persisted["version"] == 2
        assert [item["event_id"] for item in persisted["events"]] == [
            "z-first", "a-second",
        ]

        restarted = WorkerRuntime(
            manager_url="ws://localhost/ws",
            auth_token="token",
            worker_id="worker-1",
            log_dir=str(log_dir),
        )

        class WebSocket:
            def __init__(self):
                self.sent = []

            async def send(self, data):
                self.sent.append(json.loads(data)["type"])

        restarted._ws = WebSocket()
        await restarted._replay_reliable_events()
        assert restarted._ws.sent == ["RUN_EXHAUSTED", "PROCESS_EXIT"]

    def test_reliable_outbox_loads_legacy_object_format(self, tmp_path):
        from elastic_agent.core.protocols.messages import ProcessExitMessage

        log_dir = tmp_path / "legacy-outbox"
        log_dir.mkdir()
        first = ProcessExitMessage(
            task_id="task-1", exit_code=1, event_id="legacy-z",
        )
        second = ProcessExitMessage(
            task_id="task-2", exit_code=2, event_id="legacy-a",
        )
        (log_dir / "event_outbox.json").write_text(json.dumps({
            "version": 1,
            "events": {
                first.event_id: first.model_dump_json(),
                second.event_id: second.model_dump_json(),
            },
        }))

        restarted = WorkerRuntime(
            manager_url="ws://localhost/ws",
            auth_token="token",
            worker_id="worker-1",
            log_dir=str(log_dir),
        )
        assert list(restarted._reliable_events) == ["legacy-z", "legacy-a"]

    @pytest.mark.asyncio
    async def test_reliable_replay_is_sent_before_queued_status(self, runtime):
        from elastic_agent.core.protocols.messages import (
            ProcessExitMessage,
            StatusMessage,
        )

        class WebSocket:
            def __init__(self):
                self.sent = []

            async def send(self, data):
                self.sent.append(json.loads(data)["type"])

        event = ProcessExitMessage(task_id="task-1", exit_code=0)
        await runtime._send_event(event)
        await runtime._send_queue.put(StatusMessage(
            worker_id="worker-1", active_processes=[], cpu=0, mem=0, disk=0,
        ).model_dump_json())
        runtime._ws = WebSocket()

        await runtime._replay_reliable_events()

        assert runtime._ws.sent == ["PROCESS_EXIT"]

    @pytest.mark.asyncio
    async def test_pending_process_exit_is_advertised_until_ack(self, runtime):
        from elastic_agent.core.protocols.messages import (
            EventAckMessage,
            ProcessExitMessage,
            RunExhaustedMessage,
        )

        exited = ProcessExitMessage(task_id="task-finished", exit_code=0)
        await runtime._send_event(exited)
        await runtime._send_event(RunExhaustedMessage(
            task_id="task-other",
            job_id="job-1",
            worker_id="worker-1",
        ))

        assert await runtime._pending_process_exit_task_ids() == ["task-finished"]
        await runtime._handle_event_ack(EventAckMessage(event_id=exited.event_id))
        assert await runtime._pending_process_exit_task_ids() == []

    @pytest.mark.asyncio
    async def test_exiting_task_is_advertised_during_final_sync(self, runtime, tmp_path):
        class Process:
            returncode = 0
            stdout = None
            stderr = None

        class BlockingSync:
            def __init__(self):
                self.entered = asyncio.Event()
                self.release = asyncio.Event()

            async def force_sync(self, _task_id):
                self.entered.set()
                await self.release.wait()
                return 0

        task_id = "task-final-sync"
        process = Process()
        sync = BlockingSync()
        runtime._file_sync_manager = sync
        runtime._processes[task_id] = process

        monitor = asyncio.create_task(runtime._monitor_process(
            task_id, process, tmp_path / "process.ndjson", timeout=None,
        ))
        await asyncio.wait_for(sync.entered.wait(), timeout=1)

        assert task_id not in runtime.active_processes
        assert await runtime._pending_process_exit_task_ids() == [task_id]

        sync.release.set()
        await asyncio.wait_for(monitor, timeout=1)
        # After persistence the durable PROCESS_EXIT, rather than the transient
        # marker, keeps the task advertised until the Manager ACKs it.
        assert task_id not in runtime._exiting_task_ids
        assert await runtime._pending_process_exit_task_ids() == [task_id]

    @pytest.mark.asyncio
    async def test_failed_send_retries_ahead_of_later_frames(self, runtime):
        class FailingWebSocket:
            async def send(self, _data):
                raise ConnectionError("disconnected")

        await runtime._send_queue.put("first")
        await runtime._send_queue.put("second")
        runtime._running = True
        runtime._ws = FailingWebSocket()
        with pytest.raises(ConnectionError):
            await runtime._sender_loop()
        assert runtime._retry_send == "first"

        sent = []

        class WorkingWebSocket:
            async def send(self, data):
                sent.append(data)
                if len(sent) == 2:
                    runtime._running = False

        runtime._ws = WorkingWebSocket()
        await runtime._sender_loop()
        assert sent == ["first", "second"]


class TestNDJSONParsing:
    def test_valid_claude_output(self):
        line = json.dumps({"type": "assistant", "content": "hello", "subtype": "text"})
        result = WorkerRuntime._try_parse_ndjson(line)
        assert result is not None
        assert result["type"] == "assistant"
        assert result["subtype"] == "text"
        assert result["cost_usd"] is None
        assert result["session_id"] is None

    def test_result_with_session_and_cost(self):
        line = json.dumps({
            "type": "result",
            "session_id": "sess-abc",
            "cost_usd": 0.05,
        })
        result = WorkerRuntime._try_parse_ndjson(line)
        assert result["type"] == "result"
        assert result["session_id"] == "sess-abc"
        assert result["cost_usd"] == 0.05

    def test_non_json_line(self):
        assert WorkerRuntime._try_parse_ndjson("plain text output") is None

    def test_json_without_type(self):
        line = json.dumps({"data": "value"})
        assert WorkerRuntime._try_parse_ndjson(line) is None

    def test_empty_string(self):
        assert WorkerRuntime._try_parse_ndjson("") is None

    def test_json_array(self):
        assert WorkerRuntime._try_parse_ndjson("[1,2,3]") is None


class TestProcessExecution:
    @pytest.mark.asyncio
    async def test_execute_and_capture_output(self, runtime, tmp_path):
        """Test that EXECUTE starts a process and captures stdout/stderr."""
        from elastic_agent.core.protocols.messages import ExecuteMessage

        sent_messages: list[str] = []

        async def mock_send_event(msg):
            sent_messages.append(msg.model_dump_json())

        runtime._send_event = mock_send_event
        runtime._running = True

        msg = ExecuteMessage(
            task_id="task-1",
            command=[sys.executable, "-c", "import sys; print('hello stdout'); print('error line', file=sys.stderr)"],
            cwd=str(tmp_path),
        )

        await runtime._handle_execute(msg)
        assert "task-1" in runtime._processes

        task = runtime._process_tasks["task-1"]
        await task

        assert "task-1" not in runtime._processes

        log_msgs = [json.loads(m) for m in sent_messages if '"LOG"' in m]
        exit_msgs = [json.loads(m) for m in sent_messages if '"PROCESS_EXIT"' in m]

        stdout_logs = [m for m in log_msgs if m["stream"] == "stdout"]
        stderr_logs = [m for m in log_msgs if m["stream"] == "stderr"]

        assert len(stdout_logs) >= 1
        assert any("hello stdout" in m["data"] for m in stdout_logs)
        assert len(stderr_logs) >= 1
        assert any("error line" in m["data"] for m in stderr_logs)

        assert len(exit_msgs) == 1
        assert exit_msgs[0]["exit_code"] == 0

    @pytest.mark.asyncio
    async def test_exit_reported_even_when_child_holds_pipe_open(self, runtime, tmp_path, monkeypatch):
        """Regression: a run process can exit while a lingering child (e.g. a
        docker container from `--sandbox os`) keeps stdout open, so it never
        EOFs. The exit MUST still be reported (bounded drain) — otherwise the
        Manager's run phase stays RUNNING forever and collect/S3-upload never
        fire. Without the bounded drain this hangs ~20s and times out."""
        import elastic_agent.worker.runtime as rt_mod
        from elastic_agent.core.protocols.messages import ExecuteMessage

        monkeypatch.setattr(rt_mod, "_EXIT_DRAIN_TIMEOUT", 0.5)
        sent: list[str] = []

        async def cap(msg):
            sent.append(msg.model_dump_json())

        runtime._send_event = cap
        runtime._running = True

        leak = "import subprocess,sys; subprocess.Popen(['sleep','20']); print('done',flush=True); sys.exit(0)"
        msg = ExecuteMessage(task_id="t-leak", command=[sys.executable, "-c", leak], cwd=str(tmp_path))

        await runtime._handle_execute(msg)
        # Must complete well under the child's 20s (drain bounded to 0.5s).
        await asyncio.wait_for(runtime._process_tasks["t-leak"], timeout=6)

        exit_msgs = [json.loads(m) for m in sent if '"PROCESS_EXIT"' in m]
        assert len(exit_msgs) == 1
        assert exit_msgs[0]["exit_code"] == 0

    @pytest.mark.asyncio
    async def test_exit_reported_when_log_open_fails(self, runtime, tmp_path, monkeypatch):
        """A mis-permissioned log dir must not swallow the exit: if opening the
        per-task log fails, degrade to no local log — don't crash _monitor_process
        before ProcessExitMessage is sent (else the Manager's run phase sticks at
        RUNNING and collect/S3 never fire). This was the real 'stuck at running'."""
        import builtins
        import elastic_agent.worker.runtime as rt_mod
        from elastic_agent.core.protocols.messages import ExecuteMessage

        real_open = builtins.open

        def failing_open(path, *a, **k):
            if str(path).endswith(".ndjson"):
                raise PermissionError("log dir not writable")
            return real_open(path, *a, **k)

        monkeypatch.setattr(rt_mod, "open", failing_open, raising=False)
        sent: list[str] = []

        async def cap(msg):
            sent.append(msg.model_dump_json())

        runtime._send_event = cap
        runtime._running = True

        msg = ExecuteMessage(task_id="t-nolog",
                             command=[sys.executable, "-c", "print('hi')"], cwd=str(tmp_path))
        await runtime._handle_execute(msg)
        await asyncio.wait_for(runtime._process_tasks["t-nolog"], timeout=6)

        exit_msgs = [json.loads(m) for m in sent if '"PROCESS_EXIT"' in m]
        assert len(exit_msgs) == 1
        assert exit_msgs[0]["exit_code"] == 0

    @pytest.mark.asyncio
    async def test_execute_logs_to_file(self, runtime, tmp_path):
        """Test that process output is dual-written to a local NDJSON file."""
        from elastic_agent.core.protocols.messages import ExecuteMessage

        sent_messages: list[str] = []

        async def mock_send_event(msg):
            sent_messages.append(msg.model_dump_json())

        runtime._send_event = mock_send_event
        runtime._running = True

        msg = ExecuteMessage(
            task_id="task-log",
            command=[sys.executable, "-c", "print('logged line')"],
            cwd=str(tmp_path),
        )

        await runtime._handle_execute(msg)
        await runtime._process_tasks["task-log"]

        log_file = runtime._log_dir / "task-log.ndjson"
        assert log_file.exists()
        lines = [json.loads(l) for l in log_file.read_text().strip().splitlines()]
        assert len(lines) >= 1
        assert any(l["data"] == "logged line" for l in lines)

    @pytest.mark.asyncio
    async def test_execute_nonexistent_command(self, runtime, tmp_path):
        """Test that executing a nonexistent command sends error + exit events."""
        from elastic_agent.core.protocols.messages import ExecuteMessage

        sent_messages: list[str] = []

        async def mock_send_event(msg):
            sent_messages.append(msg.model_dump_json())

        runtime._send_event = mock_send_event
        runtime._running = True

        msg = ExecuteMessage(
            task_id="task-bad",
            command=["/nonexistent/binary"],
            cwd=str(tmp_path),
        )

        await runtime._handle_execute(msg)

        error_msgs = [json.loads(m) for m in sent_messages if '"ERROR"' in m]
        exit_msgs = [json.loads(m) for m in sent_messages if '"PROCESS_EXIT"' in m]

        assert len(error_msgs) >= 1
        assert error_msgs[0]["error_type"] == "execute_failed"
        assert len(exit_msgs) == 1
        assert exit_msgs[0]["exit_code"] == -1

    @pytest.mark.asyncio
    async def test_duplicate_task_rejected(self, runtime, tmp_path):
        """Test that starting a second process with same task_id is rejected."""
        from elastic_agent.core.protocols.messages import ExecuteMessage

        sent_messages: list[str] = []

        async def mock_send_event(msg):
            sent_messages.append(msg.model_dump_json())

        runtime._send_event = mock_send_event
        runtime._running = True

        msg = ExecuteMessage(
            task_id="task-dup",
            command=[sys.executable, "-c", "import time; time.sleep(10)"],
            cwd=str(tmp_path),
        )

        await runtime._handle_execute(msg)
        assert "task-dup" in runtime._processes

        await runtime._handle_execute(msg)

        error_msgs = [json.loads(m) for m in sent_messages if '"duplicate_task"' in m]
        assert len(error_msgs) == 1

        await runtime._stop_process("task-dup", "SIGKILL")
        await runtime._process_tasks["task-dup"]


class TestExhaustionWatch:
    """Mode-B rotation (a): watch opaque-command output for exhaustion banners."""

    @pytest.mark.asyncio
    async def test_banner_signals_and_interrupts(self, runtime, tmp_path):
        from elastic_agent.core.protocols.messages import ExecuteMessage

        sent: list[dict] = []

        async def mock_send_event(msg):
            sent.append(json.loads(msg.model_dump_json()))

        runtime._send_event = mock_send_event
        runtime._running = True

        # Print a usage-limit banner then block; the worker should interrupt.
        msg = ExecuteMessage(
            task_id="task-exh",
            command=[sys.executable, "-u", "-c",
                     "print('You hit your usage limit'); import time; time.sleep(30)"],
            cwd=str(tmp_path),
            job_id="job-xyz",
            watch_exhaustion=True,
        )
        await runtime._handle_execute(msg)
        await asyncio.wait_for(runtime._process_tasks["task-exh"], timeout=20)

        exh = [m for m in sent if m["type"] == "RUN_EXHAUSTED"]
        assert len(exh) == 1
        assert exh[0]["job_id"] == "job-xyz"
        assert exh[0]["worker_id"] == "test-worker-1"
        assert exh[0]["reason"] == "rate_limit"
        # process was interrupted → cleaned up
        assert "task-exh" not in runtime._processes
        assert "task-exh" not in runtime._exhaustion_watch

    @pytest.mark.asyncio
    async def test_no_watch_no_signal(self, runtime, tmp_path):
        from elastic_agent.core.protocols.messages import ExecuteMessage

        sent: list[dict] = []

        async def mock_send_event(msg):
            sent.append(json.loads(msg.model_dump_json()))

        runtime._send_event = mock_send_event
        runtime._running = True

        # Same banner, but watch disabled → run completes normally, no signal.
        msg = ExecuteMessage(
            task_id="task-nowatch",
            command=[sys.executable, "-u", "-c", "print('You hit your usage limit')"],
            cwd=str(tmp_path),
            watch_exhaustion=False,
        )
        await runtime._handle_execute(msg)
        await asyncio.wait_for(runtime._process_tasks["task-nowatch"], timeout=20)

        assert not [m for m in sent if m["type"] == "RUN_EXHAUSTED"]

    @pytest.mark.asyncio
    async def test_signal_exhaustion_dedupes(self, runtime):
        sent: list[dict] = []

        async def mock_send_event(msg):
            sent.append(json.loads(msg.model_dump_json()))

        runtime._send_event = mock_send_event
        runtime._exhaustion_watch["t"] = "job-1"

        await runtime._signal_exhaustion("t")
        await runtime._signal_exhaustion("t")  # second call is a no-op

        assert len([m for m in sent if m["type"] == "RUN_EXHAUSTED"]) == 1

    @pytest.mark.asyncio
    async def test_signal_exhaustion_stops_old_process_before_event(self, runtime):
        order = []

        async def stop(task_id, signal_name):
            order.append(("stop", task_id, signal_name))

        async def send(message):
            order.append(("send", message.type))

        runtime._stop_process = stop
        runtime._send_event = send
        runtime._exhaustion_watch["t"] = "job-1"

        await runtime._signal_exhaustion("t")

        assert order == [
            ("stop", "t", "SIGINT"),
            ("send", "RUN_EXHAUSTED"),
        ]

    @pytest.mark.asyncio
    async def test_signal_exhaustion_confirms_exit_before_event(self, runtime):
        order = []
        process = MagicMock()
        process.returncode = None
        runtime._processes["t"] = process

        async def stop(task_id, signal_name):
            order.append(("stop", task_id, signal_name))

        async def wait_for_exit(observed, timeout):
            assert observed is process
            order.append(("wait", timeout))
            process.returncode = 130
            return True

        async def send(message):
            order.append(("send", message.type))

        runtime._stop_process = stop
        runtime._wait_process_exit = wait_for_exit
        runtime._send_event = send
        runtime._exhaustion_watch["t"] = "job-1"

        await runtime._signal_exhaustion("t")

        assert order == [
            ("stop", "t", "SIGINT"),
            ("wait", 15),
            ("send", "RUN_EXHAUSTED"),
        ]


class TestStopProcess:
    @pytest.mark.asyncio
    async def test_stop_running_process(self, runtime, tmp_path):
        """Test that STOP sends signal and process exits."""
        from elastic_agent.core.protocols.messages import ExecuteMessage, StopMessage

        sent_messages: list[str] = []

        async def mock_send_event(msg):
            sent_messages.append(msg.model_dump_json())

        runtime._send_event = mock_send_event
        runtime._running = True

        msg = ExecuteMessage(
            task_id="task-stop",
            command=[sys.executable, "-c", "import time; time.sleep(60)"],
            cwd=str(tmp_path),
        )

        await runtime._handle_execute(msg)
        assert "task-stop" in runtime._processes

        process_task = runtime._process_tasks["task-stop"]
        await asyncio.sleep(0.2)

        stop_msg = StopMessage(task_id="task-stop", signal="SIGTERM")
        await runtime._handle_stop(stop_msg)
        await process_task

        exit_msgs = [json.loads(m) for m in sent_messages if '"PROCESS_EXIT"' in m]
        assert len(exit_msgs) == 1
        assert exit_msgs[0]["exit_code"] != 0

    @pytest.mark.asyncio
    async def test_stop_nonexistent_process(self, runtime):
        """Test stopping a process that doesn't exist is a no-op."""
        from elastic_agent.core.protocols.messages import StopMessage

        stop_msg = StopMessage(task_id="nonexistent", signal="SIGTERM")
        await runtime._handle_stop(stop_msg)


class TestReadFile:
    @pytest.mark.asyncio
    async def test_read_existing_file(self, runtime, tmp_path):
        from elastic_agent.core.protocols.messages import ReadFileMessage

        test_file = tmp_path / "test.txt"
        test_file.write_text("hello world")

        sent_messages: list[str] = []

        async def mock_send_event(msg):
            sent_messages.append(msg.model_dump_json())

        runtime._send_event = mock_send_event

        msg = ReadFileMessage(request_id="req-1", path=str(test_file))
        await runtime._handle_read_file(msg)

        content_msgs = [json.loads(m) for m in sent_messages if '"FILE_CONTENT"' in m]
        assert len(content_msgs) == 1
        assert content_msgs[0]["content"] == "hello world"
        assert content_msgs[0]["request_id"] == "req-1"

    @pytest.mark.asyncio
    async def test_read_nonexistent_file(self, runtime):
        from elastic_agent.core.protocols.messages import ReadFileMessage

        sent_messages: list[str] = []

        async def mock_send_event(msg):
            sent_messages.append(msg.model_dump_json())

        runtime._send_event = mock_send_event

        msg = ReadFileMessage(request_id="req-2", path="/nonexistent/file.txt")
        await runtime._handle_read_file(msg)

        error_msgs = [json.loads(m) for m in sent_messages if '"ERROR"' in m]
        assert len(error_msgs) == 1
        assert "read_file_failed" in error_msgs[0]["error_type"]


class TestUploadFile:
    @pytest.mark.asyncio
    async def test_upload_file(self, runtime, tmp_path):
        from elastic_agent.core.protocols.messages import UploadFileMessage
        import base64

        target = tmp_path / "uploaded" / "data.txt"
        content = base64.b64encode(b"file content here").decode()

        sent_messages: list[str] = []

        async def mock_send_event(msg):
            sent_messages.append(msg.model_dump_json())

        runtime._send_event = mock_send_event

        msg = UploadFileMessage(path=str(target), content_base64=content, mode="0644")
        await runtime._handle_upload_file(msg)

        assert target.exists()
        assert target.read_text() == "file content here"


class TestHealthCheck:
    @pytest.mark.asyncio
    async def test_health_check_response(self, runtime):
        from elastic_agent.core.protocols.messages import HealthCheckMessage

        sent_messages: list[str] = []

        async def mock_send_event(msg):
            sent_messages.append(msg.model_dump_json())

        runtime._send_event = mock_send_event
        runtime._check_claude_cli = lambda: {
            "ok": True,
            "path": "/usr/bin/claude",
            "version": "2.1.181 (Claude Code)",
            "error": None,
        }

        await runtime._handle_health_check(HealthCheckMessage())

        status_msgs = [json.loads(m) for m in sent_messages if '"STATUS"' in m]
        assert len(status_msgs) == 1
        s = status_msgs[0]
        assert "cpu" in s
        assert "mem" in s
        assert "disk" in s
        assert isinstance(s["active_processes"], list)
        assert s["runtime_ready"] is True
        assert s["claude_cli_ok"] is True
        assert s["claude_version"] == "2.1.181 (Claude Code)"

    @pytest.mark.asyncio
    async def test_codex_worker_health_uses_codex_cli(
        self, runtime, monkeypatch,
    ):
        from elastic_agent.core.protocols.messages import HealthCheckMessage

        sent = []
        runtime._send_event = lambda message: sent.append(message) or asyncio.sleep(0)
        runtime._check_claude_cli = MagicMock(side_effect=AssertionError(
            "Codex worker must not require Claude"
        ))
        runtime._check_codex_cli = lambda: {
            "ok": True,
            "path": "/usr/local/bin/codex",
            "version": "codex-cli 0.144.6",
            "error": None,
        }
        monkeypatch.setenv("ELASTIC_AGENT_AGENT_TYPE", "codex")

        await runtime._handle_health_check(HealthCheckMessage())

        status = sent[0]
        assert status.runtime_ready is True
        assert status.agent_type == "codex"
        assert status.codex_cli_ok is True
        assert status.codex_version == "codex-cli 0.144.6"
        assert status.claude_cli_ok is False


class TestForceSyncOnExit:
    @pytest.mark.asyncio
    async def test_force_sync_called_on_process_exit(self, runtime, tmp_path):
        """T-038: FileSyncManager.force_sync() is called when process exits."""
        from elastic_agent.core.protocols.messages import ExecuteMessage
        from unittest.mock import AsyncMock

        sent_messages: list[str] = []

        async def mock_send_event(msg):
            sent_messages.append(msg.model_dump_json())

        runtime._send_event = mock_send_event
        runtime._running = True

        mock_fsm = AsyncMock()
        mock_fsm.force_sync = AsyncMock(return_value=2)
        runtime._file_sync_manager = mock_fsm

        msg = ExecuteMessage(
            task_id="task-sync",
            command=[sys.executable, "-c", "print('done')"],
            cwd=str(tmp_path),
        )
        await runtime._handle_execute(msg)
        await runtime._process_tasks["task-sync"]

        mock_fsm.force_sync.assert_called_once_with("task-sync")

    @pytest.mark.asyncio
    async def test_force_sync_error_does_not_block_exit(self, runtime, tmp_path):
        """T-038: Even if force_sync fails, PROCESS_EXIT is still sent."""
        from elastic_agent.core.protocols.messages import ExecuteMessage
        from unittest.mock import AsyncMock

        sent_messages: list[str] = []

        async def mock_send_event(msg):
            sent_messages.append(msg.model_dump_json())

        runtime._send_event = mock_send_event
        runtime._running = True

        mock_fsm = AsyncMock()
        mock_fsm.force_sync = AsyncMock(side_effect=RuntimeError("sync failed"))
        runtime._file_sync_manager = mock_fsm

        msg = ExecuteMessage(
            task_id="task-sync-err",
            command=[sys.executable, "-c", "print('done')"],
            cwd=str(tmp_path),
        )
        await runtime._handle_execute(msg)
        await runtime._process_tasks["task-sync-err"]

        exit_msgs = [json.loads(m) for m in sent_messages if '"PROCESS_EXIT"' in m]
        assert len(exit_msgs) == 1
        assert exit_msgs[0]["exit_code"] == 0

    @pytest.mark.asyncio
    async def test_no_force_sync_when_no_fsm(self, runtime, tmp_path):
        """T-038: No error when file_sync_manager is None."""
        from elastic_agent.core.protocols.messages import ExecuteMessage

        sent_messages: list[str] = []

        async def mock_send_event(msg):
            sent_messages.append(msg.model_dump_json())

        runtime._send_event = mock_send_event
        runtime._running = True
        runtime._file_sync_manager = None

        msg = ExecuteMessage(
            task_id="task-no-fsm",
            command=[sys.executable, "-c", "print('done')"],
            cwd=str(tmp_path),
        )
        await runtime._handle_execute(msg)
        await runtime._process_tasks["task-no-fsm"]

        exit_msgs = [json.loads(m) for m in sent_messages if '"PROCESS_EXIT"' in m]
        assert len(exit_msgs) == 1


class TestMessageInput:
    @pytest.mark.asyncio
    async def test_send_input_no_process(self, runtime):
        from elastic_agent.core.protocols.messages import SendInputMessage

        sent_messages: list[str] = []

        async def mock_send_event(msg):
            sent_messages.append(msg.model_dump_json())

        runtime._send_event = mock_send_event

        msg = SendInputMessage(task_id="no-such-task", payload="hello")
        await runtime._handle_message(msg)

        error_msgs = [json.loads(m) for m in sent_messages if '"no_process"' in m]
        assert len(error_msgs) == 1


class TestSyncMappingStorageBackend:
    @pytest.mark.asyncio
    async def test_register_sync_mapping_uses_env_storage_type(self, runtime, monkeypatch):
        """Worker Runtime creates OSSBackend when STORAGE_TYPE=oss is set."""
        from elastic_agent.core.protocols.messages import RegisterSyncMappingMessage
        from elastic_agent.worker.file_sync import OSSBackend

        monkeypatch.setenv("STORAGE_TYPE", "oss")
        monkeypatch.setenv("OSS_BUCKET", "test-bucket")
        monkeypatch.setenv("OSS_ENDPOINT", "oss-cn-hangzhou.aliyuncs.com")
        monkeypatch.setenv("OSS_ACCESS_KEY_ID", "ak")
        monkeypatch.setenv("OSS_ACCESS_KEY_SECRET", "sk")

        sent_messages: list[str] = []

        async def mock_send_event(msg):
            sent_messages.append(msg.model_dump_json())

        runtime._send_event = mock_send_event

        msg = RegisterSyncMappingMessage(
            task_id="t1",
            book_slug="book-1",
            oss_prefix="tasks/t1",
            watch_paths=["/tmp/nonexistent"],
            session_path_hash="abc",
        )
        await runtime._handle_register_sync_mapping(msg)

        assert runtime._file_sync_manager is not None
        assert isinstance(runtime._file_sync_manager._storage, OSSBackend)
        await runtime._file_sync_manager.stop()

    @pytest.mark.asyncio
    async def test_register_sync_mapping_defaults_to_local(self, runtime, monkeypatch):
        """Worker Runtime falls back to LocalBackend when STORAGE_TYPE is not set."""
        from elastic_agent.core.protocols.messages import RegisterSyncMappingMessage
        from elastic_agent.worker.file_sync import LocalBackend

        monkeypatch.delenv("STORAGE_TYPE", raising=False)

        sent_messages: list[str] = []

        async def mock_send_event(msg):
            sent_messages.append(msg.model_dump_json())

        runtime._send_event = mock_send_event

        msg = RegisterSyncMappingMessage(
            task_id="t2",
            book_slug="book-2",
            oss_prefix="tasks/t2",
            watch_paths=["/tmp/nonexistent"],
            session_path_hash="def",
        )
        await runtime._handle_register_sync_mapping(msg)

        assert runtime._file_sync_manager is not None
        assert isinstance(runtime._file_sync_manager._storage, LocalBackend)
        await runtime._file_sync_manager.stop()


class TestHandleAccountLogin:
    """P3: worker-autonomous login — the Manager sends the account identity +
    接码 token; the worker logs itself in locally."""

    def _msg(self, **over):
        from elastic_agent.core.protocols.messages import AccountLoginMessage

        base = dict(
            login_request_id="login-request-1",
            account_id="a1", email="u@foo.com", email_token="tok",
            password="", agent_type="claude",
            config_dir="/root/.claude-prod", provider="171mail", slot_index=2,
        )
        base.update(over)
        return AccountLoginMessage(**base)

    @pytest.mark.asyncio
    async def test_success_warms_recycles_and_reports(self, runtime):
        from elastic_agent.core.claude_oauth import LoginResult
        from elastic_agent.core.protocols.messages import AccountLoginResultMessage

        sent = []
        runtime._send_event = lambda m: sent.append(m) or asyncio.sleep(0)
        runtime._verify_config_identity = AsyncMock(return_value=True)
        runtime._warmup_config_dir = AsyncMock(return_value=True)
        runtime._pty_backend = MagicMock()
        runtime._pty_backend.recycle_config_dir = AsyncMock(return_value=1)
        runtime._quota_checker = MagicMock()

        with patch(
            "elastic_agent.core.claude_oauth.ClaudeOAuthProvider.login",
            new=AsyncMock(return_value=LoginResult(
                success=True, account_id="a1", access_token="at")),
        ):
            await runtime._handle_account_login(self._msg())

        runtime._warmup_config_dir.assert_awaited_once_with("/root/.claude-prod")
        runtime._pty_backend.recycle_config_dir.assert_awaited_once_with("/root/.claude-prod")
        runtime._quota_checker.add_slot.assert_called_once_with("a1", "/root/.claude-prod")
        res = [m for m in sent if isinstance(m, AccountLoginResultMessage)][0]
        assert res.success and res.account_id == "a1" and res.slot_index == 2
        assert res.login_request_id == "login-request-1"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("raw_config_dir", ["", "relative-slot"])
    async def test_empty_or_relative_config_dir_is_normalized_for_all_consumers(
        self, runtime, tmp_path, raw_config_dir,
    ):
        from elastic_agent.core.claude_oauth import LoginResult

        expected = str(tmp_path / ".claude")
        runtime._send_event = AsyncMock()
        runtime._verify_config_identity = AsyncMock(return_value=True)
        runtime._warmup_config_dir = AsyncMock(return_value=True)
        runtime._pty_backend = MagicMock()
        runtime._pty_backend.recycle_config_dir = AsyncMock(return_value=1)
        runtime._quota_checker = MagicMock()
        login = AsyncMock(return_value=LoginResult(
            success=True, account_id="a1", access_token="at",
        ))

        with (
            patch("pathlib.Path.home", return_value=tmp_path),
            patch("elastic_agent.core.claude_oauth.ClaudeOAuthProvider.login", new=login),
        ):
            await runtime._handle_account_login(self._msg(config_dir=raw_config_dir))

        assert login.await_args.args[0].config_dir == expected
        runtime._warmup_config_dir.assert_awaited_once_with(expected)
        runtime._pty_backend.recycle_config_dir.assert_awaited_once_with(expected)
        runtime._quota_checker.add_slot.assert_called_once_with("a1", expected)

    @pytest.mark.asyncio
    async def test_failure_reports_error_without_recycle(self, runtime):
        from elastic_agent.core.claude_oauth import LoginResult
        from elastic_agent.core.protocols.messages import AccountLoginResultMessage

        sent = []
        runtime._send_event = lambda m: sent.append(m) or asyncio.sleep(0)
        runtime._warmup_config_dir = AsyncMock()
        runtime._pty_backend = MagicMock()
        runtime._pty_backend.recycle_config_dir = AsyncMock()

        with patch(
            "elastic_agent.core.claude_oauth.ClaudeOAuthProvider.login",
            new=AsyncMock(return_value=LoginResult(
                success=False, account_id="a1", error="cf blocked")),
        ):
            await runtime._handle_account_login(self._msg())

        runtime._warmup_config_dir.assert_not_awaited()
        runtime._pty_backend.recycle_config_dir.assert_not_awaited()
        res = [m for m in sent if isinstance(m, AccountLoginResultMessage)][0]
        assert not res.success and res.error == "cf blocked"

    @pytest.mark.asyncio
    async def test_failed_credential_validation_reports_login_failure(
        self, runtime
    ):
        from elastic_agent.core.claude_oauth import LoginResult
        from elastic_agent.core.protocols.messages import AccountLoginResultMessage

        sent = []
        runtime._send_event = lambda m: sent.append(m) or asyncio.sleep(0)
        runtime._verify_config_identity = AsyncMock(return_value=True)
        runtime._warmup_config_dir = AsyncMock(return_value=False)
        runtime._pty_backend = MagicMock()
        runtime._pty_backend.recycle_config_dir = AsyncMock()
        runtime._quota_checker = MagicMock()

        with patch(
            "elastic_agent.core.claude_oauth.ClaudeOAuthProvider.login",
            new=AsyncMock(return_value=LoginResult(
                success=True, account_id="a1", access_token="at"
            )),
        ):
            await runtime._handle_account_login(self._msg())

        res = [m for m in sent if isinstance(m, AccountLoginResultMessage)][0]
        assert res.success is False
        assert "validation command failed" in res.error
        runtime._pty_backend.recycle_config_dir.assert_not_awaited()
        runtime._quota_checker.add_slot.assert_not_called()

    @pytest.mark.asyncio
    async def test_wrong_authenticated_email_never_runs_on_selected_account(
        self, runtime
    ):
        from elastic_agent.core.claude_oauth import LoginResult
        from elastic_agent.core.protocols.messages import AccountLoginResultMessage

        sent = []
        runtime._send_event = lambda m: sent.append(m) or asyncio.sleep(0)
        runtime._verify_config_identity = AsyncMock(return_value=False)
        runtime._warmup_config_dir = AsyncMock(return_value=True)
        runtime._pty_backend = MagicMock()
        runtime._pty_backend.recycle_config_dir = AsyncMock()
        runtime._quota_checker = MagicMock()

        with patch(
            "elastic_agent.core.claude_oauth.ClaudeOAuthProvider.login",
            new=AsyncMock(return_value=LoginResult(
                success=True, account_id="a1", access_token="at"
            )),
        ):
            await runtime._handle_account_login(self._msg(email="expected@x.com"))

        runtime._verify_config_identity.assert_awaited_once_with(
            "/root/.claude-prod", "expected@x.com"
        )
        runtime._warmup_config_dir.assert_not_awaited()
        runtime._pty_backend.recycle_config_dir.assert_not_awaited()
        runtime._quota_checker.add_slot.assert_not_called()
        result = [
            message
            for message in sent
            if isinstance(message, AccountLoginResultMessage)
        ][0]
        assert result.success is False
        assert "different or unknown email" in result.error

    @pytest.mark.asyncio
    async def test_login_exception_reports_failure(self, runtime):
        from elastic_agent.core.protocols.messages import AccountLoginResultMessage

        sent = []
        runtime._send_event = lambda m: sent.append(m) or asyncio.sleep(0)
        with patch(
            "elastic_agent.core.claude_oauth.ClaudeOAuthProvider.login",
            new=AsyncMock(side_effect=RuntimeError("boom")),
        ):
            await runtime._handle_account_login(self._msg())
        res = [m for m in sent if isinstance(m, AccountLoginResultMessage)][0]
        assert not res.success and "boom" in res.error

    @pytest.mark.asyncio
    async def test_codex_password_login_uses_codex_home_and_reports_success(
        self, runtime,
    ):
        from elastic_agent.core.protocols.messages import AccountLoginResultMessage

        sent = []
        runtime._send_event = lambda message: sent.append(message) or asyncio.sleep(0)
        login = AsyncMock(return_value={"ok": True, "logs": []})
        message = self._msg(
            agent_type="codex",
            email_token="",
            password="openai-secret",
            config_dir="/root/.codex-a1",
        )

        with patch(
            "elastic_agent.worker.login.codex_login.codex_login", new=login,
        ):
            await runtime._handle_account_login(message)

        kwargs = login.await_args.kwargs
        assert kwargs["email"] == "u@foo.com"
        assert kwargs["password"] == "openai-secret"
        assert kwargs["token_171"] == ""
        assert kwargs["codex_home"] == "/root/.codex-a1"
        assert kwargs["attempt_id"] == "login-request-1"
        result = next(
            item for item in sent if isinstance(item, AccountLoginResultMessage)
        )
        assert result.success is True
        assert runtime._account_login_otp_readers == {}

    @pytest.mark.asyncio
    async def test_codex_blank_home_uses_actual_runtime_user_home(
        self, runtime, tmp_path,
    ):
        login = AsyncMock(return_value={"ok": True, "logs": []})
        runtime._send_event = AsyncMock()
        message = self._msg(
            agent_type="codex",
            email_token="",
            password="openai-secret",
            config_dir="",
        )

        with (
            patch("pathlib.Path.home", return_value=tmp_path),
            patch(
                "elastic_agent.worker.login.codex_login.codex_login",
                new=login,
            ),
        ):
            await runtime._handle_account_login(message)

        assert login.await_args.kwargs["codex_home"] == str(tmp_path / ".codex")

    @pytest.mark.asyncio
    async def test_codex_password_only_otp_reuses_live_login_request(
        self, runtime,
    ):
        from elastic_agent.core.protocols.messages import (
            AccountLoginOtpMessage,
            AccountLoginOtpRequiredMessage,
        )
        from elastic_agent.worker.runtime import _WorkerLoginOtpReader

        sent = []
        runtime._send_event = lambda message: sent.append(message) or asyncio.sleep(0)
        request = self._msg(
            agent_type="codex",
            email_token="",
            password="openai-secret",
            config_dir="/root/.codex-a1",
        )
        reader = _WorkerLoginOtpReader(runtime, request)
        waiting = asyncio.create_task(reader.read_code(
            attempt_id=request.login_request_id,
            timeout_s=5,
            logs=[],
        ))
        await asyncio.sleep(0)
        challenge = next(
            item for item in sent
            if isinstance(item, AccountLoginOtpRequiredMessage)
        )
        response = AccountLoginOtpMessage(
            login_request_id=request.login_request_id,
            account_id=request.account_id,
            challenge_id=challenge.challenge_id,
            code="123456",
        )

        assert reader.submit(response) is True
        assert await waiting == "123456"
        assert reader.submit(response) is False

    @pytest.mark.asyncio
    async def test_codex_email_token_only_login_is_forwarded(
        self, runtime,
    ):
        from elastic_agent.core.protocols.messages import AccountLoginResultMessage

        sent = []
        runtime._send_event = lambda message: sent.append(message) or asyncio.sleep(0)
        login = AsyncMock(return_value={"ok": True, "logs": []})
        message = self._msg(
            agent_type="codex",
            email_token="mail-query-token",
            password="",
            config_dir="/root/.codex-a1",
        )

        with patch(
            "elastic_agent.worker.login.codex_login.codex_login", new=login,
        ):
            await runtime._handle_account_login(message)

        assert login.await_args.kwargs["password"] == ""
        assert login.await_args.kwargs["token_171"] == "mail-query-token"
        result = next(
            item for item in sent if isinstance(item, AccountLoginResultMessage)
        )
        assert result.success is True

    @pytest.mark.asyncio
    async def test_codex_login_requires_password_or_email_token(
        self, runtime,
    ):
        from elastic_agent.core.protocols.messages import AccountLoginResultMessage

        sent = []
        runtime._send_event = lambda message: sent.append(message) or asyncio.sleep(0)
        await runtime._handle_account_login(self._msg(
            agent_type="codex",
            email_token="",
            password="",
            config_dir="/root/.codex-a1",
        ))

        result = next(
            item for item in sent if isinstance(item, AccountLoginResultMessage)
        )
        assert result.success is False
        assert "email token or OpenAI password" in result.error

    @pytest.mark.asyncio
    async def test_background_login_exception_sends_correlated_safe_failure(
        self, runtime,
    ):
        from elastic_agent.core.protocols.messages import AccountLoginResultMessage

        sent = []
        runtime._send_event = lambda message: sent.append(message) or asyncio.sleep(0)

        async def fail(_message):
            raise RuntimeError("secret-that-must-not-be-returned")

        runtime._handle_account_login = fail
        message = self._msg(
            agent_type="codex",
            password="openai-secret",
        )

        await runtime._dispatch(message)
        task = runtime._account_login_tasks[message.login_request_id]
        await task

        result = next(
            item for item in sent if isinstance(item, AccountLoginResultMessage)
        )
        assert result.login_request_id == message.login_request_id
        assert result.success is False
        assert result.error == "account login failed unexpectedly"
        assert "secret-that-must-not-be-returned" not in result.error

    @pytest.mark.asyncio
    async def test_correlated_cancel_stops_background_login(self, runtime):
        from elastic_agent.core.protocols.messages import (
            AccountLoginCancelledMessage,
            AccountLoginCancelMessage,
        )

        started = asyncio.Event()
        cancelled = asyncio.Event()
        sent = []
        runtime._send_event = (
            lambda message: sent.append(message) or asyncio.sleep(0)
        )

        async def block(_message):
            started.set()
            try:
                await asyncio.Future()
            finally:
                cancelled.set()

        runtime._handle_account_login = block
        message = self._msg(
            agent_type="codex",
            password="openai-secret",
        )
        await runtime._dispatch(message)
        await started.wait()
        task = runtime._account_login_tasks[message.login_request_id]

        await runtime._dispatch(AccountLoginCancelMessage(
            login_request_id=message.login_request_id,
            account_id=message.account_id,
            reason="manager_timeout",
        ))
        await cancelled.wait()
        with pytest.raises(asyncio.CancelledError):
            await task

        await asyncio.sleep(0)
        assert message.login_request_id not in runtime._account_login_tasks
        acknowledgement = next(
            item for item in sent
            if isinstance(item, AccountLoginCancelledMessage)
        )
        assert acknowledgement.login_request_id == message.login_request_id
        assert acknowledgement.cleanup_complete is True

    @pytest.mark.asyncio
    async def test_cancel_mismatch_reports_cleanup_not_confirmed(self, runtime):
        from elastic_agent.core.protocols.messages import (
            AccountLoginCancelledMessage,
            AccountLoginCancelMessage,
        )

        sent = []
        runtime._send_event = (
            lambda message: sent.append(message) or asyncio.sleep(0)
        )
        blocker = asyncio.ensure_future(asyncio.Future())
        runtime._account_login_tasks["login-request-1"] = blocker
        runtime._account_login_accounts["login-request-1"] = "other-account"

        await runtime._handle_account_login_cancel(AccountLoginCancelMessage(
            login_request_id="login-request-1",
            account_id="a1",
            reason="manager_timeout",
        ))

        acknowledgement = next(
            item for item in sent
            if isinstance(item, AccountLoginCancelledMessage)
        )
        assert acknowledgement.cleanup_complete is False
        assert blocker.done() is False
        blocker.cancel()
        await asyncio.gather(blocker, return_exceptions=True)

    @pytest.mark.asyncio
    async def test_warmup_requires_zero_exit_status(self, runtime):
        proc = MagicMock(pid=1234, returncode=7)
        proc.wait = AsyncMock(return_value=7)
        with patch(
            "asyncio.create_subprocess_exec",
            new=AsyncMock(return_value=proc),
        ) as create:
            assert await runtime._warmup_config_dir("/tmp/claude") is False
        assert create.await_args.kwargs["start_new_session"] is True

    @pytest.mark.asyncio
    async def test_auth_identity_uses_casefolded_exact_email(self, runtime):
        proc = MagicMock(pid=1234, returncode=0)
        proc.communicate = AsyncMock(return_value=(
            json.dumps({
                "loggedIn": True,
                "email": "User@Example.COM",
            }).encode(),
            None,
        ))
        with patch(
            "asyncio.create_subprocess_exec",
            new=AsyncMock(return_value=proc),
        ):
            assert await runtime._verify_config_identity(
                "/tmp/claude", "user@example.com"
            ) is True

    @pytest.mark.asyncio
    async def test_warmup_timeout_terminates_and_reaps_process_group(
        self, runtime
    ):
        class SlowProcess:
            def __init__(self):
                self.pid = 4321
                self.returncode = None
                self.wait_calls = 0

            async def wait(self):
                self.wait_calls += 1
                if self.wait_calls == 1:
                    await asyncio.Event().wait()
                self.returncode = -signal.SIGTERM
                return self.returncode

        proc = SlowProcess()
        with (
            patch(
                "asyncio.create_subprocess_exec",
                new=AsyncMock(return_value=proc),
            ),
            patch("os.killpg") as killpg,
        ):
            assert await runtime._warmup_config_dir(
                "/tmp/claude", timeout=0.01
            ) is False

        killpg.assert_called_once_with(proc.pid, signal.SIGTERM)
        assert proc.wait_calls == 2
