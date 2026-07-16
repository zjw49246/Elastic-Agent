"""Tests for Worker Runtime server (T-007)."""

from __future__ import annotations

import asyncio
import json
import os
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

    def test_custom_log_dir(self, tmp_path):
        custom = tmp_path / "custom-logs"
        rt = WorkerRuntime(
            manager_url="ws://localhost:9999/ws/runtime",
            auth_token="tok",
            log_dir=str(custom),
        )
        assert rt._log_dir == custom


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
            account_id="a1", email="u@foo.com", email_token="tok",
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
        runtime._warmup_config_dir = AsyncMock()
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
