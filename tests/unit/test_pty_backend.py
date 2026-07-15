"""Tests for PTY-hosted agent execution (claude-pty integration)."""

from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from elastic_agent.core.agent_type import ClaudeCodeAgentType
from elastic_agent.core.bootstrap_steps import build_default_bootstrap_steps, pty_install_step
from elastic_agent.core.protocols.messages import (
    ExecuteMessage,
    LogMessage,
    ProcessExitMessage,
    parse_message,
)
from elastic_agent.worker.pty_backend import (
    PTY_AVAILABLE,
    event_to_log_line,
    synthesize_result_line,
    classify_turn_error,
)
from elastic_agent.worker.runtime import WorkerRuntime

# ---------------------------------------------------------------------------
# Protocol: ExecuteMessage.agent_params
# ---------------------------------------------------------------------------


class TestExecuteMessageAgentParams:
    def test_default_none(self):
        msg = ExecuteMessage(task_id="t1", command=["claude"], cwd=".")
        assert msg.agent_params is None

    def test_roundtrip(self):
        msg = ExecuteMessage(
            task_id="t1",
            command=["claude", "-p", "hi"],
            cwd="/work",
            agent_params={"agent": "claude-code", "prompt": "hi"},
        )
        parsed = parse_message(msg.model_dump_json())
        assert isinstance(parsed, ExecuteMessage)
        assert parsed.agent_params == {"agent": "claude-code", "prompt": "hi"}

    def test_backward_compat_without_field(self):
        # Old managers serialize without agent_params; must still parse.
        raw = json.dumps({
            "type": "EXECUTE", "task_id": "t1",
            "command": ["claude"], "cwd": ".",
        })
        parsed = parse_message(raw)
        assert parsed.agent_params is None


class TestGetLaunchParams:
    def test_minimal(self):
        agent = ClaudeCodeAgentType()
        params = agent.get_launch_params(prompt="do it")
        assert params == {"agent": "claude-code", "prompt": "do it"}

    def test_full(self):
        agent = ClaudeCodeAgentType()
        params = agent.get_launch_params(
            prompt="continue",
            session_id="sess-1",
            config_dir="/root/.claude-edit-1",
            model="claude-opus-4-8",
        )
        assert params["resume_session_id"] == "sess-1"
        assert params["config_dir"] == "/root/.claude-edit-1"
        assert params["model"] == "claude-opus-4-8"


# ---------------------------------------------------------------------------
# Event mapping helpers
# ---------------------------------------------------------------------------


class TestEventToLogLine:
    def test_raw_json_passthrough(self):
        raw = json.dumps({"type": "assistant", "message": {"content": []}})
        line = event_to_log_line({"event_type": "message", "raw_json": raw})
        assert line == raw

    def test_internal_event_without_content_dropped(self):
        assert event_to_log_line({"event_type": "session_started"}) is None

    def test_internal_error_wrapped_as_system(self):
        line = event_to_log_line({
            "event_type": "system_event",
            "content": "Response timed out after 600s",
            "is_error": True,
            "session_id": "sess-1",
        })
        obj = json.loads(line)
        assert obj["type"] == "system"
        assert obj["subtype"] == "pty_system_event"
        assert obj["is_error"] is True
        assert obj["session_id"] == "sess-1"

    def test_manager_parser_accepts_wrapped_event(self):
        line = event_to_log_line({
            "event_type": "message", "content": "limit reached", "is_error": True,
        })
        parsed = WorkerRuntime._try_parse_ndjson(line)
        assert parsed is not None
        assert parsed["type"] == "system"

    def test_response_timeout_classified_as_runtime_timeout(self):
        error_type, message = classify_turn_error("Response timed out after 600s")
        assert error_type == "runtime_timeout"
        assert "Worker runtime timed out" in message
        assert "Response timed out after 600s" in message

    def test_usage_limit_classified_as_rate_limited(self):
        error_type, message = classify_turn_error("usage limit reached")
        assert error_type == "claude_rate_limited"
        assert "usage limit was reached" in message
        assert "usage limit reached" in message

    def test_generic_error_stays_pty_turn_error(self):
        error_type, message = classify_turn_error("the turn failed unexpectedly")
        assert error_type == "pty_turn_error"
        assert message == "the turn failed unexpectedly"


class TestSynthesizeResultLine:
    def test_success_shape(self):
        obj = json.loads(synthesize_result_line("sess-1", is_error=False))
        assert obj["type"] == "result"
        assert obj["subtype"] == "success"
        assert obj["session_id"] == "sess-1"

    def test_error_shape(self):
        obj = json.loads(synthesize_result_line("sess-1", True, "rate limited"))
        assert obj["subtype"] == "error"
        assert obj["error"] == "rate limited"

    def test_session_id_extractable_by_manager(self):
        # The whole point: manager-side parsers extract session_id from it.
        line = synthesize_result_line("sess-xyz", is_error=False)
        parsed = WorkerRuntime._try_parse_ndjson(line)
        assert parsed["type"] == "result"
        assert parsed["session_id"] == "sess-xyz"

        agent = ClaudeCodeAgentType()
        assert agent.extract_session_id(json.loads(line)) == "sess-xyz"


# ---------------------------------------------------------------------------
# Runtime dispatch
# ---------------------------------------------------------------------------


class _FakeBackend:
    def __init__(self):
        self.launches = []
        self.stops = []
        self.marked_error = None
        self._tasks: set[str] = set()

    def has_task(self, task_id):
        return task_id in self._tasks

    @property
    def active_tasks(self):
        return list(self._tasks)

    async def launch(self, key, prompt, cwd, **kwargs):
        self.launches.append({"key": key, "prompt": prompt, "cwd": cwd, **kwargs})
        self._tasks.add(key)
        return "sess-fake"

    async def stop(self, key):
        self.stops.append(key)
        self._tasks.discard(key)

    def mark_task_error(self, task_id, error_type, error_message):
        self.marked_error = (task_id, error_type, error_message)

    async def shutdown(self):
        self._tasks.clear()


@pytest.fixture
def runtime(tmp_path):
    return WorkerRuntime(
        manager_url="ws://localhost:9999/ws/runtime",
        auth_token="tok",
        worker_id="w1",
        log_dir=str(tmp_path / "logs"),
    )


def _exec_msg(**kw):
    defaults = dict(task_id="t1:abc", command=["claude", "-p", "hi"], cwd="/work")
    defaults.update(kw)
    return ExecuteMessage(**defaults)


class TestRuntimePTYDispatch:
    @pytest.mark.asyncio
    async def test_agent_params_routes_to_pty(self, runtime):
        fake = _FakeBackend()
        runtime._pty_backend = fake
        msg = _exec_msg(agent_params={
            "agent": "claude-code", "prompt": "hi",
            "resume_session_id": "sess-1", "config_dir": "/root/.claude-edit-1",
        })
        await runtime._handle_execute(msg)
        assert len(fake.launches) == 1
        launch = fake.launches[0]
        assert launch["key"] == "t1:abc"
        assert launch["prompt"] == "hi"
        assert launch["resume_session_id"] == "sess-1"
        assert launch["config_dir"] == "/root/.claude-edit-1"
        # No subprocess was spawned
        assert runtime._processes == {}
        assert "t1:abc" in runtime.active_processes

    @pytest.mark.asyncio
    async def test_config_dir_falls_back_to_env(self, runtime):
        fake = _FakeBackend()
        runtime._pty_backend = fake
        msg = _exec_msg(
            agent_params={"agent": "claude-code", "prompt": "hi"},
            env={"CLAUDE_CONFIG_DIR": "/root/.claude-prod", "FOO": "bar"},
        )
        await runtime._handle_execute(msg)
        launch = fake.launches[0]
        assert launch["config_dir"] == "/root/.claude-prod"
        # CLAUDE_CONFIG_DIR is consumed, the rest passes through
        assert launch["env_overrides"] == {"FOO": "bar"}

    @pytest.mark.asyncio
    async def test_no_agent_params_keeps_subprocess_path(self, runtime, monkeypatch):
        fake = _FakeBackend()
        runtime._pty_backend = fake
        spawned = []

        async def fake_spawn(*args, **kwargs):
            spawned.append(args)
            raise OSError("stop here")  # short-circuit after recording

        monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_spawn)
        runtime._send_event = AsyncMock()
        await runtime._handle_execute(_exec_msg())
        assert fake.launches == []
        assert len(spawned) == 1

    @pytest.mark.asyncio
    async def test_pty_unavailable_falls_back_to_subprocess(self, runtime, monkeypatch):
        import elastic_agent.worker.pty_backend as pb
        monkeypatch.setattr(pb, "PTY_AVAILABLE", False)
        spawned = []

        async def fake_spawn(*args, **kwargs):
            spawned.append(args)
            raise OSError("stop here")

        monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_spawn)
        runtime._send_event = AsyncMock()
        await runtime._handle_execute(_exec_msg(agent_params={"prompt": "hi"}))
        assert len(spawned) == 1

    @pytest.mark.asyncio
    async def test_duplicate_pty_task_rejected(self, runtime):
        fake = _FakeBackend()
        fake._tasks.add("t1:abc")
        runtime._pty_backend = fake
        runtime._send_event = AsyncMock()
        await runtime._handle_execute(_exec_msg(agent_params={"prompt": "hi"}))
        assert fake.launches == []
        sent = runtime._send_event.call_args[0][0]
        assert sent.type == "ERROR"
        assert sent.error_type == "duplicate_task"

    @pytest.mark.asyncio
    async def test_launch_failure_reports_exit(self, runtime):
        fake = _FakeBackend()

        async def boom(**kwargs):
            raise RuntimeError("spawn failed")

        fake.launch = boom
        runtime._pty_backend = fake
        runtime._send_event = AsyncMock()
        await runtime._handle_execute(_exec_msg(agent_params={"prompt": "hi"}))
        types = [c.args[0].type for c in runtime._send_event.call_args_list]
        assert "ERROR" in types
        assert "PROCESS_EXIT" in types

    @pytest.mark.asyncio
    async def test_stop_routes_to_pty_backend(self, runtime):
        from elastic_agent.core.protocols.messages import StopMessage
        fake = _FakeBackend()
        fake._tasks.add("t1:abc")
        runtime._pty_backend = fake
        await runtime._handle_stop(StopMessage(task_id="t1:abc", signal="SIGINT"))
        assert fake.stops == ["t1:abc"]

    @pytest.mark.asyncio
    async def test_stop_unknown_task_uses_subprocess_path(self, runtime):
        from elastic_agent.core.protocols.messages import StopMessage
        fake = _FakeBackend()
        runtime._pty_backend = fake
        await runtime._handle_stop(StopMessage(task_id="other", signal="SIGTERM"))
        assert fake.stops == []

    @pytest.mark.asyncio
    async def test_on_pty_exit_sends_process_exit_and_cancels_timer(self, runtime):
        runtime._send_event = AsyncMock()
        timer = asyncio.get_event_loop().create_task(asyncio.sleep(60))
        runtime._pty_timeouts["t1:abc"] = timer
        await runtime._on_pty_exit("t1:abc", 1)
        sent = runtime._send_event.call_args[0][0]
        assert isinstance(sent, ProcessExitMessage)
        assert sent.exit_code == 1
        assert timer.cancelled() or timer.cancelling()
        timer.cancel()

    @pytest.mark.asyncio
    async def test_timeout_stops_pty_session(self, runtime):
        fake = _FakeBackend()
        fake._tasks.add("t1:abc")
        runtime._pty_backend = fake
        await runtime._pty_timeout_watch("t1:abc", 0)
        assert fake.stops == ["t1:abc"]
        assert fake.marked_error[0] == "t1:abc"
        assert fake.marked_error[1] == "runtime_timeout"
        assert "timed out" in fake.marked_error[2]


# ---------------------------------------------------------------------------
# ElasticPTYBackend on_event / on_exit (requires claude-pty installed)
# ---------------------------------------------------------------------------


pty_required = pytest.mark.skipif(not PTY_AVAILABLE, reason="claude-pty not installed")


def _make_backend(tmp_path):
    """Build an ElasticPTYBackend without starting a BridgeHub."""
    from elastic_agent.worker.pty_backend import ElasticPTYBackend

    backend = object.__new__(ElasticPTYBackend)
    backend._runtime = MagicMock()
    backend._runtime._send_event = AsyncMock()
    backend._runtime._on_pty_exit = AsyncMock()
    backend._log_dir = tmp_path / "logs"
    backend._task_session_ids = {}
    backend._turn_errors = {}
    backend._turn_error_types = {}
    backend._saw_result = set()
    backend._saw_claude_output = set()
    backend._sessions = {}
    backend._consumers = {}
    backend._launch_kwargs = {}
    backend._transient_retries = {}
    backend._transient_retry_max = 5
    backend._transient_retry_base = 10.0
    backend._transient_retry_cap = 120.0
    return backend


@pty_required
class TestElasticPTYBackendEvents:
    @pytest.mark.asyncio
    async def test_on_event_forwards_raw_json(self, tmp_path):
        backend = _make_backend(tmp_path)
        raw = json.dumps({"type": "assistant", "message": {"content": [{"type": "text", "text": "hi"}]}})
        await backend.on_event("t1", {
            "event_type": "message", "raw_json": raw, "session_id": "sess-1",
        })
        sent = backend._runtime._send_event.call_args[0][0]
        assert isinstance(sent, LogMessage)
        assert sent.stream == "stdout"
        assert sent.data == raw
        assert sent.parsed["type"] == "assistant"
        assert backend._task_session_ids["t1"] == "sess-1"
        # Local ndjson log written
        log_lines = (tmp_path / "logs" / "t1.ndjson").read_text().splitlines()
        assert json.loads(log_lines[0])["data"] == raw

    @pytest.mark.asyncio
    async def test_on_exit_synthesizes_success_after_claude_output(self, tmp_path):
        backend = _make_backend(tmp_path)
        raw = json.dumps({"type": "assistant", "message": {"content": []}})
        await backend.on_event("t1", {
            "event_type": "message", "raw_json": raw, "session_id": "sess-1",
        })
        backend._runtime._send_event.reset_mock()
        await backend.on_exit("t1", 0)
        sent = backend._runtime._send_event.call_args[0][0]
        obj = json.loads(sent.data)
        assert obj["type"] == "result"
        assert obj["subtype"] == "success"
        assert obj["session_id"] == "sess-1"
        backend._runtime._on_pty_exit.assert_awaited_once_with(
            "t1", 0, session_id="sess-1", error_type=None, error_message=None
        )

    @pytest.mark.asyncio
    async def test_on_exit_empty_turn_forces_failure(self, tmp_path):
        backend = _make_backend(tmp_path)
        backend._task_session_ids["t1"] = "sess-1"
        await backend.on_exit("t1", 0)
        sent = backend._runtime._send_event.call_args[0][0]
        obj = json.loads(sent.data)
        assert obj["type"] == "result"
        assert obj["subtype"] == "error"
        assert "produced no Claude output" in obj["error"]
        backend._runtime._on_pty_exit.assert_awaited_once_with(
            "t1",
            1,
            session_id="sess-1",
            error_type="no_claude_output",
            error_message="Claude PTY session produced no Claude output; prompt injection may have failed",
        )

    @pytest.mark.asyncio
    async def test_on_exit_error_turn_forces_nonzero_exit(self, tmp_path):
        backend = _make_backend(tmp_path)
        await backend.on_event("t1", {
            "event_type": "message", "content": "the turn failed unexpectedly",
            "is_error": True, "session_id": "sess-1",
        })
        await backend.on_exit("t1", 0)
        # Synthesized result is an error and exit code propagated as 1
        result_msg = backend._runtime._send_event.call_args[0][0]
        obj = json.loads(result_msg.data)
        assert obj["subtype"] == "error"
        assert obj["error"] == "the turn failed unexpectedly"
        backend._runtime._on_pty_exit.assert_awaited_once_with(
            "t1", 1, session_id="sess-1", error_type="pty_turn_error",
            error_message="the turn failed unexpectedly",
        )

    @pytest.mark.asyncio
    async def test_on_exit_response_timeout_reports_runtime_timeout(self, tmp_path):
        backend = _make_backend(tmp_path)
        await backend.on_event("t1", {
            "event_type": "system_event", "content": "Response timed out after 600s",
            "is_error": True, "session_id": "sess-1",
        })
        await backend.on_exit("t1", 0)
        result_msg = backend._runtime._send_event.call_args[0][0]
        obj = json.loads(result_msg.data)
        assert obj["subtype"] == "error"
        assert obj["error_type"] == "runtime_timeout"
        assert "Worker runtime timed out" in obj["error"]
        backend._runtime._on_pty_exit.assert_awaited_once_with(
            "t1",
            1,
            session_id="sess-1",
            error_type="runtime_timeout",
            error_message="Worker runtime timed out and interrupted the Claude process (Response timed out after 600s)",
        )

    @pytest.mark.asyncio
    async def test_on_exit_skips_result_if_real_one_seen(self, tmp_path):
        backend = _make_backend(tmp_path)
        raw = json.dumps({"type": "result", "session_id": "sess-1"})
        await backend.on_event("t1", {"event_type": "result", "raw_json": raw, "session_id": "sess-1"})
        backend._runtime._send_event.reset_mock()
        await backend.on_exit("t1", 0)
        # No second result line emitted
        backend._runtime._send_event.assert_not_called()
        backend._runtime._on_pty_exit.assert_awaited_once_with(
            "t1", 0, session_id="sess-1", error_type=None, error_message=None
        )

    @pytest.mark.asyncio
    async def test_orphan_error_does_not_poison_turn(self, tmp_path):
        """A stale api_error replayed on cold-resume (orphan) must not fail
        a turn that then succeeds — the recover-then-failed regression."""
        backend = _make_backend(tmp_path)
        # Backlog from the previous turn is replayed with orphan=True.
        await backend.on_event("t1", {
            "event_type": "message", "content": "usage limit reached",
            "is_error": True, "orphan": True, "session_id": "sess-1",
        })
        # The fresh turn produces real Claude output and no error.
        raw = json.dumps({"type": "assistant", "message": {"content": []}})
        await backend.on_event("t1", {
            "event_type": "message", "raw_json": raw, "session_id": "sess-1",
        })
        await backend.on_exit("t1", 0)
        obj = json.loads(backend._runtime._send_event.call_args[0][0].data)
        assert obj["subtype"] == "success"
        backend._runtime._on_pty_exit.assert_awaited_once_with(
            "t1", 0, session_id="sess-1", error_type=None, error_message=None
        )

    @pytest.mark.asyncio
    async def test_orphan_event_not_forwarded(self, tmp_path):
        """Orphan replays are duplicates of already-forwarded lines — drop
        them so the Manager never double-logs or re-parses an old result."""
        backend = _make_backend(tmp_path)
        raw = json.dumps({"type": "result", "session_id": "sess-1"})
        await backend.on_event("t1", {
            "event_type": "result", "raw_json": raw, "orphan": True, "session_id": "sess-1",
        })
        backend._runtime._send_event.assert_not_called()
        # Orphan result must not satisfy the turn's own result accounting.
        assert "t1" not in backend._saw_result

    @pytest.mark.asyncio
    async def test_autonomous_error_does_not_fail_turn(self, tmp_path):
        """A background sub-agent (autonomous) error is not the foreground
        turn's error and must not mark the task failed."""
        backend = _make_backend(tmp_path)
        raw = json.dumps({"type": "assistant", "message": {"content": []}})
        await backend.on_event("t1", {
            "event_type": "message", "raw_json": raw, "session_id": "sess-1",
        })
        await backend.on_event("t1", {
            "event_type": "message", "content": "sub-agent hit an error",
            "is_error": True, "autonomous": True, "session_id": "subagent-9",
        })
        # The sub-agent's session_id must not clobber the task's own.
        assert backend._task_session_ids["t1"] == "sess-1"
        await backend.on_exit("t1", 0)
        obj = json.loads(backend._runtime._send_event.call_args[0][0].data)
        assert obj["subtype"] == "success"
        # The synthesized result carries the task's session, not the sub-agent's.
        assert obj["session_id"] == "sess-1"


# ---------------------------------------------------------------------------
# TaskRouter use_pty
# ---------------------------------------------------------------------------


class TestTaskRouterUsePty:
    @pytest.fixture
    def setup(self, tmp_path):
        from elastic_agent.core.registry import NodeRegistry
        from elastic_agent.core.task_registry import TaskRegistry
        from elastic_agent.manager.connection import WorkerConnectionManager

        node_registry = NodeRegistry(tmp_path / "registry.json")
        task_registry = TaskRegistry(tmp_path / "task_registry.json")
        cm = WorkerConnectionManager(node_registry)
        cm.execute = AsyncMock()
        cm.is_connected = MagicMock(return_value=True)
        return node_registry, task_registry, cm

    @pytest.mark.asyncio
    async def test_followup_attaches_agent_params(self, setup):
        from elastic_agent.core.registry import NodeRecord, NodeStatus
        from elastic_agent.core.task_router import TaskRouter

        node_registry, task_registry, cm = setup
        await node_registry.add(NodeRecord(
            node_id="w1", instance_id="i-1", platform="test", status=NodeStatus.READY,
        ))
        await task_registry.register("t1", "w1")
        await task_registry.update("t1", session_id="sess-123")

        router = TaskRouter(task_registry, node_registry, cm,
                            ClaudeCodeAgentType(), use_pty=True)
        await router.send_followup("t1", "fix the bug", config_dir="/root/.claude-edit-1")

        kw = cm.execute.call_args.kwargs
        # Fallback command still present
        assert "--resume" in kw["command"]
        # Structured params for PTY-capable workers
        assert kw["agent_params"] == {
            "agent": "claude-code",
            "prompt": "fix the bug",
            "resume_session_id": "sess-123",
            "config_dir": "/root/.claude-edit-1",
        }

    @pytest.mark.asyncio
    async def test_followup_without_use_pty_sends_none(self, setup):
        from elastic_agent.core.registry import NodeRecord, NodeStatus
        from elastic_agent.core.task_router import TaskRouter

        node_registry, task_registry, cm = setup
        await node_registry.add(NodeRecord(
            node_id="w1", instance_id="i-1", platform="test", status=NodeStatus.READY,
        ))
        await task_registry.register("t1", "w1")

        router = TaskRouter(task_registry, node_registry, cm, ClaudeCodeAgentType())
        await router.send_followup("t1", "do something")
        assert cm.execute.call_args.kwargs["agent_params"] is None


# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------


class TestPTYBootstrap:
    def test_pty_install_step(self):
        step = pty_install_step()
        assert step.name == "pty-install"
        assert "Claude-Code-PTY" in step.command

    def test_default_steps_exclude_pty(self):
        steps = build_default_bootstrap_steps("ws://m", "tok", "w1")
        assert "pty-install" not in [s.name for s in steps]

    def test_include_pty_inserts_before_runtime_deploy(self):
        steps = build_default_bootstrap_steps("ws://m", "tok", "w1", include_pty=True)
        names = [s.name for s in steps]
        assert "pty-install" in names
        assert names.index("pty-install") < names.index("runtime-deploy")

    def test_include_pty_appends_refresh_hook(self):
        # resume_node skips bootstrap, so the worker needs a self-refresh hook
        steps = build_default_bootstrap_steps("ws://m", "tok", "w1", include_pty=True)
        names = [s.name for s in steps]
        assert "pty-refresh-hook" in names
        assert "claude-cli-health-hook" in names
        # must run after runtime-deploy writes the unit it drops into
        assert names.index("pty-refresh-hook") > names.index("runtime-deploy")
        assert names.index("claude-cli-health-hook") > names.index("runtime-deploy")

    def test_default_steps_exclude_refresh_hook(self):
        steps = build_default_bootstrap_steps("ws://m", "tok", "w1")
        assert "pty-refresh-hook" not in [s.name for s in steps]

    def test_pty_refresh_step_content(self):
        from elastic_agent.core.bootstrap_steps import pty_refresh_step

        step = pty_refresh_step()
        # script compares installed direct_url commit vs upstream main HEAD
        assert "direct_url.json" in step.command
        assert "ls-remote" in step.command
        assert "refs/heads/main" in step.command
        assert "--force-reinstall" in step.command
        # ExecStartPre with `-`: refresh failure must not block runtime start
        assert "ExecStartPre=-/bin/bash /usr/local/bin/claude-pty-refresh.sh" in step.command
        assert "10-pty-refresh.conf" in step.command
        assert "daemon-reload" in step.command
        # repo URL is templated in, no leftover placeholder
        assert "{pty_repo_url}" not in step.command
        assert "Claude-Code-PTY" in step.command

    def test_claude_cli_health_step_content(self):
        from elastic_agent.core.bootstrap_steps import claude_cli_health_step

        step = claude_cli_health_step()
        assert "claude --version" in step.command
        assert "@anthropic-ai/claude-code@$VERSION" in step.command
        assert "2.1.181" in step.command
        assert "ExecStartPre=/bin/bash /usr/local/bin/claude-cli-healthcheck.sh" in step.command
        assert "20-claude-cli-health.conf" in step.command

    def test_all_pip_installs_break_system_packages(self):
        # PEP 668 (Ubuntu 24.04 images) rejects system pip3 installs without
        # --break-system-packages — first real include_pty scale-out failed at
        # pty-install because of this (2026-06-12 21:45 prod incident)
        import re

        from elastic_agent.core import bootstrap_steps as bs

        steps = bs.build_default_bootstrap_steps(
            "ws://m", "tok", "w1", include_pty=True, include_login_deps=True
        )
        for step in steps:
            for m in re.finditer(r"pip3 install[^&\n]*", step.command):
                assert "--break-system-packages" in m.group(0), (
                    f"step {step.name!r}: {m.group(0)}"
                )

    def test_pty_refresh_step_custom_repo(self):
        from elastic_agent.core.bootstrap_steps import pty_refresh_step

        step = pty_refresh_step(pty_repo_url="https://example.com/fork")
        assert 'URL="https://example.com/fork"' in step.command


# ---------------------------------------------------------------------------
# Phase 3: credential rotation recycles warm sessions
# ---------------------------------------------------------------------------


class _FakeSession:
    def __init__(self, config_dir):
        self.config = MagicMock()
        self.config.config_dir = config_dir


class _FakePool:
    def __init__(self):
        self._sessions = {}
        self.removed = []

    async def remove(self, sid):
        self.removed.append(sid)
        self._sessions.pop(sid, None)


@pty_required
class TestRecycleConfigDir:
    def _backend(self, tmp_path):
        backend = _make_backend(tmp_path)
        backend._pool = _FakePool()
        backend.stopped = []

        async def fake_stop(key):
            backend.stopped.append(key)
            backend._sessions.pop(key, None)

        backend.stop = fake_stop
        return backend

    @pytest.mark.asyncio
    async def test_recycles_matching_keyed_and_warm_sessions(self, tmp_path):
        backend = self._backend(tmp_path)
        backend._sessions["task:A"] = _FakeSession("/root/.claude-edit-1")
        backend._sessions["task:B"] = _FakeSession("/root/.claude-edit-2")
        backend._pool._sessions["sess-warm"] = _FakeSession("/root/.claude-edit-1")
        backend._pool._sessions["sess-other"] = _FakeSession("/root/.claude-edit-2")

        recycled = await backend.recycle_config_dir("/root/.claude-edit-1")

        assert recycled == 2
        assert backend.stopped == ["task:A"]
        assert backend._pool.removed == ["sess-warm"]
        # Other config_dir untouched
        assert "task:B" in backend._sessions
        assert "sess-other" in backend._pool._sessions

    @pytest.mark.asyncio
    async def test_none_config_dir_matches_default_sessions(self, tmp_path):
        backend = self._backend(tmp_path)
        backend._pool._sessions["sess-default"] = _FakeSession(None)
        backend._pool._sessions["sess-slot"] = _FakeSession("/root/.claude-edit-1")
        recycled = await backend.recycle_config_dir(None)
        assert recycled == 1
        assert backend._pool.removed == ["sess-default"]

    @pytest.mark.asyncio
    async def test_no_match_is_noop(self, tmp_path):
        backend = self._backend(tmp_path)
        backend._pool._sessions["sess-1"] = _FakeSession("/root/.claude-edit-2")
        assert await backend.recycle_config_dir("/root/.claude-edit-9") == 0


class TestCredentialLoginRecyclesPTY:
    @pytest.mark.asyncio
    async def test_login_triggers_recycle(self, runtime, tmp_path):
        from elastic_agent.core.protocols.messages import CredentialLoginMessage

        backend = MagicMock()
        backend.recycle_config_dir = AsyncMock(return_value=1)
        runtime._pty_backend = backend
        runtime._send_event = AsyncMock()

        config_dir = str(tmp_path / "claude-edit-1")
        await runtime._handle_credential_login(CredentialLoginMessage(
            task_id="", slot_index=1,
            credentials={"account_id": "acc-2", "accessToken": "tok"},
            config_dir=config_dir,
        ))
        backend.recycle_config_dir.assert_awaited_once_with(config_dir)
        # Login result still reported
        sent = runtime._send_event.call_args[0][0]
        assert sent.type == "CREDENTIAL_LOGIN_RESULT"
        assert sent.success is True

    @pytest.mark.asyncio
    async def test_login_without_backend_unaffected(self, runtime, tmp_path):
        from elastic_agent.core.protocols.messages import CredentialLoginMessage

        runtime._send_event = AsyncMock()
        await runtime._handle_credential_login(CredentialLoginMessage(
            task_id="", slot_index=1,
            credentials={"account_id": "acc-2", "accessToken": "tok"},
            config_dir=str(tmp_path / "claude-edit-1"),
        ))
        sent = runtime._send_event.call_args[0][0]
        assert sent.success is True

    @pytest.mark.asyncio
    async def test_recycle_failure_does_not_break_login(self, runtime, tmp_path):
        from elastic_agent.core.protocols.messages import CredentialLoginMessage

        backend = MagicMock()
        backend.recycle_config_dir = AsyncMock(side_effect=RuntimeError("boom"))
        runtime._pty_backend = backend
        runtime._send_event = AsyncMock()
        await runtime._handle_credential_login(CredentialLoginMessage(
            task_id="", slot_index=1,
            credentials={"account_id": "acc-2", "accessToken": "tok"},
            config_dir=str(tmp_path / "claude-edit-1"),
        ))
        sent = runtime._send_event.call_args[0][0]
        assert sent.success is True


# ---------------------------------------------------------------------------
# Per-task response timeout (long production turns)
# ---------------------------------------------------------------------------


@pty_required
class TestResponseTimeoutPlumbing:
    def test_build_config_sets_response_timeout(self, tmp_path):
        backend = _make_backend(tmp_path)
        config = backend.build_config(response_timeout=7200)
        assert config.response_timeout == 7200.0

    def test_build_config_default_unchanged(self, tmp_path):
        backend = _make_backend(tmp_path)
        config = backend.build_config()
        assert config.response_timeout == 7200.0


class TestRuntimePassesResponseTimeout:
    @pytest.mark.asyncio
    async def test_msg_timeout_becomes_response_timeout(self, runtime):
        fake = _FakeBackend()
        runtime._pty_backend = fake
        await runtime._handle_execute(_exec_msg(
            agent_params={"agent": "claude-code", "prompt": "build it"},
            timeout=7200,
        ))
        assert fake.launches[0]["response_timeout"] == 7200
        # Hard watchdog runs behind the graceful PTY timeout
        assert "t1:abc" in runtime._pty_timeouts
        runtime._pty_timeouts["t1:abc"].cancel()

    @pytest.mark.asyncio
    async def test_agent_params_override_wins(self, runtime):
        fake = _FakeBackend()
        runtime._pty_backend = fake
        await runtime._handle_execute(_exec_msg(
            agent_params={"agent": "claude-code", "prompt": "x",
                          "response_timeout": 3600},
            timeout=7200,
        ))
        assert fake.launches[0]["response_timeout"] == 3600

    @pytest.mark.asyncio
    async def test_no_timeout_leaves_default(self, runtime):
        fake = _FakeBackend()
        runtime._pty_backend = fake
        await runtime._handle_execute(_exec_msg(
            agent_params={"agent": "claude-code", "prompt": "x"},
        ))
        assert fake.launches[0]["response_timeout"] is None
        assert runtime._pty_timeouts == {}


class TestRootSandboxEnv:
    @pytest.mark.asyncio
    async def test_root_gets_is_sandbox(self, runtime, monkeypatch):
        import os as _os
        monkeypatch.setattr(_os, "geteuid", lambda: 0)
        fake = _FakeBackend()
        runtime._pty_backend = fake
        await runtime._handle_execute(_exec_msg(agent_params={"prompt": "x"}))
        assert fake.launches[0]["env_overrides"]["IS_SANDBOX"] == "1"

    @pytest.mark.asyncio
    async def test_non_root_unchanged(self, runtime, monkeypatch):
        import os as _os
        monkeypatch.setattr(_os, "geteuid", lambda: 1000)
        fake = _FakeBackend()
        runtime._pty_backend = fake
        await runtime._handle_execute(_exec_msg(agent_params={"prompt": "x"}))
        assert fake.launches[0]["env_overrides"] is None

    @pytest.mark.asyncio
    async def test_explicit_env_not_overridden(self, runtime, monkeypatch):
        import os as _os
        monkeypatch.setattr(_os, "geteuid", lambda: 0)
        fake = _FakeBackend()
        runtime._pty_backend = fake
        await runtime._handle_execute(_exec_msg(
            agent_params={"prompt": "x"}, env={"IS_SANDBOX": "0"},
        ))
        assert fake.launches[0]["env_overrides"]["IS_SANDBOX"] == "0"


@pty_required
class TestTurnErrorSemantics:
    @pytest.mark.asyncio
    async def test_tool_result_error_not_fatal(self, tmp_path):
        # The exact production bug: one failed tool_result mid-run marked a
        # fully delivered book as failed.
        backend = _make_backend(tmp_path)
        await backend.on_event("t1", {
            "event_type": "tool_result", "is_error": True,
            "tool_output": "Agent type 'x' not found",
            "raw_json": json.dumps({"type": "user", "message": {}}),
            "session_id": "sess-1",
        })
        await backend.on_exit("t1", 0)
        result_msg = backend._runtime._send_event.call_args[0][0]
        obj = json.loads(result_msg.data)
        assert obj["subtype"] == "success"
        backend._runtime._on_pty_exit.assert_awaited_once_with(
            "t1", 0, session_id="sess-1", error_type=None, error_message=None
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize("etype", ["system_event", "message", "result"])
    async def test_session_level_errors_stay_fatal(self, tmp_path, etype):
        backend = _make_backend(tmp_path)
        await backend.on_event("t1", {
            "event_type": etype, "is_error": True,
            "content": "the turn failed unexpectedly", "session_id": "sess-1",
        })
        await backend.on_exit("t1", 0)
        backend._runtime._on_pty_exit.assert_awaited_once_with(
            "t1", 1, session_id="sess-1", error_type="pty_turn_error",
            error_message="the turn failed unexpectedly",
        )


@pty_required
class TestTransientOverloadRetry:
    """P2: a server-side 429/overload turn is retried on the SAME session
    after backoff instead of failing the task."""

    @pytest.mark.asyncio
    async def test_overload_error_classified_and_retried(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "elastic_agent.worker.pty_backend.transient_retry_delay",
            lambda *a, **k: 0.0,
        )
        backend = _make_backend(tmp_path)
        backend.launch = AsyncMock()
        backend._launch_kwargs["t1"] = {"key": "t1", "prompt": "hi", "cwd": "/x"}
        # An overload error turn (produces real output, then the api error).
        await backend.on_event("t1", {
            "event_type": "message", "raw_json": "{}", "session_id": "s1",
        })
        await backend.on_event("t1", {
            "event_type": "message", "content": "API Error: overloaded_error",
            "is_error": True, "session_id": "s1",
        })
        await backend.on_exit("t1", 1)
        # Deferred: the Manager is NOT told the task failed…
        backend._runtime._on_pty_exit.assert_not_called()
        assert backend._transient_retries["t1"] == 1
        # …and the session is relaunched (resumed) in the background.
        await asyncio.sleep(0.05)
        backend.launch.assert_awaited()
        assert backend.launch.await_args.kwargs["resume_session_id"] == "s1"

    def test_schedule_false_without_launch_kwargs(self, tmp_path):
        backend = _make_backend(tmp_path)
        assert backend._schedule_transient_retry("t1", "s1") is False

    def test_schedule_false_when_budget_exhausted(self, tmp_path):
        backend = _make_backend(tmp_path)
        backend._launch_kwargs["t1"] = {"key": "t1"}
        backend._transient_retries["t1"] = 5  # already at max
        assert backend._schedule_transient_retry("t1", "s1") is False

    @pytest.mark.asyncio
    async def test_run_retry_resumes_same_session(self, tmp_path):
        backend = _make_backend(tmp_path)
        backend.launch = AsyncMock()
        backend._launch_kwargs["t1"] = {"key": "t1", "resume_session_id": None}
        await backend._run_transient_retry("t1", "s1", delay=0.0)
        backend.launch.assert_awaited_once()
        assert backend.launch.await_args.kwargs["resume_session_id"] == "s1"

    @pytest.mark.asyncio
    async def test_run_retry_launch_failure_reports_exit(self, tmp_path):
        backend = _make_backend(tmp_path)
        backend.launch = AsyncMock(side_effect=RuntimeError("boom"))
        backend._launch_kwargs["t1"] = {"key": "t1"}
        await backend._run_transient_retry("t1", "s1", delay=0.0)
        backend._runtime._on_pty_exit.assert_awaited_once()
        args, kwargs = backend._runtime._on_pty_exit.await_args
        assert args[1] == 1
        assert kwargs["error_type"] == "transient_overload"

    @pytest.mark.asyncio
    async def test_exhausted_budget_fails_normally(self, tmp_path):
        backend = _make_backend(tmp_path)
        backend._launch_kwargs["t1"] = {"key": "t1"}
        backend._transient_retries["t1"] = 5  # at max
        await backend.on_event("t1", {
            "event_type": "message", "content": "overloaded_error",
            "is_error": True, "raw_json": "{}", "session_id": "s1",
        })
        await backend.on_exit("t1", 1)
        # Budget exhausted → report the failure to the Manager.
        backend._runtime._on_pty_exit.assert_awaited_once()
        assert backend._runtime._on_pty_exit.await_args.kwargs["error_type"] == "transient_overload"
        # Retry state cleared once the task truly ends.
        assert "t1" not in backend._transient_retries
        assert "t1" not in backend._launch_kwargs
