"""Tests for Bootstrap Pipeline (T-018)."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from elastic_agent.core.bootstrap import (
    BootstrapPipeline,
    BootstrapResult,
    FailureStrategy,
    SSHExecutor,
    StepResult,
    StepStatus,
)
from elastic_agent.core.config import BootstrapConfig
from elastic_agent.harness.base import BootstrapStep


class FakeExecutor(SSHExecutor):
    """Executor that returns preconfigured results instead of running SSH."""

    def __init__(self, results: list[tuple[int, str, str]] | None = None):
        super().__init__(host="test-host")
        self._results = list(results) if results else []
        self._call_count = 0
        self.calls: list[dict] = []

    async def execute(self, command, timeout=300, env=None, cwd=None):
        self.calls.append({
            "command": command,
            "timeout": timeout,
            "env": env,
            "cwd": cwd,
        })
        if self._call_count < len(self._results):
            result = self._results[self._call_count]
        else:
            result = (0, "ok", "")
        self._call_count += 1
        return result


def _make_config(**overrides) -> BootstrapConfig:
    defaults = {
        "default_step_timeout": 300,
        "max_retries": 2,
        "failure_strategy": "terminate_and_retry",
    }
    defaults.update(overrides)
    return BootstrapConfig(**defaults)


def _make_steps(count: int = 3) -> list[BootstrapStep]:
    return [
        BootstrapStep(name=f"step-{i}", command=f"echo step-{i}", timeout=60)
        for i in range(count)
    ]


class TestFailureStrategyEnum:
    def test_all_variants(self):
        assert FailureStrategy.TERMINATE_AND_RETRY.value == "terminate_and_retry"
        assert FailureStrategy.RETRY_FROM_FAILED.value == "retry_from_failed"
        assert FailureStrategy.LEAVE_FOR_DEBUG.value == "leave_for_debug"

    def test_from_config_string(self):
        for val in ["terminate_and_retry", "retry_from_failed", "leave_for_debug"]:
            assert FailureStrategy(val).value == val


class TestBootstrapPipelineSuccess:
    @pytest.mark.asyncio
    async def test_all_steps_succeed(self):
        steps = _make_steps(3)
        executor = FakeExecutor([(0, "ok", "")] * 3)
        pipeline = BootstrapPipeline(_make_config())

        result = await pipeline.execute("node-1", steps, executor)

        assert result.success is True
        assert result.node_id == "node-1"
        assert len(result.steps) == 3
        assert all(s.status == StepStatus.COMPLETED for s in result.steps)
        assert result.attempts_used == 1
        assert result.total_duration_seconds >= 0

    @pytest.mark.asyncio
    async def test_single_step(self):
        steps = [BootstrapStep(name="init", command="apt update")]
        executor = FakeExecutor([(0, "done", "")])
        pipeline = BootstrapPipeline(_make_config())

        result = await pipeline.execute("node-1", steps, executor)

        assert result.success is True
        assert len(result.steps) == 1

    @pytest.mark.asyncio
    async def test_empty_steps(self):
        executor = FakeExecutor()
        pipeline = BootstrapPipeline(_make_config())

        result = await pipeline.execute("node-1", [], executor)

        assert result.success is True
        assert len(result.steps) == 0


class TestBootstrapPipelineFailure:
    @pytest.mark.asyncio
    async def test_first_step_fails(self):
        steps = _make_steps(3)
        executor = FakeExecutor([(1, "", "error")])
        pipeline = BootstrapPipeline(_make_config(failure_strategy="terminate_and_retry"))

        result = await pipeline.execute("node-1", steps, executor)

        assert result.success is False
        assert result.steps[0].status == StepStatus.FAILED
        assert result.steps[1].status == StepStatus.SKIPPED
        assert result.steps[2].status == StepStatus.SKIPPED
        assert result.error is not None

    @pytest.mark.asyncio
    async def test_middle_step_fails(self):
        steps = _make_steps(3)
        executor = FakeExecutor([
            (0, "ok", ""),
            (1, "", "middle failed"),
            (0, "ok", ""),
        ])
        pipeline = BootstrapPipeline(_make_config(failure_strategy="terminate_and_retry"))

        result = await pipeline.execute("node-1", steps, executor)

        assert result.success is False
        assert result.steps[0].status == StepStatus.COMPLETED
        assert result.steps[1].status == StepStatus.FAILED
        assert result.steps[2].status == StepStatus.SKIPPED

    @pytest.mark.asyncio
    async def test_last_step_fails(self):
        steps = _make_steps(3)
        executor = FakeExecutor([
            (0, "ok", ""),
            (0, "ok", ""),
            (1, "", "last failed"),
        ])
        pipeline = BootstrapPipeline(_make_config(failure_strategy="terminate_and_retry"))

        result = await pipeline.execute("node-1", steps, executor)

        assert result.success is False
        assert result.steps[0].status == StepStatus.COMPLETED
        assert result.steps[1].status == StepStatus.COMPLETED
        assert result.steps[2].status == StepStatus.FAILED


class TestBootstrapStepRetry:
    @pytest.mark.asyncio
    async def test_step_level_retry_succeeds(self):
        steps = [BootstrapStep(name="flaky", command="flaky-cmd", retry_count=2)]
        executor = FakeExecutor([
            (1, "", "fail 1"),
            (1, "", "fail 2"),
            (0, "ok", ""),
        ])
        pipeline = BootstrapPipeline(_make_config())

        result = await pipeline.execute("node-1", steps, executor)

        assert result.success is True
        assert len(executor.calls) == 3

    @pytest.mark.asyncio
    async def test_step_level_retry_exhausted(self):
        steps = [BootstrapStep(name="flaky", command="flaky-cmd", retry_count=1)]
        executor = FakeExecutor([
            (1, "", "fail 1"),
            (1, "", "fail 2"),
        ])
        pipeline = BootstrapPipeline(_make_config(failure_strategy="terminate_and_retry"))

        result = await pipeline.execute("node-1", steps, executor)

        assert result.success is False
        assert len(executor.calls) == 2


class TestFailureStrategyTerminateAndRetry:
    @pytest.mark.asyncio
    async def test_signals_terminate_and_retry(self):
        steps = _make_steps(2)
        executor = FakeExecutor([(1, "", "boom")])
        pipeline = BootstrapPipeline(_make_config(
            failure_strategy="terminate_and_retry",
            max_retries=0,
        ))

        result = await pipeline.execute("node-1", steps, executor)

        assert result.success is False
        assert result.failure_strategy_applied == FailureStrategy.TERMINATE_AND_RETRY
        assert result.attempts_used == 1


class TestFailureStrategyRetryFromFailed:
    @pytest.mark.asyncio
    async def test_retries_from_failed_step(self):
        steps = _make_steps(3)
        executor = FakeExecutor([
            (0, "ok", ""),
            (1, "", "fail"),
            (0, "ok", ""),
            (0, "ok", ""),
        ])
        pipeline = BootstrapPipeline(_make_config(
            failure_strategy="retry_from_failed",
            max_retries=1,
        ))

        result = await pipeline.execute("node-1", steps, executor)

        assert result.success is True
        assert result.attempts_used == 2

    @pytest.mark.asyncio
    async def test_retry_from_failed_exhausted(self):
        steps = _make_steps(2)
        executor = FakeExecutor([
            (0, "ok", ""),
            (1, "", "fail"),
            (1, "", "fail again"),
        ])
        pipeline = BootstrapPipeline(_make_config(
            failure_strategy="retry_from_failed",
            max_retries=1,
        ))

        result = await pipeline.execute("node-1", steps, executor)

        assert result.success is False
        assert result.attempts_used == 2


class TestFailureStrategyLeaveForDebug:
    @pytest.mark.asyncio
    async def test_leaves_for_debug(self):
        steps = _make_steps(2)
        executor = FakeExecutor([(1, "", "broken")])
        pipeline = BootstrapPipeline(_make_config(
            failure_strategy="leave_for_debug",
            max_retries=2,
        ))

        result = await pipeline.execute("node-1", steps, executor)

        assert result.success is False
        assert result.failure_strategy_applied == FailureStrategy.LEAVE_FOR_DEBUG
        assert result.attempts_used == 1


class TestBootstrapStepTimeout:
    @pytest.mark.asyncio
    async def test_step_uses_own_timeout(self):
        steps = [BootstrapStep(name="fast", command="echo fast", timeout=42)]
        executor = FakeExecutor([(0, "ok", "")])
        pipeline = BootstrapPipeline(_make_config(default_step_timeout=300))

        await pipeline.execute("node-1", steps, executor)

        assert executor.calls[0]["timeout"] == 42

    @pytest.mark.asyncio
    async def test_step_uses_default_timeout_when_zero(self):
        steps = [BootstrapStep(name="default", command="echo", timeout=0)]
        executor = FakeExecutor([(0, "ok", "")])
        pipeline = BootstrapPipeline(_make_config(default_step_timeout=600))

        await pipeline.execute("node-1", steps, executor)

        assert executor.calls[0]["timeout"] == 600


class TestBootstrapStepEnvAndCwd:
    @pytest.mark.asyncio
    async def test_env_passed_to_executor(self):
        steps = [BootstrapStep(
            name="with-env",
            command="echo $FOO",
            env={"FOO": "bar", "BAZ": "qux"},
        )]
        executor = FakeExecutor([(0, "ok", "")])
        pipeline = BootstrapPipeline(_make_config())

        await pipeline.execute("node-1", steps, executor)

        assert executor.calls[0]["env"] == {"FOO": "bar", "BAZ": "qux"}

    @pytest.mark.asyncio
    async def test_cwd_passed_to_executor(self):
        steps = [BootstrapStep(name="with-cwd", command="ls", cwd="/opt/app")]
        executor = FakeExecutor([(0, "ok", "")])
        pipeline = BootstrapPipeline(_make_config())

        await pipeline.execute("node-1", steps, executor)

        assert executor.calls[0]["cwd"] == "/opt/app"


class TestBootstrapCallbacks:
    @pytest.mark.asyncio
    async def test_callbacks_called(self):
        steps = _make_steps(2)
        executor = FakeExecutor([(0, "ok", "")] * 2)
        pipeline = BootstrapPipeline(_make_config())

        on_start = AsyncMock()
        on_complete = AsyncMock()

        await pipeline.execute(
            "node-1", steps, executor,
            on_step_start=on_start,
            on_step_complete=on_complete,
        )

        assert on_start.call_count == 2
        assert on_complete.call_count == 2
        on_start.assert_any_call("step-0", 0, 2)
        on_start.assert_any_call("step-1", 1, 2)


class TestBootstrapResult:
    @pytest.mark.asyncio
    async def test_result_fields(self):
        steps = _make_steps(2)
        executor = FakeExecutor([(0, "ok", ""), (0, "done", "")])
        pipeline = BootstrapPipeline(_make_config())

        result = await pipeline.execute("node-42", steps, executor)

        assert result.node_id == "node-42"
        assert result.success is True
        assert result.total_duration_seconds > 0
        assert len(result.steps) == 2
        assert result.steps[0].stdout == "ok"
        assert result.steps[1].stdout == "done"

    @pytest.mark.asyncio
    async def test_result_captures_exit_code(self):
        steps = [BootstrapStep(name="fail", command="false")]
        executor = FakeExecutor([(42, "", "bad exit")])
        pipeline = BootstrapPipeline(_make_config(failure_strategy="leave_for_debug"))

        result = await pipeline.execute("node-1", steps, executor)

        assert result.steps[0].exit_code == 42


class TestSSHExecutor:
    def test_build_ssh_cmd_simple(self):
        executor = SSHExecutor(host="1.2.3.4", user="root", key_path="/keys/id.pem")
        cmd = executor._build_ssh_cmd("apt update")
        assert "ssh" in cmd
        assert "-i" in cmd
        assert "/keys/id.pem" in cmd
        assert "root@1.2.3.4" in cmd
        assert "apt update" in cmd

    def test_build_ssh_cmd_with_env_and_cwd(self):
        executor = SSHExecutor(host="1.2.3.4")
        cmd = executor._build_ssh_cmd("ls", env={"A": "1"}, cwd="/opt")
        remote_part = cmd[-1]
        assert "export A=" in remote_part
        assert "cd " in remote_part
        assert "ls" in remote_part

    def test_build_ssh_cmd_list_command(self):
        executor = SSHExecutor(host="1.2.3.4")
        cmd = executor._build_ssh_cmd(["echo", "hello", "world"])
        assert "echo hello world" in cmd[-1]

    def test_custom_port(self):
        executor = SSHExecutor(host="1.2.3.4", port=2222)
        cmd = executor._build_ssh_cmd("whoami")
        assert "-p" in cmd
        idx = cmd.index("-p")
        assert cmd[idx + 1] == "2222"

    @pytest.mark.asyncio
    async def test_execute_timeout(self):
        executor = SSHExecutor(host="1.2.3.4")
        with patch("asyncio.create_subprocess_exec") as mock_exec:
            mock_proc = AsyncMock()
            mock_proc.communicate = AsyncMock(side_effect=asyncio.TimeoutError)
            mock_proc.kill = MagicMock()
            mock_proc.wait = AsyncMock()
            mock_exec.return_value = mock_proc

            exit_code, stdout, stderr = await executor.execute("sleep 999", timeout=1)

            assert exit_code == -1
            assert "timed out" in stderr.lower()

    @pytest.mark.asyncio
    async def test_execute_success(self):
        executor = SSHExecutor(host="1.2.3.4")
        with patch("asyncio.create_subprocess_exec") as mock_exec:
            mock_proc = AsyncMock()
            mock_proc.communicate = AsyncMock(return_value=(b"output", b""))
            mock_proc.returncode = 0
            mock_exec.return_value = mock_proc

            exit_code, stdout, stderr = await executor.execute("echo hi")

            assert exit_code == 0
            assert stdout == "output"

    @pytest.mark.asyncio
    async def test_execute_failure(self):
        executor = SSHExecutor(host="1.2.3.4")
        with patch("asyncio.create_subprocess_exec") as mock_exec:
            mock_proc = AsyncMock()
            mock_proc.communicate = AsyncMock(return_value=(b"", b"not found"))
            mock_proc.returncode = 127
            mock_exec.return_value = mock_proc

            exit_code, stdout, stderr = await executor.execute("bad-cmd")

            assert exit_code == 127
            assert stderr == "not found"

    @pytest.mark.asyncio
    async def test_execute_exception(self):
        executor = SSHExecutor(host="1.2.3.4")
        with patch("asyncio.create_subprocess_exec", side_effect=OSError("no ssh")):
            exit_code, stdout, stderr = await executor.execute("echo hi")
            assert exit_code == -1
            assert "no ssh" in stderr


class TestMaxRetries:
    @pytest.mark.asyncio
    async def test_max_retries_zero_means_one_attempt(self):
        steps = [BootstrapStep(name="fail", command="false")]
        executor = FakeExecutor([(1, "", "err")])
        pipeline = BootstrapPipeline(_make_config(
            failure_strategy="retry_from_failed",
            max_retries=0,
        ))

        result = await pipeline.execute("node-1", steps, executor)

        assert result.success is False
        assert result.attempts_used == 1

    @pytest.mark.asyncio
    async def test_max_retries_respected(self):
        steps = [BootstrapStep(name="fail", command="false")]
        executor = FakeExecutor([(1, "", "e")] * 10)
        pipeline = BootstrapPipeline(_make_config(
            failure_strategy="retry_from_failed",
            max_retries=2,
        ))

        result = await pipeline.execute("node-1", steps, executor)

        assert result.success is False
        assert result.attempts_used <= 3
