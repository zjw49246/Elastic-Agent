"""T-113: Bootstrap E2E integration tests.

Tests: full step execution, single step failure+retry, credential recovery on failure.
Uses a FakeExecutor to simulate SSH without real remote hosts.
"""

from __future__ import annotations

import asyncio
import json

import pytest

from elastic_agent.core.bootstrap import (
    BootstrapPipeline,
    BootstrapResult,
    FailureStrategy,
    SSHExecutor,
    StepStatus,
)
from elastic_agent.core.config import BootstrapConfig
from elastic_agent.harness.base import BootstrapStep
from elastic_agent.testing import create_test_manager


class FakeExecutor(SSHExecutor):
    """In-process executor that returns preconfigured results."""

    def __init__(self, results: list[tuple[int, str, str]] | None = None):
        super().__init__(host="fake-host")
        self._results = list(results) if results else []
        self._call_idx = 0
        self.calls: list[dict] = []

    async def execute(self, command, timeout=300, env=None, cwd=None):
        call = {"command": command, "timeout": timeout, "env": env, "cwd": cwd}
        self.calls.append(call)
        if self._call_idx < len(self._results):
            result = self._results[self._call_idx]
        else:
            result = (0, "ok", "")
        self._call_idx += 1
        return result


@pytest.mark.level1
class TestBootstrapFullExecution:
    """All bootstrap steps execute successfully end-to-end."""

    @pytest.mark.asyncio
    async def test_all_steps_succeed(self):
        config = BootstrapConfig(default_step_timeout=10, max_retries=1)
        pipeline = BootstrapPipeline(config)
        executor = FakeExecutor([
            (0, "system init done", ""),
            (0, "agent installed", ""),
            (0, "runtime deployed", ""),
            (0, "harness ready", ""),
        ])
        steps = [
            BootstrapStep(name="system_init", command="apt-get update"),
            BootstrapStep(name="install_agent", command="npm install -g claude"),
            BootstrapStep(name="deploy_runtime", command="pip install elastic-agent"),
            BootstrapStep(name="setup_harness", command="git clone repo"),
        ]

        result = await pipeline.execute("node-1", steps, executor)

        assert result.success
        assert result.node_id == "node-1"
        assert len(result.steps) == 4
        assert all(s.status == StepStatus.COMPLETED for s in result.steps)
        assert result.total_duration_seconds > 0
        assert len(executor.calls) == 4

    @pytest.mark.asyncio
    async def test_step_callbacks_invoked(self):
        config = BootstrapConfig(default_step_timeout=10, max_retries=0)
        pipeline = BootstrapPipeline(config)
        executor = FakeExecutor([(0, "ok", "")] * 2)
        steps = [
            BootstrapStep(name="step_a", command="echo a"),
            BootstrapStep(name="step_b", command="echo b"),
        ]

        start_calls: list[str] = []
        complete_calls: list[str] = []

        async def on_start(name, idx, total):
            start_calls.append(name)

        async def on_complete(name, result):
            complete_calls.append(name)

        result = await pipeline.execute(
            "node-2", steps, executor,
            on_step_start=on_start,
            on_step_complete=on_complete,
        )

        assert result.success
        assert start_calls == ["step_a", "step_b"]
        assert complete_calls == ["step_a", "step_b"]


@pytest.mark.level1
class TestBootstrapSingleStepFailure:
    """Single step failure triggers retry logic."""

    @pytest.mark.asyncio
    async def test_step_failure_skips_remaining(self):
        config = BootstrapConfig(
            default_step_timeout=10,
            max_retries=0,
            failure_strategy="leave_for_debug",
        )
        pipeline = BootstrapPipeline(config)
        executor = FakeExecutor([
            (0, "ok", ""),
            (1, "", "install failed"),
            (0, "should not run", ""),
        ])
        steps = [
            BootstrapStep(name="step_1", command="echo ok"),
            BootstrapStep(name="step_2", command="fail"),
            BootstrapStep(name="step_3", command="echo never"),
        ]

        result = await pipeline.execute("node-fail", steps, executor)

        assert not result.success
        assert result.steps[0].status == StepStatus.COMPLETED
        assert result.steps[1].status == StepStatus.FAILED
        assert result.steps[2].status == StepStatus.SKIPPED
        assert result.failure_strategy_applied == FailureStrategy.LEAVE_FOR_DEBUG

    @pytest.mark.asyncio
    async def test_retry_from_failed_step(self):
        config = BootstrapConfig(
            default_step_timeout=10,
            max_retries=1,
            failure_strategy="retry_from_failed",
        )
        pipeline = BootstrapPipeline(config)
        executor = FakeExecutor([
            (0, "ok", ""),
            (1, "", "fail first time"),
            (0, "ok retry", ""),
            (0, "ok step3", ""),
        ])
        steps = [
            BootstrapStep(name="step_1", command="echo ok"),
            BootstrapStep(name="step_2", command="maybe_fail"),
            BootstrapStep(name="step_3", command="echo after"),
        ]

        result = await pipeline.execute("node-retry", steps, executor)

        assert result.success
        assert result.attempts_used == 2
        assert all(s.status == StepStatus.COMPLETED for s in result.steps)

    @pytest.mark.asyncio
    async def test_per_step_retry(self):
        config = BootstrapConfig(default_step_timeout=10, max_retries=0)
        pipeline = BootstrapPipeline(config)
        executor = FakeExecutor([
            (1, "", "fail 1"),
            (1, "", "fail 2"),
            (0, "ok after retries", ""),
        ])
        steps = [
            BootstrapStep(name="flaky_step", command="flaky", retry_count=2),
        ]

        result = await pipeline.execute("node-per-step", steps, executor)

        assert result.success
        assert len(executor.calls) == 3


@pytest.mark.level1
class TestBootstrapCredentialRecovery:
    """Bootstrap interacts with credential pool on failure scenarios."""

    @pytest.mark.asyncio
    async def test_bootstrap_failure_does_not_corrupt_state(self, tmp_path):
        tm = create_test_manager(tmp_dir=tmp_path)
        await tm.manager.start()

        try:
            nodes = await tm.manager.scale_out(count=1)
            node = nodes[0]

            config = BootstrapConfig(
                default_step_timeout=5,
                max_retries=0,
                failure_strategy="terminate_and_retry",
            )
            pipeline = BootstrapPipeline(config)
            executor = FakeExecutor([(1, "", "boom")])
            steps = [BootstrapStep(name="cred_step", command="setup_creds")]

            result = await pipeline.execute(node.node_id, steps, executor)
            assert not result.success
            assert result.failure_strategy_applied == FailureStrategy.TERMINATE_AND_RETRY

            reg_node = await tm.manager.registry.get(node.node_id)
            assert reg_node is not None
        finally:
            await tm.manager.stop()

    @pytest.mark.asyncio
    async def test_bootstrap_success_with_env_vars(self):
        config = BootstrapConfig(default_step_timeout=10, max_retries=0)
        pipeline = BootstrapPipeline(config)
        executor = FakeExecutor([(0, "done", "")])
        steps = [
            BootstrapStep(
                name="env_step",
                command="echo $SECRET",
                env={"SECRET": "hunter2", "HOME": "/root"},
                cwd="/opt/agent",
            ),
        ]

        result = await pipeline.execute("node-env", steps, executor)

        assert result.success
        call = executor.calls[0]
        assert call["env"] == {"SECRET": "hunter2", "HOME": "/root"}
        assert call["cwd"] == "/opt/agent"
