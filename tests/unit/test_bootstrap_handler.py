"""Tests for Bootstrap failure handling (T-023)."""

from __future__ import annotations

import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from elastic_agent.core.bootstrap import FailureStrategy, StepResult, StepStatus
from elastic_agent.core.bootstrap_handler import BootstrapHandler
from elastic_agent.core.config import BootstrapConfig
from elastic_agent.core.event_bus import EventBus
from elastic_agent.core.registry import NodeRecord, NodeRegistry, NodeStatus
from elastic_agent.harness.base import BootstrapStep


STEPS = [
    BootstrapStep(name="step-1", command="echo ok", timeout=5),
    BootstrapStep(name="step-2", command="echo ok", timeout=5),
]


@pytest.fixture
async def setup(tmp_path: Path):
    registry = NodeRegistry(str(tmp_path / "registry.json"))
    await registry.load()
    event_bus = EventBus()
    await registry.add(NodeRecord(
        node_id="node-1",
        instance_id="inst-1",
        platform="dryrun",
        status=NodeStatus.CREATING,
        public_ip="1.2.3.4",
    ))
    return registry, event_bus


@pytest.mark.asyncio
async def test_successful_bootstrap(setup) -> None:
    registry, event_bus = setup
    config = BootstrapConfig(failure_strategy="leave_for_debug")
    handler = BootstrapHandler(config, registry, event_bus)

    with patch("elastic_agent.core.bootstrap_handler.SSHExecutor") as mock_executor_cls:
        mock_executor = MagicMock()
        mock_executor.execute = AsyncMock(return_value=(0, "ok", ""))
        mock_executor_cls.return_value = mock_executor

        with patch.object(handler._pipeline, "execute", new_callable=AsyncMock) as mock_execute:
            from elastic_agent.core.bootstrap import BootstrapResult
            mock_execute.return_value = BootstrapResult(
                node_id="node-1",
                success=True,
                total_duration_seconds=5.0,
                attempts_used=1,
            )
            result = await handler.bootstrap_node("node-1", "1.2.3.4", STEPS)

    assert result.success is True
    node = await registry.get("node-1")
    assert node.status == NodeStatus.READY


@pytest.mark.asyncio
async def test_leave_for_debug_strategy(setup) -> None:
    registry, event_bus = setup
    config = BootstrapConfig(failure_strategy="leave_for_debug")
    handler = BootstrapHandler(config, registry, event_bus)

    with patch("elastic_agent.core.bootstrap_handler.SSHExecutor"):
        with patch.object(handler._pipeline, "execute", new_callable=AsyncMock) as mock_execute:
            from elastic_agent.core.bootstrap import BootstrapResult
            mock_execute.return_value = BootstrapResult(
                node_id="node-1",
                success=False,
                error="step-2 failed",
                failure_strategy_applied=FailureStrategy.LEAVE_FOR_DEBUG,
            )
            result = await handler.bootstrap_node("node-1", "1.2.3.4", STEPS)

    assert result.success is False
    node = await registry.get("node-1")
    assert node.status == NodeStatus.FAILED


@pytest.mark.asyncio
async def test_terminate_and_retry_strategy(setup) -> None:
    registry, event_bus = setup
    config = BootstrapConfig(failure_strategy="terminate_and_retry")

    replace_called = False

    async def mock_replace(node_id: str) -> str | None:
        nonlocal replace_called
        replace_called = True
        return "node-2"

    handler = BootstrapHandler(
        config, registry, event_bus,
        on_terminate_and_replace=mock_replace,
    )

    with patch("elastic_agent.core.bootstrap_handler.SSHExecutor"):
        with patch.object(handler._pipeline, "execute", new_callable=AsyncMock) as mock_execute:
            from elastic_agent.core.bootstrap import BootstrapResult
            mock_execute.return_value = BootstrapResult(
                node_id="node-1",
                success=False,
                error="step-1 failed",
                failure_strategy_applied=FailureStrategy.TERMINATE_AND_RETRY,
            )
            result = await handler.bootstrap_node("node-1", "1.2.3.4", STEPS)

    assert replace_called is True
    node = await registry.get("node-1")
    assert node.status == NodeStatus.FAILED


@pytest.mark.asyncio
async def test_credential_recovery_on_failure(setup) -> None:
    registry, event_bus = setup
    config = BootstrapConfig(failure_strategy="leave_for_debug")

    recover_called = False

    async def mock_recover(node_id: str) -> None:
        nonlocal recover_called
        recover_called = True

    handler = BootstrapHandler(
        config, registry, event_bus,
        on_recover_credentials=mock_recover,
    )

    with patch("elastic_agent.core.bootstrap_handler.SSHExecutor"):
        with patch.object(handler._pipeline, "execute", new_callable=AsyncMock) as mock_execute:
            from elastic_agent.core.bootstrap import BootstrapResult
            mock_execute.return_value = BootstrapResult(
                node_id="node-1",
                success=False,
                error="failed",
            )
            await handler.bootstrap_node("node-1", "1.2.3.4", STEPS)

    assert recover_called is True


@pytest.mark.asyncio
async def test_events_emitted(setup) -> None:
    registry, event_bus = setup
    config = BootstrapConfig(failure_strategy="leave_for_debug")
    handler = BootstrapHandler(config, registry, event_bus)

    events: list[str] = []
    event_bus.subscribe("*", lambda et, wid, data: events.append(et))

    with patch("elastic_agent.core.bootstrap_handler.SSHExecutor"):
        with patch.object(handler._pipeline, "execute", new_callable=AsyncMock) as mock_execute:
            from elastic_agent.core.bootstrap import BootstrapResult
            mock_execute.return_value = BootstrapResult(
                node_id="node-1",
                success=True,
                total_duration_seconds=3.0,
                attempts_used=1,
            )
            await handler.bootstrap_node("node-1", "1.2.3.4", STEPS)

    assert "BOOTSTRAP_STARTED" in events
    assert "BOOTSTRAP_COMPLETED" in events
