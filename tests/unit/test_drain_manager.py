"""Tests for graceful drain (T-025)."""

from __future__ import annotations

import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from elastic_agent.core.drain_manager import DrainManager
from elastic_agent.core.event_bus import EventBus
from elastic_agent.core.providers.base import CloudProvider, Instance, InstanceConfig, InstanceState
from elastic_agent.core.registry import NodeRecord, NodeRegistry, NodeStatus
from elastic_agent.core.task_registry import TaskRegistry, TaskStatus
from elastic_agent.manager.connection import WorkerConnectionManager


class MockProvider(CloudProvider):
    def __init__(self):
        self.terminated: list[str] = []

    @property
    def platform(self) -> str:
        return "dryrun"

    async def create_instance(self, config): ...
    async def start_instance(self, instance_id): ...
    async def stop_instance(self, instance_id): ...
    async def reboot_instance(self, instance_id): ...

    async def terminate_instance(self, instance_id: str) -> None:
        self.terminated.append(instance_id)

    async def list_instances(self, filters=None): return []
    async def get_instance(self, instance_id): ...
    async def wait_until_running(self, instance_id, timeout=300): ...


@pytest.fixture
async def setup(tmp_path: Path):
    registry = NodeRegistry(str(tmp_path / "registry.json"))
    task_registry = TaskRegistry(str(tmp_path / "task_registry.json"))
    await registry.load()
    await task_registry.load()

    conn_mgr = MagicMock(spec=WorkerConnectionManager)
    conn_mgr.disconnect_worker = AsyncMock()
    conn_mgr.stop_process = AsyncMock()

    provider = MockProvider()
    event_bus = EventBus()

    await registry.add(NodeRecord(
        node_id="w1", instance_id="inst-1", platform="dryrun",
        status=NodeStatus.READY,
    ))

    return registry, task_registry, conn_mgr, provider, event_bus


@pytest.mark.asyncio
async def test_drain_node_no_tasks(setup) -> None:
    registry, task_registry, conn_mgr, provider, event_bus = setup
    dm = DrainManager(registry, task_registry, conn_mgr, provider, event_bus, drain_timeout=5)

    result = await dm.drain_node("w1")
    assert result is True

    await asyncio.sleep(1)

    node = await registry.get("w1")
    assert node.status == NodeStatus.TERMINATED
    assert "inst-1" in provider.terminated
    conn_mgr.disconnect_worker.assert_called_with("w1")


@pytest.mark.asyncio
async def test_drain_waits_for_tasks(setup) -> None:
    registry, task_registry, conn_mgr, provider, event_bus = setup
    await task_registry.register("t1", "w1")
    await task_registry.update("t1", status=TaskStatus.RUNNING)

    dm = DrainManager(registry, task_registry, conn_mgr, provider, event_bus, drain_timeout=10)
    await dm.drain_node("w1")

    await asyncio.sleep(0.5)
    node = await registry.get("w1")
    assert node.status == NodeStatus.DRAINING

    await task_registry.update("t1", status=TaskStatus.COMPLETED)
    await asyncio.sleep(6)

    node = await registry.get("w1")
    assert node.status == NodeStatus.TERMINATED


@pytest.mark.asyncio
async def test_drain_timeout_force_stop(setup) -> None:
    registry, task_registry, conn_mgr, provider, event_bus = setup
    await task_registry.register("t1", "w1")
    await task_registry.update("t1", status=TaskStatus.RUNNING)

    dm = DrainManager(registry, task_registry, conn_mgr, provider, event_bus, drain_timeout=1)
    await dm.drain_node("w1")

    await asyncio.sleep(8)

    conn_mgr.stop_process.assert_called()
    node = await registry.get("w1")
    assert node.status == NodeStatus.TERMINATED


@pytest.mark.asyncio
async def test_drain_nonexistent_node(setup) -> None:
    registry, task_registry, conn_mgr, provider, event_bus = setup
    dm = DrainManager(registry, task_registry, conn_mgr, provider, event_bus)
    result = await dm.drain_node("nonexistent")
    assert result is False


@pytest.mark.asyncio
async def test_cancel_drain(setup) -> None:
    registry, task_registry, conn_mgr, provider, event_bus = setup
    await task_registry.register("t1", "w1")
    await task_registry.update("t1", status=TaskStatus.RUNNING)

    dm = DrainManager(registry, task_registry, conn_mgr, provider, event_bus, drain_timeout=60)
    await dm.drain_node("w1")

    assert "w1" in dm.draining_nodes

    result = await dm.cancel_drain("w1")
    assert result is True
    assert "w1" not in dm.draining_nodes

    node = await registry.get("w1")
    assert node.status == NodeStatus.READY


@pytest.mark.asyncio
async def test_cancel_drain_not_draining(setup) -> None:
    registry, task_registry, conn_mgr, provider, event_bus = setup
    dm = DrainManager(registry, task_registry, conn_mgr, provider, event_bus)
    result = await dm.cancel_drain("w1")
    assert result is False


@pytest.mark.asyncio
async def test_duplicate_drain_request(setup) -> None:
    registry, task_registry, conn_mgr, provider, event_bus = setup
    await task_registry.register("t1", "w1")
    await task_registry.update("t1", status=TaskStatus.RUNNING)

    dm = DrainManager(registry, task_registry, conn_mgr, provider, event_bus, drain_timeout=60)
    await dm.drain_node("w1")
    result = await dm.drain_node("w1")
    assert result is True

    await dm.stop()


@pytest.mark.asyncio
async def test_credential_recovery(setup) -> None:
    registry, task_registry, conn_mgr, provider, event_bus = setup
    recovered = []

    async def recover(node_id: str) -> None:
        recovered.append(node_id)

    dm = DrainManager(
        registry, task_registry, conn_mgr, provider, event_bus,
        drain_timeout=5, on_recover_credentials=recover,
    )
    await dm.drain_node("w1")
    await asyncio.sleep(2)

    assert "w1" in recovered


@pytest.mark.asyncio
async def test_events_emitted(setup) -> None:
    registry, task_registry, conn_mgr, provider, event_bus = setup
    events: list[str] = []
    event_bus.subscribe("*", lambda et, wid, data: events.append(et))

    dm = DrainManager(registry, task_registry, conn_mgr, provider, event_bus, drain_timeout=5)
    await dm.drain_node("w1")
    await asyncio.sleep(2)

    assert "NODE_DRAINING" in events
    assert "NODE_TERMINATED" in events


@pytest.mark.asyncio
async def test_stop_cancels_all_drains(setup) -> None:
    registry, task_registry, conn_mgr, provider, event_bus = setup
    await task_registry.register("t1", "w1")
    await task_registry.update("t1", status=TaskStatus.RUNNING)

    dm = DrainManager(registry, task_registry, conn_mgr, provider, event_bus, drain_timeout=60)
    await dm.drain_node("w1")

    await dm.stop()
    assert len(dm.draining_nodes) == 0
