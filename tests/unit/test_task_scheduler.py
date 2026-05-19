"""Tests for TaskScheduler — T-051."""

from __future__ import annotations

import pytest

from elastic_agent.core.registry import NodeRecord, NodeRegistry, NodeStatus
from elastic_agent.core.task_registry import TaskRegistry
from elastic_agent.core.task_scheduler import TaskScheduler
from elastic_agent.harness.base import Harness, WorkerCapacity, BootstrapStep


@pytest.fixture
def node_registry(tmp_path):
    return NodeRegistry(tmp_path / "registry.json")


@pytest.fixture
def task_registry(tmp_path):
    return TaskRegistry(tmp_path / "task_registry.json")


class FakeHarness(Harness):
    def __init__(self, max_tasks: int = 2):
        self._capacity = WorkerCapacity(max_concurrent_tasks=max_tasks)

    def get_bootstrap_steps(self) -> list[BootstrapStep]:
        return []

    def get_worker_capacity(self) -> WorkerCapacity:
        return self._capacity


@pytest.mark.asyncio
async def test_no_ready_workers(node_registry, task_registry):
    scheduler = TaskScheduler(node_registry, task_registry)
    result = await scheduler.find_available_worker()
    assert result is None


@pytest.mark.asyncio
async def test_single_ready_worker(node_registry, task_registry):
    await node_registry.add(NodeRecord(
        node_id="w1", instance_id="i-1", platform="test", status=NodeStatus.READY,
    ))
    scheduler = TaskScheduler(node_registry, task_registry)
    result = await scheduler.find_available_worker()
    assert result == "w1"


@pytest.mark.asyncio
async def test_worker_at_capacity(node_registry, task_registry):
    await node_registry.add(NodeRecord(
        node_id="w1", instance_id="i-1", platform="test", status=NodeStatus.READY,
    ))
    await task_registry.register("t1", "w1")

    scheduler = TaskScheduler(node_registry, task_registry)
    result = await scheduler.find_available_worker()
    assert result is None


@pytest.mark.asyncio
async def test_selects_least_busy(node_registry, task_registry):
    await node_registry.add(NodeRecord(
        node_id="w1", instance_id="i-1", platform="test", status=NodeStatus.READY,
    ))
    await node_registry.add(NodeRecord(
        node_id="w2", instance_id="i-2", platform="test", status=NodeStatus.READY,
    ))
    harness = FakeHarness(max_tasks=3)
    scheduler = TaskScheduler(node_registry, task_registry, harness)

    await task_registry.register("t1", "w1")
    await task_registry.register("t2", "w1")

    result = await scheduler.find_available_worker()
    assert result == "w2"


@pytest.mark.asyncio
async def test_all_at_capacity(node_registry, task_registry):
    harness = FakeHarness(max_tasks=1)
    await node_registry.add(NodeRecord(
        node_id="w1", instance_id="i-1", platform="test", status=NodeStatus.READY,
    ))
    await node_registry.add(NodeRecord(
        node_id="w2", instance_id="i-2", platform="test", status=NodeStatus.READY,
    ))
    await task_registry.register("t1", "w1")
    await task_registry.register("t2", "w2")

    scheduler = TaskScheduler(node_registry, task_registry, harness)
    result = await scheduler.find_available_worker()
    assert result is None


@pytest.mark.asyncio
async def test_ignores_non_ready_workers(node_registry, task_registry):
    await node_registry.add(NodeRecord(
        node_id="w1", instance_id="i-1", platform="test", status=NodeStatus.DRAINING,
    ))
    await node_registry.add(NodeRecord(
        node_id="w2", instance_id="i-2", platform="test", status=NodeStatus.FAILED,
    ))
    await node_registry.add(NodeRecord(
        node_id="w3", instance_id="i-3", platform="test", status=NodeStatus.CREATING,
    ))

    scheduler = TaskScheduler(node_registry, task_registry)
    result = await scheduler.find_available_worker()
    assert result is None


@pytest.mark.asyncio
async def test_completed_tasks_dont_count(node_registry, task_registry):
    await node_registry.add(NodeRecord(
        node_id="w1", instance_id="i-1", platform="test", status=NodeStatus.READY,
    ))
    harness = FakeHarness(max_tasks=1)

    await task_registry.register("t1", "w1")
    from elastic_agent.core.task_registry import TaskStatus
    await task_registry.update("t1", status=TaskStatus.COMPLETED)

    scheduler = TaskScheduler(node_registry, task_registry, harness)
    result = await scheduler.find_available_worker()
    assert result == "w1"


@pytest.mark.asyncio
async def test_default_capacity_without_harness(node_registry, task_registry):
    await node_registry.add(NodeRecord(
        node_id="w1", instance_id="i-1", platform="test", status=NodeStatus.READY,
    ))
    scheduler = TaskScheduler(node_registry, task_registry)

    await task_registry.register("t1", "w1")
    result = await scheduler.find_available_worker()
    assert result is None


@pytest.mark.asyncio
async def test_multiple_workers_select_empty_one(node_registry, task_registry):
    harness = FakeHarness(max_tasks=2)
    for i in range(3):
        await node_registry.add(NodeRecord(
            node_id=f"w{i}", instance_id=f"i-{i}", platform="test", status=NodeStatus.READY,
        ))
    await task_registry.register("t1", "w0")
    await task_registry.register("t2", "w1")

    scheduler = TaskScheduler(node_registry, task_registry, harness)
    result = await scheduler.find_available_worker()
    assert result == "w2"
