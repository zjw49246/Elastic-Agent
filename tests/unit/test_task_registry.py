"""Tests for TaskRegistry — T-050."""

from __future__ import annotations

import asyncio
import json

import pytest

from elastic_agent.core.task_registry import TaskRecord, TaskRegistry, TaskStatus


@pytest.fixture
def registry_path(tmp_path):
    return tmp_path / "task_registry.json"


@pytest.fixture
def registry(registry_path):
    return TaskRegistry(registry_path)


@pytest.mark.asyncio
async def test_register_and_get(registry):
    rec = await registry.register("t1", "worker-1", {"book": "abc"})
    assert rec.task_id == "t1"
    assert rec.worker_id == "worker-1"
    assert rec.status == TaskStatus.RUNNING
    assert rec.metadata == {"book": "abc"}

    got = await registry.get("t1")
    assert got is not None
    assert got.task_id == "t1"


@pytest.mark.asyncio
async def test_get_nonexistent(registry):
    assert await registry.get("nope") is None


@pytest.mark.asyncio
async def test_update(registry):
    await registry.register("t1", "w1")
    updated = await registry.update("t1", session_id="sess-1", status=TaskStatus.COMPLETED)
    assert updated is not None
    assert updated.session_id == "sess-1"
    assert updated.status == TaskStatus.COMPLETED


@pytest.mark.asyncio
async def test_update_nonexistent(registry):
    assert await registry.update("nope", status=TaskStatus.FAILED) is None


@pytest.mark.asyncio
async def test_unregister(registry):
    await registry.register("t1", "w1")
    removed = await registry.unregister("t1")
    assert removed is not None
    assert removed.task_id == "t1"
    assert await registry.get("t1") is None


@pytest.mark.asyncio
async def test_unregister_nonexistent(registry):
    assert await registry.unregister("nope") is None


@pytest.mark.asyncio
async def test_list_by_worker(registry):
    await registry.register("t1", "w1")
    await registry.register("t2", "w1")
    await registry.register("t3", "w2")

    w1_tasks = await registry.list_by_worker("w1")
    assert len(w1_tasks) == 2
    assert {r.task_id for r in w1_tasks} == {"t1", "t2"}

    w2_tasks = await registry.list_by_worker("w2")
    assert len(w2_tasks) == 1


@pytest.mark.asyncio
async def test_list_by_status(registry):
    await registry.register("t1", "w1")
    await registry.register("t2", "w1")
    await registry.update("t2", status=TaskStatus.COMPLETED)

    running = await registry.list_by_status(TaskStatus.RUNNING)
    assert len(running) == 1
    assert running[0].task_id == "t1"

    completed = await registry.list_by_status(TaskStatus.COMPLETED)
    assert len(completed) == 1


@pytest.mark.asyncio
async def test_list_all(registry):
    await registry.register("t1", "w1")
    await registry.register("t2", "w2")
    all_tasks = await registry.list_all()
    assert len(all_tasks) == 2


@pytest.mark.asyncio
async def test_count(registry):
    assert await registry.count() == 0
    await registry.register("t1", "w1")
    assert await registry.count() == 1
    await registry.register("t2", "w1")
    assert await registry.count() == 2


@pytest.mark.asyncio
async def test_count_active_by_worker(registry):
    await registry.register("t1", "w1")
    await registry.register("t2", "w1")
    await registry.register("t3", "w2")
    await registry.update("t2", status=TaskStatus.COMPLETED)

    assert await registry.count_active_by_worker("w1") == 1
    assert await registry.count_active_by_worker("w2") == 1
    assert await registry.count_active_by_worker("w3") == 0


@pytest.mark.asyncio
async def test_cleanup_worker(registry):
    await registry.register("t1", "w1")
    await registry.register("t2", "w1")
    await registry.register("t3", "w2")
    await registry.update("t2", status=TaskStatus.COMPLETED)

    affected = await registry.cleanup_worker("w1")
    assert len(affected) == 1
    assert affected[0].task_id == "t1"
    assert affected[0].status == TaskStatus.WORKER_LOST

    t2 = await registry.get("t2")
    assert t2.status == TaskStatus.COMPLETED

    t3 = await registry.get("t3")
    assert t3.status == TaskStatus.RUNNING


@pytest.mark.asyncio
async def test_cleanup_worker_no_running_tasks(registry):
    await registry.register("t1", "w1")
    await registry.update("t1", status=TaskStatus.COMPLETED)
    affected = await registry.cleanup_worker("w1")
    assert len(affected) == 0


@pytest.mark.asyncio
async def test_persistence(registry_path):
    reg1 = TaskRegistry(registry_path)
    await reg1.register("t1", "w1", {"key": "value"})
    await reg1.update("t1", session_id="sess-abc")

    reg2 = TaskRegistry(registry_path)
    await reg2.load()
    got = await reg2.get("t1")
    assert got is not None
    assert got.task_id == "t1"
    assert got.worker_id == "w1"
    assert got.session_id == "sess-abc"
    assert got.metadata == {"key": "value"}


@pytest.mark.asyncio
async def test_crash_recovery(registry_path):
    reg1 = TaskRegistry(registry_path)
    await reg1.register("t1", "w1")
    await reg1.register("t2", "w2")
    await reg1.register("t3", "w1")
    await reg1.update("t3", status=TaskStatus.COMPLETED)

    reg2 = TaskRegistry(registry_path)
    await reg2.load()
    affected = await reg2.recover(online_worker_ids={"w2"})
    assert len(affected) == 1
    assert affected[0].task_id == "t1"
    assert affected[0].status == TaskStatus.WORKER_LOST

    t2 = await reg2.get("t2")
    assert t2.status == TaskStatus.RUNNING

    t3 = await reg2.get("t3")
    assert t3.status == TaskStatus.COMPLETED


@pytest.mark.asyncio
async def test_recover_no_offline(registry):
    await registry.register("t1", "w1")
    affected = await registry.recover(online_worker_ids={"w1"})
    assert len(affected) == 0


@pytest.mark.asyncio
async def test_lazy_load_on_first_operation(registry_path):
    reg = TaskRegistry(registry_path)
    await reg.register("t1", "w1")
    assert await reg.count() == 1


@pytest.mark.asyncio
async def test_corrupted_file(registry_path):
    registry_path.write_text("not json", encoding="utf-8")
    reg = TaskRegistry(registry_path)
    await reg.load()
    assert await reg.count() == 0


@pytest.mark.asyncio
async def test_concurrent_operations(registry):
    async def add_task(i):
        await registry.register(f"t{i}", f"w{i % 3}")

    await asyncio.gather(*[add_task(i) for i in range(20)])
    assert await registry.count() == 20


@pytest.mark.asyncio
async def test_update_sets_updated_at(registry):
    rec = await registry.register("t1", "w1")
    original_updated = rec.updated_at

    await asyncio.sleep(0.01)
    updated = await registry.update("t1", status=TaskStatus.COMPLETED)
    assert updated.updated_at > original_updated
