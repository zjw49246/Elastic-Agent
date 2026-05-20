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


# ===========================================================================
# T-138: TaskRegistry extended — CRUD, persistence, crash recovery,
#        worker offline cleanup
# ===========================================================================


class TestT138CRUD:
    """T-138: Comprehensive CRUD operations."""

    @pytest.mark.asyncio
    async def test_register_overwrites_existing(self, registry):
        rec1 = await registry.register("t1", "w1", {"v": 1})
        rec2 = await registry.register("t1", "w2", {"v": 2})
        assert rec2.worker_id == "w2"
        assert rec2.metadata == {"v": 2}
        assert await registry.count() == 1

    @pytest.mark.asyncio
    async def test_update_multiple_fields(self, registry):
        await registry.register("t1", "w1")
        updated = await registry.update(
            "t1", session_id="s-1", status=TaskStatus.COMPLETED,
            metadata={"result": "ok"},
        )
        assert updated.session_id == "s-1"
        assert updated.status == TaskStatus.COMPLETED
        assert updated.metadata == {"result": "ok"}

    @pytest.mark.asyncio
    async def test_unregister_reduces_count(self, registry):
        await registry.register("t1", "w1")
        await registry.register("t2", "w1")
        assert await registry.count() == 2
        await registry.unregister("t1")
        assert await registry.count() == 1

    @pytest.mark.asyncio
    async def test_list_by_worker_empty(self, registry):
        tasks = await registry.list_by_worker("no-worker")
        assert tasks == []

    @pytest.mark.asyncio
    async def test_register_with_complex_metadata(self, registry):
        meta = {"book": "abc", "tags": ["fiction", "sci-fi"], "config": {"timeout": 300}}
        rec = await registry.register("t1", "w1", meta)
        assert rec.metadata["tags"] == ["fiction", "sci-fi"]
        assert rec.metadata["config"]["timeout"] == 300


class TestT138Persistence:
    """T-138: Persistence and file handling."""

    @pytest.mark.asyncio
    async def test_rapid_register_unregister_persists(self, registry_path):
        reg = TaskRegistry(registry_path)
        for i in range(10):
            await reg.register(f"t{i}", f"w{i % 3}")
        for i in range(0, 10, 2):
            await reg.unregister(f"t{i}")

        reg2 = TaskRegistry(registry_path)
        await reg2.load()
        assert await reg2.count() == 5
        for i in range(1, 10, 2):
            t = await reg2.get(f"t{i}")
            assert t is not None

    @pytest.mark.asyncio
    async def test_metadata_roundtrip(self, registry_path):
        reg = TaskRegistry(registry_path)
        await reg.register("t1", "w1", {"nested": {"key": [1, 2, 3]}})

        reg2 = TaskRegistry(registry_path)
        await reg2.load()
        t = await reg2.get("t1")
        assert t.metadata["nested"]["key"] == [1, 2, 3]

    @pytest.mark.asyncio
    async def test_session_id_persists(self, registry_path):
        reg = TaskRegistry(registry_path)
        await reg.register("t1", "w1")
        await reg.update("t1", session_id="session-xyz-123")

        reg2 = TaskRegistry(registry_path)
        await reg2.load()
        t = await reg2.get("t1")
        assert t.session_id == "session-xyz-123"

    @pytest.mark.asyncio
    async def test_empty_file_loads_cleanly(self, registry_path):
        registry_path.write_text("{}", encoding="utf-8")
        reg = TaskRegistry(registry_path)
        await reg.load()
        assert await reg.count() == 0


class TestT138CrashRecovery:
    """T-138: Crash recovery scenarios."""

    @pytest.mark.asyncio
    async def test_recovery_all_workers_offline(self, registry_path):
        reg = TaskRegistry(registry_path)
        await reg.register("t1", "w1")
        await reg.register("t2", "w2")
        await reg.register("t3", "w3")

        reg2 = TaskRegistry(registry_path)
        await reg2.load()
        affected = await reg2.recover(online_worker_ids=set())
        assert len(affected) == 3
        for t in affected:
            assert t.status == TaskStatus.WORKER_LOST

    @pytest.mark.asyncio
    async def test_recovery_partial_workers_online(self, registry_path):
        reg = TaskRegistry(registry_path)
        await reg.register("t1", "w1")
        await reg.register("t2", "w2")
        await reg.register("t3", "w3")
        await reg.update("t3", status=TaskStatus.COMPLETED)

        reg2 = TaskRegistry(registry_path)
        await reg2.load()
        affected = await reg2.recover(online_worker_ids={"w1"})
        assert len(affected) == 1
        assert affected[0].task_id == "t2"
        t1 = await reg2.get("t1")
        assert t1.status == TaskStatus.RUNNING
        t3 = await reg2.get("t3")
        assert t3.status == TaskStatus.COMPLETED

    @pytest.mark.asyncio
    async def test_recovery_idempotent(self, registry_path):
        reg = TaskRegistry(registry_path)
        await reg.register("t1", "w1")

        reg2 = TaskRegistry(registry_path)
        await reg2.load()
        await reg2.recover(online_worker_ids=set())
        affected2 = await reg2.recover(online_worker_ids=set())
        assert len(affected2) == 0


class TestT138WorkerCleanup:
    """T-138: Worker offline cleanup."""

    @pytest.mark.asyncio
    async def test_cleanup_multiple_running_tasks(self, registry):
        for i in range(5):
            await registry.register(f"t{i}", "w1")
        affected = await registry.cleanup_worker("w1")
        assert len(affected) == 5
        for t in affected:
            assert t.status == TaskStatus.WORKER_LOST

    @pytest.mark.asyncio
    async def test_cleanup_preserves_other_workers(self, registry):
        await registry.register("t1", "w1")
        await registry.register("t2", "w2")
        affected = await registry.cleanup_worker("w1")
        assert len(affected) == 1
        t2 = await registry.get("t2")
        assert t2.status == TaskStatus.RUNNING

    @pytest.mark.asyncio
    async def test_cleanup_skips_completed_and_failed(self, registry):
        await registry.register("t1", "w1")
        await registry.register("t2", "w1")
        await registry.register("t3", "w1")
        await registry.update("t1", status=TaskStatus.COMPLETED)
        await registry.update("t2", status=TaskStatus.FAILED)
        affected = await registry.cleanup_worker("w1")
        assert len(affected) == 1
        assert affected[0].task_id == "t3"

    @pytest.mark.asyncio
    async def test_cleanup_empty_worker(self, registry):
        affected = await registry.cleanup_worker("nobody")
        assert len(affected) == 0

    @pytest.mark.asyncio
    async def test_sequential_worker_cleanup(self, registry):
        await registry.register("t1", "w1")
        await registry.register("t2", "w2")
        await registry.register("t3", "w1")
        a1 = await registry.cleanup_worker("w1")
        assert len(a1) == 2
        a2 = await registry.cleanup_worker("w2")
        assert len(a2) == 1
