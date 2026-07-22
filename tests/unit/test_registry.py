"""Unit tests for NodeRegistry — CRUD, persistence, crash recovery, concurrency (T-101)."""

from __future__ import annotations

import asyncio
import json
import stat
from pathlib import Path

import pytest

from elastic_agent.core.registry import NodeRecord, NodeRegistry, NodeStatus


@pytest.fixture
def registry_path(tmp_path: Path) -> Path:
    return tmp_path / "registry.json"


@pytest.fixture
def registry(registry_path: Path) -> NodeRegistry:
    return NodeRegistry(registry_path)


def _make_record(node_id: str = "aliyun:i-test001", **kwargs) -> NodeRecord:
    defaults = {
        "node_id": node_id,
        "instance_id": "i-test001",
        "platform": "aliyun",
        "status": NodeStatus.CREATING,
    }
    defaults.update(kwargs)
    return NodeRecord(**defaults)


async def test_add_and_get(registry: NodeRegistry) -> None:
    rec = _make_record()
    await registry.add(rec)
    got = await registry.get("aliyun:i-test001")
    assert got is not None
    assert got.node_id == "aliyun:i-test001"
    assert got.platform == "aliyun"


async def test_update(registry: NodeRegistry) -> None:
    await registry.add(_make_record())
    updated = await registry.update("aliyun:i-test001", status=NodeStatus.READY)
    assert updated is not None
    assert updated.status == NodeStatus.READY


async def test_remove(registry: NodeRegistry) -> None:
    await registry.add(_make_record())
    removed = await registry.remove("aliyun:i-test001")
    assert removed is not None
    assert await registry.get("aliyun:i-test001") is None


async def test_list_all(registry: NodeRegistry) -> None:
    await registry.add(_make_record("n1"))
    await registry.add(_make_record("n2"))
    all_nodes = await registry.list_all()
    assert len(all_nodes) == 2


async def test_list_by_status(registry: NodeRegistry) -> None:
    await registry.add(_make_record("n1", status=NodeStatus.READY))
    await registry.add(_make_record("n2", status=NodeStatus.BUSY))
    await registry.add(_make_record("n3", status=NodeStatus.READY))
    ready = await registry.list_by_status(NodeStatus.READY)
    assert len(ready) == 2


async def test_persistence(registry_path: Path) -> None:
    reg1 = NodeRegistry(registry_path)
    await reg1.add(_make_record("n1", status=NodeStatus.READY))
    await reg1.add(_make_record("n2", status=NodeStatus.BUSY))

    reg2 = NodeRegistry(registry_path)
    await reg2.load()
    all_nodes = await reg2.list_all()
    assert len(all_nodes) == 2
    n1 = await reg2.get("n1")
    assert n1 is not None
    assert n1.status == NodeStatus.READY


async def test_crash_recovery(registry_path: Path) -> None:
    """Simulate crash: write registry, then reload from disk."""
    reg = NodeRegistry(registry_path)
    await reg.add(_make_record("crash-node", status=NodeStatus.BOOTSTRAPPING))
    assert registry_path.exists()

    raw = json.loads(registry_path.read_text())
    assert "crash-node" in raw["nodes"]

    reg2 = NodeRegistry(registry_path)
    await reg2.load()
    rec = await reg2.get("crash-node")
    assert rec is not None
    assert rec.status == NodeStatus.BOOTSTRAPPING


async def test_get_nonexistent(registry: NodeRegistry) -> None:
    assert await registry.get("no-such-node") is None


async def test_update_nonexistent(registry: NodeRegistry) -> None:
    assert await registry.update("no-such-node", status=NodeStatus.READY) is None


async def test_remove_nonexistent(registry: NodeRegistry) -> None:
    assert await registry.remove("no-such-node") is None


async def test_count(registry: NodeRegistry) -> None:
    assert await registry.count() == 0
    await registry.add(_make_record("n1"))
    assert await registry.count() == 1
    await registry.add(_make_record("n2"))
    assert await registry.count() == 2


async def test_atomic_write(registry_path: Path) -> None:
    """Verify flush uses atomic rename (tmp file)."""
    reg = NodeRegistry(registry_path)
    await reg.add(_make_record())
    assert registry_path.exists()
    assert not registry_path.with_suffix(".tmp").exists()


async def test_registry_state_permissions_are_private(registry_path: Path) -> None:
    registry_path.parent.chmod(0o755)
    reg = NodeRegistry(registry_path)
    await reg.add(_make_record(auth_token="worker-bearer"))

    assert stat.S_IMODE(registry_path.parent.stat().st_mode) == 0o700
    assert stat.S_IMODE(registry_path.stat().st_mode) == 0o600


async def test_load_repairs_legacy_registry_permissions(registry_path: Path) -> None:
    registry_path.write_text('{"nodes": {}}', encoding="utf-8")
    registry_path.chmod(0o644)
    registry_path.parent.chmod(0o755)

    await NodeRegistry(registry_path).load()

    assert stat.S_IMODE(registry_path.parent.stat().st_mode) == 0o700
    assert stat.S_IMODE(registry_path.stat().st_mode) == 0o600


async def test_load_repairs_recovery_job_spec_permissions(registry_path: Path) -> None:
    registry_path.write_text('{"nodes": {}}', encoding="utf-8")
    specs = registry_path.with_name("specs")
    specs.mkdir(mode=0o755)
    spec = specs / "job-legacy.json"
    spec.write_text("{}", encoding="utf-8")
    spec.chmod(0o644)

    await NodeRegistry(registry_path).load()

    assert stat.S_IMODE(specs.stat().st_mode) == 0o700
    assert stat.S_IMODE(spec.stat().st_mode) == 0o600


# ---------------------------------------------------------------------------
# Concurrency safety tests (T-101)
# ---------------------------------------------------------------------------


async def test_concurrent_add(registry: NodeRegistry) -> None:
    """Multiple concurrent adds should not lose data."""
    async def add_node(i: int):
        await registry.add(_make_record(f"node-{i}", instance_id=f"inst-{i}"))

    await asyncio.gather(*[add_node(i) for i in range(20)])

    assert await registry.count() == 20
    for i in range(20):
        assert await registry.get(f"node-{i}") is not None


async def test_concurrent_update(registry: NodeRegistry) -> None:
    """Concurrent updates to different nodes should all succeed."""
    for i in range(10):
        await registry.add(_make_record(f"n-{i}", instance_id=f"inst-{i}"))

    async def update_node(i: int):
        await registry.update(f"n-{i}", status=NodeStatus.READY)

    await asyncio.gather(*[update_node(i) for i in range(10)])

    for i in range(10):
        rec = await registry.get(f"n-{i}")
        assert rec is not None
        assert rec.status == NodeStatus.READY


async def test_concurrent_add_remove(registry: NodeRegistry) -> None:
    """Concurrent add and remove should not corrupt state."""
    for i in range(10):
        await registry.add(_make_record(f"n-{i}", instance_id=f"inst-{i}"))

    async def remove_even(i: int):
        if i % 2 == 0:
            await registry.remove(f"n-{i}")

    async def add_new(i: int):
        await registry.add(_make_record(f"new-{i}", instance_id=f"inst-new-{i}"))

    tasks = [remove_even(i) for i in range(10)] + [add_new(i) for i in range(5)]
    await asyncio.gather(*tasks)

    remaining = await registry.list_all()
    ids = {r.node_id for r in remaining}
    for i in range(10):
        if i % 2 == 0:
            assert f"n-{i}" not in ids
        else:
            assert f"n-{i}" in ids
    for i in range(5):
        assert f"new-{i}" in ids


async def test_concurrent_persistence(registry_path: Path) -> None:
    """Concurrent operations should produce consistent file on disk."""
    reg = NodeRegistry(registry_path)

    async def add_and_update(i: int):
        await reg.add(_make_record(f"n-{i}", instance_id=f"inst-{i}"))
        await reg.update(f"n-{i}", status=NodeStatus.READY)

    await asyncio.gather(*[add_and_update(i) for i in range(10)])

    reg2 = NodeRegistry(registry_path)
    await reg2.load()
    assert await reg2.count() == 10
    for i in range(10):
        rec = await reg2.get(f"n-{i}")
        assert rec is not None
        assert rec.status == NodeStatus.READY


async def test_list_all_ids(registry: NodeRegistry) -> None:
    await registry.add(_make_record("a"))
    await registry.add(_make_record("b"))
    await registry.add(_make_record("c"))
    ids = await registry.list_all_ids()
    assert ids == {"a", "b", "c"}
