"""Unit tests for NodeRegistry — CRUD, persistence, crash recovery."""

from __future__ import annotations

import json
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
