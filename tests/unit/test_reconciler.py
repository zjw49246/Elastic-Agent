"""Unit tests for CloudReconciler — orphan/ghost/conflict detection."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from elastic_agent.core.providers.base import CloudProvider, Instance, InstanceState
from elastic_agent.core.reconciler import CloudReconciler
from elastic_agent.core.registry import NodeRecord, NodeRegistry, NodeStatus


def _make_instance(instance_id: str, state: str = "running", **kwargs) -> Instance:
    defaults = {
        "instance_id": instance_id,
        "platform": "aliyun",
        "native_id": instance_id.split(":")[-1] if ":" in instance_id else instance_id,
        "state": InstanceState(state),
        "public_ip": "1.2.3.4",
        "private_ip": "10.0.0.1",
        "created_at": datetime.now(timezone.utc),
    }
    defaults.update(kwargs)
    return Instance(**defaults)


def _make_record(node_id: str, status: NodeStatus = NodeStatus.READY) -> NodeRecord:
    return NodeRecord(
        node_id=node_id,
        instance_id=node_id,
        platform="aliyun",
        status=status,
    )


@pytest.fixture
def registry(tmp_path: Path) -> NodeRegistry:
    return NodeRegistry(tmp_path / "registry.json")


@pytest.fixture
def provider() -> AsyncMock:
    mock = AsyncMock(spec=CloudProvider)
    mock.MANAGED_TAG_KEY = CloudProvider.MANAGED_TAG_KEY
    mock.MANAGED_TAG_VALUE = CloudProvider.MANAGED_TAG_VALUE
    mock.list_instances = AsyncMock(return_value=[])
    return mock


async def test_no_divergence(registry: NodeRegistry, provider: AsyncMock) -> None:
    """Both in sync — nothing to do."""
    inst = _make_instance("n1")
    provider.list_instances.return_value = [inst]
    await registry.add(_make_record("n1"))

    reconciler = CloudReconciler(provider, registry)
    result = await reconciler.reconcile()

    assert result.orphans_adopted == []
    assert result.ghosts_removed == []
    assert result.state_conflicts_resolved == []


async def test_orphan_adoption(registry: NodeRegistry, provider: AsyncMock) -> None:
    """Instance in cloud but not in registry — should be adopted."""
    provider.list_instances.return_value = [_make_instance("orphan-1")]

    reconciler = CloudReconciler(provider, registry)
    result = await reconciler.reconcile()

    assert "orphan-1" in result.orphans_adopted
    rec = await registry.get("orphan-1")
    assert rec is not None
    assert rec.status == NodeStatus.READY


async def test_ghost_removal(registry: NodeRegistry, provider: AsyncMock) -> None:
    """Node in registry but not in cloud — should be removed."""
    await registry.add(_make_record("ghost-1", NodeStatus.READY))
    provider.list_instances.return_value = []

    reconciler = CloudReconciler(provider, registry)
    result = await reconciler.reconcile()

    assert "ghost-1" in result.ghosts_removed
    assert await registry.get("ghost-1") is None


async def test_state_conflict_terminated(registry: NodeRegistry, provider: AsyncMock) -> None:
    """Cloud says terminated but registry says READY — update to terminated."""
    provider.list_instances.return_value = [_make_instance("n1", state="terminated")]
    await registry.add(_make_record("n1", NodeStatus.READY))

    reconciler = CloudReconciler(provider, registry)
    result = await reconciler.reconcile()

    assert "n1" in result.state_conflicts_resolved
    rec = await registry.get("n1")
    assert rec is not None
    assert rec.status == NodeStatus.TERMINATED


async def test_state_conflict_stopped(registry: NodeRegistry, provider: AsyncMock) -> None:
    """Cloud says stopped but registry says BUSY — update to terminated."""
    provider.list_instances.return_value = [_make_instance("n1", state="stopped")]
    await registry.add(_make_record("n1", NodeStatus.BUSY))

    reconciler = CloudReconciler(provider, registry)
    result = await reconciler.reconcile()

    assert "n1" in result.state_conflicts_resolved
    rec = await registry.get("n1")
    assert rec is not None
    assert rec.status == NodeStatus.TERMINATED


async def test_multiple_scenarios(registry: NodeRegistry, provider: AsyncMock) -> None:
    """Mix of orphans, ghosts, and normal nodes."""
    provider.list_instances.return_value = [
        _make_instance("normal"),
        _make_instance("orphan"),
    ]
    await registry.add(_make_record("normal"))
    await registry.add(_make_record("ghost"))

    reconciler = CloudReconciler(provider, registry)
    result = await reconciler.reconcile()

    assert "orphan" in result.orphans_adopted
    assert "ghost" in result.ghosts_removed
    assert result.cloud_instance_count == 2
    assert await registry.get("orphan") is not None
    assert await registry.get("ghost") is None
    assert await registry.get("normal") is not None


async def test_ip_update_on_reconcile(registry: NodeRegistry, provider: AsyncMock) -> None:
    """Running instance with new IP should update registry."""
    await registry.add(_make_record("n1"))
    provider.list_instances.return_value = [
        _make_instance("n1", public_ip="5.6.7.8", private_ip="10.0.0.99")
    ]

    reconciler = CloudReconciler(provider, registry)
    await reconciler.reconcile()

    rec = await registry.get("n1")
    assert rec is not None
    assert rec.public_ip == "5.6.7.8"
    assert rec.private_ip == "10.0.0.99"
