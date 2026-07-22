"""Unit tests for CloudReconciler — orphan/ghost/conflict detection."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from elastic_agent.core.providers.base import (
    CloudProvider,
    Instance,
    InstanceNotFoundError,
    InstanceState,
)
from elastic_agent.core.reconciler import CloudReconciler
from elastic_agent.core.registry import NodeRecord, NodeRegistry, NodeStatus


def _make_instance(instance_id: str, state: str = "running", **kwargs) -> Instance:
    tags = {
        CloudProvider.MANAGED_TAG_KEY: CloudProvider.MANAGED_TAG_VALUE,
        **kwargs.pop("tags", {}),
    }
    defaults = {
        "instance_id": instance_id,
        "platform": "aliyun",
        "native_id": instance_id.split(":")[-1] if ":" in instance_id else instance_id,
        "state": InstanceState(state),
        "public_ip": "1.2.3.4",
        "private_ip": "10.0.0.1",
        "created_at": datetime.now(timezone.utc),
        "tags": tags,
    }
    defaults.update(kwargs)
    return Instance(**defaults)


def _make_record(
    node_id: str, status: NodeStatus = NodeStatus.READY, **kwargs
) -> NodeRecord:
    defaults = {
        "node_id": node_id,
        "instance_id": node_id,
        "platform": "aliyun",
        "status": status,
    }
    defaults.update(kwargs)
    return NodeRecord(
        **defaults,
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


async def test_terminated_unbound_orphan_is_not_adopted(
    registry: NodeRegistry, provider: AsyncMock
) -> None:
    """Completed disposable workers must not repopulate the Node registry."""
    provider.list_instances.return_value = [
        _make_instance(
            "aws:i-complete",
            state="terminated",
            platform="aws",
            tags={"ElasticAgentJob": "job-complete"},
        )
    ]

    result = await CloudReconciler(provider, registry).reconcile()

    assert result.orphans_adopted == []
    assert await registry.get("aws:i-complete") is None


async def test_orphan_adoption_uses_canonical_id_and_restores_lifecycle_metadata(
    registry: NodeRegistry, provider: AsyncMock
) -> None:
    """Recovery must retain canonical provider ids and durable lease tags."""
    provider.list_instances.return_value = [
        _make_instance(
            "aws:i-orphan",
            platform="aws",
            native_id="i-orphan",
            tags={
                "ElasticAgentJob": "job-1",
                "ElasticAgentAccount": "account-1",
                "ElasticAgentLease": "lease-1",
                "Unrelated": "not-control-plane-metadata",
            },
        )
    ]

    reconciler = CloudReconciler(provider, registry)
    result = await reconciler.reconcile()

    assert result.orphans_adopted == ["aws:i-orphan"]
    rec = await registry.get("aws:i-orphan")
    assert rec is not None
    assert rec.instance_id == "aws:i-orphan"
    assert rec.metadata == {
        "job_id": "job-1",
        "account_id": "account-1",
        "lease_id": "lease-1",
    }


async def test_ghost_removal(registry: NodeRegistry, provider: AsyncMock) -> None:
    """Node in registry but not in cloud — should be removed."""
    await registry.add(_make_record("ghost-1", NodeStatus.READY))
    provider.list_instances.return_value = []

    reconciler = CloudReconciler(provider, registry)
    result = await reconciler.reconcile()

    assert "ghost-1" in result.ghosts_removed
    assert await registry.get("ghost-1") is None


async def test_bound_ghost_delegates_cleanup_and_is_not_removed(
    registry: NodeRegistry, provider: AsyncMock
) -> None:
    """A missing leased instance must go through durable lease cleanup."""
    callback = AsyncMock()
    await registry.add(
        _make_record(
            "aws:i-bound",
            NodeStatus.READY,
            metadata={"lease_id": "lease-1", "job_id": "job-1"},
        )
    )

    reconciler = CloudReconciler(provider, registry, on_bound_lost=callback)
    result = await reconciler.reconcile()

    callback.assert_awaited_once_with("aws:i-bound", "lease-1")
    assert result.bound_nodes_lost == ["aws:i-bound"]
    assert result.ghosts_removed == []
    rec = await registry.get("aws:i-bound")
    assert rec is not None
    assert rec.status == NodeStatus.TERMINATED


async def test_bound_ghost_without_callback_is_retained(
    registry: NodeRegistry, provider: AsyncMock
) -> None:
    """Missing wiring must fail closed instead of bypassing lease cleanup."""
    await registry.add(
        _make_record("aws:i-bound", metadata={"lease_id": "lease-1"})
    )

    result = await CloudReconciler(provider, registry).reconcile()

    assert result.bound_nodes_lost == ["aws:i-bound"]
    assert result.ghosts_removed == []
    assert await registry.get("aws:i-bound") is not None


async def test_bound_ghost_callback_failure_is_retryable(
    registry: NodeRegistry, provider: AsyncMock
) -> None:
    """A callback error must retain the record and not abort other cleanup."""
    callback = AsyncMock(side_effect=RuntimeError("cleanup failed"))
    await registry.add(
        _make_record("aws:i-bound", metadata={"lease_id": "lease-1"})
    )
    await registry.add(_make_record("plain-ghost"))

    reconciler = CloudReconciler(provider, registry, on_bound_lost=callback)
    result = await reconciler.reconcile()

    callback.assert_awaited_once_with("aws:i-bound", "lease-1")
    assert result.bound_nodes_lost == ["aws:i-bound"]
    assert result.ghosts_removed == ["plain-ghost"]
    assert await registry.get("aws:i-bound") is not None
    assert await registry.get("plain-ghost") is None


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


async def test_cloud_terminated_bound_node_delegates_cleanup(
    registry: NodeRegistry, provider: AsyncMock
) -> None:
    """A leased instance terminated in cloud must trigger lease cleanup."""
    callback = AsyncMock()
    provider.list_instances.return_value = [
        _make_instance("aws:i-bound", state="terminated")
    ]
    await registry.add(
        _make_record("aws:i-bound", metadata={"lease_id": "lease-1"})
    )

    reconciler = CloudReconciler(provider, registry, on_bound_lost=callback)
    result = await reconciler.reconcile()

    callback.assert_awaited_once_with("aws:i-bound", "lease-1")
    assert result.bound_nodes_lost == ["aws:i-bound"]
    assert result.ghosts_removed == []
    rec = await registry.get("aws:i-bound")
    assert rec is not None
    assert rec.status == NodeStatus.TERMINATED


async def test_terminated_bound_orphan_triggers_cleanup_immediately(
    registry: NodeRegistry, provider: AsyncMock
) -> None:
    """A terminated orphan need not wait for the next reconciliation pass."""
    callback = AsyncMock()
    provider.list_instances.return_value = [
        _make_instance(
            "aws:i-bound",
            state="terminated",
            tags={"ElasticAgentLease": "lease-1"},
        )
    ]

    reconciler = CloudReconciler(provider, registry, on_bound_lost=callback)
    result = await reconciler.reconcile()

    callback.assert_awaited_once_with("aws:i-bound", "lease-1")
    assert result.orphans_adopted == ["aws:i-bound"]
    assert result.bound_nodes_lost == ["aws:i-bound"]
    assert await registry.get("aws:i-bound") is not None


async def test_state_conflict_stopped(registry: NodeRegistry, provider: AsyncMock) -> None:
    """Cloud says stopped but registry says BUSY — update to stopped."""
    provider.list_instances.return_value = [_make_instance("n1", state="stopped")]
    await registry.add(_make_record("n1", NodeStatus.BUSY))

    reconciler = CloudReconciler(provider, registry)
    result = await reconciler.reconcile()

    assert "n1" in result.state_conflicts_resolved
    rec = await registry.get("n1")
    assert rec is not None
    assert rec.status == NodeStatus.STOPPED


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


async def test_controller_scope_defensively_ignores_foreign_instances(
    registry: NodeRegistry, provider: AsyncMock
) -> None:
    provider.list_instances.return_value = [
        _make_instance(
            "aws:i-own", platform="aws",
            tags={"ElasticAgentController": "controller-own"},
        ),
        _make_instance(
            "aws:i-foreign", platform="aws",
            tags={"ElasticAgentController": "controller-foreign"},
        ),
    ]
    reconciler = CloudReconciler(
        provider, registry, controller_id="controller-own"
    )

    result = await reconciler.reconcile()

    assert result.orphans_adopted == ["aws:i-own"]
    assert await registry.get("aws:i-foreign") is None


async def test_controller_scope_rejects_instance_without_managed_tag(
    registry: NodeRegistry, provider: AsyncMock
) -> None:
    instance = _make_instance(
        "aws:i-unmanaged",
        platform="aws",
        tags={
            "ElasticAgentController": "controller-own",
            "ElasticAgentLease": "lease-1",
            "ElasticAgentAccount": "account-1",
        },
    )
    instance.tags.pop(CloudProvider.MANAGED_TAG_KEY)
    provider.list_instances.return_value = [instance]
    callback = AsyncMock()
    reconciler = CloudReconciler(
        provider,
        registry,
        on_bound_lost=callback,
        controller_id="controller-own",
    )

    result = await reconciler.reconcile()

    assert result.orphans_adopted == []
    callback.assert_not_awaited()
    assert await registry.get(instance.instance_id) is None


async def test_exact_controller_match_without_managed_tag_is_unresolved(
    registry: NodeRegistry, provider: AsyncMock
) -> None:
    await registry.add(_make_record(
        "aws:i-unmanaged",
        platform="aws",
        metadata={"lease_id": "lease-1"},
    ))
    candidate = _make_instance(
        "aws:i-unmanaged",
        platform="aws",
        tags={"ElasticAgentController": "controller-own"},
    )
    candidate.tags.pop(CloudProvider.MANAGED_TAG_KEY)
    provider.list_instances.return_value = []
    provider.get_instance.return_value = candidate
    callback = AsyncMock()
    reconciler = CloudReconciler(
        provider,
        registry,
        on_bound_lost=callback,
        controller_id="controller-own",
    )

    result = await reconciler.reconcile()

    assert result.ghosts_removed == []
    callback.assert_not_awaited()
    assert await registry.get(candidate.instance_id) is not None


async def test_exact_lookup_network_error_retains_legacy_registry_record(
    registry: NodeRegistry, provider: AsyncMock
) -> None:
    await registry.add(_make_record("aws:i-legacy", platform="aws"))
    provider.list_instances.return_value = []
    provider.get_instance.side_effect = RuntimeError("throttled")
    reconciler = CloudReconciler(
        provider, registry, controller_id="controller-own"
    )

    result = await reconciler.reconcile()

    assert result.ghosts_removed == []
    assert await registry.get("aws:i-legacy") is not None


async def test_exact_not_found_requires_three_scans_before_ghost_cleanup(
    registry: NodeRegistry, provider: AsyncMock
) -> None:
    await registry.add(_make_record("aws:i-missing", platform="aws"))
    provider.list_instances.return_value = []
    provider.get_instance.side_effect = InstanceNotFoundError("missing")
    reconciler = CloudReconciler(
        provider, registry, controller_id="controller-own"
    )

    assert (await reconciler.reconcile()).ghosts_removed == []
    assert (await reconciler.reconcile()).ghosts_removed == []
    assert (await reconciler.reconcile()).ghosts_removed == ["aws:i-missing"]


async def test_running_bound_orphan_cleanup_failure_retries_next_scan(
    registry: NodeRegistry, provider: AsyncMock
) -> None:
    instance = _make_instance(
        "aws:i-orphan",
        platform="aws",
        tags={"ElasticAgentLease": "lease-1"},
    )
    provider.list_instances.return_value = [instance]
    callback = AsyncMock(side_effect=[RuntimeError("transient"), None])
    reconciler = CloudReconciler(provider, registry, on_bound_lost=callback)

    await reconciler.reconcile()
    await reconciler.reconcile()

    assert callback.await_count == 2


async def test_foreign_exact_instance_never_becomes_a_bound_ghost(
    registry: NodeRegistry, provider: AsyncMock
) -> None:
    await registry.add(_make_record(
        "aws:i-foreign",
        platform="aws",
        metadata={"lease_id": "lease-foreign"},
    ))
    provider.list_instances.return_value = []
    provider.get_instance.return_value = _make_instance(
        "aws:i-foreign",
        platform="aws",
        tags={
            "ManagedBy": "elastic-agent",
            "ElasticAgentController": "controller-other",
        },
    )
    callback = AsyncMock()
    reconciler = CloudReconciler(
        provider,
        registry,
        controller_id="controller-own",
        on_bound_lost=callback,
    )

    result = await reconciler.reconcile()

    assert result.ghosts_removed == []
    assert result.bound_nodes_lost == []
    callback.assert_not_awaited()
    assert await registry.get("aws:i-foreign") is not None
