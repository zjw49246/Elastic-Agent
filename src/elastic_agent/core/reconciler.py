"""CloudReconciler — startup + periodic scan to detect orphan/ghost instances."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone

from elastic_agent.core.providers.base import (
    CloudProvider,
    Instance,
    InstanceNotFoundError,
)
from elastic_agent.core.registry import NodeRecord, NodeRegistry, NodeStatus

logger = logging.getLogger(__name__)


@dataclass
class ReconcileResult:
    orphans_adopted: list[str] = field(default_factory=list)
    ghosts_removed: list[str] = field(default_factory=list)
    bound_nodes_lost: list[str] = field(default_factory=list)
    state_conflicts_resolved: list[str] = field(default_factory=list)
    cloud_instance_count: int = 0
    registry_node_count: int = 0
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


_CLOUD_STATE_TO_NODE_STATUS = {
    "running": NodeStatus.READY,
    "pending": NodeStatus.CREATING,
    "starting": NodeStatus.CREATING,
    "stopping": NodeStatus.DRAINING,
    "stopped": NodeStatus.STOPPED,
    "terminated": NodeStatus.TERMINATED,
}

_LIFECYCLE_TAG_TO_METADATA = {
    "ElasticAgentJob": "job_id",
    "ElasticAgentAccount": "account_id",
    "ElasticAgentLease": "lease_id",
    "ElasticAgentController": "controller_id",
}

BoundNodeLostCallback = Callable[[str, str], Awaitable[None]]
_DEFINITIVE_MISSING_SCANS = 3


def _is_definitive_not_found(exc: Exception) -> bool:
    if isinstance(exc, (KeyError, InstanceNotFoundError)):
        return True
    response = getattr(exc, "response", None)
    if isinstance(response, dict):
        code = str(response.get("Error", {}).get("Code", ""))
        return code in {"InvalidInstanceID.NotFound", "InvalidInstance.NotFound"}
    return False


class CloudReconciler:
    """Reconciles cloud provider state with local NodeRegistry.

    Detects three categories:
    - Orphans: instances in cloud but not in registry (adopt into registry)
    - Ghosts: nodes in registry but not in cloud (remove from registry)
    - State conflicts: both exist but status disagrees (cloud wins)

    Nodes carrying a durable EIP ``lease_id`` are not ordinary ghosts. They
    remain in the registry and are handed to ``on_bound_lost`` so the Manager
    can run the lease's detach/terminate cleanup before removing control-plane
    state. A failed callback is deliberately non-fatal to the scan; retaining
    the record makes the cleanup retryable on the next reconciliation pass.
    """

    def __init__(
        self,
        provider: CloudProvider,
        registry: NodeRegistry,
        reconcile_interval: int = 300,
        on_bound_lost: BoundNodeLostCallback | None = None,
        controller_id: str = "",
    ) -> None:
        self._provider = provider
        self._registry = registry
        self._reconcile_interval = reconcile_interval
        self._on_bound_lost = on_bound_lost
        self._controller_id = controller_id
        self._task: asyncio.Task | None = None
        self._running = False
        self._stop_event = asyncio.Event()
        self._missing_counts: dict[str, int] = {}
        self._bound_cleanup_pending: set[str] = set()

    def set_controller_id(self, controller_id: str) -> None:
        """Set the durable cloud ownership scope before periodic scans start."""
        if self._running:
            raise RuntimeError("cannot change reconciler controller while running")
        self._controller_id = controller_id

    async def _handle_bound_lost(
        self,
        record: NodeRecord,
        result: ReconcileResult,
        *,
        reason: str,
        mark_terminated: bool = True,
    ) -> None:
        """Mark a leased node lost and delegate its durable cleanup."""
        lease_id = str(record.metadata.get("lease_id") or "")
        if mark_terminated and record.status != NodeStatus.TERMINATED:
            await self._registry.update(record.node_id, status=NodeStatus.TERMINATED)
            result.state_conflicts_resolved.append(record.node_id)

        result.bound_nodes_lost.append(record.node_id)
        if self._on_bound_lost is None:
            self._bound_cleanup_pending.add(record.node_id)
            logger.error(
                "Reconciler: bound node %s was lost (%s), but no cleanup callback "
                "is configured; retaining lease %s for retry",
                record.node_id,
                reason,
                lease_id,
            )
            return

        try:
            await self._on_bound_lost(record.node_id, lease_id)
        except Exception:
            self._bound_cleanup_pending.add(record.node_id)
            logger.exception(
                "Reconciler: bound-node cleanup failed for %s lease %s (%s); "
                "retaining registry record for retry",
                record.node_id,
                lease_id,
                reason,
            )
        else:
            self._bound_cleanup_pending.discard(record.node_id)

    async def reconcile(self) -> ReconcileResult:
        result = ReconcileResult()

        filters = {
            CloudProvider.MANAGED_TAG_KEY: CloudProvider.MANAGED_TAG_VALUE
        }
        if self._controller_id:
            filters["ElasticAgentController"] = self._controller_id
        cloud_instances = await self._provider.list_instances(filters=filters)
        # Cloud APIs and test/provider implementations are not a security
        # boundary: defensively enforce the base ownership tag even if the
        # provider ignored or partially applied the requested filters.
        cloud_instances = [
            instance
            for instance in cloud_instances
            if instance.tags.get(CloudProvider.MANAGED_TAG_KEY)
            == CloudProvider.MANAGED_TAG_VALUE
        ]
        if self._controller_id:
            # Providers/mocks are not all guaranteed to enforce tag filters;
            # keep the destructive boundary defensive in the reconciler too.
            cloud_instances = [
                instance
                for instance in cloud_instances
                if instance.tags.get("ElasticAgentController")
                == self._controller_id
            ]
        cloud_map: dict[str, Instance] = {inst.instance_id: inst for inst in cloud_instances}

        registered_ids = await self._registry.list_all_ids()
        result.registry_node_count = len(registered_ids)

        if self._controller_id:
            # Upgrade compatibility: pre-controller ordinary fleet instances
            # are absent from the strict tag query.  Verify only the exact ids
            # already present in our durable registry and retain unscoped
            # ManagedBy instances.  Never enumerate/adopt arbitrary untagged
            # cloud resources, and never accept another controller's tag.
            unresolved_ids: set[str] = set()

            async def lookup_legacy(node_id: str) -> Instance | None:
                record = await self._registry.get(node_id)
                if record is None:
                    return None
                try:
                    candidate = await self._provider.get_instance(record.instance_id)
                except Exception as exc:  # noqa: BLE001
                    if _is_definitive_not_found(exc):
                        count = self._missing_counts.get(node_id, 0) + 1
                        self._missing_counts[node_id] = count
                        if count < _DEFINITIVE_MISSING_SCANS:
                            unresolved_ids.add(node_id)
                    else:
                        # Throttling/network/auth failures are UNKNOWN, never
                        # evidence that a billable instance disappeared.
                        unresolved_ids.add(node_id)
                    return None
                if not isinstance(candidate, Instance):
                    unresolved_ids.add(node_id)
                    return None
                if (
                    candidate.tags.get(CloudProvider.MANAGED_TAG_KEY)
                    != CloudProvider.MANAGED_TAG_VALUE
                ):
                    unresolved_ids.add(node_id)
                    return None
                owner = candidate.tags.get("ElasticAgentController", "")
                if owner:
                    if owner == self._controller_id:
                        self._missing_counts.pop(node_id, None)
                        return candidate
                    unresolved_ids.add(node_id)
                    return None
                self._missing_counts.pop(node_id, None)
                return candidate

            legacy = await asyncio.gather(*(
                lookup_legacy(node_id)
                for node_id in registered_ids - set(cloud_map)
            ))
            for instance in legacy:
                if instance is not None:
                    cloud_map[instance.instance_id] = instance
        else:
            unresolved_ids = set()

        for visible_id in cloud_map:
            self._missing_counts.pop(visible_id, None)

        result.cloud_instance_count = len(cloud_map)

        cloud_ids = set(cloud_map.keys())

        orphan_ids = cloud_ids - registered_ids
        for oid in orphan_ids:
            inst = cloud_map[oid]
            status = _CLOUD_STATE_TO_NODE_STATUS.get(inst.state.value, NodeStatus.READY)
            metadata = {
                metadata_key: value
                for tag_key, metadata_key in _LIFECYCLE_TAG_TO_METADATA.items()
                if (value := inst.tags.get(tag_key))
            }
            record = NodeRecord(
                node_id=oid,
                instance_id=inst.instance_id,
                platform=inst.platform,
                status=status,
                public_ip=inst.public_ip,
                private_ip=inst.private_ip,
                created_at=inst.created_at or datetime.now(timezone.utc),
                metadata=metadata,
            )
            await self._registry.add(record)
            result.orphans_adopted.append(oid)
            logger.warning("Reconciler: adopted orphan instance %s (cloud state=%s)", oid, inst.state)
            if metadata.get("lease_id"):
                await self._handle_bound_lost(
                    record,
                    result,
                    reason=(
                        "adopted already terminated"
                        if status == NodeStatus.TERMINATED
                        else "adopted tagged orphan"
                    ),
                    mark_terminated=status == NodeStatus.TERMINATED,
                )

        ghost_ids = registered_ids - cloud_ids - unresolved_ids
        for gid in ghost_ids:
            rec = await self._registry.get(gid)
            if rec and rec.metadata.get("lease_id"):
                await self._handle_bound_lost(rec, result, reason="not found in cloud")
                continue
            if rec and rec.status == NodeStatus.TERMINATED:
                await self._registry.remove(gid)
                self._missing_counts.pop(gid, None)
                result.ghosts_removed.append(gid)
                logger.info("Reconciler: removed terminated ghost %s", gid)
                continue
            await self._registry.update(gid, status=NodeStatus.TERMINATED)
            await self._registry.remove(gid)
            self._missing_counts.pop(gid, None)
            result.ghosts_removed.append(gid)
            logger.warning("Reconciler: removed ghost node %s (not found in cloud)", gid)

        both = cloud_ids & registered_ids
        for nid in both:
            inst = cloud_map[nid]
            rec = await self._registry.get(nid)
            if rec is None:
                continue

            if nid in self._bound_cleanup_pending and rec.metadata.get("lease_id"):
                await self._handle_bound_lost(
                    rec,
                    result,
                    reason="retrying bound cleanup",
                    mark_terminated=inst.state.value == "terminated",
                )
                continue

            if inst.state.value == "terminated":
                if rec.metadata.get("lease_id"):
                    await self._handle_bound_lost(
                        rec, result, reason="terminated in cloud"
                    )
                    continue
                if rec.status != NodeStatus.TERMINATED:
                    await self._registry.update(nid, status=NodeStatus.TERMINATED)
                    result.state_conflicts_resolved.append(nid)
                    logger.warning("Reconciler: cloud says %s is terminated, updated registry", nid)
            elif inst.state.value == "stopped":
                if rec.status not in (NodeStatus.STOPPED, NodeStatus.DRAINING):
                    await self._registry.update(nid, status=NodeStatus.STOPPED)
                    result.state_conflicts_resolved.append(nid)
                    logger.warning("Reconciler: cloud says %s is stopped, marked stopped", nid)
            else:
                if inst.public_ip and inst.public_ip != rec.public_ip:
                    await self._registry.update(nid, public_ip=inst.public_ip)
                if inst.private_ip and inst.private_ip != rec.private_ip:
                    await self._registry.update(nid, private_ip=inst.private_ip)

        logger.info(
            "Reconcile complete: cloud=%d, registry=%d, orphans=%d, ghosts=%d, "
            "bound_lost=%d, conflicts=%d",
            result.cloud_instance_count,
            result.registry_node_count,
            len(result.orphans_adopted),
            len(result.ghosts_removed),
            len(result.bound_nodes_lost),
            len(result.state_conflicts_resolved),
        )
        return result

    async def start_periodic(self) -> None:
        if self._running:
            return
        self._running = True
        self._stop_event.clear()
        self._task = asyncio.create_task(self._periodic_loop())
        logger.info("Reconciler: periodic loop started (interval=%ds)", self._reconcile_interval)

    async def stop_periodic(self) -> None:
        self._running = False
        self._stop_event.set()
        if self._task and not self._task.done():
            # Reconcile may be inside an uncancellable boto3 `to_thread` call or
            # a lease teardown callback.  Wake idle sleep, but never cancel an
            # in-flight cloud transaction and unlock underneath it.
            await self._task
        self._task = None
        logger.info("Reconciler: periodic loop stopped")

    async def _periodic_loop(self) -> None:
        while self._running:
            try:
                try:
                    await asyncio.wait_for(
                        self._stop_event.wait(),
                        timeout=self._reconcile_interval,
                    )
                    break
                except asyncio.TimeoutError:
                    pass
                if not self._running:
                    break
                await self.reconcile()
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("Reconciler: periodic reconcile failed")
