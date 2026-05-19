"""CloudReconciler — startup + periodic scan to detect orphan/ghost instances."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone

from elastic_agent.core.providers.base import CloudProvider, Instance
from elastic_agent.core.registry import NodeRecord, NodeRegistry, NodeStatus

logger = logging.getLogger(__name__)


@dataclass
class ReconcileResult:
    orphans_adopted: list[str] = field(default_factory=list)
    ghosts_removed: list[str] = field(default_factory=list)
    state_conflicts_resolved: list[str] = field(default_factory=list)
    cloud_instance_count: int = 0
    registry_node_count: int = 0
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


_CLOUD_STATE_TO_NODE_STATUS = {
    "running": NodeStatus.READY,
    "pending": NodeStatus.CREATING,
    "starting": NodeStatus.CREATING,
    "stopping": NodeStatus.DRAINING,
    "stopped": NodeStatus.TERMINATED,
    "terminated": NodeStatus.TERMINATED,
}


class CloudReconciler:
    """Reconciles cloud provider state with local NodeRegistry.

    Detects three categories:
    - Orphans: instances in cloud but not in registry (adopt into registry)
    - Ghosts: nodes in registry but not in cloud (remove from registry)
    - State conflicts: both exist but status disagrees (cloud wins)
    """

    def __init__(
        self,
        provider: CloudProvider,
        registry: NodeRegistry,
        reconcile_interval: int = 300,
    ) -> None:
        self._provider = provider
        self._registry = registry
        self._reconcile_interval = reconcile_interval
        self._task: asyncio.Task | None = None
        self._running = False

    async def reconcile(self) -> ReconcileResult:
        result = ReconcileResult()

        cloud_instances = await self._provider.list_instances(
            filters={CloudProvider.MANAGED_TAG_KEY: CloudProvider.MANAGED_TAG_VALUE}
        )
        cloud_map: dict[str, Instance] = {inst.instance_id: inst for inst in cloud_instances}
        result.cloud_instance_count = len(cloud_map)

        registered_ids = await self._registry.list_all_ids()
        result.registry_node_count = len(registered_ids)

        cloud_ids = set(cloud_map.keys())

        orphan_ids = cloud_ids - registered_ids
        for oid in orphan_ids:
            inst = cloud_map[oid]
            status = _CLOUD_STATE_TO_NODE_STATUS.get(inst.state.value, NodeStatus.READY)
            record = NodeRecord(
                node_id=oid,
                instance_id=inst.native_id,
                platform=inst.platform,
                status=status,
                public_ip=inst.public_ip,
                private_ip=inst.private_ip,
                created_at=inst.created_at or datetime.now(timezone.utc),
            )
            await self._registry.add(record)
            result.orphans_adopted.append(oid)
            logger.warning("Reconciler: adopted orphan instance %s (cloud state=%s)", oid, inst.state)

        ghost_ids = registered_ids - cloud_ids
        for gid in ghost_ids:
            rec = await self._registry.get(gid)
            if rec and rec.status == NodeStatus.TERMINATED:
                await self._registry.remove(gid)
                result.ghosts_removed.append(gid)
                logger.info("Reconciler: removed terminated ghost %s", gid)
                continue
            await self._registry.update(gid, status=NodeStatus.TERMINATED)
            await self._registry.remove(gid)
            result.ghosts_removed.append(gid)
            logger.warning("Reconciler: removed ghost node %s (not found in cloud)", gid)

        both = cloud_ids & registered_ids
        for nid in both:
            inst = cloud_map[nid]
            rec = await self._registry.get(nid)
            if rec is None:
                continue

            if inst.state.value == "terminated":
                if rec.status != NodeStatus.TERMINATED:
                    await self._registry.update(nid, status=NodeStatus.TERMINATED)
                    result.state_conflicts_resolved.append(nid)
                    logger.warning("Reconciler: cloud says %s is terminated, updated registry", nid)
            elif inst.state.value == "stopped":
                if rec.status not in (NodeStatus.TERMINATED, NodeStatus.DRAINING):
                    await self._registry.update(nid, status=NodeStatus.TERMINATED)
                    result.state_conflicts_resolved.append(nid)
                    logger.warning("Reconciler: cloud says %s is stopped, marked terminated", nid)
            else:
                if inst.public_ip and inst.public_ip != rec.public_ip:
                    await self._registry.update(nid, public_ip=inst.public_ip)
                if inst.private_ip and inst.private_ip != rec.private_ip:
                    await self._registry.update(nid, private_ip=inst.private_ip)

        logger.info(
            "Reconcile complete: cloud=%d, registry=%d, orphans=%d, ghosts=%d, conflicts=%d",
            result.cloud_instance_count,
            result.registry_node_count,
            len(result.orphans_adopted),
            len(result.ghosts_removed),
            len(result.state_conflicts_resolved),
        )
        return result

    async def start_periodic(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._periodic_loop())
        logger.info("Reconciler: periodic loop started (interval=%ds)", self._reconcile_interval)

    async def stop_periodic(self) -> None:
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        logger.info("Reconciler: periodic loop stopped")

    async def _periodic_loop(self) -> None:
        while self._running:
            try:
                await asyncio.sleep(self._reconcile_interval)
                await self.reconcile()
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("Reconciler: periodic reconcile failed")
