"""Worker multi-layer health check — L1 VM + L2 Runtime + L3 Agent process.

T-024: Periodic health checking with three layers:
- L1: VM reachability (heartbeat-based)
- L2: Worker Runtime WebSocket connectivity and responsiveness
- L3: Agent process health (via Runtime health check command)
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from elastic_agent.core.event_bus import EventBus
from elastic_agent.core.registry import NodeRecord, NodeRegistry, NodeStatus
from elastic_agent.manager.connection import WorkerConnectionManager

logger = logging.getLogger(__name__)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class HealthLevel(str, Enum):
    L1_VM = "l1_vm"
    L2_RUNTIME = "l2_runtime"
    L3_AGENT = "l3_agent"


class HealthStatus(str, Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


class NodeHealthReport:
    __slots__ = ("node_id", "l1", "l2", "l3", "overall", "details", "checked_at")

    def __init__(
        self,
        node_id: str,
        l1: HealthStatus = HealthStatus.UNKNOWN,
        l2: HealthStatus = HealthStatus.UNKNOWN,
        l3: HealthStatus = HealthStatus.UNKNOWN,
        details: dict[str, Any] | None = None,
    ) -> None:
        self.node_id = node_id
        self.l1 = l1
        self.l2 = l2
        self.l3 = l3
        self.details = details or {}
        self.checked_at = _utcnow()

        if l1 == HealthStatus.UNHEALTHY or l2 == HealthStatus.UNHEALTHY:
            self.overall = HealthStatus.UNHEALTHY
        elif l3 == HealthStatus.UNHEALTHY:
            self.overall = HealthStatus.DEGRADED
        elif any(s == HealthStatus.UNKNOWN for s in (l1, l2, l3)):
            self.overall = HealthStatus.UNKNOWN
        else:
            self.overall = HealthStatus.HEALTHY

    def to_dict(self) -> dict[str, Any]:
        return {
            "node_id": self.node_id,
            "l1_vm": self.l1.value,
            "l2_runtime": self.l2.value,
            "l3_agent": self.l3.value,
            "overall": self.overall.value,
            "details": self.details,
            "checked_at": self.checked_at.isoformat(),
        }


class HealthChecker:
    """Multi-layer health checker that monitors Worker nodes periodically.

    L1 (VM): Based on heartbeat recency — if last heartbeat was more than
    heartbeat_interval * unhealthy_threshold ago, VM is considered unhealthy.

    L2 (Runtime): Based on WebSocket connectivity — checks if the Worker
    is currently connected.

    L3 (Agent): Sends a HEALTH_CHECK message and waits for response with
    CPU/memory/disk stats.
    """

    def __init__(
        self,
        registry: NodeRegistry,
        connection_manager: WorkerConnectionManager,
        event_bus: EventBus,
        check_interval: int = 30,
        heartbeat_interval: int = 30,
        unhealthy_threshold: int = 3,
    ) -> None:
        self._registry = registry
        self._connection_manager = connection_manager
        self._event_bus = event_bus
        self._check_interval = check_interval
        self._heartbeat_interval = heartbeat_interval
        self._unhealthy_threshold = unhealthy_threshold
        self._consecutive_failures: dict[str, int] = {}
        self._task: asyncio.Task | None = None
        self._running = False

    async def check_node(self, node_id: str) -> NodeHealthReport:
        """Run a full 3-layer health check on a single node."""
        node = await self._registry.get(node_id)
        if node is None:
            return NodeHealthReport(node_id=node_id)

        l1 = self._check_l1(node)
        l2 = self._check_l2(node_id)
        l3 = await self._check_l3(node_id)

        report = NodeHealthReport(
            node_id=node_id,
            l1=l1,
            l2=l2,
            l3=l3,
            details={
                "last_heartbeat": node.last_heartbeat.isoformat() if node.last_heartbeat else None,
                "ws_connected": self._connection_manager.is_connected(node_id),
            },
        )

        await self._process_report(node_id, report)
        return report

    def _check_l1(self, node: NodeRecord) -> HealthStatus:
        """L1: VM reachability based on heartbeat recency."""
        if node.last_heartbeat is None:
            if node.status in (NodeStatus.CREATING, NodeStatus.BOOTSTRAPPING):
                return HealthStatus.UNKNOWN
            return HealthStatus.UNHEALTHY

        elapsed = (_utcnow() - node.last_heartbeat).total_seconds()
        max_gap = self._heartbeat_interval * self._unhealthy_threshold
        if elapsed > max_gap:
            return HealthStatus.UNHEALTHY
        return HealthStatus.HEALTHY

    def _check_l2(self, node_id: str) -> HealthStatus:
        """L2: Worker Runtime WebSocket connectivity."""
        if self._connection_manager.is_connected(node_id):
            return HealthStatus.HEALTHY
        return HealthStatus.UNHEALTHY

    async def _check_l3(self, node_id: str) -> HealthStatus:
        """L3: Agent process health via HEALTH_CHECK message."""
        if not self._connection_manager.is_connected(node_id):
            return HealthStatus.UNKNOWN

        try:
            await self._connection_manager.health_check(node_id)
            return HealthStatus.HEALTHY
        except Exception:
            return HealthStatus.UNHEALTHY

    async def _process_report(self, node_id: str, report: NodeHealthReport) -> None:
        """Update consecutive failure count and emit events."""
        if report.overall == HealthStatus.UNHEALTHY:
            count = self._consecutive_failures.get(node_id, 0) + 1
            self._consecutive_failures[node_id] = count

            if count >= self._unhealthy_threshold:
                await self._registry.update(node_id, status=NodeStatus.UNHEALTHY)
                await self._event_bus.emit("WORKER_UNHEALTHY", node_id, {
                    "consecutive_failures": count,
                    "report": report.to_dict(),
                })
                logger.warning(
                    "Node %s marked UNHEALTHY after %d consecutive failures",
                    node_id, count,
                )
        else:
            if node_id in self._consecutive_failures:
                del self._consecutive_failures[node_id]

    async def check_all(self) -> list[NodeHealthReport]:
        """Run health checks on all active nodes."""
        active_statuses = {NodeStatus.READY, NodeStatus.BUSY, NodeStatus.DRAINING}
        reports: list[NodeHealthReport] = []

        for status in active_statuses:
            nodes = await self._registry.list_by_status(status)
            for node in nodes:
                report = await self.check_node(node.node_id)
                reports.append(report)

        return reports

    async def start_periodic(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._periodic_loop())
        logger.info("HealthChecker started (interval=%ds)", self._check_interval)

    async def stop_periodic(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        logger.info("HealthChecker stopped")

    async def _periodic_loop(self) -> None:
        while self._running:
            try:
                await self.check_all()
            except Exception:
                logger.exception("Health check cycle failed")
            await asyncio.sleep(self._check_interval)
