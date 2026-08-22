"""TaskScheduler — capacity-aware task distribution across Workers.

T-051: Finds Workers with available capacity using WorkerCapacity checks.
Selects the least-busy Worker when multiple candidates are available.
Returns None when no Worker has free capacity.
"""

from __future__ import annotations

import logging
from typing import Any

from elastic_agent.core.registry import NodeRegistry, NodeStatus
from elastic_agent.core.task_registry import TaskRegistry
from elastic_agent.harness.base import Harness, WorkerCapacity

logger = logging.getLogger(__name__)


class TaskScheduler:
    """Capacity-aware task scheduler.

    Iterates READY workers, compares active task counts against
    WorkerCapacity.max_concurrent_tasks, and returns the least-busy
    worker. Returns None if no capacity is available.
    """

    def __init__(
        self,
        node_registry: NodeRegistry,
        task_registry: TaskRegistry,
        harness: Harness | None = None,
    ) -> None:
        self._node_registry = node_registry
        self._task_registry = task_registry
        self._harness = harness

    def _get_capacity(self) -> WorkerCapacity:
        if self._harness is not None:
            return self._harness.get_worker_capacity()
        return WorkerCapacity(max_concurrent_tasks=1)

    async def find_available_worker(
        self,
        capacity_requirement: dict[str, Any] | None = None,
    ) -> str | None:
        """Find a READY worker with free capacity.

        Args:
            capacity_requirement: Optional dict of additional capacity constraints
                (e.g. {"task_type": "production"} for Harness-specific scheduling).
                Passed through to allow Harness subclasses to implement custom logic.

        Returns:
            worker_id of the least-busy available Worker, or None.
        """
        capacity = self._get_capacity()
        max_tasks = capacity.max_concurrent_tasks

        ready_workers = await self._node_registry.list_by_status(NodeStatus.READY)
        if not ready_workers:
            logger.debug("TaskScheduler: no READY workers available")
            return None

        best_worker_id: str | None = None
        best_active_count: int | None = None

        for node in ready_workers:
            active_count = await self._task_registry.count_active_by_worker(node.node_id)
            if active_count >= max_tasks:
                continue
            if best_active_count is None or active_count < best_active_count:
                best_worker_id = node.node_id
                best_active_count = active_count

        if best_worker_id is not None:
            logger.debug(
                "TaskScheduler: selected worker %s (active_tasks=%d, max=%d)",
                best_worker_id,
                best_active_count,
                max_tasks,
            )
        else:
            logger.debug("TaskScheduler: all workers at capacity (max=%d)", max_tasks)

        return best_worker_id
