"""Graceful drain — wait for tasks to complete, then terminate.

T-025: Implements the drain lifecycle:
1. Mark node as DRAINING
2. Wait for all active tasks to complete (with timeout)
3. Disconnect worker and terminate cloud instance
4. Mark node as TERMINATED

Supports:
- Per-node drain with configurable timeout
- Timeout-based force termination
- Credential recovery before termination
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Callable, Awaitable

from elastic_agent.core.event_bus import EventBus
from elastic_agent.core.registry import NodeRegistry, NodeStatus
from elastic_agent.core.task_registry import TaskRegistry, TaskStatus
from elastic_agent.core.providers.base import CloudProvider
from elastic_agent.manager.connection import WorkerConnectionManager

logger = logging.getLogger(__name__)


class DrainManager:
    """Manages graceful node drain and termination.

    Drain flow:
        drain_node(node_id) →
            mark DRAINING →
            wait for tasks to complete (or timeout) →
            send stop signals to remaining processes →
            disconnect worker →
            recover credentials →
            terminate cloud instance →
            mark TERMINATED
    """

    def __init__(
        self,
        registry: NodeRegistry,
        task_registry: TaskRegistry,
        connection_manager: WorkerConnectionManager,
        provider: CloudProvider,
        event_bus: EventBus,
        drain_timeout: int = 3600,
        on_recover_credentials: Callable[[str], Awaitable[None]] | None = None,
    ) -> None:
        self._registry = registry
        self._task_registry = task_registry
        self._connection_manager = connection_manager
        self._provider = provider
        self._event_bus = event_bus
        self._drain_timeout = drain_timeout
        self._on_recover_credentials = on_recover_credentials
        self._active_drains: dict[str, asyncio.Task] = {}

    async def drain_node(self, node_id: str, timeout: int | None = None) -> bool:
        """Start draining a node. Returns False if node doesn't exist."""
        node = await self._registry.get(node_id)
        if node is None:
            return False

        if node_id in self._active_drains:
            logger.info("Node %s is already draining", node_id)
            return True

        await self._registry.update(node_id, status=NodeStatus.DRAINING)
        await self._event_bus.emit("NODE_DRAINING", node_id, {
            "timeout": timeout or self._drain_timeout,
        })

        task = asyncio.create_task(
            self._drain_loop(node_id, timeout or self._drain_timeout)
        )
        self._active_drains[node_id] = task
        task.add_done_callback(lambda _: self._active_drains.pop(node_id, None))

        logger.info("Started drain for node %s (timeout=%ds)", node_id, timeout or self._drain_timeout)
        return True

    async def _drain_loop(self, node_id: str, timeout: int) -> None:
        """Wait for tasks to complete, then terminate."""
        try:
            completed = await asyncio.wait_for(
                self._wait_tasks_complete(node_id),
                timeout=timeout,
            )
        except asyncio.TimeoutError:
            logger.warning("Drain timeout for node %s, force terminating", node_id)
            await self._force_stop_tasks(node_id)
            await asyncio.sleep(5)

        await self._terminate_node(node_id)

    async def _wait_tasks_complete(self, node_id: str) -> None:
        """Poll until all tasks on the node are done."""
        while True:
            tasks = await self._task_registry.list_by_worker(node_id)
            active = [
                t for t in tasks
                if t.status in (TaskStatus.RUNNING,)
            ]
            if not active:
                return
            await asyncio.sleep(5)

    async def _force_stop_tasks(self, node_id: str) -> None:
        """Send SIGINT to all active tasks on the node."""
        tasks = await self._task_registry.list_by_worker(node_id)
        for task in tasks:
            if task.status == TaskStatus.RUNNING:
                try:
                    await self._connection_manager.stop_process(
                        node_id, task.task_id, "SIGINT"
                    )
                except Exception:
                    logger.warning("Failed to stop task %s on node %s", task.task_id, node_id)

    async def _terminate_node(self, node_id: str) -> None:
        """Disconnect, recover credentials, and terminate the instance."""
        if self._on_recover_credentials:
            try:
                await self._on_recover_credentials(node_id)
            except Exception:
                logger.exception("Failed to recover credentials for node %s", node_id)

        await self._connection_manager.disconnect_worker(node_id)

        node = await self._registry.get(node_id)
        if node is not None:
            try:
                await self._provider.terminate_instance(node.instance_id)
            except Exception:
                logger.exception("Failed to terminate instance for node %s", node_id)

        await self._registry.update(node_id, status=NodeStatus.TERMINATED)
        await self._event_bus.emit("NODE_TERMINATED", node_id, {
            "reason": "drain_completed",
        })
        logger.info("Node %s terminated after drain", node_id)

    async def cancel_drain(self, node_id: str) -> bool:
        """Cancel an active drain. Returns False if not draining."""
        task = self._active_drains.pop(node_id, None)
        if task is None:
            return False
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        await self._registry.update(node_id, status=NodeStatus.READY)
        logger.info("Drain cancelled for node %s", node_id)
        return True

    async def stop(self) -> None:
        """Cancel all active drains."""
        for node_id in list(self._active_drains):
            await self.cancel_drain(node_id)

    @property
    def draining_nodes(self) -> list[str]:
        return list(self._active_drains.keys())
