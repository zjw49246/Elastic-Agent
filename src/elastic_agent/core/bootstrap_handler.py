"""Bootstrap failure handling — orchestrates retry strategies at the Manager level.

T-023: Implements the three failure strategies:
- TERMINATE_AND_RETRY: terminate instance, create new one, re-bootstrap
- RETRY_FROM_FAILED: retry from the failed step on the same instance
- LEAVE_FOR_DEBUG: mark FAILED and leave instance for manual inspection
"""

from __future__ import annotations

import logging
from typing import Any, Callable, Awaitable

from elastic_agent.core.bootstrap import (
    BootstrapPipeline,
    BootstrapResult,
    FailureStrategy,
    SSHExecutor,
)
from elastic_agent.core.config import BootstrapConfig
from elastic_agent.core.registry import NodeRegistry, NodeStatus
from elastic_agent.core.event_bus import EventBus
from elastic_agent.harness.base import BootstrapStep, Harness

logger = logging.getLogger(__name__)


class BootstrapHandler:
    """Manages the full bootstrap lifecycle for a node, including failure strategies.

    Wraps BootstrapPipeline with Manager-level orchestration:
    - Updates NodeRegistry status during bootstrap
    - Emits events via EventBus
    - Applies failure strategy when steps fail
    - Supports credential recovery callbacks
    """

    def __init__(
        self,
        config: BootstrapConfig,
        registry: NodeRegistry,
        event_bus: EventBus,
        harness: Harness | None = None,
        on_terminate_and_replace: Callable[[str], Awaitable[str | None]] | None = None,
        on_recover_credentials: Callable[[str], Awaitable[None]] | None = None,
    ) -> None:
        self._config = config
        self._registry = registry
        self._event_bus = event_bus
        self._harness = harness
        self._on_terminate_and_replace = on_terminate_and_replace
        self._on_recover_credentials = on_recover_credentials
        self._pipeline = BootstrapPipeline(config)
        self._failure_strategy = FailureStrategy(config.failure_strategy)

    async def bootstrap_node(
        self,
        node_id: str,
        host: str,
        steps: list[BootstrapStep],
        ssh_user: str = "root",
        ssh_key_path: str | None = None,
    ) -> BootstrapResult:
        """Run the full bootstrap pipeline with failure handling."""
        await self._registry.update(node_id, status=NodeStatus.BOOTSTRAPPING)
        await self._event_bus.emit("BOOTSTRAP_STARTED", node_id, {
            "steps": [s.name for s in steps],
            "strategy": self._failure_strategy.value,
        })

        executor = SSHExecutor(
            host=host,
            user=ssh_user,
            key_path=ssh_key_path,
        )

        result = await self._pipeline.execute(
            node_id=node_id,
            steps=steps,
            executor=executor,
            on_step_start=self._on_step_start,
            on_step_complete=self._on_step_complete,
        )

        if result.success:
            await self._registry.update(node_id, status=NodeStatus.READY)
            await self._event_bus.emit("BOOTSTRAP_COMPLETED", node_id, {
                "duration_seconds": result.total_duration_seconds,
                "attempts": result.attempts_used,
            })
            logger.info("Bootstrap succeeded for node %s", node_id)
            return result

        return await self._handle_failure(node_id, host, steps, result, executor)

    async def _handle_failure(
        self,
        node_id: str,
        host: str,
        steps: list[BootstrapStep],
        result: BootstrapResult,
        executor: SSHExecutor,
    ) -> BootstrapResult:
        """Apply the configured failure strategy."""
        logger.warning(
            "Bootstrap failed for node %s (strategy=%s, error=%s)",
            node_id, self._failure_strategy.value, result.error,
        )

        if self._on_recover_credentials:
            await self._on_recover_credentials(node_id)

        if self._failure_strategy == FailureStrategy.LEAVE_FOR_DEBUG:
            await self._registry.update(node_id, status=NodeStatus.FAILED)
            await self._event_bus.emit("BOOTSTRAP_FAILED", node_id, {
                "strategy": "leave_for_debug",
                "error": result.error,
            })
            return result

        if self._failure_strategy == FailureStrategy.TERMINATE_AND_RETRY:
            await self._registry.update(node_id, status=NodeStatus.FAILED)
            await self._event_bus.emit("BOOTSTRAP_FAILED", node_id, {
                "strategy": "terminate_and_retry",
                "error": result.error,
                "will_retry": self._on_terminate_and_replace is not None,
            })

            if self._on_terminate_and_replace:
                new_node_id = await self._on_terminate_and_replace(node_id)
                if new_node_id:
                    logger.info(
                        "Replaced failed node %s with %s for retry", node_id, new_node_id
                    )
            return result

        if self._failure_strategy == FailureStrategy.RETRY_FROM_FAILED:
            failed_index = self._pipeline._find_resume_index(result.steps)
            logger.info(
                "Retrying bootstrap for node %s from step %d", node_id, failed_index
            )

            retry_result = await self._pipeline.execute(
                node_id=node_id,
                steps=steps,
                executor=executor,
                on_step_start=self._on_step_start,
                on_step_complete=self._on_step_complete,
            )

            if retry_result.success:
                await self._registry.update(node_id, status=NodeStatus.READY)
                await self._event_bus.emit("BOOTSTRAP_COMPLETED", node_id, {
                    "duration_seconds": retry_result.total_duration_seconds,
                    "retried_from_step": failed_index,
                })
                return retry_result

            await self._registry.update(node_id, status=NodeStatus.FAILED)
            await self._event_bus.emit("BOOTSTRAP_FAILED", node_id, {
                "strategy": "retry_from_failed",
                "error": retry_result.error,
            })
            return retry_result

        await self._registry.update(node_id, status=NodeStatus.FAILED)
        return result

    async def _on_step_start(self, step_name: str, index: int, total: int) -> None:
        logger.info("Bootstrap step %d/%d: %s", index + 1, total, step_name)

    async def _on_step_complete(self, step_name: str, step_result: Any) -> None:
        logger.info(
            "Bootstrap step %s: %s (%.1fs)",
            step_name, step_result.status.value, step_result.duration_seconds,
        )
