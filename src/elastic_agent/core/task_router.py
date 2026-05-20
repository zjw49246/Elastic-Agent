"""TaskRouter — route follow-up commands to the Worker running a task.

T-052: Looks up the task's worker in TaskRegistry, checks connectivity,
sends EXECUTE commands via WorkerConnectionManager, and auto-assembles
--resume commands using the stored session_id.
"""

from __future__ import annotations

import logging
import uuid
from typing import TYPE_CHECKING, Any

from elastic_agent.core.agent_type import AgentType
from elastic_agent.core.registry import NodeRegistry
from elastic_agent.core.task_registry import TaskRegistry, TaskStatus

if TYPE_CHECKING:
    from elastic_agent.manager.connection import WorkerConnectionManager

logger = logging.getLogger(__name__)


class TaskNotFoundError(Exception):
    pass


class WorkerOfflineError(Exception):
    pass


class TaskRouter:
    """Routes commands to the Worker where a task is running.

    Provides two main operations:
    - send_command: send an arbitrary command to a task's worker
    - send_followup: auto-assemble a --resume command using session_id
    """

    def __init__(
        self,
        task_registry: TaskRegistry,
        node_registry: NodeRegistry,
        connection_manager: WorkerConnectionManager,
        agent_type: AgentType | None = None,
    ) -> None:
        self._task_registry = task_registry
        self._node_registry = node_registry
        self._connection_manager = connection_manager
        self._agent_type = agent_type

    async def send_command(
        self,
        task_id: str,
        command: list[str],
        env: dict[str, str] | None = None,
        cwd: str = ".",
        timeout: int | None = None,
    ) -> str:
        """Send a command to the Worker running a task.

        Returns:
            process_task_id — a unique ID for tracking this execution.

        Raises:
            TaskNotFoundError: task_id not in registry.
            WorkerOfflineError: Worker is not connected.
        """
        record = await self._task_registry.get(task_id)
        if record is None:
            raise TaskNotFoundError(f"Task {task_id} not found in registry")

        worker_id = record.worker_id

        node = await self._node_registry.get(worker_id)
        if node is None or not self._connection_manager.is_connected(worker_id):
            raise WorkerOfflineError(f"Worker {worker_id} is not online")

        process_task_id = f"{task_id}:{uuid.uuid4().hex[:8]}"

        await self._connection_manager.execute(
            worker_id=worker_id,
            task_id=process_task_id,
            command=command,
            cwd=cwd,
            env=env or {},
            timeout=timeout,
        )

        logger.info(
            "TaskRouter: sent command to worker %s for task %s (process=%s)",
            worker_id,
            task_id,
            process_task_id,
        )
        return process_task_id

    async def send_followup(
        self,
        task_id: str,
        message: str,
        config_dir: str | None = None,
        cwd: str = ".",
        timeout: int | None = None,
    ) -> str:
        """Send a follow-up message, auto-assembling --resume if session_id is available.

        Uses the AgentType to build the appropriate command with --resume flag
        when a session_id exists for the task.

        Returns:
            process_task_id for tracking this execution.

        Raises:
            TaskNotFoundError: task_id not in registry.
            WorkerOfflineError: Worker is not connected.
            RuntimeError: No agent_type configured for resume support.
        """
        if self._agent_type is None:
            raise RuntimeError("TaskRouter requires an AgentType for send_followup")

        record = await self._task_registry.get(task_id)
        if record is None:
            raise TaskNotFoundError(f"Task {task_id} not found in registry")

        if record.session_id:
            command = self._agent_type.get_launch_command(
                prompt=message,
                session_id=record.session_id,
                config_dir=config_dir,
            )
        else:
            command = self._agent_type.get_launch_command(
                prompt=message,
                config_dir=config_dir,
            )

        env = self._agent_type.get_env_overrides(config_dir=config_dir)

        return await self.send_command(
            task_id=task_id,
            command=command,
            env=env,
            cwd=cwd,
            timeout=timeout,
        )
