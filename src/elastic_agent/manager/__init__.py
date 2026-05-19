"""Manager-side components for Elastic-Agent framework."""

from elastic_agent.manager.connection import WorkerConnectionManager, WorkerNotConnectedError

__all__ = ["WorkerConnectionManager", "WorkerNotConnectedError"]
