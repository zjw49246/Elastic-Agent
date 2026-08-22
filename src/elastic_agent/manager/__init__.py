"""Manager-side components for Elastic-Agent framework."""

from elastic_agent.manager.connection import WorkerConnectionManager, WorkerNotConnectedError
from elastic_agent.manager.manager import ElasticAgentManager

__all__ = ["ElasticAgentManager", "WorkerConnectionManager", "WorkerNotConnectedError"]
