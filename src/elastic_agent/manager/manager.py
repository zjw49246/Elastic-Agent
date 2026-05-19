"""ElasticAgentManager — central orchestration hub.

T-016: Manager FastAPI skeleton — assembles registry, event bus, connection manager,
cloud provider, reconciler, and harness into a running FastAPI server.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from elastic_agent.core.agent_type import AgentType
from elastic_agent.core.config import ElasticAgentConfig
from elastic_agent.core.event_bus import EventBus
from elastic_agent.core.protocols.messages import Message
from elastic_agent.core.reconciler import CloudReconciler
from elastic_agent.core.registry import NodeRecord, NodeRegistry, NodeStatus
from elastic_agent.core.providers.base import CloudProvider
from elastic_agent.core.task_registry import TaskRegistry
from elastic_agent.core.task_router import TaskRouter
from elastic_agent.core.task_scheduler import TaskScheduler
from elastic_agent.core.webhook_emitter import WebhookEmitter
from elastic_agent.harness.base import Harness
from elastic_agent.manager.connection import WorkerConnectionManager
from elastic_agent.worker.file_sync import StorageBackend

logger = logging.getLogger(__name__)


class ElasticAgentManager:
    """Central coordinator that owns all subsystems.

    Lifecycle:
        manager = ElasticAgentManager(config, provider, harness)
        await manager.start()   # load registry, start reconciler
        ...
        await manager.stop()    # graceful shutdown
    """

    def __init__(
        self,
        config: ElasticAgentConfig,
        provider: CloudProvider,
        harness: Harness | None = None,
        agent_type: AgentType | None = None,
        file_storage: StorageBackend | None = None,
    ) -> None:
        self.config = config
        self.provider = provider
        self.harness = harness
        self.file_storage = file_storage

        self.registry = NodeRegistry(config.registry.path)
        self.event_bus = EventBus()
        self.connection_manager = WorkerConnectionManager(self.registry)
        self.reconciler = CloudReconciler(
            provider=provider,
            registry=self.registry,
            reconcile_interval=config.monitor.reconcile_interval,
        )

        self.task_registry = TaskRegistry(config.task_registry.path)
        self.task_scheduler = TaskScheduler(
            node_registry=self.registry,
            task_registry=self.task_registry,
            harness=harness,
        )
        self.task_router = TaskRouter(
            task_registry=self.task_registry,
            node_registry=self.registry,
            connection_manager=self.connection_manager,
            agent_type=agent_type,
        )
        self.webhook_emitter = WebhookEmitter(
            dead_letter_path=config.webhook.dead_letter_path,
            retry_delays=config.webhook.retry_delays,
            send_timeout=config.webhook.send_timeout,
        )

        self.connection_manager.on_message = self._on_worker_message
        self.connection_manager.on_connect = self._on_worker_connect
        self.connection_manager.on_disconnect = self._on_worker_disconnect

        self._started = False

    async def start(self) -> None:
        await self.registry.load()
        await self.task_registry.load()

        online_workers = set(self.connection_manager.connected_workers)
        await self.task_registry.recover(online_workers)

        await self.reconciler.start_periodic()
        self._started = True
        logger.info("ElasticAgentManager started")

    async def stop(self) -> None:
        await self.reconciler.stop_periodic()
        await self.webhook_emitter.stop()
        self._started = False
        logger.info("ElasticAgentManager stopped")

    # ------------------------------------------------------------------
    # Node operations
    # ------------------------------------------------------------------

    async def scale_out(
        self,
        count: int = 1,
        instance_type: str | None = None,
        region: str | None = None,
    ) -> list[NodeRecord]:
        from elastic_agent.core.auth import generate_worker_token
        from elastic_agent.core.providers.base import InstanceConfig

        provider_cfg = self.config.provider
        if provider_cfg.type == "aliyun":
            cfg = InstanceConfig(
                instance_type=instance_type or provider_cfg.aliyun.instance_type,
                image_id=provider_cfg.aliyun.image_id,
                key_pair_name=provider_cfg.aliyun.key_pair_name,
                security_group_ids=[provider_cfg.aliyun.security_group_id],
                subnet_id=provider_cfg.aliyun.vswitch_id,
                tags={"ManagedBy": "elastic-agent"},
            )
        else:
            cfg = InstanceConfig(
                instance_type=instance_type or provider_cfg.aws.default_instance_type,
                image_id=provider_cfg.aws.ami_id,
                key_pair_name=provider_cfg.aws.key_pair_name,
                security_group_ids=provider_cfg.aws.security_group_ids,
                subnet_id=provider_cfg.aws.subnet_id,
                tags={"ManagedBy": "elastic-agent"},
            )

        records: list[NodeRecord] = []
        for _ in range(count):
            instance = await self.provider.create_instance(cfg)
            token = generate_worker_token()
            record = NodeRecord(
                node_id=f"{instance.platform}:{instance.native_id}",
                instance_id=instance.instance_id,
                platform=instance.platform,
                status=NodeStatus.CREATING,
                public_ip=instance.public_ip,
                private_ip=instance.private_ip,
                auth_token=token,
            )
            await self.registry.add(record)
            await self.event_bus.emit("NODE_CREATING", record.node_id, {"instance_id": instance.instance_id})
            records.append(record)

        return records

    async def scale_in(self, node_ids: list[str], force: bool = False) -> list[str]:
        terminated: list[str] = []
        for nid in node_ids:
            node = await self.registry.get(nid)
            if node is None:
                continue
            if not force:
                await self.registry.update(nid, status=NodeStatus.DRAINING)
            else:
                try:
                    await self.provider.terminate_instance(node.instance_id)
                except Exception:
                    logger.exception("Failed to terminate instance %s", node.instance_id)
                await self.registry.update(nid, status=NodeStatus.TERMINATED)
                await self.connection_manager.disconnect_worker(nid)
                terminated.append(nid)
        return terminated

    async def drain_node(self, node_id: str) -> bool:
        node = await self.registry.get(node_id)
        if node is None:
            return False
        await self.registry.update(node_id, status=NodeStatus.DRAINING)
        return True

    async def remove_node(self, node_id: str) -> bool:
        """Remove a node from registry. Terminates instance if still running."""
        node = await self.registry.get(node_id)
        if node is None:
            return False
        if node.status not in (NodeStatus.TERMINATED, NodeStatus.FAILED):
            try:
                await self.provider.terminate_instance(node.instance_id)
            except Exception:
                logger.warning("Failed to terminate instance %s during removal", node.instance_id)
            await self.connection_manager.disconnect_worker(node_id)
        await self.task_registry.cleanup_worker(node_id)
        await self.registry.remove(node_id)
        await self.event_bus.emit("NODE_REMOVED", node_id, {"instance_id": node.instance_id})
        logger.info("Node %s removed from registry", node_id)
        return True

    async def get_node_status(self, node_id: str) -> dict[str, Any] | None:
        node = await self.registry.get(node_id)
        if node is None:
            return None
        connected = self.connection_manager.is_connected(node_id)
        return {
            "node_id": node.node_id,
            "status": node.status.value,
            "platform": node.platform,
            "public_ip": node.public_ip,
            "private_ip": node.private_ip,
            "ws_connected": connected,
            "created_at": node.created_at.isoformat() if node.created_at else None,
            "last_heartbeat": node.last_heartbeat.isoformat() if node.last_heartbeat else None,
            "metadata": node.metadata,
        }

    # ------------------------------------------------------------------
    # Event callbacks
    # ------------------------------------------------------------------

    async def _on_worker_message(self, worker_id: str, msg: Message) -> None:
        await self.event_bus.emit(msg.type, worker_id, msg.model_dump())

    async def _on_worker_connect(self, worker_id: str) -> None:
        await self.event_bus.emit("WORKER_CONNECTED", worker_id, {})
        logger.info("Worker %s connected to Manager", worker_id)

    async def _on_worker_disconnect(self, worker_id: str) -> None:
        await self.task_registry.cleanup_worker(worker_id)
        await self.event_bus.emit("WORKER_DISCONNECTED", worker_id, {})
        logger.info("Worker %s disconnected from Manager", worker_id)
