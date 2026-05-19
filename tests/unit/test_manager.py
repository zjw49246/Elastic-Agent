"""Tests for ElasticAgentManager (T-016)."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from elastic_agent.core.config import ElasticAgentConfig
from elastic_agent.core.registry import NodeRecord, NodeRegistry, NodeStatus
from elastic_agent.core.providers.base import CloudProvider, Instance, InstanceConfig, InstanceState
from elastic_agent.manager.manager import ElasticAgentManager


# ------------------------------------------------------------------
# Helpers — in-memory mock provider
# ------------------------------------------------------------------


class InMemoryProvider(CloudProvider):
    """Minimal mock cloud provider for unit testing."""

    def __init__(self):
        self._instances: dict[str, Instance] = {}
        self._counter = 0

    @property
    def platform(self) -> str:
        return "dryrun"

    async def create_instance(self, config: InstanceConfig) -> Instance:
        self._counter += 1
        iid = f"i-dry-{self._counter:04d}"
        inst = Instance(
            instance_id=f"dryrun:{iid}",
            platform="dryrun",
            native_id=iid,
            state=InstanceState.PENDING,
            public_ip=f"1.2.3.{self._counter}",
            private_ip=f"10.0.0.{self._counter}",
            instance_type=config.instance_type,
            region="test",
            zone="test-a",
            tags=config.tags,
        )
        self._instances[iid] = inst
        return inst

    async def terminate_instance(self, instance_id: str) -> None:
        native = instance_id.split(":", 1)[-1] if ":" in instance_id else instance_id
        self._instances.pop(native, None)

    async def start_instance(self, instance_id: str) -> None:
        pass

    async def stop_instance(self, instance_id: str) -> None:
        pass

    async def list_instances(self, filters: dict | None = None) -> list[Instance]:
        return list(self._instances.values())

    async def get_instance(self, instance_id: str) -> Instance | None:
        native = instance_id.split(":", 1)[-1] if ":" in instance_id else instance_id
        return self._instances.get(native)

    async def wait_until_running(self, instance_id: str, timeout: int = 300) -> Instance:
        inst = await self.get_instance(instance_id)
        if inst:
            inst.state = InstanceState.RUNNING
        return inst


# ------------------------------------------------------------------
# Fixtures
# ------------------------------------------------------------------


@pytest.fixture
def tmp_config(tmp_path):
    registry_path = tmp_path / "registry.json"
    cfg = ElasticAgentConfig()
    cfg.registry.path = str(registry_path)
    return cfg


@pytest.fixture
def provider():
    return InMemoryProvider()


@pytest.fixture
def manager(tmp_config, provider):
    return ElasticAgentManager(tmp_config, provider)


# ------------------------------------------------------------------
# Tests: lifecycle
# ------------------------------------------------------------------


class TestManagerLifecycle:
    @pytest.mark.asyncio
    async def test_start_stop(self, manager):
        await manager.start()
        assert manager._started is True
        await manager.stop()
        assert manager._started is False

    @pytest.mark.asyncio
    async def test_registry_loaded_on_start(self, manager):
        await manager.start()
        nodes = await manager.registry.list_all()
        assert nodes == []
        await manager.stop()


# ------------------------------------------------------------------
# Tests: scale_out
# ------------------------------------------------------------------


class TestScaleOut:
    @pytest.mark.asyncio
    async def test_scale_out_single(self, manager):
        await manager.start()
        records = await manager.scale_out(count=1)
        assert len(records) == 1
        rec = records[0]
        assert rec.status == NodeStatus.CREATING
        assert rec.auth_token is not None
        assert rec.node_id.startswith("dryrun:")
        all_nodes = await manager.registry.list_all()
        assert len(all_nodes) == 1
        await manager.stop()

    @pytest.mark.asyncio
    async def test_scale_out_multiple(self, manager):
        await manager.start()
        records = await manager.scale_out(count=3)
        assert len(records) == 3
        ids = {r.node_id for r in records}
        assert len(ids) == 3
        all_nodes = await manager.registry.list_all()
        assert len(all_nodes) == 3
        await manager.stop()

    @pytest.mark.asyncio
    async def test_scale_out_event_emitted(self, manager):
        await manager.start()
        events = []

        async def handler(event_type, worker_id, data):
            events.append((event_type, worker_id))

        manager.event_bus.subscribe("NODE_CREATING", handler)
        await manager.scale_out(count=2)
        assert len(events) == 2
        assert all(et == "NODE_CREATING" for et, _ in events)
        await manager.stop()


# ------------------------------------------------------------------
# Tests: scale_in
# ------------------------------------------------------------------


class TestScaleIn:
    @pytest.mark.asyncio
    async def test_scale_in_force(self, manager, provider):
        await manager.start()
        records = await manager.scale_out(count=2)
        node_ids = [r.node_id for r in records]
        terminated = await manager.scale_in(node_ids, force=True)
        assert set(terminated) == set(node_ids)
        for nid in node_ids:
            node = await manager.registry.get(nid)
            assert node.status == NodeStatus.TERMINATED
        await manager.stop()

    @pytest.mark.asyncio
    async def test_scale_in_drain(self, manager):
        await manager.start()
        records = await manager.scale_out(count=1)
        nid = records[0].node_id
        terminated = await manager.scale_in([nid], force=False)
        assert terminated == []
        node = await manager.registry.get(nid)
        assert node.status == NodeStatus.DRAINING
        await manager.stop()

    @pytest.mark.asyncio
    async def test_scale_in_nonexistent_node(self, manager):
        await manager.start()
        terminated = await manager.scale_in(["nonexistent"], force=True)
        assert terminated == []
        await manager.stop()


# ------------------------------------------------------------------
# Tests: drain_node
# ------------------------------------------------------------------


class TestDrainNode:
    @pytest.mark.asyncio
    async def test_drain_existing(self, manager):
        await manager.start()
        records = await manager.scale_out(count=1)
        nid = records[0].node_id
        result = await manager.drain_node(nid)
        assert result is True
        node = await manager.registry.get(nid)
        assert node.status == NodeStatus.DRAINING
        await manager.stop()

    @pytest.mark.asyncio
    async def test_drain_nonexistent(self, manager):
        await manager.start()
        result = await manager.drain_node("no-such-node")
        assert result is False
        await manager.stop()


# ------------------------------------------------------------------
# Tests: get_node_status
# ------------------------------------------------------------------


class TestGetNodeStatus:
    @pytest.mark.asyncio
    async def test_existing_node(self, manager):
        await manager.start()
        records = await manager.scale_out(count=1)
        nid = records[0].node_id
        info = await manager.get_node_status(nid)
        assert info is not None
        assert info["node_id"] == nid
        assert info["status"] == "creating"
        assert info["ws_connected"] is False
        await manager.stop()

    @pytest.mark.asyncio
    async def test_nonexistent_node(self, manager):
        await manager.start()
        info = await manager.get_node_status("nope")
        assert info is None
        await manager.stop()


# ------------------------------------------------------------------
# Tests: event routing
# ------------------------------------------------------------------


class TestEventRouting:
    @pytest.mark.asyncio
    async def test_on_worker_message_emits_to_bus(self, manager):
        await manager.start()
        events = []

        async def handler(event_type, worker_id, data):
            events.append((event_type, worker_id))

        manager.event_bus.subscribe("HEARTBEAT", handler)

        from elastic_agent.core.protocols.messages import HeartbeatMessage

        msg = HeartbeatMessage(uptime_seconds=100)
        await manager._on_worker_message("w-1", msg)
        assert len(events) == 1
        assert events[0] == ("HEARTBEAT", "w-1")
        await manager.stop()

    @pytest.mark.asyncio
    async def test_on_worker_connect_emits(self, manager):
        await manager.start()
        events = []

        async def handler(event_type, worker_id, data):
            events.append(event_type)

        manager.event_bus.subscribe("WORKER_CONNECTED", handler)
        await manager._on_worker_connect("w-1")
        assert "WORKER_CONNECTED" in events
        await manager.stop()

    @pytest.mark.asyncio
    async def test_on_worker_disconnect_emits(self, manager):
        await manager.start()
        events = []

        async def handler(event_type, worker_id, data):
            events.append(event_type)

        manager.event_bus.subscribe("WORKER_DISCONNECTED", handler)
        await manager._on_worker_disconnect("w-1")
        assert "WORKER_DISCONNECTED" in events
        await manager.stop()
