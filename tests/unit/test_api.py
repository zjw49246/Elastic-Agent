"""Tests for FastAPI routes (T-016)."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

import pytest
from httpx import ASGITransport, AsyncClient

from elastic_agent.api.app import create_app
from elastic_agent.api.auth import reset_api_keys
from elastic_agent.core.config import ElasticAgentConfig
from elastic_agent.core.providers.base import CloudProvider, Instance, InstanceConfig, InstanceState
from elastic_agent.core.registry import NodeRecord, NodeStatus
from elastic_agent.manager.manager import ElasticAgentManager

API_KEY = "test-api-key-for-routes"


# ------------------------------------------------------------------
# Minimal in-memory provider (reuse from test_manager)
# ------------------------------------------------------------------


class InMemoryProvider(CloudProvider):
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

    async def reboot_instance(self, instance_id: str) -> None:
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


@pytest.fixture(autouse=True)
def setup_api_keys(monkeypatch):
    monkeypatch.setenv("ELASTIC_AGENT_EXTERNAL_API_KEYS", API_KEY)
    reset_api_keys()
    yield
    reset_api_keys()


@pytest.fixture
def tmp_config(tmp_path):
    cfg = ElasticAgentConfig()
    cfg.registry.path = str(tmp_path / "registry.json")
    return cfg


@pytest.fixture
def provider():
    return InMemoryProvider()


@pytest.fixture
def manager(tmp_config, provider):
    return ElasticAgentManager(tmp_config, provider)


@pytest.fixture
def app(manager):
    return create_app(manager)


@pytest.fixture
async def client(app, manager):
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://testserver",
        headers={"Authorization": f"Bearer {API_KEY}"},
    ) as ac:
        await manager.start()
        yield ac
        await manager.stop()


# ------------------------------------------------------------------
# Health endpoint
# ------------------------------------------------------------------


class TestHealthEndpoint:
    @pytest.mark.asyncio
    async def test_health(self, client):
        resp = await client.get("/api/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "healthy"
        assert "uptime_seconds" in data
        assert data["worker_count"] == 0
        assert data["provider"] == "aliyun"


# ------------------------------------------------------------------
# Node list
# ------------------------------------------------------------------


class TestNodeListEndpoint:
    @pytest.mark.asyncio
    async def test_empty_list(self, client):
        resp = await client.get("/api/nodes")
        assert resp.status_code == 200
        data = resp.json()
        assert data["nodes"] == []
        assert data["total"] == 0

    @pytest.mark.asyncio
    async def test_list_after_scale_out(self, client, manager):
        await manager.scale_out(count=2)
        resp = await client.get("/api/nodes")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 2
        assert len(data["nodes"]) == 2

    @pytest.mark.asyncio
    async def test_filter_by_status(self, client, manager):
        await manager.scale_out(count=2)
        resp = await client.get("/api/nodes?status=creating")
        data = resp.json()
        assert data["total"] == 2

        resp = await client.get("/api/nodes?status=ready")
        data = resp.json()
        assert data["total"] == 0

    @pytest.mark.asyncio
    async def test_invalid_status_filter(self, client):
        resp = await client.get("/api/nodes?status=banana")
        assert resp.status_code == 400

    @pytest.mark.asyncio
    async def test_pagination(self, client, manager):
        await manager.scale_out(count=5)
        resp = await client.get("/api/nodes?limit=2&offset=0")
        data = resp.json()
        assert data["total"] == 5
        assert len(data["nodes"]) == 2

        resp = await client.get("/api/nodes?limit=2&offset=4")
        data = resp.json()
        assert len(data["nodes"]) == 1


# ------------------------------------------------------------------
# Node detail
# ------------------------------------------------------------------


class TestNodeDetailEndpoint:
    @pytest.mark.asyncio
    async def test_get_node(self, client, manager):
        records = await manager.scale_out(count=1)
        nid = records[0].node_id
        resp = await client.get(f"/api/nodes/{nid}")
        assert resp.status_code == 200
        data = resp.json()
        assert data["node_id"] == nid
        assert data["status"] == "creating"

    @pytest.mark.asyncio
    async def test_get_nonexistent_node(self, client):
        resp = await client.get("/api/nodes/no-such-node")
        assert resp.status_code == 404


# ------------------------------------------------------------------
# Node status
# ------------------------------------------------------------------


class TestNodeStatusEndpoint:
    @pytest.mark.asyncio
    async def test_get_status(self, client, manager):
        records = await manager.scale_out(count=1)
        nid = records[0].node_id
        resp = await client.get(f"/api/nodes/{nid}/status")
        assert resp.status_code == 200
        data = resp.json()
        assert data["node_id"] == nid
        assert data["ws_connected"] is False

    @pytest.mark.asyncio
    async def test_status_nonexistent(self, client):
        resp = await client.get("/api/nodes/nope/status")
        assert resp.status_code == 404


# ------------------------------------------------------------------
# Scale-out
# ------------------------------------------------------------------


class TestScaleOutEndpoint:
    @pytest.mark.asyncio
    async def test_scale_out_basic(self, client):
        resp = await client.post("/api/scale-out", json={"count": 2})
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["nodes"]) == 2
        for n in data["nodes"]:
            assert n["status"] == "creating"
            assert n["node_id"].startswith("dryrun:")

    @pytest.mark.asyncio
    async def test_scale_out_defaults(self, client):
        resp = await client.post("/api/scale-out", json={})
        assert resp.status_code == 200
        assert len(resp.json()["nodes"]) == 1


# ------------------------------------------------------------------
# Scale-in
# ------------------------------------------------------------------


class TestScaleInEndpoint:
    @pytest.mark.asyncio
    async def test_scale_in_force(self, client, manager):
        records = await manager.scale_out(count=2)
        node_ids = [r.node_id for r in records]
        resp = await client.post("/api/scale-in", json={"node_ids": node_ids, "force": True})
        assert resp.status_code == 200
        data = resp.json()
        assert set(data["terminated"]) == set(node_ids)

    @pytest.mark.asyncio
    async def test_scale_in_drain(self, client, manager):
        records = await manager.scale_out(count=1)
        nid = records[0].node_id
        resp = await client.post("/api/scale-in", json={"node_ids": [nid]})
        assert resp.status_code == 200
        data = resp.json()
        assert data["terminated"] == []


# ------------------------------------------------------------------
# Drain
# ------------------------------------------------------------------


class TestDrainEndpoint:
    @pytest.mark.asyncio
    async def test_drain(self, client, manager):
        records = await manager.scale_out(count=1)
        nid = records[0].node_id
        resp = await client.post(f"/api/nodes/{nid}/drain")
        assert resp.status_code == 200
        assert resp.json()["status"] == "draining"

    @pytest.mark.asyncio
    async def test_drain_nonexistent(self, client):
        resp = await client.post("/api/nodes/no-node/drain")
        assert resp.status_code == 404


class TestNodeLogs:
    @pytest.mark.asyncio
    async def test_logs_via_ssh(self, client, manager, monkeypatch):
        import elastic_agent.core.bootstrap as bootstrap_mod
        captured = {}

        class FakeSSH:
            def __init__(self, host, *, user=None, key_path=None, use_sudo=None):
                captured["host"] = host

            async def execute(self, cmd, timeout=None):
                captured["cmd"] = cmd
                return 0, "LOGDATA\nline2", ""

        monkeypatch.setattr(bootstrap_mod, "SSHExecutor", FakeSSH)
        manager.config.provider.type = "aws"
        rec = (await manager.scale_out(count=1))[0]
        resp = await client.get(f"/api/nodes/{rec.node_id}/logs?lines=50")
        assert resp.status_code == 200
        assert resp.json()["logs"] == "LOGDATA\nline2"
        assert captured["host"] == "10.0.0.1"
        assert "journalctl -u ea-runtime" in captured["cmd"]
        assert "-n 50" in captured["cmd"]

    @pytest.mark.asyncio
    async def test_logs_404_for_unknown_node(self, client):
        resp = await client.get("/api/nodes/no-such/logs")
        assert resp.status_code == 404
