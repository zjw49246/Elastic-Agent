"""Tests for external API authentication (T-015)."""

from __future__ import annotations

import os
from unittest.mock import AsyncMock

import pytest
from httpx import ASGITransport, AsyncClient

from elastic_agent.api.app import create_app
from elastic_agent.api.auth import get_api_keys, reset_api_keys
from elastic_agent.core.config import ElasticAgentConfig
from elastic_agent.core.providers.base import CloudProvider, Instance, InstanceConfig, InstanceState
from elastic_agent.manager.manager import ElasticAgentManager


class InMemoryProvider(CloudProvider):
    def __init__(self):
        self._counter = 0

    @property
    def platform(self) -> str:
        return "dryrun"

    async def create_instance(self, config: InstanceConfig) -> Instance:
        self._counter += 1
        iid = f"i-dry-{self._counter:04d}"
        return Instance(
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

    async def terminate_instance(self, instance_id: str) -> None:
        pass

    async def start_instance(self, instance_id: str) -> None:
        pass

    async def stop_instance(self, instance_id: str) -> None:
        pass

    async def list_instances(self, filters: dict | None = None) -> list[Instance]:
        return []

    async def get_instance(self, instance_id: str) -> Instance | None:
        return None

    async def wait_until_running(self, instance_id: str, timeout: int = 300) -> Instance:
        return None


API_KEY = "test-key-abc123"


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
def manager(tmp_config):
    return ElasticAgentManager(tmp_config, InMemoryProvider())


@pytest.fixture
def app(manager):
    return create_app(manager)


@pytest.fixture
async def client(app, manager):
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://testserver",
    ) as ac:
        await manager.start()
        yield ac
        await manager.stop()


class TestAuthBearerHeader:
    @pytest.mark.asyncio
    async def test_valid_bearer_token(self, client):
        resp = await client.get(
            "/api/nodes",
            headers={"Authorization": f"Bearer {API_KEY}"},
        )
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_invalid_bearer_token(self, client):
        resp = await client.get(
            "/api/nodes",
            headers={"Authorization": "Bearer wrong-key"},
        )
        assert resp.status_code == 401
        assert "Invalid API key" in resp.json()["detail"]

    @pytest.mark.asyncio
    async def test_missing_auth_header(self, client):
        resp = await client.get("/api/nodes")
        assert resp.status_code == 401
        assert "Missing API key" in resp.json()["detail"]

    @pytest.mark.asyncio
    async def test_bearer_case_insensitive(self, client):
        resp = await client.get(
            "/api/nodes",
            headers={"Authorization": f"bearer {API_KEY}"},
        )
        assert resp.status_code == 200


class TestAuthQueryParam:
    @pytest.mark.asyncio
    async def test_valid_query_param(self, client):
        resp = await client.get(f"/api/nodes?api_key={API_KEY}")
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_invalid_query_param(self, client):
        resp = await client.get("/api/nodes?api_key=wrong")
        assert resp.status_code == 401

    @pytest.mark.asyncio
    async def test_bearer_takes_precedence(self, client):
        resp = await client.get(
            "/api/nodes?api_key=wrong",
            headers={"Authorization": f"Bearer {API_KEY}"},
        )
        assert resp.status_code == 200


class TestAuthMultipleKeys:
    @pytest.mark.asyncio
    async def test_multiple_keys(self, client, monkeypatch):
        monkeypatch.setenv("ELASTIC_AGENT_EXTERNAL_API_KEYS", "key-a,key-b,key-c")
        reset_api_keys()

        for key in ["key-a", "key-b", "key-c"]:
            resp = await client.get(
                "/api/nodes",
                headers={"Authorization": f"Bearer {key}"},
            )
            assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_none_match(self, client, monkeypatch):
        monkeypatch.setenv("ELASTIC_AGENT_EXTERNAL_API_KEYS", "key-a,key-b")
        reset_api_keys()

        resp = await client.get(
            "/api/nodes",
            headers={"Authorization": "Bearer key-x"},
        )
        assert resp.status_code == 401


class TestAuthNoKeysConfigured:
    @pytest.mark.asyncio
    async def test_no_keys_returns_503(self, client, monkeypatch):
        monkeypatch.setenv("ELASTIC_AGENT_EXTERNAL_API_KEYS", "")
        reset_api_keys()

        resp = await client.get(
            "/api/nodes",
            headers={"Authorization": "Bearer anything"},
        )
        assert resp.status_code == 503
        assert "No API keys configured" in resp.json()["detail"]


class TestAuthProtectsAllNodeEndpoints:
    @pytest.mark.asyncio
    async def test_scale_out_requires_auth(self, client):
        resp = await client.post("/api/scale-out", json={"count": 1})
        assert resp.status_code == 401

    @pytest.mark.asyncio
    async def test_scale_in_requires_auth(self, client):
        resp = await client.post("/api/scale-in", json={"node_ids": []})
        assert resp.status_code == 401

    @pytest.mark.asyncio
    async def test_drain_requires_auth(self, client):
        resp = await client.post("/api/nodes/x/drain")
        assert resp.status_code == 401

    @pytest.mark.asyncio
    async def test_node_detail_requires_auth(self, client):
        resp = await client.get("/api/nodes/x")
        assert resp.status_code == 401


class TestHealthNoAuth:
    @pytest.mark.asyncio
    async def test_health_is_public(self, client):
        resp = await client.get("/api/health")
        assert resp.status_code == 200
