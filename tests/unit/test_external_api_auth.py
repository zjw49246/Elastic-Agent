"""T-127: External API authentication — valid/invalid/expired API keys."""

from __future__ import annotations

import os
from unittest.mock import AsyncMock

import pytest
from httpx import ASGITransport, AsyncClient

from elastic_agent.api.app import create_app
from elastic_agent.api.auth import (
    get_api_keys,
    reset_api_keys,
    require_api_key,
    _extract_bearer,
    _load_api_keys,
)
from elastic_agent.core.auth import verify_token_constant_time
from elastic_agent.core.config import ElasticAgentConfig
from elastic_agent.core.providers.base import CloudProvider, Instance, InstanceConfig, InstanceState
from elastic_agent.manager.manager import ElasticAgentManager


# ---------------------------------------------------------------------------
# Minimal in-memory provider for testing
# ---------------------------------------------------------------------------


class _TestProvider(CloudProvider):
    def __init__(self):
        self._counter = 0

    @property
    def platform(self) -> str:
        return "dryrun"

    async def create_instance(self, config: InstanceConfig) -> Instance:
        self._counter += 1
        nid = f"i-test-{self._counter:04d}"
        return Instance(
            instance_id=f"dryrun:{nid}", platform="dryrun", native_id=nid,
            state=InstanceState.PENDING, instance_type=config.instance_type,
            region="test", zone="test-a", tags=config.tags,
        )

    async def terminate_instance(self, instance_id: str) -> None: pass
    async def start_instance(self, instance_id: str) -> None: pass
    async def stop_instance(self, instance_id: str) -> None: pass
    async def reboot_instance(self, instance_id: str) -> None: pass
    async def list_instances(self, filters=None) -> list[Instance]: return []
    async def get_instance(self, instance_id: str) -> Instance | None: return None
    async def wait_until_running(self, instance_id: str, timeout: int = 300) -> Instance: return None


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


VALID_KEY = "test-valid-key-12345"
SECOND_KEY = "second-key-67890"


@pytest.fixture(autouse=True)
def setup_keys(monkeypatch):
    monkeypatch.setenv("ELASTIC_AGENT_EXTERNAL_API_KEYS", VALID_KEY)
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
    return ElasticAgentManager(tmp_config, _TestProvider())


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


# ---------------------------------------------------------------------------
# Valid key scenarios
# ---------------------------------------------------------------------------


class TestValidKey:

    @pytest.mark.asyncio
    async def test_bearer_header_valid(self, client):
        resp = await client.get(
            "/api/nodes",
            headers={"Authorization": f"Bearer {VALID_KEY}"},
        )
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_query_param_valid(self, client):
        resp = await client.get(f"/api/nodes?api_key={VALID_KEY}")
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_bearer_case_insensitive(self, client):
        resp = await client.get(
            "/api/nodes",
            headers={"Authorization": f"bearer {VALID_KEY}"},
        )
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_bearer_with_extra_whitespace(self, client):
        resp = await client.get(
            "/api/nodes",
            headers={"Authorization": f"Bearer   {VALID_KEY}  "},
        )
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# Invalid key scenarios
# ---------------------------------------------------------------------------


class TestInvalidKey:

    @pytest.mark.asyncio
    async def test_wrong_bearer_token(self, client):
        resp = await client.get(
            "/api/nodes",
            headers={"Authorization": "Bearer totally-wrong-key"},
        )
        assert resp.status_code == 401
        assert "Invalid API key" in resp.json()["detail"]

    @pytest.mark.asyncio
    async def test_wrong_query_param(self, client):
        resp = await client.get("/api/nodes?api_key=bad-key")
        assert resp.status_code == 401

    @pytest.mark.asyncio
    async def test_empty_bearer_value(self, client):
        resp = await client.get(
            "/api/nodes",
            headers={"Authorization": "Bearer "},
        )
        assert resp.status_code == 401

    @pytest.mark.asyncio
    async def test_no_auth_provided(self, client):
        resp = await client.get("/api/nodes")
        assert resp.status_code == 401
        assert "Missing API key" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# No keys configured (503)
# ---------------------------------------------------------------------------


class TestNoKeysConfigured:

    @pytest.mark.asyncio
    async def test_empty_env_returns_503(self, client, monkeypatch):
        monkeypatch.setenv("ELASTIC_AGENT_EXTERNAL_API_KEYS", "")
        reset_api_keys()

        resp = await client.get(
            "/api/nodes",
            headers={"Authorization": "Bearer anything"},
        )
        assert resp.status_code == 503
        assert "No API keys configured" in resp.json()["detail"]

    @pytest.mark.asyncio
    async def test_whitespace_only_env_returns_503(self, client, monkeypatch):
        monkeypatch.setenv("ELASTIC_AGENT_EXTERNAL_API_KEYS", "  ,  , ")
        reset_api_keys()

        resp = await client.get(
            "/api/nodes",
            headers={"Authorization": "Bearer test"},
        )
        assert resp.status_code == 503


# ---------------------------------------------------------------------------
# Multiple keys
# ---------------------------------------------------------------------------


class TestMultipleKeys:

    @pytest.mark.asyncio
    async def test_all_configured_keys_work(self, client, monkeypatch):
        monkeypatch.setenv("ELASTIC_AGENT_EXTERNAL_API_KEYS", f"{VALID_KEY},{SECOND_KEY}")
        reset_api_keys()

        for key in [VALID_KEY, SECOND_KEY]:
            resp = await client.get(
                "/api/nodes",
                headers={"Authorization": f"Bearer {key}"},
            )
            assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_unknown_key_rejected_with_multiple(self, client, monkeypatch):
        monkeypatch.setenv("ELASTIC_AGENT_EXTERNAL_API_KEYS", f"{VALID_KEY},{SECOND_KEY}")
        reset_api_keys()

        resp = await client.get(
            "/api/nodes",
            headers={"Authorization": "Bearer unknown-key"},
        )
        assert resp.status_code == 401

    @pytest.mark.asyncio
    async def test_bearer_takes_precedence_over_query(self, client):
        resp = await client.get(
            "/api/nodes?api_key=wrong-key",
            headers={"Authorization": f"Bearer {VALID_KEY}"},
        )
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# Constant-time comparison
# ---------------------------------------------------------------------------


class TestConstantTimeComparison:

    def test_matching_tokens(self):
        assert verify_token_constant_time("secret123", "secret123") is True

    def test_non_matching_tokens(self):
        assert verify_token_constant_time("secret123", "secret456") is False

    def test_empty_strings(self):
        assert verify_token_constant_time("", "") is True

    def test_different_lengths(self):
        assert verify_token_constant_time("short", "much-longer-string") is False


# ---------------------------------------------------------------------------
# Bearer extraction
# ---------------------------------------------------------------------------


class TestBearerExtraction:

    def test_load_api_keys_from_env(self, monkeypatch):
        monkeypatch.setenv("ELASTIC_AGENT_EXTERNAL_API_KEYS", "key-a, key-b , key-c")
        reset_api_keys()
        keys = get_api_keys()
        assert keys == ["key-a", "key-b", "key-c"]

    def test_load_api_keys_strips_whitespace(self, monkeypatch):
        monkeypatch.setenv("ELASTIC_AGENT_EXTERNAL_API_KEYS", "  key  ")
        reset_api_keys()
        keys = get_api_keys()
        assert keys == ["key"]


# ---------------------------------------------------------------------------
# Endpoint protection
# ---------------------------------------------------------------------------


class TestEndpointProtection:

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
    async def test_health_is_public(self, client):
        resp = await client.get("/api/health")
        assert resp.status_code == 200
