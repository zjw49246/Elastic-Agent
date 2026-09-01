"""ApexRouter provider contract tests.

These tests intentionally keep the public Agent API provider id (``apex``)
separate from the Codex model-provider id (``apexrouter``).  The behavior is
aligned with CCM's ApexRouter integration.
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from elastic_agent.core.agent_api import (
    AgentApiAccountStore,
    AgentApiProviderRegistry,
    AgentApiUnsupportedProviderError,
    AgentApiUpstreamError,
)
from elastic_agent.core.apexrouter import (
    APEX_CODEX_BASE_URL,
    APEX_CODEX_CLIENT_VERSION,
    APEX_MODELS_URL,
    APEX_USAGE_URL,
    ApexRouterAdapter,
)
from elastic_agent.core.bootstrap_steps import CODEX_CLI_VERSION


def _active_usage_payload() -> dict:
    return {
        "key_name": "test-key",
        "group_name": "apex-research",
        # ``used`` is usage by this one Key, not usage by the shared group.
        "used": {
            "requests_5h": 3,
            "requests_day": 7,
            "tokens_day": 1_000,
            "tokens_month": 2_000,
        },
        # ``remaining`` and ``limits`` describe the shared Apex group.
        "remaining": {
            "requests_5h": 24_000,
            "requests_day": 49_000,
            "tokens_day": 9_000_000,
            "tokens_month": 90_000_000,
        },
        "limits": {
            "requests_5h": 25_000,
            "requests_day": 50_000,
            "tokens_day": 10_000_000,
            "tokens_month": 100_000_000,
            "concurrency": 20,
        },
    }


def test_default_registry_enables_apex_after_cloudrouter() -> None:
    registry = AgentApiProviderRegistry.default()

    # Preserve the established CloudRouter-first allocation order when Apex is
    # enabled. ``apexrouter`` is a Codex config id, not a public API provider.
    assert registry.providers == ("cloudrouter", "apex")
    assert isinstance(registry.require("apex"), ApexRouterAdapter)
    with pytest.raises(AgentApiUnsupportedProviderError):
        registry.require("apexrouter")


def test_apex_endpoints_and_client_version_are_fixed() -> None:
    adapter = ApexRouterAdapter()

    assert APEX_CODEX_BASE_URL == "https://api.apexin.ai/v1"
    assert APEX_MODELS_URL == f"{APEX_CODEX_BASE_URL}/models"
    assert APEX_USAGE_URL == f"{APEX_CODEX_BASE_URL}/usage"
    assert APEX_CODEX_CLIENT_VERSION == CODEX_CLI_VERSION == "0.144.6"
    assert APEX_CODEX_BASE_URL in adapter.endpoints.values()
    assert APEX_MODELS_URL in adapter.endpoints.values()
    assert APEX_USAGE_URL in adapter.endpoints.values()
    assert all(
        value is None
        or value.startswith("https://api.apexin.ai/")
        for value in adapter.endpoints.values()
    )


@pytest.mark.asyncio
async def test_apex_native_model_probe_is_versioned_filtered_and_codex_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = ApexRouterAdapter()
    request = AsyncMock(return_value={
        "models": [
            {
                "slug": "claude-opus-4-8",
                "supported_in_api": True,
                "visibility": "list",
            },
            {
                "slug": "gpt-5.4",
                "supported_in_api": True,
                "visibility": "list",
            },
            {
                "slug": "o3",
                "supported_in_api": True,
                "visibility": "list",
            },
            {
                "slug": "gpt-hidden",
                "supported_in_api": True,
                "visibility": "hide",
            },
            {
                "slug": "gpt-disabled",
                "supported_in_api": False,
                "visibility": "list",
            },
            {
                "slug": "gpt-5.4",
                "supported_in_api": True,
                "visibility": "list",
            },
            {"not_a_slug": "gpt-ignored"},
        ],
    })
    monkeypatch.setattr(adapter, "_request_json", request)

    models = await adapter.probe_models("lck-private")

    assert models == {
        "claude": [],
        "codex": ["gpt-5.4", "o3"],
    }
    request.assert_awaited_once_with(
        (
            f"{APEX_MODELS_URL}?client_version="
            f"{APEX_CODEX_CLIENT_VERSION}"
        ),
        "lck-private",
    )


@pytest.mark.asyncio
async def test_apex_openai_model_probe_is_filtered_and_codex_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = ApexRouterAdapter()
    request = AsyncMock(return_value={
        "object": "list",
        "success": True,
        "data": [
            {"id": "gpt-5.6-sol", "object": "model"},
            {"id": "codex-auto-review", "object": "model"},
            {"id": "claude-opus-5", "object": "model"},
            {"id": "gemini-3.7-flash", "object": "model"},
            {"id": "gpt-5.6-sol", "object": "model"},
            {"not_an_id": "gpt-ignored"},
        ],
    })
    monkeypatch.setattr(adapter, "_request_json", request)

    models = await adapter.probe_models("lck-private")

    assert models == {
        "claude": [],
        "codex": ["codex-auto-review", "gpt-5.6-sol"],
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [
        {"object": "list", "success": False, "data": [{"id": "gpt-5.6-sol"}]},
        {"object": "error", "success": True, "data": [{"id": "gpt-5.6-sol"}]},
        {
            "object": "list",
            "success": True,
            "data": [{"id": "gpt-5.6-sol"}],
            "models": [{"slug": "gpt-5.6-sol"}],
        },
    ],
)
async def test_apex_model_probe_rejects_ambiguous_or_failed_schema(
    monkeypatch: pytest.MonkeyPatch,
    payload: dict,
) -> None:
    adapter = ApexRouterAdapter()
    monkeypatch.setattr(adapter, "_request_json", AsyncMock(return_value=payload))

    with pytest.raises(AgentApiUpstreamError, match="invalid_models_response"):
        await adapter.probe_models("lck-private")


@pytest.mark.asyncio
async def test_apex_usage_keeps_key_usage_separate_from_shared_group_quota(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = ApexRouterAdapter()
    request = AsyncMock(return_value=_active_usage_payload())
    monkeypatch.setattr(adapter, "_request_json", request)

    snapshot = await adapter.fetch_usage("apex-1", "lck-private")

    assert snapshot["account_id"] == "apex-1"
    assert snapshot["known"] is True
    assert snapshot["available"] is True
    assert snapshot["state"] == "active"
    assert snapshot["mode"] == "shared_group"
    assert snapshot["key_name"] == "test-key"
    assert snapshot["group_name"] == "apex-research"
    assert snapshot["concurrency"] == 20
    assert snapshot["key_usage"] == {
        "requests_5h": 3,
        "requests_day": 7,
        "tokens_day": 1_000,
        "tokens_month": 2_000,
    }
    assert snapshot["usage"]["key"] == snapshot["key_usage"]

    windows = {window["id"]: window for window in snapshot["windows"]}
    requests_5h = windows["requests_5h"]
    assert requests_5h["scope"] == "group"
    assert requests_5h["used"] == 1_000
    assert requests_5h["remaining"] == 24_000
    assert requests_5h["limit"] == 25_000
    assert requests_5h["key_used"] == 3
    request.assert_awaited_once_with(APEX_USAGE_URL, "lck-private")


@pytest.mark.asyncio
async def test_apex_null_shared_group_windows_are_unlimited(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = ApexRouterAdapter()
    payload = _active_usage_payload()
    for window_id in (
        "requests_5h",
        "requests_day",
        "tokens_day",
        "tokens_month",
    ):
        payload["remaining"][window_id] = None
        payload["limits"][window_id] = None
    monkeypatch.setattr(
        adapter,
        "_request_json",
        AsyncMock(return_value=payload),
    )

    snapshot = await adapter.fetch_usage("apex-1", "lck-private")

    assert snapshot["state"] == "active"
    assert snapshot["available"] is True
    assert snapshot["concurrency"] == 20
    assert snapshot["key_usage"] == payload["used"]
    assert len(snapshot["windows"]) == 4
    assert all(window["unlimited"] is True for window in snapshot["windows"])
    assert all("limit" not in window for window in snapshot["windows"])
    assert all("remaining" not in window for window in snapshot["windows"])
    assert all("utilization" not in window for window in snapshot["windows"])


@pytest.mark.asyncio
async def test_apex_usage_supports_mixed_limited_and_unlimited_windows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = ApexRouterAdapter()
    payload = _active_usage_payload()
    payload["remaining"]["requests_5h"] = None
    payload["limits"]["requests_5h"] = None
    payload["remaining"]["tokens_day"] = None
    payload["limits"]["tokens_day"] = None
    monkeypatch.setattr(
        adapter,
        "_request_json",
        AsyncMock(return_value=payload),
    )

    snapshot = await adapter.fetch_usage("apex-1", "lck-private")

    windows = {window["id"]: window for window in snapshot["windows"]}
    assert windows["requests_5h"]["unlimited"] is True
    assert windows["requests_5h"]["key_used"] == 3
    assert windows["tokens_day"]["unlimited"] is True
    assert windows["requests_day"]["limit"] == 50_000
    assert windows["requests_day"]["remaining"] == 49_000
    assert windows["tokens_month"]["used"] == 10_000_000
    assert snapshot["state"] == "active"
    assert snapshot["available"] is True


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("remaining", "limit"),
    [
        (None, 100),
        (100, None),
        ("invalid", 100),
        (100, "invalid"),
    ],
)
async def test_apex_usage_rejects_asymmetric_or_invalid_window_values(
    monkeypatch: pytest.MonkeyPatch,
    remaining: object,
    limit: object,
) -> None:
    adapter = ApexRouterAdapter()
    payload = _active_usage_payload()
    payload["remaining"]["requests_5h"] = remaining
    payload["limits"]["requests_5h"] = limit
    monkeypatch.setattr(
        adapter,
        "_request_json",
        AsyncMock(return_value=payload),
    )

    with pytest.raises(
        AgentApiUpstreamError,
        match="invalid_usage_response",
    ):
        await adapter.fetch_usage("apex-1", "lck-private")


@pytest.mark.asyncio
async def test_apex_partial_shared_group_usage_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = ApexRouterAdapter()
    partial = _active_usage_payload()
    del partial["remaining"]["requests_day"]
    monkeypatch.setattr(
        adapter,
        "_request_json",
        AsyncMock(return_value=partial),
    )

    with pytest.raises(
        AgentApiUpstreamError,
        match="invalid_usage_response",
    ):
        await adapter.fetch_usage("apex-1", "lck-private")


@pytest.mark.asyncio
async def test_apex_underflowing_shared_quota_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = ApexRouterAdapter()
    payload = _active_usage_payload()
    payload["remaining"]["requests_5h"] = "1e-1000"
    monkeypatch.setattr(
        adapter,
        "_request_json",
        AsyncMock(return_value=payload),
    )

    with pytest.raises(
        AgentApiUpstreamError,
        match="invalid_usage_response",
    ):
        await adapter.fetch_usage("apex-1", "lck-private")


@pytest.mark.asyncio
@pytest.mark.parametrize("exhaustion", ["window", "concurrency"])
async def test_apex_shared_group_exhaustion_is_unavailable(
    monkeypatch: pytest.MonkeyPatch,
    exhaustion: str,
) -> None:
    adapter = ApexRouterAdapter()
    payload = _active_usage_payload()
    if exhaustion == "window":
        payload["remaining"]["requests_5h"] = 0
    else:
        payload["limits"]["concurrency"] = 0
    monkeypatch.setattr(
        adapter,
        "_request_json",
        AsyncMock(return_value=payload),
    )

    snapshot = await adapter.fetch_usage("apex-1", "lck-private")

    assert snapshot["known"] is True
    assert snapshot["available"] is False
    assert snapshot["state"] == "exhausted"
    assert snapshot["reason"] == "exhausted"


@pytest.mark.asyncio
async def test_same_key_can_exist_once_per_provider_with_cloudrouter_first(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = AgentApiProviderRegistry.default()
    cloudrouter = registry.require("cloudrouter")
    apex = registry.require("apex")
    monkeypatch.setattr(
        cloudrouter,
        "probe_models",
        AsyncMock(return_value={
            "claude": ["claude-opus-4-8"],
            "codex": ["gpt-5.4"],
        }),
    )
    monkeypatch.setattr(
        apex,
        "probe_models",
        AsyncMock(return_value={
            "claude": [],
            "codex": ["gpt-5.4"],
        }),
    )
    store = AgentApiAccountStore(
        tmp_path / "agent-api",
        registry=registry,
    )

    cloudrouter_account = await store.add(
        "cloudrouter",
        "CloudRouter",
        "shared-private-key",
    )
    apex_account = await store.add(
        "apex",
        "ApexRouter",
        "shared-private-key",
    )

    assert cloudrouter_account.id == "cloudrouter-1"
    assert apex_account.id == "apex-1"
    assert apex_account.supported_agent_types == ["codex"]
    assert not apex_account.supports_agent_type("claude")
    assert [account.id for account in await store.list()] == [
        "cloudrouter-1",
        "apex-1",
    ]
