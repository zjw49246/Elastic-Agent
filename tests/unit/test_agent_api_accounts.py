import asyncio
import json
import os
import shutil
import stat
import sys
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from elastic_agent.core import agent_api as agent_api_module
from elastic_agent.core.agent_api import (
    MAX_AGENT_API_MODEL_ID_LENGTH,
    MAX_AGENT_API_MODELS,
    AgentApiAccountNotFoundError,
    AgentApiAccountStore,
    AgentApiDuplicateKeyError,
    AgentApiProviderRegistry,
    AgentApiStorageError,
    AgentApiUnsupportedProviderError,
    AgentApiUpstreamError,
)
from elastic_agent.core.cloudrouter import (
    CLOUDROUTER_ANTHROPIC_BASE_URL,
    CLOUDROUTER_MODELS_URL,
    CLOUDROUTER_OPENAI_BASE_URL,
    CLOUDROUTER_USAGE_URL,
    MAX_API_KEY_BYTES,
    MAX_API_RESPONSE_BYTES,
    CloudRouterAdapter,
)

MODELS = {
    "claude": ["claude-opus-4-8", "claude-sonnet-5"],
    "codex": ["gpt-5.4", "o3"],
}


def test_platform_reference_uses_secret_arn_region(monkeypatch):
    reference = (
        "arn:aws:secretsmanager:ap-northeast-1:297645381734:secret:"
        "task-platform/workspace/user/apex/123e4567-e89b-42d3-a456-426614174000-AbCd12"
    )
    client_calls: list[tuple[str, dict[str, str]]] = []

    class FakeSecretsManager:
        def get_secret_value(self, **kwargs: str) -> dict[str, str]:
            assert kwargs == {"SecretId": reference}
            return {"SecretString": json.dumps({"api_key": "apex-private-value"})}

    class FakeBoto3:
        @staticmethod
        def client(service_name: str, **kwargs: str) -> FakeSecretsManager:
            client_calls.append((service_name, kwargs))
            return FakeSecretsManager()

    monkeypatch.setitem(sys.modules, "boto3", FakeBoto3)

    assert agent_api_module._resolve_platform_credential_ref(reference) == (
        "apex-private-value"
    )
    assert client_calls == [
        ("secretsmanager", {"region_name": "ap-northeast-1"})
    ]


def _mode(path: Path) -> int:
    return stat.S_IMODE(path.lstat().st_mode)


def _adapter(store: AgentApiAccountStore) -> CloudRouterAdapter:
    adapter = store.registry.require("cloudrouter")
    assert isinstance(adapter, CloudRouterAdapter)
    return adapter


async def _add(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    models: dict[str, list[str]] | None = None,
) -> tuple[AgentApiAccountStore, object]:
    store = AgentApiAccountStore(tmp_path / "agent-api")
    monkeypatch.setattr(
        _adapter(store),
        "probe_models",
        AsyncMock(return_value=models or MODELS),
    )
    account = await store.add(
        "cloudrouter",
        "Primary API",
        "cr-private-value",
        group="research",
    )
    return store, account


@pytest.mark.asyncio
async def test_platform_reference_is_resolved_just_in_time_and_never_persisted(
    tmp_path, monkeypatch,
):
    reference = (
        "arn:aws:secretsmanager:ap-northeast-1:297645381734:secret:"
        "task-platform/workspace/user/apex/123e4567-e89b-42d3-a456-426614174000-AbCd12"
    )
    resolved: list[str] = []

    def resolver(value: str) -> str:
        resolved.append(value)
        return "apex-private-value"

    store = AgentApiAccountStore(
        tmp_path / "agent-api",
        credential_resolver=resolver,
    )
    adapter = store.registry.require("apex")
    monkeypatch.setattr(adapter, "probe_models", AsyncMock(return_value={"claude": [], "codex": ["gpt-5.4"]}))
    account = await store.add_reference("apex", "Production Apex", reference)

    account_root = store.root / account.id
    assert resolved == [reference]
    assert not (account_root / "api.key").exists()
    metadata = (account_root / "account.json").read_text()
    assert reference in metadata
    assert "apex-private-value" not in metadata
    assert account.credential_ref == reference
    assert account.public_dict()["credential_source"] == "platform_ref"

    assert store.read_api_key(account.id) == "apex-private-value"
    assert resolved == [reference, reference]

    reloaded = AgentApiAccountStore(store.root, credential_resolver=resolver)
    assert (await reloaded.get(account.id)).credential_ref == reference
    assert not (account_root / "api.key").exists()


@pytest.mark.asyncio
async def test_platform_reference_rejects_unbounded_or_duplicate_refs(tmp_path, monkeypatch):
    reference = (
        "arn:aws:secretsmanager:ap-northeast-1:297645381734:secret:"
        "task-platform/workspace/user/apex/123e4567-e89b-42d3-a456-426614174000"
    )
    store = AgentApiAccountStore(tmp_path / "agent-api", credential_resolver=lambda _ref: "secret")
    monkeypatch.setattr(
        store.registry.require("apex"),
        "probe_models",
        AsyncMock(return_value={"claude": [], "codex": ["gpt-5.4"]}),
    )

    with pytest.raises(ValueError, match="platform credential reference"):
        await store.add_reference("apex", "Bad", "arn:aws:secretsmanager:ap-northeast-1:297645381734:secret:anything")
    await store.add_reference("apex", "First", reference)
    with pytest.raises(AgentApiDuplicateKeyError):
        await store.add_reference("apex", "Second", reference)


def test_default_registry_exposes_cloudrouter_then_apex():
    registry = AgentApiProviderRegistry.default()

    assert registry.providers == ("cloudrouter", "apex")
    assert registry.require("cloudrouter").provider == "cloudrouter"
    assert registry.require("apex").provider == "apex"
    with pytest.raises(AgentApiUnsupportedProviderError):
        registry.require("apexrouter")


@pytest.mark.asyncio
async def test_model_catalog_preserves_valid_fingerprint_call_names(monkeypatch):
    adapter = CloudRouterAdapter()
    monkeypatch.setattr(adapter, "_request_json", AsyncMock(return_value={
        "data": [
            {"id": "5MHXZWKA/gpt-4o"},
            {"id": "CLAUDE01/claude-sonnet-4-6"},
        ],
    }))

    assert await adapter.probe_models("sk-private") == {
        "claude": ["CLAUDE01/claude-sonnet-4-6"],
        "codex": ["5MHXZWKA/gpt-4o"],
    }

    adapter._request_json.return_value = {
        "data": [{"id": "../gpt-4o"}],
    }
    with pytest.raises(AgentApiUpstreamError, match="invalid_models_response"):
        await adapter.probe_models("sk-private")


@pytest.mark.asyncio
async def test_account_is_private_provider_neutral_and_rest_safe(
    tmp_path, monkeypatch,
):
    store, account = await _add(tmp_path, monkeypatch)

    assert account.id == "cloudrouter-1"
    assert account.name == "Primary API"
    assert account.email == "Primary API"
    assert account.group == "research"
    assert account.auth_kind == "agent_api"
    assert account.api_provider == "cloudrouter"
    assert account.enabled is True
    assert account.supports_agent_type("claude")
    assert account.supports_agent_type("codex")
    assert not account.supports_agent_type("apex")
    assert account.supports_model("claude", "claude-opus-4-8")
    assert account.supports_model("codex", "o3")

    public = account.public_dict()
    encoded = json.dumps(public)
    assert public["has_api_key"] is True
    assert public["key_fingerprint"].startswith("sha256:")
    assert "cr-private-value" not in encoded
    assert "key" not in public
    assert "api_key" not in public
    assert public["endpoints"] == {
        "anthropic_base_url": CLOUDROUTER_ANTHROPIC_BASE_URL,
        "openai_base_url": CLOUDROUTER_OPENAI_BASE_URL,
        "models_url": CLOUDROUTER_MODELS_URL,
        "usage_url": CLOUDROUTER_USAGE_URL,
    }
    assert store.read_api_key(account.id) == "cr-private-value"
    assert "cr-private-value" not in repr(account)

    root = tmp_path / "agent-api"
    account_root = root / account.id
    assert _mode(root) == 0o700
    assert _mode(account_root) == 0o700
    assert _mode(account_root / "account.json") == 0o600
    assert _mode(account_root / "api.key") == 0o600
    assert "cr-private-value" not in (account_root / "account.json").read_text()

    reloaded = AgentApiAccountStore(root)
    listed = await reloaded.list()
    assert listed == [account]
    assert await reloaded.get(account.id) == account


@pytest.mark.asyncio
async def test_duplicate_provider_key_is_rejected_sequentially_and_concurrently(
    tmp_path, monkeypatch,
):
    store = AgentApiAccountStore(tmp_path / "agent-api")
    monkeypatch.setattr(
        _adapter(store),
        "probe_models",
        AsyncMock(return_value=MODELS),
    )

    first, second = await asyncio.gather(
        store.add("cloudrouter", "First", "same-private-key"),
        store.add("cloudrouter", "Second", "same-private-key"),
        return_exceptions=True,
    )

    results = (first, second)
    assert sum(not isinstance(result, Exception) for result in results) == 1
    assert sum(
        isinstance(result, AgentApiDuplicateKeyError)
        for result in results
    ) == 1
    with pytest.raises(AgentApiDuplicateKeyError):
        await store.add("cloudrouter", "Third", "same-private-key")
    assert len(await store.list()) == 1


@pytest.mark.asyncio
async def test_new_account_stays_pending_until_known_active_usage(
    tmp_path, monkeypatch,
):
    store, account = await _add(tmp_path, monkeypatch)
    adapter = _adapter(store)

    assert account.admission_pending is True
    assert store.availability_decision(account.id) == {
        "available": False,
        "known": True,
        "reason": "initial_usage_pending",
    }
    assert AgentApiAccountStore(store.root).availability_decision(
        account.id
    )["available"] is False

    monkeypatch.setattr(
        adapter,
        "fetch_usage",
        AsyncMock(side_effect=AgentApiUpstreamError("timeout")),
    )
    transient = await store.fetch_usage(account.id)
    assert transient["available"] is False
    assert transient["reason"] == "initial_usage_pending"
    assert (await store.get(account.id)).admission_pending is True

    adapter.fetch_usage.side_effect = None
    adapter.fetch_usage.return_value = {
        "account_id": account.id,
        "state": "active",
        "status": "active",
        "known": True,
        "available": True,
        "reason": "active",
        "mode": "wallet",
        "windows": [],
    }
    active = await store.fetch_usage(account.id, force=True)

    assert active["available"] is True
    assert (await store.get(account.id)).admission_pending is False
    assert AgentApiAccountStore(store.root).availability_decision(
        account.id
    ) == {
        "available": True,
        "known": False,
        "reason": "not_fetched",
    }


@pytest.mark.asyncio
async def test_duplicate_stored_fingerprint_fails_closed_on_restart(
    tmp_path, monkeypatch,
):
    store, account = await _add(tmp_path, monkeypatch)
    duplicate = store.root / "cloudrouter-2"
    shutil.copytree(account.root, duplicate)
    metadata_path = duplicate / "account.json"
    metadata = json.loads(metadata_path.read_text())
    metadata["id"] = "cloudrouter-2"
    metadata_path.write_text(json.dumps(metadata))
    os.chmod(metadata_path, 0o600)

    with pytest.raises(
        AgentApiStorageError,
        match="duplicate Agent API provider key fingerprint",
    ):
        AgentApiAccountStore(store.root)


@pytest.mark.asyncio
async def test_account_ids_are_never_reused_and_skip_durable_resource_ids(
    tmp_path, monkeypatch,
):
    store = AgentApiAccountStore(tmp_path / "agent-api")
    monkeypatch.setattr(
        _adapter(store),
        "probe_models",
        AsyncMock(return_value=MODELS),
    )
    first = await store.add("cloudrouter", "First", "first-key")
    assert first.id == "cloudrouter-1"
    assert await store.remove(first.id) is True

    second = await store.add(
        "cloudrouter",
        "Second",
        "second-key",
        excluded_ids={"cloudrouter-9"},
    )

    assert second.id == "cloudrouter-10"
    reloaded = AgentApiAccountStore(store.root)
    monkeypatch.setattr(
        _adapter(reloaded),
        "probe_models",
        AsyncMock(return_value=MODELS),
    )
    third = await reloaded.add("cloudrouter", "Third", "third-key")
    assert third.id == "cloudrouter-11"


@pytest.mark.asyncio
async def test_transaction_cleanup_persists_id_before_crash_boundary(
    tmp_path, monkeypatch,
):
    store, account = await _add(tmp_path, monkeypatch)
    transaction = store.root / f".{account.id}.remove-999"
    account.root.rename(transaction)
    (store.root / ".store.json").unlink()
    recover = store._recover_transaction_directories_sync

    def recover_then_crash(children):
        recover(children)
        raise RuntimeError("simulated crash after transaction cleanup")

    monkeypatch.setattr(
        store,
        "_recover_transaction_directories_sync",
        recover_then_crash,
    )
    with pytest.raises(RuntimeError, match="simulated crash"):
        store._reload_sync()

    assert not transaction.exists()
    state = json.loads((store.root / ".store.json").read_text())
    assert state["high_water"]["cloudrouter"] == 1

    restarted = AgentApiAccountStore(store.root)
    monkeypatch.setattr(
        _adapter(restarted),
        "probe_models",
        AsyncMock(return_value=MODELS),
    )
    replacement = await restarted.add(
        "cloudrouter",
        "Replacement",
        "replacement-key",
    )
    assert replacement.id == "cloudrouter-2"


@pytest.mark.asyncio
async def test_model_projection_gates_agent_types_independently(
    tmp_path, monkeypatch,
):
    _store, account = await _add(
        tmp_path,
        monkeypatch,
        models={"claude": ["claude-haiku-4-5-20251001"], "codex": []},
    )

    assert account.supports_agent_type("claude")
    assert not account.supports_agent_type("codex")
    assert account.supports_model("claude", "claude-haiku-4-5")
    assert account.supports_model("claude", "claude-haiku-4-5[1m]")
    assert not account.supports_model("claude", "claude-haiku-4")


@pytest.mark.asyncio
async def test_refresh_reprobes_models_without_replacing_key(tmp_path, monkeypatch):
    store, account = await _add(tmp_path, monkeypatch)
    probe = AsyncMock(return_value={"claude": [], "codex": ["gpt-5.5"]})
    monkeypatch.setattr(_adapter(store), "probe_models", probe)

    refreshed = await store.refresh(account.id)

    assert refreshed.models == {"claude": [], "codex": ["gpt-5.5"]}
    probe.assert_awaited_once_with("cr-private-value")
    assert (account.root / "api.key").read_text() == "cr-private-value"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("probe_result", "expected_reason"),
    [
        (AgentApiUpstreamError("invalid_api_key", 401), "invalid_api_key"),
        ({"claude": [], "codex": []}, "no_supported_models"),
        (
            {"claude": ["claude-valid\ninjected"], "codex": []},
            "invalid_models_response",
        ),
    ],
)
async def test_deterministic_model_refresh_failure_benches_cached_account(
    tmp_path, monkeypatch, probe_result, expected_reason,
):
    store, account = await _add(tmp_path, monkeypatch)
    adapter = _adapter(store)
    monkeypatch.setattr(adapter, "fetch_usage", AsyncMock(return_value={
        "account_id": account.id,
        "state": "active",
        "status": "active",
        "known": True,
        "available": True,
        "reason": "active",
        "mode": "wallet",
        "windows": [],
    }))
    await store.fetch_usage(account.id, force=True)
    if isinstance(probe_result, Exception):
        probe = AsyncMock(side_effect=probe_result)
    else:
        probe = AsyncMock(return_value=probe_result)
    monkeypatch.setattr(adapter, "probe_models", probe)

    with pytest.raises(AgentApiUpstreamError):
        await store.refresh(account.id)

    assert store.availability_decision(account.id) == {
        "available": False,
        "known": True,
        "reason": expected_reason,
    }
    assert (await store.get(account.id)).models == account.models


@pytest.mark.asyncio
async def test_transient_model_refresh_failure_preserves_last_known_usage(
    tmp_path, monkeypatch,
):
    store, account = await _add(tmp_path, monkeypatch)
    adapter = _adapter(store)
    monkeypatch.setattr(adapter, "fetch_usage", AsyncMock(return_value={
        "account_id": account.id,
        "state": "active",
        "status": "active",
        "known": True,
        "available": True,
        "reason": "active",
        "mode": "wallet",
        "windows": [],
    }))
    await store.fetch_usage(account.id, force=True)
    monkeypatch.setattr(
        adapter,
        "probe_models",
        AsyncMock(side_effect=AgentApiUpstreamError("timeout")),
    )

    with pytest.raises(AgentApiUpstreamError, match="timeout"):
        await store.refresh(account.id)

    assert store.availability_decision(account.id) == {
        "available": True,
        "known": True,
        "reason": "active",
    }


@pytest.mark.asyncio
async def test_deterministic_model_failure_survives_restart_until_full_refresh(
    tmp_path, monkeypatch,
):
    store, account = await _add(tmp_path, monkeypatch)
    adapter = _adapter(store)
    active_usage = {
        "account_id": account.id,
        "state": "active",
        "status": "active",
        "known": True,
        "available": True,
        "reason": "active",
        "mode": "wallet",
        "windows": [],
    }
    monkeypatch.setattr(
        adapter,
        "fetch_usage",
        AsyncMock(return_value=active_usage),
    )
    await store.fetch_usage(account.id, force=True)
    monkeypatch.setattr(
        adapter,
        "probe_models",
        AsyncMock(return_value={"claude": [], "codex": []}),
    )

    with pytest.raises(AgentApiUpstreamError, match="no_supported_models"):
        await store.refresh(account.id)
    # A later runtime-auth observation may strengthen the reason, but must not
    # downgrade the requirement for a successful model+usage refresh.
    await store.mark_runtime_unavailable(
        account.id,
        "runtime_invalid_api_key",
    )

    restarted = AgentApiAccountStore(store.root)
    restarted_adapter = _adapter(restarted)
    monkeypatch.setattr(
        restarted_adapter,
        "fetch_usage",
        AsyncMock(return_value=active_usage),
    )
    assert restarted.availability_decision(account.id)["available"] is False
    usage_only = await restarted.fetch_usage(account.id, force=True)
    assert usage_only["available"] is False
    assert usage_only["reason"] == "runtime_invalid_api_key"

    monkeypatch.setattr(
        restarted_adapter,
        "probe_models",
        AsyncMock(return_value=MODELS),
    )
    await restarted.refresh(account.id)
    recovered = await restarted.fetch_usage(
        account.id,
        force=True,
        allow_model_tombstone_clear=True,
    )
    assert recovered["available"] is True
    assert restarted.availability_decision(account.id)["available"] is True


@pytest.mark.asyncio
async def test_invalid_key_or_failed_probe_creates_no_account(tmp_path, monkeypatch):
    store = AgentApiAccountStore(tmp_path / "agent-api")
    adapter = _adapter(store)

    with pytest.raises(ValueError, match="API key"):
        await store.add("cloudrouter", "Bad", "x" * (MAX_API_KEY_BYTES + 1))
    monkeypatch.setattr(
        adapter,
        "probe_models",
        AsyncMock(side_effect=AgentApiUpstreamError("invalid_api_key", 401)),
    )
    with pytest.raises(AgentApiUpstreamError):
        await store.add("cloudrouter", "Bad", "bad-key")

    assert await store.list() == []
    assert list((tmp_path / "agent-api").iterdir()) == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "models",
    [
        {
            "claude": ["claude-" + "x" * MAX_AGENT_API_MODEL_ID_LENGTH],
            "codex": [],
        },
        {
            "claude": ["claude-valid\ninjected"],
            "codex": [],
        },
        {
            "claude": ["claude-\ud800"],
            "codex": [],
        },
        {
            "claude": [
                f"claude-model-{index}"
                for index in range(MAX_AGENT_API_MODELS + 1)
            ],
            "codex": [],
        },
    ],
)
async def test_oversized_or_unsafe_model_catalog_is_never_published(
    tmp_path, monkeypatch, models,
):
    store = AgentApiAccountStore(tmp_path / "agent-api")
    monkeypatch.setattr(
        _adapter(store),
        "probe_models",
        AsyncMock(return_value=models),
    )

    with pytest.raises(AgentApiUpstreamError, match="invalid_models_response"):
        await store.add("cloudrouter", "Bad catalog", "private-key")

    assert await store.list() == []
    assert list(store.root.iterdir()) == []


@pytest.mark.asyncio
async def test_unsafe_symlink_type_and_modes_fail_closed(tmp_path, monkeypatch):
    store, account = await _add(tmp_path, monkeypatch)
    metadata = account.root / "account.json"
    outside = tmp_path / "outside.json"
    outside.write_text(metadata.read_text())
    metadata.unlink()
    metadata.symlink_to(outside)

    with pytest.raises(AgentApiStorageError):
        AgentApiAccountStore(store.root)

    metadata.unlink()
    metadata.write_text(outside.read_text())
    os.chmod(metadata, 0o600)
    key_path = account.root / "api.key"
    os.chmod(key_path, 0o644)
    with pytest.raises(AgentApiStorageError):
        AgentApiAccountStore(store.root)

    os.chmod(key_path, 0o600)
    os.chmod(account.root, 0o755)
    with pytest.raises(AgentApiStorageError):
        AgentApiAccountStore(store.root)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("version", True),
        ("enabled", "false"),
        ("name", "x" * 101),
        ("created_at", float("nan")),
        ("updated_at", 10**4000),
        ("unexpected", "field"),
    ],
)
async def test_tampered_metadata_schema_fails_closed(
    tmp_path, monkeypatch, field, value,
):
    store, account = await _add(tmp_path, monkeypatch)
    metadata_path = account.root / "account.json"
    metadata = json.loads(metadata_path.read_text())
    metadata[field] = value
    metadata_path.write_text(json.dumps(metadata))

    with pytest.raises(AgentApiStorageError):
        AgentApiAccountStore(store.root)


@pytest.mark.asyncio
async def test_cloudrouter_keys_must_be_visible_ascii(tmp_path, monkeypatch):
    store = AgentApiAccountStore(tmp_path / "agent-api")
    probe = AsyncMock(return_value=MODELS)
    monkeypatch.setattr(_adapter(store), "probe_models", probe)

    for key in ("clé", "key-🔑", "key\u0085value"):
        with pytest.raises(ValueError, match="API key"):
            await store.add("cloudrouter", "Invalid key", key)
    probe.assert_not_awaited()
    assert list(store.root.iterdir()) == []

    store, account = await _add(tmp_path / "stored", monkeypatch)
    (account.root / "api.key").write_bytes("clé".encode())
    with pytest.raises(AgentApiStorageError, match="invalid Agent API key"):
        AgentApiAccountStore(store.root)

    with pytest.raises(ValueError, match="API key"):
        await CloudRouterAdapter().probe_models("clé")


@pytest.mark.asyncio
async def test_unknown_ids_do_not_allocate_unbounded_operation_locks(tmp_path):
    store = AgentApiAccountStore(tmp_path / "agent-api")

    for index in range(100):
        with pytest.raises(AgentApiAccountNotFoundError):
            await store.fetch_usage(f"cloudrouter-{index + 1}")

    assert store._usage_fetch_locks == {}


@pytest.mark.asyncio
async def test_usage_active_exhausted_unlimited_and_auth_failure(
    tmp_path, monkeypatch,
):
    store, account = await _add(tmp_path, monkeypatch)
    adapter = _adapter(store)

    monkeypatch.setattr(adapter, "_request_json", AsyncMock(return_value={
        "mode": "quota_limited",
        "status": "active",
        "quota": {"limit": 100, "used": 100, "remaining": -1},
        "rate_limits": [{
            "window": "7d", "limit": 100, "used": 100, "remaining": -1,
        }],
    }))
    unlimited = await store.fetch_usage(account.id, force=True)
    assert unlimited["state"] == "active"
    assert unlimited["known"] is True
    assert unlimited["available"] is True
    assert unlimited["quota"]["unlimited"] is True
    assert unlimited["windows"][0]["unlimited"] is True

    adapter._request_json.return_value = {
        "mode": "wallet", "status": "active", "balance": 0,
    }
    exhausted = await store.fetch_usage(account.id, force=True)
    assert exhausted["state"] == "exhausted"
    assert exhausted["known"] is True
    assert exhausted["available"] is False

    for status_code, reason in ((401, "invalid_api_key"), (403, "forbidden")):
        adapter._request_json.side_effect = AgentApiUpstreamError(
            reason,
            status_code,
        )
        unavailable = await store.fetch_usage(account.id, force=True)
        assert unavailable["state"] == "unavailable"
        assert unavailable["known"] is True
        assert unavailable["available"] is False
        assert unavailable["reason"] == reason


@pytest.mark.asyncio
async def test_unrestricted_zero_balance_remains_available(
    tmp_path, monkeypatch,
):
    store, account = await _add(tmp_path, monkeypatch)
    adapter = _adapter(store)
    monkeypatch.setattr(adapter, "_request_json", AsyncMock(return_value={
        "balance": 0,
        "isValid": True,
        "mode": "unrestricted",
        "planName": "钱包余额",
        "remaining": 0,
        "unit": "USD",
    }))

    snapshot = await store.fetch_usage(account.id, force=True)

    assert snapshot["mode"] == "unrestricted"
    assert snapshot["state"] == "active"
    assert snapshot["available"] is True
    assert snapshot["currency"] == "USD"
    assert snapshot["balance"] == 0
    assert snapshot["remaining"] == 0
    assert "balance_unlimited" not in snapshot
    assert "remaining_unlimited" not in snapshot
    assert store.availability_decision(account.id)["available"] is True


@pytest.mark.asyncio
async def test_unrestricted_explicit_quota_exhaustion_still_blocks_admission(
    tmp_path, monkeypatch,
):
    store, account = await _add(tmp_path, monkeypatch)
    adapter = _adapter(store)
    monkeypatch.setattr(adapter, "_request_json", AsyncMock(return_value={
        "balance": 0,
        "isValid": True,
        "mode": "unrestricted",
        "remaining": 0,
        "quota": {"limit": 100, "used": 100, "remaining": 0},
    }))

    snapshot = await store.fetch_usage(account.id, force=True)

    assert snapshot["mode"] == "unrestricted"
    assert snapshot["state"] == "exhausted"
    assert snapshot["available"] is False
    assert store.availability_decision(account.id)["available"] is False


@pytest.mark.asyncio
async def test_wallet_sentinels_are_independent_and_usage_is_allowlisted(
    tmp_path, monkeypatch,
):
    store, account = await _add(tmp_path, monkeypatch)
    adapter = _adapter(store)
    monkeypatch.setattr(adapter, "_request_json", AsyncMock(return_value={
        "mode": "wallet",
        "status": "active",
        "balance": 0,
        "remaining": -1,
        "usage": {
            "today": {
                "requests": 3,
                "total_tokens": 42,
                "credential": "must-never-reach-rest",
            },
            "rpm": 2,
            "model_stats": [{
                "model": "gpt-5.4",
                "requests": 3,
                "api_key": "must-never-reach-rest",
            }],
            "arbitrary": {"api_key": "must-never-reach-rest"},
        },
    }))

    usage = await store.fetch_usage(account.id, force=True)

    # -1 applies only to the exact field carrying it; a depleted balance still
    # makes the wallet unavailable.
    assert usage["state"] == "exhausted"
    assert usage["available"] is False
    assert usage["remaining_unlimited"] is True
    assert usage["usage"] == {
        "today": {"requests": 3, "total_tokens": 42},
        "rpm": 2,
    }
    assert usage["model_stats"] == [{
        "model": "gpt-5.4",
        "requests": 3,
    }]
    assert "must-never-reach-rest" not in json.dumps(usage)


@pytest.mark.asyncio
async def test_invalid_usage_shapes_fail_closed_and_zero_limit_is_exhausted(
    tmp_path, monkeypatch,
):
    store, account = await _add(tmp_path, monkeypatch)
    adapter = _adapter(store)
    request = AsyncMock(return_value={})
    monkeypatch.setattr(adapter, "_request_json", request)

    for payload in (
        {},
        {"mode": "wallet", "status": "active"},
        {"mode": "mystery", "status": "active", "balance": 10},
        {"mode": "wallet", "status": "mystery", "balance": 10},
        {
            "mode": "subscription",
            "status": "active",
            "subscription": {"foo": "bar"},
        },
        {
            "mode": "quota_limited",
            "status": "active",
            "rate_limits": [{}],
        },
        {
            "mode": "quota_limited",
            "status": "active",
            "quota": {"limit": "1e100000", "used": 0},
        },
        {
            "mode": "quota_limited",
            "status": "active",
            "quota": {"limit": "1e-1000000", "used": 1},
        },
        {
            "mode": "quota_limited",
            "status": "active",
            "quota": {"used": 999},
        },
        {
            "mode": "quota_limited",
            "status": "active",
            "quota": {"limit": 100},
        },
        {
            "mode": "quota_limited",
            "status": "active",
            "rate_limits": [{"window": "7d", "used": 999}],
        },
        {
            "mode": "subscription",
            "status": "active",
            "subscription": {
                "daily_usage_credits": "malformed",
                "weekly_usage_credits": 1,
                "weekly_limit_credits": 10,
            },
        },
        {
            "mode": "subscription",
            "status": "active",
            "subscription": {
                "daily_usage_credits": 10,
                "daily_limit_credits": 10,
                "weekly_usage_usd": 1,
                "weekly_limit_usd": 10,
            },
        },
    ):
        request.return_value = payload
        snapshot = await store.fetch_usage(account.id, force=True)
        assert snapshot["state"] == "unavailable"
        assert snapshot["available"] is False
        assert snapshot["known"] is True
        assert snapshot["reason"] == "invalid_usage_response"

    for status in ("disabled", "suspended", "invalid"):
        request.return_value = {"status": status}
        snapshot = await store.fetch_usage(account.id, force=True)
        assert snapshot["state"] == "unavailable"
        assert snapshot["available"] is False
        assert snapshot["reason"] == status

    request.return_value = {
        "mode": "quota_limited",
        "status": "active",
        "quota": {"limit": 0},
    }
    exhausted = await store.fetch_usage(account.id, force=True)
    assert exhausted["state"] == "exhausted"
    assert exhausted["available"] is False

    for payload in (
        {
            "mode": "quota_limited",
            "status": "active",
            "quota": {"remaining": 0},
        },
        {
            "mode": "quota_limited",
            "status": "active",
            "rate_limits": [{"window": "1d", "remaining": -2}],
        },
    ):
        request.return_value = payload
        exhausted = await store.fetch_usage(account.id, force=True)
        assert exhausted["state"] == "exhausted"
        assert exhausted["available"] is False

    for payload in (
        {
            "mode": "quota_limited",
            "status": "active",
            "quota": {"used": 1, "limit": 10},
            "remaining": 0,
        },
        {
            "mode": "subscription",
            "status": "active",
            "remaining": 0,
            "subscription": {
                "daily_usage_credits": 1,
                "daily_limit_credits": 10,
            },
        },
        {
            "mode": "subscription",
            "status": "active",
            "balance": 5,
            "subscription": {
                "daily_usage_credits": 10,
                "daily_limit_credits": 10,
                "plan_price_usd": 20,
            },
        },
    ):
        request.return_value = payload
        exhausted = await store.fetch_usage(account.id, force=True)
        assert exhausted["state"] == "exhausted"
        assert exhausted["available"] is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expiry",
    [
        {"expires_at": "2020-01-01T00:00:00Z"},
        {"daysUntilExpiry": 0},
        {"daysUntilExpiry": -1},
    ],
)
async def test_usage_expiry_fields_bench_unrestricted_key(
    tmp_path, monkeypatch, expiry,
):
    store, account = await _add(tmp_path, monkeypatch)
    adapter = _adapter(store)
    monkeypatch.setattr(adapter, "_request_json", AsyncMock(return_value={
        "mode": "unrestricted",
        "isValid": True,
        "remaining": 5,
        "subscription": {
            "daily_usage_usd": 1,
            "daily_limit_usd": 5,
            **expiry,
        },
    }))

    snapshot = await store.fetch_usage(account.id, force=True)

    assert snapshot["state"] == "expired"
    assert snapshot["available"] is False
    assert snapshot["reason"] == "expired"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("top_level", "subscription_expiry"),
    [
        (
            {
                "expires_at": "2099-01-01T00:00:00Z",
                "expiry": "2020-01-01T00:00:00Z",
            },
            {},
        ),
        (
            {
                "days_until_expiry": 10,
                "daysUntilExpiry": 0,
            },
            {},
        ),
        (
            {"expires_at": "2099-01-01T00:00:00Z"},
            {"expiresAt": "2020-01-01T00:00:00Z"},
        ),
    ],
)
async def test_any_expired_alias_or_scope_benches_key(
    tmp_path,
    monkeypatch,
    top_level,
    subscription_expiry,
):
    store, account = await _add(tmp_path, monkeypatch)
    adapter = _adapter(store)
    monkeypatch.setattr(adapter, "_request_json", AsyncMock(return_value={
        "mode": "unrestricted",
        "isValid": True,
        "remaining": 5,
        **top_level,
        "subscription": {
            "daily_usage_usd": 1,
            "daily_limit_usd": 5,
            **subscription_expiry,
        },
    }))

    snapshot = await store.fetch_usage(account.id, force=True)

    assert snapshot["state"] == "expired"
    assert snapshot["available"] is False
    assert snapshot["reason"] == "expired"


@pytest.mark.asyncio
async def test_usage_fetches_are_per_account_and_runtime_bench_wins_race(
    tmp_path, monkeypatch,
):
    store, first = await _add(tmp_path, monkeypatch)
    adapter = _adapter(store)
    monkeypatch.setattr(
        adapter,
        "probe_models",
        AsyncMock(return_value=MODELS),
    )
    second = await store.add(
        "cloudrouter",
        "Secondary API",
        "cr-second-private-value",
    )

    entered: set[str] = set()
    both_entered = asyncio.Event()
    release = asyncio.Event()

    async def delayed_usage(account_id, _api_key):
        entered.add(account_id)
        if len(entered) == 2:
            both_entered.set()
        await release.wait()
        return {
            "account_id": account_id,
            "state": "active",
            "status": "active",
            "known": True,
            "available": True,
            "reason": "active",
            "mode": "wallet",
            "windows": [],
        }

    monkeypatch.setattr(adapter, "fetch_usage", delayed_usage)
    first_fetch = asyncio.create_task(store.fetch_usage(first.id, force=True))
    second_fetch = asyncio.create_task(store.fetch_usage(second.id, force=True))
    await asyncio.wait_for(both_entered.wait(), timeout=1)

    await store.mark_runtime_unavailable(
        first.id,
        "runtime_invalid_api_key",
    )
    release.set()
    first_result, second_result = await asyncio.gather(
        first_fetch,
        second_fetch,
    )

    assert first_result["available"] is False
    assert first_result["reason"] == "runtime_invalid_api_key"
    assert store.availability_decision(first.id)["available"] is False
    assert second_result["available"] is True


@pytest.mark.asyncio
async def test_transient_unknown_cannot_resurrect_known_dead_account(
    tmp_path, monkeypatch,
):
    store, account = await _add(tmp_path, monkeypatch)
    adapter = _adapter(store)
    request = AsyncMock(return_value={
        "mode": "wallet",
        "status": "active",
        "balance": 10,
    })
    monkeypatch.setattr(adapter, "_request_json", request)
    assert (await store.fetch_usage(account.id, force=True))["available"] is True

    request.return_value = {
        "mode": "quota_limited",
        "status": "quota_exhausted",
        "quota": {"limit": 10, "used": 10, "remaining": 0},
    }
    assert (await store.fetch_usage(account.id, force=True))["available"] is False

    request.side_effect = AgentApiUpstreamError(
        "upstream_unavailable", 503,
    )
    unknown = await store.fetch_usage(account.id, force=True)

    assert unknown["state"] == "unknown"
    assert unknown["known"] is False
    assert unknown["available"] is True
    assert unknown["last_known_available"] is False
    assert store.availability_decision(account.id) == {
        "available": False,
        "known": True,
        "reason": "quota_exhausted",
    }


@pytest.mark.asyncio
async def test_last_known_unavailable_survives_restart_and_timeout(
    tmp_path, monkeypatch,
):
    store, account = await _add(tmp_path, monkeypatch)
    request = AsyncMock(return_value={
        "mode": "wallet",
        "status": "active",
        "balance": 10,
    })
    monkeypatch.setattr(_adapter(store), "_request_json", request)
    assert (await store.fetch_usage(account.id, force=True))["available"] is True

    request.return_value = {
        "mode": "quota_limited",
        "status": "quota_exhausted",
        "quota": {"limit": 10, "used": 10, "remaining": 0},
    }
    exhausted = await store.fetch_usage(account.id, force=True)
    assert exhausted["available"] is False

    marker = account.root / "last-known-unavailable.json"
    marker_value = json.loads(marker.read_text())
    assert _mode(marker) == 0o600
    assert marker_value["account_id"] == account.id
    assert marker_value["key_fingerprint"] == account.key_fingerprint
    assert marker_value["reason"] == "quota_exhausted"
    assert "cr-private-value" not in marker.read_text()

    restarted = AgentApiAccountStore(store.root)
    retry = AsyncMock(side_effect=AgentApiUpstreamError("timeout"))
    monkeypatch.setattr(_adapter(restarted), "_request_json", retry)

    # This record is a fallback, not a sticky runtime tombstone: a normal
    # allocator refresh still reaches the provider after restart.
    unknown = await restarted.fetch_usage(account.id)
    retry.assert_awaited_once()
    assert unknown["state"] == "unknown"
    assert unknown["known"] is False
    assert unknown["available"] is True
    assert unknown["last_known_available"] is False
    assert unknown["last_known_reason"] == "quota_exhausted"
    assert restarted.availability_decision(account.id) == {
        "available": False,
        "known": True,
        "reason": "quota_exhausted",
    }
    assert marker.exists()


@pytest.mark.asyncio
async def test_runtime_hard_limit_is_durable_but_reprobeable(
    tmp_path,
    monkeypatch,
):
    store, account = await _add(tmp_path, monkeypatch)

    await store.mark_runtime_quota_unavailable(
        account.id,
        "runtime_rate_limited",
    )

    marker = account.root / "last-known-unavailable.json"
    assert marker.exists()
    assert store.availability_decision(account.id) == {
        "available": False,
        "known": True,
        "reason": "runtime_rate_limited",
    }
    restarted = AgentApiAccountStore(store.root)
    active_probe = AsyncMock(return_value={
        "mode": "wallet",
        "status": "active",
        "balance": 10,
    })
    monkeypatch.setattr(
        _adapter(restarted),
        "_request_json",
        active_probe,
    )

    active = await restarted.fetch_usage(account.id)

    assert active["available"] is True
    active_probe.assert_awaited_once()
    assert not marker.exists()


@pytest.mark.asyncio
async def test_successful_active_usage_clears_last_known_unavailable(
    tmp_path, monkeypatch,
):
    store, account = await _add(tmp_path, monkeypatch)
    request = AsyncMock(return_value={
        "mode": "wallet",
        "status": "active",
        "balance": 10,
    })
    monkeypatch.setattr(_adapter(store), "_request_json", request)
    await store.fetch_usage(account.id, force=True)
    request.return_value = {
        "mode": "quota_limited",
        "status": "quota_exhausted",
        "quota": {"limit": 10, "used": 10, "remaining": 0},
    }
    await store.fetch_usage(account.id, force=True)

    marker = account.root / "last-known-unavailable.json"
    assert marker.exists()
    restarted = AgentApiAccountStore(store.root)
    active_probe = AsyncMock(return_value={
        "mode": "wallet",
        "status": "active",
        "balance": 10,
    })
    monkeypatch.setattr(
        _adapter(restarted),
        "_request_json",
        active_probe,
    )

    active = await restarted.fetch_usage(account.id)

    assert active["available"] is True
    active_probe.assert_awaited_once()
    assert not marker.exists()
    assert restarted.availability_decision(account.id) == {
        "available": True,
        "known": True,
        "reason": "active",
    }
    assert AgentApiAccountStore(store.root).availability_decision(
        account.id
    ) == {
        "available": True,
        "known": False,
        "reason": "not_fetched",
    }


@pytest.mark.asyncio
async def test_last_known_unavailable_is_bound_to_key_fingerprint(
    tmp_path, monkeypatch,
):
    store, account = await _add(tmp_path, monkeypatch)
    request = AsyncMock(return_value={
        "mode": "quota_limited",
        "status": "quota_exhausted",
        "quota": {"limit": 10, "used": 10, "remaining": 0},
    })
    monkeypatch.setattr(_adapter(store), "_request_json", request)
    await store.fetch_usage(account.id, force=True)

    marker = account.root / "last-known-unavailable.json"
    value = json.loads(marker.read_text())
    value["key_fingerprint"] = "sha256:" + ("0" * 64)
    marker.write_text(json.dumps(value))

    with pytest.raises(
        AgentApiStorageError,
        match="invalid Agent API last-known usage state",
    ):
        AgentApiAccountStore(store.root)


@pytest.mark.asyncio
async def test_runtime_auth_failure_benches_key_until_operator_refresh(
    tmp_path, monkeypatch,
):
    store, account = await _add(tmp_path, monkeypatch)

    await store.mark_runtime_unavailable(
        account.id,
        "runtime_invalid_api_key",
    )

    assert store.availability_decision(account.id) == {
        "available": False,
        "known": True,
        "reason": "runtime_invalid_api_key",
    }
    assert store.usage_snapshot(account.id)["available"] is False
    tombstone = account.root / "runtime-unavailable.json"
    assert tombstone.exists()
    assert _mode(tombstone) == 0o600
    assert "cr-private-value" not in tombstone.read_text()

    reloaded = AgentApiAccountStore(store.root, quota_cache_ttl=0)
    adapter = _adapter(reloaded)
    fetch = AsyncMock(return_value={
        "account_id": account.id,
        "state": "active",
        "status": "active",
        "known": True,
        "available": True,
        "reason": "active",
        "mode": "wallet",
        "windows": [],
    })
    monkeypatch.setattr(adapter, "fetch_usage", fetch)

    # Scheduler/allocator reads are not operator refreshes and cannot revive
    # the key, even after cache expiry or Manager restart.
    ordinary = await reloaded.fetch_usage(account.id)
    assert ordinary["available"] is False
    assert ordinary["reason"] == "runtime_invalid_api_key"
    fetch.assert_not_awaited()

    # A transient explicit probe also retains the durable bench.
    fetch.side_effect = AgentApiUpstreamError("timeout")
    transient = await reloaded.fetch_usage(account.id, force=True)
    assert transient["available"] is False
    assert tombstone.exists()

    # Only a successful explicit provider refresh clears it.
    fetch.side_effect = None
    refreshed = await reloaded.fetch_usage(account.id, force=True)
    assert refreshed["available"] is True
    assert not tombstone.exists()
    assert AgentApiAccountStore(store.root).availability_decision(
        account.id
    ) == {
        "available": True,
        "known": False,
        "reason": "not_fetched",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation",
    [
        lambda value: value.update(version=True),
        lambda value: value.update(unexpected="field"),
        lambda value: value.update(reason="\ud800"),
    ],
)
async def test_runtime_tombstone_schema_is_exact(
    tmp_path,
    monkeypatch,
    mutation,
):
    store, account = await _add(tmp_path, monkeypatch)
    await store.mark_runtime_unavailable(
        account.id,
        "runtime_invalid_api_key",
    )
    tombstone = account.root / "runtime-unavailable.json"
    value = json.loads(tombstone.read_text())
    mutation(value)
    tombstone.write_text(json.dumps(value))

    with pytest.raises(
        AgentApiStorageError,
        match="invalid Agent API runtime tombstone",
    ):
        AgentApiAccountStore(store.root)


@pytest.mark.asyncio
async def test_store_high_water_schema_rejects_bool_version_and_extra_fields(
    tmp_path,
    monkeypatch,
):
    store, _account = await _add(tmp_path, monkeypatch)
    state_path = store.root / ".store.json"
    state = json.loads(state_path.read_text())
    state["version"] = True
    state["unexpected"] = "field"
    state_path.write_text(json.dumps(state))

    with pytest.raises(
        AgentApiStorageError,
        match="invalid Agent API store state",
    ):
        AgentApiAccountStore(store.root)


@pytest.mark.asyncio
async def test_cloudrouter_requests_are_bounded_bearer_get_and_no_redirects(
    monkeypatch,
):
    captured = {}

    class Response:
        status_code = 200

        async def aiter_bytes(self):
            yield json.dumps({"data": [{"id": "claude-opus-4-8"}]}).encode()

    class Stream:
        async def __aenter__(self):
            return Response()

        async def __aexit__(self, *_args):
            return False

    class Client:
        def __init__(self, **kwargs):
            captured.update(kwargs)

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        def stream(self, method, url, *, headers):
            captured.update({"method": method, "url": url, "headers": headers})
            return Stream()

    monkeypatch.setattr("elastic_agent.core.cloudrouter.httpx.AsyncClient", Client)
    adapter = CloudRouterAdapter()

    models = await adapter.probe_models("cr-private")

    assert models == {"claude": ["claude-opus-4-8"], "codex": []}
    assert captured["follow_redirects"] is False
    assert captured["timeout"] is not None
    assert captured["method"] == "GET"
    assert captured["url"] == CLOUDROUTER_MODELS_URL
    assert captured["headers"] == {
        "Authorization": "Bearer cr-private",
        "Accept": "application/json",
    }


@pytest.mark.asyncio
async def test_cloudrouter_wall_clock_timeout_cancels_drip_response(
    monkeypatch,
):
    stream_closed = asyncio.Event()

    class Response:
        status_code = 200

        async def aiter_bytes(self):
            try:
                while True:
                    yield b" "
                    await asyncio.sleep(0.001)
            finally:
                stream_closed.set()

    class Stream:
        async def __aenter__(self):
            return Response()

        async def __aexit__(self, *_args):
            return False

    class Client:
        def __init__(self, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        def stream(self, *_args, **_kwargs):
            return Stream()

    monkeypatch.setattr("elastic_agent.core.cloudrouter.httpx.AsyncClient", Client)
    adapter = CloudRouterAdapter(total_timeout_seconds=0.01)

    with pytest.raises(AgentApiUpstreamError, match="timeout"):
        await asyncio.wait_for(adapter.probe_models("key"), timeout=0.2)

    assert stream_closed.is_set()


@pytest.mark.asyncio
async def test_cloudrouter_redirect_and_large_response_are_rejected(monkeypatch):
    response = type("Response", (), {"status_code": 302})()

    async def aiter_bytes():
        yield b"{}"

    response.aiter_bytes = aiter_bytes

    class Stream:
        async def __aenter__(self):
            return response

        async def __aexit__(self, *_args):
            return False

    class Client:
        def __init__(self, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        def stream(self, *_args, **_kwargs):
            return Stream()

    monkeypatch.setattr("elastic_agent.core.cloudrouter.httpx.AsyncClient", Client)
    adapter = CloudRouterAdapter()
    with pytest.raises(AgentApiUpstreamError, match="unexpected_redirect"):
        await adapter.probe_models("key")

    response.status_code = 200

    async def too_large():
        yield b"x" * (MAX_API_RESPONSE_BYTES + 1)

    response.aiter_bytes = too_large
    with pytest.raises(AgentApiUpstreamError, match="response_too_large"):
        await adapter.probe_models("key")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [
        b'{"value":' + (b"9" * 5000) + b"}",
    ],
)
async def test_cloudrouter_pathological_json_is_normalized_to_upstream_error(
    monkeypatch, payload,
):
    class Response:
        status_code = 200

        async def aiter_bytes(self):
            yield payload

    class Stream:
        async def __aenter__(self):
            return Response()

        async def __aexit__(self, *_args):
            return False

    class Client:
        def __init__(self, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        def stream(self, *_args, **_kwargs):
            return Stream()

    monkeypatch.setattr(
        "elastic_agent.core.cloudrouter.httpx.AsyncClient",
        Client,
    )

    with pytest.raises(AgentApiUpstreamError, match="invalid_json"):
        await CloudRouterAdapter().probe_models("key")


@pytest.mark.asyncio
async def test_cloudrouter_json_parser_exception_is_normalized(monkeypatch):
    class Response:
        status_code = 200

        async def aiter_bytes(self):
            yield b"{}"

    class Stream:
        async def __aenter__(self):
            return Response()

        async def __aexit__(self, *_args):
            return False

    class Client:
        def __init__(self, **_kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        def stream(self, *_args, **_kwargs):
            return Stream()

    monkeypatch.setattr(
        "elastic_agent.core.cloudrouter.httpx.AsyncClient",
        Client,
    )
    def fail_json_parse(_payload):
        raise RecursionError("too deep")

    monkeypatch.setattr(
        "elastic_agent.core.cloudrouter.json.loads",
        fail_json_parse,
    )

    with pytest.raises(AgentApiUpstreamError, match="invalid_json"):
        await CloudRouterAdapter().probe_models("key")
