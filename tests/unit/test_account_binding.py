"""Durable account -> EIP bindings and per-job account leases."""

from __future__ import annotations

import asyncio
import json
import stat
import time

import pytest

from elastic_agent.core.account_binding import (
    AccountBinding,
    AccountBindingStore,
    AccountLease,
    BindingsConfig,
    BindingState,
    BindingStoreCorruptError,
    LeaseConflictError,
    LeaseState,
)

pytestmark = pytest.mark.asyncio


def _store(tmp_path):
    return AccountBindingStore(str(tmp_path / "bindings.json"))


async def test_empty_initially(tmp_path):
    store = _store(tmp_path)
    assert await store.list_bindings() == []
    assert await store.list_leases() == []
    assert stat.S_IMODE(store.path.stat().st_mode) == 0o600


async def test_load_repairs_legacy_journal_permissions(tmp_path):
    path = tmp_path / "bindings.json"
    config = BindingsConfig()
    path.write_text(config.model_dump_json(), encoding="utf-8")
    path.chmod(0o644)

    await AccountBindingStore(str(path)).load()

    assert stat.S_IMODE(path.stat().st_mode) == 0o600


async def test_corrupt_store_fails_closed_and_preserves_source(tmp_path):
    path = tmp_path / "bindings.json"
    original = "{ definitely-not-json"
    path.write_text(original, encoding="utf-8")
    store = AccountBindingStore(str(path))

    with pytest.raises(BindingStoreCorruptError, match="binding store is corrupt"):
        await store.list_bindings()

    assert path.read_text(encoding="utf-8") == original


@pytest.mark.parametrize(
    "mutate, message",
    [
        (lambda raw: raw.update(version=99), "binding store is corrupt"),
        (lambda raw: raw.update(future_field=True), "binding store is corrupt"),
        (
            lambda raw: raw["bindings"].append(dict(raw["bindings"][0])),
            "binding store is corrupt",
        ),
        (
            lambda raw: raw["bindings"].append({
                **raw["bindings"][0],
                "account_id": "acc-2",
            }),
            "binding store is corrupt",
        ),
    ],
)
async def test_schema_and_ownership_contradictions_fail_closed(
    tmp_path, mutate, message,
):
    path = tmp_path / "bindings.json"
    raw = BindingsConfig(bindings=[AccountBinding(
        account_id="acc-1", eip_allocation_id="eipalloc-1",
    )]).model_dump(mode="json")
    mutate(raw)
    source = json.dumps(raw)
    path.write_text(source, encoding="utf-8")

    with pytest.raises(BindingStoreCorruptError, match=message):
        await AccountBindingStore(str(path)).load()

    assert path.read_text(encoding="utf-8") == source


@pytest.mark.parametrize(
    "mutation",
    [
        {"eip_detached": False},
        {"instance_terminated": False},
        {"last_operation": "attach"},
        {"released_at": None},
        {"worker_cleanup_required": True, "worker_cleanup_done": False},
    ],
)
async def test_contradictory_released_lease_fails_closed(tmp_path, mutation):
    path = tmp_path / "bindings.json"
    lease = AccountLease(
        account_id="acc-1",
        job_id="job-1",
        state=LeaseState.RELEASED,
        eip_detached=True,
        instance_terminated=True,
        last_operation="release",
        released_at=time.time(),
    )
    raw = BindingsConfig(
        bindings=[AccountBinding(account_id="acc-1")],
        leases=[lease],
    ).model_dump(mode="json")
    raw["leases"][0].update(mutation)
    source = json.dumps(raw)
    path.write_text(source, encoding="utf-8")

    with pytest.raises(BindingStoreCorruptError):
        await AccountBindingStore(str(path)).load()

    assert path.read_text(encoding="utf-8") == source


async def test_worker_without_instance_fails_closed_and_preserves_journal(
    tmp_path,
):
    path = tmp_path / "bindings.json"
    raw = BindingsConfig(
        bindings=[AccountBinding(account_id="acc-1")],
        leases=[AccountLease(
            account_id="acc-1",
            job_id="job-1",
            worker_id="worker-1",
        )],
    ).model_dump(mode="json")
    source = json.dumps(raw)
    path.write_text(source, encoding="utf-8")

    with pytest.raises(BindingStoreCorruptError, match="binding store is corrupt"):
        await AccountBindingStore(str(path)).load()

    assert path.read_text(encoding="utf-8") == source


async def test_update_rejects_invalid_runtime_state_and_phase(tmp_path):
    store = _store(tmp_path)
    await store.upsert_binding(AccountBinding(account_id="acc-1"))
    lease = await store.reserve_lease("acc-1", job_id="job-1")

    with pytest.raises(ValueError, match="invalid state"):
        await store.update_lease(lease.lease_id, state="bogus")
    with pytest.raises(ValueError, match="has no instance"):
        await store.update_lease(lease.lease_id, state=LeaseState.ATTACHED)
    with pytest.raises(ValueError, match="invalid state"):
        await store.update_binding("acc-1", state="bogus")


async def test_v1_persistent_instance_migrates_to_cleanup_lease(tmp_path):
    path = tmp_path / "bindings.json"
    path.write_text(json.dumps({
        "bindings": [{
            "account_id": "acc-legacy",
            "email": "old@example.com",
            "instance_id": "aws:i-old",
            "eip_allocation_id": "eipalloc-old",
            "eip_ip": "198.51.100.10",
            "region": "us-east-1",
            "state": "stopped",
        }],
    }), encoding="utf-8")

    store = AccountBindingStore(str(path))
    binding = await store.get_binding("acc-legacy")
    leases = await store.list_leases(active_only=True)

    assert binding is not None and binding.state == BindingState.READY
    assert len(leases) == 1
    assert leases[0].instance_id == "aws:i-old"
    assert leases[0].job_id == "legacy-binding-migration"
    assert leases[0].last_operation == "release"
    assert json.loads(path.read_text(encoding="utf-8"))["version"] == 2


async def test_v1_without_stable_account_id_fails_closed(tmp_path):
    path = tmp_path / "bindings.json"
    source = json.dumps({
        "bindings": [{
            "email": "unknown@example.com",
            "instance_id": "aws:i-old",
            "eip_allocation_id": "eipalloc-old",
        }],
    })
    path.write_text(source, encoding="utf-8")

    with pytest.raises(BindingStoreCorruptError):
        await AccountBindingStore(str(path)).load()
    assert path.read_text(encoding="utf-8") == source


async def test_failed_write_does_not_publish_unpersisted_lease(tmp_path, monkeypatch):
    store = _store(tmp_path)
    await store.upsert_binding(AccountBinding(account_id="acc-1"))

    def fail_write(_config):
        raise OSError("disk full")

    monkeypatch.setattr(store, "_write_config_sync", fail_write)
    with pytest.raises(OSError, match="disk full"):
        await store.reserve_lease("acc-1", job_id="job-1")

    assert await store.list_leases() == []


async def test_binding_is_keyed_by_account_id_not_email(tmp_path):
    store = _store(tmp_path)
    await store.upsert_binding(
        AccountBinding(account_id="acc-1", email="old@example.com")
    )
    await store.upsert_binding(
        AccountBinding(account_id="acc-1", email="new@example.com")
    )
    await store.upsert_binding(
        AccountBinding(account_id="acc-2", email="new@example.com")
    )

    bindings = await store.list_bindings()
    assert {binding.account_id for binding in bindings} == {"acc-1", "acc-2"}
    assert (await store.get_binding("acc-1")).email == "new@example.com"
    assert await store.get_binding("missing") is None


async def test_binding_and_lease_persist_to_disk(tmp_path):
    path = tmp_path / "bindings.json"
    store = AccountBindingStore(str(path))
    await store.upsert_binding(
        AccountBinding(
            account_id="acc-1",
            email="a@example.com",
            eip_allocation_id="eipalloc-1",
            eip_ip="52.0.0.1",
            region="us-east-1",
            state=BindingState.READY,
        )
    )
    lease = await store.reserve_lease(
        "acc-1", email="a@example.com", job_id="job-1", slot=2
    )

    raw = json.loads(path.read_text())
    config = BindingsConfig.model_validate(raw)
    assert config.bindings[0].eip_ip == "52.0.0.1"
    assert config.leases[0].lease_id == lease.lease_id

    reloaded = AccountBindingStore(str(path))
    assert (await reloaded.get_binding("acc-1")).region == "us-east-1"
    assert (await reloaded.get_lease(lease.lease_id)).job_id == "job-1"


async def test_update_binding_patches_copy_and_timestamp(tmp_path):
    store = _store(tmp_path)
    await store.upsert_binding(AccountBinding(account_id="acc-1"))
    before = (await store.get_binding("acc-1")).updated_at
    updated = await store.update_binding(
        "acc-1", eip_ip="52.0.0.9", state=BindingState.READY
    )
    assert updated.eip_ip == "52.0.0.9"
    assert updated.updated_at >= before
    updated.state = BindingState.ERROR
    assert (await store.get_binding("acc-1")).state == BindingState.READY
    assert await store.update_binding("missing", state=BindingState.ERROR) is None


async def test_reserve_is_idempotent_for_same_job_slot(tmp_path):
    store = _store(tmp_path)
    await store.upsert_binding(AccountBinding(account_id="acc-1"))
    first = await store.reserve_lease(
        "acc-1", email="a@example.com", job_id="job-1", slot=0
    )
    second = await store.reserve_lease(
        "acc-1", email="renamed@example.com", job_id="job-1", slot=0
    )
    assert second.lease_id == first.lease_id
    assert second.generation == 1
    assert len(await store.list_leases()) == 1


async def test_list_leases_filters_jobs_before_copy_and_honors_limit(tmp_path):
    store = _store(tmp_path)
    for index in range(4):
        account_id = f"acc-{index}"
        await store.upsert_binding(AccountBinding(account_id=account_id))
        await store.reserve_lease(
            account_id,
            job_id="selected" if index < 3 else "unrelated",
            slot=index,
        )

    selected = await store.list_leases(
        job_ids={"selected"},
        limit=2,
    )

    assert len(selected) == 2
    assert {lease.job_id for lease in selected} == {"selected"}
    with pytest.raises(ValueError, match="limit must be positive"):
        await store.list_leases(limit=0)


async def test_reserve_atomically_rejects_concurrent_jobs(tmp_path):
    store = _store(tmp_path)
    await store.upsert_binding(AccountBinding(account_id="acc-1"))

    async def reserve(job_id):
        return await store.reserve_lease("acc-1", job_id=job_id)

    results = await asyncio.gather(
        reserve("job-a"), reserve("job-b"), return_exceptions=True
    )
    assert sum(isinstance(result, AccountLease) for result in results) == 1
    assert sum(isinstance(result, LeaseConflictError) for result in results) == 1
    active = await store.list_leases(account_id="acc-1", active_only=True)
    assert len(active) == 1


async def test_released_lease_allows_next_generation(tmp_path):
    store = _store(tmp_path)
    await store.upsert_binding(AccountBinding(account_id="acc-1"))
    first = await store.reserve_lease("acc-1", job_id="job-1")
    await store.update_lease(
        first.lease_id,
        state=LeaseState.RELEASED,
        eip_detached=True,
        instance_terminated=True,
        last_operation="release",
        released_at=time.time(),
    )
    second = await store.reserve_lease("acc-1", job_id="job-2")
    assert second.generation == 2
    assert second.lease_id != first.lease_id


async def test_claim_instance_rejects_cross_account_reuse(tmp_path):
    store = _store(tmp_path)
    await store.upsert_binding(AccountBinding(account_id="acc-1"))
    await store.upsert_binding(AccountBinding(account_id="acc-2"))
    one = await store.reserve_lease("acc-1", job_id="job-1")
    two = await store.reserve_lease("acc-2", job_id="job-2")
    await store.begin_attach(one.lease_id, "aws:i-1", "worker-1")
    with pytest.raises(LeaseConflictError, match="aws:i-1"):
        await store.begin_attach(two.lease_id, "aws:i-1", "worker-2")


async def test_get_lease_by_instance_and_list_filters(tmp_path):
    store = _store(tmp_path)
    await store.upsert_binding(AccountBinding(account_id="acc-1"))
    lease = await store.reserve_lease("acc-1", job_id="job-1")
    await store.begin_attach(lease.lease_id, "aws:i-1", "worker-1")
    assert (await store.get_lease_by_instance("aws:i-1")).lease_id == lease.lease_id
    assert len(await store.list_leases(account_id="acc-1", active_only=True)) == 1
    assert await store.get_lease_by_instance("aws:i-missing") is None


async def test_release_intent_atomically_freezes_lease_identity(tmp_path):
    store = _store(tmp_path)
    await store.upsert_binding(AccountBinding(account_id="acc-1"))
    lease = await store.reserve_lease("acc-1", job_id="job-1")

    releasing = await store.begin_release(
        lease.lease_id, expected_lease=lease
    )
    assert releasing.state == LeaseState.RELEASING

    with pytest.raises(LeaseConflictError, match="identity is frozen"):
        await store.update_lease(
            lease.lease_id,
            instance_id="aws:i-late",
            worker_id="worker-late",
        )

    current = await store.get_lease(lease.lease_id)
    assert current.instance_id is None
    assert current.worker_id == ""


async def test_remove_binding_refuses_active_lease(tmp_path):
    store = _store(tmp_path)
    await store.upsert_binding(AccountBinding(account_id="acc-1"))
    lease = await store.reserve_lease("acc-1", job_id="job-1")
    with pytest.raises(LeaseConflictError):
        await store.remove_binding("acc-1")
    await store.update_lease(
        lease.lease_id,
        state=LeaseState.RELEASED,
        eip_detached=True,
        instance_terminated=True,
        last_operation="release",
        released_at=time.time(),
    )
    assert await store.remove_binding("acc-1") is True
    assert await store.remove_binding("acc-1") is False
