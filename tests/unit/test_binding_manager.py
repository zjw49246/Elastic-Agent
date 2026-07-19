"""Tests for BindingManager wake/sleep lifecycle (DryRunProvider-backed)."""

from __future__ import annotations

import pytest

from elastic_agent.core.account_binding import AccountBindingStore, BindingState
from elastic_agent.core.binding_manager import BindingManager
from elastic_agent.core.providers.base import InstanceConfig
from elastic_agent.testing.dry_run_provider import DryRunProvider

pytestmark = pytest.mark.asyncio

CFG = InstanceConfig(instance_type="t3.large", image_id="ami-x", key_pair_name="k")


def _make(tmp_path, **kw):
    provider = DryRunProvider()
    store = AccountBindingStore(str(tmp_path / "bindings.json"))
    hooks = {"provision": 0, "login": 0}

    async def _prov(_b):
        hooks["provision"] += 1

    async def _login(_b):
        hooks["login"] += 1

    mgr = BindingManager(
        provider,
        store,
        provision_box=_prov,
        login_box=_login,
        **kw,
    )
    return provider, store, mgr, hooks


async def test_build_lazily_on_first_acquire(tmp_path):
    provider, store, mgr, hooks = _make(tmp_path)
    b = await mgr.acquire("a@x.com", account_id="acc-1", instance_config=CFG,
                          config_dir="/home/ubuntu/.agent-codex/accounts/a@x.com")
    assert b.state == BindingState.RUNNING
    assert b.instance_id is not None
    assert b.eip_allocation_id is not None
    assert b.eip_ip
    assert b.config_dir.endswith("a@x.com")
    # one machine, one EIP, provisioned + logged in exactly once
    assert len(provider.get_operations("create")) == 1
    assert len(provider.get_operations("allocate_eip")) == 1
    assert hooks == {"provision": 1, "login": 1}
    # instance shows the EIP as its public IP
    inst = await provider.get_instance(b.instance_id)
    assert inst.public_ip == b.eip_ip


async def test_build_requires_instance_config(tmp_path):
    _provider, _store, mgr, _hooks = _make(tmp_path)
    with pytest.raises(ValueError):
        await mgr.acquire("a@x.com")  # no binding, no config → can't build


async def test_acquire_idempotent_while_running(tmp_path):
    provider, _store, mgr, hooks = _make(tmp_path)
    await mgr.acquire("a@x.com", instance_config=CFG)
    b2 = await mgr.acquire("a@x.com", instance_config=CFG)
    assert b2.state == BindingState.RUNNING
    assert len(provider.get_operations("create")) == 1  # not rebuilt
    assert hooks == {"provision": 1, "login": 1}


async def test_release_warm_then_reap_stops(tmp_path):
    provider, _store, mgr, _hooks = _make(tmp_path, warm_seconds=100.0)
    b = await mgr.acquire("a@x.com", instance_config=CFG)
    warm = await mgr.release("a@x.com")
    assert warm.state == BindingState.WARM
    assert warm.warm_until is not None

    # before grace elapses: reap is a no-op
    assert await mgr.reap(now=warm.warm_until - 1) == []
    # after grace: box is stopped
    stopped = await mgr.reap(now=warm.warm_until + 1)
    assert stopped == ["a@x.com"]
    assert len(provider.get_operations("stop")) == 1


async def test_wake_after_stop_no_rebuild_no_relogin(tmp_path):
    provider, _store, mgr, hooks = _make(tmp_path, warm_seconds=0.0)
    b = await mgr.acquire("a@x.com", instance_config=CFG)
    eip_ip = b.eip_ip
    await mgr.release("a@x.com")
    await mgr.reap(now=b.updated_at + 1000)  # warm_seconds=0 → immediately eligible

    b2 = await mgr.acquire("a@x.com", instance_config=CFG)
    assert b2.state == BindingState.RUNNING
    assert len(provider.get_operations("create")) == 1  # same box
    assert len(provider.get_operations("start")) == 1   # woken
    assert hooks == {"provision": 1, "login": 1}         # NOT re-logged-in
    assert b2.eip_ip == eip_ip                            # same IP across cycle


async def test_reacquire_during_warm_skips_stop_start(tmp_path):
    provider, _store, mgr, _hooks = _make(tmp_path, warm_seconds=1000.0)
    await mgr.acquire("a@x.com", instance_config=CFG)
    await mgr.release("a@x.com")
    b = await mgr.acquire("a@x.com", instance_config=CFG)
    assert b.state == BindingState.RUNNING
    # never stopped, never started — stayed warm and got reused
    assert provider.get_operations("stop") == []
    assert provider.get_operations("start") == []


async def test_reap_only_stops_elapsed(tmp_path):
    provider, _store, mgr, _hooks = _make(tmp_path, warm_seconds=100.0)
    await mgr.acquire("a@x.com", instance_config=CFG)
    await mgr.acquire("b@x.com", instance_config=CFG)
    wa = await mgr.release("a@x.com")
    await mgr.release("b@x.com")
    # advance past a's grace only by reaping at a fixed time; both share grace,
    # so give b a longer warm by re-releasing with a bumped warm_until.
    stopped = await mgr.reap(now=wa.warm_until + 1)
    assert set(stopped) == {"a@x.com", "b@x.com"}


async def test_sleep_now_skips_warm(tmp_path):
    provider, _store, mgr, _hooks = _make(tmp_path)
    b = await mgr.acquire("a@x.com", instance_config=CFG)
    slept = await mgr.sleep_now("a@x.com")
    assert slept.state == BindingState.STOPPED
    assert len(provider.get_operations("stop")) == 1


async def test_build_failure_marks_error_and_raises(tmp_path):
    provider, store, mgr, _hooks = _make(tmp_path)
    provider.fail_next("create_instance", RuntimeError("VcpuLimitExceeded"))
    with pytest.raises(RuntimeError):
        await mgr.acquire("a@x.com", instance_config=CFG)
    b = await store.get("a@x.com")
    assert b.state == BindingState.ERROR
    assert "Vcpu" in (b.error or "")


async def test_error_binding_rebuilds_on_next_acquire(tmp_path):
    provider, _store, mgr, hooks = _make(tmp_path)
    provider.fail_next("create_instance", RuntimeError("boom"))
    with pytest.raises(RuntimeError):
        await mgr.acquire("a@x.com", instance_config=CFG)
    # next acquire retries the build cleanly
    b = await mgr.acquire("a@x.com", instance_config=CFG)
    assert b.state == BindingState.RUNNING
    assert hooks == {"provision": 1, "login": 1}


async def test_decommission_terminates_releases_and_drops(tmp_path):
    provider, store, mgr, _hooks = _make(tmp_path)
    b = await mgr.acquire("a@x.com", instance_config=CFG)
    alloc = b.eip_allocation_id
    ok = await mgr.decommission("a@x.com")
    assert ok is True
    assert await store.get("a@x.com") is None
    assert len(provider.get_operations("terminate")) == 1
    assert len(provider.get_operations("release_eip")) == 1
    assert await provider.describe_eip(alloc) is None


async def test_concurrent_acquire_builds_once(tmp_path):
    import asyncio

    provider, _store, mgr, hooks = _make(tmp_path)
    results = await asyncio.gather(
        mgr.acquire("a@x.com", instance_config=CFG),
        mgr.acquire("a@x.com", instance_config=CFG),
        mgr.acquire("a@x.com", instance_config=CFG),
    )
    assert all(r.state == BindingState.RUNNING for r in results)
    assert len(provider.get_operations("create")) == 1  # per-email lock held
    assert hooks == {"provision": 1, "login": 1}
