"""Tests for AccountBindingStore (durable account↔machine↔EIP map)."""

from __future__ import annotations

import json

import pytest

from elastic_agent.core.account_binding import (
    AccountBinding,
    AccountBindingStore,
    BindingState,
    BindingsConfig,
)

pytestmark = pytest.mark.asyncio


def _store(tmp_path):
    return AccountBindingStore(str(tmp_path / "bindings.json"))


async def test_empty_initially(tmp_path):
    assert await _store(tmp_path).list() == []


async def test_upsert_and_get(tmp_path):
    s = _store(tmp_path)
    await s.upsert(AccountBinding(email="a@x.com", instance_id="aws:i-1"))
    b = await s.get("a@x.com")
    assert b is not None
    assert b.instance_id == "aws:i-1"
    assert b.state == BindingState.UNBOUND
    assert await s.get("missing@x.com") is None


async def test_upsert_replaces_by_email(tmp_path):
    s = _store(tmp_path)
    await s.upsert(AccountBinding(email="a@x.com", instance_id="aws:i-1"))
    await s.upsert(AccountBinding(email="a@x.com", instance_id="aws:i-2"))
    all_ = await s.list()
    assert len(all_) == 1
    assert all_[0].instance_id == "aws:i-2"


async def test_persists_to_disk(tmp_path):
    path = tmp_path / "bindings.json"
    s = AccountBindingStore(str(path))
    await s.upsert(
        AccountBinding(
            email="a@x.com",
            instance_id="aws:i-1",
            eip_allocation_id="eipalloc-1",
            eip_ip="52.0.0.1",
            state=BindingState.STOPPED,
        )
    )
    raw = json.loads(path.read_text())
    cfg = BindingsConfig.model_validate(raw)
    assert cfg.bindings[0].eip_ip == "52.0.0.1"
    assert cfg.bindings[0].state == BindingState.STOPPED


async def test_reload_from_disk(tmp_path):
    path = str(tmp_path / "bindings.json")
    s1 = AccountBindingStore(path)
    await s1.upsert(AccountBinding(email="a@x.com", eip_allocation_id="eipalloc-1"))
    s2 = AccountBindingStore(path)
    b = await s2.get("a@x.com")
    assert b is not None and b.eip_allocation_id == "eipalloc-1"


async def test_update_patches_fields(tmp_path):
    s = _store(tmp_path)
    await s.upsert(AccountBinding(email="a@x.com", state=BindingState.PROVISIONING))
    updated = await s.update("a@x.com", state=BindingState.STOPPED, eip_ip="52.0.0.9")
    assert updated is not None
    assert updated.state == BindingState.STOPPED
    assert updated.eip_ip == "52.0.0.9"
    # updating a missing binding is a no-op returning None
    assert await s.update("missing@x.com", state=BindingState.RUNNING) is None


async def test_update_bumps_updated_at(tmp_path):
    s = _store(tmp_path)
    await s.upsert(AccountBinding(email="a@x.com"))
    before = (await s.get("a@x.com")).updated_at
    await s.update("a@x.com", state=BindingState.RUNNING)
    after = (await s.get("a@x.com")).updated_at
    assert after >= before


async def test_remove(tmp_path):
    s = _store(tmp_path)
    await s.upsert(AccountBinding(email="a@x.com"))
    assert await s.remove("a@x.com") is True
    assert await s.remove("a@x.com") is False
    assert await s.list() == []


async def test_by_state(tmp_path):
    s = _store(tmp_path)
    await s.upsert(AccountBinding(email="a@x.com", state=BindingState.STOPPED))
    await s.upsert(AccountBinding(email="b@x.com", state=BindingState.RUNNING))
    await s.upsert(AccountBinding(email="c@x.com", state=BindingState.STOPPED))
    stopped = await s.by_state(BindingState.STOPPED)
    assert {b.email for b in stopped} == {"a@x.com", "c@x.com"}
    idle = await s.by_state(BindingState.STOPPED, BindingState.WARM)
    assert len(idle) == 2


async def test_get_by_instance(tmp_path):
    s = _store(tmp_path)
    await s.upsert(AccountBinding(email="a@x.com", instance_id="aws:i-1"))
    b = await s.get_by_instance("aws:i-1")
    assert b is not None and b.email == "a@x.com"
    assert await s.get_by_instance("aws:i-nope") is None


async def test_list_returns_copies(tmp_path):
    """Mutating a returned binding must not corrupt stored state."""
    s = _store(tmp_path)
    await s.upsert(AccountBinding(email="a@x.com", state=BindingState.STOPPED))
    got = await s.list()
    got[0].state = BindingState.ERROR
    assert (await s.get("a@x.com")).state == BindingState.STOPPED
