"""Tests for the live provision/login wiring (batch_hooks)."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from elastic_agent.core.account_store import AccountStore
from elastic_agent.core.batch_hooks import (
    AccountAllocator,
    LoginCoordinator,
    make_login_hook,
    make_provision_hook,
    wire_batch,
)
from elastic_agent.core.config import ElasticAgentConfig
from elastic_agent.core.credential_pool import AccountDefinition
from elastic_agent.core.event_bus import EventBus

pytestmark = pytest.mark.asyncio


async def _store(tmp_path, accounts):
    s = AccountStore(str(tmp_path / "accounts.json"))
    for a in accounts:
        await s.add(a)
    return s


def _acct(i, group="standard", enabled=True):
    return AccountDefinition(id=f"a{i}", email=f"a{i}@x.com", email_token=f"t{i}",
                             group=group, enabled=enabled)


class FakeConn:
    def __init__(self, connected=True):
        self.sent = []
        self._connected = connected

    async def send_command(self, worker_id, msg):
        self.sent.append((worker_id, msg))

    def is_connected(self, worker_id):
        return self._connected


class FakeManager:
    def __init__(self, tmp_path, store, *, connected=True, host="1.2.3.4"):
        self.config = ElasticAgentConfig()
        self.config.registry.path = str(tmp_path / "registry.json")
        self.account_store = store
        self.event_bus = EventBus()
        self.connection_manager = FakeConn(connected=connected)
        node = SimpleNamespace(instance_id="i-1", public_ip=host, auth_token="tok",
                               node_id="w1", private_ip="10.0.0.1", metadata={})
        self.registry = SimpleNamespace(get=lambda wid: _async(node))
        self.provider = SimpleNamespace(
            wait_until_running=lambda iid: _async(SimpleNamespace(public_ip=host)))


async def _async(v):
    return v


# --------------------------------------------------------------------------
# AccountAllocator
# --------------------------------------------------------------------------


class TestAccountAllocator:
    async def test_distinct_per_worker(self, tmp_path):
        alloc = AccountAllocator(await _store(tmp_path, [_acct(1), _acct(2)]))
        a = await alloc.allocate("w1", "standard")
        b = await alloc.allocate("w2", "standard")
        assert {a.id, b.id} == {"a1", "a2"}

    async def test_reallocate_retires_old(self, tmp_path):
        alloc = AccountAllocator(await _store(tmp_path, [_acct(1), _acct(2), _acct(3)]))
        first = await alloc.allocate("w1", "standard")
        second = await alloc.allocate("w1", "standard")  # rotation
        assert second.id != first.id
        # exhausted account never comes back, even after others freed
        third = await alloc.allocate("w1", "standard")
        assert third.id not in {first.id}

    async def test_none_when_pool_empty(self, tmp_path):
        alloc = AccountAllocator(await _store(tmp_path, [_acct(1)]))
        assert (await alloc.allocate("w1", "standard")) is not None
        assert (await alloc.allocate("w2", "standard")) is None

    async def test_group_and_enabled_filter(self, tmp_path):
        alloc = AccountAllocator(await _store(tmp_path, [
            _acct(1, group="other"), _acct(2, enabled=False), _acct(3),
        ]))
        got = await alloc.allocate("w1", "standard")
        assert got.id == "a3"

    async def test_release_worker_frees_account(self, tmp_path):
        alloc = AccountAllocator(await _store(tmp_path, [_acct(1)]))
        await alloc.allocate("w1", "standard")
        await alloc.release_worker("w1")
        assert (await alloc.allocate("w2", "standard")).id == "a1"


# --------------------------------------------------------------------------
# LoginCoordinator
# --------------------------------------------------------------------------


class TestLoginCoordinator:
    async def test_login_success(self, tmp_path):
        bus = EventBus()
        conn = FakeConn()
        coord = LoginCoordinator(conn, bus, timeout=5)
        task = asyncio.create_task(coord.login("w1", _acct(1), "/root/.claude"))
        await asyncio.sleep(0.01)
        # worker replied success
        await bus.emit("ACCOUNT_LOGIN_RESULT", "w1", {"account_id": "a1", "success": True})
        outcome = await task
        assert outcome.success
        assert outcome.account_email == "a1@x.com"
        # ACCOUNT_LOGIN was sent
        assert conn.sent[0][1].type == "ACCOUNT_LOGIN"

    async def test_login_failure(self, tmp_path):
        bus = EventBus()
        coord = LoginCoordinator(FakeConn(), bus, timeout=5)
        task = asyncio.create_task(coord.login("w1", _acct(1), "/root/.claude"))
        await asyncio.sleep(0.01)
        await bus.emit("ACCOUNT_LOGIN_RESULT", "w1", {"account_id": "a1", "success": False, "error": "boom"})
        outcome = await task
        assert not outcome.success
        assert outcome.error == "boom"

    async def test_login_timeout(self, tmp_path):
        coord = LoginCoordinator(FakeConn(), EventBus(), timeout=0.05)
        outcome = await coord.login("w1", _acct(1), "/root/.claude")
        assert not outcome.success
        assert "timed out" in outcome.error


# --------------------------------------------------------------------------
# login hook
# --------------------------------------------------------------------------


class TestLoginHook:
    async def test_allocates_and_logs_in(self, tmp_path):
        from elastic_agent.core.job_spec import JobSpec, RunSpec
        store = await _store(tmp_path, [_acct(1)])
        mgr = FakeManager(tmp_path, store)
        alloc = AccountAllocator(store)
        coord = LoginCoordinator(mgr.connection_manager, mgr.event_bus, timeout=5)
        hook = make_login_hook(mgr, alloc, coord)
        spec = JobSpec(name="j", run=RunSpec(command="x"))

        task = asyncio.create_task(hook("w1", spec, "/root/.claude"))
        await asyncio.sleep(0.01)
        await mgr.event_bus.emit("ACCOUNT_LOGIN_RESULT", "w1", {"account_id": "a1", "success": True})
        outcome = await task
        assert outcome.success and outcome.account_id == "a1"

    async def test_no_account_fails(self, tmp_path):
        from elastic_agent.core.job_spec import JobSpec, RunSpec
        store = await _store(tmp_path, [])
        mgr = FakeManager(tmp_path, store)
        hook = make_login_hook(mgr, AccountAllocator(store),
                               LoginCoordinator(mgr.connection_manager, mgr.event_bus))
        outcome = await hook("w1", JobSpec(name="j", run=RunSpec(command="x")), "")
        assert not outcome.success
        assert "no available account" in outcome.error


# --------------------------------------------------------------------------
# provision hook
# --------------------------------------------------------------------------


class TestProvisionHook:
    async def test_success(self, tmp_path):
        from elastic_agent.core.job_spec import JobSpec, RunSpec
        mgr = FakeManager(tmp_path, await _store(tmp_path, []), connected=True)
        ran = {}

        async def runner(node_id, host, steps, user, key):
            ran["host"] = host
            ran["steps"] = [s.name for s in steps]
            return True

        hook = make_provision_hook(mgr, bootstrap_runner=runner, ws_wait_timeout=1)
        ok = await hook("w1", None, JobSpec(name="j", run=RunSpec(command="x")))
        assert ok is True
        assert ran["host"] == "1.2.3.4"
        assert "runtime-deploy" in ran["steps"]

    async def test_bootstrap_failure(self, tmp_path):
        from elastic_agent.core.job_spec import JobSpec, RunSpec
        mgr = FakeManager(tmp_path, await _store(tmp_path, []))

        async def runner(*a):
            return False

        hook = make_provision_hook(mgr, bootstrap_runner=runner, ws_wait_timeout=1)
        assert await hook("w1", None, JobSpec(name="j", run=RunSpec(command="x"))) is False

    async def test_ws_never_connects(self, tmp_path):
        from elastic_agent.core.job_spec import JobSpec, RunSpec
        mgr = FakeManager(tmp_path, await _store(tmp_path, []), connected=False)

        async def runner(*a):
            return True

        hook = make_provision_hook(mgr, bootstrap_runner=runner, ws_wait_timeout=0.1)
        assert await hook("w1", None, JobSpec(name="j", run=RunSpec(command="x"))) is False


# --------------------------------------------------------------------------
# wire_batch event routing
# --------------------------------------------------------------------------


class TestWireBatchRouting:
    async def test_routes_exhausted_and_exit(self, tmp_path):
        mgr = FakeManager(tmp_path, await _store(tmp_path, [_acct(1)]))
        orch = wire_batch(mgr)

        calls = {"exh": [], "exit": []}

        async def fake_exh(worker_id):
            calls["exh"].append(worker_id)
            return True

        async def fake_exit(worker_id, exit_code, task_id=None):
            calls["exit"].append((worker_id, exit_code, task_id))

        orch.handle_exhausted = fake_exh
        orch.handle_exit = fake_exit
        # mark w1 as a batch-owned worker so PROCESS_EXIT routes
        orch._worker_index["w1"] = "job-1"

        await mgr.event_bus.emit("RUN_EXHAUSTED", "w1",
                                 {"worker_id": "w1", "job_id": "job-1", "reason": "rate_limit"})
        await mgr.event_bus.emit("PROCESS_EXIT", "w1",
                                 {"task_id": "job-1:w1:abc", "exit_code": 0})
        assert calls["exh"] == ["w1"]
        assert calls["exit"] == [("w1", 0, "job-1:w1:abc")]

    async def test_non_batch_exit_ignored(self, tmp_path):
        mgr = FakeManager(tmp_path, await _store(tmp_path, []))
        orch = wire_batch(mgr)
        seen = []
        orch.handle_exit = lambda *a, **k: seen.append(a)  # noqa
        # w-other is not in the worker index → should be ignored
        await mgr.event_bus.emit("PROCESS_EXIT", "w-other", {"task_id": "t", "exit_code": 1})
        assert seen == []
