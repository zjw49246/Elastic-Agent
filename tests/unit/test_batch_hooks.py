"""Tests for the live provision/login wiring (batch_hooks)."""

from __future__ import annotations

import asyncio
import time
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from elastic_agent.core.account_store import AccountStore
from elastic_agent.core.batch_hooks import (
    AccountAllocator,
    AccountClaimConflictError,
    AgentApiCoordinator,
    LoginCoordinator,
    make_bound_hooks,
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


def _acct(
    i, group="standard", enabled=True, agent_type="claude", password="",
):
    return AccountDefinition(
        id=f"a{i}",
        email=f"a{i}@x.com",
        email_token=f"t{i}",
        password=password,
        agent_type=agent_type,
        group=group,
        enabled=enabled,
    )


class FakeConn:
    def __init__(self, connected=True):
        self.sent = []
        self._connected = connected
        self.disconnected = []

    async def send_command(self, worker_id, msg):
        self.sent.append((worker_id, msg))

    def is_connected(self, worker_id):
        return self._connected

    async def disconnect_worker(self, worker_id):
        self.disconnected.append(worker_id)


class FakeRegistry:
    def __init__(self, node):
        self.node = node
        self.removed = []

    async def get(self, worker_id):
        return (
            self.node
            if self.node is not None and worker_id == self.node.node_id
            else None
        )

    async def update(self, worker_id, **fields):
        node = await self.get(worker_id)
        if node is None:
            return None
        for key, value in fields.items():
            setattr(node, key, value)
        return node

    async def remove(self, worker_id):
        node = await self.get(worker_id)
        if node is None:
            return False
        self.removed.append(worker_id)
        self.node = None
        return True


class FakeManager:
    def __init__(self, tmp_path, store, *, connected=True, host="1.2.3.4"):
        self.config = ElasticAgentConfig()
        self.config.server.host = "127.0.0.1"
        self.config.registry.path = str(tmp_path / "registry.json")
        self.collected_root = str(tmp_path / "collected")
        self.account_store = store
        self.event_bus = EventBus()
        self.connection_manager = FakeConn(connected=connected)
        node = SimpleNamespace(instance_id="i-1", public_ip=host, auth_token="tok",
                               node_id="w1", private_ip="10.0.0.1", metadata={})
        self.registry = FakeRegistry(node)
        self.provider = SimpleNamespace(
            wait_until_running=lambda iid: _async(SimpleNamespace(public_ip=host)))
        self.archived_job_logs = []
        self.binding_recovery_ready = True

    async def remove_terminated_node_record(self, worker_id):
        return await self.registry.remove(worker_id)

    async def archive_job_task_log(self, job_id, worker_id, data):
        self.archived_job_logs.append((job_id, worker_id, dict(data)))
        return True


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

    async def test_multiple_accounts_per_worker_distinct(self, tmp_path):
        # per_worker > 1: several distinct accounts on one worker; an already-held
        # (e.g. exhausted) account is never handed out again while assigned.
        alloc = AccountAllocator(await _store(tmp_path, [_acct(1), _acct(2), _acct(3)]))
        got = [(await alloc.allocate("w1", "standard")).id for _ in range(3)]
        assert sorted(got) == ["a1", "a2", "a3"]
        assert len(set(got)) == 3
        # pool now spent for this worker
        assert await alloc.allocate("w1", "standard") is None

    async def test_none_when_pool_empty(self, tmp_path):
        alloc = AccountAllocator(await _store(tmp_path, [_acct(1)]))
        assert (await alloc.allocate("w1", "standard")) is not None
        assert (await alloc.allocate("w2", "standard")) is None

    async def test_agent_type_is_part_of_automatic_and_explicit_selection(
        self, tmp_path,
    ):
        store = await _store(tmp_path, [
            _acct(1, agent_type="claude"),
            _acct(2, agent_type="codex", password="openai-secret"),
        ])
        alloc = AccountAllocator(store)

        codex = await alloc.allocate(
            "codex-worker", "standard", agent_type="codex"
        )
        assert codex is not None and codex.id == "a2"
        assert await alloc.reserve(
            "wrong-agent", "standard", account_id="a1", agent_type="codex"
        ) is None

    async def test_oauth_constraint_bypasses_agent_api_store_and_probe(
        self, tmp_path,
    ):
        native_store = await _store(tmp_path, [
            _acct(9, agent_type="codex", password="native-password"),
        ])
        api_store = _FakeAgentApiStore(_api_acct())
        api_store.list = AsyncMock(side_effect=AssertionError(
            "oauth allocation must not enumerate Agent API accounts"
        ))
        api_store.fetch_usage = AsyncMock(side_effect=AssertionError(
            "oauth allocation must not probe Agent API usage"
        ))
        alloc = AccountAllocator(native_store, api_store)

        selected = await alloc.allocate(
            "oauth-only-worker", "standard", agent_type="codex",
            auth_kind="oauth",
        )

        assert selected is not None and selected.id == "a9"
        api_store.list.assert_not_awaited()
        api_store.fetch_usage.assert_not_awaited()

    async def test_agent_api_constraint_never_falls_back_to_oauth(
        self, tmp_path,
    ):
        native_store = await _store(tmp_path, [
            _acct(9, agent_type="codex", password="native-password"),
        ])
        api_store = _FakeAgentApiStore(_api_acct())
        api_store.decision = {
            "known": True, "available": False,
            "reason": "quota_exhausted",
        }
        alloc = AccountAllocator(native_store, api_store)

        selected = await alloc.allocate(
            "api-only-worker", "standard", agent_type="codex",
            auth_kind="agent_api",
        )

        assert selected is None
        assert api_store.fetch_calls == 1

    @pytest.mark.parametrize(
        ("account_id", "auth_kind"),
        [("a9", "agent_api"), ("cloudrouter-1", "oauth")],
    )
    async def test_explicit_account_must_match_auth_kind(
        self, tmp_path, account_id, auth_kind,
    ):
        native_store = await _store(tmp_path, [
            _acct(9, agent_type="codex", password="native-password"),
        ])
        alloc = AccountAllocator(
            native_store, _FakeAgentApiStore(_api_acct()),
        )

        claim = await alloc.reserve(
            "wrong-auth-kind", "standard", account_id=account_id,
            agent_type="codex", auth_kind=auth_kind,
        )

        assert claim is None

    async def test_group_and_enabled_filter(self, tmp_path):
        alloc = AccountAllocator(await _store(tmp_path, [
            _acct(1, group="other"), _acct(2, enabled=False), _acct(3),
        ]))
        got = await alloc.allocate("w1", "standard")
        assert got.id == "a3"

    async def test_automatic_and_explicit_allocation_honor_exclusions(
        self, tmp_path,
    ):
        alloc = AccountAllocator(await _store(
            tmp_path, [_acct(1), _acct(2)],
        ))

        automatic = await alloc.allocate(
            "automatic",
            "standard",
            excluded_account_ids={"a1"},
        )
        explicit = await alloc.allocate(
            "explicit",
            "standard",
            account_id="a1",
            excluded_account_ids={"a1"},
        )

        assert automatic is not None and automatic.id == "a2"
        assert explicit is None

    async def test_release_worker_frees_account(self, tmp_path):
        alloc = AccountAllocator(await _store(tmp_path, [_acct(1)]))
        await alloc.allocate("w1", "standard")
        await alloc.release_worker("w1")
        assert (await alloc.allocate("w2", "standard")).id == "a1"

    async def test_unbound_agent_api_claims_are_reference_counted(
        self, tmp_path,
    ):
        account = _api_acct()
        alloc = AccountAllocator(
            await _store(tmp_path, []),
            _FakeAgentApiStore(account),
        )

        first = await alloc.allocate(
            "job-1:w1", "standard", account_id=account.id,
            agent_type="codex",
        )
        second = await alloc.allocate(
            "job-2:w2", "standard", account_id=account.id,
            agent_type="codex",
        )

        assert first.id == second.id == account.id
        await alloc.release_worker("job-1:w1")
        with pytest.raises(AccountClaimConflictError, match="actively claimed"):
            async with alloc.mutation_guard(account.id):
                pass

        await alloc.release_worker("job-2:w2")
        async with alloc.mutation_guard(account.id):
            pass

    async def test_automatic_unbound_workers_share_only_api_key(
        self, tmp_path,
    ):
        account = _api_acct()
        alloc = AccountAllocator(
            await _store(tmp_path, []),
            _FakeAgentApiStore(account),
        )

        first = await alloc.allocate(
            "w1", "standard", agent_type="codex"
        )
        second = await alloc.allocate(
            "w2", "standard", agent_type="codex"
        )

        assert first.id == second.id == account.id

    async def test_automatic_slots_stay_distinct_but_explicit_can_repeat(
        self, tmp_path,
    ):
        accounts = [_api_acct(), _apex_api_acct()]
        alloc = AccountAllocator(
            await _store(tmp_path, []),
            _MultiAgentApiStore(accounts),
        )

        first = await alloc.allocate(
            "same-worker", "standard", agent_type="codex"
        )
        second = await alloc.allocate(
            "same-worker", "standard", agent_type="codex"
        )
        repeated = await alloc.allocate(
            "same-worker", "standard", account_id=first.id,
            agent_type="codex",
        )

        assert {first.id, second.id} == {account.id for account in accounts}
        assert repeated.id == first.id

    async def test_bound_style_agent_api_reserve_remains_exclusive(
        self, tmp_path,
    ):
        account = _api_acct()
        alloc = AccountAllocator(
            await _store(tmp_path, []),
            _FakeAgentApiStore(account),
        )

        first = await alloc.reserve(
            "job-1:0", "standard", account_id=account.id,
            agent_type="codex",
        )
        second = await alloc.reserve(
            "job-2:0", "standard", account_id=account.id,
            agent_type="codex",
        )

        assert first is not None
        assert second is None

    async def test_durably_bound_api_is_excluded_from_unbound_allocator(
        self, tmp_path,
    ):
        account = _api_acct()

        async def bindings():
            return [SimpleNamespace(account_id=account.id)]

        alloc = AccountAllocator(
            await _store(tmp_path, []),
            _FakeAgentApiStore(account),
            durable_binding_loader=bindings,
        )

        assert await alloc.allocate(
            "worker-unbound",
            "standard",
            account_id=account.id,
            agent_type="codex",
        ) is None
        bound = await alloc.reserve(
            "job-bound:0",
            "standard",
            account_id=account.id,
            agent_type="codex",
            allow_durable_binding=True,
        )

        assert bound is not None
        assert bound.account.id == account.id
        assert bound.shareable is False

    async def test_allocator_rechecks_binding_after_concurrent_decommission(
        self, tmp_path,
    ):
        account = _api_acct()
        binding_exists = True
        snapshot_taken = asyncio.Event()
        release_snapshot = asyncio.Event()
        loader_calls = 0

        async def bindings():
            nonlocal loader_calls
            loader_calls += 1
            snapshot = binding_exists
            if loader_calls == 1:
                snapshot_taken.set()
                await release_snapshot.wait()
            return (
                [SimpleNamespace(account_id=account.id)]
                if snapshot
                else []
            )

        alloc = AccountAllocator(
            await _store(tmp_path, []),
            _FakeAgentApiStore(account),
            durable_binding_loader=bindings,
        )
        allocation = asyncio.create_task(alloc.allocate(
            "worker-after-decommission",
            "standard",
            account_id=account.id,
            agent_type="codex",
        ))
        await snapshot_taken.wait()
        async with alloc.mutation_guard(account.id):
            binding_exists = False
        release_snapshot.set()

        selected = await allocation

        assert selected is not None
        assert selected.id == account.id
        assert loader_calls == 2

    async def test_bound_agent_api_claim_rejects_later_unbound_sharing(
        self, tmp_path,
    ):
        account = _api_acct()
        alloc = AccountAllocator(
            await _store(tmp_path, []),
            _FakeAgentApiStore(account),
        )

        bound = await alloc.reserve(
            "job-bound:0", "standard", account_id=account.id,
            agent_type="codex",
        )
        unbound = await alloc.allocate(
            "worker-unbound", "standard", account_id=account.id,
            agent_type="codex",
        )

        assert bound is not None
        assert unbound is None

    async def test_unbound_agent_api_claim_rejects_later_bound_reserve(
        self, tmp_path,
    ):
        account = _api_acct()
        alloc = AccountAllocator(
            await _store(tmp_path, []),
            _FakeAgentApiStore(account),
        )

        unbound = await alloc.allocate(
            "worker-unbound", "standard", account_id=account.id,
            agent_type="codex",
        )
        bound = await alloc.reserve(
            "job-bound:0", "standard", account_id=account.id,
            agent_type="codex",
        )

        assert unbound is not None
        assert bound is None

    async def test_releasing_one_shared_api_claim_keeps_all_other_refs(
        self, tmp_path,
    ):
        account = _api_acct()
        alloc = AccountAllocator(
            await _store(tmp_path, []),
            _FakeAgentApiStore(account),
        )
        claims = [
            await alloc.reserve(
                f"worker-{index}", "standard", account_id=account.id,
                agent_type="codex", allow_shared_agent_api=True,
            )
            for index in range(3)
        ]

        await alloc.release_claim(claims[1].claim_id)

        assert await alloc.get_claim(claims[0].claim_id) is claims[0]
        assert await alloc.get_claim(claims[1].claim_id) is None
        assert await alloc.get_claim(claims[2].claim_id) is claims[2]
        with pytest.raises(AccountClaimConflictError, match="actively claimed"):
            async with alloc.mutation_guard(account.id):
                pass

    async def test_quarantine_survives_claim_release_until_explicit_clear(
        self, tmp_path,
    ):
        alloc = AccountAllocator(await _store(tmp_path, [_acct(1)]))
        claim = await alloc.reserve("job-1", "standard")
        await alloc.quarantine(claim.account.id)
        await alloc.release_claim(claim.claim_id)

        assert await alloc.is_quarantined("a1") is True
        assert await alloc.reserve("job-2", "standard") is None

        await alloc.clear_quarantine("a1")
        assert (await alloc.reserve("job-2", "standard")).account.id == "a1"

    async def test_explicit_account_claim_uses_id_not_group(self, tmp_path):
        alloc = AccountAllocator(await _store(tmp_path, [
            _acct(1, group="other"), _acct(2),
        ]))
        claim = await alloc.reserve("job-1:0", "standard", account_id="a1")
        assert claim.account.id == "a1"
        assert await alloc.reserve("job-2:0", "other", account_id="a1") is None

    async def test_release_by_claim_not_owner(self, tmp_path):
        alloc = AccountAllocator(await _store(tmp_path, [_acct(1), _acct(2)]))
        first = await alloc.reserve("job-1", "standard")
        second = await alloc.reserve("job-1", "standard")
        await alloc.release_claim(first.claim_id)
        reused = await alloc.reserve("job-2", "standard", account_id=first.account.id)
        assert reused.account.id == first.account.id
        assert await alloc.get_claim(second.claim_id) is second

    async def test_release_claim_refuses_crossed_identity(self, tmp_path):
        alloc = AccountAllocator(await _store(tmp_path, [_acct(1)]))
        claim = await alloc.reserve("job-1:0", "standard", account_id="a1")

        with pytest.raises(AccountClaimConflictError, match="belongs to"):
            await alloc.release_claim(
                claim.claim_id,
                expected_owner="job-other:0",
                expected_account_id="a1",
            )

        assert await alloc.get_claim(claim.claim_id) is claim

    async def test_release_owner_account_keeps_other_account_claims(self, tmp_path):
        alloc = AccountAllocator(await _store(tmp_path, [_acct(1), _acct(2)]))
        first = await alloc.reserve("job-1:0", "standard", account_id="a1")
        second = await alloc.reserve("job-1:0", "standard", account_id="a2")

        await alloc.release_owner_account("job-1:0", "a1")

        assert await alloc.get_claim(first.claim_id) is None
        assert await alloc.get_claim(second.claim_id) is second

    async def test_mutation_guard_rejects_active_claim(self, tmp_path):
        alloc = AccountAllocator(await _store(tmp_path, [_acct(1)]))
        claim = await alloc.reserve("job-live:0", "standard", account_id="a1")

        with pytest.raises(AccountClaimConflictError, match="actively claimed"):
            async with alloc.mutation_guard("a1"):
                pass

        await alloc.release_claim(claim.claim_id)

    async def test_mutation_guard_serializes_store_update_before_reserve(
        self, tmp_path,
    ):
        store = await _store(tmp_path, [_acct(1)])
        alloc = AccountAllocator(store)

        async with alloc.mutation_guard("a1"):
            reserve = asyncio.create_task(
                alloc.reserve("job-next:0", "standard", account_id="a1")
            )
            await asyncio.sleep(0)
            assert reserve.done() is False
            await store.add(AccountDefinition(
                id="a1",
                email="updated@x.com",
                email_token="new-token",
            ))

        claim = await reserve
        assert claim.account.email == "updated@x.com"
        assert claim.account.email_token == "new-token"

    async def test_mutation_guard_does_not_block_unrelated_claim_lifecycle(
        self,
        tmp_path,
    ):
        alloc = AccountAllocator(
            await _store(tmp_path, [_acct(1), _acct(2)])
        )
        existing = await alloc.reserve(
            "job-old:0",
            "standard",
            account_id="a2",
        )

        async with alloc.mutation_guard("a1"):
            await asyncio.wait_for(
                alloc.release_claim(existing.claim_id),
                timeout=0.5,
            )
            unrelated = await asyncio.wait_for(
                alloc.reserve(
                    "job-new:0",
                    "standard",
                    account_id="a2",
                ),
                timeout=0.5,
            )
            assert unrelated is not None
            assert unrelated.account.id == "a2"

    async def test_mutation_guard_repeated_cancel_still_releases_marker(
        self,
        tmp_path,
    ):
        alloc = AccountAllocator(await _store(tmp_path, [_acct(1)]))
        entered = asyncio.Event()

        async def mutate_forever():
            async with alloc.mutation_guard("a1"):
                entered.set()
                await asyncio.Future()

        mutation = asyncio.create_task(mutate_forever())
        await entered.wait()
        completed = alloc._account_mutations["a1"]

        # Hold the allocator lock so cancellation enters marker cleanup but
        # cannot finish before a second cancellation arrives.
        await alloc._lock.acquire()
        try:
            mutation.cancel()
            await asyncio.sleep(0)
            mutation.cancel()
        finally:
            alloc._lock.release()

        with pytest.raises(asyncio.CancelledError):
            await mutation
        assert completed.is_set()
        assert "a1" not in alloc._account_mutations

        claim = await asyncio.wait_for(
            alloc.reserve("job-next:0", "standard", account_id="a1"),
            timeout=0.5,
        )
        assert claim is not None
        assert claim.account.id == "a1"


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
        request_id = conn.sent[0][1].login_request_id
        await bus.emit("ACCOUNT_LOGIN_RESULT", "w1", {
            "login_request_id": request_id, "account_id": "a1", "success": True,
        })
        outcome = await task
        assert outcome.success
        assert outcome.account_email == "a1@x.com"
        # ACCOUNT_LOGIN was sent
        assert conn.sent[0][1].type == "ACCOUNT_LOGIN"

    async def test_login_failure(self, tmp_path):
        bus = EventBus()
        conn = FakeConn()
        coord = LoginCoordinator(conn, bus, timeout=5)
        task = asyncio.create_task(coord.login("w1", _acct(1), "/root/.claude"))
        await asyncio.sleep(0.01)
        await bus.emit("ACCOUNT_LOGIN_RESULT", "w1", {
            "login_request_id": conn.sent[0][1].login_request_id,
            "account_id": "a1", "success": False, "error": "boom",
        })
        outcome = await task
        assert not outcome.success
        assert outcome.error == "boom"

    @pytest.mark.parametrize(
        ("quarantine_enabled", "expected_quarantine"),
        [(True, ["a1"]), (False, [])],
    )
    async def test_uncertain_result_quarantines_only_reusable_worker(
        self,
        quarantine_enabled,
        expected_quarantine,
    ):
        bus = EventBus()
        conn = FakeConn()
        quarantined = []
        coord = LoginCoordinator(
            conn,
            bus,
            timeout=5,
            quarantine_account=lambda account_id: (
                quarantined.append(account_id) or asyncio.sleep(0)
            ),
        )
        task = asyncio.create_task(coord.login(
            "w1",
            _acct(1),
            "/root/.claude",
            # An EIP caller passes False because destroying its instance is the
            # isolation boundary; a reusable ordinary worker passes True.
            quarantine_on_uncertain_cleanup=quarantine_enabled,
        ))
        await asyncio.sleep(0)
        request = conn.sent[0][1]
        await bus.emit("ACCOUNT_LOGIN_RESULT", "w1", {
            "login_request_id": request.login_request_id,
            "account_id": "a1",
            "success": False,
            "error": "account login cleanup could not be verified",
            "cleanup_complete": False,
        })

        outcome = await task

        assert outcome.success is False
        assert quarantined == expected_quarantine

    @pytest.mark.parametrize(
        ("success", "cleanup_complete", "expected_quarantine"),
        [
            (False, None, ["a1"]),  # legacy failed result: no proof
            (False, True, []),      # failed, verified rollback
            (True, None, []),       # success intentionally commits credentials
        ],
    )
    async def test_legacy_result_cleanup_compatibility_is_fail_closed(
        self,
        success,
        cleanup_complete,
        expected_quarantine,
    ):
        bus = EventBus()
        conn = FakeConn()
        quarantined = []
        coord = LoginCoordinator(
            conn,
            bus,
            timeout=5,
            quarantine_account=lambda account_id: (
                quarantined.append(account_id) or asyncio.sleep(0)
            ),
        )
        task = asyncio.create_task(
            coord.login("w1", _acct(1), "/root/.claude")
        )
        await asyncio.sleep(0)
        request = conn.sent[0][1]
        result = {
            "login_request_id": request.login_request_id,
            "account_id": "a1",
            "success": success,
        }
        if cleanup_complete is not None:
            result["cleanup_complete"] = cleanup_complete

        await bus.emit("ACCOUNT_LOGIN_RESULT", "w1", result)
        outcome = await task

        assert outcome.success is success
        assert quarantined == expected_quarantine

    async def test_codex_password_and_agent_type_are_sent_to_worker(self):
        bus = EventBus()
        conn = FakeConn()
        coord = LoginCoordinator(conn, bus, timeout=5)
        account = _acct(
            2, agent_type="codex", password="openai-secret"
        )
        task = asyncio.create_task(
            coord.login(
                "w1", account, "/root/.codex",
                login_timeout_seconds=1100,
            )
        )
        await asyncio.sleep(0)

        message = conn.sent[0][1]
        assert message.agent_type == "codex"
        assert message.password == "openai-secret"
        assert message.config_dir == "/root/.codex"
        assert message.login_timeout_seconds == 1100
        await bus.emit("ACCOUNT_LOGIN_RESULT", "w1", {
            "login_request_id": message.login_request_id,
            "account_id": account.id,
            "success": True,
        })
        assert (await task).success is True

    async def test_codex_token_only_credential_is_sent_to_worker(self):
        bus = EventBus()
        conn = FakeConn()
        coord = LoginCoordinator(conn, bus, timeout=5)
        account = AccountDefinition(
            id="codex-token-only",
            email="token-only@163.com",
            agent_type="codex",
            email_token="mail-query-token",
        )
        task = asyncio.create_task(
            coord.login("w1", account, "/root/.codex-token-only")
        )
        await asyncio.sleep(0)

        message = conn.sent[0][1]
        assert message.agent_type == "codex"
        assert message.email_token == "mail-query-token"
        assert message.password == ""
        await bus.emit("ACCOUNT_LOGIN_RESULT", "w1", {
            "login_request_id": message.login_request_id,
            "account_id": account.id,
            "success": True,
        })
        assert (await task).success is True

    async def test_correlated_otp_is_forwarded_without_being_retained(self):
        from elastic_agent.core.protocols.messages import AccountLoginOtpMessage

        bus = EventBus()
        conn = FakeConn()
        coord = LoginCoordinator(conn, bus, timeout=5)
        account = _acct(2, agent_type="codex", password="openai-secret")
        task = asyncio.create_task(
            coord.login("w1", account, "/root/.codex")
        )
        await asyncio.sleep(0)
        login_message = conn.sent[0][1]

        await bus.emit("ACCOUNT_LOGIN_OTP_REQUIRED", "w1", {
            "login_request_id": login_message.login_request_id,
            "account_id": account.id,
            "challenge_id": "a" * 32,
            "expires_at": int(time.time()) + 60,
        })
        attempts = coord.list_otp_challenges()
        assert attempts == [{
            "login_request_id": login_message.login_request_id,
            "worker_id": "w1",
            "account_id": account.id,
            "account_email": account.email,
            "job_id": "",
            "job_name": "",
            "shard_index": None,
            "challenge_id": "a" * 32,
            "expires_at": attempts[0]["expires_at"],
            "status": "awaiting_otp",
        }]

        result = await coord.submit_otp(
            login_message.login_request_id, "a" * 32, "123456"
        )
        otp_message = conn.sent[-1][1]
        assert isinstance(otp_message, AccountLoginOtpMessage)
        assert otp_message.code == "123456"
        assert result["status"] == "verifying_otp"
        assert coord.list_otp_challenges() == []

        await bus.emit("ACCOUNT_LOGIN_RESULT", "w1", {
            "login_request_id": login_message.login_request_id,
            "account_id": account.id,
            "success": True,
        })
        assert (await task).success is True

    async def test_concurrent_otp_challenges_keep_exact_worker_account_context(
        self,
    ):
        from elastic_agent.core.protocols.messages import AccountLoginOtpMessage

        bus = EventBus()
        conn = FakeConn()
        coord = LoginCoordinator(conn, bus, timeout=5)
        first_account = _acct(
            1, agent_type="codex", password="first-openai-secret",
        )
        second_account = _acct(
            2, agent_type="codex", password="second-openai-secret",
        )
        first_login = asyncio.create_task(coord.login(
            "worker-a",
            first_account,
            "/home/ubuntu/.codex-a",
            job_id="job-a",
            job_name="batch-a",
            shard_index=0,
        ))
        second_login = asyncio.create_task(coord.login(
            "worker-b",
            second_account,
            "/home/ubuntu/.codex-b",
            job_id="job-b",
            job_name="batch-b",
            shard_index=7,
        ))
        await asyncio.sleep(0)
        requests = {
            worker_id: message
            for worker_id, message in conn.sent
        }
        expires_at = int(time.time()) + 60
        # A valid request id cannot be replayed from another authenticated
        # Worker, and a Worker cannot relabel it as another account.
        await bus.emit("ACCOUNT_LOGIN_OTP_REQUIRED", "worker-b", {
            "login_request_id": requests["worker-a"].login_request_id,
            "account_id": first_account.id,
            "challenge_id": "c" * 32,
            "expires_at": expires_at,
        })
        await bus.emit("ACCOUNT_LOGIN_OTP_REQUIRED", "worker-a", {
            "login_request_id": requests["worker-a"].login_request_id,
            "account_id": second_account.id,
            "challenge_id": "d" * 32,
            "expires_at": expires_at,
        })
        assert coord.list_otp_challenges() == []

        await bus.emit("ACCOUNT_LOGIN_OTP_REQUIRED", "worker-a", {
            "login_request_id": requests["worker-a"].login_request_id,
            "account_id": first_account.id,
            "challenge_id": "a" * 32,
            "expires_at": expires_at,
        })
        await bus.emit("ACCOUNT_LOGIN_OTP_REQUIRED", "worker-b", {
            "login_request_id": requests["worker-b"].login_request_id,
            "account_id": second_account.id,
            "challenge_id": "b" * 32,
            "expires_at": expires_at,
        })

        attempts = {
            str(item["worker_id"]): item
            for item in coord.list_otp_challenges()
        }
        assert {
            worker_id: (
                item["account_id"],
                item["account_email"],
                item["job_id"],
                item["job_name"],
                item["shard_index"],
            )
            for worker_id, item in attempts.items()
        } == {
            "worker-a": (first_account.id, first_account.email, "job-a", "batch-a", 0),
            "worker-b": (second_account.id, second_account.email, "job-b", "batch-b", 7),
        }

        await coord.submit_otp(
            requests["worker-a"].login_request_id,
            "a" * 32,
            "123456",
        )
        sent_worker_id, sent_message = conn.sent[-1]
        assert sent_worker_id == "worker-a"
        assert isinstance(sent_message, AccountLoginOtpMessage)
        assert sent_message.account_id == first_account.id
        assert sent_message.code == "123456"
        remaining = coord.list_otp_challenges()
        assert [item["worker_id"] for item in remaining] == ["worker-b"]
        assert remaining[0]["account_id"] == second_account.id

        for worker_id, account, login in (
            ("worker-a", first_account, first_login),
            ("worker-b", second_account, second_login),
        ):
            await bus.emit("ACCOUNT_LOGIN_RESULT", worker_id, {
                "login_request_id": requests[worker_id].login_request_id,
                "account_id": account.id,
                "success": True,
            })
            assert (await login).success is True

    async def test_otp_rejects_wrong_challenge_without_sending_code(self):
        bus = EventBus()
        conn = FakeConn()
        coord = LoginCoordinator(conn, bus, timeout=5)
        account = _acct(2, agent_type="codex", password="openai-secret")
        task = asyncio.create_task(
            coord.login("w1", account, "/root/.codex")
        )
        await asyncio.sleep(0)
        login_message = conn.sent[0][1]
        await bus.emit("ACCOUNT_LOGIN_OTP_REQUIRED", "w1", {
            "login_request_id": login_message.login_request_id,
            "account_id": account.id,
            "challenge_id": "b" * 32,
            "expires_at": int(time.time()) + 60,
        })

        with pytest.raises(ValueError, match="does not match"):
            await coord.submit_otp(
                login_message.login_request_id, "c" * 32, "123456"
            )
        assert len(conn.sent) == 1

        await bus.emit("ACCOUNT_LOGIN_RESULT", "w1", {
            "login_request_id": login_message.login_request_id,
            "account_id": account.id,
            "success": False,
        })
        assert (await task).success is False

    async def test_concurrent_submissions_send_one_code_for_one_challenge(self):
        bus = EventBus()

        class BlockingOtpConn(FakeConn):
            def __init__(self):
                super().__init__()
                self.otp_send_started = asyncio.Event()
                self.release_otp_send = asyncio.Event()

            async def send_command(self, worker_id, message):
                if message.type == "ACCOUNT_LOGIN_OTP":
                    self.otp_send_started.set()
                    await self.release_otp_send.wait()
                await super().send_command(worker_id, message)

        conn = BlockingOtpConn()
        coord = LoginCoordinator(conn, bus, timeout=5)
        account = _acct(2, agent_type="codex", password="openai-secret")
        login = asyncio.create_task(
            coord.login("w1", account, "/root/.codex")
        )
        await asyncio.sleep(0)
        request = conn.sent[0][1]
        await bus.emit("ACCOUNT_LOGIN_OTP_REQUIRED", "w1", {
            "login_request_id": request.login_request_id,
            "account_id": account.id,
            "challenge_id": "a" * 32,
            "expires_at": int(time.time()) + 60,
        })

        first_submit = asyncio.create_task(coord.submit_otp(
            request.login_request_id, "a" * 32, "123456",
        ))
        await conn.otp_send_started.wait()
        with pytest.raises(ValueError, match="already being submitted"):
            await coord.submit_otp(
                request.login_request_id, "a" * 32, "654321",
            )
        conn.release_otp_send.set()
        assert (await first_submit)["status"] == "verifying_otp"
        otp_messages = [
            message for _worker_id, message in conn.sent
            if message.type == "ACCOUNT_LOGIN_OTP"
        ]
        assert [message.code for message in otp_messages] == ["123456"]

        await bus.emit("ACCOUNT_LOGIN_RESULT", "w1", {
            "login_request_id": request.login_request_id,
            "account_id": account.id,
            "success": True,
        })
        assert (await login).success is True

    async def test_otp_transport_failure_restores_active_challenge(self):
        bus = EventBus()

        class FailingOtpConn(FakeConn):
            fail_otp = True

            async def send_command(self, worker_id, message):
                if self.fail_otp and message.type == "ACCOUNT_LOGIN_OTP":
                    raise RuntimeError("temporary transport failure")
                await super().send_command(worker_id, message)

        conn = FailingOtpConn()
        coord = LoginCoordinator(conn, bus, timeout=5)
        account = _acct(2, agent_type="codex", password="openai-secret")
        login = asyncio.create_task(
            coord.login("w1", account, "/root/.codex")
        )
        await asyncio.sleep(0)
        request = conn.sent[0][1]
        await bus.emit("ACCOUNT_LOGIN_OTP_REQUIRED", "w1", {
            "login_request_id": request.login_request_id,
            "account_id": account.id,
            "challenge_id": "a" * 32,
            "expires_at": int(time.time()) + 60,
        })

        with pytest.raises(RuntimeError, match="temporary transport failure"):
            await coord.submit_otp(
                request.login_request_id, "a" * 32, "123456",
            )
        assert coord.list_otp_challenges()[0]["status"] == "awaiting_otp"

        conn.fail_otp = False
        assert (
            await coord.submit_otp(
                request.login_request_id, "a" * 32, "123456",
            )
        )["status"] == "verifying_otp"
        await bus.emit("ACCOUNT_LOGIN_RESULT", "w1", {
            "login_request_id": request.login_request_id,
            "account_id": account.id,
            "success": True,
        })
        assert (await login).success is True

    async def test_login_timeout(self, tmp_path):
        from elastic_agent.core.protocols.messages import (
            AccountLoginCancelMessage,
        )

        bus = EventBus()
        conn = FakeConn()
        coord = LoginCoordinator(conn, bus, timeout=0.02, cancel_timeout=1)
        login = asyncio.create_task(
            coord.login("w1", _acct(1), "/root/.claude")
        )
        await asyncio.sleep(0.03)
        cancel = conn.sent[-1][1]
        assert isinstance(cancel, AccountLoginCancelMessage)
        await bus.emit("ACCOUNT_LOGIN_CANCELLED", "w1", {
            "login_request_id": cancel.login_request_id,
            "account_id": cancel.account_id,
            "cleanup_complete": True,
        })

        outcome = await login
        assert not outcome.success
        assert "timed out" in outcome.error
        assert cancel.reason == "manager_timeout"

    async def test_manager_task_cancellation_cancels_worker_login(self):
        from elastic_agent.core.protocols.messages import AccountLoginCancelMessage

        bus = EventBus()
        conn = FakeConn()
        coord = LoginCoordinator(conn, bus, timeout=5, cancel_timeout=1)
        login = asyncio.create_task(
            coord.login("w1", _acct(1), "/root/.claude")
        )
        await asyncio.sleep(0)

        login.cancel()
        for _ in range(20):
            if len(conn.sent) >= 2:
                break
            await asyncio.sleep(0.01)
        cancel = conn.sent[-1][1]
        assert isinstance(cancel, AccountLoginCancelMessage)
        await bus.emit("ACCOUNT_LOGIN_CANCELLED", "w1", {
            "login_request_id": cancel.login_request_id,
            "account_id": cancel.account_id,
            "cleanup_complete": True,
        })
        with pytest.raises(asyncio.CancelledError):
            await login

        assert cancel.reason == "manager_cancelled"

    async def test_disconnect_fails_immediately_and_quarantines_account(self):
        bus = EventBus()
        conn = FakeConn()
        quarantined = []
        coord = LoginCoordinator(
            conn,
            bus,
            timeout=30,
            quarantine_account=lambda account_id: (
                quarantined.append(account_id) or asyncio.sleep(0)
            ),
        )
        login = asyncio.create_task(
            coord.login("w1", _acct(1), "/root/.claude")
        )
        await asyncio.sleep(0)

        await bus.emit("WORKER_DISCONNECTED", "w1", {})

        outcome = await asyncio.wait_for(login, timeout=0.2)
        assert outcome.success is False
        assert outcome.error == "worker disconnected during account login"
        assert quarantined == ["a1"]

    async def test_unconfirmed_cancel_quarantines_only_when_requested(self):
        first_quarantine = []
        bus = EventBus()
        coord = LoginCoordinator(
            FakeConn(),
            bus,
            timeout=0.01,
            cancel_timeout=0.01,
            quarantine_account=lambda account_id: (
                first_quarantine.append(account_id) or asyncio.sleep(0)
            ),
        )
        outcome = await coord.login("w1", _acct(1), "/root/.claude")
        assert outcome.success is False
        assert first_quarantine == ["a1"]

        eip_quarantine = []
        coord = LoginCoordinator(
            FakeConn(),
            EventBus(),
            timeout=0.01,
            cancel_timeout=0.01,
            quarantine_account=lambda account_id: (
                eip_quarantine.append(account_id) or asyncio.sleep(0)
            ),
        )
        outcome = await coord.login(
            "w2",
            _acct(1),
            "/root/.claude",
            quarantine_on_uncertain_cleanup=False,
        )
        assert outcome.success is False
        assert eip_quarantine == []

    async def test_otp_send_does_not_delete_a_newer_retry_challenge(self):
        bus = EventBus()

        class RacingConn(FakeConn):
            async def send_command(self, worker_id, message):
                await super().send_command(worker_id, message)
                if message.type == "ACCOUNT_LOGIN_OTP":
                    await bus.emit("ACCOUNT_LOGIN_OTP_REQUIRED", worker_id, {
                        "login_request_id": message.login_request_id,
                        "account_id": message.account_id,
                        "challenge_id": "d" * 32,
                        "expires_at": int(time.time()) + 60,
                    })

        conn = RacingConn()
        coord = LoginCoordinator(conn, bus, timeout=5)
        account = _acct(2, agent_type="codex", password="openai-secret")
        login = asyncio.create_task(
            coord.login("w1", account, "/home/ubuntu/.codex")
        )
        await asyncio.sleep(0)
        request = conn.sent[0][1]
        await bus.emit("ACCOUNT_LOGIN_OTP_REQUIRED", "w1", {
            "login_request_id": request.login_request_id,
            "account_id": account.id,
            "challenge_id": "e" * 32,
            "expires_at": int(time.time()) + 60,
        })

        await coord.submit_otp(request.login_request_id, "e" * 32, "123456")

        assert coord.list_otp_challenges()[0]["challenge_id"] == "d" * 32
        await bus.emit("ACCOUNT_LOGIN_RESULT", "w1", {
            "login_request_id": request.login_request_id,
            "account_id": account.id,
            "success": False,
        })
        assert (await login).success is False

    async def test_malicious_otp_challenge_id_is_ignored(self):
        bus = EventBus()
        conn = FakeConn()
        coord = LoginCoordinator(conn, bus, timeout=5)
        account = _acct(2, agent_type="codex", password="openai-secret")
        login = asyncio.create_task(
            coord.login("w1", account, "/home/ubuntu/.codex")
        )
        await asyncio.sleep(0)
        request = conn.sent[0][1]

        await bus.emit("ACCOUNT_LOGIN_OTP_REQUIRED", "w1", {
            "login_request_id": request.login_request_id,
            "account_id": account.id,
            "challenge_id": "</div><script>alert(1)</script>",
            "expires_at": int(time.time()) + 60,
        })

        assert coord.list_otp_challenges() == []
        await bus.emit("ACCOUNT_LOGIN_RESULT", "w1", {
            "login_request_id": request.login_request_id,
            "account_id": account.id,
            "success": False,
        })
        assert (await login).success is False

    async def test_late_result_cannot_complete_a_new_login_for_same_account(self):
        bus = EventBus()
        conn = FakeConn()
        coord = LoginCoordinator(
            conn, bus, timeout=0.02, cancel_timeout=0.01,
        )

        first = await coord.login("w1", _acct(1), "/root/.claude-old")
        assert not first.success
        old_request_id = conn.sent[-1][1].login_request_id

        second_task = asyncio.create_task(
            coord.login("w1", _acct(1), "/root/.claude-new")
        )
        await asyncio.sleep(0)
        new_request_id = conn.sent[-1][1].login_request_id
        assert new_request_id != old_request_id

        await bus.emit("ACCOUNT_LOGIN_RESULT", "w1", {
            "login_request_id": old_request_id,
            "account_id": "a1",
            "success": True,
        })
        await asyncio.sleep(0)
        assert not second_task.done()

        await bus.emit("ACCOUNT_LOGIN_RESULT", "w1", {
            "login_request_id": new_request_id,
            "account_id": "a1",
            "success": True,
        })
        assert (await second_task).success is True

    async def test_legacy_result_without_request_id_is_never_accepted(self):
        bus = EventBus()
        conn = FakeConn()
        coord = LoginCoordinator(conn, bus, timeout=1)
        login = asyncio.create_task(
            coord.login("w1", _acct(1), "/root/.claude")
        )
        await asyncio.sleep(0)
        request_id = conn.sent[-1][1].login_request_id

        await bus.emit("ACCOUNT_LOGIN_RESULT", "w1", {
            "account_id": "a1",
            "success": True,
        })
        await asyncio.sleep(0)
        assert login.done() is False

        await bus.emit("ACCOUNT_LOGIN_RESULT", "w1", {
            "login_request_id": request_id,
            "account_id": "a1",
            "success": True,
        })
        assert (await login).success is True

    async def test_non_eip_legacy_result_requires_explicit_unambiguous_opt_in(self):
        bus = EventBus()
        conn = FakeConn()
        coord = LoginCoordinator(conn, bus, timeout=1)
        login = asyncio.create_task(coord.login(
            "w1",
            _acct(1),
            "/root/.claude",
            allow_legacy_result=True,
        ))
        await asyncio.sleep(0)

        await bus.emit("ACCOUNT_LOGIN_RESULT", "w1", {
            "account_id": "a1",
            "success": True,
        })

        assert (await login).success is True

    async def test_legacy_result_is_rejected_when_worker_has_two_pending_logins(self):
        bus = EventBus()
        conn = FakeConn()
        coord = LoginCoordinator(conn, bus, timeout=1)
        first = asyncio.create_task(coord.login(
            "w1", _acct(1), "/root/.claude-1", allow_legacy_result=True,
        ))
        second = asyncio.create_task(coord.login(
            "w1", _acct(2), "/root/.claude-2", allow_legacy_result=True,
        ))
        await asyncio.sleep(0)

        await bus.emit("ACCOUNT_LOGIN_RESULT", "w1", {
            "account_id": "a1",
            "success": True,
        })
        await asyncio.sleep(0)
        assert first.done() is False
        assert second.done() is False

        for _worker, message in conn.sent:
            await bus.emit("ACCOUNT_LOGIN_RESULT", "w1", {
                "login_request_id": message.login_request_id,
                "account_id": message.account_id,
                "success": True,
            })
        assert (await first).success is True
        assert (await second).success is True


# --------------------------------------------------------------------------
# AgentApiCoordinator
# --------------------------------------------------------------------------


class _FakeAgentApiStore:
    def __init__(self, account, *, providers=("cloudrouter", "apex")):
        self.account = account
        self.registry = SimpleNamespace(providers=tuple(providers))
        self.api_key = "cloudrouter-manager-secret"
        self.usage = {
            "state": "active",
            "known": True,
            "available": True,
            "reason": "active",
        }
        self.decision = {
            "known": True,
            "available": True,
            "reason": "active",
        }
        self.fetch_calls = 0
        self.read_calls = 0
        self.runtime_marks: list[tuple[str, str]] = []
        self.runtime_quota_marks: list[tuple[str, str]] = []

    async def list(self):
        return [self.account]

    async def fetch_usage(self, account_id, force=False):
        assert account_id == self.account.id
        self.fetch_calls += 1
        return dict(self.usage)

    def availability_decision(self, account_id):
        assert account_id == self.account.id
        return dict(self.decision)

    def read_api_key(self, account_id):
        assert account_id == self.account.id
        self.read_calls += 1
        return self.api_key

    async def mark_runtime_unavailable(self, account_id, reason):
        self.runtime_marks.append((account_id, reason))

    async def mark_runtime_quota_unavailable(self, account_id, reason):
        self.runtime_quota_marks.append((account_id, reason))


class _MultiAgentApiStore:
    def __init__(self, accounts):
        self.accounts = list(accounts)

    async def list(self):
        return list(self.accounts)

    async def fetch_usage(self, account_id, force=False):
        return {
            "account_id": account_id,
            "known": True,
            "available": True,
            "reason": "active",
        }

    def availability_decision(self, account_id):
        return {
            "known": True,
            "available": True,
            "reason": "active",
        }


def _api_acct():
    return SimpleNamespace(
        id="cloudrouter-1",
        email="Shared CloudRouter",
        name="Shared CloudRouter",
        group="standard",
        enabled=True,
        auth_kind="agent_api",
        api_provider="cloudrouter",
        models={
            "claude": ["claude-opus-4-8"],
            "codex": ["gpt-5.4"],
        },
        supported_agent_types=["claude", "codex"],
        supports_agent_type=lambda agent_type: agent_type in {"claude", "codex"},
        supports_model=lambda agent_type, model: (
            not model
            or model
            in {
                "claude": {"claude-opus-4-8"},
                "codex": {"gpt-5.4"},
            }.get(agent_type, set())
        ),
    )


def _apex_api_acct():
    return SimpleNamespace(
        id="apex-3",
        email="Shared ApexRouter",
        name="Shared ApexRouter",
        group="standard",
        enabled=True,
        auth_kind="agent_api",
        api_provider="apex",
        models={"claude": [], "codex": ["gpt-5.4"]},
        supported_agent_types=["codex"],
        supports_agent_type=lambda agent_type: agent_type == "codex",
        supports_model=lambda agent_type, model: (
            agent_type == "codex"
            and (not model or model == "gpt-5.4")
        ),
    )


class TestAgentApiCoordinator:
    async def test_registered_apex_codex_configures_provider_specific_home(
        self,
    ):
        bus = EventBus()
        conn = FakeConn()
        account = _apex_api_acct()
        store = _FakeAgentApiStore(account)
        store.api_key = "apex-manager-secret"
        coordinator = AgentApiCoordinator(conn, bus, store, timeout=5)

        pending = asyncio.create_task(coordinator.configure(
            "worker-apex",
            account,
            agent_type="codex",
            config_dir="/home/ubuntu/.codex-apex",
            model="gpt-5.4",
        ))
        await asyncio.sleep(0)
        worker_id, message = conn.sent[0]
        assert worker_id == "worker-apex"
        assert message.provider == "apex"
        assert message.account_id == "apex-3"
        assert message.agent_type == "codex"
        assert message.api_key == store.api_key

        actual_home = (
            "/home/ubuntu/.codex-apex/.elastic-agent-api/"
            "apex/apex-3/codex"
        )
        await bus.emit("AGENT_API_CONFIGURE_RESULT", "worker-apex", {
            "request_id": message.request_id,
            "account_id": account.id,
            "provider": "apex",
            "agent_type": "codex",
            "success": True,
            "error": None,
            "config_dir": actual_home,
        })

        outcome = await pending
        assert outcome.success is True
        assert outcome.auth_kind == "agent_api"
        assert outcome.config_dir == actual_home

    async def test_provider_must_be_registered_before_key_is_read(self):
        bus = EventBus()
        conn = FakeConn()
        account = _apex_api_acct()
        store = _FakeAgentApiStore(account, providers=("cloudrouter",))
        coordinator = AgentApiCoordinator(conn, bus, store, timeout=5)

        outcome = await coordinator.configure(
            "worker-apex",
            account,
            agent_type="codex",
            config_dir="/home/ubuntu/.codex-apex",
        )

        assert outcome.success is False
        assert outcome.error == "unsupported Agent API account"
        assert store.fetch_calls == 0
        assert store.read_calls == 0
        assert conn.sent == []

    @pytest.mark.parametrize("probe_kind", ["never", "drip"])
    async def test_configure_absolute_deadline_covers_usage_probe(
        self,
        probe_kind,
    ):
        bus = EventBus()
        conn = FakeConn()
        account = _api_acct()
        store = _FakeAgentApiStore(account)
        probe_started = asyncio.Event()
        probe_cancelled = asyncio.Event()

        async def stalled_probe(*_args, **_kwargs):
            probe_started.set()
            try:
                if probe_kind == "never":
                    await asyncio.Future()
                while True:
                    # Activity more frequent than an inactivity timeout models
                    # an upstream response that drip-feeds bytes forever.
                    await asyncio.sleep(0.001)
            finally:
                probe_cancelled.set()

        store.fetch_usage = AsyncMock(side_effect=stalled_probe)
        coordinator = AgentApiCoordinator(
            conn,
            bus,
            store,
            timeout=0.01,
        )

        outcome = await asyncio.wait_for(
            coordinator.configure(
                "worker-1",
                account,
                agent_type="codex",
                config_dir="/home/ubuntu/.codex-slot-1",
            ),
            timeout=0.5,
        )

        assert outcome.success is False
        assert outcome.auth_kind == "agent_api"
        assert outcome.error == "Agent API configuration timed out"
        assert probe_started.is_set()
        assert probe_cancelled.is_set()
        assert conn.sent == []
        assert coordinator._pending == {}

    async def test_startup_recovery_gate_excludes_api_but_keeps_oauth(
        self, tmp_path,
    ):
        native_store = await _store(tmp_path, [
            _acct(9, agent_type="codex", password="native-password"),
        ])
        account = _api_acct()
        api_store = _FakeAgentApiStore(account)
        recovery_ready = False
        allocator = AccountAllocator(
            native_store,
            api_store,
            agent_api_admission=lambda: recovery_ready,
        )

        fallback = await allocator.allocate(
            "worker-native",
            "standard",
            agent_type="codex",
        )
        assert fallback.id == "a9"
        assert api_store.fetch_calls == 0
        await allocator.release_worker("worker-native")

        blocked = await allocator.allocate(
            "worker-explicit-api",
            "standard",
            account_id=account.id,
            agent_type="codex",
        )
        assert blocked is None
        assert api_store.fetch_calls == 0

        recovery_ready = True
        selected = await allocator.allocate(
            "worker-api",
            "standard",
            account_id=account.id,
            agent_type="codex",
        )
        assert selected.id == account.id
        assert api_store.fetch_calls == 1

    async def test_allocator_prefers_live_api_and_falls_back_when_unavailable(
        self, tmp_path,
    ):
        native_store = await _store(tmp_path, [
            _acct(9, agent_type="codex", password="native-password"),
        ])
        account = _api_acct()
        api_store = _FakeAgentApiStore(account)
        allocator = AccountAllocator(native_store, api_store)

        preferred = await allocator.allocate(
            "worker-api",
            "standard",
            agent_type="codex",
        )
        assert preferred.id == account.id
        await allocator.release_worker("worker-api")

        api_store.decision = {
            "known": True,
            "available": False,
            "reason": "quota_exhausted",
        }
        fallback = await allocator.allocate(
            "worker-native",
            "standard",
            agent_type="codex",
        )
        assert fallback.id == "a9"

    async def test_explicit_oauth_does_not_wait_for_unrelated_api_refresh(
        self,
        tmp_path,
    ):
        native_store = await _store(tmp_path, [
            _acct(9, agent_type="codex", password="native-password"),
        ])
        api_store = _FakeAgentApiStore(_api_acct())
        refresh_started = asyncio.Event()

        async def never_finishes(*_args, **_kwargs):
            refresh_started.set()
            await asyncio.Future()

        api_store.fetch_usage = AsyncMock(side_effect=never_finishes)
        allocator = AccountAllocator(native_store, api_store)

        claim = await asyncio.wait_for(
            allocator.reserve(
                "worker-native",
                "standard",
                account_id="a9",
                agent_type="codex",
            ),
            timeout=0.5,
        )
        assert claim is not None
        assert claim.account.id == "a9"
        assert refresh_started.is_set() is False
        api_store.fetch_usage.assert_not_awaited()

    async def test_api_refresh_deadline_excludes_stalled_key_and_falls_back(
        self,
        tmp_path,
        monkeypatch,
    ):
        monkeypatch.setattr(
            "elastic_agent.core.batch_hooks."
            "_AGENT_API_USAGE_REFRESH_TIMEOUT_SECONDS",
            0.01,
        )
        native_store = await _store(tmp_path, [
            _acct(9, agent_type="codex", password="native-password"),
        ])
        api_store = _FakeAgentApiStore(_api_acct())
        refresh_started = asyncio.Event()
        refresh_cancelled = asyncio.Event()

        async def stalls_until_cancelled(*_args, **_kwargs):
            refresh_started.set()
            try:
                await asyncio.Future()
            finally:
                refresh_cancelled.set()

        api_store.fetch_usage = AsyncMock(side_effect=stalls_until_cancelled)
        allocator = AccountAllocator(native_store, api_store)

        selected = await asyncio.wait_for(
            allocator.allocate(
                "worker-fallback",
                "standard",
                agent_type="codex",
            ),
            timeout=0.5,
        )
        assert selected.id == "a9"
        assert refresh_started.is_set()
        assert refresh_cancelled.is_set()

    async def test_allocator_honors_declared_agent_api_model(
        self, tmp_path,
    ):
        native_store = await _store(tmp_path, [
            _acct(9, agent_type="codex", password="native-password"),
        ])
        account = _api_acct()
        api_store = _FakeAgentApiStore(account)
        allocator = AccountAllocator(native_store, api_store)

        fallback = await allocator.allocate(
            "worker-model-fallback",
            "standard",
            agent_type="codex",
            model="gpt-not-on-router",
        )
        assert fallback.id == "a9"
        await allocator.release_worker("worker-model-fallback")

        selected = await allocator.allocate(
            "worker-model-match",
            "standard",
            agent_type="codex",
            model="gpt-5.4",
        )
        assert selected.id == account.id

    async def test_correlated_config_returns_actual_cli_home_without_key_leak(
        self,
    ):
        bus = EventBus()
        conn = FakeConn()
        account = _api_acct()
        store = _FakeAgentApiStore(account)
        coordinator = AgentApiCoordinator(conn, bus, store, timeout=5)

        pending = asyncio.create_task(coordinator.configure(
            "worker-1",
            account,
            agent_type="codex",
            config_dir="/home/ubuntu/.codex-slot-1",
        ))
        await asyncio.sleep(0)
        worker_id, message = conn.sent[0]
        assert worker_id == "worker-1"
        assert message.type == "AGENT_API_CONFIGURE"
        assert message.api_key == store.api_key
        assert store.api_key not in repr(message)

        actual_home = (
            "/home/ubuntu/.codex-slot-1/.elastic-agent-api/"
            "cloudrouter/cloudrouter-1/codex"
        )
        await bus.emit("AGENT_API_CONFIGURE_RESULT", "worker-1", {
            "request_id": message.request_id,
            "account_id": account.id,
            "provider": "cloudrouter",
            "agent_type": "codex",
            "success": True,
            "error": None,
            "config_dir": actual_home,
        })

        outcome = await pending
        assert outcome.success is True
        assert outcome.account_id == account.id
        assert outcome.config_dir == actual_home

    async def test_configure_rejects_wrong_worker_home_for_correlated_result(
        self,
    ):
        bus = EventBus()
        conn = FakeConn()
        account = _api_acct()
        store = _FakeAgentApiStore(account)
        coordinator = AgentApiCoordinator(conn, bus, store, timeout=5)

        pending = asyncio.create_task(coordinator.configure(
            "worker-1",
            account,
            agent_type="claude",
            config_dir="/home/ubuntu/.claude-slot-2",
        ))
        await asyncio.sleep(0)
        message = conn.sent[0][1]
        common = {
            "request_id": message.request_id,
            "account_id": account.id,
            "provider": "cloudrouter",
            "agent_type": "claude",
            "success": True,
            "error": None,
        }
        await bus.emit("AGENT_API_CONFIGURE_RESULT", "worker-1", {
            **common,
            "config_dir": (
                "/home/ubuntu/.claude-slot-1/.elastic-agent-api/"
                "cloudrouter/cloudrouter-1/claude"
            ),
        })
        await asyncio.sleep(0)
        assert pending.done() is False

        expected = (
            "/home/ubuntu/.claude-slot-2/.elastic-agent-api/"
            "cloudrouter/cloudrouter-1/claude"
        )
        await bus.emit("AGENT_API_CONFIGURE_RESULT", "worker-1", {
            **common,
            "config_dir": expected,
        })
        outcome = await pending
        assert outcome.success is True
        assert outcome.config_dir == expected

    async def test_transient_usage_cannot_revive_last_known_exhausted_key(
        self,
    ):
        bus = EventBus()
        conn = FakeConn()
        account = _api_acct()
        store = _FakeAgentApiStore(account)
        store.usage = {
            "state": "unknown",
            "known": False,
            "available": True,
            "reason": "upstream_unavailable",
            "last_known_available": False,
        }
        store.decision = {
            "known": True,
            "available": False,
            "reason": "quota_exhausted",
        }
        coordinator = AgentApiCoordinator(conn, bus, store, timeout=5)

        outcome = await coordinator.configure(
            "worker-1",
            account,
            agent_type="claude",
            config_dir="/home/ubuntu/.claude-slot-1",
        )

        assert outcome.success is False
        assert "quota_exhausted" in outcome.error
        assert conn.sent == []

    async def test_rechecks_recovery_admission_immediately_before_key_read(
        self,
    ):
        bus = EventBus()
        conn = FakeConn()
        account = _api_acct()
        store = _FakeAgentApiStore(account)
        admission = {"ready": True}
        real_fetch_usage = store.fetch_usage

        async def fetch_then_close_gate(account_id, force=False):
            usage = await real_fetch_usage(account_id, force=force)
            admission["ready"] = False
            return usage

        store.fetch_usage = fetch_then_close_gate
        coordinator = AgentApiCoordinator(
            conn,
            bus,
            store,
            timeout=5,
            agent_api_admission=lambda: admission["ready"],
        )

        outcome = await coordinator.configure(
            "worker-1",
            account,
            agent_type="claude",
            config_dir="/home/ubuntu/.claude-slot-1",
        )

        assert outcome.success is False
        assert outcome.auth_kind == "agent_api"
        assert "recovery" in outcome.error.lower()
        assert store.fetch_calls == 1
        assert store.read_calls == 0
        assert conn.sent == []

    async def test_recovery_admission_callback_error_fails_closed(
        self,
    ):
        bus = EventBus()
        conn = FakeConn()
        account = _api_acct()
        store = _FakeAgentApiStore(account)

        def broken_admission():
            raise RuntimeError("internal recovery detail")

        coordinator = AgentApiCoordinator(
            conn,
            bus,
            store,
            timeout=5,
            agent_api_admission=broken_admission,
        )

        outcome = await coordinator.configure(
            "worker-1",
            account,
            agent_type="codex",
            config_dir="/home/ubuntu/.codex-slot-1",
        )

        assert outcome.success is False
        assert "recovery" in outcome.error.lower()
        assert "internal recovery detail" not in outcome.error
        assert store.read_calls == 0
        assert conn.sent == []


# --------------------------------------------------------------------------
# login hook
# --------------------------------------------------------------------------


class TestLoginHook:
    @pytest.mark.parametrize(
        ("failure_kind", "expected_next_account"),
        [
            ("hard_quota", "a2"),
            ("auth_failure", "a2"),
            (None, "a1"),
            ("transient", "a1"),
        ],
    )
    async def test_eip_fresh_login_quarantines_only_proven_account_failure(
        self, tmp_path, failure_kind, expected_next_account,
    ):
        from elastic_agent.core.job_spec import JobSpec, RunSpec

        store = await _store(tmp_path, [
            _acct(1, agent_type="codex", password="first-password"),
            _acct(2, agent_type="codex", password="second-password"),
        ])
        mgr = FakeManager(tmp_path, store)
        allocator = AccountAllocator(store)
        coordinator = LoginCoordinator(
            mgr.connection_manager, mgr.event_bus, timeout=5,
        )
        hook = make_login_hook(mgr, allocator, coordinator)
        first_claim = await allocator.reserve(
            "job-1:0",
            "standard",
            account_id="a1",
            agent_type="codex",
            auth_kind="oauth",
            allow_durable_binding=True,
        )
        assert first_claim is not None
        spec = JobSpec(
            name="eip-codex",
            run=RunSpec(command="bench"),
            account={
                "agent_type": "codex",
                "auth_kind": "oauth",
                "binding": "eip",
                "ids": ["a1"],
            },
            fanout={"workers": 1},
        )

        pending = asyncio.create_task(hook(
            "worker-1",
            spec,
            "/home/ubuntu/.codex",
            "a1",
            first_claim.claim_id,
        ))
        await asyncio.sleep(0)
        worker_id, message = mgr.connection_manager.sent[0]
        result = {
            "login_request_id": message.login_request_id,
            "account_id": "a1",
            "success": False,
            "error": "Codex exec smoke test failed",
            "cleanup_complete": True,
        }
        if failure_kind is not None:
            result["failure_kind"] = failure_kind
        await mgr.event_bus.emit("ACCOUNT_LOGIN_RESULT", worker_id, result)

        outcome = await pending
        assert outcome.success is False
        assert outcome.failure_kind == (
            failure_kind
            if failure_kind in {"hard_quota", "auth_failure"}
            else None
        )
        await allocator.release_claim(
            first_claim.claim_id,
            expected_owner="job-1:0",
            expected_account_id="a1",
        )
        next_claim = await allocator.reserve(
            "job-2:0",
            "standard",
            agent_type="codex",
            auth_kind="oauth",
            allow_durable_binding=True,
        )

        assert next_claim is not None
        assert next_claim.account.id == expected_next_account

    async def test_preclaimed_account_cannot_bypass_auth_kind_constraint(
        self, tmp_path,
    ):
        from elastic_agent.core.job_spec import JobSpec, RunSpec

        native_store = await _store(tmp_path, [])
        account = _api_acct()
        api_store = _FakeAgentApiStore(account)
        mgr = FakeManager(tmp_path, native_store)
        allocator = AccountAllocator(native_store, api_store)
        claim = await allocator.reserve(
            "worker-1", "standard", account_id=account.id,
            agent_type="codex",
        )
        hook = make_login_hook(
            mgr, allocator,
            LoginCoordinator(
                mgr.connection_manager, mgr.event_bus, timeout=5,
            ),
            AgentApiCoordinator(
                mgr.connection_manager, mgr.event_bus, api_store, timeout=5,
            ),
        )

        outcome = await hook(
            "worker-1",
            JobSpec(
                name="oauth-only", run=RunSpec(command="x"),
                account={"agent_type": "codex", "auth_kind": "oauth"},
            ),
            "/home/ubuntu/.codex", account.id, claim.claim_id,
        )

        assert outcome.success is False
        assert "auth kind" in outcome.error
        assert mgr.connection_manager.sent == []

    async def test_oauth_constraint_uses_browser_login_without_api_lookup(
        self, tmp_path,
    ):
        from elastic_agent.core.job_spec import JobSpec, RunSpec

        native_store = await _store(tmp_path, [
            _acct(9, agent_type="codex", password="native-password"),
        ])
        api_store = _FakeAgentApiStore(_api_acct())
        api_store.list = AsyncMock(side_effect=AssertionError(
            "oauth login must not enumerate Agent API accounts"
        ))
        api_store.fetch_usage = AsyncMock(side_effect=AssertionError(
            "oauth login must not probe Agent API usage"
        ))
        mgr = FakeManager(tmp_path, native_store)
        hook = make_login_hook(
            mgr, AccountAllocator(native_store, api_store),
            LoginCoordinator(
                mgr.connection_manager, mgr.event_bus, timeout=5,
            ),
            AgentApiCoordinator(
                mgr.connection_manager, mgr.event_bus, api_store, timeout=5,
            ),
        )
        spec = JobSpec(
            name="oauth-only", run=RunSpec(command="x"),
            account={"agent_type": "codex", "auth_kind": "oauth"},
        )

        pending = asyncio.create_task(
            hook("worker-oauth", spec, "/home/ubuntu/.codex")
        )
        await asyncio.sleep(0)
        worker_id, message = mgr.connection_manager.sent[0]
        assert message.type == "ACCOUNT_LOGIN"
        await mgr.event_bus.emit("ACCOUNT_LOGIN_RESULT", worker_id, {
            "login_request_id": message.login_request_id,
            "account_id": "a9", "success": True,
        })

        outcome = await pending
        assert outcome.success is True and outcome.auth_kind == "oauth"
        api_store.list.assert_not_awaited()
        api_store.fetch_usage.assert_not_awaited()

    async def test_recovery_gate_blocks_preclaimed_api_before_key_send(
        self, tmp_path,
    ):
        from elastic_agent.core.job_spec import JobSpec, RunSpec

        native_store = await _store(tmp_path, [_acct(1)])
        account = _api_acct()
        api_store = _FakeAgentApiStore(account)
        mgr = FakeManager(tmp_path, native_store)
        mgr.binding_recovery_ready = False
        allocator = AccountAllocator(native_store, api_store)
        claim = await allocator.reserve(
            "worker-1",
            "standard",
            account_id=account.id,
            agent_type="codex",
        )
        coordinator = LoginCoordinator(
            mgr.connection_manager,
            mgr.event_bus,
            timeout=5,
        )
        hook = make_login_hook(
            mgr,
            allocator,
            coordinator,
            AgentApiCoordinator(
                mgr.connection_manager,
                mgr.event_bus,
                api_store,
                timeout=5,
            ),
        )

        outcome = await hook(
            "worker-1",
            JobSpec(
                name="api-job",
                run=RunSpec(command="x"),
                account={"agent_type": "codex"},
            ),
            "/home/ubuntu/.codex-slot",
            account.id,
            claim.claim_id,
        )

        assert outcome.success is False
        assert outcome.auth_kind == "agent_api"
        assert "recovery" in outcome.error.lower()
        assert api_store.read_calls == 0
        assert mgr.connection_manager.sent == []

    async def test_recovery_gate_does_not_block_oauth_login(self, tmp_path):
        from elastic_agent.core.job_spec import JobSpec, RunSpec

        store = await _store(tmp_path, [_acct(1)])
        mgr = FakeManager(tmp_path, store)
        mgr.binding_recovery_ready = False
        coordinator = LoginCoordinator(
            mgr.connection_manager,
            mgr.event_bus,
            timeout=5,
        )
        hook = make_login_hook(mgr, AccountAllocator(store), coordinator)
        spec = JobSpec(name="oauth-job", run=RunSpec(command="x"))

        pending = asyncio.create_task(
            hook("worker-oauth", spec, "/home/ubuntu/.claude")
        )
        await asyncio.sleep(0)
        worker_id, message = mgr.connection_manager.sent[0]
        assert message.type == "ACCOUNT_LOGIN"
        await mgr.event_bus.emit("ACCOUNT_LOGIN_RESULT", worker_id, {
            "login_request_id": message.login_request_id,
            "account_id": "a1",
            "success": True,
        })
        assert (await pending).success is True

    async def test_explicit_api_account_uses_api_projection_not_browser_login(
        self, tmp_path,
    ):
        from elastic_agent.core.job_spec import JobSpec, RunSpec

        native_store = await _store(tmp_path, [_acct(1)])
        account = _api_acct()
        api_store = _FakeAgentApiStore(account)
        mgr = FakeManager(tmp_path, native_store)
        mgr.agent_api_store = api_store
        allocator = AccountAllocator(native_store, api_store)
        login_coordinator = LoginCoordinator(
            mgr.connection_manager,
            mgr.event_bus,
            timeout=5,
        )
        api_coordinator = AgentApiCoordinator(
            mgr.connection_manager,
            mgr.event_bus,
            api_store,
            timeout=5,
        )
        hook = make_login_hook(
            mgr,
            allocator,
            login_coordinator,
            api_coordinator,
        )
        spec = JobSpec(
            name="api-job",
            run=RunSpec(command="x"),
            account={
                "agent_type": "codex",
                "ids": [account.id],
            },
        )

        pending = asyncio.create_task(hook(
            "worker-1",
            spec,
            "/home/ubuntu/.codex-slot",
            account.id,
        ))
        for _ in range(20):
            if mgr.connection_manager.sent:
                break
            await asyncio.sleep(0)
        assert mgr.connection_manager.sent
        worker_id, message = mgr.connection_manager.sent[0]
        assert worker_id == "worker-1"
        assert message.type == "AGENT_API_CONFIGURE"
        assert all(
            sent.type != "ACCOUNT_LOGIN"
            for _worker_id, sent in mgr.connection_manager.sent
        )
        await mgr.event_bus.emit("AGENT_API_CONFIGURE_RESULT", worker_id, {
            "request_id": message.request_id,
            "account_id": account.id,
            "provider": "cloudrouter",
            "agent_type": "codex",
            "success": True,
            "config_dir": (
                "/home/ubuntu/.codex-slot/.elastic-agent-api/"
                "cloudrouter/cloudrouter-1/codex"
            ),
        })

        outcome = await pending
        assert outcome.success is True
        assert outcome.account_id == account.id
        assert outcome.config_dir.endswith(
            "/.elastic-agent-api/cloudrouter/cloudrouter-1/codex"
        )

    async def test_allocates_and_logs_in(self, tmp_path):
        from elastic_agent.core.job_spec import JobSpec, RunSpec
        store = await _store(tmp_path, [_acct(1)])
        mgr = FakeManager(tmp_path, store)
        alloc = AccountAllocator(store)
        coord = LoginCoordinator(mgr.connection_manager, mgr.event_bus, timeout=5)
        hook = make_login_hook(mgr, alloc, coord)
        spec = JobSpec(
            name="j",
            run=RunSpec(command="x"),
            account={"login_timeout_seconds": 1100},
        )
        run = SimpleNamespace(ctx=SimpleNamespace(shard_index=4))
        job = SimpleNamespace(job_id="job-1", spec=spec, runs={"w1": run})
        mgr.batch = SimpleNamespace(
            job_id_for_worker=lambda worker_id: (
                "job-1" if worker_id == "w1" else None
            ),
            get_job=lambda job_id: job if job_id == "job-1" else None,
        )

        task = asyncio.create_task(hook("w1", spec, "/root/.claude"))
        await asyncio.sleep(0.01)
        login_message = mgr.connection_manager.sent[0][1]
        assert login_message.login_timeout_seconds == 1100
        await mgr.event_bus.emit("ACCOUNT_LOGIN_OTP_REQUIRED", "w1", {
            "login_request_id": login_message.login_request_id,
            "account_id": "a1",
            "challenge_id": "f" * 32,
            "expires_at": int(time.time()) + 60,
        })
        challenge = coord.list_otp_challenges()[0]
        assert challenge["account_email"] == "a1@x.com"
        assert challenge["job_id"] == "job-1"
        assert challenge["job_name"] == "j"
        assert challenge["shard_index"] == 4
        await mgr.event_bus.emit("ACCOUNT_LOGIN_RESULT", "w1", {
            "login_request_id": login_message.login_request_id,
            "account_id": "a1", "success": True,
        })
        outcome = await task
        assert outcome.success and outcome.account_id == "a1"

    async def test_codex_rejects_uncorrelated_legacy_login_result(self, tmp_path):
        from elastic_agent.core.job_spec import JobSpec, RunSpec

        store = await _store(tmp_path, [
            _acct(1, agent_type="codex", password="openai-password"),
        ])
        mgr = FakeManager(tmp_path, store)
        coord = LoginCoordinator(mgr.connection_manager, mgr.event_bus, timeout=5)
        hook = make_login_hook(mgr, AccountAllocator(store), coord)
        spec = JobSpec(
            name="codex-job",
            run=RunSpec(command="x"),
            account={"agent_type": "codex"},
        )

        task = asyncio.create_task(hook("w1", spec, "/root/.codex"))
        await asyncio.sleep(0.01)
        message = mgr.connection_manager.sent[0][1]
        await mgr.event_bus.emit("ACCOUNT_LOGIN_RESULT", "w1", {
            "account_id": "a1",
            "success": True,
        })
        await asyncio.sleep(0)
        assert task.done() is False

        await mgr.event_bus.emit("ACCOUNT_LOGIN_RESULT", "w1", {
            "login_request_id": message.login_request_id,
            "account_id": "a1",
            "success": True,
        })
        assert (await task).success is True

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
# EIP-bound reserve / attach / release hooks
# --------------------------------------------------------------------------


class FakeBindingManager:
    def __init__(self):
        self.calls = []
        self.binding = None
        self.release_error = None
        self.released: set[str] = set()
        self.leases = {}

    async def reserve(self, account_id, *, email, job_id, slot, region):
        self.calls.append(("reserve", account_id, job_id, slot, region))
        self.binding = SimpleNamespace(
            account_id=account_id,
            email=email,
            eip_allocation_id=f"eipalloc-{account_id}",
            eip_ip="198.51.100.42",
            region=region,
        )
        lease = SimpleNamespace(
            lease_id=f"lease-{account_id}",
            account_id=account_id,
            job_id=job_id,
            worker_id="",
            instance_id=None,
            state="reserved",
        )
        self.leases[lease.lease_id] = lease
        return lease

    async def get_lease(self, lease_id):
        return self.leases.get(lease_id)

    async def get_binding(self, account_id):
        return self.binding if self.binding and self.binding.account_id == account_id else None

    async def attach_instance(self, lease_id, instance_id, worker_id):
        self.calls.append(("attach", lease_id, instance_id, worker_id))
        lease = self.leases[lease_id]
        lease.instance_id = instance_id
        lease.worker_id = worker_id
        lease.state = "attached"
        return lease

    async def release(
        self, lease_id, cleanup_worker=None, *, expected_lease=None
    ):
        self.calls.append(("release", lease_id))
        if self.release_error:
            raise self.release_error
        self.released.add(lease_id)
        lease = self.leases.get(lease_id)
        if lease is None:
            return None
        if cleanup_worker:
            await cleanup_worker(lease)
        lease.state = "released"
        return lease


class TestBoundHooks:
    @staticmethod
    def _spec(*, account_id="a1", region="ap-northeast-1"):
        from elastic_agent.core.job_spec import JobSpec, RunSpec
        return JobSpec(
            name="bound",
            run=RunSpec(command="x"),
            account={"binding": "eip", "ids": [account_id]},
            fanout={"workers": 1, "region": region},
        )

    async def _setup(self, tmp_path):
        store = await _store(tmp_path, [_acct(1), _acct(2)])
        mgr = FakeManager(tmp_path, store)
        mgr.config.provider.type = "aws"
        mgr.config.provider.aws.region = "ap-northeast-1"
        mgr.binding_manager = FakeBindingManager()
        alloc = AccountAllocator(store)
        hooks = make_bound_hooks(mgr, alloc)
        return mgr, alloc, hooks

    async def test_explicit_reserve_attach_updates_registry_then_release(self, tmp_path):
        mgr, alloc, (reserve, attach, release) = await self._setup(tmp_path)
        assignment = await reserve("job-1", 0, self._spec(), "a1")
        assert assignment.account_id == "a1"
        assert assignment.eip == "198.51.100.42"

        attached = await attach("w1", assignment)
        assert attached.eip_allocation_id == "eipalloc-a1"
        assert mgr.registry.node.public_ip == "198.51.100.42"
        assert mgr.registry.node.metadata["job_id"] == "job-1"
        assert mgr.binding_manager.calls[1] == ("attach", "lease-a1", "i-1", "w1")

        await release(attached, "w1")
        assert mgr.registry.removed == ["w1"]
        assert mgr.connection_manager.disconnected == ["w1"]
        assert await alloc.get_claim(attached.claim_id) is None

    async def test_eip_reserve_enforces_auth_kind_before_durable_side_effect(
        self, tmp_path,
    ):
        mgr, _alloc, (reserve, _attach, _release) = await self._setup(tmp_path)
        spec = self._spec()
        spec.account.auth_kind = "agent_api"

        with pytest.raises(ValueError, match="could not reserve account 'a1'"):
            await reserve("job-1", 0, spec, "a1")

        assert mgr.binding_manager.calls == []

    async def test_eip_automatic_reserve_skips_spec_exclusions(self, tmp_path):
        mgr, _alloc, (reserve, _attach, _release) = await self._setup(tmp_path)
        spec = self._spec()
        spec.account.ids = []
        spec.account.exclude_ids = ["a1"]

        assignment = await reserve("job-1", 0, spec)

        assert assignment.account_id == "a2"
        assert mgr.binding_manager.calls[0][:2] == ("reserve", "a2")

    async def test_region_mismatch_rejected_before_claim(self, tmp_path):
        mgr, alloc, (reserve, _attach, _release) = await self._setup(tmp_path)
        with pytest.raises(ValueError, match="does not match Manager AWS region"):
            await reserve("job-1", 0, self._spec(region="us-east-1"), "a1")
        # No leaked allocator claim when validation fails.
        assert (await alloc.reserve("other", "standard", account_id="a1")) is not None
        assert mgr.binding_manager.calls == []

    async def test_plaintext_remote_manager_rejected_before_claim(
        self, tmp_path, monkeypatch
    ):
        mgr, alloc, (reserve, _attach, _release) = await self._setup(tmp_path)
        mgr.config.server.host = "manager.internal"
        monkeypatch.delenv("ELASTIC_AGENT_MANAGER_URL", raising=False)
        monkeypatch.delenv(
            "ELASTIC_AGENT_ALLOW_INSECURE_ACCOUNT_LOGIN", raising=False
        )

        with pytest.raises(ValueError, match="requires a wss://"):
            await reserve("job-1", 0, self._spec(), "a1")

        assert await alloc.reserve(
            "other", "standard", account_id="a1"
        ) is not None
        assert mgr.binding_manager.calls == []

    async def test_wss_manager_transport_allows_bound_reservation(
        self, tmp_path, monkeypatch
    ):
        mgr, _alloc, (reserve, _attach, _release) = await self._setup(tmp_path)
        mgr.config.server.host = "manager.internal"
        monkeypatch.setenv(
            "ELASTIC_AGENT_MANAGER_URL",
            "wss://manager.example/ws/runtime",
        )

        assignment = await reserve("job-1", 0, self._spec(), "a1")

        assert assignment.account_id == "a1"

    async def test_durable_release_failure_retains_allocator_claim(self, tmp_path):
        mgr, alloc, (reserve, attach, release) = await self._setup(tmp_path)
        assignment = await reserve("job-1", 0, self._spec(), "a1")
        assignment = await attach("w1", assignment)
        mgr.binding_manager.release_error = RuntimeError("detach failed")
        with pytest.raises(RuntimeError, match="detach failed"):
            await release(assignment, "w1")
        assert await alloc.get_claim(assignment.claim_id) is not None
        assert mgr.connection_manager.disconnected == []

    async def test_missing_durable_release_retains_node_and_allocator_claim(
        self, tmp_path
    ):
        mgr, alloc, (reserve, attach, release) = await self._setup(tmp_path)
        assignment = await reserve("job-1", 0, self._spec(), "a1")
        assignment = await attach("w1", assignment)

        async def missing_release(
            _lease_id, cleanup_worker=None, *, expected_lease=None
        ):
            return None

        mgr.binding_manager.release = missing_release

        with pytest.raises(RuntimeError, match="did not return a released lease"):
            await release(assignment, "w1")

        assert mgr.registry.removed == []
        assert mgr.registry.node is not None
        assert mgr.connection_manager.disconnected == []
        assert await alloc.get_claim(assignment.claim_id) is not None

    async def test_durable_worker_mismatch_retains_node_and_allocator_claim(
        self, tmp_path
    ):
        mgr, alloc, (reserve, attach, release) = await self._setup(tmp_path)
        assignment = await reserve("job-1", 0, self._spec(), "a1")
        assignment = await attach("w1", assignment)

        async def mismatched_release(
            _lease_id, cleanup_worker=None, *, expected_lease=None
        ):
            lease = SimpleNamespace(
                lease_id=assignment.lease_id,
                account_id=assignment.account_id,
                job_id=assignment.job_id,
                worker_id="w-other",
                instance_id="i-other",
                state="releasing",
            )
            if cleanup_worker:
                await cleanup_worker(lease)
            return lease

        mgr.binding_manager.release = mismatched_release

        with pytest.raises(RuntimeError, match="conflicts with durable worker"):
            await release(assignment, "w1")

        assert mgr.registry.removed == []
        assert mgr.registry.node is not None
        assert mgr.connection_manager.disconnected == []
        assert await alloc.get_claim(assignment.claim_id) is not None

    async def test_missing_durable_instance_retains_live_node_and_claim(
        self, tmp_path
    ):
        mgr, alloc, (reserve, _attach, release) = await self._setup(tmp_path)
        assignment = await reserve("job-1", 0, self._spec(), "a1")
        lease = mgr.binding_manager.leases[assignment.lease_id]
        lease.worker_id = "w1"
        lease.instance_id = None
        release_calls = list(mgr.binding_manager.calls)

        with pytest.raises(RuntimeError, match="has no instance id"):
            await release(assignment, "w1")

        assert mgr.binding_manager.calls == release_calls
        assert mgr.registry.removed == []
        assert mgr.registry.node is not None
        assert mgr.connection_manager.disconnected == []
        assert await alloc.get_claim(assignment.claim_id) is not None

    async def test_crossed_claim_refuses_cloud_release(self, tmp_path):
        mgr, alloc, (reserve, attach, release) = await self._setup(tmp_path)
        assignment = await reserve("job-1", 0, self._spec(), "a1")
        assignment = await attach("w1", assignment)
        crossed = await alloc.reserve(
            "job-other:0", "standard", account_id="a2"
        )
        crossed_assignment = replace(
            assignment, claim_id=crossed.claim_id
        )
        release_calls = list(mgr.binding_manager.calls)

        with pytest.raises(AccountClaimConflictError, match="does not belong"):
            await release(crossed_assignment, "w1")

        assert mgr.binding_manager.calls == release_calls
        assert mgr.registry.removed == []
        assert await alloc.get_claim(assignment.claim_id) is not None
        assert await alloc.get_claim(crossed.claim_id) is crossed

    async def test_durable_instance_mismatch_retains_node_and_allocator_claim(
        self, tmp_path
    ):
        mgr, alloc, (reserve, _attach, release) = await self._setup(tmp_path)
        assignment = await reserve("job-1", 0, self._spec(), "a1")
        lease = mgr.binding_manager.leases[assignment.lease_id]
        lease.worker_id = "w1"
        lease.instance_id = "i-other"

        with pytest.raises(RuntimeError, match="conflicts with durable instance"):
            await release(assignment, "w1")

        assert mgr.registry.removed == []
        assert mgr.registry.node is not None
        assert mgr.connection_manager.disconnected == []
        assert await alloc.get_claim(assignment.claim_id) is not None

    async def test_durable_assignment_mismatch_retains_node_and_allocator_claim(
        self, tmp_path
    ):
        mgr, alloc, (reserve, attach, release) = await self._setup(tmp_path)
        assignment = await reserve("job-1", 0, self._spec(), "a1")
        assignment = await attach("w1", assignment)
        mgr.binding_manager.leases[assignment.lease_id].account_id = "a2"

        with pytest.raises(RuntimeError, match="does not match its worker assignment"):
            await release(assignment, "w1")

        assert mgr.registry.removed == []
        assert mgr.registry.node is not None
        assert mgr.connection_manager.disconnected == []
        assert await alloc.get_claim(assignment.claim_id) is not None

    async def test_cancelled_durable_reserve_does_not_leak_allocator_claim(
        self, tmp_path
    ):
        mgr, alloc, (reserve, _attach, _release) = await self._setup(tmp_path)
        entered = asyncio.Event()

        async def blocked_reserve(*args, **kwargs):
            entered.set()
            await asyncio.Event().wait()

        mgr.binding_manager.reserve = blocked_reserve
        task = asyncio.create_task(
            reserve("job-1", 0, self._spec(), "a1")
        )
        await entered.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        reclaimed = await alloc.reserve(
            "job-2:0", "standard", account_id="a1"
        )
        assert reclaimed is not None

    async def test_cancel_after_lease_reserve_rolls_back_before_claim_release(
        self, tmp_path
    ):
        mgr, alloc, (reserve, _attach, _release) = await self._setup(tmp_path)
        binding_read_entered = asyncio.Event()
        cleanup_entered = asyncio.Event()
        allow_cleanup = asyncio.Event()
        real_release = mgr.binding_manager.release

        async def blocked_get_binding(_account_id):
            binding_read_entered.set()
            await asyncio.Future()

        async def gated_release(
            lease_id, cleanup_worker=None, *, expected_lease=None
        ):
            cleanup_entered.set()
            await allow_cleanup.wait()
            return await real_release(
                lease_id,
                cleanup_worker,
                expected_lease=expected_lease,
            )

        mgr.binding_manager.get_binding = blocked_get_binding
        mgr.binding_manager.release = gated_release
        task = asyncio.create_task(
            reserve("job-1", 0, self._spec(), "a1")
        )
        await asyncio.wait_for(binding_read_entered.wait(), timeout=1)

        task.cancel()
        await asyncio.wait_for(cleanup_entered.wait(), timeout=1)
        task.cancel()
        await asyncio.sleep(0)
        assert task.done() is False
        assert await alloc.reserve(
            "other", "standard", account_id="a1"
        ) is None

        allow_cleanup.set()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert "lease-a1" in mgr.binding_manager.released
        reclaimed = await alloc.reserve(
            "job-2:0", "standard", account_id="a1"
        )
        assert reclaimed is not None

    async def test_post_reserve_rollback_failure_retains_claim(self, tmp_path):
        mgr, alloc, (reserve, _attach, _release) = await self._setup(tmp_path)

        async def failed_get_binding(_account_id):
            raise RuntimeError("binding read failed")

        mgr.binding_manager.get_binding = failed_get_binding
        mgr.binding_manager.release_error = RuntimeError("lease release failed")

        with pytest.raises(RuntimeError, match="account claim retained"):
            await reserve("job-1", 0, self._spec(), "a1")

        assert await alloc.reserve(
            "other", "standard", account_id="a1"
        ) is None

    async def test_post_reserve_missing_rollback_retains_claim(self, tmp_path):
        mgr, alloc, (reserve, _attach, _release) = await self._setup(tmp_path)

        async def failed_get_binding(_account_id):
            raise RuntimeError("binding read failed")

        async def missing_release(
            _lease_id, cleanup_worker=None, *, expected_lease=None
        ):
            return None

        mgr.binding_manager.get_binding = failed_get_binding
        mgr.binding_manager.release = missing_release

        with pytest.raises(RuntimeError, match="account claim retained"):
            await reserve("job-1", 0, self._spec(), "a1")

        assert await alloc.reserve(
            "other", "standard", account_id="a1"
        ) is None


# --------------------------------------------------------------------------
# provision hook
# --------------------------------------------------------------------------


class TestProvisionHook:
    @pytest.fixture(autouse=True)
    def _stub_current_framework_delivery(self, monkeypatch):
        """Every declarative Job now receives the current dual-unit runtime."""

        import elastic_agent.core.bootstrap as bootstrap_mod
        import elastic_agent.core.code_sync as code_sync_mod

        class FakeSync:
            def __init__(self, *args, **kwargs):
                pass

            async def deliver(self, local, host, target):
                return True

        class FakeSSHExecutor:
            def __init__(self, *args, **kwargs):
                pass

            async def execute(self, command, timeout=None):
                return 0, "", ""

        monkeypatch.setattr(code_sync_mod, "ManagerCodeSync", FakeSync)
        monkeypatch.setattr(bootstrap_mod, "SSHExecutor", FakeSSHExecutor)

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
        assert "runtime-deploy" not in ran["steps"]

    async def test_aws_bootstrap_uses_private_worker_address(self, tmp_path):
        """SG-to-SG SSH rules only apply reliably on the VPC/private path."""
        from elastic_agent.core.job_spec import JobSpec, RunSpec

        mgr = FakeManager(tmp_path, await _store(tmp_path, []), connected=True)
        mgr.config.provider.type = "aws"
        captured = {}

        async def runner(node_id, host, steps, user, key):
            captured["host"] = host
            return True

        hook = make_provision_hook(
            mgr, bootstrap_runner=runner, ws_wait_timeout=1,
        )

        assert await hook(
            "w1", None, JobSpec(name="j", run=RunSpec(command="x")),
        ) is True
        assert captured["host"] == "10.0.0.1"

    async def test_s3_dataset_is_rendered_for_the_worker_shard(
        self, tmp_path, monkeypatch,
    ):
        import elastic_agent.core.bootstrap as bootstrap_mod
        from elastic_agent.core.job_spec import JobSpec, RunSpec, WorkerContext

        mgr = FakeManager(tmp_path, await _store(tmp_path, []), connected=True)
        mgr._batch = SimpleNamespace(
            worker_context_for=lambda worker_id: (
                WorkerContext(shard_index=7, num_shards=10)
                if worker_id == "w1"
                else None
            ),
        )
        commands = []

        class FakeSSHExecutor:
            def __init__(self, *args, **kwargs):
                self.use_sudo = kwargs.get("use_sudo")

            async def execute(self, command, timeout=None, **kwargs):
                if "ea-task-supervisor.service" in command:
                    return 0, "", ""
                assert self.use_sudo is False
                commands.append(command)
                return 0, "", ""

        monkeypatch.setattr(bootstrap_mod, "SSHExecutor", FakeSSHExecutor)

        async def runner(*args):
            return True

        spec = JobSpec(
            name="j",
            run=RunSpec(command="bench"),
            account={"mode": "none"},
            fanout={"workers": 10, "shard_by": "shard_index"},
            setup={
                "s3_datasets": [{
                    "uri": "s3://private-data/run/shard-{{shard_id}}.jsonl",
                    "dest": "/srv/replay/shard-{{shard_id}}.jsonl",
                }],
            },
        )
        hook = make_provision_hook(
            mgr, bootstrap_runner=runner, ws_wait_timeout=1,
        )

        assert await hook("w1", None, spec) is True
        assert any(command.startswith("command -v aws ") for command in commands)
        assert all("apt-get" not in command for command in commands)
        assert any(
            "aws s3 cp" in command
            and "s3://private-data/run/shard-00007.jsonl" in command
            and "/srv/replay/shard-00007.jsonl" in command
            for command in commands
        )
        assert all("aws s3 sync" not in command for command in commands)

    async def test_s3_dataset_missing_awscli_fails_without_runtime_install(
        self, tmp_path, monkeypatch,
    ):
        import elastic_agent.core.bootstrap as bootstrap_mod
        from elastic_agent.core.job_spec import JobSpec, RunSpec

        mgr = FakeManager(tmp_path, await _store(tmp_path, []), connected=True)
        commands = []

        class FakeSSHExecutor:
            def __init__(self, *args, **kwargs):
                pass

            async def execute(self, command, timeout=None, **kwargs):
                if "ea-task-supervisor.service" in command:
                    return 0, "", ""
                commands.append(command)
                if command.startswith("command -v aws "):
                    return 127, "", "awscli is required"
                raise AssertionError("dataset pull must stop before aws s3")

        monkeypatch.setattr(bootstrap_mod, "SSHExecutor", FakeSSHExecutor)

        async def runner(*args):
            return True

        spec = JobSpec(
            name="j",
            run=RunSpec(command="bench"),
            account={"mode": "none"},
            setup={
                "s3_datasets": [{
                    "uri": "s3://private-data/shard.jsonl",
                    "dest": "/srv/replay/shard.jsonl",
                }],
            },
        )
        hook = make_provision_hook(
            mgr, bootstrap_runner=runner, ws_wait_timeout=1,
        )

        assert await hook("w1", None, spec) is False
        assert commands
        assert all("apt-get" not in command for command in commands)

    async def test_templated_dataset_without_worker_context_fails_closed(
        self, tmp_path, monkeypatch,
    ):
        import elastic_agent.core.bootstrap as bootstrap_mod
        from elastic_agent.core.job_spec import JobSpec, RunSpec

        mgr = FakeManager(tmp_path, await _store(tmp_path, []), connected=True)
        commands = []

        class FakeSSHExecutor:
            def __init__(self, *args, **kwargs):
                pass

            async def execute(self, command, timeout=None, **kwargs):
                commands.append(command)
                return 0, "", ""

        monkeypatch.setattr(bootstrap_mod, "SSHExecutor", FakeSSHExecutor)

        async def runner(*args):
            return True

        spec = JobSpec(
            name="j",
            run=RunSpec(command="bench"),
            account={"mode": "none"},
            fanout={"workers": 2, "shard_by": "shard_index"},
            setup={
                "s3_datasets": [{
                    "uri": "s3://private-data/shard-{{shard_id}}.jsonl",
                    "dest": "/srv/replay/shard-{{shard_id}}.jsonl",
                }],
            },
        )
        hook = make_provision_hook(
            mgr, bootstrap_runner=runner, ws_wait_timeout=1,
        )

        assert await hook("missing-worker", None, spec) is False
        assert not any(command.startswith("aws s3 ") for command in commands)

    async def test_single_dataset_quotes_parent_directory_as_one_argument(
        self, tmp_path, monkeypatch,
    ):
        import elastic_agent.core.bootstrap as bootstrap_mod
        from elastic_agent.core.job_spec import JobSpec, RunSpec, WorkerContext

        mgr = FakeManager(tmp_path, await _store(tmp_path, []), connected=True)
        mgr._batch = SimpleNamespace(
            worker_context_for=lambda _worker_id: WorkerContext(
                shard_index=0,
                num_shards=1,
            ),
        )
        commands = []

        class FakeSSHExecutor:
            def __init__(self, *args, **kwargs):
                pass

            async def execute(self, command, timeout=None, **kwargs):
                commands.append(command)
                return 0, "", ""

        monkeypatch.setattr(bootstrap_mod, "SSHExecutor", FakeSSHExecutor)

        async def runner(*args):
            return True

        spec = JobSpec(
            name="j",
            run=RunSpec(command="bench"),
            account={"mode": "none"},
            setup={
                "s3_datasets": [{
                    "uri": "s3://private-data/shard.jsonl",
                    "dest": "/srv/replay/shard data/input.jsonl",
                }],
            },
        )
        hook = make_provision_hook(
            mgr, bootstrap_runner=runner, ws_wait_timeout=1,
        )

        assert await hook("w1", None, spec) is True
        copy = next(command for command in commands if "aws s3 cp" in command)
        assert "mkdir -p '/srv/replay/shard data'" in copy
        assert "$(dirname " not in copy

    async def test_bootstrap_failure(self, tmp_path):
        from elastic_agent.core.job_spec import JobSpec, RunSpec
        mgr = FakeManager(tmp_path, await _store(tmp_path, []))

        async def runner(*a):
            return False

        hook = make_provision_hook(mgr, bootstrap_runner=runner, ws_wait_timeout=1)
        assert await hook("w1", None, JobSpec(name="j", run=RunSpec(command="x"))) is False

    async def test_bound_job_bootstraps_via_private_vpc_and_current_source(
        self, tmp_path, monkeypatch,
    ):
        import elastic_agent.core.bootstrap as bootstrap_mod
        import elastic_agent.core.code_sync as code_sync_mod
        from elastic_agent.core.job_spec import JobSpec, RunSpec

        mgr = FakeManager(
            tmp_path, await _store(tmp_path, []), connected=True,
            host="198.51.100.42",
        )
        mgr.config.provider.type = "aws"
        # Provider wait returns an old/ephemeral address; attach_bound already
        # made registry.public_ip authoritative for the Worker's outbound EIP.
        # Manager-initiated SSH must still use the registry private address.
        mgr.provider.wait_until_running = lambda iid: _async(
            SimpleNamespace(
                public_ip="203.0.113.9", private_ip="10.0.0.99",
            )
        )
        captured = {"deliveries": []}

        class FakeSync:
            def __init__(self, *args, **kwargs):
                pass

            async def deliver(self, local, host, target):
                captured["deliveries"].append((local, host, target))
                return True

        class FakeSSHExecutor:
            def __init__(self, *args, **kwargs):
                pass

            async def execute(self, command, timeout=None):
                captured["runtime_command"] = command
                return 0, "", ""

        # Even an explicit stale override cannot downgrade an EIP worker.
        monkeypatch.setenv("ELASTIC_AGENT_FRAMEWORK_SRC", "/tmp/stale-framework")
        monkeypatch.setattr(code_sync_mod, "ManagerCodeSync", FakeSync)
        monkeypatch.setattr(bootstrap_mod, "SSHExecutor", FakeSSHExecutor)

        async def runner(node_id, host, steps, user, key):
            captured["host"] = host
            captured["steps"] = [step.name for step in steps]
            return True

        spec = JobSpec(
            name="j",
            run=RunSpec(command="x"),
            account={"binding": "eip", "ids": ["a1"]},
            fanout={"workers": 1},
        )
        hook = make_provision_hook(mgr, bootstrap_runner=runner, ws_wait_timeout=1)
        assert await hook("w1", None, spec) is True
        assert captured["host"] == "10.0.0.1"
        assert "runtime-deploy" not in captured["steps"]
        local, delivered_host, target = captured["deliveries"][0]
        assert delivered_host == "10.0.0.1"
        assert local.endswith("/elastic_agent")
        assert target == "/opt/elastic-agent/framework/src/elastic_agent"
        assert "runtime_main" in captured["runtime_command"]
        assert "disable --now elastic-agent-runtime.service" in (
            captured["runtime_command"]
        )
        assert mgr.connection_manager.disconnected == ["w1"]

    async def test_codex_job_always_bootstraps_current_worker_source(
        self, tmp_path, monkeypatch,
    ):
        """An old PyPI worker could misread Codex login as legacy Claude login."""
        import elastic_agent.core.bootstrap as bootstrap_mod
        import elastic_agent.core.code_sync as code_sync_mod
        from elastic_agent.core.job_spec import JobSpec, RunSpec

        mgr = FakeManager(tmp_path, await _store(tmp_path, []), connected=True)
        captured = {"deliveries": []}

        class FakeSync:
            def __init__(self, *args, **kwargs):
                pass

            async def deliver(self, local, host, target):
                captured["deliveries"].append((local, host, target))
                return True

        class FakeSSHExecutor:
            def __init__(self, *args, **kwargs):
                pass

            async def execute(self, command, timeout=None):
                captured["runtime_command"] = command
                return 0, "", ""

        # Codex ignores an explicitly configured stale framework for the same
        # identity-verification reason as an EIP-bound login.
        monkeypatch.setenv("ELASTIC_AGENT_FRAMEWORK_SRC", "/tmp/stale-framework")
        monkeypatch.setattr(code_sync_mod, "ManagerCodeSync", FakeSync)
        monkeypatch.setattr(bootstrap_mod, "SSHExecutor", FakeSSHExecutor)

        async def runner(node_id, host, steps, user, key):
            captured["steps"] = [step.name for step in steps]
            return True

        spec = JobSpec(
            name="codex-job",
            run=RunSpec(command="x"),
            account={"agent_type": "codex"},
        )
        hook = make_provision_hook(mgr, bootstrap_runner=runner, ws_wait_timeout=1)

        assert await hook("w1", None, spec) is True
        assert "runtime-deploy" not in captured["steps"]
        assert "credential-login-deps" in captured["steps"]
        local, delivered_host, target = captured["deliveries"][0]
        assert delivered_host == "1.2.3.4"
        assert local.endswith("/elastic_agent")
        assert target == "/opt/elastic-agent/framework/src/elastic_agent"
        assert "runtime_main" in captured["runtime_command"]
        assert "ELASTIC_AGENT_AGENT_TYPE=codex" in captured["runtime_command"]
        assert mgr.connection_manager.disconnected == ["w1"]

    async def test_ws_never_connects(self, tmp_path):
        from elastic_agent.core.job_spec import JobSpec, RunSpec
        mgr = FakeManager(tmp_path, await _store(tmp_path, []), connected=False)

        async def runner(*a):
            return True

        hook = make_provision_hook(mgr, bootstrap_runner=runner, ws_wait_timeout=0.1)
        assert await hook("w1", None, JobSpec(name="j", run=RunSpec(command="x"))) is False

    async def test_manager_rsync_setup_runs_as_job_user_no_sudo(self, tmp_path, monkeypatch):
        """Setup commands must run as the ssh_user (no sudo) so per-user installs
        (e.g. `uv` in $HOME/.local) match the user the run command executes as.
        Regression: sudo-wrapped setup put uv/.venv under /root, invisible to the
        ubuntu runtime → run died with `$HOME/.local/bin/uv: No such file`."""
        import elastic_agent.core.bootstrap as bootstrap_mod
        import elastic_agent.core.code_sync as code_sync_mod
        from elastic_agent.core.job_spec import JobSpec, RunSpec, SetupSpec

        mgr = FakeManager(tmp_path, await _store(tmp_path, []), connected=True)
        mgr.config.worker.ssh_user = "ubuntu"  # non-root → default would sudo-wrap
        mgr.collected_root = str(tmp_path / "collected")

        class FakeSync:
            def __init__(self, *a, **k): ...
            async def ensure_clone(
                self, repo, branch, *, resolved_commit="",
            ):
                return "/local/clone"
            async def deliver(self, local, host, target): return True

        captured = {"calls": [], "executors": []}

        class FakeSSHExecutor:
            def __init__(self, host, *, user=None, key_path=None, use_sudo=None):
                self.user = user
                self.use_sudo = use_sudo
                captured["executors"].append((user, use_sudo))

            async def execute(self, cmd, timeout=None, env=None, cwd=None):
                if "ea-task-supervisor.service" in cmd:
                    return 0, "", ""
                assert self.use_sudo is False
                captured["calls"].append({
                    "cmd": cmd, "timeout": timeout, "env": env, "cwd": cwd,
                })
                return 0, "", ""

        monkeypatch.setattr(code_sync_mod, "ManagerCodeSync", FakeSync)
        monkeypatch.setattr(bootstrap_mod, "SSHExecutor", FakeSSHExecutor)

        async def runner(*a):
            return True

        spec = JobSpec(
            name="j",
            run=RunSpec(command="$HOME/.local/bin/uv run x"),
            setup=SetupSpec(repo="https://example.com/r.git", deliver="manager_rsync",
                            target_dir="/home/ubuntu/bench",
                            commands=["curl -LsSf https://astral.sh/uv/install.sh | sh"]),
        )
        hook = make_provision_hook(mgr, bootstrap_runner=runner, ws_wait_timeout=1)
        assert await hook("w1", None, spec) is True
        assert ("ubuntu", False) in captured["executors"]
        assert captured["calls"][0]["cwd"] == "/home/ubuntu/bench"

    async def test_manager_rsync_structured_setup_honors_step_policy(
        self, tmp_path, monkeypatch,
    ):
        import elastic_agent.core.bootstrap as bootstrap_mod
        import elastic_agent.core.code_sync as code_sync_mod
        from elastic_agent.core.job_spec import JobSpec, RunSpec

        mgr = FakeManager(tmp_path, await _store(tmp_path, []), connected=True)
        mgr.config.worker.ssh_user = "ubuntu"
        mgr.collected_root = str(tmp_path / "collected")

        class FakeSync:
            def __init__(self, *args, **kwargs): ...
            async def ensure_clone(
                self, repo, branch, *, resolved_commit="",
            ):
                return "/local/clone"
            async def deliver(self, local, host, target): return True

        calls = []

        class FakeSSHExecutor:
            def __init__(self, *args, **kwargs):
                self.use_sudo = kwargs.get("use_sudo")

            async def execute(self, command, timeout=None, env=None, cwd=None):
                if "ea-task-supervisor.service" in command:
                    return 0, "", ""
                assert self.use_sudo is False
                calls.append((command, timeout, env, cwd))
                # Exercise retry_count=1 without delaying the test.
                return (1, "", "retry") if len(calls) == 1 else (0, "", "")

        monkeypatch.setattr(code_sync_mod, "ManagerCodeSync", FakeSync)
        monkeypatch.setattr(bootstrap_mod, "SSHExecutor", FakeSSHExecutor)

        async def runner(*args):
            return True

        spec = JobSpec(
            name="j",
            run=RunSpec(command="bench"),
            account={"mode": "none"},
            setup={
                "repo": "https://example.com/r.git",
                "deliver": "manager_rsync",
                "target_dir": "/home/ubuntu/bench",
                "steps": [{
                    "name": "install", "command": "uv sync",
                    "env": {"UV_LINK_MODE": "copy"}, "cwd": "python",
                    "timeout": 777, "retries": 1,
                }],
            },
        )
        hook = make_provision_hook(
            mgr, bootstrap_runner=runner, ws_wait_timeout=1,
        )

        assert await hook("w1", None, spec) is True
        assert len(calls) == 2
        assert calls[-1] == (
            "uv sync", 777, {"UV_LINK_MODE": "copy"},
            "/home/ubuntu/bench/python",
        )


# --------------------------------------------------------------------------
# wire_batch event routing
# --------------------------------------------------------------------------


class TestWireBatchRouting:
    async def test_oauth_exhaustion_quarantine_skips_account_for_next_job(
        self, tmp_path,
    ):
        mgr = FakeManager(
            tmp_path,
            await _store(tmp_path, [_acct(1), _acct(2)]),
        )
        orch = wire_batch(mgr)
        orch._worker_index["w1"] = "job-1"
        orch.runtime_account_for_task = lambda *_args, **_kwargs: (
            "a1", "oauth"
        )
        exhausted = []
        orch.defer_exhausted = lambda worker_id, **kwargs: (
            exhausted.append((worker_id, kwargs["task_id"])) or True
        )

        await mgr.event_bus.emit("RUN_EXHAUSTED", "w1", {
            "task_id": "task-a",
            "reason": "rate_limit",
        })
        next_claim = await orch._allocator.reserve(
            "job-2:0",
            "standard",
            allow_durable_binding=True,
        )

        assert exhausted == [("w1", "task-a")]
        assert await orch._allocator.is_quarantined("a1") is True
        assert next_claim is not None and next_claim.account.id == "a2"

    async def test_runtime_tombstone_failure_does_not_block_lifecycle(
        self, tmp_path,
    ):
        mgr = FakeManager(tmp_path, await _store(tmp_path, []))
        api_store = _FakeAgentApiStore(_api_acct())
        api_store.mark_runtime_unavailable = AsyncMock(
            side_effect=OSError("disk full")
        )
        mgr.agent_api_store = api_store
        orch = wire_batch(mgr)
        orch._worker_index["w1"] = "job-1"
        orch.runtime_account_for_task = lambda *_args, **_kwargs: (
            "cloudrouter-1", "agent_api"
        )
        exhausted = []
        exits = []
        orch.defer_exhausted = lambda worker_id, **kwargs: (
            exhausted.append((worker_id, kwargs["task_id"])) or True
        )
        orch.begin_exit_archive = lambda *_args, **_kwargs: False

        async def handle_exit(worker_id, exit_code, task_id=None):
            exits.append((worker_id, exit_code, task_id))

        orch.handle_exit = handle_exit

        await mgr.event_bus.emit("RUN_EXHAUSTED", "w1", {
            "task_id": "task-a",
            "reason": "agent_api_auth_failure",
        })
        await mgr.event_bus.emit("PROCESS_EXIT", "w1", {
            "task_id": "task-a",
            "exit_code": 1,
            "error_type": "agent_api_auth_failure",
        })

        assert exhausted == [("w1", "task-a")]
        assert exits == [("w1", 1, "task-a")]
        assert await orch._allocator.is_quarantined("cloudrouter-1") is True

    async def test_runtime_api_feedback_is_bound_to_exact_dispatch(
        self, tmp_path,
    ):
        mgr = FakeManager(tmp_path, await _store(tmp_path, []))
        api_store = _FakeAgentApiStore(_api_acct())
        mgr.agent_api_store = api_store
        orch = wire_batch(mgr)
        orch._worker_index["w1"] = "job-1"
        current_task = "job-1:w1:a"
        current_account = "cloudrouter-a"

        def runtime_account(worker_id, *, task_id=None):
            if worker_id != "w1" or task_id != current_task:
                return None
            return current_account, "agent_api"

        def rotate(worker_id, *, task_id=None):
            nonlocal current_task, current_account
            if worker_id != "w1" or task_id != current_task:
                return False
            current_task = "job-1:w1:b"
            current_account = "cloudrouter-b"
            return True

        orch.runtime_account_for_task = runtime_account
        orch.defer_exhausted = rotate

        await mgr.event_bus.emit("RUN_EXHAUSTED", "w1", {
            "task_id": "job-1:w1:a",
            "reason": "agent_api_auth_failure",
        })
        assert api_store.runtime_marks == [
            ("cloudrouter-a", "runtime_invalid_api_key")
        ]

        # Both late events belong to A. They cannot bench newly active B.
        await mgr.event_bus.emit("PROCESS_EXIT", "w1", {
            "task_id": "job-1:w1:a",
            "exit_code": 1,
            "error_type": "agent_api_auth_failure",
        })
        await mgr.event_bus.emit("RUN_EXHAUSTED", "w1", {
            "task_id": "job-1:w1:a",
            "reason": "agent_api_auth_failure",
        })
        assert api_store.runtime_marks == [
            ("cloudrouter-a", "runtime_invalid_api_key")
        ]

        # A current non-rotation failure still benches its exact account.
        await mgr.event_bus.emit("PROCESS_EXIT", "w1", {
            "task_id": "job-1:w1:b",
            "exit_code": 1,
            "error_type": "agent_api_auth_failure",
        })
        assert api_store.runtime_marks == [
            ("cloudrouter-a", "runtime_invalid_api_key"),
            ("cloudrouter-b", "runtime_invalid_api_key"),
        ]

        # A hard provider limit is non-sticky but must survive account release
        # and be tied to the exact dispatch that observed it.
        await mgr.event_bus.emit("PROCESS_EXIT", "w1", {
            "task_id": "job-1:w1:b",
            "exit_code": 1,
            "error_type": "agent_api_rate_limited",
        })
        assert api_store.runtime_quota_marks == [
            ("cloudrouter-b", "runtime_rate_limited"),
        ]
        await mgr.event_bus.emit("PROCESS_EXIT", "w1", {
            "task_id": "job-1:w1:a",
            "exit_code": 1,
            "error_type": "agent_api_rate_limited",
        })
        assert api_store.runtime_quota_marks == [
            ("cloudrouter-b", "runtime_rate_limited"),
        ]

    async def test_routes_exhausted_and_exit(self, tmp_path):
        mgr = FakeManager(tmp_path, await _store(tmp_path, [_acct(1)]))
        orch = wire_batch(mgr)

        calls = {"exh": [], "exit": []}
        exit_order = []

        def fake_exh(worker_id, *, task_id=None):
            calls["exh"].append((worker_id, task_id))
            return True

        async def fake_exit(worker_id, exit_code, task_id=None):
            exit_order.append("handle")
            calls["exit"].append((worker_id, exit_code, task_id))

        def begin_archive(worker_id, *, task_id=None):
            exit_order.append("begin")
            return True

        def finish_archive(worker_id, *, task_id=None):
            exit_order.append("finish")

        orch.defer_exhausted = fake_exh
        orch.handle_exit = fake_exit
        orch.begin_exit_archive = begin_archive
        orch.finish_exit_archive = finish_archive
        original_archive = mgr.archive_job_task_log

        async def archive(*args, **kwargs):
            exit_order.append("archive")
            return await original_archive(*args, **kwargs)

        mgr.archive_job_task_log = archive
        # mark w1 as a batch-owned worker so PROCESS_EXIT routes
        orch._worker_index["w1"] = "job-1"

        await mgr.event_bus.emit("RUN_EXHAUSTED", "w1",
                                 {"worker_id": "w1", "job_id": "job-1",
                                  "task_id": "job-1:w1:old", "reason": "rate_limit"})
        await mgr.event_bus.emit("PROCESS_EXIT", "w1",
                                 {"task_id": "job-1:w1:abc", "exit_code": 0})
        assert calls["exh"] == [("w1", "job-1:w1:old")]
        assert calls["exit"] == [("w1", 0, "job-1:w1:abc")]
        assert exit_order == ["begin", "archive", "finish", "handle"]
        assert mgr.archived_job_logs == [(
            "job-1",
            "w1",
            {"task_id": "job-1:w1:abc", "exit_code": 0},
        )]

    async def test_non_batch_exit_ignored(self, tmp_path):
        mgr = FakeManager(tmp_path, await _store(tmp_path, []))
        orch = wire_batch(mgr)
        seen = []
        orch.handle_exit = lambda *a, **k: seen.append(a)  # noqa
        # w-other is not in the worker index → should be ignored
        await mgr.event_bus.emit("PROCESS_EXIT", "w-other", {"task_id": "t", "exit_code": 1})
        assert seen == []
        assert mgr.archived_job_logs == []
