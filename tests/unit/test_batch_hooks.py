"""Tests for the live provision/login wiring (batch_hooks)."""

from __future__ import annotations

import asyncio
import time
from types import SimpleNamespace

import pytest

from elastic_agent.core.account_store import AccountStore
from elastic_agent.core.batch_hooks import (
    AccountAllocator,
    AccountClaimConflictError,
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

    async def get(self, worker_id):
        return self.node if worker_id == self.node.node_id else None

    async def update(self, worker_id, **fields):
        node = await self.get(worker_id)
        if node is None:
            return None
        for key, value in fields.items():
            setattr(node, key, value)
        return node


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

    async def test_codex_password_and_agent_type_are_sent_to_worker(self):
        bus = EventBus()
        conn = FakeConn()
        coord = LoginCoordinator(conn, bus, timeout=5)
        account = _acct(
            2, agent_type="codex", password="openai-secret"
        )
        task = asyncio.create_task(
            coord.login("w1", account, "/root/.codex")
        )
        await asyncio.sleep(0)

        message = conn.sent[0][1]
        assert message.agent_type == "codex"
        assert message.password == "openai-secret"
        assert message.config_dir == "/root/.codex"
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
        await mgr.event_bus.emit("ACCOUNT_LOGIN_RESULT", "w1", {
            "login_request_id": mgr.connection_manager.sent[0][1].login_request_id,
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

    async def reserve(self, account_id, *, email, job_id, slot, region):
        self.calls.append(("reserve", account_id, job_id, slot, region))
        self.binding = SimpleNamespace(
            account_id=account_id,
            email=email,
            eip_allocation_id=f"eipalloc-{account_id}",
            eip_ip="198.51.100.42",
            region=region,
        )
        return SimpleNamespace(lease_id=f"lease-{account_id}")

    async def get_binding(self, account_id):
        return self.binding if self.binding and self.binding.account_id == account_id else None

    async def attach_instance(self, lease_id, instance_id, worker_id):
        self.calls.append(("attach", lease_id, instance_id, worker_id))
        return SimpleNamespace(lease_id=lease_id, instance_id=instance_id, worker_id=worker_id)

    async def release(self, lease_id, cleanup_worker=None):
        self.calls.append(("release", lease_id))
        if self.release_error:
            raise self.release_error
        self.released.add(lease_id)
        lease = SimpleNamespace(lease_id=lease_id, worker_id="w1", instance_id="i-1")
        if cleanup_worker:
            await cleanup_worker(lease)
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
        from elastic_agent.core.registry import NodeStatus

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
        assert mgr.registry.node.status == NodeStatus.TERMINATED
        assert mgr.connection_manager.disconnected == ["w1"]
        assert await alloc.get_claim(attached.claim_id) is None

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
        mgr, alloc, (reserve, _attach, release) = await self._setup(tmp_path)
        assignment = await reserve("job-1", 0, self._spec(), "a1")
        mgr.binding_manager.release_error = RuntimeError("detach failed")
        with pytest.raises(RuntimeError, match="detach failed"):
            await release(assignment, "w1")
        assert await alloc.get_claim(assignment.claim_id) is not None
        assert mgr.connection_manager.disconnected == []

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

        async def gated_release(lease_id, cleanup_worker=None):
            cleanup_entered.set()
            await allow_cleanup.wait()
            return await real_release(lease_id, cleanup_worker)

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

    async def test_bound_job_bootstraps_via_registry_eip_and_current_source(
        self, tmp_path, monkeypatch,
    ):
        import elastic_agent.core.bootstrap as bootstrap_mod
        import elastic_agent.core.code_sync as code_sync_mod
        from elastic_agent.core.job_spec import JobSpec, RunSpec

        mgr = FakeManager(
            tmp_path, await _store(tmp_path, []), connected=True,
            host="198.51.100.42",
        )
        # Provider wait returns an old/ephemeral address; attach_bound already
        # made registry.public_ip authoritative.
        mgr.provider.wait_until_running = lambda iid: _async(
            SimpleNamespace(public_ip="203.0.113.9")
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
        assert captured["host"] == "198.51.100.42"
        assert "runtime-deploy" not in captured["steps"]
        local, delivered_host, target = captured["deliveries"][0]
        assert delivered_host == "198.51.100.42"
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
            async def ensure_clone(self, repo, branch): return "/local/clone"
            async def deliver(self, local, host, target): return True

        captured = {}

        class FakeSSHExecutor:
            def __init__(self, host, *, user=None, key_path=None, use_sudo=None):
                captured["user"] = user
                captured["use_sudo"] = use_sudo

            async def execute(self, cmd, timeout=None):
                captured["cmd"] = cmd
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
        assert captured["user"] == "ubuntu"
        assert captured["use_sudo"] is False
        assert "/home/ubuntu/bench" in captured["cmd"]


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
