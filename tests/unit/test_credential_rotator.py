"""Tests for CredentialRotator — T-045 and T-046 (cooldown recovery)."""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from elastic_agent.core.config import CredentialConfig
from elastic_agent.core.credential_binding import CredentialBinding
from elastic_agent.core.credential_pool import AccountDefinition, CredentialPool
from elastic_agent.core.credential_rotator import CredentialRotator, RotationResult
from elastic_agent.core.event_bus import EventBus


@pytest.fixture
def tmp_pool(tmp_path):
    accounts_data = {
        "accounts": [
            {"id": "prod-1", "email": "p1@test.com", "email_token": "t1", "group": "standard"},
            {"id": "prod-2", "email": "p2@test.com", "email_token": "t2", "group": "standard"},
            {"id": "prod-3", "email": "p3@test.com", "email_token": "t3", "group": "standard"},
            {"id": "hq-1", "email": "hq1@test.com", "email_token": "t4", "group": "high_quota"},
        ],
        "groups": {
            "standard": {"description": "Standard"},
            "high_quota": {"description": "High quota"},
        },
    }
    accounts_file = tmp_path / "accounts.json"
    accounts_file.write_text(json.dumps(accounts_data))

    config = CredentialConfig(
        accounts_file=str(accounts_file),
        pool_status_file=str(tmp_path / "pool_status.json"),
        quota_threshold=0.85,
        max_accounts_per_worker=4,
    )
    pool = CredentialPool(config)
    return pool


@pytest.fixture
def binding():
    return CredentialBinding(max_accounts_per_worker=4)


@pytest.fixture
def event_bus():
    return EventBus()


@pytest.fixture
def rotator(tmp_pool, binding, event_bus):
    return CredentialRotator(tmp_pool, binding, event_bus)


class TestRotationResult:
    def test_success(self):
        r = RotationResult(success=True, old_account_id="prod-1", new_account_id="prod-2")
        assert r.success
        assert r.old_account_id == "prod-1"
        assert r.new_account_id == "prod-2"
        assert r.error is None

    def test_failure(self):
        r = RotationResult(success=False, old_account_id="prod-1", error="no replacement")
        assert not r.success
        assert r.error == "no replacement"
        assert r.new_account_id is None


class TestCredentialRotator:
    @pytest.mark.asyncio
    async def test_successful_rotation(self, rotator, tmp_pool, binding, event_bus):
        await tmp_pool.load()

        # Allocate prod-1 to worker-1
        alloc = await tmp_pool.allocate("worker-1", "production", "standard")
        assert alloc is not None
        assert alloc.id == "prod-1"
        await binding.bind("prod-1", "worker-1")

        # Set up send_credential mock
        rotator.send_credential = AsyncMock(return_value=True)
        rotator.wait_for_task_completion = AsyncMock()

        # Track events
        events = []
        event_bus.subscribe("CREDENTIAL_ROTATED", lambda et, wid, data: events.append(data))

        result = await rotator.rotate("worker-1", "prod-1", "quota_exceeded")

        assert result.success
        assert result.old_account_id == "prod-1"
        assert result.new_account_id is not None
        assert result.new_account_id != "prod-1"

        # Old account should be in cooldown
        old_status = tmp_pool.get_status("prod-1")
        assert old_status.assigned_to is None
        assert old_status.backoff_until is not None

        # New account should be assigned
        new_status = tmp_pool.get_status(result.new_account_id)
        assert new_status.assigned_to == "worker-1"

        # Rotation event should be emitted
        # Give event propagation a moment
        await asyncio.sleep(0.01)
        rotated_events = [e for e in events if e.get("old_id") == "prod-1"]
        assert len(rotated_events) >= 1

    @pytest.mark.asyncio
    async def test_rotation_no_replacement_available(self, rotator, tmp_pool, binding, event_bus):
        await tmp_pool.load()

        # Allocate all standard accounts
        for i, acct_id in enumerate(["prod-1", "prod-2", "prod-3"]):
            worker = f"worker-{i+1}"
            alloc = await tmp_pool.allocate(worker, "production", "standard")
            if alloc:
                await binding.bind(alloc.id, worker)

        # Now try to rotate prod-1 — no free standard accounts
        exhausted_events = []
        event_bus.subscribe("CREDENTIAL_EXHAUSTED", lambda et, wid, data: exhausted_events.append(data))

        result = await rotator.rotate("worker-1", "prod-1", "quota_exceeded")

        assert not result.success
        assert "no replacement" in result.error.lower()

    @pytest.mark.asyncio
    async def test_rotation_waits_for_task(self, rotator, tmp_pool, binding):
        await tmp_pool.load()

        alloc = await tmp_pool.allocate("worker-1", "production", "standard")
        await binding.bind(alloc.id, "worker-1")

        wait_called = False

        async def mock_wait(worker_id):
            nonlocal wait_called
            wait_called = True
            assert worker_id == "worker-1"

        rotator.wait_for_task_completion = mock_wait
        rotator.send_credential = AsyncMock(return_value=True)

        result = await rotator.rotate("worker-1", "prod-1", "quota_exceeded")

        assert result.success
        assert wait_called

    @pytest.mark.asyncio
    async def test_rotation_login_needed(self, rotator, tmp_pool, binding):
        await tmp_pool.load()

        alloc = await tmp_pool.allocate("worker-1", "production", "standard")
        await binding.bind(alloc.id, "worker-1")

        login_result = MagicMock()
        login_result.success = True
        rotator.execute_login = AsyncMock(return_value=login_result)
        rotator.send_credential = AsyncMock(return_value=True)

        result = await rotator.rotate("worker-1", "prod-1", "quota_exceeded")

        assert result.success
        rotator.execute_login.assert_called_once()

    @pytest.mark.asyncio
    async def test_rotation_login_failure_releases_new_account(self, rotator, tmp_pool, binding):
        await tmp_pool.load()

        alloc = await tmp_pool.allocate("worker-1", "production", "standard")
        await binding.bind(alloc.id, "worker-1")

        login_result = MagicMock()
        login_result.success = False
        login_result.error = "browser crash"
        rotator.execute_login = AsyncMock(return_value=login_result)

        result = await rotator.rotate("worker-1", "prod-1", "quota_exceeded")

        assert not result.success
        assert "login failed" in result.error.lower()

        # New account should be released
        new_status = tmp_pool.get_status(result.new_account_id)
        assert new_status.assigned_to is None

    @pytest.mark.asyncio
    async def test_rotation_send_credential_failure(self, rotator, tmp_pool, binding):
        await tmp_pool.load()

        alloc = await tmp_pool.allocate("worker-1", "production", "standard")
        await binding.bind(alloc.id, "worker-1")

        # Mark new account as already logged in so no login needed
        for acct_id, status in tmp_pool._pool_status.accounts.items():
            if acct_id != "prod-1" and status.group == "standard":
                status.login_status = "logged_in"
                break

        rotator.send_credential = AsyncMock(return_value=False)

        result = await rotator.rotate("worker-1", "prod-1", "quota_exceeded")

        assert not result.success
        assert "failed to send" in result.error.lower()

    @pytest.mark.asyncio
    async def test_rotation_account_not_found(self, rotator, tmp_pool):
        await tmp_pool.load()

        result = await rotator.rotate("worker-1", "nonexistent", "quota_exceeded")
        assert not result.success
        assert "not found" in result.error.lower()

    @pytest.mark.asyncio
    async def test_rotation_uses_same_group(self, rotator, tmp_pool, binding):
        await tmp_pool.load()

        # Allocate high_quota account
        alloc = await tmp_pool.allocate("worker-1", "production", "high_quota")
        assert alloc is not None
        assert alloc.id == "hq-1"
        await binding.bind("hq-1", "worker-1")

        rotator.send_credential = AsyncMock(return_value=True)

        # Try to rotate — no other high_quota accounts available
        result = await rotator.rotate("worker-1", "hq-1", "quota_exceeded")
        assert not result.success

    @pytest.mark.asyncio
    async def test_rotation_old_account_cooldown(self, rotator, tmp_pool, binding):
        await tmp_pool.load()

        alloc = await tmp_pool.allocate("worker-1", "production", "standard")
        await binding.bind(alloc.id, "worker-1")

        rotator.send_credential = AsyncMock(return_value=True)

        result = await rotator.rotate("worker-1", "prod-1", "quota_exceeded")
        assert result.success

        # Old account should have cooldown set
        old_status = tmp_pool.get_status("prod-1")
        assert old_status.backoff_until is not None
        assert not old_status.available

    @pytest.mark.asyncio
    async def test_rotation_binding_updated(self, rotator, tmp_pool, binding):
        await tmp_pool.load()

        alloc = await tmp_pool.allocate("worker-1", "production", "standard")
        await binding.bind(alloc.id, "worker-1")

        rotator.send_credential = AsyncMock(return_value=True)

        result = await rotator.rotate("worker-1", "prod-1", "quota_exceeded")
        assert result.success

        # Old binding removed
        assert not binding.is_bound("prod-1")

        # New binding created
        assert binding.is_bound(result.new_account_id)
        assert binding.get_worker(result.new_account_id) == "worker-1"

    @pytest.mark.asyncio
    async def test_handle_exhausted(self, rotator, event_bus):
        events = []
        event_bus.subscribe("CREDENTIAL_EXHAUSTED", lambda et, wid, data: events.append(data))

        await rotator.handle_exhausted("worker-1", "prod-1", "all_exhausted")

        assert len(events) == 1
        assert events[0]["worker_id"] == "worker-1"
        assert events[0]["account_id"] == "prod-1"
        assert events[0]["reason"] == "all_exhausted"

    @pytest.mark.asyncio
    async def test_rotation_wait_failure_continues(self, rotator, tmp_pool, binding):
        await tmp_pool.load()

        alloc = await tmp_pool.allocate("worker-1", "production", "standard")
        await binding.bind(alloc.id, "worker-1")

        rotator.wait_for_task_completion = AsyncMock(side_effect=Exception("timeout"))
        rotator.send_credential = AsyncMock(return_value=True)

        result = await rotator.rotate("worker-1", "prod-1", "quota_exceeded")
        assert result.success

    @pytest.mark.asyncio
    async def test_rotation_tries_cooldown_recovery(self, tmp_path, binding, event_bus):
        """T-046: If no accounts available, rotator checks cooldown recovery before giving up."""
        accounts_data = {
            "accounts": [
                {"id": "prod-1", "email": "p1@test.com", "email_token": "t1", "group": "standard"},
                {"id": "prod-2", "email": "p2@test.com", "email_token": "t2", "group": "standard"},
            ],
            "groups": {"standard": {"description": "Standard"}},
        }
        accounts_file = tmp_path / "accounts.json"
        accounts_file.write_text(json.dumps(accounts_data))

        config = CredentialConfig(
            accounts_file=str(accounts_file),
            pool_status_file=str(tmp_path / "pool_status.json"),
            quota_threshold=0.85,
        )
        pool = CredentialPool(config)
        await pool.load()

        # Allocate prod-1 to worker-1
        await pool.allocate("worker-1", "production", "standard")
        await binding.bind("prod-1", "worker-1")

        # Mark prod-2 in cooldown but with past backoff (should recover)
        status2 = pool.get_status("prod-2")
        status2.backoff_until = datetime.now(timezone.utc) - timedelta(hours=1)
        status2.available = False
        status2.five_hour.utilization = 10.0

        rotator = CredentialRotator(pool, binding, event_bus)
        rotator.send_credential = AsyncMock(return_value=True)

        result = await rotator.rotate("worker-1", "prod-1", "quota_exceeded")

        assert result.success
        assert result.new_account_id == "prod-2"


class TestCooldownRecovery:
    """T-046: Cooldown recovery tests (via CredentialPool.check_cooldown_recovery)."""

    @pytest.mark.asyncio
    async def test_account_recovers_after_cooldown(self, tmp_pool):
        await tmp_pool.load()

        # Allocate and then release with cooldown
        alloc = await tmp_pool.allocate("worker-1", "production", "standard")
        await tmp_pool.release(alloc.id)
        await tmp_pool.mark_cooldown(alloc.id)

        status = tmp_pool.get_status(alloc.id)
        assert status.backoff_until is not None
        assert not status.available

        # Manually set backoff to past
        status.backoff_until = datetime.now(timezone.utc) - timedelta(hours=1)
        status.five_hour.utilization = 10.0

        await tmp_pool.check_cooldown_recovery()

        status = tmp_pool.get_status(alloc.id)
        assert status.backoff_until is None
        assert status.available

    @pytest.mark.asyncio
    async def test_account_stays_in_cooldown_if_not_expired(self, tmp_pool):
        await tmp_pool.load()

        alloc = await tmp_pool.allocate("worker-1", "production", "standard")
        await tmp_pool.release(alloc.id)
        await tmp_pool.mark_cooldown(alloc.id)

        status = tmp_pool.get_status(alloc.id)
        assert status.backoff_until is not None

        # Cooldown not expired yet
        await tmp_pool.check_cooldown_recovery()

        status = tmp_pool.get_status(alloc.id)
        assert status.backoff_until is not None

    @pytest.mark.asyncio
    async def test_multiple_accounts_recover(self, tmp_pool):
        await tmp_pool.load()

        # Allocate two accounts and put both in cooldown
        a1 = await tmp_pool.allocate("worker-1", "production", "standard")
        a2 = await tmp_pool.allocate("worker-2", "production", "standard")

        await tmp_pool.release(a1.id)
        await tmp_pool.release(a2.id)
        await tmp_pool.mark_cooldown(a1.id)
        await tmp_pool.mark_cooldown(a2.id)

        # Set both to past backoff
        for acct_id in [a1.id, a2.id]:
            s = tmp_pool.get_status(acct_id)
            s.backoff_until = datetime.now(timezone.utc) - timedelta(hours=1)
            s.five_hour.utilization = 5.0

        await tmp_pool.check_cooldown_recovery()

        for acct_id in [a1.id, a2.id]:
            s = tmp_pool.get_status(acct_id)
            assert s.backoff_until is None
            assert s.available
