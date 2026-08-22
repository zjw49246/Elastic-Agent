"""T-130: Credential rotation E2E integration test.

Tests: quota exhaustion → auto credential switch → process uses new credential.
Verifies the full CredentialPool → QuotaMonitor → CredentialRotator pipeline.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

from elastic_agent.core.config import CredentialConfig
from elastic_agent.core.credential_binding import CredentialBinding
from elastic_agent.core.credential_pool import CredentialPool
from elastic_agent.core.credential_rotator import CredentialRotator
from elastic_agent.core.event_bus import EventBus
from elastic_agent.core.quota_monitor import QuotaMonitor


def _write_accounts(tmp_path: Path, accounts: list[dict]) -> Path:
    path = tmp_path / "accounts.json"
    data = {"accounts": accounts, "groups": {"standard": {"description": "default"}}}
    path.write_text(json.dumps(data), encoding="utf-8")
    return path


@pytest.mark.level1
class TestCredentialRotationE2E:
    """Full credential rotation pipeline."""

    @pytest.mark.asyncio
    async def test_quota_exhaustion_triggers_rotation(self, tmp_path):
        accounts = [
            {"id": "acct-1", "email": "a1@test.com", "group": "standard"},
            {"id": "acct-2", "email": "a2@test.com", "group": "standard"},
        ]
        _write_accounts(tmp_path, accounts)

        config = CredentialConfig(
            accounts_file=str(tmp_path / "accounts.json"),
            pool_status_file=str(tmp_path / "pool_status.json"),
            quota_threshold=0.85,
        )
        pool = CredentialPool(config)
        await pool.load()

        binding = CredentialBinding(max_accounts_per_worker=4)
        event_bus = EventBus()
        rotator = CredentialRotator(pool, binding, event_bus)

        # Allocate acct-1 to worker-1
        acct1 = await pool.allocate("worker-1", "production", "standard")
        assert acct1.id == "acct-1"
        await binding.bind("acct-1", "worker-1")

        # Set up send_credential callback
        sent_creds: list[dict] = []

        async def mock_send(worker_id, account_id, creds, config_dir):
            sent_creds.append({"worker": worker_id, "account": account_id})
            return True

        rotator.send_credential = mock_send

        # Rotate
        result = await rotator.rotate("worker-1", "acct-1", "quota_exceeded")

        assert result.success
        assert result.old_account_id == "acct-1"
        assert result.new_account_id == "acct-2"
        assert len(sent_creds) == 1
        assert sent_creds[0]["account"] == "acct-2"

        # Verify pool state
        old_status = pool.get_status("acct-1")
        assert old_status.assigned_to is None
        assert old_status.backoff_until is not None

    @pytest.mark.asyncio
    async def test_no_replacement_available(self, tmp_path):
        accounts = [
            {"id": "only-one", "email": "only@test.com", "group": "standard"},
        ]
        _write_accounts(tmp_path, accounts)

        config = CredentialConfig(
            accounts_file=str(tmp_path / "accounts.json"),
            pool_status_file=str(tmp_path / "pool_status.json"),
        )
        pool = CredentialPool(config)
        await pool.load()

        binding = CredentialBinding()
        event_bus = EventBus()
        rotator = CredentialRotator(pool, binding, event_bus)

        acct = await pool.allocate("worker-1", "production", "standard")
        await binding.bind(acct.id, "worker-1")

        exhausted_events: list[dict] = []

        async def on_exhausted(event_type, worker_id, data):
            exhausted_events.append(data)

        event_bus.subscribe("CREDENTIAL_EXHAUSTED", on_exhausted)

        result = await rotator.rotate("worker-1", "only-one", "quota_exceeded")

        assert not result.success
        assert "no replacement" in result.error.lower() or "No replacement" in result.error
        assert exhausted_events == []
        status = pool.get_status("only-one")
        assert status is not None
        assert status.assigned_to == "worker-1"

    @pytest.mark.asyncio
    async def test_rotation_waits_for_task(self, tmp_path):
        accounts = [
            {"id": "a1", "email": "a1@test.com", "group": "standard"},
            {"id": "a2", "email": "a2@test.com", "group": "standard"},
        ]
        _write_accounts(tmp_path, accounts)

        config = CredentialConfig(
            accounts_file=str(tmp_path / "accounts.json"),
            pool_status_file=str(tmp_path / "pool_status.json"),
        )
        pool = CredentialPool(config)
        await pool.load()

        binding = CredentialBinding()
        event_bus = EventBus()
        rotator = CredentialRotator(pool, binding, event_bus)

        await pool.allocate("w1", "production", "standard")
        await binding.bind("a1", "w1")

        wait_called = False

        async def mock_wait(worker_id):
            nonlocal wait_called
            wait_called = True
            await asyncio.sleep(0.1)

        async def mock_send(wid, aid, creds, cd):
            return True

        rotator.wait_for_task_completion = mock_wait
        rotator.send_credential = mock_send

        result = await rotator.rotate("w1", "a1", "quota_exceeded")

        assert result.success
        assert wait_called

    @pytest.mark.asyncio
    async def test_rotation_with_login(self, tmp_path):
        accounts = [
            {"id": "login-old", "email": "old@test.com", "group": "standard"},
            {"id": "login-new", "email": "new@test.com", "group": "standard"},
        ]
        _write_accounts(tmp_path, accounts)

        config = CredentialConfig(
            accounts_file=str(tmp_path / "accounts.json"),
            pool_status_file=str(tmp_path / "pool_status.json"),
        )
        pool = CredentialPool(config)
        await pool.load()

        binding = CredentialBinding()
        event_bus = EventBus()
        rotator = CredentialRotator(pool, binding, event_bus)

        await pool.allocate("w1", "production", "standard")
        await binding.bind("login-old", "w1")

        login_calls: list[str] = []

        class LoginResult:
            success = True

        async def mock_login(worker_id, account_def, config_dir):
            login_calls.append(account_def.id)
            return LoginResult()

        async def mock_send(wid, aid, creds, cd):
            return True

        rotator.execute_login = mock_login
        rotator.send_credential = mock_send

        result = await rotator.rotate("w1", "login-old", "quota_exceeded")

        assert result.success
        assert "login-new" in login_calls


@pytest.mark.level1
class TestQuotaMonitorIntegration:
    """QuotaMonitor + EventBus + CredentialPool integration."""

    @pytest.mark.asyncio
    async def test_quota_critical_triggers_rotation_callback(self, tmp_path):
        accounts = [{"id": "qm-1", "email": "qm@test.com", "group": "standard"}]
        _write_accounts(tmp_path, accounts)

        config = CredentialConfig(
            accounts_file=str(tmp_path / "accounts.json"),
            pool_status_file=str(tmp_path / "pool_status.json"),
            quota_threshold=0.85,
        )
        pool = CredentialPool(config)
        await pool.load()

        event_bus = EventBus()
        monitor = QuotaMonitor(pool, event_bus, config)

        rotation_requests: list[tuple] = []

        async def on_rotation(worker_id, account_id, reason):
            rotation_requests.append((worker_id, account_id, reason))

        monitor.on_rotation_needed = on_rotation
        await monitor.start()

        try:
            await event_bus.emit("QUOTA_STATUS", "worker-qm", {
                "account_id": "qm-1",
                "five_hour_pct": 96.0,
                "seven_day_pct": 40.0,
            })
            await asyncio.sleep(0.2)

            assert len(rotation_requests) == 1
            assert rotation_requests[0] == ("worker-qm", "qm-1", "quota_exceeded")
        finally:
            await monitor.stop()

    @pytest.mark.asyncio
    async def test_quota_warning_triggers_graceful_rotation(self, tmp_path):
        accounts = [{"id": "warn-1", "email": "w@test.com", "group": "standard"}]
        _write_accounts(tmp_path, accounts)

        config = CredentialConfig(
            accounts_file=str(tmp_path / "accounts.json"),
            pool_status_file=str(tmp_path / "pool_status.json"),
            quota_threshold=0.85,
        )
        pool = CredentialPool(config)
        await pool.load()

        event_bus = EventBus()
        monitor = QuotaMonitor(pool, event_bus, config)

        rotation_requests: list = []

        async def on_rotation(wid, aid, reason):
            rotation_requests.append(reason)

        monitor.on_rotation_needed = on_rotation

        warning_events: list[dict] = []

        async def on_warning(event_type, worker_id, data):
            warning_events.append(data)

        event_bus.subscribe("QUOTA_WARNING", on_warning)

        await monitor.start()
        try:
            await event_bus.emit("QUOTA_STATUS", "w-warn", {
                "account_id": "warn-1",
                "five_hour_pct": 87.0,
                "seven_day_pct": 30.0,
            })
            await asyncio.sleep(0.2)

            assert rotation_requests == ["quota_exceeded"]
            assert len(warning_events) == 1
            assert warning_events[0]["reason"] == "quota_warning"
        finally:
            await monitor.stop()

    @pytest.mark.asyncio
    async def test_quota_updates_pool_status(self, tmp_path):
        accounts = [{"id": "pool-upd", "email": "p@test.com", "group": "standard"}]
        _write_accounts(tmp_path, accounts)

        config = CredentialConfig(
            accounts_file=str(tmp_path / "accounts.json"),
            pool_status_file=str(tmp_path / "pool_status.json"),
            quota_threshold=0.85,
        )
        pool = CredentialPool(config)
        await pool.load()

        event_bus = EventBus()
        monitor = QuotaMonitor(pool, event_bus, config)
        await monitor.start()

        try:
            await event_bus.emit("QUOTA_STATUS", "w-upd", {
                "account_id": "pool-upd",
                "five_hour_pct": 42.5,
                "seven_day_pct": 15.0,
            })
            await asyncio.sleep(0.2)

            status = pool.get_status("pool-upd")
            assert status.five_hour.utilization == 42.5
            assert status.seven_day.utilization == 15.0
        finally:
            await monitor.stop()
