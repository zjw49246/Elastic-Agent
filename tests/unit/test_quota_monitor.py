"""Tests for Manager-side QuotaMonitor — T-044."""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from elastic_agent.core.config import CredentialConfig
from elastic_agent.core.credential_pool import AccountDefinition, CredentialPool
from elastic_agent.core.event_bus import EventBus
from elastic_agent.core.quota_monitor import QuotaMonitor


@pytest.fixture
def tmp_pool(tmp_path):
    accounts_data = {
        "accounts": [
            {"id": "prod-1", "email": "p1@test.com", "email_token": "t1", "group": "standard"},
            {"id": "prod-2", "email": "p2@test.com", "email_token": "t2", "group": "standard"},
            {"id": "prod-3", "email": "p3@test.com", "email_token": "t3", "group": "standard"},
        ],
        "groups": {"standard": {"description": "Standard"}},
    }
    accounts_file = tmp_path / "accounts.json"
    accounts_file.write_text(json.dumps(accounts_data))

    config = CredentialConfig(
        accounts_file=str(accounts_file),
        pool_status_file=str(tmp_path / "pool_status.json"),
        quota_threshold=0.85,
        weekly_reserve_per_day=0,
    )
    pool = CredentialPool(config)
    return pool


@pytest.fixture
def event_bus():
    return EventBus()


@pytest.fixture
def config():
    return CredentialConfig(
        quota_threshold=0.85,
        weekly_reserve_per_day=0,
    )


@pytest.fixture
def monitor(tmp_pool, event_bus, config):
    return QuotaMonitor(tmp_pool, event_bus, config)


class TestQuotaMonitor:
    @pytest.mark.asyncio
    async def test_start_stop(self, monitor, event_bus):
        await monitor.start()
        assert monitor._sub_id is not None
        assert event_bus.subscriber_count("QUOTA_STATUS") == 1

        await monitor.stop()
        assert monitor._sub_id is None
        assert event_bus.subscriber_count("QUOTA_STATUS") == 0

    @pytest.mark.asyncio
    async def test_handle_normal_quota(self, monitor, tmp_pool, event_bus):
        await tmp_pool.load()
        await monitor.start()

        warnings = []

        async def capture_warning(etype, wid, data):
            warnings.append(data)

        event_bus.subscribe("QUOTA_WARNING", capture_warning)

        await event_bus.emit("QUOTA_STATUS", "worker-1", {
            "account_id": "prod-1",
            "five_hour_pct": 50.0,
            "seven_day_pct": 30.0,
            "five_hour_resets_at": "2026-05-19T17:34:56Z",
            "seven_day_resets_at": "2026-05-26T00:00:00Z",
        })

        assert len(warnings) == 0

        status = tmp_pool.get_status("prod-1")
        assert status.five_hour.utilization == 50.0
        assert status.seven_day.utilization == 30.0

        await monitor.stop()

    @pytest.mark.asyncio
    async def test_handle_warning_threshold(self, monitor, tmp_pool, event_bus):
        await tmp_pool.load()
        await monitor.start()

        warnings = []

        async def capture_warning(etype, wid, data):
            warnings.append(data)

        event_bus.subscribe("QUOTA_WARNING", capture_warning)

        await event_bus.emit("QUOTA_STATUS", "worker-1", {
            "account_id": "prod-1",
            "five_hour_pct": 86.0,
            "seven_day_pct": 30.0,
        })

        assert len(warnings) == 1
        assert warnings[0]["reason"] == "quota_warning"
        assert warnings[0]["usage_pct"] == 86.0

        await monitor.stop()

    @pytest.mark.asyncio
    async def test_handle_critical_triggers_rotation(self, monitor, tmp_pool, event_bus):
        await tmp_pool.load()

        rotation_calls = []
        monitor.on_rotation_needed = AsyncMock(
            side_effect=lambda w, a, r: rotation_calls.append((w, a, r))
        )
        await monitor.start()

        await event_bus.emit("QUOTA_STATUS", "worker-1", {
            "account_id": "prod-1",
            "five_hour_pct": 96.0,
            "seven_day_pct": 30.0,
        })

        assert len(rotation_calls) == 1
        assert rotation_calls[0] == ("worker-1", "prod-1", "quota_exceeded")

        await monitor.stop()

    @pytest.mark.asyncio
    async def test_handle_rate_limited_triggers_rotation(self, monitor, tmp_pool, event_bus):
        await tmp_pool.load()

        rotation_calls = []
        monitor.on_rotation_needed = AsyncMock(
            side_effect=lambda w, a, r: rotation_calls.append((w, a, r))
        )
        await monitor.start()

        await event_bus.emit("QUOTA_STATUS", "worker-1", {
            "account_id": "prod-1",
            "five_hour_pct": 95.0,
            "seven_day_pct": 30.0,
            "error": "rate_limited",
        })

        assert len(rotation_calls) == 1

        await monitor.stop()

    @pytest.mark.asyncio
    async def test_handle_relogin_needed(self, monitor, tmp_pool, event_bus):
        await tmp_pool.load()

        relogin_calls = []
        monitor.on_relogin_needed = AsyncMock(
            side_effect=lambda w, a: relogin_calls.append((w, a))
        )
        await monitor.start()

        await event_bus.emit("QUOTA_STATUS", "worker-1", {
            "account_id": "prod-1",
            "five_hour_pct": 50.0,
            "seven_day_pct": 30.0,
            "needs_relogin": True,
        })

        assert len(relogin_calls) == 1
        assert relogin_calls[0] == ("worker-1", "prod-1")

        await monitor.stop()

    @pytest.mark.asyncio
    async def test_weekly_budget_management(self, tmp_pool, event_bus):
        config = CredentialConfig(
            quota_threshold=0.85,
            weekly_reserve_per_day=10,
        )
        monitor = QuotaMonitor(tmp_pool, event_bus, config)
        await tmp_pool.load()
        await monitor.start()

        warnings = []

        async def capture_warning(etype, wid, data):
            warnings.append(data)

        event_bus.subscribe("QUOTA_WARNING", capture_warning)

        # With 3 days remaining and 10% reserve per day:
        # dynamic_threshold = 100 - (3 * 10) = 70
        resets_at = (datetime.now(timezone.utc) + timedelta(days=3)).isoformat()

        await event_bus.emit("QUOTA_STATUS", "worker-1", {
            "account_id": "prod-1",
            "five_hour_pct": 50.0,
            "seven_day_pct": 75.0,
            "seven_day_resets_at": resets_at,
        })

        # 75% >= 70% dynamic threshold → should trigger weekly warning
        weekly_warnings = [w for w in warnings if w.get("reason") == "weekly_budget_exceeded"]
        assert len(weekly_warnings) == 1

        await monitor.stop()

    @pytest.mark.asyncio
    async def test_weekly_budget_not_triggered_when_disabled(self, tmp_pool, event_bus):
        config = CredentialConfig(
            quota_threshold=0.85,
            weekly_reserve_per_day=0,
        )
        monitor = QuotaMonitor(tmp_pool, event_bus, config)
        await tmp_pool.load()
        await monitor.start()

        warnings = []

        async def capture_warning(etype, wid, data):
            warnings.append(data)

        event_bus.subscribe("QUOTA_WARNING", capture_warning)

        resets_at = (datetime.now(timezone.utc) + timedelta(days=3)).isoformat()
        await event_bus.emit("QUOTA_STATUS", "worker-1", {
            "account_id": "prod-1",
            "five_hour_pct": 50.0,
            "seven_day_pct": 99.0,
            "seven_day_resets_at": resets_at,
        })

        weekly_warnings = [w for w in warnings if w.get("reason") == "weekly_budget_exceeded"]
        assert len(weekly_warnings) == 0

        await monitor.stop()

    @pytest.mark.asyncio
    async def test_process_quota_status_directly(self, monitor, tmp_pool):
        await tmp_pool.load()

        await monitor.process_quota_status("worker-1", {
            "account_id": "prod-1",
            "five_hour_pct": 42.0,
            "seven_day_pct": 21.0,
        })

        status = tmp_pool.get_status("prod-1")
        assert status.five_hour.utilization == 42.0
        assert status.seven_day.utilization == 21.0


class TestDynamicThreshold:
    def test_3_days_remaining(self):
        config = CredentialConfig(weekly_reserve_per_day=10)
        monitor = QuotaMonitor(CredentialPool(config), EventBus(), config)

        resets_at = datetime.now(timezone.utc) + timedelta(days=3)
        threshold = monitor._calculate_dynamic_threshold(resets_at)
        assert 69 <= threshold <= 71  # ~70%

    def test_7_days_remaining(self):
        config = CredentialConfig(weekly_reserve_per_day=10)
        monitor = QuotaMonitor(CredentialPool(config), EventBus(), config)

        resets_at = datetime.now(timezone.utc) + timedelta(days=7)
        threshold = monitor._calculate_dynamic_threshold(resets_at)
        assert 29 <= threshold <= 31  # ~30%

    def test_0_days_remaining(self):
        config = CredentialConfig(weekly_reserve_per_day=10)
        monitor = QuotaMonitor(CredentialPool(config), EventBus(), config)

        resets_at = datetime.now(timezone.utc)
        threshold = monitor._calculate_dynamic_threshold(resets_at)
        assert threshold >= 99  # ~100%

    def test_none_resets_at(self):
        config = CredentialConfig(weekly_reserve_per_day=10)
        monitor = QuotaMonitor(CredentialPool(config), EventBus(), config)

        threshold = monitor._calculate_dynamic_threshold(None)
        assert threshold == 100.0


class TestParseDatetime:
    def test_iso_string(self):
        config = CredentialConfig()
        monitor = QuotaMonitor(CredentialPool(config), EventBus(), config)

        dt = monitor._parse_datetime("2026-05-19T17:34:56Z")
        assert dt is not None
        assert dt.year == 2026
        assert dt.month == 5

    def test_none(self):
        config = CredentialConfig()
        monitor = QuotaMonitor(CredentialPool(config), EventBus(), config)
        assert monitor._parse_datetime(None) is None

    def test_datetime_passthrough(self):
        config = CredentialConfig()
        monitor = QuotaMonitor(CredentialPool(config), EventBus(), config)

        dt = datetime(2026, 5, 19, tzinfo=timezone.utc)
        assert monitor._parse_datetime(dt) is dt

    def test_invalid_string(self):
        config = CredentialConfig()
        monitor = QuotaMonitor(CredentialPool(config), EventBus(), config)
        assert monitor._parse_datetime("not-a-date") is None


class TestCooldownLoop:
    @pytest.mark.asyncio
    async def test_cooldown_recovery_runs(self, tmp_pool, event_bus, config):
        await tmp_pool.load()

        # Mark an account in cooldown with past backoff
        await tmp_pool.allocate("worker-1", "production", "standard")
        for acct_id, status in tmp_pool._pool_status.accounts.items():
            if status.assigned_to == "worker-1":
                assigned_id = acct_id
                break

        await tmp_pool.release(assigned_id)
        await tmp_pool.mark_cooldown(assigned_id)

        # Set backoff to past
        status = tmp_pool.get_status(assigned_id)
        status.backoff_until = datetime.now(timezone.utc) - timedelta(hours=1)
        status.five_hour.utilization = 0.0

        monitor = QuotaMonitor(tmp_pool, event_bus, config)
        await monitor.start()

        # Give the cooldown loop a moment to run
        await asyncio.sleep(0.1)

        await tmp_pool.check_cooldown_recovery()
        recovered = tmp_pool.get_status(assigned_id)
        assert recovered.backoff_until is None

        await monitor.stop()


# ===========================================================================
# T-133: QuotaMonitor extended — threshold detection, QUOTA_WARNING events,
#        cooldown recovery
# ===========================================================================


def _pool_available(pool, account_id):
    s = pool.get_status(account_id)
    return s.available if s else None


class TestT133ThresholdDetection:
    """T-133: Threshold detection edge cases."""

    @pytest.mark.asyncio
    async def test_exactly_at_threshold_triggers_warning(self, tmp_pool, event_bus, config):
        await tmp_pool.load()
        monitor = QuotaMonitor(tmp_pool, event_bus, config)
        await monitor.start()

        warnings = []
        event_bus.subscribe("QUOTA_WARNING", lambda et, wid, data: warnings.append(data))

        await event_bus.emit("QUOTA_STATUS", "worker-1", {
            "account_id": "prod-1",
            "five_hour_pct": 85.0,
            "seven_day_pct": 30.0,
        })

        assert len(warnings) == 1
        assert warnings[0]["reason"] == "quota_warning"
        await monitor.stop()

    @pytest.mark.asyncio
    async def test_just_below_threshold_no_warning(self, tmp_pool, event_bus, config):
        await tmp_pool.load()
        monitor = QuotaMonitor(tmp_pool, event_bus, config)
        await monitor.start()

        warnings = []
        event_bus.subscribe("QUOTA_WARNING", lambda et, wid, data: warnings.append(data))

        await event_bus.emit("QUOTA_STATUS", "worker-1", {
            "account_id": "prod-1",
            "five_hour_pct": 84.9,
            "seven_day_pct": 30.0,
        })

        assert len(warnings) == 0
        await monitor.stop()

    @pytest.mark.asyncio
    async def test_critical_95_triggers_rotation(self, tmp_pool, event_bus, config):
        await tmp_pool.load()
        monitor = QuotaMonitor(tmp_pool, event_bus, config)
        rotation_calls = []
        monitor.on_rotation_needed = AsyncMock(
            side_effect=lambda w, a, r: rotation_calls.append((w, a, r))
        )
        await monitor.start()

        await event_bus.emit("QUOTA_STATUS", "worker-1", {
            "account_id": "prod-1",
            "five_hour_pct": 95.0,
            "seven_day_pct": 30.0,
        })

        assert len(rotation_calls) == 1
        assert rotation_calls[0][2] == "quota_exceeded"
        await monitor.stop()

    @pytest.mark.asyncio
    async def test_multiple_accounts_threshold(self, tmp_pool, event_bus, config):
        await tmp_pool.load()
        monitor = QuotaMonitor(tmp_pool, event_bus, config)
        await monitor.start()

        warnings = []
        event_bus.subscribe("QUOTA_WARNING", lambda et, wid, data: warnings.append(data))

        for i in range(1, 4):
            await event_bus.emit("QUOTA_STATUS", f"worker-{i}", {
                "account_id": f"prod-{i}",
                "five_hour_pct": 86.0 + i,
                "seven_day_pct": 30.0,
            })

        assert len(warnings) == 3
        acct_ids = {w["account_id"] for w in warnings}
        assert acct_ids == {"prod-1", "prod-2", "prod-3"}
        await monitor.stop()

    @pytest.mark.asyncio
    async def test_unknown_account_no_crash(self, tmp_pool, event_bus, config):
        await tmp_pool.load()
        monitor = QuotaMonitor(tmp_pool, event_bus, config)
        await monitor.start()

        await event_bus.emit("QUOTA_STATUS", "worker-1", {
            "account_id": "nonexistent",
            "five_hour_pct": 50.0,
            "seven_day_pct": 30.0,
        })
        await monitor.stop()


class TestT133QuotaWarningPayload:
    """T-133: QUOTA_WARNING event payload validation."""

    @pytest.mark.asyncio
    async def test_warning_payload_has_required_fields(self, tmp_pool, event_bus, config):
        await tmp_pool.load()
        monitor = QuotaMonitor(tmp_pool, event_bus, config)
        await monitor.start()

        warnings = []
        event_bus.subscribe("QUOTA_WARNING", lambda et, wid, data: warnings.append((wid, data)))

        await event_bus.emit("QUOTA_STATUS", "worker-1", {
            "account_id": "prod-1",
            "five_hour_pct": 87.5,
            "seven_day_pct": 30.0,
        })

        assert len(warnings) == 1
        wid, data = warnings[0]
        assert wid == "worker-1"
        assert data["account_id"] == "prod-1"
        assert data["usage_pct"] == 87.5
        assert data["reason"] in ("quota_warning", "quota_critical")
        await monitor.stop()

    @pytest.mark.asyncio
    async def test_critical_warning_emits_before_rotation(self, tmp_pool, event_bus, config):
        await tmp_pool.load()
        monitor = QuotaMonitor(tmp_pool, event_bus, config)
        event_order = []

        async def on_rotation(w, a, r):
            event_order.append("rotation")

        async def on_warning(et, wid, data):
            event_order.append("warning")

        monitor.on_rotation_needed = on_rotation
        event_bus.subscribe("QUOTA_WARNING", on_warning)
        await monitor.start()

        await event_bus.emit("QUOTA_STATUS", "worker-1", {
            "account_id": "prod-1",
            "five_hour_pct": 96.0,
            "seven_day_pct": 30.0,
        })

        assert event_order == ["warning", "rotation"]
        await monitor.stop()


class TestT133CooldownRecoveryExtended:
    """T-133: Cooldown recovery integration."""

    @pytest.mark.asyncio
    async def test_cooldown_recovery_makes_account_allocatable(self, tmp_pool, event_bus, config):
        await tmp_pool.load()

        acct = await tmp_pool.allocate("w1", "production", "standard")
        await tmp_pool.release(acct.id)
        await tmp_pool.mark_cooldown(acct.id)
        assert _pool_available(tmp_pool, acct.id) is False

        status = tmp_pool.get_status(acct.id)
        status.backoff_until = datetime.now(timezone.utc) - timedelta(hours=1)
        status.five_hour.utilization = 0.0

        await tmp_pool.check_cooldown_recovery()

        assert _pool_available(tmp_pool, acct.id) is True
        re_alloc = await tmp_pool.allocate("w2", "production", "standard")
        assert re_alloc is not None
        assert re_alloc.id == acct.id

    @pytest.mark.asyncio
    async def test_cooldown_not_recovered_while_quota_high(self, tmp_pool, event_bus, config):
        await tmp_pool.load()
        acct = await tmp_pool.allocate("w1", "production", "standard")
        await tmp_pool.release(acct.id)
        await tmp_pool.update_quota(acct.id, 90.0, 50.0)
        await tmp_pool.mark_cooldown(acct.id)

        status = tmp_pool.get_status(acct.id)
        status.backoff_until = datetime.now(timezone.utc) - timedelta(hours=1)

        await tmp_pool.check_cooldown_recovery()

        status = tmp_pool.get_status(acct.id)
        assert status.backoff_until is None
        assert status.available is False
