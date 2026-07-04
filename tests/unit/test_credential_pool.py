"""Unit tests for CredentialPool — account pool management, persistence, concurrency."""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from elastic_agent.core.config import CredentialConfig
from elastic_agent.core.credential_pool import (
    AccountDefinition,
    AccountsConfig,
    AccountStatus,
    CredentialPool,
    GroupDefinition,
    PoolStatus,
    QuotaWindow,
)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _write_accounts(path: Path, accounts: list[dict], groups: dict | None = None) -> None:
    """Helper to write an accounts.json file."""
    data = {
        "accounts": accounts,
        "groups": groups or {},
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")


def _make_accounts(count: int = 3, group: str = "high_quota") -> list[dict]:
    """Create a list of account dicts for testing."""
    return [
        {
            "id": f"prod-{i}",
            "email": f"user{i}@171mail.com",
            "email_token": f"sk-mailapi-{i}",
            "group": group,
            "enabled": True,
        }
        for i in range(1, count + 1)
    ]


@pytest.fixture
def accounts_path(tmp_path: Path) -> Path:
    return tmp_path / "accounts.json"


@pytest.fixture
def status_path(tmp_path: Path) -> Path:
    return tmp_path / "pool_status.json"


@pytest.fixture
def config(accounts_path: Path, status_path: Path) -> CredentialConfig:
    return CredentialConfig(
        accounts_file=str(accounts_path),
        pool_status_file=str(status_path),
        quota_threshold=0.85,
        max_accounts_per_worker=4,
    )


@pytest.fixture
def pool(config: CredentialConfig, accounts_path: Path) -> CredentialPool:
    _write_accounts(
        accounts_path,
        _make_accounts(3, "high_quota"),
        {"high_quota": {"description": "Primary production accounts"}},
    )
    return CredentialPool(config)


# ---------------------------------------------------------------------------
# 1. Load accounts.json
# ---------------------------------------------------------------------------
async def test_load_accounts(pool: CredentialPool) -> None:
    await pool.load()
    summary = pool.pool_summary()
    assert summary["total"] == 3
    assert summary["groups"]["high_quota"]["total"] == 3


# ---------------------------------------------------------------------------
# 2. Load with no pool_status.json (fresh start)
# ---------------------------------------------------------------------------
async def test_load_no_status_file(pool: CredentialPool, status_path: Path) -> None:
    assert not status_path.exists()
    await pool.load()
    # All accounts should have status entries initialised
    for i in range(1, 4):
        status = pool.get_status(f"prod-{i}")
        assert status is not None
        assert status.email == f"user{i}@171mail.com"
        assert status.group == "high_quota"


# ---------------------------------------------------------------------------
# 3. Allocate account — returns correct group
# ---------------------------------------------------------------------------
async def test_allocate_correct_group(pool: CredentialPool) -> None:
    await pool.load()
    acct = await pool.allocate("worker-1", "production", "high_quota")
    assert acct is not None
    assert acct.group == "high_quota"
    assert isinstance(acct, AccountDefinition)


# ---------------------------------------------------------------------------
# 4. Allocate — respects max_accounts_per_worker
# ---------------------------------------------------------------------------
async def test_allocate_max_per_worker(
    config: CredentialConfig, accounts_path: Path
) -> None:
    # Create more accounts than max_accounts_per_worker (4)
    _write_accounts(accounts_path, _make_accounts(6, "high_quota"))
    config_limited = CredentialConfig(
        accounts_file=str(accounts_path),
        pool_status_file=str(config.pool_status_file),
        quota_threshold=0.85,
        max_accounts_per_worker=2,
    )
    pool = CredentialPool(config_limited)
    await pool.load()

    acct1 = await pool.allocate("worker-1", "production", "high_quota")
    acct2 = await pool.allocate("worker-1", "production", "high_quota")
    acct3 = await pool.allocate("worker-1", "production", "high_quota")

    assert acct1 is not None
    assert acct2 is not None
    assert acct3 is None  # exceeds limit of 2


async def test_allocate_replacement_allows_handoff_with_single_worker_slot(
    config: CredentialConfig, accounts_path: Path
) -> None:
    _write_accounts(accounts_path, _make_accounts(2, "high_quota"))
    config_limited = CredentialConfig(
        accounts_file=str(accounts_path),
        pool_status_file=str(config.pool_status_file),
        quota_threshold=0.85,
        max_accounts_per_worker=1,
    )
    pool = CredentialPool(config_limited)
    await pool.load()

    old = await pool.allocate("worker-1", "production", "high_quota")
    assert old is not None
    assert await pool.allocate("worker-1", "production", "high_quota") is None

    replacement = await pool.allocate_replacement(
        old.id,
        "worker-1",
        "production",
        "high_quota",
    )

    assert replacement is not None
    assert replacement.id != old.id


# ---------------------------------------------------------------------------
# 5. Allocate — skips unavailable accounts
# ---------------------------------------------------------------------------
async def test_allocate_skips_unavailable(pool: CredentialPool) -> None:
    await pool.load()

    # Mark first two accounts as unavailable (quota exhausted)
    await pool.update_quota("prod-1", 90.0, 50.0)
    await pool.update_quota("prod-2", 90.0, 50.0)

    acct = await pool.allocate("worker-1", "production", "high_quota")
    assert acct is not None
    assert acct.id == "prod-3"


# ---------------------------------------------------------------------------
# 6. Allocate — skips accounts in backoff
# ---------------------------------------------------------------------------
async def test_allocate_skips_backoff(pool: CredentialPool) -> None:
    await pool.load()

    # Put prod-1 in backoff
    await pool.mark_cooldown("prod-1")

    acct = await pool.allocate("worker-1", "production", "high_quota")
    assert acct is not None
    assert acct.id != "prod-1"


# ---------------------------------------------------------------------------
# 7. Allocate — least_used_first ordering
# ---------------------------------------------------------------------------
async def test_allocate_least_used_first(pool: CredentialPool) -> None:
    await pool.load()

    # Set different utilization levels
    await pool.update_quota("prod-1", 50.0, 30.0)
    await pool.update_quota("prod-2", 10.0, 20.0)
    await pool.update_quota("prod-3", 70.0, 40.0)

    acct = await pool.allocate("worker-1", "production", "high_quota")
    assert acct is not None
    assert acct.id == "prod-2"  # lowest five_hour utilization


# ---------------------------------------------------------------------------
# 8. Release account
# ---------------------------------------------------------------------------
async def test_release_account(pool: CredentialPool) -> None:
    await pool.load()
    acct = await pool.allocate("worker-1", "production", "high_quota")
    assert acct is not None

    status = pool.get_status(acct.id)
    assert status is not None
    assert status.assigned_to == "worker-1"

    await pool.release(acct.id)
    status = pool.get_status(acct.id)
    assert status is not None
    assert status.assigned_to is None
    assert status.slot_type is None


# ---------------------------------------------------------------------------
# 9. Release all worker accounts
# ---------------------------------------------------------------------------
async def test_release_worker(pool: CredentialPool) -> None:
    await pool.load()
    a1 = await pool.allocate("worker-1", "production", "high_quota")
    a2 = await pool.allocate("worker-1", "edit", "high_quota")
    assert a1 is not None
    assert a2 is not None

    assigned = pool.list_assigned_to("worker-1")
    assert len(assigned) == 2

    await pool.release_worker("worker-1")
    assigned = pool.list_assigned_to("worker-1")
    assert len(assigned) == 0


# ---------------------------------------------------------------------------
# 10. Update quota — marks unavailable at threshold
# ---------------------------------------------------------------------------
async def test_update_quota_marks_unavailable(pool: CredentialPool) -> None:
    await pool.load()

    # Below threshold — should be available
    await pool.update_quota("prod-1", 50.0, 50.0)
    status = pool.get_status("prod-1")
    assert status is not None
    assert status.available is True

    # At threshold — should be unavailable
    await pool.update_quota("prod-1", 85.0, 50.0)
    status = pool.get_status("prod-1")
    assert status is not None
    assert status.available is False


# ---------------------------------------------------------------------------
# 11. Update login status
# ---------------------------------------------------------------------------
async def test_update_login_status(pool: CredentialPool) -> None:
    await pool.load()
    await pool.update_login_status("prod-1", "logged_in")
    status = pool.get_status("prod-1")
    assert status is not None
    assert status.login_status == "logged_in"
    assert status.last_login_at is not None
    assert status.error is None

    await pool.update_login_status("prod-2", "failed", error="auth timeout")
    status2 = pool.get_status("prod-2")
    assert status2 is not None
    assert status2.login_status == "failed"
    assert status2.error == "auth timeout"
    assert status2.available is False


# ---------------------------------------------------------------------------
# 12. Mark cooldown
# ---------------------------------------------------------------------------
async def test_mark_cooldown(pool: CredentialPool) -> None:
    await pool.load()
    await pool.mark_cooldown("prod-1")
    status = pool.get_status("prod-1")
    assert status is not None
    assert status.available is False
    assert status.backoff_until is not None
    assert status.backoff_until > _utcnow()


# ---------------------------------------------------------------------------
# 13. Check cooldown recovery
# ---------------------------------------------------------------------------
async def test_check_cooldown_recovery(pool: CredentialPool) -> None:
    await pool.load()

    # Set a backoff_until in the past
    status = pool.get_status("prod-1")
    assert status is not None
    status.backoff_until = _utcnow() - timedelta(minutes=5)
    status.available = False

    await pool.check_cooldown_recovery()

    status = pool.get_status("prod-1")
    assert status is not None
    assert status.backoff_until is None
    assert status.available is True


# ---------------------------------------------------------------------------
# 14. Pool summary stats
# ---------------------------------------------------------------------------
async def test_pool_summary(
    config: CredentialConfig, accounts_path: Path
) -> None:
    # Mix of groups
    accounts = _make_accounts(2, "high_quota") + [
        {
            "id": "std-1",
            "email": "std@test.com",
            "email_token": "sk-std",
            "group": "standard",
            "enabled": True,
        }
    ]
    _write_accounts(accounts_path, accounts, {
        "high_quota": {"description": "Primary"},
        "standard": {"description": "Secondary"},
    })
    pool = CredentialPool(config)
    await pool.load()

    # Allocate one, exhaust another
    await pool.allocate("worker-1", "production", "high_quota")
    await pool.update_quota("prod-2", 90.0, 50.0)

    summary = pool.pool_summary()
    assert summary["total"] == 3
    assert summary["assigned"] == 1
    assert summary["exhausted"] == 1
    assert "high_quota" in summary["groups"]
    assert "standard" in summary["groups"]
    assert summary["groups"]["high_quota"]["total"] == 2
    assert summary["groups"]["standard"]["total"] == 1


# ---------------------------------------------------------------------------
# 15. Concurrent allocate/release safety
# ---------------------------------------------------------------------------
async def test_concurrent_allocate_release(pool: CredentialPool) -> None:
    await pool.load()

    async def allocate_and_release(worker_id: str) -> None:
        acct = await pool.allocate(worker_id, "production", "high_quota")
        if acct is not None:
            await asyncio.sleep(0.01)
            await pool.release(acct.id)

    # Run many concurrent allocate/release operations
    tasks = [allocate_and_release(f"worker-{i}") for i in range(10)]
    await asyncio.gather(*tasks)

    # After all releases, no accounts should be assigned
    for i in range(1, 4):
        status = pool.get_status(f"prod-{i}")
        assert status is not None
        assert status.assigned_to is None


# ---------------------------------------------------------------------------
# 16. Save/reload pool_status.json round-trip
# ---------------------------------------------------------------------------
async def test_save_reload_roundtrip(
    config: CredentialConfig, accounts_path: Path, status_path: Path
) -> None:
    _write_accounts(accounts_path, _make_accounts(3, "high_quota"))
    pool1 = CredentialPool(config)
    await pool1.load()

    # Make some changes
    await pool1.allocate("worker-1", "production", "high_quota")
    await pool1.update_quota("prod-2", 60.0, 40.0)
    await pool1.update_login_status("prod-3", "logged_in")
    await pool1.save_status()

    # Verify file exists
    assert status_path.exists()

    # Reload in a new instance
    pool2 = CredentialPool(config)
    await pool2.load()

    # Verify state was persisted
    s1 = pool2.get_status("prod-1")
    assert s1 is not None
    assert s1.assigned_to == "worker-1"
    assert s1.slot_type == "production"

    s2 = pool2.get_status("prod-2")
    assert s2 is not None
    assert s2.five_hour.utilization == 60.0
    assert s2.seven_day.utilization == 40.0

    s3 = pool2.get_status("prod-3")
    assert s3 is not None
    assert s3.login_status == "logged_in"
    assert s3.last_login_at is not None


# ---------------------------------------------------------------------------
# Additional tests for completeness
# ---------------------------------------------------------------------------

async def test_allocate_wrong_group(pool: CredentialPool) -> None:
    """Allocate should return None when no accounts match the group."""
    await pool.load()
    acct = await pool.allocate("worker-1", "production", "nonexistent_group")
    assert acct is None


async def test_list_available(pool: CredentialPool) -> None:
    """list_available returns only accounts that pass availability checks."""
    await pool.load()
    available = pool.list_available("high_quota")
    assert len(available) == 3

    # Exhaust one
    await pool.update_quota("prod-1", 90.0, 50.0)
    available = pool.list_available("high_quota")
    assert len(available) == 2

    # Filter by group — nonexistent returns empty
    available = pool.list_available("nonexistent")
    assert len(available) == 0


async def test_list_available_no_group_filter(pool: CredentialPool) -> None:
    """list_available with no group returns all available accounts."""
    await pool.load()
    available = pool.list_available()
    assert len(available) == 3


async def test_get_status_nonexistent(pool: CredentialPool) -> None:
    """get_status returns None for unknown account."""
    await pool.load()
    assert pool.get_status("no-such-account") is None


async def test_release_nonexistent(pool: CredentialPool) -> None:
    """Release on nonexistent account should not raise."""
    await pool.load()
    await pool.release("no-such-account")  # should not raise


async def test_cooldown_with_reset_times(pool: CredentialPool) -> None:
    """mark_cooldown should use the earlier of the two reset times."""
    await pool.load()
    now = _utcnow()
    five_hour_reset = now + timedelta(hours=2)
    seven_day_reset = now + timedelta(hours=10)
    await pool.update_quota(
        "prod-1", 80.0, 70.0,
        five_hour_resets_at=five_hour_reset,
        seven_day_resets_at=seven_day_reset,
    )
    await pool.mark_cooldown("prod-1")
    status = pool.get_status("prod-1")
    assert status is not None
    assert status.backoff_until is not None
    # Should use the earlier reset time (five_hour)
    assert abs((status.backoff_until - five_hour_reset).total_seconds()) < 2


async def test_mark_backoff_until_does_not_shorten_existing_backoff(
    pool: CredentialPool,
) -> None:
    await pool.load()
    later_reset = _utcnow() + timedelta(hours=1)
    shorter_fallback = _utcnow() + timedelta(minutes=30)

    await pool.mark_backoff_until(
        "prod-1",
        later_reset,
        error="claude_rate_limited",
    )
    await pool.mark_backoff_until(
        "prod-1",
        shorter_fallback,
        error="claude_rate_limited",
    )

    status = pool.get_status("prod-1")
    assert status is not None
    assert status.available is False
    assert status.error == "claude_rate_limited"
    assert status.backoff_until == later_reset


async def test_atomic_write(pool: CredentialPool, status_path: Path) -> None:
    """Verify flush uses atomic rename (no leftover tmp file)."""
    await pool.load()
    await pool.allocate("worker-1", "production", "high_quota")
    await pool.save_status()
    assert status_path.exists()
    assert not status_path.with_suffix(".tmp").exists()


async def test_allocate_disabled_account(
    config: CredentialConfig, accounts_path: Path
) -> None:
    """Disabled accounts should not be allocated."""
    accounts = [
        {
            "id": "disabled-1",
            "email": "d@test.com",
            "email_token": "sk-d",
            "group": "high_quota",
            "enabled": False,
        }
    ]
    _write_accounts(accounts_path, accounts)
    pool = CredentialPool(config)
    await pool.load()

    acct = await pool.allocate("worker-1", "production", "high_quota")
    assert acct is None


# ===========================================================================
# T-132: CredentialPool extended — accounts.json loading, grouping,
#        allocation, reclaim, pool_status persistence
# ===========================================================================


class TestT132AccountsJsonLoading:
    """T-132: accounts.json loading and group handling."""

    async def test_load_multi_group_accounts(
        self, config: CredentialConfig, accounts_path: Path
    ) -> None:
        accounts = (
            _make_accounts(2, "high_quota")
            + [
                {"id": "std-1", "email": "s1@test.com", "email_token": "st1", "group": "standard", "enabled": True},
                {"id": "std-2", "email": "s2@test.com", "email_token": "st2", "group": "standard", "enabled": True},
                {"id": "low-1", "email": "l1@test.com", "email_token": "lt1", "group": "low_priority", "enabled": True},
            ]
        )
        groups = {
            "high_quota": {"description": "Primary"},
            "standard": {"description": "Secondary"},
            "low_priority": {"description": "Overflow"},
        }
        _write_accounts(accounts_path, accounts, groups)
        pool = CredentialPool(config)
        await pool.load()
        summary = pool.pool_summary()
        assert summary["total"] == 5
        assert summary["groups"]["high_quota"]["total"] == 2
        assert summary["groups"]["standard"]["total"] == 2
        assert summary["groups"]["low_priority"]["total"] == 1

    async def test_load_corrupted_accounts_json(
        self, config: CredentialConfig, accounts_path: Path
    ) -> None:
        accounts_path.write_text("{ invalid json !", encoding="utf-8")
        pool = CredentialPool(config)
        await pool.load()
        summary = pool.pool_summary()
        assert summary["total"] == 0

    async def test_load_missing_accounts_json(self, config: CredentialConfig) -> None:
        pool = CredentialPool(config)
        await pool.load()
        summary = pool.pool_summary()
        assert summary["total"] == 0

    async def test_load_empty_accounts_list(
        self, config: CredentialConfig, accounts_path: Path
    ) -> None:
        _write_accounts(accounts_path, [], {"standard": {"description": "Empty"}})
        pool = CredentialPool(config)
        await pool.load()
        assert pool.pool_summary()["total"] == 0

    async def test_load_accounts_disabled_mixed(
        self, config: CredentialConfig, accounts_path: Path
    ) -> None:
        accounts = [
            {"id": "e-1", "email": "e1@t.com", "email_token": "et1", "group": "standard", "enabled": True},
            {"id": "d-1", "email": "d1@t.com", "email_token": "dt1", "group": "standard", "enabled": False},
            {"id": "e-2", "email": "e2@t.com", "email_token": "et2", "group": "standard", "enabled": True},
        ]
        _write_accounts(accounts_path, accounts)
        pool = CredentialPool(config)
        await pool.load()
        available = pool.list_available("standard")
        assert len(available) == 2
        acct = await pool.allocate("w1", "production", "standard")
        assert acct is not None
        assert acct.id != "d-1"

    async def test_load_accounts_without_groups_key(
        self, config: CredentialConfig, accounts_path: Path
    ) -> None:
        data = {"accounts": _make_accounts(2, "standard")}
        accounts_path.write_text(json.dumps(data), encoding="utf-8")
        pool = CredentialPool(config)
        await pool.load()
        assert pool.pool_summary()["total"] == 2


class TestT132Grouping:
    """T-132: Group-based allocation and queries."""

    async def test_allocate_from_specific_group_only(
        self, config: CredentialConfig, accounts_path: Path
    ) -> None:
        accounts = _make_accounts(2, "high_quota") + [
            {"id": "std-1", "email": "s@t.com", "email_token": "s", "group": "standard", "enabled": True},
        ]
        _write_accounts(accounts_path, accounts)
        pool = CredentialPool(config)
        await pool.load()
        acct = await pool.allocate("w1", "production", "standard")
        assert acct is not None
        assert acct.id == "std-1"
        assert acct.group == "standard"

    async def test_group_summary_after_operations(
        self, config: CredentialConfig, accounts_path: Path
    ) -> None:
        accounts = _make_accounts(3, "high_quota") + [
            {"id": "std-1", "email": "s@t.com", "email_token": "s", "group": "standard", "enabled": True},
        ]
        _write_accounts(accounts_path, accounts, {
            "high_quota": {"description": "HQ"}, "standard": {"description": "STD"},
        })
        pool = CredentialPool(config)
        await pool.load()
        await pool.allocate("w1", "production", "high_quota")
        await pool.update_quota("prod-2", 90.0, 50.0)
        summary = pool.pool_summary()
        hq = summary["groups"]["high_quota"]
        assert hq["assigned"] == 1
        assert hq["exhausted"] == 1
        assert hq["available"] == 1
        std = summary["groups"]["standard"]
        assert std["available"] == 1
        assert std["assigned"] == 0

    async def test_list_available_across_groups(
        self, config: CredentialConfig, accounts_path: Path
    ) -> None:
        accounts = _make_accounts(2, "high_quota") + [
            {"id": "std-1", "email": "s@t.com", "email_token": "s", "group": "standard", "enabled": True},
        ]
        _write_accounts(accounts_path, accounts)
        pool = CredentialPool(config)
        await pool.load()
        assert len(pool.list_available()) == 3
        assert len(pool.list_available("high_quota")) == 2


class TestT132AllocationReclaim:
    """T-132: Allocation and reclaim edge cases."""

    async def test_release_and_reallocate_to_different_worker(
        self, pool: CredentialPool
    ) -> None:
        await pool.load()
        acct = await pool.allocate("worker-1", "production", "high_quota")
        assert acct is not None
        await pool.release(acct.id)
        acct2 = await pool.allocate("worker-2", "edit", "high_quota")
        assert acct2 is not None
        status = pool.get_status(acct2.id)
        assert status.assigned_to == "worker-2"
        assert status.slot_type == "edit"

    async def test_exhaust_then_release_one_and_reallocate(
        self, pool: CredentialPool
    ) -> None:
        await pool.load()
        ids = []
        for i in range(3):
            acct = await pool.allocate(f"w-{i}", "production", "high_quota")
            assert acct is not None
            ids.append(acct.id)
        assert await pool.allocate("w-new", "production", "high_quota") is None
        await pool.release(ids[1])
        realloc = await pool.allocate("w-new", "production", "high_quota")
        assert realloc is not None
        assert realloc.id == ids[1]

    async def test_release_worker_then_reallocate(
        self, pool: CredentialPool
    ) -> None:
        await pool.load()
        a1 = await pool.allocate("w1", "production", "high_quota")
        a2 = await pool.allocate("w1", "edit", "high_quota")
        assert a1 is not None and a2 is not None
        await pool.release_worker("w1")
        assert len(pool.list_available("high_quota")) == 3
        new = await pool.allocate("w2", "production", "high_quota")
        assert new is not None

    async def test_allocate_assigns_last_used(self, pool: CredentialPool) -> None:
        await pool.load()
        acct = await pool.allocate("w1", "production", "high_quota")
        assert acct is not None
        status = pool.get_status(acct.id)
        assert status.last_used is not None


class TestT132PoolStatusPersistence:
    """T-132: pool_status.json persistence edge cases."""

    async def test_multiple_save_reload_cycles(
        self, config: CredentialConfig, accounts_path: Path, status_path: Path
    ) -> None:
        _write_accounts(accounts_path, _make_accounts(3, "high_quota"))
        pool = CredentialPool(config)
        await pool.load()
        await pool.allocate("w1", "production", "high_quota")
        await pool.save_status()
        await pool.update_quota("prod-2", 70.0, 30.0)
        await pool.save_status()
        pool2 = CredentialPool(config)
        await pool2.load()
        assert pool2.get_status("prod-1").assigned_to == "w1"
        assert pool2.get_status("prod-2").five_hour.utilization == 70.0

    async def test_corrupted_pool_status_recovery(
        self, config: CredentialConfig, accounts_path: Path, status_path: Path
    ) -> None:
        _write_accounts(accounts_path, _make_accounts(2, "high_quota"))
        status_path.parent.mkdir(parents=True, exist_ok=True)
        status_path.write_text("{{bad json}}", encoding="utf-8")
        pool = CredentialPool(config)
        await pool.load()
        assert pool.pool_summary()["total"] == 2
        for i in range(1, 3):
            s = pool.get_status(f"prod-{i}")
            assert s is not None
            assert s.assigned_to is None

    async def test_quota_with_reset_times_persists(
        self, config: CredentialConfig, accounts_path: Path, status_path: Path
    ) -> None:
        _write_accounts(accounts_path, _make_accounts(2, "high_quota"))
        pool = CredentialPool(config)
        await pool.load()
        reset_time = _utcnow() + timedelta(hours=3)
        await pool.update_quota("prod-1", 75.0, 45.0,
                                five_hour_resets_at=reset_time,
                                seven_day_resets_at=_utcnow() + timedelta(days=5))
        await pool.save_status()
        pool2 = CredentialPool(config)
        await pool2.load()
        s = pool2.get_status("prod-1")
        assert s.five_hour.utilization == 75.0
        assert s.seven_day.utilization == 45.0
        assert s.five_hour.resets_at is not None

    async def test_new_account_added_after_reload(
        self, config: CredentialConfig, accounts_path: Path, status_path: Path
    ) -> None:
        _write_accounts(accounts_path, _make_accounts(2, "high_quota"))
        pool = CredentialPool(config)
        await pool.load()
        await pool.allocate("w1", "production", "high_quota")
        await pool.save_status()
        _write_accounts(accounts_path, _make_accounts(3, "high_quota"))
        pool2 = CredentialPool(config)
        await pool2.load()
        assert pool2.get_status("prod-1").assigned_to == "w1"
        s3 = pool2.get_status("prod-3")
        assert s3 is not None
        assert s3.assigned_to is None

    async def test_login_status_persists(
        self, config: CredentialConfig, accounts_path: Path, status_path: Path
    ) -> None:
        _write_accounts(accounts_path, _make_accounts(2, "high_quota"))
        pool = CredentialPool(config)
        await pool.load()
        await pool.update_login_status("prod-1", "logged_in")
        await pool.update_login_status("prod-2", "failed", error="timeout")
        await pool.save_status()
        pool2 = CredentialPool(config)
        await pool2.load()
        assert pool2.get_status("prod-1").login_status == "logged_in"
        assert pool2.get_status("prod-2").login_status == "failed"
        assert pool2.get_status("prod-2").error == "timeout"
