"""CredentialPool — account pool management with JSON persistence and async locking."""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

from elastic_agent.core.config import CredentialConfig

logger = logging.getLogger(__name__)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Pydantic models — accounts.json
# ---------------------------------------------------------------------------


class AccountDefinition(BaseModel):
    """Single account entry from accounts.json."""

    id: str
    email: str
    email_token: str = ""
    group: str = "standard"
    enabled: bool = True


class GroupDefinition(BaseModel):
    """Group metadata from accounts.json."""

    description: str = ""


class AccountsConfig(BaseModel):
    """Root model for accounts.json."""

    accounts: list[AccountDefinition] = Field(default_factory=list)
    groups: dict[str, GroupDefinition] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# Pydantic models — pool_status.json (runtime state)
# ---------------------------------------------------------------------------


class QuotaWindow(BaseModel):
    """Utilization tracking for a single quota window."""

    utilization: float = 0.0
    resets_at: datetime | None = None


class AccountStatus(BaseModel):
    """Runtime state of a single account in the pool."""

    email: str = ""
    group: str = "standard"
    assigned_to: str | None = None
    slot_type: str | None = None
    config_dir: str | None = None
    five_hour: QuotaWindow = Field(default_factory=QuotaWindow)
    seven_day: QuotaWindow = Field(default_factory=QuotaWindow)
    available: bool = True
    login_status: str = "unknown"
    stale: bool = False
    error: str | None = None
    backoff_until: datetime | None = None
    last_login_at: datetime | None = None
    last_quota_check: datetime | None = None
    last_used: datetime | None = None
    last_assigned_to: str | None = None


class PoolStatus(BaseModel):
    """Root model for pool_status.json."""

    last_updated: datetime = Field(default_factory=_utcnow)
    accounts: dict[str, AccountStatus] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# CredentialPool — core manager
# ---------------------------------------------------------------------------


class CredentialPool:
    """Account pool management with async locking and atomic JSON persistence.

    All mutations are protected by an asyncio.Lock and flushed to disk
    atomically (write to tmp file then rename) so crash recovery works.
    """

    def __init__(self, config: CredentialConfig) -> None:
        self._config = config
        self._accounts_path = Path(config.accounts_file).expanduser()
        self._status_path = Path(config.pool_status_file).expanduser()
        self._lock = asyncio.Lock()
        self._accounts_config: AccountsConfig = AccountsConfig()
        self._pool_status: PoolStatus = PoolStatus()
        self._loaded = False

    # -- persistence ---------------------------------------------------------

    async def load(self) -> None:
        """Load accounts.json and pool_status.json (if exists)."""
        async with self._lock:
            self._load_sync()

    def _load_sync(self) -> None:
        # Load account definitions
        if self._accounts_path.exists():
            try:
                raw = json.loads(self._accounts_path.read_text(encoding="utf-8"))
                self._accounts_config = AccountsConfig.model_validate(raw)
                logger.info(
                    "CredentialPool loaded %d accounts from %s",
                    len(self._accounts_config.accounts),
                    self._accounts_path,
                )
            except Exception:
                logger.exception(
                    "Failed to load accounts from %s, starting with empty config",
                    self._accounts_path,
                )
                self._accounts_config = AccountsConfig()
        else:
            logger.warning("Accounts file not found: %s", self._accounts_path)
            self._accounts_config = AccountsConfig()

        # Load pool status (or initialise from account definitions)
        if self._status_path.exists():
            try:
                raw = json.loads(self._status_path.read_text(encoding="utf-8"))
                self._pool_status = PoolStatus.model_validate(raw)
                logger.info(
                    "CredentialPool loaded pool_status with %d accounts from %s",
                    len(self._pool_status.accounts),
                    self._status_path,
                )
            except Exception:
                logger.exception(
                    "Failed to load pool_status from %s, initialising from accounts",
                    self._status_path,
                )
                self._init_pool_status()
        else:
            self._init_pool_status()

        # Ensure every account definition has a status entry
        for acct in self._accounts_config.accounts:
            if acct.id not in self._pool_status.accounts:
                self._pool_status.accounts[acct.id] = AccountStatus(
                    email=acct.email,
                    group=acct.group,
                )

        self._loaded = True

    def _init_pool_status(self) -> None:
        """Build initial pool status from account definitions."""
        accounts: dict[str, AccountStatus] = {}
        for acct in self._accounts_config.accounts:
            accounts[acct.id] = AccountStatus(
                email=acct.email,
                group=acct.group,
            )
        self._pool_status = PoolStatus(accounts=accounts)

    async def save_status(self) -> None:
        """Write pool_status.json atomically (tmp + rename)."""
        async with self._lock:
            self._flush_sync()

    def _flush_sync(self) -> None:
        self._pool_status.last_updated = _utcnow()
        self._status_path.parent.mkdir(parents=True, exist_ok=True)
        payload = json.loads(self._pool_status.model_dump_json())
        tmp = self._status_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
        tmp.replace(self._status_path)

    def _ensure_loaded(self) -> None:
        if not self._loaded:
            self._load_sync()

    # -- helpers -------------------------------------------------------------

    def _account_def(self, account_id: str) -> AccountDefinition | None:
        for acct in self._accounts_config.accounts:
            if acct.id == account_id:
                return acct
        return None

    def _is_available(self, acct_def: AccountDefinition, status: AccountStatus) -> bool:
        """Determine whether an account is available for allocation.

        This computes availability from underlying fields (enabled, assigned_to,
        login_status, backoff, quota). It does NOT check status.available itself
        to avoid circular dependency — status.available is the *output* of this
        method, not an input.
        """
        if not acct_def.enabled:
            return False
        if status.assigned_to is not None:
            return False
        if status.login_status in {"logging_in", "failed", "login_failed"}:
            return False
        if status.backoff_until is not None and _utcnow() < status.backoff_until:
            return False
        # utilization is 0-100 (percentage), threshold is 0-1 (fraction)
        threshold_pct = self._config.quota_threshold * 100
        if status.five_hour.utilization >= threshold_pct:
            return False
        if status.seven_day.utilization >= threshold_pct:
            return False
        return True

    def _count_assigned_to_worker(self, worker_id: str) -> int:
        return sum(
            1 for s in self._pool_status.accounts.values() if s.assigned_to == worker_id
        )

    # -- allocation ----------------------------------------------------------

    async def allocate(
        self, worker_id: str, slot_type: str, group: str
    ) -> AccountDefinition | None:
        """Allocate a free account to a worker.

        Filters by group, enabled, available (login OK, quota below threshold,
        not assigned, not in backoff). Sorts by least-used-first
        (five_hour.utilization ascending). Checks max_accounts_per_worker.
        """
        async with self._lock:
            self._ensure_loaded()

            # Check worker limit
            if self._count_assigned_to_worker(worker_id) >= self._config.max_accounts_per_worker:
                return None

            # Build candidate list
            candidates: list[tuple[AccountDefinition, AccountStatus]] = []
            for acct_def in self._accounts_config.accounts:
                if acct_def.group != group:
                    continue
                status = self._pool_status.accounts.get(acct_def.id)
                if status is None:
                    continue
                if self._is_available(acct_def, status):
                    candidates.append((acct_def, status))

            if not candidates:
                return None

            # Sort by least used first
            candidates.sort(key=lambda pair: pair[1].five_hour.utilization)

            chosen_def, chosen_status = candidates[0]
            chosen_status.assigned_to = worker_id
            chosen_status.slot_type = slot_type
            chosen_status.last_used = _utcnow()
            chosen_status.available = False

            self._flush_sync()
            logger.info(
                "CredentialPool: allocated %s to worker %s (slot=%s)",
                chosen_def.id,
                worker_id,
                slot_type,
            )
            return chosen_def

    async def allocate_replacement(
        self,
        old_account_id: str,
        worker_id: str,
        slot_type: str,
        group: str,
    ) -> AccountDefinition | None:
        """Allocate a replacement account for an already-assigned worker slot.

        Rotation is a handoff: for a short period the old account is still
        bound to the worker while the replacement is reserved and logged in.
        Count the old account as the slot being replaced so
        max_accounts_per_worker=1 still allows one replacement candidate.
        """
        async with self._lock:
            self._ensure_loaded()

            assigned_count = self._count_assigned_to_worker(worker_id)
            old_status = self._pool_status.accounts.get(old_account_id)
            if old_status and old_status.assigned_to == worker_id:
                assigned_count -= 1
            if assigned_count >= self._config.max_accounts_per_worker:
                return None

            candidates: list[tuple[AccountDefinition, AccountStatus]] = []
            for acct_def in self._accounts_config.accounts:
                if acct_def.id == old_account_id:
                    continue
                if acct_def.group != group:
                    continue
                status = self._pool_status.accounts.get(acct_def.id)
                if status is None:
                    continue
                if self._is_available(acct_def, status):
                    candidates.append((acct_def, status))

            if not candidates:
                return None

            candidates.sort(key=lambda pair: pair[1].five_hour.utilization)
            chosen_def, chosen_status = candidates[0]
            chosen_status.assigned_to = worker_id
            chosen_status.slot_type = slot_type
            chosen_status.last_used = _utcnow()
            chosen_status.available = False

            self._flush_sync()
            logger.info(
                "CredentialPool: allocated replacement %s for %s on worker %s (slot=%s)",
                chosen_def.id,
                old_account_id,
                worker_id,
                slot_type,
            )
            return chosen_def

    async def allocate_specific(
        self, account_id: str, worker_id: str, slot_type: str
    ) -> AccountDefinition | None:
        """Allocate a specific account to a worker (used for affinity reuse).

        Returns the AccountDefinition if the account is available and allocation
        succeeds, or None if the account is unavailable (assigned elsewhere,
        disabled, in backoff, or quota-exceeded).
        """
        async with self._lock:
            self._ensure_loaded()

            if self._count_assigned_to_worker(worker_id) >= self._config.max_accounts_per_worker:
                return None

            acct_def = self._account_def(account_id)
            if acct_def is None:
                return None

            status = self._pool_status.accounts.get(account_id)
            if status is None:
                return None

            if not self._is_available(acct_def, status):
                return None

            status.assigned_to = worker_id
            status.slot_type = slot_type
            status.last_used = _utcnow()
            status.available = False

            self._flush_sync()
            logger.info(
                "CredentialPool: allocated_specific %s to worker %s (slot=%s)",
                account_id,
                worker_id,
                slot_type,
            )
            return acct_def

    # -- release -------------------------------------------------------------

    async def release(self, account_id: str) -> None:
        """Release a single account (unassign). Saves last_assigned_to for affinity."""
        async with self._lock:
            self._ensure_loaded()
            status = self._pool_status.accounts.get(account_id)
            if status is None:
                return
            if status.assigned_to:
                status.last_assigned_to = status.assigned_to
            status.assigned_to = None
            status.slot_type = None
            status.config_dir = None
            # Recalculate availability
            acct_def = self._account_def(account_id)
            if acct_def is not None:
                status.available = self._is_available(acct_def, status)
            else:
                status.available = True
            self._flush_sync()
            logger.info("CredentialPool: released account %s", account_id)

    async def release_worker(self, worker_id: str) -> None:
        """Release all accounts assigned to a worker."""
        async with self._lock:
            self._ensure_loaded()
            released = []
            for acct_id, status in self._pool_status.accounts.items():
                if status.assigned_to == worker_id:
                    status.last_assigned_to = worker_id
                    status.assigned_to = None
                    status.slot_type = None
                    status.config_dir = None
                    acct_def = self._account_def(acct_id)
                    if acct_def is not None:
                        status.available = self._is_available(acct_def, status)
                    else:
                        status.available = True
                    released.append(acct_id)
            if released:
                self._flush_sync()
                logger.info(
                    "CredentialPool: released %d accounts from worker %s: %s",
                    len(released),
                    worker_id,
                    released,
                )

    # -- quota updates -------------------------------------------------------

    async def update_quota(
        self,
        account_id: str,
        five_hour_pct: float,
        seven_day_pct: float,
        five_hour_resets_at: datetime | None = None,
        seven_day_resets_at: datetime | None = None,
    ) -> None:
        """Update quota stats and recalculate availability."""
        async with self._lock:
            self._ensure_loaded()
            status = self._pool_status.accounts.get(account_id)
            if status is None:
                return
            status.five_hour.utilization = five_hour_pct
            status.five_hour.resets_at = five_hour_resets_at
            status.seven_day.utilization = seven_day_pct
            status.seven_day.resets_at = seven_day_resets_at
            status.last_quota_check = _utcnow()

            # Recalculate availability (only if not assigned)
            if status.assigned_to is None:
                acct_def = self._account_def(account_id)
                if acct_def is not None:
                    status.available = self._is_available(acct_def, status)

            self._flush_sync()

    # -- login status --------------------------------------------------------

    async def update_login_status(
        self, account_id: str, status: str, error: str | None = None
    ) -> None:
        """Update the login_status field of an account."""
        async with self._lock:
            self._ensure_loaded()
            acct_status = self._pool_status.accounts.get(account_id)
            if acct_status is None:
                return
            acct_status.login_status = status
            acct_status.error = error
            if status == "logged_in":
                acct_status.last_login_at = _utcnow()

            # Recalculate availability if not assigned
            if acct_status.assigned_to is None:
                acct_def = self._account_def(account_id)
                if acct_def is not None:
                    acct_status.available = self._is_available(acct_def, acct_status)

            self._flush_sync()

    # -- cooldown management -------------------------------------------------

    async def mark_cooldown(self, account_id: str) -> None:
        """Mark an account as unavailable (cooldown after rotation).

        Sets backoff_until to the earliest of five_hour or seven_day reset times.
        If neither is set, defaults to 5 hours from now.
        """
        async with self._lock:
            self._ensure_loaded()
            status = self._pool_status.accounts.get(account_id)
            if status is None:
                return

            # Determine backoff time from reset windows
            now = _utcnow()
            candidates: list[datetime] = []
            if status.five_hour.resets_at is not None:
                candidates.append(status.five_hour.resets_at)
            if status.seven_day.resets_at is not None:
                candidates.append(status.seven_day.resets_at)

            if candidates:
                status.backoff_until = min(candidates)
            else:
                status.backoff_until = now + timedelta(hours=5)

            status.available = False
            self._flush_sync()
            logger.info(
                "CredentialPool: account %s in cooldown until %s",
                account_id,
                status.backoff_until,
            )

    async def mark_backoff_until(
        self,
        account_id: str,
        backoff_until: datetime,
        error: str | None = None,
    ) -> None:
        """Mark an account unavailable until a known reset time."""
        async with self._lock:
            self._ensure_loaded()
            status = self._pool_status.accounts.get(account_id)
            if status is None:
                return
            status.backoff_until = backoff_until
            status.available = False
            if error:
                status.error = error
            self._flush_sync()
            logger.info(
                "CredentialPool: account %s in explicit backoff until %s",
                account_id,
                backoff_until,
            )

    async def check_cooldown_recovery(self) -> None:
        """Check all accounts in cooldown; mark available if reset time has passed."""
        async with self._lock:
            self._ensure_loaded()
            now = _utcnow()
            recovered = []
            for acct_id, status in self._pool_status.accounts.items():
                if status.backoff_until is not None and now >= status.backoff_until:
                    status.backoff_until = None
                    if status.login_status in ("failed", "login_failed"):
                        status.login_status = "unknown"
                        status.error = None
                    acct_def = self._account_def(acct_id)
                    if acct_def is not None and status.assigned_to is None:
                        status.available = self._is_available(acct_def, status)
                    recovered.append(acct_id)
            if recovered:
                self._flush_sync()
                logger.info("CredentialPool: recovered from cooldown: %s", recovered)

    # -- read-only queries ---------------------------------------------------

    def get_status(self, account_id: str) -> AccountStatus | None:
        """Get current status for a single account (non-async, read-only)."""
        return self._pool_status.accounts.get(account_id)

    def list_available(self, group: str | None = None) -> list[AccountStatus]:
        """List all available accounts, optionally filtered by group."""
        result: list[AccountStatus] = []
        for acct_id, status in self._pool_status.accounts.items():
            if not status.available:
                continue
            if group is not None and status.group != group:
                continue
            acct_def = self._account_def(acct_id)
            if acct_def is not None and self._is_available(acct_def, status):
                result.append(status)
        return result

    def list_assigned_to(self, worker_id: str) -> list[AccountStatus]:
        """List all accounts assigned to a specific worker."""
        return [
            status
            for status in self._pool_status.accounts.values()
            if status.assigned_to == worker_id
        ]

    def list_assigned_account_ids(self, worker_id: str) -> list[str]:
        """List account ids currently assigned to a specific worker."""
        return [
            account_id
            for account_id, status in self._pool_status.accounts.items()
            if status.assigned_to == worker_id
        ]

    def get_idle_accounts(self, group: str | None = None) -> list[tuple[str, AccountStatus]]:
        """Return idle accounts with their IDs, optionally filtered by group.

        Each entry is (account_id, AccountStatus) where assigned_to is None.
        Useful for auto-scaling: check last_assigned_to for worker affinity.
        """
        result: list[tuple[str, AccountStatus]] = []
        for acct_def in self._accounts_config.accounts:
            if group is not None and acct_def.group != group:
                continue
            status = self._pool_status.accounts.get(acct_def.id)
            if status is None:
                continue
            if self._is_available(acct_def, status):
                result.append((acct_def.id, status))
        return result

    def pool_summary(self) -> dict[str, Any]:
        """Summary stats: total, available, assigned, exhausted per group."""
        groups: dict[str, dict[str, int]] = {}
        for acct_def in self._accounts_config.accounts:
            g = acct_def.group
            if g not in groups:
                groups[g] = {"total": 0, "available": 0, "assigned": 0, "exhausted": 0}
            groups[g]["total"] += 1

            status = self._pool_status.accounts.get(acct_def.id)
            if status is None:
                continue

            if status.assigned_to is not None:
                groups[g]["assigned"] += 1
            elif self._is_available(acct_def, status):
                groups[g]["available"] += 1

            threshold_pct = self._config.quota_threshold * 100
            if (
                status.five_hour.utilization >= threshold_pct
                or status.seven_day.utilization >= threshold_pct
            ):
                groups[g]["exhausted"] += 1

        total_all = sum(v["total"] for v in groups.values())
        available_all = sum(v["available"] for v in groups.values())
        assigned_all = sum(v["assigned"] for v in groups.values())
        exhausted_all = sum(v["exhausted"] for v in groups.values())

        return {
            "total": total_all,
            "available": available_all,
            "assigned": assigned_all,
            "exhausted": exhausted_all,
            "groups": groups,
        }
