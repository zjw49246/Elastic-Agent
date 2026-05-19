"""Account-to-worker credential binding with mutual exclusion and IP affinity.

Manages which accounts are assigned to which workers, enforcing:
- Mutual exclusion: an account can only be bound to one worker at a time
- Max capacity: each worker has a configurable upper limit on concurrent accounts
- Affinity reuse: when re-assigning an account, prefer the worker it was last bound to
"""

from __future__ import annotations

import asyncio
from collections import defaultdict
from datetime import datetime, timezone


class CredentialBinding:
    """Manages account-worker assignment rules: mutual exclusion, affinity reuse, max accounts per worker."""

    def __init__(self, max_accounts_per_worker: int = 4):
        self._max_accounts = max_accounts_per_worker
        self._bindings: dict[str, str] = {}  # account_id -> worker_id
        self._worker_accounts: dict[str, set[str]] = defaultdict(set)  # worker_id -> set of account_ids
        self._affinity: dict[tuple[str, str], datetime] = {}  # (account_id, worker_id) -> last_used_at
        self._lock = asyncio.Lock()

    async def bind(self, account_id: str, worker_id: str) -> bool:
        """Bind account to worker. Returns False if mutual exclusion or max limit violated."""
        async with self._lock:
            # Mutual exclusion: account already bound to a different worker
            if account_id in self._bindings:
                if self._bindings[account_id] == worker_id:
                    return True  # Already bound to this worker, idempotent
                return False

            # Max capacity check
            if len(self._worker_accounts[worker_id]) >= self._max_accounts:
                return False

            self._bindings[account_id] = worker_id
            self._worker_accounts[worker_id].add(account_id)
            # Record affinity on successful bind
            self._affinity[(account_id, worker_id)] = datetime.now(timezone.utc)
            return True

    async def unbind(self, account_id: str) -> str | None:
        """Unbind account from worker. Returns worker_id it was bound to, or None."""
        async with self._lock:
            worker_id = self._bindings.pop(account_id, None)
            if worker_id is not None:
                self._worker_accounts[worker_id].discard(account_id)
                if not self._worker_accounts[worker_id]:
                    del self._worker_accounts[worker_id]
            return worker_id

    async def unbind_worker(self, worker_id: str) -> list[str]:
        """Unbind all accounts from a worker (e.g., when worker goes offline). Returns account IDs."""
        async with self._lock:
            accounts = list(self._worker_accounts.pop(worker_id, set()))
            for account_id in accounts:
                self._bindings.pop(account_id, None)
            return accounts

    def is_bound(self, account_id: str) -> bool:
        """Check if account is currently bound."""
        return account_id in self._bindings

    def get_worker(self, account_id: str) -> str | None:
        """Get worker an account is bound to."""
        return self._bindings.get(account_id)

    def get_accounts(self, worker_id: str) -> set[str]:
        """Get all accounts bound to a worker."""
        return set(self._worker_accounts.get(worker_id, set()))

    def worker_account_count(self, worker_id: str) -> int:
        """Count of accounts currently bound to a worker."""
        return len(self._worker_accounts.get(worker_id, set()))

    def can_bind(self, worker_id: str) -> bool:
        """Check if worker can accept more accounts."""
        return len(self._worker_accounts.get(worker_id, set())) < self._max_accounts

    async def record_affinity(self, account_id: str, worker_id: str) -> None:
        """Record affinity for future reuse preference."""
        async with self._lock:
            self._affinity[(account_id, worker_id)] = datetime.now(timezone.utc)

    def get_preferred_worker(self, account_id: str) -> str | None:
        """Get preferred worker based on affinity history (most recent binding)."""
        best_worker: str | None = None
        best_time: datetime | None = None
        for (aid, wid), ts in self._affinity.items():
            if aid == account_id:
                if best_time is None or ts > best_time:
                    best_time = ts
                    best_worker = wid
        return best_worker
