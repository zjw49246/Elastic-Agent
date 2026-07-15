"""AccountStore — the frontend-editable account pool.

A thin CRUD layer over ``accounts.json`` (the same file CredentialPool reads for
allocation). The frontend's Accounts panel adds/removes account *identities*
(email + 接码 token + group); credentials are never stored here — they are minted
on the worker at login time. Kept deliberately separate from CredentialPool,
which owns runtime allocation/quota state.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

from elastic_agent.core.credential_pool import AccountDefinition, AccountsConfig


class AccountStore:
    def __init__(self, path: str) -> None:
        self._path = Path(path).expanduser()
        self._lock = asyncio.Lock()
        self._config = AccountsConfig()
        self._loaded = False

    async def load(self) -> None:
        async with self._lock:
            self._load_sync()

    def _load_sync(self) -> None:
        if self._path.exists():
            try:
                raw = json.loads(self._path.read_text(encoding="utf-8"))
                self._config = AccountsConfig.model_validate(raw)
            except Exception:
                self._config = AccountsConfig()
        else:
            self._config = AccountsConfig()
        self._loaded = True

    def _ensure_loaded(self) -> None:
        if not self._loaded:
            self._load_sync()

    def _flush_sync(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        payload = json.loads(self._config.model_dump_json())
        tmp = self._path.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        tmp.replace(self._path)

    # -- CRUD --------------------------------------------------------------

    async def list(self) -> list[AccountDefinition]:
        async with self._lock:
            self._ensure_loaded()
            return list(self._config.accounts)

    async def get(self, account_id: str) -> AccountDefinition | None:
        async with self._lock:
            self._ensure_loaded()
            return next((a for a in self._config.accounts if a.id == account_id), None)

    async def add(self, account: AccountDefinition) -> AccountDefinition:
        """Add or replace an account by id."""
        async with self._lock:
            self._ensure_loaded()
            self._config.accounts = [a for a in self._config.accounts if a.id != account.id]
            self._config.accounts.append(account)
            self._flush_sync()
            return account

    async def remove(self, account_id: str) -> bool:
        async with self._lock:
            self._ensure_loaded()
            before = len(self._config.accounts)
            self._config.accounts = [a for a in self._config.accounts if a.id != account_id]
            removed = len(self._config.accounts) < before
            if removed:
                self._flush_sync()
            return removed
