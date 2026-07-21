"""AccountStore — the frontend-editable account pool.

A thin CRUD layer over ``accounts.json`` (the same file CredentialPool reads for
allocation). The frontend's Accounts panel adds/removes account *identities*
(agent type + email + login secrets + group). Passwords and mailbox tokens are
stored write-only in a mode-0600 file and sent to the selected worker over a
protected transport; OAuth access/refresh credentials are minted only on that
worker and are never stored here. Kept deliberately separate from
CredentialPool, which owns runtime allocation/quota state.
"""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path

from elastic_agent.core.credential_pool import AccountDefinition, AccountsConfig


class AccountStoreCorruptError(RuntimeError):
    """The identity/token source file is invalid and must be repaired."""


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
                # The file contains passwords and mailbox authorization tokens.
                # Tighten legacy permissions before reading; refusing to run is
                # safer than continuing with a world-readable credential source.
                os.chmod(self._path, 0o600)
                raw = json.loads(self._path.read_text(encoding="utf-8"))
                self._config = AccountsConfig.model_validate(raw)
            except Exception as exc:
                self._loaded = False
                # Resetting to empty would make the next UI edit overwrite all
                # valid accounts/tokens because one legacy row was malformed.
                raise AccountStoreCorruptError(
                    f"account store is corrupt: {self._path}"
                ) from exc
        else:
            self._config = AccountsConfig()
        self._loaded = True

    def _ensure_loaded(self) -> None:
        if not self._loaded:
            self._load_sync()

    def _flush_sync(self, config: AccountsConfig) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        # Revalidate the whole candidate before touching the only copy of
        # account identities/tokens, then publish in memory only after the
        # atomic durable replace succeeds.
        config = AccountsConfig.model_validate(config.model_dump())
        payload = json.loads(config.model_dump_json())
        tmp = self._path.with_suffix(self._path.suffix + ".tmp")
        try:
            with tmp.open("w", encoding="utf-8") as stream:
                os.fchmod(stream.fileno(), 0o600)
                stream.write(json.dumps(payload, indent=2))
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(tmp, self._path)
            if hasattr(os, "O_DIRECTORY"):
                directory_fd = os.open(
                    self._path.parent, os.O_RDONLY | os.O_DIRECTORY
                )
                try:
                    os.fsync(directory_fd)
                finally:
                    os.close(directory_fd)
        except Exception:
            try:
                tmp.unlink(missing_ok=True)
            except OSError:
                pass
            raise
        self._config = config

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
            collision = next(
                (
                    existing
                    for existing in self._config.accounts
                    if existing.id != account.id
                    and existing.agent_type == account.agent_type
                    and existing.email.casefold() == account.email.casefold()
                ),
                None,
            )
            if collision is not None:
                raise ValueError(
                    f"{account.agent_type} email {account.email!r} is already "
                    f"account {collision.id!r}"
                )
            candidate = self._config.model_copy(deep=True)
            candidate.accounts = [
                existing
                for existing in candidate.accounts
                if existing.id != account.id
            ]
            candidate.accounts.append(account.model_copy(deep=True))
            self._flush_sync(candidate)
            return account

    async def remove(self, account_id: str) -> bool:
        async with self._lock:
            self._ensure_loaded()
            before = len(self._config.accounts)
            candidate = self._config.model_copy(deep=True)
            candidate.accounts = [
                account
                for account in candidate.accounts
                if account.id != account_id
            ]
            removed = len(candidate.accounts) < before
            if removed:
                self._flush_sync(candidate)
            return removed
