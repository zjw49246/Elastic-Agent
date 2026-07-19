"""AccountBindingStore — the durable account↔machine↔EIP 1:1 map.

Each codex/ChatGPT account is pinned to exactly one machine and one Elastic IP.
The machine is stopped/started (never terminated) so the account is always seen
from the same IP and — with the persistent EBS root carrying ``auth.json`` — the
same device. This store is the source of truth for "which box is this account's
home", surviving Manager restarts.

Deliberately separate from the account *identity* store (``AccountStore`` →
``accounts.json``): identities are edited by the frontend; bindings are runtime
infrastructure state owned by the orchestrator. Credentials are never stored
here — they live only on the worker box.
"""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path

from pydantic import BaseModel, Field


class BindingState:
    """Lifecycle of a bound box (string constants, stored verbatim)."""

    UNBOUND = "unbound"          # no machine yet
    PROVISIONING = "provisioning"  # first-time build (create+eip+bootstrap+login)
    STOPPED = "stopped"          # machine + EIP + login ready, powered off
    RUNNING = "running"          # started, executing a job
    WARM = "warm"                # job done, warm-hold grace before stop
    ERROR = "error"              # last op failed; needs attention


class AccountBinding(BaseModel):
    """One account's permanent machine + EIP assignment."""

    email: str
    account_id: str = ""
    instance_id: str | None = None       # namespaced provider id, e.g. aws:i-abc
    eip_allocation_id: str | None = None
    eip_ip: str | None = None
    region: str | None = None
    config_dir: str = ""                 # codex CODEX_HOME on the box
    state: str = BindingState.UNBOUND
    warm_until: float | None = None      # epoch; set while in WARM state
    error: str | None = None
    created_at: float = Field(default_factory=time.time)
    updated_at: float = Field(default_factory=time.time)
    last_used_at: float | None = None

    def touch(self) -> None:
        self.updated_at = time.time()


class BindingsConfig(BaseModel):
    """Root model for bindings.json."""

    bindings: list[AccountBinding] = Field(default_factory=list)


class AccountBindingStore:
    """JSON-backed CRUD over ``bindings.json``, keyed by account email."""

    def __init__(self, path: str) -> None:
        self._path = Path(path).expanduser()
        self._lock = asyncio.Lock()
        self._config = BindingsConfig()
        self._loaded = False

    async def load(self) -> None:
        async with self._lock:
            self._load_sync()

    def _load_sync(self) -> None:
        if self._path.exists():
            try:
                raw = json.loads(self._path.read_text(encoding="utf-8"))
                self._config = BindingsConfig.model_validate(raw)
            except Exception:
                self._config = BindingsConfig()
        else:
            self._config = BindingsConfig()
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

    def _find(self, email: str) -> AccountBinding | None:
        return next((b for b in self._config.bindings if b.email == email), None)

    # -- CRUD --------------------------------------------------------------

    async def list(self) -> list[AccountBinding]:
        async with self._lock:
            self._ensure_loaded()
            return [b.model_copy(deep=True) for b in self._config.bindings]

    async def get(self, email: str) -> AccountBinding | None:
        async with self._lock:
            self._ensure_loaded()
            b = self._find(email)
            return b.model_copy(deep=True) if b else None

    async def upsert(self, binding: AccountBinding) -> AccountBinding:
        """Insert or replace the binding for ``binding.email``."""
        async with self._lock:
            self._ensure_loaded()
            binding.touch()
            self._config.bindings = [
                b for b in self._config.bindings if b.email != binding.email
            ]
            self._config.bindings.append(binding)
            self._flush_sync()
            return binding.model_copy(deep=True)

    async def update(self, email: str, **fields) -> AccountBinding | None:
        """Patch selected fields of an existing binding (no-op if absent)."""
        async with self._lock:
            self._ensure_loaded()
            b = self._find(email)
            if b is None:
                return None
            for k, v in fields.items():
                setattr(b, k, v)
            b.touch()
            self._flush_sync()
            return b.model_copy(deep=True)

    async def remove(self, email: str) -> bool:
        async with self._lock:
            self._ensure_loaded()
            before = len(self._config.bindings)
            self._config.bindings = [
                b for b in self._config.bindings if b.email != email
            ]
            removed = len(self._config.bindings) < before
            if removed:
                self._flush_sync()
            return removed

    # -- Queries used by the orchestrator ---------------------------------

    async def by_state(self, *states: str) -> list[AccountBinding]:
        async with self._lock:
            self._ensure_loaded()
            wanted = set(states)
            return [
                b.model_copy(deep=True)
                for b in self._config.bindings
                if b.state in wanted
            ]

    async def get_by_instance(self, instance_id: str) -> AccountBinding | None:
        async with self._lock:
            self._ensure_loaded()
            b = next(
                (b for b in self._config.bindings if b.instance_id == instance_id),
                None,
            )
            return b.model_copy(deep=True) if b else None
