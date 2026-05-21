"""Credential login service — orchestrates auto-login on Workers after bootstrap.

Listens for BOOTSTRAP_COMPLETED events, allocates ONE account per Worker from
CredentialPool, then logs that account into all credential slots (session
isolation via separate CLAUDE_CONFIG_DIRs) using CredentialLoginStep.

Harness provides slot configuration via get_credential_slots().
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from elastic_agent.core.claude_oauth import ClaudeOAuthProvider, LoginResult
from elastic_agent.core.config import CredentialConfig
from elastic_agent.core.credential_login_step import CredentialLoginStep
from elastic_agent.core.credential_pool import CredentialPool
from elastic_agent.core.event_bus import EventBus

logger = logging.getLogger(__name__)


@dataclass
class CredentialSlot:
    """A credential session slot on a Worker."""

    slot_type: str
    config_dir: str


class CredentialLoginService:
    """Manages credential allocation and login execution on Workers.

    Each Worker gets ONE account from the pool. That account is logged into
    multiple session slots (each with its own CLAUDE_CONFIG_DIR) so the Worker
    can run concurrent tasks under the same account.
    """

    def __init__(
        self,
        credential_pool: CredentialPool,
        credential_config: CredentialConfig,
        event_bus: EventBus,
        slots: list[CredentialSlot] | None = None,
        oauth_provider: ClaudeOAuthProvider | None = None,
    ) -> None:
        self._pool = credential_pool
        self._config = credential_config
        self._event_bus = event_bus
        self._slots = slots or []
        self._login_step = CredentialLoginStep(
            pool=credential_pool,
            oauth_provider=oauth_provider,
            login_timeout=credential_config.login_timeout,
        )

    def set_slots(self, slots: list[CredentialSlot]) -> None:
        self._slots = slots

    def register_event_handlers(self) -> None:
        self._event_bus.subscribe("BOOTSTRAP_COMPLETED", self._on_bootstrap_completed)
        self._event_bus.subscribe("WORKER_DISCONNECTED", self._on_worker_disconnected)

    async def login_worker(self, worker_id: str) -> list[LoginResult]:
        """Allocate one account and log it into all session slots on a Worker."""
        if not self._slots:
            logger.warning("No credential slots configured — skipping login for %s", worker_id)
            return []

        account = await self._pool.allocate(worker_id, "production", "standard")
        if account is None:
            logger.warning("No available account for worker %s", worker_id)
            return []

        logger.info("Allocated account %s to worker %s", account.id, worker_id)

        accounts_for_login = [
            (account, slot.config_dir, slot.slot_type)
            for slot in self._slots
        ]

        results = await self._login_step.execute(
            worker_id=worker_id,
            accounts=accounts_for_login,
        )

        success_count = sum(1 for r in results if r.success)
        logger.info(
            "Credential login for worker %s: %d/%d slots succeeded",
            worker_id, success_count, len(self._slots),
        )

        return results

    async def release_worker(self, worker_id: str) -> None:
        """Release all accounts allocated to a Worker."""
        await self._pool.release_worker(worker_id)
        logger.info("Released all credentials for worker %s", worker_id)

    async def _on_bootstrap_completed(
        self, event_type: str, worker_id: str, data: dict[str, Any]
    ) -> None:
        logger.info("Bootstrap completed for worker %s — starting credential login", worker_id)
        try:
            await self.login_worker(worker_id)
        except Exception:
            logger.exception("Credential login failed for worker %s", worker_id)

    async def _on_worker_disconnected(
        self, event_type: str, worker_id: str, data: dict[str, Any]
    ) -> None:
        await self.release_worker(worker_id)
