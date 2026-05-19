"""Auto-rotation — wait for task completion, allocate new account, login, restore slot.

T-045: When QuotaMonitor detects an exhausted account, the CredentialRotator:
1. Waits for the current task to complete (non-interrupting)
2. Finds a replacement account from the pool
3. Handles login (skip if already logged in, refresh if expired, full OAuth if needed)
4. Distributes new credentials to Worker
5. Updates pool status

T-046: Cooldown recovery is handled by QuotaMonitor's periodic loop calling
CredentialPool.check_cooldown_recovery(). When resets_at passes, accounts
are automatically marked available again.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable

from elastic_agent.core.credential_pool import AccountDefinition, AccountStatus, CredentialPool
from elastic_agent.core.credential_binding import CredentialBinding
from elastic_agent.core.event_bus import EventBus

logger = logging.getLogger(__name__)

SendCredentialCallback = Callable[[str, str, dict[str, str], str], Awaitable[bool]]


class CredentialRotator:
    """Manages credential rotation when accounts are exhausted.

    Coordinates between CredentialPool, CredentialBinding, and Worker communication
    to perform non-interrupting credential rotation.

    Usage:
        rotator = CredentialRotator(pool, binding, event_bus)
        rotator.send_credential = async_callback  # (worker_id, account_id, creds, config_dir) -> success
        rotator.execute_login = async_callback  # (worker_id, account_def) -> LoginResult
        await rotator.rotate(worker_id="w1", old_account_id="prod-1", reason="quota_exceeded")
    """

    def __init__(
        self,
        pool: CredentialPool,
        binding: CredentialBinding,
        event_bus: EventBus,
    ) -> None:
        self._pool = pool
        self._binding = binding
        self._bus = event_bus
        self._pending_rotations: dict[str, asyncio.Event] = {}
        self._rotation_lock = asyncio.Lock()

        self.send_credential: SendCredentialCallback | None = None
        self.execute_login: Callable[..., Awaitable[Any]] | None = None
        self.wait_for_task_completion: Callable[[str], Awaitable[None]] | None = None

    async def rotate(
        self,
        worker_id: str,
        old_account_id: str,
        reason: str = "quota_exceeded",
    ) -> RotationResult:
        """Execute credential rotation for a Worker slot.

        Non-interrupting: waits for current task to complete before switching.
        """
        async with self._rotation_lock:
            return await self._do_rotate(worker_id, old_account_id, reason)

    async def _do_rotate(
        self,
        worker_id: str,
        old_account_id: str,
        reason: str,
    ) -> RotationResult:
        """Internal rotation logic."""
        logger.info(
            "CredentialRotator: starting rotation for account %s on worker %s (reason: %s)",
            old_account_id, worker_id, reason,
        )

        old_status = self._pool.get_status(old_account_id)
        if old_status is None:
            return RotationResult(
                success=False,
                old_account_id=old_account_id,
                error="Account not found in pool",
            )

        slot_type = old_status.slot_type or "production"
        config_dir = old_status.config_dir or ""
        group = old_status.group

        # Step 1: Wait for current task to complete (non-interrupting)
        if self.wait_for_task_completion:
            try:
                logger.info(
                    "CredentialRotator: waiting for task completion on worker %s",
                    worker_id,
                )
                await self.wait_for_task_completion(worker_id)
            except Exception:
                logger.warning(
                    "CredentialRotator: wait_for_task_completion failed, proceeding anyway"
                )

        # Step 2: Find replacement account
        new_account = await self._pool.allocate(worker_id, slot_type, group)

        if new_account is None:
            # No replacement available — check for cooled-down accounts
            await self._pool.check_cooldown_recovery()
            new_account = await self._pool.allocate(worker_id, slot_type, group)

        if new_account is None:
            logger.warning(
                "CredentialRotator: no replacement account available for %s",
                old_account_id,
            )
            await self._bus.emit(
                "CREDENTIAL_EXHAUSTED",
                worker_id,
                {
                    "worker_id": worker_id,
                    "account_id": old_account_id,
                    "reason": "no_replacement_available",
                },
            )
            return RotationResult(
                success=False,
                old_account_id=old_account_id,
                error="No replacement account available",
            )

        # Step 3: Handle login for new account
        new_status = self._pool.get_status(new_account.id)
        login_needed = True

        if new_status and new_status.login_status == "logged_in":
            login_needed = False
        elif new_status and new_status.login_status in ("unknown", "login_failed"):
            login_needed = True

        if login_needed and self.execute_login:
            try:
                login_result = await self.execute_login(worker_id, new_account, config_dir)
                if not getattr(login_result, "success", False):
                    await self._pool.release(new_account.id)
                    return RotationResult(
                        success=False,
                        old_account_id=old_account_id,
                        new_account_id=new_account.id,
                        error=f"Login failed for new account: {getattr(login_result, 'error', 'unknown')}",
                    )
            except Exception as e:
                await self._pool.release(new_account.id)
                return RotationResult(
                    success=False,
                    old_account_id=old_account_id,
                    new_account_id=new_account.id,
                    error=f"Login error: {e}",
                )

        # Step 4: Distribute new credentials to Worker
        if self.send_credential:
            creds = {
                "account_id": new_account.id,
                "email": new_account.email,
                "email_token": new_account.email_token,
            }
            sent = await self.send_credential(worker_id, new_account.id, creds, config_dir)
            if not sent:
                await self._pool.release(new_account.id)
                return RotationResult(
                    success=False,
                    old_account_id=old_account_id,
                    new_account_id=new_account.id,
                    error="Failed to send credentials to Worker",
                )

        # Step 5: Update pool status
        # Release old account and mark cooldown
        await self._binding.unbind(old_account_id)
        await self._pool.release(old_account_id)
        await self._pool.mark_cooldown(old_account_id)

        # Bind new account
        await self._binding.bind(new_account.id, worker_id)

        # Emit rotation event
        await self._bus.emit(
            "CREDENTIAL_ROTATED",
            worker_id,
            {
                "node_id": worker_id,
                "old_id": old_account_id,
                "new_id": new_account.id,
                "reason": reason,
            },
        )

        logger.info(
            "CredentialRotator: rotated %s → %s on worker %s (reason: %s)",
            old_account_id, new_account.id, worker_id, reason,
        )

        return RotationResult(
            success=True,
            old_account_id=old_account_id,
            new_account_id=new_account.id,
        )

    async def handle_exhausted(
        self, worker_id: str, account_id: str, reason: str
    ) -> None:
        """Handle a CREDENTIAL_EXHAUSTED event (all accounts used up)."""
        logger.warning(
            "CredentialRotator: all accounts exhausted for worker %s (trigger: %s, reason: %s)",
            worker_id, account_id, reason,
        )
        await self._bus.emit(
            "CREDENTIAL_EXHAUSTED",
            worker_id,
            {
                "worker_id": worker_id,
                "account_id": account_id,
                "reason": reason,
            },
        )


class RotationResult:
    """Result of a credential rotation attempt."""

    __slots__ = ("success", "old_account_id", "new_account_id", "error")

    def __init__(
        self,
        success: bool,
        old_account_id: str,
        new_account_id: str | None = None,
        error: str | None = None,
    ):
        self.success = success
        self.old_account_id = old_account_id
        self.new_account_id = new_account_id
        self.error = error
