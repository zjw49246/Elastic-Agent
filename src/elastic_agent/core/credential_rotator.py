"""Auto-rotation — wait for task completion, allocate new account, login, restore slot.

T-045: When QuotaMonitor detects an exhausted account, the CredentialRotator:
1. Blocks new task dispatch (on_rotation_started callback)
2. Waits for the current task to complete OR interrupts on timeout/rate-limit
3. Finds a replacement account from the pool
4. Handles login (skip if already logged in, refresh if expired, full OAuth if needed)
5. Distributes new credentials to Worker
6. Updates pool status
7. Resumes interrupted task with new account if needed
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Awaitable, Callable

from elastic_agent.core.credential_pool import AccountDefinition, CredentialPool
from elastic_agent.core.credential_binding import CredentialBinding
from elastic_agent.core.event_bus import EventBus

logger = logging.getLogger(__name__)

SendCredentialCallback = Callable[[str, str, dict[str, str], str], Awaitable[bool]]


class CredentialRotator:
    """Manages credential rotation when accounts are exhausted.

    Supports two rotation paths:
    - **Graceful (95%+)**: block new tasks → wait up to 30 min for current task →
      if task finishes: normal swap; if timeout or rate-limited during wait:
      interrupt task → swap → resume with new account.
    - **Immediate (rate_limited/100%)**: block new tasks → interrupt task →
      swap → resume with new account.
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
        self._rotation_lock = asyncio.Lock()
        self._interrupt_events: dict[str, asyncio.Event] = {}
        self._no_replacement_last_warn: dict[tuple[str, str, str], float] = {}
        self._no_replacement_warn_interval = 600.0

        self.send_credential: SendCredentialCallback | None = None
        self.execute_login: Callable[..., Awaitable[Any]] | None = None
        self.wait_for_task_completion: Callable[[str], Awaitable[bool]] | None = None
        self.on_rotation_started: Callable[[str], Awaitable[None]] | None = None
        self.interrupt_task: Callable[[str], Awaitable[dict | None]] | None = None
        self.resume_task: Callable[[str, dict], Awaitable[None]] | None = None

    def signal_rate_limited(self, worker_id: str) -> None:
        """Signal a waiting rotation to stop waiting and proceed immediately."""
        evt = self._interrupt_events.get(worker_id)
        if evt:
            evt.set()
            logger.info("CredentialRotator: rate-limit signal sent for worker %s", worker_id)

    async def rotate(
        self,
        worker_id: str,
        old_account_id: str,
        reason: str = "quota_exceeded",
    ) -> RotationResult:
        """Execute credential rotation for a Worker slot."""
        async with self._rotation_lock:
            return await self._do_rotate(worker_id, old_account_id, reason)

    async def _do_rotate(
        self,
        worker_id: str,
        old_account_id: str,
        reason: str,
    ) -> RotationResult:
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

        # Guard: skip if this account was already rotated out (e.g. by a prior call)
        if old_status.assigned_to != worker_id:
            logger.info(
                "CredentialRotator: account %s no longer assigned to %s, skipping",
                old_account_id, worker_id,
            )
            return RotationResult(
                success=True,
                old_account_id=old_account_id,
                error="Already rotated",
            )

        slot_type = old_status.slot_type or "production"
        config_dir = old_status.config_dir or ""
        group = old_status.group

        # Preflight: do not disturb a live task unless a replacement account
        # exists. Quota polling is advisory; Claude's own rate-limit event is
        # the source of truth for stopping a production run.
        new_account = await self._pool.allocate_replacement(
            old_account_id,
            worker_id,
            slot_type,
            group,
        )

        if new_account is None:
            await self._pool.check_cooldown_recovery()
            new_account = await self._pool.allocate_replacement(
                old_account_id,
                worker_id,
                slot_type,
                group,
            )

        if new_account is None:
            self._log_no_replacement(worker_id, old_account_id, reason)
            return RotationResult(
                success=False,
                old_account_id=old_account_id,
                error="No replacement account available; keeping current worker/task running",
            )

        # Step 1: Block new tasks immediately
        if self.on_rotation_started:
            await self.on_rotation_started(worker_id)

        # Step 2: Wait for task or interrupt
        task_info: dict | None = None
        needs_resume = False

        if reason == "rate_limited":
            # Immediate: don't wait, interrupt right away
            logger.info("CredentialRotator: rate-limited, interrupting task immediately on %s", worker_id)
            if self.interrupt_task:
                task_info = await self.interrupt_task(worker_id)
                needs_resume = task_info is not None
        else:
            # Graceful: wait up to 30 min, interruptible by rate-limit signal
            task_completed = await self._wait_or_interrupt(worker_id)
            if not task_completed:
                logger.info("CredentialRotator: wait expired/interrupted on %s, interrupting task", worker_id)
                if self.interrupt_task:
                    task_info = await self.interrupt_task(worker_id)
                    needs_resume = task_info is not None

        # Step 4: Login new account
        new_status = self._pool.get_status(new_account.id)
        login_needed = not (new_status and new_status.login_status == "logged_in")

        if login_needed and self.execute_login:
            try:
                login_result = await self.execute_login(worker_id, new_account, config_dir)
                if not getattr(login_result, "success", False):
                    await self._pool.release(new_account.id)
                    await self._cleanup_old_account(old_account_id, worker_id)
                    return RotationResult(
                        success=False,
                        old_account_id=old_account_id,
                        new_account_id=new_account.id,
                        error=f"Login failed for new account: {getattr(login_result, 'error', 'unknown')}",
                    )
            except Exception as e:
                await self._pool.release(new_account.id)
                await self._cleanup_old_account(old_account_id, worker_id)
                return RotationResult(
                    success=False,
                    old_account_id=old_account_id,
                    new_account_id=new_account.id,
                    error=f"Login error: {e}",
                )

        # Step 5: Send credential notification to Worker
        if self.send_credential:
            creds = {
                "account_id": new_account.id,
                "email": new_account.email,
                "email_token": new_account.email_token,
            }
            sent = await self.send_credential(worker_id, new_account.id, creds, config_dir)
            if not sent:
                await self._pool.release(new_account.id)
                await self._cleanup_old_account(old_account_id, worker_id)
                return RotationResult(
                    success=False,
                    old_account_id=old_account_id,
                    new_account_id=new_account.id,
                    error="Failed to send credentials to Worker",
                )

        # Step 6: Update pool status
        await self._binding.unbind(old_account_id)
        await self._pool.release(old_account_id)
        await self._pool.mark_cooldown(old_account_id)
        await self._binding.bind(new_account.id, worker_id)

        # Step 7: Emit rotation event
        await self._bus.emit(
            "CREDENTIAL_ROTATED",
            worker_id,
            {
                "node_id": worker_id,
                "old_id": old_account_id,
                "new_id": new_account.id,
                "reason": reason,
                "task_resumed": needs_resume,
            },
        )

        logger.info(
            "CredentialRotator: rotated %s → %s on worker %s (reason: %s)",
            old_account_id, new_account.id, worker_id, reason,
        )

        # Step 8: Resume interrupted task with new account
        if needs_resume and task_info and self.resume_task:
            logger.info(
                "CredentialRotator: resuming interrupted task on worker %s with new account %s",
                worker_id, new_account.id,
            )
            try:
                await self.resume_task(worker_id, task_info)
            except Exception:
                logger.exception("CredentialRotator: failed to resume task on worker %s", worker_id)

        return RotationResult(
            success=True,
            old_account_id=old_account_id,
            new_account_id=new_account.id,
        )

    def _log_no_replacement(self, worker_id: str, account_id: str, reason: str) -> None:
        """Throttle advisory no-replacement warnings.

        No replacement during quota polling should not pause dispatch or
        interrupt the current task; it is only an operator warning. Throttle it
        so the periodic checker does not flood logs with the same state.
        """
        key = (worker_id, account_id, reason)
        now = time.monotonic()
        last = self._no_replacement_last_warn.get(key, 0.0)
        if now - last < self._no_replacement_warn_interval:
            logger.debug(
                "CredentialRotator: no replacement for %s on %s still active; "
                "suppressing duplicate warning",
                account_id,
                worker_id,
            )
            return
        self._no_replacement_last_warn[key] = now
        logger.warning(
            "CredentialRotator: account %s on worker %s needs rotation "
            "(reason=%s), but no replacement is available; keeping the current "
            "task and dispatch state unchanged until Claude reports an actual "
            "rate limit or an account recovers",
            account_id,
            worker_id,
            reason,
        )

    async def _cleanup_old_account(self, old_account_id: str, worker_id: str) -> None:
        """Release and cooldown the old account when rotation fails."""
        try:
            await self._binding.unbind(old_account_id)
            await self._pool.release(old_account_id)
            await self._pool.mark_cooldown(old_account_id)
            logger.info(
                "CredentialRotator: cleaned up old account %s from worker %s after rotation failure",
                old_account_id, worker_id,
            )
        except Exception:
            logger.exception(
                "CredentialRotator: failed to clean up old account %s", old_account_id,
            )

    async def _wait_or_interrupt(self, worker_id: str) -> bool:
        """Wait for task completion, interruptible by rate-limit signal.

        Returns True if the task completed naturally, False if interrupted or timed out.
        """
        evt = asyncio.Event()
        self._interrupt_events[worker_id] = evt

        try:
            if self.wait_for_task_completion:
                wait_task = asyncio.create_task(self.wait_for_task_completion(worker_id))
                interrupt_task = asyncio.create_task(evt.wait())

                done, pending = await asyncio.wait(
                    [wait_task, interrupt_task],
                    return_when=asyncio.FIRST_COMPLETED,
                )

                for p in pending:
                    p.cancel()
                    try:
                        await p
                    except asyncio.CancelledError:
                        pass

                if wait_task in done:
                    try:
                        result = wait_task.result()
                    except Exception:
                        logger.exception(
                            "CredentialRotator: wait_for_task_completion failed for %s; "
                            "treating task as still running",
                            worker_id,
                        )
                        return False
                    return result if isinstance(result, bool) else True
                # Interrupted by rate-limit signal
                return False
            return True
        finally:
            self._interrupt_events.pop(worker_id, None)

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
