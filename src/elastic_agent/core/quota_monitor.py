"""Manager-side QuotaMonitor — aggregate quota data, threshold detection, events.

T-044: Collects QUOTA_STATUS events from all Workers, updates pool_status.json,
detects 5h window threshold breaches, manages 7d budget, and emits QUOTA_WARNING events.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable

from elastic_agent.core.config import CredentialConfig
from elastic_agent.core.credential_pool import CredentialPool
from elastic_agent.core.event_bus import EventBus

logger = logging.getLogger(__name__)

RotationCallback = Callable[[str, str, str], Awaitable[None]]


class QuotaMonitor:
    """Manager-side quota aggregation and threshold detection.

    Subscribes to QUOTA_STATUS events from the EventBus, updates the CredentialPool,
    and triggers rotation or warning events when thresholds are breached.

    Usage:
        monitor = QuotaMonitor(pool, event_bus, config)
        monitor.on_rotation_needed = async_callback  # (worker_id, account_id, reason)
        await monitor.start()
        # ... later ...
        await monitor.stop()
    """

    def __init__(
        self,
        pool: CredentialPool,
        event_bus: EventBus,
        config: CredentialConfig | None = None,
    ) -> None:
        self._pool = pool
        self._bus = event_bus
        self._config = config or CredentialConfig()
        self._sub_id: str | None = None
        self._cooldown_task: asyncio.Task | None = None
        self._running = False

        self.on_rotation_needed: RotationCallback | None = None
        self.on_relogin_needed: Callable[[str, str], Awaitable[None]] | None = None

    async def start(self) -> None:
        """Subscribe to QUOTA_STATUS events and start cooldown recovery loop."""
        self._sub_id = self._bus.subscribe("QUOTA_STATUS", self._handle_quota_status)
        self._running = True
        self._cooldown_task = asyncio.create_task(self._cooldown_loop())
        logger.info("QuotaMonitor: started")

    async def stop(self) -> None:
        """Stop monitoring and unsubscribe."""
        self._running = False
        if self._sub_id:
            self._bus.unsubscribe(self._sub_id)
            self._sub_id = None
        if self._cooldown_task:
            self._cooldown_task.cancel()
            try:
                await self._cooldown_task
            except asyncio.CancelledError:
                pass
            self._cooldown_task = None
        logger.info("QuotaMonitor: stopped")

    async def _handle_quota_status(
        self, event_type: str, worker_id: str, data: dict[str, Any]
    ) -> None:
        """Process a QUOTA_STATUS event from a Worker."""
        account_id = data.get("account_id", "")
        five_hour_pct = data.get("five_hour_pct", 0.0)
        seven_day_pct = data.get("seven_day_pct", 0.0)
        five_hour_resets_at = self._parse_datetime(data.get("five_hour_resets_at"))
        seven_day_resets_at = self._parse_datetime(data.get("seven_day_resets_at"))
        needs_relogin = data.get("needs_relogin", False)

        # 1. Update pool_status.json
        await self._pool.update_quota(
            account_id,
            five_hour_pct=five_hour_pct,
            seven_day_pct=seven_day_pct,
            five_hour_resets_at=five_hour_resets_at,
            seven_day_resets_at=seven_day_resets_at,
        )

        # Handle re-login requests
        if needs_relogin:
            logger.warning(
                "QuotaMonitor: account %s on worker %s needs re-login",
                account_id, worker_id,
            )
            if self.on_relogin_needed:
                await self.on_relogin_needed(worker_id, account_id)
            return

        # 2. 5h window threshold detection
        threshold_pct = self._config.quota_threshold * 100

        if five_hour_pct >= 95 or data.get("error") == "rate_limited":
            # Critical: trigger rotation
            logger.warning(
                "QuotaMonitor: account %s quota critical (%.1f%%) — triggering rotation",
                account_id, five_hour_pct,
            )
            await self._emit_warning(worker_id, account_id, five_hour_pct, "quota_critical")
            if self.on_rotation_needed:
                await self.on_rotation_needed(worker_id, account_id, "quota_exceeded")
            return

        if five_hour_pct >= threshold_pct:
            # Warning: approaching limit
            logger.warning(
                "QuotaMonitor: account %s quota warning (%.1f%% >= %.1f%%)",
                account_id, five_hour_pct, threshold_pct,
            )
            await self._emit_warning(worker_id, account_id, five_hour_pct, "quota_warning")

        # 3. 7d window budget management
        if self._config.weekly_reserve_per_day > 0:
            dynamic_threshold = self._calculate_dynamic_threshold(seven_day_resets_at)
            if seven_day_pct >= dynamic_threshold:
                logger.warning(
                    "QuotaMonitor: account %s 7-day budget exceeded (%.1f%% >= %.1f%%)",
                    account_id, seven_day_pct, dynamic_threshold,
                )
                await self._pool.mark_cooldown(account_id)
                await self._emit_warning(
                    worker_id, account_id, seven_day_pct, "weekly_budget_exceeded"
                )

    def _calculate_dynamic_threshold(self, resets_at: datetime | None) -> float:
        """Calculate dynamic 7-day threshold based on remaining days."""
        if resets_at is None:
            return 100.0

        now = datetime.now(timezone.utc)
        remaining = (resets_at - now).total_seconds() / 86400
        remaining_days = max(0, remaining)

        threshold = 100 - (remaining_days * self._config.weekly_reserve_per_day)
        return max(0, min(100, threshold))

    async def _emit_warning(
        self, worker_id: str, account_id: str, usage_pct: float, reason: str
    ) -> None:
        """Emit a QUOTA_WARNING event on the EventBus."""
        await self._bus.emit(
            "QUOTA_WARNING",
            worker_id,
            {
                "account_id": account_id,
                "usage_pct": usage_pct,
                "reason": reason,
            },
        )

    async def _cooldown_loop(self) -> None:
        """T-046: Periodically check cooldown recovery for all accounts."""
        while self._running:
            try:
                await self._pool.check_cooldown_recovery()
            except Exception:
                logger.exception("QuotaMonitor: cooldown recovery check failed")
            await asyncio.sleep(60)

    def _parse_datetime(self, value: Any) -> datetime | None:
        """Parse an ISO 8601 datetime string."""
        if value is None:
            return None
        if isinstance(value, datetime):
            return value
        try:
            if isinstance(value, str):
                return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            pass
        return None

    async def process_quota_status(
        self, worker_id: str, data: dict[str, Any]
    ) -> None:
        """Directly process a quota status update (alternative to EventBus)."""
        await self._handle_quota_status("QUOTA_STATUS", worker_id, data)
