"""Worker-side quota monitoring — periodic usage API checks with token refresh.

T-043: Each Worker periodically checks all active credentials' quota usage via
the Anthropic Usage API. Reports QUOTA_STATUS events to the Manager.

Check interval: Random 170-190s delay between accounts (anti-detection).
Per-account cycle: N_accounts * ~180s (e.g., 4 accounts -> every ~12 minutes).
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable

from elastic_agent.core.claude_oauth import (
    ANTHROPIC_USAGE_URL,
    OAUTH_CLIENT_ID,
    read_credentials,
    refresh_access_token,
    write_credentials,
)

logger = logging.getLogger(__name__)

QuotaCallback = Callable[[dict[str, Any]], Awaitable[None]]


class QuotaCheckResult:
    """Result of a single account's quota check."""

    __slots__ = (
        "account_id", "success", "five_hour_pct", "seven_day_pct",
        "five_hour_resets_at", "seven_day_resets_at", "available",
        "error", "stale", "needs_relogin",
    )

    def __init__(
        self,
        account_id: str,
        success: bool = False,
        five_hour_pct: float = 0.0,
        seven_day_pct: float = 0.0,
        five_hour_resets_at: str | None = None,
        seven_day_resets_at: str | None = None,
        available: bool = True,
        error: str | None = None,
        stale: bool = False,
        needs_relogin: bool = False,
    ):
        self.account_id = account_id
        self.success = success
        self.five_hour_pct = five_hour_pct
        self.seven_day_pct = seven_day_pct
        self.five_hour_resets_at = five_hour_resets_at
        self.seven_day_resets_at = seven_day_resets_at
        self.available = available
        self.error = error
        self.stale = stale
        self.needs_relogin = needs_relogin


class QuotaChecker:
    """Worker-side periodic quota monitoring for all active credential slots.

    Runs as a background task on the Worker, cycling through all active
    credentials and checking their Usage API quota.

    Usage:
        checker = QuotaChecker(
            active_slots=[
                {"account_id": "prod-1", "config_dir": "/root/.claude-prod"},
            ],
            on_quota_status=async_callback,
            delay_min=170,
            delay_max=190,
        )
        await checker.start()
        # ... later ...
        await checker.stop()
    """

    def __init__(
        self,
        active_slots: list[dict[str, str]] | None = None,
        on_quota_status: QuotaCallback | None = None,
        delay_min: int = 170,
        delay_max: int = 190,
        http_client: Any | None = None,
    ) -> None:
        self._slots: list[dict[str, str]] = active_slots or []
        self._on_quota_status = on_quota_status
        self._delay_min = delay_min
        self._delay_max = delay_max
        self._http_client = http_client
        self._running = False
        self._task: asyncio.Task | None = None
        self._cache: dict[str, tuple[float, QuotaCheckResult]] = {}
        self._backoff: dict[str, float] = {}
        self._base_backoff = 180.0
        self._max_backoff = 2880.0
        self._cache_ttl = 1800.0

    def add_slot(self, account_id: str, config_dir: str) -> None:
        """Add a credential slot to monitor."""
        for slot in self._slots:
            if slot["account_id"] == account_id:
                slot["config_dir"] = config_dir
                return
        self._slots.append({"account_id": account_id, "config_dir": config_dir})

    def remove_slot(self, account_id: str) -> None:
        """Remove a credential slot from monitoring."""
        self._slots = [s for s in self._slots if s["account_id"] != account_id]
        self._cache.pop(account_id, None)
        self._backoff.pop(account_id, None)

    async def start(self) -> None:
        """Start the periodic quota checking background task."""
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._run_loop())
        logger.info("QuotaChecker: started with %d slots", len(self._slots))

    async def stop(self) -> None:
        """Stop the periodic quota checking."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        logger.info("QuotaChecker: stopped")

    async def check_account(self, account_id: str, config_dir: str) -> QuotaCheckResult:
        """Check quota for a single account (on-demand or from the loop)."""
        # Step 1: Read credentials
        creds = read_credentials(config_dir)
        if creds is None:
            return QuotaCheckResult(
                account_id=account_id,
                error="credentials.json not found",
            )

        access_token = creds.get("accessToken", "")
        refresh_token_val = creds.get("refreshToken", "")
        expires_at = creds.get("expiresAt", 0)

        # Step 2: Token refresh if needed
        now_ms = int(time.time() * 1000)
        if now_ms >= expires_at - 5 * 60 * 1000:
            refreshed = await refresh_access_token(
                refresh_token_val, http_client=self._http_client
            )
            if refreshed:
                access_token = refreshed["accessToken"]
                write_credentials(config_dir, refreshed)
                logger.debug("QuotaChecker: refreshed token for %s", account_id)
            else:
                # Fallback: try with possibly expired token
                logger.warning(
                    "QuotaChecker: token refresh failed for %s, trying expired token",
                    account_id,
                )

        # Step 3: Call Usage API
        try:
            usage = await self._fetch_usage(access_token)
        except RateLimitError:
            # Rate limited: use cache if available
            cached = self._get_cached(account_id)
            if cached:
                cached.stale = True
                self._increase_backoff(account_id)
                return cached
            return QuotaCheckResult(
                account_id=account_id,
                error="rate_limited",
                stale=True,
            )
        except AuthError:
            # Auth error with expired token
            if not refresh_token_val:
                return QuotaCheckResult(
                    account_id=account_id,
                    error="auth_error_no_refresh_token",
                    needs_relogin=True,
                )
            return QuotaCheckResult(
                account_id=account_id,
                error="auth_error_refresh_failed",
                needs_relogin=True,
            )
        except Exception as e:
            return QuotaCheckResult(
                account_id=account_id,
                error=str(e),
            )

        # Step 4: Parse response
        five_hour = usage.get("five_hour", {})
        seven_day = usage.get("seven_day", {})

        result = QuotaCheckResult(
            account_id=account_id,
            success=True,
            five_hour_pct=float(five_hour.get("utilization", 0)),
            seven_day_pct=float(seven_day.get("utilization", 0)),
            five_hour_resets_at=five_hour.get("resets_at"),
            seven_day_resets_at=seven_day.get("resets_at"),
            available=True,
        )

        # Cache the result
        self._cache[account_id] = (time.monotonic(), result)
        self._reset_backoff(account_id)

        return result

    async def check_all(self) -> list[QuotaCheckResult]:
        """Check quota for all active slots (returns results in order)."""
        results = []
        for slot in list(self._slots):
            result = await self.check_account(slot["account_id"], slot["config_dir"])
            results.append(result)

            # Report to Manager via callback
            if self._on_quota_status and result.success:
                await self._report_status(result)

        return results

    # -- Internal -----------------------------------------------------------

    async def _run_loop(self) -> None:
        """Main loop: cycle through slots with random delays."""
        while self._running:
            for slot in list(self._slots):
                if not self._running:
                    break

                account_id = slot["account_id"]

                # Check backoff
                backoff_until = self._backoff.get(account_id, 0)
                if time.monotonic() < backoff_until:
                    await asyncio.sleep(1)
                    continue

                try:
                    result = await self.check_account(account_id, slot["config_dir"])
                    if self._on_quota_status:
                        await self._report_status(result)
                except Exception:
                    logger.exception(
                        "QuotaChecker: error checking account %s", account_id
                    )

                # Random delay between accounts (anti-detection)
                if self._running:
                    delay = random.uniform(self._delay_min, self._delay_max)
                    await asyncio.sleep(delay)

            # If no slots, sleep briefly before retrying
            if not self._slots and self._running:
                await asyncio.sleep(10)

    async def _report_status(self, result: QuotaCheckResult) -> None:
        """Send QUOTA_STATUS event to the Manager."""
        if not self._on_quota_status:
            return

        status = {
            "type": "QUOTA_STATUS",
            "account_id": result.account_id,
            "five_hour_pct": result.five_hour_pct,
            "seven_day_pct": result.seven_day_pct,
            "five_hour_resets_at": result.five_hour_resets_at,
            "seven_day_resets_at": result.seven_day_resets_at,
            "available": result.available,
            "stale": result.stale,
            "needs_relogin": result.needs_relogin,
            "error": result.error,
        }
        try:
            await self._on_quota_status(status)
        except Exception:
            logger.exception("QuotaChecker: failed to report status for %s", result.account_id)

    async def _fetch_usage(self, access_token: str) -> dict[str, Any]:
        """Call Anthropic Usage API."""
        if self._http_client:
            return await self._http_client.get_usage(access_token)

        import aiohttp

        headers = {
            "Authorization": f"Bearer {access_token}",
            "anthropic-beta": "oauth-2025-04-20",
        }
        async with aiohttp.ClientSession() as session:
            async with session.get(
                ANTHROPIC_USAGE_URL,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                if resp.status == 429:
                    raise RateLimitError("Usage API rate limited")
                if resp.status in (401, 403):
                    raise AuthError("Usage API auth error")
                if resp.status != 200:
                    raise UsageAPIError(f"Usage API returned {resp.status}")
                return await resp.json()

    def _get_cached(self, account_id: str) -> QuotaCheckResult | None:
        """Get cached result if within TTL."""
        entry = self._cache.get(account_id)
        if entry is None:
            return None
        cached_at, result = entry
        if time.monotonic() - cached_at > self._cache_ttl:
            return None
        return result

    def _increase_backoff(self, account_id: str) -> None:
        """Increase backoff for an account (exponential)."""
        current = self._backoff.get(account_id, 0)
        if current == 0:
            next_delay = self._base_backoff
        else:
            elapsed = time.monotonic() - current + self._base_backoff
            next_delay = min(elapsed * 2, self._max_backoff)
        self._backoff[account_id] = time.monotonic() + next_delay

    def _reset_backoff(self, account_id: str) -> None:
        """Reset backoff for an account after successful check."""
        self._backoff.pop(account_id, None)


class RateLimitError(Exception):
    pass


class AuthError(Exception):
    pass


class UsageAPIError(Exception):
    pass
