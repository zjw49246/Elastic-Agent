"""Worker Bootstrap login step — execute auto-login for each assigned credential slot.

T-042: Each slot runs an independent OAuth flow to obtain its own refresh_token.
Refresh tokens are single-use, so slots cannot share credentials.
On failure, rollback (release) already-logged-in accounts.
"""

from __future__ import annotations

import logging
from typing import Any, Awaitable, Callable

from elastic_agent.core.claude_oauth import ClaudeOAuthProvider, LoginResult, OAuthConfig
from elastic_agent.core.credential_pool import AccountDefinition, CredentialPool

logger = logging.getLogger(__name__)

LoginCallback = Callable[[str, int, LoginResult], Awaitable[None]]


class CredentialLoginStep:
    """Orchestrates serial credential login across multiple slots on a single Worker.

    Each slot gets its own independent OAuth session (refresh_token is single-use,
    so sharing credentials across slots causes 401 errors).

    Usage during Bootstrap:
        step = CredentialLoginStep(pool, oauth_provider, login_timeout=240)
        results = await step.execute(
            worker_id="aliyun:i-bp1xxx",
            accounts=[(acct_def, "/root/.claude-prod", "production")],
        )
    """

    def __init__(
        self,
        pool: CredentialPool,
        oauth_provider: ClaudeOAuthProvider | None = None,
        login_timeout: int = 240,
        on_login_result: LoginCallback | None = None,
    ) -> None:
        self._pool = pool
        self._oauth = oauth_provider or ClaudeOAuthProvider()
        self._login_timeout = login_timeout
        self._on_login_result = on_login_result

    async def execute(
        self,
        worker_id: str,
        accounts: list[tuple[AccountDefinition, str, str]],
    ) -> list[LoginResult]:
        """Execute serial OAuth login for each slot independently.

        Args:
            worker_id: The Worker node ID.
            accounts: List of (AccountDefinition, config_dir, slot_type) tuples.

        Returns:
            List of LoginResult for each account (same order as input).
            On first failure, remaining accounts are skipped and already-logged-in
            accounts are rolled back.
        """
        results: list[LoginResult] = []
        logged_in_accounts: list[str] = []

        for slot_index, (acct, config_dir, slot_type) in enumerate(accounts):
            logger.info(
                "CredentialLoginStep: logging in account %s (slot %d/%d, type=%s) on worker %s",
                acct.id, slot_index + 1, len(accounts), slot_type, worker_id,
            )

            await self._pool.update_login_status(acct.id, "logging_in")

            config = OAuthConfig(
                account_id=acct.id,
                email=acct.email,
                email_token=acct.email_token,
                config_dir=config_dir,
                login_timeout=self._login_timeout,
            )

            result = await self._oauth.login(config)
            results.append(result)

            if self._on_login_result:
                await self._on_login_result(acct.id, slot_index, result)

            if result.success:
                await self._pool.update_login_status(acct.id, "logged_in")
                logged_in_accounts.append(acct.id)
                logger.info(
                    "CredentialLoginStep: account %s logged in successfully (slot %d)",
                    acct.id, slot_index,
                )
            else:
                await self._pool.update_login_status(
                    acct.id, "login_failed", error=result.error
                )
                logger.error(
                    "CredentialLoginStep: account %s login failed: %s — rolling back",
                    acct.id, result.error,
                )
                await self._rollback(logged_in_accounts, worker_id)
                break

        return results

    async def execute_single(
        self,
        account: AccountDefinition,
        config_dir: str,
        slot_type: str = "production",
    ) -> LoginResult:
        """Execute login for a single account (used during rotation or re-login)."""
        await self._pool.update_login_status(account.id, "logging_in")

        config = OAuthConfig(
            account_id=account.id,
            email=account.email,
            email_token=account.email_token,
            config_dir=config_dir,
            login_timeout=self._login_timeout,
        )

        result = await self._oauth.login(config)

        if result.success:
            await self._pool.update_login_status(account.id, "logged_in")
        else:
            await self._pool.update_login_status(
                account.id, "login_failed", error=result.error
            )

        return result

    async def _rollback(self, account_ids: list[str], worker_id: str) -> None:
        """Release all successfully logged-in accounts on failure."""
        for acct_id in account_ids:
            logger.info(
                "CredentialLoginStep: rolling back account %s on worker %s",
                acct_id, worker_id,
            )
            await self._pool.release(acct_id)
