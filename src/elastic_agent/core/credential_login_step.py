"""Worker Bootstrap login step — execute auto-login for each assigned credential slot.

T-042: First slot runs the full OAuth flow. Remaining slots for the same account
reuse the credentials via file copy (no duplicate OAuth round-trips).
On failure, rollback (release) already-logged-in accounts.
"""

from __future__ import annotations

import logging
import shutil
from pathlib import Path
from typing import Any, Awaitable, Callable

from elastic_agent.core.claude_oauth import ClaudeOAuthProvider, LoginResult, OAuthConfig
from elastic_agent.core.credential_pool import AccountDefinition, CredentialPool

logger = logging.getLogger(__name__)

LoginCallback = Callable[[str, int, LoginResult], Awaitable[None]]

_CREDENTIAL_FILES = (".credentials.json", ".claude.json")


def _copy_credentials(src_dir: str, dst_dir: str) -> bool:
    """Copy credential files from one config_dir to another."""
    dst = Path(dst_dir)
    dst.mkdir(parents=True, exist_ok=True)
    copied = False
    for fname in _CREDENTIAL_FILES:
        src_file = Path(src_dir) / fname
        if src_file.exists():
            shutil.copy2(src_file, dst / fname)
            copied = True
    return copied


class CredentialLoginStep:
    """Orchestrates serial credential login across multiple slots on a single Worker.

    When multiple slots use the same account, only the first slot runs the full
    OAuth flow. Subsequent slots copy the credential files directly.

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
        """Execute login for each account slot.

        For the same account across multiple slots, only the first slot runs the
        full OAuth flow. Remaining slots copy credentials from the first.

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
        # Track which account has already been OAuth'd and where
        oauth_done: dict[str, str] = {}  # account_id -> first successful config_dir

        for slot_index, (acct, config_dir, slot_type) in enumerate(accounts):
            logger.info(
                "CredentialLoginStep: logging in account %s (slot %d/%d, type=%s) on worker %s",
                acct.id, slot_index + 1, len(accounts), slot_type, worker_id,
            )

            await self._pool.update_login_status(acct.id, "logging_in")

            if acct.id in oauth_done:
                # Same account already logged in — copy credentials
                src_dir = oauth_done[acct.id]
                ok = _copy_credentials(src_dir, config_dir)
                if ok:
                    result = LoginResult(success=True, account_id=acct.id)
                    logger.info(
                        "CredentialLoginStep: copied credentials from %s to %s (slot %d)",
                        src_dir, config_dir, slot_index,
                    )
                else:
                    result = LoginResult(
                        success=False, account_id=acct.id,
                        error=f"Failed to copy credentials from {src_dir}",
                    )
            else:
                # First slot for this account — run full OAuth
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
                if acct.id not in oauth_done:
                    oauth_done[acct.id] = config_dir
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
