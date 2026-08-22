"""Credential login service — orchestrates auto-login on Workers with affinity.

Listens for BOOTSTRAP_COMPLETED and WORKER_CONNECTED events. On each:
1. Check affinity — try to re-allocate the same account this Worker had before
2. If affinity account unavailable, allocate any available account (least-used-first)
3. Check if existing credentials on Worker are still valid (skip OAuth if so)
4. If credentials expired or missing, run full OAuth login into all slots

On WORKER_DISCONNECTED: unbind and release credentials back to pool.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import shlex
from dataclasses import dataclass
from typing import Any

from elastic_agent.core.claude_oauth import ClaudeOAuthProvider, LoginResult
from elastic_agent.core.config import CredentialConfig
from elastic_agent.core.credential_binding import CredentialBinding
from elastic_agent.core.credential_login_step import CredentialLoginStep
from elastic_agent.core.credential_pool import AccountDefinition, CredentialPool
from elastic_agent.core.event_bus import EventBus
from elastic_agent.core.network import worker_management_host
from elastic_agent.core.registry import NodeRegistry

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

    Affinity: When a Worker reconnects, we try to give it back the same account
    it had before. If that account is taken, we fall back to normal allocation.
    """

    def __init__(
        self,
        credential_pool: CredentialPool,
        credential_config: CredentialConfig,
        credential_binding: CredentialBinding,
        event_bus: EventBus,
        node_registry: NodeRegistry | None = None,
        slots: list[CredentialSlot] | None = None,
        oauth_provider: ClaudeOAuthProvider | None = None,
        ssh_key_path: str = "/root/.ssh/elastic-agent-aliyun.pem",
        ssh_user: str = "root",
    ) -> None:
        self._pool = credential_pool
        self._config = credential_config
        self._binding = credential_binding
        self._event_bus = event_bus
        self._registry = node_registry
        self._slots = slots or []
        self._ssh_key_path = ssh_key_path
        self._ssh_user = ssh_user
        self._oauth_provider = oauth_provider
        self._login_step = CredentialLoginStep(
            pool=credential_pool,
            oauth_provider=oauth_provider,
            login_timeout=credential_config.login_timeout,
        )
        self._login_tasks: dict[str, asyncio.Task] = {}

    def set_slots(self, slots: list[CredentialSlot]) -> None:
        self._slots = slots

    def register_event_handlers(self) -> None:
        self._event_bus.subscribe("BOOTSTRAP_COMPLETED", self._on_bootstrap_completed)
        self._event_bus.subscribe("WORKER_CONNECTED", self._on_worker_connected)
        self._event_bus.subscribe("WORKER_DISCONNECTED", self._on_worker_disconnected)

    # -- public API ----------------------------------------------------------

    async def login_worker(
        self, worker_id: str, *, skip_validity_check: bool = False
    ) -> list[LoginResult]:
        """Allocate account (with affinity preference) and login all slots.

        Args:
            worker_id: The Worker node ID (e.g. "aliyun:i-bp1xxx" or IP).
            skip_validity_check: If True, always do full OAuth (used after bootstrap).
        """
        if not self._slots:
            logger.warning("No credential slots configured — skipping login for %s", worker_id)
            return []

        account = await self._allocate_with_affinity(worker_id)
        if account is None:
            logger.warning("No available account for worker %s", worker_id)
            return []

        logger.info("Allocated account %s to worker %s", account.id, worker_id)

        await self._binding.bind(account.id, worker_id)

        if not skip_validity_check:
            host = await self._resolve_host(worker_id)
            if host and await self._check_credentials_valid(
                host,
                self._slots[0].config_dir,
                expected_email=account.email,
            ):
                logger.info(
                    "Credentials for account %s still valid on worker %s — skipping OAuth",
                    account.id, worker_id,
                )
                await self._pool.update_login_status(account.id, "logged_in")
                await self._write_account_markers(host, account.id)
                await self._notify_worker_credential_ready(worker_id, account.id)
                return [LoginResult(success=True, account_id=account.id)]

        host = await self._resolve_host(worker_id)
        if host:
            await self._ensure_display_server(host)

        accounts_for_login = [
            (account, slot.config_dir, slot.slot_type)
            for slot in self._slots
        ]

        results = await self._login_step.execute(
            worker_id=worker_id,
            accounts=accounts_for_login,
            worker_host=host,
            ssh_key_path=self._ssh_key_path,
            ssh_user=self._ssh_user,
        )

        success_count = sum(1 for r in results if r.success)
        logger.info(
            "Credential login for worker %s: %d/%d slots succeeded",
            worker_id, success_count, len(self._slots),
        )

        if success_count > 0:
            await self._pool.update_login_status(account.id, "logged_in")
            host = await self._resolve_host(worker_id)
            if host:
                await self._write_account_markers(host, account.id)
            await self._notify_worker_credential_ready(worker_id, account.id)
        else:
            logger.error(
                "All credential slots failed for worker %s (account %s) — releasing account",
                worker_id, account.id,
            )
            await self._binding.unbind(account.id)
            await self._pool.release(account.id)

        return results

    async def retry_failed_accounts(self, worker_id: str) -> int:
        """Re-attempt accounts stuck in login_failed (verification login).

        CF flakiness can mark a healthy account login_failed, and nothing
        retries it until rotation happens to allocate it — the pool quietly
        loses capacity. After a worker finishes its normal login (Chrome is
        free), re-verify each failed unassigned account against a scratch
        config_dir on that worker and flip the pool state back on success.
        Returns the number of recovered accounts.
        """
        failed = [
            a for a in self._pool._accounts_config.accounts
            if (s := self._pool.get_status(a.id)) is not None
            and s.login_status == "login_failed"
            and not s.assigned_to
        ]
        if not failed:
            return 0

        host = await self._resolve_host(worker_id)
        if not host:
            return 0
        await self._ensure_display_server(host)

        from elastic_agent.core.claude_oauth import ClaudeOAuthProvider, OAuthConfig

        recovered = 0
        for acct in failed:
            logger.info(
                "Retrying login_failed account %s (verification login on %s)",
                acct.id, worker_id,
            )
            provider = self._oauth_provider or ClaudeOAuthProvider()
            try:
                result = await provider.login(OAuthConfig(
                    account_id=acct.id,
                    email=acct.email,
                    email_token=acct.email_token,
                    # Scratch dir on a disposable worker VM; never a real slot
                    config_dir=f"/tmp/claude-verify-{acct.id}",
                    login_timeout=self._config.login_timeout,
                    worker_host=host,
                    ssh_key_path=self._ssh_key_path,
                    ssh_user=self._ssh_user,
                ))
            except Exception:
                logger.exception("Verification login errored for account %s", acct.id)
                continue
            if result.success:
                await self._pool.update_login_status(acct.id, "logged_in")
                recovered += 1
                logger.info("Recovered login_failed account %s", acct.id)
            else:
                logger.warning(
                    "Account %s still failing login: %s", acct.id, result.error
                )
        return recovered

    async def _worker_has_active_claude_process(self, worker_id: str) -> bool:
        """Return True when the worker is already running a Claude task."""
        host = await self._resolve_host(worker_id)
        if not host:
            return False
        try:
            ssh_opts = [
                "-o", "StrictHostKeyChecking=no",
                "-o", "UserKnownHostsFile=/dev/null",
                "-o", "ConnectTimeout=5",
            ]
            cmd = (
                "pgrep -af "
                "'[c]laude -p|[c]laude .*--output-format stream-json|"
                "[c]laude .*server:pty-bridge|[c]laude-pty-channel' "
                ">/dev/null"
            )
            proc = await asyncio.create_subprocess_exec(
                "ssh", *ssh_opts, "-i", self._ssh_key_path,
                f"{self._ssh_user}@{host}", cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            await asyncio.wait_for(proc.communicate(), timeout=8)
            return proc.returncode == 0
        except Exception:
            logger.debug("Failed to check active Claude process on worker %s", worker_id)
            return False

    async def _restore_active_worker_credential(self, worker_id: str) -> bool:
        """Restore a credential binding for a Worker that is already running Claude.

        On manager restart the pool/binding state can forget which account a
        still-running Worker is using. Allocating a fresh account at that point
        can duplicate an account across Workers. This method reclaims the local
        account instead, or blocks login if doing so would violate exclusivity.
        """
        if not self._slots:
            return False
        if not await self._worker_has_active_claude_process(worker_id):
            return False

        host = await self._resolve_host(worker_id)
        if not host:
            logger.warning(
                "Worker %s has an active Claude process but no SSH host; "
                "skipping credential allocation",
                worker_id,
            )
            return True

        config_dir = self._slots[0].config_dir
        marker_account_id = await self._read_account_marker(host, config_dir)
        logged_in, actual_email = await self._get_credentials_status(host, config_dir)
        account = self._resolve_account_from_local_state(marker_account_id, actual_email)

        if not logged_in or account is None:
            logger.error(
                "Worker %s has an active Claude process but local credentials "
                "could not be mapped safely (marker=%s, email=%s, logged_in=%s); "
                "blocking automatic credential allocation",
                worker_id,
                marker_account_id or "<missing>",
                actual_email or "<unknown>",
                logged_in,
            )
            return True

        bound_worker = self._binding.get_worker(account.id)
        if bound_worker and bound_worker != worker_id:
            logger.error(
                "Worker %s is running with account %s, but the account is "
                "already bound to worker %s; blocking duplicate login",
                worker_id,
                account.id,
                bound_worker,
            )
            return True

        claimed = await self._pool.claim_existing_assignment(
            account.id,
            worker_id,
            "production",
            config_dir,
        )
        if claimed is None:
            logger.error(
                "Worker %s is running with account %s, but the pool refused "
                "to claim it; blocking automatic credential allocation",
                worker_id,
                account.id,
            )
            return True

        if not await self._binding.bind(account.id, worker_id):
            logger.error(
                "Worker %s restored account %s in the pool, but binding "
                "rejected it; blocking duplicate login",
                worker_id,
                account.id,
            )
            return True

        await self._write_account_markers(host, account.id)
        await self._notify_worker_credential_ready(worker_id, account.id)
        logger.info(
            "Worker %s active Claude process restored credential account %s",
            worker_id,
            account.id,
        )
        return True

    async def release_worker(self, worker_id: str) -> None:
        """Unbind and release all accounts allocated to a Worker."""
        await self._binding.unbind_worker(worker_id)
        await self._pool.release_worker(worker_id)
        logger.info("Released all credentials for worker %s", worker_id)

    async def _write_account_markers(self, host: str, account_id: str) -> None:
        """Write account_id marker files to Worker config dirs via SSH."""
        for slot in self._slots:
            config_dir = slot.config_dir
            try:
                ssh_opts = ["-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null",
                            "-o", "ConnectTimeout=10"]
                cmd = f"echo '{account_id}' > {config_dir}/.account_id"
                proc = await asyncio.create_subprocess_exec(
                    "ssh", *ssh_opts, "-i", self._ssh_key_path,
                    f"{self._ssh_user}@{host}", cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                await asyncio.wait_for(proc.communicate(), timeout=10)
            except Exception:
                logger.debug("Failed to write account_id marker to %s on %s", config_dir, host)

    async def _read_account_marker(self, host: str, config_dir: str) -> str | None:
        try:
            ssh_opts = ["-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null",
                        "-o", "ConnectTimeout=10"]
            quoted_config_dir = shlex.quote(config_dir)
            cmd = f"cat {quoted_config_dir}/.account_id 2>/dev/null || true"
            proc = await asyncio.create_subprocess_exec(
                "ssh", *ssh_opts, "-i", self._ssh_key_path,
                f"{self._ssh_user}@{host}", cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=10)
            value = stdout.decode().strip()
            return value or None
        except Exception:
            logger.debug("Failed to read account_id marker from %s on %s", config_dir, host)
            return None

    async def _notify_worker_credential_ready(self, worker_id: str, account_id: str) -> None:
        """Emit CREDENTIAL_READY event so the Worker's QuotaChecker can pick up active slots."""
        slot_info = [
            {"account_id": account_id, "config_dir": slot.config_dir, "slot_type": slot.slot_type}
            for slot in self._slots
        ]
        await self._event_bus.emit("CREDENTIAL_READY", worker_id, {
            "account_id": account_id,
            "slots": slot_info,
        })

    # -- affinity allocation -------------------------------------------------

    async def _allocate_with_affinity(self, worker_id: str) -> AccountDefinition | None:
        """Try affinity-based allocation first, then fall back to normal allocation."""
        preferred_account_id = self._get_affinity_account(worker_id)

        if preferred_account_id:
            logger.info(
                "Affinity: worker %s previously used account %s — trying allocate_specific",
                worker_id, preferred_account_id,
            )
            account = await self._pool.allocate_specific(
                preferred_account_id, worker_id, "production"
            )
            if account is not None:
                logger.info("Affinity allocation succeeded: %s -> %s", preferred_account_id, worker_id)
                return account
            logger.info(
                "Affinity allocation failed for %s (unavailable) — falling back to normal",
                preferred_account_id,
            )

        return await self._pool.allocate(worker_id, "production", "standard")

    def _get_affinity_account(self, worker_id: str) -> str | None:
        """Find the account that was most recently bound to this worker."""
        best_account: str | None = None
        best_time = None
        for (aid, wid), ts in self._binding._affinity.items():
            if wid == worker_id:
                if best_time is None or ts > best_time:
                    best_time = ts
                    best_account = aid
        for aid, status in self._pool._pool_status.accounts.items():
            if status.last_assigned_to != worker_id:
                continue
            ts = status.last_used or status.last_login_at
            if best_time is None or (ts is not None and ts > best_time):
                best_time = ts
                best_account = aid
        return best_account

    # -- display server (Xvfb) check ----------------------------------------

    async def _ensure_display_server(self, host: str) -> None:
        """SSH into Worker and ensure Xvfb + openbox are running before OAuth."""
        try:
            ssh_opts = ["-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null",
                        "-o", "ConnectTimeout=10"]
            cmd = (
                "if systemctl is-active --quiet xvfb.service 2>/dev/null; then "
                "  echo XVFB_OK; "
                "else "
                "  if command -v systemctl >/dev/null && [ -f /etc/systemd/system/xvfb.service ]; then "
                "    systemctl start xvfb.service openbox.service && echo XVFB_STARTED; "
                "  else "
                "    ( pgrep -x Xvfb || (Xvfb :99 -screen 0 1365x900x24 -ac "
                "+extension GLX +render -noreset >/dev/null 2>&1 &) ) && "
                "    sleep 1 && "
                "    ( pgrep openbox || (DISPLAY=:99 openbox --sm-disable >/dev/null 2>&1 &) ) && "
                "    echo XVFB_STARTED_MANUAL; "
                "  fi; "
                "fi"
            )
            proc = await asyncio.create_subprocess_exec(
                "ssh", *ssh_opts, "-i", self._ssh_key_path,
                f"{self._ssh_user}@{host}", cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=30)
            output = stdout.decode().strip()
            logger.info("Display server check on %s: %s", host, output)
        except Exception as e:
            logger.warning("Failed to ensure display server on %s: %s", host, e)

    # -- credential validity check -------------------------------------------

    async def _check_credentials_valid(
        self,
        host: str,
        config_dir: str,
        *,
        expected_email: str | None = None,
    ) -> bool:
        """SSH into Worker and check if Claude credentials are valid.

        Runs `claude auth status` with the given config_dir. Returns True only
        if the credentials are logged in and, when provided, the logged-in email
        matches the account that the manager intends to bind to the Worker.
        """
        logged_in, actual_email = await self._get_credentials_status(host, config_dir)
        if not logged_in:
            return False
        if expected_email and (
            not actual_email
            or actual_email.casefold() != expected_email.strip().casefold()
        ):
            logger.warning(
                "Credential email mismatch on %s (%s): expected %s, got %s",
                host,
                config_dir,
                expected_email,
                actual_email or "<unknown>",
            )
            return False
        return True

    async def _get_credentials_status(
        self,
        host: str,
        config_dir: str,
    ) -> tuple[bool, str | None]:
        try:
            ssh_opts = ["-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null",
                        "-o", "ConnectTimeout=10"]
            quoted_config_dir = shlex.quote(config_dir)
            cmd = f"CLAUDE_CONFIG_DIR={quoted_config_dir} claude auth status 2>/dev/null || true"
            proc = await asyncio.create_subprocess_exec(
                "ssh", *ssh_opts, "-i", self._ssh_key_path,
                f"{self._ssh_user}@{host}", cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=30)
            output = stdout.decode().strip()
            logged_in = False
            actual_email: str | None = None

            try:
                status = json.loads(output)
                logged_in = bool(status.get("loggedIn"))
                email = status.get("email")
                if isinstance(email, str):
                    actual_email = email.strip()
            except json.JSONDecodeError:
                logged_in = "Logged in" in output or '"loggedIn": true' in output
                match = re.search(r"[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}", output)
                if match:
                    actual_email = match.group(0)

            return logged_in, actual_email
        except Exception as e:
            logger.debug("Credential status check failed for %s: %s", host, e)
            return False, None

    def _resolve_account_from_local_state(
        self,
        marker_account_id: str | None,
        actual_email: str | None,
    ) -> AccountDefinition | None:
        marker_account = None
        if marker_account_id:
            marker_account = next(
                (
                    acct for acct in self._pool._accounts_config.accounts
                    if acct.id == marker_account_id
                ),
                None,
            )

        email_account = None
        if actual_email:
            email_account = next(
                (
                    acct for acct in self._pool._accounts_config.accounts
                    if acct.email.casefold() == actual_email.casefold()
                ),
                None,
            )

        if marker_account and email_account and marker_account.id != email_account.id:
            logger.warning(
                "Local account marker %s disagrees with Claude auth email %s "
                "(mapped to %s); trusting auth email",
                marker_account.id,
                actual_email,
                email_account.id,
            )
            return email_account
        return email_account or marker_account

    # -- helpers -------------------------------------------------------------

    async def _resolve_host(self, worker_id: str) -> str | None:
        """Resolve SSH-reachable host (IP) from worker_id via NodeRegistry."""
        if self._registry:
            node = await self._registry.get(worker_id)
            host = worker_management_host(node)
            if host:
                return host

        # Fallback: if worker_id looks like an IP or contains one after ":"
        if ":" in worker_id:
            suffix = worker_id.split(":", 1)[1]
            if suffix[0].isdigit():
                return suffix
        elif worker_id[0].isdigit():
            return worker_id
        return None

    # -- event handlers ------------------------------------------------------

    def _start_login_task(
        self,
        worker_id: str,
        *,
        skip_validity_check: bool,
        retry_failed_accounts: bool,
        reason: str,
    ) -> None:
        existing = self._login_tasks.get(worker_id)
        if existing and not existing.done():
            logger.info(
                "Credential login already running for worker %s; skipping %s",
                worker_id,
                reason,
            )
            return

        async def _run() -> None:
            try:
                await self.login_worker(worker_id, skip_validity_check=skip_validity_check)
                if retry_failed_accounts:
                    # CREDENTIAL_READY may dispatch a queued production task immediately.
                    # Give the dispatcher a short window to start Claude before running
                    # any scratch verification login on the same Worker.
                    await asyncio.sleep(5)
                    if await self._worker_has_active_claude_process(worker_id):
                        logger.info(
                            "Worker %s already has an active Claude process; "
                            "skipping failed-account verification login",
                            worker_id,
                        )
                    else:
                        await self.retry_failed_accounts(worker_id)
            except asyncio.CancelledError:
                logger.info("Credential login task cancelled for worker %s", worker_id)
                raise
            except Exception:
                logger.exception("Credential login failed for worker %s", worker_id)

        task = asyncio.create_task(_run())
        self._login_tasks[worker_id] = task

        def _done(done_task: asyncio.Task) -> None:
            if self._login_tasks.get(worker_id) is done_task:
                self._login_tasks.pop(worker_id, None)
            if done_task.cancelled():
                return
            exc = done_task.exception()
            if exc:
                logger.error(
                    "Credential login background task failed for worker %s",
                    worker_id,
                    exc_info=(type(exc), exc, exc.__traceback__),
                )

        task.add_done_callback(_done)

    async def _on_bootstrap_completed(
        self, event_type: str, worker_id: str, data: dict[str, Any]
    ) -> None:
        logger.info("Bootstrap completed for worker %s — starting credential login", worker_id)
        self._start_login_task(
            worker_id,
            skip_validity_check=True,
            retry_failed_accounts=False,
            reason="bootstrap",
        )

    async def _on_worker_connected(
        self, event_type: str, worker_id: str, data: dict[str, Any]
    ) -> None:
        """Worker reconnected — try to restore previous account with validity check."""
        logger.info("Worker %s connected — checking credential state", worker_id)
        if await self._restore_active_worker_credential(worker_id):
            return
        self._start_login_task(
            worker_id,
            skip_validity_check=False,
            retry_failed_accounts=True,
            reason="worker reconnect",
        )

    async def _on_worker_disconnected(
        self, event_type: str, worker_id: str, data: dict[str, Any]
    ) -> None:
        task = self._login_tasks.pop(worker_id, None)
        if task and not task.done():
            task.cancel()
        await self.release_worker(worker_id)
