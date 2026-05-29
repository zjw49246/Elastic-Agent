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
import logging
from dataclasses import dataclass
from typing import Any

from elastic_agent.core.claude_oauth import ClaudeOAuthProvider, LoginResult
from elastic_agent.core.config import CredentialConfig
from elastic_agent.core.credential_binding import CredentialBinding
from elastic_agent.core.credential_login_step import CredentialLoginStep
from elastic_agent.core.credential_pool import AccountDefinition, CredentialPool
from elastic_agent.core.event_bus import EventBus
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
        self._login_step = CredentialLoginStep(
            pool=credential_pool,
            oauth_provider=oauth_provider,
            login_timeout=credential_config.login_timeout,
        )

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
            if host and await self._check_credentials_valid(host, self._slots[0].config_dir):
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
        )

        success_count = sum(1 for r in results if r.success)
        logger.info(
            "Credential login for worker %s: %d/%d slots succeeded",
            worker_id, success_count, len(self._slots),
        )

        if success_count > 0:
            host = await self._resolve_host(worker_id)
            if host:
                await self._write_account_markers(host, account.id)
            await self._notify_worker_credential_ready(worker_id, account.id)

        return results

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
                "    ( pgrep -x Xvfb || (Xvfb :99 -screen 0 1365x900x24 -ac +extension GLX +render -noreset >/dev/null 2>&1 &) ) && "
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

    async def _check_credentials_valid(self, host: str, config_dir: str) -> bool:
        """SSH into Worker and check if Claude credentials are valid.

        Runs `claude auth status` with the given config_dir. Returns True if
        the credentials file exists and the auth check passes.
        """
        try:
            ssh_opts = ["-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null",
                        "-o", "ConnectTimeout=10"]
            cmd = (
                f"CLAUDE_CONFIG_DIR={config_dir} claude auth status 2>&1 | "
                f"grep -q 'Logged in' && echo VALID || echo INVALID"
            )
            proc = await asyncio.create_subprocess_exec(
                "ssh", *ssh_opts, "-i", self._ssh_key_path,
                f"{self._ssh_user}@{host}", cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=30)
            output = stdout.decode().strip()
            return output == "VALID"
        except Exception as e:
            logger.debug("Credential validity check failed for %s: %s", host, e)
            return False

    # -- helpers -------------------------------------------------------------

    async def _resolve_host(self, worker_id: str) -> str | None:
        """Resolve SSH-reachable host (IP) from worker_id via NodeRegistry."""
        if self._registry:
            node = await self._registry.get(worker_id)
            if node and node.public_ip:
                return node.public_ip
            if node and node.private_ip:
                return node.private_ip

        # Fallback: if worker_id looks like an IP or contains one after ":"
        if ":" in worker_id:
            suffix = worker_id.split(":", 1)[1]
            if suffix[0].isdigit():
                return suffix
        elif worker_id[0].isdigit():
            return worker_id
        return None

    # -- event handlers ------------------------------------------------------

    async def _on_bootstrap_completed(
        self, event_type: str, worker_id: str, data: dict[str, Any]
    ) -> None:
        logger.info("Bootstrap completed for worker %s — starting credential login", worker_id)
        try:
            await self.login_worker(worker_id, skip_validity_check=True)
        except Exception:
            logger.exception("Credential login failed for worker %s", worker_id)

    async def _on_worker_connected(
        self, event_type: str, worker_id: str, data: dict[str, Any]
    ) -> None:
        """Worker reconnected — try to restore previous account with validity check."""
        logger.info("Worker %s connected — checking credential state", worker_id)
        try:
            await self.login_worker(worker_id, skip_validity_check=False)
        except Exception:
            logger.exception("Credential login failed for reconnected worker %s", worker_id)

    async def _on_worker_disconnected(
        self, event_type: str, worker_id: str, data: dict[str, Any]
    ) -> None:
        await self.release_worker(worker_id)
