"""Live provision/login wiring for BatchOrchestrator.

Turns the ManagerFleetDriver's injected hooks into real behavior so ``/batch``
can actually stand a fleet up:

- **AccountAllocator**: hands each worker a distinct account identity from the
  AccountStore, retires exhausted accounts on rotation so they are never
  re-picked. Mode-B rotation is driven by stdout banners (not the quota API), so
  a simple in-memory allocator is sufficient — no CredentialPool needed.
- **LoginCoordinator**: sends ACCOUNT_LOGIN and awaits the matching
  ACCOUNT_LOGIN_RESULT over the event bus. Credentials are minted on the worker.
- **provision hook**: waits for the instance to run, runs the bootstrap pipeline
  over SSH, then waits for the worker's WS to connect.
- **wire_batch**: assembles the orchestrator with these hooks and routes
  RUN_EXHAUSTED / PROCESS_EXIT from workers back into it.
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import time
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Awaitable, Callable
from urllib.parse import urlparse

from elastic_agent.core.batch_orchestrator import (
    BatchOrchestrator,
    LoginOutcome,
    WorkerAssignment,
)
from elastic_agent.core.credential_pool import AccountDefinition
from elastic_agent.core.job_spec import JobSpec
from elastic_agent.core.manager_fleet_driver import ManagerFleetDriver
from elastic_agent.harness.base import Harness
from elastic_agent.harness.generic import (
    compile_bootstrap_steps,
    compile_job_setup_steps,
)

logger = logging.getLogger(__name__)


async def _await_cleanup_task(task: asyncio.Task) -> None:
    """Finish a tiny ownership cleanup even if the caller is cancelled."""
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            if task.done() and task.cancelled():
                raise
            continue
    task.result()


# ---------------------------------------------------------------------------
# Account allocation (in-memory, identity-only)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class AccountClaim:
    claim_id: str
    owner: str
    account: AccountDefinition


class AccountClaimConflictError(RuntimeError):
    """Identity mutation was attempted while a Job owns the account."""


class AccountAllocator:
    def __init__(self, account_store) -> None:
        self._store = account_store
        self._claims: dict[str, AccountClaim] = {}
        self._claim_by_account: dict[str, str] = {}
        self._by_owner: dict[str, set[str]] = {}
        self._quarantined_account_ids: set[str] = set()
        self._lock = asyncio.Lock()

    @asynccontextmanager
    async def mutation_guard(self, account_id: str) -> AsyncIterator[None]:
        """Serialize account CRUD with claim selection and reject live owners.

        The guard intentionally remains held while the caller re-reads and
        mutates ``AccountStore``.  ``reserve`` uses the same lock while reading
        that store, so a Job can only observe the complete old or new identity.
        """

        async with self._lock:
            claim_id = self._claim_by_account.get(account_id)
            if claim_id:
                claim = self._claims[claim_id]
                raise AccountClaimConflictError(
                    f"account {account_id!r} is actively claimed by {claim.owner!r}"
                )
            yield

    async def reserve(
        self, owner: str, group: str, *, account_id: str = "",
        claim_id: str = "", excluded_account_ids: set[str] | None = None,
        agent_type: str = "claude",
    ) -> AccountClaim | None:
        """Atomically claim an explicit account, or the next account in group.

        ``owner`` exists before a worker does (bound jobs use ``job:slot``), and
        ``claim_id`` gives cleanup a precise, idempotent release handle.  An
        explicit account still must exist and be enabled, but intentionally does
        not need to match ``group`` — explicit selection supersedes the pool
        filter.
        """
        async with self._lock:
            accounts = await self._store.list()
            if account_id:
                candidates = [
                    a for a in accounts
                    if a.id == account_id
                    and a.enabled
                    and a.agent_type == agent_type
                ]
            else:
                candidates = [
                    a for a in accounts
                    if a.enabled
                    and a.group == group
                    and a.agent_type == agent_type
                ]

            excluded = excluded_account_ids or set()
            account = next(
                (
                    a for a in candidates
                    if a.id not in self._claim_by_account
                    and a.id not in self._quarantined_account_ids
                    and a.id not in excluded
                ),
                None,
            )
            if account is None:
                return None

            cid = claim_id or f"claim-{uuid.uuid4().hex}"
            if cid in self._claims:
                raise ValueError(f"duplicate account claim id {cid}")
            claim = AccountClaim(claim_id=cid, owner=owner, account=account)
            self._claims[cid] = claim
            self._claim_by_account[account.id] = cid
            self._by_owner.setdefault(owner, set()).add(cid)
            return claim

    async def allocate(
        self, worker_id: str, group: str, *, agent_type: str = "claude",
    ) -> AccountDefinition | None:
        """Backward-compatible worker allocation used by unbound jobs.

        Each call returns a different account (so per_worker > 1 gets several) and
        the account stays assigned to the worker for the job's lifetime — an
        exhausted account is never re-picked because it remains assigned. Freed in
        bulk by :meth:`release_worker`.
        """
        claim = await self.reserve(worker_id, group, agent_type=agent_type)
        return claim.account if claim else None

    async def get_claim(self, claim_id: str) -> AccountClaim | None:
        async with self._lock:
            return self._claims.get(claim_id)

    async def release_claim(self, claim_id: str) -> None:
        async with self._lock:
            claim = self._claims.pop(claim_id, None)
            if claim is None:
                return
            self._claim_by_account.pop(claim.account.id, None)
            owner_claims = self._by_owner.get(claim.owner)
            if owner_claims is not None:
                owner_claims.discard(claim_id)
                if not owner_claims:
                    self._by_owner.pop(claim.owner, None)

    async def release_owner(self, owner: str) -> None:
        async with self._lock:
            claim_ids = list(self._by_owner.pop(owner, set()))
            for claim_id in claim_ids:
                claim = self._claims.pop(claim_id, None)
                if claim is not None:
                    self._claim_by_account.pop(claim.account.id, None)

    async def release_worker(self, worker_id: str) -> None:
        """Free all of a worker's accounts (e.g. on scale-in)."""
        await self.release_owner(worker_id)

    async def quarantine(self, account_id: str) -> None:
        """Keep an account unavailable after worker cleanup became uncertain."""
        async with self._lock:
            self._quarantined_account_ids.add(account_id)

    async def clear_quarantine(self, account_id: str) -> None:
        """Explicitly make an account selectable after external cleanup."""
        async with self._lock:
            self._quarantined_account_ids.discard(account_id)

    async def is_quarantined(self, account_id: str) -> bool:
        async with self._lock:
            return account_id in self._quarantined_account_ids


# ---------------------------------------------------------------------------
# Login coordination (ACCOUNT_LOGIN → await ACCOUNT_LOGIN_RESULT)
# ---------------------------------------------------------------------------


class LoginCoordinator:
    def __init__(
        self,
        connection_manager,
        event_bus,
        *,
        timeout: float = 2700.0,
        cancel_timeout: float = 60.0,
        quarantine_account: Callable[[str], Awaitable[None]] | None = None,
    ) -> None:
        self._conn = connection_manager
        self._timeout = timeout
        self._cancel_timeout = cancel_timeout
        self._quarantine_account = quarantine_account
        self._pending: dict[
            tuple[str, str], tuple[str, asyncio.Future, bool]
        ] = {}
        self._cancel_acks: dict[
            tuple[str, str], tuple[str, asyncio.Future]
        ] = {}
        self._otp_challenges: dict[str, dict[str, object]] = {}
        event_bus.subscribe("ACCOUNT_LOGIN_RESULT", self._on_result)
        event_bus.subscribe(
            "ACCOUNT_LOGIN_OTP_REQUIRED", self._on_otp_required
        )
        event_bus.subscribe("ACCOUNT_LOGIN_CANCELLED", self._on_cancelled)
        event_bus.subscribe("WORKER_DISCONNECTED", self._on_worker_disconnected)

    async def _on_cancelled(
        self, event_type: str, worker_id: str, data: dict,
    ) -> None:
        request_id = str(data.get("login_request_id") or "")
        pending = self._cancel_acks.get((worker_id, request_id))
        if pending is None:
            logger.warning(
                "Ignoring stale/unmatched login cleanup ACK from %s request %s",
                worker_id,
                request_id or "<missing>",
            )
            return
        expected_account_id, future = pending
        if data.get("account_id") != expected_account_id:
            logger.error(
                "Ignoring login cleanup ACK account mismatch for %s request %s",
                worker_id,
                request_id,
            )
            return
        if not future.done():
            future.set_result(bool(data.get("cleanup_complete")))

    async def _on_worker_disconnected(
        self, event_type: str, worker_id: str, data: dict,
    ) -> None:
        """End login waits immediately; a disconnected browser may still run."""
        for (pending_worker, request_id), (account_id, future, _legacy) in list(
            self._pending.items()
        ):
            if pending_worker != worker_id or future.done():
                continue
            future.set_result({
                "login_request_id": request_id,
                "account_id": account_id,
                "success": False,
                "error": "worker disconnected during account login",
                "cleanup_complete": False,
            })
            self._otp_challenges.pop(request_id, None)
        for (pending_worker, _request_id), (_account_id, future) in list(
            self._cancel_acks.items()
        ):
            if pending_worker == worker_id and not future.done():
                future.set_result(False)

    async def _quarantine_if_uncertain(
        self, account_id: str, *, enabled: bool, reason: str,
    ) -> None:
        if not enabled or self._quarantine_account is None:
            return
        try:
            await self._quarantine_account(account_id)
        except Exception:
            logger.exception(
                "Failed to quarantine account %s after %s",
                account_id,
                reason,
            )
            return
        logger.error(
            "Quarantined account %s because worker login cleanup was not confirmed (%s)",
            account_id,
            reason,
        )

    async def _on_otp_required(
        self, event_type: str, worker_id: str, data: dict,
    ) -> None:
        """Record a correlated, non-secret OTP challenge from a worker."""
        request_id = str(data.get("login_request_id") or "")
        challenge_id = str(data.get("challenge_id") or "")
        pending = self._pending.get((worker_id, request_id))
        if (
            pending is None
            or not request_id
            or not re.fullmatch(r"[0-9a-f]{32}", challenge_id)
        ):
            logger.warning(
                "Ignoring stale/unmatched OTP challenge from %s request %s",
                worker_id,
                request_id or "<missing>",
            )
            return
        expected_account_id, future, _allow_legacy = pending
        if (
            future.done()
            or data.get("account_id") != expected_account_id
        ):
            logger.warning(
                "Ignoring mismatched OTP challenge from %s request %s",
                worker_id,
                request_id,
            )
            return
        expires_at = int(data.get("expires_at") or 0)
        if expires_at <= int(time.time()):
            return
        self._otp_challenges[request_id] = {
            "login_request_id": request_id,
            "worker_id": worker_id,
            "account_id": expected_account_id,
            "challenge_id": challenge_id,
            "expires_at": expires_at,
            "status": "awaiting_otp",
        }

    def list_otp_challenges(self) -> list[dict[str, object]]:
        """Return live challenge metadata; passwords and OTPs never enter it."""
        now = int(time.time())
        stale = [
            request_id
            for request_id, challenge in self._otp_challenges.items()
            if int(challenge["expires_at"]) <= now
            or not any(key[1] == request_id for key in self._pending)
        ]
        for request_id in stale:
            self._otp_challenges.pop(request_id, None)
        return [dict(challenge) for challenge in self._otp_challenges.values()]

    async def submit_otp(
        self, login_request_id: str, challenge_id: str, code: str,
    ) -> dict[str, object]:
        """Forward one six-digit code to the worker that owns the challenge."""
        from elastic_agent.core.protocols.messages import AccountLoginOtpMessage

        normalized_code = code.strip()
        if not re.fullmatch(r"\d{6}", normalized_code):
            raise ValueError("verification code must be exactly 6 digits")
        challenge = self._otp_challenges.get(login_request_id)
        if challenge is None:
            raise KeyError("login challenge is not active")
        if challenge["challenge_id"] != challenge_id:
            raise ValueError("login challenge id does not match")
        if int(challenge["expires_at"]) <= int(time.time()):
            self._otp_challenges.pop(login_request_id, None)
            raise TimeoutError("login challenge has expired")

        worker_id = str(challenge["worker_id"])
        account_id = str(challenge["account_id"])
        if (worker_id, login_request_id) not in self._pending:
            self._otp_challenges.pop(login_request_id, None)
            raise KeyError("login request is no longer active")
        await self._conn.send_command(worker_id, AccountLoginOtpMessage(
            login_request_id=login_request_id,
            account_id=account_id,
            challenge_id=challenge_id,
            code=normalized_code,
        ))
        # Never retain the submitted code. A visibly rejected code causes the
        # worker to publish a fresh challenge with a fresh challenge_id.
        latest = self._otp_challenges.get(login_request_id)
        if latest is not None and latest.get("challenge_id") == challenge_id:
            self._otp_challenges.pop(login_request_id, None)
        return {
            "login_request_id": login_request_id,
            "account_id": account_id,
            "status": "verifying_otp",
        }

    async def _on_result(self, event_type: str, worker_id: str, data: dict) -> None:
        request_id = str(data.get("login_request_id") or "")
        pending = self._pending.get((worker_id, request_id))
        if pending is None and not request_id:
            # Compatibility is deliberately opt-in for non-EIP jobs only.  An
            # EIP job must use the current worker, whose correlated result also
            # proves the exact-email and warm-up checks ran.  Even for legacy
            # mode, never guess when a worker has concurrent login requests.
            worker_pending = [
                value
                for (pending_worker, _pending_id), value in self._pending.items()
                if pending_worker == worker_id
            ]
            if len(worker_pending) == 1:
                candidate = worker_pending[0]
                expected_account_id, _future, allow_legacy = candidate
                if (
                    allow_legacy
                    and data.get("account_id") == expected_account_id
                ):
                    pending = candidate
        if pending is None:
            logger.warning(
                "Ignoring stale/unmatched login result from %s request %s",
                worker_id,
                request_id or "<missing>",
            )
            return
        expected_account_id, fut, _allow_legacy = pending
        if data.get("account_id") != expected_account_id:
            logger.error(
                "Ignoring login result account mismatch for %s request %s",
                worker_id,
                request_id,
            )
            return
        if not fut.done():
            fut.set_result(data)
        self._otp_challenges.pop(request_id, None)

    async def login(
        self, worker_id: str, account: AccountDefinition, config_dir: str,
        provider: str | None = None, slot_index: int = 0,
        *, allow_legacy_result: bool = False,
        quarantine_on_uncertain_cleanup: bool = True,
    ) -> LoginOutcome:
        from elastic_agent.core.protocols.messages import (
            AccountLoginCancelMessage,
            AccountLoginMessage,
        )

        loop = asyncio.get_running_loop()
        fut: asyncio.Future = loop.create_future()
        request_id = f"login-{uuid.uuid4().hex}"
        key = (worker_id, request_id)

        async def cancel_worker(reason: str) -> bool:
            ack_future: asyncio.Future = loop.create_future()
            self._cancel_acks[key] = (account.id, ack_future)
            try:
                try:
                    await self._conn.send_command(
                        worker_id,
                        AccountLoginCancelMessage(
                            login_request_id=request_id,
                            account_id=account.id,
                            reason=reason,
                        ),
                    )
                except Exception:
                    # The worker may already be disconnected/terminated. Never
                    # let cleanup transport failure mask the original failure.
                    logger.warning(
                        "Could not cancel account login %s on worker %s",
                        request_id,
                        worker_id,
                    )
                    return False
                try:
                    return bool(await asyncio.wait_for(
                        asyncio.shield(ack_future),
                        timeout=self._cancel_timeout,
                    ))
                except asyncio.TimeoutError:
                    logger.error(
                        "Timed out waiting for login cleanup ACK %s from worker %s",
                        request_id,
                        worker_id,
                    )
                    return False
            finally:
                self._cancel_acks.pop(key, None)

        async def cancel_and_protect(reason: str) -> bool:
            cleanup_confirmed = await cancel_worker(reason)
            if not cleanup_confirmed:
                await self._quarantine_if_uncertain(
                    account.id,
                    enabled=quarantine_on_uncertain_cleanup,
                    reason=reason,
                )
            return cleanup_confirmed

        self._pending[key] = (account.id, fut, allow_legacy_result)
        try:
            await self._conn.send_command(worker_id, AccountLoginMessage(
                login_request_id=request_id,
                account_id=account.id, email=account.email, email_token=account.email_token,
                password=account.password, agent_type=account.agent_type,
                config_dir=config_dir, provider=provider, slot_index=slot_index,
            ))
            data = await asyncio.wait_for(fut, timeout=self._timeout)
        except asyncio.TimeoutError:
            await cancel_and_protect("manager_timeout")
            return LoginOutcome(success=False, account_id=account.id,
                                account_email=account.email, error="login timed out")
        except asyncio.CancelledError:
            cleanup_task = asyncio.create_task(
                cancel_and_protect("manager_cancelled")
            )
            await _await_cleanup_task(cleanup_task)
            raise
        except Exception as exc:  # pragma: no cover - defensive
            await cancel_and_protect("manager_cancelled")
            return LoginOutcome(success=False, account_id=account.id,
                                account_email=account.email,
                                error=(
                                    "account login transport failed "
                                    f"({type(exc).__name__})"
                                ))
        finally:
            self._pending.pop(key, None)
            self._cancel_acks.pop(key, None)
            self._otp_challenges.pop(request_id, None)

        if data.get("cleanup_complete") is False:
            await self._quarantine_if_uncertain(
                account.id,
                enabled=quarantine_on_uncertain_cleanup,
                reason="worker_disconnect",
            )

        return LoginOutcome(
            success=bool(data.get("success")),
            account_id=account.id,
            account_email=account.email,
            error=data.get("error"),
        )


# ---------------------------------------------------------------------------
# Hook factories
# ---------------------------------------------------------------------------


BootstrapRunner = Callable[[str, str, list, str, str | None], Awaitable[bool]]


def _default_manager_url(manager) -> str:
    env = os.environ.get("ELASTIC_AGENT_MANAGER_URL")
    if env:
        return env
    srv = manager.config.server
    return f"ws://{srv.host}:{srv.port}/ws/runtime"


def _account_login_transport_error(manager_url: str) -> str | None:
    """Require TLS before account-login secrets cross a host boundary."""
    parsed = urlparse(manager_url)
    if parsed.scheme == "wss":
        return None
    if parsed.scheme == "ws" and parsed.hostname in {
        "localhost", "127.0.0.1", "::1",
    }:
        return None
    if os.environ.get("ELASTIC_AGENT_ALLOW_INSECURE_ACCOUNT_LOGIN", "").lower() in {
        "1", "true", "yes",
    }:
        logger.warning(
            "Account login is explicitly allowing plaintext WebSocket transport: %s",
            manager_url,
        )
        return None
    return (
        "account login requires a wss:// Manager URL because login secrets "
        "cross the worker WebSocket; configure "
        "ELASTIC_AGENT_MANAGER_URL=wss://... (or explicitly set "
        "ELASTIC_AGENT_ALLOW_INSECURE_ACCOUNT_LOGIN=1 only on a trusted test network)"
    )


def _ssh_settings(manager) -> tuple[str, str | None]:
    pc = manager.config.provider
    ssh_user = manager.config.worker.ssh_user
    key = pc.aliyun.ssh_key_path if pc.type == "aliyun" else pc.aws.ssh_key_path
    return ssh_user, key


def make_provision_hook(
    manager,
    *,
    manager_url: str | None = None,
    include_pty: bool = False,
    ws_wait_timeout: float = 300.0,
    bootstrap_runner: BootstrapRunner | None = None,
):
    manager_url = manager_url or _default_manager_url(manager)
    ssh_user, ssh_key = _ssh_settings(manager)

    async def _run_bootstrap(node_id, host, steps, user, key) -> bool:
        from elastic_agent.core.bootstrap_handler import BootstrapHandler
        handler = BootstrapHandler(
            manager.config.bootstrap, manager.registry, manager.event_bus,
        )
        result = await handler.bootstrap_node(
            node_id=node_id, host=host, steps=steps, ssh_user=user, ssh_key_path=key,
        )
        return bool(result.success)

    runner = bootstrap_runner or _run_bootstrap
    # A custom runner means test/dry mode — skip the real SSH-readiness poll.
    real_provision = bootstrap_runner is None

    async def provision(worker_id: str, harness: Harness, spec: JobSpec) -> bool:
        node = await manager.registry.get(worker_id)
        if node is None:
            return False
        # Ensure the instance is running and has an address before SSH.
        host = node.public_ip
        try:
            inst = await manager.provider.wait_until_running(node.instance_id)
            # Bound attach already replaced registry.public_ip with the durable
            # EIP.  Never overwrite it with a possibly stale ephemeral address
            # returned by the launch/wait API.
            if getattr(spec.account, "binding", "none") != "eip":
                host = (inst.public_ip if inst else None) or host
        except Exception:
            logger.exception("provision: wait_until_running failed for %s", worker_id)
        if not host:
            logger.error("provision: no host address for %s", worker_id)
            return False

        # A freshly-booted instance isn't SSH-ready immediately — poll until it is.
        if real_provision and not await _wait_ssh_ready(host, ssh_user, ssh_key):
            logger.error("provision: %s never became SSH-ready", worker_id)
            return False

        # EIP and Codex jobs require this Manager's worker protocol and identity
        # checks.  An older PyPI worker neither understands Codex password/OTP
        # fields nor verifies the selected Codex identity, and could interpret
        # the message as a legacy Claude login.  Always deliver the currently
        # running package for these paths.  Do not honor a deployment override:
        # it could point at an older source tree that accepts request IDs but
        # lacks the exact-email/smoke-test enforcement.  Other non-EIP jobs may
        # still opt into a full source tree.
        framework_src = os.environ.get("ELASTIC_AGENT_FRAMEWORK_SRC")
        framework_target = "/opt/elastic-agent/framework/src"
        protocol_pinned = (
            spec.account.binding == "eip"
            or spec.account.agent_type == "codex"
        )
        if protocol_pinned:
            framework_src = str(Path(__file__).resolve().parents[1])
            framework_target += "/elastic_agent"

        steps = compile_bootstrap_steps(
            spec, manager_url=manager_url, auth_token=node.auth_token or "",
            worker_id=worker_id, include_pty=include_pty,
            runtime_from_src=bool(framework_src), run_as=ssh_user,
        )
        if not await runner(worker_id, host, steps, ssh_user, ssh_key):
            return False

        need_manager_rsync = spec.setup.deliver == "manager_rsync" and spec.setup.repo
        if need_manager_rsync or framework_src:
            from elastic_agent.core.bootstrap import SSHExecutor
            from elastic_agent.core.code_sync import ManagerCodeSync
            _sync = ManagerCodeSync(
                cache_dir=os.path.join(os.path.dirname(manager.collected_root), "repo_cache"),
                git_token=os.environ.get("ELASTIC_AGENT_GIT_TOKEN") or None,
                ssh_key=ssh_key, ssh_user=ssh_user,
            )

        # manager_rsync: clone on the Manager (token stays here) → rsync to the
        # worker (no token) → run setup commands on the worker.
        if need_manager_rsync:
            local = await _sync.ensure_clone(
                spec.setup.repo, spec.setup.checkout_ref,
            )
            if spec.setup.resolved_commit:
                proc = await asyncio.create_subprocess_exec(
                    "git", "-C", local, "rev-parse", "HEAD",
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                stdout, stderr = await proc.communicate()
                actual = stdout.decode(errors="replace").strip().lower()
                if proc.returncode != 0 or actual != spec.setup.resolved_commit:
                    logger.error(
                        "manager_rsync source commit mismatch for %s: "
                        "expected=%s actual=%s error=%s",
                        worker_id, spec.setup.resolved_commit, actual,
                        stderr.decode(errors="replace")[-200:],
                    )
                    return False
            if not await _sync.deliver(local, host, spec.setup.target_dir):
                logger.error("manager_rsync deliver failed for %s", worker_id)
                return False
            setup_steps = compile_job_setup_steps(
                spec,
                run_as=ssh_user,
                wrap_user=False,
                # manager_rsync strips .git; the immutable commit was verified
                # against the Manager checkout immediately above.
                include_source_manifest=False,
            )
            if setup_steps:
                # Run setup AS THE JOB USER (no sudo). The benchmark/run command
                # executes as ssh_user via the runtime; setup must share its HOME
                # so per-user installs land where run can find them. If setup were
                # sudo-wrapped (SSHExecutor's default for non-root users), things
                # like `curl uv/install.sh | sh` + `uv sync` would install into
                # /root/.local + a root-owned .venv, invisible to the ssh_user run
                # → `$HOME/.local/bin/uv: No such file or directory` at run time.
                ex = SSHExecutor(
                    host, user=ssh_user, key_path=ssh_key, use_sudo=False,
                )
                for setup_step in setup_steps:
                    succeeded = False
                    for _attempt in range(setup_step.retry_count + 1):
                        rc, _out, _err = await ex.execute(
                            setup_step.command,
                            timeout=setup_step.timeout,
                            env=setup_step.env or None,
                            cwd=setup_step.cwd,
                        )
                        if rc == 0:
                            succeeded = True
                            break
                    if not succeeded:
                        logger.error(
                            "manager_rsync setup step %s failed on %s "
                            "(rc=%s): %s",
                            setup_step.name, worker_id, rc, _err[-200:],
                        )
                        return False

        # Framework src → worker + systemd unit (runtime runs from src, survives
        # SSH disconnects). No token needed — it's a local directory.
        if framework_src:
            from elastic_agent.core.bootstrap_steps import runtime_deploy_from_src_step
            fw_dir = "/opt/elastic-agent/framework/src"
            if not await _sync.deliver(framework_src, host, framework_target):
                logger.error("framework rsync failed for %s", worker_id)
                return False
            step = runtime_deploy_from_src_step(
                manager_url=manager_url, auth_token=node.auth_token or "",
                worker_id=worker_id, src_dir=fw_dir, run_as=ssh_user,
                agent_type=spec.account.agent_type,
            )
            ex = SSHExecutor(host, user=ssh_user, key_path=ssh_key)
            rc, _out, _err = await ex.execute(step.command, timeout=step.timeout)
            if rc != 0:
                logger.error("framework runtime deploy (from src) failed on %s (rc=%s)", worker_id, rc)
                return False
            if protocol_pinned:
                # The service command has completed, but a baked/stale runtime
                # might have satisfied an earlier connection check.  Close the
                # current socket and require the source-pinned service to prove
                # it can reconnect before any account identity is sent.
                await manager.connection_manager.disconnect_worker(worker_id)

        # S3 datasets: the worker pulls them DIRECTLY from S3 with its instance-
        # profile credentials (no Manager download+rsync relay). The worker IAM
        # role (fanout provider's worker_instance_profile) grants S3 access; we
        # ensure awscli is present, then `aws s3 sync/cp` each dataset in place
        # before the run. Done on the worker so large datasets never transit the
        # Manager. (GitHub code delivery is unchanged — still manager_rsync.)
        if spec.setup.s3_datasets:
            from elastic_agent.core.bootstrap import SSHExecutor, _shell_quote
            ex = SSHExecutor(host, user=ssh_user, key_path=ssh_key, use_sudo=False)
            rc, _o, _e = await ex.execute(
                "command -v aws >/dev/null 2>&1 || "
                "(sudo apt-get update -qq && sudo apt-get install -y -qq awscli)",
                timeout=600,
            )
            if rc != 0:
                logger.error("awscli install failed on %s: %s", worker_id, _e[:200])
                return False
            for ds in spec.setup.s3_datasets:
                uri = ds.uri.strip()
                # trailing '/' → prefix (recursive sync); otherwise a single object.
                if uri.endswith("/"):
                    cmd = (f"mkdir -p {_shell_quote(ds.dest)} && "
                           f"aws s3 sync {_shell_quote(uri)} {_shell_quote(ds.dest)} --no-progress")
                else:
                    cmd = (f"mkdir -p $(dirname {_shell_quote(ds.dest)}) && "
                           f"aws s3 cp {_shell_quote(uri)} {_shell_quote(ds.dest)} --no-progress")
                rc, _o, _e = await ex.execute(cmd, timeout=3600)
                if rc != 0:
                    logger.error("s3 dataset pull failed on %s: %s (%s)", worker_id, uri, _e[:200])
                    return False

        return await _wait_ws_connected(manager, worker_id, ws_wait_timeout)

    return provision


def make_login_hook(manager, allocator: AccountAllocator, coordinator: LoginCoordinator):
    async def login(
        worker_id: str, spec: JobSpec, config_dir: str,
        account_id: str = "", claim_id: str = "",
    ) -> LoginOutcome:
        transport_error = _account_login_transport_error(
            _default_manager_url(manager)
        )
        if transport_error:
            return LoginOutcome(
                success=False,
                account_id=account_id,
                error=transport_error,
            )
        if claim_id:
            claim = await allocator.get_claim(claim_id)
            if claim is None:
                return LoginOutcome(
                    success=False,
                    account_id=account_id,
                    error=f"account claim '{claim_id}' is not active",
                )
            acct = claim.account
            if account_id and acct.id != account_id:
                return LoginOutcome(
                    success=False,
                    account_id=account_id,
                    error="account claim does not match bound assignment",
                )
        else:
            acct = await allocator.allocate(
                worker_id,
                spec.account.group,
                agent_type=spec.account.agent_type,
            )
            if acct is None:
                return LoginOutcome(
                    success=False,
                    error=f"no available account in group '{spec.account.group}'",
                )
        return await coordinator.login(
            worker_id, acct, config_dir or spec.account.config_dir,
            provider=spec.account.__dict__.get("provider") if hasattr(spec.account, "__dict__") else None,
            # A legacy Claude worker cannot perform Codex's password/OTP flow;
            # accepting its uncorrelated result could validate the wrong agent.
            allow_legacy_result=(
                spec.account.binding != "eip"
                and spec.account.agent_type == "claude"
            ),
            # EIP cleanup terminates the temporary instance before releasing
            # its durable claim. Ordinary workers remain alive, so uncertain
            # browser cleanup must quarantine the account from future jobs.
            quarantine_on_uncertain_cleanup=spec.account.binding != "eip",
        )

    return login


def make_bound_hooks(manager, allocator: AccountAllocator):
    """Create reserve/attach/release hooks for one-account/one-EIP jobs."""

    async def reserve_bound(
        job_id: str, slot: int, spec: JobSpec, account_id: str = "",
    ) -> WorkerAssignment:
        if not getattr(manager, "binding_recovery_ready", True):
            raise RuntimeError(
                "EIP binding recovery is still cleaning resources from a "
                "previous Manager run"
            )
        if spec.account.mode != "none":
            transport_error = _account_login_transport_error(
                _default_manager_url(manager)
            )
            if transport_error:
                # Validate before allocator claim/EIP reservation so an unsafe
                # transport cannot cause any billable side effect.
                raise ValueError(transport_error)
        provider_cfg = manager.config.provider
        if provider_cfg.type != "aws":
            raise ValueError("account.binding='eip' requires the AWS provider")
        manager_region = provider_cfg.aws.region
        region = spec.fanout.region or manager_region
        if region != manager_region:
            raise ValueError(
                f"EIP binding region '{region}' does not match Manager AWS region "
                f"'{manager_region}'"
            )

        owner = f"{job_id}:{slot}"
        attempted: set[str] = set()
        while True:
            claim = await allocator.reserve(
                owner,
                spec.account.group,
                account_id=account_id,
                excluded_account_ids=attempted,
                agent_type=spec.account.agent_type,
            )
            if claim is None:
                selector = f"account '{account_id}'" if account_id else (
                    f"an available account in group '{spec.account.group}'"
                )
                raise ValueError(f"could not reserve {selector}")

            acct = claim.account
            lease = None
            try:
                lease = await manager.binding_manager.reserve(
                    acct.id,
                    email=acct.email,
                    job_id=job_id,
                    slot=slot,
                    region=region,
                )
                binding = await manager.binding_manager.get_binding(acct.id)
                if binding is None:
                    raise RuntimeError(
                        f"binding disappeared for account '{acct.id}'"
                    )
                break
            except BaseException as exc:
                # Once ``reserve`` returns, the durable lease must be released
                # before its allocator claim.  Otherwise cancellation during
                # the following binding read makes the lease invisible to the
                # orchestrator while also allowing the identity to be claimed
                # again.  Both cleanups are shielded from repeated caller
                # cancellation; on durable cleanup failure, retain the claim
                # fail-closed for operator/startup recovery.
                if lease is not None:
                    durable_cleanup = asyncio.create_task(
                        manager.binding_manager.release(lease.lease_id)
                    )
                    try:
                        await _await_cleanup_task(durable_cleanup)
                    except BaseException as cleanup_exc:
                        logger.exception(
                            "Failed to roll back reserved EIP lease %s; "
                            "retaining account claim %s",
                            lease.lease_id,
                            claim.claim_id,
                        )
                        raise RuntimeError(
                            f"failed to roll back EIP lease {lease.lease_id!r}; "
                            "account claim retained"
                        ) from cleanup_exc

                claim_cleanup = asyncio.create_task(
                    allocator.release_claim(claim.claim_id)
                )
                await _await_cleanup_task(claim_cleanup)
                # Explicit selection is fail-fast.  Automatic group selection
                # skips an account whose durable lease/integrity state is busy
                # and tries the next identity instead of wedging the whole pool.
                from elastic_agent.core.account_binding import LeaseConflictError
                if account_id or not isinstance(exc, LeaseConflictError):
                    raise
                attempted.add(acct.id)
                logger.warning(
                    "Skipping unavailable EIP account %s: %s", acct.id, exc
                )

        return WorkerAssignment(
            slot=slot,
            job_id=job_id,
            account_id=acct.id,
            account_email=acct.email,
            claim_id=claim.claim_id,
            lease_id=lease.lease_id,
            eip_allocation_id=binding.eip_allocation_id or "",
            eip=binding.eip_ip or "",
            region=binding.region or region,
        )

    async def attach_bound(
        worker_id: str, assignment: WorkerAssignment,
    ) -> WorkerAssignment:
        node = await manager.registry.get(worker_id)
        if node is None:
            raise ValueError(f"worker '{worker_id}' disappeared before EIP attach")
        # RunInstances may return before the instance id is visible to
        # AssociateAddress.  Use the provider's eventual-consistency waiter
        # before touching the persistent EIP; provision will reuse the already
        # running machine afterward.
        await manager.provider.wait_until_running(node.instance_id)
        await manager.binding_manager.attach_instance(
            assignment.lease_id,
            node.instance_id,
            worker_id,
        )
        binding = await manager.binding_manager.get_binding(assignment.account_id)
        if binding is None or not binding.eip_ip:
            raise RuntimeError(
                f"EIP attach returned no public IP for account '{assignment.account_id}'"
            )
        metadata = dict(node.metadata)
        metadata.update({
            "job_id": assignment.job_id,
            "account_id": assignment.account_id,
            "lease_id": assignment.lease_id,
            "eip_allocation_id": binding.eip_allocation_id,
        })
        # Provision must resolve the newly-associated EIP, never the instance's
        # ephemeral launch address.
        await manager.registry.update(
            worker_id,
            public_ip=binding.eip_ip,
            metadata=metadata,
        )
        return replace(
            assignment,
            eip_allocation_id=binding.eip_allocation_id or "",
            eip=binding.eip_ip or "",
            region=binding.region or assignment.region,
        )

    async def release_bound(
        assignment: WorkerAssignment, worker_id: str | None,
    ) -> None:
        async def cleanup_worker(lease) -> None:
            target = worker_id or getattr(lease, "worker_id", "")
            if target:
                # BindingManager owns provider ordering: detach EIP first, then
                # terminate EC2, then invoke this callback.  The live hook only
                # mirrors that completed teardown into Manager control-plane
                # state; calling scale_in here would terminate before detach and
                # double-call the provider.
                from elastic_agent.core.registry import NodeStatus
                await manager.registry.update(target, status=NodeStatus.TERMINATED)
                await manager.connection_manager.disconnect_worker(target)

        await manager.binding_manager.release(
            assignment.lease_id,
            cleanup_worker=cleanup_worker,
        )
        # Keep the in-memory claim if durable cleanup failed.  Together with the
        # ERROR lease this prevents accidental same-process reuse until release
        # is retried successfully.
        await allocator.release_claim(assignment.claim_id)

    return reserve_bound, attach_bound, release_bound


async def _wait_ssh_ready(host: str, ssh_user: str, ssh_key: str | None, timeout: float = 240.0) -> bool:
    """Poll SSH until a fresh instance accepts connections."""
    args = ["ssh", "-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null",
            "-o", "ConnectTimeout=8", "-o", "BatchMode=yes"]
    if ssh_key:
        args += ["-i", ssh_key]
    args += [f"{ssh_user}@{host}", "echo ready"]
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        try:
            proc = await asyncio.create_subprocess_exec(
                *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
            out, _ = await proc.communicate()
            if proc.returncode == 0 and b"ready" in out:
                return True
        except Exception:
            pass
        await asyncio.sleep(5)
    return False


async def _wait_ws_connected(manager, worker_id: str, timeout: float) -> bool:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if manager.connection_manager.is_connected(worker_id):
            return True
        await asyncio.sleep(2.0)
    logger.error("provision: worker %s never connected within %ss", worker_id, timeout)
    return False


# ---------------------------------------------------------------------------
# Assembly + event routing
# ---------------------------------------------------------------------------


def wire_batch(
    manager,
    *,
    include_pty: bool = False,
    scale_in_on_complete: bool = True,
    login_timeout: float = 2700.0,
) -> BatchOrchestrator:
    """Build a fully-wired BatchOrchestrator and route worker events into it."""
    allocator = getattr(manager, "account_allocator", None)
    if allocator is None:
        # Lightweight test/deployment Managers predating the shared property.
        allocator = AccountAllocator(manager.account_store)
    coordinator = LoginCoordinator(
        manager.connection_manager,
        manager.event_bus,
        timeout=login_timeout,
        quarantine_account=allocator.quarantine,
    )
    manager._account_login_coordinator = coordinator
    bound_reserve, bound_attach, bound_release = make_bound_hooks(manager, allocator)
    driver = ManagerFleetDriver(
        manager,
        provision_hook=make_provision_hook(manager, include_pty=include_pty),
        login_hook=make_login_hook(manager, allocator, coordinator),
        bound_reserve_hook=bound_reserve,
        bound_attach_hook=bound_attach,
        bound_release_hook=bound_release,
    )
    orch = BatchOrchestrator(
        driver,
        scale_in_on_complete=scale_in_on_complete,
        persist_spec_hook=getattr(manager, "_persist_batch_job_spec", None),
        job_state_hook=getattr(manager, "_update_batch_job_state", None),
    )

    async def _on_exhausted(event_type, worker_id, data):
        # Claim ROTATING synchronously, then return so the connection layer can
        # ACK this durable event and resume the worker's sole WS receive loop.
        # A dynamic rotation's ACCOUNT_LOGIN_RESULT arrives on that same loop;
        # awaiting the login here would deadlock it until the 2700s timeout.
        orch.defer_exhausted(
            worker_id,
            task_id=data.get("task_id"),
        )

    async def _on_exit(event_type, worker_id, data):
        # Only route batch-owned workers; ignore other PROCESS_EXITs.
        if orch.job_id_for_worker(worker_id) is not None:
            await orch.handle_exit(
                worker_id, int(data.get("exit_code", -1)), task_id=data.get("task_id"),
            )

    async def _on_status(event_type, worker_id, data):
        if orch.job_id_for_worker(worker_id) is not None:
            active_or_pending = list(data.get("active_processes") or [])
            active_or_pending.extend(data.get("pending_process_exits") or [])
            await orch.reconcile_worker_status(
                worker_id, active_or_pending
            )

    async def _on_disconnect(event_type, worker_id, data):
        await orch.handle_disconnect(worker_id)

    async def _on_connect(event_type, worker_id, data):
        await orch.handle_reconnect(worker_id)

    manager.event_bus.subscribe("RUN_EXHAUSTED", _on_exhausted)
    manager.event_bus.subscribe("PROCESS_EXIT", _on_exit)
    manager.event_bus.subscribe("STATUS", _on_status)
    manager.event_bus.subscribe("WORKER_DISCONNECTED", _on_disconnect)
    manager.event_bus.subscribe("WORKER_CONNECTED", _on_connect)

    orch._allocator = allocator  # keep a handle for scale-in release / tests
    return orch
