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
from typing import Awaitable, Callable

from elastic_agent.core.batch_orchestrator import BatchOrchestrator, LoginOutcome
from elastic_agent.core.credential_pool import AccountDefinition
from elastic_agent.core.job_spec import JobSpec
from elastic_agent.core.manager_fleet_driver import ManagerFleetDriver
from elastic_agent.harness.base import Harness
from elastic_agent.harness.generic import compile_bootstrap_steps

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Account allocation (in-memory, identity-only)
# ---------------------------------------------------------------------------


class AccountAllocator:
    def __init__(self, account_store) -> None:
        self._store = account_store
        self._by_worker: dict[str, list[str]] = {}   # worker_id -> [account_id]
        self._assigned: set[str] = set()
        self._lock = asyncio.Lock()

    async def allocate(self, worker_id: str, group: str) -> AccountDefinition | None:
        """Give ``worker_id`` a fresh, distinct account in ``group``.

        Each call returns a different account (so per_worker > 1 gets several) and
        the account stays assigned to the worker for the job's lifetime — an
        exhausted account is never re-picked because it remains assigned. Freed in
        bulk by :meth:`release_worker`.
        """
        async with self._lock:
            for acct in await self._store.list():
                if acct.enabled and acct.group == group and acct.id not in self._assigned:
                    self._assigned.add(acct.id)
                    self._by_worker.setdefault(worker_id, []).append(acct.id)
                    return acct
            return None

    async def release_worker(self, worker_id: str) -> None:
        """Free all of a worker's accounts (e.g. on scale-in)."""
        async with self._lock:
            for acct_id in self._by_worker.pop(worker_id, []):
                self._assigned.discard(acct_id)


# ---------------------------------------------------------------------------
# Login coordination (ACCOUNT_LOGIN → await ACCOUNT_LOGIN_RESULT)
# ---------------------------------------------------------------------------


class LoginCoordinator:
    def __init__(self, connection_manager, event_bus, *, timeout: float = 300.0) -> None:
        self._conn = connection_manager
        self._timeout = timeout
        self._pending: dict[str, asyncio.Future] = {}   # account_id -> future
        event_bus.subscribe("ACCOUNT_LOGIN_RESULT", self._on_result)

    async def _on_result(self, event_type: str, worker_id: str, data: dict) -> None:
        fut = self._pending.get(data.get("account_id", ""))
        if fut is not None and not fut.done():
            fut.set_result(data)

    async def login(
        self, worker_id: str, account: AccountDefinition, config_dir: str,
        provider: str | None = None, slot_index: int = 0,
    ) -> LoginOutcome:
        from elastic_agent.core.protocols.messages import AccountLoginMessage

        loop = asyncio.get_running_loop()
        fut: asyncio.Future = loop.create_future()
        self._pending[account.id] = fut
        try:
            await self._conn.send_command(worker_id, AccountLoginMessage(
                account_id=account.id, email=account.email, email_token=account.email_token,
                config_dir=config_dir, provider=provider, slot_index=slot_index,
            ))
            data = await asyncio.wait_for(fut, timeout=self._timeout)
        except asyncio.TimeoutError:
            return LoginOutcome(success=False, account_id=account.id,
                                account_email=account.email, error="login timed out")
        except Exception as exc:  # pragma: no cover - defensive
            return LoginOutcome(success=False, account_id=account.id,
                                account_email=account.email, error=str(exc))
        finally:
            self._pending.pop(account.id, None)

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

    async def provision(worker_id: str, harness: Harness, spec: JobSpec) -> bool:
        node = await manager.registry.get(worker_id)
        if node is None:
            return False
        # Ensure the instance is running and has an address before SSH.
        host = node.public_ip
        try:
            inst = await manager.provider.wait_until_running(node.instance_id)
            host = (inst.public_ip if inst else None) or host
        except Exception:
            logger.exception("provision: wait_until_running failed for %s", worker_id)
        if not host:
            logger.error("provision: no host address for %s", worker_id)
            return False

        # A freshly-booted instance isn't SSH-ready immediately — poll until it is.
        if not await _wait_ssh_ready(host, ssh_user, ssh_key):
            logger.error("provision: %s never became SSH-ready", worker_id)
            return False

        # Deliver THIS branch's framework to the worker (not PyPI) when
        # ELASTIC_AGENT_FRAMEWORK_SRC is set — the last mile for one-click auto.
        framework_src = os.environ.get("ELASTIC_AGENT_FRAMEWORK_SRC")

        steps = compile_bootstrap_steps(
            spec, manager_url=manager_url, auth_token=node.auth_token or "",
            worker_id=worker_id, include_pty=include_pty,
            runtime_from_src=bool(framework_src),
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
            local = await _sync.ensure_clone(spec.setup.repo, spec.setup.branch)
            if not await _sync.deliver(local, host, spec.setup.target_dir):
                logger.error("manager_rsync deliver failed for %s", worker_id)
                return False
            if spec.setup.commands:
                ex = SSHExecutor(host, user=ssh_user, key_path=ssh_key)
                setup_cmd = f"cd {spec.setup.target_dir} && " + " && ".join(spec.setup.commands)
                rc, _out, _err = await ex.execute(setup_cmd, timeout=1200)
                if rc != 0:
                    logger.error("manager_rsync setup commands failed on %s (rc=%s)", worker_id, rc)
                    return False

        # Framework src → worker + systemd unit (runtime runs from src, survives
        # SSH disconnects). No token needed — it's a local directory.
        if framework_src:
            from elastic_agent.core.bootstrap_steps import runtime_deploy_from_src_step
            fw_dir = "/opt/elastic-agent/framework/src"
            if not await _sync.deliver(framework_src, host, fw_dir):
                logger.error("framework rsync failed for %s", worker_id)
                return False
            step = runtime_deploy_from_src_step(
                manager_url=manager_url, auth_token=node.auth_token or "",
                worker_id=worker_id, src_dir=fw_dir, run_as=ssh_user,
            )
            ex = SSHExecutor(host, user=ssh_user, key_path=ssh_key)
            rc, _out, _err = await ex.execute(step.command, timeout=step.timeout)
            if rc != 0:
                logger.error("framework runtime deploy (from src) failed on %s (rc=%s)", worker_id, rc)
                return False

        return await _wait_ws_connected(manager, worker_id, ws_wait_timeout)

    return provision


def make_login_hook(manager, allocator: AccountAllocator, coordinator: LoginCoordinator):
    async def login(worker_id: str, spec: JobSpec, config_dir: str) -> LoginOutcome:
        acct = await allocator.allocate(worker_id, spec.account.group)
        if acct is None:
            return LoginOutcome(success=False, error=f"no available account in group '{spec.account.group}'")
        return await coordinator.login(
            worker_id, acct, config_dir or spec.account.config_dir,
            provider=spec.account.__dict__.get("provider") if hasattr(spec.account, "__dict__") else None,
        )

    return login


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
    scale_in_on_complete: bool = False,
    login_timeout: float = 300.0,
) -> BatchOrchestrator:
    """Build a fully-wired BatchOrchestrator and route worker events into it."""
    allocator = AccountAllocator(manager.account_store)
    coordinator = LoginCoordinator(manager.connection_manager, manager.event_bus, timeout=login_timeout)
    driver = ManagerFleetDriver(
        manager,
        provision_hook=make_provision_hook(manager, include_pty=include_pty),
        login_hook=make_login_hook(manager, allocator, coordinator),
    )
    orch = BatchOrchestrator(driver, scale_in_on_complete=scale_in_on_complete)

    async def _on_exhausted(event_type, worker_id, data):
        await orch.handle_exhausted(data.get("worker_id") or worker_id)

    async def _on_exit(event_type, worker_id, data):
        # Only route batch-owned workers; ignore other PROCESS_EXITs.
        if orch.job_id_for_worker(worker_id) is not None:
            await orch.handle_exit(
                worker_id, int(data.get("exit_code", -1)), task_id=data.get("task_id"),
            )

    manager.event_bus.subscribe("RUN_EXHAUSTED", _on_exhausted)
    manager.event_bus.subscribe("PROCESS_EXIT", _on_exit)

    orch._allocator = allocator  # keep a handle for scale-in release / tests
    return orch
