"""ManagerFleetDriver — binds BatchOrchestrator to a live ElasticAgentManager.

Implements the parts of :class:`FleetDriver` that map cleanly onto the Manager
today: creating instances, reading a worker's hostname, dispatching the run
command, and tearing workers down. The two steps that depend on subsystems that
are provisioned per-deployment — bootstrap (SSH pipeline) and worker-local
account login (ACCOUNT_LOGIN + result correlation) — are injected as async hooks
so the wiring lives at the deployment boundary and this module stays honest:
without hooks, a live batch run raises a clear error instead of silently
pretending a worker was provisioned.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Awaitable, Callable

from elastic_agent.core.batch_orchestrator import LoginOutcome, WorkerAssignment
from elastic_agent.core.job_spec import JobSpec
from elastic_agent.harness.base import Harness

logger = logging.getLogger(__name__)

ProvisionHook = Callable[[str, Harness, JobSpec], Awaitable[bool]]
LoginHook = Callable[
    [str, JobSpec, str, str, str],
    Awaitable[LoginOutcome],
]
BoundReserveHook = Callable[[str, int, JobSpec, str], Awaitable[WorkerAssignment]]
BoundAttachHook = Callable[[str, WorkerAssignment], Awaitable[WorkerAssignment]]
BoundReleaseHook = Callable[[WorkerAssignment, str | None], Awaitable[None]]


async def _no_provision(worker_id: str, harness: Harness, spec: JobSpec) -> bool:
    raise NotImplementedError(
        "ManagerFleetDriver.provision requires a provision_hook (bootstrap pipeline) "
        "— wire it when deploying live batch runs"
    )


async def _no_login(
    worker_id: str, spec: JobSpec, config_dir: str,
    account_id: str = "", claim_id: str = "",
) -> LoginOutcome:
    raise NotImplementedError(
        "ManagerFleetDriver.login requires a login_hook (allocate account + ACCOUNT_LOGIN) "
        "— wire it when deploying live batch runs"
    )


async def _no_bound_reserve(
    job_id: str, slot: int, spec: JobSpec, account_id: str,
) -> WorkerAssignment:
    raise NotImplementedError(
        "ManagerFleetDriver.reserve_bound requires EIP binding hooks"
    )


async def _no_bound_attach(
    worker_id: str, assignment: WorkerAssignment,
) -> WorkerAssignment:
    raise NotImplementedError(
        "ManagerFleetDriver.attach_bound requires EIP binding hooks"
    )


async def _no_bound_release(
    assignment: WorkerAssignment, worker_id: str | None,
) -> None:
    raise NotImplementedError(
        "ManagerFleetDriver.release_bound requires EIP binding hooks"
    )


class ManagerFleetDriver:
    def __init__(
        self,
        manager,
        *,
        provision_hook: ProvisionHook | None = None,
        login_hook: LoginHook | None = None,
        bound_reserve_hook: BoundReserveHook | None = None,
        bound_attach_hook: BoundAttachHook | None = None,
        bound_release_hook: BoundReleaseHook | None = None,
    ) -> None:
        self._mgr = manager
        self._provision = provision_hook or _no_provision
        self._login = login_hook or _no_login
        self._bound_reserve = bound_reserve_hook or _no_bound_reserve
        self._bound_attach = bound_attach_hook or _no_bound_attach
        self._bound_release = bound_release_hook or _no_bound_release

    async def acquire_capacity(self, count: int) -> str:
        return await self._mgr.acquire_instance_capacity(count)

    async def release_capacity(self, reservation_id: str) -> None:
        await self._mgr.release_instance_capacity(reservation_id)

    async def scale_out(self, count: int, name_prefix: str = "",
                        instance_type: str = "", region: str = "",
                        disk_gb: int = 0, spot: bool = False,
                        tags: dict[str, str] | None = None) -> list[str]:
        records = await self._mgr.scale_out(
            count=count, name_prefix=name_prefix or None,
            instance_type=instance_type or None, region=region or None,
            disk_gb=disk_gb or None, spot=spot, tags=tags)
        return [r.node_id for r in records]

    async def reserve_bound(
        self, job_id: str, slot: int, spec: JobSpec, account_id: str = "",
    ) -> WorkerAssignment:
        return await self._bound_reserve(job_id, slot, spec, account_id)

    async def attach_bound(
        self, worker_id: str, assignment: WorkerAssignment,
    ) -> WorkerAssignment:
        return await self._bound_attach(worker_id, assignment)

    async def release_bound(
        self, assignment: WorkerAssignment, worker_id: str | None,
    ) -> None:
        await self._bound_release(assignment, worker_id)

    async def hostname_of(self, worker_id: str) -> str:
        node = await self._mgr.registry.get(worker_id)
        if node is None:
            return ""
        return node.metadata.get("hostname") or node.private_ip or ""

    async def provision(self, worker_id: str, harness: Harness, spec: JobSpec) -> bool:
        return await self._provision(worker_id, harness, spec)

    async def login(
        self, worker_id: str, spec: JobSpec, config_dir: str, *,
        account_id: str = "", claim_id: str = "",
    ) -> LoginOutcome:
        if account_id or claim_id:
            return await self._login(
                worker_id, spec, config_dir, account_id, claim_id,
            )
        # Preserve compatibility with deployment-specific legacy hooks that
        # accept only the original three arguments.
        return await self._login(worker_id, spec, config_dir)

    async def run_command(
        self, worker_id: str, task_id: str, command: list[str], cwd: str,
        env: dict[str, str], timeout: int | None, job_id: str, watch_exhaustion: bool,
    ) -> None:
        await self._mgr.connection_manager.execute(
            worker_id=worker_id,
            task_id=task_id,
            command=command,
            cwd=cwd,
            env=env,
            timeout=timeout,
            job_id=job_id,
            watch_exhaustion=watch_exhaustion,
        )

    async def collect(self, worker_id: str, spec, job_id: str) -> None:
        """Push the worker's results (collect.paths, or results/) to durable
        storage.

        Worker-direct (AWS + worker_instance_profile + a results bucket): the
        WORKER ``aws s3 sync``s its outputs straight to
        ``s3://<bucket>/jobs/<job_id>/<rel>/`` using its instance-profile creds —
        no Manager relay, so large result sets never transit the Manager. This
        matches the S3ResultUploader layout.

        Otherwise (fallback): rsync worker → Manager ``collected/<job_id>/`` and
        let the S3 uploader mirror it."""
        import os

        node = await self._mgr.registry.get(worker_id)
        host = (node.public_ip or node.private_ip) if node else None
        if not host:
            raise RuntimeError(
                f"cannot collect worker {worker_id!r}: no registry host address"
            )
        pc = self._mgr.config.provider
        ssh_user = self._mgr.config.worker.ssh_user
        ssh_key = pc.aliyun.ssh_key_path if pc.type == "aliyun" else pc.aws.ssh_key_path
        paths = spec.collect.paths or ["results"]

        bucket = os.environ.get("ELASTIC_AGENT_RESULTS_S3_BUCKET", "")
        worker_direct = bool(pc.type == "aws" and pc.aws.worker_instance_profile and bucket)

        if worker_direct:
            from elastic_agent.core.bootstrap import SSHExecutor, _shell_quote
            ex = SSHExecutor(host, user=ssh_user, key_path=ssh_key, use_sudo=False)
            for rel in paths:
                r = rel.rstrip("/")
                src = f"{spec.setup.target_dir.rstrip('/')}/{r}/"
                uri = f"s3://{bucket}/jobs/{job_id}/{r}/"
                rc, _stdout, stderr = await ex.execute(
                    "command -v aws >/dev/null 2>&1 || "
                    "(sudo apt-get update -qq && sudo apt-get install -y -qq awscli); "
                    f"aws s3 sync {_shell_quote(src)} {_shell_quote(uri)} --no-progress",
                    timeout=1800,
                )
                if rc != 0:
                    raise RuntimeError(
                        f"worker S3 collect failed for {r!r} (rc={rc}): "
                        f"{stderr[-500:]}"
                    )
            return

        dest = os.path.join(self._mgr.collected_root, job_id)
        os.makedirs(dest, exist_ok=True)
        ssh = (
            "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "
            "-o BatchMode=yes -o ConnectTimeout=10 "
            "-o ServerAliveInterval=15 -o ServerAliveCountMax=2"
        )
        if ssh_key:
            ssh += f" -i {ssh_key}"
        for rel in paths:
            src = f"{ssh_user}@{host}:{spec.setup.target_dir.rstrip('/')}/{rel.rstrip('/')}/"
            proc = await asyncio.create_subprocess_exec(
                "rsync", "-az", "-e", ssh, src, f"{dest}/{rel.rstrip('/')}/",
                stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE)
            try:
                _stdout, stderr = await asyncio.wait_for(
                    proc.communicate(), timeout=1800
                )
            except asyncio.TimeoutError as exc:
                await _terminate_subprocess(proc)
                raise RuntimeError(
                    f"rsync collect timed out for {rel!r} on {worker_id!r}"
                ) from exc
            except asyncio.CancelledError:
                # Startup recovery wraps collection in a much shorter deadline.
                # asyncio.wait_for cancels this coroutine on expiry; explicitly
                # reap rsync/ssh so it cannot survive Manager shutdown and keep
                # talking to an EIP after that lease is released.
                await _terminate_subprocess(proc)
                raise
            if proc.returncode != 0:
                detail = (stderr or b"").decode(errors="replace")[-500:]
                raise RuntimeError(
                    f"rsync collect failed for {rel!r} (rc={proc.returncode}): "
                    f"{detail}"
                )

    async def scale_in(self, worker_ids: list[str]) -> None:
        await self._mgr.scale_in(node_ids=list(worker_ids), force=False)


async def _terminate_subprocess(proc: asyncio.subprocess.Process) -> None:
    """Best-effort terminate and reap a collection subprocess."""
    if proc.returncode is not None:
        return
    try:
        proc.terminate()
    except ProcessLookupError:
        return
    try:
        await asyncio.wait_for(proc.wait(), timeout=5)
    except asyncio.TimeoutError:
        try:
            proc.kill()
        except ProcessLookupError:
            return
        await proc.wait()
