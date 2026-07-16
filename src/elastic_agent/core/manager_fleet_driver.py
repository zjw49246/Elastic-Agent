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

import logging
from typing import Awaitable, Callable

from elastic_agent.core.batch_orchestrator import LoginOutcome
from elastic_agent.core.job_spec import JobSpec
from elastic_agent.harness.base import Harness

logger = logging.getLogger(__name__)

ProvisionHook = Callable[[str, Harness, JobSpec], Awaitable[bool]]
LoginHook = Callable[[str, JobSpec, str], Awaitable[LoginOutcome]]


async def _no_provision(worker_id: str, harness: Harness, spec: JobSpec) -> bool:
    raise NotImplementedError(
        "ManagerFleetDriver.provision requires a provision_hook (bootstrap pipeline) "
        "— wire it when deploying live batch runs"
    )


async def _no_login(worker_id: str, spec: JobSpec, config_dir: str) -> LoginOutcome:
    raise NotImplementedError(
        "ManagerFleetDriver.login requires a login_hook (allocate account + ACCOUNT_LOGIN) "
        "— wire it when deploying live batch runs"
    )


class ManagerFleetDriver:
    def __init__(
        self,
        manager,
        *,
        provision_hook: ProvisionHook | None = None,
        login_hook: LoginHook | None = None,
    ) -> None:
        self._mgr = manager
        self._provision = provision_hook or _no_provision
        self._login = login_hook or _no_login

    async def scale_out(self, count: int, name_prefix: str = "") -> list[str]:
        records = await self._mgr.scale_out(count=count, name_prefix=name_prefix or None)
        return [r.node_id for r in records]

    async def hostname_of(self, worker_id: str) -> str:
        node = await self._mgr.registry.get(worker_id)
        if node is None:
            return ""
        return node.metadata.get("hostname") or node.private_ip or ""

    async def provision(self, worker_id: str, harness: Harness, spec: JobSpec) -> bool:
        return await self._provision(worker_id, harness, spec)

    async def login(self, worker_id: str, spec: JobSpec, config_dir: str) -> LoginOutcome:
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
        """rsync the worker's results (collect.paths, or results/) into the
        Manager's collected/<job_id>/ — the S3 uploader then picks them up."""
        import asyncio
        import os

        node = await self._mgr.registry.get(worker_id)
        host = (node.public_ip or node.private_ip) if node else None
        if not host:
            return
        pc = self._mgr.config.provider
        ssh_user = self._mgr.config.worker.ssh_user
        ssh_key = pc.aliyun.ssh_key_path if pc.type == "aliyun" else pc.aws.ssh_key_path
        dest = os.path.join(self._mgr.collected_root, job_id)
        os.makedirs(dest, exist_ok=True)
        ssh = f"ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"
        if ssh_key:
            ssh += f" -i {ssh_key}"
        paths = spec.collect.paths or ["results"]
        for rel in paths:
            src = f"{ssh_user}@{host}:{spec.setup.target_dir.rstrip('/')}/{rel.rstrip('/')}/"
            proc = await asyncio.create_subprocess_exec(
                "rsync", "-az", "-e", ssh, src, f"{dest}/{rel.rstrip('/')}/",
                stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
            await proc.communicate()

    async def scale_in(self, worker_ids: list[str]) -> None:
        await self._mgr.scale_in(node_ids=list(worker_ids), force=False)
