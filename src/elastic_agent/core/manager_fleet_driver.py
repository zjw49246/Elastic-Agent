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
import base64
import hashlib
import json
import logging
import os
import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Awaitable, Callable
from urllib.parse import urlparse

from elastic_agent.core.batch_orchestrator import LoginOutcome, WorkerAssignment
from elastic_agent.core.job_spec import JobSpec
from elastic_agent.core.network import worker_management_host
from elastic_agent.core.secret_env import resolve_secret_env as resolve_aws_secret_env
from elastic_agent.harness.base import Harness

logger = logging.getLogger(__name__)

_COLLECTION_MANIFEST = "_elastic_agent/collection.json"


def _secret_env_transport_error(manager) -> str | None:
    """Reject plaintext cross-host delivery of resolved Job secrets.

    Secret references are deliberately resolved only at dispatch time, but the
    resulting values still cross the Manager/worker WebSocket in EXECUTE.env.
    Account-less jobs therefore need the same TLS boundary as account login.
    """
    manager_url = os.environ.get("ELASTIC_AGENT_MANAGER_URL")
    if not manager_url:
        server = manager.config.server
        manager_url = f"ws://{server.host}:{server.port}/ws/runtime"
    parsed = urlparse(manager_url)
    if parsed.scheme == "wss":
        return None
    if parsed.scheme == "ws" and parsed.hostname in {
        "localhost", "127.0.0.1", "::1",
    }:
        return None
    if os.environ.get("ELASTIC_AGENT_ALLOW_INSECURE_SECRET_ENV", "").lower() in {
        "1", "true", "yes",
    }:
        logger.warning(
            "Job secret_env is explicitly allowing plaintext WebSocket "
            "transport: %s",
            manager_url,
        )
        return None
    return (
        "run.secret_env requires a wss:// Manager URL because resolved secrets "
        "cross the worker WebSocket; configure ELASTIC_AGENT_MANAGER_URL=wss://... "
        "(or set ELASTIC_AGENT_ALLOW_INSECURE_SECRET_ENV=1 only on a trusted "
        "test network)"
    )

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

    async def resolve_secret_env(
        self, secret_env: dict[str, str],
    ) -> dict[str, str]:
        """Resolve opaque AWS references only at the dispatch boundary."""
        if secret_env:
            transport_error = _secret_env_transport_error(self._mgr)
            if transport_error:
                # Fail before calling Secrets Manager/SSM so plaintext never
                # enters Manager memory when it cannot be transported safely.
                raise ValueError(transport_error)
        return await resolve_aws_secret_env(secret_env)

    async def stop_command(
        self, worker_id: str, task_id: str, signal: str = "SIGTERM",
    ) -> None:
        """Stop the exact process owned by a Job cancellation request."""
        await self._mgr.connection_manager.stop_process(
            worker_id, task_id, sig=signal,
        )

    async def _collection_identity(
        self, worker_id: str, job_id: str, node=None,
    ) -> tuple[str, int | None]:
        """Return a stable, path-safe namespace for one Job fan-out slot.

        Live Jobs use their immutable ``shard_index``.  Startup recovery has no
        in-memory BatchJob, so it falls back to a collision-resistant worker-id
        component; this still prevents two workers from ever sharing a result
        prefix.
        """
        batch = getattr(self._mgr, "_batch", None)
        if batch is not None:
            job = batch.get_job(job_id)
            run = job.runs.get(worker_id) if job is not None else None
            shard_index = getattr(getattr(run, "ctx", None), "shard_index", None)
            if isinstance(shard_index, int) and shard_index >= 0:
                return f"shard-{shard_index:05d}", shard_index

        # EIP restart recovery reconstructs a registry node from the durable
        # lease, while the in-memory BatchJob no longer exists.  Recover the
        # original slot so a final retry lands in the same prefix as periodic
        # uploads made before the restart.
        lease_store = getattr(self._mgr, "account_binding_store", None)
        if lease_store is not None:
            lease_id = str(getattr(node, "metadata", {}).get("lease_id") or "")
            try:
                leases = await lease_store.list_leases()
            except Exception:  # pragma: no cover - fallback is still isolated
                logger.warning(
                    "cannot resolve result shard from durable leases for %s",
                    worker_id,
                    exc_info=True,
                )
            else:
                for lease in leases:
                    same_lease = lease_id and lease.lease_id == lease_id
                    same_worker = (
                        lease.job_id == job_id
                        and worker_id in {lease.worker_id, lease.instance_id}
                    )
                    if same_lease or same_worker:
                        return f"shard-{lease.slot:05d}", lease.slot

        safe = re.sub(r"[^A-Za-z0-9._-]+", "-", worker_id).strip(".-") or "worker"
        digest = hashlib.sha256(worker_id.encode("utf-8")).hexdigest()[:10]
        return f"worker-{safe[:48]}-{digest}", None

    @staticmethod
    def _s3_job_prefix(job_id: str, namespace: str) -> str:
        prefix = os.environ.get("ELASTIC_AGENT_RESULTS_S3_PREFIX", "jobs").strip("/")
        base = f"{prefix}/{job_id}" if prefix else job_id
        return f"{base}/workers/{namespace}"

    @staticmethod
    def _manifest_bytes(
        *, job_id: str, worker_id: str, namespace: str,
        shard_index: int | None, paths: list[str], destination: str,
    ) -> bytes:
        return (json.dumps({
            "schema_version": 1,
            "job_id": job_id,
            "worker_id": worker_id,
            "worker_namespace": namespace,
            "shard_index": shard_index,
            "paths": paths,
            "destination": destination,
            "collected_at": datetime.now(timezone.utc).isoformat(),
        }, sort_keys=True, indent=2) + "\n").encode("utf-8")

    @staticmethod
    def _write_manifest(path: Path, payload: bytes) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(prefix=".collection-", dir=path.parent)
        try:
            os.fchmod(fd, 0o600)
            with os.fdopen(fd, "wb") as fh:
                fd = -1
                fh.write(payload)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp_name, path)
        finally:
            if fd >= 0:
                os.close(fd)
            try:
                os.unlink(tmp_name)
            except FileNotFoundError:
                pass

    async def collect(self, worker_id: str, spec, job_id: str) -> None:
        """Push the worker's explicitly configured ``collect.paths`` to
        durable storage.  An empty path list is an intentional no-op.

        Worker-direct (AWS + worker_instance_profile + a results bucket): the
        WORKER recursively copies its outputs straight to
        ``s3://<bucket>/<prefix>/<job_id>/workers/<shard>/<rel>/`` using its
        instance-profile creds — no Manager relay, so large result sets never
        transit the Manager.

        Otherwise (fallback): rsync worker → Manager
        ``collected/<job_id>/workers/<shard>/``.  If an S3 bucket is configured,
        this method awaits that Job's Manager-side upload; upload failures are
        therefore collection failures rather than silent log messages.
        """

        paths = list(spec.collect.paths)
        if not paths:
            logger.debug(
                "result collection disabled for job %s worker %s: no paths",
                job_id,
                worker_id,
            )
            return

        node = await self._mgr.registry.get(worker_id)
        pc = self._mgr.config.provider
        host = worker_management_host(node, provider_type=pc.type)
        if not host:
            raise RuntimeError(
                f"cannot collect worker {worker_id!r}: no registry host address"
            )
        ssh_user = self._mgr.config.worker.ssh_user
        ssh_key = pc.aliyun.ssh_key_path if pc.type == "aliyun" else pc.aws.ssh_key_path
        namespace, shard_index = await self._collection_identity(
            worker_id, job_id, node,
        )

        bucket = os.environ.get("ELASTIC_AGENT_RESULTS_S3_BUCKET", "").strip()
        worker_direct = bool(pc.type == "aws" and pc.aws.worker_instance_profile and bucket)

        if worker_direct:
            from elastic_agent.core.bootstrap import SSHExecutor, _shell_quote
            ex = SSHExecutor(host, user=ssh_user, key_path=ssh_key, use_sudo=False)
            s3_root = self._s3_job_prefix(job_id, namespace)
            for rel in paths:
                r = rel.rstrip("/")
                src = f"{spec.setup.target_dir.rstrip('/')}/{r}/"
                uri = f"s3://{bucket}/{s3_root}/{r}/"
                rc, _stdout, stderr = await ex.execute(
                    "command -v aws >/dev/null 2>&1 || "
                    "(sudo apt-get update -qq && sudo apt-get install -y -qq awscli); "
                    # ``aws s3 sync`` can skip an in-place rewrite whose size
                    # and mtime were preserved.  Collection correctness is more
                    # important than that metadata shortcut: recursive cp
                    # uploads every current regular file, so the awaited final
                    # collect always refreshes S3 from the stopped worker.
                    f"aws s3 cp {_shell_quote(src)} {_shell_quote(uri)} "
                    "--recursive --no-follow-symlinks --no-progress",
                    timeout=1800,
                )
                if rc != 0:
                    raise RuntimeError(
                        f"worker S3 collect failed for {r!r} (rc={rc}): "
                        f"{stderr[-500:]}"
                    )
            manifest = self._manifest_bytes(
                job_id=job_id,
                worker_id=worker_id,
                namespace=namespace,
                shard_index=shard_index,
                paths=paths,
                destination="s3-worker-direct",
            )
            encoded = base64.b64encode(manifest).decode("ascii")
            manifest_uri = f"s3://{bucket}/{s3_root}/{_COLLECTION_MANIFEST}"
            rc, _stdout, stderr = await ex.execute(
                f"printf %s {_shell_quote(encoded)} | base64 -d | "
                f"aws s3 cp - {_shell_quote(manifest_uri)} --no-progress",
                timeout=300,
            )
            if rc != 0:
                raise RuntimeError(
                    "worker S3 collection manifest upload failed "
                    f"(rc={rc}): {stderr[-500:]}"
                )
            return

        dest = Path(self._mgr.collected_root) / job_id / "workers" / namespace
        dest.mkdir(parents=True, exist_ok=True)
        ssh = (
            "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "
            "-o BatchMode=yes -o ConnectTimeout=10 "
            "-o ServerAliveInterval=15 -o ServerAliveCountMax=2"
        )
        if ssh_key:
            ssh += f" -i {ssh_key}"
        for rel in paths:
            clean_rel = rel.rstrip("/")
            local_path = dest / clean_rel
            local_path.mkdir(parents=True, exist_ok=True)
            src = f"{ssh_user}@{host}:{spec.setup.target_dir.rstrip('/')}/{clean_rel}/"
            proc = await asyncio.create_subprocess_exec(
                # Checksum comparison catches same-size, same-mtime rewrites;
                # the Manager-side uploader then uses SHA-256 as its own
                # authoritative S3 deduplication key.
                "rsync", "-azc", "--safe-links", "-e", ssh, src,
                str(local_path) + "/",
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

        manifest = self._manifest_bytes(
            job_id=job_id,
            worker_id=worker_id,
            namespace=namespace,
            shard_index=shard_index,
            paths=paths,
            destination="manager-rsync",
        )
        self._write_manifest(dest / _COLLECTION_MANIFEST, manifest)

        if bucket:
            uploader = getattr(self._mgr, "_s3_uploader", None)
            if uploader is None:
                raise RuntimeError(
                    "ELASTIC_AGENT_RESULTS_S3_BUCKET is configured, but the "
                    "Manager S3 uploader is not initialized"
                )
            try:
                await asyncio.to_thread(uploader.sync_job, job_id)
            except Exception as exc:  # noqa: BLE001
                raise RuntimeError(
                    f"Manager S3 collect failed for job {job_id!r}: {exc}"
                ) from exc

    async def scale_in(self, worker_ids: list[str]) -> None:
        # Batch completion owns these ephemeral workers.  A graceful drain only
        # changes registry state and leaves the EC2 instance billable.
        terminated = await self._mgr.scale_in(
            node_ids=list(worker_ids), force=True,
        )
        # Job workers are disposable implementation details, not fleet history.
        # Keeping a TERMINATED NodeRecord for every shard makes the dashboard and
        # registry grow without bound across large fan-out workloads.  Cloud
        # termination has succeeded at this point; remove the disconnected
        # record through Manager's normal task/registry cleanup path.
        for worker_id in terminated:
            await self._mgr.remove_node(worker_id)


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
