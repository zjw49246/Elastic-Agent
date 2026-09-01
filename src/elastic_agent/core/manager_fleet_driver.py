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
import shlex
import shutil
import signal
import stat
import tempfile
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Awaitable, Callable
from urllib.parse import urlparse

from elastic_agent.core.batch_orchestrator import LoginOutcome, WorkerAssignment
from elastic_agent.core.checkpoint_store import (
    IncompleteCheckpointSetError,
    S3CheckpointStore,
)
from elastic_agent.core.job_spec import JobSpec, WorkerContext
from elastic_agent.core.job_spec_store import load_job_spec_journal
from elastic_agent.core.network import worker_management_host
from elastic_agent.core.secret_env import resolve_secret_env as resolve_aws_secret_env
from elastic_agent.core.secure_store import (
    atomic_write_private,
    fsync_directory,
    secure_state_directory,
)
from elastic_agent.harness.base import Harness
from elastic_agent.worker.recovery_transaction import (
    descriptor_sha256 as recovery_descriptor_sha256,
)
from elastic_agent.worker.recovery_transaction import (
    encode_identity as encode_recovery_transaction_identity,
)
from elastic_agent.worker.recovery_transaction import (
    encode_payload as encode_recovery_transaction_payload,
)
from elastic_agent.worker.recovery_transaction import (
    staged_path as recovery_staged_path,
)

logger = logging.getLogger(__name__)

_COLLECTION_MANIFEST = "_elastic_agent/collection.json"
_INTERRUPT_CLEANUP_PROOF_SCHEMA = 1
_TERMINAL_JOB_STATES = frozenset({
    "succeeded",
    "failed",
    "cancelled",
    "suspended",
})
_SAFE_JOB_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")
_DEFAULT_MAX_RECOVERY_STAGING_BYTES = 20 * 1024 * 1024 * 1024
_DEFAULT_MAX_RECOVERY_STAGING_OBJECTS = 500_000
_RECOVERY_FREE_SPACE_RESERVE_BYTES = 1024 * 1024 * 1024
_RECOVERY_FREE_INODE_RESERVE = 10_000
_RECOVERY_ALLOCATION_FLOOR_BYTES = 4096
_DEFAULT_MAX_COLLECTION_STAGING_BYTES = 20 * 1024 * 1024 * 1024
_DEFAULT_MAX_COLLECTION_STAGING_OBJECTS = 100_000
_DEFAULT_MAX_COLLECTION_FILE_BYTES = 20 * 1024 * 1024 * 1024
_COLLECTION_FREE_SPACE_RESERVE_BYTES = 1024 * 1024 * 1024
_COLLECTION_FREE_INODE_RESERVE = 10_000
_COLLECTION_MONITOR_INTERVAL_SECONDS = 0.25
_COLLECTION_VANISHED_SOURCE_RETRIES = 2
_S3_COLLECTION_DEADLINE_SECONDS = 1_800
_COLLECTION_STAGING_RESERVATION_WAIT_SECONDS = 1_800
_RECOVERY_STAGING_DEADLINE_SECONDS = 1800
_RECOVERY_PREFLIGHT_DEADLINE_SECONDS = 30
_RECOVERY_CONTRACT_VERSION = 3
_LEGACY_RECOVERY_CONTRACT_VERSION = 2
_WORKER_RECOVERY_DISK_RESERVE_BYTES = 10 * 1024 * 1024 * 1024
_SUBPROCESS_TERMINATE_TIMEOUT_SECONDS = 5
_SUBPROCESS_GROUP_REAP_TIMEOUT_SECONDS = 5
_SUBPROCESS_GROUP_POLL_SECONDS = 0.05
_RECOVERY_TRANSFER_ENV = "ELASTIC_AGENT_RECOVERY_TRANSFER_ID"
_RECOVERY_TRANSFER_SCHEMA_VERSION = 1
_RSYNC_VANISHED_ENTRY = re.compile(
    r"^(?:file|directory) has vanished: .+$",
)
_RSYNC_VANISHED_SUMMARY = re.compile(
    r"^rsync warning: some files vanished before they could be transferred "
    r"\(code 24\) at .+$",
)
_SSH_NEW_HOST_WARNING = re.compile(
    r"^Warning: Permanently added .+ to the list of known hosts\.$",
)
_RECOVERY_PROCESS_SCANNER = r"""
import base64
import json
import os
import pwd
import signal
import stat
import sys
import time

request = json.loads(base64.b64decode(sys.argv[1], validate=True))
target = request["target_dir"]
run_user = request["run_user"]
if not isinstance(target, str) or not target.startswith("/") or target == "/":
    raise SystemExit("invalid recovery process scan target")
normalized = os.path.normpath(target)
if normalized != target.rstrip("/"):
    raise SystemExit("invalid recovery process scan target")
current = "/"
for part in normalized.strip("/").split("/"):
    current = os.path.join(current, part)
    mode = os.lstat(current).st_mode
    if stat.S_ISLNK(mode) or not stat.S_ISDIR(mode):
        raise SystemExit("recovery process scan target has an unsafe path chain")
target = os.path.realpath(normalized)
run_uid = pwd.getpwnam(run_user).pw_uid
container_tokens = (
    "dockerd",
    "containerd",
    "containerd-shim",
    "docker-proxy",
    "buildkitd",
    "buildkit-runc",
    "runc",
    "crun",
    "conmon",
)

def ancestors():
    result = set()
    pid = os.getpid()
    while pid > 1 and pid not in result:
        result.add(pid)
        try:
            status = open(
                f"/proc/{pid}/status", encoding="utf-8",
            ).read().splitlines()
        except FileNotFoundError:
            break
        parent = next(
            (
                int(line.split()[1])
                for line in status
                if line.startswith("PPid:")
            ),
            0,
        )
        pid = parent
    return result

def starttime(pid):
    try:
        tail = open(
            f"/proc/{pid}/stat", encoding="utf-8",
        ).read().rsplit(")", 1)[1].split()
        return int(tail[19])
    except (FileNotFoundError, ProcessLookupError):
        return None
    except (OSError, IndexError, ValueError) as exc:
        raise SystemExit(
            f"cannot prove process identity for pid {pid}: {exc}"
        )

def reap_job_user_processes():
    excluded = ancestors()
    targets = []
    for name in os.listdir("/proc"):
        if not name.isdigit():
            continue
        pid = int(name)
        if pid in excluded:
            continue
        try:
            status = open(
                f"/proc/{pid}/status", encoding="utf-8",
            ).read().splitlines()
            uid = int(next(
                line.split()[1]
                for line in status
                if line.startswith("Uid:")
            ))
        except (FileNotFoundError, ProcessLookupError):
            continue
        except (OSError, StopIteration, ValueError) as exc:
            raise SystemExit(
                f"cannot inspect process ownership for pid {pid}: {exc}"
            )
        if uid == run_uid:
            identity = starttime(pid)
            if identity is not None:
                targets.append((pid, identity))
    for pid, _identity in targets:
        try:
            os.kill(pid, signal.SIGTERM)
        except (ProcessLookupError, FileNotFoundError):
            pass
    time.sleep(2)
    for pid, identity in targets:
        if starttime(pid) != identity:
            continue
        try:
            os.kill(pid, signal.SIGKILL)
        except (ProcessLookupError, FileNotFoundError):
            pass
    time.sleep(1)

def contained(path):
    try:
        actual = os.path.realpath(path)
        return os.path.commonpath((target, actual)) == target
    except (OSError, ValueError):
        return False

def inspect_once():
    excluded = ancestors()
    offenders = []
    for name in os.listdir("/proc"):
        if not name.isdigit():
            continue
        pid = int(name)
        if pid in excluded:
            continue
        proc = f"/proc/{pid}"
        try:
            status = open(
                f"{proc}/status", encoding="utf-8",
            ).read().splitlines()
            state = next(
                line.split()[1]
                for line in status
                if line.startswith("State:")
            )
            uid = int(next(
                line.split()[1]
                for line in status
                if line.startswith("Uid:")
            ))
            comm = open(
                f"{proc}/comm", encoding="utf-8",
            ).read().strip().lower()
        except (FileNotFoundError, ProcessLookupError):
            continue
        except (OSError, StopIteration, ValueError) as exc:
            raise SystemExit(
                f"cannot prove process quiescence for pid {pid}: {exc}"
            )
        if state == "Z":
            continue
        reasons = []
        if uid == run_uid:
            reasons.append("job-user")
        if any(token in comm for token in container_tokens):
            reasons.append("container-runtime")
        for link in ("cwd", "root", "exe"):
            try:
                value = os.readlink(f"{proc}/{link}")
            except (FileNotFoundError, ProcessLookupError):
                continue
            except OSError as exc:
                raise SystemExit(
                    f"cannot inspect process {pid} {link}: {exc}"
                )
            if contained(value):
                reasons.append(link)
        try:
            descriptors = os.listdir(f"{proc}/fd")
        except (FileNotFoundError, ProcessLookupError):
            descriptors = []
        except OSError as exc:
            raise SystemExit(
                f"cannot inspect process {pid} descriptors: {exc}"
            )
        for descriptor in descriptors:
            try:
                value = os.readlink(f"{proc}/fd/{descriptor}")
            except (FileNotFoundError, ProcessLookupError):
                continue
            except OSError as exc:
                raise SystemExit(
                    f"cannot inspect process {pid} descriptor: {exc}"
                )
            if contained(value):
                reasons.append("fd")
                break
        if reasons:
            offenders.append(
                f"{pid}:{comm}:{','.join(sorted(set(reasons)))}"
            )
    if offenders:
        raise SystemExit(
            "recovered worker still has mutating processes: "
            + "; ".join(offenders[:20])
        )

reap_job_user_processes()
inspect_once()
time.sleep(1)
inspect_once()
"""


class _UnsettledSubprocessError(RuntimeError):
    """A child may still be able to mutate its Manager-local staging tree."""


def _positive_env_bytes(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be a positive integer") from exc
    if value <= 0:
        raise RuntimeError(f"{name} must be a positive integer")
    return value


def _checkpoint_s3_prefix() -> str:
    """Keep internal generations outside each public result Job prefix."""

    configured = os.environ.get(
        "ELASTIC_AGENT_CHECKPOINT_S3_PREFIX", "",
    ).strip("/")
    if configured:
        return configured
    results = os.environ.get(
        "ELASTIC_AGENT_RESULTS_S3_PREFIX", "jobs",
    ).strip("/")
    return (
        f"{results}/.elastic-agent-checkpoints"
        if results
        else ".elastic-agent-checkpoints"
    )


async def _await_owned_thread(
    callable_,
    /,
    *args,
    cancellation_event: threading.Event | None = None,
    **kwargs,
):
    """Do not abandon an S3/filesystem transaction in a cancelled thread.

    ``asyncio.to_thread`` cannot stop its underlying operation.  Propagate
    cancellation only after the operation has settled, so a late COMMITTED
    write cannot race instance teardown or staging cleanup.
    """

    task = asyncio.create_task(
        asyncio.to_thread(callable_, *args, **kwargs)
    )
    cancellation: asyncio.CancelledError | None = None
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError as exc:
            if cancellation is None:
                cancellation = exc
                if cancellation_event is not None:
                    cancellation_event.set()
        except BaseException:
            # The owned operation itself settled with an error. Read it from
            # task.result() below so an already-recorded caller cancellation
            # remains the primary control-flow signal.
            break
    operation_error: BaseException | None = None
    result = None
    try:
        result = task.result()
    except BaseException as exc:  # preserve cancellation as the primary signal
        operation_error = exc
    if cancellation is not None:
        if operation_error is not None:
            raise cancellation from operation_error
        raise cancellation
    if operation_error is not None:
        raise operation_error
    return result


async def _await_owned_task(
    task: asyncio.Task,
    *,
    operation_error_wins: bool = False,
):
    """Wait for an independently owned task despite repeated cancellation."""

    cancellation: asyncio.CancelledError | None = None
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError as exc:
            if task.done() and task.cancelled():
                break
            if cancellation is None:
                cancellation = exc
        except BaseException:
            # Read the concrete child failure from task.result() below.
            break
    operation_error: BaseException | None = None
    result = None
    try:
        result = task.result()
    except BaseException as exc:
        operation_error = exc
    if operation_error_wins and operation_error is not None:
        if cancellation is not None:
            raise operation_error from cancellation
        raise operation_error
    if cancellation is not None:
        if operation_error is not None:
            raise cancellation from operation_error
        raise cancellation
    if operation_error is not None:
        raise operation_error
    return result


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


def _rsync_reported_only_vanished_sources(stderr: bytes) -> bool:
    """Recognize rsync's narrow exit-24 vanished-source diagnostic.

    Exit 24 is retryable, not successful: the caller must run a fresh rsync
    pass and receive rc=0 before it can publish the staging tree. Requiring the
    per-entry diagnostic and the canonical summary prevents an unrelated
    partial-transfer error from entering that retry path.
    """

    try:
        lines = [
            line.strip()
            for line in stderr.decode("utf-8", errors="strict").splitlines()
            if line.strip()
        ]
    except UnicodeDecodeError:
        return False
    saw_entry = False
    saw_summary = False
    for line in lines:
        if _RSYNC_VANISHED_ENTRY.fullmatch(line):
            saw_entry = True
        elif _RSYNC_VANISHED_SUMMARY.fullmatch(line):
            saw_summary = True
        elif _SSH_NEW_HOST_WARNING.fullmatch(line):
            # The collection transport intentionally uses an isolated
            # known-hosts file, so OpenSSH may emit this on every connection.
            continue
        else:
            return False
    return saw_entry and saw_summary


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
        # A successful cloud termination and registry removal are one ordered
        # transaction. Retain the exact proof in memory if registry cleanup
        # raises so a retry does not ask Manager to rediscover an already-removed
        # node and then mistake an empty response for success.
        self._scale_in_state_lock = asyncio.Lock()
        self._scale_in_locks: dict[str, asyncio.Lock] = {}
        self._scale_in_lock_users: dict[str, int] = {}
        self._proven_terminated_workers: set[str] = set()
        self._proven_removed_workers: set[str] = set()
        # A completed proof is needed only while an overlapping caller is
        # already queued on the same per-worker lock.  The last lock user drops
        # it, so successful Jobs do not leave an ever-growing tombstone set.
        self._completed_scale_in_workers: set[str] = set()

    async def acquire_capacity(self, count: int) -> str:
        return await self._mgr.acquire_instance_capacity(count)

    async def release_capacity(self, reservation_id: str) -> None:
        await self._mgr.release_instance_capacity(reservation_id)

    async def launch_admission_ready(self, spec: JobSpec) -> bool:
        """Keep credential-bearing launches behind startup recovery.

        Agent API credentials are intentionally unavailable until the Manager
        proves that workers from its previous process no longer own delegated
        access.  Apply that same fence before ``scale_out`` so a queued Job does
        not create a billable worker that is guaranteed to fail at login and
        add another instance to the recovery set.
        """

        if getattr(self._mgr, "binding_recovery_ready", True):
            return True
        account = spec.account
        if account.mode == "none":
            return True
        if account.binding == "eip" or account.auth_kind == "agent_api":
            return False
        if account.auth_kind == "oauth" or not account.ids:
            return True
        store = getattr(self._mgr, "agent_api_store", None)
        if store is None:
            return True
        api_ids = {candidate.id for candidate in await store.list()}
        return not any(account_id in api_ids for account_id in account.ids)

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

    async def record_bound_interrupt_proof(
        self,
        assignment: WorkerAssignment,
        worker_id: str | None,
        *,
        collected: bool,
        collection_error: str | None,
    ) -> None:
        """Fence EIP destruction behind a durable per-shard result proof."""

        store = getattr(self._mgr, "account_binding_store", None)
        if store is None:
            raise RuntimeError(
                "bound interrupt cleanup requires a durable lease store"
            )
        lease = await store.get_lease(assignment.lease_id)
        if lease is None:
            raise RuntimeError(
                f"durable lease {assignment.lease_id!r} disappeared before "
                "interrupt cleanup proof"
            )
        if (
            lease.account_id != assignment.account_id
            or lease.job_id != assignment.job_id
            or lease.slot != assignment.slot
            or (
                worker_id
                and lease.worker_id
                and lease.worker_id != worker_id
            )
        ):
            raise RuntimeError(
                "bound interrupt cleanup proof conflicts with durable lease "
                "identity"
            )
        updated = await store.update_lease(
            assignment.lease_id,
            recovery_collection_attempted=True,
            recovery_collected=bool(collected),
            recovery_collection_error=(
                str(collection_error)[:2_000]
                if collection_error
                else None
            ),
        )
        if (
            updated is None
            or updated.lease_id != assignment.lease_id
            or updated.account_id != assignment.account_id
            or updated.job_id != assignment.job_id
            or updated.slot != assignment.slot
            or not updated.recovery_collection_attempted
        ):
            raise RuntimeError(
                "durable lease did not confirm bound interrupt cleanup proof"
            )

    async def hostname_of(self, worker_id: str) -> str:
        node = await self._mgr.registry.get(worker_id)
        if node is None:
            return ""
        return node.metadata.get("hostname") or node.private_ip or ""

    async def provision(self, worker_id: str, harness: Harness, spec: JobSpec) -> bool:
        return await self._provision(worker_id, harness, spec)

    def _checkpoint_store(self) -> S3CheckpointStore:
        existing = getattr(self._mgr, "_checkpoint_store", None)
        if existing is not None:
            return existing
        bucket = os.environ.get(
            "ELASTIC_AGENT_RESULTS_S3_BUCKET", "",
        ).strip()
        if not bucket:
            raise RuntimeError(
                "checkpoint recovery requires "
                "ELASTIC_AGENT_RESULTS_S3_BUCKET"
            )
        provider = self._mgr.config.provider
        region = (
            getattr(provider.aws, "region", "ap-northeast-1")
            if provider.type == "aws"
            else "ap-northeast-1"
        )
        store = S3CheckpointStore(
            bucket=bucket,
            prefix=_checkpoint_s3_prefix(),
            region=region,
            snapshot_root=self._checkpoint_snapshot_root(),
        )
        self._mgr._checkpoint_store = store
        return store

    def _recovery_staging_root(self) -> Path:
        registry_path = Path(
            self._mgr.config.registry.path,
        ).expanduser()
        return secure_state_directory(
            registry_path.with_name("recovery-staging")
        )

    def _checkpoint_snapshot_root(self) -> Path:
        registry_path = Path(
            self._mgr.config.registry.path,
        ).expanduser()
        return secure_state_directory(
            registry_path.with_name("checkpoint-snapshots")
        )

    def _recovery_transfer_root(self) -> Path:
        registry_path = Path(
            self._mgr.config.registry.path,
        ).expanduser()
        return secure_state_directory(
            registry_path.with_name("recovery-transfers")
        )

    def _recovery_transfer_job_root(
        self,
        job_id: str,
        *,
        create: bool,
    ) -> Path:
        if _SAFE_JOB_ID.fullmatch(job_id) is None:
            raise RuntimeError("invalid recovery transfer Job id")
        root = self._recovery_transfer_root()
        target = root / job_id
        if target.is_symlink():
            raise RuntimeError("unsafe recovery transfer journal")
        if target.exists() and not target.is_dir():
            raise RuntimeError("unsafe recovery transfer journal")
        if create:
            secure_state_directory(target)
        return target

    @staticmethod
    def _process_identity(
        pid: int,
    ) -> tuple[int, int, int] | None:
        """Return Linux ``(pgrp, session, starttime)`` without PID races."""

        try:
            raw = Path(f"/proc/{pid}/stat").read_text(
                encoding="utf-8",
            )
        except (FileNotFoundError, ProcessLookupError):
            return None
        except OSError as exc:
            raise _UnsettledSubprocessError(
                f"cannot inspect recovery transfer pid {pid}"
            ) from exc
        try:
            fields = raw.rsplit(")", 1)[1].split()
            return int(fields[2]), int(fields[3]), int(fields[19])
        except (IndexError, ValueError) as exc:
            raise _UnsettledSubprocessError(
                f"invalid recovery transfer process identity for pid {pid}"
            ) from exc

    @classmethod
    def _transfer_process_groups(cls, token: str) -> set[int]:
        """Find sessions inheriting a pre-spawn durable transfer token."""

        marker = (
            f"{_RECOVERY_TRANSFER_ENV}={token}".encode("ascii")
        )
        groups: set[int] = set()
        own_uid = os.geteuid()
        try:
            proc_entries = list(Path("/proc").iterdir())
        except OSError as exc:
            raise _UnsettledSubprocessError(
                "cannot scan recovery transfer processes"
            ) from exc
        for entry in proc_entries:
            if not entry.name.isdigit():
                continue
            pid = int(entry.name)
            try:
                status = (entry / "status").read_text(
                    encoding="utf-8",
                ).splitlines()
                uid = int(next(
                    line.split()[1]
                    for line in status
                    if line.startswith("Uid:")
                ))
            except (FileNotFoundError, ProcessLookupError):
                continue
            except (OSError, StopIteration, ValueError):
                # Other users' processes cannot inherit this Manager-created
                # token. Only an uninspectable process owned by this uid is
                # relevant to the proof below.
                continue
            if uid != own_uid:
                continue
            command_line: bytes | None
            try:
                with (entry / "cmdline").open("rb") as stream:
                    command_line = stream.read(1024 * 1024 + 1)
            except (FileNotFoundError, ProcessLookupError):
                continue
            except PermissionError:
                command_line = None
            except OSError:
                command_line = None
            if (
                command_line is not None
                and len(command_line) > 1024 * 1024
            ):
                raise _UnsettledSubprocessError(
                    f"recovery transfer pid {pid} has oversized command line"
                )
            token_in_command = (
                command_line is not None
                and marker in command_line
            )
            token_in_environment = False
            if not token_in_command:
                try:
                    with (entry / "environ").open("rb") as stream:
                        environment = stream.read(1024 * 1024 + 1)
                except (FileNotFoundError, ProcessLookupError):
                    continue
                except (PermissionError, OSError) as exc:
                    if command_line is None:
                        # Both discovery channels are unavailable for this
                        # same-uid process, so absence of the token cannot be
                        # proven.
                        raise _UnsettledSubprocessError(
                            f"cannot inspect recovery transfer pid {pid}"
                        ) from exc
                    # The local rsync and its ssh transport both carry the
                    # tokenized --rsync-path in argv. A readable command line
                    # without that invariant marker is not this transfer.
                    continue
                if len(environment) > 1024 * 1024:
                    raise _UnsettledSubprocessError(
                        f"recovery transfer pid {pid} has oversized environment"
                    )
                token_in_environment = (
                    marker in environment.split(b"\0")
                )
            if not token_in_command and not token_in_environment:
                continue
            identity = cls._process_identity(pid)
            if identity is None:
                continue
            process_group, session, _starttime = identity
            if process_group <= 1 or process_group != session:
                raise _UnsettledSubprocessError(
                    "recovery transfer escaped its dedicated process session"
                )
            groups.add(process_group)
        return groups

    @staticmethod
    def _validate_recovery_transfer_record(
        raw: object,
        *,
        job_id: str,
        transfer_id: str,
    ) -> dict:
        if not isinstance(raw, dict):
            raise _UnsettledSubprocessError(
                "invalid recovery transfer journal"
            )
        token = raw.get("token")
        pid = raw.get("pid")
        starttime = raw.get("starttime")
        if (
            raw.get("schema_version")
            != _RECOVERY_TRANSFER_SCHEMA_VERSION
            or raw.get("job_id") != job_id
            or raw.get("transfer_id") != transfer_id
            or not isinstance(token, str)
            or re.fullmatch(r"[0-9a-f]{32}", token) is None
            or isinstance(pid, bool)
            or not isinstance(pid, int)
            or pid < 0
            or isinstance(starttime, bool)
            or not isinstance(starttime, int)
            or starttime < 0
            or (pid == 0) != (starttime == 0)
        ):
            raise _UnsettledSubprocessError(
                "invalid recovery transfer journal"
            )
        return raw

    def _create_recovery_transfer_record(
        self,
        *,
        job_id: str,
        shard_index: int,
        relative: str,
    ) -> tuple[Path, dict]:
        job_root = self._recovery_transfer_job_root(
            job_id, create=True,
        )
        transfer_id = uuid.uuid4().hex
        record = {
            "schema_version": _RECOVERY_TRANSFER_SCHEMA_VERSION,
            "job_id": job_id,
            "transfer_id": transfer_id,
            "token": transfer_id,
            "pid": 0,
            "starttime": 0,
            "shard_index": shard_index,
            "path": relative,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        path = job_root / f"{transfer_id}.json"
        atomic_write_private(
            path,
            json.dumps(
                record,
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n",
        )
        return path, record

    @classmethod
    def _record_recovery_transfer_process(
        cls,
        path: Path,
        record: dict,
        pid: int,
    ) -> dict:
        identity = cls._process_identity(pid)
        if identity is None:
            raise _UnsettledSubprocessError(
                "recovery transfer exited before its identity was recorded"
            )
        process_group, session, starttime = identity
        if process_group != pid or session != pid:
            raise _UnsettledSubprocessError(
                "recovery transfer did not enter a dedicated process session"
            )
        updated = {
            **record,
            "pid": pid,
            "starttime": starttime,
        }
        atomic_write_private(
            path,
            json.dumps(
                updated,
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n",
        )
        return updated

    @staticmethod
    def _remove_recovery_transfer_record(path: Path) -> None:
        parent = path.parent
        path.unlink(missing_ok=True)
        if parent.exists():
            fsync_directory(parent)

    async def _settle_live_recovery_transfer(
        self,
        proc: asyncio.subprocess.Process,
        record_path: Path,
    ) -> None:
        """Reap the whole live PGID before making its staging reclaimable."""

        async def settle() -> None:
            await _terminate_subprocess_transaction(proc)
            await asyncio.to_thread(
                self._remove_recovery_transfer_record,
                record_path,
            )

        task = asyncio.create_task(settle())
        await _await_owned_task(task, operation_error_wins=True)

    async def _settle_recorded_recovery_transfer(
        self,
        path: Path,
        *,
        job_id: str,
    ) -> None:
        if (
            path.is_symlink()
            or not path.is_file()
            or path.suffix != ".json"
            or re.fullmatch(r"[0-9a-f]{32}", path.stem) is None
            or path.stat().st_size > 64 * 1024
        ):
            raise _UnsettledSubprocessError(
                "unsafe recovery transfer journal entry"
            )
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise _UnsettledSubprocessError(
                "invalid recovery transfer journal"
            ) from exc
        record = self._validate_recovery_transfer_record(
            raw,
            job_id=job_id,
            transfer_id=path.stem,
        )
        groups: set[int] = set()
        pid = record["pid"]
        if pid:
            identity = self._process_identity(pid)
            if (
                identity is not None
                and identity[2] == record["starttime"]
            ):
                if identity[0] != pid or identity[1] != pid:
                    raise _UnsettledSubprocessError(
                        "recorded recovery transfer changed process session"
                    )
                groups.add(pid)
            elif identity is None and _process_group_exists(pid):
                # The leader may have exited while ssh/rsync descendants keep
                # the original session alive. A live PGID equal to an absent
                # PID cannot have been freshly reused, so it is safe to reap.
                groups.add(pid)
            # A different starttime proves PID reuse. Linux cannot reuse that
            # numeric PID while the old process-group identifier still exists.
        else:
            # The record is fsynced before spawn. If Manager died between spawn
            # and recording the PID, find the dedicated session by its unique
            # inherited token. The token is present in both the environment and
            # rsync argv so non-dumpable processes remain discoverable.
            groups = self._transfer_process_groups(record["token"])

        for process_group in groups:
            try:
                os.killpg(process_group, signal.SIGTERM)
            except ProcessLookupError:
                continue
        deadline = (
            asyncio.get_running_loop().time()
            + _SUBPROCESS_TERMINATE_TIMEOUT_SECONDS
        )
        while any(_process_group_exists(group) for group in groups):
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                break
            await asyncio.sleep(
                min(_SUBPROCESS_GROUP_POLL_SECONDS, remaining)
            )
        remaining_groups = {
            group for group in groups if _process_group_exists(group)
        }
        for process_group in remaining_groups:
            try:
                os.killpg(process_group, signal.SIGKILL)
            except ProcessLookupError:
                continue
        deadline = (
            asyncio.get_running_loop().time()
            + _SUBPROCESS_GROUP_REAP_TIMEOUT_SECONDS
        )
        while any(
            _process_group_exists(group) for group in remaining_groups
        ):
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                raise _UnsettledSubprocessError(
                    "recorded recovery transfer process group could not "
                    "be proven terminated"
                )
            await asyncio.sleep(
                min(_SUBPROCESS_GROUP_POLL_SECONDS, remaining)
            )
        await asyncio.to_thread(
            self._remove_recovery_transfer_record,
            path,
        )

    async def _settle_recovery_transfer_records(
        self,
        job_id: str,
    ) -> None:
        job_root = self._recovery_transfer_job_root(
            job_id, create=False,
        )
        if not job_root.exists():
            return
        entries = list(job_root.iterdir())
        for path in entries:
            await self._settle_recorded_recovery_transfer(
                path,
                job_id=job_id,
            )
        try:
            job_root.rmdir()
        except FileNotFoundError:
            pass
        except OSError as exc:
            raise _UnsettledSubprocessError(
                "recovery transfer journal is not empty"
            ) from exc
        fsync_directory(job_root.parent)

    async def resolve_recovery_checkpoint(
        self,
        *,
        source_job_id: str,
        generation: str,
        source_spec: JobSpec,
        target_spec: JobSpec,
    ) -> dict:
        """Resolve and fully validate one set during side-effect-free preflight."""

        cancel_event = threading.Event()
        checkpoint_set = await _await_owned_thread(
            self._checkpoint_store().resolve_checkpoint_set,
            source_job_id=source_job_id,
            generation=generation,
            deadline_monotonic=(
                time.monotonic() + _RECOVERY_PREFLIGHT_DEADLINE_SECONDS
            ),
            cancel_event=cancel_event,
            cancellation_event=cancel_event,
        )
        self._validate_resolved_checkpoint_set(
            checkpoint_set,
            source_spec=source_spec,
            target_spec=target_spec,
        )
        return checkpoint_set

    @classmethod
    def _validate_resolved_checkpoint_set(
        cls,
        checkpoint_set: dict,
        *,
        source_spec: JobSpec,
        target_spec: JobSpec,
    ) -> dict[str, dict]:
        """Authenticate the aggregate set contract before durable Job prepare.

        The S3 resolver proves the individual manifests and COMMITTED marker.
        This second layer binds that proof to the source/target Job contracts,
        exact fanout map, configured Manager staging budget, and target root
        disk.  ``prepare_recovery`` repeats it to close the preflight/staging
        time-of-check gap.
        """

        if not isinstance(checkpoint_set, dict):
            raise RuntimeError("checkpoint set is invalid")
        set_generation = checkpoint_set.get("generation")
        if (
            not isinstance(set_generation, str)
            or _SAFE_JOB_ID.fullmatch(set_generation) is None
            or (
                target_spec.recovery.generation
                and set_generation != target_spec.recovery.generation
            )
        ):
            raise RuntimeError("checkpoint set generation is invalid")
        expected_common_metadata = {
            "resolved_commit": source_spec.setup.resolved_commit,
            "fanout_workers": source_spec.fanout.workers,
            "shard_by": source_spec.fanout.shard_by,
            "collect_paths": list(source_spec.collect.paths),
            "collect_exclude": list(source_spec.collect.exclude),
        }
        metadata = checkpoint_set.get("metadata")
        metadata_matches_common = (
            isinstance(metadata, dict)
            and all(
                metadata.get(key) == value
                for key, value in expected_common_metadata.items()
            )
        )
        metadata_matches_current = (
            metadata_matches_common
            and metadata.get("recovery_contract_version")
            == _RECOVERY_CONTRACT_VERSION
            and metadata.get("recovery_contract_sha256")
            == cls._checkpoint_contract_hash(source_spec)
        )
        metadata_matches_legacy_any = (
            metadata_matches_common
            and source_spec.account.auth_kind == "any"
            and metadata.get("recovery_contract_version")
            == _LEGACY_RECOVERY_CONTRACT_VERSION
            and metadata.get("recovery_contract_sha256")
            == cls._checkpoint_contract_hash(
                source_spec,
                contract_version=_LEGACY_RECOVERY_CONTRACT_VERSION,
            )
        )
        if not (metadata_matches_current or metadata_matches_legacy_any):
            raise RuntimeError(
                "checkpoint set metadata does not match source Job"
            )
        raw_shards = checkpoint_set.get("shards")
        if not isinstance(raw_shards, list):
            raise RuntimeError("checkpoint set shard map is invalid")
        checkpoint_shards: dict[str, dict] = {}
        summed_bytes = 0
        summed_objects = 0
        for entry in raw_shards:
            if not isinstance(entry, dict):
                raise RuntimeError("checkpoint set shard map is invalid")
            namespace = entry.get("worker_namespace")
            shard_generation = entry.get("generation")
            manifest_sha256 = entry.get("manifest_sha256")
            shard_bytes = entry.get("total_bytes")
            shard_objects = entry.get("total_objects")
            if (
                not isinstance(namespace, str)
                or _SAFE_JOB_ID.fullmatch(namespace) is None
                or namespace in checkpoint_shards
                or not isinstance(shard_generation, str)
                or _SAFE_JOB_ID.fullmatch(shard_generation) is None
                or not isinstance(manifest_sha256, str)
                or re.fullmatch(r"[0-9a-f]{64}", manifest_sha256) is None
                or isinstance(shard_bytes, bool)
                or not isinstance(shard_bytes, int)
                or shard_bytes < 0
                or isinstance(shard_objects, bool)
                or not isinstance(shard_objects, int)
                or shard_objects < 0
            ):
                raise RuntimeError("checkpoint set shard map is invalid")
            checkpoint_shards[namespace] = entry
            summed_bytes += shard_bytes
            summed_objects += shard_objects
        expected_namespaces = {
            f"shard-{index:05d}"
            for index in range(target_spec.fanout.workers)
        }
        if set(checkpoint_shards) != expected_namespaces:
            raise RuntimeError(
                "checkpoint set does not contain every source shard"
            )
        total_bytes = checkpoint_set.get("total_bytes")
        total_objects = checkpoint_set.get("total_objects")
        if (
            isinstance(total_bytes, bool)
            or not isinstance(total_bytes, int)
            or total_bytes < 0
            or total_bytes != summed_bytes
        ):
            raise RuntimeError("checkpoint set total size is invalid")
        if (
            isinstance(total_objects, bool)
            or not isinstance(total_objects, int)
            or total_objects < 0
            or total_objects != summed_objects
        ):
            raise RuntimeError(
                "checkpoint set total object count is invalid"
            )
        root_disk_gb = target_spec.fanout.disk_gb or 40
        usable_root_bytes = (
            root_disk_gb * 1024 * 1024 * 1024
            - _WORKER_RECOVERY_DISK_RESERVE_BYTES
        )
        worker_wrapper_objects = cls._worker_recovery_wrapper_objects(
            list(target_spec.recovery.paths),
        )
        oversized_shards = [
            namespace
            for namespace, entry in checkpoint_shards.items()
            if (
                entry["total_bytes"]
                + (
                    entry["total_objects"] + worker_wrapper_objects
                ) * _RECOVERY_ALLOCATION_FLOOR_BYTES
                > usable_root_bytes
            )
        ]
        if oversized_shards:
            raise RuntimeError(
                "checkpoint cannot fit target worker root disk with "
                "10 GiB reserved for OS, code, and datasets: "
                + ", ".join(sorted(oversized_shards)[:3])
            )
        max_staging = _positive_env_bytes(
            "ELASTIC_AGENT_MAX_RECOVERY_STAGING_BYTES",
            _DEFAULT_MAX_RECOVERY_STAGING_BYTES,
        )
        max_total_staging = _positive_env_bytes(
            "ELASTIC_AGENT_MAX_RECOVERY_STAGING_TOTAL_BYTES",
            max_staging,
        )
        max_objects = _positive_env_bytes(
            "ELASTIC_AGENT_MAX_RECOVERY_STAGING_OBJECTS",
            _DEFAULT_MAX_RECOVERY_STAGING_OBJECTS,
        )
        max_total_objects = _positive_env_bytes(
            "ELASTIC_AGENT_MAX_RECOVERY_STAGING_TOTAL_OBJECTS",
            max_objects,
        )
        _, wrapper_objects = cls._recovery_wrapper_object_counts(
            list(target_spec.recovery.paths),
            target_spec.fanout.workers,
        )
        if total_bytes > max_staging:
            raise RuntimeError("recovery staging byte limit exceeded")
        if total_bytes > max_total_staging:
            raise RuntimeError(
                "recovery exceeds Manager total staging byte ceiling"
            )
        if total_objects + wrapper_objects > max_objects:
            raise RuntimeError("recovery staging object limit exceeded")
        if total_objects + wrapper_objects > max_total_objects:
            raise RuntimeError(
                "recovery exceeds Manager total staging object ceiling"
            )
        return checkpoint_shards

    async def cleanup_stale_recovery_staging(self) -> None:
        """Remove Manager-local staging left by a previous process.

        This is called while the Manager controller lock is held and before the
        API accepts new Jobs, so every child is necessarily stale. Unknown
        entries or deletion failures fail startup visibly instead of silently
        consuming the Manager disk.
        """

        transfer_root = self._recovery_transfer_root()
        for child in list(transfer_root.iterdir()):
            if (
                _SAFE_JOB_ID.fullmatch(child.name) is None
                or child.is_symlink()
                or not child.is_dir()
            ):
                raise RuntimeError(
                    f"unsafe recovery transfer journal: {child.name!r}"
                )
            # A pre-spawn token is durable before rsync exists. On restart,
            # settle every matching inherited process session before any source
            # tree can be reclaimed.
            await self._settle_recovery_transfer_records(child.name)

        root = self._recovery_staging_root()
        for child in root.iterdir():
            if (
                _SAFE_JOB_ID.fullmatch(child.name) is None
                or child.is_symlink()
                or not child.is_dir()
            ):
                raise RuntimeError(
                    f"unsafe recovery staging entry: {child.name!r}"
                )
            await _await_owned_thread(shutil.rmtree, child)
        snapshot_root = self._checkpoint_snapshot_root()
        for child in snapshot_root.iterdir():
            if (
                child.is_symlink()
                or not child.is_dir()
                or not child.name.startswith(".elastic-checkpoint-")
            ):
                raise RuntimeError(
                    f"unsafe checkpoint snapshot entry: {child.name!r}"
                )
            await _await_owned_thread(shutil.rmtree, child)

    async def _reserve_recovery_staging(
        self,
        *,
        job_id: str,
        total_bytes: int,
        total_objects: int,
        wrapper_objects: int = 0,
    ) -> None:
        """Atomically reserve the Manager-wide recovery staging budget."""

        if (
            isinstance(total_objects, bool)
            or not isinstance(total_objects, int)
            or total_objects < 0
            or isinstance(wrapper_objects, bool)
            or not isinstance(wrapper_objects, int)
            or wrapper_objects < 0
        ):
            raise RuntimeError(
                "recovery staging object reservation is invalid"
            )
        physical_objects = total_objects + wrapper_objects
        lock = getattr(self._mgr, "_recovery_staging_budget_lock", None)
        if lock is None:
            lock = asyncio.Lock()
            self._mgr._recovery_staging_budget_lock = lock
        reservations = getattr(
            self._mgr, "_recovery_staging_reservations", None,
        )
        if reservations is None:
            reservations = {}
            self._mgr._recovery_staging_reservations = reservations
        max_job_bytes = _positive_env_bytes(
            "ELASTIC_AGENT_MAX_RECOVERY_STAGING_BYTES",
            _DEFAULT_MAX_RECOVERY_STAGING_BYTES,
        )
        max_total_bytes = _positive_env_bytes(
            "ELASTIC_AGENT_MAX_RECOVERY_STAGING_TOTAL_BYTES",
            max_job_bytes,
        )
        max_job_objects = _positive_env_bytes(
            "ELASTIC_AGENT_MAX_RECOVERY_STAGING_OBJECTS",
            _DEFAULT_MAX_RECOVERY_STAGING_OBJECTS,
        )
        max_total_objects = _positive_env_bytes(
            "ELASTIC_AGENT_MAX_RECOVERY_STAGING_TOTAL_OBJECTS",
            max_job_objects,
        )
        if total_bytes > max_job_bytes:
            raise RuntimeError(
                "recovery staging byte limit exceeded"
            )
        if physical_objects > max_job_objects:
            raise RuntimeError(
                "recovery staging object limit exceeded"
            )
        async with lock:
            if job_id in reservations:
                raise RuntimeError(
                    "recovery staging reservation already exists"
                )
            root = self._recovery_staging_root()
            filesystem = os.statvfs(root)
            allocation_unit = self._allocation_unit_from_statvfs(
                filesystem
            )
            physical_bytes = (
                total_bytes + physical_objects * allocation_unit
            )
            reserved_physical_bytes = sum(
                reservation[0]
                for reservation in reservations.values()
            )
            reserved_logical_bytes = sum(
                (
                    reservation[2]
                    if len(reservation) > 2
                    else reservation[0]
                )
                for reservation in reservations.values()
            )
            reserved_objects = sum(
                reservation[1]
                for reservation in reservations.values()
            )
            if reserved_logical_bytes + total_bytes > max_total_bytes:
                raise RuntimeError(
                    "Manager recovery staging byte budget is exhausted"
                )
            if reserved_objects + physical_objects > max_total_objects:
                raise RuntimeError(
                    "Manager recovery staging object budget is exhausted"
                )
            # The configured budget alone is not enough: two concurrent restores
            # can each observe sufficient free space before either starts writing.
            # Fence the filesystem capacity under the same Manager-wide lock.
            # Counting all reservations as still outstanding is intentionally
            # conservative once an earlier restore has begun materializing files,
            # but guarantees concurrent restores cannot spend the same free bytes.
            free = shutil.disk_usage(root).free
            if (
                reserved_physical_bytes
                + physical_bytes
                + _RECOVERY_FREE_SPACE_RESERVE_BYTES
                > free
            ):
                raise RuntimeError(
                    "insufficient Manager disk for recovery staging reservations"
                )
            free_inodes = filesystem.f_favail
            if (
                reserved_objects
                + physical_objects
                + _RECOVERY_FREE_INODE_RESERVE
                > free_inodes
            ):
                raise RuntimeError(
                    "insufficient Manager inodes for recovery staging "
                    "reservations"
                )
            reservations[job_id] = (
                physical_bytes,
                physical_objects,
                total_bytes,
            )

    async def _release_recovery_staging_reservation(
        self,
        job_id: str,
    ) -> None:
        lock = getattr(self._mgr, "_recovery_staging_budget_lock", None)
        reservations = getattr(
            self._mgr, "_recovery_staging_reservations", None,
        )
        if lock is None or reservations is None:
            return
        async with lock:
            reservations.pop(job_id, None)

    @staticmethod
    def _recovery_wrapper_object_counts(
        paths: list[str],
        workers: int,
    ) -> tuple[int, int]:
        """Return base wrappers and exact checkpoint-only parent overhead."""

        strict_parents: set[tuple[str, ...]] = set()
        for raw in paths:
            parts = PurePosixPath(raw).parts
            strict_parents.update(
                parts[:depth]
                for depth in range(1, len(parts))
            )
        # One Job root and one destination root per shard are never represented
        # by either S3 manifest. Checkpoint manifests contain each requested
        # path itself, but not the strict parents mkdir(parents=True) creates.
        base = 1 + workers
        return base, base + workers * len(strict_parents)

    @staticmethod
    def _worker_recovery_wrapper_objects(paths: list[str]) -> int:
        """Bound transaction metadata, staging, backups, and path parents."""

        return 64 + 2 * sum(
            len(PurePosixPath(path).parts)
            for path in paths
        )

    @staticmethod
    def _filesystem_allocation_unit(root: Path) -> int:
        return ManagerFleetDriver._allocation_unit_from_statvfs(
            os.statvfs(root)
        )

    @staticmethod
    def _allocation_unit_from_statvfs(filesystem) -> int:
        fragment = int(
            getattr(filesystem, "f_frsize", 0)
            or getattr(filesystem, "f_bsize", 0)
            or _RECOVERY_ALLOCATION_FLOOR_BYTES
        )
        return max(_RECOVERY_ALLOCATION_FLOOR_BYTES, fragment)

    @staticmethod
    def _validate_live_recovery_capacity(
        root: Path,
        *,
        remaining_bytes: int,
        remaining_objects: int,
    ) -> None:
        """Fence external disk/inode consumption throughout a restore."""

        if remaining_bytes < 0 or remaining_objects < 0:
            raise RuntimeError(
                "recovery staging reservation was exceeded"
            )
        filesystem = os.statvfs(root)
        allocation_unit = (
            ManagerFleetDriver._allocation_unit_from_statvfs(filesystem)
        )
        required_free_bytes = (
            _RECOVERY_FREE_SPACE_RESERVE_BYTES
            + remaining_bytes
            + remaining_objects * allocation_unit
        )
        if (
            shutil.disk_usage(root).free
            < required_free_bytes
        ):
            raise RuntimeError(
                "recovery staging exhausted Manager disk reserve"
            )
        if (
            filesystem.f_favail
            < _RECOVERY_FREE_INODE_RESERVE + remaining_objects
        ):
            raise RuntimeError(
                "recovery staging exhausted Manager inode reserve"
            )

    @staticmethod
    def _collection_staging_limits() -> tuple[int, int, int]:
        max_bytes = _positive_env_bytes(
            "ELASTIC_AGENT_MAX_COLLECTION_STAGING_BYTES",
            _DEFAULT_MAX_COLLECTION_STAGING_BYTES,
        )
        max_objects = _positive_env_bytes(
            "ELASTIC_AGENT_MAX_COLLECTION_STAGING_OBJECTS",
            _DEFAULT_MAX_COLLECTION_STAGING_OBJECTS,
        )
        max_file_bytes = _positive_env_bytes(
            "ELASTIC_AGENT_MAX_COLLECTION_FILE_BYTES",
            _DEFAULT_MAX_COLLECTION_FILE_BYTES,
        )
        if max_file_bytes > max_bytes:
            raise RuntimeError(
                "ELASTIC_AGENT_MAX_COLLECTION_FILE_BYTES cannot exceed "
                "ELASTIC_AGENT_MAX_COLLECTION_STAGING_BYTES"
            )
        return max_bytes, max_objects, max_file_bytes

    async def _reserve_collection_staging(
        self,
        *,
        reservation_id: str,
        max_bytes: int,
        max_objects: int,
        allow_smaller: bool = False,
        concurrency_hint: int = 1,
    ) -> tuple[int, int]:
        """Reserve bounded Manager disk/inode capacity for one shard rsync.

        A full per-shard allowance is reserved before rsync starts.  This is
        intentionally conservative: concurrent fanout shards may proceed while
        they fit, but they can never all spend the same bytes reported free by
        the filesystem.
        """

        lock = getattr(self._mgr, "_collection_staging_budget_lock", None)
        if lock is None:
            lock = asyncio.Lock()
            self._mgr._collection_staging_budget_lock = lock
        condition = getattr(
            self._mgr, "_collection_staging_budget_condition", None,
        )
        if condition is None:
            condition = asyncio.Condition(lock)
            self._mgr._collection_staging_budget_condition = condition
        reservations = getattr(
            self._mgr, "_collection_staging_reservations", None,
        )
        if reservations is None:
            reservations = {}
            self._mgr._collection_staging_reservations = reservations
        waiters = getattr(
            self._mgr, "_collection_staging_waiters", None,
        )
        if waiters is None:
            waiters = []
            self._mgr._collection_staging_waiters = waiters
        max_total_bytes = _positive_env_bytes(
            "ELASTIC_AGENT_MAX_COLLECTION_STAGING_TOTAL_BYTES",
            _DEFAULT_MAX_COLLECTION_STAGING_BYTES * 4,
        )
        max_total_objects = _positive_env_bytes(
            "ELASTIC_AGENT_MAX_COLLECTION_STAGING_TOTAL_OBJECTS",
            _DEFAULT_MAX_COLLECTION_STAGING_OBJECTS * 4,
        )
        root = Path(self._mgr.collected_root)
        root.mkdir(mode=0o700, parents=True, exist_ok=True)
        wait_deadline = (
            asyncio.get_running_loop().time()
            + _COLLECTION_STAGING_RESERVATION_WAIT_SECONDS
        )
        async with condition:
            if reservation_id in reservations:
                raise RuntimeError(
                    "collection staging reservation already exists"
                )
            if reservation_id in waiters:
                raise RuntimeError(
                    "collection staging waiter already exists"
                )
            waiters.append(reservation_id)
            try:
                while True:
                    free = shutil.disk_usage(root).free
                    free_inodes = os.statvfs(root).f_favail
                    intrinsic_bytes = min(
                        max_total_bytes,
                        free - _COLLECTION_FREE_SPACE_RESERVE_BYTES,
                    )
                    intrinsic_objects = min(
                        max_total_objects,
                        free_inodes - _COLLECTION_FREE_INODE_RESERVE,
                    )
                    if intrinsic_bytes <= 0:
                        raise RuntimeError(
                            "Manager collection staging has no byte capacity"
                        )
                    if intrinsic_objects <= 0:
                        raise RuntimeError(
                            "Manager collection staging has no object capacity"
                        )
                    if not allow_smaller and max_bytes > intrinsic_bytes:
                        raise RuntimeError(
                            "insufficient Manager disk for collection staging "
                            "reservations"
                        )
                    if not allow_smaller and max_objects > intrinsic_objects:
                        raise RuntimeError(
                            "insufficient Manager inodes for collection "
                            "staging reservations"
                        )

                    reserved_bytes = sum(
                        reservation[0]
                        for reservation in reservations.values()
                    )
                    reserved_objects = sum(
                        reservation[1]
                        for reservation in reservations.values()
                    )
                    available_bytes = min(
                        max_total_bytes - reserved_bytes,
                        free
                        - reserved_bytes
                        - _COLLECTION_FREE_SPACE_RESERVE_BYTES,
                    )
                    available_objects = min(
                        max_total_objects - reserved_objects,
                        free_inodes
                        - reserved_objects
                        - _COLLECTION_FREE_INODE_RESERVE,
                    )
                    queue_head = bool(
                        waiters and waiters[0] == reservation_id
                    )
                    if allow_smaller:
                        # Size the share for work that is actually active or
                        # queued now. As peers finish, later waiters may borrow
                        # the released capacity instead of being permanently
                        # pinned to the original fanout count.
                        fair_share = max(
                            1,
                            min(
                                max(1, concurrency_hint),
                                len(reservations) + len(waiters),
                            ),
                        )
                        effective_bytes = min(
                            max_bytes,
                            available_bytes,
                            max(1, intrinsic_bytes // fair_share),
                        )
                        effective_objects = min(
                            max_objects,
                            available_objects,
                            max(1, intrinsic_objects // fair_share),
                        )
                    else:
                        effective_bytes = max_bytes
                        effective_objects = max_objects
                    capacity_ready = (
                        effective_bytes > 0
                        and effective_objects > 0
                        and (
                            allow_smaller
                            or (
                                effective_bytes <= available_bytes
                                and effective_objects <= available_objects
                            )
                        )
                    )
                    if queue_head and capacity_ready:
                        waiters.pop(0)
                        reservations[reservation_id] = (
                            effective_bytes,
                            effective_objects,
                        )
                        condition.notify_all()
                        return effective_bytes, effective_objects

                    remaining = (
                        wait_deadline
                        - asyncio.get_running_loop().time()
                    )
                    if remaining <= 0:
                        raise RuntimeError(
                            "timed out waiting for Manager collection "
                            "staging capacity"
                        )
                    try:
                        await asyncio.wait_for(
                            condition.wait(), timeout=remaining,
                        )
                    except asyncio.TimeoutError as exc:
                        raise RuntimeError(
                            "timed out waiting for Manager collection "
                            "staging capacity"
                        ) from exc
            finally:
                if reservation_id in waiters:
                    waiters.remove(reservation_id)
                    condition.notify_all()

    async def _release_collection_staging_reservation(
        self,
        reservation_id: str,
    ) -> None:
        lock = getattr(self._mgr, "_collection_staging_budget_lock", None)
        reservations = getattr(
            self._mgr, "_collection_staging_reservations", None,
        )
        condition = getattr(
            self._mgr, "_collection_staging_budget_condition", None,
        )
        if lock is None or reservations is None:
            return
        async with (condition or lock):
            reservations.pop(reservation_id, None)
            if condition is not None:
                condition.notify_all()

    @staticmethod
    def _validate_collection_tree(
        root: Path,
        *,
        max_bytes: int,
        max_objects: int,
        max_file_bytes: int,
    ) -> tuple[int, int]:
        """Bound a worker-controlled relay tree without following symlinks."""

        if not root.exists():
            return 0, 0
        if root.is_symlink() or not root.is_dir():
            raise RuntimeError("unsafe Manager collection staging tree")
        total_bytes = 0
        total_objects = 0
        pending = [root]
        while pending:
            directory = pending.pop()
            try:
                entries = os.scandir(directory)
            except FileNotFoundError:
                # rsync publishes and removes temporary paths while the live
                # budget monitor walks its private staging attempt.  A path
                # that no longer exists cannot consume the final snapshot's
                # object/byte budget; the mandatory post-rsync scan validates
                # the settled tree before it is published.
                continue
            except OSError as exc:
                raise RuntimeError(
                    "cannot inspect Manager collection staging tree"
                ) from exc
            with entries:
                for entry in entries:
                    total_objects += 1
                    if total_objects > max_objects:
                        raise RuntimeError(
                            "Manager collection staging object limit exceeded"
                        )
                    try:
                        entry_stat = entry.stat(follow_symlinks=False)
                    except FileNotFoundError:
                        # The entry was returned by scandir but rsync renamed
                        # or removed it before lstat.  Keep the conservative
                        # object count and continue; all non-ENOENT failures
                        # still fail closed.
                        continue
                    except OSError as exc:
                        raise RuntimeError(
                            "cannot inspect Manager collection staging entry"
                        ) from exc
                    if stat.S_ISDIR(entry_stat.st_mode):
                        pending.append(Path(entry.path))
                        continue
                    if not stat.S_ISREG(entry_stat.st_mode):
                        continue
                    if entry_stat.st_size > max_file_bytes:
                        raise RuntimeError(
                            "Manager collection single-file limit exceeded"
                        )
                    total_bytes += entry_stat.st_size
                    if total_bytes > max_bytes:
                        raise RuntimeError(
                            "Manager collection staging byte limit exceeded"
                        )
        if (
            shutil.disk_usage(root).free
            < _COLLECTION_FREE_SPACE_RESERVE_BYTES
        ):
            raise RuntimeError(
                "Manager collection staging exhausted disk reserve"
            )
        if os.statvfs(root).f_favail < _COLLECTION_FREE_INODE_RESERVE:
            raise RuntimeError(
                "Manager collection staging exhausted inode reserve"
            )
        return total_bytes, total_objects

    async def _run_bounded_collection_rsync(
        self,
        proc: asyncio.subprocess.Process,
        *,
        staging_root: Path,
        max_bytes: int,
        max_objects: int,
        max_file_bytes: int,
        timeout: float,
    ) -> bytes:
        """Drain rsync while continuously enforcing the local relay budget."""

        deadline = time.monotonic() + timeout
        communication = asyncio.create_task(proc.communicate())
        try:
            while not communication.done():
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise asyncio.TimeoutError
                await asyncio.wait(
                    {communication},
                    timeout=min(
                        _COLLECTION_MONITOR_INTERVAL_SECONDS,
                        remaining,
                    ),
                )
                await asyncio.to_thread(
                    self._validate_collection_tree,
                    staging_root,
                    max_bytes=max_bytes,
                    max_objects=max_objects,
                    max_file_bytes=max_file_bytes,
                )
            _stdout, stderr = communication.result()
            # A final scan closes the interval between the last timer tick and
            # process exit, including a sender that materialized many entries
            # immediately before returning success.
            await asyncio.to_thread(
                self._validate_collection_tree,
                staging_root,
                max_bytes=max_bytes,
                max_objects=max_objects,
                max_file_bytes=max_file_bytes,
            )
        except BaseException:
            async def settle_failed_transfer() -> None:
                if not communication.done():
                    communication.cancel()
                await asyncio.gather(
                    communication, return_exceptions=True,
                )
                await _terminate_subprocess(proc)

            cleanup = asyncio.create_task(settle_failed_transfer())
            await _await_owned_task(
                cleanup,
                operation_error_wins=True,
            )
            raise
        # ``communicate`` proves only that the leader and its inherited pipes
        # settled. Close and prove the saved process group as well before the
        # caller may delete the attempt or release its disk/inode reservation.
        await _terminate_subprocess(proc)
        return stderr or b""

    async def _remote_collection_inventory(
        self,
        *,
        host: str,
        ssh_user: str,
        ssh_key: str,
        source: str,
        exclude: list[str],
        max_bytes: int,
        max_objects: int,
        max_file_bytes: int,
    ) -> tuple[int, int]:
        """Bound the sender tree before rsync starts allocating local inodes.

        The live receiver scan remains the TOCTOU fence.  This first pass makes
        an already-oversized or inode-hostile tree fail on the Worker, before a
        single entry is materialized on the Manager filesystem.
        """

        from elastic_agent.core.bootstrap import SSHExecutor, _shell_quote

        request = base64.b64encode(json.dumps({
            "root": source,
            "exclude": exclude,
            "max_bytes": max_bytes,
            "max_objects": max_objects,
            "max_file_bytes": max_file_bytes,
        }, separators=(",", ":")).encode()).decode("ascii")
        script = r"""
import base64, fnmatch, json, os, stat, sys
def emit(ok, reason="", total_bytes=0, total_objects=0):
    print(json.dumps({"ok": ok, "reason": reason,
                      "bytes": total_bytes, "objects": total_objects},
                     separators=(",", ":")))
def main():
    cfg = json.loads(base64.b64decode(sys.argv[1], validate=True))
    root = cfg["root"]
    patterns = cfg["exclude"]
    total_bytes = 0
    total_objects = 0
    pending = [("", root)]
    while pending:
        rel_parent, absolute = pending.pop()
        with os.scandir(absolute) as entries:
            for entry in entries:
                rel = entry.name if not rel_parent else rel_parent + "/" + entry.name
                if any(fnmatch.fnmatch(rel, pattern) for pattern in patterns):
                    continue
                info = entry.stat(follow_symlinks=False)
                total_objects += 1
                if total_objects > cfg["max_objects"]:
                    emit(False, "object_limit", total_bytes, total_objects)
                    return 3
                if stat.S_ISDIR(info.st_mode):
                    pending.append((rel, entry.path))
                elif stat.S_ISREG(info.st_mode):
                    if info.st_size > cfg["max_file_bytes"]:
                        emit(False, "file_limit", total_bytes, total_objects)
                        return 4
                    total_bytes += info.st_size
                    if total_bytes > cfg["max_bytes"]:
                        emit(False, "byte_limit", total_bytes, total_objects)
                        return 5
    emit(True, total_bytes=total_bytes, total_objects=total_objects)
    return 0
try:
    code = main()
except Exception:
    emit(False, "scan_failed")
    code = 2
raise SystemExit(code)
"""
        executor = SSHExecutor(
            host,
            user=ssh_user,
            key_path=ssh_key,
            use_sudo=False,
        )
        rc, stdout, _stderr = await executor.execute(
            "python3 -c "
            + _shell_quote(script)
            + " "
            + _shell_quote(request),
            timeout=300,
        )
        if len(stdout.encode("utf-8", errors="replace")) > 1_024:
            raise RuntimeError(
                "remote collection inventory returned oversized output"
            )
        try:
            result = json.loads(stdout)
        except (json.JSONDecodeError, TypeError) as exc:
            raise RuntimeError(
                "remote collection inventory returned invalid output"
            ) from exc
        if (
            rc != 0
            or not isinstance(result, dict)
            or result.get("ok") is not True
        ):
            reason = (
                str(result.get("reason") or "failed")
                if isinstance(result, dict) else "failed"
            )
            raise RuntimeError(
                f"remote collection inventory rejected the tree ({reason})"
            )
        total_bytes = result.get("bytes")
        total_objects = result.get("objects")
        if (
            isinstance(total_bytes, bool)
            or not isinstance(total_bytes, int)
            or total_bytes < 0
            or isinstance(total_objects, bool)
            or not isinstance(total_objects, int)
            or total_objects < 0
        ):
            raise RuntimeError(
                "remote collection inventory returned invalid counts"
            )
        return total_bytes, total_objects

    @staticmethod
    def _cleanup_partial_collection_path(path: Path) -> None:
        """Remove an uncommitted rsync destination without following links."""

        try:
            path_stat = os.lstat(path)
        except FileNotFoundError:
            return
        if stat.S_ISDIR(path_stat.st_mode) and not stat.S_ISLNK(
            path_stat.st_mode
        ):
            shutil.rmtree(path)
        else:
            path.unlink()

    @staticmethod
    def _new_collection_attempt(parent: Path, namespace: str) -> Path:
        """Recover an interrupted swap, then create a private shard attempt.

        rsync must never mutate the last published local snapshot.  A failed
        transfer is discarded as one whole attempt, so multiple collect paths
        cannot expose a mixed generation or delete previously durable output.
        """

        parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        destination = parent / namespace
        backup = parent / f".{namespace}.backup"
        if backup.exists():
            if backup.is_symlink() or not backup.is_dir():
                raise RuntimeError("unsafe Manager collection backup")
            if destination.exists():
                shutil.rmtree(backup)
            else:
                os.replace(backup, destination)
        prefix = f".{namespace}.attempt-"
        with os.scandir(parent) as entries:
            stale = [
                Path(entry.path)
                for entry in entries
                if entry.name.startswith(prefix)
            ]
        for path in stale:
            ManagerFleetDriver._cleanup_partial_collection_path(path)
        attempt = Path(tempfile.mkdtemp(prefix=prefix, dir=parent))
        os.chmod(attempt, 0o700)
        return attempt

    @staticmethod
    def _install_collection_attempt(
        attempt: Path,
        destination: Path,
    ) -> None:
        """Publish a complete local shard tree with rollback on rename error."""

        if attempt.is_symlink() or not attempt.is_dir():
            raise RuntimeError("unsafe Manager collection attempt")
        parent = destination.parent
        backup = parent / f".{destination.name}.backup"
        if backup.exists():
            raise RuntimeError("Manager collection backup was not recovered")
        moved_old = False
        if destination.exists():
            if destination.is_symlink() or not destination.is_dir():
                raise RuntimeError("unsafe Manager collection destination")
            os.replace(destination, backup)
            moved_old = True
        try:
            os.replace(attempt, destination)
        except BaseException:
            if moved_old and backup.exists() and not destination.exists():
                os.replace(backup, destination)
            raise
        try:
            parent_fd = os.open(parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(parent_fd)
            finally:
                os.close(parent_fd)
        except OSError:
            # The completed tree is already visible. Treat inability to prove
            # directory durability as a failed publish and restore the prior
            # complete snapshot when one existed.
            if moved_old and backup.exists() and destination.exists():
                os.replace(destination, attempt)
                os.replace(backup, destination)
            raise
        if moved_old:
            shutil.rmtree(backup)

    @staticmethod
    def _workload_recovery_identity(spec: JobSpec) -> dict:
        """Canonical workload inputs that a checkpoint is allowed to resume."""

        source = spec
        recovery_source = getattr(
            spec, "_checkpoint_contract_source", None,
        )
        if callable(recovery_source):
            source = recovery_source()
        return {
            "environment_profile": source.environment.profile,
            "repo": source.setup.repo,
            "resolved_commit": source.setup.resolved_commit,
            "needs_docker": source.setup.needs_docker,
            "setup_steps": [
                {
                    "name": step.name,
                    "command": step.command,
                    "env": dict(sorted(step.env.items())),
                    "cwd": step.cwd,
                    "timeout": step.timeout,
                    "retries": step.retries,
                    "run_as": step.run_as,
                }
                for step in source.setup.normalized_steps()
            ],
            "s3_datasets": [
                {
                    "uri": dataset.uri,
                    "dest": dataset.dest,
                }
                for dataset in source.setup.s3_datasets
            ],
            # The command itself is deliberately allowed to change from run to
            # resume, but cwd and declarative inputs must retain their identity.
            "run_cwd": source.run.cwd,
            "run_env": dict(sorted(source.run.env.items())),
            "run_secret_env": dict(sorted(source.run.secret_env.items())),
            "fanout_workers": source.fanout.workers,
            "shard_by": source.fanout.shard_by,
            "agent_type": source.account.agent_type,
            "auth_kind": source.account.auth_kind,
            "model": source.account.model,
        }

    @classmethod
    def _checkpoint_contract_hash(
        cls, spec: JobSpec, *, contract_version: int | None = None,
    ) -> str:
        """Hash only versioned recovery invariants, not the evolving schema.

        Hashing ``JobSpec.model_dump()`` makes an old checkpoint unreadable
        whenever a later release adds an unrelated field with a default.
        Keep this payload explicit and bump ``schema_version`` only for a
        deliberate recovery-contract change.
        """

        source = spec
        recovery_source = getattr(
            spec, "_checkpoint_contract_source", None,
        )
        if callable(recovery_source):
            source = recovery_source()
        version = (
            _RECOVERY_CONTRACT_VERSION
            if contract_version is None
            else contract_version
        )
        if version not in {
            _LEGACY_RECOVERY_CONTRACT_VERSION,
            _RECOVERY_CONTRACT_VERSION,
        }:
            raise ValueError(
                f"unsupported recovery contract version {version}"
            )
        contract = cls._workload_recovery_identity(source)
        if version == _LEGACY_RECOVERY_CONTRACT_VERSION:
            # v2 predates independent credential-source admission. Callers
            # only accept it for the compatible ``auth_kind='any'`` default.
            contract.pop("auth_kind")
        contract.update({
            "schema_version": version,
            "collect_paths": list(source.collect.paths),
            "collect_exclude": list(source.collect.exclude),
        })
        payload = json.dumps(
            contract,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return hashlib.sha256(payload).hexdigest()

    @staticmethod
    def _command_hash(spec: JobSpec, ctx: WorkerContext) -> str:
        payload = json.dumps(
            spec.render_command(ctx),
            separators=(",", ":"),
        ).encode("utf-8")
        return hashlib.sha256(payload).hexdigest()

    @staticmethod
    def _validate_recovery_contract(
        source_payload: dict,
        source_spec: JobSpec,
        target_spec: JobSpec,
        *,
        source_quiescent: bool = False,
    ) -> None:
        source_state = source_payload.get("submission_state")
        if source_state not in (
            _TERMINAL_JOB_STATES | {"launching", "running"}
        ):
            raise RuntimeError(
                "recovery source Job must be terminal, or an interrupted Job "
                "whose workers and cloud resources are proven quiescent"
            )
        if not source_quiescent:
            raise RuntimeError(
                "recovery source Job resources are not proven quiescent"
            )
        if source_spec.fanout.workers != target_spec.fanout.workers:
            raise RuntimeError(
                "recovery source and target fanout.workers must match"
            )
        source_commit = source_spec.setup.resolved_commit
        target_commit = target_spec.setup.resolved_commit
        if (
            not source_commit
            or not target_commit
            or source_commit != target_commit
        ):
            raise RuntimeError(
                "recovery requires the same non-empty setup.resolved_commit "
                "as the source Job"
            )
        if source_spec.setup.repo != target_spec.setup.repo:
            raise RuntimeError(
                "recovery source and target setup.repo must match"
            )
        if (
            ManagerFleetDriver._workload_recovery_identity(source_spec)
            != ManagerFleetDriver._workload_recovery_identity(target_spec)
        ):
            raise RuntimeError(
                "recovery source and target workload inputs must match "
                "(environment, setup, datasets, run cwd/env, fanout, account "
                "auth kind, and model)"
            )
        unavailable = (
            set(target_spec.recovery.paths)
            - set(source_spec.collect.paths)
        )
        if unavailable:
            raise RuntimeError(
                "recovery paths were not collected by the source Job: "
                + ", ".join(sorted(unavailable))
            )
        if (
            target_spec.recovery.policy == "checkpoint"
            and not source_spec.collect.checkpoint
        ):
            raise RuntimeError(
                "source Job did not enable immutable checkpoint collection"
            )
        if target_spec.recovery.policy == "checkpoint":
            if target_spec.recovery.paths != source_spec.collect.paths:
                raise RuntimeError(
                    "checkpoint recovery.paths must exactly match the source "
                    "collect.paths"
                )
            if (
                source_spec.fanout.shard_by != "shard_index"
                or target_spec.fanout.shard_by != "shard_index"
            ):
                raise RuntimeError(
                    "checkpoint recovery requires source and target "
                    "fanout.shard_by='shard_index'; hostnames change on new EC2s"
                )
            if (
                source_spec.account.agent_type
                != target_spec.account.agent_type
                or source_spec.account.auth_kind
                != target_spec.account.auth_kind
                or source_spec.account.model != target_spec.account.model
            ):
                raise RuntimeError(
                    "checkpoint recovery requires the same account agent_type, "
                    "auth_kind, and model as the source Job"
                )
        if target_spec.recovery.policy == "legacy_final_collection":
            raise RuntimeError(
                "legacy mutable result recovery is disabled because it cannot "
                "prove file deletions or a complete generation; restart the "
                "workload from the beginning, then enable collect.checkpoint"
            )

    async def _source_recovery_quiescent(
        self,
        source_job_id: str,
    ) -> bool:
        """Prove an interrupted historical Job no longer owns live resources."""

        if not getattr(self._mgr, "binding_recovery_ready", False):
            return False
        batch = getattr(self._mgr, "_batch", None)
        if batch is not None:
            live_job = batch.get_job(source_job_id)
            if live_job is not None:
                summary = live_job.summary()
                if (
                    not summary.get("done")
                    or summary.get("cleanup_pending") != 0
                    or not getattr(
                        live_job, "resources_released", False,
                    )
                    or not getattr(
                        live_job, "accounts_released", False,
                    )
                ):
                    return False
        if getattr(
            self._mgr, "_unbound_launch_intent_counts", {},
        ).get(source_job_id):
            return False
        if source_job_id in getattr(
            self._mgr, "_recovery_unbound_launch_scans", {},
        ):
            return False
        binding_store = getattr(self._mgr, "account_binding_store", None)
        if binding_store is not None:
            leases = await binding_store.list_leases(active_only=True)
            if any(lease.job_id == source_job_id for lease in leases):
                return False
        registry = getattr(self._mgr, "registry", None)
        if registry is None or not callable(getattr(registry, "list_all", None)):
            return False
        for node in await registry.list_all():
            if str(
                getattr(node, "metadata", {}).get("job_id") or ""
            ) == source_job_id and str(
                getattr(getattr(node, "status", None), "value", "")
            ) != "terminated":
                return False
        return True

    async def prepare_recovery(self, job_id: str, spec: JobSpec) -> None:
        """Stage every source shard before any billable worker is created."""

        if _SAFE_JOB_ID.fullmatch(job_id) is None:
            raise RuntimeError("invalid recovery target Job id")
        source_id = spec.recovery.source_job_id
        source_payload = await asyncio.to_thread(
            load_job_spec_journal,
            self._mgr.config.registry.path,
            source_id,
        )
        try:
            source_spec = JobSpec.model_validate(source_payload["spec"])
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError("recovery source JobSpec is invalid") from exc
        source_quiescent = await self._source_recovery_quiescent(source_id)
        self._validate_recovery_contract(
            source_payload,
            source_spec,
            spec,
            source_quiescent=source_quiescent,
        )
        if not spec.recovery.generation:
            raise RuntimeError(
                "recovery staging requires a checkpoint generation pinned "
                "by preflight"
            )

        root = self._recovery_staging_root() / job_id
        if root.exists():
            # A deterministic/idempotent resubmit may follow a Manager crash.
            # Never trust an uncommitted partial staging tree; rebuild it from
            # the authoritative S3 generation.
            if root.is_symlink() or not root.is_dir():
                raise RuntimeError("unsafe recovery staging target")
            await _await_owned_thread(
                self._remove_tree_with_retries,
                root,
            )
        store = self._checkpoint_store()
        staging_deadline = (
            time.monotonic() + _RECOVERY_STAGING_DEADLINE_SECONDS
        )
        staging_cancel = threading.Event()
        reserved = False
        reservation_bytes = 0
        reservation_objects = 0
        try:
            checkpoint_shards: dict[str, dict] = {}
            checkpoint_contract_metadata: dict[str, object] = {}
            max_staging = _positive_env_bytes(
                "ELASTIC_AGENT_MAX_RECOVERY_STAGING_BYTES",
                _DEFAULT_MAX_RECOVERY_STAGING_BYTES,
            )
            max_objects = _positive_env_bytes(
                "ELASTIC_AGENT_MAX_RECOVERY_STAGING_OBJECTS",
                _DEFAULT_MAX_RECOVERY_STAGING_OBJECTS,
            )
            (
                base_wrapper_objects,
                checkpoint_wrapper_objects,
            ) = self._recovery_wrapper_object_counts(
                list(spec.recovery.paths),
                spec.fanout.workers,
            )
            checkpoint_shard_wrappers = (
                (checkpoint_wrapper_objects - 1)
                // spec.fanout.workers
            )
            if spec.recovery.policy == "checkpoint":
                checkpoint_set = await _await_owned_thread(
                    store.resolve_checkpoint_set,
                    source_job_id=source_id,
                    generation=spec.recovery.generation,
                    deadline_monotonic=staging_deadline,
                    cancel_event=staging_cancel,
                    cancellation_event=staging_cancel,
                )
                checkpoint_shards = (
                    self._validate_resolved_checkpoint_set(
                        checkpoint_set,
                        source_spec=source_spec,
                        target_spec=spec,
                    )
                )
                checkpoint_contract_metadata = checkpoint_set["metadata"]
                total_bytes = int(checkpoint_set["total_bytes"])
                total_objects = int(checkpoint_set["total_objects"])
                await self._reserve_recovery_staging(
                    job_id=job_id,
                    total_bytes=total_bytes,
                    total_objects=total_objects,
                    wrapper_objects=checkpoint_wrapper_objects,
                )
                reservation_objects = (
                    total_objects + checkpoint_wrapper_objects
                )
                reservation_bytes = total_bytes
                reserved = True
            else:
                # Legacy prefixes do not have an immutable aggregate manifest.
                # Reserve the full per-Job allowance so two unknown-size
                # restores cannot concurrently consume the same disk/inode
                # budget. Base wrappers spend part of this physical-object cap.
                await self._reserve_recovery_staging(
                    job_id=job_id,
                    total_bytes=max_staging,
                    total_objects=max(
                        0,
                        max_objects - base_wrapper_objects,
                    ),
                    wrapper_objects=base_wrapper_objects,
                )
                reservation_objects = max_objects
                reservation_bytes = max_staging
                reserved = True
            root.mkdir(mode=0o700)
            total_staged = 0
            total_entries = 1  # the per-Job staging root
            for shard_index in range(spec.fanout.workers):
                await asyncio.to_thread(
                    self._validate_live_recovery_capacity,
                    root,
                    remaining_bytes=(
                        reservation_bytes - total_staged
                    ),
                    remaining_objects=(
                        reservation_objects - total_entries
                    ),
                )
                namespace = f"shard-{shard_index:05d}"
                destination = root / namespace
                kwargs = {
                    "source_job_id": source_id,
                    "worker_namespace": namespace,
                    "destination": destination,
                    "paths": list(spec.recovery.paths),
                }
                if spec.recovery.policy == "checkpoint":
                    shard_entry = checkpoint_shards[namespace]
                    kwargs["generation"] = str(
                        shard_entry.get("generation") or ""
                    )
                    kwargs["expected_manifest_sha256"] = str(
                        shard_entry.get("manifest_sha256") or ""
                    )
                    kwargs["expected_metadata"] = {
                        "resolved_commit": source_spec.setup.resolved_commit,
                        "recovery_contract_version": checkpoint_contract_metadata[
                            "recovery_contract_version"
                        ],
                        "recovery_contract_sha256": checkpoint_contract_metadata[
                            "recovery_contract_sha256"
                        ],
                        "shard_index": shard_index,
                    }
                    await _await_owned_thread(
                        store.restore_checkpoint,
                        **kwargs,
                        deadline_monotonic=staging_deadline,
                        cancel_event=staging_cancel,
                        cancellation_event=staging_cancel,
                    )
                    # The set/shard manifests were authenticated and the
                    # restore verifies every object size and SHA-256. Reuse
                    # those bounded totals instead of repeatedly walking every
                    # previously restored shard on the event loop.
                    total_staged += int(shard_entry["total_bytes"])
                    total_entries += (
                        int(shard_entry["total_objects"])
                        + checkpoint_shard_wrappers
                    )
                else:
                    await _await_owned_thread(
                        store.restore_legacy_collection,
                        **kwargs,
                        deadline_monotonic=staging_deadline,
                        cancel_event=staging_cancel,
                        cancellation_event=staging_cancel,
                    )
                    shard_bytes, shard_entries = await asyncio.to_thread(
                        self._measure_recovery_tree,
                        destination,
                    )
                    total_staged += shard_bytes
                    # The tree walk counts all descendants, including strict
                    # parents of requested paths, but not the shard root itself.
                    total_entries += shard_entries + 1
                if total_staged > max_staging:
                    raise RuntimeError(
                        "recovery staging byte limit exceeded"
                    )
                if total_entries > max_objects:
                    raise RuntimeError(
                        "recovery staging object limit exceeded"
                    )
                await asyncio.to_thread(
                    self._validate_live_recovery_capacity,
                    root,
                    remaining_bytes=(
                        reservation_bytes - total_staged
                    ),
                    remaining_objects=(
                        reservation_objects - total_entries
                    ),
                )
        except BaseException:
            cleaned = not root.exists()
            if root.exists() and not root.is_symlink() and root.is_dir():
                try:
                    await _await_owned_thread(
                        self._remove_tree_with_retries,
                        root,
                    )
                    cleaned = True
                except Exception:
                    logger.exception(
                        "failed to clean partial recovery staging for %s",
                        job_id,
                    )
            if reserved and cleaned:
                await self._release_recovery_staging_reservation(
                    job_id
                )
            raise

    @staticmethod
    def _measure_recovery_tree(root: Path) -> tuple[int, int]:
        """Measure one legacy shard once, without following worker links."""

        if root.is_symlink() or not root.is_dir():
            raise RuntimeError("unsafe recovery staging shard")
        total_bytes = 0
        total_entries = 0
        pending = [root]
        while pending:
            directory = pending.pop()
            with os.scandir(directory) as entries:
                for entry in entries:
                    total_entries += 1
                    entry_stat = entry.stat(follow_symlinks=False)
                    if stat.S_ISDIR(entry_stat.st_mode):
                        pending.append(Path(entry.path))
                    elif stat.S_ISREG(entry_stat.st_mode):
                        total_bytes += entry_stat.st_size
        return total_bytes, total_entries

    @staticmethod
    def _measure_recovery_paths(
        root: Path,
        paths: list[str],
    ) -> tuple[int, int]:
        """Measure exactly the selected roots represented by a shard manifest."""

        if root.is_symlink() or not root.is_dir():
            raise RuntimeError("unsafe recovery staging shard")
        total_bytes = 0
        total_objects = 0
        for relative in paths:
            selected = root / relative
            if selected.is_symlink() or not selected.is_dir():
                raise RuntimeError(
                    f"prepared recovery path is missing: {relative!r}"
                )
            pending = [selected]
            while pending:
                directory = pending.pop()
                total_objects += 1
                with os.scandir(directory) as entries:
                    for entry in entries:
                        entry_stat = entry.stat(follow_symlinks=False)
                        if stat.S_ISDIR(entry_stat.st_mode):
                            pending.append(Path(entry.path))
                        elif stat.S_ISREG(entry_stat.st_mode):
                            total_objects += 1
                            total_bytes += entry_stat.st_size
                        else:
                            raise RuntimeError(
                                "prepared recovery contains a non-regular entry"
                            )
        return total_bytes, total_objects

    def _recovery_transaction_payload(
        self,
        *,
        worker_id: str,
        job_id: str,
        shard_index: int,
        spec: JobSpec,
        total_bytes: int | None = None,
        total_objects: int | None = None,
    ) -> dict:
        payload = {
            "schema_version": 1,
            "job_id": job_id,
            "shard_index": shard_index,
            "target_dir": spec.setup.target_dir.rstrip("/"),
            "generation": spec.recovery.generation,
            "source_job_id": spec.recovery.source_job_id,
            "worker_id": worker_id,
            "run_user": self._mgr.config.worker.ssh_user,
            "recovery_contract_sha256": self._checkpoint_contract_hash(spec),
            "paths": list(spec.recovery.paths),
        }
        if total_bytes is not None and total_objects is not None:
            payload.update({
                "total_bytes": total_bytes,
                "total_objects": total_objects,
                "disk_reserve_bytes": _WORKER_RECOVERY_DISK_RESERVE_BYTES,
            })
        return payload

    @staticmethod
    def _recovery_transaction_command(
        mode: str,
        payload: dict,
    ) -> str:
        encoded = (
            encode_recovery_transaction_identity(payload)
            if mode == "reconcile-existing"
            else encode_recovery_transaction_payload(payload)
        )
        return (
            "/usr/bin/env -i "
            "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin "
            "PYTHONPATH=/opt/elastic-agent/framework/src "
            "/usr/bin/python3 -m "
            "elastic_agent.worker.recovery_transaction "
            f"--mode={shlex.quote(mode)} --payload={shlex.quote(encoded)}"
        )

    async def _execute_recovery_transaction(
        self,
        executor,
        *,
        mode: str,
        payload: dict,
        expected_descriptor_sha256: str | None,
    ) -> dict:
        rc, stdout, stderr = await executor.execute(
            self._recovery_transaction_command(mode, payload),
            timeout=1_800,
        )
        if rc != 0:
            raise RuntimeError(
                "remote recovery transaction failed "
                f"during {mode} (rc={rc}): {stderr[-500:]}"
            )
        if len(stdout.encode("utf-8", errors="replace")) > 64 * 1024:
            raise RuntimeError(
                "remote recovery transaction returned oversized output"
            )
        lines = [line for line in stdout.splitlines() if line.strip()]
        try:
            result = json.loads(lines[-1])
        except (IndexError, json.JSONDecodeError, TypeError) as exc:
            raise RuntimeError(
                "remote recovery transaction returned invalid output"
            ) from exc
        descriptor = (
            result.get("descriptor_sha256")
            if isinstance(result, dict)
            else None
        )
        if (
            not isinstance(descriptor, str)
            or re.fullmatch(r"[0-9a-f]{64}", descriptor) is None
            or (
                expected_descriptor_sha256 is not None
                and descriptor != expected_descriptor_sha256
            )
        ):
            raise RuntimeError(
                "remote recovery transaction identity verification failed"
            )
        return result

    @staticmethod
    def _remove_tree_with_retries(path: Path) -> None:
        """Retry bounded transient local cleanup without following symlinks."""

        last_error: OSError | None = None
        for attempt in range(3):
            try:
                shutil.rmtree(path)
                return
            except FileNotFoundError:
                return
            except OSError as exc:
                last_error = exc
                if attempt < 2:
                    time.sleep(0.05 * (attempt + 1))
        if last_error is not None:
            raise last_error

    async def restore_recovery(
        self,
        worker_id: str,
        job_id: str,
        spec: JobSpec,
        ctx: WorkerContext,
    ) -> None:
        """Stage and roll-forward atomically install one checkpoint shard."""

        shard_index = int(ctx.shard_index)
        if shard_index < 0 or shard_index >= spec.fanout.workers:
            raise RuntimeError("invalid recovery shard index")
        if (
            spec.recovery.policy != "checkpoint"
            or not spec.recovery.generation
        ):
            raise RuntimeError(
                "remote recovery requires a pinned immutable checkpoint"
            )
        source = (
            self._recovery_staging_root()
            / job_id
            / f"shard-{shard_index:05d}"
        )
        if not source.is_dir():
            raise RuntimeError("prepared recovery shard is missing")
        node = await self._mgr.registry.get(worker_id)
        if node is None:
            raise RuntimeError("recovery worker is missing from registry")
        provider = self._mgr.config.provider
        host = worker_management_host(node, provider_type=provider.type)
        if not host:
            raise RuntimeError("recovery worker has no management address")
        ssh_user = self._mgr.config.worker.ssh_user
        ssh_key = (
            provider.aliyun.ssh_key_path
            if provider.type == "aliyun"
            else provider.aws.ssh_key_path
        )
        ssh = (
            "ssh -o StrictHostKeyChecking=no "
            "-o UserKnownHostsFile=/dev/null -o BatchMode=yes "
            "-o ConnectTimeout=10 -o ServerAliveInterval=15 "
            "-o ServerAliveCountMax=2"
        )
        if ssh_key:
            ssh += f" -i {shlex.quote(ssh_key)}"
        remote_rsync = (
            "/usr/bin/rsync"
            if ssh_user == "root"
            else "/usr/bin/sudo -n /usr/bin/rsync"
        )
        from elastic_agent.core.bootstrap import SSHExecutor

        executor = SSHExecutor(
            host,
            user=ssh_user,
            key_path=ssh_key,
        )
        total_bytes, total_objects = await asyncio.to_thread(
            self._measure_recovery_paths,
            source,
            list(spec.recovery.paths),
        )
        transaction_worker_id = str(
            getattr(node, "instance_id", "") or worker_id
        )
        payload = self._recovery_transaction_payload(
            worker_id=transaction_worker_id,
            job_id=job_id,
            shard_index=shard_index,
            spec=spec,
            total_bytes=total_bytes,
            total_objects=total_objects,
        )
        descriptor = recovery_descriptor_sha256(payload)
        prepared = await self._execute_recovery_transaction(
            executor,
            mode="prepare",
            payload=payload,
            expected_descriptor_sha256=descriptor,
        )
        status = prepared.get("status")
        if status == "installed":
            return
        if status not in {"receiving", "installing"}:
            raise RuntimeError(
                "remote recovery transaction entered an invalid state"
            )
        if status == "receiving":
            for relative in spec.recovery.paths:
                local = source / relative
                remote_path = (
                    str(recovery_staged_path(payload, relative)).rstrip("/")
                    + "/"
                )
                remote = (
                    f"{ssh_user}@{host}:{shlex.quote(remote_path)}"
                )
                record_path, record = await _await_owned_thread(
                    self._create_recovery_transfer_record,
                    job_id=job_id,
                    shard_index=shard_index,
                    relative=relative,
                )
                proc: asyncio.subprocess.Process | None = None
                operation_error: BaseException | None = None
                settlement_error: BaseException | None = None
                stderr = b""
                returncode: int | None = None
                try:
                    transfer_env = dict(os.environ)
                    transfer_env[_RECOVERY_TRANSFER_ENV] = record["token"]
                    tokenized_remote_rsync = (
                        f"/usr/bin/env {_RECOVERY_TRANSFER_ENV}="
                        f"{record['token']} {remote_rsync}"
                    )
                    proc = await asyncio.create_subprocess_exec(
                        "rsync",
                        "-azc",
                        "--safe-links",
                        "--delete",
                        "--delete-excluded",
                        "--rsync-path",
                        tokenized_remote_rsync,
                        "-e",
                        ssh,
                        str(local) + "/",
                        remote,
                        stdout=asyncio.subprocess.DEVNULL,
                        stderr=asyncio.subprocess.PIPE,
                        start_new_session=True,
                        env=transfer_env,
                    )
                    await _await_owned_thread(
                        self._record_recovery_transfer_process,
                        record_path,
                        record,
                        proc.pid,
                    )
                    _stdout, stderr = await asyncio.wait_for(
                        proc.communicate(), timeout=3_600,
                    )
                    returncode = proc.returncode
                except BaseException as exc:
                    operation_error = exc
                finally:
                    if proc is None:
                        try:
                            # Cancellation can arrive after fork but before
                            # create_subprocess_exec returns the Process
                            # handle. Settle by the durable pre-spawn token;
                            # ``proc is None`` alone is not proof that no rsync
                            # session exists.
                            settle_task = asyncio.create_task(
                                self._settle_recorded_recovery_transfer(
                                    record_path,
                                    job_id=job_id,
                                )
                            )
                            await _await_owned_task(
                                settle_task,
                                operation_error_wins=True,
                            )
                        except BaseException as exc:
                            settlement_error = exc
                    else:
                        try:
                            await self._settle_live_recovery_transfer(
                                proc,
                                record_path,
                            )
                        except BaseException as exc:
                            settlement_error = exc
                if settlement_error is not None:
                    if operation_error is not None:
                        raise settlement_error from operation_error
                    raise settlement_error
                if operation_error is not None:
                    if isinstance(operation_error, asyncio.TimeoutError):
                        raise RuntimeError(
                            "recovery restore timed out"
                        ) from operation_error
                    raise operation_error
                if returncode != 0:
                    detail = (
                        stderr or b""
                    ).decode(errors="replace")[-500:]
                    raise RuntimeError(
                        "recovery rsync failed "
                        f"(rc={returncode}): {detail}"
                    )
        installed = await self._execute_recovery_transaction(
            executor,
            mode="install",
            payload=payload,
            expected_descriptor_sha256=descriptor,
        )
        if installed.get("status") != "installed":
            raise RuntimeError(
                "remote recovery transaction did not commit"
            )

    async def reconcile_recovery_install(
        self,
        worker_id: str,
        job_id: str,
        spec: JobSpec,
        shard_index: int,
    ) -> None:
        """Prove or finish a remote install before startup final collection."""

        if spec.recovery.policy == "none":
            return
        if (
            spec.recovery.policy != "checkpoint"
            or not spec.recovery.generation
        ):
            raise RuntimeError(
                "startup recovery has no pinned immutable checkpoint"
            )
        if (
            isinstance(shard_index, bool)
            or not isinstance(shard_index, int)
            or shard_index < 0
            or shard_index >= spec.fanout.workers
        ):
            raise RuntimeError(
                "startup recovery has no trustworthy shard identity"
            )
        node = await self._mgr.registry.get(worker_id)
        if node is None:
            raise RuntimeError("recovery worker is missing from registry")
        provider = self._mgr.config.provider
        host = worker_management_host(node, provider_type=provider.type)
        if not host:
            raise RuntimeError(
                "recovery worker has no management address"
            )
        ssh_user = self._mgr.config.worker.ssh_user
        ssh_key = (
            provider.aliyun.ssh_key_path
            if provider.type == "aliyun"
            else provider.aws.ssh_key_path
        )
        from elastic_agent.core.bootstrap import SSHExecutor

        executor = SSHExecutor(
            host,
            user=ssh_user,
            key_path=ssh_key,
        )
        payload = self._recovery_transaction_payload(
            worker_id=str(
                getattr(node, "instance_id", "") or worker_id
            ),
            job_id=job_id,
            shard_index=shard_index,
            spec=spec,
        )
        result = await self._execute_recovery_transaction(
            executor,
            mode="reconcile-existing",
            payload=payload,
            expected_descriptor_sha256=None,
        )
        if result.get("status") != "installed":
            raise RuntimeError(
                "remote recovery transaction is not installed"
            )

    def _recovery_cleanup_lock(self, job_id: str) -> asyncio.Lock:
        locks = getattr(self._mgr, "_recovery_cleanup_locks", None)
        if locks is None:
            locks = {}
            self._mgr._recovery_cleanup_locks = locks
        lock = locks.get(job_id)
        if lock is None:
            lock = asyncio.Lock()
            locks[job_id] = lock
        return lock

    async def _cleanup_recovery_once(self, job_id: str) -> None:
        if _SAFE_JOB_ID.fullmatch(job_id) is None:
            raise RuntimeError("invalid recovery target Job id")
        async with self._recovery_cleanup_lock(job_id):
            # The transfer journal is the deletion fence. A source tree and its
            # reservation remain quarantined until every recorded rsync session
            # has reached ESRCH, including a session inherited after restart.
            await self._settle_recovery_transfer_records(job_id)
            root = self._recovery_staging_root()
            target = root / job_id
            try:
                target.relative_to(root)
            except ValueError as exc:  # pragma: no cover - defensive
                raise RuntimeError("unsafe recovery cleanup target") from exc
            if target.exists():
                if target.is_symlink() or not target.is_dir():
                    raise RuntimeError("unsafe recovery cleanup target")
                await _await_owned_thread(
                    self._remove_tree_with_retries,
                    target,
                )
            await self._release_recovery_staging_reservation(job_id)

    def _schedule_recovery_cleanup_retry(self, job_id: str) -> None:
        tasks = getattr(
            self._mgr, "_recovery_transfer_cleanup_tasks", None,
        )
        if tasks is None:
            tasks = {}
            self._mgr._recovery_transfer_cleanup_tasks = tasks
        existing = tasks.get(job_id)
        if existing is not None and not existing.done():
            return

        async def retry() -> None:
            while True:
                await asyncio.sleep(5.0)
                try:
                    await self._cleanup_recovery_once(job_id)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:  # noqa: BLE001
                    quarantines = getattr(
                        self._mgr,
                        "_recovery_staging_quarantines",
                        {},
                    )
                    quarantines[job_id] = (
                        str(exc) or type(exc).__name__
                    )
                    self._mgr._recovery_staging_quarantines = quarantines
                    logger.warning(
                        "recovery staging for %s remains quarantined",
                        job_id,
                        exc_info=True,
                    )
                    continue
                quarantines = getattr(
                    self._mgr,
                    "_recovery_staging_quarantines",
                    {},
                )
                quarantines.pop(job_id, None)
                return

        task = asyncio.create_task(retry())
        tasks[job_id] = task

        def finished(done: asyncio.Task) -> None:
            if tasks.get(job_id) is done:
                tasks.pop(job_id, None)
            if done.cancelled():
                return
            error = done.exception()
            if error is not None:
                logger.error(
                    "recovery staging cleanup retry failed for %s",
                    job_id,
                    exc_info=(type(error), error, error.__traceback__),
                )

        task.add_done_callback(finished)

    async def cleanup_recovery(self, job_id: str) -> None:
        """Reclaim staging only after every durable transfer fence settles."""

        try:
            await self._cleanup_recovery_once(job_id)
        except asyncio.CancelledError:
            quarantines = getattr(
                self._mgr, "_recovery_staging_quarantines", {},
            )
            quarantines[job_id] = "cleanup cancelled"
            self._mgr._recovery_staging_quarantines = quarantines
            self._schedule_recovery_cleanup_retry(job_id)
            raise
        except Exception as exc:
            quarantines = getattr(
                self._mgr, "_recovery_staging_quarantines", {},
            )
            quarantines[job_id] = str(exc) or type(exc).__name__
            self._mgr._recovery_staging_quarantines = quarantines
            self._schedule_recovery_cleanup_retry(job_id)
            raise RuntimeError(
                "recovery staging is quarantined until its transfer "
                "process group is proven terminated"
            ) from exc

    async def _publish_checkpoint_generation(
        self,
        *,
        job_id: str,
        spec: JobSpec,
        worker_namespace: str,
        generation: str,
        shard_manifest: dict,
        shard_generations: dict[str, str] | None = None,
        deadline_monotonic: float | None = None,
    ) -> bool:
        """Publish one complete Job set and elect bounded GC ownership.

        Every attempt names the complete expected mapping. The store rebuilds
        readiness from S3, so a Manager crash after one shard commit cannot
        strand an otherwise complete generation in process-local memory. The
        return value elects one caller to prune incomplete history even when a
        peer shard never commits and no complete set can be published.
        """

        manifest_generation = str(
            shard_manifest.get("generation") or ""
        )
        shard_generations = dict(shard_generations or {
            f"shard-{index:05d}": manifest_generation
            for index in range(spec.fanout.workers)
        })
        expected_namespaces = {
            f"shard-{index:05d}"
            for index in range(spec.fanout.workers)
        }
        if set(shard_generations) != expected_namespaces:
            raise RuntimeError(
                "checkpoint set mapping does not match Job fanout"
            )
        if worker_namespace not in shard_generations:
            raise RuntimeError(
                "checkpoint worker namespace is outside Job fanout"
            )
        if shard_generations[worker_namespace] != manifest_generation:
            raise RuntimeError(
                "checkpoint shard generation identity mismatch"
            )
        metadata = {
            "resolved_commit": spec.setup.resolved_commit,
            "recovery_contract_version": _RECOVERY_CONTRACT_VERSION,
            "recovery_contract_sha256": (
                self._checkpoint_contract_hash(spec)
            ),
            "fanout_workers": spec.fanout.workers,
            "shard_by": spec.fanout.shard_by,
            "collect_paths": list(spec.collect.paths),
            "collect_exclude": list(spec.collect.exclude),
        }
        guard = getattr(self._mgr, "_checkpoint_publish_guard", None)
        if guard is None:
            guard = asyncio.Lock()
            self._mgr._checkpoint_publish_guard = guard
        states = getattr(self._mgr, "_checkpoint_publish_states", None)
        if states is None:
            states = {}
            self._mgr._checkpoint_publish_states = states
        completed = getattr(
            self._mgr, "_checkpoint_published_generations", None,
        )
        if completed is None:
            completed = set()
            self._mgr._checkpoint_published_generations = completed
        state_key = (job_id, generation)

        while True:
            async with guard:
                if state_key in completed:
                    return False
                # A Job-wide checkpoint loop never advances until the previous
                # generation's gather has settled. Its old in-memory barrier is
                # therefore disposable; durable shard manifests remain in S3
                # for restart seeding and later pruning.
                for old_key, old_state in list(states.items()):
                    if (
                        old_key[0] == job_id
                        and old_key != state_key
                        and not old_state["publishing"]
                    ):
                        states.pop(old_key, None)
                state = states.setdefault(
                    state_key,
                    {
                        "seen": set(),
                        "seeded": False,
                        "publishing": False,
                    },
                )
                if len(states) > 4096:
                    for old_key, old_state in list(states.items()):
                        if old_key != state_key and not old_state["publishing"]:
                            states.pop(old_key, None)
                            if len(states) <= 4096:
                                break
                state["seen"].add(worker_namespace)
                if state["publishing"]:
                    return False
                if (
                    state["seeded"]
                    and len(state["seen"]) < len(shard_generations)
                ):
                    return False
                state["seeded"] = True
                state["publishing"] = True
            try:
                publish_cancel = threading.Event()
                checkpoint_set_manifest = await _await_owned_thread(
                    self._checkpoint_store().publish_checkpoint_set,
                    job_id=job_id,
                    shard_generations=dict(shard_generations),
                    generation=generation,
                    metadata=metadata,
                    keep_last_n=(
                        spec.collect.checkpoint_keep_generations
                    ),
                    deadline_monotonic=deadline_monotonic,
                    cancel_event=publish_cancel,
                    cancellation_event=publish_cancel,
                )
            except IncompleteCheckpointSetError as exc:
                async with guard:
                    state = states.get(state_key)
                    if state is None:
                        return False
                    state["seen"].update(exc.committed_namespaces)
                    state["publishing"] = False
                    ready = len(state["seen"]) >= len(shard_generations)
                if ready:
                    continue
                return True
            except BaseException:
                async with guard:
                    state = states.get(state_key)
                    if state is not None:
                        state["publishing"] = False
                raise
            else:
                committed_at = str(
                    checkpoint_set_manifest.get("committed_at") or ""
                )
                try:
                    checkpoint_order = (
                        datetime.fromisoformat(
                            committed_at.replace("Z", "+00:00")
                        ),
                        generation,
                    )
                except ValueError as exc:
                    async with guard:
                        state = states.get(state_key)
                        if state is not None:
                            state["publishing"] = False
                    raise RuntimeError(
                        "checkpoint set returned an invalid committed_at"
                    ) from exc
                if checkpoint_order[0].tzinfo is None:
                    async with guard:
                        state = states.get(state_key)
                        if state is not None:
                            state["publishing"] = False
                    raise RuntimeError(
                        "checkpoint set committed_at has no timezone"
                    )

                # S3 COMMITTED and the Manager-local pointer form one
                # publication transaction for lifecycle decisions. Never
                # expose this generation to BatchJob/_maybe_finish, nor mark
                # the attempt complete, until the private journal has durably
                # recorded the exact set pointer. A retry can safely republish
                # the immutable S3 set and repeat this pointer write.
                persist_checkpoint = getattr(
                    self._mgr,
                    "_update_batch_checkpoint_generation",
                    None,
                )
                try:
                    if callable(persist_checkpoint):
                        await persist_checkpoint(
                            job_id,
                            generation,
                            committed_at,
                        )
                except BaseException:
                    async with guard:
                        state = states.get(state_key)
                        if state is not None:
                            state["publishing"] = False
                    raise

                async with guard:
                    states.pop(state_key, None)
                    completed.add(state_key)
                    # Jobs retained in memory are already bounded; cap this
                    # auxiliary idempotency cache independently as well.
                    if len(completed) > 4096:
                        completed.pop()
                batch = getattr(self._mgr, "_batch", None)
                job = (
                    batch.get_job(job_id)
                    if batch is not None else None
                )
                if job is not None:
                    current_order = None
                    latest_committed_at = str(
                        getattr(
                            job,
                            "latest_checkpoint_committed_at",
                            "",
                        )
                        or ""
                    )
                    if latest_committed_at:
                        current_order = (
                            datetime.fromisoformat(
                                latest_committed_at.replace(
                                    "Z", "+00:00",
                                )
                            ),
                            str(
                                getattr(
                                    job,
                                    "latest_checkpoint_generation",
                                    "",
                                )
                                or ""
                            ),
                        )
                    if current_order is None or checkpoint_order > current_order:
                        job.latest_checkpoint_generation = generation
                        job.latest_checkpoint_committed_at = committed_at
                return True

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

    async def stage_prompt_metadata(
        self,
        worker_id: str,
        task_id: str,
        job_id: str,
        prompt_metadata: dict[str, Any],
    ) -> None:
        await asyncio.to_thread(
            self._mgr.job_log_store.save_prompt_metadata,
            job_id=job_id,
            task_id=task_id,
            worker_id=worker_id,
            prompt_metadata=prompt_metadata,
        )

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
        self,
        worker_id: str,
        task_id: str,
        signal: str = "SIGTERM",
        *,
        scope: str = "group",
        escalate: bool = True,
    ) -> None:
        """Stop the exact process owned by a Job cancellation request."""
        if scope == "group" and escalate:
            await self._mgr.connection_manager.stop_process(
                worker_id, task_id, sig=signal,
            )
        else:
            await self._mgr.connection_manager.stop_process(
                worker_id,
                task_id,
                sig=signal,
                scope=scope,
                escalate=escalate,
            )

    async def quiesce_recovered_worker(
        self,
        worker_id: str,
        job_id: str,
        spec: JobSpec,
    ) -> None:
        """Prove framework, containers, and escaped writers are all stopped."""

        node = await self._mgr.registry.get(worker_id)
        if node is None:
            raise RuntimeError("recovery worker is missing from registry")
        if str(node.metadata.get("job_id") or "") != job_id:
            raise RuntimeError(
                "recovery worker Job identity does not match"
            )
        provider = self._mgr.config.provider
        host = worker_management_host(
            node, provider_type=provider.type,
        )
        if not host:
            raise RuntimeError(
                "recovery worker has no management address"
            )
        ssh_user = self._mgr.config.worker.ssh_user
        if ssh_user == "root":
            raise RuntimeError(
                "cannot prove process ownership quiescence for a root "
                "runtime user"
            )
        ssh_key = (
            provider.aliyun.ssh_key_path
            if provider.type == "aliyun"
            else provider.aws.ssh_key_path
        )
        from elastic_agent.core.bootstrap import SSHExecutor, _shell_quote

        executor = SSHExecutor(
            host,
            user=ssh_user,
            key_path=ssh_key,
        )
        process_request = base64.b64encode(json.dumps({
            "schema_version": 1,
            "job_id": job_id,
            "target_dir": spec.setup.target_dir.rstrip("/"),
            "run_user": ssh_user,
        }, sort_keys=True, separators=(",", ":")).encode()).decode("ascii")
        scanner = (
            "/usr/bin/python3 -c "
            + _shell_quote(_RECOVERY_PROCESS_SCANNER)
            + " "
            + _shell_quote(process_request)
        )
        quoted_run_user = _shell_quote(ssh_user)
        command = f"""set -Eeuo pipefail
export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
run_user={quoted_run_user}
run_uid="$(id -u -- "$run_user")"
case "$run_uid" in
  ''|*[!0-9]*) echo "invalid recovery run uid" >&2; exit 1 ;;
esac

unit_exists() {{
  [ "$(systemctl show -p LoadState --value "$1")" != "not-found" ]
}}
stop_and_mask() {{
  local unit="$1"
  if unit_exists "$unit"; then
    # Mask first so an activation race cannot respawn the unit. systemd can
    # report a non-zero command status while changing an already loaded unit;
    # final state, PID, and cgroup verification below remain authoritative.
    systemctl mask --runtime "$unit" >/dev/null 2>&1 || true
    systemctl daemon-reload
    systemctl stop "$unit" >/dev/null 2>&1 || true
  fi
}}
verify_unit() {{
  local unit="$1" load_state state pid cgroup file
  unit_exists "$unit" || return 0
  # UnitFileState describes the installed template and can remain ``static``
  # when one concrete template instance is runtime-masked. LoadState is the
  # authoritative runtime view after daemon-reload.
  load_state="$(systemctl show -p LoadState --value "$unit")"
  [ "$load_state" = "masked" ] || {{
    echo "unit was not runtime-masked: $unit ($load_state)" >&2
    return 1
  }}
  state="$(systemctl show -p ActiveState --value "$unit")"
  case "$state" in
    inactive|failed) ;;
    *) echo "unit remained active: $unit ($state)" >&2; return 1 ;;
  esac
  pid="$(systemctl show -p MainPID --value "$unit")"
  [ "${{pid:-0}}" = "0" ] || {{
    echo "unit retained MainPID: $unit ($pid)" >&2
    return 1
  }}
  cgroup="$(systemctl show -p ControlGroup --value "$unit")"
  [ -z "$cgroup" ] && return 0
  for file in \
    "/sys/fs/cgroup${{cgroup}}/cgroup.procs" \
    "/sys/fs/cgroup/systemd${{cgroup}}/tasks"; do
    if [ -f "$file" ] && grep -Eq '[0-9]' "$file"; then
      echo "unit cgroup retained processes: $unit" >&2
      return 1
    fi
  done
}}

framework=(
  ea-task-supervisor.service
  elastic-agent-task-supervisor.service
  ea-runtime.service
  elastic-agent-runtime.service
  "user@${{run_uid}}.service"
  cron.service
  crond.service
  atd.service
)
command -v loginctl >/dev/null 2>&1 || {{
  echo "loginctl is required to quiesce the Job user" >&2
  exit 1
}}
# A noninteractive SSH login commonly leaves systemd --user/dbus as sibling
# processes. Disable linger and mask the user manager before killing remaining
# Job-user processes, otherwise user@UID.service can immediately respawn them
# and make every startup recovery fail forever.
loginctl disable-linger "$run_user"
loaded="$(systemctl list-units --all --plain --no-legend \
  'ea-task@*.service' 'elastic-agent-task@*.service')"
while IFS=' ' read -r unit _rest; do
  [ -z "${{unit:-}}" ] && continue
  case "$unit" in
    ea-task@*.service|elastic-agent-task@*.service)
      framework+=("$unit")
      ;;
    *) echo "unexpected task unit name: $unit" >&2; exit 1 ;;
  esac
done <<< "$loaded"
for unit in "${{framework[@]}}"; do
  stop_and_mask "$unit"
done
for unit in "${{framework[@]}}"; do
  verify_unit "$unit"
done

stop_and_mask docker.socket
docker_needed=0
if systemctl is-active --quiet docker.service \
  || [ -S /run/docker.sock ] || [ -S /var/run/docker.sock ]; then
  docker_needed=1
fi
if [ "$docker_needed" = "1" ]; then
  command -v docker >/dev/null 2>&1 || {{
    echo "Docker is active but the CLI is unavailable" >&2
    exit 1
  }}
  docker info >/dev/null
  ids="$(docker ps -aq --no-trunc)"
  while IFS= read -r container_id; do
    [ -z "$container_id" ] && continue
    docker rm -f -- "$container_id" >/dev/null
  done <<< "$ids"
  [ -z "$(docker ps -aq --no-trunc)" ] || {{
    echo "Docker containers remained after forced removal" >&2
    exit 1
  }}
fi
for unit in docker.service containerd.service containerd.socket; do
  stop_and_mask "$unit"
done
for unit in \
  docker.socket docker.service containerd.service containerd.socket; do
  verify_unit "$unit"
done

sleep 1
{scanner}
"""
        rc, _stdout, stderr = await executor.execute(
            command, timeout=240,
        )
        if rc != 0:
            raise RuntimeError(
                "cannot prove recovered worker process quiescence "
                f"(rc={rc}): {stderr[-500:]}"
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

        metadata_shard = getattr(node, "metadata", {}).get(
            "shard_index",
        )
        if (
            isinstance(metadata_shard, int)
            and not isinstance(metadata_shard, bool)
            and metadata_shard >= 0
        ):
            return (
                f"shard-{metadata_shard:05d}",
                metadata_shard,
            )

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
        if spec.collect.checkpoint and not bucket:
            raise RuntimeError(
                "checkpoint collection requires "
                "ELASTIC_AGENT_RESULTS_S3_BUCKET"
            )
        # Mutable worker-direct keys cannot form an atomic generation. Exact
        # checkpoint mode intentionally relays through the Manager, which
        # hashes a quiescent rsync snapshot before publishing COMMITTED.json.
        worker_direct = bool(
            pc.type == "aws"
            and pc.aws.worker_instance_profile
            and bucket
            and not spec.collect.checkpoint
        )
        exclude = list(spec.collect.exclude)

        if worker_direct:
            from elastic_agent.core.bootstrap import SSHExecutor, _shell_quote
            ex = SSHExecutor(host, user=ssh_user, key_path=ssh_key, use_sudo=False)
            s3_root = self._s3_job_prefix(job_id, namespace)
            exclude_flags = " ".join(
                f"--exclude {_shell_quote(pattern)}"
                for pattern in exclude
            )
            rc, _stdout, stderr = await ex.execute(
                "command -v aws >/dev/null 2>&1",
                timeout=30,
            )
            if rc != 0:
                raise RuntimeError(
                    "worker S3 collect cannot start: awscli is unavailable "
                    f"(rc={rc}); worker bootstrap must install it before "
                    f"ea-runtime starts: {stderr[-300:]}"
                )
            for rel in paths:
                r = rel.rstrip("/")
                src = f"{spec.setup.target_dir.rstrip('/')}/{r}/"
                uri = f"s3://{bucket}/{s3_root}/{r}/"
                rc, _stdout, stderr = await ex.execute(
                    # ``aws s3 sync`` can skip an in-place rewrite whose size
                    # and mtime were preserved.  Collection correctness is more
                    # important than that metadata shortcut: recursive cp
                    # uploads every current regular file, so the awaited final
                    # collect always refreshes S3 from the stopped worker.
                    f"aws s3 cp {_shell_quote(src)} {_shell_quote(uri)} "
                    "--recursive --no-follow-symlinks --no-progress "
                    f"{exclude_flags}".rstrip(),
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

        if (
            _SAFE_JOB_ID.fullmatch(job_id) is None
            or _SAFE_JOB_ID.fullmatch(namespace) is None
        ):
            raise RuntimeError("unsafe Manager collection identity")
        dest_parent = (
            Path(self._mgr.collected_root) / job_id / "workers"
        )
        dest = dest_parent / namespace
        ssh = (
            "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "
            "-o BatchMode=yes -o ConnectTimeout=10 "
            "-o ServerAliveInterval=15 -o ServerAliveCountMax=2"
        )
        if ssh_key:
            ssh += f" -i {ssh_key}"
        max_bytes, max_objects, max_file_bytes = (
            self._collection_staging_limits()
        )
        reservation_id = f"{job_id}/{namespace}"
        max_bytes, max_objects = await self._reserve_collection_staging(
            reservation_id=reservation_id,
            max_bytes=max_bytes,
            max_objects=max_objects,
            # Defaults adapt downward on deliberately small state filesystems
            # (including test/dev tmpfs). Explicit operator limits are strict
            # reservations and fail before rsync if the capacity is absent.
            allow_smaller=(
                "ELASTIC_AGENT_MAX_COLLECTION_STAGING_BYTES"
                not in os.environ
                and "ELASTIC_AGENT_MAX_COLLECTION_STAGING_TOTAL_BYTES"
                not in os.environ
            ),
            concurrency_hint=max(
                1,
                int(getattr(spec.fanout, "workers", 1) or 1),
            ),
        )
        attempt: Path | None = None
        installed = False
        reservation_safe_to_release = False
        try:
            attempt = await asyncio.to_thread(
                self._new_collection_attempt,
                dest_parent,
                namespace,
            )
            inventoried_bytes = 0
            inventoried_objects = 0
            for rel in paths:
                clean_rel = rel.rstrip("/")
                local_path = attempt / clean_rel
                local_path.mkdir(parents=True, exist_ok=True)
                remote_path = (
                    f"{spec.setup.target_dir.rstrip('/')}/{clean_rel}"
                )
                remote_bytes, remote_objects = (
                    await self._remote_collection_inventory(
                        host=host,
                        ssh_user=ssh_user,
                        ssh_key=ssh_key,
                        source=remote_path,
                        exclude=exclude,
                        max_bytes=max_bytes,
                        max_objects=max_objects,
                        max_file_bytes=max_file_bytes,
                    )
                )
                inventoried_bytes += remote_bytes
                inventoried_objects += remote_objects
                if inventoried_bytes > max_bytes:
                    raise RuntimeError(
                        "remote collection inventory exceeds the aggregate "
                        "byte limit"
                    )
                if inventoried_objects > max_objects:
                    raise RuntimeError(
                        "remote collection inventory exceeds the aggregate "
                        "object limit"
                    )
                src = (
                    f"{ssh_user}@{host}:"
                    f"{remote_path}/"
                )
                rsync_args = ["rsync", "-azc", "--safe-links"]
                # Bound bytes that can arrive between 250ms monitor scans. This
                # is a safety-rate cap, not a quota substitute; the live tree
                # scan remains authoritative for total/single-file limits.
                rsync_args.append("--bwlimit=131072")
                # Every attempt starts empty. Deletion is still explicit so
                # repeated rsync passes cannot preserve a remote file that
                # disappeared during transfer.
                rsync_args.extend(["--delete", "--delete-excluded"])
                for pattern in exclude:
                    rsync_args.extend(["--exclude", pattern])
                rsync_args.extend(["-e", ssh, src, str(local_path) + "/"])
                rsync_deadline = time.monotonic() + 1800
                for transfer_attempt in range(
                    _COLLECTION_VANISHED_SOURCE_RETRIES + 1,
                ):
                    try:
                        remaining = rsync_deadline - time.monotonic()
                        if remaining <= 0:
                            raise asyncio.TimeoutError
                        proc = await asyncio.create_subprocess_exec(
                            # Checksum comparison catches same-size, same-mtime
                            # rewrites; the Manager-side uploader then uses
                            # SHA-256 as its authoritative S3 deduplication key.
                            *rsync_args,
                            stdout=asyncio.subprocess.DEVNULL,
                            stderr=asyncio.subprocess.PIPE,
                            env={**os.environ, "LC_ALL": "C"},
                            start_new_session=(os.name == "posix"),
                        )
                        stderr = await self._run_bounded_collection_rsync(
                            proc,
                            staging_root=attempt,
                            max_bytes=max_bytes,
                            max_objects=max_objects,
                            max_file_bytes=max_file_bytes,
                            timeout=remaining,
                        )
                    except asyncio.TimeoutError as exc:
                        raise RuntimeError(
                            f"rsync collect timed out for {rel!r} on "
                            f"{worker_id!r}"
                        ) from exc
                    except BaseException:
                        raise
                    if proc.returncode == 0:
                        break
                    if (
                        spec.collect.checkpoint
                        and proc.returncode == 24
                        and _rsync_reported_only_vanished_sources(stderr)
                        and transfer_attempt
                        < _COLLECTION_VANISHED_SOURCE_RETRIES
                    ):
                        # Never publish an rc=24 staging tree. A fresh pass over
                        # the same --delete destination must settle at rc=0,
                        # which both picks up the atomically published name and
                        # removes any stale receiver-side entry.
                        logger.warning(
                            "checkpoint rsync source changed during transfer "
                            "for job %s worker %s path %s; retrying (%d/%d)",
                            job_id,
                            worker_id,
                            rel,
                            transfer_attempt + 1,
                            _COLLECTION_VANISHED_SOURCE_RETRIES,
                        )
                        continue
                    detail = stderr.decode(errors="replace")[-500:]
                    raise RuntimeError(
                        f"rsync collect failed for {rel!r} "
                        f"(rc={proc.returncode}): {detail}"
                    )
            await asyncio.to_thread(
                self._validate_collection_tree,
                attempt,
                max_bytes=max_bytes,
                max_objects=max_objects,
                max_file_bytes=max_file_bytes,
            )
            manifest = self._manifest_bytes(
                job_id=job_id,
                worker_id=worker_id,
                namespace=namespace,
                shard_index=shard_index,
                paths=paths,
                destination="manager-rsync",
            )
            await asyncio.to_thread(
                self._write_manifest,
                attempt / _COLLECTION_MANIFEST,
                manifest,
            )

            if spec.collect.checkpoint:
                if shard_index is None:
                    raise RuntimeError(
                        "checkpoint collection requires a stable shard index"
                    )
                batch = getattr(self._mgr, "_batch", None)
                run = None
                if batch is not None:
                    job = batch.get_job(job_id)
                    run = job.runs.get(worker_id) if job is not None else None
                generation = (
                    str(
                        getattr(run, "checkpoint_generation", "")
                        or ""
                    ).strip()
                    or "final"
                )
                set_generation = (
                    str(
                        getattr(run, "checkpoint_set_generation", "")
                        or ""
                    ).strip()
                    or generation
                )
                shard_generations = (
                    dict(
                        getattr(
                            run,
                            "checkpoint_shard_generations",
                            {},
                        )
                        or {}
                    )
                    if run is not None
                    else {}
                )
                metadata = {
                    "resolved_commit": spec.setup.resolved_commit,
                    "recovery_contract_version": _RECOVERY_CONTRACT_VERSION,
                    "recovery_contract_sha256": (
                        self._checkpoint_contract_hash(spec)
                    ),
                    "shard_index": shard_index,
                }
                try:
                    checkpoint_deadline = (
                        time.monotonic()
                        + _S3_COLLECTION_DEADLINE_SECONDS
                    )
                    checkpoint_cancel = threading.Event()
                    shard_manifest = await _await_owned_thread(
                        self._checkpoint_store().commit,
                        job_id=job_id,
                        worker_namespace=namespace,
                        source_root=attempt,
                        paths=paths,
                        exclude=exclude,
                        generation=generation,
                        metadata=metadata,
                        deadline_monotonic=checkpoint_deadline,
                        cancel_event=checkpoint_cancel,
                        cancellation_event=checkpoint_cancel,
                    )
                    if run is not None:
                        run.last_checkpoint_generation = generation
                        if generation == "final" and batch is not None:
                            current_job = batch.get_job(job_id)
                            if current_job is not None:
                                # Final collectors can finish concurrently.
                                # Rebuild after this shard's durable COMMITTED
                                # write so the last finisher necessarily
                                # publishes the aggregate of every final
                                # pointer visible before all workers disappear.
                                shard_generations = {
                                    f"shard-{other.ctx.shard_index:05d}": (
                                        other.last_checkpoint_generation
                                        or "pending"
                                    )
                                    for other in current_job.runs.values()
                                }
                    maintenance_due = await self._publish_checkpoint_generation(
                        job_id=job_id,
                        spec=spec,
                        worker_namespace=namespace,
                        generation=set_generation,
                        shard_manifest=shard_manifest,
                        shard_generations=shard_generations or None,
                        deadline_monotonic=checkpoint_deadline,
                    )
                    # Bound failed/incomplete fanout generations only after the
                    # complete-set publication attempt. This is maintenance, not
                    # part of the durable commit transaction.
                    if maintenance_due:
                        try:
                            prune_cancel = threading.Event()
                            await _await_owned_thread(
                                self._checkpoint_store().prune_incomplete_generations,
                                job_id=job_id,
                                keep_per_shard=max(
                                    2,
                                    min(
                                        8,
                                        spec.collect.checkpoint_keep_generations,
                                    ),
                                ),
                                deadline_monotonic=checkpoint_deadline,
                                cancel_event=prune_cancel,
                                cancellation_event=prune_cancel,
                            )
                        except Exception:
                            logger.warning(
                                "checkpoint incomplete-generation pruning "
                                "deferred for job %s",
                                job_id,
                                exc_info=True,
                            )
                except Exception as exc:  # noqa: BLE001
                    raise RuntimeError(
                        f"S3 checkpoint commit failed for job "
                        f"{job_id!r}: {exc}"
                    ) from exc

            # Only a fully transferred, validated, and (for checkpoint mode)
            # durably committed attempt may replace the previous local
            # snapshot.
            await asyncio.to_thread(
                self._install_collection_attempt,
                attempt,
                dest,
            )
            installed = True
            reservation_safe_to_release = True
        except _UnsettledSubprocessError:
            # The still-reserved attempt is a fail-closed quarantine: deleting
            # it or returning its capacity while a child may still write would
            # permit unbounded reuse of the same Manager disk and inodes.
            raise
        except BaseException:
            if (
                attempt is not None
                and attempt.exists()
                and not installed
            ):
                await _await_owned_thread(
                    self._cleanup_partial_collection_path,
                    attempt,
                )
            reservation_safe_to_release = True
            raise
        finally:
            if reservation_safe_to_release:
                release = asyncio.create_task(
                    self._release_collection_staging_reservation(
                        reservation_id
                    )
                )
                await _await_owned_task(release)

        if bucket:
            uploader = getattr(self._mgr, "_s3_uploader", None)
            if uploader is None:
                raise RuntimeError(
                    "ELASTIC_AGENT_RESULTS_S3_BUCKET is configured, but the "
                    "Manager S3 uploader is not initialized"
                )
            try:
                # Public mutable results are derived only from the atomically
                # installed snapshot. In checkpoint mode the immutable commit
                # above deliberately wins the first independent S3 budget.
                upload_deadline = (
                    time.monotonic() + _S3_COLLECTION_DEADLINE_SECONDS
                )
                upload_cancel = threading.Event()
                await _await_owned_thread(
                    uploader.sync_worker,
                    job_id,
                    namespace,
                    deadline_monotonic=upload_deadline,
                    cancel_event=upload_cancel,
                    cancellation_event=upload_cancel,
                )
            except Exception as exc:  # noqa: BLE001
                raise RuntimeError(
                    f"Manager S3 collect failed for job {job_id!r}: {exc}"
                ) from exc

    async def release_ordinary_for_interrupt(
        self,
        worker_id: str,
        job_id: str,
        shard_index: int,
        *,
        collected: bool,
        collection_error: str | None,
    ) -> None:
        """Terminate one ordinary Worker but retain its crash tombstone."""

        from elastic_agent.core.registry import NodeStatus

        if _SAFE_JOB_ID.fullmatch(job_id) is None:
            raise ValueError("invalid interrupt cleanup Job id")
        if (
            isinstance(shard_index, bool)
            or not isinstance(shard_index, int)
            or shard_index < 0
        ):
            raise ValueError("invalid interrupt cleanup shard index")
        node = await self._mgr.registry.get(worker_id)
        if node is None:
            raise RuntimeError(
                f"interrupt cleanup worker {worker_id!r} is missing"
            )
        metadata = dict(node.metadata)
        if (
            str(metadata.get("job_id") or "") != job_id
            or metadata.get("shard_index") != shard_index
            or metadata.get("lease_id")
        ):
            raise RuntimeError(
                "interrupt cleanup worker does not match ordinary Job/shard "
                "ownership"
            )
        proof = {
            "schema": _INTERRUPT_CLEANUP_PROOF_SCHEMA,
            "job_id": job_id,
            "worker_id": worker_id,
            "instance_id": node.instance_id,
            "shard_index": shard_index,
            "collection_attempted": True,
            "collected": bool(collected),
            "collection_error": (
                str(collection_error)[:2_000]
                if collection_error
                else None
            ),
        }
        metadata["interrupt_cleanup_proof"] = proof
        updated = await self._mgr.registry.update(
            worker_id,
            metadata=metadata,
        )
        if (
            updated is None
            or updated.instance_id != node.instance_id
            or updated.metadata.get("interrupt_cleanup_proof") != proof
        ):
            raise RuntimeError(
                "interrupt cleanup tombstone intent was not persisted"
            )

        await self.scale_in([worker_id], retain_tombstones=True)
        terminal = await self._mgr.registry.get(worker_id)
        if (
            terminal is None
            or terminal.instance_id != node.instance_id
            or terminal.status != NodeStatus.TERMINATED
            or terminal.metadata.get("interrupt_cleanup_proof") != proof
        ):
            raise RuntimeError(
                "interrupt cleanup did not retain exact terminal proof"
            )

    async def finalize_interrupt_tombstones(
        self,
        job_id: str,
        worker_ids: list[str],
    ) -> None:
        """Remove only exact cold-interrupt tombstones after terminal commit."""

        from elastic_agent.core.registry import NodeStatus

        payload = await asyncio.to_thread(
            load_job_spec_journal,
            self._mgr.config.registry.path,
            job_id,
        )
        summary = payload.get("terminal_summary")
        intent = payload.get("interrupt_intent")
        if (
            payload.get("submission_state") not in {"suspended", "failed"}
            or not isinstance(summary, dict)
            or summary.get("done") is not True
            or summary.get("cleanup_pending") != 0
            or summary.get("interrupt_requested") is not True
            or not isinstance(intent, dict)
            or intent.get("schema") != 1
            or re.fullmatch(
                r"[0-9a-f]{64}",
                str(intent.get("idempotency_digest") or ""),
            )
            is None
        ):
            raise RuntimeError(
                "interrupt tombstones require an exact terminal Job journal"
            )

        for worker_id in dict.fromkeys(worker_ids):
            node = await self._mgr.registry.get(worker_id)
            if node is None:
                continue
            proof = node.metadata.get("interrupt_cleanup_proof")
            if (
                node.status != NodeStatus.TERMINATED
                or str(node.metadata.get("job_id") or "") != job_id
                or not isinstance(proof, dict)
                or proof.get("schema") != _INTERRUPT_CLEANUP_PROOF_SCHEMA
                or proof.get("job_id") != job_id
                or proof.get("worker_id") != worker_id
                or proof.get("instance_id") != node.instance_id
                or proof.get("shard_index")
                != node.metadata.get("shard_index")
            ):
                raise RuntimeError(
                    f"refusing to remove mismatched interrupt tombstone "
                    f"{worker_id!r}"
                )
            await self._mgr.remove_terminated_node_record(worker_id)
            if await self._mgr.registry.get(worker_id) is not None:
                raise RuntimeError(
                    f"interrupt tombstone {worker_id!r} was not removed"
                )

    async def scale_in(
        self,
        worker_ids: list[str],
        *,
        retain_tombstones: bool = False,
    ) -> None:
        requested = list(dict.fromkeys(worker_ids))
        if not requested:
            return

        async def terminate_and_remove() -> None:
            ordered_workers = sorted(requested)
            async with self._scale_in_state_lock:
                worker_locks = []
                for worker_id in ordered_workers:
                    worker_lock = self._scale_in_locks.setdefault(
                        worker_id, asyncio.Lock()
                    )
                    self._scale_in_lock_users[worker_id] = (
                        self._scale_in_lock_users.get(worker_id, 0) + 1
                    )
                    worker_locks.append((worker_id, worker_lock))
            acquired: list[asyncio.Lock] = []
            try:
                # Sorted acquisition prevents overlapping multi-worker cleanup
                # calls from deadlocking, while disjoint shard terminations stay
                # fully concurrent.
                for _worker_id, worker_lock in worker_locks:
                    await worker_lock.acquire()
                    acquired.append(worker_lock)
                async with self._scale_in_state_lock:
                    active_requested = [
                        worker_id
                        for worker_id in requested
                        if worker_id not in self._completed_scale_in_workers
                    ]
                if not active_requested:
                    return
                pending = [
                    worker_id
                    for worker_id in active_requested
                    if worker_id not in self._proven_terminated_workers
                ]
                if pending:
                    # Batch completion owns these ephemeral workers. A graceful
                    # drain only changes registry state and leaves the EC2
                    # instance billable.
                    raw_terminated = await self._mgr.scale_in(
                        node_ids=pending,
                        force=True,
                    )
                    try:
                        terminated = list(raw_terminated)
                    except TypeError as exc:
                        raise RuntimeError(
                            "Manager returned an invalid worker termination proof"
                        ) from exc
                    terminated_set = set(terminated)
                    pending_set = set(pending)
                    missing = sorted(pending_set - terminated_set)
                    extra = sorted(terminated_set - pending_set)
                    duplicates = len(terminated) != len(terminated_set)
                    if missing or extra or duplicates:
                        details = []
                        if missing:
                            details.append("missing=" + ",".join(missing))
                        if extra:
                            details.append("extra=" + ",".join(extra))
                        if duplicates:
                            details.append("duplicate worker ids")
                        raise RuntimeError(
                            "Manager returned an incomplete or invalid worker "
                            "termination proof (" + "; ".join(details) + ")"
                        )
                    self._proven_terminated_workers.update(pending)

                if not retain_tombstones:
                    # Job workers are disposable implementation details, not
                    # fleet history. Remove records only after every requested
                    # instance has an exact termination proof. Cold interrupt
                    # is the exception: its TERMINATED rows remain until the
                    # terminal Job journal commits.
                    for worker_id in active_requested:
                        if worker_id in self._proven_removed_workers:
                            continue
                        removed = await self._mgr.remove_node(worker_id)
                        remaining = await self._mgr.registry.get(worker_id)
                        if remaining is not None:
                            raise RuntimeError(
                                "Manager did not provide registry-removal "
                                f"proof for worker {worker_id!r} "
                                f"(remove_node returned {removed!r})"
                            )
                        # Absence is the authoritative postcondition. In a
                        # narrow concurrent-removal race Manager may return
                        # False because another cleanup removed the row first.
                        self._proven_removed_workers.add(worker_id)

                self._proven_terminated_workers.difference_update(
                    active_requested
                )
                self._proven_removed_workers.difference_update(active_requested)
                async with self._scale_in_state_lock:
                    self._completed_scale_in_workers.update(active_requested)
            finally:
                for worker_lock in reversed(acquired):
                    worker_lock.release()
                async with self._scale_in_state_lock:
                    for worker_id, worker_lock in worker_locks:
                        users = self._scale_in_lock_users[worker_id] - 1
                        if users:
                            self._scale_in_lock_users[worker_id] = users
                        else:
                            self._scale_in_lock_users.pop(worker_id, None)
                            self._completed_scale_in_workers.discard(worker_id)
                            if self._scale_in_locks.get(worker_id) is worker_lock:
                                self._scale_in_locks.pop(worker_id, None)

        # Once Manager has begun a destructive termination transaction, caller
        # cancellation must not strand the operation between exact cloud proof
        # and registry cleanup. The independently owned task also absorbs
        # repeated cancellation; a concrete child failure is still propagated.
        transaction = asyncio.create_task(terminate_and_remove())
        while not transaction.done():
            try:
                await asyncio.shield(transaction)
            except asyncio.CancelledError:
                if transaction.done() and transaction.cancelled():
                    raise
                continue
        transaction.result()


def _process_group_exists(process_group: int) -> bool:
    try:
        os.killpg(process_group, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


async def _terminate_subprocess_transaction(
    proc: asyncio.subprocess.Process,
) -> None:
    """Terminate, reap the leader, and prove its saved process group is gone."""

    pid = getattr(proc, "pid", None)
    process_group = (
        pid
        if (
            os.name == "posix"
            and isinstance(pid, int)
            and pid > 0
        )
        else None
    )
    wait = getattr(proc, "wait", None)
    if not callable(wait):
        if process_group is None and getattr(proc, "returncode", None) is not None:
            return
        raise _UnsettledSubprocessError(
            "subprocess does not expose a reap operation"
        )
    try:
        if process_group is not None:
            # start_new_session=True makes the original pid the stable pgid.
            # Do not call getpgid here: the leader may already have exited
            # while an ssh/rsync descendant still owns that group.
            os.killpg(process_group, signal.SIGTERM)
        elif proc.returncode is None:
            proc.terminate()
    except ProcessLookupError:
        pass
    try:
        await asyncio.wait_for(
            wait(),
            timeout=_SUBPROCESS_TERMINATE_TIMEOUT_SECONDS,
        )
    except asyncio.TimeoutError:
        try:
            if process_group is not None:
                os.killpg(process_group, signal.SIGKILL)
            elif proc.returncode is None:
                proc.kill()
        except ProcessLookupError:
            pass
        try:
            await asyncio.wait_for(
                wait(),
                timeout=_SUBPROCESS_TERMINATE_TIMEOUT_SECONDS,
            )
        except asyncio.TimeoutError as exc:
            raise _UnsettledSubprocessError(
                "subprocess leader could not be reaped"
            ) from exc

    if process_group is None:
        return

    # ``wait`` reaps only the leader. A child can keep the session alive after
    # that leader exits, so close the saved group id and prove ESRCH before
    # returning control to any staging cleanup.
    try:
        os.killpg(process_group, signal.SIGKILL)
    except ProcessLookupError:
        return
    deadline = (
        asyncio.get_running_loop().time()
        + _SUBPROCESS_GROUP_REAP_TIMEOUT_SECONDS
    )
    while _process_group_exists(process_group):
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            raise _UnsettledSubprocessError(
                "subprocess process group could not be proven terminated"
            )
        await asyncio.sleep(
            min(_SUBPROCESS_GROUP_POLL_SECONDS, remaining)
        )


async def _terminate_subprocess(proc: asyncio.subprocess.Process) -> None:
    """Own termination until process-group cleanup settles despite cancellation."""

    transaction = asyncio.create_task(
        _terminate_subprocess_transaction(proc)
    )
    try:
        await _await_owned_task(
            transaction,
            operation_error_wins=True,
        )
    except (asyncio.CancelledError, _UnsettledSubprocessError):
        raise
    except Exception as exc:
        raise _UnsettledSubprocessError(
            "subprocess termination could not be confirmed"
        ) from exc
