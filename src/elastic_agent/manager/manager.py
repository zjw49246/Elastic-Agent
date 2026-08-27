"""ElasticAgentManager — central orchestration hub.

T-016: Manager FastAPI skeleton — assembles registry, event bus, connection manager,
cloud provider, reconciler, and harness into a running FastAPI server.
"""

from __future__ import annotations

import asyncio
import logging
import re
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any

from elastic_agent.core.agent_type import AgentType
from elastic_agent.core.config import ElasticAgentConfig
from elastic_agent.core.event_bus import EventBus
from elastic_agent.core.job_log_store import JobLogStore
from elastic_agent.core.log_event_parser import LogEventParser
from elastic_agent.core.operations_logger import OperationsLogger
from elastic_agent.core.protocols.messages import Message
from elastic_agent.core.providers.base import (
    CloudProvider,
    InstanceNotFoundError,
    InstanceState,
)
from elastic_agent.core.release_evidence import (
    ReleaseEvidenceError,
    load_release_manifest,
)
from elastic_agent.core.reconciler import CloudReconciler
from elastic_agent.core.registry import NodeRecord, NodeRegistry, NodeStatus
from elastic_agent.core.task_registry import TaskRegistry
from elastic_agent.core.task_router import TaskRouter
from elastic_agent.core.task_scheduler import TaskScheduler
from elastic_agent.core.webhook_emitter import WebhookEmitter
from elastic_agent.harness.base import Harness
from elastic_agent.manager.connection import WorkerConnectionManager
from elastic_agent.worker.file_sync import StorageBackend

if TYPE_CHECKING:
    from elastic_agent.core.job_spec import (
        CollectSpec,
        FanoutSpec,
        JobSpec,
        RecoverySpec,
        SetupSpec,
        WorkerContext,
    )

logger = logging.getLogger(__name__)

RESUME_STOPPING_TIMEOUT_SECONDS = 300
RESUME_STOPPING_POLL_SECONDS = 10
BOUND_RECOVERY_RETRY_SECONDS = 30
BOUND_RECOVERY_SCAN_SECONDS = 10
BOUND_RECOVERY_COLLECT_TIMEOUT_SECONDS = 7_200
EIP_ALLOCATION_RECOVERY_STABLE_SCANS = 30
# AWS documents an eventually-consistent EC2 control plane and recommends
# bounded exponential/polling retries for newly-created resources.  The only
# ambiguous crash window here is a RESERVED lease with no persisted instance;
# quarantine EIP work for up to five minutes before declaring it launch-free.
BOUND_RECOVERY_STABLE_SCANS = 30
BOUND_DISCONNECT_GRACE_SECONDS = 30


@dataclass(frozen=True)
class _RecoveryCollectionSpec:
    """Strictly limited view of a persisted Job used during teardown only."""

    name: str
    setup: SetupSpec
    collect: CollectSpec
    fanout: FanoutSpec
    recovery: RecoverySpec
    _validated: JobSpec

    def worker_contexts(self) -> list[WorkerContext]:
        return self._validated.worker_contexts()

    def render_command(self, ctx: WorkerContext) -> list[str]:
        return self._validated.render_command(ctx)

    def _checkpoint_contract_source(self) -> JobSpec:
        return self._validated


def _load_recovery_collection_spec(raw_spec: object) -> _RecoveryCollectionSpec:
    """Validate a persisted spec for final collection, including one legacy mode.

    ``manager_distribute`` was accepted by older Managers.  New submission and
    resubmission continue to reject it, but restart recovery must still collect
    those already-running Jobs before destroying their workers.  Return only
    the fields consumed by ``ManagerFleetDriver.collect`` so this compatibility
    object cannot be passed back into provisioning, login, or run dispatch.
    """
    from elastic_agent.core.job_spec import JobSpec

    if not isinstance(raw_spec, dict):
        raise ValueError("persisted recovery JobSpec must be a JSON object")
    candidate = dict(raw_spec)
    raw_account = candidate.get("account")
    if isinstance(raw_account, dict):
        account = dict(raw_account)
        if account.get("mode") == "manager_distribute":
            account["mode"] = "worker_local_login"
        candidate["account"] = account
    validated = JobSpec.model_validate(candidate)
    return _RecoveryCollectionSpec(
        name=validated.name,
        setup=validated.setup,
        collect=validated.collect,
        fanout=validated.fanout,
        recovery=validated.recovery,
        _validated=validated,
    )


class ElasticAgentManager:
    """Central coordinator that owns all subsystems.

    Lifecycle:
        manager = ElasticAgentManager(config, provider, harness)
        await manager.start()   # load registry, start reconciler
        ...
        await manager.stop()    # graceful shutdown
    """

    def __init__(
        self,
        config: ElasticAgentConfig,
        provider: CloudProvider,
        harness: Harness | None = None,
        agent_type: AgentType | None = None,
        file_storage: StorageBackend | None = None,
    ) -> None:
        self.config = config
        self.provider = provider
        self.harness = harness
        self.file_storage = file_storage

        self.registry = NodeRegistry(config.registry.path)
        self.event_bus = EventBus()
        self.connection_manager = WorkerConnectionManager(self.registry)

        # Frontend-editable account pool + batch fan-out orchestrator (lazy).
        from pathlib import Path as _Path

        from elastic_agent.core.account_binding import AccountBindingStore
        from elastic_agent.core.account_store import AccountStore
        from elastic_agent.core.agent_api import AgentApiAccountStore
        from elastic_agent.core.binding_manager import BindingManager

        self.account_store = AccountStore(
            str(_Path(config.registry.path).with_name("accounts.json"))
        )
        self.agent_api_store = AgentApiAccountStore(
            _Path(config.registry.path).with_name("agent-api-accounts")
        )
        self.account_binding_store = AccountBindingStore(
            str(_Path(config.registry.path).with_name("bindings.json"))
        )
        self.binding_manager = BindingManager(
            provider,
            self.account_binding_store,
            recovery_notifier=self._mark_allocation_recovery_needed,
        )
        self._binding_recovery_ready = False
        self._startup_binding_recovery = False
        self._binding_recovery_scan_pending = True
        self._binding_recovery_scans_remaining = 1
        self._recovery_lease_ids: set[str] = set()
        self._recovery_unsafe_lease_ids: set[str] = set()
        self._recovery_unknown_lease_ids: set[str] = set()
        self._recovery_decommission_ids: set[str] = set()
        self._recovery_allocation_attempts: dict[str, int] = {}
        self._recovery_instances: dict[str, Any] = {}
        self._recovery_unbound_instances: dict[str, Any] = {}
        # A provider timeout/cancellation can occur after RunInstances was
        # accepted but before it returned an instance id.  Keep scanning the
        # exact controller/job tag scope for a full visibility quarantine.
        self._recovery_unbound_launch_scans: dict[str, int] = {}
        # Once registry/event publication succeeds, the durable NodeRecord is
        # the exact recovery handle.  On a fresh process, quarantine those ids
        # across eventual-consistency misses before declaring them gone.
        self._recovery_unbound_registry_scans: dict[str, int] = {}
        # Unlike the scan countdown, this count is durably journaled before
        # each ordinary Job create call and remains until the instance has a
        # durable exact registry handle (or its teardown is confirmed).  It
        # survives a second Manager crash after the orchestrator has already
        # marked the Job failed.
        self._unbound_launch_intent_counts: dict[str, int] = {}
        # Once an exact instance has consumed/transferred its no-id launch
        # intent, it may still need compensation or local cleanup retries.
        # Prevent those retries from decrementing another same-Job launch.
        self._resolved_unbound_instance_ids: set[str] = set()
        self._binding_recovery_task: asyncio.Task | None = None
        self._binding_recovery_wakeup = asyncio.Event()
        self._bound_disconnect_tasks: dict[str, asyncio.Task] = {}
        self._bound_disconnect_cancel_events: dict[str, asyncio.Event] = {}
        self._shutdown_event = asyncio.Event()
        self._binding_lock_fd: int | None = None
        self._instance_capacity_lock = asyncio.Lock()
        # Startup/live recovery and instance publication share this fence.
        # Otherwise a controller-tag scan can observe RunInstances after the
        # cloud accepted it but before the ordinary Job's registry row exists,
        # and mistake the current worker for a previous-process orphan.
        self._instance_lifecycle_lock = asyncio.Lock()
        # This is deliberately process-local.  Durable registry metadata alone
        # must never suppress startup cleanup after a Manager restart.
        self._current_unbound_instance_ids: set[str] = set()
        self._job_state_lock = asyncio.Lock()
        self._inflight_instance_creates = 0
        # A RunInstances result can become visible to the provider/registry
        # scan before the scale_out call releases its process-local inflight
        # reservation.  Track that overlap explicitly so admission counts the
        # instance once, not once as owned and again as inflight.
        self._inflight_visible_instance_ids: set[str] = set()
        self._instance_capacity_holds: dict[str, int] = {}
        self._account_allocator: Any = None
        self._account_login_coordinator: Any = None
        self._batch: Any = None
        from elastic_agent.core.job_batch import JobBatchQueue

        self.job_batch_queue = JobBatchQueue(self)

        # Optional S3 result upload: collected/<job_id>/ → s3://<bucket>/<prefix>/.
        # Enabled by ELASTIC_AGENT_RESULTS_S3_BUCKET.
        self.collected_root = str(_Path(config.registry.path).with_name("collected"))
        self._s3_uploader: Any = None
        self._s3_task: Any = None
        self.reconciler = CloudReconciler(
            provider=provider,
            registry=self.registry,
            reconcile_interval=config.monitor.reconcile_interval,
            on_bound_lost=self._on_reconciler_bound_lost,
            is_bound_released=self._is_reconciler_bound_released,
        )

        self.task_registry = TaskRegistry(config.task_registry.path)
        self.task_scheduler = TaskScheduler(
            node_registry=self.registry,
            task_registry=self.task_registry,
            harness=harness,
        )
        self.task_router = TaskRouter(
            task_registry=self.task_registry,
            node_registry=self.registry,
            connection_manager=self.connection_manager,
            agent_type=agent_type,
        )
        self.webhook_emitter = WebhookEmitter(
            dead_letter_path=config.webhook.dead_letter_path,
            retry_delays=config.webhook.retry_delays,
            send_timeout=config.webhook.send_timeout,
        )

        self.operations_logger = OperationsLogger(
            log_path=config.logging.operations_log,
            retention_days=config.logging.retention_days,
            log_level=config.logging.log_level,
        )
        self.log_event_parser = LogEventParser(
            buffer_size=config.external_api.trace_buffer_size,
        )
        self.job_log_store = JobLogStore(
            _Path(config.registry.path).with_name("job-logs"),
            max_entries=config.external_api.trace_buffer_size,
            retention_days=config.logging.retention_days,
        )
        try:
            self.job_log_store.prune()
        except Exception:  # noqa: BLE001
            logger.warning("Could not prune historical Job logs", exc_info=True)

        self.connection_manager.on_message = self._on_worker_message
        self.connection_manager.on_connect = self._on_worker_connect
        self.connection_manager.on_disconnect = self._on_worker_disconnect

        self._started = False
        # Populated only after the immutable release manifest passes validation
        # during start().  Health never invents evidence for a failed startup.
        self.release_evidence: dict[str, Any] | None = None

    async def start(self) -> None:
        if self._started:
            return
        try:
            release_evidence = await asyncio.to_thread(load_release_manifest)
        except ReleaseEvidenceError:
            logger.exception("Release evidence validation failed; refusing startup")
            raise
        self._acquire_binding_leader_lock()
        self._shutdown_event.clear()

        async def initialize_owned_components() -> None:
            """Initialize while the controller lock is held.

            This runs in its own task so cancellation of the ASGI lifespan
            cannot abandon an in-flight ``asyncio.to_thread`` cloud call.  The
            outer task waits for this transaction to settle, then quiesces all
            components before releasing the lock.
            """
            await self.registry.load()
            await self.task_registry.load()
            await self.account_store.load()
            native_account_ids = {
                account.id for account in await self.account_store.list()
            }
            api_account_ids = {
                account.id for account in await self.agent_api_store.list()
            }
            duplicate_account_ids = sorted(
                native_account_ids & api_account_ids
            )
            if duplicate_account_ids:
                raise RuntimeError(
                    "duplicate account ids across OAuth and Agent API stores: "
                    + ", ".join(duplicate_account_ids)
                )
            await self.account_binding_store.load()
            self.reconciler.set_controller_id(
                self.account_binding_store.controller_id
            )
            # Recovery staging is ephemeral and belongs to one Manager process.
            # With the controller lock held and before the API is ready, any
            # existing child is proof of an interrupted prior process.
            from elastic_agent.core.manager_fleet_driver import (
                ManagerFleetDriver,
            )
            await ManagerFleetDriver(self).cleanup_stale_recovery_staging()
            import os as _os
            bucket = _os.environ.get("ELASTIC_AGENT_RESULTS_S3_BUCKET")
            interval = 120.0
            if bucket:
                from elastic_agent.core.result_uploader import S3ResultUploader
                interval = float(
                    _os.environ.get(
                        "ELASTIC_AGENT_RESULTS_S3_INTERVAL", "120",
                    )
                )
                self._s3_uploader = S3ResultUploader(
                    bucket, self.collected_root,
                    prefix=_os.environ.get("ELASTIC_AGENT_RESULTS_S3_PREFIX", "jobs"),
                    region=self.config.provider.aws.region,
                )

            # Startup recovery may collect a previous controller's final
            # output.  Initialize the authoritative S3 sink first so relay-mode
            # recovery can await the upload instead of recording a false
            # permanent collection failure merely because startup ordering left
            # ``_s3_uploader`` unset.
            await self._initialize_binding_recovery()
            # Do not let the whole-tree periodic uploader acquire its sync lock
            # ahead of startup final collection. Recovery must settle old
            # billable workers before background mirroring starts.
            if (
                self._s3_uploader is not None
                and self.config.results.s3_periodic_enabled
            ):
                self._s3_task = asyncio.create_task(
                    self._s3_uploader.run_periodic(interval)
                )
                logger.info("S3 result upload enabled → s3://%s", bucket)
            elif self._s3_uploader is not None:
                logger.info(
                    "Periodic whole-tree S3 result mirroring is disabled; "
                    "awaited per-Job uploads remain enabled"
                )

            online_workers = set(self.connection_manager.connected_workers)
            await self.task_registry.recover(online_workers)

            await self.reconciler.start_periodic()

            # Load accepted JSON batches only after Job/account recovery is
            # initialized. The queue may then safely replay each queued item
            # through the canonical single-Job idempotency boundary.
            await self.job_batch_queue.start()

            # Publish health evidence only after every owned startup component
            # has completed.  A partially initialized process must never look
            # like a verified release to the platform.
            self.release_evidence = release_evidence
            self._started = True
            logger.info("ElasticAgentManager started")

        startup_task = asyncio.create_task(initialize_owned_components())
        try:
            await self._await_owned_task(startup_task)
        except BaseException:
            try:
                quiesce_task = asyncio.create_task(
                    self._quiesce_background_tasks()
                )
                await self._await_owned_task(quiesce_task)
            finally:
                self._started = False
                self.release_evidence = None
                self._release_binding_leader_lock()
            raise

    async def stop(self) -> None:
        quiesce_task = asyncio.create_task(self._quiesce_background_tasks())
        try:
            await self._await_owned_task(quiesce_task)
        finally:
            self._started = False
            self.release_evidence = None
            self._release_binding_leader_lock()
        logger.info("ElasticAgentManager stopped")

    @staticmethod
    async def _await_owned_task(task: asyncio.Task) -> Any:
        """Await an ownership transaction without propagating cancellation.

        Python cannot stop work already running in ``asyncio.to_thread``.  A
        cancelled lifespan/request therefore waits until the protected task
        really finishes and only then re-raises cancellation.  Repeated cancel
        requests are handled without ever cancelling the owned task.
        """
        pending_cancel: asyncio.CancelledError | None = None
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError as exc:
                if task.done() and task.cancelled():
                    raise
                if pending_cancel is None:
                    pending_cancel = exc
        if task.cancelled():
            raise asyncio.CancelledError
        result = task.result()
        if pending_cancel is not None:
            raise pending_cancel
        return result

    async def _quiesce_background_tasks(self) -> None:
        """Quiesce and await every owner task before controller unlock."""
        self._shutdown_event.set()
        self._binding_recovery_wakeup.set()
        for event in self._bound_disconnect_cancel_events.values():
            event.set()
        # Stop the reconciler first so it cannot start a fresh bound-loss
        # callback while BatchOrchestrator is settling its tracked lifecycle.
        # CloudReconciler wakes idle sleep but awaits any in-flight cloud
        # transaction instead of cancelling a boto3 thread.
        try:
            await self.reconciler.stop_periodic()
        except Exception:  # noqa: BLE001
            logger.exception("Reconciler shutdown failed")

        # Fence new batch item submissions before asking the underlying Job
        # orchestrator to settle its owned launch/cleanup tasks.
        try:
            await self.job_batch_queue.stop()
        except Exception:  # noqa: BLE001
            logger.exception("Job batch queue shutdown failed")

        # Batch launch/collect/cleanup tasks may still own a temporary EIP
        # worker.  Settle them before startup recovery is cancelled.
        if self._batch is not None:
            try:
                await self._batch.shutdown()
            except Exception:  # noqa: BLE001
                logger.exception("Batch shutdown failed")

        durable_tasks: list[asyncio.Task] = []
        if self._binding_recovery_task is not None:
            durable_tasks.append(self._binding_recovery_task)
            self._binding_recovery_task = None
        durable_tasks.extend(self._bound_disconnect_tasks.values())
        if durable_tasks:
            # These tasks may already be in an uncancellable cloud teardown.
            # Their stop events wake idle sleeps; await active transactions
            # rather than releasing the controller lock under a boto3 thread.
            await asyncio.gather(*durable_tasks, return_exceptions=True)
        self._bound_disconnect_tasks.clear()
        self._bound_disconnect_cancel_events.clear()

        cancellable_tasks: list[asyncio.Task] = []
        if self._s3_task is not None:
            cancellable_tasks.append(self._s3_task)
            self._s3_task = None
        recovery_cleanup_tasks = getattr(
            self, "_recovery_transfer_cleanup_tasks", {},
        )
        cancellable_tasks.extend(recovery_cleanup_tasks.values())
        recovery_cleanup_tasks.clear()
        for task in cancellable_tasks:
            task.cancel()
        if cancellable_tasks:
            await asyncio.gather(*cancellable_tasks, return_exceptions=True)

        try:
            await self.webhook_emitter.stop()
        except Exception:  # noqa: BLE001
            logger.exception("Webhook emitter shutdown failed")
        self.operations_logger.close()

    def _acquire_binding_leader_lock(self) -> None:
        """Fail fast when two Managers target the same durable binding store.

        The JSON store deliberately optimizes for a single control-plane
        process.  Without this OS lock, two uvicorn workers could both allocate
        an EIP or one process could mistake the other's live EC2 for an orphan.
        """
        if self._binding_lock_fd is not None:
            return
        import fcntl
        import os

        lock_path = self.account_binding_store.path.with_suffix(".manager.lock")
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            os.ftruncate(fd, 0)
            os.write(fd, str(os.getpid()).encode())
            os.fsync(fd)
        except BlockingIOError as exc:
            os.close(fd)
            raise RuntimeError(
                f"another ElasticAgentManager owns EIP bindings at {lock_path}"
            ) from exc
        except Exception:
            os.close(fd)
            raise
        self._binding_lock_fd = fd

    def _release_binding_leader_lock(self) -> None:
        if self._binding_lock_fd is None:
            return
        import fcntl
        import os

        fd = self._binding_lock_fd
        self._binding_lock_fd = None
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)

    @property
    def binding_recovery_ready(self) -> bool:
        """Whether startup proved that no prior EIP worker still owns resources."""
        return self._binding_recovery_ready

    async def _initialize_binding_recovery(self) -> None:
        """Recover leases/instances left by the previous Manager process.

        Batch jobs are currently in-memory, so an active durable lease at
        startup cannot have a live orchestrator owner.  We fail EIP Job
        reservations closed until every such lease and every tagged crash-window
        instance has been detached/terminated.  Normal non-EIP jobs are not
        blocked.
        """
        self._startup_binding_recovery = True
        self._recovery_unsafe_lease_ids.clear()
        self._recovery_unknown_lease_ids.clear()
        self._resolved_unbound_instance_ids.clear()
        self._current_unbound_instance_ids.clear()
        from elastic_agent.core.job_spec_store import (
            load_unbound_launch_intents,
        )

        async with self._job_state_lock:
            self._unbound_launch_intent_counts = await asyncio.to_thread(
                load_unbound_launch_intents,
                self.config.registry.path,
                self.account_binding_store.controller_id,
            )
        self._recovery_unbound_launch_scans = {
            job_id: BOUND_RECOVERY_STABLE_SCANS
            for job_id in self._unbound_launch_intent_counts
        }
        self._recovery_unbound_registry_scans = {}
        for node in await self.registry.list_all():
            job_id = str(node.metadata.get("job_id") or "")
            controller_id = str(
                node.metadata.get("controller_id") or ""
            )
            if (
                node.metadata.get("lease_id")
                or node.status == NodeStatus.TERMINATED
                or not job_id
                or (
                    controller_id
                    and controller_id
                    != self.account_binding_store.controller_id
                )
            ):
                continue
            self._recovery_unbound_registry_scans[node.node_id] = (
                BOUND_RECOVERY_STABLE_SCANS
            )
        active = await self.account_binding_store.list_leases(active_only=True)
        self._recovery_lease_ids = {lease.lease_id for lease in active}
        # Close the narrow crash window where cloud/lease cleanup committed but
        # the per-Job terminal summary did not. Released leases retain the exact
        # shard and collection outcome; merge that proof idempotently at startup.
        for lease in await self.account_binding_store.list_leases():
            if (
                lease.state == "released"
                and lease.recovery_collection_attempted
                and lease.job_id != "legacy-binding-migration"
            ):
                try:
                    await self._merge_recovered_terminal_worker(
                        job_id=lease.job_id,
                        worker_id=lease.worker_id or lease.instance_id or "",
                        shard_index=lease.slot,
                        collected=lease.recovery_collected,
                        collection_error=lease.recovery_collection_error,
                        worker_released=True,
                    )
                except FileNotFoundError:
                    pass
                except Exception:  # noqa: BLE001
                    logger.exception(
                        "Cannot reconcile released recovery lease %s",
                        lease.lease_id,
                    )
        for node in await self.registry.list_all():
            shard_index = node.metadata.get("shard_index")
            job_id = str(node.metadata.get("job_id") or "")
            if (
                node.status == NodeStatus.TERMINATED
                and job_id
                and isinstance(shard_index, int)
                and not isinstance(shard_index, bool)
            ):
                try:
                    collected, collection_error, is_interrupt_tombstone = (
                        self._interrupt_cleanup_outcome(
                            node,
                            job_id=job_id,
                            shard_index=shard_index,
                        )
                    )
                    merged = await self._merge_recovered_terminal_worker(
                        job_id=job_id,
                        worker_id=node.node_id,
                        shard_index=shard_index,
                        collected=collected,
                        collection_error=collection_error,
                        worker_released=True,
                    )
                    if merged and is_interrupt_tombstone:
                        current = await self.registry.get(node.node_id)
                        if (
                            current is not None
                            and current.instance_id == node.instance_id
                            and current.status == NodeStatus.TERMINATED
                            and current.metadata.get(
                                "interrupt_cleanup_proof"
                            )
                            == node.metadata.get("interrupt_cleanup_proof")
                        ):
                            await self.remove_terminated_node_record(
                                node.node_id
                            )
                except FileNotFoundError:
                    pass
                except Exception:  # noqa: BLE001
                    logger.exception(
                        "Cannot reconcile terminal recovery worker %s",
                        node.node_id,
                    )
        bindings = await self.account_binding_store.list_bindings()
        self._recovery_allocation_attempts = {
            binding.account_id: EIP_ALLOCATION_RECOVERY_STABLE_SCANS
            for binding in bindings
            if binding.last_operation == "allocate_eip"
            and not binding.eip_allocation_id
        }
        self._recovery_decommission_ids = {
            binding.account_id
            for binding in bindings
            if binding.last_operation == "decommission"
            or binding.state == "decommissioning"
        }
        # Even a persisted instance id can briefly be invisible immediately
        # after RunInstances.  A freshly restarted AWSProvider has no in-memory
        # `_recent_instances` hint, so treating the first NotFound as definitive
        # could release the lease while the EC2 appears moments later.  Keep a
        # bounded visibility quarantine for every active startup lease; a
        # visible tagged instance ends it after the first successful scan.
        self._binding_recovery_scans_remaining = (
            BOUND_RECOVERY_STABLE_SCANS if active else 1
        )
        # Do not make ASGI/systemd readiness wait for final rsync/S3 collection.
        # A recovered worker can legitimately need hours to upload a large
        # checkpoint, while the production unit has a much shorter startup
        # deadline.  Keep Agent-API/EIP admission fail-closed through
        # ``binding_recovery_ready`` and let the controller-lock-owned background
        # task perform the first inventory/cleanup pass immediately.
        self._binding_recovery_ready = False
        self._binding_recovery_wakeup.set()
        self._binding_recovery_task = asyncio.create_task(
            self._binding_recovery_loop()
        )

    async def _binding_recovery_loop(self) -> None:
        try:
            while not self._binding_recovery_ready:
                delay = (
                    BOUND_RECOVERY_SCAN_SECONDS
                    if self._binding_recovery_scan_pending
                    or self._recovery_allocation_attempts
                    else BOUND_RECOVERY_RETRY_SECONDS
                )
                try:
                    await asyncio.wait_for(
                        self._binding_recovery_wakeup.wait(),
                        timeout=delay,
                    )
                except asyncio.TimeoutError:
                    pass
                self._binding_recovery_wakeup.clear()
                if self._shutdown_event.is_set():
                    return
                try:
                    await self._recover_bound_resources_once()
                except asyncio.CancelledError:
                    raise
                except Exception:  # noqa: BLE001
                    # Startup no longer awaits this potentially multi-hour
                    # transaction. An unexpected provider/store failure must
                    # therefore keep admission closed and retry in this owned
                    # loop, rather than terminating the only recovery task.
                    self._binding_recovery_ready = False
                    logger.exception(
                        "Binding recovery pass failed; admission remains "
                        "blocked and recovery will retry"
                    )
        except asyncio.CancelledError:
            return

    def _recovery_instance_validation_error(self, instance, lease) -> str | None:
        """Return why a cloud instance is unsafe for destructive recovery."""
        tags = instance.tags
        if (
            tags.get(CloudProvider.MANAGED_TAG_KEY)
            != CloudProvider.MANAGED_TAG_VALUE
        ):
            return "instance is not tagged ManagedBy=elastic-agent"
        controller = tags.get("ElasticAgentController", "")
        tagged_lease = tags.get("ElasticAgentLease", "")
        tagged_account = tags.get("ElasticAgentAccount", "")
        tagged_job = tags.get("ElasticAgentJob", "")
        if lease is None:
            if controller != self.account_binding_store.controller_id:
                return "instance controller tag does not match this Manager"
            if not tagged_lease or not tagged_account or not tagged_job:
                return "controller-owned orphan lacks lease/account/Job tags"
            return None
        if lease.job_id == "legacy-binding-migration":
            if controller and controller != self.account_binding_store.controller_id:
                return "legacy instance has a foreign controller tag"
            if tagged_lease and tagged_lease != lease.lease_id:
                return "legacy instance lease tag conflicts with durable lease"
            if tagged_account and tagged_account != lease.account_id:
                return "legacy instance account tag conflicts with durable lease"
            return None
        if lease.instance_id and lease.instance_id != instance.instance_id:
            return "cloud instance conflicts with durable lease instance"
        if controller != self.account_binding_store.controller_id:
            return "instance controller tag does not match durable store"
        if tagged_lease != lease.lease_id:
            return "instance lease tag does not match durable lease"
        if tagged_account != lease.account_id:
            return "instance account tag does not match durable lease"
        if tagged_job != lease.job_id:
            return "instance Job tag does not match durable lease"
        return None

    def _recovery_lease_registry_validation_error(
        self,
        node: NodeRecord,
        lease,
    ) -> str | None:
        """Reject conflicting registry identity before enriching legacy rows.

        Older releases did not persist every ownership field in ``metadata``.
        Missing fields may be reconstructed only after the exact cloud instance
        has independently matched the durable controller/lease/account/Job
        tuple.  A present conflicting value is corruption, never a value to
        overwrite and then use as its own quiescence proof.
        """

        if node.instance_id != lease.instance_id:
            return "registry instance does not match durable lease"
        expected = {
            "job_id": lease.job_id,
            "account_id": lease.account_id,
            "lease_id": lease.lease_id,
            "controller_id": self.account_binding_store.controller_id,
        }
        for key, value in expected.items():
            observed = str(node.metadata.get(key) or "")
            if observed and observed != value:
                return (
                    f"registry {key} does not match durable recovery identity"
                )
        return None

    async def _detach_then_terminate_orphan(
        self, instance_id: str, binding
    ) -> None:
        """Always attempt exact termination even when EIP detach fails."""
        errors: list[Exception] = []
        if binding is not None and binding.eip_allocation_id:
            try:
                eip = await self.provider.describe_eip(
                    binding.eip_allocation_id
                )
                if eip is not None:
                    # A durable JSON handle alone is not ownership proof. Check
                    # the actual cloud tags (and safely migrate exact v1 tags)
                    # before disassociating any address. Instance termination
                    # remains independent below because its controller/account/
                    # lease tags were already verified by the caller.
                    binding, eip = (
                        await self.binding_manager.verify_binding_eip(
                            binding, eip
                        )
                    )
                if eip is not None and eip.instance_id == instance_id:
                    await self.provider.disassociate_eip(
                        eip.allocation_id,
                        association_id=eip.association_id,
                        expected_instance_id=instance_id,
                    )
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)
                logger.exception("Failed to detach EIP from orphan %s", instance_id)
        try:
            await self._terminate_instance_confirmed(instance_id)
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)
            logger.exception("Failed to terminate orphan %s", instance_id)
        if errors:
            raise RuntimeError(
                f"orphan {instance_id} cleanup incomplete: "
                + "; ".join(str(error) or type(error).__name__ for error in errors)
            )

    async def _terminate_instance_confirmed(self, instance_id: str) -> None:
        """Request termination and fence on terminal cloud readback."""

        # Once teardown/compensation starts, recovery may adopt and retry this
        # exact instance if the direct cloud request fails.  Keeping it marked
        # as a live current Job would make that retry incorrectly skip it.
        self._current_unbound_instance_ids.discard(instance_id)
        await self.provider.terminate_instance(instance_id)
        await self.binding_manager.wait_instance_terminated(instance_id)

    async def _recover_bound_resources_once(self) -> None:
        """Serialize one recovery pass with cloud create -> registry publication."""

        async with self._instance_lifecycle_lock:
            await self._recover_bound_resources_once_locked()

    async def _current_unbound_instance_is_live(self, instance) -> bool:
        """Prove an unbound cloud row still belongs to this process's live Job."""

        instance_id = instance.instance_id
        if instance_id not in self._current_unbound_instance_ids:
            return False
        tags = instance.tags
        job_id = str(tags.get("ElasticAgentJob") or "")
        controller_id = str(tags.get("ElasticAgentController") or "")
        if (
            instance.state == InstanceState.TERMINATED
            or tags.get("ElasticAgentLease")
            or not job_id
            or controller_id != self.account_binding_store.controller_id
        ):
            self._current_unbound_instance_ids.discard(instance_id)
            return False
        node_id = f"{instance.platform}:{instance.native_id}"
        node = await self.registry.get(node_id)
        if (
            node is None
            or node.instance_id != instance_id
            or str(node.metadata.get("job_id") or "") != job_id
            or str(node.metadata.get("controller_id") or "") != controller_id
            or node.metadata.get("lease_id")
            or node.status == NodeStatus.TERMINATED
        ):
            self._current_unbound_instance_ids.discard(instance_id)
            return False
        return True

    def _unbound_registry_instance_validation_error(
        self,
        instance,
        node: NodeRecord,
    ) -> str | None:
        """Validate an exact cloud row before recovering a durable Job node."""

        job_id = str(node.metadata.get("job_id") or "")
        if instance.instance_id != node.instance_id:
            return "exact lookup returned a different instance id"
        if (
            instance.tags.get(CloudProvider.MANAGED_TAG_KEY)
            != CloudProvider.MANAGED_TAG_VALUE
        ):
            return "instance is not tagged ManagedBy=elastic-agent"
        if (
            instance.tags.get("ElasticAgentController")
            != self.account_binding_store.controller_id
        ):
            return "instance controller tag does not match this Manager"
        if str(instance.tags.get("ElasticAgentJob") or "") != job_id:
            return "instance Job tag does not match durable registry"
        if instance.tags.get("ElasticAgentLease"):
            return "unbound registry node unexpectedly has a lease tag"
        return None

    async def _recover_bound_resources_once_locked(self) -> None:
        """One idempotent pass over startup leases and tagged orphan EC2s."""
        if self._binding_recovery_scan_pending:
            visible_unbound_jobs = {
                str(instance.tags.get("ElasticAgentJob") or "")
                for instance_id, instance
                in self._recovery_unbound_instances.items()
                if (
                    instance.state != InstanceState.TERMINATED
                    and instance_id
                    not in self._resolved_unbound_instance_ids
                )
            }
            try:
                instances = await self.provider.list_instances(filters={
                    CloudProvider.MANAGED_TAG_KEY: CloudProvider.MANAGED_TAG_VALUE,
                    "ElasticAgentController": self.account_binding_store.controller_id,
                })
                listed_instances = {
                    instance.instance_id: instance
                    for instance in instances
                }
                for instance in instances:
                    if (
                        instance.tags.get(CloudProvider.MANAGED_TAG_KEY)
                        != CloudProvider.MANAGED_TAG_VALUE
                        or
                        instance.tags.get("ElasticAgentController")
                        != self.account_binding_store.controller_id
                    ):
                        continue
                    lease_id = instance.tags.get("ElasticAgentLease", "")
                    if (
                        not lease_id
                        and instance.tags.get("ElasticAgentJob")
                        and instance.state != InstanceState.TERMINATED
                    ):
                        if await self._current_unbound_instance_is_live(instance):
                            # A process-local ownership token plus an exact
                            # registry/controller/Job match proves this is a
                            # current ordinary Job, not restart debris.
                            continue
                        # Ordinary Jobs now carry the same controller/job
                        # ownership tags as EIP Jobs.  Anything not proven live
                        # by this process is collected/terminated fail-closed.
                        self._recovery_unbound_instances[
                            instance.instance_id
                        ] = instance
                        if (
                            instance.instance_id
                            not in self._resolved_unbound_instance_ids
                        ):
                            visible_unbound_jobs.add(
                                str(
                                    instance.tags.get("ElasticAgentJob")
                                    or ""
                                )
                            )
                        continue
                    if lease_id and (
                        self._startup_binding_recovery
                        or lease_id in self._recovery_lease_ids
                    ):
                        lease = await self.binding_manager.get_lease(lease_id)
                        claimed = (
                            await self.account_binding_store.get_lease_by_instance(
                                instance.instance_id
                            )
                        )
                        if (
                            claimed is not None
                            and (
                                lease is None
                                or claimed.lease_id != lease.lease_id
                            )
                        ):
                            self._recovery_unsafe_lease_ids.add(
                                claimed.lease_id
                            )
                            logger.error(
                                "Refusing tagged recovery instance %s: cloud "
                                "lease %s conflicts with active durable lease %s",
                                instance.instance_id,
                                lease_id,
                                claimed.lease_id,
                            )
                            continue
                        if (
                            instance.state == InstanceState.TERMINATED
                            and self._lease_proves_released_instance(
                                lease,
                                instance_id=instance.instance_id,
                                account_id=str(
                                    instance.tags.get(
                                        "ElasticAgentAccount", ""
                                    )
                                ),
                                job_id=str(
                                    instance.tags.get("ElasticAgentJob", "")
                                ),
                            )
                        ):
                            # AWS still enumerates terminated instances after a
                            # fully committed release. They are history, not a
                            # new crash-window orphan.
                            continue
                        validation_error = self._recovery_instance_validation_error(
                            instance, lease
                        )
                        if validation_error:
                            if lease is not None:
                                self._recovery_unsafe_lease_ids.add(lease_id)
                            logger.error(
                                "Refusing tagged recovery instance %s: %s",
                                instance.instance_id,
                                validation_error,
                            )
                            continue
                        if lease is not None:
                            self._recovery_unsafe_lease_ids.discard(lease_id)
                            self._recovery_unknown_lease_ids.discard(lease_id)
                        self._recovery_instances[instance.instance_id] = instance
                # A crash can occur after the launch intent was cleared by
                # durable registry/event publication but before scale_out
                # returned ownership to the in-memory orchestrator.  Recover
                # every previous-process unbound NodeRecord by its exact id;
                # a single eventually-consistent tag-list miss is not proof
                # that the billable instance never existed.
                for node_id, remaining in list(
                    self._recovery_unbound_registry_scans.items()
                ):
                    node = await self.registry.get(node_id)
                    if node is None:
                        self._recovery_unbound_registry_scans.pop(
                            node_id, None
                        )
                        continue
                    candidate = listed_instances.get(node.instance_id)
                    if candidate is None:
                        try:
                            candidate = await self.provider.get_instance(
                                node.instance_id
                            )
                        except InstanceNotFoundError:
                            if remaining > 1:
                                self._recovery_unbound_registry_scans[
                                    node_id
                                ] = remaining - 1
                            else:
                                try:
                                    await self.registry.update(
                                        node_id,
                                        status=NodeStatus.TERMINATED,
                                    )
                                    await self.connection_manager.disconnect_worker(
                                        node_id
                                    )
                                except Exception:  # noqa: BLE001
                                    self._recovery_unbound_registry_scans[
                                        node_id
                                    ] = 1
                                    logger.exception(
                                        "Cannot settle missing recovered "
                                        "unbound node %s",
                                        node_id,
                                    )
                                else:
                                    shard_index = node.metadata.get(
                                        "shard_index"
                                    )
                                    job_id = str(
                                        node.metadata.get("job_id") or ""
                                    )
                                    if (
                                        job_id
                                        and isinstance(shard_index, int)
                                        and not isinstance(
                                            shard_index, bool
                                        )
                                    ):
                                        (
                                            collected,
                                            collection_error,
                                            is_interrupt_tombstone,
                                        ) = self._interrupt_cleanup_outcome(
                                            node,
                                            job_id=job_id,
                                            shard_index=shard_index,
                                        )
                                        if is_interrupt_tombstone:
                                            try:
                                                merged = await (
                                                    self
                                                    ._merge_recovered_terminal_worker(
                                                        job_id=job_id,
                                                        worker_id=node_id,
                                                        shard_index=shard_index,
                                                        collected=collected,
                                                        collection_error=(
                                                            collection_error
                                                        ),
                                                        worker_released=True,
                                                    )
                                                )
                                                if merged:
                                                    await (
                                                        self
                                                        .remove_terminated_node_record(
                                                            node_id
                                                        )
                                                    )
                                            except Exception:  # noqa: BLE001
                                                self._recovery_unbound_registry_scans[
                                                    node_id
                                                ] = 1
                                                logger.exception(
                                                    "Cannot reconcile missing "
                                                    "interrupt worker %s",
                                                    node_id,
                                                )
                                                continue
                                    self._recovery_unbound_registry_scans.pop(
                                        node_id, None
                                    )
                            continue
                        except Exception:  # noqa: BLE001
                            logger.exception(
                                "Cannot verify recovered unbound node %s "
                                "instance %s",
                                node_id,
                                node.instance_id,
                            )
                            continue
                    if candidate is None:
                        logger.error(
                            "Exact lookup returned no state for recovered "
                            "unbound node %s instance %s",
                            node_id,
                            node.instance_id,
                        )
                        continue
                    validation_error = (
                        self._unbound_registry_instance_validation_error(
                            candidate,
                            node,
                        )
                    )
                    if validation_error:
                        logger.error(
                            "Refusing exact unbound recovery for node %s "
                            "instance %s: %s",
                            node_id,
                            node.instance_id,
                            validation_error,
                        )
                        continue
                    if candidate.state == InstanceState.TERMINATED:
                        await self.registry.update(
                            node_id,
                            status=NodeStatus.TERMINATED,
                        )
                        shard_index = node.metadata.get("shard_index")
                        job_id = str(node.metadata.get("job_id") or "")
                        if (
                            job_id
                            and isinstance(shard_index, int)
                            and not isinstance(shard_index, bool)
                        ):
                            try:
                                current_node = await self.registry.get(
                                    node_id
                                )
                                collected = None
                                collection_error = None
                                is_interrupt_tombstone = False
                                if current_node is not None:
                                    (
                                        collected,
                                        collection_error,
                                        is_interrupt_tombstone,
                                    ) = self._interrupt_cleanup_outcome(
                                        current_node,
                                        job_id=job_id,
                                        shard_index=shard_index,
                                    )
                                merged = (
                                    await self._merge_recovered_terminal_worker(
                                    job_id=job_id,
                                    worker_id=node_id,
                                    shard_index=shard_index,
                                    collected=collected,
                                    collection_error=collection_error,
                                    worker_released=True,
                                    )
                                )
                                if merged and is_interrupt_tombstone:
                                    current_node = await self.registry.get(
                                        node_id
                                    )
                                    if (
                                        current_node is not None
                                        and current_node.instance_id
                                        == node.instance_id
                                        and current_node.status
                                        == NodeStatus.TERMINATED
                                    ):
                                        await self.remove_terminated_node_record(
                                            node_id
                                        )
                            except FileNotFoundError:
                                pass
                            except Exception:  # noqa: BLE001
                                logger.exception(
                                    "Cannot reconcile terminal worker %s",
                                    node_id,
                                )
                        await self.connection_manager.disconnect_worker(
                            node_id
                        )
                        self._recovery_unbound_registry_scans.pop(
                            node_id, None
                        )
                        continue
                    self._recovery_unbound_instances[
                        candidate.instance_id
                    ] = candidate
                # Upgrade/crash compatibility: an active durable lease may
                # reference a pre-controller instance that strict tag filters
                # cannot return.  Verify only that exact immutable id; never
                # enumerate or adopt another deployment's unscoped fleet.
                scan_had_unknown = False
                for lease_id in list(self._recovery_lease_ids):
                    lease = await self.binding_manager.get_lease(lease_id)
                    if (
                        lease is None
                        or not lease.instance_id
                    ):
                        continue
                    recovered_candidate = self._recovery_instances.get(
                        lease.instance_id
                    )
                    if recovered_candidate is not None:
                        validation_error = self._recovery_instance_validation_error(
                            recovered_candidate, lease
                        )
                        if validation_error:
                            self._recovery_unsafe_lease_ids.add(lease_id)
                            self._recovery_instances.pop(
                                lease.instance_id, None
                            )
                            logger.error(
                                "Refusing tagged recovery instance %s for "
                                "lease %s: %s",
                                lease.instance_id,
                                lease_id,
                                validation_error,
                            )
                        continue
                    try:
                        exact = await self.provider.get_instance(lease.instance_id)
                    except InstanceNotFoundError:
                        self._recovery_unknown_lease_ids.discard(lease_id)
                        continue
                    except Exception:  # noqa: BLE001
                        # A throttled/failed exact lookup is UNKNOWN, not proof
                        # that a persisted id is safe to terminate or gone. Do
                        # not advance the visibility quarantine on this pass.
                        self._recovery_unknown_lease_ids.add(lease_id)
                        scan_had_unknown = True
                        logger.exception(
                            "Cannot verify recovery instance %s for lease %s",
                            lease.instance_id,
                            lease_id,
                        )
                        continue
                    if exact is None:
                        self._recovery_unknown_lease_ids.discard(lease_id)
                        continue
                    self._recovery_unknown_lease_ids.discard(lease_id)
                    validation_error = self._recovery_instance_validation_error(
                        exact, lease
                    )
                    if validation_error:
                        self._recovery_unsafe_lease_ids.add(lease_id)
                        logger.error(
                            "Refusing exact recovery instance %s for lease %s: %s",
                            lease.instance_id,
                            lease_id,
                            validation_error,
                        )
                        continue
                    self._recovery_unsafe_lease_ids.discard(lease_id)
                    self._recovery_instances[exact.instance_id] = exact
                if not scan_had_unknown:
                    self._binding_recovery_scans_remaining = max(
                        0, self._binding_recovery_scans_remaining - 1
                    )
                # The controller-scoped list call itself succeeded even if an
                # unrelated exact bound-instance lookup was UNKNOWN.  Advance
                # each ordinary Job only when neither this scan nor a retained
                # exact recovery handle found one of its instances.
                for job_id, remaining in list(
                    self._recovery_unbound_launch_scans.items()
                ):
                    if job_id in visible_unbound_jobs:
                        continue
                    if remaining > 1:
                        self._recovery_unbound_launch_scans[
                            job_id
                        ] = remaining - 1
                        continue
                    try:
                        await self._resolve_unbound_launch_intent(
                            job_id,
                            all_launches=True,
                        )
                    except Exception:  # noqa: BLE001
                        # Clearing the durable intent is part of proving
                        # recovery complete.  Keep one scan pending and retry
                        # instead of admitting Agent API key use prematurely.
                        self._recovery_unbound_launch_scans[job_id] = 1
                        logger.exception(
                            "Cannot clear settled unbound launch intent for %s",
                            job_id,
                        )
                self._binding_recovery_scan_pending = (
                    self._binding_recovery_scans_remaining > 0
                    or bool(self._recovery_unbound_launch_scans)
                    or bool(self._recovery_unbound_registry_scans)
                )
            except Exception:  # noqa: BLE001
                logger.exception(
                    "Cannot scan cloud instances for interrupted EIP launches; "
                    "EIP jobs remain blocked"
                )

        # Link a tagged instance created just before a crash to its lease.  This
        # closes the RunInstances -> local registry/lease persistence window.
        for instance_id, instance in list(self._recovery_instances.items()):
            lease_id = instance.tags.get("ElasticAgentLease", "")
            lease = await self.binding_manager.get_lease(lease_id) if lease_id else None
            if lease is None or lease.state == "released":
                continue
            self._recovery_lease_ids.add(lease.lease_id)
            if lease.instance_id is None:
                claimed = await self.account_binding_store.get_lease_by_instance(
                    instance_id
                )
                if claimed is None or claimed.lease_id == lease.lease_id:
                    try:
                        await self.account_binding_store.update_lease(
                            lease.lease_id,
                            instance_id=instance_id,
                            worker_id=instance_id,
                            launch_uncertain=False,
                        )
                    except Exception:  # noqa: BLE001
                        logger.exception(
                            "Failed to recover instance %s into lease %s",
                            instance_id,
                            lease.lease_id,
                        )

        if self._binding_recovery_scan_pending:
            unresolved_launch = False
            for lease_id in self._recovery_lease_ids:
                lease = await self.binding_manager.get_lease(lease_id)
                if lease is not None and (
                    lease.instance_id is None
                    or lease.instance_id not in self._recovery_instances
                ):
                    unresolved_launch = True
                    break
            if not unresolved_launch:
                # Once every active lease's exact instance id is visible, the
                # ambiguous post-RunInstances crash window is closed and no
                # additional bound-launch quarantine is needed.  An unbound
                # timeout still keeps its independent tag scan active.
                self._binding_recovery_scans_remaining = 0
                self._binding_recovery_scan_pending = bool(
                    self._recovery_unbound_launch_scans
                    or self._recovery_unbound_registry_scans
                )

        async def cleanup_control_plane(lease) -> None:
            worker_id = self._durable_lease_worker_target(
                lease, expected_lease_id=lease.lease_id
            )
            if worker_id:
                await self.registry.update(worker_id, status=NodeStatus.TERMINATED)
                await self.connection_manager.disconnect_worker(worker_id)

        async def recover_lease(lease_id: str) -> None:
            if (
                lease_id in self._recovery_unsafe_lease_ids
                or lease_id in self._recovery_unknown_lease_ids
            ):
                return
            current = await self.binding_manager.get_lease(lease_id)
            if current is not None:
                if current.worker_id and not current.instance_id:
                    self._recovery_unsafe_lease_ids.add(lease_id)
                    logger.error(
                        "Refusing startup recovery for lease %s: durable "
                        "worker %s has no instance id",
                        lease_id,
                        current.worker_id,
                    )
                    return
                worker_id = current.worker_id or current.instance_id or ""
                node = await self.registry.get(worker_id) if worker_id else None
                if (
                    node is not None
                    and current.instance_id
                    and node.instance_id != current.instance_id
                ):
                    self._recovery_unsafe_lease_ids.add(lease_id)
                    logger.error(
                        "Refusing startup recovery for lease %s: durable "
                        "worker %s maps to registry instance %s, not %s",
                        lease_id,
                        worker_id,
                        node.instance_id,
                        current.instance_id,
                    )
                    return
                if node is not None and current.instance_id:
                    registry_error = (
                        self._recovery_lease_registry_validation_error(
                            node,
                            current,
                        )
                    )
                    if registry_error:
                        self._recovery_unsafe_lease_ids.add(lease_id)
                        logger.error(
                            "Refusing startup recovery for lease %s: %s",
                            lease_id,
                            registry_error,
                        )
                        return
            if (
                current is not None
                and self._binding_recovery_scan_pending
                and (
                    current.instance_id is None
                    or current.instance_id not in self._recovery_instances
                )
            ):
                # RunInstances is eventually consistent.  Do not declare a
                # lease harmless merely because its persisted id is not yet
                # visible; bounded tag scans must first close that crash window.
                return
            try:
                if (
                    current is not None
                    and current.launch_uncertain
                    and current.instance_id is None
                    and not self._binding_recovery_scan_pending
                ):
                    # The full set of successful controller-tag scans found no
                    # matching EC2, so a timeout-before-request is now proven
                    # harmless and the reservation can be released.
                    current = await self.account_binding_store.update_lease(
                        lease_id, launch_uncertain=False
                    )
                if (
                    current is not None
                    and current.instance_id
                    and not current.recovery_collection_attempted
                    and current.job_id != "legacy-binding-migration"
                ):
                    await self._collect_recovered_lease(current)
                    current = await self.binding_manager.get_lease(lease_id)
                if (
                    current is not None
                    and current.recovery_collection_attempted
                    and current.job_id != "legacy-binding-migration"
                ):
                    try:
                        await self._merge_recovered_terminal_worker(
                            job_id=current.job_id,
                            worker_id=(
                                current.worker_id
                                or current.instance_id
                                or ""
                            ),
                            shard_index=current.slot,
                            collected=current.recovery_collected,
                            collection_error=(
                                current.recovery_collection_error
                            ),
                            worker_released=False,
                        )
                    except FileNotFoundError:
                        pass
                    except Exception:  # noqa: BLE001
                        logger.exception(
                            "Cannot persist recovery collection proof for "
                            "lease %s",
                            lease_id,
                        )
                released = await self.binding_manager.release(
                    lease_id,
                    cleanup_worker=cleanup_control_plane,
                    expected_lease=current,
                )
                if released is None or released.state != "released":
                    raise RuntimeError(
                        f"startup cleanup for lease {lease_id!r} did not "
                        "return a released lease"
                    )
                worker_id = self._durable_lease_worker_target(
                    released, expected_lease_id=lease_id
                )
                if worker_id:
                    await self.remove_terminated_node_record(worker_id)
                if (
                    released.recovery_collection_attempted
                    and released.job_id != "legacy-binding-migration"
                ):
                    try:
                        await self._merge_recovered_terminal_worker(
                            job_id=released.job_id,
                            worker_id=(
                                released.worker_id
                                or released.instance_id
                                or ""
                            ),
                            shard_index=released.slot,
                            collected=released.recovery_collected,
                            collection_error=(
                                released.recovery_collection_error
                            ),
                            worker_released=True,
                        )
                    except FileNotFoundError:
                        pass
                    except Exception:  # noqa: BLE001
                        logger.exception(
                            "Cannot finalize recovery proof for lease %s",
                            lease_id,
                        )
                self._recovery_lease_ids.discard(lease_id)
                if released.instance_id:
                    self._recovery_instances.pop(released.instance_id, None)
            except Exception:  # noqa: BLE001
                logger.exception("Startup cleanup failed for EIP lease %s", lease_id)

        # A stopped Manager may leave many fan-out shards.  Recover them in
        # parallel so one unreachable SSH endpoint cannot serialize and delay
        # EIP detach/instance termination for every other shard.
        await asyncio.gather(
            *(recover_lease(lease_id) for lease_id in list(self._recovery_lease_ids))
        )

        async def recover_allocation(account_id: str) -> None:
            try:
                settled = await self.binding_manager.recover_pending_allocation(
                    account_id
                )
                if settled:
                    self._recovery_allocation_attempts.pop(account_id, None)
                    return
                remaining = self._recovery_allocation_attempts.get(account_id, 1) - 1
                if remaining > 0:
                    self._recovery_allocation_attempts[account_id] = remaining
                    return
                # The full visibility quarantine completed without finding a
                # tagged address.  Clear the ambiguous marker so a future
                # explicit ensure/job may retry allocation; do not allocate in
                # the recovery loop itself.
                await self.account_binding_store.update_binding(
                    account_id,
                    state="error",
                    error="no tagged EIP appeared during allocation recovery",
                    last_operation="ensure",
                )
                self._recovery_allocation_attempts.pop(account_id, None)
            except Exception:  # noqa: BLE001
                logger.exception(
                    "Startup/live EIP allocation recovery failed for account %s",
                    account_id,
                )

        await asyncio.gather(*(
            recover_allocation(account_id)
            for account_id in list(self._recovery_allocation_attempts)
        ))

        # A hard crash can happen after RunInstances but before even the lease's
        # instance_id is stored.  Tags are the recovery journal for that gap.
        for instance_id, instance in list(self._recovery_instances.items()):
            lease_id = instance.tags.get("ElasticAgentLease", "")
            lease = await self.binding_manager.get_lease(lease_id) if lease_id else None
            claimed = await self.account_binding_store.get_lease_by_instance(
                instance_id
            )
            if (
                claimed is not None
                and claimed.lease_id != lease_id
            ):
                self._recovery_unsafe_lease_ids.add(claimed.lease_id)
                logger.error(
                    "Refusing raw orphan cleanup for %s: cloud lease %s "
                    "conflicts with active durable lease %s",
                    instance_id,
                    lease_id,
                    claimed.lease_id,
                )
                continue
            if (
                instance.state == InstanceState.TERMINATED
                and self._lease_proves_released_instance(
                    lease,
                    instance_id=instance_id,
                    account_id=str(
                        instance.tags.get("ElasticAgentAccount", "")
                    ),
                    job_id=str(instance.tags.get("ElasticAgentJob", "")),
                )
            ):
                self._recovery_instances.pop(instance_id, None)
                continue
            if lease is not None and lease.state != "released" and lease.instance_id == instance_id:
                continue  # its durable release above will retry
            try:
                account_id = instance.tags.get("ElasticAgentAccount", "")
                binding = (
                    await self.binding_manager.get_binding(account_id)
                    if account_id
                    else None
                )
                await self._detach_then_terminate_orphan(instance_id, binding)
                await self.registry.update(instance_id, status=NodeStatus.TERMINATED)
                await self.connection_manager.disconnect_worker(instance_id)
                self._recovery_instances.pop(instance_id, None)
            except Exception:  # noqa: BLE001
                logger.exception("Startup cleanup failed for orphan %s", instance_id)

        async def recover_unbound(instance_id: str, instance) -> None:
            collection_error: str | None = None
            try:
                await self._collect_recovered_unbound(instance)
            except Exception as exc:  # noqa: BLE001
                collection_error = str(exc) or type(exc).__name__
                # Partial output may already be in S3 through periodic collect.
                # Collection loss is visible in logs but must never retain a
                # billable instance indefinitely.
                logger.exception(
                    "Startup result collection failed for unbound Job worker %s",
                    instance_id,
                )
            try:
                job_id = str(instance.tags.get("ElasticAgentJob") or "")
                recovery_node = await self.registry.get(instance_id)
                shard_index = (
                    recovery_node.metadata.get("shard_index")
                    if recovery_node is not None
                    else None
                )
                if (
                    re.fullmatch(
                        r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", job_id,
                    )
                    and isinstance(shard_index, int)
                    and not isinstance(shard_index, bool)
                ):
                    try:
                        await self._merge_recovered_terminal_worker(
                            job_id=job_id,
                            worker_id=instance_id,
                            shard_index=shard_index,
                            collected=collection_error is None,
                            collection_error=collection_error,
                            worker_released=False,
                        )
                    except FileNotFoundError:
                        pass
                    except Exception:  # noqa: BLE001
                        logger.exception(
                            "Cannot persist recovered worker collection proof "
                            "for %s",
                            instance_id,
                        )
                await self._terminate_instance_confirmed(instance_id)
                registry_node_ids: list[str] = []
                for node_id in self._recovery_unbound_registry_scans:
                    node = await self.registry.get(node_id)
                    if node is not None and node.instance_id == instance_id:
                        registry_node_ids.append(node_id)
                if (
                    not registry_node_ids
                    and job_id in self._unbound_launch_intent_counts
                ):
                    # One cloud row proves one accepted create.  Resolve it
                    # only after terminal readback; additional same-Job
                    # uncertain creates keep their independent scan fence.
                    await self._resolve_unbound_launch_intent(
                        job_id,
                        instance_id=instance_id,
                    )
                for node_id in registry_node_ids:
                    self._recovery_unbound_registry_scans.pop(
                        node_id, None
                    )
                self._binding_recovery_scan_pending = (
                    self._binding_recovery_scans_remaining > 0
                    or bool(self._recovery_unbound_launch_scans)
                    or bool(self._recovery_unbound_registry_scans)
                )
                recovered_node_ids = set(registry_node_ids)
                recovered_node_ids.add(instance_id)
                for node_id in recovered_node_ids:
                    node = await self.registry.get(node_id)
                    if (
                        node is not None
                        and node.instance_id == instance_id
                    ):
                        await self.registry.update(
                            node_id, status=NodeStatus.TERMINATED
                        )
                        await self.connection_manager.disconnect_worker(
                            node_id
                        )
                self._recovery_unbound_instances.pop(instance_id, None)
                self._resolved_unbound_instance_ids.discard(instance_id)
                if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", job_id):
                    try:
                        if (
                            isinstance(shard_index, int)
                            and not isinstance(shard_index, bool)
                        ):
                            await self._merge_recovered_terminal_worker(
                                job_id=job_id,
                                worker_id=instance_id,
                                shard_index=shard_index,
                                collected=collection_error is None,
                                collection_error=collection_error,
                                worker_released=True,
                            )
                    except FileNotFoundError:
                        # A legacy/manual tagged instance may have no JobSpec.
                        pass
                    except Exception:  # noqa: BLE001
                        logger.exception(
                            "Could not persist recovered terminal state for %s",
                            job_id,
                        )
                for node_id in recovered_node_ids:
                    node = await self.registry.get(node_id)
                    if (
                        node is not None
                        and node.instance_id == instance_id
                        and node.status == NodeStatus.TERMINATED
                    ):
                        await self.remove_terminated_node_record(node_id)
            except Exception:  # noqa: BLE001
                logger.exception(
                    "Startup termination failed for unbound Job worker %s",
                    instance_id,
                )

        await asyncio.gather(*(
            recover_unbound(instance_id, instance)
            for instance_id, instance in list(
                self._recovery_unbound_instances.items()
            )
        ))

        # A decommission is durable administrative intent.  Complete it after
        # lease cleanup, including the crash window where AWS released the EIP
        # but the local binding record was not removed yet.
        for account_id in list(self._recovery_decommission_ids):
            try:
                removed = await self.binding_manager.decommission(account_id)
                if removed or await self.binding_manager.get_binding(account_id) is None:
                    self._recovery_decommission_ids.discard(account_id)
            except Exception:  # noqa: BLE001
                logger.exception(
                    "Startup decommission retry failed for account %s", account_id
                )

        self._binding_recovery_ready = (
            not self._binding_recovery_scan_pending
            and not self._recovery_lease_ids
            and not self._recovery_instances
            and not self._recovery_unbound_instances
            and not self._recovery_unbound_launch_scans
            and not self._recovery_unbound_registry_scans
            and not self._recovery_decommission_ids
            and not self._recovery_allocation_attempts
        )
        if self._binding_recovery_ready:
            self._startup_binding_recovery = False

    def _mark_allocation_recovery_needed(self, account_id: str) -> None:
        """Wake bounded adoption after an ambiguous AllocateAddress call."""
        self._recovery_allocation_attempts[account_id] = (
            EIP_ALLOCATION_RECOVERY_STABLE_SCANS
        )
        self._binding_recovery_ready = False
        self._ensure_binding_recovery_task()

    @staticmethod
    def _interrupt_cleanup_outcome(
        node: NodeRecord,
        *,
        job_id: str,
        shard_index: int,
    ) -> tuple[bool | None, str | None, bool]:
        """Validate one ordinary cold-interrupt registry tombstone."""

        proof = node.metadata.get("interrupt_cleanup_proof")
        valid = bool(
            isinstance(proof, dict)
            and proof.get("schema") == 1
            and proof.get("job_id") == job_id
            and proof.get("worker_id") == node.node_id
            and proof.get("instance_id") == node.instance_id
            and proof.get("shard_index") == shard_index
            and proof.get("collection_attempted") is True
            and isinstance(proof.get("collected"), bool)
        )
        if not valid:
            return None, None, False
        error = proof.get("collection_error")
        return (
            proof["collected"],
            str(error)[:2_000] if error else None,
            True,
        )

    async def _merge_recovered_terminal_worker(
        self,
        *,
        job_id: str,
        worker_id: str,
        shard_index: int,
        collected: bool | None,
        collection_error: str | None,
        worker_released: bool,
    ) -> bool:
        """Durably merge one startup-recovered shard into the Job journal.

        Startup recovery settles fanout workers concurrently and may itself be
        interrupted. A whole-summary overwrite loses whichever shard completed
        first. Merge by the durable shard index under the Job-state lock instead;
        successful collection/release evidence is monotonic across retries.
        """

        from elastic_agent.core.job_spec_store import (
            load_job_spec_journal,
            update_job_state,
        )

        if (
            re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", job_id)
            is None
        ):
            raise ValueError("invalid recovered Job id")
        if (
            isinstance(shard_index, bool)
            or not isinstance(shard_index, int)
            or shard_index < 0
        ):
            raise ValueError("invalid recovered shard index")

        def merge() -> bool:
            payload = load_job_spec_journal(
                self.config.registry.path, job_id,
            )
            persisted_summary = payload.get("terminal_summary")
            persisted_intent = payload.get("interrupt_intent")
            terminal_interrupt = bool(
                isinstance(persisted_summary, dict)
                and persisted_summary.get("done") is True
                and persisted_summary.get("cleanup_pending") == 0
                and persisted_summary.get("interrupt_requested") is True
                and persisted_summary.get("state")
                in {"suspended", "failed"}
                and isinstance(persisted_intent, dict)
                and persisted_intent.get("schema") == 1
                and re.fullmatch(
                    r"[0-9a-f]{64}",
                    str(
                        persisted_intent.get("idempotency_digest")
                        or ""
                    ),
                )
                is not None
                and payload.get("submission_state")
                == persisted_summary.get("state")
            )
            if terminal_interrupt:
                # A prior recovery pass already committed the exact checkpoint
                # (or fail-closed absence) and zero-cleanup proof. Reliable
                # cleanup/tombstone replays are no-ops.
                return True
            recovery_spec = _load_recovery_collection_spec(payload["spec"])
            expected_workers = recovery_spec.fanout.workers
            if shard_index >= expected_workers:
                raise ValueError(
                    "recovered shard index is outside persisted fanout"
                )
            raw_summary = payload.get("terminal_summary")
            summary = raw_summary if isinstance(raw_summary, dict) else {}
            raw_workers = summary.get("terminal_workers")
            workers_by_shard: dict[int, dict[str, Any]] = {}
            if isinstance(raw_workers, list):
                for raw_worker in raw_workers:
                    if not isinstance(raw_worker, dict):
                        raise ValueError(
                            "persisted terminal worker proof is invalid"
                        )
                    index = raw_worker.get("shard_index")
                    if (
                        isinstance(index, bool)
                        or not isinstance(index, int)
                        or index < 0
                        or index >= expected_workers
                        or index in workers_by_shard
                    ):
                        raise ValueError(
                            "persisted terminal shard proof is invalid"
                        )
                    workers_by_shard[index] = {
                        "worker_id": str(
                            raw_worker.get("worker_id") or ""
                        )[:256],
                        "shard_index": index,
                        "phase": "failed",
                        "task_id": str(
                            raw_worker.get("task_id") or ""
                        )[:256],
                        "error": (
                            str(raw_worker.get("error"))[:2_000]
                            if raw_worker.get("error")
                            else None
                        ),
                        "final_collected": (
                            raw_worker.get("final_collected") is True
                            or (
                                "final_collected" not in raw_worker
                                and "collection_error" in raw_worker
                                and not raw_worker.get("collection_error")
                            )
                        ),
                        "collection_error": (
                            str(raw_worker.get("collection_error"))[:2_000]
                            if raw_worker.get("collection_error")
                            else None
                        ),
                        "cleanup_error": (
                            str(raw_worker.get("cleanup_error"))[:2_000]
                            if raw_worker.get("cleanup_error")
                            else None
                        ),
                        "worker_released": (
                            raw_worker.get("worker_released") is True
                            or (
                                summary.get("done") is True
                                and summary.get("cleanup_pending") == 0
                            )
                        ),
                    }

            existing = workers_by_shard.get(shard_index)
            if existing is None and collected is None:
                return False
            recovered_worker_id = (
                str(worker_id)[:256]
                or f"recovered-shard-{shard_index:05d}"
            )
            if existing is None:
                existing = {
                    "worker_id": recovered_worker_id,
                    "shard_index": shard_index,
                    "phase": "failed",
                    "task_id": "",
                    "error": "Manager restarted during execution",
                    "final_collected": bool(collected),
                    "collection_error": (
                        str(collection_error)[:2_000]
                        if collection_error
                        else None
                    ),
                    "cleanup_error": None,
                    "worker_released": worker_released,
                }
            elif (
                existing["worker_id"]
                and existing["worker_id"] != recovered_worker_id
            ):
                # Two distinct resources claiming one shard make its filesystem
                # provenance ambiguous. Cleanup may proceed, but legacy replay
                # must fail closed rather than selecting one arbitrarily.
                existing["final_collected"] = False
                existing["collection_error"] = (
                    "multiple recovered workers claimed the same shard"
                )
                existing["worker_released"] = (
                    existing["worker_released"] and worker_released
                )
            else:
                existing["worker_id"] = recovered_worker_id
                if collected is True:
                    existing["final_collected"] = True
                    existing["collection_error"] = None
                elif (
                    collected is False
                    and not existing["final_collected"]
                ):
                    existing["collection_error"] = (
                        str(collection_error)[:2_000]
                        if collection_error
                        else "final recovery collection failed"
                    )
                existing["worker_released"] = (
                    existing["worker_released"] or worker_released
                )
            workers_by_shard[shard_index] = existing

            terminal_workers = [
                workers_by_shard[index]
                for index in sorted(workers_by_shard)
            ]
            released_shards = sum(
                worker["worker_released"] for worker in terminal_workers
            )
            cleanup_pending = (
                expected_workers - len(terminal_workers)
                + len(terminal_workers) - released_shards
            )
            complete = (
                len(terminal_workers) == expected_workers
                and cleanup_pending == 0
            )
            failures = [
                worker["collection_error"]
                for worker in terminal_workers
                if worker["collection_error"]
            ]
            raw_interrupt_intent = payload.get("interrupt_intent")
            interrupt_intent_valid = bool(
                isinstance(raw_interrupt_intent, dict)
                and raw_interrupt_intent.get("schema") == 1
                and isinstance(
                    raw_interrupt_intent.get("idempotency_digest"),
                    str,
                )
                and re.fullmatch(
                    r"[0-9a-f]{64}",
                    raw_interrupt_intent["idempotency_digest"],
                )
                is not None
            )
            # A generic/corrupt terminal summary is not authority to create a
            # resumable outcome. Only the dedicated atomic private envelope
            # can identify a cold-interrupt transaction on startup.
            suspending = (
                payload.get("submission_state") == "suspending"
                and interrupt_intent_valid
                and summary.get("state") == "suspending"
                and summary.get("interrupt_requested") is True
                and summary.get("resume_available") is False
            )
            checkpoint_generation = str(
                payload.get("latest_checkpoint_generation") or ""
            )
            checkpoint_committed_at = str(
                payload.get("checkpoint_committed_at") or ""
            )
            checkpoint_valid = False
            if checkpoint_generation and checkpoint_committed_at:
                try:
                    checkpoint_time = datetime.fromisoformat(
                        checkpoint_committed_at.replace("Z", "+00:00")
                    )
                except ValueError:
                    checkpoint_time = None
                checkpoint_valid = bool(
                    checkpoint_time is not None
                    and checkpoint_time.tzinfo is not None
                )
            if suspending and complete and checkpoint_valid:
                recovered_state = "suspended"
            elif suspending and not complete:
                recovered_state = "suspending"
            else:
                recovered_state = "failed"

            suspend_warning: str | None = None
            if recovered_state == "suspended":
                if (
                    checkpoint_generation
                    == summary.get(
                        "interrupt_checkpoint_generation_before"
                    )
                    and checkpoint_committed_at
                    == summary.get(
                        "interrupt_checkpoint_committed_at_before"
                    )
                ):
                    suspend_warning = (
                        "the restart-time final checkpoint did not commit; "
                        "resume will use the previous complete generation "
                        f"{checkpoint_generation}"
                    )
                elif failures:
                    suspend_warning = (
                        "some restart-time final collections failed; the "
                        "committed checkpoint set remains resumable"
                    )
            elif suspending and complete:
                suspend_warning = (
                    "no complete checkpoint set is available; this interrupted "
                    "Job cannot be resumed"
                )

            raw_lineage = payload.get("lineage")
            lineage = raw_lineage if isinstance(raw_lineage, dict) else {}
            if recovered_state == "suspended":
                for worker in terminal_workers:
                    worker["phase"] = "suspended"
            rebuilt = {
                "job_id": job_id,
                "name": recovery_spec.name,
                "state": recovered_state,
                "done": complete,
                "workers": expected_workers,
                "phases": {
                    (
                        "suspended"
                        if recovered_state == "suspended"
                        else "failed"
                    ): len(terminal_workers)
                },
                "cleanup_pending": cleanup_pending,
                "error": (
                    None
                    if recovered_state == "suspended"
                    else (
                        suspend_warning
                        if suspending and complete
                        else (
                            "Manager restarted during execution; "
                            + (
                                "final recovery collection failed: "
                                + "; ".join(failures[:3])
                                if failures
                                else (
                                    "recovered workers were collected and "
                                    "terminated"
                                )
                            )
                        )
                    )
                ),
                "terminal_workers": terminal_workers,
                "startup_recovered": True,
                "interrupt_requested": suspending,
                "interrupt_reason": summary.get("interrupt_reason"),
                "interrupt_requested_at": summary.get(
                    "interrupt_requested_at"
                ),
                "interrupt_available": False,
                "resume_available": (
                    recovered_state == "suspended"
                    and complete
                    and cleanup_pending == 0
                    and checkpoint_valid
                ),
                "resume_generation": (
                    checkpoint_generation
                    if recovered_state == "suspended"
                    else None
                ),
                "resume_committed_at": (
                    checkpoint_committed_at
                    if recovered_state == "suspended"
                    else None
                ),
                "latest_checkpoint_generation": (
                    checkpoint_generation or None
                ),
                "suspend_warning": suspend_warning,
                "resumed_from_job_id": lineage.get(
                    "resumed_from_job_id"
                ),
                "root_job_id": lineage.get("root_job_id") or job_id,
                "attempt_no": lineage.get("attempt_no", 1),
            }
            for key in (
                "cancel_requested",
                "cancel_reason",
                "created_at",
                "started_at",
                "completed_at",
                "interrupt_checkpoint_generation_before",
                "interrupt_checkpoint_committed_at_before",
            ):
                if key in summary:
                    rebuilt[key] = summary[key]
            update_job_state(
                self.config.registry.path,
                job_id,
                recovered_state,
                summary=rebuilt,
            )
            return True

        async with self._job_state_lock:
            return await asyncio.to_thread(merge)

    async def _collect_recovered_unbound(self, instance) -> None:
        """Bounded best-effort collect for a prior Manager's ordinary Job."""
        import json

        from elastic_agent.core.manager_fleet_driver import ManagerFleetDriver

        job_id = str(instance.tags.get("ElasticAgentJob") or "")
        if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", job_id) is None:
            logger.error(
                "Refusing unsafe recovered Job id %r on instance %s",
                job_id,
                instance.instance_id,
            )
            return
        spec_path = self.account_binding_store.path.with_name("specs") / (
            f"{job_id}.json"
        )
        if not spec_path.is_file():
            return
        payload = json.loads(spec_path.read_text(encoding="utf-8"))
        spec = _load_recovery_collection_spec(payload["spec"])
        worker_id = instance.instance_id
        node = await self.registry.get(worker_id)
        if node is None:
            raw_shard_index = str(
                instance.tags.get("ElasticAgentShardIndex") or ""
            )
            shard_index = (
                int(raw_shard_index)
                if re.fullmatch(r"[0-9]{1,5}", raw_shard_index)
                else None
            )
            await self.registry.add(NodeRecord(
                node_id=worker_id,
                instance_id=instance.instance_id,
                platform=instance.platform,
                status=NodeStatus.DRAINING,
                public_ip=instance.public_ip,
                private_ip=instance.private_ip,
                metadata={
                    "job_id": job_id,
                    "controller_id": instance.tags.get(
                        "ElasticAgentController", ""
                    ),
                    **(
                        {"shard_index": shard_index}
                        if shard_index is not None
                        else {}
                    ),
                },
            ))
        else:
            await self.registry.update(
                worker_id,
                status=NodeStatus.DRAINING,
                public_ip=instance.public_ip,
                private_ip=instance.private_ip,
            )
        node = await self.registry.get(worker_id)
        shard_index = (
            node.metadata.get("shard_index")
            if node is not None
            else None
        )
        driver = ManagerFleetDriver(self)
        await asyncio.wait_for(
            driver.quiesce_recovered_worker(
                worker_id, job_id, spec,
            ),
            timeout=300.0,
        )
        if spec.recovery.policy != "none":
            await asyncio.wait_for(
                driver.reconcile_recovery_install(
                    worker_id,
                    job_id,
                    spec,
                    shard_index,
                ),
                timeout=1_800.0,
            )
        await asyncio.wait_for(
            driver.collect(worker_id, spec, job_id),
            timeout=BOUND_RECOVERY_COLLECT_TIMEOUT_SECONDS,
        )

    async def _collect_recovered_lease(self, lease) -> None:
        """Best-effort persisted-spec collection before restart teardown.

        Collection failure is durable and visible, but never retains a billable
        EC2 forever: one strictly bounded attempt is followed by release.
        """
        import json

        from elastic_agent.core.manager_fleet_driver import ManagerFleetDriver

        specs_dir = self.account_binding_store.path.with_name("specs")
        spec_path = specs_dir / f"{lease.job_id}.json"
        error: str | None = None
        collected = False
        try:
            if not spec_path.is_file():
                raise FileNotFoundError(
                    f"persisted JobSpec missing for recovery job {lease.job_id!r}"
                )
            payload = json.loads(spec_path.read_text(encoding="utf-8"))
            spec = _load_recovery_collection_spec(payload["spec"])

            worker_id = lease.worker_id or lease.instance_id
            binding = await self.binding_manager.get_binding(lease.account_id)
            recovered = self._recovery_instances.get(lease.instance_id)
            if recovered is None:
                try:
                    recovered = await self.provider.get_instance(lease.instance_id)
                except Exception as exc:  # noqa: BLE001
                    raise RuntimeError(
                        "cannot prove recovered cloud instance identity"
                    ) from exc
            if recovered is None:
                raise RuntimeError(
                    "cannot prove recovered cloud instance identity"
                )
            cloud_error = self._recovery_instance_validation_error(
                recovered,
                lease,
            )
            if cloud_error:
                raise RuntimeError(
                    f"recovered cloud instance identity mismatch: {cloud_error}"
                )
            if recovered.state == InstanceState.TERMINATED:
                raise RuntimeError(
                    "recovered cloud instance is already terminated"
                )

            attached_eip_ip: str | None = None
            if binding is not None and binding.eip_allocation_id:
                eip = await self.provider.describe_eip(binding.eip_allocation_id)
                # Never route recovery collection through an EIP now owned by a
                # different instance.  A stale registry address must not send
                # SSH to another account's machine.
                if eip is not None and eip.instance_id == lease.instance_id:
                    attached_eip_ip = eip.public_ip

            public_ip = attached_eip_ip or (
                recovered.public_ip if recovered is not None else None
            )
            private_ip = recovered.private_ip if recovered is not None else None
            node = await self.registry.get(worker_id)
            if node is None:
                await self.registry.add(NodeRecord(
                    node_id=worker_id,
                    instance_id=lease.instance_id,
                    platform=lease.instance_id.split(":", 1)[0],
                    status=NodeStatus.DRAINING,
                    public_ip=public_ip,
                    private_ip=private_ip,
                    metadata={
                        "job_id": lease.job_id,
                        "account_id": lease.account_id,
                        "lease_id": lease.lease_id,
                        "shard_index": lease.slot,
                    },
                ))
            else:
                registry_error = (
                    self._recovery_lease_registry_validation_error(
                        node,
                        lease,
                    )
                )
                if registry_error:
                    raise RuntimeError(
                        "recovered registry identity mismatch: "
                        f"{registry_error}"
                    )
                metadata = dict(node.metadata)
                metadata.update({
                    "job_id": lease.job_id,
                    "account_id": lease.account_id,
                    "lease_id": lease.lease_id,
                    "shard_index": lease.slot,
                })
                await self.registry.update(
                    worker_id,
                    instance_id=lease.instance_id,
                    status=NodeStatus.DRAINING,
                    public_ip=public_ip,
                    private_ip=private_ip,
                    metadata=metadata,
                )

            driver = ManagerFleetDriver(self)
            await asyncio.wait_for(
                driver.quiesce_recovered_worker(
                    worker_id, lease.job_id, spec,
                ),
                timeout=300.0,
            )
            if spec.recovery.policy != "none":
                await asyncio.wait_for(
                    driver.reconcile_recovery_install(
                        worker_id,
                        lease.job_id,
                        spec,
                        lease.slot,
                    ),
                    timeout=1_800.0,
                )
            await asyncio.wait_for(
                driver.collect(worker_id, spec, lease.job_id),
                timeout=BOUND_RECOVERY_COLLECT_TIMEOUT_SECONDS,
            )
            collected = True
        except Exception as exc:  # noqa: BLE001
            error = str(exc)
            logger.exception(
                "Cannot reconstruct final collection for lease %s", lease.lease_id
            )
        finally:
            await self.account_binding_store.update_lease(
                lease.lease_id,
                recovery_collection_attempted=True,
                recovery_collected=collected,
                recovery_collection_error=error,
            )

    def _ensure_binding_recovery_task(self) -> None:
        if self._shutdown_event.is_set():
            return
        self._binding_recovery_wakeup.set()
        if self._binding_recovery_task is None or self._binding_recovery_task.done():
            self._binding_recovery_task = asyncio.create_task(
                self._binding_recovery_loop()
            )

    async def _begin_unbound_launch_intent(self, job_id: str) -> int:
        """Durably fence one ordinary create before entering the cloud API."""

        from elastic_agent.core.job_spec_store import add_unbound_launch_intent

        async def commit() -> int:
            async with self._job_state_lock:
                expected_count = self._unbound_launch_intent_counts.get(
                    job_id, 0
                )
                count = await asyncio.to_thread(
                    add_unbound_launch_intent,
                    self.config.registry.path,
                    self.account_binding_store.controller_id,
                    job_id,
                    expected_count=expected_count,
                )
                self._unbound_launch_intent_counts[job_id] = count
                return count

        # A cancelled request cannot abandon the fsync thread halfway through
        # and leave in-memory recovery unaware of the now-durable intent.
        return await self._await_owned_task(asyncio.create_task(commit()))

    async def _resolve_unbound_launch_intent(
        self,
        job_id: str,
        *,
        all_launches: bool = False,
        instance_id: str = "",
    ) -> int:
        """Commit that one create, or a stable all-miss scan, is harmless."""

        from elastic_agent.core.job_spec_store import (
            resolve_unbound_launch_intent,
        )

        async def commit() -> int:
            async with self._job_state_lock:
                if (
                    instance_id
                    and instance_id in self._resolved_unbound_instance_ids
                ):
                    return self._unbound_launch_intent_counts.get(job_id, 0)
                expected_count = self._unbound_launch_intent_counts.get(
                    job_id, 0
                )
                remaining = await asyncio.to_thread(
                    resolve_unbound_launch_intent,
                    self.config.registry.path,
                    self.account_binding_store.controller_id,
                    job_id,
                    all_launches=all_launches,
                    expected_count=expected_count,
                )
                if remaining:
                    self._unbound_launch_intent_counts[job_id] = remaining
                else:
                    self._unbound_launch_intent_counts.pop(job_id, None)
                    self._recovery_unbound_launch_scans.pop(job_id, None)
                if instance_id:
                    self._resolved_unbound_instance_ids.add(instance_id)
                self._binding_recovery_scan_pending = (
                    self._binding_recovery_scans_remaining > 0
                    or bool(self._recovery_unbound_launch_scans)
                    or bool(self._recovery_unbound_registry_scans)
                )
                return remaining

        return await self._await_owned_task(asyncio.create_task(commit()))

    def _mark_unbound_launch_recovery_needed(self, job_id: str) -> None:
        """Activate bounded scans for an already-durable ordinary intent."""

        if not job_id or not self._unbound_launch_intent_counts.get(job_id):
            return
        self._recovery_unbound_launch_scans[job_id] = max(
            self._recovery_unbound_launch_scans.get(job_id, 0),
            BOUND_RECOVERY_STABLE_SCANS,
        )
        self._binding_recovery_scan_pending = True
        self._binding_recovery_ready = False
        self._ensure_binding_recovery_task()

    # ------------------------------------------------------------------
    # Node operations
    # ------------------------------------------------------------------

    async def _persist_batch_job_spec(
        self,
        job_id: str,
        spec,
        request_fingerprint: str | None = None,
    ) -> dict[str, Any] | None:
        """Journal a JobSpec before the orchestrator can reserve or scale."""

        from elastic_agent.core.job_spec_store import (
            load_job_spec_journal,
            persist_job_spec,
        )

        async with self._job_state_lock:
            await asyncio.to_thread(
                persist_job_spec,
                self.config.registry.path,
                job_id,
                spec,
                request_fingerprint,
            )
            payload = await asyncio.to_thread(
                load_job_spec_journal,
                self.config.registry.path,
                job_id,
            )
            lineage = payload.get("lineage")
            return lineage if isinstance(lineage, dict) else None

    async def _update_batch_job_state(
        self, job_id: str, state: str, summary: dict | None = None,
    ) -> None:
        """Publish a lifecycle marker into the already-durable Job journal."""

        from elastic_agent.core.job_spec_store import update_job_state

        async def commit() -> None:
            async with self._job_state_lock:
                await asyncio.to_thread(
                    update_job_state,
                    self.config.registry.path,
                    job_id,
                    state,
                    summary=summary,
                )

        await self._await_owned_task(asyncio.create_task(commit()))

    async def _update_batch_interrupt_intent(
        self,
        job_id: str,
        idempotency_digest: str,
        summary: dict,
    ) -> None:
        """Commit private request identity and suspending state together."""

        from elastic_agent.core.job_spec_store import (
            update_job_interrupt_intent,
        )

        async def commit() -> None:
            async with self._job_state_lock:
                await asyncio.to_thread(
                    update_job_interrupt_intent,
                    self.config.registry.path,
                    job_id,
                    idempotency_digest,
                    summary=summary,
                )

        await self._await_owned_task(asyncio.create_task(commit()))

    async def _update_batch_checkpoint_generation(
        self,
        job_id: str,
        generation: str,
        committed_at: str,
    ) -> None:
        """Durably expose an S3 COMMITTED set across Manager crashes."""

        from elastic_agent.core.job_spec_store import update_job_checkpoint

        async def commit() -> None:
            async with self._job_state_lock:
                await asyncio.to_thread(
                    update_job_checkpoint,
                    self.config.registry.path,
                    job_id,
                    generation,
                    committed_at=committed_at,
                )

        await self._await_owned_task(asyncio.create_task(commit()))

    @property
    def account_allocator(self):
        """One claim coordinator shared by batch launch and account CRUD."""

        if self._account_allocator is None:
            from elastic_agent.core.batch_hooks import AccountAllocator

            self._account_allocator = AccountAllocator(
                self.account_store,
                self.agent_api_store,
                agent_api_admission=lambda: self.binding_recovery_ready,
                durable_binding_loader=self.binding_manager.list_bindings,
            )
        return self._account_allocator

    @property
    def batch(self):
        """Lazily-built, fully-wired BatchOrchestrator bound to this Manager.

        Default wiring (``wire_batch``) provisions via the bootstrap SSH pipeline
        and logs accounts in on the worker (ACCOUNT_LOGIN), and routes
        RUN_EXHAUSTED / PROCESS_EXIT from workers back into the orchestrator.
        Override with ``configure_batch(...)`` for custom hooks.
        """
        if self._batch is None:
            from elastic_agent.core.batch_hooks import wire_batch
            self._batch = wire_batch(self)
        return self._batch

    @property
    def account_login_coordinator(self):
        """Coordinator backing password login and interactive OTP challenges."""

        if self._account_login_coordinator is None:
            # Default batch wiring owns the coordinator because its lifetime
            # must match the login hooks that wait for worker results.
            _ = self.batch
        return self._account_login_coordinator

    def configure_batch(self, *, provision_hook=None, login_hook=None,
                        scale_in_on_complete: bool = True, include_pty: bool = False) -> None:
        """Rewire the batch orchestrator.

        With no hooks, uses the default live wiring (``wire_batch``). Pass
        ``provision_hook`` / ``login_hook`` to override either step.
        """
        if provision_hook is None and login_hook is None:
            from elastic_agent.core.batch_hooks import wire_batch
            self._batch = wire_batch(
                self, include_pty=include_pty, scale_in_on_complete=scale_in_on_complete,
            )
            return
        from elastic_agent.core.batch_hooks import (
            AgentApiCoordinator,
            LoginCoordinator,
            make_bound_hooks,
            make_login_hook,
            make_provision_hook,
        )
        from elastic_agent.core.batch_orchestrator import BatchOrchestrator
        from elastic_agent.core.manager_fleet_driver import ManagerFleetDriver
        allocator = self.account_allocator
        coordinator = LoginCoordinator(
            self.connection_manager,
            self.event_bus,
            quarantine_account=allocator.quarantine,
        )
        self._account_login_coordinator = coordinator
        agent_api_coordinator = AgentApiCoordinator(
            self.connection_manager,
            self.event_bus,
            self.agent_api_store,
            agent_api_admission=lambda: self.binding_recovery_ready,
        )
        bound_reserve, bound_attach, bound_release = make_bound_hooks(self, allocator)
        driver = ManagerFleetDriver(
            self,
            provision_hook=provision_hook or make_provision_hook(
                self, include_pty=include_pty,
            ),
            login_hook=login_hook or make_login_hook(
                self,
                allocator,
                coordinator,
                agent_api_coordinator,
            ),
            bound_reserve_hook=bound_reserve,
            bound_attach_hook=bound_attach,
            bound_release_hook=bound_release,
        )
        self._batch = BatchOrchestrator(
            driver,
            scale_in_on_complete=scale_in_on_complete,
            worker_concurrency=self.config.batch_runtime.worker_concurrency,
            collect_concurrency=self.config.batch_runtime.collect_concurrency,
            collect_jitter_ratio=(
                self.config.batch_runtime.collect_jitter_ratio
            ),
            persist_spec_hook=self._persist_batch_job_spec,
            job_state_hook=self._update_batch_job_state,
            interrupt_intent_hook=self._update_batch_interrupt_intent,
        )
        self._batch._allocator = allocator

    async def acquire_instance_capacity(self, count: int) -> str:
        """Reserve whole-Job capacity before any account/EIP side effect.

        The short-lived hold covers the interval in which a bound Job creates
        its account leases.  Once all leases exist they themselves are counted
        as planned instances, so the orchestrator releases this hold before
        calling ``scale_out`` for each shard.
        """

        if count < 1:
            raise ValueError("capacity reservation count must be at least 1")
        provider_cfg = self.config.provider
        limit = (
            provider_cfg.aliyun.max_instances
            if provider_cfg.type == "aliyun"
            else provider_cfg.aws.max_instances
        )
        async with self._instance_capacity_lock:
            used = await self._owned_instance_capacity_usage()
            projected = used + self._inflight_instance_creates + count
            if projected > limit:
                raise RuntimeError(
                    f"instance limit exceeded: {projected} requested/active, "
                    f"configured maximum is {limit}"
                )
            reservation_id = f"capacity-{uuid.uuid4().hex}"
            self._instance_capacity_holds[reservation_id] = count
            self._inflight_instance_creates += count
            return reservation_id

    async def release_instance_capacity(self, reservation_id: str) -> None:
        """Idempotently release a whole-Job capacity hold."""

        async with self._instance_capacity_lock:
            count = self._instance_capacity_holds.pop(reservation_id, 0)
            self._inflight_instance_creates = max(
                0, self._inflight_instance_creates - count
            )

    async def _owned_instance_capacity_usage(self) -> int:
        """Count owned live/planned instances for the configured fleet cap."""
        owned_ids: set[str] = set()
        for node in await self.registry.list_all():
            if node.status != NodeStatus.TERMINATED:
                owned_ids.add(node.instance_id)

        # Include controller-owned instances that exist in AWS but have not yet
        # reached the registry (or were recovered after a crash). A failed scan
        # is UNKNOWN and must block new billable resources rather than undercount.
        try:
            cloud = await self.provider.list_instances(filters={
                CloudProvider.MANAGED_TAG_KEY: CloudProvider.MANAGED_TAG_VALUE,
                "ElasticAgentController": self.account_binding_store.controller_id,
            })
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(
                "cannot verify current fleet size; refusing to create instances"
            ) from exc
        for instance in cloud:
            if (
                instance.state != InstanceState.TERMINATED
                and instance.tags.get(CloudProvider.MANAGED_TAG_KEY)
                == CloudProvider.MANAGED_TAG_VALUE
                and instance.tags.get("ElasticAgentController")
                == self.account_binding_store.controller_id
            ):
                owned_ids.add(instance.instance_id)

        # A durable bound lease can represent an accepted but not-yet-visible
        # RunInstances request. Count those placeholders so eventual
        # consistency cannot be used to step over the configured maximum.
        placeholders = 0
        for lease in await self.account_binding_store.list_leases(
            active_only=True
        ):
            if lease.instance_id:
                owned_ids.add(lease.instance_id)
            else:
                placeholders += 1
        visible_inflight_overlap = len(
            owned_ids.intersection(self._inflight_visible_instance_ids)
        )
        return len(owned_ids) + placeholders - visible_inflight_overlap

    async def _reserve_instance_capacity(
        self, count: int, tags: dict[str, str] | None
    ) -> int:
        """Atomically reserve non-lease create slots; return reserved count."""
        if count < 1:
            raise ValueError("scale_out count must be at least 1")
        provider_cfg = self.config.provider
        limit = (
            provider_cfg.aliyun.max_instances
            if provider_cfg.type == "aliyun"
            else provider_cfg.aws.max_instances
        )
        requested_lease_id = str((tags or {}).get("ElasticAgentLease") or "")
        async with self._instance_capacity_lock:
            used = await self._owned_instance_capacity_usage()
            # A bound lease without an instance is already included as a
            # planned slot. The matching one-instance scale call consumes that
            # durable placeholder rather than reserving a second slot.
            pre_reserved = 0
            if requested_lease_id and count == 1:
                lease = await self.binding_manager.get_lease(requested_lease_id)
                if lease is not None and not lease.instance_id:
                    pre_reserved = 1
            additional = count - pre_reserved
            projected = used + self._inflight_instance_creates + additional
            if projected > limit:
                raise RuntimeError(
                    f"instance limit exceeded: {projected} requested/active, "
                    f"configured maximum is {limit}"
                )
            self._inflight_instance_creates += additional
            return additional

    async def scale_out(
        self,
        count: int = 1,
        instance_type: str | None = None,
        region: str | None = None,
        name_prefix: str | None = None,
        disk_gb: int | None = None,
        spot: bool = False,
        tags: dict[str, str] | None = None,
    ) -> list[NodeRecord]:
        reserved_capacity = await self._reserve_instance_capacity(count, tags)
        capacity_visible_ids: set[str] = set()
        try:
            async with self._instance_lifecycle_lock:
                return await self._scale_out_unchecked(
                    count=count,
                    instance_type=instance_type,
                    region=region,
                    name_prefix=name_prefix,
                    disk_gb=disk_gb,
                    spot=spot,
                    tags=tags,
                    capacity_visible_ids=(
                        capacity_visible_ids if reserved_capacity else None
                    ),
                )
        finally:
            async with self._instance_capacity_lock:
                self._inflight_visible_instance_ids.difference_update(
                    capacity_visible_ids
                )
                self._inflight_instance_creates = max(
                    0,
                    self._inflight_instance_creates - reserved_capacity,
                )

    async def _scale_out_unchecked(
        self,
        count: int = 1,
        instance_type: str | None = None,
        region: str | None = None,
        name_prefix: str | None = None,
        disk_gb: int | None = None,
        spot: bool = False,
        tags: dict[str, str] | None = None,
        capacity_visible_ids: set[str] | None = None,
    ) -> list[NodeRecord]:
        from elastic_agent.core.auth import generate_worker_token
        from elastic_agent.core.providers.base import InstanceConfig

        provider_cfg = self.config.provider
        configured_region = (
            provider_cfg.aliyun.region_id
            if provider_cfg.type == "aliyun"
            else provider_cfg.aws.region
        )
        if region and region != configured_region:
            raise ValueError(
                f"requested region {region!r} does not match configured provider "
                f"region {configured_region!r}"
            )
        if provider_cfg.type == "aliyun":
            cfg = InstanceConfig(
                instance_type=instance_type or provider_cfg.aliyun.instance_type,
                image_id=provider_cfg.aliyun.image_id,
                key_pair_name=provider_cfg.aliyun.key_pair_name,
                security_group_ids=[provider_cfg.aliyun.security_group_id],
                subnet_id=provider_cfg.aliyun.vswitch_id,
                tags={**(tags or {}), "ManagedBy": "elastic-agent"},
            )
        else:
            cfg = InstanceConfig(
                instance_type=instance_type or provider_cfg.aws.default_instance_type,
                image_id=provider_cfg.aws.ami_id,
                key_pair_name=provider_cfg.aws.key_pair_name,
                security_group_ids=provider_cfg.aws.security_group_ids,
                subnet_id=provider_cfg.aws.subnet_id,
                tags={**(tags or {}), "ManagedBy": "elastic-agent"},
            )
        # Per-job disk/spot overrides; disk_gb falsy → keep InstanceConfig default.
        if disk_gb:
            cfg.root_disk_size_gb = disk_gb
        cfg.spot = spot
        cfg.tags["ElasticAgentController"] = (
            self.account_binding_store.controller_id
        )

        records: list[NodeRecord] = []
        for i in range(count):
            instance_cfg = cfg.model_copy(deep=True)
            if (
                instance_cfg.tags.get("ElasticAgentJob")
                and not instance_cfg.tags.get("ElasticAgentLease")
            ):
                # Survives a Manager crash between RunInstances and registry
                # publication. Startup checkpoint reconciliation must never
                # guess which immutable shard belongs on an adopted instance.
                instance_cfg.tags["ElasticAgentShardIndex"] = str(i)
            # Name each instance "<name_prefix>-<i>" so it's identifiable in the
            # cloud console; without a prefix only the ManagedBy tag is set.
            if name_prefix:
                instance_cfg.tags["Name"] = f"{name_prefix}-{i}"
            instance = None
            record = None
            lease_id = instance_cfg.tags.get("ElasticAgentLease", "")
            job_id = str(instance_cfg.tags.get("ElasticAgentJob") or "")
            unbound_intent_before = self._unbound_launch_intent_counts.get(
                job_id, 0
            )
            unbound_intent_started = False
            try:
                if lease_id:
                    import hashlib

                    instance_cfg.client_token = (
                        "ea-" + hashlib.sha256(lease_id.encode()).hexdigest()[:61]
                    )
                    # Persist intent before RunInstances.  The SDK can time out
                    # after AWS accepted the request, in which case no Instance
                    # is returned to this coroutine even though a billable EC2
                    # will become visible later.
                    await self.account_binding_store.update_lease(
                        lease_id,
                        launch_uncertain=True,
                        last_operation="create_instance",
                        error=None,
                    )
                elif job_id:
                    # A random idempotency token prevents transport-level SDK
                    # retries from creating duplicates.  The separate durable
                    # counter is the recovery authority across Manager restarts.
                    instance_cfg.client_token = f"ea-u-{uuid.uuid4().hex}"
                    await self._begin_unbound_launch_intent(job_id)
                    unbound_intent_started = True
                if capacity_visible_ids is None:
                    instance = await self.provider.create_instance(instance_cfg)
                else:
                    # Keep the capacity scan fenced across the exact boundary
                    # where AWS may expose the new instance but this process
                    # still owns its inflight reservation.  Once the id is
                    # recorded, scans can proceed and deduct that overlap.
                    async with self._instance_capacity_lock:
                        instance = await self.provider.create_instance(instance_cfg)
                        capacity_visible_ids.add(instance.instance_id)
                        self._inflight_visible_instance_ids.add(
                            instance.instance_id
                        )
                if (
                    not lease_id
                    and instance_cfg.tags.get("ElasticAgentJob")
                ):
                    # Record exact current-process ownership immediately after
                    # the cloud returns.  The lifecycle lock prevents recovery
                    # from inspecting it until registry/event publication has
                    # either completed or compensation has started.
                    self._current_unbound_instance_ids.add(
                        instance.instance_id
                    )
                node_id = f"{instance.platform}:{instance.native_id}"
                if lease_id:
                    # Persist the new instance against its pre-reserved lease
                    # immediately after RunInstances returns, before registry or
                    # event work.  If any following step fails, release can now
                    # find and terminate the exact EC2.
                    await self.account_binding_store.begin_attach(
                        lease_id, instance.instance_id, node_id
                    )
                token = generate_worker_token()
                controller_id = str(
                    instance_cfg.tags.get("ElasticAgentController") or ""
                )
                metadata: dict[str, Any] = (
                    {
                        "job_id": job_id,
                        "controller_id": controller_id,
                        "shard_index": i,
                    }
                    if job_id and not lease_id
                    else {}
                )
                if lease_id:
                    metadata = {
                        "job_id": job_id,
                        "account_id": instance_cfg.tags.get(
                            "ElasticAgentAccount", ""
                        ),
                        "lease_id": lease_id,
                        "controller_id": controller_id,
                    }
                record = NodeRecord(
                    node_id=node_id,
                    instance_id=instance.instance_id,
                    platform=instance.platform,
                    status=NodeStatus.CREATING,
                    public_ip=instance.public_ip,
                    private_ip=instance.private_ip,
                    auth_token=token,
                    metadata=metadata,
                )
                await self.registry.add(record)
                await self.event_bus.emit(
                    "NODE_CREATING",
                    record.node_id,
                    {"instance_id": instance.instance_id},
                )
                if unbound_intent_started:
                    await self._resolve_unbound_launch_intent(
                        job_id,
                        instance_id=instance.instance_id,
                    )
                    unbound_intent_started = False
                records.append(record)
            except BaseException as exc:  # noqa: BLE001
                # The cancellation-safe durable helper may finish its fsync
                # and then re-raise the caller's cancellation.  Infer that
                # completion from the protected count before compensating.
                unbound_intent_started = (
                    unbound_intent_started
                    or (
                        bool(job_id)
                        and self._unbound_launch_intent_counts.get(job_id, 0)
                        > unbound_intent_before
                    )
                )
                compensation_succeeded = instance is None
                if instance is not None:
                    try:
                        await self._terminate_instance_confirmed(
                            instance.instance_id
                        )
                        compensation_succeeded = True
                        if record is not None:
                            await self.registry.update(
                                record.node_id, status=NodeStatus.TERMINATED
                            )
                    except BaseException as compensation_exc:  # noqa: BLE001
                        # Bound instances remain discoverable by their immutable
                        # cloud tags and will be retried by startup recovery.
                        if isinstance(compensation_exc, Exception):
                            logger.exception(
                                "Compensating termination failed for %s",
                                instance.instance_id,
                            )
                if (
                    unbound_intent_started
                    and instance is not None
                    and compensation_succeeded
                ):
                    try:
                        await self._resolve_unbound_launch_intent(
                            job_id,
                            instance_id=instance.instance_id,
                        )
                    except BaseException as resolution_exc:  # noqa: BLE001
                        if isinstance(resolution_exc, Exception):
                            logger.exception(
                                "Could not resolve compensated unbound launch "
                                "intent for %s",
                                job_id,
                            )
                    unbound_intent_started = (
                        self._unbound_launch_intent_counts.get(job_id, 0)
                        > unbound_intent_before
                    )
                    if unbound_intent_started:
                        self._mark_unbound_launch_recovery_needed(job_id)
                    else:
                        self._resolved_unbound_instance_ids.discard(
                            instance.instance_id
                        )
                if lease_id and instance is not None and not compensation_succeeded:
                    # Keep the current Manager fail-closed too; startup recovery
                    # is not the only time a post-RunInstances compensation can
                    # fail.  Cloud tags retain the exact lease/account identity.
                    self._recovery_lease_ids.add(lease_id)
                    self._recovery_instances[instance.instance_id] = instance
                    self._binding_recovery_scan_pending = False
                    self._binding_recovery_ready = False
                    self._ensure_binding_recovery_task()
                elif (
                    not lease_id
                    and instance is not None
                    and not compensation_succeeded
                ):
                    # The current ordinary instance is not part of ``records``
                    # yet when registry/event publication fails.  If its direct
                    # compensation also fails, track the exact returned instance
                    # and wake live recovery now; relying on a future Manager
                    # restart would leave a controller-tagged EC2 billable with
                    # no BatchJob/registry owner.
                    self._recovery_unbound_instances[
                        instance.instance_id
                    ] = instance
                    if unbound_intent_started:
                        self._mark_unbound_launch_recovery_needed(job_id)
                    else:
                        self._binding_recovery_ready = False
                        self._ensure_binding_recovery_task()
                elif lease_id and instance is not None and compensation_succeeded:
                    # The exact returned instance was successfully destroyed;
                    # release no longer needs an eventual-consistency scan.
                    await self.account_binding_store.update_lease(
                        lease_id, launch_uncertain=False
                    )
                elif lease_id and instance is None:
                    # The external create call is ambiguous.  Keep the durable
                    # lease unreleasable and scan its immutable tags until the
                    # instance appears or the bounded quarantine expires.
                    self._recovery_lease_ids.add(lease_id)
                    self._binding_recovery_scans_remaining = max(
                        self._binding_recovery_scans_remaining,
                        BOUND_RECOVERY_STABLE_SCANS,
                    )
                    self._binding_recovery_scan_pending = True
                    self._binding_recovery_ready = False
                    self._ensure_binding_recovery_task()
                elif (
                    not lease_id
                    and instance is None
                    and unbound_intent_started
                ):
                    # The provider may have accepted RunInstances before a
                    # timeout/cancellation prevented it from returning the
                    # instance id.  The controller/job tags are the only safe
                    # recovery handle for this ordinary launch.
                    self._mark_unbound_launch_recovery_needed(job_id)

                # ``scale_out(count=N)`` is one ownership transaction.  If a
                # later create fails, every earlier success from this call must
                # be compensated before the error escapes; otherwise the
                # orchestrator never receives their ids and cannot tear down
                # those billable instances.
                for created in reversed(records):
                    created_job_id = str(
                        created.metadata.get("job_id") or ""
                    )
                    try:
                        await self._terminate_instance_confirmed(
                            created.instance_id
                        )
                        if (
                            created_job_id
                            in self._unbound_launch_intent_counts
                        ):
                            await self._resolve_unbound_launch_intent(
                                created_job_id,
                                instance_id=created.instance_id,
                            )
                        self._resolved_unbound_instance_ids.discard(
                            created.instance_id
                        )
                        await self.registry.update(
                            created.node_id, status=NodeStatus.TERMINATED
                        )
                        await self.connection_manager.disconnect_worker(
                            created.node_id
                        )
                    except BaseException:  # noqa: BLE001
                        logger.exception(
                            "Failed to compensate earlier worker %s after "
                            "partial scale-out",
                            created.instance_id,
                        )
                        # The instance is controller/job tagged and therefore
                        # discoverable without trusting this in-memory record.
                        # Trigger the same retry scanner used at startup now;
                        # waiting for a future process restart would leave a
                        # billable ordinary Job worker running indefinitely.
                        self._mark_unbound_launch_recovery_needed(
                            created_job_id
                        )
                        self._binding_recovery_scan_pending = True
                        self._binding_recovery_ready = False
                        self._ensure_binding_recovery_task()
                if isinstance(exc, Exception):
                    logger.exception("Failed to create worker instance")
                raise

        return records

    async def scale_in(self, node_ids: list[str], force: bool = False) -> list[str]:
        terminated: list[str] = []
        failures: list[tuple[str, BaseException]] = []
        for nid in node_ids:
            try:
                node = await self.registry.get(nid)
                if node is None:
                    continue
                if not force:
                    await self.registry.update(nid, status=NodeStatus.DRAINING)
                else:
                    lease_id = await self._lease_id_for_node(node)
                    if lease_id:
                        await self._cleanup_bound_lease(
                            node.node_id,
                            lease_id,
                            reason="worker force-scaled in by administrator",
                        )
                    else:
                        await self._terminate_instance_confirmed(
                            node.instance_id
                        )
                        self._resolved_unbound_instance_ids.discard(
                            node.instance_id
                        )
                        await self.registry.update(nid, status=NodeStatus.TERMINATED)
                        await self.connection_manager.disconnect_worker(nid)
                    terminated.append(nid)
            except BaseException as exc:  # noqa: BLE001
                failures.append((nid, exc))
                logger.exception("Failed to scale in worker %s", nid)
        if failures:
            detail = "; ".join(
                f"{node_id}: {error or type(error).__name__}"
                for node_id, error in failures[:3]
            )
            raise RuntimeError(
                f"failed to scale in {len(failures)} worker(s): {detail}"
            ) from failures[0][1]
        return terminated

    async def _cleanup_bound_node(self, node: NodeRecord, *, reason: str) -> None:
        """Finalize a bound worker through its lease, never raw-terminate it."""
        lease_id = str(node.metadata.get("lease_id") or "")
        if not lease_id:
            raise ValueError(f"node {node.node_id} has no EIP lease metadata")
        await self._cleanup_bound_lease(node.node_id, lease_id, reason=reason)

    async def _on_reconciler_bound_lost(
        self, worker_id: str, lease_id: str
    ) -> None:
        """Resolve a tagged orphan/lost worker without crossing controllers."""
        from elastic_agent.core.account_binding import LeaseState

        node = await self.registry.get(worker_id)
        if node is None:
            return
        if node.metadata.get("controller_id") not in (
            None,
            "",
            self.account_binding_store.controller_id,
        ):
            raise RuntimeError(
                f"refusing cleanup for foreign controller node {worker_id}"
            )

        lease = await self.binding_manager.get_lease(lease_id)
        claimed = await self.account_binding_store.get_lease_by_instance(
            node.instance_id
        )
        if claimed is not None and claimed.lease_id != lease_id:
            raise RuntimeError(
                f"refusing cleanup for {worker_id}: cloud lease {lease_id!r} "
                f"conflicts with active durable lease {claimed.lease_id!r}"
            )
        if (
            node.status == NodeStatus.TERMINATED
            and self._lease_proves_released_instance(
                lease,
                instance_id=node.instance_id,
                account_id=str(node.metadata.get("account_id") or ""),
                job_id=str(node.metadata.get("job_id") or ""),
            )
        ):
            # BindingManager persisted every teardown phase before RELEASED.
            # This row is only EC2's short-lived terminated history, not work
            # that should invoke detach/terminate a second time.
            await self.connection_manager.disconnect_worker(worker_id)
            await self.remove_terminated_node_record(worker_id)
            return
        if lease is not None and lease.state != LeaseState.RELEASED:
            if lease.instance_id and lease.instance_id != node.instance_id:
                raise RuntimeError(
                    f"refusing cleanup for {worker_id}: durable lease "
                    "references another instance"
                )
            tagged_controller = str(
                node.metadata.get("controller_id") or ""
            )
            tagged_account = str(node.metadata.get("account_id") or "")
            if lease.job_id == "legacy-binding-migration":
                if (
                    tagged_controller
                    and tagged_controller
                    != self.account_binding_store.controller_id
                ):
                    raise RuntimeError(
                        f"legacy node {worker_id} has a foreign controller"
                    )
                if tagged_account and tagged_account != lease.account_id:
                    raise RuntimeError(
                        f"legacy node {worker_id} has a conflicting account"
                    )
            else:
                if tagged_controller != self.account_binding_store.controller_id:
                    raise RuntimeError(
                        f"cannot prove controller ownership for node {worker_id}"
                    )
                if tagged_account != lease.account_id:
                    raise RuntimeError(
                        f"cannot prove account ownership for node {worker_id}"
                    )
            if (
                lease.launch_uncertain
                and not lease.instance_id
                and lease_id not in self._recovery_lease_ids
            ):
                # RunInstances is still in flight in this Manager.  Only its
                # exception path adds the lease to live recovery; a periodic
                # reconcile that sees AWS before the SDK returns must not kill
                # the just-created shard.
                return
            if (
                node.status != NodeStatus.TERMINATED
                and not lease.launch_uncertain
                and lease.instance_id == node.instance_id
                and lease.state in {
                    LeaseState.RESERVED,
                    LeaseState.ATTACHING,
                    LeaseState.ATTACHED,
                }
                and lease.last_operation != "release"
            ):
                # Reconciler raced the tiny RunInstances→registry.add window.
                # Its adopted row is useful and scale_out will shortly replace
                # it with the authenticated record; this is not a lost worker.
                return
            if lease.launch_uncertain and not lease.instance_id:
                await self.account_binding_store.begin_attach(
                    lease_id, node.instance_id, worker_id
                )
            await self._cleanup_bound_lease(
                worker_id,
                lease_id,
                reason="cloud instance lost during reconciliation",
            )
        else:
            # The create API may have timed out and the unattached lease may
            # have completed before an older deployment learned to quarantine
            # ambiguous launches.  Tags still prove this exact controller owns
            # the orphan.  Detach only this instance's observed association,
            # then terminate the exact tagged id.
            if (
                node.metadata.get("controller_id")
                != self.account_binding_store.controller_id
            ):
                raise RuntimeError(
                    f"cannot prove controller ownership for orphan {worker_id}"
                )
            account_id = str(node.metadata.get("account_id") or "")
            if not account_id:
                raise RuntimeError(
                    f"cannot prove account ownership for orphan {worker_id}"
                )
            binding = (
                await self.binding_manager.get_binding(account_id)
                if account_id
                else None
            )
            await self._detach_then_terminate_orphan(node.instance_id, binding)
            await self.connection_manager.disconnect_worker(worker_id)
        # Reconciler deliberately retains leased records until this callback
        # succeeds.  Removing only now prevents a failed detach/termination
        # from being silently forgotten on the next scan.
        await self.remove_terminated_node_record(worker_id)

    @staticmethod
    def _lease_proves_released_instance(
        lease: Any,
        *,
        instance_id: str,
        account_id: str,
        job_id: str,
    ) -> bool:
        """Return true only for an exact, fully committed teardown record."""
        from elastic_agent.core.account_binding import LeaseState

        return bool(
            lease is not None
            and lease.state == LeaseState.RELEASED
            and lease.lease_id
            and lease.instance_id == instance_id
            and account_id
            and lease.account_id == account_id
            and job_id
            and lease.job_id == job_id
            and lease.eip_detached
            and lease.instance_terminated
            and (
                not lease.worker_cleanup_required
                or lease.worker_cleanup_done
            )
            and lease.released_at is not None
        )

    async def _is_reconciler_bound_released(
        self,
        lease_id: str,
        instance_id: str,
        account_id: str,
        job_id: str,
    ) -> bool:
        """Positively prove a terminated cloud row is settled history."""
        claimed = await self.account_binding_store.get_lease_by_instance(
            instance_id
        )
        if claimed is not None:
            # A released historical tag must never suppress the callback for
            # an instance that is now claimed by any active durable lease. The
            # callback will quarantine mismatched ownership without mutation.
            return False
        lease = await self.binding_manager.get_lease(lease_id)
        return self._lease_proves_released_instance(
            lease,
            instance_id=instance_id,
            account_id=account_id,
            job_id=job_id,
        )

    async def _lease_id_for_node(self, node: NodeRecord) -> str:
        """Resolve durable ownership even for old/reconciler registry rows."""
        lease_id = str(node.metadata.get("lease_id") or "")
        if lease_id:
            return lease_id
        lease = await self.account_binding_store.get_lease_by_instance(
            node.instance_id
        )
        if lease is None and node.node_id != node.instance_id:
            lease = await self.account_binding_store.get_lease_by_instance(
                node.node_id
            )
        return lease.lease_id if lease else ""

    async def _cleanup_bound_lease(
        self, worker_id: str, lease_id: str, *, reason: str
    ) -> None:
        """Finalize a lease even if reconciliation already removed its node."""

        orchestrator = self._batch
        if (
            orchestrator is not None
            and orchestrator.job_id_for_worker(worker_id) is not None
        ):
            cleaned = await orchestrator.cancel_worker(worker_id, reason)
            if not cleaned:
                raise RuntimeError(
                    f"bound worker {worker_id} cleanup is pending; "
                    "registry record retained"
                )
            return

        node = await self.registry.get(worker_id)
        expected_instance_id = node.instance_id if node is not None else ""

        async def cleanup_control_plane(lease) -> None:
            target = self._durable_lease_worker_target(
                lease,
                expected_lease_id=lease_id,
                expected_worker_id=worker_id,
                expected_instance_id=expected_instance_id,
            )
            await self.registry.update(target, status=NodeStatus.TERMINATED)
            await self.connection_manager.disconnect_worker(target)

        current = await self.binding_manager.get_lease(lease_id)
        if current is None:
            raise RuntimeError(
                f"bound worker {worker_id} durable lease {lease_id!r} "
                "disappeared before cleanup"
            )
        self._durable_lease_worker_target(
            current,
            expected_lease_id=lease_id,
            expected_worker_id=worker_id,
            expected_instance_id=expected_instance_id,
        )
        released = await self.binding_manager.release(
            lease_id,
            cleanup_worker=cleanup_control_plane,
            expected_lease=current,
        )
        if released is None or released.state != "released":
            raise RuntimeError(
                f"bound worker {worker_id} lease cleanup did not complete"
            )
        target = self._durable_lease_worker_target(
            released,
            expected_lease_id=lease_id,
            expected_worker_id=worker_id,
            expected_instance_id=expected_instance_id,
        )
        await self.remove_terminated_node_record(target)
        allocator = getattr(orchestrator, "_allocator", None) if orchestrator else None
        if allocator is not None:
            await allocator.release_owner_account(
                f"{released.job_id}:{released.slot}", released.account_id
            )

    @staticmethod
    def _durable_lease_worker_target(
        lease: Any,
        *,
        expected_lease_id: str,
        expected_worker_id: str = "",
        expected_instance_id: str = "",
    ) -> str:
        """Resolve one lease's Node id without crossing durable identities."""
        durable_lease_id = str(getattr(lease, "lease_id", "") or "")
        if durable_lease_id != expected_lease_id:
            raise RuntimeError(
                f"expected lease {expected_lease_id!r}, got durable lease "
                f"{durable_lease_id!r}"
            )
        durable_worker = str(getattr(lease, "worker_id", "") or "")
        durable_instance = str(getattr(lease, "instance_id", "") or "")
        if (expected_worker_id or durable_worker) and not durable_instance:
            raise RuntimeError(
                f"durable lease {expected_lease_id!r} identifies a worker "
                "but has no instance id"
            )
        if expected_worker_id:
            if not durable_worker and not durable_instance:
                raise RuntimeError(
                    f"durable lease {expected_lease_id!r} does not identify "
                    f"worker {expected_worker_id!r}"
                )
            if (
                not durable_worker
                and durable_instance
                and not expected_instance_id
            ):
                raise RuntimeError(
                    f"cannot prove worker {expected_worker_id!r} maps to "
                    f"durable instance {durable_instance!r} for lease "
                    f"{expected_lease_id!r}"
                )
            if durable_worker and durable_worker != expected_worker_id:
                raise RuntimeError(
                    f"worker {expected_worker_id!r} conflicts with durable "
                    f"worker {durable_worker!r} for lease {expected_lease_id!r}"
                )
            if (
                durable_instance
                and expected_instance_id
                and durable_instance != expected_instance_id
            ):
                raise RuntimeError(
                    f"worker {expected_worker_id!r} instance "
                    f"{expected_instance_id!r} conflicts with durable instance "
                    f"{durable_instance!r} for lease {expected_lease_id!r}"
                )
            return expected_worker_id
        return durable_worker or durable_instance

    async def resume_node(self, node_id: str) -> NodeRecord | None:
        """Start a stopped ECS instance and mark it as CREATING.

        The Worker's systemd service (Restart=always) will auto-reconnect
        once the instance is running, so no bootstrap is needed.
        """
        node = await self.registry.get(node_id)
        if node is None:
            logger.warning("resume_node: node %s not found", node_id)
            return None
        if node.status != NodeStatus.STOPPED:
            logger.warning("resume_node: node %s is %s, not stopped", node_id, node.status)
            return None
        if not await self._wait_until_instance_startable(node.instance_id):
            return None
        try:
            await self.provider.start_instance(node.instance_id)
        except Exception:
            logger.exception("resume_node: failed to start instance %s", node.instance_id)
            return None
        await self.registry.update(node_id, status=NodeStatus.CREATING)
        await self.event_bus.emit("NODE_RESUMING", node_id, {"instance_id": node.instance_id})
        logger.info("resume_node: started stopped instance %s (node %s)", node.instance_id, node_id)
        return await self.registry.get(node_id)

    async def _wait_until_instance_startable(self, instance_id: str) -> bool:
        """Wait for a cloud instance to leave STOPPING before StartInstance.

        Cloud APIs reject StartInstance while an instance is still shutting down.
        The registry may already say STOPPED because stop_instance returned, so
        verify the provider state here and wait briefly instead of immediately
        falling through to scale-out.
        """
        deadline = asyncio.get_running_loop().time() + RESUME_STOPPING_TIMEOUT_SECONDS

        while True:
            try:
                instance = await self.provider.get_instance(instance_id)
            except Exception:
                logger.warning(
                    "resume_node: failed to read instance state for %s before start; trying start anyway",
                    instance_id,
                    exc_info=True,
                )
                return True

            if instance is None or instance.state != InstanceState.STOPPING:
                return True

            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                logger.error(
                    "resume_node: instance %s stayed in stopping for %ss; will not start now",
                    instance_id,
                    RESUME_STOPPING_TIMEOUT_SECONDS,
                )
                return False

            sleep_seconds = min(RESUME_STOPPING_POLL_SECONDS, remaining)
            logger.info(
                "resume_node: instance %s is still stopping; waiting %.0fs before start",
                instance_id,
                sleep_seconds,
            )
            await asyncio.sleep(sleep_seconds)

    async def drain_node(self, node_id: str) -> bool:
        node = await self.registry.get(node_id)
        if node is None:
            return False
        await self.registry.update(node_id, status=NodeStatus.DRAINING)
        return True

    async def remove_node(self, node_id: str) -> bool:
        """Remove a node from registry. Terminates instance if still running."""
        node = await self.registry.get(node_id)
        if node is None:
            return False
        lease_id = await self._lease_id_for_node(node)
        if lease_id:
            await self._cleanup_bound_lease(
                node.node_id,
                lease_id,
                reason="worker terminated by administrator",
            )
        elif node.status not in (NodeStatus.TERMINATED, NodeStatus.FAILED):
            # Failure is intentionally propagated: deleting the registry record
            # after a failed cloud termination would report success while losing
            # the only handle to a still-billable instance.
            try:
                await self._terminate_instance_confirmed(node.instance_id)
                self._resolved_unbound_instance_ids.discard(
                    node.instance_id
                )
            except Exception:
                logger.exception(
                    "Failed to terminate instance %s during removal",
                    node.instance_id,
                )
                raise
            await self.connection_manager.disconnect_worker(node_id)
        await self.remove_terminated_node_record(node_id)
        return True

    async def remove_terminated_node_record(self, node_id: str) -> bool:
        """Forget a cloud-terminal worker without re-entering lease teardown."""
        node = await self.registry.get(node_id)
        await self.task_registry.cleanup_worker(node_id)
        if node is None:
            return False
        self._current_unbound_instance_ids.discard(node.instance_id)
        if node.status not in (NodeStatus.TERMINATED, NodeStatus.FAILED):
            await self.registry.update(node_id, status=NodeStatus.TERMINATED)
        removed = await self.registry.remove(node_id)
        if removed:
            await self.event_bus.emit(
                "NODE_REMOVED", node_id, {"instance_id": node.instance_id}
            )
            logger.info("Node %s removed from registry", node_id)
        return removed

    async def get_node_status(self, node_id: str) -> dict[str, Any] | None:
        node = await self.registry.get(node_id)
        if node is None:
            return None
        connected = self.connection_manager.is_connected(node_id)
        return {
            "node_id": node.node_id,
            "status": node.status.value,
            "platform": node.platform,
            "public_ip": node.public_ip,
            "private_ip": node.private_ip,
            "ws_connected": connected,
            "created_at": node.created_at.isoformat() if node.created_at else None,
            "last_heartbeat": node.last_heartbeat.isoformat() if node.last_heartbeat else None,
            "metadata": node.metadata,
        }

    # ------------------------------------------------------------------
    # Event callbacks
    # ------------------------------------------------------------------

    async def archive_job_task_log(
        self,
        job_id: str,
        worker_id: str,
        data: dict[str, Any],
    ) -> bool:
        """Persist one batch task's buffered output before Worker teardown.

        Log durability is important for diagnosis but must never retain a
        billable instance: a disk error is reported and lifecycle cleanup
        continues.  The bounded in-memory trace is released after every archive
        attempt so acknowledged exits cannot accumulate one deque per task.
        """

        task_id = str(data.get("task_id") or "")
        if not task_id:
            return False
        entries = self.log_event_parser.get_task_logs(task_id)
        source_truncated = (
            len(entries) >= self.config.external_api.trace_buffer_size
        )
        if not entries:
            # A reliable PROCESS_EXIT can replay after a Manager restart while
            # the in-memory LOG deque is empty.  The Worker fsyncs the same
            # NDJSON stream locally, so recover a bounded tail before the exit
            # handler destroys that temporary instance.
            entries, source_truncated = await self._recover_worker_task_log(
                worker_id,
                task_id,
            )
        try:
            await asyncio.to_thread(
                self.job_log_store.save_snapshot,
                job_id=job_id,
                task_id=task_id,
                worker_id=worker_id,
                entries=entries,
                exit_info=data,
                source_truncated=source_truncated,
                prompt_metadata=self.log_event_parser.get_task_prompt(task_id),
            )
        except Exception:  # noqa: BLE001
            logger.exception(
                "Failed to archive run output for Job %s task %s",
                job_id,
                task_id,
            )
            return False
        finally:
            # The immutable local copy above is bounded.  A state-disk failure
            # must not leave one deque per completed task in Manager memory
            # after the reliable exit is ACKed and its Worker is destroyed.
            self.log_event_parser.release_task(task_id)
        return True

    async def _recover_worker_task_log(
        self,
        worker_id: str,
        task_id: str,
    ) -> tuple[list[dict[str, Any]], bool]:
        """Best-effort bounded recovery of a Worker's local task NDJSON."""

        node = await self.registry.get(worker_id)
        if node is None:
            return [], False
        from elastic_agent.core.bootstrap import SSHExecutor, _shell_quote
        from elastic_agent.core.network import worker_management_host

        provider_config = self.config.provider
        host = worker_management_host(node, provider_type=provider_config.type)
        if not host:
            return [], False
        ssh_user = self.config.worker.ssh_user
        ssh_key = (
            provider_config.aliyun.ssh_key_path
            if provider_config.type == "aliyun"
            else provider_config.aws.ssh_key_path
        )
        home = "/root" if ssh_user == "root" else f"/home/{ssh_user}"
        path = f"{home}/ea-logs/{task_id}.ndjson"
        executor = SSHExecutor(
            host,
            user=ssh_user,
            key_path=ssh_key,
            use_sudo=False,
        )
        byte_limit = 8 * 1024 * 1024
        try:
            rc, stdout, _stderr = await executor.execute(
                f"test -f {_shell_quote(path)} && "
                f"stat -c %s -- {_shell_quote(path)} && "
                f"tail -c {byte_limit} -- {_shell_quote(path)}",
                timeout=10,
            )
        except Exception:  # noqa: BLE001
            logger.warning(
                "Could not recover local run output for %s", task_id,
                exc_info=True,
            )
            return [], False
        if rc != 0:
            return [], False

        import json

        size_line, separator, log_text = stdout.partition("\n")
        if not separator:
            return [], False
        try:
            source_truncated = int(size_line.strip()) > byte_limit
        except ValueError:
            return [], False
        recovered: list[dict[str, Any]] = []
        for raw_line in log_text.splitlines():
            try:
                entry = json.loads(raw_line)
            except (TypeError, ValueError, json.JSONDecodeError):
                # tail -c may begin halfway through the oldest retained line.
                continue
            if (
                not isinstance(entry, dict)
                or entry.get("task_id") != task_id
                or entry.get("stream") not in {"stdout", "stderr"}
                or not isinstance(entry.get("data"), str)
            ):
                continue
            entry["worker_id"] = worker_id
            recovered.append(entry)
        entry_limit = self.config.external_api.trace_buffer_size
        source_truncated = source_truncated or len(recovered) > entry_limit
        return recovered[-entry_limit:], source_truncated

    async def _on_worker_message(self, worker_id: str, msg: Message) -> None:
        data = msg.model_dump()
        await self.event_bus.emit(
            msg.type,
            worker_id,
            data,
            # The connection layer ACKs reliable terminal events only after
            # this call succeeds.  Propagate subscriber failures so the worker
            # retains and replays the fsynced event on reconnect.
            raise_on_error=bool(getattr(msg, "event_id", "")),
        )

        if msg.type == "LOG":
            self.log_event_parser.process_log_event(worker_id, data)

    async def _on_worker_connect(self, worker_id: str) -> None:
        pending = self._bound_disconnect_tasks.get(worker_id)
        cancel_event = self._bound_disconnect_cancel_events.get(worker_id)
        if cancel_event is not None:
            cancel_event.set()
        if pending is not None and not pending.done():
            # If grace sleep is active this returns immediately; if teardown
            # already entered boto3, wait for it instead of cancelling a thread
            # that can mutate EIP state after this callback returns.
            await asyncio.gather(pending, return_exceptions=True)
        await self.event_bus.emit("WORKER_CONNECTED", worker_id, {})
        self.operations_logger.log_worker_connected(worker_id)
        logger.info("Worker %s connected to Manager", worker_id)

    async def _on_worker_disconnect(self, worker_id: str) -> None:
        await self.task_registry.cleanup_worker(worker_id)
        await self.event_bus.emit("WORKER_DISCONNECTED", worker_id, {})
        self.operations_logger.log_worker_disconnected(worker_id, reason="connection_lost")
        logger.info("Worker %s disconnected from Manager", worker_id)
        node = await self.registry.get(worker_id)
        lease_id = str(node.metadata.get("lease_id") or "") if node else ""
        if not lease_id:
            # Reconciler may observe a Spot/external termination first and
            # remove the registry row before the WebSocket close callback.  The
            # durable lease, not registry status, is the source of truth.
            try:
                active = await self.account_binding_store.list_leases(active_only=True)
                lease = next(
                    (item for item in active if item.worker_id == worker_id), None
                )
                lease_id = lease.lease_id if lease else ""
            except Exception:  # noqa: BLE001
                logger.exception(
                    "Could not resolve EIP lease for disconnected worker %s",
                    worker_id,
                )
        batch_job_id = None
        orchestrator = self._batch
        if orchestrator is not None and callable(
            getattr(orchestrator, "job_id_for_worker", None)
        ):
            batch_job_id = orchestrator.job_id_for_worker(worker_id)
        if lease_id and batch_job_id is None:
            existing = self._bound_disconnect_tasks.get(worker_id)
            if existing is None or existing.done():
                self._bound_disconnect_cancel_events[worker_id] = asyncio.Event()
                self._bound_disconnect_tasks[worker_id] = asyncio.create_task(
                    self._cleanup_bound_after_disconnect(worker_id, lease_id)
                )
        elif lease_id:
            logger.warning(
                "Retaining disconnected bound Worker %s for active Job %s; "
                "the task supervisor, Job TTL, and cloud reconciler own "
                "liveness",
                worker_id,
                batch_job_id,
            )

    async def _cleanup_bound_after_disconnect(
        self, worker_id: str, lease_id: str
    ) -> None:
        """Give a transient reconnect time, then finalize a lost bound worker.

        Spot interruption, an externally terminated EC2, or a permanently dead
        runtime may never emit PROCESS_EXIT.  Without this path its Job remains
        RUNNING forever and the durable account lease is never released.
        """
        this_task = asyncio.current_task()
        try:
            if BOUND_DISCONNECT_GRACE_SECONDS > 0:
                cancel_event = self._bound_disconnect_cancel_events.get(worker_id)
                waiters = [asyncio.create_task(self._shutdown_event.wait())]
                if cancel_event is not None:
                    waiters.append(asyncio.create_task(cancel_event.wait()))
                done, pending = await asyncio.wait(
                    waiters,
                    timeout=BOUND_DISCONNECT_GRACE_SECONDS,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for waiter in pending:
                    waiter.cancel()
                if pending:
                    await asyncio.gather(*pending, return_exceptions=True)
                if done:
                    return
            if self.connection_manager.is_connected(worker_id):
                return
            current = await self.binding_manager.get_lease(lease_id)
            if current is None or current.state == "released":
                return
            try:
                await self._cleanup_bound_lease(
                    worker_id,
                    lease_id,
                    reason=(
                        "bound worker disconnected and did not reconnect within "
                        f"{BOUND_DISCONNECT_GRACE_SECONDS}s"
                    ),
                )
            except Exception:  # noqa: BLE001
                # Orchestrator/BindingManager retain durable ERROR state and
                # schedule their own teardown retry.  Never misreport success.
                logger.exception(
                    "Lost bound worker %s cleanup is pending", worker_id
                )
        except asyncio.CancelledError:
            return
        finally:
            if self._bound_disconnect_tasks.get(worker_id) is this_task:
                self._bound_disconnect_tasks.pop(worker_id, None)
                self._bound_disconnect_cancel_events.pop(worker_id, None)
