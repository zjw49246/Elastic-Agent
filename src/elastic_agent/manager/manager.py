"""ElasticAgentManager — central orchestration hub.

T-016: Manager FastAPI skeleton — assembles registry, event bus, connection manager,
cloud provider, reconciler, and harness into a running FastAPI server.
"""

from __future__ import annotations

import asyncio
import logging
import re
import uuid
from typing import Any

from elastic_agent.core.agent_type import AgentType
from elastic_agent.core.config import ElasticAgentConfig
from elastic_agent.core.event_bus import EventBus
from elastic_agent.core.log_event_parser import LogEventParser
from elastic_agent.core.operations_logger import OperationsLogger
from elastic_agent.core.protocols.messages import Message
from elastic_agent.core.providers.base import (
    CloudProvider,
    InstanceNotFoundError,
    InstanceState,
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

logger = logging.getLogger(__name__)

RESUME_STOPPING_TIMEOUT_SECONDS = 300
RESUME_STOPPING_POLL_SECONDS = 10
BOUND_RECOVERY_RETRY_SECONDS = 30
BOUND_RECOVERY_SCAN_SECONDS = 10
BOUND_RECOVERY_COLLECT_TIMEOUT_SECONDS = 30
EIP_ALLOCATION_RECOVERY_STABLE_SCANS = 30
# AWS documents an eventually-consistent EC2 control plane and recommends
# bounded exponential/polling retries for newly-created resources.  The only
# ambiguous crash window here is a RESERVED lease with no persisted instance;
# quarantine EIP work for up to five minutes before declaring it launch-free.
BOUND_RECOVERY_STABLE_SCANS = 30
BOUND_DISCONNECT_GRACE_SECONDS = 30


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
        from elastic_agent.core.binding_manager import BindingManager

        self.account_store = AccountStore(
            str(_Path(config.registry.path).with_name("accounts.json"))
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
        self._binding_recovery_task: asyncio.Task | None = None
        self._bound_disconnect_tasks: dict[str, asyncio.Task] = {}
        self._bound_disconnect_cancel_events: dict[str, asyncio.Event] = {}
        self._shutdown_event = asyncio.Event()
        self._binding_lock_fd: int | None = None
        self._instance_capacity_lock = asyncio.Lock()
        self._job_state_lock = asyncio.Lock()
        self._inflight_instance_creates = 0
        self._instance_capacity_holds: dict[str, int] = {}
        self._account_allocator: Any = None
        self._account_login_coordinator: Any = None
        self._batch: Any = None

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

        self.connection_manager.on_message = self._on_worker_message
        self.connection_manager.on_connect = self._on_worker_connect
        self.connection_manager.on_disconnect = self._on_worker_disconnect

        self._started = False

    async def start(self) -> None:
        if self._started:
            return
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
            await self.account_binding_store.load()
            self.reconciler.set_controller_id(
                self.account_binding_store.controller_id
            )
            import os as _os
            bucket = _os.environ.get("ELASTIC_AGENT_RESULTS_S3_BUCKET")
            if bucket:
                from elastic_agent.core.result_uploader import S3ResultUploader
                interval = float(_os.environ.get("ELASTIC_AGENT_RESULTS_S3_INTERVAL", "120"))
                self._s3_uploader = S3ResultUploader(
                    bucket, self.collected_root,
                    prefix=_os.environ.get("ELASTIC_AGENT_RESULTS_S3_PREFIX", "jobs"),
                    region=self.config.provider.aws.region,
                )
                self._s3_task = asyncio.create_task(
                    self._s3_uploader.run_periodic(interval)
                )
                logger.info("S3 result upload enabled → s3://%s", bucket)

            # Startup recovery may collect a previous controller's final
            # output.  Initialize the authoritative S3 sink first so relay-mode
            # recovery can await the upload instead of recording a false
            # permanent collection failure merely because startup ordering left
            # ``_s3_uploader`` unset.
            await self._initialize_binding_recovery()

            online_workers = set(self.connection_manager.connected_workers)
            await self.task_registry.recover(online_workers)

            await self.reconciler.start_periodic()

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
                self._release_binding_leader_lock()
            raise

    async def stop(self) -> None:
        quiesce_task = asyncio.create_task(self._quiesce_background_tasks())
        try:
            await self._await_owned_task(quiesce_task)
        finally:
            self._started = False
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
        active = await self.account_binding_store.list_leases(active_only=True)
        self._recovery_lease_ids = {lease.lease_id for lease in active}
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
        await self._recover_bound_resources_once()
        if not self._binding_recovery_ready:
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
                        self._shutdown_event.wait(), timeout=delay
                    )
                    return
                except asyncio.TimeoutError:
                    pass
                await self._recover_bound_resources_once()
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
        if lease is None:
            if controller != self.account_binding_store.controller_id:
                return "instance controller tag does not match this Manager"
            if not tagged_lease or not tagged_account:
                return "controller-owned orphan lacks lease/account tags"
            return None
        if lease.job_id == "legacy-binding-migration":
            if controller and controller != self.account_binding_store.controller_id:
                return "legacy instance has a foreign controller tag"
            if tagged_lease and tagged_lease != lease.lease_id:
                return "legacy instance lease tag conflicts with durable lease"
            if tagged_account and tagged_account != lease.account_id:
                return "legacy instance account tag conflicts with durable lease"
            return None
        if controller != self.account_binding_store.controller_id:
            return "instance controller tag does not match durable store"
        if tagged_lease != lease.lease_id:
            return "instance lease tag does not match durable lease"
        if tagged_account != lease.account_id:
            return "instance account tag does not match durable lease"
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
            await self.provider.terminate_instance(instance_id)
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)
            logger.exception("Failed to terminate orphan %s", instance_id)
        if errors:
            raise RuntimeError(
                f"orphan {instance_id} cleanup incomplete: "
                + "; ".join(str(error) or type(error).__name__ for error in errors)
            )

    async def _recover_bound_resources_once(self) -> None:
        """One idempotent pass over startup leases and tagged orphan EC2s."""
        if self._binding_recovery_scan_pending:
            try:
                instances = await self.provider.list_instances(filters={
                    CloudProvider.MANAGED_TAG_KEY: CloudProvider.MANAGED_TAG_VALUE,
                    "ElasticAgentController": self.account_binding_store.controller_id,
                })
                for instance in instances:
                    if (
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
                        # Ordinary Jobs now carry the same controller/job
                        # ownership tags as EIP Jobs.  Since BatchJob state is
                        # process-local, a startup instance cannot have a live
                        # owner and must be collected/terminated fail-closed.
                        self._recovery_unbound_instances[
                            instance.instance_id
                        ] = instance
                        continue
                    if lease_id and (
                        self._startup_binding_recovery
                        or lease_id in self._recovery_lease_ids
                    ):
                        lease = await self.binding_manager.get_lease(lease_id)
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
                        or lease.instance_id in self._recovery_instances
                    ):
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
                self._binding_recovery_scan_pending = (
                    self._binding_recovery_scans_remaining > 0
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
                # additional eventual-consistency quarantine is needed.
                self._binding_recovery_scans_remaining = 0
                self._binding_recovery_scan_pending = False

        async def cleanup_control_plane(lease) -> None:
            worker_id = lease.worker_id or lease.instance_id or ""
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
                released = await self.binding_manager.release(
                    lease_id, cleanup_worker=cleanup_control_plane
                )
                if released is None or released.state == "released":
                    self._recovery_lease_ids.discard(lease_id)
                    if released and released.instance_id:
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
                await self.provider.terminate_instance(instance_id)
                await self.registry.update(
                    instance_id, status=NodeStatus.TERMINATED
                )
                await self.connection_manager.disconnect_worker(instance_id)
                self._recovery_unbound_instances.pop(instance_id, None)
                job_id = str(instance.tags.get("ElasticAgentJob") or "")
                if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", job_id):
                    try:
                        await self._update_batch_job_state(
                            job_id,
                            "failed",
                            {
                                "job_id": job_id,
                                "state": "failed",
                                "done": True,
                                "workers": 1,
                                "phases": {"failed": 1},
                                "cleanup_pending": 0,
                                "error": (
                                    "Manager restarted during execution; "
                                    + (
                                        "final recovery collection failed: "
                                        f"{collection_error}"
                                        if collection_error
                                        else "the orphan worker was collected "
                                        "and terminated"
                                    )
                                ),
                            },
                        )
                    except FileNotFoundError:
                        # A legacy/manual tagged instance may have no JobSpec.
                        pass
                    except Exception:  # noqa: BLE001
                        logger.exception(
                            "Could not persist recovered terminal state for %s",
                            job_id,
                        )
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

    async def _collect_recovered_unbound(self, instance) -> None:
        """Bounded best-effort collect for a prior Manager's ordinary Job."""
        import json

        from elastic_agent.core.job_spec import JobSpec
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
        spec = JobSpec.model_validate(payload["spec"])
        worker_id = instance.instance_id
        node = await self.registry.get(worker_id)
        if node is None:
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
                },
            ))
        else:
            await self.registry.update(
                worker_id,
                status=NodeStatus.DRAINING,
                public_ip=instance.public_ip,
                private_ip=instance.private_ip,
            )
        await asyncio.wait_for(
            ManagerFleetDriver(self).collect(worker_id, spec, job_id),
            timeout=30.0,
        )

    async def _collect_recovered_lease(self, lease) -> None:
        """Best-effort persisted-spec collection before restart teardown.

        Collection failure is durable and visible, but never retains a billable
        EC2 forever: one strictly bounded attempt is followed by release.
        """
        import json

        from elastic_agent.core.job_spec import JobSpec
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
            spec = JobSpec.model_validate(payload["spec"])

            worker_id = lease.worker_id or lease.instance_id
            binding = await self.binding_manager.get_binding(lease.account_id)
            recovered = self._recovery_instances.get(lease.instance_id)
            if recovered is None:
                try:
                    recovered = await self.provider.get_instance(lease.instance_id)
                except Exception:  # noqa: BLE001
                    # The instance may already be terminating.  The registry or
                    # attached EIP below can still provide a routable address.
                    logger.debug(
                        "Cannot refresh recovered instance %s",
                        lease.instance_id,
                        exc_info=True,
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
                    },
                ))
            else:
                metadata = dict(node.metadata)
                metadata.update({
                    "job_id": lease.job_id,
                    "account_id": lease.account_id,
                    "lease_id": lease.lease_id,
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
        if self._binding_recovery_task is None or self._binding_recovery_task.done():
            self._binding_recovery_task = asyncio.create_task(
                self._binding_recovery_loop()
            )

    # ------------------------------------------------------------------
    # Node operations
    # ------------------------------------------------------------------

    async def _persist_batch_job_spec(self, job_id: str, spec) -> None:
        """Journal a JobSpec before the orchestrator can reserve or scale."""

        from elastic_agent.core.job_spec_store import persist_job_spec

        async with self._job_state_lock:
            await asyncio.to_thread(
                persist_job_spec,
                self.config.registry.path,
                job_id,
                spec,
            )

    async def _update_batch_job_state(
        self, job_id: str, state: str, summary: dict | None = None,
    ) -> None:
        """Publish a lifecycle marker into the already-durable Job journal."""

        from elastic_agent.core.job_spec_store import update_job_state

        async with self._job_state_lock:
            await asyncio.to_thread(
                update_job_state,
                self.config.registry.path,
                job_id,
                state,
                summary=summary,
            )

    @property
    def account_allocator(self):
        """One claim coordinator shared by batch launch and account CRUD."""

        if self._account_allocator is None:
            from elastic_agent.core.batch_hooks import AccountAllocator

            self._account_allocator = AccountAllocator(self.account_store)
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
        bound_reserve, bound_attach, bound_release = make_bound_hooks(self, allocator)
        driver = ManagerFleetDriver(
            self,
            provision_hook=provision_hook or make_provision_hook(
                self, include_pty=include_pty,
            ),
            login_hook=login_hook or make_login_hook(self, allocator, coordinator),
            bound_reserve_hook=bound_reserve,
            bound_attach_hook=bound_attach,
            bound_release_hook=bound_release,
        )
        self._batch = BatchOrchestrator(
            driver,
            scale_in_on_complete=scale_in_on_complete,
            persist_spec_hook=self._persist_batch_job_spec,
            job_state_hook=self._update_batch_job_state,
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
        return len(owned_ids) + placeholders

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
        try:
            return await self._scale_out_unchecked(
                count=count,
                instance_type=instance_type,
                region=region,
                name_prefix=name_prefix,
                disk_gb=disk_gb,
                spot=spot,
                tags=tags,
            )
        finally:
            async with self._instance_capacity_lock:
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
            # Name each instance "<name_prefix>-<i>" so it's identifiable in the
            # cloud console; without a prefix only the ManagedBy tag is set.
            if name_prefix:
                instance_cfg.tags["Name"] = f"{name_prefix}-{i}"
            instance = None
            record = None
            lease_id = instance_cfg.tags.get("ElasticAgentLease", "")
            if lease_id:
                import hashlib

                instance_cfg.client_token = (
                    "ea-" + hashlib.sha256(lease_id.encode()).hexdigest()[:61]
                )
                # Persist intent before RunInstances.  The SDK can time out
                # after AWS accepted the request, in which case no Instance is
                # returned to this coroutine even though a billable EC2 will
                # become visible later.
                await self.account_binding_store.update_lease(
                    lease_id,
                    launch_uncertain=True,
                    last_operation="create_instance",
                    error=None,
                )
            try:
                instance = await self.provider.create_instance(instance_cfg)
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
                metadata: dict[str, Any] = {}
                if lease_id:
                    metadata = {
                        "job_id": instance_cfg.tags.get("ElasticAgentJob", ""),
                        "account_id": instance_cfg.tags.get(
                            "ElasticAgentAccount", ""
                        ),
                        "lease_id": lease_id,
                        "controller_id": instance_cfg.tags.get(
                            "ElasticAgentController", ""
                        ),
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
                records.append(record)
            except BaseException as exc:  # noqa: BLE001
                compensation_succeeded = instance is None
                if instance is not None:
                    try:
                        await self.provider.terminate_instance(instance.instance_id)
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

                # ``scale_out(count=N)`` is one ownership transaction.  If a
                # later create fails, every earlier success from this call must
                # be compensated before the error escapes; otherwise the
                # orchestrator never receives their ids and cannot tear down
                # those billable instances.
                for created in reversed(records):
                    try:
                        await self.provider.terminate_instance(
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
                        await self.provider.terminate_instance(node.instance_id)
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
        if lease is not None and lease.state != LeaseState.RELEASED:
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
        await self.registry.remove(worker_id)

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

        async def cleanup_control_plane(lease) -> None:
            target = lease.worker_id or worker_id
            await self.registry.update(target, status=NodeStatus.TERMINATED)
            await self.connection_manager.disconnect_worker(target)

        released = await self.binding_manager.release(
            lease_id, cleanup_worker=cleanup_control_plane
        )
        if released is None or released.state != "released":
            raise RuntimeError(
                f"bound worker {worker_id} lease cleanup did not complete"
            )
        allocator = getattr(orchestrator, "_allocator", None) if orchestrator else None
        if allocator is not None:
            await allocator.release_owner(f"{released.job_id}:{released.slot}")

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
                await self.provider.terminate_instance(node.instance_id)
            except Exception:
                logger.exception(
                    "Failed to terminate instance %s during removal",
                    node.instance_id,
                )
                raise
            await self.connection_manager.disconnect_worker(node_id)
        await self.task_registry.cleanup_worker(node_id)
        await self.registry.remove(node_id)
        await self.event_bus.emit("NODE_REMOVED", node_id, {"instance_id": node.instance_id})
        logger.info("Node %s removed from registry", node_id)
        return True

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
        if lease_id:
            existing = self._bound_disconnect_tasks.get(worker_id)
            if existing is None or existing.done():
                self._bound_disconnect_cancel_events[worker_id] = asyncio.Event()
                self._bound_disconnect_tasks[worker_id] = asyncio.create_task(
                    self._cleanup_bound_after_disconnect(worker_id, lease_id)
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
