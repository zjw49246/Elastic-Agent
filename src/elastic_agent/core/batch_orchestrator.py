"""BatchOrchestrator — fan a single JobSpec out across N workers.

The missing orchestration layer: today the Manager can create instances
(`scale_out`) but nothing wires scale → bootstrap → login → dispatch → track for
a *batch*. This drives that lifecycle for a declarative (or uploaded-code) job:

    launch(spec):
      scale_out(N) → per worker, concurrently:
        provision (bootstrap) → login one account (worker-local) → run_command

Per-worker context (shard_index / hostname / account_email) is rendered into the
command before dispatch, so the same JobSpec produces correctly-sharded runs.

Mode-B rotation (strategy "a", `on_exhaust_restart_resume`): when a worker's run
command trips the rate-limit detectors, the worker interrupts it and signals the
Manager; :meth:`on_worker_exhausted` allocates+logs in a fresh account (into the
same config_dir) and restarts the command with ``rotation.resume_args`` so the
harness resumes instead of redoing completed work. Credentials are always minted
on the worker — the Manager only supplies account identities.

The orchestrator is decoupled from the concrete Manager via :class:`FleetDriver`,
so it unit-tests against a fake and plugs the real Manager in for production.
"""

from __future__ import annotations

import asyncio
import enum
import functools
import logging
import uuid
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Protocol

from elastic_agent.core.job_spec import JobSpec, WorkerContext
from elastic_agent.harness.base import Harness
from elastic_agent.harness.generic import build_execute, resolve_harness

logger = logging.getLogger(__name__)

PersistSpecHook = Callable[[str, JobSpec], Awaitable[None]]


class JobSpecPersistenceError(RuntimeError):
    """A Job could not be journaled before its first external side effect."""


class WorkerPhase(str, enum.Enum):
    PENDING = "pending"
    BOOTSTRAPPING = "bootstrapping"
    LOGGING_IN = "logging_in"
    RUNNING = "running"
    ROTATING = "rotating"
    DONE = "done"
    FAILED = "failed"


@dataclass
class LoginOutcome:
    success: bool
    account_id: str = ""
    account_email: str = ""
    error: str | None = None


@dataclass
class WorkerAssignment:
    """A reserved account/EIP lease for one fan-out slot.

    Bound jobs reserve all assignments *before* creating any instances.  The
    claim protects the account in :class:`AccountAllocator`; the lease protects
    the durable account/EIP binding in :class:`BindingManager`.  Keeping this as
    an orchestration DTO (instead of leaking either subsystem's persistence
    model into the orchestrator) makes the lifecycle and its compensation hooks
    explicit.
    """

    slot: int
    job_id: str
    account_id: str
    account_email: str
    claim_id: str
    lease_id: str
    eip_allocation_id: str
    eip: str
    region: str

    def instance_tags(self, job_id: str) -> dict[str, str]:
        """Cloud tags used to find/reconcile an interrupted bound launch."""
        return {
            "ElasticAgentJob": job_id,
            "ElasticAgentAccount": self.account_id,
            "ElasticAgentLease": self.lease_id,
        }


@dataclass
class WorkerRun:
    """Per-worker slice of a batch job.

    A worker may hold multiple pre-logged accounts (per_worker > 1), each in its
    own config_dir. ``active_slot`` indexes the account currently driving the run;
    on exhaustion the orchestrator advances to the next pre-logged slot (fast) or
    logs a fresh account into a new dir when the local pool is spent.
    """

    worker_id: str
    ctx: WorkerContext
    phase: WorkerPhase = WorkerPhase.PENDING
    task_id: str = ""
    config_dirs: list[str] = field(default_factory=list)
    account_ids: list[str] = field(default_factory=list)
    account_emails: list[str] = field(default_factory=list)
    active_slot: int = 0
    rotations: int = 0
    error: str | None = None
    # EIP-bound jobs are one-account/one-instance.  These fields let cleanup be
    # resumed deterministically after any bootstrap/login/run failure.
    lease_id: str = ""
    claim_id: str = ""
    eip_allocation_id: str = ""
    eip: str = ""
    assignment: WorkerAssignment | None = field(default=None, repr=False)
    final_collected: bool = False
    collection_error: str | None = None
    cleaned_up: bool = False
    cleanup_error: str | None = None
    cleanup_attempts: int = 0
    _finalize_lock: asyncio.Lock = field(default_factory=asyncio.Lock, repr=False)

    @property
    def account_id(self) -> str:
        return self.account_ids[self.active_slot] if self.active_slot < len(self.account_ids) else ""

    @property
    def account_email(self) -> str:
        return self.account_emails[self.active_slot] if self.active_slot < len(self.account_emails) else ""

    @property
    def config_dir(self) -> str:
        return self.config_dirs[self.active_slot] if self.active_slot < len(self.config_dirs) else ""


@dataclass
class BatchJob:
    job_id: str
    spec: JobSpec
    harness: Harness
    runs: dict[str, WorkerRun] = field(default_factory=dict)
    # Reservations that never reached a usable WorkerRun still need durable
    # lease/account cleanup (for example, one shard failed before scale-out or
    # RunInstances compensation hit a transient cloud error).
    pending_cleanup: dict[str, WorkerAssignment] = field(default_factory=dict)
    cleanup_errors: dict[str, str] = field(default_factory=dict)
    error: str | None = None

    @property
    def config_dir(self) -> str:
        return self.spec.account.config_dir

    def summary(self) -> dict[str, Any]:
        by_phase: dict[str, int] = {}
        for r in self.runs.values():
            by_phase[r.phase.value] = by_phase.get(r.phase.value, 0) + 1
        terminal = all(
            r.phase in (WorkerPhase.DONE, WorkerPhase.FAILED)
            for r in self.runs.values()
        )
        cleanup_pending = sum(
            1
            for r in self.runs.values()
            if self.spec.account.binding == "eip" and not r.cleaned_up
        ) + len(self.pending_cleanup)
        return {
            "job_id": self.job_id,
            "name": self.spec.name,
            "workers": len(self.runs),
            "phases": by_phase,
            "done": terminal and cleanup_pending == 0,
            "cleanup_pending": cleanup_pending,
            "error": self.error,
        }


class FleetDriver(Protocol):
    """What the orchestrator needs from the Manager. Real impl wraps
    ElasticAgentManager; tests inject a fake."""

    async def acquire_capacity(self, count: int) -> str:
        """Hold instance slots before a bound Job allocates any EIPs."""
        ...

    async def release_capacity(self, reservation_id: str) -> None:
        """Release a preflight hold after leases replace it or rollback ends."""
        ...

    async def scale_out(self, count: int, name_prefix: str = "",
                        instance_type: str = "", region: str = "",
                        disk_gb: int = 0, spot: bool = False,
                        tags: dict[str, str] | None = None) -> list[str]:
        """Create ``count`` workers (named ``<name_prefix>-<i>``); return worker_ids."""
        ...

    async def reserve_bound(
        self, job_id: str, slot: int, spec: JobSpec, account_id: str = "",
    ) -> WorkerAssignment:
        """Atomically claim one account and its durable EIP lease."""
        ...

    async def attach_bound(
        self, worker_id: str, assignment: WorkerAssignment,
    ) -> WorkerAssignment:
        """Attach the reserved EIP immediately after instance creation."""
        ...

    async def release_bound(
        self, assignment: WorkerAssignment, worker_id: str | None,
    ) -> None:
        """Detach EIP, force-terminate worker (if any), and free lease/claim."""
        ...

    async def hostname_of(self, worker_id: str) -> str:
        """Short hostname of a worker (for {{hostname}} / display). May be ''."""
        ...

    async def provision(self, worker_id: str, harness: Harness, spec: JobSpec) -> bool:
        """Run the bootstrap pipeline; return True on success."""
        ...

    async def login(
        self, worker_id: str, spec: JobSpec, config_dir: str, *,
        account_id: str = "", claim_id: str = "",
    ) -> LoginOutcome:
        """Allocate an account and have the worker log it in locally.

        ``ACCOUNT_LOGIN`` carries the mailbox authorization token from the
        Manager; generated Claude OAuth credentials are not returned.
        """
        ...

    async def run_command(
        self, worker_id: str, task_id: str, command: list[str], cwd: str,
        env: dict[str, str], timeout: int | None,
        job_id: str, watch_exhaustion: bool,
    ) -> None:
        """Dispatch an EXECUTE to the worker. ``watch_exhaustion`` tells the
        worker to scan output for rate-limit banners (Mode-B rotation)."""
        ...

    async def collect(self, worker_id: str, spec: JobSpec, job_id: str) -> None:
        """Pull the worker's results (collect.paths) back to the Manager."""
        ...

    async def scale_in(self, worker_ids: list[str]) -> None:
        """Tear down workers (idle scale-in)."""
        ...


async def _settle_owned_awaitable(
    awaitable: Awaitable[Any],
) -> tuple[Any, BaseException | None, asyncio.CancelledError | None]:
    """Let an ownership transaction finish despite repeated caller cancels.

    Cloud SDK work may already be running in a thread, so cancelling its asyncio
    waiter does not undo the external mutation.  Keep the child shielded until
    it has a real outcome, remember the first cancellation request, and let the
    caller decide how to compensate before re-raising it.
    """

    owned = asyncio.ensure_future(awaitable)
    pending_cancel: asyncio.CancelledError | None = None
    while not owned.done():
        try:
            await asyncio.shield(owned)
        except asyncio.CancelledError as exc:
            # A child that cancels itself is its own outcome, not a request to
            # cancel this owner. ``owned.result()`` below preserves it.
            if owned.done() and owned.cancelled():
                continue
            if pending_cancel is None:
                pending_cancel = exc
        except BaseException:
            # The child has a concrete failure.  Read it below instead of
            # letting the await boundary bypass the caller's compensation.
            pass

    try:
        return owned.result(), None, pending_cancel
    except BaseException as exc:  # child cancellation is an owned outcome
        return None, exc, pending_cancel


def _tracked_lifecycle(method):
    """Keep event-driven finalization alive and visible through shutdown."""
    @functools.wraps(method)
    async def wrapper(self, *args, **kwargs):
        async def run():
            task = asyncio.current_task()
            if task is not None:
                self._lifecycle_tasks.add(task)
            try:
                return await method(self, *args, **kwargs)
            finally:
                if task is not None:
                    self._lifecycle_tasks.discard(task)

        result, error, cancellation = await _settle_owned_awaitable(run())
        if error is not None:
            raise error
        if cancellation is not None:
            raise cancellation
        return result

    return wrapper


class BatchOrchestrator:
    def __init__(
        self,
        driver: FleetDriver,
        *,
        scale_in_on_complete: bool = False,
        final_collect_attempts: int = 3,
        final_collect_timeout: float = 300.0,
        cleanup_retry_seconds: float = 5.0,
        persist_spec_hook: PersistSpecHook | None = None,
    ) -> None:
        self._driver = driver
        self._scale_in_on_complete = scale_in_on_complete
        self._final_collect_attempts = max(1, final_collect_attempts)
        self._final_collect_timeout = max(0.01, final_collect_timeout)
        self._cleanup_retry_seconds = max(0.01, cleanup_retry_seconds)
        self._persist_spec_hook = persist_spec_hook
        self._jobs: dict[str, BatchJob] = {}
        self._worker_index: dict[str, str] = {}  # worker_id -> job_id
        self._collect_tasks: dict[str, asyncio.Task] = {}  # worker_id -> periodic collect
        self._cleanup_tasks: dict[str, asyncio.Task] = {}  # worker_id -> durable teardown retry
        self._launch_tasks: set[asyncio.Task] = set()  # background bring-ups (submit)
        self._lifecycle_tasks: set[asyncio.Task] = set()
        self._shutting_down = False
        self._shutdown_lock = asyncio.Lock()
        self._shutdown_event = asyncio.Event()

    def get_job(self, job_id: str) -> BatchJob | None:
        return self._jobs.get(job_id)

    def list_jobs(self) -> list[BatchJob]:
        return list(self._jobs.values())

    def job_id_for_worker(self, worker_id: str) -> str | None:
        return self._worker_index.get(worker_id)

    async def handle_exhausted(self, worker_id: str) -> bool:
        """Route a worker's RUN_EXHAUSTED by resolving its job (connection knows
        the worker_id; the run command carried the job_id)."""
        job_id = self._worker_index.get(worker_id)
        if job_id is None:
            return False
        return await self.on_worker_exhausted(job_id, worker_id)

    async def handle_exit(self, worker_id: str, exit_code: int, task_id: str | None = None) -> None:
        """Route a worker's PROCESS_EXIT by resolving its job."""
        job_id = self._worker_index.get(worker_id)
        if job_id is not None:
            await self.on_worker_exit(job_id, worker_id, exit_code, task_id=task_id)

    def prepare(self, spec: JobSpec) -> BatchJob:
        """Validate and assign a job id without registering or starting it.

        API callers use this side-effect-free boundary to durably persist the
        spec before any account reservation or cloud operation can begin.
        ``resolve_harness`` stays synchronous so bad uploads still fail fast.
        """
        if self._shutting_down:
            raise RuntimeError("batch orchestrator is shutting down")
        harness = resolve_harness(spec)
        return BatchJob(
            job_id=f"job-{uuid.uuid4().hex}",
            spec=spec,
            harness=harness,
        )

    def _schedule_prepared(self, job: BatchJob) -> asyncio.Task:
        if self._shutting_down:
            raise RuntimeError("batch orchestrator is shutting down")
        if job.job_id in self._jobs:
            raise ValueError(f"job {job.job_id!r} is already registered")
        self._jobs[job.job_id] = job
        task = asyncio.create_task(self._bring_up_all(job))
        self._launch_tasks.add(task)
        task.add_done_callback(self._launch_tasks.discard)
        return task

    async def _persist_then_schedule(self, job: BatchJob) -> asyncio.Task:
        """Journal ``job`` before registering it or starting bring-up."""

        if self._shutting_down:
            raise RuntimeError("batch orchestrator is shutting down")
        if job.job_id in self._jobs:
            raise ValueError(f"job {job.job_id!r} is already registered")
        if self._persist_spec_hook is not None:
            try:
                await self._persist_spec_hook(job.job_id, job.spec)
            except Exception as exc:  # noqa: BLE001
                raise JobSpecPersistenceError(
                    f"failed to persist JobSpec for {job.job_id!r} before launch: {exc}"
                ) from exc
        return self._schedule_prepared(job)

    async def submit_prepared(self, job: BatchJob) -> BatchJob:
        """Persist, register, and start background bring-up."""
        await self._persist_then_schedule(job)
        return job

    async def submit(self, spec: JobSpec) -> BatchJob:
        """Register the job and return immediately; run scale-out + bring-up in
        the background. Scale-out and per-worker bootstrap/login take tens of
        seconds to minutes — awaiting them (as ``launch`` does) makes the HTTP
        submit hang with no UI feedback, which reads as a dead button and invites
        double-submits. ``submit`` returns as soon as the job_id exists so the UI
        can start polling ``/jobs/{id}``."""
        return await self.submit_prepared(self.prepare(spec))

    async def launch(self, spec: JobSpec) -> BatchJob:
        """Scale out, then bring every worker up concurrently (awaits fully)."""
        job = self.prepare(spec)
        task = await self._persist_then_schedule(job)
        _result, error, cancellation = await _settle_owned_awaitable(task)
        if error is not None:
            raise error
        if cancellation is not None:
            raise cancellation
        return job

    async def shutdown(self, timeout: float = 30.0) -> None:
        """Stop background work and best-effort release every bound worker.

        The durable lease journal remains the authority if the bounded cleanup
        cannot finish.  Crucially, this method awaits cancellation of every
        task before Manager releases its controller lock, so an old process can
        no longer mutate EIPs concurrently with its replacement.
        """
        async with self._shutdown_lock:
            self._shutting_down = True
            self._shutdown_event.set()

            periodic = list(self._collect_tasks.values())
            self._collect_tasks.clear()
            for task in periodic:
                task.cancel()
            if periodic:
                await asyncio.gather(*periodic, return_exceptions=True)

            launches = list(self._launch_tasks)
            if launches:
                # Do not cancel cloud work backed by asyncio.to_thread: task
                # cancellation cannot stop the boto3 thread and would let it
                # mutate EC2/EIP after the Manager unlocks.  Each bring-up sees
                # `_shutting_down` at transaction boundaries and compensates;
                # wait until those external calls actually return.
                await asyncio.gather(*launches, return_exceptions=True)
            self._launch_tasks.clear()

            # PROCESS_EXIT, disconnect cancellation, and exhaustion handlers
            # are awaited by external connection tasks rather than registered
            # in `_launch_tasks`.  They can hold a finalize lock and write the
            # durable lease store, so quiesce every one before direct settlement
            # or controller unlock.  Repeat in case one was queued at the
            # boundary while the first snapshot was completing.
            while self._lifecycle_tasks:
                await asyncio.gather(
                    *list(self._lifecycle_tasks), return_exceptions=True
                )

            retries = list(self._cleanup_tasks.values())
            if retries:
                # Retry loops wake on `_shutdown_event`; a retry already inside
                # a cloud call is allowed to finish before direct settlement.
                await asyncio.gather(*retries, return_exceptions=True)
            self._cleanup_tasks.clear()

            for job in self._jobs.values():
                for run in job.runs.values():
                    if run.phase not in (WorkerPhase.DONE, WorkerPhase.FAILED):
                        self._fail(run, "manager shutting down")
                        job.error = job.error or "manager shutting down"

            async def settle_run(job: BatchJob, run: WorkerRun) -> None:
                if not self._is_eip_bound(job.spec) or run.cleaned_up:
                    return
                async with run._finalize_lock:
                    if not run.final_collected:
                        if run.task_id:
                            try:
                                await asyncio.wait_for(
                                    self._driver.collect(
                                        run.worker_id, job.spec, job.job_id
                                    ),
                                    timeout=max(0.1, min(10.0, timeout / 2)),
                                )
                                run.collection_error = None
                            except Exception as exc:  # noqa: BLE001
                                run.collection_error = str(exc) or type(exc).__name__
                                run.error = (
                                    "shutdown result collection failed: "
                                    f"{run.collection_error}"
                                )
                                job.error = job.error or run.error
                        run.final_collected = True
                    if run.assignment is None:
                        return
                    try:
                        run.cleanup_attempts += 1
                        await self._driver.release_bound(
                            run.assignment, run.worker_id
                        )
                    except Exception as exc:  # noqa: BLE001
                        run.cleanup_error = str(exc) or type(exc).__name__
                        logger.exception(
                            "shutdown cleanup deferred for lease %s", run.lease_id
                        )
                    else:
                        run.cleaned_up = True
                        run.cleanup_error = None
                        self._worker_index.pop(run.worker_id, None)

            async def settle_pending(
                job: BatchJob, assignment: WorkerAssignment
            ) -> None:
                try:
                    await self._driver.release_bound(assignment, None)
                except Exception as exc:  # noqa: BLE001
                    job.cleanup_errors[assignment.lease_id] = (
                        str(exc) or type(exc).__name__
                    )
                else:
                    job.pending_cleanup.pop(assignment.lease_id, None)
                    job.cleanup_errors.pop(assignment.lease_id, None)

            settlement = [
                settle_run(job, run)
                for job in self._jobs.values()
                for run in job.runs.values()
            ] + [
                settle_pending(job, assignment)
                for job in self._jobs.values()
                for assignment in list(job.pending_cleanup.values())
            ]
            if settlement:
                # Destructive cloud calls commonly run in `to_thread`; a
                # timeout would cancel only the coroutine while boto3 kept
                # mutating resources under a soon-to-be-released controller
                # lock. Await the real transactions to completion. The timeout
                # parameter only bounds non-destructive final collection above.
                await asyncio.gather(*settlement, return_exceptions=True)

    async def _bring_up_all(self, job: BatchJob) -> None:
        spec = job.spec
        try:
            if self._is_eip_bound(spec):
                await self._bring_up_bound_all(job)
            else:
                await self._bring_up_unbound_all(job)
        except Exception as exc:
            # Runs in a background task under ``submit`` — an unhandled scale-out
            # error would otherwise vanish. Log and fall through to settle state.
            job.error = str(exc)
            logger.exception("bring-up failed for job %s", job.job_id)
        # A job that fails entirely during provision/login never reaches a run
        # exit, so settle terminal state (and release its accounts) here too.
        await self._maybe_finish(job)

    async def _bring_up_unbound_all(self, job: BatchJob) -> None:
        """Original fleet path, kept unchanged for ``account.binding=none``."""
        spec = job.spec
        n = max(1, spec.fanout.workers)
        worker_ids = await self._driver.scale_out(
            n, name_prefix=spec.fanout.name_prefix or spec.name,
            instance_type=spec.fanout.instance_type, region=spec.fanout.region,
            disk_gb=spec.fanout.disk_gb, spot=spec.fanout.spot)
        contexts = spec.worker_contexts()
        for wid, ctx in zip(worker_ids, contexts):
            ctx.hostname = await self._driver.hostname_of(wid)
            job.runs[wid] = WorkerRun(worker_id=wid, ctx=ctx)
            self._worker_index[wid] = job.job_id

        await asyncio.gather(
            *(self._bring_up(job, wid) for wid in job.runs),
            return_exceptions=True,
        )

    async def _bring_up_bound_all(self, job: BatchJob) -> None:
        """Reserve account/EIP leases first, then create one EC2 per lease.

        This ordering is the central invariant of EIP binding: no chargeable
        instance is created until every requested shard owns a durable account
        lease.  Reservations run concurrently, but every task is settled before
        the capacity hold is released or compensation begins.  A reservation
        failure rolls the whole successful set back and creates no instances.
        """
        spec = job.spec
        contexts = spec.worker_contexts()
        requested_ids = list(getattr(spec.account, "ids", []) or [])
        try:
            capacity_reservation = await self._driver.acquire_capacity(
                len(contexts)
            )
        except BaseException as exc:
            job.error = f"instance capacity preflight failed: {exc}"
            logger.exception("bound capacity preflight failed for job %s", job.job_id)
            if isinstance(exc, asyncio.CancelledError):
                raise
            return

        assignments: list[WorkerAssignment] = []
        cancellation: asyncio.CancelledError | None = None
        reservation_error: BaseException | None = None
        rolled_back = False

        def remember_cancel(observed: asyncio.CancelledError | None) -> None:
            nonlocal cancellation
            if cancellation is None and observed is not None:
                cancellation = observed

        async def rollback_assignments() -> None:
            """Release every successful lease without abandoning cloud cleanup."""
            nonlocal rolled_back
            if rolled_back or not assignments:
                rolled_back = True
                return
            cleanup_group = asyncio.gather(
                *(
                    self._release_unattached(job, assignment)
                    for assignment in reversed(assignments)
                ),
                return_exceptions=True,
            )
            results, group_error, observed_cancel = (
                await _settle_owned_awaitable(cleanup_group)
            )
            remember_cancel(observed_cancel)
            rolled_back = True
            for result in results or []:
                if isinstance(result, asyncio.CancelledError):
                    remember_cancel(result)
            cleanup_failures = [
                result for result in (results or [])
                if isinstance(result, BaseException)
            ]
            if group_error is not None:
                cleanup_failures.append(group_error)
            if cleanup_failures:
                detail = str(cleanup_failures[0]) or type(
                    cleanup_failures[0]
                ).__name__
                job.error = job.error or f"EIP reservation rollback failed: {detail}"

        if self._shutting_down:
            reservation_error = RuntimeError("manager shutting down")
        else:
            # Start every reservation before awaiting any one of them.  The
            # all-settled group prevents a fast failure (or repeated caller
            # cancellation) from orphaning a slower AllocateAddress call.
            reservation_tasks = [
                asyncio.create_task(self._driver.reserve_bound(
                    job.job_id,
                    slot,
                    spec,
                    account_id=(requested_ids[slot] if requested_ids else ""),
                ))
                for slot, _ctx in enumerate(contexts)
            ]
            reservation_group = asyncio.gather(
                *reservation_tasks, return_exceptions=True
            )
            results, group_error, observed_cancel = (
                await _settle_owned_awaitable(reservation_group)
            )
            remember_cancel(observed_cancel)
            if group_error is not None:
                if isinstance(group_error, asyncio.CancelledError):
                    remember_cancel(group_error)
                else:
                    reservation_error = group_error
            else:
                failures: list[BaseException] = []
                for result in results:
                    if isinstance(result, WorkerAssignment):
                        assignments.append(result)
                    elif isinstance(result, asyncio.CancelledError):
                        remember_cancel(result)
                    elif isinstance(result, BaseException):
                        failures.append(result)
                    else:
                        failures.append(RuntimeError(
                            "reserve_bound returned an invalid assignment"
                        ))
                if failures:
                    reservation_error = failures[0]
                elif len(assignments) != len(contexts):
                    reservation_error = RuntimeError(
                        "invalid assignment result count"
                    )

        if cancellation is not None:
            job.error = "EIP reservation cancelled"
        elif reservation_error is not None:
            detail = str(reservation_error) or type(reservation_error).__name__
            job.error = f"EIP reservation failed: {detail}"
            logger.error(
                "bound reservation failed for job %s: %s",
                job.job_id,
                detail,
            )

        if cancellation is not None or reservation_error is not None:
            await rollback_assignments()

        # The hold prevents concurrent Jobs from consuming these slots while
        # account/EIP reservations are created.  Settle release despite repeated
        # cancellation.  An ordinary release error is fail-closed: roll back all
        # leases, retry the idempotent hold release once, and never scale.
        _result, capacity_error, observed_cancel = (
            await _settle_owned_awaitable(
                self._driver.release_capacity(capacity_reservation)
            )
        )
        remember_cancel(observed_cancel)
        if capacity_error is not None:
            if isinstance(capacity_error, asyncio.CancelledError):
                remember_cancel(capacity_error)
            detail = str(capacity_error) or type(capacity_error).__name__
            job.error = f"instance capacity release failed: {detail}"
            await rollback_assignments()
            _result, retry_error, observed_cancel = (
                await _settle_owned_awaitable(
                    self._driver.release_capacity(capacity_reservation)
                )
            )
            remember_cancel(observed_cancel)
            if retry_error is not None:
                retry_detail = str(retry_error) or type(retry_error).__name__
                job.error += f"; retry failed: {retry_detail}"

        # Cancellation may arrive while the capacity hold itself is being
        # released.  It still changes a successful reservation transaction into
        # rollback, and cleanup must settle before cancellation is re-raised.
        if cancellation is not None and not rolled_back:
            job.error = "EIP reservation cancelled"
            await rollback_assignments()

        if cancellation is not None:
            raise cancellation
        if job.error is not None:
            return

        # Each call creates exactly one temporary instance and associates its
        # reserved EIP before hostname lookup/bootstrap can touch the box.
        await asyncio.gather(
            *(
                self._create_and_bring_up_bound(job, ctx, assignment)
                for ctx, assignment in zip(contexts, assignments)
            ),
            return_exceptions=True,
        )

    async def _release_unattached(
        self, job: BatchJob, assignment: WorkerAssignment
    ) -> None:
        job.pending_cleanup[assignment.lease_id] = assignment
        try:
            await self._driver.release_bound(assignment, None)
        except BaseException as exc:  # noqa: BLE001
            job.cleanup_errors[assignment.lease_id] = (
                str(exc) or type(exc).__name__
            )
            logger.exception("failed to release unattached lease %s", assignment.lease_id)
            self._schedule_unattached_cleanup(job, assignment)
            if isinstance(exc, asyncio.CancelledError):
                raise
        else:
            job.pending_cleanup.pop(assignment.lease_id, None)
            job.cleanup_errors.pop(assignment.lease_id, None)

    def _schedule_unattached_cleanup(
        self, job: BatchJob, assignment: WorkerAssignment
    ) -> None:
        key = f"lease:{assignment.lease_id}"
        existing = self._cleanup_tasks.get(key)
        if existing is not None and not existing.done():
            return

        async def retry() -> None:
            try:
                while assignment.lease_id in job.pending_cleanup:
                    try:
                        await asyncio.wait_for(
                            self._shutdown_event.wait(),
                            timeout=self._cleanup_retry_seconds,
                        )
                        return
                    except asyncio.TimeoutError:
                        pass
                    try:
                        await self._driver.release_bound(assignment, None)
                    except Exception as exc:  # noqa: BLE001
                        job.cleanup_errors[assignment.lease_id] = str(exc)
                        logger.exception(
                            "retrying unattached cleanup failed for lease %s",
                            assignment.lease_id,
                        )
                    else:
                        job.pending_cleanup.pop(assignment.lease_id, None)
                        job.cleanup_errors.pop(assignment.lease_id, None)
                        return
            except asyncio.CancelledError:
                return
            finally:
                self._cleanup_tasks.pop(key, None)

        self._cleanup_tasks[key] = asyncio.create_task(retry())

    async def _create_and_bring_up_bound(
        self, job: BatchJob, ctx: WorkerContext, assignment: WorkerAssignment,
    ) -> None:
        spec = job.spec
        worker_id: str | None = None
        run: WorkerRun | None = None
        try:
            prefix = spec.fanout.name_prefix or spec.name
            worker_ids = await self._driver.scale_out(
                1, name_prefix=f"{prefix}-{assignment.slot}",
                instance_type=spec.fanout.instance_type, region=spec.fanout.region,
                disk_gb=spec.fanout.disk_gb, spot=spec.fanout.spot,
                tags=assignment.instance_tags(job.job_id),
            )
            if len(worker_ids) != 1:
                raise RuntimeError(
                    f"bound scale_out returned {len(worker_ids)} workers for one lease"
                )
            worker_id = worker_ids[0]
            if self._shutting_down:
                raise RuntimeError("manager shutting down")
            assignment = await self._driver.attach_bound(worker_id, assignment)
            ctx.hostname = await self._driver.hostname_of(worker_id)

            slots = self._slot_dirs(job)
            config_dir = slots[0] if slots else spec.account.config_dir
            run = WorkerRun(
                worker_id=worker_id,
                ctx=ctx,
                config_dirs=[config_dir],
                account_ids=[assignment.account_id],
                account_emails=[assignment.account_email],
                lease_id=assignment.lease_id,
                claim_id=assignment.claim_id,
                eip_allocation_id=assignment.eip_allocation_id,
                eip=assignment.eip,
                assignment=assignment,
            )
            job.runs[worker_id] = run
            self._worker_index[worker_id] = job.job_id
            await self._bring_up(job, worker_id)
        except BaseException as exc:
            logger.exception(
                "bound create/attach failed for job %s slot %s",
                job.job_id, assignment.slot,
            )
            cancelled = isinstance(exc, asyncio.CancelledError)
            detail = "manager shutting down" if cancelled else str(exc)
            job.error = job.error or (
                f"bound worker slot {assignment.slot} failed: {detail}"
            )
            if run is None and worker_id is not None:
                # Keep the failed real instance visible in job detail while its
                # release hook detaches the EIP and force-terminates it.
                run = WorkerRun(
                    worker_id=worker_id,
                    ctx=ctx,
                    phase=WorkerPhase.FAILED,
                    account_ids=[assignment.account_id],
                    account_emails=[assignment.account_email],
                    lease_id=assignment.lease_id,
                    claim_id=assignment.claim_id,
                    eip_allocation_id=assignment.eip_allocation_id,
                    eip=assignment.eip,
                    assignment=assignment,
                    error=detail,
                )
                job.runs[worker_id] = run
                self._worker_index[worker_id] = job.job_id
            elif run is not None:
                self._fail(run, detail)

            if run is None:
                # scale_out itself failed, so there is no worker to collect or
                # terminate; the lease/account claim still must be released.
                if cancelled:
                    job.pending_cleanup[assignment.lease_id] = assignment
                    raise
                await self._release_unattached(job, assignment)
                return

            if cancelled:
                raise

        if run is not None and run.phase == WorkerPhase.FAILED:
            await self._finalize_terminal_run(job, run)

    async def _bring_up(self, job: BatchJob, worker_id: str) -> None:
        run = job.runs[worker_id]
        spec = job.spec
        try:
            run.phase = WorkerPhase.BOOTSTRAPPING
            if not await self._driver.provision(worker_id, job.harness, spec):
                return self._fail(run, "bootstrap failed")
            if self._shutting_down:
                return self._fail(run, "manager shutting down")
            if run.phase in (WorkerPhase.DONE, WorkerPhase.FAILED):
                return

            if spec.account.mode != "none":
                run.phase = WorkerPhase.LOGGING_IN
                if self._is_eip_bound(spec):
                    # The account was selected and claimed before EC2 creation;
                    # login that exact identity rather than allocating again.
                    outcome = await self._driver.login(
                        worker_id, spec, run.config_dir,
                        account_id=run.account_id, claim_id=run.claim_id,
                    )
                    if not outcome.success:
                        return self._fail(run, outcome.error or "login failed")
                    if outcome.account_id and outcome.account_id != run.account_id:
                        return self._fail(run, "bound login returned a different account")
                    if outcome.account_email:
                        run.account_emails[0] = outcome.account_email
                else:
                    # Log in one account per credential slot (per_worker accounts,
                    # each into its own config_dir) so rotation can switch locally.
                    for slot in self._slot_dirs(job):
                        outcome = await self._driver.login(worker_id, spec, slot)
                        if not outcome.success:
                            return self._fail(run, outcome.error or "login failed")
                        run.config_dirs.append(slot)
                        run.account_ids.append(outcome.account_id)
                        run.account_emails.append(outcome.account_email)
                    run.active_slot = 0

            if self._shutting_down:
                return self._fail(run, "manager shutting down")

            if run.phase in (WorkerPhase.DONE, WorkerPhase.FAILED):
                return

            await self._dispatch(job, run, resume=False)
        except Exception as exc:  # pragma: no cover - defensive
            logger.exception("bring-up failed for %s", worker_id)
            self._fail(run, str(exc))

    def _slot_dirs(self, job: BatchJob) -> list[str]:
        """config_dirs to log accounts into — from the harness credential slots,
        falling back to a single default slot."""
        slots = [s.get("config_dir", "") for s in job.harness.get_credential_slots()]
        return slots or [job.spec.account.config_dir]

    @staticmethod
    def _is_eip_bound(spec: JobSpec) -> bool:
        return getattr(spec.account, "binding", "none") == "eip"

    async def _dispatch(self, job: BatchJob, run: WorkerRun, *, resume: bool) -> None:
        if self._shutting_down:
            self._fail(run, "manager shutting down")
            return
        spec = job.spec
        run.ctx.config_dir = run.config_dir
        run.ctx.account_id = run.account_id
        run.ctx.account_email = run.account_email
        ex = build_execute(spec, run.ctx, resume=resume)
        run.task_id = f"{job.job_id}:{run.worker_id}:{uuid.uuid4().hex[:6]}"
        run.phase = WorkerPhase.RUNNING
        await self._driver.run_command(
            run.worker_id, run.task_id,
            command=ex["command"], cwd=ex["cwd"], env=ex["env"], timeout=ex["timeout"],
            job_id=job.job_id,
            watch_exhaustion=spec.rotation.strategy != "none",
        )
        self._start_periodic_collect(job, run.worker_id)

    def _start_periodic_collect(self, job: BatchJob, worker_id: str) -> None:
        """While the run goes, pull results back every ``collect.interval_seconds``
        (0 = off) so long runs stream partial results to the Manager → S3 as
        tasks finish. One loop per worker; survives rotation."""
        spec = job.spec
        interval = spec.collect.interval_seconds
        if interval <= 0 or not spec.collect.paths:
            return
        existing = self._collect_tasks.get(worker_id)
        if existing and not existing.done():
            return

        async def _loop() -> None:
            try:
                while True:
                    await asyncio.sleep(interval)
                    run = job.runs.get(worker_id)
                    if run is None or run.phase in (WorkerPhase.DONE, WorkerPhase.FAILED):
                        return
                    try:
                        await self._driver.collect(worker_id, spec, job.job_id)
                    except Exception:
                        logger.exception("periodic collect failed for %s", worker_id)
            except asyncio.CancelledError:
                return

        self._collect_tasks[worker_id] = asyncio.create_task(_loop())

    async def _stop_periodic_collect(self, worker_id: str) -> None:
        """Cancel and fully quiesce a periodic collect before final teardown."""
        task = self._collect_tasks.pop(worker_id, None)
        if task and not task.done():
            task.cancel()
        if task is not None and task is not asyncio.current_task():
            await asyncio.gather(task, return_exceptions=True)

    # -- lifecycle events (called by the Manager's message handlers) --------

    @_tracked_lifecycle
    async def on_worker_exhausted(self, job_id: str, worker_id: str) -> bool:
        """Mode-B rotation (strategy a): swap account + restart with --resume.

        Returns True if a rotation was started, False if the policy declined
        (wrong strategy, max rotations reached, or re-login failed → FAILED).
        """
        if self._shutting_down:
            return False
        job = self._jobs.get(job_id)
        if job is None or worker_id not in job.runs:
            return False
        run = job.runs[worker_id]
        spec = job.spec

        if spec.rotation.strategy != "on_exhaust_restart_resume":
            self._fail(run, "account exhausted (no rotation policy)")
            await self._finalize_terminal_run(job, run)
            await self._maybe_finish(job)
            return False
        if run.rotations >= spec.rotation.max_rotations:
            self._fail(run, f"account exhausted (max {spec.rotation.max_rotations} rotations reached)")
            await self._finalize_terminal_run(job, run)
            await self._maybe_finish(job)
            return False

        run.phase = WorkerPhase.ROTATING
        run.rotations += 1

        if run.active_slot + 1 < len(run.config_dirs):
            # A pre-logged account is ready in the local pool — switch to it with
            # no re-login (the fast path that per_worker > 1 buys).
            run.active_slot += 1
        else:
            # Local pool spent: log a fresh account into a new config_dir.
            new_dir = self._extra_dir(job, run)
            outcome = await self._driver.login(worker_id, spec, new_dir)
            if not outcome.success:
                self._fail(run, outcome.error or "rotation login failed")
                await self._finalize_terminal_run(job, run)
                await self._maybe_finish(job)
                return False
            run.config_dirs.append(new_dir)
            run.account_ids.append(outcome.account_id)
            run.account_emails.append(outcome.account_email)
            run.active_slot = len(run.config_dirs) - 1

        await self._dispatch(job, run, resume=True)
        return True

    @staticmethod
    def _extra_dir(job: BatchJob, run: WorkerRun) -> str:
        """A fresh config_dir for a rotation beyond the pre-logged pool."""
        base = run.config_dirs[0] or job.spec.account.config_dir or "/root/.claude"
        return f"{base}-rot-{run.rotations}"

    @_tracked_lifecycle
    async def on_worker_exit(
        self, job_id: str, worker_id: str, exit_code: int, task_id: str | None = None,
    ) -> None:
        """Terminal process exit for a worker's run command."""
        if self._shutting_down:
            return
        job = self._jobs.get(job_id)
        if job is None or worker_id not in job.runs:
            return
        run = job.runs[worker_id]
        # Ignore a stale exit from a superseded run: when an exhausted run is
        # interrupted and the rotation restart has already re-dispatched (new
        # task_id), the interrupted run's non-zero exit still arrives — it must
        # not fail the fresh run. Matching on task_id closes the race that the
        # ROTATING phase-guard alone can't (the restart may already be RUNNING).
        if task_id is not None and run.task_id and task_id != run.task_id:
            return
        if run.phase in (WorkerPhase.DONE, WorkerPhase.FAILED, WorkerPhase.ROTATING):
            return
        if exit_code == job.spec.completion.on_process_exit:
            run.phase = WorkerPhase.DONE
            run.error = None
        else:
            self._fail(run, f"run exited {exit_code}")
        # Final collect happens before a bound EIP is detached and its temporary
        # EC2 is force-terminated.  A failed run still yields partial results.
        await self._finalize_terminal_run(job, run)
        await self._maybe_finish(job)

    @_tracked_lifecycle
    async def cancel_worker(self, worker_id: str, reason: str) -> bool:
        """Fail and safely finalize one batch worker (admin/disconnect path).

        Returns ``True`` only after bound infrastructure cleanup completed.  A
        ``False`` result means the durable background retry remains active and
        callers must preserve the registry record instead of reporting success.
        """
        if self._shutting_down:
            return False
        job_id = self._worker_index.get(worker_id)
        job = self._jobs.get(job_id) if job_id else None
        if job is None or worker_id not in job.runs:
            return False
        run = job.runs[worker_id]
        if run.phase not in (WorkerPhase.DONE, WorkerPhase.FAILED):
            self._fail(run, reason)
        await self._finalize_terminal_run(job, run)
        await self._maybe_finish(job)
        return not self._is_eip_bound(job.spec) or run.cleaned_up

    async def _finalize_terminal_run(self, job: BatchJob, run: WorkerRun) -> None:
        """Collect once, then compensate EIP-bound infrastructure once.

        The per-run lock makes duplicate PROCESS_EXIT / bring-up settlement
        harmless.  Cleanup is deliberately after collection while the EIP still
        routes to the worker.
        """
        if run.phase not in (WorkerPhase.DONE, WorkerPhase.FAILED):
            return
        async with run._finalize_lock:
            # Periodic SSH/rsync must cross this barrier before the lease can
            # detach its EIP and destroy the remote filesystem.
            await self._stop_periodic_collect(run.worker_id)
            if not run.final_collected and run.task_id:
                async def collect_with_retries() -> None:
                    for attempt in range(1, self._final_collect_attempts + 1):
                        try:
                            await self._driver.collect(
                                run.worker_id, job.spec, job.job_id
                            )
                            run.collection_error = None
                            return
                        except Exception as exc:  # noqa: BLE001
                            run.collection_error = str(exc) or type(exc).__name__
                            logger.exception(
                                "final collect attempt %s/%s failed for %s",
                                attempt,
                                self._final_collect_attempts,
                                run.worker_id,
                            )
                            if attempt < self._final_collect_attempts:
                                await asyncio.sleep(min(float(attempt), 3.0))

                try:
                    await asyncio.wait_for(
                        collect_with_retries(),
                        timeout=self._final_collect_timeout,
                    )
                except asyncio.TimeoutError:
                    run.collection_error = (
                        "timed out after "
                        f"{self._final_collect_timeout:g}s total final collection"
                    )
                    logger.error(
                        "final collect timed out after %ss for %s",
                        self._final_collect_timeout,
                        run.worker_id,
                    )
                # A failed collection must not retain a costly EC2 forever.
                # Surface the loss risk explicitly; periodically pushed results
                # remain durable, but the temporary root disk is still removed.
                run.final_collected = True
                if run.collection_error:
                    run.phase = WorkerPhase.FAILED
                    run.error = (
                        "final result collection failed before instance teardown: "
                        f"{run.collection_error}"
                    )
                    job.error = job.error or run.error
            elif not run.task_id:
                # Bootstrap/login failed before a command existed; attempting
                # rsync here can hang on an unreachable fresh instance and delay
                # EIP detach/termination, while there cannot be run output yet.
                run.final_collected = True

            if (
                self._is_eip_bound(job.spec)
                and not run.cleaned_up
                and run.assignment is not None
            ):
                try:
                    run.cleanup_attempts += 1
                    await self._driver.release_bound(run.assignment, run.worker_id)
                except Exception as exc:  # noqa: BLE001
                    run.cleanup_error = str(exc)
                    logger.exception(
                        "bound cleanup failed for worker %s lease %s",
                        run.worker_id, run.lease_id,
                    )
                    self._schedule_bound_cleanup(job, run)
                else:
                    run.cleaned_up = True
                    run.cleanup_error = None
                    self._worker_index.pop(run.worker_id, None)

    def _schedule_bound_cleanup(self, job: BatchJob, run: WorkerRun) -> None:
        existing = self._cleanup_tasks.get(run.worker_id)
        if existing is not None and not existing.done():
            return

        async def retry() -> None:
            try:
                while not run.cleaned_up and run.assignment is not None:
                    try:
                        await asyncio.wait_for(
                            self._shutdown_event.wait(),
                            timeout=self._cleanup_retry_seconds,
                        )
                        return
                    except asyncio.TimeoutError:
                        pass
                    async with run._finalize_lock:
                        if run.cleaned_up:
                            return
                        try:
                            run.cleanup_attempts += 1
                            await self._driver.release_bound(
                                run.assignment, run.worker_id
                            )
                        except Exception as exc:  # noqa: BLE001
                            run.cleanup_error = str(exc)
                            logger.exception(
                                "retrying bound cleanup failed for %s lease %s",
                                run.worker_id,
                                run.lease_id,
                            )
                        else:
                            run.cleaned_up = True
                            run.cleanup_error = None
                            self._worker_index.pop(run.worker_id, None)
                            return
            except asyncio.CancelledError:
                return
            finally:
                self._cleanup_tasks.pop(run.worker_id, None)

        self._cleanup_tasks[run.worker_id] = asyncio.create_task(retry())

    async def _maybe_finish(self, job: BatchJob) -> None:
        terminal = (WorkerPhase.DONE, WorkerPhase.FAILED)
        # A bootstrap/login failure must release its EIP immediately even while
        # other shards keep running for hours.
        for run in job.runs.values():
            if run.phase in terminal:
                await self._finalize_terminal_run(job, run)

        if not all(r.phase in terminal for r in job.runs.values()):
            return
        # Release each worker's accounts back to the allocator once the job is
        # terminal (DONE/FAILED) so a later job can reuse them. Previously
        # accounts were only freed on scale-in (_scale_in_on_complete defaults
        # to False), which starved single-account setups after the first job
        # finished. release_worker is idempotent (pops by worker_id), so calling
        # this across the several terminal paths is safe.
        allocator = getattr(self, "_allocator", None)
        if allocator is not None:
            for worker_id in job.runs:
                try:
                    await allocator.release_worker(worker_id)
                except Exception:  # pragma: no cover - defensive
                    logger.exception("account release failed for %s", worker_id)
        if self._scale_in_on_complete and not self._is_eip_bound(job.spec):
            await self._driver.scale_in(list(job.runs.keys()))

    # -- helpers -----------------------------------------------------------

    @staticmethod
    def _fail(run: WorkerRun, error: str) -> None:
        run.phase = WorkerPhase.FAILED
        run.error = error
        logger.warning("worker %s failed: %s", run.worker_id, error)
