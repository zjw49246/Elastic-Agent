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
import logging
import uuid
from dataclasses import dataclass, field
from typing import Any, Protocol

from elastic_agent.core.job_spec import JobSpec, WorkerContext
from elastic_agent.harness.base import Harness
from elastic_agent.harness.generic import build_execute, resolve_harness

logger = logging.getLogger(__name__)


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

    @property
    def config_dir(self) -> str:
        return self.spec.account.config_dir

    def summary(self) -> dict[str, Any]:
        by_phase: dict[str, int] = {}
        for r in self.runs.values():
            by_phase[r.phase.value] = by_phase.get(r.phase.value, 0) + 1
        return {
            "job_id": self.job_id,
            "name": self.spec.name,
            "workers": len(self.runs),
            "phases": by_phase,
            "done": all(r.phase in (WorkerPhase.DONE, WorkerPhase.FAILED) for r in self.runs.values()),
        }


class FleetDriver(Protocol):
    """What the orchestrator needs from the Manager. Real impl wraps
    ElasticAgentManager; tests inject a fake."""

    async def scale_out(self, count: int, name_prefix: str = "",
                        instance_type: str = "", region: str = "") -> list[str]:
        """Create ``count`` workers (named ``<name_prefix>-<i>``); return worker_ids."""
        ...

    async def hostname_of(self, worker_id: str) -> str:
        """Short hostname of a worker (for {{hostname}} / display). May be ''."""
        ...

    async def provision(self, worker_id: str, harness: Harness, spec: JobSpec) -> bool:
        """Run the bootstrap pipeline; return True on success."""
        ...

    async def login(self, worker_id: str, spec: JobSpec, config_dir: str) -> LoginOutcome:
        """Allocate an account and have the worker log it in locally
        (ACCOUNT_LOGIN). Credentials never transit the Manager."""
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


class BatchOrchestrator:
    def __init__(self, driver: FleetDriver, *, scale_in_on_complete: bool = False) -> None:
        self._driver = driver
        self._scale_in_on_complete = scale_in_on_complete
        self._jobs: dict[str, BatchJob] = {}
        self._worker_index: dict[str, str] = {}  # worker_id -> job_id

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

    async def launch(self, spec: JobSpec) -> BatchJob:
        """Scale out, then bring every worker up concurrently."""
        harness = resolve_harness(spec)
        job = BatchJob(job_id=f"job-{uuid.uuid4().hex[:8]}", spec=spec, harness=harness)
        self._jobs[job.job_id] = job

        n = max(1, spec.fanout.workers)
        worker_ids = await self._driver.scale_out(
            n, name_prefix=spec.fanout.name_prefix or spec.name,
            instance_type=spec.fanout.instance_type, region=spec.fanout.region)
        contexts = spec.worker_contexts()
        for wid, ctx in zip(worker_ids, contexts):
            ctx.hostname = await self._driver.hostname_of(wid)
            job.runs[wid] = WorkerRun(worker_id=wid, ctx=ctx)
            self._worker_index[wid] = job.job_id

        await asyncio.gather(
            *(self._bring_up(job, wid) for wid in job.runs),
            return_exceptions=True,
        )
        return job

    async def _bring_up(self, job: BatchJob, worker_id: str) -> None:
        run = job.runs[worker_id]
        spec = job.spec
        try:
            run.phase = WorkerPhase.BOOTSTRAPPING
            if not await self._driver.provision(worker_id, job.harness, spec):
                return self._fail(run, "bootstrap failed")

            if spec.account.mode != "none":
                run.phase = WorkerPhase.LOGGING_IN
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

            await self._dispatch(job, run, resume=False)
        except Exception as exc:  # pragma: no cover - defensive
            logger.exception("bring-up failed for %s", worker_id)
            self._fail(run, str(exc))

    def _slot_dirs(self, job: BatchJob) -> list[str]:
        """config_dirs to log accounts into — from the harness credential slots,
        falling back to a single default slot."""
        slots = [s.get("config_dir", "") for s in job.harness.get_credential_slots()]
        return slots or [job.spec.account.config_dir]

    async def _dispatch(self, job: BatchJob, run: WorkerRun, *, resume: bool) -> None:
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

    # -- lifecycle events (called by the Manager's message handlers) --------

    async def on_worker_exhausted(self, job_id: str, worker_id: str) -> bool:
        """Mode-B rotation (strategy a): swap account + restart with --resume.

        Returns True if a rotation was started, False if the policy declined
        (wrong strategy, max rotations reached, or re-login failed → FAILED).
        """
        job = self._jobs.get(job_id)
        if job is None or worker_id not in job.runs:
            return False
        run = job.runs[worker_id]
        spec = job.spec

        if spec.rotation.strategy != "on_exhaust_restart_resume":
            self._fail(run, "account exhausted (no rotation policy)")
            return False
        if run.rotations >= spec.rotation.max_rotations:
            self._fail(run, f"account exhausted (max {spec.rotation.max_rotations} rotations reached)")
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

    async def on_worker_exit(
        self, job_id: str, worker_id: str, exit_code: int, task_id: str | None = None,
    ) -> None:
        """Terminal process exit for a worker's run command."""
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
            try:
                await self._driver.collect(worker_id, job.spec, job_id)
            except Exception:
                logger.exception("collect failed for %s", worker_id)
        else:
            self._fail(run, f"run exited {exit_code}")
        await self._maybe_finish(job)

    async def _maybe_finish(self, job: BatchJob) -> None:
        if not all(r.phase in (WorkerPhase.DONE, WorkerPhase.FAILED) for r in job.runs.values()):
            return
        if self._scale_in_on_complete:
            await self._driver.scale_in(list(job.runs.keys()))

    # -- helpers -----------------------------------------------------------

    @staticmethod
    def _fail(run: WorkerRun, error: str) -> None:
        run.phase = WorkerPhase.FAILED
        run.error = error
        logger.warning("worker %s failed: %s", run.worker_id, error)
