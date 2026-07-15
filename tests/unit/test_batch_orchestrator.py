"""Tests for BatchOrchestrator — fan-out lifecycle + Mode-B rotation."""

from __future__ import annotations

import pytest

from elastic_agent.core.batch_orchestrator import (
    BatchOrchestrator,
    LoginOutcome,
    WorkerPhase,
)
from elastic_agent.core.job_spec import JobSpec, RunSpec

pytestmark = pytest.mark.asyncio


class FakeDriver:
    """Records calls; each step can be told to fail."""

    def __init__(self, workers=4):
        self._workers = workers
        self.provision_ok = True
        self.login_ok = True
        self.login_calls: list[tuple[str, str]] = []
        self.dispatched: list[dict] = []
        self.scaled_in: list[str] = []
        self._account_seq = 0

    async def scale_out(self, count):
        return [f"w{i}" for i in range(count)]

    async def hostname_of(self, worker_id):
        return f"host-{worker_id}"

    async def provision(self, worker_id, harness, spec):
        return self.provision_ok

    async def login(self, worker_id, spec, config_dir):
        self.login_calls.append((worker_id, config_dir))
        if not self.login_ok:
            return LoginOutcome(success=False, error="login boom")
        self._account_seq += 1
        return LoginOutcome(success=True, account_id=f"acc-{self._account_seq}",
                            account_email=f"a{self._account_seq}@x.com")

    async def run_command(self, worker_id, task_id, command, cwd, env, timeout,
                          job_id, watch_exhaustion):
        self.dispatched.append({
            "worker_id": worker_id, "task_id": task_id, "command": command,
            "cwd": cwd, "env": env, "timeout": timeout,
            "job_id": job_id, "watch_exhaustion": watch_exhaustion,
        })

    async def scale_in(self, worker_ids):
        self.scaled_in.extend(worker_ids)


def _spec(**kw):
    kw.setdefault("name", "ai4sci")
    kw.setdefault("run", RunSpec(command="uv run bench --shard {{shard_index}} --host $(hostname -s)"))
    return JobSpec(**kw)


class TestLaunch:
    async def test_fans_out_to_all_workers(self):
        d = FakeDriver()
        orch = BatchOrchestrator(d)
        job = await orch.launch(_spec(fanout={"workers": 4}))
        assert len(job.runs) == 4
        assert all(r.phase == WorkerPhase.RUNNING for r in job.runs.values())
        assert len(d.dispatched) == 4

    async def test_shard_index_rendered_per_worker(self):
        d = FakeDriver()
        orch = BatchOrchestrator(d)
        await orch.launch(_spec(fanout={"workers": 3}))
        rendered = sorted(disp["command"][2] for disp in d.dispatched)
        assert "--shard 0" in rendered[0]
        assert "--shard 1" in rendered[1]
        assert "--shard 2" in rendered[2]
        # $(hostname -s) preserved (worker shell evaluates it)
        assert all("$(hostname -s)" in disp["command"][2] for disp in d.dispatched)

    async def test_login_happens_before_dispatch(self):
        d = FakeDriver()
        orch = BatchOrchestrator(d)
        job = await orch.launch(_spec(fanout={"workers": 2}))
        assert len(d.login_calls) == 2
        for r in job.runs.values():
            assert r.account_id  # each worker got an account
            assert r.ctx.account_email

    async def test_account_none_skips_login(self):
        d = FakeDriver()
        orch = BatchOrchestrator(d)
        await orch.launch(_spec(fanout={"workers": 2}, account={"mode": "none"}))
        assert d.login_calls == []
        assert len(d.dispatched) == 2

    async def test_watch_exhaustion_flag_reflects_rotation_policy(self):
        d = FakeDriver()
        orch = BatchOrchestrator(d)
        await orch.launch(_spec(fanout={"workers": 1}))  # rotation none
        assert d.dispatched[0]["watch_exhaustion"] is False
        assert d.dispatched[0]["job_id"]

        d2 = FakeDriver()
        orch2 = BatchOrchestrator(d2)
        await orch2.launch(_spec(
            fanout={"workers": 1},
            rotation={"strategy": "on_exhaust_restart_resume", "resume_args": "-r"},
        ))
        assert d2.dispatched[0]["watch_exhaustion"] is True

    async def test_bootstrap_failure_marks_failed_no_dispatch(self):
        d = FakeDriver()
        d.provision_ok = False
        orch = BatchOrchestrator(d)
        job = await orch.launch(_spec(fanout={"workers": 2}))
        assert all(r.phase == WorkerPhase.FAILED for r in job.runs.values())
        assert d.dispatched == []

    async def test_login_failure_marks_failed(self):
        d = FakeDriver()
        d.login_ok = False
        orch = BatchOrchestrator(d)
        job = await orch.launch(_spec(fanout={"workers": 2}))
        assert all(r.phase == WorkerPhase.FAILED for r in job.runs.values())
        assert d.dispatched == []


class TestRotation:
    async def test_exhaustion_rotates_and_resumes(self):
        d = FakeDriver()
        orch = BatchOrchestrator(d)
        spec = _spec(
            fanout={"workers": 1},
            rotation={"strategy": "on_exhaust_restart_resume", "resume_args": "--resume out", "max_rotations": 3},
        )
        job = await orch.launch(spec)
        wid = next(iter(job.runs))
        d.dispatched.clear()

        started = await orch.on_worker_exhausted(job.job_id, wid)
        assert started is True
        run = job.runs[wid]
        assert run.rotations == 1
        assert run.phase == WorkerPhase.RUNNING
        # restart command carries the resume args
        assert "--resume out" in d.dispatched[-1]["command"][2]
        # a fresh account was logged in
        assert len(d.login_calls) == 2

    async def test_no_rotation_policy_fails(self):
        d = FakeDriver()
        orch = BatchOrchestrator(d)
        job = await orch.launch(_spec(fanout={"workers": 1}))  # rotation.strategy == none
        wid = next(iter(job.runs))
        started = await orch.on_worker_exhausted(job.job_id, wid)
        assert started is False
        assert job.runs[wid].phase == WorkerPhase.FAILED

    async def test_max_rotations_enforced(self):
        d = FakeDriver()
        orch = BatchOrchestrator(d)
        spec = _spec(
            fanout={"workers": 1},
            rotation={"strategy": "on_exhaust_restart_resume", "max_rotations": 2, "resume_args": "--resume x"},
        )
        job = await orch.launch(spec)
        wid = next(iter(job.runs))
        assert await orch.on_worker_exhausted(job.job_id, wid) is True   # 1
        assert await orch.on_worker_exhausted(job.job_id, wid) is True   # 2
        assert await orch.on_worker_exhausted(job.job_id, wid) is False  # capped
        assert job.runs[wid].phase == WorkerPhase.FAILED

    async def test_interrupt_exit_during_rotation_ignored(self):
        # The interrupted run's non-zero exit (from SIGINT) must not mark the
        # worker FAILED while a rotation is mid-flight.
        d = FakeDriver()
        orch = BatchOrchestrator(d)
        spec = _spec(
            fanout={"workers": 1},
            rotation={"strategy": "on_exhaust_restart_resume", "resume_args": "-r", "max_rotations": 3},
        )
        job = await orch.launch(spec)
        wid = next(iter(job.runs))
        # Simulate: exit arrives while ROTATING (before restart completes).
        job.runs[wid].phase = WorkerPhase.ROTATING
        await orch.on_worker_exit(job.job_id, wid, 130)  # 128+SIGINT
        assert job.runs[wid].phase == WorkerPhase.ROTATING  # unchanged

    async def test_rotation_login_failure_fails(self):
        d = FakeDriver()
        orch = BatchOrchestrator(d)
        spec = _spec(
            fanout={"workers": 1},
            rotation={"strategy": "on_exhaust_restart_resume", "resume_args": "-r", "max_rotations": 3},
        )
        job = await orch.launch(spec)
        wid = next(iter(job.runs))
        d.login_ok = False
        assert await orch.on_worker_exhausted(job.job_id, wid) is False
        assert job.runs[wid].phase == WorkerPhase.FAILED


class TestCompletion:
    async def test_exit_zero_marks_done(self):
        d = FakeDriver()
        orch = BatchOrchestrator(d)
        job = await orch.launch(_spec(fanout={"workers": 1}))
        wid = next(iter(job.runs))
        await orch.on_worker_exit(job.job_id, wid, 0)
        assert job.runs[wid].phase == WorkerPhase.DONE

    async def test_nonzero_exit_marks_failed(self):
        d = FakeDriver()
        orch = BatchOrchestrator(d)
        job = await orch.launch(_spec(fanout={"workers": 1}))
        wid = next(iter(job.runs))
        await orch.on_worker_exit(job.job_id, wid, 1)
        assert job.runs[wid].phase == WorkerPhase.FAILED

    async def test_scale_in_on_complete(self):
        d = FakeDriver()
        orch = BatchOrchestrator(d, scale_in_on_complete=True)
        job = await orch.launch(_spec(fanout={"workers": 2}))
        for wid in list(job.runs):
            await orch.on_worker_exit(job.job_id, wid, 0)
        assert set(d.scaled_in) == set(job.runs.keys())

    async def test_summary_shape(self):
        d = FakeDriver()
        orch = BatchOrchestrator(d)
        job = await orch.launch(_spec(fanout={"workers": 2}))
        s = job.summary()
        assert s["workers"] == 2
        assert s["name"] == "ai4sci"
        assert s["done"] is False
