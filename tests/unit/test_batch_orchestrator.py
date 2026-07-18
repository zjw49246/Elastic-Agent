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
        self.collected: list = []
        self._account_seq = 0

    async def scale_out(self, count, name_prefix="", instance_type="", region="",
                        disk_gb=0, spot=False):
        self.scale_name_prefix = name_prefix
        self.scale_instance_type = instance_type
        self.scale_disk_gb = disk_gb
        self.scale_spot = spot
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

    async def collect(self, worker_id, spec, job_id):
        self.collected.append((worker_id, job_id))

    async def scale_in(self, worker_ids):
        self.scaled_in.extend(worker_ids)


def _spec(**kw):
    kw.setdefault("name", "ai4sci")
    kw.setdefault("run", RunSpec(command="uv run bench --shard {{shard_index}} --host $(hostname -s)"))
    return JobSpec(**kw)


class TestSubmit:
    async def test_submit_returns_before_bringup_and_finishes_in_background(self):
        import asyncio
        gate = asyncio.Event()
        d = FakeDriver()
        orig = d.scale_out
        async def gated_scale(*a, **k):
            await gate.wait()
            return await orig(*a, **k)
        d.scale_out = gated_scale
        orch = BatchOrchestrator(d)

        job = await orch.submit(_spec(fanout={"workers": 2}))
        # Returned immediately with a job_id, registered, but bring-up gated → no runs yet.
        assert job.job_id and orch.get_job(job.job_id) is job
        assert job.runs == {}

        gate.set()  # release the background bring-up
        for _ in range(200):
            if len(job.runs) == 2:
                break
            await asyncio.sleep(0.01)
        assert len(job.runs) == 2  # bring-up completed off the request path

    async def test_submit_bad_harness_ref_raises_synchronously(self):
        orch = BatchOrchestrator(FakeDriver())
        with pytest.raises(Exception):
            await orch.submit(_spec(harness_ref="no.such.module:Nope"))


class TestLaunch:
    async def test_fans_out_to_all_workers(self):
        d = FakeDriver()
        orch = BatchOrchestrator(d)
        job = await orch.launch(_spec(fanout={"workers": 4}))
        assert len(job.runs) == 4
        assert all(r.phase == WorkerPhase.RUNNING for r in job.runs.values())
        assert len(d.dispatched) == 4

    async def test_scale_out_named_by_prefix_or_job_name(self):
        d = FakeDriver()
        orch = BatchOrchestrator(d)
        await orch.launch(_spec(fanout={"workers": 1, "name_prefix": "myfleet"}))
        assert d.scale_name_prefix == "myfleet"
        d2 = FakeDriver()
        await BatchOrchestrator(d2).launch(_spec(name="ai4sci", fanout={"workers": 1}))
        assert d2.scale_name_prefix == "ai4sci"   # falls back to job name

    async def test_fanout_disk_and_spot_flow_to_scale_out(self):
        d = FakeDriver()
        await BatchOrchestrator(d).launch(
            _spec(fanout={"workers": 1, "disk_gb": 80, "spot": True}))
        assert d.scale_disk_gb == 80
        assert d.scale_spot is True

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

    async def test_stale_exit_after_rotation_ignored_by_task_id(self):
        # Race: after rotation re-dispatches (new task_id, phase RUNNING again),
        # the interrupted run's stale exit arrives with the OLD task_id — it must
        # be ignored so the fresh run isn't failed.
        d = FakeDriver()
        orch = BatchOrchestrator(d)
        spec = _spec(
            fanout={"workers": 1},
            rotation={"strategy": "on_exhaust_restart_resume", "resume_args": "-r", "max_rotations": 3},
        )
        job = await orch.launch(spec)
        wid = next(iter(job.runs))
        old_task_id = job.runs[wid].task_id

        await orch.on_worker_exhausted(job.job_id, wid)  # → new task_id, RUNNING
        new_task_id = job.runs[wid].task_id
        assert new_task_id != old_task_id

        # stale exit from the interrupted run
        await orch.on_worker_exit(job.job_id, wid, 130, task_id=old_task_id)
        assert job.runs[wid].phase == WorkerPhase.RUNNING  # unaffected

        # genuine completion of the fresh run
        await orch.on_worker_exit(job.job_id, wid, 0, task_id=new_task_id)
        assert job.runs[wid].phase == WorkerPhase.DONE

    async def test_final_collect_on_failed_run(self):
        # A non-zero exit (e.g. quota-out) still pulls back whatever completed —
        # partial results must reach the Manager → S3, not be discarded.
        d = FakeDriver()
        orch = BatchOrchestrator(d)
        job = await orch.launch(_spec(fanout={"workers": 1}, collect={"paths": ["results"]}))
        wid = next(iter(job.runs))
        await orch.on_worker_exit(job.job_id, wid, 1, task_id=job.runs[wid].task_id)
        assert job.runs[wid].phase == WorkerPhase.FAILED
        assert (wid, job.job_id) in d.collected

    async def test_periodic_collect_streams_partial_results(self):
        import asyncio
        d = FakeDriver()
        orch = BatchOrchestrator(d)
        job = await orch.launch(_spec(
            fanout={"workers": 1},
            collect={"paths": ["results"], "interval_seconds": 1}))
        wid = next(iter(job.runs))
        await asyncio.sleep(1.3)             # one interval elapses mid-run
        assert len(d.collected) >= 1         # collected while still RUNNING
        orch._stop_periodic_collect(wid)     # cleanup the background loop

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


class TestMultiAccountPerWorker:
    def _spec3(self):
        return _spec(
            fanout={"workers": 1},
            account={"per_worker": 3, "config_dir": "/root/.claude"},
            rotation={"strategy": "on_exhaust_restart_resume", "resume_args": "-r", "max_rotations": 10},
        )

    async def test_logs_in_all_slots(self):
        d = FakeDriver()
        orch = BatchOrchestrator(d)
        job = await orch.launch(self._spec3())
        run = next(iter(job.runs.values()))
        assert len(d.login_calls) == 3
        assert run.config_dirs == ["/root/.claude-slot-0", "/root/.claude-slot-1", "/root/.claude-slot-2"]
        assert len(run.account_ids) == 3
        assert run.active_slot == 0
        # each slot logged into its own dir
        assert sorted(c[1] for c in d.login_calls) == run.config_dirs

    async def test_rotation_uses_prelogged_slots_before_relogin(self):
        d = FakeDriver()
        orch = BatchOrchestrator(d)
        job = await orch.launch(self._spec3())
        wid = next(iter(job.runs))
        run = job.runs[wid]

        await orch.on_worker_exhausted(job.job_id, wid)   # slot 0→1, no new login
        assert run.active_slot == 1 and len(d.login_calls) == 3
        await orch.on_worker_exhausted(job.job_id, wid)   # slot 1→2, no new login
        assert run.active_slot == 2 and len(d.login_calls) == 3
        # local pool spent → fresh login into a new dir
        await orch.on_worker_exhausted(job.job_id, wid)
        assert len(d.login_calls) == 4
        assert run.active_slot == 3
        assert run.config_dirs[3].endswith("-rot-3")

    async def test_active_config_dir_rendered_into_command(self):
        d = FakeDriver()
        orch = BatchOrchestrator(d)
        job = await orch.launch(self._spec3())
        wid = next(iter(job.runs))
        assert d.dispatched[-1]["env"]["CLAUDE_CONFIG_DIR"] == "/root/.claude-slot-0"
        await orch.on_worker_exhausted(job.job_id, wid)
        assert d.dispatched[-1]["env"]["CLAUDE_CONFIG_DIR"] == "/root/.claude-slot-1"


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


class _RecordingAllocator:
    """Minimal allocator double: records which workers were released."""

    def __init__(self):
        self.released: list[str] = []

    async def release_worker(self, worker_id: str) -> None:
        self.released.append(worker_id)


class TestAccountRelease:
    """A finished job (DONE or FAILED, via any terminal path) must release its
    workers' accounts back to the allocator so a later job can reuse them —
    regression guard for single-account starvation."""

    async def test_release_on_done(self):
        d = FakeDriver()
        orch = BatchOrchestrator(d)
        alloc = _RecordingAllocator()
        orch._allocator = alloc
        job = await orch.launch(_spec(fanout={"workers": 1}))
        wid = next(iter(job.runs))
        await orch.on_worker_exit(job.job_id, wid, 0)  # DONE
        assert alloc.released == [wid]

    async def test_release_on_failed_run_exit(self):
        d = FakeDriver()
        orch = BatchOrchestrator(d)
        alloc = _RecordingAllocator()
        orch._allocator = alloc
        job = await orch.launch(_spec(fanout={"workers": 1}))
        wid = next(iter(job.runs))
        await orch.on_worker_exit(job.job_id, wid, 1)  # FAILED
        assert alloc.released == [wid]

    async def test_release_on_bringup_login_failure(self):
        d = FakeDriver()
        d.login_ok = False
        orch = BatchOrchestrator(d)
        alloc = _RecordingAllocator()
        orch._allocator = alloc
        job = await orch.launch(_spec(fanout={"workers": 2}))
        assert all(r.phase == WorkerPhase.FAILED for r in job.runs.values())
        assert set(alloc.released) == set(job.runs.keys())

    async def test_release_on_rotation_exhausted(self):
        d = FakeDriver()
        orch = BatchOrchestrator(d)
        alloc = _RecordingAllocator()
        orch._allocator = alloc
        spec = _spec(
            fanout={"workers": 1},
            rotation={"strategy": "on_exhaust_restart_resume", "max_rotations": 1, "resume_args": "--resume x"},
        )
        job = await orch.launch(spec)
        wid = next(iter(job.runs))
        assert await orch.on_worker_exhausted(job.job_id, wid) is True   # rotation 1
        assert await orch.on_worker_exhausted(job.job_id, wid) is False  # capped → FAILED
        assert job.runs[wid].phase == WorkerPhase.FAILED
        assert wid in alloc.released

    async def test_no_allocator_is_noop(self):
        # Orchestrator without an _allocator handle (e.g. tests/custom wiring)
        # must not blow up on terminal.
        d = FakeDriver()
        orch = BatchOrchestrator(d)
        job = await orch.launch(_spec(fanout={"workers": 1}))
        wid = next(iter(job.runs))
        await orch.on_worker_exit(job.job_id, wid, 0)
        assert job.runs[wid].phase == WorkerPhase.DONE
