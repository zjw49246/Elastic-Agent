"""Tests for BatchOrchestrator — fan-out lifecycle + Mode-B rotation."""

from __future__ import annotations

import asyncio

import pytest

from elastic_agent.core.batch_orchestrator import (
    BatchOrchestrator,
    LoginOutcome,
    WorkerAssignment,
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
        self._bound_worker_seq = 0
        self.events: list[tuple] = []
        self.bound_requested: list[str] = []
        self.bound_released: list[tuple[str, str | None]] = []
        self.bound_reserve_fail_slot: int | None = None
        self.bound_attach_ok = True
        self.scale_tags: list[dict[str, str]] = []
        self.bound_release_failures = 0
        self.bound_release_attempts = 0
        self.collect_failures = 0
        self.capacity_error: str | None = None
        self.capacity_holds: set[str] = set()

    async def acquire_capacity(self, count):
        self.events.append(("capacity_acquire", count))
        if self.capacity_error:
            raise RuntimeError(self.capacity_error)
        reservation_id = f"capacity-{len(self.capacity_holds) + 1}"
        self.capacity_holds.add(reservation_id)
        return reservation_id

    async def release_capacity(self, reservation_id):
        self.events.append(("capacity_release", reservation_id))
        self.capacity_holds.discard(reservation_id)

    async def scale_out(self, count, name_prefix="", instance_type="", region="",
                        disk_gb=0, spot=False, tags=None):
        self.scale_name_prefix = name_prefix
        self.scale_instance_type = instance_type
        self.scale_disk_gb = disk_gb
        self.scale_spot = spot
        self.events.append(("scale", count))
        if tags is not None:
            self.scale_tags.append(tags)
            start = self._bound_worker_seq
            self._bound_worker_seq += count
            return [f"bw{i}" for i in range(start, start + count)]
        return [f"w{i}" for i in range(count)]

    async def reserve_bound(self, job_id, slot, spec, account_id=""):
        self.events.append(("reserve", slot, account_id))
        self.bound_requested.append(account_id)
        if self.bound_reserve_fail_slot == slot:
            raise RuntimeError("reserve boom")
        aid = account_id or f"auto-{slot}"
        return WorkerAssignment(
            slot=slot,
            job_id=job_id,
            account_id=aid,
            account_email=f"{aid}@x.com",
            claim_id=f"claim-{slot}",
            lease_id=f"lease-{slot}",
            eip_allocation_id=f"eipalloc-{slot}",
            eip=f"203.0.113.{slot + 1}",
            region="ap-northeast-1",
        )

    async def attach_bound(self, worker_id, assignment):
        self.events.append(("attach", worker_id, assignment.lease_id))
        if not self.bound_attach_ok:
            raise RuntimeError("attach boom")
        return assignment

    async def release_bound(self, assignment, worker_id):
        self.bound_release_attempts += 1
        if self.bound_release_failures:
            self.bound_release_failures -= 1
            raise RuntimeError("release transient")
        self.events.append(("release", worker_id, assignment.lease_id))
        self.bound_released.append((assignment.lease_id, worker_id))

    async def hostname_of(self, worker_id):
        return f"host-{worker_id}"

    async def provision(self, worker_id, harness, spec):
        self.events.append(("provision", worker_id))
        return self.provision_ok

    async def login(self, worker_id, spec, config_dir, *, account_id="", claim_id=""):
        self.login_calls.append((worker_id, config_dir))
        self.events.append(("login", worker_id, account_id, claim_id))
        if not self.login_ok:
            return LoginOutcome(success=False, error="login boom")
        if account_id:
            return LoginOutcome(
                success=True,
                account_id=account_id,
                account_email=f"{account_id}@x.com",
            )
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
        self.events.append(("collect", worker_id))
        if self.collect_failures:
            self.collect_failures -= 1
            raise RuntimeError("collect transient")
        self.collected.append((worker_id, job_id))

    async def scale_in(self, worker_ids):
        self.scaled_in.extend(worker_ids)


def _spec(**kw):
    kw.setdefault("name", "ai4sci")
    kw.setdefault("run", RunSpec(command="uv run bench --shard {{shard_index}} --host $(hostname -s)"))
    return JobSpec(**kw)


class TestSubmit:
    async def test_prepare_uses_full_uuid_hex_job_id(self):
        orch = BatchOrchestrator(FakeDriver())

        first = orch.prepare(_spec())
        second = orch.prepare(_spec())

        assert len(first.job_id) == len("job-") + 32
        assert first.job_id.startswith("job-")
        assert set(first.job_id.removeprefix("job-")) <= set("0123456789abcdef")
        assert first.job_id != second.job_id

    async def test_prepare_does_not_register_or_start_until_submitted(self):
        d = FakeDriver()
        orch = BatchOrchestrator(d)
        job = orch.prepare(_spec(fanout={"workers": 1}))

        assert orch.get_job(job.job_id) is None
        assert d.events == []

        await orch.submit_prepared(job)
        assert orch.get_job(job.job_id) is job
        for _ in range(100):
            if d.dispatched:
                break
            await asyncio.sleep(0.01)
        assert d.dispatched

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

    async def test_direct_launch_is_tracked_and_shutdown_waits_for_it(self):
        d = FakeDriver()
        entered = asyncio.Event()
        unblock = asyncio.Event()
        original_scale_out = d.scale_out

        async def gated_scale_out(*args, **kwargs):
            entered.set()
            await unblock.wait()
            return await original_scale_out(*args, **kwargs)

        d.scale_out = gated_scale_out
        orch = BatchOrchestrator(d)
        launch_task = asyncio.create_task(
            orch.launch(_spec(fanout={"workers": 1}))
        )
        await entered.wait()

        shutdown_task = asyncio.create_task(orch.shutdown(timeout=0.2))
        await asyncio.sleep(0)
        assert shutdown_task.done() is False
        assert orch._launch_tasks

        unblock.set()
        job = await launch_task
        await shutdown_task
        assert all(run.error == "manager shutting down" for run in job.runs.values())
        assert orch._launch_tasks == set()

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


class TestEipBoundLaunch:
    def _bound_spec(self, workers=2, ids=None, **extra):
        account = {"binding": "eip", "ids": ids or []}
        account.update(extra.pop("account", {}))
        return _spec(
            fanout={"workers": workers, "region": "ap-northeast-1"},
            account=account,
            **extra,
        )

    async def test_reserves_every_assignment_before_creating_instances(self):
        d = FakeDriver()
        orch = BatchOrchestrator(d)
        job = await orch.launch(self._bound_spec(ids=["acct-z", "acct-a"]))

        assert d.bound_requested == ["acct-z", "acct-a"]
        first_scale = next(i for i, event in enumerate(d.events) if event[0] == "scale")
        assert [e[0] for e in d.events[:first_scale]] == [
            "capacity_acquire", "reserve", "reserve", "capacity_release",
        ]
        assert len(job.runs) == 2
        assert {r.account_id for r in job.runs.values()} == {"acct-z", "acct-a"}

    async def test_reservations_start_concurrently_and_scale_waits_for_barrier(self):
        d = FakeDriver()
        orch = BatchOrchestrator(d)
        original_reserve = d.reserve_bound
        both_entered = asyncio.Event()
        release_barrier = asyncio.Event()
        entered: set[int] = set()

        async def barrier_reserve(job_id, slot, spec, account_id=""):
            entered.add(slot)
            if entered == {0, 1}:
                both_entered.set()
            await release_barrier.wait()
            return await original_reserve(
                job_id, slot, spec, account_id=account_id
            )

        d.reserve_bound = barrier_reserve
        launch = asyncio.create_task(
            orch.launch(self._bound_spec(ids=["acct-1", "acct-2"]))
        )

        await asyncio.wait_for(both_entered.wait(), timeout=1)
        assert launch.done() is False
        assert d.capacity_holds == {"capacity-1"}
        assert not any(event[0] == "capacity_release" for event in d.events)
        assert not any(event[0] == "scale" for event in d.events)

        release_barrier.set()
        job = await launch
        assert len(job.runs) == 2
        release_index = next(
            i for i, event in enumerate(d.events)
            if event[0] == "capacity_release"
        )
        first_scale = next(
            i for i, event in enumerate(d.events) if event[0] == "scale"
        )
        assert release_index < first_scale

    async def test_fast_failure_waits_for_delayed_success_then_rolls_it_back(self):
        d = FakeDriver()
        orch = BatchOrchestrator(d)
        original_reserve = d.reserve_bound
        d.bound_reserve_fail_slot = 0
        failure_settled = asyncio.Event()
        allow_delayed_success = asyncio.Event()
        delayed_success_settled = asyncio.Event()

        async def staggered_reserve(job_id, slot, spec, account_id=""):
            if slot == 1:
                await allow_delayed_success.wait()
            try:
                result = await original_reserve(
                    job_id, slot, spec, account_id=account_id
                )
            except RuntimeError:
                failure_settled.set()
                raise
            if slot == 1:
                delayed_success_settled.set()
            return result

        d.reserve_bound = staggered_reserve
        launch = asyncio.create_task(
            orch.launch(self._bound_spec(ids=["acct-1", "acct-2"]))
        )

        await asyncio.wait_for(failure_settled.wait(), timeout=1)
        await asyncio.sleep(0)
        assert launch.done() is False
        assert d.capacity_holds == {"capacity-1"}
        assert d.bound_released == []
        assert not any(event[0] == "scale" for event in d.events)

        allow_delayed_success.set()
        job = await launch
        assert delayed_success_settled.is_set()
        assert "reservation failed" in job.error
        assert d.bound_released == [("lease-1", None)]
        assert d.capacity_holds == set()
        assert not any(event[0] == "scale" for event in d.events)

    async def test_repeated_cancellation_settles_and_rolls_back_reservations(self):
        d = FakeDriver()
        orch = BatchOrchestrator(d)
        job = orch.prepare(self._bound_spec(ids=["acct-1", "acct-2"]))
        original_reserve = d.reserve_bound
        first_settled = asyncio.Event()
        delayed_entered = asyncio.Event()
        allow_delayed_success = asyncio.Event()

        async def staggered_reserve(job_id, slot, spec, account_id=""):
            if slot == 1:
                delayed_entered.set()
                await allow_delayed_success.wait()
            result = await original_reserve(
                job_id, slot, spec, account_id=account_id
            )
            if slot == 0:
                first_settled.set()
            return result

        d.reserve_bound = staggered_reserve
        bring_up = asyncio.create_task(orch._bring_up_bound_all(job))
        await asyncio.wait_for(first_settled.wait(), timeout=1)
        await asyncio.wait_for(delayed_entered.wait(), timeout=1)

        bring_up.cancel()
        await asyncio.sleep(0)
        bring_up.cancel()
        await asyncio.sleep(0)
        assert bring_up.done() is False
        assert job.pending_cleanup == {}
        assert d.capacity_holds == {"capacity-1"}

        allow_delayed_success.set()
        with pytest.raises(asyncio.CancelledError):
            await bring_up

        assert job.pending_cleanup == {}
        assert d.capacity_holds == set()
        assert set(d.bound_released) == {
            ("lease-0", None),
            ("lease-1", None),
        }
        assert not any(event[0] == "scale" for event in d.events)

    async def test_cancellation_during_capacity_release_rolls_back_before_raise(self):
        d = FakeDriver()
        orch = BatchOrchestrator(d)
        job = orch.prepare(self._bound_spec(ids=["acct-1", "acct-2"]))
        real_release_capacity = d.release_capacity
        release_entered = asyncio.Event()
        allow_release = asyncio.Event()

        async def gated_release_capacity(reservation_id):
            release_entered.set()
            await allow_release.wait()
            await real_release_capacity(reservation_id)

        d.release_capacity = gated_release_capacity
        bring_up = asyncio.create_task(orch._bring_up_bound_all(job))
        await asyncio.wait_for(release_entered.wait(), timeout=1)

        bring_up.cancel()
        await asyncio.sleep(0)
        bring_up.cancel()
        await asyncio.sleep(0)
        assert bring_up.done() is False
        assert d.capacity_holds == {"capacity-1"}
        assert d.bound_released == []

        allow_release.set()
        with pytest.raises(asyncio.CancelledError):
            await bring_up

        assert d.capacity_holds == set()
        assert job.pending_cleanup == {}
        assert set(d.bound_released) == {
            ("lease-0", None),
            ("lease-1", None),
        }
        assert not any(event[0] == "scale" for event in d.events)

    async def test_capacity_release_error_retries_and_rolls_back(self):
        d = FakeDriver()
        orch = BatchOrchestrator(d)
        real_release_capacity = d.release_capacity
        attempts = 0

        async def flaky_release_capacity(reservation_id):
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise RuntimeError("release-capacity boom")
            await real_release_capacity(reservation_id)

        d.release_capacity = flaky_release_capacity
        job = await orch.launch(self._bound_spec(ids=["acct-1", "acct-2"]))

        assert "capacity release failed" in job.error
        assert attempts == 2
        assert d.capacity_holds == set()
        assert job.pending_cleanup == {}
        assert set(d.bound_released) == {
            ("lease-0", None),
            ("lease-1", None),
        }
        assert not any(event[0] == "scale" for event in d.events)

    async def test_repeated_cancellation_during_rollback_still_releases(self):
        d = FakeDriver()
        d.bound_reserve_fail_slot = 1
        orch = BatchOrchestrator(d)
        job = orch.prepare(self._bound_spec(ids=["acct-1", "acct-2"]))
        real_release_bound = d.release_bound
        cleanup_entered = asyncio.Event()
        allow_cleanup = asyncio.Event()

        async def gated_release_bound(assignment, worker_id):
            cleanup_entered.set()
            await allow_cleanup.wait()
            await real_release_bound(assignment, worker_id)

        d.release_bound = gated_release_bound
        bring_up = asyncio.create_task(orch._bring_up_bound_all(job))
        await asyncio.wait_for(cleanup_entered.wait(), timeout=1)

        bring_up.cancel()
        await asyncio.sleep(0)
        bring_up.cancel()
        await asyncio.sleep(0)
        assert bring_up.done() is False
        assert d.capacity_holds == {"capacity-1"}

        allow_cleanup.set()
        with pytest.raises(asyncio.CancelledError):
            await bring_up

        assert d.capacity_holds == set()
        assert job.pending_cleanup == {}
        assert d.bound_released == [("lease-0", None)]
        assert not any(event[0] == "scale" for event in d.events)

    async def test_capacity_failure_precedes_every_account_and_eip_reservation(self):
        d = FakeDriver()
        d.capacity_error = "configured maximum is 5"

        job = await BatchOrchestrator(d).launch(self._bound_spec(workers=20))

        assert "configured maximum is 5" in job.error
        assert d.bound_requested == []
        assert not any(event[0] == "reserve" for event in d.events)
        assert not any(event[0] == "scale" for event in d.events)
        assert d.capacity_holds == set()

    async def test_auto_selection_passes_empty_ids_by_shard(self):
        d = FakeDriver()
        await BatchOrchestrator(d).launch(self._bound_spec(workers=2))
        assert d.bound_requested == ["", ""]

    async def test_attaches_eip_before_provision_and_records_lease(self):
        d = FakeDriver()
        job = await BatchOrchestrator(d).launch(
            self._bound_spec(workers=1, ids=["acct-1"])
        )
        run = next(iter(job.runs.values()))
        attach_i = next(i for i, e in enumerate(d.events) if e[0] == "attach")
        provision_i = next(i for i, e in enumerate(d.events) if e[0] == "provision")
        assert attach_i < provision_i
        assert run.lease_id == "lease-0"
        assert run.claim_id == "claim-0"
        assert run.eip == "203.0.113.1"
        assert d.scale_tags == [{
            "ElasticAgentJob": job.job_id,
            "ElasticAgentAccount": "acct-1",
            "ElasticAgentLease": "lease-0",
        }]
        # Bound login uses the account already claimed before scale-out.
        login = next(e for e in d.events if e[0] == "login")
        assert login[2:] == ("acct-1", "claim-0")

    async def test_terminal_collect_precedes_bound_release_and_is_idempotent(self):
        d = FakeDriver()
        orch = BatchOrchestrator(d)
        job = await orch.launch(self._bound_spec(workers=1, ids=["acct-1"]))
        run = next(iter(job.runs.values()))
        await orch.on_worker_exit(job.job_id, run.worker_id, 0, task_id=run.task_id)
        await orch.on_worker_exit(job.job_id, run.worker_id, 0, task_id=run.task_id)

        final_events = [e[0] for e in d.events if e[0] in ("collect", "release")]
        assert final_events[-2:] == ["collect", "release"]
        assert d.bound_released == [("lease-0", run.worker_id)]
        assert run.cleaned_up is True

    async def test_bootstrap_failure_skips_collect_and_releases_immediately(self):
        d = FakeDriver()
        d.provision_ok = False
        job = await BatchOrchestrator(d).launch(
            self._bound_spec(workers=1, ids=["acct-1"])
        )
        run = next(iter(job.runs.values()))
        assert run.phase == WorkerPhase.FAILED
        assert d.collected == []
        assert d.bound_released == [("lease-0", run.worker_id)]

    async def test_reservation_failure_rolls_back_and_creates_no_instances(self):
        d = FakeDriver()
        d.bound_reserve_fail_slot = 1
        job = await BatchOrchestrator(d).launch(
            self._bound_spec(ids=["acct-1", "acct-2"])
        )
        assert "reservation failed" in job.error
        assert not any(e[0] == "scale" for e in d.events)
        assert d.bound_released == [("lease-0", None)]

    async def test_unattached_release_failure_stays_pending_and_retries(self):
        d = FakeDriver()
        d.bound_reserve_fail_slot = 1
        d.bound_release_failures = 1
        orch = BatchOrchestrator(d, cleanup_retry_seconds=0.01)

        job = await orch.launch(self._bound_spec(ids=["acct-1", "acct-2"]))

        assert job.summary()["cleanup_pending"] == 1
        assert job.summary()["done"] is False
        assert "lease-0" in job.cleanup_errors
        for _ in range(50):
            if not job.pending_cleanup:
                break
            await asyncio.sleep(0.01)
        assert job.pending_cleanup == {}
        assert job.cleanup_errors == {}
        assert job.summary()["done"] is True
        assert d.bound_release_attempts == 2

    async def test_attach_failure_force_release_path_skips_bootstrap(self):
        d = FakeDriver()
        d.bound_attach_ok = False
        job = await BatchOrchestrator(d).launch(
            self._bound_spec(workers=1, ids=["acct-1"])
        )
        run = next(iter(job.runs.values()))
        assert run.phase == WorkerPhase.FAILED
        assert not any(e[0] == "provision" for e in d.events)
        assert d.bound_released == [("lease-0", run.worker_id)]


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

    async def test_final_collect_retries_then_cleans_bound_instance(self):
        d = FakeDriver()
        d.collect_failures = 2
        orch = BatchOrchestrator(d, final_collect_attempts=2)
        spec = TestEipBoundLaunch()._bound_spec(workers=1, ids=["acct-1"])
        job = await orch.launch(spec)
        run = next(iter(job.runs.values()))

        await orch.on_worker_exit(job.job_id, run.worker_id, 0, task_id=run.task_id)

        assert run.phase == WorkerPhase.FAILED
        assert run.final_collected is True
        assert run.collection_error == "collect transient"
        assert "final result collection failed" in job.error
        assert run.cleaned_up is True
        assert d.bound_released == [("lease-0", run.worker_id)]

    async def test_final_collect_total_timeout_still_releases_bound_instance(self):
        d = FakeDriver()
        never = asyncio.Event()

        async def hung_collect(*_args, **_kwargs):
            await never.wait()

        d.collect = hung_collect
        orch = BatchOrchestrator(
            d,
            final_collect_attempts=3,
            final_collect_timeout=0.05,
        )
        spec = TestEipBoundLaunch()._bound_spec(workers=1, ids=["acct-1"])
        job = await orch.launch(spec)
        run = next(iter(job.runs.values()))

        await asyncio.wait_for(
            orch.on_worker_exit(job.job_id, run.worker_id, 0, task_id=run.task_id),
            timeout=0.5,
        )

        assert run.final_collected is True
        assert "timed out" in run.collection_error
        assert run.cleaned_up is True
        assert d.bound_released == [(run.lease_id, run.worker_id)]

    async def test_finalization_awaits_slow_periodic_cancel_before_release(self):
        d = FakeDriver()
        orch = BatchOrchestrator(d)
        spec = TestEipBoundLaunch()._bound_spec(workers=1, ids=["acct-1"])
        job = await orch.launch(spec)
        run = next(iter(job.runs.values()))
        started = asyncio.Event()
        cancellation_finished = asyncio.Event()

        async def slow_periodic_collect():
            started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                await asyncio.sleep(0.05)
                cancellation_finished.set()

        periodic = asyncio.create_task(slow_periodic_collect())
        orch._collect_tasks[run.worker_id] = periodic
        await started.wait()
        run.phase = WorkerPhase.DONE
        run.final_collected = True

        await orch._finalize_terminal_run(job, run)

        assert cancellation_finished.is_set()
        assert run.cleaned_up is True
        assert d.bound_released == [(run.lease_id, run.worker_id)]

    async def test_bound_cleanup_failure_retries_until_success(self):
        d = FakeDriver()
        # on_worker_exit and _maybe_finish both make an immediate idempotent
        # attempt; fail both so the background retry path is exercised.
        d.bound_release_failures = 2
        orch = BatchOrchestrator(d, cleanup_retry_seconds=0.01)
        spec = TestEipBoundLaunch()._bound_spec(workers=1, ids=["acct-1"])
        job = await orch.launch(spec)
        run = next(iter(job.runs.values()))

        await orch.on_worker_exit(job.job_id, run.worker_id, 0, task_id=run.task_id)
        assert run.cleaned_up is False
        assert job.summary()["cleanup_pending"] == 1
        for _ in range(50):
            if run.cleaned_up:
                break
            await asyncio.sleep(0.01)
        assert run.cleaned_up is True
        assert run.cleanup_attempts == 3
        assert job.summary()["cleanup_pending"] == 0

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
        await orch._stop_periodic_collect(wid)  # cleanup the background loop

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


class TestShutdown:
    async def test_running_bound_workers_are_collected_and_released(self):
        driver = FakeDriver()
        orch = BatchOrchestrator(driver)
        job = await orch.launch(_spec(
            fanout={"workers": 1, "region": "ap-northeast-1"},
            account={"binding": "eip", "ids": ["acct-1"]},
        ))
        run = next(iter(job.runs.values()))
        assert run.phase == WorkerPhase.RUNNING

        await orch.shutdown(timeout=1)

        assert run.phase == WorkerPhase.FAILED
        assert run.final_collected is True
        assert run.cleaned_up is True
        assert (run.lease_id, run.worker_id) in driver.bound_released
        with pytest.raises(RuntimeError, match="shutting down"):
            await orch.submit(_spec())

        # Shutdown is idempotent and must not destroy/release twice.
        release_count = driver.bound_release_attempts
        await orch.shutdown(timeout=1)
        assert driver.bound_release_attempts == release_count
