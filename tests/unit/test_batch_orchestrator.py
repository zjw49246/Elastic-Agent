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
        self.scale_in_calls = 0
        self.scale_in_failures = 0
        self.stopped: list[tuple[str, str, str]] = []
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
        self.resolved_secret_env: dict[str, str] = {}
        self.secret_resolve_calls: list[dict[str, str]] = []
        self.secret_resolve_error: str | None = None

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
        if tags and tags.get("ElasticAgentLease"):
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

    async def resolve_secret_env(self, secret_env):
        self.secret_resolve_calls.append(dict(secret_env))
        if self.secret_resolve_error:
            raise RuntimeError(self.secret_resolve_error)
        return dict(self.resolved_secret_env)

    async def collect(self, worker_id, spec, job_id):
        self.events.append(("collect", worker_id))
        if self.collect_failures:
            self.collect_failures -= 1
            raise RuntimeError("collect transient")
        self.collected.append((worker_id, job_id))

    async def stop_command(self, worker_id, task_id, signal="SIGTERM"):
        self.stopped.append((worker_id, task_id, signal))

    async def scale_in(self, worker_ids):
        self.scale_in_calls += 1
        if self.scale_in_failures:
            self.scale_in_failures -= 1
            raise RuntimeError("scale-in transient")
        self.scaled_in.extend(worker_ids)


def _spec(**kw):
    kw.setdefault("name", "ai4sci")
    kw.setdefault("run", RunSpec(command="uv run bench --shard {{shard_index}} --host $(hostname -s)"))
    return JobSpec(**kw)


class TestSubmit:
    async def test_new_job_is_not_done_before_workers_exist(self):
        orch = BatchOrchestrator(FakeDriver())
        job = orch.prepare(_spec(fanout={"workers": 1}))

        assert job.summary()["done"] is False
        assert job.summary()["state"] == "queued"

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

    async def test_unbound_workers_are_tagged_with_job_ownership(self):
        d = FakeDriver()
        job = await BatchOrchestrator(d).launch(
            _spec(fanout={"workers": 1})
        )
        assert d.scale_tags == [{"ElasticAgentJob": job.job_id}]

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

    async def test_secret_env_is_resolved_only_at_dispatch_and_merged(self):
        d = FakeDriver()
        d.resolved_secret_env = {"TOKEN": "resolved-plaintext"}
        spec = _spec(
            account={"mode": "none"},
            fanout={"workers": 1},
            run={
                "command": "bench",
                "env": {"VISIBLE": "value"},
                "secret_env": {"TOKEN": "aws-ssm:///prod/token"},
            },
        )

        await BatchOrchestrator(d).launch(spec)

        assert d.secret_resolve_calls == [{"TOKEN": "aws-ssm:///prod/token"}]
        assert d.dispatched[0]["env"] == {
            "VISIBLE": "value", "TOKEN": "resolved-plaintext",
        }
        # Plaintext never mutates the durable JobSpec.
        assert spec.run.secret_env == {"TOKEN": "aws-ssm:///prod/token"}
        assert "resolved-plaintext" not in spec.model_dump_json()

    async def test_selected_credential_home_wins_after_secret_resolution(self):
        d = FakeDriver()
        d.resolved_secret_env = {
            "CLAUDE_CONFIG_DIR": "/attacker/credential-home",
        }
        orch = BatchOrchestrator(d)
        job = await orch.launch(_spec(
            account={"config_dir": "/managed/selected-home"},
            run={
                "command": "true",
                "secret_env": {
                    "CLAUDE_CONFIG_DIR": "aws-ssm:///prod/credential-home",
                },
            },
        ))
        run = next(iter(job.runs.values()))

        assert run.config_dir == "/managed/selected-home"
        assert d.dispatched[-1]["env"]["CLAUDE_CONFIG_DIR"] == run.config_dir

    async def test_secret_resolution_failure_fails_worker_before_dispatch(self):
        d = FakeDriver()
        d.secret_resolve_error = "AccessDenied"
        job = await BatchOrchestrator(d).launch(_spec(
            account={"mode": "none"},
            fanout={"workers": 1},
            run={
                "command": "bench",
                "secret_env": {"TOKEN": "aws-secretsmanager://prod/token"},
            },
        ))

        assert d.dispatched == []
        assert next(iter(job.runs.values())).phase == WorkerPhase.FAILED
        assert "AccessDenied" in next(iter(job.runs.values())).error

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

    async def test_admin_cancel_during_reservation_rolls_back_before_scale(self):
        d = FakeDriver()
        orch = BatchOrchestrator(d)
        entered = asyncio.Event()
        release = asyncio.Event()
        original_reserve = d.reserve_bound

        async def gated_reserve(job_id, slot, spec, account_id=""):
            entered.set()
            await release.wait()
            return await original_reserve(
                job_id, slot, spec, account_id=account_id,
            )

        d.reserve_bound = gated_reserve
        job = await orch.submit(self._bound_spec(workers=1, ids=["acct-1"]))
        await entered.wait()
        cancelling = asyncio.create_task(
            orch.cancel_job(job.job_id, reason="admin cancelled")
        )
        await asyncio.sleep(0)
        release.set()

        assert await cancelling is True
        assert d.bound_released == [("lease-0", None)]
        assert not any(event[0] == "scale" for event in d.events)
        assert job.pending_cleanup == {}
        assert job.summary()["state"] == "cancelled"

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
    async def test_deferred_exhaustion_does_not_block_dynamic_login_result(self):
        d = FakeDriver()
        orch = BatchOrchestrator(d)
        job = await orch.launch(_spec(
            fanout={"workers": 1},
            rotation={
                "strategy": "on_exhaust_restart_resume",
                "resume_args": "--resume out",
                "max_rotations": 3,
            },
        ))
        run = next(iter(job.runs.values()))
        old_task_id = run.task_id
        login_started = asyncio.Event()
        login_result = asyncio.Event()
        original_login = d.login

        async def gated_login(*args, **kwargs):
            login_started.set()
            await login_result.wait()
            return await original_login(*args, **kwargs)

        d.login = gated_login

        # The event callback can ACK immediately: claim is synchronous and the
        # correlated login result is awaited by a separate lifecycle task.
        assert orch.defer_exhausted(
            run.worker_id, task_id=old_task_id,
        ) is True
        rotation_task = run.rotation_task
        assert rotation_task is not None
        assert run.phase == WorkerPhase.ROTATING
        await asyncio.wait_for(login_started.wait(), timeout=1)
        assert rotation_task.done() is False

        # PROCESS_EXIT queued behind RUN_EXHAUSTED cannot fail the claimed run.
        await orch.on_worker_exit(
            job.job_id, run.worker_id, 130, task_id=old_task_id,
        )
        assert run.phase == WorkerPhase.ROTATING

        login_result.set()
        assert await asyncio.wait_for(rotation_task, timeout=1) is True
        assert run.phase == WorkerPhase.RUNNING
        assert run.task_id != old_task_id

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

    async def test_runtime_account_snapshot_does_not_follow_active_slot(self):
        d = FakeDriver()
        orch = BatchOrchestrator(d)
        job = await orch.launch(_spec(
            fanout={"workers": 1},
            account={"per_worker": 2},
        ))
        wid = next(iter(job.runs))
        run = job.runs[wid]
        task_id = run.task_id
        first_account = run.account_ids[0]
        run.task_auth_kind = "agent_api"

        assert orch.runtime_account_for_task(
            wid, task_id=task_id,
        ) == (first_account, "agent_api")

        # Rotation advances the selected slot before the replacement dispatch
        # receives its new task id. Feedback for the old id remains pinned to A.
        run.active_slot = 1
        assert run.account_id != first_account
        assert orch.runtime_account_for_task(
            wid, task_id=task_id,
        ) == (first_account, "agent_api")
        assert orch.runtime_account_for_task(
            wid, task_id="stale-or-future",
        ) is None

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

    async def test_stale_or_replayed_exhaustion_cannot_rotate_new_task(self):
        d = FakeDriver()
        orch = BatchOrchestrator(d)
        job = await orch.launch(_spec(
            fanout={"workers": 1},
            rotation={
                "strategy": "on_exhaust_restart_resume",
                "resume_args": "-r",
                "max_rotations": 3,
            },
        ))
        wid = next(iter(job.runs))
        old_task = job.runs[wid].task_id

        assert await orch.on_worker_exhausted(
            job.job_id, wid, task_id=old_task,
        ) is True
        new_task = job.runs[wid].task_id
        dispatches = len(d.dispatched)

        assert await orch.on_worker_exhausted(
            job.job_id, wid, task_id=old_task,
        ) is False
        assert job.runs[wid].task_id == new_task
        assert len(d.dispatched) == dispatches

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

    async def test_explicit_ids_are_mapped_by_worker_then_slot(self):
        d = FakeDriver()
        orch = BatchOrchestrator(d)
        job = await orch.launch(_spec(
            fanout={"workers": 2},
            account={
                "per_worker": 2,
                "config_dir": "/root/.claude",
                "ids": ["a1", "a2", "a3", "a4"],
            },
        ))

        requested = [
            event[2] for event in d.events if event[0] == "login"
        ]
        assert requested == ["a1", "a2", "a3", "a4"]
        by_shard = sorted(job.runs.values(), key=lambda run: run.ctx.shard_index)
        assert by_shard[0].account_ids == ["a1", "a2"]
        assert by_shard[1].account_ids == ["a3", "a4"]

    async def test_login_may_return_a_nested_agent_api_home(self):
        class AgentApiDriver(FakeDriver):
            async def login(
                self, worker_id, spec, config_dir, *,
                account_id="", claim_id="",
            ):
                outcome = await super().login(
                    worker_id,
                    spec,
                    config_dir,
                    account_id=account_id,
                    claim_id=claim_id,
                )
                outcome.config_dir = f"{config_dir}/claude"
                return outcome

        d = AgentApiDriver()
        orch = BatchOrchestrator(d)
        job = await orch.launch(_spec(
            account={"config_dir": "/managed/cloudrouter-1"},
        ))
        run = next(iter(job.runs.values()))

        assert run.config_dir == "/managed/cloudrouter-1/claude"
        assert d.dispatched[-1]["env"]["CLAUDE_CONFIG_DIR"] == run.config_dir

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

    async def test_codex_rotation_never_guesses_root_home(self):
        from types import SimpleNamespace

        spec = _spec(account={"agent_type": "codex"})
        job = SimpleNamespace(spec=spec)
        run = SimpleNamespace(config_dirs=[""], rotations=2)

        with pytest.raises(RuntimeError, match="worker-writable config_dir"):
            BatchOrchestrator._extra_dir(job, run)

    @pytest.mark.parametrize(
        ("agent_type", "explicit_slot"),
        [("claude", False), ("codex", True)],
    )
    async def test_agent_api_rotation_uses_source_slot_outside_projection(
        self,
        tmp_path,
        agent_type,
        explicit_slot,
    ):
        from pathlib import Path, PurePosixPath
        from types import SimpleNamespace

        from elastic_agent.core.protocols.messages import ExecuteMessage
        from elastic_agent.worker.agent_api import configure_agent_api
        from elastic_agent.worker.runtime import WorkerRuntime

        slot = tmp_path / f"{agent_type}-slot"
        home = configure_agent_api(
            provider="cloudrouter",
            agent_type=agent_type,
            config_dir=slot,
            api_key="cloudrouter-secret",
            account_id="cloudrouter-1",
            models=[
                "claude-opus-4-8"
                if agent_type == "claude"
                else "gpt-5.4"
            ],
        )
        spec = _spec(
            account={
                "agent_type": agent_type,
                "config_dir": str(slot) if explicit_slot else "",
            },
            rotation={
                "strategy": "on_exhaust_restart_resume",
                "resume_args": "--resume",
            },
        )
        job = SimpleNamespace(spec=spec)
        run = SimpleNamespace(
            config_dirs=[home],
            account_ids=["cloudrouter-1"],
            account_auth_kinds=["agent_api"],
            rotations=1,
        )

        rotation_home = BatchOrchestrator._extra_dir(job, run)

        assert rotation_home == f"{slot}-rot-1"
        assert ".elastic-agent-api" not in PurePosixPath(rotation_home).parts
        Path(rotation_home).mkdir(mode=0o700)
        credential_env = (
            "CLAUDE_CONFIG_DIR"
            if agent_type == "claude"
            else "CODEX_HOME"
        )
        execute = ExecuteMessage(
            task_id="rotation-task",
            command=["true"],
            cwd="/",
            env={credential_env: rotation_home},
        )
        assert WorkerRuntime._agent_api_projection_for_execute(execute) is None

    async def test_agent_api_rotation_rejects_mismatched_projection_home(self):
        from types import SimpleNamespace

        spec = _spec(
            rotation={
                "strategy": "on_exhaust_restart_resume",
                "resume_args": "--resume",
            },
        )
        job = SimpleNamespace(spec=spec)
        run = SimpleNamespace(
            config_dirs=[
                "/safe/.elastic-agent-api/cloudrouter/cloudrouter-other/claude"
            ],
            account_ids=["cloudrouter-1"],
            account_auth_kinds=["agent_api"],
            rotations=1,
        )

        with pytest.raises(RuntimeError, match="cannot derive rotation slot"):
            BatchOrchestrator._extra_dir(job, run)


class TestCompletion:
    async def test_status_reconciliation_finalizes_missing_running_task(self):
        d = FakeDriver()
        orch = BatchOrchestrator(d, status_reconcile_grace_seconds=0)
        job = await orch.launch(_spec(fanout={"workers": 1}))
        wid = next(iter(job.runs))

        assert await orch.reconcile_worker_status(wid, []) is True

        assert job.runs[wid].phase == WorkerPhase.FAILED
        assert "no longer active" in job.runs[wid].error
        assert d.scaled_in == [wid]

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

    async def test_default_lifecycle_terminates_ephemeral_workers(self):
        d = FakeDriver()
        orch = BatchOrchestrator(d)
        job = await orch.launch(_spec(fanout={"workers": 2}))
        for wid in list(job.runs):
            await orch.on_worker_exit(job.job_id, wid, 0)
        assert set(d.scaled_in) == set(job.runs.keys())

    async def test_terminal_replay_retries_failed_scale_in(self):
        d = FakeDriver()
        d.scale_in_failures = 1
        orch = BatchOrchestrator(d, cleanup_retry_seconds=60)
        job = await orch.launch(_spec(fanout={"workers": 1}))
        run = next(iter(job.runs.values()))

        await orch.on_worker_exit(
            job.job_id, run.worker_id, 0, task_id=run.task_id,
        )
        assert run.phase == WorkerPhase.DONE
        assert job.resources_released is False

        await orch.on_worker_exit(
            job.job_id, run.worker_id, 0, task_id=run.task_id,
        )
        assert job.resources_released is True
        assert d.scale_in_calls == 2

    async def test_concurrent_shard_exits_scale_in_once(self):
        d = FakeDriver()
        orch = BatchOrchestrator(d)
        job = await orch.launch(_spec(fanout={"workers": 2}))
        await asyncio.gather(*(
            orch.on_worker_exit(
                job.job_id, run.worker_id, 0, task_id=run.task_id,
            )
            for run in job.runs.values()
        ))
        assert d.scale_in_calls == 1

    async def test_cancel_job_stops_collects_and_scales_in_all_workers(self):
        d = FakeDriver()
        orch = BatchOrchestrator(
            d, cancel_grace_seconds=0.01, cancel_kill_grace_seconds=0.01,
        )
        job = await orch.launch(_spec(fanout={"workers": 2}))

        assert await orch.cancel_job(job.job_id, reason="admin requested") is True

        assert {worker for worker, _task, _signal in d.stopped} == set(job.runs)
        assert {worker for worker, _job in d.collected} == set(job.runs)
        assert set(d.scaled_in) == set(job.runs)
        assert all(run.phase == WorkerPhase.CANCELLED for run in job.runs.values())
        assert job.summary()["state"] == "cancelled"

    async def test_cancel_stops_uncertain_dispatch_even_after_phase_is_terminal(self):
        d = FakeDriver()
        orch = BatchOrchestrator(
            d, cancel_grace_seconds=0.01, cancel_kill_grace_seconds=0.01,
        )
        dispatch_entered = asyncio.Event()
        hold_dispatch = asyncio.Event()
        original_run = d.run_command
        original_stop = d.stop_command

        async def accepted_but_blocked_dispatch(*args, **kwargs):
            # Model send_text having handed EXECUTE to the socket before its
            # await is cancelled by the admin request.
            await original_run(*args, **kwargs)
            dispatch_entered.set()
            await hold_dispatch.wait()

        async def record_stop(*args, **kwargs):
            signal = kwargs.get("signal", args[2] if len(args) > 2 else "SIGTERM")
            d.events.append(("stop", args[0], signal))
            await original_stop(*args, **kwargs)

        d.run_command = accepted_but_blocked_dispatch
        d.stop_command = record_stop
        job = await orch.submit(_spec(fanout={"workers": 1}))
        await asyncio.wait_for(dispatch_entered.wait(), timeout=1)
        run = next(iter(job.runs.values()))

        assert await orch.cancel_job(job.job_id, "admin") is True

        assert run.phase == WorkerPhase.CANCELLED
        assert [signal for _wid, _task, signal in d.stopped] == [
            "SIGTERM", "SIGKILL",
        ]
        stop_indexes = [
            index for index, event in enumerate(d.events) if event[0] == "stop"
        ]
        collect_index = next(
            index for index, event in enumerate(d.events)
            if event == ("collect", run.worker_id)
        )
        assert stop_indexes and max(stop_indexes) < collect_index

    async def test_cancel_waits_for_matching_exit_before_collect(self):
        d = FakeDriver()
        orch = BatchOrchestrator(
            d, cancel_grace_seconds=1, cancel_kill_grace_seconds=0.01,
        )
        job = await orch.launch(_spec(fanout={"workers": 1}))
        run = next(iter(job.runs.values()))
        original_stop = d.stop_command

        async def stop_then_confirm(worker_id, task_id, signal="SIGTERM"):
            await original_stop(worker_id, task_id, signal)
            if signal == "SIGTERM":
                asyncio.create_task(orch.on_worker_exit(
                    job.job_id, worker_id, 143, task_id=task_id,
                ))

        d.stop_command = stop_then_confirm
        assert await orch.cancel_job(job.job_id, "admin") is True
        assert d.collected == [(run.worker_id, job.job_id)]
        assert all(signal != "SIGKILL" for _wid, _task, signal in d.stopped)

    async def test_cancel_waits_for_exit_log_archive_barrier(self):
        d = FakeDriver()
        orch = BatchOrchestrator(d, exit_archive_grace_seconds=1)
        job = await orch.launch(_spec(fanout={"workers": 1}))
        run = next(iter(job.runs.values()))

        assert orch.begin_exit_archive(run.worker_id, task_id=run.task_id) is True
        cancelling = asyncio.create_task(orch.cancel_job(job.job_id, "admin"))
        await asyncio.sleep(0)

        assert not d.collected
        assert cancelling.done() is False

        orch.finish_exit_archive(run.worker_id, task_id=run.task_id)
        assert await cancelling is True
        assert d.collected == [(run.worker_id, job.job_id)]

    async def test_cancel_worker_waits_for_exit_log_archive_barrier(self):
        d = FakeDriver()
        orch = BatchOrchestrator(d, exit_archive_grace_seconds=1)
        job = await orch.launch(_spec(fanout={"workers": 1}))
        run = next(iter(job.runs.values()))

        assert orch.begin_exit_archive(run.worker_id, task_id=run.task_id) is True
        cancelling = asyncio.create_task(
            orch.cancel_worker(run.worker_id, "admin worker stop")
        )
        await asyncio.sleep(0)

        assert not d.collected
        assert cancelling.done() is False

        orch.finish_exit_archive(run.worker_id, task_id=run.task_id)
        assert await cancelling is True
        assert d.collected == [(run.worker_id, job.job_id)]

    async def test_cancel_completed_job_does_not_rewrite_outcome(self):
        d = FakeDriver()
        orch = BatchOrchestrator(d)
        job = await orch.launch(_spec(fanout={"workers": 1}))
        run = next(iter(job.runs.values()))
        await orch.on_worker_exit(
            job.job_id, run.worker_id, 0, task_id=run.task_id,
        )
        assert job.summary()["state"] == "succeeded"

        assert await orch.cancel_job(job.job_id, "late cancel") is True
        assert job.summary()["state"] == "succeeded"
        assert job.cancel_requested is False

    async def test_job_state_hook_reaches_terminal_after_cleanup(self):
        d = FakeDriver()
        states = []

        async def record(job_id, state, summary):
            states.append((state, summary))

        orch = BatchOrchestrator(d, job_state_hook=record)
        job = await orch.launch(_spec(fanout={"workers": 1}))
        run = next(iter(job.runs.values()))
        await orch.on_worker_exit(
            job.job_id, run.worker_id, 0, task_id=run.task_id,
        )

        assert [state for state, _summary in states] == [
            "launching", "running", "succeeded",
        ]
        assert states[-1][1]["done"] is True

    async def test_permanent_disconnect_fails_and_tears_down_worker(self):
        d = FakeDriver()
        orch = BatchOrchestrator(d, disconnect_grace_seconds=0.01)
        job = await orch.launch(_spec(fanout={"workers": 1}))
        run = next(iter(job.runs.values()))

        await orch.handle_disconnect(run.worker_id)
        for _ in range(100):
            if job.summary()["done"]:
                break
            await asyncio.sleep(0.01)

        assert run.phase == WorkerPhase.FAILED
        assert job.resources_released is True

    async def test_reconnect_cancels_disconnect_teardown(self):
        d = FakeDriver()
        orch = BatchOrchestrator(d, disconnect_grace_seconds=0.1)
        job = await orch.launch(_spec(fanout={"workers": 1}))
        run = next(iter(job.runs.values()))

        await orch.handle_disconnect(run.worker_id)
        await orch.handle_reconnect(run.worker_id)
        await asyncio.sleep(0.12)

        assert run.phase == WorkerPhase.RUNNING
        assert d.scale_in_calls == 0

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

    async def test_ordinary_account_is_not_released_until_worker_teardown_succeeds(
        self,
    ):
        driver = FakeDriver()
        driver.scale_in_failures = 1
        orch = BatchOrchestrator(driver, cleanup_retry_seconds=60)
        allocator = _RecordingAllocator()
        orch._allocator = allocator
        job = await orch.launch(_spec(fanout={"workers": 1}))
        run = next(iter(job.runs.values()))

        await orch.on_worker_exit(
            job.job_id,
            run.worker_id,
            0,
            task_id=run.task_id,
        )
        assert job.resources_released is False
        assert job.accounts_released is False
        assert allocator.released == []

        await orch.on_worker_exit(
            job.job_id,
            run.worker_id,
            0,
            task_id=run.task_id,
        )
        assert job.resources_released is True
        assert job.accounts_released is True
        assert allocator.released == [run.worker_id]

    async def test_agent_api_forces_ephemeral_teardown_when_global_scale_in_is_off(
        self,
    ):
        class AgentApiDriver(FakeDriver):
            async def login(
                self, worker_id, spec, config_dir, *,
                account_id="", claim_id="",
            ):
                outcome = await super().login(
                    worker_id,
                    spec,
                    config_dir,
                    account_id=account_id,
                    claim_id=claim_id,
                )
                outcome.auth_kind = "agent_api"
                outcome.config_dir = (
                    f"{config_dir}/.elastic-agent-api/cloudrouter/"
                    f"{outcome.account_id}/claude"
                )
                return outcome

        driver = AgentApiDriver()
        orch = BatchOrchestrator(driver, scale_in_on_complete=False)
        allocator = _RecordingAllocator()
        orch._allocator = allocator
        job = await orch.launch(_spec(
            account={"config_dir": "/managed/slot"},
        ))
        run = next(iter(job.runs.values()))

        assert job.release_workers_on_complete is True
        await orch.on_worker_exit(
            job.job_id,
            run.worker_id,
            0,
            task_id=run.task_id,
        )
        assert driver.scaled_in == [run.worker_id]
        assert allocator.released == [run.worker_id]

    async def test_failed_agent_api_delivery_still_forces_worker_teardown(
        self,
    ):
        class FailedAgentApiDriver(FakeDriver):
            async def login(
                self, worker_id, spec, config_dir, *,
                account_id="", claim_id="",
            ):
                return LoginOutcome(
                    success=False,
                    account_id="cloudrouter-1",
                    auth_kind="agent_api",
                    error="worker disconnected after key delivery",
                )

        driver = FailedAgentApiDriver()
        orch = BatchOrchestrator(driver, scale_in_on_complete=False)
        allocator = _RecordingAllocator()
        orch._allocator = allocator
        job = await orch.launch(_spec(
            account={"config_dir": "/managed/slot"},
        ))
        run = next(iter(job.runs.values()))

        assert run.phase == WorkerPhase.FAILED
        assert driver.scaled_in == [run.worker_id]
        assert allocator.released == [run.worker_id]

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

    async def test_running_ordinary_workers_are_collected_and_terminated(self):
        driver = FakeDriver()
        orch = BatchOrchestrator(
            driver,
            cancel_grace_seconds=0.01,
            cancel_kill_grace_seconds=0.01,
        )
        job = await orch.launch(_spec(fanout={"workers": 1}))
        run = next(iter(job.runs.values()))

        await orch.shutdown(timeout=0.1)

        assert run.phase == WorkerPhase.FAILED
        assert driver.collected == [(run.worker_id, job.job_id)]
        assert driver.scaled_in == [run.worker_id]
        assert job.resources_released is True
