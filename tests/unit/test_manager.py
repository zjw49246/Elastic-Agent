"""Tests for ElasticAgentManager (T-016)."""

from __future__ import annotations

import asyncio
import json
import threading
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from elastic_agent.core.account_binding import AccountBinding, BindingState, LeaseState
from elastic_agent.core.config import ElasticAgentConfig
from elastic_agent.core.providers.base import (
    CloudProvider,
    Instance,
    InstanceConfig,
    InstanceNotFoundError,
    InstanceState,
)
from elastic_agent.core.registry import NodeRecord, NodeStatus
from elastic_agent.manager.manager import ElasticAgentManager
from elastic_agent.testing.dry_run_provider import DryRunProvider

# ------------------------------------------------------------------
# Helpers — in-memory mock provider
# ------------------------------------------------------------------


class InMemoryProvider(CloudProvider):
    """Minimal mock cloud provider for unit testing."""

    def __init__(self):
        self._instances: dict[str, Instance] = {}
        self._counter = 0

    @property
    def platform(self) -> str:
        return "dryrun"

    async def create_instance(self, config: InstanceConfig) -> Instance:
        self._last_config = config
        self._counter += 1
        iid = f"i-dry-{self._counter:04d}"
        inst = Instance(
            instance_id=f"dryrun:{iid}",
            platform="dryrun",
            native_id=iid,
            state=InstanceState.PENDING,
            public_ip=f"1.2.3.{self._counter}",
            private_ip=f"10.0.0.{self._counter}",
            instance_type=config.instance_type,
            region="test",
            zone="test-a",
            tags=config.tags,
        )
        self._instances[iid] = inst
        return inst

    async def terminate_instance(self, instance_id: str) -> None:
        native = instance_id.split(":", 1)[-1] if ":" in instance_id else instance_id
        self._instances.pop(native, None)

    async def start_instance(self, instance_id: str) -> None:
        native = instance_id.split(":", 1)[-1] if ":" in instance_id else instance_id
        inst = self._instances.get(native)
        if inst:
            inst.state = InstanceState.RUNNING

    async def stop_instance(self, instance_id: str) -> None:
        pass

    async def reboot_instance(self, instance_id: str) -> None:
        pass

    async def list_instances(self, filters: dict | None = None) -> list[Instance]:
        return list(self._instances.values())

    async def get_instance(self, instance_id: str) -> Instance:
        native = instance_id.split(":", 1)[-1] if ":" in instance_id else instance_id
        instance = self._instances.get(native)
        if instance is None:
            raise InstanceNotFoundError(f"Instance not found: {instance_id}")
        return instance

    async def wait_until_running(self, instance_id: str, timeout: int = 300) -> Instance:
        inst = await self.get_instance(instance_id)
        inst.state = InstanceState.RUNNING
        return inst


# ------------------------------------------------------------------
# Fixtures
# ------------------------------------------------------------------


@pytest.fixture
def tmp_config(tmp_path):
    registry_path = tmp_path / "registry.json"
    return ElasticAgentConfig(
        registry={"path": str(registry_path)},
        task_registry={"path": str(tmp_path / "task_registry.json")},
        webhook={"dead_letter_path": str(tmp_path / "dead_letters.json")},
        logging={"operations_log": str(tmp_path / "operations.log")},
    )


@pytest.fixture(autouse=True)
def fast_binding_manager_polling(monkeypatch):
    """Keep production's five-minute cloud ambiguity window out of unit tests."""
    import elastic_agent.core.binding_manager as binding_manager_module

    monkeypatch.setattr(
        binding_manager_module, "EIP_ALLOCATION_CONVERGENCE_ATTEMPTS", 2
    )
    monkeypatch.setattr(
        binding_manager_module, "EIP_ALLOCATION_CONVERGENCE_SECONDS", 0
    )
    monkeypatch.setattr(binding_manager_module, "TEARDOWN_CONFIRM_SECONDS", 0)


@pytest.fixture
def provider():
    return InMemoryProvider()


@pytest.fixture
def manager(tmp_config, provider):
    return ElasticAgentManager(tmp_config, provider)


def _rewrite_persisted_account_mode(
    registry_path: str,
    job_id: str,
    mode: str,
) -> None:
    """Model a Job journal written by an older Manager release."""
    spec_path = Path(registry_path).with_name("specs") / f"{job_id}.json"
    payload = json.loads(spec_path.read_text(encoding="utf-8"))
    payload["spec"]["account"]["mode"] = mode
    spec_path.write_text(json.dumps(payload), encoding="utf-8")


async def _wait_for_binding_recovery(
    manager: ElasticAgentManager,
    *,
    timeout: float = 2,
) -> None:
    """Wait for startup recovery when a test needs its terminal effects."""
    async with asyncio.timeout(timeout):
        while not manager.binding_recovery_ready:
            await asyncio.sleep(0.01)


# ------------------------------------------------------------------
# Tests: lifecycle
# ------------------------------------------------------------------


class TestManagerLifecycle:
    @pytest.mark.asyncio
    async def test_interrupt_journal_thread_keeps_lock_through_cancellation(
        self,
        manager,
        monkeypatch,
    ):
        from elastic_agent.core import job_spec_store
        from elastic_agent.core.job_spec import JobSpec
        from elastic_agent.core.job_spec_store import load_job_spec_journal

        job_id = "job-cancelled-intent-thread"
        await manager._persist_batch_job_spec(
            job_id,
            JobSpec.model_validate({
                "name": "cancelled-intent-thread",
                "run": {"command": "run", "resume_command": "resume"},
                "fanout": {
                    "workers": 1,
                    "shard_by": "shard_index",
                },
                "collect": {"paths": ["results"], "checkpoint": True},
            }),
        )
        entered = threading.Event()
        release = threading.Event()
        checkpoint_entered = threading.Event()
        original_intent = job_spec_store.update_job_interrupt_intent
        original_checkpoint = job_spec_store.update_job_checkpoint

        def gated_intent(*args, **kwargs):
            entered.set()
            assert release.wait(timeout=5)
            return original_intent(*args, **kwargs)

        def observed_checkpoint(*args, **kwargs):
            checkpoint_entered.set()
            return original_checkpoint(*args, **kwargs)

        monkeypatch.setattr(
            job_spec_store,
            "update_job_interrupt_intent",
            gated_intent,
        )
        monkeypatch.setattr(
            job_spec_store,
            "update_job_checkpoint",
            observed_checkpoint,
        )
        intent = asyncio.create_task(
            manager._update_batch_interrupt_intent(
                job_id,
                "e" * 64,
                {
                    "state": "suspending",
                    "done": False,
                    "interrupt_requested": True,
                    "resume_available": False,
                },
            )
        )
        assert await asyncio.to_thread(entered.wait, 2)
        intent.cancel()
        checkpoint = asyncio.create_task(
            manager._update_batch_checkpoint_generation(
                job_id,
                "periodic-00000001",
                "2026-07-29T12:00:00+00:00",
            )
        )
        await asyncio.sleep(0.05)
        assert checkpoint_entered.is_set() is False
        assert intent.done() is False

        release.set()
        with pytest.raises(asyncio.CancelledError):
            await intent
        await checkpoint

        payload = load_job_spec_journal(
            manager.config.registry.path,
            job_id,
        )
        assert payload["submission_state"] == "suspending"
        assert payload["interrupt_intent"]["idempotency_digest"] == "e" * 64
        assert payload["latest_checkpoint_generation"] == (
            "periodic-00000001"
        )

    @pytest.mark.asyncio
    async def test_startup_merge_finishes_suspending_intent_only_after_cleanup(
        self, manager,
    ):
        from elastic_agent.core.job_spec import JobSpec
        from elastic_agent.core.job_spec_store import load_job_spec_journal

        job_id = "job-suspending-recovery"
        await manager._persist_batch_job_spec(
            job_id,
            JobSpec.model_validate({
                "name": "suspending-recovery",
                "run": {
                    "command": "run",
                    "resume_command": "resume",
                },
                "fanout": {
                    "workers": 2,
                    "shard_by": "shard_index",
                },
                "collect": {
                    "paths": ["results"],
                    "checkpoint": True,
                },
            }),
        )
        committed_at = "2026-07-29T12:00:00+00:00"
        await manager._update_batch_checkpoint_generation(
            job_id,
            "periodic-00000005",
            committed_at,
        )
        await manager._update_batch_interrupt_intent(
            job_id,
            "a" * 64,
            {
                "state": "suspending",
                "done": False,
                "interrupt_requested": True,
                "interrupt_reason": "save progress",
                "interrupt_requested_at": committed_at,
                "interrupt_checkpoint_generation_before": (
                    "periodic-00000005"
                ),
                "interrupt_checkpoint_committed_at_before": committed_at,
                "resume_available": False,
            },
        )

        assert await manager._merge_recovered_terminal_worker(
            job_id=job_id,
            worker_id="worker-0",
            shard_index=0,
            collected=False,
            collection_error="final copy failed",
            worker_released=True,
        )
        partial = load_job_spec_journal(
            manager.config.registry.path,
            job_id,
        )
        assert partial["submission_state"] == "suspending"
        assert partial["terminal_summary"]["cleanup_pending"] == 1
        assert partial["terminal_summary"]["resume_available"] is False

        assert await manager._merge_recovered_terminal_worker(
            job_id=job_id,
            worker_id="worker-1",
            shard_index=1,
            collected=False,
            collection_error="final copy failed",
            worker_released=True,
        )
        terminal = load_job_spec_journal(
            manager.config.registry.path,
            job_id,
        )
        summary = terminal["terminal_summary"]
        assert terminal["submission_state"] == "suspended"
        assert summary["state"] == "suspended"
        assert summary["done"] is True
        assert summary["cleanup_pending"] == 0
        assert summary["resume_available"] is True
        assert summary["resume_generation"] == "periodic-00000005"
        assert "previous complete generation" in summary["suspend_warning"]
        assert summary["phases"] == {"suspended": 2}
        assert {
            worker["phase"] for worker in summary["terminal_workers"]
        } == {"suspended"}

    @pytest.mark.asyncio
    async def test_startup_merge_without_complete_checkpoint_fails_closed(
        self, manager,
    ):
        from elastic_agent.core.job_spec import JobSpec
        from elastic_agent.core.job_spec_store import load_job_spec_journal

        job_id = "job-suspending-no-checkpoint"
        await manager._persist_batch_job_spec(
            job_id,
            JobSpec.model_validate({
                "name": "suspending-no-checkpoint",
                "run": {
                    "command": "run",
                    "resume_command": "resume",
                },
                "fanout": {
                    "workers": 1,
                    "shard_by": "shard_index",
                },
                "collect": {
                    "paths": ["results"],
                    "checkpoint": True,
                },
            }),
        )
        await manager._update_batch_interrupt_intent(
            job_id,
            "b" * 64,
            {
                "state": "suspending",
                "done": False,
                "interrupt_requested": True,
                "resume_available": False,
            },
        )

        await manager._merge_recovered_terminal_worker(
            job_id=job_id,
            worker_id="worker-0",
            shard_index=0,
            collected=False,
            collection_error="no checkpoint",
            worker_released=True,
        )
        terminal = load_job_spec_journal(
            manager.config.registry.path,
            job_id,
        )
        summary = terminal["terminal_summary"]
        assert terminal["submission_state"] == "failed"
        assert summary["state"] == "failed"
        assert summary["done"] is True
        assert summary["cleanup_pending"] == 0
        assert summary["resume_available"] is False
        assert summary["resume_generation"] is None

    @pytest.mark.asyncio
    async def test_startup_corrupt_interrupt_summary_cannot_create_resume(
        self,
        manager,
    ):
        from elastic_agent.core.job_spec import JobSpec
        from elastic_agent.core.job_spec_store import load_job_spec_journal

        job_id = "job-corrupt-interrupt-summary"
        await manager._persist_batch_job_spec(
            job_id,
            JobSpec.model_validate({
                "name": "corrupt-interrupt",
                "run": {"command": "run", "resume_command": "resume"},
                "fanout": {
                    "workers": 1,
                    "shard_by": "shard_index",
                },
                "collect": {"paths": ["results"], "checkpoint": True},
            }),
        )
        await manager._update_batch_checkpoint_generation(
            job_id,
            "periodic-00000001",
            "2026-07-29T12:00:00+00:00",
        )
        await manager._update_batch_interrupt_intent(
            job_id,
            "2" * 64,
            {
                "state": "suspending",
                "done": False,
                "interrupt_requested": True,
                "resume_available": False,
            },
        )
        journal = (
            Path(manager.config.registry.path).with_name("specs")
            / f"{job_id}.json"
        )
        corrupt = json.loads(journal.read_text(encoding="utf-8"))
        corrupt["terminal_summary"]["interrupt_requested"] = False
        journal.write_text(json.dumps(corrupt), encoding="utf-8")

        await manager._merge_recovered_terminal_worker(
            job_id=job_id,
            worker_id="worker-corrupt",
            shard_index=0,
            collected=True,
            collection_error=None,
            worker_released=True,
        )

        payload = load_job_spec_journal(
            manager.config.registry.path,
            job_id,
        )
        assert payload["submission_state"] == "failed"
        assert payload["terminal_summary"]["resume_available"] is False
        assert payload["terminal_summary"]["interrupt_requested"] is False

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("with_checkpoint", "expected_state"),
        [(True, "suspended"), (False, "failed")],
    )
    async def test_startup_terminal_interrupt_tombstone_closes_crash_gap(
        self,
        manager,
        with_checkpoint,
        expected_state,
    ):
        from elastic_agent.core.job_spec import JobSpec
        from elastic_agent.core.job_spec_store import load_job_spec_journal

        job_id = f"job-interrupt-tombstone-{expected_state}"
        await manager._persist_batch_job_spec(
            job_id,
            JobSpec.model_validate({
                "name": "interrupt-tombstone",
                "run": {
                    "command": "run",
                    "resume_command": "resume",
                },
                "fanout": {
                    "workers": 1,
                    "shard_by": "shard_index",
                },
                "collect": {
                    "paths": ["results"],
                    "checkpoint": True,
                },
            }),
        )
        committed_at = "2026-07-29T12:00:00+00:00"
        if with_checkpoint:
            await manager._update_batch_checkpoint_generation(
                job_id,
                "periodic-00000009",
                committed_at,
            )
        await manager._update_batch_interrupt_intent(
            job_id,
            "c" * 64,
            {
                "state": "suspending",
                "done": False,
                "interrupt_requested": True,
                "interrupt_reason": "save",
                "interrupt_requested_at": committed_at,
                "interrupt_checkpoint_generation_before": (
                    "periodic-00000009" if with_checkpoint else None
                ),
                "interrupt_checkpoint_committed_at_before": (
                    committed_at if with_checkpoint else None
                ),
                "resume_available": False,
                "terminal_workers": [],
            },
        )
        worker_id = f"dryrun:i-{expected_state}-tombstone"
        proof = {
            "schema": 1,
            "job_id": job_id,
            "worker_id": worker_id,
            "instance_id": worker_id,
            "shard_index": 0,
            "collection_attempted": True,
            "collected": with_checkpoint,
            "collection_error": (
                None if with_checkpoint else "final checkpoint failed"
            ),
        }
        await manager.registry.add(NodeRecord(
            node_id=worker_id,
            instance_id=worker_id,
            platform="dryrun",
            status=NodeStatus.TERMINATED,
            metadata={
                "job_id": job_id,
                "controller_id": (
                    manager.account_binding_store.controller_id
                ),
                "shard_index": 0,
                "interrupt_cleanup_proof": proof,
            },
        ))

        # Exact crash point: EC2 termination and TERMINATED registry fsync
        # committed, but the live orchestrator had not written its terminal
        # suspended/failed Job state or removed the tombstone.
        await manager._initialize_binding_recovery()

        payload = load_job_spec_journal(
            manager.config.registry.path,
            job_id,
        )
        summary = payload["terminal_summary"]
        assert payload["submission_state"] == expected_state
        assert summary["state"] == expected_state
        assert summary["done"] is True
        assert summary["cleanup_pending"] == 0
        assert summary["interrupt_requested"] is True
        assert summary["resume_available"] is with_checkpoint
        assert payload["interrupt_intent"]["idempotency_digest"] == "c" * 64
        assert await manager.registry.get(worker_id) is None

    @pytest.mark.asyncio
    async def test_startup_failed_interrupt_tombstone_replay_preserves_identity(
        self,
        manager,
    ):
        from elastic_agent.core.job_spec import JobSpec
        from elastic_agent.core.job_spec_store import load_job_spec_journal

        job_id = "job-failed-interrupt-tombstone"
        await manager._persist_batch_job_spec(
            job_id,
            JobSpec.model_validate({
                "name": "failed-interrupt",
                "run": {
                    "command": "run",
                    "resume_command": "resume",
                },
                "fanout": {
                    "workers": 1,
                    "shard_by": "shard_index",
                },
                "collect": {
                    "paths": ["results"],
                    "checkpoint": True,
                },
            }),
        )
        await manager._update_batch_interrupt_intent(
            job_id,
            "d" * 64,
            {
                "state": "suspending",
                "done": False,
                "interrupt_requested": True,
                "resume_available": False,
            },
        )
        failed_summary = {
            "job_id": job_id,
            "state": "failed",
            "done": True,
            "cleanup_pending": 0,
            "interrupt_requested": True,
            "resume_available": False,
            "resume_generation": None,
            "resume_committed_at": None,
            "terminal_workers": [{
                "worker_id": "dryrun:i-failed-terminal",
                "shard_index": 0,
                "phase": "failed",
                "task_id": "",
                "error": "no checkpoint",
                "final_collected": False,
                "collection_error": "no checkpoint",
                "cleanup_error": None,
                "worker_released": True,
            }],
        }
        await manager._update_batch_job_state(
            job_id,
            "failed",
            failed_summary,
        )
        worker_id = "dryrun:i-failed-terminal"
        await manager.registry.add(NodeRecord(
            node_id=worker_id,
            instance_id=worker_id,
            platform="dryrun",
            status=NodeStatus.TERMINATED,
            metadata={
                "job_id": job_id,
                "shard_index": 0,
                "interrupt_cleanup_proof": {
                    "schema": 1,
                    "job_id": job_id,
                    "worker_id": worker_id,
                    "instance_id": worker_id,
                    "shard_index": 0,
                    "collection_attempted": True,
                    "collected": False,
                    "collection_error": "no checkpoint",
                },
            },
        ))

        await manager._initialize_binding_recovery()

        payload = load_job_spec_journal(
            manager.config.registry.path,
            job_id,
        )
        assert payload["submission_state"] == "failed"
        assert payload["terminal_summary"] == failed_summary
        assert payload["interrupt_intent"]["idempotency_digest"] == "d" * 64
        assert await manager.registry.get(worker_id) is None

    @pytest.mark.asyncio
    async def test_startup_long_recovery_runs_after_manager_is_ready(
        self, tmp_config, provider,
    ):
        manager = ElasticAgentManager(tmp_config, provider)
        recovery_entered = asyncio.Event()
        allow_recovery = asyncio.Event()

        async def slow_recovery():
            recovery_entered.set()
            await allow_recovery.wait()
            manager._binding_recovery_ready = True

        manager._recover_bound_resources_once = slow_recovery

        await asyncio.wait_for(manager.start(), timeout=1)
        await asyncio.wait_for(recovery_entered.wait(), timeout=1)
        try:
            assert manager._started is True
            assert manager.binding_recovery_ready is False
            assert manager._binding_recovery_task is not None
            assert manager._binding_recovery_task.done() is False
        finally:
            allow_recovery.set()
            if manager._binding_recovery_task is not None:
                await asyncio.wait_for(
                    manager._binding_recovery_task,
                    timeout=1,
                )
            await manager.stop()

    @pytest.mark.asyncio
    async def test_background_startup_recovery_retries_unexpected_failure(
        self, tmp_config, provider,
    ):
        manager = ElasticAgentManager(tmp_config, provider)
        attempts = 0
        first_failed = asyncio.Event()

        async def recover():
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                first_failed.set()
                raise RuntimeError("transient recovery failure")
            manager._binding_recovery_ready = True

        manager._recover_bound_resources_once = recover

        await manager.start()
        try:
            await asyncio.wait_for(first_failed.wait(), timeout=1)
            for _ in range(10):
                await asyncio.sleep(0)
            assert manager.binding_recovery_ready is False
            assert manager._binding_recovery_task is not None
            assert manager._binding_recovery_task.done() is False

            manager._binding_recovery_wakeup.set()
            await _wait_for_binding_recovery(manager)
            assert attempts == 2
        finally:
            await manager.stop()

    @pytest.mark.asyncio
    async def test_startup_terminates_unbound_job_worker_from_previous_manager(
        self, tmp_config, provider
    ):
        from elastic_agent.core.job_spec import JobSpec

        previous = ElasticAgentManager(tmp_config, provider)
        await previous.start()
        await previous._persist_batch_job_spec(
            "job-interrupted",
            JobSpec.model_validate({
                "name": "interrupted",
                "run": {"command": "true"},
                "account": {"mode": "none"},
            }),
        )
        await previous._update_batch_job_state(
            "job-interrupted", "running",
        )
        instance = await provider.create_instance(InstanceConfig(
            instance_type="t3.small",
            image_id="ami-test",
            key_pair_name="key",
            tags={
                "ManagedBy": "elastic-agent",
                "ElasticAgentController": (
                    previous.account_binding_store.controller_id
                ),
                "ElasticAgentJob": "job-interrupted",
            },
        ))
        await previous.registry.add(NodeRecord(
            node_id=instance.instance_id,
            instance_id=instance.instance_id,
            platform=instance.platform,
            status=NodeStatus.READY,
            public_ip=instance.public_ip,
            private_ip=instance.private_ip,
            metadata={"job_id": "job-interrupted", "shard_index": 0},
        ))
        await previous.stop()

        restarted = ElasticAgentManager(tmp_config, provider)
        real_wait_terminated = restarted.binding_manager.wait_instance_terminated
        restarted.binding_manager.wait_instance_terminated = AsyncMock(
            side_effect=real_wait_terminated
        )
        await restarted.start()
        try:
            await _wait_for_binding_recovery(restarted)
            assert provider._instances == {}
            restarted.binding_manager.wait_instance_terminated.assert_awaited_once_with(
                instance.instance_id
            )
            recovered = await restarted.registry.get(instance.instance_id)
            assert recovered is None
            assert restarted.binding_recovery_ready is True
            journal = json.loads(
                (
                    Path(tmp_config.registry.path).with_name("specs")
                    / "job-interrupted.json"
                ).read_text(encoding="utf-8")
            )
            assert journal["submission_state"] == "failed"
            assert journal["terminal_summary"]["done"] is True
        finally:
            await restarted.stop()

    @pytest.mark.asyncio
    async def test_startup_collects_legacy_manager_distribute_unbound_job(
        self, tmp_config, provider, monkeypatch
    ):
        from elastic_agent.core.job_spec import JobSpec
        from elastic_agent.core.manager_fleet_driver import ManagerFleetDriver

        job_id = "job-legacy-manager-distribute"
        previous = ElasticAgentManager(tmp_config, provider)
        await previous.start()
        await previous._persist_batch_job_spec(
            job_id,
            JobSpec.model_validate({
                "name": "legacy-manager-distribute",
                "setup": {"target_dir": "/srv/legacy-job"},
                "run": {"command": "must-never-be-replayed"},
                "account": {"mode": "worker_local_login"},
                "collect": {"paths": ["results"]},
            }),
        )
        await previous._update_batch_job_state(job_id, "running")
        instance = await provider.create_instance(InstanceConfig(
            instance_type="t3.small",
            image_id="ami-test",
            key_pair_name="key",
            tags={
                "ManagedBy": "elastic-agent",
                "ElasticAgentController": (
                    previous.account_binding_store.controller_id
                ),
                "ElasticAgentJob": job_id,
            },
        ))
        await previous.registry.add(NodeRecord(
            node_id=instance.instance_id,
            instance_id=instance.instance_id,
            platform=instance.platform,
            status=NodeStatus.READY,
            public_ip=instance.public_ip,
            private_ip=instance.private_ip,
            metadata={"job_id": job_id, "shard_index": 0},
        ))
        await previous.stop()
        _rewrite_persisted_account_mode(
            tmp_config.registry.path,
            job_id,
            "manager_distribute",
        )

        collected: list[tuple[str, str]] = []

        async def record_collect(_driver, worker_id, recovery_spec, recovered_job_id):
            assert recovered_job_id == job_id
            assert recovery_spec.name == "legacy-manager-distribute"
            assert recovery_spec.setup.target_dir == "/srv/legacy-job"
            assert recovery_spec.collect.paths == ["results"]
            # Recovery compatibility exposes no executable/login fields.
            assert not hasattr(recovery_spec, "run")
            assert not hasattr(recovery_spec, "account")
            collected.append((worker_id, recovered_job_id))

        async def quiesce(_driver, worker_id, recovered_job_id, spec):
            assert worker_id == instance.instance_id
            assert recovered_job_id == job_id
            assert spec.setup.target_dir == "/srv/legacy-job"

        monkeypatch.setattr(
            ManagerFleetDriver,
            "quiesce_recovered_worker",
            quiesce,
        )
        monkeypatch.setattr(ManagerFleetDriver, "collect", record_collect)
        restarted = ElasticAgentManager(tmp_config, provider)
        await restarted.start()
        try:
            await _wait_for_binding_recovery(restarted)
            assert collected == [(instance.instance_id, job_id)]
            assert provider._instances == {}
            recovered = await restarted.registry.get(instance.instance_id)
            assert recovered is None
        finally:
            await restarted.stop()

    @pytest.mark.asyncio
    async def test_startup_checkpoint_collection_reconciles_before_collect(
        self, manager, monkeypatch,
    ):
        from elastic_agent.core.job_spec import JobSpec
        from elastic_agent.core.manager_fleet_driver import ManagerFleetDriver

        job_id = "job-startup-checkpoint-order"
        await manager._persist_batch_job_spec(
            "job-checkpoint-source",
            JobSpec.model_validate({
                "name": "checkpoint-source",
                "setup": {
                    "repo": "https://github.com/example/bench.git",
                    "target_dir": "/srv/checkpoint-source",
                    "resolved_commit": "a" * 40,
                },
                "run": {"command": "capture"},
                "account": {"mode": "none"},
                "fanout": {"shard_by": "shard_index"},
                "collect": {"paths": ["results"], "checkpoint": True},
            }),
        )
        spec = JobSpec.model_validate({
            "name": "startup-checkpoint-order",
            "setup": {
                "repo": "https://github.com/example/bench.git",
                "target_dir": "/srv/checkpoint-order",
                "resolved_commit": "a" * 40,
            },
            "run": {"command": "resume"},
            "account": {"mode": "none"},
            "fanout": {"shard_by": "shard_index"},
            "collect": {"paths": ["results"], "checkpoint": True},
            "recovery": {
                "policy": "checkpoint",
                "source_job_id": "job-checkpoint-source",
                "paths": ["results"],
                "generation": "periodic-00000003",
            },
        })
        await manager._persist_batch_job_spec(job_id, spec)
        instance = SimpleNamespace(
            instance_id="dryrun:i-recovered-checkpoint",
            platform="dryrun",
            public_ip="198.51.100.20",
            private_ip="10.0.0.20",
            tags={
                "ElasticAgentJob": job_id,
                "ElasticAgentController": (
                    manager.account_binding_store.controller_id
                ),
                "ElasticAgentShardIndex": "0",
            },
        )
        await manager.registry.add(NodeRecord(
            node_id=instance.instance_id,
            instance_id=instance.instance_id,
            platform=instance.platform,
            status=NodeStatus.DRAINING,
            public_ip=instance.public_ip,
            private_ip=instance.private_ip,
            metadata={"job_id": job_id, "shard_index": 0},
        ))
        order = []

        async def quiesce(
            _driver, worker_id, recovered_job_id, recovered_spec,
        ):
            assert worker_id == instance.instance_id
            assert recovered_job_id == job_id
            assert recovered_spec.recovery.generation == (
                "periodic-00000003"
            )
            order.append("quiesce")

        async def reconcile(
            _driver,
            worker_id,
            recovered_job_id,
            recovered_spec,
            shard_index,
        ):
            assert worker_id == instance.instance_id
            assert recovered_job_id == job_id
            assert recovered_spec.recovery.source_job_id == (
                "job-checkpoint-source"
            )
            assert shard_index == 0
            order.append("reconcile")

        async def collect(
            _driver, worker_id, _recovered_spec, recovered_job_id,
        ):
            assert worker_id == instance.instance_id
            assert recovered_job_id == job_id
            order.append("collect")

        monkeypatch.setattr(
            ManagerFleetDriver, "quiesce_recovered_worker", quiesce,
        )
        monkeypatch.setattr(
            ManagerFleetDriver, "reconcile_recovery_install", reconcile,
        )
        monkeypatch.setattr(ManagerFleetDriver, "collect", collect)

        await manager._collect_recovered_unbound(instance)

        assert order == ["quiesce", "reconcile", "collect"]

    @pytest.mark.asyncio
    async def test_startup_checkpoint_never_collects_unproven_install(
        self, manager, monkeypatch,
    ):
        from elastic_agent.core.job_spec import JobSpec
        from elastic_agent.core.manager_fleet_driver import ManagerFleetDriver

        job_id = "job-unproven-checkpoint-install"
        await manager._persist_batch_job_spec(
            "job-checkpoint-source",
            JobSpec.model_validate({
                "name": "checkpoint-source",
                "setup": {
                    "repo": "https://github.com/example/bench.git",
                    "target_dir": "/srv/checkpoint-source",
                    "resolved_commit": "b" * 40,
                },
                "run": {"command": "capture"},
                "account": {"mode": "none"},
                "fanout": {"shard_by": "shard_index"},
                "collect": {
                    "paths": ["results"],
                    "checkpoint": True,
                },
            }),
        )
        await manager._persist_batch_job_spec(
            job_id,
            JobSpec.model_validate({
                "name": "unproven-checkpoint-install",
                "setup": {
                    "repo": "https://github.com/example/bench.git",
                    "target_dir": "/srv/unproven-checkpoint",
                    "resolved_commit": "b" * 40,
                },
                "run": {"command": "resume"},
                "account": {"mode": "none"},
                "fanout": {"shard_by": "shard_index"},
                "collect": {
                    "paths": ["results"],
                    "checkpoint": True,
                },
                "recovery": {
                    "policy": "checkpoint",
                    "source_job_id": "job-checkpoint-source",
                    "paths": ["results"],
                    "generation": "periodic-00000004",
                },
            }),
        )
        instance = SimpleNamespace(
            instance_id="dryrun:i-unproven-checkpoint",
            platform="dryrun",
            public_ip="198.51.100.21",
            private_ip="10.0.0.21",
            tags={
                "ElasticAgentJob": job_id,
                "ElasticAgentController": (
                    manager.account_binding_store.controller_id
                ),
                "ElasticAgentShardIndex": "0",
            },
        )
        collect = AsyncMock()

        async def quiesce(
            _driver, _worker_id, _job_id, _spec,
        ):
            return None

        async def reject_install(
            _driver, _worker_id, _job_id, _spec, _shard_index,
        ):
            raise RuntimeError(
                "recovery transfer was not durably committed"
            )

        monkeypatch.setattr(
            ManagerFleetDriver, "quiesce_recovered_worker", quiesce,
        )
        monkeypatch.setattr(
            ManagerFleetDriver,
            "reconcile_recovery_install",
            reject_install,
        )
        monkeypatch.setattr(ManagerFleetDriver, "collect", collect)

        with pytest.raises(
            RuntimeError, match="not durably committed",
        ):
            await manager._collect_recovered_unbound(instance)

        collect.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_recovered_fanout_summary_merges_every_shard_atomically(
        self, tmp_config, provider,
    ):
        from elastic_agent.core.job_spec import JobSpec
        from elastic_agent.core.job_spec_store import load_job_spec_journal
        from elastic_agent.core.manager_fleet_driver import ManagerFleetDriver

        job_id = "job-recovered-fanout"
        manager = ElasticAgentManager(tmp_config, provider)
        await manager.start()
        source = JobSpec.model_validate({
            "name": "recovered-fanout",
            "setup": {
                "repo": "https://github.com/example/bench.git",
                "resolved_commit": "a" * 40,
            },
            "run": {"command": "capture"},
            "fanout": {"workers": 2, "shard_by": "shard_index"},
            "collect": {"paths": ["results"]},
        })
        await manager._persist_batch_job_spec(job_id, source)
        await manager._update_batch_job_state(job_id, "running")
        try:
            await asyncio.gather(*(
                manager._merge_recovered_terminal_worker(
                    job_id=job_id,
                    worker_id=f"worker-{shard}",
                    shard_index=shard,
                    collected=True,
                    collection_error=None,
                    worker_released=False,
                )
                for shard in range(2)
            ))
            partial = load_job_spec_journal(
                tmp_config.registry.path, job_id,
            )["terminal_summary"]
            assert partial["done"] is False
            assert partial["cleanup_pending"] == 2

            await asyncio.gather(*(
                manager._merge_recovered_terminal_worker(
                    job_id=job_id,
                    worker_id=f"worker-{shard}",
                    shard_index=shard,
                    collected=None,
                    collection_error=None,
                    worker_released=True,
                )
                for shard in reversed(range(2))
            ))
            terminal = load_job_spec_journal(
                tmp_config.registry.path, job_id,
            )["terminal_summary"]
            assert terminal["done"] is True
            assert terminal["cleanup_pending"] == 0
            assert [
                worker["shard_index"]
                for worker in terminal["terminal_workers"]
            ] == [0, 1]
            assert all(
                worker["final_collected"]
                and worker["worker_released"]
                and worker["collection_error"] is None
                for worker in terminal["terminal_workers"]
            )

            target = JobSpec.model_validate({
                "name": "resume",
                "setup": {
                    "repo": source.setup.repo,
                    "resolved_commit": source.setup.resolved_commit,
                },
                "run": {"command": "resume"},
                "fanout": {"workers": 2, "shard_by": "shard_index"},
                "recovery": {
                    "policy": "legacy_final_collection",
                    "source_job_id": job_id,
                    "paths": ["results"],
                },
            })
            with pytest.raises(
                RuntimeError, match="legacy mutable.*disabled",
            ):
                ManagerFleetDriver._validate_recovery_contract(
                    load_job_spec_journal(
                        tmp_config.registry.path, job_id,
                    ),
                    source,
                    target,
                    source_quiescent=True,
                )
        finally:
            await manager.stop()

    @pytest.mark.asyncio
    async def test_restart_does_not_trust_previous_process_unbound_ownership(
        self, tmp_config, provider
    ):
        from elastic_agent.core.job_spec_store import (
            load_unbound_launch_intents,
        )

        previous = ElasticAgentManager(tmp_config, provider)
        await previous.start()
        record = (
            await previous.scale_out(
                tags={"ElasticAgentJob": "job-previous-process"},
            )
        )[0]
        assert (
            record.instance_id
            in previous._current_unbound_instance_ids
        )
        assert record.metadata == {
            "job_id": "job-previous-process",
            "controller_id": (
                previous.account_binding_store.controller_id
            ),
            "shard_index": 0,
        }
        assert load_unbound_launch_intents(
            tmp_config.registry.path,
            previous.account_binding_store.controller_id,
        ) == {}
        await previous.stop()

        visible = list(provider._instances.values())
        provider.list_instances = AsyncMock(side_effect=[[], visible])
        real_get_instance = provider.get_instance
        exact_attempts = 0

        async def eventually_visible_exact(instance_id):
            nonlocal exact_attempts
            exact_attempts += 1
            if exact_attempts == 1:
                raise InstanceNotFoundError("not visible by exact id yet")
            return await real_get_instance(instance_id)

        provider.get_instance = eventually_visible_exact
        restarted = ElasticAgentManager(tmp_config, provider)
        assert restarted._current_unbound_instance_ids == set()
        await restarted.start()
        try:
            # Full registry/event publication does not discard the durable
            # ownership fence. A first eventually-consistent miss after a
            # crash must therefore remain quarantined.
            assert len(provider._instances) == 1
            assert restarted.binding_recovery_ready is False
            assert restarted._recovery_unbound_registry_scans == {
                record.node_id: 29
            }
            recovery_task = restarted._binding_recovery_task
            if recovery_task is not None:
                recovery_task.cancel()
                await recovery_task
                restarted._binding_recovery_task = None
            await restarted._recover_bound_resources_once()

            assert provider._instances == {}
            recovered = await restarted.registry.get(record.node_id)
            assert recovered is None
            assert restarted.binding_recovery_ready is True
            assert load_unbound_launch_intents(
                tmp_config.registry.path,
                restarted.account_binding_store.controller_id,
            ) == {}
        finally:
            await restarted.stop()

    @pytest.mark.asyncio
    async def test_terminal_job_unbound_intent_survives_second_restart(
        self, tmp_config, provider
    ):
        from elastic_agent.core.job_spec import JobSpec
        from elastic_agent.core.job_spec_store import (
            load_unbound_launch_intents,
        )

        previous = ElasticAgentManager(tmp_config, provider)
        await previous.start()
        await previous._persist_batch_job_spec(
            "job-failed-before-scan",
            JobSpec.model_validate({
                "name": "failed-before-recovery-scan",
                "run": {"command": "true"},
                "account": {"mode": "none"},
            }),
        )
        await previous._update_batch_job_state(
            "job-failed-before-scan", "launching",
        )
        real_create = provider.create_instance

        async def timeout_after_acceptance(config):
            await real_create(config)
            raise TimeoutError("cloud accepted before SDK timeout")

        provider.create_instance = timeout_after_acceptance
        previous._ensure_binding_recovery_task = MagicMock()
        with pytest.raises(TimeoutError, match="SDK timeout"):
            await previous.scale_out(
                tags={"ElasticAgentJob": "job-failed-before-scan"},
            )
        await previous._update_batch_job_state(
            "job-failed-before-scan",
            "failed",
            {"state": "failed", "done": True},
        )
        journal_path = (
            Path(tmp_config.registry.path).with_name("specs")
            / "job-failed-before-scan.json"
        )
        assert json.loads(
            journal_path.read_text(encoding="utf-8")
        )["submission_state"] == "failed"
        assert load_unbound_launch_intents(
            tmp_config.registry.path,
            previous.account_binding_store.controller_id,
        ) == {"job-failed-before-scan": 1}
        await previous.stop()

        # A fresh process initially misses the eventually-consistent cloud row.
        visible = list(provider._instances.values())
        provider.list_instances = AsyncMock(side_effect=[[], visible])
        restarted = ElasticAgentManager(tmp_config, provider)
        restarted._collect_recovered_unbound = AsyncMock()
        real_wait_terminated = restarted.binding_manager.wait_instance_terminated
        restarted.binding_manager.wait_instance_terminated = AsyncMock(
            side_effect=real_wait_terminated
        )
        await restarted.start()
        try:
            assert restarted.binding_recovery_ready is False
            assert len(provider._instances) == 1
            assert restarted._unbound_launch_intent_counts == {
                "job-failed-before-scan": 1
            }

            # Avoid waiting for the production polling interval; the second
            # successful scan sees, collects, and confirms termination.
            recovery_task = restarted._binding_recovery_task
            if recovery_task is not None:
                recovery_task.cancel()
                await recovery_task
                restarted._binding_recovery_task = None
            await restarted._recover_bound_resources_once()

            assert provider._instances == {}
            restarted._collect_recovered_unbound.assert_awaited_once()
            restarted.binding_manager.wait_instance_terminated.assert_awaited_once()
            assert restarted.binding_recovery_ready is True
            assert restarted._unbound_launch_intent_counts == {}
            assert load_unbound_launch_intents(
                tmp_config.registry.path,
                restarted.account_binding_store.controller_id,
            ) == {}
            assert json.loads(
                journal_path.read_text(encoding="utf-8")
            )["submission_state"] == "failed"
        finally:
            await restarted.stop()

    @pytest.mark.asyncio
    async def test_startup_rejects_unsafe_unbound_launch_intent_job_id(
        self, tmp_config, provider
    ):
        previous = ElasticAgentManager(tmp_config, provider)
        await previous.start()
        controller_id = previous.account_binding_store.controller_id
        await previous.stop()

        intent_path = Path(tmp_config.registry.path).with_name(
            "unbound-launches.json"
        )
        intent_path.write_text(
            json.dumps({
                "version": 1,
                "controller_id": controller_id,
                "jobs": {"../escape": 1},
            }),
            encoding="utf-8",
        )
        restarted = ElasticAgentManager(tmp_config, provider)
        with pytest.raises(ValueError, match="invalid unbound launch intent"):
            await restarted.start()
        assert restarted._started is False
        assert restarted._binding_lock_fd is None

        # Repairing the corrupt journal makes the controller immediately
        # startable; failure did not trust the path or retain the leader lock.
        intent_path.unlink()
        clean = ElasticAgentManager(tmp_config, provider)
        await clean.start()
        await clean.stop()

    @pytest.mark.asyncio
    async def test_start_stop(self, manager):
        await manager.start()
        assert manager._started is True
        await manager.stop()
        assert manager._started is False

    @pytest.mark.asyncio
    async def test_registry_loaded_on_start(self, manager):
        await manager.start()
        nodes = await manager.registry.list_all()
        assert nodes == []
        await manager.stop()

    @pytest.mark.asyncio
    async def test_startup_never_adopts_unmanaged_controller_tagged_instance(
        self, tmp_config, provider
    ):
        manager = ElasticAgentManager(tmp_config, provider)
        instance = await provider.create_instance(InstanceConfig(
            instance_type="t3.small",
            image_id="ami-test",
            key_pair_name="key",
            tags={
                # A provider/mock may ignore the requested ManagedBy filter.
                # Controller/Job tags alone are not destructive authority.
                "ElasticAgentController": (
                    manager.account_binding_store.controller_id
                ),
                "ElasticAgentJob": "job-not-owned",
            },
        ))

        await manager.start()
        try:
            assert await provider.get_instance(instance.instance_id) is instance
            assert manager._recovery_unbound_instances == {}
            assert manager.binding_recovery_ready is True
        finally:
            await manager.stop()
            await provider.terminate_instance(instance.instance_id)

    @pytest.mark.asyncio
    async def test_binding_store_has_single_manager_leader(self, tmp_config, provider):
        first = ElasticAgentManager(tmp_config, provider)
        second = ElasticAgentManager(tmp_config, provider)

        await first.start()
        try:
            with pytest.raises(RuntimeError, match="another ElasticAgentManager"):
                await second.start()
        finally:
            await first.stop()

        # Releasing the first Manager's lock makes the same durable store
        # available immediately; the failed start did not poison ``second``.
        await second.start()
        assert second._started is True
        await second.stop()

    @pytest.mark.asyncio
    async def test_startup_releases_attached_lease_but_retains_eip(
        self, tmp_config
    ):
        provider = DryRunProvider()
        previous = ElasticAgentManager(tmp_config, provider)
        await previous.start()
        lease = await previous.binding_manager.reserve(
            "acct-1", job_id="interrupted-job", region="dryrun-region"
        )
        instance = await provider.create_instance(
            InstanceConfig(
                instance_type="t3.small",
                image_id="ami-test",
                key_pair_name="test-key",
                tags={
                    "ManagedBy": "elastic-agent",
                    "ElasticAgentController": (
                        previous.account_binding_store.controller_id
                    ),
                    "ElasticAgentAccount": "acct-1",
                    "ElasticAgentJob": lease.job_id,
                    "ElasticAgentLease": lease.lease_id,
                },
            )
        )
        await previous.registry.add(
            NodeRecord(
                node_id=instance.instance_id,
                instance_id=instance.instance_id,
                platform=instance.platform,
                status=NodeStatus.READY,
                metadata={"lease_id": lease.lease_id},
            )
        )
        attached = await previous.binding_manager.attach_instance(
            lease.lease_id,
            instance.instance_id,
            worker_id=instance.instance_id,
        )
        binding = await previous.binding_manager.get_binding("acct-1")
        allocation_id = binding.eip_allocation_id
        assert attached.state == LeaseState.ATTACHED
        await previous.stop()

        restarted = ElasticAgentManager(tmp_config, provider)
        await restarted.start()
        try:
            await _wait_for_binding_recovery(restarted)
            recovered = await restarted.binding_manager.get_lease(lease.lease_id)
            assert recovered.state == LeaseState.RELEASED
            assert recovered.eip_detached is True
            assert recovered.instance_terminated is True
            assert provider.instances[instance.instance_id].state == InstanceState.TERMINATED
            assert await restarted.registry.get(instance.instance_id) is None
            assert (await provider.describe_eip(allocation_id)).instance_id is None
            assert len(provider.get_operations("release_eip")) == 0
            assert restarted.binding_recovery_ready is True
        finally:
            await restarted.stop()

    @pytest.mark.asyncio
    async def test_startup_collects_legacy_manager_distribute_eip_job(
        self, tmp_config, monkeypatch
    ):
        from elastic_agent.core.job_spec import JobSpec
        from elastic_agent.core.manager_fleet_driver import ManagerFleetDriver

        job_id = "legacy-manager-distribute-eip"
        provider = DryRunProvider()
        previous = ElasticAgentManager(tmp_config, provider)
        await previous.start()
        await previous._persist_batch_job_spec(
            job_id,
            JobSpec.model_validate({
                "name": "legacy-manager-distribute-eip",
                "setup": {"target_dir": "/srv/legacy-eip-job"},
                "run": {"command": "must-never-be-replayed"},
                "account": {
                    "mode": "worker_local_login",
                    "binding": "eip",
                },
                "collect": {"paths": ["results"]},
            }),
        )
        await previous._update_batch_job_state(job_id, "running")
        lease = await previous.binding_manager.reserve(
            "acct-legacy",
            job_id=job_id,
            region="dryrun-region",
        )
        instance = await provider.create_instance(InstanceConfig(
            instance_type="t3.small",
            image_id="ami-test",
            key_pair_name="test-key",
            tags={
                "ManagedBy": "elastic-agent",
                "ElasticAgentController": (
                    previous.account_binding_store.controller_id
                ),
                "ElasticAgentAccount": "acct-legacy",
                "ElasticAgentLease": lease.lease_id,
                "ElasticAgentJob": job_id,
            },
        ))
        await previous.registry.add(NodeRecord(
            node_id=instance.instance_id,
            instance_id=instance.instance_id,
            platform=instance.platform,
            status=NodeStatus.READY,
            public_ip=instance.public_ip,
            private_ip=instance.private_ip,
            metadata={"job_id": job_id, "lease_id": lease.lease_id},
        ))
        await previous.binding_manager.attach_instance(
            lease.lease_id,
            instance.instance_id,
            worker_id=instance.instance_id,
        )
        await previous.stop()
        _rewrite_persisted_account_mode(
            tmp_config.registry.path,
            job_id,
            "manager_distribute",
        )

        collected: list[tuple[str, str]] = []

        async def record_collect(_driver, worker_id, recovery_spec, recovered_job_id):
            assert recovered_job_id == job_id
            assert recovery_spec.name == "legacy-manager-distribute-eip"
            assert recovery_spec.setup.target_dir == "/srv/legacy-eip-job"
            assert recovery_spec.collect.paths == ["results"]
            assert not hasattr(recovery_spec, "run")
            assert not hasattr(recovery_spec, "account")
            collected.append((worker_id, recovered_job_id))

        async def quiesce(_driver, worker_id, recovered_job_id, spec):
            assert worker_id == instance.instance_id
            assert recovered_job_id == job_id
            assert spec.setup.target_dir == "/srv/legacy-eip-job"

        monkeypatch.setattr(
            ManagerFleetDriver,
            "quiesce_recovered_worker",
            quiesce,
        )
        monkeypatch.setattr(ManagerFleetDriver, "collect", record_collect)
        restarted = ElasticAgentManager(tmp_config, provider)
        await restarted.start()
        try:
            await _wait_for_binding_recovery(restarted)
            recovered = await restarted.binding_manager.get_lease(lease.lease_id)
            assert collected == [(instance.instance_id, job_id)]
            assert recovered.recovery_collection_attempted is True
            assert recovered.recovery_collected is True
            assert recovered.recovery_collection_error is None
            assert recovered.state == LeaseState.RELEASED
            assert provider.instances[instance.instance_id].state == (
                InstanceState.TERMINATED
            )
        finally:
            await restarted.stop()

    @pytest.mark.asyncio
    async def test_direct_eip_launch_persists_spec_before_crash_and_recovery_collects(
        self, tmp_config, monkeypatch,
    ):
        """The Python API must have the same recovery journal as REST submit."""
        from elastic_agent.core.credential_pool import AccountDefinition
        from elastic_agent.core.job_spec import JobSpec
        from elastic_agent.core.manager_fleet_driver import ManagerFleetDriver

        tmp_config.provider.type = "aws"
        tmp_config.provider.aws.region = "dryrun-region"
        monkeypatch.setenv(
            "ELASTIC_AGENT_MANAGER_URL", "wss://manager.example/ws/runtime"
        )
        provider = DryRunProvider()
        previous = ElasticAgentManager(tmp_config, provider)
        await previous.start()
        await previous.account_store.add(AccountDefinition(
            id="acct-direct",
            email="direct@example.com",
            email_token="mailbox-token",
        ))
        spec = JobSpec.model_validate({
            "name": "direct-eip-crash",
            "run": {"command": "echo run"},
            "account": {
                "mode": "worker_local_login",
                "binding": "eip",
                "ids": ["acct-direct"],
            },
            "fanout": {"workers": 1, "region": "dryrun-region"},
            "collect": {"paths": ["results"]},
        })

        orchestrator = previous.batch
        real_reserve = orchestrator._driver._bound_reserve
        crashed: dict[str, object] = {}

        async def crash_after_instance(job_id, slot, job_spec, account_id=""):
            assignment = await real_reserve(
                job_id, slot, job_spec, account_id
            )
            records = await previous.scale_out(
                count=1,
                name_prefix="crash-window",
                tags=assignment.instance_tags(job_id),
            )
            assignment = await orchestrator._driver.attach_bound(
                records[0].node_id, assignment
            )
            path = Path(tmp_config.registry.path).with_name("specs") / f"{job_id}.json"
            # This assertion is deliberately inside the first reservation hook:
            # the spec must predate the lease and every later cloud side effect.
            assert json.loads(path.read_text(encoding="utf-8"))["spec"]["name"] == (
                "direct-eip-crash"
            )
            crashed.update(
                lease_id=assignment.lease_id,
                instance_id=records[0].instance_id,
                job_id=job_id,
            )
            # Raising before returning the assignment models a hard process loss:
            # the orchestrator cannot compensate a lease it never received.
            raise RuntimeError("simulated Manager crash after attached lease")

        orchestrator._driver._bound_reserve = crash_after_instance
        job = await orchestrator.launch(spec)
        assert "simulated Manager crash" in (job.error or "")
        assert crashed
        await previous.stop()

        collected: list[tuple[str, str, str]] = []

        async def record_recovery_collect(_driver, worker_id, recovered_spec, job_id):
            instance = provider.instances[str(crashed["instance_id"])]
            assert instance.state != InstanceState.TERMINATED
            collected.append((worker_id, recovered_spec.name, job_id))

        async def quiesce(_driver, worker_id, recovered_job_id, spec):
            assert worker_id == str(crashed["instance_id"])
            assert recovered_job_id == str(crashed["job_id"])
            assert spec.name == "direct-eip-crash"

        monkeypatch.setattr(
            ManagerFleetDriver,
            "quiesce_recovered_worker",
            quiesce,
        )
        monkeypatch.setattr(
            ManagerFleetDriver, "collect", record_recovery_collect
        )
        restarted = ElasticAgentManager(tmp_config, provider)
        await restarted.start()
        try:
            await _wait_for_binding_recovery(restarted)
            recovered = await restarted.binding_manager.get_lease(
                str(crashed["lease_id"])
            )
            assert collected == [(
                str(crashed["instance_id"]),
                "direct-eip-crash",
                str(crashed["job_id"]),
            )]
            assert recovered.recovery_collection_attempted is True
            assert recovered.recovery_collection_error is None
            assert recovered.state == LeaseState.RELEASED
            assert provider.instances[str(crashed["instance_id"])].state == (
                InstanceState.TERMINATED
            )
        finally:
            await restarted.stop()

    @pytest.mark.asyncio
    async def test_startup_waits_for_reserved_lease_instance_to_become_visible(
        self, tmp_config
    ):
        provider = DryRunProvider()
        previous = ElasticAgentManager(tmp_config, provider)
        await previous.start()
        lease = await previous.binding_manager.reserve(
            "acct-1", job_id="crashed-launch", region="dryrun-region"
        )
        instance = await provider.create_instance(
            InstanceConfig(
                instance_type="t3.small",
                image_id="ami-test",
                key_pair_name="test-key",
                tags={
                    "ManagedBy": "elastic-agent",
                    "ElasticAgentController": (
                        previous.account_binding_store.controller_id
                    ),
                    "ElasticAgentAccount": "acct-1",
                    "ElasticAgentJob": lease.job_id,
                    "ElasticAgentLease": lease.lease_id,
                },
            )
        )
        await previous.stop()

        real_list_instances = provider.list_instances
        provider.list_instances = AsyncMock(
            side_effect=[[], await real_list_instances()]
        )
        restarted = ElasticAgentManager(tmp_config, provider)
        await restarted.start()
        try:
            # The first successful but empty scan is not proof that the
            # RunInstances call never happened: EC2's control plane is
            # eventually consistent, so EIP jobs remain quarantined.
            after_first_scan = await restarted.binding_manager.get_lease(
                lease.lease_id
            )
            assert after_first_scan.state == LeaseState.RESERVED
            assert after_first_scan.instance_id is None
            assert restarted.binding_recovery_ready is False
            api_account = SimpleNamespace(
                id="cloudrouter-recovery",
                email="CloudRouter recovery",
                group="standard",
                enabled=True,
                auth_kind="agent_api",
                supports_agent_type=lambda value: value == "codex",
            )
            restarted.agent_api_store.list = AsyncMock(
                return_value=[api_account]
            )
            restarted.agent_api_store.fetch_usage = AsyncMock()
            restarted.agent_api_store.availability_decision = MagicMock(
                return_value={"available": True}
            )
            blocked = await restarted.account_allocator.reserve(
                "job-during-recovery",
                "standard",
                account_id=api_account.id,
                agent_type="codex",
            )
            assert blocked is None
            restarted.agent_api_store.list.assert_not_awaited()

            # Drive the next pass directly so this test does not sleep for the
            # production scan interval. The immutable cloud tags reconnect the
            # instance to its durable lease, then normal release tears it down.
            await restarted._recover_bound_resources_once()

            recovered = await restarted.binding_manager.get_lease(lease.lease_id)
            assert recovered.state == LeaseState.RELEASED
            assert recovered.instance_id == instance.instance_id
            assert recovered.instance_terminated is True
            assert provider.instances[instance.instance_id].state == InstanceState.TERMINATED
            assert restarted.binding_recovery_ready is True
            assert provider.list_instances.await_count == 2
            admitted = await restarted.account_allocator.reserve(
                "job-after-recovery",
                "standard",
                account_id=api_account.id,
                agent_type="codex",
            )
            assert admitted.account is api_account
        finally:
            await restarted.stop()


# ------------------------------------------------------------------
# Tests: scale_out
# ------------------------------------------------------------------


class TestScaleOut:
    @pytest.mark.asyncio
    async def test_unbound_create_never_reaches_cloud_without_durable_intent(
        self, manager, provider
    ):
        await manager.start()
        real_create = provider.create_instance
        provider.create_instance = AsyncMock(side_effect=real_create)
        manager._begin_unbound_launch_intent = AsyncMock(
            side_effect=OSError("intent fsync failed")
        )

        with pytest.raises(OSError, match="fsync failed"):
            await manager.scale_out(
                tags={"ElasticAgentJob": "job-no-journal-no-create"},
            )

        provider.create_instance.assert_not_awaited()
        assert provider._instances == {}
        await manager.stop()

    @pytest.mark.asyncio
    async def test_partial_scale_out_failure_terminates_every_created_instance(
        self, manager, provider
    ):
        await manager.start()
        real_create = provider.create_instance
        attempts = 0

        async def fail_second(config):
            nonlocal attempts
            attempts += 1
            if attempts == 2:
                raise RuntimeError("second create failed")
            return await real_create(config)

        provider.create_instance = fail_second
        with pytest.raises(RuntimeError, match="second create failed"):
            await manager.scale_out(count=3)

        assert provider._instances == {}
        nodes = await manager.registry.list_all()
        assert len(nodes) == 1
        assert nodes[0].status == NodeStatus.TERMINATED
        await manager.stop()

    @pytest.mark.asyncio
    async def test_current_unbound_compensation_failure_enters_live_recovery(
        self, manager, provider
    ):
        await manager.start()
        manager.registry.add = AsyncMock(
            side_effect=RuntimeError("registry write failed")
        )
        real_terminate = provider.terminate_instance
        terminate_attempts = 0

        async def fail_first_termination(instance_id):
            nonlocal terminate_attempts
            terminate_attempts += 1
            if terminate_attempts == 1:
                raise RuntimeError("transient terminate failure")
            await real_terminate(instance_id)

        provider.terminate_instance = fail_first_termination
        with pytest.raises(RuntimeError, match="registry write failed"):
            await manager.scale_out(
                count=1,
                tags={"ElasticAgentJob": "job-live-recovery"},
            )

        assert len(provider._instances) == 1
        instance = next(iter(provider._instances.values()))
        assert instance.instance_id in manager._recovery_unbound_instances
        assert manager.binding_recovery_ready is False
        assert manager._binding_recovery_task is not None

        # Drive the scheduled live-recovery pass without sleeping for the
        # production retry interval.
        await manager._recover_bound_resources_once()
        assert provider._instances == {}
        assert instance.instance_id not in manager._recovery_unbound_instances
        assert terminate_attempts == 2
        await manager.stop()

    @pytest.mark.asyncio
    async def test_published_worker_compensation_retry_does_not_consume_other_intent(
        self, manager, provider
    ):
        await manager.start()
        real_create = provider.create_instance
        create_attempts = 0

        async def fail_second_before_acceptance(config):
            nonlocal create_attempts
            create_attempts += 1
            if create_attempts == 2:
                raise RuntimeError("second create rejected")
            return await real_create(config)

        provider.create_instance = fail_second_before_acceptance
        real_terminate = provider.terminate_instance
        terminate_attempts = 0

        async def fail_first_termination(instance_id):
            nonlocal terminate_attempts
            terminate_attempts += 1
            if terminate_attempts == 1:
                raise RuntimeError("known worker termination failed")
            await real_terminate(instance_id)

        provider.terminate_instance = fail_first_termination
        manager._ensure_binding_recovery_task = MagicMock()
        with pytest.raises(RuntimeError, match="second create rejected"):
            await manager.scale_out(
                count=2,
                tags={"ElasticAgentJob": "job-partial-known"},
            )

        assert len(provider._instances) == 1
        known = next(iter(provider._instances.values()))
        assert known.instance_id in manager._resolved_unbound_instance_ids
        # Only the rejected second create remains uncertain.
        assert manager._unbound_launch_intent_counts == {
            "job-partial-known": 1
        }

        await manager._recover_bound_resources_once()

        assert provider._instances == {}
        assert manager._unbound_launch_intent_counts == {
            "job-partial-known": 1
        }
        assert manager._recovery_unbound_instances == {}
        # The known, already-published worker was retried and destroyed but
        # did not consume the distinct no-id intent.
        assert (
            manager._recovery_unbound_launch_scans[
                "job-partial-known"
            ]
            > 0
        )
        manager._recovery_unbound_launch_scans[
            "job-partial-known"
        ] = 1
        await manager._recover_bound_resources_once()
        assert manager._unbound_launch_intent_counts == {}
        assert manager.binding_recovery_ready is True
        await manager.stop()

    @pytest.mark.asyncio
    async def test_unbound_timeout_after_cloud_acceptance_scans_until_visible(
        self, manager, provider
    ):
        await manager.start()
        real_create = provider.create_instance

        async def timeout_after_create(config):
            await real_create(config)
            raise TimeoutError("SDK timed out after cloud acceptance")

        provider.create_instance = timeout_after_create
        manager._ensure_binding_recovery_task = MagicMock()
        with pytest.raises(TimeoutError, match="cloud acceptance"):
            await manager.scale_out(
                tags={"ElasticAgentJob": "job-unbound-timeout"},
            )

        assert len(provider._instances) == 1
        assert (
            manager._recovery_unbound_launch_scans[
                "job-unbound-timeout"
            ]
            > 1
        )
        assert manager._binding_recovery_scan_pending is True
        assert manager.binding_recovery_ready is False
        manager._ensure_binding_recovery_task.assert_called_once_with()

        # Model EC2 eventual consistency: the first controller-tag scan misses
        # the accepted request, while the next pass can see it.
        real_list_instances = provider.list_instances
        visible = await real_list_instances()
        provider.list_instances = AsyncMock(
            side_effect=[[], visible],
        )
        manager._recovery_unbound_launch_scans[
            "job-unbound-timeout"
        ] = 2

        await manager._recover_bound_resources_once()
        assert len(provider._instances) == 1
        assert manager._binding_recovery_scan_pending is True

        await manager._recover_bound_resources_once()
        assert provider._instances == {}
        assert manager._recovery_unbound_launch_scans == {}
        assert manager.binding_recovery_ready is True
        await manager.stop()

    @pytest.mark.asyncio
    async def test_unbound_cancel_after_cloud_acceptance_uses_live_recovery(
        self, manager, provider
    ):
        await manager.start()
        real_create = provider.create_instance
        cloud_created = asyncio.Event()

        async def create_then_block(config):
            await real_create(config)
            cloud_created.set()
            await asyncio.Future()

        provider.create_instance = create_then_block
        manager._ensure_binding_recovery_task = MagicMock()
        scale_task = asyncio.create_task(manager.scale_out(
            tags={"ElasticAgentJob": "job-unbound-cancel"},
        ))
        await cloud_created.wait()
        scale_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await scale_task

        assert len(provider._instances) == 1
        initial_scans = manager._recovery_unbound_launch_scans[
            "job-unbound-cancel"
        ]
        assert initial_scans > 1
        assert manager._binding_recovery_scan_pending is True
        assert manager.binding_recovery_ready is False
        manager._ensure_binding_recovery_task.assert_called_once_with()

        await manager._recover_bound_resources_once()
        assert provider._instances == {}
        # The durable counter proves this Job had exactly one unresolved
        # create.  Confirmed termination resolves it immediately rather than
        # keeping Agent API admission blocked for the remaining quarantine.
        assert manager._recovery_unbound_launch_scans == {}
        assert manager._unbound_launch_intent_counts == {}
        assert manager.binding_recovery_ready is True
        await manager.stop()

    @pytest.mark.asyncio
    async def test_same_job_unbound_intents_resolve_each_confirmed_instance(
        self, manager, provider
    ):
        from elastic_agent.core.job_spec_store import (
            load_unbound_launch_intents,
        )

        await manager.start()
        real_create = provider.create_instance

        async def timeout_after_create(config):
            await real_create(config)
            raise TimeoutError("accepted without returning an instance")

        provider.create_instance = timeout_after_create
        manager._ensure_binding_recovery_task = MagicMock()
        for _ in range(2):
            with pytest.raises(TimeoutError, match="without returning"):
                await manager.scale_out(
                    tags={"ElasticAgentJob": "job-two-uncertain"},
                )

        assert len(provider._instances) == 2
        assert manager._unbound_launch_intent_counts == {
            "job-two-uncertain": 2
        }
        assert load_unbound_launch_intents(
            manager.config.registry.path,
            manager.account_binding_store.controller_id,
        ) == {"job-two-uncertain": 2}

        await manager._recover_bound_resources_once()

        assert provider._instances == {}
        assert manager._recovery_unbound_launch_scans == {}
        assert manager._unbound_launch_intent_counts == {}
        assert load_unbound_launch_intents(
            manager.config.registry.path,
            manager.account_binding_store.controller_id,
        ) == {}
        assert manager.binding_recovery_ready is True
        await manager.stop()

    @pytest.mark.asyncio
    async def test_pending_recovery_scan_skips_current_unbound_job(
        self, manager, provider
    ):
        await manager.start()
        record = (
            await manager.scale_out(
                tags={"ElasticAgentJob": "job-current-oauth"},
            )
        )[0]
        manager._collect_recovered_unbound = AsyncMock()
        manager._binding_recovery_scan_pending = True
        manager._binding_recovery_scans_remaining = 1

        await manager._recover_bound_resources_once()

        assert await provider.get_instance(record.instance_id) is not None
        assert await manager.registry.get(record.node_id) is not None
        assert (
            record.instance_id
            in manager._current_unbound_instance_ids
        )
        assert (
            record.instance_id
            not in manager._recovery_unbound_instances
        )
        manager._collect_recovered_unbound.assert_not_awaited()

        await manager.scale_in([record.node_id], force=True)
        assert (
            record.instance_id
            not in manager._current_unbound_instance_ids
        )
        await manager.stop()

    @pytest.mark.asyncio
    async def test_recovery_waits_for_unbound_registry_publication(
        self, manager, provider
    ):
        await manager.start()
        publication_entered = asyncio.Event()
        publish = asyncio.Event()
        real_emit = manager.event_bus.emit

        async def block_node_creating(event_type, source, data=None):
            if event_type == "NODE_CREATING":
                publication_entered.set()
                await publish.wait()
            await real_emit(event_type, source, data)

        manager.event_bus.emit = block_node_creating
        real_list_instances = provider.list_instances
        provider.list_instances = AsyncMock(
            side_effect=real_list_instances
        )
        manager._collect_recovered_unbound = AsyncMock()
        manager._binding_recovery_scan_pending = True
        manager._binding_recovery_scans_remaining = 1

        scale_task = asyncio.create_task(manager.scale_out(
            tags={"ElasticAgentJob": "job-publication-fence"},
        ))
        await publication_entered.wait()
        # The registry write has completed, but scale_out still owns the
        # lifecycle fence until NODE_CREATING publication also completes.
        assert len(await manager.registry.list_all()) == 1
        scans_before_recovery = provider.list_instances.await_count
        recovery_task = asyncio.create_task(
            manager._recover_bound_resources_once()
        )
        await asyncio.sleep(0)

        assert recovery_task.done() is False
        assert provider.list_instances.await_count == scans_before_recovery

        publish.set()
        records = await scale_task
        await recovery_task
        record = records[0]
        assert await provider.get_instance(record.instance_id) is not None
        assert await manager.registry.get(record.node_id) is not None
        manager._collect_recovered_unbound.assert_not_awaited()

        await manager.scale_in([record.node_id], force=True)
        await manager.stop()

    @pytest.mark.asyncio
    async def test_finalizing_failed_unbound_worker_stays_with_current_job(
        self, manager, provider
    ):
        await manager.start()
        record = (
            await manager.scale_out(
                tags={"ElasticAgentJob": "job-current-failed"},
            )
        )[0]
        await manager.registry.update(
            record.node_id, status=NodeStatus.FAILED
        )
        manager._collect_recovered_unbound = AsyncMock()
        manager._binding_recovery_scan_pending = True
        manager._binding_recovery_scans_remaining = 1

        await manager._recover_bound_resources_once()

        assert await provider.get_instance(record.instance_id) is not None
        assert (
            record.instance_id
            in manager._current_unbound_instance_ids
        )
        assert (
            record.instance_id
            not in manager._recovery_unbound_instances
        )
        manager._collect_recovered_unbound.assert_not_awaited()

        # The normal terminal path explicitly relinquishes process ownership
        # as it starts confirmed teardown.
        await manager.scale_in([record.node_id], force=True)
        assert (
            record.instance_id
            not in manager._current_unbound_instance_ids
        )
        await manager.stop()

    @pytest.mark.asyncio
    async def test_scale_out_single(self, manager):
        await manager.start()
        records = await manager.scale_out(count=1)
        assert len(records) == 1
        rec = records[0]
        assert rec.status == NodeStatus.CREATING
        assert rec.auth_token is not None
        assert rec.node_id.startswith("dryrun:")
        all_nodes = await manager.registry.list_all()
        assert len(all_nodes) == 1
        await manager.stop()

    @pytest.mark.asyncio
    async def test_unbound_intent_clears_after_durable_publication(
        self, manager
    ):
        from elastic_agent.core.job_spec_store import (
            load_unbound_launch_intents,
        )

        await manager.start()
        record = (
            await manager.scale_out(
                tags={"ElasticAgentJob": "job-normal-lifecycle"},
            )
        )[0]

        assert manager._unbound_launch_intent_counts == {}
        assert load_unbound_launch_intents(
            manager.config.registry.path,
            manager.account_binding_store.controller_id,
        ) == {}

        await manager.scale_in([record.node_id], force=True)

        assert manager._unbound_launch_intent_counts == {}
        assert load_unbound_launch_intents(
            manager.config.registry.path,
            manager.account_binding_store.controller_id,
        ) == {}
        await manager.stop()

    @pytest.mark.asyncio
    async def test_scale_out_disk_and_spot_reach_instance_config(self, manager, provider):
        await manager.start()
        await manager.scale_out(count=1, disk_gb=80, spot=True)
        assert provider._last_config.root_disk_size_gb == 80
        assert provider._last_config.spot is True
        await manager.stop()

    @pytest.mark.asyncio
    async def test_scale_out_disk_default_when_unset(self, manager, provider):
        await manager.start()
        await manager.scale_out(count=1)  # no disk_gb → InstanceConfig default (40)
        assert provider._last_config.root_disk_size_gb == 40
        assert provider._last_config.spot is False
        await manager.stop()

    @pytest.mark.asyncio
    async def test_spot_is_honored_when_disk_override_is_unset(
        self, manager, provider
    ):
        await manager.start()
        await manager.scale_out(count=1, disk_gb=0, spot=True)
        assert provider._last_config.root_disk_size_gb == 40
        assert provider._last_config.spot is True
        await manager.stop()

    @pytest.mark.asyncio
    async def test_configured_instance_limit_rejects_oversized_scale_out(
        self, manager, provider
    ):
        manager.config.provider.type = "aws"
        manager.config.provider.aws.max_instances = 1
        await manager.start()

        with pytest.raises(RuntimeError, match="configured maximum is 1"):
            await manager.scale_out(count=2)

        assert provider._counter == 0
        await manager.stop()

    @pytest.mark.asyncio
    async def test_bound_fanout_over_limit_allocates_no_eips_or_leases(
        self, manager, provider, monkeypatch,
    ):
        from elastic_agent.core.job_spec import JobSpec

        manager.config.provider.type = "aws"
        manager.config.provider.aws.max_instances = 5
        monkeypatch.setenv(
            "ELASTIC_AGENT_MANAGER_URL", "wss://manager.example/ws/runtime"
        )
        await manager.start()
        spec = JobSpec.model_validate({
            "name": "too-wide-eip-job",
            "run": {"command": "echo run"},
            "account": {"mode": "worker_local_login", "binding": "eip"},
            "fanout": {"workers": 20},
        })

        job = await manager.batch.launch(spec)

        assert "configured maximum is 5" in (job.error or "")
        assert provider._counter == 0
        assert await manager.binding_manager.list_bindings() == []
        assert await manager.binding_manager.list_leases() == []
        assert manager._instance_capacity_holds == {}
        assert manager._inflight_instance_creates == 0
        await manager.stop()

    @pytest.mark.asyncio
    async def test_instance_limit_counts_concurrent_inflight_creates(
        self, manager, provider
    ):
        manager.config.provider.type = "aws"
        manager.config.provider.aws.max_instances = 1
        await manager.start()
        entered = asyncio.Event()
        release = asyncio.Event()
        real_create = provider.create_instance

        async def slow_create(config):
            entered.set()
            await release.wait()
            return await real_create(config)

        provider.create_instance = slow_create
        first = asyncio.create_task(manager.scale_out())
        await entered.wait()

        second = asyncio.create_task(manager.scale_out())
        await asyncio.sleep(0.05)
        assert second.done() is False
        release.set()
        assert len(await first) == 1
        with pytest.raises(RuntimeError, match="configured maximum is 1"):
            await second
        await manager.stop()

    @pytest.mark.asyncio
    async def test_instance_limit_deduplicates_visible_inflight_create(
        self, manager, provider
    ):
        manager.config.provider.type = "aws"
        manager.config.provider.aws.max_instances = 2
        await manager.start()
        publication_entered = asyncio.Event()
        release_publication = asyncio.Event()
        real_emit = manager.event_bus.emit

        async def block_first_publication(event_type, source, data=None):
            if event_type == "NODE_CREATING" and not publication_entered.is_set():
                publication_entered.set()
                await release_publication.wait()
            return await real_emit(event_type, source, data)

        manager.event_bus.emit = block_first_publication
        first = asyncio.create_task(manager.scale_out())
        await publication_entered.wait()

        second = asyncio.create_task(manager.scale_out())
        await asyncio.sleep(0.05)
        assert second.done() is False
        assert manager._inflight_instance_creates == 2

        release_publication.set()
        first_records, second_records = await asyncio.gather(first, second)
        assert len(first_records) == len(second_records) == 1
        assert manager._inflight_instance_creates == 0
        assert manager._inflight_visible_instance_ids == set()
        await manager.stop()

    @pytest.mark.asyncio
    async def test_scale_out_merges_bound_lifecycle_tags(self, manager, provider):
        await manager.start()
        await manager.account_binding_store.upsert_binding(
            AccountBinding(
                account_id="acct-1",
                eip_allocation_id="eipalloc-test",
                eip_ip="203.0.113.10",
                state=BindingState.READY,
            )
        )
        lease = await manager.account_binding_store.reserve_lease(
            "acct-1", job_id="job-1"
        )
        records = await manager.scale_out(
            count=1,
            name_prefix="bound-job-0",
            tags={
                "ElasticAgentJob": "job-1",
                "ElasticAgentAccount": "acct-1",
                "ElasticAgentLease": lease.lease_id,
            },
        )
        assert provider._last_config.tags == {
            "ManagedBy": "elastic-agent",
            "Name": "bound-job-0-0",
            "ElasticAgentJob": "job-1",
            "ElasticAgentAccount": "acct-1",
            "ElasticAgentLease": lease.lease_id,
            "ElasticAgentController": manager.account_binding_store.controller_id,
        }
        persisted = await manager.account_binding_store.get_lease(lease.lease_id)
        assert persisted.instance_id == records[0].instance_id
        assert persisted.worker_id == records[0].node_id
        assert persisted.state == LeaseState.ATTACHING
        assert records[0].metadata == {
            "job_id": "job-1",
            "account_id": "acct-1",
            "lease_id": lease.lease_id,
            "controller_id": manager.account_binding_store.controller_id,
        }
        await manager.stop()

    @pytest.mark.asyncio
    async def test_scale_out_multiple(self, manager):
        await manager.start()
        records = await manager.scale_out(count=3)
        assert len(records) == 3
        ids = {r.node_id for r in records}
        assert len(ids) == 3
        all_nodes = await manager.registry.list_all()
        assert len(all_nodes) == 3
        await manager.stop()

    @pytest.mark.asyncio
    async def test_scale_out_event_emitted(self, manager):
        await manager.start()
        events = []

        async def handler(event_type, worker_id, data):
            events.append((event_type, worker_id))

        manager.event_bus.subscribe("NODE_CREATING", handler)
        await manager.scale_out(count=2)
        assert len(events) == 2
        assert all(et == "NODE_CREATING" for et, _ in events)
        await manager.stop()


# ------------------------------------------------------------------
# Tests: scale_in
# ------------------------------------------------------------------


class TestScaleIn:
    @pytest.mark.asyncio
    async def test_scale_in_force(self, manager, provider):
        await manager.start()
        manager.binding_manager.wait_instance_terminated = AsyncMock()
        records = await manager.scale_out(count=2)
        node_ids = [r.node_id for r in records]
        terminated = await manager.scale_in(node_ids, force=True)
        assert set(terminated) == set(node_ids)
        for nid in node_ids:
            node = await manager.registry.get(nid)
            assert node.status == NodeStatus.TERMINATED
        assert (
            manager.binding_manager.wait_instance_terminated.await_count
            == len(records)
        )
        assert {
            call.args[0]
            for call in manager.binding_manager.wait_instance_terminated.await_args_list
        } == {record.instance_id for record in records}
        await manager.stop()

    @pytest.mark.asyncio
    async def test_scale_in_drain(self, manager):
        await manager.start()
        records = await manager.scale_out(count=1)
        nid = records[0].node_id
        terminated = await manager.scale_in([nid], force=False)
        assert terminated == []
        node = await manager.registry.get(nid)
        assert node.status == NodeStatus.DRAINING
        await manager.stop()

    @pytest.mark.asyncio
    async def test_scale_in_nonexistent_node(self, manager):
        await manager.start()
        terminated = await manager.scale_in(["nonexistent"], force=True)
        assert terminated == []
        await manager.stop()

    @pytest.mark.asyncio
    async def test_force_scale_in_attempts_every_node_after_one_failure(
        self, manager, provider
    ):
        await manager.start()
        records = await manager.scale_out(count=2)
        attempted = []

        async def terminate(instance_id):
            attempted.append(instance_id)
            if len(attempted) == 1:
                raise RuntimeError("transient terminate failure")
            native = instance_id.split(":", 1)[-1]
            provider._instances.pop(native, None)

        provider.terminate_instance = terminate
        with pytest.raises(RuntimeError, match="failed to scale in 1 worker"):
            await manager.scale_in([record.node_id for record in records], force=True)

        assert attempted == [record.instance_id for record in records]
        assert (await manager.registry.get(records[1].node_id)).status == NodeStatus.TERMINATED
        await manager.stop()

    @pytest.mark.asyncio
    async def test_force_scale_in_bound_node_uses_lease_cleanup(
        self, manager, provider
    ):
        await manager.start()
        node = NodeRecord(
            node_id="dryrun:i-bound",
            instance_id="dryrun:i-bound",
            platform="dryrun",
            status=NodeStatus.READY,
            metadata={"lease_id": "lease-1"},
        )
        await manager.registry.add(node)
        manager._cleanup_bound_lease = AsyncMock()
        provider.terminate_instance = AsyncMock()

        assert await manager.scale_in([node.node_id], force=True) == [node.node_id]
        manager._cleanup_bound_lease.assert_awaited_once_with(
            node.node_id,
            "lease-1",
            reason="worker force-scaled in by administrator",
        )
        provider.terminate_instance.assert_not_awaited()
        await manager.stop()


# ------------------------------------------------------------------
# Tests: resume_node
# ------------------------------------------------------------------


class TestResumeNode:
    @pytest.mark.asyncio
    async def test_resume_node_waits_for_stopping_instance(self, manager, provider):
        await manager.start()
        records = await manager.scale_out(count=1)
        node = records[0]
        await manager.registry.update(node.node_id, status=NodeStatus.STOPPED)

        stopping = Instance(
            instance_id=node.instance_id,
            platform="dryrun",
            native_id=node.instance_id.split(":", 1)[-1],
            state=InstanceState.STOPPING,
        )
        stopped = stopping.model_copy(update={"state": InstanceState.STOPPED})
        provider.get_instance = AsyncMock(side_effect=[stopping, stopped])
        provider.start_instance = AsyncMock()

        with patch("asyncio.sleep", new=AsyncMock()) as sleep_mock:
            result = await manager.resume_node(node.node_id)

        assert result is not None
        assert result.status == NodeStatus.CREATING
        assert provider.get_instance.await_count == 2
        sleep_mock.assert_awaited_once()
        provider.start_instance.assert_awaited_once_with(node.instance_id)
        await manager.stop()


# ------------------------------------------------------------------
# Tests: drain_node
# ------------------------------------------------------------------


class TestDrainNode:
    @pytest.mark.asyncio
    async def test_drain_existing(self, manager):
        await manager.start()
        records = await manager.scale_out(count=1)
        nid = records[0].node_id
        result = await manager.drain_node(nid)
        assert result is True
        node = await manager.registry.get(nid)
        assert node.status == NodeStatus.DRAINING
        await manager.stop()

    @pytest.mark.asyncio
    async def test_drain_nonexistent(self, manager):
        await manager.start()
        result = await manager.drain_node("no-such-node")
        assert result is False
        await manager.stop()


# ------------------------------------------------------------------
# Tests: get_node_status
# ------------------------------------------------------------------


class TestGetNodeStatus:
    @pytest.mark.asyncio
    async def test_existing_node(self, manager):
        await manager.start()
        records = await manager.scale_out(count=1)
        nid = records[0].node_id
        info = await manager.get_node_status(nid)
        assert info is not None
        assert info["node_id"] == nid
        assert info["status"] == "creating"
        assert info["ws_connected"] is False
        await manager.stop()

    @pytest.mark.asyncio
    async def test_nonexistent_node(self, manager):
        await manager.start()
        info = await manager.get_node_status("nope")
        assert info is None
        await manager.stop()


# ------------------------------------------------------------------
# Tests: event routing
# ------------------------------------------------------------------


class TestEventRouting:
    @pytest.mark.asyncio
    async def test_on_worker_message_emits_to_bus(self, manager):
        await manager.start()
        events = []

        async def handler(event_type, worker_id, data):
            events.append((event_type, worker_id))

        manager.event_bus.subscribe("HEARTBEAT", handler)

        from elastic_agent.core.protocols.messages import HeartbeatMessage

        msg = HeartbeatMessage(uptime_seconds=100)
        await manager._on_worker_message("w-1", msg)
        assert len(events) == 1
        assert events[0] == ("HEARTBEAT", "w-1")
        await manager.stop()

    @pytest.mark.asyncio
    async def test_archive_job_task_log_fsyncs_then_releases_buffer(self, manager):
        task_id = "job-log-test:w-1:abcdef"
        manager.log_event_parser.process_log_event("w-1", {
            "task_id": task_id,
            "stream": "stderr",
            "data": "actionable error",
            "timestamp": "2026-07-25T12:00:00+00:00",
            "parsed": None,
        })

        archived = await manager.archive_job_task_log(
            "job-log-test",
            "w-1",
            {"task_id": task_id, "exit_code": 1, "event_id": "exit-1"},
        )

        assert archived is True
        assert manager.log_event_parser.buffer_size(task_id) == 0
        snapshots = manager.job_log_store.read_job("job-log-test")
        assert snapshots[0]["entries"][0]["data"] == "actionable error"
        assert snapshots[0]["exit"]["exit_code"] == 1

    @pytest.mark.asyncio
    async def test_archive_failure_releases_bounded_buffer_without_blocking_cleanup(
        self, manager, monkeypatch,
    ):
        task_id = "job-log-failure:w-1:abcdef"
        manager.log_event_parser.process_log_event("w-1", {
            "task_id": task_id,
            "stream": "stdout",
            "data": "retain me",
            "parsed": None,
        })

        def fail_snapshot(**_kwargs):
            raise OSError("disk full")

        monkeypatch.setattr(manager.job_log_store, "save_snapshot", fail_snapshot)
        archived = await manager.archive_job_task_log(
            "job-log-failure",
            "w-1",
            {"task_id": task_id, "exit_code": 1},
        )

        assert archived is False
        assert manager.log_event_parser.buffer_size(task_id) == 0

    @pytest.mark.asyncio
    async def test_empty_manager_buffer_recovers_bounded_worker_ndjson(
        self, manager, monkeypatch,
    ):
        task_id = "job-replay:w-1:abcdef"
        manager.registry.get = AsyncMock(return_value=SimpleNamespace(
            private_ip="10.0.0.8",
            public_ip="198.51.100.8",
        ))
        captured = {}

        class FakeSSHExecutor:
            def __init__(self, host, **kwargs):
                captured["host"] = host
                captured["kwargs"] = kwargs

            async def execute(self, command, timeout):
                captured["command"] = command
                captured["timeout"] = timeout
                return 0, (
                    "100\npartial-json\n"
                    + json.dumps({
                        "task_id": task_id,
                        "stream": "stderr",
                        "data": "recovered detail",
                        "timestamp": "2026-07-25T12:00:00+00:00",
                        "parsed": None,
                    })
                    + "\n"
                ), ""

        monkeypatch.setattr(
            "elastic_agent.core.bootstrap.SSHExecutor", FakeSSHExecutor,
        )

        archived = await manager.archive_job_task_log(
            "job-replay",
            "w-1",
            {"task_id": task_id, "exit_code": 1, "event_id": "replayed"},
        )

        assert archived is True
        assert captured["host"] == "198.51.100.8"
        assert "tail -c 8388608" in captured["command"]
        assert captured["timeout"] == 10
        snapshot = manager.job_log_store.read_job("job-replay")[0]
        assert snapshot["entries"][0]["data"] == "recovered detail"

    @pytest.mark.asyncio
    async def test_worker_tail_recovery_marks_byte_truncation(
        self, manager, monkeypatch,
    ):
        task_id = "job-truncated:w-1:abcdef"
        manager.registry.get = AsyncMock(return_value=SimpleNamespace(
            private_ip="10.0.0.8",
            public_ip="198.51.100.8",
        ))

        class FakeSSHExecutor:
            def __init__(self, *_args, **_kwargs):
                pass

            async def execute(self, _command, timeout):
                assert timeout == 10
                return 0, (
                    f"{8 * 1024 * 1024 + 1}\n"
                    + json.dumps({
                        "task_id": task_id,
                        "stream": "stderr",
                        "data": "tail only",
                    })
                    + "\n"
                ), ""

        monkeypatch.setattr(
            "elastic_agent.core.bootstrap.SSHExecutor", FakeSSHExecutor,
        )

        assert await manager.archive_job_task_log(
            "job-truncated",
            "w-1",
            {"task_id": task_id, "exit_code": 1},
        )
        snapshot = manager.job_log_store.read_job("job-truncated")[0]
        assert snapshot["truncated"] is True

    @pytest.mark.asyncio
    async def test_on_worker_connect_emits(self, manager):
        await manager.start()
        events = []

        async def handler(event_type, worker_id, data):
            events.append(event_type)

        manager.event_bus.subscribe("WORKER_CONNECTED", handler)
        await manager._on_worker_connect("w-1")
        assert "WORKER_CONNECTED" in events
        await manager.stop()

    @pytest.mark.asyncio
    async def test_on_worker_disconnect_emits(self, manager):
        await manager.start()
        events = []

        async def handler(event_type, worker_id, data):
            events.append(event_type)

        manager.event_bus.subscribe("WORKER_DISCONNECTED", handler)
        await manager._on_worker_disconnect("w-1")
        assert "WORKER_DISCONNECTED" in events
        await manager.stop()

    @pytest.mark.asyncio
    async def test_bound_disconnect_grace_uses_safe_cleanup(self, manager):
        await manager.start()
        node = NodeRecord(
            node_id="w-bound",
            instance_id="dryrun:i-bound",
            platform="dryrun",
            status=NodeStatus.READY,
            metadata={"lease_id": "lease-bound"},
        )
        await manager.registry.add(node)
        manager._cleanup_bound_lease = AsyncMock()
        manager.binding_manager.get_lease = AsyncMock(
            return_value=MagicMock(state="attached")
        )

        with patch(
            "elastic_agent.manager.manager.BOUND_DISCONNECT_GRACE_SECONDS", 0
        ):
            await manager._on_worker_disconnect(node.node_id)
            for _ in range(3):
                await asyncio.sleep(0)

        manager._cleanup_bound_lease.assert_awaited_once()
        assert manager._cleanup_bound_lease.await_args.args == (
            node.node_id, "lease-bound"
        )
        assert "did not reconnect" in manager._cleanup_bound_lease.await_args.kwargs["reason"]
        await manager.stop()

    @pytest.mark.asyncio
    async def test_bound_batch_disconnect_does_not_destroy_supervised_run(
        self, manager,
    ):
        await manager.start()
        node = NodeRecord(
            node_id="w-bound-job",
            instance_id="dryrun:i-bound-job",
            platform="dryrun",
            status=NodeStatus.READY,
            metadata={"lease_id": "lease-bound-job"},
        )
        await manager.registry.add(node)
        manager._cleanup_bound_lease = AsyncMock()
        orchestrator = manager.batch
        orchestrator.job_id_for_worker = MagicMock(
            return_value="job-long-running",
        )

        with patch(
            "elastic_agent.manager.manager.BOUND_DISCONNECT_GRACE_SECONDS", 0
        ):
            await manager._on_worker_disconnect(node.node_id)
            for _ in range(3):
                await asyncio.sleep(0)

        manager._cleanup_bound_lease.assert_not_awaited()
        assert node.node_id not in manager._bound_disconnect_tasks
        await manager.stop()

    @pytest.mark.asyncio
    async def test_bound_reconnect_cancels_pending_disconnect_cleanup(self, manager):
        await manager.start()
        node = NodeRecord(
            node_id="w-bound",
            instance_id="dryrun:i-bound",
            platform="dryrun",
            status=NodeStatus.READY,
            metadata={"lease_id": "lease-bound"},
        )
        await manager.registry.add(node)
        manager._cleanup_bound_node = AsyncMock()

        await manager._on_worker_disconnect(node.node_id)
        assert node.node_id in manager._bound_disconnect_tasks
        await manager._on_worker_connect(node.node_id)
        await asyncio.sleep(0)

        manager._cleanup_bound_node.assert_not_awaited()
        assert node.node_id not in manager._bound_disconnect_tasks
        await manager.stop()


class TestEipRecoveryHardening:
    @staticmethod
    def _lease_tags(manager, lease, account_id):
        return {
            "ElasticAgentJob": lease.job_id,
            "ElasticAgentAccount": account_id,
            "ElasticAgentLease": lease.lease_id,
        }

    @pytest.mark.asyncio
    async def test_timeout_after_create_is_found_and_terminated(self, tmp_config):
        provider = DryRunProvider()
        manager = ElasticAgentManager(tmp_config, provider)
        await manager.start()
        lease = await manager.binding_manager.reserve(
            "acct-timeout", job_id="job-timeout"
        )
        real_create = provider.create_instance

        async def timeout_after_create(config):
            await real_create(config)
            raise TimeoutError("SDK timed out after acceptance")

        provider.create_instance = timeout_after_create
        manager._ensure_binding_recovery_task = MagicMock()
        with pytest.raises(TimeoutError):
            await manager.scale_out(
                tags=self._lease_tags(manager, lease, "acct-timeout")
            )

        uncertain = await manager.binding_manager.get_lease(lease.lease_id)
        assert uncertain.launch_uncertain is True
        assert uncertain.instance_id is None

        await manager._recover_bound_resources_once()

        recovered = await manager.binding_manager.get_lease(lease.lease_id)
        assert recovered.state == LeaseState.RELEASED
        assert recovered.instance_terminated is True
        assert len(provider.get_operations("terminate")) == 1
        await manager.stop()

    @pytest.mark.asyncio
    async def test_cancelled_create_after_cloud_acceptance_enters_live_recovery(
        self, tmp_config
    ):
        """Cancellation while RunInstances is returning must not lose the EC2."""
        provider = DryRunProvider()
        manager = ElasticAgentManager(tmp_config, provider)
        await manager.start()
        lease = await manager.binding_manager.reserve(
            "acct-cancelled-create", job_id="job-cancelled-create"
        )
        real_create = provider.create_instance
        cloud_created = asyncio.Event()

        async def create_then_block(config):
            await real_create(config)
            cloud_created.set()
            await asyncio.Future()

        provider.create_instance = create_then_block
        # Drive recovery explicitly so the assertions cannot race its background
        # task. The production notifier still has to be called by scale_out.
        manager._ensure_binding_recovery_task = MagicMock()
        scale_task = asyncio.create_task(manager.scale_out(
            tags=self._lease_tags(
                manager, lease, "acct-cancelled-create"
            )
        ))
        await cloud_created.wait()
        scale_task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await scale_task

        uncertain = await manager.binding_manager.get_lease(lease.lease_id)
        assert uncertain.launch_uncertain is True
        assert uncertain.instance_id is None
        assert lease.lease_id in manager._recovery_lease_ids
        assert manager._binding_recovery_scan_pending is True
        manager._ensure_binding_recovery_task.assert_called_once_with()
        assert len(provider.instances) == 1

        await manager._recover_bound_resources_once()

        recovered = await manager.binding_manager.get_lease(lease.lease_id)
        assert recovered.state == LeaseState.RELEASED
        assert recovered.instance_terminated is True
        assert len(provider.get_operations("terminate")) == 1
        assert manager.binding_recovery_ready is True
        await manager.stop()

    @pytest.mark.asyncio
    async def test_cancelled_after_instance_id_persisted_terminates_exact_instance(
        self, tmp_config
    ):
        """Cancellation after RunInstances returns uses the known-id compensation."""
        provider = DryRunProvider()
        manager = ElasticAgentManager(tmp_config, provider)
        await manager.start()
        lease = await manager.binding_manager.reserve(
            "acct-cancelled-persist", job_id="job-cancelled-persist"
        )
        real_begin_attach = manager.account_binding_store.begin_attach
        instance_persisted = asyncio.Event()

        async def persist_then_block(lease_id, instance_id, worker_id=""):
            await real_begin_attach(lease_id, instance_id, worker_id)
            instance_persisted.set()
            await asyncio.Future()

        manager.account_binding_store.begin_attach = persist_then_block
        scale_task = asyncio.create_task(manager.scale_out(
            tags=self._lease_tags(
                manager, lease, "acct-cancelled-persist"
            )
        ))
        await instance_persisted.wait()
        scale_task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await scale_task

        compensated = await manager.binding_manager.get_lease(lease.lease_id)
        assert compensated.instance_id
        assert compensated.launch_uncertain is False
        assert provider.instances[compensated.instance_id].state == (
            InstanceState.TERMINATED
        )
        assert len(provider.get_operations("terminate")) == 1
        assert lease.lease_id not in manager._recovery_lease_ids

        # Settle the durable reservation so this test also proves compensation
        # remains compatible with the idempotent normal release path.
        manager.account_binding_store.begin_attach = real_begin_attach
        released = await manager.binding_manager.release(lease.lease_id)
        assert released.state == LeaseState.RELEASED
        await manager.stop()

    @pytest.mark.asyncio
    async def test_timeout_before_create_clears_uncertainty_after_full_scan(
        self, tmp_config
    ):
        provider = DryRunProvider()
        manager = ElasticAgentManager(tmp_config, provider)
        await manager.start()
        lease = await manager.binding_manager.reserve(
            "acct-no-create", job_id="job-no-create"
        )

        async def timeout_before_create(_config):
            raise TimeoutError("request never left process")

        provider.create_instance = timeout_before_create
        manager._ensure_binding_recovery_task = MagicMock()
        with pytest.raises(TimeoutError):
            await manager.scale_out(
                tags=self._lease_tags(manager, lease, "acct-no-create")
            )

        manager._binding_recovery_scans_remaining = 1
        await manager._recover_bound_resources_once()

        recovered = await manager.binding_manager.get_lease(lease.lease_id)
        assert recovered.state == LeaseState.RELEASED
        assert recovered.launch_uncertain is False
        assert provider.get_operations("terminate") == []
        await manager.stop()

    @pytest.mark.asyncio
    async def test_live_ambiguity_does_not_tear_down_other_active_lease(
        self, tmp_config
    ):
        provider = DryRunProvider()
        manager = ElasticAgentManager(tmp_config, provider)
        await manager.start()
        live = await manager.binding_manager.reserve(
            "acct-live", job_id="job-live"
        )
        live_record = (await manager.scale_out(
            tags=self._lease_tags(manager, live, "acct-live")
        ))[0]

        ambiguous = await manager.binding_manager.reserve(
            "acct-ambiguous", job_id="job-ambiguous"
        )
        real_create = provider.create_instance

        async def timeout_after_create(config):
            await real_create(config)
            raise TimeoutError("ambiguous")

        provider.create_instance = timeout_after_create
        manager._ensure_binding_recovery_task = MagicMock()
        with pytest.raises(TimeoutError):
            await manager.scale_out(
                tags=self._lease_tags(manager, ambiguous, "acct-ambiguous")
            )
        await manager._recover_bound_resources_once()

        live_after = await manager.binding_manager.get_lease(live.lease_id)
        assert live_after.state == LeaseState.ATTACHING
        assert provider.instances[live_record.instance_id].state != InstanceState.TERMINATED

        await manager.binding_manager.release(live.lease_id)
        await manager.stop()

    @pytest.mark.asyncio
    async def test_foreign_exact_instance_remains_fail_closed_after_quarantine(
        self, tmp_config
    ):
        provider = DryRunProvider()
        previous = ElasticAgentManager(tmp_config, provider)
        await previous.start()
        lease = await previous.binding_manager.reserve(
            "acct-foreign", job_id="job-foreign"
        )
        instance = await provider.create_instance(InstanceConfig(
            instance_type="t3.small",
            image_id="ami-test",
            key_pair_name="test-key",
            tags={
                "ManagedBy": "elastic-agent",
                "ElasticAgentController": "another-controller",
                "ElasticAgentAccount": lease.account_id,
                "ElasticAgentLease": lease.lease_id,
            },
        ))
        await previous.account_binding_store.begin_attach(
            lease.lease_id, instance.instance_id, instance.instance_id
        )
        await previous.stop()

        restarted = ElasticAgentManager(tmp_config, provider)
        await restarted.start()
        task = restarted._binding_recovery_task
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            restarted._binding_recovery_task = None
        restarted._binding_recovery_scans_remaining = 1
        restarted._binding_recovery_scan_pending = True
        await restarted._recover_bound_resources_once()

        still_active = await restarted.binding_manager.get_lease(lease.lease_id)
        assert still_active.state != LeaseState.RELEASED
        assert lease.lease_id in restarted._recovery_unsafe_lease_ids
        assert provider.get_operations("terminate") == []
        await restarted.stop()

    @pytest.mark.asyncio
    async def test_unknown_exact_lookup_never_advances_to_raw_termination(
        self, tmp_config
    ):
        provider = DryRunProvider()
        previous = ElasticAgentManager(tmp_config, provider)
        await previous.start()
        lease = await previous.binding_manager.reserve(
            "acct-unknown", job_id="job-unknown"
        )
        instance = await provider.create_instance(InstanceConfig(
            instance_type="t3.small",
            image_id="ami-test",
            key_pair_name="test-key",
            tags={
                "ManagedBy": "elastic-agent",
                "ElasticAgentController": (
                    previous.account_binding_store.controller_id
                ),
                "ElasticAgentAccount": lease.account_id,
                "ElasticAgentJob": lease.job_id,
                "ElasticAgentLease": lease.lease_id,
            },
        ))
        await previous.account_binding_store.begin_attach(
            lease.lease_id, instance.instance_id, instance.instance_id
        )
        await previous.stop()

        provider.list_instances = AsyncMock(return_value=[])
        provider.get_instance = AsyncMock(
            side_effect=RuntimeError("AWS throttling")
        )
        restarted = ElasticAgentManager(tmp_config, provider)
        await restarted.start()
        task = restarted._binding_recovery_task
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            restarted._binding_recovery_task = None

        restarted._binding_recovery_scans_remaining = 1
        restarted._binding_recovery_scan_pending = True
        await restarted._recover_bound_resources_once()

        current = await restarted.binding_manager.get_lease(lease.lease_id)
        assert current.state != LeaseState.RELEASED
        assert lease.lease_id in restarted._recovery_unknown_lease_ids
        assert restarted._binding_recovery_scans_remaining == 1
        assert provider.get_operations("terminate") == []

        # A later strict controller-tag scan is affirmative evidence. It must
        # clear UNKNOWN and resume normal durable release rather than leaving
        # the instance quarantined forever after one transient API failure.
        provider.list_instances.return_value = [instance]
        provider.get_instance = AsyncMock(return_value=instance)
        restarted._binding_recovery_scan_pending = True
        await restarted._recover_bound_resources_once()

        recovered = await restarted.binding_manager.get_lease(lease.lease_id)
        assert recovered.state == LeaseState.RELEASED
        assert lease.lease_id not in restarted._recovery_unknown_lease_ids
        assert len(provider.get_operations("terminate")) == 1
        await restarted.stop()

    @pytest.mark.asyncio
    async def test_live_reconciler_does_not_kill_runinstances_in_flight(
        self, tmp_config
    ):
        provider = DryRunProvider()
        manager = ElasticAgentManager(tmp_config, provider)
        await manager.start()
        lease = await manager.binding_manager.reserve(
            "acct-inflight", job_id="job-inflight"
        )
        await manager.account_binding_store.update_lease(
            lease.lease_id,
            launch_uncertain=True,
            last_operation="create_instance",
        )
        instance = await provider.create_instance(InstanceConfig(
            instance_type="t3.small",
            image_id="ami-test",
            key_pair_name="test-key",
            tags={
                "ManagedBy": "elastic-agent",
                "ElasticAgentController": manager.account_binding_store.controller_id,
                "ElasticAgentAccount": lease.account_id,
                "ElasticAgentLease": lease.lease_id,
            },
        ))
        await manager.registry.add(NodeRecord(
            node_id=instance.instance_id,
            instance_id=instance.instance_id,
            platform=instance.platform,
            status=NodeStatus.CREATING,
            metadata={
                "controller_id": manager.account_binding_store.controller_id,
                "account_id": lease.account_id,
                "lease_id": lease.lease_id,
            },
        ))

        await manager._on_reconciler_bound_lost(
            instance.instance_id, lease.lease_id
        )

        still_pending = await manager.binding_manager.get_lease(lease.lease_id)
        assert still_pending.launch_uncertain is True
        assert still_pending.instance_id is None
        assert provider.get_operations("terminate") == []

        await provider.terminate_instance(instance.instance_id)
        await manager.account_binding_store.update_lease(
            lease.lease_id, launch_uncertain=False
        )
        await manager.binding_manager.release(lease.lease_id)
        await manager.stop()

    @pytest.mark.asyncio
    async def test_live_reconciler_rejects_mismatched_account_metadata(
        self, tmp_config
    ):
        provider = DryRunProvider()
        manager = ElasticAgentManager(tmp_config, provider)
        await manager.start()
        lease = await manager.binding_manager.reserve(
            "acct-owner", job_id="job-owner"
        )
        instance = await provider.create_instance(InstanceConfig(
            instance_type="t3.small",
            image_id="ami-test",
            key_pair_name="test-key",
            tags={
                "ManagedBy": "elastic-agent",
                "ElasticAgentController": manager.account_binding_store.controller_id,
                "ElasticAgentAccount": "acct-wrong",
                "ElasticAgentLease": lease.lease_id,
            },
        ))
        await manager.account_binding_store.begin_attach(
            lease.lease_id, instance.instance_id, instance.instance_id
        )
        await manager.registry.add(NodeRecord(
            node_id=instance.instance_id,
            instance_id=instance.instance_id,
            platform=instance.platform,
            status=NodeStatus.READY,
            metadata={
                "controller_id": manager.account_binding_store.controller_id,
                "account_id": "acct-wrong",
                "lease_id": lease.lease_id,
            },
        ))

        with pytest.raises(RuntimeError, match="account ownership"):
            await manager._on_reconciler_bound_lost(
                instance.instance_id, lease.lease_id
            )

        assert provider.get_operations("terminate") == []
        await manager.binding_manager.release(lease.lease_id)
        await manager.stop()

    @pytest.mark.asyncio
    async def test_live_reconciler_never_raw_terminates_another_active_lease(
        self, tmp_config
    ):
        provider = DryRunProvider()
        manager = ElasticAgentManager(tmp_config, provider)
        await manager.start()
        lease = await manager.binding_manager.reserve(
            "acct-live-claim", job_id="job-live-claim"
        )
        instance = await provider.create_instance(InstanceConfig(
            instance_type="t3.small",
            image_id="ami-test",
            key_pair_name="test-key",
            tags={
                "ManagedBy": "elastic-agent",
                "ElasticAgentController": (
                    manager.account_binding_store.controller_id
                ),
                "ElasticAgentAccount": lease.account_id,
                "ElasticAgentJob": lease.job_id,
                "ElasticAgentLease": "lease-unknown-tag",
            },
        ))
        await manager.account_binding_store.begin_attach(
            lease.lease_id, instance.instance_id, instance.instance_id
        )

        result = await manager.reconciler.reconcile()

        current = await manager.binding_manager.get_lease(lease.lease_id)
        assert current.state == LeaseState.ATTACHING
        assert result.orphans_adopted == [instance.instance_id]
        assert result.bound_nodes_lost == [instance.instance_id]
        assert await manager.registry.get(instance.instance_id) is not None
        assert provider.get_operations("terminate") == []
        assert provider.get_operations("disassociate_eip") == []

        await manager.binding_manager.release(lease.lease_id)
        await manager.registry.remove(instance.instance_id)
        await manager.stop()

    @pytest.mark.asyncio
    async def test_startup_quarantines_unknown_tag_claimed_by_active_lease(
        self, tmp_config
    ):
        provider = DryRunProvider()
        previous = ElasticAgentManager(tmp_config, provider)
        await previous.start()
        lease = await previous.binding_manager.reserve(
            "acct-startup-claim", job_id="job-startup-claim"
        )
        instance = await provider.create_instance(InstanceConfig(
            instance_type="t3.small",
            image_id="ami-test",
            key_pair_name="test-key",
            tags={
                "ManagedBy": "elastic-agent",
                "ElasticAgentController": (
                    previous.account_binding_store.controller_id
                ),
                "ElasticAgentAccount": lease.account_id,
                "ElasticAgentJob": lease.job_id,
                "ElasticAgentLease": "lease-unknown-tag",
            },
        ))
        await previous.account_binding_store.begin_attach(
            lease.lease_id, instance.instance_id, instance.instance_id
        )
        await previous.stop()

        restarted = ElasticAgentManager(tmp_config, provider)
        await restarted.start()

        current = await restarted.binding_manager.get_lease(lease.lease_id)
        assert current.state == LeaseState.ATTACHING
        assert lease.lease_id in restarted._recovery_unsafe_lease_ids
        assert restarted.binding_recovery_ready is False
        assert provider.get_operations("terminate") == []
        assert provider.get_operations("disassociate_eip") == []
        await restarted.stop()

    @pytest.mark.asyncio
    async def test_startup_quarantines_durable_worker_registry_collision(
        self, tmp_config
    ):
        provider = DryRunProvider()
        previous = ElasticAgentManager(tmp_config, provider)
        await previous.start()
        lease = await previous.binding_manager.reserve(
            "acct-worker-collision", job_id="job-worker-collision"
        )
        instance = await provider.create_instance(InstanceConfig(
            instance_type="t3.small",
            image_id="ami-test",
            key_pair_name="test-key",
            tags={
                "ManagedBy": "elastic-agent",
                "ElasticAgentController": (
                    previous.account_binding_store.controller_id
                ),
                "ElasticAgentAccount": lease.account_id,
                "ElasticAgentJob": lease.job_id,
                "ElasticAgentLease": lease.lease_id,
            },
        ))
        worker_id = "worker-collision"
        await previous.account_binding_store.begin_attach(
            lease.lease_id, instance.instance_id, worker_id
        )
        await previous.registry.add(NodeRecord(
            node_id=worker_id,
            instance_id="dryrun:i-other",
            platform="dryrun",
            status=NodeStatus.READY,
        ))
        await previous.stop()

        restarted = ElasticAgentManager(tmp_config, provider)
        await restarted.start()

        current = await restarted.binding_manager.get_lease(lease.lease_id)
        node = await restarted.registry.get(worker_id)
        assert current.state == LeaseState.ATTACHING
        assert lease.lease_id in restarted._recovery_unsafe_lease_ids
        assert restarted.binding_recovery_ready is False
        assert node is not None
        assert node.instance_id == "dryrun:i-other"
        assert provider.get_operations("terminate") == []
        assert provider.get_operations("disassociate_eip") == []
        await restarted.stop()

    @pytest.mark.asyncio
    async def test_startup_quarantines_cloud_job_tag_mismatch(
        self, tmp_config,
    ):
        provider = DryRunProvider()
        previous = ElasticAgentManager(tmp_config, provider)
        await previous.start()
        lease = await previous.binding_manager.reserve(
            "acct-job-mismatch",
            job_id="job-durable",
        )
        instance = await provider.create_instance(InstanceConfig(
            instance_type="t3.small",
            image_id="ami-test",
            key_pair_name="test-key",
            tags={
                "ManagedBy": "elastic-agent",
                "ElasticAgentController": (
                    previous.account_binding_store.controller_id
                ),
                "ElasticAgentAccount": lease.account_id,
                "ElasticAgentJob": "job-cloud-conflict",
                "ElasticAgentLease": lease.lease_id,
            },
        ))
        await previous.account_binding_store.begin_attach(
            lease.lease_id,
            instance.instance_id,
            instance.instance_id,
        )
        await previous.stop()

        restarted = ElasticAgentManager(tmp_config, provider)
        restarted._collect_recovered_lease = AsyncMock()
        await restarted.start()
        try:
            for _ in range(100):
                if lease.lease_id in restarted._recovery_unsafe_lease_ids:
                    break
                await asyncio.sleep(0.01)
            assert lease.lease_id in restarted._recovery_unsafe_lease_ids
            restarted._collect_recovered_lease.assert_not_awaited()
            assert provider.get_operations("terminate") == []
        finally:
            await restarted.stop()

    @pytest.mark.asyncio
    async def test_startup_quarantines_conflicting_registry_job_identity(
        self, tmp_config,
    ):
        provider = DryRunProvider()
        previous = ElasticAgentManager(tmp_config, provider)
        await previous.start()
        lease = await previous.binding_manager.reserve(
            "acct-registry-job-mismatch",
            job_id="job-durable-registry",
        )
        instance = await provider.create_instance(InstanceConfig(
            instance_type="t3.small",
            image_id="ami-test",
            key_pair_name="test-key",
            tags={
                "ManagedBy": "elastic-agent",
                "ElasticAgentController": (
                    previous.account_binding_store.controller_id
                ),
                "ElasticAgentAccount": lease.account_id,
                "ElasticAgentJob": lease.job_id,
                "ElasticAgentLease": lease.lease_id,
            },
        ))
        await previous.account_binding_store.begin_attach(
            lease.lease_id,
            instance.instance_id,
            instance.instance_id,
        )
        await previous.registry.add(NodeRecord(
            node_id=instance.instance_id,
            instance_id=instance.instance_id,
            platform=instance.platform,
            status=NodeStatus.READY,
            metadata={
                "job_id": "job-registry-conflict",
                "account_id": lease.account_id,
                "lease_id": lease.lease_id,
                "controller_id": (
                    previous.account_binding_store.controller_id
                ),
            },
        ))
        await previous.stop()

        restarted = ElasticAgentManager(tmp_config, provider)
        restarted._collect_recovered_lease = AsyncMock()
        await restarted.start()
        try:
            for _ in range(100):
                if lease.lease_id in restarted._recovery_unsafe_lease_ids:
                    break
                await asyncio.sleep(0.01)
            assert lease.lease_id in restarted._recovery_unsafe_lease_ids
            restarted._collect_recovered_lease.assert_not_awaited()
            node = await restarted.registry.get(instance.instance_id)
            assert node is not None
            assert node.metadata["job_id"] == "job-registry-conflict"
            assert provider.get_operations("terminate") == []
        finally:
            await restarted.stop()

    @pytest.mark.asyncio
    async def test_startup_quarantines_worker_without_durable_instance(
        self, tmp_config
    ):
        provider = DryRunProvider()
        manager = ElasticAgentManager(tmp_config, provider)
        await manager.start()
        lease = await manager.binding_manager.reserve(
            "acct-missing-instance", job_id="job-missing-instance"
        )
        instance = await provider.create_instance(InstanceConfig(
            instance_type="t3.small",
            image_id="ami-test",
            key_pair_name="test-key",
        ))
        await manager.registry.add(NodeRecord(
            node_id="worker-missing-instance",
            instance_id=instance.instance_id,
            platform="dryrun",
            status=NodeStatus.READY,
        ))
        corrupt = lease.model_copy(update={
            "worker_id": "worker-missing-instance",
            "instance_id": None,
        })
        real_get_lease = manager.binding_manager.get_lease

        async def corrupt_get_lease(lease_id):
            if lease_id == lease.lease_id:
                return corrupt.model_copy(deep=True)
            return await real_get_lease(lease_id)

        manager.binding_manager.get_lease = corrupt_get_lease
        manager._recovery_lease_ids.add(lease.lease_id)
        manager._binding_recovery_scan_pending = False

        await manager._recover_bound_resources_once()

        assert lease.lease_id in manager._recovery_unsafe_lease_ids
        assert await manager.registry.get("worker-missing-instance") is not None
        assert provider.get_operations("terminate") == []
        assert provider.get_operations("disassociate_eip") == []

        manager.binding_manager.get_lease = real_get_lease
        manager._recovery_lease_ids.discard(lease.lease_id)
        await manager.binding_manager.release(
            lease.lease_id, expected_lease=lease
        )
        await manager.registry.remove("worker-missing-instance")
        await provider.terminate_instance(instance.instance_id)
        await manager.stop()

    @pytest.mark.asyncio
    async def test_startup_ignores_exact_released_terminated_history(
        self, tmp_config
    ):
        provider = DryRunProvider()
        previous = ElasticAgentManager(tmp_config, provider)
        await previous.start()
        lease = await previous.binding_manager.reserve(
            "acct-startup-history", job_id="job-startup-history"
        )
        instance = await provider.create_instance(InstanceConfig(
            instance_type="t3.small",
            image_id="ami-test",
            key_pair_name="test-key",
            tags={
                "ManagedBy": "elastic-agent",
                "ElasticAgentController": (
                    previous.account_binding_store.controller_id
                ),
                "ElasticAgentAccount": lease.account_id,
                "ElasticAgentJob": lease.job_id,
                "ElasticAgentLease": lease.lease_id,
            },
        ))
        await previous.binding_manager.attach_instance(
            lease.lease_id, instance.instance_id, instance.instance_id
        )
        released = await previous.binding_manager.release(lease.lease_id)
        assert released.state == LeaseState.RELEASED
        terminate_count = len(provider.get_operations("terminate"))
        await previous.stop()

        restarted = ElasticAgentManager(tmp_config, provider)
        await restarted.start()

        assert len(provider.get_operations("terminate")) == terminate_count
        assert await restarted.registry.get(instance.instance_id) is None
        assert restarted.binding_recovery_ready is True
        await restarted.stop()

    @pytest.mark.asyncio
    async def test_live_reconciler_forgets_released_terminated_lease_without_cloud_calls(
        self, tmp_config
    ):
        provider = DryRunProvider()
        manager = ElasticAgentManager(tmp_config, provider)
        await manager.start()
        lease = await manager.binding_manager.reserve(
            "acct-released-history", job_id="job-released-history"
        )
        instance = await provider.create_instance(InstanceConfig(
            instance_type="t3.small",
            image_id="ami-test",
            key_pair_name="test-key",
            tags={
                "ManagedBy": "elastic-agent",
                "ElasticAgentController": (
                    manager.account_binding_store.controller_id
                ),
                "ElasticAgentAccount": lease.account_id,
                "ElasticAgentJob": lease.job_id,
                "ElasticAgentLease": lease.lease_id,
            },
        ))
        await manager.binding_manager.attach_instance(
            lease.lease_id, instance.instance_id, instance.instance_id
        )
        released = await manager.binding_manager.release(lease.lease_id)
        assert released.state == LeaseState.RELEASED
        assert released.instance_id == instance.instance_id
        assert await manager._is_reconciler_bound_released(
            lease.lease_id,
            instance.instance_id,
            lease.account_id,
            lease.job_id,
        ) is True
        assert await manager._is_reconciler_bound_released(
            lease.lease_id,
            "dryrun:i-other",
            lease.account_id,
            lease.job_id,
        ) is False
        for incomplete in (
            released.model_copy(update={"state": LeaseState.ERROR}),
            released.model_copy(update={"eip_detached": False}),
            released.model_copy(update={"instance_terminated": False}),
            released.model_copy(update={
                "worker_cleanup_required": True,
                "worker_cleanup_done": False,
            }),
            released.model_copy(update={"released_at": None}),
        ):
            assert manager._lease_proves_released_instance(
                incomplete,
                instance_id=instance.instance_id,
                account_id=lease.account_id,
                job_id=lease.job_id,
            ) is False
        terminate_count = len(provider.get_operations("terminate"))
        worker_id = instance.instance_id
        await manager.registry.add(NodeRecord(
            node_id=worker_id,
            instance_id=worker_id,
            platform="dryrun",
            status=NodeStatus.TERMINATED,
            metadata={
                "controller_id": manager.account_binding_store.controller_id,
                "account_id": lease.account_id,
                "lease_id": lease.lease_id,
                "job_id": lease.job_id,
            },
        ))

        await manager._on_reconciler_bound_lost(worker_id, lease.lease_id)

        assert await manager.registry.get(worker_id) is None
        assert len(provider.get_operations("terminate")) == terminate_count
        await manager.stop()

    @pytest.mark.asyncio
    async def test_released_history_never_suppresses_conflicting_active_claim(
        self, tmp_config
    ):
        provider = DryRunProvider()
        manager = ElasticAgentManager(tmp_config, provider)
        await manager.start()
        history = await manager.binding_manager.reserve(
            "acct-history", job_id="job-history"
        )
        instance = await provider.create_instance(InstanceConfig(
            instance_type="t3.small",
            image_id="ami-test",
            key_pair_name="test-key",
            tags={
                "ManagedBy": "elastic-agent",
                "ElasticAgentController": (
                    manager.account_binding_store.controller_id
                ),
                "ElasticAgentAccount": history.account_id,
                "ElasticAgentJob": history.job_id,
                "ElasticAgentLease": history.lease_id,
            },
        ))
        await manager.binding_manager.attach_instance(
            history.lease_id, instance.instance_id, instance.instance_id
        )
        released = await manager.binding_manager.release(history.lease_id)
        assert released.state == LeaseState.RELEASED
        terminate_count = len(provider.get_operations("terminate"))
        disassociate_count = len(
            provider.get_operations("disassociate_eip")
        )

        active = await manager.binding_manager.reserve(
            "acct-active-conflict", job_id="job-active-conflict"
        )
        await manager.account_binding_store.begin_attach(
            active.lease_id, instance.instance_id, instance.instance_id
        )

        result = await manager.reconciler.reconcile()

        assert result.orphans_adopted == [instance.instance_id]
        assert result.bound_nodes_lost == [instance.instance_id]
        assert await manager.registry.get(instance.instance_id) is not None
        assert (
            await manager.binding_manager.get_lease(active.lease_id)
        ).state == LeaseState.ATTACHING
        assert len(provider.get_operations("terminate")) == terminate_count
        assert len(
            provider.get_operations("disassociate_eip")
        ) == disassociate_count

        await manager.binding_manager.release(active.lease_id)
        await manager.registry.remove(instance.instance_id)
        await manager.stop()

    @pytest.mark.asyncio
    async def test_orphan_termination_is_attempted_even_if_eip_detach_fails(
        self, tmp_config
    ):
        provider = DryRunProvider()
        manager = ElasticAgentManager(tmp_config, provider)
        await manager.start()
        lease = await manager.binding_manager.reserve(
            "acct-orphan", job_id="job-orphan"
        )
        binding = await manager.binding_manager.get_binding(lease.account_id)
        instance = await provider.create_instance(InstanceConfig(
            instance_type="t3.small",
            image_id="ami-test",
            key_pair_name="test-key",
            tags={
                "ManagedBy": "elastic-agent",
                "ElasticAgentController": manager.account_binding_store.controller_id,
                "ElasticAgentAccount": lease.account_id,
                "ElasticAgentLease": "lease-crashed-before-persist",
            },
        ))
        await provider.associate_eip(
            instance.instance_id, binding.eip_allocation_id
        )
        provider.fail_next("disassociate_eip", RuntimeError("detach failed"))

        with pytest.raises(RuntimeError, match="cleanup incomplete"):
            await manager._detach_then_terminate_orphan(
                instance.instance_id, binding
            )

        assert provider.instances[instance.instance_id].state == InstanceState.TERMINATED
        assert len(provider.get_operations("terminate")) == 1
        await manager.stop()

    @pytest.mark.asyncio
    async def test_orphan_wrong_eip_tags_refuse_detach_but_still_terminate_instance(
        self, tmp_config
    ):
        provider = DryRunProvider()
        manager = ElasticAgentManager(tmp_config, provider)
        await manager.start()
        lease = await manager.binding_manager.reserve(
            "acct-tag-guard", job_id="job-tag-guard"
        )
        binding = await manager.binding_manager.get_binding(lease.account_id)
        instance = await provider.create_instance(InstanceConfig(
            instance_type="t3.small",
            image_id="ami-test",
            key_pair_name="test-key",
            tags={
                "ManagedBy": "elastic-agent",
                "ElasticAgentController": manager.account_binding_store.controller_id,
                "ElasticAgentAccount": lease.account_id,
                "ElasticAgentLease": "lease-orphan",
            },
        ))
        await provider.associate_eip(
            instance.instance_id, binding.eip_allocation_id
        )
        provider.eips[binding.eip_allocation_id].tags["AccountId"] = "foreign"

        with pytest.raises(RuntimeError, match="ownership tags do not match"):
            await manager._detach_then_terminate_orphan(
                instance.instance_id, binding
            )

        assert provider.get_operations("disassociate_eip") == []
        assert provider.instances[instance.instance_id].state == InstanceState.TERMINATED
        assert len(provider.get_operations("terminate")) == 1
        await manager.stop()

    @pytest.mark.asyncio
    async def test_release_error_running_orphan_retries_on_next_reconcile(
        self, tmp_config
    ):
        provider = DryRunProvider()
        manager = ElasticAgentManager(tmp_config, provider)
        await manager.start()
        lease = await manager.binding_manager.reserve(
            "acct-release-retry", job_id="job-release-retry"
        )
        record = (await manager.scale_out(
            tags=self._lease_tags(manager, lease, lease.account_id)
        ))[0]
        await manager.binding_manager.attach_instance(
            lease.lease_id, record.instance_id, worker_id=record.node_id
        )
        provider.fail_next("terminate_instance", RuntimeError("temporary failure"))

        with pytest.raises(RuntimeError, match="temporary failure"):
            await manager.binding_manager.release(lease.lease_id)
        failed = await manager.binding_manager.get_lease(lease.lease_id)
        assert failed.state == LeaseState.ERROR
        assert failed.last_operation == "release"

        # Simulate the registry write being lost with the Manager crash. The
        # strict controller/account/lease tags let the next reconcile adopt the
        # exact instance and retry the durable release transaction.
        await manager.registry.remove(record.node_id)
        await manager.reconciler.reconcile()

        released = await manager.binding_manager.get_lease(lease.lease_id)
        assert released.state == LeaseState.RELEASED
        assert provider.instances[record.instance_id].state == InstanceState.TERMINATED
        assert len(provider.get_operations("terminate")) == 1
        await manager.stop()


class TestManagerShutdownSafety:
    @pytest.mark.asyncio
    async def test_failed_start_cancels_recovery_before_unlock(
        self, tmp_config, provider
    ):
        manager = ElasticAgentManager(tmp_config, provider)
        cancelled = asyncio.Event()

        async def recovery_loop():
            try:
                await manager._shutdown_event.wait()
            finally:
                cancelled.set()

        async def initialize():
            manager._binding_recovery_task = asyncio.create_task(recovery_loop())
            await asyncio.sleep(0)

        manager._initialize_binding_recovery = initialize
        manager.task_registry.recover = AsyncMock(
            side_effect=RuntimeError("startup failed after recovery began")
        )

        with pytest.raises(RuntimeError, match="startup failed"):
            await manager.start()
        assert cancelled.is_set()
        assert manager._binding_lock_fd is None

        replacement = ElasticAgentManager(tmp_config, provider)
        await replacement.start()
        await replacement.stop()

    @pytest.mark.asyncio
    async def test_controller_lock_is_held_until_batch_shutdown_finishes(
        self, tmp_config, provider
    ):
        first = ElasticAgentManager(tmp_config, provider)
        await first.start()
        entered = asyncio.Event()
        release = asyncio.Event()

        class SlowBatch:
            async def shutdown(self):
                entered.set()
                await release.wait()

        first._batch = SlowBatch()
        stop_task = asyncio.create_task(first.stop())
        await entered.wait()

        second = ElasticAgentManager(tmp_config, provider)
        with pytest.raises(RuntimeError, match="another ElasticAgentManager"):
            await second.start()

        release.set()
        await stop_task
        await second.start()
        await second.stop()

    @pytest.mark.asyncio
    async def test_cancelled_stop_still_holds_lock_until_shutdown_finishes(
        self, tmp_config, provider
    ):
        first = ElasticAgentManager(tmp_config, provider)
        await first.start()
        entered = asyncio.Event()
        release = asyncio.Event()

        class SlowBatch:
            async def shutdown(self):
                entered.set()
                await release.wait()

        first._batch = SlowBatch()
        stop_task = asyncio.create_task(first.stop())
        await entered.wait()
        stop_task.cancel()
        await asyncio.sleep(0)

        second = ElasticAgentManager(tmp_config, provider)
        with pytest.raises(RuntimeError, match="another ElasticAgentManager"):
            await second.start()
        assert not stop_task.done()

        release.set()
        with pytest.raises(asyncio.CancelledError):
            await stop_task
        assert first._binding_lock_fd is None

        await second.start()
        await second.stop()

    @pytest.mark.asyncio
    async def test_cancelled_start_settles_cloud_work_before_unlock(
        self, tmp_config, provider
    ):
        first = ElasticAgentManager(tmp_config, provider)
        entered = asyncio.Event()
        release = asyncio.Event()

        async def slow_initialize():
            entered.set()
            await release.wait()

        first._initialize_binding_recovery = slow_initialize
        start_task = asyncio.create_task(first.start())
        await entered.wait()
        start_task.cancel()
        await asyncio.sleep(0)

        second = ElasticAgentManager(tmp_config, provider)
        with pytest.raises(RuntimeError, match="another ElasticAgentManager"):
            await second.start()
        assert not start_task.done()

        release.set()
        with pytest.raises(asyncio.CancelledError):
            await start_task
        assert first._binding_lock_fd is None

        await second.start()
        await second.stop()
