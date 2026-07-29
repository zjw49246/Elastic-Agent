"""Focused tests for ManagerFleetDriver result durability and teardown."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from elastic_agent.core.job_spec import JobSpec
from elastic_agent.core.job_spec_store import (
    persist_job_spec,
    update_job_state,
)
from elastic_agent.core.manager_fleet_driver import ManagerFleetDriver

pytestmark = pytest.mark.asyncio


class FakeRegistry:
    def __init__(self):
        self.nodes = {
            "worker-a": SimpleNamespace(public_ip="203.0.113.10", private_ip="10.0.0.10"),
            "worker-b": SimpleNamespace(public_ip="203.0.113.11", private_ip="10.0.0.11"),
        }

    async def get(self, worker_id):
        return self.nodes.get(worker_id)


class FakeConnectionManager:
    def __init__(self):
        self.stopped = []

    async def stop_process(self, worker_id, task_id, sig="SIGTERM"):
        self.stopped.append((worker_id, task_id, sig))


class FakeBatch:
    def __init__(self):
        self.job = SimpleNamespace(runs={
            "worker-a": SimpleNamespace(ctx=SimpleNamespace(shard_index=0)),
            "worker-b": SimpleNamespace(ctx=SimpleNamespace(shard_index=1)),
        })

    def get_job(self, job_id):
        return self.job if job_id == "job-1" else None


class FakeManager:
    def __init__(self, tmp_path, *, worker_profile=""):
        self.registry = FakeRegistry()
        self.connection_manager = FakeConnectionManager()
        self.collected_root = str(tmp_path / "collected")
        self._batch = FakeBatch()
        self._s3_uploader = None
        self.scale_in_calls = []
        self.removed_nodes = []
        self.config = SimpleNamespace(
            registry=SimpleNamespace(path=str(tmp_path / "registry.json")),
            server=SimpleNamespace(host="127.0.0.1", port=8080),
            worker=SimpleNamespace(ssh_user="ubuntu"),
            provider=SimpleNamespace(
                type="aws",
                aws=SimpleNamespace(
                    ssh_key_path="/tmp/key",
                    worker_instance_profile=worker_profile,
                ),
                aliyun=SimpleNamespace(ssh_key_path=""),
            ),
        )

    async def scale_in(self, *, node_ids, force):
        self.scale_in_calls.append((node_ids, force))
        return list(node_ids)

    async def remove_node(self, node_id):
        self.removed_nodes.append(node_id)
        self.registry.nodes.pop(node_id, None)
        return True


class FakeProcess:
    returncode = 0

    async def communicate(self):
        return b"", b""


def _spec(tmp_path):
    return JobSpec.model_validate({
        "name": "result-test",
        "setup": {"target_dir": str(tmp_path / "remote-work")},
        "run": {"command": "true"},
        "collect": {"paths": ["results"]},
    })


class FakeCheckpointStore:
    def __init__(self):
        self.restores = []
        self.commits = []

    def restore_checkpoint(self, **kwargs):
        self.restores.append(("checkpoint", kwargs))
        destination = Path(kwargs["destination"])
        (destination / "results").mkdir(parents=True)
        (destination / "results" / "restored.json").write_text("ok")
        return {
            "generation": "g1",
            "metadata": dict(kwargs.get("expected_metadata") or {}),
        }

    def restore_legacy_collection(self, **kwargs):
        self.restores.append(("legacy", kwargs))
        destination = Path(kwargs["destination"])
        (destination / "results").mkdir(parents=True)
        (destination / "results" / "restored.json").write_text("ok")
        return {"collected_at": "now"}

    def commit(self, **kwargs):
        self.commits.append(kwargs)
        return {"generation": "new-generation"}


async def test_local_collect_isolated_by_shard_and_writes_manifest(
    tmp_path, monkeypatch,
):
    monkeypatch.delenv("ELASTIC_AGENT_RESULTS_S3_BUCKET", raising=False)
    calls = []

    async def fake_subprocess(*args, **kwargs):
        calls.append(args)
        return FakeProcess()

    monkeypatch.setattr(
        "elastic_agent.core.manager_fleet_driver.asyncio.create_subprocess_exec",
        fake_subprocess,
    )
    manager = FakeManager(tmp_path)
    driver = ManagerFleetDriver(manager)

    await driver.collect("worker-a", _spec(tmp_path), "job-1")
    await driver.collect("worker-b", _spec(tmp_path), "job-1")

    destinations = [args[-1] for args in calls]
    assert any("job-1/workers/shard-00000/results/" in p for p in destinations)
    assert any("job-1/workers/shard-00001/results/" in p for p in destinations)
    assert len(set(destinations)) == 2
    assert all("--safe-links" in args for args in calls)
    assert all("-azc" in args for args in calls)
    assert any("ubuntu@10.0.0.10:" in args[-2] for args in calls)
    assert any("ubuntu@10.0.0.11:" in args[-2] for args in calls)

    manifest = Path(manager.collected_root) / (
        "job-1/workers/shard-00000/_elastic_agent/collection.json"
    )
    metadata = json.loads(manifest.read_text())
    assert metadata == {
        "collected_at": metadata["collected_at"],
        "destination": "manager-rsync",
        "job_id": "job-1",
        "paths": ["results"],
        "schema_version": 1,
        "shard_index": 0,
        "worker_id": "worker-a",
        "worker_namespace": "shard-00000",
    }
    assert manifest.stat().st_mode & 0o777 == 0o600


async def test_worker_direct_s3_uses_isolated_prefix_and_manifest(
    tmp_path, monkeypatch,
):
    monkeypatch.setenv("ELASTIC_AGENT_RESULTS_S3_BUCKET", "result-bucket")
    monkeypatch.setenv("ELASTIC_AGENT_RESULTS_S3_PREFIX", "batch-results")
    commands = []
    hosts = []

    class FakeSSHExecutor:
        def __init__(self, host, *args, **kwargs):
            hosts.append(host)

        async def execute(self, command, timeout=None):
            commands.append(command)
            return 0, "", ""

    monkeypatch.setattr(
        "elastic_agent.core.bootstrap.SSHExecutor", FakeSSHExecutor,
    )
    manager = FakeManager(tmp_path, worker_profile="worker-role")

    await ManagerFleetDriver(manager).collect(
        "worker-b", _spec(tmp_path), "job-1",
    )

    assert hosts and set(hosts) == {"10.0.0.11"}
    assert any(command.startswith("command -v aws ") for command in commands)
    assert all("apt-get" not in command for command in commands)
    assert any(
        "s3://result-bucket/batch-results/job-1/workers/"
        "shard-00001/results/" in command
        for command in commands
    )
    assert any("--no-follow-symlinks" in command for command in commands)
    assert any("aws s3 cp" in command and "--recursive" in command
               for command in commands)
    assert any(
        "s3://result-bucket/batch-results/job-1/workers/"
        "shard-00001/_elastic_agent/collection.json" in command
        for command in commands
    )


async def test_checkpoint_collection_uses_manager_snapshot_and_commits(
    tmp_path, monkeypatch,
):
    monkeypatch.setenv("ELASTIC_AGENT_RESULTS_S3_BUCKET", "result-bucket")
    calls = []

    async def fake_subprocess(*args, **kwargs):
        calls.append(args)
        return FakeProcess()

    monkeypatch.setattr(
        "elastic_agent.core.manager_fleet_driver.asyncio.create_subprocess_exec",
        fake_subprocess,
    )

    class Uploader:
        def sync_job(self, _job_id):
            return 1

    manager = FakeManager(tmp_path, worker_profile="worker-role")
    manager._s3_uploader = Uploader()
    manager._checkpoint_store = FakeCheckpointStore()
    local_result = Path(manager.collected_root) / (
        "job-1/workers/shard-00000/results"
    )
    local_result.mkdir(parents=True)
    (local_result / "answer.json").write_text("42")
    spec = JobSpec.model_validate({
        "name": "checkpoint",
        "setup": {
            "repo": "https://github.com/example/bench.git",
            "target_dir": str(tmp_path / "remote-work"),
            "resolved_commit": "a" * 40,
        },
        "run": {"command": "bench"},
        "collect": {
            "paths": ["results"],
            "checkpoint": True,
            "exclude": ["**/core"],
        },
    })

    await ManagerFleetDriver(manager).collect("worker-a", spec, "job-1")

    assert calls
    assert "--delete" in calls[0]
    assert "--delete-excluded" in calls[0]
    assert "--exclude" in calls[0]
    assert manager._checkpoint_store.commits
    commit = manager._checkpoint_store.commits[0]
    assert commit["job_id"] == "job-1"
    assert commit["worker_namespace"] == "shard-00000"
    assert commit["paths"] == ["results"]
    assert commit["exclude"] == ["**/core"]
    assert commit["metadata"]["resolved_commit"] == "a" * 40
    assert commit["metadata"]["shard_index"] == 0


async def test_worker_direct_s3_missing_awscli_fails_without_runtime_install(
    tmp_path, monkeypatch,
):
    monkeypatch.setenv("ELASTIC_AGENT_RESULTS_S3_BUCKET", "result-bucket")
    commands = []

    class FakeSSHExecutor:
        def __init__(self, *args, **kwargs):
            pass

        async def execute(self, command, timeout=None):
            commands.append(command)
            if command.startswith("command -v aws "):
                return 127, "", "awscli is required"
            raise AssertionError("collection must stop before any S3 upload")

    monkeypatch.setattr(
        "elastic_agent.core.bootstrap.SSHExecutor", FakeSSHExecutor,
    )
    manager = FakeManager(tmp_path, worker_profile="worker-role")

    with pytest.raises(RuntimeError, match="awscli is unavailable"):
        await ManagerFleetDriver(manager).collect(
            "worker-a", _spec(tmp_path), "job-1",
        )

    assert commands
    assert all("apt-get" not in command for command in commands)


async def test_restart_recovery_reuses_durable_lease_slot(tmp_path):
    manager = FakeManager(tmp_path)
    manager._batch = None

    class LeaseStore:
        async def list_leases(self):
            return [SimpleNamespace(
                lease_id="lease-1",
                job_id="job-1",
                worker_id="worker-a",
                instance_id="i-123",
                slot=7,
            )]

    manager.account_binding_store = LeaseStore()
    namespace, shard_index = await ManagerFleetDriver(
        manager,
    )._collection_identity("worker-a", "job-1")
    assert (namespace, shard_index) == ("shard-00007", 7)


async def test_prepare_and_restore_checkpoint_before_run(
    tmp_path, monkeypatch,
):
    monkeypatch.setenv("ELASTIC_AGENT_RESULTS_S3_BUCKET", "result-bucket")
    manager = FakeManager(tmp_path)
    manager._checkpoint_store = FakeCheckpointStore()
    source = JobSpec.model_validate({
        "name": "source",
        "setup": {
            "repo": "https://github.com/example/bench.git",
            "resolved_commit": "a" * 40,
        },
        "run": {"command": "capture"},
        "fanout": {"workers": 2},
        "collect": {"paths": ["results"], "checkpoint": True},
    })
    persist_job_spec(
        manager.config.registry.path,
        "job-source",
        source,
    )
    update_job_state(
        manager.config.registry.path,
        "job-source",
        "failed",
    )
    recovering = JobSpec.model_validate({
        "name": "resume",
        "setup": {
            "repo": "https://github.com/example/bench.git",
            "target_dir": str(tmp_path / "remote-work"),
            "resolved_commit": "a" * 40,
        },
        "run": {"command": "resume"},
        "fanout": {"workers": 2},
        "recovery": {
            "policy": "checkpoint",
            "source_job_id": "job-source",
            "paths": ["results"],
        },
    })
    driver = ManagerFleetDriver(manager)

    await driver.prepare_recovery("job-new", recovering)

    assert [
        (kind, call["worker_namespace"])
        for kind, call in manager._checkpoint_store.restores
    ] == [
        ("checkpoint", "shard-00000"),
        ("checkpoint", "shard-00001"),
    ]
    assert [
        call["expected_metadata"]["shard_index"]
        for _kind, call in manager._checkpoint_store.restores
    ] == [0, 1]
    assert all(
        call["expected_metadata"]["job_spec_sha256"]
        == driver._job_hash(source)
        for _kind, call in manager._checkpoint_store.restores
    )

    commands = []

    async def fake_subprocess(*args, **kwargs):
        commands.append(args)
        return FakeProcess()

    monkeypatch.setattr(
        "elastic_agent.core.manager_fleet_driver.asyncio.create_subprocess_exec",
        fake_subprocess,
    )
    await driver.restore_recovery(
        "worker-a",
        "job-new",
        recovering,
        SimpleNamespace(shard_index=0),
    )

    assert commands
    assert commands[0][0] == "rsync"
    assert "--safe-links" in commands[0]
    assert commands[0][-1].endswith(
        f":{recovering.setup.target_dir.rstrip('/')}/"
    )

    await driver.cleanup_recovery("job-new")
    assert not (
        Path(manager.config.registry.path).with_name("recovery-staging")
        / "job-new"
    ).exists()


async def test_prepare_recovery_rejects_mismatched_source_contract(
    tmp_path, monkeypatch,
):
    monkeypatch.setenv("ELASTIC_AGENT_RESULTS_S3_BUCKET", "result-bucket")
    manager = FakeManager(tmp_path)
    manager._checkpoint_store = FakeCheckpointStore()
    source = JobSpec.model_validate({
        "name": "source",
        "setup": {
            "repo": "https://github.com/example/bench.git",
            "resolved_commit": "a" * 40,
        },
        "run": {"command": "capture"},
        "fanout": {"workers": 1},
        "collect": {"paths": ["results"], "checkpoint": True},
    })
    persist_job_spec(
        manager.config.registry.path,
        "job-source",
        source,
    )
    update_job_state(
        manager.config.registry.path,
        "job-source",
        "failed",
    )
    wrong_commit = JobSpec.model_validate({
        "name": "resume",
        "setup": {
            "repo": "https://github.com/example/bench.git",
            "resolved_commit": "b" * 40,
        },
        "run": {"command": "resume"},
        "recovery": {
            "policy": "checkpoint",
            "source_job_id": "job-source",
            "paths": ["results"],
        },
    })

    with pytest.raises(RuntimeError, match="resolved_commit"):
        await ManagerFleetDriver(manager).prepare_recovery(
            "job-new", wrong_commit,
        )

    assert manager._checkpoint_store.restores == []


async def test_legacy_recovery_accepts_old_proven_final_collection_summary():
    source = JobSpec.model_validate({
        "name": "source",
        "setup": {
            "repo": "https://github.com/example/bench.git",
            "resolved_commit": "a" * 40,
        },
        "run": {"command": "capture"},
        "fanout": {"workers": 2},
        "collect": {"paths": ["results"]},
    })
    target = JobSpec.model_validate({
        "name": "resume",
        "setup": {
            "repo": source.setup.repo,
            "resolved_commit": source.setup.resolved_commit,
        },
        "run": {"command": "resume"},
        "fanout": {"workers": 2},
        "recovery": {
            "policy": "legacy_final_collection",
            "source_job_id": "job-source",
            "paths": ["results"],
        },
    })
    payload = {
        "submission_state": "failed",
        "terminal_summary": {
            "terminal_workers": [
                {
                    "shard_index": shard,
                    # Compatibility proof written by the old summary schema.
                    "collection_error": None,
                }
                for shard in range(2)
            ],
        },
    }

    ManagerFleetDriver._validate_recovery_contract(
        payload, source, target,
    )


async def test_legacy_recovery_rejects_duplicate_or_failed_shard_proof():
    source = JobSpec.model_validate({
        "name": "source",
        "setup": {
            "repo": "https://github.com/example/bench.git",
            "resolved_commit": "a" * 40,
        },
        "run": {"command": "capture"},
        "fanout": {"workers": 2},
        "collect": {"paths": ["results"]},
    })
    target = JobSpec.model_validate({
        "name": "resume",
        "setup": {
            "repo": source.setup.repo,
            "resolved_commit": source.setup.resolved_commit,
        },
        "run": {"command": "resume"},
        "fanout": {"workers": 2},
        "recovery": {
            "policy": "legacy_final_collection",
            "source_job_id": "job-source",
            "paths": ["results"],
        },
    })
    payload = {
        "submission_state": "failed",
        "terminal_summary": {
            "terminal_workers": [
                {
                    "shard_index": 0,
                    "final_collected": True,
                    "collection_error": None,
                },
                {
                    "shard_index": 0,
                    "final_collected": True,
                    "collection_error": "S3 failed",
                },
            ],
        },
    }

    with pytest.raises(RuntimeError, match="proven successful"):
        ManagerFleetDriver._validate_recovery_contract(
            payload, source, target,
        )


async def test_empty_collect_paths_is_noop_before_registry_or_storage(
    tmp_path, monkeypatch,
):
    monkeypatch.setenv("ELASTIC_AGENT_RESULTS_S3_BUCKET", "result-bucket")
    manager = FakeManager(tmp_path, worker_profile="worker-role")

    class ExplodingRegistry:
        async def get(self, worker_id):
            raise AssertionError("empty collection must not resolve a worker")

    manager.registry = ExplodingRegistry()
    spec = JobSpec.model_validate({
        "name": "no-results",
        "run": {"command": "true"},
        "collect": {"paths": []},
    })

    await ManagerFleetDriver(manager).collect("missing", spec, "job-empty")
    assert not Path(manager.collected_root).exists()


async def test_manager_side_s3_failure_fails_collection(tmp_path, monkeypatch):
    monkeypatch.setenv("ELASTIC_AGENT_RESULTS_S3_BUCKET", "result-bucket")

    async def fake_subprocess(*args, **kwargs):
        return FakeProcess()

    monkeypatch.setattr(
        "elastic_agent.core.manager_fleet_driver.asyncio.create_subprocess_exec",
        fake_subprocess,
    )

    class FailingUploader:
        def sync_job(self, job_id):
            raise RuntimeError("AccessDenied")

    manager = FakeManager(tmp_path)
    manager._s3_uploader = FailingUploader()
    with pytest.raises(RuntimeError, match="Manager S3 collect failed.*AccessDenied"):
        await ManagerFleetDriver(manager).collect(
            "worker-a", _spec(tmp_path), "job-1",
        )


async def test_configured_bucket_requires_initialized_manager_uploader(
    tmp_path, monkeypatch,
):
    monkeypatch.setenv("ELASTIC_AGENT_RESULTS_S3_BUCKET", "result-bucket")

    async def fake_subprocess(*args, **kwargs):
        return FakeProcess()

    monkeypatch.setattr(
        "elastic_agent.core.manager_fleet_driver.asyncio.create_subprocess_exec",
        fake_subprocess,
    )
    with pytest.raises(RuntimeError, match="uploader is not initialized"):
        await ManagerFleetDriver(FakeManager(tmp_path)).collect(
            "worker-a", _spec(tmp_path), "job-1",
        )


async def test_scale_in_force_terminates_and_stop_command_forwards_signal(tmp_path):
    manager = FakeManager(tmp_path)
    driver = ManagerFleetDriver(manager)

    await driver.scale_in(["worker-a", "worker-b"])
    await driver.stop_command("worker-a", "task-1", signal="SIGINT")

    assert manager.scale_in_calls == [(["worker-a", "worker-b"], True)]
    assert manager.removed_nodes == ["worker-a", "worker-b"]
    assert manager.connection_manager.stopped == [
        ("worker-a", "task-1", "SIGINT")
    ]


@pytest.mark.parametrize(
    "terminated",
    [
        ["worker-a"],
        ["worker-a", "worker-b", "worker-extra"],
        ["worker-a", "worker-a", "worker-b"],
    ],
)
async def test_scale_in_requires_exact_termination_proof_before_node_removal(
    tmp_path, terminated,
):
    manager = FakeManager(tmp_path)

    async def incomplete_scale_in(*, node_ids, force):
        manager.scale_in_calls.append((node_ids, force))
        return list(terminated)

    manager.scale_in = incomplete_scale_in

    with pytest.raises(RuntimeError, match="termination proof"):
        await ManagerFleetDriver(manager).scale_in(["worker-a", "worker-b"])

    assert manager.removed_nodes == []


async def test_scale_in_normalizes_requested_duplicates_and_accepts_reordering(
    tmp_path,
):
    manager = FakeManager(tmp_path)

    async def reordered_scale_in(*, node_ids, force):
        manager.scale_in_calls.append((node_ids, force))
        return list(reversed(node_ids))

    manager.scale_in = reordered_scale_in

    await ManagerFleetDriver(manager).scale_in([
        "worker-a", "worker-a", "worker-b",
    ])

    assert manager.scale_in_calls == [(["worker-a", "worker-b"], True)]
    assert manager.removed_nodes == ["worker-a", "worker-b"]


async def test_scale_in_finishes_proven_node_cleanup_when_caller_is_cancelled(
    tmp_path,
):
    manager = FakeManager(tmp_path)
    removal_started = asyncio.Event()
    allow_removal = asyncio.Event()

    async def blocking_remove_node(node_id):
        removal_started.set()
        await allow_removal.wait()
        manager.removed_nodes.append(node_id)
        manager.registry.nodes.pop(node_id, None)
        return True

    manager.remove_node = blocking_remove_node
    operation = asyncio.create_task(
        ManagerFleetDriver(manager).scale_in(["worker-a"])
    )
    await removal_started.wait()
    operation.cancel()
    await asyncio.sleep(0)
    operation.cancel()
    await asyncio.sleep(0)
    allow_removal.set()

    # Once Manager returned an exact termination proof, caller cancellation
    # must not turn the transaction into a retry against an already-removed
    # registry record.
    await operation
    assert manager.removed_nodes == ["worker-a"]


async def test_scale_in_reuses_exact_proof_after_node_cleanup_failure(tmp_path):
    manager = FakeManager(tmp_path)
    attempts = 0

    async def fail_once_remove_node(node_id):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise OSError("registry fsync failed")
        manager.removed_nodes.append(node_id)
        manager.registry.nodes.pop(node_id, None)
        return True

    manager.remove_node = fail_once_remove_node
    driver = ManagerFleetDriver(manager)

    with pytest.raises(OSError, match="registry fsync failed"):
        await driver.scale_in(["worker-a"])
    await driver.scale_in(["worker-a"])

    assert manager.scale_in_calls == [(["worker-a"], True)]
    assert manager.removed_nodes == ["worker-a"]


async def test_scale_in_retries_only_unremoved_records_after_partial_failure(
    tmp_path,
):
    manager = FakeManager(tmp_path)
    removal_calls: list[str] = []
    worker_b_attempts = 0

    async def partial_remove_node(node_id):
        nonlocal worker_b_attempts
        removal_calls.append(node_id)
        if node_id == "worker-a":
            if removal_calls.count(node_id) > 1:
                raise AssertionError("worker-a removal is not idempotent")
        else:
            worker_b_attempts += 1
            if worker_b_attempts == 1:
                raise OSError("worker-b registry fsync failed")
        manager.registry.nodes.pop(node_id, None)
        manager.removed_nodes.append(node_id)
        return True

    manager.remove_node = partial_remove_node
    driver = ManagerFleetDriver(manager)

    with pytest.raises(OSError, match="worker-b registry fsync failed"):
        await driver.scale_in(["worker-a", "worker-b"])
    await driver.scale_in(["worker-a", "worker-b"])

    assert manager.scale_in_calls == [(["worker-a", "worker-b"], True)]
    assert removal_calls == ["worker-a", "worker-b", "worker-b"]
    assert manager.removed_nodes == ["worker-a", "worker-b"]
    assert driver._proven_terminated_workers == set()
    assert driver._proven_removed_workers == set()


async def test_scale_in_requires_registry_absence_after_remove_node(tmp_path):
    manager = FakeManager(tmp_path)

    async def lying_remove_node(node_id):
        manager.removed_nodes.append(node_id)
        return True

    manager.remove_node = lying_remove_node
    driver = ManagerFleetDriver(manager)

    with pytest.raises(RuntimeError, match="registry-removal proof"):
        await driver.scale_in(["worker-a"])

    assert manager.scale_in_calls == [(["worker-a"], True)]
    assert manager.registry.nodes["worker-a"] is not None


async def test_scale_in_coalesces_overlapping_same_worker_cleanup(tmp_path):
    manager = FakeManager(tmp_path)
    cloud_entered = asyncio.Event()
    allow_cloud_finish = asyncio.Event()

    async def blocking_scale_in(*, node_ids, force):
        manager.scale_in_calls.append((node_ids, force))
        cloud_entered.set()
        await allow_cloud_finish.wait()
        return list(node_ids)

    manager.scale_in = blocking_scale_in
    driver = ManagerFleetDriver(manager)
    first = asyncio.create_task(driver.scale_in(["worker-a"]))
    await cloud_entered.wait()
    second = asyncio.create_task(driver.scale_in(["worker-a"]))
    for _ in range(100):
        async with driver._scale_in_state_lock:
            if driver._scale_in_lock_users.get("worker-a") == 2:
                break
        await asyncio.sleep(0)
    else:
        pytest.fail("second cleanup did not join the per-worker lock")

    allow_cloud_finish.set()
    await asyncio.gather(first, second)

    assert manager.scale_in_calls == [(["worker-a"], True)]
    assert manager.removed_nodes == ["worker-a"]
    assert driver._scale_in_locks == {}
    assert driver._scale_in_lock_users == {}
    assert driver._completed_scale_in_workers == set()


async def test_scale_in_keeps_disjoint_worker_terminations_concurrent(tmp_path):
    manager = FakeManager(tmp_path)
    entered: set[str] = set()
    both_entered = asyncio.Event()
    allow_finish = asyncio.Event()

    async def concurrent_scale_in(*, node_ids, force):
        assert len(node_ids) == 1
        entered.add(node_ids[0])
        if len(entered) == 2:
            both_entered.set()
        await allow_finish.wait()
        return list(node_ids)

    manager.scale_in = concurrent_scale_in
    driver = ManagerFleetDriver(manager)
    first = asyncio.create_task(driver.scale_in(["worker-a"]))
    second = asyncio.create_task(driver.scale_in(["worker-b"]))

    await asyncio.wait_for(both_entered.wait(), timeout=0.5)
    allow_finish.set()
    await asyncio.gather(first, second)

    assert entered == {"worker-a", "worker-b"}
    assert set(manager.removed_nodes) == entered


async def test_resolve_secret_env_delegates_without_mutating_refs(
    tmp_path, monkeypatch,
):
    references = {"TOKEN": "aws-ssm:///prod/token"}
    seen = []

    async def fake_resolve(value):
        seen.append(dict(value))
        return {"TOKEN": "plaintext"}

    monkeypatch.setattr(
        "elastic_agent.core.manager_fleet_driver.resolve_aws_secret_env",
        fake_resolve,
    )
    resolved = await ManagerFleetDriver(FakeManager(tmp_path)).resolve_secret_env(
        references,
    )

    assert resolved == {"TOKEN": "plaintext"}
    assert references == {"TOKEN": "aws-ssm:///prod/token"}
    assert seen == [references]


async def test_resolve_secret_env_rejects_plaintext_remote_worker_transport(
    tmp_path, monkeypatch,
):
    manager = FakeManager(tmp_path)
    manager.config.server.host = "0.0.0.0"
    monkeypatch.delenv("ELASTIC_AGENT_MANAGER_URL", raising=False)
    monkeypatch.delenv("ELASTIC_AGENT_ALLOW_INSECURE_SECRET_ENV", raising=False)
    resolver = AsyncMock()
    monkeypatch.setattr(
        "elastic_agent.core.manager_fleet_driver.resolve_aws_secret_env",
        resolver,
    )

    with pytest.raises(ValueError, match="requires a wss://"):
        await ManagerFleetDriver(manager).resolve_secret_env(
            {"TOKEN": "aws-ssm:///prod/token"},
        )

    resolver.assert_not_awaited()


async def test_resolve_secret_env_allows_wss_transport(tmp_path, monkeypatch):
    monkeypatch.setenv(
        "ELASTIC_AGENT_MANAGER_URL", "wss://manager.example/ws/runtime",
    )
    resolver = AsyncMock(return_value={"TOKEN": "plaintext"})
    monkeypatch.setattr(
        "elastic_agent.core.manager_fleet_driver.resolve_aws_secret_env",
        resolver,
    )

    result = await ManagerFleetDriver(FakeManager(tmp_path)).resolve_secret_env(
        {"TOKEN": "aws-ssm:///prod/token"},
    )

    assert result == {"TOKEN": "plaintext"}
