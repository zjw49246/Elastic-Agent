"""Focused tests for ManagerFleetDriver result durability and teardown."""

from __future__ import annotations

import asyncio
import json
import os
import sys
import threading
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from elastic_agent.core.checkpoint_store import IncompleteCheckpointSetError
from elastic_agent.core.job_log_store import JobLogStore
from elastic_agent.core.job_spec import JobSpec
from elastic_agent.core.job_spec_store import (
    persist_job_spec,
    update_job_checkpoint,
    update_job_interrupt_intent,
    update_job_state,
)
from elastic_agent.core.manager_fleet_driver import (
    ManagerFleetDriver,
    _terminate_subprocess,
    _UnsettledSubprocessError,
)
from elastic_agent.core.registry import NodeStatus

pytestmark = pytest.mark.asyncio
_REAL_REMOTE_COLLECTION_INVENTORY = (
    ManagerFleetDriver._remote_collection_inventory
)


@pytest.fixture(autouse=True)
def stub_remote_collection_inventory(monkeypatch):
    async def empty_inventory(self, **_kwargs):
        return 0, 0

    monkeypatch.setattr(
        ManagerFleetDriver,
        "_remote_collection_inventory",
        empty_inventory,
    )


class FakeRegistry:
    def __init__(self):
        self.nodes = {
            "worker-a": SimpleNamespace(
                instance_id="worker-a",
                status=NodeStatus.READY,
                public_ip="203.0.113.10",
                private_ip="10.0.0.10",
                metadata={"job_id": "job-1", "shard_index": 0},
            ),
            "worker-b": SimpleNamespace(
                instance_id="worker-b",
                status=NodeStatus.READY,
                public_ip="203.0.113.11",
                private_ip="10.0.0.11",
                metadata={"job_id": "job-1", "shard_index": 1},
            ),
        }

    async def get(self, worker_id):
        return self.nodes.get(worker_id)

    async def list_all(self):
        return list(self.nodes.values())

    async def update(self, worker_id, **fields):
        node = self.nodes.get(worker_id)
        if node is None:
            return None
        for name, value in fields.items():
            setattr(node, name, value)
        return node


class FakeConnectionManager:
    def __init__(self):
        self.stopped = []
        self.stop_policies = []

    async def stop_process(
        self,
        worker_id,
        task_id,
        sig="SIGTERM",
        *,
        scope="group",
        escalate=True,
    ):
        self.stopped.append((worker_id, task_id, sig))
        self.stop_policies.append((scope, escalate))


class FakeBatch:
    def __init__(self):
        self.job = SimpleNamespace(runs={
            "worker-a": SimpleNamespace(
                ctx=SimpleNamespace(shard_index=0),
                checkpoint_generation="",
                checkpoint_set_generation="",
                checkpoint_shard_generations={},
                last_checkpoint_generation="",
            ),
            "worker-b": SimpleNamespace(
                ctx=SimpleNamespace(shard_index=1),
                checkpoint_generation="",
                checkpoint_set_generation="",
                checkpoint_shard_generations={},
                last_checkpoint_generation="",
            ),
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
        self.binding_recovery_ready = True
        self.account_binding_store = None
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
        for node_id in node_ids:
            node = self.registry.nodes.get(node_id)
            if node is not None:
                node.status = NodeStatus.TERMINATED
        return list(node_ids)

    async def remove_node(self, node_id):
        self.removed_nodes.append(node_id)
        self.registry.nodes.pop(node_id, None)
        return True

    async def remove_terminated_node_record(self, node_id):
        return await self.remove_node(node_id)


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


def _checkpoint_spec(tmp_path):
    return JobSpec.model_validate({
        "name": "checkpoint-test",
        "setup": {
            "repo": "https://github.com/example/bench.git",
            "target_dir": str(tmp_path / "remote-work"),
            "resolved_commit": "a" * 40,
        },
        "run": {"command": "true"},
        "fanout": {"shard_by": "shard_index"},
        "collect": {"paths": ["results"], "checkpoint": True},
    })


class FakeCheckpointStore:
    def __init__(self):
        self.restores = []
        self.commits = []
        self.checkpoint_sets = []
        self.checkpoint_set_attempts = []
        self.prune_calls = []
        self.resolved_metadata = {
            "resolved_commit": "a" * 40,
            "recovery_contract_version": 3,
            "recovery_contract_sha256": "",
            "fanout_workers": 2,
            "shard_by": "shard_index",
            "collect_paths": ["results"],
            "collect_exclude": [],
        }

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
        return {
            "generation": kwargs.get("generation") or "new-generation",
        }

    def publish_checkpoint_set(self, **kwargs):
        self.checkpoint_set_attempts.append(kwargs)
        available = {
            (
                commit["worker_namespace"],
                commit.get("generation") or "new-generation",
            )
            for commit in self.commits
        }
        committed = {
            namespace
            for namespace, generation in kwargs[
                "shard_generations"
            ].items()
            if (namespace, generation) in available
        }
        if len(committed) != len(kwargs["shard_generations"]):
            raise IncompleteCheckpointSetError(
                "checkpoint set references an uncommitted shard",
                committed_namespaces=committed,
            )
        self.checkpoint_sets.append(kwargs)
        return {
            "generation": kwargs.get("generation") or "latest",
            "committed_at": (
                "2026-07-29T00:00:"
                f"{len(self.checkpoint_sets):02d}+00:00"
            ),
        }

    def prune_incomplete_generations(self, **kwargs):
        self.prune_calls.append(kwargs)
        return 0

    def resolve_checkpoint_set(self, **kwargs):
        generation = kwargs.get("generation") or "periodic-00000001"
        return {
            "generation": generation,
            "total_bytes": 4,
            "total_objects": 4,
            "metadata": dict(self.resolved_metadata),
            "shards": [
                {
                    "worker_namespace": f"shard-{index:05d}",
                    "generation": generation,
                    "manifest_sha256": str(index) * 64,
                    "total_bytes": 2,
                    "total_objects": 2,
                }
                for index in range(2)
            ],
        }


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
    assert any(
        "job-1/workers/.shard-00000.attempt-" in p
        and p.endswith("/results/")
        for p in destinations
    )
    assert any(
        "job-1/workers/.shard-00001.attempt-" in p
        and p.endswith("/results/")
        for p in destinations
    )
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


async def test_collection_tree_enforces_object_file_and_byte_limits(
    tmp_path, monkeypatch,
):
    monkeypatch.setattr(
        "elastic_agent.core.manager_fleet_driver.shutil.disk_usage",
        lambda _path: SimpleNamespace(
            free=2 * 1024 * 1024 * 1024,
        ),
    )
    root = tmp_path / "relay"
    root.mkdir()
    payload = root / "payload.bin"
    payload.write_bytes(b"123456")

    with pytest.raises(RuntimeError, match="single-file"):
        ManagerFleetDriver._validate_collection_tree(
            root, max_bytes=10, max_objects=10, max_file_bytes=5,
        )
    with pytest.raises(RuntimeError, match="byte limit"):
        ManagerFleetDriver._validate_collection_tree(
            root, max_bytes=5, max_objects=10, max_file_bytes=10,
        )

    payload.unlink()
    (root / "a").mkdir()
    (root / "b").mkdir()
    with pytest.raises(RuntimeError, match="object limit"):
        ManagerFleetDriver._validate_collection_tree(
            root, max_bytes=10, max_objects=1, max_file_bytes=10,
        )
    monkeypatch.setattr(
        "elastic_agent.core.manager_fleet_driver.os.statvfs",
        lambda _path: SimpleNamespace(f_favail=1),
    )
    with pytest.raises(RuntimeError, match="inode reserve"):
        ManagerFleetDriver._validate_collection_tree(
            root, max_bytes=10, max_objects=10, max_file_bytes=10,
        )


async def test_remote_inventory_rejects_before_manager_rsync(
    tmp_path, monkeypatch,
):
    commands = []

    class FakeSSHExecutor:
        def __init__(self, *_args, **_kwargs):
            pass

        async def execute(self, command, timeout=None):
            commands.append((command, timeout))
            return 3, json.dumps({
                "ok": False,
                "reason": "object_limit",
                "bytes": 0,
                "objects": 11,
            }), "private worker path"

    monkeypatch.setattr(
        "elastic_agent.core.bootstrap.SSHExecutor",
        FakeSSHExecutor,
    )
    driver = ManagerFleetDriver(FakeManager(tmp_path))

    with pytest.raises(
        RuntimeError,
        match=r"rejected the tree \(object_limit\)",
    ) as error:
        await _REAL_REMOTE_COLLECTION_INVENTORY(
            driver,
            host="10.0.0.10",
            ssh_user="ubuntu",
            ssh_key="/tmp/key",
            source="/opt/work/results",
            exclude=["**/cache"],
            max_bytes=10,
            max_objects=10,
            max_file_bytes=10,
        )

    assert "private worker path" not in str(error.value)
    assert commands and commands[0][1] == 300


async def test_terminate_subprocess_closes_group_after_leader_exit(
    monkeypatch,
):
    calls = []
    killed = False

    class ExitedLeader:
        pid = 4321
        returncode = 1

        async def wait(self):
            calls.append(("wait",))
            return self.returncode

    def fake_killpg(pgid, sig):
        nonlocal killed
        if sig == 0:
            if killed:
                raise ProcessLookupError
            return
        calls.append(("killpg", pgid, sig))
        if sig == 9:
            killed = True

    monkeypatch.setattr(
        "elastic_agent.core.manager_fleet_driver.os.killpg",
        fake_killpg,
    )

    await _terminate_subprocess(ExitedLeader())

    assert calls[0][:2] == ("killpg", 4321)
    assert ("wait",) in calls
    assert len([call for call in calls if call[0] == "killpg"]) == 2


async def test_collect_terminates_and_reclaims_rsync_on_runtime_limit(
    tmp_path, monkeypatch,
):
    monkeypatch.delenv("ELASTIC_AGENT_RESULTS_S3_BUCKET", raising=False)
    monkeypatch.setenv("ELASTIC_AGENT_MAX_COLLECTION_STAGING_BYTES", "10")
    monkeypatch.setenv("ELASTIC_AGENT_MAX_COLLECTION_STAGING_OBJECTS", "10")
    monkeypatch.setenv("ELASTIC_AGENT_MAX_COLLECTION_FILE_BYTES", "5")
    monkeypatch.setenv(
        "ELASTIC_AGENT_MAX_COLLECTION_STAGING_TOTAL_BYTES", "20",
    )
    monkeypatch.setattr(
        "elastic_agent.core.manager_fleet_driver."
        "_COLLECTION_MONITOR_INTERVAL_SECONDS",
        0.001,
    )
    monkeypatch.setattr(
        "elastic_agent.core.manager_fleet_driver.shutil.disk_usage",
        lambda _path: SimpleNamespace(
            free=2 * 1024 * 1024 * 1024,
        ),
    )

    processes = []

    class GrowingProcess:
        def __init__(self, destination):
            self.destination = Path(destination)
            self.returncode = None
            self.terminated = False
            self.waited = False
            self._finished = asyncio.Event()

        async def communicate(self):
            self.destination.mkdir(parents=True, exist_ok=True)
            (self.destination / "oversized.bin").write_bytes(b"123456")
            await self._finished.wait()
            return b"", b""

        def terminate(self):
            self.terminated = True
            self.returncode = -15
            self._finished.set()

        def kill(self):
            self.returncode = -9
            self._finished.set()

        async def wait(self):
            self.waited = True
            await self._finished.wait()
            return self.returncode

    async def fake_subprocess(*args, **kwargs):
        process = GrowingProcess(args[-1])
        processes.append(process)
        return process

    monkeypatch.setattr(
        "elastic_agent.core.manager_fleet_driver."
        "asyncio.create_subprocess_exec",
        fake_subprocess,
    )
    manager = FakeManager(tmp_path)

    with pytest.raises(RuntimeError, match="single-file"):
        await ManagerFleetDriver(manager).collect(
            "worker-a", _spec(tmp_path), "job-1",
        )

    assert len(processes) == 1
    assert processes[0].terminated
    assert processes[0].waited
    assert not (
        Path(manager.collected_root)
        / "job-1/workers/shard-00000/results"
    ).exists()
    assert manager._collection_staging_reservations == {}


async def test_collect_cleanup_resists_repeated_cancellation(
    tmp_path, monkeypatch,
):
    monkeypatch.delenv("ELASTIC_AGENT_RESULTS_S3_BUCKET", raising=False)
    monkeypatch.setenv("ELASTIC_AGENT_MAX_COLLECTION_STAGING_BYTES", "100")
    monkeypatch.setenv("ELASTIC_AGENT_MAX_COLLECTION_STAGING_OBJECTS", "100")
    monkeypatch.setenv("ELASTIC_AGENT_MAX_COLLECTION_FILE_BYTES", "100")
    monkeypatch.setenv(
        "ELASTIC_AGENT_MAX_COLLECTION_STAGING_TOTAL_BYTES", "100",
    )
    monkeypatch.setenv(
        "ELASTIC_AGENT_MAX_COLLECTION_STAGING_TOTAL_OBJECTS", "100",
    )
    monkeypatch.setattr(
        "elastic_agent.core.manager_fleet_driver.shutil.disk_usage",
        lambda _path: SimpleNamespace(free=2 * 1024 * 1024 * 1024),
    )
    monkeypatch.setattr(
        "elastic_agent.core.manager_fleet_driver.os.statvfs",
        lambda _path: SimpleNamespace(f_favail=100_000),
    )

    class SlowReapProcess:
        pid = None

        def __init__(self, destination):
            self.destination = Path(destination)
            self.returncode = None
            self.communicating = asyncio.Event()
            self.wait_entered = asyncio.Event()
            self.allow_reap = asyncio.Event()
            self.terminated = False
            self.waited = False

        async def communicate(self):
            self.destination.mkdir(parents=True, exist_ok=True)
            (self.destination / "partial.txt").write_text("partial")
            self.communicating.set()
            await asyncio.Event().wait()

        def terminate(self):
            self.terminated = True

        def kill(self):
            self.returncode = -9

        async def wait(self):
            self.waited = True
            self.wait_entered.set()
            await self.allow_reap.wait()
            self.returncode = -15
            return self.returncode

    processes = []

    async def fake_subprocess(*args, **_kwargs):
        process = SlowReapProcess(args[-1])
        processes.append(process)
        return process

    monkeypatch.setattr(
        "elastic_agent.core.manager_fleet_driver."
        "asyncio.create_subprocess_exec",
        fake_subprocess,
    )
    manager = FakeManager(tmp_path)
    task = asyncio.create_task(
        ManagerFleetDriver(manager).collect(
            "worker-a", _spec(tmp_path), "job-1",
        )
    )
    while not processes:
        await asyncio.sleep(0)
    process = processes[0]
    await process.communicating.wait()

    task.cancel()
    await process.wait_entered.wait()
    task.cancel()
    await asyncio.sleep(0.01)

    assert task.done() is False
    assert process.terminated
    assert process.waited
    assert manager._collection_staging_reservations
    attempts = list(
        (
            Path(manager.collected_root)
            / "job-1/workers"
        ).glob(".shard-00000.attempt-*")
    )
    assert attempts

    process.allow_reap.set()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert manager._collection_staging_reservations == {}
    assert not any(path.exists() for path in attempts)


async def test_collect_quarantines_attempt_when_reap_is_unconfirmed(
    tmp_path, monkeypatch,
):
    monkeypatch.delenv("ELASTIC_AGENT_RESULTS_S3_BUCKET", raising=False)
    monkeypatch.setenv("ELASTIC_AGENT_MAX_COLLECTION_STAGING_BYTES", "100")
    monkeypatch.setenv("ELASTIC_AGENT_MAX_COLLECTION_STAGING_OBJECTS", "100")
    monkeypatch.setenv("ELASTIC_AGENT_MAX_COLLECTION_FILE_BYTES", "100")
    monkeypatch.setenv(
        "ELASTIC_AGENT_MAX_COLLECTION_STAGING_TOTAL_BYTES", "100",
    )
    monkeypatch.setenv(
        "ELASTIC_AGENT_MAX_COLLECTION_STAGING_TOTAL_OBJECTS", "100",
    )
    monkeypatch.setattr(
        "elastic_agent.core.manager_fleet_driver.shutil.disk_usage",
        lambda _path: SimpleNamespace(free=2 * 1024 * 1024 * 1024),
    )
    monkeypatch.setattr(
        "elastic_agent.core.manager_fleet_driver.os.statvfs",
        lambda _path: SimpleNamespace(f_favail=100_000),
    )
    monkeypatch.setattr(
        "elastic_agent.core.manager_fleet_driver."
        "_SUBPROCESS_TERMINATE_TIMEOUT_SECONDS",
        0.01,
    )

    class UnreapableProcess:
        pid = None
        returncode = None

        def __init__(self, destination):
            self.destination = Path(destination)

        async def communicate(self):
            self.destination.mkdir(parents=True, exist_ok=True)
            (self.destination / "partial.txt").write_text("partial")
            raise RuntimeError("receiver failed")

        def terminate(self):
            pass

        def kill(self):
            pass

        async def wait(self):
            await asyncio.Event().wait()

    async def fake_subprocess(*args, **_kwargs):
        return UnreapableProcess(args[-1])

    monkeypatch.setattr(
        "elastic_agent.core.manager_fleet_driver."
        "asyncio.create_subprocess_exec",
        fake_subprocess,
    )
    manager = FakeManager(tmp_path)

    with pytest.raises(RuntimeError, match="could not be reaped"):
        await ManagerFleetDriver(manager).collect(
            "worker-a", _spec(tmp_path), "job-1",
        )

    assert manager._collection_staging_reservations
    assert list(
        (
            Path(manager.collected_root)
            / "job-1/workers"
        ).glob(".shard-00000.attempt-*")
    )


async def test_failed_collection_attempt_preserves_last_complete_snapshot(
    tmp_path, monkeypatch,
):
    monkeypatch.delenv("ELASTIC_AGENT_RESULTS_S3_BUCKET", raising=False)
    manager = FakeManager(tmp_path)
    published = (
        Path(manager.collected_root)
        / "job-1/workers/shard-00000/results"
    )
    published.mkdir(parents=True)
    (published / "old.txt").write_text("last-complete")

    class FailingProcess:
        returncode = 23

        def __init__(self, destination):
            self.destination = Path(destination)

        async def communicate(self):
            self.destination.mkdir(parents=True, exist_ok=True)
            (self.destination / "partial.txt").write_text("partial")
            return b"", b"simulated rsync failure"

        async def wait(self):
            return self.returncode

    async def fake_subprocess(*args, **_kwargs):
        return FailingProcess(args[-1])

    monkeypatch.setattr(
        "elastic_agent.core.manager_fleet_driver."
        "asyncio.create_subprocess_exec",
        fake_subprocess,
    )

    with pytest.raises(RuntimeError, match="rsync collect failed"):
        await ManagerFleetDriver(manager).collect(
            "worker-a", _spec(tmp_path), "job-1",
        )

    assert (published / "old.txt").read_text() == "last-complete"
    assert not (published / "partial.txt").exists()
    workers = published.parent.parent
    assert not list(workers.glob(".shard-00000.attempt-*"))
    assert not (workers / ".shard-00000.backup").exists()


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
        def sync_worker(self, _job_id, _namespace, **_kwargs):
            return 1

    manager = FakeManager(tmp_path, worker_profile="worker-role")
    manager._batch.job.runs.pop("worker-b")
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
        "fanout": {"shard_by": "shard_index"},
        "collect": {
            "paths": ["results"],
            "checkpoint": True,
            "exclude": ["**/core"],
        },
    })

    driver = ManagerFleetDriver(manager)
    await driver.collect("worker-a", spec, "job-1")

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
    checkpoint_set = dict(
        manager._checkpoint_store.checkpoint_sets[0]
    )
    assert isinstance(
        checkpoint_set.pop("deadline_monotonic"), float,
    )
    assert not checkpoint_set.pop("cancel_event").is_set()
    assert checkpoint_set == {
        "job_id": "job-1",
        "shard_generations": {"shard-00000": "final"},
        "generation": "final",
        "metadata": {
            "resolved_commit": "a" * 40,
            "recovery_contract_version": 3,
            "recovery_contract_sha256": (
                driver._checkpoint_contract_hash(spec)
            ),
            "fanout_workers": 1,
            "shard_by": "shard_index",
            "collect_paths": ["results"],
            "collect_exclude": ["**/core"],
        },
        "keep_last_n": 3,
    }


async def test_checkpoint_rsync_retries_c_locale_vanished_then_requires_rc0(
    tmp_path, monkeypatch,
):
    monkeypatch.setenv("ELASTIC_AGENT_RESULTS_S3_BUCKET", "result-bucket")
    monkeypatch.setenv("LC_ALL", "zh_CN.UTF-8")
    monkeypatch.setenv("ELASTIC_AGENT_TEST_REQUIRED_ENV", "preserved")
    vanished = (
        b"Warning: Permanently added '10.0.0.10' (ED25519) to the list "
        b"of known hosts.\n"
        b'file has vanished: "/opt/work/results/.state.json.abc.tmp"\n'
        b"rsync warning: some files vanished before they could be transferred "
        b"(code 24) at main.c(1338) [sender=3.2.7]\n"
    )
    outcomes = [(24, vanished), (0, b"")]
    calls = []
    subprocess_envs = []

    class CompletedProcess:
        def __init__(self, returncode, stderr):
            self.returncode = returncode
            self.stderr = stderr

        async def communicate(self):
            return b"", self.stderr

        async def wait(self):
            return self.returncode

    async def fake_subprocess(*args, **kwargs):
        calls.append(args)
        subprocess_envs.append(kwargs["env"])
        return CompletedProcess(*outcomes.pop(0))

    monkeypatch.setattr(
        "elastic_agent.core.manager_fleet_driver."
        "asyncio.create_subprocess_exec",
        fake_subprocess,
    )
    manager = FakeManager(tmp_path)
    manager._batch.job.runs.pop("worker-b")
    manager._checkpoint_store = FakeCheckpointStore()
    manager._s3_uploader = SimpleNamespace(
        sync_worker=lambda *_args, **_kwargs: 1,
    )

    await ManagerFleetDriver(manager).collect(
        "worker-a", _checkpoint_spec(tmp_path), "job-1",
    )

    assert len(calls) == 2
    assert all(env["LC_ALL"] == "C" for env in subprocess_envs)
    assert all(
        env["ELASTIC_AGENT_TEST_REQUIRED_ENV"] == "preserved"
        for env in subprocess_envs
    )
    assert os.environ["LC_ALL"] == "zh_CN.UTF-8"
    assert manager._checkpoint_store.commits


async def test_checkpoint_rsync_second_retry_must_reach_rc0_before_commit(
    tmp_path, monkeypatch,
):
    monkeypatch.setenv("ELASTIC_AGENT_RESULTS_S3_BUCKET", "result-bucket")
    vanished = (
        b'file has vanished: "/opt/work/results/.state.json.abc.tmp"\n'
        b"rsync warning: some files vanished before they could be transferred "
        b"(code 24) at main.c(1338) [sender=3.2.7]\n"
    )
    outcomes = [(24, vanished), (24, vanished), (0, b"")]
    calls = []
    manager = FakeManager(tmp_path)
    manager._batch.job.runs.pop("worker-b")
    checkpoint_store = FakeCheckpointStore()
    manager._checkpoint_store = checkpoint_store
    manager._s3_uploader = SimpleNamespace(
        sync_worker=lambda *_args, **_kwargs: 1,
    )

    class CompletedProcess:
        def __init__(self, returncode, stderr):
            self.returncode = returncode
            self.stderr = stderr

        async def communicate(self):
            return b"", self.stderr

        async def wait(self):
            return self.returncode

    async def fake_subprocess(*args, **_kwargs):
        calls.append(args)
        assert not checkpoint_store.commits
        return CompletedProcess(*outcomes.pop(0))

    monkeypatch.setattr(
        "elastic_agent.core.manager_fleet_driver."
        "asyncio.create_subprocess_exec",
        fake_subprocess,
    )

    await ManagerFleetDriver(manager).collect(
        "worker-a", _checkpoint_spec(tmp_path), "job-1",
    )

    assert len(calls) == 3
    assert not outcomes
    assert len(checkpoint_store.commits) == 1


async def test_checkpoint_rsync_does_not_retry_mixed_rc24_error(
    tmp_path, monkeypatch,
):
    monkeypatch.setenv("ELASTIC_AGENT_RESULTS_S3_BUCKET", "result-bucket")
    mixed_error = (
        b'file has vanished: "/opt/work/results/.state.json.abc.tmp"\n'
        b"rsync: [receiver] write failed: Input/output error (5)\n"
        b"rsync warning: some files vanished before they could be transferred "
        b"(code 24) at main.c(1338) [sender=3.2.7]\n"
    )
    calls = []

    class MixedFailure:
        returncode = 24

        async def communicate(self):
            return b"", mixed_error

        async def wait(self):
            return self.returncode

    async def fake_subprocess(*args, **_kwargs):
        calls.append(args)
        return MixedFailure()

    monkeypatch.setattr(
        "elastic_agent.core.manager_fleet_driver."
        "asyncio.create_subprocess_exec",
        fake_subprocess,
    )
    manager = FakeManager(tmp_path)
    manager._batch.job.runs.pop("worker-b")
    manager._checkpoint_store = FakeCheckpointStore()

    with pytest.raises(RuntimeError, match=r"rc=24"):
        await ManagerFleetDriver(manager).collect(
            "worker-a", _checkpoint_spec(tmp_path), "job-1",
        )

    assert len(calls) == 1
    assert not manager._checkpoint_store.commits


async def test_checkpoint_rsync_fails_after_bounded_vanished_source_retries(
    tmp_path, monkeypatch,
):
    monkeypatch.setenv("ELASTIC_AGENT_RESULTS_S3_BUCKET", "result-bucket")
    vanished = (
        b'file has vanished: "/opt/work/results/.state.json.abc.tmp"\n'
        b"rsync warning: some files vanished before they could be transferred "
        b"(code 24) at main.c(1338) [sender=3.2.7]\n"
    )
    calls = []

    class VanishedFailure:
        returncode = 24

        async def communicate(self):
            return b"", vanished

        async def wait(self):
            return self.returncode

    async def fake_subprocess(*args, **_kwargs):
        calls.append(args)
        return VanishedFailure()

    monkeypatch.setattr(
        "elastic_agent.core.manager_fleet_driver."
        "asyncio.create_subprocess_exec",
        fake_subprocess,
    )
    manager = FakeManager(tmp_path)
    manager._batch.job.runs.pop("worker-b")
    manager._checkpoint_store = FakeCheckpointStore()

    with pytest.raises(RuntimeError, match=r"rc=24"):
        await ManagerFleetDriver(manager).collect(
            "worker-a", _checkpoint_spec(tmp_path), "job-1",
        )

    assert len(calls) == 3
    assert not manager._checkpoint_store.commits


async def test_non_checkpoint_rsync_does_not_retry_vanished_rc24(
    tmp_path, monkeypatch,
):
    monkeypatch.setenv("ELASTIC_AGENT_RESULTS_S3_BUCKET", "result-bucket")
    vanished = (
        b'file has vanished: "/opt/work/results/.state.json.abc.tmp"\n'
        b"rsync warning: some files vanished before they could be transferred "
        b"(code 24) at main.c(1338) [sender=3.2.7]\n"
    )
    calls = []

    class VanishedFailure:
        returncode = 24

        async def communicate(self):
            return b"", vanished

        async def wait(self):
            return self.returncode

    async def fake_subprocess(*args, **_kwargs):
        calls.append(args)
        return VanishedFailure()

    monkeypatch.setattr(
        "elastic_agent.core.manager_fleet_driver."
        "asyncio.create_subprocess_exec",
        fake_subprocess,
    )
    manager = FakeManager(tmp_path)
    manager._checkpoint_store = FakeCheckpointStore()

    with pytest.raises(RuntimeError, match=r"rc=24"):
        await ManagerFleetDriver(manager).collect(
            "worker-a", _spec(tmp_path), "job-1",
        )

    assert len(calls) == 1
    assert not manager._checkpoint_store.commits


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


async def test_checkpoint_set_rebuilds_from_s3_after_manager_restart(
    tmp_path, monkeypatch,
):
    monkeypatch.setenv("ELASTIC_AGENT_RESULTS_S3_BUCKET", "result-bucket")

    async def fake_subprocess(*args, **kwargs):
        return FakeProcess()

    monkeypatch.setattr(
        "elastic_agent.core.manager_fleet_driver.asyncio.create_subprocess_exec",
        fake_subprocess,
    )

    class Uploader:
        def sync_worker(self, _job_id, _namespace, **_kwargs):
            return 1

    manager = FakeManager(tmp_path)
    manager._s3_uploader = Uploader()
    manager._checkpoint_store = FakeCheckpointStore()
    manager._batch.job.runs["worker-a"].checkpoint_generation = (
        "periodic-00000001"
    )
    manager._batch.job.runs["worker-b"].checkpoint_generation = (
        "periodic-00000001"
    )
    spec = JobSpec.model_validate({
        "name": "checkpoint-fanout",
        "setup": {
            "repo": "https://github.com/example/bench.git",
            "target_dir": str(tmp_path / "remote-work"),
            "resolved_commit": "a" * 40,
        },
        "run": {"command": "bench --shard {{shard_index}}"},
        "fanout": {"workers": 2, "shard_by": "shard_index"},
        "collect": {"paths": ["results"], "checkpoint": True},
    })
    driver = ManagerFleetDriver(manager)

    await driver.collect("worker-a", spec, "job-1")
    assert manager._checkpoint_store.checkpoint_sets == []
    assert len(manager._checkpoint_store.prune_calls) == 1

    # Simulate losing every process-local coordination object after shard 0
    # has already been destroyed. Shard manifests in S3 are the authority.
    del manager._checkpoint_publish_guard
    del manager._checkpoint_publish_states
    del manager._checkpoint_published_generations
    driver = ManagerFleetDriver(manager)
    await driver.collect("worker-b", spec, "job-1")
    assert len(manager._checkpoint_store.checkpoint_sets) == 1
    published = manager._checkpoint_store.checkpoint_sets[0]
    assert published["generation"] == "periodic-00000001"
    assert published["shard_generations"] == {
        "shard-00000": "periodic-00000001",
        "shard-00001": "periodic-00000001",
    }
    assert manager._batch.job.latest_checkpoint_generation == (
        "periodic-00000001"
    )


async def test_fanout_checkpoint_publishes_with_constant_s3_inventory_scans(
    tmp_path,
):
    manager = FakeManager(tmp_path)
    manager._batch = None
    manager._checkpoint_store = FakeCheckpointStore()
    driver = ManagerFleetDriver(manager)
    spec = JobSpec.model_validate({
        "name": "checkpoint-fanout",
        "setup": {
            "repo": "https://github.com/example/bench.git",
            "resolved_commit": "a" * 40,
        },
        "run": {"command": "bench"},
        "fanout": {"workers": 8, "shard_by": "shard_index"},
        "collect": {"paths": ["results"], "checkpoint": True},
    })

    for index in range(8):
        namespace = f"shard-{index:05d}"
        manager._checkpoint_store.commits.append({
            "worker_namespace": namespace,
            "generation": "periodic-00000001",
        })
        await driver._publish_checkpoint_generation(
            job_id="job-1",
            spec=spec,
            worker_namespace=namespace,
            generation="periodic-00000001",
            shard_manifest={"generation": "periodic-00000001"},
        )

    # First call seeds durable readiness; the final local shard performs the
    # only retry. Intermediate shards never repeat an O(fanout) S3 inventory.
    assert len(
        manager._checkpoint_store.checkpoint_set_attempts
    ) == 2
    assert len(manager._checkpoint_store.checkpoint_sets) == 1


async def test_checkpoint_pointer_failure_is_not_exposed_and_retries(
    tmp_path,
):
    manager = FakeManager(tmp_path)
    manager._checkpoint_store = FakeCheckpointStore()
    manager._checkpoint_store.commits.append({
        "worker_namespace": "shard-00000",
        "generation": "periodic-00000001",
    })
    manager._batch.job.latest_checkpoint_generation = (
        "periodic-00000000"
    )
    manager._batch.job.latest_checkpoint_committed_at = (
        "2026-07-28T00:00:00+00:00"
    )
    pointer_attempts: list[tuple[str, str, str]] = []

    async def persist_pointer(job_id, generation, committed_at):
        pointer_attempts.append((job_id, generation, committed_at))
        if len(pointer_attempts) == 1:
            raise OSError("injected journal fsync failure")

    manager._update_batch_checkpoint_generation = persist_pointer
    spec = JobSpec.model_validate({
        "name": "checkpoint-pointer-retry",
        "setup": {
            "repo": "https://github.com/example/bench.git",
            "resolved_commit": "a" * 40,
        },
        "run": {"command": "bench"},
        "fanout": {"workers": 1, "shard_by": "shard_index"},
        "collect": {"paths": ["results"], "checkpoint": True},
    })
    driver = ManagerFleetDriver(manager)
    publish = {
        "job_id": "job-1",
        "spec": spec,
        "worker_namespace": "shard-00000",
        "generation": "periodic-00000001",
        "shard_manifest": {"generation": "periodic-00000001"},
    }

    with pytest.raises(OSError, match="journal fsync failure"):
        await driver._publish_checkpoint_generation(**publish)
    assert manager._batch.job.latest_checkpoint_generation == (
        "periodic-00000000"
    )
    assert (
        "job-1",
        "periodic-00000001",
    ) not in manager._checkpoint_published_generations

    assert await driver._publish_checkpoint_generation(**publish) is True
    assert len(pointer_attempts) == 2
    assert manager._batch.job.latest_checkpoint_generation == (
        "periodic-00000001"
    )
    assert (
        "job-1",
        "periodic-00000001",
    ) in manager._checkpoint_published_generations


async def test_concurrent_terminal_collects_publish_all_final_shards(
    tmp_path, monkeypatch,
):
    monkeypatch.setenv("ELASTIC_AGENT_RESULTS_S3_BUCKET", "result-bucket")
    monkeypatch.setenv(
        "ELASTIC_AGENT_MAX_COLLECTION_STAGING_BYTES", str(1024 * 1024),
    )
    monkeypatch.setenv(
        "ELASTIC_AGENT_MAX_COLLECTION_STAGING_TOTAL_BYTES",
        str(4 * 1024 * 1024),
    )
    monkeypatch.setenv(
        "ELASTIC_AGENT_MAX_COLLECTION_FILE_BYTES", str(1024 * 1024),
    )
    monkeypatch.setenv("ELASTIC_AGENT_MAX_COLLECTION_STAGING_OBJECTS", "100")
    monkeypatch.setenv(
        "ELASTIC_AGENT_MAX_COLLECTION_STAGING_TOTAL_OBJECTS", "1000",
    )

    async def fake_subprocess(*args, **kwargs):
        return FakeProcess()

    monkeypatch.setattr(
        "elastic_agent.core.manager_fleet_driver.asyncio.create_subprocess_exec",
        fake_subprocess,
    )

    barrier = threading.Barrier(2, timeout=2)

    class ConcurrentFinalStore(FakeCheckpointStore):
        def commit(self, **kwargs):
            manifest = super().commit(**kwargs)
            barrier.wait()
            return manifest

    class Uploader:
        def sync_worker(self, _job_id, _namespace, **_kwargs):
            return 1

    manager = FakeManager(tmp_path)
    manager._s3_uploader = Uploader()
    store = ConcurrentFinalStore()
    store.commits.extend([
        {
            "worker_namespace": "shard-00000",
            "generation": "periodic-00000001",
        },
        {
            "worker_namespace": "shard-00001",
            "generation": "periodic-00000001",
        },
    ])
    manager._checkpoint_store = store
    for index, worker_id in enumerate(("worker-a", "worker-b")):
        result = (
            Path(manager.collected_root)
            / "job-1"
            / "workers"
            / f"shard-{index:05d}"
            / "results"
        )
        result.mkdir(parents=True)
        (result / "answer.txt").write_text(worker_id)
        run = manager._batch.job.runs[worker_id]
        run.checkpoint_generation = "final"
        run.checkpoint_set_generation = f"terminal-{index}"
        run.checkpoint_shard_generations = {
            "shard-00000": (
                "final" if index == 0 else "periodic-00000001"
            ),
            "shard-00001": (
                "final" if index == 1 else "periodic-00000001"
            ),
        }
        run.last_checkpoint_generation = "periodic-00000001"

    spec = JobSpec.model_validate({
        "name": "checkpoint-final-race",
        "setup": {
            "repo": "https://github.com/example/bench.git",
            "resolved_commit": "a" * 40,
            "target_dir": str(tmp_path / "remote-work"),
        },
        "run": {"command": "bench"},
        "fanout": {"workers": 2, "shard_by": "shard_index"},
        "collect": {"paths": ["results"], "checkpoint": True},
    })
    driver = ManagerFleetDriver(manager)

    await asyncio.gather(
        driver.collect("worker-a", spec, "job-1"),
        driver.collect("worker-b", spec, "job-1"),
    )

    assert any(
        checkpoint_set["shard_generations"] == {
            "shard-00000": "final",
            "shard-00001": "final",
        }
        for checkpoint_set in store.checkpoint_sets
    )


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


async def test_restart_collection_projection_commits_checkpoint_set(
    tmp_path, monkeypatch,
):
    from elastic_agent.manager.manager import (
        _load_recovery_collection_spec,
    )

    monkeypatch.setenv(
        "ELASTIC_AGENT_RESULTS_S3_BUCKET", "result-bucket",
    )

    async def fake_subprocess(*args, **kwargs):
        return FakeProcess()

    monkeypatch.setattr(
        "elastic_agent.core.manager_fleet_driver.asyncio.create_subprocess_exec",
        fake_subprocess,
    )

    class Uploader:
        def sync_worker(self, _job_id, _namespace, **_kwargs):
            return 1

    manager = FakeManager(tmp_path)
    manager._batch = None
    manager._s3_uploader = Uploader()
    manager._checkpoint_store = FakeCheckpointStore()
    manager.registry.nodes["worker-a"].metadata = {
        "job_id": "job-restart",
        "shard_index": 0,
    }
    result = Path(manager.collected_root) / (
        "job-restart/workers/shard-00000/results"
    )
    result.mkdir(parents=True)
    (result / "answer.json").write_text("42")
    source = JobSpec.model_validate({
        "name": "restart-checkpoint",
        "setup": {
            "repo": "https://github.com/example/bench.git",
            "target_dir": str(tmp_path / "remote-work"),
            "resolved_commit": "a" * 40,
        },
        "run": {"command": "bench --shard {{shard_index}}"},
        "fanout": {"workers": 1, "shard_by": "shard_index"},
        "collect": {"paths": ["results"], "checkpoint": True},
    })
    recovery_spec = _load_recovery_collection_spec(
        source.model_dump(mode="json")
    )

    await ManagerFleetDriver(manager).collect(
        "worker-a", recovery_spec, "job-restart",
    )

    assert manager._checkpoint_store.commits[0][
        "worker_namespace"
    ] == "shard-00000"
    assert manager._checkpoint_store.checkpoint_sets[0][
        "generation"
    ] == "final"


async def test_recovered_worker_quiescence_stops_all_runtime_units(
    tmp_path, monkeypatch,
):
    commands = []

    async def fake_execute(self, command, timeout=300, env=None, cwd=None):
        commands.append(command)
        return 0, "", ""

    monkeypatch.setattr(
        "elastic_agent.core.bootstrap.SSHExecutor.execute",
        fake_execute,
    )
    manager = FakeManager(tmp_path)

    await ManagerFleetDriver(manager).quiesce_recovered_worker(
        "worker-a", "job-1", _spec(tmp_path),
    )

    assert len(commands) == 1
    assert "systemctl stop \"$unit\"" in commands[0]
    assert "systemctl mask --runtime \"$unit\"" in commands[0]
    assert "ea-task-supervisor.service" in commands[0]
    assert "elastic-agent-task-supervisor.service" in commands[0]
    assert "ea-runtime.service" in commands[0]
    assert "elastic-agent-runtime.service" in commands[0]
    assert "ea-task@*.service" in commands[0]
    assert 'user@${run_uid}.service' in commands[0]
    assert 'loginctl disable-linger "$run_user"' in commands[0]
    assert "cron.service" in commands[0]
    assert "docker rm -f" in commands[0]
    assert "containerd.service" in commands[0]
    assert "/proc" in commands[0]
    assert str(tmp_path / "remote-work") not in commands[0]
    assert commands[0].index("systemctl stop") < commands[0].index(
        "systemctl is-active"
    )
    assert commands[0].index("systemctl mask --runtime") < (
        commands[0].index("systemctl stop")
    )


async def test_recovered_worker_quiescence_fails_closed(
    tmp_path, monkeypatch,
):
    async def fake_execute(
        self, command, timeout=300, env=None, cwd=None,
    ):
        assert "docker rm -f" in command
        return 1, "", "containerd-shim remained"

    monkeypatch.setattr(
        "elastic_agent.core.bootstrap.SSHExecutor.execute",
        fake_execute,
    )
    manager = FakeManager(tmp_path)

    with pytest.raises(
        RuntimeError, match="cannot prove.*quiescence",
    ):
        await ManagerFleetDriver(
            manager
        ).quiesce_recovered_worker(
            "worker-a", "job-1", _spec(tmp_path),
        )


async def test_recovery_staging_budget_is_atomic_across_jobs(
    tmp_path, monkeypatch,
):
    monkeypatch.setattr(
        "elastic_agent.core.manager_fleet_driver.shutil.disk_usage",
        lambda _path: SimpleNamespace(
            free=4 * 1024 * 1024 * 1024,
        ),
    )
    monkeypatch.setenv(
        "ELASTIC_AGENT_MAX_RECOVERY_STAGING_BYTES", "100"
    )
    monkeypatch.setenv(
        "ELASTIC_AGENT_MAX_RECOVERY_STAGING_TOTAL_BYTES", "100"
    )
    monkeypatch.setenv(
        "ELASTIC_AGENT_MAX_RECOVERY_STAGING_OBJECTS", "100"
    )
    monkeypatch.setenv(
        "ELASTIC_AGENT_MAX_RECOVERY_STAGING_TOTAL_OBJECTS", "100"
    )
    manager = FakeManager(tmp_path)
    driver = ManagerFleetDriver(manager)

    results = await asyncio.gather(
        driver._reserve_recovery_staging(
            job_id="job-a",
            total_bytes=60,
            total_objects=60,
        ),
        driver._reserve_recovery_staging(
            job_id="job-b",
            total_bytes=60,
            total_objects=60,
        ),
        return_exceptions=True,
    )

    assert sum(result is None for result in results) == 1
    failure = next(
        result for result in results
        if isinstance(result, RuntimeError)
    )
    assert "budget is exhausted" in str(failure)
    winner = next(iter(manager._recovery_staging_reservations))
    await driver._release_recovery_staging_reservation(winner)
    assert manager._recovery_staging_reservations == {}


async def test_recovery_staging_disk_is_reserved_atomically_across_jobs(
    tmp_path, monkeypatch,
):
    ten_gib = 10 * 1024 * 1024 * 1024
    monkeypatch.setenv(
        "ELASTIC_AGENT_MAX_RECOVERY_STAGING_BYTES", str(ten_gib)
    )
    monkeypatch.setenv(
        "ELASTIC_AGENT_MAX_RECOVERY_STAGING_TOTAL_BYTES",
        str(2 * ten_gib),
    )
    monkeypatch.setattr(
        "elastic_agent.core.manager_fleet_driver.shutil.disk_usage",
        lambda _path: SimpleNamespace(
            free=15 * 1024 * 1024 * 1024,
        ),
    )
    driver = ManagerFleetDriver(FakeManager(tmp_path))

    results = await asyncio.gather(
        driver._reserve_recovery_staging(
            job_id="job-a", total_bytes=ten_gib, total_objects=1,
        ),
        driver._reserve_recovery_staging(
            job_id="job-b", total_bytes=ten_gib, total_objects=1,
        ),
        return_exceptions=True,
    )

    assert sum(result is None for result in results) == 1
    failure = next(
        result for result in results if isinstance(result, RuntimeError)
    )
    assert "insufficient Manager disk" in str(failure)


async def test_recovery_staging_reserves_one_block_per_small_object(
    tmp_path, monkeypatch,
):
    object_count = 100
    logical_bytes = 100
    block_size = 4096
    monkeypatch.setenv(
        "ELASTIC_AGENT_MAX_RECOVERY_STAGING_BYTES", "1000",
    )
    monkeypatch.setenv(
        "ELASTIC_AGENT_MAX_RECOVERY_STAGING_TOTAL_BYTES", "1000",
    )
    monkeypatch.setenv(
        "ELASTIC_AGENT_MAX_RECOVERY_STAGING_OBJECTS", "1000",
    )
    monkeypatch.setenv(
        "ELASTIC_AGENT_MAX_RECOVERY_STAGING_TOTAL_OBJECTS", "1000",
    )
    monkeypatch.setattr(
        "elastic_agent.core.manager_fleet_driver.shutil.disk_usage",
        lambda _path: SimpleNamespace(
            free=(
                1024 * 1024 * 1024
                + logical_bytes
                + object_count * block_size
                - 1
            ),
        ),
    )
    monkeypatch.setattr(
        "elastic_agent.core.manager_fleet_driver.os.statvfs",
        lambda _path: SimpleNamespace(
            f_favail=100_000,
            f_frsize=block_size,
        ),
    )

    with pytest.raises(RuntimeError, match="insufficient Manager disk"):
        await ManagerFleetDriver(
            FakeManager(tmp_path)
        )._reserve_recovery_staging(
            job_id="job-tiny-files",
            total_bytes=logical_bytes,
            total_objects=object_count,
        )


async def test_recovery_staging_inodes_are_reserved_atomically_across_jobs(
    tmp_path, monkeypatch,
):
    monkeypatch.setenv(
        "ELASTIC_AGENT_MAX_RECOVERY_STAGING_BYTES", "100"
    )
    monkeypatch.setenv(
        "ELASTIC_AGENT_MAX_RECOVERY_STAGING_TOTAL_BYTES", "200"
    )
    monkeypatch.setenv(
        "ELASTIC_AGENT_MAX_RECOVERY_STAGING_OBJECTS", "100"
    )
    monkeypatch.setenv(
        "ELASTIC_AGENT_MAX_RECOVERY_STAGING_TOTAL_OBJECTS", "200"
    )
    monkeypatch.setattr(
        "elastic_agent.core.manager_fleet_driver.shutil.disk_usage",
        lambda _path: SimpleNamespace(
            free=4 * 1024 * 1024 * 1024,
        ),
    )
    monkeypatch.setattr(
        "elastic_agent.core.manager_fleet_driver.os.statvfs",
        lambda _path: SimpleNamespace(f_favail=10_060),
    )
    manager = FakeManager(tmp_path)
    driver = ManagerFleetDriver(manager)

    results = await asyncio.gather(
        driver._reserve_recovery_staging(
            job_id="job-a", total_bytes=1, total_objects=60,
        ),
        driver._reserve_recovery_staging(
            job_id="job-b", total_bytes=1, total_objects=60,
        ),
        return_exceptions=True,
    )

    assert sum(result is None for result in results) == 1
    failure = next(
        result for result in results if isinstance(result, RuntimeError)
    )
    assert "insufficient Manager inodes" in str(failure)
    winner = next(iter(manager._recovery_staging_reservations))
    await driver._release_recovery_staging_reservation(winner)


async def test_collection_staging_disk_is_reserved_across_fanout(
    tmp_path, monkeypatch,
):
    ten_gib = 10 * 1024 * 1024 * 1024
    monkeypatch.setenv(
        "ELASTIC_AGENT_MAX_COLLECTION_STAGING_TOTAL_BYTES",
        str(2 * ten_gib),
    )
    monkeypatch.setenv(
        "ELASTIC_AGENT_MAX_COLLECTION_STAGING_TOTAL_OBJECTS", "10",
    )
    monkeypatch.setattr(
        "elastic_agent.core.manager_fleet_driver.shutil.disk_usage",
        lambda _path: SimpleNamespace(
            free=15 * 1024 * 1024 * 1024,
        ),
    )
    manager = FakeManager(tmp_path)
    driver = ManagerFleetDriver(manager)

    first = await driver._reserve_collection_staging(
        reservation_id="job/shard-00000",
        max_bytes=ten_gib,
        max_objects=1,
    )
    second = asyncio.create_task(driver._reserve_collection_staging(
        reservation_id="job/shard-00001",
        max_bytes=ten_gib,
        max_objects=1,
    ))
    await asyncio.sleep(0)

    assert first == (ten_gib, 1)
    assert second.done() is False
    await driver._release_collection_staging_reservation(
        "job/shard-00000",
    )
    assert await asyncio.wait_for(second, timeout=1) == (ten_gib, 1)
    await driver._release_collection_staging_reservation(
        "job/shard-00001",
    )
    assert manager._collection_staging_reservations == {}


async def test_collection_staging_allows_disjoint_fanout_with_capacity(
    tmp_path, monkeypatch,
):
    ten_gib = 10 * 1024 * 1024 * 1024
    monkeypatch.setenv(
        "ELASTIC_AGENT_MAX_COLLECTION_STAGING_TOTAL_BYTES",
        str(2 * ten_gib),
    )
    monkeypatch.setattr(
        "elastic_agent.core.manager_fleet_driver.shutil.disk_usage",
        lambda _path: SimpleNamespace(
            free=25 * 1024 * 1024 * 1024,
        ),
    )
    manager = FakeManager(tmp_path)
    driver = ManagerFleetDriver(manager)

    reservations = await asyncio.gather(
        driver._reserve_collection_staging(
            reservation_id="job/shard-00000",
            max_bytes=ten_gib,
            max_objects=1,
        ),
        driver._reserve_collection_staging(
            reservation_id="job/shard-00001",
            max_bytes=ten_gib,
            max_objects=1,
        ),
    )

    assert reservations == [(ten_gib, 1), (ten_gib, 1)]
    assert set(manager._collection_staging_reservations) == {
        "job/shard-00000", "job/shard-00001",
    }
    await asyncio.gather(*(
        driver._release_collection_staging_reservation(reservation_id)
        for reservation_id in tuple(
            manager._collection_staging_reservations
        )
    ))
    assert manager._collection_staging_reservations == {}


async def test_prepare_and_restore_checkpoint_before_run(
    tmp_path, monkeypatch,
):
    monkeypatch.setenv("ELASTIC_AGENT_RESULTS_S3_BUCKET", "result-bucket")
    monkeypatch.setattr(
        "elastic_agent.core.manager_fleet_driver.shutil.disk_usage",
        lambda _path: SimpleNamespace(free=2 * 1024 * 1024 * 1024),
    )
    manager = FakeManager(tmp_path)
    manager._checkpoint_store = FakeCheckpointStore()
    source = JobSpec.model_validate({
        "name": "source",
        "setup": {
            "repo": "https://github.com/example/bench.git",
            "resolved_commit": "a" * 40,
        },
        "run": {"command": "capture"},
        "fanout": {"workers": 2, "shard_by": "shard_index"},
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
        "fanout": {"workers": 2, "shard_by": "shard_index"},
        "recovery": {
            "policy": "checkpoint",
            "source_job_id": "job-source",
            "paths": ["results"],
            "generation": "periodic-00000001",
        },
    })
    driver = ManagerFleetDriver(manager)
    manager._checkpoint_store.resolved_metadata[
        "recovery_contract_sha256"
    ] = (
        driver._checkpoint_contract_hash(source)
    )

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
        call["expected_metadata"]["recovery_contract_sha256"]
        == driver._checkpoint_contract_hash(source)
        for _kind, call in manager._checkpoint_store.restores
    )

    commands = []

    async def fake_subprocess(*args, **kwargs):
        commands.append(args)
        process = FakeProcess()
        process.pid = 12345
        return process

    monkeypatch.setattr(
        "elastic_agent.core.manager_fleet_driver.asyncio.create_subprocess_exec",
        fake_subprocess,
    )
    monkeypatch.setattr(
        driver,
        "_record_recovery_transfer_process",
        lambda _path, record, _pid: record,
    )

    async def fake_settle_transfer(_proc, record_path):
        driver._remove_recovery_transfer_record(record_path)

    monkeypatch.setattr(
        driver,
        "_settle_live_recovery_transfer",
        fake_settle_transfer,
    )
    transaction_calls = []

    async def fake_transaction(
        _executor,
        *,
        mode,
        payload,
        expected_descriptor_sha256,
    ):
        transaction_calls.append(
            (mode, payload, expected_descriptor_sha256)
        )
        return {
            "status": (
                "receiving" if mode == "prepare" else "installed"
            ),
            "descriptor_sha256": expected_descriptor_sha256,
        }

    monkeypatch.setattr(
        driver,
        "_execute_recovery_transaction",
        fake_transaction,
    )
    await driver.restore_recovery(
        "worker-a",
        "job-new",
        recovering,
        SimpleNamespace(shard_index=0),
    )

    assert [call[0] for call in transaction_calls] == [
        "prepare",
        "install",
    ]
    assert transaction_calls[0][1]["generation"] == (
        "periodic-00000001"
    )
    assert transaction_calls[0][1]["total_bytes"] == 2
    assert transaction_calls[0][1]["total_objects"] == 2
    assert transaction_calls[0][1]["run_user"] == "ubuntu"
    assert commands
    assert commands[0][0] == "rsync"
    assert "--safe-links" in commands[0]
    assert "--delete" in commands[0]
    assert "--delete-excluded" in commands[0]
    assert "--rsync-path" in commands[0]
    remote_rsync = commands[0][
        commands[0].index("--rsync-path") + 1
    ]
    assert remote_rsync.startswith(
        "/usr/bin/env ELASTIC_AGENT_RECOVERY_TRANSFER_ID="
    )
    assert remote_rsync.endswith(
        " /usr/bin/sudo -n /usr/bin/rsync"
    )
    assert "/var/lib/elastic-agent/recovery-transactions-v1" in (
        commands[0][-1]
    )
    assert commands[0][-1].endswith("/staged/results/")

    await driver.cleanup_recovery("job-new")
    assert not (
        Path(manager.config.registry.path).with_name("recovery-staging")
        / "job-new"
    ).exists()


async def test_restore_reaps_spawn_hidden_by_create_cancellation(
    tmp_path, monkeypatch,
):
    manager = FakeManager(tmp_path)
    driver = ManagerFleetDriver(manager)
    spec = JobSpec.model_validate({
        "name": "resume",
        "setup": {
            "target_dir": str(tmp_path / "remote-work"),
        },
        "run": {"command": "resume"},
        "recovery": {
            "policy": "checkpoint",
            "source_job_id": "job-source",
            "paths": ["results"],
            "generation": "periodic-00000001",
        },
    })
    staged = (
        driver._recovery_staging_root()
        / "job-new"
        / "shard-00000"
        / "results"
    )
    staged.mkdir(parents=True)
    (staged / "checkpoint.json").write_text("checkpoint")

    async def fake_transaction(
        _executor,
        *,
        mode,
        payload,
        expected_descriptor_sha256,
    ):
        assert mode == "prepare"
        return {
            "status": "receiving",
            "descriptor_sha256": expected_descriptor_sha256,
        }

    monkeypatch.setattr(
        driver,
        "_execute_recovery_transaction",
        fake_transaction,
    )
    original_create = asyncio.create_subprocess_exec
    spawned = []

    async def spawn_then_cancel(*_args, **kwargs):
        token = kwargs["env"][
            "ELASTIC_AGENT_RECOVERY_TRANSFER_ID"
        ]
        marker = (
            "ELASTIC_AGENT_RECOVERY_TRANSFER_ID="
            f"{token}"
        )
        process = await original_create(
            sys.executable,
            "-c",
            "import time; time.sleep(60)",
            marker,
            start_new_session=True,
            env=kwargs["env"],
        )
        spawned.append(process)
        raise asyncio.CancelledError

    monkeypatch.setattr(
        "elastic_agent.core.manager_fleet_driver."
        "asyncio.create_subprocess_exec",
        spawn_then_cancel,
    )

    with pytest.raises(asyncio.CancelledError):
        await driver.restore_recovery(
            "worker-a",
            "job-new",
            spec,
            SimpleNamespace(shard_index=0),
        )

    assert len(spawned) == 1
    await asyncio.wait_for(spawned[0].wait(), timeout=2)
    transfer_root = driver._recovery_transfer_job_root(
        "job-new",
        create=False,
    )
    assert list(transfer_root.glob("*.json")) == []
    assert staged.exists()


async def test_recovery_cleanup_quarantines_unsettled_transfer(
    tmp_path, monkeypatch,
):
    manager = FakeManager(tmp_path)
    manager._recovery_staging_budget_lock = asyncio.Lock()
    manager._recovery_staging_reservations = {"job-new": (10, 2)}
    driver = ManagerFleetDriver(manager)
    staging = (
        driver._recovery_staging_root()
        / "job-new"
        / "shard-00000"
    )
    staging.mkdir(parents=True)
    (staging / "payload").write_bytes(b"checkpoint")
    driver._create_recovery_transfer_record(
        job_id="job-new",
        shard_index=0,
        relative="results",
    )

    async def unsettled(_job_id):
        raise _UnsettledSubprocessError("still running")

    monkeypatch.setattr(
        driver,
        "_settle_recovery_transfer_records",
        unsettled,
    )

    with pytest.raises(RuntimeError, match="quarantined"):
        await driver.cleanup_recovery("job-new")

    assert staging.exists()
    assert "job-new" in manager._recovery_staging_reservations
    assert manager._recovery_staging_quarantines["job-new"] == (
        "still running"
    )
    retry = manager._recovery_transfer_cleanup_tasks["job-new"]
    retry.cancel()
    await asyncio.gather(retry, return_exceptions=True)


async def test_live_recovery_transfer_keeps_journal_until_group_is_gone(
    tmp_path, monkeypatch,
):
    manager = FakeManager(tmp_path)
    driver = ManagerFleetDriver(manager)
    record_path, _record = driver._create_recovery_transfer_record(
        job_id="job-new",
        shard_index=0,
        relative="results",
    )

    async def unsettled(_proc):
        raise _UnsettledSubprocessError("process group remains live")

    monkeypatch.setattr(
        "elastic_agent.core.manager_fleet_driver."
        "_terminate_subprocess_transaction",
        unsettled,
    )

    with pytest.raises(
        _UnsettledSubprocessError,
        match="process group remains live",
    ):
        await driver._settle_live_recovery_transfer(
            SimpleNamespace(pid=12345),
            record_path,
        )

    assert record_path.exists()


async def test_cancelled_recovery_cleanup_keeps_background_retry(
    tmp_path, monkeypatch,
):
    manager = FakeManager(tmp_path)
    driver = ManagerFleetDriver(manager)

    async def cancelled(_job_id):
        raise asyncio.CancelledError

    monkeypatch.setattr(
        driver,
        "_cleanup_recovery_once",
        cancelled,
    )

    with pytest.raises(asyncio.CancelledError):
        await driver.cleanup_recovery("job-new")

    assert manager._recovery_staging_quarantines["job-new"] == (
        "cleanup cancelled"
    )
    retry = manager._recovery_transfer_cleanup_tasks["job-new"]
    retry.cancel()
    await asyncio.gather(retry, return_exceptions=True)


async def test_startup_reaps_recorded_transfer_before_staging_cleanup(
    tmp_path,
):
    manager = FakeManager(tmp_path)
    driver = ManagerFleetDriver(manager)
    staging = (
        driver._recovery_staging_root()
        / "job-new"
        / "shard-00000"
    )
    staging.mkdir(parents=True)
    (staging / "payload").write_bytes(b"checkpoint")
    record_path, record = driver._create_recovery_transfer_record(
        job_id="job-new",
        shard_index=0,
        relative="results",
    )
    environment = dict(os.environ)
    environment[
        "ELASTIC_AGENT_RECOVERY_TRANSFER_ID"
    ] = record["token"]
    process = await asyncio.create_subprocess_exec(
        "/bin/sleep",
        "60",
        start_new_session=True,
        env=environment,
    )
    driver._record_recovery_transfer_process(
        record_path,
        record,
        process.pid,
    )
    waiter = asyncio.create_task(process.wait())

    await driver.cleanup_stale_recovery_staging()
    await asyncio.wait_for(waiter, timeout=2)

    assert process.returncode is not None
    assert not staging.exists()
    assert not record_path.exists()


async def test_startup_reaps_transfer_spawned_before_pid_journal(
    tmp_path,
):
    manager = FakeManager(tmp_path)
    driver = ManagerFleetDriver(manager)
    staging = (
        driver._recovery_staging_root()
        / "job-new"
        / "shard-00000"
    )
    staging.mkdir(parents=True)
    (staging / "payload").write_bytes(b"checkpoint")
    record_path, record = driver._create_recovery_transfer_record(
        job_id="job-new",
        shard_index=0,
        relative="results",
    )
    marker = (
        "ELASTIC_AGENT_RECOVERY_TRANSFER_ID="
        f"{record['token']}"
    )
    environment = dict(os.environ)
    environment[
        "ELASTIC_AGENT_RECOVERY_TRANSFER_ID"
    ] = record["token"]
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-c",
        "import time; time.sleep(60)",
        marker,
        start_new_session=True,
        env=environment,
    )
    waiter = asyncio.create_task(process.wait())

    await driver.cleanup_stale_recovery_staging()
    await asyncio.wait_for(waiter, timeout=2)

    assert process.returncode is not None
    assert not staging.exists()
    assert not record_path.exists()


async def test_prepare_recovery_reserves_checkpoint_wrapper_inodes(
    tmp_path, monkeypatch,
):
    monkeypatch.setenv("ELASTIC_AGENT_RESULTS_S3_BUCKET", "result-bucket")
    monkeypatch.setenv(
        "ELASTIC_AGENT_MAX_RECOVERY_STAGING_BYTES", "100",
    )
    monkeypatch.setenv(
        "ELASTIC_AGENT_MAX_RECOVERY_STAGING_TOTAL_BYTES", "100",
    )
    monkeypatch.setenv(
        "ELASTIC_AGENT_MAX_RECOVERY_STAGING_OBJECTS", "100",
    )
    monkeypatch.setenv(
        "ELASTIC_AGENT_MAX_RECOVERY_STAGING_TOTAL_OBJECTS", "100",
    )
    monkeypatch.setattr(
        "elastic_agent.core.manager_fleet_driver.shutil.disk_usage",
        lambda _path: SimpleNamespace(free=2 * 1024 * 1024 * 1024),
    )
    # The four manifest entries fit, but the Job root, two shard roots, and
    # two strict parent directories per shard require seven more inodes.
    monkeypatch.setattr(
        "elastic_agent.core.manager_fleet_driver.os.statvfs",
        lambda _path: SimpleNamespace(f_favail=10_010),
    )
    manager = FakeManager(tmp_path)
    manager._checkpoint_store = FakeCheckpointStore()
    source = JobSpec.model_validate({
        "name": "source",
        "setup": {
            "repo": "https://github.com/example/bench.git",
            "resolved_commit": "a" * 40,
        },
        "run": {"command": "capture"},
        "fanout": {"workers": 2, "shard_by": "shard_index"},
        "collect": {
            "paths": ["nested/checkpoints/results"],
            "checkpoint": True,
        },
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
            "repo": source.setup.repo,
            "resolved_commit": source.setup.resolved_commit,
        },
        "run": {"command": "resume"},
        "fanout": {"workers": 2, "shard_by": "shard_index"},
        "recovery": {
            "policy": "checkpoint",
            "source_job_id": "job-source",
            "paths": ["nested/checkpoints/results"],
            "generation": "periodic-00000001",
        },
    })
    driver = ManagerFleetDriver(manager)
    manager._checkpoint_store.resolved_metadata.update({
        "collect_paths": ["nested/checkpoints/results"],
        "recovery_contract_sha256": (
            driver._checkpoint_contract_hash(source)
        ),
    })

    with pytest.raises(
        RuntimeError,
        match="insufficient Manager inodes",
    ):
        await driver.prepare_recovery("job-new", recovering)

    assert manager._checkpoint_store.restores == []
    assert manager._recovery_staging_reservations == {}
    assert not (
        Path(manager.config.registry.path).with_name("recovery-staging")
        / "job-new"
    ).exists()


async def test_prepare_recovery_rechecks_live_inode_capacity_per_shard(
    tmp_path, monkeypatch,
):
    monkeypatch.setenv("ELASTIC_AGENT_RESULTS_S3_BUCKET", "result-bucket")
    monkeypatch.setenv(
        "ELASTIC_AGENT_MAX_RECOVERY_STAGING_BYTES", "100",
    )
    monkeypatch.setenv(
        "ELASTIC_AGENT_MAX_RECOVERY_STAGING_TOTAL_BYTES", "100",
    )
    monkeypatch.setenv(
        "ELASTIC_AGENT_MAX_RECOVERY_STAGING_OBJECTS", "100",
    )
    monkeypatch.setenv(
        "ELASTIC_AGENT_MAX_RECOVERY_STAGING_TOTAL_OBJECTS", "100",
    )
    monkeypatch.setattr(
        "elastic_agent.core.manager_fleet_driver.shutil.disk_usage",
        lambda _path: SimpleNamespace(free=2 * 1024 * 1024 * 1024),
    )
    stat_calls = 0

    def changing_statvfs(_path):
        nonlocal stat_calls
        stat_calls += 1
        return SimpleNamespace(
            f_favail=100_000 if stat_calls <= 2 else 10_002,
        )

    monkeypatch.setattr(
        "elastic_agent.core.manager_fleet_driver.os.statvfs",
        changing_statvfs,
    )
    manager = FakeManager(tmp_path)
    manager._checkpoint_store = FakeCheckpointStore()
    source = JobSpec.model_validate({
        "name": "source",
        "setup": {
            "repo": "https://github.com/example/bench.git",
            "resolved_commit": "a" * 40,
        },
        "run": {"command": "capture"},
        "fanout": {"workers": 2, "shard_by": "shard_index"},
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
            "repo": source.setup.repo,
            "resolved_commit": source.setup.resolved_commit,
        },
        "run": {"command": "resume"},
        "fanout": {"workers": 2, "shard_by": "shard_index"},
        "recovery": {
            "policy": "checkpoint",
            "source_job_id": "job-source",
            "paths": ["results"],
            "generation": "periodic-00000001",
        },
    })
    driver = ManagerFleetDriver(manager)
    manager._checkpoint_store.resolved_metadata[
        "recovery_contract_sha256"
    ] = driver._checkpoint_contract_hash(source)

    with pytest.raises(
        RuntimeError,
        match="exhausted Manager inode reserve",
    ):
        await driver.prepare_recovery("job-new", recovering)

    assert [
        call["worker_namespace"]
        for _kind, call in manager._checkpoint_store.restores
    ] == ["shard-00000"]
    assert manager._recovery_staging_reservations == {}
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
        "fanout": {"workers": 1, "shard_by": "shard_index"},
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
        "fanout": {"shard_by": "shard_index"},
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


async def test_legacy_recovery_is_disabled_even_with_old_final_summary():
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

    with pytest.raises(RuntimeError, match="legacy mutable.*disabled"):
        ManagerFleetDriver._validate_recovery_contract(
            payload, source, target, source_quiescent=True,
        )


async def test_interrupted_checkpoint_source_requires_quiescence_proof():
    source = JobSpec.model_validate({
        "name": "source",
        "setup": {
            "repo": "https://github.com/example/bench.git",
            "resolved_commit": "a" * 40,
        },
        "run": {"command": "bench"},
        "fanout": {"workers": 1, "shard_by": "shard_index"},
        "collect": {"paths": ["results"], "checkpoint": True},
    })
    target = JobSpec.model_validate({
        "name": "target",
        "setup": {
            "repo": source.setup.repo,
            "resolved_commit": source.setup.resolved_commit,
        },
        "run": {"command": "bench"},
        "fanout": {"workers": 1, "shard_by": "shard_index"},
        "recovery": {
            "policy": "checkpoint",
            "source_job_id": "job-source",
            "paths": ["results"],
        },
    })
    payload = {"submission_state": "running"}

    with pytest.raises(RuntimeError, match="proven quiescent"):
        ManagerFleetDriver._validate_recovery_contract(
            payload, source, target,
        )

    ManagerFleetDriver._validate_recovery_contract(
        payload,
        source,
        target,
        source_quiescent=True,
    )

    ManagerFleetDriver._validate_recovery_contract(
        {"submission_state": "suspended"},
        source,
        target,
        source_quiescent=True,
    )


async def test_recovery_rejects_changed_workload_dataset_identity():
    base = {
        "name": "source",
        "environment": {"profile": "ubuntu-agent-v1"},
        "setup": {
            "repo": "https://github.com/example/bench.git",
            "resolved_commit": "a" * 40,
            "s3_datasets": [{
                "uri": "s3://bench/source/shard-{{shard_id}}.jsonl",
                "dest": "/srv/input/shard.jsonl",
            }],
        },
        "run": {
            "command": "capture",
            "env": {"DATASET_SPLIT": "test"},
        },
        "fanout": {"workers": 1, "shard_by": "shard_index"},
        "collect": {"paths": ["results"], "checkpoint": True},
    }
    source = JobSpec.model_validate(base)
    changed = {
        **base,
        "name": "resume",
        "setup": {
            **base["setup"],
            "s3_datasets": [{
                "uri": "s3://bench/different/shard-{{shard_id}}.jsonl",
                "dest": "/srv/input/shard.jsonl",
            }],
        },
        "run": {
            "command": "resume",
            "env": {"DATASET_SPLIT": "test"},
        },
        "collect": {},
        "recovery": {
            "policy": "checkpoint",
            "source_job_id": "job-source",
            "paths": ["results"],
        },
    }
    target = JobSpec.model_validate(changed)

    with pytest.raises(RuntimeError, match="workload inputs must match"):
        ManagerFleetDriver._validate_recovery_contract(
            {"submission_state": "failed"},
            source,
            target,
            source_quiescent=True,
        )


async def test_recovery_identity_binds_account_auth_kind():
    base = {
        "name": "source",
        "setup": {
            "repo": "https://github.com/example/bench.git",
            "resolved_commit": "a" * 40,
        },
        "run": {"command": "capture"},
        "account": {"agent_type": "codex", "auth_kind": "oauth"},
        "fanout": {"workers": 1, "shard_by": "shard_index"},
        "collect": {"paths": ["results"], "checkpoint": True},
    }
    source = JobSpec.model_validate(base)
    target = JobSpec.model_validate({
        **base,
        "name": "resume",
        "run": {"command": "resume"},
        "account": {**base["account"], "auth_kind": "agent_api"},
        "collect": {},
        "recovery": {
            "policy": "checkpoint",
            "source_job_id": "job-source",
            "paths": ["results"],
        },
    })
    changed_auth_source = source.model_copy(update={
        "account": source.account.model_copy(
            update={"auth_kind": "agent_api"}
        ),
    })

    assert ManagerFleetDriver._workload_recovery_identity(source)[
        "auth_kind"
    ] == "oauth"
    assert (
        ManagerFleetDriver._checkpoint_contract_hash(source)
        != ManagerFleetDriver._checkpoint_contract_hash(changed_auth_source)
    )
    with pytest.raises(RuntimeError, match="workload inputs must match"):
        ManagerFleetDriver._validate_recovery_contract(
            {"submission_state": "failed"}, source, target,
            source_quiescent=True,
        )


async def test_recovery_identity_does_not_bind_account_selection_history():
    base = {
        "name": "source",
        "setup": {
            "repo": "https://github.com/example/bench.git",
            "resolved_commit": "a" * 40,
        },
        "run": {"command": "capture"},
        "account": {
            "agent_type": "codex",
            "auth_kind": "oauth",
            "ids": ["oauth-a"],
        },
        "fanout": {"workers": 1, "shard_by": "shard_index"},
        "collect": {"paths": ["results"], "checkpoint": True},
    }
    source = JobSpec.model_validate(base)
    target = JobSpec.model_validate({
        **base,
        "name": "resume",
        "run": {"command": "resume"},
        "account": {
            **base["account"],
            "ids": [],
            "exclude_ids": ["oauth-a"],
        },
        "collect": {},
        "recovery": {
            "policy": "checkpoint",
            "source_job_id": "job-source",
            "paths": ["results"],
        },
    })

    assert (
        ManagerFleetDriver._workload_recovery_identity(source)
        == ManagerFleetDriver._workload_recovery_identity(target)
    )
    selection_changed = source.model_copy(update={
        "account": source.account.model_copy(update={
            "ids": [],
            "exclude_ids": ["oauth-a"],
        }),
    })
    assert (
        ManagerFleetDriver._checkpoint_contract_hash(source)
        == ManagerFleetDriver._checkpoint_contract_hash(selection_changed)
    )
    ManagerFleetDriver._validate_recovery_contract(
        {"submission_state": "failed"},
        source,
        target,
        source_quiescent=True,
    )


async def test_v2_checkpoint_is_compatible_only_with_default_any_auth_kind():
    source = JobSpec.model_validate({
        "name": "source",
        "setup": {
            "repo": "https://github.com/example/bench.git",
            "resolved_commit": "a" * 40,
        },
        "run": {"command": "capture"},
        "fanout": {"workers": 1, "shard_by": "shard_index"},
        "collect": {"paths": ["results"], "checkpoint": True},
    })
    target = JobSpec.model_validate({
        "name": "resume",
        "setup": {
            "repo": source.setup.repo,
            "resolved_commit": source.setup.resolved_commit,
        },
        "run": {"command": "resume"},
        "fanout": {"workers": 1, "shard_by": "shard_index"},
        "recovery": {
            "policy": "checkpoint",
            "source_job_id": "job-source",
            "paths": ["results"],
            "generation": "periodic-00000001",
        },
    })
    checkpoint_set = {
        "generation": "periodic-00000001",
        "total_bytes": 1,
        "total_objects": 1,
        "metadata": {
            "resolved_commit": source.setup.resolved_commit,
            "recovery_contract_version": 2,
            "recovery_contract_sha256": (
                ManagerFleetDriver._checkpoint_contract_hash(
                    source, contract_version=2,
                )
            ),
            "fanout_workers": 1,
            "shard_by": "shard_index",
            "collect_paths": ["results"],
            "collect_exclude": [],
        },
        "shards": [{
            "worker_namespace": "shard-00000",
            "generation": "periodic-00000001",
            "manifest_sha256": "0" * 64,
            "total_bytes": 1,
            "total_objects": 1,
        }],
    }

    shards = ManagerFleetDriver._validate_resolved_checkpoint_set(
        checkpoint_set, source_spec=source, target_spec=target,
    )
    assert list(shards) == ["shard-00000"]

    oauth_source = source.model_copy(update={
        "account": source.account.model_copy(update={"auth_kind": "oauth"}),
    })
    checkpoint_set["metadata"]["recovery_contract_sha256"] = (
        ManagerFleetDriver._checkpoint_contract_hash(
            oauth_source, contract_version=2,
        )
    )
    with pytest.raises(RuntimeError, match="metadata does not match"):
        ManagerFleetDriver._validate_resolved_checkpoint_set(
            checkpoint_set, source_spec=oauth_source, target_spec=target,
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
        def sync_worker(self, job_id, namespace, **_kwargs):
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
    await driver.stop_command(
        "worker-a",
        "task-2",
        signal="SIGINT",
        scope="process",
        escalate=False,
    )

    assert manager.scale_in_calls == [(["worker-a", "worker-b"], True)]
    assert manager.removed_nodes == ["worker-a", "worker-b"]
    assert manager.connection_manager.stopped == [
        ("worker-a", "task-1", "SIGINT"),
        ("worker-a", "task-2", "SIGINT"),
    ]
    assert manager.connection_manager.stop_policies == [
        ("group", True),
        ("process", False),
    ]


async def test_interrupt_scale_in_retains_exact_tombstone_until_terminal_journal(
    tmp_path,
):
    manager = FakeManager(tmp_path)
    driver = ManagerFleetDriver(manager)
    spec = JobSpec.model_validate({
        "name": "interrupt-tombstone",
        "run": {"command": "run", "resume_command": "resume"},
        "fanout": {"workers": 1, "shard_by": "shard_index"},
        "collect": {"paths": ["results"], "checkpoint": True},
    })
    persist_job_spec(manager.config.registry.path, "job-1", spec)
    update_job_checkpoint(
        manager.config.registry.path,
        "job-1",
        "periodic-00000001",
        committed_at="2026-07-29T12:00:00+00:00",
    )
    update_job_interrupt_intent(
        manager.config.registry.path,
        "job-1",
        "a" * 64,
        summary={
            "state": "suspending",
            "done": False,
            "interrupt_requested": True,
            "resume_available": False,
        },
    )

    await driver.release_ordinary_for_interrupt(
        "worker-a",
        "job-1",
        0,
        collected=True,
        collection_error=None,
    )

    retained = await manager.registry.get("worker-a")
    assert retained is not None
    assert retained.status == NodeStatus.TERMINATED
    assert retained.metadata["interrupt_cleanup_proof"] == {
        "schema": 1,
        "job_id": "job-1",
        "worker_id": "worker-a",
        "instance_id": "worker-a",
        "shard_index": 0,
        "collection_attempted": True,
        "collected": True,
        "collection_error": None,
    }
    assert manager.removed_nodes == []

    update_job_state(
        manager.config.registry.path,
        "job-1",
        "suspended",
        summary={
            "state": "suspended",
            "done": True,
            "cleanup_pending": 0,
            "interrupt_requested": True,
            "resume_available": True,
            "resume_generation": "periodic-00000001",
            "resume_committed_at": "2026-07-29T12:00:00+00:00",
        },
    )
    await driver.finalize_interrupt_tombstones(
        "job-1",
        ["worker-a"],
    )
    assert await manager.registry.get("worker-a") is None
    assert manager.removed_nodes == ["worker-a"]


async def test_interrupt_tombstone_refuses_other_job_identity(tmp_path):
    manager = FakeManager(tmp_path)
    driver = ManagerFleetDriver(manager)

    with pytest.raises(RuntimeError, match="Job/shard ownership"):
        await driver.release_ordinary_for_interrupt(
            "worker-a",
            "job-other",
            0,
            collected=True,
            collection_error=None,
        )

    assert await manager.registry.get("worker-a") is not None
    assert manager.scale_in_calls == []
    assert manager.removed_nodes == []


async def test_bound_interrupt_proof_is_durable_before_release(tmp_path):
    manager = FakeManager(tmp_path)
    lease = SimpleNamespace(
        lease_id="lease-1",
        account_id="account-1",
        job_id="job-1",
        slot=0,
        worker_id="worker-a",
        recovery_collection_attempted=False,
    )
    updates = []

    class LeaseStore:
        async def get_lease(self, lease_id):
            assert lease_id == "lease-1"
            return lease

        async def update_lease(self, lease_id, **fields):
            updates.append((lease_id, fields))
            for name, value in fields.items():
                setattr(lease, name, value)
            return lease

    manager.account_binding_store = LeaseStore()
    assignment = SimpleNamespace(
        lease_id="lease-1",
        account_id="account-1",
        job_id="job-1",
        slot=0,
    )

    await ManagerFleetDriver(manager).record_bound_interrupt_proof(
        assignment,
        "worker-a",
        collected=False,
        collection_error="checkpoint upload failed",
    )

    assert updates == [(
        "lease-1",
        {
            "recovery_collection_attempted": True,
            "recovery_collected": False,
            "recovery_collection_error": "checkpoint upload failed",
        },
    )]
    assert lease.recovery_collection_attempted is True


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


async def test_run_command_persists_prompt_before_remote_dispatch(tmp_path):
    manager = FakeManager(tmp_path)
    manager.job_log_store = JobLogStore(tmp_path / "job-logs")
    task_id = "job-prompt:worker-a:abcdef"
    prompt = {
        "schema": 1,
        "agent_type": "codex",
        "capture_mode": "declared",
        "complete": False,
        "components": {
            "system": {
                "text": "system rules",
                "sha256": "97e2b5a8fd07a75081a1205a6f5154b39730b01846a2a399eaf5ae41b00b22a1",
                "bytes": 12,
            },
        },
        "sources": [],
        "unavailable_components": ["provider_builtin_system_prompt"],
        "invocation": {"argv_sha256": "a" * 64, "resumed": False},
    }

    async def execute(**_kwargs):
        snapshot = manager.job_log_store.read_job("job-prompt")[0]
        assert snapshot["complete"] is False
        assert snapshot["prompt"]["components"]["system"]["text"] == "system rules"

    manager.connection_manager.execute = execute

    driver = ManagerFleetDriver(manager)
    await driver.stage_prompt_metadata(
        "worker-a",
        task_id,
        "job-prompt",
        prompt,
    )
    await driver.run_command(
        "worker-a",
        task_id,
        ["codex", "exec", "task"],
        ".",
        {},
        60,
        "job-prompt",
        False,
    )

    assert manager.job_log_store.read_job("job-prompt")[0]["prompt"] == prompt
