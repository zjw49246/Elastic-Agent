"""Focused tests for ManagerFleetDriver result durability and teardown."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from elastic_agent.core.job_spec import JobSpec
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
