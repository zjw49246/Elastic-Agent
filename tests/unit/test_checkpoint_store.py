"""Immutable S3 checkpoint commit/restore tests."""

from __future__ import annotations

import hashlib
import io
import json
import os
import stat
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Callable

import pytest

from elastic_agent.core.checkpoint_store import (
    CheckpointError,
    S3CheckpointStore,
)


class FakeClientError(RuntimeError):
    def __init__(self, code: str, status: int):
        super().__init__(code)
        self.response = {
            "Error": {"Code": code},
            "ResponseMetadata": {"HTTPStatusCode": status},
        }


class _Paginator:
    def __init__(self, client):
        self._client = client

    def paginate(self, *, Bucket, Prefix):  # noqa: N803
        assert Bucket == self._client.bucket
        objects = [
            {"Key": key, "Size": len(value)}
            for key, value in sorted(self._client.objects.items())
            if key.startswith(Prefix)
        ]
        yield {"Contents": objects}


class FakeS3:
    def __init__(self):
        self.bucket = "results"
        self.objects: dict[str, bytes] = {}
        self.metadata: dict[str, dict[str, str]] = {}
        self.operations: list[tuple[str, str]] = []
        self.put_kwargs: list[dict[str, Any]] = []
        self.upload_hook: Callable[[], None] | None = None

    def upload_fileobj(  # noqa: N803
        self, stream, bucket, key, ExtraArgs=None,  # noqa: N803
    ):
        assert bucket == self.bucket
        if self.upload_hook is not None:
            self.upload_hook()
        self.objects[key] = stream.read()
        self.metadata[key] = dict((ExtraArgs or {}).get("Metadata") or {})
        self.operations.append(("upload", key))

    def put_object(self, *, Bucket, Key, Body, **kwargs):  # noqa: N803
        assert Bucket == self.bucket
        if kwargs.get("IfNoneMatch") == "*" and Key in self.objects:
            raise FakeClientError("PreconditionFailed", 412)
        self.objects[Key] = bytes(Body)
        self.metadata[Key] = dict(kwargs.get("Metadata") or {})
        self.operations.append(("put", Key))
        self.put_kwargs.append({"Key": Key, **kwargs})

    def get_object(self, *, Bucket, Key):  # noqa: N803
        assert Bucket == self.bucket
        if Key not in self.objects:
            raise KeyError(Key)
        return {
            "Body": io.BytesIO(self.objects[Key]),
            "ContentLength": len(self.objects[Key]),
        }

    def head_object(self, *, Bucket, Key):  # noqa: N803
        assert Bucket == self.bucket
        if Key not in self.objects:
            raise KeyError(Key)
        return {
            "ContentLength": len(self.objects[Key]),
            "Metadata": dict(self.metadata.get(Key) or {}),
        }

    def delete_objects(self, *, Bucket, Delete):  # noqa: N803
        assert Bucket == self.bucket
        for item in Delete["Objects"]:
            key = item["Key"]
            self.objects.pop(key, None)
            self.metadata.pop(key, None)
            self.operations.append(("delete", key))
        return {"Deleted": list(Delete["Objects"])}

    def get_paginator(self, operation):
        assert operation == "list_objects_v2"
        return _Paginator(self)


def _store(client: FakeS3) -> S3CheckpointStore:
    return S3CheckpointStore(
        bucket=client.bucket,
        prefix="jobs",
        client=client,
        max_objects=100,
        max_total_bytes=1024 * 1024,
        max_manifest_bytes=1024 * 1024,
    )


def test_commit_is_immutable_manifest_last_and_round_trips(
    tmp_path: Path,
):
    source = tmp_path / "source"
    (source / "results" / "nested").mkdir(parents=True)
    (source / "results" / "a.json").write_text("alpha")
    (source / "results" / "nested" / "b.json").write_text("beta")
    (source / "results" / "nested" / "core").write_bytes(b"core dump")
    (source / "results" / "scratch.tmp").write_text("temporary")
    client = FakeS3()
    store = _store(client)

    manifest = store.commit(
        job_id="job-new",
        worker_namespace="shard-00000",
        source_root=source,
        paths=["results"],
        exclude=["**/core", "*.tmp"],
        generation="20260729T010203Z-test",
        metadata={
            "resolved_commit": "a" * 40,
            "job_spec_sha256": "b" * 64,
            "command_sha256": "c" * 64,
        },
    )

    committed_key = (
        "jobs/job-new/workers/shard-00000/checkpoints/"
        "20260729T010203Z-test/COMMITTED.json"
    )
    assert client.operations[-1] == ("put", committed_key)
    assert manifest["generation"] == "20260729T010203Z-test"
    assert [entry["path"] for entry in manifest["files"]] == [
        "results/a.json",
        "results/nested/b.json",
    ]
    assert manifest["schema_version"] == 2
    assert all(
        "/job-new/checkpoint-blobs/" in entry["object_key"]
        for entry in manifest["files"]
    )
    assert client.put_kwargs[-1]["IfNoneMatch"] == "*"

    restored = tmp_path / "restored"
    restored_manifest = store.restore_checkpoint(
        source_job_id="job-new",
        worker_namespace="shard-00000",
        destination=restored,
        paths=["results"],
    )

    assert restored_manifest["generation"] == manifest["generation"]
    assert (restored / "results" / "a.json").read_text() == "alpha"
    assert (restored / "results" / "nested" / "b.json").read_text() == "beta"
    assert not (restored / "results" / "nested" / "core").exists()
    assert not (restored / "results" / "scratch.tmp").exists()


def test_restore_rejects_corrupt_blob_and_removes_partial_tree(tmp_path: Path):
    source = tmp_path / "source"
    (source / "results").mkdir(parents=True)
    (source / "results" / "answer.json").write_text("correct")
    client = FakeS3()
    store = _store(client)
    manifest = store.commit(
        job_id="job-source",
        worker_namespace="shard-00000",
        source_root=source,
        paths=["results"],
        generation="g1",
        metadata={},
    )
    client.objects[manifest["files"][0]["object_key"]] = b"corrupt"
    destination = tmp_path / "restore"

    with pytest.raises(CheckpointError, match="checksum"):
        store.restore_checkpoint(
            source_job_id="job-source",
            worker_namespace="shard-00000",
            destination=destination,
            paths=["results"],
        )

    assert not destination.exists()


def test_restore_surfaces_partial_tree_cleanup_failure(
    tmp_path: Path,
    monkeypatch,
):
    source = tmp_path / "source"
    (source / "results").mkdir(parents=True)
    (source / "results" / "answer.json").write_text("correct")
    client = FakeS3()
    store = _store(client)
    manifest = store.commit(
        job_id="job-cleanup",
        worker_namespace="shard-00000",
        source_root=source,
        paths=["results"],
        generation="g1",
    )
    client.objects[manifest["files"][0]["object_key"]] = b"corrupt"
    monkeypatch.setattr(
        "elastic_agent.core.checkpoint_store.shutil.rmtree",
        lambda _path: (_ for _ in ()).throw(OSError("cleanup denied")),
    )

    with pytest.raises(BaseExceptionGroup) as raised:
        store.restore_checkpoint(
            source_job_id="job-cleanup",
            worker_namespace="shard-00000",
            destination=tmp_path / "restore",
            paths=["results"],
        )

    assert any(
        isinstance(error, CheckpointError)
        for error in raised.value.exceptions
    )
    assert any(
        isinstance(error, OSError)
        for error in raised.value.exceptions
    )


def test_restore_rejects_manifest_path_traversal(tmp_path: Path):
    client = FakeS3()
    manifest_key = (
        "jobs/job-source/workers/shard-00000/checkpoints/g1/COMMITTED.json"
    )
    client.objects[manifest_key] = json.dumps({
        "schema_version": 1,
        "job_id": "job-source",
        "worker_namespace": "shard-00000",
        "generation": "g1",
        "paths": ["results"],
        "metadata": {},
        "committed_at": "2026-07-29T00:00:00+00:00",
        "total_bytes": 1,
        "files": [{
            "path": "../escape",
            "size": 1,
            "sha256": "0" * 64,
            "object_key": (
                "jobs/job-source/workers/shard-00000/checkpoints/g1/blobs/"
                + "0" * 64
            ),
        }],
    }).encode()

    with pytest.raises(CheckpointError, match="unsafe checkpoint path"):
        _store(client).restore_checkpoint(
            source_job_id="job-source",
            worker_namespace="shard-00000",
            destination=tmp_path / "restore",
            paths=["results"],
        )

    assert not (tmp_path / "escape").exists()


def test_commit_refuses_to_overwrite_committed_generation(tmp_path: Path):
    source = tmp_path / "source"
    (source / "results").mkdir(parents=True)
    (source / "results" / "answer.json").write_text("first")
    client = FakeS3()
    store = _store(client)
    store.commit(
        job_id="job-source",
        worker_namespace="shard-00000",
        source_root=source,
        paths=["results"],
        generation="g1",
    )
    first_objects = dict(client.objects)
    (source / "results" / "answer.json").write_text("second")

    with pytest.raises(CheckpointError, match="already committed"):
        store.commit(
            job_id="job-source",
            worker_namespace="shard-00000",
            source_root=source,
            paths=["results"],
            generation="g1",
        )

    assert client.objects == first_objects


def test_restore_rejects_checkpoint_metadata_mismatch(tmp_path: Path):
    source = tmp_path / "source"
    (source / "results").mkdir(parents=True)
    (source / "results" / "answer.json").write_text("correct")
    client = FakeS3()
    store = _store(client)
    store.commit(
        job_id="job-source",
        worker_namespace="shard-00000",
        source_root=source,
        paths=["results"],
        generation="g1",
        metadata={"resolved_commit": "a" * 40},
    )

    with pytest.raises(CheckpointError, match="metadata mismatch"):
        store.restore_checkpoint(
            source_job_id="job-source",
            worker_namespace="shard-00000",
            destination=tmp_path / "restore",
            paths=["results"],
            expected_metadata={"resolved_commit": "b" * 40},
        )

    assert not (tmp_path / "restore").exists()


def test_legacy_restore_requires_matching_collection_manifest(tmp_path: Path):
    client = FakeS3()
    root = "jobs/job-old/workers/shard-00000"
    client.objects[f"{root}/_elastic_agent/collection.json"] = json.dumps({
        "schema_version": 1,
        "job_id": "job-old",
        "worker_namespace": "shard-00000",
        "shard_index": 0,
        "paths": ["results"],
        "destination": "s3-worker-direct",
        "collected_at": "2026-07-29T00:00:00+00:00",
    }).encode()
    client.objects[f"{root}/results/a.json"] = b"one"
    client.objects[f"{root}/other/secret.txt"] = b"do not restore"

    manifest = _store(client).restore_legacy_collection(
        source_job_id="job-old",
        worker_namespace="shard-00000",
        destination=tmp_path / "restore",
        paths=["results"],
    )

    assert manifest["job_id"] == "job-old"
    assert (tmp_path / "restore" / "results" / "a.json").read_bytes() == b"one"
    assert not (tmp_path / "restore" / "other").exists()


def test_legacy_restore_rejects_manifest_for_other_job(tmp_path: Path):
    client = FakeS3()
    root = "jobs/job-old/workers/shard-00000"
    client.objects[f"{root}/_elastic_agent/collection.json"] = json.dumps({
        "schema_version": 1,
        "job_id": "job-attacker",
        "worker_namespace": "shard-00000",
        "paths": ["results"],
    }).encode()

    with pytest.raises(CheckpointError, match="collection manifest identity"):
        _store(client).restore_legacy_collection(
            source_job_id="job-old",
            worker_namespace="shard-00000",
            destination=tmp_path / "restore",
            paths=["results"],
        )


def test_legacy_restore_surfaces_cleanup_failure(tmp_path: Path, monkeypatch):
    client = FakeS3()
    root = "jobs/job-old/workers/shard-00000"
    client.objects[f"{root}/_elastic_agent/collection.json"] = b"not-json"
    monkeypatch.setattr(
        "elastic_agent.core.checkpoint_store.shutil.rmtree",
        lambda _path: (_ for _ in ()).throw(OSError("cleanup denied")),
    )

    with pytest.raises(BaseExceptionGroup) as raised:
        _store(client).restore_legacy_collection(
            source_job_id="job-old",
            worker_namespace="shard-00000",
            destination=tmp_path / "restore",
            paths=["results"],
        )

    assert len(raised.value.exceptions) == 2


def test_repeated_snapshots_upload_only_new_job_level_blobs(tmp_path: Path):
    source = tmp_path / "source"
    (source / "results").mkdir(parents=True)
    (source / "results" / "a.txt").write_text("same")
    (source / "results" / "b.txt").write_text("same")
    client = FakeS3()
    store = _store(client)

    first = store.commit(
        job_id="job-dedup",
        worker_namespace="shard-00000",
        source_root=source,
        paths=["results"],
        generation="g1",
    )
    store.commit(
        job_id="job-dedup",
        worker_namespace="shard-00000",
        source_root=source,
        paths=["results"],
        generation="g2",
    )

    uploads = [
        key for operation, key in client.operations
        if operation == "upload"
    ]
    assert len(uploads) == 1
    assert uploads[0].startswith("jobs/job-dedup/checkpoint-blobs/")
    assert client.metadata[uploads[0]] == {
        "sha256": first["files"][0]["sha256"],
    }

    (source / "results" / "b.txt").write_text("changed")
    store.commit(
        job_id="job-dedup",
        worker_namespace="shard-00000",
        source_root=source,
        paths=["results"],
        generation="g3",
    )
    assert sum(
        operation == "upload" for operation, _key in client.operations
    ) == 2


def test_existing_blob_requires_matching_size_and_sha_metadata(tmp_path: Path):
    source = tmp_path / "source"
    (source / "results").mkdir(parents=True)
    (source / "results" / "a.txt").write_text("same")
    client = FakeS3()
    store = _store(client)
    first = store.commit(
        job_id="job-blob",
        worker_namespace="shard-00000",
        source_root=source,
        paths=["results"],
        generation="g1",
    )
    blob = first["files"][0]["object_key"]
    client.metadata[blob] = {}

    with pytest.raises(CheckpointError, match="blob identity mismatch"):
        store.commit(
            job_id="job-blob",
            worker_namespace="shard-00000",
            source_root=source,
            paths=["results"],
            generation="g2",
        )

    assert not any(
        key.endswith("/g2/COMMITTED.json") for key in client.objects
    )


def test_upload_reads_private_snapshot_not_mutated_live_file(tmp_path: Path):
    source = tmp_path / "source"
    (source / "results").mkdir(parents=True)
    live_file = source / "results" / "answer.txt"
    live_file.write_text("before")
    client = FakeS3()
    client.upload_hook = lambda: live_file.write_text("after")
    store = _store(client)

    manifest = store.commit(
        job_id="job-snapshot",
        worker_namespace="shard-00000",
        source_root=source,
        paths=["results"],
        generation="g1",
    )

    entry = manifest["files"][0]
    assert client.objects[entry["object_key"]] == b"before"
    assert entry["sha256"] == hashlib.sha256(b"before").hexdigest()
    assert live_file.read_text() == "after"


def test_same_generation_retry_is_idempotent_but_drift_fails(tmp_path: Path):
    source = tmp_path / "source"
    (source / "results").mkdir(parents=True)
    answer = source / "results" / "answer.txt"
    answer.write_text("stable")
    client = FakeS3()
    store = _store(client)
    first = store.commit(
        job_id="job-retry",
        worker_namespace="shard-00000",
        source_root=source,
        paths=["results"],
        generation="periodic-000001",
        metadata={"epoch": 1},
    )
    before_operations = list(client.operations)

    retried = store.commit(
        job_id="job-retry",
        worker_namespace="shard-00000",
        source_root=source,
        paths=["results"],
        generation="periodic-000001",
        metadata={"epoch": 1},
    )

    assert retried == first
    assert client.operations == before_operations
    with pytest.raises(CheckpointError, match="different content"):
        store.commit(
            job_id="job-retry",
            worker_namespace="shard-00000",
            source_root=source,
            paths=["results"],
            generation="periodic-000001",
            metadata={"epoch": 2},
        )


def test_empty_directories_and_executable_modes_round_trip(tmp_path: Path):
    source = tmp_path / "source"
    empty = source / "results" / "nested" / "empty"
    empty.mkdir(parents=True)
    executable = source / "results" / "run.sh"
    executable.write_text("#!/bin/sh\nexit 0\n")
    os.chmod(source / "results", 0o750)
    os.chmod(empty, 0o711)
    os.chmod(executable, 0o751)
    client = FakeS3()
    store = _store(client)

    manifest = store.commit(
        job_id="job-modes",
        worker_namespace="shard-00000",
        source_root=source,
        paths=["results"],
        generation="g1",
    )
    restored = tmp_path / "restored"
    store.restore_checkpoint(
        source_job_id="job-modes",
        worker_namespace="shard-00000",
        destination=restored,
        paths=["results"],
        generation="g1",
    )

    assert any(
        entry["path"] == "results/nested/empty"
        for entry in manifest["directories"]
    )
    assert (restored / "results" / "nested" / "empty").is_dir()
    assert stat.S_IMODE(
        (restored / "results").stat().st_mode
    ) == 0o750
    assert stat.S_IMODE(
        (restored / "results" / "nested" / "empty").stat().st_mode
    ) == 0o711
    assert stat.S_IMODE(
        (restored / "results" / "run.sh").stat().st_mode
    ) == 0o751


def test_checkpoint_set_resolves_shards_sizes_hashes_and_restores(
    tmp_path: Path,
):
    source = tmp_path / "source"
    (source / "results").mkdir(parents=True)
    (source / "results" / "answer.txt").write_text("payload")
    client = FakeS3()
    store = _store(client)
    generations: dict[str, str] = {}
    for index in range(2):
        namespace = f"shard-{index:05d}"
        generation = f"periodic-{index}"
        store.commit(
            job_id="job-set",
            worker_namespace=namespace,
            source_root=source,
            paths=["results"],
            generation=generation,
            metadata={"shard_index": index},
        )
        generations[namespace] = generation

    checkpoint_set = store.publish_checkpoint_set(
        job_id="job-set",
        shard_generations=generations,
        generation="set-000001",
        metadata={"epoch": 1},
    )

    assert checkpoint_set["total_bytes"] == len(b"payload") * 2
    assert [shard["worker_namespace"] for shard in checkpoint_set["shards"]] == [
        "shard-00000",
        "shard-00001",
    ]
    for shard in checkpoint_set["shards"]:
        manifest_key = (
            "jobs/job-set/workers/"
            f"{shard['worker_namespace']}/checkpoints/"
            f"{shard['generation']}/COMMITTED.json"
        )
        assert shard["manifest_sha256"] == hashlib.sha256(
            client.objects[manifest_key]
        ).hexdigest()
        assert shard["total_bytes"] == len(b"payload")
    assert store.resolve_checkpoint_set(
        source_job_id="job-set",
        generation="set-000001",
    ) == checkpoint_set
    set_key = (
        "jobs/job-set/checkpoint-sets/set-000001/COMMITTED.json"
    )
    assert next(
        item for item in client.put_kwargs if item["Key"] == set_key
    )["IfNoneMatch"] == "*"

    retried = store.publish_checkpoint_set(
        job_id="job-set",
        shard_generations=generations,
        generation="set-000001",
        metadata={"epoch": 1},
    )
    assert retried == checkpoint_set
    restored = tmp_path / "restored"
    store.restore_checkpoint(
        source_job_id="job-set",
        worker_namespace="shard-00001",
        destination=restored,
        paths=["results"],
        checkpoint_set_generation="set-000001",
        expected_metadata={"shard_index": 1},
    )
    assert (restored / "results" / "answer.txt").read_text() == "payload"


def test_latest_checkpoint_set_and_forced_manifest_hash(tmp_path: Path):
    source = tmp_path / "source"
    (source / "results").mkdir(parents=True)
    (source / "results" / "answer.txt").write_text("payload")
    client = FakeS3()
    store = _store(client)
    store.commit(
        job_id="job-hash",
        worker_namespace="shard-00000",
        source_root=source,
        paths=["results"],
        generation="g1",
    )
    first = store.publish_checkpoint_set(
        job_id="job-hash",
        shard_generations={"shard-00000": "g1"},
        generation="set-1",
    )
    second = store.publish_checkpoint_set(
        job_id="job-hash",
        shard_generations={"shard-00000": "g1"},
        generation="set-2",
    )

    assert store.resolve_checkpoint_set(
        source_job_id="job-hash",
    ) == second
    assert store.resolve_checkpoint_set(
        source_job_id="job-hash", generation="set-1",
    ) == first
    with pytest.raises(CheckpointError, match="checksum"):
        store.restore_checkpoint(
            source_job_id="job-hash",
            worker_namespace="shard-00000",
            destination=tmp_path / "bad-hash",
            paths=["results"],
            generation="g1",
            expected_manifest_sha256="0" * 64,
        )
    assert not (tmp_path / "bad-hash").exists()


def test_retention_gc_preserves_shared_and_unpublished_blobs(tmp_path: Path):
    source = tmp_path / "source"
    (source / "results").mkdir(parents=True)
    answer = source / "results" / "answer.txt"
    client = FakeS3()
    store = _store(client)
    shared_blob = ""

    for index, value in enumerate(("shared", "shared", "three", "four"), 1):
        answer.write_text(value)
        manifest = store.commit(
            job_id="job-gc",
            worker_namespace="shard-00000",
            source_root=source,
            paths=["results"],
            generation=f"g{index}",
        )
        if index == 1:
            shared_blob = manifest["files"][0]["object_key"]
        store.publish_checkpoint_set(
            job_id="job-gc",
            shard_generations={"shard-00000": f"g{index}"},
            generation=f"set-{index}",
        )

    assert (
        "jobs/job-gc/checkpoint-sets/set-1/COMMITTED.json"
        not in client.objects
    )
    assert (
        "jobs/job-gc/workers/shard-00000/checkpoints/g1/COMMITTED.json"
        not in client.objects
    )
    assert shared_blob in client.objects

    answer.write_text("orphan")
    orphan = store.commit(
        job_id="job-gc",
        worker_namespace="shard-00000",
        source_root=source,
        paths=["results"],
        generation="orphan",
    )
    orphan_blob = orphan["files"][0]["object_key"]
    answer.write_text("five")
    store.commit(
        job_id="job-gc",
        worker_namespace="shard-00000",
        source_root=source,
        paths=["results"],
        generation="g5",
    )
    store.publish_checkpoint_set(
        job_id="job-gc",
        shard_generations={"shard-00000": "g5"},
        generation="set-5",
    )

    assert shared_blob not in client.objects
    assert orphan_blob in client.objects
    assert (
        "jobs/job-gc/workers/shard-00000/checkpoints/orphan/COMMITTED.json"
        in client.objects
    )
    assert {
        key for key in client.objects
        if "/checkpoint-sets/" in key
    } == {
        "jobs/job-gc/checkpoint-sets/set-3/COMMITTED.json",
        "jobs/job-gc/checkpoint-sets/set-4/COMMITTED.json",
        "jobs/job-gc/checkpoint-sets/set-5/COMMITTED.json",
    }


def test_gc_bounds_long_running_incomplete_shard_generations(tmp_path: Path):
    source = tmp_path / "source"
    (source / "results").mkdir(parents=True)
    answer = source / "results" / "answer.txt"
    client = FakeS3()
    store = _store(client)
    answer.write_text("selected")
    store.commit(
        job_id="job-incomplete",
        worker_namespace="shard-00000",
        source_root=source,
        paths=["results"],
        generation="selected",
    )
    store.publish_checkpoint_set(
        job_id="job-incomplete",
        shard_generations={"shard-00000": "selected"},
        generation="set-1",
    )

    orphan_blobs: list[str] = []
    for index in range(5):
        answer.write_text(f"incomplete-{index}")
        manifest = store.commit(
            job_id="job-incomplete",
            worker_namespace="shard-00001",
            source_root=source,
            paths=["results"],
            generation=f"incomplete-{index}",
        )
        orphan_blobs.append(manifest["files"][0]["object_key"])
    store.publish_checkpoint_set(
        job_id="job-incomplete",
        shard_generations={"shard-00000": "selected"},
        generation="set-2",
    )

    orphan_manifests = {
        key for key in client.objects
        if (
            "/workers/shard-00001/checkpoints/" in key
            and key.endswith("/COMMITTED.json")
        )
    }
    assert orphan_manifests == {
        "jobs/job-incomplete/workers/shard-00001/"
        "checkpoints/incomplete-4/COMMITTED.json"
    }
    assert all(
        blob not in client.objects for blob in orphan_blobs[:-1]
    )
    assert orphan_blobs[-1] in client.objects


def test_commit_side_prune_bounds_job_that_never_forms_a_set(
    tmp_path: Path,
):
    source = tmp_path / "source"
    (source / "results").mkdir(parents=True)
    answer = source / "results" / "answer.txt"
    client = FakeS3()
    store = _store(client)

    for index in range(12):
        answer.write_text(f"generation-{index}")
        store.commit(
            job_id="job-never-complete",
            worker_namespace="shard-00000",
            source_root=source,
            paths=["results"],
            generation=f"periodic-{index:08d}",
        )
        store.prune_incomplete_generations(
            job_id="job-never-complete",
            keep_per_shard=3,
        )

    manifests = {
        key for key in client.objects
        if (
            "/job-never-complete/workers/shard-00000/"
            "checkpoints/" in key
            and key.endswith("/COMMITTED.json")
        )
    }
    assert manifests == {
        "jobs/job-never-complete/workers/shard-00000/"
        f"checkpoints/periodic-{index:08d}/COMMITTED.json"
        for index in (9, 10, 11)
    }
    blobs = {
        key for key in client.objects
        if "/job-never-complete/checkpoint-blobs/" in key
    }
    assert len(blobs) == 3


def test_corrupt_referenced_shard_blocks_set_publish_and_restore(tmp_path: Path):
    source = tmp_path / "source"
    (source / "results").mkdir(parents=True)
    (source / "results" / "answer.txt").write_text("payload")
    client = FakeS3()
    store = _store(client)
    store.commit(
        job_id="job-corrupt-set",
        worker_namespace="shard-00000",
        source_root=source,
        paths=["results"],
        generation="g1",
    )
    store.publish_checkpoint_set(
        job_id="job-corrupt-set",
        shard_generations={"shard-00000": "g1"},
        generation="set-1",
    )
    shard_key = (
        "jobs/job-corrupt-set/workers/shard-00000/"
        "checkpoints/g1/COMMITTED.json"
    )
    shard_manifest = json.loads(client.objects[shard_key])
    shard_manifest["metadata"]["tampered"] = True
    client.objects[shard_key] = json.dumps(
        shard_manifest, sort_keys=True, separators=(",", ":"),
    ).encode()

    with pytest.raises(CheckpointError, match="missing or changed"):
        store.publish_checkpoint_set(
            job_id="job-corrupt-set",
            shard_generations={"shard-00000": "g1"},
            generation="set-2",
        )
    assert (
        "jobs/job-corrupt-set/checkpoint-sets/set-2/COMMITTED.json"
        not in client.objects
    )
    with pytest.raises(
        CheckpointError,
        match="(?:checksum|missing or changed)",
    ):
        store.restore_checkpoint(
            source_job_id="job-corrupt-set",
            worker_namespace="shard-00000",
            destination=tmp_path / "restore",
            paths=["results"],
            checkpoint_set_generation="set-1",
        )
    assert not (tmp_path / "restore").exists()


def test_checkpoint_set_cannot_underreport_referenced_object_count(
    tmp_path: Path,
):
    source = tmp_path / "source"
    (source / "results").mkdir(parents=True)
    (source / "results" / "answer.txt").write_text("payload")
    client = FakeS3()
    store = _store(client)
    shard = store.commit(
        job_id="job-object-count",
        worker_namespace="shard-00000",
        source_root=source,
        paths=["results"],
        generation="g1",
    )
    assert shard["total_objects"] > 0
    store.publish_checkpoint_set(
        job_id="job-object-count",
        shard_generations={"shard-00000": "g1"},
        generation="set-1",
    )
    set_key = (
        "jobs/job-object-count/checkpoint-sets/set-1/COMMITTED.json"
    )
    checkpoint_set = json.loads(client.objects[set_key])
    checkpoint_set["shards"][0]["total_objects"] = 0
    checkpoint_set["total_objects"] = 0
    client.objects[set_key] = json.dumps(
        checkpoint_set, sort_keys=True, separators=(",", ":"),
    ).encode()

    with pytest.raises(CheckpointError, match="missing or changed"):
        store.resolve_checkpoint_set(
            source_job_id="job-object-count",
            generation="set-1",
        )


def test_corrupt_v2_directory_graph_is_rejected_before_download(tmp_path: Path):
    source = tmp_path / "source"
    (source / "results" / "nested").mkdir(parents=True)
    (source / "results" / "nested" / "answer.txt").write_text("payload")
    client = FakeS3()
    store = _store(client)
    store.commit(
        job_id="job-directories",
        worker_namespace="shard-00000",
        source_root=source,
        paths=["results"],
        generation="g1",
    )
    manifest_key = (
        "jobs/job-directories/workers/shard-00000/"
        "checkpoints/g1/COMMITTED.json"
    )
    manifest = json.loads(client.objects[manifest_key])
    manifest["directories"] = [
        entry for entry in manifest["directories"]
        if entry["path"] != "results/nested"
    ]
    client.objects[manifest_key] = json.dumps(manifest).encode()

    before_gets = len(client.operations)
    with pytest.raises(CheckpointError, match="parent directory"):
        store.restore_checkpoint(
            source_job_id="job-directories",
            worker_namespace="shard-00000",
            destination=tmp_path / "restore",
            paths=["results"],
            generation="g1",
        )
    assert len(client.operations) == before_gets
    assert not (tmp_path / "restore").exists()


@pytest.mark.parametrize(
    ("limits", "payload", "expected"),
    [
        ({"max_objects": 2}, b"x", "object limit"),
        ({"max_total_bytes": 3}, b"four", "byte limit"),
        ({"max_manifest_bytes": 64}, b"", "manifest limit"),
    ],
)
def test_checkpoint_commit_limits_fail_closed(
    tmp_path: Path,
    limits: dict[str, int],
    payload: bytes,
    expected: str,
):
    source = tmp_path / "source"
    (source / "results" / "nested").mkdir(parents=True)
    if payload:
        (source / "results" / "nested" / "answer.bin").write_bytes(payload)
    client = FakeS3()
    options = {
        "bucket": client.bucket,
        "prefix": "jobs",
        "client": client,
        "max_objects": 100,
        "max_total_bytes": 1024,
        "max_manifest_bytes": 1024 * 1024,
        **limits,
    }
    store = S3CheckpointStore(**options)

    with pytest.raises(CheckpointError, match=expected):
        store.commit(
            job_id="job-limits",
            worker_namespace="shard-00000",
            source_root=source,
            paths=["results"],
            generation="g1",
        )
    assert not any(
        key.endswith("/g1/COMMITTED.json") for key in client.objects
    )


def test_checkpoint_store_rejects_nonadjacent_overlapping_paths(
    tmp_path: Path,
):
    source = tmp_path / "source"
    (source / "a" / "c").mkdir(parents=True)
    (source / "a-b").mkdir()

    with pytest.raises(CheckpointError, match="must not overlap"):
        _store(FakeS3()).commit(
            job_id="job-overlap",
            worker_namespace="shard-00000",
            source_root=source,
            paths=["a", "a-b", "a/c"],
            generation="g1",
        )


def test_checkpoint_snapshot_uses_controlled_root_and_cleans_it(
    tmp_path: Path,
):
    source = tmp_path / "source"
    (source / "results").mkdir(parents=True)
    (source / "results" / "answer.bin").write_bytes(b"payload")
    snapshots = tmp_path / "state" / "checkpoint-snapshots"
    client = FakeS3()
    store = S3CheckpointStore(
        bucket=client.bucket,
        client=client,
        snapshot_root=snapshots,
        snapshot_free_reserve_bytes=0,
    )

    store.commit(
        job_id="job-snapshot-root",
        worker_namespace="shard-00000",
        source_root=source,
        paths=["results"],
        generation="g1",
    )

    assert snapshots.is_dir()
    assert list(snapshots.iterdir()) == []


def test_checkpoint_snapshot_enforces_single_file_limit(tmp_path: Path):
    source = tmp_path / "source"
    (source / "results").mkdir(parents=True)
    (source / "results" / "answer.bin").write_bytes(b"four")
    client = FakeS3()
    store = S3CheckpointStore(
        bucket=client.bucket,
        client=client,
        max_total_bytes=100,
        max_file_bytes=3,
        snapshot_free_reserve_bytes=0,
    )

    with pytest.raises(CheckpointError, match="single-file limit"):
        store.commit(
            job_id="job-single-limit",
            worker_namespace="shard-00000",
            source_root=source,
            paths=["results"],
            generation="g1",
        )


def test_checkpoint_snapshot_preserves_disk_reserve(
    tmp_path: Path, monkeypatch,
):
    source = tmp_path / "source"
    (source / "results").mkdir(parents=True)
    (source / "results" / "answer.bin").write_bytes(b"four")
    monkeypatch.setattr(
        "elastic_agent.core.checkpoint_store.shutil.disk_usage",
        lambda _path: type("Usage", (), {"free": 7})(),
    )
    client = FakeS3()
    store = S3CheckpointStore(
        bucket=client.bucket,
        client=client,
        max_total_bytes=100,
        snapshot_free_reserve_bytes=4,
    )

    with pytest.raises(CheckpointError, match="insufficient disk"):
        store.commit(
            job_id="job-disk-limit",
            worker_namespace="shard-00000",
            source_root=source,
            paths=["results"],
            generation="g1",
        )


def test_checkpoint_snapshot_never_copies_growth_beyond_open_size(
    tmp_path: Path, monkeypatch,
):
    source = tmp_path / "source"
    result = source / "results" / "answer.bin"
    result.parent.mkdir(parents=True)
    result.write_bytes(b"four")
    client = FakeS3()
    store = S3CheckpointStore(
        bucket=client.bucket,
        client=client,
        max_total_bytes=100,
        max_snapshot_bytes=4,
        snapshot_free_reserve_bytes=0,
    )
    opened = result.open("rb")
    opened_stat = os.fstat(opened.fileno())
    read_sizes: list[int] = []

    class GrowingReader:
        def read(self, size=-1):
            read_sizes.append(size)
            chunk = opened.read(size)
            if len(read_sizes) == 1:
                with result.open("ab") as writer:
                    writer.write(b"-growth")
            return chunk

        def fileno(self):
            return opened.fileno()

        def close(self):
            opened.close()

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            self.close()

    monkeypatch.setattr(
        store,
        "_open_validated",
        lambda _root, _path: (GrowingReader(), opened_stat),
    )

    with pytest.raises(CheckpointError, match="changed while snapshotting"):
        store.commit(
            job_id="job-growing-snapshot",
            worker_namespace="shard-00000",
            source_root=source,
            paths=["results"],
            generation="g1",
        )

    assert read_sizes == [4, 1]
    assert client.operations == []


def test_checkpoint_blob_upload_concurrency_is_bounded(tmp_path: Path):
    source = tmp_path / "source"
    (source / "results").mkdir(parents=True)
    (source / "results" / "answer.bin").write_bytes(b"payload")
    client = FakeS3()
    first_started = threading.Event()
    second_started = threading.Event()
    release = threading.Event()
    state_lock = threading.Lock()
    active = 0
    max_active = 0
    entered = 0

    def upload_hook():
        nonlocal active, max_active, entered
        with state_lock:
            active += 1
            entered += 1
            max_active = max(max_active, active)
            current = entered
        if current == 1:
            first_started.set()
            release.wait(timeout=2)
        else:
            second_started.set()
        with state_lock:
            active -= 1

    client.upload_hook = upload_hook
    store = S3CheckpointStore(
        bucket=client.bucket,
        client=client,
        max_concurrent_uploads=1,
        snapshot_free_reserve_bytes=0,
    )

    def commit(job_id):
        return store.commit(
            job_id=job_id,
            worker_namespace="shard-00000",
            source_root=source,
            paths=["results"],
            generation="g1",
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(commit, "job-upload-a")
        assert first_started.wait(timeout=1)
        second = pool.submit(commit, "job-upload-b")
        assert not second_started.wait(timeout=0.05)
        release.set()
        first.result(timeout=2)
        second.result(timeout=2)

    assert second_started.is_set()
    assert max_active == 1


def test_checkpoint_commit_honors_internal_deadline(
    tmp_path: Path, monkeypatch,
):
    source = tmp_path / "source"
    (source / "results").mkdir(parents=True)
    (source / "results" / "answer.bin").write_bytes(b"late")
    client = FakeS3()
    monkeypatch.setattr(
        "elastic_agent.core.checkpoint_store.time.monotonic",
        lambda: 10.0,
    )

    with pytest.raises(CheckpointError, match="deadline"):
        _store(client).commit(
            job_id="job-deadline",
            worker_namespace="shard-00000",
            source_root=source,
            paths=["results"],
            generation="g1",
            deadline_monotonic=9.0,
        )

    assert not any(
        key.endswith("COMMITTED.json") for key in client.objects
    )


def test_restore_honors_cooperative_cancellation_before_writing(
    tmp_path: Path,
):
    source = tmp_path / "source"
    (source / "results").mkdir(parents=True)
    (source / "results" / "answer.bin").write_bytes(b"payload")
    store = _store(FakeS3())
    store.commit(
        job_id="job-cancel-restore",
        worker_namespace="shard-00000",
        source_root=source,
        paths=["results"],
        generation="g1",
    )
    cancel_event = threading.Event()
    cancel_event.set()
    destination = tmp_path / "restored"

    with pytest.raises(CheckpointError, match="cancelled"):
        store.restore_checkpoint(
            source_job_id="job-cancel-restore",
            worker_namespace="shard-00000",
            destination=destination,
            paths=["results"],
            generation="g1",
            cancel_event=cancel_event,
        )

    assert not destination.exists()


@pytest.mark.parametrize(
    ("holder_mode", "waiter_mode"),
    [("write", "read"), ("read", "write")],
)
def test_checkpoint_job_lock_wait_honors_deadline(
    holder_mode: str,
    waiter_mode: str,
):
    store = _store(FakeS3())
    lock = store._job_lock(f"job-lock-deadline-{holder_mode}")
    entered = threading.Event()
    release = threading.Event()

    def hold_lock():
        with getattr(lock, holder_mode)():
            entered.set()
            assert release.wait(timeout=2)

    with ThreadPoolExecutor(max_workers=1) as pool:
        holder = pool.submit(hold_lock)
        assert entered.wait(timeout=1)
        started = time.monotonic()
        try:
            with pytest.raises(CheckpointError, match="deadline"):
                with getattr(lock, waiter_mode)(
                    deadline_monotonic=started + 0.05,
                ):
                    raise AssertionError("blocked lock was acquired")
        finally:
            release.set()
        holder.result(timeout=2)

    assert time.monotonic() - started < 0.5


def test_checkpoint_job_lock_wait_honors_late_cancellation():
    store = _store(FakeS3())
    lock = store._job_lock("job-lock-cancel")
    entered = threading.Event()
    release = threading.Event()
    cancel_event = threading.Event()

    def hold_writer():
        with lock.write():
            entered.set()
            assert release.wait(timeout=2)

    with ThreadPoolExecutor(max_workers=1) as pool:
        holder = pool.submit(hold_writer)
        assert entered.wait(timeout=1)
        timer = threading.Timer(0.05, cancel_event.set)
        timer.start()
        started = time.monotonic()
        try:
            with pytest.raises(CheckpointError, match="cancelled"):
                with lock.read(cancel_event=cancel_event):
                    raise AssertionError("blocked lock was acquired")
        finally:
            timer.cancel()
            release.set()
        holder.result(timeout=2)

    assert time.monotonic() - started < 0.5


@pytest.mark.parametrize("operation", ["limited-read", "restore-write"])
def test_rejected_s3_object_body_is_always_closed(
    tmp_path: Path, operation: str,
):
    class DeclaredSizeClient(FakeS3):
        def __init__(self):
            super().__init__()
            self.body = io.BytesIO(b"data")

        def get_object(self, *, Bucket, Key):  # noqa: N803
            assert Bucket == self.bucket
            return {"Body": self.body, "ContentLength": 99}

    client = DeclaredSizeClient()
    store = _store(client)

    with pytest.raises(CheckpointError):
        if operation == "limited-read":
            store._read_object_limited("oversized", limit=4)
        else:
            store._write_restored_object(
                key="wrong-size",
                destination=tmp_path / "result.bin",
                expected_size=4,
                expected_sha256=None,
            )

    assert client.body.closed


def test_checkpoint_set_listing_limit_is_bounded():
    client = FakeS3()
    for generation in ("set-1", "set-2"):
        key = (
            "jobs/job-list/checkpoint-sets/"
            f"{generation}/COMMITTED.json"
        )
        client.objects[key] = b"{}"
    store = S3CheckpointStore(
        bucket=client.bucket,
        prefix="jobs",
        client=client,
        max_checkpoint_sets=1,
    )

    with pytest.raises(CheckpointError, match="listing limit"):
        store.resolve_checkpoint_set(source_job_id="job-list")


def test_schema_v1_checkpoint_restore_remains_supported(tmp_path: Path):
    client = FakeS3()
    digest = hashlib.sha256(b"legacy").hexdigest()
    root = "jobs/job-v1/workers/shard-00000/checkpoints/g1"
    blob = f"{root}/blobs/{digest}"
    manifest_key = f"{root}/COMMITTED.json"
    client.objects[blob] = b"legacy"
    client.objects[manifest_key] = json.dumps({
        "schema_version": 1,
        "job_id": "job-v1",
        "worker_namespace": "shard-00000",
        "generation": "g1",
        "paths": ["results"],
        "files": [{
            "path": "results/answer.txt",
            "size": len(b"legacy"),
            "sha256": digest,
            "object_key": blob,
        }],
        "total_bytes": len(b"legacy"),
        "committed_at": "2026-07-29T00:00:00+00:00",
        "metadata": {},
    }, sort_keys=True, separators=(",", ":")).encode()
    restored = tmp_path / "restored"

    _store(client).restore_checkpoint(
        source_job_id="job-v1",
        worker_namespace="shard-00000",
        destination=restored,
        paths=["results"],
        generation="g1",
    )

    assert (restored / "results" / "answer.txt").read_bytes() == b"legacy"
    assert stat.S_IMODE(
        (restored / "results" / "answer.txt").stat().st_mode
    ) == 0o600
