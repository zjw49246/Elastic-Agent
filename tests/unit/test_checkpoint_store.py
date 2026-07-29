"""Immutable S3 checkpoint commit/restore tests."""

from __future__ import annotations

import io
import json
from pathlib import Path

import pytest

from elastic_agent.core.checkpoint_store import (
    CheckpointError,
    S3CheckpointStore,
)


class _Paginator:
    def __init__(self, client):
        self._client = client

    def paginate(self, *, Bucket, Prefix):
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
        self.operations: list[tuple[str, str]] = []

    def upload_fileobj(self, stream, bucket, key):
        assert bucket == self.bucket
        self.objects[key] = stream.read()
        self.operations.append(("upload", key))

    def put_object(self, *, Bucket, Key, Body, **_kwargs):
        assert Bucket == self.bucket
        self.objects[Key] = bytes(Body)
        self.operations.append(("put", Key))

    def get_object(self, *, Bucket, Key):
        assert Bucket == self.bucket
        if Key not in self.objects:
            raise KeyError(Key)
        return {
            "Body": io.BytesIO(self.objects[Key]),
            "ContentLength": len(self.objects[Key]),
        }

    def head_object(self, *, Bucket, Key):
        assert Bucket == self.bucket
        if Key not in self.objects:
            raise KeyError(Key)
        return {"ContentLength": len(self.objects[Key])}

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
    assert all("/blobs/" in entry["object_key"] for entry in manifest["files"])
    assert all(
        "/checkpoints/20260729T010203Z-test/blobs/" in entry["object_key"]
        for entry in manifest["files"]
    )

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
