"""Tests for S3ResultUploader (fake S3 client)."""

from __future__ import annotations

import asyncio
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from elastic_agent.core.result_uploader import S3ResultUploader, S3ResultUploadError


class FakeS3:
    def __init__(self):
        self.uploads = []  # (content, bucket, key)

    def upload_fileobj(self, stream, bucket, key):
        self.uploads.append((stream.read(), bucket, key))


class FailingS3(FakeS3):
    def upload_fileobj(self, stream, bucket, key):
        raise RuntimeError("AccessDenied")


class ClosingS3(FakeS3):
    def upload_fileobj(self, stream, bucket, key):
        super().upload_fileobj(stream, bucket, key)
        stream.close()


def _uploader(tmp_path, **kw):
    root = tmp_path / "collected"
    root.mkdir()
    return S3ResultUploader(str(kw.pop("bucket", "b")), str(root), client=FakeS3(), **kw), root


def test_uploads_new_files_with_prefixed_keys(tmp_path):
    up, root = _uploader(tmp_path)
    (root / "job-1").mkdir()
    (root / "job-1" / "res.json").write_text("{}")
    (root / "job-1" / "meta.txt").write_text("x")
    n = up.sync_once()
    assert n == 2
    keys = sorted(k for _, _, k in up._client.uploads)
    assert keys == ["jobs/job-1/meta.txt", "jobs/job-1/res.json"]


def test_skips_unchanged_files(tmp_path):
    up, root = _uploader(tmp_path)
    (root / "job-1").mkdir()
    f = root / "job-1" / "res.json"
    f.write_text("{}")
    assert up.sync_once() == 1
    assert up.sync_once() == 0            # unchanged → skipped
    f.write_text('{"x":1}')               # mtime changes → re-uploaded
    import time
    os.utime(f, (time.time() + 5, time.time() + 5))
    assert up.sync_once() == 1


def test_same_size_and_mtime_content_change_is_reuploaded(tmp_path):
    up, root = _uploader(tmp_path)
    (root / "job-1").mkdir()
    result = root / "job-1" / "result.bin"
    result.write_bytes(b"AAAA")

    assert up.sync_job("job-1") == 1
    original = result.stat()
    result.write_bytes(b"BBBB")
    os.utime(
        result,
        ns=(original.st_atime_ns, original.st_mtime_ns),
    )

    assert up.sync_job("job-1") == 1
    assert [content for content, _, _ in up._client.uploads] == [
        b"AAAA",
        b"BBBB",
    ]


def test_upload_client_may_close_its_file_object(tmp_path):
    root = tmp_path / "collected"
    result = root / "job-1" / "result.bin"
    result.parent.mkdir(parents=True)
    result.write_bytes(b"payload")
    client = ClosingS3()
    uploader = S3ResultUploader("bucket", str(root), client=client)

    assert uploader.sync_job("job-1") == 1
    assert client.uploads == [(b"payload", "bucket", "jobs/job-1/result.bin")]


def test_empty_root_noop(tmp_path):
    up, _ = _uploader(tmp_path)
    assert up.sync_once() == 0


def test_custom_prefix_and_uri(tmp_path):
    up, root = _uploader(tmp_path, prefix="runs/")
    (root / "j").mkdir()
    (root / "j" / "a").write_text("a")
    up.sync_once()
    assert up._client.uploads[0][2] == "runs/j/a"
    assert up.s3_uri("j") == "s3://b/runs/j/"


def test_periodic_sync_skips_private_collection_attempts(tmp_path):
    up, root = _uploader(tmp_path)
    published = root / "job-1" / "workers" / "shard-00000"
    published.mkdir(parents=True)
    (published / "answer.txt").write_text("complete")
    (published / ".application-state").write_text("keep")
    attempt = (
        root / "job-1" / "workers"
        / ".shard-00000.attempt-interrupted"
    )
    attempt.mkdir()
    (attempt / "partial.txt").write_text("must stay private")

    assert up.sync_once() == 2
    assert sorted(key for _, _, key in up._client.uploads) == [
        "jobs/job-1/workers/shard-00000/.application-state",
        "jobs/job-1/workers/shard-00000/answer.txt",
    ]


def test_periodic_sync_applies_limits_per_job_not_all_history(tmp_path):
    root = tmp_path / "collected"
    for job_id in ("job-1", "job-2"):
        result = root / job_id / "answer.bin"
        result.parent.mkdir(parents=True)
        result.write_bytes(b"1234")
    uploader = S3ResultUploader(
        "bucket",
        str(root),
        client=FakeS3(),
        max_objects=10,
        max_total_bytes=4,
    )

    assert uploader.sync_once() == 2
    assert sorted(key for _, _, key in uploader._client.uploads) == [
        "jobs/job-1/answer.bin",
        "jobs/job-2/answer.bin",
    ]


def test_sync_job_only_uploads_requested_job(tmp_path):
    up, root = _uploader(tmp_path)
    for job in ("j1", "j2"):
        (root / job).mkdir()
        (root / job / "a").write_text(job)

    assert up.sync_job("j1") == 1
    assert [key for _, _, key in up._client.uploads] == ["jobs/j1/a"]


def test_sync_worker_only_uploads_requested_namespace(tmp_path):
    up, root = _uploader(tmp_path)
    for namespace in ("shard-00000", "shard-00001"):
        result = root / "j1" / "workers" / namespace / "results"
        result.mkdir(parents=True)
        (result / "answer.txt").write_text(namespace)

    assert up.sync_worker("j1", "shard-00001") == 1
    assert [key for _, _, key in up._client.uploads] == [
        "jobs/j1/workers/shard-00001/results/answer.txt"
    ]


def test_disjoint_worker_namespaces_upload_concurrently(tmp_path):
    barrier = threading.Barrier(2, timeout=2)

    class ConcurrentS3(FakeS3):
        def upload_fileobj(self, stream, bucket, key, **_kwargs):
            barrier.wait()
            super().upload_fileobj(stream, bucket, key)

    root = tmp_path / "collected"
    for namespace in ("shard-00000", "shard-00001"):
        result = root / "j1" / "workers" / namespace / "results"
        result.mkdir(parents=True)
        (result / "answer.txt").write_text(namespace)
    uploader = S3ResultUploader(
        "bucket",
        str(root),
        client=ConcurrentS3(),
        max_concurrent_uploads=2,
    )

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(
            lambda namespace: uploader.sync_worker("j1", namespace),
            ("shard-00000", "shard-00001"),
        ))

    assert results == [1, 1]
    assert len(uploader._client.uploads) == 2


@pytest.mark.parametrize(
    ("job_id", "namespace"),
    [
        ("", "shard-00000"),
        ("j1", ""),
        ("../j1", "shard-00000"),
        ("j1", "../shard-00000"),
        ("j1/sub", "shard-00000"),
        ("j1", "workers/shard-00000"),
    ],
)
def test_sync_worker_rejects_unsafe_components(
    tmp_path, job_id, namespace,
):
    up, _ = _uploader(tmp_path)
    with pytest.raises(ValueError, match="safe path component"):
        up.sync_worker(job_id, namespace)


def test_sync_worker_honors_internal_deadline(tmp_path, monkeypatch):
    up, root = _uploader(tmp_path)
    result = root / "j1" / "workers" / "shard-00000" / "results"
    result.mkdir(parents=True)
    (result / "answer.txt").write_text("late")
    monkeypatch.setattr(
        "elastic_agent.core.result_uploader.time.monotonic",
        lambda: 10.0,
    )

    with pytest.raises(TimeoutError, match="deadline"):
        up.sync_worker(
            "j1", "shard-00000", deadline_monotonic=9.0,
        )

    assert up._client.uploads == []


def test_sync_tree_enforces_global_object_and_byte_limits(tmp_path):
    root = tmp_path / "collected"
    result = root / "j1" / "workers" / "shard-00000" / "results"
    result.mkdir(parents=True)
    (result / "a.bin").write_bytes(b"1234")
    (result / "b.bin").write_bytes(b"5")

    object_limited = S3ResultUploader(
        "bucket",
        str(root),
        client=FakeS3(),
        max_objects=2,
        max_total_bytes=100,
    )
    with pytest.raises(S3ResultUploadError, match="object limit"):
        object_limited.sync_worker("j1", "shard-00000")

    byte_limited = S3ResultUploader(
        "bucket",
        str(root),
        client=FakeS3(),
        max_objects=10,
        max_total_bytes=4,
    )
    with pytest.raises(S3ResultUploadError, match="byte limit"):
        byte_limited.sync_worker("j1", "shard-00000")


def test_cancel_event_without_deadline_interrupts_active_upload(tmp_path):
    cancel_event = threading.Event()

    class CancellingS3(FakeS3):
        def __init__(self):
            super().__init__()
            self.callback_present = False

        def upload_fileobj(self, stream, bucket, key, **kwargs):
            callback = kwargs.get("Callback")
            self.callback_present = callback is not None
            cancel_event.set()
            if callback is not None:
                callback(1)
            super().upload_fileobj(stream, bucket, key)

    root = tmp_path / "collected"
    result = root / "j1" / "workers" / "shard-00000" / "results"
    result.mkdir(parents=True)
    (result / "answer.txt").write_text("payload")
    client = CancellingS3()
    uploader = S3ResultUploader("bucket", str(root), client=client)

    with pytest.raises(TimeoutError, match="cancelled"):
        uploader.sync_worker(
            "j1",
            "shard-00000",
            cancel_event=cancel_event,
        )

    assert client.callback_present
    assert client.uploads == []


def test_scan_and_hash_concurrency_are_bounded(tmp_path, monkeypatch):
    root = tmp_path / "collected"
    for namespace in ("shard-00000", "shard-00001"):
        result = root / "j1" / "workers" / namespace / "results"
        result.mkdir(parents=True)
        (result / "answer.txt").write_text(namespace)
    uploader = S3ResultUploader(
        "bucket",
        str(root),
        client=FakeS3(),
        max_concurrent_scans=1,
        max_concurrent_hashes=1,
        max_concurrent_uploads=2,
    )
    scan_lock = threading.Lock()
    hash_lock = threading.Lock()
    scan_active = 0
    hash_active = 0
    max_scan_active = 0
    max_hash_active = 0
    original_safe_files = uploader._safe_files
    original_hash = uploader._content_sha256

    def tracked_safe_files(*args, **kwargs):
        nonlocal scan_active, max_scan_active
        for item in original_safe_files(*args, **kwargs):
            with scan_lock:
                scan_active += 1
                max_scan_active = max(max_scan_active, scan_active)
            threading.Event().wait(0.02)
            with scan_lock:
                scan_active -= 1
            yield item

    def tracked_hash(stream, deadline_monotonic=None, cancel_event=None):
        nonlocal hash_active, max_hash_active
        with hash_lock:
            hash_active += 1
            max_hash_active = max(max_hash_active, hash_active)
        threading.Event().wait(0.02)
        try:
            return original_hash(
                stream, deadline_monotonic, cancel_event,
            )
        finally:
            with hash_lock:
                hash_active -= 1

    monkeypatch.setattr(uploader, "_safe_files", tracked_safe_files)
    monkeypatch.setattr(uploader, "_content_sha256", tracked_hash)
    start = threading.Barrier(2, timeout=2)

    def sync(namespace):
        start.wait()
        return uploader.sync_worker("j1", namespace)

    with ThreadPoolExecutor(max_workers=2) as pool:
        assert list(pool.map(
            sync,
            ("shard-00000", "shard-00001"),
        )) == [1, 1]

    assert max_scan_active == 1
    assert max_hash_active == 1


def test_uploaded_digest_cache_is_bounded_across_worker_syncs(tmp_path):
    root = tmp_path / "collected"
    uploader = S3ResultUploader(
        "bucket",
        str(root),
        client=FakeS3(),
        max_objects=2,
    )
    for index in range(3):
        namespace = f"shard-{index:05d}"
        result = root / "j1" / "workers" / namespace / "results"
        result.mkdir(parents=True)
        (result / "answer.txt").write_text(namespace)
        assert uploader.sync_worker("j1", namespace) == 1

    assert len(uploader._uploaded) == 2


def test_upload_failure_is_not_silently_treated_as_success(tmp_path):
    root = tmp_path / "collected"
    (root / "j").mkdir(parents=True)
    (root / "j" / "result.json").write_text("{}")
    up = S3ResultUploader("bucket", str(root), client=FailingS3())

    with pytest.raises(S3ResultUploadError, match="AccessDenied"):
        up.sync_job("j")
    assert up._uploaded == {}


@pytest.mark.parametrize("job_id", ["", ".", "..", "../other", "a/b"])
def test_sync_job_rejects_unsafe_job_id(tmp_path, job_id):
    up, _ = _uploader(tmp_path)
    with pytest.raises(ValueError, match="safe path component"):
        up.sync_job(job_id)


def test_empty_bucket_is_rejected(tmp_path):
    with pytest.raises(ValueError, match="bucket must not be empty"):
        S3ResultUploader("  ", str(tmp_path))


def test_skips_symlinks_and_special_files_without_following_them(tmp_path):
    root = tmp_path / "collected"
    job = root / "j"
    real_dir = job / "real"
    real_dir.mkdir(parents=True)
    (real_dir / "ok.json").write_text("{}")

    outside = tmp_path / "manager-secret"
    outside.write_text("do not upload")
    (job / "outside-link").symlink_to(outside)
    (job / "directory-link").symlink_to(real_dir, target_is_directory=True)
    if hasattr(os, "mkfifo"):
        os.mkfifo(job / "named-pipe")

    up = S3ResultUploader("bucket", str(root), client=FakeS3())
    assert up.sync_job("j") == 1
    assert [key for _, _, key in up._client.uploads] == [
        "jobs/j/real/ok.json"
    ]
    assert all(content != b"do not upload" for content, _, _ in up._client.uploads)


def test_skips_job_tree_whose_root_is_a_symlink(tmp_path):
    root = tmp_path / "collected"
    root.mkdir()
    outside = tmp_path / "outside-job"
    outside.mkdir()
    (outside / "secret.txt").write_text("secret")
    (root / "j").symlink_to(outside, target_is_directory=True)

    up = S3ResultUploader("bucket", str(root), client=FakeS3())
    assert up.sync_job("j") == 0
    assert up._client.uploads == []


@pytest.mark.asyncio
async def test_periodic_default_deadline_covers_large_production_tree(
    tmp_path, monkeypatch,
):
    root = tmp_path / "collected"
    root.mkdir()
    uploader = S3ResultUploader("bucket", str(root), client=FakeS3())
    observed = threading.Event()
    remaining: list[float] = []

    def sync_once(*, deadline_monotonic, cancel_event):
        assert cancel_event is not None
        remaining.append(deadline_monotonic - time.monotonic())
        observed.set()
        return 0

    monkeypatch.setattr(uploader, "sync_once", sync_once)
    periodic = asyncio.create_task(uploader.run_periodic(interval=3600))
    assert await asyncio.to_thread(observed.wait, 2)
    periodic.cancel()
    with pytest.raises(asyncio.CancelledError):
        await periodic

    assert remaining[0] >= 1_799


@pytest.mark.asyncio
async def test_periodic_cancel_wins_over_simultaneous_upload_failure(
    tmp_path,
):
    started = threading.Event()
    release = threading.Event()

    class BlockingFailureS3(FakeS3):
        def upload_fileobj(self, stream, bucket, key, **_kwargs):
            started.set()
            release.wait(timeout=2)
            raise RuntimeError("late upload failure")

    root = tmp_path / "collected"
    result = root / "j1" / "result.txt"
    result.parent.mkdir(parents=True)
    result.write_text("payload")
    uploader = S3ResultUploader(
        "bucket", str(root), client=BlockingFailureS3(),
    )

    periodic = asyncio.create_task(
        uploader.run_periodic(interval=3600, operation_timeout=60)
    )
    assert await asyncio.to_thread(started.wait, 2)
    periodic.cancel()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(periodic, timeout=2)
