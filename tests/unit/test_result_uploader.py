"""Tests for S3ResultUploader (fake S3 client)."""

from __future__ import annotations

import os

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


def test_sync_job_only_uploads_requested_job(tmp_path):
    up, root = _uploader(tmp_path)
    for job in ("j1", "j2"):
        (root / job).mkdir()
        (root / job / "a").write_text(job)

    assert up.sync_job("j1") == 1
    assert [key for _, _, key in up._client.uploads] == ["jobs/j1/a"]


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
