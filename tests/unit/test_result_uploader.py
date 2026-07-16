"""Tests for S3ResultUploader (fake S3 client)."""

from __future__ import annotations

from elastic_agent.core.result_uploader import S3ResultUploader


class FakeS3:
    def __init__(self):
        self.uploads = []  # (path, bucket, key)

    def upload_file(self, path, bucket, key):
        self.uploads.append((path, bucket, key))


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
    import os, time
    os.utime(f, (time.time() + 5, time.time() + 5))
    assert up.sync_once() == 1


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
