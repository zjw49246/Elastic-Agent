"""Unit tests for FileSyncManager (T-030, T-031, T-032)."""

from __future__ import annotations

import io
import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from elastic_agent.worker.file_sync import (
    FileSyncManager,
    LocalBackend,
    OSSBackend,
    S3Backend,
    StorageObjectMetadataError,
    SyncedFile,
    SyncManifest,
    SyncMappingEntry,
    _file_md5,
    _guess_role,
    _is_critical,
    _match_debounce,
    _should_exclude,
    cloud_storage_credentials_step,
    create_storage_backend,
    load_storage_env,
)

# ---------------------------------------------------------------------------
# Helper fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def tmp_dir(tmp_path):
    return tmp_path


@pytest.fixture
def watch_dir(tmp_path):
    d = tmp_path / "watch"
    d.mkdir()
    return d


@pytest.fixture
def sync_dir(tmp_path):
    d = tmp_path / "sync_output"
    d.mkdir()
    return d


@pytest.fixture
def local_backend(sync_dir):
    return LocalBackend(str(sync_dir))


@pytest.fixture
def sync_manager(local_backend, watch_dir):
    return FileSyncManager(
        worker_id="test-worker-1",
        storage=local_backend,
        debounce_tiers={"state.json": 0.1, "manuscript_*": 0.2, "*": 0.3},
        exclude_patterns=["*.tmp", "*.swp", "__pycache__/"],
        buffer_path=None,
    )


# ---------------------------------------------------------------------------
# Unit tests: helper functions
# ---------------------------------------------------------------------------


class TestHelperFunctions:

    def test_file_md5(self, tmp_path):
        f = tmp_path / "test.txt"
        f.write_text("hello world")
        md5 = _file_md5(str(f))
        assert len(md5) == 32
        assert md5 == "5eb63bbbe01eeed093cb22bb8f5acdc3"

    def test_guess_role_state_json(self):
        assert _guess_role("state.json", {}) == "state"

    def test_guess_role_manifest(self):
        assert _guess_role("_sync_manifest.json", {}) == "metadata"
        assert _guess_role("manifest.json", {}) == "metadata"

    def test_guess_role_audio(self):
        assert _guess_role("chapter01.mp3", {}) == "audio"
        assert _guess_role("narration.wav", {}) == "audio"

    def test_guess_role_manuscript(self):
        assert _guess_role("manuscript_ch01.md", {}) == "manuscript"
        assert _guess_role("chapter.md", {}) == "manuscript"

    def test_guess_role_delivery_files(self):
        assert _guess_role("delivery/audiobook_manuscript.md", {}) == "delivery_manuscript"
        assert _guess_role("delivery/manuscript.md", {}) == "delivery_manuscript"
        assert _guess_role("delivery/custom_output.md", {}) == "manuscript"
        assert _guess_role("delivery/intro_final.md", {}) == "delivery_intro"
        assert _guess_role("delivery/custom.zip", {}) == "delivery_export"

    def test_guess_role_other(self):
        assert _guess_role("config.yaml", {}) == "other"

    def test_match_debounce_exact(self):
        tiers = {"state.json": 0.5, "manuscript_*": 2.0, "*": 5.0}
        assert _match_debounce("state.json", tiers) == 0.5

    def test_match_debounce_glob(self):
        tiers = {"state.json": 0.5, "manuscript_*": 2.0}
        assert _match_debounce("manuscript_ch01.md", tiers) == 2.0

    def test_match_debounce_default(self):
        tiers = {"state.json": 0.5}
        assert _match_debounce("config.yaml", tiers, default=5.0) == 5.0

    def test_is_critical(self):
        assert _is_critical("state.json") is True
        assert _is_critical("manifest.json") is True
        assert _is_critical("_sync_manifest.json") is True
        assert _is_critical("chapter.md") is False

    def test_should_exclude(self):
        patterns = ["*.tmp", "*.swp", "__pycache__/"]
        assert _should_exclude("test.tmp", patterns) is True
        assert _should_exclude("file.swp", patterns) is True
        assert _should_exclude("chapter.md", patterns) is False

    def test_should_exclude_path_pattern(self):
        patterns = ["*/node_modules/*"]
        assert _should_exclude("/root/work/node_modules/pkg/index.js", patterns) is True


# ---------------------------------------------------------------------------
# Unit tests: SyncManifest
# ---------------------------------------------------------------------------


class TestSyncManifest:

    def test_to_dict_and_from_dict(self):
        sf = SyncedFile(
            path="/root/work/ch01.md",
            oss_key="tasks/abc/ch01.md",
            size=1234,
            md5="abc123",
            content_type="text/markdown",
            role="manuscript",
            synced_at="2026-01-01T00:00:00Z",
        )
        manifest = SyncManifest(
            task_id="task-1",
            worker_id="worker-1",
            status="idle",
            updated_at="2026-01-01T00:00:00Z",
            files=[sf],
        )
        d = manifest.to_dict()
        assert d["task_id"] == "task-1"
        assert len(d["files"]) == 1
        assert d["files"][0]["path"] == "/root/work/ch01.md"

        restored = SyncManifest.from_dict(d)
        assert restored.task_id == "task-1"
        assert len(restored.files) == 1
        assert restored.files[0].md5 == "abc123"

    def test_empty_manifest(self):
        manifest = SyncManifest(task_id="t", worker_id="w")
        d = manifest.to_dict()
        assert d["files"] == []
        assert d["status"] == "idle"


# ---------------------------------------------------------------------------
# Unit tests: LocalBackend
# ---------------------------------------------------------------------------


class TestLocalBackend:

    async def test_upload_file(self, tmp_path, sync_dir):
        backend = LocalBackend(str(sync_dir))
        src = tmp_path / "source.txt"
        src.write_text("hello")

        await backend.upload_file(str(src), "prefix/source.txt")
        dest = sync_dir / "prefix" / "source.txt"
        assert dest.exists()
        assert dest.read_text() == "hello"

    async def test_upload_bytes(self, sync_dir):
        backend = LocalBackend(str(sync_dir))
        await backend.upload_bytes(b'{"test": true}', "data/test.json", "application/json")
        dest = sync_dir / "data" / "test.json"
        assert dest.exists()
        assert json.loads(dest.read_text()) == {"test": True}

    @pytest.mark.parametrize(
        "key",
        [
            "../outside.txt",
            "safe/../../outside.txt",
            "/tmp/outside.txt",
            "safe\\..\\outside.txt",
            "safe/\x00outside.txt",
        ],
    )
    async def test_all_operations_reject_unsafe_keys(
        self, tmp_path, sync_dir, key,
    ):
        backend = LocalBackend(str(sync_dir))
        source = tmp_path / "source.txt"
        source.write_text("payload")

        with pytest.raises(ValueError, match="unsafe storage key"):
            await backend.upload_file(str(source), key)
        with pytest.raises(ValueError, match="unsafe storage key"):
            await backend.upload_bytes(b"payload", key)
        with pytest.raises(ValueError, match="unsafe storage key"):
            await backend.read_file(key)
        with pytest.raises(ValueError, match="unsafe storage key"):
            await backend.file_exists(key)

    async def test_symlink_parent_cannot_escape_storage_root(
        self, tmp_path, sync_dir,
    ):
        backend = LocalBackend(str(sync_dir))
        outside = tmp_path / "outside"
        outside.mkdir()
        (sync_dir / "link").symlink_to(outside, target_is_directory=True)

        with pytest.raises(ValueError, match="escapes storage root"):
            await backend.upload_bytes(b"secret", "link/escaped.txt")
        with pytest.raises(ValueError, match="escapes storage root"):
            await backend.read_file("link/escaped.txt")
        assert not (outside / "escaped.txt").exists()

    async def test_open_reader_is_bounded_and_closeable(
        self, sync_dir,
    ):
        backend = LocalBackend(str(sync_dir))
        (sync_dir / "data.bin").write_bytes(b"abcdef")

        reader = await backend.open_reader("data.bin")
        assert reader.size == 6
        assert await reader.read(2) == b"ab"
        assert await reader.read(4) == b"cdef"
        assert await reader.read(1) == b""
        await reader.close()
        await reader.close()

    @pytest.mark.parametrize("key", ["line\u2028break", "zero\u200bwidth"])
    async def test_open_reader_rejects_nonprintable_unicode_key(
        self, sync_dir, key,
    ):
        backend = LocalBackend(str(sync_dir))
        with pytest.raises(ValueError, match="unsafe storage key"):
            await backend.open_reader(key)


class _SdkBody(io.BytesIO):
    def __init__(self, payload: bytes) -> None:
        super().__init__(payload)
        self.close_calls = 0
        self.read_sizes: list[int] = []

    def read(self, size=-1):
        self.read_sizes.append(size)
        return super().read(size)

    def close(self) -> None:
        self.close_calls += 1
        super().close()


class TestCloudObjectReaders:
    async def test_s3_reader_uses_get_length_and_closes_body(self):
        body = _SdkBody(b"abcdef")
        client = MagicMock()
        client.get_object.return_value = {
            "Body": body,
            "ContentLength": 6,
        }
        backend = S3Backend("bucket")
        backend._client = client

        reader = await backend.open_reader("safe/key")
        assert reader.size == 6
        assert await reader.read(3) == b"abc"
        await reader.close()
        await reader.close()

        assert body.close_calls == 1
        client.get_object.assert_called_once_with(
            Bucket="bucket", Key="safe/key",
        )

    async def test_s3_invalid_length_closes_body(self):
        body = _SdkBody(b"payload")
        client = MagicMock()
        client.get_object.return_value = {
            "Body": body,
            "ContentLength": "7",
        }
        backend = S3Backend("bucket")
        backend._client = client

        with pytest.raises(StorageObjectMetadataError):
            await backend.open_reader("safe/key")

        assert body.close_calls >= 1

    async def test_oss_reader_uses_content_length_and_closes_body(self):
        body = _SdkBody(b"payload")
        body.content_length = 7
        bucket = MagicMock()
        bucket.get_object.return_value = body
        backend = OSSBackend("bucket", "endpoint", "id", "secret")
        backend._bucket = bucket

        reader = await backend.open_reader("safe/key")
        assert reader.size == 7
        assert await reader.read(7) == b"payload"
        await reader.close()

        assert body.close_calls == 1
        bucket.get_object.assert_called_once_with("safe/key")

    async def test_oss_missing_length_closes_body(self):
        body = _SdkBody(b"payload")
        bucket = MagicMock()
        bucket.get_object.return_value = body
        backend = OSSBackend("bucket", "endpoint", "id", "secret")
        backend._bucket = bucket

        with pytest.raises(StorageObjectMetadataError):
            await backend.open_reader("safe/key")

        assert body.close_calls >= 1


# ---------------------------------------------------------------------------
# Unit tests: create_storage_backend
# ---------------------------------------------------------------------------


class TestCreateStorageBackend:

    def test_default_local(self):
        backend = create_storage_backend("local", base_dir="/tmp/test-sync")
        assert isinstance(backend, LocalBackend)

    def test_no_args_defaults_to_local(self):
        backend = create_storage_backend()
        assert isinstance(backend, LocalBackend)

    def test_oss_backend(self):
        from elastic_agent.worker.file_sync import OSSBackend
        backend = create_storage_backend(
            "oss",
            bucket_name="test-bucket",
            endpoint="oss-cn-hangzhou.aliyuncs.com",
            access_key_id="key",
            access_key_secret="secret",
        )
        assert isinstance(backend, OSSBackend)

    def test_s3_backend(self):
        from elastic_agent.worker.file_sync import S3Backend
        backend = create_storage_backend("s3", bucket_name="test-bucket", region="us-east-1")
        assert isinstance(backend, S3Backend)

    def test_reads_storage_type_from_env_oss(self, monkeypatch):
        from elastic_agent.worker.file_sync import OSSBackend
        monkeypatch.setenv("STORAGE_TYPE", "oss")
        monkeypatch.setenv("OSS_BUCKET", "my-bucket")
        monkeypatch.setenv("OSS_ENDPOINT", "oss-cn-hangzhou.aliyuncs.com")
        monkeypatch.setenv("OSS_ACCESS_KEY_ID", "ak")
        monkeypatch.setenv("OSS_ACCESS_KEY_SECRET", "sk")
        backend = create_storage_backend()
        assert isinstance(backend, OSSBackend)

    def test_reads_storage_type_from_env_s3(self, monkeypatch):
        from elastic_agent.worker.file_sync import S3Backend
        monkeypatch.setenv("STORAGE_TYPE", "s3")
        monkeypatch.setenv("S3_BUCKET", "my-bucket")
        backend = create_storage_backend()
        assert isinstance(backend, S3Backend)

    def test_explicit_type_overrides_env(self, monkeypatch):
        monkeypatch.setenv("STORAGE_TYPE", "s3")
        backend = create_storage_backend("local", base_dir="/tmp/test")
        assert isinstance(backend, LocalBackend)

    def test_env_fallback_to_local_when_unset(self, monkeypatch):
        monkeypatch.delenv("STORAGE_TYPE", raising=False)
        backend = create_storage_backend()
        assert isinstance(backend, LocalBackend)


# ---------------------------------------------------------------------------
# Unit tests: FileSyncManager
# ---------------------------------------------------------------------------


class TestFileSyncManager:

    async def test_register_mapping(self, sync_manager, watch_dir):
        mapping = SyncMappingEntry(
            task_id="task-1",
            book_slug="test-book",
            oss_prefix="tasks/task-1/",
            watch_paths=[str(watch_dir)],
        )
        sync_manager.register_mapping(mapping)
        assert "task-1" in sync_manager.active_mappings
        assert sync_manager.active_mappings["task-1"].book_slug == "test-book"

    async def test_unregister_mapping(self, sync_manager, watch_dir):
        await sync_manager.start()
        try:
            mapping = SyncMappingEntry(
                task_id="task-1",
                book_slug="test-book",
                oss_prefix="tasks/task-1/",
                watch_paths=[str(watch_dir)],
            )
            sync_manager.register_mapping(mapping)
            assert "task-1" in sync_manager.active_mappings

            await sync_manager.unregister_mapping("task-1")
            assert "task-1" not in sync_manager.active_mappings
        finally:
            await sync_manager.stop()

    async def test_force_sync(self, sync_manager, watch_dir, sync_dir):
        await sync_manager.start()
        try:
            (watch_dir / "chapter01.md").write_text("Hello world")
            (watch_dir / "state.json").write_text('{"phase": 1}')

            mapping = SyncMappingEntry(
                task_id="task-1",
                book_slug="test-book",
                oss_prefix="tasks/task-1",
                watch_paths=[str(watch_dir)],
            )
            sync_manager.register_mapping(mapping)

            count = await sync_manager.force_sync("task-1")
            assert count == 2

            assert (sync_dir / "tasks" / "task-1" / "chapter01.md").exists()
            assert (sync_dir / "tasks" / "task-1" / "state.json").exists()
            manifest_path = sync_dir / "tasks" / "task-1" / "_sync_manifest.json"
            assert manifest_path.exists()
            manifest = json.loads(manifest_path.read_text())
            assert manifest["task_id"] == "task-1"
            assert len(manifest["files"]) == 2
        finally:
            await sync_manager.stop()

    async def test_force_sync_excludes_tmp(self, sync_manager, watch_dir, sync_dir):
        await sync_manager.start()
        try:
            (watch_dir / "chapter.md").write_text("content")
            (watch_dir / "temp.tmp").write_text("temporary")

            mapping = SyncMappingEntry(
                task_id="task-1",
                book_slug="test",
                oss_prefix="tasks/task-1",
                watch_paths=[str(watch_dir)],
            )
            sync_manager.register_mapping(mapping)

            count = await sync_manager.force_sync("task-1")
            assert count == 1
            assert (sync_dir / "tasks" / "task-1" / "chapter.md").exists()
            assert not (sync_dir / "tasks" / "task-1" / "temp.tmp").exists()
        finally:
            await sync_manager.stop()

    async def test_force_sync_nonexistent_task(self, sync_manager):
        await sync_manager.start()
        try:
            count = await sync_manager.force_sync("nonexistent")
            assert count == 0
        finally:
            await sync_manager.stop()

    async def test_scan_task_artifacts_prefers_delivery_with_standard_manuscript(self, sync_manager, watch_dir):
        old_delivery = watch_dir / "old" / "delivery"
        old_delivery.mkdir(parents=True)
        (old_delivery / "notes.txt").write_text("not a manuscript")

        new_delivery = watch_dir / "workspace" / "delivery"
        new_delivery.mkdir(parents=True)
        manuscript = new_delivery / "audiobook_manuscript.md"
        manuscript.write_text("final manuscript")

        mapping = SyncMappingEntry(
            task_id="task-1",
            book_slug="test-book",
            oss_prefix="tasks/task-1",
            watch_paths=[str(watch_dir)],
        )

        scan = sync_manager.scan_task_artifacts("task-1", mapping=mapping)

        assert scan.delivery_found is True
        assert scan.delivery_path == str(new_delivery)
        assert scan.manuscript_path == str(manuscript)

    async def test_scan_task_artifacts_skips_unreadable_candidate_root(
        self, sync_manager, watch_dir, monkeypatch
    ):
        delivery_dir = watch_dir / "delivery"
        delivery_dir.mkdir()
        manuscript = delivery_dir / "audiobook_manuscript.md"
        manuscript.write_text("final manuscript")
        forbidden_root = Path("/root/books/test-book")
        original_is_dir = Path.is_dir

        def guarded_is_dir(path: Path) -> bool:
            if path == forbidden_root:
                raise PermissionError(path)
            return original_is_dir(path)

        monkeypatch.setattr(Path, "is_dir", guarded_is_dir)
        mapping = SyncMappingEntry(
            task_id="task-1",
            book_slug="test-book",
            oss_prefix="tasks/task-1",
            watch_paths=[str(watch_dir)],
        )

        scan = sync_manager.scan_task_artifacts("task-1", mapping=mapping)

        assert scan.delivery_path == str(delivery_dir)
        assert scan.manuscript_path == str(manuscript)

    async def test_force_sync_marks_standard_delivery_md_as_manuscript(self, sync_manager, watch_dir, sync_dir):
        await sync_manager.start()
        try:
            delivery_dir = watch_dir / "delivery"
            delivery_dir.mkdir()
            (delivery_dir / "audiobook_manuscript.md").write_text("final manuscript")

            mapping = SyncMappingEntry(
                task_id="task-1",
                book_slug="test-book",
                oss_prefix="tasks/task-1",
                watch_paths=[str(watch_dir)],
            )
            sync_manager.register_mapping(mapping)

            await sync_manager.force_sync("task-1")

            manifest_path = sync_dir / "tasks" / "task-1" / "_sync_manifest.json"
            manifest = json.loads(manifest_path.read_text())
            delivery_file = next(
                f for f in manifest["files"] if f["path"].endswith("audiobook_manuscript.md")
            )
            assert delivery_file["role"] == "delivery_manuscript"
        finally:
            await sync_manager.stop()

    async def test_on_file_event_excluded(self, sync_manager, watch_dir):
        await sync_manager.start()
        try:
            mapping = SyncMappingEntry(
                task_id="task-1", book_slug="test", oss_prefix="p/",
                watch_paths=[str(watch_dir)],
            )
            sync_manager.register_mapping(mapping)
            sync_manager.on_file_event(str(watch_dir / "test.tmp"), "created")
        finally:
            await sync_manager.stop()

    async def test_on_file_event_unmatched_path(self, sync_manager, watch_dir):
        await sync_manager.start()
        try:
            mapping = SyncMappingEntry(
                task_id="task-1", book_slug="test", oss_prefix="p/",
                watch_paths=[str(watch_dir)],
            )
            sync_manager.register_mapping(mapping)
            sync_manager.on_file_event("/some/other/path/file.md", "created")
        finally:
            await sync_manager.stop()

    async def test_callback_on_file_synced(self, local_backend, watch_dir):
        callback = AsyncMock()
        mgr = FileSyncManager(
            worker_id="w1",
            storage=local_backend,
            on_file_synced=callback,
        )
        await mgr.start()
        try:
            (watch_dir / "test.md").write_text("data")
            mapping = SyncMappingEntry(
                task_id="t1", book_slug="b", oss_prefix="tasks/t1",
                watch_paths=[str(watch_dir)],
            )
            mgr.register_mapping(mapping)
            await mgr.force_sync("t1")

            assert callback.called
            call_args = callback.call_args
            assert call_args[0][0] == "t1"
        finally:
            await mgr.stop()

    async def test_pending_buffer(self, watch_dir, tmp_path):
        failing_backend = AsyncMock(spec=LocalBackend)
        failing_backend.upload_file = AsyncMock(side_effect=Exception("upload failed"))
        failing_backend.upload_bytes = AsyncMock(side_effect=Exception("upload failed"))

        mgr = FileSyncManager(
            worker_id="w1",
            storage=failing_backend,
            buffer_path=str(tmp_path / "buffer.json"),
        )
        await mgr.start()
        try:
            (watch_dir / "test.md").write_text("data")
            mapping = SyncMappingEntry(
                task_id="t1", book_slug="b", oss_prefix="tasks/t1",
                watch_paths=[str(watch_dir)],
            )
            mgr.register_mapping(mapping)
            await mgr.force_sync("t1")

            assert mgr.pending_count > 0
        finally:
            await mgr.stop()

    async def test_relative_path_computation(self):
        result = FileSyncManager._compute_relative_path(
            "/root/work/book/chapter01.md",
            ["/root/work/book"],
        )
        assert result == "chapter01.md"

        result = FileSyncManager._compute_relative_path(
            "/root/work/book/subdir/file.txt",
            ["/root/work/book"],
        )
        assert result == "subdir/file.txt"

    async def test_relative_path_no_match(self):
        result = FileSyncManager._compute_relative_path(
            "/other/path/file.txt",
            ["/root/work/book"],
        )
        assert result is None

    async def test_manifest_update_replaces_existing(self, sync_manager, watch_dir, sync_dir):
        await sync_manager.start()
        try:
            (watch_dir / "file.md").write_text("v1")
            mapping = SyncMappingEntry(
                task_id="t1", book_slug="b", oss_prefix="tasks/t1",
                watch_paths=[str(watch_dir)],
            )
            sync_manager.register_mapping(mapping)

            await sync_manager.force_sync("t1")
            manifest_path = sync_dir / "tasks" / "t1" / "_sync_manifest.json"
            m1 = json.loads(manifest_path.read_text())
            assert len(m1["files"]) == 1

            (watch_dir / "file.md").write_text("v2")
            await sync_manager.force_sync("t1")
            m2 = json.loads(manifest_path.read_text())
            assert len(m2["files"]) == 1
            assert m2["files"][0]["md5"] != m1["files"][0]["md5"]
        finally:
            await sync_manager.stop()

    async def test_buffer_persistence(self, watch_dir, tmp_path):
        buffer_path = str(tmp_path / "buffer.json")
        failing_backend = AsyncMock(spec=LocalBackend)
        failing_backend.upload_file = AsyncMock(side_effect=Exception("fail"))
        failing_backend.upload_bytes = AsyncMock(side_effect=Exception("fail"))

        mgr = FileSyncManager(
            worker_id="w1",
            storage=failing_backend,
            buffer_path=buffer_path,
        )
        await mgr.start()
        try:
            (watch_dir / "test.md").write_text("data")
            mapping = SyncMappingEntry(
                task_id="t1", book_slug="b", oss_prefix="tasks/t1",
                watch_paths=[str(watch_dir)],
            )
            mgr.register_mapping(mapping)
            await mgr.force_sync("t1")
        finally:
            await mgr.stop()

        assert Path(buffer_path).exists()
        data = json.loads(Path(buffer_path).read_text())
        assert len(data) > 0

        mgr2 = FileSyncManager(
            worker_id="w1",
            storage=failing_backend,
            buffer_path=buffer_path,
        )
        await mgr2.start()
        try:
            assert mgr2.pending_count > 0
        finally:
            await mgr2.stop()


# ---------------------------------------------------------------------------
# Unit tests: cloud_storage_credentials_step (T-032)
# ---------------------------------------------------------------------------


class TestCloudStorageCredentialsStep:

    def test_oss_step(self):
        step = cloud_storage_credentials_step(
            storage_type="oss",
            oss_access_key_id="LTAI5t_test",
            oss_access_key_secret="secret123",
            oss_bucket="my-bucket",
            oss_endpoint="oss-cn-hangzhou.aliyuncs.com",
        )
        assert step.name == "cloud-storage-credentials"
        assert "OSS_ACCESS_KEY_ID" in step.command
        assert "LTAI5t_test" in step.command
        assert "STORAGE_TYPE=oss" in step.command

    def test_s3_step(self):
        step = cloud_storage_credentials_step(
            storage_type="s3",
            s3_bucket="my-s3-bucket",
            s3_region="us-east-1",
        )
        assert step.name == "cloud-storage-credentials"
        assert "S3_BUCKET" in step.command
        assert "STORAGE_TYPE=s3" in step.command

    def test_step_timeout(self):
        step = cloud_storage_credentials_step(timeout=120)
        assert step.timeout == 120


class TestLoadStorageEnv:

    def test_load_existing(self, tmp_path):
        env_file = tmp_path / "storage.env"
        env_file.write_text("OSS_BUCKET=my-bucket\nOSS_ENDPOINT=oss-cn-hangzhou.aliyuncs.com\n")
        result = load_storage_env(str(env_file))
        assert result["OSS_BUCKET"] == "my-bucket"
        assert result["OSS_ENDPOINT"] == "oss-cn-hangzhou.aliyuncs.com"

    def test_load_nonexistent(self):
        result = load_storage_env("/nonexistent/path.env")
        assert result == {}

    def test_skip_comments_and_empty(self, tmp_path):
        env_file = tmp_path / "storage.env"
        env_file.write_text("# comment\n\nKEY=value\n")
        result = load_storage_env(str(env_file))
        assert result == {"KEY": "value"}
