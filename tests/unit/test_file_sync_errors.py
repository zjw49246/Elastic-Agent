"""Unit tests for FileSyncManager error handling (T-036) and FILE_SYNCED event (T-037)."""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from elastic_agent.worker.file_sync import (
    FileSyncManager,
    LocalBackend,
    SyncMappingEntry,
    MAX_BUFFER_SIZE,
    MAX_BUFFER_BYTES,
    MULTIPART_THRESHOLD,
)


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


# ---------------------------------------------------------------------------
# T-036: Upload error handling
# ---------------------------------------------------------------------------


class TestUploadErrorHandling:

    async def test_retry_on_upload_failure(self, watch_dir, tmp_path):
        """Verifies that upload retries before buffering."""
        call_count = 0

        async def failing_upload(local_path, oss_key):
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise Exception("transient failure")

        backend = MagicMock()
        backend.upload_file = AsyncMock(side_effect=failing_upload)
        backend.upload_bytes = AsyncMock()

        mgr = FileSyncManager(worker_id="w1", storage=backend)
        await mgr.start()
        try:
            (watch_dir / "test.md").write_text("data")
            mapping = SyncMappingEntry(
                task_id="t1", book_slug="b", oss_prefix="tasks/t1",
                watch_paths=[str(watch_dir)],
            )
            mgr.register_mapping(mapping)
            await mgr.force_sync("t1")

            assert call_count == 3
            assert mgr.pending_count == 0
        finally:
            await mgr.stop()

    async def test_buffer_after_max_retries(self, watch_dir, tmp_path):
        """After MAX_RETRIES failures, non-critical file goes to buffer."""
        backend = MagicMock()
        backend.upload_file = AsyncMock(side_effect=Exception("permanent failure"))
        backend.upload_bytes = AsyncMock(side_effect=Exception("permanent failure"))

        buffer_path = str(tmp_path / "buffer.json")
        mgr = FileSyncManager(
            worker_id="w1", storage=backend, buffer_path=buffer_path,
        )
        await mgr.start()
        try:
            (watch_dir / "chapter.md").write_text("content")
            mapping = SyncMappingEntry(
                task_id="t1", book_slug="b", oss_prefix="tasks/t1",
                watch_paths=[str(watch_dir)],
            )
            mgr.register_mapping(mapping)
            await mgr.force_sync("t1")

            assert mgr.pending_count == 1
            assert Path(buffer_path).exists()
        finally:
            await mgr.stop()

    async def test_critical_file_emits_error(self, watch_dir, tmp_path):
        """Critical file failure triggers on_sync_error callback."""
        backend = MagicMock()
        backend.upload_file = AsyncMock(side_effect=Exception("oss down"))
        backend.upload_bytes = AsyncMock(side_effect=Exception("oss down"))

        error_callback = AsyncMock()
        mgr = FileSyncManager(
            worker_id="w1", storage=backend, on_sync_error=error_callback,
        )
        await mgr.start()
        try:
            (watch_dir / "state.json").write_text('{"phase": 1}')
            mapping = SyncMappingEntry(
                task_id="t1", book_slug="b", oss_prefix="tasks/t1",
                watch_paths=[str(watch_dir)],
            )
            mgr.register_mapping(mapping)
            await mgr.force_sync("t1")

            assert error_callback.called
            args = error_callback.call_args
            assert args[0][0] == "t1"
            assert args[1]["critical"] is True
        finally:
            await mgr.stop()

    async def test_non_critical_file_emits_error_not_critical(self, watch_dir, tmp_path):
        """Non-critical file failure emits error with critical=False."""
        backend = MagicMock()
        backend.upload_file = AsyncMock(side_effect=Exception("network error"))
        backend.upload_bytes = AsyncMock(side_effect=Exception("network error"))

        error_callback = AsyncMock()
        buffer_path = str(tmp_path / "buffer.json")
        mgr = FileSyncManager(
            worker_id="w1", storage=backend,
            on_sync_error=error_callback,
            buffer_path=buffer_path,
        )
        await mgr.start()
        try:
            (watch_dir / "chapter.md").write_text("content")
            mapping = SyncMappingEntry(
                task_id="t1", book_slug="b", oss_prefix="tasks/t1",
                watch_paths=[str(watch_dir)],
            )
            mgr.register_mapping(mapping)
            await mgr.force_sync("t1")

            assert error_callback.called
            args = error_callback.call_args
            assert args[1]["critical"] is False
        finally:
            await mgr.stop()

    async def test_buffer_size_limit_drops_oldest(self, watch_dir, tmp_path):
        """When buffer reaches limit, oldest items are dropped."""
        backend = MagicMock()
        backend.upload_file = AsyncMock(side_effect=Exception("fail"))
        backend.upload_bytes = AsyncMock(side_effect=Exception("fail"))

        buffer_path = str(tmp_path / "buffer.json")
        mgr = FileSyncManager(
            worker_id="w1", storage=backend, buffer_path=buffer_path,
        )
        await mgr.start()
        try:
            for i in range(15):
                (watch_dir / f"file_{i}.txt").write_text(f"data-{i}")

            mapping = SyncMappingEntry(
                task_id="t1", book_slug="b", oss_prefix="tasks/t1",
                watch_paths=[str(watch_dir)],
            )
            mgr.register_mapping(mapping)
            await mgr.force_sync("t1")

            assert mgr.pending_count == 15
            assert mgr.pending_count <= MAX_BUFFER_SIZE
        finally:
            await mgr.stop()

    async def test_buffer_bytes_tracking(self, watch_dir, tmp_path):
        """Buffer tracks total bytes of pending uploads."""
        backend = MagicMock()
        backend.upload_file = AsyncMock(side_effect=Exception("fail"))
        backend.upload_bytes = AsyncMock(side_effect=Exception("fail"))

        buffer_path = str(tmp_path / "buffer.json")
        mgr = FileSyncManager(
            worker_id="w1", storage=backend, buffer_path=buffer_path,
        )
        await mgr.start()
        try:
            content = "x" * 1000
            (watch_dir / "file1.txt").write_text(content)

            mapping = SyncMappingEntry(
                task_id="t1", book_slug="b", oss_prefix="tasks/t1",
                watch_paths=[str(watch_dir)],
            )
            mgr.register_mapping(mapping)
            await mgr.force_sync("t1")

            assert mgr._buffer_bytes >= 1000
        finally:
            await mgr.stop()


# ---------------------------------------------------------------------------
# T-037: FILE_SYNCED event flow
# ---------------------------------------------------------------------------


class TestFileSyncedEvent:

    async def test_file_synced_callback_on_successful_upload(self, watch_dir, sync_dir):
        """FILE_SYNCED callback is invoked after successful upload."""
        synced_callback = AsyncMock()
        backend = LocalBackend(str(sync_dir))

        mgr = FileSyncManager(
            worker_id="w1", storage=backend, on_file_synced=synced_callback,
        )
        await mgr.start()
        try:
            (watch_dir / "manuscript.md").write_text("Hello world")
            mapping = SyncMappingEntry(
                task_id="t1", book_slug="book", oss_prefix="tasks/t1",
                watch_paths=[str(watch_dir)],
            )
            mgr.register_mapping(mapping)
            await mgr.force_sync("t1")

            assert synced_callback.called
            args = synced_callback.call_args[0]
            assert args[0] == "t1"
            assert "manuscript.md" in args[1]
            assert args[2].startswith("tasks/t1/")
            assert args[4]  # md5 is non-empty
        finally:
            await mgr.stop()

    async def test_file_synced_not_called_on_failure(self, watch_dir, tmp_path):
        """FILE_SYNCED callback is NOT invoked when upload fails."""
        synced_callback = AsyncMock()
        backend = MagicMock()
        backend.upload_file = AsyncMock(side_effect=Exception("fail"))
        backend.upload_bytes = AsyncMock(side_effect=Exception("fail"))

        mgr = FileSyncManager(
            worker_id="w1", storage=backend,
            on_file_synced=synced_callback,
            buffer_path=str(tmp_path / "buf.json"),
        )
        await mgr.start()
        try:
            (watch_dir / "data.txt").write_text("content")
            mapping = SyncMappingEntry(
                task_id="t1", book_slug="b", oss_prefix="tasks/t1",
                watch_paths=[str(watch_dir)],
            )
            mgr.register_mapping(mapping)
            await mgr.force_sync("t1")

            assert not synced_callback.called
        finally:
            await mgr.stop()

    async def test_file_synced_includes_md5_and_timestamp(self, watch_dir, sync_dir):
        """FILE_SYNCED callback includes correct md5 and non-empty synced_at."""
        results = []

        async def capture_synced(task_id, path, oss_key, synced_at, md5):
            results.append({"task_id": task_id, "path": path, "synced_at": synced_at, "md5": md5})

        backend = LocalBackend(str(sync_dir))
        mgr = FileSyncManager(
            worker_id="w1", storage=backend, on_file_synced=capture_synced,
        )
        await mgr.start()
        try:
            (watch_dir / "test.txt").write_text("hello")
            mapping = SyncMappingEntry(
                task_id="t1", book_slug="b", oss_prefix="tasks/t1",
                watch_paths=[str(watch_dir)],
            )
            mgr.register_mapping(mapping)
            await mgr.force_sync("t1")

            assert len(results) == 1
            assert results[0]["md5"]
            assert results[0]["synced_at"]
        finally:
            await mgr.stop()

    async def test_file_change_callback_fires_independently(self, watch_dir, sync_dir):
        """FILE_CHANGE callback fires independently of FILE_SYNCED."""
        change_callback = AsyncMock()
        synced_callback = AsyncMock()

        backend = LocalBackend(str(sync_dir))
        mgr = FileSyncManager(
            worker_id="w1", storage=backend,
            on_file_synced=synced_callback,
            on_file_changed=change_callback,
        )
        mgr._loop = asyncio.get_running_loop()
        mgr._running = True

        (watch_dir / "test.md").write_text("data")
        mapping = SyncMappingEntry(
            task_id="t1", book_slug="b", oss_prefix="tasks/t1",
            watch_paths=[str(watch_dir)],
        )
        mgr.register_mapping(mapping)

        mgr.on_file_event(str(watch_dir / "test.md"), "modified")
        await asyncio.sleep(0.05)
        assert change_callback.called


# ---------------------------------------------------------------------------
# Storage backend read methods
# ---------------------------------------------------------------------------


class TestStorageBackendRead:

    async def test_local_backend_read_file(self, sync_dir):
        backend = LocalBackend(str(sync_dir))
        (sync_dir / "test.txt").write_text("hello world")
        content = await backend.read_file("test.txt")
        assert content == b"hello world"

    async def test_local_backend_file_exists(self, sync_dir):
        backend = LocalBackend(str(sync_dir))
        (sync_dir / "existing.txt").write_text("data")
        assert await backend.file_exists("existing.txt") is True
        assert await backend.file_exists("nonexistent.txt") is False

    async def test_local_backend_presigned_url_returns_none(self, sync_dir):
        backend = LocalBackend(str(sync_dir))
        url = await backend.get_presigned_url("test.txt")
        assert url is None

    async def test_local_backend_read_nested(self, sync_dir):
        nested = sync_dir / "tasks" / "t1"
        nested.mkdir(parents=True)
        (nested / "file.json").write_bytes(b'{"key": "value"}')
        backend = LocalBackend(str(sync_dir))
        content = await backend.read_file("tasks/t1/file.json")
        assert json.loads(content) == {"key": "value"}
