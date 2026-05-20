"""T-119: FileSyncManager debounce logic + sync list generation + large/small file routing."""

from __future__ import annotations

import asyncio
import json
import os
import time
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from elastic_agent.worker.file_sync import (
    FileSyncManager,
    LocalBackend,
    SyncMappingEntry,
    SyncManifest,
    SyncedFile,
    PendingUpload,
    StorageBackend,
    _file_md5,
    _guess_role,
    _is_critical,
    _match_debounce,
    _should_exclude,
    MAX_BUFFER_SIZE,
    MAX_BUFFER_BYTES,
    MAX_RETRIES,
    MULTIPART_THRESHOLD,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


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
def debounce_tiers():
    return {"state.json": 0.5, "manuscript_*": 2.0}


@pytest.fixture
def sync_manager(local_backend, watch_dir, debounce_tiers):
    return FileSyncManager(
        worker_id="test-worker-1",
        storage=local_backend,
        debounce_tiers=debounce_tiers,
        exclude_patterns=["*.tmp", "*.swp", "__pycache__/"],
        buffer_path=None,
    )


# ---------------------------------------------------------------------------
# Debounce tier matching
# ---------------------------------------------------------------------------


class TestDebounceTierMatching:

    def test_state_json_gets_fast_debounce(self, debounce_tiers):
        assert _match_debounce("state.json", debounce_tiers) == 0.5

    def test_manuscript_glob_match(self, debounce_tiers):
        assert _match_debounce("manuscript_ch01.md", debounce_tiers) == 2.0
        assert _match_debounce("manuscript_final.txt", debounce_tiers) == 2.0

    def test_default_debounce_for_unmatched(self, debounce_tiers):
        assert _match_debounce("random_file.py", debounce_tiers, default=5.0) == 5.0

    def test_empty_tiers_returns_default(self):
        assert _match_debounce("any.txt", {}, default=3.0) == 3.0

    def test_full_path_uses_basename(self, debounce_tiers):
        assert _match_debounce("/root/work/book/state.json", debounce_tiers) == 0.5

    def test_first_match_wins(self):
        tiers = {"*.json": 1.0, "state.json": 0.5}
        result = _match_debounce("state.json", tiers)
        assert result == 1.0


# ---------------------------------------------------------------------------
# Critical file detection
# ---------------------------------------------------------------------------


class TestCriticalFileHandling:

    def test_state_json_is_critical(self):
        assert _is_critical("state.json") is True
        assert _is_critical("/root/work/state.json") is True

    def test_manifest_json_is_critical(self):
        assert _is_critical("manifest.json") is True

    def test_sync_manifest_is_critical(self):
        assert _is_critical("_sync_manifest.json") is True

    def test_regular_file_not_critical(self):
        assert _is_critical("chapter01.md") is False
        assert _is_critical("audio.mp3") is False


# ---------------------------------------------------------------------------
# on_file_event debounce behavior
# ---------------------------------------------------------------------------


class TestOnFileEventDebounce:

    async def test_critical_file_bypasses_debounce(self, local_backend, watch_dir):
        callback = AsyncMock()
        mgr = FileSyncManager(
            worker_id="w1",
            storage=local_backend,
            debounce_tiers={"state.json": 10.0},
            on_file_synced=callback,
        )
        await mgr.start()
        try:
            state_path = watch_dir / "state.json"
            state_path.write_text('{"phase": 1}')

            mapping = SyncMappingEntry(
                task_id="t1", book_slug="b", oss_prefix="tasks/t1",
                watch_paths=[str(watch_dir)],
            )
            mgr.register_mapping(mapping)

            mgr.on_file_event(str(state_path), "modified")
            await asyncio.sleep(0.5)

            assert callback.called
        finally:
            await mgr.stop()

    async def test_normal_file_uses_debounce(self, local_backend, watch_dir):
        callback = AsyncMock()
        mgr = FileSyncManager(
            worker_id="w1",
            storage=local_backend,
            debounce_tiers={"*": 0.5},
            on_file_synced=callback,
        )
        await mgr.start()
        try:
            md_path = watch_dir / "chapter.md"
            md_path.write_text("content")

            mapping = SyncMappingEntry(
                task_id="t1", book_slug="b", oss_prefix="tasks/t1",
                watch_paths=[str(watch_dir)],
            )
            mgr.register_mapping(mapping)

            mgr.on_file_event(str(md_path), "modified")
            await asyncio.sleep(0.1)
            assert not callback.called

            await asyncio.sleep(0.6)
            assert callback.called
        finally:
            await mgr.stop()

    async def test_debounce_timer_reset_on_rapid_changes(self, local_backend, watch_dir):
        callback = AsyncMock()
        mgr = FileSyncManager(
            worker_id="w1",
            storage=local_backend,
            debounce_tiers={"*": 0.5},
            on_file_synced=callback,
        )
        await mgr.start()
        try:
            md_path = watch_dir / "chapter.md"
            md_path.write_text("v1")

            mapping = SyncMappingEntry(
                task_id="t1", book_slug="b", oss_prefix="tasks/t1",
                watch_paths=[str(watch_dir)],
            )
            mgr.register_mapping(mapping)

            mgr.on_file_event(str(md_path), "modified")
            await asyncio.sleep(0.3)
            md_path.write_text("v2")
            mgr.on_file_event(str(md_path), "modified")
            await asyncio.sleep(0.3)
            assert not callback.called

            await asyncio.sleep(0.4)
            assert callback.call_count == 1
        finally:
            await mgr.stop()

    async def test_excluded_file_not_synced(self, sync_manager, watch_dir):
        await sync_manager.start()
        try:
            mapping = SyncMappingEntry(
                task_id="t1", book_slug="b", oss_prefix="p/",
                watch_paths=[str(watch_dir)],
            )
            sync_manager.register_mapping(mapping)

            (watch_dir / "temp.tmp").write_text("temp")
            sync_manager.on_file_event(str(watch_dir / "temp.tmp"), "created")

            (watch_dir / "edit.swp").write_text("swap")
            sync_manager.on_file_event(str(watch_dir / "edit.swp"), "created")
        finally:
            await sync_manager.stop()

    async def test_unmatched_path_ignored(self, sync_manager, watch_dir):
        await sync_manager.start()
        try:
            mapping = SyncMappingEntry(
                task_id="t1", book_slug="b", oss_prefix="p/",
                watch_paths=[str(watch_dir)],
            )
            sync_manager.register_mapping(mapping)
            sync_manager.on_file_event("/some/other/file.md", "created")
        finally:
            await sync_manager.stop()


# ---------------------------------------------------------------------------
# Sync list generation (force_sync)
# ---------------------------------------------------------------------------


class TestSyncListGeneration:

    async def test_force_sync_walks_directory_tree(self, sync_manager, watch_dir, sync_dir):
        await sync_manager.start()
        try:
            sub = watch_dir / "subdir"
            sub.mkdir()
            (watch_dir / "root.md").write_text("root")
            (sub / "nested.md").write_text("nested")

            mapping = SyncMappingEntry(
                task_id="t1", book_slug="b", oss_prefix="tasks/t1",
                watch_paths=[str(watch_dir)],
            )
            sync_manager.register_mapping(mapping)

            count = await sync_manager.force_sync("t1")
            assert count == 2
            assert (sync_dir / "tasks" / "t1" / "root.md").exists()
            assert (sync_dir / "tasks" / "t1" / "subdir" / "nested.md").exists()
        finally:
            await sync_manager.stop()

    async def test_force_sync_skips_excluded(self, sync_manager, watch_dir, sync_dir):
        await sync_manager.start()
        try:
            (watch_dir / "good.md").write_text("ok")
            (watch_dir / "bad.tmp").write_text("skip")

            mapping = SyncMappingEntry(
                task_id="t1", book_slug="b", oss_prefix="tasks/t1",
                watch_paths=[str(watch_dir)],
            )
            sync_manager.register_mapping(mapping)
            count = await sync_manager.force_sync("t1")
            assert count == 1
        finally:
            await sync_manager.stop()

    async def test_force_sync_multiple_watch_paths(self, local_backend, tmp_path, sync_dir):
        dir_a = tmp_path / "a"
        dir_b = tmp_path / "b"
        dir_a.mkdir()
        dir_b.mkdir()
        (dir_a / "file_a.md").write_text("a")
        (dir_b / "file_b.md").write_text("b")

        mgr = FileSyncManager(worker_id="w1", storage=local_backend)
        await mgr.start()
        try:
            mapping = SyncMappingEntry(
                task_id="t1", book_slug="book", oss_prefix="tasks/t1",
                watch_paths=[str(dir_a), str(dir_b)],
            )
            mgr.register_mapping(mapping)
            count = await mgr.force_sync("t1")
            assert count == 2
        finally:
            await mgr.stop()

    async def test_force_sync_nonexistent_watch_path(self, local_backend, tmp_path):
        mgr = FileSyncManager(worker_id="w1", storage=local_backend)
        await mgr.start()
        try:
            mapping = SyncMappingEntry(
                task_id="t1", book_slug="b", oss_prefix="p/",
                watch_paths=[str(tmp_path / "nonexistent")],
            )
            mgr.register_mapping(mapping)
            count = await mgr.force_sync("t1")
            assert count == 0
        finally:
            await mgr.stop()

    async def test_manifest_tracks_synced_files(self, sync_manager, watch_dir, sync_dir):
        await sync_manager.start()
        try:
            (watch_dir / "a.md").write_text("aaa")
            (watch_dir / "b.md").write_text("bbb")

            mapping = SyncMappingEntry(
                task_id="t1", book_slug="b", oss_prefix="tasks/t1",
                watch_paths=[str(watch_dir)],
            )
            sync_manager.register_mapping(mapping)
            await sync_manager.force_sync("t1")

            manifest_path = sync_dir / "tasks" / "t1" / "_sync_manifest.json"
            assert manifest_path.exists()
            manifest = json.loads(manifest_path.read_text())
            assert len(manifest["files"]) == 2
            paths = {f["path"] for f in manifest["files"]}
            assert str(watch_dir / "a.md") in paths
            assert str(watch_dir / "b.md") in paths
        finally:
            await sync_manager.stop()


# ---------------------------------------------------------------------------
# Large/small file routing
# ---------------------------------------------------------------------------


class TestFileSizeRouting:

    async def test_small_file_uses_simple_upload(self, watch_dir, sync_dir):
        backend = AsyncMock(spec=StorageBackend)
        backend.upload_file = AsyncMock()
        backend.upload_bytes = AsyncMock()

        mgr = FileSyncManager(
            worker_id="w1",
            storage=backend,
        )
        await mgr.start()
        try:
            small_file = watch_dir / "small.txt"
            small_file.write_text("small content")

            mapping = SyncMappingEntry(
                task_id="t1", book_slug="b", oss_prefix="tasks/t1",
                watch_paths=[str(watch_dir)],
            )
            mgr.register_mapping(mapping)
            await mgr.force_sync("t1")

            backend.upload_file.assert_called()
        finally:
            await mgr.stop()

    def test_multipart_threshold_constant(self):
        assert MULTIPART_THRESHOLD == 10 * 1024 * 1024


# ---------------------------------------------------------------------------
# Retry buffer management
# ---------------------------------------------------------------------------


class TestRetryBuffer:

    async def test_failed_upload_goes_to_buffer(self, watch_dir, tmp_path):
        failing = AsyncMock(spec=StorageBackend)
        failing.upload_file = AsyncMock(side_effect=Exception("fail"))
        failing.upload_bytes = AsyncMock(side_effect=Exception("fail"))

        mgr = FileSyncManager(
            worker_id="w1",
            storage=failing,
            buffer_path=str(tmp_path / "buffer.json"),
        )
        await mgr.start()
        try:
            (watch_dir / "file.md").write_text("data")
            mapping = SyncMappingEntry(
                task_id="t1", book_slug="b", oss_prefix="tasks/t1",
                watch_paths=[str(watch_dir)],
            )
            mgr.register_mapping(mapping)
            await mgr.force_sync("t1")

            assert mgr.pending_count > 0
        finally:
            await mgr.stop()

    async def test_buffer_drops_oldest_when_full(self, watch_dir, tmp_path):
        mgr = FileSyncManager(
            worker_id="w1",
            storage=AsyncMock(spec=StorageBackend),
        )
        for i in range(MAX_BUFFER_SIZE + 5):
            f = watch_dir / f"file_{i}.txt"
            f.write_text(f"data_{i}")
            mgr._buffer_failed_upload(str(f), f"key_{i}", "task-1")

        assert mgr.pending_count <= MAX_BUFFER_SIZE

    async def test_buffer_persistence_and_reload(self, watch_dir, tmp_path):
        buffer_path = str(tmp_path / "buffer.json")
        failing = AsyncMock(spec=StorageBackend)
        failing.upload_file = AsyncMock(side_effect=Exception("fail"))
        failing.upload_bytes = AsyncMock(side_effect=Exception("fail"))

        mgr = FileSyncManager(
            worker_id="w1", storage=failing, buffer_path=buffer_path,
        )
        await mgr.start()
        try:
            (watch_dir / "test.md").write_text("data")
            mapping = SyncMappingEntry(
                task_id="t1", book_slug="b", oss_prefix="p/",
                watch_paths=[str(watch_dir)],
            )
            mgr.register_mapping(mapping)
            await mgr.force_sync("t1")
        finally:
            await mgr.stop()

        assert Path(buffer_path).exists()

        mgr2 = FileSyncManager(
            worker_id="w1", storage=failing, buffer_path=buffer_path,
        )
        await mgr2.start()
        try:
            assert mgr2.pending_count > 0
        finally:
            await mgr2.stop()

    async def test_critical_file_not_buffered_on_failure(self, watch_dir, tmp_path):
        error_callback = AsyncMock()
        failing = AsyncMock(spec=StorageBackend)
        failing.upload_file = AsyncMock(side_effect=Exception("fail"))
        failing.upload_bytes = AsyncMock(side_effect=Exception("fail"))

        mgr = FileSyncManager(
            worker_id="w1",
            storage=failing,
            on_sync_error=error_callback,
        )
        await mgr.start()
        try:
            (watch_dir / "state.json").write_text('{"phase": 1}')
            mapping = SyncMappingEntry(
                task_id="t1", book_slug="b", oss_prefix="p/",
                watch_paths=[str(watch_dir)],
            )
            mgr.register_mapping(mapping)
            await mgr.force_sync("t1")

            assert error_callback.called
            call_kwargs = error_callback.call_args
            assert call_kwargs[1]["critical"] is True
        finally:
            await mgr.stop()


# ---------------------------------------------------------------------------
# Unregister clears debounce timers
# ---------------------------------------------------------------------------


class TestUnregisterClearsTimers:

    async def test_unregister_cancels_pending_debounce(self, local_backend, watch_dir):
        mgr = FileSyncManager(
            worker_id="w1",
            storage=local_backend,
            debounce_tiers={"*": 10.0},
        )
        await mgr.start()
        try:
            (watch_dir / "file.md").write_text("data")
            mapping = SyncMappingEntry(
                task_id="t1", book_slug="b", oss_prefix="p/",
                watch_paths=[str(watch_dir)],
            )
            mgr.register_mapping(mapping)

            mgr.on_file_event(str(watch_dir / "file.md"), "modified")
            assert any(k.startswith("t1:") for k in mgr._debounce_timers)

            await mgr.unregister_mapping("t1")
            assert not any(k.startswith("t1:") for k in mgr._debounce_timers)
        finally:
            await mgr.stop()


# ---------------------------------------------------------------------------
# File role guessing
# ---------------------------------------------------------------------------


class TestFileRoleGuessing:

    def test_state_file(self):
        assert _guess_role("state.json", {}) == "state"

    def test_audio_files(self):
        for ext in (".mp3", ".wav", ".flac", ".m4a"):
            assert _guess_role(f"track{ext}", {}) == "audio"

    def test_manuscript_prefix(self):
        assert _guess_role("manuscript_ch01.txt", {}) == "manuscript"

    def test_markdown_extension(self):
        assert _guess_role("chapter.md", {}) == "manuscript"

    def test_other(self):
        assert _guess_role("data.csv", {}) == "other"


# ---------------------------------------------------------------------------
# Exclusion patterns
# ---------------------------------------------------------------------------


class TestExclusionPatterns:

    def test_glob_exclusion(self):
        patterns = ["*.tmp", "*.swp"]
        assert _should_exclude("file.tmp", patterns) is True
        assert _should_exclude("edit.swp", patterns) is True
        assert _should_exclude("good.md", patterns) is False

    def test_directory_pattern(self):
        patterns = ["__pycache__/"]
        assert _should_exclude("__pycache__/", patterns) is True

    def test_path_glob(self):
        patterns = ["*/node_modules/*"]
        assert _should_exclude("/root/project/node_modules/pkg/index.js", patterns) is True

    def test_empty_patterns(self):
        assert _should_exclude("any_file.txt", []) is False
