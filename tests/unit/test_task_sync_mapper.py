"""Unit tests for TaskSyncMapper (T-034)."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from elastic_agent.worker.task_sync_mapper import ActiveMapping, TaskSyncMapper


class TestTaskSyncMapper:

    def test_register_creates_mapping(self):
        mapper = TaskSyncMapper()
        mapping = mapper.register(
            task_id="task-1",
            book_slug="test-book",
            oss_prefix="oss://bucket/tasks/task-1/",
            watch_paths=["/root/.work/test-book/"],
            session_path_hash="abc123",
        )
        assert mapping.task_id == "task-1"
        assert mapping.book_slug == "test-book"
        assert mapper.task_count == 1
        assert mapper.has_task("task-1")

    def test_register_replaces_existing(self):
        mapper = TaskSyncMapper()
        mapper.register("task-1", "book-a", "prefix-a/", ["/path-a/"])
        mapper.register("task-1", "book-b", "prefix-b/", ["/path-b/"])
        assert mapper.task_count == 1
        m = mapper.get_mapping("task-1")
        assert m.book_slug == "book-b"

    def test_get_mapping_returns_none_for_missing(self):
        mapper = TaskSyncMapper()
        assert mapper.get_mapping("nonexistent") is None

    def test_has_task(self):
        mapper = TaskSyncMapper()
        assert mapper.has_task("task-1") is False
        mapper.register("task-1", "book", "prefix/", ["/path/"])
        assert mapper.has_task("task-1") is True

    async def test_unregister_removes_mapping(self):
        mapper = TaskSyncMapper()
        mapper.register("task-1", "book", "prefix/", ["/path/"])
        result = await mapper.unregister("task-1")
        assert result is True
        assert mapper.task_count == 0
        assert mapper.has_task("task-1") is False

    async def test_unregister_nonexistent_returns_false(self):
        mapper = TaskSyncMapper()
        result = await mapper.unregister("nonexistent")
        assert result is False

    def test_match_path_finds_correct_task(self):
        mapper = TaskSyncMapper()
        mapper.register("task-1", "book-a", "p1/", ["/root/.work/book-a/"])
        mapper.register("task-2", "book-b", "p2/", ["/root/.work/book-b/"])

        result = mapper.match_path("/root/.work/book-a/chapter01.md")
        assert result is not None
        assert result[0] == "task-1"

        result = mapper.match_path("/root/.work/book-b/state.json")
        assert result is not None
        assert result[0] == "task-2"

    def test_match_path_returns_none_for_unmatched(self):
        mapper = TaskSyncMapper()
        mapper.register("task-1", "book", "p/", ["/root/.work/book/"])
        assert mapper.match_path("/other/path/file.txt") is None

    def test_match_path_handles_multiple_watch_paths(self):
        mapper = TaskSyncMapper()
        mapper.register(
            "task-1", "book", "p/",
            ["/root/.work/book/", "/root/.claude/projects/"],
        )
        result = mapper.match_path("/root/.claude/projects/config.json")
        assert result is not None
        assert result[0] == "task-1"

    def test_all_watch_paths(self):
        mapper = TaskSyncMapper()
        mapper.register("task-1", "a", "p1/", ["/path-1/", "/path-2/"])
        mapper.register("task-2", "b", "p2/", ["/path-3/"])
        paths = mapper.all_watch_paths()
        assert set(paths) == {"/path-1/", "/path-2/", "/path-3/"}

    def test_active_mappings_is_copy(self):
        mapper = TaskSyncMapper()
        mapper.register("task-1", "book", "p/", ["/path/"])
        mappings = mapper.active_mappings
        mappings["task-2"] = None
        assert mapper.task_count == 1

    def test_on_register_callback(self):
        callback = MagicMock()
        mapper = TaskSyncMapper(on_register=callback)
        mapper.register("task-1", "book", "p/", ["/path/"])
        callback.assert_called_once()
        arg = callback.call_args[0][0]
        assert isinstance(arg, ActiveMapping)
        assert arg.task_id == "task-1"

    async def test_on_unregister_callback(self):
        callback = AsyncMock()
        mapper = TaskSyncMapper(on_unregister=callback)
        mapper.register("task-1", "book", "p/", ["/path/"])
        await mapper.unregister("task-1")
        callback.assert_called_once_with("task-1")

    async def test_unregister_all(self):
        mapper = TaskSyncMapper()
        mapper.register("task-1", "a", "p1/", ["/p1/"])
        mapper.register("task-2", "b", "p2/", ["/p2/"])
        mapper.register("task-3", "c", "p3/", ["/p3/"])
        count = await mapper.unregister_all()
        assert count == 3
        assert mapper.task_count == 0
