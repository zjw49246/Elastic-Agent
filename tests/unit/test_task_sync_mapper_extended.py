"""T-123: TaskSyncMapper — register/unregister mappings, path matching, multi-task coexistence."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from elastic_agent.worker.task_sync_mapper import ActiveMapping, TaskSyncMapper


# ---------------------------------------------------------------------------
# Registration lifecycle
# ---------------------------------------------------------------------------


class TestRegistrationLifecycle:

    def test_register_creates_mapping_with_all_fields(self):
        mapper = TaskSyncMapper()
        m = mapper.register(
            task_id="task-1",
            book_slug="my-book",
            oss_prefix="oss://bucket/tasks/task-1/",
            watch_paths=["/root/.work/my-book/"],
            session_path_hash="hash123",
        )
        assert m.task_id == "task-1"
        assert m.book_slug == "my-book"
        assert m.oss_prefix == "oss://bucket/tasks/task-1/"
        assert m.watch_paths == ["/root/.work/my-book/"]
        assert m.session_path_hash == "hash123"
        assert m.registered_at is not None

    def test_register_replaces_existing_mapping(self):
        mapper = TaskSyncMapper()
        mapper.register("task-1", "book-v1", "p1/", ["/v1/"])
        mapper.register("task-1", "book-v2", "p2/", ["/v2/"])
        assert mapper.task_count == 1
        m = mapper.get_mapping("task-1")
        assert m.book_slug == "book-v2"
        assert m.watch_paths == ["/v2/"]

    async def test_unregister_removes_mapping(self):
        mapper = TaskSyncMapper()
        mapper.register("task-1", "book", "p/", ["/path/"])
        result = await mapper.unregister("task-1")
        assert result is True
        assert mapper.task_count == 0
        assert mapper.has_task("task-1") is False

    async def test_unregister_nonexistent_returns_false(self):
        mapper = TaskSyncMapper()
        result = await mapper.unregister("no-such-task")
        assert result is False

    async def test_unregister_all_clears_everything(self):
        mapper = TaskSyncMapper()
        for i in range(5):
            mapper.register(f"task-{i}", f"book-{i}", f"p{i}/", [f"/path-{i}/"])
        count = await mapper.unregister_all()
        assert count == 5
        assert mapper.task_count == 0


# ---------------------------------------------------------------------------
# Path matching
# ---------------------------------------------------------------------------


class TestPathMatching:

    def test_exact_path_match(self):
        mapper = TaskSyncMapper()
        mapper.register("task-1", "book", "p/", ["/root/.work/book"])
        result = mapper.match_path("/root/.work/book")
        assert result is not None
        assert result[0] == "task-1"

    def test_subpath_match(self):
        mapper = TaskSyncMapper()
        mapper.register("task-1", "book", "p/", ["/root/.work/book/"])
        result = mapper.match_path("/root/.work/book/chapter01.md")
        assert result is not None
        assert result[0] == "task-1"

    def test_nested_subpath_match(self):
        mapper = TaskSyncMapper()
        mapper.register("task-1", "book", "p/", ["/root/.work/book/"])
        result = mapper.match_path("/root/.work/book/subdir/deep/file.txt")
        assert result is not None
        assert result[0] == "task-1"

    def test_no_match_for_different_prefix(self):
        mapper = TaskSyncMapper()
        mapper.register("task-1", "book", "p/", ["/root/.work/book/"])
        assert mapper.match_path("/root/.work/other/file.txt") is None

    def test_no_match_for_partial_prefix(self):
        mapper = TaskSyncMapper()
        mapper.register("task-1", "book", "p/", ["/root/.work/book/"])
        assert mapper.match_path("/root/.work/bookmarks/file.txt") is None

    def test_trailing_slash_normalized(self):
        mapper = TaskSyncMapper()
        mapper.register("task-1", "book", "p/", ["/root/.work/book"])
        result = mapper.match_path("/root/.work/book/file.txt")
        assert result is not None
        assert result[0] == "task-1"

    def test_multiple_watch_paths_per_task(self):
        mapper = TaskSyncMapper()
        mapper.register(
            "task-1", "book", "p/",
            ["/root/.work/book/", "/root/.claude/config/"],
        )
        r1 = mapper.match_path("/root/.work/book/chapter.md")
        r2 = mapper.match_path("/root/.claude/config/settings.json")
        assert r1 is not None and r1[0] == "task-1"
        assert r2 is not None and r2[0] == "task-1"


# ---------------------------------------------------------------------------
# Multi-task coexistence
# ---------------------------------------------------------------------------


class TestMultiTaskCoexistence:

    def test_multiple_tasks_distinct_paths(self):
        mapper = TaskSyncMapper()
        mapper.register("task-a", "book-a", "pa/", ["/work/book-a/"])
        mapper.register("task-b", "book-b", "pb/", ["/work/book-b/"])
        mapper.register("task-c", "book-c", "pc/", ["/work/book-c/"])

        assert mapper.match_path("/work/book-a/f.md")[0] == "task-a"
        assert mapper.match_path("/work/book-b/f.md")[0] == "task-b"
        assert mapper.match_path("/work/book-c/f.md")[0] == "task-c"

    def test_all_watch_paths_aggregated(self):
        mapper = TaskSyncMapper()
        mapper.register("t1", "a", "p1/", ["/path1/", "/path2/"])
        mapper.register("t2", "b", "p2/", ["/path3/"])
        paths = mapper.all_watch_paths()
        assert set(paths) == {"/path1/", "/path2/", "/path3/"}

    async def test_unregister_one_doesnt_affect_others(self):
        mapper = TaskSyncMapper()
        mapper.register("t1", "a", "p1/", ["/p1/"])
        mapper.register("t2", "b", "p2/", ["/p2/"])

        await mapper.unregister("t1")
        assert mapper.has_task("t1") is False
        assert mapper.has_task("t2") is True
        assert mapper.match_path("/p2/file.txt")[0] == "t2"

    def test_active_mappings_returns_copy(self):
        mapper = TaskSyncMapper()
        mapper.register("t1", "a", "p/", ["/p/"])
        copy = mapper.active_mappings
        copy.clear()
        assert mapper.task_count == 1


# ---------------------------------------------------------------------------
# Callbacks
# ---------------------------------------------------------------------------


class TestCallbacks:

    def test_on_register_called_with_mapping(self):
        cb = MagicMock()
        mapper = TaskSyncMapper(on_register=cb)
        mapper.register("task-1", "book", "p/", ["/path/"])
        cb.assert_called_once()
        arg = cb.call_args[0][0]
        assert isinstance(arg, ActiveMapping)
        assert arg.task_id == "task-1"

    async def test_on_unregister_called_with_task_id(self):
        cb = AsyncMock()
        mapper = TaskSyncMapper(on_unregister=cb)
        mapper.register("task-1", "book", "p/", ["/path/"])
        await mapper.unregister("task-1")
        cb.assert_called_once_with("task-1")

    async def test_on_unregister_called_before_removal(self):
        call_order = []

        async def on_unreg(task_id):
            call_order.append(("unreg_callback", task_id))

        mapper = TaskSyncMapper(on_unregister=on_unreg)
        mapper.register("task-1", "book", "p/", ["/path/"])
        await mapper.unregister("task-1")
        assert ("unreg_callback", "task-1") in call_order

    def test_no_callback_if_not_set(self):
        mapper = TaskSyncMapper()
        mapper.register("task-1", "book", "p/", ["/path/"])

    async def test_unregister_all_calls_callback_per_task(self):
        cb = AsyncMock()
        mapper = TaskSyncMapper(on_unregister=cb)
        mapper.register("t1", "a", "p1/", ["/p1/"])
        mapper.register("t2", "b", "p2/", ["/p2/"])
        await mapper.unregister_all()
        assert cb.call_count == 2

    def test_on_register_called_on_replacement(self):
        cb = MagicMock()
        mapper = TaskSyncMapper(on_register=cb)
        mapper.register("task-1", "book-v1", "p1/", ["/v1/"])
        mapper.register("task-1", "book-v2", "p2/", ["/v2/"])
        assert cb.call_count == 2


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:

    def test_empty_watch_paths(self):
        mapper = TaskSyncMapper()
        m = mapper.register("task-1", "book", "p/", [])
        assert m.watch_paths == []
        assert mapper.match_path("/any/file.txt") is None

    def test_get_mapping_returns_none_for_missing(self):
        mapper = TaskSyncMapper()
        assert mapper.get_mapping("nope") is None

    def test_has_task_false_initially(self):
        mapper = TaskSyncMapper()
        assert mapper.has_task("any") is False

    def test_task_count_accuracy(self):
        mapper = TaskSyncMapper()
        assert mapper.task_count == 0
        mapper.register("t1", "a", "p/", ["/p/"])
        assert mapper.task_count == 1
        mapper.register("t2", "b", "p/", ["/p/"])
        assert mapper.task_count == 2
        mapper.register("t1", "a2", "p/", ["/p/"])
        assert mapper.task_count == 2

    def test_session_path_hash_default_empty(self):
        mapper = TaskSyncMapper()
        m = mapper.register("t1", "b", "p/", ["/p/"])
        assert m.session_path_hash == ""
