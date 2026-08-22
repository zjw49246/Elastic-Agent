"""T-124: LOG event structured parsing — type extraction, non-JSON tolerance."""

from __future__ import annotations

import pytest

from elastic_agent.core.log_event_parser import LogEventParser, TaskSession, CLAUDE_CODE_TYPES


@pytest.fixture
def parser():
    return LogEventParser(buffer_size=100)


# ---------------------------------------------------------------------------
# Type extraction for each Claude Code type
# ---------------------------------------------------------------------------


class TestTypeExtraction:

    @pytest.mark.parametrize("event_type", ["system", "assistant", "user", "tool_use", "tool_result"])
    def test_non_result_types_return_none(self, parser, event_type):
        data = {
            "task_id": "task-1",
            "stream": "stdout",
            "data": f'{{"type": "{event_type}"}}',
            "parsed": {"type": event_type, "subtype": None, "cost_usd": None, "session_id": None},
        }
        result = parser.process_log_event("worker-1", data)
        assert result is None
        assert parser.buffer_size("task-1") == 1

    def test_result_type_returns_extracted_metadata(self, parser):
        data = {
            "task_id": "task-1",
            "stream": "stdout",
            "data": '{"type": "result", "session_id": "sess-1", "cost_usd": 0.05}',
            "parsed": {"type": "result", "session_id": "sess-1", "cost_usd": 0.05, "subtype": None},
        }
        result = parser.process_log_event("worker-1", data)
        assert result is not None
        assert result["session_id"] == "sess-1"
        assert result["cost_usd"] == 0.05
        assert result["total_cost_usd"] == 0.05

    def test_unknown_type_returns_none(self, parser):
        data = {
            "task_id": "task-1",
            "stream": "stdout",
            "data": "",
            "parsed": {"type": "custom_event", "subtype": None},
        }
        result = parser.process_log_event("worker-1", data)
        assert result is None

    def test_claude_code_types_frozenset(self):
        expected = {"system", "assistant", "user", "tool_use", "tool_result", "result"}
        assert CLAUDE_CODE_TYPES == expected


# ---------------------------------------------------------------------------
# Non-JSON line tolerance
# ---------------------------------------------------------------------------


class TestNonJSONTolerance:

    def test_parsed_none_stored_in_buffer(self, parser):
        data = {
            "task_id": "task-1",
            "stream": "stdout",
            "data": "plain text, not JSON",
            "parsed": None,
        }
        result = parser.process_log_event("worker-1", data)
        assert result is None
        assert parser.buffer_size("task-1") == 1

    def test_parsed_non_dict_ignored(self, parser):
        data = {
            "task_id": "task-1",
            "stream": "stdout",
            "data": "[1,2,3]",
            "parsed": "not a dict",
        }
        result = parser.process_log_event("worker-1", data)
        assert result is None
        assert parser.buffer_size("task-1") == 1

    def test_stderr_lines_stored(self, parser):
        data = {
            "task_id": "task-1",
            "stream": "stderr",
            "data": "Warning: something happened",
            "parsed": None,
        }
        result = parser.process_log_event("worker-1", data)
        assert result is None
        logs = parser.get_task_logs("task-1")
        assert len(logs) == 1
        assert logs[0]["stream"] == "stderr"

    def test_missing_task_id_returns_none(self, parser):
        data = {"stream": "stdout", "data": "no task_id"}
        result = parser.process_log_event("worker-1", data)
        assert result is None
        assert parser.buffer_size("no-such-task") == 0

    def test_mixed_valid_and_invalid_events(self, parser):
        parser.process_log_event("w1", {
            "task_id": "t1", "stream": "stdout", "data": "text", "parsed": None,
        })
        parser.process_log_event("w1", {
            "task_id": "t1", "stream": "stdout", "data": "",
            "parsed": {"type": "assistant", "subtype": None, "cost_usd": None, "session_id": None},
        })
        parser.process_log_event("w1", {
            "task_id": "t1", "stream": "stdout", "data": "",
            "parsed": {"type": "result", "session_id": "s1", "cost_usd": 0.1, "subtype": None},
        })

        assert parser.buffer_size("t1") == 3
        assert parser.get_task_session("t1").session_id == "s1"


# ---------------------------------------------------------------------------
# Buffer behavior
# ---------------------------------------------------------------------------


class TestBufferBehavior:

    def test_buffer_evicts_oldest_on_overflow(self):
        parser = LogEventParser(buffer_size=3)
        for i in range(5):
            parser.process_log_event("w1", {
                "task_id": "t1", "stream": "stdout", "data": f"line-{i}", "parsed": None,
            })
        assert parser.buffer_size("t1") == 3
        logs = parser.get_task_logs("t1")
        assert logs[0]["data"] == "line-2"
        assert logs[-1]["data"] == "line-4"

    def test_separate_buffers_per_task(self, parser):
        for i in range(3):
            parser.process_log_event("w1", {
                "task_id": "t1", "stream": "stdout", "data": f"t1-{i}", "parsed": None,
            })
        for i in range(2):
            parser.process_log_event("w1", {
                "task_id": "t2", "stream": "stdout", "data": f"t2-{i}", "parsed": None,
            })
        assert parser.buffer_size("t1") == 3
        assert parser.buffer_size("t2") == 2

    def test_release_clears_buffer_and_session(self, parser):
        parser.process_log_event("w1", {
            "task_id": "t1", "stream": "stdout", "data": "",
            "parsed": {"type": "result", "session_id": "s1", "cost_usd": 0.1, "subtype": None},
        })
        parser.release_task("t1")
        assert parser.buffer_size("t1") == 0
        assert parser.get_task_session("t1") is None
        assert parser.get_task_cost("t1") == 0.0


# ---------------------------------------------------------------------------
# Filtering
# ---------------------------------------------------------------------------


class TestFiltering:

    def test_filter_by_single_type(self, parser):
        for t in ["system", "assistant", "tool_use", "assistant", "result"]:
            parser.process_log_event("w1", {
                "task_id": "t1", "stream": "stdout", "data": f"type={t}",
                "parsed": {"type": t, "subtype": None,
                           "cost_usd": 0.01 if t == "result" else None,
                           "session_id": None},
            })
        logs = parser.get_task_logs("t1", types=["assistant"])
        assert len(logs) == 2

    def test_filter_by_multiple_types(self, parser):
        for t in ["system", "assistant", "tool_use", "result"]:
            parser.process_log_event("w1", {
                "task_id": "t1", "stream": "stdout", "data": "",
                "parsed": {"type": t, "subtype": None,
                           "cost_usd": 0.01 if t == "result" else None,
                           "session_id": None},
            })
        logs = parser.get_task_logs("t1", types=["assistant", "result"])
        assert len(logs) == 2

    def test_filter_excludes_none_parsed(self, parser):
        parser.process_log_event("w1", {
            "task_id": "t1", "stream": "stdout", "data": "text", "parsed": None,
        })
        parser.process_log_event("w1", {
            "task_id": "t1", "stream": "stdout", "data": "",
            "parsed": {"type": "assistant", "subtype": None, "cost_usd": None, "session_id": None},
        })
        logs = parser.get_task_logs("t1", types=["assistant"])
        assert len(logs) == 1

    def test_limit_returns_tail(self, parser):
        for i in range(10):
            parser.process_log_event("w1", {
                "task_id": "t1", "stream": "stdout", "data": f"line-{i}", "parsed": None,
            })
        logs = parser.get_task_logs("t1", limit=3)
        assert len(logs) == 3
        assert logs[0]["data"] == "line-7"
        assert logs[2]["data"] == "line-9"

    def test_empty_task_returns_empty(self, parser):
        assert parser.get_task_logs("nonexistent") == []
        assert parser.get_task_logs("nonexistent", types=["result"]) == []


# ---------------------------------------------------------------------------
# Session and cost extraction
# ---------------------------------------------------------------------------


class TestSessionAndCost:

    def test_session_id_extracted_from_result(self, parser):
        parser.process_log_event("w1", {
            "task_id": "t1", "stream": "stdout", "data": "",
            "parsed": {"type": "result", "session_id": "sess-abc", "cost_usd": 0.0, "subtype": None},
        })
        s = parser.get_task_session("t1")
        assert s.session_id == "sess-abc"

    def test_session_id_updated_by_later_result(self, parser):
        for sid in ["sess-1", "sess-2"]:
            parser.process_log_event("w1", {
                "task_id": "t1", "stream": "stdout", "data": "",
                "parsed": {"type": "result", "session_id": sid, "cost_usd": 0.01, "subtype": None},
            })
        assert parser.get_task_session("t1").session_id == "sess-2"

    def test_cost_accumulated(self, parser):
        for cost in [0.05, 0.10, 0.03]:
            parser.process_log_event("w1", {
                "task_id": "t1", "stream": "stdout", "data": "",
                "parsed": {"type": "result", "session_id": None, "cost_usd": cost, "subtype": None},
            })
        assert abs(parser.get_task_cost("t1") - 0.18) < 0.001

    def test_worker_cost_accumulated_across_tasks(self, parser):
        for tid in ["t1", "t2"]:
            parser.process_log_event("w1", {
                "task_id": tid, "stream": "stdout", "data": "",
                "parsed": {"type": "result", "session_id": None, "cost_usd": 0.10, "subtype": None},
            })
        assert abs(parser.get_worker_cost("w1") - 0.20) < 0.001

    def test_invalid_cost_ignored(self, parser):
        parser.process_log_event("w1", {
            "task_id": "t1", "stream": "stdout", "data": "",
            "parsed": {"type": "result", "session_id": None, "cost_usd": "bad", "subtype": None},
        })
        assert parser.get_task_cost("t1") == 0.0

    def test_none_cost_ignored(self, parser):
        parser.process_log_event("w1", {
            "task_id": "t1", "stream": "stdout", "data": "",
            "parsed": {"type": "result", "session_id": "s", "cost_usd": None, "subtype": None},
        })
        assert parser.get_task_cost("t1") == 0.0

    def test_no_session_for_nonexistent_task(self, parser):
        assert parser.get_task_session("x") is None
        assert parser.get_task_cost("x") == 0.0
        assert parser.get_worker_cost("x") == 0.0


# ---------------------------------------------------------------------------
# Active tasks
# ---------------------------------------------------------------------------


class TestActiveTasks:

    def test_active_tasks_lists_buffered_tasks(self, parser):
        parser.process_log_event("w1", {"task_id": "a", "stream": "stdout", "data": "", "parsed": None})
        parser.process_log_event("w1", {"task_id": "b", "stream": "stdout", "data": "", "parsed": None})
        assert set(parser.active_tasks) == {"a", "b"}

    def test_active_tasks_after_release(self, parser):
        parser.process_log_event("w1", {"task_id": "a", "stream": "stdout", "data": "", "parsed": None})
        parser.process_log_event("w1", {"task_id": "b", "stream": "stdout", "data": "", "parsed": None})
        parser.release_task("a")
        assert parser.active_tasks == ["b"]
