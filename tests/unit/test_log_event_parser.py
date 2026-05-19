"""Tests for LOG event structured parsing (T-040)."""

from __future__ import annotations

import pytest

from elastic_agent.core.log_event_parser import LogEventParser, TaskSession


@pytest.fixture
def parser():
    return LogEventParser(buffer_size=100)


class TestProcessLogEvent:
    def test_basic_log_event(self, parser):
        data = {
            "task_id": "task-1",
            "stream": "stdout",
            "data": '{"type":"assistant","content":"hello"}',
            "timestamp": "2026-05-17T10:00:00Z",
            "parsed": {"type": "assistant", "subtype": None, "cost_usd": None, "session_id": None},
        }
        result = parser.process_log_event("worker-1", data)
        assert result is None  # assistant events don't return extracted metadata

    def test_result_event_extracts_session(self, parser):
        data = {
            "task_id": "task-1",
            "stream": "stdout",
            "data": '{"type":"result","session_id":"sess-abc","cost_usd":0.05}',
            "parsed": {"type": "result", "session_id": "sess-abc", "cost_usd": 0.05, "subtype": None},
        }
        result = parser.process_log_event("worker-1", data)
        assert result is not None
        assert result["session_id"] == "sess-abc"
        assert result["cost_usd"] == 0.05
        assert result["total_cost_usd"] == 0.05

    def test_cost_accumulation(self, parser):
        for i, cost in enumerate([0.05, 0.10, 0.03]):
            data = {
                "task_id": "task-1",
                "stream": "stdout",
                "data": "",
                "parsed": {"type": "result", "session_id": "sess-abc", "cost_usd": cost, "subtype": None},
            }
            result = parser.process_log_event("worker-1", data)

        assert result is not None
        assert abs(result["total_cost_usd"] - 0.18) < 0.001

    def test_stderr_no_parsed(self, parser):
        data = {
            "task_id": "task-1",
            "stream": "stderr",
            "data": "some error message",
            "parsed": None,
        }
        result = parser.process_log_event("worker-1", data)
        assert result is None

    def test_non_json_line(self, parser):
        data = {
            "task_id": "task-1",
            "stream": "stdout",
            "data": "plain text output",
            "parsed": None,
        }
        result = parser.process_log_event("worker-1", data)
        assert result is None

    def test_missing_task_id(self, parser):
        data = {"stream": "stdout", "data": "no task"}
        result = parser.process_log_event("worker-1", data)
        assert result is None

    def test_unknown_type_ignored(self, parser):
        data = {
            "task_id": "task-1",
            "stream": "stdout",
            "data": "",
            "parsed": {"type": "custom_unknown", "subtype": None, "cost_usd": None, "session_id": None},
        }
        result = parser.process_log_event("worker-1", data)
        assert result is None


class TestTaskBuffer:
    def test_buffer_stores_events(self, parser):
        for i in range(5):
            parser.process_log_event("worker-1", {
                "task_id": "task-1",
                "stream": "stdout",
                "data": f"line {i}",
                "parsed": {"type": "assistant", "subtype": None, "cost_usd": None, "session_id": None},
            })
        assert parser.buffer_size("task-1") == 5

    def test_buffer_capacity(self):
        parser = LogEventParser(buffer_size=3)
        for i in range(5):
            parser.process_log_event("worker-1", {
                "task_id": "task-1",
                "stream": "stdout",
                "data": f"line {i}",
                "parsed": None,
            })
        assert parser.buffer_size("task-1") == 3
        logs = parser.get_task_logs("task-1")
        assert logs[0]["data"] == "line 2"

    def test_separate_task_buffers(self, parser):
        parser.process_log_event("worker-1", {"task_id": "t1", "stream": "stdout", "data": "a", "parsed": None})
        parser.process_log_event("worker-1", {"task_id": "t2", "stream": "stdout", "data": "b", "parsed": None})

        assert parser.buffer_size("t1") == 1
        assert parser.buffer_size("t2") == 1

    def test_empty_buffer(self, parser):
        assert parser.buffer_size("nonexistent") == 0
        assert parser.get_task_logs("nonexistent") == []


class TestFilterByType:
    def test_filter_assistant_only(self, parser):
        events = [
            ("system", None),
            ("assistant", None),
            ("tool_use", None),
            ("assistant", None),
            ("result", "sess-1"),
        ]
        for typ, sess in events:
            parser.process_log_event("worker-1", {
                "task_id": "task-1",
                "stream": "stdout",
                "data": f"type={typ}",
                "parsed": {"type": typ, "subtype": None, "cost_usd": 0.01 if typ == "result" else None, "session_id": sess},
            })

        logs = parser.get_task_logs("task-1", types=["assistant"])
        assert len(logs) == 2
        assert all(e["parsed"]["type"] == "assistant" for e in logs)

    def test_filter_multiple_types(self, parser):
        for typ in ["system", "assistant", "tool_use", "tool_result", "result"]:
            parser.process_log_event("worker-1", {
                "task_id": "task-1",
                "stream": "stdout",
                "data": f"type={typ}",
                "parsed": {"type": typ, "subtype": None, "cost_usd": 0.01 if typ == "result" else None, "session_id": None},
            })

        logs = parser.get_task_logs("task-1", types=["assistant", "result"])
        assert len(logs) == 2

    def test_filter_with_limit(self, parser):
        for i in range(10):
            parser.process_log_event("worker-1", {
                "task_id": "task-1",
                "stream": "stdout",
                "data": f"line {i}",
                "parsed": {"type": "assistant", "subtype": None, "cost_usd": None, "session_id": None},
            })

        logs = parser.get_task_logs("task-1", types=["assistant"], limit=3)
        assert len(logs) == 3
        assert logs[0]["data"] == "line 7"

    def test_filter_no_match(self, parser):
        parser.process_log_event("worker-1", {
            "task_id": "task-1",
            "stream": "stdout",
            "data": "hello",
            "parsed": {"type": "system", "subtype": None, "cost_usd": None, "session_id": None},
        })
        logs = parser.get_task_logs("task-1", types=["result"])
        assert len(logs) == 0

    def test_no_filter_returns_all(self, parser):
        for typ in ["system", "assistant", "tool_use"]:
            parser.process_log_event("worker-1", {
                "task_id": "task-1",
                "stream": "stdout",
                "data": f"type={typ}",
                "parsed": {"type": typ, "subtype": None, "cost_usd": None, "session_id": None},
            })
        parser.process_log_event("worker-1", {
            "task_id": "task-1",
            "stream": "stderr",
            "data": "error",
            "parsed": None,
        })

        logs = parser.get_task_logs("task-1")
        assert len(logs) == 4

    def test_limit_without_filter(self, parser):
        for i in range(10):
            parser.process_log_event("worker-1", {
                "task_id": "task-1",
                "stream": "stdout",
                "data": f"line {i}",
                "parsed": None,
            })
        logs = parser.get_task_logs("task-1", limit=5)
        assert len(logs) == 5
        assert logs[0]["data"] == "line 5"


class TestSessionExtraction:
    def test_get_task_session(self, parser):
        parser.process_log_event("worker-1", {
            "task_id": "task-1",
            "stream": "stdout",
            "data": "",
            "parsed": {"type": "result", "session_id": "sess-xyz", "cost_usd": 0.12, "subtype": None},
        })

        session = parser.get_task_session("task-1")
        assert session is not None
        assert session.session_id == "sess-xyz"
        assert session.total_cost_usd == 0.12

    def test_no_session_returns_none(self, parser):
        assert parser.get_task_session("nonexistent") is None

    def test_session_id_updated_by_latest_result(self, parser):
        parser.process_log_event("worker-1", {
            "task_id": "task-1",
            "stream": "stdout",
            "data": "",
            "parsed": {"type": "result", "session_id": "sess-1", "cost_usd": 0.05, "subtype": None},
        })
        parser.process_log_event("worker-1", {
            "task_id": "task-1",
            "stream": "stdout",
            "data": "",
            "parsed": {"type": "result", "session_id": "sess-2", "cost_usd": 0.03, "subtype": None},
        })

        session = parser.get_task_session("task-1")
        assert session.session_id == "sess-2"
        assert abs(session.total_cost_usd - 0.08) < 0.001


class TestCostAggregation:
    def test_get_task_cost(self, parser):
        parser.process_log_event("worker-1", {
            "task_id": "task-1",
            "stream": "stdout",
            "data": "",
            "parsed": {"type": "result", "session_id": None, "cost_usd": 0.25, "subtype": None},
        })
        assert abs(parser.get_task_cost("task-1") - 0.25) < 0.001

    def test_get_task_cost_zero(self, parser):
        assert parser.get_task_cost("nonexistent") == 0.0

    def test_get_worker_cost(self, parser):
        for task in ["t1", "t2"]:
            parser.process_log_event("worker-1", {
                "task_id": task,
                "stream": "stdout",
                "data": "",
                "parsed": {"type": "result", "session_id": None, "cost_usd": 0.10, "subtype": None},
            })
        assert abs(parser.get_worker_cost("worker-1") - 0.20) < 0.001

    def test_get_worker_cost_zero(self, parser):
        assert parser.get_worker_cost("nonexistent") == 0.0

    def test_invalid_cost_ignored(self, parser):
        parser.process_log_event("worker-1", {
            "task_id": "task-1",
            "stream": "stdout",
            "data": "",
            "parsed": {"type": "result", "session_id": None, "cost_usd": "not_a_number", "subtype": None},
        })
        assert parser.get_task_cost("task-1") == 0.0


class TestReleaseTask:
    def test_release_clears_buffer(self, parser):
        parser.process_log_event("worker-1", {
            "task_id": "task-1",
            "stream": "stdout",
            "data": "hello",
            "parsed": {"type": "result", "session_id": "s", "cost_usd": 0.1, "subtype": None},
        })

        assert parser.buffer_size("task-1") == 1
        assert parser.get_task_session("task-1") is not None

        parser.release_task("task-1")

        assert parser.buffer_size("task-1") == 0
        assert parser.get_task_session("task-1") is None

    def test_release_nonexistent_no_error(self, parser):
        parser.release_task("nonexistent")


class TestActiveTasks:
    def test_active_tasks(self, parser):
        parser.process_log_event("w1", {"task_id": "t1", "stream": "stdout", "data": "a", "parsed": None})
        parser.process_log_event("w1", {"task_id": "t2", "stream": "stdout", "data": "b", "parsed": None})

        active = parser.active_tasks
        assert set(active) == {"t1", "t2"}

    def test_active_tasks_after_release(self, parser):
        parser.process_log_event("w1", {"task_id": "t1", "stream": "stdout", "data": "a", "parsed": None})
        parser.process_log_event("w1", {"task_id": "t2", "stream": "stdout", "data": "b", "parsed": None})
        parser.release_task("t1")

        assert parser.active_tasks == ["t2"]
