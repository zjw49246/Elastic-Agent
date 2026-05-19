"""Tests for log_parser (T-040)."""

from __future__ import annotations

import json

import pytest

from elastic_agent.core.log_parser import ParsedLogEvent, parse_log_event
from elastic_agent.core.protocols.messages import LogMessage


def _make_log(data: str, stream: str = "stdout", parsed: dict | None = None) -> LogMessage:
    """Helper to build a LogMessage for testing."""
    return LogMessage(task_id="test-task", stream=stream, data=data, parsed=parsed)


class TestParseLogEventKnownTypes:
    def test_assistant_event(self) -> None:
        data = json.dumps({"type": "assistant", "subtype": "text", "content": "Hello user"})
        result = parse_log_event(_make_log(data))
        assert result.event_type == "assistant"
        assert result.subtype == "text"
        assert result.raw_type == "assistant"
        assert result.raw_subtype == "text"
        assert result.content_preview == "Hello user"

    def test_user_event(self) -> None:
        data = json.dumps({"type": "user", "content": "Please help"})
        result = parse_log_event(_make_log(data))
        assert result.event_type == "user"
        assert result.content_preview == "Please help"

    def test_tool_use_event(self) -> None:
        data = json.dumps({"type": "tool_use", "tool": "bash", "subtype": "bash"})
        result = parse_log_event(_make_log(data))
        assert result.event_type == "tool_use"
        assert result.subtype == "bash"

    def test_tool_result_event(self) -> None:
        data = json.dumps({"type": "tool_result", "content": "output text"})
        result = parse_log_event(_make_log(data))
        assert result.event_type == "tool_result"
        assert result.content_preview == "output text"

    def test_system_event(self) -> None:
        data = json.dumps({"type": "system", "message": "session started"})
        result = parse_log_event(_make_log(data))
        assert result.event_type == "system"
        assert result.content_preview == "session started"

    def test_result_event_with_cost(self) -> None:
        data = json.dumps({
            "type": "result",
            "session_id": "sess-abc",
            "cost_usd": 0.042,
            "content": "done",
        })
        result = parse_log_event(_make_log(data))
        assert result.event_type == "result"
        assert result.session_id == "sess-abc"
        assert result.cost_usd == pytest.approx(0.042)
        assert result.content_preview == "done"

    def test_error_event(self) -> None:
        data = json.dumps({"type": "error", "message": "something broke"})
        result = parse_log_event(_make_log(data))
        assert result.event_type == "error"
        assert result.content_preview == "something broke"


class TestParseLogEventUnknown:
    def test_non_json_line(self) -> None:
        result = parse_log_event(_make_log("plain text output"))
        assert result.event_type == "unknown"
        assert result.content_preview == "plain text output"
        assert result.raw_type is None

    def test_json_without_type_field(self) -> None:
        data = json.dumps({"data": "value", "foo": "bar"})
        result = parse_log_event(_make_log(data))
        assert result.event_type == "unknown"

    def test_unrecognised_type(self) -> None:
        data = json.dumps({"type": "custom_event", "content": "data"})
        result = parse_log_event(_make_log(data))
        assert result.event_type == "unknown"
        assert result.raw_type == "custom_event"

    def test_json_array(self) -> None:
        result = parse_log_event(_make_log("[1,2,3]"))
        assert result.event_type == "unknown"

    def test_empty_string(self) -> None:
        result = parse_log_event(_make_log(""))
        assert result.event_type == "unknown"


class TestParseLogEventEdgeCases:
    def test_content_preview_truncated(self) -> None:
        long_content = "x" * 500
        data = json.dumps({"type": "assistant", "content": long_content})
        result = parse_log_event(_make_log(data))
        assert result.content_preview is not None
        assert len(result.content_preview) == 200

    def test_session_id_extraction(self) -> None:
        data = json.dumps({"type": "assistant", "session_id": "s-123"})
        result = parse_log_event(_make_log(data))
        assert result.session_id == "s-123"

    def test_session_id_from_sessionId(self) -> None:
        data = json.dumps({"type": "assistant", "sessionId": "s-456"})
        result = parse_log_event(_make_log(data))
        assert result.session_id == "s-456"

    def test_session_id_from_metadata(self) -> None:
        data = json.dumps({"type": "assistant", "metadata": {"session_id": "s-789"}})
        result = parse_log_event(_make_log(data))
        assert result.session_id == "s-789"

    def test_cost_from_nested_result(self) -> None:
        data = json.dumps({"type": "result", "result": {"cost_usd": 0.1}})
        result = parse_log_event(_make_log(data))
        assert result.cost_usd == pytest.approx(0.1)

    def test_content_list_blocks(self) -> None:
        data = json.dumps({
            "type": "assistant",
            "content": [
                {"type": "text", "text": "hello"},
                {"type": "text", "text": "world"},
            ],
        })
        result = parse_log_event(_make_log(data))
        assert result.content_preview == "hello world"

    def test_tool_use_prefers_tool_field(self) -> None:
        data = json.dumps({"type": "tool_use", "tool": "read_file", "subtype": "bash"})
        result = parse_log_event(_make_log(data))
        assert result.subtype == "read_file"
        assert result.raw_subtype == "bash"


class TestParsedLogEventModel:
    def test_model_serialisation(self) -> None:
        event = ParsedLogEvent(
            event_type="assistant",
            subtype="text",
            session_id="s-1",
            cost_usd=0.01,
            content_preview="hi",
            raw_type="assistant",
            raw_subtype="text",
        )
        d = event.model_dump()
        assert d["event_type"] == "assistant"
        assert d["cost_usd"] == 0.01

    def test_model_defaults(self) -> None:
        event = ParsedLogEvent(event_type="unknown")
        assert event.subtype is None
        assert event.session_id is None
        assert event.cost_usd is None
        assert event.content_preview is None
        assert event.raw_type is None
        assert event.raw_subtype is None
