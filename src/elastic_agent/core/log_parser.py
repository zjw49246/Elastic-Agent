"""LOG Event Structured Parsing (T-040).

Parses worker NDJSON ``LogMessage`` events into typed ``ParsedLogEvent``
Pydantic models for downstream analytics and dashboards.
"""

from __future__ import annotations

import json
import logging

from pydantic import BaseModel

from elastic_agent.core.protocols.messages import LogMessage

logger = logging.getLogger(__name__)

# Event types recognised by the parser.
_KNOWN_EVENT_TYPES = frozenset(
    {
        "assistant",
        "user",
        "tool_use",
        "tool_result",
        "system",
        "result",
        "cost",
        "error",
    }
)


class ParsedLogEvent(BaseModel):
    """Structured representation of a single Claude Code NDJSON event."""

    event_type: str
    """One of: assistant, user, tool_use, tool_result, system, result, cost, error, unknown."""

    subtype: str | None = None
    """For ``tool_use`` this is the tool name; otherwise the original subtype."""

    session_id: str | None = None
    """Claude Code session ID if present in the raw event."""

    cost_usd: float | None = None
    """Cost information (typically from ``result`` type events)."""

    content_preview: str | None = None
    """First 200 characters of the message content (if available)."""

    raw_type: str | None = None
    """Original ``"type"`` field from the NDJSON line."""

    raw_subtype: str | None = None
    """Original ``"subtype"`` field from the NDJSON line."""


def parse_log_event(log_msg: LogMessage) -> ParsedLogEvent:
    """Parse a ``LogMessage`` into a ``ParsedLogEvent``.

    If ``log_msg.data`` is valid JSON with a ``"type"`` key the event is
    classified accordingly.  Non-JSON lines or unrecognised types result
    in ``event_type="unknown"``.

    Pre-parsed fields from ``log_msg.parsed`` are used when available
    to avoid redundant JSON parsing.
    """
    # Try to extract structured info from the raw data.
    obj = _try_parse_json(log_msg.data)

    if obj is None:
        # Not JSON — return an unknown event with the raw text as preview.
        return ParsedLogEvent(
            event_type="unknown",
            content_preview=_preview(log_msg.data),
        )

    raw_type = obj.get("type")
    raw_subtype = obj.get("subtype")

    if raw_type is None:
        return ParsedLogEvent(
            event_type="unknown",
            raw_type=None,
            raw_subtype=raw_subtype,
            content_preview=_preview(log_msg.data),
        )

    # Classify event_type.
    event_type = raw_type if raw_type in _KNOWN_EVENT_TYPES else "unknown"

    # Extract content preview.
    content = obj.get("content") or obj.get("message") or obj.get("text")
    if isinstance(content, list):
        # Claude sometimes returns content as a list of blocks.
        parts = []
        for block in content:
            if isinstance(block, dict):
                parts.append(block.get("text", ""))
            elif isinstance(block, str):
                parts.append(block)
        content = " ".join(parts)
    content_preview = _preview(str(content)) if content else None

    # Subtype: for tool_use, prefer the tool name.
    subtype: str | None = None
    if raw_type == "tool_use":
        subtype = obj.get("tool", raw_subtype) or raw_subtype
    else:
        subtype = raw_subtype

    # Session ID — look in several common locations.
    session_id = (
        obj.get("session_id")
        or obj.get("sessionId")
        or _nested_get(obj, "metadata", "session_id")
    )

    # Cost — typically in result events.
    cost_usd = _extract_cost(obj)

    return ParsedLogEvent(
        event_type=event_type,
        subtype=subtype,
        session_id=session_id,
        cost_usd=cost_usd,
        content_preview=content_preview,
        raw_type=raw_type,
        raw_subtype=raw_subtype,
    )


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------


def _try_parse_json(data: str) -> dict | None:
    """Attempt to parse *data* as a JSON object.  Return ``None`` on failure."""
    try:
        obj = json.loads(data)
        if isinstance(obj, dict):
            return obj
    except (json.JSONDecodeError, TypeError):
        pass
    return None


def _preview(text: str, max_len: int = 200) -> str:
    """Return the first *max_len* characters of *text*."""
    return text[:max_len]


def _nested_get(obj: dict, *keys: str):  # noqa: ANN202
    """Traverse nested dicts by *keys*, returning ``None`` if any key is missing."""
    current = obj
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _extract_cost(obj: dict) -> float | None:
    """Pull cost_usd from a parsed JSON object, trying several field names."""
    for key in ("cost_usd", "costUsd", "cost"):
        val = obj.get(key)
        if val is not None:
            try:
                return float(val)
            except (TypeError, ValueError):
                pass
    # Also check nested: result.cost_usd
    result = obj.get("result")
    if isinstance(result, dict):
        for key in ("cost_usd", "costUsd", "cost"):
            val = result.get(key)
            if val is not None:
                try:
                    return float(val)
                except (TypeError, ValueError):
                    pass
    return None
