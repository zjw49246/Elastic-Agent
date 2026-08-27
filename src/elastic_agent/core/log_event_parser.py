"""LOG event structured parsing — Manager-side consumption of parsed LOG events.

T-040: Processes LOG events from Workers, extracts session_id and cost_usd
from 'result' type events, and provides trace buffer filtering by parsed.type.
"""

from __future__ import annotations

import logging
import math
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

CLAUDE_CODE_TYPES = frozenset({"system", "assistant", "user", "tool_use", "tool_result", "result"})
MAX_SESSION_ID_LENGTH = 1024
MAX_ACCOUNTED_COST_USD = 1_000_000_000.0


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class TaskSession:
    """Extracted session metadata from a task's LOG events."""
    task_id: str
    session_id: str | None = None
    total_cost_usd: float = 0.0
    last_updated: datetime = field(default_factory=_utcnow)


class LogEventParser:
    """Manager-side processor for LOG events received from Workers.

    Responsibilities:
    - Maintain per-task trace buffers with configurable capacity
    - Extract session_id and cost_usd from 'result' type events
    - Support filtering trace buffer by parsed.type
    - Aggregate costs per task and per worker

    Usage:
        parser = LogEventParser(buffer_size=5000)

        # Called when Manager receives a LOG event from Worker
        parser.process_log_event("worker-1", log_data)

        # Query filtered logs
        logs = parser.get_task_logs("task-1", types=["assistant", "result"])

        # Get session info
        session = parser.get_task_session("task-1")
    """

    def __init__(self, buffer_size: int = 5000) -> None:
        self._buffer_size = buffer_size
        self._task_buffers: dict[str, deque[dict[str, Any]]] = defaultdict(
            lambda: deque(maxlen=self._buffer_size)
        )
        self._task_sessions: dict[str, TaskSession] = {}
        self._task_prompts: dict[str, dict[str, Any]] = {}
        self._worker_costs: dict[str, float] = defaultdict(float)

    def process_log_event(
        self,
        worker_id: str,
        data: dict[str, Any],
    ) -> dict[str, Any] | None:
        """Process an incoming LOG event and return extracted metadata if any.

        Returns a dict with extracted fields if a 'result' event was found, else None.
        """
        task_id = data.get("task_id")
        if not task_id:
            return None

        entry = {
            "task_id": task_id,
            "worker_id": worker_id,
            "stream": data.get("stream"),
            "data": data.get("data"),
            "timestamp": data.get("timestamp", _utcnow().isoformat()),
            "parsed": data.get("parsed"),
        }
        self._task_buffers[task_id].append(entry)

        parsed = data.get("parsed")
        if not isinstance(parsed, dict):
            return None

        event_type = parsed.get("type")
        if event_type not in CLAUDE_CODE_TYPES:
            return None

        if event_type == "result":
            return self._handle_result_event(task_id, worker_id, parsed)

        return None

    def _handle_result_event(
        self,
        task_id: str,
        worker_id: str,
        parsed: dict[str, Any],
    ) -> dict[str, Any]:
        raw_session_id = parsed.get("session_id")
        raw_cost_usd = parsed.get("cost_usd")

        if task_id not in self._task_sessions:
            self._task_sessions[task_id] = TaskSession(task_id=task_id)

        session = self._task_sessions[task_id]
        if (
            isinstance(raw_session_id, str)
            and 0 < len(raw_session_id) <= MAX_SESSION_ID_LENGTH
            and raw_session_id.isprintable()
        ):
            session.session_id = raw_session_id

        accepted_cost: float | None = None
        if raw_cost_usd is not None and not isinstance(raw_cost_usd, bool):
            try:
                candidate = float(raw_cost_usd)
                task_total = session.total_cost_usd + candidate
                worker_total = self._worker_costs[worker_id] + candidate
                if (
                    math.isfinite(candidate)
                    and 0.0 <= candidate <= MAX_ACCOUNTED_COST_USD
                    and math.isfinite(task_total)
                    and task_total <= MAX_ACCOUNTED_COST_USD
                    and math.isfinite(worker_total)
                    and worker_total <= MAX_ACCOUNTED_COST_USD
                ):
                    accepted_cost = candidate
                    session.total_cost_usd = task_total
                    self._worker_costs[worker_id] = worker_total
            except (OverflowError, ValueError, TypeError):
                pass
        session.last_updated = _utcnow()

        return {
            "session_id": session.session_id,
            "cost_usd": accepted_cost,
            "total_cost_usd": session.total_cost_usd,
        }

    def get_task_logs(
        self,
        task_id: str,
        *,
        types: list[str] | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        """Get buffered log entries for a task, optionally filtered by parsed.type."""
        buf = self._task_buffers.get(task_id)
        if buf is None:
            return []

        if types is None:
            entries = list(buf)
        else:
            type_set = set(types)
            entries = [
                e for e in buf
                if isinstance(e.get("parsed"), dict) and e["parsed"].get("type") in type_set
            ]

        if limit is not None:
            entries = entries[-limit:]

        return entries

    def get_task_session(self, task_id: str) -> TaskSession | None:
        """Get extracted session metadata for a task."""
        return self._task_sessions.get(task_id)

    def register_task_prompt(
        self,
        task_id: str,
        metadata: dict[str, Any],
    ) -> None:
        """Retain prompt metadata outside the bounded event deque."""

        self._task_prompts[task_id] = metadata

    def get_task_prompt(self, task_id: str) -> dict[str, Any] | None:
        return self._task_prompts.get(task_id)

    def get_task_cost(self, task_id: str) -> float:
        """Get aggregated cost for a task."""
        session = self._task_sessions.get(task_id)
        return session.total_cost_usd if session else 0.0

    def get_worker_cost(self, worker_id: str) -> float:
        """Get aggregated cost for a worker."""
        return self._worker_costs.get(worker_id, 0.0)

    def release_task(self, task_id: str) -> None:
        """Release buffer and session data for a completed task."""
        self._task_buffers.pop(task_id, None)
        self._task_sessions.pop(task_id, None)
        self._task_prompts.pop(task_id, None)

    def buffer_size(self, task_id: str) -> int:
        """Return current buffer size for a task."""
        buf = self._task_buffers.get(task_id)
        return len(buf) if buf else 0

    @property
    def active_tasks(self) -> list[str]:
        """Return list of task IDs with active buffers."""
        return list(self._task_buffers.keys())
