"""Worker Process Log Persistence (T-038).

Provides a dedicated, thread-safe NDJSON log writer for worker subprocess
stdout/stderr output.  Each task gets its own ``{task_id}.ndjson`` file
under a configurable *log_dir*.
"""

from __future__ import annotations

import json
import logging
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import IO, Any

logger = logging.getLogger(__name__)


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class ProcessLogger:
    """Thread-safe NDJSON log writer for worker processes.

    Usage::

        pl = ProcessLogger(log_dir=Path("/var/log/worker"))
        pl.write_line("task-1", "stdout", "hello world", parsed={"type": "assistant"})
        pl.close("task-1")
        # or on shutdown:
        pl.close_all()
    """

    def __init__(self, log_dir: Path) -> None:
        self._log_dir = log_dir
        self._lock = threading.Lock()
        self._files: dict[str, IO[str]] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def write_line(
        self,
        task_id: str,
        stream: str,
        data: str,
        parsed: dict[str, Any] | None = None,
    ) -> None:
        """Append one NDJSON line for *task_id*.

        Opens the log file on first write.  Each line is flushed
        immediately so no data is lost on crash.
        """
        entry = {
            "task_id": task_id,
            "stream": stream,
            "data": data,
            "timestamp": _utcnow_iso(),
            "parsed": parsed,
        }
        line = json.dumps(entry, ensure_ascii=False) + "\n"

        with self._lock:
            fh = self._files.get(task_id)
            if fh is None:
                fh = self._open_file(task_id)
            try:
                fh.write(line)
                fh.flush()
            except IOError:
                logger.exception("Failed to write log for task %s", task_id)

    def close(self, task_id: str) -> None:
        """Close the log file for *task_id* (no-op if not open)."""
        with self._lock:
            fh = self._files.pop(task_id, None)
            if fh is not None:
                try:
                    fh.close()
                except IOError:
                    logger.exception("Error closing log for task %s", task_id)

    def close_all(self) -> None:
        """Close all open log files (called on shutdown)."""
        with self._lock:
            for task_id, fh in self._files.items():
                try:
                    fh.close()
                except IOError:
                    logger.exception("Error closing log for task %s", task_id)
            self._files.clear()

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _open_file(self, task_id: str) -> IO[str]:
        """Open (or create) the NDJSON log file for *task_id*.

        Must be called while holding ``self._lock``.
        """
        path = self._log_dir / f"{task_id}.ndjson"
        path.parent.mkdir(parents=True, exist_ok=True)
        fh = open(path, "a", encoding="utf-8")  # noqa: SIM115
        self._files[task_id] = fh
        return fh
