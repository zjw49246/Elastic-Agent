"""Manager Structured Operation Log (T-039).

Writes JSON-Lines entries for all Manager-level critical operations,
with daily rotation and configurable retention.
"""

from __future__ import annotations

import json
import logging
import os
import threading
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import IO, Any

logger = logging.getLogger(__name__)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _today_utc() -> date:
    return _utcnow().date()


# Valid categories for operations log entries.
VALID_CATEGORIES = frozenset({"node", "task", "credential", "system"})


class OperationsLogger:
    """Thread-safe JSON-Lines logger with daily rotation and retention.

    Parameters
    ----------
    log_dir:
        Directory where log files are stored.
        Defaults to ``~/.elastic-agent``.
    base_name:
        Base file name (without date suffix).  The *current* day's file
        is ``{base_name}.log``; rotated files are named
        ``{base_name}-YYYY-MM-DD.log``.
    retention_days:
        Number of days of rotated logs to keep.  Files older than this
        are deleted during rotation.
    """

    def __init__(
        self,
        log_dir: str | Path | None = None,
        base_name: str = "operations",
        retention_days: int = 30,
    ) -> None:
        if log_dir is None:
            log_dir = Path.home() / ".elastic-agent"
        self._log_dir = Path(log_dir)
        self._base_name = base_name
        self._retention_days = retention_days

        self._lock = threading.Lock()
        self._fh: IO[str] | None = None
        self._current_date: date | None = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def log(
        self,
        operation: str,
        category: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Append one structured log entry.

        Parameters
        ----------
        operation:
            Short verb-noun description, e.g. ``"scale_out"`` or ``"task_dispatch"``.
        category:
            One of ``"node"``, ``"task"``, ``"credential"``, ``"system"``.
        details:
            Arbitrary key-value context.
        """
        entry = {
            "timestamp": _utcnow().isoformat(),
            "operation": operation,
            "category": category,
            "details": details or {},
        }
        line = json.dumps(entry, ensure_ascii=False) + "\n"

        with self._lock:
            self._maybe_rotate()
            if self._fh is None:
                self._open_current()
            try:
                self._fh.write(line)  # type: ignore[union-attr]
                self._fh.flush()  # type: ignore[union-attr]
            except IOError:
                logger.exception("Failed to write operations log entry")

    def close(self) -> None:
        """Flush and close the current log file."""
        with self._lock:
            if self._fh is not None:
                try:
                    self._fh.close()
                except IOError:
                    logger.exception("Error closing operations log")
                finally:
                    self._fh = None
                    self._current_date = None

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _current_log_path(self) -> Path:
        """Path for the *current* (active) log file."""
        return self._log_dir / f"{self._base_name}.log"

    def _dated_log_path(self, d: date) -> Path:
        """Path for a rotated log file dated *d*."""
        return self._log_dir / f"{self._base_name}-{d.isoformat()}.log"

    def _open_current(self) -> None:
        """Open the current log file for appending.

        Must be called while holding ``self._lock``.
        """
        self._log_dir.mkdir(parents=True, exist_ok=True)
        self._fh = open(self._current_log_path(), "a", encoding="utf-8")  # noqa: SIM115
        self._current_date = _today_utc()

    def _maybe_rotate(self) -> None:
        """If the UTC date has changed since last write, rotate.

        Must be called while holding ``self._lock``.
        """
        today = _today_utc()
        if self._current_date is not None and self._current_date != today:
            self._rotate(self._current_date, today)

    def _rotate(self, old_date: date, new_date: date) -> None:  # noqa: ARG002
        """Close old file, rename to dated file, purge old rotated files.

        Must be called while holding ``self._lock``.
        """
        # Close the current file handle.
        if self._fh is not None:
            try:
                self._fh.close()
            except IOError:
                pass
            self._fh = None

        # Rename current → dated.
        current = self._current_log_path()
        if current.exists():
            target = self._dated_log_path(old_date)
            try:
                os.replace(str(current), str(target))
            except OSError:
                logger.exception("Failed to rotate operations log")

        self._current_date = None  # will be set when _open_current is called

        # Purge expired rotated files.
        self._purge_old_files()

    def _purge_old_files(self) -> None:
        """Delete rotated log files older than *retention_days*.

        Must be called while holding ``self._lock``.
        """
        cutoff = _today_utc() - timedelta(days=self._retention_days)
        pattern = f"{self._base_name}-*.log"
        for path in self._log_dir.glob(pattern):
            # Extract date from filename: operations-YYYY-MM-DD.log
            stem = path.stem  # e.g. "operations-2024-01-15"
            date_str = stem[len(self._base_name) + 1 :]  # "2024-01-15"
            try:
                file_date = date.fromisoformat(date_str)
            except ValueError:
                continue
            if file_date < cutoff:
                try:
                    path.unlink()
                except OSError:
                    logger.exception("Failed to delete old log %s", path)
