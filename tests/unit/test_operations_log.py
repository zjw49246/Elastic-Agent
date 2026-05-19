"""Tests for OperationsLogger (T-039)."""

from __future__ import annotations

import json
import threading
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import patch

import pytest

from elastic_agent.core.operations_log import OperationsLogger


@pytest.fixture
def log_dir(tmp_path: Path) -> Path:
    return tmp_path / "ops"


@pytest.fixture
def ops_logger(log_dir: Path) -> OperationsLogger:
    return OperationsLogger(log_dir=log_dir, retention_days=7)


class TestOperationsLoggerBasic:
    def test_log_creates_file(self, ops_logger: OperationsLogger, log_dir: Path) -> None:
        ops_logger.log("scale_out", "node", {"count": 2})
        path = log_dir / "operations.log"
        assert path.exists()

    def test_log_entry_format(self, ops_logger: OperationsLogger, log_dir: Path) -> None:
        ops_logger.log("task_dispatch", "task", {"task_id": "t1"})
        path = log_dir / "operations.log"
        entry = json.loads(path.read_text().strip())
        assert entry["operation"] == "task_dispatch"
        assert entry["category"] == "task"
        assert entry["details"] == {"task_id": "t1"}
        assert "timestamp" in entry

    def test_log_default_details_empty_dict(self, ops_logger: OperationsLogger, log_dir: Path) -> None:
        ops_logger.log("system_start", "system")
        path = log_dir / "operations.log"
        entry = json.loads(path.read_text().strip())
        assert entry["details"] == {}

    def test_multiple_entries_appended(self, ops_logger: OperationsLogger, log_dir: Path) -> None:
        ops_logger.log("op1", "node")
        ops_logger.log("op2", "task")
        ops_logger.log("op3", "credential")
        path = log_dir / "operations.log"
        lines = path.read_text().strip().splitlines()
        assert len(lines) == 3
        assert json.loads(lines[0])["operation"] == "op1"
        assert json.loads(lines[1])["operation"] == "op2"
        assert json.loads(lines[2])["operation"] == "op3"

    def test_close_and_reopen(self, ops_logger: OperationsLogger, log_dir: Path) -> None:
        ops_logger.log("before_close", "system")
        ops_logger.close()
        ops_logger.log("after_close", "system")
        path = log_dir / "operations.log"
        lines = path.read_text().strip().splitlines()
        assert len(lines) == 2


class TestOperationsLoggerRotation:
    def test_rotation_renames_old_file(self, log_dir: Path) -> None:
        ol = OperationsLogger(log_dir=log_dir, retention_days=30)

        # Write an entry for "yesterday".
        fake_yesterday = date(2025, 6, 10)
        fake_today = date(2025, 6, 11)

        with patch("elastic_agent.core.operations_log._today_utc", return_value=fake_yesterday):
            ol.log("old_entry", "system")

        # Now simulate date change.
        with patch("elastic_agent.core.operations_log._today_utc", return_value=fake_today):
            ol.log("new_entry", "system")

        # The old file should be rotated.
        rotated = log_dir / "operations-2025-06-10.log"
        assert rotated.exists()
        old_entry = json.loads(rotated.read_text().strip())
        assert old_entry["operation"] == "old_entry"

        # Current file should only have the new entry.
        current = log_dir / "operations.log"
        new_entry = json.loads(current.read_text().strip())
        assert new_entry["operation"] == "new_entry"

        ol.close()

    def test_retention_purges_old_files(self, log_dir: Path) -> None:
        log_dir.mkdir(parents=True, exist_ok=True)
        ol = OperationsLogger(log_dir=log_dir, retention_days=3)

        # Create some old rotated files.
        today = date(2025, 6, 15)
        for days_ago in range(1, 10):
            d = today - timedelta(days=days_ago)
            (log_dir / f"operations-{d.isoformat()}.log").write_text("{}\n")

        # Trigger rotation with a fake date change.
        with patch("elastic_agent.core.operations_log._today_utc", return_value=today - timedelta(days=1)):
            ol.log("yesterday_entry", "system")

        with patch("elastic_agent.core.operations_log._today_utc", return_value=today):
            ol.log("today_entry", "system")

        # Files older than 3 days from today should be gone.
        remaining = list(log_dir.glob("operations-*.log"))
        remaining_dates = []
        for p in remaining:
            ds = p.stem.replace("operations-", "")
            remaining_dates.append(date.fromisoformat(ds))

        cutoff = today - timedelta(days=3)
        for d in remaining_dates:
            assert d >= cutoff, f"File dated {d} should have been purged (cutoff={cutoff})"

        ol.close()

    def test_no_rotation_same_day(self, ops_logger: OperationsLogger, log_dir: Path) -> None:
        ops_logger.log("entry1", "system")
        ops_logger.log("entry2", "system")
        # No rotated files should exist.
        rotated = list(log_dir.glob("operations-*.log"))
        assert len(rotated) == 0


class TestOperationsLoggerThreadSafety:
    def test_concurrent_log_writes(self, log_dir: Path) -> None:
        ol = OperationsLogger(log_dir=log_dir)
        n_threads = 4
        n_entries = 30
        errors: list[Exception] = []

        def writer(tid: int) -> None:
            try:
                for i in range(n_entries):
                    ol.log(f"op-{tid}-{i}", "system", {"tid": tid, "i": i})
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=writer, args=(t,)) for t in range(n_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        path = log_dir / "operations.log"
        lines = path.read_text().strip().splitlines()
        assert len(lines) == n_threads * n_entries
        ol.close()


class TestOperationsLoggerDefaults:
    def test_default_log_dir_is_home(self) -> None:
        ol = OperationsLogger()
        assert ol._log_dir == Path.home() / ".elastic-agent"

    def test_custom_base_name(self, log_dir: Path) -> None:
        ol = OperationsLogger(log_dir=log_dir, base_name="audit")
        ol.log("test", "system")
        assert (log_dir / "audit.log").exists()
        ol.close()
