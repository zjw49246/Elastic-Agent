"""Tests for ProcessLogger (T-038)."""

from __future__ import annotations

import json
import threading
from pathlib import Path

import pytest

from elastic_agent.worker.process_logger import ProcessLogger


@pytest.fixture
def log_dir(tmp_path: Path) -> Path:
    return tmp_path / "logs"


@pytest.fixture
def plogger(log_dir: Path) -> ProcessLogger:
    return ProcessLogger(log_dir=log_dir)


class TestProcessLoggerBasic:
    def test_write_creates_file(self, plogger: ProcessLogger, log_dir: Path) -> None:
        plogger.write_line("task-1", "stdout", "hello world")
        path = log_dir / "task-1.ndjson"
        assert path.exists()

    def test_write_ndjson_format(self, plogger: ProcessLogger, log_dir: Path) -> None:
        plogger.write_line("task-1", "stdout", "line one", parsed={"type": "assistant"})
        path = log_dir / "task-1.ndjson"
        lines = path.read_text().strip().splitlines()
        assert len(lines) == 1
        entry = json.loads(lines[0])
        assert entry["task_id"] == "task-1"
        assert entry["stream"] == "stdout"
        assert entry["data"] == "line one"
        assert entry["parsed"] == {"type": "assistant"}
        assert "timestamp" in entry

    def test_write_stderr_stream(self, plogger: ProcessLogger, log_dir: Path) -> None:
        plogger.write_line("task-2", "stderr", "error message")
        path = log_dir / "task-2.ndjson"
        entry = json.loads(path.read_text().strip())
        assert entry["stream"] == "stderr"
        assert entry["data"] == "error message"

    def test_write_null_parsed(self, plogger: ProcessLogger, log_dir: Path) -> None:
        plogger.write_line("task-3", "stdout", "plain text")
        path = log_dir / "task-3.ndjson"
        entry = json.loads(path.read_text().strip())
        assert entry["parsed"] is None

    def test_multiple_writes_append(self, plogger: ProcessLogger, log_dir: Path) -> None:
        plogger.write_line("task-4", "stdout", "first")
        plogger.write_line("task-4", "stdout", "second")
        plogger.write_line("task-4", "stderr", "err")
        path = log_dir / "task-4.ndjson"
        lines = path.read_text().strip().splitlines()
        assert len(lines) == 3
        assert json.loads(lines[0])["data"] == "first"
        assert json.loads(lines[1])["data"] == "second"
        assert json.loads(lines[2])["stream"] == "stderr"

    def test_multiple_tasks_separate_files(self, plogger: ProcessLogger, log_dir: Path) -> None:
        plogger.write_line("task-a", "stdout", "aaa")
        plogger.write_line("task-b", "stdout", "bbb")
        assert (log_dir / "task-a.ndjson").exists()
        assert (log_dir / "task-b.ndjson").exists()
        a_entry = json.loads((log_dir / "task-a.ndjson").read_text().strip())
        b_entry = json.loads((log_dir / "task-b.ndjson").read_text().strip())
        assert a_entry["data"] == "aaa"
        assert b_entry["data"] == "bbb"


class TestProcessLoggerClose:
    def test_close_task(self, plogger: ProcessLogger, log_dir: Path) -> None:
        plogger.write_line("task-c", "stdout", "data")
        plogger.close("task-c")
        assert "task-c" not in plogger._files

    def test_close_nonexistent_task_noop(self, plogger: ProcessLogger) -> None:
        # Should not raise.
        plogger.close("nonexistent")

    def test_close_all(self, plogger: ProcessLogger) -> None:
        plogger.write_line("t1", "stdout", "a")
        plogger.write_line("t2", "stdout", "b")
        plogger.close_all()
        assert len(plogger._files) == 0

    def test_write_after_close_reopens(self, plogger: ProcessLogger, log_dir: Path) -> None:
        plogger.write_line("task-r", "stdout", "before")
        plogger.close("task-r")
        plogger.write_line("task-r", "stdout", "after")
        path = log_dir / "task-r.ndjson"
        lines = path.read_text().strip().splitlines()
        assert len(lines) == 2


class TestProcessLoggerThreadSafety:
    def test_concurrent_writes(self, plogger: ProcessLogger, log_dir: Path) -> None:
        """Write from multiple threads and verify all lines are present."""
        n_threads = 4
        n_lines = 50
        errors: list[Exception] = []

        def writer(thread_id: int) -> None:
            try:
                for i in range(n_lines):
                    plogger.write_line(
                        "task-concurrent",
                        "stdout",
                        f"thread-{thread_id}-line-{i}",
                    )
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=writer, args=(tid,)) for tid in range(n_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        path = log_dir / "task-concurrent.ndjson"
        lines = path.read_text().strip().splitlines()
        assert len(lines) == n_threads * n_lines


class TestProcessLoggerCreatesDirectories:
    def test_nested_log_dir(self, tmp_path: Path) -> None:
        deep_dir = tmp_path / "a" / "b" / "c"
        pl = ProcessLogger(log_dir=deep_dir)
        pl.write_line("task-deep", "stdout", "deep")
        assert (deep_dir / "task-deep.ndjson").exists()
        pl.close_all()
