"""T-121: Worker log persistence — file integrity on normal exit and crash exit."""

from __future__ import annotations

import json
import os
import threading
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from elastic_agent.worker.process_logger import ProcessLogger


@pytest.fixture
def log_dir(tmp_path: Path) -> Path:
    return tmp_path / "logs"


@pytest.fixture
def plogger(log_dir: Path) -> ProcessLogger:
    return ProcessLogger(log_dir=log_dir)


# ---------------------------------------------------------------------------
# Normal exit: all lines flushed
# ---------------------------------------------------------------------------


class TestNormalExitIntegrity:

    def test_all_lines_present_after_close(self, plogger: ProcessLogger, log_dir: Path) -> None:
        for i in range(100):
            plogger.write_line("task-1", "stdout", f"line-{i}")
        plogger.close("task-1")

        path = log_dir / "task-1.ndjson"
        lines = path.read_text().strip().splitlines()
        assert len(lines) == 100
        for i, line in enumerate(lines):
            entry = json.loads(line)
            assert entry["data"] == f"line-{i}"

    def test_each_line_is_valid_json(self, plogger: ProcessLogger, log_dir: Path) -> None:
        plogger.write_line("task-1", "stdout", "hello", parsed={"type": "assistant"})
        plogger.write_line("task-1", "stderr", "err msg")
        plogger.close("task-1")

        path = log_dir / "task-1.ndjson"
        for line in path.read_text().strip().splitlines():
            entry = json.loads(line)
            assert "task_id" in entry
            assert "stream" in entry
            assert "data" in entry
            assert "timestamp" in entry

    def test_close_all_flushes_all_tasks(self, plogger: ProcessLogger, log_dir: Path) -> None:
        for tid in ["t-a", "t-b", "t-c"]:
            plogger.write_line(tid, "stdout", f"data-{tid}")
        plogger.close_all()

        for tid in ["t-a", "t-b", "t-c"]:
            path = log_dir / f"{tid}.ndjson"
            assert path.exists()
            entry = json.loads(path.read_text().strip())
            assert entry["data"] == f"data-{tid}"

    def test_file_ends_with_newline(self, plogger: ProcessLogger, log_dir: Path) -> None:
        plogger.write_line("task-1", "stdout", "data")
        plogger.close("task-1")
        content = (log_dir / "task-1.ndjson").read_text()
        assert content.endswith("\n")

    def test_immediate_flush(self, plogger: ProcessLogger, log_dir: Path) -> None:
        plogger.write_line("task-1", "stdout", "flushed")
        path = log_dir / "task-1.ndjson"
        content = path.read_text()
        assert "flushed" in content


# ---------------------------------------------------------------------------
# Crash exit simulation: partial write recovery
# ---------------------------------------------------------------------------


class TestCrashExitIntegrity:

    def test_reopen_after_crash_appends(self, log_dir: Path) -> None:
        pl1 = ProcessLogger(log_dir=log_dir)
        pl1.write_line("task-1", "stdout", "before-crash")

        pl2 = ProcessLogger(log_dir=log_dir)
        pl2.write_line("task-1", "stdout", "after-crash")
        pl2.close("task-1")

        path = log_dir / "task-1.ndjson"
        lines = path.read_text().strip().splitlines()
        assert len(lines) == 2
        assert json.loads(lines[0])["data"] == "before-crash"
        assert json.loads(lines[1])["data"] == "after-crash"

    def test_truncated_last_line_recoverable(self, log_dir: Path) -> None:
        log_dir.mkdir(parents=True, exist_ok=True)
        path = log_dir / "task-trunc.ndjson"
        good_line = json.dumps({"task_id": "task-trunc", "stream": "stdout", "data": "good", "timestamp": "t", "parsed": None})
        path.write_text(good_line + "\n" + '{"incomplete":')

        valid = []
        for line in path.read_text().strip().splitlines():
            try:
                valid.append(json.loads(line))
            except json.JSONDecodeError:
                pass
        assert len(valid) == 1
        assert valid[0]["data"] == "good"

    def test_file_survives_process_logger_gc(self, log_dir: Path) -> None:
        pl = ProcessLogger(log_dir=log_dir)
        pl.write_line("task-gc", "stdout", "before-gc")
        del pl

        path = log_dir / "task-gc.ndjson"
        assert path.exists()
        entry = json.loads(path.read_text().strip())
        assert entry["data"] == "before-gc"


# ---------------------------------------------------------------------------
# Concurrent write integrity
# ---------------------------------------------------------------------------


class TestConcurrentWriteIntegrity:

    def test_concurrent_writes_no_data_loss(self, plogger: ProcessLogger, log_dir: Path) -> None:
        n_threads = 8
        n_lines = 100
        errors: list[Exception] = []

        def writer(tid: int) -> None:
            try:
                for i in range(n_lines):
                    plogger.write_line("task-concurrent", "stdout", f"t{tid}-{i}")
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=writer, args=(t,)) for t in range(n_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        path = log_dir / "task-concurrent.ndjson"
        lines = path.read_text().strip().splitlines()
        assert len(lines) == n_threads * n_lines

        for line in lines:
            entry = json.loads(line)
            assert entry["task_id"] == "task-concurrent"

    def test_concurrent_write_and_close(self, log_dir: Path) -> None:
        pl = ProcessLogger(log_dir=log_dir)
        errors: list[Exception] = []

        def writer() -> None:
            try:
                for i in range(50):
                    pl.write_line("task-wc", "stdout", f"line-{i}")
            except Exception as e:
                errors.append(e)

        def closer() -> None:
            try:
                import time
                time.sleep(0.01)
                pl.close("task-wc")
            except Exception as e:
                errors.append(e)

        t1 = threading.Thread(target=writer)
        t2 = threading.Thread(target=closer)
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        assert not errors


# ---------------------------------------------------------------------------
# Multiple task isolation
# ---------------------------------------------------------------------------


class TestMultiTaskIsolation:

    def test_separate_files_per_task(self, plogger: ProcessLogger, log_dir: Path) -> None:
        plogger.write_line("alpha", "stdout", "data-alpha")
        plogger.write_line("beta", "stdout", "data-beta")
        plogger.close_all()

        alpha = json.loads((log_dir / "alpha.ndjson").read_text().strip())
        beta = json.loads((log_dir / "beta.ndjson").read_text().strip())
        assert alpha["data"] == "data-alpha"
        assert beta["data"] == "data-beta"

    def test_close_one_doesnt_affect_other(self, plogger: ProcessLogger, log_dir: Path) -> None:
        plogger.write_line("t1", "stdout", "a")
        plogger.write_line("t2", "stdout", "b")
        plogger.close("t1")
        plogger.write_line("t2", "stdout", "c")
        plogger.close("t2")

        lines = (log_dir / "t2.ndjson").read_text().strip().splitlines()
        assert len(lines) == 2


# ---------------------------------------------------------------------------
# Special characters and encoding
# ---------------------------------------------------------------------------


class TestSpecialCharacters:

    def test_unicode_content(self, plogger: ProcessLogger, log_dir: Path) -> None:
        plogger.write_line("task-uni", "stdout", "你好世界 🎵 emoji")
        plogger.close("task-uni")

        entry = json.loads((log_dir / "task-uni.ndjson").read_text().strip())
        assert "你好世界" in entry["data"]

    def test_newline_in_data(self, plogger: ProcessLogger, log_dir: Path) -> None:
        plogger.write_line("task-nl", "stdout", "line1\nline2")
        plogger.close("task-nl")

        lines = (log_dir / "task-nl.ndjson").read_text().strip().splitlines()
        assert len(lines) == 1
        entry = json.loads(lines[0])
        assert entry["data"] == "line1\nline2"

    def test_empty_data(self, plogger: ProcessLogger, log_dir: Path) -> None:
        plogger.write_line("task-empty", "stdout", "")
        plogger.close("task-empty")
        entry = json.loads((log_dir / "task-empty.ndjson").read_text().strip())
        assert entry["data"] == ""


# ---------------------------------------------------------------------------
# Directory creation
# ---------------------------------------------------------------------------


class TestDirectoryCreation:

    def test_nested_directory_created(self, tmp_path: Path) -> None:
        deep = tmp_path / "a" / "b" / "c" / "d"
        pl = ProcessLogger(log_dir=deep)
        pl.write_line("task-deep", "stdout", "deep data")
        pl.close_all()
        assert (deep / "task-deep.ndjson").exists()
