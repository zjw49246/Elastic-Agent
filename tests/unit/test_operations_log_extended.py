"""T-125: Manager operation log — format, rotation, category recording."""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

from elastic_agent.core.operations_logger import OperationsLogger


@pytest.fixture
def ops_logger(tmp_path):
    path = tmp_path / "operations.log"
    lg = OperationsLogger(log_path=str(path), retention_days=30, log_level="DEBUG")
    yield lg
    lg.close()


# ---------------------------------------------------------------------------
# Log entry format
# ---------------------------------------------------------------------------


class TestLogEntryFormat:

    def test_entry_has_required_fields(self, ops_logger):
        entry = ops_logger.log("component", "action")
        assert "ts" in entry
        assert "level" in entry
        assert "component" in entry
        assert "action" in entry
        assert "node_id" in entry
        assert "details" in entry
        assert "trace_id" in entry

    def test_default_level_is_info(self, ops_logger):
        entry = ops_logger.log("test", "act")
        assert entry["level"] == "INFO"

    def test_timestamp_is_iso_format(self, ops_logger):
        entry = ops_logger.log("test", "act")
        ts = entry["ts"]
        datetime.fromisoformat(ts)

    def test_trace_id_auto_generated(self, ops_logger):
        entry = ops_logger.log("test", "act")
        assert entry["trace_id"].startswith("op-")

    def test_custom_trace_id(self, ops_logger):
        entry = ops_logger.log("test", "act", trace_id="my-trace-123")
        assert entry["trace_id"] == "my-trace-123"

    def test_node_id_optional(self, ops_logger):
        entry = ops_logger.log("test", "act")
        assert entry["node_id"] is None
        entry2 = ops_logger.log("test", "act", node_id="node-1")
        assert entry2["node_id"] == "node-1"

    def test_details_defaults_to_empty_dict(self, ops_logger):
        entry = ops_logger.log("test", "act")
        assert entry["details"] == {}

    def test_json_lines_format(self, ops_logger, tmp_path):
        ops_logger.log("a", "b", details={"key": "val1"})
        ops_logger.log("c", "d", details={"key": "val2"})

        log_path = tmp_path / "operations.log"
        lines = log_path.read_text().strip().splitlines()
        assert len(lines) == 2
        for line in lines:
            parsed = json.loads(line)
            assert isinstance(parsed, dict)
            assert "component" in parsed

    def test_each_entry_flushed_immediately(self, ops_logger, tmp_path):
        ops_logger.log("test", "action")
        log_path = tmp_path / "operations.log"
        assert log_path.exists()
        content = log_path.read_text()
        assert len(content.strip()) > 0


# ---------------------------------------------------------------------------
# Level filtering
# ---------------------------------------------------------------------------


class TestLevelFiltering:

    def test_debug_level_logs_everything(self, tmp_path):
        lg = OperationsLogger(str(tmp_path / "ops.log"), log_level="DEBUG")
        for level in ["DEBUG", "INFO", "WARNING", "ERROR"]:
            assert lg.log("t", "a", level=level) is not None
        lg.close()

    def test_info_filters_debug(self, tmp_path):
        lg = OperationsLogger(str(tmp_path / "ops.log"), log_level="INFO")
        assert lg.log("t", "a", level="DEBUG") is None
        assert lg.log("t", "a", level="INFO") is not None
        assert lg.log("t", "a", level="WARNING") is not None
        lg.close()

    def test_warning_filters_info_and_debug(self, tmp_path):
        lg = OperationsLogger(str(tmp_path / "ops.log"), log_level="WARNING")
        assert lg.log("t", "a", level="DEBUG") is None
        assert lg.log("t", "a", level="INFO") is None
        assert lg.log("t", "a", level="WARNING") is not None
        assert lg.log("t", "a", level="ERROR") is not None
        lg.close()

    def test_error_level_only(self, tmp_path):
        lg = OperationsLogger(str(tmp_path / "ops.log"), log_level="ERROR")
        assert lg.log("t", "a", level="WARNING") is None
        assert lg.log("t", "a", level="ERROR") is not None
        lg.close()

    def test_case_insensitive_level(self, tmp_path):
        lg = OperationsLogger(str(tmp_path / "ops.log"), log_level="info")
        assert lg.log("t", "a", level="info") is not None
        assert lg.log("t", "a", level="INFO") is not None
        lg.close()


# ---------------------------------------------------------------------------
# Category recording (convenience methods)
# ---------------------------------------------------------------------------


class TestCategoryRecording:

    def test_scaling_category(self, ops_logger):
        ops_logger.log_scale_out_started(3, "api_request")
        ops_logger.log_scale_out_completed(["n1", "n2"], 5000)
        ops_logger.log_scale_in_started("n1", "manual")
        ops_logger.log_instance_created("n1", "ecs.c6.large", "cn-hangzhou")
        ops_logger.log_instance_terminated("n1", "scale_in")

        entries = ops_logger.read_entries()
        assert all(e["component"] == "scaling" for e in entries)
        actions = [e["action"] for e in entries]
        assert actions == [
            "scale_out_started", "scale_out_completed",
            "scale_in_started", "instance_created", "instance_terminated",
        ]

    def test_bootstrap_category(self, ops_logger):
        ops_logger.log_bootstrap_started("n1", ["step-a", "step-b"])
        ops_logger.log_step_started("n1", "step-a")
        ops_logger.log_step_completed("n1", "step-a", 1200)
        ops_logger.log_step_failed("n1", "step-b", "timeout", "cmd not found")
        ops_logger.log_bootstrap_completed("n1", 5000)
        ops_logger.log_bootstrap_failed("n1", "step-b", "terminate_and_retry")

        entries = ops_logger.read_entries()
        assert all(e["component"] == "bootstrap" for e in entries)

    def test_health_category(self, ops_logger):
        ops_logger.log_health_check_passed("n1", "L1")
        ops_logger.log_health_check_failed("n1", "L2", 3)
        ops_logger.log_worker_marked_unhealthy("n1", "3 failures")

        entries = ops_logger.read_entries()
        assert all(e["component"] == "health" for e in entries)
        assert entries[1]["level"] == "WARNING"
        assert entries[2]["level"] == "WARNING"

    def test_credential_category(self, ops_logger):
        ops_logger.log_credential_assigned("n1", "cred-a", "production")
        ops_logger.log_credential_rotated("n1", "cred-a", "cred-b", "quota")
        ops_logger.log_credential_recovered("n1", "cred-a")
        ops_logger.log_quota_warning("cred-a", 87.5)

        entries = ops_logger.read_entries()
        assert all(e["component"] == "credentials" for e in entries)
        assert entries[0]["details"]["slot_type"] == "production"
        assert entries[3]["details"]["usage_pct"] == 87.5

    def test_reconciler_category(self, ops_logger):
        ops_logger.log_reconcile_started()
        ops_logger.log_orphan_found("i-123", "terminate")
        ops_logger.log_ghost_found("ghost-1", "remove")
        ops_logger.log_reconcile_completed(1, 1, 2500)

        entries = ops_logger.read_entries()
        assert all(e["component"] == "reconciler" for e in entries)

    def test_webhook_category(self, ops_logger):
        ops_logger.log_webhook_sent("task.completed", "t1", "https://x.com", 200)
        ops_logger.log_webhook_retry("task.completed", "t1", 2, "2026-01-01T00:05:00Z")
        ops_logger.log_webhook_failed("task.completed", "t1", 5, "refused")

        entries = ops_logger.read_entries()
        assert all(e["component"] == "webhook" for e in entries)
        assert entries[0]["details"]["status_code"] == 200
        assert entries[1]["level"] == "WARNING"
        assert entries[2]["level"] == "ERROR"

    def test_connection_category(self, ops_logger):
        ops_logger.log_worker_connected("n1", reconnect=True)
        ops_logger.log_worker_disconnected("n1", reason="timeout")

        entries = ops_logger.read_entries()
        assert all(e["component"] == "connection" for e in entries)
        assert entries[0]["details"]["reconnect"] is True


# ---------------------------------------------------------------------------
# Rotation
# ---------------------------------------------------------------------------


class TestRotation:

    def test_rotation_renames_old_log(self, tmp_path):
        log_path = tmp_path / "operations.log"
        log_path.write_text('{"old": true}\n')
        old_mtime = time.time() - 86400 * 2
        os.utime(str(log_path), (old_mtime, old_mtime))

        lg = OperationsLogger(str(log_path), retention_days=30)
        lg.log("test", "new_entry")
        lg.close()

        rotated = list(tmp_path.glob("operations-*.log"))
        assert len(rotated) >= 1


# ---------------------------------------------------------------------------
# Retention cleanup
# ---------------------------------------------------------------------------


class TestRetentionCleanup:

    def test_old_logs_removed(self, tmp_path):
        log_path = tmp_path / "operations.log"
        old_file = tmp_path / "operations-2020-01-01.log"
        old_file.write_text('{"old": true}\n')
        old_mtime = time.time() - 100 * 86400
        os.utime(str(old_file), (old_mtime, old_mtime))

        lg = OperationsLogger(str(log_path), retention_days=30)
        lg.log("test", "trigger")
        lg.close()

        assert not old_file.exists()

    def test_recent_logs_preserved(self, tmp_path):
        log_path = tmp_path / "operations.log"
        recent_file = tmp_path / "operations-recent.log"
        recent_file.write_text('{"recent": true}\n')

        lg = OperationsLogger(str(log_path), retention_days=30)
        lg.log("test", "trigger")
        lg.close()

        assert recent_file.exists()


# ---------------------------------------------------------------------------
# Read entries
# ---------------------------------------------------------------------------


class TestReadEntries:

    def test_read_entries_returns_recent(self, ops_logger):
        for i in range(10):
            ops_logger.log("test", f"action_{i}")
        entries = ops_logger.read_entries(limit=5)
        assert len(entries) == 5
        assert entries[0]["action"] == "action_5"
        assert entries[4]["action"] == "action_9"

    def test_read_entries_empty_log(self, tmp_path):
        lg = OperationsLogger(str(tmp_path / "empty.log"))
        assert lg.read_entries() == []
        lg.close()

    def test_read_entries_all(self, ops_logger):
        for i in range(3):
            ops_logger.log("test", f"a{i}")
        entries = ops_logger.read_entries(limit=100)
        assert len(entries) == 3


# ---------------------------------------------------------------------------
# Close and reopen
# ---------------------------------------------------------------------------


class TestCloseAndReopen:

    def test_close_and_rewrite(self, tmp_path):
        path = str(tmp_path / "ops.log")
        lg1 = OperationsLogger(path)
        lg1.log("test", "first")
        lg1.close()

        lg2 = OperationsLogger(path)
        lg2.log("test", "second")
        entries = lg2.read_entries()
        assert len(entries) >= 1
        lg2.close()
