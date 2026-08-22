"""Tests for Manager structured operations log (T-039)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from elastic_agent.core.operations_logger import OperationsLogger


@pytest.fixture
def ops_logger(tmp_path):
    path = tmp_path / "operations.log"
    lg = OperationsLogger(log_path=str(path), retention_days=30, log_level="DEBUG")
    yield lg
    lg.close()


class TestBasicLogging:
    def test_log_creates_file(self, ops_logger, tmp_path):
        ops_logger.log("test", "test_action")
        assert (tmp_path / "operations.log").exists()

    def test_log_entry_format(self, ops_logger):
        entry = ops_logger.log("bootstrap", "step_completed", node_id="node-1", details={"step": "install"})
        assert entry is not None
        assert entry["component"] == "bootstrap"
        assert entry["action"] == "step_completed"
        assert entry["node_id"] == "node-1"
        assert entry["details"]["step"] == "install"
        assert entry["level"] == "INFO"
        assert "ts" in entry
        assert entry["trace_id"].startswith("op-")

    def test_log_custom_level(self, ops_logger):
        entry = ops_logger.log("health", "check_failed", level="WARNING")
        assert entry["level"] == "WARNING"

    def test_log_custom_trace_id(self, ops_logger):
        entry = ops_logger.log("test", "action", trace_id="custom-trace-123")
        assert entry["trace_id"] == "custom-trace-123"

    def test_log_persists_to_file(self, ops_logger, tmp_path):
        ops_logger.log("test", "action1", details={"key": "val1"})
        ops_logger.log("test", "action2", details={"key": "val2"})

        entries = ops_logger.read_entries()
        assert len(entries) == 2
        assert entries[0]["action"] == "action1"
        assert entries[1]["action"] == "action2"

    def test_json_lines_format(self, ops_logger, tmp_path):
        ops_logger.log("a", "b")
        ops_logger.log("c", "d")

        log_path = tmp_path / "operations.log"
        lines = log_path.read_text().strip().splitlines()
        assert len(lines) == 2
        for line in lines:
            parsed = json.loads(line)
            assert "ts" in parsed
            assert "component" in parsed


class TestLevelFiltering:
    def test_debug_level_allows_all(self, tmp_path):
        lg = OperationsLogger(str(tmp_path / "ops.log"), log_level="DEBUG")
        assert lg.log("t", "a", level="DEBUG") is not None
        assert lg.log("t", "a", level="INFO") is not None
        assert lg.log("t", "a", level="WARNING") is not None
        assert lg.log("t", "a", level="ERROR") is not None
        lg.close()

    def test_info_level_filters_debug(self, tmp_path):
        lg = OperationsLogger(str(tmp_path / "ops.log"), log_level="INFO")
        assert lg.log("t", "a", level="DEBUG") is None
        assert lg.log("t", "a", level="INFO") is not None
        lg.close()

    def test_warning_level_filters_info(self, tmp_path):
        lg = OperationsLogger(str(tmp_path / "ops.log"), log_level="WARNING")
        assert lg.log("t", "a", level="DEBUG") is None
        assert lg.log("t", "a", level="INFO") is None
        assert lg.log("t", "a", level="WARNING") is not None
        assert lg.log("t", "a", level="ERROR") is not None
        lg.close()

    def test_error_level_filters_warning(self, tmp_path):
        lg = OperationsLogger(str(tmp_path / "ops.log"), log_level="ERROR")
        assert lg.log("t", "a", level="WARNING") is None
        assert lg.log("t", "a", level="ERROR") is not None
        lg.close()


class TestConvenienceMethods:
    def test_scale_out_started(self, ops_logger):
        ops_logger.log_scale_out_started(3, "api_request")
        entries = ops_logger.read_entries()
        assert len(entries) == 1
        assert entries[0]["action"] == "scale_out_started"
        assert entries[0]["details"]["count"] == 3
        assert entries[0]["details"]["trigger"] == "api_request"

    def test_scale_out_completed(self, ops_logger):
        ops_logger.log_scale_out_completed(["n1", "n2"], 5000)
        entries = ops_logger.read_entries()
        assert entries[0]["details"]["nodes"] == ["n1", "n2"]
        assert entries[0]["details"]["duration_ms"] == 5000

    def test_bootstrap_step_flow(self, ops_logger):
        ops_logger.log_bootstrap_started("node-1", ["step-a", "step-b"])
        ops_logger.log_step_started("node-1", "step-a")
        ops_logger.log_step_completed("node-1", "step-a", 1200)
        ops_logger.log_step_started("node-1", "step-b")
        ops_logger.log_step_failed("node-1", "step-b", "timeout", "command not found")
        ops_logger.log_bootstrap_failed("node-1", "step-b", "terminate_and_retry")

        entries = ops_logger.read_entries()
        assert len(entries) == 6
        actions = [e["action"] for e in entries]
        assert actions == [
            "bootstrap_started", "step_started", "step_completed",
            "step_started", "step_failed", "bootstrap_failed",
        ]
        assert entries[4]["level"] == "ERROR"

    def test_health_check_methods(self, ops_logger):
        ops_logger.log_health_check_passed("node-1", "L1")
        ops_logger.log_health_check_failed("node-1", "L2", 3)
        ops_logger.log_worker_marked_unhealthy("node-1", "3 consecutive failures")

        entries = ops_logger.read_entries()
        assert len(entries) == 3
        assert entries[1]["level"] == "WARNING"
        assert entries[1]["details"]["consecutive_failures"] == 3
        assert entries[2]["level"] == "WARNING"

    def test_credential_methods(self, ops_logger):
        ops_logger.log_credential_assigned("node-1", "cred-a", "production")
        ops_logger.log_credential_rotated("node-1", "cred-a", "cred-b", "quota_exceeded")
        ops_logger.log_credential_recovered("node-1", "cred-a")
        ops_logger.log_quota_warning("cred-a", 87.5)

        entries = ops_logger.read_entries()
        assert len(entries) == 4
        assert entries[0]["details"]["slot_type"] == "production"
        assert entries[1]["details"]["old_id"] == "cred-a"
        assert entries[3]["details"]["usage_pct"] == 87.5

    def test_reconcile_methods(self, ops_logger):
        ops_logger.log_reconcile_started()
        ops_logger.log_orphan_found("i-12345", "terminate")
        ops_logger.log_ghost_found("node-ghost", "remove")
        ops_logger.log_reconcile_completed(1, 1, 3200)

        entries = ops_logger.read_entries()
        assert len(entries) == 4
        assert entries[1]["details"]["instance_id"] == "i-12345"
        assert entries[3]["details"]["orphans"] == 1

    def test_webhook_methods(self, ops_logger):
        ops_logger.log_webhook_sent("task.completed", "t-1", "https://example.com", 200)
        ops_logger.log_webhook_retry("task.completed", "t-1", 2, "2026-01-01T00:05:00Z")
        ops_logger.log_webhook_failed("task.completed", "t-1", 5, "connection refused")

        entries = ops_logger.read_entries()
        assert len(entries) == 3
        assert entries[0]["details"]["status_code"] == 200
        assert entries[1]["level"] == "WARNING"
        assert entries[2]["level"] == "ERROR"

    def test_worker_connection_methods(self, ops_logger):
        ops_logger.log_worker_connected("node-1", reconnect=False)
        ops_logger.log_worker_disconnected("node-1", reason="connection_lost")

        entries = ops_logger.read_entries()
        assert len(entries) == 2
        assert entries[0]["details"]["reconnect"] is False
        assert entries[1]["details"]["reason"] == "connection_lost"

    def test_instance_lifecycle(self, ops_logger):
        ops_logger.log_instance_created("node-1", "ecs.c6.large", "cn-hangzhou")
        ops_logger.log_instance_terminated("node-1", "scale_in")
        ops_logger.log_scale_in_started("node-1", "manual")

        entries = ops_logger.read_entries()
        assert len(entries) == 3
        assert entries[0]["details"]["instance_type"] == "ecs.c6.large"
        assert entries[1]["details"]["reason"] == "scale_in"


class TestReadEntries:
    def test_read_entries_limit(self, ops_logger):
        for i in range(10):
            ops_logger.log("test", f"action_{i}")
        entries = ops_logger.read_entries(limit=3)
        assert len(entries) == 3
        assert entries[0]["action"] == "action_7"

    def test_read_entries_empty(self, tmp_path):
        lg = OperationsLogger(str(tmp_path / "empty.log"))
        assert lg.read_entries() == []
        lg.close()


class TestRetention:
    def test_cleanup_old_logs(self, tmp_path):
        import time
        log_path = tmp_path / "operations.log"
        old_file = tmp_path / "operations-2020-01-01.log"
        old_file.write_text('{"old": true}\n')
        os.utime(str(old_file), (time.time() - 100 * 86400, time.time() - 100 * 86400))

        lg = OperationsLogger(str(log_path), retention_days=30)
        lg.log("test", "trigger_cleanup")
        lg.close()

        assert not old_file.exists()


import os
