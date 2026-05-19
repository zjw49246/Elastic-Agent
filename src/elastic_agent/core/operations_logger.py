"""Manager structured operations log — JSON Lines with daily rotation.

T-039: All critical Manager operations are logged in structured JSON Lines format
for operational debugging and auditing.
"""

from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class OperationsLogger:
    """Structured JSON Lines logger with daily rotation and retention.

    Each log entry contains:
        ts, level, component, action, node_id, details, trace_id

    Usage:
        ops = OperationsLogger("~/.elastic-agent/operations.log")
        ops.log("bootstrap", "step_completed", node_id="aliyun:i-bp1xxx",
                details={"step": "install-claude-code", "duration_ms": 12345})
    """

    def __init__(
        self,
        log_path: str = "~/.elastic-agent/operations.log",
        retention_days: int = 30,
        log_level: str = "INFO",
    ) -> None:
        self._log_path = Path(os.path.expanduser(log_path))
        self._retention_days = retention_days
        self._log_level = log_level.upper()
        self._current_date: str = ""
        self._file: Any = None

        self._log_path.parent.mkdir(parents=True, exist_ok=True)
        self._rotate_if_needed()

    def log(
        self,
        component: str,
        action: str,
        *,
        level: str = "INFO",
        node_id: str | None = None,
        details: dict[str, Any] | None = None,
        trace_id: str | None = None,
    ) -> dict[str, Any] | None:
        """Write a structured log entry. Returns the entry dict, or None if filtered."""
        if not self._should_log(level):
            return None

        self._rotate_if_needed()

        entry = {
            "ts": _utcnow().isoformat(),
            "level": level.upper(),
            "component": component,
            "action": action,
            "node_id": node_id,
            "details": details or {},
            "trace_id": trace_id or f"op-{uuid.uuid4().hex[:12]}",
        }

        line = json.dumps(entry, ensure_ascii=False, default=str)
        try:
            self._ensure_file()
            self._file.write(line + "\n")
            self._file.flush()
        except Exception:
            logger.exception("Failed to write operations log entry")

        return entry

    def log_scale_out_started(self, count: int, trigger: str, **kw: Any) -> None:
        self.log("scaling", "scale_out_started", details={"count": count, "trigger": trigger}, **kw)

    def log_scale_out_completed(self, nodes: list[str], duration_ms: int, **kw: Any) -> None:
        self.log("scaling", "scale_out_completed", details={"nodes": nodes, "duration_ms": duration_ms}, **kw)

    def log_scale_in_started(self, node_id: str, reason: str, **kw: Any) -> None:
        self.log("scaling", "scale_in_started", node_id=node_id, details={"node_id": node_id, "reason": reason}, **kw)

    def log_instance_created(self, node_id: str, instance_type: str, region: str, **kw: Any) -> None:
        self.log("scaling", "instance_created", node_id=node_id, details={"node_id": node_id, "instance_type": instance_type, "region": region}, **kw)

    def log_instance_terminated(self, node_id: str, reason: str, **kw: Any) -> None:
        self.log("scaling", "instance_terminated", node_id=node_id, details={"node_id": node_id, "reason": reason}, **kw)

    def log_bootstrap_started(self, node_id: str, steps: list[str], **kw: Any) -> None:
        self.log("bootstrap", "bootstrap_started", node_id=node_id, details={"node_id": node_id, "steps": steps}, **kw)

    def log_step_started(self, node_id: str, step_name: str, **kw: Any) -> None:
        self.log("bootstrap", "step_started", node_id=node_id, details={"node_id": node_id, "step_name": step_name}, **kw)

    def log_step_completed(self, node_id: str, step_name: str, duration_ms: int, **kw: Any) -> None:
        self.log("bootstrap", "step_completed", node_id=node_id, details={"node_id": node_id, "step_name": step_name, "duration_ms": duration_ms}, **kw)

    def log_step_failed(self, node_id: str, step_name: str, error: str, stderr: str = "", **kw: Any) -> None:
        self.log("bootstrap", "step_failed", level="ERROR", node_id=node_id, details={"node_id": node_id, "step_name": step_name, "error": error, "stderr": stderr}, **kw)

    def log_bootstrap_completed(self, node_id: str, total_duration_ms: int, **kw: Any) -> None:
        self.log("bootstrap", "bootstrap_completed", node_id=node_id, details={"node_id": node_id, "total_duration_ms": total_duration_ms}, **kw)

    def log_bootstrap_failed(self, node_id: str, failed_step: str, strategy: str, **kw: Any) -> None:
        self.log("bootstrap", "bootstrap_failed", level="ERROR", node_id=node_id, details={"node_id": node_id, "failed_step": failed_step, "strategy": strategy}, **kw)

    def log_health_check_passed(self, node_id: str, level_name: str, **kw: Any) -> None:
        self.log("health", "health_check_passed", node_id=node_id, details={"node_id": node_id, "level": level_name}, **kw)

    def log_health_check_failed(self, node_id: str, level_name: str, consecutive_failures: int, **kw: Any) -> None:
        self.log("health", "health_check_failed", level="WARNING", node_id=node_id, details={"node_id": node_id, "level": level_name, "consecutive_failures": consecutive_failures}, **kw)

    def log_worker_marked_unhealthy(self, node_id: str, reason: str, **kw: Any) -> None:
        self.log("health", "worker_marked_unhealthy", level="WARNING", node_id=node_id, details={"node_id": node_id, "reason": reason}, **kw)

    def log_credential_assigned(self, node_id: str, credential_id: str, slot_type: str, **kw: Any) -> None:
        self.log("credentials", "credential_assigned", node_id=node_id, details={"node_id": node_id, "credential_id": credential_id, "slot_type": slot_type}, **kw)

    def log_credential_rotated(self, node_id: str, old_id: str, new_id: str, reason: str, **kw: Any) -> None:
        self.log("credentials", "credential_rotated", node_id=node_id, details={"node_id": node_id, "old_id": old_id, "new_id": new_id, "reason": reason}, **kw)

    def log_credential_recovered(self, node_id: str, credential_id: str, **kw: Any) -> None:
        self.log("credentials", "credential_recovered", node_id=node_id, details={"node_id": node_id, "credential_id": credential_id}, **kw)

    def log_quota_warning(self, credential_id: str, usage_pct: float, **kw: Any) -> None:
        self.log("credentials", "quota_warning", level="WARNING", details={"credential_id": credential_id, "usage_pct": usage_pct}, **kw)

    def log_reconcile_started(self, **kw: Any) -> None:
        self.log("reconciler", "reconcile_started", **kw)

    def log_orphan_found(self, instance_id: str, action: str, **kw: Any) -> None:
        self.log("reconciler", "orphan_found", level="WARNING", details={"instance_id": instance_id, "action": action}, **kw)

    def log_ghost_found(self, node_id: str, action: str, **kw: Any) -> None:
        self.log("reconciler", "ghost_found", level="WARNING", node_id=node_id, details={"node_id": node_id, "action": action}, **kw)

    def log_reconcile_completed(self, orphans: int, ghosts: int, duration_ms: int, **kw: Any) -> None:
        self.log("reconciler", "reconcile_completed", details={"orphans": orphans, "ghosts": ghosts, "duration_ms": duration_ms}, **kw)

    def log_webhook_sent(self, event_type: str, task_id: str, target_url: str, status_code: int, **kw: Any) -> None:
        self.log("webhook", "webhook_sent", details={"event_type": event_type, "task_id": task_id, "target_url": target_url, "status_code": status_code}, **kw)

    def log_webhook_retry(self, event_type: str, task_id: str, attempt: int, next_retry_at: str, **kw: Any) -> None:
        self.log("webhook", "webhook_retry", level="WARNING", details={"event_type": event_type, "task_id": task_id, "attempt": attempt, "next_retry_at": next_retry_at}, **kw)

    def log_webhook_failed(self, event_type: str, task_id: str, attempts: int, error: str, **kw: Any) -> None:
        self.log("webhook", "webhook_failed", level="ERROR", details={"event_type": event_type, "task_id": task_id, "attempts": attempts, "error": error}, **kw)

    def log_worker_connected(self, node_id: str, reconnect: bool = False, **kw: Any) -> None:
        self.log("connection", "worker_connected", node_id=node_id, details={"node_id": node_id, "reconnect": reconnect}, **kw)

    def log_worker_disconnected(self, node_id: str, reason: str = "", **kw: Any) -> None:
        self.log("connection", "worker_disconnected", node_id=node_id, details={"node_id": node_id, "reason": reason}, **kw)

    # ------------------------------------------------------------------
    # Rotation & cleanup
    # ------------------------------------------------------------------

    def _rotate_if_needed(self) -> None:
        today = _utcnow().strftime("%Y-%m-%d")
        if today == self._current_date:
            return

        self._close_file()
        self._current_date = today

        if self._log_path.exists() and self._log_path.stat().st_size > 0:
            rotated = self._log_path.with_suffix(f".{self._log_path.stem.split('.')[0]}-prev.log")
            try:
                stat = self._log_path.stat()
                from datetime import date
                file_date = date.fromtimestamp(stat.st_mtime).isoformat()
                if file_date != today:
                    rotated_name = self._log_path.parent / f"operations-{file_date}.log"
                    self._log_path.rename(rotated_name)
            except Exception:
                pass

        self._cleanup_old_logs()

    def _cleanup_old_logs(self) -> None:
        if self._retention_days <= 0:
            return
        try:
            import time as time_mod
            cutoff = time_mod.time() - self._retention_days * 86400
            for f in self._log_path.parent.glob("operations-*.log"):
                if f.stat().st_mtime < cutoff:
                    f.unlink()
        except Exception:
            pass

    def _ensure_file(self) -> None:
        if self._file is None or self._file.closed:
            self._log_path.parent.mkdir(parents=True, exist_ok=True)
            self._file = open(self._log_path, "a", encoding="utf-8")

    def _close_file(self) -> None:
        if self._file and not self._file.closed:
            self._file.close()
        self._file = None

    def _should_log(self, level: str) -> bool:
        levels = {"DEBUG": 0, "INFO": 1, "WARNING": 2, "ERROR": 3}
        return levels.get(level.upper(), 1) >= levels.get(self._log_level, 1)

    def close(self) -> None:
        self._close_file()

    def read_entries(self, limit: int = 100) -> list[dict[str, Any]]:
        """Read recent log entries (for testing/debugging)."""
        entries: list[dict[str, Any]] = []
        if not self._log_path.exists():
            return entries
        for line in self._log_path.read_text().strip().splitlines():
            if line.strip():
                try:
                    entries.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
        return entries[-limit:]
