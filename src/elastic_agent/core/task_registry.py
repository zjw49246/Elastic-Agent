"""TaskRegistry — task-to-worker mapping with JSON persistence and crash recovery.

T-050: Manages the mapping between tasks and workers.
Provides CRUD operations, per-worker listing, crash recovery on restart,
and automatic cleanup when a Worker goes offline.
"""

from __future__ import annotations

import asyncio
import enum
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class TaskStatus(str, enum.Enum):
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    WORKER_LOST = "worker_lost"


class TaskRecord(BaseModel):
    task_id: str
    worker_id: str
    session_id: str | None = None
    status: TaskStatus = TaskStatus.RUNNING
    metadata: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=_utcnow)
    updated_at: datetime = Field(default_factory=_utcnow)


TERMINAL_LOGICAL_STATUSES = {"completed", "failed", "cancelled"}


def is_record_active(record: TaskRecord) -> bool:
    """Return True when a record should count against worker capacity."""
    logical_status = (record.metadata or {}).get("logical_status")
    if logical_status in TERMINAL_LOGICAL_STATUSES:
        return False
    return record.status == TaskStatus.RUNNING


class TaskRegistry:
    """Thread-safe, JSON-file-backed task-to-worker mapping.

    All mutations are protected by an asyncio.Lock and flushed to disk
    immediately so that crash recovery can reload the last-written state.
    """

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path).expanduser()
        self._lock = asyncio.Lock()
        self._tasks: dict[str, TaskRecord] = {}
        self._loaded = False

    async def load(self) -> None:
        async with self._lock:
            self._load_sync()

    def _load_sync(self) -> None:
        if self._path.exists():
            try:
                raw = json.loads(self._path.read_text(encoding="utf-8"))
                tasks: dict[str, TaskRecord] = {}
                for tid, data in raw.get("tasks", {}).items():
                    tasks[tid] = TaskRecord.model_validate(data)
                self._tasks = tasks
                logger.info("TaskRegistry loaded %d tasks from %s", len(self._tasks), self._path)
            except Exception:
                logger.exception("Failed to load TaskRegistry from %s, starting empty", self._path)
                self._tasks = {}
        else:
            self._tasks = {}
        self._loaded = True

    def _flush_sync(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "tasks": {tid: json.loads(rec.model_dump_json()) for tid, rec in self._tasks.items()},
        }
        tmp = self._path.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
        tmp.replace(self._path)

    async def register(self, task_id: str, worker_id: str, metadata: dict[str, Any] | None = None) -> TaskRecord:
        async with self._lock:
            if not self._loaded:
                self._load_sync()
            record = TaskRecord(
                task_id=task_id,
                worker_id=worker_id,
                metadata=metadata or {},
            )
            self._tasks[task_id] = record
            self._flush_sync()
            logger.info("TaskRegistry: registered task %s on worker %s", task_id, worker_id)
            return record

    async def get(self, task_id: str) -> TaskRecord | None:
        async with self._lock:
            if not self._loaded:
                self._load_sync()
            return self._tasks.get(task_id)

    async def update(self, task_id: str, **fields: Any) -> TaskRecord | None:
        async with self._lock:
            if not self._loaded:
                self._load_sync()
            rec = self._tasks.get(task_id)
            if rec is None:
                return None
            for k, v in fields.items():
                if hasattr(rec, k):
                    setattr(rec, k, v)
            rec.updated_at = _utcnow()
            self._flush_sync()
            logger.debug("TaskRegistry: updated task %s fields=%s", task_id, list(fields))
            return rec

    async def unregister(self, task_id: str) -> TaskRecord | None:
        async with self._lock:
            if not self._loaded:
                self._load_sync()
            rec = self._tasks.pop(task_id, None)
            if rec is not None:
                self._flush_sync()
                logger.info("TaskRegistry: unregistered task %s", task_id)
            return rec

    async def list_by_worker(self, worker_id: str) -> list[TaskRecord]:
        async with self._lock:
            if not self._loaded:
                self._load_sync()
            return [r for r in self._tasks.values() if r.worker_id == worker_id]

    async def list_by_status(self, status: TaskStatus) -> list[TaskRecord]:
        async with self._lock:
            if not self._loaded:
                self._load_sync()
            return [r for r in self._tasks.values() if r.status == status]

    async def list_all(self) -> list[TaskRecord]:
        async with self._lock:
            if not self._loaded:
                self._load_sync()
            return list(self._tasks.values())

    async def count(self) -> int:
        async with self._lock:
            if not self._loaded:
                self._load_sync()
            return len(self._tasks)

    async def count_active_by_worker(self, worker_id: str) -> int:
        async with self._lock:
            if not self._loaded:
                self._load_sync()
            return sum(
                1 for r in self._tasks.values()
                if r.worker_id == worker_id and is_record_active(r)
            )

    async def cleanup_worker(self, worker_id: str) -> list[TaskRecord]:
        """Mark all running tasks on a worker as WORKER_LOST. Called on worker disconnect."""
        async with self._lock:
            if not self._loaded:
                self._load_sync()
            affected: list[TaskRecord] = []
            for rec in self._tasks.values():
                if rec.worker_id == worker_id and is_record_active(rec):
                    rec.status = TaskStatus.WORKER_LOST
                    rec.updated_at = _utcnow()
                    affected.append(rec)
            if affected:
                self._flush_sync()
                logger.warning(
                    "TaskRegistry: marked %d tasks as worker_lost for worker %s",
                    len(affected),
                    worker_id,
                )
            return affected

    async def recover(self, online_worker_ids: set[str]) -> list[TaskRecord]:
        """Crash recovery: mark running tasks on offline workers as WORKER_LOST."""
        async with self._lock:
            if not self._loaded:
                self._load_sync()
            affected: list[TaskRecord] = []
            for rec in self._tasks.values():
                if is_record_active(rec) and rec.worker_id not in online_worker_ids:
                    rec.status = TaskStatus.WORKER_LOST
                    rec.updated_at = _utcnow()
                    affected.append(rec)
            if affected:
                self._flush_sync()
                logger.warning(
                    "TaskRegistry: crash recovery marked %d tasks as worker_lost",
                    len(affected),
                )
            return affected
