"""Bounded, private Manager-side snapshots of batch Job command output."""

from __future__ import annotations

import hashlib
import heapq
import json
import logging
import re
import stat
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from elastic_agent.core.secure_store import (
    atomic_write_private,
    fsync_directory,
    secure_state_directory,
    tighten_state_file,
)

logger = logging.getLogger(__name__)

_SAFE_JOB_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")
_SNAPSHOT_VERSION = 1
_TAIL_RESPONSE_BYTES = 8 * 1024 * 1024
_PRUNED_MARKER = ".pruned"
_PERSISTED_EXIT_FIELDS = frozenset(
    {
        "event_id",
        "exit_code",
        "session_id",
        "error_type",
        "error_message",
    }
)


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _validate_job_id(job_id: str) -> str:
    if _SAFE_JOB_ID.fullmatch(job_id) is None:
        raise ValueError("invalid job_id")
    return job_id


def _task_key(task_id: str) -> str:
    return hashlib.sha256(task_id.encode("utf-8")).hexdigest()


def _bounded_text(value: object, limit: int) -> str:
    text = str(value or "")
    if len(text.encode("utf-8")) <= limit:
        return text
    marker = "\n[… log line truncated …]\n"
    marker_bytes = len(marker.encode("utf-8"))
    budget = max(0, limit - marker_bytes)
    raw = text.encode("utf-8")
    head = raw[: budget // 2].decode("utf-8", errors="ignore")
    tail = raw[-(budget - budget // 2) :].decode("utf-8", errors="ignore")
    return head + marker + tail


class JobLogStore:
    """Persist one bounded snapshot per batch task.

    A worker's reliable ``PROCESS_EXIT`` is the commit point.  The Manager
    writes the tail already held by ``LogEventParser`` before final collection
    and instance teardown.  Raw task ids never become filesystem components.
    """

    def __init__(
        self,
        root: str | Path,
        *,
        max_entries: int = 5_000,
        max_bytes: int = 8 * 1024 * 1024,
        max_line_bytes: int = 64 * 1024,
        max_job_bytes: int = 64 * 1024 * 1024,
        max_total_bytes: int = 1024 * 1024 * 1024,
        max_tasks_per_job: int = 512,
        retention_days: int = 30,
    ) -> None:
        if max_entries < 1:
            raise ValueError("max_entries must be positive")
        if max_bytes < 4_096:
            raise ValueError("max_bytes must be at least 4096")
        if max_line_bytes < 256:
            raise ValueError("max_line_bytes must be at least 256")
        if max_job_bytes < max_bytes:
            raise ValueError("max_job_bytes must cover one task snapshot")
        if max_total_bytes < max_job_bytes:
            raise ValueError("max_total_bytes must cover one Job")
        if max_tasks_per_job < 1:
            raise ValueError("max_tasks_per_job must be positive")
        self.root = Path(root).expanduser()
        self.max_entries = max_entries
        self.max_bytes = max_bytes
        self.max_line_bytes = min(max_line_bytes, max_bytes // 2)
        self.max_job_bytes = max_job_bytes
        self.max_total_bytes = max_total_bytes
        self.max_tasks_per_job = max_tasks_per_job
        self.retention_days = max(0, retention_days)
        self._write_lock = threading.RLock()
        self._last_full_prune = 0.0
        self._known_total_bytes: int | None = None

    def _job_directory(self, job_id: str, *, create: bool) -> Path:
        job_id = _validate_job_id(job_id)
        if create:
            if self.root.is_symlink() or (self.root.exists() and not self.root.is_dir()):
                raise RuntimeError(f"Job log root is not a real directory: {self.root}")
            secure_state_directory(self.root)
            candidate = self.root / job_id
            if candidate.is_symlink() or (candidate.exists() and not candidate.is_dir()):
                raise RuntimeError(f"Job log path is not a real directory: {candidate}")
            return secure_state_directory(candidate)
        if self.root.is_symlink() or (self.root.exists() and not self.root.is_dir()):
            raise RuntimeError(f"Job log root is not a real directory: {self.root}")
        directory = self.root / job_id
        if not directory.exists():
            return directory
        if directory.is_symlink() or not directory.is_dir():
            raise RuntimeError(f"Job log path is not a real directory: {directory}")
        secure_state_directory(self.root)
        secure_state_directory(directory)
        return directory

    def _normalize_entry(
        self,
        entry: dict[str, Any],
        *,
        task_id: str,
        worker_id: str,
    ) -> tuple[dict[str, str], bool]:
        entry_task = str(entry.get("task_id") or task_id)
        entry_worker = str(entry.get("worker_id") or worker_id)
        if entry_task != task_id or entry_worker != worker_id:
            raise ValueError("log entry ownership does not match snapshot")
        data = str(entry.get("data") or "")
        bounded = _bounded_text(data, self.max_line_bytes)
        return (
            {
                "task_id": task_id,
                "worker_id": worker_id,
                "stream": ("stderr" if str(entry.get("stream") or "") == "stderr" else "stdout"),
                "data": bounded,
                "timestamp": _bounded_text(
                    entry.get("timestamp") or _utcnow_iso(),
                    128,
                ),
            },
            bounded != data,
        )

    def save_snapshot(
        self,
        *,
        job_id: str,
        task_id: str,
        worker_id: str,
        entries: Iterable[dict[str, Any]],
        exit_info: dict[str, Any],
        source_truncated: bool = False,
    ) -> Path:
        """Atomically replace the deterministic snapshot for ``task_id``."""

        job_id = _validate_job_id(job_id)
        if not task_id.startswith(f"{job_id}:"):
            raise ValueError("task_id does not belong to the batch Job")
        if not worker_id or len(task_id) > 1_024 or len(worker_id) > 512:
            raise ValueError("invalid task or worker identity")
        with self._write_lock:
            return self._save_snapshot_locked(
                job_id=job_id,
                task_id=task_id,
                worker_id=worker_id,
                entries=entries,
                exit_info=exit_info,
                source_truncated=source_truncated,
            )

    def _save_snapshot_locked(
        self,
        *,
        job_id: str,
        task_id: str,
        worker_id: str,
        entries: Iterable[dict[str, Any]],
        exit_info: dict[str, Any],
        source_truncated: bool,
    ) -> Path:
        destination = self._job_directory(job_id, create=True) / f"{_task_key(task_id)}.json"
        normalized: list[dict[str, str]] = []
        line_truncated = False
        for entry in entries:
            item, was_truncated = self._normalize_entry(
                entry,
                task_id=task_id,
                worker_id=worker_id,
            )
            normalized.append(item)
            line_truncated = line_truncated or was_truncated
        if not normalized and destination.exists():
            existing = self._read_snapshot(destination, job_id)
            if existing is not None:
                # Reliable PROCESS_EXIT may replay after the first archive
                # released the in-memory parser buffer.  Never replace useful
                # output with that replay's empty snapshot.
                return destination

        count_truncated = len(normalized) > self.max_entries
        normalized = normalized[-self.max_entries :]
        safe_exit: dict[str, Any] = {}
        for key in _PERSISTED_EXIT_FIELDS:
            if key not in exit_info:
                continue
            value = exit_info[key]
            if key == "exit_code":
                try:
                    safe_exit[key] = int(value)
                except (TypeError, ValueError):
                    safe_exit[key] = -1
            elif value is not None:
                safe_exit[key] = _bounded_text(value, 4_096)

        saved_at = _utcnow_iso()
        truncated = source_truncated or count_truncated or line_truncated
        while True:
            payload = {
                "version": _SNAPSHOT_VERSION,
                "job_id": job_id,
                "task_id": task_id,
                "worker_id": worker_id,
                "saved_at": saved_at,
                "complete": True,
                "truncated": truncated,
                "exit": safe_exit,
                "entries": normalized,
            }
            encoded = json.dumps(
                payload,
                ensure_ascii=False,
                separators=(",", ":"),
            )
            if len(encoded.encode("utf-8")) <= self.max_bytes:
                break
            if not normalized:
                # Constructor limits leave ample room for metadata.  Keep this
                # guard explicit in case a future schema adds large fields.
                raise ValueError("Job log snapshot metadata exceeds max_bytes")
            normalized.pop(0)
            truncated = True

        try:
            old_size = destination.stat().st_size
        except OSError:
            old_size = 0
        saved = atomic_write_private(destination, encoded)
        if self._known_total_bytes is not None:
            self._known_total_bytes += saved.stat().st_size - old_size
        cutoff_ns = self._retention_cutoff_ns()
        self._prune_job_locked(destination.parent, cutoff_ns=cutoff_ns)
        # Per-Job quotas are enforced for every exit.  Scanning the entire
        # 1-GiB store for every shard would serialize a large fan-out's cleanup,
        # so global retention/quota enforcement is throttled and also runs at
        # every Manager startup.
        if (
            self._known_total_bytes is None
            or self._known_total_bytes > self.max_total_bytes
            or time.monotonic() - self._last_full_prune >= 60
        ):
            self._prune_locked(cutoff_ns=cutoff_ns)
        return saved

    def _read_snapshot(
        self,
        path: Path,
        expected_job_id: str,
    ) -> dict[str, Any] | None:
        try:
            if path.is_symlink() or not stat.S_ISREG(path.lstat().st_mode):
                return None
            tighten_state_file(path)
            payload = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(payload, dict):
                raise ValueError("snapshot must be a JSON object")
            task_id = payload.get("task_id")
            if (
                payload.get("version") != _SNAPSHOT_VERSION
                or payload.get("job_id") != expected_job_id
                or not isinstance(task_id, str)
                or path.name != f"{_task_key(task_id)}.json"
                or not task_id.startswith(f"{expected_job_id}:")
                or not isinstance(payload.get("worker_id"), str)
                or not isinstance(payload.get("entries"), list)
                or not isinstance(payload.get("exit"), dict)
            ):
                raise ValueError("snapshot identity/schema mismatch")
            valid_entries = []
            for entry in payload["entries"]:
                if (
                    not isinstance(entry, dict)
                    or entry.get("task_id") != task_id
                    or entry.get("worker_id") != payload["worker_id"]
                    or entry.get("stream") not in {"stdout", "stderr"}
                    or not isinstance(entry.get("data"), str)
                    or not isinstance(entry.get("timestamp"), str)
                ):
                    raise ValueError("invalid Job log entry")
                valid_entries.append(entry)
            payload["entries"] = valid_entries
            return payload
        except (
            AttributeError,
            json.JSONDecodeError,
            OSError,
            RuntimeError,
            TypeError,
            ValueError,
        ):
            logger.warning("Ignoring invalid Job log snapshot %s", path)
            return None

    def _snapshot_paths(
        self,
        job_id: str,
        *,
        task_id: str | None = None,
    ) -> list[Path]:
        directory = self._job_directory(job_id, create=False)
        if not directory.exists():
            return []
        if task_id is not None:
            candidate = directory / f"{_task_key(task_id)}.json"
            return [candidate] if candidate.exists() else []
        return sorted(directory.glob("*.json"))

    def read_job(self, job_id: str) -> list[dict[str, Any]]:
        """Read valid snapshots for one Job, ignoring corrupt unrelated files."""

        snapshots: list[dict[str, Any]] = []
        for path in self._snapshot_paths(job_id):
            payload = self._read_snapshot(path, job_id)
            if payload is not None:
                snapshots.append(payload)
        snapshots.sort(
            key=lambda item: (
                str(item.get("saved_at") or ""),
                str(item.get("task_id") or ""),
            )
        )
        return snapshots

    def read_job_tail(
        self,
        job_id: str,
        *,
        lines: int,
        worker_id: str | None = None,
        task_id: str | None = None,
    ) -> dict[str, Any]:
        """Stream snapshots one at a time and retain only the newest entries."""

        if lines < 1 or lines > 5_000:
            raise ValueError("invalid Job log tail size")
        if task_id is not None and not task_id.startswith(f"{job_id}:"):
            raise ValueError("task_id does not belong to the batch Job")

        newest: list[tuple[tuple[str, str, int], int, dict[str, Any]]] = []
        retained_bytes = 0
        tasks: list[dict[str, Any]] = []
        total = 0
        history_truncated = self._has_pruned_marker(job_id)
        for path in self._snapshot_paths(job_id, task_id=task_id):
            payload = self._read_snapshot(path, job_id)
            if payload is None or (worker_id is not None and payload["worker_id"] != worker_id):
                continue
            entries = payload.pop("entries")
            tasks.append(payload)
            history_truncated = history_truncated or bool(payload.get("truncated"))
            candidate_task = str(payload["task_id"])
            for ordinal, entry in enumerate(entries):
                total += 1
                key = (
                    str(entry.get("timestamp") or ""),
                    candidate_task,
                    ordinal,
                )
                item = dict(entry)
                item_bytes = len(str(item.get("data") or "").encode("utf-8"))
                heapq.heappush(newest, (key, item_bytes, item))
                retained_bytes += item_bytes
                while len(newest) > lines or retained_bytes > _TAIL_RESPONSE_BYTES:
                    _old_key, old_bytes, _old_item = heapq.heappop(newest)
                    retained_bytes -= old_bytes

        tasks.sort(
            key=lambda item: (
                str(item.get("saved_at") or ""),
                str(item.get("task_id") or ""),
            )
        )
        entries = [item for _key, _size, item in sorted(newest)]
        return {
            "tasks": tasks,
            "entries": entries,
            "total": total,
            "history_truncated": history_truncated,
            "truncated": history_truncated or total > len(entries),
        }

    def _has_pruned_marker(self, job_id: str) -> bool:
        directory = self._job_directory(job_id, create=False)
        marker = directory / _PRUNED_MARKER
        if not marker.exists():
            return False
        try:
            if marker.is_symlink() or not stat.S_ISREG(marker.lstat().st_mode):
                return True
            tighten_state_file(marker)
            payload = json.loads(marker.read_text(encoding="utf-8"))
            if (
                not isinstance(payload, dict)
                or payload.get("version") != _SNAPSHOT_VERSION
                or int(payload.get("pruned_snapshots") or 0) < 1
            ):
                raise ValueError("invalid Job log prune marker")
            return True
        except (
            json.JSONDecodeError,
            OSError,
            RuntimeError,
            TypeError,
            ValueError,
        ):
            # A marker's very presence means history may be incomplete.  Fail
            # closed even if its explanatory metadata was damaged.
            return True

    def _mark_pruned(
        self,
        directory: Path,
        *,
        reason: str,
        count: int,
    ) -> None:
        if count < 1:
            return
        marker = directory / _PRUNED_MARKER
        previous_count = 0
        reasons: set[str] = set()
        if marker.exists():
            if marker.is_symlink():
                raise RuntimeError(f"Job log prune marker is a symlink: {marker}")
            try:
                tighten_state_file(marker)
                previous = json.loads(marker.read_text(encoding="utf-8"))
                if isinstance(previous, dict):
                    previous_count = max(
                        0,
                        int(previous.get("pruned_snapshots") or 0),
                    )
                    previous_reasons = previous.get("reasons")
                    if isinstance(previous_reasons, list):
                        reasons.update(str(item) for item in previous_reasons if item)
            except (
                json.JSONDecodeError,
                OSError,
                RuntimeError,
                TypeError,
                ValueError,
            ):
                # Replace corrupt metadata with a conservative fresh marker.
                previous_count = 0
                reasons.clear()
        reasons.add(reason)
        atomic_write_private(
            marker,
            json.dumps(
                {
                    "version": _SNAPSHOT_VERSION,
                    "pruned_at": _utcnow_iso(),
                    "pruned_snapshots": previous_count + count,
                    "reasons": sorted(reasons),
                },
                ensure_ascii=False,
                separators=(",", ":"),
            ),
        )

    @staticmethod
    def _regular_json_files(directory: Path) -> list[tuple[Path, int, int]]:
        files: list[tuple[Path, int, int]] = []
        for path in directory.glob("*.json"):
            try:
                info = path.lstat()
            except OSError:
                continue
            if path.is_symlink() or not stat.S_ISREG(info.st_mode):
                continue
            files.append((path, info.st_size, info.st_mtime_ns))
        return files

    @staticmethod
    def _unlink_files(
        files: Iterable[tuple[Path, int, int]],
        directory: Path,
    ) -> set[Path]:
        removed: set[Path] = set()
        for path, _size, _mtime in files:
            try:
                path.unlink()
                removed.add(path)
            except FileNotFoundError:
                removed.add(path)
            except OSError:
                logger.warning("Could not prune Job log snapshot %s", path)
        if removed:
            fsync_directory(directory)
        return removed

    def _prune_job_locked(
        self,
        directory: Path,
        *,
        cutoff_ns: int | None,
    ) -> list[tuple[Path, int, int]]:
        files = self._regular_json_files(directory)
        expired = [item for item in files if cutoff_ns is not None and item[2] < cutoff_ns]
        if expired:
            self._mark_pruned(
                directory,
                reason="retention",
                count=len(expired),
            )
            expired_paths = self._unlink_files(expired, directory)
            files = [item for item in files if item[0] not in expired_paths]

        files.sort(key=lambda item: (item[2], item[0].name))
        remove_count = max(0, len(files) - self.max_tasks_per_job)
        total = sum(item[1] for item in files)
        remove_bytes = sum(item[1] for item in files[:remove_count])
        while remove_count < len(files) and total - remove_bytes > self.max_job_bytes:
            remove_bytes += files[remove_count][1]
            remove_count += 1
        if remove_count:
            self._mark_pruned(
                directory,
                reason="job_quota",
                count=remove_count,
            )
            removed_paths = self._unlink_files(files[:remove_count], directory)
            files = [item for item in files if item[0] not in removed_paths]
        return files

    def _retention_cutoff_ns(self) -> int | None:
        return int((time.time() - self.retention_days * 86_400) * 1_000_000_000) if self.retention_days > 0 else None

    def _prune_locked(self, *, cutoff_ns: int | None = None) -> None:
        if not self.root.exists():
            self._last_full_prune = time.monotonic()
            self._known_total_bytes = 0
            return
        if self.root.is_symlink() or not self.root.is_dir():
            raise RuntimeError(f"Job log root is not a real directory: {self.root}")
        if cutoff_ns is None:
            cutoff_ns = self._retention_cutoff_ns()
        retained: list[tuple[Path, int, int]] = []
        for directory in sorted(self.root.iterdir()):
            if directory.is_symlink() or not directory.is_dir():
                continue
            retained.extend(self._prune_job_locked(directory, cutoff_ns=cutoff_ns))
        retained.sort(key=lambda item: (item[2], str(item[0])))
        total = sum(item[1] for item in retained)
        index = 0
        while index < len(retained) and total > self.max_total_bytes:
            path, size, _mtime = retained[index]
            self._mark_pruned(
                path.parent,
                reason="global_quota",
                count=1,
            )
            if path in self._unlink_files([retained[index]], path.parent):
                total -= size
            index += 1
        self._last_full_prune = time.monotonic()
        self._known_total_bytes = total

    def prune(self) -> None:
        """Apply retention and byte/task quotas to existing snapshots."""

        with self._write_lock:
            self._prune_locked()
