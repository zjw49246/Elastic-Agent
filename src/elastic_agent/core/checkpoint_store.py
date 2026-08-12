"""Immutable, hash-verified S3 checkpoints for recoverable Mode-B Jobs."""

from __future__ import annotations

import fnmatch
import hashlib
import json
import logging
import os
import re
import shutil
import stat
import tempfile
import threading
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any, BinaryIO, Iterator

_SAFE_COMPONENT = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")
_SHA256 = re.compile(r"[0-9a-f]{64}")
_SCHEMA_VERSION = 2
_SUPPORTED_CHECKPOINT_SCHEMAS = frozenset({1, _SCHEMA_VERSION})
_CHECKPOINT_SET_SCHEMA_VERSION = 1
_READ_CHUNK = 1024 * 1024
_S3_DELETE_BATCH = 1_000
_DEFAULT_SNAPSHOT_FREE_RESERVE_BYTES = 1024 * 1024 * 1024
logger = logging.getLogger(__name__)


class _ReadWriteLock:
    """Writer-preferring lock shared by stores addressing the same Job."""

    def __init__(self) -> None:
        self._condition = threading.Condition()
        self._readers = 0
        self._writer = False
        self._waiting_writers = 0

    def _wait(
        self,
        predicate,
        *,
        deadline_monotonic: float | None,
        cancel_event: threading.Event | None,
    ) -> None:
        while predicate():
            if cancel_event is not None and cancel_event.is_set():
                raise CheckpointError("checkpoint operation cancelled")
            remaining = (
                None
                if deadline_monotonic is None
                else deadline_monotonic - time.monotonic()
            )
            if remaining is not None and remaining <= 0:
                raise CheckpointError(
                    "checkpoint operation deadline exceeded"
                )
            if deadline_monotonic is None and cancel_event is None:
                self._condition.wait()
            else:
                self._condition.wait(
                    timeout=(
                        0.1
                        if remaining is None
                        else min(0.1, remaining)
                    )
                )

    @contextmanager
    def read(
        self,
        *,
        deadline_monotonic: float | None = None,
        cancel_event: threading.Event | None = None,
    ) -> Iterator[None]:
        with self._condition:
            self._wait(
                lambda: self._writer or self._waiting_writers,
                deadline_monotonic=deadline_monotonic,
                cancel_event=cancel_event,
            )
            self._readers += 1
        try:
            yield
        finally:
            with self._condition:
                self._readers -= 1
                if self._readers == 0:
                    self._condition.notify_all()

    @contextmanager
    def write(
        self,
        *,
        deadline_monotonic: float | None = None,
        cancel_event: threading.Event | None = None,
    ) -> Iterator[None]:
        with self._condition:
            self._waiting_writers += 1
            try:
                self._wait(
                    lambda: self._writer or self._readers,
                    deadline_monotonic=deadline_monotonic,
                    cancel_event=cancel_event,
                )
                self._writer = True
            finally:
                self._waiting_writers -= 1
                # A cancelled/timed-out writer must wake readers that were
                # blocked solely by writer preference.
                self._condition.notify_all()
        try:
            yield
        finally:
            with self._condition:
                self._writer = False
                self._condition.notify_all()


_JOB_LOCKS_GUARD = threading.Lock()
_JOB_LOCKS: dict[tuple[str, str, str], _ReadWriteLock] = {}


class CheckpointError(RuntimeError):
    """A checkpoint cannot be committed or restored safely."""


class IncompleteCheckpointSetError(CheckpointError):
    """Not every requested shard manifest has committed yet."""

    def __init__(
        self,
        message: str,
        *,
        committed_namespaces: set[str] | None = None,
    ) -> None:
        super().__init__(message)
        self.committed_namespaces = frozenset(
            committed_namespaces or set()
        )


def _safe_component(value: str, *, label: str) -> str:
    if not isinstance(value, str) or _SAFE_COMPONENT.fullmatch(value) is None:
        raise CheckpointError(f"invalid {label}")
    return value


def _safe_relative_path(value: str, *, label: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > 4_096
        or value.startswith("/")
        or "\\" in value
        or "\x00" in value
        or any(ord(character) < 0x20 for character in value)
    ):
        raise CheckpointError(f"unsafe {label}: {value!r}")
    path = PurePosixPath(value)
    if ".." in path.parts or path == PurePosixPath("."):
        raise CheckpointError(f"unsafe {label}: {value!r}")
    return path.as_posix().rstrip("/")


def _under_any_path(relative: str, paths: list[str]) -> bool:
    return any(
        relative == path or relative.startswith(path.rstrip("/") + "/")
        for path in paths
    )


def _stable_identity(file_stat: os.stat_result) -> tuple[int, int, int, int, int]:
    return (
        file_stat.st_dev,
        file_stat.st_ino,
        file_stat.st_size,
        file_stat.st_mtime_ns,
        file_stat.st_ctime_ns,
    )


def _sha256_stream(stream: BinaryIO) -> str:
    stream.seek(0)
    digest = hashlib.sha256()
    while chunk := stream.read(_READ_CHUNK):
        digest.update(chunk)
    stream.seek(0)
    return digest.hexdigest()


def _new_generation() -> str:
    return (
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
        + "-"
        + uuid.uuid4().hex
    )


def _is_not_found(exc: BaseException) -> bool:
    if isinstance(exc, KeyError):
        return True
    response = getattr(exc, "response", {})
    error = response.get("Error", {}) if isinstance(response, dict) else {}
    code = str(error.get("Code") or "")
    status = (
        response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if isinstance(response, dict)
        else None
    )
    return code in {"404", "NoSuchKey", "NotFound"} or status == 404


def _is_precondition_failed(exc: BaseException) -> bool:
    response = getattr(exc, "response", {})
    error = response.get("Error", {}) if isinstance(response, dict) else {}
    code = str(error.get("Code") or "")
    status = (
        response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if isinstance(response, dict)
        else None
    )
    return code in {"PreconditionFailed", "412"} or status == 412


class S3CheckpointStore:
    """Commit local result trees and stage trusted prior Job results.

    V2 blobs are immutable, content addressed, and shared by every shard
    generation of one Job. A shard ``COMMITTED.json`` and a Job-level
    checkpoint-set ``COMMITTED.json`` are conditionally uploaded last.
    """

    def __init__(
        self,
        *,
        bucket: str,
        prefix: str = "jobs",
        client: Any | None = None,
        region: str = "ap-northeast-1",
        max_objects: int = 100_000,
        max_total_bytes: int = 20 * 1024 * 1024 * 1024,
        max_manifest_bytes: int = 16 * 1024 * 1024,
        max_generations: int = 10_000,
        max_checkpoint_sets: int = 1_000,
        max_gc_objects: int = 1_000_000,
        snapshot_root: str | Path | None = None,
        max_file_bytes: int | None = None,
        max_snapshot_bytes: int | None = None,
        snapshot_free_reserve_bytes: int = (
            _DEFAULT_SNAPSHOT_FREE_RESERVE_BYTES
        ),
        max_concurrent_uploads: int = 8,
    ) -> None:
        max_file_bytes = max_total_bytes if max_file_bytes is None else max_file_bytes
        max_snapshot_bytes = (
            max_total_bytes
            if max_snapshot_bytes is None
            else max_snapshot_bytes
        )
        if not bucket.strip():
            raise ValueError("checkpoint bucket cannot be empty")
        if min(
            max_objects,
            max_total_bytes,
            max_manifest_bytes,
            max_generations,
            max_checkpoint_sets,
            max_gc_objects,
            max_file_bytes,
            max_snapshot_bytes,
        ) <= 0:
            raise ValueError("checkpoint limits must be positive")
        if snapshot_free_reserve_bytes < 0:
            raise ValueError(
                "checkpoint snapshot free-space reserve cannot be negative"
            )
        if (
            isinstance(max_concurrent_uploads, bool)
            or not isinstance(max_concurrent_uploads, int)
            or max_concurrent_uploads <= 0
            or max_concurrent_uploads > 64
        ):
            raise ValueError(
                "checkpoint max_concurrent_uploads must be between 1 and 64"
            )
        self._bucket = bucket.strip()
        self._prefix = prefix.strip("/")
        self._client = client
        self._region = region
        self._max_objects = max_objects
        self._max_total_bytes = max_total_bytes
        self._max_manifest_bytes = max_manifest_bytes
        self._max_generations = max_generations
        self._max_checkpoint_sets = max_checkpoint_sets
        self._max_gc_objects = max_gc_objects
        self._max_file_bytes = max_file_bytes
        self._max_snapshot_bytes = max_snapshot_bytes
        self._snapshot_free_reserve_bytes = snapshot_free_reserve_bytes
        self._snapshot_budget_lock = threading.Lock()
        self._snapshot_reserved_bytes = 0
        self._upload_slots = threading.BoundedSemaphore(
            max_concurrent_uploads
        )
        self._snapshot_root: Path | None = None
        if snapshot_root is not None:
            configured_root = Path(snapshot_root).expanduser()
            if configured_root.is_symlink():
                raise ValueError("checkpoint snapshot root cannot be a symlink")
            configured_root.mkdir(parents=True, exist_ok=True, mode=0o700)
            if not configured_root.is_dir():
                raise ValueError(
                    "checkpoint snapshot root must be a directory"
                )
            os.chmod(configured_root, 0o700)
            self._snapshot_root = configured_root

    def _s3(self):
        if self._client is None:
            import boto3
            from botocore.config import Config

            self._client = boto3.client(
                "s3",
                region_name=self._region,
                config=Config(
                    connect_timeout=3,
                    read_timeout=10,
                    retries={
                        "total_max_attempts": 3,
                        "mode": "standard",
                    },
                    tcp_keepalive=True,
                ),
            )
        return self._client

    def _job_root(self, job_id: str) -> str:
        job_id = _safe_component(job_id, label="job id")
        return f"{self._prefix}/{job_id}" if self._prefix else job_id

    def _worker_root(self, job_id: str, worker_namespace: str) -> str:
        worker_namespace = _safe_component(
            worker_namespace, label="worker namespace",
        )
        return f"{self._job_root(job_id)}/workers/{worker_namespace}"

    def _job_lock(self, job_id: str) -> _ReadWriteLock:
        safe_job_id = _safe_component(job_id, label="job id")
        key = (self._bucket, self._prefix, safe_job_id)
        with _JOB_LOCKS_GUARD:
            lock = _JOB_LOCKS.get(key)
            if lock is None:
                lock = _ReadWriteLock()
                _JOB_LOCKS[key] = lock
            return lock

    def _normalize_paths(self, paths: list[str]) -> list[str]:
        if not isinstance(paths, list) or len(paths) > self._max_objects:
            raise CheckpointError("checkpoint path limit exceeded")
        normalized = [
            _safe_relative_path(path, label="checkpoint path")
            for path in paths
        ]
        if not normalized:
            raise CheckpointError("checkpoint paths cannot be empty")
        if len(set(normalized)) != len(normalized):
            raise CheckpointError("checkpoint paths must be unique")
        ordered = sorted(normalized, key=lambda value: PurePosixPath(value).parts)
        for path, other in zip(ordered, ordered[1:], strict=False):
            if other.startswith(path.rstrip("/") + "/"):
                raise CheckpointError(
                    "checkpoint paths must not overlap"
                )
        return normalized

    def _normalize_excludes(self, patterns: list[str] | None) -> list[str]:
        normalized = list(patterns or [])
        if len(normalized) > self._max_objects:
            raise CheckpointError("checkpoint exclude pattern limit exceeded")
        for pattern in normalized:
            if (
                not isinstance(pattern, str)
                or not pattern
                or len(pattern) > 4_096
                or "\x00" in pattern
                or any(ord(character) < 0x20 for character in pattern)
            ):
                raise CheckpointError("invalid checkpoint exclude pattern")
        return normalized

    @staticmethod
    def _normalize_metadata(
        metadata: dict[str, Any] | None,
        *,
        label: str,
    ) -> dict[str, Any]:
        if metadata is None:
            return {}
        if not isinstance(metadata, dict):
            raise CheckpointError(f"{label} metadata must be an object")
        return dict(metadata)

    @staticmethod
    def _excluded(relative: str, patterns: list[str]) -> bool:
        return any(
            fnmatch.fnmatchcase(relative, pattern)
            or fnmatch.fnmatchcase(PurePosixPath(relative).name, pattern)
            for pattern in patterns
        )

    def _safe_entries(
        self,
        source_root: Path,
        paths: list[str],
        exclude: list[str],
    ) -> Iterator[tuple[str, Path, Path, str, os.stat_result]]:
        try:
            root = source_root.resolve(strict=True)
        except OSError as exc:
            raise CheckpointError(
                "checkpoint source root is missing"
            ) from exc
        if not root.is_dir():
            raise CheckpointError("checkpoint source root is not a directory")
        for relative_root in paths:
            traversal_root = root / relative_root
            try:
                traversal_root.resolve(strict=True).relative_to(root)
            except ValueError as exc:
                raise CheckpointError("checkpoint path escaped source root") from exc
            except OSError as exc:
                raise CheckpointError(
                    f"checkpoint path is missing: {relative_root!r}"
                ) from exc
            try:
                traversal_stat = os.lstat(traversal_root)
            except OSError as exc:
                raise CheckpointError(
                    f"checkpoint path is missing: {relative_root!r}"
                ) from exc
            if not stat.S_ISDIR(traversal_stat.st_mode):
                raise CheckpointError(
                    f"checkpoint path is missing or not a directory: "
                    f"{relative_root!r}"
                )
            for dirpath, dirnames, filenames in os.walk(
                traversal_root, topdown=True, followlinks=False,
            ):
                directory = Path(dirpath)
                try:
                    directory.resolve(strict=True).relative_to(root)
                    directory_stat = os.lstat(directory)
                except (OSError, ValueError) as exc:
                    raise CheckpointError(
                        "checkpoint directory changed while traversing"
                    ) from exc
                if not stat.S_ISDIR(directory_stat.st_mode):
                    raise CheckpointError(
                        "checkpoint directory changed while traversing"
                    )
                directory_relative = directory.relative_to(root).as_posix()
                yield (
                    "directory",
                    root,
                    directory,
                    directory_relative,
                    directory_stat,
                )
                safe_dirs: list[str] = []
                for name in sorted(dirnames):
                    candidate = directory / name
                    try:
                        candidate_stat = os.lstat(candidate)
                    except OSError:
                        continue
                    relative = candidate.relative_to(root).as_posix()
                    if (
                        stat.S_ISDIR(candidate_stat.st_mode)
                        and not self._excluded(relative, exclude)
                    ):
                        safe_dirs.append(name)
                dirnames[:] = safe_dirs
                for name in sorted(filenames):
                    candidate = directory / name
                    relative = candidate.relative_to(root).as_posix()
                    if self._excluded(relative, exclude):
                        continue
                    try:
                        file_stat = os.lstat(candidate)
                    except OSError:
                        continue
                    if not stat.S_ISREG(file_stat.st_mode):
                        continue
                    yield "file", root, candidate, relative, file_stat

    @staticmethod
    def _open_validated(root: Path, path: Path) -> tuple[BinaryIO, os.stat_result]:
        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        descriptor = os.open(path, flags)
        try:
            opened_stat = os.fstat(descriptor)
            if not stat.S_ISREG(opened_stat.st_mode):
                raise CheckpointError(f"checkpoint file is not regular: {path}")
            proc_path = Path(f"/proc/self/fd/{descriptor}")
            if not proc_path.exists():
                raise CheckpointError(
                    "cannot verify checkpoint file descriptor ancestry"
                )
            proc_path.resolve(strict=True).relative_to(root)
            return os.fdopen(descriptor, "rb"), opened_stat
        except BaseException:
            os.close(descriptor)
            raise

    def _remove_snapshot_and_release_budget(
        self,
        snapshot: Path,
        size: int,
    ) -> None:
        """Remove one private snapshot and return its in-memory reservation."""

        try:
            snapshot.unlink()
        except FileNotFoundError:
            pass
        with self._snapshot_budget_lock:
            self._snapshot_reserved_bytes -= size

    def _snapshot_file(
        self,
        *,
        root: Path,
        path: Path,
        relative: str,
        snapshot_root: Path,
        remaining_bytes: int,
        deadline_monotonic: float | None,
        cancel_event: threading.Event | None = None,
    ) -> tuple[Path, int, str, int]:
        """Copy a live file into a private, fsynced, read-only local snapshot."""

        source, opened_stat = self._open_validated(root, path)
        snapshot = snapshot_root / uuid.uuid4().hex
        descriptor = -1
        reserved = False
        try:
            if opened_stat.st_size > remaining_bytes:
                raise CheckpointError("checkpoint byte limit exceeded")
            if opened_stat.st_size > self._max_file_bytes:
                raise CheckpointError("checkpoint single-file limit exceeded")
            with self._snapshot_budget_lock:
                free = shutil.disk_usage(snapshot_root).free
                if (
                    self._snapshot_reserved_bytes + opened_stat.st_size
                    > self._max_snapshot_bytes
                ):
                    raise CheckpointError(
                        "checkpoint snapshot byte budget is exhausted"
                    )
                if (
                    self._snapshot_reserved_bytes
                    + opened_stat.st_size
                    + self._snapshot_free_reserve_bytes
                    > free
                ):
                    raise CheckpointError(
                        "insufficient disk for checkpoint snapshot"
                    )
                self._snapshot_reserved_bytes += opened_stat.st_size
                reserved = True
            descriptor = os.open(
                snapshot,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0),
                0o600,
            )
            digest = hashlib.sha256()
            copied = 0
            expected_size = opened_stat.st_size
            with source, os.fdopen(descriptor, "wb") as output:
                descriptor = -1
                before = _stable_identity(opened_stat)
                while copied < expected_size:
                    self._check_deadline(deadline_monotonic, cancel_event)
                    chunk = source.read(min(
                        _READ_CHUNK,
                        expected_size - copied,
                    ))
                    if not chunk:
                        raise CheckpointError(
                            f"checkpoint file changed while snapshotting: "
                            f"{relative}"
                        )
                    copied += len(chunk)
                    output.write(chunk)
                    digest.update(chunk)
                # The reservation is exactly the size observed at open. Never
                # copy appended bytes into the private snapshot: one-byte look
                # ahead detects growth without allowing it to spend unreserved
                # Manager disk.
                self._check_deadline(deadline_monotonic, cancel_event)
                if source.read(1):
                    raise CheckpointError(
                        f"checkpoint file changed while snapshotting: "
                        f"{relative}"
                    )
                output.flush()
                os.fsync(output.fileno())
                after = os.fstat(source.fileno())
                if (
                    _stable_identity(after) != before
                    or copied != opened_stat.st_size
                ):
                    raise CheckpointError(
                        f"checkpoint file changed while snapshotting: {relative}"
                    )
            os.chmod(snapshot, 0o400)
            return (
                snapshot,
                copied,
                digest.hexdigest(),
                stat.S_IMODE(opened_stat.st_mode) & 0o777,
            )
        except BaseException:
            if descriptor >= 0:
                os.close(descriptor)
            source.close()
            if reserved:
                self._remove_snapshot_and_release_budget(
                    snapshot,
                    opened_stat.st_size,
                )
            raise

    def _head_blob(self, key: str) -> dict[str, Any] | None:
        try:
            return self._s3().head_object(Bucket=self._bucket, Key=key)
        except Exception as exc:  # noqa: BLE001
            if _is_not_found(exc):
                return None
            raise CheckpointError(
                f"cannot verify checkpoint blob: {key}"
            ) from exc

    @staticmethod
    def _validate_blob_head(
        response: dict[str, Any],
        *,
        key: str,
        size: int,
        digest: str,
    ) -> None:
        try:
            content_length = int(response.get("ContentLength"))
        except (TypeError, ValueError) as exc:
            raise CheckpointError(
                f"checkpoint blob metadata is invalid: {key}"
            ) from exc
        metadata = response.get("Metadata")
        if (
            content_length != size
            or not isinstance(metadata, dict)
            or metadata.get("sha256") != digest
        ):
            raise CheckpointError(
                f"checkpoint blob identity mismatch: {key}"
            )

    def _ensure_blob(
        self,
        *,
        key: str,
        snapshot: Path,
        size: int,
        digest: str,
        deadline_monotonic: float | None,
        cancel_event: threading.Event | None = None,
    ) -> bool:
        """Upload a new stable blob or prove the existing blob is identical."""

        self._check_deadline(deadline_monotonic, cancel_event)
        existing = self._head_blob(key)
        if existing is not None:
            self._validate_blob_head(
                existing, key=key, size=size, digest=digest,
            )
            return False
        with snapshot.open("rb") as stream:
            kwargs: dict[str, Any] = {
                "ExtraArgs": {
                    "ContentType": "application/octet-stream",
                    "Metadata": {"sha256": digest},
                },
            }
            if deadline_monotonic is not None or cancel_event is not None:
                kwargs["Callback"] = lambda _bytes: self._check_deadline(
                    deadline_monotonic, cancel_event,
                )
            self._acquire_bounded(
                self._upload_slots,
                deadline_monotonic=deadline_monotonic,
                cancel_event=cancel_event,
                label="upload slot",
            )
            try:
                self._s3().upload_fileobj(
                    stream, self._bucket, key, **kwargs,
                )
            finally:
                self._upload_slots.release()
        self._check_deadline(deadline_monotonic, cancel_event)
        uploaded = self._head_blob(key)
        if uploaded is None:
            raise CheckpointError(
                f"checkpoint blob disappeared after upload: {key}"
            )
        self._validate_blob_head(
            uploaded, key=key, size=size, digest=digest,
        )
        return True

    @staticmethod
    def _check_deadline(
        deadline_monotonic: float | None,
        cancel_event: threading.Event | None = None,
    ) -> None:
        if cancel_event is not None and cancel_event.is_set():
            raise CheckpointError("checkpoint operation cancelled")
        if (
            deadline_monotonic is not None
            and time.monotonic() >= deadline_monotonic
        ):
            raise CheckpointError("checkpoint operation deadline exceeded")

    @staticmethod
    def _acquire_bounded(
        lock,
        *,
        deadline_monotonic: float | None,
        cancel_event: threading.Event | None,
        label: str,
    ) -> None:
        while True:
            S3CheckpointStore._check_deadline(
                deadline_monotonic, cancel_event,
            )
            remaining = (
                None
                if deadline_monotonic is None
                else max(0.0, deadline_monotonic - time.monotonic())
            )
            wait = 0.1 if remaining is None else min(0.1, remaining)
            if lock.acquire(timeout=wait):
                return
            if remaining is not None and remaining <= 0:
                raise CheckpointError(
                    f"checkpoint {label} deadline exceeded"
                )

    def _json_payload(self, value: dict[str, Any], *, label: str) -> bytes:
        try:
            payload = json.dumps(
                value,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        except (TypeError, ValueError) as exc:
            raise CheckpointError(f"{label} is not JSON serializable") from exc
        if len(payload) > self._max_manifest_bytes:
            raise CheckpointError(f"{label} limit exceeded")
        return payload

    def _put_committed(
        self,
        *,
        key: str,
        payload: bytes,
        duplicate_message: str,
        allow_existing: bool = False,
        deadline_monotonic: float | None = None,
        cancel_event: threading.Event | None = None,
    ) -> bool:
        self._check_deadline(deadline_monotonic, cancel_event)
        try:
            self._s3().put_object(
                Bucket=self._bucket,
                Key=key,
                Body=payload,
                ContentType="application/json",
                IfNoneMatch="*",
            )
        except Exception as exc:  # noqa: BLE001
            if _is_precondition_failed(exc):
                if allow_existing:
                    return False
                raise CheckpointError(duplicate_message) from exc
            raise CheckpointError(
                f"cannot atomically publish checkpoint manifest: {key}"
            ) from exc
        return True

    def commit(
        self,
        *,
        job_id: str,
        worker_namespace: str,
        source_root: Path,
        paths: list[str],
        exclude: list[str] | None = None,
        generation: str | None = None,
        metadata: dict[str, Any] | None = None,
        deadline_monotonic: float | None = None,
        cancel_event: threading.Event | None = None,
    ) -> dict[str, Any]:
        """Upload all blobs, then atomically publish one generation manifest."""

        normalized_paths = self._normalize_paths(paths)
        normalized_exclude = self._normalize_excludes(exclude)
        normalized_metadata = self._normalize_metadata(
            metadata, label="checkpoint",
        )
        generation = generation or _new_generation()
        generation = _safe_component(generation, label="checkpoint generation")
        worker_root = self._worker_root(job_id, worker_namespace)
        committed_key = (
            f"{worker_root}/checkpoints/{generation}/COMMITTED.json"
        )
        blob_root = f"{self._job_root(job_id)}/checkpoint-blobs"

        with self._job_lock(job_id).read(
            deadline_monotonic=deadline_monotonic,
            cancel_event=cancel_event,
        ):
            self._check_deadline(deadline_monotonic, cancel_event)
            generation_exists = self._head_blob(committed_key) is not None
            self._check_deadline(deadline_monotonic, cancel_event)
            files: list[dict[str, Any]] = []
            directories: list[dict[str, Any]] = []
            total_bytes = 0
            with tempfile.TemporaryDirectory(
                prefix=".elastic-checkpoint-",
                dir=(
                    str(self._snapshot_root)
                    if self._snapshot_root is not None
                    else None
                ),
            ) as temporary:
                snapshot_root = Path(temporary)
                os.chmod(snapshot_root, 0o700)
                for kind, root, path, relative, entry_stat in self._safe_entries(
                    Path(source_root), normalized_paths, normalized_exclude,
                ):
                    self._check_deadline(deadline_monotonic, cancel_event)
                    if len(files) + len(directories) >= self._max_objects:
                        raise CheckpointError(
                            "checkpoint object limit exceeded"
                        )
                    if kind == "directory":
                        directories.append({
                            "path": relative,
                            "mode": stat.S_IMODE(entry_stat.st_mode) & 0o777,
                        })
                        continue
                    snapshot, size, digest, mode = self._snapshot_file(
                        root=root,
                        path=path,
                        relative=relative,
                        snapshot_root=snapshot_root,
                        remaining_bytes=self._max_total_bytes - total_bytes,
                        deadline_monotonic=deadline_monotonic,
                        cancel_event=cancel_event,
                    )
                    total_bytes += size
                    object_key = f"{blob_root}/{digest}"
                    try:
                        if not generation_exists:
                            self._ensure_blob(
                                key=object_key,
                                snapshot=snapshot,
                                size=size,
                                digest=digest,
                                deadline_monotonic=deadline_monotonic,
                                cancel_event=cancel_event,
                            )
                    finally:
                        self._remove_snapshot_and_release_budget(
                            snapshot,
                            size,
                        )
                    files.append({
                        "path": relative,
                        "size": size,
                        "sha256": digest,
                        "object_key": object_key,
                        "mode": mode,
                    })

            manifest: dict[str, Any] = {
                "schema_version": _SCHEMA_VERSION,
                "job_id": job_id,
                "worker_namespace": worker_namespace,
                "generation": generation,
                "paths": normalized_paths,
                "directories": directories,
                "files": files,
                "total_objects": len(directories) + len(files),
                "total_bytes": total_bytes,
                "committed_at": datetime.now(timezone.utc).isoformat(),
                "metadata": normalized_metadata,
            }
            payload = self._json_payload(
                manifest, label="checkpoint manifest",
            )
            if generation_exists:
                existing, _manifest_sha256 = self._committed_manifest(
                    source_job_id=job_id,
                    worker_namespace=worker_namespace,
                    generation=generation,
                    deadline_monotonic=deadline_monotonic,
                    cancel_event=cancel_event,
                )
                comparable_keys = (
                    "schema_version",
                    "job_id",
                    "worker_namespace",
                    "generation",
                    "paths",
                    "directories",
                    "files",
                    "total_objects",
                    "total_bytes",
                    "metadata",
                )
                if any(
                    existing.get(key) != manifest.get(key)
                    for key in comparable_keys
                ):
                    raise CheckpointError(
                        "checkpoint generation is already committed "
                        "with different content"
                    )
                return existing
            published = self._put_committed(
                key=committed_key,
                payload=payload,
                duplicate_message=(
                    "checkpoint generation is already committed"
                ),
                allow_existing=True,
                deadline_monotonic=deadline_monotonic,
                cancel_event=cancel_event,
            )
            if not published:
                existing, _manifest_sha256 = self._committed_manifest(
                    source_job_id=job_id,
                    worker_namespace=worker_namespace,
                    generation=generation,
                    deadline_monotonic=deadline_monotonic,
                    cancel_event=cancel_event,
                )
                comparable_keys = (
                    "schema_version",
                    "job_id",
                    "worker_namespace",
                    "generation",
                    "paths",
                    "directories",
                    "files",
                    "total_objects",
                    "total_bytes",
                    "metadata",
                )
                if any(
                    existing.get(key) != manifest.get(key)
                    for key in comparable_keys
                ):
                    raise CheckpointError(
                        "checkpoint generation is already committed "
                        "with different content"
                    )
                return existing
            return manifest

    def _read_object_limited(
        self,
        key: str,
        *,
        limit: int,
        deadline_monotonic: float | None = None,
        cancel_event: threading.Event | None = None,
    ) -> bytes:
        self._check_deadline(deadline_monotonic, cancel_event)
        try:
            response = self._s3().get_object(
                Bucket=self._bucket, Key=key,
            )
            body = response["Body"]
        except Exception as exc:  # noqa: BLE001
            raise CheckpointError(
                f"cannot read checkpoint object: {key}"
            ) from exc
        try:
            try:
                declared = int(response.get("ContentLength"))
            except Exception as exc:  # noqa: BLE001
                raise CheckpointError(
                    f"checkpoint object size is invalid: {key}"
                ) from exc
            if declared < 0 or declared > limit:
                raise CheckpointError(f"S3 object exceeds read limit: {key}")
            chunks: list[bytes] = []
            consumed = 0
            while True:
                self._check_deadline(deadline_monotonic, cancel_event)
                chunk = body.read(min(_READ_CHUNK, limit + 1 - consumed))
                if not chunk:
                    break
                chunks.append(chunk)
                consumed += len(chunk)
                if consumed > limit:
                    raise CheckpointError(
                        f"S3 object exceeds read limit: {key}"
                    )
            if consumed != declared:
                raise CheckpointError(
                    f"S3 object size changed while reading: {key}"
                )
            return b"".join(chunks)
        finally:
            close = getattr(body, "close", None)
            if callable(close):
                close()

    def _list_matching_keys(
        self,
        *,
        prefix: str,
        matcher: Any,
        label: str,
        limit: int | None = None,
        deadline_monotonic: float | None = None,
        cancel_event: threading.Event | None = None,
    ) -> list[str]:
        listing_limit = self._max_objects if limit is None else limit
        keys: list[str] = []
        seen: set[str] = set()
        try:
            self._check_deadline(deadline_monotonic, cancel_event)
            pages = self._s3().get_paginator("list_objects_v2").paginate(
                Bucket=self._bucket,
                Prefix=prefix,
            )
            for page in pages:
                self._check_deadline(deadline_monotonic, cancel_event)
                for item in page.get("Contents") or []:
                    self._check_deadline(deadline_monotonic, cancel_event)
                    key = str(item.get("Key") or "")
                    if not matcher(key):
                        continue
                    if key in seen:
                        raise CheckpointError(
                            f"duplicate key in {label} listing"
                        )
                    seen.add(key)
                    keys.append(key)
                    if len(keys) > listing_limit:
                        raise CheckpointError(f"{label} listing limit exceeded")
        except CheckpointError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise CheckpointError(f"cannot list {label}") from exc
        return keys

    def _decode_json_object(
        self,
        key: str,
        *,
        label: str,
        deadline_monotonic: float | None = None,
        cancel_event: threading.Event | None = None,
    ) -> tuple[
        dict[str, Any], bytes, str
    ]:
        try:
            raw = self._read_object_limited(
                key,
                limit=self._max_manifest_bytes,
                deadline_monotonic=deadline_monotonic,
                cancel_event=cancel_event,
            )
            value = json.loads(raw.decode("utf-8"))
        except CheckpointError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise CheckpointError(f"invalid {label}") from exc
        if not isinstance(value, dict):
            raise CheckpointError(f"invalid {label}")
        return value, raw, hashlib.sha256(raw).hexdigest()

    def _committed_manifest(
        self,
        *,
        source_job_id: str,
        worker_namespace: str,
        generation: str,
        expected_sha256: str | None = None,
        deadline_monotonic: float | None = None,
        cancel_event: threading.Event | None = None,
    ) -> tuple[dict[str, Any], str]:
        worker_root = self._worker_root(source_job_id, worker_namespace)
        if generation:
            selected = _safe_component(
                generation, label="checkpoint generation",
            )
            key = (
                f"{worker_root}/checkpoints/{selected}/COMMITTED.json"
            )
        else:
            prefix = f"{worker_root}/checkpoints/"
            def matches(key_value: str) -> bool:
                suffix = key_value.removeprefix(prefix)
                parts = suffix.split("/")
                return (
                    key_value.startswith(prefix)
                    and len(parts) == 2
                    and parts[1] == "COMMITTED.json"
                    and _SAFE_COMPONENT.fullmatch(parts[0]) is not None
                )

            candidates = self._list_matching_keys(
                prefix=prefix,
                matcher=matches,
                label="checkpoint generation",
                limit=self._max_generations,
                deadline_monotonic=deadline_monotonic,
                cancel_event=cancel_event,
            )
            if not candidates:
                raise CheckpointError("no committed checkpoint generation found")
            key = max(candidates)
            selected = key.removeprefix(prefix).split("/", 1)[0]
        if expected_sha256 is not None and _SHA256.fullmatch(
            expected_sha256
        ) is None:
            raise CheckpointError("invalid expected checkpoint manifest hash")
        manifest, _raw, manifest_sha256 = self._decode_json_object(
            key,
            label="checkpoint manifest",
            deadline_monotonic=deadline_monotonic,
            cancel_event=cancel_event,
        )
        if manifest.get("generation") != selected:
            raise CheckpointError(
                "checkpoint generation identity mismatch"
            )
        if (
            expected_sha256 is not None
            and manifest_sha256 != expected_sha256
        ):
            raise CheckpointError("checkpoint manifest checksum mismatch")
        return manifest, manifest_sha256

    def _validate_manifest_identity(
        self,
        manifest: dict[str, Any],
        *,
        source_job_id: str,
        worker_namespace: str,
        paths: list[str] | None = None,
        expected_metadata: dict[str, Any] | None = None,
    ) -> None:
        schema_version = manifest.get("schema_version")
        if (
            schema_version not in _SUPPORTED_CHECKPOINT_SCHEMAS
            or manifest.get("job_id") != source_job_id
            or manifest.get("worker_namespace") != worker_namespace
            or not isinstance(manifest.get("files"), list)
        ):
            raise CheckpointError("checkpoint manifest identity mismatch")
        manifest_paths = manifest.get("paths")
        if not isinstance(manifest_paths, list):
            raise CheckpointError("checkpoint manifest identity mismatch")
        try:
            normalized_manifest_paths = self._normalize_paths(manifest_paths)
        except CheckpointError as exc:
            raise CheckpointError(
                "checkpoint manifest paths are invalid"
            ) from exc
        if normalized_manifest_paths != manifest_paths:
            raise CheckpointError("checkpoint manifest paths are invalid")
        if paths is not None and manifest_paths != paths:
            raise CheckpointError("checkpoint manifest identity mismatch")
        self._parse_committed_at(
            manifest.get("committed_at"),
            label="checkpoint manifest",
        )
        metadata = manifest.get("metadata")
        if not isinstance(metadata, dict):
            raise CheckpointError("checkpoint manifest metadata is invalid")
        if expected_metadata is not None and not isinstance(
            expected_metadata, dict,
        ):
            raise CheckpointError(
                "expected checkpoint metadata must be an object"
            )
        for key, expected in (expected_metadata or {}).items():
            if metadata.get(key) != expected:
                raise CheckpointError(
                    f"checkpoint metadata mismatch: {key}"
                )

    @staticmethod
    def _parse_committed_at(value: Any, *, label: str) -> datetime:
        if not isinstance(value, str) or not value or len(value) > 128:
            raise CheckpointError(f"{label} committed_at is invalid")
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise CheckpointError(
                f"{label} committed_at is invalid"
            ) from exc
        if parsed.tzinfo is None:
            raise CheckpointError(f"{label} committed_at is invalid")
        return parsed.astimezone(timezone.utc)

    @staticmethod
    def _entry_mode(value: Any, *, label: str) -> int:
        if (
            isinstance(value, bool)
            or not isinstance(value, int)
            or value < 0
            or value > 0o777
        ):
            raise CheckpointError(f"invalid checkpoint {label} mode")
        return value

    def _validated_manifest_entries(
        self,
        manifest: dict[str, Any],
        *,
        source_job_id: str,
        worker_namespace: str,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], set[str]]:
        """Validate every path/key/size before restore or garbage collection."""

        schema_version = int(manifest["schema_version"])
        paths = list(manifest["paths"])
        raw_directories = manifest.get("directories", [])
        if schema_version == _SCHEMA_VERSION and not isinstance(
            raw_directories, list,
        ):
            raise CheckpointError("invalid checkpoint directory entries")
        if schema_version == 1:
            raw_directories = []
        files = manifest["files"]
        if len(files) + len(raw_directories) > self._max_objects:
            raise CheckpointError("checkpoint object limit exceeded")

        seen: set[str] = set()
        directories: list[dict[str, Any]] = []
        for raw in raw_directories:
            if not isinstance(raw, dict):
                raise CheckpointError("invalid checkpoint directory entry")
            relative = _safe_relative_path(
                raw.get("path"), label="checkpoint path",
            )
            if relative in seen or not _under_any_path(relative, paths):
                raise CheckpointError("invalid checkpoint directory entry")
            seen.add(relative)
            directories.append({
                "path": relative,
                "mode": self._entry_mode(
                    raw.get("mode"), label="directory",
                ),
            })

        if schema_version == _SCHEMA_VERSION:
            directory_paths = {entry["path"] for entry in directories}
            if not set(paths).issubset(directory_paths):
                raise CheckpointError(
                    "checkpoint manifest is missing requested directories"
                )
            for relative in directory_paths:
                parent = PurePosixPath(relative).parent
                while parent != PurePosixPath("."):
                    parent_text = parent.as_posix()
                    if _under_any_path(parent_text, paths):
                        if parent_text not in directory_paths:
                            raise CheckpointError(
                                "checkpoint manifest is missing a parent directory"
                            )
                    parent = parent.parent
        else:
            directory_paths = set()

        worker_root = self._worker_root(
            source_job_id, worker_namespace,
        )
        if schema_version == 1:
            generation = _safe_component(
                manifest.get("generation"),
                label="checkpoint generation",
            )
            blob_root = (
                f"{worker_root}/checkpoints/{generation}/blobs/"
            )
        else:
            blob_root = f"{self._job_root(source_job_id)}/checkpoint-blobs/"

        validated_files: list[dict[str, Any]] = []
        blob_keys: set[str] = set()
        total = 0
        for raw in files:
            if not isinstance(raw, dict):
                raise CheckpointError("invalid checkpoint file entry")
            relative = _safe_relative_path(
                raw.get("path"), label="checkpoint path",
            )
            if relative in seen:
                raise CheckpointError("duplicate checkpoint entry path")
            seen.add(relative)
            if not _under_any_path(relative, paths):
                raise CheckpointError(
                    f"checkpoint path is outside requested roots: {relative}"
                )
            digest = raw.get("sha256")
            key = raw.get("object_key")
            size = raw.get("size")
            if (
                isinstance(size, bool)
                or not isinstance(size, int)
                or size < 0
                or not isinstance(digest, str)
                or _SHA256.fullmatch(digest) is None
                or key != f"{blob_root}{digest}"
            ):
                raise CheckpointError("invalid checkpoint file entry")
            mode = (
                0o600
                if schema_version == 1
                else self._entry_mode(raw.get("mode"), label="file")
            )
            if schema_version == _SCHEMA_VERSION:
                parent = PurePosixPath(relative).parent
                while parent != PurePosixPath("."):
                    parent_text = parent.as_posix()
                    if _under_any_path(parent_text, paths):
                        if parent_text not in directory_paths:
                            raise CheckpointError(
                                "checkpoint manifest is missing a parent directory"
                            )
                    parent = parent.parent
            total += size
            if total > self._max_total_bytes:
                raise CheckpointError("checkpoint byte limit exceeded")
            validated_files.append({
                "path": relative,
                "size": size,
                "sha256": digest,
                "object_key": key,
                "mode": mode,
            })
            blob_keys.add(key)
        manifest_total = manifest.get("total_bytes")
        if (
            isinstance(manifest_total, bool)
            or not isinstance(manifest_total, int)
            or manifest_total != total
        ):
            raise CheckpointError("checkpoint total size mismatch")
        manifest_objects = manifest.get("total_objects")
        if (
            schema_version == _SCHEMA_VERSION
            and manifest_objects is not None
            and (
                isinstance(manifest_objects, bool)
                or not isinstance(manifest_objects, int)
                or manifest_objects
                != len(directories) + len(validated_files)
            )
        ):
            raise CheckpointError("checkpoint object count mismatch")
        return directories, validated_files, blob_keys

    def _checkpoint_set_key(self, job_id: str, generation: str) -> str:
        generation = _safe_component(
            generation, label="checkpoint set generation",
        )
        return (
            f"{self._job_root(job_id)}/checkpoint-sets/"
            f"{generation}/COMMITTED.json"
        )

    def _validate_checkpoint_set_manifest(
        self,
        manifest: dict[str, Any],
        *,
        source_job_id: str,
        generation: str,
    ) -> list[dict[str, Any]]:
        shards = manifest.get("shards")
        metadata = manifest.get("metadata")
        if (
            manifest.get("schema_version") != _CHECKPOINT_SET_SCHEMA_VERSION
            or manifest.get("kind") != "checkpoint-set"
            or manifest.get("job_id") != source_job_id
            or manifest.get("generation") != generation
            or not isinstance(shards, list)
            or not shards
            or len(shards) > self._max_objects
            or not isinstance(metadata, dict)
            or not isinstance(manifest.get("committed_at"), str)
            or not manifest["committed_at"]
        ):
            raise CheckpointError("checkpoint set identity mismatch")
        self._parse_committed_at(
            manifest["committed_at"], label="checkpoint set manifest",
        )
        seen: set[str] = set()
        validated: list[dict[str, Any]] = []
        total = 0
        total_objects = 0
        for raw in shards:
            if not isinstance(raw, dict):
                raise CheckpointError("invalid checkpoint set shard entry")
            namespace = _safe_component(
                raw.get("worker_namespace"), label="worker namespace",
            )
            shard_generation = _safe_component(
                raw.get("generation"), label="checkpoint generation",
            )
            manifest_sha256 = raw.get("manifest_sha256")
            shard_bytes = raw.get("total_bytes")
            shard_objects = raw.get("total_objects")
            if (
                namespace in seen
                or not isinstance(manifest_sha256, str)
                or _SHA256.fullmatch(manifest_sha256) is None
                or isinstance(shard_bytes, bool)
                or not isinstance(shard_bytes, int)
                or shard_bytes < 0
                or shard_bytes > self._max_total_bytes
                or (
                    shard_objects is not None
                    and (
                        isinstance(shard_objects, bool)
                        or not isinstance(shard_objects, int)
                        or shard_objects < 0
                        or shard_objects > self._max_objects
                    )
                )
            ):
                raise CheckpointError("invalid checkpoint set shard entry")
            seen.add(namespace)
            total += shard_bytes
            if shard_objects is not None:
                total_objects += shard_objects
            validated.append({
                "worker_namespace": namespace,
                "generation": shard_generation,
                "manifest_sha256": manifest_sha256,
                "total_bytes": shard_bytes,
                "total_objects": shard_objects,
            })
        declared_total = manifest.get("total_bytes")
        if (
            isinstance(declared_total, bool)
            or not isinstance(declared_total, int)
            or declared_total != total
        ):
            raise CheckpointError("checkpoint set total size mismatch")
        declared_objects = manifest.get("total_objects")
        missing_objects = any(
            shard["total_objects"] is None
            for shard in validated
        )
        if (
            (declared_objects is None and not missing_objects)
            or (
                declared_objects is not None
                and (
                    missing_objects
                    or isinstance(declared_objects, bool)
                    or not isinstance(declared_objects, int)
                    or declared_objects != total_objects
                )
            )
        ):
            raise CheckpointError(
                "checkpoint set total object count mismatch"
            )
        return validated

    def _checkpoint_set_records(
        self,
        source_job_id: str,
        *,
        deadline_monotonic: float | None = None,
        cancel_event: threading.Event | None = None,
    ) -> list[dict[str, Any]]:
        prefix = f"{self._job_root(source_job_id)}/checkpoint-sets/"

        def matches(key: str) -> bool:
            suffix = key.removeprefix(prefix)
            parts = suffix.split("/")
            return (
                key.startswith(prefix)
                and len(parts) == 2
                and parts[1] == "COMMITTED.json"
                and _SAFE_COMPONENT.fullmatch(parts[0]) is not None
            )

        keys = self._list_matching_keys(
            prefix=prefix,
            matcher=matches,
            label="checkpoint set",
            limit=self._max_checkpoint_sets,
            deadline_monotonic=deadline_monotonic,
            cancel_event=cancel_event,
        )
        records: list[dict[str, Any]] = []
        for key in keys:
            generation = key.removeprefix(prefix).split("/", 1)[0]
            manifest, _raw, manifest_sha256 = self._decode_json_object(
                key,
                label="checkpoint set manifest",
                deadline_monotonic=deadline_monotonic,
                cancel_event=cancel_event,
            )
            shards = self._validate_checkpoint_set_manifest(
                manifest,
                source_job_id=source_job_id,
                generation=generation,
            )
            records.append({
                "key": key,
                "generation": generation,
                "manifest": manifest,
                "manifest_sha256": manifest_sha256,
                "shards": shards,
            })
        if records:
            inventory = self._shard_manifest_inventory(
                source_job_id,
                deadline_monotonic=deadline_monotonic,
                cancel_event=cancel_event,
            )
            self._verify_set_references(records, inventory)
            for record in records:
                normalized_shards: list[dict[str, Any]] = []
                total_objects = 0
                for shard in record["shards"]:
                    normalized = dict(shard)
                    stored = inventory[(
                        normalized["worker_namespace"],
                        normalized["generation"],
                    )]
                    # The immutable shard manifest is authoritative. Legacy
                    # set markers omitted this field; modern markers must
                    # match it exactly (verified above). Always normalize from
                    # the reference so resource admission cannot trust an
                    # underreported set-level count.
                    normalized["total_objects"] = int(
                        stored["manifest"]["total_objects"]
                    )
                    total_objects += normalized["total_objects"]
                    normalized_shards.append(normalized)
                manifest = dict(record["manifest"])
                manifest["shards"] = normalized_shards
                manifest["total_objects"] = total_objects
                record["manifest"] = manifest
                record["shards"] = normalized_shards
        return records

    def _resolve_checkpoint_set_unlocked(
        self,
        *,
        source_job_id: str,
        generation: str,
        deadline_monotonic: float | None = None,
        cancel_event: threading.Event | None = None,
    ) -> dict[str, Any]:
        records = self._checkpoint_set_records(
            source_job_id,
            deadline_monotonic=deadline_monotonic,
            cancel_event=cancel_event,
        )
        if generation:
            selected = _safe_component(
                generation, label="checkpoint set generation",
            )
            matches = [
                record for record in records
                if record["generation"] == selected
            ]
            if not matches:
                raise CheckpointError("checkpoint set generation not found")
            return matches[0]["manifest"]
        if not records:
            raise CheckpointError("no committed checkpoint set found")
        selected_record = max(
            records,
            key=lambda record: (
                self._parse_committed_at(
                    record["manifest"]["committed_at"],
                    label="checkpoint set manifest",
                ),
                record["generation"],
            ),
        )
        return selected_record["manifest"]

    def resolve_checkpoint_set(
        self,
        *,
        source_job_id: str,
        generation: str = "",
        deadline_monotonic: float | None = None,
        cancel_event: threading.Event | None = None,
    ) -> dict[str, Any]:
        """Return the complete latest or explicitly selected Job checkpoint set."""

        with self._job_lock(source_job_id).read(
            deadline_monotonic=deadline_monotonic,
            cancel_event=cancel_event,
        ):
            return self._resolve_checkpoint_set_unlocked(
                source_job_id=source_job_id,
                generation=generation,
                deadline_monotonic=deadline_monotonic,
                cancel_event=cancel_event,
            )

    def _shard_manifest_inventory(
        self,
        job_id: str,
        *,
        deadline_monotonic: float | None = None,
        cancel_event: threading.Event | None = None,
    ) -> dict[tuple[str, str], dict[str, Any]]:
        prefix = f"{self._job_root(job_id)}/workers/"

        def matches(key: str) -> bool:
            suffix = key.removeprefix(prefix)
            parts = suffix.split("/")
            return (
                key.startswith(prefix)
                and len(parts) == 4
                and parts[1] == "checkpoints"
                and parts[3] == "COMMITTED.json"
                and _SAFE_COMPONENT.fullmatch(parts[0]) is not None
                and _SAFE_COMPONENT.fullmatch(parts[2]) is not None
            )

        keys = self._list_matching_keys(
            prefix=prefix,
            matcher=matches,
            label="checkpoint shard manifest",
            limit=self._max_generations,
            deadline_monotonic=deadline_monotonic,
            cancel_event=cancel_event,
        )
        inventory: dict[tuple[str, str], dict[str, Any]] = {}
        for key in keys:
            parts = key.removeprefix(prefix).split("/")
            namespace, generation = parts[0], parts[2]
            manifest, _raw, manifest_sha256 = self._decode_json_object(
                key,
                label="checkpoint manifest",
                deadline_monotonic=deadline_monotonic,
                cancel_event=cancel_event,
            )
            self._validate_manifest_identity(
                manifest,
                source_job_id=job_id,
                worker_namespace=namespace,
            )
            directories, files, blob_keys = (
                self._validated_manifest_entries(
                    manifest,
                    source_job_id=job_id,
                    worker_namespace=namespace,
                )
            )
            if manifest.get("total_objects") is None:
                manifest = dict(manifest)
                manifest["total_objects"] = (
                    len(directories) + len(files)
                )
            if manifest.get("generation") != generation:
                raise CheckpointError(
                    "checkpoint generation identity mismatch"
                )
            reference = (namespace, generation)
            if reference in inventory:
                raise CheckpointError("duplicate checkpoint shard manifest")
            inventory[reference] = {
                "key": key,
                "manifest": manifest,
                "manifest_sha256": manifest_sha256,
                "blob_keys": blob_keys,
            }
        return inventory

    @staticmethod
    def _verify_set_references(
        records: list[dict[str, Any]],
        inventory: dict[tuple[str, str], dict[str, Any]],
    ) -> None:
        for record in records:
            for shard in record["shards"]:
                reference = (
                    shard["worker_namespace"],
                    shard["generation"],
                )
                stored = inventory.get(reference)
                if (
                    stored is None
                    or stored["manifest_sha256"]
                    != shard["manifest_sha256"]
                    or stored["manifest"].get("total_bytes")
                    != shard["total_bytes"]
                    or (
                        shard.get("total_objects") is not None
                        and stored["manifest"].get("total_objects")
                        != shard["total_objects"]
                    )
                ):
                    raise CheckpointError(
                        "checkpoint set references a missing or changed "
                        "shard manifest"
                    )

    def _stable_blob_keys(
        self,
        job_id: str,
        *,
        deadline_monotonic: float | None = None,
        cancel_event: threading.Event | None = None,
    ) -> list[str]:
        prefix = f"{self._job_root(job_id)}/checkpoint-blobs/"

        def matches(key: str) -> bool:
            suffix = key.removeprefix(prefix)
            return (
                key.startswith(prefix)
                and "/" not in suffix
                and _SHA256.fullmatch(suffix) is not None
            )

        return self._list_matching_keys(
            prefix=prefix,
            matcher=matches,
            label="checkpoint blob",
            limit=self._max_gc_objects,
            deadline_monotonic=deadline_monotonic,
            cancel_event=cancel_event,
        )

    def _delete_keys(
        self,
        keys: set[str] | list[str],
        *,
        deadline_monotonic: float | None = None,
        cancel_event: threading.Event | None = None,
    ) -> None:
        ordered = sorted(set(keys))
        if len(ordered) > self._max_gc_objects:
            raise CheckpointError("checkpoint garbage collection limit exceeded")
        for start in range(0, len(ordered), _S3_DELETE_BATCH):
            self._check_deadline(deadline_monotonic, cancel_event)
            batch = ordered[start : start + _S3_DELETE_BATCH]
            if not batch:
                continue
            try:
                response = self._s3().delete_objects(
                    Bucket=self._bucket,
                    Delete={
                        "Objects": [{"Key": key} for key in batch],
                        "Quiet": True,
                    },
                )
            except Exception as exc:  # noqa: BLE001
                raise CheckpointError(
                    "checkpoint garbage collection failed"
                ) from exc
            errors = response.get("Errors") if isinstance(response, dict) else None
            if errors:
                raise CheckpointError(
                    "checkpoint garbage collection was incomplete"
                )

    def _apply_retention(
        self,
        *,
        job_id: str,
        current_generation: str,
        keep_last_n: int,
        records: list[dict[str, Any]],
        inventory: dict[tuple[str, str], dict[str, Any]],
        deadline_monotonic: float | None = None,
        cancel_event: threading.Event | None = None,
    ) -> None:
        self._check_deadline(deadline_monotonic, cancel_event)
        ordered = sorted(
            records,
            key=lambda record: (
                self._parse_committed_at(
                    record["manifest"]["committed_at"],
                    label="checkpoint set manifest",
                ),
                record["generation"],
            ),
        )
        retained = ordered[-keep_last_n:]
        if not any(
            record["generation"] == current_generation
            for record in retained
        ):
            retained = (
                retained[-(keep_last_n - 1) :] if keep_last_n > 1 else []
            )
            retained.append(next(
                record for record in records
                if record["generation"] == current_generation
            ))
        retained_generations = {
            record["generation"] for record in retained
        }
        pruned = [
            record for record in records
            if record["generation"] not in retained_generations
        ]
        retained_references = {
            (
                shard["worker_namespace"],
                shard["generation"],
            )
            for record in retained
            for shard in record["shards"]
        }
        pruned_references = {
            (
                shard["worker_namespace"],
                shard["generation"],
            )
            for record in pruned
            for shard in record["shards"]
        }
        all_set_references = retained_references | pruned_references
        orphan_references = set(inventory) - all_set_references
        current_record = next(
            record for record in records
            if record["generation"] == current_generation
        )
        current_committed_at = self._parse_committed_at(
            current_record["manifest"]["committed_at"],
            label="checkpoint set manifest",
        )
        protected_orphans: set[tuple[str, str]] = set()
        older_orphans: dict[str, list[tuple[str, str]]] = {}
        for reference in orphan_references:
            manifest = inventory[reference]["manifest"]
            committed_at = self._parse_committed_at(
                manifest.get("committed_at"),
                label="checkpoint manifest",
            )
            if committed_at > current_committed_at:
                # A separate producer may have completed this shard after the
                # set snapshot. Never collect a provably future generation.
                protected_orphans.add(reference)
            else:
                older_orphans.setdefault(reference[0], []).append(reference)
        for references in older_orphans.values():
            protected_orphans.add(max(
                references,
                key=lambda reference: (
                    self._parse_committed_at(
                        inventory[reference]["manifest"].get("committed_at"),
                        label="checkpoint manifest",
                    ),
                    reference[1],
                ),
            ))
        stale_orphans = orphan_references - protected_orphans
        removed_references = (
            (pruned_references - retained_references) | stale_orphans
        )
        surviving_inventory = {
            reference: value
            for reference, value in inventory.items()
            if reference not in removed_references
        }
        protected_blobs = {
            blob_key
            for value in surviving_inventory.values()
            for blob_key in value["blob_keys"]
        }
        candidate_blobs = {
            blob_key
            for reference in removed_references
            for blob_key in inventory[reference]["blob_keys"]
            if blob_key not in protected_blobs
        }
        stable_orphans = set(
            self._stable_blob_keys(
                job_id, deadline_monotonic=deadline_monotonic,
                cancel_event=cancel_event,
            )
        ) - protected_blobs
        # Remove discoverability before data. A crash can leak bytes, but can
        # never leave a visible retained set pointing at deleted data.
        self._delete_keys(
            {record["key"] for record in pruned},
            deadline_monotonic=deadline_monotonic,
            cancel_event=cancel_event,
        )
        self._delete_keys(
            {
                inventory[reference]["key"]
                for reference in removed_references
            },
            deadline_monotonic=deadline_monotonic,
            cancel_event=cancel_event,
        )
        self._delete_keys(
            candidate_blobs | stable_orphans,
            deadline_monotonic=deadline_monotonic,
            cancel_event=cancel_event,
        )

    def prune_incomplete_generations(
        self,
        *,
        job_id: str,
        keep_per_shard: int = 3,
        deadline_monotonic: float | None = None,
        cancel_event: threading.Event | None = None,
    ) -> int:
        """Bound shard generations that have never entered a complete set.

        A permanently failing shard must not let every healthy shard append
        manifests and blobs forever. Complete-set references are immutable;
        among unreferenced generations retain only a small newest window per
        shard so temporarily lagging peers can still catch up.
        """

        if (
            isinstance(keep_per_shard, bool)
            or not isinstance(keep_per_shard, int)
            or keep_per_shard <= 0
            or keep_per_shard > 32
        ):
            raise CheckpointError(
                "invalid incomplete checkpoint retention"
            )
        with self._job_lock(job_id).write(
            deadline_monotonic=deadline_monotonic,
            cancel_event=cancel_event,
        ):
            self._check_deadline(deadline_monotonic, cancel_event)
            records = self._checkpoint_set_records(
                job_id,
                deadline_monotonic=deadline_monotonic,
                cancel_event=cancel_event,
            )
            inventory = self._shard_manifest_inventory(
                job_id,
                deadline_monotonic=deadline_monotonic,
                cancel_event=cancel_event,
            )
            self._verify_set_references(records, inventory)
            referenced = {
                (
                    shard["worker_namespace"],
                    shard["generation"],
                )
                for record in records
                for shard in record["shards"]
            }
            grouped: dict[str, list[tuple[str, str]]] = {}
            for reference in set(inventory) - referenced:
                grouped.setdefault(reference[0], []).append(reference)
            stale: set[tuple[str, str]] = set()
            for references in grouped.values():
                ordered = sorted(
                    references,
                    key=lambda reference: (
                        self._parse_committed_at(
                            inventory[reference]["manifest"].get(
                                "committed_at"
                            ),
                            label="checkpoint manifest",
                        ),
                        reference[1],
                    ),
                )
                stale.update(ordered[:-keep_per_shard])
            if not stale:
                return 0
            surviving_inventory = {
                reference: value
                for reference, value in inventory.items()
                if reference not in stale
            }
            protected_blobs = {
                blob_key
                for value in surviving_inventory.values()
                for blob_key in value["blob_keys"]
            }
            candidate_blobs = {
                blob_key
                for reference in stale
                for blob_key in inventory[reference]["blob_keys"]
                if blob_key not in protected_blobs
            }
            # Hide stale generations before deleting their unreferenced data.
            self._delete_keys({
                inventory[reference]["key"]
                for reference in stale
            },
                deadline_monotonic=deadline_monotonic,
                cancel_event=cancel_event,
            )
            self._delete_keys(
                candidate_blobs,
                deadline_monotonic=deadline_monotonic,
                cancel_event=cancel_event,
            )
            return len(stale)

    def publish_checkpoint_set(
        self,
        *,
        job_id: str,
        shard_generations: dict[str, str],
        generation: str | None = None,
        metadata: dict[str, Any] | None = None,
        keep_last_n: int = 3,
        deadline_monotonic: float | None = None,
        cancel_event: threading.Event | None = None,
    ) -> dict[str, Any]:
        """Atomically publish a complete Job-level selection of shard manifests."""

        if (
            isinstance(keep_last_n, bool)
            or not isinstance(keep_last_n, int)
            or keep_last_n <= 0
            or keep_last_n > self._max_checkpoint_sets
        ):
            raise CheckpointError("invalid checkpoint set retention")
        if (
            not isinstance(shard_generations, dict)
            or not shard_generations
            or len(shard_generations) > self._max_objects
        ):
            raise CheckpointError("invalid checkpoint set shard mapping")
        generation = _safe_component(
            generation or _new_generation(),
            label="checkpoint set generation",
        )
        normalized_shards = sorted(
            (
                _safe_component(namespace, label="worker namespace"),
                _safe_component(
                    shard_generation, label="checkpoint generation",
                ),
            )
            for namespace, shard_generation in shard_generations.items()
        )
        normalized_metadata = self._normalize_metadata(
            metadata, label="checkpoint set",
        )

        with self._job_lock(job_id).write(
            deadline_monotonic=deadline_monotonic,
            cancel_event=cancel_event,
        ):
            self._check_deadline(deadline_monotonic, cancel_event)
            existing_records = self._checkpoint_set_records(
                job_id,
                deadline_monotonic=deadline_monotonic,
                cancel_event=cancel_event,
            )
            if (
                len(existing_records) >= self._max_checkpoint_sets
                and keep_last_n >= self._max_checkpoint_sets
            ):
                raise CheckpointError("checkpoint set listing limit exceeded")
            inventory = self._shard_manifest_inventory(
                job_id,
                deadline_monotonic=deadline_monotonic,
                cancel_event=cancel_event,
            )
            self._verify_set_references(existing_records, inventory)
            committed_namespaces = {
                namespace
                for namespace, shard_generation in normalized_shards
                if (namespace, shard_generation) in inventory
            }
            if len(committed_namespaces) != len(normalized_shards):
                raise IncompleteCheckpointSetError(
                    "checkpoint set references an uncommitted shard",
                    committed_namespaces=committed_namespaces,
                )
            shards: list[dict[str, Any]] = []
            total_bytes = 0
            total_objects = 0
            for namespace, shard_generation in normalized_shards:
                reference = (namespace, shard_generation)
                stored = inventory.get(reference)
                if stored is None:  # pragma: no cover - guarded above
                    raise CheckpointError(
                        "checkpoint inventory changed while publishing"
                    )
                shard_bytes = stored["manifest"].get("total_bytes")
                shard_objects = stored["manifest"].get(
                    "total_objects",
                )
                if (
                    isinstance(shard_bytes, bool)
                    or not isinstance(shard_bytes, int)
                    or shard_bytes < 0
                    or shard_bytes > self._max_total_bytes
                ):
                    raise CheckpointError(
                        "checkpoint shard total size is invalid"
                    )
                if (
                    isinstance(shard_objects, bool)
                    or not isinstance(shard_objects, int)
                    or shard_objects < 0
                    or shard_objects > self._max_objects
                ):
                    raise CheckpointError(
                        "checkpoint shard total object count is invalid"
                    )
                total_bytes += shard_bytes
                total_objects += shard_objects
                shards.append({
                    "worker_namespace": namespace,
                    "generation": shard_generation,
                    "manifest_sha256": stored["manifest_sha256"],
                    "total_bytes": shard_bytes,
                    "total_objects": shard_objects,
                })
            manifest: dict[str, Any] = {
                "schema_version": _CHECKPOINT_SET_SCHEMA_VERSION,
                "kind": "checkpoint-set",
                "job_id": job_id,
                "generation": generation,
                "shards": shards,
                "total_bytes": total_bytes,
                "total_objects": total_objects,
                "committed_at": datetime.now(timezone.utc).isoformat(),
                "metadata": normalized_metadata,
            }
            payload = self._json_payload(
                manifest, label="checkpoint set manifest",
            )
            key = self._checkpoint_set_key(job_id, generation)
            published = self._put_committed(
                key=key,
                payload=payload,
                duplicate_message=(
                    "checkpoint set generation is already committed"
                ),
                allow_existing=True,
                deadline_monotonic=deadline_monotonic,
                cancel_event=cancel_event,
            )
            if published:
                record = {
                    "key": key,
                    "generation": generation,
                    "manifest": manifest,
                    "manifest_sha256": hashlib.sha256(payload).hexdigest(),
                    "shards": shards,
                }
                records = [*existing_records, record]
                returned_manifest = manifest
            else:
                matches = [
                    record for record in existing_records
                    if record["generation"] == generation
                ]
                if len(matches) != 1:
                    raise CheckpointError(
                        "checkpoint set generation appeared concurrently; "
                        "retry resolution is required"
                    )
                record = matches[0]
                comparable_keys = (
                    "schema_version",
                    "kind",
                    "job_id",
                    "generation",
                    "shards",
                    "total_bytes",
                    "total_objects",
                    "metadata",
                )
                if any(
                    record["manifest"].get(field) != manifest.get(field)
                    for field in comparable_keys
                ):
                    raise CheckpointError(
                        "checkpoint set generation is already committed "
                        "with different content"
                    )
                records = existing_records
                returned_manifest = record["manifest"]
            self._verify_set_references(records, inventory)
            try:
                self._apply_retention(
                    job_id=job_id,
                    current_generation=generation,
                    keep_last_n=keep_last_n,
                    records=records,
                    inventory=inventory,
                    deadline_monotonic=deadline_monotonic,
                    cancel_event=cancel_event,
                )
            except Exception:  # noqa: BLE001
                # COMMITTED is already visible and fully reference-verified.
                # Retention is maintenance: never report a durable checkpoint as
                # failed merely because bounded garbage collection needs retry.
                logger.warning(
                    "checkpoint retention deferred for %s generation %s",
                    job_id,
                    generation,
                    exc_info=True,
                )
            return returned_manifest

    def _write_restored_object(
        self,
        *,
        key: str,
        destination: Path,
        expected_size: int,
        expected_sha256: str | None,
        mode: int = 0o600,
        deadline_monotonic: float | None = None,
        cancel_event: threading.Event | None = None,
    ) -> None:
        self._check_deadline(deadline_monotonic, cancel_event)
        try:
            response = self._s3().get_object(
                Bucket=self._bucket, Key=key,
            )
            body = response["Body"]
        except Exception as exc:  # noqa: BLE001
            raise CheckpointError(
                f"cannot read checkpoint object: {key}"
            ) from exc
        temporary: Path | None = None
        try:
            try:
                declared = int(response.get("ContentLength"))
            except Exception as exc:  # noqa: BLE001
                raise CheckpointError(
                    f"checkpoint object size is invalid: {key}"
                ) from exc
            if declared != expected_size:
                raise CheckpointError(
                    f"checkpoint object size mismatch: {key}"
                )
            destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            temporary = destination.with_name(
                f".{destination.name}.part-{uuid.uuid4().hex}"
            )
            digest = hashlib.sha256()
            consumed = 0
            descriptor = os.open(
                temporary,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                0o600,
            )
            try:
                with os.fdopen(descriptor, "wb") as output:
                    descriptor = -1
                    while True:
                        self._check_deadline(
                            deadline_monotonic, cancel_event,
                        )
                        chunk = body.read(_READ_CHUNK)
                        if not chunk:
                            break
                        consumed += len(chunk)
                        if consumed > expected_size:
                            raise CheckpointError(
                                f"checkpoint object grew while reading: {key}"
                            )
                        output.write(chunk)
                        digest.update(chunk)
                    output.flush()
                    os.fsync(output.fileno())
            finally:
                if descriptor >= 0:
                    os.close(descriptor)
            if consumed != expected_size:
                raise CheckpointError(
                    f"checkpoint object size mismatch: {key}"
                )
            if (
                expected_sha256 is not None
                and digest.hexdigest() != expected_sha256
            ):
                raise CheckpointError(
                    f"checkpoint object checksum mismatch: {key}"
                )
            os.chmod(temporary, mode)
            os.replace(temporary, destination)
        finally:
            close = getattr(body, "close", None)
            if callable(close):
                close()
            if temporary is not None:
                try:
                    os.unlink(temporary)
                except FileNotFoundError:
                    pass

    @staticmethod
    def _remove_partial_restore(
        destination: Path,
        original: BaseException,
    ) -> None:
        try:
            shutil.rmtree(destination)
        except FileNotFoundError:
            return
        except BaseException as cleanup:
            raise BaseExceptionGroup(
                "checkpoint restore failed and partial-tree cleanup failed",
                [original, cleanup],
            )

    def restore_checkpoint(
        self,
        *,
        source_job_id: str,
        worker_namespace: str,
        destination: Path,
        paths: list[str],
        generation: str = "",
        expected_metadata: dict[str, Any] | None = None,
        expected_manifest_sha256: str | None = None,
        checkpoint_set_generation: str | None = None,
        deadline_monotonic: float | None = None,
        cancel_event: threading.Event | None = None,
    ) -> dict[str, Any]:
        """Restore one committed generation into a new private staging tree."""

        normalized_paths = self._normalize_paths(paths)
        destination = Path(destination)
        if destination.exists():
            raise CheckpointError("checkpoint restore destination already exists")
        with self._job_lock(source_job_id).read(
            deadline_monotonic=deadline_monotonic,
            cancel_event=cancel_event,
        ):
            self._check_deadline(deadline_monotonic, cancel_event)
            selected_generation = generation
            selected_sha256 = expected_manifest_sha256
            if checkpoint_set_generation is not None:
                checkpoint_set = self._resolve_checkpoint_set_unlocked(
                    source_job_id=source_job_id,
                    generation=checkpoint_set_generation,
                    deadline_monotonic=deadline_monotonic,
                    cancel_event=cancel_event,
                )
                matching = [
                    shard for shard in checkpoint_set["shards"]
                    if shard["worker_namespace"] == worker_namespace
                ]
                if len(matching) != 1:
                    raise CheckpointError(
                        "checkpoint set does not contain the requested shard"
                    )
                shard = matching[0]
                if (
                    selected_generation
                    and selected_generation != shard["generation"]
                ):
                    raise CheckpointError(
                        "checkpoint set shard generation mismatch"
                    )
                if (
                    selected_sha256 is not None
                    and selected_sha256 != shard["manifest_sha256"]
                ):
                    raise CheckpointError(
                        "checkpoint set shard hash mismatch"
                    )
                selected_generation = shard["generation"]
                selected_sha256 = shard["manifest_sha256"]

            manifest, _manifest_sha256 = self._committed_manifest(
                source_job_id=source_job_id,
                worker_namespace=worker_namespace,
                generation=selected_generation,
                expected_sha256=selected_sha256,
                deadline_monotonic=deadline_monotonic,
                cancel_event=cancel_event,
            )
            self._validate_manifest_identity(
                manifest,
                source_job_id=source_job_id,
                worker_namespace=worker_namespace,
                paths=normalized_paths,
                expected_metadata=expected_metadata,
            )
            directories, files, _blob_keys = (
                self._validated_manifest_entries(
                    manifest,
                    source_job_id=source_job_id,
                    worker_namespace=worker_namespace,
                )
            )
            manifest_generation = _safe_component(
                manifest.get("generation"),
                label="checkpoint generation",
            )
            if (
                selected_generation
                and manifest_generation != selected_generation
            ):
                raise CheckpointError(
                    "checkpoint generation identity mismatch"
                )

            created = False
            try:
                destination.parent.mkdir(
                    parents=True, exist_ok=True, mode=0o700,
                )
                destination.mkdir(mode=0o700)
                created = True
                for requested in normalized_paths:
                    self._check_deadline(
                        deadline_monotonic, cancel_event,
                    )
                    (destination / requested).mkdir(
                        parents=True, exist_ok=True, mode=0o700,
                    )
                for directory in sorted(
                    directories,
                    key=lambda entry: (
                        len(PurePosixPath(entry["path"]).parts),
                        entry["path"],
                    ),
                ):
                    self._check_deadline(
                        deadline_monotonic, cancel_event,
                    )
                    (destination / directory["path"]).mkdir(
                        parents=True, exist_ok=True, mode=0o700,
                    )
                for entry in files:
                    self._check_deadline(
                        deadline_monotonic, cancel_event,
                    )
                    self._write_restored_object(
                        key=entry["object_key"],
                        destination=destination / entry["path"],
                        expected_size=entry["size"],
                        expected_sha256=entry["sha256"],
                        mode=entry["mode"],
                        deadline_monotonic=deadline_monotonic,
                        cancel_event=cancel_event,
                    )
                for directory in sorted(
                    directories,
                    key=lambda entry: (
                        -len(PurePosixPath(entry["path"]).parts),
                        entry["path"],
                    ),
                ):
                    self._check_deadline(
                        deadline_monotonic, cancel_event,
                    )
                    os.chmod(
                        destination / directory["path"],
                        directory["mode"],
                    )
                return manifest
            except BaseException as exc:
                if created:
                    self._remove_partial_restore(destination, exc)
                raise

    def restore_legacy_collection(
        self,
        *,
        source_job_id: str,
        worker_namespace: str,
        destination: Path,
        paths: list[str],
        deadline_monotonic: float | None = None,
        cancel_event: threading.Event | None = None,
    ) -> dict[str, Any]:
        """Explicitly restore a pre-checkpoint mutable final collection."""

        normalized_paths = self._normalize_paths(paths)
        destination = Path(destination)
        if destination.exists():
            raise CheckpointError("checkpoint restore destination already exists")
        destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        destination.mkdir(mode=0o700)
        worker_root = self._worker_root(source_job_id, worker_namespace)
        manifest_key = (
            f"{worker_root}/_elastic_agent/collection.json"
        )
        try:
            raw_manifest = self._read_object_limited(
                manifest_key,
                limit=self._max_manifest_bytes,
                deadline_monotonic=deadline_monotonic,
                cancel_event=cancel_event,
            )
            manifest = json.loads(raw_manifest.decode("utf-8"))
            if (
                not isinstance(manifest, dict)
                or manifest.get("schema_version") != 1
                or manifest.get("job_id") != source_job_id
                or manifest.get("worker_namespace") != worker_namespace
                or not set(normalized_paths).issubset(
                    set(manifest.get("paths") or [])
                )
            ):
                raise CheckpointError("collection manifest identity mismatch")

            count = 0
            total = 0
            for requested in normalized_paths:
                self._check_deadline(deadline_monotonic, cancel_event)
                prefix = f"{worker_root}/{requested}/"
                for page in self._s3().get_paginator(
                    "list_objects_v2"
                ).paginate(Bucket=self._bucket, Prefix=prefix):
                    self._check_deadline(deadline_monotonic, cancel_event)
                    for item in page.get("Contents") or []:
                        self._check_deadline(
                            deadline_monotonic, cancel_event,
                        )
                        key = str(item.get("Key") or "")
                        if not key.startswith(prefix):
                            raise CheckpointError(
                                "legacy collection key escaped requested prefix"
                            )
                        relative = key.removeprefix(worker_root + "/")
                        relative = _safe_relative_path(
                            relative, label="legacy collection path",
                        )
                        if not _under_any_path(relative, normalized_paths):
                            raise CheckpointError(
                                "legacy collection key escaped requested paths"
                            )
                        try:
                            size = int(item.get("Size"))
                        except (TypeError, ValueError) as exc:
                            raise CheckpointError(
                                "invalid legacy collection object size"
                            ) from exc
                        count += 1
                        total += size
                        if count > self._max_objects:
                            raise CheckpointError(
                                "checkpoint object limit exceeded"
                            )
                        if total > self._max_total_bytes:
                            raise CheckpointError(
                                "checkpoint byte limit exceeded"
                            )
                        self._write_restored_object(
                            key=key,
                            destination=destination / relative,
                            expected_size=size,
                            expected_sha256=None,
                            deadline_monotonic=deadline_monotonic,
                            cancel_event=cancel_event,
                        )
            return manifest
        except BaseException as exc:
            self._remove_partial_restore(destination, exc)
            raise
