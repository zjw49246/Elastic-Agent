"""Batch job REST API — the frontend's Job panel.

Submit a declarative JobSpec (or one referencing uploaded Harness code) and fan
it out across the fleet via the Manager's BatchOrchestrator. Also accepts
Harness code uploads so the "upload code" path has somewhere to land.
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import copy
import errno
import functools
import hashlib
import heapq
import hmac
import json
import logging
import math
import os
import re
import shlex
import shutil
import stat
import struct
import tarfile
import tempfile
import threading
import time
from collections import Counter
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable
from concurrent.futures import ThreadPoolExecutor
from itertools import islice
from pathlib import Path, PurePosixPath
from urllib.parse import urlsplit, urlunsplit

from fastapi import (
    APIRouter,
    Depends,
    Header,
    HTTPException,
    Query,
    Request,
    Response,
)
from fastapi.exceptions import RequestValidationError
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from elastic_agent.api.auth import require_api_key
from elastic_agent.api.body_limit import (
    JOB_SUBMIT_MAX_BODY_BYTES,
    REQUEST_BODY_LIMIT_STATE_KEY,
)
from elastic_agent.core.batch_orchestrator import (
    TERMINAL_WORKER_PHASES,
    JobSpecPersistenceError,
)
from elastic_agent.core.ephemeral_stdin import (
    MAX_EPHEMERAL_STDIN_TTL_SECONDS,
    EphemeralStdinLeaseError,
)
from elastic_agent.core.job_spec import JobSpec
from elastic_agent.core.job_spec_store import (
    JOB_REQUEST_FINGERPRINT_ALGORITHM,
    JOB_REQUEST_FINGERPRINT_SCHEMA,
    load_job_spec_journal,
)
from elastic_agent.core.manager_fleet_driver import sensitive_transport_error
from elastic_agent.core.secure_store import (
    atomic_write_private,
    fsync_directory,
    secure_state_directory,
    tighten_state_file,
)

router = APIRouter(tags=["jobs"], dependencies=[Depends(require_api_key)])
logger = logging.getLogger(__name__)
_submit_lock = asyncio.Lock()
_job_action_locks_guard = threading.Lock()
_SAFE_JOB_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")
_SAFE_RESUME_GENERATION = re.compile(
    r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}"
)
_SAFE_PERSISTED_ACCOUNT_REFERENCE = re.compile(
    r"[A-Za-z0-9][A-Za-z0-9._:@+-]{0,127}"
)
_READ_ONLY_SANDBOX_PROFILE = "ubuntu-agent-docker-sandbox-v1"
_READ_ONLY_SANDBOX_VALIDATION_PROFILE = "ubuntu-agent-docker-v2"
_READ_ONLY_ACCOUNT_AUTH_KINDS = frozenset({"any", "oauth", "agent_api"})
_READ_ONLY_MAX_EXCLUDED_ACCOUNTS = 100
_PERSISTED_JOB_STATES = {
    "prepared", "launching", "running", "suspending", "suspended",
    "succeeded", "failed", "cancelled",
}
_TERMINAL_JOB_STATES = {"suspended", "succeeded", "failed", "cancelled"}
_JOB_ACTION_IDEMPOTENCY_SCHEMA = 1
_JOB_ACTION_IDEMPOTENCY_MAX_BYTES = 4 * 1024
_JOB_INTERRUPT_INTENT_SCHEMA = 1

# Results endpoints must remain bounded even when an S3 prefix or local results
# directory contains unexpectedly many files.  Listing has a higher ceiling;
# downloads additionally cap the uncompressed payload size.
RESULT_LIST_MAX_OBJECTS = 100_000
RESULT_LIST_MAX_METADATA_BYTES = 64 * 1024 * 1024
RESULT_LIST_MAX_SCANNED_ENTRIES = 200_000
RESULT_LIST_MAX_S3_PAGES = 10_000
RESULT_FILE_LIST_MAX_ENTRIES = 500
RESULT_FILE_LIST_MAX_PATH_BYTES = 8 * 1024 * 1024
RESULT_FILE_LIST_MAX_JSON_BYTES = 10 * 1024 * 1024
RESULT_ARCHIVE_MAX_OBJECTS = 10_000
RESULT_ARCHIVE_MAX_BYTES = 10 * 1024 * 1024 * 1024
RESULT_SCORE_MAX_ATTEMPTS = 500
RESULT_SCORE_MAX_BYTES = 2_000_000
RESULT_SCORE_TOTAL_READ_BYTES = 16 * 1024 * 1024
RESULT_SCORE_MAX_ENTRIES = 500
RESULT_SCORE_TEXT_MAX_CHARS = 512
RESULT_SCORE_ABS_MAX = 1_000_000_000_000
JOB_LOG_RESPONSE_MAX_BYTES = 8 * 1024 * 1024
JOB_LOG_LINE_MAX_BYTES = 64 * 1024
JOB_JOURNAL_MAX_BYTES = 32 * 1024 * 1024
JOB_HISTORY_WORKERS = 2
JOB_LOG_READ_WORKERS = 4
JOB_LIST_HISTORY_MAX_SCANNED_ENTRIES = 10_000
JOB_LIST_HISTORY_MAX_NAME_BYTES = 2 * 1024 * 1024
JOB_LIST_HISTORY_MAX_CANDIDATES = 1_000
JOB_LIST_HISTORY_MAX_RETURNED = 500
JOB_LIST_HISTORY_MAX_READ_BYTES = 64 * 1024 * 1024
JOB_LIST_HISTORY_MAX_RESPONSE_BYTES = 16 * 1024 * 1024
JOB_LIST_HISTORY_MAX_LEASES = 10_000
# One detail request is scoped to a single Job.  EIP Jobs currently create at
# most one durable lease per worker, but retain a generous compatibility margin
# while preventing a corrupt binding journal from being fully materialized.
JOB_DETAIL_MAX_RECOVERY_LEASES = 1_000
# The persisted journal itself is capped at 32 MiB.  Redaction replaces short
# values with visible markers and can therefore expand a configuration, so give
# the final serialized snapshot a separate, still-finite response boundary.
JOB_CONFIG_MAX_RESPONSE_BYTES = 64 * 1024 * 1024
JOB_CONFIG_NO_STORE_HEADERS = {
    "Cache-Control": "no-store",
    "Pragma": "no-cache",
}
RESULT_ARCHIVE_STREAM_WORKERS = 4
RESULT_ARCHIVE_BUILD_WORKERS = 2
RESULT_READ_WORKERS = 4
RESULT_SUMMARY_MAX_JOBS = 1_000
RESULT_SUMMARY_MAX_OBJECTS = 100_000
RESULT_SUMMARY_MAX_METADATA_BYTES = 16 * 1024 * 1024
RESULT_SUMMARY_MAX_DIRECTORY_ENTRIES = 5_000
RESULT_SUMMARY_MAX_S3_ROOT_ENTRIES = 10_000
RESULT_SUMMARY_MAX_S3_PAGES = 2_000
RESULT_ARCHIVE_SPOOL_MAX_BYTES = 20 * 1024 * 1024 * 1024
RESULT_ARCHIVE_DISK_SAFETY_BYTES = 512 * 1024 * 1024
RESULT_ARCHIVE_STALE_SECONDS = 24 * 60 * 60
HARNESS_UPLOAD_MAX_BYTES = 1024 * 1024
RUN_BENCHMARK_STDIN_MAGIC = b"RBWORK01"
RUN_BENCHMARK_FRAME_HEADER = struct.Struct(">8sII")
RUN_BENCHMARK_MAX_PUBLIC_BYTES = 64 * 1024
RUN_BENCHMARK_MAX_KEY_BYTES = 8 * 1024
RUN_BENCHMARK_MAX_WALL_SECONDS = 10_800
RUN_BENCHMARK_REPOSITORY = "git@github.com:panjose/Run-Benchmark.git"
RUN_BENCHMARK_HARNESSES = frozenset({
    "codex-api", "claude-code", "kimi-code", "mimo-code", "openhands",
    "openai-chat",
})
_RUN_BENCHMARK_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")
_RUN_BENCHMARK_DIGEST = re.compile(r"[0-9a-f]{64}")
_RUN_BENCHMARK_COMMIT = re.compile(r"[0-9a-f]{40,64}")


class RunBenchmarkJobRequest(BaseModel):
    """Narrow trusted bridge from Run-Benchmark into dynamic fleet Jobs."""

    model_config = ConfigDict(extra="forbid", hide_input_in_errors=True)

    run_id: str = Field(min_length=1, max_length=128)
    resolved_commit: str = Field(min_length=40, max_length=64)
    worker_release_digest: str = Field(min_length=64, max_length=64)
    input_digest: str = Field(min_length=64, max_length=64)
    input_uri: str = Field(min_length=1, max_length=1024)
    instance_digest: str = Field(min_length=64, max_length=64)
    harness_id: str = Field(min_length=1, max_length=128)
    wall_time_seconds: int = Field(ge=1, le=RUN_BENCHMARK_MAX_WALL_SECONDS)
    credential_frame: str = Field(
        repr=False,
        min_length=1,
        max_length=400_000,
    )

# Archive producers perform long-lived blocking S3 reads. Keep them off
# asyncio's shared default executor so concurrent downloads cannot starve
# unrelated lifecycle, log, and collection work that also uses ``to_thread``.
_RESULT_ARCHIVE_EXECUTOR = ThreadPoolExecutor(
    max_workers=RESULT_ARCHIVE_STREAM_WORKERS,
    thread_name_prefix="result-archive",
)
_RESULT_ARCHIVE_BUILD_EXECUTOR = ThreadPoolExecutor(
    max_workers=RESULT_ARCHIVE_BUILD_WORKERS,
    thread_name_prefix="result-archive-build",
)
_RESULT_ARCHIVE_CLEANUP_EXECUTOR = ThreadPoolExecutor(
    max_workers=RESULT_ARCHIVE_STREAM_WORKERS,
    thread_name_prefix="result-archive-close",
)
_RESULT_ARCHIVE_FILE_EXECUTOR = ThreadPoolExecutor(
    max_workers=RESULT_ARCHIVE_STREAM_WORKERS,
    thread_name_prefix="result-archive-file",
)
_RESULT_READ_EXECUTOR = ThreadPoolExecutor(
    max_workers=RESULT_READ_WORKERS,
    thread_name_prefix="result-read",
)
_JOB_HISTORY_EXECUTOR = ThreadPoolExecutor(
    max_workers=JOB_HISTORY_WORKERS,
    thread_name_prefix="job-history",
)
_JOB_LOG_READ_EXECUTOR = ThreadPoolExecutor(
    max_workers=JOB_LOG_READ_WORKERS,
    thread_name_prefix="job-log-read",
)


class _ResultOperationPermit:
    """Exactly-once token for one fail-fast results operation."""

    def __init__(self, admission: _ResultOperationAdmission) -> None:
        self._admission = admission
        self._released = False
        self._lock = threading.Lock()

    def release(self) -> None:
        with self._lock:
            if self._released:
                return
            self._released = True
        self._admission._release()


class _ResultOperationAdmission:
    """Bound active work without accumulating coroutine waiter queues."""

    def __init__(self, limit: int) -> None:
        if limit <= 0:
            raise ValueError("result operation limit must be positive")
        self._limit = limit
        self._active = 0
        self._lock = threading.Lock()

    def try_acquire(self) -> _ResultOperationPermit | None:
        with self._lock:
            if self._active >= self._limit:
                return None
            self._active += 1
        return _ResultOperationPermit(self)

    def _release(self) -> None:
        with self._lock:
            if self._active <= 0:
                raise RuntimeError("result operation admission underflow")
            self._active -= 1

    @property
    def active(self) -> int:
        with self._lock:
            return self._active


_RESULT_ARCHIVE_STREAM_ADMISSION = _ResultOperationAdmission(
    RESULT_ARCHIVE_STREAM_WORKERS
)
_RESULT_ARCHIVE_BUILD_ADMISSION = _ResultOperationAdmission(
    RESULT_ARCHIVE_BUILD_WORKERS
)
_RESULT_READ_ADMISSION = _ResultOperationAdmission(RESULT_READ_WORKERS)
_JOB_HISTORY_ADMISSION = _ResultOperationAdmission(JOB_HISTORY_WORKERS)
_JOB_LOG_READ_ADMISSION = _ResultOperationAdmission(JOB_LOG_READ_WORKERS)
_RESULT_ARCHIVE_SPOOL_LOCK = threading.Lock()
_RESULT_ARCHIVE_SPOOL_RESERVED = 0
_RESULT_ARCHIVE_TEMP_RESERVATIONS: dict[Path, int] = {}
_RESULT_ARCHIVE_STALE_CLEANED = False

# ``ETag`` is retained from ListObjectsV2 so every later GET can use
# ``IfMatch``. A backend that cannot provide immutable object identity is not a
# safe source for an archive assembled after LIST and therefore fails closed.
S3ResultObject = tuple[str, int, str, str]


class S3ResultsUnavailable(RuntimeError):  # noqa: N818
    """The configured authoritative results backend could not be read."""


class ResultsLimitExceeded(RuntimeError):  # noqa: N818
    """A result set is too large to safely enumerate or archive."""


class LocalResultsUnavailable(RuntimeError):  # noqa: N818
    """Manager-local result files changed or became unreadable while archiving."""


class ResultsSpoolUnavailable(RuntimeError):  # noqa: N818
    """The Manager cannot safely reserve temporary archive disk space."""


class RecoveryRunOverrides(BaseModel):
    """The only run fields an operator may change for checkpoint recovery."""

    model_config = ConfigDict(extra="forbid")

    command: str | None = Field(default=None, min_length=1, max_length=65_536)
    timeout: int | None = Field(default=None, ge=60, le=2_592_000)


class RecoveryJobRequest(BaseModel):
    """Server-side recovery request built from one private persisted JobSpec."""

    model_config = ConfigDict(extra="forbid")

    source_job_id: str = Field(min_length=1, max_length=128)
    generation: str = Field(default="", max_length=128)
    run: RecoveryRunOverrides = Field(default_factory=RecoveryRunOverrides)
    ttl_seconds: int | None = Field(default=None, ge=300, le=2_592_000)


class ResumeJobRequest(BaseModel):
    """One-click continuation of one verified suspended checkpoint."""

    model_config = ConfigDict(extra="forbid")

    resume_generation: str = Field(
        min_length=1,
        max_length=128,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$",
    )


def _acquire_result_operation(
    admission: _ResultOperationAdmission,
    *,
    operation: str,
) -> _ResultOperationPermit:
    permit = admission.try_acquire()
    if permit is None:
        raise HTTPException(
            503,
            f"{operation} capacity is currently exhausted",
            headers={"Retry-After": "1"},
        )
    return permit


async def _run_owned_executor(executor, function, *args, **kwargs):
    """Wait for real thread completion before propagating caller cancellation."""

    future = asyncio.get_running_loop().run_in_executor(
        executor,
        functools.partial(function, *args, **kwargs),
    )
    cancellation: asyncio.CancelledError | None = None
    while True:
        try:
            result = await asyncio.shield(future)
            break
        except asyncio.CancelledError as exc:
            if future.cancelled():
                raise
            # A running thread cannot be cancelled. Keep the route's admission
            # token until the callable really exits, then propagate the
            # original request cancellation.
            cancellation = exc
        except BaseException as exc:
            if cancellation is not None:
                raise cancellation from exc
            raise
    if cancellation is not None:
        del result
        raise cancellation
    return result


async def _run_result_read(function, *args, **kwargs):
    return await _run_owned_executor(
        _RESULT_READ_EXECUTOR,
        function,
        *args,
        **kwargs,
    )


def _mgr():
    from elastic_agent.api.app import get_manager
    return get_manager()


def _specs_dir(mgr) -> Path:
    """Where submitted JobSpecs are persisted so they survive a Manager restart
    (the orchestrator's job records are in-memory and lost on restart).

    Do not repair every historical file on each API request: that old helper
    used an unbounded glob. Exact files are tightened immediately before their
    bounded read instead.
    """
    return secure_state_directory(
        Path(mgr.config.registry.path).expanduser().with_name("specs")
    )


def _job_actions_dir(mgr) -> Path:
    """Private durable identities for non-creating Job actions."""

    return secure_state_directory(
        Path(mgr.config.registry.path).expanduser().with_name("job-actions")
    )


def _manager_job_action_lock(mgr) -> asyncio.Lock:
    """Return the one action transaction lock owned by this Manager.

    The lock covers the authoritative journal scan and the interrupt-intent
    commit.  Creating it under a synchronous guard avoids two first requests
    installing different locks before either one reaches its first await.
    """

    with _job_action_locks_guard:
        lock = getattr(mgr, "_api_job_action_lock", None)
        if lock is None:
            lock = asyncio.Lock()
            setattr(mgr, "_api_job_action_lock", lock)
        if not isinstance(lock, asyncio.Lock):
            raise RuntimeError("Manager Job action lock is invalid")
        return lock


async def _settle_owned_job_action(action):
    """Let an authoritative action settle even if its HTTP caller disconnects.

    ``asyncio.shield`` alone is insufficient because it immediately unwinds
    the surrounding transaction lock.  This helper remembers caller
    cancellation, keeps awaiting the owned task, and hands the cancellation
    back only after the caller has verified the resulting journal.
    """

    task = asyncio.create_task(action)
    cancellation: asyncio.CancelledError | None = None
    while True:
        try:
            result = await asyncio.shield(task)
            return result, cancellation
        except asyncio.CancelledError as exc:
            if task.done():
                if task.cancelled():
                    if cancellation is not None:
                        raise cancellation
                    raise
                # The caller's cancellation can race with a successful owned
                # commit.  Preserve that result so the endpoint can verify the
                # journal and publish its rebuildable cache before handing
                # cancellation back to the HTTP stack.
                try:
                    return task.result(), cancellation or exc
                except Exception:
                    logger.exception(
                        "owned Job action failed as its HTTP caller cancelled"
                    )
                    raise cancellation or exc
            if cancellation is None:
                cancellation = exc
        except Exception:
            if cancellation is not None:
                logger.exception(
                    "owned Job action failed after its HTTP caller cancelled"
                )
                raise cancellation
            raise


def _normalize_idempotency_key(
    value: str | None,
    *,
    required: bool = False,
) -> str | None:
    if value is None:
        if required:
            raise HTTPException(400, "Idempotency-Key is required")
        return None
    value = value.strip()
    if not value or len(value) > 200 or any(
        ord(char) < 0x20 for char in value
    ):
        raise HTTPException(400, "invalid Idempotency-Key")
    return value


def _job_action_index_path(
    mgr,
    *,
    operation: str,
    digest: str,
) -> Path:
    if re.fullmatch(r"[0-9a-f]{64}", digest) is None:
        raise ValueError("invalid Job action digest")
    return _job_actions_dir(mgr) / f"{operation}-{digest}.json"


def _read_job_action_index(
    mgr,
    *,
    operation: str,
    digest: str,
) -> str | None:
    """Read the optional digest→Job cache without treating it as authority."""

    path = _job_action_index_path(
        mgr,
        operation=operation,
        digest=digest,
    )
    if not (path.exists() or path.is_symlink()):
        return None
    try:
        payload, _consumed = _read_bounded_json_file(
            path,
            max_bytes=_JOB_ACTION_IDEMPOTENCY_MAX_BYTES,
        )
    except (
        json.JSONDecodeError,
        OSError,
        RecursionError,
        RuntimeError,
        UnicodeDecodeError,
        ValueError,
    ) as exc:
        raise HTTPException(
            500,
            "persisted Job action index is unavailable",
        ) from exc
    expected = {
        "schema": _JOB_ACTION_IDEMPOTENCY_SCHEMA,
        "operation": operation,
        "key_digest": digest,
    }
    if not isinstance(payload.get("job_id"), str):
        raise HTTPException(500, "persisted Job action index is invalid")
    comparable = {key: payload.get(key) for key in expected}
    if not hmac.compare_digest(
        json.dumps(
            comparable,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8"),
        json.dumps(
            expected,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8"),
    ):
        raise HTTPException(500, "persisted Job action index is invalid")
    owner = payload["job_id"]
    if _SAFE_JOB_ID.fullmatch(owner) is None:
        raise HTTPException(500, "persisted Job action index is invalid")
    return owner


def _write_job_action_index(
    mgr,
    *,
    operation: str,
    digest: str,
    job_id: str,
) -> None:
    """Publish a rebuildable cache only after the Job journal is authoritative."""

    path = _job_action_index_path(
        mgr,
        operation=operation,
        digest=digest,
    )
    expected = {
        "schema": _JOB_ACTION_IDEMPOTENCY_SCHEMA,
        "operation": operation,
        "key_digest": digest,
        "job_id": job_id,
    }
    atomic_write_private(
        path,
        json.dumps(expected, ensure_ascii=False, indent=2),
    )


def _authoritative_interrupt_digest(payload: dict) -> str | None:
    """Read the private digest only when its atomic intent is committed."""

    intent = payload.get("interrupt_intent")
    if intent is None:
        return None
    if not isinstance(intent, dict):
        raise ValueError("invalid persisted interrupt intent")
    digest = intent.get("idempotency_digest")
    if (
        intent.get("schema") != _JOB_INTERRUPT_INTENT_SCHEMA
        or not isinstance(digest, str)
        or re.fullmatch(r"[0-9a-f]{64}", digest) is None
    ):
        raise ValueError("invalid persisted interrupt intent")
    summary = payload.get("terminal_summary")
    state = payload.get("submission_state")
    if (
        not isinstance(summary, dict)
        or summary.get("interrupt_requested") is not True
        or (
            state == "suspending"
            and summary.get("state") != "suspending"
        )
        or state not in {
            "suspending",
            "suspended",
            "failed",
            "cancelled",
        }
    ):
        raise ValueError("interrupt intent is not bound to a durable state")
    return digest


def _read_interrupt_intent_index(mgr) -> dict[str, str]:
    """Rebuild the authority index from the current private Job journals.

    Sidecars are written after the atomic journal commit, so a Manager can
    crash or an HTTP coroutine can be cancelled with a committed intent but no
    sidecar.  A loaded-once cache is therefore unsafe: every action transaction
    fully validates every individually bounded journal and only then replaces
    the non-authoritative memory cache.

    There is intentionally no total-file cutoff.  A fixed journal-count limit
    would permanently make interrupts unavailable once enough historical Jobs
    exist.  Each individual journal read remains bounded.
    """

    directory = _specs_dir(mgr)
    index: dict[str, str] = {}
    try:
        entries = os.scandir(directory)
    except FileNotFoundError:
        setattr(mgr, "_api_interrupt_intent_index", {})
        return {}
    with entries:
        for entry in entries:
            if not entry.name.endswith(".json"):
                continue
            job_id = entry.name[:-5]
            if _SAFE_JOB_ID.fullmatch(job_id) is None:
                continue
            try:
                metadata = entry.stat(follow_symlinks=False)
                if (
                    not stat.S_ISREG(metadata.st_mode)
                    or metadata.st_size < 1
                    or metadata.st_size > JOB_JOURNAL_MAX_BYTES
                ):
                    raise ValueError(
                        f"unsafe Job journal while indexing {job_id!r}"
                    )
                # This scan is the idempotency authority, not a best-effort
                # history listing.  Parse every validly named journal so a
                # malformed/unreadable file cannot conceal a committed digest
                # and permit the same key to bind a second Job.
                payload = _read_job_journal(
                    Path(entry.path),
                    job_id,
                )
            except (
                json.JSONDecodeError,
                OSError,
                RecursionError,
                RuntimeError,
                UnicodeDecodeError,
                ValueError,
            ) as exc:
                raise RuntimeError(
                    f"cannot verify interrupt identity journal {job_id!r}"
                ) from exc
            authoritative = _authoritative_interrupt_digest(payload)
            if authoritative is None:
                continue
            previous = index.get(authoritative)
            if previous is not None and previous != job_id:
                raise ValueError(
                    "interrupt Idempotency-Key is bound to multiple Jobs"
                )
            index[authoritative] = job_id
    setattr(mgr, "_api_interrupt_intent_index", index)
    return index


def _load_interrupt_journal_optional(mgr, job_id: str) -> dict | None:
    path = _job_spec_path(mgr, job_id)
    if not _job_journal_exists(path):
        return None
    return _read_job_journal(path, job_id)


def _validate_job_id(job_id: str) -> str:
    """Require the same single-component identifier accepted by the journal."""
    if _SAFE_JOB_ID.fullmatch(job_id) is None:
        raise HTTPException(400, "invalid job_id")
    return job_id


def _job_spec_path(mgr, job_id: str) -> Path:
    return _specs_dir(mgr) / f"{_validate_job_id(job_id)}.json"


def _read_bounded_json_file(
    path: Path,
    *,
    max_bytes: int,
) -> tuple[dict, int]:
    """Read one private regular JSON file without following a replacement link."""

    if max_bytes <= 0:
        raise ValueError("JSON read budget is exhausted")
    tighten_state_file(path)
    descriptor = os.open(
        path,
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0),
    )
    try:
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_size > max_bytes
        ):
            raise ValueError(
                f"JSON file {path.name!r} exceeds its read boundary"
            )
        chunks: list[bytes] = []
        bytes_read = 0
        while bytes_read <= max_bytes:
            chunk = os.read(
                descriptor,
                min(64 * 1024, max_bytes + 1 - bytes_read),
            )
            if not chunk:
                break
            chunks.append(chunk)
            bytes_read += len(chunk)
        if bytes_read > max_bytes:
            raise ValueError(
                f"JSON file {path.name!r} exceeds its read boundary"
            )
    finally:
        os.close(descriptor)
    payload = json.loads(b"".join(chunks).decode("utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"invalid JSON object in {path.name}")
    return payload, bytes_read


def _read_job_journal_sized(
    path: Path,
    expected_job_id: str,
    *,
    max_bytes: int | None = None,
) -> tuple[dict, int]:
    if max_bytes is None:
        max_bytes = JOB_JOURNAL_MAX_BYTES
    payload, bytes_read = _read_bounded_json_file(
        path,
        max_bytes=max_bytes,
    )
    if (
        payload.get("job_id") != expected_job_id
        or not isinstance(payload.get("spec"), dict)
    ):
        raise ValueError(f"invalid JobSpec journal for {expected_job_id!r}")
    return payload, bytes_read


def _read_job_journal(path: Path, expected_job_id: str) -> dict:
    return _read_job_journal_sized(path, expected_job_id)[0]


def _job_journal_exists(path: Path) -> bool:
    """Treat a symlink as present so the bounded no-follow reader rejects it."""

    return path.exists() or path.is_symlink()


def _read_and_redact_job_config(path: Path, job_id: str) -> tuple[dict, dict]:
    """Read, validate, redact, and bound one snapshot off the event loop."""

    payload = _read_job_journal(path, job_id)
    # Do not hand the raw environment values/secret references back across the
    # executor boundary.  Detail rendering needs only lifecycle metadata plus
    # the already-redacted projection.
    raw_spec = payload.pop("spec")
    raw_collect = (
        raw_spec.get("collect")
        if isinstance(raw_spec, dict) else None
    )
    payload["checkpoint_recovery_available"] = bool(
        isinstance(raw_collect, dict)
        and raw_collect.get("checkpoint") is True
    )
    return payload, _redacted_spec(raw_spec)


async def _read_job_journal_for_detail(path: Path, job_id: str) -> tuple[dict, dict]:
    """Build an exact config snapshot on the bounded history executor."""

    try:
        permit = _acquire_result_operation(
            _JOB_HISTORY_ADMISSION,
            operation="Job config read",
        )
    except HTTPException as exc:
        exc.headers = {
            **(exc.headers or {}),
            **JOB_CONFIG_NO_STORE_HEADERS,
        }
        raise
    try:
        return await _run_owned_executor(
            _JOB_HISTORY_EXECUTOR,
            _read_and_redact_job_config,
            path,
            job_id,
        )
    finally:
        # A cancelled request cannot return this permit while its disk read is
        # still occupying one of the dedicated history threads.
        permit.release()


def _load_historical_job_journals(
    directory: Path,
    live_ids: frozenset[str],
) -> dict:
    """Scandir and parse a bounded newest-first historical Job snapshot."""

    candidates: list[tuple[int, str, int]] = []
    scanned = 0
    name_bytes = 0
    truncated = False
    candidate_limit = max(
        JOB_LIST_HISTORY_MAX_CANDIDATES,
        JOB_LIST_HISTORY_MAX_RETURNED,
    )
    try:
        entries = os.scandir(directory)
    except FileNotFoundError:
        return {
            "entries": [],
            "scanned": 0,
            "read_bytes": 0,
            "truncated": False,
        }
    with entries:
        for entry in entries:
            if scanned >= JOB_LIST_HISTORY_MAX_SCANNED_ENTRIES:
                truncated = True
                break
            scanned += 1
            try:
                name_bytes += len(entry.name.encode("utf-8"))
            except UnicodeEncodeError:
                truncated = True
                continue
            if name_bytes > JOB_LIST_HISTORY_MAX_NAME_BYTES:
                truncated = True
                break
            if not entry.name.endswith(".json"):
                continue
            job_id = entry.name[:-5]
            if (
                _SAFE_JOB_ID.fullmatch(job_id) is None
                or job_id in live_ids
            ):
                continue
            try:
                metadata = entry.stat(follow_symlinks=False)
            except OSError:
                continue
            if (
                not stat.S_ISREG(metadata.st_mode)
                or metadata.st_size < 1
                or metadata.st_size > JOB_JOURNAL_MAX_BYTES
            ):
                continue
            candidate = (
                int(metadata.st_mtime_ns),
                entry.name,
                int(metadata.st_size),
            )
            if len(candidates) < candidate_limit:
                heapq.heappush(candidates, candidate)
            elif candidate > candidates[0]:
                heapq.heapreplace(candidates, candidate)
                truncated = True
            else:
                truncated = True

    ordered = sorted(candidates, reverse=True)
    parsed: list[tuple[str, dict]] = []
    read_bytes = 0
    for index, (_mtime_ns, name, listed_size) in enumerate(ordered):
        if len(parsed) >= JOB_LIST_HISTORY_MAX_RETURNED:
            truncated = True
            break
        remaining = JOB_LIST_HISTORY_MAX_READ_BYTES - read_bytes
        if remaining <= 0:
            truncated = True
            break
        if listed_size > remaining:
            truncated = True
            continue
        job_id = name[:-5]
        try:
            payload, consumed = _read_job_journal_sized(
                directory / name,
                job_id,
                max_bytes=min(JOB_JOURNAL_MAX_BYTES, remaining),
            )
        except (
            json.JSONDecodeError,
            OSError,
            RecursionError,
            RuntimeError,
            UnicodeDecodeError,
            ValueError,
        ):
            continue
        read_bytes += consumed
        parsed.append((job_id, payload))
        if (
            len(parsed) >= JOB_LIST_HISTORY_MAX_RETURNED
            and index + 1 < len(ordered)
        ):
            truncated = True

    return {
        "entries": parsed,
        "scanned": scanned,
        "read_bytes": read_bytes,
        "truncated": truncated,
    }


def _journal_state(payload: dict) -> str:
    state = payload.get("submission_state")
    return state if state in _PERSISTED_JOB_STATES else "unknown"


def _recovery_source_job_id(spec: JobSpec | dict | None) -> str | None:
    """Return the direct checkpoint source without exposing any other spec."""

    if isinstance(spec, JobSpec):
        recovery = spec.recovery
        if recovery.policy != "checkpoint":
            return None
        source = recovery.source_job_id
    elif isinstance(spec, dict):
        recovery = spec.get("recovery")
        if (
            not isinstance(recovery, dict)
            or recovery.get("policy") != "checkpoint"
        ):
            return None
        source = recovery.get("source_job_id")
    else:
        return None
    if not isinstance(source, str) or _SAFE_JOB_ID.fullmatch(source) is None:
        return None
    return source


def _lineage_fields(
    job_id: str,
    spec: JobSpec | dict | None,
    metadata: dict | None = None,
) -> dict:
    """Project a compact, non-secret direct recovery lineage.

    Newer journals may carry an exact root/attempt calculated by the lifecycle
    layer. Direct recovery lineage remains derivable from JobSpec for rolling
    upgrades and old records.
    """

    metadata = metadata if isinstance(metadata, dict) else {}
    source = metadata.get("resumed_from_job_id")
    if not isinstance(source, str) or _SAFE_JOB_ID.fullmatch(source) is None:
        source = _recovery_source_job_id(spec)
    root = metadata.get("root_job_id")
    if not isinstance(root, str) or _SAFE_JOB_ID.fullmatch(root) is None:
        root = source or job_id
    raw_attempt = metadata.get("attempt_no")
    try:
        attempt = int(raw_attempt)
    except (TypeError, ValueError):
        attempt = 2 if source else 1
    if attempt < 1 or attempt > 1_000_000:
        attempt = 2 if source else 1
    return {
        "source_job_id": source,
        "resumed_from_job_id": source,
        "root_job_id": root,
        "attempt_no": attempt,
    }


def _verified_resume_fields(
    *,
    state: str,
    cleanup_pending: int,
    latest_generation: object,
    latest_committed_at: object,
    metadata: dict | None,
) -> dict:
    """Fail closed unless the suspended snapshot and local pointer agree."""

    metadata = metadata if isinstance(metadata, dict) else {}
    generation = metadata.get("resume_generation")
    if not isinstance(generation, str) or (
        _SAFE_RESUME_GENERATION.fullmatch(generation) is None
    ):
        generation = None
    latest = latest_generation if isinstance(latest_generation, str) else None
    committed_at = metadata.get("resume_committed_at")
    if not isinstance(committed_at, str) or not committed_at:
        committed_at = None
    latest_commit = (
        latest_committed_at
        if isinstance(latest_committed_at, str)
        else None
    )
    available = bool(
        state == "suspended"
        and metadata.get("state") == "suspended"
        and metadata.get("done") is True
        and cleanup_pending == 0
        and metadata.get("resume_available") is True
        and generation
        and latest == generation
        and committed_at
        and latest_commit == committed_at
    )
    return {
        "resume_available": available,
        "resume_generation": generation,
        "resume_committed_at": committed_at,
        "suspend_warning": metadata.get("suspend_warning"),
        "interrupt_requested": bool(metadata.get("interrupt_requested")),
        "interrupt_reason": metadata.get("interrupt_reason"),
    }


def _verified_checkpoint_recovery_available(
    *,
    state: str,
    done: bool,
    cleanup_pending: int,
    latest_generation: object,
    advertised: bool,
) -> bool:
    """Expose manual recovery only for a quiescent, pinned terminal source."""

    return bool(
        advertised
        and state in {"succeeded", "failed", "cancelled"}
        and done
        and cleanup_pending == 0
        and isinstance(latest_generation, str)
        and _SAFE_RESUME_GENERATION.fullmatch(latest_generation) is not None
    )


def _persisted_job_view(
    job_id: str,
    payload: dict,
    recovered: list,
    *,
    include_spec: bool,
    recovery_leases_truncated: bool = False,
) -> dict:
    """Render a crash-recovered journal without inventing completion.

    A journal in ``launching``/``running`` means the Manager disappeared while
    ownership was live.  It is deliberately shown as interrupted and never as
    done.  Only an explicit durable terminal state can report completion.
    """
    submission_state = _journal_state(payload)
    terminal = submission_state in _TERMINAL_JOB_STATES
    cleanup_pending = (
        sum(lease.state != "released" for lease in recovered)
        # A truncated tail may contain an active lease. Never report a
        # recovered Job fully released when that cannot be proven.
        + int(recovery_leases_truncated)
    )
    collection_errors = [
        lease.recovery_collection_error
        for lease in recovered
        if lease.recovery_collection_error
    ]
    terminal_summary = payload.get("terminal_summary")
    if not isinstance(terminal_summary, dict):
        terminal_summary = {}
    summary_error = terminal_summary.get("error")
    errors = collection_errors + ([str(summary_error)] if summary_error else [])
    raw_terminal_workers = terminal_summary.get("terminal_workers")
    if not isinstance(raw_terminal_workers, list):
        raw_terminal_workers = []
    terminal_workers = []
    for raw in raw_terminal_workers:
        if not isinstance(raw, dict):
            continue
        worker_id = raw.get("worker_id")
        if not isinstance(worker_id, str):
            continue
        try:
            shard_index = int(raw.get("shard_index") or 0)
        except (TypeError, ValueError):
            shard_index = 0
        collection_error = (
            str(raw["collection_error"])
            if raw.get("collection_error") else None
        )
        final_collected = (
            raw.get("final_collected") is True
            or (
                "final_collected" not in raw
                and "collection_error" in raw
            )
        )
        terminal_workers.append({
            "worker_id": worker_id,
            "phase": str(raw.get("phase") or "failed"),
            "shard_index": shard_index,
            "account_id": "",
            "account_email": "",
            "active_slot": 0,
            "accounts": [],
            "rotations": 0,
            "task_id": str(raw.get("task_id") or ""),
            "error": str(raw["error"]) if raw.get("error") else None,
            "lease_id": "",
            "eip": "",
            "eip_allocation_id": "",
            "final_collected": final_collected,
            "collection_error": collection_error,
            "cleaned_up": bool(raw.get("worker_released", terminal)),
            "cleanup_error": (
                str(raw["cleanup_error"]) if raw.get("cleanup_error") else None
            ),
            "cleanup_attempts": 0,
            "worker_released": bool(raw.get("worker_released", terminal)),
            "worker_release_expected": True,
        })

    if submission_state in {"launching", "running"}:
        state = "interrupted"
    elif submission_state == "unknown":
        state = "recovered"
    else:
        state = submission_state
    raw_spec = payload.get("spec")
    raw_collect = (
        raw_spec.get("collect")
        if isinstance(raw_spec, dict) else None
    )
    checkpoint_recovery_advertised = bool(
        payload.get("checkpoint_recovery_available") is True
        or (
            isinstance(raw_collect, dict)
            and raw_collect.get("checkpoint") is True
        )
    )
    latest_checkpoint_generation = payload.get(
        "latest_checkpoint_generation"
    ) or terminal_summary.get("latest_checkpoint_generation")
    done = terminal and cleanup_pending == 0
    checkpoint_recovery_available = (
        _verified_checkpoint_recovery_available(
            state=state,
            done=done,
            cleanup_pending=cleanup_pending,
            latest_generation=latest_checkpoint_generation,
            advertised=checkpoint_recovery_advertised,
        )
    )
    lineage_metadata = payload.get("lineage")
    if not isinstance(lineage_metadata, dict):
        lineage_metadata = {}
    lineage_metadata = {
        **lineage_metadata,
        **{
            key: terminal_summary[key]
            for key in (
                "resumed_from_job_id",
                "root_job_id",
                "attempt_no",
            )
            if key in terminal_summary
        },
    }
    resume = _verified_resume_fields(
        state=state,
        cleanup_pending=cleanup_pending,
        latest_generation=latest_checkpoint_generation,
        latest_committed_at=payload.get("checkpoint_committed_at"),
        metadata=terminal_summary,
    )

    view = {
        "job_id": job_id,
        "name": payload.get("name", ""),
        "workers": terminal_summary.get("workers", len(recovered)),
        "phases": terminal_summary.get("phases", {}),
        "state": state,
        "submission_state": submission_state,
        "done": done,
        "cleanup_pending": cleanup_pending,
        "error": "; ".join(errors) or None,
        "cancel_requested": bool(terminal_summary.get("cancel_requested")),
        "cancel_reason": terminal_summary.get("cancel_reason"),
        "created_at": terminal_summary.get("created_at"),
        "started_at": terminal_summary.get("started_at"),
        "completed_at": terminal_summary.get("completed_at"),
        # The top-level pointer is advanced independently using the
        # checkpoint's committed_at timestamp.  A terminal summary can be
        # written later from a stale in-memory snapshot, so it is only a
        # compatibility fallback for journals that predate that pointer.
        "latest_checkpoint_generation": latest_checkpoint_generation,
        "checkpoint_recovery_available": checkpoint_recovery_available,
        **resume,
        **_lineage_fields(
            job_id,
            raw_spec,
            lineage_metadata,
        ),
        "in_memory": False,
        "workers_detail": terminal_workers,
        "recovery_leases": [lease.model_dump() for lease in recovered],
        "recovery_leases_truncated": recovery_leases_truncated,
    }
    if include_spec:
        view["spec"] = _redacted_spec(payload.get("spec") or {})
    if submission_state in {"launching", "running", "unknown"}:
        view["note"] = (
            "submission was interrupted by a Manager restart; inspect recovery "
            "cleanup before explicitly resubmitting"
        )
    elif submission_state == "prepared":
        view["note"] = "submission was durably prepared but was not launched"
    return view


def _api_spec_projection(spec: JobSpec | dict) -> dict:
    """Project a JobSpec onto the known schema before any API exposure.

    A current in-memory model is already validated.  Persisted dictionaries are
    untrusted recovery input: validating with ``extra='forbid'`` prevents a
    legacy/corrupt journal from smuggling an unknown secret field into the
    response. ``exclude_unset`` preserves old snapshots without inventing
    defaults that did not exist when they were written.

    A small allowlist covers values written by known adjacent JobSpec revisions:
    ``manager_distribute``, the sandbox environment profile, and account
    selection metadata. They are mapped or removed only for structural
    validation and then restored in the projection. Current submissions and
    resubmits continue to reject them.
    """

    if isinstance(spec, JobSpec):
        return spec.model_dump(mode="json")
    if not isinstance(spec, dict):
        raise ValueError("persisted JobSpec is not an object")

    candidate = copy.deepcopy(spec)
    legacy_manager_distribution = False
    read_only_sandbox_profile = False
    read_only_auth_kind: str | None = None
    read_only_exclude_ids: list[str] | None = None

    environment = candidate.get("environment")
    if (
        isinstance(environment, dict)
        and environment.get("profile") == _READ_ONLY_SANDBOX_PROFILE
    ):
        environment["profile"] = _READ_ONLY_SANDBOX_VALIDATION_PROFILE
        read_only_sandbox_profile = True

    account = candidate.get("account")
    try:
        if isinstance(account, dict):
            if account.get("mode") == "manager_distribute":
                account["mode"] = "worker_local_login"
                legacy_manager_distribution = True

            if "auth_kind" in account:
                auth_kind = account.pop("auth_kind")
                if (
                    not isinstance(auth_kind, str)
                    or auth_kind not in _READ_ONLY_ACCOUNT_AUTH_KINDS
                ):
                    raise ValueError("invalid persisted account auth kind")
                read_only_auth_kind = auth_kind

            if "exclude_ids" in account:
                excluded = account.pop("exclude_ids")
                if (
                    not isinstance(excluded, list)
                    or len(excluded) > _READ_ONLY_MAX_EXCLUDED_ACCOUNTS
                ):
                    raise ValueError("invalid persisted excluded account list")
                normalized: list[str] = []
                seen: set[str] = set()
                for raw_account_id in excluded:
                    if not isinstance(raw_account_id, str):
                        raise ValueError("invalid persisted excluded account")
                    account_id = raw_account_id.strip()
                    if not account_id:
                        continue
                    if _SAFE_PERSISTED_ACCOUNT_REFERENCE.fullmatch(account_id) is None:
                        raise ValueError("unsafe persisted excluded account")
                    if account_id not in seen:
                        seen.add(account_id)
                        normalized.append(account_id)
                read_only_exclude_ids = normalized

        validated = JobSpec.model_validate(candidate)
    except (TypeError, ValueError, RecursionError) as exc:
        raise ValueError("persisted JobSpec is incompatible with the API schema") from exc

    projected = validated.model_dump(mode="json", exclude_unset=True)
    if read_only_sandbox_profile:
        projected_environment = projected.get("environment")
        if not isinstance(projected_environment, dict):
            raise ValueError("persisted legacy environment section is invalid")
        projected_environment["profile"] = _READ_ONLY_SANDBOX_PROFILE

    if (
        legacy_manager_distribution
        or read_only_auth_kind is not None
        or read_only_exclude_ids is not None
    ):
        projected_account = projected.get("account")
        if not isinstance(projected_account, dict):
            raise ValueError("persisted legacy account section is invalid")
        if legacy_manager_distribution:
            projected_account["mode"] = "manager_distribute"
        if read_only_auth_kind is not None:
            projected_account["auth_kind"] = read_only_auth_kind
        if read_only_exclude_ids is not None:
            selected = projected_account.get("ids", [])
            if not isinstance(selected, list):
                raise ValueError("persisted selected account list is invalid")
            overlap = set(read_only_exclude_ids).intersection(selected)
            if overlap:
                raise ValueError("persisted account selection lists overlap")
            projected_account["exclude_ids"] = read_only_exclude_ids
    return projected


def _redacted_spec(spec: JobSpec | dict) -> dict:
    """Return a bounded, schema-aware API snapshot with secret values removed."""

    data = _api_spec_projection(spec)
    run = data.get("run") if isinstance(data, dict) else None
    if isinstance(run, dict):
        env = run.get("env")
        if isinstance(env, dict):
            run["env"] = {str(key): "[REDACTED]" for key in env}
        secret_env = run.get("secret_env")
        if isinstance(secret_env, dict):
            run["secret_env"] = {
                str(key): "[SECRET_REFERENCE]" for key in secret_env
            }
    setup = data.get("setup") if isinstance(data, dict) else None
    if isinstance(setup, dict):
        repo = setup.get("repo")
        if isinstance(repo, str):
            try:
                parsed = urlsplit(repo)
                embedded_secret = (
                    parsed.username is not None
                    or parsed.query
                    or parsed.fragment
                )
                if embedded_secret:
                    hostname = parsed.hostname or ""
                    if parsed.port:
                        hostname = f"{hostname}:{parsed.port}"
                    setup["repo"] = urlunsplit((
                        parsed.scheme, hostname, parsed.path, "", "",
                    )) or "[REDACTED_REPOSITORY_URL]"
            except ValueError:
                setup["repo"] = "[REDACTED_REPOSITORY_URL]"
        steps = setup.get("steps")
        if isinstance(steps, list):
            for step in steps:
                if not isinstance(step, dict) or not isinstance(step.get("env"), dict):
                    continue
                step["env"] = {
                    str(key): "[REDACTED]" for key in step["env"]
                }

    try:
        encoded = json.dumps(
            data,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        ).encode("utf-8")
    except (
        RecursionError,
        TypeError,
        UnicodeEncodeError,
        ValueError,
    ) as exc:
        raise ValueError("persisted JobSpec cannot be serialized safely") from exc
    if len(encoded) > JOB_CONFIG_MAX_RESPONSE_BYTES:
        raise ValueError("persisted JobSpec exceeds the config response boundary")
    return data


def _canonical_spec(spec: object) -> object:
    """Normalize legacy persisted specs before idempotency comparison."""

    candidate = copy.deepcopy(spec)
    legacy_manager_distribution = False
    if isinstance(candidate, dict):
        account = candidate.get("account")
        if (
            isinstance(account, dict)
            and account.get("mode") == "manager_distribute"
        ):
            # ``manager_distribute`` existed in an older schema. Preserve its
            # identity marker while borrowing the current model only to fill
            # defaults and normalize the rest of an otherwise compatible
            # legacy request.
            account["mode"] = "worker_local_login"
            legacy_manager_distribution = True
    try:
        normalized = JobSpec.model_validate(candidate).model_dump(mode="json")
    except (TypeError, ValueError):
        # An invalid/different journal must remain a mismatch. The caller
        # reports the same 409 as before without exposing validation details.
        return spec
    if legacy_manager_distribution:
        normalized["account"]["mode"] = "manager_distribute"
    return normalized


def _reject_nonstandard_json_constant(value: str) -> None:
    """Reject NaN/Infinity, which are not JSON and lack stable semantics."""

    raise ValueError(f"invalid JSON constant {value!r}")


def _reject_duplicate_json_keys(
    pairs: list[tuple[str, object]],
) -> dict[str, object]:
    """Reject ambiguous JSON objects instead of fingerprinting last-key-wins."""

    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON object key")
        result[key] = value
    return result


async def _read_bounded_job_request(request: Request) -> dict[str, object]:
    """Read one POST /jobs payload before applying the current JobSpec model."""

    raw_content_length = request.headers.get("content-length")
    if raw_content_length is not None:
        try:
            content_length = int(raw_content_length)
        except ValueError as exc:
            raise HTTPException(400, "invalid Content-Length") from exc
        if content_length < 0:
            raise HTTPException(400, "invalid Content-Length")
        if content_length > JOB_SUBMIT_MAX_BODY_BYTES:
            raise HTTPException(
                413,
                "Job request body exceeds the "
                f"{JOB_SUBMIT_MAX_BODY_BYTES}-byte limit",
            )

    request_state = request.scope.get("state")
    middleware_limit = (
        request_state.get(REQUEST_BODY_LIMIT_STATE_KEY)
        if isinstance(request_state, dict)
        else None
    )
    if (
        isinstance(middleware_limit, int)
        and 0 < middleware_limit <= JOB_SUBMIT_MAX_BODY_BYTES
    ):
        body: bytes | bytearray = await request.body()
        if len(body) > JOB_SUBMIT_MAX_BODY_BYTES:
            raise HTTPException(
                413,
                "Job request body exceeds the "
                f"{JOB_SUBMIT_MAX_BODY_BYTES}-byte limit",
            )
    else:
        # Routers embedded without the production application middleware keep
        # the same independent hard ceiling, including for chunked bodies.
        incremental = bytearray()
        async for chunk in request.stream():
            if len(incremental) + len(chunk) > JOB_SUBMIT_MAX_BODY_BYTES:
                raise HTTPException(
                    413,
                    "Job request body exceeds the "
                    f"{JOB_SUBMIT_MAX_BODY_BYTES}-byte limit",
                )
            incremental.extend(chunk)
        body = incremental
    if not body:
        raise HTTPException(422, "Job request body must be a JSON object")
    try:
        payload = json.loads(
            body,
            object_pairs_hook=_reject_duplicate_json_keys,
            parse_constant=_reject_nonstandard_json_constant,
        )
    except (
        json.JSONDecodeError,
        UnicodeDecodeError,
        RecursionError,
        ValueError,
    ) as exc:
        raise HTTPException(422, "invalid Job JSON request body") from exc
    if not isinstance(payload, dict):
        raise HTTPException(422, "Job request body must be a JSON object")
    return payload


def _request_fingerprint(payload: object) -> str:
    """Return a stable digest of one parsed, standards-compliant JSON value."""

    canonical = json.dumps(
        payload,
        ensure_ascii=True,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")
    return hashlib.sha256(canonical).hexdigest()


def _validated_job_spec(payload: dict[str, object]) -> JobSpec:
    """Apply the current schema while retaining FastAPI's safe 422 handling."""

    try:
        return JobSpec.model_validate(payload)
    except ValidationError as exc:
        errors = exc.errors(
            include_url=True,
            include_context=False,
            include_input=False,
        )
        for error in errors:
            error["loc"] = ("body", *error.get("loc", ()))
        raise RequestValidationError(errors) from exc


def _validate_job_stdin_protocol(
    spec: JobSpec, *, allow_stdin_protocol: bool,
) -> JobSpec:
    if spec.run.stdin_protocol != "none" and not allow_stdin_protocol:
        raise HTTPException(
            422,
            "run.stdin_protocol is reserved for a trusted server-side constructor",
        )
    return spec


def _strict_json_object(encoded: bytes, *, label: str) -> dict[str, object]:
    def unique(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"{label} contains a duplicate field")
            result[key] = value
        return result

    def reject(_value: str) -> None:
        raise ValueError(f"{label} contains a non-finite number")

    try:
        value = json.loads(
            encoded.decode("utf-8", errors="strict"),
            object_pairs_hook=unique,
            parse_constant=reject,
        )
    except (UnicodeError, json.JSONDecodeError, ValueError) as exc:
        raise HTTPException(422, f"{label} is invalid") from exc
    if not isinstance(value, dict) or not all(
        isinstance(key, str) for key in value
    ):
        raise HTTPException(422, f"{label} must be a JSON object")
    return value


def _decode_run_benchmark_frame(
    encoded: str,
) -> tuple[bytearray, dict[str, object]]:
    try:
        frame = bytearray(base64.b64decode(encoded, validate=True))
    except (binascii.Error, ValueError) as exc:
        raise HTTPException(422, "credential frame is invalid") from exc
    try:
        if len(frame) < RUN_BENCHMARK_FRAME_HEADER.size:
            raise HTTPException(422, "credential frame is invalid")
        magic, public_size, secret_size = RUN_BENCHMARK_FRAME_HEADER.unpack(
            frame[: RUN_BENCHMARK_FRAME_HEADER.size]
        )
        expected_size = (
            RUN_BENCHMARK_FRAME_HEADER.size + public_size + secret_size
        )
        if (
            magic != RUN_BENCHMARK_STDIN_MAGIC
            or public_size > RUN_BENCHMARK_MAX_PUBLIC_BYTES
            or not 1 <= secret_size <= RUN_BENCHMARK_MAX_KEY_BYTES
            or len(frame) != expected_size
        ):
            raise HTTPException(422, "credential frame is invalid")
        public_start = RUN_BENCHMARK_FRAME_HEADER.size
        secret_start = public_start + public_size
        secret = frame[secret_start:]
        if any(byte < 0x21 or byte > 0x7E for byte in secret):
            raise HTTPException(422, "credential frame is invalid")
        public = _strict_json_object(
            bytes(frame[public_start:secret_start]),
            label="credential frame public request",
        )
        return frame, public
    except BaseException:
        for index in range(len(frame)):
            frame[index] = 0
        raise


def _safe_run_benchmark_text(
    value: object, label: str, *, maximum: int = 1024,
) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or len(value) > maximum
        or any(ord(character) < 0x20 or ord(character) == 0x7F for character in value)
    ):
        raise HTTPException(422, f"credential frame {label} is invalid")
    return value


def _validate_run_benchmark_public_request(
    envelope: RunBenchmarkJobRequest,
    public: dict[str, object],
) -> dict[str, object]:
    required = {
        "instance_id", "instance_digest", "harness_id",
        "model", "server_id", "tags", "data_tag", "worker_release_digest",
        "credential_mode",
    }
    optional = {
        "accepted_timeout", "effort", "api_protocol", "api_base", "instance_ref",
    }
    if set(public) - required - optional or required - set(public):
        raise HTTPException(
            422, "credential frame public request schema is invalid"
        )
    string_fields = {
        field: _safe_run_benchmark_text(public[field], field)
        for field in required - {"tags"}
    }
    if (
        string_fields["server_id"] != "elastic"
        or string_fields["harness_id"] != envelope.harness_id
        or string_fields["instance_digest"] != envelope.instance_digest
        or string_fields["worker_release_digest"]
        != envelope.worker_release_digest
        or string_fields["credential_mode"] != "ephemeral_per_run"
    ):
        raise HTTPException(
            422, "credential frame public request binding does not match"
        )
    if envelope.harness_id not in RUN_BENCHMARK_HARNESSES:
        raise HTTPException(422, "Run-Benchmark harness is not allowed")
    if (
        _RUN_BENCHMARK_ID.fullmatch(string_fields["instance_id"]) is None
        or _RUN_BENCHMARK_ID.fullmatch(string_fields["harness_id"]) is None
        or _RUN_BENCHMARK_DIGEST.fullmatch(string_fields["instance_digest"])
        is None
        or _RUN_BENCHMARK_DIGEST.fullmatch(
            string_fields["worker_release_digest"]
        )
        is None
    ):
        raise HTTPException(422, "credential frame public request is invalid")
    tags = public["tags"]
    if (
        not isinstance(tags, list)
        or len(tags) > 32
        or any(
            not isinstance(tag, str)
            or not tag
            or len(tag) > 128
            or tag != tag.strip()
            for tag in tags
        )
        or len(tags) != len(set(tags))
    ):
        raise HTTPException(422, "credential frame tags are invalid")
    accepted_timeout = public.get("accepted_timeout")
    if accepted_timeout is not None and type(accepted_timeout) is not bool:
        raise HTTPException(422, "credential frame accepted_timeout is invalid")
    effort = public.get("effort")
    if effort is not None:
        _safe_run_benchmark_text(effort, "effort", maximum=128)
    if public.get("instance_ref") is not None:
        _safe_run_benchmark_text(public["instance_ref"], "instance_ref")
    protocol = public.get("api_protocol")
    base = public.get("api_base")
    if (protocol is None) != (base is None):
        raise HTTPException(422, "credential frame route is incomplete")
    if protocol is not None:
        protocol_value = _safe_run_benchmark_text(
            protocol, "api_protocol", maximum=64
        )
        base_value = _safe_run_benchmark_text(base, "api_base", maximum=2048)
        parsed = urlsplit(base_value)
        if (
            protocol_value
            not in {
                "openai_responses", "openai", "anthropic", "kimi",
                "google-genai", "vertexai", "gemini", "openrouter",
            }
            or parsed.scheme != "https"
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
        ):
            raise HTTPException(422, "credential frame route is invalid")
    return public


def _run_benchmark_job_spec(
    request: RunBenchmarkJobRequest,
    public: dict[str, object],
) -> dict[str, object]:
    bucket = os.environ.get("ELASTIC_AGENT_RESULTS_S3_BUCKET", "").strip()
    if not bucket:
        raise HTTPException(
            503, "Run-Benchmark input storage is not configured"
        )
    expected_uri = (
        f"s3://{bucket}/jobs/datasets/run-benchmark/v1/sha256/"
        f"{request.input_digest}/"
    )
    if request.input_uri != expected_uri:
        raise HTTPException(422, "Run-Benchmark input URI is not canonical")
    if (
        _RUN_BENCHMARK_ID.fullmatch(request.run_id) is None
        or _RUN_BENCHMARK_COMMIT.fullmatch(request.resolved_commit) is None
        or any(
            _RUN_BENCHMARK_DIGEST.fullmatch(value) is None
            for value in (
                request.worker_release_digest,
                request.input_digest,
                request.instance_digest,
            )
        )
    ):
        raise HTTPException(422, "Run-Benchmark envelope is invalid")

    runtime_root = "elastic-runtime"
    input_root = "elastic-input"
    venv_python = ".elastic-runtime-venv/bin/python"
    common = [
        "--data-root", runtime_root,
        "--input-root", input_root,
        "--run-id", request.run_id,
        "--input-digest", request.input_digest,
        "--instance-digest", request.instance_digest,
        "--worker-release-digest", request.worker_release_digest,
    ]
    prepare_args = [
        venv_python,
        "-m", "run_benchmark.elastic_worker", "prepare",
        "--source-root", ".",
        *common,
        "--harness-id", request.harness_id,
        "--model", str(public["model"]),
    ]
    if public.get("effort") is not None:
        prepare_args.extend(("--effort", str(public["effort"])))
    if public.get("api_protocol") is not None:
        prepare_args.extend((
            "--api-protocol", str(public["api_protocol"]),
            "--api-base", str(public["api_base"]),
        ))
    setup_command = " && ".join((
        "python3 -m venv .elastic-runtime-venv",
        ".elastic-runtime-venv/bin/python -m pip install "
        "--disable-pip-version-check .",
    ))
    exact_source = f"{runtime_root}/releases/{request.worker_release_digest}/src"
    execute_args = [
        "env", f"PYTHONPATH={exact_source}", venv_python,
        "-m", "run_benchmark.elastic_worker", "execute",
        *common,
        "--wall-time-seconds", str(request.wall_time_seconds),
    ]
    run_timeout = request.wall_time_seconds + 900
    # Manager delivery order is code -> setup steps -> S3 datasets -> run.
    # Keep dependency installation in setup, but perform the sealed input,
    # release, and image attestation at the start of the trusted run process.
    # The one-shot stdin frame may already be waiting in that process pipe;
    # ``prepare`` never reads stdin and ``execute`` becomes the first reader
    # only after every attestation succeeds.
    run_command = f"{shlex.join(prepare_args)} && exec {shlex.join(execute_args)}"
    ttl_seconds = min(
        int(MAX_EPHEMERAL_STDIN_TTL_SECONDS),
        max(3600, request.wall_time_seconds + 10_800),
    )
    return {
        "name": f"run-benchmark-{request.run_id}",
        "environment": {"profile": "ubuntu-agent-docker-v2"},
        "setup": {
            "repo": RUN_BENCHMARK_REPOSITORY,
            "ref": "main",
            "resolved_commit": request.resolved_commit,
            "target_dir": "/opt/elastic-agent/run-benchmark",
            "deliver": "manager_rsync",
            "needs_docker": True,
            "steps": [{
                "name": "prepare-sealed-run-benchmark-runtime",
                "command": setup_command,
                "timeout": 7200,
                "retries": 0,
            }],
            "s3_datasets": [{"uri": request.input_uri, "dest": (
                "/opt/elastic-agent/run-benchmark/" + input_root
            )}],
        },
        "run": {
            "command": run_command,
            "cwd": ".",
            "timeout": run_timeout,
            "shell": True,
            "stdin_protocol": "run_benchmark_v1",
            "env": {},
            "secret_env": {},
        },
        "account": {"mode": "none", "binding": "none"},
        "rotation": {"strategy": "none"},
        "fanout": {"workers": 1, "shard_by": "none"},
        "collect": {
            "paths": [f"{runtime_root}/results"],
            "interval_seconds": 0,
        },
        "completion": {"on_process_exit": 0},
        "ttl_seconds": ttl_seconds,
    }


def _journal_request_match(
    payload: dict,
    raw_request: dict[str, object],
    request_fingerprint: str,
) -> tuple[bool, bool]:
    """Return ``(matches, is_legacy)`` for a persisted idempotent request.

    New journals compare the canonical raw-request digest. Old journals can
    replay only when their stored spec is byte-semantically identical JSON or
    both sides normalize identically under a known compatible legacy schema.
    """

    stored = payload.get("request_fingerprint")
    if stored is not None:
        if (
            not isinstance(stored, dict)
            or set(stored) != {"schema", "algorithm", "digest"}
            or stored.get("schema") != JOB_REQUEST_FINGERPRINT_SCHEMA
            or stored.get("algorithm") != JOB_REQUEST_FINGERPRINT_ALGORITHM
            or not isinstance(stored.get("digest"), str)
            or re.fullmatch(r"[0-9a-f]{64}", stored["digest"]) is None
        ):
            raise ValueError("invalid persisted Job request fingerprint")
        if hmac.compare_digest(stored["digest"], request_fingerprint):
            return True, False
        # Canonical raw JSON intentionally distinguishes omitted defaults from
        # explicitly serialized defaults. Preserve the prior semantic
        # idempotency contract when *both* requests still validate under the
        # current schema and normalize to the same JobSpec. Cross-version
        # historical requests that no longer validate get only the strict raw
        # fingerprint path above.
        try:
            persisted_current = JobSpec.model_validate(
                payload.get("spec")
            ).model_dump(mode="json")
            request_current = JobSpec.model_validate(
                raw_request
            ).model_dump(mode="json")
            return (
                hmac.compare_digest(
                    _request_fingerprint(persisted_current),
                    _request_fingerprint(request_current),
                ),
                False,
            )
        except (TypeError, ValueError, RecursionError):
            return False, False

    persisted_spec = payload.get("spec")
    try:
        if hmac.compare_digest(
            _request_fingerprint(persisted_spec),
            request_fingerprint,
        ):
            return True, True
        persisted_normalized = _canonical_spec(persisted_spec)
        request_normalized = _canonical_spec(raw_request)
        return (
            hmac.compare_digest(
                _request_fingerprint(persisted_normalized),
                _request_fingerprint(request_normalized),
            ),
            True,
        )
    except (TypeError, ValueError, RecursionError):
        return False, True


def _idempotency_conflict(*, legacy: bool) -> HTTPException:
    if legacy:
        return HTTPException(
            409,
            "Idempotency-Key belongs to a legacy Job journal without a "
            "request fingerprint; exact request equivalence cannot be proven",
        )
    return HTTPException(
        409,
        "Idempotency-Key was already used for another Job request",
    )


def _job_detail(
    job,
    *,
    include_spec: bool = True,
) -> dict:
    is_eip_bound = job.spec.account.binding == "eip"
    worker_release_expected = is_eip_bound or job.release_workers_on_complete
    summary = job.summary()
    summary.pop("terminal_workers", None)
    detail = {
        **summary,
        "workers_detail": [
            {
                "worker_id": r.worker_id,
                "phase": r.phase.value,
                "shard_index": r.ctx.shard_index,
                "account_id": r.account_id,
                "account_email": r.account_email,
                "active_slot": r.active_slot,
                # All accounts logged in on this worker (per_worker), active flagged.
                "accounts": [
                    {
                        "account_id": aid,
                        "email": r.account_emails[i] if i < len(r.account_emails) else "",
                        "config_dir": r.config_dirs[i] if i < len(r.config_dirs) else "",
                        "active": i == r.active_slot,
                    }
                    for i, aid in enumerate(r.account_ids)
                ],
                "rotations": r.rotations,
                "task_id": r.task_id,
                "error": r.error,
                "lease_id": r.lease_id,
                "eip": r.eip,
                "eip_allocation_id": r.eip_allocation_id,
                "final_collected": r.final_collected,
                "collection_error": r.collection_error,
                "cleaned_up": r.cleaned_up,
                "cleanup_error": r.cleanup_error,
                "cleanup_attempts": r.cleanup_attempts,
                # A WorkerRun is retained as Job execution history after its
                # compute resource is gone.  Keep that distinction explicit
                # for API clients instead of asking them to infer it from a
                # terminal process phase.
                "worker_released": (
                    r.cleaned_up
                    if is_eip_bound
                    else (
                        job.resources_released
                        or (
                            job.release_workers_on_complete
                            and r.cleaned_up
                        )
                    )
                ),
                "worker_release_expected": worker_release_expected,
            }
            for r in job.runs.values()
        ],
        "pending_cleanup_detail": [
            {
                "lease_id": assignment.lease_id,
                "account_id": assignment.account_id,
                "slot": assignment.slot,
                "error": job.cleanup_errors.get(assignment.lease_id),
            }
            for assignment in job.pending_cleanup.values()
        ],
    }
    detail.update(_lineage_fields(job.job_id, job.spec, summary))
    detail["checkpoint_recovery_available"] = (
        _verified_checkpoint_recovery_available(
            state=str(detail.get("state") or ""),
            done=detail.get("done") is True,
            cleanup_pending=int(detail.get("cleanup_pending") or 0),
            latest_generation=detail.get(
                "latest_checkpoint_generation"
            ),
            advertised=(
                detail.get("checkpoint_recovery_available") is True
            ),
        )
    )
    detail.update(_verified_resume_fields(
        state=str(detail.get("state") or ""),
        cleanup_pending=int(detail.get("cleanup_pending") or 0),
        latest_generation=detail.get("latest_checkpoint_generation"),
        latest_committed_at=getattr(
            job,
            "latest_checkpoint_committed_at",
            None,
        ),
        metadata=summary,
    ))
    if include_spec:
        detail["spec"] = _redacted_spec(job.spec)
    return detail


def _job_list_item(job) -> dict:
    """Job summary + workers_detail but WITHOUT the (heavy) spec — enough for the
    UI's job list to render a full card without a per-job detail request."""
    d = _job_detail(job, include_spec=False)
    d["in_memory"] = True
    return d


async def _preflight_job(mgr, spec: JobSpec) -> dict:
    """Build a side-effect-free, secret-safe launch plan for ``spec``.

    This deliberately does not resolve/import an uploaded Harness, reserve an
    account, persist the spec, or call the cloud provider.  The same validation
    is run by both ``/jobs/plan`` and the real submit/resubmit paths before their
    first durable or billable action.
    """
    provider = mgr.config.provider
    if spec.harness_ref and os.environ.get(
        "ELASTIC_AGENT_ENABLE_HARNESS_UPLOAD", ""
    ).strip().lower() not in {"1", "true", "yes"}:
        raise HTTPException(
            422,
            "custom Harness execution is disabled; submit a declarative JobSpec",
        )
    if provider.type == "aws":
        configured_region = provider.aws.region
        max_instances = provider.aws.max_instances
        worker_profile = provider.aws.worker_instance_profile
        default_instance_type = provider.aws.default_instance_type
    elif provider.type == "aliyun":
        configured_region = provider.aliyun.region_id
        max_instances = provider.aliyun.max_instances
        worker_profile = ""
        default_instance_type = provider.aliyun.instance_type
    else:
        raise HTTPException(422, f"unsupported provider type {provider.type!r}")

    requested_region = spec.fanout.region.strip()
    if requested_region and requested_region != configured_region:
        raise HTTPException(
            422,
            f"Job region {requested_region!r} is unavailable: this Manager is "
            f"configured only for {configured_region!r}",
        )
    if spec.fanout.workers > max_instances:
        raise HTTPException(
            422,
            f"fanout.workers={spec.fanout.workers} exceeds this Manager's "
            f"configured maximum of {max_instances}",
        )

    configured_types = os.environ.get("ELASTIC_AGENT_ALLOWED_INSTANCE_TYPES", "")
    if configured_types.strip():
        allowed_instance_types = {
            item.strip() for item in configured_types.split(",") if item.strip()
        }
        if not allowed_instance_types:
            raise HTTPException(500, "ELASTIC_AGENT_ALLOWED_INSTANCE_TYPES is empty")
    else:
        # Fail closed: an unconfigured UI/API cannot silently select an
        # unexpectedly expensive machine type.
        allowed_instance_types = {default_instance_type}
    effective_instance_type = spec.fanout.instance_type or default_instance_type
    if effective_instance_type not in allowed_instance_types:
        raise HTTPException(
            422,
            f"instance type {effective_instance_type!r} is not allowed; allowed: "
            + ", ".join(sorted(allowed_instance_types)),
        )

    raw_worker_hours = os.environ.get(
        "ELASTIC_AGENT_MAX_JOB_WORKER_HOURS", "1440"
    ).strip()
    try:
        max_worker_hours = float(raw_worker_hours)
    except ValueError as exc:
        raise HTTPException(
            500, "ELASTIC_AGENT_MAX_JOB_WORKER_HOURS must be numeric",
        ) from exc
    if not math.isfinite(max_worker_hours) or max_worker_hours <= 0:
        raise HTTPException(
            500, "ELASTIC_AGENT_MAX_JOB_WORKER_HOURS must be greater than zero",
        )
    worst_case_worker_hours = (
        spec.fanout.workers * spec.ttl_seconds / 3_600
    )
    if worst_case_worker_hours > max_worker_hours:
        raise HTTPException(
            422,
            f"worst-case worker-hours {worst_case_worker_hours:g} exceeds "
            f"configured maximum {max_worker_hours:g}",
        )
    if spec.account.binding == "eip" and provider.type != "aws":
        raise HTTPException(422, "account.binding='eip' is supported only on AWS")
    if (
        spec.fanout.spot
        and provider.type == "aliyun"
        and not provider.aliyun.spot_enabled
    ):
        raise HTTPException(422, "Spot is disabled by this Manager's provider config")
    if spec.setup.s3_datasets and not (
        provider.type == "aws" and worker_profile
    ):
        raise HTTPException(
            422,
            "setup.s3_datasets requires an AWS worker_instance_profile so the "
            "worker can read S3 without static credentials",
        )

    results_bucket = _s3_bucket()
    if spec.collect.checkpoint and not results_bucket:
        raise HTTPException(
            422,
            "collect.checkpoint requires "
            "ELASTIC_AGENT_RESULTS_S3_BUCKET",
        )

    recovery_preview = None
    if spec.recovery.policy != "none":
        if spec.recovery.policy == "legacy_final_collection":
            raise HTTPException(
                422,
                "legacy mutable result recovery is disabled because it cannot "
                "prove file deletions or a complete generation; restart the "
                "workload from the beginning, then enable collect.checkpoint",
            )
        if not results_bucket:
            raise HTTPException(
                422,
                "checkpoint recovery requires "
                "ELASTIC_AGENT_RESULTS_S3_BUCKET",
            )
        try:
            permit = _acquire_result_operation(
                _JOB_HISTORY_ADMISSION,
                operation="recovery source validation",
            )
        except HTTPException:
            raise
        try:
            source_payload = await _run_owned_executor(
                _JOB_HISTORY_EXECUTOR,
                load_job_spec_journal,
                mgr.config.registry.path,
                spec.recovery.source_job_id,
            )
        except FileNotFoundError as exc:
            raise HTTPException(
                422,
                f"recovery source Job "
                f"{spec.recovery.source_job_id!r} does not exist",
            ) from exc
        except (
            json.JSONDecodeError,
            OSError,
            RuntimeError,
            UnicodeDecodeError,
            ValueError,
        ) as exc:
            raise HTTPException(
                422,
                "recovery source Job journal is invalid",
            ) from exc
        finally:
            permit.release()
        try:
            source_spec = JobSpec.model_validate(source_payload["spec"])
            from elastic_agent.core.manager_fleet_driver import (
                ManagerFleetDriver,
            )

            recovery_driver = ManagerFleetDriver(mgr)
            source_quiescent = (
                await recovery_driver._source_recovery_quiescent(
                    spec.recovery.source_job_id
                )
            )
            ManagerFleetDriver._validate_recovery_contract(
                source_payload,
                source_spec,
                spec,
                source_quiescent=source_quiescent,
            )
            resolved_checkpoint = None
            if spec.recovery.policy == "checkpoint":
                try:
                    resolved_checkpoint = (
                        await recovery_driver.resolve_recovery_checkpoint(
                            source_job_id=spec.recovery.source_job_id,
                            generation=spec.recovery.generation,
                            source_spec=source_spec,
                            target_spec=spec,
                        )
                    )
                except Exception as exc:  # S3 details stay Manager-private
                    raise HTTPException(
                        422,
                        "requested complete checkpoint set is unavailable",
                    ) from exc
        except ValidationError as exc:
            # Detailed Pydantic errors include rejected source values. A
            # persisted source may contain private environment values and
            # secret references that must never cross the REST boundary.
            raise HTTPException(
                422,
                "recovery source JobSpec is incompatible with the current schema",
            ) from exc
        except RuntimeError as exc:
            raise HTTPException(422, str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(
                422,
                "recovery source validation failed",
            ) from exc
        recovery_preview = {
            "policy": spec.recovery.policy,
            "source_job_id": spec.recovery.source_job_id,
            "paths": list(spec.recovery.paths),
            "generation": spec.recovery.generation or "latest",
            "source_state": source_payload["submission_state"],
            "source_quiescent": source_quiescent,
            "source_resolved_commit": source_spec.setup.resolved_commit,
            "staged_before_cloud_create": True,
        }
        if resolved_checkpoint is not None:
            recovery_preview["resolved_generation"] = (
                resolved_checkpoint.get("generation")
            )

    accounts = await mgr.account_store.list()
    agent_api_store = getattr(mgr, "agent_api_store", None)
    if agent_api_store is not None:
        # Local metadata only: Job plan is a pure read and must not make an
        # upstream CloudRouter request or claim an identity.
        accounts = [*(await agent_api_store.list()), *accounts]
    binding_manager = getattr(mgr, "binding_manager", None)
    durable_binding_ids = {
        binding.account_id
        for binding in (
            await binding_manager.list_bindings()
            if (
                binding_manager is not None
                and spec.account.mode != "none"
            )
            else []
        )
    }

    def supports_agent(account, agent_type: str, model: str = "") -> bool:
        check = getattr(account, "supports_agent_type", None)
        if callable(check):
            supported = bool(check(agent_type))
        else:
            supported = account.agent_type == agent_type
        model_check = getattr(account, "supports_model", None)
        return supported and (
            not model
            or not callable(model_check)
            or bool(model_check(agent_type, model))
        )

    def agent_api_decision(account) -> dict:
        if getattr(account, "auth_kind", "oauth") != "agent_api":
            return {"available": True}
        if agent_api_store is None:
            return {
                "available": False,
                "reason": "Agent API store is not configured",
            }
        return agent_api_store.availability_decision(account.id)

    if spec.account.mode != "none":
        by_id = {account.id: account for account in accounts}
        for account_id in spec.account.ids:
            account = by_id.get(account_id)
            if account is None:
                raise HTTPException(422, f"selected account {account_id!r} does not exist")
            if not account.enabled:
                raise HTTPException(422, f"selected account {account_id!r} is disabled")
            if (
                spec.account.binding == "none"
                and account_id in durable_binding_ids
            ):
                raise HTTPException(
                    422,
                    f"selected account {account_id!r} has a durable EIP binding; "
                    "use account.binding='eip' or explicitly decommission the "
                    "binding first",
                )
            decision = agent_api_decision(account)
            if decision.get("available") is False:
                raise HTTPException(
                    422,
                    f"selected account {account_id!r} is unavailable: "
                    f"{decision.get('reason') or 'quota'}",
                )
            if not supports_agent(
                account,
                spec.account.agent_type,
                spec.account.model,
            ):
                available_types = getattr(
                    account, "supported_agent_types",
                    [getattr(account, "agent_type", "claude")],
                )
                model_suffix = (
                    f" model {spec.account.model!r}"
                    if spec.account.model
                    else ""
                )
                raise HTTPException(
                    422,
                    f"selected account {account_id!r} supports "
                    f"{', '.join(available_types)}, not "
                    f"{spec.account.agent_type}{model_suffix}",
                )
        duplicate_ids = {
            account_id for account_id, count
            in Counter(spec.account.ids).items()
            if count > 1
        }
        for account_id in duplicate_ids:
            account = by_id[account_id]
            if (
                spec.account.binding != "none"
                or getattr(account, "auth_kind", "oauth") != "agent_api"
            ):
                raise HTTPException(
                    422,
                    f"selected OAuth/EIP account {account_id!r} cannot be "
                    "shared across worker slots",
                )
        if not spec.account.ids:
            eligible = [
                account for account in accounts
                if account.enabled
                and account.group == spec.account.group
                and (
                    spec.account.binding != "none"
                    or account.id not in durable_binding_ids
                )
                and supports_agent(
                    account,
                    spec.account.agent_type,
                    spec.account.model,
                )
                and agent_api_decision(account).get("available") is not False
            ]
            required = spec.fanout.workers * spec.account.per_worker
            eligible_api = [
                account for account in eligible
                if getattr(account, "auth_kind", "oauth") == "agent_api"
            ]
            eligible_oauth = [
                account for account in eligible
                if getattr(account, "auth_kind", "oauth") != "agent_api"
            ]
            if spec.account.binding == "none" and eligible_api:
                # Automatic allocation may reuse each API account once per
                # worker, but never twice among that worker's pre-login slots.
                # Explicit ids are a separate, administrator-chosen mapping
                # and may repeat one API id deliberately.
                available_slots = (
                    len(eligible_oauth)
                    + len(eligible_api) * spec.fanout.workers
                )
            else:
                available_slots = len(eligible_oauth) + len(eligible_api)
            if available_slots < required:
                raise HTTPException(
                    422,
                    f"account group {spec.account.group!r} has "
                    f"{len(eligible_oauth)} eligible OAuth account(s) and "
                    f"{len(eligible_api)} eligible Agent API account(s); "
                    f"this Job requires {required}",
                )

    warnings: list[str] = []
    if spec.setup.repo and not spec.setup.resolved_commit:
        warnings.append(
            "source is selected by a mutable branch/ref; set "
            "setup.resolved_commit for reproducible replay"
        )
    if spec.setup.repo and spec.setup.deliver == "worker_clone":
        warnings.append(
            "worker_clone receives no Manager Git credential; private "
            "repositories must use setup.deliver='manager_rsync'"
        )
    if not spec.collect.paths:
        warnings.append(
            "collect.paths is empty; command stdout remains in worker logs but "
            "no Job data files will be collected"
        )
    elif spec.collect.interval_seconds == 0:
        warnings.append(
            "results are collected only at process exit; set an interval for "
            "long-running Jobs that need partial-result durability"
        )
    if (
        provider.type == "aws"
        and spec.account.mode == "worker_local_login"
        and spec.account.binding == "none"
    ):
        warnings.append(
            "account.binding='none' uses a temporary public IP and excludes "
            "identities with durable EIP bindings; use binding='eip' for "
            "stable login identity"
        )

    if spec.collect.checkpoint:
        collection_mode = "manager-relay-s3-checkpoint"
    elif results_bucket and provider.type == "aws" and worker_profile:
        collection_mode = "worker-direct-s3"
    elif results_bucket:
        collection_mode = "manager-relay-s3"
    else:
        collection_mode = "manager-local-only"
        if spec.collect.paths:
            warnings.append(
                "ELASTIC_AGENT_RESULTS_S3_BUCKET is not configured; collected "
                "files remain on the Manager and are not uploaded to S3"
            )

    ctx = spec.worker_contexts()[0]
    # A plan has no real worker yet, but hostname is a valid render variable.
    # Use a conspicuous stable preview value rather than passing an empty
    # hostname into the runtime fail-closed dataset renderer.
    ctx.hostname = "plan-worker-00000"
    command_preview = spec.render_command(ctx)
    dataset_preview = spec.render_s3_datasets(ctx)
    return {
        "valid": True,
        "side_effects": False,
        "environment": spec.environment.manifest(),
        "source": {
            "repo": spec.setup.repo,
            "ref": spec.setup.checkout_ref if spec.setup.repo else None,
            "resolved_commit": spec.setup.resolved_commit or None,
            "target_dir": spec.setup.target_dir,
            "delivery": spec.setup.deliver,
        },
        "setup_steps": [
            {
                "name": step.name,
                "cwd": step.cwd,
                "timeout": step.timeout,
                "retries": step.retries,
                "run_as": step.run_as,
                "env_keys": sorted(step.env),
            }
            for step in spec.setup.normalized_steps()
        ],
        "datasets": [
            {"uri": dataset.uri, "dest": dataset.dest}
            for dataset in dataset_preview
        ],
        "run": {
            "command": command_preview,
            "cwd": spec.resolved_cwd(),
            "shell": spec.run.shell,
            "timeout_seconds": spec.run.timeout,
            "env_keys": sorted(spec.run.env),
            "secret_env_keys": sorted(spec.run.secret_env),
        },
        "lifecycle": {"ttl_seconds": spec.ttl_seconds},
        "fanout": {
            "workers": spec.fanout.workers,
            "region": configured_region,
            "instance_type": effective_instance_type,
            "instance_type_allowlist": sorted(allowed_instance_types),
            "disk_gb": spec.fanout.disk_gb or "manager-default",
            "spot": spec.fanout.spot,
            "provider_max_instances": max_instances,
            "worst_case_worker_hours": worst_case_worker_hours,
            "max_job_worker_hours": max_worker_hours,
        },
        "results": {
            "paths": list(spec.collect.paths),
            "exclude": list(spec.collect.exclude),
            "interval_seconds": spec.collect.interval_seconds,
            "checkpoint": spec.collect.checkpoint,
            "mode": collection_mode,
            "s3_bucket": results_bucket or None,
            "automatic_final_collect": bool(spec.collect.paths),
        },
        "recovery": recovery_preview,
        "warnings": warnings,
    }


def _pin_preflight_checkpoint_generation(
    spec: JobSpec,
    plan: dict,
) -> JobSpec:
    """Freeze a ``latest`` recovery request to the set preflight resolved.

    The caller's request fingerprint deliberately remains based on the public
    request (where an empty generation means "resolve once").  The private
    JobSpec persisted before launch must instead contain that exact immutable
    generation so staging, crash replay, and idempotent retry cannot drift to a
    newer set.
    """

    if (
        spec.recovery.policy != "checkpoint"
        or spec.recovery.generation
    ):
        return spec
    recovery = plan.get("recovery")
    generation = (
        recovery.get("resolved_generation")
        if isinstance(recovery, dict)
        else None
    )
    if not isinstance(generation, str) or not generation:
        raise HTTPException(
            500,
            "checkpoint preflight did not return a resolved generation",
        )
    payload = spec.model_dump(mode="json")
    payload["recovery"]["generation"] = generation
    try:
        return JobSpec.model_validate(payload)
    except ValidationError as exc:
        raise HTTPException(
            422,
            "requested complete checkpoint set is unavailable",
        ) from exc


@router.post("/jobs/plan")
async def plan_job(spec: JobSpec) -> dict:
    """Validate and preview a Job without persistence/cloud/account mutation."""
    _validate_job_stdin_protocol(spec, allow_stdin_protocol=False)
    return await _preflight_job(_mgr(), spec)


@router.post(
    "/jobs",
    status_code=201,
    openapi_extra={
        "requestBody": {
            "required": True,
            "content": {
                "application/json": {
                    "schema": {"$ref": "#/components/schemas/JobSpec"},
                },
            },
        },
    },
)
async def submit_job(
    request: Request,
    idempotency_key: str | None = Header(
        default=None, alias="Idempotency-Key"
    ),
) -> dict:
    """Replay history or validate + launch a new batch Job."""

    raw_request = await _read_bounded_job_request(request)
    return await _submit_job_payload(raw_request, idempotency_key)


@router.post("/jobs/run-benchmark", status_code=201)
async def submit_run_benchmark_job(
    request: RunBenchmarkJobRequest,
    idempotency_key: str | None = Header(
        default=None, alias="Idempotency-Key"
    ),
) -> dict:
    """Launch one sealed Run-Benchmark attempt on an ephemeral Worker.

    This endpoint is intentionally narrower than ``POST /jobs``: repository,
    command, environment, S3 namespace, fan-out, account mode, and collection
    paths are all Manager-owned.  Only the one-shot binary credential frame is
    delegated to the process-local lease store.
    """

    if idempotency_key is None or not idempotency_key.strip():
        raise HTTPException(422, "Idempotency-Key is required")
    mgr = _mgr()
    transport_error = sensitive_transport_error(
        mgr, feature="Run-Benchmark credential input"
    )
    if transport_error:
        raise HTTPException(503, transport_error)

    frame, public = _decode_run_benchmark_frame(request.credential_frame)
    adopted = False
    try:
        public = _validate_run_benchmark_public_request(request, public)
        raw_spec = _run_benchmark_job_spec(request, public)
        identity_request = request.model_dump(
            mode="json", exclude={"credential_frame"}
        )
        identity_request["request_kind"] = "run-benchmark-v1"

        async def install_lease(job, recovered_prepared: bool) -> None:
            nonlocal adopted
            if recovered_prepared:
                raise HTTPException(
                    409,
                    "prepared Run-Benchmark Job lost its ephemeral credential; "
                    "submit a new attempt with a new Idempotency-Key",
                )
            try:
                mgr.ephemeral_stdin_leases.put(
                    job.job_id,
                    frame,
                    ttl_seconds=float(raw_spec["ttl_seconds"]),
                )
            except EphemeralStdinLeaseError as exc:
                raise HTTPException(
                    503, "ephemeral credential lease is unavailable"
                ) from exc
            adopted = True

        async def discard_lease(job) -> None:
            mgr.ephemeral_stdin_leases.discard(job.job_id)

        return await _submit_job_payload(
            raw_spec,
            idempotency_key,
            identity_request=identity_request,
            before_submit=install_lease,
            on_submit_error=discard_lease,
            allow_stdin_protocol=True,
        )
    finally:
        if not adopted:
            for index in range(len(frame)):
                frame[index] = 0


async def _submit_job_payload(
    raw_request: dict[str, object] | None,
    idempotency_key: str | None,
    *,
    identity_request: dict[str, object] | None = None,
    raw_request_factory: (
        Callable[[], Awaitable[dict[str, object]]] | None
    ) = None,
    before_submit: (
        Callable[[object, bool], Awaitable[None]] | None
    ) = None,
    on_submit_error: (
        Callable[[object], Awaitable[None]] | None
    ) = None,
    allow_stdin_protocol: bool = False,
) -> dict:
    """Run one JobSpec through the canonical submit path.

    ``identity_request`` lets a server-side constructor durably bind an
    idempotency key to its small public request envelope instead of to the
    private JobSpec it synthesizes.  The factory remains lazy so an exact
    replay of an already accepted request can be answered from its journal
    without loading or revalidating the source Job.
    """

    if raw_request is None and (
        raw_request_factory is None or identity_request is None
    ):
        raise RuntimeError(
            "lazy Job submission requires an identity request and factory"
        )
    if raw_request is not None and raw_request_factory is not None:
        raise RuntimeError(
            "Job submission cannot provide both a request and factory"
        )
    fingerprint_request = (
        identity_request if identity_request is not None else raw_request
    )

    try:
        fingerprint = _request_fingerprint(fingerprint_request)
    except (TypeError, ValueError, RecursionError) as exc:
        raise HTTPException(422, "invalid Job JSON request body") from exc

    mgr = _mgr()
    idempotency_key = _normalize_idempotency_key(idempotency_key)

    # Unkeyed submissions have no shared identity to serialize. Keep their
    # potentially slow account/model/capacity probes outside the idempotency
    # lock so one provider cannot stall unrelated submitters.
    spec: JobSpec | None = None
    if not idempotency_key:
        if raw_request is None:
            assert raw_request_factory is not None
            raw_request = await raw_request_factory()
        spec = _validate_job_stdin_protocol(
            _validated_job_spec(raw_request),
            allow_stdin_protocol=allow_stdin_protocol,
        )
        plan = await _preflight_job(mgr, spec)
        spec = _pin_preflight_checkpoint_generation(spec, plan)

    async with _submit_lock:
        deterministic_id = None
        recovered_prepared = False
        if idempotency_key:
            deterministic_id = "job-" + hashlib.sha256(
                idempotency_key.encode("utf-8")
            ).hexdigest()[:32]
            persisted = _job_spec_path(mgr, deterministic_id)
            live = mgr.batch.get_job(deterministic_id)
            if persisted.is_file():
                try:
                    payload = await asyncio.to_thread(
                        _read_job_journal, persisted, deterministic_id,
                    )
                except (OSError, ValueError, json.JSONDecodeError) as exc:
                    raise HTTPException(
                        500, f"cannot read persisted Job {deterministic_id}"
                    ) from exc
                try:
                    matches, legacy_journal = _journal_request_match(
                        payload,
                        fingerprint_request,
                        fingerprint,
                    )
                except ValueError as exc:
                    raise HTTPException(
                        500,
                        f"invalid request fingerprint for persisted Job "
                        f"{deterministic_id}",
                    ) from exc
                if not matches:
                    raise _idempotency_conflict(legacy=legacy_journal)

                # The journal is authoritative for request identity, while a
                # live record proves scheduling already crossed the launch
                # boundary even if its durable state is momentarily prepared.
                if live is not None:
                    detail = _job_detail(live)
                    detail["idempotent_replay"] = True
                    return detail

                submission_state = _journal_state(payload)
                if submission_state == "prepared":
                    # The durable write won, but scheduling did not.  Reusing
                    # the same deterministic id is the safe continuation; a
                    # fresh id would violate the idempotency contract. The
                    # persisted private spec is also authoritative: preflight
                    # may have frozen a mutable "latest" checkpoint selector
                    # before the crash, and replaying the public request must
                    # not resolve it again to a different generation.
                    recovered_prepared = True
                    persisted_spec = payload.get("spec")
                    if not isinstance(persisted_spec, dict):
                        raise HTTPException(
                            500,
                            f"invalid persisted spec for Job "
                            f"{deterministic_id}",
                        )
                    # This applies equally to direct /jobs submissions and the
                    # server-side /jobs/recover constructor. The already
                    # matched public request proves identity; only this exact
                    # accepted snapshot is eligible for rescheduling.
                    raw_request = copy.deepcopy(persisted_spec)
                else:
                    leases = await mgr.account_binding_store.list_leases()
                    detail = _persisted_job_view(
                        deterministic_id,
                        payload,
                        [lease for lease in leases if lease.job_id == deterministic_id],
                        include_spec=True,
                    )
                    detail["idempotent_replay"] = True
                    return detail
            elif live is not None:
                # Compatibility only for an in-memory Job created by an older
                # integration that did not install the persistence hook.
                # Without a raw fingerprint, require strict legacy-normalized
                # equality and otherwise fail closed.
                matches, _legacy = _journal_request_match(
                    {"spec": live.spec.model_dump(mode="json")},
                    fingerprint_request,
                    fingerprint,
                )
                if not matches:
                    raise _idempotency_conflict(legacy=True)
                detail = _job_detail(live)
                detail["idempotent_replay"] = True
                return detail

        # Exact replays of a live or durable Job above are historical reads:
        # the current schema and policy must not invalidate a Job that already
        # launched. New submissions and recovery of a merely prepared journal
        # do require current validation and the complete preflight.
        if spec is None:
            if raw_request is None:
                assert raw_request_factory is not None
                raw_request = await raw_request_factory()
            spec = _validate_job_stdin_protocol(
                _validated_job_spec(raw_request),
                allow_stdin_protocol=allow_stdin_protocol,
            )
            plan = await _preflight_job(mgr, spec)
            spec = _pin_preflight_checkpoint_generation(spec, plan)

        job = None
        try:
            # The raw request fingerprint is carried into the very first
            # atomic prepared journal, before registration, account claims, or
            # cloud calls. This applies to keyed and unkeyed submissions.
            job = mgr.batch.prepare(spec)
            job.request_fingerprint = fingerprint
            if deterministic_id:
                job.job_id = deterministic_id
            if before_submit is not None:
                await before_submit(job, recovered_prepared)
            await mgr.batch.submit_prepared(job)
        except HTTPException:
            if job is not None and on_submit_error is not None:
                await on_submit_error(job)
            raise
        except JobSpecPersistenceError as exc:
            if job is not None and on_submit_error is not None:
                await on_submit_error(job)
            raise HTTPException(500, str(exc)) from exc
        except NotImplementedError as exc:
            if job is not None and on_submit_error is not None:
                await on_submit_error(job)
            raise HTTPException(503, str(exc)) from exc
        except BaseException:
            if job is not None and on_submit_error is not None:
                await on_submit_error(job)
            raise
        detail = _job_detail(job)
        if recovered_prepared:
            detail["idempotent_replay"] = True
        return detail


async def _load_private_recovery_source(mgr, source_job_id: str) -> dict:
    """Read one raw persisted source spec without exposing it through REST."""

    source_job_id = _validate_job_id(source_job_id)
    path = _job_spec_path(mgr, source_job_id)
    if not _job_journal_exists(path):
        raise HTTPException(
            404,
            f"no persisted spec for source Job {source_job_id}",
        )
    permit = _acquire_result_operation(
        _JOB_HISTORY_ADMISSION,
        operation="recovery source read",
    )
    try:
        return await _run_owned_executor(
            _JOB_HISTORY_EXECUTOR,
            _read_job_journal,
            path,
            source_job_id,
        )
    except (
        json.JSONDecodeError,
        OSError,
        RecursionError,
        RuntimeError,
        UnicodeDecodeError,
        ValueError,
    ) as exc:
        raise HTTPException(
            500,
            f"persisted source Job {source_job_id} is unavailable",
        ) from exc
    finally:
        permit.release()


def _build_checkpoint_recovery_spec(
    source_job_id: str,
    source_payload: dict,
    request: RecoveryJobRequest,
) -> dict[str, object]:
    """Clone the private source spec and apply the narrow recovery whitelist."""

    try:
        source_spec = JobSpec.model_validate(source_payload["spec"])
    except (
        KeyError,
        RecursionError,
        TypeError,
        ValidationError,
        ValueError,
    ) as exc:
        # Pydantic's detailed error includes rejected input values. Keep raw
        # environment values and secret references on the Manager.
        raise HTTPException(
            422,
            "persisted source JobSpec is incompatible with checkpoint recovery",
        ) from exc
    if source_spec.harness_ref:
        raise HTTPException(
            422,
            "server-side checkpoint recovery supports declarative Jobs only",
        )
    if not source_spec.collect.checkpoint or not source_spec.collect.paths:
        raise HTTPException(
            422,
            "source Job did not enable immutable checkpoint collection",
        )

    target = source_spec.model_dump(mode="json")
    target["recovery"] = {
        "policy": "checkpoint",
        "source_job_id": source_job_id,
        "paths": list(source_spec.collect.paths),
        "generation": request.generation,
    }
    run = target["run"]
    if request.run.command is not None:
        run["command"] = request.run.command
    if request.run.timeout is not None:
        run["timeout"] = request.run.timeout
    if request.ttl_seconds is not None:
        target["ttl_seconds"] = request.ttl_seconds
    try:
        # Validate the synthesized request before it reaches fingerprinting or
        # preflight. This also normalizes whitespace in generation/command.
        normalized = JobSpec.model_validate(target).model_dump(mode="json")
    except (RecursionError, TypeError, ValidationError, ValueError) as exc:
        raise HTTPException(
            422,
            "checkpoint recovery overrides are incompatible with the source Job",
        ) from exc
    try:
        encoded = json.dumps(
            normalized,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except (RecursionError, TypeError, UnicodeEncodeError, ValueError) as exc:
        raise HTTPException(
            422,
            "checkpoint recovery JobSpec cannot be serialized safely",
        ) from exc
    if len(encoded) > JOB_SUBMIT_MAX_BODY_BYTES:
        raise HTTPException(
            413,
            "checkpoint recovery JobSpec exceeds the current Job request limit",
        )
    return normalized


def _checkpoint_recovery_request_identity(
    source_job_id: str,
    request: RecoveryJobRequest,
) -> dict[str, object]:
    """Return the stable public identity of one recovery submission."""

    command = request.run.command
    return {
        "request_kind": "checkpoint-recovery",
        "schema_version": 1,
        "source_job_id": source_job_id,
        "generation": request.generation.strip(),
        "run": {
            "command": command.strip() if command is not None else None,
            "timeout": request.run.timeout,
        },
        "ttl_seconds": request.ttl_seconds,
    }


def _build_suspended_resume_spec(
    source_job_id: str,
    source_payload: dict,
    request: ResumeJobRequest,
) -> dict[str, object]:
    """Build a continuation only from the exact verified suspend generation."""

    terminal_summary = source_payload.get("terminal_summary")
    if not isinstance(terminal_summary, dict):
        terminal_summary = {}
    state = source_payload.get("submission_state")
    latest_generation = source_payload.get(
        "latest_checkpoint_generation"
    ) or terminal_summary.get("latest_checkpoint_generation")
    resume = _verified_resume_fields(
        state=str(state or ""),
        cleanup_pending=int(terminal_summary.get("cleanup_pending") or 0),
        latest_generation=latest_generation,
        latest_committed_at=source_payload.get("checkpoint_committed_at"),
        metadata=terminal_summary,
    )
    if state != "suspended":
        raise HTTPException(
            409,
            f"source Job {source_job_id} is not suspended",
        )
    if not resume["resume_available"]:
        raise HTTPException(
            409,
            "source Job has no verified resumable checkpoint",
        )
    if not hmac.compare_digest(
        request.resume_generation,
        str(resume["resume_generation"]),
    ):
        raise HTTPException(
            409,
            "resume_generation does not match the source Job's verified "
            "suspend generation",
        )
    try:
        source_spec = JobSpec.model_validate(source_payload["spec"])
    except (
        KeyError,
        RecursionError,
        TypeError,
        ValidationError,
        ValueError,
    ) as exc:
        raise HTTPException(
            422,
            "persisted source JobSpec is incompatible with suspended resume",
        ) from exc
    resume_command = source_spec.run.resume_command
    if not isinstance(resume_command, str) or not resume_command.strip():
        raise HTTPException(
            422,
            "source Job did not configure run.resume_command; it cannot be "
            "resumed automatically",
        )
    recovery_request = RecoveryJobRequest(
        source_job_id=source_job_id,
        generation=request.resume_generation,
        run=RecoveryRunOverrides(command=resume_command),
    )
    target = _build_checkpoint_recovery_spec(
        source_job_id,
        source_payload,
        recovery_request,
    )
    # The resumed attempt's base command is already the application's complete
    # resume command. A later credential rotation must rerun that same base,
    # not append the source attempt's legacy resume_args a second time.
    rotation = target.get("rotation")
    if isinstance(rotation, dict):
        rotation["resume_args"] = ""
    return target


def _suspended_resume_request_identity(
    source_job_id: str,
    request: ResumeJobRequest,
) -> dict[str, object]:
    return {
        "request_kind": "suspended-job-resume",
        "schema_version": 1,
        "source_job_id": source_job_id,
        "resume_generation": request.resume_generation,
    }


@router.post("/jobs/recover", status_code=201)
async def create_recovery_job(
    request: RecoveryJobRequest,
    idempotency_key: str | None = Header(
        default=None, alias="Idempotency-Key"
    ),
) -> dict:
    """Create a new Job from a private source spec and an immutable checkpoint.

    Unlike the redacted Job detail response, this path never asks the browser
    to reconstruct environment values or secret references. They remain in the
    Manager's private journal and are copied only into the new private journal.
    """

    mgr = _mgr()
    source_job_id = _validate_job_id(request.source_job_id)
    identity_request = _checkpoint_recovery_request_identity(
        source_job_id,
        request,
    )

    async def build_target() -> dict[str, object]:
        source_payload = await _load_private_recovery_source(
            mgr,
            source_job_id,
        )
        return _build_checkpoint_recovery_spec(
            source_job_id,
            source_payload,
            request,
        )

    return await _submit_job_payload(
        None,
        idempotency_key,
        identity_request=identity_request,
        raw_request_factory=build_target,
    )


@router.post("/jobs/{job_id}/resume", status_code=201)
async def resume_suspended_job(
    job_id: str,
    request: ResumeJobRequest,
    idempotency_key: str | None = Header(
        default=None, alias="Idempotency-Key"
    ),
) -> dict:
    """Create a new attempt from one exact, verified suspend checkpoint."""

    source_job_id = _validate_job_id(job_id)
    idempotency_key = _normalize_idempotency_key(
        idempotency_key,
        required=True,
    )
    assert idempotency_key is not None
    mgr = _mgr()
    identity_request = _suspended_resume_request_identity(
        source_job_id,
        request,
    )

    async def build_target() -> dict[str, object]:
        source_payload = await _load_private_recovery_source(
            mgr,
            source_job_id,
        )
        return _build_suspended_resume_spec(
            source_job_id,
            source_payload,
            request,
        )

    return await _submit_job_payload(
        None,
        idempotency_key,
        identity_request=identity_request,
        raw_request_factory=build_target,
    )


@router.post("/jobs/{job_id}/interrupt", status_code=202)
async def interrupt_job(
    job_id: str,
    idempotency_key: str | None = Header(
        default=None, alias="Idempotency-Key"
    ),
) -> dict:
    """Start a durable graceful interrupt and return after intent persistence."""

    job_id = _validate_job_id(job_id)
    idempotency_key = _normalize_idempotency_key(
        idempotency_key,
        required=True,
    )
    assert idempotency_key is not None
    idempotency_digest = hashlib.sha256(
        idempotency_key.encode("utf-8")
    ).hexdigest()
    mgr = _mgr()
    job = mgr.batch.get_job(job_id)
    request_cancellation: asyncio.CancelledError | None = None
    start_interrupt = getattr(mgr.batch, "start_interrupt_job", None)
    if job is not None and not callable(start_interrupt):
        raise HTTPException(
            503,
            "graceful Job interruption is unavailable on this Manager",
        )

    async with _manager_job_action_lock(mgr):
        try:
            target_payload = await asyncio.to_thread(
                _load_interrupt_journal_optional,
                mgr,
                job_id,
            )
            target_digest = (
                _authoritative_interrupt_digest(target_payload)
                if target_payload is not None
                else None
            )
            try:
                sidecar_owner = await asyncio.to_thread(
                    _read_job_action_index,
                    mgr,
                    operation="interrupt",
                    digest=idempotency_digest,
                )
            except (
                HTTPException,
                json.JSONDecodeError,
                OSError,
                RecursionError,
                RuntimeError,
                UnicodeDecodeError,
                ValueError,
            ):
                # This file is only a rebuildable accelerator.  A malformed,
                # stale, or unreadable cache must never override a valid Job
                # journal or make an exact authoritative replay unavailable.
                sidecar_owner = None
            authoritative_owners: set[str] = set()

            if target_digest is not None:
                if not hmac.compare_digest(
                    target_digest,
                    idempotency_digest,
                ):
                    raise HTTPException(
                        409,
                        "Job interrupt is already bound to another "
                        "Idempotency-Key",
                    )
                authoritative_owners.add(job_id)

            intent_index = await asyncio.to_thread(
                _read_interrupt_intent_index,
                mgr,
            )
            indexed_owner = intent_index.get(idempotency_digest)
            if indexed_owner is not None:
                authoritative_owners.add(indexed_owner)
            if len(authoritative_owners) > 1:
                raise HTTPException(
                    503,
                    "interrupt Idempotency-Key has conflicting authoritative "
                    "Job journals",
                )
            authoritative_owner = next(
                iter(authoritative_owners),
                None,
            )
            if authoritative_owner is not None and authoritative_owner != job_id:
                raise HTTPException(
                    409,
                    "Idempotency-Key was already used for another Job action",
                )
        except HTTPException:
            raise
        except (
            json.JSONDecodeError,
            OSError,
            RecursionError,
            RuntimeError,
            UnicodeDecodeError,
            ValueError,
        ) as exc:
            raise HTTPException(
                503,
                "interrupt request identity cannot be verified safely",
            ) from exc

        replay = target_digest == idempotency_digest
        if job is not None:
            assert callable(start_interrupt)
            pending_digest = str(
                getattr(job, "interrupt_idempotency_digest", "") or ""
            )
            if pending_digest and not hmac.compare_digest(
                pending_digest,
                idempotency_digest,
            ):
                raise HTTPException(
                    409,
                    "Job interrupt is already bound to another "
                    "Idempotency-Key",
                )
            try:
                (
                    interrupted,
                    request_cancellation,
                ) = await _settle_owned_job_action(
                    start_interrupt(
                        job_id,
                        reason="interrupted by administrator",
                        idempotency_digest=idempotency_digest,
                    )
                )
            except (RuntimeError, ValueError) as exc:
                raise HTTPException(409, str(exc)) from exc
            committed_payload = await asyncio.to_thread(
                _load_interrupt_journal_optional,
                mgr,
                job_id,
            )
            committed_digest = (
                _authoritative_interrupt_digest(committed_payload)
                if committed_payload is not None
                else None
            )
            if (
                committed_digest is None
                or not hmac.compare_digest(
                    committed_digest,
                    idempotency_digest,
                )
            ):
                raise HTTPException(
                    500,
                    "interrupt intent was not committed atomically",
                )
            target_payload = committed_payload
        else:
            interrupted = None
            if not replay:
                if sidecar_owner is not None:
                    raise HTTPException(
                        409,
                        "interrupt action cache exists, but the Job journal "
                        "does not contain a committed interrupt intent",
                    )
                if target_payload is None:
                    raise HTTPException(
                        404,
                        f"Job {job_id} not found or no longer live",
                    )
                raise HTTPException(
                    409,
                    "Job has no committed interrupt request for this "
                    "Idempotency-Key",
                )

        intent_index[idempotency_digest] = job_id
        try:
            await asyncio.to_thread(
                _write_job_action_index,
                mgr,
                operation="interrupt",
                digest=idempotency_digest,
                job_id=job_id,
            )
        except (
            json.JSONDecodeError,
            OSError,
            RecursionError,
            RuntimeError,
            UnicodeDecodeError,
            ValueError,
        ):
            # The atomic Job journal is already authoritative.  Cache
            # publication failure is recoverable by the next full scan and
            # must not turn an accepted interrupt into an HTTP failure.
            logger.warning(
                "could not publish rebuildable interrupt action cache for %s",
                job_id,
                exc_info=True,
            )
        if request_cancellation is not None:
            raise request_cancellation

    if job is None:
        assert target_payload is not None
        leases = await mgr.account_binding_store.list_leases(
            job_ids={job_id},
            limit=JOB_DETAIL_MAX_RECOVERY_LEASES + 1,
        )
        detail = _persisted_job_view(
            job_id,
            target_payload,
            leases[:JOB_DETAIL_MAX_RECOVERY_LEASES],
            include_spec=True,
            recovery_leases_truncated=(
                len(leases) > JOB_DETAIL_MAX_RECOVERY_LEASES
            ),
        )
        detail["idempotent_replay"] = True
        return detail

    if interrupted is None:
        raise HTTPException(
            409,
            f"Job {job_id} could not enter graceful interruption",
        )
    detail = _job_detail(interrupted)
    if replay:
        detail["idempotent_replay"] = True
    return detail


@router.post("/jobs/{job_id}/cancel")
async def cancel_job(job_id: str) -> dict:
    """Idempotently stop, collect partial results, and destroy a live Job."""
    job_id = _validate_job_id(job_id)
    mgr = _mgr()
    job = mgr.batch.get_job(job_id)
    if job is None:
        raise HTTPException(404, f"Job {job_id} not found or no longer live")
    if not await mgr.batch.cancel_job(job_id, reason="cancelled by administrator"):
        raise HTTPException(409, f"Job {job_id} could not be cancelled")
    return _job_detail(job)


@router.post("/jobs/{job_id}/resubmit", status_code=201)
async def resubmit_job(job_id: str) -> dict:
    """Relaunch a job from its persisted spec — works even after a Manager
    restart wiped the in-memory record."""
    job_id = _validate_job_id(job_id)
    mgr = _mgr()
    p = _job_spec_path(mgr, job_id)
    if not p.exists():
        raise HTTPException(404, f"no persisted spec for job {job_id}")
    try:
        payload = await asyncio.to_thread(_read_job_journal, p, job_id)
        spec = JobSpec(**payload["spec"])
    except (OSError, KeyError, ValueError, json.JSONDecodeError) as exc:
        raise HTTPException(500, f"invalid persisted spec for job {job_id}") from exc
    plan = await _preflight_job(mgr, spec)
    spec = _pin_preflight_checkpoint_generation(spec, plan)
    try:
        job = await mgr.batch.submit(spec)
    except JobSpecPersistenceError as exc:
        raise HTTPException(500, str(exc)) from exc
    except NotImplementedError as exc:
        raise HTTPException(503, str(exc)) from exc
    return _job_detail(job)


@router.get("/jobs")
async def list_jobs() -> dict:
    mgr = _mgr()
    live = mgr.batch.list_jobs()
    live_ids = {j.job_id for j in live}
    # Include workers_detail inline (via _job_list_item) so the UI renders each
    # job card straight from this one response instead of firing a detail request
    # per job — that per-job fan-out floods the Manager and, once many jobs pile
    # up, makes the jobs panel silently fail to render ("No jobs yet").
    out = [_job_list_item(j) for j in live]
    # Persisted specs whose jobs are no longer in memory (restarted) are
    # useful history, but this directory grows for the lifetime of the
    # Manager. Scan and parse it under explicit aggregate budgets in a
    # dedicated executor; fail fast instead of queuing unlimited UI polls.
    permit = _acquire_result_operation(
        _JOB_HISTORY_ADMISSION,
        operation="Job history read",
    )
    try:
        history = await _run_owned_executor(
            _JOB_HISTORY_EXECUTOR,
            _load_historical_job_journals,
            _specs_dir(mgr),
            frozenset(live_ids),
        )
        history_ids = {
            job_id for job_id, _payload in history["entries"]
        }
        leases_by_job: dict[str, list] = {}
        leases_truncated = False
        if history_ids:
            # The binding store already owns a bounded durable state file.
            # Group only leases relevant to the bounded history snapshot so
            # the response cannot include unrelated lease metadata.
            relevant_leases = await mgr.account_binding_store.list_leases(
                job_ids=history_ids,
                limit=JOB_LIST_HISTORY_MAX_LEASES + 1,
            )
            if len(relevant_leases) > JOB_LIST_HISTORY_MAX_LEASES:
                leases_truncated = True
                relevant_leases = relevant_leases[
                    :JOB_LIST_HISTORY_MAX_LEASES
                ]
            for lease in relevant_leases:
                leases_by_job.setdefault(lease.job_id, []).append(lease)

        response_bytes = 2  # JSON array brackets
        history_returned = 0
        response_truncated = False
        for job_id, data in history["entries"]:
            try:
                view = _persisted_job_view(
                    job_id,
                    data,
                    leases_by_job.get(job_id, []),
                    include_spec=False,
                )
                encoded = json.dumps(
                    view,
                    ensure_ascii=False,
                    allow_nan=False,
                    separators=(",", ":"),
                    default=str,
                ).encode("utf-8")
            except (
                RecursionError,
                TypeError,
                UnicodeEncodeError,
                ValueError,
            ):
                logger.warning(
                    "Ignoring invalid historical Job view %s",
                    job_id,
                )
                continue
            next_size = response_bytes + len(encoded) + bool(history_returned)
            if next_size > JOB_LIST_HISTORY_MAX_RESPONSE_BYTES:
                response_truncated = True
                continue
            response_bytes = next_size
            out.append(view)
            history_returned += 1

        truncated = bool(
            history["truncated"]
            or response_truncated
            or leases_truncated
        )
        return {
            "jobs": out,
            # Preserve the existing UI/API meaning: number of returned rows.
            "total": len(out),
            "truncated": truncated,
            "history_scanned": history["scanned"],
            "history_returned": history_returned,
        }
    finally:
        permit.release()


@router.get("/jobs/{job_id}")
async def get_job(job_id: str, response: Response) -> dict:
    try:
        job_id = _validate_job_id(job_id)
    except HTTPException as exc:
        exc.headers = {
            **(exc.headers or {}),
            **JOB_CONFIG_NO_STORE_HEADERS,
        }
        raise
    response.headers.update(JOB_CONFIG_NO_STORE_HEADERS)
    mgr = _mgr()
    job = mgr.batch.get_job(job_id)
    p = _job_spec_path(mgr, job_id)
    if job is not None:
        # A persisted snapshot is authoritative even while the Job is live:
        # runtime code may mutate its in-memory model after submission.  The
        # fallback exists only for older/test integrations that created an
        # in-memory Job without installing the persistence hook.
        if not _job_journal_exists(p):
            return _job_detail(job)
        try:
            _data, submitted_spec = await _read_job_journal_for_detail(
                p,
                job_id,
            )
            detail = _job_detail(job, include_spec=False)
            detail["spec"] = submitted_spec
            return detail
        except HTTPException:
            raise
        except (OSError, RecursionError, RuntimeError, ValueError) as exc:
            raise HTTPException(
                500,
                f"persisted Job config is unavailable for job {job_id}",
                headers=dict(JOB_CONFIG_NO_STORE_HEADERS),
            ) from exc

    # Fall back to the persisted spec (job gone from memory after a restart).
    if _job_journal_exists(p):
        try:
            data, submitted_spec = await _read_job_journal_for_detail(
                p,
                job_id,
            )
            leases = await mgr.account_binding_store.list_leases(
                job_ids={job_id},
                limit=JOB_DETAIL_MAX_RECOVERY_LEASES + 1,
            )
            leases_truncated = len(leases) > JOB_DETAIL_MAX_RECOVERY_LEASES
            recovered = leases[:JOB_DETAIL_MAX_RECOVERY_LEASES]
            detail = _persisted_job_view(
                job_id,
                data,
                recovered,
                include_spec=False,
                recovery_leases_truncated=leases_truncated,
            )
            detail["spec"] = submitted_spec
            return detail
        except HTTPException:
            raise
        except (OSError, RecursionError, RuntimeError, ValueError) as exc:
            raise HTTPException(
                500,
                f"persisted Job config is unavailable for job {job_id}",
                headers=dict(JOB_CONFIG_NO_STORE_HEADERS),
            ) from exc
    raise HTTPException(
        404,
        f"Job {job_id} not found",
        headers=dict(JOB_CONFIG_NO_STORE_HEADERS),
    )


@router.get("/jobs/{job_id}/logs")
async def job_logs(
    job_id: str,
    response: Response,
    worker_id: str | None = Query(default=None, max_length=512),
    task_id: str | None = Query(default=None, max_length=1_024),
    lines: int = Query(default=400, ge=1, le=5_000),
) -> dict:
    """Return bounded run stdout/stderr after the ephemeral Worker is gone."""

    job_id = _validate_job_id(job_id)
    mgr = _mgr()
    job = mgr.batch.get_job(job_id)
    persisted = _job_spec_path(mgr, job_id).exists()
    if job is None and not persisted:
        raise HTTPException(404, f"Job {job_id} not found")
    if task_id is not None and not task_id.startswith(f"{job_id}:"):
        # Do not disclose whether a task from another Job exists.
        raise HTTPException(404, "task output not found for this Job")

    response.headers["Cache-Control"] = "no-store"
    permit = _acquire_result_operation(
        _JOB_LOG_READ_ADMISSION,
        operation="Job log read",
    )
    try:
        archived = await _run_owned_executor(
            _JOB_LOG_READ_EXECUTOR,
            mgr.job_log_store.read_job_tail,
            job_id,
            lines=lines,
            worker_id=worker_id,
            task_id=task_id,
        )
    finally:
        # `_run_owned_executor` shields a running thread through request
        # cancellation, so this token cannot be returned while the disk scan
        # still occupies its dedicated worker.
        permit.release()
    archived_by_task = {
        str(snapshot["task_id"]): snapshot
        for snapshot in archived["tasks"]
    }

    # A task remains in the live parser until its fsynced PROCESS_EXIT snapshot
    # succeeds.  Older attempts from credential rotation may already be
    # archived while the latest attempt is still running.
    live_by_task: dict[str, list[dict]] = {}
    live_totals: dict[str, int] = {}
    for candidate in mgr.log_event_parser.active_tasks:
        if not candidate.startswith(f"{job_id}:"):
            continue
        if task_id is not None and candidate != task_id:
            continue
        # A completed task snapshot is authoritative; reliable LOG ordering
        # makes a same-id live buffer a duplicate replay rather than a new run.
        if candidate in archived_by_task:
            continue
        entries = mgr.log_event_parser.get_task_logs(candidate, limit=lines)
        if worker_id is not None and not any(
            entry.get("worker_id") == worker_id for entry in entries
        ):
            continue
        live_by_task[candidate] = entries
        live_totals[candidate] = mgr.log_event_parser.buffer_size(candidate)

    # Keep only the globally newest ``lines`` while visiting each active task's
    # already-bounded tail.  Archive reads use the same heap strategy in a
    # worker thread, so API memory is independent of total Job history size.
    newest: list[tuple[tuple[str, str, int, int], int, dict]] = []
    serial = 0
    retained_bytes = 0
    response_truncated = False

    def retain(entry: dict, candidate: str, ordinal: int) -> None:
        nonlocal retained_bytes, response_truncated, serial
        data = str(entry.get("data") or "")
        raw_data = data.encode("utf-8")
        if len(raw_data) > JOB_LOG_LINE_MAX_BYTES:
            data = (
                raw_data[: JOB_LOG_LINE_MAX_BYTES]
                .decode("utf-8", errors="ignore")
                + "\n[… live log line truncated …]"
            )
            raw_data = data.encode("utf-8")
            response_truncated = True
        item = {
            "task_id": candidate,
            "worker_id": str(entry.get("worker_id") or ""),
            "stream": (
                "stderr" if entry.get("stream") == "stderr" else "stdout"
            ),
            "data": data,
            "timestamp": str(entry.get("timestamp") or ""),
        }
        key = (item["timestamp"], candidate, ordinal, serial)
        serial += 1
        item_bytes = len(raw_data)
        heapq.heappush(newest, (key, item_bytes, item))
        retained_bytes += item_bytes
        while (
            len(newest) > lines
            or retained_bytes > JOB_LOG_RESPONSE_MAX_BYTES
        ):
            _old_key, old_bytes, _old_item = heapq.heappop(newest)
            retained_bytes -= old_bytes

    for ordinal, entry in enumerate(archived["entries"]):
        retain(entry, str(entry.get("task_id") or ""), ordinal)

    tasks: list[dict] = []
    sources: set[str] = set()
    if archived_by_task:
        sources.add("archive")
    for candidate, snapshot in archived_by_task.items():
        exit_info = snapshot.get("exit", {})
        tasks.append({
            "task_id": candidate,
            "worker_id": str(snapshot.get("worker_id") or ""),
            "archived": True,
            "complete": bool(snapshot.get("complete")),
            "exit_code": exit_info.get("exit_code"),
            "error_type": exit_info.get("error_type"),
            "error_message": exit_info.get("error_message"),
        })
    for candidate, entries in live_by_task.items():
        sources.add("live")
        selected_worker = str(entries[0].get("worker_id") or "") if entries else ""
        tasks.append({
            "task_id": candidate,
            "worker_id": selected_worker,
            "archived": False,
            "complete": False,
            "exit_code": None,
            "error_type": None,
            "error_message": None,
        })
        for ordinal, entry in enumerate(entries):
            retain(entry, candidate, ordinal)

    returned_entries = [
        item for _key, _size, item in sorted(newest)
    ]
    total = int(archived["total"]) + sum(live_totals.values())
    tasks.sort(key=lambda item: item["task_id"])

    scope_active = False
    if job is not None:
        if task_id is not None:
            scope_active = any(
                run.task_id == task_id
                and run.phase not in TERMINAL_WORKER_PHASES
                for run in job.runs.values()
            )
        elif worker_id is not None:
            run = job.runs.get(worker_id)
            scope_active = bool(
                run is not None and run.phase not in TERMINAL_WORKER_PHASES
            )
        else:
            scope_active = (
                not job.launch_complete
                or any(
                    run.phase not in TERMINAL_WORKER_PHASES
                    for run in job.runs.values()
                )
            )

    if sources == {"live"}:
        source = "live"
        status = "live"
    elif sources == {"archive"}:
        source = "archive"
        status = "live" if scope_active else "archived"
    elif sources:
        source = "mixed"
        status = "live"
    else:
        source = "none"
        status = "pending" if scope_active else "unavailable"

    if returned_entries:
        message = ""
    elif status == "pending" or scope_active:
        phases = set((job.summary().get("phases") or {}) if job else {})
        if "logging_in" in phases:
            message = (
                "账号正在登录，命令尚未启动；若页面出现验证码，请先提交 6 位 OTP。"
            )
        elif "bootstrapping" in phases or "provisioning" in phases:
            message = "Worker 正在初始化环境，命令尚未启动。"
        else:
            message = "命令尚未产生 stdout/stderr。"
    elif status == "archived":
        message = "命令已结束，但没有产生 stdout/stderr。"
    else:
        message = (
            "Worker 已销毁且没有可用的命令输出；旧 Job 在日志归档上线前"
            "无法回取临时实例日志。"
        )

    history_truncated = bool(archived.get("history_truncated"))
    return {
        "job_id": job_id,
        "status": status,
        "source": source,
        "complete": (
            bool(tasks)
            and not scope_active
            and not history_truncated
            and all(task["complete"] for task in tasks)
        ),
        "truncated": (
            bool(archived["truncated"])
            or response_truncated
            or total > len(returned_entries)
        ),
        "message": message,
        "total": total,
        "returned": len(returned_entries),
        "tasks": tasks,
        "entries": returned_entries,
    }


def _collected_dir(mgr, job_id: str) -> Path:
    """Where a job's collected results live on the Manager.

    The batch flow rsyncs each worker's ``collect.paths`` here after the run; the
    endpoints below expose them for download — that's how results reach the user.
    """
    job_id = _validate_job_id(job_id)
    root = Path(mgr.config.registry.path).with_name("collected").resolve()
    unresolved = root / job_id
    try:
        if stat.S_ISLNK(unresolved.lstat().st_mode):
            raise HTTPException(
                400, "job result path escapes collected root or is a symbolic link",
            )
    except FileNotFoundError:
        pass
    candidate = unresolved.resolve()
    try:
        candidate.relative_to(root)
    except ValueError as exc:
        raise HTTPException(400, "job result path escapes collected root") from exc
    return candidate


def _is_real_directory(path: Path) -> bool:
    try:
        return stat.S_ISDIR(path.lstat().st_mode)
    except OSError:
        return False


def _is_safe_result_relative_path(relative: str) -> bool:
    return bool(relative) and not (
        relative.startswith("/")
        or "\\" in relative
        or "\x00" in relative
        or ".." in relative.split("/")
    )


def _is_internal_result_relative_path(relative: str) -> bool:
    """Hide Manager control state while retaining application dotfiles."""

    parts = PurePosixPath(relative).parts
    if not parts:
        return False
    if "_elastic_agent" in parts:
        return True
    if parts[0] in {
        "checkpoint-blobs",
        "checkpoint-sets",
        "_elastic_agent_checkpoints",
        ".elastic-agent-checkpoints",
    }:
        return True
    if (
        len(parts) >= 2
        and parts[0] == "workers"
        and parts[1].startswith(".")
    ):
        return True
    return (
        len(parts) >= 3
        and parts[0] == "workers"
        and parts[2] == "checkpoints"
    )


def _local_regular_files(
    base: Path,
    *,
    max_objects: int,
    max_total_bytes: int | None = None,
    max_metadata_bytes: int = RESULT_LIST_MAX_METADATA_BYTES,
    max_scanned_entries: int = RESULT_LIST_MAX_SCANNED_ENTRIES,
    scan_usage: dict[str, int] | None = None,
) -> list[tuple[Path, str, os.stat_result]]:
    """Enumerate only regular files below ``base`` without following links."""
    if not _is_real_directory(base):
        return []
    files: list[tuple[Path, str, os.stat_result]] = []
    total_bytes = 0
    metadata_bytes = 0
    scanned_entries = 0
    pending_directories = [base]
    while pending_directories:
        directory = pending_directories.pop()
        try:
            entries = os.scandir(directory)
        except OSError:
            continue
        with entries:
            for entry in entries:
                scanned_entries += 1
                if scanned_entries > max_scanned_entries:
                    raise ResultsLimitExceeded(
                        "local results contain more than "
                        f"{max_scanned_entries} filesystem entries"
                    )
                path = Path(entry.path)
                try:
                    rel = path.relative_to(base).as_posix()
                    encoded_path_bytes = len(rel.encode("utf-8"))
                except (UnicodeEncodeError, ValueError) as exc:
                    raise LocalResultsUnavailable(
                        "local results contain a path that is not valid UTF-8"
                    ) from exc
                if _is_internal_result_relative_path(rel):
                    continue
                metadata_bytes += encoded_path_bytes
                if metadata_bytes > max_metadata_bytes:
                    raise ResultsLimitExceeded(
                        "local result path metadata exceeds the "
                        f"{max_metadata_bytes}-byte limit"
                    )
                try:
                    if entry.is_dir(follow_symlinks=False):
                        pending_directories.append(path)
                        continue
                    if not entry.is_file(follow_symlinks=False):
                        continue
                    file_stat = entry.stat(follow_symlinks=False)
                except OSError:
                    continue
                if not stat.S_ISREG(file_stat.st_mode):
                    continue
                if not _is_safe_result_relative_path(rel):
                    raise LocalResultsUnavailable(
                        "local results contain an unsafe relative path: "
                        f"{rel!r}"
                    )
                files.append((path, rel, file_stat))
                total_bytes += file_stat.st_size
                if len(files) > max_objects:
                    raise ResultsLimitExceeded(
                        f"results contain more than {max_objects} regular files"
                    )
                if (
                    max_total_bytes is not None
                    and total_bytes > max_total_bytes
                ):
                    raise ResultsLimitExceeded(
                        "results exceed the "
                        f"{max_total_bytes}-byte archive limit"
                    )
    files.sort(key=lambda item: item[1])
    if scan_usage is not None:
        scan_usage["entries"] = scanned_entries
        scan_usage["metadata_bytes"] = metadata_bytes
    return files


def _same_file_snapshot(
    current: os.stat_result,
    expected: os.stat_result,
) -> bool:
    return (
        current.st_dev == expected.st_dev
        and current.st_ino == expected.st_ino
        and current.st_size == expected.st_size
        and current.st_mtime_ns == expected.st_mtime_ns
    )


def _read_small_regular_file(
    path: Path,
    *,
    max_bytes: int,
    expected_stat: os.stat_result | None = None,
) -> bytes | None:
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(path, flags)
    except OSError:
        return None
    try:
        file_stat = os.fstat(fd)
        if (
            not stat.S_ISREG(file_stat.st_mode)
            or file_stat.st_size > max_bytes
            or (
                expected_stat is not None
                and not _same_file_snapshot(file_stat, expected_stat)
            )
        ):
            return None
        with os.fdopen(fd, "rb", closefd=False) as stream:
            payload = stream.read(file_stat.st_size + 1)
        if len(payload) != file_stat.st_size:
            return None
        final_stat = os.fstat(fd)
        if not _same_file_snapshot(final_stat, file_stat):
            return None
        return payload
    except OSError:
        return None
    finally:
        os.close(fd)


def _bounded_score(raw: object) -> dict | None:
    if not isinstance(raw, dict):
        return None

    values: dict[str, str | None] = {}
    for field in ("task_id", "prompt_level", "status"):
        value = raw.get(field)
        if value is None:
            # Preserve compatibility with historical benchmark summaries that
            # only emitted ``final_score`` while still bounding present text.
            values[field] = None
            continue
        if (
            not isinstance(value, str)
            or len(value) > RESULT_SCORE_TEXT_MAX_CHARS
            or not value.isprintable()
        ):
            return None
        values[field] = value

    score = raw.get("final_score")
    if isinstance(score, bool) or not isinstance(score, (int, float)):
        return None
    if isinstance(score, float) and not math.isfinite(score):
        return None
    try:
        if abs(score) > RESULT_SCORE_ABS_MAX:
            return None
    except (OverflowError, TypeError, ValueError):
        return None
    return {**values, "final_score": score}


def _bounded_result_files(
    rows: Iterable[tuple[str, int]],
    *,
    file_count: int,
) -> list[dict]:
    """Build an explicitly bounded result-file response.

    ``file_count`` comes from the complete authoritative local/S3 listing.
    Callers expose ``files_returned`` and ``files_truncated`` so the fixed-size
    preview can never be mistaken for a complete list. Path and exact compact
    JSON byte budgets additionally protect unusual but valid long filenames.
    """

    files: list[dict] = []
    path_bytes = 0
    # JSON array brackets. Each entry below includes its exact compact JSON
    # representation plus the separating comma used by the response.
    serialized_bytes = 2
    for rel, size in islice(rows, RESULT_FILE_LIST_MAX_ENTRIES):
        try:
            path_bytes += len(rel.encode("utf-8"))
        except UnicodeEncodeError as exc:
            raise ResultsLimitExceeded(
                f"result listing has {file_count} files but contains a path "
                "that is not valid UTF-8"
            ) from exc
        if path_bytes > RESULT_FILE_LIST_MAX_PATH_BYTES:
            raise ResultsLimitExceeded(
                f"result listing has {file_count} files but UTF-8 path "
                f"metadata exceeds {RESULT_FILE_LIST_MAX_PATH_BYTES} bytes"
            )
        item = {"path": rel, "size": size}
        encoded_item = json.dumps(
            item,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        ).encode("utf-8")
        serialized_bytes += len(encoded_item) + bool(files)
        if serialized_bytes > RESULT_FILE_LIST_MAX_JSON_BYTES:
            raise ResultsLimitExceeded(
                f"result listing has {file_count} files but serialized file "
                f"metadata exceeds {RESULT_FILE_LIST_MAX_JSON_BYTES} bytes"
            )
        files.append(item)
    return files


def _results_for(
    mgr,
    job_id: str,
    base: Path,
    *,
    parse_scores: bool = True,
    include_files: bool = True,
    max_objects: int = RESULT_LIST_MAX_OBJECTS,
    max_metadata_bytes: int = RESULT_LIST_MAX_METADATA_BYTES,
    include_scan_usage: bool = False,
) -> dict:
    scan_usage: dict[str, int] | None = {} if include_scan_usage else None
    regular = _local_regular_files(
        base,
        max_objects=max_objects,
        max_metadata_bytes=max_metadata_bytes,
        scan_usage=scan_usage,
    )
    file_count = len(regular)
    files = (
        _bounded_result_files(
            (
                (rel, item_stat.st_size)
                for _, rel, item_stat in regular
            ),
            file_count=file_count,
        )
        if include_files
        else []
    )
    scores: list[dict] = []
    attempted = 0
    total_read_bytes = 0
    for path, rel, listed_stat in regular if parse_scores else ():
        parts = PurePosixPath(rel).parts
        if not rel.endswith(".json") or "instances" in parts:
            continue
        if listed_stat.st_size > RESULT_SCORE_MAX_BYTES:
            continue
        if (
            attempted >= RESULT_SCORE_MAX_ATTEMPTS
            or len(scores) >= RESULT_SCORE_MAX_ENTRIES
            or total_read_bytes + listed_stat.st_size
            > RESULT_SCORE_TOTAL_READ_BYTES
        ):
            break
        attempted += 1
        total_read_bytes += listed_stat.st_size
        payload = _read_small_regular_file(
            path,
            max_bytes=RESULT_SCORE_MAX_BYTES,
            expected_stat=listed_stat,
        )
        if payload is None:
            continue
        try:
            d = json.loads(payload)
        except (RecursionError, UnicodeDecodeError, TypeError, ValueError):
            continue
        score = _bounded_score(d)
        if score is not None:
            scores.append(score)
    s3_uri = mgr._s3_uploader.s3_uri(job_id) if getattr(mgr, "_s3_uploader", None) else None
    result = {
        "job_id": job_id,
        "file_count": file_count,
        "files_returned": len(files),
        "files_truncated": include_files and len(files) < file_count,
        "scores": scores,
        "s3_uri": s3_uri,
        "files": files,
    }
    if include_scan_usage:
        assert scan_usage is not None
        result["_scan_metadata_bytes"] = scan_usage["metadata_bytes"]
    return result


# --- S3-backed results (worker-direct push lands only in S3, not the Manager's
# local collected/ — read straight from S3 so download/UI still work). ---------

def _s3_bucket() -> str:
    return os.environ.get("ELASTIC_AGENT_RESULTS_S3_BUCKET", "").strip()


def _s3_prefix() -> str:
    return os.environ.get("ELASTIC_AGENT_RESULTS_S3_PREFIX", "jobs").strip("/")


def _s3_job_prefix(job_id: str) -> str:
    if _SAFE_JOB_ID.fullmatch(job_id) is None:
        raise ValueError(f"invalid S3 result job id: {job_id!r}")
    prefix = _s3_prefix()
    return f"{prefix}/{job_id}/" if prefix else f"{job_id}/"


def _s3_client():
    import boto3
    return boto3.client("s3")


def _safe_s3_relative_key(relative: str) -> str:
    if not _is_safe_result_relative_path(relative):
        raise S3ResultsUnavailable("S3 results contain an unsafe object key")
    return relative


def _is_internal_s3_result_key(relative: str) -> bool:
    """Hide checkpoint/control objects from public result enumeration."""

    return _is_internal_result_relative_path(relative)


def _s3_list_job(
    job_id: str,
    *,
    max_objects: int | None = None,
    max_total_bytes: int | None = None,
    max_metadata_bytes: int = RESULT_LIST_MAX_METADATA_BYTES,
) -> list[S3ResultObject]:
    """Objects under s3://<bucket>/jobs/<job_id>/ → result metadata tuples.
    Empty when no bucket is configured or nothing is there."""
    bucket = _s3_bucket()
    if not bucket:
        return []
    if max_objects is None:
        max_objects = RESULT_LIST_MAX_OBJECTS
    prefix = _s3_job_prefix(job_id)
    out: list[S3ResultObject] = []
    total_bytes = 0
    metadata_bytes = 0
    scanned_entries = 0
    scanned_pages = 0
    try:
        s3 = _s3_client()
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
            scanned_pages += 1
            if scanned_pages > RESULT_LIST_MAX_S3_PAGES:
                raise ResultsLimitExceeded(
                    "S3 result listing contains more than "
                    f"{RESULT_LIST_MAX_S3_PAGES} pages"
                )
            contents = page.get("Contents", [])
            if not isinstance(contents, list):
                raise S3ResultsUnavailable(
                    "S3 returned an invalid result object listing"
                )
            for obj in contents:
                if not isinstance(obj, dict):
                    raise S3ResultsUnavailable(
                        "S3 returned an invalid result object"
                    )
                key = obj.get("Key")
                if not isinstance(key, str) or not key.startswith(prefix):
                    raise S3ResultsUnavailable(
                        "S3 results contain an object outside the requested prefix"
                    )
                rel = key[len(prefix):]
                if rel:
                    rel = _safe_s3_relative_key(rel)
                    if _is_internal_s3_result_key(rel):
                        continue
                    scanned_entries += 1
                    if scanned_entries > RESULT_LIST_MAX_SCANNED_ENTRIES:
                        raise ResultsLimitExceeded(
                            "S3 results contain more than "
                            f"{RESULT_LIST_MAX_SCANNED_ENTRIES} listed entries"
                        )
                    size = obj.get("Size")
                    if not isinstance(size, int) or isinstance(size, bool) or size < 0:
                        raise S3ResultsUnavailable(
                            f"S3 result object {key!r} has an invalid size"
                        )
                    etag = obj.get("ETag")
                    if not isinstance(etag, str) or not etag:
                        raise S3ResultsUnavailable(
                            f"S3 result object {key!r} has no immutable ETag"
                        )
                    try:
                        metadata_bytes += len(rel.encode("utf-8"))
                        metadata_bytes += len(key.encode("utf-8"))
                        metadata_bytes += len(etag.encode("utf-8"))
                    except UnicodeEncodeError as exc:
                        raise S3ResultsUnavailable(
                            "S3 results contain metadata that is not valid UTF-8"
                        ) from exc
                    if metadata_bytes > max_metadata_bytes:
                        raise ResultsLimitExceeded(
                            "S3 result metadata exceeds the "
                            f"{max_metadata_bytes}-byte limit"
                        )
                    out.append((rel, size, key, etag))
                    total_bytes += size
                    if len(out) > max_objects:
                        raise ResultsLimitExceeded(
                            f"results contain more than {max_objects} objects"
                        )
                    if max_total_bytes is not None and total_bytes > max_total_bytes:
                        raise ResultsLimitExceeded(
                            f"results exceed the {max_total_bytes}-byte archive limit"
                        )
    except (S3ResultsUnavailable, ResultsLimitExceeded):
        raise
    except Exception as exc:  # noqa: BLE001
        raise S3ResultsUnavailable(
            f"cannot list s3://{bucket}/{prefix}: {exc or type(exc).__name__}"
        ) from exc
    return out


def _close_s3_body(body) -> None:
    close = getattr(body, "close", None)
    if close is not None:
        close()


def _get_s3_result_body(s3, bucket: str, obj: S3ResultObject):
    """GET the exact object observed by ListObjectsV2 or fail closed.

    ``IfMatch`` closes the same-size replacement window.  ContentLength closes
    the size-change window before a caller starts consuming the body.  Callers
    still verify EOF because small S3-compatible test/backends may omit either
    metadata field.
    """

    _rel, expected_size, key, listed_etag = obj
    if not isinstance(listed_etag, str) or not listed_etag:
        raise S3ResultsUnavailable(
            f"S3 result object {key!r} has no immutable ETag"
        )
    request = {"Bucket": bucket, "Key": key, "IfMatch": listed_etag}
    response = s3.get_object(**request)
    body = response["Body"]
    try:
        content_length = response.get("ContentLength")
        if (
            content_length is not None
            and (
                not isinstance(content_length, int)
                or isinstance(content_length, bool)
                or content_length != expected_size
            )
        ):
            raise S3ResultsUnavailable(
                f"S3 result object {key!r} changed size while being read"
            )
        response_etag = response.get("ETag")
        if response_etag != listed_etag:
            raise S3ResultsUnavailable(
                f"S3 result object {key!r} changed while being read"
            )
    except Exception:
        _close_s3_body(body)
        raise
    return body


def _read_s3_body_exact(body, expected_size: int, *, max_bytes: int) -> bytes:
    """Read exactly ``expected_size`` bytes plus at most one EOF probe byte."""

    if expected_size > max_bytes:
        raise ResultsLimitExceeded(
            f"S3 result object exceeds the {max_bytes}-byte read limit"
        )
    chunks: list[bytes] = []
    remaining = expected_size
    while remaining:
        requested = min(64 * 1024, remaining)
        chunk = body.read(requested)
        if not isinstance(chunk, (bytes, bytearray)):
            raise S3ResultsUnavailable("S3 result body returned non-bytes data")
        if not chunk:
            raise S3ResultsUnavailable("S3 result object became shorter while being read")
        if len(chunk) > requested:
            raise S3ResultsUnavailable("S3 result body exceeded the bounded read request")
        chunks.append(bytes(chunk))
        remaining -= len(chunk)
    extra = body.read(1)
    if not isinstance(extra, (bytes, bytearray)):
        raise S3ResultsUnavailable("S3 result body returned non-bytes data")
    if extra:
        raise S3ResultsUnavailable("S3 result object became larger while being read")
    return b"".join(chunks)


def _results_from_s3(
    job_id: str,
    objs: list[S3ResultObject],
    *,
    parse_scores: bool,
    include_files: bool = True,
) -> dict:
    bucket = _s3_bucket()
    file_count = len(objs)
    files = (
        _bounded_result_files(
            ((rel, size) for rel, size, _, _ in objs),
            file_count=file_count,
        )
        if include_files
        else []
    )
    scores: list[dict] = []
    if parse_scores:
        s3 = _s3_client()
        attempted = 0
        total_read_bytes = 0
        for obj in objs:
            rel, size, key, _etag = obj
            if not rel.endswith(".json") or "instances" in rel.split("/"):
                continue
            if size > RESULT_SCORE_MAX_BYTES:  # agent stdout dumps etc.
                continue
            if (
                attempted >= RESULT_SCORE_MAX_ATTEMPTS
                or len(scores) >= RESULT_SCORE_MAX_ENTRIES
                or total_read_bytes + size > RESULT_SCORE_TOTAL_READ_BYTES
            ):
                break
            # Count attempts, not successful JSON parses: an attacker must not
            # turn 100k invalid .json objects into 100k bounded-but-costly GETs.
            attempted += 1
            total_read_bytes += size
            try:
                body = _get_s3_result_body(s3, bucket, obj)
                try:
                    payload = _read_s3_body_exact(
                        body, size, max_bytes=RESULT_SCORE_MAX_BYTES,
                    )
                finally:
                    _close_s3_body(body)
            except (S3ResultsUnavailable, ResultsLimitExceeded):
                raise
            except Exception as exc:  # noqa: BLE001
                raise S3ResultsUnavailable(
                    f"cannot read s3://{bucket}/{key}: "
                    f"{exc or type(exc).__name__}"
                ) from exc
            try:
                d = json.loads(payload)
            except (RecursionError, UnicodeDecodeError, TypeError, ValueError):
                continue
            score = _bounded_score(d)
            if score is not None:
                scores.append(score)
    return {
        "job_id": job_id,
        "file_count": file_count,
        "files_returned": len(files),
        "files_truncated": include_files and len(files) < file_count,
        "scores": scores,
        "s3_uri": f"s3://{bucket}/{_s3_job_prefix(job_id)}",
        "files": files,
    }


def _s3_result_summaries(
    *,
    max_jobs: int,
    max_objects: int,
    max_metadata_bytes: int,
) -> tuple[list[dict], set[str], int, int, bool]:
    bucket = _s3_bucket()
    if not bucket:
        return [], set(), 0, 0, False
    root_prefix = f"{_s3_prefix()}/" if _s3_prefix() else ""
    configured_checkpoint_prefix = os.environ.get(
        "ELASTIC_AGENT_CHECKPOINT_S3_PREFIX", "",
    ).strip("/")
    if not configured_checkpoint_prefix:
        configured_checkpoint_prefix = (
            f"{_s3_prefix()}/.elastic-agent-checkpoints"
            if _s3_prefix()
            else ".elastic-agent-checkpoints"
        )
    reserved_common_prefix = None
    if configured_checkpoint_prefix.startswith(root_prefix):
        relative_checkpoint = configured_checkpoint_prefix[
            len(root_prefix):
        ].strip("/")
        if relative_checkpoint:
            reserved_common_prefix = (
                root_prefix
                + relative_checkpoint.split("/", 1)[0]
                + "/"
            )
    jobs: list[dict] = []
    seen: set[str] = set()
    object_count = 0
    metadata_bytes = 0
    truncated = False
    scanned_entries = 0
    scanned_pages = 0
    try:
        s3 = _s3_client()
        for page in s3.get_paginator("list_objects_v2").paginate(
            Bucket=bucket, Prefix=root_prefix, Delimiter="/",
        ):
            scanned_pages += 1
            if scanned_pages > RESULT_SUMMARY_MAX_S3_PAGES:
                return jobs, seen, object_count, metadata_bytes, True
            direct_objects = page.get("Contents", [])
            common_prefixes = page.get("CommonPrefixes", [])
            if (
                not isinstance(direct_objects, list)
                or not isinstance(common_prefixes, list)
            ):
                raise S3ResultsUnavailable(
                    "S3 returned an invalid root result listing"
                )
            for direct_object in direct_objects:
                scanned_entries += 1
                if scanned_entries > RESULT_SUMMARY_MAX_S3_ROOT_ENTRIES:
                    return jobs, seen, object_count, metadata_bytes, True
                if not isinstance(direct_object, dict):
                    raise S3ResultsUnavailable(
                        "S3 returned an invalid root result object"
                    )
                direct_key = direct_object.get("Key")
                if not isinstance(direct_key, str):
                    raise S3ResultsUnavailable(
                        "S3 returned an invalid root result object key"
                    )
                try:
                    metadata_bytes += len(direct_key.encode("utf-8"))
                except UnicodeEncodeError as exc:
                    raise S3ResultsUnavailable(
                        "S3 returned root metadata that is not valid UTF-8"
                    ) from exc
                if metadata_bytes > max_metadata_bytes:
                    return jobs, seen, object_count, metadata_bytes, True
            for common_prefix in common_prefixes:
                if (
                    isinstance(common_prefix, dict)
                    and common_prefix.get("Prefix")
                    == reserved_common_prefix
                ):
                    continue
                scanned_entries += 1
                if scanned_entries > RESULT_SUMMARY_MAX_S3_ROOT_ENTRIES:
                    return jobs, seen, object_count, metadata_bytes, True
                if not isinstance(common_prefix, dict):
                    raise S3ResultsUnavailable(
                        "S3 returned an invalid result prefix"
                    )
                raw_prefix = common_prefix.get("Prefix")
                if not isinstance(raw_prefix, str) or not raw_prefix.startswith(root_prefix):
                    raise S3ResultsUnavailable("S3 returned an invalid result prefix")
                try:
                    prefix_bytes = len(raw_prefix.encode("utf-8"))
                except UnicodeEncodeError as exc:
                    raise S3ResultsUnavailable(
                        "S3 returned a result prefix that is not valid UTF-8"
                    ) from exc
                metadata_bytes += prefix_bytes
                if metadata_bytes > max_metadata_bytes:
                    return jobs, seen, object_count, metadata_bytes, True
                job_id = raw_prefix[len(root_prefix):].strip("/")
                if _SAFE_JOB_ID.fullmatch(job_id) is None:
                    raise S3ResultsUnavailable("S3 returned an unsafe result job id")
                if job_id in seen:
                    continue
                if len(jobs) >= max_jobs or object_count >= max_objects:
                    truncated = True
                    return (
                        jobs,
                        seen,
                        object_count,
                        metadata_bytes,
                        truncated,
                    )
                seen.add(job_id)
                try:
                    objects = _s3_list_job(
                        job_id,
                        max_objects=max_objects - object_count,
                        max_metadata_bytes=(
                            max_metadata_bytes
                            - metadata_bytes
                        ),
                    )
                except ResultsLimitExceeded:
                    truncated = True
                    return (
                        jobs,
                        seen,
                        object_count,
                        metadata_bytes,
                        truncated,
                    )
                object_metadata = sum(
                    len(rel.encode("utf-8"))
                    + len(key.encode("utf-8"))
                    + (len(etag.encode("utf-8")) if etag else 0)
                    for rel, _size, key, etag in objects
                )
                object_count += len(objects)
                metadata_bytes += object_metadata
                result = _results_from_s3(
                    job_id,
                    objects,
                    parse_scores=False,
                    include_files=False,
                )
                jobs.append({
                    key: result[key]
                    for key in ("job_id", "file_count", "scores", "s3_uri")
                })
    except (S3ResultsUnavailable, ResultsLimitExceeded):
        raise
    except Exception as exc:  # noqa: BLE001
        raise S3ResultsUnavailable(
            "cannot list configured S3 results backend: "
            f"{exc or type(exc).__name__}"
        ) from exc
    return jobs, seen, object_count, metadata_bytes, truncated


def _local_result_summaries(
    mgr,
    seen: set[str],
    *,
    max_jobs: int,
    max_objects: int,
    max_metadata_bytes: int,
) -> tuple[list[dict], int, int, bool]:
    root = Path(mgr.collected_root)
    if not _is_real_directory(root):
        return [], 0, 0, False
    jobs: list[dict] = []
    object_count = 0
    metadata_bytes = 0
    scanned_entries = 0
    truncated = False
    try:
        with os.scandir(root) as entries:
            for entry in entries:
                scanned_entries += 1
                if scanned_entries > RESULT_SUMMARY_MAX_DIRECTORY_ENTRIES:
                    truncated = True
                    break
                name = entry.name
                if name in seen or _SAFE_JOB_ID.fullmatch(name) is None:
                    continue
                try:
                    is_directory = entry.is_dir(follow_symlinks=False)
                except OSError:
                    continue
                if not is_directory:
                    continue
                if len(jobs) >= max_jobs or object_count >= max_objects:
                    truncated = True
                    break
                try:
                    name_bytes = len(name.encode("utf-8"))
                except UnicodeEncodeError:
                    continue
                if metadata_bytes + name_bytes > max_metadata_bytes:
                    truncated = True
                    break
                directory = Path(entry.path)
                try:
                    result = _results_for(
                        mgr,
                        name,
                        directory,
                        parse_scores=False,
                        include_files=False,
                        max_objects=max_objects - object_count,
                        max_metadata_bytes=(
                            max_metadata_bytes
                            - metadata_bytes
                            - name_bytes
                        ),
                        include_scan_usage=True,
                    )
                except ResultsLimitExceeded:
                    truncated = True
                    break
                object_count += result["file_count"]
                metadata_bytes += (
                    name_bytes + result["_scan_metadata_bytes"]
                )
                jobs.append({
                    key: result[key]
                    for key in ("job_id", "file_count", "scores", "s3_uri")
                })
    except OSError as exc:
        raise LocalResultsUnavailable(
            "cannot enumerate local result summaries"
        ) from exc
    jobs.sort(key=lambda item: item["job_id"])
    return jobs, object_count, metadata_bytes, truncated


@router.get("/results")
async def list_all_results() -> dict:
    """List every job's results — from S3 (authoritative once uploaded) with a
    local collected/ fallback for non-S3 deployments."""
    permit = _acquire_result_operation(
        _RESULT_READ_ADMISSION,
        operation="result listing",
    )
    mgr = _mgr()
    jobs: list[dict] = []
    seen: set[str] = set()
    object_count = 0
    metadata_bytes = 0
    truncated = False
    try:
        bucket = _s3_bucket()
        if bucket:
            try:
                (
                    s3_jobs,
                    s3_seen,
                    s3_objects,
                    s3_metadata,
                    s3_truncated,
                ) = await _run_result_read(
                    _s3_result_summaries,
                    max_jobs=RESULT_SUMMARY_MAX_JOBS,
                    max_objects=RESULT_SUMMARY_MAX_OBJECTS,
                    max_metadata_bytes=RESULT_SUMMARY_MAX_METADATA_BYTES,
                )
                jobs.extend(s3_jobs)
                seen.update(s3_seen)
                object_count += s3_objects
                metadata_bytes += s3_metadata
                truncated = s3_truncated
            except S3ResultsUnavailable as exc:
                logger.exception(
                    "Configured S3 results backend is unavailable"
                )
                raise HTTPException(503, str(exc)) from exc
        if not truncated:
            try:
                (
                    local_jobs,
                    _local_objects,
                    _local_metadata,
                    local_truncated,
                ) = await _run_result_read(
                    _local_result_summaries,
                    mgr,
                    seen,
                    max_jobs=RESULT_SUMMARY_MAX_JOBS - len(jobs),
                    max_objects=RESULT_SUMMARY_MAX_OBJECTS - object_count,
                    max_metadata_bytes=(
                        RESULT_SUMMARY_MAX_METADATA_BYTES - metadata_bytes
                    ),
                )
                jobs.extend(local_jobs)
                truncated = local_truncated
            except LocalResultsUnavailable as exc:
                raise HTTPException(503, str(exc)) from exc
    finally:
        permit.release()
    jobs.sort(key=lambda item: item["job_id"])
    return {
        "jobs": jobs,
        "total": len(jobs),
        "truncated": truncated,
    }


@router.get("/jobs/{job_id}/results")
async def job_results(job_id: str) -> dict:
    """List a job's result files + benchmark scores (S3 first, local fallback)."""
    job_id = _validate_job_id(job_id)
    permit = _acquire_result_operation(
        _RESULT_READ_ADMISSION,
        operation="result read",
    )
    try:
        try:
            objs = await _run_result_read(_s3_list_job, job_id)
        except ResultsLimitExceeded as exc:
            raise HTTPException(413, str(exc)) from exc
        except S3ResultsUnavailable as exc:
            raise HTTPException(503, str(exc)) from exc
        if objs:
            try:
                return await _run_result_read(
                    _results_from_s3, job_id, objs, parse_scores=True,
                )
            except ResultsLimitExceeded as exc:
                raise HTTPException(413, str(exc)) from exc
            except S3ResultsUnavailable as exc:
                raise HTTPException(503, str(exc)) from exc
        base = _collected_dir(_mgr(), job_id)
        if not _is_real_directory(base):
            raise HTTPException(404, f"no results for job {job_id}")
        try:
            return await _run_result_read(
                _results_for,
                _mgr(),
                job_id,
                base,
            )
        except ResultsLimitExceeded as exc:
            raise HTTPException(413, str(exc)) from exc
        except LocalResultsUnavailable as exc:
            raise HTTPException(503, str(exc)) from exc
    finally:
        permit.release()


def _archive_spool_reservation(
    job_id: str,
    members: Iterable[tuple[str, int]],
) -> int:
    """Return a conservative upper bound for one ``tar.gz`` tempfile.

    Python's default PAX tar format can add substantially more than one KiB of
    metadata for a long/non-ASCII path. Account for every member's padded data,
    ordinary header, a worst-case PAX header and its path/size/mtime records.
    The final one-percent-plus-one-MiB margin dominates zlib's default DEFLATE
    bound and includes the gzip wrapper, so aggregate reservations bound actual
    spool usage rather than merely the source payload.
    """

    tar_bytes = 0
    for relative, size in members:
        if isinstance(size, bool) or not isinstance(size, int) or size < 0:
            raise ResultsLimitExceeded(
                "result archive contains an invalid member size"
            )
        try:
            archive_name_bytes = len(
                f"{job_id}/{relative}".encode("utf-8", "strict")
            )
        except UnicodeEncodeError as exc:
            raise ResultsLimitExceeded(
                "result archive contains a path that is not valid UTF-8"
            ) from exc

        padded_payload = ((size + 511) // 512) * 512
        # PAX records contain the UTF-8 path plus short bounded size/mtime
        # records and decimal length prefixes. 256 bytes is a conservative
        # bound for all non-path record data; round the payload to a tar block
        # and include both its extension header and the member's own header.
        pax_payload = archive_name_bytes + 256
        padded_pax_payload = ((pax_payload + 511) // 512) * 512
        tar_bytes += 512 + padded_payload + 512 + padded_pax_payload

    # TarFile.close writes two zero blocks and pads the complete stream to its
    # 10 KiB record size.
    tar_bytes += 1024
    tar_bytes = ((tar_bytes + tarfile.RECORDSIZE - 1) // tarfile.RECORDSIZE) * (
        tarfile.RECORDSIZE
    )
    return max(
        1024 * 1024,
        tar_bytes + tar_bytes // 100 + 1024 * 1024,
    )


def _cleanup_stale_temp_archives() -> None:
    global _RESULT_ARCHIVE_SPOOL_RESERVED, _RESULT_ARCHIVE_STALE_CLEANED
    with _RESULT_ARCHIVE_SPOOL_LOCK:
        if _RESULT_ARCHIVE_STALE_CLEANED:
            return
        cutoff = time.time() - RESULT_ARCHIVE_STALE_SECONDS
        temp_root = Path(tempfile.gettempdir())
        try:
            candidates = list(
                temp_root.glob("elastic-agent-results-*.tar.gz")
            )
        except OSError:
            candidates = []
        retained_bytes = 0
        for candidate in candidates:
            try:
                item_stat = candidate.lstat()
                if item_stat.st_mtime < cutoff:
                    candidate.unlink(missing_ok=True)
                elif stat.S_ISREG(item_stat.st_mode):
                    # A recent file can belong to an overlapping old Manager
                    # during a rolling replacement. Do not unlink it, but
                    # charge its actual bytes to this process's global budget.
                    retained_bytes += item_stat.st_size
            except OSError:
                logger.warning(
                    "Failed to inspect/remove stale results archive %s",
                    candidate,
                )
        _RESULT_ARCHIVE_SPOOL_RESERVED += retained_bytes
        _RESULT_ARCHIVE_STALE_CLEANED = True


_cleanup_stale_temp_archives()


def _new_temp_archive(*, reserve_bytes: int) -> Path:
    global _RESULT_ARCHIVE_SPOOL_RESERVED
    _cleanup_stale_temp_archives()
    if reserve_bytes <= 0:
        raise ValueError("archive spool reservation must be positive")
    with _RESULT_ARCHIVE_SPOOL_LOCK:
        if (
            reserve_bytes > RESULT_ARCHIVE_SPOOL_MAX_BYTES
            or _RESULT_ARCHIVE_SPOOL_RESERVED + reserve_bytes
            > RESULT_ARCHIVE_SPOOL_MAX_BYTES
        ):
            raise ResultsSpoolUnavailable(
                "temporary results archive disk budget is exhausted"
            )
        # Logical reservations alone do not protect a small/full Manager root
        # filesystem. Charge every tracked archive's not-yet-written portion
        # against current free bytes, and always preserve a fixed emergency
        # margin for journals, logs, package state and OS operation.
        outstanding_reserved = 0
        for path, reserved in _RESULT_ARCHIVE_TEMP_RESERVATIONS.items():
            try:
                actual = min(reserved, path.stat().st_size)
            except OSError:
                actual = 0
            outstanding_reserved += reserved - actual
        try:
            free_bytes = shutil.disk_usage(tempfile.gettempdir()).free
        except OSError as exc:
            raise ResultsSpoolUnavailable(
                "cannot verify temporary results archive disk capacity"
            ) from exc
        required_free = (
            RESULT_ARCHIVE_DISK_SAFETY_BYTES
            + outstanding_reserved
            + reserve_bytes
        )
        if free_bytes < required_free:
            raise ResultsSpoolUnavailable(
                "temporary results archive would consume the Manager disk "
                "safety margin"
            )
        _RESULT_ARCHIVE_SPOOL_RESERVED += reserve_bytes
        try:
            fd, raw_path = tempfile.mkstemp(
                prefix="elastic-agent-results-",
                suffix=".tar.gz",
            )
            try:
                os.fchmod(fd, 0o600)
            finally:
                os.close(fd)
            path = Path(raw_path)
            _RESULT_ARCHIVE_TEMP_RESERVATIONS[path] = reserve_bytes
        except BaseException:
            _RESULT_ARCHIVE_SPOOL_RESERVED -= reserve_bytes
            try:
                Path(raw_path).unlink(missing_ok=True)
            except (NameError, OSError):
                pass
            raise
    return path


def _remove_temp_archive(path: Path) -> None:
    global _RESULT_ARCHIVE_SPOOL_RESERVED
    removed = False
    try:
        path.unlink(missing_ok=True)
        removed = True
    except OSError:
        logger.warning("Failed to remove temporary results archive %s", path)
    if removed:
        with _RESULT_ARCHIVE_SPOOL_LOCK:
            reserved = _RESULT_ARCHIVE_TEMP_RESERVATIONS.pop(path, 0)
            _RESULT_ARCHIVE_SPOOL_RESERVED = max(
                0, _RESULT_ARCHIVE_SPOOL_RESERVED - reserved,
            )


async def _prepare_temp_archive(
    builder,
    *args,
    permit: _ResultOperationPermit | None = None,
) -> Path:
    """Run a bounded archive build and clean an abandoned result.

    ``asyncio.to_thread`` keeps running after its waiter is cancelled. A done
    callback therefore owns cleanup when a client disconnects during the
    prebuild phase, so neither the tempfile nor its disk reservation leaks.
    """

    if permit is None:
        permit = _acquire_result_operation(
            _RESULT_ARCHIVE_BUILD_ADMISSION,
            operation="result archive build",
        )
    try:
        task = asyncio.ensure_future(
            asyncio.get_running_loop().run_in_executor(
                _RESULT_ARCHIVE_BUILD_EXECUTOR,
                functools.partial(builder, *args),
            )
        )
    except BaseException:
        permit.release()
        raise
    state = {"abandoned": False}

    def build_done(completed: asyncio.Task) -> None:
        permit.release()
        if not state["abandoned"]:
            return
        try:
            archive = completed.result()
        except BaseException:
            return
        _remove_temp_archive(archive)

    task.add_done_callback(build_done)
    try:
        return await asyncio.shield(task)
    except BaseException:
        state["abandoned"] = True
        if task.done():
            try:
                archive = task.result()
            except BaseException:
                pass
            else:
                _remove_temp_archive(archive)
        raise


async def _stream_temp_archive(
    path: Path,
    *,
    permit: _ResultOperationPermit | None = None,
) -> AsyncIterator[bytes]:
    """Stream one private tempfile and unlink it on every exit path."""

    try:
        with path.open("rb") as stream:
            while True:
                read_future = asyncio.get_running_loop().run_in_executor(
                    _RESULT_ARCHIVE_FILE_EXECUTOR,
                    stream.read,
                    256 * 1024,
                )
                cancellation: asyncio.CancelledError | None = None
                while True:
                    try:
                        chunk = await asyncio.shield(read_future)
                        break
                    except asyncio.CancelledError as exc:
                        if read_future.cancelled():
                            raise
                        # Executor work cannot be cancelled safely. Do not
                        # close/unlink the file or return admission while a
                        # real read still owns the file object.
                        cancellation = exc
                    except BaseException as exc:
                        if cancellation is not None:
                            raise cancellation from exc
                        raise
                if cancellation is not None:
                    del chunk
                    raise cancellation
                if not chunk:
                    break
                yield chunk
    finally:
        _remove_temp_archive(path)
        if permit is not None:
            permit.release()


class _TemporaryArchiveResponse(StreamingResponse):
    """A prebuilt archive response whose tempfile has response-level ownership."""

    def __init__(
        self,
        path: Path,
        *,
        job_id: str,
        permit: _ResultOperationPermit | None = None,
    ) -> None:
        self._archive_path = path
        self._permit = permit
        super().__init__(
            _stream_temp_archive(path, permit=permit),
            media_type="application/gzip",
            headers={
                "Cache-Control": "no-store",
                "Content-Disposition": (
                    f'attachment; filename="{job_id}-results.tar.gz"'
                ),
                "Content-Length": str(path.stat().st_size),
                "X-Content-Type-Options": "nosniff",
            },
        )

    async def __call__(self, scope, receive, send) -> None:
        try:
            await super().__call__(scope, receive, send)
        finally:
            # StreamingResponse does not promise to enter/close its iterator
            # when response-start or the first body send is cancelled.
            _remove_temp_archive(self._archive_path)
            if self._permit is not None:
                self._permit.release()


def _build_s3_archive(
    job_id: str, objs: list[S3ResultObject],
) -> Path:
    destination = _new_temp_archive(
        reserve_bytes=_archive_spool_reservation(
            job_id,
            ((rel, size) for rel, size, _key, _etag in objs),
        )
    )
    bucket = _s3_bucket()
    try:
        s3 = _s3_client()
        with tarfile.open(destination, mode="w:gz") as archive:
            for obj in objs:
                rel, size, key, _etag = obj
                try:
                    body = _get_s3_result_body(s3, bucket, obj)
                except Exception as exc:  # noqa: BLE001
                    if isinstance(exc, S3ResultsUnavailable):
                        raise
                    raise S3ResultsUnavailable(
                        f"cannot read s3://{bucket}/{key}: "
                        f"{exc or type(exc).__name__}"
                    ) from exc
                info = tarfile.TarInfo(name=f"{job_id}/{rel}")
                info.size = size
                info.mode = 0o600
                try:
                    # StreamingBody is consumed incrementally by tarfile; no
                    # object or complete archive is buffered in memory.
                    archive.addfile(info, body)
                    extra = body.read(1)
                    if not isinstance(extra, (bytes, bytearray)):
                        raise S3ResultsUnavailable(
                            f"S3 result body for {key!r} returned non-bytes data"
                        )
                    if extra:
                        raise S3ResultsUnavailable(
                            f"S3 result object {key!r} became larger while being archived"
                        )
                except Exception as exc:  # noqa: BLE001
                    if isinstance(exc, S3ResultsUnavailable):
                        raise
                    raise S3ResultsUnavailable(
                        f"cannot archive s3://{bucket}/{key}: "
                        f"{exc or type(exc).__name__}"
                    ) from exc
                finally:
                    _close_s3_body(body)
    except Exception:
        _remove_temp_archive(destination)
        raise
    return destination


class _ArchiveStreamCancelledError(Exception):
    """Internal signal used to stop a live archive after client disconnect."""


class _S3ArchiveStreamControl:
    """Coordinate cancellation with the blocking S3/tar producer thread."""

    def __init__(self) -> None:
        self._cancelled = threading.Event()
        self._lock = threading.Lock()
        self._body = None

    @property
    def cancelled(self) -> bool:
        return self._cancelled.is_set()

    def check(self) -> None:
        if self.cancelled:
            raise _ArchiveStreamCancelledError

    def bind_body(self, body) -> None:
        with self._lock:
            if self._cancelled.is_set():
                close_now = True
            else:
                self._body = body
                close_now = False
        if close_now:
            _close_s3_body(body)
            raise _ArchiveStreamCancelledError

    def release_body(self, body) -> bool:
        with self._lock:
            if self._body is body:
                self._body = None
                return True
            return False

    def cancel(self):
        """Signal cancellation and transfer any live body to the caller."""

        self._cancelled.set()
        with self._lock:
            body = self._body
            self._body = None
        return body


def _write_s3_archive_stream(
    job_id: str,
    objs: list[S3ResultObject],
    write_fd: int,
    control: _S3ArchiveStreamControl,
) -> None:
    """Write a gzip tar stream to a bounded OS pipe.

    The pipe applies backpressure when the browser is slow, while mode
    ``w|gz`` lets the response send bytes before every S3 object has been read.
    """

    bucket = _s3_bucket()
    try:
        with os.fdopen(write_fd, "wb", buffering=0) as sink:
            control.check()
            s3 = _s3_client()
            with tarfile.open(
                fileobj=sink, mode="w|gz", compresslevel=1,
            ) as archive:
                for obj in objs:
                    control.check()
                    rel, size, key, _etag = obj
                    try:
                        body = _get_s3_result_body(s3, bucket, obj)
                    except Exception as exc:  # noqa: BLE001
                        if control.cancelled:
                            raise _ArchiveStreamCancelledError from exc
                        if isinstance(exc, S3ResultsUnavailable):
                            raise
                        raise S3ResultsUnavailable(
                            f"cannot read s3://{bucket}/{key}: "
                            f"{exc or type(exc).__name__}"
                        ) from exc
                    control.bind_body(body)
                    try:
                        info = tarfile.TarInfo(name=f"{job_id}/{rel}")
                        info.size = size
                        info.mode = 0o600
                        archive.addfile(info, body)
                        extra = body.read(1)
                        if not isinstance(extra, (bytes, bytearray)):
                            raise S3ResultsUnavailable(
                                f"S3 result body for {key!r} returned non-bytes data"
                            )
                        if extra:
                            raise S3ResultsUnavailable(
                                f"S3 result object {key!r} became larger "
                                "while being archived"
                            )
                    except Exception as exc:  # noqa: BLE001
                        if control.cancelled:
                            raise _ArchiveStreamCancelledError from exc
                        if isinstance(exc, S3ResultsUnavailable):
                            raise
                        raise S3ResultsUnavailable(
                            f"cannot archive s3://{bucket}/{key}: "
                            f"{exc or type(exc).__name__}"
                        ) from exc
                    finally:
                        if control.release_body(body):
                            _close_s3_body(body)
    except _ArchiveStreamCancelledError:
        return
    except (BrokenPipeError, ConnectionError, OSError) as exc:
        if control.cancelled or (
            isinstance(exc, OSError) and exc.errno == errno.EPIPE
        ):
            return
        raise


def _consume_archive_producer(task: asyncio.Future) -> None:
    """Retrieve a detached producer exception so asyncio does not warn."""

    if task.cancelled():
        return
    try:
        task.exception()
    except BaseException:  # task result is diagnostic-only during disconnect
        return


def _consume_archive_cleanup(task: asyncio.Future) -> None:
    """Retrieve and report a detached blocking-body close failure."""

    if task.cancelled():
        return
    try:
        task.result()
    except BaseException:  # noqa: BLE001
        logger.warning(
            "Failed to close an active S3 result body during "
            "archive cancellation",
            exc_info=True,
        )


class _LiveArchivePermitOwner:
    """Tell response cleanup whether its archive iterator ever took ownership."""

    def __init__(self, permit: _ResultOperationPermit) -> None:
        self.permit = permit
        self.started = False


async def _stream_s3_archive(
    job_id: str,
    objs: list[S3ResultObject],
    *,
    permit: _ResultOperationPermit | None = None,
    owner: _LiveArchivePermitOwner | None = None,
) -> AsyncIterator[bytes]:
    """Yield a gzip tar while a blocking producer reads S3 into an OS pipe."""

    if permit is None:
        permit = _acquire_result_operation(
            _RESULT_ARCHIVE_STREAM_ADMISSION,
            operation="live result archive",
        )
    if owner is not None:
        owner.started = True
    try:
        read_fd, write_fd = os.pipe()
    except BaseException:
        permit.release()
        raise
    control = _S3ArchiveStreamControl()
    loop = asyncio.get_running_loop()
    read_pipe = os.fdopen(read_fd, "rb", buffering=0)
    reader = asyncio.StreamReader(limit=256 * 1024)
    protocol = asyncio.StreamReaderProtocol(reader)
    try:
        transport, _ = await loop.connect_read_pipe(lambda: protocol, read_pipe)
    except BaseException:
        read_pipe.close()
        os.close(write_fd)
        permit.release()
        raise
    try:
        producer = loop.run_in_executor(
            _RESULT_ARCHIVE_EXECUTOR,
            _write_s3_archive_stream,
            job_id,
            objs,
            write_fd,
            control,
        )
    except BaseException:
        transport.close()
        os.close(write_fd)
        permit.release()
        raise
    try:
        while True:
            chunk = await reader.read(256 * 1024)
            if not chunk:
                break
            yield chunk
        await producer
    finally:
        close_future = None
        try:
            body = control.cancel()
            if body is not None:
                # ``StreamingBody.close`` is an SDK call and may block. Never
                # invoke it on the Manager event loop; a separate cleanup pool
                # can interrupt the producer's blocking read even when every
                # archive-producer worker is occupied.
                close_future = loop.run_in_executor(
                    _RESULT_ARCHIVE_CLEANUP_EXECUTOR,
                    _close_s3_body,
                    body,
                )
        finally:
            transport.close()
            # A cancelled response may finish before either the blocking
            # producer or a detached SDK-body close unwinds. Keep admission
            # until *both* operations have really exited so replacement work
            # cannot accumulate behind hung threads or retained raw FDs.
            completions: list[
                tuple[asyncio.Future, object]
            ] = [(producer, _consume_archive_producer)]
            if close_future is not None:
                completions.append(
                    (close_future, _consume_archive_cleanup)
                )
            completion_lock = threading.Lock()
            remaining_completions = len(completions)

            def operation_done(
                completed: asyncio.Future,
                consumer,
            ) -> None:
                nonlocal remaining_completions
                consumer(completed)
                with completion_lock:
                    remaining_completions -= 1
                    is_last = remaining_completions == 0
                if is_last:
                    permit.release()

            for completion, consumer in completions:
                completion.add_done_callback(
                    functools.partial(
                        operation_done,
                        consumer=consumer,
                    )
                )


class _LiveArchiveResponse(StreamingResponse):
    """Own a pre-admitted live stream even if response start is cancelled."""

    def __init__(
        self,
        iterator: AsyncIterator[bytes],
        *,
        owner: _LiveArchivePermitOwner,
        headers: dict[str, str],
    ) -> None:
        self._archive_iterator = iterator
        self._owner = owner
        super().__init__(
            iterator,
            media_type="application/gzip",
            headers=headers,
        )

    @staticmethod
    def _consume_close(task: asyncio.Task) -> None:
        if task.cancelled():
            return
        try:
            task.result()
        except BaseException:  # noqa: BLE001
            logger.warning(
                "Failed to finalize cancelled live result archive",
                exc_info=True,
            )

    async def __call__(self, scope, receive, send) -> None:
        try:
            await super().__call__(scope, receive, send)
        finally:
            cleanup = asyncio.create_task(self._archive_iterator.aclose())
            cleanup.add_done_callback(self._consume_close)
            try:
                await asyncio.shield(cleanup)
            finally:
                if not self._owner.started:
                    self._owner.permit.release()


def _build_local_archive(job_id: str, base: Path) -> Path:
    regular = _local_regular_files(
        base,
        max_objects=RESULT_ARCHIVE_MAX_OBJECTS,
        max_total_bytes=RESULT_ARCHIVE_MAX_BYTES,
    )
    destination = _new_temp_archive(
        reserve_bytes=_archive_spool_reservation(
            job_id,
            (
                (rel, item_stat.st_size)
                for _, rel, item_stat in regular
            ),
        )
    )
    total_bytes = 0
    try:
        with tarfile.open(destination, mode="w:gz") as archive:
            for path, rel, listed_stat in regular:
                try:
                    resolved = path.resolve(strict=True)
                    resolved.relative_to(base)
                    if resolved != path:
                        raise OSError("result path traverses a symbolic link")
                    fd = os.open(
                        path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
                    )
                except (OSError, ValueError) as exc:
                    raise LocalResultsUnavailable(
                        f"local result {rel!r} changed during archive creation"
                    ) from exc
                try:
                    file_stat = os.fstat(fd)
                    if (
                        not stat.S_ISREG(file_stat.st_mode)
                        or not _same_file_snapshot(file_stat, listed_stat)
                    ):
                        raise LocalResultsUnavailable(
                            f"local result {rel!r} changed during archive creation"
                        )
                    total_bytes += file_stat.st_size
                    if total_bytes > RESULT_ARCHIVE_MAX_BYTES:
                        raise ResultsLimitExceeded(
                            f"results exceed the {RESULT_ARCHIVE_MAX_BYTES}-byte "
                            "archive limit"
                        )
                    info = tarfile.TarInfo(name=f"{job_id}/{rel}")
                    info.size = file_stat.st_size
                    # Preserve ordinary rwx bits but never export setuid/setgid
                    # or sticky metadata from an untrusted worker filesystem.
                    info.mode = stat.S_IMODE(file_stat.st_mode) & 0o777
                    info.mtime = int(file_stat.st_mtime)
                    with os.fdopen(fd, "rb", closefd=False) as stream:
                        archive.addfile(info, stream)
                        if stream.read(1):
                            raise LocalResultsUnavailable(
                                f"local result {rel!r} grew during archive creation"
                            )
                    if not _same_file_snapshot(os.fstat(fd), file_stat):
                        raise LocalResultsUnavailable(
                            f"local result {rel!r} changed during archive creation"
                        )
                finally:
                    os.close(fd)
    except Exception:
        _remove_temp_archive(destination)
        raise
    return destination


@router.get("/jobs/{job_id}/results/download")
async def job_results_download(job_id: str) -> Response:
    """Download a job's results as a .tar.gz (S3 first, local fallback)."""
    job_id = _validate_job_id(job_id)
    build_permit = _acquire_result_operation(
        _RESULT_ARCHIVE_BUILD_ADMISSION,
        operation="result archive build",
    )
    try:
        stream_permit = _acquire_result_operation(
            _RESULT_ARCHIVE_STREAM_ADMISSION,
            operation="result archive download",
        )
    except BaseException:
        build_permit.release()
        raise
    try:
        read_permit = _acquire_result_operation(
            _RESULT_READ_ADMISSION,
            operation="result listing",
        )
    except BaseException:
        stream_permit.release()
        build_permit.release()
        raise
    archive: Path | None = None
    build_owned: _ResultOperationPermit | None = build_permit
    stream_owned: _ResultOperationPermit | None = stream_permit
    try:
        try:
            objs = await _run_result_read(
                _s3_list_job,
                job_id,
                max_objects=RESULT_ARCHIVE_MAX_OBJECTS,
                max_total_bytes=RESULT_ARCHIVE_MAX_BYTES,
            )
        except ResultsLimitExceeded as exc:
            raise HTTPException(413, str(exc)) from exc
        except S3ResultsUnavailable as exc:
            raise HTTPException(503, str(exc)) from exc
        finally:
            read_permit.release()

        owned_for_builder = build_owned
        build_owned = None
        if objs:
            try:
                archive = await _prepare_temp_archive(
                    _build_s3_archive,
                    job_id,
                    objs,
                    permit=owned_for_builder,
                )
            except ResultsLimitExceeded as exc:
                raise HTTPException(413, str(exc)) from exc
            except S3ResultsUnavailable as exc:
                raise HTTPException(503, str(exc)) from exc
            except ResultsSpoolUnavailable as exc:
                raise HTTPException(507, str(exc)) from exc
        else:
            base = _collected_dir(_mgr(), job_id)
            if not _is_real_directory(base):
                # No builder took ownership in this branch.
                if owned_for_builder is not None:
                    owned_for_builder.release()
                raise HTTPException(404, f"no results for job {job_id}")
            try:
                archive = await _prepare_temp_archive(
                    _build_local_archive,
                    job_id,
                    base,
                    permit=owned_for_builder,
                )
            except ResultsLimitExceeded as exc:
                raise HTTPException(413, str(exc)) from exc
            except LocalResultsUnavailable as exc:
                raise HTTPException(503, str(exc)) from exc
            except ResultsSpoolUnavailable as exc:
                raise HTTPException(507, str(exc)) from exc

        response = _TemporaryArchiveResponse(
            archive,
            job_id=job_id,
            permit=stream_owned,
        )
        archive = None
        stream_owned = None
        return response
    finally:
        if archive is not None:
            _remove_temp_archive(archive)
        if build_owned is not None:
            build_owned.release()
        if stream_owned is not None:
            stream_owned.release()


@router.get("/jobs/{job_id}/results/download/stream")
async def job_results_download_stream(job_id: str) -> Response:
    """Stream an S3 result archive immediately for the interactive web UI.

    The original ``/download`` endpoint deliberately builds the full archive
    before returning so an object error can still become an HTTP 503.  This
    live variant trades that late-error status for bounded, cancellable
    streaming: a mid-stream S3 consistency failure aborts the response rather
    than silently producing a complete-looking archive.
    """

    job_id = _validate_job_id(job_id)
    stream_permit = _acquire_result_operation(
        _RESULT_ARCHIVE_STREAM_ADMISSION,
        operation="live result archive",
    )
    try:
        read_permit = _acquire_result_operation(
            _RESULT_READ_ADMISSION,
            operation="result listing",
        )
    except BaseException:
        stream_permit.release()
        raise
    try:
        try:
            objs = await _run_result_read(
                _s3_list_job,
                job_id,
                max_objects=RESULT_ARCHIVE_MAX_OBJECTS,
                max_total_bytes=RESULT_ARCHIVE_MAX_BYTES,
            )
        except ResultsLimitExceeded as exc:
            raise HTTPException(413, str(exc)) from exc
        except S3ResultsUnavailable as exc:
            raise HTTPException(503, str(exc)) from exc
    except BaseException:
        stream_permit.release()
        raise
    finally:
        read_permit.release()
    if not objs:
        # Manager-local archives are normally small and do not incur thousands
        # of remote object round trips, so retain the strict prebuilt behavior.
        stream_permit.release()
        return await job_results_download(job_id)

    source_bytes = sum(obj[1] for obj in objs)
    headers = {
        "Cache-Control": "no-store",
        "Content-Disposition": (
            f'attachment; filename="{job_id}-results.tar.gz"'
        ),
        "X-Content-Type-Options": "nosniff",
        "X-Accel-Buffering": "no",
        "X-Elastic-Agent-Object-Count": str(len(objs)),
        "X-Elastic-Agent-Source-Bytes": str(source_bytes),
    }
    owner = _LiveArchivePermitOwner(stream_permit)
    iterator = _stream_s3_archive(
        job_id,
        objs,
        permit=stream_permit,
        owner=owner,
    )
    try:
        return _LiveArchiveResponse(
            iterator,
            owner=owner,
            headers=headers,
        )
    except BaseException:
        stream_permit.release()
        raise


class HarnessUploadRequest(BaseModel):
    filename: str = Field(min_length=4, max_length=128)
    content: str = Field(min_length=1, max_length=HARNESS_UPLOAD_MAX_BYTES)
    class_name: str = Field(min_length=1, max_length=128)


class HarnessUploadResponse(BaseModel):
    harness_ref: str
    path: str


@router.post("/jobs/harness", response_model=HarnessUploadResponse, status_code=201)
async def upload_harness(req: HarnessUploadRequest) -> HarnessUploadResponse:
    """Save uploaded Harness code and return a harness_ref usable in a JobSpec.

    The returned ref plugs straight into ``JobSpec.harness_ref`` for the
    upload-code path.
    """
    if os.environ.get(
        "ELASTIC_AGENT_ENABLE_HARNESS_UPLOAD", ""
    ).strip().lower() not in {"1", "true", "yes"}:
        raise HTTPException(
            403,
            "Harness upload is disabled; use declarative JobSpec fields",
        )
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*\.py", req.filename):
        raise HTTPException(400, "filename must be a simple <name>.py")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", req.class_name):
        raise HTTPException(400, "class_name must be a valid identifier")
    content_bytes = req.content.encode("utf-8")
    if len(content_bytes) > HARNESS_UPLOAD_MAX_BYTES:
        raise HTTPException(413, "Harness source exceeds the 1 MiB limit")

    mgr = _mgr()
    base = Path(mgr.config.registry.path).with_name("harness_plugins")
    secure_state_directory(base)
    digest = hashlib.sha256(content_bytes).hexdigest()
    logical_stem = Path(req.filename).stem
    dest = base / f"{logical_stem}-{digest}.py"
    fd, validation_path_raw = tempfile.mkstemp(
        prefix=".harness-validation-",
        suffix=".py",
        dir=base,
    )
    os.close(fd)
    validation_path = Path(validation_path_raw)
    atomic_write_private(validation_path, req.content)

    # Validate a unique private candidate first. Never overwrite or remove a
    # previously published path: persisted JobSpecs must keep referring to
    # byte-identical code for their entire lifetime.
    from elastic_agent.harness.generic import load_harness_class
    try:
        load_harness_class(f"{validation_path}:{req.class_name}")
    except Exception as exc:
        validation_path.unlink(missing_ok=True)
        raise HTTPException(400, f"uploaded code is not a valid Harness: {exc}")
    try:
        validation_stat = validation_path.lstat()
        if not stat.S_ISREG(validation_stat.st_mode):
            raise OSError("validation source is no longer a regular file")
        validated_bytes = validation_path.read_bytes()
    except OSError as exc:
        validation_path.unlink(missing_ok=True)
        raise HTTPException(
            400, "uploaded Harness source disappeared during validation",
        ) from exc
    if validated_bytes != content_bytes:
        validation_path.unlink(missing_ok=True)
        raise HTTPException(
            400, "uploaded Harness modified its source during validation",
        )
    try:
        try:
            os.link(validation_path, dest)
            fsync_directory(base)
        except FileExistsError:
            if dest.read_bytes() != content_bytes:
                raise HTTPException(
                    500, "Harness content-address collision detected",
                )
    finally:
        validation_path.unlink(missing_ok=True)

    ref = f"{dest}:{req.class_name}"
    return HarnessUploadResponse(harness_ref=ref, path=str(dest))
