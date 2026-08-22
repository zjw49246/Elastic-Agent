"""Durable storage for JobSpecs used by crash recovery.

The in-memory batch job registry disappears with the Manager process, while an
EIP-bound lease and its temporary EC2 can survive that crash.  Persisting the
spec before the first reservation/scale side effect gives startup recovery the
collection paths it needs before destroying the temporary worker.
"""

from __future__ import annotations

import json
import os
import re
import secrets
import stat
import time
from datetime import datetime
from pathlib import Path

from elastic_agent.core.job_spec import JobSpec
from elastic_agent.core.secure_store import (
    atomic_write_private,
    tighten_private_json_directory,
    tighten_state_file,
)

_SAFE_JOB_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")
_SAFE_REQUEST_FINGERPRINT = re.compile(r"[0-9a-f]{64}")
_SAFE_CHECKPOINT_GENERATION = re.compile(
    r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}"
)
_JOB_STATES = {
    "prepared",
    "launching",
    "running",
    "suspending",
    "suspended",
    "succeeded",
    "failed",
    "cancelled",
}
JOB_REQUEST_FINGERPRINT_SCHEMA = 1
JOB_REQUEST_FINGERPRINT_ALGORITHM = "sha256"
JOB_INTERRUPT_INTENT_SCHEMA = 1
_UNBOUND_LAUNCH_INTENTS_VERSION = 1
_MAX_UNBOUND_LAUNCH_JOBS = 10_000
_MAX_UNBOUND_LAUNCHES_PER_JOB = 1_000
_MAX_UNBOUND_LAUNCH_INTENTS_BYTES = 2 * 1024 * 1024
_MAX_JOB_SPEC_JOURNAL_BYTES = 32 * 1024 * 1024


def _valid_interrupt_intent(payload: dict) -> dict | None:
    """Return the trusted private interrupt envelope, if one is present."""

    intent = payload.get("interrupt_intent")
    if (
        not isinstance(intent, dict)
        or intent.get("schema") != JOB_INTERRUPT_INTENT_SCHEMA
        or not isinstance(intent.get("idempotency_digest"), str)
        or _SAFE_REQUEST_FINGERPRINT.fullmatch(
            intent["idempotency_digest"]
        )
        is None
    ):
        return None
    return intent


def _derive_job_lineage(
    registry_path: str | Path,
    job_id: str,
    spec: JobSpec,
) -> dict[str, str | int | None]:
    """Build trusted attempt lineage from the persisted direct source."""

    source_job_id = (
        spec.recovery.source_job_id
        if spec.recovery.policy != "none"
        else ""
    )
    if not source_job_id:
        return {
            "resumed_from_job_id": None,
            "root_job_id": job_id,
            "attempt_no": 1,
        }
    if (
        source_job_id == job_id
        or _SAFE_JOB_ID.fullmatch(source_job_id) is None
    ):
        raise ValueError("invalid recovery source Job id for lineage")

    source_payload = load_job_spec_journal(
        registry_path,
        source_job_id,
    )
    source_lineage: dict = {}
    raw_lineage = source_payload.get("lineage")
    if isinstance(raw_lineage, dict):
        source_lineage = raw_lineage
    root_job_id = str(
        source_lineage.get("root_job_id") or source_job_id
    )
    source_attempt = source_lineage.get("attempt_no", 1)
    if (
        _SAFE_JOB_ID.fullmatch(root_job_id) is None
        or isinstance(source_attempt, bool)
        or not isinstance(source_attempt, int)
        or source_attempt < 1
        or source_attempt >= 1_000_000
    ):
        raise ValueError("invalid persisted recovery Job lineage")
    return {
        "resumed_from_job_id": source_job_id,
        "root_job_id": root_job_id,
        "attempt_no": source_attempt + 1,
    }


def job_specs_dir(registry_path: str | Path) -> Path:
    """Return the Manager-local JobSpec journal directory."""

    directory = Path(os.fspath(registry_path)).expanduser().with_name("specs")
    # Repair specs produced by older versions before recovery code can consume
    # them.  Ignore transient files; they are never valid recovery journals.
    return tighten_private_json_directory(directory, create=True)


def persist_job_spec(
    registry_path: str | Path,
    job_id: str,
    spec: JobSpec,
    request_fingerprint: str | None = None,
) -> Path:
    """Atomically and durably write one recovery JobSpec.

    The temporary file is created beside the destination so ``os.replace`` is
    atomic.  Both the file and directory entry are fsynced; the mode is 0600
    because a JobSpec can contain environment variables or repository secrets.
    """

    if _SAFE_JOB_ID.fullmatch(job_id) is None:
        raise ValueError(f"invalid job id for persistence: {job_id!r}")
    if (
        request_fingerprint is not None
        and _SAFE_REQUEST_FINGERPRINT.fullmatch(request_fingerprint) is None
    ):
        raise ValueError("invalid Job request fingerprint")

    specs_dir = job_specs_dir(registry_path)
    destination = specs_dir / f"{job_id}.json"
    journal = {
        "job_id": job_id,
        "name": spec.name,
        "submitted_at": time.time(),
        # ``prepared`` means the durable write completed but the orchestrator
        # has not crossed its account/cloud launch gate. It may include
        # pre-cloud checkpoint staging, which is safe to rebuild. Idempotent
        # retry may therefore schedule this exact spec instead of mistaking
        # the mere presence of a journal for a completed submission.
        "submission_state": "prepared",
        "state_updated_at": time.time(),
        "spec": spec.model_dump(),
        "lineage": _derive_job_lineage(registry_path, job_id, spec),
    }
    if request_fingerprint is not None:
        # Keep the fingerprint in the same atomic/fsynced write as the first
        # prepared journal.  A later Manager version can therefore recognize
        # an exact historical request before attempting to validate it with
        # the current JobSpec schema.
        journal["request_fingerprint"] = {
            "schema": JOB_REQUEST_FINGERPRINT_SCHEMA,
            "algorithm": JOB_REQUEST_FINGERPRINT_ALGORITHM,
            "digest": request_fingerprint,
        }
    payload = json.dumps(
        journal,
        ensure_ascii=False,
        indent=2,
    )

    return atomic_write_private(destination, payload)


def load_job_spec_journal(
    registry_path: str | Path,
    job_id: str,
    *,
    max_bytes: int = _MAX_JOB_SPEC_JOURNAL_BYTES,
) -> dict:
    """Read one exact private Job journal without following replacements."""

    if _SAFE_JOB_ID.fullmatch(job_id) is None:
        raise ValueError(f"invalid Job id: {job_id!r}")
    if max_bytes <= 0 or max_bytes > _MAX_JOB_SPEC_JOURNAL_BYTES:
        raise ValueError("invalid Job journal read limit")
    path = job_specs_dir(registry_path) / f"{job_id}.json"
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
            or metadata.st_size < 1
            or metadata.st_size > max_bytes
        ):
            raise ValueError("invalid Job journal file")
        chunks: list[bytes] = []
        consumed = 0
        while chunk := os.read(
            descriptor,
            min(64 * 1024, max_bytes + 1 - consumed),
        ):
            chunks.append(chunk)
            consumed += len(chunk)
            if consumed > max_bytes:
                raise ValueError("Job journal exceeds read limit")
    finally:
        os.close(descriptor)
    payload = json.loads(b"".join(chunks).decode("utf-8"))
    if (
        not isinstance(payload, dict)
        or payload.get("job_id") != job_id
        or not isinstance(payload.get("spec"), dict)
    ):
        raise ValueError("invalid Job journal payload")
    return payload


def update_job_state(
    registry_path: str | Path,
    job_id: str,
    state: str,
    *,
    summary: dict | None = None,
) -> Path:
    """Durably advance a persisted Job submission/lifecycle marker."""

    if _SAFE_JOB_ID.fullmatch(job_id) is None:
        raise ValueError(f"invalid job id for state update: {job_id!r}")
    if state not in _JOB_STATES:
        raise ValueError(f"invalid persisted job state: {state!r}")
    destination = job_specs_dir(registry_path) / f"{job_id}.json"
    payload = load_job_spec_journal(registry_path, job_id)
    if state == "suspending":
        if (
            not isinstance(summary, dict)
            or summary.get("state") != "suspending"
            or summary.get("interrupt_requested") is not True
            or summary.get("resume_available") is not False
        ):
            raise ValueError(
                "suspending state requires a non-resumable interrupt intent"
            )
        if _valid_interrupt_intent(payload) is None:
            raise ValueError(
                "suspending state requires a private interrupt intent"
            )
    if state == "suspended":
        if not isinstance(summary, dict):
            raise ValueError("suspended state requires a terminal summary")
        generation = summary.get("resume_generation")
        committed_at = summary.get("resume_committed_at")
        if (
            summary.get("state") != "suspended"
            or summary.get("done") is not True
            or summary.get("cleanup_pending") != 0
            or summary.get("resume_available") is not True
            or not isinstance(generation, str)
            or _SAFE_CHECKPOINT_GENERATION.fullmatch(generation) is None
            or not isinstance(committed_at, str)
            or generation != payload.get("latest_checkpoint_generation")
            or committed_at != payload.get("checkpoint_committed_at")
        ):
            raise ValueError(
                "suspended state requires exact committed checkpoint and "
                "zero pending cleanup"
            )
        if _valid_interrupt_intent(payload) is None:
            raise ValueError(
                "suspended state requires a private interrupt intent"
            )
        try:
            commit_time = datetime.fromisoformat(
                committed_at.replace("Z", "+00:00")
            )
        except ValueError as exc:
            raise ValueError(
                "suspended checkpoint committed_at is invalid"
            ) from exc
        if commit_time.tzinfo is None:
            raise ValueError(
                "suspended checkpoint committed_at must include a timezone"
            )
    payload["submission_state"] = state
    payload["state_updated_at"] = time.time()
    if summary is not None:
        payload["terminal_summary"] = summary
    return atomic_write_private(
        destination,
        json.dumps(payload, ensure_ascii=False, indent=2),
    )


def update_job_interrupt_intent(
    registry_path: str | Path,
    job_id: str,
    idempotency_digest: str,
    *,
    summary: dict,
) -> Path:
    """Atomically bind the private request digest to ``suspending`` state."""

    if _SAFE_JOB_ID.fullmatch(job_id) is None:
        raise ValueError(f"invalid job id for interrupt intent: {job_id!r}")
    if _SAFE_REQUEST_FINGERPRINT.fullmatch(idempotency_digest) is None:
        raise ValueError("invalid interrupt idempotency digest")
    if (
        not isinstance(summary, dict)
        or summary.get("state") != "suspending"
        or summary.get("interrupt_requested") is not True
        or summary.get("resume_available") is not False
    ):
        raise ValueError(
            "interrupt intent requires a non-resumable suspending summary"
        )
    destination = job_specs_dir(registry_path) / f"{job_id}.json"
    payload = load_job_spec_journal(registry_path, job_id)
    if payload.get("submission_state") in {
        "suspended",
        "succeeded",
        "failed",
        "cancelled",
    }:
        raise ValueError("completed Job cannot accept an interrupt intent")
    existing = payload.get("interrupt_intent")
    if existing is not None:
        if (
            _valid_interrupt_intent(payload) is None
            or not secrets.compare_digest(
                str(existing.get("idempotency_digest") or ""),
                idempotency_digest,
            )
        ):
            raise ValueError("interrupt intent identity conflicts with journal")
    payload["submission_state"] = "suspending"
    payload["state_updated_at"] = time.time()
    payload["terminal_summary"] = summary
    if existing is None:
        payload["interrupt_intent"] = {
            "schema": JOB_INTERRUPT_INTENT_SCHEMA,
            "idempotency_digest": idempotency_digest,
            "requested_at": summary.get("interrupt_requested_at"),
        }
    return atomic_write_private(
        destination,
        json.dumps(payload, ensure_ascii=False, indent=2),
    )


def update_job_checkpoint(
    registry_path: str | Path,
    job_id: str,
    generation: str,
    *,
    committed_at: str | None = None,
) -> Path:
    """Persist the latest durable S3 set without changing lifecycle state."""

    if _SAFE_JOB_ID.fullmatch(job_id) is None:
        raise ValueError(f"invalid job id for checkpoint update: {job_id!r}")
    if _SAFE_CHECKPOINT_GENERATION.fullmatch(generation) is None:
        raise ValueError("invalid checkpoint generation")
    committed_at = committed_at or datetime.now().astimezone().isoformat()
    try:
        committed_time = datetime.fromisoformat(
            committed_at.replace("Z", "+00:00")
        )
    except (TypeError, ValueError) as exc:
        raise ValueError("invalid checkpoint committed_at") from exc
    if committed_time.tzinfo is None:
        raise ValueError("checkpoint committed_at must include a timezone")
    destination = job_specs_dir(registry_path) / f"{job_id}.json"
    payload = load_job_spec_journal(registry_path, job_id)
    current_generation = payload.get("latest_checkpoint_generation")
    current_committed_at = payload.get("checkpoint_committed_at")
    if current_generation and current_committed_at:
        try:
            current_time = datetime.fromisoformat(
                str(current_committed_at).replace("Z", "+00:00")
            )
        except ValueError as exc:
            raise ValueError(
                f"invalid checkpoint pointer for {job_id!r}"
            ) from exc
        if current_time.tzinfo is None:
            raise ValueError(
                f"invalid checkpoint pointer for {job_id!r}"
            )
        if (current_time, str(current_generation)) >= (
            committed_time,
            generation,
        ):
            return destination
    payload["latest_checkpoint_generation"] = generation
    payload["checkpoint_committed_at"] = committed_at
    payload["checkpoint_updated_at"] = time.time()
    return atomic_write_private(
        destination,
        json.dumps(payload, ensure_ascii=False, indent=2),
    )


def _unbound_launch_intents_path(registry_path: str | Path) -> Path:
    return Path(os.fspath(registry_path)).expanduser().with_name(
        "unbound-launches.json"
    )


def _new_unbound_launch_intents(controller_id: str) -> dict:
    return {
        "version": _UNBOUND_LAUNCH_INTENTS_VERSION,
        "controller_id": controller_id,
        "jobs": {},
    }


def _read_unbound_launch_intents(
    registry_path: str | Path,
    controller_id: str,
) -> tuple[Path, dict]:
    """Strictly read the dedicated ordinary-RunInstances intent journal."""

    if not controller_id or len(controller_id) > 256:
        raise ValueError("invalid controller id for unbound launch intents")
    path = _unbound_launch_intents_path(registry_path)
    if not path.exists() and not path.is_symlink():
        return path, _new_unbound_launch_intents(controller_id)
    tighten_state_file(path)
    flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    descriptor = os.open(path, flags)
    try:
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != os.getuid()
            or stat.S_IMODE(metadata.st_mode) != 0o600
            or metadata.st_size > _MAX_UNBOUND_LAUNCH_INTENTS_BYTES
        ):
            raise ValueError("unsafe unbound launch intent journal")
        chunks: list[bytes] = []
        bytes_read = 0
        while bytes_read <= _MAX_UNBOUND_LAUNCH_INTENTS_BYTES:
            chunk = os.read(
                descriptor,
                min(
                    64 * 1024,
                    _MAX_UNBOUND_LAUNCH_INTENTS_BYTES + 1 - bytes_read,
                ),
            )
            if not chunk:
                break
            chunks.append(chunk)
            bytes_read += len(chunk)
        payload_bytes = b"".join(chunks)
        if len(payload_bytes) > _MAX_UNBOUND_LAUNCH_INTENTS_BYTES:
            raise ValueError("unbound launch intent journal is too large")
        current = path.stat(follow_symlinks=False)
        if (
            current.st_dev != metadata.st_dev
            or current.st_ino != metadata.st_ino
            or not stat.S_ISREG(current.st_mode)
        ):
            raise ValueError("unbound launch intent journal changed while reading")
    finally:
        os.close(descriptor)
    try:
        payload = json.loads(payload_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("invalid unbound launch intent journal") from exc
    if (
        not isinstance(payload, dict)
        or set(payload) != {"version", "controller_id", "jobs"}
        or isinstance(payload.get("version"), bool)
        or not isinstance(payload.get("version"), int)
        or payload.get("version") != _UNBOUND_LAUNCH_INTENTS_VERSION
        or payload.get("controller_id") != controller_id
        or not isinstance(payload.get("jobs"), dict)
        or len(payload["jobs"]) > _MAX_UNBOUND_LAUNCH_JOBS
    ):
        raise ValueError("invalid unbound launch intent journal")
    for job_id, count in payload["jobs"].items():
        if (
            not isinstance(job_id, str)
            or _SAFE_JOB_ID.fullmatch(job_id) is None
            or isinstance(count, bool)
            or not isinstance(count, int)
            or not 1 <= count <= _MAX_UNBOUND_LAUNCHES_PER_JOB
        ):
            raise ValueError("invalid unbound launch intent entry")
    return path, payload


def load_unbound_launch_intents(
    registry_path: str | Path,
    controller_id: str,
) -> dict[str, int]:
    """Load outstanding ordinary creates for this durable Manager controller."""

    _path, payload = _read_unbound_launch_intents(
        registry_path,
        controller_id,
    )
    return dict(payload["jobs"])


def add_unbound_launch_intent(
    registry_path: str | Path,
    controller_id: str,
    job_id: str,
    *,
    expected_count: int | None = None,
) -> int:
    """Persist intent before an ordinary Job calls the cloud create API."""

    if _SAFE_JOB_ID.fullmatch(job_id) is None:
        raise ValueError(f"invalid job id for unbound launch: {job_id!r}")
    path, payload = _read_unbound_launch_intents(
        registry_path,
        controller_id,
    )
    jobs = payload["jobs"]
    current = int(jobs.get(job_id, 0))
    if expected_count is not None and current != expected_count:
        raise RuntimeError(
            "unbound launch intent journal changed unexpectedly"
        )
    count = current + 1
    if count > _MAX_UNBOUND_LAUNCHES_PER_JOB:
        raise ValueError("too many uncertain unbound launches for one Job")
    if job_id not in jobs and len(jobs) >= _MAX_UNBOUND_LAUNCH_JOBS:
        raise ValueError("too many uncertain unbound launch Jobs")
    jobs[job_id] = count
    atomic_write_private(
        path,
        json.dumps(payload, ensure_ascii=False, indent=2),
    )
    return count


def resolve_unbound_launch_intent(
    registry_path: str | Path,
    controller_id: str,
    job_id: str,
    *,
    all_launches: bool = False,
    expected_count: int | None = None,
) -> int:
    """Durably resolve one confirmed instance, or all after stable miss scans."""

    if _SAFE_JOB_ID.fullmatch(job_id) is None:
        raise ValueError(f"invalid job id for unbound launch: {job_id!r}")
    path, payload = _read_unbound_launch_intents(
        registry_path,
        controller_id,
    )
    jobs = payload["jobs"]
    current = int(jobs.get(job_id, 0))
    if expected_count is not None and current != expected_count:
        raise RuntimeError(
            "unbound launch intent journal changed unexpectedly"
        )
    if current == 0:
        return 0
    remaining = 0 if all_launches else current - 1
    if remaining:
        jobs[job_id] = remaining
    else:
        jobs.pop(job_id, None)
    atomic_write_private(
        path,
        json.dumps(payload, ensure_ascii=False, indent=2),
    )
    return remaining
