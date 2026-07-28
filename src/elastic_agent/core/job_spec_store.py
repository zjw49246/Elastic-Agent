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
import stat
import time
from pathlib import Path

from elastic_agent.core.job_spec import JobSpec
from elastic_agent.core.secure_store import (
    atomic_write_private,
    tighten_private_json_directory,
    tighten_state_file,
)

_SAFE_JOB_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")
_SAFE_REQUEST_FINGERPRINT = re.compile(r"[0-9a-f]{64}")
_JOB_STATES = {"prepared", "launching", "running", "succeeded", "failed", "cancelled"}
JOB_REQUEST_FINGERPRINT_SCHEMA = 1
JOB_REQUEST_FINGERPRINT_ALGORITHM = "sha256"
_UNBOUND_LAUNCH_INTENTS_VERSION = 1
_MAX_UNBOUND_LAUNCH_JOBS = 10_000
_MAX_UNBOUND_LAUNCHES_PER_JOB = 1_000
_MAX_UNBOUND_LAUNCH_INTENTS_BYTES = 2 * 1024 * 1024


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
        # ``prepared`` means the durable write completed but the
        # orchestrator has not crossed its launch gate.  Idempotent retry
        # may safely schedule this exact spec instead of mistaking the mere
        # presence of a journal for a completed submission.
        "submission_state": "prepared",
        "state_updated_at": time.time(),
        "spec": spec.model_dump(),
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
    if not destination.is_file():
        raise FileNotFoundError(f"JobSpec journal not found: {job_id}")
    payload = json.loads(destination.read_text(encoding="utf-8"))
    if payload.get("job_id") != job_id or not isinstance(payload.get("spec"), dict):
        raise ValueError(f"invalid JobSpec journal for {job_id!r}")
    payload["submission_state"] = state
    payload["state_updated_at"] = time.time()
    if summary is not None:
        payload["terminal_summary"] = summary
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
