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
import time
import uuid
from pathlib import Path

from elastic_agent.core.job_spec import JobSpec

_SAFE_JOB_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")


def job_specs_dir(registry_path: str | Path) -> Path:
    """Return the Manager-local JobSpec journal directory."""

    directory = Path(registry_path).expanduser().with_name("specs")
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def persist_job_spec(
    registry_path: str | Path,
    job_id: str,
    spec: JobSpec,
) -> Path:
    """Atomically and durably write one recovery JobSpec.

    The temporary file is created beside the destination so ``os.replace`` is
    atomic.  Both the file and directory entry are fsynced; the mode is 0600
    because a JobSpec can contain environment variables or repository secrets.
    """

    if _SAFE_JOB_ID.fullmatch(job_id) is None:
        raise ValueError(f"invalid job id for persistence: {job_id!r}")

    specs_dir = job_specs_dir(registry_path)
    destination = specs_dir / f"{job_id}.json"
    temporary = specs_dir / f".{job_id}.{uuid.uuid4().hex}.tmp"
    payload = json.dumps(
        {
            "job_id": job_id,
            "name": spec.name,
            "submitted_at": time.time(),
            "spec": spec.model_dump(),
        },
        ensure_ascii=False,
        indent=2,
    )

    try:
        with temporary.open("x", encoding="utf-8") as stream:
            os.fchmod(stream.fileno(), 0o600)
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, destination)
        flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
        directory_fd = os.open(specs_dir, flags)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        # ``replace`` removes the temporary name.  Earlier failures must not
        # leave debris that a recovery/listing path could treat as a real spec.
        temporary.unlink(missing_ok=True)

    return destination
