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
from pathlib import Path

from elastic_agent.core.job_spec import JobSpec
from elastic_agent.core.secure_store import (
    atomic_write_private,
    tighten_private_json_directory,
)

_SAFE_JOB_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")
_JOB_STATES = {"prepared", "launching", "running", "succeeded", "failed", "cancelled"}


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
    payload = json.dumps(
        {
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
        },
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
