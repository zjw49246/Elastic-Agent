"""Batch job REST API — the frontend's Job panel.

Submit a declarative JobSpec (or one referencing uploaded Harness code) and fan
it out across the fleet via the Manager's BatchOrchestrator. Also accepts
Harness code uploads so the "upload code" path has somewhere to land.
"""

from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import logging
import math
import os
import re
import stat
import tarfile
import tempfile
from pathlib import Path, PurePosixPath
from urllib.parse import urlsplit, urlunsplit

from fastapi import APIRouter, Depends, Header, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel
from starlette.background import BackgroundTask

from elastic_agent.api.auth import require_api_key
from elastic_agent.core.batch_orchestrator import JobSpecPersistenceError
from elastic_agent.core.job_spec import JobSpec
from elastic_agent.core.job_spec_store import job_specs_dir
from elastic_agent.core.secure_store import atomic_write_private, secure_state_directory

router = APIRouter(tags=["jobs"], dependencies=[Depends(require_api_key)])
logger = logging.getLogger(__name__)
_submit_lock = asyncio.Lock()
_SAFE_JOB_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")
_PERSISTED_JOB_STATES = {
    "prepared", "launching", "running", "succeeded", "failed", "cancelled",
}
_TERMINAL_JOB_STATES = {"succeeded", "failed", "cancelled"}

# Results endpoints must remain bounded even when an S3 prefix or local results
# directory contains unexpectedly many files.  Listing has a higher ceiling;
# downloads additionally cap the uncompressed payload size.
RESULT_LIST_MAX_OBJECTS = 100_000
RESULT_ARCHIVE_MAX_OBJECTS = 10_000
RESULT_ARCHIVE_MAX_BYTES = 10 * 1024 * 1024 * 1024
RESULT_SCORE_MAX_ATTEMPTS = 500
RESULT_SCORE_MAX_BYTES = 2_000_000

# ``ETag`` is retained from ListObjectsV2 so GET can use ``IfMatch``.  The
# fourth item may be ``None`` only for an S3-compatible backend that omitted an
# ETag; exact byte-count validation still prevents truncation/over-read there.
S3ResultObject = tuple[str, int, str, str | None]


class S3ResultsUnavailable(RuntimeError):  # noqa: N818
    """The configured authoritative results backend could not be read."""


class ResultsLimitExceeded(RuntimeError):  # noqa: N818
    """A result set is too large to safely enumerate or archive."""


class LocalResultsUnavailable(RuntimeError):  # noqa: N818
    """Manager-local result files changed or became unreadable while archiving."""


def _mgr():
    from elastic_agent.api.app import get_manager
    return get_manager()


def _specs_dir(mgr) -> Path:
    """Where submitted JobSpecs are persisted so they survive a Manager restart
    (the orchestrator's job records are in-memory and lost on restart)."""
    return job_specs_dir(mgr.config.registry.path)


def _validate_job_id(job_id: str) -> str:
    """Require the same single-component identifier accepted by the journal."""
    if _SAFE_JOB_ID.fullmatch(job_id) is None:
        raise HTTPException(400, "invalid job_id")
    return job_id


def _job_spec_path(mgr, job_id: str) -> Path:
    return _specs_dir(mgr) / f"{_validate_job_id(job_id)}.json"


def _read_json_file(path: Path) -> dict:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"invalid JSON object in {path.name}")
    return payload


def _read_job_journal(path: Path, expected_job_id: str) -> dict:
    payload = _read_json_file(path)
    if (
        payload.get("job_id") != expected_job_id
        or not isinstance(payload.get("spec"), dict)
    ):
        raise ValueError(f"invalid JobSpec journal for {expected_job_id!r}")
    return payload


def _journal_state(payload: dict) -> str:
    state = payload.get("submission_state")
    return state if state in _PERSISTED_JOB_STATES else "unknown"


def _persisted_job_view(
    job_id: str,
    payload: dict,
    recovered: list,
    *,
    include_spec: bool,
) -> dict:
    """Render a crash-recovered journal without inventing completion.

    A journal in ``launching``/``running`` means the Manager disappeared while
    ownership was live.  It is deliberately shown as interrupted and never as
    done.  Only an explicit durable terminal state can report completion.
    """
    submission_state = _journal_state(payload)
    terminal = submission_state in _TERMINAL_JOB_STATES
    cleanup_pending = sum(lease.state != "released" for lease in recovered)
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

    if submission_state in {"launching", "running"}:
        state = "interrupted"
    elif submission_state == "unknown":
        state = "recovered"
    else:
        state = submission_state

    view = {
        "job_id": job_id,
        "name": payload.get("name", ""),
        "workers": terminal_summary.get("workers", len(recovered)),
        "phases": terminal_summary.get("phases", {}),
        "state": state,
        "submission_state": submission_state,
        "done": terminal and cleanup_pending == 0,
        "cleanup_pending": cleanup_pending,
        "error": "; ".join(errors) or None,
        "in_memory": False,
        "workers_detail": [],
        "recovery_leases": [lease.model_dump() for lease in recovered],
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


def _redacted_spec(spec: JobSpec | dict) -> dict:
    """Return API-safe JobSpec data while retaining useful key names."""
    raw = spec.model_dump(mode="json") if isinstance(spec, JobSpec) else spec
    data = copy.deepcopy(raw)
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
    return data


def _canonical_spec(spec: object) -> object:
    """Normalize legacy persisted specs before idempotency comparison."""

    try:
        return JobSpec.model_validate(spec).model_dump(mode="json")
    except (TypeError, ValueError):
        # An invalid/different journal must remain a mismatch. The caller
        # reports the same 409 as before without exposing validation details.
        return spec


def _job_detail(job) -> dict:
    is_eip_bound = job.spec.account.binding == "eip"
    worker_release_expected = is_eip_bound or job.release_workers_on_complete
    return {
        **job.summary(),
        "spec": _redacted_spec(job.spec),
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
                    r.cleaned_up if is_eip_bound else job.resources_released
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


def _job_list_item(job) -> dict:
    """Job summary + workers_detail but WITHOUT the (heavy) spec — enough for the
    UI's job list to render a full card without a per-job detail request."""
    d = _job_detail(job)
    d.pop("spec", None)
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

    accounts = await mgr.account_store.list()
    if spec.account.mode != "none":
        by_id = {account.id: account for account in accounts}
        for account_id in spec.account.ids:
            account = by_id.get(account_id)
            if account is None:
                raise HTTPException(422, f"selected account {account_id!r} does not exist")
            if not account.enabled:
                raise HTTPException(422, f"selected account {account_id!r} is disabled")
            if account.agent_type != spec.account.agent_type:
                raise HTTPException(
                    422,
                    f"selected account {account_id!r} is {account.agent_type}, "
                    f"not {spec.account.agent_type}",
                )
        if not spec.account.ids:
            eligible = [
                account for account in accounts
                if account.enabled
                and account.group == spec.account.group
                and account.agent_type == spec.account.agent_type
            ]
            required = spec.fanout.workers * spec.account.per_worker
            if len(eligible) < required:
                raise HTTPException(
                    422,
                    f"account group {spec.account.group!r} has "
                    f"{len(eligible)} eligible {spec.account.agent_type} account(s); "
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
            "account.binding='none' uses a temporary public IP and bypasses "
            "the account's durable EIP; use binding='eip' for stable login identity"
        )

    results_bucket = _s3_bucket()
    if results_bucket and provider.type == "aws" and worker_profile:
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
    command_preview = spec.render_command(ctx)
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
            "interval_seconds": spec.collect.interval_seconds,
            "mode": collection_mode,
            "s3_bucket": results_bucket or None,
            "automatic_final_collect": bool(spec.collect.paths),
        },
        "warnings": warnings,
    }


@router.post("/jobs/plan")
async def plan_job(spec: JobSpec) -> dict:
    """Validate and preview a Job without persistence/cloud/account mutation."""
    return await _preflight_job(_mgr(), spec)


@router.post("/jobs", status_code=201)
async def submit_job(
    spec: JobSpec,
    idempotency_key: str | None = Header(
        default=None, alias="Idempotency-Key"
    ),
) -> dict:
    """Validate + launch a batch job. Returns the job summary."""
    mgr = _mgr()
    await _preflight_job(mgr, spec)
    if idempotency_key is not None:
        idempotency_key = idempotency_key.strip()
        if not idempotency_key or len(idempotency_key) > 200 or any(
            ord(char) < 0x20 for char in idempotency_key
        ):
            raise HTTPException(400, "invalid Idempotency-Key")

    async with _submit_lock:
        deterministic_id = None
        recovered_prepared = False
        if idempotency_key:
            deterministic_id = "job-" + hashlib.sha256(
                idempotency_key.encode("utf-8")
            ).hexdigest()[:32]
            live = mgr.batch.get_job(deterministic_id)
            if live is not None:
                if live.spec.model_dump(mode="json") != spec.model_dump(mode="json"):
                    raise HTTPException(
                        409, "Idempotency-Key was already used for another JobSpec"
                    )
                detail = _job_detail(live)
                detail["idempotent_replay"] = True
                return detail

            persisted = _job_spec_path(mgr, deterministic_id)
            if persisted.is_file():
                try:
                    payload = await asyncio.to_thread(
                        _read_job_journal, persisted, deterministic_id,
                    )
                except (OSError, ValueError, json.JSONDecodeError) as exc:
                    raise HTTPException(
                        500, f"cannot read persisted Job {deterministic_id}"
                    ) from exc
                if _canonical_spec(payload.get("spec")) != spec.model_dump(mode="json"):
                    raise HTTPException(
                        409, "Idempotency-Key was already used for another JobSpec"
                    )
                submission_state = _journal_state(payload)
                if submission_state == "prepared":
                    # The durable write won, but scheduling did not.  Reusing
                    # the same deterministic id is the safe continuation; a
                    # fresh id would violate the idempotency contract.
                    recovered_prepared = True
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

        try:
            # Prepare first so an idempotency key can own a deterministic Job
            # id before the durable spec and any cloud side effect are created.
            if deterministic_id:
                job = mgr.batch.prepare(spec)
                job.job_id = deterministic_id
                await mgr.batch.submit_prepared(job)
            else:
                job = await mgr.batch.submit(spec)
        except JobSpecPersistenceError as exc:
            raise HTTPException(500, str(exc)) from exc
        except NotImplementedError as exc:
            raise HTTPException(503, str(exc)) from exc
        detail = _job_detail(job)
        if recovered_prepared:
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
    await _preflight_job(mgr, spec)
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
    # Persisted specs whose jobs are no longer in memory (restarted) — surface
    # them so they can be reviewed / resubmitted.
    leases = await mgr.account_binding_store.list_leases()
    leases_by_job: dict[str, list] = {}
    for lease in leases:
        leases_by_job.setdefault(lease.job_id, []).append(lease)
    spec_files = await asyncio.to_thread(
        lambda: sorted(_specs_dir(mgr).glob("*.json"))
    )
    for f in spec_files:
        if _SAFE_JOB_ID.fullmatch(f.stem) is None:
            continue
        if f.stem in live_ids:
            continue
        try:
            data = await asyncio.to_thread(_read_job_journal, f, f.stem)
        except Exception:
            continue
        recovered = leases_by_job.get(f.stem, [])
        out.append(_persisted_job_view(
            f.stem, data, recovered, include_spec=False,
        ))
    return {"jobs": out, "total": len(out)}


@router.get("/jobs/{job_id}")
async def get_job(job_id: str) -> dict:
    job_id = _validate_job_id(job_id)
    mgr = _mgr()
    job = mgr.batch.get_job(job_id)
    if job is not None:
        return _job_detail(job)
    # Fall back to the persisted spec (job gone from memory after a restart).
    p = _job_spec_path(mgr, job_id)
    if p.exists():
        try:
            data = await asyncio.to_thread(_read_job_journal, p, job_id)
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            raise HTTPException(500, f"invalid persisted spec for job {job_id}") from exc
        leases = await mgr.account_binding_store.list_leases()
        recovered = [lease for lease in leases if lease.job_id == job_id]
        return _persisted_job_view(
            job_id, data, recovered, include_spec=True,
        )
    raise HTTPException(404, f"Job {job_id} not found")


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


def _local_regular_files(
    base: Path,
    *,
    max_objects: int,
    max_total_bytes: int | None = None,
) -> list[tuple[Path, str, os.stat_result]]:
    """Enumerate only regular files below ``base`` without following links."""
    if not _is_real_directory(base):
        return []
    files: list[tuple[Path, str, os.stat_result]] = []
    total_bytes = 0
    for dirpath, dirnames, filenames in os.walk(base, topdown=True, followlinks=False):
        directory = Path(dirpath)
        real_dirs: list[str] = []
        for name in sorted(dirnames):
            child = directory / name
            try:
                child_stat = child.lstat()
            except OSError:
                continue
            if stat.S_ISDIR(child_stat.st_mode):
                real_dirs.append(name)
        dirnames[:] = real_dirs
        for name in sorted(filenames):
            path = directory / name
            try:
                file_stat = path.lstat()
            except OSError:
                continue
            if not stat.S_ISREG(file_stat.st_mode):
                continue
            rel = path.relative_to(base).as_posix()
            if not _is_safe_result_relative_path(rel):
                raise LocalResultsUnavailable(
                    f"local results contain an unsafe relative path: {rel!r}"
                )
            files.append((path, rel, file_stat))
            total_bytes += file_stat.st_size
            if len(files) > max_objects:
                raise ResultsLimitExceeded(
                    f"results contain more than {max_objects} regular files"
                )
            if max_total_bytes is not None and total_bytes > max_total_bytes:
                raise ResultsLimitExceeded(
                    f"results exceed the {max_total_bytes}-byte archive limit"
                )
    return files


def _read_small_regular_file(path: Path, *, max_bytes: int) -> bytes | None:
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(path, flags)
    except OSError:
        return None
    try:
        file_stat = os.fstat(fd)
        if not stat.S_ISREG(file_stat.st_mode) or file_stat.st_size > max_bytes:
            return None
        with os.fdopen(fd, "rb", closefd=False) as stream:
            return stream.read(max_bytes + 1)
    except OSError:
        return None
    finally:
        os.close(fd)


def _results_for(mgr, job_id: str, base: Path) -> dict:
    regular = _local_regular_files(
        base, max_objects=RESULT_LIST_MAX_OBJECTS,
    )
    files = [{"path": rel, "size": item_stat.st_size}
             for _, rel, item_stat in regular]
    scores = []
    for path, rel, _ in regular:
        parts = PurePosixPath(rel).parts
        if not rel.endswith(".json") or "instances" in parts:
            continue
        payload = _read_small_regular_file(path, max_bytes=2_000_000)
        if payload is None:
            continue
        try:
            d = json.loads(payload)
        except (json.JSONDecodeError, UnicodeDecodeError, TypeError):
            continue
        if isinstance(d, dict) and "final_score" in d:
            scores.append({
                "task_id": d.get("task_id"), "prompt_level": d.get("prompt_level"),
                "status": d.get("status"), "final_score": d.get("final_score"),
            })
    s3_uri = mgr._s3_uploader.s3_uri(job_id) if getattr(mgr, "_s3_uploader", None) else None
    return {"job_id": job_id, "file_count": len(files), "scores": scores, "s3_uri": s3_uri, "files": files}


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


def _s3_list_job(
    job_id: str,
    *,
    max_objects: int | None = None,
    max_total_bytes: int | None = None,
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
    try:
        s3 = _s3_client()
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if not isinstance(key, str) or not key.startswith(prefix):
                    raise S3ResultsUnavailable(
                        "S3 results contain an object outside the requested prefix"
                    )
                rel = key[len(prefix):]
                if rel:
                    rel = _safe_s3_relative_key(rel)
                    size = obj["Size"]
                    if not isinstance(size, int) or isinstance(size, bool) or size < 0:
                        raise S3ResultsUnavailable(
                            f"S3 result object {key!r} has an invalid size"
                        )
                    etag = obj.get("ETag")
                    if etag is not None and not isinstance(etag, str):
                        raise S3ResultsUnavailable(
                            f"S3 result object {key!r} has an invalid ETag"
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
    request = {"Bucket": bucket, "Key": key}
    if listed_etag:
        request["IfMatch"] = listed_etag
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
        if listed_etag and response_etag != listed_etag:
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


def _results_from_s3(job_id: str, objs: list[S3ResultObject], *, parse_scores: bool) -> dict:
    bucket = _s3_bucket()
    files = [{"path": rel, "size": size} for rel, size, _, _ in objs]
    scores: list[dict] = []
    if parse_scores:
        s3 = _s3_client()
        attempted = 0
        for obj in objs:
            rel, size, key, _etag = obj
            if not rel.endswith(".json") or "instances" in rel.split("/"):
                continue
            if size > RESULT_SCORE_MAX_BYTES:  # agent stdout dumps etc.
                continue
            if attempted >= RESULT_SCORE_MAX_ATTEMPTS:
                break
            # Count attempts, not successful JSON parses: an attacker must not
            # turn 100k invalid .json objects into 100k bounded-but-costly GETs.
            attempted += 1
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
            except (json.JSONDecodeError, UnicodeDecodeError, TypeError):
                continue
            if isinstance(d, dict) and "final_score" in d:
                scores.append({
                    "task_id": d.get("task_id"), "prompt_level": d.get("prompt_level"),
                    "status": d.get("status"), "final_score": d.get("final_score"),
                })
    return {"job_id": job_id, "file_count": len(files), "scores": scores,
            "s3_uri": f"s3://{bucket}/{_s3_job_prefix(job_id)}", "files": files}


def _s3_result_summaries() -> list[dict]:
    bucket = _s3_bucket()
    if not bucket:
        return []
    root_prefix = f"{_s3_prefix()}/" if _s3_prefix() else ""
    jobs: list[dict] = []
    seen: set[str] = set()
    try:
        s3 = _s3_client()
        for page in s3.get_paginator("list_objects_v2").paginate(
            Bucket=bucket, Prefix=root_prefix, Delimiter="/",
        ):
            for common_prefix in page.get("CommonPrefixes", []):
                raw_prefix = common_prefix.get("Prefix")
                if not isinstance(raw_prefix, str) or not raw_prefix.startswith(root_prefix):
                    raise S3ResultsUnavailable("S3 returned an invalid result prefix")
                job_id = raw_prefix[len(root_prefix):].strip("/")
                if _SAFE_JOB_ID.fullmatch(job_id) is None:
                    raise S3ResultsUnavailable("S3 returned an unsafe result job id")
                if job_id in seen:
                    continue
                seen.add(job_id)
                result = _results_from_s3(
                    job_id, _s3_list_job(job_id), parse_scores=False,
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
    return jobs


def _local_result_summaries(mgr, seen: set[str]) -> list[dict]:
    root = Path(mgr.collected_root)
    if not _is_real_directory(root):
        return []
    jobs: list[dict] = []
    for directory in sorted(root.iterdir()):
        if (
            directory.name in seen
            or _SAFE_JOB_ID.fullmatch(directory.name) is None
            or not _is_real_directory(directory)
        ):
            continue
        result = _results_for(mgr, directory.name, directory)
        jobs.append({
            key: result[key]
            for key in ("job_id", "file_count", "scores", "s3_uri")
        })
    return jobs


@router.get("/results")
async def list_all_results() -> dict:
    """List every job's results — from S3 (authoritative once uploaded) with a
    local collected/ fallback for non-S3 deployments."""
    mgr = _mgr()
    jobs, seen = [], set()
    bucket = _s3_bucket()
    if bucket:  # list job prefixes cheaply (no per-file score parsing here)
        try:
            jobs.extend(await asyncio.to_thread(_s3_result_summaries))
            seen.update(item["job_id"] for item in jobs)
        except ResultsLimitExceeded as exc:
            raise HTTPException(413, str(exc)) from exc
        except S3ResultsUnavailable as exc:
            logger.exception("Configured S3 results backend is unavailable")
            raise HTTPException(503, str(exc)) from exc
    try:
        jobs.extend(await asyncio.to_thread(_local_result_summaries, mgr, seen))
    except ResultsLimitExceeded as exc:
        raise HTTPException(413, str(exc)) from exc
    except LocalResultsUnavailable as exc:
        raise HTTPException(503, str(exc)) from exc
    return {"jobs": jobs, "total": len(jobs)}


@router.get("/jobs/{job_id}/results")
async def job_results(job_id: str) -> dict:
    """List a job's result files + benchmark scores (S3 first, local fallback)."""
    job_id = _validate_job_id(job_id)
    try:
        objs = await asyncio.to_thread(_s3_list_job, job_id)
    except ResultsLimitExceeded as exc:
        raise HTTPException(413, str(exc)) from exc
    except S3ResultsUnavailable as exc:
        raise HTTPException(503, str(exc)) from exc
    if objs:
        try:
            r = await asyncio.to_thread(
                _results_from_s3, job_id, objs, parse_scores=True,
            )
        except S3ResultsUnavailable as exc:
            raise HTTPException(503, str(exc)) from exc
        r["files"] = r["files"][:500]
        return r
    base = _collected_dir(_mgr(), job_id)
    if not _is_real_directory(base):
        raise HTTPException(404, f"no results for job {job_id}")
    try:
        r = await asyncio.to_thread(_results_for, _mgr(), job_id, base)
    except ResultsLimitExceeded as exc:
        raise HTTPException(413, str(exc)) from exc
    except LocalResultsUnavailable as exc:
        raise HTTPException(503, str(exc)) from exc
    r["files"] = r["files"][:500]
    return r


def _new_temp_archive() -> Path:
    fd, raw_path = tempfile.mkstemp(prefix="elastic-agent-results-", suffix=".tar.gz")
    try:
        os.fchmod(fd, 0o600)
    finally:
        os.close(fd)
    return Path(raw_path)


def _remove_temp_archive(path: Path) -> None:
    try:
        path.unlink(missing_ok=True)
    except OSError:
        logger.warning("Failed to remove temporary results archive %s", path)


def _build_s3_archive(
    job_id: str, objs: list[S3ResultObject],
) -> Path:
    destination = _new_temp_archive()
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


def _build_local_archive(job_id: str, base: Path) -> Path:
    regular = _local_regular_files(
        base,
        max_objects=RESULT_ARCHIVE_MAX_OBJECTS,
        max_total_bytes=RESULT_ARCHIVE_MAX_BYTES,
    )
    destination = _new_temp_archive()
    total_bytes = 0
    try:
        with tarfile.open(destination, mode="w:gz") as archive:
            for path, rel, _ in regular:
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
                    if not stat.S_ISREG(file_stat.st_mode):
                        raise LocalResultsUnavailable(
                            f"local result {rel!r} is not a regular file"
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
                finally:
                    os.close(fd)
    except Exception:
        _remove_temp_archive(destination)
        raise
    return destination


@router.get("/jobs/{job_id}/results/download")
async def job_results_download(job_id: str) -> FileResponse:
    """Download a job's results as a .tar.gz (S3 first, local fallback)."""
    job_id = _validate_job_id(job_id)
    try:
        objs = await asyncio.to_thread(
            _s3_list_job,
            job_id,
            max_objects=RESULT_ARCHIVE_MAX_OBJECTS,
            max_total_bytes=RESULT_ARCHIVE_MAX_BYTES,
        )
    except ResultsLimitExceeded as exc:
        raise HTTPException(413, str(exc)) from exc
    except S3ResultsUnavailable as exc:
        raise HTTPException(503, str(exc)) from exc
    if objs:
        try:
            archive = await asyncio.to_thread(_build_s3_archive, job_id, objs)
        except S3ResultsUnavailable as exc:
            raise HTTPException(503, str(exc)) from exc
    else:
        base = _collected_dir(_mgr(), job_id)
        if not _is_real_directory(base):
            raise HTTPException(404, f"no results for job {job_id}")
        try:
            archive = await asyncio.to_thread(_build_local_archive, job_id, base)
        except ResultsLimitExceeded as exc:
            raise HTTPException(413, str(exc)) from exc
        except LocalResultsUnavailable as exc:
            raise HTTPException(503, str(exc)) from exc
    return FileResponse(
        archive,
        media_type="application/gzip",
        filename=f"{job_id}-results.tar.gz",
        background=BackgroundTask(_remove_temp_archive, archive),
    )


class HarnessUploadRequest(BaseModel):
    filename: str
    content: str
    class_name: str


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

    mgr = _mgr()
    base = Path(mgr.config.registry.path).with_name("harness_plugins")
    secure_state_directory(base)
    dest = base / req.filename
    atomic_write_private(dest, req.content)

    ref = f"{dest}:{req.class_name}"
    # Fail fast if the uploaded code doesn't actually resolve to a Harness.
    from elastic_agent.harness.generic import load_harness_class
    try:
        load_harness_class(ref)
    except Exception as exc:
        dest.unlink(missing_ok=True)
        raise HTTPException(400, f"uploaded code is not a valid Harness: {exc}")

    return HarnessUploadResponse(harness_ref=ref, path=str(dest))
