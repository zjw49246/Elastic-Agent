"""Durable, bounded orchestration for JSON batches of declarative Jobs.

The batch layer is intentionally a queue, not a cloud transaction.  It owns
only a private manifest journal and stable per-item idempotency keys; each Job
still crosses the canonical Job submission boundary independently.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import math
import os
import re
from collections import Counter
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from elastic_agent.core.job_spec import JobSpec
from elastic_agent.core.job_spec_store import load_job_spec_journal
from elastic_agent.core.secure_store import (
    atomic_write_private,
    secure_state_directory,
    tighten_private_json_directory,
    tighten_state_file,
)

logger = logging.getLogger(__name__)

JOB_BATCH_SCHEMA_VERSION = 1
JOB_BATCH_JOURNAL_SCHEMA_VERSION = 1
JOB_BATCH_MAX_BODY_BYTES = 2 * 1024 * 1024
JOB_BATCH_ABSOLUTE_MAX_ITEMS = 100
# A manifest remains bounded by its 100-item schema limit.  Production may
# submit several manifests concurrently, so both per-manifest and Manager-wide
# scheduling ceilings use the same published upper bound.
JOB_BATCH_POLICY_MAX_ACTIVE_JOBS = 500
JOB_BATCH_GLOBAL_MAX_ACTIVE_JOBS = 500
JOB_BATCH_JOURNAL_MAX_BYTES = 4 * 1024 * 1024
JOB_BATCH_LIST_DEFAULT_LIMIT = 200
JOB_BATCH_LIST_MAX_LIMIT = 500

_SAFE_PUBLIC_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")
_SAFE_JOB_BATCH_ID = re.compile(r"batch-[0-9a-f]{32}")
_SAFE_JOB_ID = re.compile(r"job-[0-9a-f]{32}")
_FINGERPRINT = re.compile(r"[0-9a-f]{64}")
_TERMINAL_JOB_STATES = frozenset(
    {"suspended", "succeeded", "failed", "cancelled"}
)
_ITEM_STATES = frozenset({"queued", "accepted", "terminal", "error"})
_BATCH_STATES = frozenset({"queued", "running", "terminal"})
_PUBLIC_SUMMARY_FIELDS = frozenset(
    {
        "job_count",
        "total_workers",
        "total_worker_hours",
        "max_active_jobs",
        "instance_types",
        "account_requirements",
        "max_concurrent_workers",
        "provider_max_instances",
    }
)
_REDACTED_SECRET_PLACEHOLDERS = frozenset(
    {
        "[secret_reference]",
        "[redacted]",
        "<secret_reference>",
        "<redacted>",
        "***",
        "redacted",
    }
)

Submitter = Callable[[dict[str, object], str], Awaitable[dict[str, Any]]]


class StrictBatchModel(BaseModel):
    """Base model shared by the public manifest envelope."""

    model_config = ConfigDict(extra="forbid")


class JobBatchPolicy(StrictBatchModel):
    max_active_jobs: int = Field(
        default=3,
        ge=1,
        le=JOB_BATCH_POLICY_MAX_ACTIVE_JOBS,
    )
    on_job_failure: Literal["continue"] = "continue"


class JobBatchItemSpec(StrictBatchModel):
    client_id: str = Field(min_length=1, max_length=128)
    spec: JobSpec

    @model_validator(mode="before")
    @classmethod
    def reject_redacted_secret_references(cls, value: Any) -> Any:
        """Do not mistake a copied/redacted response for an executable spec."""

        if not isinstance(value, dict):
            return value
        spec = value.get("spec")
        run = spec.get("run") if isinstance(spec, dict) else None
        secret_env = run.get("secret_env") if isinstance(run, dict) else None
        if isinstance(secret_env, dict):
            for reference in secret_env.values():
                if (
                    isinstance(reference, str)
                    and reference.strip().lower()
                    in _REDACTED_SECRET_PLACEHOLDERS
                ):
                    raise ValueError(
                        "run.secret_env contains a redacted placeholder; "
                        "supply the original AWS secret reference"
                    )
        return value

    @model_validator(mode="after")
    def safe_client_id(self) -> JobBatchItemSpec:
        if _SAFE_PUBLIC_ID.fullmatch(self.client_id) is None:
            raise ValueError(
                "client_id must contain only letters, digits, '.', '_' or '-'"
            )
        return self


class JobBatchManifest(StrictBatchModel):
    schema_version: Literal[1]
    batch_id: str = Field(min_length=1, max_length=128)
    policy: JobBatchPolicy = Field(default_factory=JobBatchPolicy)
    jobs: list[JobBatchItemSpec] = Field(
        min_length=1,
        max_length=JOB_BATCH_ABSOLUTE_MAX_ITEMS,
    )

    @model_validator(mode="after")
    def safe_and_unique_ids(self) -> JobBatchManifest:
        if _SAFE_PUBLIC_ID.fullmatch(self.batch_id) is None:
            raise ValueError(
                "batch_id must contain only letters, digits, '.', '_' or '-'"
            )
        duplicate_ids = sorted(
            client_id
            for client_id, count in Counter(
                item.client_id for item in self.jobs
            ).items()
            if count > 1
        )
        if duplicate_ids:
            raise ValueError(
                "jobs.client_id values must be unique: "
                + ", ".join(duplicate_ids)
            )
        return self


def _configured_int(
    name: str,
    *,
    default: int,
    minimum: int,
    maximum: int,
) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if not minimum <= value <= maximum:
        raise ValueError(f"{name} must be between {minimum} and {maximum}")
    return value


def _configured_float(
    name: str,
    *,
    default: float,
    minimum: float,
    maximum: float,
) -> float:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be numeric") from exc
    if not math.isfinite(value) or not minimum <= value <= maximum:
        raise ValueError(f"{name} must be between {minimum:g} and {maximum:g}")
    return value


class JobBatchLimits:
    """Deployment admission and scheduling limits, validated fail closed."""

    def __init__(self) -> None:
        self.max_items = _configured_int(
            "ELASTIC_AGENT_MAX_JOB_BATCH_ITEMS",
            default=20,
            minimum=1,
            maximum=JOB_BATCH_ABSOLUTE_MAX_ITEMS,
        )
        self.max_total_workers = _configured_int(
            "ELASTIC_AGENT_MAX_JOB_BATCH_TOTAL_WORKERS",
            default=100,
            minimum=1,
            maximum=10_000,
        )
        self.max_worker_hours = _configured_float(
            "ELASTIC_AGENT_MAX_JOB_BATCH_WORKER_HOURS",
            default=1_440.0,
            minimum=1.0,
            maximum=1_000_000_000.0,
        )
        self.global_max_active_jobs = _configured_int(
            "ELASTIC_AGENT_JOB_BATCH_MAX_ACTIVE_JOBS",
            default=3,
            minimum=1,
            maximum=JOB_BATCH_GLOBAL_MAX_ACTIVE_JOBS,
        )


def _json_fingerprint(payload: object) -> str:
    canonical = json.dumps(
        payload,
        ensure_ascii=True,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")
    return hashlib.sha256(canonical).hexdigest()


def _raw_manifest_fingerprint(manifest: object) -> str:
    """Hash an unnormalized manifest exactly as it was persisted."""

    return _json_fingerprint(manifest)


def manifest_fingerprint(manifest: JobBatchManifest) -> str:
    """Hash the complete normalized private manifest for idempotency."""

    return _json_fingerprint(manifest.model_dump(mode="json"))


def _terminal_manifest_identities(
    payload: dict[str, Any],
) -> list[tuple[str, str]]:
    """Validate an immutable terminal manifest without parsing its JobSpec.

    A terminal batch can outlive the JobSpec revision that created it.  Its
    Jobs will never be dispatched again, so startup only needs the signed-by-
    hash envelope and item identities used by the public history view.  Active
    batches remain bound to the current strict JobSpec model.
    """

    if payload.get("state") != "terminal":
        raise ValueError("Job batch compatibility is terminal-only")
    raw_manifest = payload.get("manifest")
    if not isinstance(raw_manifest, dict) or set(raw_manifest) != {
        "schema_version",
        "batch_id",
        "policy",
        "jobs",
    }:
        raise ValueError("terminal Job batch manifest has invalid fields")
    batch_id = raw_manifest.get("batch_id")
    if (
        raw_manifest.get("schema_version") != JOB_BATCH_SCHEMA_VERSION
        or not isinstance(batch_id, str)
        or _SAFE_PUBLIC_ID.fullmatch(batch_id) is None
        or batch_id != payload.get("batch_id")
    ):
        raise ValueError("terminal Job batch manifest identity is invalid")
    policy = JobBatchPolicy.model_validate(raw_manifest.get("policy"))
    normalized_policy = policy.model_dump(mode="json")
    if (
        raw_manifest.get("policy") != normalized_policy
        or payload.get("policy") != normalized_policy
    ):
        raise ValueError("terminal Job batch manifest policy differs")
    jobs = raw_manifest.get("jobs")
    if (
        not isinstance(jobs, list)
        or not jobs
        or len(jobs) > JOB_BATCH_ABSOLUTE_MAX_ITEMS
    ):
        raise ValueError("terminal Job batch manifest items are invalid")

    identities: list[tuple[str, str]] = []
    seen_client_ids: set[str] = set()
    for item in jobs:
        if not isinstance(item, dict) or set(item) != {"client_id", "spec"}:
            raise ValueError("terminal Job batch manifest item fields are invalid")
        client_id = item.get("client_id")
        spec = item.get("spec")
        name = spec.get("name") if isinstance(spec, dict) else None
        if (
            not isinstance(client_id, str)
            or _SAFE_PUBLIC_ID.fullmatch(client_id) is None
            or client_id in seen_client_ids
            or not isinstance(name, str)
        ):
            raise ValueError("terminal Job batch manifest item identity is invalid")
        seen_client_ids.add(client_id)
        identities.append((client_id, name))

    fingerprint = _raw_manifest_fingerprint(raw_manifest)
    if not hmac.compare_digest(fingerprint, payload["manifest_fingerprint"]):
        raise ValueError("terminal Job batch manifest fingerprint differs")
    return identities


def _journal_manifest_matches(
    journal: dict[str, Any], requested_fingerprint: str,
) -> bool:
    """Compare a request to current or default-compatible legacy manifests."""

    stored = journal["manifest_fingerprint"]
    if hmac.compare_digest(stored, requested_fingerprint):
        return True
    try:
        normalized = JobBatchManifest.model_validate(journal["manifest"])
    except (TypeError, ValueError):
        return False
    return hmac.compare_digest(
        manifest_fingerprint(normalized), requested_fingerprint,
    )


def aggregate_manifest(manifest: JobBatchManifest) -> dict[str, Any]:
    """Return a secret-free resource summary for plan and journal responses."""

    total_workers = sum(item.spec.fanout.workers for item in manifest.jobs)
    worker_hours = sum(
        item.spec.fanout.workers * item.spec.ttl_seconds / 3_600
        for item in manifest.jobs
    )
    account_groups: dict[tuple[str, str, str, str, str, str], int] = {}
    explicit_slots = 0
    for item in manifest.jobs:
        spec = item.spec
        if spec.account.mode == "none":
            continue
        required = spec.fanout.workers * spec.account.per_worker
        key = (
            spec.account.agent_type,
            spec.account.auth_kind,
            spec.account.group,
            spec.account.binding,
            spec.account.mode,
            spec.account.model,
        )
        account_groups[key] = account_groups.get(key, 0) + required
        if spec.account.ids:
            explicit_slots += required
    account_requirements = [
        {
            "agent_type": key[0],
            "auth_kind": key[1],
            "group": key[2],
            "binding": key[3],
            "mode": key[4],
            "model": key[5] or None,
            "required_slots": required,
        }
        for key, required in sorted(account_groups.items())
    ]
    return {
        "job_count": len(manifest.jobs),
        "total_workers": total_workers,
        "total_worker_hours": worker_hours,
        "max_active_jobs": manifest.policy.max_active_jobs,
        "instance_types": [],
        "account_requirements": {
            "total_slots": sum(account_groups.values()),
            "explicit_slots": explicit_slots,
            "by_pool": account_requirements,
        },
    }


def validate_manifest_limits(
    manifest: JobBatchManifest,
    limits: JobBatchLimits,
) -> list[str]:
    """Return deployment-limit failures without reflecting any Job values."""

    summary = aggregate_manifest(manifest)
    errors: list[str] = []
    if len(manifest.jobs) > limits.max_items:
        errors.append(
            f"job_count {len(manifest.jobs)} exceeds configured maximum "
            f"{limits.max_items}"
        )
    if summary["total_workers"] > limits.max_total_workers:
        errors.append(
            f"total_workers {summary['total_workers']} exceeds configured "
            f"maximum {limits.max_total_workers}"
        )
    if summary["total_worker_hours"] > limits.max_worker_hours:
        errors.append(
            "total_worker_hours "
            f"{summary['total_worker_hours']:g} exceeds configured maximum "
            f"{limits.max_worker_hours:g}"
        )
    if manifest.policy.max_active_jobs > limits.global_max_active_jobs:
        errors.append(
            f"policy.max_active_jobs {manifest.policy.max_active_jobs} exceeds "
            "this Manager's configured global maximum "
            f"{limits.global_max_active_jobs}"
        )
    return errors


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_error(value: object, *, fallback: str) -> str:
    if not isinstance(value, str):
        return fallback
    cleaned = " ".join(value.replace("\x00", "").split())
    return cleaned[:2_048] or fallback


class JobBatchQueue:
    """Private journals plus one process-local durable queue dispatcher."""

    def __init__(
        self,
        manager: Any,
        *,
        submitter: Submitter | None = None,
        poll_interval_seconds: float = 1.0,
    ) -> None:
        self._manager = manager
        self._submitter = submitter
        self._poll_interval_seconds = max(0.05, poll_interval_seconds)
        self.limits = JobBatchLimits()
        self.path = Path(manager.config.registry.path).expanduser().with_name(
            "job-batches"
        )
        self._journals: dict[str, dict[str, Any]] = {}
        self._lock = asyncio.Lock()
        self._wakeup = asyncio.Event()
        self._stopping = asyncio.Event()
        self._runner: asyncio.Task[None] | None = None
        self._dispatch_tasks: dict[tuple[str, str], asyncio.Task[None]] = {}
        self._dirty_journals: set[str] = set()

    @property
    def started(self) -> bool:
        return self._runner is not None and not self._runner.done()

    async def start(self) -> None:
        if self.started:
            return
        self._stopping.clear()
        journals = await asyncio.to_thread(self._load_journals)
        async with self._lock:
            self._journals = journals
            self._dirty_journals.clear()
        self._runner = asyncio.create_task(
            self._run(),
            name="elastic-agent-job-batch-queue",
        )
        self._wakeup.set()

    async def stop(self) -> None:
        self._stopping.set()
        self._wakeup.set()
        runner = self._runner
        if runner is not None:
            await runner
        self._runner = None

    async def shutdown(self) -> None:
        """Lifecycle-compatible alias used by Manager and tests."""

        await self.stop()

    async def accept(
        self,
        manifest: JobBatchManifest,
        *,
        idempotency_key: str,
        plan_summary: dict[str, Any],
    ) -> tuple[dict[str, Any], bool]:
        """Durably accept one fully preflighted manifest or replay it."""

        digest = hashlib.sha256(idempotency_key.encode("utf-8")).hexdigest()
        job_batch_id = f"batch-{digest[:32]}"
        fingerprint = manifest_fingerprint(manifest)
        now = _now()
        async with self._lock:
            existing = self._journals.get(job_batch_id)
            if existing is not None:
                if not _journal_manifest_matches(existing, fingerprint):
                    raise JobBatchIdempotencyConflictError
                return self._public_view(existing), True

            journal = {
                "journal_schema_version": JOB_BATCH_JOURNAL_SCHEMA_VERSION,
                "job_batch_id": job_batch_id,
                "batch_id": manifest.batch_id,
                "manifest_fingerprint": fingerprint,
                # The private manifest contains AWS reference identifiers but
                # never resolved secret values. It is not projected by REST.
                "manifest": manifest.model_dump(mode="json"),
                "policy": manifest.policy.model_dump(mode="json"),
                "state": "queued",
                "created_at": now,
                "updated_at": now,
                "plan_summary": dict(plan_summary),
                "items": [
                    {
                        "client_id": item.client_id,
                        "name": item.spec.name,
                        "state": "queued",
                        "job_id": None,
                        "job_state": None,
                        "error": None,
                        "accepted_at": None,
                        "completed_at": None,
                    }
                    for item in manifest.jobs
                ],
            }
            await asyncio.to_thread(self._write_journal, journal)
            self._journals[job_batch_id] = journal
            view = self._public_view(journal)
        self._wakeup.set()
        return view, False

    async def replay_if_exists(
        self,
        manifest: JobBatchManifest,
        *,
        idempotency_key: str,
    ) -> dict[str, Any] | None:
        """Return an exact durable replay before mutable admission checks."""

        digest = hashlib.sha256(idempotency_key.encode("utf-8")).hexdigest()
        job_batch_id = f"batch-{digest[:32]}"
        fingerprint = manifest_fingerprint(manifest)
        async with self._lock:
            existing = self._journals.get(job_batch_id)
            if existing is None:
                return None
            if not _journal_manifest_matches(existing, fingerprint):
                raise JobBatchIdempotencyConflictError
            return self._public_view(existing)

    async def get(self, job_batch_id: str) -> dict[str, Any] | None:
        async with self._lock:
            journal = self._journals.get(job_batch_id)
            return self._public_view(journal) if journal is not None else None

    async def list(self, *, limit: int = JOB_BATCH_LIST_DEFAULT_LIMIT) -> list[dict[str, Any]]:
        limit = max(1, min(limit, JOB_BATCH_LIST_MAX_LIMIT))
        async with self._lock:
            journals = sorted(
                self._journals.values(),
                key=lambda value: value["created_at"],
                reverse=True,
            )[:limit]
            return [self._public_view(journal) for journal in journals]

    async def count(self) -> int:
        """Return the complete durable batch count, independent of list paging."""

        async with self._lock:
            return len(self._journals)

    async def _run(self) -> None:
        try:
            while not self._stopping.is_set():
                try:
                    await self._flush_dirty_journals()
                    await self._reconcile_accepted()
                    await self._dispatch_available()
                except Exception:  # noqa: BLE001 - keep durable runner alive
                    logger.exception("Job batch queue iteration failed")
                self._wakeup.clear()
                try:
                    await asyncio.wait_for(
                        self._wakeup.wait(),
                        timeout=self._poll_interval_seconds,
                    )
                except TimeoutError:
                    pass
        finally:
            # A canonical submit is itself crash-safe and idempotent. During a
            # graceful Manager stop, let already-entered submissions finish so
            # their batch journal can record the returned Job id.
            if self._dispatch_tasks:
                await asyncio.gather(
                    *tuple(self._dispatch_tasks.values()),
                    return_exceptions=True,
                )
            await self._flush_dirty_journals()

    async def _dispatch_available(self) -> None:
        async with self._lock:
            # Do not cross another Job submission boundary while an earlier
            # item transition is only in memory. Stable replay makes a crash
            # recoverable, but continuing would weaken the durable queue cap.
            if self._dirty_journals:
                return
            global_active = sum(
                1
                for journal in self._journals.values()
                for item in journal["items"]
                if item["state"] == "accepted"
            ) + len(self._dispatch_tasks)
            available = self.limits.global_max_active_jobs - global_active
            if available <= 0:
                return

            # Job count alone is not a sufficient fleet guard: three Jobs can
            # each fan out to many workers. Reserve scheduling room by declared
            # fanout across every accepted/dispatching batch item, and include
            # live Jobs submitted outside JobBatch. The Manager's lower-level
            # acquire_instance_capacity remains the authoritative race-safe
            # cloud guard; this queue check prevents predictable overcommit.
            provider = self._manager.config.provider
            provider_max_instances = (
                provider.aws.max_instances
                if provider.type == "aws"
                else provider.aliyun.max_instances
            )
            active_batch_job_ids: set[str] = set()
            active_worker_demand = 0
            for journal in self._journals.values():
                for index, item in enumerate(journal["items"]):
                    key = (journal["job_batch_id"], item["client_id"])
                    if (
                        item["state"] != "accepted"
                        and key not in self._dispatch_tasks
                    ):
                        continue
                    active_worker_demand += int(
                        journal["manifest"]["jobs"][index]["spec"]
                        ["fanout"]["workers"]
                    )
                    active_batch_job_ids.add(
                        item["job_id"]
                        or self._expected_job_id(*key)
                    )
            batch_orchestrator = getattr(self._manager, "_batch", None)
            list_jobs = getattr(batch_orchestrator, "list_jobs", None)
            if callable(list_jobs):
                for job in list_jobs():
                    summary = job.summary()
                    if (
                        job.job_id not in active_batch_job_ids
                        and summary.get("done") is not True
                    ):
                        active_worker_demand += job.spec.fanout.workers

            ordered = sorted(
                self._journals.values(), key=lambda value: value["created_at"]
            )
            for journal in ordered:
                if available <= 0:
                    break
                job_batch_id = journal["job_batch_id"]
                per_batch_active = sum(
                    item["state"] == "accepted"
                    for item in journal["items"]
                ) + sum(
                    key[0] == job_batch_id for key in self._dispatch_tasks
                )
                per_batch_limit = min(
                    journal["policy"]["max_active_jobs"],
                    self.limits.global_max_active_jobs,
                )
                for item in journal["items"]:
                    if available <= 0 or per_batch_active >= per_batch_limit:
                        break
                    key = (job_batch_id, item["client_id"])
                    if item["state"] != "queued" or key in self._dispatch_tasks:
                        continue
                    item_index = self._item_index(
                        journal, item["client_id"]
                    )
                    worker_demand = int(
                        journal["manifest"]["jobs"][item_index]["spec"]
                        ["fanout"]["workers"]
                    )
                    if (
                        active_worker_demand + worker_demand
                        > provider_max_instances
                    ):
                        # Strict global FIFO among concurrency-eligible items:
                        # do not let a later small Job starve this older large
                        # Job. Batches already at their per-batch active limit
                        # were skipped above and do not head-block the queue.
                        return
                    task = asyncio.create_task(
                        self._dispatch_one(job_batch_id, item["client_id"]),
                        name=(
                            "elastic-agent-job-batch-item-"
                            f"{job_batch_id}-{item['client_id']}"
                        ),
                    )
                    self._dispatch_tasks[key] = task
                    task.add_done_callback(
                        lambda completed, item_key=key: self._dispatch_done(
                            item_key, completed
                        )
                    )
                    available -= 1
                    per_batch_active += 1
                    active_worker_demand += worker_demand

    def _dispatch_done(
        self,
        key: tuple[str, str],
        task: asyncio.Task[None],
    ) -> None:
        self._dispatch_tasks.pop(key, None)
        if not task.cancelled():
            try:
                task.result()
            except Exception:  # pragma: no cover - defensive task boundary
                logger.exception(
                    "Unhandled Job batch dispatch failure for %s/%s", *key
                )
        self._wakeup.set()

    async def _dispatch_one(self, job_batch_id: str, client_id: str) -> None:
        async with self._lock:
            journal = self._journals.get(job_batch_id)
            if journal is None:
                return
            item_index = self._item_index(journal, client_id)
            item = journal["items"][item_index]
            if item["state"] != "queued":
                return
            manifest_item = journal["manifest"]["jobs"][item_index]
            raw_spec = dict(manifest_item["spec"])

        item_key = self._item_idempotency_key(job_batch_id, client_id)
        try:
            submitter = self._submitter or self._canonical_submit
            detail = await submitter(raw_spec, item_key)
            job_id = detail.get("job_id")
            if not isinstance(job_id, str) or _SAFE_JOB_ID.fullmatch(job_id) is None:
                raise RuntimeError("canonical Job submission returned no valid Job id")
            terminal = detail.get("done") is True or detail.get("state") in (
                _TERMINAL_JOB_STATES
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - isolate one queued item
            status_code = getattr(exc, "status_code", None)
            if isinstance(status_code, int):
                detail = _safe_error(
                    getattr(exc, "detail", None),
                    fallback=f"Job submission rejected ({status_code})",
                )
            else:
                logger.exception(
                    "Job batch item submission failed for %s/%s",
                    job_batch_id,
                    client_id,
                )
                detail = "internal Job submission error"
            async with self._lock:
                journal = self._journals.get(job_batch_id)
                if journal is None:
                    return
                item = journal["items"][self._item_index(journal, client_id)]
                item["state"] = "error"
                item["error"] = detail
                item["completed_at"] = _now()
                self._refresh_batch_state(journal)
                await self._persist_journal_locked(journal)
            return

        async with self._lock:
            journal = self._journals.get(job_batch_id)
            if journal is None:
                return
            item = journal["items"][self._item_index(journal, client_id)]
            item["job_id"] = job_id
            item["accepted_at"] = item["accepted_at"] or _now()
            if terminal:
                item["state"] = "terminal"
                item["job_state"] = str(detail.get("state") or "unknown")
                item["completed_at"] = _now()
                item["error"] = (
                    _safe_error(detail.get("error"), fallback="Job failed")
                    if detail.get("state") == "failed"
                    else None
                )
            else:
                item["state"] = "accepted"
                item["job_state"] = str(detail.get("state") or "accepted")
            self._refresh_batch_state(journal)
            await self._persist_journal_locked(journal)

    async def _canonical_submit(
        self,
        raw_spec: dict[str, object],
        idempotency_key: str,
    ) -> dict[str, Any]:
        # Imported lazily to keep the core model usable without FastAPI and to
        # make this dependency on the canonical route boundary explicit.
        from elastic_agent.api.routes.jobs import _submit_job_payload

        return await _submit_job_payload(raw_spec, idempotency_key)

    async def _reconcile_accepted(self) -> None:
        async with self._lock:
            accepted = [
                (journal["job_batch_id"], item["client_id"], item["job_id"])
                for journal in self._journals.values()
                for item in journal["items"]
                if item["state"] == "accepted"
            ]
        updates: list[tuple[str, str, str, str | None]] = []
        for job_batch_id, client_id, job_id in accepted:
            state, error = await self._underlying_job_terminal(job_id)
            if state is not None:
                updates.append((job_batch_id, client_id, state, error))
        if not updates:
            return
        async with self._lock:
            dirty: set[str] = set()
            for job_batch_id, client_id, job_state, error in updates:
                journal = self._journals.get(job_batch_id)
                if journal is None:
                    continue
                item = journal["items"][self._item_index(journal, client_id)]
                if item["state"] != "accepted":
                    continue
                item["job_state"] = job_state
                if job_state == "prepared":
                    # The canonical Job journal proves that submission never
                    # crossed its account/cloud launch gate. Persistently put
                    # the item back on the queue; its stable idempotency key
                    # then resumes this exact Job instead of creating another.
                    item["state"] = "queued"
                    item["error"] = None
                    item["completed_at"] = None
                else:
                    item["state"] = "terminal"
                    item["error"] = error
                    item["completed_at"] = _now()
                self._refresh_batch_state(journal)
                dirty.add(job_batch_id)
            for job_batch_id in dirty:
                await self._persist_journal_locked(
                    self._journals[job_batch_id]
                )

    async def _persist_journal_locked(self, journal: dict[str, Any]) -> bool:
        """Persist a mutated in-memory journal, retaining a retry fence."""

        job_batch_id = journal["job_batch_id"]
        try:
            await asyncio.to_thread(self._write_journal, journal)
        except Exception:  # noqa: BLE001 - retry from the durable runner
            self._dirty_journals.add(job_batch_id)
            logger.exception("Cannot persist Job batch %s", job_batch_id)
            return False
        self._dirty_journals.discard(job_batch_id)
        return True

    async def _flush_dirty_journals(self) -> None:
        async with self._lock:
            for job_batch_id in tuple(sorted(self._dirty_journals)):
                journal = self._journals.get(job_batch_id)
                if journal is None:
                    self._dirty_journals.discard(job_batch_id)
                    continue
                await self._persist_journal_locked(journal)

    async def _underlying_job_terminal(
        self, job_id: str
    ) -> tuple[str | None, str | None]:
        batch = getattr(self._manager, "_batch", None)
        if batch is not None:
            live = batch.get_job(job_id)
            if live is not None:
                summary = live.summary()
                if summary.get("done") is True:
                    state = str(summary.get("state") or "unknown")
                    error = (
                        _safe_error(summary.get("error"), fallback="Job failed")
                        if state == "failed"
                        else None
                    )
                    return state, error
                return None, None
        try:
            payload = await asyncio.to_thread(
                load_job_spec_journal,
                self._manager.config.registry.path,
                job_id,
            )
        except FileNotFoundError:
            # Fail closed. An accepted Job with missing durable state must not
            # release queue capacity and permit accidental over-scheduling.
            return None, None
        except Exception:  # noqa: BLE001
            logger.exception("Cannot reconcile accepted Job %s", job_id)
            return None, None
        state = payload.get("submission_state")
        if state == "prepared":
            # A prepared journal has no account/cloud side effects and the
            # canonical submit path can safely replay it with the same stable
            # idempotency identity. Wait for startup recovery first so the
            # replay cannot race prior-process ownership or admission gates.
            if getattr(self._manager, "binding_recovery_ready", False):
                return "prepared", None
            return None, None
        if state in {"launching", "running"}:
            # A nonterminal durable Job that is absent from this process's
            # orchestrator was owned by an earlier Manager process.  Startup
            # recovery is the authority for its cloud/account side effects;
            # do not release the JobBatch slot until that recovery proves all
            # prior-process ownership settled.  Once it does, the lost
            # coroutine cannot resume, so retaining the item as ``accepted``
            # forever would create a ghost active Job and permanently reduce
            # queue/fleet concurrency after every Manager restart.
            if getattr(self._manager, "binding_recovery_ready", False):
                return (
                    "failed",
                    "Manager restarted during execution; startup recovery "
                    "settled the previous worker ownership",
                )
            return None, None
        if state not in _TERMINAL_JOB_STATES:
            return None, None
        summary = payload.get("terminal_summary")
        raw_error = summary.get("error") if isinstance(summary, dict) else None
        error = (
            _safe_error(raw_error, fallback="Job failed")
            if state == "failed"
            else None
        )
        return str(state), error

    @staticmethod
    def _item_idempotency_key(job_batch_id: str, client_id: str) -> str:
        client_digest = hashlib.sha256(client_id.encode("utf-8")).hexdigest()
        return f"elastic-job-batch/{job_batch_id}/{client_digest}"

    @classmethod
    def _expected_job_id(cls, job_batch_id: str, client_id: str) -> str:
        item_key = cls._item_idempotency_key(job_batch_id, client_id)
        return "job-" + hashlib.sha256(item_key.encode("utf-8")).hexdigest()[:32]

    @staticmethod
    def _item_index(journal: dict[str, Any], client_id: str) -> int:
        for index, item in enumerate(journal["items"]):
            if item["client_id"] == client_id:
                return index
        raise RuntimeError("Job batch journal item is unavailable")

    @staticmethod
    def _refresh_batch_state(journal: dict[str, Any]) -> None:
        states = [item["state"] for item in journal["items"]]
        if all(state in {"terminal", "error"} for state in states):
            journal["state"] = "terminal"
        elif any(state == "accepted" for state in states):
            journal["state"] = "running"
        else:
            journal["state"] = "queued"
        journal["updated_at"] = _now()

    @staticmethod
    def _public_view(journal: dict[str, Any]) -> dict[str, Any]:
        counts = Counter(item["state"] for item in journal["items"])
        summary = {
            key: value
            for key, value in journal["plan_summary"].items()
            if key in _PUBLIC_SUMMARY_FIELDS
        }
        summary.update(
            {
                "queued": counts.get("queued", 0),
                "accepted": counts.get("accepted", 0),
                "terminal": counts.get("terminal", 0),
                "error": counts.get("error", 0),
            }
        )
        return {
            "job_batch_id": journal["job_batch_id"],
            "batch_id": journal["batch_id"],
            "state": journal["state"],
            "atomic": False,
            "created_at": journal["created_at"],
            "updated_at": journal["updated_at"],
            "summary": summary,
            "items": [
                {
                    "client_id": item["client_id"],
                    "name": item["name"],
                    "job_id": item["job_id"],
                    "state": item["state"],
                    "job_state": item["job_state"],
                    "error": item["error"],
                    "accepted_at": item["accepted_at"],
                    "completed_at": item["completed_at"],
                }
                for item in journal["items"]
            ],
        }

    def _load_journals(self) -> dict[str, dict[str, Any]]:
        directory = tighten_private_json_directory(self.path, create=True)
        journals: dict[str, dict[str, Any]] = {}
        for path in sorted(directory.glob("*.json")):
            if _SAFE_JOB_BATCH_ID.fullmatch(path.stem) is None:
                raise RuntimeError(
                    f"unexpected file in Job batch journal directory: {path.name}"
                )
            tighten_state_file(path)
            if path.stat().st_size > JOB_BATCH_JOURNAL_MAX_BYTES:
                raise RuntimeError(f"Job batch journal is too large: {path.name}")
            with path.open("r", encoding="utf-8") as stream:
                payload = json.load(stream)
            self._validate_journal(
                payload,
                expected_id=path.stem,
                allow_terminal_compat=True,
            )
            journals[payload["job_batch_id"]] = payload
        return journals

    def _write_journal(self, journal: dict[str, Any]) -> None:
        self._validate_journal(journal, expected_id=journal["job_batch_id"])
        encoded = json.dumps(
            journal,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        if len(encoded.encode("utf-8")) > JOB_BATCH_JOURNAL_MAX_BYTES:
            raise RuntimeError("Job batch journal exceeds its private size limit")
        directory = secure_state_directory(self.path)
        atomic_write_private(
            directory / f"{journal['job_batch_id']}.json", encoded
        )

    @staticmethod
    def _validate_journal(
        payload: Any,
        *,
        expected_id: str,
        allow_terminal_compat: bool = False,
    ) -> None:
        if not isinstance(payload, dict):
            raise ValueError("Job batch journal must be an object")
        required = {
            "journal_schema_version",
            "job_batch_id",
            "batch_id",
            "manifest_fingerprint",
            "manifest",
            "policy",
            "state",
            "created_at",
            "updated_at",
            "plan_summary",
            "items",
        }
        if set(payload) != required:
            raise ValueError("Job batch journal has invalid fields")
        job_batch_id = payload.get("job_batch_id")
        if (
            payload.get("journal_schema_version")
            != JOB_BATCH_JOURNAL_SCHEMA_VERSION
            or not isinstance(job_batch_id, str)
            or _SAFE_JOB_BATCH_ID.fullmatch(job_batch_id) is None
            or job_batch_id != expected_id
            or payload.get("state") not in _BATCH_STATES
            or not isinstance(payload.get("manifest_fingerprint"), str)
            or _FINGERPRINT.fullmatch(payload["manifest_fingerprint"]) is None
        ):
            raise ValueError("Job batch journal identity/state is invalid")
        try:
            manifest = JobBatchManifest.model_validate(payload.get("manifest"))
        except ValidationError:
            if not allow_terminal_compat:
                raise
            requested_identities = _terminal_manifest_identities(payload)
            manifest = None
        else:
            if manifest.batch_id != payload.get("batch_id"):
                raise ValueError("Job batch journal manifest identity differs")
            if not hmac.compare_digest(
                manifest_fingerprint(manifest),
                payload["manifest_fingerprint"],
            ) and not hmac.compare_digest(
                _json_fingerprint(payload["manifest"]),
                payload["manifest_fingerprint"],
            ):
                raise ValueError("Job batch journal manifest fingerprint differs")
            if payload.get("policy") != manifest.policy.model_dump(mode="json"):
                raise ValueError("Job batch journal policy differs")
            requested_identities = [
                (item.client_id, item.spec.name) for item in manifest.jobs
            ]
        items = payload.get("items")
        if (
            not isinstance(items, list)
            or len(items) != len(requested_identities)
        ):
            raise ValueError("Job batch journal items are invalid")
        for stored, requested in zip(items, requested_identities, strict=True):
            if not isinstance(stored, dict) or set(stored) != {
                "client_id",
                "name",
                "state",
                "job_id",
                "job_state",
                "error",
                "accepted_at",
                "completed_at",
            }:
                raise ValueError("Job batch journal item fields are invalid")
            if (
                stored.get("client_id") != requested[0]
                or stored.get("name") != requested[1]
                or stored.get("state") not in _ITEM_STATES
            ):
                raise ValueError("Job batch journal item identity/state is invalid")
            if manifest is None and stored["state"] not in {"terminal", "error"}:
                raise ValueError("compatible Job batch item is not terminal")
            job_id = stored.get("job_id")
            if job_id is not None and (
                not isinstance(job_id, str)
                or _SAFE_JOB_ID.fullmatch(job_id) is None
            ):
                raise ValueError("Job batch journal contains an invalid Job id")
            if stored["state"] in {"accepted", "terminal"} and job_id is None:
                raise ValueError("accepted Job batch item has no Job id")
        if not isinstance(payload.get("plan_summary"), dict):
            raise ValueError("Job batch journal plan summary is invalid")
        if not set(payload["plan_summary"]).issubset(_PUBLIC_SUMMARY_FIELDS):
            raise ValueError("Job batch journal plan summary has invalid fields")


class JobBatchIdempotencyConflictError(Exception):
    """The deterministic batch id exists for another normalized manifest."""
