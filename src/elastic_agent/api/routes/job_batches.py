"""REST endpoints for durable JSON Job batches."""

from __future__ import annotations

import json
from typing import Any

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request, Response
from fastapi.exceptions import RequestValidationError
from pydantic import ValidationError

from elastic_agent.api.auth import require_api_key
from elastic_agent.api.body_limit import REQUEST_BODY_LIMIT_STATE_KEY
from elastic_agent.core.job_batch import (
    JOB_BATCH_LIST_DEFAULT_LIMIT,
    JOB_BATCH_LIST_MAX_LIMIT,
    JOB_BATCH_MAX_BODY_BYTES,
    JobBatchIdempotencyConflictError,
    JobBatchManifest,
    aggregate_manifest,
    validate_manifest_limits,
)

router = APIRouter(
    tags=["job-batches"],
    dependencies=[Depends(require_api_key)],
)

_NO_STORE_HEADERS = {
    "Cache-Control": "no-store",
    "Pragma": "no-cache",
}


def _mgr():
    from elastic_agent.api.app import get_manager

    return get_manager()


def _reject_nonstandard_json_constant(value: str) -> None:
    raise ValueError(f"invalid JSON constant {value!r}")


def _reject_duplicate_json_keys(
    pairs: list[tuple[str, object]],
) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON object key")
        result[key] = value
    return result


async def _read_manifest_request(request: Request) -> dict[str, object]:
    """Read a JSON manifest with an endpoint-specific 2 MiB hard ceiling."""

    raw_content_length = request.headers.get("content-length")
    if raw_content_length is not None:
        try:
            content_length = int(raw_content_length)
        except ValueError as exc:
            raise HTTPException(400, "invalid Content-Length") from exc
        if content_length < 0:
            raise HTTPException(400, "invalid Content-Length")
        if content_length > JOB_BATCH_MAX_BODY_BYTES:
            raise HTTPException(
                413,
                "Job batch request body exceeds the 2097152-byte limit",
            )

    request_state = request.scope.get("state")
    middleware_limit = (
        request_state.get(REQUEST_BODY_LIMIT_STATE_KEY)
        if isinstance(request_state, dict)
        else None
    )
    if (
        isinstance(middleware_limit, int)
        and 0 < middleware_limit <= JOB_BATCH_MAX_BODY_BYTES
    ):
        body: bytes | bytearray = await request.body()
        if len(body) > JOB_BATCH_MAX_BODY_BYTES:
            raise HTTPException(
                413,
                "Job batch request body exceeds the 2097152-byte limit",
            )
    else:
        incremental = bytearray()
        async for chunk in request.stream():
            if len(incremental) + len(chunk) > JOB_BATCH_MAX_BODY_BYTES:
                raise HTTPException(
                    413,
                    "Job batch request body exceeds the 2097152-byte limit",
                )
            incremental.extend(chunk)
        body = incremental
    if not body:
        raise HTTPException(422, "Job batch request body must be a JSON object")
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
        raise HTTPException(422, "invalid Job batch JSON request body") from exc
    if not isinstance(payload, dict):
        raise HTTPException(422, "Job batch request body must be a JSON object")
    return payload


def _validated_manifest(payload: dict[str, object]) -> JobBatchManifest:
    try:
        return JobBatchManifest.model_validate(payload)
    except ValidationError as exc:
        errors = exc.errors(
            include_url=True,
            include_context=False,
            include_input=False,
        )
        for error in errors:
            error["loc"] = ("body", *error.get("loc", ()))
        raise RequestValidationError(errors) from exc


def _normalize_idempotency_key(value: str | None) -> str:
    if value is None:
        raise HTTPException(400, "Idempotency-Key is required")
    normalized = value.strip()
    if (
        not normalized
        or len(normalized) > 200
        or any(ord(character) < 0x20 for character in normalized)
    ):
        raise HTTPException(400, "invalid Idempotency-Key")
    return normalized


def _safe_preflight_error(exc: HTTPException) -> str:
    if isinstance(exc.detail, str):
        return " ".join(exc.detail.replace("\x00", "").split())[:2_048]
    return f"Job preflight failed ({exc.status_code})"


async def _plan(manifest: JobBatchManifest) -> dict[str, Any]:
    """Run every canonical Job preflight and return only secret-free fields."""

    from elastic_agent.api.routes.jobs import _preflight_job

    manager = _mgr()
    summary = aggregate_manifest(manifest)
    aggregate_errors = validate_manifest_limits(
        manifest, manager.job_batch_queue.limits
    )
    provider = manager.config.provider
    provider_max_instances = (
        provider.aws.max_instances
        if provider.type == "aws"
        else provider.aliyun.max_instances
    )
    effective_active_jobs = min(
        manifest.policy.max_active_jobs,
        manager.job_batch_queue.limits.global_max_active_jobs,
    )
    max_concurrent_workers = sum(
        sorted(
            (item.spec.fanout.workers for item in manifest.jobs),
            reverse=True,
        )[:effective_active_jobs]
    )
    summary["max_concurrent_workers"] = max_concurrent_workers
    summary["provider_max_instances"] = provider_max_instances
    if max_concurrent_workers > provider_max_instances:
        aggregate_errors.append(
            f"maximum concurrent fanout {max_concurrent_workers} exceeds "
            f"provider instance capacity {provider_max_instances}"
        )
    item_views: list[dict[str, Any]] = []
    effective_instance_types: set[str] = set()
    all_warnings: list[str] = []
    valid = not aggregate_errors

    # Deliberately bounded and sequential. A maximum-size manifest must not
    # create 100 simultaneous account/history probes against the Manager.
    for item in manifest.jobs:
        warnings: list[str] = []
        errors: list[str] = []
        try:
            plan = await _preflight_job(manager, item.spec)
            raw_warnings = plan.get("warnings")
            if isinstance(raw_warnings, list):
                warnings = [
                    str(warning)[:2_048]
                    for warning in raw_warnings
                    if isinstance(warning, str)
                ]
            fanout = plan.get("fanout")
            if isinstance(fanout, dict):
                instance_type = fanout.get("instance_type")
                if isinstance(instance_type, str) and instance_type:
                    effective_instance_types.add(instance_type)
        except HTTPException as exc:
            errors.append(_safe_preflight_error(exc))
            valid = False
        item_views.append(
            {
                "client_id": item.client_id,
                "name": item.spec.name,
                "valid": not errors,
                "warnings": warnings,
                "errors": errors,
            }
        )
        all_warnings.extend(warnings)

    summary["instance_types"] = sorted(effective_instance_types)
    # The platform uses the side-effect-free plan call as the capability
    # handshake before partitioning a campaign.  ``summary.max_active_jobs``
    # intentionally describes this particular manifest's requested policy,
    # which may be a one-job probe; it must not be mistaken for the Manager's
    # published deployment ceiling.  Return the latter explicitly so a small
    # probe cannot permanently downgrade a larger campaign to serial work.
    by_pool = summary.get("account_requirements", {}).get("by_pool", [])
    supported_models = sorted(
        {
            str(pool["model"])
            for pool in by_pool
            if isinstance(pool, dict)
            and isinstance(pool.get("model"), str)
            and pool["model"]
        }
    )
    return {
        "valid": valid,
        "side_effects": False,
        "atomic": False,
        "batch_id": manifest.batch_id,
        "capabilities": {
            "max_batch_items": manager.job_batch_queue.limits.max_items,
            "max_active_jobs": manager.job_batch_queue.limits.global_max_active_jobs,
            "supported_models": supported_models,
        },
        "summary": summary,
        "items": item_views,
        "errors": aggregate_errors,
        "warnings": [
            "Jobs are accepted independently; this batch is not a cloud transaction.",
            *dict.fromkeys(all_warnings),
        ],
    }


@router.post("/job-batches/plan")
async def plan_job_batch(request: Request, response: Response) -> dict[str, Any]:
    response.headers.update(_NO_STORE_HEADERS)
    manifest = _validated_manifest(await _read_manifest_request(request))
    return await _plan(manifest)


@router.post("/job-batches", status_code=201)
async def submit_job_batch(
    request: Request,
    response: Response,
    idempotency_key: str | None = Header(
        default=None, alias="Idempotency-Key"
    ),
) -> dict[str, Any]:
    response.headers.update(_NO_STORE_HEADERS)
    manifest = _validated_manifest(await _read_manifest_request(request))
    key = _normalize_idempotency_key(idempotency_key)
    queue = _mgr().job_batch_queue
    try:
        replay = await queue.replay_if_exists(
            manifest, idempotency_key=key
        )
    except JobBatchIdempotencyConflictError as exc:
        raise HTTPException(
            409,
            "Idempotency-Key was already used for another Job batch manifest",
        ) from exc
    if replay is not None:
        replay["idempotent_replay"] = True
        return replay

    plan = await _plan(manifest)
    if plan["valid"] is not True:
        rejected_items = [
            {
                "client_id": item["client_id"],
                "errors": item["errors"],
            }
            for item in plan["items"]
            if item["errors"]
        ]
        raise HTTPException(
            422,
            {
                "message": "Job batch preflight failed; no batch was accepted",
                "errors": plan["errors"],
                "items": rejected_items,
            },
        )
    try:
        view, replayed = await queue.accept(
            manifest,
            idempotency_key=key,
            plan_summary=plan["summary"],
        )
    except JobBatchIdempotencyConflictError as exc:
        raise HTTPException(
            409,
            "Idempotency-Key was already used for another Job batch manifest",
        ) from exc
    if replayed:
        view["idempotent_replay"] = True
    return view


@router.get("/job-batches")
async def list_job_batches(
    response: Response,
    limit: int = Query(
        default=JOB_BATCH_LIST_DEFAULT_LIMIT,
        ge=1,
        le=JOB_BATCH_LIST_MAX_LIMIT,
    ),
) -> dict[str, Any]:
    response.headers.update(_NO_STORE_HEADERS)
    batches = await _mgr().job_batch_queue.list(limit=limit)
    total = await _mgr().job_batch_queue.count()
    return {"batches": batches, "total": total}


@router.get("/job-batches/{job_batch_id}")
async def get_job_batch(job_batch_id: str, response: Response) -> dict[str, Any]:
    response.headers.update(_NO_STORE_HEADERS)
    if (
        len(job_batch_id) != 38
        or not job_batch_id.startswith("batch-")
        or any(character not in "0123456789abcdef" for character in job_batch_id[6:])
    ):
        raise HTTPException(404, "Job batch not found")
    view = await _mgr().job_batch_queue.get(job_batch_id)
    if view is None:
        raise HTTPException(404, "Job batch not found")
    return view
