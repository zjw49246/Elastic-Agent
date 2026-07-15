"""Batch job REST API — the frontend's Job panel.

Submit a declarative JobSpec (or one referencing uploaded Harness code) and fan
it out across the fleet via the Manager's BatchOrchestrator. Also accepts
Harness code uploads so the "upload code" path has somewhere to land.
"""

from __future__ import annotations

import re
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from elastic_agent.api.auth import require_api_key
from elastic_agent.core.job_spec import JobSpec

router = APIRouter(tags=["jobs"], dependencies=[Depends(require_api_key)])


def _mgr():
    from elastic_agent.api.app import get_manager
    return get_manager()


def _job_detail(job) -> dict:
    return {
        **job.summary(),
        "spec": job.spec.model_dump(),
        "workers_detail": [
            {
                "worker_id": r.worker_id,
                "phase": r.phase.value,
                "shard_index": r.ctx.shard_index,
                "account_id": r.account_id,
                "account_email": r.ctx.account_email,
                "rotations": r.rotations,
                "task_id": r.task_id,
                "error": r.error,
            }
            for r in job.runs.values()
        ],
    }


@router.post("/jobs", status_code=201)
async def submit_job(spec: JobSpec) -> dict:
    """Validate + launch a batch job. Returns the job summary."""
    try:
        job = await _mgr().batch.launch(spec)
    except NotImplementedError as exc:
        # provision/login hooks not wired for live runs — surface clearly.
        raise HTTPException(503, str(exc))
    return _job_detail(job)


@router.get("/jobs")
async def list_jobs() -> dict:
    jobs = _mgr().batch.list_jobs()
    return {"jobs": [j.summary() for j in jobs], "total": len(jobs)}


@router.get("/jobs/{job_id}")
async def get_job(job_id: str) -> dict:
    job = _mgr().batch.get_job(job_id)
    if job is None:
        raise HTTPException(404, f"Job {job_id} not found")
    return _job_detail(job)


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
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*\.py", req.filename):
        raise HTTPException(400, "filename must be a simple <name>.py")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", req.class_name):
        raise HTTPException(400, "class_name must be a valid identifier")

    mgr = _mgr()
    base = Path(mgr.config.registry.path).with_name("harness_plugins")
    base.mkdir(parents=True, exist_ok=True)
    dest = base / req.filename
    dest.write_text(req.content, encoding="utf-8")

    ref = f"{dest}:{req.class_name}"
    # Fail fast if the uploaded code doesn't actually resolve to a Harness.
    from elastic_agent.harness.generic import load_harness_class
    try:
        load_harness_class(ref)
    except Exception as exc:
        dest.unlink(missing_ok=True)
        raise HTTPException(400, f"uploaded code is not a valid Harness: {exc}")

    return HarnessUploadResponse(harness_ref=ref, path=str(dest))
