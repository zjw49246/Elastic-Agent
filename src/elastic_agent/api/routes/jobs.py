"""Batch job REST API — the frontend's Job panel.

Submit a declarative JobSpec (or one referencing uploaded Harness code) and fan
it out across the fleet via the Manager's BatchOrchestrator. Also accepts
Harness code uploads so the "upload code" path has somewhere to land.
"""

from __future__ import annotations

import io
import json
import re
import tarfile
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse
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


def _collected_dir(mgr, job_id: str) -> Path:
    """Where a job's collected results live on the Manager.

    The batch flow rsyncs each worker's ``collect.paths`` here after the run; the
    endpoints below expose them for download — that's how results reach the user.
    """
    return Path(mgr.config.registry.path).with_name("collected") / job_id


def _results_for(mgr, job_id: str, base) -> dict:
    files, scores = [], []
    for p in sorted(base.rglob("*")):
        if p.is_file():
            files.append({"path": str(p.relative_to(base)), "size": p.stat().st_size})
    for p in base.rglob("*.json"):
        if "instances" in p.parts:
            continue
        try:
            d = json.loads(p.read_text())
        except Exception:
            continue
        if isinstance(d, dict) and "final_score" in d:
            scores.append({
                "task_id": d.get("task_id"), "prompt_level": d.get("prompt_level"),
                "status": d.get("status"), "final_score": d.get("final_score"),
            })
    s3_uri = mgr._s3_uploader.s3_uri(job_id) if getattr(mgr, "_s3_uploader", None) else None
    return {"job_id": job_id, "file_count": len(files), "scores": scores, "s3_uri": s3_uri, "files": files}


@router.get("/results")
async def list_all_results() -> dict:
    """List every collected result dir on the Manager (browsable regardless of
    how the run was launched)."""
    mgr = _mgr()
    root = Path(mgr.collected_root)
    jobs = []
    if root.is_dir():
        for d in sorted(root.iterdir()):
            if d.is_dir():
                r = _results_for(mgr, d.name, d)
                jobs.append({k: r[k] for k in ("job_id", "file_count", "scores", "s3_uri")})
    return {"jobs": jobs, "total": len(jobs)}


@router.get("/jobs/{job_id}/results")
async def job_results(job_id: str) -> dict:
    """List a job's collected result files + surface benchmark scores."""
    base = _collected_dir(_mgr(), job_id)
    if not base.is_dir():
        raise HTTPException(404, f"no collected results for job {job_id}")
    r = _results_for(_mgr(), job_id, base)
    r["files"] = r["files"][:500]
    return r


@router.get("/jobs/{job_id}/results/download")
async def job_results_download(job_id: str) -> StreamingResponse:
    """Download a job's collected results as a .tar.gz."""
    base = _collected_dir(_mgr(), job_id)
    if not base.is_dir():
        raise HTTPException(404, f"no collected results for job {job_id}")
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        tar.add(str(base), arcname=job_id)
    buf.seek(0)
    return StreamingResponse(
        buf, media_type="application/gzip",
        headers={"Content-Disposition": f'attachment; filename="{job_id}-results.tar.gz"'},
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
