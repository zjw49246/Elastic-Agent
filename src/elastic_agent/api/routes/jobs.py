"""Batch job REST API — the frontend's Job panel.

Submit a declarative JobSpec (or one referencing uploaded Harness code) and fan
it out across the fleet via the Manager's BatchOrchestrator. Also accepts
Harness code uploads so the "upload code" path has somewhere to land.
"""

from __future__ import annotations

import io
import json
import os
import re
import tarfile
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from elastic_agent.api.auth import require_api_key
from elastic_agent.core.batch_orchestrator import JobSpecPersistenceError
from elastic_agent.core.job_spec import JobSpec
from elastic_agent.core.job_spec_store import job_specs_dir

router = APIRouter(tags=["jobs"], dependencies=[Depends(require_api_key)])


def _mgr():
    from elastic_agent.api.app import get_manager
    return get_manager()


def _specs_dir(mgr) -> Path:
    """Where submitted JobSpecs are persisted so they survive a Manager restart
    (the orchestrator's job records are in-memory and lost on restart)."""
    return job_specs_dir(mgr.config.registry.path)


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
                "lease_id": r.lease_id,
                "eip": r.eip,
                "eip_allocation_id": r.eip_allocation_id,
                "final_collected": r.final_collected,
                "collection_error": r.collection_error,
                "cleaned_up": r.cleaned_up,
                "cleanup_error": r.cleanup_error,
                "cleanup_attempts": r.cleanup_attempts,
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


@router.post("/jobs", status_code=201)
async def submit_job(spec: JobSpec) -> dict:
    """Validate + launch a batch job. Returns the job summary."""
    mgr = _mgr()
    try:
        # Manager-wired submit journals the spec before registration; scale-out
        # and bring-up remain background work after this call returns.
        job = await mgr.batch.submit(spec)
    except JobSpecPersistenceError as exc:
        raise HTTPException(500, str(exc)) from exc
    except NotImplementedError as exc:
        raise HTTPException(503, str(exc)) from exc
    return _job_detail(job)


@router.post("/jobs/{job_id}/resubmit", status_code=201)
async def resubmit_job(job_id: str) -> dict:
    """Relaunch a job from its persisted spec — works even after a Manager
    restart wiped the in-memory record."""
    mgr = _mgr()
    p = _specs_dir(mgr) / f"{job_id}.json"
    if not p.exists():
        raise HTTPException(404, f"no persisted spec for job {job_id}")
    spec = JobSpec(**json.loads(p.read_text(encoding="utf-8"))["spec"])
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
    for f in sorted(_specs_dir(mgr).glob("*.json")):
        if f.stem in live_ids:
            continue
        try:
            data = json.loads(f.read_text(encoding="utf-8"))
        except Exception:
            continue
        recovered = leases_by_job.get(f.stem, [])
        collection_errors = [
            lease.recovery_collection_error
            for lease in recovered
            if lease.recovery_collection_error
        ]
        cleanup_pending = sum(lease.state != "released" for lease in recovered)
        out.append({
            "job_id": f.stem,
            "name": data.get("name", ""),
            "workers": len(recovered),
            "phases": {"failed": len(collection_errors)} if collection_errors else {},
            "done": cleanup_pending == 0,
            "cleanup_pending": cleanup_pending,
            "error": "; ".join(collection_errors) or None,
            "in_memory": False,
            "workers_detail": [],
            "recovery_leases": [lease.model_dump() for lease in recovered],
        })
    return {"jobs": out, "total": len(out)}


@router.get("/jobs/{job_id}")
async def get_job(job_id: str) -> dict:
    mgr = _mgr()
    job = mgr.batch.get_job(job_id)
    if job is not None:
        return _job_detail(job)
    # Fall back to the persisted spec (job gone from memory after a restart).
    p = _specs_dir(mgr) / f"{job_id}.json"
    if p.exists():
        data = json.loads(p.read_text(encoding="utf-8"))
        leases = await mgr.account_binding_store.list_leases()
        recovered = [lease for lease in leases if lease.job_id == job_id]
        collection_errors = [
            lease.recovery_collection_error
            for lease in recovered
            if lease.recovery_collection_error
        ]
        return {
            "job_id": job_id,
            "name": data.get("name"),
            "in_memory": False,
            "spec": data.get("spec"),
            "workers_detail": [],
            "cleanup_pending": sum(
                lease.state != "released" for lease in recovered
            ),
            "error": "; ".join(collection_errors) or None,
            "recovery_leases": [lease.model_dump() for lease in recovered],
            "note": (
                "not in Manager memory (restarted); recovery collection/cleanup "
                "outcomes are shown above; POST /jobs/{id}/resubmit to rerun"
            ),
        }
    raise HTTPException(404, f"Job {job_id} not found")


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


# --- S3-backed results (worker-direct push lands only in S3, not the Manager's
# local collected/ — read straight from S3 so download/UI still work). ---------

def _s3_bucket() -> str:
    return os.environ.get("ELASTIC_AGENT_RESULTS_S3_BUCKET", "")


def _s3_client():
    import boto3
    return boto3.client("s3")


def _s3_list_job(job_id: str) -> list[tuple[str, int, str]]:
    """Objects under s3://<bucket>/jobs/<job_id>/ → [(rel_path, size, key)].
    Empty when no bucket is configured or nothing is there."""
    bucket = _s3_bucket()
    if not bucket:
        return []
    prefix = f"jobs/{job_id}/"
    out: list[tuple[str, int, str]] = []
    try:
        s3 = _s3_client()
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                rel = obj["Key"][len(prefix):]
                if rel:
                    out.append((rel, obj["Size"], obj["Key"]))
    except Exception:
        pass
    return out


def _results_from_s3(job_id: str, objs: list[tuple[str, int, str]], *, parse_scores: bool) -> dict:
    bucket = _s3_bucket()
    files = [{"path": rel, "size": size} for rel, size, _ in objs]
    scores: list[dict] = []
    if parse_scores:
        s3 = _s3_client()
        parsed = 0
        for rel, size, key in objs:
            if parsed >= 500 or not rel.endswith(".json") or "instances" in rel.split("/"):
                continue
            if size > 2_000_000:  # skip large blobs (agent stdout dumps etc.)
                continue
            try:
                d = json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
            except Exception:
                continue
            parsed += 1
            if isinstance(d, dict) and "final_score" in d:
                scores.append({
                    "task_id": d.get("task_id"), "prompt_level": d.get("prompt_level"),
                    "status": d.get("status"), "final_score": d.get("final_score"),
                })
    return {"job_id": job_id, "file_count": len(files), "scores": scores,
            "s3_uri": f"s3://{bucket}/jobs/{job_id}/", "files": files}


@router.get("/results")
async def list_all_results() -> dict:
    """List every job's results — from S3 (authoritative once uploaded) with a
    local collected/ fallback for non-S3 deployments."""
    mgr = _mgr()
    jobs, seen = [], set()
    bucket = _s3_bucket()
    if bucket:  # list job prefixes cheaply (no per-file score parsing here)
        try:
            s3 = _s3_client()
            for page in s3.get_paginator("list_objects_v2").paginate(
                    Bucket=bucket, Prefix="jobs/", Delimiter="/"):
                for cp in page.get("CommonPrefixes", []):
                    jid = cp["Prefix"][len("jobs/"):].strip("/")
                    if jid and jid not in seen:
                        seen.add(jid)
                        r = _results_from_s3(jid, _s3_list_job(jid), parse_scores=False)
                        jobs.append({k: r[k] for k in ("job_id", "file_count", "scores", "s3_uri")})
        except Exception:
            pass
    root = Path(mgr.collected_root)
    if root.is_dir():
        for d in sorted(root.iterdir()):
            if d.is_dir() and d.name not in seen:
                seen.add(d.name)
                r = _results_for(mgr, d.name, d)
                jobs.append({k: r[k] for k in ("job_id", "file_count", "scores", "s3_uri")})
    return {"jobs": jobs, "total": len(jobs)}


@router.get("/jobs/{job_id}/results")
async def job_results(job_id: str) -> dict:
    """List a job's result files + benchmark scores (S3 first, local fallback)."""
    objs = _s3_list_job(job_id)
    if objs:
        r = _results_from_s3(job_id, objs, parse_scores=True)
        r["files"] = r["files"][:500]
        return r
    base = _collected_dir(_mgr(), job_id)
    if not base.is_dir():
        raise HTTPException(404, f"no results for job {job_id}")
    r = _results_for(_mgr(), job_id, base)
    r["files"] = r["files"][:500]
    return r


@router.get("/jobs/{job_id}/results/download")
async def job_results_download(job_id: str) -> StreamingResponse:
    """Download a job's results as a .tar.gz (S3 first, local fallback)."""
    objs = _s3_list_job(job_id)
    buf = io.BytesIO()
    if objs:
        bucket = _s3_bucket()
        s3 = _s3_client()
        with tarfile.open(fileobj=buf, mode="w:gz") as tar:
            for rel, size, key in objs:
                try:
                    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
                except Exception:
                    continue
                info = tarfile.TarInfo(name=f"{job_id}/{rel}")
                info.size = len(body)
                tar.addfile(info, io.BytesIO(body))
    else:
        base = _collected_dir(_mgr(), job_id)
        if not base.is_dir():
            raise HTTPException(404, f"no results for job {job_id}")
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
