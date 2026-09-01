"""Authenticated deployment health and route contract endpoint."""

from __future__ import annotations

import os
import time

from fastapi import APIRouter, Depends, HTTPException, Request

from elastic_agent.api.auth import require_api_key


async def _require_production_health_auth(request: Request) -> None:
    if os.environ.get("ELASTIC_AGENT_RELEASE_MANIFEST", "").strip():
        await require_api_key(request)


router = APIRouter(
    tags=["health"],
    dependencies=[Depends(_require_production_health_auth)],
)

_start_time = time.monotonic()


@router.get("/api/health")
async def health() -> dict:
    from elastic_agent.api.app import get_manager

    mgr = get_manager()
    production_gate = bool(os.environ.get("ELASTIC_AGENT_RELEASE_MANIFEST", "").strip())
    evidence = getattr(mgr, "release_evidence", None)
    if production_gate and not isinstance(evidence, dict):
        raise HTTPException(status_code=503, detail="release evidence unavailable")
    worker_count = len(mgr.connection_manager.connected_workers)
    payload = {
        "status": "healthy",
        "healthy": True,
        "uptime_seconds": round(time.monotonic() - _start_time, 1),
        "worker_count": worker_count,
        "provider": mgr.config.provider.type,
    }
    if isinstance(evidence, dict):
        payload.update(
            {
                "manager_state_schema": evidence["manager_state_schema"],
                "worker_profile_digest": evidence["worker_profile_digest"],
                "worker_runtime_provenance_digest": evidence.get(
                    "worker_runtime_provenance_digest",
                    evidence["worker_profile_digest"],
                ),
                "release_digest": evidence["release_digest"],
                "revision": os.environ.get("ELASTIC_AGENT_RELEASE_REVISION", ""),
                "aws_account_id": os.environ.get("ELASTIC_AGENT_AWS_ACCOUNT_ID", ""),
                "region": os.environ.get("ELASTIC_AGENT_AWS_REGION", ""),
            }
        )
    return payload
