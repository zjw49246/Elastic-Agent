"""Authenticated deployment health and route contract endpoint."""

from __future__ import annotations

import os
import time

from fastapi import APIRouter, Depends

from elastic_agent.api.auth import require_api_key

router = APIRouter(tags=["health"], dependencies=[Depends(require_api_key)])

_start_time = time.monotonic()
_ROUTES = [
    "GET /api/health",
    "POST /api/job-batches/plan",
    "POST /api/job-batches",
    "GET /api/job-batches/{id}",
    "GET /api/jobs/{id}",
    "GET /api/jobs/{id}/logs",
    "GET /api/jobs/{id}/results",
    "POST /api/jobs/{id}/cancel",
    "POST /api/jobs/{id}/interrupt",
    "POST /api/jobs/{id}/resume",
    "POST /api/accounts",
    "GET /api/accounts",
    "GET /api/accounts/{id}",
]
_IDEMPOTENCY_ROUTES = [
    "POST /api/accounts",
    "POST /api/job-batches",
    "POST /api/jobs/{id}/cancel",
    "POST /api/jobs/{id}/interrupt",
    "POST /api/jobs/{id}/resume",
]


@router.get("/api/health")
async def health() -> dict:
    from elastic_agent.api.app import get_manager

    mgr = get_manager()
    evidence = getattr(mgr, "release_evidence", None)
    if not getattr(mgr, "_started", False) or not isinstance(evidence, dict):
        # A process that has not completed the fail-closed startup gate is not
        # healthy, even if FastAPI itself is accepting connections.
        from fastapi import HTTPException

        raise HTTPException(status_code=503, detail="release evidence unavailable")
    revision = os.environ.get("ELASTIC_AGENT_RELEASE_REVISION", "")
    account_id = os.environ.get("ELASTIC_AGENT_AWS_ACCOUNT_ID", "")
    region = os.environ.get("ELASTIC_AGENT_AWS_REGION", "")
    return {
        "status": "healthy",
        "healthy": True,
        "uptime_seconds": round(time.monotonic() - _start_time, 1),
        "worker_count": len(mgr.connection_manager.connected_workers),
        "provider": mgr.config.provider.type,
        "manager_state_schema": evidence["manager_state_schema"],
        "worker_profile_digest": evidence["worker_profile_digest"],
        "release_digest": evidence["release_digest"],
        "revision": revision,
        "aws_account_id": account_id,
        "region": region,
        "route_contract": {
            "authenticated": True,
            "network_scope": "private",
            "routes": _ROUTES,
            "idempotency_key_routes": _IDEMPOTENCY_ROUTES,
        },
    }
