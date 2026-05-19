"""Health check endpoint."""

from __future__ import annotations

import time

from fastapi import APIRouter

router = APIRouter(tags=["health"])

_start_time = time.monotonic()


@router.get("/api/health")
async def health() -> dict:
    from elastic_agent.api.app import get_manager

    mgr = get_manager()
    worker_count = len(mgr.connection_manager.connected_workers)
    return {
        "status": "healthy",
        "uptime_seconds": round(time.monotonic() - _start_time, 1),
        "worker_count": worker_count,
    }
