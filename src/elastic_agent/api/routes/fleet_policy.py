"""Authenticated runtime policy for future Elastic fleet instances."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, ConfigDict, Field

from elastic_agent.api.auth import require_api_key
from elastic_agent.core.fleet_policy import FleetRuntimePolicyError

router = APIRouter(
    tags=["fleet-policy"],
    dependencies=[Depends(require_api_key)],
)


def _manager():
    from elastic_agent.api.app import get_manager

    return get_manager()


class FleetPolicyUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    default_instance_type: str = Field(
        min_length=2,
        max_length=64,
        pattern=r"^[a-z0-9][a-z0-9.-]{0,62}[a-z0-9]$",
    )
    default_root_disk_gb: int = Field(ge=8, le=2048)
    max_instances: int = Field(ge=1, le=100)


@router.get("/fleet-policy")
async def get_fleet_policy() -> dict[str, Any]:
    return await _manager().get_fleet_runtime_policy()


@router.put("/fleet-policy")
async def update_fleet_policy(request: FleetPolicyUpdate) -> dict[str, Any]:
    try:
        return await _manager().update_fleet_runtime_policy(
            default_instance_type=request.default_instance_type,
            default_root_disk_gb=request.default_root_disk_gb,
            max_instances=request.max_instances,
        )
    except FleetRuntimePolicyError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error


__all__ = ["router"]
