"""Node management REST API.

T-016: CRUD endpoints for managing Worker nodes.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from elastic_agent.api.auth import require_api_key
from elastic_agent.core.registry import NodeStatus

router = APIRouter(tags=["nodes"], dependencies=[Depends(require_api_key)])


def _mgr():
    from elastic_agent.api.app import get_manager
    return get_manager()


# ------------------------------------------------------------------
# Request / response models
# ------------------------------------------------------------------


class ScaleOutRequest(BaseModel):
    count: int = Field(default=1, ge=1, le=50)
    instance_type: str | None = None
    region: str | None = None


class ScaleOutResponse(BaseModel):
    nodes: list[dict[str, Any]]


class ScaleInRequest(BaseModel):
    node_ids: list[str]
    force: bool = False


class ScaleInResponse(BaseModel):
    terminated: list[str]
    status: str = "in_progress"


class NodeResponse(BaseModel):
    node_id: str
    instance_id: str
    platform: str
    status: str
    public_ip: str | None = None
    private_ip: str | None = None
    ws_connected: bool = False
    created_at: str | None = None
    last_heartbeat: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class NodeListResponse(BaseModel):
    nodes: list[NodeResponse]
    total: int


# ------------------------------------------------------------------
# Endpoints
# ------------------------------------------------------------------


def _node_to_response(node, connected: bool) -> NodeResponse:
    return NodeResponse(
        node_id=node.node_id,
        instance_id=node.instance_id,
        platform=node.platform,
        status=node.status.value,
        public_ip=node.public_ip,
        private_ip=node.private_ip,
        ws_connected=connected,
        created_at=node.created_at.isoformat() if node.created_at else None,
        last_heartbeat=node.last_heartbeat.isoformat() if node.last_heartbeat else None,
        metadata=node.metadata,
    )


@router.get("/nodes", response_model=NodeListResponse)
async def list_nodes(
    status: str | None = Query(None, description="Filter by node status"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
) -> NodeListResponse:
    mgr = _mgr()
    if status:
        try:
            ns = NodeStatus(status)
        except ValueError:
            raise HTTPException(400, f"Invalid status: {status}")
        nodes = await mgr.registry.list_by_status(ns)
    else:
        nodes = await mgr.registry.list_all()
    total = len(nodes)
    page = nodes[offset: offset + limit]
    return NodeListResponse(
        nodes=[_node_to_response(n, mgr.connection_manager.is_connected(n.node_id)) for n in page],
        total=total,
    )


@router.get("/nodes/{node_id}", response_model=NodeResponse)
async def get_node(node_id: str) -> NodeResponse:
    mgr = _mgr()
    node = await mgr.registry.get(node_id)
    if node is None:
        raise HTTPException(404, f"Node {node_id} not found")
    return _node_to_response(node, mgr.connection_manager.is_connected(node_id))


@router.get("/nodes/{node_id}/status")
async def get_node_status(node_id: str) -> dict:
    mgr = _mgr()
    info = await mgr.get_node_status(node_id)
    if info is None:
        raise HTTPException(404, f"Node {node_id} not found")
    return info


@router.post("/scale-out", response_model=ScaleOutResponse)
async def scale_out(req: ScaleOutRequest) -> ScaleOutResponse:
    mgr = _mgr()
    records = await mgr.scale_out(count=req.count, instance_type=req.instance_type, region=req.region)
    return ScaleOutResponse(
        nodes=[
            {
                "node_id": r.node_id,
                "instance_id": r.instance_id,
                "status": r.status.value,
                "public_ip": r.public_ip,
            }
            for r in records
        ]
    )


@router.post("/scale-in", response_model=ScaleInResponse)
async def scale_in(req: ScaleInRequest) -> ScaleInResponse:
    mgr = _mgr()
    terminated = await mgr.scale_in(node_ids=req.node_ids, force=req.force)
    return ScaleInResponse(terminated=terminated)


@router.post("/nodes/{node_id}/drain")
async def drain_node(node_id: str) -> dict:
    mgr = _mgr()
    ok = await mgr.drain_node(node_id)
    if not ok:
        raise HTTPException(404, f"Node {node_id} not found")
    return {"node_id": node_id, "status": "draining"}


@router.delete("/nodes/{node_id}")
async def remove_node(node_id: str) -> dict:
    """T-028: Remove a node from the registry. Terminates instance if still running."""
    mgr = _mgr()
    ok = await mgr.remove_node(node_id)
    if not ok:
        raise HTTPException(404, f"Node {node_id} not found")
    return {"node_id": node_id, "status": "removed"}
