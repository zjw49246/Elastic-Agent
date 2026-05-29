"""NodeRegistry — node state persistence with JSON file + thread-safe locking."""

from __future__ import annotations

import asyncio
import enum
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class NodeStatus(str, enum.Enum):
    CREATING = "creating"
    BOOTSTRAPPING = "bootstrapping"
    READY = "ready"
    BUSY = "busy"
    DRAINING = "draining"
    FAILED = "failed"
    UNHEALTHY = "unhealthy"
    STOPPED = "stopped"
    TERMINATED = "terminated"


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class NodeRecord(BaseModel):
    node_id: str
    instance_id: str
    platform: str
    status: NodeStatus = NodeStatus.CREATING
    public_ip: str | None = None
    private_ip: str | None = None
    auth_token: str | None = None
    created_at: datetime = Field(default_factory=_utcnow)
    bootstrap_started_at: datetime | None = None
    connected_at: datetime | None = None
    last_heartbeat: datetime | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class NodeRegistry:
    """Thread-safe, JSON-file-backed node state registry.

    All mutations are protected by an asyncio.Lock and flushed to disk
    immediately so that crash recovery can reload the last-written state.
    """

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path).expanduser()
        self._lock = asyncio.Lock()
        self._nodes: dict[str, NodeRecord] = {}
        self._loaded = False

    async def load(self) -> None:
        async with self._lock:
            self._load_sync()

    def _load_sync(self) -> None:
        if self._path.exists():
            try:
                raw = json.loads(self._path.read_text(encoding="utf-8"))
                nodes: dict[str, NodeRecord] = {}
                for nid, data in raw.get("nodes", {}).items():
                    nodes[nid] = NodeRecord.model_validate(data)
                self._nodes = nodes
                logger.info("NodeRegistry loaded %d nodes from %s", len(self._nodes), self._path)
            except Exception:
                logger.exception("Failed to load NodeRegistry from %s, starting empty", self._path)
                self._nodes = {}
        else:
            self._nodes = {}
        self._loaded = True

    def _flush_sync(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "nodes": {nid: json.loads(rec.model_dump_json()) for nid, rec in self._nodes.items()},
        }
        tmp = self._path.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
        tmp.replace(self._path)

    async def add(self, record: NodeRecord) -> None:
        async with self._lock:
            if not self._loaded:
                self._load_sync()
            self._nodes[record.node_id] = record
            self._flush_sync()
            logger.info("NodeRegistry: added node %s (status=%s)", record.node_id, record.status)

    async def get(self, node_id: str) -> NodeRecord | None:
        async with self._lock:
            if not self._loaded:
                self._load_sync()
            return self._nodes.get(node_id)

    async def update(self, node_id: str, **fields: Any) -> NodeRecord | None:
        async with self._lock:
            if not self._loaded:
                self._load_sync()
            rec = self._nodes.get(node_id)
            if rec is None:
                return None
            for k, v in fields.items():
                if hasattr(rec, k):
                    setattr(rec, k, v)
            self._flush_sync()
            logger.debug("NodeRegistry: updated node %s fields=%s", node_id, list(fields))
            return rec

    async def remove(self, node_id: str) -> NodeRecord | None:
        async with self._lock:
            if not self._loaded:
                self._load_sync()
            rec = self._nodes.pop(node_id, None)
            if rec is not None:
                self._flush_sync()
                logger.info("NodeRegistry: removed node %s", node_id)
            return rec

    async def list_all(self) -> list[NodeRecord]:
        async with self._lock:
            if not self._loaded:
                self._load_sync()
            return list(self._nodes.values())

    async def list_all_ids(self) -> set[str]:
        async with self._lock:
            if not self._loaded:
                self._load_sync()
            return set(self._nodes.keys())

    async def list_by_status(self, status: NodeStatus) -> list[NodeRecord]:
        async with self._lock:
            if not self._loaded:
                self._load_sync()
            return [r for r in self._nodes.values() if r.status == status]

    async def count(self) -> int:
        async with self._lock:
            if not self._loaded:
                self._load_sync()
            return len(self._nodes)
