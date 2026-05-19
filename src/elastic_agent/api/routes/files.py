"""External service API — file content read from cloud storage.

T-033: GET /api/external/files/{task_id}/{path} reads files from OSS/S3,
not from the Worker. Includes synced_at metadata from the sync manifest.

Data flow:
  External request → Manager → OSS/S3 read → Response with synced_at metadata

Supports two modes:
  - Direct content: returns file bytes with appropriate Content-Type
  - Pre-signed URL: returns a redirect URL for direct download (if supported)
"""

from __future__ import annotations

import json
import logging
import mimetypes
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Response
from pydantic import BaseModel, Field

from elastic_agent.api.auth import require_api_key

logger = logging.getLogger(__name__)

router = APIRouter(tags=["files"], dependencies=[Depends(require_api_key)])


def _mgr():
    from elastic_agent.api.app import get_manager
    return get_manager()


class FileInfoResponse(BaseModel):
    task_id: str
    path: str
    oss_key: str
    size: int = 0
    md5: str = ""
    content_type: str = "application/octet-stream"
    role: str = "other"
    synced_at: str = ""


class ManifestResponse(BaseModel):
    task_id: str
    worker_id: str
    status: str = "idle"
    updated_at: str = ""
    files: list[FileInfoResponse] = Field(default_factory=list)


@router.get("/external/files/{task_id}/manifest")
async def get_manifest(task_id: str) -> ManifestResponse:
    """Read the sync manifest for a task from cloud storage."""
    mgr = _mgr()
    storage = mgr.file_storage
    if storage is None:
        raise HTTPException(503, "File storage not configured")

    manifest_key = f"tasks/{task_id}/_sync_manifest.json"
    try:
        exists = await storage.file_exists(manifest_key)
        if not exists:
            raise HTTPException(404, f"No manifest found for task {task_id}")

        data = await storage.read_file(manifest_key)
        manifest = json.loads(data)
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to read manifest for task %s: %s", task_id, exc)
        raise HTTPException(502, f"Failed to read manifest from storage: {exc}")

    return ManifestResponse(
        task_id=manifest.get("task_id", task_id),
        worker_id=manifest.get("worker_id", ""),
        status=manifest.get("status", "idle"),
        updated_at=manifest.get("updated_at", ""),
        files=[
            FileInfoResponse(
                task_id=task_id,
                path=f.get("path", ""),
                oss_key=f.get("oss_key", ""),
                size=f.get("size", 0),
                md5=f.get("md5", ""),
                content_type=f.get("content_type", "application/octet-stream"),
                role=f.get("role", "other"),
                synced_at=f.get("synced_at", ""),
            )
            for f in manifest.get("files", [])
        ],
    )


@router.get("/external/files/{task_id}/{path:path}")
async def read_file(
    task_id: str,
    path: str,
    mode: str = Query("content", description="'content' returns raw bytes, 'url' returns pre-signed URL"),
) -> Response:
    """Read a file from cloud storage for a given task.

    The file is identified by its relative path within the task's OSS prefix.
    Response includes X-Synced-At header with the last sync timestamp from the manifest.
    """
    mgr = _mgr()
    storage = mgr.file_storage
    if storage is None:
        raise HTTPException(503, "File storage not configured")

    oss_key = f"tasks/{task_id}/{path}"
    synced_at = ""

    manifest_key = f"tasks/{task_id}/_sync_manifest.json"
    try:
        if await storage.file_exists(manifest_key):
            manifest_data = await storage.read_file(manifest_key)
            manifest = json.loads(manifest_data)
            for f in manifest.get("files", []):
                if f.get("oss_key", "").endswith(path) or f.get("oss_key") == oss_key:
                    synced_at = f.get("synced_at", "")
                    break
    except Exception:
        pass

    if mode == "url":
        url = await storage.get_presigned_url(oss_key)
        if url is None:
            raise HTTPException(501, "Pre-signed URLs not supported by this storage backend")
        return Response(
            content=json.dumps({"url": url, "synced_at": synced_at}),
            media_type="application/json",
            headers={"X-Synced-At": synced_at} if synced_at else {},
        )

    try:
        exists = await storage.file_exists(oss_key)
        if not exists:
            raise HTTPException(404, f"File not found: {path}")

        content = await storage.read_file(oss_key)
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to read file %s for task %s: %s", path, task_id, exc)
        raise HTTPException(502, f"Failed to read file from storage: {exc}")

    content_type = mimetypes.guess_type(path)[0] or "application/octet-stream"
    headers: dict[str, str] = {}
    if synced_at:
        headers["X-Synced-At"] = synced_at

    return Response(content=content, media_type=content_type, headers=headers)
