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

import asyncio
import json
import logging
import mimetypes
import re
import threading
from collections.abc import AsyncIterator
from concurrent.futures import ThreadPoolExecutor
from pathlib import PurePosixPath
from typing import Any, Literal

from fastapi import APIRouter, Depends, HTTPException, Query, Response
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from elastic_agent.api.auth import require_api_key
from elastic_agent.worker.file_sync import (
    StorageObjectMetadataError,
    StorageObjectReader,
)

logger = logging.getLogger(__name__)

router = APIRouter(tags=["files"], dependencies=[Depends(require_api_key)])
_SAFE_TASK_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,255}")
_MAX_EXTERNAL_FILE_PATH_CHARS = 2_048
EXTERNAL_MANIFEST_MAX_BYTES = 8 * 1024 * 1024
EXTERNAL_MANIFEST_MAX_FILES = 10_000
EXTERNAL_CONTENT_MAX_BYTES = 2 * 1024 * 1024 * 1024
EXTERNAL_STREAM_CHUNK_BYTES = 256 * 1024
EXTERNAL_STREAM_MAX_CONCURRENCY = 4
EXTERNAL_PRESIGN_MAX_CONCURRENCY = 4
EXTERNAL_PRESIGN_TIMEOUT_SECONDS = 15.0
_MAX_MANIFEST_TEXT_CHARS = 2_048
_MAX_MANIFEST_SHORT_TEXT_CHARS = 256
_MAX_MANIFEST_OBJECT_SIZE = (1 << 63) - 1
_EXTERNAL_FILE_EXECUTOR = ThreadPoolExecutor(
    # Each admitted stream can own one blocking read. Reserve a second worker
    # per stream so cancellation cleanup can enter the executor instead of
    # deadlocking behind a pool made entirely of in-flight reads.
    max_workers=EXTERNAL_STREAM_MAX_CONCURRENCY * 2,
    thread_name_prefix="external-file",
)
_EXTERNAL_PRESIGN_EXECUTOR = ThreadPoolExecutor(
    max_workers=EXTERNAL_PRESIGN_MAX_CONCURRENCY,
    thread_name_prefix="external-presign",
)


class _ExternalStreamPermit:
    """Exactly-once lease for one Manager-proxied storage stream."""

    def __init__(self, admission: _ExternalStreamAdmission) -> None:
        self._admission = admission
        self._lock = threading.Lock()
        self._released = False

    def release(self) -> None:
        with self._lock:
            if self._released:
                return
            self._released = True
        self._admission._release()


class _ExternalStreamAdmission:
    """Thread-safe non-waiting admission without an unbounded waiter queue."""

    def __init__(self, limit: int) -> None:
        if limit <= 0:
            raise ValueError("external stream limit must be positive")
        self._limit = limit
        self._active = 0
        self._lock = threading.Lock()

    def try_acquire(self) -> _ExternalStreamPermit | None:
        with self._lock:
            if self._active >= self._limit:
                return None
            self._active += 1
        return _ExternalStreamPermit(self)

    def _release(self) -> None:
        with self._lock:
            if self._active <= 0:
                raise RuntimeError("external stream admission underflow")
            self._active -= 1

    @property
    def active(self) -> int:
        with self._lock:
            return self._active


_EXTERNAL_STREAM_ADMISSION = _ExternalStreamAdmission(
    EXTERNAL_STREAM_MAX_CONCURRENCY
)
_EXTERNAL_PRESIGN_ADMISSION = _ExternalStreamAdmission(
    EXTERNAL_PRESIGN_MAX_CONCURRENCY
)


def _mgr():
    from elastic_agent.api.app import get_manager
    return get_manager()


def _validate_task_id(task_id: str) -> str:
    if _SAFE_TASK_ID.fullmatch(task_id) is None:
        raise HTTPException(400, "invalid task_id")
    return task_id


def _validate_file_path(path: str) -> str:
    if (
        not path
        or len(path) > _MAX_EXTERNAL_FILE_PATH_CHARS
        or not path.isprintable()
        or "\\" in path
        or PurePosixPath(path).is_absolute()
        or any(part in {"", ".", ".."} for part in path.split("/"))
    ):
        raise HTTPException(400, "invalid file path")
    return path


class FileInfoResponse(BaseModel):
    task_id: str = Field(max_length=256)
    path: str = Field(max_length=_MAX_MANIFEST_TEXT_CHARS)
    oss_key: str = Field(max_length=_MAX_MANIFEST_TEXT_CHARS)
    size: int = Field(default=0, ge=0, le=_MAX_MANIFEST_OBJECT_SIZE)
    md5: str = Field(default="", max_length=_MAX_MANIFEST_SHORT_TEXT_CHARS)
    content_type: str = Field(
        default="application/octet-stream",
        max_length=_MAX_MANIFEST_SHORT_TEXT_CHARS,
    )
    role: str = Field(default="other", max_length=_MAX_MANIFEST_SHORT_TEXT_CHARS)
    synced_at: str = Field(
        default="",
        max_length=_MAX_MANIFEST_SHORT_TEXT_CHARS,
    )


class ManifestResponse(BaseModel):
    task_id: str = Field(max_length=256)
    worker_id: str = Field(max_length=_MAX_MANIFEST_SHORT_TEXT_CHARS)
    status: str = Field(default="idle", max_length=64)
    updated_at: str = Field(
        default="",
        max_length=_MAX_MANIFEST_SHORT_TEXT_CHARS,
    )
    files: list[FileInfoResponse] = Field(default_factory=list)


class ExternalStorageReadError(OSError):
    """A storage object changed or violated its declared stream metadata."""


class ExternalManifestError(ValueError):
    """A sync manifest is syntactically valid JSON but not bounded/schema-safe."""


def _acquire_external_stream() -> _ExternalStreamPermit:
    permit = _EXTERNAL_STREAM_ADMISSION.try_acquire()
    if permit is None:
        raise HTTPException(
            503,
            "external file stream capacity is currently exhausted",
            headers={"Retry-After": "1"},
        )
    return permit


async def _bounded_presigned_url(storage, oss_key: str) -> str | None:
    permit = _EXTERNAL_PRESIGN_ADMISSION.try_acquire()
    if permit is None:
        raise HTTPException(
            503,
            "external pre-signed URL capacity is currently exhausted",
            headers={"Retry-After": "1"},
        )
    try:
        task = asyncio.create_task(
            storage.get_presigned_url(
                oss_key,
                executor=_EXTERNAL_PRESIGN_EXECUTOR,
            )
        )
    except BaseException:
        permit.release()
        raise
    background_owned = False

    def finish_background(completed: asyncio.Task) -> None:
        try:
            completed.result()
        except BaseException:  # noqa: BLE001
            logger.warning(
                "Detached pre-signed URL operation failed",
                exc_info=True,
            )
        finally:
            permit.release()

    try:
        try:
            async with asyncio.timeout(EXTERNAL_PRESIGN_TIMEOUT_SECONDS):
                return await asyncio.shield(task)
        except TimeoutError as exc:
            background_owned = True
            task.add_done_callback(finish_background)
            raise HTTPException(
                504,
                "pre-signed URL generation exceeded its deadline",
            ) from exc
        except asyncio.CancelledError:
            background_owned = True
            task.add_done_callback(finish_background)
            raise
        except Exception as exc:
            logger.warning(
                "Failed to generate pre-signed URL for %s",
                oss_key,
                exc_info=True,
            )
            raise HTTPException(
                503, "storage backend cannot generate a pre-signed URL",
            ) from exc
    finally:
        if not background_owned:
            permit.release()


async def _close_reader(reader: StorageObjectReader) -> None:
    # Wait for actual SDK/file close before returning a stream permit. A single
    # shield is insufficient: it raises immediately when the request is
    # cancelled while leaving cleanup in the background.
    cleanup = asyncio.create_task(reader.close())
    cancellation: asyncio.CancelledError | None = None
    while not cleanup.done():
        try:
            await asyncio.shield(cleanup)
        except asyncio.CancelledError as exc:
            cancellation = exc
    try:
        cleanup.result()
    except BaseException:
        if cancellation is None:
            raise
        logger.warning(
            "Failed to close cancelled external storage body",
            exc_info=True,
        )
    if cancellation is not None:
        raise cancellation


async def _open_bounded_reader(
    storage,
    oss_key: str,
    *,
    max_bytes: int,
) -> StorageObjectReader:
    open_task = asyncio.create_task(
        storage.open_reader(
            oss_key,
            executor=_EXTERNAL_FILE_EXECUTOR,
        )
    )
    cancellation: asyncio.CancelledError | None = None
    try:
        while True:
            try:
                reader = await asyncio.shield(open_task)
                break
            except asyncio.CancelledError as exc:
                if open_task.cancelled():
                    raise
                # The executor GET cannot be cancelled. Retain request
                # ownership until it returns, then close any body it produced
                # before propagating cancellation and releasing admission.
                cancellation = exc
    except FileNotFoundError as exc:
        if cancellation is not None:
            raise cancellation from exc
        raise HTTPException(404, "storage object not found") from exc
    except (StorageObjectMetadataError, NotImplementedError) as exc:
        if cancellation is not None:
            raise cancellation from exc
        raise HTTPException(
            503, "storage backend cannot provide a bounded object stream",
        ) from exc
    except Exception as exc:
        if cancellation is not None:
            raise cancellation from exc
        logger.warning("Failed to open storage object %s", oss_key, exc_info=True)
        raise HTTPException(503, "storage backend is unavailable") from exc
    if cancellation is not None:
        try:
            await _close_reader(reader)
        except BaseException:  # cancellation remains the primary outcome
            logger.warning(
                "Failed to close storage body opened after request cancellation",
                exc_info=True,
            )
        raise cancellation
    if reader.size > max_bytes:
        await _close_reader(reader)
        raise HTTPException(
            413,
            f"storage object exceeds the {max_bytes}-byte response limit",
        )
    return reader


async def _iter_exact_object(
    reader: StorageObjectReader,
    *,
    owner: _ExternalFileStreamOwner | None = None,
) -> AsyncIterator[bytes]:
    remaining = reader.size
    try:
        while remaining:
            chunk = await reader.read(
                min(EXTERNAL_STREAM_CHUNK_BYTES, remaining)
            )
            if not chunk:
                raise ExternalStorageReadError(
                    "storage object became shorter than its declared length"
                )
            remaining -= len(chunk)
            yield chunk
        if await reader.read(1):
            raise ExternalStorageReadError(
                "storage object exceeded its declared length"
            )
    finally:
        if owner is None:
            await _close_reader(reader)
        else:
            await owner.close()


async def _read_bounded_object(
    storage,
    oss_key: str,
    *,
    max_bytes: int,
) -> bytes:
    reader = await _open_bounded_reader(
        storage, oss_key, max_bytes=max_bytes,
    )
    chunks: list[bytes] = []
    try:
        async for chunk in _iter_exact_object(reader):
            chunks.append(chunk)
    except ExternalStorageReadError as exc:
        raise HTTPException(
            503, "storage object changed while being read",
        ) from exc
    return b"".join(chunks)


def _manifest_text(
    value: Any,
    *,
    field: str,
    max_chars: int,
    default: str,
) -> str:
    if value is None:
        return default
    if (
        not isinstance(value, str)
        or len(value) > max_chars
        or (value != "" and not value.isprintable())
    ):
        raise ExternalManifestError(
            f"manifest field {field!r} is not a bounded printable string"
        )
    return value


def _parse_manifest(payload: bytes, *, task_id: str) -> ManifestResponse:
    try:
        raw = json.loads(payload)
    except (RecursionError, UnicodeDecodeError, TypeError, ValueError) as exc:
        raise ExternalManifestError("manifest is not valid bounded JSON") from exc
    if not isinstance(raw, dict):
        raise ExternalManifestError("manifest root must be an object")
    raw_task_id = _manifest_text(
        raw.get("task_id"),
        field="task_id",
        max_chars=256,
        default=task_id,
    )
    if raw_task_id != task_id:
        raise ExternalManifestError(
            "manifest task_id does not match the requested task"
        )
    raw_files = raw.get("files", [])
    if not isinstance(raw_files, list):
        raise ExternalManifestError("manifest files must be an array")
    if len(raw_files) > EXTERNAL_MANIFEST_MAX_FILES:
        raise HTTPException(
            413,
            f"manifest contains more than {EXTERNAL_MANIFEST_MAX_FILES} files",
        )

    files: list[FileInfoResponse] = []
    for index, item in enumerate(raw_files):
        if not isinstance(item, dict):
            raise ExternalManifestError(
                f"manifest files[{index}] must be an object"
            )
        size = item.get("size", 0)
        if (
            isinstance(size, bool)
            or not isinstance(size, int)
            or not 0 <= size <= _MAX_MANIFEST_OBJECT_SIZE
        ):
            raise ExternalManifestError(
                f"manifest files[{index}].size is invalid"
            )
        files.append(FileInfoResponse(
            task_id=task_id,
            path=_manifest_text(
                item.get("path"),
                field=f"files[{index}].path",
                max_chars=_MAX_MANIFEST_TEXT_CHARS,
                default="",
            ),
            oss_key=_manifest_text(
                item.get("oss_key"),
                field=f"files[{index}].oss_key",
                max_chars=_MAX_MANIFEST_TEXT_CHARS,
                default="",
            ),
            size=size,
            md5=_manifest_text(
                item.get("md5"),
                field=f"files[{index}].md5",
                max_chars=_MAX_MANIFEST_SHORT_TEXT_CHARS,
                default="",
            ),
            content_type=_manifest_text(
                item.get("content_type"),
                field=f"files[{index}].content_type",
                max_chars=_MAX_MANIFEST_SHORT_TEXT_CHARS,
                default="application/octet-stream",
            ),
            role=_manifest_text(
                item.get("role"),
                field=f"files[{index}].role",
                max_chars=_MAX_MANIFEST_SHORT_TEXT_CHARS,
                default="other",
            ),
            synced_at=_manifest_text(
                item.get("synced_at"),
                field=f"files[{index}].synced_at",
                max_chars=_MAX_MANIFEST_SHORT_TEXT_CHARS,
                default="",
            ),
        ))
    return ManifestResponse(
        task_id=task_id,
        worker_id=_manifest_text(
            raw.get("worker_id"),
            field="worker_id",
            max_chars=_MAX_MANIFEST_SHORT_TEXT_CHARS,
            default="",
        ),
        status=_manifest_text(
            raw.get("status"),
            field="status",
            max_chars=64,
            default="idle",
        ),
        updated_at=_manifest_text(
            raw.get("updated_at"),
            field="updated_at",
            max_chars=_MAX_MANIFEST_SHORT_TEXT_CHARS,
            default="",
        ),
        files=files,
    )


async def _load_manifest(storage, task_id: str) -> ManifestResponse:
    manifest_key = f"tasks/{task_id}/_sync_manifest.json"
    payload = await _read_bounded_object(
        storage,
        manifest_key,
        max_bytes=EXTERNAL_MANIFEST_MAX_BYTES,
    )
    try:
        return _parse_manifest(payload, task_id=task_id)
    except HTTPException:
        raise
    except ExternalManifestError as exc:
        raise HTTPException(503, f"invalid sync manifest: {exc}") from exc


class _ExternalFileStreamOwner:
    """Coordinate overlapping iterator/response cleanup as one durable task."""

    def __init__(
        self,
        reader: StorageObjectReader,
        permit: _ExternalStreamPermit,
    ) -> None:
        self.reader = reader
        self._permit = permit
        self._cleanup_task: asyncio.Task | None = None

    async def _close_and_release(self) -> None:
        try:
            await self.reader.close()
        finally:
            self._permit.release()

    @staticmethod
    def _consume_cleanup(task: asyncio.Task) -> None:
        if task.cancelled():
            return
        try:
            task.result()
        except BaseException:  # noqa: BLE001
            logger.warning(
                "Failed to close external storage response body",
                exc_info=True,
            )

    async def close(self) -> None:
        if self._cleanup_task is None:
            self._cleanup_task = asyncio.create_task(self._close_and_release())
            self._cleanup_task.add_done_callback(self._consume_cleanup)
        # If an ASGI response is cancelled, cleanup continues independently
        # and releases admission only after the blocking body close finishes.
        await asyncio.shield(self._cleanup_task)


class _ExternalFileResponse(StreamingResponse):
    """Streaming response that closes its backend body on every ASGI exit."""

    def __init__(
        self,
        owner: _ExternalFileStreamOwner,
        *,
        content_type: str,
        headers: dict[str, str],
    ) -> None:
        self._owner = owner
        super().__init__(
            _iter_exact_object(owner.reader, owner=owner),
            media_type=content_type,
            headers=headers,
        )

    async def __call__(self, scope, receive, send) -> None:
        try:
            await super().__call__(scope, receive, send)
        finally:
            await self._owner.close()


@router.get("/external/files/{task_id}/manifest")
async def get_manifest(task_id: str) -> ManifestResponse:
    """Read the sync manifest for a task from cloud storage."""
    task_id = _validate_task_id(task_id)
    mgr = _mgr()
    storage = mgr.file_storage
    if storage is None:
        raise HTTPException(503, "File storage not configured")

    permit = _acquire_external_stream()
    try:
        return await _load_manifest(storage, task_id)
    except HTTPException:
        raise
    except Exception as exc:
        logger.warning(
            "Failed to read manifest for task %s", task_id, exc_info=True,
        )
        raise HTTPException(503, "storage backend is unavailable") from exc
    finally:
        permit.release()


@router.get("/external/files/{task_id}/{path:path}")
async def read_file(
    task_id: str,
    path: str,
    mode: Literal["content", "url"] = Query(
        "content",
        description="'content' streams bytes, 'url' returns a pre-signed URL",
    ),
) -> Response:
    """Read a file from cloud storage for a given task.

    The file is identified by its relative path within the task's OSS prefix.
    Response includes X-Synced-At header with the last sync timestamp from the manifest.
    """
    task_id = _validate_task_id(task_id)
    path = _validate_file_path(path)
    mgr = _mgr()
    storage = mgr.file_storage
    if storage is None:
        raise HTTPException(503, "File storage not configured")

    oss_key = f"tasks/{task_id}/{path}"
    synced_at = ""

    if mode == "url":
        # A pre-signed URL transfers bytes directly from OSS/S3 to the caller;
        # it intentionally does not apply the Manager-protection content cap.
        # Manifest metadata is optional. Read it only when stream admission is
        # immediately available; a saturated Manager can still issue the
        # direct-storage URL without opening another object body.
        metadata_permit = _EXTERNAL_STREAM_ADMISSION.try_acquire()
        if metadata_permit is not None:
            try:
                try:
                    manifest = await _load_manifest(storage, task_id)
                    for item in manifest.files:
                        if item.oss_key == oss_key:
                            synced_at = item.synced_at
                            break
                except HTTPException:
                    pass
            finally:
                metadata_permit.release()
        url = await _bounded_presigned_url(storage, oss_key)
        if url is None:
            raise HTTPException(501, "Pre-signed URLs not supported by this storage backend")
        return Response(
            content=json.dumps({"url": url, "synced_at": synced_at}),
            media_type="application/json",
            headers={
                "Cache-Control": "no-store",
                "X-Content-Type-Options": "nosniff",
                **({"X-Synced-At": synced_at} if synced_at else {}),
            },
        )

    permit = _acquire_external_stream()
    owner: _ExternalFileStreamOwner | None = None
    try:
        try:
            manifest = await _load_manifest(storage, task_id)
            for item in manifest.files:
                if item.oss_key == oss_key:
                    synced_at = item.synced_at
                    break
        except HTTPException:
            # Manifest metadata is optional for content delivery. It remains
            # strictly bounded and shares this response's stream admission.
            pass

        reader = await _open_bounded_reader(
            storage,
            oss_key,
            max_bytes=EXTERNAL_CONTENT_MAX_BYTES,
        )
        owner = _ExternalFileStreamOwner(reader, permit)
        content_type = (
            mimetypes.guess_type(path)[0] or "application/octet-stream"
        )
        headers = {
            "Cache-Control": "no-store",
            "Content-Length": str(reader.size),
            "X-Content-Type-Options": "nosniff",
        }
        if synced_at:
            headers["X-Synced-At"] = synced_at

        return _ExternalFileResponse(
            owner,
            content_type=content_type,
            headers=headers,
        )
    except BaseException:
        if owner is None:
            permit.release()
        else:
            await owner.close()
        raise
