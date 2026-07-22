"""FileSyncManager — Worker-side file sync to cloud storage (OSS/S3).

T-030: Core sync engine (watchdog monitoring, debounce tiers, upload, manifest)
T-031: Harness config integration (FileSyncConfig debounce/exclude from Harness)
T-032: Cloud storage credential injection (env-based OSS/S3 credentials)

Architecture:
  - watchdog Observer monitors registered watch paths (from REGISTER_SYNC_MAPPING)
  - File changes are debounced per-tier (state.json=0.5s, manuscript_*=2s, default=5s)
  - Uploads go to OSS (oss2) or S3 (boto3) based on configured storage type
  - _sync_manifest.json tracks all synced files per task
  - Failed uploads buffer locally and retry in background
"""

from __future__ import annotations

import asyncio
import fnmatch
import hashlib
import json
import logging
import mimetypes
import os
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

logger = logging.getLogger(__name__)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------


@dataclass
class SyncMappingEntry:
    task_id: str
    book_slug: str
    oss_prefix: str
    watch_paths: list[str]
    session_path_hash: str = ""
    registered_at: datetime = field(default_factory=_utcnow)


@dataclass
class SyncedFile:
    path: str
    oss_key: str
    size: int
    md5: str
    content_type: str
    role: str
    synced_at: str


@dataclass
class SyncManifest:
    task_id: str
    worker_id: str
    status: str = "idle"
    updated_at: str = ""
    files: list[SyncedFile] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "task_id": self.task_id,
            "worker_id": self.worker_id,
            "status": self.status,
            "updated_at": self.updated_at,
            "files": [
                {
                    "path": f.path,
                    "oss_key": f.oss_key,
                    "size": f.size,
                    "md5": f.md5,
                    "content_type": f.content_type,
                    "role": f.role,
                    "synced_at": f.synced_at,
                }
                for f in self.files
            ],
        }

    @classmethod
    def from_dict(cls, data: dict) -> SyncManifest:
        files = [
            SyncedFile(
                path=f["path"],
                oss_key=f["oss_key"],
                size=f["size"],
                md5=f["md5"],
                content_type=f.get("content_type", "application/octet-stream"),
                role=f.get("role", "other"),
                synced_at=f["synced_at"],
            )
            for f in data.get("files", [])
        ]
        return cls(
            task_id=data["task_id"],
            worker_id=data["worker_id"],
            status=data.get("status", "idle"),
            updated_at=data.get("updated_at", ""),
            files=files,
        )


@dataclass
class ArtifactScanResult:
    delivery_found: bool = False
    delivery_path: str | None = None
    manuscript_path: str | None = None
    searched_paths: list[str] = field(default_factory=list)


@dataclass
class PendingUpload:
    local_path: str
    oss_key: str
    task_id: str
    retry_count: int = 0
    queued_at: float = field(default_factory=time.monotonic)


# ---------------------------------------------------------------------------
# Storage backends
# ---------------------------------------------------------------------------


class StorageBackend:
    """Abstract interface for cloud storage uploads and reads."""

    async def upload_file(self, local_path: str, oss_key: str) -> None:
        raise NotImplementedError

    async def upload_bytes(self, data: bytes, oss_key: str, content_type: str = "application/octet-stream") -> None:
        raise NotImplementedError

    async def read_file(self, oss_key: str) -> bytes:
        """Read file content from storage. Returns raw bytes."""
        raise NotImplementedError

    async def file_exists(self, oss_key: str) -> bool:
        """Check if a file exists in storage."""
        raise NotImplementedError

    async def get_presigned_url(self, oss_key: str, expires: int = 3600) -> str | None:
        """Generate a pre-signed URL for direct download. Returns None if unsupported."""
        return None


class OSSBackend(StorageBackend):
    """Alibaba Cloud OSS storage backend."""

    def __init__(self, bucket_name: str, endpoint: str, access_key_id: str, access_key_secret: str) -> None:
        self._bucket_name = bucket_name
        self._endpoint = endpoint
        self._access_key_id = access_key_id
        self._access_key_secret = access_key_secret
        self._bucket: Any = None

    def _get_bucket(self):
        if self._bucket is None:
            import oss2

            auth = oss2.Auth(self._access_key_id, self._access_key_secret)
            self._bucket = oss2.Bucket(auth, self._endpoint, self._bucket_name)
        return self._bucket

    async def upload_file(self, local_path: str, oss_key: str) -> None:
        bucket = self._get_bucket()
        file_size = os.path.getsize(local_path)

        if file_size < 10 * 1024 * 1024:
            await asyncio.to_thread(bucket.put_object_from_file, oss_key, local_path)
        else:
            await self._multipart_upload(bucket, local_path, oss_key, file_size)

    async def _multipart_upload(self, bucket, local_path: str, oss_key: str, file_size: int) -> None:
        import oss2

        part_size = 5 * 1024 * 1024
        upload_id = (await asyncio.to_thread(bucket.init_multipart_upload, oss_key)).upload_id
        parts = []
        try:
            part_number = 1
            with open(local_path, "rb") as f:
                while True:
                    data = f.read(part_size)
                    if not data:
                        break
                    for attempt in range(PART_MAX_RETRIES):
                        try:
                            result = await asyncio.to_thread(
                                bucket.upload_part, oss_key, upload_id, part_number, data,
                            )
                            parts.append(oss2.models.PartInfo(part_number, result.etag))
                            break
                        except Exception:
                            if attempt < PART_MAX_RETRIES - 1:
                                await asyncio.sleep(2 ** attempt)
                            else:
                                raise
                    part_number += 1
            await asyncio.to_thread(bucket.complete_multipart_upload, oss_key, upload_id, parts)
        except Exception:
            await asyncio.to_thread(bucket.abort_multipart_upload, oss_key, upload_id)
            raise

    async def upload_bytes(self, data: bytes, oss_key: str, content_type: str = "application/octet-stream") -> None:
        bucket = self._get_bucket()
        import oss2

        headers = {"Content-Type": content_type}
        await asyncio.to_thread(bucket.put_object, oss_key, data, headers=headers)

    async def read_file(self, oss_key: str) -> bytes:
        bucket = self._get_bucket()
        result = await asyncio.to_thread(bucket.get_object, oss_key)
        return await asyncio.to_thread(result.read)

    async def file_exists(self, oss_key: str) -> bool:
        bucket = self._get_bucket()
        return await asyncio.to_thread(bucket.object_exists, oss_key)

    async def get_presigned_url(self, oss_key: str, expires: int = 3600) -> str | None:
        bucket = self._get_bucket()
        return await asyncio.to_thread(bucket.sign_url, "GET", oss_key, expires)


class S3Backend(StorageBackend):
    """AWS S3 storage backend."""

    def __init__(self, bucket_name: str, region: str | None = None) -> None:
        self._bucket_name = bucket_name
        self._region = region
        self._client: Any = None

    def _get_client(self):
        if self._client is None:
            import boto3

            self._client = boto3.client("s3", region_name=self._region)
        return self._client

    async def upload_file(self, local_path: str, oss_key: str) -> None:
        client = self._get_client()
        content_type = mimetypes.guess_type(local_path)[0] or "application/octet-stream"
        await asyncio.to_thread(
            client.upload_file,
            local_path,
            self._bucket_name,
            oss_key,
            ExtraArgs={"ContentType": content_type},
        )

    async def upload_bytes(self, data: bytes, oss_key: str, content_type: str = "application/octet-stream") -> None:
        client = self._get_client()
        await asyncio.to_thread(
            client.put_object,
            Bucket=self._bucket_name,
            Key=oss_key,
            Body=data,
            ContentType=content_type,
        )

    async def read_file(self, oss_key: str) -> bytes:
        client = self._get_client()
        resp = await asyncio.to_thread(client.get_object, Bucket=self._bucket_name, Key=oss_key)
        return await asyncio.to_thread(resp["Body"].read)

    async def file_exists(self, oss_key: str) -> bool:
        client = self._get_client()
        try:
            await asyncio.to_thread(client.head_object, Bucket=self._bucket_name, Key=oss_key)
            return True
        except Exception:
            return False

    async def get_presigned_url(self, oss_key: str, expires: int = 3600) -> str | None:
        client = self._get_client()
        return await asyncio.to_thread(
            client.generate_presigned_url,
            "get_object",
            Params={"Bucket": self._bucket_name, "Key": oss_key},
            ExpiresIn=expires,
        )


class LocalBackend(StorageBackend):
    """Local filesystem backend for testing."""

    def __init__(self, base_dir: str) -> None:
        self._base_dir = Path(base_dir)
        self._base_dir.mkdir(parents=True, exist_ok=True)

    async def upload_file(self, local_path: str, oss_key: str) -> None:
        import shutil

        dest = self._base_dir / oss_key
        dest.parent.mkdir(parents=True, exist_ok=True)
        await asyncio.to_thread(shutil.copy2, local_path, str(dest))

    async def upload_bytes(self, data: bytes, oss_key: str, content_type: str = "application/octet-stream") -> None:
        dest = self._base_dir / oss_key
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(data)

    async def read_file(self, oss_key: str) -> bytes:
        dest = self._base_dir / oss_key
        return await asyncio.to_thread(dest.read_bytes)

    async def file_exists(self, oss_key: str) -> bool:
        return (self._base_dir / oss_key).exists()

    async def get_presigned_url(self, oss_key: str, expires: int = 3600) -> str | None:
        return None


def create_storage_backend(storage_type: str | None = None, **kwargs) -> StorageBackend:
    """Factory for storage backends.

    Reads STORAGE_TYPE from env when storage_type is not explicitly provided
    (set by cloud_storage_credentials_step during Bootstrap). Falls back to "local".
    """
    if storage_type is None:
        storage_type = os.environ.get("STORAGE_TYPE", "local")
    if storage_type == "oss":
        return OSSBackend(
            bucket_name=kwargs.get("bucket_name", os.environ.get("OSS_BUCKET", "")),
            endpoint=kwargs.get("endpoint", os.environ.get("OSS_ENDPOINT", "")),
            access_key_id=kwargs.get("access_key_id", os.environ.get("OSS_ACCESS_KEY_ID", "")),
            access_key_secret=kwargs.get("access_key_secret", os.environ.get("OSS_ACCESS_KEY_SECRET", "")),
        )
    elif storage_type == "s3":
        return S3Backend(
            bucket_name=kwargs.get("bucket_name", os.environ.get("S3_BUCKET", "")),
            region=kwargs.get("region", os.environ.get("AWS_DEFAULT_REGION")),
        )
    else:
        return LocalBackend(base_dir=kwargs.get("base_dir", "/tmp/elastic-agent-sync"))


# ---------------------------------------------------------------------------
# FileSyncManager
# ---------------------------------------------------------------------------

CRITICAL_FILES = {"state.json", "manifest.json", "_sync_manifest.json"}
MAX_RETRIES = 3
PART_MAX_RETRIES = 3
RETRY_BUFFER_INTERVAL = 30.0
MAX_BUFFER_SIZE = 1000
MAX_BUFFER_BYTES = 500 * 1024 * 1024  # 500 MB
MULTIPART_THRESHOLD = 10 * 1024 * 1024  # 10 MB


def _file_md5(path: str) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def _is_delivery_manuscript_name(name: str) -> bool:
    lower = name.lower()
    return lower in {
        "audiobook_manuscript.md",
        "manuscript.md",
        "manuscript_compliant.md",
        "manuscript_final.md",
    }


def _guess_role(filename: str, debounce_tiers: dict[str, float]) -> str:
    normalized = filename.replace("\\", "/")
    name = os.path.basename(normalized).lower()
    path_parts = normalized.lower().split("/")
    path_part_set = set(path_parts)
    delivery_index = path_parts.index("delivery") if "delivery" in path_part_set else -1
    is_delivery_root_file = delivery_index >= 0 and delivery_index == len(path_parts) - 2
    if is_delivery_root_file:
        if name in ("intro.md", "audiobook_intro.md", "intro_final.md"):
            return "delivery_intro"
        if name in ("delivery.zip", "audiobook_delivery.zip") or name.endswith(".zip"):
            return "delivery_export"
        if _is_delivery_manuscript_name(name):
            return "delivery_manuscript"
    if name in ("state.json",):
        return "state"
    if name in ("manifest.json", "_sync_manifest.json"):
        return "metadata"
    if name.endswith((".mp3", ".wav", ".flac", ".m4a")):
        return "audio"
    if name.startswith("manuscript") or name.endswith(".md"):
        return "manuscript"
    return "other"


def _match_debounce(filename: str, debounce_tiers: dict[str, float], default: float = 5.0) -> float:
    name = os.path.basename(filename)
    for pattern, delay in debounce_tiers.items():
        if fnmatch.fnmatch(name, pattern):
            return delay
    return default


def _is_critical(filename: str) -> bool:
    return os.path.basename(filename) in CRITICAL_FILES


def _should_exclude(path: str, exclude_patterns: list[str]) -> bool:
    name = os.path.basename(path)
    for pattern in exclude_patterns:
        if fnmatch.fnmatch(name, pattern) or fnmatch.fnmatch(path, pattern):
            return True
    return False


class FileSyncManager:
    """Worker-side file sync engine.

    Monitors registered watch paths, debounces changes, and uploads to cloud storage.
    Maintains per-task _sync_manifest.json for tracking synced files.
    """

    def __init__(
        self,
        worker_id: str,
        storage: StorageBackend,
        debounce_tiers: dict[str, float] | None = None,
        exclude_patterns: list[str] | None = None,
        on_file_synced: Callable | None = None,
        on_file_changed: Callable | None = None,
        on_sync_error: Callable | None = None,
        buffer_path: str | None = None,
    ) -> None:
        self._worker_id = worker_id
        self._storage = storage
        self._debounce_tiers = debounce_tiers or {}
        self._exclude_patterns = exclude_patterns or ["*.tmp", "*.swp", "__pycache__/", ".git/"]
        self._on_file_synced = on_file_synced
        self._on_file_changed = on_file_changed
        self._on_sync_error = on_sync_error

        self._mappings: dict[str, SyncMappingEntry] = {}
        self._manifests: dict[str, SyncManifest] = {}
        self._debounce_timers: dict[str, asyncio.TimerHandle | None] = {}
        self._pending_buffer: list[PendingUpload] = []
        self._buffer_bytes: int = 0
        self._buffer_path = Path(buffer_path) if buffer_path else None

        self._observer: Any = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._running = False
        self._retry_task: asyncio.Task | None = None
        self._lock = asyncio.Lock()

    @property
    def active_mappings(self) -> dict[str, SyncMappingEntry]:
        return dict(self._mappings)

    @property
    def pending_count(self) -> int:
        return len(self._pending_buffer)

    async def start(self) -> None:
        self._loop = asyncio.get_running_loop()
        self._running = True
        self._load_buffer()
        self._retry_task = asyncio.create_task(self._retry_loop())
        logger.info("FileSyncManager started (worker=%s)", self._worker_id)

    async def stop(self) -> None:
        self._running = False
        if self._retry_task:
            self._retry_task.cancel()
            try:
                await self._retry_task
            except asyncio.CancelledError:
                pass
        self._stop_observer()
        self._save_buffer()
        logger.info("FileSyncManager stopped")

    def register_mapping(self, mapping: SyncMappingEntry) -> None:
        task_id = mapping.task_id
        self._mappings[task_id] = mapping
        self._manifests[task_id] = SyncManifest(
            task_id=task_id,
            worker_id=self._worker_id,
            status="idle",
            updated_at=_utcnow().isoformat(),
        )
        self._start_watching(mapping.watch_paths)
        logger.info("Registered sync mapping for task %s (%d watch paths)", task_id, len(mapping.watch_paths))

    async def unregister_mapping(self, task_id: str) -> None:
        mapping = self._mappings.pop(task_id, None)
        if mapping is None:
            return

        await self._final_sync(task_id, mapping)

        self._manifests.pop(task_id, None)

        for key in list(self._debounce_timers):
            if key.startswith(f"{task_id}:"):
                handle = self._debounce_timers.pop(key)
                if handle:
                    handle.cancel()

        self._update_observer()
        logger.info("Unregistered sync mapping for task %s", task_id)

    async def force_sync(self, task_id: str) -> int:
        """Force immediate sync of all files for a task. Returns count of files synced."""
        mapping = self._mappings.get(task_id)
        if mapping is None:
            return 0
        return await self._sync_all_files(task_id, mapping)

    async def force_sync_mapping(self, mapping: SyncMappingEntry, *, transient: bool = False) -> int:
        """Force sync using a supplied mapping, optionally without keeping it active."""
        task_id = mapping.task_id
        had_mapping = task_id in self._mappings
        had_manifest = task_id in self._manifests

        if not had_mapping and not transient:
            self.register_mapping(mapping)
            return await self.force_sync(task_id)

        if not had_mapping:
            self._mappings[task_id] = mapping
        if not had_manifest:
            self._manifests[task_id] = SyncManifest(
                task_id=task_id,
                worker_id=self._worker_id,
                status="idle",
                updated_at=_utcnow().isoformat(),
            )

        try:
            return await self._sync_all_files(task_id, mapping)
        finally:
            if transient and not had_mapping:
                self._mappings.pop(task_id, None)
            if transient and not had_manifest:
                self._manifests.pop(task_id, None)

    def scan_task_artifacts(
        self,
        task_id: str,
        *,
        mapping: SyncMappingEntry | None = None,
        book_slug: str | None = None,
        cwd: str | None = None,
        watch_paths: list[str] | None = None,
    ) -> ArtifactScanResult:
        """Search likely task workspace roots for a delivery directory."""
        active_mapping = mapping or self._mappings.get(task_id)
        roots: list[str] = []
        if watch_paths:
            roots.extend(watch_paths)
        if active_mapping:
            roots.extend(active_mapping.watch_paths)
            book_slug = book_slug or active_mapping.book_slug
        if cwd:
            roots.append(cwd)
            if book_slug:
                roots.append(os.path.join(cwd, ".work", book_slug))
        if book_slug:
            roots.append(f"/root/books/{book_slug}")
            roots.append(f"/root/books/{book_slug}/.work/{book_slug}")
            roots.append(f"/root/.work/{book_slug}")

        normalized_roots: list[str] = []
        seen: set[str] = set()
        for root in roots:
            root_norm = os.path.abspath(root.rstrip("/"))
            if root_norm and root_norm not in seen:
                seen.add(root_norm)
                normalized_roots.append(root_norm)

        delivery_dirs: list[Path] = []
        searched: list[str] = []
        for root in normalized_roots:
            root_path = Path(root)
            searched.append(str(root_path))
            try:
                is_dir = root_path.is_dir()
            except OSError:
                continue
            if not is_dir:
                continue
            if root_path.name == "delivery":
                delivery_dirs.append(root_path)
                continue
            for current_root, dirs, _files in os.walk(str(root_path)):
                for dirname in dirs:
                    if dirname == "delivery":
                        delivery_dirs.append(Path(current_root) / dirname)

        def _delivery_score(path: Path) -> tuple[int, float]:
            try:
                children = list(path.iterdir())
            except OSError:
                children = []
            has_manuscript = any(item.is_file() and _is_delivery_manuscript_name(item.name) for item in children)
            try:
                mtime = path.stat().st_mtime
            except OSError:
                mtime = 0.0
            return (1 if has_manuscript else 0, mtime)

        if not delivery_dirs:
            return ArtifactScanResult(searched_paths=searched)

        delivery_path = sorted(delivery_dirs, key=_delivery_score, reverse=True)[0]
        manuscript_path = None
        try:
            delivery_children = sorted(delivery_path.iterdir())
        except OSError:
            delivery_children = []
        for candidate in delivery_children:
            if candidate.is_file() and _is_delivery_manuscript_name(candidate.name):
                manuscript_path = str(candidate)
                break

        return ArtifactScanResult(
            delivery_found=True,
            delivery_path=str(delivery_path),
            manuscript_path=manuscript_path,
            searched_paths=searched,
        )

    def on_file_event(self, path: str, event_type: str) -> None:
        """Called by watchdog handler (from thread) when a file changes."""
        if not self._running or not self._loop:
            return

        if _should_exclude(path, self._exclude_patterns):
            return

        matched = self._match_path_to_task(path)
        if matched is None:
            return

        task_id, mapping = matched

        if self._on_file_changed:
            try:
                self._loop.call_soon_threadsafe(
                    asyncio.ensure_future,
                    self._on_file_changed(task_id, path, event_type),
                )
            except RuntimeError:
                pass

        debounce_key = f"{task_id}:{path}"
        debounce_delay = _match_debounce(path, self._debounce_tiers)

        existing = self._debounce_timers.get(debounce_key)
        if existing:
            existing.cancel()

        if _is_critical(path):
            self._loop.call_soon_threadsafe(
                asyncio.ensure_future,
                self._upload_file(task_id, mapping, path),
            )
        else:
            handle = self._loop.call_later(
                debounce_delay,
                lambda t=task_id, m=mapping, p=path: asyncio.ensure_future(self._upload_file(t, m, p)),
            )
            self._debounce_timers[debounce_key] = handle

    # ------------------------------------------------------------------
    # Internal: upload
    # ------------------------------------------------------------------

    async def _upload_file(self, task_id: str, mapping: SyncMappingEntry, local_path: str) -> None:
        if not os.path.exists(local_path):
            return

        rel_path = self._compute_relative_path(local_path, mapping.watch_paths)
        if rel_path is None:
            return

        oss_key = f"{mapping.oss_prefix.rstrip('/')}/{rel_path}"

        for attempt in range(MAX_RETRIES):
            try:
                async with self._lock:
                    manifest = self._manifests.get(task_id)
                    if manifest:
                        manifest.status = "syncing"

                await self._storage.upload_file(local_path, oss_key)

                md5 = _file_md5(local_path)
                size = os.path.getsize(local_path)
                content_type = mimetypes.guess_type(local_path)[0] or "application/octet-stream"
                role = _guess_role(rel_path, self._debounce_tiers)
                now = _utcnow()

                async with self._lock:
                    self._update_manifest(task_id, local_path, oss_key, size, md5, content_type, role, now)
                    await self._upload_manifest(task_id, mapping)

                if self._on_file_synced:
                    await self._on_file_synced(task_id, local_path, oss_key, now.isoformat(), md5)

                return
            except Exception as exc:
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(2 ** attempt)
                    continue
                logger.warning("Upload failed after %d retries: %s → %s: %s", MAX_RETRIES, local_path, oss_key, exc)
                if _is_critical(local_path):
                    logger.error("CRITICAL file upload failed: %s", local_path)
                    if self._on_sync_error:
                        try:
                            await self._on_sync_error(task_id, local_path, oss_key, str(exc), critical=True)
                        except Exception:
                            pass
                else:
                    self._buffer_failed_upload(local_path, oss_key, task_id)
                    if self._on_sync_error:
                        try:
                            await self._on_sync_error(task_id, local_path, oss_key, str(exc), critical=False)
                        except Exception:
                            pass

    def _update_manifest(
        self,
        task_id: str,
        local_path: str,
        oss_key: str,
        size: int,
        md5: str,
        content_type: str,
        role: str,
        synced_at: datetime,
    ) -> None:
        manifest = self._manifests.get(task_id)
        if manifest is None:
            return

        existing = next((f for f in manifest.files if f.path == local_path), None)
        entry = SyncedFile(
            path=local_path,
            oss_key=oss_key,
            size=size,
            md5=md5,
            content_type=content_type,
            role=role,
            synced_at=synced_at.isoformat(),
        )
        if existing:
            idx = manifest.files.index(existing)
            manifest.files[idx] = entry
        else:
            manifest.files.append(entry)
        manifest.status = "idle"
        manifest.updated_at = synced_at.isoformat()

    async def _upload_manifest(self, task_id: str, mapping: SyncMappingEntry) -> None:
        manifest = self._manifests.get(task_id)
        if manifest is None:
            return
        oss_key = f"{mapping.oss_prefix.rstrip('/')}/_sync_manifest.json"
        data = json.dumps(manifest.to_dict(), indent=2).encode("utf-8")
        try:
            await self._storage.upload_bytes(data, oss_key, content_type="application/json")
        except Exception as exc:
            logger.warning("Failed to upload manifest for task %s: %s", task_id, exc)

    # ------------------------------------------------------------------
    # Internal: final sync, sync all
    # ------------------------------------------------------------------

    async def _final_sync(self, task_id: str, mapping: SyncMappingEntry) -> None:
        await self._sync_all_files(task_id, mapping)

    async def _sync_all_files(self, task_id: str, mapping: SyncMappingEntry) -> int:
        count = 0
        for watch_path in mapping.watch_paths:
            wp = Path(watch_path)
            if not wp.exists():
                continue
            if wp.is_file():
                if not _should_exclude(str(wp), self._exclude_patterns):
                    await self._upload_file(task_id, mapping, str(wp))
                    count += 1
            else:
                for root, _dirs, files in os.walk(str(wp)):
                    for fname in files:
                        fpath = os.path.join(root, fname)
                        if not _should_exclude(fpath, self._exclude_patterns):
                            await self._upload_file(task_id, mapping, fpath)
                            count += 1
        return count

    # ------------------------------------------------------------------
    # Internal: path matching
    # ------------------------------------------------------------------

    def _match_path_to_task(self, path: str) -> tuple[str, SyncMappingEntry] | None:
        for task_id, mapping in self._mappings.items():
            for wp in mapping.watch_paths:
                if path.startswith(wp.rstrip("/") + "/") or path == wp:
                    return (task_id, mapping)
        return None

    @staticmethod
    def _compute_relative_path(local_path: str, watch_paths: list[str]) -> str | None:
        for wp in watch_paths:
            wp_norm = wp.rstrip("/")
            if local_path.startswith(wp_norm + "/"):
                return local_path[len(wp_norm) + 1:]
            if local_path == wp_norm:
                return os.path.basename(local_path)
        return None

    # ------------------------------------------------------------------
    # Internal: watchdog observer
    # ------------------------------------------------------------------

    def _start_watching(self, paths: list[str]) -> None:
        try:
            from watchdog.observers import Observer
            from watchdog.events import FileSystemEventHandler, FileSystemEvent

            if self._observer is None:
                self._observer = Observer()
                self._observer.daemon = True
                self._observer.start()

            handler = _WatchdogHandler(self)
            for path in paths:
                p = Path(path)
                if p.exists() and p.is_dir():
                    self._observer.schedule(handler, str(p), recursive=True)
                elif p.parent.exists():
                    self._observer.schedule(handler, str(p.parent), recursive=False)
        except ImportError:
            logger.warning("watchdog not installed — file sync will only work via force_sync()")

    def _stop_observer(self) -> None:
        if self._observer:
            self._observer.stop()
            self._observer.join(timeout=5)
            self._observer = None

    def _update_observer(self) -> None:
        self._stop_observer()
        all_paths: list[str] = []
        for mapping in self._mappings.values():
            all_paths.extend(mapping.watch_paths)
        if all_paths:
            self._start_watching(all_paths)

    # ------------------------------------------------------------------
    # Internal: retry buffer
    # ------------------------------------------------------------------

    def _buffer_failed_upload(self, local_path: str, oss_key: str, task_id: str) -> None:
        file_size = 0
        try:
            file_size = os.path.getsize(local_path)
        except OSError:
            pass

        if len(self._pending_buffer) >= MAX_BUFFER_SIZE or self._buffer_bytes + file_size > MAX_BUFFER_BYTES:
            logger.error(
                "Upload buffer limit reached (items=%d/%d, bytes=%d/%d), dropping oldest",
                len(self._pending_buffer), MAX_BUFFER_SIZE, self._buffer_bytes, MAX_BUFFER_BYTES,
            )
            dropped = self._pending_buffer.pop(0)
            try:
                self._buffer_bytes -= os.path.getsize(dropped.local_path)
            except OSError:
                pass

        self._pending_buffer.append(PendingUpload(local_path=local_path, oss_key=oss_key, task_id=task_id))
        self._buffer_bytes += file_size
        self._save_buffer()

    async def _retry_loop(self) -> None:
        while self._running:
            await asyncio.sleep(RETRY_BUFFER_INTERVAL)
            if not self._pending_buffer:
                continue

            remaining: list[PendingUpload] = []
            new_bytes = 0
            for item in self._pending_buffer:
                if not os.path.exists(item.local_path):
                    continue
                try:
                    await self._storage.upload_file(item.local_path, item.oss_key)
                    logger.info("Retry upload succeeded: %s", item.oss_key)
                except Exception:
                    item.retry_count += 1
                    if item.retry_count < 10:
                        remaining.append(item)
                        try:
                            new_bytes += os.path.getsize(item.local_path)
                        except OSError:
                            pass
                    else:
                        logger.error("Giving up on upload after 10 retries: %s", item.oss_key)
            self._pending_buffer = remaining
            self._buffer_bytes = new_bytes
            self._save_buffer()

    def _save_buffer(self) -> None:
        if self._buffer_path is None:
            return
        try:
            self._buffer_path.parent.mkdir(parents=True, exist_ok=True)
            data = [
                {"local_path": p.local_path, "oss_key": p.oss_key, "task_id": p.task_id, "retry_count": p.retry_count}
                for p in self._pending_buffer
            ]
            self._buffer_path.write_text(json.dumps(data))
        except Exception:
            pass

    def _load_buffer(self) -> None:
        if self._buffer_path is None or not self._buffer_path.exists():
            return
        try:
            data = json.loads(self._buffer_path.read_text())
            self._pending_buffer = [
                PendingUpload(
                    local_path=item["local_path"],
                    oss_key=item["oss_key"],
                    task_id=item["task_id"],
                    retry_count=item.get("retry_count", 0),
                )
                for item in data
            ]
            self._buffer_bytes = sum(
                os.path.getsize(p.local_path) for p in self._pending_buffer
                if os.path.exists(p.local_path)
            )
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Watchdog handler bridge
# ---------------------------------------------------------------------------

class _WatchdogHandler:
    """Bridges watchdog's thread-based events to FileSyncManager's async handlers."""

    def __init__(self, manager: FileSyncManager) -> None:
        self._manager = manager

    def dispatch(self, event) -> None:
        if event.is_directory:
            return
        event_type = event.event_type
        if event_type in ("created", "modified", "moved"):
            path = event.dest_path if event_type == "moved" else event.src_path
            self._manager.on_file_event(path, event_type)


# ---------------------------------------------------------------------------
# Bootstrap step for cloud storage credential injection (T-032)
# ---------------------------------------------------------------------------


def cloud_storage_credentials_step(
    storage_type: str = "oss",
    oss_access_key_id: str = "",
    oss_access_key_secret: str = "",
    oss_bucket: str = "",
    oss_endpoint: str = "",
    s3_bucket: str = "",
    s3_region: str = "",
    timeout: int = 60,
) -> "BootstrapStep":
    """T-032: Bootstrap step to inject cloud storage credentials on Worker.

    Writes environment variables to /etc/elastic-agent/storage.env so the
    Worker Runtime's FileSyncManager can authenticate with OSS/S3.
    """
    from elastic_agent.harness.base import BootstrapStep

    env_lines = []
    if storage_type == "oss":
        env_lines.extend([
            f"OSS_ACCESS_KEY_ID={oss_access_key_id}",
            f"OSS_ACCESS_KEY_SECRET={oss_access_key_secret}",
            f"OSS_BUCKET={oss_bucket}",
            f"OSS_ENDPOINT={oss_endpoint}",
            f"STORAGE_TYPE=oss",
        ])
    elif storage_type == "s3":
        env_lines.extend([
            f"S3_BUCKET={s3_bucket}",
            f"S3_REGION={s3_region}",
            f"STORAGE_TYPE=s3",
        ])

    env_content = "\\n".join(env_lines)

    cmd = (
        "mkdir -p /etc/elastic-agent && "
        f"echo -e '{env_content}' > /etc/elastic-agent/storage.env && "
        "chmod 600 /etc/elastic-agent/storage.env && "
        "grep -qxF 'set -a; source /etc/elastic-agent/storage.env; set +a' /etc/profile.d/elastic-agent.sh 2>/dev/null || "
        "echo 'set -a; source /etc/elastic-agent/storage.env; set +a' >> /etc/profile.d/elastic-agent.sh"
    )

    return BootstrapStep(
        name="cloud-storage-credentials",
        command=cmd,
        timeout=timeout,
        retry_count=0,
        description="Inject cloud storage credentials for FileSyncManager",
    )


def load_storage_env(env_path: str = "/etc/elastic-agent/storage.env") -> dict[str, str]:
    """Load storage environment variables from the injected env file."""
    result: dict[str, str] = {}
    try:
        for line in Path(env_path).read_text().splitlines():
            line = line.strip()
            if line and "=" in line and not line.startswith("#"):
                key, _, value = line.partition("=")
                result[key.strip()] = value.strip()
    except FileNotFoundError:
        pass
    return result
