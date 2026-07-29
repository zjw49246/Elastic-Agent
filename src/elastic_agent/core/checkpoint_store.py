"""Immutable, hash-verified S3 checkpoints for recoverable Mode-B Jobs."""

from __future__ import annotations

import fnmatch
import hashlib
import json
import os
import re
import shutil
import stat
import uuid
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any, BinaryIO

_SAFE_COMPONENT = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")
_SHA256 = re.compile(r"[0-9a-f]{64}")
_SCHEMA_VERSION = 1
_READ_CHUNK = 1024 * 1024


class CheckpointError(RuntimeError):
    """A checkpoint cannot be committed or restored safely."""


def _safe_component(value: str, *, label: str) -> str:
    if _SAFE_COMPONENT.fullmatch(value) is None:
        raise CheckpointError(f"invalid {label}")
    return value


def _safe_relative_path(value: str, *, label: str) -> str:
    if (
        not value
        or len(value) > 4_096
        or value.startswith("/")
        or "\\" in value
        or "\x00" in value
        or any(ord(character) < 0x20 for character in value)
    ):
        raise CheckpointError(f"unsafe {label}: {value!r}")
    path = PurePosixPath(value)
    if ".." in path.parts or path == PurePosixPath("."):
        raise CheckpointError(f"unsafe {label}: {value!r}")
    return path.as_posix().rstrip("/")


def _under_any_path(relative: str, paths: list[str]) -> bool:
    return any(
        relative == path or relative.startswith(path.rstrip("/") + "/")
        for path in paths
    )


def _stable_identity(file_stat: os.stat_result) -> tuple[int, int, int, int, int]:
    return (
        file_stat.st_dev,
        file_stat.st_ino,
        file_stat.st_size,
        file_stat.st_mtime_ns,
        file_stat.st_ctime_ns,
    )


def _sha256_stream(stream: BinaryIO) -> str:
    stream.seek(0)
    digest = hashlib.sha256()
    while chunk := stream.read(_READ_CHUNK):
        digest.update(chunk)
    stream.seek(0)
    return digest.hexdigest()


class S3CheckpointStore:
    """Commit local result trees and stage trusted prior Job results.

    File blobs are immutable and content addressed. ``COMMITTED.json`` is
    uploaded last, so a generation is either absent or fully discoverable.
    """

    def __init__(
        self,
        *,
        bucket: str,
        prefix: str = "jobs",
        client: Any | None = None,
        region: str = "ap-northeast-1",
        max_objects: int = 100_000,
        max_total_bytes: int = 20 * 1024 * 1024 * 1024,
        max_manifest_bytes: int = 16 * 1024 * 1024,
    ) -> None:
        if not bucket.strip():
            raise ValueError("checkpoint bucket cannot be empty")
        if min(max_objects, max_total_bytes, max_manifest_bytes) <= 0:
            raise ValueError("checkpoint limits must be positive")
        self._bucket = bucket.strip()
        self._prefix = prefix.strip("/")
        self._client = client
        self._region = region
        self._max_objects = max_objects
        self._max_total_bytes = max_total_bytes
        self._max_manifest_bytes = max_manifest_bytes

    def _s3(self):
        if self._client is None:
            import boto3

            self._client = boto3.client("s3", region_name=self._region)
        return self._client

    def _worker_root(self, job_id: str, worker_namespace: str) -> str:
        job_id = _safe_component(job_id, label="job id")
        worker_namespace = _safe_component(
            worker_namespace, label="worker namespace",
        )
        base = f"{self._prefix}/{job_id}" if self._prefix else job_id
        return f"{base}/workers/{worker_namespace}"

    @staticmethod
    def _normalize_paths(paths: list[str]) -> list[str]:
        normalized = [
            _safe_relative_path(path, label="checkpoint path")
            for path in paths
        ]
        if not normalized:
            raise CheckpointError("checkpoint paths cannot be empty")
        if len(set(normalized)) != len(normalized):
            raise CheckpointError("checkpoint paths must be unique")
        return normalized

    @staticmethod
    def _excluded(relative: str, patterns: list[str]) -> bool:
        return any(
            fnmatch.fnmatchcase(relative, pattern)
            or fnmatch.fnmatchcase(PurePosixPath(relative).name, pattern)
            for pattern in patterns
        )

    def _safe_files(
        self,
        source_root: Path,
        paths: list[str],
        exclude: list[str],
    ):
        root = source_root.resolve(strict=True)
        for relative_root in paths:
            traversal_root = root / relative_root
            try:
                traversal_root.relative_to(root)
            except ValueError as exc:
                raise CheckpointError("checkpoint path escaped source root") from exc
            if not traversal_root.is_dir():
                raise CheckpointError(
                    f"checkpoint path is missing or not a directory: "
                    f"{relative_root!r}"
                )
            for dirpath, dirnames, filenames in os.walk(
                traversal_root, topdown=True, followlinks=False,
            ):
                directory = Path(dirpath)
                safe_dirs: list[str] = []
                for name in sorted(dirnames):
                    candidate = directory / name
                    try:
                        mode = os.lstat(candidate).st_mode
                    except OSError:
                        continue
                    relative = candidate.relative_to(root).as_posix()
                    if stat.S_ISDIR(mode) and not self._excluded(relative, exclude):
                        safe_dirs.append(name)
                dirnames[:] = safe_dirs
                for name in sorted(filenames):
                    candidate = directory / name
                    relative = candidate.relative_to(root).as_posix()
                    if self._excluded(relative, exclude):
                        continue
                    try:
                        file_stat = os.lstat(candidate)
                    except OSError:
                        continue
                    if not stat.S_ISREG(file_stat.st_mode):
                        continue
                    yield root, candidate, relative

    @staticmethod
    def _open_validated(root: Path, path: Path) -> tuple[BinaryIO, os.stat_result]:
        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        descriptor = os.open(path, flags)
        try:
            opened_stat = os.fstat(descriptor)
            if not stat.S_ISREG(opened_stat.st_mode):
                raise CheckpointError(f"checkpoint file is not regular: {path}")
            proc_path = Path(f"/proc/self/fd/{descriptor}")
            if proc_path.exists():
                proc_path.resolve(strict=True).relative_to(root)
            return os.fdopen(descriptor, "rb"), opened_stat
        except BaseException:
            os.close(descriptor)
            raise

    def commit(
        self,
        *,
        job_id: str,
        worker_namespace: str,
        source_root: Path,
        paths: list[str],
        exclude: list[str] | None = None,
        generation: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Upload all blobs, then atomically publish one generation manifest."""

        normalized_paths = self._normalize_paths(paths)
        generation = generation or (
            datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
            + "-"
            + uuid.uuid4().hex
        )
        generation = _safe_component(generation, label="checkpoint generation")
        worker_root = self._worker_root(job_id, worker_namespace)
        blob_root = f"{worker_root}/checkpoints/blobs"
        files: list[dict[str, Any]] = []
        total_bytes = 0

        for root, path, relative in self._safe_files(
            Path(source_root), normalized_paths, list(exclude or []),
        ):
            if len(files) >= self._max_objects:
                raise CheckpointError("checkpoint object limit exceeded")
            stream, opened_stat = self._open_validated(root, path)
            with stream:
                before = _stable_identity(opened_stat)
                digest = _sha256_stream(stream)
                size = opened_stat.st_size
                total_bytes += size
                if total_bytes > self._max_total_bytes:
                    raise CheckpointError("checkpoint byte limit exceeded")
                object_key = f"{blob_root}/{digest}"
                duplicate = os.dup(stream.fileno())
                with os.fdopen(duplicate, "rb") as upload:
                    upload.seek(0)
                    self._s3().upload_fileobj(
                        upload, self._bucket, object_key,
                    )
                after_stat = os.fstat(stream.fileno())
                after_digest = _sha256_stream(stream)
                if (
                    _stable_identity(after_stat) != before
                    or after_digest != digest
                ):
                    raise CheckpointError(
                        f"checkpoint file changed while uploading: {relative}"
                    )
            files.append({
                "path": relative,
                "size": size,
                "sha256": digest,
                "object_key": object_key,
            })

        manifest: dict[str, Any] = {
            "schema_version": _SCHEMA_VERSION,
            "job_id": job_id,
            "worker_namespace": worker_namespace,
            "generation": generation,
            "paths": normalized_paths,
            "files": files,
            "total_bytes": total_bytes,
            "committed_at": datetime.now(timezone.utc).isoformat(),
            "metadata": dict(metadata or {}),
        }
        payload = json.dumps(
            manifest,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        if len(payload) > self._max_manifest_bytes:
            raise CheckpointError("checkpoint manifest limit exceeded")
        committed_key = (
            f"{worker_root}/checkpoints/{generation}/COMMITTED.json"
        )
        self._s3().put_object(
            Bucket=self._bucket,
            Key=committed_key,
            Body=payload,
            ContentType="application/json",
        )
        return manifest

    def _read_object_limited(self, key: str, *, limit: int) -> bytes:
        response = self._s3().get_object(Bucket=self._bucket, Key=key)
        declared = int(response.get("ContentLength") or 0)
        if declared > limit:
            raise CheckpointError(f"S3 object exceeds read limit: {key}")
        body = response["Body"]
        chunks: list[bytes] = []
        consumed = 0
        try:
            while chunk := body.read(min(_READ_CHUNK, limit + 1 - consumed)):
                chunks.append(chunk)
                consumed += len(chunk)
                if consumed > limit:
                    raise CheckpointError(
                        f"S3 object exceeds read limit: {key}"
                    )
        finally:
            close = getattr(body, "close", None)
            if callable(close):
                close()
        return b"".join(chunks)

    def _committed_manifest(
        self,
        *,
        source_job_id: str,
        worker_namespace: str,
        generation: str,
    ) -> dict[str, Any]:
        worker_root = self._worker_root(source_job_id, worker_namespace)
        if generation:
            selected = _safe_component(
                generation, label="checkpoint generation",
            )
            key = (
                f"{worker_root}/checkpoints/{selected}/COMMITTED.json"
            )
        else:
            prefix = f"{worker_root}/checkpoints/"
            candidates: list[str] = []
            for page in self._s3().get_paginator("list_objects_v2").paginate(
                Bucket=self._bucket,
                Prefix=prefix,
            ):
                for item in page.get("Contents") or []:
                    key_value = str(item.get("Key") or "")
                    suffix = key_value.removeprefix(prefix)
                    parts = suffix.split("/")
                    if (
                        len(parts) == 2
                        and parts[1] == "COMMITTED.json"
                        and _SAFE_COMPONENT.fullmatch(parts[0])
                    ):
                        candidates.append(key_value)
                        if len(candidates) > self._max_objects:
                            raise CheckpointError(
                                "checkpoint generation listing limit exceeded"
                            )
            if not candidates:
                raise CheckpointError("no committed checkpoint generation found")
            key = max(candidates)
        try:
            raw = self._read_object_limited(
                key, limit=self._max_manifest_bytes,
            )
            manifest = json.loads(raw.decode("utf-8"))
        except CheckpointError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise CheckpointError("invalid checkpoint manifest") from exc
        if not isinstance(manifest, dict):
            raise CheckpointError("invalid checkpoint manifest")
        return manifest

    @staticmethod
    def _validate_manifest_identity(
        manifest: dict[str, Any],
        *,
        source_job_id: str,
        worker_namespace: str,
        paths: list[str],
    ) -> None:
        if (
            manifest.get("schema_version") != _SCHEMA_VERSION
            or manifest.get("job_id") != source_job_id
            or manifest.get("worker_namespace") != worker_namespace
            or manifest.get("paths") != paths
            or not isinstance(manifest.get("files"), list)
        ):
            raise CheckpointError("checkpoint manifest identity mismatch")

    def _write_restored_object(
        self,
        *,
        key: str,
        destination: Path,
        expected_size: int,
        expected_sha256: str | None,
    ) -> None:
        response = self._s3().get_object(Bucket=self._bucket, Key=key)
        declared = int(response.get("ContentLength") or 0)
        if declared != expected_size:
            raise CheckpointError(f"checkpoint object size mismatch: {key}")
        destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        temporary = destination.with_name(
            f".{destination.name}.part-{uuid.uuid4().hex}"
        )
        digest = hashlib.sha256()
        consumed = 0
        body = response["Body"]
        try:
            descriptor = os.open(
                temporary,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                0o600,
            )
            try:
                with os.fdopen(descriptor, "wb") as output:
                    descriptor = -1
                    while chunk := body.read(_READ_CHUNK):
                        consumed += len(chunk)
                        if consumed > expected_size:
                            raise CheckpointError(
                                f"checkpoint object grew while reading: {key}"
                            )
                        output.write(chunk)
                        digest.update(chunk)
                    output.flush()
                    os.fsync(output.fileno())
            finally:
                if descriptor >= 0:
                    os.close(descriptor)
            if consumed != expected_size:
                raise CheckpointError(
                    f"checkpoint object size mismatch: {key}"
                )
            if (
                expected_sha256 is not None
                and digest.hexdigest() != expected_sha256
            ):
                raise CheckpointError(
                    f"checkpoint object checksum mismatch: {key}"
                )
            os.replace(temporary, destination)
        finally:
            close = getattr(body, "close", None)
            if callable(close):
                close()
            try:
                os.unlink(temporary)
            except FileNotFoundError:
                pass

    def restore_checkpoint(
        self,
        *,
        source_job_id: str,
        worker_namespace: str,
        destination: Path,
        paths: list[str],
        generation: str = "",
    ) -> dict[str, Any]:
        """Restore one committed generation into a new private staging tree."""

        normalized_paths = self._normalize_paths(paths)
        destination = Path(destination)
        if destination.exists():
            raise CheckpointError("checkpoint restore destination already exists")
        destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        destination.mkdir(mode=0o700)
        try:
            manifest = self._committed_manifest(
                source_job_id=source_job_id,
                worker_namespace=worker_namespace,
                generation=generation,
            )
            self._validate_manifest_identity(
                manifest,
                source_job_id=source_job_id,
                worker_namespace=worker_namespace,
                paths=normalized_paths,
            )
            files = manifest["files"]
            if len(files) > self._max_objects:
                raise CheckpointError("checkpoint object limit exceeded")
            total = 0
            worker_root = self._worker_root(
                source_job_id, worker_namespace,
            )
            blob_root = f"{worker_root}/checkpoints/blobs/"
            for raw in files:
                if not isinstance(raw, dict):
                    raise CheckpointError("invalid checkpoint file entry")
                relative = _safe_relative_path(
                    str(raw.get("path") or ""),
                    label="checkpoint path",
                )
                if not _under_any_path(relative, normalized_paths):
                    raise CheckpointError(
                        f"checkpoint path is outside requested roots: {relative}"
                    )
                digest = str(raw.get("sha256") or "")
                key = str(raw.get("object_key") or "")
                try:
                    size = int(raw.get("size"))
                except (TypeError, ValueError) as exc:
                    raise CheckpointError(
                        "invalid checkpoint file size"
                    ) from exc
                if (
                    size < 0
                    or _SHA256.fullmatch(digest) is None
                    or key != f"{blob_root}{digest}"
                ):
                    raise CheckpointError("invalid checkpoint file entry")
                total += size
                if total > self._max_total_bytes:
                    raise CheckpointError("checkpoint byte limit exceeded")
                self._write_restored_object(
                    key=key,
                    destination=destination / relative,
                    expected_size=size,
                    expected_sha256=digest,
                )
            if int(manifest.get("total_bytes") or 0) != total:
                raise CheckpointError("checkpoint total size mismatch")
            return manifest
        except BaseException:
            shutil.rmtree(destination, ignore_errors=True)
            raise

    def restore_legacy_collection(
        self,
        *,
        source_job_id: str,
        worker_namespace: str,
        destination: Path,
        paths: list[str],
    ) -> dict[str, Any]:
        """Explicitly restore a pre-checkpoint mutable final collection."""

        normalized_paths = self._normalize_paths(paths)
        destination = Path(destination)
        if destination.exists():
            raise CheckpointError("checkpoint restore destination already exists")
        destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        destination.mkdir(mode=0o700)
        worker_root = self._worker_root(source_job_id, worker_namespace)
        manifest_key = (
            f"{worker_root}/_elastic_agent/collection.json"
        )
        try:
            raw_manifest = self._read_object_limited(
                manifest_key, limit=self._max_manifest_bytes,
            )
            manifest = json.loads(raw_manifest.decode("utf-8"))
            if (
                not isinstance(manifest, dict)
                or manifest.get("schema_version") != 1
                or manifest.get("job_id") != source_job_id
                or manifest.get("worker_namespace") != worker_namespace
                or not set(normalized_paths).issubset(
                    set(manifest.get("paths") or [])
                )
            ):
                raise CheckpointError("collection manifest identity mismatch")

            count = 0
            total = 0
            for requested in normalized_paths:
                prefix = f"{worker_root}/{requested}/"
                for page in self._s3().get_paginator(
                    "list_objects_v2"
                ).paginate(Bucket=self._bucket, Prefix=prefix):
                    for item in page.get("Contents") or []:
                        key = str(item.get("Key") or "")
                        if not key.startswith(prefix):
                            raise CheckpointError(
                                "legacy collection key escaped requested prefix"
                            )
                        relative = key.removeprefix(worker_root + "/")
                        relative = _safe_relative_path(
                            relative, label="legacy collection path",
                        )
                        if not _under_any_path(relative, normalized_paths):
                            raise CheckpointError(
                                "legacy collection key escaped requested paths"
                            )
                        try:
                            size = int(item.get("Size"))
                        except (TypeError, ValueError) as exc:
                            raise CheckpointError(
                                "invalid legacy collection object size"
                            ) from exc
                        count += 1
                        total += size
                        if count > self._max_objects:
                            raise CheckpointError(
                                "checkpoint object limit exceeded"
                            )
                        if total > self._max_total_bytes:
                            raise CheckpointError(
                                "checkpoint byte limit exceeded"
                            )
                        self._write_restored_object(
                            key=key,
                            destination=destination / relative,
                            expected_size=size,
                            expected_sha256=None,
                        )
            return manifest
        except BaseException:
            shutil.rmtree(destination, ignore_errors=True)
            raise
