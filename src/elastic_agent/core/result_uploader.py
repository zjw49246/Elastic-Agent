"""S3ResultUploader — periodically push collected job results to S3.

After the batch flow rsyncs each worker's ``collect.paths`` into the Manager's
``collected/<job_id>/`` dir, this uploader mirrors that tree to
``s3://<bucket>/<prefix>/<job_id>/`` on a timer, so results land in durable
object storage as they arrive (not only at job end). Only new/changed files are
re-uploaded (tracked by mtime).
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import stat
import threading
from pathlib import Path

logger = logging.getLogger(__name__)


class S3ResultUploadError(RuntimeError):
    """One or more result objects could not be durably uploaded."""


class S3ResultUploader:
    def __init__(
        self,
        bucket: str,
        collected_root: str,
        *,
        prefix: str = "jobs",
        client=None,
        region: str = "ap-northeast-1",
    ) -> None:
        if not bucket.strip():
            raise ValueError("S3 results bucket must not be empty")
        self._bucket = bucket
        self._root = Path(collected_root)
        self._prefix = prefix.strip("/")
        self._client = client
        self._region = region
        # S3 durability is keyed by content, not mutable filesystem metadata.
        # mtime+size can remain unchanged across an in-place rewrite (and rsync
        # deliberately preserves mtime), which previously let final collection
        # report success while S3 still held an older same-sized result.
        self._uploaded: dict[str, str] = {}
        # Periodic sync and a Job's awaited final collection may run at the same
        # time.  Serialise them so the upload cache cannot report an object as
        # durable before upload_file has actually returned.
        self._sync_lock = threading.Lock()

    def _s3(self):
        if self._client is None:
            import boto3
            self._client = boto3.client("s3", region_name=self._region)
        return self._client

    def _safe_directory_chain(self, target: Path) -> bool:
        """Whether ``self._root → target`` consists only of real directories."""
        base = self._root.absolute()
        target = target.absolute()
        try:
            relative = target.relative_to(base)
        except ValueError:
            return False

        current = base
        candidates = [current]
        for part in relative.parts:
            current /= part
            candidates.append(current)
        try:
            return all(
                stat.S_ISDIR(os.lstat(candidate).st_mode)
                for candidate in candidates
            )
        except OSError:
            return False

    def _safe_regular_stat(
        self, traversal_root: Path, candidate: Path,
    ) -> os.stat_result | None:
        """lstat a contained regular file without following any symlink."""
        root = traversal_root.absolute()
        path = candidate.absolute()
        try:
            path.relative_to(root)
        except ValueError:
            return None
        if not self._safe_directory_chain(root):
            return None
        if not self._safe_directory_chain(path.parent):
            return None
        try:
            result = os.lstat(path)
        except OSError:
            return None
        return result if stat.S_ISREG(result.st_mode) else None

    def _safe_files(self, root: Path):
        """Yield path/stat pairs without traversing symlinks or special files."""
        if not self._safe_directory_chain(root):
            logger.warning("Skipping unsafe result tree %s", root)
            return
        for dirpath, dirnames, filenames in os.walk(
            root, topdown=True, followlinks=False,
        ):
            directory = Path(dirpath)
            # os.walk lists directory symlinks in dirnames even when it will not
            # follow them.  Prune them explicitly so later code cannot mistake
            # them for an approved path component.
            safe_dirs: list[str] = []
            for name in sorted(dirnames):
                child = directory / name
                try:
                    mode = os.lstat(child).st_mode
                except OSError:
                    continue
                if stat.S_ISDIR(mode):
                    safe_dirs.append(name)
                else:
                    logger.warning("Skipping non-directory result path %s", child)
            dirnames[:] = safe_dirs

            for name in sorted(filenames):
                candidate = directory / name
                file_stat = self._safe_regular_stat(root, candidate)
                if file_stat is None:
                    logger.warning("Skipping unsafe/non-regular result path %s", candidate)
                    continue
                yield candidate, file_stat

    def _open_safe_regular(self, root: Path, path: Path):
        """Open one result by descriptor without following a final symlink.

        The worker-controlled rsync tree can change while a periodic upload is
        running.  Passing a pathname to boto3 after an earlier lstat leaves a
        check/use window in which that path can become a symlink.  Upload from
        the already-validated descriptor instead and, on Linux, verify its
        kernel-resolved target is still inside this exact Job tree.
        """
        flags = os.O_RDONLY
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        fd = os.open(path, flags)
        try:
            opened_stat = os.fstat(fd)
            if not stat.S_ISREG(opened_stat.st_mode):
                raise RuntimeError(f"result path is not a regular file: {path}")
            proc_path = Path(f"/proc/self/fd/{fd}")
            if proc_path.exists():
                actual = proc_path.resolve(strict=True)
                actual.relative_to(root.resolve(strict=True))
            else:  # portable fallback: recheck lexical chain + inode identity
                if not self._safe_directory_chain(path.parent):
                    raise RuntimeError(f"unsafe result directory chain: {path}")
                current = os.lstat(path)
                if (
                    not stat.S_ISREG(current.st_mode)
                    or current.st_dev != opened_stat.st_dev
                    or current.st_ino != opened_stat.st_ino
                ):
                    raise RuntimeError(f"result path changed while opening: {path}")
            return os.fdopen(fd, "rb"), opened_stat
        except BaseException:
            os.close(fd)
            raise

    @staticmethod
    def _content_sha256(stream) -> str:
        """Hash one already-open regular file without pathname races."""

        stream.seek(0)
        digest = hashlib.sha256()
        while True:
            chunk = stream.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
        stream.seek(0)
        return digest.hexdigest()

    @staticmethod
    def _stable_identity(file_stat: os.stat_result) -> tuple[int, int, int, int, int]:
        """Metadata used only to detect mutation during hash/upload.

        It is not used for deduplication; the SHA-256 digest is authoritative.
        """

        return (
            file_stat.st_dev,
            file_stat.st_ino,
            file_stat.st_size,
            file_stat.st_mtime_ns,
            file_stat.st_ctime_ns,
        )

    def _sync_tree(self, root: Path) -> int:
        """Upload one local tree and raise if any object failed.

        Upload errors used to be logged and swallowed, which let a Job finish
        while its only durable result copy was missing.  Collection callers can
        now retry or mark the Job failed with the concrete S3 error.
        """
        if not root.is_dir():
            return 0
        uploaded = 0
        failures: list[str] = []
        with self._sync_lock:
            for p, _file_stat in self._safe_files(root):
                rel = p.relative_to(self._root).as_posix()   # <job_id>/...
                key = f"{self._prefix}/{rel}" if self._prefix else rel
                try:
                    stream, opened_stat = self._open_safe_regular(root, p)
                    with stream:
                        before = self._stable_identity(opened_stat)
                        content_digest = self._content_sha256(stream)
                        if self._uploaded.get(key) == content_digest:
                            continue
                        # boto3/s3transfer is allowed to close the file object it
                        # receives.  Give it a duplicate descriptor so the
                        # original validated descriptor remains available for
                        # the post-upload stability check.
                        upload_fd = os.dup(stream.fileno())
                        with os.fdopen(upload_fd, "rb") as upload_stream:
                            upload_stream.seek(0)
                            self._s3().upload_fileobj(
                                upload_stream, self._bucket, key,
                            )
                        # A producer may still be writing while periodic/final
                        # collection runs.  Verify the descriptor remained the
                        # exact hashed snapshot; otherwise do not cache success
                        # and force the orchestrator's retry/failure path.
                        after_stat = os.fstat(stream.fileno())
                        stream.seek(0)
                        after_digest = self._content_sha256(stream)
                        if (
                            self._stable_identity(after_stat) != before
                            or after_digest != content_digest
                        ):
                            raise RuntimeError(
                                "result changed while it was being uploaded"
                            )
                    self._uploaded[key] = content_digest
                    uploaded += 1
                except Exception as exc:  # noqa: BLE001
                    logger.exception("S3 upload failed for %s", key)
                    failures.append(f"{key}: {exc or type(exc).__name__}")
        if uploaded:
            logger.info("S3ResultUploader: uploaded %d file(s) to s3://%s/%s",
                        uploaded, self._bucket, self._prefix)
        if failures:
            sample = "; ".join(failures[:3])
            if len(failures) > 3:
                sample += f"; and {len(failures) - 3} more"
            raise S3ResultUploadError(
                f"failed to upload {len(failures)} result object(s): {sample}"
            )
        return uploaded

    def sync_once(self) -> int:
        """Upload new/changed files under collected_root. Returns #uploaded."""
        return self._sync_tree(self._root)

    def sync_job(self, job_id: str) -> int:
        """Synchronously upload one Job tree for an awaited collection.

        ``job_id`` is required to be a single path component so an API-provided
        identifier cannot make this method walk outside ``collected_root``.
        """
        if not job_id or Path(job_id).name != job_id or job_id in {".", ".."}:
            raise ValueError("job_id must be a single safe path component")
        return self._sync_tree(self._root / job_id)

    def s3_uri(self, job_id: str) -> str:
        base = f"s3://{self._bucket}"
        return f"{base}/{self._prefix}/{job_id}/" if self._prefix else f"{base}/{job_id}/"

    async def run_periodic(self, interval: float = 300.0) -> None:
        while True:
            try:
                # boto3 is synchronous; do not stall websocket/lifecycle work.
                await asyncio.to_thread(self.sync_once)
            except Exception:  # pragma: no cover - defensive
                logger.exception("S3ResultUploader periodic sync failed")
            await asyncio.sleep(interval)
