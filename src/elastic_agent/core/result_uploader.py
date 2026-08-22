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
import time
from collections import OrderedDict
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
        max_concurrent_uploads: int = 8,
        max_concurrent_scans: int = 4,
        max_concurrent_hashes: int = 4,
        max_objects: int = 100_000,
        max_total_bytes: int = 20 * 1024 * 1024 * 1024,
        max_file_bytes: int | None = None,
    ) -> None:
        if not bucket.strip():
            raise ValueError("S3 results bucket must not be empty")
        for label, value in (
            ("max_concurrent_uploads", max_concurrent_uploads),
            ("max_concurrent_scans", max_concurrent_scans),
            ("max_concurrent_hashes", max_concurrent_hashes),
        ):
            if (
                isinstance(value, bool)
                or not isinstance(value, int)
                or value <= 0
                or value > 64
            ):
                raise ValueError(f"{label} must be between 1 and 64")
        max_file_bytes = (
            max_total_bytes
            if max_file_bytes is None
            else max_file_bytes
        )
        if (
            isinstance(max_objects, bool)
            or not isinstance(max_objects, int)
            or max_objects <= 0
            or isinstance(max_total_bytes, bool)
            or not isinstance(max_total_bytes, int)
            or max_total_bytes <= 0
            or isinstance(max_file_bytes, bool)
            or not isinstance(max_file_bytes, int)
            or max_file_bytes <= 0
            or max_file_bytes > max_total_bytes
        ):
            raise ValueError("result upload limits must be positive and bounded")
        self._bucket = bucket
        self._root = Path(collected_root)
        self._prefix = prefix.strip("/")
        self._client = client
        self._region = region
        # S3 durability is keyed by content, not mutable filesystem metadata.
        # mtime+size can remain unchanged across an in-place rewrite (and rsync
        # deliberately preserves mtime), which previously let final collection
        # report success while S3 still held an older same-sized result.
        self._uploaded: OrderedDict[str, str] = OrderedDict()
        self._uploaded_lock = threading.Lock()
        self._max_objects = max_objects
        self._max_total_bytes = max_total_bytes
        self._max_file_bytes = max_file_bytes
        # Exact S3 keys are serialized while disjoint fanout namespaces remain
        # concurrent. A single tree-wide lock made N shards share one 240s
        # deadline and caused healthy large fanout checkpoints to time out.
        self._key_locks = tuple(threading.Lock() for _ in range(64))
        self._upload_slots = threading.BoundedSemaphore(
            max_concurrent_uploads
        )
        self._scan_slots = threading.BoundedSemaphore(
            max_concurrent_scans
        )
        self._hash_slots = threading.BoundedSemaphore(
            max_concurrent_hashes
        )

    def _s3(self):
        if self._client is None:
            import boto3
            from botocore.config import Config

            self._client = boto3.client(
                "s3",
                region_name=self._region,
                config=Config(
                    connect_timeout=3,
                    read_timeout=10,
                    retries={
                        "total_max_attempts": 3,
                        "mode": "standard",
                    },
                    tcp_keepalive=True,
                ),
            )
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

    def _is_private_collection_control_path(self, path: Path) -> bool:
        """Whether ``path`` is an unpublished shard attempt/backup tree."""

        try:
            parts = path.absolute().relative_to(
                self._root.absolute(),
            ).parts
        except ValueError:
            return True
        return (
            len(parts) >= 3
            and parts[1] == "workers"
            and parts[2].startswith(".")
        )

    def _safe_files(
        self,
        root: Path,
        *,
        deadline_monotonic: float | None,
        cancel_event: threading.Event | None,
    ):
        """Stream a bounded tree without following links or listing it in RAM."""

        if not self._safe_directory_chain(root):
            logger.warning("Skipping unsafe result tree %s", root)
            return
        total_objects = 0
        total_bytes = 0
        pending = [root]
        while pending:
            self._check_deadline(deadline_monotonic, cancel_event)
            directory = pending.pop()
            try:
                entries = os.scandir(directory)
            except OSError as exc:
                raise S3ResultUploadError(
                    f"cannot scan result directory: {directory}"
                ) from exc
            with entries:
                for entry in entries:
                    self._check_deadline(
                        deadline_monotonic, cancel_event,
                    )
                    # Private collection transactions live next to the last
                    # published shard as ``.shard-*.attempt-*`` / ``.backup``.
                    # Preserve ordinary application dotfiles elsewhere.
                    child = Path(entry.path)
                    if self._is_private_collection_control_path(child):
                        continue
                    total_objects += 1
                    if total_objects > self._max_objects:
                        raise S3ResultUploadError(
                            "result upload object limit exceeded"
                        )
                    try:
                        mode = entry.stat(
                            follow_symlinks=False,
                        ).st_mode
                    except OSError as exc:
                        raise S3ResultUploadError(
                            f"cannot inspect result path: {child}"
                        ) from exc
                    if stat.S_ISDIR(mode):
                        pending.append(child)
                        continue
                    if not stat.S_ISREG(mode):
                        logger.warning(
                            "Skipping non-regular result path %s",
                            child,
                        )
                        continue
                    file_stat = self._safe_regular_stat(root, child)
                    if file_stat is None:
                        raise S3ResultUploadError(
                            f"result path changed while scanning: {child}"
                        )
                    if file_stat.st_size > self._max_file_bytes:
                        raise S3ResultUploadError(
                            "result upload single-file limit exceeded"
                        )
                    total_bytes += file_stat.st_size
                    if total_bytes > self._max_total_bytes:
                        raise S3ResultUploadError(
                            "result upload byte limit exceeded"
                        )
                    yield child, file_stat

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
    def _content_sha256(
        stream,
        deadline_monotonic: float | None = None,
        cancel_event: threading.Event | None = None,
    ) -> str:
        """Hash one already-open regular file without pathname races."""

        stream.seek(0)
        digest = hashlib.sha256()
        while True:
            S3ResultUploader._check_deadline(
                deadline_monotonic, cancel_event,
            )
            chunk = stream.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
        stream.seek(0)
        return digest.hexdigest()

    @staticmethod
    def _check_deadline(
        deadline_monotonic: float | None,
        cancel_event: threading.Event | None = None,
    ) -> None:
        if cancel_event is not None and cancel_event.is_set():
            raise TimeoutError("S3 result upload cancelled")
        if (
            deadline_monotonic is not None
            and time.monotonic() >= deadline_monotonic
        ):
            raise TimeoutError("S3 result upload deadline exceeded")

    @staticmethod
    def _acquire_bounded(
        lock,
        *,
        deadline_monotonic: float | None,
        cancel_event: threading.Event | None,
        label: str,
    ) -> None:
        while True:
            S3ResultUploader._check_deadline(
                deadline_monotonic, cancel_event,
            )
            remaining = (
                None
                if deadline_monotonic is None
                else max(0.0, deadline_monotonic - time.monotonic())
            )
            wait = 0.1 if remaining is None else min(0.1, remaining)
            if lock.acquire(timeout=wait):
                return
            if remaining is not None and remaining <= 0:
                raise TimeoutError(f"S3 result {label} deadline exceeded")

    def _key_lock(self, key: str) -> threading.Lock:
        digest = hashlib.sha256(key.encode("utf-8")).digest()
        return self._key_locks[int.from_bytes(digest[:2], "big") % len(
            self._key_locks
        )]

    def _cached_digest(self, key: str) -> str | None:
        with self._uploaded_lock:
            digest = self._uploaded.get(key)
            if digest is not None:
                self._uploaded.move_to_end(key)
            return digest

    def _remember_digest(self, key: str, digest: str) -> None:
        with self._uploaded_lock:
            self._uploaded[key] = digest
            self._uploaded.move_to_end(key)
            while len(self._uploaded) > self._max_objects:
                self._uploaded.popitem(last=False)

    def _bounded_content_sha256(
        self,
        stream,
        *,
        deadline_monotonic: float | None,
        cancel_event: threading.Event | None,
    ) -> str:
        self._acquire_bounded(
            self._hash_slots,
            deadline_monotonic=deadline_monotonic,
            cancel_event=cancel_event,
            label="hash slot",
        )
        try:
            return self._content_sha256(
                stream, deadline_monotonic, cancel_event,
            )
        finally:
            self._hash_slots.release()

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

    def _sync_tree(
        self,
        root: Path,
        *,
        deadline_monotonic: float | None = None,
        cancel_event: threading.Event | None = None,
    ) -> int:
        """Upload one local tree and raise if any object failed.

        Upload errors used to be logged and swallowed, which let a Job finish
        while its only durable result copy was missing.  Collection callers can
        now retry or mark the Job failed with the concrete S3 error.
        """
        if not root.is_dir():
            return 0
        uploaded = 0
        failures: list[str] = []
        files = self._safe_files(
            root,
            deadline_monotonic=deadline_monotonic,
            cancel_event=cancel_event,
        )
        try:
            while True:
                self._acquire_bounded(
                    self._scan_slots,
                    deadline_monotonic=deadline_monotonic,
                    cancel_event=cancel_event,
                    label="scan slot",
                )
                try:
                    try:
                        p, listed_stat = next(files)
                    except StopIteration:
                        break
                finally:
                    self._scan_slots.release()

                self._check_deadline(deadline_monotonic, cancel_event)
                rel = p.relative_to(self._root).as_posix()   # <job_id>/...
                key = f"{self._prefix}/{rel}" if self._prefix else rel
                key_lock = self._key_lock(key)
                key_acquired = False
                try:
                    self._acquire_bounded(
                        key_lock,
                        deadline_monotonic=deadline_monotonic,
                        cancel_event=cancel_event,
                        label="key lock",
                    )
                    key_acquired = True
                    self._check_deadline(deadline_monotonic, cancel_event)
                    try:
                        stream, opened_stat = self._open_safe_regular(root, p)
                        with stream:
                            before = self._stable_identity(opened_stat)
                            if before != self._stable_identity(listed_stat):
                                raise RuntimeError(
                                    "result changed between scan and open"
                                )
                            content_digest = self._bounded_content_sha256(
                                stream,
                                deadline_monotonic=deadline_monotonic,
                                cancel_event=cancel_event,
                            )
                            if self._cached_digest(key) == content_digest:
                                continue
                            # boto3/s3transfer is allowed to close the file object it
                            # receives.  Give it a duplicate descriptor so the
                            # original validated descriptor remains available for
                            # the post-upload stability check.
                            upload_fd = os.dup(stream.fileno())
                            with os.fdopen(upload_fd, "rb") as upload_stream:
                                upload_stream.seek(0)
                                kwargs = {}
                                if (
                                    deadline_monotonic is not None
                                    or cancel_event is not None
                                ):
                                    kwargs["Callback"] = lambda _bytes: (
                                        self._check_deadline(
                                            deadline_monotonic, cancel_event,
                                        )
                                    )
                                self._acquire_bounded(
                                    self._upload_slots,
                                    deadline_monotonic=deadline_monotonic,
                                    cancel_event=cancel_event,
                                    label="upload slot",
                                )
                                try:
                                    self._s3().upload_fileobj(
                                        upload_stream,
                                        self._bucket,
                                        key,
                                        **kwargs,
                                    )
                                finally:
                                    self._upload_slots.release()
                            # A producer may still be writing while periodic/final
                            # collection runs.  Verify the descriptor remained the
                            # exact hashed snapshot; otherwise do not cache success
                            # and force the orchestrator's retry/failure path.
                            after_stat = os.fstat(stream.fileno())
                            stream.seek(0)
                            after_digest = self._bounded_content_sha256(
                                stream,
                                deadline_monotonic=deadline_monotonic,
                                cancel_event=cancel_event,
                            )
                            if (
                                self._stable_identity(after_stat) != before
                                or after_digest != content_digest
                            ):
                                raise RuntimeError(
                                    "result changed while it was being uploaded"
                                )
                        self._remember_digest(key, content_digest)
                        uploaded += 1
                    except Exception as exc:  # noqa: BLE001
                        # Cancellation/deadline is tree-scoped, not an individual
                        # object failure. Stop walking immediately so finalization
                        # does not scale with the number of remaining filenames.
                        self._check_deadline(
                            deadline_monotonic, cancel_event,
                        )
                        logger.exception("S3 upload failed for %s", key)
                        failures.append(
                            f"{key}: {exc or type(exc).__name__}"
                        )
                finally:
                    if key_acquired:
                        key_lock.release()
        finally:
            close = getattr(files, "close", None)
            if callable(close):
                close()
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

    def sync_once(
        self,
        *,
        deadline_monotonic: float | None = None,
        cancel_event: threading.Event | None = None,
    ) -> int:
        """Upload each bounded Job tree without aggregating all history.

        Applying one 20-GiB/100k-object allowance to the lifetime
        ``collected_root`` made the periodic uploader fail forever once enough
        unrelated historical Jobs accumulated. Each Job is an independent S3
        namespace and receives its own bounded scan.
        """

        if not self._safe_directory_chain(self._root):
            return 0
        uploaded = 0
        seen_jobs = 0
        try:
            entries = os.scandir(self._root)
        except FileNotFoundError:
            return 0
        except OSError as exc:
            raise S3ResultUploadError(
                "cannot scan collected result root"
            ) from exc
        with entries:
            for entry in entries:
                self._check_deadline(
                    deadline_monotonic, cancel_event,
                )
                if self._is_private_collection_control_path(
                    Path(entry.path),
                ):
                    continue
                try:
                    is_directory = entry.is_dir(follow_symlinks=False)
                except OSError as exc:
                    raise S3ResultUploadError(
                        "cannot inspect collected Job directory"
                    ) from exc
                if not is_directory:
                    continue
                seen_jobs += 1
                if seen_jobs > self._max_objects:
                    raise S3ResultUploadError(
                        "collected Job directory limit exceeded"
                    )
                uploaded += self._sync_tree(
                    Path(entry.path),
                    deadline_monotonic=deadline_monotonic,
                    cancel_event=cancel_event,
                )
        return uploaded

    def sync_job(
        self,
        job_id: str,
        *,
        deadline_monotonic: float | None = None,
        cancel_event: threading.Event | None = None,
    ) -> int:
        """Synchronously upload one Job tree for an awaited collection.

        ``job_id`` is required to be a single path component so an API-provided
        identifier cannot make this method walk outside ``collected_root``.
        """
        if not job_id or Path(job_id).name != job_id or job_id in {".", ".."}:
            raise ValueError("job_id must be a single safe path component")
        return self._sync_tree(
            self._root / job_id,
            deadline_monotonic=deadline_monotonic,
            cancel_event=cancel_event,
        )

    def sync_worker(
        self,
        job_id: str,
        worker_namespace: str,
        *,
        deadline_monotonic: float | None = None,
        cancel_event: threading.Event | None = None,
    ) -> int:
        """Upload exactly one immutable-per-collection worker namespace.

        Fanout collection rsyncs different worker trees concurrently. Walking the
        whole Job from each shard can observe another shard halfway through rsync
        and incorrectly fail an otherwise valid checkpoint as unstable.
        """

        for label, value in (
            ("job_id", job_id),
            ("worker_namespace", worker_namespace),
        ):
            if (
                not value
                or Path(value).name != value
                or value in {".", ".."}
            ):
                raise ValueError(f"{label} must be a single safe path component")
        return self._sync_tree(
            self._root / job_id / "workers" / worker_namespace,
            deadline_monotonic=deadline_monotonic,
            cancel_event=cancel_event,
        )

    def s3_uri(self, job_id: str) -> str:
        base = f"s3://{self._bucket}"
        return f"{base}/{self._prefix}/{job_id}/" if self._prefix else f"{base}/{job_id}/"

    async def run_periodic(
        self,
        interval: float = 300.0,
        operation_timeout: float = 240.0,
    ) -> None:
        while True:
            try:
                # boto3 is synchronous; do not stall websocket/lifecycle work.
                cancel_event = threading.Event()
                task = asyncio.create_task(asyncio.to_thread(
                    self.sync_once,
                    deadline_monotonic=(
                        time.monotonic() + max(1.0, operation_timeout)
                    ),
                    cancel_event=cancel_event,
                ))
                cancellation: asyncio.CancelledError | None = None
                while not task.done():
                    try:
                        await asyncio.shield(task)
                    except asyncio.CancelledError as exc:
                        if cancellation is None:
                            cancellation = exc
                            cancel_event.set()
                    except BaseException:
                        # Preserve an earlier outer cancellation; consume the
                        # owned operation's exception through task.result().
                        break
                operation_error: BaseException | None = None
                try:
                    task.result()
                except BaseException as exc:
                    operation_error = exc
                if cancellation is not None:
                    if operation_error is not None:
                        raise cancellation from operation_error
                    raise cancellation
                if operation_error is not None:
                    raise operation_error
            except asyncio.CancelledError:
                raise
            except Exception:  # pragma: no cover - defensive
                logger.exception("S3ResultUploader periodic sync failed")
            await asyncio.sleep(interval)
