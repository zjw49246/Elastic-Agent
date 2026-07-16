"""S3ResultUploader — periodically push collected job results to S3.

After the batch flow rsyncs each worker's ``collect.paths`` into the Manager's
``collected/<job_id>/`` dir, this uploader mirrors that tree to
``s3://<bucket>/<prefix>/<job_id>/`` on a timer, so results land in durable
object storage as they arrive (not only at job end). Only new/changed files are
re-uploaded (tracked by mtime).
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


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
        self._bucket = bucket
        self._root = Path(collected_root)
        self._prefix = prefix.strip("/")
        self._client = client
        self._region = region
        self._uploaded: dict[str, float] = {}   # s3 key -> source mtime

    def _s3(self):
        if self._client is None:
            import boto3
            self._client = boto3.client("s3", region_name=self._region)
        return self._client

    def sync_once(self) -> int:
        """Upload new/changed files under collected_root. Returns #uploaded."""
        if not self._root.is_dir():
            return 0
        uploaded = 0
        for p in sorted(self._root.rglob("*")):
            if not p.is_file():
                continue
            rel = p.relative_to(self._root).as_posix()   # <job_id>/...
            key = f"{self._prefix}/{rel}" if self._prefix else rel
            mtime = p.stat().st_mtime
            if self._uploaded.get(key) == mtime:
                continue
            try:
                self._s3().upload_file(str(p), self._bucket, key)
                self._uploaded[key] = mtime
                uploaded += 1
            except Exception:
                logger.exception("S3 upload failed for %s", key)
        if uploaded:
            logger.info("S3ResultUploader: uploaded %d file(s) to s3://%s/%s",
                        uploaded, self._bucket, self._prefix)
        return uploaded

    def s3_uri(self, job_id: str) -> str:
        base = f"s3://{self._bucket}"
        return f"{base}/{self._prefix}/{job_id}/" if self._prefix else f"{base}/{job_id}/"

    async def run_periodic(self, interval: float = 300.0) -> None:
        while True:
            try:
                self.sync_once()
            except Exception:  # pragma: no cover - defensive
                logger.exception("S3ResultUploader periodic sync failed")
            await asyncio.sleep(interval)
