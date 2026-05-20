"""MockOSS — local filesystem mock for cloud storage.

T-203: Implements StorageBackend interface using local filesystem.
Tracks all operations for test assertions.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
from pathlib import Path

from elastic_agent.worker.file_sync import StorageBackend


class MockOSS(StorageBackend):
    """Local filesystem-backed storage for testing.

    Implements the full StorageBackend interface so it can replace
    OSSBackend / S3Backend in tests. Tracks all operations for assertions.

    Usage::

        oss = MockOSS(tmp_path / "oss")
        await oss.upload_bytes(b"hello", "bucket/key.txt")
        data = await oss.read_file("bucket/key.txt")
        assert data == b"hello"

        # Inspect operations
        assert len(oss.uploads) == 1
        assert oss.uploads[0]["oss_key"] == "bucket/key.txt"
    """

    def __init__(self, root_dir: str | Path) -> None:
        self._root = Path(root_dir)
        self._root.mkdir(parents=True, exist_ok=True)
        self.uploads: list[dict] = []
        self.reads: list[dict] = []
        self._fail_next: dict[str, Exception] = {}

    @property
    def root(self) -> Path:
        return self._root

    def fail_next(self, method: str, error: Exception) -> None:
        """Make the next call to *method* raise *error*."""
        self._fail_next[method] = error

    def _check_failure(self, method: str) -> None:
        exc = self._fail_next.pop(method, None)
        if exc is not None:
            raise exc

    def reset(self) -> None:
        """Clear recorded operations (does NOT delete files)."""
        self.uploads.clear()
        self.reads.clear()
        self._fail_next.clear()

    def list_keys(self, prefix: str = "") -> list[str]:
        """List all stored keys matching prefix."""
        result = []
        for path in self._root.rglob("*"):
            if path.is_file():
                key = str(path.relative_to(self._root))
                if key.startswith(prefix):
                    result.append(key)
        return sorted(result)

    def get_content(self, oss_key: str) -> bytes:
        """Synchronously read stored content by key."""
        path = self._root / oss_key
        return path.read_bytes()

    def put_content(self, oss_key: str, content: bytes | str) -> None:
        """Synchronously write content. Useful for pre-seeding test data."""
        if isinstance(content, str):
            content = content.encode("utf-8")
        path = self._root / oss_key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)

    async def upload_file(self, local_path: str, oss_key: str) -> None:
        self._check_failure("upload_file")
        src = Path(local_path)
        if not src.exists():
            raise FileNotFoundError(f"Local file not found: {local_path}")
        data = await asyncio.to_thread(src.read_bytes)
        dest = self._root / oss_key
        dest.parent.mkdir(parents=True, exist_ok=True)
        await asyncio.to_thread(dest.write_bytes, data)
        md5 = hashlib.md5(data).hexdigest()
        self.uploads.append({
            "method": "upload_file",
            "local_path": local_path,
            "oss_key": oss_key,
            "size": len(data),
            "md5": md5,
        })

    async def upload_bytes(self, data: bytes, oss_key: str, content_type: str = "application/octet-stream") -> None:
        self._check_failure("upload_bytes")
        dest = self._root / oss_key
        dest.parent.mkdir(parents=True, exist_ok=True)
        await asyncio.to_thread(dest.write_bytes, data)
        md5 = hashlib.md5(data).hexdigest()
        self.uploads.append({
            "method": "upload_bytes",
            "oss_key": oss_key,
            "size": len(data),
            "md5": md5,
            "content_type": content_type,
        })

    async def read_file(self, oss_key: str) -> bytes:
        self._check_failure("read_file")
        path = self._root / oss_key
        if not path.exists():
            raise FileNotFoundError(f"Key not found: {oss_key}")
        data = await asyncio.to_thread(path.read_bytes)
        self.reads.append({
            "oss_key": oss_key,
            "size": len(data),
        })
        return data

    async def file_exists(self, oss_key: str) -> bool:
        self._check_failure("file_exists")
        return (self._root / oss_key).exists()

    async def get_presigned_url(self, oss_key: str, expires: int = 3600) -> str | None:
        self._check_failure("get_presigned_url")
        if not (self._root / oss_key).exists():
            return None
        return f"file://{self._root / oss_key}?expires={expires}"
