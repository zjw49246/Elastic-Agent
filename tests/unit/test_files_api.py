"""Unit tests for external files API (T-033)."""

from __future__ import annotations

import asyncio
import io
import json
import os
import threading
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from elastic_agent.worker.file_sync import (
    LocalBackend,
    StorageBackend,
    StorageObjectReader,
)


class _TrackingBody(io.BytesIO):
    def __init__(self, payload: bytes) -> None:
        super().__init__(payload)
        self.close_calls = 0
        self.read_sizes: list[int] = []

    def read(self, size=-1):
        self.read_sizes.append(size)
        return super().read(size)

    def close(self) -> None:
        self.close_calls += 1
        super().close()


class _TrackingStorage(StorageBackend):
    def __init__(
        self,
        objects: dict[str, tuple[int, bytes]],
        *,
        presigned_url: str | None = None,
    ) -> None:
        self.objects = objects
        self.presigned_url = presigned_url
        self.bodies: dict[str, list[_TrackingBody]] = {}
        self.opened: list[str] = []
        self.whole_file_reads = 0

    async def open_reader(
        self,
        oss_key: str,
        *,
        executor=None,
    ) -> StorageObjectReader:
        self.opened.append(oss_key)
        try:
            declared_size, payload = self.objects[oss_key]
        except KeyError:
            raise FileNotFoundError(oss_key) from None
        body = _TrackingBody(payload)
        self.bodies.setdefault(oss_key, []).append(body)
        return StorageObjectReader(
            body,
            size=declared_size,
            executor=executor,
        )

    async def read_file(self, oss_key: str) -> bytes:
        self.whole_file_reads += 1
        raise AssertionError("external file API must not call read_file")

    async def file_exists(self, oss_key: str) -> bool:
        return oss_key in self.objects

    async def get_presigned_url(
        self,
        oss_key: str,
        expires: int = 3600,
        *,
        executor=None,
    ) -> str | None:
        return self.presigned_url


@pytest.fixture
def storage_dir(tmp_path):
    d = tmp_path / "storage"
    d.mkdir()
    return d


@pytest.fixture
def local_storage(storage_dir):
    return LocalBackend(str(storage_dir))


@pytest.fixture
def mock_manager(local_storage):
    mgr = MagicMock()
    mgr.file_storage = local_storage
    return mgr


@pytest.fixture
def client(mock_manager):
    os.environ["ELASTIC_AGENT_EXTERNAL_API_KEYS"] = "test-key-123"
    from elastic_agent.api.auth import reset_api_keys
    reset_api_keys()

    from fastapi import FastAPI

    from elastic_agent.api.routes.files import router

    app = FastAPI()
    app.include_router(router, prefix="/api")

    with patch("elastic_agent.api.routes.files._mgr", return_value=mock_manager):
        yield TestClient(app)

    os.environ.pop("ELASTIC_AGENT_EXTERNAL_API_KEYS", None)
    reset_api_keys()


class TestReadFile:

    async def test_read_file_success(self, client, local_storage, storage_dir):
        (storage_dir / "tasks" / "task-1").mkdir(parents=True)
        (storage_dir / "tasks" / "task-1" / "chapter01.md").write_text("Hello world")

        resp = client.get(
            "/api/external/files/task-1/chapter01.md",
            headers={"Authorization": "Bearer test-key-123"},
        )
        assert resp.status_code == 200
        assert resp.text == "Hello world"
        assert "text/markdown" in resp.headers.get("content-type", "")

    async def test_read_file_not_found(self, client):
        resp = client.get(
            "/api/external/files/task-1/nonexistent.md",
            headers={"Authorization": "Bearer test-key-123"},
        )
        assert resp.status_code == 404

    async def test_read_file_with_synced_at(self, client, storage_dir):
        task_dir = storage_dir / "tasks" / "task-1"
        task_dir.mkdir(parents=True)
        (task_dir / "chapter01.md").write_text("Content")

        manifest = {
            "task_id": "task-1",
            "worker_id": "w1",
            "files": [
                {
                    "path": "/root/work/chapter01.md",
                    "oss_key": "tasks/task-1/chapter01.md",
                    "size": 7,
                    "md5": "abc",
                    "synced_at": "2026-05-01T12:00:00Z",
                }
            ],
        }
        (task_dir / "_sync_manifest.json").write_text(json.dumps(manifest))

        resp = client.get(
            "/api/external/files/task-1/chapter01.md",
            headers={"Authorization": "Bearer test-key-123"},
        )
        assert resp.status_code == 200
        assert resp.headers.get("x-synced-at") == "2026-05-01T12:00:00Z"

    async def test_read_file_url_mode_not_supported(self, client, storage_dir):
        task_dir = storage_dir / "tasks" / "task-1"
        task_dir.mkdir(parents=True)
        (task_dir / "file.txt").write_text("data")

        resp = client.get(
            "/api/external/files/task-1/file.txt?mode=url",
            headers={"Authorization": "Bearer test-key-123"},
        )
        assert resp.status_code == 501

    async def test_read_file_nested_path(self, client, storage_dir):
        nested = storage_dir / "tasks" / "task-1" / "sub" / "dir"
        nested.mkdir(parents=True)
        (nested / "file.txt").write_text("nested content")

        resp = client.get(
            "/api/external/files/task-1/sub/dir/file.txt",
            headers={"Authorization": "Bearer test-key-123"},
        )
        assert resp.status_code == 200
        assert resp.text == "nested content"

    async def test_read_file_accepts_real_batch_task_id_with_colons(
        self, client, storage_dir,
    ):
        task_id = "job-abc:aws:i-123:deadbe"
        task_dir = storage_dir / "tasks" / task_id
        task_dir.mkdir(parents=True)
        (task_dir / "result.txt").write_text("batch output")

        resp = client.get(
            f"/api/external/files/{task_id}/result.txt",
            headers={"Authorization": "Bearer test-key-123"},
        )

        assert resp.status_code == 200
        assert resp.text == "batch output"

    async def test_read_file_requires_auth(self, client):
        resp = client.get("/api/external/files/task-1/file.txt")
        assert resp.status_code == 401

    @pytest.mark.parametrize(
        "url",
        [
            "/api/external/files/task-1/%2e%2e/%2e%2e/secret.txt",
            "/api/external/files/%2e%2e/secret.txt",
            "/api/external/files/task-1/safe%5c..%5csecret.txt",
            "/api/external/files/task-1/line%E2%80%A8break.txt",
            "/api/external/files/task-1/zero%E2%80%8Bwidth.txt",
        ],
    )
    async def test_read_file_rejects_path_traversal(self, client, url):
        resp = client.get(
            url,
            headers={"Authorization": "Bearer test-key-123"},
        )
        # URL clients/proxies may normalize encoded dot segments before
        # routing; either explicit rejection or a route-level 404 is safe.
        assert resp.status_code in {400, 404}

    async def test_content_streams_without_whole_object_buffering(
        self, client, mock_manager, monkeypatch,
    ):
        from elastic_agent.api.routes import files as files_route

        key = "tasks/task-1/large.bin"
        storage = _TrackingStorage({key: (10, b"abcdefghij")})
        mock_manager.file_storage = storage
        monkeypatch.setattr(files_route, "EXTERNAL_STREAM_CHUNK_BYTES", 4)

        response = client.get(
            "/api/external/files/task-1/large.bin",
            headers={"Authorization": "Bearer test-key-123"},
        )

        assert response.status_code == 200
        assert response.content == b"abcdefghij"
        assert response.headers["content-length"] == "10"
        assert storage.whole_file_reads == 0
        body = storage.bodies[key][0]
        assert body.read_sizes == [4, 4, 2, 1]
        assert body.close_calls == 1

    async def test_saturated_content_stream_is_rejected_before_object_open(
        self, client, mock_manager, monkeypatch,
    ):
        from elastic_agent.api.routes import files as files_route

        admission = files_route._ExternalStreamAdmission(1)
        monkeypatch.setattr(
            files_route,
            "_EXTERNAL_STREAM_ADMISSION",
            admission,
        )
        held = admission.try_acquire()
        assert held is not None
        storage = _TrackingStorage({
            "tasks/task-1/data.bin": (4, b"data"),
        })
        mock_manager.file_storage = storage
        try:
            response = client.get(
                "/api/external/files/task-1/data.bin",
                headers={"Authorization": "Bearer test-key-123"},
            )
        finally:
            held.release()

        assert response.status_code == 503
        assert response.headers["retry-after"] == "1"
        assert storage.opened == []
        assert admission.active == 0

    async def test_content_open_read_close_avoid_default_executor(
        self, client, mock_manager, storage_dir, monkeypatch,
    ):
        from elastic_agent.api.routes import files as files_route

        task_dir = storage_dir / "tasks" / "task-1"
        task_dir.mkdir(parents=True)
        (task_dir / "data.bin").write_bytes(b"data")
        admission = files_route._ExternalStreamAdmission(1)
        monkeypatch.setattr(
            files_route,
            "_EXTERNAL_STREAM_ADMISSION",
            admission,
        )

        async def forbidden_to_thread(*_args, **_kwargs):
            raise AssertionError("external stream used asyncio default executor")

        monkeypatch.setattr(asyncio, "to_thread", forbidden_to_thread)
        response = await files_route.read_file("task-1", "data.bin")
        payload = b"".join([chunk async for chunk in response.body_iterator])

        assert payload == b"data"
        assert admission.active == 0

    async def test_oversized_content_is_rejected_before_streaming(
        self, client, mock_manager, monkeypatch,
    ):
        from elastic_agent.api.routes import files as files_route

        key = "tasks/task-1/large.bin"
        storage = _TrackingStorage({key: (5, b"12345")})
        mock_manager.file_storage = storage
        monkeypatch.setattr(files_route, "EXTERNAL_CONTENT_MAX_BYTES", 4)

        response = client.get(
            "/api/external/files/task-1/large.bin",
            headers={"Authorization": "Bearer test-key-123"},
        )

        assert response.status_code == 413
        assert storage.bodies[key][0].close_calls == 1
        assert storage.bodies[key][0].read_sizes == []

    @pytest.mark.parametrize(
        ("declared_size", "payload", "message"),
        [
            (5, b"abc", "shorter"),
            (3, b"abcd", "exceeded"),
        ],
    )
    async def test_content_length_mismatch_aborts_and_closes_body(
        self, client, mock_manager, declared_size, payload, message,
    ):
        from elastic_agent.api.routes import files as files_route

        key = "tasks/task-1/data.bin"
        storage = _TrackingStorage({key: (declared_size, payload)})
        mock_manager.file_storage = storage

        response = await files_route.read_file("task-1", "data.bin")
        with pytest.raises(
            files_route.ExternalStorageReadError, match=message,
        ):
            async for _chunk in response.body_iterator:
                pass

        assert storage.bodies[key][0].close_calls == 1

    async def test_stream_disconnect_closes_backend_body(
        self, client, mock_manager, monkeypatch,
    ):
        from elastic_agent.api.routes import files as files_route

        key = "tasks/task-1/data.bin"
        storage = _TrackingStorage({key: (8, b"abcdefgh")})
        mock_manager.file_storage = storage
        monkeypatch.setattr(files_route, "EXTERNAL_STREAM_CHUNK_BYTES", 4)
        admission = files_route._ExternalStreamAdmission(1)
        monkeypatch.setattr(
            files_route,
            "_EXTERNAL_STREAM_ADMISSION",
            admission,
        )

        response = await files_route.read_file("task-1", "data.bin")
        assert admission.active == 1
        assert await anext(response.body_iterator) == b"abcd"
        await response.body_iterator.aclose()

        assert storage.bodies[key][0].close_calls == 1
        assert admission.active == 0

    async def test_response_start_cancellation_still_closes_backend_body(
        self, client, mock_manager, monkeypatch,
    ):
        from elastic_agent.api.routes import files as files_route

        key = "tasks/task-1/data.bin"
        storage = _TrackingStorage({key: (4, b"data")})
        mock_manager.file_storage = storage
        admission = files_route._ExternalStreamAdmission(1)
        monkeypatch.setattr(
            files_route,
            "_EXTERNAL_STREAM_ADMISSION",
            admission,
        )
        response = await files_route.read_file("task-1", "data.bin")

        async def receive():
            return {"type": "http.disconnect"}

        async def send(message):
            if message["type"] == "http.response.start":
                raise asyncio.CancelledError

        with pytest.raises(asyncio.CancelledError):
            await response(
                {
                    "type": "http",
                    "asgi": {"version": "3.0", "spec_version": "2.4"},
                    "http_version": "1.1",
                    "method": "GET",
                    "scheme": "http",
                    "path": "/",
                    "raw_path": b"/",
                    "query_string": b"",
                    "headers": [],
                    "client": ("127.0.0.1", 1),
                    "server": ("127.0.0.1", 80),
                    "root_path": "",
                },
                receive,
                send,
            )

        assert storage.bodies[key][0].close_calls == 1
        assert admission.active == 0

    async def test_cancelled_slow_open_keeps_admission_until_body_is_closed(
        self, client, mock_manager, monkeypatch,
    ):
        from elastic_agent.api.routes import files as files_route

        open_started = threading.Event()
        allow_open = threading.Event()
        bodies: list[_TrackingBody] = []

        class SlowOpenStorage(StorageBackend):
            async def open_reader(
                self,
                oss_key: str,
                *,
                executor=None,
            ) -> StorageObjectReader:
                if oss_key.endswith("_sync_manifest.json"):
                    raise FileNotFoundError(oss_key)

                def blocking_open() -> StorageObjectReader:
                    open_started.set()
                    assert allow_open.wait(timeout=5)
                    body = _TrackingBody(b"x")
                    bodies.append(body)
                    return StorageObjectReader(
                        body,
                        size=1,
                        executor=executor,
                    )

                return await asyncio.get_running_loop().run_in_executor(
                    executor,
                    blocking_open,
                )

        admission = files_route._ExternalStreamAdmission(1)
        monkeypatch.setattr(
            files_route, "_EXTERNAL_STREAM_ADMISSION", admission,
        )
        mock_manager.file_storage = SlowOpenStorage()
        request = asyncio.create_task(
            files_route.read_file("task-1", "slow.bin")
        )
        assert await asyncio.to_thread(open_started.wait, 1)
        assert admission.active == 1

        request.cancel()
        await asyncio.sleep(0)
        assert not request.done()
        assert admission.try_acquire() is None

        allow_open.set()
        with pytest.raises(asyncio.CancelledError):
            await request

        assert len(bodies) == 1
        assert bodies[0].close_calls == 1
        assert admission.active == 0

    async def test_close_does_not_release_admission_before_read_thread_exits(
        self, client, mock_manager, monkeypatch,
    ):
        from elastic_agent.api.routes import files as files_route

        class SlowReadBody:
            def __init__(self) -> None:
                self.read_started = threading.Event()
                self.allow_read = threading.Event()
                self.close_called = threading.Event()
                self.close_calls = 0

            def read(self, _size=-1):
                self.read_started.set()
                assert self.allow_read.wait(timeout=5)
                return b"x"

            def close(self):
                # Deliberately return without interrupting the active read.
                self.close_calls += 1
                self.close_called.set()

        body = SlowReadBody()

        class SlowReadStorage(StorageBackend):
            async def open_reader(
                self,
                oss_key: str,
                *,
                executor=None,
            ) -> StorageObjectReader:
                if oss_key.endswith("_sync_manifest.json"):
                    raise FileNotFoundError(oss_key)
                return StorageObjectReader(
                    body,
                    size=1,
                    executor=executor,
                )

        admission = files_route._ExternalStreamAdmission(1)
        monkeypatch.setattr(
            files_route, "_EXTERNAL_STREAM_ADMISSION", admission,
        )
        mock_manager.file_storage = SlowReadStorage()
        response = await files_route.read_file("task-1", "slow.bin")
        read = asyncio.create_task(anext(response.body_iterator))
        assert await asyncio.to_thread(body.read_started.wait, 1)

        read.cancel()
        assert await asyncio.to_thread(body.close_called.wait, 1)
        assert body.close_calls == 1
        assert admission.active == 1
        assert admission.try_acquire() is None

        body.allow_read.set()
        with pytest.raises(asyncio.CancelledError):
            await read

        async def wait_for_release() -> None:
            while admission.active:
                await asyncio.sleep(0)

        await asyncio.wait_for(wait_for_release(), timeout=1)
        assert admission.active == 0

    async def test_hung_reads_can_be_interrupted_without_blocking_event_loop(
        self, client, mock_manager, monkeypatch,
    ):
        from elastic_agent.api.routes import files as files_route

        class InterruptibleBody:
            def __init__(self) -> None:
                self.read_started = threading.Event()
                self.closed = threading.Event()

            def read(self, _size=-1):
                self.read_started.set()
                self.closed.wait(timeout=5)
                return b""

            def close(self):
                self.closed.set()

        bodies: dict[str, InterruptibleBody] = {}

        class HungStorage(StorageBackend):
            async def open_reader(
                self,
                oss_key: str,
                *,
                executor=None,
            ) -> StorageObjectReader:
                if oss_key.endswith("_sync_manifest.json"):
                    raise FileNotFoundError(oss_key)
                body = InterruptibleBody()
                bodies[oss_key] = body
                return StorageObjectReader(
                    body,
                    size=1,
                    executor=executor,
                )

        admission = files_route._ExternalStreamAdmission(4)
        monkeypatch.setattr(
            files_route,
            "_EXTERNAL_STREAM_ADMISSION",
            admission,
        )
        mock_manager.file_storage = HungStorage()
        responses = [
            await files_route.read_file("task-1", f"data-{index}.bin")
            for index in range(4)
        ]
        reads = [
            asyncio.create_task(anext(response.body_iterator))
            for response in responses
        ]
        for body in bodies.values():
            assert await asyncio.to_thread(body.read_started.wait, 1)
        assert admission.active == 4

        for read in reads:
            read.cancel()
        # The event loop must remain responsive while close runs concurrently
        # with all four blocked SDK reads.
        await asyncio.wait_for(asyncio.sleep(0), timeout=0.1)
        await asyncio.wait_for(
            asyncio.gather(*reads, return_exceptions=True),
            timeout=1,
        )

        async def wait_for_cleanup() -> None:
            while admission.active:
                await asyncio.sleep(0.01)

        await asyncio.wait_for(wait_for_cleanup(), timeout=1)
        assert all(body.closed.is_set() for body in bodies.values())
        assert admission.active == 0

    async def test_presigned_url_intentionally_bypasses_manager_content_cap(
        self, client, mock_manager, monkeypatch,
    ):
        from elastic_agent.api.routes import files as files_route

        key = "tasks/task-1/huge.bin"
        storage = _TrackingStorage(
            {key: (100, b"")},
            presigned_url="https://storage.example/direct",
        )
        mock_manager.file_storage = storage
        monkeypatch.setattr(files_route, "EXTERNAL_CONTENT_MAX_BYTES", 1)

        response = client.get(
            "/api/external/files/task-1/huge.bin?mode=url",
            headers={"Authorization": "Bearer test-key-123"},
        )

        assert response.status_code == 200
        assert response.json()["url"] == "https://storage.example/direct"
        assert response.headers["cache-control"] == "no-store"
        assert response.headers["x-content-type-options"] == "nosniff"
        assert key not in storage.opened

    async def test_presign_saturation_is_fail_fast_before_backend_call(
        self, monkeypatch,
    ):
        from elastic_agent.api.routes import files as files_route

        storage = _TrackingStorage(
            {},
            presigned_url="https://storage.example/direct",
        )
        admission = files_route._ExternalStreamAdmission(1)
        monkeypatch.setattr(
            files_route, "_EXTERNAL_PRESIGN_ADMISSION", admission,
        )
        held = admission.try_acquire()
        assert held is not None
        try:
            with pytest.raises(HTTPException) as exc_info:
                await files_route._bounded_presigned_url(
                    storage, "tasks/task-1/file.bin",
                )
            assert exc_info.value.status_code == 503
            assert exc_info.value.headers == {"Retry-After": "1"}
        finally:
            held.release()
        assert admission.active == 0

    async def test_presign_timeout_retains_permit_until_backend_exits(
        self, monkeypatch,
    ):
        from elastic_agent.api.routes import files as files_route

        started = threading.Event()
        release = threading.Event()
        calls = 0

        class SlowPresignStorage(StorageBackend):
            async def get_presigned_url(
                self,
                oss_key: str,
                expires: int = 3600,
                *,
                executor=None,
            ) -> str | None:
                nonlocal calls
                calls += 1

                def blocking_presign() -> str:
                    started.set()
                    assert release.wait(timeout=5)
                    return "https://storage.example/direct"

                return await asyncio.get_running_loop().run_in_executor(
                    executor,
                    blocking_presign,
                )

        admission = files_route._ExternalStreamAdmission(1)
        monkeypatch.setattr(
            files_route, "_EXTERNAL_PRESIGN_ADMISSION", admission,
        )
        monkeypatch.setattr(
            files_route, "EXTERNAL_PRESIGN_TIMEOUT_SECONDS", 0.01,
        )
        storage = SlowPresignStorage()

        with pytest.raises(HTTPException) as exc_info:
            await files_route._bounded_presigned_url(
                storage, "tasks/task-1/file.bin",
            )
        assert exc_info.value.status_code == 504
        assert started.is_set()
        assert admission.active == 1

        with pytest.raises(HTTPException) as saturated:
            await files_route._bounded_presigned_url(
                storage, "tasks/task-1/second.bin",
            )
        assert saturated.value.status_code == 503
        assert calls == 1

        release.set()

        async def wait_for_release() -> None:
            while admission.active:
                await asyncio.sleep(0)

        await asyncio.wait_for(wait_for_release(), timeout=1)
        assert admission.active == 0


class TestGetManifest:

    async def test_get_manifest_success(self, client, storage_dir):
        task_dir = storage_dir / "tasks" / "task-1"
        task_dir.mkdir(parents=True)

        manifest = {
            "task_id": "task-1",
            "worker_id": "worker-1",
            "status": "idle",
            "updated_at": "2026-05-01T12:00:00Z",
            "files": [
                {
                    "path": "/root/work/ch01.md",
                    "oss_key": "tasks/task-1/ch01.md",
                    "size": 1234,
                    "md5": "abc123",
                    "content_type": "text/markdown",
                    "role": "manuscript",
                    "synced_at": "2026-05-01T12:00:00Z",
                }
            ],
        }
        (task_dir / "_sync_manifest.json").write_text(json.dumps(manifest))

        resp = client.get(
            "/api/external/files/task-1/manifest",
            headers={"Authorization": "Bearer test-key-123"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["task_id"] == "task-1"
        assert data["worker_id"] == "worker-1"
        assert len(data["files"]) == 1
        assert data["files"][0]["role"] == "manuscript"

    async def test_get_manifest_not_found(self, client):
        resp = client.get(
            "/api/external/files/nonexistent/manifest",
            headers={"Authorization": "Bearer test-key-123"},
        )
        assert resp.status_code == 404

    async def test_get_manifest_no_storage(self):
        os.environ["ELASTIC_AGENT_EXTERNAL_API_KEYS"] = "test-key-123"
        from elastic_agent.api.auth import reset_api_keys
        reset_api_keys()

        from fastapi import FastAPI

        from elastic_agent.api.routes.files import router

        app = FastAPI()
        app.include_router(router, prefix="/api")

        mgr = MagicMock()
        mgr.file_storage = None

        with patch("elastic_agent.api.routes.files._mgr", return_value=mgr):
            client = TestClient(app)
            resp = client.get(
                "/api/external/files/task-1/manifest",
                headers={"Authorization": "Bearer test-key-123"},
            )
            assert resp.status_code == 503

        os.environ.pop("ELASTIC_AGENT_EXTERNAL_API_KEYS", None)
        reset_api_keys()

    async def test_manifest_rejects_unsafe_task_id(self, client):
        resp = client.get(
            "/api/external/files/%2e%2e/manifest",
            headers={"Authorization": "Bearer test-key-123"},
        )
        assert resp.status_code == 400

    async def test_oversized_manifest_is_rejected_before_json_parse(
        self, client, mock_manager, monkeypatch,
    ):
        from elastic_agent.api.routes import files as files_route

        key = "tasks/task-1/_sync_manifest.json"
        storage = _TrackingStorage({key: (9, b"123456789")})
        mock_manager.file_storage = storage
        monkeypatch.setattr(files_route, "EXTERNAL_MANIFEST_MAX_BYTES", 8)

        response = client.get(
            "/api/external/files/task-1/manifest",
            headers={"Authorization": "Bearer test-key-123"},
        )

        assert response.status_code == 413
        assert storage.bodies[key][0].close_calls == 1
        assert storage.bodies[key][0].read_sizes == []

    async def test_manifest_entry_count_is_bounded(
        self, client, storage_dir, monkeypatch,
    ):
        from elastic_agent.api.routes import files as files_route

        task_dir = storage_dir / "tasks" / "task-1"
        task_dir.mkdir(parents=True)
        (task_dir / "_sync_manifest.json").write_text(json.dumps({
            "task_id": "task-1",
            "worker_id": "worker",
            "files": [{}, {}],
        }))
        monkeypatch.setattr(files_route, "EXTERNAL_MANIFEST_MAX_FILES", 1)

        response = client.get(
            "/api/external/files/task-1/manifest",
            headers={"Authorization": "Bearer test-key-123"},
        )

        assert response.status_code == 413

    @pytest.mark.parametrize("unsafe_text", ["line\u2028break", "\u200b", "\ud800"])
    async def test_manifest_rejects_nonprintable_unicode_fields(
        self, client, storage_dir, unsafe_text,
    ):
        task_dir = storage_dir / "tasks" / "task-1"
        task_dir.mkdir(parents=True)
        (task_dir / "_sync_manifest.json").write_text(json.dumps({
            "task_id": "task-1",
            "worker_id": unsafe_text,
            "files": [],
        }))

        response = client.get(
            "/api/external/files/task-1/manifest",
            headers={"Authorization": "Bearer test-key-123"},
        )

        assert response.status_code == 503

    @pytest.mark.parametrize(
        ("declared_size", "payload"),
        [(10, b"{}"), (1, b"{}")],
    )
    async def test_manifest_length_mismatch_is_503_and_closes_body(
        self, client, mock_manager, declared_size, payload,
    ):
        key = "tasks/task-1/_sync_manifest.json"
        storage = _TrackingStorage({key: (declared_size, payload)})
        mock_manager.file_storage = storage

        response = client.get(
            "/api/external/files/task-1/manifest",
            headers={"Authorization": "Bearer test-key-123"},
        )

        assert response.status_code == 503
        assert storage.bodies[key][0].close_calls == 1

    async def test_cancelled_manifest_reads_hold_admission_until_close(
        self, client, mock_manager, monkeypatch,
    ):
        from elastic_agent.api.routes import files as files_route

        class InterruptibleManifestBody:
            def __init__(self) -> None:
                self.read_started = threading.Event()
                self.closed = threading.Event()

            def read(self, _size=-1):
                self.read_started.set()
                self.closed.wait(timeout=5)
                return b""

            def close(self):
                self.closed.set()

        bodies: list[InterruptibleManifestBody] = []

        class HungManifestStorage(StorageBackend):
            async def open_reader(
                self,
                _oss_key: str,
                *,
                executor=None,
            ) -> StorageObjectReader:
                body = InterruptibleManifestBody()
                bodies.append(body)
                return StorageObjectReader(
                    body,
                    size=1,
                    executor=executor,
                )

        admission = files_route._ExternalStreamAdmission(4)
        monkeypatch.setattr(
            files_route,
            "_EXTERNAL_STREAM_ADMISSION",
            admission,
        )
        mock_manager.file_storage = HungManifestStorage()
        requests = [
            asyncio.create_task(files_route.get_manifest(f"task-{index}"))
            for index in range(4)
        ]
        while len(bodies) < 4:
            await asyncio.sleep(0)
        for body in bodies:
            assert await asyncio.to_thread(body.read_started.wait, 1)
        assert admission.active == 4

        for request in requests:
            request.cancel()
        await asyncio.wait_for(asyncio.sleep(0), timeout=0.1)
        await asyncio.wait_for(
            asyncio.gather(*requests, return_exceptions=True),
            timeout=1,
        )

        assert all(body.closed.is_set() for body in bodies)
        assert admission.active == 0
        assert files_route._EXTERNAL_FILE_EXECUTOR._work_queue.qsize() == 0
