"""Unit tests for external files API (T-033)."""

from __future__ import annotations

import json
import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from elastic_agent.worker.file_sync import LocalBackend


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

    from elastic_agent.api.routes.files import router
    from fastapi import FastAPI

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

    async def test_read_file_requires_auth(self, client):
        resp = client.get("/api/external/files/task-1/file.txt")
        assert resp.status_code == 401


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

        from elastic_agent.api.routes.files import router
        from fastapi import FastAPI

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
