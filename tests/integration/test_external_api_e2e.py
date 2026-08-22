"""T-114: External API E2E integration tests.

Tests: trace stream subscription, file read from storage, API authentication.
Verifies that the external API layer works end-to-end with Manager components.
"""

from __future__ import annotations

import asyncio
import json

import pytest
from httpx import ASGITransport, AsyncClient

from elastic_agent.api.app import create_app
from elastic_agent.api.auth import reset_api_keys
from elastic_agent.testing import MockOSS, create_test_manager

from .conftest import TEST_API_KEY, connect_mock_worker


@pytest.fixture
def api_keys(monkeypatch):
    monkeypatch.setenv("ELASTIC_AGENT_EXTERNAL_API_KEYS", TEST_API_KEY)
    reset_api_keys()
    yield
    reset_api_keys()


@pytest.mark.level1
class TestExternalAPIAuth:
    """API authentication via Bearer token."""

    @pytest.mark.asyncio
    async def test_valid_key_accepted(self, tmp_path, api_keys):
        tm = create_test_manager(tmp_dir=tmp_path)
        app = create_app(tm.manager)
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://testserver",
            headers={"Authorization": f"Bearer {TEST_API_KEY}"},
        ) as client:
            await tm.manager.start()
            resp = await client.get("/api/health")
            assert resp.status_code == 200
            await tm.manager.stop()

    @pytest.mark.asyncio
    async def test_invalid_key_rejected(self, tmp_path, api_keys):
        tm = create_test_manager(tmp_dir=tmp_path)
        app = create_app(tm.manager)
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://testserver",
            headers={"Authorization": "Bearer wrong-key"},
        ) as client:
            await tm.manager.start()
            resp = await client.get("/api/nodes")
            assert resp.status_code == 401
            await tm.manager.stop()

    @pytest.mark.asyncio
    async def test_missing_key_rejected(self, tmp_path, api_keys):
        tm = create_test_manager(tmp_dir=tmp_path)
        app = create_app(tm.manager)
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://testserver",
        ) as client:
            await tm.manager.start()
            resp = await client.get("/api/nodes")
            assert resp.status_code == 401
            await tm.manager.stop()


@pytest.mark.level1
class TestTraceStream:
    """Trace stream subscription via LOG events flowing to LogEventParser."""

    @pytest.mark.asyncio
    async def test_log_events_stored_in_parser(self, running_server):
        srv = running_server
        nodes = await srv["manager"].scale_out(count=1)
        node = nodes[0]

        worker = await connect_mock_worker(srv["ws_url"], node.auth_token, node.node_id)
        worker.set_script("trace-task", [
            '{"type":"system","model":"claude-opus-4-6"}',
            '{"type":"assistant","content":"Generating output..."}',
            '{"type":"result","session_id":"s-trace","cost_usd":0.5}',
        ])

        try:
            await srv["manager"].connection_manager.execute(
                worker_id=node.node_id,
                task_id="trace-task",
                command=["claude", "run"],
            )
            await asyncio.sleep(0.5)

            logs = srv["manager"].log_event_parser.get_task_logs("trace-task")
            assert len(logs) >= 3

            session = srv["manager"].log_event_parser.get_task_session("trace-task")
            assert session is not None
            assert session.session_id == "s-trace"
            assert session.total_cost_usd == 0.5
        finally:
            await worker.disconnect()

    @pytest.mark.asyncio
    async def test_filtered_trace_by_type(self, running_server):
        srv = running_server
        nodes = await srv["manager"].scale_out(count=1)
        node = nodes[0]

        worker = await connect_mock_worker(srv["ws_url"], node.auth_token, node.node_id)
        worker.set_script("filter-task", [
            '{"type":"system","model":"claude"}',
            '{"type":"assistant","content":"Hello"}',
            '{"type":"tool_use","name":"Read","id":"t1"}',
            '{"type":"result","session_id":"s-filter","cost_usd":0.2}',
        ])

        try:
            await srv["manager"].connection_manager.execute(
                worker_id=node.node_id,
                task_id="filter-task",
                command=["claude", "run"],
            )
            await asyncio.sleep(0.5)

            assistant_logs = srv["manager"].log_event_parser.get_task_logs(
                "filter-task", types=["assistant"]
            )
            assert len(assistant_logs) >= 1
            for log in assistant_logs:
                parsed = log.get("parsed", {})
                assert parsed.get("type") == "assistant"
        finally:
            await worker.disconnect()


@pytest.mark.level1
class TestFileReadFromStorage:
    """File read from cloud storage via External API."""

    @pytest.mark.asyncio
    async def test_read_file_from_oss(self, tmp_path, api_keys):
        tm = create_test_manager(tmp_dir=tmp_path)
        app = create_app(tm.manager)

        oss_key = "tasks/task-file-1/output.txt"
        await tm.oss.upload_bytes(b"Hello from OSS", oss_key)

        manifest = {
            "task_id": "task-file-1",
            "worker_id": "w1",
            "status": "syncing",
            "updated_at": "2025-01-01T00:00:00Z",
            "files": [
                {
                    "path": "output.txt",
                    "oss_key": oss_key,
                    "size": 14,
                    "md5": "abc123",
                    "synced_at": "2025-01-01T00:00:00Z",
                }
            ],
        }
        await tm.oss.upload_bytes(
            json.dumps(manifest).encode(),
            "tasks/task-file-1/_sync_manifest.json",
        )

        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://testserver",
            headers={"Authorization": f"Bearer {TEST_API_KEY}"},
        ) as client:
            await tm.manager.start()

            resp = await client.get("/api/external/files/task-file-1/output.txt")
            assert resp.status_code == 200
            assert resp.text == "Hello from OSS"
            assert resp.headers.get("X-Synced-At") == "2025-01-01T00:00:00Z"

            await tm.manager.stop()

    @pytest.mark.asyncio
    async def test_read_manifest(self, tmp_path, api_keys):
        tm = create_test_manager(tmp_dir=tmp_path)
        app = create_app(tm.manager)

        manifest = {
            "task_id": "task-man-1",
            "worker_id": "w-man",
            "status": "synced",
            "updated_at": "2025-02-01T12:00:00Z",
            "files": [
                {"path": "a.txt", "oss_key": "tasks/task-man-1/a.txt", "size": 10},
                {"path": "b.md", "oss_key": "tasks/task-man-1/b.md", "size": 20},
            ],
        }
        await tm.oss.upload_bytes(
            json.dumps(manifest).encode(),
            "tasks/task-man-1/_sync_manifest.json",
        )

        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://testserver",
            headers={"Authorization": f"Bearer {TEST_API_KEY}"},
        ) as client:
            await tm.manager.start()

            resp = await client.get("/api/external/files/task-man-1/manifest")
            assert resp.status_code == 200
            data = resp.json()
            assert data["task_id"] == "task-man-1"
            assert data["worker_id"] == "w-man"
            assert len(data["files"]) == 2

            await tm.manager.stop()

    @pytest.mark.asyncio
    async def test_file_not_found_returns_404(self, tmp_path, api_keys):
        tm = create_test_manager(tmp_dir=tmp_path)
        app = create_app(tm.manager)

        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://testserver",
            headers={"Authorization": f"Bearer {TEST_API_KEY}"},
        ) as client:
            await tm.manager.start()

            resp = await client.get("/api/external/files/no-task/missing.txt")
            assert resp.status_code == 404

            await tm.manager.stop()
