"""T-120: File sync end-to-end integration test.

Tests: Worker file change → FileSyncManager → OSS/S3 → External API read.
Simulates the full file sync pipeline using MockOSS.
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
class TestFileSyncE2E:
    """Worker file changes → OSS → API readback."""

    @pytest.mark.asyncio
    async def test_file_synced_event_received(self, running_server):
        srv = running_server
        nodes = await srv["manager"].scale_out(count=1)
        node = nodes[0]

        synced_events: list[dict] = []
        srv["manager"].event_bus.subscribe(
            "FILE_SYNCED", lambda et, wid, d: synced_events.append(d)
        )

        worker = await connect_mock_worker(srv["ws_url"], node.auth_token, node.node_id)
        try:
            await worker.send_raw({
                "type": "FILE_SYNCED",
                "task_id": "sync-task-1",
                "path": "/workspace/output.txt",
                "oss_key": "tasks/sync-task-1/output.txt",
                "synced_at": "2025-01-15T10:00:00Z",
                "md5": "abc123",
            })
            await asyncio.sleep(0.3)

            assert len(synced_events) == 1
            assert synced_events[0]["path"] == "/workspace/output.txt"
            assert synced_events[0]["oss_key"] == "tasks/sync-task-1/output.txt"
        finally:
            await worker.disconnect()

    @pytest.mark.asyncio
    async def test_multiple_files_synced(self, running_server):
        srv = running_server
        nodes = await srv["manager"].scale_out(count=1)
        node = nodes[0]

        synced: list[dict] = []
        srv["manager"].event_bus.subscribe("FILE_SYNCED", lambda et, wid, d: synced.append(d))

        worker = await connect_mock_worker(srv["ws_url"], node.auth_token, node.node_id)
        try:
            for i in range(5):
                await worker.send_raw({
                    "type": "FILE_SYNCED",
                    "task_id": "multi-sync",
                    "path": f"/workspace/file_{i}.txt",
                    "oss_key": f"tasks/multi-sync/file_{i}.txt",
                    "synced_at": f"2025-01-15T10:0{i}:00Z",
                    "md5": f"md5-{i}",
                })
            await asyncio.sleep(0.5)

            assert len(synced) == 5
            paths = {e["path"] for e in synced}
            assert len(paths) == 5
        finally:
            await worker.disconnect()

    @pytest.mark.asyncio
    async def test_synced_file_readable_via_api(self, tmp_path, api_keys):
        tm = create_test_manager(tmp_dir=tmp_path)
        app = create_app(tm.manager)

        file_content = b"This is the synced manuscript content"
        await tm.oss.upload_bytes(file_content, "tasks/read-sync/manuscript.txt")

        manifest = {
            "task_id": "read-sync",
            "worker_id": "w1",
            "status": "synced",
            "updated_at": "2025-01-15T12:00:00Z",
            "files": [
                {
                    "path": "manuscript.txt",
                    "oss_key": "tasks/read-sync/manuscript.txt",
                    "size": len(file_content),
                    "md5": "deadbeef",
                    "synced_at": "2025-01-15T12:00:00Z",
                    "role": "manuscript",
                }
            ],
        }
        await tm.oss.upload_bytes(
            json.dumps(manifest).encode(),
            "tasks/read-sync/_sync_manifest.json",
        )

        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://testserver",
            headers={"Authorization": f"Bearer {TEST_API_KEY}"},
        ) as client:
            await tm.manager.start()

            resp = await client.get("/api/external/files/read-sync/manuscript.txt")
            assert resp.status_code == 200
            assert resp.content == file_content

            manifest_resp = await client.get("/api/external/files/read-sync/manifest")
            assert manifest_resp.status_code == 200
            mdata = manifest_resp.json()
            assert mdata["task_id"] == "read-sync"
            assert len(mdata["files"]) == 1
            assert mdata["files"][0]["role"] == "manuscript"

            await tm.manager.stop()

    @pytest.mark.asyncio
    async def test_sync_mapping_message_received(self, running_server):
        srv = running_server
        nodes = await srv["manager"].scale_out(count=1)
        node = nodes[0]

        worker = await connect_mock_worker(srv["ws_url"], node.auth_token, node.node_id)
        try:
            await srv["manager"].connection_manager.register_sync_mapping(
                worker_id=node.node_id,
                task_id="map-task-1",
                book_slug="test-book-slug",
                oss_prefix="tasks/map-task-1/",
                watch_paths=["/workspace/delivery"],
                session_path_hash="abc123",
            )
            await asyncio.sleep(0.2)

            received = worker.messages_received
            mapping_msgs = [m for m in received if m.get("type") == "REGISTER_SYNC_MAPPING"]
            assert len(mapping_msgs) == 1
            assert mapping_msgs[0]["task_id"] == "map-task-1"
            assert mapping_msgs[0]["book_slug"] == "test-book-slug"
        finally:
            await worker.disconnect()

    @pytest.mark.asyncio
    async def test_unregister_sync_mapping(self, running_server):
        srv = running_server
        nodes = await srv["manager"].scale_out(count=1)
        node = nodes[0]

        worker = await connect_mock_worker(srv["ws_url"], node.auth_token, node.node_id)
        try:
            await srv["manager"].connection_manager.unregister_sync_mapping(
                worker_id=node.node_id,
                task_id="unmap-task",
            )
            await asyncio.sleep(0.2)

            received = worker.messages_received
            unmap_msgs = [m for m in received if m.get("type") == "UNREGISTER_SYNC_MAPPING"]
            assert len(unmap_msgs) == 1
            assert unmap_msgs[0]["task_id"] == "unmap-task"
        finally:
            await worker.disconnect()
