"""T-122: Worker log sync end-to-end integration test.

Tests: Worker log persistence → FileSyncManager → OSS → history query.
Verifies that log events are captured, stored, and queryable.
"""

from __future__ import annotations

import asyncio
import json

import pytest
from httpx import ASGITransport, AsyncClient

from elastic_agent.api.app import create_app
from elastic_agent.api.auth import reset_api_keys
from elastic_agent.testing import create_test_manager

from .conftest import TEST_API_KEY, connect_mock_worker


@pytest.fixture
def api_keys(monkeypatch):
    monkeypatch.setenv("ELASTIC_AGENT_EXTERNAL_API_KEYS", TEST_API_KEY)
    reset_api_keys()
    yield
    reset_api_keys()


@pytest.mark.level1
class TestLogSyncE2E:
    """Worker log → OSS → query pipeline."""

    @pytest.mark.asyncio
    async def test_log_events_captured_in_parser(self, running_server):
        srv = running_server
        nodes = await srv["manager"].scale_out(count=1)
        node = nodes[0]

        worker = await connect_mock_worker(srv["ws_url"], node.auth_token, node.node_id)
        worker.set_script("log-cap-1", [
            '{"type":"system","model":"claude-opus-4-6","tools":["Read","Write"]}',
            '{"type":"assistant","content":"Reading the file..."}',
            '{"type":"tool_use","name":"Read","id":"tu-1","input":{"path":"/test.txt"}}',
            '{"type":"tool_result","id":"tu-1","output":"file contents here"}',
            '{"type":"assistant","content":"Done processing"}',
            '{"type":"result","session_id":"s-log-cap","cost_usd":1.5,"duration_ms":8000}',
        ])

        try:
            await srv["manager"].connection_manager.execute(
                worker_id=node.node_id,
                task_id="log-cap-1",
                command=["claude", "run"],
            )
            await asyncio.sleep(0.5)

            logs = srv["manager"].log_event_parser.get_task_logs("log-cap-1")
            assert len(logs) >= 6

            session = srv["manager"].log_event_parser.get_task_session("log-cap-1")
            assert session.session_id == "s-log-cap"
            assert session.total_cost_usd == 1.5
        finally:
            await worker.disconnect()

    @pytest.mark.asyncio
    async def test_log_type_filtering(self, running_server):
        srv = running_server
        nodes = await srv["manager"].scale_out(count=1)
        node = nodes[0]

        worker = await connect_mock_worker(srv["ws_url"], node.auth_token, node.node_id)
        worker.set_script("log-filter", [
            '{"type":"system","model":"claude"}',
            '{"type":"assistant","content":"Line 1"}',
            '{"type":"tool_use","name":"Bash","id":"t1"}',
            '{"type":"tool_result","id":"t1","output":"ok"}',
            '{"type":"assistant","content":"Line 2"}',
            '{"type":"result","session_id":"s-filt","cost_usd":0.3}',
        ])

        try:
            await srv["manager"].connection_manager.execute(
                worker_id=node.node_id,
                task_id="log-filter",
                command=["claude", "run"],
            )
            await asyncio.sleep(0.5)

            assistants = srv["manager"].log_event_parser.get_task_logs(
                "log-filter", types=["assistant"]
            )
            results = srv["manager"].log_event_parser.get_task_logs(
                "log-filter", types=["result"]
            )
            assert len(assistants) >= 2
            assert len(results) >= 1
        finally:
            await worker.disconnect()

    @pytest.mark.asyncio
    async def test_log_stored_in_oss_readable_via_api(self, tmp_path, api_keys):
        tm = create_test_manager(tmp_dir=tmp_path)
        app = create_app(tm.manager)

        log_lines = [
            {"type": "system", "model": "claude-opus-4-6"},
            {"type": "assistant", "content": "Working..."},
            {"type": "result", "session_id": "s-oss-log", "cost_usd": 0.8},
        ]
        log_content = "\n".join(json.dumps(l) for l in log_lines)
        await tm.oss.upload_bytes(
            log_content.encode(),
            "tasks/oss-log-task/logs/process.ndjson",
        )

        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://testserver",
            headers={"Authorization": f"Bearer {TEST_API_KEY}"},
        ) as client:
            await tm.manager.start()

            resp = await client.get("/api/external/files/oss-log-task/logs/process.ndjson")
            assert resp.status_code == 200
            lines = resp.text.strip().split("\n")
            assert len(lines) == 3

            parsed = [json.loads(l) for l in lines]
            assert parsed[0]["type"] == "system"
            assert parsed[2]["session_id"] == "s-oss-log"

            await tm.manager.stop()

    @pytest.mark.asyncio
    async def test_multiple_tasks_isolated_logs(self, running_server):
        srv = running_server
        nodes = await srv["manager"].scale_out(count=1)
        node = nodes[0]

        worker = await connect_mock_worker(srv["ws_url"], node.auth_token, node.node_id)
        worker.set_script("iso-task-a", [
            '{"type":"result","session_id":"sa","cost_usd":1.0}',
        ])
        worker.set_script("iso-task-b", [
            '{"type":"result","session_id":"sb","cost_usd":2.0}',
        ])

        try:
            await srv["manager"].connection_manager.execute(
                worker_id=node.node_id, task_id="iso-task-a", command=["a"]
            )
            await asyncio.sleep(0.3)

            await srv["manager"].connection_manager.execute(
                worker_id=node.node_id, task_id="iso-task-b", command=["b"]
            )
            await asyncio.sleep(0.3)

            logs_a = srv["manager"].log_event_parser.get_task_logs("iso-task-a")
            logs_b = srv["manager"].log_event_parser.get_task_logs("iso-task-b")

            sa = srv["manager"].log_event_parser.get_task_session("iso-task-a")
            sb = srv["manager"].log_event_parser.get_task_session("iso-task-b")

            assert sa.session_id == "sa"
            assert sb.session_id == "sb"
            assert sa.total_cost_usd == 1.0
            assert sb.total_cost_usd == 2.0
        finally:
            await worker.disconnect()

    @pytest.mark.asyncio
    async def test_non_json_log_lines_handled(self, running_server):
        srv = running_server
        nodes = await srv["manager"].scale_out(count=1)
        node = nodes[0]

        worker = await connect_mock_worker(srv["ws_url"], node.auth_token, node.node_id)
        try:
            await worker.send_log("raw-task", "stderr", "plain text error line")
            await worker.send_log(
                "raw-task", "stdout",
                '{"type":"result","session_id":"s-raw","cost_usd":0.1}',
                parsed={"type": "result", "session_id": "s-raw", "cost_usd": 0.1},
            )
            await asyncio.sleep(0.3)

            logs = srv["manager"].log_event_parser.get_task_logs("raw-task")
            assert len(logs) >= 2
        finally:
            await worker.disconnect()
