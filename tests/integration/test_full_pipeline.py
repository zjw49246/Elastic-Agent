"""T-115: Full pipeline integration test.

Tests the complete flow: scale out → execute command → get output → scale down.
"""

from __future__ import annotations

import asyncio
import json

import pytest

from elastic_agent.core.registry import NodeStatus

from .conftest import connect_mock_worker


@pytest.mark.level1
class TestFullPipeline:
    """Scale out → execute → output → scale down."""

    @pytest.mark.asyncio
    async def test_single_task_full_flow(self, running_server):
        srv = running_server

        # 1. Scale out
        nodes = await srv["manager"].scale_out(count=1)
        assert len(nodes) == 1
        node = nodes[0]

        # 2. Connect worker
        worker = await connect_mock_worker(srv["ws_url"], node.auth_token, node.node_id)
        worker.set_script("pipeline-1", [
            '{"type":"system","model":"claude-opus-4-6","tools":[]}',
            '{"type":"assistant","content":"Processing manuscript..."}',
            '{"type":"tool_use","name":"Write","id":"tu-1"}',
            '{"type":"tool_result","id":"tu-1","output":"file written"}',
            '{"type":"result","session_id":"s-pipeline","cost_usd":2.5,"duration_ms":10000}',
        ])

        log_events: list[dict] = []
        exit_events: list[dict] = []
        srv["manager"].event_bus.subscribe(
            "LOG", lambda et, wid, d: log_events.append(d)
        )
        srv["manager"].event_bus.subscribe(
            "PROCESS_EXIT", lambda et, wid, d: exit_events.append(d)
        )

        try:
            # 3. Execute command
            await srv["manager"].connection_manager.execute(
                worker_id=node.node_id,
                task_id="pipeline-1",
                command=["claude", "--dangerously-skip-permissions", "/audiobook", "test-book"],
                cwd="/workspace",
            )
            await asyncio.sleep(0.5)

            # 4. Verify output captured
            assert len(log_events) >= 5
            assert len(exit_events) == 1
            assert exit_events[0]["exit_code"] == 0

            session = srv["manager"].log_event_parser.get_task_session("pipeline-1")
            assert session.session_id == "s-pipeline"
            assert session.total_cost_usd == 2.5
        finally:
            await worker.disconnect()

        await asyncio.sleep(0.2)

        # 5. Scale down
        terminated = await srv["manager"].scale_in([node.node_id], force=True)
        assert node.node_id in terminated
        final = await srv["manager"].registry.get(node.node_id)
        assert final.status == NodeStatus.TERMINATED

    @pytest.mark.asyncio
    async def test_failed_task_full_flow(self, running_server):
        srv = running_server

        nodes = await srv["manager"].scale_out(count=1)
        node = nodes[0]

        worker = await connect_mock_worker(srv["ws_url"], node.auth_token, node.node_id)
        worker.set_script("pipeline-fail", [
            '{"type":"system","model":"claude-opus-4-6"}',
            '{"type":"assistant","content":"Error occurred"}',
        ], exit_code=1)

        exit_events: list[dict] = []
        srv["manager"].event_bus.subscribe(
            "PROCESS_EXIT", lambda et, wid, d: exit_events.append(d)
        )

        try:
            await srv["manager"].connection_manager.execute(
                worker_id=node.node_id,
                task_id="pipeline-fail",
                command=["claude", "run"],
            )
            await asyncio.sleep(0.3)

            assert len(exit_events) == 1
            assert exit_events[0]["exit_code"] == 1
        finally:
            await worker.disconnect()

        await srv["manager"].scale_in([node.node_id], force=True)

    @pytest.mark.asyncio
    async def test_sequential_tasks_on_same_worker(self, running_server):
        srv = running_server

        nodes = await srv["manager"].scale_out(count=1)
        node = nodes[0]

        worker = await connect_mock_worker(srv["ws_url"], node.auth_token, node.node_id)
        worker.set_script("seq-1", [
            '{"type":"result","session_id":"s1","cost_usd":1.0}',
        ])
        worker.set_script("seq-2", [
            '{"type":"result","session_id":"s2","cost_usd":2.0}',
        ])

        try:
            await srv["manager"].connection_manager.execute(
                worker_id=node.node_id, task_id="seq-1", command=["task1"]
            )
            await asyncio.sleep(0.3)

            await srv["manager"].connection_manager.execute(
                worker_id=node.node_id, task_id="seq-2", command=["task2"]
            )
            await asyncio.sleep(0.3)

            s1 = srv["manager"].log_event_parser.get_task_session("seq-1")
            s2 = srv["manager"].log_event_parser.get_task_session("seq-2")
            assert s1.session_id == "s1"
            assert s2.session_id == "s2"
            assert s1.total_cost_usd == 1.0
            assert s2.total_cost_usd == 2.0
        finally:
            await worker.disconnect()

        await srv["manager"].scale_in([node.node_id], force=True)

    @pytest.mark.asyncio
    async def test_drain_before_scale_down(self, running_server):
        srv = running_server

        nodes = await srv["manager"].scale_out(count=1)
        node = nodes[0]

        worker = await connect_mock_worker(srv["ws_url"], node.auth_token, node.node_id)
        try:
            drained = await srv["manager"].drain_node(node.node_id)
            assert drained

            status = await srv["manager"].registry.get(node.node_id)
            assert status.status == NodeStatus.DRAINING
        finally:
            await worker.disconnect()

        terminated = await srv["manager"].scale_in([node.node_id], force=True)
        assert node.node_id in terminated

    @pytest.mark.asyncio
    async def test_remove_node_cleans_everything(self, running_server):
        srv = running_server

        nodes = await srv["manager"].scale_out(count=1)
        node = nodes[0]

        worker = await connect_mock_worker(srv["ws_url"], node.auth_token, node.node_id)
        await srv["manager"].task_registry.register("rm-task", node.node_id, {})

        try:
            removed = await srv["manager"].remove_node(node.node_id)
            assert removed
        finally:
            try:
                await worker.disconnect()
            except Exception:
                pass

        assert await srv["manager"].registry.get(node.node_id) is None

        task = await srv["manager"].task_registry.get("rm-task")
        assert task.status.value == "worker_lost"
