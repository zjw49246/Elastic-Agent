"""T-111 / T-112: Cloud provider full lifecycle integration tests.

T-111: Aliyun lifecycle — create → bootstrap → ready → execute → terminate
T-112: AWS lifecycle — create → bootstrap → ready → execute → terminate

Both tests follow the same pattern but use level2 markers requiring real
cloud credentials. At level1 they run against DryRunProvider to verify
the lifecycle orchestration logic.
"""

from __future__ import annotations

import asyncio
import os

import pytest

from elastic_agent.core.registry import NodeStatus
from elastic_agent.testing import MockWorker

from .conftest import connect_mock_worker


@pytest.mark.level1
class TestDryRunLifecycle:
    """Full lifecycle with DryRunProvider — verifies orchestration logic."""

    @pytest.mark.asyncio
    async def test_create_bootstrap_ready_execute_terminate(self, running_server):
        srv = running_server

        # 1. Create
        nodes = await srv["manager"].scale_out(count=1)
        assert len(nodes) == 1
        node = nodes[0]
        assert node.status == NodeStatus.CREATING

        creates = srv["provider"].get_operations("create")
        assert len(creates) == 1

        # 2. Connect (simulates bootstrap completion → READY)
        worker = await connect_mock_worker(srv["ws_url"], node.auth_token, node.node_id)
        try:
            status = await srv["manager"].registry.get(node.node_id)
            assert status.status == NodeStatus.READY
            assert srv["manager"].connection_manager.is_connected(node.node_id)

            # 3. Execute
            worker.set_script("lifecycle-task", [
                '{"type":"system","model":"claude-opus-4-6"}',
                '{"type":"assistant","content":"Done!"}',
                '{"type":"result","session_id":"s-lc","cost_usd":0.1}',
            ])
            exit_events: list[dict] = []
            sub_id = srv["manager"].event_bus.subscribe(
                "PROCESS_EXIT", lambda et, wid, d: exit_events.append(d)
            )

            await srv["manager"].connection_manager.execute(
                worker_id=node.node_id,
                task_id="lifecycle-task",
                command=["claude", "--help"],
            )
            await asyncio.sleep(0.5)
            assert len(exit_events) == 1

            srv["manager"].event_bus.unsubscribe(sub_id)
        finally:
            await worker.disconnect()

        await asyncio.sleep(0.2)

        # 4. Terminate
        terminated = await srv["manager"].scale_in([node.node_id], force=True)
        assert node.node_id in terminated

        term_ops = srv["provider"].get_operations("terminate")
        assert len(term_ops) == 1

        final = await srv["manager"].registry.get(node.node_id)
        assert final.status == NodeStatus.TERMINATED

    @pytest.mark.asyncio
    async def test_multiple_nodes_lifecycle(self, running_server):
        srv = running_server

        nodes = await srv["manager"].scale_out(count=3)
        assert len(nodes) == 3

        workers = []
        for node in nodes:
            w = await connect_mock_worker(srv["ws_url"], node.auth_token, node.node_id)
            workers.append(w)

        assert len(srv["manager"].connection_manager.connected_workers) == 3

        for w in workers:
            await w.disconnect()
        await asyncio.sleep(0.3)

        node_ids = [n.node_id for n in nodes]
        terminated = await srv["manager"].scale_in(node_ids, force=True)
        assert len(terminated) == 3


@pytest.mark.level2
class TestAliyunLifecycle:
    """T-111: Aliyun full lifecycle — requires real credentials."""

    @pytest.mark.asyncio
    async def test_aliyun_create_and_terminate(self, running_server):
        if not os.environ.get("ALICLOUD_ACCESS_KEY_ID"):
            pytest.skip("ALICLOUD credentials not available")

        srv = running_server
        nodes = await srv["manager"].scale_out(count=1)
        assert len(nodes) == 1
        node = nodes[0]

        await asyncio.sleep(2)

        cloud_instances = await srv["provider"].list_instances()
        instance_ids = {i.instance_id for i in cloud_instances}
        assert node.instance_id in instance_ids

        await srv["manager"].scale_in([node.node_id], force=True)


@pytest.mark.level2
class TestAWSLifecycle:
    """T-112: AWS full lifecycle — requires real credentials."""

    @pytest.mark.asyncio
    async def test_aws_create_and_terminate(self, running_server):
        if not os.environ.get("AWS_ACCESS_KEY_ID"):
            pytest.skip("AWS credentials not available")

        srv = running_server
        nodes = await srv["manager"].scale_out(count=1)
        assert len(nodes) == 1
        node = nodes[0]

        await asyncio.sleep(2)

        cloud_instances = await srv["provider"].list_instances()
        instance_ids = {i.instance_id for i in cloud_instances}
        assert node.instance_id in instance_ids

        await srv["manager"].scale_in([node.node_id], force=True)
