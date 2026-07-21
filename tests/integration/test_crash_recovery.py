"""T-129: Manager crash recovery integration test.

Tests: Manager restart → NodeRegistry rebuild → Worker reconnect → state consistent.
Verifies that persistent state (JSON files) survives a Manager restart cycle.
"""

from __future__ import annotations

import asyncio

import pytest

from elastic_agent.core.registry import NodeRecord, NodeRegistry, NodeStatus
from elastic_agent.core.task_registry import TaskRegistry, TaskStatus
from elastic_agent.testing import create_test_config

from .conftest import connect_mock_worker


@pytest.mark.level1
class TestCrashRecovery:
    """Manager crash recovery via persistent state."""

    @pytest.mark.asyncio
    async def test_registry_survives_restart(self, tmp_path):
        config, tmp_dir = create_test_config(tmp_path)

        reg1 = NodeRegistry(config.registry.path)
        await reg1.load()
        await reg1.add(NodeRecord(
            node_id="n-survive-1",
            instance_id="dryrun:i-001",
            platform="dryrun",
            status=NodeStatus.READY,
            public_ip="10.0.0.1",
            auth_token="token-1",
        ))
        await reg1.add(NodeRecord(
            node_id="n-survive-2",
            instance_id="dryrun:i-002",
            platform="dryrun",
            status=NodeStatus.BUSY,
            public_ip="10.0.0.2",
            auth_token="token-2",
        ))

        reg2 = NodeRegistry(config.registry.path)
        await reg2.load()

        all_nodes = await reg2.list_all()
        assert len(all_nodes) == 2
        ids = {n.node_id for n in all_nodes}
        assert ids == {"n-survive-1", "n-survive-2"}

        n1 = await reg2.get("n-survive-1")
        assert n1.status == NodeStatus.READY
        assert n1.auth_token == "token-1"

    @pytest.mark.asyncio
    async def test_task_registry_survives_restart(self, tmp_path):
        config, tmp_dir = create_test_config(tmp_path)

        tr1 = TaskRegistry(config.task_registry.path)
        await tr1.load()
        await tr1.register("task-r-1", "worker-1", {"book": "test"})
        await tr1.register("task-r-2", "worker-1", {"book": "test2"})

        tr2 = TaskRegistry(config.task_registry.path)
        await tr2.load()

        t1 = await tr2.get("task-r-1")
        t2 = await tr2.get("task-r-2")
        assert t1.worker_id == "worker-1"
        assert t2.worker_id == "worker-1"
        assert t1.status == TaskStatus.RUNNING

    @pytest.mark.asyncio
    async def test_task_recovery_marks_offline_workers(self, tmp_path):
        config, tmp_dir = create_test_config(tmp_path)

        tr = TaskRegistry(config.task_registry.path)
        await tr.load()
        await tr.register("task-off-1", "worker-offline", {})
        await tr.register("task-on-1", "worker-online", {})

        online_workers = {"worker-online"}
        await tr.recover(online_workers)

        off_task = await tr.get("task-off-1")
        on_task = await tr.get("task-on-1")
        assert off_task.status == TaskStatus.WORKER_LOST
        assert on_task.status == TaskStatus.RUNNING

    @pytest.mark.asyncio
    async def test_full_manager_restart_cycle(self, running_server):
        """Reject split brain, then recover state after the owner has stopped."""
        from elastic_agent.manager.manager import ElasticAgentManager

        srv = running_server

        nodes = await srv["manager"].scale_out(count=2)
        worker = await connect_mock_worker(
            srv["ws_url"], nodes[0].auth_token, nodes[0].node_id
        )
        try:
            await srv["manager"].task_registry.register(
                "restart-task", nodes[0].node_id, {"test": True}
            )

            all_nodes = await srv["manager"].registry.list_all()
            assert len(all_nodes) == 2
        finally:
            await worker.disconnect()

        await asyncio.sleep(0.2)

        manager2 = ElasticAgentManager(
            config=srv["config"],
            provider=srv["provider"],
            file_storage=srv["oss"],
        )

        with pytest.raises(
            RuntimeError,
            match="another ElasticAgentManager owns EIP bindings",
        ):
            await manager2.start()

        # A real replacement process can acquire ownership only after the old
        # Manager has stopped (or its process has exited and the flock closed).
        await srv["manager"].stop()
        await manager2.start()
        try:
            all_nodes2 = await manager2.registry.list_all()
            assert len(all_nodes2) == 2

            task = await manager2.task_registry.get("restart-task")
            assert task is not None
            assert task.status == TaskStatus.WORKER_LOST
        finally:
            await manager2.stop()

    @pytest.mark.asyncio
    async def test_worker_reconnect_after_restart(self, running_server):
        srv = running_server

        nodes = await srv["manager"].scale_out(count=1)
        node = nodes[0]

        worker = await connect_mock_worker(srv["ws_url"], node.auth_token, node.node_id)
        assert srv["manager"].connection_manager.is_connected(node.node_id)

        await worker.disconnect()
        await asyncio.sleep(0.2)
        assert not srv["manager"].connection_manager.is_connected(node.node_id)

        worker2 = await connect_mock_worker(srv["ws_url"], node.auth_token, node.node_id)
        try:
            assert srv["manager"].connection_manager.is_connected(node.node_id)
            status = await srv["manager"].registry.get(node.node_id)
            assert status.status == NodeStatus.READY
        finally:
            await worker2.disconnect()

    @pytest.mark.asyncio
    async def test_registry_atomic_writes(self, tmp_path):
        config, tmp_dir = create_test_config(tmp_path)

        reg = NodeRegistry(config.registry.path)
        await reg.load()

        for i in range(20):
            await reg.add(NodeRecord(
                node_id=f"atomic-{i}",
                instance_id=f"dryrun:i-{i}",
                platform="dryrun",
                status=NodeStatus.READY,
                auth_token=f"tok-{i}",
            ))

        reg2 = NodeRegistry(config.registry.path)
        await reg2.load()
        nodes = await reg2.list_all()
        assert len(nodes) == 20

    @pytest.mark.asyncio
    async def test_concurrent_registry_operations(self, tmp_path):
        config, tmp_dir = create_test_config(tmp_path)

        reg = NodeRegistry(config.registry.path)
        await reg.load()

        async def add_and_update(i):
            await reg.add(NodeRecord(
                node_id=f"conc-{i}",
                instance_id=f"dryrun:i-{i}",
                platform="dryrun",
                status=NodeStatus.CREATING,
                auth_token=f"tok-{i}",
            ))
            await reg.update(f"conc-{i}", status=NodeStatus.READY)

        await asyncio.gather(*[add_and_update(i) for i in range(10)])

        nodes = await reg.list_all()
        assert len(nodes) == 10
        assert all(n.status == NodeStatus.READY for n in nodes)
