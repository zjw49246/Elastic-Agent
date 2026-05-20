"""T-131: Multi-worker concurrent integration test.

Tests: 5+ Workers connect simultaneously, execute commands concurrently,
all results delivered correctly without interference.
"""

from __future__ import annotations

import asyncio

import pytest

from elastic_agent.core.registry import NodeStatus

from .conftest import connect_mock_worker


@pytest.mark.level1
class TestMultiWorkerConcurrent:
    """Multiple workers connected and executing concurrently."""

    @pytest.mark.asyncio
    async def test_five_workers_connect_simultaneously(self, running_server):
        srv = running_server

        nodes = await srv["manager"].scale_out(count=5)
        assert len(nodes) == 5

        workers = await asyncio.gather(*[
            connect_mock_worker(srv["ws_url"], n.auth_token, n.node_id)
            for n in nodes
        ])

        try:
            connected = srv["manager"].connection_manager.connected_workers
            assert len(connected) == 5
            for node in nodes:
                assert srv["manager"].connection_manager.is_connected(node.node_id)
        finally:
            await asyncio.gather(*[w.disconnect() for w in workers])

    @pytest.mark.asyncio
    async def test_concurrent_command_execution(self, running_server):
        srv = running_server

        nodes = await srv["manager"].scale_out(count=5)
        workers = []
        for node in nodes:
            w = await connect_mock_worker(srv["ws_url"], node.auth_token, node.node_id)
            w.set_script(f"task-{node.node_id}", [
                f'{{"type":"result","session_id":"s-{node.node_id}","cost_usd":1.0}}',
            ])
            workers.append(w)

        exit_events: dict[str, list] = {}

        def track_exit(et, wid, d):
            exit_events.setdefault(wid, []).append(d)

        srv["manager"].event_bus.subscribe("PROCESS_EXIT", track_exit)

        try:
            await asyncio.gather(*[
                srv["manager"].connection_manager.execute(
                    worker_id=n.node_id,
                    task_id=f"task-{n.node_id}",
                    command=["echo", "hello"],
                )
                for n in nodes
            ])

            await asyncio.sleep(1.0)

            assert len(exit_events) == 5
            for node in nodes:
                assert node.node_id in exit_events
                assert exit_events[node.node_id][0]["exit_code"] == 0
        finally:
            await asyncio.gather(*[w.disconnect() for w in workers])

    @pytest.mark.asyncio
    async def test_concurrent_results_isolated(self, running_server):
        srv = running_server

        nodes = await srv["manager"].scale_out(count=5)
        workers = []
        for i, node in enumerate(nodes):
            w = await connect_mock_worker(srv["ws_url"], node.auth_token, node.node_id)
            w.set_script(f"iso-{i}", [
                f'{{"type":"result","session_id":"s-{i}","cost_usd":{i+1}.0}}',
            ])
            workers.append(w)

        try:
            await asyncio.gather(*[
                srv["manager"].connection_manager.execute(
                    worker_id=nodes[i].node_id,
                    task_id=f"iso-{i}",
                    command=["test"],
                )
                for i in range(5)
            ])
            await asyncio.sleep(1.0)

            for i in range(5):
                session = srv["manager"].log_event_parser.get_task_session(f"iso-{i}")
                assert session.session_id == f"s-{i}"
                assert session.total_cost_usd == float(i + 1)
        finally:
            await asyncio.gather(*[w.disconnect() for w in workers])

    @pytest.mark.asyncio
    async def test_worker_disconnect_does_not_affect_others(self, running_server):
        srv = running_server

        nodes = await srv["manager"].scale_out(count=3)
        workers = []
        for node in nodes:
            w = await connect_mock_worker(srv["ws_url"], node.auth_token, node.node_id)
            workers.append(w)

        try:
            await workers[1].disconnect()
            await asyncio.sleep(0.3)

            assert not srv["manager"].connection_manager.is_connected(nodes[1].node_id)
            assert srv["manager"].connection_manager.is_connected(nodes[0].node_id)
            assert srv["manager"].connection_manager.is_connected(nodes[2].node_id)

            workers[0].set_script("after-dc", [
                '{"type":"result","session_id":"s-ok","cost_usd":0.5}',
            ])
            exit_events: list[dict] = []
            srv["manager"].event_bus.subscribe(
                "PROCESS_EXIT", lambda et, wid, d: exit_events.append(d)
            )

            await srv["manager"].connection_manager.execute(
                worker_id=nodes[0].node_id,
                task_id="after-dc",
                command=["test"],
            )
            await asyncio.sleep(0.5)

            assert len(exit_events) == 1
        finally:
            await workers[0].disconnect()
            await workers[2].disconnect()

    @pytest.mark.asyncio
    async def test_broadcast_reaches_all_workers(self, running_server):
        srv = running_server

        nodes = await srv["manager"].scale_out(count=5)
        workers = []
        for node in nodes:
            w = await connect_mock_worker(srv["ws_url"], node.auth_token, node.node_id)
            workers.append(w)

        try:
            from elastic_agent.core.protocols.messages import HealthCheckMessage
            results = await srv["manager"].connection_manager.broadcast(HealthCheckMessage())
            assert len(results) == 5
            assert all(results.values())

            await asyncio.sleep(0.3)

            for w in workers:
                hc_msgs = [m for m in w.messages_received if m.get("type") == "HEALTH_CHECK"]
                assert len(hc_msgs) >= 1
        finally:
            await asyncio.gather(*[w.disconnect() for w in workers])

    @pytest.mark.asyncio
    async def test_scale_out_and_in_with_active_workers(self, running_server):
        srv = running_server

        nodes = await srv["manager"].scale_out(count=5)
        workers = []
        for node in nodes:
            w = await connect_mock_worker(srv["ws_url"], node.auth_token, node.node_id)
            workers.append(w)

        try:
            to_remove = [nodes[0].node_id, nodes[1].node_id]
            terminated = await srv["manager"].scale_in(to_remove, force=True)
            assert len(terminated) == 2

            await asyncio.sleep(0.3)
            connected = srv["manager"].connection_manager.connected_workers
            assert len(connected) == 3

            new_nodes = await srv["manager"].scale_out(count=2)
            new_workers = []
            for node in new_nodes:
                w = await connect_mock_worker(srv["ws_url"], node.auth_token, node.node_id)
                new_workers.append(w)
                workers.append(w)

            connected = srv["manager"].connection_manager.connected_workers
            assert len(connected) == 5
        finally:
            for w in workers:
                try:
                    await w.disconnect()
                except Exception:
                    pass

    @pytest.mark.asyncio
    async def test_seven_workers_connect_and_execute(self, running_server):
        srv = running_server

        nodes = await srv["manager"].scale_out(count=7)
        workers = []
        for node in nodes:
            w = await connect_mock_worker(srv["ws_url"], node.auth_token, node.node_id)
            w.set_script(f"seven-{node.node_id}", [
                f'{{"type":"result","session_id":"s7-{node.node_id}","cost_usd":0.1}}',
            ])
            workers.append(w)

        exit_count = 0

        def count_exits(et, wid, d):
            nonlocal exit_count
            exit_count += 1

        srv["manager"].event_bus.subscribe("PROCESS_EXIT", count_exits)

        try:
            await asyncio.gather(*[
                srv["manager"].connection_manager.execute(
                    worker_id=n.node_id,
                    task_id=f"seven-{n.node_id}",
                    command=["go"],
                )
                for n in nodes
            ])
            await asyncio.sleep(1.5)

            assert exit_count == 7
        finally:
            await asyncio.gather(*[w.disconnect() for w in workers])
