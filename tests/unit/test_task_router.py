"""Tests for TaskRouter — T-052."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from elastic_agent.core.agent_type import ClaudeCodeAgentType
from elastic_agent.core.registry import NodeRecord, NodeRegistry, NodeStatus
from elastic_agent.core.task_registry import TaskRecord, TaskRegistry, TaskStatus
from elastic_agent.core.task_router import TaskNotFoundError, TaskRouter, WorkerOfflineError
from elastic_agent.manager.connection import WorkerConnectionManager


@pytest.fixture
def node_registry(tmp_path):
    return NodeRegistry(tmp_path / "registry.json")


@pytest.fixture
def task_registry(tmp_path):
    return TaskRegistry(tmp_path / "task_registry.json")


@pytest.fixture
def connection_manager(node_registry):
    cm = WorkerConnectionManager(node_registry)
    cm.execute = AsyncMock()
    cm.is_connected = MagicMock(return_value=True)
    return cm


@pytest.fixture
def agent_type():
    return ClaudeCodeAgentType()


@pytest.fixture
def router(task_registry, node_registry, connection_manager, agent_type):
    return TaskRouter(task_registry, node_registry, connection_manager, agent_type)


@pytest.mark.asyncio
async def test_send_command_success(router, task_registry, node_registry, connection_manager):
    await node_registry.add(NodeRecord(
        node_id="w1", instance_id="i-1", platform="test", status=NodeStatus.READY,
    ))
    await task_registry.register("t1", "w1")

    process_id = await router.send_command("t1", ["echo", "hello"])
    assert process_id.startswith("t1:")
    connection_manager.execute.assert_called_once()
    call_kwargs = connection_manager.execute.call_args
    assert call_kwargs.kwargs["worker_id"] == "w1"
    assert call_kwargs.kwargs["command"] == ["echo", "hello"]


@pytest.mark.asyncio
async def test_send_command_task_not_found(router):
    with pytest.raises(TaskNotFoundError):
        await router.send_command("nonexistent", ["echo"])


@pytest.mark.asyncio
async def test_send_command_worker_offline(router, task_registry, node_registry, connection_manager):
    await node_registry.add(NodeRecord(
        node_id="w1", instance_id="i-1", platform="test", status=NodeStatus.READY,
    ))
    await task_registry.register("t1", "w1")
    connection_manager.is_connected = MagicMock(return_value=False)

    with pytest.raises(WorkerOfflineError):
        await router.send_command("t1", ["echo"])


@pytest.mark.asyncio
async def test_send_command_worker_not_in_registry(router, task_registry, connection_manager):
    await task_registry.register("t1", "w-gone")

    with pytest.raises(WorkerOfflineError):
        await router.send_command("t1", ["echo"])


@pytest.mark.asyncio
async def test_send_followup_with_session_id(router, task_registry, node_registry, connection_manager):
    await node_registry.add(NodeRecord(
        node_id="w1", instance_id="i-1", platform="test", status=NodeStatus.READY,
    ))
    await task_registry.register("t1", "w1")
    await task_registry.update("t1", session_id="sess-123")

    process_id = await router.send_followup("t1", "fix the bug")
    assert process_id.startswith("t1:")

    call_kwargs = connection_manager.execute.call_args.kwargs
    assert "--resume" in call_kwargs["command"]
    assert "sess-123" in call_kwargs["command"]
    assert "fix the bug" in call_kwargs["command"]


@pytest.mark.asyncio
async def test_send_followup_without_session_id(router, task_registry, node_registry, connection_manager):
    await node_registry.add(NodeRecord(
        node_id="w1", instance_id="i-1", platform="test", status=NodeStatus.READY,
    ))
    await task_registry.register("t1", "w1")

    process_id = await router.send_followup("t1", "do something")
    call_kwargs = connection_manager.execute.call_args.kwargs
    assert "--resume" not in call_kwargs["command"]
    assert "do something" in call_kwargs["command"]


@pytest.mark.asyncio
async def test_send_followup_no_agent_type(task_registry, node_registry, connection_manager):
    router = TaskRouter(task_registry, node_registry, connection_manager, agent_type=None)
    await task_registry.register("t1", "w1")

    with pytest.raises(RuntimeError, match="AgentType"):
        await router.send_followup("t1", "message")


@pytest.mark.asyncio
async def test_send_followup_task_not_found(router):
    with pytest.raises(TaskNotFoundError):
        await router.send_followup("nope", "message")


@pytest.mark.asyncio
async def test_send_command_with_env_and_cwd(router, task_registry, node_registry, connection_manager):
    await node_registry.add(NodeRecord(
        node_id="w1", instance_id="i-1", platform="test", status=NodeStatus.READY,
    ))
    await task_registry.register("t1", "w1")

    await router.send_command("t1", ["ls"], env={"FOO": "bar"}, cwd="/tmp")
    call_kwargs = connection_manager.execute.call_args.kwargs
    assert call_kwargs["env"] == {"FOO": "bar"}
    assert call_kwargs["cwd"] == "/tmp"


@pytest.mark.asyncio
async def test_send_followup_with_config_dir(router, task_registry, node_registry, connection_manager):
    await node_registry.add(NodeRecord(
        node_id="w1", instance_id="i-1", platform="test", status=NodeStatus.READY,
    ))
    await task_registry.register("t1", "w1")

    await router.send_followup("t1", "test", config_dir="/root/.claude-edit-0")
    call_kwargs = connection_manager.execute.call_args.kwargs
    assert call_kwargs["env"].get("CLAUDE_CONFIG_DIR") == "/root/.claude-edit-0"


# ===========================================================================
# T-140: TaskRouter extended — routing, --resume assembly, worker offline
# ===========================================================================


class TestT140Routing:
    """T-140: Route to correct worker among multiple."""

    @pytest.mark.asyncio
    async def test_route_to_correct_worker(self, router, task_registry, node_registry, connection_manager):
        await node_registry.add(NodeRecord(
            node_id="w1", instance_id="i-1", platform="test", status=NodeStatus.READY,
        ))
        await node_registry.add(NodeRecord(
            node_id="w2", instance_id="i-2", platform="test", status=NodeStatus.READY,
        ))
        await task_registry.register("t1", "w1")
        await task_registry.register("t2", "w2")

        await router.send_command("t2", ["echo", "hello"])
        call_kwargs = connection_manager.execute.call_args.kwargs
        assert call_kwargs["worker_id"] == "w2"

    @pytest.mark.asyncio
    async def test_process_task_id_format(self, router, task_registry, node_registry, connection_manager):
        await node_registry.add(NodeRecord(
            node_id="w1", instance_id="i-1", platform="test", status=NodeStatus.READY,
        ))
        await task_registry.register("t1", "w1")
        pid = await router.send_command("t1", ["echo"])
        assert pid.startswith("t1:")
        suffix = pid.split(":")[1]
        assert len(suffix) == 8
        int(suffix, 16)

    @pytest.mark.asyncio
    async def test_send_command_with_timeout(self, router, task_registry, node_registry, connection_manager):
        await node_registry.add(NodeRecord(
            node_id="w1", instance_id="i-1", platform="test", status=NodeStatus.READY,
        ))
        await task_registry.register("t1", "w1")
        await router.send_command("t1", ["long-running"], timeout=3600)
        call_kwargs = connection_manager.execute.call_args.kwargs
        assert call_kwargs["timeout"] == 3600

    @pytest.mark.asyncio
    async def test_multiple_sends_different_process_ids(self, router, task_registry, node_registry, connection_manager):
        await node_registry.add(NodeRecord(
            node_id="w1", instance_id="i-1", platform="test", status=NodeStatus.READY,
        ))
        await task_registry.register("t1", "w1")
        pid1 = await router.send_command("t1", ["cmd1"])
        pid2 = await router.send_command("t1", ["cmd2"])
        assert pid1 != pid2
        assert pid1.startswith("t1:")
        assert pid2.startswith("t1:")


class TestT140ResumeAssembly:
    """T-140: --resume auto-assembly."""

    @pytest.mark.asyncio
    async def test_resume_includes_session_and_prompt(self, router, task_registry, node_registry, connection_manager):
        await node_registry.add(NodeRecord(
            node_id="w1", instance_id="i-1", platform="test", status=NodeStatus.READY,
        ))
        await task_registry.register("t1", "w1")
        await task_registry.update("t1", session_id="sess-abc")

        await router.send_followup("t1", "fix the bug please")
        cmd = connection_manager.execute.call_args.kwargs["command"]
        assert "--resume" in cmd
        assert "sess-abc" in cmd
        assert "fix the bug please" in cmd

    @pytest.mark.asyncio
    async def test_no_resume_without_session(self, router, task_registry, node_registry, connection_manager):
        await node_registry.add(NodeRecord(
            node_id="w1", instance_id="i-1", platform="test", status=NodeStatus.READY,
        ))
        await task_registry.register("t1", "w1")

        await router.send_followup("t1", "do something")
        cmd = connection_manager.execute.call_args.kwargs["command"]
        assert "--resume" not in cmd

    @pytest.mark.asyncio
    async def test_followup_sets_config_dir_env(self, router, task_registry, node_registry, connection_manager):
        await node_registry.add(NodeRecord(
            node_id="w1", instance_id="i-1", platform="test", status=NodeStatus.READY,
        ))
        await task_registry.register("t1", "w1")

        await router.send_followup("t1", "msg", config_dir="/root/.claude-edit-1")
        env = connection_manager.execute.call_args.kwargs["env"]
        assert env["CLAUDE_CONFIG_DIR"] == "/root/.claude-edit-1"


class TestT140WorkerOffline:
    """T-140: Worker offline error handling."""

    @pytest.mark.asyncio
    async def test_worker_not_in_registry(self, router, task_registry, connection_manager):
        await task_registry.register("t1", "w-gone")
        with pytest.raises(WorkerOfflineError):
            await router.send_command("t1", ["echo"])

    @pytest.mark.asyncio
    async def test_worker_disconnected(self, router, task_registry, node_registry, connection_manager):
        await node_registry.add(NodeRecord(
            node_id="w1", instance_id="i-1", platform="test", status=NodeStatus.READY,
        ))
        await task_registry.register("t1", "w1")
        connection_manager.is_connected = MagicMock(return_value=False)
        with pytest.raises(WorkerOfflineError):
            await router.send_command("t1", ["echo"])

    @pytest.mark.asyncio
    async def test_followup_worker_offline(self, router, task_registry, node_registry, connection_manager):
        await node_registry.add(NodeRecord(
            node_id="w1", instance_id="i-1", platform="test", status=NodeStatus.READY,
        ))
        await task_registry.register("t1", "w1")
        connection_manager.is_connected = MagicMock(return_value=False)
        with pytest.raises(WorkerOfflineError):
            await router.send_followup("t1", "message")

    @pytest.mark.asyncio
    async def test_task_not_found_for_command(self, router):
        with pytest.raises(TaskNotFoundError):
            await router.send_command("no-task", ["echo"])

    @pytest.mark.asyncio
    async def test_task_not_found_for_followup(self, router):
        with pytest.raises(TaskNotFoundError):
            await router.send_followup("no-task", "msg")
