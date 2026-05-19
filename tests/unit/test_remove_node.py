"""Unit tests for remove_node API (T-028)."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from elastic_agent.core.config import ElasticAgentConfig
from elastic_agent.core.registry import NodeRecord, NodeStatus
from elastic_agent.manager.manager import ElasticAgentManager


@pytest.fixture
def mock_provider():
    provider = AsyncMock()
    provider.terminate_instance = AsyncMock()
    return provider


@pytest.fixture
def manager(tmp_path, mock_provider):
    config = ElasticAgentConfig(
        registry={"path": str(tmp_path / "registry.json")},
        task_registry={"path": str(tmp_path / "task_registry.json")},
        webhook={"dead_letter_path": str(tmp_path / "dead_letters.json")},
    )
    mgr = ElasticAgentManager(config=config, provider=mock_provider)
    return mgr


class TestRemoveNode:

    async def test_remove_terminated_node(self, manager):
        await manager.start()
        try:
            node = NodeRecord(
                node_id="aliyun:i-test1",
                instance_id="i-test1",
                platform="aliyun",
                status=NodeStatus.TERMINATED,
            )
            await manager.registry.add(node)

            result = await manager.remove_node("aliyun:i-test1")
            assert result is True

            removed = await manager.registry.get("aliyun:i-test1")
            assert removed is None

            manager.provider.terminate_instance.assert_not_called()
        finally:
            await manager.stop()

    async def test_remove_failed_node(self, manager):
        await manager.start()
        try:
            node = NodeRecord(
                node_id="aliyun:i-test2",
                instance_id="i-test2",
                platform="aliyun",
                status=NodeStatus.FAILED,
            )
            await manager.registry.add(node)

            result = await manager.remove_node("aliyun:i-test2")
            assert result is True
            assert await manager.registry.get("aliyun:i-test2") is None
        finally:
            await manager.stop()

    async def test_remove_running_node_terminates_first(self, manager):
        await manager.start()
        try:
            node = NodeRecord(
                node_id="aliyun:i-test3",
                instance_id="i-test3",
                platform="aliyun",
                status=NodeStatus.READY,
            )
            await manager.registry.add(node)

            result = await manager.remove_node("aliyun:i-test3")
            assert result is True
            assert await manager.registry.get("aliyun:i-test3") is None
            manager.provider.terminate_instance.assert_called_once_with("i-test3")
        finally:
            await manager.stop()

    async def test_remove_nonexistent_returns_false(self, manager):
        await manager.start()
        try:
            result = await manager.remove_node("nonexistent")
            assert result is False
        finally:
            await manager.stop()

    async def test_remove_survives_terminate_failure(self, manager):
        manager.provider.terminate_instance = AsyncMock(side_effect=Exception("cloud error"))
        await manager.start()
        try:
            node = NodeRecord(
                node_id="aliyun:i-test4",
                instance_id="i-test4",
                platform="aliyun",
                status=NodeStatus.READY,
            )
            await manager.registry.add(node)

            result = await manager.remove_node("aliyun:i-test4")
            assert result is True
            assert await manager.registry.get("aliyun:i-test4") is None
        finally:
            await manager.stop()
