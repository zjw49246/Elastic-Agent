"""Tests for EventBus LOG event dispatch (T-013)."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

import pytest

from elastic_agent.core.event_bus import EventBus


class TestSubscribeUnsubscribe:
    def test_subscribe_returns_id(self):
        bus = EventBus()
        sub_id = bus.subscribe("LOG", AsyncMock())
        assert isinstance(sub_id, str)
        assert len(sub_id) > 0

    def test_subscriber_count(self):
        bus = EventBus()
        assert bus.subscriber_count() == 0

        bus.subscribe("LOG", AsyncMock())
        assert bus.subscriber_count() == 1
        assert bus.subscriber_count("LOG") == 1
        assert bus.subscriber_count("OTHER") == 0

    def test_unsubscribe_removes(self):
        bus = EventBus()
        sub_id = bus.subscribe("LOG", AsyncMock())
        assert bus.subscriber_count() == 1

        result = bus.unsubscribe(sub_id)
        assert result is True
        assert bus.subscriber_count() == 0

    def test_unsubscribe_nonexistent(self):
        bus = EventBus()
        assert bus.unsubscribe("nonexistent-id") is False

    def test_multiple_subscribers_same_type(self):
        bus = EventBus()
        bus.subscribe("LOG", AsyncMock())
        bus.subscribe("LOG", AsyncMock())
        assert bus.subscriber_count("LOG") == 2

    def test_clear(self):
        bus = EventBus()
        bus.subscribe("LOG", AsyncMock())
        bus.subscribe("FILE_SYNCED", AsyncMock())
        bus.subscribe("*", AsyncMock())
        assert bus.subscriber_count() == 3

        bus.clear()
        assert bus.subscriber_count() == 0


class TestEmit:
    @pytest.mark.asyncio
    async def test_emit_to_matching_subscriber(self):
        bus = EventBus()
        handler = AsyncMock()
        bus.subscribe("LOG", handler)

        count = await bus.emit("LOG", "worker-1", {"task_id": "t1", "data": "hello"})

        assert count == 1
        handler.assert_called_once_with("LOG", "worker-1", {"task_id": "t1", "data": "hello"})

    @pytest.mark.asyncio
    async def test_emit_no_matching_subscribers(self):
        bus = EventBus()
        handler = AsyncMock()
        bus.subscribe("LOG", handler)

        count = await bus.emit("HEARTBEAT", "worker-1", {})

        assert count == 0
        handler.assert_not_called()

    @pytest.mark.asyncio
    async def test_wildcard_receives_all_events(self):
        bus = EventBus()
        wildcard = AsyncMock()
        bus.subscribe("*", wildcard)

        await bus.emit("LOG", "w1", {"data": "a"})
        await bus.emit("HEARTBEAT", "w2", {"uptime": 100})
        await bus.emit("FILE_SYNCED", "w1", {"path": "/tmp/x"})

        assert wildcard.call_count == 3

    @pytest.mark.asyncio
    async def test_wildcard_and_specific_both_called(self):
        bus = EventBus()
        specific = AsyncMock()
        wildcard = AsyncMock()
        bus.subscribe("LOG", specific)
        bus.subscribe("*", wildcard)

        count = await bus.emit("LOG", "w1", {"data": "test"})

        assert count == 2
        specific.assert_called_once()
        wildcard.assert_called_once()

    @pytest.mark.asyncio
    async def test_fan_out_multiple_subscribers(self):
        bus = EventBus()
        h1 = AsyncMock()
        h2 = AsyncMock()
        h3 = AsyncMock()
        bus.subscribe("LOG", h1)
        bus.subscribe("LOG", h2)
        bus.subscribe("LOG", h3)

        count = await bus.emit("LOG", "w1", {"data": "test"})

        assert count == 3
        h1.assert_called_once()
        h2.assert_called_once()
        h3.assert_called_once()

    @pytest.mark.asyncio
    async def test_callback_error_doesnt_block_others(self):
        bus = EventBus()
        failing = AsyncMock(side_effect=ValueError("boom"))
        passing = AsyncMock()
        bus.subscribe("LOG", failing)
        bus.subscribe("LOG", passing)

        count = await bus.emit("LOG", "w1", {"data": "test"})

        assert count == 1
        passing.assert_called_once()

    @pytest.mark.asyncio
    async def test_emit_after_unsubscribe(self):
        bus = EventBus()
        handler = AsyncMock()
        sub_id = bus.subscribe("LOG", handler)
        bus.unsubscribe(sub_id)

        count = await bus.emit("LOG", "w1", {})

        assert count == 0
        handler.assert_not_called()

    @pytest.mark.asyncio
    async def test_emit_empty_bus(self):
        bus = EventBus()
        count = await bus.emit("LOG", "w1", {})
        assert count == 0

    @pytest.mark.asyncio
    async def test_event_data_passed_correctly(self):
        bus = EventBus()
        received = []

        async def handler(event_type, worker_id, data):
            received.append((event_type, worker_id, data))

        bus.subscribe("FILE_SYNCED", handler)

        data = {"task_id": "t1", "path": "/out/ch01.md", "oss_key": "tasks/t1/ch01.md"}
        await bus.emit("FILE_SYNCED", "worker-3", data)

        assert len(received) == 1
        assert received[0] == ("FILE_SYNCED", "worker-3", data)

    @pytest.mark.asyncio
    async def test_multiple_event_types_routed_correctly(self):
        bus = EventBus()
        log_handler = AsyncMock()
        file_handler = AsyncMock()
        bus.subscribe("LOG", log_handler)
        bus.subscribe("FILE_SYNCED", file_handler)

        await bus.emit("LOG", "w1", {"data": "line1"})
        await bus.emit("FILE_SYNCED", "w1", {"path": "/a"})
        await bus.emit("LOG", "w1", {"data": "line2"})

        assert log_handler.call_count == 2
        assert file_handler.call_count == 1
