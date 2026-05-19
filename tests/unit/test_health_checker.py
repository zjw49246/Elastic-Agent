"""Tests for Worker multi-layer health check (T-024)."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from elastic_agent.core.event_bus import EventBus
from elastic_agent.core.health_checker import (
    HealthChecker,
    HealthLevel,
    HealthStatus,
    NodeHealthReport,
)
from elastic_agent.core.registry import NodeRecord, NodeRegistry, NodeStatus
from elastic_agent.manager.connection import WorkerConnectionManager


def _utcnow():
    return datetime.now(timezone.utc)


@pytest.fixture
async def setup(tmp_path: Path):
    registry = NodeRegistry(str(tmp_path / "registry.json"))
    await registry.load()
    event_bus = EventBus()
    conn_mgr = MagicMock(spec=WorkerConnectionManager)
    conn_mgr.is_connected = MagicMock(return_value=True)
    conn_mgr.health_check = AsyncMock()
    return registry, conn_mgr, event_bus


@pytest.mark.asyncio
async def test_healthy_node(setup) -> None:
    registry, conn_mgr, event_bus = setup
    await registry.add(NodeRecord(
        node_id="w1", instance_id="inst-1", platform="dryrun",
        status=NodeStatus.READY, last_heartbeat=_utcnow(),
    ))
    checker = HealthChecker(registry, conn_mgr, event_bus)
    report = await checker.check_node("w1")

    assert report.l1 == HealthStatus.HEALTHY
    assert report.l2 == HealthStatus.HEALTHY
    assert report.l3 == HealthStatus.HEALTHY
    assert report.overall == HealthStatus.HEALTHY


@pytest.mark.asyncio
async def test_l1_unhealthy_stale_heartbeat(setup) -> None:
    registry, conn_mgr, event_bus = setup
    old_time = _utcnow() - timedelta(seconds=200)
    await registry.add(NodeRecord(
        node_id="w1", instance_id="inst-1", platform="dryrun",
        status=NodeStatus.READY, last_heartbeat=old_time,
    ))
    checker = HealthChecker(registry, conn_mgr, event_bus, heartbeat_interval=30, unhealthy_threshold=3)
    report = await checker.check_node("w1")

    assert report.l1 == HealthStatus.UNHEALTHY
    assert report.overall == HealthStatus.UNHEALTHY


@pytest.mark.asyncio
async def test_l1_unknown_creating_node(setup) -> None:
    registry, conn_mgr, event_bus = setup
    await registry.add(NodeRecord(
        node_id="w1", instance_id="inst-1", platform="dryrun",
        status=NodeStatus.CREATING,
    ))
    checker = HealthChecker(registry, conn_mgr, event_bus)
    report = await checker.check_node("w1")
    assert report.l1 == HealthStatus.UNKNOWN


@pytest.mark.asyncio
async def test_l2_unhealthy_disconnected(setup) -> None:
    registry, conn_mgr, event_bus = setup
    conn_mgr.is_connected.return_value = False
    await registry.add(NodeRecord(
        node_id="w1", instance_id="inst-1", platform="dryrun",
        status=NodeStatus.READY, last_heartbeat=_utcnow(),
    ))
    checker = HealthChecker(registry, conn_mgr, event_bus)
    report = await checker.check_node("w1")

    assert report.l2 == HealthStatus.UNHEALTHY


@pytest.mark.asyncio
async def test_l3_unhealthy_health_check_fails(setup) -> None:
    registry, conn_mgr, event_bus = setup
    conn_mgr.health_check.side_effect = Exception("timeout")
    await registry.add(NodeRecord(
        node_id="w1", instance_id="inst-1", platform="dryrun",
        status=NodeStatus.READY, last_heartbeat=_utcnow(),
    ))
    checker = HealthChecker(registry, conn_mgr, event_bus)
    report = await checker.check_node("w1")

    assert report.l3 == HealthStatus.UNHEALTHY
    assert report.overall == HealthStatus.DEGRADED


@pytest.mark.asyncio
async def test_l3_unknown_when_disconnected(setup) -> None:
    registry, conn_mgr, event_bus = setup
    conn_mgr.is_connected.return_value = False
    await registry.add(NodeRecord(
        node_id="w1", instance_id="inst-1", platform="dryrun",
        status=NodeStatus.READY, last_heartbeat=_utcnow(),
    ))
    checker = HealthChecker(registry, conn_mgr, event_bus)
    report = await checker.check_node("w1")
    assert report.l3 == HealthStatus.UNKNOWN


@pytest.mark.asyncio
async def test_consecutive_failures_trigger_unhealthy_event(setup) -> None:
    registry, conn_mgr, event_bus = setup
    conn_mgr.is_connected.return_value = False
    old_time = _utcnow() - timedelta(seconds=200)
    await registry.add(NodeRecord(
        node_id="w1", instance_id="inst-1", platform="dryrun",
        status=NodeStatus.READY, last_heartbeat=old_time,
    ))

    events: list[str] = []
    event_bus.subscribe("WORKER_UNHEALTHY", lambda et, wid, data: events.append(wid))

    checker = HealthChecker(
        registry, conn_mgr, event_bus,
        heartbeat_interval=30, unhealthy_threshold=3,
    )

    for _ in range(3):
        await checker.check_node("w1")

    assert "w1" in events
    node = await registry.get("w1")
    assert node.status == NodeStatus.UNHEALTHY


@pytest.mark.asyncio
async def test_failure_count_resets_on_healthy(setup) -> None:
    registry, conn_mgr, event_bus = setup
    await registry.add(NodeRecord(
        node_id="w1", instance_id="inst-1", platform="dryrun",
        status=NodeStatus.READY, last_heartbeat=_utcnow(),
    ))

    checker = HealthChecker(registry, conn_mgr, event_bus, unhealthy_threshold=3)

    conn_mgr.is_connected.return_value = False
    old_time = _utcnow() - timedelta(seconds=200)
    await registry.update("w1", last_heartbeat=old_time)
    await checker.check_node("w1")
    await checker.check_node("w1")
    assert checker._consecutive_failures.get("w1") == 2

    conn_mgr.is_connected.return_value = True
    await registry.update("w1", last_heartbeat=_utcnow())
    await checker.check_node("w1")
    assert "w1" not in checker._consecutive_failures


@pytest.mark.asyncio
async def test_check_all(setup) -> None:
    registry, conn_mgr, event_bus = setup
    await registry.add(NodeRecord(
        node_id="w1", instance_id="inst-1", platform="dryrun",
        status=NodeStatus.READY, last_heartbeat=_utcnow(),
    ))
    await registry.add(NodeRecord(
        node_id="w2", instance_id="inst-2", platform="dryrun",
        status=NodeStatus.BUSY, last_heartbeat=_utcnow(),
    ))
    await registry.add(NodeRecord(
        node_id="w3", instance_id="inst-3", platform="dryrun",
        status=NodeStatus.CREATING,
    ))

    checker = HealthChecker(registry, conn_mgr, event_bus)
    reports = await checker.check_all()
    checked_ids = {r.node_id for r in reports}
    assert "w1" in checked_ids
    assert "w2" in checked_ids
    assert "w3" not in checked_ids


@pytest.mark.asyncio
async def test_nonexistent_node(setup) -> None:
    registry, conn_mgr, event_bus = setup
    checker = HealthChecker(registry, conn_mgr, event_bus)
    report = await checker.check_node("nonexistent")
    assert report.overall == HealthStatus.UNKNOWN


@pytest.mark.asyncio
async def test_report_to_dict(setup) -> None:
    report = NodeHealthReport(
        node_id="w1",
        l1=HealthStatus.HEALTHY,
        l2=HealthStatus.HEALTHY,
        l3=HealthStatus.UNHEALTHY,
    )
    d = report.to_dict()
    assert d["node_id"] == "w1"
    assert d["overall"] == "degraded"
    assert "checked_at" in d
