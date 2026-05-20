"""T-136: Quota monitoring E2E integration test.

Tests: Worker quota report → Manager aggregation → threshold alert → trigger rotation.
Verifies the full pipeline with real EventBus and MockWorker.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

from elastic_agent.core.config import CredentialConfig
from elastic_agent.core.credential_binding import CredentialBinding
from elastic_agent.core.credential_pool import CredentialPool
from elastic_agent.core.credential_rotator import CredentialRotator
from elastic_agent.core.event_bus import EventBus
from elastic_agent.core.quota_monitor import QuotaMonitor

from .conftest import connect_mock_worker, write_accounts_json


@pytest.mark.level1
class TestQuotaMonitorE2E:
    """Full quota monitoring pipeline with Worker → Manager → rotation."""

    @pytest.mark.asyncio
    async def test_worker_reports_quota_via_ws(self, running_server):
        srv = running_server
        nodes = await srv["manager"].scale_out(count=1)
        node = nodes[0]

        quota_events: list[dict] = []
        srv["manager"].event_bus.subscribe(
            "QUOTA_STATUS", lambda et, wid, d: quota_events.append(d)
        )

        worker = await connect_mock_worker(srv["ws_url"], node.auth_token, node.node_id)
        try:
            await worker.send_quota_status(
                task_id="qt-1",
                account_id="acct-qt-1",
                five_hour_pct=72.5,
                seven_day_pct=30.0,
            )
            await asyncio.sleep(0.3)

            assert len(quota_events) == 1
            assert quota_events[0]["account_id"] == "acct-qt-1"
            assert quota_events[0]["five_hour_pct"] == 72.5
        finally:
            await worker.disconnect()

    @pytest.mark.asyncio
    async def test_quota_warning_emitted_at_threshold(self, tmp_path):
        write_accounts_json(tmp_path, [
            {"id": "warn-acct", "email": "w@t.com", "group": "standard"},
        ])

        config = CredentialConfig(
            accounts_file=str(tmp_path / "accounts.json"),
            pool_status_file=str(tmp_path / "pool_status.json"),
            quota_threshold=0.85,
        )
        pool = CredentialPool(config)
        await pool.load()

        event_bus = EventBus()
        monitor = QuotaMonitor(pool, event_bus, config)

        warnings: list[dict] = []
        event_bus.subscribe("QUOTA_WARNING", lambda et, wid, d: warnings.append(d))

        await monitor.start()
        try:
            await event_bus.emit("QUOTA_STATUS", "w1", {
                "account_id": "warn-acct",
                "five_hour_pct": 88.0,
                "seven_day_pct": 25.0,
            })
            await asyncio.sleep(0.2)

            assert len(warnings) == 1
            assert warnings[0]["reason"] == "quota_warning"
            assert warnings[0]["usage_pct"] == 88.0
        finally:
            await monitor.stop()

    @pytest.mark.asyncio
    async def test_critical_quota_triggers_rotation(self, tmp_path):
        write_accounts_json(tmp_path, [
            {"id": "crit-a", "email": "ca@t.com", "group": "standard"},
            {"id": "crit-b", "email": "cb@t.com", "group": "standard"},
        ])

        config = CredentialConfig(
            accounts_file=str(tmp_path / "accounts.json"),
            pool_status_file=str(tmp_path / "pool_status.json"),
            quota_threshold=0.85,
        )
        pool = CredentialPool(config)
        await pool.load()

        event_bus = EventBus()
        monitor = QuotaMonitor(pool, event_bus, config)

        rotation_requests: list[tuple] = []

        async def on_rotation(wid, aid, reason):
            rotation_requests.append((wid, aid, reason))

        monitor.on_rotation_needed = on_rotation

        await monitor.start()
        try:
            await event_bus.emit("QUOTA_STATUS", "w-crit", {
                "account_id": "crit-a",
                "five_hour_pct": 96.0,
                "seven_day_pct": 50.0,
            })
            await asyncio.sleep(0.2)

            assert len(rotation_requests) == 1
            assert rotation_requests[0] == ("w-crit", "crit-a", "quota_exceeded")
        finally:
            await monitor.stop()

    @pytest.mark.asyncio
    async def test_rate_limited_triggers_rotation(self, tmp_path):
        write_accounts_json(tmp_path, [
            {"id": "rl-a", "email": "rl@t.com", "group": "standard"},
        ])

        config = CredentialConfig(
            accounts_file=str(tmp_path / "accounts.json"),
            pool_status_file=str(tmp_path / "pool_status.json"),
            quota_threshold=0.85,
        )
        pool = CredentialPool(config)
        await pool.load()

        event_bus = EventBus()
        monitor = QuotaMonitor(pool, event_bus, config)

        rotation_requests: list = []

        async def on_rotation(wid, aid, reason):
            rotation_requests.append(reason)

        monitor.on_rotation_needed = on_rotation

        await monitor.start()
        try:
            await event_bus.emit("QUOTA_STATUS", "w-rl", {
                "account_id": "rl-a",
                "five_hour_pct": 50.0,
                "seven_day_pct": 20.0,
                "error": "rate_limited",
            })
            await asyncio.sleep(0.2)

            assert len(rotation_requests) == 1
            assert rotation_requests[0] == "quota_exceeded"
        finally:
            await monitor.stop()

    @pytest.mark.asyncio
    async def test_multiple_workers_report_independently(self, running_server):
        srv = running_server
        nodes = await srv["manager"].scale_out(count=3)

        quota_events: list[dict] = []
        srv["manager"].event_bus.subscribe(
            "QUOTA_STATUS", lambda et, wid, d: quota_events.append({"worker": wid, **d})
        )

        workers = []
        for i, node in enumerate(nodes):
            w = await connect_mock_worker(srv["ws_url"], node.auth_token, node.node_id)
            workers.append(w)

        try:
            for i, worker in enumerate(workers):
                await worker.send_quota_status(
                    task_id=f"qt-{i}",
                    account_id=f"acct-{i}",
                    five_hour_pct=float(i * 30),
                    seven_day_pct=float(i * 10),
                )

            await asyncio.sleep(0.5)

            assert len(quota_events) == 3
            worker_ids = {e["worker"] for e in quota_events}
            assert len(worker_ids) == 3
        finally:
            for w in workers:
                await w.disconnect()

    @pytest.mark.asyncio
    async def test_relogin_needed_callback(self, tmp_path):
        write_accounts_json(tmp_path, [
            {"id": "relogin-a", "email": "r@t.com", "group": "standard"},
        ])

        config = CredentialConfig(
            accounts_file=str(tmp_path / "accounts.json"),
            pool_status_file=str(tmp_path / "pool_status.json"),
        )
        pool = CredentialPool(config)
        await pool.load()

        event_bus = EventBus()
        monitor = QuotaMonitor(pool, event_bus, config)

        relogin_requests: list[tuple] = []

        async def on_relogin(wid, aid):
            relogin_requests.append((wid, aid))

        monitor.on_relogin_needed = on_relogin

        await monitor.start()
        try:
            await event_bus.emit("QUOTA_STATUS", "w-relogin", {
                "account_id": "relogin-a",
                "five_hour_pct": 40.0,
                "seven_day_pct": 10.0,
                "needs_relogin": True,
            })
            await asyncio.sleep(0.2)

            assert len(relogin_requests) == 1
            assert relogin_requests[0] == ("w-relogin", "relogin-a")
        finally:
            await monitor.stop()

    @pytest.mark.asyncio
    async def test_full_pipeline_worker_to_rotation(self, tmp_path):
        """End-to-end: Worker sends QUOTA_STATUS → Monitor detects critical
        → triggers rotation → Rotator switches account."""
        write_accounts_json(tmp_path, [
            {"id": "full-old", "email": "old@t.com", "group": "standard"},
            {"id": "full-new", "email": "new@t.com", "group": "standard"},
        ])

        config = CredentialConfig(
            accounts_file=str(tmp_path / "accounts.json"),
            pool_status_file=str(tmp_path / "pool_status.json"),
            quota_threshold=0.85,
        )
        pool = CredentialPool(config)
        await pool.load()

        event_bus = EventBus()
        binding = CredentialBinding()
        rotator = CredentialRotator(pool, binding, event_bus)

        sent_creds: list[str] = []

        async def mock_send(wid, aid, creds, cd):
            sent_creds.append(aid)
            return True

        rotator.send_credential = mock_send

        # Pre-assign old account
        await pool.allocate("worker-full", "production", "standard")
        await binding.bind("full-old", "worker-full")

        monitor = QuotaMonitor(pool, event_bus, config)

        async def on_rotation(wid, aid, reason):
            await rotator.rotate(wid, aid, reason)

        monitor.on_rotation_needed = on_rotation

        await monitor.start()
        try:
            await event_bus.emit("QUOTA_STATUS", "worker-full", {
                "account_id": "full-old",
                "five_hour_pct": 97.0,
                "seven_day_pct": 60.0,
            })
            await asyncio.sleep(0.5)

            assert "full-new" in sent_creds

            old_status = pool.get_status("full-old")
            assert old_status.assigned_to is None
            assert old_status.backoff_until is not None
        finally:
            await monitor.stop()
