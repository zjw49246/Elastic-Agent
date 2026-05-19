"""Tests for Worker-side QuotaChecker — T-043."""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from elastic_agent.core.claude_oauth import write_credentials
from elastic_agent.worker.quota_checker import (
    AuthError,
    QuotaCheckResult,
    QuotaChecker,
    RateLimitError,
    UsageAPIError,
)


class MockUsageClient:
    """Mock HTTP client for quota checker tests."""

    def __init__(self):
        self.usage_response = {
            "five_hour": {"utilization": 45, "resets_at": "2026-05-19T17:34:56Z"},
            "seven_day": {"utilization": 62, "resets_at": "2026-05-26T00:00:00Z"},
        }
        self.refresh_response = {
            "access_token": "refreshed-token",
            "refresh_token": "new-refresh",
            "expires_in": 3600,
        }
        self.usage_calls = 0
        self.refresh_calls = 0
        self.raise_on_usage = None
        self.raise_on_refresh = False

    async def get_usage(self, access_token: str) -> dict:
        self.usage_calls += 1
        if self.raise_on_usage:
            raise self.raise_on_usage
        return self.usage_response

    async def post_form(self, url: str, body: str) -> dict:
        self.refresh_calls += 1
        if self.raise_on_refresh:
            raise Exception("refresh failed")
        return self.refresh_response


@pytest.fixture
def mock_client():
    return MockUsageClient()


@pytest.fixture
def config_dir(tmp_path):
    d = tmp_path / "claude-config"
    d.mkdir()
    # Write valid credentials
    write_credentials(str(d), {
        "accessToken": "test-access-token",
        "refreshToken": "test-refresh-token",
        "expiresAt": int((time.time() + 3600) * 1000),  # expires in 1h
    })
    return str(d)


@pytest.fixture
def expired_config_dir(tmp_path):
    d = tmp_path / "claude-expired"
    d.mkdir()
    write_credentials(str(d), {
        "accessToken": "expired-token",
        "refreshToken": "old-refresh",
        "expiresAt": int((time.time() - 600) * 1000),  # expired 10min ago
    })
    return str(d)


class TestQuotaCheckResult:
    def test_success_result(self):
        r = QuotaCheckResult(
            account_id="a1",
            success=True,
            five_hour_pct=45.0,
            seven_day_pct=62.0,
        )
        assert r.success
        assert r.five_hour_pct == 45.0
        assert r.error is None
        assert not r.stale
        assert not r.needs_relogin

    def test_error_result(self):
        r = QuotaCheckResult(
            account_id="a1",
            error="rate_limited",
            stale=True,
        )
        assert not r.success
        assert r.stale
        assert r.error == "rate_limited"


class TestQuotaChecker:
    @pytest.mark.asyncio
    async def test_check_account_success(self, mock_client, config_dir):
        checker = QuotaChecker(http_client=mock_client)
        result = await checker.check_account("prod-1", config_dir)

        assert result.success
        assert result.five_hour_pct == 45.0
        assert result.seven_day_pct == 62.0
        assert result.five_hour_resets_at == "2026-05-19T17:34:56Z"
        assert result.available
        assert not result.stale
        assert mock_client.usage_calls == 1

    @pytest.mark.asyncio
    async def test_check_account_no_credentials(self, mock_client, tmp_path):
        checker = QuotaChecker(http_client=mock_client)
        result = await checker.check_account("prod-1", str(tmp_path / "nonexistent"))

        assert not result.success
        assert "not found" in result.error

    @pytest.mark.asyncio
    async def test_check_account_token_refresh(self, mock_client, expired_config_dir):
        checker = QuotaChecker(http_client=mock_client)
        result = await checker.check_account("prod-1", expired_config_dir)

        assert result.success
        assert mock_client.refresh_calls == 1
        assert mock_client.usage_calls == 1

    @pytest.mark.asyncio
    async def test_check_account_rate_limited_with_cache(self, mock_client, config_dir):
        checker = QuotaChecker(http_client=mock_client)

        # First call succeeds (populates cache)
        result1 = await checker.check_account("prod-1", config_dir)
        assert result1.success

        # Second call rate-limited
        mock_client.raise_on_usage = RateLimitError("rate limited")
        result2 = await checker.check_account("prod-1", config_dir)
        assert result2.stale
        assert result2.five_hour_pct == 45.0  # from cache

    @pytest.mark.asyncio
    async def test_check_account_rate_limited_no_cache(self, mock_client, config_dir):
        mock_client.raise_on_usage = RateLimitError("rate limited")
        checker = QuotaChecker(http_client=mock_client)
        result = await checker.check_account("prod-1", config_dir)
        assert not result.success
        assert result.error == "rate_limited"
        assert result.stale

    @pytest.mark.asyncio
    async def test_check_account_auth_error(self, mock_client, config_dir):
        mock_client.raise_on_usage = AuthError("auth error")
        checker = QuotaChecker(http_client=mock_client)
        result = await checker.check_account("prod-1", config_dir)
        assert not result.success
        assert result.needs_relogin

    @pytest.mark.asyncio
    async def test_add_remove_slot(self, mock_client):
        checker = QuotaChecker(http_client=mock_client)
        assert len(checker._slots) == 0

        checker.add_slot("prod-1", "/config/1")
        assert len(checker._slots) == 1

        checker.add_slot("prod-2", "/config/2")
        assert len(checker._slots) == 2

        # Update existing slot
        checker.add_slot("prod-1", "/config/new")
        assert len(checker._slots) == 2
        assert checker._slots[0]["config_dir"] == "/config/new"

        checker.remove_slot("prod-1")
        assert len(checker._slots) == 1
        assert checker._slots[0]["account_id"] == "prod-2"

    @pytest.mark.asyncio
    async def test_check_all(self, mock_client, config_dir):
        callback = AsyncMock()
        checker = QuotaChecker(
            active_slots=[
                {"account_id": "prod-1", "config_dir": config_dir},
            ],
            on_quota_status=callback,
            http_client=mock_client,
        )

        results = await checker.check_all()
        assert len(results) == 1
        assert results[0].success

        callback.assert_called_once()
        status_data = callback.call_args[0][0]
        assert status_data["account_id"] == "prod-1"
        assert status_data["five_hour_pct"] == 45.0

    @pytest.mark.asyncio
    async def test_start_stop(self, mock_client):
        checker = QuotaChecker(http_client=mock_client, delay_min=0, delay_max=0)
        await checker.start()
        assert checker._running
        assert checker._task is not None

        await checker.stop()
        assert not checker._running
        assert checker._task is None

    @pytest.mark.asyncio
    async def test_start_idempotent(self, mock_client):
        checker = QuotaChecker(http_client=mock_client)
        await checker.start()
        task1 = checker._task

        await checker.start()  # Should not create new task
        assert checker._task is task1

        await checker.stop()

    @pytest.mark.asyncio
    async def test_backoff_increases_on_rate_limit(self, mock_client, config_dir):
        checker = QuotaChecker(http_client=mock_client)
        assert "prod-1" not in checker._backoff

        # Successful check: no backoff
        await checker.check_account("prod-1", config_dir)
        assert "prod-1" not in checker._backoff

        # Rate limited: backoff should be set
        mock_client.raise_on_usage = RateLimitError("rate limited")
        await checker.check_account("prod-1", config_dir)
        assert "prod-1" in checker._backoff

    @pytest.mark.asyncio
    async def test_callback_on_error_not_called(self, mock_client, config_dir):
        mock_client.raise_on_usage = UsageAPIError("server error")
        callback = AsyncMock()
        checker = QuotaChecker(
            active_slots=[{"account_id": "prod-1", "config_dir": config_dir}],
            on_quota_status=callback,
            http_client=mock_client,
        )
        results = await checker.check_all()
        assert not results[0].success
        # Callback should not be called for failed checks
        callback.assert_not_called()

    @pytest.mark.asyncio
    async def test_report_status_format(self, mock_client, config_dir):
        reported = []

        async def capture(data):
            reported.append(data)

        checker = QuotaChecker(
            active_slots=[{"account_id": "prod-1", "config_dir": config_dir}],
            on_quota_status=capture,
            http_client=mock_client,
        )
        await checker.check_all()

        assert len(reported) == 1
        status = reported[0]
        assert status["type"] == "QUOTA_STATUS"
        assert status["account_id"] == "prod-1"
        assert "five_hour_pct" in status
        assert "seven_day_pct" in status
        assert "five_hour_resets_at" in status
        assert "seven_day_resets_at" in status
        assert "available" in status
        assert "stale" in status
        assert "needs_relogin" in status


class TestQuotaCheckerExceptions:
    def test_rate_limit_error(self):
        e = RateLimitError("too fast")
        assert str(e) == "too fast"

    def test_auth_error(self):
        e = AuthError("bad token")
        assert str(e) == "bad token"

    def test_usage_api_error(self):
        e = UsageAPIError("server error")
        assert str(e) == "server error"
