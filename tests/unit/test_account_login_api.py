"""Tests for the interactive Codex OTP REST bridge."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import HTTPException
from pydantic import ValidationError

from elastic_agent.api.routes import account_login


@pytest.mark.asyncio
async def test_list_exposes_only_non_secret_challenge_metadata(monkeypatch):
    coordinator = MagicMock()
    coordinator.list_otp_challenges.return_value = [{
        "login_request_id": "login-1",
        "worker_id": "worker-1",
        "account_id": "codex-1",
        "challenge_id": "a" * 32,
        "expires_at": 1_900_000_000,
        "status": "awaiting_otp",
    }]
    monkeypatch.setattr(account_login, "_coordinator", lambda: coordinator)

    result = await account_login.list_account_login_attempts()

    assert result["total"] == 1
    assert "code" not in result["attempts"][0]
    assert "password" not in result["attempts"][0]


@pytest.mark.asyncio
async def test_submit_forwards_code_without_putting_it_in_response_or_repr(
    monkeypatch,
):
    coordinator = AsyncMock()
    coordinator.submit_otp.return_value = {
        "login_request_id": "login-1",
        "account_id": "codex-1",
        "status": "verifying_otp",
    }
    monkeypatch.setattr(account_login, "_coordinator", lambda: coordinator)
    body = account_login.OtpSubmission(
        challenge_id="a" * 32, code="123456",
    )

    result = await account_login.submit_account_login_otp("login-1", body)

    coordinator.submit_otp.assert_awaited_once_with(
        "login-1", "a" * 32, "123456"
    )
    assert "123456" not in repr(body)
    assert "code" not in result


@pytest.mark.asyncio
async def test_submit_maps_stale_challenge_to_404(monkeypatch):
    coordinator = AsyncMock()
    coordinator.submit_otp.side_effect = KeyError("not active")
    monkeypatch.setattr(account_login, "_coordinator", lambda: coordinator)

    with pytest.raises(HTTPException) as raised:
        await account_login.submit_account_login_otp(
            "login-1",
            account_login.OtpSubmission(
                challenge_id="a" * 32, code="123456"
            ),
        )

    assert raised.value.status_code == 404


def test_submission_rejects_non_hex_challenge_id_before_forwarding():
    with pytest.raises(ValidationError, match="32-character lowercase hex"):
        account_login.OtpSubmission(
            challenge_id="</div><script>alert(1)</script>",
            code="123456",
        )
