"""Unit tests for enhanced credential protocol messages (T-047)."""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from elastic_agent.core.protocols.messages import (
    CredentialExhaustedMessage,
    CredentialLoginMessage,
    CredentialLoginResultMessage,
    CredentialRotateMessage,
    QuotaStatusMessage,
    parse_message,
)


class TestCredentialLoginResultMessage:
    """Tests for CredentialLoginResultMessage (Worker -> Manager response)."""

    def test_serialization_success(self):
        msg = CredentialLoginResultMessage(
            account_id="acct-123",
            slot_index=0,
            success=True,
            expires_at=datetime(2026, 6, 1, 12, 0, 0, tzinfo=timezone.utc),
        )
        data = json.loads(msg.model_dump_json())
        assert data["type"] == "CREDENTIAL_LOGIN_RESULT"
        assert data["account_id"] == "acct-123"
        assert data["slot_index"] == 0
        assert data["success"] is True
        assert data["error"] is None
        assert "expires_at" in data

    def test_serialization_failure(self):
        msg = CredentialLoginResultMessage(
            account_id="acct-456",
            slot_index=1,
            success=False,
            error="invalid credentials",
        )
        data = json.loads(msg.model_dump_json())
        assert data["success"] is False
        assert data["error"] == "invalid credentials"
        assert data["expires_at"] is None

    def test_deserialization_via_parse_message(self):
        raw = {
            "type": "CREDENTIAL_LOGIN_RESULT",
            "account_id": "acct-789",
            "slot_index": 2,
            "success": True,
        }
        msg = parse_message(raw)
        assert isinstance(msg, CredentialLoginResultMessage)
        assert msg.account_id == "acct-789"
        assert msg.slot_index == 2
        assert msg.success is True

    def test_roundtrip_json(self):
        original = CredentialLoginResultMessage(
            account_id="acct-rt",
            slot_index=3,
            success=True,
            expires_at=datetime(2026, 7, 1, 0, 0, 0, tzinfo=timezone.utc),
        )
        json_str = original.model_dump_json()
        restored = parse_message(json_str)
        assert isinstance(restored, CredentialLoginResultMessage)
        assert restored.account_id == original.account_id
        assert restored.slot_index == original.slot_index
        assert restored.success == original.success
        assert restored.expires_at == original.expires_at


class TestCredentialExhaustedMessage:
    """Tests for CredentialExhaustedMessage (internal event)."""

    def test_serialization(self):
        msg = CredentialExhaustedMessage(
            worker_id="worker-1",
            slot_index=0,
            account_id="acct-100",
            reason="quota exceeded in 5-hour window",
        )
        data = json.loads(msg.model_dump_json())
        assert data["type"] == "CREDENTIAL_EXHAUSTED"
        assert data["worker_id"] == "worker-1"
        assert data["slot_index"] == 0
        assert data["account_id"] == "acct-100"
        assert data["reason"] == "quota exceeded in 5-hour window"

    def test_deserialization_via_parse_message(self):
        raw = {
            "type": "CREDENTIAL_EXHAUSTED",
            "worker_id": "worker-2",
            "slot_index": 1,
            "account_id": "acct-200",
            "reason": "rate limited",
        }
        msg = parse_message(raw)
        assert isinstance(msg, CredentialExhaustedMessage)
        assert msg.worker_id == "worker-2"
        assert msg.reason == "rate limited"

    def test_roundtrip_json(self):
        original = CredentialExhaustedMessage(
            worker_id="worker-3",
            slot_index=2,
            account_id="acct-300",
            reason="token limit reached",
        )
        json_str = original.model_dump_json()
        restored = parse_message(json_str)
        assert isinstance(restored, CredentialExhaustedMessage)
        assert restored.worker_id == original.worker_id
        assert restored.account_id == original.account_id
        assert restored.reason == original.reason


class TestEnhancedQuotaStatusMessage:
    """Tests for QuotaStatusMessage with new fields."""

    def test_new_fields_defaults(self):
        msg = QuotaStatusMessage(
            task_id="task-1",
            account_id="acct-1",
            usage_percent=45.5,
        )
        assert msg.five_hour_pct == 0.0
        assert msg.seven_day_pct == 0.0
        assert msg.five_hour_resets_at is None
        assert msg.seven_day_resets_at is None
        assert msg.available is True

    def test_new_fields_set(self):
        five_h = datetime(2026, 5, 19, 17, 0, 0, tzinfo=timezone.utc)
        seven_d = datetime(2026, 5, 25, 0, 0, 0, tzinfo=timezone.utc)
        msg = QuotaStatusMessage(
            task_id="task-2",
            account_id="acct-2",
            usage_percent=80.0,
            five_hour_pct=75.0,
            seven_day_pct=60.0,
            five_hour_resets_at=five_h,
            seven_day_resets_at=seven_d,
            available=False,
        )
        assert msg.five_hour_pct == 75.0
        assert msg.seven_day_pct == 60.0
        assert msg.five_hour_resets_at == five_h
        assert msg.seven_day_resets_at == seven_d
        assert msg.available is False

    def test_backwards_compat_existing_fields(self):
        reset_at = datetime(2026, 5, 20, 0, 0, 0, tzinfo=timezone.utc)
        msg = QuotaStatusMessage(
            task_id="task-3",
            account_id="acct-3",
            usage_percent=50.0,
            remaining_tokens=10000,
            window_resets_at=reset_at,
        )
        assert msg.remaining_tokens == 10000
        assert msg.window_resets_at == reset_at

    def test_roundtrip_with_all_fields(self):
        original = QuotaStatusMessage(
            task_id="task-4",
            account_id="acct-4",
            usage_percent=90.0,
            remaining_tokens=500,
            window_resets_at=datetime(2026, 5, 20, 0, 0, 0, tzinfo=timezone.utc),
            five_hour_pct=85.0,
            seven_day_pct=70.0,
            five_hour_resets_at=datetime(2026, 5, 19, 18, 0, 0, tzinfo=timezone.utc),
            seven_day_resets_at=datetime(2026, 5, 26, 0, 0, 0, tzinfo=timezone.utc),
            available=False,
        )
        json_str = original.model_dump_json()
        restored = parse_message(json_str)
        assert isinstance(restored, QuotaStatusMessage)
        assert restored.five_hour_pct == 85.0
        assert restored.seven_day_pct == 70.0
        assert restored.available is False
        assert restored.remaining_tokens == 500


class TestParseAllCredentialMessages:
    """Test parse_message() with all credential-related message types."""

    def test_parse_credential_login(self):
        raw = {
            "type": "CREDENTIAL_LOGIN",
            "task_id": "task-1",
            "slot_index": 0,
            "credentials": {"email": "a@b.com", "password": "secret"},
            "config_dir": "/etc/config",
        }
        msg = parse_message(raw)
        assert isinstance(msg, CredentialLoginMessage)

    def test_parse_credential_rotate(self):
        raw = {
            "type": "CREDENTIAL_ROTATE",
            "task_id": "task-1",
            "slot_index": 1,
            "old_account_id": "acct-old",
            "new_credentials": {"email": "new@b.com", "token": "abc"},
            "config_dir": "/etc/config",
        }
        msg = parse_message(raw)
        assert isinstance(msg, CredentialRotateMessage)

    def test_parse_credential_login_result(self):
        raw = {
            "type": "CREDENTIAL_LOGIN_RESULT",
            "account_id": "acct-1",
            "slot_index": 0,
            "success": True,
        }
        msg = parse_message(raw)
        assert isinstance(msg, CredentialLoginResultMessage)

    def test_parse_credential_exhausted(self):
        raw = {
            "type": "CREDENTIAL_EXHAUSTED",
            "worker_id": "worker-1",
            "slot_index": 0,
            "account_id": "acct-1",
            "reason": "quota hit",
        }
        msg = parse_message(raw)
        assert isinstance(msg, CredentialExhaustedMessage)

    def test_parse_quota_status(self):
        raw = {
            "type": "QUOTA_STATUS",
            "task_id": "task-1",
            "account_id": "acct-1",
            "usage_percent": 55.0,
            "five_hour_pct": 40.0,
            "seven_day_pct": 30.0,
            "available": True,
        }
        msg = parse_message(raw)
        assert isinstance(msg, QuotaStatusMessage)
        assert msg.five_hour_pct == 40.0
        assert msg.seven_day_pct == 30.0
