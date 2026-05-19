"""Tests for FILE_SYNCED webhook notification (T-014)."""

from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, patch, MagicMock

import pytest

from elastic_agent.core.event_bus import EventBus
from elastic_agent.core.webhook import (
    FileSyncedNotifier,
    WebhookTarget,
    compute_signature,
    verify_signature,
)


class TestSignature:
    def test_compute_signature_deterministic(self):
        payload = b'{"event": "test"}'
        sig1 = compute_signature(payload, "secret")
        sig2 = compute_signature(payload, "secret")
        assert sig1 == sig2

    def test_compute_signature_hex_format(self):
        sig = compute_signature(b"hello", "key")
        assert isinstance(sig, str)
        assert all(c in "0123456789abcdef" for c in sig)

    def test_different_payload_different_signature(self):
        sig1 = compute_signature(b"payload-a", "secret")
        sig2 = compute_signature(b"payload-b", "secret")
        assert sig1 != sig2

    def test_different_secret_different_signature(self):
        sig1 = compute_signature(b"payload", "secret-a")
        sig2 = compute_signature(b"payload", "secret-b")
        assert sig1 != sig2

    def test_verify_valid_signature(self):
        payload = b'{"event": "task.file.synced"}'
        secret = "my-secret"
        sig = compute_signature(payload, secret)
        assert verify_signature(payload, secret, sig) is True

    def test_verify_invalid_signature(self):
        payload = b'{"event": "task.file.synced"}'
        assert verify_signature(payload, "secret", "invalid-sig") is False

    def test_verify_wrong_secret(self):
        payload = b"data"
        sig = compute_signature(payload, "correct-secret")
        assert verify_signature(payload, "wrong-secret", sig) is False


class TestWebhookTarget:
    def test_defaults(self):
        target = WebhookTarget(url="https://example.com/hook", secret="s3cret")
        assert target.enabled is True
        assert "FILE_SYNCED" in target.event_types

    def test_custom_event_types(self):
        target = WebhookTarget(
            url="https://example.com/hook",
            secret="s3cret",
            event_types=["LOG", "FILE_SYNCED"],
        )
        assert "LOG" in target.event_types
        assert "FILE_SYNCED" in target.event_types


class TestFileSyncedNotifier:
    @pytest.fixture
    def bus(self):
        return EventBus()

    @pytest.fixture
    def notifier(self, bus):
        n = FileSyncedNotifier(bus, retry_delays=[0.01], send_timeout=2.0)
        yield n
        n.stop()

    def test_add_target(self, notifier):
        target = WebhookTarget(url="https://a.com/hook", secret="s")
        notifier.add_target(target)
        assert len(notifier._targets) == 1

    def test_remove_target(self, notifier):
        notifier.add_target(WebhookTarget(url="https://a.com/hook", secret="s"))
        notifier.add_target(WebhookTarget(url="https://b.com/hook", secret="s"))
        assert notifier.remove_target("https://a.com/hook") is True
        assert len(notifier._targets) == 1
        assert notifier._targets[0].url == "https://b.com/hook"

    def test_remove_nonexistent_target(self, notifier):
        assert notifier.remove_target("https://not.exist") is False

    def test_start_subscribes(self, bus, notifier):
        assert bus.subscriber_count("FILE_SYNCED") == 0
        notifier.start()
        assert bus.subscriber_count("FILE_SYNCED") == 1

    def test_start_idempotent(self, bus, notifier):
        notifier.start()
        notifier.start()
        assert bus.subscriber_count("FILE_SYNCED") == 1

    def test_stop_unsubscribes(self, bus, notifier):
        notifier.start()
        assert bus.subscriber_count("FILE_SYNCED") == 1
        notifier.stop()
        assert bus.subscriber_count("FILE_SYNCED") == 0

    @pytest.mark.asyncio
    async def test_file_synced_triggers_delivery(self, bus, notifier):
        target = WebhookTarget(url="https://example.com/hook", secret="test-secret")
        notifier.add_target(target)
        notifier.start()

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=mock_response)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("elastic_agent.core.webhook.httpx.AsyncClient", return_value=mock_client):
            await bus.emit("FILE_SYNCED", "worker-1", {
                "task_id": "task-1",
                "path": "/out/ch01.md",
                "oss_key": "tasks/task-1/ch01.md",
                "synced_at": "2026-05-19T12:00:00Z",
                "md5": "abc123",
            })
            await asyncio.sleep(0.1)

        mock_client.post.assert_called_once()
        call_args = mock_client.post.call_args
        assert call_args[1]["headers"]["X-Webhook-Event"] == "task.file.synced"
        assert "X-Webhook-Signature" in call_args[1]["headers"]

        body = json.loads(call_args[1]["content"])
        assert body["event"] == "task.file.synced"
        assert body["worker_id"] == "worker-1"
        assert body["data"]["task_id"] == "task-1"

    @pytest.mark.asyncio
    async def test_disabled_target_skipped(self, bus, notifier):
        target = WebhookTarget(url="https://example.com/hook", secret="s", enabled=False)
        notifier.add_target(target)
        notifier.start()

        with patch("elastic_agent.core.webhook.httpx.AsyncClient") as mock_cls:
            await bus.emit("FILE_SYNCED", "w1", {"task_id": "t1"})
            await asyncio.sleep(0.05)
            mock_cls.assert_not_called()

    @pytest.mark.asyncio
    async def test_non_matching_event_type_skipped(self, bus, notifier):
        target = WebhookTarget(url="https://example.com/hook", secret="s", event_types=["LOG"])
        notifier.add_target(target)
        notifier.start()

        with patch("elastic_agent.core.webhook.httpx.AsyncClient") as mock_cls:
            await bus.emit("FILE_SYNCED", "w1", {"task_id": "t1"})
            await asyncio.sleep(0.05)
            mock_cls.assert_not_called()

    @pytest.mark.asyncio
    async def test_delivery_retries_on_failure(self, bus):
        notifier = FileSyncedNotifier(bus, retry_delays=[0.01, 0.01], send_timeout=2.0)
        target = WebhookTarget(url="https://example.com/hook", secret="s")
        notifier.add_target(target)
        notifier.start()

        fail_response = MagicMock()
        fail_response.status_code = 503
        ok_response = MagicMock()
        ok_response.status_code = 200

        mock_client = AsyncMock()
        mock_client.post = AsyncMock(side_effect=[fail_response, ok_response])
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("elastic_agent.core.webhook.httpx.AsyncClient", return_value=mock_client):
            await bus.emit("FILE_SYNCED", "w1", {"task_id": "t1"})
            await asyncio.sleep(0.2)

        assert mock_client.post.call_count == 2
        notifier.stop()

    @pytest.mark.asyncio
    async def test_signature_in_header_is_valid(self, bus, notifier):
        secret = "verify-me"
        target = WebhookTarget(url="https://example.com/hook", secret=secret)
        notifier.add_target(target)
        notifier.start()

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=mock_response)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("elastic_agent.core.webhook.httpx.AsyncClient", return_value=mock_client):
            await bus.emit("FILE_SYNCED", "w1", {"task_id": "t1"})
            await asyncio.sleep(0.1)

        call_args = mock_client.post.call_args
        body_bytes = call_args[1]["content"]
        sig = call_args[1]["headers"]["X-Webhook-Signature"]
        assert verify_signature(body_bytes, secret, sig) is True
