"""Tests for WebhookEmitter — T-053."""

from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from elastic_agent.core.webhook_emitter import (
    DeadLetter,
    WebhookEmitter,
    WebhookTarget,
    compute_signature,
    verify_signature,
)


@pytest.fixture
def dead_letter_path(tmp_path):
    return tmp_path / "dead_letters.json"


@pytest.fixture
def emitter(dead_letter_path):
    return WebhookEmitter(
        dead_letter_path=dead_letter_path,
        retry_delays=[0.01, 0.01],
        send_timeout=5.0,
    )


class TestSignature:
    def test_compute_signature(self):
        sig = compute_signature(b'{"test": 1}', "secret", "2026-01-01T00:00:00")
        assert isinstance(sig, str)
        assert len(sig) == 64

    def test_verify_valid_signature(self):
        ts = "2026-05-19T00:00:00+00:00"
        payload = b'{"data": "test"}'
        sig = compute_signature(payload, "secret", ts)
        assert verify_signature(payload, "secret", sig, ts, max_age_seconds=999999999) is True

    def test_verify_invalid_signature(self):
        ts = "2026-05-19T00:00:00+00:00"
        payload = b'{"data": "test"}'
        assert verify_signature(payload, "secret", "badsig", ts, max_age_seconds=999999999) is False

    def test_verify_expired_timestamp(self):
        ts = "2020-01-01T00:00:00+00:00"
        payload = b'{"data": "test"}'
        sig = compute_signature(payload, "secret", ts)
        assert verify_signature(payload, "secret", sig, ts, max_age_seconds=300) is False


class TestWebhookTarget:
    def test_create_target(self):
        target = WebhookTarget("t1", "https://example.com", "secret")
        assert target.target_id == "t1"
        assert target.event_types is None
        assert target.enabled is True


class TestDeadLetter:
    def test_round_trip(self):
        dl = DeadLetter(
            event_id="e1",
            target_id="t1",
            url="https://example.com",
            event_type="task.completed",
            task_id="task-1",
            payload={"key": "value"},
            error="HTTP 500",
            attempts=3,
        )
        d = dl.to_dict()
        dl2 = DeadLetter.from_dict(d)
        assert dl2.event_id == "e1"
        assert dl2.attempts == 3
        assert dl2.payload == {"key": "value"}


class TestEmitterRegistration:
    def test_register(self, emitter):
        emitter.register("t1", "https://example.com", "secret")
        assert len(emitter.list_targets()) == 1
        target = emitter.get_target("t1")
        assert target is not None
        assert target.url == "https://example.com"

    def test_unregister(self, emitter):
        emitter.register("t1", "https://example.com", "secret")
        assert emitter.unregister("t1") is True
        assert emitter.get_target("t1") is None

    def test_unregister_nonexistent(self, emitter):
        assert emitter.unregister("nope") is False

    def test_register_with_event_filter(self, emitter):
        emitter.register("t1", "https://example.com", "secret", event_types=["task.completed"])
        target = emitter.get_target("t1")
        assert target.event_types == ["task.completed"]


class TestEmitterDelivery:
    @pytest.mark.asyncio
    async def test_successful_delivery(self, emitter):
        emitter.register("t1", "https://example.com/hook", "secret")

        mock_response = httpx.Response(200)
        with patch("elastic_agent.core.webhook_emitter.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.post = AsyncMock(return_value=mock_response)
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            await emitter.emit("task.completed", "task-1", {"status": "done"})
            await asyncio.sleep(0.1)

        mock_client.post.assert_called_once()
        call_kwargs = mock_client.post.call_args
        assert "X-Elastic-Agent-Signature" in call_kwargs.kwargs["headers"]
        assert "X-Elastic-Agent-Event" in call_kwargs.kwargs["headers"]

    @pytest.mark.asyncio
    async def test_event_type_filtering(self, emitter):
        emitter.register("t1", "https://example.com", "secret", event_types=["task.failed"])

        mock_response = httpx.Response(200)
        with patch("elastic_agent.core.webhook_emitter.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.post = AsyncMock(return_value=mock_response)
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            await emitter.emit("task.completed", "task-1", {})
            await asyncio.sleep(0.1)

        mock_client.post.assert_not_called()

    @pytest.mark.asyncio
    async def test_disabled_target_skipped(self, emitter):
        emitter.register("t1", "https://example.com", "secret")
        emitter.get_target("t1").enabled = False

        mock_response = httpx.Response(200)
        with patch("elastic_agent.core.webhook_emitter.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.post = AsyncMock(return_value=mock_response)
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            await emitter.emit("task.completed", "task-1", {})
            await asyncio.sleep(0.1)

        mock_client.post.assert_not_called()

    @pytest.mark.asyncio
    async def test_retry_and_dead_letter(self, dead_letter_path):
        emitter = WebhookEmitter(
            dead_letter_path=dead_letter_path,
            retry_delays=[0.01, 0.01],
            send_timeout=5.0,
        )
        emitter.register("t1", "https://example.com", "secret")

        mock_response = httpx.Response(500)
        with patch("elastic_agent.core.webhook_emitter.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.post = AsyncMock(return_value=mock_response)
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            await emitter.emit("task.failed", "task-1", {"error": "boom"})
            await asyncio.sleep(0.5)

        assert emitter.dead_letter_count == 1
        dl = emitter.dead_letters[0]
        assert dl.event_type == "task.failed"
        assert dl.task_id == "task-1"

        assert dead_letter_path.exists()
        stored = json.loads(dead_letter_path.read_text())
        assert len(stored) == 1

    @pytest.mark.asyncio
    async def test_dead_letter_persistence(self, dead_letter_path):
        emitter1 = WebhookEmitter(
            dead_letter_path=dead_letter_path,
            retry_delays=[0.01],
            send_timeout=5.0,
        )
        emitter1.register("t1", "https://example.com", "secret")

        mock_response = httpx.Response(500)
        with patch("elastic_agent.core.webhook_emitter.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.post = AsyncMock(return_value=mock_response)
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            await emitter1.emit("test.event", "t1", {})
            await asyncio.sleep(0.5)

        emitter2 = WebhookEmitter(
            dead_letter_path=dead_letter_path,
            retry_delays=[0.01],
        )
        assert emitter2.dead_letter_count == 1


class TestEmitterReplay:
    @pytest.mark.asyncio
    async def test_replay_success(self, dead_letter_path):
        dl_data = [DeadLetter(
            event_id="e1",
            target_id="t1",
            url="https://example.com",
            event_type="task.completed",
            task_id="task-1",
            payload={"event": "task.completed", "task_id": "task-1", "timestamp": "2026-01-01T00:00:00", "data": {}},
            error="HTTP 500",
            attempts=3,
        ).to_dict()]
        dead_letter_path.write_text(json.dumps(dl_data))

        emitter = WebhookEmitter(dead_letter_path=dead_letter_path)
        emitter.register("t1", "https://example.com", "secret")

        mock_response = httpx.Response(200)
        with patch("elastic_agent.core.webhook_emitter.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.post = AsyncMock(return_value=mock_response)
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            results = await emitter.replay_dead_letters()

        assert results["e1"] is True
        assert emitter.dead_letter_count == 0

    @pytest.mark.asyncio
    async def test_replay_failure_re_queues(self, dead_letter_path):
        dl_data = [DeadLetter(
            event_id="e1",
            target_id="t1",
            url="https://example.com",
            event_type="task.completed",
            task_id="task-1",
            payload={"event": "task.completed", "task_id": "task-1", "timestamp": "2026-01-01T00:00:00", "data": {}},
            error="HTTP 500",
            attempts=3,
        ).to_dict()]
        dead_letter_path.write_text(json.dumps(dl_data))

        emitter = WebhookEmitter(dead_letter_path=dead_letter_path)
        emitter.register("t1", "https://example.com", "secret")

        mock_response = httpx.Response(500)
        with patch("elastic_agent.core.webhook_emitter.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.post = AsyncMock(return_value=mock_response)
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            results = await emitter.replay_dead_letters()

        assert results["e1"] is False
        assert emitter.dead_letter_count == 1

    @pytest.mark.asyncio
    async def test_replay_missing_target(self, dead_letter_path):
        dl_data = [DeadLetter(
            event_id="e1",
            target_id="missing",
            url="https://example.com",
            event_type="task.completed",
            task_id="task-1",
            payload={},
            error="HTTP 500",
            attempts=3,
        ).to_dict()]
        dead_letter_path.write_text(json.dumps(dl_data))

        emitter = WebhookEmitter(dead_letter_path=dead_letter_path)
        results = await emitter.replay_dead_letters()

        assert results["e1"] is False
        assert emitter.dead_letter_count == 1


class TestEmitterStop:
    @pytest.mark.asyncio
    async def test_stop_cancels_pending(self, emitter):
        emitter.register("t1", "https://example.com", "secret")
        await emitter.stop()


class TestPayloadStructure:
    @pytest.mark.asyncio
    async def test_payload_contains_required_fields(self, emitter):
        emitter.register("t1", "https://example.com", "secret")

        captured_body = None

        async def capture_post(url, content=None, headers=None):
            nonlocal captured_body
            captured_body = json.loads(content)
            return httpx.Response(200)

        with patch("elastic_agent.core.webhook_emitter.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.post = capture_post
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            await emitter.emit("task.completed", "task-42", {"phase": 3})
            await asyncio.sleep(0.1)

        assert captured_body is not None
        assert "event_id" in captured_body
        assert captured_body["event"] == "task.completed"
        assert captured_body["task_id"] == "task-42"
        assert "timestamp" in captured_body
        assert captured_body["data"] == {"phase": 3}


# ===========================================================================
# T-141: WebhookEmitter extended — HMAC signing, retry delays, dead letter
#        queue, idempotency
# ===========================================================================


class TestT141HMACSignature:
    """T-141: HMAC signing tests."""

    def test_different_secrets_different_signatures(self):
        payload = b'{"test": 1}'
        ts = "2026-01-01T00:00:00"
        sig1 = compute_signature(payload, "secret-a", ts)
        sig2 = compute_signature(payload, "secret-b", ts)
        assert sig1 != sig2

    def test_different_payloads_different_signatures(self):
        ts = "2026-01-01T00:00:00"
        sig1 = compute_signature(b'{"a": 1}', "secret", ts)
        sig2 = compute_signature(b'{"b": 2}', "secret", ts)
        assert sig1 != sig2

    def test_different_timestamps_different_signatures(self):
        payload = b'{"test": 1}'
        sig1 = compute_signature(payload, "secret", "2026-01-01T00:00:00")
        sig2 = compute_signature(payload, "secret", "2026-01-02T00:00:00")
        assert sig1 != sig2

    def test_signature_is_hex_sha256(self):
        sig = compute_signature(b'test', "secret", "ts")
        assert len(sig) == 64
        int(sig, 16)

    def test_verify_with_large_max_age(self):
        ts = "2020-01-01T00:00:00+00:00"
        payload = b'{"data": "old"}'
        sig = compute_signature(payload, "secret", ts)
        assert verify_signature(payload, "secret", sig, ts, max_age_seconds=999999999) is True

    def test_verify_rejects_tampered_payload(self):
        ts = "2026-05-19T00:00:00+00:00"
        sig = compute_signature(b'{"original": true}', "secret", ts)
        assert verify_signature(b'{"tampered": true}', "secret", sig, ts, max_age_seconds=999999999) is False


class TestT141RetryDelays:
    """T-141: Retry delay behavior."""

    @pytest.mark.asyncio
    async def test_retry_count_matches_delays(self, dead_letter_path):
        emitter = WebhookEmitter(
            dead_letter_path=dead_letter_path,
            retry_delays=[0.01, 0.01, 0.01],
            send_timeout=5.0,
        )
        emitter.register("t1", "https://example.com", "secret")

        call_count = 0

        async def count_post(url, content=None, headers=None):
            nonlocal call_count
            call_count += 1
            return httpx.Response(500)

        with patch("elastic_agent.core.webhook_emitter.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.post = count_post
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            await emitter.emit("test.event", "t1", {})
            await asyncio.sleep(0.5)

        assert call_count == 4  # 1 initial + 3 retries
        assert emitter.dead_letter_count == 1

    @pytest.mark.asyncio
    async def test_success_on_retry_no_dead_letter(self, dead_letter_path):
        emitter = WebhookEmitter(
            dead_letter_path=dead_letter_path,
            retry_delays=[0.01],
            send_timeout=5.0,
        )
        emitter.register("t1", "https://example.com", "secret")

        attempt = 0

        async def fail_then_succeed(url, content=None, headers=None):
            nonlocal attempt
            attempt += 1
            if attempt == 1:
                return httpx.Response(500)
            return httpx.Response(200)

        with patch("elastic_agent.core.webhook_emitter.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.post = fail_then_succeed
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            await emitter.emit("test.event", "t1", {})
            await asyncio.sleep(0.5)

        assert emitter.dead_letter_count == 0

    @pytest.mark.asyncio
    async def test_connection_error_triggers_retry(self, dead_letter_path):
        emitter = WebhookEmitter(
            dead_letter_path=dead_letter_path,
            retry_delays=[0.01],
            send_timeout=5.0,
        )
        emitter.register("t1", "https://example.com", "secret")

        async def raise_error(url, content=None, headers=None):
            raise httpx.ConnectError("Connection refused")

        with patch("elastic_agent.core.webhook_emitter.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.post = raise_error
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            await emitter.emit("test.event", "t1", {})
            await asyncio.sleep(0.5)

        assert emitter.dead_letter_count == 1
        dl = emitter.dead_letters[0]
        assert "Connection refused" in dl.error


class TestT141DeadLetterQueue:
    """T-141: Dead letter queue management."""

    @pytest.mark.asyncio
    async def test_dead_letter_count_tracking(self, dead_letter_path):
        emitter = WebhookEmitter(
            dead_letter_path=dead_letter_path,
            retry_delays=[0.01],
            send_timeout=5.0,
        )
        emitter.register("t1", "https://example.com", "secret")

        with patch("elastic_agent.core.webhook_emitter.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.post = AsyncMock(return_value=httpx.Response(500))
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            for i in range(3):
                await emitter.emit(f"test.event.{i}", f"task-{i}", {})
            await asyncio.sleep(1.0)

        assert emitter.dead_letter_count == 3

    @pytest.mark.asyncio
    async def test_dead_letter_persistence_roundtrip(self, dead_letter_path):
        emitter1 = WebhookEmitter(
            dead_letter_path=dead_letter_path,
            retry_delays=[0.01],
        )
        emitter1.register("t1", "https://example.com", "secret")

        with patch("elastic_agent.core.webhook_emitter.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.post = AsyncMock(return_value=httpx.Response(500))
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            await emitter1.emit("task.failed", "task-1", {"error": "crash"})
            await asyncio.sleep(0.5)

        assert dead_letter_path.exists()
        emitter2 = WebhookEmitter(dead_letter_path=dead_letter_path)
        assert emitter2.dead_letter_count == 1
        dl = emitter2.dead_letters[0]
        assert dl.event_type == "task.failed"
        assert dl.task_id == "task-1"

    @pytest.mark.asyncio
    async def test_dead_letter_replay_clears_on_success(self, dead_letter_path):
        dl_data = [DeadLetter(
            event_id="e1",
            target_id="t1",
            url="https://example.com",
            event_type="task.completed",
            task_id="task-1",
            payload={"event_id": "e1", "event": "task.completed", "task_id": "task-1",
                      "timestamp": "2026-01-01T00:00:00", "data": {}},
            error="HTTP 500",
            attempts=3,
        ).to_dict()]
        dead_letter_path.write_text(json.dumps(dl_data))

        emitter = WebhookEmitter(dead_letter_path=dead_letter_path)
        emitter.register("t1", "https://example.com", "secret")

        with patch("elastic_agent.core.webhook_emitter.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.post = AsyncMock(return_value=httpx.Response(200))
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            results = await emitter.replay_dead_letters()

        assert results["e1"] is True
        assert emitter.dead_letter_count == 0


class TestT141Idempotency:
    """T-141: Idempotent event ID generation."""

    @pytest.mark.asyncio
    async def test_each_emit_gets_unique_event_id(self, emitter):
        emitter.register("t1", "https://example.com", "secret")
        event_ids = set()

        async def capture_post(url, content=None, headers=None):
            body = json.loads(content)
            event_ids.add(body["event_id"])
            return httpx.Response(200)

        with patch("elastic_agent.core.webhook_emitter.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.post = capture_post
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            for i in range(5):
                await emitter.emit("test.event", f"task-{i}", {"i": i})
            await asyncio.sleep(0.2)

        assert len(event_ids) == 5

    @pytest.mark.asyncio
    async def test_event_id_is_valid_uuid(self, emitter):
        emitter.register("t1", "https://example.com", "secret")
        captured_id = None

        async def capture_post(url, content=None, headers=None):
            nonlocal captured_id
            body = json.loads(content)
            captured_id = body["event_id"]
            return httpx.Response(200)

        with patch("elastic_agent.core.webhook_emitter.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.post = capture_post
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            await emitter.emit("test.event", "task-1", {})
            await asyncio.sleep(0.1)

        import uuid
        assert captured_id is not None
        uuid.UUID(captured_id)

    @pytest.mark.asyncio
    async def test_multiple_targets_same_event(self, dead_letter_path):
        emitter = WebhookEmitter(dead_letter_path=dead_letter_path)
        emitter.register("t1", "https://example1.com", "secret1")
        emitter.register("t2", "https://example2.com", "secret2")

        delivered_urls = []

        async def capture_post(url, content=None, headers=None):
            delivered_urls.append(url)
            return httpx.Response(200)

        with patch("elastic_agent.core.webhook_emitter.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.post = capture_post
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            await emitter.emit("task.completed", "task-1", {})
            await asyncio.sleep(0.2)

        assert set(delivered_urls) == {"https://example1.com", "https://example2.com"}

    @pytest.mark.asyncio
    async def test_headers_include_event_id(self, emitter):
        emitter.register("t1", "https://example.com", "secret")
        captured_headers = None

        async def capture_post(url, content=None, headers=None):
            nonlocal captured_headers
            captured_headers = headers
            return httpx.Response(200)

        with patch("elastic_agent.core.webhook_emitter.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.post = capture_post
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            await emitter.emit("task.done", "t1", {})
            await asyncio.sleep(0.1)

        assert captured_headers is not None
        assert "X-Elastic-Agent-Signature" in captured_headers
        assert "X-Elastic-Agent-Event" in captured_headers
        assert "X-Elastic-Agent-Event-Id" in captured_headers
        assert captured_headers["X-Elastic-Agent-Event"] == "task.done"
