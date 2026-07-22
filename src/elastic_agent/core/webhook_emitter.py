"""WebhookEmitter — event callback with HMAC signing, retry, and dead-letter queue.

T-053: Full-featured webhook system that pushes event notifications to
external systems. Extends the basic FileSyncedNotifier (T-014) with:
- Generic event support (any event type, not just FILE_SYNCED)
- Per-target event filtering
- Configurable retry delays (default: 1s, 5s, 30s, 5min, 30min)
- Dead-letter queue with JSON persistence
- Idempotent event IDs
- Replay capability for dead letters
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx

from elastic_agent.core.secure_store import (
    atomic_write_private,
    secure_state_directory,
    tighten_state_file,
)

logger = logging.getLogger(__name__)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def compute_signature(payload: bytes, secret: str, timestamp: str) -> str:
    signing_input = f"{timestamp}.".encode() + payload
    return hmac.new(secret.encode(), signing_input, hashlib.sha256).hexdigest()


def verify_signature(
    payload: bytes,
    secret: str,
    signature: str,
    timestamp: str,
    max_age_seconds: int = 300,
) -> bool:
    expected = compute_signature(payload, secret, timestamp)
    if not hmac.compare_digest(expected, signature):
        return False
    try:
        ts = datetime.fromisoformat(timestamp)
        age = (_utcnow() - ts).total_seconds()
        if abs(age) > max_age_seconds:
            return False
    except (ValueError, TypeError):
        return False
    return True


class WebhookTarget:
    def __init__(
        self,
        target_id: str,
        url: str,
        secret: str,
        event_types: list[str] | None = None,
        enabled: bool = True,
    ) -> None:
        self.target_id = target_id
        self.url = url
        self.secret = secret
        self.event_types = event_types  # None = all events
        self.enabled = enabled


class DeadLetter:
    def __init__(
        self,
        event_id: str,
        target_id: str,
        url: str,
        event_type: str,
        task_id: str,
        payload: dict[str, Any],
        error: str,
        attempts: int,
        created_at: str | None = None,
    ) -> None:
        self.event_id = event_id
        self.target_id = target_id
        self.url = url
        self.event_type = event_type
        self.task_id = task_id
        self.payload = payload
        self.error = error
        self.attempts = attempts
        self.created_at = created_at or _utcnow().isoformat()

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "target_id": self.target_id,
            "url": self.url,
            "event_type": self.event_type,
            "task_id": self.task_id,
            "payload": self.payload,
            "error": self.error,
            "attempts": self.attempts,
            "created_at": self.created_at,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> DeadLetter:
        return cls(**data)


class WebhookEmitter:
    """Pushes event notifications to registered external targets.

    Usage:
        emitter = WebhookEmitter(dead_letter_path="~/.elastic-agent/webhook_dead_letters.json")
        emitter.register("frontend", "https://example.com/webhook", "secret123")
        await emitter.emit("task.completed", "task-1", {"status": "done"})
    """

    def __init__(
        self,
        dead_letter_path: str | Path = "~/.elastic-agent/webhook_dead_letters.json",
        retry_delays: list[float] | None = None,
        send_timeout: float = 10.0,
    ) -> None:
        self._targets: dict[str, WebhookTarget] = {}
        self._dead_letter_path = Path(dead_letter_path).expanduser()
        self._retry_delays = retry_delays or [1, 5, 30, 300, 1800]
        self._send_timeout = send_timeout
        self._dead_letters: list[DeadLetter] = []
        self._pending_tasks: set[asyncio.Task] = set()
        self._lock = asyncio.Lock()
        self._load_dead_letters()

    def _load_dead_letters(self) -> None:
        secure_state_directory(self._dead_letter_path.parent)
        if self._dead_letter_path.exists():
            try:
                tighten_state_file(self._dead_letter_path)
                raw = json.loads(self._dead_letter_path.read_text(encoding="utf-8"))
                self._dead_letters = [DeadLetter.from_dict(d) for d in raw]
                logger.info("Loaded %d dead letters from %s", len(self._dead_letters), self._dead_letter_path)
            except Exception:
                logger.exception("Failed to load dead letters from %s", self._dead_letter_path)
                self._dead_letters = []

    def _flush_dead_letters(self) -> None:
        payload = [dl.to_dict() for dl in self._dead_letters]
        atomic_write_private(
            self._dead_letter_path,
            json.dumps(payload, indent=2, default=str),
        )

    def register(self, target_id: str, url: str, secret: str, event_types: list[str] | None = None) -> None:
        self._targets[target_id] = WebhookTarget(
            target_id=target_id,
            url=url,
            secret=secret,
            event_types=event_types,
        )
        logger.info("WebhookEmitter: registered target %s → %s", target_id, url)

    def unregister(self, target_id: str) -> bool:
        removed = self._targets.pop(target_id, None)
        if removed:
            logger.info("WebhookEmitter: unregistered target %s", target_id)
        return removed is not None

    def get_target(self, target_id: str) -> WebhookTarget | None:
        return self._targets.get(target_id)

    def list_targets(self) -> list[WebhookTarget]:
        return list(self._targets.values())

    async def emit(self, event_type: str, task_id: str, data: dict[str, Any]) -> None:
        """Emit an event to all matching registered targets.

        Each delivery runs as a background task with retry logic.
        """
        event_id = str(uuid.uuid4())
        timestamp = _utcnow().isoformat()

        payload = {
            "event_id": event_id,
            "event": event_type,
            "task_id": task_id,
            "timestamp": timestamp,
            "data": data,
        }

        for target in self._targets.values():
            if not target.enabled:
                continue
            if target.event_types is not None and event_type not in target.event_types:
                continue
            task = asyncio.create_task(
                self._deliver_with_retry(target, payload, event_id, event_type, task_id)
            )
            self._pending_tasks.add(task)
            task.add_done_callback(self._pending_tasks.discard)

    async def _deliver_with_retry(
        self,
        target: WebhookTarget,
        payload: dict[str, Any],
        event_id: str,
        event_type: str,
        task_id: str,
    ) -> bool:
        body = json.dumps(payload, default=str).encode()
        timestamp = payload["timestamp"]
        signature = compute_signature(body, target.secret, timestamp)

        headers = {
            "Content-Type": "application/json",
            "X-Elastic-Agent-Signature": signature,
            "X-Elastic-Agent-Event": event_type,
            "X-Elastic-Agent-Timestamp": timestamp,
            "X-Elastic-Agent-Event-Id": event_id,
        }

        last_error = ""
        for attempt, delay in enumerate([0] + self._retry_delays, start=1):
            if delay > 0:
                await asyncio.sleep(delay)

            try:
                async with httpx.AsyncClient(timeout=self._send_timeout) as client:
                    resp = await client.post(target.url, content=body, headers=headers)
                if resp.status_code < 300:
                    logger.debug(
                        "Webhook delivered to %s (event=%s, attempt=%d)",
                        target.url,
                        event_type,
                        attempt,
                    )
                    return True
                last_error = f"HTTP {resp.status_code}"
                logger.warning(
                    "Webhook to %s returned %d (attempt %d/%d)",
                    target.url,
                    resp.status_code,
                    attempt,
                    len(self._retry_delays) + 1,
                )
            except Exception as exc:
                last_error = str(exc)
                logger.warning(
                    "Webhook to %s failed: %s (attempt %d/%d)",
                    target.url,
                    exc,
                    attempt,
                    len(self._retry_delays) + 1,
                )

        dead = DeadLetter(
            event_id=event_id,
            target_id=target.target_id,
            url=target.url,
            event_type=event_type,
            task_id=task_id,
            payload=payload,
            error=last_error,
            attempts=len(self._retry_delays) + 1,
        )
        async with self._lock:
            self._dead_letters.append(dead)
            self._flush_dead_letters()

        logger.error(
            "Webhook to %s exhausted all retries for event %s (task=%s), written to dead letter queue",
            target.url,
            event_type,
            task_id,
        )
        return False

    async def replay_dead_letters(self) -> dict[str, bool]:
        """Attempt to re-deliver all dead letters. Returns {event_id: success}."""
        async with self._lock:
            to_replay = list(self._dead_letters)
            self._dead_letters.clear()
            self._flush_dead_letters()

        results: dict[str, bool] = {}
        for dl in to_replay:
            target = self._targets.get(dl.target_id)
            if target is None:
                async with self._lock:
                    self._dead_letters.append(dl)
                results[dl.event_id] = False
                continue

            success = await self._deliver_single(target, dl.payload)
            results[dl.event_id] = success
            if not success:
                dl.attempts += 1
                dl.error = "replay failed"
                async with self._lock:
                    self._dead_letters.append(dl)

        async with self._lock:
            self._flush_dead_letters()

        replayed = sum(1 for v in results.values() if v)
        logger.info("WebhookEmitter: replayed %d/%d dead letters", replayed, len(results))
        return results

    async def _deliver_single(self, target: WebhookTarget, payload: dict[str, Any]) -> bool:
        body = json.dumps(payload, default=str).encode()
        timestamp = payload.get("timestamp", _utcnow().isoformat())
        signature = compute_signature(body, target.secret, timestamp)

        headers = {
            "Content-Type": "application/json",
            "X-Elastic-Agent-Signature": signature,
            "X-Elastic-Agent-Event": payload.get("event", ""),
            "X-Elastic-Agent-Timestamp": timestamp,
            "X-Elastic-Agent-Event-Id": payload.get("event_id", ""),
        }

        try:
            async with httpx.AsyncClient(timeout=self._send_timeout) as client:
                resp = await client.post(target.url, content=body, headers=headers)
            return resp.status_code < 300
        except Exception as exc:
            logger.warning("Dead letter replay to %s failed: %s", target.url, exc)
            return False

    @property
    def dead_letter_count(self) -> int:
        return len(self._dead_letters)

    @property
    def dead_letters(self) -> list[DeadLetter]:
        return list(self._dead_letters)

    async def stop(self) -> None:
        for task in self._pending_tasks:
            task.cancel()
        self._pending_tasks.clear()
        logger.info("WebhookEmitter stopped")
