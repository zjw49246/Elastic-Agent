"""FILE_SYNCED webhook notification — push file sync events to external consumers.

T-014: When a Worker reports FILE_SYNCED, push a webhook to registered targets.

This module provides:
- WebhookTarget: a registered endpoint to receive notifications
- FileSyncedNotifier: subscribes to EventBus FILE_SYNCED events and pushes webhooks
- HMAC-SHA256 payload signing for verification by receivers
- Async HTTP POST with configurable retry

The full-featured WebhookEmitter (T-053) will extend this with dead-letter queue,
generic event support, and advanced retry policies.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import httpx

from elastic_agent.core.event_bus import EventBus

logger = logging.getLogger(__name__)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class WebhookTarget:
    url: str
    secret: str
    event_types: list[str] = field(default_factory=lambda: ["FILE_SYNCED"])
    enabled: bool = True


def compute_signature(payload: bytes, secret: str) -> str:
    """Compute HMAC-SHA256 signature for webhook payload verification."""
    return hmac.new(secret.encode(), payload, hashlib.sha256).hexdigest()


def verify_signature(payload: bytes, secret: str, signature: str) -> bool:
    """Verify an incoming webhook signature (constant-time comparison)."""
    expected = compute_signature(payload, secret)
    return hmac.compare_digest(expected, signature)


class FileSyncedNotifier:
    """Subscribes to EventBus FILE_SYNCED events and pushes webhooks to targets.

    Usage:
        bus = EventBus()
        notifier = FileSyncedNotifier(bus)
        notifier.add_target(WebhookTarget(url="https://example.com/webhook", secret="s3cret"))
        # FILE_SYNCED events on the bus will now trigger webhook POSTs
    """

    def __init__(
        self,
        event_bus: EventBus,
        retry_delays: list[float] | None = None,
        send_timeout: float = 10.0,
    ) -> None:
        self._event_bus = event_bus
        self._targets: list[WebhookTarget] = []
        self._retry_delays = retry_delays or [1, 5, 30]
        self._send_timeout = send_timeout
        self._subscription_id: str | None = None
        self._pending_tasks: set[asyncio.Task] = set()

    def add_target(self, target: WebhookTarget) -> None:
        self._targets.append(target)

    def remove_target(self, url: str) -> bool:
        before = len(self._targets)
        self._targets = [t for t in self._targets if t.url != url]
        return len(self._targets) < before

    def start(self) -> None:
        """Subscribe to FILE_SYNCED events on the EventBus."""
        if self._subscription_id is not None:
            return
        self._subscription_id = self._event_bus.subscribe("FILE_SYNCED", self._on_file_synced)
        logger.info("FileSyncedNotifier started, subscribed to FILE_SYNCED events")

    def stop(self) -> None:
        """Unsubscribe from EventBus and cancel pending deliveries."""
        if self._subscription_id:
            self._event_bus.unsubscribe(self._subscription_id)
            self._subscription_id = None
        for task in self._pending_tasks:
            task.cancel()
        self._pending_tasks.clear()
        logger.info("FileSyncedNotifier stopped")

    async def _on_file_synced(
        self,
        event_type: str,
        worker_id: str,
        data: dict[str, Any],
    ) -> None:
        """EventBus callback: fan-out to all matching webhook targets."""
        payload = {
            "event": "task.file.synced",
            "timestamp": _utcnow().isoformat(),
            "worker_id": worker_id,
            "data": data,
        }
        for target in self._targets:
            if not target.enabled:
                continue
            if "FILE_SYNCED" not in target.event_types:
                continue
            task = asyncio.create_task(self._deliver(target, payload))
            self._pending_tasks.add(task)
            task.add_done_callback(self._pending_tasks.discard)

    async def _deliver(
        self,
        target: WebhookTarget,
        payload: dict[str, Any],
    ) -> bool:
        """Deliver a webhook with retries."""
        body = json.dumps(payload, default=str).encode()
        signature = compute_signature(body, target.secret)
        headers = {
            "Content-Type": "application/json",
            "X-Webhook-Signature": signature,
            "X-Webhook-Event": "task.file.synced",
        }

        for attempt, delay in enumerate(
            [0] + self._retry_delays, start=1
        ):
            if delay > 0:
                await asyncio.sleep(delay)

            try:
                async with httpx.AsyncClient(timeout=self._send_timeout) as client:
                    resp = await client.post(target.url, content=body, headers=headers)
                if resp.status_code < 300:
                    logger.debug(
                        "Webhook delivered to %s (attempt %d, status=%d)",
                        target.url,
                        attempt,
                        resp.status_code,
                    )
                    return True
                logger.warning(
                    "Webhook to %s returned %d (attempt %d/%d)",
                    target.url,
                    resp.status_code,
                    attempt,
                    len(self._retry_delays) + 1,
                )
            except Exception as exc:
                logger.warning(
                    "Webhook to %s failed: %s (attempt %d/%d)",
                    target.url,
                    exc,
                    attempt,
                    len(self._retry_delays) + 1,
                )

        logger.error(
            "Webhook delivery to %s exhausted all retries for event %s",
            target.url,
            payload.get("event"),
        )
        return False
