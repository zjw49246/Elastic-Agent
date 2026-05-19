"""Internal EventBus — LOG event dispatch for Harness callbacks and phase detection.

T-013: Internal event distribution system.

The EventBus is the central nervous system of the Manager process.
All Worker events (LOG, FILE_SYNCED, HEARTBEAT, etc.) flow through it,
and internal consumers (Harness callbacks, phase detection, trace buffer)
subscribe to relevant event types.

This is NOT an external API — external consumers read from OSS or receive webhooks.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Awaitable

logger = logging.getLogger(__name__)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


EventCallback = Callable[[str, str, dict[str, Any]], Awaitable[None]]
"""Signature: async def handler(event_type: str, worker_id: str, data: dict) -> None"""


@dataclass
class Subscription:
    id: str
    event_type: str
    callback: EventCallback
    created_at: datetime = field(default_factory=_utcnow)


class EventBus:
    """Async fan-out event distribution with type-based filtering.

    Usage:
        bus = EventBus()

        # Subscribe to specific event type
        sub_id = bus.subscribe("LOG", my_handler)

        # Subscribe to all events
        sub_id = bus.subscribe("*", catch_all_handler)

        # Emit an event
        await bus.emit("LOG", "worker-1", {"task_id": "t1", "data": "..."})

        # Unsubscribe
        bus.unsubscribe(sub_id)
    """

    def __init__(self) -> None:
        self._subscriptions: dict[str, Subscription] = {}
        self._by_type: dict[str, set[str]] = defaultdict(set)
        self._lock = asyncio.Lock()

    def subscribe(
        self,
        event_type: str,
        callback: EventCallback,
    ) -> str:
        """Register a callback for an event type. Use "*" for all events.

        Returns a subscription ID for later unsubscription.
        """
        sub_id = str(uuid.uuid4())
        sub = Subscription(id=sub_id, event_type=event_type, callback=callback)
        self._subscriptions[sub_id] = sub
        self._by_type[event_type].add(sub_id)
        logger.debug("EventBus: subscribed %s to event_type=%s", sub_id[:8], event_type)
        return sub_id

    def unsubscribe(self, subscription_id: str) -> bool:
        """Remove a subscription. Returns True if found and removed."""
        sub = self._subscriptions.pop(subscription_id, None)
        if sub is None:
            return False
        self._by_type[sub.event_type].discard(subscription_id)
        if not self._by_type[sub.event_type]:
            del self._by_type[sub.event_type]
        logger.debug("EventBus: unsubscribed %s from event_type=%s", subscription_id[:8], sub.event_type)
        return True

    async def emit(
        self,
        event_type: str,
        worker_id: str,
        data: dict[str, Any],
    ) -> int:
        """Dispatch an event to all matching subscribers.

        Subscribers for the exact event_type AND wildcard "*" are notified.
        Errors in individual callbacks are logged but don't affect others.

        Returns the number of callbacks invoked.
        """
        target_ids: set[str] = set()
        target_ids.update(self._by_type.get(event_type, set()))
        target_ids.update(self._by_type.get("*", set()))

        if not target_ids:
            return 0

        count = 0
        for sub_id in target_ids:
            sub = self._subscriptions.get(sub_id)
            if sub is None:
                continue
            try:
                await sub.callback(event_type, worker_id, data)
                count += 1
            except Exception:
                logger.exception(
                    "EventBus: callback %s failed for event_type=%s worker=%s",
                    sub_id[:8],
                    event_type,
                    worker_id,
                )
        return count

    def subscriber_count(self, event_type: str | None = None) -> int:
        """Return total subscriber count, or count for a specific event type."""
        if event_type is None:
            return len(self._subscriptions)
        return len(self._by_type.get(event_type, set()))

    def clear(self) -> None:
        """Remove all subscriptions."""
        self._subscriptions.clear()
        self._by_type.clear()
