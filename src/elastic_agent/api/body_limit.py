"""Bound HTTP request bodies before FastAPI parses JSON or form payloads."""

from __future__ import annotations

import asyncio
import json
import math
import os
import threading
from collections.abc import Awaitable, Callable
from typing import Any

from elastic_agent.core.job_batch import JOB_BATCH_MAX_BODY_BYTES

DEFAULT_MAX_REQUEST_BODY_BYTES = 16 * 1024 * 1024
MIN_MAX_REQUEST_BODY_BYTES = 64 * 1024
MAX_MAX_REQUEST_BODY_BYTES = 64 * 1024 * 1024
JOB_SUBMIT_MAX_BODY_BYTES = 16 * 1024 * 1024
REQUEST_BODY_LIMIT_STATE_KEY = "elastic_agent_max_request_body_bytes"

DEFAULT_REQUEST_BODY_READ_TIMEOUT_SECONDS = 30.0
MIN_REQUEST_BODY_READ_TIMEOUT_SECONDS = 1.0
MAX_REQUEST_BODY_READ_TIMEOUT_SECONDS = 300.0

DEFAULT_MAX_CONCURRENT_REQUEST_BODIES = 16
MIN_MAX_CONCURRENT_REQUEST_BODIES = 1
MAX_MAX_CONCURRENT_REQUEST_BODIES = 256

# One raw body can coexist with its frozen replay bytes and JSON/Pydantic's
# parsed representation. Reserve three logical copies for the complete
# downstream application lifetime, not merely while reading from the socket.
REQUEST_BODY_MEMORY_MULTIPLIER = 3
DEFAULT_MAX_AGGREGATE_REQUEST_BODY_BYTES = 256 * 1024 * 1024
MIN_MAX_AGGREGATE_REQUEST_BODY_BYTES = 1024 * 1024
MAX_MAX_AGGREGATE_REQUEST_BODY_BYTES = 4 * 1024 * 1024 * 1024

_CONVENTIONALLY_BODYLESS_METHODS = frozenset(
    {
        "GET",
        "HEAD",
        "OPTIONS",
        "TRACE",
    }
)

AsgiMessage = dict[str, Any]
Receive = Callable[[], Awaitable[AsgiMessage]]
Send = Callable[[AsgiMessage], Awaitable[None]]


def _configured_int(
    name: str,
    *,
    default: int,
    minimum: int,
    maximum: int,
) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if not minimum <= value <= maximum:
        raise ValueError(f"{name} must be between {minimum} and {maximum}")
    return value


def configured_max_request_body_bytes() -> int:
    """Return the deployment-wide request body limit with fail-fast bounds."""

    return _configured_int(
        "ELASTIC_AGENT_MAX_REQUEST_BODY_BYTES",
        default=DEFAULT_MAX_REQUEST_BODY_BYTES,
        minimum=MIN_MAX_REQUEST_BODY_BYTES,
        maximum=MAX_MAX_REQUEST_BODY_BYTES,
    )


def configured_request_body_read_timeout_seconds() -> float:
    """Return the one-deadline budget for reading a complete request body."""

    name = "ELASTIC_AGENT_REQUEST_BODY_READ_TIMEOUT_SECONDS"
    raw = os.environ.get(name, "").strip()
    if not raw:
        return DEFAULT_REQUEST_BODY_READ_TIMEOUT_SECONDS
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be a number") from exc
    if (
        not math.isfinite(value)
        or not MIN_REQUEST_BODY_READ_TIMEOUT_SECONDS <= value <= MAX_REQUEST_BODY_READ_TIMEOUT_SECONDS
    ):
        raise ValueError(
            f"{name} must be between "
            f"{MIN_REQUEST_BODY_READ_TIMEOUT_SECONDS:g} and "
            f"{MAX_REQUEST_BODY_READ_TIMEOUT_SECONDS:g}"
        )
    return value


def configured_max_concurrent_request_bodies() -> int:
    """Return the maximum number of admitted body-bearing requests."""

    return _configured_int(
        "ELASTIC_AGENT_MAX_CONCURRENT_REQUEST_BODIES",
        default=DEFAULT_MAX_CONCURRENT_REQUEST_BODIES,
        minimum=MIN_MAX_CONCURRENT_REQUEST_BODIES,
        maximum=MAX_MAX_CONCURRENT_REQUEST_BODIES,
    )


def configured_max_aggregate_request_body_bytes(
    *,
    max_request_body_bytes: int,
) -> int:
    """Return the process-local conservative body-memory budget."""

    value = _configured_int(
        "ELASTIC_AGENT_MAX_AGGREGATE_REQUEST_BODY_BYTES",
        default=DEFAULT_MAX_AGGREGATE_REQUEST_BODY_BYTES,
        minimum=MIN_MAX_AGGREGATE_REQUEST_BODY_BYTES,
        maximum=MAX_MAX_AGGREGATE_REQUEST_BODY_BYTES,
    )
    required = REQUEST_BODY_MEMORY_MULTIPLIER * max_request_body_bytes
    if value < required:
        raise ValueError(
            "ELASTIC_AGENT_MAX_AGGREGATE_REQUEST_BODY_BYTES must be at least "
            f"{REQUEST_BODY_MEMORY_MULTIPLIER} times "
            "ELASTIC_AGENT_MAX_REQUEST_BODY_BYTES"
        )
    return value


class _RequestBodyBudget:
    """Small fail-fast count/byte admission controller.

    ASGI normally runs one event loop per process, but a regular threading lock
    also keeps tests and alternative servers safe without ever awaiting for a
    permit. The critical sections contain only integer checks and mutations.
    """

    def __init__(self, *, max_requests: int, max_bytes: int) -> None:
        self.max_requests = max_requests
        self.max_bytes = max_bytes
        self._active_requests = 0
        self._reserved_bytes = 0
        self._lock = threading.Lock()

    def try_admit(self) -> bool:
        with self._lock:
            if self._active_requests >= self.max_requests:
                return False
            self._active_requests += 1
            return True

    def release_admission(self) -> None:
        with self._lock:
            if self._active_requests:
                self._active_requests -= 1

    def try_reserve(self, amount: int) -> bool:
        if amount <= 0:
            return True
        with self._lock:
            if self._reserved_bytes + amount > self.max_bytes:
                return False
            self._reserved_bytes += amount
            return True

    def release_bytes(self, amount: int) -> None:
        if amount <= 0:
            return
        with self._lock:
            self._reserved_bytes = max(0, self._reserved_bytes - amount)

    @property
    def active_requests(self) -> int:
        with self._lock:
            return self._active_requests

    @property
    def reserved_bytes(self) -> int:
        with self._lock:
            return self._reserved_bytes


class RequestBodyLimitMiddleware:
    """Read body-bearing HTTP requests under strict time and memory budgets.

    A permit and a conservative three-copy byte reservation remain held through
    the entire downstream FastAPI/JSON/Pydantic lifecycle. Admission never
    waits. Conventionally bodyless requests bypass both pre-reading and
    admission so health/read APIs remain responsive under a body flood.
    """

    def __init__(
        self,
        app,
        *,
        max_bytes: int,
        read_timeout_seconds: float = DEFAULT_REQUEST_BODY_READ_TIMEOUT_SECONDS,
        max_concurrent_bodies: int = DEFAULT_MAX_CONCURRENT_REQUEST_BODIES,
        max_aggregate_body_bytes: int | None = None,
    ) -> None:
        if max_bytes <= 0:
            raise ValueError("max_bytes must be positive")
        if read_timeout_seconds <= 0 or not math.isfinite(read_timeout_seconds):
            raise ValueError("read_timeout_seconds must be positive and finite")
        if max_concurrent_bodies <= 0:
            raise ValueError("max_concurrent_bodies must be positive")
        if max_aggregate_body_bytes is None:
            max_aggregate_body_bytes = max(
                DEFAULT_MAX_AGGREGATE_REQUEST_BODY_BYTES,
                REQUEST_BODY_MEMORY_MULTIPLIER * max_bytes,
            )
        minimum_aggregate = REQUEST_BODY_MEMORY_MULTIPLIER * max_bytes
        if max_aggregate_body_bytes < minimum_aggregate:
            raise ValueError(
                f"max_aggregate_body_bytes must reserve at least {REQUEST_BODY_MEMORY_MULTIPLIER} copies of max_bytes"
            )

        self.app = app
        self.max_bytes = max_bytes
        self.read_timeout_seconds = read_timeout_seconds
        self._budget = _RequestBodyBudget(
            max_requests=max_concurrent_bodies,
            max_bytes=max_aggregate_body_bytes,
        )

    async def __call__(self, scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        max_bytes = self._effective_limit(scope)
        content_length = self._content_length(scope)
        if content_length is not None and content_length > max_bytes:
            await self._respond(
                send,
                status=413,
                detail=(
                    f"request body exceeds the configured {max_bytes}-byte limit"
                ),
            )
            return

        method = str(scope.get("method") or "POST").upper()
        if method in _CONVENTIONALLY_BODYLESS_METHODS and not self._declares_body(scope):
            await self.app(scope, receive, send)
            return

        if not self._budget.try_admit():
            await self._overloaded(send)
            return

        admitted = True
        reserved_bytes = 0
        buffered: bytearray | None = bytearray()
        frozen_body: bytes | None = None

        def release_resources() -> None:
            nonlocal admitted, reserved_bytes, buffered, frozen_body
            # Drop body references before returning budget to another request.
            buffered = None
            frozen_body = None
            if reserved_bytes:
                self._budget.release_bytes(reserved_bytes)
                reserved_bytes = 0
            if admitted:
                self._budget.release_admission()
                admitted = False

        try:
            if content_length:
                initial_reservation = REQUEST_BODY_MEMORY_MULTIPLIER * content_length
                if not self._budget.try_reserve(initial_reservation):
                    release_resources()
                    await self._overloaded(send)
                    return
                reserved_bytes = initial_reservation

            total = 0
            try:
                async with asyncio.timeout(self.read_timeout_seconds):
                    while True:
                        message = await receive()
                        if message["type"] == "http.disconnect":
                            return
                        if message["type"] != "http.request":
                            continue
                        chunk = message.get("body", b"")
                        total += len(chunk)
                        if total > max_bytes:
                            release_resources()
                            await self._respond(
                                send,
                                status=413,
                                detail=(
                                    "request body exceeds the configured "
                                    f"{max_bytes}-byte limit"
                                ),
                            )
                            return

                        desired_reservation = REQUEST_BODY_MEMORY_MULTIPLIER * total
                        additional = desired_reservation - reserved_bytes
                        if additional > 0:
                            if not self._budget.try_reserve(additional):
                                release_resources()
                                await self._overloaded(send)
                                return
                            reserved_bytes += additional
                        assert buffered is not None
                        buffered.extend(chunk)
                        if not message.get("more_body", False):
                            break
            except TimeoutError:
                release_resources()
                await self._respond(
                    send,
                    status=408,
                    detail="request body was not received before the deadline",
                )
                return

            # A dishonest Content-Length may have over-reserved. Retain exactly
            # the conservative three-copy budget for the actual body.
            desired_reservation = REQUEST_BODY_MEMORY_MULTIPLIER * total
            excess = reserved_bytes - desired_reservation
            if excess > 0:
                self._budget.release_bytes(excess)
                reserved_bytes -= excess

            # Free the mutable accumulator immediately after freezing. The
            # three-copy reservation covers this unavoidable conversion peak
            # and remains held while the route parses/validates the replay.
            assert buffered is not None
            frozen_body = bytes(buffered)
            buffered = None
            replayed = False

            async def replay_receive() -> AsgiMessage:
                nonlocal replayed, frozen_body
                if not replayed:
                    replayed = True
                    payload = frozen_body or b""
                    frozen_body = None
                    return {
                        "type": "http.request",
                        "body": payload,
                        "more_body": False,
                    }
                return await receive()

            state = scope.setdefault("state", {})
            if isinstance(state, dict):
                # Downstream routes with a stricter endpoint-specific limit can
                # safely use Request.body() without duplicating this already
                # bounded payload into another incremental bytearray.
                state[REQUEST_BODY_LIMIT_STATE_KEY] = max_bytes
            await self.app(scope, replay_receive, send)
        finally:
            # Covers disconnect, receive/send errors, downstream exceptions,
            # external cancellation, and every explicit rejection path.
            release_resources()

    @staticmethod
    def _content_length(scope) -> int | None:
        for raw_name, raw_value in scope.get("headers", ()):
            if raw_name.lower() != b"content-length":
                continue
            try:
                value = int(raw_value)
            except (TypeError, ValueError):
                return None
            return max(0, value)
        return None

    def _effective_limit(self, scope) -> int:
        # Job submission fingerprints raw JSON before current-schema
        # validation. Keep its endpoint ceiling in front of buffering even
        # when the deployment-wide limit is configured as high as 64 MiB.
        if scope.get("method") == "POST" and scope.get("path") == "/api/jobs":
            return min(self.max_bytes, JOB_SUBMIT_MAX_BODY_BYTES)
        if (
            scope.get("method") == "POST"
            and scope.get("path") in {
                "/api/job-batches",
                "/api/job-batches/plan",
            }
        ):
            return min(self.max_bytes, JOB_BATCH_MAX_BODY_BYTES)
        return self.max_bytes

    @staticmethod
    def _declares_body(scope) -> bool:
        for raw_name, raw_value in scope.get("headers", ()):
            name = raw_name.lower()
            if name == b"content-length":
                try:
                    if int(raw_value) > 0:
                        return True
                except (TypeError, ValueError):
                    return True
            elif name == b"transfer-encoding" and raw_value.strip().lower() not in {b"", b"identity"}:
                return True
        return False

    async def _overloaded(self, send: Send) -> None:
        await self._respond(
            send,
            status=503,
            detail="request body capacity is temporarily unavailable",
            extra_headers=[(b"retry-after", b"1")],
        )

    @staticmethod
    async def _respond(
        send: Send,
        *,
        status: int,
        detail: str,
        extra_headers: list[tuple[bytes, bytes]] | None = None,
    ) -> None:
        body = json.dumps(
            {"detail": detail},
            separators=(",", ":"),
        ).encode()
        headers = [
            (b"content-type", b"application/json"),
            (b"content-length", str(len(body)).encode()),
            (b"cache-control", b"no-store"),
        ]
        if extra_headers:
            headers.extend(extra_headers)
        await send(
            {
                "type": "http.response.start",
                "status": status,
                "headers": headers,
            }
        )
        await send({"type": "http.response.body", "body": body})
