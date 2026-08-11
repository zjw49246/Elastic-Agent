"""Process-local, one-shot stdin leases for trusted secret-bearing Jobs.

The lease store deliberately has no persistence adapter.  A Manager restart
therefore loses every unconsumed payload and a durable Job must start a new
attempt with a freshly delegated credential.  Buffers are mutable and are
overwritten on consume/discard/expiry/close.
"""

from __future__ import annotations

import hashlib
import hmac
import threading
import time
from dataclasses import dataclass
from typing import Callable

MAX_EPHEMERAL_STDIN_BYTES = 256 * 1024
MAX_EPHEMERAL_STDIN_LEASES = 512
MAX_EPHEMERAL_STDIN_TTL_SECONDS = 6 * 60 * 60


class EphemeralStdinLeaseError(RuntimeError):
    """A one-shot stdin lease cannot be installed or consumed safely."""


@dataclass(slots=True)
class _Lease:
    payload: bytearray
    digest: bytes
    expires_at: float


def _wipe(value: bytearray) -> None:
    for index in range(len(value)):
        value[index] = 0


class EphemeralStdinLeaseStore:
    """Bounded process-local ownership of secret stdin payloads."""

    def __init__(self, *, clock: Callable[[], float] = time.monotonic) -> None:
        self._clock = clock
        self._leases: dict[str, _Lease] = {}
        self._lock = threading.Lock()
        self._closed = False

    def put(self, job_id: str, payload: bytearray, *, ttl_seconds: float) -> None:
        """Adopt ``payload`` for one Job without copying it.

        The caller must relinquish the bytearray after this method succeeds.
        An exact concurrent replay is idempotent but the replay buffer is wiped;
        a different payload for the same Job fails closed.
        """

        if not isinstance(job_id, str) or not job_id:
            raise EphemeralStdinLeaseError("stdin lease Job id is invalid")
        if not isinstance(payload, bytearray):
            raise EphemeralStdinLeaseError("stdin lease payload must be mutable")
        if not 1 <= len(payload) <= MAX_EPHEMERAL_STDIN_BYTES:
            raise EphemeralStdinLeaseError("stdin lease payload size is invalid")
        if (
            isinstance(ttl_seconds, bool)
            or not isinstance(ttl_seconds, (int, float))
            or not 1 <= float(ttl_seconds) <= MAX_EPHEMERAL_STDIN_TTL_SECONDS
        ):
            raise EphemeralStdinLeaseError("stdin lease TTL is invalid")
        digest = hashlib.sha256(payload).digest()
        now = float(self._clock())
        with self._lock:
            self._expire_locked(now)
            if self._closed:
                raise EphemeralStdinLeaseError("stdin lease store is closed")
            existing = self._leases.get(job_id)
            if existing is not None:
                if not hmac.compare_digest(existing.digest, digest):
                    raise EphemeralStdinLeaseError(
                        "stdin lease Job id is already bound to another payload"
                    )
                _wipe(payload)
                return
            if len(self._leases) >= MAX_EPHEMERAL_STDIN_LEASES:
                raise EphemeralStdinLeaseError("stdin lease capacity is exhausted")
            self._leases[job_id] = _Lease(
                payload=payload,
                digest=digest,
                expires_at=now + float(ttl_seconds),
            )

    def consume(self, job_id: str) -> bytearray:
        """Remove and return one live payload; it can never be consumed twice."""

        now = float(self._clock())
        with self._lock:
            self._expire_locked(now)
            if self._closed:
                raise EphemeralStdinLeaseError("stdin lease store is closed")
            lease = self._leases.pop(job_id, None)
        if lease is None:
            raise EphemeralStdinLeaseError(
                "stdin credential lease is missing or expired; create a new Job attempt"
            )
        return lease.payload

    def discard(self, job_id: str) -> bool:
        """Remove and overwrite an unconsumed payload."""

        with self._lock:
            lease = self._leases.pop(job_id, None)
        if lease is None:
            return False
        _wipe(lease.payload)
        return True

    def close(self) -> None:
        """Overwrite all remaining leases and permanently close this store."""

        with self._lock:
            self._closed = True
            leases = list(self._leases.values())
            self._leases.clear()
        for lease in leases:
            _wipe(lease.payload)

    def _expire_locked(self, now: float) -> None:
        expired = [
            job_id
            for job_id, lease in self._leases.items()
            if lease.expires_at <= now
        ]
        for job_id in expired:
            lease = self._leases.pop(job_id)
            _wipe(lease.payload)

    def contains(self, job_id: str) -> bool:
        """Return process-local presence for tests/health, never payload data."""

        now = float(self._clock())
        with self._lock:
            self._expire_locked(now)
            return not self._closed and job_id in self._leases


__all__ = [
    "EphemeralStdinLeaseError",
    "EphemeralStdinLeaseStore",
    "MAX_EPHEMERAL_STDIN_BYTES",
    "MAX_EPHEMERAL_STDIN_LEASES",
    "MAX_EPHEMERAL_STDIN_TTL_SECONDS",
]
