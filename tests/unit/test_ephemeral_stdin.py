from __future__ import annotations

import pytest

from elastic_agent.core.ephemeral_stdin import (
    EphemeralStdinLeaseError,
    EphemeralStdinLeaseStore,
)


def test_lease_is_one_shot_and_duplicate_buffer_is_wiped() -> None:
    store = EphemeralStdinLeaseStore()
    original = bytearray(b"secret-frame")
    duplicate = bytearray(b"secret-frame")
    store.put("job-1", original, ttl_seconds=60)
    store.put("job-1", duplicate, ttl_seconds=60)

    assert duplicate == bytearray(len(duplicate))
    assert store.consume("job-1") is original
    with pytest.raises(EphemeralStdinLeaseError, match="missing or expired"):
        store.consume("job-1")


def test_expiry_and_close_overwrite_owned_buffers() -> None:
    now = [10.0]
    store = EphemeralStdinLeaseStore(clock=lambda: now[0])
    expired = bytearray(b"expired")
    retained = bytearray(b"retained")
    store.put("job-expired", expired, ttl_seconds=1)
    now[0] = 12.0
    assert not store.contains("job-expired")
    assert expired == bytearray(len(expired))

    store.put("job-retained", retained, ttl_seconds=60)
    store.close()
    assert retained == bytearray(len(retained))
    with pytest.raises(EphemeralStdinLeaseError, match="closed"):
        store.put("job-new", bytearray(b"new"), ttl_seconds=60)
