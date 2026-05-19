"""Per-Worker Bearer Token authentication utilities.

T-010: Manager <-> Worker authentication using per-Worker tokens.

Token lifecycle:
- Generated during node creation (scale_out)
- Stored in NodeRegistry.auth_token
- Injected into Worker during Bootstrap (runtime.yaml)
- Worker sends token in AUTH message on WS connect
- Manager validates via WorkerConnectionManager._verify_token
"""

from __future__ import annotations

import hashlib
import hmac
import secrets


def generate_worker_token(length: int = 32) -> str:
    """Generate a cryptographically secure per-Worker Bearer token.

    Returns a URL-safe base64-encoded token string.
    """
    return secrets.token_urlsafe(length)


def verify_token_constant_time(provided: str, expected: str) -> bool:
    """Constant-time token comparison to prevent timing attacks."""
    return hmac.compare_digest(provided.encode(), expected.encode())


def hash_token(token: str) -> str:
    """Hash a token for safe logging/storage where the raw value isn't needed."""
    return hashlib.sha256(token.encode()).hexdigest()[:16]
