"""External API authentication — API Key Bearer Token.

T-015: REST requests must carry an API Key via:
  - Header: Authorization: Bearer <key>
  - Query parameter: ?api_key=<key>

API keys are defined via the ELASTIC_AGENT_EXTERNAL_API_KEYS environment variable
(comma-separated) and validated using constant-time comparison.
"""

from __future__ import annotations

import os
from typing import Annotated

from fastapi import Depends, HTTPException, Query, Request, status

from elastic_agent.core.auth import verify_token_constant_time


def _load_api_keys() -> list[str]:
    raw = os.environ.get("ELASTIC_AGENT_EXTERNAL_API_KEYS", "")
    return [k.strip() for k in raw.split(",") if k.strip()]


_api_keys: list[str] | None = None


def get_api_keys() -> list[str]:
    global _api_keys
    if _api_keys is None:
        _api_keys = _load_api_keys()
    return _api_keys


def reset_api_keys() -> None:
    """Force reload on next call — used by tests."""
    global _api_keys
    _api_keys = None


def _extract_bearer(request: Request) -> str | None:
    auth = request.headers.get("authorization")
    if auth and auth.lower().startswith("bearer "):
        return auth[7:].strip()
    return None


async def require_api_key(
    request: Request,
    api_key: str | None = Query(None, alias="api_key"),
) -> str:
    """FastAPI dependency that enforces API Key auth.

    Returns the matched key on success; raises 401 on failure.
    """
    token = _extract_bearer(request) or api_key

    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing API key — provide Authorization: Bearer <key> or ?api_key=<key>",
        )

    keys = get_api_keys()
    if not keys:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="No API keys configured on the server",
        )

    for k in keys:
        if verify_token_constant_time(token, k):
            return token

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid API key",
    )


APIKeyDep = Annotated[str, Depends(require_api_key)]
