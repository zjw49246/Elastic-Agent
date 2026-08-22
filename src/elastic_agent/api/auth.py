"""Management-plane authentication for service clients and browser users.

Automation may continue to use a configured Bearer service token. Browser
users authenticate with a separate administrator account and receive an
opaque, HttpOnly session cookie. Worker ``/ws/runtime`` credentials are a
different trust boundary and never pass through this module.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import os
import secrets
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Annotated, Literal
from urllib.parse import urlsplit, urlunsplit

from fastapi import Depends, HTTPException, Request, status

from elastic_agent.core.auth import verify_token_constant_time
from elastic_agent.core.management_auth import ManagementUserStore

SESSION_COOKIE_NAME = "__Host-ea_session"
CSRF_HEADER_NAME = "X-CSRF-Token"
DEFAULT_SESSION_TTL_SECONDS = 12 * 60 * 60
DEFAULT_SESSION_IDLE_SECONDS = 30 * 60
MAX_SESSIONS_PER_USER = 5
MAX_SESSIONS_TOTAL = 1_000
_SAFE_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})


@dataclass(frozen=True)
class AuthPrincipal:
    """Non-secret identity attached to an authenticated management request."""

    kind: Literal["service", "user"]
    subject: str
    role: str = "admin"
    must_change_password: bool = False
    password_version: int = 0
    csrf_token: str = field(default="", repr=False)


@dataclass
class _BrowserSession:
    email: str
    password_version: int
    csrf_token: str = field(repr=False)
    created_at: float
    last_seen_at: float
    expires_at: float


class ManagementSessionManager:
    """Bounded in-memory browser sessions; restart intentionally logs users out."""

    def __init__(
        self,
        *,
        ttl_seconds: int = DEFAULT_SESSION_TTL_SECONDS,
        idle_seconds: int = DEFAULT_SESSION_IDLE_SECONDS,
        clock=time.time,
    ) -> None:
        if ttl_seconds < 60 or idle_seconds < 60 or idle_seconds > ttl_seconds:
            raise ValueError("invalid management session lifetime")
        self._ttl_seconds = ttl_seconds
        self._idle_seconds = idle_seconds
        self._clock = clock
        self._lock = threading.RLock()
        self._sessions: dict[str, _BrowserSession] = {}

    @staticmethod
    def _digest(token: str) -> str:
        return hashlib.sha256(token.encode("ascii")).hexdigest()

    @staticmethod
    def _valid_token_shape(token: object) -> bool:
        return (
            isinstance(token, str)
            and 32 <= len(token) <= 256
            and token.isascii()
            and all(char.isalnum() or char in "-_" for char in token)
        )

    def _prune_locked(self, now: float) -> None:
        stale = [
            digest
            for digest, session in self._sessions.items()
            if session.expires_at <= now
            or session.last_seen_at + self._idle_seconds <= now
        ]
        for digest in stale:
            self._sessions.pop(digest, None)

    def create(self, user) -> tuple[str, AuthPrincipal]:
        now = self._clock()
        token = secrets.token_urlsafe(32)
        csrf_token = secrets.token_urlsafe(32)
        record = _BrowserSession(
            email=user.email,
            password_version=user.password_version,
            csrf_token=csrf_token,
            created_at=now,
            last_seen_at=now,
            expires_at=now + self._ttl_seconds,
        )
        with self._lock:
            self._prune_locked(now)
            same_user = sorted(
                (
                    (session.created_at, digest)
                    for digest, session in self._sessions.items()
                    if session.email == user.email
                ),
            )
            while len(same_user) >= MAX_SESSIONS_PER_USER:
                _created, digest = same_user.pop(0)
                self._sessions.pop(digest, None)
            if len(self._sessions) >= MAX_SESSIONS_TOTAL:
                oldest = min(
                    self._sessions,
                    key=lambda digest: self._sessions[digest].created_at,
                )
                self._sessions.pop(oldest, None)
            self._sessions[self._digest(token)] = record
        return token, AuthPrincipal(
            kind="user",
            subject=user.email,
            role=user.role,
            must_change_password=user.must_change_password,
            password_version=user.password_version,
            csrf_token=csrf_token,
        )

    def authenticate(
        self,
        token: object,
        store: ManagementUserStore,
    ) -> AuthPrincipal | None:
        if not self._valid_token_shape(token):
            return None
        now = self._clock()
        digest = self._digest(token)
        with self._lock:
            self._prune_locked(now)
            session = self._sessions.get(digest)
            if session is None:
                return None
            user = store.get(session.email)
            if (
                user is None
                or not user.enabled
                or user.role != "admin"
                or user.password_version != session.password_version
            ):
                self._sessions.pop(digest, None)
                return None
            session.last_seen_at = now
            return AuthPrincipal(
                kind="user",
                subject=user.email,
                role=user.role,
                must_change_password=user.must_change_password,
                password_version=user.password_version,
                csrf_token=session.csrf_token,
            )

    def revoke(self, token: object) -> None:
        if not self._valid_token_shape(token):
            return
        with self._lock:
            self._sessions.pop(self._digest(token), None)

    def revoke_user(self, email: str) -> None:
        with self._lock:
            stale = [
                digest
                for digest, session in self._sessions.items()
                if hmac.compare_digest(session.email, email)
            ]
            for digest in stale:
                self._sessions.pop(digest, None)

    @property
    def ttl_seconds(self) -> int:
        return self._ttl_seconds


def _load_api_keys() -> list[str]:
    raw = os.environ.get("ELASTIC_AGENT_EXTERNAL_API_KEYS", "")
    return [key.strip() for key in raw.split(",") if key.strip()]


_api_keys: list[str] | None = None
_management_user_store: ManagementUserStore | None = None
_management_sessions: ManagementSessionManager | None = None
_management_public_origin: str | None = None


def get_api_keys() -> list[str]:
    global _api_keys
    if _api_keys is None:
        _api_keys = _load_api_keys()
    return _api_keys


def management_users_path() -> Path:
    explicit = os.environ.get("ELASTIC_AGENT_MANAGEMENT_USERS_FILE", "").strip()
    if explicit:
        path = Path(explicit)
    else:
        state_dir = os.environ.get("ELASTIC_AGENT_STATE_DIR", "").strip()
        path = Path(state_dir) / "management-users.json" if state_dir else (
            Path.cwd() / ".elastic-agent-state" / "management-users.json"
        )
    if not path.is_absolute():
        raise RuntimeError("management users file must be an absolute path")
    return path


def get_management_user_store() -> ManagementUserStore:
    global _management_user_store
    if _management_user_store is None:
        _management_user_store = ManagementUserStore(management_users_path())
    return _management_user_store


def configure_management_user_store(path: str | Path) -> ManagementUserStore:
    """Install the exact production/test user store before app construction."""

    global _management_user_store, _management_sessions
    configured = Path(path)
    if not configured.is_absolute():
        raise RuntimeError("management users file must be an absolute path")
    _management_user_store = ManagementUserStore(configured)
    _management_sessions = None
    return _management_user_store


def configure_public_origin(origin: str) -> str:
    """Pin the browser origin supplied by the trusted deployment launcher."""

    global _management_public_origin
    _management_public_origin = _normalize_public_origin(origin)
    return _management_public_origin


def get_management_sessions() -> ManagementSessionManager:
    global _management_sessions
    if _management_sessions is None:
        ttl = int(os.environ.get(
            "ELASTIC_AGENT_MANAGEMENT_SESSION_TTL_SECONDS",
            str(DEFAULT_SESSION_TTL_SECONDS),
        ))
        idle = int(os.environ.get(
            "ELASTIC_AGENT_MANAGEMENT_SESSION_IDLE_SECONDS",
            str(DEFAULT_SESSION_IDLE_SECONDS),
        ))
        _management_sessions = ManagementSessionManager(
            ttl_seconds=ttl,
            idle_seconds=idle,
        )
    return _management_sessions


def reset_api_keys() -> None:
    """Force service tokens to reload on the next call (used by tests/startup)."""

    global _api_keys
    _api_keys = None


def reset_management_auth() -> None:
    """Discard cached users and all in-memory browser sessions."""

    global _management_user_store, _management_sessions, _management_public_origin
    _management_user_store = None
    _management_sessions = None
    _management_public_origin = None


def _extract_bearer(request: Request) -> str | None:
    authorization = request.headers.get("authorization")
    if authorization and authorization.lower().startswith("bearer "):
        return authorization[7:].strip()
    return None


def _session_cookie(request: Request) -> str | None:
    return request.cookies.get(SESSION_COOKIE_NAME)


def _normalize_public_origin(value: str) -> str:
    parsed = urlsplit(value)
    try:
        parsed.port
    except ValueError as exc:
        raise RuntimeError("ELASTIC_AGENT_PUBLIC_ORIGIN is invalid") from exc
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
        or parsed.path.rstrip("/")
        or parsed.netloc.endswith(":")
    ):
        raise RuntimeError("ELASTIC_AGENT_PUBLIC_ORIGIN is invalid")
    return urlunsplit((parsed.scheme.lower(), parsed.netloc.lower(), "", "", ""))


def configured_public_origin(request: Request | None = None) -> str:
    if _management_public_origin is not None:
        return _management_public_origin
    explicit = os.environ.get("ELASTIC_AGENT_PUBLIC_ORIGIN", "").strip()
    if explicit:
        return _normalize_public_origin(explicit)
    if request is None:
        raise RuntimeError("ELASTIC_AGENT_PUBLIC_ORIGIN is not configured")
    return f"{request.url.scheme}://{request.url.netloc}"


def require_same_origin(request: Request) -> None:
    origin = request.headers.get("origin", "")
    expected = configured_public_origin(request)
    if not origin or not hmac.compare_digest(origin, expected):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Same-origin request required",
        )


async def get_session_principal(request: Request) -> AuthPrincipal | None:
    token = _session_cookie(request)
    if not token:
        return None
    return await asyncio.to_thread(
        get_management_sessions().authenticate,
        token,
        get_management_user_store(),
    )


async def require_session_principal(request: Request) -> AuthPrincipal:
    principal = await get_session_principal(request)
    if principal is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required",
        )
    return principal


def require_session_csrf(request: Request, principal: AuthPrincipal) -> None:
    require_same_origin(request)
    supplied = request.headers.get(CSRF_HEADER_NAME, "")
    if not supplied or not hmac.compare_digest(supplied, principal.csrf_token):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid CSRF token",
        )


async def require_management_auth(request: Request) -> AuthPrincipal:
    """Authenticate an explicit service Bearer token or a browser session.

    An Authorization header always takes precedence. A malformed or invalid
    Bearer credential must never fall back to a valid browser cookie.
    """

    authorization = request.headers.get("authorization")
    if authorization is not None:
        token = _extract_bearer(request)
        if not token:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid Authorization header",
            )
        keys = get_api_keys()
        if not keys:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="No API keys configured on the server",
            )
        matched = False
        for key in keys:
            matched = verify_token_constant_time(token, key) or matched
        if matched:
            return AuthPrincipal(kind="service", subject="service-token")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key",
        )

    principal = await get_session_principal(request)
    if principal is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing API key or browser session",
        )
    if principal.must_change_password:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Password change required",
        )
    if request.method.upper() not in _SAFE_METHODS:
        require_session_csrf(request, principal)
    return principal


async def require_api_key(request: Request) -> str | AuthPrincipal:
    """Preserve the historical return value for explicit service tokens."""

    principal = await require_management_auth(request)
    if principal.kind == "service":
        token = _extract_bearer(request)
        if token is None:  # Defensive: service principals require a Bearer token.
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid API key")
        return token
    return principal


APIKeyDep = Annotated[str | AuthPrincipal, Depends(require_api_key)]
