"""Browser administrator login, session, logout, and password rotation."""

from __future__ import annotations

import asyncio
import hashlib
import logging
import math
import threading
import time
from collections import deque

from fastapi import APIRouter, HTTPException, Request, Response, status
from pydantic import BaseModel, Field, SecretStr, field_validator

from elastic_agent.api.auth import (
    SESSION_COOKIE_NAME,
    AuthPrincipal,
    get_management_sessions,
    get_management_user_store,
    require_same_origin,
    require_session_csrf,
    require_session_principal,
)
from elastic_agent.core.management_auth import (
    MAX_PASSWORD_CHARACTERS,
    ManagementAuthError,
    ManagementPasswordConflictError,
    normalize_email,
)

router = APIRouter(prefix="/auth", tags=["management-auth"])
logger = logging.getLogger(__name__)
_PASSWORD_CONCURRENCY = asyncio.Semaphore(2)
_NO_STORE_HEADERS = {
    "Cache-Control": "no-store",
    "Pragma": "no-cache",
    "X-Content-Type-Options": "nosniff",
    "Referrer-Policy": "no-referrer",
}


class LoginRequest(BaseModel):
    email: str = Field(min_length=3, max_length=254)
    password: SecretStr = Field(min_length=1, max_length=MAX_PASSWORD_CHARACTERS)

    @field_validator("password")
    @classmethod
    def password_must_be_printable(cls, value: SecretStr) -> SecretStr:
        if any(not character.isprintable() for character in value.get_secret_value()):
            raise ValueError("password contains invalid characters")
        return value


class PasswordChangeRequest(BaseModel):
    current_password: SecretStr = Field(min_length=1, max_length=MAX_PASSWORD_CHARACTERS)
    new_password: SecretStr = Field(min_length=12, max_length=MAX_PASSWORD_CHARACTERS)

    @field_validator("current_password", "new_password")
    @classmethod
    def passwords_must_be_printable(cls, value: SecretStr) -> SecretStr:
        if any(not character.isprintable() for character in value.get_secret_value()):
            raise ValueError("password contains invalid characters")
        return value


class _LoginAttemptLimiter:
    """Bounded process-local limiter keyed by direct peer and normalized email."""

    def __init__(self, *, window_seconds: int = 15 * 60) -> None:
        self.window_seconds = window_seconds
        self._lock = threading.Lock()
        self._attempts: dict[str, deque[float]] = {}

    def _prune_locked(self, now: float) -> None:
        cutoff = now - self.window_seconds
        for key in list(self._attempts):
            attempts = self._attempts[key]
            while attempts and attempts[0] <= cutoff:
                attempts.popleft()
            if not attempts:
                self._attempts.pop(key, None)
        if len(self._attempts) > 10_000:
            oldest = sorted(
                self._attempts,
                key=lambda key: self._attempts[key][-1],
            )[: len(self._attempts) - 10_000]
            for key in oldest:
                self._attempts.pop(key, None)

    def _retry_after(self, key: str, limit: int) -> int:
        now = time.monotonic()
        with self._lock:
            self._prune_locked(now)
            attempts = self._attempts.get(key, ())
            if len(attempts) < limit:
                return 0
            return max(0, math.ceil(attempts[0] + self.window_seconds - now))

    def source_retry_after(self, source: str) -> int:
        return self._retry_after(f"source:{source}", 30)

    def identity_retry_after(self, email: str) -> int:
        return self._retry_after(f"identity:{email}", 5)

    def failure(self, source: str, email: str) -> None:
        now = time.monotonic()
        with self._lock:
            self._prune_locked(now)
            for key in (f"source:{source}", f"identity:{email}"):
                self._attempts.setdefault(key, deque()).append(now)

    def success(self, email: str) -> None:
        with self._lock:
            self._attempts.pop(f"identity:{email}", None)

    def reset(self) -> None:
        with self._lock:
            self._attempts.clear()


_LOGIN_LIMITER = _LoginAttemptLimiter()


def reset_login_limiter() -> None:
    _LOGIN_LIMITER.reset()


def _direct_peer(request: Request) -> str:
    # Do not trust caller-controlled forwarding headers. In production the
    # local reverse proxy is the direct peer, which gives a safe global cap.
    return request.client.host if request.client is not None else "unknown"


def _require_json(request: Request) -> None:
    content_type = request.headers.get("content-type", "").partition(";")[0].strip().lower()
    if content_type != "application/json":
        raise HTTPException(
            status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
            detail="application/json required",
        )


def _set_session_cookie(response: Response, token: str) -> None:
    response.set_cookie(
        SESSION_COOKIE_NAME,
        token,
        max_age=get_management_sessions().ttl_seconds,
        secure=True,
        httponly=True,
        samesite="strict",
        path="/",
    )


def _clear_session_cookie(response: Response) -> None:
    response.delete_cookie(
        SESSION_COOKIE_NAME,
        secure=True,
        httponly=True,
        samesite="strict",
        path="/",
    )


def _principal_payload(principal: AuthPrincipal) -> dict:
    return {
        "email": principal.subject,
        "role": principal.role,
        "must_change_password": principal.must_change_password,
        "csrf_token": principal.csrf_token,
    }


def _password_change_identity(request: Request) -> str:
    token = request.cookies.get(SESSION_COOKIE_NAME, "")
    return "password-change:" + hashlib.sha256(token.encode("ascii")).hexdigest()


@router.post("/login")
async def login(
    incoming: LoginRequest,
    request: Request,
    response: Response,
) -> dict:
    _require_json(request)
    require_same_origin(request)
    source = _direct_peer(request)
    try:
        email = normalize_email(incoming.email)
    except ValueError:
        # Invalid and unknown identities deliberately follow the same response.
        email = "invalid@example.invalid"
    source_retry_after = _LOGIN_LIMITER.source_retry_after(source)
    if source_retry_after:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Too many login attempts",
            headers={"Retry-After": str(source_retry_after), **_NO_STORE_HEADERS},
        )
    identity_retry_after = _LOGIN_LIMITER.identity_retry_after(email)

    try:
        async with _PASSWORD_CONCURRENCY:
            user = await asyncio.to_thread(
                get_management_user_store().verify_credentials,
                email,
                incoming.password.get_secret_value(),
            )
    except ManagementAuthError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Administrator authentication is unavailable",
            headers=dict(_NO_STORE_HEADERS),
        ) from exc
    if user is None or not user.enabled or user.role != "admin":
        _LOGIN_LIMITER.failure(source, email)
        logger.warning("Management login rejected from direct peer %s", source)
        if identity_retry_after:
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail="Too many login attempts",
                headers={"Retry-After": str(identity_retry_after), **_NO_STORE_HEADERS},
            )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password",
            headers=dict(_NO_STORE_HEADERS),
        )

    old_token = request.cookies.get(SESSION_COOKIE_NAME)
    if old_token:
        get_management_sessions().revoke(old_token)
    token, principal = get_management_sessions().create(user)
    _LOGIN_LIMITER.success(email)
    _set_session_cookie(response, token)
    response.headers.update(_NO_STORE_HEADERS)
    logger.info("Management user logged in: %s", user.email)
    return _principal_payload(principal)


@router.get("/me")
async def current_session(request: Request, response: Response) -> dict:
    principal = await require_session_principal(request)
    response.headers.update(_NO_STORE_HEADERS)
    return _principal_payload(principal)


@router.post("/logout", status_code=status.HTTP_204_NO_CONTENT)
async def logout(request: Request) -> Response:
    principal = await require_session_principal(request)
    require_session_csrf(request, principal)
    get_management_sessions().revoke(request.cookies.get(SESSION_COOKIE_NAME))
    response = Response(status_code=status.HTTP_204_NO_CONTENT)
    _clear_session_cookie(response)
    response.headers.update(_NO_STORE_HEADERS)
    logger.info("Management user logged out: %s", principal.subject)
    return response


@router.post("/password")
async def change_password(
    incoming: PasswordChangeRequest,
    request: Request,
    response: Response,
) -> dict:
    _require_json(request)
    principal = await require_session_principal(request)
    require_session_csrf(request, principal)
    current_password = incoming.current_password.get_secret_value()
    new_password = incoming.new_password.get_secret_value()
    if current_password == new_password:
        raise HTTPException(422, "New password must be different")

    store = get_management_user_store()
    source = _direct_peer(request)
    attempt_identity = _password_change_identity(request)
    retry_after = max(
        _LOGIN_LIMITER.source_retry_after(source),
        _LOGIN_LIMITER.identity_retry_after(attempt_identity),
    )
    if retry_after:
        get_management_sessions().revoke(request.cookies.get(SESSION_COOKIE_NAME))
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required",
            headers={"Retry-After": str(retry_after), **_NO_STORE_HEADERS},
        )
    try:
        async with _PASSWORD_CONCURRENCY:
            current_user = await asyncio.to_thread(
                store.verify_credentials,
                principal.subject,
                current_password,
            )
            if current_user is None:
                _LOGIN_LIMITER.failure(source, attempt_identity)
                if _LOGIN_LIMITER.identity_retry_after(attempt_identity):
                    get_management_sessions().revoke(
                        request.cookies.get(SESSION_COOKIE_NAME)
                    )
                    raise HTTPException(
                        status_code=status.HTTP_401_UNAUTHORIZED,
                        detail="Authentication required",
                        headers=dict(_NO_STORE_HEADERS),
                    )
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Current password is incorrect",
                )
            updated = await asyncio.to_thread(
                store.set_password,
                principal.subject,
                new_password,
                must_change_password=False,
                expected_password_version=principal.password_version,
            )
    except ManagementPasswordConflictError as exc:
        get_management_sessions().revoke(request.cookies.get(SESSION_COOKIE_NAME))
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Password changed concurrently; sign in again",
            headers=dict(_NO_STORE_HEADERS),
        ) from exc
    except ManagementAuthError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Administrator authentication is unavailable",
            headers=dict(_NO_STORE_HEADERS),
        ) from exc

    sessions = get_management_sessions()
    _LOGIN_LIMITER.success(attempt_identity)
    sessions.revoke_user(principal.subject)
    token, replacement = sessions.create(updated)
    _set_session_cookie(response, token)
    response.headers.update(_NO_STORE_HEADERS)
    logger.info("Management password changed: %s", principal.subject)
    return _principal_payload(replacement)
