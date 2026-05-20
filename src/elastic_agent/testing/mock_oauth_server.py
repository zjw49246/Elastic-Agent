"""MockOAuthServer — simulates 171mail + Anthropic OAuth + Usage API.

T-202: Local HTTP server that mocks all external APIs used in the
ClaudeOAuthProvider login flow and QuotaMonitor checking.
Configurable scenarios: success, timeout, rate-limit, quota thresholds.
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

import uvicorn

logger = logging.getLogger(__name__)


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class OAuthScenario:
    """Controls MockOAuthServer behavior for different test scenarios."""

    send_magic_link_success: bool = True
    magic_link_code: str = "mock-magic-link-code"
    magic_link_delay: float = 0.0
    poll_attempts_before_code: int = 0

    verify_success: bool = True
    verify_cookie: str = "mock-session-cookie"
    verify_session_key: str = "mock-session-key"

    token_success: bool = True
    access_token: str = "mock-access-token"
    refresh_token: str = "mock-refresh-token"
    token_expires_in: int = 3600

    account_email: str = "test@example.com"

    usage_five_hour_pct: float = 20.0
    usage_seven_day_pct: float = 10.0
    usage_five_hour_resets_at: str = ""
    usage_seven_day_resets_at: str = ""
    usage_rate_limited: bool = False


class MockOAuthServer:
    """HTTP server simulating 171mail + Anthropic OAuth + Usage API.

    Usage::

        server = MockOAuthServer()
        await server.start()  # binds to a random port

        # Configure scenario
        server.scenario.usage_five_hour_pct = 90.0  # near quota

        # Use server.url as base URL for ClaudeOAuthProvider / QuotaChecker
        print(server.url)  # e.g., "http://127.0.0.1:54321"

        await server.stop()

    Endpoints:

        171mail:
            POST /api/v1/claude/send       — send magic link
            GET  /api/v1/getClaudeMessage  — poll for magic link code
            POST /api/v1/claude/verify     — verify magic link

        Anthropic:
            GET  /api/account              — account info
            POST /v1/oauth/token           — exchange code for tokens
            GET  /api/oauth/usage          — quota usage
    """

    def __init__(self, scenario: OAuthScenario | None = None, host: str = "127.0.0.1", port: int = 0) -> None:
        self.scenario = scenario or OAuthScenario()
        self._host = host
        self._port = port
        self._server: uvicorn.Server | None = None
        self._task: asyncio.Task | None = None
        self._requests: list[dict] = []
        self._poll_count: int = 0
        self._app = self._build_app()

    @property
    def url(self) -> str:
        return f"http://{self._host}:{self._port}"

    @property
    def requests(self) -> list[dict]:
        """All received requests as dicts with keys: method, path, body."""
        return list(self._requests)

    def get_requests(self, path: str | None = None, method: str | None = None) -> list[dict]:
        result = self._requests
        if path:
            result = [r for r in result if r["path"] == path]
        if method:
            result = [r for r in result if r["method"] == method.upper()]
        return result

    def reset(self) -> None:
        """Reset recorded requests and poll counter."""
        self._requests.clear()
        self._poll_count = 0

    def _record_request(self, method: str, path: str, body: Any = None) -> None:
        self._requests.append({
            "method": method,
            "path": path,
            "body": body,
            "timestamp": _utcnow_iso(),
        })

    def _build_app(self) -> Starlette:

        async def send_magic_link(request: Request) -> JSONResponse:
            body = await request.json() if request.headers.get("content-type", "").startswith("application/json") else {}
            self._record_request("POST", "/api/v1/claude/send", body)
            if not self.scenario.send_magic_link_success:
                return JSONResponse({"error": "send failed"}, status_code=500)
            return JSONResponse({
                "deviceId": f"mock-device-{uuid.uuid4().hex[:8]}",
                "clientSha": f"mock-sha-{uuid.uuid4().hex[:8]}",
            })

        async def get_claude_message(request: Request) -> JSONResponse:
            self._record_request("GET", "/api/v1/getClaudeMessage")
            self._poll_count += 1

            if self.scenario.magic_link_delay > 0:
                await asyncio.sleep(self.scenario.magic_link_delay)

            if self._poll_count <= self.scenario.poll_attempts_before_code:
                return JSONResponse({"code": None, "message": "waiting"})

            return JSONResponse({"code": self.scenario.magic_link_code})

        async def verify_magic_link(request: Request) -> JSONResponse:
            body = await request.json() if request.headers.get("content-type", "").startswith("application/json") else {}
            self._record_request("POST", "/api/v1/claude/verify", body)
            if not self.scenario.verify_success:
                return JSONResponse({"error": "verification failed"}, status_code=401)
            return JSONResponse({
                "cookie": self.scenario.verify_cookie,
                "sessionKey": self.scenario.verify_session_key,
            })

        async def get_account(request: Request) -> JSONResponse:
            self._record_request("GET", "/api/account")
            return JSONResponse({
                "email_address": self.scenario.account_email,
                "account_id": "mock-account-id",
            })

        async def oauth_token(request: Request) -> JSONResponse:
            body = await request.json() if request.headers.get("content-type", "").startswith("application/json") else {}
            self._record_request("POST", "/v1/oauth/token", body)
            if not self.scenario.token_success:
                return JSONResponse({"error": "invalid_grant"}, status_code=400)
            return JSONResponse({
                "access_token": self.scenario.access_token,
                "refresh_token": self.scenario.refresh_token,
                "expires_in": self.scenario.token_expires_in,
                "token_type": "bearer",
            })

        async def get_usage(request: Request) -> JSONResponse:
            self._record_request("GET", "/api/oauth/usage")
            if self.scenario.usage_rate_limited:
                return JSONResponse({"error": "rate limited"}, status_code=429)

            five_resets = self.scenario.usage_five_hour_resets_at or _utcnow_iso()
            seven_resets = self.scenario.usage_seven_day_resets_at or _utcnow_iso()
            return JSONResponse({
                "five_hour": {
                    "utilization": self.scenario.usage_five_hour_pct,
                    "resets_at": five_resets,
                },
                "seven_day": {
                    "utilization": self.scenario.usage_seven_day_pct,
                    "resets_at": seven_resets,
                },
            })

        routes = [
            Route("/api/v1/claude/send", send_magic_link, methods=["POST"]),
            Route("/api/v1/getClaudeMessage", get_claude_message, methods=["GET"]),
            Route("/api/v1/claude/verify", verify_magic_link, methods=["POST"]),
            Route("/api/account", get_account, methods=["GET"]),
            Route("/v1/oauth/token", oauth_token, methods=["POST"]),
            Route("/api/oauth/usage", get_usage, methods=["GET"]),
        ]
        return Starlette(routes=routes)

    async def start(self) -> None:
        """Start the mock server. If port is 0, a random port is assigned."""
        if self._port == 0:
            import socket
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind(("127.0.0.1", 0))
                self._port = s.getsockname()[1]

        config = uvicorn.Config(
            app=self._app,
            host=self._host,
            port=self._port,
            log_level="warning",
        )
        self._server = uvicorn.Server(config)
        self._task = asyncio.create_task(self._server.serve())
        await asyncio.sleep(0.3)

    async def stop(self) -> None:
        """Stop the mock server."""
        if self._server:
            self._server.should_exit = True
        if self._task:
            try:
                await asyncio.wait_for(self._task, timeout=5.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                self._task.cancel()
            self._task = None
        self._server = None
