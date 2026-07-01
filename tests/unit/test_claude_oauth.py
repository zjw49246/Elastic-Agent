"""Tests for ClaudeOAuthProvider — T-041."""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from elastic_agent.core.claude_oauth import (
    OAUTH_CLIENT_ID,
    ClaudeOAuthProvider,
    LoginResult,
    OAuthConfig,
    read_credentials,
    refresh_access_token,
    write_credentials,
)


class MockHTTPClient:
    """Mock HTTP client for testing OAuth flows."""

    def __init__(self):
        self.post_responses: dict[str, dict] = {}
        self.get_responses: dict[str, dict] = {}
        self.post_calls: list[tuple[str, dict]] = []
        self.get_calls: list[tuple[str, dict | None]] = []

    async def post(self, url: str, json_body: dict) -> dict:
        self.post_calls.append((url, json_body))
        for pattern, response in self.post_responses.items():
            if pattern in url:
                return response
        return {"success": False, "error": "no mock configured"}

    async def get(self, url: str, params: dict | None = None) -> dict:
        self.get_calls.append((url, params))
        for pattern, response in self.get_responses.items():
            if pattern in url:
                return response
        return {}

    async def get_raw(self, url: str) -> str:
        return "OK"

    async def post_form(self, url: str, body: str) -> dict:
        self.post_calls.append((url, {"_form_body": body}))
        for pattern, response in self.post_responses.items():
            if pattern in url:
                return response
        return {}

    async def get_usage(self, access_token: str) -> dict:
        return {
            "five_hour": {"utilization": 45, "resets_at": "2026-05-19T17:34:56Z"},
            "seven_day": {"utilization": 62, "resets_at": "2026-05-26T00:00:00Z"},
        }


@pytest.fixture
def mock_http():
    return MockHTTPClient()


@pytest.fixture
def oauth_provider(mock_http):
    return ClaudeOAuthProvider(http_client=mock_http)


@pytest.fixture
def oauth_config(tmp_path):
    config_dir = str(tmp_path / "claude-config")
    Path(config_dir).mkdir(parents=True)
    return OAuthConfig(
        account_id="test-account-1",
        email="test@example.com",
        email_token="token-123",
        config_dir=config_dir,
        login_timeout=10,
    )


class TestOAuthConfig:
    def test_defaults(self):
        config = OAuthConfig(
            account_id="a1",
            email="test@test.com",
            email_token="tok",
            config_dir="/tmp/claude",
        )
        assert config.login_timeout == 240
        assert config.mitm_port == 8765

    def test_custom_values(self):
        config = OAuthConfig(
            account_id="a2",
            email="foo@bar.com",
            email_token="t2",
            config_dir="/opt/claude",
            login_timeout=120,
            mitm_port=9999,
        )
        assert config.login_timeout == 120
        assert config.mitm_port == 9999


class TestLoginResult:
    def test_success_result(self):
        r = LoginResult(success=True, account_id="a1", expires_at=123456)
        assert r.success
        assert r.account_id == "a1"
        assert r.expires_at == 123456
        assert r.error is None

    def test_failure_result(self):
        r = LoginResult(success=False, account_id="a1", error="timeout")
        assert not r.success
        assert r.error == "timeout"


class TestClaudeOAuthProvider:
    @pytest.mark.asyncio
    async def test_extract_org_uuid_prefers_account_api(self, oauth_provider):
        class FakeCDP:
            def __init__(self):
                self.calls = []

            async def evaluate(self, expression, timeout=30):
                self.calls.append(expression)
                if "/api/account" in expression:
                    return "11111111-2222-3333-4444-555555555555"
                return "99999999-9999-9999-9999-999999999999"

        cdp = FakeCDP()
        org_uuid = await oauth_provider._extract_org_uuid(cdp)

        assert org_uuid == "11111111-2222-3333-4444-555555555555"
        assert len(cdp.calls) == 1

    @pytest.mark.asyncio
    async def test_extract_org_uuid_falls_back_to_react_state(self, oauth_provider):
        class FakeCDP:
            def __init__(self):
                self.calls = []

            async def evaluate(self, expression, timeout=30):
                self.calls.append(expression)
                if "/api/account" in expression:
                    return None
                return "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"

        cdp = FakeCDP()
        org_uuid = await oauth_provider._extract_org_uuid(cdp)

        assert org_uuid == "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
        assert len(cdp.calls) == 2

    @pytest.mark.asyncio
    async def test_send_magic_link(self, oauth_provider, mock_http):
        mock_http.post_responses["claude/send"] = {
            "success": True,
            "deviceId": "dev-1",
            "clientSha": "sha-1",
        }
        result = await oauth_provider._send_magic_link("test@example.com")
        assert result["success"]
        assert result["deviceId"] == "dev-1"
        assert len(mock_http.post_calls) == 1
        assert "claude/send" in mock_http.post_calls[0][0]

    @pytest.mark.asyncio
    async def test_poll_magic_link_found(self, oauth_provider, mock_http):
        mock_http.get_responses["getClaudeMessage"] = {
            "link": "https://claude.ai/magic-link/abc123"
        }
        link = await oauth_provider._poll_magic_link("token-123")
        assert link == "https://claude.ai/magic-link/abc123"

    @pytest.mark.asyncio
    async def test_poll_magic_link_timeout(self, oauth_provider, mock_http):
        mock_http.get_responses["getClaudeMessage"] = {}
        import elastic_agent.core.claude_oauth as mod
        orig = mod.MAGIC_LINK_TIMEOUT
        mod.MAGIC_LINK_TIMEOUT = 0.1
        try:
            link = await oauth_provider._poll_magic_link("token-123")
            assert link is None
        finally:
            mod.MAGIC_LINK_TIMEOUT = orig

    @pytest.mark.asyncio
    async def test_verify_magic_link(self, oauth_provider, mock_http):
        mock_http.post_responses["claude/verify"] = {
            "success": True,
            "cookie": "session-cookie-abc",
            "sessionKey": "key-xyz",
        }
        result = await oauth_provider._verify_magic_link(
            "https://link", "dev-1", "sha-1", "test@example.com"
        )
        assert result["success"]
        assert result["cookie"] == "session-cookie-abc"

    def test_extract_code_state(self, oauth_provider):
        url = "https://platform.claude.com/oauth/code/callback?code=abc123&state=xyz"
        code, state = oauth_provider._extract_code_state(url)
        assert code == "abc123"
        assert state == "xyz"

    def test_extract_code_state_with_hash(self, oauth_provider):
        url = "https://platform.claude.com/oauth/code/callback?code=abc123%23state&state=xyz"
        code, state = oauth_provider._extract_code_state(url)
        assert "#" not in code

    def test_extract_cli_port(self, oauth_provider):
        url = "https://claude.com/cai/oauth/authorize?redirect_uri=http%3A%2F%2Flocalhost%3A54321%2Fcallback&client_id=xxx"
        port = oauth_provider._extract_cli_port(url)
        assert port == 54321

    def test_extract_cli_port_no_port(self, oauth_provider):
        url = "https://claude.com/cai/oauth/authorize?client_id=xxx"
        port = oauth_provider._extract_cli_port(url)
        assert port is None

    def test_parse_session_cookie(self, oauth_provider):
        cookies = oauth_provider._parse_session_cookie("session-abc")
        assert len(cookies) == 4
        assert all(c["name"] == "sessionKey" for c in cookies)
        assert all(c["value"] == "session-abc" for c in cookies)

    @pytest.mark.asyncio
    async def test_login_magic_link_send_failure(self, oauth_provider, mock_http, oauth_config):
        mock_http.post_responses["claude/send"] = {
            "success": False,
            "error": "invalid_email",
        }
        result = await oauth_provider.login(oauth_config)
        assert not result.success
        assert "magic link" in result.error.lower()

    @pytest.mark.asyncio
    async def test_login_magic_link_poll_timeout(self, oauth_provider, mock_http, oauth_config):
        mock_http.post_responses["claude/send"] = {
            "success": True,
            "deviceId": "d1",
            "clientSha": "s1",
        }
        mock_http.get_responses["getClaudeMessage"] = {}

        import elastic_agent.core.claude_oauth as mod
        orig = mod.MAGIC_LINK_TIMEOUT
        mod.MAGIC_LINK_TIMEOUT = 0.1
        try:
            result = await oauth_provider.login(oauth_config)
            assert not result.success
            assert "timed out" in result.error.lower()
        finally:
            mod.MAGIC_LINK_TIMEOUT = orig

    @pytest.mark.asyncio
    async def test_login_verify_failure(self, oauth_provider, mock_http, oauth_config):
        mock_http.post_responses["claude/send"] = {
            "success": True,
            "deviceId": "d1",
            "clientSha": "s1",
        }
        mock_http.get_responses["getClaudeMessage"] = {"link": "https://link"}
        mock_http.post_responses["claude/verify"] = {
            "success": False,
            "error": "invalid_link",
        }
        result = await oauth_provider.login(oauth_config)
        assert not result.success
        assert "verification" in result.error.lower()


class TestReadWriteCredentials:
    def test_write_and_read(self, tmp_path):
        config_dir = str(tmp_path / "creds")
        creds = {
            "accessToken": "sk-ant-oat01-test",
            "refreshToken": "sk-ant-ort01-test",
            "expiresAt": 1716129296000,
        }
        write_credentials(config_dir, creds)

        result = read_credentials(config_dir)
        assert result is not None
        assert result["accessToken"] == "sk-ant-oat01-test"
        assert result["refreshToken"] == "sk-ant-ort01-test"
        assert result["expiresAt"] == 1716129296000

    def test_read_nonexistent(self, tmp_path):
        assert read_credentials(str(tmp_path / "nonexistent")) is None

    def test_write_creates_directory(self, tmp_path):
        config_dir = str(tmp_path / "a" / "b" / "c")
        write_credentials(config_dir, {"accessToken": "test"})
        assert read_credentials(config_dir)["accessToken"] == "test"

    def test_write_atomic(self, tmp_path):
        config_dir = str(tmp_path / "atomic")
        write_credentials(config_dir, {"accessToken": "v1"})
        write_credentials(config_dir, {"accessToken": "v2"})
        assert read_credentials(config_dir)["accessToken"] == "v2"
        # No .tmp file should remain
        assert not (Path(config_dir) / ".credentials.tmp").exists()

    def test_read_corrupt_file(self, tmp_path):
        config_dir = str(tmp_path / "corrupt")
        Path(config_dir).mkdir(parents=True)
        (Path(config_dir) / ".credentials.json").write_text("not json")
        assert read_credentials(config_dir) is None


class TestRefreshAccessToken:
    @pytest.mark.asyncio
    async def test_refresh_success(self, mock_http):
        mock_http.post_responses["oauth/token"] = {
            "access_token": "new-access",
            "refresh_token": "new-refresh",
            "expires_in": 3600,
        }
        result = await refresh_access_token("old-refresh", http_client=mock_http)
        assert result is not None
        assert result["accessToken"] == "new-access"
        assert result["refreshToken"] == "new-refresh"
        assert result["expiresAt"] > int(time.time() * 1000)

    @pytest.mark.asyncio
    async def test_refresh_preserves_old_refresh_token(self, mock_http):
        mock_http.post_responses["oauth/token"] = {
            "access_token": "new-access",
            "expires_in": 3600,
        }
        result = await refresh_access_token("my-refresh", http_client=mock_http)
        assert result is not None
        assert result["refreshToken"] == "my-refresh"

    @pytest.mark.asyncio
    async def test_refresh_failure_returns_none(self, mock_http):
        mock_http.post_responses["oauth/token"] = {}
        result = await refresh_access_token("bad-refresh", http_client=mock_http)
        assert result is None

    @pytest.mark.asyncio
    async def test_refresh_exception_returns_none(self):
        bad_client = MagicMock()
        bad_client.post_form = AsyncMock(side_effect=Exception("network error"))
        result = await refresh_access_token("token", http_client=bad_client)
        assert result is None


class TestOAuthConstants:
    def test_client_id(self):
        assert OAUTH_CLIENT_ID == "9d1c250a-e61b-44d9-88ed-5944d1962f5e"
