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
    resolve_provider,
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
        assert config.login_timeout == 480
        assert config.mitm_port == 8765
        assert config.provider is None

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


class TestResolveProvider:
    def test_mailcom_domain_autodetected(self):
        cfg = OAuthConfig(account_id="a", email="u@mail.com", email_token="t", config_dir="/tmp/x")
        assert resolve_provider(cfg) == "mailcom"

    def test_other_domain_uses_171mail(self):
        cfg = OAuthConfig(account_id="a", email="u@foo.com", email_token="t", config_dir="/tmp/x")
        assert resolve_provider(cfg) == "171mail"

    def test_explicit_provider_overrides_autodetect(self):
        cfg = OAuthConfig(
            account_id="a", email="u@mail.com", email_token="t",
            config_dir="/tmp/x", provider="171mail",
        )
        assert resolve_provider(cfg) == "171mail"


class TestClaudeOAuthProviderLocal:
    """Local mode (worker_host is None): delegates to the vendored
    perform_login in-process and reads back the credentials it writes."""

    @pytest.mark.asyncio
    async def test_success_returns_written_tokens(self, oauth_config):
        provider = ClaudeOAuthProvider()

        async def fake_perform_login(*, email, token_171, config_dir, provider):
            write_credentials(config_dir, {
                "accessToken": "at", "refreshToken": "rt", "expiresAt": 111,
            })
            return True

        with patch("elastic_agent.worker.login.perform_login",
                   new=AsyncMock(side_effect=fake_perform_login)):
            result = await provider.login(oauth_config)
        assert result.success
        assert result.access_token == "at"
        assert result.refresh_token == "rt"
        assert result.expires_at == 111

    @pytest.mark.asyncio
    async def test_failure_surfaces_error(self, oauth_config):
        provider = ClaudeOAuthProvider()
        with patch("elastic_agent.worker.login.perform_login",
                   new=AsyncMock(return_value=False)):
            result = await provider.login(oauth_config)
        assert not result.success
        assert "auto_login failed" in result.error

    @pytest.mark.asyncio
    async def test_success_but_no_credentials_is_failure(self, oauth_config):
        provider = ClaudeOAuthProvider()
        # perform_login claims success but wrote nothing to config_dir.
        with patch("elastic_agent.worker.login.perform_login",
                   new=AsyncMock(return_value=True)):
            result = await provider.login(oauth_config)
        assert not result.success
        assert "no credentials" in result.error

    @pytest.mark.asyncio
    async def test_passes_resolved_provider_to_perform_login(self, oauth_config):
        provider = ClaudeOAuthProvider()
        oauth_config.email = "u@mail.com"  # → mailcom
        mock = AsyncMock(return_value=False)
        with patch("elastic_agent.worker.login.perform_login", new=mock):
            await provider.login(oauth_config)
        assert mock.await_args.kwargs["provider"] == "mailcom"
        assert mock.await_args.kwargs["config_dir"] == oauth_config.config_dir

    @pytest.mark.asyncio
    async def test_timeout_returns_failure(self, oauth_config):
        provider = ClaudeOAuthProvider()
        oauth_config.login_timeout = 0.05

        async def hang(**_):
            await asyncio.sleep(5)

        with patch("elastic_agent.worker.login.perform_login", new=hang):
            result = await provider.login(oauth_config)
        assert not result.success
        assert "timed out" in result.error


class TestClaudeOAuthProviderRemote:
    """Remote mode (worker_host set): Manager triggers, the worker runs the
    vendored auto_login CLI over SSH; the credential file stays on the worker."""

    def _remote_config(self):
        return OAuthConfig(
            account_id="a", email="u@foo.com", email_token="tok",
            config_dir="/root/.claude-prod", worker_host="10.0.0.5",
            ssh_key_path="/k.pem", ssh_user="root",
        )

    @pytest.mark.asyncio
    async def test_runs_vendored_cli_on_worker(self):
        provider = ClaudeOAuthProvider()
        cfg = self._remote_config()
        scripts: list[str] = []

        async def fake_ssh_run(config, script, timeout):
            scripts.append(script)
            if "auto_login" in script:
                return True, "SUCCESS"
            # the credential read-back
            return True, json.dumps({"claudeAiOauth": {
                "accessToken": "AT", "refreshToken": "RT", "expiresAt": 222,
            }})

        with patch.object(provider, "_ssh_run", new=fake_ssh_run):
            result = await provider.login(cfg)

        assert result.success
        assert result.access_token == "AT"
        login_script = next(s for s in scripts if "auto_login" in s)
        assert "elastic_agent.worker.login.auto_login" in login_script
        assert "u@foo.com" in login_script
        assert "--login-method 171mail" in login_script
        assert "Xvfb :99" in login_script  # worker-side headless display

    @pytest.mark.asyncio
    async def test_worker_login_failure_surfaces_output(self):
        provider = ClaudeOAuthProvider()
        cfg = self._remote_config()

        async def fake_ssh_run(config, script, timeout):
            return False, "chrome crashed on worker"

        with patch.object(provider, "_ssh_run", new=fake_ssh_run):
            result = await provider.login(cfg)
        assert not result.success
        assert "chrome crashed on worker" in result.error



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

    @pytest.mark.asyncio
    async def test_refresh_no_http_client_uses_stdlib_not_aiohttp(self):
        # With no injected http_client the refresh must succeed via the stdlib
        # (urllib) path — the worker framework env may not ship aiohttp, and a
        # missing aiohttp used to crash every QuotaChecker refresh.
        class _Resp:
            status = 200
            def read(self):
                return json.dumps({
                    "access_token": "AT", "refresh_token": "RT2", "expires_in": 3600,
                }).encode()
            def __enter__(self):
                return self
            def __exit__(self, *a):
                return False

        with patch("urllib.request.urlopen", return_value=_Resp()):
            result = await refresh_access_token("old-refresh")
        assert result is not None
        assert result["accessToken"] == "AT"
        assert result["refreshToken"] == "RT2"
        assert result["expiresAt"] > int(time.time() * 1000)

    @pytest.mark.asyncio
    async def test_refresh_no_http_client_transport_error_returns_none(self):
        with patch("urllib.request.urlopen", side_effect=Exception("boom")):
            result = await refresh_access_token("old-refresh")
        assert result is None


class TestOAuthConstants:
    def test_client_id(self):
        assert OAUTH_CLIENT_ID == "9d1c250a-e61b-44d9-88ed-5944d1962f5e"
