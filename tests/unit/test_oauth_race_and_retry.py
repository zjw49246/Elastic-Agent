"""Tests for the logged-in redirect race fix and login_failed auto-retry."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from elastic_agent.core.claude_oauth import ClaudeOAuthProvider, LoginResult, OAuthConfig
from elastic_agent.core.credential_login_service import CredentialLoginService, CredentialSlot


class _SentinelReached(Exception):
    pass


class _FakeCDP:
    """CDP stub: email input never renders; URL flips to /new mid-retry.

    Reproduces the production race — slot N>1 logs in via the browser's
    existing session, /login redirects to /new only after the initial
    already_logged_in check has run.
    """

    def __init__(self):
        self.url_calls = 0

    async def send(self, *a, **k):
        return {}

    async def navigate(self, *a, **k):
        return {}

    async def close(self):
        pass

    async def evaluate(self, expr):
        if "document.location.href" in expr:
            self.url_calls += 1
            # First check (pre-email): still on /login (redirect in flight)
            return "https://claude.ai/login" if self.url_calls == 1 else "https://claude.ai/new"
        if "input" in expr.lower() or "email" in expr.lower():
            return "no email input"
        return ""


@pytest.mark.asyncio
async def test_logged_in_redirect_during_email_retry_recovers():
    provider = ClaudeOAuthProvider()
    config = OAuthConfig(
        account_id="acc-1", email="a@b.c", email_token="tok",
        config_dir="/tmp/x",
    )
    fake_cdp = _FakeCDP()

    with patch.object(provider, "_launch_chrome", new=AsyncMock(return_value=(MagicMock(), 12345))), \
         patch.object(provider, "_connect_cdp", new=AsyncMock(return_value=fake_cdp)), \
         patch.object(provider, "_handle_cloudflare", new=AsyncMock(return_value=True)), \
         patch.object(provider, "_launch_cli_auth", new=AsyncMock(side_effect=_SentinelReached("REACHED_CLI_AUTH"))), \
         patch.object(provider, "_cleanup", new=AsyncMock(), create=True), \
         patch("asyncio.sleep", new=AsyncMock()):
        result = await provider.login(config)

    # The race fix must carry us past the email step (no "Email input field
    # not found" failure) all the way to the CLI-auth stage.
    assert not result.success
    assert "REACHED_CLI_AUTH" in (result.error or "")
    assert "Email input" not in (result.error or "")


@pytest.mark.asyncio
async def test_email_failure_error_includes_page_diagnostics():
    provider = ClaudeOAuthProvider()
    config = OAuthConfig(
        account_id="acc-1", email="a@b.c", email_token="tok",
        config_dir="/tmp/x",
    )

    class _StuckCDP(_FakeCDP):
        async def evaluate(self, expr):
            if "document.location.href" in expr:
                return "https://claude.ai/login"  # never redirects
            if "document.title" in expr:
                return "Just a moment..."
            return "no email input"

    with patch.object(provider, "_launch_chrome", new=AsyncMock(return_value=(MagicMock(), 12345))), \
         patch.object(provider, "_connect_cdp", new=AsyncMock(return_value=_StuckCDP())), \
         patch.object(provider, "_handle_cloudflare", new=AsyncMock(return_value=True)), \
         patch("asyncio.sleep", new=AsyncMock()):
        result = await provider.login(config)

    assert not result.success
    assert "Email input field not found" in result.error
    # Diagnostics: where the page actually was
    assert "claude.ai/login" in result.error
    assert "Just a moment" in result.error


def _make_service(provider, accounts, statuses):
    pool = MagicMock()
    pool._accounts_config.accounts = accounts
    pool.get_status = lambda aid: statuses.get(aid)
    pool.update_login_status = AsyncMock()
    service = CredentialLoginService(
        credential_pool=pool,
        credential_config=SimpleNamespace(login_timeout=60),
        credential_binding=MagicMock(),
        event_bus=MagicMock(),
        slots=[CredentialSlot(slot_type="production", config_dir="/root/.claude-prod")],
        oauth_provider=provider,
    )
    service._resolve_host = AsyncMock(return_value="1.2.3.4")
    service._ensure_display_server = AsyncMock()
    return service, pool


def _acct(aid):
    return SimpleNamespace(id=aid, email=f"{aid}@x.c", email_token="tok")


def _status(login_status, assigned=None):
    return SimpleNamespace(login_status=login_status, assigned_to=assigned)


class TestRetryFailedAccounts:
    @pytest.mark.asyncio
    async def test_recovers_failed_account(self):
        provider = MagicMock()
        provider.login = AsyncMock(return_value=LoginResult(success=True, account_id="a3"))
        service, pool = _make_service(
            provider,
            accounts=[_acct("a2"), _acct("a3")],
            statuses={"a2": _status("logged_in"), "a3": _status("login_failed")},
        )
        recovered = await service.retry_failed_accounts("w1")
        assert recovered == 1
        pool.update_login_status.assert_awaited_once_with("a3", "logged_in")
        # Verification login used a scratch config_dir, not a real slot
        cfg = provider.login.call_args[0][0]
        assert cfg.config_dir == "/tmp/claude-verify-a3"

    @pytest.mark.asyncio
    async def test_skips_assigned_and_healthy(self):
        provider = MagicMock()
        provider.login = AsyncMock()
        service, pool = _make_service(
            provider,
            accounts=[_acct("a1"), _acct("a2")],
            statuses={
                "a1": _status("login_failed", assigned="w9"),  # in use
                "a2": _status("logged_in"),
            },
        )
        assert await service.retry_failed_accounts("w1") == 0
        provider.login.assert_not_called()

    @pytest.mark.asyncio
    async def test_still_failing_account_not_flipped(self):
        provider = MagicMock()
        provider.login = AsyncMock(return_value=LoginResult(
            success=False, account_id="a3", error="CF again"))
        service, pool = _make_service(
            provider,
            accounts=[_acct("a3")],
            statuses={"a3": _status("login_failed")},
        )
        assert await service.retry_failed_accounts("w1") == 0
        pool.update_login_status.assert_not_awaited()


class _FakeProc:
    def __init__(self, stdout: bytes, returncode: int = 0):
        self._stdout = stdout
        self.returncode = returncode

    async def communicate(self):
        return self._stdout, b""


@pytest.mark.asyncio
async def test_171mail_numeric_code_falls_back_to_body_link():
    provider = ClaudeOAuthProvider()
    link = "https://claude.ai/magic-link#abc123"
    provider._http_get = AsyncMock(return_value={
        "code": 200,
        "data": {
            "code": 200,
            "body": f"Click here: {link}",
        },
    })

    assert await provider._poll_magic_link("tok") == link


class TestCredentialValidity:
    @pytest.mark.asyncio
    async def test_valid_requires_matching_email(self):
        service, _ = _make_service(MagicMock(), [], {})
        auth_status = (
            b'{"loggedIn": true, "email": "expected@example.com", '
            b'"subscriptionType": "max"}'
        )

        with patch(
            "asyncio.create_subprocess_exec",
            new=AsyncMock(return_value=_FakeProc(auth_status)),
        ):
            assert await service._check_credentials_valid(
                "1.2.3.4",
                "/root/.claude-prod",
                expected_email="expected@example.com",
            )

    @pytest.mark.asyncio
    async def test_valid_rejects_mismatched_email(self):
        service, _ = _make_service(MagicMock(), [], {})
        auth_status = (
            b'{"loggedIn": true, "email": "other@example.com", '
            b'"subscriptionType": "max"}'
        )

        with patch(
            "asyncio.create_subprocess_exec",
            new=AsyncMock(return_value=_FakeProc(auth_status)),
        ):
            assert not await service._check_credentials_valid(
                "1.2.3.4",
                "/root/.claude-prod",
                expected_email="expected@example.com",
            )


class TestActiveWorkerCredentialRestore:
    def test_affinity_falls_back_to_persisted_last_assigned_worker(self):
        newer = datetime.now(timezone.utc)
        older = newer - timedelta(hours=1)
        pool = MagicMock()
        pool._pool_status.accounts = {
            "a1": SimpleNamespace(
                last_assigned_to="worker-1",
                last_used=older,
                last_login_at=None,
            ),
            "a2": SimpleNamespace(
                last_assigned_to="worker-1",
                last_used=newer,
                last_login_at=None,
            ),
            "a3": SimpleNamespace(
                last_assigned_to="worker-2",
                last_used=newer,
                last_login_at=None,
            ),
        }
        service = CredentialLoginService(
            credential_pool=pool,
            credential_config=SimpleNamespace(login_timeout=60),
            credential_binding=SimpleNamespace(_affinity={}),
            event_bus=MagicMock(),
        )

        assert service._get_affinity_account("worker-1") == "a2"

    @pytest.mark.asyncio
    async def test_restore_active_worker_claims_local_account(self):
        account = _acct("a1")
        pool = MagicMock()
        pool._accounts_config.accounts = [account]
        pool.claim_existing_assignment = AsyncMock(return_value=account)
        binding = MagicMock()
        binding.get_worker.return_value = None
        binding.bind = AsyncMock(return_value=True)
        event_bus = MagicMock()
        event_bus.emit = AsyncMock()
        service = CredentialLoginService(
            credential_pool=pool,
            credential_config=SimpleNamespace(login_timeout=60),
            credential_binding=binding,
            event_bus=event_bus,
            slots=[CredentialSlot(slot_type="production", config_dir="/root/.claude-prod")],
        )
        service._worker_has_active_claude_process = AsyncMock(return_value=True)
        service._resolve_host = AsyncMock(return_value="1.2.3.4")
        service._read_account_marker = AsyncMock(return_value="a1")
        service._get_credentials_status = AsyncMock(return_value=(True, "a1@x.c"))
        service._write_account_markers = AsyncMock()

        assert await service._restore_active_worker_credential("worker-1")

        pool.claim_existing_assignment.assert_awaited_once_with(
            "a1",
            "worker-1",
            "production",
            "/root/.claude-prod",
        )
        binding.bind.assert_awaited_once_with("a1", "worker-1")
        event_bus.emit.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_restore_active_worker_blocks_duplicate_account(self):
        account = _acct("a1")
        pool = MagicMock()
        pool._accounts_config.accounts = [account]
        pool.claim_existing_assignment = AsyncMock()
        binding = MagicMock()
        binding.get_worker.return_value = "worker-1"
        event_bus = MagicMock()
        event_bus.emit = AsyncMock()
        service = CredentialLoginService(
            credential_pool=pool,
            credential_config=SimpleNamespace(login_timeout=60),
            credential_binding=binding,
            event_bus=event_bus,
            slots=[CredentialSlot(slot_type="production", config_dir="/root/.claude-prod")],
        )
        service._worker_has_active_claude_process = AsyncMock(return_value=True)
        service._resolve_host = AsyncMock(return_value="1.2.3.4")
        service._read_account_marker = AsyncMock(return_value="a1")
        service._get_credentials_status = AsyncMock(return_value=(True, "a1@x.c"))

        assert await service._restore_active_worker_credential("worker-2")

        pool.claim_existing_assignment.assert_not_called()
        event_bus.emit.assert_not_called()
