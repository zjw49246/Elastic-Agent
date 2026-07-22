"""Tests for the logged-in redirect race fix and login_failed auto-retry."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from elastic_agent.core.claude_oauth import ClaudeOAuthProvider, LoginResult, OAuthConfig
from elastic_agent.core.credential_login_service import CredentialLoginService, CredentialSlot


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


class TestWorkerHostResolution:
    @pytest.mark.asyncio
    async def test_aws_worker_uses_private_address_for_ssh(self):
        class Registry:
            async def get(self, worker_id):
                assert worker_id == "aws:i-123"
                return SimpleNamespace(
                    platform="aws",
                    public_ip="198.51.100.10",
                    private_ip="10.0.0.10",
                )

        service = CredentialLoginService(
            credential_pool=MagicMock(),
            credential_config=SimpleNamespace(login_timeout=60),
            credential_binding=MagicMock(),
            event_bus=MagicMock(),
            node_registry=Registry(),
        )

        assert await service._resolve_host("aws:i-123") == "10.0.0.10"


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
