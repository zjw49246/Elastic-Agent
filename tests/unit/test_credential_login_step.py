"""Tests for CredentialLoginStep — T-042."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

import pytest

from elastic_agent.core.claude_oauth import ClaudeOAuthProvider, LoginResult, OAuthConfig
from elastic_agent.core.config import CredentialConfig
from elastic_agent.core.credential_login_step import CredentialLoginStep
from elastic_agent.core.credential_pool import AccountDefinition, CredentialPool


@pytest.fixture
def credential_config(tmp_path):
    return CredentialConfig(
        accounts_file=str(tmp_path / "accounts.json"),
        pool_status_file=str(tmp_path / "pool_status.json"),
    )


@pytest.fixture
def pool(credential_config):
    return CredentialPool(credential_config)


@pytest.fixture
def mock_oauth():
    provider = AsyncMock(spec=ClaudeOAuthProvider)
    return provider


@pytest.fixture
def accounts():
    return [
        AccountDefinition(id="prod-1", email="prod1@test.com", email_token="tok-1"),
        AccountDefinition(id="prod-2", email="prod2@test.com", email_token="tok-2"),
        AccountDefinition(id="prod-3", email="prod3@test.com", email_token="tok-3"),
    ]


class TestCredentialLoginStep:
    @pytest.mark.asyncio
    async def test_all_logins_succeed(self, pool, mock_oauth, accounts):
        mock_oauth.login = AsyncMock(
            side_effect=[
                LoginResult(success=True, account_id="prod-1", expires_at=999),
                LoginResult(success=True, account_id="prod-2", expires_at=999),
            ]
        )

        step = CredentialLoginStep(pool, mock_oauth)
        inputs = [
            (accounts[0], "/root/.claude-prod", "production"),
            (accounts[1], "/root/.claude-edit-0", "edit"),
        ]
        results = await step.execute("worker-1", inputs)

        assert len(results) == 2
        assert all(r.success for r in results)
        assert mock_oauth.login.call_count == 2

    @pytest.mark.asyncio
    async def test_first_login_fails_rolls_back(self, pool, mock_oauth, accounts, tmp_path):
        import json

        # Write accounts.json so pool can load
        accounts_data = {
            "accounts": [a.model_dump() for a in accounts],
            "groups": {},
        }
        accounts_file = tmp_path / "accounts.json"
        accounts_file.write_text(json.dumps(accounts_data))

        config = CredentialConfig(
            accounts_file=str(accounts_file),
            pool_status_file=str(tmp_path / "pool_status.json"),
        )
        pool = CredentialPool(config)
        await pool.load()

        mock_oauth.login = AsyncMock(
            return_value=LoginResult(success=False, account_id="prod-1", error="network error")
        )

        step = CredentialLoginStep(pool, mock_oauth)
        inputs = [
            (accounts[0], "/root/.claude-prod", "production"),
            (accounts[1], "/root/.claude-edit-0", "edit"),
        ]
        results = await step.execute("worker-1", inputs)

        assert len(results) == 1
        assert not results[0].success
        assert mock_oauth.login.call_count == 1

    @pytest.mark.asyncio
    async def test_second_login_fails_rolls_back_first(self, pool, mock_oauth, accounts, tmp_path):
        import json

        accounts_data = {
            "accounts": [a.model_dump() for a in accounts],
            "groups": {},
        }
        accounts_file = tmp_path / "accounts.json"
        accounts_file.write_text(json.dumps(accounts_data))

        config = CredentialConfig(
            accounts_file=str(accounts_file),
            pool_status_file=str(tmp_path / "pool_status.json"),
        )
        pool = CredentialPool(config)
        await pool.load()

        # First succeeds, second fails
        mock_oauth.login = AsyncMock(
            side_effect=[
                LoginResult(success=True, account_id="prod-1", expires_at=999),
                LoginResult(success=False, account_id="prod-2", error="browser crashed"),
            ]
        )

        step = CredentialLoginStep(pool, mock_oauth)
        inputs = [
            (accounts[0], "/root/.claude-prod", "production"),
            (accounts[1], "/root/.claude-edit-0", "edit"),
        ]
        results = await step.execute("worker-1", inputs)

        assert len(results) == 2
        assert results[0].success
        assert not results[1].success

        # First account should be released (rollback)
        status = pool.get_status("prod-1")
        assert status.assigned_to is None

    @pytest.mark.asyncio
    async def test_callback_invoked(self, pool, mock_oauth, accounts):
        mock_oauth.login = AsyncMock(
            return_value=LoginResult(success=True, account_id="prod-1", expires_at=999)
        )

        callback = AsyncMock()
        step = CredentialLoginStep(pool, mock_oauth, on_login_result=callback)
        inputs = [(accounts[0], "/root/.claude-prod", "production")]
        await step.execute("worker-1", inputs)

        callback.assert_called_once()
        args = callback.call_args
        assert args[0][0] == "prod-1"
        assert args[0][1] == 0
        assert args[0][2].success

    @pytest.mark.asyncio
    async def test_execute_single_success(self, pool, mock_oauth, accounts):
        mock_oauth.login = AsyncMock(
            return_value=LoginResult(success=True, account_id="prod-1", expires_at=999)
        )

        step = CredentialLoginStep(pool, mock_oauth)
        result = await step.execute_single(accounts[0], "/root/.claude-prod")

        assert result.success

    @pytest.mark.asyncio
    async def test_execute_single_failure(self, pool, mock_oauth, accounts):
        mock_oauth.login = AsyncMock(
            return_value=LoginResult(success=False, account_id="prod-1", error="timeout")
        )

        step = CredentialLoginStep(pool, mock_oauth)
        result = await step.execute_single(accounts[0], "/root/.claude-prod")

        assert not result.success

    @pytest.mark.asyncio
    async def test_login_status_updates(self, pool, mock_oauth, accounts, tmp_path):
        import json

        accounts_data = {
            "accounts": [a.model_dump() for a in accounts],
            "groups": {},
        }
        accounts_file = tmp_path / "accounts.json"
        accounts_file.write_text(json.dumps(accounts_data))

        config = CredentialConfig(
            accounts_file=str(accounts_file),
            pool_status_file=str(tmp_path / "pool_status.json"),
        )
        pool = CredentialPool(config)
        await pool.load()

        mock_oauth.login = AsyncMock(
            return_value=LoginResult(success=True, account_id="prod-1", expires_at=999)
        )

        step = CredentialLoginStep(pool, mock_oauth)
        await step.execute_single(accounts[0], "/root/.claude-prod")

        status = pool.get_status("prod-1")
        assert status.login_status == "logged_in"
        assert status.last_login_at is not None

    @pytest.mark.asyncio
    async def test_empty_accounts_list(self, pool, mock_oauth):
        step = CredentialLoginStep(pool, mock_oauth)
        results = await step.execute("worker-1", [])
        assert results == []
        mock_oauth.login.assert_not_called()

    @pytest.mark.asyncio
    async def test_serial_execution_order(self, pool, mock_oauth, accounts):
        call_order = []

        async def mock_login(config):
            call_order.append(config.account_id)
            return LoginResult(success=True, account_id=config.account_id, expires_at=999)

        mock_oauth.login = mock_login

        step = CredentialLoginStep(pool, mock_oauth)
        inputs = [
            (accounts[0], "/root/.claude-0", "production"),
            (accounts[1], "/root/.claude-1", "edit"),
            (accounts[2], "/root/.claude-2", "edit"),
        ]
        results = await step.execute("worker-1", inputs)

        assert len(results) == 3
        assert call_order == ["prod-1", "prod-2", "prod-3"]
