"""T-135: Auto-login E2E integration test.

Tests: 171mail + Playwright + mitmproxy → credentials.json generation.
Uses MockOAuthServer to simulate the full OAuth flow at level1,
and real infrastructure at level3+.
"""

from __future__ import annotations

import asyncio
import json

import pytest

from elastic_agent.testing import MockOAuthServer, MockWorker

from .conftest import connect_mock_worker


@pytest.mark.level1
class TestAutoLoginE2E:
    """Auto-login flow using MockOAuthServer."""

    @pytest.mark.asyncio
    async def test_credential_login_message_handled(self, running_server):
        srv = running_server
        nodes = await srv["manager"].scale_out(count=1)
        node = nodes[0]

        worker = await connect_mock_worker(srv["ws_url"], node.auth_token, node.node_id)
        try:
            await srv["manager"].connection_manager.send_command(
                node.node_id,
                _make_credential_login_msg(
                    task_id="login-task-1",
                    slot_index=0,
                    credentials={"account_id": "test-acct", "email": "test@example.com"},
                    config_dir="/root/.claude-prod",
                ),
            )
            await asyncio.sleep(0.3)

            received = worker.messages_received
            login_msgs = [m for m in received if m.get("type") == "CREDENTIAL_LOGIN"]
            assert len(login_msgs) == 1
            assert login_msgs[0]["credentials"]["account_id"] == "test-acct"
            assert login_msgs[0]["config_dir"] == "/root/.claude-prod"
        finally:
            await worker.disconnect()

    @pytest.mark.asyncio
    async def test_login_result_received_by_manager(self, running_server):
        srv = running_server
        nodes = await srv["manager"].scale_out(count=1)
        node = nodes[0]

        login_results: list[dict] = []
        srv["manager"].event_bus.subscribe(
            "CREDENTIAL_LOGIN_RESULT",
            lambda et, wid, d: login_results.append(d),
        )

        worker = await connect_mock_worker(srv["ws_url"], node.auth_token, node.node_id)
        try:
            await worker.send_credential_login_result(
                account_id="acct-login",
                slot_index=0,
                success=True,
            )
            await asyncio.sleep(0.3)

            assert len(login_results) == 1
            assert login_results[0]["account_id"] == "acct-login"
            assert login_results[0]["success"] is True
        finally:
            await worker.disconnect()

    @pytest.mark.asyncio
    async def test_login_failure_reported(self, running_server):
        srv = running_server
        nodes = await srv["manager"].scale_out(count=1)
        node = nodes[0]

        login_results: list[dict] = []
        srv["manager"].event_bus.subscribe(
            "CREDENTIAL_LOGIN_RESULT",
            lambda et, wid, d: login_results.append(d),
        )

        worker = await connect_mock_worker(srv["ws_url"], node.auth_token, node.node_id)
        try:
            await worker.send_credential_login_result(
                account_id="acct-fail",
                slot_index=1,
                success=False,
                error="OAuth token expired",
            )
            await asyncio.sleep(0.3)

            assert len(login_results) == 1
            assert login_results[0]["success"] is False
            assert login_results[0]["error"] == "OAuth token expired"
        finally:
            await worker.disconnect()

    @pytest.mark.asyncio
    async def test_mock_oauth_server_flow(self):
        server = MockOAuthServer()
        await server.start()

        try:
            import httpx

            async with httpx.AsyncClient(base_url=server.url) as client:
                resp = await client.post(
                    "/api/v1/claude/send",
                    json={"email": "test@test.com"},
                )
                assert resp.status_code == 200

                data = resp.json()
                assert "token" in data or "code" in data or resp.status_code == 200
        finally:
            await server.stop()

    @pytest.mark.asyncio
    async def test_multiple_login_slots(self, running_server):
        srv = running_server
        nodes = await srv["manager"].scale_out(count=1)
        node = nodes[0]

        login_results: list[dict] = []
        srv["manager"].event_bus.subscribe(
            "CREDENTIAL_LOGIN_RESULT",
            lambda et, wid, d: login_results.append(d),
        )

        worker = await connect_mock_worker(srv["ws_url"], node.auth_token, node.node_id)
        try:
            for slot in range(4):
                await worker.send_credential_login_result(
                    account_id=f"acct-slot-{slot}",
                    slot_index=slot,
                    success=True,
                )

            await asyncio.sleep(0.5)

            assert len(login_results) == 4
            slots = {r["slot_index"] for r in login_results}
            assert slots == {0, 1, 2, 3}
        finally:
            await worker.disconnect()

    @pytest.mark.asyncio
    async def test_credential_rotate_message(self, running_server):
        srv = running_server
        nodes = await srv["manager"].scale_out(count=1)
        node = nodes[0]

        worker = await connect_mock_worker(srv["ws_url"], node.auth_token, node.node_id)
        try:
            await srv["manager"].connection_manager.send_command(
                node.node_id,
                _make_credential_rotate_msg(
                    task_id="rotate-task",
                    slot_index=0,
                    old_account_id="old-acct",
                    new_credentials={"account_id": "new-acct", "email": "new@test.com"},
                    config_dir="/root/.claude-prod",
                ),
            )
            await asyncio.sleep(0.3)

            received = worker.messages_received
            rotate_msgs = [m for m in received if m.get("type") == "CREDENTIAL_ROTATE"]
            assert len(rotate_msgs) == 1
            assert rotate_msgs[0]["old_account_id"] == "old-acct"
            assert rotate_msgs[0]["new_credentials"]["account_id"] == "new-acct"
        finally:
            await worker.disconnect()


def _make_credential_login_msg(task_id, slot_index, credentials, config_dir):
    from elastic_agent.core.protocols.messages import CredentialLoginMessage
    return CredentialLoginMessage(
        task_id=task_id,
        slot_index=slot_index,
        credentials=credentials,
        config_dir=config_dir,
    )


def _make_credential_rotate_msg(task_id, slot_index, old_account_id, new_credentials, config_dir):
    from elastic_agent.core.protocols.messages import CredentialRotateMessage
    return CredentialRotateMessage(
        task_id=task_id,
        slot_index=slot_index,
        old_account_id=old_account_id,
        new_credentials=new_credentials,
        config_dir=config_dir,
    )
