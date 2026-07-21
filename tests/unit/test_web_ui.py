"""Tests for Web UI (T-029)."""

from __future__ import annotations

import os
from unittest.mock import patch

import pytest
from httpx import ASGITransport, AsyncClient

from elastic_agent.testing import create_test_manager


@pytest.fixture
async def ui_client():
    result = create_test_manager()
    from elastic_agent.api.app import create_app
    app = create_app(result.manager)
    with patch.dict(os.environ, {"ELASTIC_AGENT_EXTERNAL_API_KEYS": "test-key"}):
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://testserver",
        ) as client:
            yield client, result


class TestDashboardEndpoint:
    @pytest.mark.asyncio
    async def test_root_returns_batch_console(self, ui_client):
        # Root now serves the Batch Console (primary surface).
        client, _ = ui_client
        resp = await client.get("/")
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]
        assert "Batch Console" in resp.text
        assert "Submit Job" in resp.text

    @pytest.mark.asyncio
    async def test_batch_console_exposes_eip_account_binding_controls(self, ui_client):
        client, _ = ui_client
        resp = await client.get("/batch")
        html = resp.text

        assert 'id="jAcctBinding"' in html
        assert 'value="eip"' in html
        assert 'id="jAcctIds"' in html
        assert "selectedOptions" in html
        assert "binding: accountBinding" in html
        assert "ids: accountIds" in html
        assert "选中账号数必须等于 Workers" in html
        assert "token 提交后不回显" in html
        assert "group=codex 只是账号池标签" in html
        assert "尚无 Codex 登录/执行链路" in html
        assert "未实现通用 IMAP" in html
        assert "worker 用实例角色直拉，不经 Manager" in html

    @pytest.mark.asyncio
    async def test_fleet_dashboard(self, ui_client):
        client, _ = ui_client
        for path in ("/fleet", "/dashboard"):
            resp = await client.get(path)
            assert resp.status_code == 200
            assert "Elastic-Agent Dashboard" in resp.text

    @pytest.mark.asyncio
    async def test_fleet_contains_key_elements(self, ui_client):
        client, _ = ui_client
        resp = await client.get("/fleet")
        html = resp.text
        assert "Scale Out" in html
        assert "nodeGrid" in html
        assert "statTotal" in html
        assert "refreshNodes" in html
        assert "drainNode" in html
        assert "removeNode" in html
        assert "scaleInNode" in html

    @pytest.mark.asyncio
    async def test_no_auth_required_for_ui(self, ui_client):
        client, _ = ui_client
        resp = await client.get("/")
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_fleet_contains_api_calls(self, ui_client):
        client, _ = ui_client
        resp = await client.get("/fleet")
        html = resp.text
        assert "'/api'" in html or '"/api"' in html
        assert "/nodes" in html
        assert "/scale-out" in html
        assert "/scale-in" in html


class TestDashboardIntegration:
    @pytest.mark.asyncio
    async def test_ui_and_api_coexist(self, ui_client):
        client, _ = ui_client
        ui_resp = await client.get("/")
        assert ui_resp.status_code == 200
        api_resp = await client.get(
            "/api/nodes",
            headers={"Authorization": "Bearer test-key"},
        )
        assert api_resp.status_code == 200
