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
        assert "提交后均不回显" in html
        assert 'id="acctPassword"' in html
        assert 'id="acctClearPassword"' in html
        assert 'id="acctClearToken"' in html
        assert 'id="jAgentType"' in html
        assert "Codex 至少配置 OpenAI 密码或接码查询 Token" in html
        assert "Codex 至少填写一项，可同时填写" in html
        assert "查询 Token 不是 OpenAI 登录凭据" in html
        assert "/accounts/login-attempts/" in html
        assert "agent_type:" in html
        assert "clear_email_token:" in html
        assert "manager_distribute" in html
        assert "distribute.disabled = agentType === 'codex'" in html
        assert "reconcileLoginAttempts" in html
        assert "container.replaceChildren()" not in html
        assert "textContent = `Codex OTP" in html
        assert "worker 用实例角色直拉，不经 Manager" in html
        assert '<textarea id="jCollect" placeholder="results">results</textarea>' in html
        assert 'id="jLoginTimeout" type="number" value="900"' in html
        assert "login_timeout_seconds:" in html
        assert "initializeProviderDefaults" in html
        assert "providerType = health.provider || ''" in html
        assert "providerDefaultsReady = initializeProviderDefaults()" in html
        assert "await providerDefaultsReady" in html
        assert 'id="jAcctMode" onchange="updateAccountModeUI()"' in html
        assert "binding.disabled = !workerLocal" in html
        assert "if (!workerLocal) binding.value = 'none'" in html
        assert "providerType === 'aws' && !eipBindingTouched" in html
        assert "accounts/bindings" in html

    @pytest.mark.asyncio
    async def test_batch_console_does_not_persist_or_put_api_key_in_download_url(
        self, ui_client
    ):
        client, _ = ui_client
        html = (await client.get("/batch")).text

        assert "localStorage" not in html
        assert "results/download?api_key=" not in html
        assert "sessionStorage" in html
        assert "function esc(value)" in html
        assert "Idempotency-Key" in html
        assert "downloadResults" in html
        assert "/cancel" in html

    @pytest.mark.asyncio
    async def test_batch_console_distinguishes_worker_history_from_live_resources(
        self, ui_client
    ):
        client, _ = ui_client
        html = (await client.get("/batch")).text
        jobs_monitor = html[
            html.index("// ---- Jobs monitor ----"):
            html.index("async function refreshJobs()")
        ]
        actions = jobs_monitor[
            jobs_monitor.index("function workerActionsHtml(worker, jobId)"):
            jobs_monitor.index("async function downloadResults")
        ]
        terminate = html[
            html.index("async function terminateWorker(wid)"):
            html.index("async function removeAccount(id)")
        ]

        assert "Worker 执行记录" in jobs_monitor
        assert "worker.worker_released === true" in jobs_monitor
        assert "worker.cleaned_up === true" in jobs_monitor
        assert "Worker 已销毁" in jobs_monitor
        assert "if (!worker.worker_id || workerReleased(worker))" in actions
        assert "const terminate = workerExecutionTerminal(worker) ? ''" in actions
        assert "showJobLogs" in actions
        assert "showWorkerLogs" in actions
        assert "workerActionsHtml(w,j.job_id)" in jobs_monitor
        assert "terminateWorker(${jsArg(worker.worker_id)})" in actions
        assert "Worker 仍存活时可看 ea-runtime systemd journal" in jobs_monitor
        assert (
            "api('POST', '/scale-in', {node_ids: [wid], force: true})"
        ) in terminate
        assert "api('DELETE'" not in terminate

    @pytest.mark.asyncio
    async def test_batch_console_updates_without_rebuilding_or_overlapping_polls(
        self, ui_client
    ):
        client, _ = ui_client
        html = (await client.get("/batch")).text

        assert "reconcileJobCards" in html
        assert "jobResultsCache" in html
        assert "dashboardPollRunning" in html
        assert "scheduleDashboardPoll" in html
        assert "jobs.filter(job =>" in html
        assert ").slice(0, 30)" in html
        assert "jobs.slice(0, 30).filter" not in html
        assert "const olderTerminal" in html
        assert "const hiddenHistory" in html
        assert "jobsList.innerHTML = jobs.map" not in html
        assert "el.outerHTML = jobRowHtml" not in html
        assert "setInterval(() => { refreshJobs()" not in html
        assert "if (document.hidden) clearTimeout(_logTimer)" in html
        assert "!document.hidden && !_logPaused" in html

    @pytest.mark.asyncio
    async def test_batch_console_jobs_start_collapsed_and_keep_user_choice(
        self, ui_client
    ):
        client, _ = ui_client
        html = (await client.get("/batch")).text

        assert '<summary class="job-summary" data-job-focus="job-summary">' in html
        assert 'class="job-detail"' in html
        assert "点击查看详情" in html
        assert "收起详情" in html
        assert "const wasOpen = node.open" in html
        assert "replacement.open = wasOpen" in html
        assert "const focusedControl = jobFocusedControl(node)" in html
        assert "restoreJobFocus(replacement, focusedControl)" in html
        assert "replacementScrolls[index].scrollLeft = scrollLeft" in html
        assert "window.scrollTo(viewportX, viewportY)" in html
        assert "state === 'failed' || state === 'running'" not in html
        assert '<span class="job-summary-main">' in html
        assert '<div class="job-summary-main">' not in html
        assert "overflow-wrap:anywhere" in html
        assert ".job-summary:focus-visible" in html

    @pytest.mark.asyncio
    async def test_batch_console_places_each_otp_on_its_exact_job_worker(
        self, ui_client
    ):
        client, _ = ui_client
        html = (await client.get("/batch")).text

        assert 'class="job-otp-summary-badge"' in html
        assert 'class="job-otp-region"' in html
        assert 'class="job-otp-list"' in html
        assert "function otpKey(attempt)" in html
        assert "attempt.login_request_id" in html
        assert "attempt.challenge_id" in html
        assert "otpCardsByKey" in html
        assert "latestLoginAttempts" in html
        assert "button.dataset.loginRequestId" in html
        assert "button.dataset.challengeId" in html
        assert "source.closest('.otp-challenge-card')" in html
        assert "card.querySelector('.otp-account-email').textContent" in html
        assert "card.querySelector('.otp-account-id').textContent" in html
        assert "card.querySelector('.otp-worker').textContent" in html
        assert "邮箱自动取码不可用或未成功，需要人工输入" in html
        assert "openedOtpChallenges" in html
        assert "jobNode.open = true" in html
        assert "setOtpActionMinimized(true)" in html
        assert "behavior:compactViewport ? 'auto' : 'smooth'" in html
        assert "if (hasNewChallenge || !latestLoginAttempts.length)" in html
        assert "toggleOtpActionCard" in html
        assert ".otp-action-card.otp-minimized" in html
        assert "const otpFocus = focusedOtpState(node)" in html
        assert "otpFocusTarget = {node:replacement, state:otpFocus}" in html
        assert "restoreOtpFocus(otpFocusTarget.node, otpFocusTarget.state)" in html
        assert "otpCards.forEach(card => otpMount.appendChild(card))" in html
        assert "otpDrafts" not in html

    @pytest.mark.asyncio
    async def test_batch_console_has_light_theme_help_and_job_log_viewer(
        self, ui_client
    ):
        client, _ = ui_client
        html = (await client.get("/batch")).text

        assert 'data-theme="light"' in html
        assert "toggleTheme" in html
        assert "Job 怎么运行" in html
        assert "任务输出" in html
        assert "/jobs/' + encodeURIComponent(jobId) + '/logs" in html
        assert "复制日志" in html
        assert "跟随最新" in html
        assert "j.error" in html
        assert "cleanup_pending" in html
        assert "const workerGone = _logMode === 'worker'" in html
        assert "[404, 409].includes(Number(error.status))" in html
        assert "if (workerGone) {" in html
        assert "_logTimer = null" in html

    @pytest.mark.asyncio
    async def test_fleet_removes_empty_state_before_first_worker(self, ui_client):
        client, _ = ui_client
        html = (await client.get("/fleet")).text

        assert "grid.querySelector('.empty-state')?.remove()" in html

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
