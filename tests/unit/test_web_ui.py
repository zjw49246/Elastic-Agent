"""Tests for Web UI (T-029)."""

from __future__ import annotations

import os
import re
from unittest.mock import patch

import pytest
from httpx import ASGITransport, AsyncClient

from elastic_agent.testing import create_test_manager


def _between(text: str, start: str, end: str) -> str:
    return text[text.index(start) : text.index(end)]


def _javascript_function(html: str, name: str) -> str:
    match = re.search(
        rf"(?:async\s+)?function\s+{re.escape(name)}\([^)]*\)\s*\{{"
        r".*?(?=\n(?:async\s+)?function\s+|\n// ----|\n\n"
        r"document\.|\n</script>)",
        html,
        re.DOTALL,
    )
    assert match is not None, f"missing JavaScript function {name}"
    return match.group(0)


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
    async def test_submit_job_form_has_ordered_sections_and_all_control_ids(
        self, ui_client
    ):
        client, _ = ui_client
        html = (await client.get("/batch")).text
        job_form = _between(
            html,
            "<!-- Job submission -->",
            "<!-- Jobs monitor -->",
        )

        section_names = (
            "basics",
            "compute",
            "source",
            "account",
            "run",
            "results",
            "rotation",
            "advanced",
        )
        section_positions = [
            job_form.index(f'data-job-section="{name}"')
            for name in section_names
        ]
        assert section_positions == sorted(section_positions)

        representative_controls = {
            "basics": ("jName", "jProfile"),
            "compute": ("jWorkers", "jInstanceType", "jDiskGb"),
            "source": ("jRepo", "jDeliver", "jSetup", "jS3"),
            "account": ("jAcctMode", "jAgentType", "jAcctBinding", "jAcctIds"),
            "run": ("jRun", "jCwd", "jRunTimeout", "jTtl"),
            "results": ("jCollect", "jCollectInterval"),
            "rotation": ("jRot", "jResume", "jMaxRotations"),
            "advanced": ("hFile", "hClass", "hCode", "jHarnessRef"),
        }
        for index, name in enumerate(section_names):
            end = (
                section_positions[index + 1]
                if index + 1 < len(section_positions)
                else len(job_form)
            )
            section = job_form[section_positions[index] : end]
            assert "<legend" in section
            for control_id in representative_controls[name]:
                assert f'id="{control_id}"' in section

        expected_ids = {
            "jName",
            "jWorkers",
            "jProfile",
            "jNamePrefix",
            "jInstanceType",
            "jRegion",
            "jDiskGb",
            "jRepo",
            "jRepoRef",
            "jResolvedCommit",
            "jTargetDir",
            "jDeliver",
            "jSetup",
            "jSetupSteps",
            "jNeedsDocker",
            "jS3",
            "jRun",
            "jCwd",
            "jShard",
            "jShell",
            "jRunTimeout",
            "jTtl",
            "jEnv",
            "jSecretEnv",
            "jCollect",
            "jCollectInterval",
            "jAcctMode",
            "jAgentType",
            "jAcctGroup",
            "jAgentModel",
            "jConfigDir",
            "jPerWorker",
            "jLoginTimeout",
            "jAcctBinding",
            "jAcctIds",
            "jEipHint",
            "jRot",
            "jResume",
            "jMaxRotations",
            "jSpot",
            "hFile",
            "hClass",
            "hCode",
            "jHarnessRef",
            "jPlanBtn",
            "jSubmitBtn",
            "jPlanOutput",
        }
        for control_id in expected_ids:
            assert job_form.count(f'id="{control_id}"') == 1, control_id

    @pytest.mark.asyncio
    async def test_submit_job_form_preserves_build_job_spec_semantics(
        self, ui_client
    ):
        client, _ = ui_client
        html = (await client.get("/batch")).text
        build_spec = _javascript_function(html, "buildJobSpec")

        expected_direct_controls = {
            "jHarnessRef",
            "jWorkers",
            "jAcctBinding",
            "jAcctIds",
            "jRepo",
            "jTargetDir",
            "jDeliver",
            "jNeedsDocker",
            "jRepoRef",
            "jResolvedCommit",
            "jName",
            "jProfile",
            "jRun",
            "jCwd",
            "jRunTimeout",
            "jShell",
            "jTtl",
            "jAcctMode",
            "jAgentType",
            "jAgentModel",
            "jAcctGroup",
            "jPerWorker",
            "jConfigDir",
            "jLoginTimeout",
            "jRot",
            "jResume",
            "jMaxRotations",
            "jShard",
            "jNamePrefix",
            "jInstanceType",
            "jRegion",
            "jDiskGb",
            "jSpot",
            "jCollectInterval",
        }
        for control_id in expected_direct_controls:
            assert f"document.getElementById('{control_id}')" in build_spec

        for line_control in ("jSetup", "jS3", "jCollect"):
            assert f"lines('{line_control}')" in build_spec
        assert "steps: parseSetupSteps()" in build_spec
        assert "env: buildEnv()" in build_spec
        assert "secret_env: buildSecretEnv()" in build_spec
        assert "const repo =" in build_spec and "|| null" in build_spec
        assert "if (repo)" in build_spec
        assert "setup.ref =" in build_spec
        assert "setup.resolved_commit =" in build_spec
        assert "accountBinding === 'eip'" in build_spec
        assert ".selectedOptions" in build_spec
        assert "accountIds.length !== workers" in build_spec
        assert "const accountEnabled = accountMode !== 'none'" in build_spec
        assert "accountMode === 'worker_local_login'" in build_spec
        assert "model: accountEnabled ?" in build_spec
        assert "config_dir: accountEnabled" in build_spec
        assert "const rotationStrategy = accountEnabled" in build_spec
        assert "const rotationEnabled =" in build_spec
        assert "resume_args: rotationEnabled" in build_spec
        assert "max_rotations: rotationEnabled" in build_spec
        assert ".map(function(l)" in build_spec
        assert ".filter(function(d)" in build_spec
        assert "=== 'true'" in build_spec
        assert "if (ref) spec.harness_ref = ref" in build_spec

        assert "function buildEnv() { return buildKeyValueLines('jEnv'); }" in html
        assert (
            "function buildSecretEnv() { return buildKeyValueLines('jSecretEnv'); }"
            in html
        )
        assert "document.getElementById('jSetupSteps')" in _javascript_function(
            html, "parseSetupSteps"
        )

    @pytest.mark.asyncio
    async def test_submit_job_form_has_accessible_labels_and_contextual_help(
        self, ui_client
    ):
        client, _ = ui_client
        html = (await client.get("/batch")).text
        job_form = _between(
            html,
            "<!-- Job submission -->",
            "<!-- Jobs monitor -->",
        )

        assert job_form.count("<fieldset") >= 8
        assert job_form.count("<legend") >= 8

        controls_with_context = (
            "jWorkers",
            "jProfile",
            "jDiskGb",
            "jRepo",
            "jRun",
            "jRunTimeout",
            "jTtl",
            "jCollect",
            "jCollectInterval",
            "jAcctMode",
            "jPerWorker",
            "jAcctBinding",
            "jAcctIds",
            "jRot",
            "jResume",
        )
        for control_id in controls_with_context:
            assert re.search(
                rf'<label\b[^>]*\bfor="{re.escape(control_id)}"',
                job_form,
            ), control_id
            control = re.search(
                rf"<(?:input|select|textarea)\b"
                rf"(?=[^>]*\bid=\"{re.escape(control_id)}\")"
                r"(?=[^>]*\baria-describedby=\"([^\"]+)\")[^>]*>",
                job_form,
                re.DOTALL,
            )
            assert control is not None, control_id
            for help_id in control.group(1).split():
                assert job_form.count(f'id="{help_id}"') == 1, (
                    control_id,
                    help_id,
                )

        assert re.search(
            r'<input\b(?=[^>]*\bid="jWorkers")(?=[^>]*\btype="number")'
            r'(?=[^>]*\bmin="1")[^>]*>',
            job_form,
        )
        assert re.search(
            r'<input\b(?=[^>]*\bid="jLoginTimeout")(?=[^>]*\bmin="60")'
            r'(?=[^>]*\bmax="1200")[^>]*>',
            job_form,
        )
        assert re.search(
            r'<select\b(?=[^>]*\bid="jAcctIds")(?=[^>]*\bmultiple\b)[^>]*>',
            job_form,
        )

    @pytest.mark.asyncio
    async def test_submit_job_dynamic_states_and_mobile_actions_are_clear(
        self, ui_client
    ):
        client, _ = ui_client
        html = (await client.get("/batch")).text
        job_form = _between(
            html,
            "<!-- Job submission -->",
            "<!-- Jobs monitor -->",
        )
        style = _between(html, "<style>", "</style>")

        disabled_rules = [
            (selector, declarations)
            for selector, declarations in re.findall(
                r"([^{}]+)\{([^{}]*)\}",
                style,
            )
            if all(
                token in selector
                for token in (
                    "input:disabled",
                    "select:disabled",
                    "textarea:disabled",
                )
            )
        ]
        assert disabled_rules
        disabled_declarations = disabled_rules[0][1]
        assert "cursor:not-allowed" in disabled_declarations.replace(" ", "")
        assert (
            "opacity:" in disabled_declarations
            or "background:" in disabled_declarations
        )

        assert 'id="jAcctMode" onchange="updateAccountModeUI()"' in job_form
        account_mode = _javascript_function(html, "updateAccountModeUI")
        assert "binding.disabled = !workerLocal" in account_mode
        assert "if (!workerLocal) binding.value = 'none'" in account_mode
        assert "rotation.disabled = accountDisabled" in account_mode
        assert "if (accountDisabled) rotation.value = 'none'" in account_mode
        assert "updateEipBindingUI()" in account_mode

        assert 'id="jRot" onchange="updateRotationUI()"' in job_form
        rotation = _javascript_function(html, "updateRotationUI")
        assert "document.getElementById('jRot')" in rotation
        assert "document.getElementById('jResume')" in rotation
        assert "document.getElementById('jMaxRotations')" in rotation
        assert "document.getElementById('jAcctMode')" in rotation
        assert "on_exhaust_restart_resume" in rotation
        assert rotation.count(".disabled") >= 2
        assert html.count("updateRotationUI()") >= 2

        assert 'class="form-actions"' in job_form
        assert ">仅校验并查看计划</button>" in job_form
        assert ">校验并启动 Job</button>" in job_form
        mobile = style[style.index("@media (max-width:800px)") :]
        assert ".form-actions" in mobile
        assert re.search(
            r"\.form-actions\s*\{[^}]*"
            r"(?:flex-direction\s*:\s*column|display\s*:\s*grid)",
            mobile,
            re.DOTALL,
        )
        assert re.search(
            r"\.form-actions\s+\.btn\s*\{[^}]*width\s*:\s*100%",
            mobile,
            re.DOTALL,
        )

    @pytest.mark.asyncio
    async def test_submit_job_validates_visible_inputs_before_preflight(
        self, ui_client
    ):
        client, _ = ui_client
        html = (await client.get("/batch")).text
        validation = _javascript_function(html, "validateJobForm")

        for control_id in ("jRun", "jSetupSteps", "jTtl", "jRunTimeout", "jS3"):
            assert f"document.getElementById('{control_id}')" in validation
        assert "run.value.trim()" in validation
        assert validation.count(".setCustomValidity(") >= 3
        assert "parseSetupSteps()" in validation
        assert "error instanceof SyntaxError" in validation
        assert "Number(ttl.value) < Number(runTimeout.value)" in validation
        assert "parts[0].startsWith('s3://')" in validation
        assert "control.closest('details')" in validation
        assert "details.open = true" in validation
        assert "control.reportValidity()" in validation

        for function_name in ("previewJob", "submitJob"):
            submit = _javascript_function(html, function_name)
            assert "if (!validateJobForm())" in submit
            assert submit.index("validateJobForm()") < submit.index("api('POST'")

    @pytest.mark.asyncio
    async def test_agent_api_provider_behavior_is_scoped_to_form_functions(
        self, ui_client
    ):
        client, _ = ui_client
        html = (await client.get("/batch")).text

        provider_meta = _javascript_function(html, "agentApiProviderMeta")
        apex_meta = provider_meta[
            provider_meta.index("id: 'apex'") : provider_meta.index(
                "id: 'cloudrouter'"
            )
        ]
        cloud_meta = provider_meta[provider_meta.index("id: 'cloudrouter'") :]
        assert "ApexRouter" in apex_meta
        assert "仅支持 Codex" in apex_meta
        assert "CloudRouter" in cloud_meta
        assert "Claude、Codex" in cloud_meta

        provider_ui = _javascript_function(html, "updateAgentApiProviderUI")
        for control_id in (
            "apiAcctProvider",
            "apiAcctHint",
            "apiAcctKey",
            "apiAcctAdd",
        ):
            assert f"document.getElementById('{control_id}')" in provider_ui
        assert ".placeholder =" in provider_ui
        assert ".textContent =" in provider_ui

        add_account = _javascript_function(html, "addAgentApiAccount")
        assert "provider: provider" in add_account
        assert "api_key: apiKey" in add_account
        assert "document.getElementById('apiAcctKey').value = ''" in add_account
        assert "document.getElementById('apiAcctName').value = ''" in add_account

        refresh_accounts = _javascript_function(html, "refreshAccounts")
        assert "agentApiProviderMeta(a.api_provider).pickerLabel" in refresh_accounts
        assert "option.dataset.agentTypes = supported.join(',')" in refresh_accounts
        assert "option.disabled = !enabled || !supported.includes(selectedAgent)" in (
            refresh_accounts
        )
        assert "option.selected = selected.has(a.id) && !option.disabled" in (
            refresh_accounts
        )

        update_agent = _javascript_function(html, "updateAgentUI")
        assert "option.dataset.agentTypes.split(',').includes(agentType)" in (
            update_agent
        )
        assert "if (option.disabled) option.selected = false" in update_agent

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
        assert 'id="acctPassword" type="password" autocomplete="new-password"' in html
        assert 'id="acctClearPassword"' in html
        assert 'id="acctToken" type="password" autocomplete="new-password"' in html
        assert 'id="acctClearToken"' in html
        assert 'id="jAgentType"' in html
        assert 'id="jAgentModel"' in html
        assert "document.getElementById('jAgentModel').value.trim()" in html
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
        assert re.search(
            r'<textarea\b(?=[^>]*\bid="jCollect")'
            r'(?=[^>]*\bplaceholder="results")[^>]*>results</textarea>',
            html,
        )
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
    async def test_batch_console_exposes_selectable_agent_api_providers(
        self, ui_client
    ):
        client, _ = ui_client
        html = (await client.get("/batch")).text

        assert "Agent API accounts" in html
        assert 'id="apiAcctProvider"' in html
        assert 'value="cloudrouter"' in html
        assert 'value="apex"' in html
        assert 'id="apiAcctProvider" onchange="updateAgentApiProviderUI()"' in html
        assert 'id="apiAcctName"' in html
        assert 'id="apiAcctGroup"' in html
        assert 'id="apiAcctKey" type="password"' in html
        assert "function agentApiProviderMeta(provider)" in html
        assert "function updateAgentApiProviderUI()" in html
        assert "async function addAgentApiAccount()" in html
        assert "provider: provider" in html
        assert "ApexRouter" in html
        assert "仅支持 Codex" in html
        assert "api('POST', '/agent-api/accounts'" in html
        assert "document.getElementById('apiAcctKey').value = ''" in html
        assert "sessionStorage.setItem('apiAcctKey'" not in html
        assert "localStorage" not in html

    @pytest.mark.asyncio
    async def test_batch_console_projects_agent_api_accounts_into_jobs(
        self, ui_client
    ):
        client, _ = ui_client
        html = (await client.get("/batch")).text

        assert "a.auth_kind === 'agent_api'" in html
        assert "a.supported_agent_types" in html
        assert "a.supported_models" in html
        assert "a.api_usage" in html
        assert "CloudRouter · API" in html
        assert "ApexRouter · API" in html
        assert "agentApiProviderLabel(a.api_provider)" in html
        assert "function accountSupportedAgentTypes(a)" in html
        assert "option.dataset.agentTypes = supported.join(',')" in html
        assert "option.dataset.agentTypes.split(',').includes(agentType)" in html
        assert "async function refreshAgentApiAccount(id)" in html
        assert "'/agent-api/accounts/' + encodeURIComponent(id) + '/refresh'" in html

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
    async def test_batch_console_keeps_failed_logs_and_download_action_stable(
        self, ui_client
    ):
        client, _ = ui_client
        html = (await client.get("/batch")).text

        assert "const jobResultsRequestVersions = new Map()" in html
        assert "function commitJobResult" in html
        assert "requestVersion !== jobResultsRequestVersions.get(jobId)" in html
        assert "knownFileCount > 0 && incomingFileCount <= 0" in html
        assert "nextResultCheck(job, incomingFileCount, previous)" in html
        assert "function jobResultActionHtml(job, result)" in html
        assert 'data-result-action="' in html
        assert "📄 查看失败日志" in html
        assert "function jobLogLineLimit(jobId, workerId)" in html
        assert "return terminal ? 5_000 : 1_000" in html
        assert "formatTaskExitSummary(data.tasks || [])" in html

    @pytest.mark.asyncio
    async def test_batch_console_streams_large_downloads_with_progress_and_cancel(
        self, ui_client
    ):
        client, _ = ui_client
        html = (await client.get("/batch")).text

        assert "/results/download/stream" in html
        assert "new AbortController()" in html
        assert "response.body.getReader()" in html
        assert "showSaveFilePicker" in html
        assert "cancelResultDownload" in html
        assert "formatResultDownloadLabel" in html
        assert "已接收" in html
        assert "点此取消" in html
        assert "当前为运行中已上传的中间结果快照" in html
        assert "Math.max(state.sourceBytes, state.total)" in html
        assert "请使用 HTTPS 下的桌面版 Chrome 重试" in html
        assert "await state.reader.cancel()" in html
        assert "data-result-download-job" in html

        start_transition = html[
            html.index("resultDownloadsInFlight.set(jobId, state)"):
            html.index("state.timer = setInterval")
        ]
        assert "reconcileJobCards(visibleJobs(latestJobs))" in start_transition
        assert "refreshResults()" in start_transition

        repaint = html[
            html.index("function repaintResultDownload"):
            html.index("function cancelResultDownload")
        ]
        assert "reconcileJobCards" not in repaint
        assert "refreshResults" not in repaint

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
