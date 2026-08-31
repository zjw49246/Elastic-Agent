"""Tests for Web UI (T-029)."""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

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


def _run_node_json(source: str) -> object:
    node = shutil.which("node")
    if node is None:
        pytest.skip("node is required for the inline JavaScript behavior test")
    completed = subprocess.run(
        [node, "-e", source],
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
    )
    return json.loads(completed.stdout)


@pytest.fixture
async def ui_client():
    result = create_test_manager()
    from elastic_agent.api.app import create_app
    app = create_app(result.manager)
    principal = SimpleNamespace(
        subject="admin@example.test",
        must_change_password=False,
    )
    with (
        patch.dict(os.environ, {"ELASTIC_AGENT_EXTERNAL_API_KEYS": "test-key"}),
        patch(
            "elastic_agent.api.routes.ui.get_session_principal",
            new=AsyncMock(return_value=principal),
        ),
    ):
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="https://testserver",
        ) as client:
            yield client, result


@pytest.fixture
async def anonymous_ui_client():
    result = create_test_manager()
    from elastic_agent.api.app import create_app

    app = create_app(result.manager)
    with patch(
        "elastic_agent.api.routes.ui.get_session_principal",
        new=AsyncMock(return_value=None),
    ):
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="https://testserver",
            follow_redirects=False,
        ) as client:
            yield client, result


class TestDashboardEndpoint:
    @pytest.mark.asyncio
    async def test_root_redirects_to_current_ui_v2(self, ui_client):
        client, _ = ui_client
        resp = await client.get("/")
        assert resp.status_code == 303
        assert resp.headers["location"] == "/ui-v2/"

    @pytest.mark.asyncio
    async def test_legacy_batch_console_remains_at_explicit_path(self, ui_client):
        client, _ = ui_client
        resp = await client.get("/batch")
        assert resp.status_code == 200
        assert "Batch Console" in resp.text
        assert "Submit Job" in resp.text

    @pytest.mark.asyncio
    async def test_legacy_batch_console_uses_500_job_policy_limit(self, ui_client):
        client, _ = ui_client
        html = (await client.get("/batch")).text

        assert "max_active_jobs</code>（1–500）" in html
        assert "manifest.policy.max_active_jobs > 500" in html
        assert "policy.max_active_jobs 必须是 1–500 的整数" in html

    @pytest.mark.asyncio
    async def test_login_page_uses_account_credentials_without_defaults(
        self, anonymous_ui_client
    ):
        client, _ = anonymous_ui_client
        resp = await client.get("/login?next=%2Ffleet")

        assert resp.status_code == 200
        assert resp.headers["cache-control"] == "no-store"
        assert resp.headers["referrer-policy"] == "same-origin"
        assert "frame-ancestors 'none'" in resp.headers["content-security-policy"]
        assert resp.headers["x-frame-options"] == "DENY"
        assert 'id="email"' in resp.text
        assert 'id="password"' in resp.text
        assert '<form id="loginForm" method="post" action="/login">' in resp.text
        assert 'name="next" type="hidden" value="/fleet"' in resp.text
        assert "'/api/auth/login'" not in resp.text
        assert "event.preventDefault()" not in resp.text
        assert "prefilled-admin@example.test" not in resp.text
        assert "prefilled-test-password" not in resp.text
        assert "Authorization" not in resp.text
        assert "Bearer" not in resp.text

    @pytest.mark.asyncio
    async def test_authenticated_login_redirect_rejects_external_next(
        self, ui_client
    ):
        client, _ = ui_client
        resp = await client.get(
            "/login?next=https%3A%2F%2Fevil.example",
            follow_redirects=False,
        )

        assert resp.status_code == 303
        assert resp.headers["location"] == "/ui-v2/overview"

    @pytest.mark.asyncio
    async def test_change_password_page_uses_csrf_and_safe_next(self, ui_client):
        client, _ = ui_client
        resp = await client.get("/change-password?next=%2Fbatch")

        assert resp.status_code == 200
        assert resp.headers["cache-control"] == "no-store"
        assert "'/api/auth/me'" in resp.text
        assert "'/api/auth/password'" in resp.text
        assert "current_password:current.value" in resp.text
        assert "new_password:next.value" in resp.text
        assert "requestHeaders.set('X-CSRF-Token', csrfToken)" in resp.text
        assert "credentials:'same-origin'" in resp.text
        assert "PASSWORD_NEXT_PATHS" in resp.text
        assert "prefilled-admin@example.test" not in resp.text
        assert "prefilled-test-password" not in resp.text

    @pytest.mark.asyncio
    async def test_anonymous_change_password_redirects_to_login(
        self, anonymous_ui_client
    ):
        client, _ = anonymous_ui_client
        resp = await client.get("/change-password")

        assert resp.status_code == 303
        assert resp.headers["location"] == "/login?next=%2Fchange-password"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("path", ["/batch", "/fleet"])
    async def test_console_uses_cookie_csrf_authentication(self, ui_client, path):
        client, _ = ui_client
        html = (await client.get(path)).text

        assert "async function authenticatedFetch" in html
        assert "'/api/auth/me'" in html
        assert "'/api/auth/logout'" in html
        assert "credentials:'same-origin'" in html
        assert "requestHeaders.set('X-CSRF-Token', csrfToken)" in html
        assert 'id="currentUserEmail"' in html
        assert "退出登录" in html
        assert "sessionStorage.removeItem('ea_api_key')" in html
        assert "sessionStorage.getItem('ea_api_key')" not in html
        assert "sessionStorage.setItem('ea_api_key'" not in html
        assert "Authorization" not in html
        assert "Bearer" not in html
        assert "请输入 API Key" not in html

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
            "run": (
                "jRun",
                "jRunResumeCommand",
                "jCwd",
                "jRunTimeout",
                "jTtl",
            ),
            "results": (
                "jCollect",
                "jCollectInterval",
                "jCollectCheckpoint",
                "jRecoveryPolicy",
                "jRecoveryJob",
            ),
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
            "jRunResumeCommand",
            "jAi4SciRecoveryPreset",
            "jCwd",
            "jShard",
            "jShell",
            "jRunTimeout",
            "jTtl",
            "jEnv",
            "jSecretEnv",
            "jCollect",
            "jCollectInterval",
            "jCollectCheckpoint",
            "jCollectExclude",
            "jRecoveryPolicy",
            "jRecoveryJob",
            "jRecoveryPaths",
            "jRecoveryGeneration",
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
            "jRepo",
            "jTargetDir",
            "jDeliver",
            "jNeedsDocker",
            "jRepoRef",
            "jResolvedCommit",
            "jName",
            "jProfile",
            "jRun",
            "jRunResumeCommand",
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
            "jCollectCheckpoint",
            "jRecoveryPolicy",
            "jRecoveryJob",
            "jRecoveryGeneration",
        }
        for control_id in expected_direct_controls:
            assert f"document.getElementById('{control_id}')" in build_spec

        for line_control in (
            "jSetup",
            "jCollect",
            "jCollectExclude",
            "jRecoveryPaths",
        ):
            assert f"lines('{line_control}')" in build_spec
        assert "lines('jS3')" in _javascript_function(html, "parseS3Datasets")
        assert "steps: parseSetupSteps()" in build_spec
        assert "env: buildEnv()" in build_spec
        assert "secret_env: buildSecretEnv()" in build_spec
        assert "const repo =" in build_spec and "|| null" in build_spec
        assert "if (repo)" in build_spec
        assert "setup.ref =" in build_spec
        assert "setup.resolved_commit =" in build_spec
        assert "buildSelectedAccountIds(" in build_spec
        selected_accounts = _javascript_function(
            html, "buildSelectedAccountIds"
        )
        assert "document.getElementById('jAcctIds')" in selected_accounts
        assert ".selectedOptions" in selected_accounts
        assert "selected.length === required" in selected_accounts
        assert "const accountEnabled = accountMode !== 'none'" in build_spec
        assert "accountMode === 'worker_local_login'" in build_spec
        assert "model: accountEnabled ?" in build_spec
        assert "config_dir: accountEnabled" in build_spec
        assert "const rotationStrategy = accountEnabled" in build_spec
        assert "const rotationEnabled =" in build_spec
        assert "resume_args: rotationEnabled" in build_spec
        assert "max_rotations: rotationEnabled" in build_spec
        assert "const recoveryEnabled =" in build_spec
        assert "resume_command:" in build_spec
        assert "document.getElementById('jRunResumeCommand')" in build_spec
        assert "source_job_id: recoveryEnabled" in build_spec
        assert "generation: recoveryPolicy === 'checkpoint'" in build_spec
        assert "旧 Job 的最终收集（兼容模式）" not in html
        assert "s3_datasets: parseS3Datasets()" in build_spec
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
    async def test_env_lines_are_strictly_validated_before_submit(
        self, ui_client,
    ):
        client, _ = ui_client
        html = (await client.get("/batch")).text
        parser = _javascript_function(html, "buildKeyValueLines")
        validation = _javascript_function(html, "validateJobForm")

        assert r"const rawLines = control.value.split('\n')" in parser
        assert "const env = Object.create(null)" in parser
        assert "/^[A-Za-z_][A-Za-z0-9_]*$/" in parser
        assert "Object.prototype.hasOwnProperty.call(env, key)" in parser
        assert "必须是 KEY=VALUE" in parser
        assert "重复定义" in parser
        assert "for (const id of ['jEnv', 'jSecretEnv'])" in validation
        assert "buildKeyValueLines(id)" in validation
        assert "control.setCustomValidity(error.message)" in validation
        assert "const details = control.closest('details')" in validation
        assert "details.open = true" in validation
        assert "control.reportValidity()" in validation
        assert "control.focus()" in validation

        cases = [
            {
                "id": "jEnv",
                "value": "FOO=one\nEMPTY=\n_UNDER=two=three\n__proto__=kept",
            },
            {"id": "jEnv", "value": "GOOD=1\nBROKEN"},
            {"id": "jEnv", "value": "BAD-NAME=x"},
            {"id": "jEnv", "value": "=x"},
            {"id": "jEnv", "value": "DUP=one\nDUP=two"},
            {"id": "jSecretEnv", "value": "SECRET_WITHOUT_EQUALS"},
        ]
        results = _run_node_json(
            """
const controls = {jEnv:{value:''}, jSecretEnv:{value:''}};
global.document = {getElementById:(id) => controls[id]};
"""
            + parser
            + "\nconst cases = "
            + json.dumps(cases, ensure_ascii=False)
            + """;
const results = cases.map((item) => {
  controls[item.id].value = item.value;
  try {
    return {ok:true, value:buildKeyValueLines(item.id)};
  } catch (error) {
    return {ok:false, error:String(error.message || error)};
  }
});
process.stdout.write(JSON.stringify(results));
"""
        )

        assert results[0] == {
            "ok": True,
            "value": {
                "FOO": "one",
                "EMPTY": "",
                "_UNDER": "two=three",
                "__proto__": "kept",
            },
        }
        assert results[1]["ok"] is False
        assert "第 2 行" in results[1]["error"]
        assert "KEY=VALUE" in results[1]["error"]
        assert results[2]["ok"] is False
        assert "变量名 BAD-NAME 无效" in results[2]["error"]
        assert results[3]["ok"] is False
        assert "第 1 行必须是 KEY=VALUE" in results[3]["error"]
        assert results[4]["ok"] is False
        assert "变量 DUP 重复定义" in results[4]["error"]
        assert results[5]["ok"] is False
        assert results[5]["error"].startswith("秘密环境变量")

    @pytest.mark.asyncio
    async def test_ai4sci_bench_uses_requested_archived_branch_by_default(
        self, ui_client,
    ):
        client, _ = ui_client
        html = (await client.get("/batch")).text
        archived_ref = "archive/youchengsong-managed-agent-api-20260728"

        assert f'id="jRepoRef" value="{archived_ref}"' in html
        assert "AI4Sci Bench 默认使用已锁定的归档分支" in html
        assert "其他仓库请改成实际分支或标签" in html

    @pytest.mark.asyncio
    async def test_ai4sci_recovery_preset_uses_stable_output_and_resume_command(
        self, ui_client,
    ):
        client, _ = ui_client
        html = (await client.get("/batch")).text
        suggestion = _javascript_function(html, "ai4sciResumeCommand")
        preset = _javascript_function(html, "applyAi4SciRecoveryPreset")

        results = _run_node_json(
            suggestion
            + """
const commands = [
  'uv run ai4sci-bench run --output-dir "results/shard-{{shard_id}}"',
  'uv run ai4sci-bench run --output-dir results/shard-{{shard_index}} --resume',
  'uv run ai4sci-bench batch-run --agent codex_cli:{} --output-dir results/batch',
  'uv run ai4sci-bench codex-run --output-dir results/codex',
  'uv run ai4sci-bench codex-replay-run --output-dir results/replay --replay-resume',
  'uv run ai4sci-bench run --output-dir "results/{{hostname}}"',
  'uv run ai4sci-bench list --output-dir results/list',
  'python other.py --output-dir results/x',
];
process.stdout.write(JSON.stringify(commands.map(ai4sciResumeCommand)));
"""
        )

        assert results == [
            (
                'uv run ai4sci-bench run --output-dir '
                '"results/shard-{{shard_id}}" '
                '--resume "results/shard-{{shard_id}}"'
            ),
            (
                "uv run ai4sci-bench run --output-dir "
                "results/shard-{{shard_index}} "
                "--resume results/shard-{{shard_index}}"
            ),
            (
                "uv run ai4sci-bench batch-run --agent codex_cli:{} "
                "--output-dir results/batch --resume results/batch"
            ),
            (
                "uv run ai4sci-bench codex-run --output-dir results/codex "
                "--resume results/codex"
            ),
            (
                "uv run ai4sci-bench codex-replay-run "
                "--output-dir results/replay --replay-resume "
                "--resume results/replay"
            ),
            "",
            "",
            "",
        ]
        assert "document.getElementById('jShard').value = 'shard_index'" in preset
        assert (
            "document.getElementById('jCollectCheckpoint').value = 'true'"
            in preset
        )
        assert (
            "document.getElementById('jCollectInterval').value = '120'"
            in preset
        )
        assert "paths.includes('results')" in preset
        assert "updateResumeCommandSuggestion()" in preset
        assert "updateCollectUI()" in preset
        assert "应用 AI4Sci 长任务可恢复预设" in html

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
            "jRunResumeCommand",
            "jRunTimeout",
            "jTtl",
            "jCollect",
            "jCollectInterval",
            "jCollectCheckpoint",
            "jCollectExclude",
            "jRecoveryPolicy",
            "jRecoveryJob",
            "jRecoveryPaths",
            "jRecoveryGeneration",
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

        for control_id in (
            "jRun",
            "jSetupSteps",
            "jTtl",
            "jRunTimeout",
            "jS3",
        ):
            assert f"document.getElementById('{control_id}')" in validation
        assert "run.value.trim()" in validation
        assert validation.count(".setCustomValidity(") >= 3
        assert "parseSetupSteps()" in validation
        assert "error instanceof SyntaxError" in validation
        assert "Number(ttl.value) < Number(runTimeout.value)" in validation
        assert "parseS3Datasets()" in validation
        assert "checkpoint && !resumeCommand.value.trim()" not in validation
        assert "run.resume_command" in html
        assert "仍可手动恢复检查点" in html
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

        refresh_accounts = _javascript_function(html, "refreshAccountsOnce")
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
        assert "指定账号数必须等于 Worker 数" in html
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
        assert 'value="manager_distribute"' not in html
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
    async def test_batch_console_decommissions_bound_eip_before_account_delete(
        self, ui_client
    ):
        client, _ = ui_client
        html = (await client.get("/batch")).text
        remove_account = _javascript_function(html, "removeAccount")

        binding_get = "await api('GET', accountPath + '/binding')"
        decommission = (
            "await api('POST', accountPath + '/binding/decommission',"
        )
        identity_delete = "await api('DELETE', accountPath)"

        assert binding_get in remove_account
        assert "if (e.status === 404) binding = null" in remove_account
        assert "永久释放" in remove_account
        assert (
            "+ '\\n失败 Job 结束后 EIP 仍会按设计保留"
            in remove_account
        )
        assert "window.confirm" in remove_account
        assert "window.prompt" in remove_account
        assert "confirmation.trim() !== id" in remove_account
        assert "release_eip: true" in remove_account
        assert "confirm_account_id: id" in remove_account
        assert "delete_identity: true" in remove_account
        assert "retired.identity_removed === true" in remove_account
        assert "if (!identityRemoved)" in remove_account
        assert "bindingReleaseIsVisible(accountPath)" in remove_account
        assert "账号删除状态需刷新确认" in remove_account
        assert decommission in remove_account
        assert identity_delete in remove_account
        assert remove_account.index(binding_get) < remove_account.index(decommission)
        assert remove_account.index(decommission) < remove_account.index(
            identity_delete
        )
        assert "仍有任务或清理流程占用" in remove_account

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
        assert "async function removeAgentApiAccount(id)" in html
        assert "api('DELETE', '/agent-api/accounts/'" in html
        assert "仍有活动任务或清理流程占用" in html
        assert "x.cleanup_pending?'·清理中':''" in html
        assert "占用状态暂不可用" in html
        assert "EIP 状态暂不可用" in html
        assert "EIP状态未知" in html

    @pytest.mark.asyncio
    async def test_non_eip_account_picker_and_shared_api_mapping_are_explicit(
        self, ui_client,
    ):
        client, _ = ui_client
        html = (await client.get("/batch")).text
        update_binding = _javascript_function(html, "updateEipBindingUI")
        mapping = _javascript_function(html, "buildSelectedAccountIds")

        assert "picker.disabled = accountDisabled" in update_binding
        assert "option.dataset.authKind = a.auth_kind" in html
        assert "selected.length === 1" in mapping
        assert "authKind === 'agent_api'" in mapping
        assert "Array(required).fill" in mapping
        assert "selected.length === required" in mapping
        assert "所选唯一账号按列表顺序映射" in html
        assert "任意排序或重复映射请直接提交 JobSpec" in html

    @pytest.mark.asyncio
    async def test_s3_dataset_parser_supports_spaced_templates_and_destinations(
        self, ui_client,
    ):
        client, _ = ui_client
        html = (await client.get("/batch")).text
        parser = _javascript_function(html, "parseS3DatasetLine")
        datasets = _javascript_function(html, "parseS3Datasets")

        assert "inTemplate" in parser
        assert "line.startsWith('{{', index)" in parser
        assert "line.startsWith('}}', index)" in parser
        assert "uri.startsWith('s3://')" in parser
        assert "dest = line.slice(index).trim()" in parser
        assert "lines('jS3').map(parseS3DatasetLine)" in datasets

    @pytest.mark.asyncio
    async def test_pending_job_idempotency_survives_page_refresh(
        self, ui_client,
    ):
        client, _ = ui_client
        html = (await client.get("/batch")).text
        submit = _javascript_function(html, "submitJob")

        assert "ea_pending_job_submission" in html
        assert "sessionStorage.getItem" in html
        assert "sessionStorage.setItem" in html
        assert "sessionStorage.removeItem" in html
        assert "pending.spec === currentSerialized" in submit
        assert "parsePendingJobSpec(pending)" in submit
        assert "retryOriginal = window.confirm" in submit
        assert "discardPending = window.confirm" in submit
        assert "clearPendingJobSubmission()" in submit
        assert "if (!retryPending)" in submit
        assert submit.index("pending.spec === currentSerialized") < submit.index(
            "api('POST', '/jobs/plan'"
        )
        assert submit.index("clearPendingJobSubmission()") < submit.index(
            "api('POST', '/jobs/plan'"
        )
        retry_branch = submit[
            submit.index("if (retryPending)")
            :submit.index("} else {", submit.index("if (retryPending)"))
        ]
        new_submission_setup = submit[
            submit.index("if (!retryPending)")
            :submit.index("if (retryPending)")
        ]
        assert "await providerDefaultsReady" not in retry_branch
        assert "await providerDefaultsReady" in new_submission_setup
        assert "spec: currentSerialized" in submit

    @pytest.mark.asyncio
    async def test_checkpoint_recovery_keeps_ambiguous_idempotency_key(
        self, ui_client,
    ):
        client, _ = ui_client
        html = (await client.get("/batch")).text
        classifier = _javascript_function(
            html,
            "recoverySubmissionDefinitivelyRejected",
        )
        recover = _javascript_function(html, "recoverJob")

        classified = _run_node_json(
            classifier
            + """
const statuses = [0, 400, 408, 409, 422, 429, 500, 503];
process.stdout.write(JSON.stringify(Object.fromEntries(
  statuses.map(code => [
    code,
    recoverySubmissionDefinitivelyRejected(code),
  ])
)));
"""
        )
        assert classified == {
            "0": False,
            "400": False,
            "408": False,
            "409": True,
            "422": False,
            "429": False,
            "500": False,
            "503": False,
        }
        assert "明确丢弃这条待重试记录" in recover
        assert "换新 Key 可能重复创建 Worker" in recover
        assert (
            "if (recoverySubmissionDefinitivelyRejected(error?.status))"
            in recover
        )
        rejection_branch = recover[
            recover.index(
                "if (recoverySubmissionDefinitivelyRejected(error?.status))"
            ):
            recover.index("} else {", recover.index(
                "if (recoverySubmissionDefinitivelyRejected(error?.status))"
            ))
        ]
        ambiguous_branch = recover[
            recover.index("} else {", recover.index(
                "if (recoverySubmissionDefinitivelyRejected(error?.status))"
            )):
        ]
        assert "sessionStorage.removeItem(pendingKey)" in rejection_branch
        assert "sessionStorage.removeItem(pendingKey)" not in ambiguous_branch

    @pytest.mark.asyncio
    async def test_account_inputs_declare_browser_side_size_limits(
        self, ui_client,
    ):
        client, _ = ui_client
        html = (await client.get("/batch")).text
        for control_id in (
            "acctId",
            "acctEmail",
            "acctPassword",
            "acctToken",
            "acctGroup",
            "apiAcctName",
            "apiAcctGroup",
            "apiAcctKey",
            "hFile",
            "hClass",
            "hCode",
        ):
            assert re.search(
                rf'<(?:input|textarea)\b(?=[^>]*\bid="{control_id}")'
                r'(?=[^>]*\bmaxlength="\d+")[^>]*>',
                html,
                re.DOTALL,
            ), control_id

    @pytest.mark.asyncio
    async def test_batch_console_download_uses_authenticated_fetch_without_browser_key(
        self, ui_client
    ):
        client, _ = ui_client
        html = (await client.get("/batch")).text

        assert "localStorage" not in html
        assert "results/download?api_key=" not in html
        assert "sessionStorage.removeItem('ea_api_key')" in html
        assert "sessionStorage.getItem('ea_api_key')" not in html
        assert "sessionStorage.setItem('ea_api_key'" not in html
        assert "Authorization" not in html
        assert "Bearer" not in html
        assert "sessionStorage" in html
        assert "function esc(value)" in html
        assert "const response = await authenticatedFetch(" in html
        assert "credentials:'same-origin'" in html
        assert "Idempotency-Key" in html
        assert "downloadResults" in html
        assert "/cancel" in html

    @pytest.mark.asyncio
    async def test_job_monitor_exposes_verified_interrupt_and_resume_actions(
        self, ui_client,
    ):
        client, _ = ui_client
        html = (await client.get("/batch")).text
        row = _javascript_function(html, "jobRowHtml")
        interrupt = _javascript_function(html, "interruptJob")
        interrupt_key = _javascript_function(
            html,
            "interruptPendingStorageKey",
        )
        reconcile_interrupt = _javascript_function(
            html,
            "reconcileInterruptRequestKeys",
        )
        resume = _javascript_function(html, "resumeJob")
        labels = _javascript_function(html, "jobStateLabel")

        assert "j.interrupt_available === true" in row
        assert "中断并保存进度" in row
        assert "state === 'suspending'" in row
        assert "hasPendingInterruptRequest(j.job_id)" in row
        assert "重试同一次中断" in row
        assert "state !== 'suspending'" in row
        assert "j.resume_available === true" in row
        assert "Boolean(j.resume_generation)" in row
        assert "一键续跑" in row
        resume_branch = row[
            row.index("const resumeAvailable"):
            row.index("const manualRecoveryAvailable")
        ]
        assert "checkpoint_recovery_available" not in resume_branch
        assert "j.latest_checkpoint_generation" not in row[
            row.index("const resumeAvailable"):
            row.index("const manualRecoveryAvailable")
        ]
        assert "j.checkpoint_recovery_available === true" in row
        assert "['failed','cancelled','succeeded'].includes(state)" in row
        assert "手动检查点恢复" in row
        assert "j.resumed_from_job_id || j.source_job_id" in row
        assert "attempt_no" in row
        assert "续跑自" in row

        assert "'Idempotency-Key': idempotencyKey" in interrupt
        assert "/interrupt" in interrupt
        assert "interruptPendingStorageKey(jobId)" in interrupt
        assert "sessionStorage.removeItem(pendingKey)" not in interrupt
        assert "尝试发布完整检查点" in interrupt
        assert "回退到上一个完整版本" in interrupt
        assert "没有旧版本则不可续跑" in interrupt
        assert "未完成的单元会重新执行" in interrupt
        assert "refreshJobs()" in interrupt
        assert "ea_job_interrupt_pending_v1_" in interrupt_key
        assert "['suspended', 'failed'].includes(state)" in reconcile_interrupt
        assert "sessionStorage.removeItem" in reconcile_interrupt
        assert "'Idempotency-Key': pending.idempotency_key" in resume
        assert "/resume" in resume
        assert "resume_generation: pending.resume_generation" in resume
        assert "ea_suspended_resume_pending_v1_" in resume

        assert "suspending:'正在中断并保存'" in labels
        assert "suspended:'已中断，可续跑'" in labels
        assert ".b-suspending" in html
        assert ".b-suspended" in html

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
    async def test_job_cards_load_and_copy_redacted_submission_config_on_demand(
        self, ui_client,
    ):
        client, _ = ui_client
        html = (await client.get("/batch")).text
        formatter = _javascript_function(html, "jobSpecTextFromDetail")
        loader = _javascript_function(html, "loadJobSpec")
        detail_request = _javascript_function(
            html, "requestJobSpecDetail"
        )
        card = _javascript_function(html, "jobConfigHtml")

        assert "const jobSpecCache = new Map()" in html
        assert "const jobSpecRequests = new Map()" in html
        assert (
            "api('GET', '/jobs/' + encodeURIComponent(jobId))"
            in detail_request
        )
        assert "JOB_SPEC_REQUEST_CONCURRENCY = 2" in html
        assert "JOB_SPEC_CACHE_MAX_ENTRIES = 8" in html
        assert "JOB_SPEC_TEXT_MAX_CHARS" in loader
        assert "detail.spec" in formatter
        assert "JSON.stringify(spec, null, 2)" in formatter
        assert "JSON.stringify(detail" not in formatter
        assert "提交时生效配置（已脱敏）" in card
        assert "复制 JSON" in card
        assert 'data-job-config=""' in card
        assert 'class="job-config-json"' in card
        assert "copyJobSpec" in card
        assert "requestJobConfigLoad" in card
        assert "handleJobConfigToggle" in card
        assert "jobConfigLoadRequested" in (
            _javascript_function(html, "handleJobConfigToggle")
        )
        assert "handleJobCardToggle" not in (
            _javascript_function(html, "jobRowHtml")
        )
        assert "[REDACTED]" in card
        assert "[SECRET_REFERENCE]" in card
        assert "命令文本会原样显示" in card
        assert "请勿把密钥直接写进命令" in card
        assert "普通重提请使用服务端 resubmit" in card
        assert "从检查点恢复" in card
        assert "由服务器复制未回显的私有配置" in card
        assert "${cached.text}" not in card
        assert ".textContent = cached.text" in (
            _javascript_function(html, "hydrateJobConfigNode")
        )
        assert "navigator.clipboard.writeText(cached.text)" in (
            _javascript_function(html, "copyJobSpec")
        )

        rendered = _run_node_json(
            formatter
            + """
const detail = {
  spec: {
    name: 'historical-job',
    run: {
      command: 'uv run benchmark',
      env: {VISIBLE_NAME:'[REDACTED]'},
      secret_env: {TOKEN:'[SECRET_REFERENCE]'},
    },
  },
  password: 'DO_NOT_RENDER_DETAIL_FIELDS',
  workers_detail: [{account_email:'private@example.test'}],
};
process.stdout.write(JSON.stringify({text:jobSpecTextFromDetail(detail)}));
"""
        )
        parsed = json.loads(rendered["text"])
        assert parsed["name"] == "historical-job"
        assert parsed["run"]["env"] == {"VISIBLE_NAME": "[REDACTED]"}
        assert parsed["run"]["secret_env"] == {
            "TOKEN": "[SECRET_REFERENCE]"
        }
        assert "DO_NOT_RENDER_DETAIL_FIELDS" not in rendered["text"]
        assert "private@example.test" not in rendered["text"]

    @pytest.mark.asyncio
    async def test_job_config_detail_cache_is_single_flight_and_handles_old_jobs(
        self, ui_client,
    ):
        client, _ = ui_client
        html = (await client.get("/batch")).text
        functions = "\n".join(
            _javascript_function(html, name)
            for name in (
                "jobSpecTextFromDetail",
                "setJobSpecState",
                "touchJobSpecState",
                "loadJobSpec",
            )
        )

        result = _run_node_json(
            """
const jobSpecCache = new Map();
const jobSpecRequests = new Map();
let jobSpecRevision = 0;
let jobSpecCacheChars = 0;
const JOB_SPEC_CACHE_MAX_ENTRIES = 8;
const JOB_SPEC_CACHE_MAX_CHARS = 4_000_000;
const JOB_SPEC_TEXT_MAX_CHARS = 1_000_000;
let latestJobs = [];
let paints = 0;
const calls = [];
let releaseFirst;
const firstResponse = new Promise(resolve => { releaseFirst = resolve; });
const responses = [
  firstResponse,
  () => ({job_id:'legacy', spec:{}}),
  () => { throw Object.assign(new Error('503: busy'), {status:503}); },
  () => ({job_id:'temporary', spec:{name:'retry-ok'}}),
  () => ({job_id:'huge', spec:{command:'x'.repeat(1_000_001)}}),
];
async function api(method, path) {
  calls.push([method, path]);
  const response = responses.shift();
  return typeof response === 'function' ? response() : response;
}
function requestJobSpecDetail(jobId) {
  return api('GET', '/jobs/' + encodeURIComponent(jobId));
}
function visibleJobs() { return []; }
function reconcileJobCards() { paints += 1; }
"""
            + functions
            + """
(async () => {
  const first = loadJobSpec('job/one');
  const duplicate = loadJobSpec('job/one');
  await new Promise(resolve => setImmediate(resolve));
  const callsWhileLoading = calls.length;
  releaseFirst({
    job_id:'job/one',
    spec:{name:'one', run:{env:{TOKEN:'[REDACTED]'}}},
  });
  await Promise.all([first, duplicate]);
  await loadJobSpec('job/one');
  const callsAfterCachedRead = calls.length;
  await loadJobSpec('legacy');
  await loadJobSpec('temporary');
  const temporaryError = {...jobSpecCache.get('temporary')};
  await loadJobSpec('temporary', true);
  await loadJobSpec('huge');
  const snapshots = {
    ready:{...jobSpecCache.get('job/one')},
    legacy:{...jobSpecCache.get('legacy')},
    temporaryError,
    temporaryRetry:{...jobSpecCache.get('temporary')},
    huge:{...jobSpecCache.get('huge')},
  };
  for (let index = 0; index < 9; index += 1) {
    setJobSpecState('lru-' + index, {status:'ready', text:'value-' + index});
  }
  process.stdout.write(JSON.stringify({
    calls,
    callsWhileLoading,
    callsAfterCachedRead,
    snapshots,
    cacheKeys:Array.from(jobSpecCache.keys()),
    cacheChars:jobSpecCacheChars,
    requestsCleared:jobSpecRequests.size === 0,
    paints,
  }));
})().catch(error => {
  process.stderr.write(String(error.stack || error));
  process.exitCode = 1;
});
"""
        )

        assert result["callsWhileLoading"] == 1
        assert result["callsAfterCachedRead"] == 1
        assert result["calls"] == [
            ["GET", "/jobs/job%2Fone"],
            ["GET", "/jobs/legacy"],
            ["GET", "/jobs/temporary"],
            ["GET", "/jobs/temporary"],
            ["GET", "/jobs/huge"],
        ]
        snapshots = result["snapshots"]
        assert snapshots["ready"]["status"] == "ready"
        assert json.loads(snapshots["ready"]["text"])["name"] == "one"
        assert snapshots["legacy"]["status"] == "missing"
        assert snapshots["temporaryError"]["status"] == "error"
        assert snapshots["temporaryError"]["error_status"] == 503
        assert snapshots["temporaryRetry"]["status"] == "ready"
        assert snapshots["huge"]["status"] == "too_large"
        assert "text" not in snapshots["huge"]
        assert result["cacheKeys"] == [
            "lru-1",
            "lru-2",
            "lru-3",
            "lru-4",
            "lru-5",
            "lru-6",
            "lru-7",
            "lru-8",
        ]
        assert result["cacheChars"] == sum(
            len(f"value-{index}") for index in range(1, 9)
        )
        assert result["requestsCleared"] is True
        assert result["paints"] >= 6

    @pytest.mark.asyncio
    async def test_job_config_detail_requests_have_global_concurrency_limit(
        self, ui_client,
    ):
        client, _ = ui_client
        html = (await client.get("/batch")).text
        functions = "\n".join(
            _javascript_function(html, name)
            for name in (
                "drainJobSpecRequestQueue",
                "requestJobSpecDetail",
            )
        )

        result = _run_node_json(
            """
const JOB_SPEC_REQUEST_CONCURRENCY = 2;
const JOB_SPEC_REQUEST_QUEUE_MAX = 8;
const jobSpecRequestQueue = [];
let jobSpecRequestActive = 0;
let active = 0;
let maximumActive = 0;
const releases = [];
async function api(method, path) {
  active += 1;
  maximumActive = Math.max(maximumActive, active);
  await new Promise(resolve => releases.push(resolve));
  active -= 1;
  return {path};
}
"""
            + functions
            + """
(async () => {
  const requests = Array.from(
    {length:10}, (_, index) => requestJobSpecDetail('job-' + index)
  );
  const overflow = requestJobSpecDetail('job-overflow')
    .then(() => null, error => error.status);
  await new Promise(resolve => setImmediate(resolve));
  const firstWave = {active, queued:jobSpecRequestQueue.length};
  while (releases.length || jobSpecRequestQueue.length || active) {
    const batch = releases.splice(0);
    batch.forEach(resolve => resolve());
    await new Promise(resolve => setImmediate(resolve));
  }
  const values = await Promise.all(requests);
  process.stdout.write(JSON.stringify({
    firstWave,
    maximumActive,
    paths:values.map(value => value.path),
    overflow:await overflow,
    active:jobSpecRequestActive,
    queued:jobSpecRequestQueue.length,
  }));
})().catch(error => {
  process.stderr.write(String(error.stack || error));
  process.exitCode = 1;
});
"""
        )

        assert result == {
            "firstWave": {"active": 2, "queued": 8},
            "maximumActive": 2,
            "paths": [f"/jobs/job-{index}" for index in range(10)],
            "overflow": 429,
            "active": 0,
            "queued": 0,
        }

    @pytest.mark.asyncio
    async def test_job_config_out_of_order_details_stay_with_their_job(
        self, ui_client,
    ):
        client, _ = ui_client
        html = (await client.get("/batch")).text
        functions = "\n".join(
            _javascript_function(html, name)
            for name in (
                "jobSpecTextFromDetail",
                "setJobSpecState",
                "loadJobSpec",
            )
        )

        result = _run_node_json(
            """
const jobSpecCache = new Map();
const jobSpecRequests = new Map();
let jobSpecRevision = 0;
let jobSpecCacheChars = 0;
const JOB_SPEC_CACHE_MAX_ENTRIES = 8;
const JOB_SPEC_CACHE_MAX_CHARS = 4_000_000;
const JOB_SPEC_TEXT_MAX_CHARS = 1_000_000;
let latestJobs = [];
const resolvers = new Map();
function requestJobSpecDetail(jobId) {
  return new Promise(resolve => resolvers.set(jobId, resolve));
}
function visibleJobs() { return []; }
function reconcileJobCards() {}
"""
            + functions
            + """
(async () => {
  const a = loadJobSpec('job-a');
  const b = loadJobSpec('job-b');
  resolvers.get('job-b')({job_id:'job-b', spec:{name:'B'}});
  await b;
  resolvers.get('job-a')({job_id:'job-a', spec:{name:'A'}});
  await a;
  process.stdout.write(JSON.stringify({
    a:JSON.parse(jobSpecCache.get('job-a').text),
    b:JSON.parse(jobSpecCache.get('job-b').text),
  }));
})().catch(error => {
  process.stderr.write(String(error.stack || error));
  process.exitCode = 1;
});
"""
        )

        assert result == {"a": {"name": "A"}, "b": {"name": "B"}}

    @pytest.mark.asyncio
    async def test_job_config_open_and_scroll_survive_card_reconciliation(
        self, ui_client,
    ):
        client, _ = ui_client
        html = (await client.get("/batch")).text
        capture = _javascript_function(html, "captureJobConfigUiState")
        restore = _javascript_function(html, "restoreJobConfigUiState")
        reconcile = _javascript_function(html, "reconcileJobCards")

        assert "replacement.open = wasOpen" in reconcile
        assert "captureJobConfigUiState(node)" in reconcile
        assert "restoreJobConfigUiState(replacement" in reconcile
        assert "restoreJobFocus(replacement, focusedControl)" in reconcile
        assert 'data-job-focus="job-config-summary"' in html
        assert 'data-job-focus="job-config-copy"' in html
        assert "window.scrollTo(viewportX, viewportY)" in reconcile

        state = _run_node_json(
            """
const oldDetails = {open:true};
const oldJson = {scrollTop:137, scrollLeft:29};
const oldNode = {
  querySelector(selector) {
    if (selector === '[data-job-config]') return oldDetails;
    if (selector === '.job-config-json') return oldJson;
    return null;
  },
};
const newDetails = {open:false};
const newJson = {scrollTop:0, scrollLeft:0};
const newNode = {
  querySelector(selector) {
    if (selector === '[data-job-config]') return newDetails;
    if (selector === '.job-config-json') return newJson;
    return null;
  },
};
"""
            + capture
            + "\n"
            + restore
            + """
const captured = captureJobConfigUiState(oldNode);
restoreJobConfigUiState(newNode, captured);
process.stdout.write(JSON.stringify({
  captured,
  open:newDetails.open,
  scrollTop:newJson.scrollTop,
  scrollLeft:newJson.scrollLeft,
}));
"""
        )

        assert state == {
            "captured": {
                "open": True,
                "scrollTop": 137,
                "scrollLeft": 29,
            },
            "open": True,
            "scrollTop": 137,
            "scrollLeft": 29,
        }

    @pytest.mark.asyncio
    async def test_account_status_refresh_is_visible_single_flight_and_ordered(
        self, ui_client,
    ):
        client, _ = ui_client
        html = (await client.get("/batch")).text
        refresh_once = _javascript_function(html, "refreshAccountsOnce")
        runner = _javascript_function(html, "runAccountRefreshes")
        refresh = _javascript_function(html, "refreshAccounts")
        dashboard_poll = _javascript_function(html, "runDashboardPoll")

        assert 'id="accountsRefresh"' in html
        assert re.search(
            r'id="accountsRefresh"[^>]*role="status"[^>]*aria-live="polite"',
            html,
        )
        assert "onclick=\"refreshAccounts(true)\"" in html
        assert "requestVersion !== accountsRequestVersion" in refresh_once
        assert "accountsRefreshInFlight" in refresh
        assert "accountsRefreshQueued = true" in refresh
        assert "Date.now() - lastAccountsRefreshAt >= 15_000" in dashboard_poll
        assert dashboard_poll.index("if (document.hidden)") < (
            dashboard_poll.index("refreshAccounts()")
        )
        assert "refreshAccounts(true)" in _javascript_function(html, "submitJob")
        assert "refreshAccounts(true)" in _javascript_function(html, "cancelJob")

        result = _run_node_json(
            """
let accountsRefreshInFlight = null;
let accountsRefreshQueued = false;
let accountsRequestVersion = 0;
let lastAccountsRefreshAt = 0;
let active = 0;
let maximumActive = 0;
const versions = [];
const releases = [];
const status = {textContent:''};
global.document = {getElementById:() => status};
Date.now = () => 1_000;
async function refreshAccountsOnce(version) {
  versions.push(version);
  active += 1;
  maximumActive = Math.max(maximumActive, active);
  await new Promise((resolve) => releases.push(resolve));
  active -= 1;
}
"""
            + runner
            + "\n"
            + refresh
            + """
(async () => {
  const first = refreshAccounts();
  const coalesced = refreshAccounts();
  const forced = refreshAccounts(true);
  const sharedPromise = first === coalesced && coalesced === forced;
  releases.shift()();
  await new Promise((resolve) => setImmediate(resolve));
  const queuedSecondPass = versions.length === 2 && releases.length === 1;
  releases.shift()();
  await Promise.all([first, coalesced, forced]);
  process.stdout.write(JSON.stringify({
    sharedPromise,
    queuedSecondPass,
    versions,
    maximumActive,
    inFlightCleared: accountsRefreshInFlight === null,
  }));
})().catch((error) => {
  process.stderr.write(String(error.stack || error));
  process.exitCode = 1;
});
"""
        )

        assert result == {
            "sharedPromise": True,
            "queuedSecondPass": True,
            "versions": [1, 2],
            "maximumActive": 1,
            "inFlightCleared": True,
        }

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
        assert "preserveKnown && job.done" not in html
        assert "knownFileCount > 0 && job.done" not in html
        assert "function jobResultActionHtml(job, result)" in html
        assert 'data-result-action="' in html
        assert "📄 查看失败日志" in html
        assert "function jobLogLineLimit(jobId, workerId)" in html
        assert "return terminal ? 5_000 : 1_000" in html
        assert "formatTaskExitSummary(data.tasks || [])" in html

    @pytest.mark.asyncio
    async def test_terminal_result_gap_keeps_snapshot_but_continues_polling(
        self, ui_client,
    ):
        client, _ = ui_client
        html = (await client.get("/batch")).text
        functions = "\n".join(
            _javascript_function(html, name)
            for name in (
                "resultFileCount",
                "nextResultCheck",
                "commitJobResult",
                "commitJobResultError",
            )
        )
        result = _run_node_json(
            """
const jobResultsCache = new Map();
const jobResultsRequestVersions = new Map();
Date.now = () => 1_000;
"""
            + functions
            + """
const job = {job_id:'job-1', done:true};
const oldValue = {job_id:'job-1', file_count:2, paths:['partial']};
jobResultsCache.set('job-1', {value:oldValue, misses:0, loading:false});

jobResultsRequestVersions.set('job-1', 1);
const failure = new Error('temporary 503');
failure.status = 503;
commitJobResultError(job, failure, 1);
const afterError = jobResultsCache.get('job-1');

jobResultsRequestVersions.set('job-1', 2);
commitJobResult(job, {job_id:'job-1', file_count:0, paths:[]}, 2);
const afterEmpty = jobResultsCache.get('job-1');

jobResultsRequestVersions.set('job-1', 3);
commitJobResult(job, {job_id:'job-1', file_count:3, paths:['final']}, 3);
const afterFinal = jobResultsCache.get('job-1');

process.stdout.write(JSON.stringify({
  errorPreserved: afterError.value === oldValue,
  errorRetries: Number.isFinite(afterError.nextCheck)
    && afterError.nextCheck > Date.now(),
  emptyPreserved: afterEmpty.value === oldValue,
  emptyRetries: Number.isFinite(afterEmpty.nextCheck)
    && afterEmpty.nextCheck > Date.now(),
  finalCount: afterFinal.value.file_count,
  finalFrozen: !Number.isFinite(afterFinal.nextCheck),
  finalError: afterFinal.error,
}));
"""
        )

        assert result == {
            "errorPreserved": True,
            "errorRetries": True,
            "emptyPreserved": True,
            "emptyRetries": True,
            "finalCount": 3,
            "finalFrozen": True,
            "finalError": None,
        }

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
    async def test_authenticated_session_can_open_ui(self, ui_client):
        client, _ = ui_client
        resp = await client.get("/")
        assert resp.status_code == 303
        assert resp.headers["location"] == "/ui-v2/"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("path", ["/batch", "/fleet", "/dashboard"])
    async def test_anonymous_ui_redirects_to_login(
        self, anonymous_ui_client, path
    ):
        client, _ = anonymous_ui_client
        resp = await client.get(path)

        assert resp.status_code == 303
        assert resp.headers["location"].startswith("/login?next=")
        assert "https" not in resp.headers["location"]

    @pytest.mark.asyncio
    async def test_anonymous_root_redirects_directly_to_current_ui_v2(
        self, anonymous_ui_client
    ):
        client, _ = anonymous_ui_client
        resp = await client.get("/")

        assert resp.status_code == 303
        assert resp.headers["location"] == "/ui-v2/"

    @pytest.mark.asyncio
    async def test_must_change_password_session_cannot_open_console(
        self, ui_client
    ):
        client, _ = ui_client
        principal = SimpleNamespace(
            subject="admin@example.test",
            must_change_password=True,
        )
        with patch(
            "elastic_agent.api.routes.ui.get_session_principal",
            new=AsyncMock(return_value=principal),
        ):
            resp = await client.get("/fleet", follow_redirects=False)

        assert resp.status_code == 303
        assert resp.headers["location"] == "/change-password?next=%2Ffleet"

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
        assert ui_resp.status_code == 303
        assert ui_resp.headers["location"] == "/ui-v2/"
        api_resp = await client.get(
            "/api/nodes",
            headers={"Authorization": "Bearer test-key"},
        )
        assert api_resp.status_code == 200
