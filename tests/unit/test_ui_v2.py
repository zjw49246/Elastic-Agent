"""Tests for UI v2 static shell routes and /api/ui/summary."""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from pathlib import Path
from unittest.mock import patch

import pytest
from httpx import ASGITransport, AsyncClient

from elastic_agent.api.routes.ui_v2 import UI_V2_ROOT, reset_summary_cache
from elastic_agent.testing import create_test_manager

pytestmark = pytest.mark.level0


@pytest.fixture
async def ui_client():
    result = create_test_manager()
    from elastic_agent.api.app import create_app

    app = create_app(result.manager)
    reset_summary_cache()
    with patch.dict(os.environ, {"ELASTIC_AGENT_EXTERNAL_API_KEYS": "test-key"}):
        from elastic_agent.api import auth

        auth.reset_api_keys()
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://testserver",
        ) as client:
            yield client, result
        auth.reset_api_keys()


AUTH = {"Authorization": "Bearer test-key"}


class TestStaticShell:
    @pytest.mark.asyncio
    async def test_ui_v2_serves_index_without_auth(self, ui_client):
        client, _ = ui_client
        for path in ("/ui-v2", "/ui-v2/", "/ui-v2/overview", "/ui-v2/jobs/some-job"):
            resp = await client.get(path)
            assert resp.status_code == 200, path
            assert "text/html" in resp.headers["content-type"]
            assert "Elastic-Agent" in resp.text
            # SPA shell contains no data and no key material.
            assert "test-key" not in resp.text

    @pytest.mark.asyncio
    async def test_index_has_no_store_and_csp_headers(self, ui_client):
        client, _ = ui_client
        resp = await client.get("/ui-v2/overview")
        assert "no-cache" in resp.headers["cache-control"]
        assert resp.headers["x-content-type-options"] == "nosniff"
        csp = resp.headers["content-security-policy"]
        assert "default-src 'self'" in csp
        assert "unsafe-inline" not in csp
        assert resp.headers["referrer-policy"] == "no-referrer"

    @pytest.mark.asyncio
    async def test_js_and_css_served_with_correct_mime(self, ui_client):
        client, _ = ui_client
        js = await client.get("/ui-v2/js/app.js")
        assert js.status_code == 200
        assert js.headers["content-type"].startswith("text/javascript")
        assert js.headers["x-content-type-options"] == "nosniff"
        css = await client.get("/ui-v2/assets/app.css")
        assert css.status_code == 200
        assert css.headers["content-type"].startswith("text/css")

    @pytest.mark.asyncio
    async def test_path_traversal_is_rejected(self, ui_client):
        client, _ = ui_client
        for path in (
            "/ui-v2/..%2f..%2fpyproject.toml",
            "/ui-v2/js/../../routes/ui.py",
            "/ui-v2/js/app.py",
        ):
            resp = await client.get(path)
            assert resp.status_code in (400, 404), path
            if resp.status_code == 200:  # pragma: no cover — defensive
                raise AssertionError(path)

    @pytest.mark.asyncio
    async def test_spa_fallback_never_claims_api_or_ws(self, ui_client):
        client, _ = ui_client
        # An unknown API path must 404 as JSON, not fall back to the shell.
        resp = await client.get("/api/does-not-exist", headers=AUTH)
        assert resp.status_code in (401, 404, 405)
        assert "text/html" not in resp.headers.get("content-type", "")

    @pytest.mark.asyncio
    async def test_legacy_batch_and_fleet_untouched(self, ui_client):
        client, _ = ui_client
        assert (await client.get("/batch")).status_code == 200
        assert (await client.get("/fleet")).status_code == 200
        assert "Batch Console" in (await client.get("/")).text


class TestUiSummary:
    @pytest.mark.asyncio
    async def test_summary_requires_api_key(self, ui_client):
        client, _ = ui_client
        assert (await client.get("/api/ui/summary")).status_code == 401

    @pytest.mark.asyncio
    async def test_summary_shape_and_no_identifiers(self, ui_client):
        client, _ = ui_client
        resp = await client.get("/api/ui/summary", headers=AUTH)
        assert resp.status_code == 200
        data = resp.json()
        for section in ("manager", "jobs", "workers", "accounts", "otp"):
            assert section in data
        assert set(data["jobs"]) >= {"active", "by_state", "terminal_total", "cleanup_pending"}
        assert set(data["workers"]) >= {"total", "connected", "by_status"}
        assert set(data["accounts"]) >= {"total", "enabled", "allocated"}
        assert "pending" in data["otp"]
        # Aggregate counters only — never IDs, emails or job names.
        serialized = json.dumps(data)
        assert "@" not in serialized
        assert "account_id" not in serialized
        assert "job_id" not in serialized


class TestFrontendSourceInvariants:
    """Static assertions over the v2 JS source (secret custody, no innerHTML)."""

    def _all_js(self) -> list[Path]:
        return sorted((UI_V2_ROOT / "js").rglob("*.js"))

    def test_no_innerhtml_or_inline_handlers(self):
        for module in self._all_js():
            source = module.read_text(encoding="utf-8")
            assert ".innerHTML" not in source, module
            assert "document.write" not in source, module
            assert re.search(r"\son[a-z]+\s*=\s*[\"']", source) is None, module

    def test_no_localstorage_and_no_service_worker(self):
        for module in self._all_js():
            source = module.read_text(encoding="utf-8")
            # Match actual API usage, not prose in comments.
            assert re.search(r"localStorage\s*[.\[(]", source) is None, module
            assert re.search(r"serviceWorker\s*[.\[(]", source) is None, module
            assert re.search(r"indexedDB\s*[.\[(]", source) is None, module

    def test_api_key_stays_in_auth_module(self):
        # Only auth.js touches the sessionStorage slot or builds the Bearer
        # header value; other modules go through authHeader()/rawFetch().
        for module in self._all_js():
            source = module.read_text(encoding="utf-8")
            if module.name == "auth.js":
                continue
            assert "ea_api_key" not in source, module
            assert "Bearer ${" not in source, module

    def test_index_has_no_inline_script(self):
        html = (UI_V2_ROOT / "index.html").read_text(encoding="utf-8")
        # The only <script> is the module entry with a src attribute.
        scripts = re.findall(r"<script[^>]*>(.*?)</script>", html, re.DOTALL)
        for body in scripts:
            assert body.strip() == "", "inline script bodies are forbidden by CSP"
        assert 'type="module"' in html
        assert "onclick=" not in html

    def test_download_urls_never_carry_key(self):
        source = (UI_V2_ROOT / "js" / "core" / "downloads.js").read_text(encoding="utf-8")
        assert "api_key" not in source
        assert "rawFetch" in source  # authenticated fetch, not <a href>
        assert "AbortController" in source

    def test_otp_component_keys_by_request_and_challenge(self):
        source = (UI_V2_ROOT / "js" / "components" / "otp-center.js").read_text(encoding="utf-8")
        assert "login_request_id" in source and "challenge_id" in source
        assert "::" in source  # composite key
        assert re.search(r"\\d\{6\}|\\d{6}", source)

    def test_job_spec_builder_covers_current_schema(self):
        source = (UI_V2_ROOT / "js" / "core" / "job-spec.js").read_text(encoding="utf-8")
        for field in (
            "resume_command", "checkpoint_keep_generations", "interval_seconds",
            "s3_datasets", "secret_env", "login_timeout_seconds", "harness_ref",
            "recovery", "source_job_id", "max_rotations",
        ):
            assert field in source, field
        # Idempotency intent machinery present.
        assert "createSubmissionIntent" in source
        assert "Idempotency-Key" in (
            UI_V2_ROOT / "js" / "pages" / "job-new.js"
        ).read_text(encoding="utf-8")

    def test_eip_decommission_flow_order(self):
        source = (UI_V2_ROOT / "js" / "pages" / "accounts.js").read_text(encoding="utf-8")
        assert "release_eip" in source
        assert "confirm_account_id" in source
        # The delete call happens after decommission and aborts on failure.
        decommission_pos = source.index("binding/decommission")
        delete_pos = source.index("await del(")
        assert decommission_pos < delete_pos

    def test_build_script_validates_imports(self):
        result = subprocess.run(
            [sys.executable, "scripts/build_ui_v2.py", "--check"],
            cwd=Path(__file__).resolve().parents[2],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, result.stderr
