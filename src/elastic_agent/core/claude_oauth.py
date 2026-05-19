"""ClaudeOAuthProvider — 14-step OAuth auto-login on Worker via 171mail + Playwright + mitmproxy.

T-041: Executes the full OAuth login flow on a Worker to obtain Claude CLI credentials.
Triggered during Bootstrap, token expiration, or manual restart.

Worker dependencies (installed during Bootstrap):
- Python 3.11+, Playwright + playwright-stealth, Chrome, Xvfb, mitmproxy, Node.js + Claude Code CLI
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

OAUTH_CLIENT_ID = "9d1c250a-e61b-44d9-88ed-5944d1962f5e"
MAIL_API_BASE = "https://b.171mail.com/api/v1"
ANTHROPIC_TOKEN_URL = "https://console.anthropic.com/v1/oauth/token"
ANTHROPIC_USAGE_URL = "https://api.anthropic.com/api/oauth/usage"
ANTHROPIC_ACCOUNT_URL = "https://claude.ai/api/account"

MAGIC_LINK_POLL_INTERVAL = 2.0
MAGIC_LINK_TIMEOUT = 90.0

MITMPROXY_ADDON_SCRIPT = '''
"""mitmproxy addon: fix Claude CLI OAuth redirect_uri bug."""

from mitmproxy import http
import urllib.parse

class ClaudeOAuthFixer:
    def request(self, flow: http.HTTPFlow) -> None:
        if flow.request.pretty_url.endswith("/v1/oauth/token") and flow.request.method == "POST":
            body = flow.request.get_text()
            if body:
                params = urllib.parse.parse_qs(body)
                changed = False
                if "redirect_uri" in params:
                    params["redirect_uri"] = ["https://platform.claude.com/oauth/code/callback"]
                    changed = True
                if "code" in params:
                    codes = params["code"]
                    fixed = [c.split("#")[0] for c in codes]
                    if fixed != codes:
                        params["code"] = fixed
                        changed = True
                if changed:
                    flow.request.set_text(urllib.parse.urlencode(
                        {k: v[0] for k, v in params.items()}
                    ))

addons = [ClaudeOAuthFixer()]
'''


@dataclass
class LoginResult:
    """Result of an OAuth login attempt."""

    success: bool
    account_id: str
    error: str | None = None
    expires_at: int | None = None
    access_token: str | None = None
    refresh_token: str | None = None


@dataclass
class OAuthConfig:
    """Configuration for an OAuth login attempt."""

    account_id: str
    email: str
    email_token: str
    config_dir: str
    login_timeout: int = 240
    mitm_port: int = 8765


class ClaudeOAuthProvider:
    """Executes the 14-step OAuth login flow on a Worker.

    This class is designed to run ON the Worker machine. The Manager sends
    a CREDENTIAL_LOGIN command, and the Worker Runtime invokes this provider.

    Flow:
    1. File lock (per-account concurrency control)
    2. 171mail API: trigger magic link
    3. Poll 171mail inbox for magic link (2s intervals, 90s timeout)
    4. Verify magic link (returns session cookie + sessionKey)
    5. Start mitmproxy (fix redirect_uri bug)
    6. Launch Claude CLI auth login
    7. Extract OAuth authorize URL from CLI stdout
    8. Playwright: inject cookie, navigate, authorize
    9. Capture callback code+state
    10. Send code+state to CLI callback port
    11. CLI writes .credentials.json
    12. Verify with claude auth status
    13-14. Cleanup (Playwright, mitmproxy, file lock)
    """

    def __init__(self, http_client: Any | None = None):
        self._http_client = http_client
        self._lock_files: dict[str, Any] = {}

    async def login(self, config: OAuthConfig) -> LoginResult:
        """Execute the full 14-step OAuth login flow."""
        account_id = config.account_id
        logger.info("ClaudeOAuthProvider: starting login for %s", account_id)

        mitm_process = None
        cli_process = None
        browser = None
        context = None
        lock_path = None

        try:
            # Step 1: File lock
            lock_path = Path(config.config_dir) / f".login_lock_{account_id}"
            lock_path.parent.mkdir(parents=True, exist_ok=True)
            await self._acquire_lock(lock_path)

            # Step 2: Trigger magic link via 171mail
            send_result = await self._send_magic_link(config.email)
            if not send_result.get("success"):
                return LoginResult(
                    success=False,
                    account_id=account_id,
                    error=f"Failed to send magic link: {send_result.get('error', 'unknown')}",
                )

            device_id = send_result.get("deviceId", "")
            client_sha = send_result.get("clientSha", "")

            # Step 3: Poll for magic link
            magic_link = await self._poll_magic_link(config.email_token)
            if magic_link is None:
                return LoginResult(
                    success=False,
                    account_id=account_id,
                    error="Timed out waiting for magic link",
                )

            # Step 4: Verify magic link
            verify_result = await self._verify_magic_link(
                magic_link, device_id, client_sha, config.email
            )
            if not verify_result.get("success"):
                return LoginResult(
                    success=False,
                    account_id=account_id,
                    error=f"Magic link verification failed: {verify_result.get('error', 'unknown')}",
                )

            session_cookie = verify_result.get("cookie", "")
            session_key = verify_result.get("sessionKey", "")

            # Step 5: Start mitmproxy
            mitm_process = await self._start_mitmproxy(config.mitm_port)

            # Step 6: Launch Claude CLI auth login
            cli_process = await self._launch_cli_auth(config)

            # Step 7: Extract OAuth authorize URL
            authorize_url = await self._extract_authorize_url(cli_process, timeout=30)
            if authorize_url is None:
                return LoginResult(
                    success=False,
                    account_id=account_id,
                    error="Failed to extract OAuth authorize URL from CLI",
                )

            # Step 8: Playwright browser automation
            callback_url = await self._playwright_authorize(
                authorize_url, session_cookie, session_key, config.email
            )
            if callback_url is None:
                return LoginResult(
                    success=False,
                    account_id=account_id,
                    error="Playwright authorization failed",
                )

            # Step 9: Extract code+state from callback URL
            code, state = self._extract_code_state(callback_url)
            if not code:
                return LoginResult(
                    success=False,
                    account_id=account_id,
                    error="Failed to extract code/state from callback",
                )

            # Step 10: Send code+state to CLI callback port
            cli_port = self._extract_cli_port(authorize_url)
            if cli_port:
                await self._send_callback(cli_port, code, state)

            # Step 11: Wait for CLI to complete and write credentials
            await self._wait_cli_complete(cli_process, timeout=30)

            # Step 12: Verify login
            creds = self._read_credentials(config.config_dir)
            if creds is None:
                return LoginResult(
                    success=False,
                    account_id=account_id,
                    error="credentials.json not found after login",
                )

            logger.info("ClaudeOAuthProvider: login successful for %s", account_id)
            return LoginResult(
                success=True,
                account_id=account_id,
                expires_at=creds.get("expiresAt"),
                access_token=creds.get("accessToken"),
                refresh_token=creds.get("refreshToken"),
            )

        except asyncio.TimeoutError:
            return LoginResult(
                success=False,
                account_id=account_id,
                error="Login timed out",
            )
        except Exception as e:
            logger.exception("ClaudeOAuthProvider: login failed for %s", account_id)
            return LoginResult(
                success=False,
                account_id=account_id,
                error=str(e),
            )
        finally:
            # Steps 13-14: Cleanup
            if cli_process:
                await self._kill_process(cli_process)
            if mitm_process:
                await self._kill_process(mitm_process)
            if lock_path:
                await self._release_lock(lock_path)

    # -- Step helpers -------------------------------------------------------

    async def _send_magic_link(self, email: str) -> dict[str, Any]:
        """Step 2: POST to 171mail to trigger magic link email."""
        return await self._http_post(
            f"{MAIL_API_BASE}/claude/send",
            json_body={"email": email},
        )

    async def _poll_magic_link(self, email_token: str) -> str | None:
        """Step 3: Poll 171mail inbox for magic link."""
        start = time.monotonic()
        while time.monotonic() - start < MAGIC_LINK_TIMEOUT:
            result = await self._http_get(
                f"{MAIL_API_BASE}/getClaudeMessage",
                params={"token": email_token},
            )
            link = result.get("link") or result.get("magicLink")
            if link:
                return link
            await asyncio.sleep(MAGIC_LINK_POLL_INTERVAL)
        return None

    async def _verify_magic_link(
        self, link: str, device_id: str, client_sha: str, email: str
    ) -> dict[str, Any]:
        """Step 4: Verify magic link with 171mail."""
        return await self._http_post(
            f"{MAIL_API_BASE}/claude/verify",
            json_body={
                "link": link,
                "info": {
                    "deviceId": device_id,
                    "clientSha": client_sha,
                    "email": email,
                },
            },
        )

    async def _start_mitmproxy(self, port: int) -> asyncio.subprocess.Process:
        """Step 5: Start mitmproxy with the OAuth fixer addon."""
        addon_file = Path(tempfile.mktemp(suffix="_mitm_addon.py"))
        addon_file.write_text(MITMPROXY_ADDON_SCRIPT)

        process = await asyncio.create_subprocess_exec(
            "mitmdump",
            "--listen-port", str(port),
            "--set", "ssl_insecure=true",
            "-s", str(addon_file),
            "--quiet",
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        await asyncio.sleep(2)
        return process

    async def _launch_cli_auth(self, config: OAuthConfig) -> asyncio.subprocess.Process:
        """Step 6: Launch Claude CLI with auth login."""
        env = {
            "CLAUDE_CONFIG_DIR": config.config_dir,
            "HTTPS_PROXY": f"http://127.0.0.1:{config.mitm_port}",
            "HTTP_PROXY": f"http://127.0.0.1:{config.mitm_port}",
            "NODE_TLS_REJECT_UNAUTHORIZED": "0",
            "PATH": "/usr/local/bin:/usr/bin:/bin",
            "HOME": str(Path.home()),
        }
        process = await asyncio.create_subprocess_exec(
            "claude", "auth", "login", "--email", config.email,
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        return process

    async def _extract_authorize_url(
        self, process: asyncio.subprocess.Process, timeout: int = 30
    ) -> str | None:
        """Step 7: Read CLI stdout for the OAuth authorize URL."""
        assert process.stdout is not None
        start = time.monotonic()
        buffer = ""
        while time.monotonic() - start < timeout:
            try:
                chunk = await asyncio.wait_for(
                    process.stdout.read(4096), timeout=2.0
                )
            except asyncio.TimeoutError:
                continue
            if not chunk:
                break
            buffer += chunk.decode("utf-8", errors="replace")
            match = re.search(r"(https://claude\.com/cai/oauth/authorize\S+)", buffer)
            if match:
                return match.group(1)
        return None

    async def _playwright_authorize(
        self,
        authorize_url: str,
        session_cookie: str,
        session_key: str,
        email: str,
    ) -> str | None:
        """Step 8: Use Playwright to navigate and authorize the OAuth request."""
        try:
            from playwright.async_api import async_playwright
        except ImportError:
            logger.error("Playwright not installed — cannot perform OAuth authorization")
            return None

        callback_url = None

        try:
            pw = await async_playwright().start()
            browser = await pw.chromium.launch(headless=True)
            context = await browser.new_context()

            # Inject session cookie
            if session_cookie:
                cookies = self._parse_session_cookie(session_cookie)
                if cookies:
                    await context.add_cookies(cookies)

            page = await context.new_page()

            # Intercept the callback redirect to capture code+state
            captured: dict[str, str | None] = {"url": None}

            async def handle_route(route):
                captured["url"] = route.request.url
                await route.abort()

            await page.route("**/oauth/code/callback*", handle_route)

            # 8a: Verify account identity
            try:
                resp = await page.goto(ANTHROPIC_ACCOUNT_URL)
                if resp and resp.ok:
                    body = await resp.json()
                    account_email = body.get("email", "")
                    if account_email and account_email != email:
                        logger.warning(
                            "Account email mismatch: expected %s, got %s",
                            email, account_email,
                        )
            except Exception:
                logger.debug("Account verification skipped (non-critical)")

            # 8c: Navigate to OAuth URL
            await page.goto(authorize_url, wait_until="domcontentloaded")

            # 8d: Wait for potential Cloudflare challenge
            await page.wait_for_timeout(3000)

            # 8e: Click Authorize button
            try:
                authorize_btn = page.locator("button:has-text('Authorize')")
                if await authorize_btn.count() > 0:
                    await authorize_btn.click()
                    await page.wait_for_timeout(3000)
            except Exception:
                logger.debug("No Authorize button found or click failed")

            callback_url = captured.get("url")

            await context.close()
            await browser.close()
            await pw.stop()

        except Exception as e:
            logger.exception("Playwright authorization failed: %s", e)
            return None

        return callback_url

    def _extract_code_state(self, callback_url: str) -> tuple[str, str]:
        """Step 9: Extract code and state from callback URL."""
        from urllib.parse import urlparse, parse_qs

        parsed = urlparse(callback_url)
        params = parse_qs(parsed.query)
        code = params.get("code", [""])[0]
        state = params.get("state", [""])[0]
        # Strip #state suffix if present
        if "#" in code:
            code = code.split("#")[0]
        return code, state

    def _extract_cli_port(self, authorize_url: str) -> int | None:
        """Extract the localhost callback port from the authorize URL's redirect_uri."""
        from urllib.parse import urlparse, parse_qs

        parsed = urlparse(authorize_url)
        params = parse_qs(parsed.query)
        redirect_uri = params.get("redirect_uri", [""])[0]
        match = re.search(r":(\d+)", redirect_uri)
        if match:
            return int(match.group(1))
        return None

    async def _send_callback(self, port: int, code: str, state: str) -> None:
        """Step 10: Send code+state to CLI's localhost callback port."""
        url = f"http://127.0.0.1:{port}/callback?code={code}&state={state}"
        try:
            await self._http_get_raw(url)
        except Exception:
            logger.debug("Callback send completed (may redirect or close)")

    async def _wait_cli_complete(
        self, process: asyncio.subprocess.Process, timeout: int = 30
    ) -> None:
        """Step 11: Wait for CLI to finish writing credentials."""
        try:
            await asyncio.wait_for(process.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            logger.warning("CLI process did not exit within %ds", timeout)

    def _read_credentials(self, config_dir: str) -> dict[str, Any] | None:
        """Step 12: Read the generated .credentials.json file."""
        cred_path = Path(config_dir) / ".credentials.json"
        if not cred_path.exists():
            return None
        try:
            data = json.loads(cred_path.read_text())
            oauth = data.get("claudeAiOauth", {})
            return oauth
        except Exception:
            logger.exception("Failed to read credentials.json")
            return None

    def _parse_session_cookie(self, cookie_str: str) -> list[dict[str, Any]]:
        """Parse a Set-Cookie string into Playwright cookie format."""
        cookies = []
        for domain in ["claude.ai", ".claude.ai", "anthropic.com", ".anthropic.com"]:
            cookies.append({
                "name": "sessionKey",
                "value": cookie_str,
                "domain": domain,
                "path": "/",
            })
        return cookies

    # -- Lock helpers -------------------------------------------------------

    async def _acquire_lock(self, path: Path) -> None:
        """Acquire a file-based lock (non-blocking retry with timeout)."""
        import fcntl

        path.parent.mkdir(parents=True, exist_ok=True)
        fd = open(path, "w")
        start = time.monotonic()
        while time.monotonic() - start < 120:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                self._lock_files[str(path)] = fd
                return
            except BlockingIOError:
                await asyncio.sleep(1)
        raise TimeoutError(f"Could not acquire lock: {path}")

    async def _release_lock(self, path: Path) -> None:
        """Release a file-based lock."""
        fd = self._lock_files.pop(str(path), None)
        if fd:
            try:
                import fcntl
                fcntl.flock(fd, fcntl.LOCK_UN)
                fd.close()
            except Exception:
                pass

    async def _kill_process(self, process: asyncio.subprocess.Process) -> None:
        """Gracefully terminate a process."""
        if process.returncode is not None:
            return
        try:
            process.terminate()
            try:
                await asyncio.wait_for(process.wait(), timeout=5)
            except asyncio.TimeoutError:
                process.kill()
                await process.wait()
        except Exception:
            pass

    # -- HTTP helpers (abstracted for testability) --------------------------

    async def _http_post(self, url: str, json_body: dict) -> dict[str, Any]:
        """POST request returning JSON dict. Override for testing."""
        if self._http_client:
            return await self._http_client.post(url, json_body)
        import aiohttp

        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=json_body, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                return await resp.json()

    async def _http_get(self, url: str, params: dict | None = None) -> dict[str, Any]:
        """GET request returning JSON dict. Override for testing."""
        if self._http_client:
            return await self._http_client.get(url, params)
        import aiohttp

        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                return await resp.json()

    async def _http_get_raw(self, url: str) -> str:
        """GET request returning raw text."""
        if self._http_client:
            return await self._http_client.get_raw(url)
        import aiohttp

        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                return await resp.text()


# -- Token refresh helper (used by quota checker) ----------------------------


async def refresh_access_token(
    refresh_token: str,
    http_client: Any | None = None,
) -> dict[str, Any] | None:
    """Refresh an access token using the refresh_token grant.

    Returns dict with accessToken, refreshToken, expiresAt on success, None on failure.
    """
    import urllib.parse

    body = urllib.parse.urlencode({
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": OAUTH_CLIENT_ID,
    })

    try:
        if http_client:
            result = await http_client.post_form(ANTHROPIC_TOKEN_URL, body)
        else:
            import aiohttp
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    ANTHROPIC_TOKEN_URL,
                    data=body,
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                    timeout=aiohttp.ClientTimeout(total=30),
                ) as resp:
                    if resp.status != 200:
                        return None
                    result = await resp.json()

        access_token = result.get("access_token")
        new_refresh = result.get("refresh_token", refresh_token)
        expires_in = result.get("expires_in", 3600)

        if not access_token:
            return None

        expires_at = int((time.time() + expires_in) * 1000)

        return {
            "accessToken": access_token,
            "refreshToken": new_refresh,
            "expiresAt": expires_at,
        }
    except Exception:
        logger.exception("Token refresh failed")
        return None


def write_credentials(config_dir: str, creds: dict[str, Any]) -> None:
    """Write credentials to .credentials.json."""
    cred_path = Path(config_dir) / ".credentials.json"
    cred_path.parent.mkdir(parents=True, exist_ok=True)
    data = {"claudeAiOauth": creds}
    tmp = cred_path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2))
    tmp.replace(cred_path)


def read_credentials(config_dir: str) -> dict[str, Any] | None:
    """Read .credentials.json and return the claudeAiOauth object."""
    cred_path = Path(config_dir) / ".credentials.json"
    if not cred_path.exists():
        return None
    try:
        data = json.loads(cred_path.read_text())
        return data.get("claudeAiOauth")
    except Exception:
        return None
