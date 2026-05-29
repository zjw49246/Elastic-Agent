"""ClaudeOAuthProvider — OAuth auto-login on Worker via 171mail + Chrome CDP + xdotool.

Uses real Chrome + CDP for navigation/JS + xdotool for Cloudflare checkbox.
Calls authorize API directly via CDP fetch(), bypassing Turnstile entirely.

Flow:
  1. 171mail: trigger magic link email → poll for magic link URL
  2. Chrome CDP: visit magic link → xdotool clicks CF checkbox → get 6-digit code
  3. Chrome CDP: login page → enter email + code → establish session
  4. CLI: launch `claude auth login` → extract OAuth URL
  5. Chrome CDP: navigate to OAuth URL → xdotool clicks CF → call authorize API → code#state
  6. Feed code#state to CLI stdin → credentials written
"""

from __future__ import annotations

import asyncio
import itertools
import json
import logging
import os
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse, parse_qs

logger = logging.getLogger(__name__)

OAUTH_CLIENT_ID = "9d1c250a-e61b-44d9-88ed-5944d1962f5e"
MAIL_API_BASE = "https://b.171mail.com/api/v1"
ANTHROPIC_TOKEN_URL = "https://console.anthropic.com/v1/oauth/token"
ANTHROPIC_USAGE_URL = "https://api.anthropic.com/api/oauth/usage"
ANTHROPIC_ACCOUNT_URL = "https://claude.ai/api/account"

MAGIC_LINK_POLL_INTERVAL = 2.0
MAGIC_LINK_TIMEOUT = 90.0

CDP_PORT = 9222
_LOCAL_PORT_COUNTER = itertools.count(19222)
CHROME_PROFILE_DIR = "/tmp/chrome-cdp-oauth"

# Cloudflare "Verify you are human" checkbox position at 1365x900
CF_CHECKBOX_X = 257
CF_CHECKBOX_Y = 476

# JS to extract org UUID from React fiber tree
_JS_EXTRACT_ORG_UUID = """(function(){
var btn=[...document.querySelectorAll("button")].find(b=>b.textContent.trim()==="Authorize");
if(!btn)return null;
var fk=Object.keys(btn).find(k=>k.startsWith("__reactFiber"));
if(!fk)return null;
var c=btn[fk];
for(var i=0;i<30&&c;i++){
  if(c.memoizedState){
    var s=c.memoizedState;var x=0;
    while(s&&x<20){
      var v=s.memoizedState;
      if(v&&Array.isArray(v)){
        for(var it of v){
          if(it&&it.email_address)return(it.memberships&&it.memberships[0]&&it.memberships[0].organization)?it.memberships[0].organization.uuid:null;
          if(Array.isArray(it)){for(var sub of it){if(sub&&sub.email_address)return(sub.memberships&&sub.memberships[0]&&sub.memberships[0].organization)?sub.memberships[0].organization.uuid:null;}}
        }
      }
      s=s.next;x++;
    }
  }
  c=c.return;
}
return null;
})()"""

# JS to set an input value via React-compatible dispatch
_JS_SET_INPUT = """(function(){{
var inputs=[...document.querySelectorAll('input[type={type}]')].filter(i=>i.offsetParent!==null);
if(!inputs.length)return 'no {type} input';
var inp=inputs[0];
var s=Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype,'value').set;
s.call(inp,'{value}');
inp.dispatchEvent(new Event('input',{{bubbles:true}}));
inp.dispatchEvent(new Event('change',{{bubbles:true}}));
return 'set';
}})()"""

# JS to click a button by text content
_JS_CLICK_BTN = """(function(){{
var btns=[...document.querySelectorAll('button')].filter(b=>b.offsetParent!==null);
for(var b of btns){{var t=b.textContent.trim();if({condition}){{b.click();return 'clicked:'+t}}}}
return 'no match';
}})()"""

# JS to enter 6-digit verification code
_JS_ENTER_CODE = """(function(){{
var code="{code}";
var inputs=[...document.querySelectorAll('input')].filter(i=>i.offsetParent!==null);
if(inputs.length>=6){{
  for(var i=0;i<code.length&&i<inputs.length;i++){{
    inputs[i].focus();
    var s=Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype,'value').set;
    s.call(inputs[i],code[i]);
    inputs[i].dispatchEvent(new Event('input',{{bubbles:true}}));
    inputs[i].dispatchEvent(new Event('change',{{bubbles:true}}));
  }}
  return 'entered '+code.length+' digits';
}}
var inp=inputs.find(i=>i.type!=='email')||inputs[0];
if(inp){{var s=Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype,'value').set;s.call(inp,code);inp.dispatchEvent(new Event('input',{{bubbles:true}}));return 'entered single'}}
return 'no inputs';
}})()"""


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
    worker_host: str | None = None
    ssh_key_path: str = "/root/.ssh/elastic-agent-aliyun.pem"
    ssh_user: str = "root"


class _CDPClient:
    """Minimal async Chrome DevTools Protocol client over WebSocket."""

    def __init__(self, ws_url: str):
        self._ws_url = ws_url
        self._ws = None
        self._session = None
        self._id = 0
        self._pending: dict[int, asyncio.Future] = {}
        self._reader_task = None

    async def connect(self):
        import aiohttp
        self._session = aiohttp.ClientSession()
        self._ws = await self._session.ws_connect(self._ws_url, max_msg_size=10 * 1024 * 1024)
        self._reader_task = asyncio.create_task(self._reader())

    async def _reader(self):
        import aiohttp
        try:
            async for msg in self._ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    data = json.loads(msg.data)
                    mid = data.get("id")
                    if mid and mid in self._pending:
                        self._pending[mid].set_result(data)
                elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                    break
        except Exception:
            pass

    async def send(self, method: str, params: dict | None = None, timeout: float = 30) -> dict:
        self._id += 1
        msg_id = self._id
        future = asyncio.get_event_loop().create_future()
        self._pending[msg_id] = future
        await self._ws.send_json({"id": msg_id, "method": method, "params": params or {}})
        try:
            return await asyncio.wait_for(future, timeout=timeout)
        finally:
            self._pending.pop(msg_id, None)

    async def evaluate(self, expression: str, timeout: float = 30) -> Any:
        """Evaluate JS expression and return the result value."""
        r = await self.send("Runtime.evaluate", {
            "expression": expression,
            "awaitPromise": True,
            "returnByValue": True,
        }, timeout=timeout)
        exc = r.get("result", {}).get("exceptionDetails")
        if exc:
            logger.debug("CDP eval exception: %s", str(exc)[:200])
            return None
        return r.get("result", {}).get("result", {}).get("value")

    async def navigate(self, url: str) -> dict:
        return await self.send("Page.navigate", {"url": url})

    async def close(self):
        if self._reader_task:
            self._reader_task.cancel()
            try:
                await self._reader_task
            except (asyncio.CancelledError, Exception):
                pass
        if self._ws:
            await self._ws.close()
        if self._session:
            await self._session.close()


class ClaudeOAuthProvider:
    """Executes OAuth login flow on a Worker.

    Uses real Chrome + CDP for navigation/JS evaluation,
    xdotool for Cloudflare checkbox clicks,
    and direct authorize API call to bypass Turnstile.
    """

    def __init__(self, http_client: Any | None = None):
        self._http_client = http_client
        self._lock_files: dict[str, Any] = {}
        self._current_config: OAuthConfig | None = None

    async def login(self, config: OAuthConfig) -> LoginResult:
        """Execute the full OAuth login flow."""
        account_id = config.account_id
        logger.info("ClaudeOAuthProvider: starting login for %s", account_id)

        self._current_config = config
        chrome_process = None
        cli_process = None
        cdp = None
        lock_path = None

        try:
            # Step 1: File lock (remote if worker_host set)
            if config.worker_host:
                await self._ssh_exec(
                    config,
                    f"mkdir -p {config.config_dir}",
                    timeout=10,
                )
            else:
                lock_path = Path(config.config_dir) / f".login_lock_{account_id}"
                lock_path.parent.mkdir(parents=True, exist_ok=True)
                await self._acquire_lock(lock_path)

            # Step 2: Trigger magic link via 171mail
            send_result = await self._send_magic_link(config.email)
            if send_result.get("code") != 200:
                return LoginResult(
                    success=False,
                    account_id=account_id,
                    error=f"Failed to send magic link: {send_result.get('message', 'unknown')}",
                )
            logger.info("Magic link email triggered for %s", config.email)

            # Step 3: Poll for magic link URL
            magic_link = await self._poll_magic_link(config.email_token)
            if magic_link is None:
                return LoginResult(
                    success=False,
                    account_id=account_id,
                    error="Timed out waiting for magic link",
                )
            logger.info("Got magic link: %s...", magic_link[:60])

            # Step 4: Launch Chrome with CDP (on Worker if remote)
            chrome_process, local_port = await self._launch_chrome(config)
            cdp = await self._connect_cdp(local_port)
            await cdp.send("Page.enable")
            logger.info("Chrome + CDP connected")

            # Step 5: Visit magic link → click CF checkbox → get verification code
            await cdp.navigate(magic_link)
            await asyncio.sleep(3)
            cf_passed = await self._handle_cloudflare(cdp, "magic link")
            if not cf_passed:
                return LoginResult(
                    success=False, account_id=account_id,
                    error="Cloudflare challenge failed on magic link page",
                )

            await asyncio.sleep(5)
            url = await cdp.evaluate("document.location.href") or ""
            body = await cdp.evaluate("document.body?.innerText?.substring(0, 500)") or ""
            logger.info("After magic link — URL: %s, body: %s", url[:80], body[:120])

            # Check if magic link auto-logged us in (redirected away from magic-link/login page)
            parsed_path = urlparse(url).path.rstrip("/")
            session_established = parsed_path in ("/new", "/chat", "/recents", "") and "magic-link" not in url

            if not session_established:
                # Magic link showed verification code — need to enter it on login page
                verify_code = None
                m = re.search(r'(\d{6})', body)
                if m:
                    verify_code = m.group(1)
                if not verify_code:
                    return LoginResult(
                        success=False, account_id=account_id,
                        error="No 6-digit verification code on magic link page",
                    )
                logger.info("Verification code: %s", verify_code)

                # Navigate to login page
                await cdp.navigate("https://claude.ai/login")
                await asyncio.sleep(3)
                await self._handle_cloudflare(cdp, "login")
                await asyncio.sleep(5)

                # Enter email
                r = await cdp.evaluate(_JS_SET_INPUT.format(type="email", value=config.email))
                logger.info("Email input: %s", r)
                await asyncio.sleep(0.5)

                r = await cdp.evaluate(
                    _JS_CLICK_BTN.format(condition="t.includes('Continue with email')")
                )
                logger.info("Email button: %s", r)
                await asyncio.sleep(2)

                r = await cdp.evaluate(
                    _JS_CLICK_BTN.format(condition="b.type==='submit'||t.includes('Continue')")
                )
                logger.info("Submit: %s", r)
                await asyncio.sleep(8)

                url = await cdp.evaluate("document.location.href") or ""
                body = await cdp.evaluate("document.body?.innerText?.substring(0, 200)") or ""
                logger.info("After email submit — URL: %s, body: %s", url[:80], body[:80])

                # Enter verification code if on code page
                if any(w in body.lower() for w in ("verification", "code", "check")):
                    r = await cdp.evaluate(_JS_ENTER_CODE.format(code=verify_code))
                    logger.info("Code entry: %s", r)
                    await asyncio.sleep(1)

                    r = await cdp.evaluate(
                        _JS_CLICK_BTN.format(
                            condition="!t.includes('Google')&&!t.includes('SSO')&&(t.includes('Verify')||t.includes('Continue')||t.includes('Submit')||b.type==='submit')"
                        )
                    )
                    logger.info("Verify button: %s", r)
                    await asyncio.sleep(10)
            else:
                logger.info("Magic link auto-login succeeded, session already established")

            # Step 7: Launch CLI auth login
            cli_process = await self._launch_cli_auth(config)

            # Step 8: Extract OAuth authorize URL
            authorize_url = await self._extract_authorize_url(cli_process, timeout=30)
            if authorize_url is None:
                return LoginResult(
                    success=False, account_id=account_id,
                    error="Failed to extract OAuth authorize URL from CLI",
                )
            logger.info("OAuth URL: %s...", authorize_url[:80])

            # Step 9: Navigate to OAuth URL → click CF → call authorize API
            await cdp.navigate(authorize_url)
            await asyncio.sleep(3)
            await self._handle_cloudflare(cdp, "OAuth")
            await asyncio.sleep(8)

            body = await cdp.evaluate("document.body?.innerText?.substring(0, 100)")
            logger.info("OAuth page: %s", (body or "")[:80])

            # Step 10: Extract org UUID and call authorize API directly
            code_state = await self._cdp_authorize(cdp, authorize_url)
            if code_state is None:
                return LoginResult(
                    success=False, account_id=account_id,
                    error="CDP authorize failed",
                )
            logger.info("Got code#state (%d chars)", len(code_state))

            # Step 11: Feed code#state to CLI stdin
            cli_process.stdin.write(f"{code_state}\n".encode())
            await cli_process.stdin.drain()
            cli_process.stdin.close()

            # Step 12: Wait for CLI to complete
            await self._wait_cli_complete(cli_process, timeout=30)

            # Step 13: Verify login
            creds = await self._read_credentials(config)
            if creds is None:
                return LoginResult(
                    success=False, account_id=account_id,
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
                success=False, account_id=account_id, error="Login timed out",
            )
        except Exception as e:
            logger.exception("ClaudeOAuthProvider: login failed for %s", account_id)
            return LoginResult(
                success=False, account_id=account_id, error=str(e),
            )
        finally:
            if cdp:
                await cdp.close()
            if cli_process:
                await self._kill_process(cli_process)
            if chrome_process:
                await self._kill_chrome(chrome_process)
            if lock_path:
                await self._release_lock(lock_path)
            self._current_config = None

    # -- 171mail helpers ----------------------------------------------------

    async def _send_magic_link(self, email: str) -> dict[str, Any]:
        """Trigger magic link email via 171mail."""
        return await self._http_post(
            f"{MAIL_API_BASE}/claude/send",
            json_body={"email": email},
        )

    async def _poll_magic_link(self, email_token: str) -> str | None:
        """Poll 171mail inbox for magic link URL."""
        start = time.monotonic()
        while time.monotonic() - start < MAGIC_LINK_TIMEOUT:
            result = await self._http_get(
                f"{MAIL_API_BASE}/getClaudeMessage",
                params={"token": email_token},
            )
            result_data = result.get("data") or {}
            raw = result_data.get("code") or result_data.get("link") or result_data.get("magicLink") or ""

            if raw.startswith("https://"):
                return raw
            body = result_data.get("body") or raw
            match = re.search(r'https://claude\.ai/magic-link#\S+', body)
            if match:
                return match.group(0)

            await asyncio.sleep(MAGIC_LINK_POLL_INTERVAL)
        return None

    # -- Chrome + CDP methods -----------------------------------------------

    def _ssh_opts(self, config: OAuthConfig) -> list[str]:
        """Common SSH options for connecting to Worker."""
        return [
            "-o", "StrictHostKeyChecking=no",
            "-o", "UserKnownHostsFile=/dev/null",
            "-o", "ConnectTimeout=10",
            "-o", "LogLevel=ERROR",
            "-i", config.ssh_key_path,
        ]

    async def _ssh_exec(self, config: OAuthConfig, cmd: str, timeout: int = 30) -> str:
        """Run a command on the Worker via SSH and return stdout."""
        proc = await asyncio.create_subprocess_exec(
            "ssh", *self._ssh_opts(config),
            f"{config.ssh_user}@{config.worker_host}", cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        return stdout.decode().strip()

    async def _launch_chrome(self, config: OAuthConfig) -> tuple[asyncio.subprocess.Process, int]:
        """Launch Chrome on the Worker via SSH and set up a CDP port tunnel.

        Returns (tunnel_process, local_cdp_port).
        """
        host = config.worker_host
        account_id = config.account_id

        # Stop any existing Chrome service on the Worker
        try:
            await self._ssh_exec(
                config,
                "systemctl stop chrome-oauth 2>/dev/null; systemctl reset-failed chrome-oauth 2>/dev/null; pkill -x chrome 2>/dev/null; true",
                timeout=10,
            )
        except Exception:
            pass

        profile_dir = f"{CHROME_PROFILE_DIR}-{account_id}"

        # Write systemd service file for Chrome on the Worker
        service_content = (
            "[Unit]\n"
            "Description=Chrome CDP for OAuth\n"
            "[Service]\n"
            "Type=simple\n"
            "Environment=DISPLAY=:99\n"
            "Environment=HOME=/root\n"
            f"ExecStart=/usr/bin/google-chrome "
            f"--no-sandbox --disable-gpu --disable-software-rasterizer "
            f"--no-first-run --no-default-browser-check "
            f"--disable-extensions --disable-component-extensions-with-background-pages "
            f"--window-size=1365,900 "
            f"--remote-debugging-port={CDP_PORT} "
            f"--user-data-dir={profile_dir} "
            f"about:blank\n"
            "Restart=no\n"
        )
        # SCP the service file to Worker, then start it
        import tempfile
        with tempfile.NamedTemporaryFile(mode="w", suffix=".service", delete=False) as f:
            f.write(service_content)
            tmp_path = f.name

        try:
            scp_proc = await asyncio.create_subprocess_exec(
                "scp", *self._ssh_opts(config),
                tmp_path,
                f"{config.ssh_user}@{host}:/etc/systemd/system/chrome-oauth.service",
                stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
            )
            await asyncio.wait_for(scp_proc.wait(), timeout=15)
        finally:
            Path(tmp_path).unlink(missing_ok=True)

        launch_cmd = (
            f"mkdir -p {profile_dir} && "
            f"systemctl daemon-reload && "
            f"systemctl start chrome-oauth && "
            f"sleep 4 && "
            f"curl -s http://127.0.0.1:{CDP_PORT}/json >/dev/null 2>&1 && echo CHROME_OK || echo CHROME_FAIL"
        )
        result = await self._ssh_exec(config, launch_cmd, timeout=30)
        logger.info("Chrome launch on Worker %s: %s", host, result)

        local_port = next(_LOCAL_PORT_COUNTER)

        # Set up SSH tunnel: local local_port → Worker CDP_PORT
        tunnel = await asyncio.create_subprocess_exec(
            "ssh", *self._ssh_opts(config),
            "-N", "-L", f"{local_port}:127.0.0.1:{CDP_PORT}",
            f"{config.ssh_user}@{host}",
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        await asyncio.sleep(2)
        logger.info("SSH tunnel established: localhost:%d → %s:%d", local_port, host, CDP_PORT)
        return tunnel, local_port

    async def _connect_cdp(self, local_port: int = CDP_PORT) -> _CDPClient:
        """Connect to Chrome via CDP WebSocket."""
        import aiohttp
        port = local_port
        async with aiohttp.ClientSession() as session:
            for attempt in range(10):
                try:
                    async with session.get(f"http://127.0.0.1:{port}/json") as resp:
                        tabs = await resp.json()
                    page_tab = next((t for t in tabs if t.get("type") == "page"), None)
                    if page_tab:
                        ws_url = page_tab["webSocketDebuggerUrl"]
                        if port != CDP_PORT:
                            ws_url = ws_url.replace(f":{CDP_PORT}/", f":{port}/")
                        cdp = _CDPClient(ws_url)
                        await cdp.connect()
                        return cdp
                except Exception:
                    pass
                await asyncio.sleep(1)
        raise RuntimeError("Could not connect to Chrome CDP")

    async def _handle_cloudflare(self, cdp: _CDPClient, context: str, timeout: int = 30) -> bool:
        """Detect and handle Cloudflare challenge by clicking checkbox with xdotool."""
        start = time.monotonic()
        while time.monotonic() - start < timeout:
            title = await cdp.evaluate("document.title") or ""
            body_snippet = await cdp.evaluate("document.body?.innerText?.substring(0, 100)") or ""

            if "just a moment" not in title.lower() and "security verification" not in body_snippet.lower():
                logger.info("CF cleared for %s", context)
                return True

            logger.info("CF challenge detected on %s, clicking checkbox at (%d,%d)...",
                        context, CF_CHECKBOX_X, CF_CHECKBOX_Y)
            await self._xdotool_click(CF_CHECKBOX_X, CF_CHECKBOX_Y)
            await asyncio.sleep(5)

        logger.error("CF challenge did not clear for %s within %ds", context, timeout)
        return False

    async def _xdotool_click(self, x: int, y: int) -> None:
        """Click at screen coordinates using xdotool on the Worker."""
        if self._current_config and self._current_config.worker_host:
            await self._ssh_exec(
                self._current_config,
                f"DISPLAY=:99 xdotool mousemove {x} {y} click 1",
                timeout=10,
            )
        else:
            proc = await asyncio.create_subprocess_exec(
                "xdotool", "mousemove", str(x), str(y), "click", "1",
                env={**os.environ, "DISPLAY": ":99"},
                stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
            )
            await proc.wait()

    async def _cdp_authorize(self, cdp: _CDPClient, authorize_url: str) -> str | None:
        """Extract org UUID and call authorize API directly via CDP fetch()."""
        # Parse OAuth URL parameters
        parsed = urlparse(authorize_url.replace("claude.com/cai/", "claude.ai/"))
        params = {k: v[0] for k, v in parse_qs(parsed.query).items()}

        # Extract org UUID from React fiber tree
        org_uuid = await cdp.evaluate(_JS_EXTRACT_ORG_UUID)
        logger.info("Org UUID: %s", org_uuid)

        if not org_uuid:
            logger.error("Could not extract org UUID")
            return None

        # Build authorize API request
        scope = " ".join(
            s for s in params.get("scope", "").split(" ")
            if s != "org:create_api_key"
        )
        api_body = json.dumps({
            "response_type": params.get("response_type", "code"),
            "client_id": params.get("client_id", OAUTH_CLIENT_ID),
            "organization_uuid": org_uuid,
            "redirect_uri": params.get("redirect_uri", ""),
            "scope": scope,
            "state": params.get("state", ""),
            "code_challenge": params.get("code_challenge", ""),
            "code_challenge_method": params.get("code_challenge_method", "S256"),
        })

        # Call authorize API from Chrome context (uses Chrome's session cookies)
        js_fetch = f"""(async function(){{
var r=await fetch("/v1/oauth/{org_uuid}/authorize",{{
  method:"POST",
  headers:{{"Content-Type":"application/json","Accept":"application/json"}},
  credentials:"include",
  body:{json.dumps(api_body)}
}});
return r.status+" | "+await r.text()
}})()"""

        api_result = await cdp.evaluate(js_fetch)
        logger.info("Authorize API: %s", (api_result or "")[:150])

        if not api_result or not api_result.startswith("200"):
            logger.error("Authorize API failed: %s", api_result)
            return None

        try:
            _, response_text = api_result.split(" | ", 1)
            response_data = json.loads(response_text)
            redirect_uri = response_data.get("redirect_uri", "")
            cb = urlparse(redirect_uri)
            cb_params = parse_qs(cb.query)
            code = cb_params.get("code", [""])[0]
            state = cb_params.get("state", [""])[0]
            if code and state:
                return f"{code}#{state}"
            logger.error("Missing code/state in redirect_uri: %s", redirect_uri[:100])
        except Exception as e:
            logger.error("Failed to parse authorize response: %s", e)

        return None

    # -- CLI methods --------------------------------------------------------

    async def _launch_cli_auth(self, config: OAuthConfig) -> asyncio.subprocess.Process:
        """Launch Claude CLI auth login on the Worker via SSH."""
        if config.worker_host:
            process = await asyncio.create_subprocess_exec(
                "ssh", *self._ssh_opts(config),
                f"{config.ssh_user}@{config.worker_host}",
                f"CLAUDE_CONFIG_DIR={config.config_dir} claude auth login",
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        else:
            env = {
                "CLAUDE_CONFIG_DIR": config.config_dir,
                "PATH": "/usr/local/bin:/usr/bin:/bin",
                "HOME": str(Path.home()),
            }
            process = await asyncio.create_subprocess_exec(
                "claude", "auth", "login",
                env=env,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        return process

    async def _extract_authorize_url(
        self, process: asyncio.subprocess.Process, timeout: int = 30
    ) -> str | None:
        """Read CLI stdout for the OAuth authorize URL."""
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
            match = re.search(r"(https://claude\.(?:ai|com)/\S*oauth/authorize\S+)", buffer)
            if match:
                return match.group(1)
        return None

    async def _wait_cli_complete(
        self, process: asyncio.subprocess.Process, timeout: int = 30
    ) -> None:
        """Wait for CLI to finish writing credentials."""
        try:
            await asyncio.wait_for(process.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            logger.warning("CLI process did not exit within %ds", timeout)

    async def _read_credentials(self, config: OAuthConfig) -> dict[str, Any] | None:
        """Read the generated .credentials.json file from the Worker."""
        cred_path = f"{config.config_dir}/.credentials.json"
        if config.worker_host:
            try:
                raw = await self._ssh_exec(
                    config, f"cat {cred_path} 2>/dev/null", timeout=10
                )
                if not raw:
                    return None
                data = json.loads(raw)
                return data.get("claudeAiOauth", {})
            except Exception:
                logger.exception("Failed to read credentials from Worker")
                return None
        else:
            p = Path(cred_path)
            if not p.exists():
                return None
            try:
                data = json.loads(p.read_text())
                return data.get("claudeAiOauth", {})
            except Exception:
                logger.exception("Failed to read credentials.json")
                return None

    # -- Chrome cleanup -----------------------------------------------------

    async def _kill_chrome(self, chrome_process: asyncio.subprocess.Process) -> None:
        """Kill Chrome on Worker and close SSH tunnel."""
        await self._kill_process(chrome_process)
        if self._current_config and self._current_config.worker_host:
            try:
                await self._ssh_exec(
                    self._current_config,
                    f"systemctl stop chrome-oauth 2>/dev/null; pkill -x chrome 2>/dev/null; true",
                    timeout=10,
                )
            except Exception:
                pass
        else:
            try:
                proc = await asyncio.create_subprocess_exec(
                    "pkill", "-f", f"chrome.*{CDP_PORT}",
                    stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
                )
                await proc.wait()
            except Exception:
                pass

    # -- Lock helpers -------------------------------------------------------

    async def _acquire_lock(self, path: Path) -> None:
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
        fd = self._lock_files.pop(str(path), None)
        if fd:
            try:
                import fcntl
                fcntl.flock(fd, fcntl.LOCK_UN)
                fd.close()
            except Exception:
                pass

    async def _kill_process(self, process: asyncio.subprocess.Process) -> None:
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

    # -- HTTP helpers -------------------------------------------------------

    async def _http_post(self, url: str, json_body: dict) -> dict[str, Any]:
        if self._http_client:
            return await self._http_client.post(url, json_body)
        import aiohttp
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=json_body, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                return await resp.json()

    async def _http_get(self, url: str, params: dict | None = None) -> dict[str, Any]:
        if self._http_client:
            return await self._http_client.get(url, params)
        import aiohttp
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                return await resp.json()

    async def _http_get_raw(self, url: str) -> str:
        if self._http_client:
            return await self._http_client.get_raw(url)
        import aiohttp
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                return await resp.text()


# -- Token refresh helper ---------------------------------------------------


async def refresh_access_token(
    refresh_token: str,
    http_client: Any | None = None,
) -> dict[str, Any] | None:
    """Refresh an access token using the refresh_token grant."""
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
