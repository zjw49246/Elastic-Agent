"""Chrome CDP 登录模块（从 auto_login.py 调用）。"""
import asyncio
import json
import logging
import os
import re
import select
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import httpx
import websockets

from elastic_agent.core.claude_oauth import ClaudeLoginCleanupError
from elastic_agent.core.secure_store import fsync_directory

for _logger_name in ("httpx", "httpcore"):
    # Mailbox tokens are query parameters.  Keep complete request URLs out of
    # ea-runtime's journal for the lifetime of the worker process.
    logging.getLogger(_logger_name).setLevel(logging.WARNING)

# Mail relay for mail.com-family accounts (magic-link 接码). Override per
# deployment via CLAUDE_MAILCATCHER_URL; only used for the mailcom backend.
MAILCATCHER = os.environ.get("CLAUDE_MAILCATCHER_URL", "https://mail.claude-code-manager.com")
CDP_PORT = 9222


def _terminate_process_group_sync(
    proc: subprocess.Popen,
    *,
    grace_seconds: float = 5.0,
) -> None:
    """Terminate and reap one login-owned POSIX process group."""

    def group_has_live_members(pgid: int) -> bool:
        if os.name != "posix" or not Path("/proc").exists():
            return proc.poll() is None
        for stat_path in Path("/proc").glob("[0-9]*/stat"):
            try:
                stat_text = stat_path.read_text()
                fields = stat_text[stat_text.rfind(")") + 2 :].split()
                state = fields[0]
                process_group = int(fields[2])
            except (OSError, ValueError, IndexError):
                continue
            if process_group == pgid and state != "Z":
                return True
        return False

    pgid = proc.pid
    try:
        if os.name == "posix":
            os.killpg(pgid, signal.SIGTERM)
        else:  # pragma: no cover - workers are Linux
            proc.terminate()
    except ProcessLookupError:
        pass
    deadline = time.monotonic() + grace_seconds
    while group_has_live_members(pgid) and time.monotonic() < deadline:
        time.sleep(0.05)
    try:
        if group_has_live_members(pgid):
            if os.name == "posix":
                os.killpg(pgid, signal.SIGKILL)
            else:  # pragma: no cover - workers are Linux
                proc.kill()
    except ProcessLookupError:
        pass
    kill_deadline = time.monotonic() + grace_seconds
    while group_has_live_members(pgid) and time.monotonic() < kill_deadline:
        time.sleep(0.05)
    try:
        proc.wait(timeout=max(0.1, grace_seconds))
    except (subprocess.TimeoutExpired, ChildProcessError):
        pass
    if group_has_live_members(pgid):
        raise RuntimeError("login subprocess group did not terminate")


async def _cleanup_tracked_processes(processes: list[subprocess.Popen]) -> None:
    """Run blocking Popen waits outside the event loop, children before Chrome."""

    failures: list[Exception] = []
    for proc in reversed(processes):
        try:
            await asyncio.to_thread(_terminate_process_group_sync, proc)
        except Exception as exc:
            # A stuck CLI must not prevent us from attempting to terminate
            # Chrome (or any other independently tracked process group).
            failures.append(exc)
    if failures:
        raise ClaudeLoginCleanupError(
            f"{len(failures)} Claude login process group(s) did not terminate"
        ) from failures[0]

async def cdp_eval(ws, expr, timeout=10):
    mid = int(time.time()*1000) % 100000
    await ws.send(json.dumps({"id": mid, "method": "Runtime.evaluate",
        "params": {"expression": expr, "returnByValue": True, "awaitPromise": True}}))
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=2)
            msg = json.loads(raw)
            if msg.get("id") == mid:
                return msg.get("result", {}).get("result", {}).get("value")
        except asyncio.TimeoutError:
            continue
    return None

def _ensure_display(env: dict) -> dict:
    """Ensure DISPLAY is set for Xvfb."""
    if not env.get("DISPLAY"):
        return {**env, "DISPLAY": ":99"}
    return env

def _write_private_debug_file(path: str | Path, payload: bytes) -> None:
    """Atomically publish one new 0600 debug artifact without following links."""

    destination = Path(path)
    directory = destination.parent
    if directory.is_symlink() or not directory.is_dir():
        raise RuntimeError("Claude login debug directory is not a private directory")
    os.chmod(directory, 0o700)
    if destination.exists() or destination.is_symlink():
        raise RuntimeError("Claude login debug artifact already exists")

    fd, temporary_name = tempfile.mkstemp(
        prefix=f".{destination.name}.",
        suffix=".tmp",
        dir=directory,
    )
    temporary = Path(temporary_name)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "wb") as stream:
            fd = -1
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        # os.replace replaces a raced symlink entry itself; it never follows the
        # link to an attacker-chosen target.
        os.replace(temporary, destination)
        fsync_directory(directory)
    finally:
        if fd >= 0:
            os.close(fd)
        temporary.unlink(missing_ok=True)


def _create_login_debug_directory() -> Path:
    """Create an unpredictable 0700 directory retained for explicit debugging."""

    directory = Path(tempfile.mkdtemp(prefix="elastic-agent-login-debug-"))
    os.chmod(directory, 0o700)
    return directory


async def cdp_screenshot(ws, path, timeout=10):
    """Optionally save a private screenshot for an explicitly enabled debug run."""
    if os.environ.get("ELASTIC_AGENT_LOGIN_DEBUG_SCREENSHOTS") != "1":
        return
    import base64
    mid = int(time.time() * 1000) % 100000 + 7
    await ws.send(json.dumps({"id": mid, "method": "Page.captureScreenshot",
        "params": {"format": "png"}}))
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=2)
            msg = json.loads(raw)
            if msg.get("id") == mid:
                data = msg.get("result", {}).get("data")
                if data:
                    _write_private_debug_file(path, base64.b64decode(data))
                    print(f"  Screenshot saved: {path}")
                return
        except asyncio.TimeoutError:
            continue
    print(f"  Screenshot timeout: {path}")

async def xdotool_click(x, y):
    p = await asyncio.create_subprocess_exec("xdotool", "mousemove", str(x), str(y), "click", "1",
        env=_ensure_display(dict(os.environ)), stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
    await p.wait()

async def handle_cf(ws, ctx, timeout=60):
    start = time.time()
    while time.time() - start < timeout:
        title = await cdp_eval(ws, "document.title") or ""
        if "just a moment" not in title.lower():
            print(f"  CF cleared: {ctx}")
            return True
        print(f"  CF challenge: {ctx}, clicking...")
        await xdotool_click(257, 476)
        await asyncio.sleep(5)
    return False

async def _cdp_login_impl(
    email: str,
    token: str,
    config_dir: str,
    oauth_url: str = "",
    cookies_171: list[dict] | None = None,
    magic_link: str | None = None,
    *,
    _processes: list[subprocess.Popen],
) -> dict | None:
    """Chrome CDP 登录全流程。

    magic_link: 171mail 预取的 magic link，有则直接导航，无则走 MailCatcher 接码。
    cookies_171: 已废弃，保留参数兼容但不使用。
    """
    debug_directory: Path | None = None
    if os.environ.get("ELASTIC_AGENT_LOGIN_DEBUG_SCREENSHOTS") == "1":
        debug_directory = _create_login_debug_directory()
        # Explicit debug artifacts are retained in this private, unpredictable
        # directory until the ephemeral worker (or /tmp cleanup) removes them.
        print(f"  Login debug directory: {debug_directory}")

    # 1. Kill old chrome and clean profile
    subprocess.run(["pkill", "-f", "chrome.*remote-debugging"], capture_output=True)
    await asyncio.sleep(2)
    subprocess.run(["pkill", "-9", "-f", "chrome.*remote-debugging"], capture_output=True)
    await asyncio.sleep(1)
    shutil.rmtree("/tmp/chrome-test-login", ignore_errors=True)

    # 2. Launch Chrome (fresh profile)
    # --disable-dev-shm-usage 必带：小机型（t3.medium 等）/dev/shm 太小，
    # 不加会让渲染进程因共享内存不足直接崩溃 → CDP 9222 端口起不来，
    # 后面连 http://127.0.0.1:9222/json 报 ConnectError（登录整段失败）。
    chrome_env = _ensure_display(dict(os.environ))
    chrome = subprocess.Popen(["google-chrome", "--no-sandbox", "--disable-gpu",
        "--disable-dev-shm-usage", "--disable-software-rasterizer",
        "--no-first-run", "--disable-extensions", "--window-size=1365,900",
        f"--remote-debugging-port={CDP_PORT}", "--user-data-dir=/tmp/chrome-test-login",
        "about:blank"], stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL, env=chrome_env, start_new_session=True)
    _processes.append(chrome)
    print(f"Chrome pid={chrome.pid}")

    # 3. Connect CDP (poll until ready)
    tabs = None
    for _attempt in range(15):
        await asyncio.sleep(2)
        if chrome.poll() is not None:
            print(f"  Chrome exited early (code={chrome.returncode})")
            return None
        try:
            async with httpx.AsyncClient() as c:
                r = await c.get(f"http://127.0.0.1:{CDP_PORT}/json", timeout=3)
                tabs = r.json()
                break
        except Exception:
            pass
    if not tabs:
        print("  Chrome CDP not ready after 30s")
        chrome.kill()
        return None
    ws_url = next(t["webSocketDebuggerUrl"] for t in tabs if t["type"] == "page")

    try:
        async with websockets.connect(ws_url, max_size=10_000_000) as ws:
            await ws.send(json.dumps({"id": 1, "method": "Page.enable"}))
            await ws.send(json.dumps({"id": 0, "method": "Network.enable"}))
            await asyncio.sleep(0.5)

            # 4. Login page
            await ws.send(json.dumps({"id": 2, "method": "Page.navigate", "params": {"url": "https://claude.ai/login"}}))
            await asyncio.sleep(3)
            await handle_cf(ws, "login")
            await asyncio.sleep(2)

            # 5. Enter email
            JS_SET = """(function(){{var inputs=[...document.querySelectorAll('input[type={type}]')].filter(i=>i.offsetParent!==null);if(!inputs.length)return 'no input';var inp=inputs[0];var s=Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype,'value').set;s.call(inp,'{value}');inp.dispatchEvent(new Event('input',{{bubbles:true}}));inp.dispatchEvent(new Event('change',{{bubbles:true}}));return 'set'}})()"""
            JS_BTN = """(function(){{var btns=[...document.querySelectorAll('button')].filter(b=>b.offsetParent!==null);for(var b of btns){{var t=b.textContent.trim();if({cond}){{b.click();return 'clicked:'+t}}}}return 'no match'}})()"""
            r = await cdp_eval(ws, JS_SET.format(type="email", value=email))
            print(f"  Email: {r}")
            await asyncio.sleep(0.5)
            r = await cdp_eval(ws, JS_BTN.format(cond="t.includes('Continue with email')"))
            print(f"  Button: {r}")
            await asyncio.sleep(3)

            # 6. Get magic link
            if magic_link:
                # 171mail 已预取 magic link
                print(f"  Using pre-fetched magic link ({len(magic_link)} chars)")
                link = magic_link
            else:
                # mail.com 路径：poll MailCatcher
                send_ts = time.time()
                print("  Polling MailCatcher...")
                link = None
                async with httpx.AsyncClient(timeout=30, headers={"User-Agent": "Mozilla/5.0"}) as mc:
                    deadline = time.time() + 120
                    while time.time() < deadline:
                        r = await mc.get(f"{MAILCATCHER}/api/v1/message", params={"token": token, "type": "claude"})
                        d = r.json().get("data", {})
                        subj = d.get("subject", "")
                        code = d.get("code", "")
                        if code.startswith("http") and subj:
                            m = re.search(r"\|\s+(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", subj)
                            if m:
                                t = time.mktime(time.strptime(m.group(1), "%Y-%m-%d %H:%M:%S"))
                                if t >= send_ts - 10:
                                    link = code
                                    print(f"  Got magic link ({len(link)} chars)")
                                    break
                        await asyncio.sleep(2)
                if not link:
                    print("  TIMEOUT waiting for magic link")
                    return None

            # 7. Navigate magic link
            await ws.send(json.dumps({"id": 3, "method": "Page.navigate", "params": {"url": link}}))
            await asyncio.sleep(3)
            await handle_cf(ws, "magic-link")
            for _ in range(15):
                url = await cdp_eval(ws, "document.location.href") or ""
                if "magic-link" not in url: break
                await asyncio.sleep(2)
            await asyncio.sleep(3)
            current_location = await cdp_eval(ws, "document.location.href") or ""
            print(f"  Magic-link navigation complete: {bool(current_location)}")
            ml_text = await cdp_eval(ws, "document.body?.innerText?.substring(0,700)") or ""
            print(f"  Magic-link page rendered ({len(ml_text)} chars)")
            if debug_directory is not None:
                await cdp_screenshot(
                    ws,
                    debug_directory / "magiclink.png",
                )

            # 8. Launch CLI
            cli = subprocess.Popen(["claude", "auth", "login", "--email", email],
                env={"CLAUDE_CONFIG_DIR": config_dir, "PATH": os.environ.get("PATH", "/usr/local/bin:/usr/bin:/bin"), "HOME": os.environ.get("HOME", str(Path.home())), "NO_COLOR": "1", "TERM": "dumb"},
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                bufsize=0, start_new_session=True)
            _processes.append(cli)
            print(f"  CLI pid={cli.pid}")

            # Read OAuth URL
            oauth_url = None
            captured = b""
            dl = time.time() + 15
            while time.time() < dl:
                if cli.poll() is not None: break
                rl, _, _ = select.select([cli.stdout], [], [], 0.2)
                if rl:
                    try: captured += os.read(cli.stdout.fileno(), 8192)
                    except: break
                m = re.search(rb"(https://claude\.com/cai/oauth/authorize\S+)", captured)
                if m: oauth_url = m.group(1).decode(); break
                await asyncio.sleep(0.1)
            if not oauth_url:
                cli.kill(); print("  NO OAuth URL"); return None
            print(f"  OAuth URL ({len(oauth_url)} chars)")

            # 9. Navigate to OAuth URL
            await ws.send(json.dumps({"id": 10, "method": "Page.navigate", "params": {"url": oauth_url}}))
            await asyncio.sleep(3)
            await handle_cf(ws, "OAuth")
            await asyncio.sleep(8)

            # 10. Authorize API (React fiber 提取 org UUID + 直接 POST)
            JS_ORG = """(function(){var btn=[...document.querySelectorAll("button")].find(b=>b.textContent.trim()==="Authorize");if(!btn)return null;var fk=Object.keys(btn).find(k=>k.startsWith("__reactFiber"));if(!fk)return null;var c=btn[fk];for(var i=0;i<30&&c;i++){if(c.memoizedState){var s=c.memoizedState;var x=0;while(s&&x<20){var v=s.memoizedState;if(v&&Array.isArray(v)){for(var it of v){if(it&&it.email_address)return(it.memberships&&it.memberships[0]&&it.memberships[0].organization)?it.memberships[0].organization.uuid:null;if(Array.isArray(it)){for(var sub of it){if(sub&&sub.email_address)return(sub.memberships&&sub.memberships[0]&&sub.memberships[0].organization)?sub.memberships[0].organization.uuid:null;}}}}s=s.next;x++;}}c=c.return;}return null;})()"""
            code, state = None, None
            org = None
            for _retry in range(5):
                if _retry == 0:
                    page_text = await cdp_eval(ws, "document.body?.innerText?.substring(0,500)") or ""
                    print(f"  OAuth page rendered ({len(page_text)} chars)")
                    if debug_directory is not None:
                        await cdp_screenshot(
                            ws,
                            debug_directory / "oauth.png",
                        )
                org = await cdp_eval(ws, JS_ORG)
                if org:
                    break
                await asyncio.sleep(3)
            print(f"  Organization context found: {bool(org)}")
            if org:
                params = {k:v[0] for k,v in parse_qs(urlparse(oauth_url).query).items()}
                scope = " ".join(s for s in params.get("scope","").split() if s != "org:create_api_key")
                body = json.dumps({"response_type":"code","client_id":params.get("client_id",""),"organization_uuid":org,"redirect_uri":params.get("redirect_uri",""),"scope":scope,"state":params.get("state",""),"code_challenge":params.get("code_challenge",""),"code_challenge_method":"S256"})
                js = f"""(async function(){{var r=await fetch("/v1/oauth/{org}/authorize",{{method:"POST",headers:{{"Content-Type":"application/json","Accept":"application/json"}},credentials:"include",body:{json.dumps(body)}}});return r.status+" | "+await r.text()}})()"""
                result = await cdp_eval(ws, js, timeout=15)
                print(f"  Authorize response received: {bool(result)}")
                if result and result.startswith("200"):
                    _, txt = result.split(" | ", 1)
                    rd = json.loads(txt).get("redirect_uri","")
                    cp = parse_qs(urlparse(rd).query)
                    code = cp.get("code", [""])[0]
                    state = cp.get("state", [""])[0]

            # Fallback: if React fiber extraction failed, try alternative org extraction then click
            if not org:
                print("  Fallback: trying alternative org extraction...")
                # Try getting org UUID from page URL or network requests
                JS_ALT_ORG = """(function(){
                    // Try from URL params
                    var u = new URL(window.location.href);
                    // Try from any visible org info on page
                    var text = document.body?.innerText || "";
                    // Try extracting from React root props
                    var root = document.getElementById("__next") || document.getElementById("root");
                    if (root) {
                        var fk = Object.keys(root).find(k => k.startsWith("__reactFiber") || k.startsWith("__reactContainer"));
                        if (fk) {
                            var node = root[fk];
                            var seen = new Set();
                            function walk(n, depth) {
                                if (!n || depth > 50 || seen.has(n)) return null;
                                seen.add(n);
                                if (n.memoizedProps) {
                                    var p = n.memoizedProps;
                                    if (p.organization && p.organization.uuid) return p.organization.uuid;
                                    if (p.organizationUuid) return p.organizationUuid;
                                }
                                if (n.memoizedState) {
                                    var s = n.memoizedState;
                                    for (var i = 0; i < 30 && s; i++) {
                                        var v = s.memoizedState;
                                        if (v && typeof v === 'object' && v.uuid && v.name) return v.uuid;
                                        s = s.next;
                                    }
                                }
                                return walk(n.child, depth+1) || walk(n.sibling, depth+1) || walk(n.return, depth+1);
                            }
                            var r = walk(node, 0);
                            if (r) return r;
                        }
                    }
                    return null;
                })()"""
                org = await cdp_eval(ws, JS_ALT_ORG, timeout=10)
                print(f"  Alternative organization context found: {bool(org)}")

                if org:
                    params = {k:v[0] for k,v in parse_qs(urlparse(oauth_url).query).items()}
                    scope = " ".join(s for s in params.get("scope","").split() if s != "org:create_api_key")
                    body = json.dumps({"response_type":"code","client_id":params.get("client_id",""),"organization_uuid":org,"redirect_uri":params.get("redirect_uri",""),"scope":scope,"state":params.get("state",""),"code_challenge":params.get("code_challenge",""),"code_challenge_method":"S256"})
                    js = f"""(async function(){{var r=await fetch("/v1/oauth/{org}/authorize",{{method:"POST",headers:{{"Content-Type":"application/json","Accept":"application/json"}},credentials:"include",body:{json.dumps(body)}}});return r.status+" | "+await r.text()}})()"""
                    result = await cdp_eval(ws, js, timeout=15)
                    print(f"  Alternative authorize response received: {bool(result)}")
                    if result and result.startswith("200"):
                        _, txt = result.split(" | ", 1)
                        rd = json.loads(txt).get("redirect_uri","")
                        cp = parse_qs(urlparse(rd).query)
                        code = cp.get("code", [""])[0]
                        state = cp.get("state", [""])[0]

                # Try getting org UUID from API endpoints
                if not org:
                    print("  Trying API-based org extraction...")
                    JS_API_ORG = """(async function(){
                        try {
                            var r = await fetch("/api/organizations", {credentials:"include"});
                            if (r.ok) { var d = await r.json(); return JSON.stringify(d); }
                        } catch(e) {}
                        try {
                            var r2 = await fetch("/api/auth/current_account", {credentials:"include"});
                            if (r2.ok) { var d2 = await r2.json(); return JSON.stringify(d2); }
                        } catch(e) {}
                        try {
                            var r3 = await fetch("/api/me", {credentials:"include"});
                            if (r3.ok) { var d3 = await r3.json(); return JSON.stringify(d3); }
                        } catch(e) {}
                        return null;
                    })()"""
                    api_result = await cdp_eval(ws, JS_API_ORG, timeout=15)
                    print(f"  Organization lookup response received: {bool(api_result)}")
                    if api_result and api_result != "null":
                        try:
                            data = json.loads(api_result)
                            if isinstance(data, list) and len(data) > 0:
                                org = data[0].get("uuid") or data[0].get("id")
                            elif isinstance(data, dict):
                                org = data.get("uuid") or data.get("organization_uuid") or data.get("id")
                                if not org and "memberships" in data:
                                    org = data["memberships"][0]["organization"]["uuid"]
                        except Exception as exc:
                            print(f"  Organization lookup parse error: {type(exc).__name__}")
                    if org:
                        print("  Organization context recovered from API")
                        params = {k:v[0] for k,v in parse_qs(urlparse(oauth_url).query).items()}
                        scope = " ".join(s for s in params.get("scope","").split() if s != "org:create_api_key")
                        body = json.dumps({"response_type":"code","client_id":params.get("client_id",""),"organization_uuid":org,"redirect_uri":params.get("redirect_uri",""),"scope":scope,"state":params.get("state",""),"code_challenge":params.get("code_challenge",""),"code_challenge_method":"S256"})
                        js = f"""(async function(){{var r=await fetch("/v1/oauth/{org}/authorize",{{method:"POST",headers:{{"Content-Type":"application/json","Accept":"application/json"}},credentials:"include",body:{json.dumps(body)}}});return r.status+" | "+await r.text()}})()"""
                        result = await cdp_eval(ws, js, timeout=15)
                        print(f"  API authorize response received: {bool(result)}")
                        if result and result.startswith("200"):
                            _, txt = result.split(" | ", 1)
                            rd = json.loads(txt).get("redirect_uri","")
                            cp = parse_qs(urlparse(rd).query)
                            code = cp.get("code", [""])[0]
                            state = cp.get("state", [""])[0]

                if not code:
                    # Last resort: click Authorize and watch page navigation + CDP Network events
                    print("  Fallback: clicking Authorize + watching navigation + network...")
                    JS_CLICK_AUTH = """(function(){var btn=[...document.querySelectorAll("button")].find(b=>b.textContent.trim()==="Authorize");if(btn){btn.click();return "clicked";}return "no button";})()"""
                    click_r = await cdp_eval(ws, JS_CLICK_AUTH, timeout=10)
                    print(f"  Click: {click_r}")
                    if click_r == "clicked":
                        auth_req_ids = {}
                        deadline_net = time.time() + 15
                        while time.time() < deadline_net:
                            try:
                                raw = await asyncio.wait_for(ws.recv(), timeout=1)
                                msg = json.loads(raw)
                                method = msg.get("method", "")
                                if method == "Network.responseReceived":
                                    resp_url = msg.get("params", {}).get("response", {}).get("url", "")
                                    status_code = msg.get("params", {}).get("response", {}).get("status", 0)
                                    print(f"  Network response received (status={status_code})")
                                    if "authorize" in resp_url or "oauth" in resp_url:
                                        req_id = msg["params"]["requestId"]
                                        auth_req_ids[req_id] = resp_url
                                elif method == "Network.loadingFinished":
                                    req_id = msg.get("params", {}).get("requestId", "")
                                    if req_id in auth_req_ids:
                                        await ws.send(json.dumps({"id": 9999, "method": "Network.getResponseBody", "params": {"requestId": req_id}}))
                                        for _ in range(10):
                                            body_raw = await asyncio.wait_for(ws.recv(), timeout=3)
                                            body_msg = json.loads(body_raw)
                                            if body_msg.get("id") == 9999:
                                                body_text = body_msg.get("result", {}).get("body", "")
                                                print(
                                                    "  Authorization response body received "
                                                    f"({len(body_text)} chars)"
                                                )
                                                try:
                                                    rd = json.loads(body_text).get("redirect_uri", "")
                                                    if rd:
                                                        rcp = parse_qs(urlparse(rd).query)
                                                        code = rcp.get("code", [""])[0]
                                                        state = rcp.get("state", [""])[0]
                                                        if code and state:
                                                            print(f"  Got code/state from CDP network intercept")
                                                except: pass
                                                break
                                        if code and state:
                                            break
                                elif method == "Page.frameNavigated":
                                    nav_url = msg.get("params", {}).get("frame", {}).get("url", "")
                                    print("  Browser navigation event received")
                                    if "code=" in nav_url:
                                        nav_params = parse_qs(urlparse(nav_url).query)
                                        code = nav_params.get("code", [""])[0]
                                        state = nav_params.get("state", [""])[0]
                                        if code and state:
                                            print(f"  Got code/state from navigation URL")
                                            break
                                    if nav_url and "code=" in (urlparse(nav_url).fragment or ""):
                                        frag_params = parse_qs(urlparse(nav_url).fragment)
                                        code = frag_params.get("code", [""])[0]
                                        state = frag_params.get("state", [""])[0]
                                        if code and state:
                                            print(f"  Got code/state from navigation fragment")
                                            break
                            except asyncio.TimeoutError:
                                continue
                            except Exception as exc:
                                print(f"  Network watch error: {type(exc).__name__}")
                                break
                        # Also check current URL after waiting
                        if not code:
                            cur_url = await cdp_eval(ws, "window.location.href", timeout=5)
                            print(f"  Current navigation state available: {bool(cur_url)}")
                            if cur_url and "code=" in cur_url:
                                cp = parse_qs(urlparse(cur_url).query)
                                code = cp.get("code", [""])[0]
                                state = cp.get("state", [""])[0]

            if code and state:
                print(f"  Feeding code#state to CLI stdin...")
                cli.stdin.write(f"{code}#{state}\n".encode())
                cli.stdin.flush()
                cli.stdin.close()
                for _ in range(30):
                    if cli.poll() is not None: break
                    await asyncio.sleep(1)
                try:
                    cli_out = b""
                    if cli.stdout and select.select([cli.stdout], [], [], 0)[0]:
                        cli_out = os.read(cli.stdout.fileno(), 4096)
                    print(f"  CLI emitted {len(cli_out)} bytes")
                except (OSError, ValueError):
                    pass
                print(f"  CLI exit code: {cli.poll()}")
                cred_path = Path(config_dir) / ".credentials.json"
                if cred_path.exists():
                    try:
                        creds = json.loads(cred_path.read_text())
                        if creds.get("claudeAiOauth", {}).get("accessToken"):
                            print("SUCCESS!")
                            return {"success": True}
                    except Exception:
                        pass
                print("FAILED: credentials not valid after login")
                return {"success": False}
            print("FAILED: authorize (no code/state obtained)")
            return None
    finally:
        # The public cdp_login wrapper owns process-group cleanup across the
        # entire startup + CDP flow, including this historical inner try block.
        pass


async def cdp_login(
    email: str,
    token: str,
    config_dir: str,
    oauth_url: str = "",
    cookies_171: list[dict] | None = None,
    magic_link: str | None = None,
) -> dict | None:
    """Cancellation-safe public wrapper for the Chrome/CLI login flow."""

    processes: list[subprocess.Popen] = []
    try:
        return await _cdp_login_impl(
            email,
            token,
            config_dir,
            oauth_url,
            cookies_171,
            magic_link,
            _processes=processes,
        )
    finally:
        # _cdp_login_impl's historical try/finally starts only after CDP is
        # ready.  This outer guard also covers cancellation during startup.
        cleanup = asyncio.create_task(_cleanup_tracked_processes(processes))
        cleanup_cancellation: asyncio.CancelledError | None = None
        while not cleanup.done():
            try:
                await asyncio.shield(cleanup)
            except asyncio.CancelledError as exc:
                # Manager shutdown and WebSocket teardown can cancel the same
                # login more than once. Keep waiting until every process group
                # has a terminal proof; a later cancellation must not let the
                # wrapper finish while the shielded cleanup still runs.
                if cleanup_cancellation is None:
                    cleanup_cancellation = exc
        # A dedicated cleanup failure takes priority over cancellation and must
        # reach WorkerRuntime so it can withhold cleanup_complete.
        cleanup.result()
        if cleanup_cancellation is not None:
            raise cleanup_cancellation


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print(f"Usage: {sys.argv[0]} <account_id> <email_token>")
        sys.exit(1)
    account_id = sys.argv[1]
    email_token = sys.argv[2]
    accounts_file = Path.home() / ".claude-pool" / "accounts.json"
    accounts = json.loads(accounts_file.read_text())
    acct = next((a for a in accounts["accounts"] if a["id"] == account_id), None)
    if not acct:
        print(f"Account {account_id} not found"); sys.exit(1)
    result = asyncio.run(cdp_login(
        email=acct["email"],
        token=email_token,
        config_dir=acct["config_dir"],
    ))
    print(f"Result success: {bool(result and result.get('success'))}")
    sys.exit(0 if result and result.get("success") else 1)
