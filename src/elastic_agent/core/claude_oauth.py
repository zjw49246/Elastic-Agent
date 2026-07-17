"""ClaudeOAuthProvider — Claude account auto-login, synced to CCM's version.

The browser+CDP+接码 flow lives in the vendored, worker-local login module
``elastic_agent.worker.login`` (near-verbatim from CCM's proven
``auto_login.py`` + ``cdp_login.py``): Chrome CDP drives the OAuth authorize
call directly (no Playwright/mitmproxy), with multi-backend magic-link 接码
(171mail API for most domains, a mail relay / mail.com web-login for
mail.com-family accounts, auto-selected by email domain).

``ClaudeOAuthProvider`` is the thin orchestration wrapper the Manager's
credential layer talks to. It keeps the ``OAuthConfig -> LoginResult`` contract
but no longer implements the browser flow itself — it runs the vendored login
on the machine that owns the config_dir:
  - ``worker_host is None`` → in-process (this machine is the worker).
  - ``worker_host`` set → ``python -m elastic_agent.worker.login.auto_login``
    over SSH on the worker (Manager triggers, worker executes; the credential
    file never transits the Manager — it is written and stays on the worker).

Shared helpers (``refresh_access_token``, ``write_credentials``,
``read_credentials``, ``OAUTH_CLIENT_ID``, ``ANTHROPIC_USAGE_URL``) are still
imported by the worker runtime and quota checker and are unchanged.
"""

from __future__ import annotations

import asyncio
import json
import logging
import shlex
import time
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)

OAUTH_CLIENT_ID = "9d1c250a-e61b-44d9-88ed-5944d1962f5e"
MAIL_API_BASE = "https://b.171mail.com/api/v1"
ANTHROPIC_TOKEN_URL = "https://console.anthropic.com/v1/oauth/token"
ANTHROPIC_USAGE_URL = "https://api.anthropic.com/api/oauth/usage"
ANTHROPIC_ACCOUNT_URL = "https://claude.ai/api/account"


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
    login_timeout: int = 480
    mitm_port: int = 8765
    worker_host: str | None = None
    ssh_key_path: str = "/root/.ssh/elastic-agent-aliyun.pem"
    ssh_user: str = "root"
    # "171mail" (API 接码) or "mailcom" (Chrome 接码). Auto-detected from the
    # email domain when None.
    provider: str | None = None


def resolve_provider(config: OAuthConfig) -> str:
    """Pick the magic-link backend: explicit ``config.provider`` wins, else
    auto-detect (mail.com-family → mailcom, everything else → 171mail)."""
    if config.provider in ("171mail", "mailcom"):
        return config.provider
    from elastic_agent.worker.login import is_mailcom_domain

    return "mailcom" if is_mailcom_domain(config.email) else "171mail"


class ClaudeOAuthProvider:
    """Runs the vendored worker-local login flow and adapts it to LoginResult.

    The heavy lifting (Chrome, CDP, Cloudflare, 接码, OAuth authorize) is the
    vendored CCM implementation in ``elastic_agent.worker.login``. This class
    only chooses where to run it (locally vs. on the worker over SSH) and reads
    back the credentials it writes.
    """

    def __init__(self, http_client: Any | None = None):
        self._http_client = http_client

    async def login(self, config: OAuthConfig) -> LoginResult:
        provider = resolve_provider(config)
        remote = bool(config.worker_host)
        logger.info(
            "ClaudeOAuthProvider: login %s (provider=%s, target=%s)",
            config.account_id, provider,
            f"worker:{config.worker_host}" if remote else "local",
        )

        try:
            if remote:
                ok, output = await self._login_on_worker(config, provider)
            else:
                ok, output = await self._login_local(config, provider)
        except asyncio.TimeoutError:
            return LoginResult(
                success=False, account_id=config.account_id,
                error=f"login timed out after {config.login_timeout}s",
            )
        except Exception as exc:  # pragma: no cover - defensive
            logger.exception("Login errored for %s", config.account_id)
            return LoginResult(success=False, account_id=config.account_id, error=str(exc))

        if not ok:
            return LoginResult(
                success=False, account_id=config.account_id,
                error=(output or "auto_login failed").strip()[-500:],
            )

        oauth = await self._fetch_written_credentials(config)
        if not oauth or not oauth.get("accessToken"):
            return LoginResult(
                success=False, account_id=config.account_id,
                error="login reported success but no credentials were written",
            )
        return LoginResult(
            success=True,
            account_id=config.account_id,
            access_token=oauth.get("accessToken"),
            refresh_token=oauth.get("refreshToken"),
            expires_at=oauth.get("expiresAt"),
        )

    # -- local: this process runs on the machine that owns config_dir --------

    async def _login_local(self, config: OAuthConfig, provider: str) -> tuple[bool, str]:
        from elastic_agent.worker.login import perform_login

        ok = await asyncio.wait_for(
            perform_login(
                email=config.email,
                token_171=config.email_token,
                config_dir=config.config_dir,
                provider=provider,
            ),
            timeout=config.login_timeout,
        )
        return bool(ok), ""

    # -- remote: Manager triggers, worker executes the vendored CLI over SSH -

    async def _login_on_worker(self, config: OAuthConfig, provider: str) -> tuple[bool, str]:
        cfg_dir = shlex.quote(config.config_dir)
        method_arg = f" --login-method {provider}" if provider in ("171mail", "mailcom") else ""
        # Start a headless X server, then run the vendored login CLI on the
        # worker. Xvfb + Chrome + xdotool + the CDP flow all live worker-side.
        script = (
            "set +e\n"
            'export PATH="$HOME/.local/bin:$PATH"\n'
            "pkill -f 'Xvfb :99' 2>/dev/null; sleep 0.5\n"
            "Xvfb :99 -screen 0 1920x1080x24 -nolisten tcp -ac >/dev/null 2>&1 &\n"
            "sleep 1\n"
            "export DISPLAY=:99\n"
            f"mkdir -p {cfg_dir}\n"
            "python3 -m elastic_agent.worker.login.auto_login"
            f" --email {shlex.quote(config.email)}"
            f" --token {shlex.quote(config.email_token)}"
            f" --config-dir {cfg_dir}{method_arg}\n"
        )
        return await self._ssh_run(config, script, timeout=config.login_timeout + 30)

    async def _ssh_run(
        self, config: OAuthConfig, script: str, timeout: int
    ) -> tuple[bool, str]:
        """Run a bash script on the worker via SSH (stdin), return (ok, output)."""
        ssh_opts = [
            "-o", "StrictHostKeyChecking=no",
            "-o", "UserKnownHostsFile=/dev/null",
            "-o", "ConnectTimeout=10",
        ]
        proc = await asyncio.create_subprocess_exec(
            "ssh", *ssh_opts, "-i", config.ssh_key_path,
            f"{config.ssh_user}@{config.worker_host}", "bash -s",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        try:
            stdout, _ = await asyncio.wait_for(
                proc.communicate(script.encode()), timeout=timeout,
            )
        except asyncio.TimeoutError:
            proc.kill()
            raise
        return proc.returncode == 0, (stdout.decode(errors="replace") if stdout else "")

    # -- read back the credentials the login wrote --------------------------

    async def _fetch_written_credentials(self, config: OAuthConfig) -> dict[str, Any] | None:
        if not config.worker_host:
            return read_credentials(config.config_dir)
        # Remote: read the claudeAiOauth object over SSH; the credential file
        # stays on the worker (never moved to the Manager).
        _ok, output = await self._ssh_run(
            config,
            f"cat {shlex.quote(config.config_dir)}/.credentials.json 2>/dev/null || true",
            timeout=30,
        )
        if not output.strip():
            return None
        try:
            return json.loads(output).get("claudeAiOauth")
        except Exception:
            return None


def _post_form_urllib(url: str, body: str) -> dict[str, Any] | None:
    """POST an x-www-form-urlencoded body via the stdlib and return parsed JSON
    (or None on non-200 / transport error). This is the aiohttp-free refresh
    path: the worker framework env may not ship aiohttp, and a missing aiohttp
    previously crashed every QuotaChecker token refresh."""
    import urllib.request
    import urllib.error

    req = urllib.request.Request(
        url,
        data=body.encode(),
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            if resp.status != 200:
                return None
            return json.loads(resp.read().decode())
    except Exception:
        return None


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
            # stdlib fallback — no aiohttp dependency (the worker framework env
            # may not ship it). Run the blocking POST off the event loop.
            import asyncio
            result = await asyncio.to_thread(_post_form_urllib, ANTHROPIC_TOKEN_URL, body)
            if not result:
                return None

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
    from pathlib import Path

    cred_path = Path(config_dir) / ".credentials.json"
    cred_path.parent.mkdir(parents=True, exist_ok=True)
    data = {"claudeAiOauth": creds}
    tmp = cred_path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2))
    tmp.replace(cred_path)


def read_credentials(config_dir: str) -> dict[str, Any] | None:
    """Read .credentials.json and return the claudeAiOauth object."""
    from pathlib import Path

    cred_path = Path(config_dir) / ".credentials.json"
    if not cred_path.exists():
        return None
    try:
        data = json.loads(cred_path.read_text())
        return data.get("claudeAiOauth")
    except Exception:
        return None
