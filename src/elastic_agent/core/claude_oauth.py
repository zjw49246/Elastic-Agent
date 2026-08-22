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
import re
import shlex
import stat
import time
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

from elastic_agent.core.secure_store import (
    atomic_write_private,
    fsync_directory,
    secure_state_directory,
    tighten_state_file,
)

logger = logging.getLogger(__name__)

OAUTH_CLIENT_ID = "9d1c250a-e61b-44d9-88ed-5944d1962f5e"
MAIL_API_BASE = "https://b.171mail.com/api/v1"
ANTHROPIC_TOKEN_URL = "https://console.anthropic.com/v1/oauth/token"
ANTHROPIC_USAGE_URL = "https://api.anthropic.com/api/oauth/usage"
ANTHROPIC_ACCOUNT_URL = "https://claude.ai/api/account"


class ClaudeLoginCleanupError(RuntimeError):
    """A Claude browser/CLI process group could not be proven terminated."""


def _safe_login_error(text: str, *, secrets: tuple[str, ...] = ()) -> str:
    """Return a bounded diagnostic with URLs and known login inputs removed."""

    safe = re.sub(r"https?://\S+", "[redacted-url]", text or "")
    safe = re.sub(
        r"(?i)(token|code|state|password)=([^\s&]+)",
        r"\1=[redacted]",
        safe,
    )
    for secret in secrets:
        if secret:
            safe = safe.replace(secret, "[redacted]")
    return (safe.strip() or "auto_login failed")[-500:]


def normalize_local_config_dir(config_dir: str | None) -> str:
    """Return the credential directory for a login running on this machine.

    An absolute path is an explicit caller choice and is preserved verbatim.
    Empty and relative values cannot safely be interpreted across Manager and
    worker working directories, so they mean Claude's default directory for
    the OS user running this worker.
    """
    if config_dir and Path(config_dir).is_absolute():
        return config_dir
    return str(Path.home() / ".claude")


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


_CLAUDE_CREDENTIAL_FILES = (".claude.json", ".credentials.json")


@dataclass
class ClaudeCredentialSnapshot:
    """Private, in-memory snapshot used to make a login transactional.

    The snapshot deliberately covers only credential-bearing files.  Settings
    are not authentication state and are merged only after the browser flow
    succeeds.
    """

    config_dir: Path
    files: dict[str, bytes | None] = field(default_factory=dict)


def snapshot_claude_credentials(config_dir: str) -> ClaudeCredentialSnapshot:
    """Capture Claude credential files without following symlinks.

    Existing legacy files are tightened before they are read.  A missing
    directory is not created here, which lets callers snapshot a fresh slot
    without introducing filesystem side effects before login starts.
    """

    directory = Path(config_dir).expanduser()
    snapshot = ClaudeCredentialSnapshot(config_dir=directory)
    if directory.is_symlink():
        raise RuntimeError(
            f"Claude credential directory must not be a symlink: {directory}"
        )
    if directory.exists():
        if not directory.is_dir():
            raise RuntimeError(
                f"Claude credential directory is not a directory: {directory}"
            )
        secure_state_directory(directory)
    for name in _CLAUDE_CREDENTIAL_FILES:
        path = directory / name
        if not path.exists() and not path.is_symlink():
            snapshot.files[name] = None
            continue
        mode = path.lstat().st_mode
        if path.is_symlink() or not stat.S_ISREG(mode):
            raise RuntimeError(f"Claude credential path is not a regular file: {path}")
        tighten_state_file(path)
        content = path.read_bytes()
        try:
            content.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise RuntimeError(
                f"Claude credential file is not valid UTF-8: {path}"
            ) from exc
        snapshot.files[name] = content
    return snapshot


def restore_claude_credentials(snapshot: ClaudeCredentialSnapshot) -> None:
    """Rollback a credential snapshot using durable 0600 replacements."""

    if snapshot.config_dir.is_symlink():
        raise RuntimeError(
            f"Claude credential directory became a symlink: {snapshot.config_dir}"
        )
    removed_entry = False
    for name, content in snapshot.files.items():
        path = snapshot.config_dir / name
        if content is None:
            if path.is_symlink():
                raise RuntimeError(f"Claude credential path became a symlink: {path}")
            removed_entry = removed_entry or path.exists()
            path.unlink(missing_ok=True)
            continue
        # Both files are JSON/UTF-8.  Reject corrupt backup bytes instead of
        # publishing an altered or lossy credential file.
        text = content.decode("utf-8")
        atomic_write_private(path, text)
    # An unlink is not durable until its parent directory is fsynced.  A late
    # cancellation ACK must never let the Manager release an account while a
    # crash can resurrect the newly written credential.
    if removed_entry and snapshot.config_dir.is_dir():
        fsync_directory(snapshot.config_dir)


def secure_claude_credentials(config_dir: str) -> None:
    """Tighten a successfully written Claude credential set in place."""

    directory = secure_state_directory(config_dir)
    for name in _CLAUDE_CREDENTIAL_FILES:
        path = directory / name
        if path.exists() or path.is_symlink():
            tighten_state_file(path)


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
        # Direct provider callers do not necessarily go through WorkerRuntime.
        # Normalize local paths here as a second, idempotent guard so the login
        # flow and credential read-back always target the same directory.
        if not config.worker_host:
            config = replace(
                config,
                config_dir=normalize_local_config_dir(config.config_dir),
            )
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
        except ClaudeLoginCleanupError:
            # This is not an ordinary failed attempt: Runtime must withhold its
            # cleanup acknowledgement so the Manager quarantines the worker.
            raise
        except asyncio.TimeoutError:
            return LoginResult(
                success=False, account_id=config.account_id,
                error=f"login timed out after {config.login_timeout}s",
            )
        except Exception as exc:  # pragma: no cover - defensive
            logger.error(
                "Login errored for %s (%s)",
                config.account_id,
                type(exc).__name__,
            )
            return LoginResult(
                success=False,
                account_id=config.account_id,
                error="Claude login failed unexpectedly",
            )

        if not ok:
            return LoginResult(
                success=False, account_id=config.account_id,
                error=_safe_login_error(
                    output,
                    secrets=(config.email_token,),
                ),
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
    import urllib.error
    import urllib.request

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
    except Exception as exc:
        # Injected HTTP clients may include the form body (refresh token) in
        # exception text; retain only the exception class in worker logs.
        logger.error("Token refresh failed (%s)", type(exc).__name__)
        return None


def write_credentials(config_dir: str, creds: dict[str, Any]) -> None:
    """Durably write Claude credentials with directory/file mode 0700/0600."""
    cred_path = Path(config_dir) / ".credentials.json"
    data = {"claudeAiOauth": creds}
    atomic_write_private(cred_path, json.dumps(data, indent=2))


def read_credentials(config_dir: str) -> dict[str, Any] | None:
    """Read .credentials.json and return the claudeAiOauth object."""
    cred_path = Path(config_dir) / ".credentials.json"
    if not cred_path.exists():
        return None
    try:
        secure_state_directory(cred_path.parent)
        tighten_state_file(cred_path)
        data = json.loads(cred_path.read_text())
        return data.get("claudeAiOauth")
    except Exception:
        return None
