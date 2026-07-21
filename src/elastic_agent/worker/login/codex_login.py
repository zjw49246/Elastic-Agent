"""Worker-local Codex login through the OpenAI browser OAuth flow.

The OpenAI account password is the primary credential.  A mailbox query token
is optional and is used only when OpenAI asks for an email OTP; without one (or
when mailbox polling fails), an injected/manual OTP reader supplies the code.

Credentials never leave ``CODEX_HOME``.  A login is committed only after the
new ``auth.json`` belongs to the requested email and a real ``codex exec``
smoke test succeeds.  Failure and cancellation atomically restore any previous
credential file.
"""

from __future__ import annotations

import asyncio
import base64
import datetime
import json
import os
import re
import shutil
import signal
import sys
import tempfile
import time
import uuid
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any

import httpx

DEFAULT_LOGIN_TIMEOUT = 300
AUTH_URL_TIMEOUT = 30
AUTH_JSON_WAIT = 30
SMOKE_TIMEOUT = 120
STATE_STEP_PAUSE_MS = 2_500
MANUAL_OTP_TIMEOUT = 600
MAX_OTP_ATTEMPTS = 3

LOGIN_EVENT_PREFIX = "ELASTIC_CODEX_LOGIN_EVENT:"

ANSI_RE = re.compile(r"\x1b(?:[@-_][0-?]*[ -/]*[@-~])")
AUTHORIZE_URL_RE = re.compile(r"(https://auth\.openai\.com/oauth/authorize\S+)")

MAIL_API_BASE = os.environ.get("CODEX_171MAIL_URL", "https://b.171mail.com/api/v1")
MAIL_DECODE_API = os.environ.get(
    "CODEX_MAILCATCHER_URL",
    "https://mail.claude-code-manager.com/api/v1/message",
)
MAIL_POLL_TIMEOUT = 120
MAIL_POLL_INTERVAL = 3

WEBMAIL_PROVIDERS = {
    "163.com": "mailcatcher",
    "mail.com": "mailcom",
    "onet.pl": "onet",
    "gazeta.pl": "gazeta",
}
MAILCATCHER_PROVIDERS = {"mailcatcher", "mailcom", "onet", "gazeta"}
MAIL_PROVIDERS = {"171mail", *MAILCATCHER_PROVIDERS}

EMAIL_SELECTOR = 'input[type="email"], input[name="email"]'
PASSWORD_SELECTOR = 'input[type="password"]'
OTP_SELECTOR = (
    'input[inputmode="numeric"], input[autocomplete="one-time-code"], '
    'input[name="code"]'
)
OTP_ERROR_SELECTOR = (
    '[role="alert"], [aria-live="assertive"], [data-error-code], '
    '[data-testid*="error"], [class*="error"]'
)
OTP_ERROR_RE = re.compile(
    r"(?:\b(?:invalid|incorrect|wrong|expired)\b.*\bcode\b|"
    r"\bcode\b.*\b(?:invalid|incorrect|wrong|expired)\b|"
    r"\bcode\b.*\bnot valid\b|"
    r"\bcode\b.*(?:does not|doesn't|did not) match|"
    r"could(?: not|n't) verify.*\bcode\b|"
    r"too many (?:verification )?attempts)",
    re.IGNORECASE,
)
CONTINUE_BUTTON_TEXTS = (
    "Continue",
    "Verify",
    "Next",
    "Log in",
    "Sign in",
    "Authorize",
    "Allow",
    "Approve",
    "Confirm",
)


class CodexLoginError(RuntimeError):
    """A safe, user-facing Codex login failure."""


def detect_mail_provider(email: str) -> str:
    """Choose CCM's mailbox backend from the address domain."""

    domain = email.rsplit("@", 1)[-1].strip().lower()
    return WEBMAIL_PROVIDERS.get(domain, "171mail")


def _mail_timestamp(data: dict[str, Any]) -> float | None:
    raw = data.get("date") or data.get("Date")
    if raw:
        value = str(raw).strip()
        try:
            return datetime.datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
        except ValueError:
            try:
                return parsedate_to_datetime(value).timestamp()
            except (TypeError, ValueError):
                pass

    subject = str(data.get("subject") or "")
    match = re.search(r"\|\s+(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", subject)
    if match:
        return time.mktime(time.strptime(match.group(1), "%Y-%m-%d %H:%M:%S"))
    return None


async def poll_verification_code(
    token: str,
    after_ts: float,
    *,
    timeout_s: int = MAIL_POLL_TIMEOUT,
    email: str = "",
    provider: str | None = None,
) -> str:
    """Poll a supported mailbox backend for a fresh six-digit OpenAI OTP."""

    selected_provider = provider or detect_mail_provider(email)
    if selected_provider not in MAIL_PROVIDERS:
        raise CodexLoginError(f"Unsupported mailbox provider: {selected_provider}")

    uses_mailcatcher = selected_provider in MAILCATCHER_PROVIDERS
    request_timeout = 120.0 if uses_mailcatcher else 15.0
    deadline = time.time() + timeout_s
    seen: set[tuple[str, str, str]] = set()

    async with httpx.AsyncClient(timeout=request_timeout) as client:
        while time.time() < deadline:
            try:
                url = MAIL_DECODE_API if uses_mailcatcher else f"{MAIL_API_BASE}/message"
                response = await client.get(url, params={"token": token, "type": "gpt"})
                if response.status_code in {401, 403}:
                    raise CodexLoginError("Mailbox API rejected the query token")
                if response.status_code >= 400:
                    response.raise_for_status()
                payload = response.json()
            except CodexLoginError:
                raise
            except (httpx.HTTPError, ValueError):
                await asyncio.sleep(MAIL_POLL_INTERVAL)
                continue

            if not isinstance(payload, dict):
                await asyncio.sleep(MAIL_POLL_INTERVAL)
                continue

            response_code = payload.get("code")
            if uses_mailcatcher:
                if response_code == 202:
                    await asyncio.sleep(MAIL_POLL_INTERVAL)
                    continue
                if response_code != 200:
                    message = payload.get("message") or payload.get("error") or "unknown error"
                    raise CodexLoginError(f"Mailbox API rejected the query token: {message}")

            data = payload.get("data") or {}
            if not isinstance(data, dict):
                await asyncio.sleep(MAIL_POLL_INTERVAL)
                continue

            subject = str(data.get("subject") or "")
            code = str(data.get("code") or "")
            body = str(data.get("body") or "")
            date_string = str(data.get("date") or data.get("Date") or "")
            if not (subject or code or body):
                await asyncio.sleep(MAIL_POLL_INTERVAL)
                continue

            key = (subject, date_string, code)
            if key in seen:
                await asyncio.sleep(MAIL_POLL_INTERVAL)
                continue

            mail_timestamp = _mail_timestamp(data)
            freshness_cutoff = int(after_ts) if uses_mailcatcher else after_ts - 120
            if mail_timestamp is not None and mail_timestamp < freshness_cutoff:
                seen.add(key)
                await asyncio.sleep(MAIL_POLL_INTERVAL)
                continue
            if uses_mailcatcher and mail_timestamp is None:
                seen.add(key)
                await asyncio.sleep(MAIL_POLL_INTERVAL)
                continue

            match = re.search(r"\b(\d{6})\b", f"{subject} {code} {body}")
            if match:
                return match.group(1)

            seen.add(key)
            await asyncio.sleep(MAIL_POLL_INTERVAL)

    raise CodexLoginError(f"No fresh OpenAI verification code within {timeout_s}s")


def _emit_login_event(event: dict[str, Any]) -> None:
    """Emit a machine event containing challenge metadata but no credentials."""

    print(f"{LOGIN_EVENT_PREFIX}{json.dumps(event, separators=(',', ':'))}", flush=True)


class ManualOtpReader:
    """Read human OTP responses from a newline-delimited stdin channel."""

    def __init__(self) -> None:
        self._reader: asyncio.StreamReader | None = None
        self._transport: Any = None

    async def _ensure_reader(self) -> asyncio.StreamReader:
        if self._reader is not None:
            return self._reader
        loop = asyncio.get_running_loop()
        reader = asyncio.StreamReader()
        protocol = asyncio.StreamReaderProtocol(reader)
        transport, _ = await loop.connect_read_pipe(lambda: protocol, sys.stdin)
        self._reader = reader
        self._transport = transport
        return reader

    async def read_code(
        self,
        *,
        attempt_id: str,
        timeout_s: int,
        logs: list[str],
    ) -> str:
        """Emit a challenge and wait for its matching six-digit response."""

        reader = await self._ensure_reader()
        challenge_id = uuid.uuid4().hex
        expires_at = int(time.time() + timeout_s)
        _emit_login_event(
            {
                "type": "otp_required",
                "attempt_id": attempt_id,
                "challenge_id": challenge_id,
                "expires_at": expires_at,
            }
        )
        logs.append("Waiting for a user-supplied email verification code")

        while True:
            remaining = expires_at - time.time()
            if remaining <= 0:
                _emit_login_event(
                    {
                        "type": "otp_expired",
                        "attempt_id": attempt_id,
                        "challenge_id": challenge_id,
                    }
                )
                raise CodexLoginError("Timed out waiting for a user-supplied verification code")
            try:
                raw = await asyncio.wait_for(reader.readline(), timeout=remaining)
            except asyncio.TimeoutError as exc:
                _emit_login_event(
                    {
                        "type": "otp_expired",
                        "attempt_id": attempt_id,
                        "challenge_id": challenge_id,
                    }
                )
                raise CodexLoginError(
                    "Timed out waiting for a user-supplied verification code"
                ) from exc
            if not raw:
                raise CodexLoginError("Verification-code input channel closed")
            try:
                payload = json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                continue
            if not isinstance(payload, dict) or payload.get("challenge_id") != challenge_id:
                continue
            code = str(payload.get("code") or "").strip()
            if not re.fullmatch(r"\d{6}", code):
                continue
            _emit_login_event(
                {
                    "type": "otp_received",
                    "attempt_id": attempt_id,
                    "challenge_id": challenge_id,
                }
            )
            return code

    def close(self) -> None:
        if self._transport is not None:
            self._transport.close()
            self._transport = None


async def _click_continue(page: Any, logs: list[str]) -> bool:
    for text in CONTINUE_BUTTON_TEXTS:
        candidates = page.locator(f'button:has-text("{text}")')
        for index in range(await candidates.count()):
            element = candidates.nth(index)
            if await element.is_visible() and await element.is_enabled():
                await element.click()
                logs.append(f"Clicked '{text}'")
                return True
    candidates = page.locator('button[type="submit"]')
    for index in range(await candidates.count()):
        element = candidates.nth(index)
        if await element.is_visible() and await element.is_enabled():
            await element.click()
            logs.append("Clicked submit")
            return True
    return False


async def _first_visible(page: Any, selectors: str) -> Any | None:
    locator = page.locator(selectors)
    for index in range(await locator.count()):
        candidate = locator.nth(index)
        if await candidate.is_visible():
            return candidate
    return None


async def _visible_action_labels(page: Any) -> list[str]:
    labels: list[str] = []
    candidates = page.locator("button, a, [role=button]")
    for index in range(min(await candidates.count(), 40)):
        candidate = candidates.nth(index)
        if not await candidate.is_visible():
            continue
        text = " ".join(
            filter(
                None,
                [
                    await candidate.inner_text(),
                    await candidate.get_attribute("aria-label"),
                ],
            )
        ).strip()
        if text:
            labels.append(text[:100])
    return labels


async def _visible_otp_error(page: Any) -> str | None:
    candidates = page.locator(OTP_ERROR_SELECTOR)
    for index in range(min(await candidates.count(), 20)):
        candidate = candidates.nth(index)
        if not await candidate.is_visible():
            continue
        text = (await candidate.inner_text()).strip()
        if text and OTP_ERROR_RE.search(text):
            return text[:200]
    return None


async def _run_state_machine(
    *,
    page: Any,
    email: str,
    password: str,
    email_token: str,
    timeout: int,
    auth_path: Path,
    logs: list[str],
    mail_provider: str | None = None,
    attempt_id: str = "",
    manual_otp_reader: Any = None,
) -> None:
    otp_poll_start = time.time()
    deadline = time.time() + timeout
    otp_submitted = False
    otp_attempts = 0
    owns_manual_reader = manual_otp_reader is None
    manual_reader = manual_otp_reader or ManualOtpReader()

    try:
        while time.time() < deadline:
            await page.wait_for_timeout(STATE_STEP_PAUSE_MS)

            if auth_path.exists():
                logs.append("auth.json appeared; browser flow complete")
                return

            email_field = await _first_visible(page, EMAIL_SELECTOR)
            if email_field and not await email_field.input_value():
                await email_field.fill(email)
                logs.append("Email filled")
                await _click_continue(page, logs)
                continue

            password_field = await _first_visible(page, PASSWORD_SELECTOR)
            if password_field and not await password_field.input_value():
                await password_field.fill(password)
                logs.append("Password filled")
                await _click_continue(page, logs)
                continue

            otp_field = await _first_visible(page, OTP_SELECTOR)
            if otp_field:
                if otp_submitted:
                    otp_error = await _visible_otp_error(page)
                    if not otp_error:
                        continue
                    # The page may echo the rejected code.  Keep only the fact
                    # of rejection; OTP values must never enter worker logs.
                    logs.append("OpenAI rejected the verification code")
                    try:
                        await otp_field.fill("")
                    except Exception:
                        pass
                    otp_submitted = False

                if otp_attempts >= MAX_OTP_ATTEMPTS:
                    raise CodexLoginError("OpenAI verification code was rejected too many times")

                code = ""
                # A mailbox endpoint can keep returning the same message.  If
                # that code was explicitly rejected, do not poll it again and
                # submit the same stale value three times; ask for a fresh
                # human response instead.
                if email_token and otp_attempts == 0:
                    provider = mail_provider or detect_mail_provider(email)
                    logs.append(f"OTP requested; polling {provider}")
                    try:
                        code = await poll_verification_code(
                            token=email_token,
                            after_ts=otp_poll_start,
                            email=email,
                            provider=provider,
                        )
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        logs.append(
                            f"Mailbox OTP lookup unavailable ({type(exc).__name__}); "
                            "waiting for user input"
                        )

                if not code:
                    wait_started = time.time()
                    code = await manual_reader.read_code(
                        attempt_id=attempt_id,
                        timeout_s=MANUAL_OTP_TIMEOUT,
                        logs=logs,
                    )
                    # Human response time does not consume the automation budget.
                    deadline += time.time() - wait_started

                code = str(code or "").strip()
                if not re.fullmatch(r"\d{6}", code):
                    raise CodexLoginError("OpenAI verification code must be exactly 6 digits")

                await otp_field.fill(code)
                otp_attempts += 1
                otp_submitted = True
                logs.append("OTP entered")
                await _click_continue(page, logs)
                continue

            await _click_continue(page, logs)

        labels = await _visible_action_labels(page)
        logs.append(f"Timed-out page actions: {labels}")
        # OAuth URLs contain state/code values and must not cross back to the
        # Manager in a result error.
        raise CodexLoginError(f"Login flow did not complete within {timeout}s")
    finally:
        if owns_manual_reader:
            manual_reader.close()


def _playwright_context():
    try:
        from playwright.async_api import async_playwright
    except ImportError as exc:
        raise CodexLoginError("playwright is not installed") from exc
    return async_playwright()


async def _drive_browser(
    *,
    authorize_url: str,
    email: str,
    password: str,
    email_token: str,
    timeout: int,
    auth_path: Path,
    logs: list[str],
    mail_provider: str | None,
    attempt_id: str,
    manual_otp_reader: Any,
) -> None:
    """Drive a headed system Chrome browser through OpenAI authentication."""

    async with _playwright_context() as playwright:
        browser = await playwright.chromium.launch(
            headless=False,
            channel="chrome",
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
            ],
        )
        try:
            context = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1280, "height": 900},
                locale="en-US",
            )
            page = await context.new_page()
            logs.append("Navigating to OpenAI authorization page")
            await page.goto(authorize_url, timeout=45_000, wait_until="domcontentloaded")
            await _run_state_machine(
                page=page,
                email=email,
                password=password,
                email_token=email_token,
                timeout=timeout,
                auth_path=auth_path,
                logs=logs,
                mail_provider=mail_provider,
                attempt_id=attempt_id,
                manual_otp_reader=manual_otp_reader,
            )
        finally:
            await browser.close()


def _decode_jwt_payload(token: str) -> dict[str, Any]:
    parts = token.split(".")
    if len(parts) != 3 or not parts[1]:
        raise CodexLoginError("Codex auth id_token is not a valid JWT")
    try:
        encoded = parts[1] + "=" * (-len(parts[1]) % 4)
        decoded = base64.urlsafe_b64decode(encoded.encode("ascii"))
        payload = json.loads(decoded)
    except (UnicodeEncodeError, ValueError, json.JSONDecodeError) as exc:
        raise CodexLoginError("Codex auth id_token is not a valid JWT") from exc
    if not isinstance(payload, dict):
        raise CodexLoginError("Codex auth id_token payload is invalid")
    return payload


def validate_auth_email(auth_path: Path, expected_email: str) -> str:
    """Require an OAuth access token and an exact, case-insensitive JWT email."""

    try:
        data = json.loads(auth_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CodexLoginError("Codex auth.json is unreadable") from exc
    if not isinstance(data, dict):
        raise CodexLoginError("Codex auth.json must contain an object")
    if data.get("auth_mode") != "chatgpt":
        raise CodexLoginError("Codex auth.json is not a ChatGPT OAuth login")

    tokens = data.get("tokens")
    if not isinstance(tokens, dict):
        raise CodexLoginError("Codex auth.json has no OAuth tokens")
    access_token = tokens.get("access_token")
    if not isinstance(access_token, str) or not access_token.strip():
        raise CodexLoginError("Codex auth.json has no access token")
    id_token = tokens.get("id_token")
    if not isinstance(id_token, str) or not id_token:
        raise CodexLoginError("Codex auth.json has no id_token JWT")

    email = _decode_jwt_payload(id_token).get("email")
    if not isinstance(email, str) or not email.strip():
        raise CodexLoginError("Codex id_token JWT has no email")
    actual_email = email.strip()
    if actual_email.casefold() != expected_email.strip().casefold():
        raise CodexLoginError(
            f"Codex authenticated email mismatch: expected {expected_email}, got {actual_email}"
        )
    return actual_email


async def _read_authorize_url(process: asyncio.subprocess.Process) -> str:
    if process.stdout is None:
        raise CodexLoginError("codex login stdout is unavailable")

    deadline = time.time() + AUTH_URL_TIMEOUT
    while time.time() < deadline:
        try:
            raw = await asyncio.wait_for(process.stdout.readline(), timeout=2)
        except asyncio.TimeoutError:
            if process.returncode is not None:
                break
            continue
        if not raw:
            if process.returncode is not None:
                break
            await asyncio.sleep(0.2)
            continue
        clean = ANSI_RE.sub("", raw.decode(errors="replace"))
        match = AUTHORIZE_URL_RE.search(clean)
        if match:
            return match.group(1).rstrip("'\"),]")

    raise CodexLoginError("codex login did not print an authorize URL")


async def _wait_for_auth_json(auth_path: Path) -> None:
    deadline = time.time() + AUTH_JSON_WAIT
    while time.time() < deadline:
        if auth_path.is_file():
            try:
                data = json.loads(auth_path.read_text(encoding="utf-8"))
            except (OSError, UnicodeDecodeError, json.JSONDecodeError):
                await asyncio.sleep(0.2)
                continue
            if isinstance(data, dict):
                return
        await asyncio.sleep(0.2)
    raise CodexLoginError("codex did not write a complete auth.json")


def _kill_process_group(process: asyncio.subprocess.Process) -> None:
    """Kill the isolated CLI process group, falling back for test doubles."""

    if process.returncode is None:
        try:
            pid = getattr(process, "pid", None)
            if isinstance(pid, int) and pid > 0 and os.getpgid(pid) == pid:
                os.killpg(pid, signal.SIGKILL)
            else:
                process.kill()
        except ProcessLookupError:
            pass


async def _stop_process(process: asyncio.subprocess.Process) -> None:
    _kill_process_group(process)
    await process.wait()


def _codex_environment(codex_home: str) -> dict[str, str]:
    environment = dict(os.environ)
    # The smoke test must exercise the OAuth file we just validated, not pass
    # accidentally because the worker inherited an unrelated API key.
    for name in (
        "OPENAI_API_KEY",
        "OPENAI_BASE_URL",
        "OPENAI_API_BASE",
        "CODEX_API_KEY",
    ):
        environment.pop(name, None)
    environment.update(
        {
            "CODEX_HOME": codex_home,
            "NO_COLOR": "1",
            "TERM": "dumb",
        }
    )
    return environment


async def _smoke_test(codex_bin: str, codex_home: str, logs: list[str]) -> bool:
    """Run a real, minimal Codex turn; a nonzero/timeout result is failure."""

    environment = _codex_environment(codex_home)
    process: asyncio.subprocess.Process | None = None
    try:
        # A clean working root plus explicit CLI isolation prevents project
        # instructions, config.toml, hooks, rules, or a custom provider from
        # turning this OAuth verification into a false positive.
        with tempfile.TemporaryDirectory(prefix="elastic-codex-login-smoke-") as workdir:
            process = await asyncio.create_subprocess_exec(
                codex_bin,
                "exec",
                "--ignore-user-config",
                "--ignore-rules",
                "--ephemeral",
                "--sandbox",
                "read-only",
                "--skip-git-repo-check",
                "--cd",
                workdir,
                "Do not use tools. Reply with exactly: LOGIN_OK",
                env=environment,
                start_new_session=True,
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            await asyncio.wait_for(process.communicate(), timeout=SMOKE_TIMEOUT)
        logs.append(f"Smoke test rc={process.returncode}")
        return process.returncode == 0
    except asyncio.TimeoutError:
        if process is not None:
            _kill_process_group(process)
            await process.wait()
        logs.append("Smoke test timed out")
        return False
    except asyncio.CancelledError:
        if process is not None and process.returncode is None:
            _kill_process_group(process)
            await process.wait()
        raise
    except Exception as exc:
        logs.append(f"Smoke test error ({type(exc).__name__})")
        return False


def _redacted_error(exc: Exception, secrets: tuple[str, ...]) -> str:
    # Only our deliberately safe failures retain their detail.  Playwright,
    # subprocess, and third-party mailbox exceptions can embed input values or
    # OAuth URLs in their messages, so expose their type rather than repr/text.
    if isinstance(exc, CodexLoginError):
        message = str(exc) or type(exc).__name__
    else:
        message = f"Codex login automation failed ({type(exc).__name__})"
    for secret in secrets:
        if secret:
            message = message.replace(secret, "[redacted]")
    return message


async def codex_login(
    email: str,
    password: str,
    codex_home: str,
    token_171: str = "",
    timeout: int = DEFAULT_LOGIN_TIMEOUT,
    mail_provider: str | None = None,
    attempt_id: str = "",
    manual_otp_reader: Any = None,
) -> dict[str, Any]:
    """Run password-based Codex login locally and transactionally commit auth."""

    started_at = time.time()
    logs: list[str] = []
    if not email.strip():
        return {"ok": False, "error": "OpenAI email is required", "logs": logs}
    if not password:
        return {"ok": False, "error": "OpenAI password is required", "logs": logs}
    if not codex_home:
        return {"ok": False, "error": "CODEX_HOME is required", "logs": logs}

    codex_bin = shutil.which("codex")
    if not codex_bin:
        return {"ok": False, "error": "codex CLI not found", "logs": logs}
    if not os.environ.get("DISPLAY"):
        return {"ok": False, "error": "DISPLAY is not set; Xvfb is required", "logs": logs}

    home_path = Path(codex_home).expanduser()
    home_path.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(home_path, 0o700)
    auth_path = home_path / "auth.json"
    home_string = str(home_path)
    attempt_id = attempt_id or uuid.uuid4().hex
    logs.append(f"Starting Codex login for {email} (CODEX_HOME={home_string})")

    had_auth = auth_path.exists()
    backup_path: Path | None = None
    process: asyncio.subprocess.Process | None = None
    login_succeeded = False
    failure: dict[str, Any] | None = None

    try:
        if had_auth:
            backup_path = home_path / f".auth.json.login-backup-{uuid.uuid4().hex}"
            os.replace(auth_path, backup_path)
            os.chmod(backup_path, 0o600)

        environment = _codex_environment(home_string)
        process = await asyncio.create_subprocess_exec(
            codex_bin,
            "login",
            env=environment,
            umask=0o077,
            start_new_session=True,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        authorize_url = await _read_authorize_url(process)
        logs.append("Received Codex authorization URL")

        await _drive_browser(
            authorize_url=authorize_url,
            email=email.strip(),
            password=password,
            email_token=token_171,
            timeout=timeout,
            auth_path=auth_path,
            logs=logs,
            mail_provider=mail_provider,
            attempt_id=attempt_id,
            manual_otp_reader=manual_otp_reader,
        )
        await _wait_for_auth_json(auth_path)
        os.chmod(auth_path, 0o600)
        actual_email = validate_auth_email(auth_path, email)
        logs.append(f"Verified Codex identity for {actual_email}")

        logs.append("Running Codex smoke test")
        if not await _smoke_test(codex_bin, home_string, logs):
            raise CodexLoginError("Codex exec smoke test failed")

        login_succeeded = True
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        failure = {
            "ok": False,
            "error": _redacted_error(exc, (password, token_171)),
            "logs": logs,
        }
    finally:
        cleanup_succeeded = False
        try:
            if process is not None:
                await _stop_process(process)
            cleanup_succeeded = True
        finally:
            if login_succeeded and cleanup_succeeded:
                try:
                    if not auth_path.is_file():
                        raise CodexLoginError("Codex auth.json disappeared before commit")
                    os.chmod(auth_path, 0o600)
                    # Delete the rollback copy last: after this operation there
                    # are no remaining commit steps that can fail.
                    if backup_path is not None:
                        backup_path.unlink(missing_ok=True)
                except BaseException:
                    if backup_path is not None and backup_path.exists():
                        os.replace(backup_path, auth_path)
                        os.chmod(auth_path, 0o600)
                    elif not had_auth:
                        auth_path.unlink(missing_ok=True)
                    raise
            elif backup_path is not None and backup_path.exists():
                os.replace(backup_path, auth_path)
                os.chmod(auth_path, 0o600)
            elif not had_auth:
                auth_path.unlink(missing_ok=True)

    if failure is not None:
        return failure

    elapsed = time.time() - started_at
    logs.append(f"Login complete in {elapsed:.1f}s")
    return {"ok": True, "elapsed": elapsed, "logs": logs}


__all__ = [
    "CodexLoginError",
    "ManualOtpReader",
    "codex_login",
    "detect_mail_provider",
    "poll_verification_code",
    "validate_auth_email",
]
