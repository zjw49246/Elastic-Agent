from __future__ import annotations

import asyncio
import base64
import importlib
import json
import logging
import stat
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest

login_module = importlib.import_module("elastic_agent.worker.login.codex_login")


def test_default_browser_login_timeout_leaves_manager_cleanup_headroom():
    assert login_module.DEFAULT_LOGIN_TIMEOUT == 900


def test_163_email_uses_mailcatcher_backend():
    assert login_module.detect_mail_provider("user@163.com") == "mailcatcher"


@pytest.mark.asyncio
async def test_mailbox_query_token_is_never_logged_by_httpx(monkeypatch, caplog):
    secret = "mail-query-token-that-must-not-reach-logs"
    real_client = httpx.AsyncClient

    def handler(request):
        assert secret in str(request.url)
        return httpx.Response(200, json={
            "code": 200,
            "data": {"date": "2099-01-01T00:00:00Z", "code": "123456"},
        })

    def client(**kwargs):
        return real_client(transport=httpx.MockTransport(handler), **kwargs)

    monkeypatch.setattr(login_module.httpx, "AsyncClient", client)
    caplog.set_level(logging.INFO, logger="httpx")
    caplog.set_level(logging.INFO, logger="httpcore")

    code = await login_module.poll_verification_code(
        secret,
        after_ts=0,
        timeout_s=1,
        email="user@163.com",
    )

    assert code == "123456"
    assert secret not in caplog.text
    assert logging.getLogger("httpx").getEffectiveLevel() >= logging.WARNING
    assert logging.getLogger("httpcore").getEffectiveLevel() >= logging.WARNING


def _jwt(email: str) -> str:
    def encode(value: dict) -> str:
        raw = json.dumps(value, separators=(",", ":")).encode()
        return base64.urlsafe_b64encode(raw).decode().rstrip("=")

    return f"{encode({'alg': 'none'})}.{encode({'email': email})}.signature"


def _auth(email: str) -> dict:
    return {
        "auth_mode": "chatgpt",
        "tokens": {
            "id_token": _jwt(email),
            "access_token": "access-token",
            "refresh_token": "refresh-token",
        },
    }


def _write_auth(path: Path, email: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_auth(email)), encoding="utf-8")


class _FakeLoginProcess:
    def __init__(self) -> None:
        self.returncode = None
        self.stdout = SimpleNamespace()
        self.killed = False

    def kill(self) -> None:
        self.killed = True
        self.returncode = -9

    async def wait(self) -> int:
        if self.returncode is None:
            self.returncode = 0
        return self.returncode


def _install_login_fakes(monkeypatch, *, drive_browser, smoke_result=True):
    spawned: dict = {}
    process = _FakeLoginProcess()

    async def spawn(*args, **kwargs):
        spawned["args"] = args
        spawned["kwargs"] = kwargs
        return process

    monkeypatch.setattr(login_module.shutil, "which", lambda name: "/usr/bin/codex")
    monkeypatch.setenv("DISPLAY", ":99")
    monkeypatch.setattr(login_module.asyncio, "create_subprocess_exec", spawn)
    monkeypatch.setattr(
        login_module,
        "_read_authorize_url",
        AsyncMock(return_value="https://auth.openai.com/oauth/authorize?state=secret"),
    )
    monkeypatch.setattr(login_module, "_drive_browser", drive_browser)
    smoke = AsyncMock(return_value=smoke_result)
    monkeypatch.setattr(login_module, "_smoke_test", smoke)
    return spawned, process, smoke


@pytest.mark.asyncio
async def test_password_login_runs_codex_on_worker_and_commits_private_auth(tmp_path, monkeypatch):
    home = tmp_path / "codex-home"
    home.mkdir(mode=0o755)
    monkeypatch.setenv("OPENAI_API_KEY", "must-not-mask-oauth")

    async def drive_browser(**kwargs):
        assert kwargs["password"] == "openai-password"
        assert kwargs["email_token"] == ""
        _write_auth(kwargs["auth_path"], "USER@example.com")

    spawned, process, smoke = _install_login_fakes(
        monkeypatch,
        drive_browser=drive_browser,
    )

    result = await login_module.codex_login(
        email="user@example.com",
        password="openai-password",
        codex_home=str(home),
    )

    assert result["ok"] is True
    assert spawned["args"] == ("/usr/bin/codex", "login")
    assert spawned["kwargs"]["env"]["CODEX_HOME"] == str(home)
    assert "OPENAI_API_KEY" not in spawned["kwargs"]["env"]
    assert spawned["kwargs"]["umask"] == 0o077
    assert spawned["kwargs"]["start_new_session"] is True
    assert spawned["kwargs"]["stdin"] is asyncio.subprocess.DEVNULL
    smoke.assert_awaited_once_with("/usr/bin/codex", str(home), result["logs"])
    assert process.killed is True
    assert stat.S_IMODE(home.stat().st_mode) == 0o700
    assert stat.S_IMODE((home / "auth.json").stat().st_mode) == 0o600
    assert not list(home.glob(".auth.json.login-backup-*"))


@pytest.mark.asyncio
async def test_mail_token_only_login_runs_codex_and_commits_private_auth(tmp_path, monkeypatch):
    home = tmp_path / "codex-home"

    async def drive_browser(**kwargs):
        assert kwargs["password"] == ""
        assert kwargs["email_token"] == "mail-query-token"
        _write_auth(kwargs["auth_path"], "user@example.com")

    spawned, process, smoke = _install_login_fakes(
        monkeypatch,
        drive_browser=drive_browser,
    )

    result = await login_module.codex_login(
        email="user@example.com",
        password="",
        token_171="mail-query-token",
        codex_home=str(home),
    )

    assert result["ok"] is True
    assert spawned["args"] == ("/usr/bin/codex", "login")
    smoke.assert_awaited_once_with("/usr/bin/codex", str(home), result["logs"])
    assert process.killed is True
    assert stat.S_IMODE((home / "auth.json").stat().st_mode) == 0o600


@pytest.mark.asyncio
async def test_token_only_password_page_without_email_code_restores_auth_and_redacts_token(
    tmp_path, monkeypatch,
):
    home = tmp_path / "codex-home"
    home.mkdir()
    auth_path = home / "auth.json"
    old_auth = b'{"old":"credential"}\n'
    auth_path.write_bytes(old_auth)
    mail_token = "mail-query-token-that-must-not-leak"

    class PasswordField:
        async def is_visible(self):
            return True

        async def input_value(self):
            return ""

        async def fill(self, _value):
            raise AssertionError("token-only login must not fill the password field")

    class Locator:
        def __init__(self, elements=()):
            self.elements = list(elements)

        async def count(self):
            return len(self.elements)

        def nth(self, index):
            return self.elements[index]

    class Page:
        async def wait_for_timeout(self, _milliseconds):
            return None

        def locator(self, selector):
            if selector == login_module.PASSWORD_SELECTOR:
                return Locator([PasswordField()])
            return Locator()

    async def drive_browser(**kwargs):
        await login_module._run_state_machine(
            page=Page(),
            email=kwargs["email"],
            password=kwargs["password"],
            email_token=kwargs["email_token"],
            timeout=kwargs["timeout"],
            auth_path=kwargs["auth_path"],
            logs=kwargs["logs"],
            mail_provider=kwargs["mail_provider"],
            attempt_id=kwargs["attempt_id"],
            manual_otp_reader=kwargs["manual_otp_reader"],
        )

    _, process, smoke = _install_login_fakes(
        monkeypatch,
        drive_browser=drive_browser,
    )

    result = await login_module.codex_login(
        email="user@example.com",
        password="",
        token_171=mail_token,
        codex_home=str(home),
    )

    assert result["ok"] is False
    assert "no email-code option" in result["error"]
    assert mail_token not in json.dumps(result)
    assert auth_path.read_bytes() == old_auth
    assert stat.S_IMODE(auth_path.stat().st_mode) == 0o600
    assert process.killed is True
    smoke.assert_not_awaited()


@pytest.mark.asyncio
async def test_identity_mismatch_restores_previous_auth(tmp_path, monkeypatch):
    home = tmp_path / "codex-home"
    home.mkdir()
    auth_path = home / "auth.json"
    old_auth = b'{"old":"credential"}\n'
    auth_path.write_bytes(old_auth)

    async def drive_browser(**kwargs):
        _write_auth(kwargs["auth_path"], "attacker@example.com")

    _, _, smoke = _install_login_fakes(monkeypatch, drive_browser=drive_browser)

    result = await login_module.codex_login(
        email="user@example.com",
        password="openai-password",
        codex_home=str(home),
    )

    assert result["ok"] is False
    assert "email" in result["error"].lower()
    assert auth_path.read_bytes() == old_auth
    assert stat.S_IMODE(auth_path.stat().st_mode) == 0o600
    assert not list(home.glob(".auth.json.login-backup-*"))
    smoke.assert_not_awaited()


@pytest.mark.asyncio
async def test_failed_smoke_test_is_login_failure_and_restores_auth(tmp_path, monkeypatch):
    home = tmp_path / "codex-home"
    home.mkdir()
    auth_path = home / "auth.json"
    old_auth = b"old-auth"
    auth_path.write_bytes(old_auth)

    async def drive_browser(**kwargs):
        _write_auth(kwargs["auth_path"], "user@example.com")

    _install_login_fakes(monkeypatch, drive_browser=drive_browser, smoke_result=False)

    result = await login_module.codex_login(
        email="user@example.com",
        password="openai-password",
        codex_home=str(home),
    )

    assert result["ok"] is False
    assert "smoke" in result["error"].lower()
    assert auth_path.read_bytes() == old_auth
    assert stat.S_IMODE(auth_path.stat().st_mode) == 0o600


@pytest.mark.asyncio
async def test_commit_failure_restores_previous_auth(tmp_path, monkeypatch):
    home = tmp_path / "codex-home"
    home.mkdir()
    auth_path = home / "auth.json"
    old_auth = b"old-auth"
    auth_path.write_bytes(old_auth)

    async def drive_browser(**kwargs):
        _write_auth(kwargs["auth_path"], "user@example.com")

    _install_login_fakes(monkeypatch, drive_browser=drive_browser)
    real_chmod = login_module.os.chmod
    auth_chmods = 0

    def fail_commit_chmod(path, mode):
        nonlocal auth_chmods
        if Path(path) == auth_path and mode == 0o600:
            auth_chmods += 1
            if auth_chmods == 2:
                raise OSError("commit chmod failed")
        return real_chmod(path, mode)

    monkeypatch.setattr(login_module.os, "chmod", fail_commit_chmod)

    with pytest.raises(OSError, match="commit chmod failed"):
        await login_module.codex_login(
            email="user@example.com",
            password="openai-password",
            codex_home=str(home),
        )

    assert auth_path.read_bytes() == old_auth
    assert stat.S_IMODE(auth_path.stat().st_mode) == 0o600
    assert not list(home.glob(".auth.json.login-backup-*"))


@pytest.mark.asyncio
async def test_cancellation_restores_previous_auth_and_propagates(tmp_path, monkeypatch):
    home = tmp_path / "codex-home"
    home.mkdir()
    auth_path = home / "auth.json"
    old_auth = b"old-auth"
    auth_path.write_bytes(old_auth)

    async def drive_browser(**kwargs):
        _write_auth(kwargs["auth_path"], "user@example.com")
        raise asyncio.CancelledError

    _, process, _ = _install_login_fakes(monkeypatch, drive_browser=drive_browser)

    with pytest.raises(asyncio.CancelledError):
        await login_module.codex_login(
            email="user@example.com",
            password="openai-password",
            codex_home=str(home),
        )

    assert process.killed is True
    assert auth_path.read_bytes() == old_auth
    assert stat.S_IMODE(auth_path.stat().st_mode) == 0o600
    assert not list(home.glob(".auth.json.login-backup-*"))


@pytest.mark.asyncio
async def test_real_task_cancellation_restores_previous_auth(tmp_path, monkeypatch):
    home = tmp_path / "codex-home"
    home.mkdir()
    auth_path = home / "auth.json"
    old_auth = b"old-auth"
    auth_path.write_bytes(old_auth)
    browser_started = asyncio.Event()

    async def drive_browser(**kwargs):
        _write_auth(kwargs["auth_path"], "user@example.com")
        browser_started.set()
        await asyncio.Event().wait()

    _install_login_fakes(monkeypatch, drive_browser=drive_browser)
    task = asyncio.create_task(
        login_module.codex_login(
            email="user@example.com",
            password="openai-password",
            codex_home=str(home),
        )
    )
    await browser_started.wait()

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert auth_path.read_bytes() == old_auth
    assert stat.S_IMODE(auth_path.stat().st_mode) == 0o600
    assert not list(home.glob(".auth.json.login-backup-*"))


@pytest.mark.asyncio
async def test_failed_first_login_removes_partial_auth(tmp_path, monkeypatch):
    home = tmp_path / "codex-home"

    async def drive_browser(**kwargs):
        kwargs["auth_path"].write_text("partial", encoding="utf-8")
        raise RuntimeError("browser failed")

    _install_login_fakes(monkeypatch, drive_browser=drive_browser)

    result = await login_module.codex_login(
        email="user@example.com",
        password="openai-password",
        codex_home=str(home),
    )

    assert result["ok"] is False
    assert not (home / "auth.json").exists()
    assert stat.S_IMODE(home.stat().st_mode) == 0o700


@pytest.mark.asyncio
async def test_third_party_failure_does_not_echo_password_or_mail_token(tmp_path, monkeypatch):
    password = "super-secret-password"
    mail_token = "super-secret-mail-token"

    async def drive_browser(**_kwargs):
        raise RuntimeError(f"browser included {password} and {mail_token}")

    _install_login_fakes(monkeypatch, drive_browser=drive_browser)

    result = await login_module.codex_login(
        email="user@example.com",
        password=password,
        token_171=mail_token,
        codex_home=str(tmp_path / "codex-home"),
    )

    serialized = json.dumps(result)
    assert result["ok"] is False
    assert password not in serialized
    assert mail_token not in serialized


def test_validate_auth_email_requires_access_token_and_exact_jwt_email(tmp_path):
    auth_path = tmp_path / "auth.json"
    _write_auth(auth_path, "User@Example.COM")

    assert login_module.validate_auth_email(auth_path, "user@example.com") == "User@Example.COM"

    with pytest.raises(login_module.CodexLoginError, match="email mismatch"):
        login_module.validate_auth_email(auth_path, "prefix-user@example.com")

    data = _auth("user@example.com")
    data["tokens"].pop("access_token")
    auth_path.write_text(json.dumps(data), encoding="utf-8")
    with pytest.raises(login_module.CodexLoginError, match="access token"):
        login_module.validate_auth_email(auth_path, "user@example.com")

    data = _auth("user@example.com")
    data["auth_mode"] = "apikey"
    auth_path.write_text(json.dumps(data), encoding="utf-8")
    with pytest.raises(login_module.CodexLoginError, match="ChatGPT OAuth"):
        login_module.validate_auth_email(auth_path, "user@example.com")


@pytest.mark.asyncio
async def test_state_machine_uses_password_then_injected_manual_otp(tmp_path, monkeypatch):
    auth_path = tmp_path / "auth.json"
    page = SimpleNamespace(state="email", url="https://auth.openai.com/")
    page.wait_for_timeout = AsyncMock()

    class Field:
        def __init__(self) -> None:
            self.value = ""
            self.fills: list[str] = []

        async def input_value(self):
            return self.value

        async def fill(self, value):
            self.value = value
            self.fills.append(value)

    email_field = Field()
    password_field = Field()
    otp_field = Field()

    async def first_visible(_page, selector):
        if page.state == "email" and selector == login_module.EMAIL_SELECTOR:
            return email_field
        if page.state == "password" and selector == login_module.PASSWORD_SELECTOR:
            return password_field
        if page.state == "otp" and selector == login_module.OTP_SELECTOR:
            return otp_field
        return None

    async def click_continue(_page, logs):
        if page.state == "email":
            page.state = "password"
        elif page.state == "password":
            page.state = "otp"
        elif page.state == "otp":
            page.state = "done"
            auth_path.write_text("{}", encoding="utf-8")
        logs.append("continued")
        return True

    reader = SimpleNamespace(read_code=AsyncMock(return_value="654321"))
    monkeypatch.setattr(login_module, "_first_visible", first_visible)
    monkeypatch.setattr(login_module, "_click_continue", click_continue)
    logs: list[str] = []

    await login_module._run_state_machine(
        page=page,
        email="user@example.com",
        password="openai-password",
        email_token="",
        timeout=30,
        auth_path=auth_path,
        logs=logs,
        attempt_id="attempt-1",
        manual_otp_reader=reader,
    )

    assert email_field.fills == ["user@example.com"]
    assert password_field.fills == ["openai-password"]
    assert otp_field.fills == ["654321"]
    reader.read_code.assert_awaited_once()
    assert reader.read_code.await_args.kwargs["attempt_id"] == "attempt-1"
    assert "654321" not in "\n".join(logs)
    assert "openai-password" not in "\n".join(logs)


@pytest.mark.asyncio
async def test_optional_mail_token_is_used_for_otp_without_manual_reader(tmp_path, monkeypatch):
    auth_path = tmp_path / "auth.json"
    page = SimpleNamespace(state="otp", url="https://auth.openai.com/")
    page.wait_for_timeout = AsyncMock()

    class OtpField:
        value = ""

        async def input_value(self):
            return self.value

        async def fill(self, value):
            self.value = value

    otp_field = OtpField()

    async def first_visible(_page, selector):
        if page.state == "otp" and selector == login_module.OTP_SELECTOR:
            return otp_field
        return None

    async def click_continue(_page, _logs):
        page.state = "done"
        auth_path.write_text("{}", encoding="utf-8")
        return True

    poll = AsyncMock(return_value="123456")
    reader = SimpleNamespace(read_code=AsyncMock(side_effect=AssertionError("manual OTP not expected")))
    monkeypatch.setattr(login_module, "_first_visible", first_visible)
    monkeypatch.setattr(login_module, "_click_continue", click_continue)
    monkeypatch.setattr(login_module, "poll_verification_code", poll)

    await login_module._run_state_machine(
        page=page,
        email="user@example.com",
        password="openai-password",
        email_token="mail-query-token",
        timeout=30,
        auth_path=auth_path,
        logs=[],
        attempt_id="attempt-2",
        manual_otp_reader=reader,
    )

    assert otp_field.value == "123456"
    assert poll.await_args.kwargs["token"] == "mail-query-token"
    reader.read_code.assert_not_awaited()


@pytest.mark.asyncio
async def test_token_only_state_machine_switches_password_page_to_email_code(
    tmp_path, monkeypatch,
):
    auth_path = tmp_path / "auth.json"
    page = SimpleNamespace(state="password", wait_for_timeout=AsyncMock())

    class Field:
        def __init__(self):
            self.value = ""
            self.fills: list[str] = []

        async def input_value(self):
            return self.value

        async def fill(self, value):
            self.value = value
            self.fills.append(value)

    password_field = Field()
    otp_field = Field()

    async def first_visible(_page, selector):
        if page.state == "password" and selector == login_module.PASSWORD_SELECTOR:
            return password_field
        if page.state == "otp" and selector == login_module.OTP_SELECTOR:
            return otp_field
        return None

    async def switch_to_email_code(_page, _logs):
        page.state = "otp"
        return True

    async def click_continue(_page, _logs):
        page.state = "done"
        auth_path.write_text("{}", encoding="utf-8")
        return True

    poll = AsyncMock(return_value="123456")
    reader = SimpleNamespace(
        read_code=AsyncMock(side_effect=AssertionError("manual OTP not expected"))
    )
    monkeypatch.setattr(login_module, "_first_visible", first_visible)
    monkeypatch.setattr(login_module, "_switch_to_email_code", switch_to_email_code)
    monkeypatch.setattr(login_module, "_click_continue", click_continue)
    monkeypatch.setattr(login_module, "poll_verification_code", poll)

    await login_module._run_state_machine(
        page=page,
        email="user@example.com",
        password="",
        email_token="mail-query-token",
        timeout=30,
        auth_path=auth_path,
        logs=[],
        attempt_id="attempt-token-only",
        manual_otp_reader=reader,
    )

    assert password_field.fills == []
    assert otp_field.fills == ["123456"]
    poll.assert_awaited_once()
    reader.read_code.assert_not_awaited()


@pytest.mark.asyncio
async def test_token_only_state_machine_handles_direct_method_picker(
    tmp_path, monkeypatch,
):
    auth_path = tmp_path / "auth.json"
    page = SimpleNamespace(state="method-menu", wait_for_timeout=AsyncMock())

    class OtpField:
        async def input_value(self):
            return ""

        async def fill(self, _value):
            return None

    otp_field = OtpField()

    async def first_visible(_page, selector):
        if page.state == "otp" and selector == login_module.OTP_SELECTOR:
            return otp_field
        return None

    async def switch_to_email_code(_page, _logs):
        page.state = "otp"
        return True

    async def click_continue(_page, _logs):
        auth_path.write_text("{}", encoding="utf-8")
        return True

    switch = AsyncMock(side_effect=switch_to_email_code)
    monkeypatch.setattr(login_module, "_first_visible", first_visible)
    monkeypatch.setattr(login_module, "_switch_to_email_code", switch)
    monkeypatch.setattr(login_module, "_click_continue", click_continue)
    monkeypatch.setattr(
        login_module, "poll_verification_code", AsyncMock(return_value="123456")
    )

    await login_module._run_state_machine(
        page=page,
        email="user@example.com",
        password="",
        email_token="mail-query-token",
        timeout=30,
        auth_path=auth_path,
        logs=[],
        manual_otp_reader=SimpleNamespace(
            read_code=AsyncMock(side_effect=AssertionError("manual OTP not expected"))
        ),
    )

    switch.assert_awaited_once()


@pytest.mark.asyncio
async def test_switch_to_email_code_clicks_visible_matching_action():
    class Element:
        def __init__(self, *, text="", visible=True, on_click=None):
            self.text = text
            self.visible = visible
            self.on_click = on_click
            self.clicked = False

        async def is_visible(self):
            return self.visible

        async def inner_text(self):
            return self.text

        async def get_attribute(self, _name):
            return None

        async def click(self):
            self.clicked = True
            if self.on_click:
                self.on_click()

    class Locator:
        def __init__(self, elements):
            self.elements = elements

        async def count(self):
            return len(self.elements)

        def nth(self, index):
            return self.elements[index]

    page = SimpleNamespace(state="password", wait_for_timeout=AsyncMock())
    password_field = Element()
    otp_field = Element()
    hidden = Element(text="Continue with email code", visible=False)
    inert = Element(text="Continue with email code")
    action = Element(
        text="Continue with a one-time code",
        on_click=lambda: setattr(page, "state", "otp"),
    )

    def locator(selector):
        if selector == "button, a, [role=button]":
            return Locator([hidden, inert, action])
        if selector == login_module.PASSWORD_SELECTOR:
            return Locator([password_field] if page.state == "password" else [])
        if selector == login_module.OTP_SELECTOR:
            return Locator([otp_field] if page.state == "otp" else [])
        return Locator([])

    page.locator = locator
    logs: list[str] = []

    assert await login_module._switch_to_email_code(page, logs) is True
    assert hidden.clicked is False
    assert inert.clicked is True
    assert action.clicked is True
    assert logs[-1] == "Switched to email-code login via 'Continue with a one-time code'"


@pytest.mark.asyncio
async def test_switch_to_email_code_handles_intermediate_method_menu():
    class Element:
        def __init__(self, text, on_click):
            self.text = text
            self.on_click = on_click
            self.clicked = False

        async def is_visible(self):
            return True

        async def inner_text(self):
            return self.text

        async def get_attribute(self, _name):
            return None

        async def click(self):
            self.clicked = True
            self.on_click()

    class Locator:
        def __init__(self, elements):
            self.elements = elements

        async def count(self):
            return len(self.elements)

        def nth(self, index):
            return self.elements[index]

    page = SimpleNamespace(state="password", wait_for_timeout=AsyncMock())
    password_field = Element("", lambda: None)
    otp_field = Element("", lambda: None)
    alternate = Element(
        "Try another method",
        lambda: setattr(page, "state", "method-menu"),
    )
    email_code = Element(
        "Email me a code",
        lambda: setattr(page, "state", "otp"),
    )

    def locator(selector):
        if selector == "button, a, [role=button]":
            actions = {
                "password": [alternate],
                "method-menu": [email_code],
            }
            return Locator(actions.get(page.state, []))
        if selector == login_module.PASSWORD_SELECTOR:
            return Locator([password_field] if page.state == "password" else [])
        if selector == login_module.OTP_SELECTOR:
            return Locator([otp_field] if page.state == "otp" else [])
        return Locator([])

    page.locator = locator
    logs: list[str] = []

    assert await login_module._switch_to_email_code(page, logs) is True
    assert alternate.clicked is True
    assert email_code.clicked is True
    assert page.state == "otp"


@pytest.mark.asyncio
async def test_rejected_mailbox_otp_switches_to_manual_instead_of_reusing_it(
    tmp_path, monkeypatch,
):
    auth_path = tmp_path / "auth.json"
    page = SimpleNamespace(url="https://auth.openai.com/")
    page.wait_for_timeout = AsyncMock()

    class OtpField:
        def __init__(self):
            self.value = ""
            self.fills: list[str] = []

        async def input_value(self):
            return self.value

        async def fill(self, value):
            self.value = value
            self.fills.append(value)

    otp_field = OtpField()
    submissions = 0

    async def first_visible(_page, selector):
        return otp_field if selector == login_module.OTP_SELECTOR else None

    async def click_continue(_page, _logs):
        nonlocal submissions
        submissions += 1
        if submissions == 2:
            auth_path.write_text("{}", encoding="utf-8")
        return True

    errors = iter(["The code is incorrect", None])

    async def visible_otp_error(_page):
        return next(errors)

    poll = AsyncMock(return_value="111111")
    reader = SimpleNamespace(read_code=AsyncMock(return_value="222222"))
    monkeypatch.setattr(login_module, "_first_visible", first_visible)
    monkeypatch.setattr(login_module, "_click_continue", click_continue)
    monkeypatch.setattr(login_module, "_visible_otp_error", visible_otp_error)
    monkeypatch.setattr(login_module, "poll_verification_code", poll)
    logs: list[str] = []

    await login_module._run_state_machine(
        page=page,
        email="user@example.com",
        password="openai-password",
        email_token="mail-query-token",
        timeout=30,
        auth_path=auth_path,
        logs=logs,
        attempt_id="attempt-3",
        manual_otp_reader=reader,
    )

    assert otp_field.fills == ["111111", "", "222222"]
    poll.assert_awaited_once()
    reader.read_code.assert_awaited_once()
    assert "111111" not in "\n".join(logs)
    assert "222222" not in "\n".join(logs)


@pytest.mark.asyncio
async def test_manual_otp_reader_emits_challenge_without_code(monkeypatch):
    events: list[dict] = []
    monkeypatch.setattr(login_module, "_emit_login_event", events.append)

    class Stream:
        async def readline(self):
            challenge_id = events[-1]["challenge_id"]
            return json.dumps({"challenge_id": challenge_id, "code": "654321"}).encode() + b"\n"

    reader = login_module.ManualOtpReader()
    monkeypatch.setattr(reader, "_ensure_reader", AsyncMock(return_value=Stream()))
    logs: list[str] = []

    code = await reader.read_code(attempt_id="attempt", timeout_s=30, logs=logs)

    assert code == "654321"
    serialized = json.dumps({"events": events, "logs": logs})
    assert "654321" not in serialized
    assert [event["type"] for event in events] == ["otp_required", "otp_received"]


@pytest.mark.asyncio
async def test_drive_browser_uses_headed_system_chrome_and_dev_shm_guard(tmp_path, monkeypatch):
    calls: dict = {}

    class Page:
        async def goto(self, url, **kwargs):
            calls["goto"] = (url, kwargs)

    class Context:
        async def new_page(self):
            return Page()

    class Browser:
        async def new_context(self, **kwargs):
            calls["context"] = kwargs
            return Context()

        async def close(self):
            calls["closed"] = True

    class Chromium:
        async def launch(self, **kwargs):
            calls["launch"] = kwargs
            return Browser()

    class PlaywrightManager:
        async def __aenter__(self):
            return SimpleNamespace(chromium=Chromium())

        async def __aexit__(self, *_args):
            return False

    state_machine = AsyncMock()
    monkeypatch.setattr(login_module, "_playwright_context", lambda: PlaywrightManager())
    monkeypatch.setattr(login_module, "_run_state_machine", state_machine)

    await login_module._drive_browser(
        authorize_url="https://auth.openai.com/oauth/authorize?state=secret",
        email="user@example.com",
        password="password",
        email_token="",
        timeout=30,
        auth_path=tmp_path / "auth.json",
        logs=[],
        mail_provider=None,
        attempt_id="attempt",
        manual_otp_reader=SimpleNamespace(),
    )

    assert calls["launch"]["headless"] is False
    assert calls["launch"]["channel"] == "chrome"
    assert "--disable-dev-shm-usage" in calls["launch"]["args"]
    assert "user_agent" not in calls["context"]
    assert calls["closed"] is True
    state_machine.assert_awaited_once()


@pytest.mark.asyncio
async def test_click_continue_skips_hidden_matching_button():
    class Button:
        def __init__(self, *, visible):
            self.visible = visible
            self.clicked = False

        async def is_visible(self):
            return self.visible

        async def is_enabled(self):
            return True

        async def click(self):
            self.clicked = True

    hidden = Button(visible=False)
    visible = Button(visible=True)

    class Locator:
        def __init__(self, buttons):
            self.buttons = buttons

        async def count(self):
            return len(self.buttons)

        def nth(self, index):
            return self.buttons[index]

    class Page:
        def locator(self, selector):
            if selector == 'button:has-text("Continue")':
                return Locator([hidden, visible])
            return Locator([])

    logs: list[str] = []
    assert await login_module._click_continue(Page(), logs) is True
    assert hidden.clicked is False
    assert visible.clicked is True
    assert logs == ["Clicked 'Continue'"]


@pytest.mark.asyncio
async def test_state_machine_timeout_does_not_expose_oauth_url(monkeypatch, tmp_path):
    secret_url = "https://auth.openai.com/oauth/authorize?state=secret-state&code=secret-code"
    page = SimpleNamespace(url=secret_url, wait_for_timeout=AsyncMock())
    monkeypatch.setattr(login_module, "_visible_action_labels", AsyncMock(return_value=[]))

    with pytest.raises(login_module.CodexLoginError) as failure:
        await login_module._run_state_machine(
            page=page,
            email="user@example.com",
            password="openai-password",
            email_token="",
            timeout=0,
            auth_path=tmp_path / "auth.json",
            logs=[],
            manual_otp_reader=SimpleNamespace(),
        )

    assert "secret-state" not in str(failure.value)
    assert "secret-code" not in str(failure.value)
    assert secret_url not in str(failure.value)


@pytest.mark.asyncio
async def test_state_machine_timeout_reports_only_safe_page_state(monkeypatch, tmp_path):
    page = SimpleNamespace(
        url="https://auth.openai.com/oauth/authorize?state=secret",
        wait_for_timeout=AsyncMock(),
        title=AsyncMock(return_value="OpenAI"),
    )
    page.locator = lambda _selector: SimpleNamespace(count=AsyncMock(return_value=0))

    async def first_visible(_page, selector):
        return object() if selector == login_module.OTP_SELECTOR else None

    monkeypatch.setattr(login_module, "_first_visible", first_visible)
    monkeypatch.setattr(
        login_module, "_visible_action_labels", AsyncMock(return_value=[])
    )

    with pytest.raises(login_module.CodexLoginError) as failure:
        await login_module._run_state_machine(
            page=page,
            email="user@example.com",
            password="",
            email_token="mail-token",
            timeout=0,
            auth_path=tmp_path / "auth.json",
            logs=[],
            manual_otp_reader=SimpleNamespace(),
        )

    assert "last page state: verification-code form" in str(failure.value)
    assert "secret" not in str(failure.value)


@pytest.mark.asyncio
async def test_timeout_page_state_recognizes_cloudflare_title():
    page = SimpleNamespace(title=AsyncMock(return_value="Just a moment..."))

    state = await login_module._timeout_page_state(page, [])

    assert state == "anti-bot challenge"


@pytest.mark.asyncio
async def test_timeout_page_state_recognizes_visible_challenge_selector(monkeypatch):
    page = SimpleNamespace(title=AsyncMock(return_value="OpenAI"))

    async def first_visible(_page, selector):
        return object() if selector == login_module.BOT_CHALLENGE_SELECTOR else None

    monkeypatch.setattr(login_module, "_first_visible", first_visible)

    state = await login_module._timeout_page_state(page, [])

    assert state == "anti-bot challenge"


@pytest.mark.asyncio
async def test_persistent_cloudflare_challenge_fails_with_bound_eip_guidance(
    monkeypatch, tmp_path,
):
    page = SimpleNamespace(
        title=AsyncMock(return_value="Just a moment..."),
        wait_for_timeout=AsyncMock(),
    )
    monkeypatch.setattr(login_module, "BOT_CHALLENGE_TIMEOUT", 0)

    with pytest.raises(login_module.CodexLoginError) as failure:
        await login_module._run_state_machine(
            page=page,
            email="user@example.com",
            password="",
            email_token="mail-token",
            timeout=30,
            auth_path=tmp_path / "auth.json",
            logs=[],
            manual_otp_reader=SimpleNamespace(),
        )

    assert "anti-bot challenge did not clear" in str(failure.value)
    assert "bound EIP" in str(failure.value)


@pytest.mark.asyncio
async def test_smoke_test_requires_zero_exit_status(monkeypatch):
    calls: list[tuple] = []

    class Process:
        returncode = 7

        async def communicate(self):
            return b"", b"failed"

    async def spawn(*args, **kwargs):
        calls.append((args, kwargs))
        return Process()

    monkeypatch.setattr(login_module.asyncio, "create_subprocess_exec", spawn)
    monkeypatch.setenv("OPENAI_API_KEY", "must-not-mask-oauth")
    monkeypatch.setenv("OPENAI_BASE_URL", "http://local-provider.invalid")
    logs: list[str] = []

    assert await login_module._smoke_test("/usr/bin/codex", "/tmp/codex-home", logs) is False
    args = calls[0][0]
    assert args[:2] == ("/usr/bin/codex", "exec")
    assert "--ignore-user-config" in args
    assert "--ignore-rules" in args
    assert "--ephemeral" in args
    assert args[args.index("--sandbox") + 1] == "read-only"
    assert "--dangerously-bypass-approvals-and-sandbox" not in args
    assert args[-1] == "Do not use tools. Reply with exactly: LOGIN_OK"
    assert calls[0][1]["env"]["CODEX_HOME"] == "/tmp/codex-home"
    assert calls[0][1]["start_new_session"] is True
    assert "OPENAI_API_KEY" not in calls[0][1]["env"]
    assert "OPENAI_BASE_URL" not in calls[0][1]["env"]
    assert logs == ["Smoke test rc=7"]


@pytest.mark.asyncio
async def test_password_or_mail_token_is_required_before_existing_auth_is_touched(
    tmp_path, monkeypatch,
):
    home = tmp_path / "codex-home"
    home.mkdir()
    auth_path = home / "auth.json"
    auth_path.write_bytes(b"old-auth")
    monkeypatch.setattr(login_module.shutil, "which", lambda name: "/usr/bin/codex")
    monkeypatch.setenv("DISPLAY", ":99")

    result = await login_module.codex_login(
        email="user@example.com",
        password="",
        token_171="",
        codex_home=str(home),
    )

    assert result["ok"] is False
    assert "password or mailbox query token" in result["error"].lower()
    assert auth_path.read_bytes() == b"old-auth"
