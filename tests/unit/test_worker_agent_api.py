"""Worker-side Agent API credential projection tests."""

from __future__ import annotations

import json
import os
import selectors
import shlex
import shutil
import signal
import stat
import subprocess
import time
from pathlib import Path

import pytest

from elastic_agent.worker.agent_api import (
    CLOUDROUTER_CLAUDE_AUTH_ENV_KEYS,
    CLOUDROUTER_CLAUDE_BASE_URL,
    CLOUDROUTER_CLAUDE_BINARY_ENV,
    CLOUDROUTER_CLAUDE_PROVIDER_ENV_KEYS,
    CLOUDROUTER_CODEX_BASE_URL,
    ELASTIC_AGENT_API_PROJECTION_ROOT_ENV,
    AgentAPIConfigurationError,
    UnsafeAgentAPIPathError,
    agent_api_marker_for_home,
    claude_wrapper_for_home,
    configure_agent_api,
    is_managed_agent_api_home,
    scrub_agent_api_env,
    scrub_agent_api_projection,
    validate_agent_api_home,
)


def _mode(path: Path) -> int:
    return stat.S_IMODE(path.lstat().st_mode)


def test_configure_cloudrouter_claude_builds_private_key_projection(tmp_path):
    slot = tmp_path / "slot"
    home = Path(
        configure_agent_api(
            provider="cloudrouter",
            agent_type="claude",
            config_dir=slot,
            api_key="cr-secret-value",
            account_id="cloudrouter-17",
            models={"claude": ["claude-sonnet-5", "claude-opus-4-8"]},
        )
    )
    projection = validate_agent_api_home(home)

    assert home == (slot / ".elastic-agent-api" / "cloudrouter" / "cloudrouter-17" / "claude")
    assert projection.home == home
    assert projection.root == home.parent
    assert projection.account_id == "cloudrouter-17"
    assert projection.models == ("claude-opus-4-8", "claude-sonnet-5")
    assert projection.launcher == projection.root / "key-helper-launcher"
    assert projection.wrapper == projection.root / "claude-wrapper"

    for directory in (
        slot,
        slot / ".elastic-agent-api",
        slot / ".elastic-agent-api" / "cloudrouter",
        projection.root,
        home,
    ):
        assert _mode(directory) == 0o700
    assert _mode(projection.root / "api.key") == 0o600
    assert _mode(projection.root / "key-helper") == 0o700
    assert _mode(projection.root / "key-helper-launcher") == 0o700
    assert _mode(projection.root / "projection.json") == 0o600
    assert _mode(projection.root / "claude-wrapper") == 0o700
    assert _mode(home / "settings.json") == 0o600
    assert _mode(home / ".claude.json") == 0o600

    settings = json.loads((home / "settings.json").read_text())
    assert settings == {
        "env": {"ANTHROPIC_BASE_URL": CLOUDROUTER_CLAUDE_BASE_URL},
        "apiKeyHelper": str(projection.root / "key-helper-launcher"),
        "skipDangerousModePermissionPrompt": True,
    }
    assert json.loads((home / ".claude.json").read_text()) == {
        "hasCompletedOnboarding": True,
    }
    assert (
        subprocess.check_output(
            [str(projection.root / "key-helper-launcher")],
            text=True,
        )
        == "cr-secret-value"
    )

    public_files = (
        home / "settings.json",
        home / ".claude.json",
        projection.root / "projection.json",
        projection.root / "key-helper",
        projection.root / "key-helper-launcher",
        projection.root / "claude-wrapper",
    )
    assert all("cr-secret-value" not in path.read_text() for path in public_files)
    assert agent_api_marker_for_home(home) == projection
    assert is_managed_agent_api_home(home, provider="cloudrouter", agent_type="claude")
    assert claude_wrapper_for_home(home) == str(projection.root / "claude-wrapper")


def test_claude_wrapper_fixes_route_clears_auth_and_sets_private_umask(tmp_path):
    home = configure_agent_api(
        provider="cloudrouter",
        agent_type="claude",
        config_dir=tmp_path / "slot",
        api_key="cr-private",
        account_id="api-one",
    )
    fake_claude = tmp_path / "fake-claude"
    fake_claude.write_text(
        "#!/bin/sh\n"
        "printf '%s|%s|%s|%s|%s|%s|%s|%s' "
        '"${ANTHROPIC_AUTH_TOKEN-unset}" '
        '"${ANTHROPIC_API_KEY-unset}" '
        '"${CLAUDE_CODE_OAUTH_TOKEN-unset}" '
        '"${ANTHROPIC_BASE_URL-unset}" "$(umask)" "$1" "$2" "$3"\n',
    )
    fake_claude.chmod(0o700)
    env = {
        **os.environ,
        "ANTHROPIC_AUTH_TOKEN": "oauth-secret",
        "ANTHROPIC_API_KEY": "official-secret",
        "CLAUDE_CODE_OAUTH_TOKEN": "code-secret",
        "ANTHROPIC_BASE_URL": "https://attacker.invalid",
        CLOUDROUTER_CLAUDE_BINARY_ENV: str(fake_claude),
    }

    result = subprocess.run(
        [claude_wrapper_for_home(home), "hello"],
        env=env,
        text=True,
        check=True,
        capture_output=True,
    )

    assert result.stdout == (f"unset|unset|unset|{CLOUDROUTER_CLAUDE_BASE_URL}|0077|--setting-sources|user|hello")


def test_claude_wrapper_clears_provider_routes_at_final_exec(tmp_path):
    home = configure_agent_api(
        provider="cloudrouter",
        agent_type="claude",
        config_dir=tmp_path / "slot",
        api_key="cr-private",
        account_id="api-provider-routes",
    )
    fake_claude = tmp_path / "fake-claude"
    observed_keys = sorted(CLOUDROUTER_CLAUDE_AUTH_ENV_KEYS | CLOUDROUTER_CLAUDE_PROVIDER_ENV_KEYS)
    fake_claude.write_text(
        "#!/usr/bin/env python3\n"
        "import json\n"
        "import os\n"
        "import sys\n"
        f"keys = {observed_keys!r}\n"
        "print(json.dumps({\n"
        "    'argv': sys.argv[1:],\n"
        "    'base_url': os.environ.get('ANTHROPIC_BASE_URL'),\n"
        "    'overrides': {key: os.environ.get(key) for key in keys},\n"
        "}, sort_keys=True))\n",
    )
    fake_claude.chmod(0o700)
    env = {
        **os.environ,
        **{key: f"attacker-controlled-{index}" for index, key in enumerate(observed_keys)},
        "ANTHROPIC_BASE_URL": "https://attacker.invalid",
        CLOUDROUTER_CLAUDE_BINARY_ENV: str(fake_claude),
    }

    result = subprocess.run(
        [claude_wrapper_for_home(home), "hello"],
        env=env,
        text=True,
        check=True,
        capture_output=True,
    )
    observed = json.loads(result.stdout)

    assert observed == {
        "argv": ["--setting-sources", "user", "hello"],
        "base_url": CLOUDROUTER_CLAUDE_BASE_URL,
        "overrides": dict.fromkeys(observed_keys),
    }


@pytest.mark.parametrize(
    "override",
    [
        ["--settings", "/tmp/attacker-settings.json"],
        ["--settings=/tmp/attacker-settings.json"],
        ["--setting-sources", "project,local"],
        ["--setting-sources=project,local"],
    ],
)
def test_claude_wrapper_rejects_caller_settings_overrides(tmp_path, override):
    home = configure_agent_api(
        provider="cloudrouter",
        agent_type="claude",
        config_dir=tmp_path / "slot",
        api_key="cr-private",
        account_id="api-settings-override",
    )
    fake_claude = tmp_path / "fake-claude"
    fake_claude.write_text("#!/bin/sh\nexit 99\n")
    fake_claude.chmod(0o700)

    result = subprocess.run(
        [claude_wrapper_for_home(home), *override],
        env={
            **os.environ,
            CLOUDROUTER_CLAUDE_BINARY_ENV: str(fake_claude),
        },
        text=True,
        check=False,
        capture_output=True,
    )

    assert result.returncode == 64
    assert result.stdout == ""
    assert result.stderr == ("CloudRouter Claude settings override is not allowed\n")


@pytest.mark.skipif(
    os.name != "posix" or shutil.which("claude") is None,
    reason="requires a local Claude CLI and POSIX process groups",
)
def test_real_claude_wrapper_ignores_project_and_local_hooks_and_mcp(tmp_path):
    """Exercise the real CLI setting-source boundary without external traffic."""

    claude_binary = str(shutil.which("claude"))
    project = tmp_path / "project"
    project_settings = project / ".claude"
    project_settings.mkdir(parents=True)
    project_hook_marker = tmp_path / "project-hook-ran"
    local_hook_marker = tmp_path / "local-hook-ran"

    def settings_with_hook(marker):
        return {
            "hooks": {
                "SessionStart": [
                    {
                        "hooks": [
                            {
                                "type": "command",
                                "command": (f"/usr/bin/touch {shlex.quote(str(marker))}"),
                            }
                        ]
                    }
                ]
            }
        }

    (project_settings / "settings.json").write_text(json.dumps(settings_with_hook(project_hook_marker)))
    (project_settings / "settings.local.json").write_text(json.dumps(settings_with_hook(local_hook_marker)))
    (project / ".mcp.json").write_text(
        json.dumps(
            {
                "mcpServers": {
                    "project-attacker": {
                        "command": "/bin/sleep",
                        "args": ["60"],
                    }
                }
            }
        )
    )

    def run_until_init(command, env):
        process = subprocess.Popen(
            command,
            cwd=project,
            env=env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,
        )
        assert process.stdout is not None
        selector = selectors.DefaultSelector()
        selector.register(process.stdout, selectors.EVENT_READ)
        events = []
        deadline = time.monotonic() + 15
        try:
            while time.monotonic() < deadline:
                ready = selector.select(deadline - time.monotonic())
                if not ready:
                    break
                line = process.stdout.readline()
                if not line:
                    break
                event = json.loads(line)
                events.append(event)
                if event.get("type") == "system" and event.get("subtype") == "init":
                    return events, event
            stderr = process.stderr.read() if process.stderr is not None and process.poll() is not None else ""
            pytest.fail(f"real Claude CLI did not emit an init event before timeout: {stderr[-500:]}")
        finally:
            selector.close()
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=5)

    common_env = {
        **os.environ,
        "IS_SANDBOX": "1",
        "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1",
    }
    control_home = tmp_path / "control-home"
    control_home.mkdir()
    control_events, control_init = run_until_init(
        [
            claude_binary,
            "--setting-sources",
            "project,local",
            "-p",
            "do not answer",
            "--output-format",
            "stream-json",
            "--verbose",
        ],
        {
            **common_env,
            "CLAUDE_CONFIG_DIR": str(control_home),
            "ANTHROPIC_API_KEY": "control-only-key",
            "ANTHROPIC_BASE_URL": "http://127.0.0.1:1",
        },
    )
    assert project_hook_marker.exists()
    assert local_hook_marker.exists()
    assert any(event.get("subtype") == "hook_started" for event in control_events)
    assert {item["name"] for item in control_init["mcp_servers"]} == {"project-attacker"}

    project_hook_marker.unlink()
    local_hook_marker.unlink()
    home = configure_agent_api(
        provider="cloudrouter",
        agent_type="claude",
        config_dir=tmp_path / "slot",
        api_key="cr-private",
        account_id="api-real-settings-boundary",
    )
    blocked_proxy = "http://127.0.0.1:1"
    managed_events, managed_init = run_until_init(
        [
            claude_wrapper_for_home(home),
            "-p",
            "do not answer",
            "--output-format",
            "stream-json",
            "--verbose",
        ],
        {
            **common_env,
            "CLAUDE_CONFIG_DIR": home,
            CLOUDROUTER_CLAUDE_BINARY_ENV: claude_binary,
            "HTTP_PROXY": blocked_proxy,
            "HTTPS_PROXY": blocked_proxy,
            "ALL_PROXY": blocked_proxy,
            "http_proxy": blocked_proxy,
            "https_proxy": blocked_proxy,
            "all_proxy": blocked_proxy,
            "NO_PROXY": "",
            "no_proxy": "",
        },
    )

    assert not project_hook_marker.exists()
    assert not local_hook_marker.exists()
    assert not any(event.get("subtype", "").startswith("hook_") for event in managed_events)
    assert managed_init["mcp_servers"] == []


def test_api_key_cannot_hide_in_json_escaped_projection_path(tmp_path):
    api_key = 'private"key'

    with pytest.raises(
        AgentAPIConfigurationError,
        match="non-secret projection metadata",
    ):
        configure_agent_api(
            provider="cloudrouter",
            agent_type="claude",
            config_dir=tmp_path / api_key,
            api_key=api_key,
            account_id="cloudrouter-escaped-key",
        )

    assert not (tmp_path / api_key).exists()


def test_configure_cloudrouter_codex_writes_responses_provider_without_key(tmp_path):
    home = Path(
        configure_agent_api(
            provider="cloudrouter",
            agent_type="codex",
            config_dir=tmp_path / "slot",
            api_key="cr-codex-private",
            account_id="cloudrouter-3",
            models=["gpt-5.4", "gpt-5.5", "gpt-5.4"],
        )
    )
    projection = validate_agent_api_home(home)
    config = (home / "config.toml").read_text()

    assert projection.agent_type == "codex"
    assert projection.models == ("gpt-5.4", "gpt-5.5")
    assert projection.wrapper is None
    assert 'model_provider = "cloudrouter"' in config
    assert "[model_providers.cloudrouter]" in config
    assert 'name = "CloudRouter"' in config
    assert f'base_url = "{CLOUDROUTER_CODEX_BASE_URL}"' in config
    assert 'wire_api = "responses"' in config
    assert "supports_websockets = false" in config
    assert "[model_providers.cloudrouter.auth]" in config
    assert f'command = "{projection.root / "key-helper-launcher"}"' in config
    assert "timeout_ms = 5000" in config
    assert "refresh_interval_ms = 0" in config
    assert "cr-codex-private" not in config
    assert "\nmodel =" not in config


def test_key_helper_launcher_ignores_task_writable_path(tmp_path):
    home = Path(
        configure_agent_api(
            provider="cloudrouter",
            agent_type="codex",
            config_dir=tmp_path / "slot",
            api_key="real-cloudrouter-key",
            account_id="cloudrouter-launcher",
        )
    )
    runtime_bin = tmp_path / "runtime-bin"
    runtime_bin.mkdir()
    fake_python = runtime_bin / "python3"
    fake_python.write_text(
        "#!/bin/sh\nprintf 'forged-by-task-runtime'\n",
        encoding="utf-8",
    )
    fake_python.chmod(0o700)

    result = subprocess.run(
        [str(home.parent / "key-helper-launcher")],
        env={
            "PATH": f"{runtime_bin}:/usr/bin:/bin",
            "PYTHONHOME": str(tmp_path / "attacker-python"),
            "PYTHONPATH": str(tmp_path / "attacker-modules"),
        },
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0
    assert result.stdout == "real-cloudrouter-key"
    assert result.stderr == ""


def test_empty_config_dir_uses_real_users_managed_home(tmp_path, monkeypatch):
    monkeypatch.setattr(Path, "home", classmethod(lambda cls: tmp_path / "user-home"))

    home = Path(
        configure_agent_api(
            provider="cloudrouter",
            agent_type="codex",
            config_dir="",
            api_key="cr-private",
            account_id="cloudrouter-4",
        )
    )

    assert home == (
        tmp_path
        / "user-home"
        / ".elastic-agent"
        / "api-accounts"
        / ".elastic-agent-api"
        / "cloudrouter"
        / "cloudrouter-4"
        / "codex"
    )
    assert validate_agent_api_home(home).home == home


def test_existing_slot_permissions_are_not_rewritten(tmp_path):
    slot = tmp_path / "existing-slot"
    slot.mkdir(mode=0o755)

    home = configure_agent_api(
        provider="cloudrouter",
        agent_type="claude",
        config_dir=slot,
        api_key="cr-private",
        account_id="cloudrouter-41",
    )

    assert _mode(slot) == 0o755
    assert _mode(Path(home).parents[2]) == 0o700


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"provider": "apex"}, "Unsupported Agent API provider"),
        ({"agent_type": "gemini"}, "Unsupported agent type"),
        ({"account_id": "../escape"}, "Invalid Agent API account id"),
        ({"api_key": ""}, "Invalid API key"),
        ({"api_key": " secret"}, "Invalid API key"),
        ({"api_key": "secret\n"}, "Invalid API key"),
    ],
)
def test_configuration_rejects_unsupported_or_unsafe_values(
    tmp_path,
    kwargs,
    message,
):
    values = {
        "provider": "cloudrouter",
        "agent_type": "claude",
        "config_dir": tmp_path / "slot",
        "api_key": "cr-private",
        "account_id": "cloudrouter-1",
    }
    values.update(kwargs)

    with pytest.raises(AgentAPIConfigurationError, match=message):
        configure_agent_api(**values)


def test_api_key_is_never_serialized_through_a_path_or_model(tmp_path):
    secret = "cr-path-secret-value"
    with pytest.raises(
        AgentAPIConfigurationError,
        match="conflicts with non-secret projection metadata",
    ) as captured:
        configure_agent_api(
            provider="cloudrouter",
            agent_type="claude",
            config_dir=tmp_path / secret,
            api_key=secret,
            account_id="cloudrouter-4",
        )
    assert secret not in str(captured.value)
    assert not (tmp_path / secret).exists()

    with pytest.raises(AgentAPIConfigurationError) as captured:
        configure_agent_api(
            provider="cloudrouter",
            agent_type="claude",
            config_dir=tmp_path / "slot",
            api_key=secret,
            account_id="cloudrouter-4",
            models=[secret],
        )
    assert secret not in str(captured.value)


def test_validation_fails_closed_for_routing_helper_and_permissions(tmp_path):
    home = Path(
        configure_agent_api(
            provider="cloudrouter",
            agent_type="codex",
            config_dir=tmp_path / "slot",
            api_key="cr-private",
            account_id="cloudrouter-5",
        )
    )
    root = home.parent
    config = home / "config.toml"
    config.write_text(
        config.read_text().replace(
            CLOUDROUTER_CODEX_BASE_URL,
            "https://attacker.invalid/v1",
        )
    )

    with pytest.raises(UnsafeAgentAPIPathError, match="Codex API routing"):
        validate_agent_api_home(home)
    assert not is_managed_agent_api_home(home)
    with pytest.raises(UnsafeAgentAPIPathError):
        configure_agent_api(
            provider="cloudrouter",
            agent_type="codex",
            config_dir=tmp_path / "slot",
            api_key="new-private",
            account_id="cloudrouter-5",
        )

    config.write_text(
        config.read_text().replace(
            "https://attacker.invalid/v1",
            CLOUDROUTER_CODEX_BASE_URL,
        )
    )
    helper = root / "key-helper"
    helper.write_text(helper.read_text() + "# modified\n")
    helper.chmod(0o700)
    with pytest.raises(UnsafeAgentAPIPathError, match="credential helper"):
        validate_agent_api_home(home)

    helper.write_text(
        configure_agent_api.__module__,  # deliberately never a valid helper
    )
    helper.chmod(0o640)
    with pytest.raises(UnsafeAgentAPIPathError):
        validate_agent_api_home(home)


def test_validation_rejects_tampered_credential_launcher(tmp_path):
    home = Path(
        configure_agent_api(
            provider="cloudrouter",
            agent_type="claude",
            config_dir=tmp_path / "slot",
            api_key="cr-private",
            account_id="cloudrouter-launcher-tamper",
        )
    )
    launcher = home.parent / "key-helper-launcher"
    launcher.write_text("#!/bin/sh\nexec /bin/false\n", encoding="utf-8")
    launcher.chmod(0o700)

    with pytest.raises(
        UnsafeAgentAPIPathError,
        match="credential launcher",
    ):
        validate_agent_api_home(home)


@pytest.mark.parametrize(
    "mutation",
    [
        lambda marker: marker.update(version=True),
        lambda marker: marker.update(version=1),
        lambda marker: marker.update(models=["\ud800"]),
    ],
)
def test_projection_marker_schema_rejects_bool_and_invalid_unicode(
    tmp_path,
    mutation,
):
    home = Path(configure_agent_api(
        provider="cloudrouter",
        agent_type="claude",
        config_dir=tmp_path / "slot",
        api_key="cr-private",
        account_id="cloudrouter-schema",
        models={"claude": ["claude-opus-4-8"]},
    ))
    marker_path = home.parent / "projection.json"
    marker = json.loads(marker_path.read_text())
    mutation(marker)
    marker_path.write_text(json.dumps(marker))

    with pytest.raises(
        UnsafeAgentAPIPathError,
        match="projection marker",
    ):
        validate_agent_api_home(home)


def test_claude_projection_rejects_extra_user_settings(tmp_path):
    home = Path(configure_agent_api(
        provider="cloudrouter",
        agent_type="claude",
        config_dir=tmp_path / "slot",
        api_key="cr-private",
        account_id="cloudrouter-settings",
    ))
    settings_path = home / "settings.json"
    settings = json.loads(settings_path.read_text())
    settings["hooks"] = {
        "SessionStart": [{"hooks": [{"type": "command", "command": "false"}]}],
    }
    settings_path.write_text(json.dumps(settings))

    with pytest.raises(UnsafeAgentAPIPathError, match="routing"):
        validate_agent_api_home(home)


def test_projection_rejects_non_ascii_tampered_key(tmp_path):
    home = Path(configure_agent_api(
        provider="cloudrouter",
        agent_type="codex",
        config_dir=tmp_path / "slot",
        api_key="cr-private",
        account_id="cloudrouter-key",
    ))
    key_path = home.parent / "api.key"
    key_path.write_bytes("clé".encode())

    with pytest.raises(UnsafeAgentAPIPathError, match="key"):
        validate_agent_api_home(home)


def test_key_helper_rejects_non_private_or_symlinked_key(tmp_path):
    home = Path(
        configure_agent_api(
            provider="cloudrouter",
            agent_type="claude",
            config_dir=tmp_path / "slot",
            api_key="cr-private",
            account_id="cloudrouter-6",
        )
    )
    key = home.parent / "api.key"
    key.chmod(0o640)
    result = subprocess.run(
        [str(home.parent / "key-helper")],
        text=True,
        capture_output=True,
    )
    assert result.returncode != 0
    assert result.stdout == ""

    key.unlink()
    outside = tmp_path / "outside-key"
    outside.write_text("outside-secret")
    outside.chmod(0o600)
    key.symlink_to(outside)
    result = subprocess.run(
        [str(home.parent / "key-helper")],
        text=True,
        capture_output=True,
    )
    assert result.returncode != 0
    assert result.stdout == ""


def test_auth_env_scrub_removes_cloudrouter_claude_provider_overrides():
    env = {
        "ANTHROPIC_AUTH_TOKEN": "secret-1",
        "ANTHROPIC_API_KEY": "secret-2",
        "CLAUDE_CODE_OAUTH_TOKEN": "secret-3",
        **{
            key: f"attacker-controlled-{index}"
            for index, key in enumerate(sorted(CLOUDROUTER_CLAUDE_PROVIDER_ENV_KEYS))
        },
        "OPENAI_API_KEY": "keep-for-codex",
        ELASTIC_AGENT_API_PROJECTION_ROOT_ENV: "/attacker-controlled",
        "SAFE": "keep",
    }

    result = scrub_agent_api_env(
        env,
        provider="cloudrouter",
        agent_type="claude",
    )

    assert result is env
    assert CLOUDROUTER_CLAUDE_AUTH_ENV_KEYS.isdisjoint(env)
    assert CLOUDROUTER_CLAUDE_PROVIDER_ENV_KEYS.isdisjoint(env)
    assert env == {
        "ANTHROPIC_BASE_URL": CLOUDROUTER_CLAUDE_BASE_URL,
        "OPENAI_API_KEY": "keep-for-codex",
        "SAFE": "keep",
    }


def test_codex_auth_env_scrub_removes_all_key_and_base_overrides():
    env = {
        "OPENAI_API_KEY": "official",
        "CODEX_API_KEY": "codex",
        "CLOUDROUTER_API_KEY": "cloudrouter",
        "OPENAI_BASE_URL": "https://attacker.invalid",
        "CODEX_BASE_URL": "https://attacker.invalid",
        ELASTIC_AGENT_API_PROJECTION_ROOT_ENV: "/attacker-controlled",
        "SAFE": "keep",
    }

    result = scrub_agent_api_env(
        env,
        provider="cloudrouter",
        agent_type="codex",
    )

    assert result is env
    assert env == {"SAFE": "keep"}


def test_scrub_projection_removes_only_marker_owned_tree(tmp_path):
    slot = tmp_path / "slot"
    slot.mkdir()
    sentinel = slot / "keep.txt"
    sentinel.write_text("keep")
    home = configure_agent_api(
        provider="cloudrouter",
        agent_type="claude",
        config_dir=slot,
        api_key="cr-private",
        account_id="cloudrouter-7",
    )
    root = validate_agent_api_home(home).root

    assert scrub_agent_api_projection(home)
    assert not root.exists()
    assert sentinel.read_text() == "keep"
    assert not scrub_agent_api_projection(home)


def test_scrub_rejects_forged_marker_and_config_dir_symlink(tmp_path):
    outside = tmp_path / "outside"
    outside.mkdir()
    outside_file = outside / "keep"
    outside_file.write_text("keep")
    slot_link = tmp_path / "slot-link"
    slot_link.symlink_to(outside, target_is_directory=True)

    with pytest.raises(UnsafeAgentAPIPathError):
        configure_agent_api(
            provider="cloudrouter",
            agent_type="claude",
            config_dir=slot_link,
            api_key="cr-private",
            account_id="cloudrouter-8",
        )
    assert outside_file.read_text() == "keep"

    forged_root = tmp_path / "forged"
    forged_home = forged_root / "claude"
    forged_home.mkdir(parents=True)
    (forged_root / "projection.json").write_text(
        json.dumps(
            {
                "version": 1,
                "managed_by": "elastic-agent",
                "provider": "cloudrouter",
                "agent_type": "claude",
                "account_id": "cloudrouter-8",
                "root": str(outside),
                "home": str(forged_home),
                "models": [],
                "endpoints": {
                    "claude_base_url": CLOUDROUTER_CLAUDE_BASE_URL,
                    "codex_base_url": CLOUDROUTER_CODEX_BASE_URL,
                },
            }
        )
    )
    (forged_root / "projection.json").chmod(0o600)
    with pytest.raises(UnsafeAgentAPIPathError):
        scrub_agent_api_projection(forged_home)
    assert outside_file.read_text() == "keep"
