"""Tests for built-in Bootstrap steps (T-019~T-022)."""

from __future__ import annotations

import pytest

from elastic_agent.core.bootstrap_steps import (
    agent_install_step,
    build_default_bootstrap_steps,
    credential_login_deps_step,
    harness_code_step,
    runtime_deploy_step,
    system_init_step,
)


class TestSystemInitStep:
    def test_default_packages(self) -> None:
        step = system_init_step()
        assert step.name == "system-init"
        assert "apt-get update" in step.command
        assert "python3" in step.command
        assert step.retry_count == 1

    def test_custom_packages(self) -> None:
        step = system_init_step(packages=["nodejs", "npm"])
        assert "nodejs npm" in step.command

    def test_custom_timeout(self) -> None:
        step = system_init_step(timeout=600)
        assert step.timeout == 600


class TestAgentInstallStep:
    def test_default_command(self) -> None:
        step = agent_install_step()
        assert step.name == "agent-install"
        assert "npm install -g" in step.command
        assert "@anthropic-ai/claude-code@2.1.181" in step.command
        assert "--include=optional" in step.command
        assert "claude --version" in step.command

    def test_custom_string_command(self) -> None:
        step = agent_install_step(agent_install_command="pip install my-agent")
        assert step.command == "pip install my-agent"

    def test_custom_list_command(self) -> None:
        step = agent_install_step(agent_install_command=["pip", "install", "my-agent"])
        assert step.command == "pip install my-agent"


class TestRuntimeDeployStep:
    def test_basic(self) -> None:
        step = runtime_deploy_step(
            manager_url="ws://10.0.0.1:8000/ws/runtime",
            auth_token="secret-token",
            worker_id="w1",
        )
        assert step.name == "runtime-deploy"
        assert "elastic-agent" in step.command
        assert "runtime.yaml" in step.command
        assert "systemctl" in step.command
        assert step.retry_count == 0

    def test_custom_port(self) -> None:
        step = runtime_deploy_step(
            manager_url="ws://10.0.0.1:8000/ws/runtime",
            auth_token="token",
            worker_id="w1",
            runtime_port=9090,
        )
        assert "9090" in step.command

    def test_environment_file_for_storage_env(self) -> None:
        step = runtime_deploy_step(
            manager_url="ws://10.0.0.1:8000/ws/runtime",
            auth_token="token",
            worker_id="w1",
        )
        assert "EnvironmentFile=-/etc/elastic-agent/storage.env" in step.command


class TestHarnessCodeStep:
    def test_with_repo(self) -> None:
        step = harness_code_step(repo_url="https://github.com/test/repo.git")
        assert step.name == "harness-code"
        assert "git clone" in step.command
        assert "test/repo" in step.command

    def test_without_repo(self) -> None:
        step = harness_code_step()
        assert "skipping" in step.command

    def test_extra_commands(self) -> None:
        step = harness_code_step(
            repo_url="https://github.com/test/repo.git",
            extra_commands=["pip install -r requirements.txt"],
        )
        assert "requirements.txt" in step.command

    def test_custom_branch(self) -> None:
        step = harness_code_step(
            repo_url="https://github.com/test/repo.git",
            branch="develop",
        )
        assert "develop" in step.command


class TestBuildDefaultSteps:
    def test_returns_four_steps(self) -> None:
        steps = build_default_bootstrap_steps(
            manager_url="ws://10.0.0.1:8000/ws/runtime",
            auth_token="token",
            worker_id="w1",
        )
        assert len(steps) == 4
        assert steps[0].name == "system-init"
        assert steps[1].name == "agent-install"
        assert steps[2].name == "runtime-deploy"
        assert steps[3].name == "harness-code"

    def test_custom_agent_command(self) -> None:
        steps = build_default_bootstrap_steps(
            manager_url="ws://10.0.0.1:8000/ws/runtime",
            auth_token="token",
            worker_id="w1",
            agent_install_command="pip install custom-agent",
        )
        assert "custom-agent" in steps[1].command

    def test_with_login_deps(self) -> None:
        steps = build_default_bootstrap_steps(
            manager_url="ws://10.0.0.1:8000/ws/runtime",
            auth_token="token",
            worker_id="w1",
            include_login_deps=True,
        )
        assert len(steps) == 5
        assert steps[4].name == "credential-login-deps"

    def test_without_login_deps(self) -> None:
        steps = build_default_bootstrap_steps(
            manager_url="ws://10.0.0.1:8000/ws/runtime",
            auth_token="token",
            worker_id="w1",
            include_login_deps=False,
        )
        assert len(steps) == 4


class TestCredentialLoginDepsStep:
    def test_default_deps(self) -> None:
        # The vendored login flow needs the real google-chrome binary + xdotool
        # + Xvfb + httpx/websockets (verified on a live Ubuntu worker) — NOT
        # playwright/chromium.
        step = credential_login_deps_step()
        assert step.name == "credential-login-deps"
        assert "google-chrome" in step.command
        assert "xdotool" in step.command
        assert "xvfb" in step.command
        assert "httpx" in step.command and "websockets" in step.command
        assert step.timeout == 600
        assert step.retry_count == 1

    def test_custom_deps_appended_as_pip(self) -> None:
        step = credential_login_deps_step(login_dependencies=["some-extra-pkg"])
        assert "some-extra-pkg" in step.command
        # base deps still present
        assert "google-chrome" in step.command

    def test_custom_timeout(self) -> None:
        step = credential_login_deps_step(timeout=900)
        assert step.timeout == 900
