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
        assert "apt-get" in step.command and "update" in step.command
        assert "python3" in step.command
        assert "cloud-init status --wait" in step.command   # fresh-boot apt lock
        assert "DPkg::Lock::Timeout" in step.command
        assert step.retry_count == 2

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

    def test_git_token_private_repo(self) -> None:
        # Token embedded in clone URL, then stripped from the persisted remote.
        step = harness_code_step(
            repo_url="https://github.com/org/private.git", git_token="ghp_secret",
        )
        assert "https://x-access-token:ghp_secret@github.com/org/private.git" in step.command
        assert "remote set-url origin https://github.com/org/private.git" in step.command

    def test_no_token_no_rewrite(self) -> None:
        step = harness_code_step(repo_url="https://github.com/org/pub.git")
        assert "x-access-token" not in step.command
        assert "remote set-url" not in step.command


class TestRuntimeDeployFromSrc:
    def test_systemd_unit_from_src(self) -> None:
        from elastic_agent.core.bootstrap_steps import runtime_deploy_from_src_step
        step = runtime_deploy_from_src_step(
            manager_url="ws://1.2.3.4:8080/ws/runtime", auth_token="tok",
            worker_id="w1", src_dir="/opt/ea/src", run_as="ubuntu",
        )
        c = step.command
        assert step.name == "runtime-deploy-from-src"
        assert "ea-runtime.service" in c and "Restart=always" in c
        assert "User=ubuntu" in c
        assert "PYTHONPATH=/opt/ea/src" in c
        assert "elastic_agent.worker.runtime_main" in c
        # --opt=value form (not space) so leading-'-' tokens don't break argparse
        assert "--manager-url=ws://1.2.3.4:8080/ws/runtime" in c and "--token=tok" in c
        assert "Xvfb :99" in c                      # display for the login flow
        assert "systemctl restart ea-runtime" in c
        assert c.startswith("set -e\n")
        assert (
            "(systemctl disable --now elastic-agent-runtime.service "
            ">/dev/null 2>&1 || true)\n"
        ) in c
        # The best-effort old-service stop must not turn an earlier install or
        # unit-write failure into a successful bootstrap exit status.
        assert "&& systemctl disable --now" not in c

    def test_token_with_leading_dash_uses_equals_form(self) -> None:
        # Regression: secrets.token_urlsafe can start with '-'. In the space form
        # (`--token -Cu2...`) argparse reads the value as another option and dies with
        # "argument --token: expected one argument" → runtime crash-loops → worker
        # never connects → provision fails ("never connected within 300s").
        from elastic_agent.core.bootstrap_steps import runtime_deploy_from_src_step
        tok = "-Cu2AifsKw6IW8G1T704zqv2S3CJrNvL1wgFYoAxmSI"
        c = runtime_deploy_from_src_step(
            manager_url="ws://1.2.3.4:8080/ws/runtime", auth_token=tok, worker_id="w1",
        ).command
        assert f"--token={tok}" in c
        assert f"--token {tok}" not in c            # never the ambiguous space form

    def test_root_gets_is_sandbox(self) -> None:
        from elastic_agent.core.bootstrap_steps import runtime_deploy_from_src_step
        step = runtime_deploy_from_src_step("u", "t", "w", run_as="root")
        assert "IS_SANDBOX=1" in step.command
        assert "User=root" in step.command


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


class TestDockerInstallStep:
    def test_installs_docker_and_grants_user(self) -> None:
        from elastic_agent.core.bootstrap_steps import docker_install_step
        step = docker_install_step(run_as="ubuntu")
        assert step.name == "docker-install"
        assert "docker.io" in step.command
        assert "docker-buildx" in step.command  # BuildKit builds (--sandbox os)
        assert "usermod -aG docker ubuntu" in step.command
        assert "enable --now docker" in step.command

    def test_run_as_is_parameterised(self) -> None:
        from elastic_agent.core.bootstrap_steps import docker_install_step
        assert "usermod -aG docker ec2-user" in docker_install_step(run_as="ec2-user").command
