"""Built-in Bootstrap steps — system init, agent install, runtime deploy, harness code.

T-019~T-022: Standard bootstrap steps that can be composed with Harness-specific steps
to form a complete bootstrap pipeline.
"""

from __future__ import annotations

from elastic_agent.harness.base import BootstrapStep


def system_init_step(
    packages: list[str] | None = None,
    timeout: int = 300,
) -> BootstrapStep:
    """T-019: System initialization — update packages, install base dependencies."""
    pkg_list = " ".join(packages) if packages else "python3 python3-pip git curl"
    return BootstrapStep(
        name="system-init",
        command=(
            "export DEBIAN_FRONTEND=noninteractive && "
            "apt-get update -qq && "
            f"apt-get install -y -qq {pkg_list}"
        ),
        timeout=timeout,
        retry_count=1,
        description="Install system packages and base dependencies",
    )


def agent_install_step(
    agent_install_command: str | list[str] | None = None,
    timeout: int = 300,
) -> BootstrapStep:
    """T-020: Agent installation — install the agent binary (e.g. Claude Code CLI)."""
    if agent_install_command is None:
        cmd = "npm install -g @anthropic-ai/claude-code@latest"
    elif isinstance(agent_install_command, list):
        cmd = " ".join(agent_install_command)
    else:
        cmd = agent_install_command

    return BootstrapStep(
        name="agent-install",
        command=cmd,
        timeout=timeout,
        retry_count=1,
        description="Install agent binary",
    )


def runtime_deploy_step(
    manager_url: str,
    auth_token: str,
    worker_id: str,
    runtime_port: int = 8080,
    heartbeat_interval: int = 30,
    timeout: int = 300,
) -> BootstrapStep:
    """T-021: Deploy and start Worker Runtime — install package, write config, start service."""
    config_content = (
        f"manager_url: {manager_url}\\n"
        f"auth_token: {auth_token}\\n"
        f"worker_id: {worker_id}\\n"
        f"runtime_port: {runtime_port}\\n"
        f"heartbeat_interval: {heartbeat_interval}"
    )

    cmd = (
        "pip3 install -q elastic-agent && "
        "mkdir -p /etc/elastic-agent && "
        f"echo -e '{config_content}' > /etc/elastic-agent/runtime.yaml && "
        "cat > /etc/systemd/system/elastic-agent-runtime.service << 'UNIT'\n"
        "[Unit]\n"
        "Description=Elastic Agent Worker Runtime\n"
        "After=network.target\n"
        "\n"
        "[Service]\n"
        "Type=simple\n"
        "EnvironmentFile=-/etc/elastic-agent/storage.env\n"
        f"ExecStart=/usr/bin/python3 -m elastic_agent.worker.runtime --config /etc/elastic-agent/runtime.yaml\n"
        "Restart=always\n"
        "RestartSec=5\n"
        "\n"
        "[Install]\n"
        "WantedBy=multi-user.target\n"
        "UNIT\n"
        "systemctl daemon-reload && "
        "systemctl enable elastic-agent-runtime && "
        "systemctl start elastic-agent-runtime"
    )

    return BootstrapStep(
        name="runtime-deploy",
        command=cmd,
        timeout=timeout,
        retry_count=0,
        description="Deploy and start Worker Runtime service",
    )


def harness_code_step(
    repo_url: str | None = None,
    branch: str = "main",
    target_dir: str = "/opt/elastic-agent/harness",
    extra_commands: list[str] | None = None,
    timeout: int = 300,
) -> BootstrapStep:
    """T-022: Deploy Harness-specific code — clone repo, install dependencies."""
    if repo_url is None:
        cmd = f"mkdir -p {target_dir} && echo 'No harness repo configured, skipping'"
    else:
        parts = [
            f"mkdir -p {target_dir}",
            f"git clone --depth 1 --branch {branch} {repo_url} {target_dir}",
        ]
        if extra_commands:
            parts.extend(extra_commands)
        cmd = " && ".join(parts)

    return BootstrapStep(
        name="harness-code",
        command=cmd,
        timeout=timeout,
        retry_count=1,
        description="Deploy Harness-specific code and dependencies",
    )


def credential_login_deps_step(
    login_dependencies: list[str] | None = None,
    timeout: int = 600,
) -> BootstrapStep:
    """T-042: Install auto-login dependencies (Playwright, mitmproxy, Chrome, Xvfb)."""
    deps = login_dependencies or [
        "playwright", "playwright-stealth", "mitmproxy", "chrome"
    ]

    install_cmds = []
    for dep in deps:
        if dep == "chrome":
            install_cmds.append(
                "apt-get install -y -qq xvfb && "
                "pip3 install -q playwright playwright-stealth && "
                "playwright install chromium --with-deps"
            )
        elif dep == "mitmproxy":
            install_cmds.append("pip3 install -q mitmproxy")
        elif dep in ("playwright", "playwright-stealth"):
            install_cmds.append(f"pip3 install -q {dep}")
        else:
            install_cmds.append(f"pip3 install -q {dep}")

    cmd = " && ".join(install_cmds)
    return BootstrapStep(
        name="credential-login-deps",
        command=cmd,
        timeout=timeout,
        retry_count=1,
        description="Install auto-login dependencies (Playwright, mitmproxy, Chrome)",
    )


def build_default_bootstrap_steps(
    manager_url: str,
    auth_token: str,
    worker_id: str,
    agent_install_command: str | list[str] | None = None,
    repo_url: str | None = None,
    runtime_port: int = 8080,
    heartbeat_interval: int = 30,
    system_packages: list[str] | None = None,
    include_login_deps: bool = False,
    login_dependencies: list[str] | None = None,
) -> list[BootstrapStep]:
    """Build the standard bootstrap sequence (4 or 5 steps depending on login deps)."""
    steps = [
        system_init_step(packages=system_packages),
        agent_install_step(agent_install_command=agent_install_command),
        runtime_deploy_step(
            manager_url=manager_url,
            auth_token=auth_token,
            worker_id=worker_id,
            runtime_port=runtime_port,
            heartbeat_interval=heartbeat_interval,
        ),
        harness_code_step(repo_url=repo_url),
    ]
    if include_login_deps:
        steps.append(credential_login_deps_step(login_dependencies=login_dependencies))
    return steps
