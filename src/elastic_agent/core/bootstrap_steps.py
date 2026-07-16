"""Built-in Bootstrap steps — system init, agent install, runtime deploy, harness code.

T-019~T-022: Standard bootstrap steps that can be composed with Harness-specific steps
to form a complete bootstrap pipeline.
"""

from __future__ import annotations

from elastic_agent.harness.base import BootstrapStep

CLAUDE_CODE_VERSION = "2.1.181"


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
        cmd = (
            f"npm install -g @anthropic-ai/claude-code@{CLAUDE_CODE_VERSION} "
            "--include=optional --foreground-scripts --force && "
            "claude --version"
        )
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
        "pip3 install -q --break-system-packages elastic-agent && "
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
    git_token: str | None = None,
) -> BootstrapStep:
    """T-022: Deploy Harness-specific code — clone repo, install dependencies.

    ``git_token`` clones a private GitHub repo: the token is embedded in the
    clone URL, then stripped from the persisted remote so it doesn't linger in
    ``.git/config`` on the worker.
    """
    if repo_url is None:
        cmd = f"mkdir -p {target_dir} && echo 'No harness repo configured, skipping'"
    else:
        clone_url = repo_url
        strip_token = False
        if git_token and repo_url.startswith("https://github.com/"):
            clone_url = repo_url.replace(
                "https://github.com/", f"https://x-access-token:{git_token}@github.com/"
            )
            strip_token = True
        parts = [
            f"mkdir -p {target_dir}",
            f"git clone --depth 1 --branch {branch} {clone_url} {target_dir}",
        ]
        if strip_token:
            parts.append(f"git -C {target_dir} remote set-url origin {repo_url}")
        if extra_commands:
            # Setup commands (uv sync, pip install, …) run INSIDE the cloned repo
            # so they find pyproject.toml / requirements.txt — same as a human
            # doing `git clone … && cd repo && uv sync`.
            parts.append(f"cd {target_dir}")
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
    """Install worker-local login deps: real Google Chrome + Xvfb + xdotool + httpx/websockets.

    The vendored CCM login flow (``worker/login/cdp_login.py``) drives the real
    ``google-chrome`` binary over CDP and clicks with ``xdotool`` under an Xvfb
    display — it does NOT use Playwright/chromium/mitmproxy. Verified on a live
    Ubuntu 26.04 worker: with only playwright-chromium the login fails because
    the code execs ``google-chrome`` (not on PATH). Extra pip packages can be
    appended via ``login_dependencies``.
    """
    apt = (
        "export DEBIAN_FRONTEND=noninteractive && "
        "apt-get install -y -qq xvfb xdotool wget ca-certificates python3-pip && "
        "wget -q https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb "
        "-O /tmp/google-chrome.deb && "
        "apt-get install -y -qq /tmp/google-chrome.deb"
    )
    pip_pkgs = ["httpx", "websockets"]
    for dep in (login_dependencies or []):
        if dep not in pip_pkgs and dep not in ("chrome", "google-chrome", "xvfb", "xdotool"):
            pip_pkgs.append(dep)
    pip = f"pip3 install -q --break-system-packages {' '.join(pip_pkgs)}"

    return BootstrapStep(
        name="credential-login-deps",
        command=f"{apt} && {pip}",
        timeout=timeout,
        retry_count=1,
        description="Install worker-local login deps (Google Chrome, Xvfb, xdotool, httpx/websockets)",
    )


def pty_install_step(
    pty_package: str = "git+https://github.com/zjw49246/Claude-Code-PTY.git",
    timeout: int = 300,
) -> BootstrapStep:
    """Install claude-pty so the Worker can host agents in PTY sessions."""
    return BootstrapStep(
        name="pty-install",
        command=f"pip3 install -q --break-system-packages {pty_package}",
        timeout=timeout,
        retry_count=1,
        description="Install claude-pty for PTY-hosted agent execution",
    )


PTY_REFRESH_SCRIPT_PATH = "/usr/local/bin/claude-pty-refresh.sh"
CLAUDE_CLI_HEALTH_SCRIPT_PATH = "/usr/local/bin/claude-cli-healthcheck.sh"

# Worker-side mechanical dep sync (CCM refresh_pty.sh pattern): on every runtime
# start — including resume of a stopped instance, which skips bootstrap — compare
# the installed claude-pty commit against the upstream main HEAD and reinstall if
# behind. Offline / failure keeps the current install (ExecStartPre uses `-`).
PTY_REFRESH_SCRIPT = """#!/bin/bash
set -u
export HOME="${HOME:-/root}"
export PATH="/usr/local/bin:/usr/bin:/bin:$PATH"
URL="{pty_repo_url}"
INSTALLED=$(python3 - <<'PYEOF'
import glob, json
g = glob.glob('/usr/local/lib/python3*/dist-packages/claude_pty-*.dist-info/direct_url.json')
print(json.load(open(g[0]))['vcs_info']['commit_id'] if g else '')
PYEOF
)
REMOTE=$(git ls-remote "$URL" refs/heads/main 2>/dev/null | awk '{print $1}')
[ -z "$REMOTE" ] && exit 0
if [ "$INSTALLED" != "$REMOTE" ]; then
  echo "claude-pty: $INSTALLED -> $REMOTE"
  pip3 install -q --break-system-packages --force-reinstall --no-deps "git+$URL@$REMOTE" || exit 0
fi
"""


def pty_refresh_step(
    pty_repo_url: str = "https://github.com/zjw49246/Claude-Code-PTY",
    timeout: int = 60,
) -> BootstrapStep:
    """Install the claude-pty refresh hook: script + ExecStartPre drop-in.

    Closes the resume_node gap — started-from-stopped instances never
    re-bootstrap, so without this they run a stale claude-pty forever.
    """
    script = PTY_REFRESH_SCRIPT.replace("{pty_repo_url}", pty_repo_url)
    cmd = (
        f"cat > {PTY_REFRESH_SCRIPT_PATH} << 'REFRESH'\n"
        f"{script}"
        "REFRESH\n"
        f"chmod +x {PTY_REFRESH_SCRIPT_PATH} && "
        "mkdir -p /etc/systemd/system/elastic-agent-runtime.service.d && "
        "cat > /etc/systemd/system/elastic-agent-runtime.service.d/10-pty-refresh.conf << 'DROPIN'\n"
        "[Service]\n"
        f"ExecStartPre=-/bin/bash {PTY_REFRESH_SCRIPT_PATH}\n"
        "DROPIN\n"
        "systemctl daemon-reload"
    )
    return BootstrapStep(
        name="pty-refresh-hook",
        command=cmd,
        timeout=timeout,
        retry_count=1,
        description="Install claude-pty auto-refresh on runtime start (mechanical dep sync)",
    )


CLAUDE_CLI_HEALTH_SCRIPT = """#!/bin/bash
set -u
export HOME="${HOME:-/root}"
export PATH="/usr/local/bin:/usr/bin:/bin:$PATH"
VERSION="{version}"

health() {
  command -v claude >/dev/null 2>&1 || return 1
  out="$(claude --version 2>&1)" || return 1
  echo "$out" | grep -qi "native binary not installed" && return 1
  case "$out" in
    "$VERSION"*) return 0 ;;
    *) echo "claude-cli-health: expected $VERSION, got: $out" >&2; return 1 ;;
  esac
}

if health; then
  exit 0
fi

echo "claude-cli-health: repairing @anthropic-ai/claude-code@$VERSION" >&2
backup="/root/claude-code-broken-backup-$(date +%Y%m%d%H%M%S)"
mkdir -p "$backup"
find /usr/lib/node_modules/@anthropic-ai -maxdepth 1 -name '.claude-code-*' -exec mv {} "$backup"/ \\; 2>/dev/null || true
find /usr/bin -maxdepth 1 -name '.claude-*' -exec mv {} "$backup"/ \\; 2>/dev/null || true
npm install -g "@anthropic-ai/claude-code@$VERSION" --include=optional --foreground-scripts --force
health
"""


def claude_cli_health_step(
    version: str = CLAUDE_CODE_VERSION,
    timeout: int = 300,
) -> BootstrapStep:
    """Install a startup guard that repairs/verifies the Claude Code CLI."""
    script = CLAUDE_CLI_HEALTH_SCRIPT.replace("{version}", version)
    cmd = (
        f"cat > {CLAUDE_CLI_HEALTH_SCRIPT_PATH} << 'HEALTH'\n"
        f"{script}"
        "HEALTH\n"
        f"chmod +x {CLAUDE_CLI_HEALTH_SCRIPT_PATH} && "
        "mkdir -p /etc/systemd/system/elastic-agent-runtime.service.d && "
        "cat > /etc/systemd/system/elastic-agent-runtime.service.d/20-claude-cli-health.conf << 'DROPIN'\n"
        "[Service]\n"
        f"ExecStartPre=/bin/bash {CLAUDE_CLI_HEALTH_SCRIPT_PATH}\n"
        "DROPIN\n"
        "systemctl daemon-reload"
    )
    return BootstrapStep(
        name="claude-cli-health-hook",
        command=cmd,
        timeout=timeout,
        retry_count=1,
        description="Verify and repair Claude Code CLI before Worker Runtime starts",
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
    include_pty: bool = False,
    pty_package: str | None = None,
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
    if include_pty:
        steps.insert(2, pty_install_step(**({"pty_package": pty_package} if pty_package else {})))
        # refresh hook must land after runtime_deploy_step writes the unit
        steps.append(pty_refresh_step())
        steps.append(claude_cli_health_step())
    if include_login_deps:
        steps.append(credential_login_deps_step(login_dependencies=login_dependencies))
    return steps
