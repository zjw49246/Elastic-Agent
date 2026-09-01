"""Built-in Bootstrap steps — system init, agent install, runtime deploy, harness code.

T-019~T-022: Standard bootstrap steps that can be composed with Harness-specific steps
to form a complete bootstrap pipeline.
"""

from __future__ import annotations

import re
import shlex
from typing import Literal

from elastic_agent.harness.base import BootstrapStep

CLAUDE_CODE_VERSION = "2.1.181"
CODEX_CLI_VERSION = "0.144.6"
GOLDEN_IMAGE_VERIFY_PATH = "/usr/local/bin/elastic-agent-image-verify"
BACKGROUND_UPDATE_UNITS = (
    "apt-daily.timer",
    "apt-daily-upgrade.timer",
    "apt-daily.service",
    "apt-daily-upgrade.service",
    "unattended-upgrades.service",
)
PREINSTALLED_SYSTEM_COMMANDS = {
    # Ubuntu Noble has no awscli APT candidate. Worker images install the
    # pinned AWS CLI v2 bundle, so bootstrap verifies that immutable image
    # dependency instead of trying to replace it with the legacy v1 package.
    "awscli": "aws",
}


def _golden_verify_command(component: str, args: list[str] | None = None) -> str:
    """Return a shell-safe, fail-closed golden-image verifier invocation."""
    return shlex.join([GOLDEN_IMAGE_VERIFY_PATH, component, *(args or [])])


def _golden_fast_path(
    component: str,
    args: list[str],
    success_message: str,
    fallback: str,
) -> str:
    """Use baked state only after manifest *and* live-state verification.

    A missing verifier, stale manifest, version drift, broken command, or failed
    Python import all select the complete legacy install path.  The marker is
    never trusted by itself.
    """
    verify = _golden_verify_command(component, args)
    return (
        f"if test -x {shlex.quote(GOLDEN_IMAGE_VERIFY_PATH)} && {verify}; then\n"
        f"  echo {shlex.quote(success_message)}\n"
        "else\n"
        f"  {fallback}\n"
        "fi"
    )


def ipv4_only_egress_step(timeout: int = 60) -> BootstrapStep:
    """Disable IPv6 before an EIP-bound account is logged in.

    AWS Elastic IPs provide a stable IPv4 identity only.  A dual-stack subnet
    could otherwise let Chrome/Claude prefer a fresh per-instance IPv6 address
    and silently bypass the account's binding.  Persist both the current and
    future-interface settings, then fail provisioning if the kernel did not
    apply them.
    """
    return BootstrapStep(
        name="ipv4-only-egress",
        command=(
            "install -d -m 0755 /etc/sysctl.d && "
            "printf '%s\\n' "
            "'net.ipv6.conf.all.disable_ipv6 = 1' "
            "'net.ipv6.conf.default.disable_ipv6 = 1' "
            "'net.ipv6.conf.lo.disable_ipv6 = 1' "
            "> /etc/sysctl.d/99-elastic-agent-eip-ipv4.conf && "
            "sysctl --system >/dev/null && "
            "test \"$(cat /proc/sys/net/ipv6/conf/all/disable_ipv6)\" = 1"
        ),
        timeout=timeout,
        retry_count=1,
        description="Force EIP-bound jobs to use the account's stable IPv4 exit",
    )


def system_init_step(
    packages: list[str] | None = None,
    timeout: int = 300,
) -> BootstrapStep:
    """T-019: System initialization — update packages, install base dependencies.

    Defaults include node/npm (Claude Code CLI is an npm package) and rsync
    (manager_rsync code delivery) so a blank Ubuntu image is provisionable.
    """
    package_names = list(packages or [
        "python3", "python3-pip", "git", "curl", "rsync", "nodejs", "npm",
    ])
    apt_packages = [
        package for package in package_names
        if package not in PREINSTALLED_SYSTEM_COMMANDS
    ]
    fallback_commands: list[str] = []
    if apt_packages:
        pkg_list = shlex.join(apt_packages)
        fallback_commands.append(
            "apt-get -o DPkg::Lock::Timeout=600 update -qq && "
            f"apt-get -o DPkg::Lock::Timeout=600 install -y -qq {pkg_list}"
        )
    fallback_commands.extend(
        f"command -v {shlex.quote(command)} >/dev/null && "
        f"{shlex.quote(command)} --version >/dev/null 2>&1"
        for package, command in PREINSTALLED_SYSTEM_COMMANDS.items()
        if package in package_names
    )
    fallback = " && ".join(fallback_commands)
    return BootstrapStep(
        name="system-init",
        command=(
            "set -e\n"
            "export DEBIAN_FRONTEND=noninteractive\n"
            # A fresh instance runs cloud-init / unattended-upgrades on boot,
            # holding the apt lock — wait for it, and give apt a lock timeout so
            # install doesn't fail instantly on a just-booted machine.
            "cloud-init status --wait 2>/dev/null || true\n"
            + _golden_fast_path(
                "system",
                package_names,
                "golden image system packages verified",
                fallback,
            )
        ),
        timeout=timeout,
        retry_count=2,
        description="Install system packages and base dependencies",
    )


def host_update_hardening_step(timeout: int = 300) -> BootstrapStep:
    """Disable host package automation before the Worker Runtime starts.

    Ubuntu 24.04+ runs needrestart in automatic mode from APT hooks.  A daily
    upgrade can consequently restart ``ea-runtime.service`` and kill opaque
    Mode-B children in its cgroup.  Explicit bootstrap APT commands have
    already completed when this step runs; afterwards, background package
    activity is disabled and needrestart is constrained to reporting only.
    """
    units = " ".join(BACKGROUND_UPDATE_UNITS)
    return BootstrapStep(
        name="host-update-hardening",
        command=(
            "set -e\n"
            "install -d -m 0755 /etc/apt/apt.conf.d\n"
            "cat > /etc/apt/apt.conf.d/99elastic-agent-no-background-upgrades "
            "<<'APTCONF'\n"
            'APT::Periodic::Enable "0";\n'
            'APT::Periodic::Update-Package-Lists "0";\n'
            'APT::Periodic::Download-Upgradeable-Packages "0";\n'
            'APT::Periodic::AutocleanInterval "0";\n'
            'APT::Periodic::Unattended-Upgrade "0";\n'
            "APTCONF\n"
            "chmod 0644 /etc/apt/apt.conf.d/"
            "99elastic-agent-no-background-upgrades\n"
            "install -d -m 0755 /etc/needrestart/conf.d\n"
            "cat > /etc/needrestart/conf.d/99-elastic-agent.conf "
            "<<'NEEDRESTART'\n"
            "$nrconf{restart} = 'l';\n"
            "$nrconf{blacklist_rc} = [] "
            "unless ref($nrconf{blacklist_rc}) eq 'ARRAY';\n"
            "push @{$nrconf{blacklist_rc}}, "
            "qr/^(?:ea-runtime|elastic-agent-runtime|"
            "ea-task-supervisor|elastic-agent-task-supervisor|"
            "ea-task@.+|elastic-agent-task@.+)\\.service$/;\n"
            "NEEDRESTART\n"
            "chmod 0644 /etc/needrestart/conf.d/99-elastic-agent.conf\n"
            # Stop future triggers first.  Do not SIGTERM a package manager
            # that may be completing a boot-time transaction; the service
            # units are masked below and any already-active oneshot is allowed
            # to finish before provisioning proceeds.
            "systemctl disable --now apt-daily.timer "
            "apt-daily-upgrade.timer >/dev/null 2>&1 || true\n"
            "systemctl disable unattended-upgrades.service "
            ">/dev/null 2>&1 || true\n"
            f"systemctl mask --force {units}\n"
            "for unit in apt-daily.service apt-daily-upgrade.service; do\n"
            "  while systemctl is-active --quiet \"$unit\"; do sleep 2; done\n"
            "done\n"
            f"for unit in {units}; do\n"
            "  state=$(systemctl is-enabled \"$unit\" 2>/dev/null || true)\n"
            "  test \"$state\" = masked\n"
            "done"
        ),
        timeout=timeout,
        retry_count=1,
        description=(
            "Disable unattended host upgrades and make needrestart report-only"
        ),
    )


def docker_install_step(run_as: str = "ubuntu", timeout: int = 420) -> BootstrapStep:
    """Install Docker Engine and add the runtime user to the ``docker`` group.

    Needed for jobs whose run command uses Docker (e.g. ai4sci-bench
    ``--sandbox os``). MUST run before the runtime systemd unit starts: systemd
    resolves a service's supplementary groups at start time, so the runtime (and
    its child run command) only gets docker-socket access if ``usermod`` ran
    first. Runs sudo-wrapped (SSHExecutor sudoes non-root users)."""
    apt_update = "apt-get -o DPkg::Lock::Timeout=600 update -qq"
    apt_install = "apt-get -o DPkg::Lock::Timeout=600 install -y -qq"
    fallback = (
        "if command -v docker >/dev/null 2>&1; then\n"
        "  if docker buildx version >/dev/null 2>&1; then\n"
        "    echo 'worker image Docker dependencies verified'\n"
        "  elif dpkg-query -W docker-ce-cli >/dev/null 2>&1; then\n"
        # Task Platform images use Docker CE. Installing Ubuntu's docker.io on
        # top of it makes APT's resolver fail; add only the matching plugin.
        f"    {apt_update} && {apt_install} docker-buildx-plugin\n"
        "  else\n"
        f"    {apt_update} && {apt_install} docker-buildx\n"
        "  fi\n"
        "else\n"
        # A blank Ubuntu image still gets the complete engine + BuildKit path.
        f"  {apt_update} && {apt_install} docker.io docker-buildx\n"
        "fi"
    )
    install_or_verify = _golden_fast_path(
        "docker", [], "golden image Docker dependencies verified", fallback,
    )
    return BootstrapStep(
        name="docker-install",
        command=(
            "set -e\n"
            "export DEBIAN_FRONTEND=noninteractive\n"
            "cloud-init status --wait 2>/dev/null || true\n"
            f"{install_or_verify}\n"
            f"usermod -aG docker {shlex.quote(run_as)}\n"
            "systemctl enable --now docker\n"
            "docker --version\n"
            "docker buildx version"
        ),
        timeout=timeout,
        retry_count=2,
        description="Install Docker (+buildx) and grant the runtime user socket access",
    )


def agent_install_step(
    agent_install_command: str | list[str] | None = None,
    timeout: int = 300,
    *,
    agent_type: Literal["claude", "codex"] = "claude",
) -> BootstrapStep:
    """T-020: Install the selected, version-pinned coding-agent CLI."""
    if agent_install_command is None:
        if agent_type == "codex":
            fallback = (
                f"npm install -g @openai/codex@{CODEX_CLI_VERSION} "
                "--include=optional --foreground-scripts --force && "
                "codex --version"
            )
            cmd = _golden_fast_path(
                "agent",
                ["codex", CODEX_CLI_VERSION],
                "golden image Codex CLI verified",
                fallback,
            )
        else:
            fallback = (
                f"npm install -g @anthropic-ai/claude-code@{CLAUDE_CODE_VERSION} "
                "--include=optional --foreground-scripts --force && "
                "claude --version"
            )
            cmd = _golden_fast_path(
                "agent",
                ["claude", CLAUDE_CODE_VERSION],
                "golden image Claude CLI verified",
                fallback,
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


def runtime_deploy_from_src_step(
    manager_url: str,
    auth_token: str,
    worker_id: str,
    src_dir: str = "/opt/elastic-agent/framework/src",
    run_as: str = "ubuntu",
    display: str = ":99",
    runtime_deps: list[str] | None = None,
    agent_type: Literal["claude", "codex"] = "claude",
    timeout: int = 300,
) -> BootstrapStep:
    """Run the Worker Runtime from rsync'd framework source (not PyPI).

    The framework is delivered by the Manager (manager_rsync) to ``src_dir``; this
    installs the runtime's Python deps, writes a wrapper (starts Xvfb for the
    login flow + execs the runtime from src with PYTHONPATH) and a systemd unit
    (``Restart=always``, ``User=run_as``) so the worker stays connected across SSH
    disconnects — the robust replacement for a foreground-held SSH session.
    """
    dependency_names = runtime_deps or [
        "pydantic", "pydantic-settings", "websockets", "httpx", "psutil",
    ]
    deps = shlex.join(dependency_names)
    dependency_install = _golden_fast_path(
        "python",
        dependency_names,
        "golden image runtime Python dependencies verified",
        f"pip3 install -q --break-system-packages {deps}",
    )
    home = "/root" if run_as == "root" else f"/home/{run_as}"
    task_socket = "/run/elastic-agent-task-supervisor/control.sock"
    task_wrapper = (
        "#!/bin/bash\n"
        "set -eu\n"
        f"export HOME={home}\n"
        f"export PYTHONPATH={src_dir}\n"
        f"export PATH={home}/.local/bin:/usr/local/bin:/usr/bin:/bin\n"
        f"exec python3 -m elastic_agent.worker.task_supervisor "
        f"--socket={task_socket} --state-dir={home}/ea-tasks "
        f"--log-dir={home}/ea-logs\n"
    )
    wrapper = (
        "#!/bin/bash\n"
        "set -u\n"
        f"export HOME={home}\n"
        f"export DISPLAY={display}\n"
        f"export ELASTIC_AGENT_AGENT_TYPE={agent_type}\n"
        f"export ELASTIC_AGENT_TASK_SUPERVISOR_SOCKET={task_socket}\n"
        f"export PYTHONPATH={src_dir}\n"
        f"export PATH={home}/.local/bin:/usr/local/bin:/usr/bin:/bin:$PATH\n"
        + ("export IS_SANDBOX=1\n" if run_as == "root" else "")
        + f'pkill -f "Xvfb {display}" 2>/dev/null; rm -f /tmp/.X{display[1:]}-lock\n'
        f"Xvfb {display} -screen 0 1280x1024x24 >/tmp/ea-xvfb.log 2>&1 &\n"
        "sleep 1\n"
        f"exec python3 -m elastic_agent.worker.runtime_main "
        # Use --opt=value (not --opt value): auth_token is secrets.token_urlsafe,
        # which can begin with '-'. In the space form argparse reads the leading-'-'
        # token as another option → "argument --token: expected one argument" → the
        # runtime crash-loops, never connects its WS, and provision fails with
        # "worker never connected within 300s". The '=' form is unambiguous.
        f"--manager-url={manager_url} --token={auth_token} --worker-id={worker_id} "
        f"--log-dir={home}/ea-logs\n"
    )
    unit = (
        "[Unit]\n"
        "Description=Elastic Agent Worker Runtime (from src)\n"
        "Wants=ea-task-supervisor.service\n"
        "After=network.target ea-task-supervisor.service\n\n"
        "[Service]\n"
        "Type=simple\n"
        f"User={run_as}\n"
        "ExecStart=/bin/bash /usr/local/bin/ea-runtime.sh\n"
        "Restart=always\n"
        "RestartSec=5\n\n"
        "[Install]\n"
        "WantedBy=multi-user.target\n"
    )
    task_unit = (
        "[Unit]\n"
        "Description=Elastic Agent Independent Mode-B Task Supervisor\n"
        "After=network.target\n\n"
        "[Service]\n"
        "Type=simple\n"
        f"User={run_as}\n"
        "RuntimeDirectory=elastic-agent-task-supervisor\n"
        "RuntimeDirectoryMode=0700\n"
        "ExecStart=/bin/bash /usr/local/bin/ea-task-supervisor.sh\n"
        "Restart=always\n"
        "RestartSec=5\n"
        "KillMode=control-group\n\n"
        "[Install]\n"
        "WantedBy=multi-user.target\n"
    )
    cmd = (
        # Keep every setup command as a simple command under ``set -e``.  An
        # ``... && old-service-stop || true`` tail would otherwise make Bash
        # treat an earlier pip/file-write failure as success and restart a
        # stale runtime.
        "set -e\n"
        f"{dependency_install}\n"
        # This step runs sudo-wrapped (root), so a bare mkdir makes ea-logs
        # root-owned — but the runtime runs as ``run_as``. It would then fail to
        # open per-task log files, crashing _monitor_process before it reports
        # the process exit → the Manager's run phase sticks at RUNNING and
        # collect/S3 never fire. chown it to the runtime user.
        f"mkdir -p {home}/ea-logs\n"
        f"mkdir -p {home}/ea-tasks\n"
        f"chown {run_as} {home}/ea-logs {home}/ea-tasks\n"
        f"chmod 700 {home}/ea-logs {home}/ea-tasks\n"
        "cat > /usr/local/bin/ea-task-supervisor.sh << 'TASKWRAP'\n"
        f"{task_wrapper}"
        "TASKWRAP\n"
        "chmod +x /usr/local/bin/ea-task-supervisor.sh\n"
        "cat > /usr/local/bin/ea-runtime.sh << 'WRAP'\n"
        f"{wrapper}"
        "WRAP\n"
        "chmod +x /usr/local/bin/ea-runtime.sh\n"
        "cat > /etc/systemd/system/ea-runtime.service << 'UNIT'\n"
        f"{unit}"
        "UNIT\n"
        "cat > /etc/systemd/system/ea-task-supervisor.service << 'TASKUNIT'\n"
        f"{task_unit}"
        "TASKUNIT\n"
        # A baked AMI may still have the old PyPI runtime unit enabled.  Stop it
        # before starting the source-pinned runtime so connection readiness can
        # only be satisfied by the worker that implements current login checks.
        "(systemctl disable --now elastic-agent-runtime.service "
        ">/dev/null 2>&1 || true)\n"
        "systemctl daemon-reload\n"
        "systemctl enable ea-task-supervisor\n"
        "systemctl restart ea-task-supervisor\n"
        "systemctl enable ea-runtime\n"
        "systemctl restart ea-runtime"
    )
    return BootstrapStep(
        name="runtime-deploy-from-src",
        command=cmd,
        timeout=timeout,
        retry_count=1,
        description="Run Worker Runtime from rsync'd framework src via systemd (+Xvfb)",
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
    display. Codex's CCM-derived flow drives that same system Chrome through
    Playwright, supplied through ``login_dependencies=["playwright"]``; it does
    not require Playwright's bundled Chromium. Extra pip packages can be
    appended via ``login_dependencies``.
    """
    apt = (
        "export DEBIAN_FRONTEND=noninteractive && "
        "cloud-init status --wait 2>/dev/null || true; "
        "apt-get -o DPkg::Lock::Timeout=600 update -qq && "
        "apt-get -o DPkg::Lock::Timeout=600 install -y -qq xvfb xdotool wget ca-certificates python3-pip && "
        "wget -q https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb "
        "-O /tmp/google-chrome.deb && "
        "apt-get -o DPkg::Lock::Timeout=600 install -y -qq /tmp/google-chrome.deb"
    )
    pip_pkgs = ["httpx", "websockets"]
    for dep in (login_dependencies or []):
        if dep not in pip_pkgs and dep not in ("chrome", "google-chrome", "xvfb", "xdotool"):
            pip_pkgs.append(dep)
    pip = f"pip3 install -q --break-system-packages {shlex.join(pip_pkgs)}"
    install_or_verify = _golden_fast_path(
        "login",
        pip_pkgs,
        "golden image login dependencies verified",
        f"{apt} && {pip}",
    )

    return BootstrapStep(
        name="credential-login-deps",
        command=install_or_verify,
        timeout=timeout,
        retry_count=1,
        description="Install worker-local login deps (Google Chrome, Xvfb, xdotool, httpx/websockets)",
    )


def pty_install_step(
    pty_package: str = "git+https://github.com/zjw49246/Claude-Code-PTY.git",
    timeout: int = 300,
) -> BootstrapStep:
    """Install claude-pty so the Worker can host agents in PTY sessions."""
    fallback = (
        "pip3 install -q --break-system-packages "
        f"{shlex.quote(pty_package)}"
    )
    # An unpinned branch cannot safely use a baked commit: upstream may have
    # advanced since image creation.  Only a full commit in the requested VCS
    # URL is eligible for the offline fast path.
    match = re.search(r"(?:@|#)([0-9a-fA-F]{40})(?:$|[&#])", pty_package)
    command = fallback
    if match:
        commit = match.group(1).lower()
        command = _golden_fast_path(
            "pty", [commit], "golden image claude-pty commit verified", fallback,
        )
    return BootstrapStep(
        name="pty-install",
        command=command,
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
find /usr/lib/node_modules/@anthropic-ai -maxdepth 1 -name '.claude-code-*' \
  -exec mv {} "$backup"/ \\; 2>/dev/null || true
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
    """Build the standard bootstrap sequence with host-update hardening."""
    steps = [
        system_init_step(packages=system_packages),
        agent_install_step(agent_install_command=agent_install_command),
    ]
    if include_pty:
        steps.append(
            pty_install_step(
                **({"pty_package": pty_package} if pty_package else {})
            )
        )
    if include_login_deps:
        steps.append(
            credential_login_deps_step(login_dependencies=login_dependencies)
        )
    # No framework-controlled APT command may run after this boundary.
    steps.extend([
        host_update_hardening_step(),
        runtime_deploy_step(
            manager_url=manager_url,
            auth_token=auth_token,
            worker_id=worker_id,
            runtime_port=runtime_port,
            heartbeat_interval=heartbeat_interval,
        ),
        harness_code_step(repo_url=repo_url),
    ])
    if include_pty:
        # refresh hook must land after runtime_deploy_step writes the unit
        steps.append(pty_refresh_step())
        steps.append(claude_cli_health_step())
    return steps
