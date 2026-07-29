"""GenericJobHarness — compile a declarative JobSpec into a Harness.

This is the "declarative" path: a JobSpec (collected by the frontend) becomes a
concrete Harness without anyone writing a Python subclass. The "upload code"
path is the existing ``Harness`` subclass mechanism; ``resolve_harness`` unifies
the two so the BatchOrchestrator only ever consumes a ``Harness``.

    resolve_harness(spec)
      ├─ spec.harness_ref set  → import & instantiate the uploaded subclass
      └─ otherwise             → GenericJobHarness(spec)
"""

from __future__ import annotations

import hashlib
import importlib
import importlib.util
import inspect
import os
import shlex
import sys
from pathlib import Path
from typing import Any

from elastic_agent.core.bootstrap_steps import (
    agent_install_step,
    claude_cli_health_step,
    credential_login_deps_step,
    docker_install_step,
    harness_code_step,
    host_update_hardening_step,
    ipv4_only_egress_step,
    pty_install_step,
    pty_refresh_step,
    runtime_deploy_step,
    system_init_step,
)
from elastic_agent.core.job_spec import JobSpec, SetupStep, WorkerContext
from elastic_agent.harness.base import (
    BootstrapStep,
    FileSyncConfig,
    Harness,
    ScalingSignal,
    WorkerCapacity,
    WorkerLifecycle,
)


class GenericJobHarness(Harness):
    """A Harness driven entirely by a declarative :class:`JobSpec`."""

    def __init__(self, spec: JobSpec) -> None:
        self.spec = spec

    # -- lifecycle / capacity ----------------------------------------------

    def get_worker_lifecycle(self) -> WorkerLifecycle:
        return WorkerLifecycle.PERSISTENT

    def get_worker_capacity(self) -> WorkerCapacity:
        # One long opaque command owns the worker; no concurrent tasks.
        return WorkerCapacity(max_concurrent_tasks=1)

    def get_repo_url(self) -> str | None:
        return self.spec.setup.repo

    # -- bootstrap ---------------------------------------------------------

    def get_bootstrap_steps(self) -> list[BootstrapStep]:
        """Harness-specific steps: clone the repo and run setup commands.

        The standard steps (system-init, agent-install, runtime-deploy, …) are
        prepended by :func:`compile_bootstrap_steps`; this method returns only
        the job-specific provisioning.
        """
        setup = self.spec.setup
        steps = [harness_code_step(
            repo_url=setup.repo,
            branch=setup.checkout_ref,
            target_dir=setup.target_dir,
        )]
        steps.extend(compile_job_setup_steps(self.spec, run_as="ubuntu", wrap_user=False))
        return steps

    # -- accounts ----------------------------------------------------------

    def get_credential_slots(self) -> list[dict[str, str]]:
        """One login slot per account-per-worker.

        With ``per_worker == 1`` the single slot uses the configured
        ``config_dir``. An empty single-slot value lets the selected CLI resolve
        its default from the runtime user's actual HOME. With multiple accounts
        each slot gets a distinct directory; Codex validation requires an
        explicit writable base because the Manager does not guess a remote
        user's home directory.
        """
        acct = self.spec.account
        if acct.mode == "none":
            return []
        n = max(1, acct.per_worker)
        if n == 1:
            return [{"slot_type": acct.group, "config_dir": acct.config_dir}]
        base = acct.config_dir or "/root/.claude"
        return [
            {"slot_type": acct.group, "config_dir": f"{base}-slot-{i}"}
            for i in range(n)
        ]

    # -- scaling / sync ----------------------------------------------------

    def get_scaling_signal(self) -> ScalingSignal | None:
        return ScalingSignal(
            desired_workers=self.spec.fanout.workers,
            reason=f"job:{self.spec.name}",
        )

    def get_file_sync_config(self) -> FileSyncConfig:
        # Result collection is orchestrator-driven (collect.paths); the
        # continuous file-sync watcher stays off for opaque batch jobs.
        return FileSyncConfig(enabled=False)


# ---------------------------------------------------------------------------
# Bootstrap assembly (full sequence for a JobSpec)
# ---------------------------------------------------------------------------


def _resolved_setup_cwd(target_dir: str, cwd: str) -> str:
    value = (cwd or ".").strip()
    if value in ("", "."):
        return target_dir
    if value.startswith("/"):
        return value
    return f"{target_dir.rstrip('/')}/{value}"


def _job_user_command(step: SetupStep, *, cwd: str, run_as: str) -> str:
    """Wrap a setup operation so a root bootstrap still runs it as the Job user."""
    inner: list[str] = []
    for key, value in step.env.items():
        inner.append(f"export {key}={shlex.quote(value)}")
    inner.append(f"cd {shlex.quote(cwd)}")
    inner.append(step.command)
    shell = " && ".join(inner)
    if run_as == "root":
        return f"bash -lc {shlex.quote(shell)}"
    return (
        f"sudo -n -H -u {shlex.quote(run_as)} "
        f"bash -lc {shlex.quote(shell)}"
    )


def compile_job_setup_steps(
    spec: JobSpec,
    *,
    run_as: str,
    wrap_user: bool = True,
    include_source_manifest: bool = True,
) -> list[BootstrapStep]:
    """Compile legacy + structured setup operations into isolated steps.

    ``wrap_user=True`` is used by the root bootstrap pipeline and explicitly
    drops privileges to the provider's Job user. Manager-rsync already creates
    a non-sudo SSH executor for that user and requests raw env/cwd metadata via
    ``wrap_user=False``.
    """
    out: list[BootstrapStep] = []
    if include_source_manifest and spec.setup.resolved_commit:
        expected = shlex.quote(spec.setup.resolved_commit)
        source_check = SetupStep(
            name="source-manifest",
            command=(
                f'test "$(git rev-parse HEAD)" = {expected} || '
                f'(echo "source commit mismatch; expected {expected}" >&2; exit 1)'
            ),
            timeout=60,
        )
        source_steps = [source_check]
    else:
        source_steps = []

    for index, step in enumerate([*source_steps, *spec.setup.normalized_steps()]):
        cwd = _resolved_setup_cwd(spec.setup.target_dir, step.cwd)
        safe_name = "-".join(step.name.strip().lower().split()) or str(index)
        if wrap_user:
            command = _job_user_command(step, cwd=cwd, run_as=run_as)
            env: dict[str, str] = {}
            step_cwd = None
        else:
            command = step.command
            env = dict(step.env)
            step_cwd = cwd
        out.append(BootstrapStep(
            name=f"job-setup-{index + 1}-{safe_name}",
            command=command,
            timeout=step.timeout,
            retry_count=step.retries,
            env=env,
            cwd=step_cwd,
            description=f"Job-owned setup step '{step.name}' as user {run_as}",
        ))
    return out


def compile_bootstrap_steps(
    spec: JobSpec,
    *,
    manager_url: str,
    auth_token: str,
    worker_id: str,
    runtime_port: int = 8080,
    heartbeat_interval: int = 30,
    system_packages: list[str] | None = None,
    include_pty: bool = False,
    include_login_deps: bool | None = None,
    pty_package: str | None = None,
    runtime_from_src: bool = False,
    run_as: str = "ubuntu",
    include_s3_cli: bool = False,
) -> list[BootstrapStep]:
    """Full bootstrap sequence for a declarative job.

    Mirrors ``build_default_bootstrap_steps`` but sources the harness-code step
    from the JobSpec's ``setup`` (repo + commands) instead of a bare repo URL,
    and auto-enables login deps when the job logs accounts in on the worker.

    ``runtime_from_src`` skips the PyPI runtime-deploy (and pty-refresh, which
    patches that unit): the Manager rsyncs the framework source and starts the
    runtime from it via systemd (see make_provision_hook) — used when the worker
    must run this branch, not the published package.
    """
    if include_login_deps is None:
        include_login_deps = spec.account.mode == "worker_local_login"
    # claude-pty and its health/refresh hooks host Claude Code only.  `include_pty`
    # is a Manager-wide provisioning option, so silently narrowing it here keeps
    # a Codex Job usable without installing or invoking Claude-only machinery.
    include_claude_pty = include_pty and spec.account.agent_type == "claude"
    profile = spec.environment.manifest()
    common_packages = system_packages or list(profile["system_packages"])
    if include_s3_cli and "awscli" not in common_packages:
        common_packages.append("awscli")

    steps: list[BootstrapStep] = []
    if spec.account.binding == "eip":
        # The stable EIP is IPv4-only. Apply this before apt, browser login, or
        # any job code gets a chance to prefer a subnet-assigned IPv6 address.
        steps.append(ipv4_only_egress_step())
    steps.extend([
        system_init_step(packages=common_packages),
        agent_install_step(agent_type=spec.account.agent_type),
    ])
    # Docker before any runtime deploy: the runtime user's docker-group
    # membership must exist when systemd starts the unit (see docker_install_step).
    if spec.setup.needs_docker or profile["docker"]:
        steps.append(docker_install_step(run_as=run_as))
    if include_claude_pty:
        steps.insert(2, pty_install_step(**({"pty_package": pty_package} if pty_package else {})))
    if include_login_deps:
        login_dependencies = (
            ["playwright"] if spec.account.agent_type == "codex" else None
        )
        steps.append(
            credential_login_deps_step(
                login_dependencies=login_dependencies
            )
        )
    # All framework-controlled APT fallback paths are above this boundary.
    # A runtime service or task must never be restarted by a background host
    # upgrade or by needrestart.
    steps.append(host_update_hardening_step())
    if not runtime_from_src:
        steps.append(runtime_deploy_step(
            manager_url=manager_url,
            auth_token=auth_token,
            worker_id=worker_id,
            runtime_port=runtime_port,
            heartbeat_interval=heartbeat_interval,
        ))

    # Job-specific provisioning. For manager_rsync *with a repository* the
    # Manager clones + rsyncs the code and runs setup commands (see
    # make_provision_hook), so no worker-side steps are emitted here. A
    # repo-less Job still needs its target directory and declared setup steps.
    setup = spec.setup
    if setup.deliver != "manager_rsync" or not setup.repo:
        clone_step = harness_code_step(
            repo_url=setup.repo, branch=setup.checkout_ref,
            target_dir=setup.target_dir,
        )
        # The bootstrap pipeline is privileged for apt/systemd work. Make the
        # checkout writable by the runtime/Job user before any Job-owned step.
        clone_step.command = (
            f"{clone_step.command} && chown -R {shlex.quote(run_as)}:"
            f"{shlex.quote(run_as)} {shlex.quote(setup.target_dir)}"
        )
        steps.append(clone_step)
        steps.extend(compile_job_setup_steps(spec, run_as=run_as))

    if include_claude_pty and not runtime_from_src:
        steps.append(pty_refresh_step())
        steps.append(claude_cli_health_step())
    return steps


# ---------------------------------------------------------------------------
# EXECUTE assembly
# ---------------------------------------------------------------------------


def build_execute(spec: JobSpec, ctx: WorkerContext, *, resume: bool = False) -> dict[str, Any]:
    """Render a JobSpec into ExecuteMessage kwargs for one worker.

    ``resume=True`` appends ``rotation.resume_args`` (Mode-B rotation restart).
    """
    command = spec.render_resume_command(ctx) if resume else spec.render_command(ctx)
    return {
        "command": command,
        "cwd": spec.resolved_cwd(),
        "env": spec.render_env(ctx),
        "timeout": spec.run.timeout,
    }


# ---------------------------------------------------------------------------
# Harness resolution — declarative vs uploaded code
# ---------------------------------------------------------------------------


def load_harness_class(ref: str) -> type[Harness]:
    """Import a Harness subclass from ``"module.path:ClassName"`` or
    ``"/abs/file.py:ClassName"``.

    File-path refs are loaded as an ad-hoc module so uploaded harness code can
    live outside the import path.
    """
    if ":" not in ref:
        raise ValueError(f"harness_ref must be 'module:Class' or '/path.py:Class', got {ref!r}")
    mod_part, _, cls_name = ref.partition(":")

    if mod_part.endswith(".py") or "/" in mod_part:
        path = Path(mod_part).resolve()
        if not path.is_file():
            raise FileNotFoundError(f"harness file not found: {path}")
        path_fingerprint = hashlib.sha256(
            os.fsencode(path)
        ).hexdigest()[:24]
        mod_name = f"_elastic_harness_{path_fingerprint}"
        loaded = sys.modules.get(mod_name)
        if loaded is None:
            spec_ = importlib.util.spec_from_file_location(mod_name, path)
            if spec_ is None or spec_.loader is None:
                raise ImportError(f"cannot load harness module from {path}")
            loaded = importlib.util.module_from_spec(spec_)
            sys.modules[mod_name] = loaded
            spec_.loader.exec_module(loaded)
        module = loaded
    else:
        module = importlib.import_module(mod_part)

    cls = getattr(module, cls_name, None)
    if cls is None or not (inspect.isclass(cls) and issubclass(cls, Harness)):
        raise TypeError(f"{ref} does not resolve to a Harness subclass")
    return cls


def resolve_harness(spec: JobSpec) -> Harness:
    """Return the Harness driving a job: uploaded subclass if ``harness_ref`` is
    set, else the declarative :class:`GenericJobHarness`.

    Uploaded classes are instantiated with the spec when their constructor
    accepts an argument, else with no args (the spec stays available as
    metadata / fan-out policy).
    """
    if not spec.harness_ref:
        return GenericJobHarness(spec)

    cls = load_harness_class(spec.harness_ref)
    try:
        params = inspect.signature(cls).parameters
    except (ValueError, TypeError):
        params = {}
    if any(p.kind in (p.POSITIONAL_OR_KEYWORD, p.POSITIONAL_ONLY) and p.name != "self"
           for p in params.values()):
        return cls(spec)  # type: ignore[call-arg]
    return cls()
