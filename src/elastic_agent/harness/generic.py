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

import importlib
import importlib.util
import inspect
import os
import sys
from pathlib import Path
from typing import Any

from elastic_agent.core.bootstrap_steps import (
    agent_install_step,
    claude_cli_health_step,
    credential_login_deps_step,
    harness_code_step,
    pty_install_step,
    pty_refresh_step,
    runtime_deploy_step,
    system_init_step,
)
from elastic_agent.core.job_spec import JobSpec, WorkerContext
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
        return [
            harness_code_step(
                repo_url=setup.repo,
                branch=setup.branch,
                target_dir=setup.target_dir,
                extra_commands=list(setup.commands),
            )
        ]

    # -- accounts ----------------------------------------------------------

    def get_credential_slots(self) -> list[dict[str, str]]:
        """One login slot per account-per-worker.

        With ``per_worker == 1`` the single slot uses the configured
        ``config_dir`` (empty → Claude's default ~/.claude). With multiple
        accounts per worker each slot gets a distinct dir so credentials don't
        clobber each other.
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

    steps: list[BootstrapStep] = [
        system_init_step(packages=system_packages),
        agent_install_step(),
    ]
    if not runtime_from_src:
        steps.append(runtime_deploy_step(
            manager_url=manager_url,
            auth_token=auth_token,
            worker_id=worker_id,
            runtime_port=runtime_port,
            heartbeat_interval=heartbeat_interval,
        ))
    if include_pty:
        steps.insert(2, pty_install_step(**({"pty_package": pty_package} if pty_package else {})))

    # Job-specific provisioning. For manager_rsync delivery the Manager clones +
    # rsyncs the code and runs setup commands (see make_provision_hook), so no
    # worker-side clone step here. For worker_clone, clone on the worker (token
    # from ELASTIC_AGENT_GIT_TOKEN for private repos — never in the JobSpec).
    setup = spec.setup
    if setup.deliver != "manager_rsync":
        steps.append(harness_code_step(
            repo_url=setup.repo, branch=setup.branch, target_dir=setup.target_dir,
            extra_commands=list(setup.commands),
            git_token=os.environ.get("ELASTIC_AGENT_GIT_TOKEN") or None,
        ))

    if include_pty and not runtime_from_src:
        steps.append(pty_refresh_step())
        steps.append(claude_cli_health_step())
    if include_login_deps:
        steps.append(credential_login_deps_step())
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
        "timeout": spec.run.timeout or None,
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
        mod_name = f"_elastic_harness_{path.stem}"
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
