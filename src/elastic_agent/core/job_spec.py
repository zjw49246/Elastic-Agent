"""Declarative JobSpec — a task described as data, not code.

A JobSpec captures everything the frontend needs to collect to run an arbitrary
job across a fleet: how to provision each worker (setup), what long-running
command to run (run), how accounts are supplied (account), how to react when an
account is exhausted (rotation), and how to fan the same job out across N
workers (fanout).

Two task shapes coexist in Elastic-Agent:

- **Mode A — Elastic-hosted agent**: task == a prompt; Elastic hosts Claude Code
  in a PTY and rotates credentials per turn. This is the existing path and does
  NOT use JobSpec.
- **Mode B — opaque long command** (e.g. a benchmark harness that spawns its own
  sandboxes and consumes the account internally): task == an arbitrary shell
  command. Elastic provisions, logs an account in on the worker, runs the
  command, watches stdout for exhaustion, and — on rotation strategy
  ``on_exhaust_restart_resume`` — swaps credentials and restarts with the
  command's own ``--resume``. JobSpec describes Mode-B jobs.

The declarative JobSpec is compiled into bootstrap steps + an ExecuteMessage by
``elastic_agent.harness.generic.GenericJobHarness``. Jobs that need real code
(custom event handlers, dynamic scheduling) set ``harness_ref`` to point at an
uploaded Harness subclass instead — see the harness registry.
"""

from __future__ import annotations

import re
import shlex
from dataclasses import asdict, dataclass, field
from typing import Literal

from pydantic import BaseModel, Field

# ---------------------------------------------------------------------------
# Template rendering — Manager renders {{var}} before dispatch; shell-native
# constructs like $(hostname -s) are left untouched for the worker's shell.
# ---------------------------------------------------------------------------

_TEMPLATE_RE = re.compile(r"\{\{\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*\}\}")


def render_template(text: str, ctx: dict[str, object]) -> str:
    """Substitute ``{{name}}`` placeholders from ``ctx``.

    Unknown variables raise ``KeyError`` so typos surface at dispatch instead of
    silently producing a broken command. Shell syntax (``$(...)``, ``$VAR``,
    ``&&``) is not touched — it is evaluated later by the worker's shell.
    """
    def repl(m: re.Match) -> str:
        key = m.group(1)
        if key not in ctx:
            raise KeyError(f"unknown template variable: {{{{{key}}}}}")
        return str(ctx[key])

    return _TEMPLATE_RE.sub(repl, text)


@dataclass
class WorkerContext:
    """Per-worker values injected into rendered commands/paths.

    ``hostname`` may be empty when the Manager does not know the worker's short
    hostname; commands can still use ``$(hostname -s)`` at runtime. ``shard_index``
    is the Manager-assigned 0-based index used for explicit sharding.
    """

    shard_index: int = 0
    num_shards: int = 1
    hostname: str = ""
    account_id: str = ""
    account_email: str = ""
    config_dir: str = ""
    job_name: str = ""

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


# ---------------------------------------------------------------------------
# Spec sections
# ---------------------------------------------------------------------------


class SetupSpec(BaseModel):
    """How to provision each worker before the run command (bootstrap-time)."""

    repo: str | None = None
    branch: str = "main"
    target_dir: str = "/opt/elastic-agent/harness"
    commands: list[str] = Field(default_factory=list)


class RunSpec(BaseModel):
    """The long-running command that consumes the Claude account."""

    command: str
    env: dict[str, str] = Field(default_factory=dict)
    cwd: str = "."
    # None / 0 == no wall-clock limit (these jobs run for hours by design).
    timeout: int | None = None
    # Mode-B commands are user-authored shell one-liners ($(hostname -s), &&,
    # env expansion). When True the rendered command is wrapped as
    # ``bash -lc "<cmd>"``; when False it is shlex-split into a bare argv.
    shell: bool = True


class AccountSpec(BaseModel):
    """How the Claude account is supplied on each worker."""

    # worker_local_login: worker runs the login flow locally (P3 ACCOUNT_LOGIN),
    #   credentials never transit the Manager.
    # manager_distribute: Manager sends already-obtained tokens (CREDENTIAL_LOGIN).
    # none: caller has already provisioned credentials; Elastic does nothing.
    mode: Literal["worker_local_login", "manager_distribute", "none"] = "worker_local_login"
    per_worker: int = 1
    group: str = "standard"
    # Where credentials are written on the worker. Empty == Claude's default
    # (~/.claude); the run command inherits it without CLAUDE_CONFIG_DIR. An
    # absolute path is required when per_worker > 1 (distinct dirs per account).
    config_dir: str = ""


class RotationSpec(BaseModel):
    """Coarse-grained (Mode-B) account rotation policy.

    ``on_exhaust_restart_resume`` (strategy "a"): when the run command's stdout
    trips the rate-limit detectors, interrupt it, log a fresh account into the
    same config_dir, and restart the command with ``resume_args`` appended so the
    harness skips already-completed work.
    """

    strategy: Literal["none", "on_exhaust_restart_resume"] = "none"
    # Extra args appended verbatim (after rendering) when restarting, e.g.
    #   --resume "results/opus48_$(hostname -s)_seed128"
    resume_args: str = ""
    max_rotations: int = 20


class FanoutSpec(BaseModel):
    """How the same job spreads across the fleet."""

    workers: int = 1
    # hostname: each worker shards itself via $(hostname -s) in the command
    #   (zero coordination — the simplest fan-out).
    # shard_index: Manager assigns {{shard_index}}/{{num_shards}} for explicit
    #   partitioning.
    # none: identical command everywhere (outputs must not collide).
    shard_by: Literal["hostname", "shard_index", "none"] = "hostname"


class CollectSpec(BaseModel):
    """What to pull back from each worker when the job finishes."""

    paths: list[str] = Field(default_factory=list)


class CompletionSpec(BaseModel):
    """How to decide a worker's run is done."""

    on_process_exit: int = 0


class JobSpec(BaseModel):
    """A complete declarative description of a fan-out job."""

    name: str
    setup: SetupSpec = Field(default_factory=SetupSpec)
    run: RunSpec
    account: AccountSpec = Field(default_factory=AccountSpec)
    rotation: RotationSpec = Field(default_factory=RotationSpec)
    fanout: FanoutSpec = Field(default_factory=FanoutSpec)
    collect: CollectSpec = Field(default_factory=CollectSpec)
    completion: CompletionSpec = Field(default_factory=CompletionSpec)
    # Escape hatch: "module.path:ClassName" of an uploaded Harness subclass.
    # When set, GenericJobHarness is bypassed and the referenced Harness drives
    # bootstrap/login/execute — the declarative fields become defaults/metadata.
    harness_ref: str | None = None

    # -- rendering helpers --------------------------------------------------

    def worker_contexts(self) -> list[WorkerContext]:
        """One context per worker for the configured fan-out."""
        n = max(1, self.fanout.workers)
        return [
            WorkerContext(shard_index=i, num_shards=n, job_name=self.name)
            for i in range(n)
        ]

    def render_command(self, ctx: WorkerContext) -> list[str]:
        """Produce the ExecuteMessage argv for one worker.

        Renders ``{{var}}`` placeholders, then either wraps in ``bash -lc`` (shell
        mode) or shlex-splits into a bare argv.
        """
        rendered = render_template(self.run.command, ctx.as_dict())
        if self.run.shell:
            return ["bash", "-lc", rendered]
        return shlex.split(rendered)

    def render_resume_command(self, ctx: WorkerContext) -> list[str]:
        """Like ``render_command`` but with ``rotation.resume_args`` appended.

        Used by the Mode-B rotation restart so the harness resumes instead of
        re-running completed work.
        """
        base = render_template(self.run.command, ctx.as_dict())
        extra = render_template(self.rotation.resume_args, ctx.as_dict()).strip()
        rendered = f"{base} {extra}".strip() if extra else base
        if self.run.shell:
            return ["bash", "-lc", rendered]
        return shlex.split(rendered)

    def render_env(self, ctx: WorkerContext) -> dict[str, str]:
        """Env for the run command, with CLAUDE_CONFIG_DIR injected when a
        non-default config_dir is configured."""
        env = {k: render_template(v, ctx.as_dict()) for k, v in self.run.env.items()}
        cfg = ctx.config_dir or self.account.config_dir
        if cfg:
            env.setdefault("CLAUDE_CONFIG_DIR", cfg)
        return env
