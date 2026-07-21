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
from dataclasses import asdict, dataclass
from pathlib import PurePosixPath
from typing import Literal

from pydantic import BaseModel, Field, field_validator, model_validator

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


class S3Dataset(BaseModel):
    """A dataset to stage onto each worker from S3 before the run.

    The **Manager** downloads ``uri`` (it has S3 creds) and rsyncs it to the
    worker at ``dest`` — workers have no IAM profile / S3 creds, mirroring how
    results flow back (worker→Manager→S3). ``uri`` may be a single object or a
    prefix (trailing ``/`` → recursive sync)."""

    uri: str                      # s3://bucket/key or s3://bucket/prefix/
    dest: str                     # absolute path on the worker


class SetupSpec(BaseModel):
    """How to provision each worker before the run command (bootstrap-time)."""

    repo: str | None = None
    branch: str = "main"
    target_dir: str = "/opt/elastic-agent/harness"
    commands: list[str] = Field(default_factory=list)
    # How the code reaches the worker:
    #  worker_clone : the worker `git clone`s the repo itself (fine for public;
    #    private needs a token pushed to the worker).
    #  manager_rsync: the Manager clones the repo locally (token stays on the
    #    Manager) then rsyncs the checkout (minus .git) to the worker — the token
    #    never touches the worker. Preferred for private repos.
    deliver: Literal["worker_clone", "manager_rsync"] = "worker_clone"
    # Install Docker in bootstrap (before the runtime starts, so the runtime user
    # is in the docker group). Required for jobs whose run uses Docker, e.g.
    # ai4sci-bench `--sandbox os`.
    needs_docker: bool = False
    # Datasets staged from S3 onto each worker before the run (Manager-side pull
    # → rsync; workers need no S3 creds).
    s3_datasets: list[S3Dataset] = Field(default_factory=list)


class RunSpec(BaseModel):
    """The long-running command that consumes the selected agent account."""

    command: str
    env: dict[str, str] = Field(default_factory=dict)
    cwd: str = "."
    # None / 0 == no wall-clock limit (these jobs run for hours by design).
    timeout: int | None = Field(default=None, ge=0, le=2_592_000)
    # Mode-B commands are user-authored shell one-liners ($(hostname -s), &&,
    # env expansion). When True the rendered command is wrapped as
    # ``bash -lc "<cmd>"``; when False it is shlex-split into a bare argv.
    shell: bool = True


class AccountSpec(BaseModel):
    """How the selected agent account is supplied on each worker."""

    # Selects both the worker-local login implementation and the CLI credential
    # environment used by the later run.  Keep Claude as the compatibility
    # default for existing JobSpec payloads.
    agent_type: Literal["claude", "codex"] = "claude"

    # worker_local_login: worker runs the login flow locally (P3 ACCOUNT_LOGIN),
    #   after the Manager sends the selected email + write-only mailbox token;
    #   generated Claude OAuth credentials stay on the worker and are not sent
    #   back. Remote worker transport is required to use WSS.
    # manager_distribute: Manager sends already-obtained tokens (CREDENTIAL_LOGIN).
    # none: caller has already provisioned credentials; Elastic does nothing.
    mode: Literal["worker_local_login", "manager_distribute", "none"] = "worker_local_login"
    per_worker: int = Field(default=1, ge=1, le=32)
    group: str = "standard"
    # eip: each account keeps a durable EIP identity while the EC2 instance is
    # ephemeral. ``ids`` optionally pins one explicit account to each fan-out
    # worker; an empty list lets the allocator choose from ``group``.
    binding: Literal["none", "eip"] = "none"
    ids: list[str] = Field(default_factory=list)
    # Where credentials are written on the worker. Empty == the selected CLI's
    # default (~/.claude or ~/.codex). An absolute path is required when
    # per_worker > 1 (distinct dirs per account).
    config_dir: str = ""

    @field_validator("config_dir")
    @classmethod
    def require_absolute_config_dir(cls, config_dir: str) -> str:
        """Keep login and the later Job process on one credential path.

        Empty means the selected agent's default directory on the worker. Every
        explicit value is also injected into the run environment, so relative
        paths would resolve against two different working directories.
        """
        value = config_dir.strip()
        if value and not PurePosixPath(value).is_absolute():
            raise ValueError("account.config_dir must be empty or an absolute path")
        return value

    @field_validator("ids")
    @classmethod
    def normalize_ids(cls, ids: list[str]) -> list[str]:
        """Trim and stably de-duplicate explicit account IDs."""
        unique: list[str] = []
        seen: set[str] = set()
        for raw_id in ids:
            account_id = raw_id.strip()
            if account_id and account_id not in seen:
                seen.add(account_id)
                unique.append(account_id)
        return unique


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
    max_rotations: int = Field(default=20, ge=0, le=100)


class FanoutSpec(BaseModel):
    """How the same job spreads across the fleet."""

    # A single malformed/API request must not fan out an unbounded bill. The
    # account/EIP model can be extended with a deployment-specific quota later;
    # this hard ceiling is the last-resort control-plane guardrail.
    workers: int = Field(default=1, ge=1, le=100)
    # hostname: each worker shards itself via $(hostname -s) in the command
    #   (zero coordination — the simplest fan-out).
    # shard_index: Manager assigns {{shard_index}}/{{num_shards}} for explicit
    #   partitioning.
    # none: identical command everywhere (outputs must not collide).
    shard_by: Literal["hostname", "shard_index", "none"] = "hostname"
    # Names the fleet: instances get an EC2 Name tag "<name_prefix>-<i>" so you
    # can spot your machines in the console. Empty → provider default naming.
    name_prefix: str = ""
    # Per-job machine size / region (override the Manager's provider defaults).
    # Empty → the Manager's configured default (AMI, subnet, SG, key are always
    # taken from the Manager config, not per-job).
    instance_type: str = ""
    region: str = ""
    # Root disk size (GiB) and spot pricing, per-job. disk_gb=0 → provider
    # default (InstanceConfig.root_disk_size_gb). Bump disk_gb for jobs whose
    # run builds heavy sandboxes/venvs (e.g. ai4sci-bench) — the default is tight.
    disk_gb: int = Field(default=0, ge=0, le=2048)
    spot: bool = False


class CollectSpec(BaseModel):
    """What to pull back from each worker, and how often."""

    paths: list[str] = Field(default_factory=list)
    # Pull results back periodically WHILE the run is going (seconds), not only
    # on completion — so long runs stream partial results to the Manager → S3 as
    # tasks finish, and a run that quota-outs/fails partway still yields whatever
    # completed. 0 = collect only at the end.
    interval_seconds: int = Field(default=0, ge=0, le=86_400)

    @field_validator("paths")
    @classmethod
    def safe_relative_paths(cls, paths: list[str]) -> list[str]:
        normalized: list[str] = []
        for raw in paths:
            value = raw.strip().rstrip("/")
            path = PurePosixPath(value)
            if (
                not value
                or value == "."
                or path.is_absolute()
                or ".." in path.parts
            ):
                raise ValueError(
                    "collect.paths entries must be non-empty relative paths "
                    "without '..'"
                )
            normalized.append(value)
        return normalized


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

    @model_validator(mode="after")
    def validate_agent_account_mode(self) -> JobSpec:
        if (
            self.account.agent_type == "codex"
            and self.account.mode == "manager_distribute"
        ):
            raise ValueError(
                "Codex accounts do not support manager_distribute; use "
                "worker_local_login so auth.json is minted on the worker"
            )
        if (
            self.account.agent_type == "codex"
            and self.account.mode == "worker_local_login"
            and not self.account.config_dir
            and (
                self.account.per_worker > 1
                or self.rotation.strategy == "on_exhaust_restart_resume"
            )
        ):
            raise ValueError(
                "Codex multi-account/rotation jobs require an explicit absolute "
                "account.config_dir writable by the worker user"
            )
        if (
            self.account.agent_type == "codex"
            and self.account.mode == "worker_local_login"
        ):
            controlled_env = {"CODEX_HOME"}
            if not self.account.config_dir:
                controlled_env.add("HOME")
            overridden = controlled_env.intersection(self.run.env)
            if overridden:
                names = ", ".join(sorted(overridden))
                raise ValueError(
                    "Codex worker_local_login does not allow run.env to "
                    f"override managed credential paths: {names}"
                )
        return self

    @model_validator(mode="after")
    def validate_eip_account_binding(self) -> JobSpec:
        """Enforce the one-account/one-EIP/one-ephemeral-worker MVP."""
        if self.account.binding != "eip":
            return self
        if self.account.per_worker != 1:
            raise ValueError("account.per_worker must be 1 when account.binding is 'eip'")
        if self.account.mode != "worker_local_login":
            raise ValueError(
                "account.binding 'eip' currently requires "
                "account.mode='worker_local_login' so the selected identity "
                "is the identity logged in on the EIP worker"
            )
        if self.account.ids and len(self.account.ids) != self.fanout.workers:
            raise ValueError(
                "account.ids must contain exactly "
                f"{self.fanout.workers} unique account(s), one per fanout worker"
            )
        if self.rotation.strategy == "on_exhaust_restart_resume":
            raise ValueError(
                "account.binding 'eip' does not support "
                "on_exhaust_restart_resume; changing accounts requires a new worker"
            )
        credential_env = (
            "CODEX_HOME" if self.account.agent_type == "codex"
            else "CLAUDE_CONFIG_DIR"
        )
        unsafe_identity_env = {
            name for name in (credential_env, "HOME")
            if name in self.run.env
        }
        if unsafe_identity_env:
            raise ValueError(
                "account.binding 'eip' does not allow run.env to override "
                + ", ".join(sorted(unsafe_identity_env))
                + "; the run must use the same credential home verified at login"
            )
        return self

    # -- rendering helpers --------------------------------------------------

    def resolved_cwd(self) -> str:
        """Working directory for the run command.

        The contract: code clones to ``setup.target_dir`` (the repo root), and the
        run command runs from there — so you write it exactly as if you'd done
        ``git clone … && cd repo && <command>``. ``run.cwd`` refines it: default
        (``.``) == the repo root; a relative path is taken under the repo root; an
        absolute path is used as-is.
        """
        base = self.setup.target_dir
        cwd = (self.run.cwd or ".").strip()
        if cwd in (".", ""):
            return base
        if cwd.startswith("/"):
            return cwd
        return f"{base.rstrip('/')}/{cwd}"

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
        """Render run env and inject the selected CLI's credential directory."""
        env = {k: render_template(v, ctx.as_dict()) for k, v in self.run.env.items()}
        cfg = ctx.config_dir or self.account.config_dir
        if cfg:
            credential_env = (
                "CODEX_HOME" if self.account.agent_type == "codex"
                else "CLAUDE_CONFIG_DIR"
            )
            if (
                self.account.binding == "eip"
                or (
                    self.account.agent_type == "codex"
                    and self.account.mode == "worker_local_login"
                )
            ):
                # The selected account was authenticated and exact-email
                # verified in this directory.  Letting user env redirect the
                # run to another credential tree would break account→EIP
                # affinity after the safety check had already passed.
                env[credential_env] = cfg
            else:
                env.setdefault(credential_env, cfg)
        return env
