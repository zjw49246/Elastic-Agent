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
from typing import Any, Literal
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from elastic_agent.core.secret_env import parse_secret_reference

# These defaults are intentionally finite.  A missing/legacy ``0`` timeout is
# normalized to the safe default below, so a malformed or forgotten Job cannot
# consume a worker forever.  Operators can still opt into a longer run, up to
# the hard 30-day ceiling.
DEFAULT_RUN_TIMEOUT_SECONDS = 86_400
DEFAULT_JOB_TTL_SECONDS = 172_800
MAX_JOB_RUNTIME_SECONDS = 2_592_000
DEFAULT_ACCOUNT_LOGIN_TIMEOUT_SECONDS = 900
MAX_ACCOUNT_LOGIN_TIMEOUT_SECONDS = 1_200
_ENV_NAME_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_S3_URI_RE = re.compile(
    r"s3://(?P<bucket>[a-z0-9][a-z0-9.-]{1,61}[a-z0-9])(?:/(?P<key>.*))?"
)


def _validate_env_map(env: dict[str, str], *, label: str) -> dict[str, str]:
    invalid = [name for name in env if _ENV_NAME_RE.fullmatch(name) is None]
    if invalid:
        raise ValueError(
            f"{label} contains invalid variable name(s): "
            + ", ".join(sorted(invalid))
        )
    for name, value in env.items():
        if len(value) > 32_768 or "\x00" in value:
            raise ValueError(f"{label}[{name!r}] is too long or contains NUL")
    return env


def _validate_command(command: str, *, label: str) -> str:
    value = command.strip()
    if not value:
        raise ValueError(f"{label} cannot be empty")
    if "\x00" in value or any(
        ord(char) < 0x20 and char not in {"\n", "\t"} for char in value
    ):
        raise ValueError(f"{label} contains unsupported control characters")
    return value


def _validate_worker_path(
    raw: str,
    *,
    label: str,
    absolute: bool | None = None,
    allow_dot: bool = True,
) -> str:
    value = raw.strip()
    if not value:
        value = "." if allow_dot else ""
    if not value or len(value) > 4_096 or "\x00" in value or "\\" in value:
        raise ValueError(f"{label} is empty, too long, or contains unsafe characters")
    if any(ord(char) < 0x20 for char in value):
        raise ValueError(f"{label} contains control characters")
    path = PurePosixPath(value)
    if ".." in path.parts:
        raise ValueError(f"{label} cannot contain '..'")
    if absolute is True and not path.is_absolute():
        raise ValueError(f"{label} must be an absolute worker path")
    if absolute is False and path.is_absolute():
        raise ValueError(f"{label} must be a relative worker path")
    if not allow_dot and value == ".":
        raise ValueError(f"{label} cannot be '.'")
    return value.rstrip("/") if value != "/" else value


# Profile ids include their immutable schema revision.  Job authors select a
# profile; they do not mutate its common platform packages.  Job-specific work
# belongs in ``setup.steps`` and ``run.env`` instead.
ENVIRONMENT_PROFILES: dict[str, dict[str, Any]] = {
    "ubuntu-agent-v1": {
        "id": "ubuntu-agent-v1",
        "os_family": "ubuntu",
        "runtime": "elastic-agent",
        "agent_cli": "version-pinned",
        "browser_login": True,
        "docker": False,
        "system_packages": [
            "python3", "python3-pip", "git", "curl", "rsync", "nodejs", "npm",
        ],
    },
    "ubuntu-agent-docker-v1": {
        "id": "ubuntu-agent-docker-v1",
        "os_family": "ubuntu",
        "runtime": "elastic-agent",
        "agent_cli": "version-pinned",
        "browser_login": True,
        "docker": True,
        "system_packages": [
            "python3", "python3-pip", "git", "curl", "rsync", "nodejs", "npm",
        ],
    },
}


class StrictSpecModel(BaseModel):
    """Base for externally supplied JobSpec sections.

    Pydantic's default silently ignores unknown keys, which turns a typo such as
    ``timeuot`` into an unbounded/default run.  Declarative jobs are an API and
    must fail closed instead.
    """

    model_config = ConfigDict(extra="forbid")

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


class EnvironmentSpec(StrictSpecModel):
    """Immutable common worker environment selected by a Job.

    The revision is part of the profile id (``*-v1``), so changing a profile's
    meaning requires publishing a new id instead of mutating running/replayed
    Jobs in place.
    """

    profile: str = "ubuntu-agent-v1"

    @field_validator("profile")
    @classmethod
    def known_profile(cls, profile: str) -> str:
        value = profile.strip()
        if value not in ENVIRONMENT_PROFILES:
            supported = ", ".join(sorted(ENVIRONMENT_PROFILES))
            raise ValueError(
                f"unknown environment profile {value!r}; supported: {supported}"
            )
        return value

    def manifest(self) -> dict[str, Any]:
        """Return a copy safe to expose in a dry-run plan."""
        return dict(ENVIRONMENT_PROFILES[self.profile])


class S3Dataset(StrictSpecModel):
    """A dataset to stage onto each worker from S3 before the run.

    The worker downloads ``uri`` with its EC2 instance profile; static AWS
    credentials are never placed in a JobSpec. ``uri`` may be a single object
    or a prefix (trailing ``/`` → recursive sync)."""

    uri: str                      # s3://bucket/key or s3://bucket/prefix/
    dest: str                     # absolute path on the worker

    @field_validator("uri")
    @classmethod
    def valid_s3_uri(cls, uri: str) -> str:
        value = uri.strip()
        match = _S3_URI_RE.fullmatch(value)
        if (
            match is None
            or ".." in match.group("bucket")
            or any(ord(char) < 0x20 or char.isspace() for char in value)
        ):
            raise ValueError("S3 dataset uri must be a safe s3://bucket/key URI")
        return value

    @field_validator("dest")
    @classmethod
    def safe_destination(cls, dest: str) -> str:
        value = _validate_worker_path(
            dest, label="S3 dataset dest", absolute=True, allow_dot=False,
        )
        if value == "/":
            raise ValueError("S3 dataset dest cannot be the worker filesystem root")
        return value


class SetupStep(StrictSpecModel):
    """One Job-owned setup operation, always executed as the Job user."""

    name: str = Field(min_length=1, max_length=96)
    command: str = Field(min_length=1, max_length=65_536)
    env: dict[str, str] = Field(default_factory=dict)
    cwd: str = "."
    timeout: int = Field(default=900, ge=1, le=7_200)
    retries: int = Field(default=0, ge=0, le=5)
    # This is deliberately not an arbitrary username/root escape hatch.  The
    # provider-configured SSH/runtime user is resolved at provision time.
    run_as: Literal["job"] = "job"

    @field_validator("env")
    @classmethod
    def validate_env_names(cls, env: dict[str, str]) -> dict[str, str]:
        return _validate_env_map(env, label="setup step env")

    @field_validator("command")
    @classmethod
    def safe_command(cls, command: str) -> str:
        return _validate_command(command, label="setup step command")

    @field_validator("cwd")
    @classmethod
    def safe_cwd(cls, cwd: str) -> str:
        try:
            return _validate_worker_path(cwd, label="setup step cwd")
        except ValueError as exc:
            if "'..'" in str(exc):
                raise ValueError(
                    "setup step cwd cannot escape setup.target_dir"
                ) from exc
            raise


class SetupSpec(StrictSpecModel):
    """How to provision each worker before the run command (bootstrap-time)."""

    repo: str | None = None
    # ``branch`` remains for old payloads. New Jobs should use ``ref`` (branch
    # or tag) and may pin the expected immutable commit in ``resolved_commit``.
    branch: str = "main"
    ref: str = ""
    resolved_commit: str = ""
    target_dir: str = "/opt/elastic-agent/harness"
    commands: list[str] = Field(default_factory=list)
    steps: list[SetupStep] = Field(default_factory=list)
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

    @field_validator("repo")
    @classmethod
    def normalize_repo(cls, repo: str | None) -> str | None:
        value = (repo or "").strip()
        if value and (
            any(ord(ch) < 32 or ch.isspace() for ch in value)
            or any(ch in value for ch in "'\"`$;&|<>(){}[]*?!\\")
        ):
            raise ValueError("setup.repo contains unsafe shell characters")
        if not value:
            return None
        parsed = urlsplit(value)
        if parsed.scheme:
            if parsed.scheme.lower() not in {"http", "https", "ssh", "git"}:
                raise ValueError("setup.repo must use http(s), ssh, or git")
            if not parsed.hostname or parsed.query or parsed.fragment:
                raise ValueError(
                    "setup.repo must not contain query/fragment credentials"
                )
            if parsed.password is not None or (
                parsed.scheme.lower() in {"http", "https", "git"}
                and parsed.username is not None
            ):
                raise ValueError(
                    "setup.repo must not embed URL credentials; configure the "
                    "Manager git credential instead"
                )
        elif re.fullmatch(
            r"[A-Za-z0-9._-]+@[A-Za-z0-9.-]+:[A-Za-z0-9._/-]+",
            value,
        ) is None:
            # Reject Manager-local paths and ambiguous shell/git transports: an
            # API-submitted Job must not turn manager_rsync into a local-file
            # exfiltration primitive.
            raise ValueError("setup.repo must be an absolute remote Git URL")
        return value

    @field_validator("branch", "ref")
    @classmethod
    def safe_git_ref(cls, ref: str) -> str:
        value = ref.strip()
        if not value:
            return value
        if (
            value.startswith("-")
            or re.fullmatch(r"[A-Za-z0-9._/-]+", value) is None
            or ".." in value
            or value.endswith((".", "/"))
        ):
            raise ValueError("git branch/ref contains unsafe or invalid characters")
        return value

    @field_validator("resolved_commit")
    @classmethod
    def exact_git_commit(cls, commit: str) -> str:
        value = commit.strip().lower()
        if value and re.fullmatch(r"[0-9a-f]{40,64}", value) is None:
            raise ValueError("setup.resolved_commit must be a full 40-64 hex commit id")
        return value

    @field_validator("target_dir")
    @classmethod
    def absolute_target_dir(cls, target_dir: str) -> str:
        value = target_dir.strip()
        if not value or not PurePosixPath(value).is_absolute():
            raise ValueError("setup.target_dir must be an absolute worker path")
        if re.fullmatch(r"/[A-Za-z0-9._/-]+", value) is None or ".." in PurePosixPath(value).parts:
            raise ValueError("setup.target_dir contains unsafe path characters")
        value = value.rstrip("/") or "/"
        if len(PurePosixPath(value).parts) < 3:
            raise ValueError(
                "setup.target_dir must be a dedicated directory, not a worker "
                "filesystem root/top-level directory"
            )
        return value

    @field_validator("commands")
    @classmethod
    def nonempty_legacy_commands(cls, commands: list[str]) -> list[str]:
        normalized = [command.strip() for command in commands if command.strip()]
        return normalized

    @model_validator(mode="after")
    def source_fields_require_repo(self) -> SetupSpec:
        if not self.repo and (self.ref or self.resolved_commit):
            raise ValueError("setup.ref/resolved_commit require setup.repo")
        return self

    @property
    def checkout_ref(self) -> str:
        """Resolved branch/tag used by both worker-clone and Manager-rsync."""
        return self.ref or self.branch or "main"

    def normalized_steps(self) -> list[SetupStep]:
        """Return legacy commands plus structured steps in execution order.

        Legacy commands stay in one shell so exports and ``cd`` state retain
        their historical behaviour. Structured steps are isolated, retryable
        units with their own environment, cwd and timeout.
        """
        out: list[SetupStep] = []
        if self.commands:
            out.append(SetupStep(
                name="legacy-commands",
                command=" && ".join(self.commands),
                timeout=1_200,
            ))
        out.extend(self.steps)
        return out


class RunSpec(StrictSpecModel):
    """The long-running command that consumes the selected agent account."""

    command: str = Field(min_length=1, max_length=65_536)
    env: dict[str, str] = Field(default_factory=dict)
    # Values are references only. Plaintext is resolved immediately before
    # dispatch and never written back to this model/persistence journal.
    secret_env: dict[str, str] = Field(default_factory=dict)
    cwd: str = "."
    # Missing/None/0 legacy values are normalized to a finite 24-hour default.
    timeout: int = Field(
        default=DEFAULT_RUN_TIMEOUT_SECONDS,
        ge=60,
        le=MAX_JOB_RUNTIME_SECONDS,
    )
    # Mode-B commands are user-authored shell one-liners ($(hostname -s), &&,
    # env expansion). When True the rendered command is wrapped as
    # ``bash -lc "<cmd>"``; when False it is shlex-split into a bare argv.
    shell: bool = True

    @field_validator("command")
    @classmethod
    def safe_command(cls, command: str) -> str:
        return _validate_command(command, label="run.command")

    @field_validator("cwd")
    @classmethod
    def safe_cwd(cls, cwd: str) -> str:
        # Absolute paths remain supported; relative paths are anchored under
        # setup.target_dir later by JobSpec.resolved_cwd().
        return _validate_worker_path(cwd, label="run.cwd")

    @field_validator("env")
    @classmethod
    def safe_env(cls, env: dict[str, str]) -> dict[str, str]:
        return _validate_env_map(env, label="run.env")

    @field_validator("secret_env")
    @classmethod
    def safe_secret_env(cls, secret_env: dict[str, str]) -> dict[str, str]:
        _validate_env_map(secret_env, label="run.secret_env")
        for reference in secret_env.values():
            parse_secret_reference(reference)
        return secret_env

    @model_validator(mode="after")
    def env_names_do_not_overlap(self) -> RunSpec:
        overlap = set(self.env).intersection(self.secret_env)
        if overlap:
            raise ValueError(
                "run.env and run.secret_env cannot define the same key(s): "
                + ", ".join(sorted(overlap))
            )
        return self

    @field_validator("timeout", mode="before")
    @classmethod
    def finite_timeout(cls, timeout: Any) -> int:
        if timeout in (None, 0, "", "0"):
            return DEFAULT_RUN_TIMEOUT_SECONDS
        return timeout


class AccountSpec(StrictSpecModel):
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
    # Browser automation only. Manager coordination has a larger fixed budget
    # so OTP waits, identity validation, smoke testing, and cleanup cannot race
    # this deadline.
    login_timeout_seconds: int = Field(
        default=DEFAULT_ACCOUNT_LOGIN_TIMEOUT_SECONDS,
        ge=60,
        le=MAX_ACCOUNT_LOGIN_TIMEOUT_SECONDS,
    )
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


class RotationSpec(StrictSpecModel):
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


class FanoutSpec(StrictSpecModel):
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


class CollectSpec(StrictSpecModel):
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
            try:
                value = _validate_worker_path(
                    raw,
                    label="collect.paths entries",
                    absolute=False,
                    allow_dot=False,
                )
            except ValueError as exc:
                raise ValueError(
                    "collect.paths entries must be safe, non-empty relative "
                    "paths without '..'"
                ) from exc
            normalized.append(value)
        return normalized


class CompletionSpec(StrictSpecModel):
    """How to decide a worker's run is done."""

    on_process_exit: int = 0


class JobSpec(StrictSpecModel):
    """A complete declarative description of a fan-out job."""

    name: str
    environment: EnvironmentSpec = Field(default_factory=EnvironmentSpec)
    setup: SetupSpec = Field(default_factory=SetupSpec)
    run: RunSpec
    account: AccountSpec = Field(default_factory=AccountSpec)
    rotation: RotationSpec = Field(default_factory=RotationSpec)
    fanout: FanoutSpec = Field(default_factory=FanoutSpec)
    collect: CollectSpec = Field(default_factory=CollectSpec)
    completion: CompletionSpec = Field(default_factory=CompletionSpec)
    # Overall control-plane lease, including provisioning/login/collection.
    # Runtime enforcement is orchestrator-owned; keeping it in the durable spec
    # gives recovery/watchdog code one stable deadline policy.
    ttl_seconds: int = Field(
        default=DEFAULT_JOB_TTL_SECONDS,
        ge=300,
        le=MAX_JOB_RUNTIME_SECONDS,
    )
    # Escape hatch: "module.path:ClassName" of an uploaded Harness subclass.
    # When set, GenericJobHarness is bypassed and the referenced Harness drives
    # bootstrap/login/execute — the declarative fields become defaults/metadata.
    harness_ref: str | None = None

    @field_validator("ttl_seconds", mode="before")
    @classmethod
    def finite_ttl(cls, ttl: Any) -> int:
        if ttl in (None, 0, "", "0"):
            return DEFAULT_JOB_TTL_SECONDS
        return ttl

    @model_validator(mode="after")
    def ttl_covers_run(self) -> JobSpec:
        if self.ttl_seconds < self.run.timeout:
            raise ValueError(
                "ttl_seconds must be greater than or equal to run.timeout"
            )
        return self

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
            overridden = controlled_env.intersection(
                {*self.run.env, *self.run.secret_env}
            )
            if overridden:
                names = ", ".join(sorted(overridden))
                raise ValueError(
                    "Codex worker_local_login does not allow run.env/run.secret_env to "
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
            if name in self.run.env or name in self.run.secret_env
        }
        if unsafe_identity_env:
            raise ValueError(
                "account.binding 'eip' does not allow run.env/run.secret_env to override "
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
