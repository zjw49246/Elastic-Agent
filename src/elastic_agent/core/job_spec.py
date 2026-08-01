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
MAX_ACCOUNT_EXCLUDE_IDS = 100
_ENV_NAME_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_ACCOUNT_REFERENCE_RE = re.compile(
    r"[A-Za-z0-9][A-Za-z0-9._:@+-]{0,127}"
)
_UNSTABLE_HOSTNAME_RE = re.compile(
    r"\{\{\s*hostname\s*\}\}"
    r"|\$\(\s*hostname\b[^)]*\)"
    r"|\$\{\s*HOSTNAME\s*\}"
    r"|\$HOSTNAME\b",
    re.IGNORECASE,
)
_ROUTING_PROXY_ENV_KEYS = frozenset({
    "http_proxy",
    "https_proxy",
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "all_proxy",
})
_S3_URI_RE = re.compile(
    r"s3://(?P<bucket>[a-z0-9][a-z0-9.-]{1,61}[a-z0-9])(?:/(?P<key>.*))?"
)
_SAFE_JOB_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")
_SAFE_CHECKPOINT_GENERATION_RE = re.compile(
    r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}"
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
    "ubuntu-agent-docker-v2": {
        "id": "ubuntu-agent-docker-v2",
        "os_family": "ubuntu",
        "runtime": "elastic-agent",
        "agent_cli": "version-pinned",
        "browser_login": True,
        "docker": True,
        "system_packages": [
            "python3", "python3-pip", "python3-venv", "git", "curl",
            "rsync", "nodejs", "npm",
        ],
    },
    "ubuntu-agent-docker-sandbox-v1": {
        "id": "ubuntu-agent-docker-sandbox-v1",
        "os_family": "ubuntu",
        "runtime": "elastic-agent",
        "agent_cli": "version-pinned",
        "browser_login": True,
        "docker": True,
        # Trusted scorer processes use an unprivileged user/network namespace
        # around Bubblewrap. Keep this separate: adding packages to the older
        # Docker profile would silently change replayed Jobs.
        "system_packages": [
            "python3", "python3-pip", "git", "curl", "rsync", "nodejs", "npm",
            "python3-venv", "bubblewrap", "util-linux",
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
        values = asdict(self)
        values["shard_id"] = f"{self.shard_index:05d}"
        return values


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
    or a prefix (trailing ``/`` → recursive sync). Both fields support
    per-worker template variables such as ``{{shard_id}}``."""

    uri: str                      # s3://bucket/key or s3://bucket/prefix/
    dest: str                     # absolute path on the worker

    @field_validator("uri")
    @classmethod
    def valid_s3_uri(cls, uri: str) -> str:
        value = uri.strip()
        # The generic template grammar permits readability whitespace inside
        # braces. Canonicalize only recognized placeholders before applying the
        # URI's strict no-whitespace rule; ordinary URI whitespace remains
        # rejected and unknown variables still fail in JobSpec rendering.
        value = _TEMPLATE_RE.sub(
            lambda match: "{{" + match.group(1) + "}}",
            value,
        )
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
    # Datasets staged from S3 onto each worker before the run using the worker's
    # instance profile.
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
    # Optional application-level command used only after a complete immutable
    # checkpoint has been restored onto a replacement Worker.  Keeping this
    # separate from rotation.resume_args prevents the control plane from
    # guessing how to replay an opaque Mode-B command and lets one-click
    # suspend/resume remain server-side and auditable.
    resume_command: str = Field(default="", max_length=65_536)
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
    # Secret-bearing stdin is not part of JobSpec persistence.  This marker is
    # accepted only by a dedicated server-side constructor which installs a
    # process-local, one-shot lease before the Job can reach dispatch.
    stdin_protocol: Literal["none", "run_benchmark_v1"] = "none"

    @field_validator("command")
    @classmethod
    def safe_command(cls, command: str) -> str:
        return _validate_command(command, label="run.command")

    @field_validator("resume_command")
    @classmethod
    def safe_resume_command(cls, command: str) -> str:
        if not command.strip():
            return ""
        return _validate_command(command, label="run.resume_command")

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
    # Independent credential-source admission. ``any`` preserves the existing
    # API-first/fallback-to-OAuth behavior; constrained modes must never select
    # an identity from the other credential source.
    auth_kind: Literal["any", "oauth", "agent_api"] = "any"
    # Optional exact model admission for Agent API accounts. OAuth identities
    # have no locally discoverable catalog, so this is an API-provider routing
    # constraint rather than a CLI model override.
    model: str = ""

    # worker_local_login: worker runs the login flow locally (P3 ACCOUNT_LOGIN),
    #   after the Manager sends the selected email + write-only mailbox token;
    #   generated Claude OAuth credentials stay on the worker and are not sent
    #   back. Remote worker transport is required to use WSS.
    # none: caller has already provisioned credentials; Elastic does nothing.
    mode: Literal["worker_local_login", "none"] = "worker_local_login"
    per_worker: int = Field(default=1, ge=1, le=32)
    group: str = "standard"
    # eip: each account keeps a durable EIP identity while the EC2 instance is
    # ephemeral. ``ids`` optionally pins one explicit account to each fan-out
    # worker; an empty list lets the allocator choose from ``group``.
    binding: Literal["none", "eip"] = "none"
    ids: list[str] = Field(default_factory=list)
    # Automatic recovery appends the non-secret account IDs used by each
    # completed source attempt here. Selection must never hand an already
    # exhausted identity back to a later attempt. Keep the history bounded so
    # a long lineage cannot grow its private JobSpec without limit.
    exclude_ids: list[str] = Field(
        default_factory=list,
        max_length=MAX_ACCOUNT_EXCLUDE_IDS,
    )
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

    @field_validator("mode", mode="before")
    @classmethod
    def reject_unimplemented_manager_distribution(cls, mode: Any) -> Any:
        if mode == "manager_distribute":
            raise ValueError(
                "account.mode='manager_distribute' is not implemented; use "
                "worker_local_login so credentials are minted on the worker"
            )
        return mode

    @field_validator("model")
    @classmethod
    def normalize_model(cls, model: str) -> str:
        value = model.strip()
        if (
            len(value) > 200
            or any(
                ord(character) < 32 or ord(character) == 127
                for character in value
            )
        ):
            raise ValueError(
                "account.model must be empty or at most 200 printable characters"
            )
        return value

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
        """Trim IDs while preserving their worker×slot positions.

        Repeated IDs are resolved against the account stores during preflight:
        only unbound Agent API identities may repeat. Schema-level
        de-duplication would erase that intentional sharing topology before the
        API can prove the selected identity kind.
        """
        normalized: list[str] = []
        for raw_id in ids:
            account_id = raw_id.strip()
            if account_id:
                normalized.append(account_id)
        return normalized

    @field_validator("exclude_ids")
    @classmethod
    def normalize_exclude_ids(cls, ids: list[str]) -> list[str]:
        """Normalize a bounded set while retaining stable serialized order."""

        normalized: list[str] = []
        seen: set[str] = set()
        for raw_id in ids:
            account_id = raw_id.strip()
            if not account_id:
                continue
            if _ACCOUNT_REFERENCE_RE.fullmatch(account_id) is None:
                raise ValueError(
                    "account.exclude_ids contains an invalid account reference"
                )
            if account_id not in seen:
                normalized.append(account_id)
                seen.add(account_id)
        return normalized

    @model_validator(mode="after")
    def selected_and_excluded_ids_do_not_overlap(self) -> AccountSpec:
        overlap = set(self.ids).intersection(self.exclude_ids)
        if overlap:
            raise ValueError(
                "account.ids and account.exclude_ids cannot overlap: "
                + ", ".join(sorted(overlap))
            )
        return self


class RotationSpec(StrictSpecModel):
    """Coarse-grained (Mode-B) account rotation policy.

    ``on_exhaust_restart_resume`` (strategy "a"): when the run command's stdout
    proves account auth/quota exhaustion, interrupt it, advance to another
    pre-logged credential slot or log a fresh account into a sibling config
    directory, and restart the command with ``resume_args`` appended so the
    harness skips already-completed work. Provider-transient 500/502 failures do
    not replay an arbitrary outer command.
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

    paths: list[str] = Field(default_factory=list, max_length=32)
    # Build an immutable, hash-verified S3 generation after each successful
    # Manager-side collection. Checkpoint mode deliberately uses the Manager
    # relay even when direct worker uploads are available: the latter overwrite
    # mutable keys and cannot prove that a live file stayed unchanged.
    checkpoint: bool = False
    # Keep a small number of complete Job-level recovery sets. Content-addressed
    # blobs shared by those sets remain reachable; older manifests and
    # unreferenced blobs are garbage-collected only after a newer complete set
    # has been published for every shard.
    checkpoint_keep_generations: int = Field(default=3, ge=1, le=100)
    # Relative glob patterns omitted from both ordinary collection and
    # checkpoint generations. This keeps crash dumps/caches out of durable
    # results without exposing an arbitrary rsync/CLI option surface.
    exclude: list[str] = Field(default_factory=list, max_length=64)
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

    @field_validator("exclude")
    @classmethod
    def safe_exclude_patterns(cls, patterns: list[str]) -> list[str]:
        normalized: list[str] = []
        for raw in patterns:
            value = raw.strip()
            if (
                not value
                or len(value) > 1_024
                or value.startswith(("-", "/"))
                or "\x00" in value
                or "\\" in value
                or any(ord(character) < 0x20 for character in value)
                or ".." in PurePosixPath(value).parts
            ):
                raise ValueError(
                    "collect.exclude entries must be safe relative glob patterns"
                )
            normalized.append(value)
        return normalized

    @model_validator(mode="after")
    def checkpoint_paths_are_disjoint(self) -> CollectSpec:
        ordered = sorted(
            self.paths,
            key=lambda value: PurePosixPath(value).parts,
        )
        if len(set(ordered)) != len(ordered):
            raise ValueError("checkpoint collect.paths must be unique")
        for parent, child in zip(ordered, ordered[1:]):
            if child.startswith(parent.rstrip("/") + "/"):
                raise ValueError(
                    "checkpoint collect.paths must not overlap"
                )
        return self


class RecoverySpec(StrictSpecModel):
    """Restore one trusted prior Job shard before dispatching this Job.

    ``checkpoint`` accepts only immutable COMMITTED generations. The legacy
    literal remains parseable solely so old private Job journals can be read;
    current preflight rejects it because a mutable S3 tree cannot prove file
    deletions or one complete generation.
    """

    policy: Literal[
        "none", "checkpoint", "legacy_final_collection",
    ] = "none"
    source_job_id: str = ""
    paths: list[str] = Field(default_factory=list, max_length=32)
    generation: str = ""

    @field_validator("source_job_id")
    @classmethod
    def safe_source_job_id(cls, source_job_id: str) -> str:
        value = source_job_id.strip()
        if value and _SAFE_JOB_ID_RE.fullmatch(value) is None:
            raise ValueError("recovery.source_job_id is invalid")
        return value

    @field_validator("paths")
    @classmethod
    def safe_recovery_paths(cls, paths: list[str]) -> list[str]:
        normalized: list[str] = []
        for raw in paths:
            try:
                value = _validate_worker_path(
                    raw,
                    label="recovery.paths entries",
                    absolute=False,
                    allow_dot=False,
                )
            except ValueError as exc:
                raise ValueError(
                    "recovery.paths entries must be safe relative paths"
                ) from exc
            normalized.append(value)
        return normalized

    @field_validator("generation")
    @classmethod
    def safe_generation(cls, generation: str) -> str:
        value = generation.strip()
        if (
            value
            and _SAFE_CHECKPOINT_GENERATION_RE.fullmatch(value) is None
        ):
            raise ValueError("recovery.generation is invalid")
        return value

    @model_validator(mode="after")
    def complete_configuration(self) -> RecoverySpec:
        if self.policy == "none":
            if self.source_job_id or self.paths or self.generation:
                raise ValueError(
                    "recovery source_job_id/paths/generation must be empty "
                    "when recovery.policy is 'none'"
                )
            return self
        if not self.source_job_id:
            raise ValueError(
                "recovery.source_job_id is required when recovery is enabled"
            )
        if not self.paths:
            raise ValueError(
                "recovery.paths is required when recovery is enabled"
            )
        if self.policy == "legacy_final_collection" and self.generation:
            raise ValueError(
                "recovery.generation is supported only for checkpoint recovery"
            )
        ordered = sorted(
            self.paths,
            key=lambda value: PurePosixPath(value).parts,
        )
        if len(set(ordered)) != len(ordered):
            raise ValueError("recovery.paths must be unique")
        for parent, child in zip(ordered, ordered[1:]):
            if child.startswith(parent.rstrip("/") + "/"):
                raise ValueError("recovery.paths must not overlap")
        return self


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
    recovery: RecoverySpec = Field(default_factory=RecoverySpec)
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
    def checkpoint_collection_has_paths(self) -> JobSpec:
        if self.collect.checkpoint and not self.collect.paths:
            raise ValueError(
                "checkpoint collection requires collect.paths"
            )
        if (
            self.collect.checkpoint
            and self.fanout.shard_by != "shard_index"
        ):
            raise ValueError(
                "checkpoint collection requires "
                "fanout.shard_by='shard_index' so a replacement worker "
                "receives the same logical shard"
            )
        if self.collect.checkpoint:
            recovery_sensitive_values = [
                self.run.command,
                self.run.resume_command,
                self.run.cwd,
                self.rotation.resume_args,
                *self.run.env.values(),
                *self.setup.commands,
                self.setup.target_dir,
                *(
                    value
                    for step in self.setup.steps
                    for value in (
                        step.command,
                        step.cwd,
                        *step.env.values(),
                    )
                ),
                *(
                    value
                    for dataset in self.setup.s3_datasets
                    for value in (dataset.uri, dataset.dest)
                ),
            ]
            if any(
                _UNSTABLE_HOSTNAME_RE.search(str(value))
                for value in recovery_sensitive_values
                if value
            ):
                raise ValueError(
                    "checkpoint Jobs cannot use hostname-derived workload "
                    "paths or inputs because replacement Workers have different "
                    "hostnames; use {{shard_id}} or {{shard_index}}"
                )
            incomplete_window = max(
                2,
                min(8, self.collect.checkpoint_keep_generations),
            )
            # The checkpoint store intentionally bounds a Job's shard-manifest
            # inventory at 10,000. Leave room for one in-progress generation
            # and the incomplete-generation recovery window so retention can
            # always run before the listing limit is reached.
            manifest_budget = self.fanout.workers * (
                self.collect.checkpoint_keep_generations
                + incomplete_window
                + 1
            )
            if manifest_budget > 10_000:
                raise ValueError(
                    "fanout.workers and "
                    "collect.checkpoint_keep_generations require more than "
                    "the 10000-manifest checkpoint retention budget"
                )
        return self

    @model_validator(mode="after")
    def dataset_templates_are_renderable(self) -> JobSpec:
        try:
            context = self.worker_contexts()[0]
            # Submission-time validation must not reject legitimate
            # ``{{hostname}}`` datasets merely because the real worker does not
            # exist yet. Runtime rendering still rejects a missing/empty
            # hostname and therefore cannot turn one object into a whole-prefix
            # sync.
            context.hostname = "validation-host"
            self.render_s3_datasets(context)
        except (KeyError, ValueError) as exc:
            raise ValueError(f"invalid S3 dataset template: {exc}") from exc
        return self

    @model_validator(mode="after")
    def validate_agent_account_mode(self) -> JobSpec:
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
            if self.account.ids:
                required = self.fanout.workers * self.account.per_worker
                if len(self.account.ids) != required:
                    raise ValueError(
                        "account.ids must contain exactly "
                        f"{required} account reference(s), one per worker slot"
                    )
            return self
        if self.account.per_worker != 1:
            raise ValueError("account.per_worker must be 1 when account.binding is 'eip'")
        if self.account.mode != "worker_local_login":
            raise ValueError(
                "account.binding 'eip' currently requires "
                "account.mode='worker_local_login' so the selected identity "
                "is the identity logged in on the EIP worker"
            )
        if self.account.ids and (
            len(self.account.ids) != self.fanout.workers
            or len(set(self.account.ids)) != self.fanout.workers
        ):
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
        routing_proxy_env = _ROUTING_PROXY_ENV_KEYS.intersection(
            {*self.run.env, *self.run.secret_env}
        )
        if routing_proxy_env:
            raise ValueError(
                "account.binding 'eip' requires direct EIP egress and does not "
                "allow run.env/run.secret_env proxy variables: "
                + ", ".join(sorted(routing_proxy_env))
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

    def render_s3_datasets(self, ctx: WorkerContext) -> list[S3Dataset]:
        """Render and revalidate per-worker S3 object locations."""
        values = ctx.as_dict()
        rendered: list[S3Dataset] = []
        for dataset in self.setup.s3_datasets:
            for field_name, template in (
                ("uri", dataset.uri),
                ("dest", dataset.dest),
            ):
                for match in _TEMPLATE_RE.finditer(template):
                    variable = match.group(1)
                    if variable not in values:
                        raise KeyError(
                            f"unknown template variable: "
                            f"{{{{{variable}}}}}"
                        )
                    if (
                        values[variable] is None
                        or not str(values[variable]).strip()
                    ):
                        raise ValueError(
                            f"S3 dataset {field_name} template variable "
                            f"{variable!r} resolved empty"
                        )
            rendered_uri = render_template(dataset.uri, values)
            rendered_dest = render_template(dataset.dest, values)
            if dataset.uri.endswith("/") != rendered_uri.endswith("/"):
                raise ValueError(
                    "S3 dataset uri rendering cannot change object/prefix mode"
                )
            rendered.append(S3Dataset(
                uri=rendered_uri,
                dest=rendered_dest,
            ))
        return rendered

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

    def render_recovery_command(self, ctx: WorkerContext) -> list[str]:
        """Render the explicit application-level checkpoint resume command."""

        if not self.run.resume_command:
            raise ValueError(
                "run.resume_command is required for one-click checkpoint resume"
            )
        rendered = render_template(
            self.run.resume_command,
            ctx.as_dict(),
        )
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
                or self.account.mode == "worker_local_login"
            ):
                # The selected account was authenticated and exact-email
                # verified in this directory.  Letting user env redirect the
                # run to another credential tree would break account→EIP
                # affinity after the safety check had already passed.
                env[credential_env] = cfg
            else:
                env.setdefault(credential_env, cfg)
        return env
