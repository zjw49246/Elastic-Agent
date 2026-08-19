"""Secure worker-local projections for third-party Agent API credentials.

An Agent API key is never placed in a CLI configuration file, environment
variable, command line, or projection marker.  Instead, each projection owns a
private key file and an executable credential helper.  The generated Claude
and Codex homes contain only the selected provider's fixed endpoint plus the
helper path. The admission probe determines which agent types a provider
account may use.
"""

from __future__ import annotations

import json
import os
import re
import shlex
import stat
import tempfile
import tomllib
from collections.abc import Iterable, Mapping, MutableMapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

CLOUDROUTER_CLAUDE_BASE_URL = "https://console.cloudrouter.online"
CLOUDROUTER_CODEX_BASE_URL = "https://console.cloudrouter.online/v1"
APEX_CLAUDE_BASE_URL = "https://api.apexin.ai"
APEX_CODEX_BASE_URL = "https://api.apexin.ai/v1"
CLOUDROUTER_CLAUDE_AUTH_ENV_KEYS = frozenset(
    {
        "ANTHROPIC_AUTH_TOKEN",
        "ANTHROPIC_API_KEY",
        "CLAUDE_CODE_OAUTH_TOKEN",
    }
)
CLOUDROUTER_CLAUDE_PROVIDER_ENV_KEYS = frozenset(
    {
        # Any of these selectors takes precedence over direct Anthropic API
        # authentication and can silently route a managed CloudRouter turn to
        # a cloud-provider backend using credentials inherited by the worker.
        "CLAUDE_CODE_USE_ANTHROPIC_AWS",
        "CLAUDE_CODE_USE_BEDROCK",
        "CLAUDE_CODE_USE_FOUNDRY",
        "CLAUDE_CODE_USE_MANTLE",
        "CLAUDE_CODE_USE_VERTEX",
        # Provider-specific auth bypasses and endpoint/credential overrides are
        # inactive after their selector is removed, but scrub them as well so a
        # login shell or future CLI precedence change cannot revive the route.
        "CLAUDE_CODE_SKIP_ANTHROPIC_AWS_AUTH",
        "CLAUDE_CODE_SKIP_BEDROCK_AUTH",
        "CLAUDE_CODE_SKIP_FOUNDRY_AUTH",
        "CLAUDE_CODE_SKIP_MANTLE_AUTH",
        "CLAUDE_CODE_SKIP_VERTEX_AUTH",
        "ANTHROPIC_AWS_API_KEY",
        "ANTHROPIC_AWS_AUTH",
        "ANTHROPIC_AWS_BASE_URL",
        "ANTHROPIC_AWS_WORKSPACE_ID",
        "ANTHROPIC_BEDROCK_BASE_URL",
        "ANTHROPIC_BEDROCK_MANTLE_API_KEY",
        "ANTHROPIC_BEDROCK_MANTLE_BASE_URL",
        "ANTHROPIC_FOUNDRY_API_KEY",
        "ANTHROPIC_FOUNDRY_AUTH_TOKEN",
        "ANTHROPIC_FOUNDRY_BASE_URL",
        "ANTHROPIC_FOUNDRY_RESOURCE",
        "ANTHROPIC_VERTEX_BASE_URL",
        "ANTHROPIC_VERTEX_PROJECT_ID",
        "AWS_BEARER_TOKEN_BEDROCK",
    }
)
AGENT_API_CODEX_AUTH_ENV_KEYS = frozenset(
    {
        "OPENAI_API_KEY",
        "CODEX_API_KEY",
        "CLOUDROUTER_API_KEY",
        "APEX_CODEX_GATEWAY_KEY",
        "APEX_CODEX_API_KEY",
        "APEXROUTER_API_KEY",
        "APEXROUTER_CODEX_API_KEY",
    }
)
# Compatibility name retained for callers that imported the CloudRouter-only
# set before multiple Codex Agent API providers were supported.
CLOUDROUTER_CODEX_AUTH_ENV_KEYS = AGENT_API_CODEX_AUTH_ENV_KEYS
CLOUDROUTER_CLAUDE_BINARY_ENV = "ELASTIC_AGENT_CLOUDROUTER_CLAUDE_BINARY"
# Non-secret hand-off understood by container owners such as AI4Sci.  The
# consumer still has to validate projection.json before bind-mounting this
# directory; arbitrary Docker invocation is intentionally not intercepted.
ELASTIC_AGENT_API_PROJECTION_ROOT_ENV = "ELASTIC_AGENT_API_PROJECTION_ROOT"

@dataclass(frozen=True)
class _ProviderSpec:
    agent_types: frozenset[str]
    claude_base_url: str | None
    codex_base_url: str
    codex_provider_id: str
    codex_provider_name: str


_PROVIDER_SPECS = {
    "cloudrouter": _ProviderSpec(
        agent_types=frozenset({"claude", "codex"}),
        claude_base_url=CLOUDROUTER_CLAUDE_BASE_URL,
        codex_base_url=CLOUDROUTER_CODEX_BASE_URL,
        codex_provider_id="cloudrouter",
        codex_provider_name="CloudRouter",
    ),
    "apex": _ProviderSpec(
        agent_types=frozenset({"claude", "codex"}),
        claude_base_url="https://api.apexin.ai",
        codex_base_url=APEX_CODEX_BASE_URL,
        codex_provider_id="apexrouter",
        codex_provider_name="ApexRouter",
    ),
}
_SUPPORTED_PROVIDERS = frozenset(_PROVIDER_SPECS)
_SUPPORTED_AGENT_TYPES = frozenset({"claude", "codex"})
_ACCOUNT_ID_RE = re.compile(
    r"^(?P<provider>cloudrouter|apex)-"
    r"(?P<suffix>[A-Za-z0-9][A-Za-z0-9._-]*)$"
)
_MAX_KEY_BYTES = 16 * 1024
_MAX_MANAGED_FILE_BYTES = 256 * 1024
_PROJECTION_VERSION = 2
_MARKER_NAME = "projection.json"
_MANAGED_DIRECTORY = ".elastic-agent-api"
_MARKER_KEYS = frozenset(
    {
        "version",
        "managed_by",
        "provider",
        "agent_type",
        "account_id",
        "root",
        "home",
        "models",
        "endpoints",
    }
)

_KEY_HELPER = r"""#!/usr/bin/env python3
import os
import stat
import sys
from pathlib import Path

path = Path(__file__).with_name("api.key")
flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
try:
    descriptor = os.open(path, flags)
    metadata = os.fstat(descriptor)
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != os.getuid()
        or stat.S_IMODE(metadata.st_mode) != 0o600
    ):
        raise RuntimeError("API key file must be a private regular file")
    payload = os.read(descriptor, 16385)
finally:
    if "descriptor" in locals():
        os.close(descriptor)
if not payload or len(payload) > 16384 or b"\n" in payload or b"\r" in payload:
    raise RuntimeError("Invalid API key file")
sys.stdout.write(payload.decode("utf-8"))
"""

_KEY_HELPER_LAUNCHER = """#!/bin/sh
exec /usr/bin/env -i PATH=/usr/local/bin:/usr/bin:/bin "${0%/*}/key-helper"
"""

_CLAUDE_WRAPPER_UNSET_ENV_KEYS = tuple(sorted(CLOUDROUTER_CLAUDE_AUTH_ENV_KEYS | CLOUDROUTER_CLAUDE_PROVIDER_ENV_KEYS))

def _claude_wrapper(base_url: str) -> str:
    return f"""#!/bin/sh
unset {" ".join(_CLAUDE_WRAPPER_UNSET_ENV_KEYS)}
export ANTHROPIC_BASE_URL={base_url}
umask 077
claude_binary=${{{CLOUDROUTER_CLAUDE_BINARY_ENV}:-}}
case "$claude_binary" in
  /*) ;;
  *) echo "Managed Claude binary is not pinned" >&2; exit 126 ;;
esac
for argument do
  case "$argument" in
    --settings|--settings=*|--setting-sources|--setting-sources=*)
      echo "CloudRouter Claude settings override is not allowed" >&2
      exit 64
      ;;
  esac
done
# The managed user settings contain the only permitted apiKeyHelper and route.
# Deliberately omit project/local sources: their env, hooks, and MCP definitions
# would otherwise execute before or alongside the managed provider request.
exec "$claude_binary" --setting-sources user "$@"
"""


_CLAUDE_WRAPPER = _claude_wrapper(CLOUDROUTER_CLAUDE_BASE_URL)


class AgentAPIError(RuntimeError):
    """Base class for worker Agent API projection failures."""


class AgentAPIConfigurationError(AgentAPIError, ValueError):
    """The caller requested an unsupported or unsafe projection."""


class UnsafeAgentAPIPathError(AgentAPIError):
    """A managed path, marker, or fixed runtime configuration is unsafe."""


@dataclass(frozen=True)
class AgentAPIProjection:
    """Non-secret metadata for one validated CLI projection."""

    provider: str
    agent_type: str
    account_id: str
    models: tuple[str, ...]
    root: Path
    home: Path
    helper: Path
    launcher: Path
    wrapper: Path | None


def _provider_spec(provider: str) -> _ProviderSpec:
    spec = _PROVIDER_SPECS.get(provider)
    if spec is None:
        raise AgentAPIConfigurationError(
            f"Unsupported Agent API provider: {provider!r}",
        )
    return spec


def _fixed_endpoints(provider: str) -> dict[str, str | None]:
    spec = _provider_spec(provider)
    return {
        "claude_base_url": spec.claude_base_url,
        "codex_base_url": spec.codex_base_url,
    }


def codex_base_url_for_provider(provider: str) -> str:
    """Return the immutable Codex route for an enabled Agent API provider."""

    return _provider_spec(provider).codex_base_url


def claude_base_url_for_provider(provider: str) -> str:
    """Return the immutable Claude route for an enabled Agent API provider."""

    base_url = _provider_spec(provider).claude_base_url
    if not base_url:
        raise AgentAPIConfigurationError(
            f"Agent API provider {provider!r} has no Claude endpoint",
        )
    return base_url


def _account_id_matches_provider(provider: Any, account_id: Any) -> bool:
    if not isinstance(account_id, str) or len(account_id) > 128:
        return False
    match = _ACCOUNT_ID_RE.fullmatch(account_id)
    return bool(
        match is not None
        and match.group("provider") == provider
    )


def _validate_request(
    provider: str,
    agent_type: str,
    account_id: str,
    api_key: str,
) -> bytes:
    spec = _provider_spec(provider)
    if agent_type not in _SUPPORTED_AGENT_TYPES:
        raise AgentAPIConfigurationError(
            f"Unsupported agent type: {agent_type!r}",
        )
    if agent_type not in spec.agent_types:
        raise AgentAPIConfigurationError(
            f"Agent API provider {provider!r} does not support "
            f"{agent_type!r}",
        )
    if not _account_id_matches_provider(provider, account_id):
        raise AgentAPIConfigurationError("Invalid Agent API account id")
    if not isinstance(api_key, str):
        raise AgentAPIConfigurationError("Invalid API key")
    try:
        payload = api_key.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise AgentAPIConfigurationError("Invalid API key") from exc
    if (
        not payload
        or len(payload) > _MAX_KEY_BYTES
        or api_key.strip() != api_key
        or any(not 33 <= byte <= 126 for byte in payload)
    ):
        raise AgentAPIConfigurationError("Invalid API key")
    return payload


def _normalise_models(
    models: Iterable[str] | Mapping[str, Iterable[str]] | None,
    agent_type: str,
) -> tuple[str, ...]:
    if models is None:
        return ()
    selected: Iterable[str] | None
    if isinstance(models, Mapping):
        selected = models.get(agent_type, ())
    else:
        selected = models
    if isinstance(selected, (str, bytes)) or selected is None:
        raise AgentAPIConfigurationError("Invalid Agent API models")
    values: set[str] = set()
    try:
        for raw in selected:
            if not isinstance(raw, str):
                raise AgentAPIConfigurationError("Invalid Agent API models")
            value = raw.strip()
            try:
                value.encode("utf-8")
            except UnicodeEncodeError as exc:
                raise AgentAPIConfigurationError(
                    "Invalid Agent API models"
                ) from exc
            if (
                not value
                or len(value) > 200
                or value != raw
                or any(not character.isprintable() for character in value)
            ):
                raise AgentAPIConfigurationError("Invalid Agent API models")
            values.add(value)
            if len(values) > 1000:
                raise AgentAPIConfigurationError("Too many Agent API models")
    except TypeError as exc:
        raise AgentAPIConfigurationError("Invalid Agent API models") from exc
    return tuple(sorted(values))


def _absolute(path: str | os.PathLike[str]) -> Path:
    # ``abspath`` collapses lexical ``..`` components without dereferencing
    # symlinks; the latter are rejected separately with lstat.
    return Path(os.path.abspath(os.fspath(Path(path).expanduser())))


def _reject_symlink_ancestors(path: Path) -> None:
    for candidate in (path, *path.parents):
        try:
            metadata = candidate.lstat()
        except FileNotFoundError:
            continue
        except OSError as exc:
            raise UnsafeAgentAPIPathError("Agent API storage is unavailable") from exc
        if stat.S_ISLNK(metadata.st_mode):
            raise UnsafeAgentAPIPathError(
                "Agent API storage has a symlink ancestor",
            )


def _ensure_private_directory(path: Path) -> None:
    _reject_symlink_ancestors(path)
    try:
        path.mkdir(parents=True, mode=0o700, exist_ok=True)
        metadata = path.lstat()
    except OSError as exc:
        raise UnsafeAgentAPIPathError("Agent API storage is unavailable") from exc
    if not stat.S_ISDIR(metadata.st_mode) or metadata.st_uid != os.getuid():
        raise UnsafeAgentAPIPathError("Unsafe Agent API directory")
    if stat.S_IMODE(metadata.st_mode) != 0o700:
        try:
            os.chmod(path, 0o700, follow_symlinks=False)
        except OSError as exc:
            raise UnsafeAgentAPIPathError("Unsafe Agent API directory") from exc
    converged = path.lstat()
    if (
        not stat.S_ISDIR(converged.st_mode)
        or converged.st_uid != os.getuid()
        or stat.S_IMODE(converged.st_mode) != 0o700
    ):
        raise UnsafeAgentAPIPathError("Unsafe Agent API directory")


def _prepare_slot_directory(path: Path) -> None:
    """Create a dedicated slot, but never chmod a caller-owned broad directory."""

    _reject_symlink_ancestors(path)
    try:
        existed = path.exists()
        path.mkdir(parents=True, mode=0o700, exist_ok=True)
        metadata = path.lstat()
    except OSError as exc:
        raise UnsafeAgentAPIPathError("Agent API storage is unavailable") from exc
    if not stat.S_ISDIR(metadata.st_mode) or metadata.st_uid != os.getuid():
        raise UnsafeAgentAPIPathError("Unsafe Agent API slot directory")
    if not existed and stat.S_IMODE(metadata.st_mode) != 0o700:
        try:
            os.chmod(path, 0o700, follow_symlinks=False)
        except OSError as exc:
            raise UnsafeAgentAPIPathError("Unsafe Agent API slot directory") from exc


def _require_private_directory(path: Path) -> None:
    _reject_symlink_ancestors(path)
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise UnsafeAgentAPIPathError("Missing Agent API directory") from exc
    if not stat.S_ISDIR(metadata.st_mode) or metadata.st_uid != os.getuid() or stat.S_IMODE(metadata.st_mode) != 0o700:
        raise UnsafeAgentAPIPathError("Unsafe Agent API directory")


def _fsync_directory(path: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
    if hasattr(os, "O_DIRECTORY"):
        flags |= os.O_DIRECTORY
    descriptor = os.open(path, flags)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _atomic_private_write(path: Path, payload: bytes, *, mode: int = 0o600) -> None:
    _ensure_private_directory(path.parent)
    try:
        existing = path.lstat()
    except FileNotFoundError:
        existing = None
    except OSError as exc:
        raise UnsafeAgentAPIPathError("Agent API storage is unavailable") from exc
    if existing is not None and (not stat.S_ISREG(existing.st_mode) or existing.st_uid != os.getuid()):
        raise UnsafeAgentAPIPathError("Unsafe Agent API file")
    descriptor = -1
    temporary: Path | None = None
    try:
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{path.name}.",
            suffix=".tmp",
            dir=path.parent,
        )
        temporary = Path(temporary_name)
        os.fchmod(descriptor, mode)
        with os.fdopen(descriptor, "wb") as stream:
            descriptor = -1
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        temporary = None
        metadata = path.lstat()
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != os.getuid()
            or stat.S_IMODE(metadata.st_mode) != mode
        ):
            raise UnsafeAgentAPIPathError("Unsafe Agent API file")
        _fsync_directory(path.parent)
    except AgentAPIError:
        raise
    except OSError as exc:
        raise UnsafeAgentAPIPathError("Agent API storage is unavailable") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def _atomic_private_json(path: Path, value: dict[str, Any]) -> None:
    payload = (
        json.dumps(
            value,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")
    _atomic_private_write(path, payload)


def _read_owned_regular(
    path: Path,
    *,
    expected_mode: int,
    maximum: int = _MAX_MANAGED_FILE_BYTES,
) -> bytes:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise UnsafeAgentAPIPathError("Unsafe or missing Agent API file") from exc
    try:
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != os.getuid()
            or stat.S_IMODE(metadata.st_mode) != expected_mode
            or metadata.st_size > maximum
        ):
            raise UnsafeAgentAPIPathError("Unsafe Agent API file")
        payload = b""
        while len(payload) <= maximum:
            chunk = os.read(descriptor, min(65536, maximum + 1 - len(payload)))
            if not chunk:
                break
            payload += chunk
        if len(payload) > maximum:
            raise UnsafeAgentAPIPathError("Agent API file is too large")
        opened = os.fstat(descriptor)
        current = path.stat(follow_symlinks=False)
        if (
            opened.st_dev != current.st_dev
            or opened.st_ino != current.st_ino
            or not stat.S_ISREG(current.st_mode)
            or current.st_uid != os.getuid()
        ):
            raise UnsafeAgentAPIPathError("Agent API file changed while reading")
        return payload
    finally:
        os.close(descriptor)


def _helper_command(root: Path) -> str:
    return shlex.quote(str(root / "key-helper-launcher"))


def _codex_config(root: Path, provider: str) -> str:
    spec = _provider_spec(provider)
    provider_id = spec.codex_provider_id
    helper = json.dumps(str(root / "key-helper-launcher"))
    return (
        f"model_provider = {json.dumps(provider_id)}\n\n"
        f"[model_providers.{provider_id}]\n"
        f"name = {json.dumps(spec.codex_provider_name)}\n"
        f"base_url = {json.dumps(spec.codex_base_url)}\n"
        'wire_api = "responses"\n'
        "supports_websockets = false\n\n"
        f"[model_providers.{provider_id}.auth]\n"
        f"command = {helper}\n"
        "timeout_ms = 5000\n"
        "refresh_interval_ms = 0\n"
    )


def _marker(
    *,
    provider: str,
    agent_type: str,
    account_id: str,
    models: tuple[str, ...],
    root: Path,
    home: Path,
) -> dict[str, Any]:
    return {
        "version": _PROJECTION_VERSION,
        "managed_by": "elastic-agent",
        "provider": provider,
        "agent_type": agent_type,
        "account_id": account_id,
        "root": str(root),
        "home": str(home),
        "models": list(models),
        "endpoints": _fixed_endpoints(provider),
    }


def _contains_secret(value: Any, secret: str) -> bool:
    if isinstance(value, str):
        return secret in value
    if isinstance(value, dict):
        return any(_contains_secret(item, secret) for item in value.values())
    if isinstance(value, (list, tuple)):
        return any(_contains_secret(item, secret) for item in value)
    return False


def _root_for_slot(
    slot: Path,
    *,
    provider: str,
    account_id: str,
) -> Path:
    return slot / _MANAGED_DIRECTORY / provider / account_id


def _candidate_root(path: Path) -> Path:
    if path.name in _SUPPORTED_AGENT_TYPES:
        return path.parent
    return path


def _load_marker(
    path: str | os.PathLike[str],
    *,
    validate_runtime: bool,
) -> AgentAPIProjection | None:
    requested = _absolute(path)
    root = _candidate_root(requested)
    marker_path = root / _MARKER_NAME
    if not marker_path.exists() and not marker_path.is_symlink():
        return None
    _require_private_directory(root)
    try:
        data = json.loads(
            _read_owned_regular(
                marker_path,
                expected_mode=0o600,
            ).decode("utf-8")
        )
    except (UnicodeDecodeError, ValueError, RecursionError) as exc:
        raise UnsafeAgentAPIPathError("Invalid Agent API projection marker") from exc
    if not isinstance(data, dict) or frozenset(data) != _MARKER_KEYS:
        raise UnsafeAgentAPIPathError("Invalid Agent API projection marker")
    provider = data.get("provider")
    agent_type = data.get("agent_type")
    account_id = data.get("account_id")
    models = data.get("models")
    if (
        type(data.get("version")) is not int
        or data.get("version") != _PROJECTION_VERSION
        or data.get("managed_by") != "elastic-agent"
        or not isinstance(provider, str)
        or provider not in _SUPPORTED_PROVIDERS
        or not isinstance(agent_type, str)
        or agent_type not in _SUPPORTED_AGENT_TYPES
        or (
            isinstance(provider, str)
            and provider in _PROVIDER_SPECS
            and isinstance(agent_type, str)
            and agent_type not in _PROVIDER_SPECS[provider].agent_types
        )
        or not _account_id_matches_provider(provider, account_id)
        or not isinstance(models, list)
        or data.get("endpoints") != _fixed_endpoints(provider)
    ):
        raise UnsafeAgentAPIPathError("Invalid Agent API projection marker")
    try:
        normalized_models = _normalise_models(models, agent_type)
    except AgentAPIConfigurationError as exc:
        raise UnsafeAgentAPIPathError(
            "Invalid Agent API projection marker"
        ) from exc
    if list(normalized_models) != models:
        raise UnsafeAgentAPIPathError("Invalid Agent API projection marker")
    home = root / agent_type
    if (
        root.name != account_id
        or root.parent.name != provider
        or root.parent.parent.name != _MANAGED_DIRECTORY
        or data.get("root") != str(root)
        or data.get("home") != str(home)
        or requested not in {root, home}
    ):
        raise UnsafeAgentAPIPathError("Agent API projection marker/path mismatch")
    _require_private_directory(home)
    projection = AgentAPIProjection(
        provider=provider,
        agent_type=agent_type,
        account_id=account_id,
        models=normalized_models,
        root=root,
        home=home,
        helper=root / "key-helper",
        launcher=root / "key-helper-launcher",
        wrapper=(root / "claude-wrapper" if agent_type == "claude" else None),
    )
    if validate_runtime:
        _validate_runtime(projection)
    return projection


def _validate_runtime(projection: AgentAPIProjection) -> None:
    try:
        launcher = _read_owned_regular(
            projection.launcher,
            expected_mode=0o700,
            maximum=len(_KEY_HELPER_LAUNCHER.encode("utf-8")),
        )
    except UnsafeAgentAPIPathError as exc:
        raise UnsafeAgentAPIPathError(
            "Modified Agent API credential launcher",
        ) from exc
    if launcher != _KEY_HELPER_LAUNCHER.encode("utf-8"):
        raise UnsafeAgentAPIPathError(
            "Modified Agent API credential launcher",
        )
    try:
        helper = _read_owned_regular(
            projection.helper,
            expected_mode=0o700,
            maximum=len(_KEY_HELPER.encode("utf-8")),
        )
    except UnsafeAgentAPIPathError as exc:
        raise UnsafeAgentAPIPathError(
            "Modified Agent API credential helper",
        ) from exc
    if helper != _KEY_HELPER.encode("utf-8"):
        raise UnsafeAgentAPIPathError(
            "Modified Agent API credential helper",
        )
    key = _read_owned_regular(
        projection.root / "api.key",
        expected_mode=0o600,
        maximum=_MAX_KEY_BYTES,
    )
    if (
        not key
        or key.strip() != key
        or any(not 33 <= byte <= 126 for byte in key)
    ):
        raise UnsafeAgentAPIPathError("Invalid Agent API key file")

    if projection.agent_type == "claude":
        claude_base_url = _provider_spec(projection.provider).claude_base_url
        if not claude_base_url:
            raise UnsafeAgentAPIPathError("Agent API provider has no Claude endpoint")
        expected_wrapper = _claude_wrapper(claude_base_url).encode("utf-8")
        wrapper = _read_owned_regular(
            projection.root / "claude-wrapper",
            expected_mode=0o700,
            maximum=len(expected_wrapper),
        )
        if wrapper != expected_wrapper:
            raise UnsafeAgentAPIPathError("Modified Claude Agent API wrapper")
        _require_private_directory(projection.root / "bin")
        shim = _read_owned_regular(
            projection.root / "bin" / "claude",
            expected_mode=0o700,
            maximum=len(expected_wrapper),
        )
        if shim != expected_wrapper:
            raise UnsafeAgentAPIPathError("Modified Claude Agent API shim")
        try:
            settings = json.loads(
                _read_owned_regular(
                    projection.home / "settings.json",
                    expected_mode=0o600,
                ).decode("utf-8")
            )
            onboarding = json.loads(
                _read_owned_regular(
                    projection.home / ".claude.json",
                    expected_mode=0o600,
                ).decode("utf-8")
            )
        except (UnicodeDecodeError, ValueError, RecursionError) as exc:
            raise UnsafeAgentAPIPathError(
                "Invalid Claude Agent API configuration",
            ) from exc
        expected_settings = {
            "env": {
                "ANTHROPIC_BASE_URL": claude_base_url,
            },
            "apiKeyHelper": _helper_command(projection.root),
            "skipDangerousModePermissionPrompt": True,
        }
        if (
            settings != expected_settings
            or onboarding != {"hasCompletedOnboarding": True}
        ):
            raise UnsafeAgentAPIPathError("Modified Claude API routing")
        return

    expected = _codex_config(
        projection.root,
        projection.provider,
    ).encode("utf-8")
    config = _read_owned_regular(
        projection.home / "config.toml",
        expected_mode=0o600,
        maximum=len(expected),
    )
    if config != expected:
        # Parse valid TOML only to keep malformed and modified routing failures
        # equally secret-free; all content is still required to be byte exact.
        try:
            tomllib.loads(config.decode("utf-8"))
        except (UnicodeDecodeError, tomllib.TOMLDecodeError):
            pass
        raise UnsafeAgentAPIPathError("Modified Codex API routing")


def configure_agent_api(
    *,
    provider: str = "cloudrouter",
    agent_type: str,
    config_dir: str | os.PathLike[str] | None = None,
    api_key: str,
    account_id: str,
    models: Iterable[str] | Mapping[str, Iterable[str]] | None = None,
) -> str:
    """Create/update a private Agent API projection and return its CLI home.

    ``config_dir`` is an account-slot root, not the returned ``CLAUDE_CONFIG_DIR``
    or ``CODEX_HOME``.  Passing an already projected CLI home is idempotently
    supported.  An empty slot is resolved beneath the actual worker user's
    home, never by guessing another user's home.
    """

    key_payload = _validate_request(provider, agent_type, account_id, api_key)
    normalised_models = _normalise_models(models, agent_type)

    raw_slot = _absolute(config_dir) if config_dir else Path.home().absolute() / ".elastic-agent" / "api-accounts"
    if raw_slot == Path(raw_slot.anchor):
        raise AgentAPIConfigurationError("Refusing broad Agent API config directory")

    existing_from_home = _load_marker(raw_slot, validate_runtime=True)
    if existing_from_home is not None:
        projection_root = existing_from_home.root
        slot = projection_root.parent.parent.parent
    else:
        slot = raw_slot
        projection_root = _root_for_slot(
            slot,
            provider=provider,
            account_id=account_id,
        )

    home = projection_root / agent_type
    marker_value = _marker(
        provider=provider,
        agent_type=agent_type,
        account_id=account_id,
        models=normalised_models,
        root=projection_root,
        home=home,
    )
    marker_payload = (
        json.dumps(
            marker_value,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")
    if _contains_secret(marker_value, api_key) or key_payload in marker_payload:
        raise AgentAPIConfigurationError(
            "API key conflicts with non-secret projection metadata",
        )

    marker_path = projection_root / _MARKER_NAME
    already_managed = marker_path.exists() or marker_path.is_symlink()
    if projection_root.exists() or projection_root.is_symlink():
        if not already_managed:
            try:
                nonempty = next(projection_root.iterdir(), None) is not None
            except OSError as exc:
                raise UnsafeAgentAPIPathError(
                    "Agent API storage is unavailable",
                ) from exc
            if nonempty:
                raise UnsafeAgentAPIPathError(
                    "Refusing an unmarked Agent API projection directory",
                )
        else:
            current = validate_agent_api_home(projection_root)
            if current.provider != provider or current.agent_type != agent_type or current.account_id != account_id:
                raise UnsafeAgentAPIPathError(
                    "Agent API projection identity mismatch",
                )

    try:
        _prepare_slot_directory(slot)
        for directory in (
            slot / _MANAGED_DIRECTORY,
            slot / _MANAGED_DIRECTORY / provider,
            projection_root,
            home,
        ):
            _ensure_private_directory(directory)

        if not already_managed:
            _atomic_private_write(
                projection_root / "key-helper",
                _KEY_HELPER.encode("utf-8"),
                mode=0o700,
            )
            _atomic_private_write(
                projection_root / "key-helper-launcher",
                _KEY_HELPER_LAUNCHER.encode("utf-8"),
                mode=0o700,
            )
            if agent_type == "claude":
                claude_base_url = _provider_spec(provider).claude_base_url
                if not claude_base_url:
                    raise AgentAPIConfigurationError(
                        "Agent API provider has no Claude endpoint"
                    )
                claude_wrapper = _claude_wrapper(claude_base_url)
                settings = {
                    "env": {
                        "ANTHROPIC_BASE_URL": claude_base_url,
                    },
                    "apiKeyHelper": _helper_command(projection_root),
                    "skipDangerousModePermissionPrompt": True,
                }
                _atomic_private_json(home / "settings.json", settings)
                _atomic_private_json(
                    home / ".claude.json",
                    {"hasCompletedOnboarding": True},
                )
                _atomic_private_write(
                    projection_root / "claude-wrapper",
                    claude_wrapper.encode("utf-8"),
                    mode=0o700,
                )
                _atomic_private_write(
                    projection_root / "bin" / "claude",
                    claude_wrapper.encode("utf-8"),
                    mode=0o700,
                )
            else:
                _atomic_private_write(
                    home / "config.toml",
                    _codex_config(
                        projection_root,
                        provider,
                    ).encode("utf-8"),
                )

        # Write the key just before the marker.  Initial projections are not
        # discoverable until all runtime files are durable; rotations retain a
        # fully valid old marker/config while the key is atomically replaced.
        _atomic_private_write(projection_root / "api.key", key_payload)
        _atomic_private_write(marker_path, marker_payload)
        validate_agent_api_home(home)
    except Exception:
        if not already_managed:
            try:
                _remove_owned_tree(projection_root)
            except Exception:
                # Preserve the original error.  A future configuration attempt
                # will fail closed on the unmarked partial tree.
                pass
        raise
    return str(home)


def validate_agent_api_home(
    path: str | os.PathLike[str],
    *,
    provider: str | None = None,
    agent_type: str | None = None,
    account_id: str | None = None,
) -> AgentAPIProjection:
    """Return validated non-secret projection metadata or fail closed."""

    projection = _load_marker(path, validate_runtime=True)
    if projection is None:
        raise UnsafeAgentAPIPathError("Agent API projection marker is missing")
    if provider is not None and projection.provider != provider:
        raise UnsafeAgentAPIPathError("Agent API provider mismatch")
    if agent_type is not None and projection.agent_type != agent_type:
        raise UnsafeAgentAPIPathError("Agent API agent type mismatch")
    if account_id is not None and projection.account_id != account_id:
        raise UnsafeAgentAPIPathError("Agent API account mismatch")
    return projection


def agent_api_marker_for_home(
    path: str | os.PathLike[str],
) -> AgentAPIProjection | None:
    """Detect and validate a marker from either a projection root or CLI home."""

    return _load_marker(path, validate_runtime=True)


def is_managed_agent_api_home(
    path: str | os.PathLike[str],
    *,
    provider: str | None = None,
    agent_type: str | None = None,
) -> bool:
    """Return whether ``path`` is a fully valid managed Agent API home."""

    try:
        projection = _load_marker(path, validate_runtime=True)
    except (AgentAPIError, OSError, ValueError):
        return False
    return bool(
        projection is not None
        and (provider is None or projection.provider == provider)
        and (agent_type is None or projection.agent_type == agent_type)
    )


def claude_wrapper_for_home(path: str | os.PathLike[str]) -> str:
    """Return the validated per-projection Claude final-exec wrapper."""

    projection = validate_agent_api_home(
        path,
        agent_type="claude",
    )
    if projection.wrapper is None:  # pragma: no cover - protected by validation
        raise UnsafeAgentAPIPathError("Claude Agent API wrapper is unavailable")
    return str(projection.wrapper)


def claude_shim_directory_for_home(path: str | os.PathLike[str]) -> str:
    """Return a validated PATH directory containing the final-exec shim."""

    projection = validate_agent_api_home(
        path,
        agent_type="claude",
    )
    return str(projection.root / "bin")


def scrub_agent_api_env(
    env: MutableMapping[str, str],
    *,
    provider: str = "cloudrouter",
    agent_type: str,
) -> MutableMapping[str, str]:
    """Remove inherited official credentials that override helper auth."""

    spec = _provider_spec(provider)
    if agent_type not in _SUPPORTED_AGENT_TYPES:
        raise AgentAPIConfigurationError(
            f"Unsupported agent type: {agent_type!r}",
        )
    if agent_type not in spec.agent_types:
        raise AgentAPIConfigurationError(
            f"Agent API provider {provider!r} does not support "
            f"{agent_type!r}",
        )
    keys = (
        CLOUDROUTER_CLAUDE_AUTH_ENV_KEYS
        | CLOUDROUTER_CLAUDE_PROVIDER_ENV_KEYS
        if agent_type == "claude"
        else AGENT_API_CODEX_AUTH_ENV_KEYS
    )
    env.pop(ELASTIC_AGENT_API_PROJECTION_ROOT_ENV, None)
    for key in keys:
        env.pop(key, None)
    if agent_type == "claude":
        # A Job-level override must not redirect a helper-authenticated request
        # and disclose the delegated key to another origin.
        claude_base_url = _provider_spec(provider).claude_base_url
        if not claude_base_url:
            raise AgentAPIConfigurationError(
                "Agent API provider has no Claude endpoint"
            )
        env["ANTHROPIC_BASE_URL"] = claude_base_url
    else:
        # The selected custom provider owns its fixed base URL in config.toml.
        # Remove generic process-level aliases that can supersede it.
        env.pop("OPENAI_BASE_URL", None)
        env.pop("CODEX_BASE_URL", None)
    return env


def apply_agent_api_runtime_env(
    env: MutableMapping[str, str],
    projection: AgentAPIProjection,
) -> MutableMapping[str, str]:
    """Apply the validated host/container runtime contract for a projection."""

    scrub_agent_api_env(
        env,
        provider=projection.provider,
        agent_type=projection.agent_type,
    )
    credential_home = "CLAUDE_CONFIG_DIR" if projection.agent_type == "claude" else "CODEX_HOME"
    env[credential_home] = str(projection.home)
    env[ELASTIC_AGENT_API_PROJECTION_ROOT_ENV] = str(projection.root)
    return env


def _validate_removable_tree(path: Path) -> None:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return
    except OSError as exc:
        raise UnsafeAgentAPIPathError("Agent API storage is unavailable") from exc
    if metadata.st_uid != os.getuid():
        raise UnsafeAgentAPIPathError("Agent API tree contains another owner")
    if stat.S_ISLNK(metadata.st_mode) or stat.S_ISREG(metadata.st_mode):
        return
    if not stat.S_ISDIR(metadata.st_mode):
        raise UnsafeAgentAPIPathError("Agent API tree contains a special file")
    try:
        children = list(path.iterdir())
    except OSError as exc:
        raise UnsafeAgentAPIPathError("Agent API storage is unavailable") from exc
    for child in children:
        _validate_removable_tree(child)


def _remove_owned_tree(path: Path) -> None:
    if not path.exists() and not path.is_symlink():
        return
    _validate_removable_tree(path)

    def remove(current: Path) -> None:
        metadata = current.lstat()
        if stat.S_ISDIR(metadata.st_mode) and not stat.S_ISLNK(metadata.st_mode):
            for child in list(current.iterdir()):
                remove(child)
            current.rmdir()
        else:
            current.unlink()

    remove(path)


def scrub_agent_api_projection(path: str | os.PathLike[str]) -> bool:
    """Delete one marker-owned projection without following links.

    Runtime routing files need not still be valid, allowing cleanup after a CLI
    or disk error, but the private marker, identity, containment, and ownership
    checks must all pass before anything is removed.
    """

    projection = _load_marker(path, validate_runtime=False)
    if projection is None:
        return False
    root = projection.root
    provider_directory = root.parent
    managed_directory = provider_directory.parent
    _validate_removable_tree(root)
    _remove_owned_tree(root)
    for candidate in (provider_directory, managed_directory):
        try:
            if candidate.exists() and not any(candidate.iterdir()):
                _require_private_directory(candidate)
                candidate.rmdir()
        except FileNotFoundError:
            continue
        except OSError as exc:
            raise UnsafeAgentAPIPathError(
                "Agent API cleanup could not complete",
            ) from exc
    return True


__all__ = [
    "AgentAPIConfigurationError",
    "AgentAPIError",
    "AgentAPIProjection",
    "AGENT_API_CODEX_AUTH_ENV_KEYS",
    "APEX_CLAUDE_BASE_URL",
    "APEX_CODEX_BASE_URL",
    "CLOUDROUTER_CLAUDE_AUTH_ENV_KEYS",
    "CLOUDROUTER_CLAUDE_BASE_URL",
    "CLOUDROUTER_CLAUDE_BINARY_ENV",
    "CLOUDROUTER_CLAUDE_PROVIDER_ENV_KEYS",
    "CLOUDROUTER_CODEX_AUTH_ENV_KEYS",
    "CLOUDROUTER_CODEX_BASE_URL",
    "ELASTIC_AGENT_API_PROJECTION_ROOT_ENV",
    "UnsafeAgentAPIPathError",
    "agent_api_marker_for_home",
    "apply_agent_api_runtime_env",
    "claude_base_url_for_provider",
    "claude_wrapper_for_home",
    "claude_shim_directory_for_home",
    "codex_base_url_for_provider",
    "configure_agent_api",
    "is_managed_agent_api_home",
    "scrub_agent_api_env",
    "scrub_agent_api_projection",
    "validate_agent_api_home",
]
