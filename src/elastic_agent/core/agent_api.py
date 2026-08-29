"""Provider-neutral Agent API accounts.

Agent API accounts are deliberately separate from browser/OAuth identities.
The Manager persists an opaque provider key, the models discovered through the
provider adapter, and non-secret routing metadata.  Provider-specific HTTP
behavior lives behind :class:`AgentApiProviderAdapter`; CloudRouter and
ApexRouter are registered explicitly by the default registry.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
import os
import re
import stat
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping, Protocol, runtime_checkable

from elastic_agent.core.secure_store import fsync_directory

ACCOUNT_ID_RE = re.compile(r"^(?P<provider>[a-z][a-z0-9-]{0,31})-(?P<number>[1-9][0-9]*)$")
PROVIDER_RE = re.compile(r"^[a-z][a-z0-9-]{0,31}$")
MAX_ACCOUNT_METADATA_BYTES = 256 * 1024
MAX_AGENT_API_KEY_BYTES = 16 * 1024
MAX_AGENT_API_MODEL_ID_LENGTH = 200
MAX_AGENT_API_MODELS = 1000
MAX_ACCOUNT_NAME_LENGTH = 100
MAX_GROUP_LENGTH = 100
MAX_ACCOUNT_NUMBER = 2_147_483_647
MAX_TIMESTAMP_SECONDS = 10_000_000_000
MAX_TIMESTAMP_NS = 9_223_372_036_854_775_807
PLATFORM_CREDENTIAL_REF_RE = re.compile(
    r"^arn:aws:secretsmanager:[a-z]{2}(?:-gov)?-[a-z]+-\d:[0-9]{12}:"
    r"secret:task-platform/[A-Za-z0-9._-]{1,128}/[A-Za-z0-9._-]{1,128}/"
    r"apex/[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}"
    r"(?:-[A-Za-z0-9]{6})?$"
)
STORE_STATE_NAME = ".store.json"
RUNTIME_UNAVAILABLE_NAME = "runtime-unavailable.json"
MAX_RUNTIME_UNAVAILABLE_BYTES = 16 * 1024
LAST_KNOWN_UNAVAILABLE_NAME = "last-known-unavailable.json"
MAX_LAST_KNOWN_UNAVAILABLE_BYTES = 16 * 1024
_FAIL_CLOSED_USAGE_ERROR_CODES = frozenset(
    {
        "invalid_json",
        "invalid_usage_response",
        "response_too_large",
        "unexpected_redirect",
        "upstream_rejected",
    }
)


class AgentApiError(RuntimeError):
    """Base class for Agent API account failures."""


class AgentApiStorageError(AgentApiError):
    """Managed credential storage failed a safety or integrity check."""


class AgentApiAccountNotFoundError(AgentApiError):
    """The requested Agent API account does not exist."""


class AgentApiDuplicateKeyError(AgentApiError):
    """The provider key is already represented by another account."""


class AgentApiUnsupportedProviderError(AgentApiError):
    """No enabled adapter handles the requested API provider."""


class AgentApiUpstreamError(AgentApiError):
    """A provider rejected or could not complete a bounded request."""

    def __init__(self, code: str, status_code: int | None = None):
        self.code = str(code)
        self.status_code = status_code
        super().__init__(self.code)


@runtime_checkable
class AgentApiProviderAdapter(Protocol):
    """Provider contract for explicitly registered Agent API gateways."""

    @property
    def provider(self) -> str:
        """Stable storage/API identifier for the provider."""

    @property
    def endpoints(self) -> Mapping[str, str | None]:
        """Fixed, non-secret endpoints shown in public account metadata."""

    async def probe_models(self, api_key: str) -> dict[str, list[str]]:
        """Validate a key and project upstream models to Claude/Codex."""

    async def fetch_usage(
        self,
        account_id: str,
        api_key: str,
    ) -> dict[str, Any]:
        """Fetch and normalize provider usage into the common availability shape."""


class AgentApiProviderRegistry:
    """Explicit provider registry.

    Merely implementing the protocol does not make a provider selectable until
    it is explicitly registered. Registration order is also allocation order,
    preserving the established CloudRouter preference when Apex is enabled.
    """

    def __init__(
        self,
        adapters: list[AgentApiProviderAdapter] | tuple[AgentApiProviderAdapter, ...] = (),
    ) -> None:
        self._adapters: dict[str, AgentApiProviderAdapter] = {}
        for adapter in adapters:
            self.register(adapter)

    @classmethod
    def default(cls) -> AgentApiProviderRegistry:
        from elastic_agent.core.apexrouter import ApexRouterAdapter
        from elastic_agent.core.cloudrouter import CloudRouterAdapter

        return cls((CloudRouterAdapter(), ApexRouterAdapter()))

    def register(self, adapter: AgentApiProviderAdapter) -> None:
        provider = str(adapter.provider or "").strip().lower()
        if not PROVIDER_RE.fullmatch(provider):
            raise ValueError("invalid Agent API provider id")
        if provider in self._adapters:
            raise ValueError(f"Agent API provider already registered: {provider}")
        self._adapters[provider] = adapter

    @property
    def providers(self) -> tuple[str, ...]:
        return tuple(self._adapters)

    def require(self, provider: str) -> AgentApiProviderAdapter:
        normalized = str(provider or "").strip().lower()
        adapter = self._adapters.get(normalized)
        if adapter is None:
            raise AgentApiUnsupportedProviderError(
                f"unsupported Agent API provider: {normalized or '<empty>'}"
            )
        return adapter


def _normalized_model(model: str | None) -> str:
    value = str(model or "").strip()
    if value.endswith("[1m]"):
        value = value[:-4]
    return value


@dataclass(frozen=True, slots=True)
class AgentApiAccount:
    """Non-secret projection of one provider key into the shared account pool."""

    id: str
    name: str
    group: str
    enabled: bool
    admission_pending: bool
    api_provider: str
    models: dict[str, list[str]]
    key_fingerprint: str
    credential_ref: str | None
    root: Path
    endpoints: dict[str, str | None]

    @property
    def email(self) -> str:
        """Compatibility identity used by existing account-pool displays."""

        return self.name

    @property
    def auth_kind(self) -> str:
        return "agent_api"

    @property
    def has_api_key(self) -> bool:
        # Instances are constructed only after the private key file is checked.
        return True

    @property
    def supported_agent_types(self) -> list[str]:
        return [
            agent_type
            for agent_type in ("claude", "codex")
            if self.models.get(agent_type)
        ]

    def supports_agent_type(self, agent_type: str) -> bool:
        return str(agent_type or "").strip().lower() in self.supported_agent_types

    def supports_model(self, agent_type: str, model: str | None) -> bool:
        provider = str(agent_type or "").strip().lower()
        if provider not in {"claude", "codex"}:
            return False
        requested = _normalized_model(model)
        if not requested or requested == "default":
            return self.supports_agent_type(provider)
        available = {
            _normalized_model(item)
            for item in self.models.get(provider, [])
        }
        if requested in available:
            return True
        if provider == "claude":
            dated = re.compile(rf"^{re.escape(requested)}-[0-9]{{8}}$")
            return any(dated.fullmatch(candidate) for candidate in available)
        return False

    def public_dict(self) -> dict[str, Any]:
        """REST-safe metadata; the key and reversible key hints never appear."""

        return {
            "id": self.id,
            "name": self.name,
            "email": self.email,
            "group": self.group,
            "enabled": self.enabled,
            "admission_pending": self.admission_pending,
            "auth_kind": self.auth_kind,
            "api_provider": self.api_provider,
            "models": {
                provider: list(models)
                for provider, models in self.models.items()
            },
            "supported_agent_types": self.supported_agent_types,
            "has_api_key": self.has_api_key,
            "key_fingerprint": self.key_fingerprint,
            "credential_source": "platform_ref" if self.credential_ref else "local_legacy",
            "endpoints": dict(self.endpoints),
        }


def _check_no_symlink_ancestors(path: Path) -> None:
    for candidate in (path, *path.parents):
        try:
            metadata = candidate.lstat()
        except FileNotFoundError:
            continue
        except OSError as exc:
            raise AgentApiStorageError("unable to inspect managed path") from exc
        if stat.S_ISLNK(metadata.st_mode):
            raise AgentApiStorageError("managed path has a symlink ancestor")


def _private_directory(path: Path, *, create: bool) -> Path:
    _check_no_symlink_ancestors(path)
    if not path.exists():
        if not create:
            raise AgentApiStorageError("managed directory is missing")
        try:
            path.mkdir(parents=True, mode=0o700)
        except OSError as exc:
            raise AgentApiStorageError("unable to create managed directory") from exc
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise AgentApiStorageError("unable to inspect managed directory") from exc
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or metadata.st_uid != os.getuid()
        or stat.S_IMODE(metadata.st_mode) != 0o700
    ):
        raise AgentApiStorageError(
            "managed directory must be an owned mode-0700 directory"
        )
    return path


def _read_private_file(path: Path, *, maximum: int) -> bytes:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise AgentApiStorageError("unable to open managed private file") from exc
    try:
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != os.getuid()
            or stat.S_IMODE(metadata.st_mode) != 0o600
            or metadata.st_size > maximum
        ):
            raise AgentApiStorageError(
                "managed file must be an owned bounded mode-0600 regular file"
            )
        chunks: list[bytes] = []
        remaining = maximum + 1
        while remaining:
            chunk = os.read(descriptor, min(65536, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        payload = b"".join(chunks)
        if len(payload) > maximum:
            raise AgentApiStorageError("managed private file is too large")
        # Ensure the opened inode is still the one named by the managed path.
        try:
            current = path.stat(follow_symlinks=False)
        except OSError as exc:
            raise AgentApiStorageError("managed private file changed while reading") from exc
        if (
            current.st_dev != metadata.st_dev
            or current.st_ino != metadata.st_ino
            or not stat.S_ISREG(current.st_mode)
        ):
            raise AgentApiStorageError("managed private file changed while reading")
        return payload
    finally:
        os.close(descriptor)


def _atomic_private_write(path: Path, payload: bytes) -> None:
    _private_directory(path.parent, create=False)
    if path.is_symlink():
        raise AgentApiStorageError("refusing to replace a managed symlink")
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=path.parent,
    )
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as stream:
            descriptor = -1
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        fsync_directory(path.parent)
    except OSError as exc:
        raise AgentApiStorageError("unable to durably write managed file") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        temporary.unlink(missing_ok=True)


def _atomic_private_json(path: Path, value: dict[str, Any]) -> None:
    _atomic_private_write(
        path,
        (json.dumps(value, ensure_ascii=False, indent=2) + "\n").encode("utf-8"),
    )


def _validate_text(value: str, *, field: str, maximum: int) -> str:
    normalized = str(value or "").strip()
    try:
        normalized.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise ValueError(
            f"{field} must be 1-{maximum} printable characters"
        ) from exc
    if (
        not normalized
        or len(normalized) > maximum
        or any(not character.isprintable() for character in normalized)
    ):
        raise ValueError(f"{field} must be 1-{maximum} printable characters")
    return normalized


def _key_fingerprint(api_key: str) -> str:
    digest = hashlib.sha256(api_key.encode("utf-8")).hexdigest()
    return f"sha256:{digest}"


def _validate_platform_credential_ref(value: str) -> str:
    normalized = str(value or "").strip()
    if PLATFORM_CREDENTIAL_REF_RE.fullmatch(normalized) is None:
        raise ValueError("Invalid platform credential reference")
    return normalized


def _resolve_platform_credential_ref(reference: str) -> str:
    """Resolve one exact Task Platform secret ARN without caching its value."""
    secret_id = _validate_platform_credential_ref(reference)
    try:
        import boto3
        region = secret_id.split(":", 5)[3]

        response = boto3.client("secretsmanager", region_name=region).get_secret_value(SecretId=secret_id)
        payload = response.get("SecretString")
        if not isinstance(payload, str):
            raise AgentApiStorageError("platform credential secret has no string value")
        parsed = json.loads(payload)
        api_key = parsed.get("api_key") if isinstance(parsed, dict) else None
        if not isinstance(api_key, str):
            raise AgentApiStorageError("platform credential secret is invalid")
        return api_key
    except AgentApiStorageError:
        raise
    except Exception as exc:
        raise AgentApiStorageError("unable to resolve platform credential") from exc


def _valid_timestamp(value: Any) -> bool:
    return bool(
        not isinstance(value, bool)
        and isinstance(value, (int, float))
        and (not isinstance(value, float) or math.isfinite(value))
        and 0 < value <= MAX_TIMESTAMP_SECONDS
    )


def _validated_models(models: Any) -> dict[str, list[str]]:
    if (
        not isinstance(models, dict)
        or any(key not in {"claude", "codex"} for key in models)
    ):
        raise ValueError("invalid Agent API model projection")
    normalized: dict[str, list[str]] = {}
    total = 0
    for agent_type in ("claude", "codex"):
        values = models.get(agent_type, [])
        if not isinstance(values, list):
            raise ValueError("invalid Agent API model projection")
        selected: set[str] = set()
        for value in values:
            try:
                value_utf8 = value.encode("utf-8") if isinstance(value, str) else b""
            except UnicodeEncodeError:
                value_utf8 = b""
            if (
                not isinstance(value, str)
                or not value
                or not value_utf8
                or value.strip() != value
                or len(value) > MAX_AGENT_API_MODEL_ID_LENGTH
                or any(not character.isprintable() for character in value)
            ):
                raise ValueError("invalid Agent API model projection")
            selected.add(value)
        total += len(selected)
        if total > MAX_AGENT_API_MODELS:
            raise ValueError("too many Agent API models")
        normalized[agent_type] = sorted(selected)
    return normalized


def _metadata_bytes(value: dict[str, Any]) -> bytes:
    payload = (
        json.dumps(value, ensure_ascii=False, indent=2) + "\n"
    ).encode("utf-8")
    if len(payload) > MAX_ACCOUNT_METADATA_BYTES:
        raise ValueError("Agent API account metadata is too large")
    return payload


def _unknown_snapshot(
    account_id: str,
    reason: str,
    previous: dict[str, Any] | None,
) -> dict[str, Any]:
    snapshot = dict(previous or {})
    if previous is not None:
        snapshot["last_known_available"] = previous.get(
            "last_known_available",
            previous.get("available"),
        )
        snapshot["last_known_reason"] = previous.get(
            "last_known_reason",
            previous.get("reason"),
        )
    snapshot.update(
        {
            "account_id": account_id,
            "fetched_at": time.time(),
            "state": "unknown",
            "status": "unknown",
            "stale": previous is not None,
            "available": True,
            "known": False,
            "reason": reason,
        }
    )
    return snapshot


def _unavailable_snapshot(account_id: str, reason: str) -> dict[str, Any]:
    return {
        "account_id": account_id,
        "fetched_at": time.time(),
        "state": "unavailable",
        "status": "unavailable",
        "stale": False,
        "available": False,
        "known": True,
        "reason": reason,
        "mode": None,
        "quota": None,
        "windows": [],
    }


def _public_usage_snapshot(snapshot: Mapping[str, Any]) -> dict[str, Any]:
    """Strip private CAS metadata before usage leaves the store."""

    return {
        key: value
        for key, value in snapshot.items()
        if not key.startswith("_")
    }


class AgentApiAccountStore:
    """Private per-account key store driven by explicit provider adapters."""

    def __init__(
        self,
        root: str | os.PathLike[str],
        *,
        registry: AgentApiProviderRegistry | None = None,
        quota_cache_ttl: float = 60.0,
        credential_resolver: Callable[[str], str] | None = None,
    ) -> None:
        expanded = os.path.expandvars(os.path.expanduser(os.fspath(root)))
        self.root = Path(expanded).absolute()
        self.registry = registry or AgentApiProviderRegistry.default()
        self._quota_cache_ttl = max(0.0, float(quota_cache_ttl))
        self._credential_resolver = credential_resolver or _resolve_platform_credential_ref
        self._accounts: dict[str, AgentApiAccount] = {}
        self._usage_cache: dict[str, dict[str, Any]] = {}
        self._usage_cached_at: dict[str, float] = {}
        self._usage_fetch_locks: dict[str, asyncio.Lock] = {}
        self._runtime_tombstones: dict[str, dict[str, Any]] = {}
        self._last_known_unavailable: dict[str, dict[str, Any]] = {}
        self._high_water: dict[str, int] = {}
        self._lock = asyncio.Lock()
        _private_directory(self.root, create=True)
        self._reload_sync()

    @staticmethod
    def _validate_api_key(api_key: str, maximum: int) -> str:
        try:
            encoded = api_key.encode("utf-8") if isinstance(api_key, str) else b""
        except UnicodeEncodeError:
            encoded = b""
        if (
            not isinstance(api_key, str)
            or not api_key
            or not encoded
            or api_key.strip() != api_key
            or any(not 33 <= ord(character) <= 126 for character in api_key)
            or len(encoded) > maximum
        ):
            raise ValueError("Invalid API key")
        return api_key

    def _account_root(self, account_id: str) -> Path:
        match = ACCOUNT_ID_RE.fullmatch(str(account_id or ""))
        if (
            match is None
            or len(match.group("number")) > 10
            or int(match.group("number")) > MAX_ACCOUNT_NUMBER
        ):
            raise AgentApiAccountNotFoundError("unknown Agent API account")
        root = self.root / account_id
        if root.parent != self.root:
            raise AgentApiStorageError("account path escaped its store root")
        return root

    def _load_account(self, root: Path) -> AgentApiAccount:
        _private_directory(root, create=False)
        match = ACCOUNT_ID_RE.fullmatch(root.name)
        if (
            match is None
            or len(match.group("number")) > 10
            or int(match.group("number")) > MAX_ACCOUNT_NUMBER
        ):
            raise AgentApiStorageError("invalid Agent API account directory")
        try:
            metadata = json.loads(
                _read_private_file(
                    root / "account.json",
                    maximum=MAX_ACCOUNT_METADATA_BYTES,
                ).decode("utf-8")
            )
        except (UnicodeDecodeError, ValueError, RecursionError) as exc:
            raise AgentApiStorageError("invalid Agent API account metadata") from exc
        if not isinstance(metadata, dict):
            raise AgentApiStorageError("invalid Agent API account metadata")
        required_fields = {
            "version",
            "id",
            "name",
            "group",
            "enabled",
            "api_provider",
            "models",
            "key_fingerprint",
            "created_at",
            "updated_at",
        }
        allowed_fields = required_fields | {"admission_pending", "credential_ref"}
        if (
            not required_fields.issubset(metadata)
            or any(field not in allowed_fields for field in metadata)
        ):
            raise AgentApiStorageError("invalid Agent API account metadata")
        provider = str(metadata.get("api_provider") or "")
        try:
            adapter = self.registry.require(provider)
        except AgentApiUnsupportedProviderError as exc:
            raise AgentApiStorageError(
                "stored Agent API provider is not enabled"
            ) from exc
        if (
            type(metadata.get("version")) is not int
            or metadata.get("version") not in {1, 2}
            or metadata.get("id") != root.name
            or match.group("provider") != provider
        ):
            raise AgentApiStorageError("mismatched Agent API account metadata")
        name = metadata.get("name")
        group = metadata.get("group")
        models = metadata.get("models")
        admission_pending = metadata.get("admission_pending", False)
        enabled = metadata.get("enabled")
        created_at = metadata.get("created_at")
        updated_at = metadata.get("updated_at")
        if (
            not isinstance(name, str)
            or not isinstance(group, str)
            or not isinstance(enabled, bool)
            or not isinstance(admission_pending, bool)
            or not _valid_timestamp(created_at)
            or not _valid_timestamp(updated_at)
            or updated_at < created_at
        ):
            raise AgentApiStorageError("invalid Agent API account metadata")
        try:
            name = _validate_text(
                name,
                field="Account name",
                maximum=MAX_ACCOUNT_NAME_LENGTH,
            )
            group = _validate_text(
                group,
                field="Account group",
                maximum=MAX_GROUP_LENGTH,
            )
            normalized_models = _validated_models(models)
            if not any(normalized_models.values()):
                raise ValueError("empty Agent API model projection")
        except ValueError as exc:
            raise AgentApiStorageError(
                "invalid Agent API account metadata"
            ) from exc

        version = int(metadata["version"])
        credential_ref = metadata.get("credential_ref")
        if version == 2:
            if not isinstance(credential_ref, str) or PLATFORM_CREDENTIAL_REF_RE.fullmatch(credential_ref) is None:
                raise AgentApiStorageError("invalid platform credential reference")
            if (root / "api.key").exists() or (root / "api.key").is_symlink():
                raise AgentApiStorageError("platform credential account contains forbidden local key")
            fingerprint = _key_fingerprint(credential_ref)
            if metadata.get("key_fingerprint") != fingerprint:
                raise AgentApiStorageError("platform credential reference fingerprint mismatch")
        else:
            credential_ref = None
            key = self._decode_api_key(_read_private_file(root / "api.key", maximum=MAX_AGENT_API_KEY_BYTES))
            fingerprint = _key_fingerprint(key)
            if metadata.get("key_fingerprint") != fingerprint:
                raise AgentApiStorageError("Agent API key fingerprint mismatch")
        return AgentApiAccount(
            id=root.name,
            name=name,
            group=group,
            enabled=enabled,
            admission_pending=admission_pending,
            api_provider=provider,
            models=normalized_models,
            key_fingerprint=fingerprint,
            credential_ref=credential_ref,
            root=root,
            endpoints=dict(adapter.endpoints),
        )

    @staticmethod
    def _transaction_account_id(name: str) -> str | None:
        """Extract the account id from a known interrupted transaction path."""

        if not name.startswith("."):
            return None
        value = name[1:]
        if ".remove-" in value:
            account_id, suffix = value.split(".remove-", 1)
            if suffix.isdigit() and ACCOUNT_ID_RE.fullmatch(account_id):
                return account_id
            return None
        if value.endswith(".tmp") and "." in value:
            account_id, random_suffix = value.split(".", 1)
            if (
                random_suffix[:-4]
                and ACCOUNT_ID_RE.fullmatch(account_id)
            ):
                return account_id
        return None

    def _recover_transaction_directories_sync(
        self,
        children: list[Path],
    ) -> set[str]:
        """Finish interrupted add/remove transactions without reusing ids."""

        observed: set[str] = set()
        recovered = False
        for child in children:
            account_id = self._transaction_account_id(child.name)
            if account_id is None:
                continue
            observed.add(account_id)
            try:
                metadata = child.lstat()
            except OSError as exc:
                raise AgentApiStorageError(
                    "unable to inspect Agent API transaction"
                ) from exc
            if (
                not stat.S_ISDIR(metadata.st_mode)
                or metadata.st_uid != os.getuid()
                or stat.S_IMODE(metadata.st_mode) != 0o700
            ):
                raise AgentApiStorageError("unsafe Agent API transaction path")
            try:
                transaction_children = list(child.iterdir())
            except OSError as exc:
                raise AgentApiStorageError(
                    "unable to inspect Agent API transaction"
                ) from exc
            for item in transaction_children:
                try:
                    item_metadata = item.lstat()
                except OSError as exc:
                    raise AgentApiStorageError(
                        "unable to inspect Agent API transaction"
                    ) from exc
                if (
                    not stat.S_ISREG(item_metadata.st_mode)
                    or item_metadata.st_uid != os.getuid()
                    or stat.S_IMODE(item_metadata.st_mode) != 0o600
                ):
                    raise AgentApiStorageError(
                        "unsafe Agent API transaction contents"
                    )
                try:
                    item.unlink()
                except OSError as exc:
                    raise AgentApiStorageError(
                        "unable to recover Agent API transaction"
                    ) from exc
            try:
                child.rmdir()
            except OSError as exc:
                raise AgentApiStorageError(
                    "unable to recover Agent API transaction"
                ) from exc
            recovered = True
        if recovered:
            fsync_directory(self.root)
        return observed

    def _load_high_water_sync(self, observed_ids: set[str]) -> None:
        state_path = self.root / STORE_STATE_NAME
        high_water: dict[str, int] = {}
        state_exists = state_path.exists() or state_path.is_symlink()
        if state_exists:
            try:
                state = json.loads(
                    _read_private_file(
                        state_path,
                        maximum=MAX_ACCOUNT_METADATA_BYTES,
                    ).decode("utf-8")
                )
            except (UnicodeDecodeError, ValueError, RecursionError) as exc:
                raise AgentApiStorageError(
                    "invalid Agent API store state"
                ) from exc
            values = state.get("high_water") if isinstance(state, dict) else None
            if (
                not isinstance(state, dict)
                or set(state) != {"version", "high_water"}
                or type(state.get("version")) is not int
                or state.get("version") != 1
                or not isinstance(values, dict)
            ):
                raise AgentApiStorageError("invalid Agent API store state")
            for provider, number in values.items():
                if (
                    not isinstance(provider, str)
                    or not PROVIDER_RE.fullmatch(provider)
                    or isinstance(number, bool)
                    or not isinstance(number, int)
                    or number < 0
                    or number > MAX_ACCOUNT_NUMBER
                ):
                    raise AgentApiStorageError("invalid Agent API store state")
                high_water[provider] = number

        changed = False
        for account_id in observed_ids:
            match = ACCOUNT_ID_RE.fullmatch(account_id)
            if match is None:
                continue
            provider = match.group("provider")
            raw_number = match.group("number")
            if (
                len(raw_number) > 10
                or (number := int(raw_number)) > MAX_ACCOUNT_NUMBER
            ):
                raise AgentApiStorageError(
                    "Agent API account number is out of range"
                )
            if number > high_water.get(provider, 0):
                high_water[provider] = number
                changed = True
        self._high_water = high_water
        if observed_ids and (changed or not state_exists):
            self._write_high_water_sync()

    def _write_high_water_sync(self) -> None:
        _atomic_private_json(
            self.root / STORE_STATE_NAME,
            {
                "version": 1,
                "high_water": dict(sorted(self._high_water.items())),
            },
        )

    def _load_runtime_tombstone(
        self,
        account: AgentApiAccount,
    ) -> dict[str, Any] | None:
        path = account.root / RUNTIME_UNAVAILABLE_NAME
        if not path.exists() and not path.is_symlink():
            return None
        try:
            value = json.loads(
                _read_private_file(
                    path,
                    maximum=MAX_RUNTIME_UNAVAILABLE_BYTES,
                ).decode("utf-8")
            )
        except (UnicodeDecodeError, ValueError, RecursionError) as exc:
            raise AgentApiStorageError(
                "invalid Agent API runtime tombstone"
            ) from exc
        if not isinstance(value, dict):
            raise AgentApiStorageError("invalid Agent API runtime tombstone")
        reason = value.get("reason")
        marked_at_ns = value.get("marked_at_ns")
        requires_model_refresh = value.get(
            "requires_model_refresh",
            False,
        )
        try:
            safe_reason = _validate_text(
                reason,
                field="Runtime unavailability reason",
                maximum=256,
            )
        except ValueError as exc:
            raise AgentApiStorageError(
                "invalid Agent API runtime tombstone"
            ) from exc
        if (
            set(value)
            != {
                "version",
                "account_id",
                "key_fingerprint",
                "reason",
                "marked_at_ns",
                "requires_model_refresh",
            }
            or type(value.get("version")) is not int
            or value.get("version") != 1
            or value.get("account_id") != account.id
            or value.get("key_fingerprint") != account.key_fingerprint
            or reason != safe_reason
            or isinstance(marked_at_ns, bool)
            or not isinstance(marked_at_ns, int)
            or marked_at_ns <= 0
            or marked_at_ns > MAX_TIMESTAMP_NS
            or not isinstance(requires_model_refresh, bool)
        ):
            raise AgentApiStorageError("invalid Agent API runtime tombstone")
        snapshot = _unavailable_snapshot(account.id, safe_reason)
        snapshot["fetched_at"] = marked_at_ns / 1_000_000_000
        snapshot["_tombstone_revision"] = marked_at_ns
        snapshot["_requires_model_refresh"] = requires_model_refresh
        return snapshot

    def _write_runtime_tombstone_sync(
        self,
        account: AgentApiAccount,
        reason: str,
        *,
        requires_model_refresh: bool = False,
    ) -> dict[str, Any]:
        current = self._runtime_tombstones.get(account.id)
        requires_model_refresh = bool(
            requires_model_refresh
            or (
                current is not None
                and current.get("_requires_model_refresh")
            )
        )
        prior_revision = (
            int(current.get("_tombstone_revision", 0))
            if current is not None
            else 0
        )
        marked_at_ns = max(time.time_ns(), prior_revision + 1)
        _atomic_private_json(
            account.root / RUNTIME_UNAVAILABLE_NAME,
            {
                "version": 1,
                "account_id": account.id,
                "key_fingerprint": account.key_fingerprint,
                "reason": reason,
                "marked_at_ns": marked_at_ns,
                "requires_model_refresh": requires_model_refresh,
            },
        )
        snapshot = _unavailable_snapshot(account.id, reason)
        snapshot["fetched_at"] = marked_at_ns / 1_000_000_000
        snapshot["_tombstone_revision"] = marked_at_ns
        snapshot["_requires_model_refresh"] = requires_model_refresh
        self._runtime_tombstones[account.id] = snapshot
        return snapshot

    def _clear_runtime_tombstone_sync(
        self,
        account: AgentApiAccount,
        *,
        expected_revision: int,
    ) -> bool:
        current = self._runtime_tombstones.get(account.id)
        if (
            current is None
            or current.get("_tombstone_revision") != expected_revision
        ):
            return False
        path = account.root / RUNTIME_UNAVAILABLE_NAME
        try:
            path.unlink()
            fsync_directory(account.root)
        except FileNotFoundError as exc:
            raise AgentApiStorageError(
                "Agent API runtime tombstone disappeared"
            ) from exc
        except OSError as exc:
            raise AgentApiStorageError(
                "unable to clear Agent API runtime tombstone"
            ) from exc
        self._runtime_tombstones.pop(account.id, None)
        self._usage_cache.pop(account.id, None)
        self._usage_cached_at.pop(account.id, None)
        return True

    def _load_last_known_unavailable(
        self,
        account: AgentApiAccount,
    ) -> dict[str, Any] | None:
        """Load a non-sticky fallback for an ordinary usage observation."""

        path = account.root / LAST_KNOWN_UNAVAILABLE_NAME
        if not path.exists() and not path.is_symlink():
            return None
        try:
            value = json.loads(
                _read_private_file(
                    path,
                    maximum=MAX_LAST_KNOWN_UNAVAILABLE_BYTES,
                ).decode("utf-8")
            )
        except (UnicodeDecodeError, ValueError, RecursionError) as exc:
            raise AgentApiStorageError(
                "invalid Agent API last-known usage state"
            ) from exc
        if not isinstance(value, dict):
            raise AgentApiStorageError(
                "invalid Agent API last-known usage state"
            )
        reason = value.get("reason")
        observed_at_ns = value.get("observed_at_ns")
        try:
            safe_reason = _validate_text(
                reason,
                field="Last-known unavailability reason",
                maximum=256,
            )
        except ValueError as exc:
            raise AgentApiStorageError(
                "invalid Agent API last-known usage state"
            ) from exc
        if (
            set(value)
            != {
                "version",
                "account_id",
                "key_fingerprint",
                "reason",
                "observed_at_ns",
            }
            or type(value.get("version")) is not int
            or value.get("version") != 1
            or value.get("account_id") != account.id
            or value.get("key_fingerprint") != account.key_fingerprint
            or reason != safe_reason
            or isinstance(observed_at_ns, bool)
            or not isinstance(observed_at_ns, int)
            or observed_at_ns <= 0
            or observed_at_ns > MAX_TIMESTAMP_NS
        ):
            raise AgentApiStorageError(
                "invalid Agent API last-known usage state"
            )
        snapshot = _unavailable_snapshot(account.id, safe_reason)
        snapshot["fetched_at"] = observed_at_ns / 1_000_000_000
        snapshot["_last_known_revision"] = observed_at_ns
        return snapshot

    def _write_last_known_unavailable_sync(
        self,
        account: AgentApiAccount,
        reason: str,
    ) -> dict[str, Any]:
        try:
            safe_reason = _validate_text(
                str(reason or "unavailable"),
                field="Last-known unavailability reason",
                maximum=256,
            )
        except ValueError:
            safe_reason = "unavailable"
        current = self._last_known_unavailable.get(account.id)
        prior_revision = (
            int(current.get("_last_known_revision", 0))
            if current is not None
            else 0
        )
        observed_at_ns = max(time.time_ns(), prior_revision + 1)
        _atomic_private_json(
            account.root / LAST_KNOWN_UNAVAILABLE_NAME,
            {
                "version": 1,
                "account_id": account.id,
                "key_fingerprint": account.key_fingerprint,
                "reason": safe_reason,
                "observed_at_ns": observed_at_ns,
            },
        )
        snapshot = _unavailable_snapshot(account.id, safe_reason)
        snapshot["fetched_at"] = observed_at_ns / 1_000_000_000
        snapshot["_last_known_revision"] = observed_at_ns
        self._last_known_unavailable[account.id] = snapshot
        return snapshot

    def _clear_last_known_unavailable_sync(
        self,
        account: AgentApiAccount,
    ) -> None:
        current = self._last_known_unavailable.get(account.id)
        if current is None:
            return
        path = account.root / LAST_KNOWN_UNAVAILABLE_NAME
        try:
            path.unlink()
            fsync_directory(account.root)
        except FileNotFoundError as exc:
            raise AgentApiStorageError(
                "Agent API last-known usage state disappeared"
            ) from exc
        except OSError as exc:
            raise AgentApiStorageError(
                "unable to clear Agent API last-known usage state"
            ) from exc
        self._last_known_unavailable.pop(account.id, None)

    def _clear_admission_pending_sync(
        self,
        account: AgentApiAccount,
    ) -> None:
        if not account.admission_pending:
            return
        metadata_path = account.root / "account.json"
        try:
            metadata = json.loads(
                _read_private_file(
                    metadata_path,
                    maximum=MAX_ACCOUNT_METADATA_BYTES,
                ).decode("utf-8")
            )
        except (UnicodeDecodeError, ValueError, RecursionError) as exc:
            raise AgentApiStorageError(
                "invalid Agent API metadata"
            ) from exc
        if (
            not isinstance(metadata, dict)
            or metadata.get("id") != account.id
            or metadata.get("key_fingerprint") != account.key_fingerprint
            or metadata.get("admission_pending") is not True
        ):
            raise AgentApiStorageError(
                "Agent API admission state changed unexpectedly"
            )
        metadata["admission_pending"] = False
        metadata["updated_at"] = time.time()
        try:
            payload = _metadata_bytes(metadata)
        except ValueError as exc:
            raise AgentApiStorageError(
                "invalid Agent API metadata"
            ) from exc
        _atomic_private_write(metadata_path, payload)

    def _reload_sync(self) -> None:
        _private_directory(self.root, create=False)
        loaded: dict[str, AgentApiAccount] = {}
        try:
            children = list(self.root.iterdir())
        except OSError as exc:
            raise AgentApiStorageError("unable to enumerate Agent API accounts") from exc
        observed_ids = {
            child.name
            for child in children
            if ACCOUNT_ID_RE.fullmatch(child.name)
        }
        observed_ids.update(
            account_id
            for child in children
            if (account_id := self._transaction_account_id(child.name))
            is not None
        )
        # Persist every observed identity before completing deletion of an
        # interrupted add/remove transaction. If the Manager crashes after
        # cleanup, the next startup still cannot reuse that old id.
        self._load_high_water_sync(observed_ids)
        self._recover_transaction_directories_sync(children)
        children = list(self.root.iterdir())
        for child in children:
            if child.name.startswith("."):
                continue
            if not ACCOUNT_ID_RE.fullmatch(child.name):
                raise AgentApiStorageError("unexpected Agent API account path")
            account = self._load_account(child)
            loaded[account.id] = account
        identities: set[tuple[str, str]] = set()
        for account in loaded.values():
            identity = (account.api_provider, account.key_fingerprint)
            if identity in identities:
                raise AgentApiStorageError(
                    "duplicate Agent API provider key fingerprint"
                )
            identities.add(identity)
        self._accounts = loaded
        self._runtime_tombstones = {
            account.id: tombstone
            for account in loaded.values()
            if (tombstone := self._load_runtime_tombstone(account)) is not None
        }
        self._last_known_unavailable = {
            account.id: snapshot
            for account in loaded.values()
            if (
                snapshot := self._load_last_known_unavailable(account)
            ) is not None
        }
        self._usage_cache = {
            account_id: snapshot
            for account_id, snapshot in self._usage_cache.items()
            if account_id in loaded
        }
        self._usage_cached_at = {
            account_id: timestamp
            for account_id, timestamp in self._usage_cached_at.items()
            if account_id in loaded
        }

    @staticmethod
    def _decode_api_key(payload: bytes) -> str:
        try:
            key = payload.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise AgentApiStorageError("invalid Agent API key encoding") from exc
        if (
            not key
            or key.strip() != key
            or any(not 33 <= ord(character) <= 126 for character in key)
        ):
            raise AgentApiStorageError("invalid Agent API key")
        return key

    def _require_sync(self, account_id: str) -> AgentApiAccount:
        self._account_root(account_id)
        account = self._accounts.get(account_id)
        if account is None:
            raise AgentApiAccountNotFoundError("unknown Agent API account")
        return account

    def _next_id(
        self,
        provider: str,
        *,
        excluded_ids: set[str] | frozenset[str] = frozenset(),
    ) -> str:
        number = self._high_water.get(provider, 0) + 1
        for excluded_id in excluded_ids:
            match = ACCOUNT_ID_RE.fullmatch(excluded_id)
            if match is not None and match.group("provider") == provider:
                raw_number = match.group("number")
                if len(raw_number) > 10:
                    raise ValueError("excluded Agent API account id is too large")
                excluded_number = int(raw_number)
                if excluded_number > MAX_ACCOUNT_NUMBER:
                    raise ValueError("excluded Agent API account id is too large")
                number = max(number, excluded_number + 1)
        if number > MAX_ACCOUNT_NUMBER:
            raise AgentApiStorageError("Agent API account id space is exhausted")
        while (
            f"{provider}-{number}" in self._accounts
            or f"{provider}-{number}" in excluded_ids
        ):
            number += 1
            if number > MAX_ACCOUNT_NUMBER:
                raise AgentApiStorageError(
                    "Agent API account id space is exhausted"
                )
        self._high_water[provider] = number
        # Commit identity allocation before publishing a credential directory.
        # A failed add may burn an id, but an old EIP/lease/key can never be
        # silently inherited by a future account after a crash or deletion.
        self._write_high_water_sync()
        return f"{provider}-{number}"

    @staticmethod
    def _metadata(
        *,
        account_id: str,
        provider: str,
        name: str,
        group: str,
        enabled: bool,
        admission_pending: bool,
        models: dict[str, list[str]],
        fingerprint: str,
        credential_ref: str | None = None,
        created_at: float | None = None,
    ) -> dict[str, Any]:
        now = time.time()
        return {
            "version": 2 if credential_ref else 1,
            "id": account_id,
            "name": name,
            "group": group,
            "enabled": enabled,
            "admission_pending": admission_pending,
            "api_provider": provider,
            "models": models,
            "key_fingerprint": fingerprint,
            **({"credential_ref": credential_ref} if credential_ref else {}),
            "created_at": created_at or now,
            "updated_at": now,
        }

    async def list(self) -> list[AgentApiAccount]:
        async with self._lock:
            self._reload_sync()
            provider_order = {
                provider: index
                for index, provider in enumerate(self.registry.providers)
            }
            return sorted(
                self._accounts.values(),
                key=lambda account: (
                    provider_order[account.api_provider],
                    int(ACCOUNT_ID_RE.fullmatch(account.id).group("number")),  # type: ignore[union-attr]
                ),
            )

    async def get(self, account_id: str) -> AgentApiAccount | None:
        async with self._lock:
            self._reload_sync()
            self._account_root(account_id)
            return self._accounts.get(account_id)

    async def _operation_lock_for_existing(
        self,
        account_id: str,
    ) -> asyncio.Lock:
        """Allocate a per-account lock only after the id is proven to exist."""

        async with self._lock:
            self._reload_sync()
            self._require_sync(account_id)
            return self._usage_fetch_locks.setdefault(
                account_id,
                asyncio.Lock(),
            )

    async def add(
        self,
        provider: str,
        name: str,
        api_key: str,
        group: str = "standard",
        *,
        enabled: bool = True,
        excluded_ids: set[str] | frozenset[str] = frozenset(),
    ) -> AgentApiAccount:
        adapter = self.registry.require(provider)
        normalized_provider = adapter.provider
        clean_name = _validate_text(
            name,
            field="Account name",
            maximum=MAX_ACCOUNT_NAME_LENGTH,
        )
        clean_group = _validate_text(
            group,
            field="Account group",
            maximum=MAX_GROUP_LENGTH,
        )
        clean_key = self._validate_api_key(api_key, MAX_AGENT_API_KEY_BYTES)
        # No local account becomes visible unless the key has passed the remote
        # model probe and supports at least one known agent type.
        models = await adapter.probe_models(clean_key)
        if not any(models.get(agent_type) for agent_type in ("claude", "codex")):
            raise AgentApiUpstreamError("no_supported_models")
        try:
            normalized_models = _validated_models(models)
        except ValueError as exc:
            raise AgentApiUpstreamError("invalid_models_response") from exc
        fingerprint = _key_fingerprint(clean_key)
        async with self._lock:
            self._reload_sync()
            if any(
                account.api_provider == normalized_provider
                and account.key_fingerprint == fingerprint
                for account in self._accounts.values()
            ):
                raise AgentApiDuplicateKeyError(
                    "Agent API provider key is already registered"
                )
            account_id = self._next_id(
                normalized_provider,
                excluded_ids=excluded_ids,
            )
            target = self._account_root(account_id)
            temporary = Path(
                tempfile.mkdtemp(
                    prefix=f".{account_id}.",
                    suffix=".tmp",
                    dir=self.root,
                )
            )
            os.chmod(temporary, 0o700)
            try:
                _private_directory(temporary, create=False)
                _atomic_private_write(temporary / "api.key", clean_key.encode("utf-8"))
                metadata = self._metadata(
                    account_id=account_id,
                    provider=normalized_provider,
                    name=clean_name,
                    group=clean_group,
                    enabled=bool(enabled),
                    admission_pending=True,
                    models=normalized_models,
                    fingerprint=fingerprint,
                )
                try:
                    metadata_payload = _metadata_bytes(metadata)
                except ValueError as exc:
                    raise AgentApiUpstreamError(
                        "invalid_models_response"
                    ) from exc
                _atomic_private_write(
                    temporary / "account.json",
                    metadata_payload,
                )
                fsync_directory(temporary)
                if target.exists() or target.is_symlink():
                    raise AgentApiStorageError("Agent API account destination exists")
                os.rename(temporary, target)
                fsync_directory(self.root)
                try:
                    self._load_account(target)
                except Exception:
                    # The directory was created exclusively by this call. If
                    # it cannot be read under the same safety rules, remove it
                    # before it can poison all future list/startup operations.
                    for name in ("api.key", "account.json"):
                        (target / name).unlink(missing_ok=True)
                    target.rmdir()
                    fsync_directory(self.root)
                    raise
            except AgentApiError:
                raise
            except OSError as exc:
                raise AgentApiStorageError("unable to publish Agent API account") from exc
            finally:
                if temporary.exists() and not temporary.is_symlink():
                    for child in temporary.iterdir():
                        child.unlink()
                    temporary.rmdir()
            self._reload_sync()
            return self._require_sync(account_id)

    async def add_reference(
        self,
        provider: str,
        name: str,
        credential_ref: str,
        group: str = "standard",
        *,
        enabled: bool = True,
        excluded_ids: set[str] | frozenset[str] = frozenset(),
    ) -> AgentApiAccount:
        """Register a platform-owned secret reference without persisting its key."""
        adapter = self.registry.require(provider)
        clean_name = _validate_text(name, field="Account name", maximum=MAX_ACCOUNT_NAME_LENGTH)
        clean_group = _validate_text(group, field="Account group", maximum=MAX_GROUP_LENGTH)
        clean_ref = _validate_platform_credential_ref(credential_ref)
        clean_key = self._validate_api_key(self._credential_resolver(clean_ref), MAX_AGENT_API_KEY_BYTES)
        models = await adapter.probe_models(clean_key)
        if not any(models.get(agent_type) for agent_type in ("claude", "codex")):
            raise AgentApiUpstreamError("no_supported_models")
        normalized_models = _validated_models(models)
        fingerprint = _key_fingerprint(clean_ref)
        async with self._lock:
            self._reload_sync()
            if any(account.credential_ref == clean_ref for account in self._accounts.values()):
                raise AgentApiDuplicateKeyError("platform credential is already registered")
            account_id = self._next_id(adapter.provider, excluded_ids=excluded_ids)
            target = self._account_root(account_id)
            temporary = Path(tempfile.mkdtemp(prefix=f".{account_id}.", suffix=".tmp", dir=self.root))
            os.chmod(temporary, 0o700)
            try:
                metadata = self._metadata(
                    account_id=account_id, provider=adapter.provider, name=clean_name,
                    group=clean_group, enabled=bool(enabled), admission_pending=True,
                    models=normalized_models, fingerprint=fingerprint, credential_ref=clean_ref,
                )
                _atomic_private_write(temporary / "account.json", _metadata_bytes(metadata))
                fsync_directory(temporary)
                if target.exists() or target.is_symlink():
                    raise AgentApiStorageError("Agent API account destination exists")
                os.rename(temporary, target)
                fsync_directory(self.root)
                self._load_account(target)
            except AgentApiError:
                raise
            except OSError as exc:
                raise AgentApiStorageError("unable to publish platform credential account") from exc
            finally:
                if temporary.exists() and not temporary.is_symlink():
                    for child in temporary.iterdir():
                        child.unlink()
                    temporary.rmdir()
            self._reload_sync()
            return self._require_sync(account_id)

    async def refresh(self, account_id: str) -> AgentApiAccount:
        operation_lock = await self._operation_lock_for_existing(
            account_id,
        )
        async with operation_lock:
            async with self._lock:
                self._reload_sync()
                account = self._require_sync(account_id)
                adapter = self.registry.require(account.api_provider)
                key = self.read_api_key(account_id)
                fingerprint = account.key_fingerprint

            try:
                models = await adapter.probe_models(key)
                if not any(
                    models.get(agent_type)
                    for agent_type in ("claude", "codex")
                ):
                    raise AgentApiUpstreamError("no_supported_models")
                try:
                    normalized_models = _validated_models(models)
                except ValueError as exc:
                    raise AgentApiUpstreamError(
                        "invalid_models_response"
                    ) from exc
            except AgentApiUpstreamError as exc:
                if (
                    exc.status_code in {401, 403}
                    or exc.code in {
                        "invalid_models_response",
                        "no_supported_models",
                    }
                ):
                    async with self._lock:
                        self._reload_sync()
                        current = self._require_sync(account_id)
                        if current.key_fingerprint == fingerprint:
                            snapshot = self._write_runtime_tombstone_sync(
                                current,
                                exc.code,
                                requires_model_refresh=True,
                            )
                            self._usage_cache[account_id] = dict(snapshot)
                            self._usage_cached_at[account_id] = time.time()
                raise

            async with self._lock:
                self._reload_sync()
                current = self._require_sync(account_id)
                if current.key_fingerprint != fingerprint:
                    raise AgentApiStorageError(
                        "Agent API credentials changed during refresh"
                    )
                metadata_path = current.root / "account.json"
                try:
                    metadata = json.loads(
                        _read_private_file(
                            metadata_path,
                            maximum=MAX_ACCOUNT_METADATA_BYTES,
                        ).decode("utf-8")
                    )
                except (UnicodeDecodeError, ValueError, RecursionError) as exc:
                    raise AgentApiStorageError(
                        "invalid Agent API metadata"
                    ) from exc
                metadata["models"] = normalized_models
                metadata["updated_at"] = time.time()
                try:
                    metadata_payload = _metadata_bytes(metadata)
                except ValueError as exc:
                    raise AgentApiUpstreamError(
                        "invalid_models_response"
                    ) from exc
                _atomic_private_write(metadata_path, metadata_payload)
                self._reload_sync()
                return self._require_sync(account_id)

    def read_api_key(self, account_id: str) -> str:
        """Read a key for protected worker delivery without storing it on the model."""

        account = self._require_sync(account_id)
        if not account.enabled:
            raise AgentApiError("Agent API account is disabled")
        if account.credential_ref:
            key = self._validate_api_key(self._credential_resolver(account.credential_ref), MAX_AGENT_API_KEY_BYTES)
        else:
            key = self._decode_api_key(_read_private_file(account.root / "api.key", maximum=MAX_AGENT_API_KEY_BYTES))
            if _key_fingerprint(key) != account.key_fingerprint:
                raise AgentApiStorageError("Agent API key fingerprint mismatch")
        return key

    async def fetch_usage(
        self,
        account_id: str,
        force: bool = False,
        *,
        allow_model_tombstone_clear: bool = False,
    ) -> dict[str, Any]:
        # A provider request must not hold the store-wide mutation lock. Besides
        # making list refreshes accidentally serial, that used to block all
        # account claims and CRUD for up to N×the provider timeout.
        fetch_lock = await self._operation_lock_for_existing(
            account_id,
        )
        async with fetch_lock:
            async with self._lock:
                self._reload_sync()
                account = self._require_sync(account_id)
                if not account.enabled:
                    return _unavailable_snapshot(account_id, "disabled")
                now = time.time()
                cached_previous = self._usage_cache.get(account_id)
                durable_previous = self._last_known_unavailable.get(account_id)
                previous = cached_previous or (
                    _public_usage_snapshot(durable_previous)
                    if durable_previous is not None
                    else None
                )
                starting_tombstone = self._runtime_tombstones.get(account_id)
                starting_tombstone_revision = (
                    starting_tombstone.get("_tombstone_revision")
                    if starting_tombstone is not None
                    else None
                )
                if starting_tombstone is not None and not force:
                    return _public_usage_snapshot(starting_tombstone)
                if (
                    not force
                    and cached_previous is not None
                    and now - self._usage_cached_at.get(account_id, 0.0)
                    < self._quota_cache_ttl
                ):
                    return dict(cached_previous)
                adapter = self.registry.require(account.api_provider)
                try:
                    api_key = self.read_api_key(account_id)
                except AgentApiStorageError:
                    snapshot = _unavailable_snapshot(
                        account_id,
                        "invalid_local_credentials",
                    )
                    self._usage_cache[account_id] = dict(snapshot)
                    self._usage_cached_at[account_id] = now
                    return dict(snapshot)
                provider = account.api_provider
                fingerprint = account.key_fingerprint
                # Object identity is a cheap per-account revision: every cache
                # writer replaces the dict. A runtime 401 arriving during this
                # network call must not be overwritten by its stale response.
                previous_object = cached_previous

            provider_succeeded = False
            try:
                snapshot = await adapter.fetch_usage(
                    account_id,
                    api_key,
                )
                provider_succeeded = True
            except AgentApiUpstreamError as exc:
                if (
                    exc.status_code in {401, 403}
                    or exc.code in _FAIL_CLOSED_USAGE_ERROR_CODES
                ):
                    snapshot = _unavailable_snapshot(account_id, exc.code)
                else:
                    snapshot = _unknown_snapshot(account_id, exc.code, previous)
            except AgentApiStorageError:
                snapshot = _unavailable_snapshot(
                    account_id,
                    "invalid_local_credentials",
                )

            async with self._lock:
                self._reload_sync()
                current = self._require_sync(account_id)
                if not current.enabled:
                    return _unavailable_snapshot(account_id, "disabled")
                if (
                    current.api_provider != provider
                    or current.key_fingerprint != fingerprint
                ):
                    changed = _unavailable_snapshot(
                        account_id,
                        "credentials_changed_during_usage_check",
                    )
                    self._usage_cache[account_id] = dict(changed)
                    self._usage_cached_at[account_id] = time.time()
                    return changed
                current_tombstone = self._runtime_tombstones.get(account_id)
                current_tombstone_revision = (
                    current_tombstone.get("_tombstone_revision")
                    if current_tombstone is not None
                    else None
                )
                if current_tombstone_revision != starting_tombstone_revision:
                    return _public_usage_snapshot(
                        current_tombstone or _unavailable_snapshot(
                        account_id,
                        "runtime_credential_state_changed",
                    ))
                if current_tombstone is not None:
                    requires_model_refresh = bool(
                        current_tombstone.get("_requires_model_refresh")
                    )
                    if (
                        not (force and provider_succeeded)
                        or (
                            requires_model_refresh
                            and not allow_model_tombstone_clear
                        )
                    ):
                        return _public_usage_snapshot(current_tombstone)
                    tombstone_cleared = self._clear_runtime_tombstone_sync(
                        current,
                        expected_revision=int(current_tombstone_revision),
                    )
                else:
                    tombstone_cleared = False
                current_snapshot = self._usage_cache.get(account_id)
                if (
                    not tombstone_cleared
                    and current_snapshot is not previous_object
                ):
                    return _public_usage_snapshot(
                        current_snapshot or _unknown_snapshot(
                        account_id,
                        "usage_state_changed",
                        None,
                    ))
                if (
                    current.admission_pending
                    and provider_succeeded
                    and bool(snapshot.get("known"))
                    and snapshot.get("available") is True
                ):
                    self._clear_admission_pending_sync(current)
                    self._reload_sync()
                    current = self._require_sync(account_id)
                if (
                    provider_succeeded
                    and bool(snapshot.get("known"))
                    and snapshot.get("available") is True
                ):
                    self._clear_last_known_unavailable_sync(current)
                elif (
                    bool(snapshot.get("known"))
                    and snapshot.get("available") is False
                ):
                    self._write_last_known_unavailable_sync(
                        current,
                        str(snapshot.get("reason") or "unavailable"),
                    )
                if (
                    current.admission_pending
                    and snapshot.get("available") is not False
                ):
                    snapshot = _unavailable_snapshot(
                        account_id,
                        "initial_usage_pending",
                    )
                self._usage_cache[account_id] = dict(snapshot)
                self._usage_cached_at[account_id] = time.time()
                return dict(snapshot)

    def availability_decision(self, account_id: str) -> dict[str, Any]:
        account = self._accounts.get(account_id)
        if account is None or not account.enabled:
            return {"available": False, "known": True, "reason": "disabled"}
        tombstone = self._runtime_tombstones.get(account_id)
        snapshot = (
            tombstone
            or self._usage_cache.get(account_id)
            or self._last_known_unavailable.get(account_id)
        )
        if snapshot is None:
            if account.admission_pending:
                return {
                    "available": False,
                    "known": True,
                    "reason": "initial_usage_pending",
                }
            return {"available": True, "known": False, "reason": "not_fetched"}
        if (
            not bool(snapshot.get("known"))
            and snapshot.get("last_known_available") is False
        ):
            return {
                "available": False,
                "known": True,
                "reason": str(
                    snapshot.get("last_known_reason")
                    or snapshot.get("reason")
                    or "last_known_unavailable"
                ),
            }
        if bool(snapshot.get("known")) and snapshot.get("available") is False:
            return {
                "available": False,
                "known": True,
                "reason": str(snapshot.get("reason") or "unavailable"),
            }
        if account.admission_pending:
            return {
                "available": False,
                "known": True,
                "reason": "initial_usage_pending",
            }
        return {
            "available": bool(snapshot.get("available", True)),
            "known": bool(snapshot.get("known", False)),
            "reason": str(snapshot.get("reason") or "unknown"),
        }

    def usage_snapshot(self, account_id: str) -> dict[str, Any]:
        """Return cached public usage without making an upstream request."""

        account = self._require_sync(account_id)
        tombstone = self._runtime_tombstones.get(account_id)
        snapshot = (
            tombstone
            or self._usage_cache.get(account_id)
            or self._last_known_unavailable.get(account_id)
        )
        if snapshot is not None:
            return _public_usage_snapshot(snapshot)
        if account.admission_pending:
            return _unavailable_snapshot(
                account_id,
                "initial_usage_pending",
            )
        return {
            "account_id": account_id,
            "state": "unknown",
            "status": "unknown",
            "stale": False,
            "available": True,
            "known": False,
            "reason": "not_fetched",
        }

    async def mark_runtime_unavailable(
        self,
        account_id: str,
        reason: str,
    ) -> None:
        """Bench a key rejected during a proven managed CLI run.

        The account remains unavailable until an explicit usage refresh
        succeeds, preventing API-first allocation from repeatedly selecting a
        key that was revoked after its previous quota probe.
        """

        try:
            safe_reason = _validate_text(
                str(reason or "runtime_auth_failure"),
                field="Runtime unavailability reason",
                maximum=256,
            )
        except ValueError:
            safe_reason = "runtime_auth_failure"
        async with self._lock:
            self._reload_sync()
            account = self._require_sync(account_id)
            snapshot = self._write_runtime_tombstone_sync(
                account,
                safe_reason,
            )
            self._usage_cache[account_id] = dict(snapshot)
            self._usage_cached_at[account_id] = time.time()

    async def mark_runtime_quota_unavailable(
        self,
        account_id: str,
        reason: str,
    ) -> None:
        """Persist a runtime quota/rate rejection as a re-probeable decision.

        Unlike an invalid-key tombstone, quota exhaustion can recover.  Keep it
        unavailable across Job release and Manager restart, but let the normal
        provider usage probe clear it once CloudRouter reports the key active.
        """

        try:
            safe_reason = _validate_text(
                str(reason or "runtime_rate_limited"),
                field="Runtime quota unavailability reason",
                maximum=256,
            )
        except ValueError:
            safe_reason = "runtime_rate_limited"
        async with self._lock:
            self._reload_sync()
            account = self._require_sync(account_id)
            snapshot = self._write_last_known_unavailable_sync(
                account,
                safe_reason,
            )
            # A sticky invalid-key/model tombstone always remains strongest.
            if account_id not in self._runtime_tombstones:
                self._usage_cache[account_id] = dict(snapshot)
                self._usage_cached_at[account_id] = time.time()

    async def remove(self, account_id: str) -> bool:
        """Remove an account using a same-directory tombstone rename.

        The initial REST release intentionally does not expose this operation;
        callers that do use the core primitive still receive durable deletion.
        """

        async with self._lock:
            self._reload_sync()
            account = self._accounts.get(account_id)
            if account is None:
                self._account_root(account_id)
                return False
            # Validate both files before making the directory unreachable.
            _read_private_file(
                account.root / "account.json",
                maximum=MAX_ACCOUNT_METADATA_BYTES,
            )
            _read_private_file(
                account.root / "api.key",
                maximum=MAX_AGENT_API_KEY_BYTES,
            )
            runtime_tombstone = account.root / RUNTIME_UNAVAILABLE_NAME
            if runtime_tombstone.exists() or runtime_tombstone.is_symlink():
                _read_private_file(
                    runtime_tombstone,
                    maximum=MAX_RUNTIME_UNAVAILABLE_BYTES,
                )
            last_known = account.root / LAST_KNOWN_UNAVAILABLE_NAME
            if last_known.exists() or last_known.is_symlink():
                _read_private_file(
                    last_known,
                    maximum=MAX_LAST_KNOWN_UNAVAILABLE_BYTES,
                )
            tombstone = self.root / f".{account.id}.remove-{os.getpid()}"
            if tombstone.exists() or tombstone.is_symlink():
                raise AgentApiStorageError("Agent API removal tombstone exists")
            try:
                os.rename(account.root, tombstone)
                fsync_directory(self.root)
                (tombstone / "api.key").unlink()
                (tombstone / "account.json").unlink()
                (tombstone / RUNTIME_UNAVAILABLE_NAME).unlink(missing_ok=True)
                (tombstone / LAST_KNOWN_UNAVAILABLE_NAME).unlink(
                    missing_ok=True
                )
                tombstone.rmdir()
                fsync_directory(self.root)
            except OSError as exc:
                raise AgentApiStorageError("unable to remove Agent API account") from exc
            self._usage_cache.pop(account_id, None)
            self._usage_cached_at.pop(account_id, None)
            self._last_known_unavailable.pop(account_id, None)
            operation_lock = self._usage_fetch_locks.get(account_id)
            if operation_lock is None or not operation_lock.locked():
                self._usage_fetch_locks.pop(account_id, None)
            self._reload_sync()
            return True
