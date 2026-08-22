"""Immutable release evidence shared by Manager startup and health.

The release manifest is intentionally small and non-secret.  Its hashes are
computed from canonical JSON so a release can be checked independently of
whitespace, object ordering, or the Python runtime used to verify it.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from pathlib import Path
from typing import Any, Mapping

RELEASE_MANIFEST_SCHEMA = 1
MANAGER_STATE_SCHEMA = "v1"
WORKER_PROFILE_SCHEMA = 1
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_SHA256_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
_COMMIT = re.compile(r"^[0-9a-f]{40}$")
_SCHEMA_NAME = re.compile(r"^v[1-9][0-9]{0,8}$")
_SECRET_NAME = re.compile(
    r"(?:secret|token|password|credential|api[_-]?key)", re.IGNORECASE
)

_MANIFEST_KEYS = frozenset(
    {
        "schema_version",
        "product",
        "release_revision",
        "source_commit",
        "archive_sha256",
        "manager_state_schema",
        "worker_profile",
        "worker_profile_digest",
        "release_digest",
    }
)
_WORKER_PROFILE_KEYS = frozenset(
    {
        "schema_version",
        "profiles",
        "runtime_source",
        "bootstrap_source",
        "dependency_lock",
        "ami_id",
        "region",
        "aws_account_id",
        "release_revision",
    }
)


class ReleaseEvidenceError(ValueError):
    """Raised when an immutable release manifest is absent or invalid."""


def canonical_json(value: Any) -> bytes:
    """Serialize JSON deterministically for hashing and comparison."""

    try:
        return json.dumps(
            value,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ReleaseEvidenceError("release manifest is not canonical JSON") from exc


def sha256_hex(value: Any) -> str:
    return hashlib.sha256(canonical_json(value)).hexdigest()


def compute_worker_profile_digest(worker_profile: Mapping[str, Any]) -> str:
    """Return the stable digest for one validated worker profile object."""

    return f"sha256:{sha256_hex(worker_profile)}"


def compute_release_digest(manifest: Mapping[str, Any]) -> str:
    """Return the stable digest for a manifest without its self-digest."""

    unsigned = dict(manifest)
    unsigned.pop("release_digest", None)
    return f"sha256:{sha256_hex(unsigned)}"


# Descriptive aliases keep callers from depending on the generic hash helper.
generate_worker_profile_digest = compute_worker_profile_digest
generate_release_digest = compute_release_digest


def _reject_secret_names(value: Any, path: str = "manifest") -> None:
    if isinstance(value, Mapping):
        for key, child in value.items():
            if not isinstance(key, str) or _SECRET_NAME.search(key):
                raise ReleaseEvidenceError(f"release manifest contains a forbidden field at {path}")
            _reject_secret_names(child, f"{path}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _reject_secret_names(child, f"{path}[{index}]")


def _require_string(raw: Mapping[str, Any], key: str) -> str:
    value = raw.get(key)
    if not isinstance(value, str) or not value or value.strip() != value:
        raise ReleaseEvidenceError(f"release manifest field {key!r} must be a non-empty string")
    return value


def _validate_worker_profile(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict) or set(raw) != _WORKER_PROFILE_KEYS:
        raise ReleaseEvidenceError("release manifest worker_profile fields are invalid")
    if raw.get("schema_version") != WORKER_PROFILE_SCHEMA:
        raise ReleaseEvidenceError("unsupported worker profile schema")
    profiles = raw.get("profiles")
    if not isinstance(profiles, list) or not profiles:
        raise ReleaseEvidenceError("worker profile list is empty")
    normalized_profiles: list[dict[str, str]] = []
    for profile in profiles:
        if not isinstance(profile, dict) or set(profile) != {"name", "kind"}:
            raise ReleaseEvidenceError("worker profile entries are invalid")
        name = profile.get("name")
        kind = profile.get("kind")
        if (
            not isinstance(name, str)
            or not name
            or not isinstance(kind, str)
            or not kind
            or name.strip() != name
            or kind.strip() != kind
        ):
            raise ReleaseEvidenceError("worker profile entries must be non-empty strings")
        normalized_profiles.append({"name": name, "kind": kind})
    if len({item["name"] for item in normalized_profiles}) != len(normalized_profiles):
        raise ReleaseEvidenceError("worker profile names must be unique")
    result = dict(raw)
    result["profiles"] = normalized_profiles
    for key in ("runtime_source", "bootstrap_source", "dependency_lock"):
        _require_string(result, key)
    ami_id = _require_string(result, "ami_id")
    if not re.fullmatch(r"ami-[0-9a-f]{17}", ami_id):
        raise ReleaseEvidenceError("worker profile ami_id is invalid")
    region = _require_string(result, "region")
    if not re.fullmatch(r"[a-z]{2}(?:-gov)?-[a-z]+-\d", region):
        raise ReleaseEvidenceError("worker profile region is invalid")
    account_id = _require_string(result, "aws_account_id")
    if not re.fullmatch(r"\d{12}", account_id):
        raise ReleaseEvidenceError("worker profile aws_account_id is invalid")
    revision = _require_string(result, "release_revision")
    if _COMMIT.fullmatch(revision) is None:
        raise ReleaseEvidenceError("worker profile release_revision is invalid")
    return result


def validate_release_manifest(raw: Any) -> dict[str, Any]:
    """Validate and return a normalized, immutable release evidence object."""

    if not isinstance(raw, dict) or set(raw) != _MANIFEST_KEYS:
        raise ReleaseEvidenceError("release manifest fields are invalid")
    _reject_secret_names(raw)
    if raw.get("schema_version") != RELEASE_MANIFEST_SCHEMA:
        raise ReleaseEvidenceError("unsupported release manifest schema")
    if _require_string(raw, "product") != "elastic-agent":
        raise ReleaseEvidenceError("release manifest product is invalid")
    source_commit = _require_string(raw, "source_commit")
    if _COMMIT.fullmatch(source_commit) is None:
        raise ReleaseEvidenceError("release manifest source_commit is invalid")
    archive_sha256 = _require_string(raw, "archive_sha256")
    if _SHA256.fullmatch(archive_sha256) is None:
        raise ReleaseEvidenceError("release manifest archive_sha256 is invalid")
    release_revision = _require_string(raw, "release_revision")
    if _COMMIT.fullmatch(release_revision) is None:
        raise ReleaseEvidenceError("release manifest release_revision is invalid")
    if release_revision != source_commit:
        raise ReleaseEvidenceError("release revision does not match source commit")
    manager_state_schema = _require_string(raw, "manager_state_schema")
    if not _SCHEMA_NAME.fullmatch(manager_state_schema):
        raise ReleaseEvidenceError("release manifest manager_state_schema is invalid")
    if manager_state_schema != MANAGER_STATE_SCHEMA:
        raise ReleaseEvidenceError("manager state schema does not match this runtime")
    worker_profile = _validate_worker_profile(raw["worker_profile"])
    if worker_profile["release_revision"] != release_revision:
        raise ReleaseEvidenceError("worker profile release revision does not match release")
    worker_profile_digest = _require_string(raw, "worker_profile_digest")
    if _SHA256_DIGEST.fullmatch(worker_profile_digest) is None:
        raise ReleaseEvidenceError("worker_profile_digest is invalid")
    if worker_profile_digest != compute_worker_profile_digest(worker_profile):
        raise ReleaseEvidenceError("worker_profile_digest does not match worker_profile")
    release_digest = _require_string(raw, "release_digest")
    if _SHA256_DIGEST.fullmatch(release_digest) is None:
        raise ReleaseEvidenceError("release_digest is invalid")
    if release_digest != compute_release_digest(raw):
        raise ReleaseEvidenceError("release_digest does not match release manifest")
    return dict(raw)


def validate_deployment_context(
    manifest: Mapping[str, Any],
    context: Mapping[str, str],
) -> None:
    """Require deployment settings to match the immutable worker profile."""

    profile = manifest.get("worker_profile")
    if not isinstance(profile, Mapping):
        raise ReleaseEvidenceError("release manifest worker profile is unavailable")
    expected = {
        "ami_id": profile.get("ami_id"),
        "region": profile.get("region"),
        "aws_account_id": profile.get("aws_account_id"),
        "release_revision": manifest.get("release_revision"),
    }
    for key, expected_value in expected.items():
        actual_value = context.get(key)
        if not isinstance(actual_value, str) or actual_value != expected_value:
            raise ReleaseEvidenceError(f"deployment {key} does not match release manifest")


def default_manifest_path() -> Path:
    configured = os.environ.get("ELASTIC_AGENT_RELEASE_MANIFEST", "").strip()
    if configured:
        path = Path(configured).expanduser()
        if not path.is_absolute():
            raise ReleaseEvidenceError("ELASTIC_AGENT_RELEASE_MANIFEST must be absolute")
        return path
    # Source checkout and production release both keep deploy/ beside src/.
    return Path(__file__).resolve().parents[3] / "deploy" / "release-manifest.json"


def load_release_manifest(path: str | Path | None = None) -> dict[str, Any]:
    manifest_path = Path(path).expanduser() if path is not None else default_manifest_path()
    if not manifest_path.is_absolute():
        raise ReleaseEvidenceError("release manifest path must be absolute")
    try:
        if manifest_path.stat().st_size > 1_048_576:
            raise ReleaseEvidenceError("release manifest exceeds size limit")
        raw = json.loads(manifest_path.read_text(encoding="utf-8"))
    except ReleaseEvidenceError:
        raise
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ReleaseEvidenceError("release manifest cannot be read") from exc
    manifest = validate_release_manifest(raw)
    expected_revision = os.environ.get("ELASTIC_AGENT_RELEASE_REVISION", "").strip()
    if expected_revision and expected_revision != manifest["release_revision"]:
        raise ReleaseEvidenceError("release revision does not match the manifest")
    return manifest
