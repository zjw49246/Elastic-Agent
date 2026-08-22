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
import stat
from pathlib import Path
from typing import Any, Mapping

RELEASE_MANIFEST_SCHEMA = 2
RELEASE_INDEX_SCHEMA = 1
MANAGER_STATE_SCHEMA = "v1"
WORKER_PROFILE_SCHEMA = 1
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_SHA256_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
_COMMIT = re.compile(r"^[0-9a-f]{40}$")
_ARTIFACT_REVISION = re.compile(r"^artifact-sha256:[0-9a-f]{64}$")
_SCHEMA_NAME = re.compile(r"^v[1-9][0-9]{0,8}$")
_SECRET_NAME = re.compile(r"(?:secret|token|password|credential|api[_-]?key)", re.IGNORECASE)

_MANIFEST_KEYS = frozenset(
    {
        "schema_version",
        "product",
        "release_revision",
        "upstream_source_commit",
        "upstream_archive_sha256",
        "release_index",
        "release_artifact_digest",
        "manager_state_schema",
        "worker_profile",
        "worker_profile_digest",
        "release_digest",
    }
)
_RELEASE_INDEX_PATH = "deploy/release-files.json"
_GENERATED_RELEASE_PATHS = frozenset({"deploy/release-manifest.json", _RELEASE_INDEX_PATH})
_ALLOWED_UNTRACKED_PARTS = frozenset({".git", ".venv", ".pytest_cache", ".mypy_cache", ".ruff_cache", "__pycache__"})
_MAX_INDEX_BYTES = 4 * 1024 * 1024
_MAX_RELEASE_FILES = 10_000
_MAX_RELEASE_BYTES = 512 * 1024 * 1024
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
        "ami_manifest_digest",
        "ami_constraints_digest",
        "ami_runner_image",
        "ami_platform_revision",
        "ami_upstream_revision",
        "ami_generator_version",
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


def compute_release_artifact_digest(index: Mapping[str, Any]) -> str:
    """Return the digest that identifies the exact indexed release tree."""

    return f"sha256:{sha256_hex(index)}"


def artifact_revision(artifact_digest: str) -> str:
    if _SHA256_DIGEST.fullmatch(artifact_digest) is None:
        raise ReleaseEvidenceError("release artifact digest is invalid")
    return f"artifact-sha256:{artifact_digest.removeprefix('sha256:')}"


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
    for key in (
        "ami_manifest_digest",
        "ami_constraints_digest",
        "ami_runner_image",
        "ami_platform_revision",
        "ami_upstream_revision",
        "ami_generator_version",
    ):
        _require_string(result, key)
    if _SHA256_DIGEST.fullmatch(result["ami_manifest_digest"]) is None:
        raise ReleaseEvidenceError("worker profile ami_manifest_digest is invalid")
    if _SHA256_DIGEST.fullmatch(result["ami_constraints_digest"]) is None:
        raise ReleaseEvidenceError("worker profile ami_constraints_digest is invalid")
    if not re.fullmatch(
        r"[0-9]{12}\.dkr\.ecr\.[a-z]{2}(?:-gov)?-[a-z]+-\d\.amazonaws\.com/"
        r"[a-z0-9][a-z0-9._/-]*@sha256:[0-9a-f]{64}",
        result["ami_runner_image"],
    ):
        raise ReleaseEvidenceError("worker profile ami_runner_image is invalid")
    if _COMMIT.fullmatch(result["ami_platform_revision"]) is None:
        raise ReleaseEvidenceError("worker profile ami_platform_revision is invalid")
    if _COMMIT.fullmatch(result["ami_upstream_revision"]) is None:
        raise ReleaseEvidenceError("worker profile ami_upstream_revision is invalid")
    if result["ami_generator_version"] != "build-only-v1":
        raise ReleaseEvidenceError("worker profile ami_generator_version is invalid")
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
    if _ARTIFACT_REVISION.fullmatch(revision) is None:
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
    upstream_source_commit = _require_string(raw, "upstream_source_commit")
    if _COMMIT.fullmatch(upstream_source_commit) is None:
        raise ReleaseEvidenceError("release manifest upstream_source_commit is invalid")
    upstream_archive_sha256 = _require_string(raw, "upstream_archive_sha256")
    if _SHA256.fullmatch(upstream_archive_sha256) is None:
        raise ReleaseEvidenceError("release manifest upstream_archive_sha256 is invalid")
    if _require_string(raw, "release_index") != _RELEASE_INDEX_PATH:
        raise ReleaseEvidenceError("release manifest release_index is invalid")
    release_artifact_digest = _require_string(raw, "release_artifact_digest")
    if _SHA256_DIGEST.fullmatch(release_artifact_digest) is None:
        raise ReleaseEvidenceError("release_artifact_digest is invalid")
    release_revision = _require_string(raw, "release_revision")
    if _ARTIFACT_REVISION.fullmatch(release_revision) is None:
        raise ReleaseEvidenceError("release manifest release_revision is invalid")
    if release_revision != artifact_revision(release_artifact_digest):
        raise ReleaseEvidenceError("release revision does not match release artifact")
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


def _validate_release_index(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict) or set(raw) != {"schema_version", "files"}:
        raise ReleaseEvidenceError("release file index fields are invalid")
    if raw.get("schema_version") != RELEASE_INDEX_SCHEMA:
        raise ReleaseEvidenceError("unsupported release file index schema")
    files = raw.get("files")
    if not isinstance(files, list) or not files or len(files) > _MAX_RELEASE_FILES:
        raise ReleaseEvidenceError("release file index size is invalid")
    previous = ""
    total_bytes = 0
    normalized: list[dict[str, Any]] = []
    for record in files:
        if not isinstance(record, dict) or set(record) != {"path", "type", "mode", "size", "sha256"}:
            raise ReleaseEvidenceError("release file index record is invalid")
        path = record.get("path")
        kind = record.get("type")
        mode = record.get("mode")
        size = record.get("size")
        digest = record.get("sha256")
        if (
            not isinstance(path, str)
            or not path
            or path.startswith("/")
            or "\\" in path
            or any(part in {"", ".", ".."} for part in path.split("/"))
            or path in _GENERATED_RELEASE_PATHS
            or path <= previous
        ):
            raise ReleaseEvidenceError("release file index path is invalid")
        if kind not in {"file", "symlink"}:
            raise ReleaseEvidenceError("release file index type is invalid")
        if mode not in {"0644", "0755", "120000"}:
            raise ReleaseEvidenceError("release file index mode is invalid")
        if kind == "symlink" and mode != "120000":
            raise ReleaseEvidenceError("release symlink mode is invalid")
        if kind == "file" and mode == "120000":
            raise ReleaseEvidenceError("release file mode is invalid")
        if not isinstance(size, int) or isinstance(size, bool) or size < 0:
            raise ReleaseEvidenceError("release file index size is invalid")
        if not isinstance(digest, str) or _SHA256_DIGEST.fullmatch(digest) is None:
            raise ReleaseEvidenceError("release file index digest is invalid")
        total_bytes += size
        if total_bytes > _MAX_RELEASE_BYTES:
            raise ReleaseEvidenceError("release tree exceeds size limit")
        normalized.append(dict(record))
        previous = path
    return {"schema_version": RELEASE_INDEX_SCHEMA, "files": normalized}


def _file_digest(path: Path) -> tuple[int, str]:
    digest = hashlib.sha256()
    size = 0
    try:
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                size += len(chunk)
                digest.update(chunk)
    except OSError as exc:
        raise ReleaseEvidenceError("release file cannot be read") from exc
    return size, f"sha256:{digest.hexdigest()}"


def _tree_visible_files(root: Path) -> set[str]:
    result: set[str] = set()
    try:
        for path in root.rglob("*"):
            relative = path.relative_to(root)
            if any(part in _ALLOWED_UNTRACKED_PARTS for part in relative.parts):
                continue
            name = relative.as_posix()
            if name in _GENERATED_RELEASE_PATHS or path.is_dir():
                continue
            if path.suffix == ".pyc":
                continue
            result.add(name)
    except OSError as exc:
        raise ReleaseEvidenceError("release tree cannot be scanned") from exc
    return result


def verify_release_tree(
    manifest: Mapping[str, Any],
    *,
    release_root: str | Path,
) -> None:
    """Verify the exact indexed release files before Manager state is loaded."""

    try:
        root = Path(release_root).resolve(strict=True)
    except OSError as exc:
        raise ReleaseEvidenceError("release root cannot be resolved") from exc
    if not root.is_dir():
        raise ReleaseEvidenceError("release root is not a directory")
    index_path = root / _RELEASE_INDEX_PATH
    try:
        if index_path.is_symlink() or index_path.stat().st_size > _MAX_INDEX_BYTES:
            raise ReleaseEvidenceError("release file index is unsafe")
        raw = json.loads(index_path.read_text(encoding="utf-8"))
    except ReleaseEvidenceError:
        raise
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ReleaseEvidenceError("release file index cannot be read") from exc
    index = _validate_release_index(raw)
    expected_digest = manifest.get("release_artifact_digest")
    if expected_digest != compute_release_artifact_digest(index):
        raise ReleaseEvidenceError("release artifact digest does not match file index")

    indexed_paths: set[str] = set()
    for record in index["files"]:
        relative = record["path"]
        indexed_paths.add(relative)
        path = root / relative
        try:
            metadata = path.lstat()
        except OSError as exc:
            raise ReleaseEvidenceError(f"release file is missing: {relative}") from exc
        if record["type"] == "symlink":
            if not stat.S_ISLNK(metadata.st_mode):
                raise ReleaseEvidenceError(f"release file type changed: {relative}")
            data = os.fsencode(os.readlink(path))
            actual_size = len(data)
            actual_digest = f"sha256:{hashlib.sha256(data).hexdigest()}"
            actual_mode = "120000"
        else:
            if not stat.S_ISREG(metadata.st_mode):
                raise ReleaseEvidenceError(f"release file type changed: {relative}")
            actual_size, actual_digest = _file_digest(path)
            actual_mode = "0755" if metadata.st_mode & stat.S_IXUSR else "0644"
        if actual_mode != record["mode"]:
            raise ReleaseEvidenceError(f"release file mode changed: {relative}")
        if actual_size != record["size"] or actual_digest != record["sha256"]:
            raise ReleaseEvidenceError(f"release file content changed: {relative}")
    if _tree_visible_files(root) != indexed_paths:
        raise ReleaseEvidenceError("release tree file set does not match file index")


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
    verify_release_tree(manifest, release_root=manifest_path.parent.parent)
    expected_revision = os.environ.get("ELASTIC_AGENT_RELEASE_REVISION", "").strip()
    if expected_revision and expected_revision != manifest["release_revision"]:
        raise ReleaseEvidenceError("release revision does not match the manifest")
    return manifest
