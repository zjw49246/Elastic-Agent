"""Release manifest and startup evidence contracts."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from elastic_agent.core.release_evidence import (
    MANAGER_STATE_SCHEMA,
    ReleaseEvidenceError,
    canonical_json,
    compute_release_digest,
    compute_worker_profile_digest,
    load_release_manifest,
    verify_release_tree,
    validate_deployment_context,
    validate_release_manifest,
)


def test_checked_in_manifest_is_canonical_and_stable() -> None:
    manifest = load_release_manifest()
    assert manifest["manager_state_schema"] == MANAGER_STATE_SCHEMA
    assert manifest["manager_state_schema"] == "v1"
    assert manifest["worker_profile_digest"] == compute_worker_profile_digest(
        manifest["worker_profile"]
    )
    assert manifest["release_digest"] == compute_release_digest(manifest)
    assert manifest["worker_profile_digest"].startswith("sha256:")
    assert manifest["release_digest"].startswith("sha256:")
    assert canonical_json(manifest) == canonical_json(json.loads(canonical_json(manifest)))


def test_manifest_tampering_is_rejected() -> None:
    manifest = load_release_manifest()
    manifest["worker_profile"] = dict(manifest["worker_profile"])
    manifest["worker_profile"]["runtime_source"] = "tampered.py"
    with pytest.raises(ReleaseEvidenceError, match="worker_profile_digest"):
        validate_release_manifest(manifest)


def test_manifest_rejects_secret_named_fields() -> None:
    manifest = load_release_manifest()
    manifest["api_token"] = "must-not-be-published"
    with pytest.raises(ReleaseEvidenceError, match="fields are invalid"):
        validate_release_manifest(manifest)


def test_manifest_path_is_bounded_and_fail_closed(tmp_path: Path) -> None:
    path = tmp_path / "release.json"
    path.write_text("{\"schema_version\": 1}", encoding="utf-8")
    with pytest.raises(ReleaseEvidenceError):
        load_release_manifest(path)


def test_checked_in_release_tree_is_bound_by_artifact_index() -> None:
    manifest = load_release_manifest()
    verify_release_tree(manifest, release_root=Path(__file__).resolve().parents[2])


def test_release_tree_tampering_is_rejected(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[2]
    manifest = load_release_manifest()
    index = json.loads((root / "deploy/release-files.json").read_text())
    copied = tmp_path / "release"
    (copied / "deploy").mkdir(parents=True)
    (copied / "src").mkdir()
    (copied / "deploy/release-files.json").write_text(
        json.dumps(index), encoding="utf-8"
    )
    first = index["files"][0]
    target = copied / first["path"]
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(b"tampered")
    with pytest.raises(ReleaseEvidenceError, match="content changed"):
        verify_release_tree(manifest, release_root=copied)


def test_deployment_context_accepts_only_canonical_aws_identity() -> None:
    manifest = load_release_manifest()
    profile = manifest["worker_profile"]
    context = {
        "ami_id": profile["ami_id"],
        "region": profile["region"],
        "aws_account_id": profile["aws_account_id"],
        "release_revision": manifest["release_revision"],
    }
    validate_deployment_context(manifest, context)

    context["ami_id"] = "ami-0aec7ffcbe44c6f7a"
    with pytest.raises(ReleaseEvidenceError, match="deployment ami_id"):
        validate_deployment_context(manifest, context)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("manager_state_schema", "elastic-agent-manager-state-v1"),
        ("worker_profile_digest", "0" * 64),
        ("release_digest", "0" * 64),
    ],
)
def test_task_platform_contract_format_is_fail_closed(field: str, value: str) -> None:
    manifest = load_release_manifest()
    manifest[field] = value
    with pytest.raises(ReleaseEvidenceError):
        validate_release_manifest(manifest)
