"""Release manifest and startup evidence contracts."""

from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

import pytest

from elastic_agent.core.release_evidence import (
    MANAGER_STATE_SCHEMA,
    ReleaseEvidenceError,
    canonical_json,
    compute_release_digest,
    compute_task_platform_worker_profile_digest,
    compute_worker_runtime_provenance_digest,
    load_release_manifest,
    validate_deployment_context,
    validate_release_manifest,
    validate_task_platform_worker_profile,
    verify_release_tree,
)

ROOT = Path(__file__).resolve().parents[2]
TEST_ONLY_PROFILE_PATH = ROOT / "tests/fixtures/task-platform-worker-profile-test-only.json"
TEST_ONLY_PROFILE_DIGEST = "sha256:17f2b0d9c4a892e75a0e668070c0147a436f84f5c169af5cd1c7695b639c1eed"


def _test_only_profile() -> dict:
    return json.loads(TEST_ONLY_PROFILE_PATH.read_text(encoding="utf-8"))


def _v3_manifest() -> dict:
    legacy = json.loads((ROOT / "deploy/release-manifest.json").read_text(encoding="utf-8"))
    provenance = deepcopy(
        legacy.get("worker_runtime_provenance", legacy["worker_profile"])
    )
    profile = _test_only_profile()
    manifest = {
        **{
            key: value
            for key, value in legacy.items()
            if key not in {"worker_profile", "worker_profile_digest", "release_digest"}
        },
        "schema_version": 3,
        "worker_runtime_provenance": provenance,
        "worker_runtime_provenance_digest": compute_worker_runtime_provenance_digest(provenance),
        "worker_profile": profile,
        "worker_profile_digest": compute_task_platform_worker_profile_digest(profile),
        "release_digest": "",
    }
    manifest["release_digest"] = compute_release_digest(manifest)
    return manifest


def test_checked_in_manifest_is_canonical_and_stable() -> None:
    manifest = load_release_manifest()
    provenance = manifest["worker_runtime_provenance"]
    assert manifest["manager_state_schema"] == MANAGER_STATE_SCHEMA
    assert manifest["manager_state_schema"] == "v1"
    assert manifest["worker_runtime_provenance_digest"] == compute_worker_runtime_provenance_digest(
        provenance
    )
    assert manifest["worker_profile_digest"] == compute_task_platform_worker_profile_digest(
        manifest["worker_profile"]
    )
    assert manifest["release_digest"] == compute_release_digest(manifest)
    assert manifest["worker_profile_digest"].startswith("sha256:")
    assert manifest["release_digest"].startswith("sha256:")
    assert provenance["ami_manifest_digest"].startswith("sha256:")
    assert provenance["ami_constraints_digest"].startswith("sha256:")
    assert provenance["ami_generator_version"] == "build-only-v1"
    assert canonical_json(manifest) == canonical_json(json.loads(canonical_json(manifest)))


def test_schema_v3_separates_task_platform_profile_from_runtime_provenance() -> None:
    manifest = _v3_manifest()
    assert validate_release_manifest(manifest) == manifest
    assert manifest["worker_profile_digest"] == TEST_ONLY_PROFILE_DIGEST
    assert manifest["worker_profile_digest"] != manifest["worker_runtime_provenance_digest"]


def test_task_platform_profile_digest_matches_utf8_canonical_json_golden() -> None:
    profile = _test_only_profile()
    assert validate_task_platform_worker_profile(profile) == profile
    assert compute_task_platform_worker_profile_digest(profile) == TEST_ONLY_PROFILE_DIGEST
    unicode_profile = {**profile, "input_prefixes": ["输入/"]}
    expected = "sha256:" + __import__("hashlib").sha256(
        json.dumps(
            unicode_profile,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()
    assert compute_task_platform_worker_profile_digest(unicode_profile) == expected


@pytest.mark.parametrize(
    "mutation",
    [
        lambda profile: profile.pop("role_arn"),
        lambda profile: profile.update({"unknown": "value"}),
        lambda profile: profile.update({"subnet_ids": []}),
        lambda profile: profile.update({"role_arn": "not-an-arn"}),
    ],
)
def test_task_platform_profile_strictly_rejects_incomplete_or_extra_input(mutation) -> None:
    profile = _test_only_profile()
    mutation(profile)
    with pytest.raises(ReleaseEvidenceError):
        validate_task_platform_worker_profile(profile)


def test_schema_v3_detects_each_digest_domain_tampering() -> None:
    manifest = _v3_manifest()
    manifest["worker_profile"] = deepcopy(manifest["worker_profile"])
    manifest["worker_profile"]["profile_id"] = "changed"
    with pytest.raises(ReleaseEvidenceError, match="worker_profile_digest"):
        validate_release_manifest(manifest)

    manifest = _v3_manifest()
    manifest["worker_runtime_provenance"] = deepcopy(manifest["worker_runtime_provenance"])
    manifest["worker_runtime_provenance"]["runtime_source"] = "changed.py"
    with pytest.raises(ReleaseEvidenceError, match="runtime provenance digest"):
        validate_release_manifest(manifest)


def test_generator_requires_external_worker_profile_and_emits_v3(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import scripts.generate_release_evidence as generator

    previous = json.loads((ROOT / "deploy/release-manifest.json").read_text(encoding="utf-8"))
    previous["worker_runtime_provenance"]["ami_id"] = _test_only_profile()["ami_id"]
    previous_path = tmp_path / "previous-release-manifest.json"
    previous_path.write_text(json.dumps(previous), encoding="utf-8")
    monkeypatch.setattr(generator, "MANIFEST_PATH", previous_path)

    _index, manifest = generator.generate(TEST_ONLY_PROFILE_PATH)
    assert manifest["schema_version"] == 3
    assert manifest["worker_profile"] == _test_only_profile()
    assert manifest["worker_profile_digest"] == TEST_ONLY_PROFILE_DIGEST
    assert "worker_runtime_provenance_digest" in manifest

    missing = tmp_path / "missing.json"
    with pytest.raises(SystemExit):
        generator.generate(missing)


def test_generator_cli_requires_worker_profile_input(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import scripts.generate_release_evidence as generator

    monkeypatch.setattr("sys.argv", ["generate_release_evidence.py", "--check"])
    with pytest.raises(SystemExit):
        generator.main()


def test_manifest_tampering_is_rejected() -> None:
    manifest = load_release_manifest()
    manifest["worker_profile"] = dict(manifest["worker_profile"])
    manifest["worker_profile"]["profile_id"] = "tampered"
    with pytest.raises(ReleaseEvidenceError, match="worker_profile_digest"):
        validate_release_manifest(manifest)


def test_manifest_rejects_secret_named_fields() -> None:
    manifest = load_release_manifest()
    manifest["api_token"] = "must-not-be-published"
    with pytest.raises(ReleaseEvidenceError, match="fields are invalid"):
        validate_release_manifest(manifest)


def test_manifest_path_is_bounded_and_fail_closed(tmp_path: Path) -> None:
    path = tmp_path / "release.json"
    path.write_text('{"schema_version": 1}', encoding="utf-8")
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
    (copied / "deploy/release-files.json").write_text(json.dumps(index), encoding="utf-8")
    first = index["files"][0]
    target = copied / first["path"]
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(b"tampered")
    with pytest.raises(ReleaseEvidenceError, match="content changed"):
        verify_release_tree(manifest, release_root=copied)


def test_deployment_context_accepts_only_canonical_aws_identity() -> None:
    manifest = load_release_manifest()
    provenance = manifest["worker_runtime_provenance"]
    context = {
        "ami_id": provenance["ami_id"],
        "region": provenance["region"],
        "aws_account_id": provenance["aws_account_id"],
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
