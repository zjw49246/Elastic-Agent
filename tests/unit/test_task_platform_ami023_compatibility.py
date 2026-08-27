import json
from pathlib import Path

from elastic_agent.core.release_evidence import (
    compute_task_platform_worker_profile_digest,
    compute_worker_runtime_provenance_digest,
    validate_release_manifest,
)


ROOT = Path(__file__).resolve().parents[2]
COMPAT = ROOT / "deploy" / "compatibility"


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def test_ami023_binding_is_a_provenance_preserving_copy() -> None:
    evidence = _load(COMPAT / "ami-023-evidence.json")
    assert evidence["compatibility"] == (
        "ami-023251121ceb0d6f3 -> source ami-03c4e1dca88d39005"
    )
    assert evidence["ami_023"] == {
        "image_id": "ami-023251121ceb0d6f3",
        "source_image_id": "ami-03c4e1dca88d39005",
        "state": "available",
        "architecture": "x86_64",
        "virtualization_type": "hvm",
        "imds_support": "v2.0",
        "owner_id": "297645381734",
        "last_launched_time": "2026-08-27T06:28:52Z",
        "tags_match": [
            "ManifestDigest",
            "ConstraintsDigest",
            "RunnerImage",
            "PlatformRevision",
            "UpstreamRevision",
            "GeneratorVersion",
            "Environment",
            "Service",
            "TaskPlatform",
        ],
    }
    assert evidence["decision"].startswith("compatible-binding-only")
    assert evidence["production_writes"] is False


def test_release_and_external_profile_share_the_ami023_binding() -> None:
    manifest = validate_release_manifest(_load(ROOT / "deploy" / "release-manifest.json"))
    profile = _load(COMPAT / "task-platform-worker-profile-ami023.json")
    provenance = manifest["worker_runtime_provenance"]

    assert profile["ami_id"] == provenance["ami_id"] == "ami-023251121ceb0d6f3"
    assert manifest["worker_profile"] == profile
    assert manifest["worker_profile_digest"] == compute_task_platform_worker_profile_digest(profile)
    assert manifest["worker_runtime_provenance_digest"] == compute_worker_runtime_provenance_digest(provenance)
    assert profile["instance_profile_name"] == "TaskPlatformBuildOnlyWorker-pilot"
    assert profile["subnet_ids"] == ["subnet-0c1db80817d054277"]
    assert profile["security_group_ids"] == ["sg-0a72ebfc1a59587c5"]
    assert profile["environment_profiles"] == ["ubuntu-agent-docker-v2"]
