"""Contracts for the hash-pinned production runtime dependency export."""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
_REQUIREMENT = re.compile(r"^([A-Za-z0-9_.-]+)==[^ ]+")


def _requirements() -> str:
    return (ROOT / "requirements.txt").read_text(encoding="utf-8")


def test_production_requirements_include_boto3_runtime_closure_with_hashes() -> None:
    text = _requirements()
    packages = set()
    current = None
    for line in text.splitlines():
        match = _REQUIREMENT.match(line)
        if match:
            current = match.group(1).lower().replace("_", "-")
            packages.add(current)
        elif line.strip().startswith("--hash="):
            assert current is not None

    assert {"boto3", "botocore", "s3transfer", "jmespath", "python-dateutil", "urllib3", "six"} <= packages
    for package in packages:
        block = re.search(
            rf"(?ms)^{re.escape(package)}==.*?(?=^[A-Za-z0-9_.-]+==|\Z)",
            text,
        )
        assert block and re.search(r"(?m)^\s+--hash=sha256:[0-9a-f]{64}$", block.group(0))


def test_manager_ami_validation_has_declared_boto3_dependency() -> None:
    pyproject = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    manager = (ROOT / "deploy/aws_manager.py").read_text(encoding="utf-8")
    assert '"boto3>=1.35.0"' in pyproject.split("[project.optional-dependencies]", 1)[0]
    assert "import boto3" in manager
    assert "def validate_worker_ami" in manager


def test_release_tree_indexes_requirements_for_drift_detection() -> None:
    index = json.loads((ROOT / "deploy/release-files.json").read_text(encoding="utf-8"))
    entry = next(item for item in index["files"] if item["path"] == "requirements.txt")
    requirements = (ROOT / "requirements.txt").read_bytes()
    assert entry["size"] == len(requirements)
    assert entry["sha256"] == "sha256:" + hashlib.sha256(requirements).hexdigest()
    generator = (ROOT / "scripts/generate_release_evidence.py").read_text(encoding="utf-8")
    assert '"deploy/release-manifest.json", "deploy/release-files.json"' in generator
