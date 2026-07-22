"""Static safety contract for the golden AMI builder."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "build_golden_ami.sh"


def test_builder_script_is_executable_and_valid_bash() -> None:
    assert os.access(SCRIPT, os.X_OK)
    result = subprocess.run(
        ["bash", "-n", str(SCRIPT)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def test_builder_is_pinned_encrypted_and_imdsv2_only() -> None:
    source = SCRIPT.read_text()
    assert "--base-ami is required and must be pinned" in source
    assert '"Encrypted": True' in source
    assert "HttpTokens=required" in source
    assert "ImdsSupport" in source and "v2.0" in source
    assert "099720109477" in source  # Canonical's owner ID
    assert "latest" not in "\n".join(line for line in source.splitlines() if not line.startswith("#"))


def test_public_egress_does_not_implicitly_move_ssh_off_private_vpc() -> None:
    source = SCRIPT.read_text()
    assert "--associate-public-ip) ASSOCIATE_PUBLIC_IP=true" in source
    assert "--use-public-ssh) USE_PUBLIC_SSH=true" in source
    private_default = source.index('BUILDER_HOST="$PRIVATE_IP"')
    public_switch = source.index('if [[ "$USE_PUBLIC_SSH" == true ]]')
    assert private_default < public_switch


def test_builder_installs_verifier_and_writes_manifest_schema() -> None:
    source = SCRIPT.read_text()
    assert "/usr/local/bin/elastic-agent-image-verify" in source
    assert "/etc/elastic-agent/image-manifest.json" in source
    assert '"schema_version": 1' in source
    for component in ("system", "agents", "login", "docker", "runtime", "pty"):
        assert f'"{component}"' in source


def test_builder_scrubs_identity_and_tags_image_and_snapshot() -> None:
    source = SCRIPT.read_text()
    assert "cloud-init clean --logs --machine-id --seed" in source
    assert "ssh_host_*" in source
    assert ".claude" in source and ".codex" in source and ".aws" in source
    assert '{"ResourceType": "image", "Tags": tags}' in source
    assert '{"ResourceType": "snapshot", "Tags": tags}' in source
    assert '{"Key": "ManagedBy", "Value": "elastic-agent"}' in source
    assert '{"Key": "Role", "Value": "worker-golden"}' in source


def test_existing_instance_must_have_builder_ownership_tags_before_cleanup() -> None:
    source = SCRIPT.read_text()
    ownership_check = source.index('"$PURPOSE_TAG" == "golden-image-build"')
    ownership_claim = source.index("BUILDER_OWNED=true", ownership_check)
    cleanup = source.index('if [[ "$BUILDER_OWNED" == true')

    assert ownership_check < ownership_claim
    assert cleanup < ownership_check  # function declaration is harmless
    assert "BUILDER_OWNED" in source
    assert '"$ROLE_TAG" == "ami-builder"' in source
    assert '"$NAME_TAG" == elastic-agent-ami-builder*' in source
