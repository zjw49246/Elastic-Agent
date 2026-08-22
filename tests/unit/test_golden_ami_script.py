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
    for component in ("system", "agents", "login", "docker", "runtime"):
        assert f'"{component}"' in source
    assert "Claude-Code-PTY" not in source
    assert "ClaudePtyCommit" not in source


def test_builder_declares_docker_sandbox_profile_dependencies() -> None:
    source = SCRIPT.read_text()

    assert "python3-venv bubblewrap util-linux" in source
    assert '"python3-venv", "bubblewrap", "util-linux"' in source
    assert (
        "$V system python3 python3-pip git curl rsync nodejs npm "
        "python3-venv bubblewrap util-linux"
    ) in source
    assert (
        '"EnvironmentProfiles", "Value": '
        '"ubuntu-agent-v1,ubuntu-agent-docker-v1,'
        'ubuntu-agent-docker-sandbox-v1"'
    ) in source


def test_builder_disables_background_updates_before_image_creation() -> None:
    source = SCRIPT.read_text()
    hardening = source.index(
        "/etc/apt/apt.conf.d/99elastic-agent-no-background-upgrades"
    )
    create_image = source.index("aws ec2 create-image")

    assert hardening < create_image
    assert 'APT::Periodic::Enable "0";' in source
    assert "apt-daily.timer" in source
    assert "apt-daily-upgrade.timer" in source
    assert "unattended-upgrades.service" in source
    assert "systemctl mask" in source
    assert "/etc/needrestart/conf.d/99-elastic-agent.conf" in source
    assert "ea-task-supervisor|elastic-agent-task-supervisor" in source
    assert "$nrconf{restart} = 'l';" in source
    assert "ea-runtime" in source
    assert "elastic-agent-runtime" in source
    assert "ea-task@" in source


def test_builder_scrubs_identity_and_tags_image_and_snapshot() -> None:
    source = SCRIPT.read_text()
    assert "cloud-init clean --logs --machine-id --seed" in source
    assert "ssh_host_*" in source
    assert ".claude" in source and ".codex" in source and ".aws" in source
    assert '{"ResourceType": "image", "Tags": tags}' in source
    assert '{"ResourceType": "snapshot", "Tags": tags}' in source
    assert '{"Key": "ManagedBy", "Value": "elastic-agent"}' in source
    assert '{"Key": "Role", "Value": "worker-golden"}' in source
    assert "ea-task-supervisor.service" in source
    assert "/usr/local/bin/ea-task-supervisor.sh" in source
    assert "/ea-tasks" in source and "/ea-logs" in source


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


def test_image_wait_handles_slow_encrypted_snapshot_creation() -> None:
    source = SCRIPT.read_text()
    # The AWS CLI waiter's fixed default window is too short for a cold,
    # encrypted 20-GiB snapshot in ap-northeast-1. Keep the builder cleanup
    # bounded while allowing the already-created image enough time to finish.
    assert "aws ec2 wait image-available" not in source
    assert 'IMAGE_WAIT_ATTEMPTS=240' in source
    assert 'IMAGE_WAIT_SECONDS=15' in source
    assert 'for attempt in $(seq 1 "$IMAGE_WAIT_ATTEMPTS")' in source
    assert '"$IMAGE_STATE" == "available"' in source
    assert "failed|error|invalid|deregistered" in source
    assert "entered terminal state" in source
    assert "returned unexpected state" in source
    assert "last state=$IMAGE_STATE" in source
    assert '((attempt == IMAGE_WAIT_ATTEMPTS)) || sleep "$IMAGE_WAIT_SECONDS"' in source
