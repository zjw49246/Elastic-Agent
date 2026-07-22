"""Unit tests for the version-controlled AWS production launcher."""

from __future__ import annotations

from copy import deepcopy
from pathlib import Path

import pytest

from deploy.aws_manager import (
    CANONICAL_OWNER_ID,
    LauncherConfigurationError,
    build_config,
    load_settings,
    prepare_local_paths,
    validate_image_description,
    validate_worker_ami,
)

CALLER_ACCOUNT = "123456789012"


def _environment(tmp_path: Path) -> dict[str, str]:
    return {
        "ELASTIC_AGENT_EXTERNAL_API_KEYS": "unit-test-secret",
        "ELASTIC_AGENT_AWS_REGION": "ap-northeast-1",
        "ELASTIC_AGENT_AWS_AMI_ID": "ami-0123456789abcdef0",
        "ELASTIC_AGENT_AWS_INSTANCE_TYPE": "t3.large",
        "ELASTIC_AGENT_AWS_WORKER_SECURITY_GROUP_IDS": ("sg-0123456789abcdef0,sg-11111111111111111"),
        "ELASTIC_AGENT_AWS_SUBNET_ID": "subnet-0123456789abcdef0",
        "ELASTIC_AGENT_AWS_KEY_PAIR_NAME": "elastic-agent-key",
        "ELASTIC_AGENT_AWS_SSH_KEY_PATH": str(tmp_path / "worker.pem"),
        "ELASTIC_AGENT_AWS_WORKER_INSTANCE_PROFILE": "elastic-agent-worker",
        "ELASTIC_AGENT_AWS_MAX_INSTANCES": "30",
        "ELASTIC_AGENT_STATE_DIR": str(tmp_path / "state"),
        "ELASTIC_AGENT_MANAGER_URL": "wss://manager.example/ws/runtime",
        "ELASTIC_AGENT_FRAMEWORK_SRC": str(tmp_path / "release" / "src"),
        "ELASTIC_AGENT_SERVER_HOST": "0.0.0.0",
        "ELASTIC_AGENT_SERVER_PORT": "8080",
        "ELASTIC_AGENT_WORKER_SSH_USER": "ubuntu",
        "ELASTIC_AGENT_LOG_LEVEL": "INFO",
        "ELASTIC_AGENT_RESULTS_S3_BUCKET": "elastic-agent-results-example",
        "ELASTIC_AGENT_RESULTS_S3_PREFIX": "jobs",
        "ELASTIC_AGENT_RESULTS_S3_INTERVAL": "60",
    }


def _golden_image() -> dict:
    return {
        "ImageId": "ami-0123456789abcdef0",
        "OwnerId": CALLER_ACCOUNT,
        "State": "available",
        "Architecture": "x86_64",
        "VirtualizationType": "hvm",
        "EnaSupport": True,
        "ImdsSupport": "v2.0",
        "RootDeviceName": "/dev/sda1",
        "BlockDeviceMappings": [
            {
                "DeviceName": "/dev/sda1",
                "Ebs": {
                    "SnapshotId": "snap-0123456789abcdef0",
                    "Encrypted": True,
                },
            }
        ],
        "Tags": [
            {"Key": "ManagedBy", "Value": "elastic-agent"},
            {"Key": "Role", "Value": "worker-golden"},
        ],
    }


def test_load_settings_and_build_config_are_fully_environment_driven(tmp_path):
    settings = load_settings(_environment(tmp_path))
    config = build_config(settings)

    assert config.provider.type == "aws"
    assert config.provider.aws.ami_id == "ami-0123456789abcdef0"
    assert config.provider.aws.security_group_ids == [
        "sg-0123456789abcdef0",
        "sg-11111111111111111",
    ]
    assert config.provider.aws.subnet_id == "subnet-0123456789abcdef0"
    assert config.provider.aws.key_pair_name == "elastic-agent-key"
    assert config.provider.aws.worker_instance_profile == "elastic-agent-worker"
    assert config.provider.aws.max_instances == 30
    assert config.worker.ssh_user == "ubuntu"
    assert config.registry.path == str(tmp_path / "state" / "registry.json")
    assert config.task_registry.path == str(tmp_path / "state" / "task_registry.json")
    assert config.logging.operations_log == str(tmp_path / "state" / "operations.log")
    assert settings.results_s3_bucket == "elastic-agent-results-example"
    assert settings.results_s3_prefix == "jobs"
    assert settings.results_s3_interval == 60


@pytest.mark.parametrize(
    "missing",
    [
        "ELASTIC_AGENT_EXTERNAL_API_KEYS",
        "ELASTIC_AGENT_AWS_AMI_ID",
        "ELASTIC_AGENT_AWS_WORKER_SECURITY_GROUP_IDS",
        "ELASTIC_AGENT_AWS_SUBNET_ID",
        "ELASTIC_AGENT_AWS_KEY_PAIR_NAME",
        "ELASTIC_AGENT_AWS_SSH_KEY_PATH",
        "ELASTIC_AGENT_AWS_WORKER_INSTANCE_PROFILE",
        "ELASTIC_AGENT_STATE_DIR",
        "ELASTIC_AGENT_MANAGER_URL",
        "ELASTIC_AGENT_RESULTS_S3_BUCKET",
        "ELASTIC_AGENT_RESULTS_S3_PREFIX",
        "ELASTIC_AGENT_RESULTS_S3_INTERVAL",
    ],
)
def test_load_settings_requires_production_values(tmp_path, missing):
    environ = _environment(tmp_path)
    environ.pop(missing)

    with pytest.raises(LauncherConfigurationError, match=missing):
        load_settings(environ)


@pytest.mark.parametrize(
    "url",
    [
        "ws://manager.example/ws/runtime",
        "wss://user:password@manager.example/ws/runtime",
        "wss://manager.example/ws/runtime?token=secret",
        "wss://manager.example/other",
    ],
)
def test_load_settings_requires_secret_free_wss_runtime_url(tmp_path, url):
    environ = _environment(tmp_path)
    environ["ELASTIC_AGENT_MANAGER_URL"] = url

    with pytest.raises(LauncherConfigurationError, match="ELASTIC_AGENT_MANAGER_URL"):
        load_settings(environ)


def test_settings_repr_does_not_contain_external_api_key(tmp_path):
    environ = _environment(tmp_path)
    environ["ELASTIC_AGENT_EXTERNAL_API_KEYS"] = "must-never-be-rendered"

    settings = load_settings(environ)

    assert "must-never-be-rendered" not in repr(settings)


def test_prepare_local_paths_secures_state_and_checks_key(tmp_path):
    environ = _environment(tmp_path)
    key_path = Path(environ["ELASTIC_AGENT_AWS_SSH_KEY_PATH"])
    key_path.write_text("test key", encoding="utf-8")
    key_path.chmod(0o600)
    Path(environ["ELASTIC_AGENT_FRAMEWORK_SRC"]).mkdir(parents=True)
    settings = load_settings(environ)

    prepare_local_paths(settings)

    assert settings.state_dir.stat().st_mode & 0o777 == 0o700


def test_prepare_local_paths_rejects_exposed_ssh_key(tmp_path):
    environ = _environment(tmp_path)
    key_path = Path(environ["ELASTIC_AGENT_AWS_SSH_KEY_PATH"])
    key_path.write_text("test key", encoding="utf-8")
    key_path.chmod(0o644)
    Path(environ["ELASTIC_AGENT_FRAMEWORK_SRC"]).mkdir(parents=True)

    with pytest.raises(LauncherConfigurationError, match="group/world"):
        prepare_local_paths(load_settings(environ))


def test_prepare_local_paths_rejects_unreadable_ssh_key(tmp_path, monkeypatch):
    environ = _environment(tmp_path)
    key_path = Path(environ["ELASTIC_AGENT_AWS_SSH_KEY_PATH"])
    key_path.write_text("test key", encoding="utf-8")
    key_path.chmod(0o600)
    Path(environ["ELASTIC_AGENT_FRAMEWORK_SRC"]).mkdir(parents=True)
    monkeypatch.setattr("deploy.aws_manager.os.access", lambda *_args: False)

    with pytest.raises(LauncherConfigurationError, match="readable"):
        prepare_local_paths(load_settings(environ))


def test_self_owned_encrypted_tagged_golden_image_is_accepted():
    result = validate_image_description(_golden_image(), caller_account_id=CALLER_ACCOUNT)

    assert result.provenance == "self-owned-golden"
    assert result.break_glass_used is False


@pytest.mark.parametrize(
    ("field", "bad_value", "message"),
    [
        ("State", "pending", "available"),
        ("Architecture", "arm64", "x86_64"),
        ("VirtualizationType", "paravirtual", "HVM"),
        ("EnaSupport", False, "ENA"),
        ("ImdsSupport", None, "IMDSv2"),
    ],
)
def test_runtime_invariants_are_mandatory(field, bad_value, message):
    image = _golden_image()
    image[field] = bad_value

    with pytest.raises(LauncherConfigurationError, match=message):
        validate_image_description(image, caller_account_id=CALLER_ACCOUNT)


def test_self_owned_image_requires_golden_tags():
    image = _golden_image()
    image["Tags"] = [{"Key": "ManagedBy", "Value": "someone-else"}]

    with pytest.raises(LauncherConfigurationError, match="golden tags"):
        validate_image_description(image, caller_account_id=CALLER_ACCOUNT)


def test_self_owned_image_requires_encrypted_root_snapshot():
    image = _golden_image()
    image["BlockDeviceMappings"][0]["Ebs"]["Encrypted"] = False

    with pytest.raises(LauncherConfigurationError, match="root snapshot must be encrypted"):
        validate_image_description(image, caller_account_id=CALLER_ACCOUNT)


def test_canonical_base_image_requires_explicit_break_glass():
    image = _golden_image()
    image["OwnerId"] = CANONICAL_OWNER_ID
    image["Tags"] = []
    image["BlockDeviceMappings"][0]["Ebs"]["Encrypted"] = False

    with pytest.raises(LauncherConfigurationError, match="ALLOW_CANONICAL"):
        validate_image_description(image, caller_account_id=CALLER_ACCOUNT)

    result = validate_image_description(
        image,
        caller_account_id=CALLER_ACCOUNT,
        allow_canonical_base_ami=True,
    )
    assert result.provenance == "canonical-break-glass"
    assert result.break_glass_used is True


def test_unrelated_publisher_is_rejected_even_with_break_glass():
    image = _golden_image()
    image["OwnerId"] = "999999999999"

    with pytest.raises(LauncherConfigurationError, match="neither"):
        validate_image_description(
            image,
            caller_account_id=CALLER_ACCOUNT,
            allow_canonical_base_ami=True,
        )


class _EC2Client:
    def __init__(self, images):
        self.images = images
        self.image_ids = None

    def describe_images(self, **kwargs):
        self.image_ids = kwargs["ImageIds"]
        return {"Images": deepcopy(self.images)}


class _STSClient:
    def get_caller_identity(self):
        return {"Account": CALLER_ACCOUNT}


def test_validate_worker_ami_queries_only_configured_image(tmp_path):
    settings = load_settings(_environment(tmp_path))
    client = _EC2Client([_golden_image()])

    result = validate_worker_ami(
        settings,
        ec2_client=client,
        sts_client=_STSClient(),
    )

    assert client.image_ids == [settings.ami_id]
    assert result.image_id == settings.ami_id


def test_validate_worker_ami_fails_closed_when_image_is_not_unique(tmp_path):
    settings = load_settings(_environment(tmp_path))

    with pytest.raises(LauncherConfigurationError, match="no unique image"):
        validate_worker_ami(
            settings,
            ec2_client=_EC2Client([]),
            sts_client=_STSClient(),
        )


def test_validate_worker_ami_fails_closed_on_mismatched_response(tmp_path):
    settings = load_settings(_environment(tmp_path))
    image = _golden_image()
    image["ImageId"] = "ami-11111111111111111"

    with pytest.raises(LauncherConfigurationError, match="different image"):
        validate_worker_ami(
            settings,
            ec2_client=_EC2Client([image]),
            sts_client=_STSClient(),
        )
