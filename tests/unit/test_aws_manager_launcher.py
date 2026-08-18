"""Unit tests for the version-controlled AWS production launcher."""

from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace

import pytest

from deploy.aws_manager import (
    CANONICAL_OWNER_ID,
    LauncherConfigurationError,
    build_application,
    build_config,
    load_settings,
    prepare_local_paths,
    validate_image_description,
    validate_worker_ami,
)
from elastic_agent.api.auth import configured_public_origin, reset_management_auth
from elastic_agent.core.management_auth import ManagementUserStore

CALLER_ACCOUNT = "123456789012"
SERVICE_UNIT = Path(__file__).resolve().parents[2] / "deploy/aws/elastic-agent-manager.service"
AWS_ENV = (
    Path(__file__).resolve().parents[2]
    / "deploy/aws/elastic-agent-manager.aws.env"
)
MANAGER_POLICY = (
    Path(__file__).resolve().parents[2]
    / "deploy/aws/elastic-agent-manager-policy.json"
)
WORKER_POLICY = (
    Path(__file__).resolve().parents[2]
    / "deploy/aws/elastic-agent-worker-policy.json"
)
RESULTS_BUCKET_POLICY = (
    Path(__file__).resolve().parents[2]
    / "deploy/aws/elastic-agent-results-bucket-policy.json"
)
IAM_CUTOVER = Path(__file__).resolve().parents[2] / "deploy/aws/iam-cutover.md"


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
        "ELASTIC_AGENT_AWS_EXPECTED_ROLE_NAME": "elastic-agent-manager",
        "ELASTIC_AGENT_AWS_MAX_INSTANCES": "30",
        "ELASTIC_AGENT_WORKER_BRINGUP_CONCURRENCY": "8",
        "ELASTIC_AGENT_PERIODIC_COLLECT_CONCURRENCY": "8",
        "ELASTIC_AGENT_PERIODIC_COLLECT_JITTER_RATIO": "0.1",
        "ELASTIC_AGENT_STATE_DIR": str(tmp_path / "state"),
        "ELASTIC_AGENT_MANAGER_URL": "wss://manager.example/ws/runtime",
        "ELASTIC_AGENT_PUBLIC_ORIGIN": "https://manager.example",
        "ELASTIC_AGENT_FRAMEWORK_SRC": str(tmp_path / "release" / "src"),
        "ELASTIC_AGENT_SERVER_HOST": "0.0.0.0",
        "ELASTIC_AGENT_SERVER_PORT": "8080",
        "ELASTIC_AGENT_WORKER_SSH_USER": "ubuntu",
        "ELASTIC_AGENT_LOG_LEVEL": "INFO",
        "ELASTIC_AGENT_RESULTS_S3_BUCKET": "elastic-agent-results-example",
        "ELASTIC_AGENT_RESULTS_S3_PREFIX": "jobs",
        "ELASTIC_AGENT_RESULTS_S3_INTERVAL": "60",
        "ELASTIC_AGENT_RESULTS_S3_PERIODIC_ENABLED": "true",
        "AWS_SHARED_CREDENTIALS_FILE": "/dev/null",
        "AWS_CONFIG_FILE": "/dev/null",
        "BOTO_CONFIG": "/dev/null",
        "AWS_EC2_METADATA_DISABLED": "false",
        "AWS_EC2_METADATA_V1_DISABLED": "true",
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


def test_systemd_unit_enforces_state_readiness_and_imds_boundary():
    source = SERVICE_UNIT.read_text(encoding="utf-8")

    assert "AssertPathIsDirectory=/home/ubuntu/.elastic-agent-demo" in source
    assert "ReadWritePaths=/home/ubuntu/.elastic-agent-demo" in source
    assert "ExecStartPost=" in source and "/api/health" in source
    assert "TimeoutStopSec=32400" in source
    assert "LimitNOFILE=65536" in source
    for setting in (
        "AWS_SHARED_CREDENTIALS_FILE=/dev/null",
        "AWS_CONFIG_FILE=/dev/null",
        "BOTO_CONFIG=/dev/null",
        "AWS_EC2_METADATA_DISABLED=false",
        "AWS_EC2_METADATA_V1_DISABLED=true",
    ):
        assert setting in source
    assert "UnsetEnvironment=AWS_ACCESS_KEY_ID" in source


def test_production_launcher_trusts_forwarded_clients_only_from_loopback_proxy():
    source = (Path(__file__).resolve().parents[2] / "deploy/aws_manager.py").read_text(
        encoding="utf-8"
    )

    assert "proxy_headers=True" in source
    assert 'forwarded_allow_ips="127.0.0.1,::1"' in source


def test_production_allowlist_covers_common_x86_worker_families():
    source = AWS_ENV.read_text(encoding="utf-8")
    assert (
        "ELASTIC_AGENT_PUBLIC_ORIGIN=https://elastic-agent.claude-code-manager.com"
        in source
    )
    configured = next(
        line.partition("=")[2]
        for line in source.splitlines()
        if line.startswith("ELASTIC_AGENT_ALLOWED_INSTANCE_TYPES=")
    )
    actual = set(configured.split(","))
    expected = {
        f"{family}.{size}"
        for family in (
            "t3",
            "m5", "m6i", "m7i",
            "c5", "c6i", "c7i",
            "r5", "r6i", "r7i",
        )
        for size in ("large", "xlarge", "2xlarge", "4xlarge")
    }
    expected.remove("t3.4xlarge")  # T3 ends at 2xlarge.

    assert actual == expected
    policy = json.loads(MANAGER_POLICY.read_text(encoding="utf-8"))
    launch = next(
        statement
        for statement in policy["Statement"]
        if statement["Sid"] == "LaunchOnlyManagedWorkers"
    )
    iam_types = launch["Condition"]["StringEquals"]["ec2:InstanceType"]
    if isinstance(iam_types, str):
        iam_types = [iam_types]
    assert set(iam_types) == actual


def test_production_targets_800_bounded_workers():
    settings = {
        line.partition("=")[0]: line.partition("=")[2]
        for line in AWS_ENV.read_text(encoding="utf-8").splitlines()
        if line and not line.startswith("#") and "=" in line
    }

    assert settings["ELASTIC_AGENT_AWS_MAX_INSTANCES"] == "800"
    assert settings["ELASTIC_AGENT_JOB_BATCH_MAX_ACTIVE_JOBS"] == "800"
    assert settings["ELASTIC_AGENT_WORKER_BRINGUP_CONCURRENCY"] == "32"
    assert settings["ELASTIC_AGENT_PERIODIC_COLLECT_CONCURRENCY"] == "32"
    assert settings["ELASTIC_AGENT_RESULTS_S3_PERIODIC_ENABLED"] == "false"


def test_manager_policy_and_cutover_pin_real_key_pair_name():
    policy = json.loads(MANAGER_POLICY.read_text(encoding="utf-8"))
    launch = next(
        statement
        for statement in policy["Statement"]
        if statement["Sid"] == "LaunchWithPinnedInfrastructure"
    )
    expected = (
        "arn:aws:ec2:ap-northeast-1:297645381734:"
        "key-pair/interview-key"
    )

    assert expected in launch["Resource"]
    assert expected in IAM_CUTOVER.read_text(encoding="utf-8")
    assert not any("key-pair/key-" in resource for resource in launch["Resource"])


def test_iam_cutover_simulates_complete_manager_policy():
    policy = json.loads(MANAGER_POLICY.read_text(encoding="utf-8"))
    compact = json.dumps(policy, separators=(",", ":"))
    cutover = IAM_CUTOVER.read_text(encoding="utf-8")

    assert len(compact) < 131_072
    assert "MANAGER_POLICY=$(jq -c ." in cutover
    assert 'MANAGER_RUN_POLICY=' not in cutover
    assert '--policy-input-list "$MANAGER_POLICY"' in cutover
    assert "EvaluationResults[?EvalDecision!=`allowed`]" in cutover


def test_iam_cutover_rollback_is_fresh_shell_and_fail_closed():
    cutover = IAM_CUTOVER.read_text(encoding="utf-8")

    assert "Status (2026-07-22): completed" in cutover
    assert "CURRENT_ASSOCIATION_ID=$(aws ec2" in cutover
    assert '--association-id "$NEW_ASSOCIATION_ID"' not in cutover
    assert "test ! -e" in cutover
    assert "flag alone is insufficient" in cutover
    assert "Do not restore the old shared SG" in cutover


def test_manager_policy_tags_and_detaches_only_managed_network_interfaces():
    policy = json.loads(MANAGER_POLICY.read_text(encoding="utf-8"))
    statements = {statement["Sid"]: statement for statement in policy["Statement"]}

    create_tags = statements["TagManagedResourcesAtCreation"]
    assert (
        "arn:aws:ec2:ap-northeast-1:297645381734:network-interface/*"
        in create_tags["Resource"]
    )
    disassociate = statements["DisassociateManagedEips"]
    assert disassociate["Action"] == "ec2:DisassociateAddress"
    assert set(disassociate["Resource"]) == {
        "arn:aws:ec2:ap-northeast-1:297645381734:elastic-ip/*",
        "arn:aws:ec2:ap-northeast-1:297645381734:network-interface/*",
    }
    assert disassociate["Condition"]["StringEquals"] == {
        "aws:RequestedRegion": "ap-northeast-1",
        "ec2:ResourceTag/ManagedBy": "elastic-agent",
    }
    release = statements["ReleaseManagedEips"]
    assert release["Action"] == "ec2:ReleaseAddress"
    assert release["Resource"].endswith(":elastic-ip/*")


def test_manager_policy_allows_shard_tag_and_only_internal_checkpoint_deletes():
    policy = json.loads(MANAGER_POLICY.read_text(encoding="utf-8"))
    statements = {
        statement["Sid"]: statement
        for statement in policy["Statement"]
    }

    for sid in ("LaunchOnlyManagedWorkers", "TagManagedResourcesAtCreation"):
        assert (
            "ElasticAgentShardIndex"
            in statements[sid]["Condition"]["ForAllValues:StringEquals"][
                "aws:TagKeys"
            ]
        )
    delete = statements["DeleteInternalCheckpointHistory"]
    assert delete == {
        "Sid": "DeleteInternalCheckpointHistory",
        "Effect": "Allow",
        "Action": "s3:DeleteObject",
        "Resource": (
            "arn:aws:s3:::elastic-agent-results-297645381734/"
            "jobs/.elastic-agent-checkpoints/*"
        ),
    }
    assert "s3:DeleteObject" not in statements["ReadAndWriteResults"]["Action"]

    cutover = IAM_CUTOVER.read_text(encoding="utf-8")
    assert (
        "ManagedBy,Name,ElasticAgentJob,ElasticAgentShardIndex,"
        "ElasticAgentController"
    ) in cutover
    assert (
        "jobs/.elastic-agent-checkpoints/job-test/"
        "checkpoint-blobs/deadbeef"
    ) in cutover
    assert "jobs/job-test/result.json" in cutover


def test_worker_policy_reads_only_datasets_and_writes_results():
    policy = json.loads(WORKER_POLICY.read_text(encoding="utf-8"))
    statements = {statement["Sid"]: statement for statement in policy["Statement"]}

    assert statements["LocateResultsBucket"] == {
        "Sid": "LocateResultsBucket",
        "Effect": "Allow",
        "Action": "s3:GetBucketLocation",
        "Resource": "arn:aws:s3:::elastic-agent-results-297645381734",
    }
    assert statements["ReadOnlyJobDatasets"] == {
        "Sid": "ReadOnlyJobDatasets",
        "Effect": "Allow",
        "Action": "s3:GetObject",
        "Resource": (
            "arn:aws:s3:::elastic-agent-results-297645381734/jobs/datasets/*"
        ),
    }
    assert set(statements["WriteOnlyResultsObjects"]["Action"]) == {
        "s3:PutObject",
        "s3:AbortMultipartUpload",
    }
    assert statements["WriteOnlyResultsObjects"]["Resource"] == (
        "arn:aws:s3:::elastic-agent-results-297645381734/jobs/*"
    )


def test_results_bucket_policy_denies_plaintext_transport():
    policy = json.loads(RESULTS_BUCKET_POLICY.read_text(encoding="utf-8"))

    assert policy["Statement"] == [{
        "Sid": "DenyInsecureTransport",
        "Effect": "Deny",
        "Principal": "*",
        "Action": "s3:*",
        "Resource": [
            "arn:aws:s3:::elastic-agent-results-297645381734",
            "arn:aws:s3:::elastic-agent-results-297645381734/*",
        ],
        "Condition": {"Bool": {"aws:SecureTransport": "false"}},
    }]


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
    assert config.batch_runtime.worker_concurrency == 8
    assert config.batch_runtime.collect_concurrency == 8
    assert config.batch_runtime.collect_jitter_ratio == 0.1
    assert config.results.s3_periodic_enabled is True
    assert settings.expected_role_name == "elastic-agent-manager"
    assert settings.public_origin == "https://manager.example"
    assert config.worker.ssh_user == "ubuntu"
    assert config.registry.path == str(tmp_path / "state" / "registry.json")
    assert config.task_registry.path == str(tmp_path / "state" / "task_registry.json")
    assert config.logging.operations_log == str(tmp_path / "state" / "operations.log")
    assert config.webhook.dead_letter_path == str(
        tmp_path / "state" / "webhook_dead_letters.json"
    )
    assert settings.results_s3_bucket == "elastic-agent-results-example"
    assert settings.results_s3_prefix == "jobs"
    assert settings.results_s3_interval == 60


def test_load_settings_accepts_800_instances_and_rejects_801(tmp_path):
    environ = _environment(tmp_path)
    environ["ELASTIC_AGENT_AWS_MAX_INSTANCES"] = "800"
    assert load_settings(environ).max_instances == 800

    environ["ELASTIC_AGENT_AWS_MAX_INSTANCES"] = "801"
    with pytest.raises(
        LauncherConfigurationError,
        match="ELASTIC_AGENT_AWS_MAX_INSTANCES must be between 1 and 800",
    ):
        load_settings(environ)


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("ELASTIC_AGENT_WORKER_BRINGUP_CONCURRENCY", "65"),
        ("ELASTIC_AGENT_PERIODIC_COLLECT_CONCURRENCY", "65"),
        ("ELASTIC_AGENT_PERIODIC_COLLECT_JITTER_RATIO", "1.01"),
    ],
)
def test_load_settings_rejects_unbounded_manager_concurrency(
    tmp_path, name, value,
):
    environ = _environment(tmp_path)
    environ[name] = value
    with pytest.raises(LauncherConfigurationError, match=name):
        load_settings(environ)


def test_build_application_requires_enabled_admin_and_pins_public_origin(
    tmp_path, monkeypatch
):
    settings = load_settings(_environment(tmp_path))
    state_file = settings.state_dir / "management-users.json"
    settings.framework_src.mkdir(parents=True)
    settings.ssh_key_path.write_text("test-only-key", encoding="utf-8")
    settings.ssh_key_path.chmod(0o600)
    monkeypatch.setattr(
        "deploy.aws_manager.validate_worker_ami",
        lambda _settings: SimpleNamespace(
            image_id=_settings.ami_id,
            provenance="unit-test",
            break_glass_used=False,
        ),
    )

    with pytest.raises(LauncherConfigurationError, match="management user store"):
        build_application(settings)

    ManagementUserStore(state_file).upsert_user(
        "launcher-owner@example.test",
        "temporary-launcher-passphrase",
        must_change_password=True,
    )
    try:
        app = build_application(settings)
        assert app is not None
        assert configured_public_origin() == "https://manager.example"
    finally:
        reset_management_auth()


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
        "ELASTIC_AGENT_AWS_EXPECTED_ROLE_NAME",
        "ELASTIC_AGENT_WORKER_BRINGUP_CONCURRENCY",
        "ELASTIC_AGENT_PERIODIC_COLLECT_CONCURRENCY",
        "ELASTIC_AGENT_PERIODIC_COLLECT_JITTER_RATIO",
        "ELASTIC_AGENT_STATE_DIR",
        "ELASTIC_AGENT_MANAGER_URL",
        "ELASTIC_AGENT_PUBLIC_ORIGIN",
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


@pytest.mark.parametrize(
    "origin",
    [
        "http://manager.example",
        "https://user:password@manager.example",
        "https://manager.example/path",
        "https://manager.example?query=value",
        "https://manager.example#fragment",
        "https://manager.example:",
        "https://manager.example:invalid",
    ],
)
def test_load_settings_requires_clean_https_public_origin(tmp_path, origin):
    environ = _environment(tmp_path)
    environ["ELASTIC_AGENT_PUBLIC_ORIGIN"] = origin

    with pytest.raises(LauncherConfigurationError, match="ELASTIC_AGENT_PUBLIC_ORIGIN"):
        load_settings(environ)


def test_settings_repr_does_not_contain_external_api_key(tmp_path):
    environ = _environment(tmp_path)
    environ["ELASTIC_AGENT_EXTERNAL_API_KEYS"] = "must-never-be-rendered"

    settings = load_settings(environ)

    assert "must-never-be-rendered" not in repr(settings)


@pytest.mark.parametrize(
    "name",
    [
        "AWS_ACCESS_KEY_ID",
        "AWS_PROFILE",
        "AWS_WEB_IDENTITY_TOKEN_FILE",
        "AWS_CONTAINER_CREDENTIALS_FULL_URI",
        "AWS_EC2_METADATA_SERVICE_ENDPOINT",
        "AWS_ENDPOINT_URL_STS",
    ],
)
def test_load_settings_rejects_alternate_aws_credential_sources(
    tmp_path, name,
):
    environ = _environment(tmp_path)
    environ[name] = "must-not-be-used"

    with pytest.raises(LauncherConfigurationError, match=name):
        load_settings(environ)


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("AWS_SHARED_CREDENTIALS_FILE", "~/.aws/credentials"),
        ("AWS_CONFIG_FILE", "~/.aws/config"),
        ("BOTO_CONFIG", "~/.boto"),
        ("AWS_EC2_METADATA_DISABLED", "true"),
        ("AWS_EC2_METADATA_V1_DISABLED", "false"),
    ],
)
def test_load_settings_requires_imdsv2_only_credential_chain(
    tmp_path, name, value,
):
    environ = _environment(tmp_path)
    environ[name] = value

    with pytest.raises(LauncherConfigurationError, match=name):
        load_settings(environ)


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
        return {
            "Account": CALLER_ACCOUNT,
            "Arn": (
                f"arn:aws:sts::{CALLER_ACCOUNT}:assumed-role/"
                "elastic-agent-manager/i-0123456789abcdef0"
            ),
        }


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


class _WrongRoleSTSClient:
    def get_caller_identity(self):
        return {
            "Account": CALLER_ACCOUNT,
            "Arn": (
                f"arn:aws:sts::{CALLER_ACCOUNT}:assumed-role/"
                "shared-administrator/i-0123456789abcdef0"
            ),
        }


def test_validate_worker_ami_requires_expected_manager_role(tmp_path):
    settings = load_settings(_environment(tmp_path))

    with pytest.raises(LauncherConfigurationError, match="expected Manager"):
        validate_worker_ami(
            settings,
            ec2_client=_EC2Client([_golden_image()]),
            sts_client=_WrongRoleSTSClient(),
        )


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
