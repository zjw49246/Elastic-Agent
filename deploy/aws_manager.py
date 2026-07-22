"""Production AWS Manager launcher.

This entry point intentionally contains no deployment-specific identifiers or
credential discovery fallbacks.  A systemd ``EnvironmentFile`` supplies every
cloud/network/state setting, while boto3 uses the Manager instance profile.
"""

from __future__ import annotations

import logging
import os
import re
import stat
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from elastic_agent.api.app import create_app
from elastic_agent.api.auth import reset_api_keys
from elastic_agent.core.config import (
    AWSProviderConfig,
    ElasticAgentConfig,
    LoggingConfig,
    ProviderConfig,
    RegistryConfig,
    ServerConfig,
    TaskRegistryConfig,
    WorkerConfig,
)
from elastic_agent.core.providers.aws import AWSProvider
from elastic_agent.manager.manager import ElasticAgentManager

logger = logging.getLogger(__name__)

CANONICAL_OWNER_ID = "099720109477"
GOLDEN_IMAGE_TAGS = {
    "ManagedBy": "elastic-agent",
    "Role": "worker-golden",
}

_AWS_ID_RE = re.compile(r"^[a-z]+-[0-9a-f]{8,17}$")
_REGION_RE = re.compile(r"^[a-z]{2}(?:-gov)?-[a-z]+-\d$")
_LOG_LEVELS = {"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"}


class LauncherConfigurationError(ValueError):
    """A production launcher setting or AMI invariant is invalid."""


@dataclass(frozen=True, slots=True)
class AWSManagerSettings:
    """Non-secret production settings loaded from the process environment."""

    region: str
    ami_id: str
    instance_type: str
    worker_security_group_ids: tuple[str, ...]
    subnet_id: str
    key_pair_name: str
    ssh_key_path: Path
    worker_instance_profile: str
    max_instances: int
    state_dir: Path
    manager_url: str
    framework_src: Path
    server_host: str
    server_port: int
    worker_ssh_user: str
    log_level: str
    results_s3_bucket: str
    results_s3_prefix: str
    results_s3_interval: float
    allow_canonical_base_ami: bool = False


@dataclass(frozen=True, slots=True)
class ImageValidationResult:
    """Safe-to-log result of the startup AMI provenance check."""

    image_id: str
    owner_id: str
    provenance: str
    break_glass_used: bool


def _required(environ: Mapping[str, str], name: str) -> str:
    value = environ.get(name, "").strip()
    if not value:
        raise LauncherConfigurationError(f"required environment variable {name} is missing")
    return value


def _positive_int(environ: Mapping[str, str], name: str, *, maximum: int) -> int:
    raw = _required(environ, name)
    try:
        value = int(raw)
    except ValueError as exc:
        raise LauncherConfigurationError(f"{name} must be an integer") from exc
    if value < 1 or value > maximum:
        raise LauncherConfigurationError(f"{name} must be between 1 and {maximum}")
    return value


def _positive_float(environ: Mapping[str, str], name: str, *, maximum: float) -> float:
    raw = _required(environ, name)
    try:
        value = float(raw)
    except ValueError as exc:
        raise LauncherConfigurationError(f"{name} must be a number") from exc
    if value <= 0 or value > maximum:
        raise LauncherConfigurationError(f"{name} must be greater than 0 and at most {maximum:g}")
    return value


def _boolean(environ: Mapping[str, str], name: str, *, default: bool = False) -> bool:
    raw = environ.get(name)
    if raw is None or not raw.strip():
        return default
    normalized = raw.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise LauncherConfigurationError(f"{name} must be true or false")


def _absolute_path(environ: Mapping[str, str], name: str) -> Path:
    path = Path(_required(environ, name)).expanduser()
    if not path.is_absolute():
        raise LauncherConfigurationError(f"{name} must be an absolute path")
    return path


def _validate_manager_url(value: str) -> None:
    parsed = urlsplit(value)
    if parsed.scheme != "wss" or not parsed.hostname:
        raise LauncherConfigurationError("ELASTIC_AGENT_MANAGER_URL must be an absolute wss:// URL")
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise LauncherConfigurationError("ELASTIC_AGENT_MANAGER_URL must not contain credentials, query, or fragment")
    if parsed.path.rstrip("/") != "/ws/runtime":
        raise LauncherConfigurationError("ELASTIC_AGENT_MANAGER_URL must end with /ws/runtime")


def _validate_api_key_presence(environ: Mapping[str, str]) -> None:
    # Deliberately validate presence only.  Secret values never enter the
    # settings dataclass, exceptions, or startup logs.
    raw = environ.get("ELASTIC_AGENT_EXTERNAL_API_KEYS", "")
    if not any(part.strip() for part in raw.split(",")):
        raise LauncherConfigurationError("required environment variable ELASTIC_AGENT_EXTERNAL_API_KEYS is missing")


def load_settings(environ: Mapping[str, str] | None = None) -> AWSManagerSettings:
    """Parse production configuration without mutating ``os.environ``."""

    source = os.environ if environ is None else environ
    _validate_api_key_presence(source)

    region = _required(source, "ELASTIC_AGENT_AWS_REGION")
    if not _REGION_RE.fullmatch(region):
        raise LauncherConfigurationError("ELASTIC_AGENT_AWS_REGION is invalid")

    ami_id = _required(source, "ELASTIC_AGENT_AWS_AMI_ID")
    if not _AWS_ID_RE.fullmatch(ami_id) or not ami_id.startswith("ami-"):
        raise LauncherConfigurationError("ELASTIC_AGENT_AWS_AMI_ID is invalid")

    subnet_id = _required(source, "ELASTIC_AGENT_AWS_SUBNET_ID")
    if not _AWS_ID_RE.fullmatch(subnet_id) or not subnet_id.startswith("subnet-"):
        raise LauncherConfigurationError("ELASTIC_AGENT_AWS_SUBNET_ID is invalid")

    raw_groups = _required(source, "ELASTIC_AGENT_AWS_WORKER_SECURITY_GROUP_IDS")
    groups = tuple(dict.fromkeys(item.strip() for item in raw_groups.split(",") if item.strip()))
    if not groups or any(not _AWS_ID_RE.fullmatch(item) or not item.startswith("sg-") for item in groups):
        raise LauncherConfigurationError(
            "ELASTIC_AGENT_AWS_WORKER_SECURITY_GROUP_IDS must contain AWS security-group IDs"
        )

    manager_url = _required(source, "ELASTIC_AGENT_MANAGER_URL")
    _validate_manager_url(manager_url)

    log_level = _required(source, "ELASTIC_AGENT_LOG_LEVEL").upper()
    if log_level not in _LOG_LEVELS:
        raise LauncherConfigurationError("ELASTIC_AGENT_LOG_LEVEL must be one of CRITICAL, ERROR, WARNING, INFO, DEBUG")

    return AWSManagerSettings(
        region=region,
        ami_id=ami_id,
        instance_type=_required(source, "ELASTIC_AGENT_AWS_INSTANCE_TYPE"),
        worker_security_group_ids=groups,
        subnet_id=subnet_id,
        key_pair_name=_required(source, "ELASTIC_AGENT_AWS_KEY_PAIR_NAME"),
        ssh_key_path=_absolute_path(source, "ELASTIC_AGENT_AWS_SSH_KEY_PATH"),
        worker_instance_profile=_required(source, "ELASTIC_AGENT_AWS_WORKER_INSTANCE_PROFILE"),
        max_instances=_positive_int(source, "ELASTIC_AGENT_AWS_MAX_INSTANCES", maximum=100),
        state_dir=_absolute_path(source, "ELASTIC_AGENT_STATE_DIR"),
        manager_url=manager_url,
        framework_src=_absolute_path(source, "ELASTIC_AGENT_FRAMEWORK_SRC"),
        server_host=_required(source, "ELASTIC_AGENT_SERVER_HOST"),
        server_port=_positive_int(source, "ELASTIC_AGENT_SERVER_PORT", maximum=65535),
        worker_ssh_user=_required(source, "ELASTIC_AGENT_WORKER_SSH_USER"),
        log_level=log_level,
        results_s3_bucket=_required(source, "ELASTIC_AGENT_RESULTS_S3_BUCKET"),
        results_s3_prefix=_required(source, "ELASTIC_AGENT_RESULTS_S3_PREFIX"),
        results_s3_interval=_positive_float(source, "ELASTIC_AGENT_RESULTS_S3_INTERVAL", maximum=86400),
        allow_canonical_base_ami=_boolean(source, "ELASTIC_AGENT_ALLOW_CANONICAL_BASE_AMI"),
    )


def build_config(settings: AWSManagerSettings) -> ElasticAgentConfig:
    """Build the framework config from an already validated environment."""

    aws = AWSProviderConfig(
        region=settings.region,
        ami_id=settings.ami_id,
        default_instance_type=settings.instance_type,
        security_group_ids=list(settings.worker_security_group_ids),
        subnet_id=settings.subnet_id,
        key_pair_name=settings.key_pair_name,
        ssh_key_path=str(settings.ssh_key_path),
        max_instances=settings.max_instances,
        worker_instance_profile=settings.worker_instance_profile,
    )
    return ElasticAgentConfig(
        server=ServerConfig(host=settings.server_host, port=settings.server_port),
        provider=ProviderConfig(type="aws", aws=aws),
        worker=WorkerConfig(ssh_user=settings.worker_ssh_user),
        registry=RegistryConfig(path=str(settings.state_dir / "registry.json")),
        task_registry=TaskRegistryConfig(path=str(settings.state_dir / "task_registry.json")),
        logging=LoggingConfig(
            operations_log=str(settings.state_dir / "operations.log"),
            log_level=settings.log_level,
        ),
    )


def prepare_local_paths(settings: AWSManagerSettings) -> None:
    """Create/check local production paths before constructing the Manager."""

    if settings.state_dir.is_symlink():
        raise LauncherConfigurationError("ELASTIC_AGENT_STATE_DIR must not be a symlink")
    settings.state_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    settings.state_dir.chmod(0o700)

    if not settings.framework_src.is_dir():
        raise LauncherConfigurationError("ELASTIC_AGENT_FRAMEWORK_SRC must be an existing directory")
    if not settings.ssh_key_path.is_file():
        raise LauncherConfigurationError("ELASTIC_AGENT_AWS_SSH_KEY_PATH must be an existing regular file")
    if not os.access(settings.ssh_key_path, os.R_OK):
        raise LauncherConfigurationError("ELASTIC_AGENT_AWS_SSH_KEY_PATH must be readable by the Manager user")
    key_mode = stat.S_IMODE(settings.ssh_key_path.stat().st_mode)
    if key_mode & 0o077:
        raise LauncherConfigurationError("ELASTIC_AGENT_AWS_SSH_KEY_PATH must not be group/world accessible")


def _image_tags(image: Mapping[str, Any]) -> dict[str, str]:
    return {
        str(tag.get("Key")): str(tag.get("Value"))
        for tag in image.get("Tags", []) or []
        if tag.get("Key") is not None and tag.get("Value") is not None
    }


def _root_snapshot_encrypted(image: Mapping[str, Any]) -> bool:
    root_device = image.get("RootDeviceName")
    for mapping in image.get("BlockDeviceMappings", []) or []:
        if mapping.get("DeviceName") != root_device:
            continue
        ebs = mapping.get("Ebs") or {}
        return bool(ebs.get("SnapshotId")) and ebs.get("Encrypted") is True
    return False


def validate_image_description(
    image: Mapping[str, Any],
    *,
    caller_account_id: str,
    allow_canonical_base_ami: bool = False,
) -> ImageValidationResult:
    """Enforce the worker AMI's runtime, encryption, and provenance policy.

    Normal production images must be encrypted, owned by the current AWS
    account, and carry both golden-image tags.  Canonical's official publisher
    image is an emergency-only exception selected with an explicit environment
    opt-in.  That exception also permits Canonical's unencrypted public source
    snapshot; ``AWSProvider`` still requests an encrypted worker root volume.
    """

    image_id = str(image.get("ImageId") or "<unknown>")
    invariants = (
        (image.get("State") == "available", "state must be available"),
        (image.get("Architecture") == "x86_64", "architecture must be x86_64"),
        (image.get("VirtualizationType") == "hvm", "virtualization must be HVM"),
        (image.get("EnaSupport") is True, "ENA support must be enabled"),
        (image.get("ImdsSupport") == "v2.0", "AMI must declare IMDSv2 support"),
    )
    for valid, reason in invariants:
        if not valid:
            raise LauncherConfigurationError(f"worker AMI {image_id}: {reason}")

    owner_id = str(image.get("OwnerId") or "")
    if owner_id == caller_account_id:
        tags = _image_tags(image)
        missing = [f"{key}={value}" for key, value in GOLDEN_IMAGE_TAGS.items() if tags.get(key) != value]
        if missing:
            raise LauncherConfigurationError(
                f"worker AMI {image_id}: self-owned image is missing required golden tags: " + ", ".join(missing)
            )
        if not _root_snapshot_encrypted(image):
            raise LauncherConfigurationError(f"worker AMI {image_id}: root snapshot must be encrypted")
        return ImageValidationResult(
            image_id=image_id,
            owner_id=owner_id,
            provenance="self-owned-golden",
            break_glass_used=False,
        )

    if owner_id == CANONICAL_OWNER_ID and allow_canonical_base_ami:
        return ImageValidationResult(
            image_id=image_id,
            owner_id=owner_id,
            provenance="canonical-break-glass",
            break_glass_used=True,
        )

    if owner_id == CANONICAL_OWNER_ID:
        raise LauncherConfigurationError(
            f"worker AMI {image_id}: Canonical base-image rollback requires ELASTIC_AGENT_ALLOW_CANONICAL_BASE_AMI=true"
        )
    raise LauncherConfigurationError(
        f"worker AMI {image_id}: owner is neither this AWS account nor approved Canonical break-glass"
    )


def validate_worker_ami(
    settings: AWSManagerSettings,
    *,
    ec2_client: Any | None = None,
    sts_client: Any | None = None,
) -> ImageValidationResult:
    """Fetch and validate the configured AMI before any Manager lifecycle starts."""

    if ec2_client is None or sts_client is None:
        import boto3

        if ec2_client is None:
            ec2_client = boto3.client("ec2", region_name=settings.region)
        if sts_client is None:
            sts_client = boto3.client("sts", region_name=settings.region)

    response = ec2_client.describe_images(ImageIds=[settings.ami_id])
    images = response.get("Images", [])
    if len(images) != 1:
        raise LauncherConfigurationError(f"worker AMI {settings.ami_id}: DescribeImages returned no unique image")
    if images[0].get("ImageId") != settings.ami_id:
        raise LauncherConfigurationError(f"worker AMI {settings.ami_id}: DescribeImages returned a different image")
    caller_account_id = str(sts_client.get_caller_identity().get("Account") or "")
    if not caller_account_id:
        raise LauncherConfigurationError("AWS caller identity has no account ID")
    return validate_image_description(
        images[0],
        caller_account_id=caller_account_id,
        allow_canonical_base_ami=settings.allow_canonical_base_ami,
    )


def build_application(settings: AWSManagerSettings):
    """Validate startup invariants and create the ASGI application."""

    prepare_local_paths(settings)
    result = validate_worker_ami(settings)
    if result.break_glass_used:
        logger.warning(
            "Worker AMI %s is using the explicit Canonical break-glass path; "
            "restore an encrypted, tagged golden image as soon as possible",
            result.image_id,
        )
    else:
        logger.info("Validated worker AMI %s (%s)", result.image_id, result.provenance)

    config = build_config(settings)
    reset_api_keys()
    manager = ElasticAgentManager(config, AWSProvider(config.provider.aws))
    return create_app(manager)


def main() -> None:
    """Run one production Manager process under systemd."""

    settings = load_settings()
    logging.basicConfig(
        level=getattr(logging, settings.log_level),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    app = build_application(settings)

    import uvicorn

    uvicorn.run(
        app,
        host=settings.server_host,
        port=settings.server_port,
        log_level=settings.log_level.lower(),
        workers=1,
    )


if __name__ == "__main__":
    main()
