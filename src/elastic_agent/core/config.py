"""Configuration models for Elastic-Agent framework."""

from __future__ import annotations

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings


class ServerConfig(BaseSettings):
    host: str = "0.0.0.0"
    port: int = 8000


class AliyunProviderConfig(BaseSettings):
    region_id: str = "cn-hangzhou"
    image_id: str = ""
    instance_type: str = "ecs.c6.large"
    security_group_id: str = ""
    vswitch_id: str = ""
    key_pair_name: str = "elastic-agent-key"
    ssh_key_path: str = "~/.ssh/elastic-agent-aliyun.pem"
    max_instances: int = 30
    spot_enabled: bool = False


class AWSProviderConfig(BaseSettings):
    region: str = "ap-northeast-1"
    ami_id: str = ""
    default_instance_type: str = "t3.large"
    security_group_ids: list[str] = Field(default_factory=list)
    subnet_id: str = ""
    key_pair_name: str = "elastic-agent-key"
    ssh_key_path: str = "~/.ssh/elastic-agent-aws.pem"
    max_instances: int = 30
    # IAM instance profile attached to worker EC2s so they can reach S3
    # directly (pull datasets, push results) without the Manager relaying data.
    # Empty → no profile attached (workers have no cloud creds).
    worker_instance_profile: str = ""


class ProviderConfig(BaseSettings):
    type: str = "aliyun"
    aliyun: AliyunProviderConfig = Field(default_factory=AliyunProviderConfig)
    aws: AWSProviderConfig = Field(default_factory=AWSProviderConfig)


class WorkerConfig(BaseSettings):
    ssh_user: str = "root"
    runtime_port: int = 8080
    heartbeat_interval: int = 30
    unhealthy_threshold: int = 3


class BootstrapConfig(BaseSettings):
    default_step_timeout: int = 300
    max_retries: int = 2
    failure_strategy: str = "terminate_and_retry"


class CredentialConfig(BaseSettings):
    accounts_file: str = "~/.elastic-agent/accounts.json"
    pool_status_file: str = "~/.elastic-agent/pool_status.json"
    quota_threshold: float = 0.85
    quota_check_delay_min: int = 170
    quota_check_delay_max: int = 190
    weekly_reserve_per_day: int = 0
    rotation_strategy: str = "least_used_first"
    login_timeout: int = 240
    max_accounts_per_worker: int = 4
    # Extra pip packages for worker-local login; the base deps (google-chrome,
    # xvfb, xdotool, httpx, websockets) are always installed by
    # credential_login_deps_step. Empty by default — the vendored CCM login flow
    # needs no Playwright/mitmproxy.
    login_dependencies: list[str] = Field(default_factory=list)


class ExternalAPIConfig(BaseSettings):
    enabled: bool = True
    trace_buffer_size: int = 5000


class LoggingConfig(BaseSettings):
    operations_log: str = "~/.elastic-agent/operations.log"
    log_level: str = "INFO"
    rotation: str = "daily"
    retention_days: int = 30
    worker_process_log_dir: str = "logs/"


class MonitorConfig(BaseSettings):
    health_check_interval: int = 30
    reconcile_interval: int = 300


class DrainConfig(BaseSettings):
    timeout: int = 3600


class RegistryConfig(BaseSettings):
    path: str = "~/.elastic-agent/registry.json"


class TaskRegistryConfig(BaseSettings):
    path: str = "~/.elastic-agent/task_registry.json"


class WebhookConfig(BaseSettings):
    retry_delays: list[float] = Field(default_factory=lambda: [1, 5, 30, 300, 1800])
    send_timeout: float = 10.0
    dead_letter_path: str = "~/.elastic-agent/webhook_dead_letters.json"


class ElasticAgentConfig(BaseSettings):
    server: ServerConfig = Field(default_factory=ServerConfig)
    provider: ProviderConfig = Field(default_factory=ProviderConfig)
    worker: WorkerConfig = Field(default_factory=WorkerConfig)
    bootstrap: BootstrapConfig = Field(default_factory=BootstrapConfig)
    credentials: CredentialConfig = Field(default_factory=CredentialConfig)
    external_api: ExternalAPIConfig = Field(default_factory=ExternalAPIConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    monitor: MonitorConfig = Field(default_factory=MonitorConfig)
    drain: DrainConfig = Field(default_factory=DrainConfig)
    registry: RegistryConfig = Field(default_factory=RegistryConfig)
    task_registry: TaskRegistryConfig = Field(default_factory=TaskRegistryConfig)
    webhook: WebhookConfig = Field(default_factory=WebhookConfig)

    @classmethod
    def from_yaml(cls, path: str | Path) -> ElasticAgentConfig:
        import yaml

        with open(path) as f:
            data = yaml.safe_load(f) or {}
        return cls(**data)
