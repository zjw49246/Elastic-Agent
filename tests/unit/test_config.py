"""Tests for Config loading — YAML parsing, env var override, Pydantic validation (T-109)."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from elastic_agent.core.config import (
    AliyunProviderConfig,
    AWSProviderConfig,
    BootstrapConfig,
    CredentialConfig,
    DrainConfig,
    ElasticAgentConfig,
    ExternalAPIConfig,
    LoggingConfig,
    MonitorConfig,
    ProviderConfig,
    RegistryConfig,
    ServerConfig,
    TaskRegistryConfig,
    WebhookConfig,
    WorkerConfig,
)


class TestDefaults:
    def test_server_defaults(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.delenv("HOST", raising=False)
        monkeypatch.delenv("PORT", raising=False)
        cfg = ServerConfig()
        assert cfg.host == "0.0.0.0"
        assert cfg.port == 8000

    def test_provider_defaults(self):
        cfg = ProviderConfig()
        assert cfg.type == "aliyun"
        assert isinstance(cfg.aliyun, AliyunProviderConfig)
        assert isinstance(cfg.aws, AWSProviderConfig)

    def test_aliyun_defaults(self):
        cfg = AliyunProviderConfig()
        assert cfg.region_id == "cn-hangzhou"
        assert cfg.instance_type == "ecs.c6.large"
        assert cfg.max_instances == 30
        assert cfg.spot_enabled is False

    def test_aws_defaults(self):
        cfg = AWSProviderConfig()
        assert cfg.region == "ap-northeast-1"
        assert cfg.default_instance_type == "t3.large"
        assert cfg.max_instances == 30
        assert cfg.security_group_ids == []

    def test_worker_defaults(self):
        cfg = WorkerConfig()
        assert cfg.ssh_user == "root"
        assert cfg.runtime_port == 8080
        assert cfg.heartbeat_interval == 30
        assert cfg.unhealthy_threshold == 3

    def test_bootstrap_defaults(self):
        cfg = BootstrapConfig()
        assert cfg.default_step_timeout == 300
        assert cfg.max_retries == 2
        assert cfg.failure_strategy == "terminate_and_retry"

    def test_credential_defaults(self):
        cfg = CredentialConfig()
        assert cfg.quota_threshold == 0.85
        assert cfg.quota_check_delay_min == 170
        assert cfg.quota_check_delay_max == 190
        assert cfg.rotation_strategy == "least_used_first"
        assert cfg.login_timeout == 240
        assert cfg.max_accounts_per_worker == 4
        # Base login deps (google-chrome/xvfb/xdotool/httpx/websockets) are always
        # installed by credential_login_deps_step; this list is extra pip pkgs only.
        assert cfg.login_dependencies == []

    def test_external_api_defaults(self):
        cfg = ExternalAPIConfig()
        assert cfg.enabled is True
        assert cfg.trace_buffer_size == 5000

    def test_logging_defaults(self):
        cfg = LoggingConfig()
        assert cfg.log_level == "INFO"
        assert cfg.rotation == "daily"
        assert cfg.retention_days == 30

    def test_monitor_defaults(self):
        cfg = MonitorConfig()
        assert cfg.health_check_interval == 30
        assert cfg.reconcile_interval == 300

    def test_drain_defaults(self):
        cfg = DrainConfig()
        assert cfg.timeout == 3600

    def test_webhook_defaults(self):
        cfg = WebhookConfig()
        assert cfg.retry_delays == [1, 5, 30, 300, 1800]
        assert cfg.send_timeout == 10.0

    def test_root_config_defaults(self):
        cfg = ElasticAgentConfig()
        assert isinstance(cfg.server, ServerConfig)
        assert isinstance(cfg.provider, ProviderConfig)
        assert isinstance(cfg.worker, WorkerConfig)
        assert isinstance(cfg.bootstrap, BootstrapConfig)
        assert isinstance(cfg.credentials, CredentialConfig)
        assert isinstance(cfg.external_api, ExternalAPIConfig)
        assert isinstance(cfg.logging, LoggingConfig)
        assert isinstance(cfg.monitor, MonitorConfig)
        assert isinstance(cfg.drain, DrainConfig)
        assert isinstance(cfg.registry, RegistryConfig)
        assert isinstance(cfg.task_registry, TaskRegistryConfig)
        assert isinstance(cfg.webhook, WebhookConfig)


class TestFromYaml:
    def test_parse_full_config(self, tmp_path: Path):
        config_data = {
            "server": {"host": "127.0.0.1", "port": 9000},
            "provider": {
                "type": "aws",
                "aliyun": {"region_id": "cn-shanghai", "max_instances": 10},
                "aws": {"region": "us-east-1", "ami_id": "ami-test123"},
            },
            "worker": {"ssh_user": "ubuntu", "runtime_port": 9090},
            "bootstrap": {
                "default_step_timeout": 600,
                "max_retries": 3,
                "failure_strategy": "retry_from_failed",
            },
            "credentials": {
                "quota_threshold": 0.90,
                "max_accounts_per_worker": 6,
            },
            "drain": {"timeout": 7200},
            "monitor": {"reconcile_interval": 120},
        }
        yaml_path = tmp_path / "config.yaml"
        yaml_path.write_text(yaml.dump(config_data))

        cfg = ElasticAgentConfig.from_yaml(yaml_path)

        assert cfg.server.host == "127.0.0.1"
        assert cfg.server.port == 9000
        assert cfg.provider.type == "aws"
        assert cfg.provider.aliyun.region_id == "cn-shanghai"
        assert cfg.provider.aliyun.max_instances == 10
        assert cfg.provider.aws.region == "us-east-1"
        assert cfg.provider.aws.ami_id == "ami-test123"
        assert cfg.worker.ssh_user == "ubuntu"
        assert cfg.worker.runtime_port == 9090
        assert cfg.bootstrap.default_step_timeout == 600
        assert cfg.bootstrap.max_retries == 3
        assert cfg.bootstrap.failure_strategy == "retry_from_failed"
        assert cfg.credentials.quota_threshold == 0.90
        assert cfg.credentials.max_accounts_per_worker == 6
        assert cfg.drain.timeout == 7200
        assert cfg.monitor.reconcile_interval == 120

    def test_partial_config_uses_defaults(self, tmp_path: Path):
        config_data = {"server": {"port": 8888}}
        yaml_path = tmp_path / "config.yaml"
        yaml_path.write_text(yaml.dump(config_data))

        cfg = ElasticAgentConfig.from_yaml(yaml_path)

        assert cfg.server.port == 8888
        assert cfg.server.host == "0.0.0.0"
        assert cfg.worker.ssh_user == "root"
        assert cfg.bootstrap.max_retries == 2

    def test_empty_yaml_uses_all_defaults(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ):
        monkeypatch.delenv("HOST", raising=False)
        monkeypatch.delenv("PORT", raising=False)
        yaml_path = tmp_path / "config.yaml"
        yaml_path.write_text("")

        cfg = ElasticAgentConfig.from_yaml(yaml_path)

        assert cfg.server.port == 8000
        assert cfg.provider.type == "aliyun"
        assert cfg.drain.timeout == 3600

    def test_nested_override(self, tmp_path: Path):
        config_data = {
            "provider": {
                "type": "aliyun",
                "aliyun": {
                    "region_id": "cn-beijing",
                    "image_id": "m-custom",
                    "instance_type": "ecs.g7.xlarge",
                    "security_group_id": "sg-test",
                    "vswitch_id": "vsw-test",
                    "spot_enabled": True,
                },
            },
        }
        yaml_path = tmp_path / "config.yaml"
        yaml_path.write_text(yaml.dump(config_data))

        cfg = ElasticAgentConfig.from_yaml(yaml_path)

        assert cfg.provider.aliyun.region_id == "cn-beijing"
        assert cfg.provider.aliyun.image_id == "m-custom"
        assert cfg.provider.aliyun.instance_type == "ecs.g7.xlarge"
        assert cfg.provider.aliyun.spot_enabled is True


class TestPydanticValidation:
    def test_invalid_port_type(self, tmp_path: Path):
        config_data = {"server": {"port": "not_a_number"}}
        yaml_path = tmp_path / "config.yaml"
        yaml_path.write_text(yaml.dump(config_data))

        with pytest.raises(Exception):
            ElasticAgentConfig.from_yaml(yaml_path)

    def test_list_fields(self):
        cfg = WebhookConfig(retry_delays=[1.0, 2.0, 5.0])
        assert cfg.retry_delays == [1.0, 2.0, 5.0]

    def test_credential_config_list_field(self):
        cfg = CredentialConfig(login_dependencies=["custom-tool"])
        assert cfg.login_dependencies == ["custom-tool"]

    def test_aws_security_group_ids(self):
        cfg = AWSProviderConfig(security_group_ids=["sg-1", "sg-2"])
        assert cfg.security_group_ids == ["sg-1", "sg-2"]

    def test_explicit_construction(self):
        cfg = ElasticAgentConfig(
            server=ServerConfig(port=3000),
            drain=DrainConfig(timeout=1800),
        )
        assert cfg.server.port == 3000
        assert cfg.drain.timeout == 1800
        assert cfg.worker.ssh_user == "root"


class TestConfigOverridePatterns:
    def test_server_environment_override(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("HOST", "127.0.0.1")
        monkeypatch.setenv("PORT", "8123")

        cfg = ServerConfig()

        assert cfg.host == "127.0.0.1"
        assert cfg.port == 8123

    def test_yaml_with_all_sections(self, tmp_path: Path):
        config_data = {
            "server": {"host": "0.0.0.0", "port": 8000},
            "provider": {"type": "dryrun"},
            "worker": {"ssh_user": "root", "runtime_port": 8080, "heartbeat_interval": 15, "unhealthy_threshold": 5},
            "bootstrap": {"default_step_timeout": 120, "max_retries": 1, "failure_strategy": "leave_for_debug"},
            "credentials": {
                "accounts_file": "/tmp/accounts.json",
                "pool_status_file": "/tmp/pool.json",
                "quota_threshold": 0.70,
                "login_timeout": 180,
            },
            "external_api": {"enabled": False, "trace_buffer_size": 1000},
            "logging": {"log_level": "DEBUG", "rotation": "daily", "retention_days": 7},
            "monitor": {"health_check_interval": 10, "reconcile_interval": 60},
            "drain": {"timeout": 600},
            "registry": {"path": "/tmp/registry.json"},
            "task_registry": {"path": "/tmp/tasks.json"},
            "webhook": {"retry_delays": [1, 2, 3], "send_timeout": 5.0, "dead_letter_path": "/tmp/dead.json"},
        }
        yaml_path = tmp_path / "config.yaml"
        yaml_path.write_text(yaml.dump(config_data))

        cfg = ElasticAgentConfig.from_yaml(yaml_path)

        assert cfg.provider.type == "dryrun"
        assert cfg.worker.heartbeat_interval == 15
        assert cfg.worker.unhealthy_threshold == 5
        assert cfg.bootstrap.failure_strategy == "leave_for_debug"
        assert cfg.credentials.quota_threshold == 0.70
        assert cfg.external_api.enabled is False
        assert cfg.external_api.trace_buffer_size == 1000
        assert cfg.logging.log_level == "DEBUG"
        assert cfg.logging.retention_days == 7
        assert cfg.monitor.health_check_interval == 10
        assert cfg.drain.timeout == 600
        assert cfg.registry.path == "/tmp/registry.json"
        assert cfg.task_registry.path == "/tmp/tasks.json"
        assert cfg.webhook.retry_delays == [1, 2, 3]
        assert cfg.webhook.send_timeout == 5.0
