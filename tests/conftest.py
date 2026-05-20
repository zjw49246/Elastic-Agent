"""Shared test fixtures for elastic-agent tests.

T-206: Environment-variable-driven provider/worker/oss fixture switching.
Set EA_TEST_LEVEL to control which fixtures use real vs mock implementations:

    EA_TEST_LEVEL=0  Pure unit tests (no fixtures)
    EA_TEST_LEVEL=1  DryRunProvider + MockWorker + MockOSS (default)
    EA_TEST_LEVEL=2  Real cloud provider + MockWorker + MockOSS
    EA_TEST_LEVEL=3  Real cloud + LocalWorkerProcess + MockOSS
    EA_TEST_LEVEL=4  Real cloud + real Worker + real Claude CLI + MockOSS
    EA_TEST_LEVEL=5  Real cloud + real Worker + MockOSS (multi-worker)
    EA_TEST_LEVEL=6  All real: cloud + Worker + Claude + OSS
"""

from __future__ import annotations

import os
import shutil

import pytest

_LEVEL_MARKERS = {"level0", "level1", "level2", "level3", "level4", "level5", "level6"}


def get_test_level() -> int:
    return int(os.environ.get("EA_TEST_LEVEL", "1"))


def pytest_collection_modifyitems(config, items):
    """T-207: Auto-skip tests whose level marker exceeds EA_TEST_LEVEL.

    Tests marked with @pytest.mark.level3 will be skipped when EA_TEST_LEVEL < 3.
    Unmarked tests always run (treated as level0).
    """
    max_level = get_test_level()
    for item in items:
        for marker in item.iter_markers():
            if marker.name in _LEVEL_MARKERS:
                required = int(marker.name.replace("level", ""))
                if required > max_level:
                    item.add_marker(pytest.mark.skip(
                        reason=f"Requires EA_TEST_LEVEL>={required}, current={max_level}"
                    ))


# ---------------------------------------------------------------------------
# Basic fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def sample_instance_config():
    from elastic_agent.core.providers.base import InstanceConfig

    return InstanceConfig(
        instance_type="ecs.c6.large",
        image_id="m-test-image",
        key_pair_name="test-key",
        security_group_id="sg-test",
        vswitch_id="vsw-test",
        tags={"env": "test"},
    )


@pytest.fixture
def tmp_test_dir(tmp_path):
    """Provide a temporary directory for test state files."""
    return tmp_path


# ---------------------------------------------------------------------------
# Provider fixture — switches between DryRun and real cloud providers
# ---------------------------------------------------------------------------


@pytest.fixture
def provider():
    """Return a cloud provider appropriate for the current test level.

    Level 0-1: DryRunProvider (in-memory)
    Level 2+: Real AliyunProvider or AWSProvider (requires credentials)
    """
    level = get_test_level()

    if level >= 2 and os.environ.get("ALICLOUD_ACCESS_KEY_ID"):
        from elastic_agent.core.config import ElasticAgentConfig
        from elastic_agent.core.providers.aliyun import AliyunProvider

        config = ElasticAgentConfig(
            provider={
                "type": "aliyun",
                "aliyun": {
                    "region_id": os.environ.get("EA_ALIYUN_REGION", "cn-hangzhou"),
                    "image_id": os.environ.get("EA_ALIYUN_IMAGE_ID", ""),
                    "instance_type": os.environ.get("EA_ALIYUN_INSTANCE_TYPE", "ecs.c6.large"),
                    "security_group_id": os.environ.get("EA_ALIYUN_SG_ID", ""),
                    "vswitch_id": os.environ.get("EA_ALIYUN_VSWITCH_ID", ""),
                    "key_pair_name": os.environ.get("EA_ALIYUN_KEY_PAIR", ""),
                },
            },
        )
        return AliyunProvider(config.provider.aliyun)
    elif level >= 2 and os.environ.get("AWS_ACCESS_KEY_ID"):
        from elastic_agent.core.config import ElasticAgentConfig
        from elastic_agent.core.providers.aws import AWSProvider

        config = ElasticAgentConfig(
            provider={
                "type": "aws",
                "aws": {
                    "region": os.environ.get("EA_AWS_REGION", "ap-northeast-1"),
                    "ami_id": os.environ.get("EA_AWS_AMI_ID", ""),
                    "default_instance_type": os.environ.get("EA_AWS_INSTANCE_TYPE", "t3.large"),
                    "security_group_ids": os.environ.get("EA_AWS_SG_IDS", "").split(","),
                    "subnet_id": os.environ.get("EA_AWS_SUBNET_ID", ""),
                    "key_pair_name": os.environ.get("EA_AWS_KEY_PAIR", ""),
                },
            },
        )
        return AWSProvider(config.provider.aws)

    from elastic_agent.testing import DryRunProvider

    return DryRunProvider()


# ---------------------------------------------------------------------------
# Worker fixture — switches between MockWorker and LocalWorkerProcess
# ---------------------------------------------------------------------------


@pytest.fixture
def worker_factory():
    """Return a factory function that creates workers for the current test level.

    Level 0-2: MockWorker (simulated WebSocket)
    Level 3+: LocalWorkerProcess (real subprocess)
    """
    level = get_test_level()

    if level >= 3:
        from elastic_agent.testing import LocalWorkerProcess

        def _create(**kwargs):
            return LocalWorkerProcess(**kwargs)

        return _create

    from elastic_agent.testing import MockWorker

    def _create(*, manager_url: str = "", token: str = "", **kwargs):
        return MockWorker(manager_url=manager_url, token=token, **kwargs)

    return _create


# ---------------------------------------------------------------------------
# OSS/Storage fixture — switches between MockOSS and real cloud storage
# ---------------------------------------------------------------------------


@pytest.fixture
def oss(tmp_test_dir):
    """Return a storage backend for the current test level.

    Level 0-4: MockOSS (local filesystem)
    Level 5+: Real OSS/S3 (requires credentials)
    """
    level = get_test_level()

    if level >= 5 and os.environ.get("ABS_OSS_ACCESS_KEY_ID"):
        pytest.skip("Real OSS integration not yet implemented in fixture")

    from elastic_agent.testing import MockOSS

    return MockOSS(tmp_test_dir / "oss_storage")


# ---------------------------------------------------------------------------
# Claude runner fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def claude_available():
    """Check if real Claude Code CLI is available (for L4+ tests)."""
    level = get_test_level()
    if level >= 4 and shutil.which("claude"):
        return True
    return False


# ---------------------------------------------------------------------------
# Test manager fixture — convenience for component-level tests
# ---------------------------------------------------------------------------


@pytest.fixture
def test_manager(tmp_test_dir):
    """Create a fully mocked test manager.

    Returns a TestManagerResult with .manager, .provider, .oss, .config, .tmp_dir.
    """
    from elastic_agent.testing import create_test_manager

    return create_test_manager(tmp_dir=tmp_test_dir)
