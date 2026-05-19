"""Shared test fixtures for elastic-agent tests."""

import pytest


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
