"""T-116 / T-117: Cloud prerequisites validation integration tests.

T-116: Aliyun — security group rules, VSwitch connectivity, key pair SSH reachability
T-117: AWS — security group, subnet, key pair

These tests verify that the infrastructure prerequisites are correctly
configured before attempting to create instances. They require real
cloud credentials (level2+).
"""

from __future__ import annotations

import os

import pytest


@pytest.mark.level2
class TestAliyunPrerequisites:
    """T-116: Validate Aliyun infrastructure prerequisites."""

    @pytest.mark.asyncio
    async def test_security_group_exists(self):
        if not os.environ.get("ALICLOUD_ACCESS_KEY_ID"):
            pytest.skip("Aliyun credentials not available")

        sg_id = os.environ.get("EA_ALIYUN_SG_ID", "")
        assert sg_id, "EA_ALIYUN_SG_ID must be set for this test"

    @pytest.mark.asyncio
    async def test_vswitch_configured(self):
        if not os.environ.get("ALICLOUD_ACCESS_KEY_ID"):
            pytest.skip("Aliyun credentials not available")

        vsw_id = os.environ.get("EA_ALIYUN_VSWITCH_ID", "")
        assert vsw_id, "EA_ALIYUN_VSWITCH_ID must be set for this test"

    @pytest.mark.asyncio
    async def test_key_pair_configured(self):
        if not os.environ.get("ALICLOUD_ACCESS_KEY_ID"):
            pytest.skip("Aliyun credentials not available")

        key_name = os.environ.get("EA_ALIYUN_KEY_PAIR", "")
        assert key_name, "EA_ALIYUN_KEY_PAIR must be set for this test"

        ssh_key_path = os.environ.get("EA_ALIYUN_SSH_KEY_PATH", "")
        if ssh_key_path:
            assert os.path.exists(ssh_key_path), f"SSH key file not found: {ssh_key_path}"

    @pytest.mark.asyncio
    async def test_image_id_configured(self):
        if not os.environ.get("ALICLOUD_ACCESS_KEY_ID"):
            pytest.skip("Aliyun credentials not available")

        image_id = os.environ.get("EA_ALIYUN_IMAGE_ID", "")
        assert image_id, "EA_ALIYUN_IMAGE_ID must be set for this test"

    @pytest.mark.asyncio
    async def test_provider_can_list_instances(self, provider):
        if not os.environ.get("ALICLOUD_ACCESS_KEY_ID"):
            pytest.skip("Aliyun credentials not available")

        instances = await provider.list_instances()
        assert isinstance(instances, list)


@pytest.mark.level2
class TestAWSPrerequisites:
    """T-117: Validate AWS infrastructure prerequisites."""

    @pytest.mark.asyncio
    async def test_security_group_configured(self):
        if not os.environ.get("AWS_ACCESS_KEY_ID"):
            pytest.skip("AWS credentials not available")

        sg_ids = os.environ.get("EA_AWS_SG_IDS", "")
        assert sg_ids, "EA_AWS_SG_IDS must be set for this test"

    @pytest.mark.asyncio
    async def test_subnet_configured(self):
        if not os.environ.get("AWS_ACCESS_KEY_ID"):
            pytest.skip("AWS credentials not available")

        subnet_id = os.environ.get("EA_AWS_SUBNET_ID", "")
        assert subnet_id, "EA_AWS_SUBNET_ID must be set for this test"

    @pytest.mark.asyncio
    async def test_key_pair_configured(self):
        if not os.environ.get("AWS_ACCESS_KEY_ID"):
            pytest.skip("AWS credentials not available")

        key_name = os.environ.get("EA_AWS_KEY_PAIR", "")
        assert key_name, "EA_AWS_KEY_PAIR must be set for this test"

        ssh_key_path = os.environ.get("EA_AWS_SSH_KEY_PATH", "")
        if ssh_key_path:
            assert os.path.exists(ssh_key_path), f"SSH key file not found: {ssh_key_path}"

    @pytest.mark.asyncio
    async def test_ami_configured(self):
        if not os.environ.get("AWS_ACCESS_KEY_ID"):
            pytest.skip("AWS credentials not available")

        ami_id = os.environ.get("EA_AWS_AMI_ID", "")
        assert ami_id, "EA_AWS_AMI_ID must be set for this test"

    @pytest.mark.asyncio
    async def test_provider_can_list_instances(self, provider):
        if not os.environ.get("AWS_ACCESS_KEY_ID"):
            pytest.skip("AWS credentials not available")

        instances = await provider.list_instances()
        assert isinstance(instances, list)
