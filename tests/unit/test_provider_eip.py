"""EIP (Elastic IP) provider capability — DryRun + AWS (boto3 mocked)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError

from elastic_agent.core.config import AWSProviderConfig
from elastic_agent.core.providers.aws import AWSProvider
from elastic_agent.core.providers.base import InstanceConfig
from elastic_agent.testing.dry_run_provider import DryRunProvider

pytestmark = pytest.mark.asyncio


def _aws(client) -> AWSProvider:
    prov = AWSProvider.__new__(AWSProvider)
    prov._config = AWSProviderConfig(region="ap-northeast-1")
    prov._client = client
    prov._root_dev_cache = {}
    return prov


# -- DryRun ----------------------------------------------------------------


async def test_dryrun_allocate_describe_release():
    p = DryRunProvider()
    eip = await p.allocate_eip(tags={"account": "a@x.com"})
    assert eip.allocation_id.startswith("eipalloc-")
    assert eip.public_ip

    got = await p.describe_eip(eip.allocation_id)
    assert got is not None and got.public_ip == eip.public_ip

    await p.release_eip(eip.allocation_id)
    assert await p.describe_eip(eip.allocation_id) is None


async def test_dryrun_associate_sets_instance_ip_and_survives_stop_start():
    """The binding invariant: a bound box shows the EIP as its public IP, and
    that IP does not change across stop/start."""
    p = DryRunProvider()
    inst = await p.create_instance(
        InstanceConfig(instance_type="t3.large", image_id="ami-x", key_pair_name="k")
    )
    eip = await p.allocate_eip()

    assoc = await p.associate_eip(inst.instance_id, eip.allocation_id)
    assert assoc.instance_id == inst.instance_id
    assert assoc.association_id
    ip_after_assoc = (await p.get_instance(inst.instance_id)).public_ip
    assert ip_after_assoc == eip.public_ip

    await p.stop_instance(inst.instance_id)
    await p.start_instance(inst.instance_id)
    ip_after_cycle = (await p.get_instance(inst.instance_id)).public_ip
    assert ip_after_cycle == eip.public_ip  # stable across power cycle


async def test_dryrun_disassociate_keeps_allocation():
    p = DryRunProvider()
    eip = await p.allocate_eip()
    inst = await p.create_instance(
        InstanceConfig(instance_type="t3.large", image_id="ami-x", key_pair_name="k")
    )
    await p.associate_eip(inst.instance_id, eip.allocation_id)
    await p.disassociate_eip(eip.allocation_id)
    still = await p.describe_eip(eip.allocation_id)
    assert still is not None  # allocation kept
    assert still.association_id is None
    assert still.instance_id is None


# -- AWS (boto3 mocked) ----------------------------------------------------


async def test_aws_allocate_tags_managed():
    client = MagicMock()
    client.allocate_address.return_value = {
        "AllocationId": "eipalloc-abc",
        "PublicIp": "52.1.2.3",
    }
    p = _aws(client)
    eip = await p.allocate_eip(tags={"account": "a@x.com"})
    assert eip.allocation_id == "eipalloc-abc"
    assert eip.public_ip == "52.1.2.3"
    # ManagedBy tag is always attached so allocations are discoverable/cleanable.
    kwargs = client.allocate_address.call_args.kwargs
    assert kwargs["Domain"] == "vpc"
    tags = kwargs["TagSpecifications"][0]["Tags"]
    keys = {t["Key"] for t in tags}
    assert "ManagedBy" in keys and "account" in keys


async def test_aws_associate_uses_native_id_and_allows_reassociation():
    client = MagicMock()
    client.associate_address.return_value = {"AssociationId": "eipassoc-1"}
    client.describe_addresses.return_value = {
        "Addresses": [
            {
                "AllocationId": "eipalloc-abc",
                "PublicIp": "52.1.2.3",
                "AssociationId": "eipassoc-1",
                "InstanceId": "i-123",
            }
        ]
    }
    p = _aws(client)
    eip = await p.associate_eip("aws:i-123", "eipalloc-abc")
    assert eip.association_id == "eipassoc-1"
    kwargs = client.associate_address.call_args.kwargs
    assert kwargs["InstanceId"] == "i-123"  # namespace prefix stripped
    assert kwargs["AllowReassociation"] is True


async def test_aws_describe_missing_returns_none():
    client = MagicMock()
    client.describe_addresses.side_effect = ClientError(
        {"Error": {"Code": "InvalidAllocationID.NotFound", "Message": "gone"}},
        "DescribeAddresses",
    )
    p = _aws(client)
    assert await p.describe_eip("eipalloc-gone") is None


async def test_aws_disassociate_looks_up_association_id():
    client = MagicMock()
    client.describe_addresses.return_value = {
        "Addresses": [
            {
                "AllocationId": "eipalloc-abc",
                "PublicIp": "52.1.2.3",
                "AssociationId": "eipassoc-1",
                "InstanceId": "i-123",
            }
        ]
    }
    p = _aws(client)
    await p.disassociate_eip("eipalloc-abc")
    client.disassociate_address.assert_called_once_with(AssociationId="eipassoc-1")


async def test_aws_disassociate_noop_when_detached():
    client = MagicMock()
    client.describe_addresses.return_value = {
        "Addresses": [{"AllocationId": "eipalloc-abc", "PublicIp": "52.1.2.3"}]
    }
    p = _aws(client)
    await p.disassociate_eip("eipalloc-abc")
    client.disassociate_address.assert_not_called()


async def test_aws_release():
    client = MagicMock()
    p = _aws(client)
    await p.release_eip("eipalloc-abc")
    client.release_address.assert_called_once_with(AllocationId="eipalloc-abc")


async def test_base_default_not_implemented():
    """Providers without EIP support raise NotImplementedError (aliyun etc.)."""
    from elastic_agent.core.providers.base import CloudProvider

    class _Bare(CloudProvider):
        async def create_instance(self, config):  # pragma: no cover - stubs
            ...

        async def start_instance(self, instance_id):
            ...

        async def stop_instance(self, instance_id):
            ...

        async def terminate_instance(self, instance_id):
            ...

        async def list_instances(self, filters=None):
            ...

        async def get_instance(self, instance_id):
            ...

        async def reboot_instance(self, instance_id):
            ...

        async def wait_until_running(self, instance_id, timeout=300):
            ...

    p = _Bare()
    with pytest.raises(NotImplementedError):
        await p.allocate_eip()
