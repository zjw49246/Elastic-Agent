"""AWSProvider unit tests (boto3 client mocked — no real AWS calls)."""

from __future__ import annotations

import time
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError

from elastic_agent.core.config import AWSProviderConfig
from elastic_agent.core.providers.aws import AWSProvider
from elastic_agent.core.providers.base import InstanceConfig, InstanceState


def _running_response(native_id: str) -> dict:
    return {
        "Reservations": [
            {
                "Instances": [
                    {
                        "InstanceId": native_id,
                        "State": {"Name": "running"},
                        "PublicIpAddress": "1.2.3.4",
                        "PrivateIpAddress": "172.31.0.1",
                        "InstanceType": "t3.large",
                        "ImageId": "ami-x",
                        "LaunchTime": datetime.now(timezone.utc),
                        "Tags": [],
                    }
                ]
            }
        ]
    }


def _not_found() -> ClientError:
    # What boto3 raises when describing an ID that hasn't propagated yet.
    return ClientError(
        {"Error": {"Code": "InvalidInstanceID.NotFound", "Message": "does not exist"}},
        "DescribeInstances",
    )


def _provider_with_client(client) -> AWSProvider:
    prov = AWSProvider.__new__(AWSProvider)
    prov._config = AWSProviderConfig(region="ap-northeast-1")
    prov._client = client
    prov._root_dev_cache = {}
    prov._recent_instances = {}
    return prov


@pytest.fixture
def _no_sleep(monkeypatch):
    """Make the poll interval instant so retry tests don't wall-clock."""

    async def _instant(_):
        return None

    monkeypatch.setattr("elastic_agent.core.providers.aws.asyncio.sleep", _instant)


@pytest.mark.asyncio
async def test_wait_until_running_retries_on_not_found_yet(_no_sleep):
    """Eventual consistency: describe raises NotFound right after RunInstances,
    then the ID propagates. wait_until_running must poll through it, not abort."""
    native_id = "i-0abc"
    client = MagicMock()
    client.describe_instances.side_effect = [
        _not_found(),
        _not_found(),
        _running_response(native_id),
    ]
    prov = _provider_with_client(client)

    inst = await prov.wait_until_running(f"aws:{native_id}", timeout=30)

    assert inst.state == InstanceState.RUNNING
    assert inst.native_id == native_id
    assert client.describe_instances.call_count == 3


@pytest.mark.asyncio
async def test_create_instance_sizes_the_amis_real_root_device():
    """Root volume must be mapped to the AMI's actual RootDeviceName. Hardcoding
    /dev/xvda on an Ubuntu AMI (root=/dev/sda1) makes a phantom unused volume
    while root stays tiny → the instance runs out of disk."""
    client = MagicMock()
    client.describe_images.return_value = {"Images": [{"RootDeviceName": "/dev/sda1"}]}
    client.run_instances.return_value = {
        "Instances": [{"InstanceId": "i-x", "State": {"Name": "pending"}}]
    }
    prov = _provider_with_client(client)

    await prov.create_instance(
        InstanceConfig(instance_type="t3.large", image_id="ami-x",
                       key_pair_name="k", root_disk_size_gb=40)
    )

    bdm = client.run_instances.call_args.kwargs["BlockDeviceMappings"]
    assert bdm[0]["DeviceName"] == "/dev/sda1"
    assert bdm[0]["Ebs"]["VolumeSize"] == 40
    assert bdm[0]["Ebs"]["Encrypted"] is True
    assert bdm[0]["Ebs"]["DeleteOnTermination"] is True
    metadata = client.run_instances.call_args.kwargs["MetadataOptions"]
    assert metadata == {
        "HttpEndpoint": "enabled",
        "HttpTokens": "required",
        "HttpPutResponseHopLimit": 1,
    }


@pytest.mark.asyncio
async def test_create_instance_falls_back_when_ami_lookup_fails():
    client = MagicMock()
    client.describe_images.side_effect = RuntimeError("no perms")
    client.run_instances.return_value = {
        "Instances": [{"InstanceId": "i-x", "State": {"Name": "pending"}}]
    }
    prov = _provider_with_client(client)

    await prov.create_instance(
        InstanceConfig(instance_type="t3.large", image_id="ami-x", key_pair_name="k")
    )

    bdm = client.run_instances.call_args.kwargs["BlockDeviceMappings"]
    assert bdm[0]["DeviceName"] == "/dev/sda1"  # safe Ubuntu default


@pytest.mark.asyncio
async def test_eip_bound_worker_does_not_get_transient_public_ip():
    client = MagicMock()
    client.describe_images.return_value = {"Images": [{"RootDeviceName": "/dev/sda1"}]}
    client.run_instances.return_value = {
        "Instances": [{"InstanceId": "i-bound", "State": {"Name": "pending"}}]
    }
    prov = _provider_with_client(client)

    await prov.create_instance(InstanceConfig(
        instance_type="t3.large",
        image_id="ami-x",
        key_pair_name="k",
        subnet_id="subnet-private",
        security_group_ids=["sg-worker"],
        tags={"ElasticAgentLease": "lease-1"},
    ))

    kwargs = client.run_instances.call_args.kwargs
    assert "SubnetId" not in kwargs
    assert "SecurityGroupIds" not in kwargs
    assert kwargs["NetworkInterfaces"] == [{
        "DeviceIndex": 0,
        "AssociatePublicIpAddress": False,
        "DeleteOnTermination": True,
        "SubnetId": "subnet-private",
        "Groups": ["sg-worker"],
    }]
    tag_specs = {
        item["ResourceType"]: {
            tag["Key"]: tag["Value"] for tag in item["Tags"]
        }
        for item in kwargs["TagSpecifications"]
    }
    assert tag_specs["network-interface"] == {
        "ElasticAgentLease": "lease-1",
        "ManagedBy": "elastic-agent",
    }


@pytest.mark.asyncio
async def test_unbound_worker_preserves_existing_subnet_public_ip_policy():
    client = MagicMock()
    client.describe_images.return_value = {"Images": [{"RootDeviceName": "/dev/sda1"}]}
    client.run_instances.return_value = {
        "Instances": [{"InstanceId": "i-normal", "State": {"Name": "pending"}}]
    }
    prov = _provider_with_client(client)

    await prov.create_instance(InstanceConfig(
        instance_type="t3.large",
        image_id="ami-x",
        key_pair_name="k",
        subnet_id="subnet-worker",
        security_group_ids=["sg-worker"],
    ))

    kwargs = client.run_instances.call_args.kwargs
    assert kwargs["SubnetId"] == "subnet-worker"
    assert kwargs["SecurityGroupIds"] == ["sg-worker"]
    assert "NetworkInterfaces" not in kwargs
    assert [
        item["ResourceType"] for item in kwargs["TagSpecifications"]
    ] == ["instance"]


@pytest.mark.asyncio
async def test_wait_until_running_reraises_real_errors():
    """A non-transient client error (e.g. auth) must propagate, not loop."""
    client = MagicMock()
    client.describe_instances.side_effect = ClientError(
        {"Error": {"Code": "UnauthorizedOperation", "Message": "nope"}},
        "DescribeInstances",
    )
    prov = _provider_with_client(client)

    with pytest.raises(ClientError):
        await prov.wait_until_running("aws:i-0abc", timeout=30)


@pytest.mark.asyncio
async def test_terminate_is_idempotent_when_instance_is_already_gone(_no_sleep):
    client = MagicMock()
    client.terminate_instances.side_effect = ClientError(
        {"Error": {"Code": "InvalidInstanceID.NotFound", "Message": "gone"}},
        "TerminateInstances",
    )
    prov = _provider_with_client(client)

    await prov.terminate_instance("aws:i-gone")

    assert client.terminate_instances.call_count == 1
    client.terminate_instances.assert_called_with(InstanceIds=["i-gone"])


@pytest.mark.asyncio
async def test_terminate_retries_not_found_for_just_created_instance(_no_sleep):
    client = MagicMock()
    client.terminate_instances.side_effect = [_not_found(), _not_found(), {}]
    prov = _provider_with_client(client)
    prov._recent_instances["i-new"] = time.monotonic()

    await prov.terminate_instance("aws:i-new")

    assert client.terminate_instances.call_count == 3
    assert "i-new" not in prov._recent_instances


@pytest.mark.asyncio
async def test_list_instances_deduplicates_managed_tag_filter():
    client = MagicMock()
    paginator = client.get_paginator.return_value
    paginator.paginate.return_value = [_running_response("i-owned")]
    prov = _provider_with_client(client)

    instances = await prov.list_instances(filters={
        "ManagedBy": "elastic-agent",
        "ElasticAgentController": "controller-1",
    })

    filters = paginator.paginate.call_args.kwargs["Filters"]
    names = [item["Name"] for item in filters]
    assert names.count("tag:ManagedBy") == 1
    assert names.count("tag:ElasticAgentController") == 1
    assert instances[0].instance_id == "aws:i-owned"
