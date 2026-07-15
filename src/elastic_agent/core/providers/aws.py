"""AWS EC2 CloudProvider implementation."""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone

from elastic_agent.core.config import AWSProviderConfig
from elastic_agent.core.providers.base import (
    CloudProvider,
    Instance,
    InstanceConfig,
    InstanceState,
)

logger = logging.getLogger(__name__)

_AWS_STATE_MAP: dict[str, InstanceState] = {
    "pending": InstanceState.PENDING,
    "running": InstanceState.RUNNING,
    "stopping": InstanceState.STOPPING,
    "stopped": InstanceState.STOPPED,
    "shutting-down": InstanceState.STOPPING,
    "terminated": InstanceState.TERMINATED,
}


def _to_instance(ec2_instance: dict, region: str) -> Instance:
    native_id = ec2_instance["InstanceId"]
    state_name = ec2_instance.get("State", {}).get("Name", "pending")
    tags_raw = ec2_instance.get("Tags", [])
    tags = {t["Key"]: t["Value"] for t in tags_raw}

    launch_time = ec2_instance.get("LaunchTime")
    if isinstance(launch_time, datetime):
        launched_at = launch_time.replace(tzinfo=timezone.utc) if launch_time.tzinfo is None else launch_time
    else:
        launched_at = None

    return Instance(
        instance_id=f"aws:{native_id}",
        platform="aws",
        native_id=native_id,
        state=_AWS_STATE_MAP.get(state_name, InstanceState.PENDING),
        public_ip=ec2_instance.get("PublicIpAddress"),
        private_ip=ec2_instance.get("PrivateIpAddress"),
        instance_type=ec2_instance.get("InstanceType"),
        image_id=ec2_instance.get("ImageId"),
        region=region,
        zone=ec2_instance.get("Placement", {}).get("AvailabilityZone"),
        tags=tags,
        created_at=launched_at,
        launched_at=launched_at,
    )


class AWSProvider(CloudProvider):
    """AWS EC2 provider using boto3 (sync calls wrapped in asyncio.to_thread)."""

    def __init__(self, config: AWSProviderConfig) -> None:
        self._config = config
        self._client = self._create_client()

    def _create_client(self):
        import boto3

        return boto3.client("ec2", region_name=self._config.region)

    def _native_id(self, instance_id: str) -> str:
        return instance_id.removeprefix("aws:")

    def _describe_one(self, native_id: str) -> dict:
        resp = self._client.describe_instances(InstanceIds=[native_id])
        reservations = resp.get("Reservations", [])
        if not reservations or not reservations[0].get("Instances"):
            raise LookupError(f"Instance not found: {native_id}")
        return reservations[0]["Instances"][0]

    async def create_instance(self, config: InstanceConfig) -> Instance:
        tags = {**config.tags, self.MANAGED_TAG_KEY: self.MANAGED_TAG_VALUE}
        tag_specs = [
            {
                "ResourceType": "instance",
                "Tags": [{"Key": k, "Value": v} for k, v in tags.items()],
            }
        ]

        kwargs: dict = dict(
            ImageId=config.image_id or self._config.ami_id,
            InstanceType=config.instance_type or self._config.default_instance_type,
            KeyName=config.key_pair_name or self._config.key_pair_name,
            MinCount=1,
            MaxCount=1,
            TagSpecifications=tag_specs,
        )

        sg_ids = config.security_group_ids or self._config.security_group_ids
        subnet_id = config.subnet_id or self._config.subnet_id

        if subnet_id:
            kwargs["SubnetId"] = subnet_id
        if sg_ids:
            kwargs["SecurityGroupIds"] = sg_ids

        if config.spot:
            kwargs["InstanceMarketOptions"] = {"MarketType": "spot"}

        if config.user_data:
            kwargs["UserData"] = config.user_data

        block_devices = [
            {
                "DeviceName": "/dev/xvda",
                "Ebs": {
                    "VolumeSize": config.root_disk_size_gb,
                    "VolumeType": "gp3",
                    "DeleteOnTermination": True,
                },
            }
        ]
        kwargs["BlockDeviceMappings"] = block_devices

        def _call():
            return self._client.run_instances(**kwargs)

        resp = await asyncio.to_thread(_call)
        ec2_inst = resp["Instances"][0]
        native_id = ec2_inst["InstanceId"]
        logger.info("Created AWS instance %s", native_id)

        return _to_instance(ec2_inst, self._config.region)

    async def start_instance(self, instance_id: str) -> None:
        native_id = self._native_id(instance_id)
        await asyncio.to_thread(self._client.start_instances, InstanceIds=[native_id])
        logger.info("Started AWS instance %s", native_id)

    async def stop_instance(self, instance_id: str) -> None:
        native_id = self._native_id(instance_id)
        await asyncio.to_thread(self._client.stop_instances, InstanceIds=[native_id])
        logger.info("Stopped AWS instance %s", native_id)

    async def terminate_instance(self, instance_id: str) -> None:
        native_id = self._native_id(instance_id)
        await asyncio.to_thread(self._client.terminate_instances, InstanceIds=[native_id])
        logger.info("Terminated AWS instance %s", native_id)

    async def reboot_instance(self, instance_id: str) -> None:
        native_id = self._native_id(instance_id)
        await asyncio.to_thread(self._client.reboot_instances, InstanceIds=[native_id])
        logger.info("Rebooted AWS instance %s", native_id)

    async def list_instances(self, filters: dict[str, str] | None = None) -> list[Instance]:
        ec2_filters = [
            {"Name": f"tag:{self.MANAGED_TAG_KEY}", "Values": [self.MANAGED_TAG_VALUE]},
        ]
        if filters:
            for k, v in filters.items():
                ec2_filters.append({"Name": f"tag:{k}", "Values": [v]})

        all_instances: list[Instance] = []
        paginator = self._client.get_paginator("describe_instances")

        def _paginate():
            results = []
            for page in paginator.paginate(Filters=ec2_filters):
                for res in page.get("Reservations", []):
                    results.extend(res.get("Instances", []))
            return results

        raw_list = await asyncio.to_thread(_paginate)
        for raw in raw_list:
            all_instances.append(_to_instance(raw, self._config.region))

        return all_instances

    async def get_instance(self, instance_id: str) -> Instance:
        native_id = self._native_id(instance_id)
        raw = await asyncio.to_thread(self._describe_one, native_id)
        return _to_instance(raw, self._config.region)

    async def wait_until_running(self, instance_id: str, timeout: int = 300) -> Instance:
        native_id = self._native_id(instance_id)
        deadline = time.monotonic() + timeout
        interval = 5

        while time.monotonic() < deadline:
            raw = await asyncio.to_thread(self._describe_one, native_id)
            inst = _to_instance(raw, self._config.region)
            if inst.state == InstanceState.RUNNING:
                return inst
            if inst.state == InstanceState.TERMINATED:
                raise RuntimeError(f"Instance {native_id} terminated while waiting")
            await asyncio.sleep(interval)

        raise TimeoutError(f"Instance {native_id} did not reach running state within {timeout}s")
