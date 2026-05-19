"""Aliyun ECS CloudProvider implementation."""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone

from elastic_agent.core.config import AliyunProviderConfig
from elastic_agent.core.providers.base import (
    CloudProvider,
    Instance,
    InstanceConfig,
    InstanceState,
)

logger = logging.getLogger(__name__)

_ALIYUN_STATE_MAP: dict[str, InstanceState] = {
    "Pending": InstanceState.PENDING,
    "Starting": InstanceState.STARTING,
    "Running": InstanceState.RUNNING,
    "Stopping": InstanceState.STOPPING,
    "Stopped": InstanceState.STOPPED,
}


def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None


def _extract_public_ip(ecs_instance: dict) -> str | None:
    eip = ecs_instance.get("EipAddress", {})
    if eip and eip.get("IpAddress"):
        return eip["IpAddress"]
    pub = ecs_instance.get("PublicIpAddress", {})
    ips = pub.get("IpAddress", []) if isinstance(pub, dict) else []
    return ips[0] if ips else None


def _extract_private_ip(ecs_instance: dict) -> str | None:
    vpc = ecs_instance.get("VpcAttributes", {})
    ips = vpc.get("PrivateIpAddress", {})
    ip_list = ips.get("IpAddress", []) if isinstance(ips, dict) else []
    return ip_list[0] if ip_list else None


def _to_instance(ecs_instance: dict, region_id: str) -> Instance:
    native_id = ecs_instance["InstanceId"]
    status = ecs_instance.get("Status", "Pending")
    tags_raw = ecs_instance.get("Tags", {}).get("Tag", [])
    tags = {t["TagKey"]: t["TagValue"] for t in tags_raw}

    return Instance(
        instance_id=f"aliyun:{native_id}",
        platform="aliyun",
        native_id=native_id,
        state=_ALIYUN_STATE_MAP.get(status, InstanceState.PENDING),
        public_ip=_extract_public_ip(ecs_instance),
        private_ip=_extract_private_ip(ecs_instance),
        instance_type=ecs_instance.get("InstanceType"),
        image_id=ecs_instance.get("ImageId"),
        region=region_id,
        zone=ecs_instance.get("ZoneId"),
        tags=tags,
        created_at=_parse_datetime(ecs_instance.get("CreationTime")),
        launched_at=_parse_datetime(ecs_instance.get("StartTime")),
    )


class AliyunProvider(CloudProvider):
    """Aliyun ECS provider using alibabacloud SDK V2.0 (sync calls wrapped in asyncio.to_thread)."""

    def __init__(self, config: AliyunProviderConfig) -> None:
        self._config = config
        self._client = self._create_client()

    def _create_client(self):
        import os

        from alibabacloud_ecs20140526.client import Client
        from alibabacloud_tea_openapi.models import Config

        ak_id = os.environ.get("ALICLOUD_ACCESS_KEY_ID", "")
        ak_secret = os.environ.get("ALICLOUD_ACCESS_KEY_SECRET", "")
        api_config = Config(
            access_key_id=ak_id,
            access_key_secret=ak_secret,
            region_id=self._config.region_id,
        )
        return Client(api_config)

    def _native_id(self, instance_id: str) -> str:
        return instance_id.removeprefix("aliyun:")

    def _describe_one(self, native_id: str) -> dict:
        from alibabacloud_ecs20140526.models import DescribeInstancesRequest

        req = DescribeInstancesRequest(
            region_id=self._config.region_id,
            instance_ids=f'["{native_id}"]',
        )
        resp = self._client.describe_instances(req)
        instances = resp.body.instances.instance or []
        if not instances:
            raise LookupError(f"Instance not found: {native_id}")
        return instances[0].to_map()

    async def create_instance(self, config: InstanceConfig) -> Instance:
        from alibabacloud_ecs20140526.models import (
            RunInstancesRequest,
            RunInstancesRequestSystemDisk,
            RunInstancesRequestTag,
        )

        tags = {**config.tags, self.MANAGED_TAG_KEY: self.MANAGED_TAG_VALUE}
        tag_models = [RunInstancesRequestTag(key=k, value=v) for k, v in tags.items()]

        disk = RunInstancesRequestSystemDisk(
            size=str(config.root_disk_size_gb),
            category=config.root_disk_type,
        )

        kwargs: dict = dict(
            region_id=self._config.region_id,
            image_id=config.image_id or self._config.image_id,
            instance_type=config.instance_type or self._config.instance_type,
            security_group_id=config.security_group_id or self._config.security_group_id,
            v_switch_id=config.vswitch_id or self._config.vswitch_id,
            key_pair_name=config.key_pair_name or self._config.key_pair_name,
            internet_charge_type="PayByTraffic",
            internet_max_bandwidth_out=100,
            instance_charge_type="PostPaid",
            system_disk=disk,
            tag=tag_models,
            amount=1,
        )

        if config.spot or self._config.spot_enabled:
            kwargs["spot_strategy"] = "SpotAsPriceGo"

        if config.user_data:
            kwargs["user_data"] = config.user_data

        req = RunInstancesRequest(**kwargs)

        def _call():
            return self._client.run_instances(req)

        resp = await asyncio.to_thread(_call)
        native_id = resp.body.instance_id_sets.instance_id_set[0]
        logger.info("Created Aliyun instance %s", native_id)

        raw = await asyncio.to_thread(self._describe_one, native_id)
        return _to_instance(raw, self._config.region_id)

    async def start_instance(self, instance_id: str) -> None:
        from alibabacloud_ecs20140526.models import StartInstanceRequest

        native_id = self._native_id(instance_id)
        req = StartInstanceRequest(instance_id=native_id)
        await asyncio.to_thread(self._client.start_instance, req)
        logger.info("Started Aliyun instance %s", native_id)

    async def stop_instance(self, instance_id: str) -> None:
        from alibabacloud_ecs20140526.models import StopInstanceRequest

        native_id = self._native_id(instance_id)
        req = StopInstanceRequest(instance_id=native_id, force_stop=False)
        await asyncio.to_thread(self._client.stop_instance, req)
        logger.info("Stopped Aliyun instance %s", native_id)

    async def terminate_instance(self, instance_id: str) -> None:
        from alibabacloud_ecs20140526.models import DeleteInstanceRequest, StopInstanceRequest

        native_id = self._native_id(instance_id)
        try:
            stop_req = StopInstanceRequest(instance_id=native_id, force_stop=True)
            await asyncio.to_thread(self._client.stop_instance, stop_req)
        except Exception:
            pass

        del_req = DeleteInstanceRequest(instance_id=native_id, force=True)
        await asyncio.to_thread(self._client.delete_instance, del_req)
        logger.info("Terminated Aliyun instance %s", native_id)

    async def list_instances(self, filters: dict[str, str] | None = None) -> list[Instance]:
        from alibabacloud_ecs20140526.models import (
            DescribeInstancesRequest,
            DescribeInstancesRequestTag,
        )

        tags = [DescribeInstancesRequestTag(key=self.MANAGED_TAG_KEY, value=self.MANAGED_TAG_VALUE)]
        if filters:
            tags.extend(DescribeInstancesRequestTag(key=k, value=v) for k, v in filters.items())

        all_instances: list[Instance] = []
        page = 1
        page_size = 50

        while True:
            req = DescribeInstancesRequest(
                region_id=self._config.region_id,
                tag=tags,
                page_number=page,
                page_size=page_size,
            )
            resp = await asyncio.to_thread(self._client.describe_instances, req)
            items = resp.body.instances.instance or []
            for item in items:
                all_instances.append(_to_instance(item.to_map(), self._config.region_id))
            if len(items) < page_size:
                break
            page += 1

        return all_instances

    async def get_instance(self, instance_id: str) -> Instance:
        native_id = self._native_id(instance_id)
        raw = await asyncio.to_thread(self._describe_one, native_id)
        return _to_instance(raw, self._config.region_id)

    async def wait_until_running(self, instance_id: str, timeout: int = 300) -> Instance:
        native_id = self._native_id(instance_id)
        deadline = time.monotonic() + timeout
        interval = 5

        while time.monotonic() < deadline:
            raw = await asyncio.to_thread(self._describe_one, native_id)
            inst = _to_instance(raw, self._config.region_id)
            if inst.state == InstanceState.RUNNING:
                return inst
            if inst.state == InstanceState.TERMINATED:
                raise RuntimeError(f"Instance {native_id} terminated while waiting")
            await asyncio.sleep(interval)

        raise TimeoutError(f"Instance {native_id} did not reach running state within {timeout}s")
