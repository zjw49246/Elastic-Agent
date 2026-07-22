"""AWS EC2 CloudProvider implementation."""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone

from elastic_agent.core.config import AWSProviderConfig
from elastic_agent.core.providers.base import (
    CloudIdentity,
    CloudProvider,
    ElasticIp,
    Instance,
    InstanceConfig,
    InstanceNotFoundError,
    InstanceState,
)

logger = logging.getLogger(__name__)

# EC2 IDs are eventually consistent: a describe right after RunInstances may not
# see the new instance yet. These signal "not visible yet, keep polling".
_NOT_FOUND_CODES = {"InvalidInstanceID.NotFound", "InvalidInstanceID.Malformed"}
_EIP_NOT_FOUND_CODES = {
    "InvalidAllocationID.NotFound",
    "InvalidAddress.NotFound",
}
RECENT_INSTANCE_VISIBILITY_SECONDS = 300
RECENT_INSTANCE_RETRY_SECONDS = 5
EIP_ASSOCIATION_CONFIRM_ATTEMPTS = 30
EIP_ASSOCIATION_CONFIRM_SECONDS = 2.0


def _is_not_found_yet(exc: Exception) -> bool:
    if isinstance(exc, (LookupError, InstanceNotFoundError)):
        return True
    resp = getattr(exc, "response", None)  # botocore ClientError
    if isinstance(resp, dict):
        return resp.get("Error", {}).get("Code") in _NOT_FOUND_CODES
    return False


def _is_eip_not_found(exc: Exception) -> bool:
    resp = getattr(exc, "response", None)  # botocore ClientError
    if isinstance(resp, dict):
        return resp.get("Error", {}).get("Code") in _EIP_NOT_FOUND_CODES
    return False


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
        self._root_dev_cache: dict[str, str] = {}
        self._recent_instances: dict[str, float] = {}
        self._identity: CloudIdentity | None = None

    def _create_client(self):
        import boto3

        return boto3.client("ec2", region_name=self._config.region)

    def _native_id(self, instance_id: str) -> str:
        return instance_id.removeprefix("aws:")

    async def get_identity(self) -> CloudIdentity:
        if getattr(self, "_identity", None) is None:
            import boto3

            def _call() -> dict:
                return boto3.client(
                    "sts", region_name=self._config.region
                ).get_caller_identity()

            response = await asyncio.to_thread(_call)
            self._identity = CloudIdentity(
                provider="aws",
                account_id=str(response.get("Account") or ""),
                region=self._config.region,
            )
        return self._identity

    def _describe_one(self, native_id: str) -> dict:
        resp = self._client.describe_instances(InstanceIds=[native_id])
        reservations = resp.get("Reservations", [])
        if not reservations or not reservations[0].get("Instances"):
            raise InstanceNotFoundError(f"Instance not found: {native_id}")
        return reservations[0]["Instances"][0]

    def _root_device_name(self, image_id: str) -> str:
        """The AMI's real root device name. Ubuntu roots on /dev/sda1, Amazon
        Linux on /dev/xvda — sizing the wrong name creates a phantom unused
        volume while the actual root stays at the AMI's baked-in size, so the
        instance runs out of disk. Cached; falls back to the common Ubuntu name."""
        if image_id in self._root_dev_cache:
            return self._root_dev_cache[image_id]
        name = "/dev/sda1"
        try:
            resp = self._client.describe_images(ImageIds=[image_id])
            images = resp.get("Images", [])
            if images and images[0].get("RootDeviceName"):
                name = images[0]["RootDeviceName"]
        except Exception:  # noqa: BLE001
            logger.warning("could not resolve root device for %s; using %s", image_id, name)
        self._root_dev_cache[image_id] = name
        return name

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
            # Worker bootstrap never needs IMDSv1.  Requiring a session token
            # prevents SSRF-style credential retrieval from the job process.
            MetadataOptions={
                "HttpEndpoint": "enabled",
                "HttpTokens": "required",
                "HttpPutResponseHopLimit": 1,
            },
        )

        sg_ids = config.security_group_ids or self._config.security_group_ids
        subnet_id = config.subnet_id or self._config.subnet_id
        is_eip_bound_worker = bool(config.tags.get("ElasticAgentLease"))

        if is_eip_bound_worker:
            # The account's durable EIP is attached before SSH bootstrap.  Do
            # not also allocate a transient public IPv4 address to the fresh
            # instance.  NetworkInterfaces is required to override a subnet's
            # auto-assign-public-IP setting; its subnet/groups fields replace
            # the mutually-exclusive top-level RunInstances fields.
            interface: dict = {
                "DeviceIndex": 0,
                "AssociatePublicIpAddress": False,
                "DeleteOnTermination": True,
            }
            if subnet_id:
                interface["SubnetId"] = subnet_id
            if sg_ids:
                interface["Groups"] = sg_ids
            kwargs["NetworkInterfaces"] = [interface]
        else:
            # Unbound workers are still bootstrapped over SSH using their
            # launch address, so retain the deployment subnet's current public
            # IP policy for compatibility.
            if subnet_id:
                kwargs["SubnetId"] = subnet_id
            if sg_ids:
                kwargs["SecurityGroupIds"] = sg_ids

        # Attach the worker IAM instance profile (if configured) so the worker
        # can reach S3 directly — dataset pull + result push without a Manager
        # relay. Workers otherwise have no cloud credentials.
        instance_profile = self._config.worker_instance_profile
        if instance_profile:
            kwargs["IamInstanceProfile"] = {"Name": instance_profile}

        if config.spot:
            kwargs["InstanceMarketOptions"] = {"MarketType": "spot"}

        if config.client_token:
            kwargs["ClientToken"] = config.client_token

        if config.user_data:
            kwargs["UserData"] = config.user_data

        image_id = config.image_id or self._config.ami_id
        root_device = await asyncio.to_thread(self._root_device_name, image_id)
        block_devices = [
            {
                "DeviceName": root_device,
                "Ebs": {
                    "VolumeSize": config.root_disk_size_gb,
                    "VolumeType": "gp3",
                    "DeleteOnTermination": True,
                    "Encrypted": True,
                },
            }
        ]
        kwargs["BlockDeviceMappings"] = block_devices

        def _call():
            return self._client.run_instances(**kwargs)

        resp = await asyncio.to_thread(_call)
        ec2_inst = resp["Instances"][0]
        native_id = ec2_inst["InstanceId"]
        self._recent_instances[native_id] = time.monotonic()
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
        created_at = getattr(self, "_recent_instances", {}).get(native_id)
        visibility_deadline = (
            created_at + RECENT_INSTANCE_VISIBILITY_SECONDS
            if created_at is not None
            else None
        )
        while True:
            try:
                await asyncio.to_thread(
                    self._client.terminate_instances, InstanceIds=[native_id]
                )
                break
            except Exception as exc:  # noqa: BLE001
                if not _is_not_found_yet(exc):
                    raise
                if (
                    visibility_deadline is not None
                    and time.monotonic() < visibility_deadline
                ):
                    # This provider created the ID during the current process,
                    # so NotFound means "not visible yet", not "already gone".
                    # Keep retrying across AWS's documented eventual-consistency
                    # window so immediate crash compensation cannot leak it.
                    await asyncio.sleep(RECENT_INSTANCE_RETRY_SECONDS)
                    continue
                logger.info("AWS instance %s is already gone", native_id)
                return
        getattr(self, "_recent_instances", {}).pop(native_id, None)
        logger.info("Terminated AWS instance %s", native_id)

    async def reboot_instance(self, instance_id: str) -> None:
        native_id = self._native_id(instance_id)
        await asyncio.to_thread(self._client.reboot_instances, InstanceIds=[native_id])
        logger.info("Rebooted AWS instance %s", native_id)

    # -- Elastic IP --------------------------------------------------------

    async def allocate_eip(self, tags: dict[str, str] | None = None) -> ElasticIp:
        all_tags = {**(tags or {}), self.MANAGED_TAG_KEY: self.MANAGED_TAG_VALUE}
        tag_spec = [
            {
                "ResourceType": "elastic-ip",
                "Tags": [{"Key": k, "Value": v} for k, v in all_tags.items()],
            }
        ]

        def _call():
            return self._client.allocate_address(Domain="vpc", TagSpecifications=tag_spec)

        resp = await asyncio.to_thread(_call)
        eip = ElasticIp(allocation_id=resp["AllocationId"], public_ip=resp["PublicIp"])
        logger.info("Allocated EIP %s (%s)", eip.public_ip, eip.allocation_id)
        return eip

    async def associate_eip(self, instance_id: str, allocation_id: str) -> ElasticIp:
        native_id = self._native_id(instance_id)

        def _call():
            # Never let a stale/concurrent lease steal an EIP from a live job.
            # BindingManager checks the current attachment first; AWS is the
            # final race-safe guard between that check and this API call.
            return self._client.associate_address(
                AllocationId=allocation_id,
                InstanceId=native_id,
                AllowReassociation=False,
            )

        resp = await asyncio.to_thread(_call)
        association_id = resp.get("AssociationId")
        observed: ElasticIp | None = None
        for attempt in range(EIP_ASSOCIATION_CONFIRM_ATTEMPTS):
            observed = await self.describe_eip(allocation_id)
            if (
                observed is not None
                and observed.instance_id == instance_id
                and (
                    not association_id
                    or observed.association_id == association_id
                )
            ):
                logger.info("Associated EIP %s -> %s", allocation_id, native_id)
                return observed
            if attempt + 1 < EIP_ASSOCIATION_CONFIRM_ATTEMPTS:
                await asyncio.sleep(EIP_ASSOCIATION_CONFIRM_SECONDS)
        if observed is None:
            detail = "not visible"
        else:
            detail = (
                f"observed instance={observed.instance_id!r}, "
                f"association={observed.association_id!r}"
            )
        raise RuntimeError(
            f"EIP {allocation_id} association did not converge to "
            f"{instance_id!r}: {detail}"
        )

    async def disassociate_eip(
        self,
        allocation_id: str,
        *,
        association_id: str | None = None,
        expected_instance_id: str | None = None,
    ) -> None:
        eip = await self.describe_eip(allocation_id)
        if eip is None or not eip.association_id:
            return  # already detached / gone — nothing to do
        if expected_instance_id and eip.instance_id != expected_instance_id:
            raise RuntimeError(
                f"EIP {allocation_id} is attached to {eip.instance_id}, "
                f"not expected instance {expected_instance_id}"
            )
        if association_id and eip.association_id != association_id:
            raise RuntimeError(
                f"EIP {allocation_id} association changed from "
                f"{association_id} to {eip.association_id}"
            )
        # Use the caller-observed association id.  Even if the EIP is moved in
        # the tiny gap after the check, AWS will receive the old association id
        # and cannot disconnect the new owner.
        target_association = association_id or eip.association_id

        def _call():
            return self._client.disassociate_address(AssociationId=target_association)

        await asyncio.to_thread(_call)
        logger.info("Disassociated EIP %s", allocation_id)

    async def release_eip(self, allocation_id: str) -> None:
        def _call():
            return self._client.release_address(AllocationId=allocation_id)

        try:
            await asyncio.to_thread(_call)
        except Exception as exc:  # noqa: BLE001
            if _is_eip_not_found(exc):
                logger.info("AWS EIP %s is already gone", allocation_id)
                return
            raise
        logger.info("Released EIP %s", allocation_id)

    async def tag_eip(
        self, allocation_id: str, tags: dict[str, str]
    ) -> None:
        all_tags = {
            **tags,
            self.MANAGED_TAG_KEY: self.MANAGED_TAG_VALUE,
        }
        await asyncio.to_thread(
            self._client.create_tags,
            Resources=[allocation_id],
            Tags=[{"Key": key, "Value": value} for key, value in all_tags.items()],
        )
        logger.info("Tagged EIP %s for Elastic Agent ownership", allocation_id)

    async def describe_eip(self, allocation_id: str) -> ElasticIp | None:
        def _call():
            return self._client.describe_addresses(AllocationIds=[allocation_id])

        try:
            resp = await asyncio.to_thread(_call)
        except Exception as exc:  # noqa: BLE001
            if _is_eip_not_found(exc):
                return None
            raise
        addrs = resp.get("Addresses", [])
        if not addrs:
            return None
        a = addrs[0]
        native_iid = a.get("InstanceId")
        return ElasticIp(
            allocation_id=a["AllocationId"],
            public_ip=a["PublicIp"],
            association_id=a.get("AssociationId"),
            instance_id=f"aws:{native_iid}" if native_iid else None,
            tags={tag["Key"]: tag["Value"] for tag in a.get("Tags", [])},
        )

    async def list_eips(
        self, filters: dict[str, str] | None = None
    ) -> list[ElasticIp]:
        wanted = {
            self.MANAGED_TAG_KEY: self.MANAGED_TAG_VALUE,
            **(filters or {}),
        }
        aws_filters = [
            {"Name": f"tag:{key}", "Values": [value]}
            for key, value in wanted.items()
        ]
        resp = await asyncio.to_thread(
            self._client.describe_addresses, Filters=aws_filters
        )
        result: list[ElasticIp] = []
        for address in resp.get("Addresses", []):
            native_iid = address.get("InstanceId")
            result.append(ElasticIp(
                allocation_id=address["AllocationId"],
                public_ip=address["PublicIp"],
                association_id=address.get("AssociationId"),
                instance_id=f"aws:{native_iid}" if native_iid else None,
                tags={tag["Key"]: tag["Value"] for tag in address.get("Tags", [])},
            ))
        return result

    async def list_instances(self, filters: dict[str, str] | None = None) -> list[Instance]:
        # Callers commonly include ManagedBy defensively.  Merge by key before
        # building the EC2 filter list: duplicate filter names are unnecessary
        # and can be rejected by the real DescribeInstances API even though
        # permissive unit mocks accept them.
        wanted = {
            **(filters or {}),
            self.MANAGED_TAG_KEY: self.MANAGED_TAG_VALUE,
        }
        ec2_filters = [
            {"Name": f"tag:{key}", "Values": [value]}
            for key, value in wanted.items()
        ]

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
        try:
            raw = await asyncio.to_thread(self._describe_one, native_id)
        except Exception as exc:
            if _is_not_found_yet(exc):
                raise InstanceNotFoundError(
                    f"Instance not found: {native_id}"
                ) from exc
            raise
        return _to_instance(raw, self._config.region)

    async def wait_until_running(self, instance_id: str, timeout: int = 300) -> Instance:
        native_id = self._native_id(instance_id)
        deadline = time.monotonic() + timeout
        interval = 5

        while time.monotonic() < deadline:
            try:
                raw = await asyncio.to_thread(self._describe_one, native_id)
            except Exception as exc:  # noqa: BLE001
                # AWS is eventually consistent: a describe issued moments after
                # RunInstances can return InvalidInstanceID.NotFound (or an empty
                # reservation) before the new ID has propagated. Treat as "not
                # ready yet" and keep polling instead of aborting provision.
                if _is_not_found_yet(exc):
                    logger.debug("wait_until_running: %s not visible yet, retrying", native_id)
                    await asyncio.sleep(interval)
                    continue
                raise
            inst = _to_instance(raw, self._config.region)
            if inst.state == InstanceState.RUNNING:
                return inst
            if inst.state == InstanceState.TERMINATED:
                raise RuntimeError(f"Instance {native_id} terminated while waiting")
            await asyncio.sleep(interval)

        raise TimeoutError(f"Instance {native_id} did not reach running state within {timeout}s")
