"""DryRunProvider — in-memory cloud provider for testing.

T-200: Simulates cloud API without consuming real resources.
All instances live in memory with configurable state transitions.
Records every operation for test assertions.
"""

from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone

from elastic_agent.core.providers.base import (
    CloudIdentity,
    CloudProvider,
    ElasticIp,
    Instance,
    InstanceConfig,
    InstanceNotFoundError,
    InstanceState,
)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class DryRunOperation:
    action: str
    instance_id: str
    timestamp: datetime = field(default_factory=_utcnow)
    details: dict = field(default_factory=dict)


class DryRunProvider(CloudProvider):
    """In-memory cloud provider that simulates instance lifecycle.

    Usage::

        provider = DryRunProvider()
        inst = await provider.create_instance(config)
        assert inst.state == InstanceState.RUNNING
        assert len(provider.operations) == 1

        # Inject failures
        provider.fail_next("create_instance", RuntimeError("quota exceeded"))

        # Configurable delays
        provider = DryRunProvider(create_delay=0.5)
    """

    PLATFORM = "dryrun"

    async def get_identity(self) -> CloudIdentity:
        return CloudIdentity(
            provider="dryrun", account_id="dryrun-account", region=""
        )

    def __init__(
        self,
        *,
        create_delay: float = 0.0,
        start_delay: float = 0.0,
        terminate_delay: float = 0.0,
        auto_assign_ip: bool = True,
    ) -> None:
        self.instances: dict[str, Instance] = {}
        self.eips: dict[str, ElasticIp] = {}
        self.operations: list[DryRunOperation] = []
        self._create_delay = create_delay
        self._start_delay = start_delay
        self._terminate_delay = terminate_delay
        self._auto_assign_ip = auto_assign_ip
        self._next_failures: dict[str, Exception] = {}
        self._counter = 0
        self._lock = asyncio.Lock()

    def fail_next(self, method: str, error: Exception) -> None:
        """Make the next call to *method* raise *error*."""
        self._next_failures[method] = error

    def _check_failure(self, method: str) -> None:
        exc = self._next_failures.pop(method, None)
        if exc is not None:
            raise exc

    def _gen_id(self) -> str:
        self._counter += 1
        short = uuid.uuid4().hex[:8]
        return f"i-dryrun-{short}"

    def _gen_ip(self) -> str:
        self._counter += 1
        a = (self._counter >> 8) & 0xFF
        b = self._counter & 0xFF
        return f"10.0.{a}.{b}"

    def _record(self, action: str, instance_id: str, **details: object) -> None:
        self.operations.append(DryRunOperation(
            action=action,
            instance_id=instance_id,
            details=details,
        ))

    def get_operations(self, action: str | None = None) -> list[DryRunOperation]:
        if action is None:
            return list(self.operations)
        return [op for op in self.operations if op.action == action]

    def reset(self) -> None:
        """Clear all instances and recorded operations."""
        self.instances.clear()
        self.operations.clear()
        self._next_failures.clear()
        self._counter = 0

    async def create_instance(self, config: InstanceConfig) -> Instance:
        self._check_failure("create_instance")
        if self._create_delay > 0:
            await asyncio.sleep(self._create_delay)

        native_id = self._gen_id()
        instance_id = f"{self.PLATFORM}:{native_id}"
        now = _utcnow()

        tags = dict(config.tags)
        tags[self.MANAGED_TAG_KEY] = self.MANAGED_TAG_VALUE

        public_ip = self._gen_ip() if self._auto_assign_ip else None
        private_ip = self._gen_ip() if self._auto_assign_ip else None

        inst = Instance(
            instance_id=instance_id,
            platform=self.PLATFORM,
            native_id=native_id,
            state=InstanceState.RUNNING,
            public_ip=public_ip,
            private_ip=private_ip,
            instance_type=config.instance_type,
            image_id=config.image_id,
            region="dryrun-region",
            zone="dryrun-zone-a",
            tags=tags,
            created_at=now,
            launched_at=now,
        )
        async with self._lock:
            self.instances[instance_id] = inst
        self._record("create", instance_id, config=config.model_dump())
        return inst

    async def start_instance(self, instance_id: str) -> None:
        self._check_failure("start_instance")
        if self._start_delay > 0:
            await asyncio.sleep(self._start_delay)

        async with self._lock:
            inst = self.instances.get(instance_id)
            if inst is None:
                raise ValueError(f"Instance {instance_id} not found")
            inst.state = InstanceState.RUNNING
            inst.launched_at = _utcnow()
        self._record("start", instance_id)

    async def stop_instance(self, instance_id: str) -> None:
        self._check_failure("stop_instance")

        async with self._lock:
            inst = self.instances.get(instance_id)
            if inst is None:
                raise ValueError(f"Instance {instance_id} not found")
            inst.state = InstanceState.STOPPED
        self._record("stop", instance_id)

    async def reboot_instance(self, instance_id: str) -> None:
        self._check_failure("reboot_instance")

        async with self._lock:
            inst = self.instances.get(instance_id)
            if inst is None:
                raise ValueError(f"Instance {instance_id} not found")
            inst.state = InstanceState.RUNNING
        self._record("reboot", instance_id)

    async def terminate_instance(self, instance_id: str) -> None:
        self._check_failure("terminate_instance")
        if self._terminate_delay > 0:
            await asyncio.sleep(self._terminate_delay)

        async with self._lock:
            inst = self.instances.get(instance_id)
            if inst is None:
                raise ValueError(f"Instance {instance_id} not found")
            inst.state = InstanceState.TERMINATED
        self._record("terminate", instance_id)

    async def list_instances(self, filters: dict[str, str] | None = None) -> list[Instance]:
        self._check_failure("list_instances")
        async with self._lock:
            instances = list(self.instances.values())

        if filters:
            result = []
            for inst in instances:
                match = all(inst.tags.get(k) == v for k, v in filters.items())
                if match:
                    result.append(inst)
            return result
        return instances

    async def get_instance(self, instance_id: str) -> Instance:
        self._check_failure("get_instance")
        async with self._lock:
            inst = self.instances.get(instance_id)
        if inst is None:
            raise InstanceNotFoundError(f"Instance {instance_id} not found")
        return inst

    async def wait_until_running(self, instance_id: str, timeout: int = 300) -> Instance:
        self._check_failure("wait_until_running")
        async with self._lock:
            inst = self.instances.get(instance_id)
        if inst is None:
            raise ValueError(f"Instance {instance_id} not found")
        inst.state = InstanceState.RUNNING
        return inst

    # -- Elastic IP --------------------------------------------------------

    async def allocate_eip(self, tags: dict[str, str] | None = None) -> ElasticIp:
        self._check_failure("allocate_eip")
        async with self._lock:
            self._counter += 1
            alloc_id = f"eipalloc-{self._counter:04d}"
            public_ip = f"52.0.0.{self._counter}"
            eip = ElasticIp(
                allocation_id=alloc_id,
                public_ip=public_ip,
                tags={
                    **(tags or {}),
                    self.MANAGED_TAG_KEY: self.MANAGED_TAG_VALUE,
                },
            )
            self.eips[alloc_id] = eip
        self._record("allocate_eip", alloc_id, public_ip=public_ip)
        return eip.model_copy()

    async def associate_eip(self, instance_id: str, allocation_id: str) -> ElasticIp:
        self._check_failure("associate_eip")
        async with self._lock:
            eip = self.eips.get(allocation_id)
            if eip is None:
                raise ValueError(f"EIP {allocation_id} not found")
            if eip.instance_id is not None and eip.instance_id != instance_id:
                raise RuntimeError(
                    f"EIP {allocation_id} is already attached to {eip.instance_id}"
                )
            eip.instance_id = instance_id
            eip.association_id = f"eipassoc-{allocation_id[-4:]}"
            # A bound instance always shows the EIP as its public IP, and it
            # stays put across stop/start (that's the whole point).
            inst = self.instances.get(instance_id)
            if inst is not None:
                inst.public_ip = eip.public_ip
        self._record("associate_eip", instance_id, allocation_id=allocation_id)
        return eip.model_copy()

    async def disassociate_eip(
        self,
        allocation_id: str,
        *,
        association_id: str | None = None,
        expected_instance_id: str | None = None,
    ) -> None:
        self._check_failure("disassociate_eip")
        async with self._lock:
            eip = self.eips.get(allocation_id)
            if eip is None:
                return
            if expected_instance_id and eip.instance_id != expected_instance_id:
                raise RuntimeError(
                    f"EIP {allocation_id} is attached to {eip.instance_id}, "
                    f"not {expected_instance_id}"
                )
            if association_id and eip.association_id != association_id:
                raise RuntimeError(
                    f"EIP {allocation_id} association changed to {eip.association_id}"
                )
            eip.instance_id = None
            eip.association_id = None
        self._record("disassociate_eip", allocation_id)

    async def release_eip(self, allocation_id: str) -> None:
        self._check_failure("release_eip")
        async with self._lock:
            self.eips.pop(allocation_id, None)
        self._record("release_eip", allocation_id)

    async def tag_eip(
        self, allocation_id: str, tags: dict[str, str]
    ) -> None:
        self._check_failure("tag_eip")
        async with self._lock:
            eip = self.eips.get(allocation_id)
            if eip is None:
                raise ValueError(f"EIP {allocation_id} not found")
            eip.tags.update({
                **tags,
                self.MANAGED_TAG_KEY: self.MANAGED_TAG_VALUE,
            })
        self._record("tag_eip", allocation_id, tags=dict(tags))

    async def describe_eip(self, allocation_id: str) -> ElasticIp | None:
        self._check_failure("describe_eip")
        async with self._lock:
            eip = self.eips.get(allocation_id)
            return eip.model_copy() if eip else None

    async def list_eips(self, filters: dict[str, str] | None = None) -> list[ElasticIp]:
        self._check_failure("list_eips")
        async with self._lock:
            values = list(self.eips.values())
            if filters:
                values = [
                    eip for eip in values
                    if all(eip.tags.get(key) == value for key, value in filters.items())
                ]
            return [eip.model_copy(deep=True) for eip in values]
