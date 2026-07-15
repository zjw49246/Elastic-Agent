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
    CloudProvider,
    Instance,
    InstanceConfig,
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

    def __init__(
        self,
        *,
        create_delay: float = 0.0,
        start_delay: float = 0.0,
        terminate_delay: float = 0.0,
        auto_assign_ip: bool = True,
    ) -> None:
        self.instances: dict[str, Instance] = {}
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
            raise ValueError(f"Instance {instance_id} not found")
        return inst

    async def wait_until_running(self, instance_id: str, timeout: int = 300) -> Instance:
        self._check_failure("wait_until_running")
        async with self._lock:
            inst = self.instances.get(instance_id)
        if inst is None:
            raise ValueError(f"Instance {instance_id} not found")
        inst.state = InstanceState.RUNNING
        return inst
