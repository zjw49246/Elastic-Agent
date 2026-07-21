"""CloudProvider abstract base class and Instance/InstanceConfig data models."""

from __future__ import annotations

import enum
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime

from pydantic import BaseModel, Field


class InstanceState(str, enum.Enum):
    PENDING = "pending"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    TERMINATED = "terminated"


class InstanceNotFoundError(ValueError):
    """Provider definitively reports that an exact instance id does not exist."""


class InstanceConfig(BaseModel):
    instance_type: str
    image_id: str
    key_pair_name: str
    security_group_id: str | None = None
    security_group_ids: list[str] = Field(default_factory=list)
    subnet_id: str | None = None
    vswitch_id: str | None = None
    spot: bool = False
    tags: dict[str, str] = Field(default_factory=dict)
    user_data: str | None = None
    root_disk_size_gb: int = 40
    root_disk_type: str = "cloud_essd"
    # Provider idempotency key. AWS maps this to RunInstances.ClientToken.
    client_token: str | None = None


class Instance(BaseModel):
    instance_id: str
    platform: str
    native_id: str
    state: InstanceState
    public_ip: str | None = None
    private_ip: str | None = None
    instance_type: str | None = None
    image_id: str | None = None
    region: str | None = None
    zone: str | None = None
    tags: dict[str, str] = Field(default_factory=dict)
    created_at: datetime | None = None
    launched_at: datetime | None = None


class ElasticIp(BaseModel):
    """A static public IP (AWS EIP) retained across temporary instances.

    The allocation is bound 1:1 to an account.  A job temporarily associates
    it with a newly-created instance, then detaches it before that instance is
    terminated.  ``association_id`` is present only during that job lease.
    """

    allocation_id: str
    public_ip: str
    association_id: str | None = None
    instance_id: str | None = None
    tags: dict[str, str] = Field(default_factory=dict)


@dataclass(frozen=True)
class CloudIdentity:
    """Cloud control-plane scope that owns persistent resources."""

    provider: str = ""
    account_id: str = ""
    region: str = ""


class CloudProvider(ABC):
    MANAGED_TAG_KEY = "ManagedBy"
    MANAGED_TAG_VALUE = "elastic-agent"

    async def get_identity(self) -> CloudIdentity:
        """Return provider/account/region used for durable-resource guards.

        Providers without persistent EIP support may keep the empty default.
        AWS overrides this with STS caller identity and configured region.
        """
        return CloudIdentity()

    @abstractmethod
    async def create_instance(self, config: InstanceConfig) -> Instance:
        ...

    @abstractmethod
    async def start_instance(self, instance_id: str) -> None:
        ...

    @abstractmethod
    async def stop_instance(self, instance_id: str) -> None:
        ...

    @abstractmethod
    async def terminate_instance(self, instance_id: str) -> None:
        ...

    @abstractmethod
    async def list_instances(self, filters: dict[str, str] | None = None) -> list[Instance]:
        ...

    @abstractmethod
    async def get_instance(self, instance_id: str) -> Instance:
        ...

    @abstractmethod
    async def reboot_instance(self, instance_id: str) -> None:
        ...

    @abstractmethod
    async def wait_until_running(self, instance_id: str, timeout: int = 300) -> Instance:
        ...

    # -- Elastic IP (static public IP) -------------------------------------
    # Optional capability: only providers backing the account↔IP binding
    # feature implement these (AWS today). Others inherit the NotImplemented
    # default so adding the API here does not force every subclass to change.

    async def allocate_eip(self, tags: dict[str, str] | None = None) -> ElasticIp:
        """Allocate a new static public IP. Durable until ``release_eip``."""
        raise NotImplementedError("Elastic IPs are not supported by this provider")

    async def associate_eip(self, instance_id: str, allocation_id: str) -> ElasticIp:
        """Attach a detached EIP; never steal it from another instance."""
        raise NotImplementedError("Elastic IPs are not supported by this provider")

    async def disassociate_eip(
        self,
        allocation_id: str,
        *,
        association_id: str | None = None,
        expected_instance_id: str | None = None,
    ) -> None:
        """Detach the expected association while retaining the allocation.

        Passing the association observed by the caller prevents a stale lease
        from looking up and detaching a newer owner's association.
        """
        raise NotImplementedError("Elastic IPs are not supported by this provider")

    async def release_eip(self, allocation_id: str) -> None:
        """Permanently release an allocated EIP (only on account decommission)."""
        raise NotImplementedError("Elastic IPs are not supported by this provider")

    async def tag_eip(
        self, allocation_id: str, tags: dict[str, str]
    ) -> None:
        """Apply ownership tags to an EIP and preserve the managed marker."""
        raise NotImplementedError("Elastic IPs are not supported by this provider")

    async def describe_eip(self, allocation_id: str) -> ElasticIp | None:
        """Look up an EIP by allocation id, or None if it no longer exists."""
        raise NotImplementedError("Elastic IPs are not supported by this provider")

    async def list_eips(
        self, filters: dict[str, str] | None = None
    ) -> list[ElasticIp]:
        """List managed EIPs, optionally filtered by exact tag values."""
        raise NotImplementedError("Elastic IPs are not supported by this provider")
