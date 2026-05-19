"""CloudProvider abstract base class and Instance/InstanceConfig data models."""

from __future__ import annotations

import enum
from abc import ABC, abstractmethod
from datetime import datetime

from pydantic import BaseModel, Field


class InstanceState(str, enum.Enum):
    PENDING = "pending"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    TERMINATED = "terminated"


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


class CloudProvider(ABC):
    MANAGED_TAG_KEY = "ManagedBy"
    MANAGED_TAG_VALUE = "elastic-agent"

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
    async def wait_until_running(self, instance_id: str, timeout: int = 300) -> Instance:
        ...
