"""Harness abstract base class and related interface definitions."""

from __future__ import annotations

import enum
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Callable


class WorkerLifecycle(str, enum.Enum):
    PERSISTENT = "persistent"
    EPHEMERAL = "ephemeral"


@dataclass
class WorkerCapacity:
    max_concurrent_tasks: int = 1


@dataclass
class BootstrapStep:
    name: str
    command: str | list[str]
    timeout: int = 300
    retry_count: int = 0
    env: dict[str, str] = field(default_factory=dict)
    cwd: str | None = None
    description: str = ""


@dataclass
class FileSyncConfig:
    enabled: bool = False
    debounce_tiers: dict[str, float] = field(default_factory=dict)
    exclude_patterns: list[str] = field(default_factory=list)


@dataclass
class SyncMapping:
    task_id: str
    book_slug: str
    oss_prefix: str
    watch_paths: list[str]
    session_path_hash: str = ""


@dataclass
class ScalingSignal:
    desired_workers: int | None = None
    pending_tasks: int = 0
    reason: str = ""


@dataclass
class ServiceDefinition:
    name: str
    command: str | list[str]
    working_directory: str = "."
    env: dict[str, str] = field(default_factory=dict)
    restart_policy: str = "on-failure"


class Harness(ABC):

    def get_worker_lifecycle(self) -> WorkerLifecycle:
        return WorkerLifecycle.PERSISTENT

    @abstractmethod
    def get_bootstrap_steps(self) -> list[BootstrapStep]:
        ...

    def get_event_handlers(self) -> dict[str, Callable]:
        return {}

    def get_worker_capacity(self) -> WorkerCapacity:
        return WorkerCapacity(max_concurrent_tasks=1)

    def get_repo_url(self) -> str | None:
        return None

    def get_service_definitions(self) -> list[ServiceDefinition]:
        return []

    def get_app_credentials(self) -> list[str]:
        return []

    def get_health_check(self) -> dict[str, Any] | None:
        return None

    def get_scaling_signal(self) -> ScalingSignal | None:
        return None

    def get_credential_slots(self) -> list[dict[str, str]]:
        """Return credential slot definitions for auto-login after bootstrap.

        Each slot is a dict with 'slot_type' and 'config_dir'.
        Override in subclass to enable auto-login. Empty list disables it.
        """
        return []

    def get_file_sync_config(self) -> FileSyncConfig:
        return FileSyncConfig(enabled=False)

    def get_task_sync_mapping(self, task_context: dict[str, Any]) -> SyncMapping:
        raise NotImplementedError

    async def on_task_completed(self, task_id: str, result: dict[str, Any]) -> None:
        pass

    async def on_task_failed(self, task_id: str, error: dict[str, Any]) -> None:
        pass
