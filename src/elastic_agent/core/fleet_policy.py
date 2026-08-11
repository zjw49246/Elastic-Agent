"""Private, hot-reloadable defaults for future Elastic fleet instances."""

from __future__ import annotations

import json
import os
import re
import stat
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from elastic_agent.core.secure_store import atomic_write_private, secure_state_directory

FLEET_POLICY_SCHEMA_VERSION = 1
MIN_ROOT_DISK_GB = 8
MAX_ROOT_DISK_GB = 2048
MAX_FLEET_INSTANCES = 100
MAX_POLICY_BYTES = 16 * 1024
_INSTANCE_TYPE = re.compile(r"[a-z0-9][a-z0-9.-]{0,62}[a-z0-9]\Z")


class FleetRuntimePolicyError(RuntimeError):
    """The Manager-local fleet policy is unsafe or invalid."""


@dataclass(frozen=True, slots=True)
class FleetRuntimePolicy:
    default_instance_type: str
    default_root_disk_gb: int
    max_instances: int

    def to_dict(self) -> dict[str, int | str]:
        return {
            "default_instance_type": self.default_instance_type,
            "default_root_disk_gb": self.default_root_disk_gb,
            "max_instances": self.max_instances,
        }


def _instance_type(value: object) -> str:
    if not isinstance(value, str) or _INSTANCE_TYPE.fullmatch(value) is None:
        raise FleetRuntimePolicyError("default instance type is invalid")
    return value


def _bounded_int(value: object, *, minimum: int, maximum: int, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise FleetRuntimePolicyError(
            f"{label} must be an integer from {minimum} to {maximum}"
        )
    return value


def validated_policy(
    *,
    default_instance_type: object,
    default_root_disk_gb: object,
    max_instances: object,
) -> FleetRuntimePolicy:
    return FleetRuntimePolicy(
        default_instance_type=_instance_type(default_instance_type),
        default_root_disk_gb=_bounded_int(
            default_root_disk_gb,
            minimum=MIN_ROOT_DISK_GB,
            maximum=MAX_ROOT_DISK_GB,
            label="default root disk size",
        ),
        max_instances=_bounded_int(
            max_instances,
            minimum=1,
            maximum=MAX_FLEET_INSTANCES,
            label="maximum instance count",
        ),
    )


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise FleetRuntimePolicyError("fleet policy contains duplicate fields")
        value[key] = item
    return value


class FleetRuntimePolicyStore:
    """Own one atomic 0600 policy file beneath the Manager state directory."""

    def __init__(self, path: str | Path, *, defaults: FleetRuntimePolicy) -> None:
        self.path = Path(path).expanduser()
        self._lock = threading.RLock()
        secure_state_directory(self.path.parent)
        self._validate_parent()
        self._policy = self._read() if os.path.lexists(self.path) else defaults
        if not os.path.lexists(self.path):
            self._persist(self._policy)

    def snapshot(self) -> FleetRuntimePolicy:
        with self._lock:
            return self._policy

    def update(
        self,
        *,
        default_instance_type: object,
        default_root_disk_gb: object,
        max_instances: object,
    ) -> FleetRuntimePolicy:
        candidate = validated_policy(
            default_instance_type=default_instance_type,
            default_root_disk_gb=default_root_disk_gb,
            max_instances=max_instances,
        )
        with self._lock:
            self._persist(candidate)
            self._policy = candidate
            return candidate

    def _validate_parent(self) -> None:
        metadata = os.lstat(self.path.parent)
        if (
            stat.S_ISLNK(metadata.st_mode)
            or not stat.S_ISDIR(metadata.st_mode)
            or metadata.st_uid != os.geteuid()
            or stat.S_IMODE(metadata.st_mode) != 0o700
        ):
            raise FleetRuntimePolicyError(
                "fleet policy directory must be owner-only"
            )

    def _read(self) -> FleetRuntimePolicy:
        descriptor = -1
        try:
            listed = os.lstat(self.path)
            descriptor = os.open(
                self.path,
                os.O_RDONLY
                | getattr(os, "O_NOFOLLOW", 0)
                | getattr(os, "O_CLOEXEC", 0),
            )
            opened = os.fstat(descriptor)
            if (
                stat.S_ISLNK(listed.st_mode)
                or not stat.S_ISREG(listed.st_mode)
                or not stat.S_ISREG(opened.st_mode)
                or (listed.st_dev, listed.st_ino) != (opened.st_dev, opened.st_ino)
                or opened.st_uid != os.geteuid()
                or stat.S_IMODE(opened.st_mode) != 0o600
                or opened.st_nlink != 1
                or not 1 <= opened.st_size <= MAX_POLICY_BYTES
            ):
                raise FleetRuntimePolicyError("fleet policy file is unsafe")
            encoded = os.read(descriptor, MAX_POLICY_BYTES + 1)
            after = os.fstat(descriptor)
            if (
                len(encoded) != opened.st_size
                or (opened.st_dev, opened.st_ino) != (after.st_dev, after.st_ino)
                or opened.st_mtime_ns != after.st_mtime_ns
            ):
                raise FleetRuntimePolicyError("fleet policy changed while reading")
        except FleetRuntimePolicyError:
            raise
        except OSError as error:
            raise FleetRuntimePolicyError("fleet policy is unavailable") from error
        finally:
            if descriptor >= 0:
                os.close(descriptor)
        try:
            value = json.loads(
                encoded.decode("utf-8", errors="strict"),
                object_pairs_hook=_unique_object,
                parse_constant=lambda item: (_ for _ in ()).throw(
                    FleetRuntimePolicyError(f"invalid numeric constant: {item}")
                ),
            )
        except (UnicodeError, json.JSONDecodeError, FleetRuntimePolicyError) as error:
            raise FleetRuntimePolicyError("fleet policy is invalid") from error
        expected = {
            "schema_version",
            "default_instance_type",
            "default_root_disk_gb",
            "max_instances",
        }
        if (
            not isinstance(value, dict)
            or set(value) != expected
            or type(value.get("schema_version")) is not int
            or value["schema_version"] != FLEET_POLICY_SCHEMA_VERSION
        ):
            raise FleetRuntimePolicyError("fleet policy schema is invalid")
        return validated_policy(
            default_instance_type=value["default_instance_type"],
            default_root_disk_gb=value["default_root_disk_gb"],
            max_instances=value["max_instances"],
        )

    def _persist(self, policy: FleetRuntimePolicy) -> None:
        payload = json.dumps(
            {
                "schema_version": FLEET_POLICY_SCHEMA_VERSION,
                **policy.to_dict(),
            },
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )
        if len(payload.encode("utf-8")) > MAX_POLICY_BYTES:
            raise FleetRuntimePolicyError("fleet policy is too large")
        try:
            atomic_write_private(self.path, payload)
        except OSError as error:
            raise FleetRuntimePolicyError(
                "fleet policy could not be written atomically"
            ) from error


__all__ = [
    "FLEET_POLICY_SCHEMA_VERSION",
    "MAX_FLEET_INSTANCES",
    "MAX_ROOT_DISK_GB",
    "MIN_ROOT_DISK_GB",
    "FleetRuntimePolicy",
    "FleetRuntimePolicyError",
    "FleetRuntimePolicyStore",
    "validated_policy",
]
