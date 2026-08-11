from __future__ import annotations

import json
import os
import stat

import pytest

from elastic_agent.core.fleet_policy import (
    FleetRuntimePolicyError,
    FleetRuntimePolicyStore,
    validated_policy,
)


def test_policy_is_private_persistent_and_reloaded(tmp_path) -> None:
    path = tmp_path / "state" / "fleet-runtime-policy.json"
    store = FleetRuntimePolicyStore(
        path,
        defaults=validated_policy(
            default_instance_type="t3.large",
            default_root_disk_gb=40,
            max_instances=30,
        ),
    )

    assert stat.S_IMODE(path.parent.stat().st_mode) == 0o700
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    store.update(
        default_instance_type="c7i.2xlarge",
        default_root_disk_gb=200,
        max_instances=12,
    )

    reloaded = FleetRuntimePolicyStore(
        path,
        defaults=validated_policy(
            default_instance_type="ignored.large",
            default_root_disk_gb=41,
            max_instances=1,
        ),
    ).snapshot()
    assert reloaded.to_dict() == {
        "default_instance_type": "c7i.2xlarge",
        "default_root_disk_gb": 200,
        "max_instances": 12,
    }


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("default_instance_type", "T3.LARGE"),
        ("default_root_disk_gb", 7),
        ("max_instances", 101),
        ("max_instances", True),
    ],
)
def test_policy_rejects_invalid_values(tmp_path, field, value) -> None:
    path = tmp_path / "fleet-runtime-policy.json"
    store = FleetRuntimePolicyStore(
        path,
        defaults=validated_policy(
            default_instance_type="t3.large",
            default_root_disk_gb=40,
            max_instances=30,
        ),
    )
    values = {
        "default_instance_type": "t3.large",
        "default_root_disk_gb": 40,
        "max_instances": 30,
    }
    values[field] = value

    with pytest.raises(FleetRuntimePolicyError):
        store.update(**values)


def test_policy_rejects_symlink_and_duplicate_fields(tmp_path) -> None:
    path = tmp_path / "fleet-runtime-policy.json"
    target = tmp_path / "target.json"
    target.write_text("{}", encoding="utf-8")
    os.chmod(target, 0o600)
    path.symlink_to(target)
    with pytest.raises(FleetRuntimePolicyError, match="unsafe|unavailable"):
        FleetRuntimePolicyStore(
            path,
            defaults=validated_policy(
                default_instance_type="t3.large",
                default_root_disk_gb=40,
                max_instances=30,
            ),
        )

    path.unlink()
    path.write_text(
        '{"schema_version":1,"default_instance_type":"t3.large",'
        '"default_instance_type":"m7i.large","default_root_disk_gb":40,'
        '"max_instances":30}',
        encoding="utf-8",
    )
    os.chmod(path, 0o600)
    with pytest.raises(FleetRuntimePolicyError, match="invalid"):
        FleetRuntimePolicyStore(
            path,
            defaults=validated_policy(
                default_instance_type="t3.large",
                default_root_disk_gb=40,
                max_instances=30,
            ),
        )


def test_policy_file_has_exact_public_schema(tmp_path) -> None:
    path = tmp_path / "fleet-runtime-policy.json"
    FleetRuntimePolicyStore(
        path,
        defaults=validated_policy(
            default_instance_type="t3.large",
            default_root_disk_gb=40,
            max_instances=30,
        ),
    )
    assert json.loads(path.read_text(encoding="utf-8")) == {
        "schema_version": 1,
        "default_instance_type": "t3.large",
        "default_root_disk_gb": 40,
        "max_instances": 30,
    }
