"""T-128: Spot/preemptible instance handling — reclamation event detection + status update."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from elastic_agent.core.providers.base import (
    CloudProvider,
    Instance,
    InstanceConfig,
    InstanceState,
)
from elastic_agent.core.config import ElasticAgentConfig, AliyunProviderConfig, AWSProviderConfig
from elastic_agent.testing.dry_run_provider import DryRunProvider


# ---------------------------------------------------------------------------
# InstanceConfig spot flag
# ---------------------------------------------------------------------------


class TestInstanceConfigSpot:

    def test_spot_default_false(self):
        config = InstanceConfig(
            instance_type="ecs.c6.large",
            image_id="m-test",
            key_pair_name="key",
        )
        assert config.spot is False

    def test_spot_flag_true(self):
        config = InstanceConfig(
            instance_type="ecs.c6.large",
            image_id="m-test",
            key_pair_name="key",
            spot=True,
        )
        assert config.spot is True

    def test_spot_flag_serialization(self):
        config = InstanceConfig(
            instance_type="t3.large",
            image_id="ami-test",
            key_pair_name="key",
            spot=True,
        )
        d = config.model_dump()
        assert d["spot"] is True

        restored = InstanceConfig.model_validate(d)
        assert restored.spot is True


# ---------------------------------------------------------------------------
# DryRunProvider with spot instances
# ---------------------------------------------------------------------------


class TestDryRunProviderSpot:

    @pytest.mark.asyncio
    async def test_create_spot_instance(self):
        provider = DryRunProvider()
        config = InstanceConfig(
            instance_type="ecs.c6.large",
            image_id="m-test",
            key_pair_name="key",
            spot=True,
        )
        inst = await provider.create_instance(config)
        assert inst.state == InstanceState.RUNNING
        ops = provider.get_operations("create")
        assert len(ops) == 1
        assert ops[0].details["config"]["spot"] is True

    @pytest.mark.asyncio
    async def test_terminate_spot_instance(self):
        provider = DryRunProvider()
        config = InstanceConfig(
            instance_type="ecs.c6.large",
            image_id="m-test",
            key_pair_name="key",
            spot=True,
        )
        inst = await provider.create_instance(config)
        await provider.terminate_instance(inst.instance_id)

        updated = await provider.get_instance(inst.instance_id)
        assert updated.state == InstanceState.TERMINATED

    @pytest.mark.asyncio
    async def test_spot_and_ondemand_coexist(self):
        provider = DryRunProvider()
        spot = InstanceConfig(instance_type="c6.large", image_id="m", key_pair_name="k", spot=True)
        od = InstanceConfig(instance_type="c6.large", image_id="m", key_pair_name="k", spot=False)

        i_spot = await provider.create_instance(spot)
        i_od = await provider.create_instance(od)

        assert len(provider.instances) == 2
        ops = provider.get_operations("create")
        assert ops[0].details["config"]["spot"] is True
        assert ops[1].details["config"]["spot"] is False


# ---------------------------------------------------------------------------
# Instance state transitions (reclamation simulation)
# ---------------------------------------------------------------------------


class TestInstanceStateTransitions:

    @pytest.mark.asyncio
    async def test_running_to_terminated(self):
        provider = DryRunProvider()
        config = InstanceConfig(instance_type="c6", image_id="m", key_pair_name="k", spot=True)
        inst = await provider.create_instance(config)
        assert inst.state == InstanceState.RUNNING

        await provider.terminate_instance(inst.instance_id)
        updated = await provider.get_instance(inst.instance_id)
        assert updated.state == InstanceState.TERMINATED

    @pytest.mark.asyncio
    async def test_running_to_stopped(self):
        provider = DryRunProvider()
        config = InstanceConfig(instance_type="c6", image_id="m", key_pair_name="k", spot=True)
        inst = await provider.create_instance(config)

        await provider.stop_instance(inst.instance_id)
        updated = await provider.get_instance(inst.instance_id)
        assert updated.state == InstanceState.STOPPED

    @pytest.mark.asyncio
    async def test_stopped_to_running(self):
        provider = DryRunProvider()
        config = InstanceConfig(instance_type="c6", image_id="m", key_pair_name="k")
        inst = await provider.create_instance(config)
        await provider.stop_instance(inst.instance_id)
        await provider.start_instance(inst.instance_id)

        updated = await provider.get_instance(inst.instance_id)
        assert updated.state == InstanceState.RUNNING


# ---------------------------------------------------------------------------
# InstanceState enum
# ---------------------------------------------------------------------------


class TestInstanceStateEnum:

    def test_all_states_defined(self):
        states = {s.value for s in InstanceState}
        assert "pending" in states
        assert "starting" in states
        assert "running" in states
        assert "stopping" in states
        assert "stopped" in states
        assert "terminated" in states

    def test_state_string_values(self):
        assert InstanceState.PENDING == "pending"
        assert InstanceState.RUNNING == "running"
        assert InstanceState.TERMINATED == "terminated"


# ---------------------------------------------------------------------------
# Config spot_enabled flag
# ---------------------------------------------------------------------------


class TestConfigSpotEnabled:

    def test_aliyun_spot_enabled_default_false(self):
        cfg = AliyunProviderConfig(
            region_id="cn-hangzhou",
            image_id="m-test",
            instance_type="ecs.c6.large",
        )
        assert cfg.spot_enabled is False

    def test_aliyun_spot_enabled_true(self):
        cfg = AliyunProviderConfig(
            region_id="cn-hangzhou",
            image_id="m-test",
            instance_type="ecs.c6.large",
            spot_enabled=True,
        )
        assert cfg.spot_enabled is True

    def test_aws_config_has_no_spot_enabled_by_default(self):
        cfg = AWSProviderConfig(
            region="us-east-1",
            ami_id="ami-test",
            default_instance_type="t3.large",
        )
        assert hasattr(cfg, "region")


# ---------------------------------------------------------------------------
# Provider fail_next for simulating reclamation
# ---------------------------------------------------------------------------


class TestProviderFailNext:

    @pytest.mark.asyncio
    async def test_fail_next_create(self):
        provider = DryRunProvider()
        provider.fail_next("create_instance", RuntimeError("quota exceeded"))
        with pytest.raises(RuntimeError, match="quota exceeded"):
            await provider.create_instance(
                InstanceConfig(instance_type="c6", image_id="m", key_pair_name="k")
            )

    @pytest.mark.asyncio
    async def test_fail_next_terminate(self):
        provider = DryRunProvider()
        config = InstanceConfig(instance_type="c6", image_id="m", key_pair_name="k", spot=True)
        inst = await provider.create_instance(config)

        provider.fail_next("terminate_instance", RuntimeError("API error"))
        with pytest.raises(RuntimeError, match="API error"):
            await provider.terminate_instance(inst.instance_id)

    @pytest.mark.asyncio
    async def test_fail_next_only_once(self):
        provider = DryRunProvider()
        provider.fail_next("create_instance", RuntimeError("transient"))
        with pytest.raises(RuntimeError):
            await provider.create_instance(
                InstanceConfig(instance_type="c6", image_id="m", key_pair_name="k")
            )
        inst = await provider.create_instance(
            InstanceConfig(instance_type="c6", image_id="m", key_pair_name="k")
        )
        assert inst.state == InstanceState.RUNNING

    @pytest.mark.asyncio
    async def test_fail_next_get_instance(self):
        provider = DryRunProvider()
        config = InstanceConfig(instance_type="c6", image_id="m", key_pair_name="k", spot=True)
        inst = await provider.create_instance(config)

        provider.fail_next("get_instance", RuntimeError("timeout"))
        with pytest.raises(RuntimeError, match="timeout"):
            await provider.get_instance(inst.instance_id)


# ---------------------------------------------------------------------------
# Tags for spot identification
# ---------------------------------------------------------------------------


class TestSpotTags:

    @pytest.mark.asyncio
    async def test_managed_tag_applied(self):
        provider = DryRunProvider()
        config = InstanceConfig(
            instance_type="c6", image_id="m", key_pair_name="k",
            spot=True,
            tags={"Name": "spot-worker-1"},
        )
        inst = await provider.create_instance(config)
        assert inst.tags["ManagedBy"] == "elastic-agent"
        assert inst.tags["Name"] == "spot-worker-1"

    @pytest.mark.asyncio
    async def test_filter_by_tags(self):
        provider = DryRunProvider()
        for i in range(3):
            await provider.create_instance(InstanceConfig(
                instance_type="c6", image_id="m", key_pair_name="k",
                tags={"env": "prod" if i < 2 else "staging"},
            ))
        prod = await provider.list_instances(filters={"env": "prod"})
        assert len(prod) == 2

    @pytest.mark.asyncio
    async def test_list_only_managed_instances(self):
        provider = DryRunProvider()
        await provider.create_instance(InstanceConfig(
            instance_type="c6", image_id="m", key_pair_name="k",
        ))
        managed = await provider.list_instances(
            filters={CloudProvider.MANAGED_TAG_KEY: CloudProvider.MANAGED_TAG_VALUE}
        )
        assert len(managed) == 1


# ---------------------------------------------------------------------------
# DryRunProvider operations log
# ---------------------------------------------------------------------------


class TestOperationsLog:

    @pytest.mark.asyncio
    async def test_operations_recorded(self):
        provider = DryRunProvider()
        config = InstanceConfig(instance_type="c6", image_id="m", key_pair_name="k", spot=True)
        inst = await provider.create_instance(config)
        await provider.stop_instance(inst.instance_id)
        await provider.start_instance(inst.instance_id)
        await provider.terminate_instance(inst.instance_id)

        ops = provider.operations
        actions = [op.action for op in ops]
        assert actions == ["create", "stop", "start", "terminate"]

    @pytest.mark.asyncio
    async def test_reset_clears_all(self):
        provider = DryRunProvider()
        await provider.create_instance(
            InstanceConfig(instance_type="c6", image_id="m", key_pair_name="k")
        )
        provider.reset()
        assert len(provider.instances) == 0
        assert len(provider.operations) == 0

    @pytest.mark.asyncio
    async def test_get_operations_by_action(self):
        provider = DryRunProvider()
        config = InstanceConfig(instance_type="c6", image_id="m", key_pair_name="k")
        inst = await provider.create_instance(config)
        await provider.terminate_instance(inst.instance_id)

        creates = provider.get_operations("create")
        terminates = provider.get_operations("terminate")
        assert len(creates) == 1
        assert len(terminates) == 1
