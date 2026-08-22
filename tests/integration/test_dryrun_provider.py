"""T-118: DryRunProvider end-to-end validation.

Verifies that the DryRunProvider correctly simulates the full cloud provider
contract: create, list, get, start, stop, terminate, tags, failure injection.
"""

from __future__ import annotations

import asyncio

import pytest

from elastic_agent.core.providers.base import InstanceConfig, InstanceState
from elastic_agent.testing import DryRunProvider


def _cfg(**overrides) -> InstanceConfig:
    defaults = {"instance_type": "ecs.test", "image_id": "img-test", "key_pair_name": "test-key"}
    defaults.update(overrides)
    return InstanceConfig(**defaults)


@pytest.mark.level1
class TestDryRunProviderE2E:
    """DryRunProvider full-contract validation."""

    @pytest.mark.asyncio
    async def test_create_returns_running_instance(self):
        provider = DryRunProvider()
        config = _cfg(tags={"env": "test"})

        inst = await provider.create_instance(config)

        assert inst.state == InstanceState.RUNNING
        assert inst.platform == "dryrun"
        assert inst.instance_type == "ecs.test"
        assert inst.public_ip is not None
        assert inst.private_ip is not None
        assert inst.tags.get("env") == "test"
        assert inst.tags.get("ManagedBy") == "elastic-agent"

    @pytest.mark.asyncio
    async def test_lifecycle_create_stop_start_terminate(self):
        provider = DryRunProvider()
        config = _cfg()

        inst = await provider.create_instance(config)
        assert inst.state == InstanceState.RUNNING

        await provider.stop_instance(inst.instance_id)
        stopped = await provider.get_instance(inst.instance_id)
        assert stopped.state == InstanceState.STOPPED

        await provider.start_instance(inst.instance_id)
        started = await provider.get_instance(inst.instance_id)
        assert started.state == InstanceState.RUNNING

        await provider.terminate_instance(inst.instance_id)
        terminated = await provider.get_instance(inst.instance_id)
        assert terminated.state == InstanceState.TERMINATED

    @pytest.mark.asyncio
    async def test_list_instances_with_filter(self):
        provider = DryRunProvider()
        c1 = _cfg(instance_type="t1", image_id="i1", tags={"group": "a"})
        c2 = _cfg(instance_type="t2", image_id="i2", tags={"group": "b"})

        await provider.create_instance(c1)
        await provider.create_instance(c2)

        all_inst = await provider.list_instances()
        assert len(all_inst) == 2

        group_a = await provider.list_instances({"group": "a"})
        assert len(group_a) == 1
        assert group_a[0].tags["group"] == "a"

    @pytest.mark.asyncio
    async def test_get_nonexistent_raises(self):
        provider = DryRunProvider()
        with pytest.raises(ValueError, match="not found"):
            await provider.get_instance("dryrun:nonexistent")

    @pytest.mark.asyncio
    async def test_wait_until_running(self):
        provider = DryRunProvider()
        config = _cfg()
        inst = await provider.create_instance(config)

        await provider.stop_instance(inst.instance_id)
        result = await provider.wait_until_running(inst.instance_id)
        assert result.state == InstanceState.RUNNING

    @pytest.mark.asyncio
    async def test_operations_recorded(self):
        provider = DryRunProvider()
        config = _cfg()

        inst = await provider.create_instance(config)
        await provider.stop_instance(inst.instance_id)
        await provider.terminate_instance(inst.instance_id)

        assert len(provider.get_operations("create")) == 1
        assert len(provider.get_operations("stop")) == 1
        assert len(provider.get_operations("terminate")) == 1
        assert len(provider.get_operations()) == 3

    @pytest.mark.asyncio
    async def test_failure_injection(self):
        provider = DryRunProvider()

        provider.fail_next("create_instance", RuntimeError("quota exceeded"))
        with pytest.raises(RuntimeError, match="quota exceeded"):
            await provider.create_instance(_cfg())

        inst = await provider.create_instance(_cfg())
        assert inst.state == InstanceState.RUNNING

    @pytest.mark.asyncio
    async def test_configurable_delays(self):
        provider = DryRunProvider(create_delay=0.1, terminate_delay=0.1)
        config = _cfg()

        import time
        start = time.monotonic()
        inst = await provider.create_instance(config)
        create_dur = time.monotonic() - start
        assert create_dur >= 0.09

        start = time.monotonic()
        await provider.terminate_instance(inst.instance_id)
        term_dur = time.monotonic() - start
        assert term_dur >= 0.09

    @pytest.mark.asyncio
    async def test_concurrent_creates(self):
        provider = DryRunProvider()
        configs = [_cfg(instance_type=f"t{i}") for i in range(10)]

        instances = await asyncio.gather(
            *[provider.create_instance(c) for c in configs]
        )

        assert len(instances) == 10
        ids = {inst.instance_id for inst in instances}
        assert len(ids) == 10

        all_inst = await provider.list_instances()
        assert len(all_inst) == 10

    @pytest.mark.asyncio
    async def test_reset_clears_state(self):
        provider = DryRunProvider()
        await provider.create_instance(_cfg())
        provider.fail_next("list_instances", RuntimeError("boom"))

        provider.reset()

        assert len(provider.instances) == 0
        assert len(provider.operations) == 0
        instances = await provider.list_instances()
        assert len(instances) == 0

    @pytest.mark.asyncio
    async def test_managed_tag_always_set(self):
        provider = DryRunProvider()
        config = _cfg(tags={})
        inst = await provider.create_instance(config)
        assert inst.tags.get("ManagedBy") == "elastic-agent"

    @pytest.mark.asyncio
    async def test_multiple_failure_injections_independent(self):
        provider = DryRunProvider()
        provider.fail_next("create_instance", ValueError("err1"))
        provider.fail_next("list_instances", ValueError("err2"))

        with pytest.raises(ValueError, match="err1"):
            await provider.create_instance(_cfg())

        with pytest.raises(ValueError, match="err2"):
            await provider.list_instances()

        inst = await provider.create_instance(_cfg())
        assert inst is not None
