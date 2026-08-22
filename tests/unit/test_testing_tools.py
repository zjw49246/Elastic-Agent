"""Tests for elastic_agent.testing mock tools (T-200 ~ T-204)."""

import asyncio
import json
import pytest

from elastic_agent.core.providers.base import InstanceConfig, InstanceState
from elastic_agent.testing import (
    DryRunProvider,
    MockOSS,
    MockOAuthServer,
    OAuthScenario,
    create_test_manager,
    create_test_config,
)


# ---------------------------------------------------------------
# T-200: DryRunProvider
# ---------------------------------------------------------------


class TestDryRunProvider:
    @pytest.fixture
    def provider(self):
        return DryRunProvider()

    @pytest.fixture
    def config(self):
        return InstanceConfig(
            instance_type="ecs.c6.large",
            image_id="m-test",
            key_pair_name="test-key",
            security_group_id="sg-test",
            tags={"env": "test"},
        )

    async def test_create_instance(self, provider, config):
        inst = await provider.create_instance(config)
        assert inst.state == InstanceState.RUNNING
        assert inst.platform == "dryrun"
        assert inst.instance_id.startswith("dryrun:")
        assert inst.public_ip is not None
        assert inst.private_ip is not None
        assert inst.tags["ManagedBy"] == "elastic-agent"
        assert inst.tags["env"] == "test"

    async def test_create_records_operation(self, provider, config):
        await provider.create_instance(config)
        ops = provider.get_operations("create")
        assert len(ops) == 1
        assert ops[0].action == "create"

    async def test_terminate_instance(self, provider, config):
        inst = await provider.create_instance(config)
        await provider.terminate_instance(inst.instance_id)
        refreshed = await provider.get_instance(inst.instance_id)
        assert refreshed.state == InstanceState.TERMINATED
        assert len(provider.get_operations("terminate")) == 1

    async def test_stop_and_start(self, provider, config):
        inst = await provider.create_instance(config)
        await provider.stop_instance(inst.instance_id)
        stopped = await provider.get_instance(inst.instance_id)
        assert stopped.state == InstanceState.STOPPED

        await provider.start_instance(inst.instance_id)
        started = await provider.get_instance(inst.instance_id)
        assert started.state == InstanceState.RUNNING

    async def test_list_instances_no_filter(self, provider, config):
        await provider.create_instance(config)
        await provider.create_instance(config)
        instances = await provider.list_instances()
        assert len(instances) == 2

    async def test_list_instances_with_filter(self, provider, config):
        await provider.create_instance(config)
        config2 = config.model_copy()
        config2.tags = {"env": "prod"}
        await provider.create_instance(config2)

        test_only = await provider.list_instances({"env": "test"})
        assert len(test_only) == 1
        assert test_only[0].tags["env"] == "test"

    async def test_get_nonexistent_instance(self, provider):
        with pytest.raises(ValueError, match="not found"):
            await provider.get_instance("dryrun:nonexistent")

    async def test_wait_until_running(self, provider, config):
        inst = await provider.create_instance(config)
        await provider.stop_instance(inst.instance_id)
        result = await provider.wait_until_running(inst.instance_id)
        assert result.state == InstanceState.RUNNING

    async def test_fail_next(self, provider, config):
        provider.fail_next("create_instance", RuntimeError("quota exceeded"))
        with pytest.raises(RuntimeError, match="quota exceeded"):
            await provider.create_instance(config)
        inst = await provider.create_instance(config)
        assert inst.state == InstanceState.RUNNING

    async def test_reset(self, provider, config):
        await provider.create_instance(config)
        assert len(provider.instances) == 1
        provider.reset()
        assert len(provider.instances) == 0
        assert len(provider.operations) == 0

    async def test_create_with_delay(self, config):
        provider = DryRunProvider(create_delay=0.05)
        inst = await provider.create_instance(config)
        assert inst.state == InstanceState.RUNNING

    async def test_no_auto_ip(self, config):
        provider = DryRunProvider(auto_assign_ip=False)
        inst = await provider.create_instance(config)
        assert inst.public_ip is None
        assert inst.private_ip is None


# ---------------------------------------------------------------
# T-203: MockOSS
# ---------------------------------------------------------------


class TestMockOSS:
    @pytest.fixture
    def oss(self, tmp_path):
        return MockOSS(tmp_path / "oss")

    async def test_upload_and_read_bytes(self, oss):
        await oss.upload_bytes(b"hello world", "test/file.txt")
        data = await oss.read_file("test/file.txt")
        assert data == b"hello world"
        assert len(oss.uploads) == 1
        assert oss.uploads[0]["oss_key"] == "test/file.txt"

    async def test_upload_file(self, oss, tmp_path):
        src = tmp_path / "source.txt"
        src.write_text("file content")
        await oss.upload_file(str(src), "dest/file.txt")
        data = await oss.read_file("dest/file.txt")
        assert data == b"file content"

    async def test_file_exists(self, oss):
        assert not await oss.file_exists("nonexistent")
        await oss.upload_bytes(b"x", "exists.txt")
        assert await oss.file_exists("exists.txt")

    async def test_read_nonexistent(self, oss):
        with pytest.raises(FileNotFoundError):
            await oss.read_file("no-such-key")

    async def test_presigned_url(self, oss):
        assert await oss.get_presigned_url("missing") is None
        await oss.upload_bytes(b"x", "present.txt")
        url = await oss.get_presigned_url("present.txt")
        assert url is not None
        assert "present.txt" in url

    async def test_list_keys(self, oss):
        await oss.upload_bytes(b"a", "tasks/t1/file.txt")
        await oss.upload_bytes(b"b", "tasks/t2/file.txt")
        await oss.upload_bytes(b"c", "other/x.txt")
        keys = oss.list_keys("tasks/")
        assert len(keys) == 2
        assert all(k.startswith("tasks/") for k in keys)

    async def test_put_content_sync(self, oss):
        oss.put_content("pre/seeded.txt", "pre-seeded data")
        data = await oss.read_file("pre/seeded.txt")
        assert data == b"pre-seeded data"

    async def test_fail_next(self, oss):
        oss.fail_next("upload_bytes", IOError("disk full"))
        with pytest.raises(IOError, match="disk full"):
            await oss.upload_bytes(b"x", "key")
        await oss.upload_bytes(b"x", "key")

    async def test_reset(self, oss):
        await oss.upload_bytes(b"x", "key")
        oss.reset()
        assert len(oss.uploads) == 0
        assert await oss.file_exists("key")


# ---------------------------------------------------------------
# T-202: MockOAuthServer
# ---------------------------------------------------------------


class TestMockOAuthServer:
    async def test_default_scenario(self):
        server = MockOAuthServer()
        await server.start()
        try:
            import httpx
            async with httpx.AsyncClient(base_url=server.url) as client:
                resp = await client.post("/api/v1/claude/send", json={"email": "test@example.com"})
                assert resp.status_code == 200
                body = resp.json()
                assert "deviceId" in body

                resp = await client.get("/api/v1/getClaudeMessage")
                assert resp.status_code == 200
                assert resp.json()["code"] == "mock-magic-link-code"

                resp = await client.post("/api/v1/claude/verify", json={"code": "abc"})
                assert resp.status_code == 200
                assert resp.json()["cookie"] == "mock-session-cookie"

                resp = await client.get("/api/account")
                assert resp.status_code == 200
                assert resp.json()["email_address"] == "test@example.com"

                resp = await client.post("/v1/oauth/token", json={"code": "auth-code"})
                assert resp.status_code == 200
                assert resp.json()["access_token"] == "mock-access-token"

                resp = await client.get("/api/oauth/usage")
                assert resp.status_code == 200
                usage = resp.json()
                assert usage["five_hour"]["utilization"] == 20.0
        finally:
            await server.stop()

    async def test_failed_send(self):
        scenario = OAuthScenario(send_magic_link_success=False)
        server = MockOAuthServer(scenario=scenario)
        await server.start()
        try:
            import httpx
            async with httpx.AsyncClient(base_url=server.url) as client:
                resp = await client.post("/api/v1/claude/send", json={})
                assert resp.status_code == 500
        finally:
            await server.stop()

    async def test_rate_limited_usage(self):
        scenario = OAuthScenario(usage_rate_limited=True)
        server = MockOAuthServer(scenario=scenario)
        await server.start()
        try:
            import httpx
            async with httpx.AsyncClient(base_url=server.url) as client:
                resp = await client.get("/api/oauth/usage")
                assert resp.status_code == 429
        finally:
            await server.stop()

    async def test_poll_attempts(self):
        scenario = OAuthScenario(poll_attempts_before_code=2)
        server = MockOAuthServer(scenario=scenario)
        await server.start()
        try:
            import httpx
            async with httpx.AsyncClient(base_url=server.url) as client:
                resp1 = await client.get("/api/v1/getClaudeMessage")
                assert resp1.json()["code"] is None

                resp2 = await client.get("/api/v1/getClaudeMessage")
                assert resp2.json()["code"] is None

                resp3 = await client.get("/api/v1/getClaudeMessage")
                assert resp3.json()["code"] == "mock-magic-link-code"
        finally:
            await server.stop()

    async def test_request_tracking(self):
        server = MockOAuthServer()
        await server.start()
        try:
            import httpx
            async with httpx.AsyncClient(base_url=server.url) as client:
                await client.post("/api/v1/claude/send", json={"email": "a@b.com"})
                await client.get("/api/account")

            reqs = server.get_requests(path="/api/v1/claude/send")
            assert len(reqs) == 1
            assert reqs[0]["method"] == "POST"

            server.reset()
            assert len(server.requests) == 0
        finally:
            await server.stop()

    async def test_token_failure(self):
        scenario = OAuthScenario(token_success=False)
        server = MockOAuthServer(scenario=scenario)
        await server.start()
        try:
            import httpx
            async with httpx.AsyncClient(base_url=server.url) as client:
                resp = await client.post("/v1/oauth/token", json={})
                assert resp.status_code == 400
        finally:
            await server.stop()


# ---------------------------------------------------------------
# T-204: create_test_manager
# ---------------------------------------------------------------


class TestCreateTestManager:
    async def test_basic_creation(self, tmp_path):
        result = create_test_manager(tmp_dir=tmp_path / "test")
        assert result.manager is not None
        assert result.provider is not None
        assert result.oss is not None
        assert result.config is not None

    async def test_manager_starts_and_stops(self, tmp_path):
        result = create_test_manager(tmp_dir=tmp_path / "test")
        await result.manager.start()
        assert result.manager._started
        await result.manager.stop()
        assert not result.manager._started

    async def test_scale_out_with_dryrun(self, tmp_path):
        result = create_test_manager(tmp_dir=tmp_path / "test")
        await result.manager.start()
        try:
            nodes = await result.manager.scale_out(count=2)
            assert len(nodes) == 2
            assert len(result.provider.get_operations("create")) == 2
            for node in nodes:
                assert node.status.value == "creating"
        finally:
            await result.manager.stop()

    async def test_custom_provider(self, tmp_path):
        custom = DryRunProvider(create_delay=0.01)
        result = create_test_manager(provider=custom, tmp_dir=tmp_path / "test")
        assert result.provider is custom

    async def test_config_overrides(self, tmp_path):
        result = create_test_manager(
            tmp_dir=tmp_path / "test",
            config_overrides={"worker": {"heartbeat_interval": 99}},
        )
        assert result.config.worker.heartbeat_interval == 99

    async def test_oss_integration(self, tmp_path):
        result = create_test_manager(tmp_dir=tmp_path / "test")
        await result.oss.upload_bytes(b"test data", "tasks/t1/file.txt")
        data = await result.oss.read_file("tasks/t1/file.txt")
        assert data == b"test data"

    def test_create_test_config(self, tmp_path):
        config, resolved = create_test_config(tmp_path / "cfg")
        assert config.provider.type == "dryrun"
        assert resolved.exists()
