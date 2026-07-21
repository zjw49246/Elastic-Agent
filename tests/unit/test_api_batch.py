"""Tests for the accounts + jobs REST API (frontend backend)."""

from __future__ import annotations

import json
import stat
from contextlib import asynccontextmanager
from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient

from elastic_agent.api.app import create_app
from elastic_agent.api.auth import reset_api_keys
from elastic_agent.core.account_binding import AccountBinding, BindingState
from elastic_agent.core.config import ElasticAgentConfig
from elastic_agent.core.providers.base import CloudProvider, Instance, InstanceConfig, InstanceState
from elastic_agent.manager.manager import ElasticAgentManager

API_KEY = "test-api-key-batch"


class InMemoryProvider(CloudProvider):
    def __init__(self):
        self._n = 0

    @property
    def platform(self) -> str:
        return "dryrun"

    async def create_instance(self, config: InstanceConfig) -> Instance:
        self._n += 1
        return Instance(
            instance_id=f"dryrun:i-{self._n}", platform="dryrun", native_id=f"i-{self._n}",
            state=InstanceState.PENDING, public_ip=f"1.2.3.{self._n}", private_ip=f"10.0.0.{self._n}",
            instance_type=config.instance_type, region="test", zone="test-a", tags=config.tags,
        )

    async def terminate_instance(self, instance_id: str) -> None: ...
    async def start_instance(self, instance_id: str) -> None: ...
    async def stop_instance(self, instance_id: str) -> None: ...
    async def reboot_instance(self, instance_id: str) -> None: ...
    async def list_instances(self, filters=None): return []
    async def get_instance(self, instance_id): return None
    async def wait_until_running(self, instance_id, timeout=300): return None


class FakeBatch:
    """Stand-in orchestrator so route logic is tested without live workers."""

    def __init__(self, persist_spec_hook=None):
        self._jobs = {}
        self.started: list[str] = []
        self.before_start = None
        self.persist_spec_hook = persist_spec_hook

    def prepare(self, spec):
        from elastic_agent.core.batch_orchestrator import BatchJob
        from elastic_agent.harness.generic import resolve_harness

        return BatchJob(
            job_id=f"job-{len(self._jobs)}",
            spec=spec,
            harness=resolve_harness(spec),
        )

    async def submit_prepared(self, job):
        from elastic_agent.core.batch_orchestrator import (
            JobSpecPersistenceError,
            WorkerPhase,
            WorkerRun,
        )
        from elastic_agent.core.job_spec import WorkerContext

        if self.persist_spec_hook is not None:
            try:
                await self.persist_spec_hook(job.job_id, job.spec)
            except Exception as exc:  # noqa: BLE001
                raise JobSpecPersistenceError(
                    f"failed to persist JobSpec before launch: {exc}"
                ) from exc
        if self.before_start is not None:
            self.before_start(job)
        self.started.append(job.job_id)
        for i in range(max(1, job.spec.fanout.workers)):
            job.runs[f"w{i}"] = WorkerRun(
                worker_id=f"w{i}",
                ctx=WorkerContext(shard_index=i),
                phase=WorkerPhase.RUNNING,
            )
        self._jobs[job.job_id] = job
        return job

    async def launch(self, spec):
        return await self.submit_prepared(self.prepare(spec))

    async def submit(self, spec):
        # Route uses submit() (background bring-up); for tests it mirrors launch().
        return await self.launch(spec)

    async def shutdown(self):
        return None

    def list_jobs(self):
        return list(self._jobs.values())

    def get_job(self, jid):
        return self._jobs.get(jid)


class FakeBindingManager:
    """Route-level stand-in for the durable EIP binding service."""

    def __init__(self):
        self.bindings: dict[str, AccountBinding] = {}
        self.ensure_calls: list[tuple[str, str, str]] = []
        self.decommissioned: list[str] = []
        self.active_accounts: set[str] = set()

    async def list_bindings(self):
        return list(self.bindings.values())

    @asynccontextmanager
    async def account_transaction(self, account_id):
        yield

    async def get_binding(self, account_id):
        return self.bindings.get(account_id)

    async def list_leases(self, *, account_id=None, active_only=False):
        if account_id in self.active_accounts:
            return [{"account_id": account_id, "state": "attached"}]
        return []

    async def ensure_binding(self, account_id, *, email="", region=""):
        self.ensure_calls.append((account_id, email, region))
        if account_id not in self.bindings:
            n = len(self.bindings) + 1
            self.bindings[account_id] = AccountBinding(
                account_id=account_id,
                email=email,
                eip_allocation_id=f"eipalloc-{n}",
                eip_ip=f"198.51.100.{n}",
                region=region or "test-1",
                state=BindingState.READY,
            )
        return self.bindings[account_id]

    async def decommission(self, account_id, *, confirm_absent=False):
        assert confirm_absent is True
        if account_id in self.active_accounts:
            raise RuntimeError(f"account {account_id} has an active lease")
        removed = self.bindings.pop(account_id, None)
        if removed is None:
            return False
        self.decommissioned.append(account_id)
        return True


@pytest.fixture(autouse=True)
def setup_api_keys(monkeypatch):
    monkeypatch.setenv("ELASTIC_AGENT_EXTERNAL_API_KEYS", API_KEY)
    reset_api_keys()
    yield
    reset_api_keys()


@pytest.fixture
def manager(tmp_path):
    cfg = ElasticAgentConfig()
    cfg.registry.path = str(tmp_path / "registry.json")
    cfg.provider.type = "aws"
    cfg.provider.aws.region = "us-west-2"
    mgr = ElasticAgentManager(cfg, InMemoryProvider())
    binding_manager = FakeBindingManager()
    mgr.binding_manager = binding_manager
    return mgr


@pytest.fixture
async def client(manager):
    app = create_app(manager)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://testserver",
        headers={"Authorization": f"Bearer {API_KEY}"},
    ) as ac:
        await manager.start()
        manager._batch = FakeBatch(
            manager._persist_batch_job_spec
        )  # inject fake orchestrator with the production persistence boundary
        yield ac
        await manager.stop()


class TestAccountsAPI:
    @pytest.mark.asyncio
    async def test_add_list_remove(self, client):
        assert (await client.get("/api/accounts")).json()["total"] == 0

        r = await client.post("/api/accounts", json={
            "id": "acc-1", "email": "a@x.com", "email_token": "tok", "group": "prod",
        })
        assert r.status_code == 201
        assert r.json()["email"] == "a@x.com"
        assert "email_token" not in r.json()
        assert r.json()["has_email_token"] is True

        lst = (await client.get("/api/accounts")).json()
        assert lst["total"] == 1
        assert lst["accounts"][0]["id"] == "acc-1"
        assert "email_token" not in lst["accounts"][0]

        r = await client.delete("/api/accounts/acc-1")
        assert r.status_code == 200
        assert (await client.get("/api/accounts")).json()["total"] == 0

    @pytest.mark.asyncio
    async def test_codex_password_is_write_only_and_persisted_mode_0600(
        self, client, manager
    ):
        created = await client.post("/api/accounts", json={
            "id": "codex-1",
            "agent_type": "codex",
            "email": "codex@example.com",
            "password": "first-password",
            "email_token": "mail-secret",
        })

        assert created.status_code == 201
        assert created.json()["agent_type"] == "codex"
        assert created.json()["has_password"] is True
        assert created.json()["has_email_token"] is True
        assert "password" not in created.json()
        assert "email_token" not in created.json()

        listed = (await client.get("/api/accounts")).json()["accounts"][0]
        assert listed["has_password"] is True
        assert "password" not in listed
        path = Path(manager.config.registry.path).with_name("accounts.json")
        assert stat.S_IMODE(path.stat().st_mode) == 0o600
        payload = json.loads(path.read_text(encoding="utf-8"))
        assert payload["accounts"][0]["password"] == "first-password"

    @pytest.mark.asyncio
    async def test_empty_secrets_preserve_existing_and_nonempty_values_rotate(
        self, client, manager
    ):
        await client.post("/api/accounts", json={
            "id": "codex-1",
            "agent_type": "codex",
            "email": "codex@example.com",
            "password": "first-password",
            "email_token": "first-mail-token",
        })

        preserved = await client.post("/api/accounts", json={
            "id": "codex-1",
            "agent_type": "codex",
            "email": "codex@example.com",
            "password": "",
            "email_token": "",
            "group": "new-group",
        })
        assert preserved.status_code == 201
        stored = await manager.account_store.get("codex-1")
        assert stored.password == "first-password"
        assert stored.email_token == "first-mail-token"
        assert stored.group == "new-group"

        rotated = await client.post("/api/accounts", json={
            "id": "codex-1",
            "agent_type": "codex",
            "email": "codex@example.com",
            "password": "second-password",
            "email_token": "second-mail-token",
        })
        assert rotated.status_code == 201
        stored = await manager.account_store.get("codex-1")
        assert stored.password == "second-password"
        assert stored.email_token == "second-mail-token"

        cleared = await client.post("/api/accounts", json={
            "id": "codex-1",
            "agent_type": "codex",
            "email": "codex@example.com",
            "password": "",
            "email_token": "",
            "clear_email_token": True,
        })
        assert cleared.status_code == 201
        stored = await manager.account_store.get("codex-1")
        assert stored.password == "second-password"
        assert stored.email_token == ""
        assert cleared.json()["has_email_token"] is False

    @pytest.mark.asyncio
    async def test_invalid_codex_account_error_never_echoes_mail_token(self, client):
        secret = "mail-token-that-must-not-echo"

        response = await client.post("/api/accounts", json={
            "id": "codex-no-password",
            "agent_type": "codex",
            "email": "codex@example.com",
            "email_token": secret,
        })

        assert response.status_code == 409
        assert secret not in response.text
        assert response.json()["detail"] == (
            "Codex accounts require an OpenAI password"
        )

    @pytest.mark.asyncio
    async def test_same_email_is_unique_per_agent_type(self, client):
        claude = await client.post("/api/accounts", json={
            "id": "claude-a", "email": "User@example.com", "agent_type": "claude",
        })
        codex = await client.post("/api/accounts", json={
            "id": "codex-a", "email": "user@EXAMPLE.com", "agent_type": "codex",
            "password": "codex-password-a",
        })
        duplicate = await client.post("/api/accounts", json={
            "id": "codex-b", "email": "USER@example.com", "agent_type": "codex",
            "password": "codex-password-b",
        })

        assert claude.status_code == codex.status_code == 201
        assert duplicate.status_code == 409

    @pytest.mark.asyncio
    async def test_active_job_claim_blocks_identity_edit_and_delete(
        self, client, manager,
    ):
        created = await client.post(
            "/api/accounts",
            json={"id": "claimed", "email": "old@x.com"},
        )
        assert created.status_code == 201
        claim = await manager.account_allocator.reserve(
            "job-live:0", "standard", account_id="claimed"
        )
        assert claim is not None

        edited = await client.post(
            "/api/accounts",
            json={"id": "claimed", "email": "new@x.com"},
        )
        deleted = await client.delete("/api/accounts/claimed")

        assert edited.status_code == 409
        assert deleted.status_code == 409
        assert "actively claimed" in edited.json()["detail"]
        stored = await manager.account_store.get("claimed")
        assert stored.email == "old@x.com"

        await manager.account_allocator.release_claim(claim.claim_id)
        edited = await client.post(
            "/api/accounts",
            json={"id": "claimed", "email": "new@x.com"},
        )
        assert edited.status_code == 201

    @pytest.mark.asyncio
    async def test_active_job_claim_blocks_manual_binding_ensure(
        self, client, manager,
    ):
        created = await client.post(
            "/api/accounts",
            json={"id": "claimed", "email": "claimed@x.com"},
        )
        assert created.status_code == 201
        claim = await manager.account_allocator.reserve(
            "job-live:0", "standard", account_id="claimed"
        )
        assert claim is not None

        blocked = await client.put("/api/accounts/claimed/binding")

        assert blocked.status_code == 409
        assert "actively claimed" in blocked.json()["detail"]
        assert manager.binding_manager.ensure_calls == []

        await manager.account_allocator.release_claim(claim.claim_id)
        ensured = await client.put("/api/accounts/claimed/binding")
        assert ensured.status_code == 200
        assert manager.binding_manager.ensure_calls == [
            ("claimed", "claimed@x.com", "us-west-2"),
        ]

    @pytest.mark.asyncio
    async def test_remove_missing_404(self, client):
        assert (await client.delete("/api/accounts/nope")).status_code == 404

    @pytest.mark.asyncio
    async def test_upsert_replaces(self, client):
        await client.post("/api/accounts", json={"id": "a", "email": "one@x.com"})
        await client.post("/api/accounts", json={"id": "a", "email": "two@x.com"})
        lst = (await client.get("/api/accounts")).json()
        assert lst["total"] == 1
        assert lst["accounts"][0]["email"] == "two@x.com"

    @pytest.mark.asyncio
    async def test_duplicate_email_casefold_is_rejected(self, client):
        assert (await client.post(
            "/api/accounts", json={"id": "a", "email": "User@x.com"}
        )).status_code == 201
        duplicate = await client.post(
            "/api/accounts", json={"id": "b", "email": "user@X.com"}
        )
        assert duplicate.status_code == 409

    @pytest.mark.asyncio
    async def test_bound_identity_email_is_immutable_but_token_can_rotate(
        self, client
    ):
        await client.post(
            "/api/accounts",
            json={"id": "a", "email": "one@x.com", "email_token": "old"},
        )
        await client.put("/api/accounts/a/binding")

        changed = await client.post(
            "/api/accounts", json={"id": "a", "email": "two@x.com"}
        )
        assert changed.status_code == 409

        changed_agent = await client.post(
            "/api/accounts",
            json={
                "id": "a",
                "email": "one@x.com",
                "agent_type": "codex",
                "password": "openai-password",
            },
        )
        assert changed_agent.status_code == 409

        rotated = await client.post(
            "/api/accounts",
            json={"id": "a", "email": "ONE@x.com", "email_token": "new"},
        )
        assert rotated.status_code == 201
        assert "email_token" not in rotated.json()

    @pytest.mark.asyncio
    async def test_ensure_get_and_list_eip_binding_is_idempotent(self, client, manager):
        await client.post("/api/accounts", json={"id": "a", "email": "one@x.com"})

        first = await client.put(
            "/api/accounts/a/binding", json={"region": "us-west-2"},
        )
        second = await client.put(
            "/api/accounts/a/binding", json={"region": "us-west-2"},
        )
        assert first.status_code == second.status_code == 200
        assert first.json()["eip_allocation_id"] == "eipalloc-1"
        assert second.json()["eip_ip"] == "198.51.100.1"
        assert manager.binding_manager.ensure_calls == [
            ("a", "one@x.com", "us-west-2"),
            ("a", "one@x.com", "us-west-2"),
        ]

        got = await client.get("/api/accounts/a/binding")
        assert got.status_code == 200
        assert got.json()["state"] == BindingState.READY
        listed = (await client.get("/api/accounts/bindings")).json()
        assert listed["total"] == 1
        assert listed["bindings"][0]["account_id"] == "a"

    @pytest.mark.asyncio
    async def test_ensure_requires_known_enabled_account(self, client):
        assert (await client.put("/api/accounts/nope/binding")).status_code == 404
        await client.post(
            "/api/accounts",
            json={"id": "off", "email": "off@x.com", "enabled": False},
        )
        assert (await client.put("/api/accounts/off/binding")).status_code == 409

    @pytest.mark.asyncio
    async def test_ensure_uses_configured_region_and_rejects_other_region(
        self, client, manager,
    ):
        await client.post("/api/accounts", json={"id": "a", "email": "one@x.com"})
        ensured = await client.put("/api/accounts/a/binding")
        assert ensured.status_code == 200
        assert ensured.json()["region"] == "us-west-2"
        assert manager.binding_manager.ensure_calls == [
            ("a", "one@x.com", "us-west-2"),
        ]

        mismatch = await client.put(
            "/api/accounts/a/binding", json={"region": "us-east-1"},
        )
        assert mismatch.status_code == 409
        assert manager.binding_manager.ensure_calls == [
            ("a", "one@x.com", "us-west-2"),
        ]

    @pytest.mark.asyncio
    async def test_ensure_rejects_non_aws_provider(self, client, manager):
        await client.post("/api/accounts", json={"id": "a", "email": "one@x.com"})
        manager.config.provider.type = "aliyun"
        response = await client.put("/api/accounts/a/binding")
        assert response.status_code == 501
        assert manager.binding_manager.ensure_calls == []

    @pytest.mark.asyncio
    async def test_binding_get_missing_404(self, client):
        assert (await client.get("/api/accounts/nope/binding")).status_code == 404

    @pytest.mark.asyncio
    async def test_delete_bound_identity_requires_explicit_decommission(
        self, client, manager,
    ):
        await client.post("/api/accounts", json={"id": "a", "email": "one@x.com"})
        await client.put("/api/accounts/a/binding")

        blocked = await client.delete("/api/accounts/a")
        assert blocked.status_code == 409
        assert "decommission" in blocked.json()["detail"]
        assert await manager.account_store.get("a") is not None
        assert await manager.binding_manager.get_binding("a") is not None

    @pytest.mark.asyncio
    async def test_decommission_requires_double_confirmation(self, client, manager):
        await client.post("/api/accounts", json={"id": "a", "email": "one@x.com"})
        await client.put("/api/accounts/a/binding")

        missing = await client.post("/api/accounts/a/binding/decommission", json={})
        refused = await client.post(
            "/api/accounts/a/binding/decommission",
            json={"release_eip": False, "confirm_account_id": "a"},
        )
        mismatch = await client.post(
            "/api/accounts/a/binding/decommission",
            json={"release_eip": True, "confirm_account_id": "someone-else"},
        )
        assert missing.status_code == 422
        assert refused.status_code == 422
        assert mismatch.status_code == 400
        assert await manager.binding_manager.get_binding("a") is not None

    @pytest.mark.asyncio
    async def test_decommission_rejects_active_lease_then_releases_eip(
        self, client, manager,
    ):
        await client.post("/api/accounts", json={"id": "a", "email": "one@x.com"})
        await client.put("/api/accounts/a/binding")
        manager.binding_manager.active_accounts.add("a")
        body = {"release_eip": True, "confirm_account_id": "a"}

        active = await client.post(
            "/api/accounts/a/binding/decommission", json=body,
        )
        assert active.status_code == 409
        assert "active lease" in active.json()["detail"]

        manager.binding_manager.active_accounts.clear()
        released = await client.post(
            "/api/accounts/a/binding/decommission", json=body,
        )
        assert released.status_code == 200
        assert released.json() == {
            "account_id": "a",
            "status": "decommissioned",
            "eip_released": True,
        }
        assert manager.binding_manager.decommissioned == ["a"]
        assert (await client.get("/api/accounts/a/binding")).status_code == 404
        # Identity CRUD stays separate and becomes available after explicit
        # infrastructure decommissioning.
        assert (await client.delete("/api/accounts/a")).status_code == 200

    @pytest.mark.asyncio
    async def test_decommission_missing_binding_404(self, client):
        response = await client.post(
            "/api/accounts/nope/binding/decommission",
            json={"release_eip": True, "confirm_account_id": "nope"},
        )
        assert response.status_code == 404


class TestJobsAPI:
    _SPEC = {
        "name": "ai4sci",
        "run": {"command": "uv run bench --shard {{shard_index}}"},
        "fanout": {"workers": 3},
        "account": {"mode": "none"},
    }

    @pytest.mark.asyncio
    async def test_submit_returns_detail(self, client):
        r = await client.post("/api/jobs", json=self._SPEC)
        assert r.status_code == 201
        body = r.json()
        assert body["workers"] == 3
        assert len(body["workers_detail"]) == 3
        assert all(w["phase"] == "running" for w in body["workers_detail"])

    @pytest.mark.asyncio
    async def test_submit_fsyncs_spec_before_starting_job(self, client, manager, monkeypatch):
        from elastic_agent.core import job_spec_store

        fsync_kinds = []
        real_fsync = job_spec_store.os.fsync

        def recording_fsync(fd):
            fsync_kinds.append(
                "dir" if stat.S_ISDIR(job_spec_store.os.fstat(fd).st_mode) else "file"
            )
            return real_fsync(fd)

        monkeypatch.setattr(job_spec_store.os, "fsync", recording_fsync)

        def assert_persisted(job):
            path = Path(manager.config.registry.path).with_name("specs") / f"{job.job_id}.json"
            data = json.loads(path.read_text(encoding="utf-8"))
            assert data["job_id"] == job.job_id
            assert data["spec"]["name"] == "ai4sci"
            assert stat.S_IMODE(path.stat().st_mode) == 0o600

        manager.batch.before_start = assert_persisted
        response = await client.post("/api/jobs", json=self._SPEC)

        assert response.status_code == 201
        assert manager.batch.started == [response.json()["job_id"]]
        assert fsync_kinds == ["file", "dir"]
        assert list(Path(manager.config.registry.path).with_name("specs").glob("*.tmp")) == []

    @pytest.mark.asyncio
    async def test_submit_persistence_failure_never_starts_job(
        self, client, manager, monkeypatch,
    ):
        from elastic_agent.core import job_spec_store

        def fail_replace(_src, _dest):
            raise OSError("disk is read-only")

        monkeypatch.setattr(job_spec_store.os, "replace", fail_replace)
        response = await client.post("/api/jobs", json=self._SPEC)

        assert response.status_code == 500
        assert "persist" in response.json()["detail"]
        assert manager.batch.started == []
        assert manager.batch.list_jobs() == []
        assert manager.provider._n == 0
        specs = Path(manager.config.registry.path).with_name("specs")
        assert list(specs.iterdir()) == []

    @pytest.mark.asyncio
    async def test_list_and_get(self, client):
        submitted = (await client.post("/api/jobs", json=self._SPEC)).json()
        jid = submitted["job_id"]

        lst = (await client.get("/api/jobs")).json()
        assert lst["total"] == 1

        detail = (await client.get(f"/api/jobs/{jid}")).json()
        assert detail["job_id"] == jid
        assert detail["spec"]["name"] == "ai4sci"

    @pytest.mark.asyncio
    async def test_list_includes_workers_detail(self, client):
        # The UI renders each job card straight from the list response, so every
        # item must carry workers_detail (no per-job detail fetch) — and drop the
        # heavy spec to keep the list lean.
        await client.post("/api/jobs", json=self._SPEC)
        item = (await client.get("/api/jobs")).json()["jobs"][0]
        assert len(item["workers_detail"]) == 3
        assert "spec" not in item

    @pytest.mark.asyncio
    async def test_get_missing_404(self, client):
        assert (await client.get("/api/jobs/nope")).status_code == 404

    @pytest.mark.asyncio
    async def test_invalid_spec_422(self, client):
        # missing run.command
        assert (await client.post("/api/jobs", json={"name": "x"})).status_code == 422


class TestHarnessUpload:
    _CODE = (
        "from elastic_agent.harness.base import Harness, BootstrapStep\n"
        "class MyHarness(Harness):\n"
        "    def get_bootstrap_steps(self):\n"
        "        return [BootstrapStep(name='c', command='echo c')]\n"
    )

    @pytest.mark.asyncio
    async def test_upload_returns_ref(self, client):
        r = await client.post("/api/jobs/harness", json={
            "filename": "myh.py", "content": self._CODE, "class_name": "MyHarness",
        })
        assert r.status_code == 201
        ref = r.json()["harness_ref"]
        assert ref.endswith(":MyHarness")

    @pytest.mark.asyncio
    async def test_bad_filename_rejected(self, client):
        r = await client.post("/api/jobs/harness", json={
            "filename": "../evil.py", "content": self._CODE, "class_name": "MyHarness",
        })
        assert r.status_code == 400

    @pytest.mark.asyncio
    async def test_not_a_harness_rejected(self, client):
        r = await client.post("/api/jobs/harness", json={
            "filename": "plain.py", "content": "class Nope:\n    pass\n", "class_name": "Nope",
        })
        assert r.status_code == 400


class TestJobResults:
    def _seed(self, manager, job_id="job-r"):
        import json as _json
        from pathlib import Path
        base = Path(manager.config.registry.path).with_name("collected") / job_id
        (base / "math.foo").mkdir(parents=True, exist_ok=True)
        (base / "run_metadata.json").write_text("{}")
        (base / "math.foo" / "res_b1.json").write_text(_json.dumps({
            "task_id": "math.foo", "prompt_level": "b1", "status": "completed", "final_score": 39.06,
        }))
        return job_id

    @pytest.mark.asyncio
    async def test_list_results_with_scores(self, client, manager):
        jid = self._seed(manager)
        r = await client.get(f"/api/jobs/{jid}/results")
        assert r.status_code == 200
        body = r.json()
        assert body["file_count"] == 2
        assert body["scores"] == [
            {"task_id": "math.foo", "prompt_level": "b1", "status": "completed", "final_score": 39.06}
        ]

    @pytest.mark.asyncio
    async def test_download_tarball(self, client, manager):
        jid = self._seed(manager, "job-dl")
        r = await client.get(f"/api/jobs/{jid}/results/download")
        assert r.status_code == 200
        assert r.headers["content-type"] == "application/gzip"
        assert "job-dl-results.tar.gz" in r.headers["content-disposition"]
        assert len(r.content) > 0

    @pytest.mark.asyncio
    async def test_missing_results_404(self, client):
        assert (await client.get("/api/jobs/nope/results")).status_code == 404

    @pytest.mark.asyncio
    async def test_list_all_results(self, client, manager):
        self._seed(manager, "job-a")
        self._seed(manager, "job-b")
        r = await client.get("/api/results")
        assert r.status_code == 200
        body = r.json()
        assert body["total"] == 2
        assert {j["job_id"] for j in body["jobs"]} == {"job-a", "job-b"}
        assert all(j["file_count"] == 2 for j in body["jobs"])


class TestBatchConsoleUI:
    @pytest.mark.asyncio
    async def test_batch_page_served(self, client):
        r = await client.get("/batch")
        assert r.status_code == 200
        assert "Batch Console" in r.text
        assert "Submit Job" in r.text
        assert "Accounts" in r.text
