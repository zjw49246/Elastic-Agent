"""Tests for the accounts + jobs REST API (frontend backend)."""

from __future__ import annotations

import pytest
from httpx import ASGITransport, AsyncClient

from elastic_agent.api.app import create_app
from elastic_agent.api.auth import reset_api_keys
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

    def __init__(self):
        self._jobs = {}

    async def launch(self, spec):
        from elastic_agent.core.batch_orchestrator import BatchJob, WorkerPhase, WorkerRun
        from elastic_agent.core.job_spec import WorkerContext
        from elastic_agent.harness.generic import resolve_harness
        job = BatchJob(job_id=f"job-{len(self._jobs)}", spec=spec, harness=resolve_harness(spec))
        for i in range(max(1, spec.fanout.workers)):
            job.runs[f"w{i}"] = WorkerRun(
                worker_id=f"w{i}", ctx=WorkerContext(shard_index=i), phase=WorkerPhase.RUNNING,
            )
        self._jobs[job.job_id] = job
        return job

    def list_jobs(self):
        return list(self._jobs.values())

    def get_job(self, jid):
        return self._jobs.get(jid)


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
    return ElasticAgentManager(cfg, InMemoryProvider())


@pytest.fixture
async def client(manager):
    app = create_app(manager)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://testserver",
        headers={"Authorization": f"Bearer {API_KEY}"},
    ) as ac:
        await manager.start()
        manager._batch = FakeBatch()  # inject fake orchestrator
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

        lst = (await client.get("/api/accounts")).json()
        assert lst["total"] == 1
        assert lst["accounts"][0]["id"] == "acc-1"

        r = await client.delete("/api/accounts/acc-1")
        assert r.status_code == 200
        assert (await client.get("/api/accounts")).json()["total"] == 0

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
    async def test_list_and_get(self, client):
        submitted = (await client.post("/api/jobs", json=self._SPEC)).json()
        jid = submitted["job_id"]

        lst = (await client.get("/api/jobs")).json()
        assert lst["total"] == 1

        detail = (await client.get(f"/api/jobs/{jid}")).json()
        assert detail["job_id"] == jid
        assert detail["spec"]["name"] == "ai4sci"

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
