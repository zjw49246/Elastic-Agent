"""Continuous end-to-end: one manager.batch.launch(spec) that opens a fresh EC2,
provisions it fully (framework rsync+systemd, private-repo manager_rsync, login),
runs the benchmark, auto-collects results, uploads to S3. Prints FR: markers."""
from __future__ import annotations
import sys
SRC = "/home/ubuntu/Projects/elastic-agent/.claude-manager/worktrees/task-ccm-sync/src"
sys.path.insert(0, SRC)
import asyncio, os, subprocess, time
os.environ["ELASTIC_AGENT_EXTERNAL_API_KEYS"] = "elastic-demo-2026"
os.environ["ELASTIC_AGENT_MANAGER_URL"] = "ws://172.31.38.111:8080/ws/runtime"
os.environ["ELASTIC_AGENT_RESULTS_S3_BUCKET"] = "elastic-agent-results-297645381734"
os.environ["ELASTIC_AGENT_RESULTS_S3_INTERVAL"] = "60"
os.environ["ELASTIC_AGENT_FRAMEWORK_SRC"] = SRC
os.environ.pop("PORT", None)
try:
    os.environ["ELASTIC_AGENT_GIT_TOKEN"] = subprocess.run(
        ["gh", "auth", "token"], capture_output=True, text=True, timeout=10).stdout.strip()
except Exception:
    pass

import uvicorn
from elastic_agent.api.app import create_app
from elastic_agent.api.auth import reset_api_keys
from elastic_agent.core.config import (AWSProviderConfig, ElasticAgentConfig, ProviderConfig,
    RegistryConfig, ServerConfig, TaskRegistryConfig, WorkerConfig)
from elastic_agent.core.credential_pool import AccountDefinition
from elastic_agent.core.job_spec import JobSpec, RunSpec, SetupSpec, AccountSpec, FanoutSpec, CollectSpec
from elastic_agent.core.providers.aws import AWSProvider
from elastic_agent.manager.manager import ElasticAgentManager

BASE = os.path.expanduser("~/.elastic-agent-fullrun"); os.makedirs(BASE, exist_ok=True)
aws = AWSProviderConfig(region="ap-northeast-1", ami_id="ami-0478d64d580a0c8e5",
    default_instance_type="t3.large", security_group_ids=["sg-056408de7cf971e02"],
    subnet_id="subnet-0c1db80817d054277", key_pair_name="interview-key",
    ssh_key_path="/home/ubuntu/.ssh/interview-key.pem")
cfg = ElasticAgentConfig(server=ServerConfig(host="0.0.0.0", port=8080),
    provider=ProviderConfig(type="aws", aws=aws), worker=WorkerConfig(ssh_user="ubuntu"),
    registry=RegistryConfig(path=f"{BASE}/registry.json"), task_registry=TaskRegistryConfig(path=f"{BASE}/tr.json"))
reset_api_keys()
manager = ElasticAgentManager(cfg, AWSProvider(aws)); app = create_app(manager)

BENCH = ('$HOME/.local/bin/uv run ai4sci-bench run --agent claude_code_cli '
    '--agent-config \'{"model":"claude-opus-4-8","effort":"medium","timeout_seconds":300}\' '
    '--tasks math.homotopy_poly_roots --prompt-levels b1 --instances-per-task 1 --seed 300 '
    '--parallel 1 --sandbox task --tool-mode restricted --output-dir "results/auto_$(hostname -s)"')

spec = JobSpec(
    name="autorun",
    fanout=FanoutSpec(workers=1, name_prefix="autorun"),
    setup=SetupSpec(repo="https://github.com/ApexIntelligence-AI/Agent-AI4Sci-Bench.git",
        branch="main", deliver="manager_rsync", target_dir="/home/ubuntu/bench",
        commands=["curl -LsSf https://astral.sh/uv/install.sh | sh",
                  "$HOME/.local/bin/uv sync --python 3.13"]),
    run=RunSpec(command=BENCH, cwd=".", timeout=0),
    account=AccountSpec(mode="worker_local_login", group="standard", per_worker=1,
        config_dir="/home/ubuntu/.claude-autorun"),
    collect=CollectSpec(paths=["results"]),
)

async def driver():
    await manager.account_store.add(AccountDefinition(id="acc-laird",
        email="LairdBakerdud@musician.org", email_token="12962883750f752785fea88536b71903",
        group="standard"))
    print("FR: launching (opens a fresh EC2, full provision)…", flush=True)
    t0 = time.time()
    job = await manager.batch.launch(spec)
    wid = next(iter(job.runs))
    last = None
    while True:
        await asyncio.sleep(15)
        run = job.runs[wid]
        el = int(time.time() - t0)
        if run.phase.value != last:
            print(f"FR: [{el}s] phase={run.phase.value} acct={run.account_email or '-'} err={run.error or '-'}", flush=True)
            last = run.phase.value
        if run.phase.value in ("done", "failed"):
            break
    # results collected on DONE; read score + s3
    coll = os.path.join(manager.collected_root, job.job_id)
    scores = []
    if os.path.isdir(coll):
        import json, glob
        for f in glob.glob(coll + "/**/*.json", recursive=True):
            if "instances" in f: continue
            try:
                d = json.load(open(f))
                if "final_score" in d: scores.append((d.get("task_id"), d.get("final_score")))
            except Exception: pass
    print(f"FR: DONE phase={job.runs[wid].phase.value} job_id={job.job_id} scores={scores}", flush=True)
    print(f"FR: download=https://elastic-agent.claude-code-manager.com/api/jobs/{job.job_id}/results/download?api_key=elastic-demo-2026", flush=True)

async def main():
    server = uvicorn.Server(uvicorn.Config(app, host="0.0.0.0", port=8080, log_level="warning"))
    await asyncio.gather(server.serve(), driver())

if __name__ == "__main__":
    asyncio.run(main())
