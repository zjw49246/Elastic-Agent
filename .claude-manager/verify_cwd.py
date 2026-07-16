"""Verify the cwd contract on a real worker via the framework's own build_execute.

Builds ExecuteMessages from a JobSpec (target_dir = the already-present benchmark
repo) using resolved_cwd, dispatches over the Manager WS, and checks:
  1. cwd proof: the command runs from the repo root (pwd == target_dir, pyproject found).
  2. real run: a benchmark task runs from the repo root and writes results/ there.
"""
from __future__ import annotations
import sys
sys.path.insert(0, "/home/ubuntu/Projects/elastic-agent/.claude-manager/worktrees/task-ccm-sync/src")
import asyncio, os
os.environ["ELASTIC_AGENT_EXTERNAL_API_KEYS"] = "elastic-demo-2026"
os.environ.pop("PORT", None)

import uvicorn
from elastic_agent.api.app import create_app
from elastic_agent.api.auth import reset_api_keys
from elastic_agent.core.config import (AWSProviderConfig, ElasticAgentConfig, ProviderConfig,
    RegistryConfig, ServerConfig, TaskRegistryConfig, WorkerConfig)
from elastic_agent.core.job_spec import JobSpec, RunSpec, SetupSpec, AccountSpec
from elastic_agent.harness.generic import build_execute
from elastic_agent.core.providers.aws import AWSProvider
from elastic_agent.core.registry import NodeRecord, NodeStatus
from elastic_agent.manager.manager import ElasticAgentManager

WORKER = "172.31.43.217"; NODE_ID = "e2e-worker"; TOKEN = "e2e-token-abc123"
REPO_DIR = "/home/ubuntu/ai4sci"; CFG = "/home/ubuntu/.claude-e2e-login"
BASE = os.path.expanduser("~/.elastic-agent-e2e"); os.makedirs(BASE, exist_ok=True)

aws = AWSProviderConfig(region="ap-northeast-1")
config = ElasticAgentConfig(
    server=ServerConfig(host="0.0.0.0", port=8080), provider=ProviderConfig(type="aws", aws=aws),
    worker=WorkerConfig(ssh_user="ubuntu"), registry=RegistryConfig(path=f"{BASE}/registry.json"),
    task_registry=TaskRegistryConfig(path=f"{BASE}/task_registry.json"))
reset_api_keys()
manager = ElasticAgentManager(config, AWSProvider(aws))
app = create_app(manager)

logs: list[dict] = []; exits: dict[str, int] = {}; ev: dict[str, asyncio.Event] = {}
async def _on_log(e, w, d): logs.append(d)
async def _on_exit(e, w, d):
    exits[d.get("task_id", "")] = d.get("exit_code"); ev.setdefault(d.get("task_id", ""), asyncio.Event()).set()

def _ctx(spec):
    c = spec.worker_contexts()[0]; return c

async def dispatch(spec, tid):
    ex = build_execute(spec, _ctx(spec))
    print(f"VERIFY: dispatch {tid} cwd={ex['cwd']} env_has_config={'CLAUDE_CONFIG_DIR' in ex['env']}", flush=True)
    ev[tid] = asyncio.Event()
    await manager.connection_manager.execute(worker_id=NODE_ID, task_id=tid,
        command=ex["command"], cwd=ex["cwd"], env=ex["env"], timeout=ex["timeout"] or 400)
    return ex

async def driver():
    await manager.registry.add(NodeRecord(node_id=NODE_ID, instance_id="i-07ed8d8a0f36629dd",
        platform="aws", status=NodeStatus.READY, public_ip="3.113.0.121", private_ip=WORKER, auth_token=TOKEN))
    manager.event_bus.subscribe("LOG", _on_log); manager.event_bus.subscribe("PROCESS_EXIT", _on_exit)
    await asyncio.sleep(2)
    print("VERIFY: waiting for worker...", flush=True)
    for _ in range(180):
        if manager.connection_manager.is_connected(NODE_ID): break
        await asyncio.sleep(1)
    print(f"VERIFY: connected={manager.connection_manager.is_connected(NODE_ID)}", flush=True)

    # --- Phase 1: cwd proof (target_dir=repo, run.cwd=".") ---
    spec1 = JobSpec(name="verify-cwd", setup=SetupSpec(target_dir=REPO_DIR),
        run=RunSpec(command="echo CWD=$(pwd); ls pyproject.toml >/dev/null 2>&1 && echo PYPROJECT_FOUND || echo NO_PYPROJECT"),
        account=AccountSpec(mode="none"))
    await dispatch(spec1, "verify-1")
    try: await asyncio.wait_for(ev["verify-1"].wait(), timeout=40)
    except asyncio.TimeoutError: print("VERIFY: phase1 timeout", flush=True)
    out1 = [l["data"] for l in logs if l.get("task_id") == "verify-1" and l.get("stream") == "stdout"]
    print(f"VERIFY: phase1 exit={exits.get('verify-1')} stdout={out1}", flush=True)

    # --- Phase 2: real benchmark run from repo root, output under results/ ---
    spec2 = JobSpec(name="verify-bench", setup=SetupSpec(target_dir=REPO_DIR),
        run=RunSpec(command=('$HOME/.local/bin/uv run ai4sci-bench run --agent claude_code_cli '
            '--agent-config \'{"model":"claude-opus-4-8","effort":"medium","timeout_seconds":300}\' '
            '--tasks math.homotopy_poly_roots --prompt-levels b1 --instances-per-task 1 --seed 200 '
            '--parallel 1 --sandbox task --tool-mode restricted --output-dir "results/verify_$(hostname -s)"')),
        account=AccountSpec(mode="none", config_dir=CFG))
    await dispatch(spec2, "verify-2")
    try: await asyncio.wait_for(ev["verify-2"].wait(), timeout=380)
    except asyncio.TimeoutError: print("VERIFY: phase2 timeout", flush=True)
    out2 = [l["data"] for l in logs if l.get("task_id") == "verify-2" and l.get("stream") == "stdout"]
    score = [l for l in out2 if "score" in l.lower() or "final_score" in l.lower()]
    print(f"VERIFY: phase2 exit={exits.get('verify-2')} score_lines={score[-3:]}", flush=True)
    print("VERIFY: DONE", flush=True)

async def main():
    server = uvicorn.Server(uvicorn.Config(app, host="0.0.0.0", port=8080, log_level="warning"))
    await asyncio.gather(server.serve(), driver())

if __name__ == "__main__":
    asyncio.run(main())
