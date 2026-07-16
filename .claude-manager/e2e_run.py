"""End-to-end: Manager ↔ real worker over WebSocket, driving EXECUTE + ACCOUNT_LOGIN.

Serves the Manager app on :8080, starts the branch's WorkerRuntime on the live
EC2 worker (172.31.43.217) pointing back over the tunnel-free private IP, then:
  1. EXECUTE a real `claude -p` turn (uses creds from the earlier login) → verify
     LOG streaming + PROCESS_EXIT exit 0.
  2. ACCOUNT_LOGIN (P3) → worker runs perform_login locally into a fresh dir →
     verify ACCOUNT_LOGIN_RESULT success.
Prints E2E: markers, then keeps serving so the worker stays connected + UI live.
"""
from __future__ import annotations

import sys
sys.path.insert(0, "/home/ubuntu/Projects/elastic-agent/.claude-manager/worktrees/task-ccm-sync/src")

import asyncio
import os

os.environ["ELASTIC_AGENT_EXTERNAL_API_KEYS"] = "elastic-demo-2026"
os.environ.pop("PORT", None)

import uvicorn

from elastic_agent.api.app import create_app
from elastic_agent.api.auth import reset_api_keys
from elastic_agent.core.config import (
    AWSProviderConfig, ElasticAgentConfig, ProviderConfig, RegistryConfig,
    ServerConfig, TaskRegistryConfig, WorkerConfig,
)
from elastic_agent.core.protocols.messages import AccountLoginMessage
from elastic_agent.core.providers.aws import AWSProvider
from elastic_agent.core.registry import NodeRecord, NodeStatus
from elastic_agent.manager.manager import ElasticAgentManager

WORKER_PRIV = "172.31.43.217"
MANAGER_PRIV = "172.31.38.111"
KEY = "/home/ubuntu/.ssh/interview-key.pem"
NODE_ID = "e2e-worker"
TOKEN = "e2e-token-abc123"
ACCT_EMAIL = "LairdBakerdud@musician.org"
ACCT_TOKEN = "12962883750f752785fea88536b71903"

BASE = os.path.expanduser("~/.elastic-agent-e2e")
os.makedirs(BASE, exist_ok=True)

aws = AWSProviderConfig(region="ap-northeast-1")
config = ElasticAgentConfig(
    server=ServerConfig(host="0.0.0.0", port=8080),
    provider=ProviderConfig(type="aws", aws=aws),
    worker=WorkerConfig(ssh_user="ubuntu"),
    registry=RegistryConfig(path=f"{BASE}/registry.json"),
    task_registry=TaskRegistryConfig(path=f"{BASE}/task_registry.json"),
)
reset_api_keys()
manager = ElasticAgentManager(config, AWSProvider(aws))
app = create_app(manager)

# Collectors for worker→manager events.
logs: list[dict] = []
exits: dict[str, int] = {}
login_results: dict[str, dict] = {}
exit_events: dict[str, asyncio.Event] = {}
login_events: dict[str, asyncio.Event] = {}


async def _on_log(evt, wid, data):
    logs.append(data)

async def _on_exit(evt, wid, data):
    tid = data.get("task_id", "")
    exits[tid] = data.get("exit_code")
    exit_events.setdefault(tid, asyncio.Event()).set()

async def _on_login(evt, wid, data):
    aid = data.get("account_id", "")
    login_results[aid] = data
    login_events.setdefault(aid, asyncio.Event()).set()


async def start_worker():
    """Start Xvfb + the branch WorkerRuntime on the EC2 worker over SSH."""
    remote = (
        'pkill -f "[r]untime_main" 2>/dev/null; mkdir -p /home/ubuntu/ea-logs; '
        'pgrep -f "[X]vfb :99" >/dev/null || { rm -f /tmp/.X99-lock; '
        'setsid Xvfb :99 -screen 0 1280x1024x24 </dev/null >/tmp/xvfb.log 2>&1 & }; '
        'sleep 1; '
        f'setsid env DISPLAY=:99 PYTHONPATH=/home/ubuntu/ea-src python3 -m elastic_agent.worker.runtime_main '
        f'--manager-url ws://{MANAGER_PRIV}:8080/ws/runtime --token {TOKEN} --worker-id {NODE_ID} '
        f'--log-dir /home/ubuntu/ea-logs </dev/null >/home/ubuntu/ea-runtime.log 2>&1 & '
        'sleep 1; echo WORKER_STARTED'
    )
    proc = await asyncio.create_subprocess_exec(
        "ssh", "-i", KEY, "-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null",
        f"ubuntu@{WORKER_PRIV}", remote,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    out, err = await proc.communicate()
    print("E2E: start_worker:", out.decode().strip(), err.decode()[-200:], flush=True)


async def wait_connected(timeout=60):
    for _ in range(timeout):
        if manager.connection_manager.is_connected(NODE_ID):
            return True
        await asyncio.sleep(1)
    return False


async def driver():
    # Register the worker node so its AUTH token validates.
    await manager.registry.add(NodeRecord(
        node_id=NODE_ID, instance_id="i-07ed8d8a0f36629dd", platform="aws",
        status=NodeStatus.READY, public_ip="3.113.0.121", private_ip=WORKER_PRIV,
        auth_token=TOKEN,
    ))
    manager.event_bus.subscribe("LOG", _on_log)
    manager.event_bus.subscribe("PROCESS_EXIT", _on_exit)
    manager.event_bus.subscribe("ACCOUNT_LOGIN_RESULT", _on_login)

    await asyncio.sleep(2)  # let uvicorn bind
    print("E2E: Manager serving; waiting for worker to connect (start it externally)...", flush=True)
    ok = await wait_connected(180)
    print(f"E2E: worker_connected={ok}", flush=True)
    if not ok:
        return

    # --- Phase 1: EXECUTE a real claude turn (existing creds) ---
    tid = "e2e-exec-1"
    exit_events[tid] = asyncio.Event()
    await manager.connection_manager.execute(
        worker_id=NODE_ID, task_id=tid,
        command=["claude", "-p", "reply with exactly: E2E_EXEC_OK", "--dangerously-skip-permissions"],
        cwd="/home/ubuntu", env={"CLAUDE_CONFIG_DIR": "/home/ubuntu/.claude-test"}, timeout=120,
    )
    try:
        await asyncio.wait_for(exit_events[tid].wait(), timeout=140)
    except asyncio.TimeoutError:
        print("E2E: EXECUTE timed out", flush=True)
    stdout_lines = [l["data"] for l in logs if l.get("task_id") == tid and l.get("stream") == "stdout"]
    print(f"E2E: EXECUTE exit={exits.get(tid)} | stdout_tail={stdout_lines[-3:]}", flush=True)

    # --- Phase 2: ACCOUNT_LOGIN over WS (P3) into a fresh dir ---
    login_events[  "acc-laird"] = asyncio.Event()
    await manager.connection_manager.send_command(NODE_ID, AccountLoginMessage(
        account_id="acc-laird", email=ACCT_EMAIL, email_token=ACCT_TOKEN,
        config_dir="/home/ubuntu/.claude-e2e-login", slot_index=0,
    ))
    try:
        await asyncio.wait_for(login_events["acc-laird"].wait(), timeout=220)
        r = login_results.get("acc-laird", {})
        print(f"E2E: ACCOUNT_LOGIN success={r.get('success')} error={r.get('error')}", flush=True)
    except asyncio.TimeoutError:
        print("E2E: ACCOUNT_LOGIN timed out", flush=True)

    print("E2E: DONE — keeping Manager serving (worker stays connected)", flush=True)


async def main():
    server = uvicorn.Server(uvicorn.Config(app, host="0.0.0.0", port=8080, log_level="warning"))
    await asyncio.gather(server.serve(), driver())


if __name__ == "__main__":
    asyncio.run(main())
