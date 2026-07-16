"""Launch a live Elastic-Agent Manager (real AWS provider, this VPC).

Serves the Fleet Dashboard (/) and Batch Console (/batch) on :8080, wired so
scale-out lands workers in the same VPC/subnet as this box (Manager reachable
over private IP). API key gates /api/*; the UI takes ?api_key=.
"""
from __future__ import annotations

import os
import sys

# Guarantee we load THIS worktree's elastic_agent (not an installed copy).
sys.path.insert(
    0, "/home/ubuntu/Projects/elastic-agent/.claude-manager/worktrees/task-ccm-sync/src"
)

# Manager URL workers connect back to (same-VPC private IP + open port).
os.environ["ELASTIC_AGENT_MANAGER_URL"] = "ws://172.31.38.111:8080/ws/runtime"
os.environ["ELASTIC_AGENT_EXTERNAL_API_KEYS"] = "elastic-demo-2026"
os.environ["ELASTIC_AGENT_RESULTS_S3_BUCKET"] = "elastic-agent-results-297645381734"
os.environ["ELASTIC_AGENT_RESULTS_S3_INTERVAL"] = "60"
# Deliver THIS worktree's framework to workers (rsync from src + systemd), so
# UI-submitted jobs run this branch's code (incl. ACCOUNT_LOGIN handler), not PyPI.
os.environ["ELASTIC_AGENT_FRAMEWORK_SRC"] = (
    "/home/ubuntu/Projects/elastic-agent/.claude-manager/worktrees/task-ccm-sync/src"
)
os.environ.pop("PORT", None)

# Git token for private-repo delivery (manager_rsync). Read at runtime from the
# box's authenticated gh — stays in the Manager process env, never hardcoded and
# never sent to workers (rsync excludes .git).
try:
    import subprocess as _sp
    _tok = _sp.run(["gh", "auth", "token"], capture_output=True, text=True, timeout=10).stdout.strip()
    if _tok:
        os.environ["ELASTIC_AGENT_GIT_TOKEN"] = _tok
except Exception:
    pass

import uvicorn

from elastic_agent.api.app import create_app
from elastic_agent.api.auth import reset_api_keys
from elastic_agent.core.config import (
    AWSProviderConfig,
    ElasticAgentConfig,
    ProviderConfig,
    RegistryConfig,
    ServerConfig,
    TaskRegistryConfig,
    WorkerConfig,
)
from elastic_agent.core.providers.aws import AWSProvider
from elastic_agent.manager.manager import ElasticAgentManager

BASE = os.path.expanduser("~/.elastic-agent-demo")
os.makedirs(BASE, exist_ok=True)

aws = AWSProviderConfig(
    region="ap-northeast-1",
    ami_id="ami-0478d64d580a0c8e5",              # Ubuntu 26.04
    default_instance_type="t3.large",
    security_group_ids=["sg-056408de7cf971e02"],
    subnet_id="subnet-0c1db80817d054277",
    key_pair_name="interview-key",
    ssh_key_path="/home/ubuntu/.ssh/interview-key.pem",
)
config = ElasticAgentConfig(
    server=ServerConfig(host="0.0.0.0", port=8080),
    provider=ProviderConfig(type="aws", aws=aws),
    worker=WorkerConfig(ssh_user="ubuntu"),        # Ubuntu AMI login user
    registry=RegistryConfig(path=f"{BASE}/registry.json"),
    task_registry=TaskRegistryConfig(path=f"{BASE}/task_registry.json"),
)

reset_api_keys()
manager = ElasticAgentManager(config, AWSProvider(aws))
app = create_app(manager)

if __name__ == "__main__":
    import asyncio

    async def _serve():
        server = uvicorn.Server(uvicorn.Config(app, host="0.0.0.0", port=8080, log_level="info"))
        await server.serve()

    asyncio.run(_serve())
