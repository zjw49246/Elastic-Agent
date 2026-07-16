"""Minimal Manager: register the e2e worker node + serve, to validate the
systemd-from-src runtime (does it connect, and survive SSH disconnect?)."""
from __future__ import annotations
import sys
sys.path.insert(0, "/home/ubuntu/Projects/elastic-agent/.claude-manager/worktrees/task-ccm-sync/src")
import asyncio, os
os.environ["ELASTIC_AGENT_EXTERNAL_API_KEYS"] = "elastic-demo-2026"; os.environ.pop("PORT", None)
import uvicorn
from elastic_agent.api.app import create_app
from elastic_agent.api.auth import reset_api_keys
from elastic_agent.core.config import (AWSProviderConfig, ElasticAgentConfig, ProviderConfig,
    RegistryConfig, ServerConfig, TaskRegistryConfig, WorkerConfig)
from elastic_agent.core.providers.aws import AWSProvider
from elastic_agent.core.registry import NodeRecord, NodeStatus
from elastic_agent.manager.manager import ElasticAgentManager

BASE = os.path.expanduser("~/.elastic-agent-e2e"); os.makedirs(BASE, exist_ok=True)
aws = AWSProviderConfig(region="ap-northeast-1")
cfg = ElasticAgentConfig(server=ServerConfig(host="0.0.0.0", port=8080),
    provider=ProviderConfig(type="aws", aws=aws), worker=WorkerConfig(ssh_user="ubuntu"),
    registry=RegistryConfig(path=f"{BASE}/registry.json"), task_registry=TaskRegistryConfig(path=f"{BASE}/tr.json"))
reset_api_keys()
manager = ElasticAgentManager(cfg, AWSProvider(aws)); app = create_app(manager)

async def driver():
    await manager.registry.add(NodeRecord(node_id="e2e-worker", instance_id="i-07ed8d8a0f36629dd",
        platform="aws", status=NodeStatus.READY, public_ip="3.113.0.121", private_ip="172.31.43.217",
        auth_token="e2e-token-abc123"))
    print("VS: node registered; serving", flush=True)
    while True:
        await asyncio.sleep(10)
        print(f"VS: e2e-worker connected={manager.connection_manager.is_connected('e2e-worker')}", flush=True)

async def main():
    server = uvicorn.Server(uvicorn.Config(app, host="0.0.0.0", port=8080, log_level="warning"))
    await asyncio.gather(server.serve(), driver())

if __name__ == "__main__":
    asyncio.run(main())
