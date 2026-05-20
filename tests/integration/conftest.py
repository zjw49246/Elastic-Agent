"""Shared fixtures for integration tests.

Provides a running FastAPI+uvicorn server with WebSocket support
so MockWorkers can connect via real WebSocket connections.
"""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path

import pytest
import uvicorn

from elastic_agent.api.app import create_app
from elastic_agent.api.auth import reset_api_keys
from elastic_agent.testing import (
    DryRunProvider,
    MockOSS,
    MockWorker,
    create_test_manager,
)

TEST_API_KEY = "integration-test-key-001"


def _free_port() -> int:
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class _UvicornServer:
    """Runs uvicorn in a background asyncio task."""

    def __init__(self, app, host: str, port: int):
        self._config = uvicorn.Config(app, host=host, port=port, log_level="warning")
        self._server = uvicorn.Server(self._config)
        self._task: asyncio.Task | None = None

    async def start(self):
        self._task = asyncio.create_task(self._server.serve())
        for _ in range(100):
            if self._server.started:
                break
            await asyncio.sleep(0.05)

    async def stop(self):
        self._server.should_exit = True
        if self._task:
            await self._task
            self._task = None


@pytest.fixture
def api_key_env(monkeypatch):
    monkeypatch.setenv("ELASTIC_AGENT_EXTERNAL_API_KEYS", TEST_API_KEY)
    reset_api_keys()
    yield TEST_API_KEY
    reset_api_keys()


@pytest.fixture
async def running_server(tmp_path, api_key_env):
    """Start a full Manager + FastAPI server. Returns dict with connection info."""
    port = _free_port()
    result = create_test_manager(tmp_dir=tmp_path)

    app = create_app(result.manager)
    server = _UvicornServer(app, "127.0.0.1", port)
    await server.start()

    yield {
        "manager": result.manager,
        "provider": result.provider,
        "oss": result.oss,
        "config": result.config,
        "tmp_dir": result.tmp_dir,
        "port": port,
        "base_url": f"http://127.0.0.1:{port}",
        "ws_url": f"ws://127.0.0.1:{port}/ws/runtime",
        "api_key": api_key_env,
    }

    await server.stop()


async def connect_mock_worker(
    ws_url: str,
    token: str,
    worker_id: str | None = None,
    **kwargs,
) -> MockWorker:
    """Create and connect a MockWorker to the running server."""
    worker = MockWorker(
        manager_url=ws_url,
        token=token,
        worker_id=worker_id,
        heartbeat_interval=0,
        **kwargs,
    )
    await worker.connect()
    return worker


def write_accounts_json(tmp_dir: Path, accounts: list[dict]) -> Path:
    """Write an accounts.json file for credential pool testing."""
    path = tmp_dir / "accounts.json"
    data = {"accounts": accounts, "groups": {"standard": {"description": "default"}}}
    path.write_text(json.dumps(data), encoding="utf-8")
    return path
