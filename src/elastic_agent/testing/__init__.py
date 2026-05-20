"""elastic_agent.testing — Mock tools for testing EA framework and downstream services.

Exports:
    DryRunProvider      — In-memory cloud provider (T-200)
    MockWorker          — Simulated Worker Runtime via WebSocket (T-201)
    MockOAuthServer     — Simulated 171mail + Anthropic OAuth + Usage API (T-202)
    MockOSS             — Local filesystem storage backend (T-203)
    create_test_manager — One-click factory for fully mocked Manager (T-204)
    TestManagerResult   — Return type of create_test_manager
    create_test_config  — Helper to create test-friendly config
    LocalWorkerProcess  — Real Worker Runtime subprocess for L3+ tests (T-205)

Usage::

    from elastic_agent.testing import (
        DryRunProvider,
        MockWorker,
        MockOAuthServer,
        MockOSS,
        LocalWorkerProcess,
        create_test_manager,
    )
"""

from elastic_agent.testing.dry_run_provider import DryRunProvider, DryRunOperation
from elastic_agent.testing.mock_worker import MockWorker, DEFAULT_NDJSON_SEQUENCE
from elastic_agent.testing.mock_oauth_server import MockOAuthServer, OAuthScenario
from elastic_agent.testing.mock_oss import MockOSS
from elastic_agent.testing.factory import create_test_manager, create_test_config, TestManagerResult
from elastic_agent.testing.local_worker_process import LocalWorkerProcess

__all__ = [
    "DryRunProvider",
    "DryRunOperation",
    "MockWorker",
    "DEFAULT_NDJSON_SEQUENCE",
    "MockOAuthServer",
    "OAuthScenario",
    "MockOSS",
    "LocalWorkerProcess",
    "create_test_manager",
    "create_test_config",
    "TestManagerResult",
]
