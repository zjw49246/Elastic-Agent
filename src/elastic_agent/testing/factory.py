"""create_test_manager() — one-click factory for fully mocked test environment.

T-204: Creates an ElasticAgentManager wired up with DryRunProvider,
MockOSS, and optionally auto-scaled MockWorkers. Used by both
EA framework tests and downstream ABS tests.
"""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Any

from elastic_agent.core.config import ElasticAgentConfig
from elastic_agent.harness.base import Harness
from elastic_agent.manager.manager import ElasticAgentManager
from elastic_agent.testing.dry_run_provider import DryRunProvider
from elastic_agent.testing.mock_oss import MockOSS


class TestManagerResult:
    """Container for test manager and its associated mock components.

    Attributes:
        manager: The ElasticAgentManager instance with all mocks wired up.
        provider: The DryRunProvider used by the manager.
        oss: The MockOSS storage backend.
        config: The ElasticAgentConfig used.
        tmp_dir: Path to temporary directory holding all state files.
    """

    def __init__(
        self,
        manager: ElasticAgentManager,
        provider: DryRunProvider,
        oss: MockOSS,
        config: ElasticAgentConfig,
        tmp_dir: Path,
    ) -> None:
        self.manager = manager
        self.provider = provider
        self.oss = oss
        self.config = config
        self.tmp_dir = tmp_dir


def create_test_config(tmp_dir: str | Path | None = None) -> tuple[ElasticAgentConfig, Path]:
    """Create an ElasticAgentConfig that writes all state to a temp directory.

    Returns (config, tmp_dir_path).
    """
    if tmp_dir is None:
        tmp_dir = Path(tempfile.mkdtemp(prefix="ea-test-"))
    else:
        tmp_dir = Path(tmp_dir)
    tmp_dir.mkdir(parents=True, exist_ok=True)

    config = ElasticAgentConfig(
        server={"host": "127.0.0.1", "port": 0},
        provider={"type": "dryrun"},
        worker={"heartbeat_interval": 5, "unhealthy_threshold": 3},
        bootstrap={"default_step_timeout": 10, "max_retries": 1},
        credentials={
            "accounts_file": str(tmp_dir / "accounts.json"),
            "pool_status_file": str(tmp_dir / "pool_status.json"),
        },
        external_api={"trace_buffer_size": 100},
        logging={
            "operations_log": str(tmp_dir / "operations.log"),
            "log_level": "DEBUG",
        },
        monitor={"health_check_interval": 5, "reconcile_interval": 60},
        drain={"timeout": 30},
        registry={"path": str(tmp_dir / "registry.json")},
        task_registry={"path": str(tmp_dir / "task_registry.json")},
        webhook={
            "dead_letter_path": str(tmp_dir / "webhook_dead_letters.json"),
            "retry_delays": [0.1, 0.5],
            "send_timeout": 5.0,
        },
    )
    return config, tmp_dir


def create_test_manager(
    *,
    provider: DryRunProvider | None = None,
    oss: MockOSS | None = None,
    harness: Harness | None = None,
    tmp_dir: str | Path | None = None,
    config_overrides: dict[str, Any] | None = None,
) -> TestManagerResult:
    """Create an ElasticAgentManager fully wired with mock components.

    Usage::

        result = create_test_manager()
        await result.manager.start()

        # Scale out creates DryRun instances
        nodes = await result.manager.scale_out(count=2)

        # Inspect provider operations
        assert len(result.provider.get_operations("create")) == 2

        # Clean up
        await result.manager.stop()

    Args:
        provider: Custom DryRunProvider instance. Created if None.
        oss: Custom MockOSS instance. Created if None.
        harness: Optional Harness implementation.
        tmp_dir: Directory for state files. Uses tempdir if None.
        config_overrides: Extra config values merged into the generated config.

    Returns:
        TestManagerResult with manager, provider, oss, config, and tmp_dir.
    """
    config, resolved_tmp = create_test_config(tmp_dir)

    if config_overrides:
        config_dict = config.model_dump()
        _deep_merge(config_dict, config_overrides)
        config = ElasticAgentConfig(**config_dict)

    if provider is None:
        provider = DryRunProvider()
    if oss is None:
        oss = MockOSS(resolved_tmp / "oss_storage")

    manager = ElasticAgentManager(
        config=config,
        provider=provider,
        harness=harness,
        file_storage=oss,
    )

    return TestManagerResult(
        manager=manager,
        provider=provider,
        oss=oss,
        config=config,
        tmp_dir=resolved_tmp,
    )


def _deep_merge(base: dict, override: dict) -> None:
    """Recursively merge override into base dict."""
    for key, value in override.items():
        if key in base and isinstance(base[key], dict) and isinstance(value, dict):
            _deep_merge(base[key], value)
        else:
            base[key] = value
