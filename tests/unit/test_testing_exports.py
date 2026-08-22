"""Tests for elastic_agent.testing package exports (T-208)."""

from __future__ import annotations


class TestTestingPackageExports:
    def test_all_exports_present(self):
        import elastic_agent.testing as t

        expected = [
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
        for name in expected:
            assert name in t.__all__, f"{name} missing from __all__"
            assert hasattr(t, name), f"{name} not importable from elastic_agent.testing"

    def test_individual_imports(self):
        from elastic_agent.testing import DryRunProvider
        from elastic_agent.testing import DryRunOperation
        from elastic_agent.testing import MockWorker
        from elastic_agent.testing import DEFAULT_NDJSON_SEQUENCE
        from elastic_agent.testing import MockOAuthServer
        from elastic_agent.testing import OAuthScenario
        from elastic_agent.testing import MockOSS
        from elastic_agent.testing import LocalWorkerProcess
        from elastic_agent.testing import create_test_manager
        from elastic_agent.testing import create_test_config
        from elastic_agent.testing import TestManagerResult

        assert DryRunProvider is not None
        assert DryRunOperation is not None
        assert MockWorker is not None
        assert DEFAULT_NDJSON_SEQUENCE is not None
        assert MockOAuthServer is not None
        assert OAuthScenario is not None
        assert MockOSS is not None
        assert LocalWorkerProcess is not None
        assert create_test_manager is not None
        assert create_test_config is not None
        assert TestManagerResult is not None

    def test_local_worker_process_class(self):
        from elastic_agent.testing import LocalWorkerProcess

        proc = LocalWorkerProcess()
        assert not proc.started
        assert proc.process is None
        assert proc.pid is None

    def test_create_test_manager_returns_result(self, tmp_path):
        from elastic_agent.testing import create_test_manager, TestManagerResult

        result = create_test_manager(tmp_dir=tmp_path)
        assert isinstance(result, TestManagerResult)
        assert result.manager is not None
        assert result.provider is not None
        assert result.oss is not None
        assert result.config is not None
        assert result.tmp_dir == tmp_path
