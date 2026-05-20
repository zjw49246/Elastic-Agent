"""Tests for the test-level framework (T-206) and pytest markers (T-207)."""

from __future__ import annotations

import os
from unittest.mock import patch

import pytest

from tests.conftest import get_test_level, pytest_collection_modifyitems


class TestGetTestLevel:
    def test_default_level(self):
        with patch.dict(os.environ, {}, clear=True):
            os.environ.pop("EA_TEST_LEVEL", None)
            assert get_test_level() == 1

    def test_custom_level(self):
        with patch.dict(os.environ, {"EA_TEST_LEVEL": "3"}):
            assert get_test_level() == 3


class TestPytestCollectionModifyItems:
    def _make_item(self, markers=None):
        """Create a minimal mock pytest item."""

        class MockMarker:
            def __init__(self, name):
                self.name = name

        class MockItem:
            def __init__(self, markers):
                self._markers = [MockMarker(m) for m in (markers or [])]
                self._added = []

            def iter_markers(self):
                return iter(self._markers)

            def add_marker(self, marker):
                self._added.append(marker)

        return MockItem(markers)

    def test_skip_high_level_tests(self):
        item = self._make_item(["level3"])
        with patch.dict(os.environ, {"EA_TEST_LEVEL": "1"}):
            pytest_collection_modifyitems(None, [item])
        assert len(item._added) == 1

    def test_allow_matching_level(self):
        item = self._make_item(["level1"])
        with patch.dict(os.environ, {"EA_TEST_LEVEL": "1"}):
            pytest_collection_modifyitems(None, [item])
        assert len(item._added) == 0

    def test_allow_lower_level(self):
        item = self._make_item(["level0"])
        with patch.dict(os.environ, {"EA_TEST_LEVEL": "3"}):
            pytest_collection_modifyitems(None, [item])
        assert len(item._added) == 0

    def test_unmarked_tests_always_run(self):
        item = self._make_item([])
        with patch.dict(os.environ, {"EA_TEST_LEVEL": "0"}):
            pytest_collection_modifyitems(None, [item])
        assert len(item._added) == 0

    def test_non_level_markers_ignored(self):
        item = self._make_item(["slow", "integration"])
        with patch.dict(os.environ, {"EA_TEST_LEVEL": "0"}):
            pytest_collection_modifyitems(None, [item])
        assert len(item._added) == 0


class TestProviderFixture:
    def test_default_returns_dryrun(self, provider):
        from elastic_agent.testing import DryRunProvider
        assert isinstance(provider, DryRunProvider)


class TestWorkerFactory:
    def test_default_returns_mock_factory(self, worker_factory):
        worker = worker_factory(manager_url="ws://test:8000/ws", token="t1")
        from elastic_agent.testing import MockWorker
        assert isinstance(worker, MockWorker)


class TestOssFixture:
    def test_default_returns_mock_oss(self, oss):
        from elastic_agent.testing import MockOSS
        assert isinstance(oss, MockOSS)


class TestTestManagerFixture:
    def test_returns_test_manager_result(self, test_manager):
        from elastic_agent.testing import TestManagerResult
        assert isinstance(test_manager, TestManagerResult)
        assert test_manager.manager is not None
        assert test_manager.provider is not None
        assert test_manager.oss is not None


@pytest.mark.level0
class TestLevel0Marker:
    def test_level0_runs_at_default(self):
        assert True


@pytest.mark.level1
class TestLevel1Marker:
    def test_level1_runs_at_default(self):
        assert True
