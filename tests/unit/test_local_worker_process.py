"""Tests for LocalWorkerProcess (T-205)."""

from __future__ import annotations

import pytest

from elastic_agent.testing import LocalWorkerProcess


class TestLocalWorkerProcessInit:
    def test_default_init(self):
        proc = LocalWorkerProcess()
        assert not proc.started
        assert proc.process is None
        assert proc.pid is None
        assert proc.output_lines == []

    def test_custom_init(self, tmp_path):
        proc = LocalWorkerProcess(
            worker_id="w-1",
            heartbeat_interval=10,
            log_dir=tmp_path / "logs",
            env={"CUSTOM_VAR": "test"},
            startup_timeout=15.0,
        )
        assert not proc.started
        assert proc._worker_id == "w-1"
        assert proc._heartbeat_interval == 10
        assert proc._startup_timeout == 15.0

    @pytest.mark.asyncio
    async def test_double_start_raises(self):
        proc = LocalWorkerProcess(startup_timeout=2.0)
        proc._started = True
        with pytest.raises(RuntimeError, match="already started"):
            await proc.start("ws://localhost:9999/ws/runtime", "token")

    @pytest.mark.asyncio
    async def test_stop_returns_none_when_not_started(self):
        proc = LocalWorkerProcess()
        result = await proc.stop()
        assert result is None

    @pytest.mark.asyncio
    async def test_wait_raises_when_not_started(self):
        proc = LocalWorkerProcess()
        with pytest.raises(RuntimeError, match="not started"):
            await proc.wait()

    @pytest.mark.asyncio
    async def test_start_with_bad_url_raises(self):
        proc = LocalWorkerProcess(startup_timeout=2.0)
        with pytest.raises(RuntimeError):
            await proc.start("ws://127.0.0.1:1/ws/runtime", "bad-token")


class TestLocalWorkerProcessImport:
    def test_importable_from_testing(self):
        from elastic_agent.testing import LocalWorkerProcess as LWP
        assert LWP is LocalWorkerProcess

    def test_in_all(self):
        import elastic_agent.testing as t
        assert "LocalWorkerProcess" in t.__all__
