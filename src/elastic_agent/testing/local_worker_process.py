"""LocalWorkerProcess — starts a real Worker Runtime as a local subprocess.

T-205: For L3+ tests, launches the actual Worker Runtime in a subprocess
that connects to a test Manager's WebSocket endpoint. Unlike MockWorker,
this uses the real runtime code with real process execution capabilities.
"""

from __future__ import annotations

import asyncio
import logging
import sys
from pathlib import Path

logger = logging.getLogger(__name__)


class LocalWorkerProcess:
    """Launches a real Worker Runtime process locally for integration tests.

    The process runs ``python -m elastic_agent.worker.runtime_main`` which
    starts a WorkerRuntime that connects to the given Manager URL.

    Usage::

        proc = LocalWorkerProcess()
        await proc.start("ws://localhost:8000/ws/runtime", token="worker-token-1")

        # ... run tests that interact with the worker via Manager ...

        await proc.stop()
    """

    def __init__(
        self,
        *,
        worker_id: str | None = None,
        heartbeat_interval: int = 5,
        log_dir: str | Path | None = None,
        env: dict[str, str] | None = None,
        startup_timeout: float = 10.0,
    ) -> None:
        self._worker_id = worker_id
        self._heartbeat_interval = heartbeat_interval
        self._log_dir = str(log_dir) if log_dir else None
        self._extra_env = env or {}
        self._startup_timeout = startup_timeout

        self._process: asyncio.subprocess.Process | None = None
        self._started = False
        self._output_lines: list[str] = []
        self._reader_task: asyncio.Task | None = None

    @property
    def started(self) -> bool:
        return self._started

    @property
    def process(self) -> asyncio.subprocess.Process | None:
        return self._process

    @property
    def pid(self) -> int | None:
        return self._process.pid if self._process else None

    @property
    def output_lines(self) -> list[str]:
        return list(self._output_lines)

    async def start(self, manager_url: str, token: str) -> None:
        """Start the Worker Runtime subprocess.

        Args:
            manager_url: WebSocket URL of the Manager (e.g. ws://localhost:8000/ws/runtime).
            token: Authentication token for this worker.

        Raises:
            RuntimeError: If the process fails to start within the startup timeout.
        """
        if self._started:
            raise RuntimeError("LocalWorkerProcess already started")

        import os
        env = {**os.environ, **self._extra_env}

        cmd = [
            sys.executable, "-m", "elastic_agent.worker.runtime_main",
            "--manager-url", manager_url,
            "--token", token,
        ]
        if self._worker_id:
            cmd.extend(["--worker-id", self._worker_id])
        cmd.extend(["--heartbeat-interval", str(self._heartbeat_interval)])
        if self._log_dir:
            cmd.extend(["--log-dir", self._log_dir])

        logger.info("Starting LocalWorkerProcess: %s", " ".join(cmd))

        self._process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            env=env,
        )

        self._reader_task = asyncio.create_task(self._read_output())

        try:
            await asyncio.wait_for(self._wait_for_startup(), self._startup_timeout)
        except asyncio.TimeoutError:
            await self.stop()
            raise RuntimeError(
                f"LocalWorkerProcess failed to start within {self._startup_timeout}s. "
                f"Output: {''.join(self._output_lines[-20:])}"
            )

        self._started = True
        logger.info("LocalWorkerProcess started (pid=%s)", self._process.pid)

    async def _read_output(self) -> None:
        """Continuously read stdout/stderr from the subprocess."""
        assert self._process and self._process.stdout
        while True:
            line = await self._process.stdout.readline()
            if not line:
                break
            decoded = line.decode("utf-8", errors="replace").rstrip()
            self._output_lines.append(decoded)
            logger.debug("[LocalWorker pid=%s] %s", self._process.pid, decoded)

    async def _wait_for_startup(self) -> None:
        """Wait until the worker process emits a sign of successful connection."""
        while True:
            if self._process and self._process.returncode is not None:
                raise RuntimeError(
                    f"Worker process exited with code {self._process.returncode} "
                    f"during startup. Output: {''.join(self._output_lines[-20:])}"
                )
            for line in self._output_lines:
                if "authenticated" in line.lower() or "connected" in line.lower():
                    return
            await asyncio.sleep(0.1)

    async def stop(self, timeout: float = 5.0) -> int | None:
        """Stop the Worker Runtime subprocess.

        Sends SIGTERM, waits up to ``timeout`` seconds, then SIGKILL.

        Returns:
            The process exit code, or None if already stopped.
        """
        if self._process is None:
            return None

        if self._process.returncode is not None:
            self._started = False
            if self._reader_task:
                self._reader_task.cancel()
                try:
                    await self._reader_task
                except asyncio.CancelledError:
                    pass
            return self._process.returncode

        logger.info("Stopping LocalWorkerProcess (pid=%s)", self._process.pid)
        self._process.terminate()

        try:
            code = await asyncio.wait_for(self._process.wait(), timeout)
        except asyncio.TimeoutError:
            logger.warning("Worker process did not exit in %.0fs, sending SIGKILL", timeout)
            self._process.kill()
            code = await self._process.wait()

        self._started = False
        if self._reader_task:
            self._reader_task.cancel()
            try:
                await self._reader_task
            except asyncio.CancelledError:
                pass

        logger.info("LocalWorkerProcess exited with code %s", code)
        return code

    async def wait(self) -> int:
        """Wait for the process to finish and return exit code."""
        if self._process is None:
            raise RuntimeError("Process not started")
        return await self._process.wait()
