"""Bootstrap Pipeline — pluggable step execution with failure strategies.

T-018: Orchestrates the sequence of BootstrapStep items returned by the Harness,
executing each on a remote Worker via SSH. Supports per-step timeouts,
configurable failure strategies, and retry limits.
"""

from __future__ import annotations

import asyncio
import enum
import logging
import time
from dataclasses import dataclass, field
from typing import Any

from elastic_agent.core.config import BootstrapConfig
from elastic_agent.harness.base import BootstrapStep

logger = logging.getLogger(__name__)


class FailureStrategy(str, enum.Enum):
    TERMINATE_AND_RETRY = "terminate_and_retry"
    RETRY_FROM_FAILED = "retry_from_failed"
    LEAVE_FOR_DEBUG = "leave_for_debug"


class StepStatus(str, enum.Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class StepResult:
    step_name: str
    status: StepStatus
    exit_code: int | None = None
    stdout: str = ""
    stderr: str = ""
    duration_seconds: float = 0.0
    attempt: int = 1
    error: str | None = None


@dataclass
class BootstrapResult:
    node_id: str
    success: bool
    steps: list[StepResult] = field(default_factory=list)
    total_duration_seconds: float = 0.0
    attempts_used: int = 1
    failure_strategy_applied: FailureStrategy | None = None
    error: str | None = None


class SSHExecutor:
    """Executes commands on a remote host via SSH subprocess."""

    def __init__(
        self,
        host: str,
        user: str = "root",
        key_path: str | None = None,
        port: int = 22,
        use_sudo: bool | None = None,
    ) -> None:
        self.host = host
        self.user = user
        self.key_path = key_path
        self.port = port
        # Non-root SSH users (e.g. Ubuntu AMIs log in as `ubuntu`) can't run the
        # bootstrap's apt/systemctl/`/etc` writes directly; wrap the whole remote
        # command in `sudo` so it runs as root. Assumes passwordless sudo (the
        # default on cloud images). Explicit override via use_sudo.
        self.use_sudo = (user != "root") if use_sudo is None else use_sudo

    def _build_ssh_cmd(self, command: str | list[str], env: dict[str, str] | None = None, cwd: str | None = None) -> list[str]:
        ssh_args = [
            "ssh",
            "-o", "StrictHostKeyChecking=no",
            "-o", "UserKnownHostsFile=/dev/null",
            "-o", "ConnectTimeout=10",
            "-p", str(self.port),
        ]
        if self.key_path:
            ssh_args.extend(["-i", self.key_path])
        ssh_args.append(f"{self.user}@{self.host}")

        remote_parts: list[str] = []
        if env:
            for k, v in env.items():
                remote_parts.append(f"export {k}={_shell_quote(v)}")
        if cwd:
            remote_parts.append(f"cd {_shell_quote(cwd)}")

        if isinstance(command, list):
            cmd_str = " ".join(command)
        else:
            cmd_str = command
        remote_parts.append(cmd_str)

        remote_cmd = " && ".join(remote_parts)
        if self.use_sudo:
            # Run the whole pipeline as root under a login shell.
            remote_cmd = f"sudo -n bash -c {_shell_quote(remote_cmd)}"
        ssh_args.append(remote_cmd)
        return ssh_args

    async def execute(
        self,
        command: str | list[str],
        timeout: int = 300,
        env: dict[str, str] | None = None,
        cwd: str | None = None,
    ) -> tuple[int, str, str]:
        ssh_cmd = self._build_ssh_cmd(command, env=env, cwd=cwd)
        try:
            proc = await asyncio.create_subprocess_exec(
                *ssh_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout_bytes, stderr_bytes = await asyncio.wait_for(
                proc.communicate(), timeout=timeout
            )
            return (
                proc.returncode or 0,
                stdout_bytes.decode(errors="replace"),
                stderr_bytes.decode(errors="replace"),
            )
        except asyncio.TimeoutError:
            try:
                proc.kill()
                await proc.wait()
            except Exception:
                pass
            return -1, "", f"Command timed out after {timeout}s"
        except Exception as exc:
            return -1, "", str(exc)


def _shell_quote(s: str) -> str:
    return "'" + s.replace("'", "'\"'\"'") + "'"


class BootstrapPipeline:
    """Executes bootstrap steps on a node with retry and failure handling."""

    def __init__(self, config: BootstrapConfig) -> None:
        self.config = config
        self.failure_strategy = FailureStrategy(config.failure_strategy)
        self.max_retries = config.max_retries
        self.default_timeout = config.default_step_timeout

    async def execute(
        self,
        node_id: str,
        steps: list[BootstrapStep],
        executor: SSHExecutor,
        on_step_start: Any = None,
        on_step_complete: Any = None,
    ) -> BootstrapResult:
        overall_start = time.monotonic()
        result = BootstrapResult(node_id=node_id, success=False)

        for attempt in range(1, self.max_retries + 2):
            result.attempts_used = attempt
            step_results = await self._run_steps(
                steps, executor, attempt,
                on_step_start=on_step_start,
                on_step_complete=on_step_complete,
                start_from=self._find_resume_index(result.steps) if attempt > 1 else 0,
            )
            result.steps = step_results

            if all(sr.status == StepStatus.COMPLETED for sr in step_results):
                result.success = True
                break

            failed_step = next((sr for sr in step_results if sr.status == StepStatus.FAILED), None)
            if failed_step:
                result.error = f"Step '{failed_step.step_name}' failed: {failed_step.error or failed_step.stderr}"

            if attempt > self.max_retries:
                result.failure_strategy_applied = self.failure_strategy
                break

            if self.failure_strategy == FailureStrategy.LEAVE_FOR_DEBUG:
                result.failure_strategy_applied = FailureStrategy.LEAVE_FOR_DEBUG
                break

            if self.failure_strategy == FailureStrategy.TERMINATE_AND_RETRY:
                result.failure_strategy_applied = FailureStrategy.TERMINATE_AND_RETRY
                break

            if self.failure_strategy == FailureStrategy.RETRY_FROM_FAILED:
                result.failure_strategy_applied = FailureStrategy.RETRY_FROM_FAILED
                logger.info(
                    "Retrying bootstrap for node %s from failed step (attempt %d/%d)",
                    node_id, attempt + 1, self.max_retries + 1,
                )

        result.total_duration_seconds = time.monotonic() - overall_start
        return result

    async def _run_steps(
        self,
        steps: list[BootstrapStep],
        executor: SSHExecutor,
        attempt: int,
        on_step_start: Any = None,
        on_step_complete: Any = None,
        start_from: int = 0,
    ) -> list[StepResult]:
        results: list[StepResult] = []

        for i, step in enumerate(steps):
            if i < start_from:
                results.append(StepResult(
                    step_name=step.name,
                    status=StepStatus.COMPLETED,
                    attempt=attempt,
                ))
                continue

            if on_step_start:
                await on_step_start(step.name, i, len(steps))

            step_result = await self._execute_step(step, executor, attempt)
            results.append(step_result)

            if on_step_complete:
                await on_step_complete(step.name, step_result)

            if step_result.status == StepStatus.FAILED:
                for remaining in steps[i + 1:]:
                    results.append(StepResult(
                        step_name=remaining.name,
                        status=StepStatus.SKIPPED,
                        attempt=attempt,
                    ))
                break

        return results

    async def _execute_step(
        self,
        step: BootstrapStep,
        executor: SSHExecutor,
        attempt: int,
    ) -> StepResult:
        timeout = step.timeout or self.default_timeout
        max_step_retries = step.retry_count

        for step_attempt in range(max_step_retries + 1):
            start = time.monotonic()
            exit_code, stdout, stderr = await executor.execute(
                command=step.command,
                timeout=timeout,
                env=step.env or None,
                cwd=step.cwd,
            )
            duration = time.monotonic() - start

            if exit_code == 0:
                return StepResult(
                    step_name=step.name,
                    status=StepStatus.COMPLETED,
                    exit_code=exit_code,
                    stdout=stdout,
                    stderr=stderr,
                    duration_seconds=duration,
                    attempt=attempt,
                )

            if step_attempt < max_step_retries:
                logger.warning(
                    "Step '%s' failed (exit=%s), retrying (%d/%d)",
                    step.name, exit_code, step_attempt + 1, max_step_retries,
                )

        return StepResult(
            step_name=step.name,
            status=StepStatus.FAILED,
            exit_code=exit_code,
            stdout=stdout,
            stderr=stderr,
            duration_seconds=duration,
            attempt=attempt,
            error=f"stdout: {stdout.strip()[-300:]}\nstderr: {stderr.strip()[-200:]}" if stdout or stderr else f"exit code {exit_code}",
        )

    def _find_resume_index(self, previous_results: list[StepResult]) -> int:
        for i, sr in enumerate(previous_results):
            if sr.status == StepStatus.FAILED:
                return i
        return 0
