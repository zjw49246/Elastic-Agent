"""AgentType abstraction — how to invoke, parse, and resume agent processes.

T-017: Claude Code AgentType (install, launch, NDJSON parse, session_id, --resume, health check).
"""

from __future__ import annotations

import json
from abc import ABC, abstractmethod
from typing import Any

from elastic_agent.core.bootstrap_steps import CLAUDE_CODE_VERSION


class AgentType(ABC):
    """Abstract base class for agent implementations."""

    @property
    @abstractmethod
    def name(self) -> str:
        ...

    @property
    @abstractmethod
    def version(self) -> str:
        ...

    @abstractmethod
    def get_install_command(self) -> list[str]:
        ...

    @abstractmethod
    def get_launch_command(
        self,
        prompt: str,
        session_id: str | None = None,
        config_dir: str | None = None,
    ) -> list[str]:
        ...

    @abstractmethod
    def get_env_overrides(self, config_dir: str | None = None) -> dict[str, str]:
        """Return environment variable overrides for the agent process."""
        ...

    @abstractmethod
    def parse_stream_json(self, line: str) -> dict[str, Any] | None:
        ...

    @abstractmethod
    def extract_session_id(self, parsed_event: dict[str, Any]) -> str | None:
        ...

    @abstractmethod
    def get_health_check_command(self) -> list[str]:
        ...

    def get_resume_command(
        self,
        prompt: str,
        session_id: str,
        config_dir: str | None = None,
    ) -> list[str]:
        """Convenience wrapper — launch with an existing session_id."""
        return self.get_launch_command(prompt, session_id=session_id, config_dir=config_dir)

    def get_launch_params(
        self,
        prompt: str,
        session_id: str | None = None,
        config_dir: str | None = None,
        model: str | None = None,
    ) -> dict[str, Any]:
        """Structured launch params for PTY-hosted execution (ExecuteMessage.agent_params).

        Workers with claude-pty installed use these instead of exec'ing the
        command line. Default implementation works for any agent.
        """
        params: dict[str, Any] = {"agent": self.name, "prompt": prompt}
        if session_id:
            params["resume_session_id"] = session_id
        if config_dir:
            params["config_dir"] = config_dir
        if model:
            params["model"] = model
        return params


class ClaudeCodeAgentType(AgentType):
    """Concrete implementation for Claude Code (Node.js CLI).

    Encapsulates:
    - Installation via npm
    - CLI invocation with --output-format stream-json
    - NDJSON stream parsing (system/assistant/tool_use/tool_result/result)
    - session_id extraction from "result" events
    - --resume / --session assembly for edit flows
    - Health check via ``claude --version``
    """

    def __init__(
        self,
        binary_path: str = "claude",
        default_flags: list[str] | None = None,
    ) -> None:
        self.binary_path = binary_path
        self.default_flags = default_flags or [
            "--dangerously-skip-permissions",
            "--output-format",
            "stream-json",
        ]

    @property
    def name(self) -> str:
        return "claude-code"

    @property
    def version(self) -> str:
        return "0.1.0"

    def get_install_command(self) -> list[str]:
        return [
            "npm",
            "install",
            "-g",
            f"@anthropic-ai/claude-code@{CLAUDE_CODE_VERSION}",
            "--include=optional",
            "--foreground-scripts",
            "--force",
        ]

    def get_launch_command(
        self,
        prompt: str,
        session_id: str | None = None,
        config_dir: str | None = None,
    ) -> list[str]:
        cmd = [self.binary_path, "-p", prompt]
        if session_id:
            cmd.extend(["--resume", session_id])
        cmd.extend(self.default_flags)
        return cmd

    def get_env_overrides(self, config_dir: str | None = None) -> dict[str, str]:
        env: dict[str, str] = {}
        if config_dir:
            env["CLAUDE_CONFIG_DIR"] = config_dir
        return env

    def parse_stream_json(self, line: str) -> dict[str, Any] | None:
        if not line or not line.strip():
            return None
        try:
            obj = json.loads(line)
        except (json.JSONDecodeError, TypeError):
            return None
        if not isinstance(obj, dict) or "type" not in obj:
            return None
        return obj

    def extract_session_id(self, parsed_event: dict[str, Any]) -> str | None:
        if parsed_event.get("type") == "result":
            sid = parsed_event.get("session_id")
            return str(sid) if sid is not None else None
        return None

    def extract_cost(self, parsed_event: dict[str, Any]) -> float | None:
        if parsed_event.get("type") == "result":
            cost = parsed_event.get("cost_usd")
            if cost is not None:
                return float(cost)
        return None

    def extract_duration(self, parsed_event: dict[str, Any]) -> int | None:
        if parsed_event.get("type") == "result":
            dur = parsed_event.get("duration_seconds")
            if dur is not None:
                return int(dur)
        return None

    def get_health_check_command(self) -> list[str]:
        return [self.binary_path, "--version"]

    def scan_for_session_id(self, lines: list[str]) -> str | None:
        """Scan lines in reverse for the last result event with a session_id."""
        for line in reversed(lines):
            parsed = self.parse_stream_json(line)
            if parsed is None:
                continue
            sid = self.extract_session_id(parsed)
            if sid:
                return sid
        return None
