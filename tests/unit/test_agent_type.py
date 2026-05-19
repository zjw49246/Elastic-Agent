"""Tests for AgentType and ClaudeCodeAgentType (T-017)."""

from __future__ import annotations

import json

import pytest

from elastic_agent.core.agent_type import AgentType, ClaudeCodeAgentType


# ------------------------------------------------------------------
# ClaudeCodeAgentType instantiation
# ------------------------------------------------------------------


class TestClaudeCodeAgentTypeBasics:
    def setup_method(self):
        self.agent = ClaudeCodeAgentType()

    def test_name(self):
        assert self.agent.name == "claude-code"

    def test_version(self):
        assert isinstance(self.agent.version, str)
        assert self.agent.version

    def test_custom_binary_path(self):
        agent = ClaudeCodeAgentType(binary_path="/usr/local/bin/claude")
        cmd = agent.get_health_check_command()
        assert cmd[0] == "/usr/local/bin/claude"

    def test_custom_default_flags(self):
        agent = ClaudeCodeAgentType(default_flags=["--verbose"])
        cmd = agent.get_launch_command("hello")
        assert "--verbose" in cmd


# ------------------------------------------------------------------
# get_install_command
# ------------------------------------------------------------------


class TestInstallCommand:
    def test_install_command_uses_npm(self):
        agent = ClaudeCodeAgentType()
        cmd = agent.get_install_command()
        assert cmd[0] == "npm"
        assert "install" in cmd
        assert any("claude-code" in part for part in cmd)


# ------------------------------------------------------------------
# get_launch_command
# ------------------------------------------------------------------


class TestLaunchCommand:
    def setup_method(self):
        self.agent = ClaudeCodeAgentType()

    def test_basic_prompt(self):
        cmd = self.agent.get_launch_command("do something")
        assert cmd[0] == "claude"
        assert "-p" in cmd
        idx = cmd.index("-p")
        assert cmd[idx + 1] == "do something"

    def test_with_session_id(self):
        cmd = self.agent.get_launch_command("continue work", session_id="abc123")
        assert "--resume" in cmd
        idx = cmd.index("--resume")
        assert cmd[idx + 1] == "abc123"

    def test_without_session_id(self):
        cmd = self.agent.get_launch_command("new task")
        assert "--resume" not in cmd

    def test_default_flags_included(self):
        cmd = self.agent.get_launch_command("test")
        assert "--output-format" in cmd
        assert "stream-json" in cmd
        assert "--dangerously-skip-permissions" in cmd

    def test_resume_command_delegates(self):
        cmd = self.agent.get_resume_command("edit chapter 3", "sess-42")
        assert "--resume" in cmd
        assert "sess-42" in cmd


# ------------------------------------------------------------------
# get_env_overrides
# ------------------------------------------------------------------


class TestEnvOverrides:
    def setup_method(self):
        self.agent = ClaudeCodeAgentType()

    def test_no_config_dir(self):
        env = self.agent.get_env_overrides()
        assert env == {}

    def test_with_config_dir(self):
        env = self.agent.get_env_overrides(config_dir="/root/.claude-prod")
        assert env == {"CLAUDE_CONFIG_DIR": "/root/.claude-prod"}


# ------------------------------------------------------------------
# parse_stream_json
# ------------------------------------------------------------------


class TestParseStreamJson:
    def setup_method(self):
        self.agent = ClaudeCodeAgentType()

    def test_valid_assistant_message(self):
        line = json.dumps({"type": "assistant", "content": "Hello"})
        result = self.agent.parse_stream_json(line)
        assert result is not None
        assert result["type"] == "assistant"
        assert result["content"] == "Hello"

    def test_valid_result_message(self):
        line = json.dumps({
            "type": "result",
            "session_id": "sess-001",
            "cost_usd": 0.45,
            "duration_seconds": 123,
        })
        result = self.agent.parse_stream_json(line)
        assert result is not None
        assert result["type"] == "result"
        assert result["session_id"] == "sess-001"

    def test_valid_tool_use(self):
        line = json.dumps({"type": "tool_use", "tool": "Read", "input": {"file_path": "/tmp/x"}})
        result = self.agent.parse_stream_json(line)
        assert result is not None
        assert result["type"] == "tool_use"
        assert result["tool"] == "Read"

    def test_valid_system_message(self):
        line = json.dumps({"type": "system", "content": "Model: Claude Opus 4.6"})
        result = self.agent.parse_stream_json(line)
        assert result is not None
        assert result["type"] == "system"

    def test_empty_line(self):
        assert self.agent.parse_stream_json("") is None
        assert self.agent.parse_stream_json("   ") is None

    def test_none_line(self):
        assert self.agent.parse_stream_json(None) is None

    def test_non_json_line(self):
        assert self.agent.parse_stream_json("not json at all") is None

    def test_json_without_type_field(self):
        line = json.dumps({"content": "missing type"})
        assert self.agent.parse_stream_json(line) is None

    def test_json_array(self):
        assert self.agent.parse_stream_json("[1,2,3]") is None

    def test_json_string(self):
        assert self.agent.parse_stream_json('"just a string"') is None


# ------------------------------------------------------------------
# extract_session_id
# ------------------------------------------------------------------


class TestExtractSessionId:
    def setup_method(self):
        self.agent = ClaudeCodeAgentType()

    def test_from_result_event(self):
        event = {"type": "result", "session_id": "abc-123", "cost_usd": 0.5}
        assert self.agent.extract_session_id(event) == "abc-123"

    def test_from_non_result_event(self):
        event = {"type": "assistant", "content": "hello"}
        assert self.agent.extract_session_id(event) is None

    def test_result_without_session_id(self):
        event = {"type": "result", "cost_usd": 0.3}
        assert self.agent.extract_session_id(event) is None

    def test_numeric_session_id(self):
        event = {"type": "result", "session_id": 42}
        assert self.agent.extract_session_id(event) == "42"


# ------------------------------------------------------------------
# extract_cost / extract_duration
# ------------------------------------------------------------------


class TestExtractCostAndDuration:
    def setup_method(self):
        self.agent = ClaudeCodeAgentType()

    def test_extract_cost(self):
        event = {"type": "result", "cost_usd": 1.23}
        assert self.agent.extract_cost(event) == pytest.approx(1.23)

    def test_extract_cost_non_result(self):
        assert self.agent.extract_cost({"type": "assistant"}) is None

    def test_extract_duration(self):
        event = {"type": "result", "duration_seconds": 45}
        assert self.agent.extract_duration(event) == 45

    def test_extract_duration_non_result(self):
        assert self.agent.extract_duration({"type": "assistant"}) is None


# ------------------------------------------------------------------
# scan_for_session_id
# ------------------------------------------------------------------


class TestScanForSessionId:
    def setup_method(self):
        self.agent = ClaudeCodeAgentType()

    def test_finds_session_in_last_result(self):
        lines = [
            json.dumps({"type": "assistant", "content": "working..."}),
            json.dumps({"type": "tool_use", "tool": "Read"}),
            json.dumps({"type": "result", "session_id": "final-sess"}),
        ]
        assert self.agent.scan_for_session_id(lines) == "final-sess"

    def test_finds_last_session_id(self):
        lines = [
            json.dumps({"type": "result", "session_id": "first-sess"}),
            json.dumps({"type": "assistant", "content": "more work"}),
            json.dumps({"type": "result", "session_id": "second-sess"}),
        ]
        assert self.agent.scan_for_session_id(lines) == "second-sess"

    def test_no_result_returns_none(self):
        lines = [
            json.dumps({"type": "assistant", "content": "working"}),
            "not json",
            "",
        ]
        assert self.agent.scan_for_session_id(lines) is None

    def test_empty_lines(self):
        assert self.agent.scan_for_session_id([]) is None

    def test_handles_mixed_content(self):
        lines = [
            "plain text log line",
            json.dumps({"type": "system", "content": "init"}),
            "another plain line",
            json.dumps({"type": "result", "session_id": "found-it"}),
            "trailing text",
        ]
        assert self.agent.scan_for_session_id(lines) == "found-it"


# ------------------------------------------------------------------
# get_health_check_command
# ------------------------------------------------------------------


class TestHealthCheck:
    def test_health_check_command(self):
        agent = ClaudeCodeAgentType()
        cmd = agent.get_health_check_command()
        assert cmd == ["claude", "--version"]

    def test_health_check_custom_binary(self):
        agent = ClaudeCodeAgentType(binary_path="/opt/claude")
        cmd = agent.get_health_check_command()
        assert cmd == ["/opt/claude", "--version"]


# ------------------------------------------------------------------
# AgentType is abstract
# ------------------------------------------------------------------


class TestAgentTypeAbstract:
    def test_cannot_instantiate(self):
        with pytest.raises(TypeError):
            AgentType()
