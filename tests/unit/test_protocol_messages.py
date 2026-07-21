"""Tests for Protocol message serialization/deserialization — ALL message types (T-102)."""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from elastic_agent.core.protocols.messages import (
    AccountLoginMessage,
    AccountLoginResultMessage,
    AuthMessage,
    AuthResultMessage,
    CredentialExhaustedMessage,
    CredentialLoginMessage,
    CredentialLoginResultMessage,
    CredentialRotateMessage,
    ExecuteMessage,
    ErrorMessage,
    FileChangeMessage,
    FileContentMessage,
    FileSyncedMessage,
    HealthCheckMessage,
    HeartbeatMessage,
    LogMessage,
    ProcessExitMessage,
    QuotaStatusMessage,
    ReadFileMessage,
    RegisterSyncMappingMessage,
    SendInputMessage,
    StatusMessage,
    StopMessage,
    UnregisterSyncMappingMessage,
    UnwatchMessage,
    UploadFileMessage,
    WatchFilesMessage,
    parse_message,
)


class TestManagerToWorkerMessages:
    def test_execute_roundtrip(self):
        msg = ExecuteMessage(
            task_id="t-1",
            command=["claude", "--dangerously-skip-permissions", "-p", "hello"],
            cwd="/workspace",
            env={"HOME": "/root"},
            timeout=3600,
        )
        restored = parse_message(msg.model_dump_json())
        assert isinstance(restored, ExecuteMessage)
        assert restored.task_id == "t-1"
        assert restored.command == ["claude", "--dangerously-skip-permissions", "-p", "hello"]
        assert restored.cwd == "/workspace"
        assert restored.env == {"HOME": "/root"}
        assert restored.timeout == 3600

    def test_execute_defaults(self):
        msg = ExecuteMessage(task_id="t-1", command=["echo", "hi"], cwd="/tmp")
        assert msg.env == {}
        assert msg.timeout is None

    def test_stop_roundtrip(self):
        msg = StopMessage(task_id="t-1", signal="SIGKILL")
        restored = parse_message(msg.model_dump_json())
        assert isinstance(restored, StopMessage)
        assert restored.task_id == "t-1"
        assert restored.signal == "SIGKILL"

    def test_stop_default_signal(self):
        msg = StopMessage(task_id="t-1")
        assert msg.signal == "SIGTERM"

    def test_read_file_roundtrip(self):
        msg = ReadFileMessage(request_id="req-1", path="/tmp/out.txt", encoding="utf-8")
        restored = parse_message(msg.model_dump_json())
        assert isinstance(restored, ReadFileMessage)
        assert restored.request_id == "req-1"
        assert restored.path == "/tmp/out.txt"

    def test_watch_files_roundtrip(self):
        msg = WatchFilesMessage(
            request_id="req-2",
            paths=["/workspace/delivery", "/workspace/output"],
            events=["created", "modified"],
        )
        restored = parse_message(msg.model_dump_json())
        assert isinstance(restored, WatchFilesMessage)
        assert len(restored.paths) == 2
        assert restored.events == ["created", "modified"]

    def test_watch_files_default_events(self):
        msg = WatchFilesMessage(request_id="req-2", paths=["/a"])
        assert msg.events == ["created", "modified", "deleted"]

    def test_unwatch_roundtrip(self):
        msg = UnwatchMessage(request_id="req-2")
        restored = parse_message(msg.model_dump_json())
        assert isinstance(restored, UnwatchMessage)
        assert restored.request_id == "req-2"

    def test_health_check_roundtrip(self):
        msg = HealthCheckMessage()
        restored = parse_message(msg.model_dump_json())
        assert isinstance(restored, HealthCheckMessage)
        assert restored.type == "HEALTH_CHECK"

    def test_upload_file_roundtrip(self):
        msg = UploadFileMessage(
            path="/opt/app/config.yaml",
            content_base64="dGVzdA==",
            mode="0755",
            write_mode="append",
        )
        restored = parse_message(msg.model_dump_json())
        assert isinstance(restored, UploadFileMessage)
        assert restored.path == "/opt/app/config.yaml"
        assert restored.content_base64 == "dGVzdA=="
        assert restored.mode == "0755"
        assert restored.write_mode == "append"

    def test_upload_file_default_mode(self):
        msg = UploadFileMessage(path="/a", content_base64="YQ==")
        assert msg.mode == "0644"
        assert msg.write_mode == "overwrite"

    def test_send_input_roundtrip(self):
        msg = SendInputMessage(task_id="t-1", payload="fix the bug please")
        restored = parse_message(msg.model_dump_json())
        assert isinstance(restored, SendInputMessage)
        assert restored.task_id == "t-1"
        assert restored.payload == "fix the bug please"
        assert restored.type == "MESSAGE"

    def test_register_sync_mapping_roundtrip(self):
        msg = RegisterSyncMappingMessage(
            task_id="t-1",
            book_slug="my-book",
            oss_prefix="tasks/t-1/",
            watch_paths=["/workspace/delivery", "/workspace/state.json"],
            session_path_hash="abc123",
        )
        restored = parse_message(msg.model_dump_json())
        assert isinstance(restored, RegisterSyncMappingMessage)
        assert restored.book_slug == "my-book"
        assert restored.oss_prefix == "tasks/t-1/"
        assert len(restored.watch_paths) == 2

    def test_unregister_sync_mapping_roundtrip(self):
        msg = UnregisterSyncMappingMessage(task_id="t-1")
        restored = parse_message(msg.model_dump_json())
        assert isinstance(restored, UnregisterSyncMappingMessage)
        assert restored.task_id == "t-1"

    def test_credential_login_roundtrip(self):
        msg = CredentialLoginMessage(
            task_id="t-1",
            slot_index=0,
            credentials={"email": "a@b.com", "token": "sk-123"},
            config_dir="/root/.claude-prod",
        )
        restored = parse_message(msg.model_dump_json())
        assert isinstance(restored, CredentialLoginMessage)
        assert restored.slot_index == 0
        assert restored.credentials["email"] == "a@b.com"
        assert restored.config_dir == "/root/.claude-prod"

    def test_credential_rotate_roundtrip(self):
        msg = CredentialRotateMessage(
            task_id="t-1",
            slot_index=1,
            old_account_id="acct-old",
            new_credentials={"email": "new@b.com", "token": "sk-new"},
            config_dir="/root/.claude-edit-1",
        )
        restored = parse_message(msg.model_dump_json())
        assert isinstance(restored, CredentialRotateMessage)
        assert restored.old_account_id == "acct-old"
        assert restored.new_credentials["email"] == "new@b.com"

    def test_auth_result_roundtrip(self):
        msg = AuthResultMessage(success=True, worker_id="w-1")
        restored = parse_message(msg.model_dump_json())
        assert isinstance(restored, AuthResultMessage)
        assert restored.success is True
        assert restored.worker_id == "w-1"

    def test_auth_result_failure(self):
        msg = AuthResultMessage(success=False, error="invalid token")
        restored = parse_message(msg.model_dump_json())
        assert restored.success is False
        assert restored.error == "invalid token"


class TestWorkerToManagerMessages:
    def test_log_roundtrip(self):
        msg = LogMessage(
            task_id="t-1",
            stream="stdout",
            data='{"type":"assistant","content":"hello"}',
            parsed={"type": "assistant", "content": "hello"},
        )
        restored = parse_message(msg.model_dump_json())
        assert isinstance(restored, LogMessage)
        assert restored.task_id == "t-1"
        assert restored.stream == "stdout"
        assert restored.parsed["type"] == "assistant"

    def test_log_stderr(self):
        msg = LogMessage(task_id="t-1", stream="stderr", data="error output")
        assert msg.parsed is None
        restored = parse_message(msg.model_dump_json())
        assert restored.stream == "stderr"
        assert restored.parsed is None

    def test_process_exit_roundtrip(self):
        msg = ProcessExitMessage(task_id="t-1", exit_code=0)
        restored = parse_message(msg.model_dump_json())
        assert isinstance(restored, ProcessExitMessage)
        assert restored.exit_code == 0

    def test_process_exit_nonzero(self):
        msg = ProcessExitMessage(task_id="t-2", exit_code=137)
        restored = parse_message(msg.model_dump_json())
        assert restored.exit_code == 137

    def test_process_exit_with_failure_metadata(self):
        msg = ProcessExitMessage(
            task_id="t-3",
            exit_code=1,
            session_id="sess-123",
            error_type="runtime_timeout",
            error_message="Worker runtime timed out",
        )
        restored = parse_message(msg.model_dump_json())
        assert isinstance(restored, ProcessExitMessage)
        assert restored.session_id == "sess-123"
        assert restored.error_type == "runtime_timeout"
        assert restored.error_message == "Worker runtime timed out"

    def test_file_content_roundtrip(self):
        msg = FileContentMessage(
            request_id="req-1",
            path="/workspace/output.md",
            content="# Chapter 1\nOnce upon a time...",
        )
        restored = parse_message(msg.model_dump_json())
        assert isinstance(restored, FileContentMessage)
        assert restored.request_id == "req-1"
        assert "Chapter 1" in restored.content

    def test_file_change_roundtrip(self):
        msg = FileChangeMessage(
            path="/workspace/delivery/ch01.md",
            event="modified",
            content="updated content",
        )
        restored = parse_message(msg.model_dump_json())
        assert isinstance(restored, FileChangeMessage)
        assert restored.event == "modified"
        assert restored.content == "updated content"

    def test_file_change_no_content(self):
        msg = FileChangeMessage(path="/tmp/x", event="deleted")
        assert msg.content is None

    def test_status_roundtrip(self):
        msg = StatusMessage(
            cpu=45.2,
            mem=72.8,
            disk=60.0,
            active_processes=["claude-pid-1234", "claude-pid-5678"],
        )
        restored = parse_message(msg.model_dump_json())
        assert isinstance(restored, StatusMessage)
        assert restored.cpu == 45.2
        assert restored.mem == 72.8
        assert len(restored.active_processes) == 2
        assert restored.runtime_ready is True
        assert restored.claude_cli_ok is True

    def test_status_defaults(self):
        msg = StatusMessage(cpu=0.0, mem=0.0, disk=0.0)
        assert msg.active_processes == []

    def test_heartbeat_roundtrip(self):
        msg = HeartbeatMessage(uptime_seconds=3600)
        restored = parse_message(msg.model_dump_json())
        assert isinstance(restored, HeartbeatMessage)
        assert restored.uptime_seconds == 3600

    def test_error_roundtrip(self):
        msg = ErrorMessage(
            error_type="process_crash",
            message="Worker process segfaulted",
            recoverable=False,
        )
        restored = parse_message(msg.model_dump_json())
        assert isinstance(restored, ErrorMessage)
        assert restored.error_type == "process_crash"
        assert restored.message == "Worker process segfaulted"
        assert restored.recoverable is False

    def test_error_default_recoverable(self):
        msg = ErrorMessage(error_type="timeout", message="slow")
        assert msg.recoverable is True

    def test_file_synced_roundtrip(self):
        ts = datetime(2026, 5, 20, 12, 0, 0, tzinfo=timezone.utc)
        msg = FileSyncedMessage(
            task_id="t-1",
            path="/workspace/delivery/ch01.md",
            oss_key="tasks/t-1/delivery/ch01.md",
            synced_at=ts,
            md5="abc123def456",
        )
        restored = parse_message(msg.model_dump_json())
        assert isinstance(restored, FileSyncedMessage)
        assert restored.oss_key == "tasks/t-1/delivery/ch01.md"
        assert restored.md5 == "abc123def456"
        assert restored.synced_at == ts

    def test_quota_status_roundtrip(self):
        msg = QuotaStatusMessage(
            task_id="t-1",
            account_id="acct-1",
            usage_percent=65.0,
            remaining_tokens=50000,
            five_hour_pct=60.0,
            seven_day_pct=40.0,
            available=True,
        )
        restored = parse_message(msg.model_dump_json())
        assert isinstance(restored, QuotaStatusMessage)
        assert restored.five_hour_pct == 60.0
        assert restored.remaining_tokens == 50000

    def test_credential_login_result_roundtrip(self):
        msg = CredentialLoginResultMessage(
            account_id="acct-1",
            slot_index=0,
            success=True,
            expires_at=datetime(2026, 6, 1, 0, 0, 0, tzinfo=timezone.utc),
        )
        restored = parse_message(msg.model_dump_json())
        assert isinstance(restored, CredentialLoginResultMessage)
        assert restored.success is True

    def test_credential_exhausted_roundtrip(self):
        msg = CredentialExhaustedMessage(
            worker_id="w-1",
            slot_index=0,
            account_id="acct-1",
            reason="5-hour quota exceeded",
        )
        restored = parse_message(msg.model_dump_json())
        assert isinstance(restored, CredentialExhaustedMessage)
        assert restored.reason == "5-hour quota exceeded"

    def test_auth_roundtrip(self):
        msg = AuthMessage(token="bearer-token-xyz", worker_id="w-1")
        restored = parse_message(msg.model_dump_json())
        assert isinstance(restored, AuthMessage)
        assert restored.token == "bearer-token-xyz"
        assert restored.worker_id == "w-1"

    def test_auth_no_worker_id(self):
        msg = AuthMessage(token="tok")
        assert msg.worker_id is None


class TestParseMessageFromDict:
    def test_account_login_request_id_defaults_for_legacy_payloads(self):
        command = parse_message({
            "type": "ACCOUNT_LOGIN",
            "account_id": "acct-legacy",
            "email": "legacy@example.com",
            "email_token": "mail-token",
            "config_dir": "/root/.claude-legacy",
        })
        result = parse_message({
            "type": "ACCOUNT_LOGIN_RESULT",
            "account_id": "acct-legacy",
            "slot_index": 0,
            "success": True,
        })

        assert isinstance(command, AccountLoginMessage)
        assert command.login_request_id == ""
        assert isinstance(result, AccountLoginResultMessage)
        assert result.login_request_id == ""

    def test_all_types_from_dict(self):
        cases = [
            ({"type": "EXECUTE", "task_id": "t", "command": ["ls"], "cwd": "/"}, ExecuteMessage),
            ({"type": "STOP", "task_id": "t"}, StopMessage),
            ({"type": "READ_FILE", "request_id": "r", "path": "/a"}, ReadFileMessage),
            ({"type": "WATCH_FILES", "request_id": "r", "paths": ["/a"]}, WatchFilesMessage),
            ({"type": "UNWATCH", "request_id": "r"}, UnwatchMessage),
            ({"type": "HEALTH_CHECK"}, HealthCheckMessage),
            ({"type": "UPLOAD_FILE", "path": "/a", "content_base64": "YQ=="}, UploadFileMessage),
            ({"type": "MESSAGE", "task_id": "t", "payload": "hi"}, SendInputMessage),
            ({"type": "REGISTER_SYNC_MAPPING", "task_id": "t", "book_slug": "b", "oss_prefix": "p/", "watch_paths": ["/a"], "session_path_hash": "h"}, RegisterSyncMappingMessage),
            ({"type": "UNREGISTER_SYNC_MAPPING", "task_id": "t"}, UnregisterSyncMappingMessage),
            ({"type": "CREDENTIAL_LOGIN", "task_id": "t", "slot_index": 0, "credentials": {}, "config_dir": "/c"}, CredentialLoginMessage),
            ({"type": "CREDENTIAL_ROTATE", "task_id": "t", "slot_index": 0, "old_account_id": "o", "new_credentials": {}, "config_dir": "/c"}, CredentialRotateMessage),
            ({"type": "AUTH_RESULT", "success": True}, AuthResultMessage),
            ({"type": "LOG", "task_id": "t", "stream": "stdout", "data": "x"}, LogMessage),
            ({"type": "PROCESS_EXIT", "task_id": "t", "exit_code": 0}, ProcessExitMessage),
            ({"type": "FILE_CONTENT", "request_id": "r", "path": "/a", "content": "c"}, FileContentMessage),
            ({"type": "FILE_CHANGE", "path": "/a", "event": "created"}, FileChangeMessage),
            ({"type": "STATUS", "cpu": 1.0, "mem": 2.0, "disk": 3.0}, StatusMessage),
            ({"type": "HEARTBEAT", "uptime_seconds": 100}, HeartbeatMessage),
            ({"type": "ERROR", "error_type": "e", "message": "m"}, ErrorMessage),
            ({"type": "FILE_SYNCED", "task_id": "t", "path": "/a", "oss_key": "k", "synced_at": "2026-05-20T00:00:00Z", "md5": "m"}, FileSyncedMessage),
            ({"type": "QUOTA_STATUS", "task_id": "t", "account_id": "a", "usage_percent": 50.0}, QuotaStatusMessage),
            ({"type": "CREDENTIAL_LOGIN_RESULT", "account_id": "a", "slot_index": 0, "success": True}, CredentialLoginResultMessage),
            ({"type": "CREDENTIAL_EXHAUSTED", "worker_id": "w", "slot_index": 0, "account_id": "a", "reason": "r"}, CredentialExhaustedMessage),
            ({"type": "AUTH", "token": "t"}, AuthMessage),
        ]
        for raw, expected_cls in cases:
            msg = parse_message(raw)
            assert isinstance(msg, expected_cls), f"Expected {expected_cls.__name__} for type={raw['type']}, got {type(msg).__name__}"


class TestParseMessageFromBytes:
    def test_json_bytes(self):
        raw = b'{"type":"HEARTBEAT","uptime_seconds":999}'
        msg = parse_message(raw)
        assert isinstance(msg, HeartbeatMessage)
        assert msg.uptime_seconds == 999


class TestTimestampField:
    def test_timestamp_auto_generated(self):
        msg = HealthCheckMessage()
        assert msg.timestamp is not None
        assert msg.timestamp.tzinfo is not None

    def test_timestamp_preserved_on_roundtrip(self):
        ts = datetime(2026, 1, 15, 8, 30, 0, tzinfo=timezone.utc)
        msg = HeartbeatMessage(uptime_seconds=1, timestamp=ts)
        restored = parse_message(msg.model_dump_json())
        assert restored.timestamp == ts
