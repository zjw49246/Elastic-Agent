"""Manager <-> Worker communication protocol message types."""

from __future__ import annotations

import enum
from datetime import datetime, timezone
from typing import Annotated, Literal, Union

from pydantic import BaseModel, Field


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Base
# ---------------------------------------------------------------------------


class Message(BaseModel):
    type: str
    timestamp: datetime = Field(default_factory=_utcnow)


# ---------------------------------------------------------------------------
# Manager -> Worker (Commands)
# ---------------------------------------------------------------------------


class ExecuteMessage(Message):
    type: Literal["EXECUTE"] = "EXECUTE"
    task_id: str
    command: list[str]
    cwd: str
    env: dict[str, str] = Field(default_factory=dict)
    timeout: int | None = None
    # Structured agent params for PTY-hosted execution (claude-pty).
    # When set and the worker has claude-pty installed, the worker hosts the
    # agent in a persistent PTY session instead of spawning `command`.
    # `command` remains the subprocess fallback. Keys: prompt,
    # resume_session_id, config_dir, model.
    agent_params: dict | None = None


class StopMessage(Message):
    type: Literal["STOP"] = "STOP"
    task_id: str
    signal: str = "SIGTERM"


class ReadFileMessage(Message):
    type: Literal["READ_FILE"] = "READ_FILE"
    request_id: str
    path: str
    encoding: str = "utf-8"


class WatchFilesMessage(Message):
    type: Literal["WATCH_FILES"] = "WATCH_FILES"
    request_id: str
    paths: list[str]
    events: list[str] = Field(default_factory=lambda: ["created", "modified", "deleted"])


class UnwatchMessage(Message):
    type: Literal["UNWATCH"] = "UNWATCH"
    request_id: str


class HealthCheckMessage(Message):
    type: Literal["HEALTH_CHECK"] = "HEALTH_CHECK"


class UploadFileMessage(Message):
    type: Literal["UPLOAD_FILE"] = "UPLOAD_FILE"
    path: str
    content_base64: str
    mode: str = "0644"
    write_mode: Literal["overwrite", "append"] = "overwrite"


class SendInputMessage(Message):
    type: Literal["MESSAGE"] = "MESSAGE"
    task_id: str
    payload: str


class RegisterSyncMappingMessage(Message):
    type: Literal["REGISTER_SYNC_MAPPING"] = "REGISTER_SYNC_MAPPING"
    task_id: str
    book_slug: str
    oss_prefix: str
    watch_paths: list[str]
    session_path_hash: str


class UnregisterSyncMappingMessage(Message):
    type: Literal["UNREGISTER_SYNC_MAPPING"] = "UNREGISTER_SYNC_MAPPING"
    task_id: str


class ForceSyncMessage(Message):
    type: Literal["FORCE_SYNC"] = "FORCE_SYNC"
    task_id: str
    request_id: str | None = None
    book_slug: str | None = None
    cwd: str | None = None
    oss_prefix: str | None = None
    watch_paths: list[str] | None = None
    transient: bool = False


class CredentialLoginMessage(Message):
    type: Literal["CREDENTIAL_LOGIN"] = "CREDENTIAL_LOGIN"
    task_id: str
    slot_index: int
    credentials: dict[str, str]
    config_dir: str


class CredentialRotateMessage(Message):
    type: Literal["CREDENTIAL_ROTATE"] = "CREDENTIAL_ROTATE"
    task_id: str
    slot_index: int
    old_account_id: str
    new_credentials: dict[str, str]
    config_dir: str


# ---------------------------------------------------------------------------
# Worker -> Manager (Events)
# ---------------------------------------------------------------------------


class LogMessage(Message):
    type: Literal["LOG"] = "LOG"
    task_id: str
    stream: Literal["stdout", "stderr"]
    data: str
    parsed: dict | None = None


class ProcessExitMessage(Message):
    type: Literal["PROCESS_EXIT"] = "PROCESS_EXIT"
    task_id: str
    exit_code: int
    session_id: str | None = None
    error_type: str | None = None
    error_message: str | None = None


class FileContentMessage(Message):
    type: Literal["FILE_CONTENT"] = "FILE_CONTENT"
    request_id: str
    path: str
    content: str


class FileChangeMessage(Message):
    type: Literal["FILE_CHANGE"] = "FILE_CHANGE"
    path: str
    event: str
    content: str | None = None


class StatusMessage(Message):
    type: Literal["STATUS"] = "STATUS"
    cpu: float
    mem: float
    disk: float
    active_processes: list[str] = Field(default_factory=list)
    runtime_ready: bool = True
    runtime_error: str | None = None
    claude_cli_ok: bool = True
    claude_version: str | None = None
    claude_path: str | None = None


class HeartbeatMessage(Message):
    type: Literal["HEARTBEAT"] = "HEARTBEAT"
    uptime_seconds: int


class ErrorMessage(Message):
    type: Literal["ERROR"] = "ERROR"
    error_type: str
    message: str
    recoverable: bool = True


class FileSyncedMessage(Message):
    type: Literal["FILE_SYNCED"] = "FILE_SYNCED"
    task_id: str
    path: str
    oss_key: str
    synced_at: datetime
    md5: str


class ForceSyncResultMessage(Message):
    type: Literal["FORCE_SYNC_RESULT"] = "FORCE_SYNC_RESULT"
    task_id: str
    request_id: str | None = None
    files_attempted: int = 0
    files_synced: int
    success: bool
    error: str | None = None
    delivery_found: bool = False
    delivery_path: str | None = None
    manifest_key: str | None = None
    manuscript_path: str | None = None


class QuotaStatusMessage(Message):
    type: Literal["QUOTA_STATUS"] = "QUOTA_STATUS"
    task_id: str
    account_id: str
    usage_percent: float
    remaining_tokens: int | None = None
    window_resets_at: datetime | None = None
    five_hour_pct: float = 0.0
    seven_day_pct: float = 0.0
    five_hour_resets_at: datetime | None = None
    seven_day_resets_at: datetime | None = None
    available: bool = True


class CredentialLoginResultMessage(Message):
    type: Literal["CREDENTIAL_LOGIN_RESULT"] = "CREDENTIAL_LOGIN_RESULT"
    account_id: str
    slot_index: int
    success: bool
    error: str | None = None
    expires_at: datetime | None = None


class CredentialExhaustedMessage(Message):
    type: Literal["CREDENTIAL_EXHAUSTED"] = "CREDENTIAL_EXHAUSTED"
    worker_id: str
    slot_index: int
    account_id: str
    reason: str


class AuthMessage(Message):
    type: Literal["AUTH"] = "AUTH"
    token: str
    worker_id: str | None = None


class AuthResultMessage(Message):
    type: Literal["AUTH_RESULT"] = "AUTH_RESULT"
    success: bool
    worker_id: str | None = None
    error: str | None = None


# ---------------------------------------------------------------------------
# Message type enums and unions
# ---------------------------------------------------------------------------


class MessageDirection(str, enum.Enum):
    MANAGER_TO_WORKER = "manager_to_worker"
    WORKER_TO_MANAGER = "worker_to_manager"


ManagerToWorkerMessage = Annotated[
    Union[
        ExecuteMessage,
        StopMessage,
        ReadFileMessage,
        WatchFilesMessage,
        UnwatchMessage,
        HealthCheckMessage,
        UploadFileMessage,
        SendInputMessage,
        RegisterSyncMappingMessage,
        UnregisterSyncMappingMessage,
        ForceSyncMessage,
        CredentialLoginMessage,
        CredentialRotateMessage,
        AuthResultMessage,
    ],
    Field(discriminator="type"),
]

WorkerToManagerMessage = Annotated[
    Union[
        LogMessage,
        ProcessExitMessage,
        FileContentMessage,
        FileChangeMessage,
        StatusMessage,
        HeartbeatMessage,
        ErrorMessage,
        FileSyncedMessage,
        ForceSyncResultMessage,
        QuotaStatusMessage,
        CredentialLoginResultMessage,
        CredentialExhaustedMessage,
        AuthMessage,
    ],
    Field(discriminator="type"),
]

AnyMessage = Annotated[
    Union[
        ExecuteMessage,
        StopMessage,
        ReadFileMessage,
        WatchFilesMessage,
        UnwatchMessage,
        HealthCheckMessage,
        UploadFileMessage,
        SendInputMessage,
        RegisterSyncMappingMessage,
        UnregisterSyncMappingMessage,
        ForceSyncMessage,
        CredentialLoginMessage,
        CredentialRotateMessage,
        AuthResultMessage,
        LogMessage,
        ProcessExitMessage,
        FileContentMessage,
        FileChangeMessage,
        StatusMessage,
        HeartbeatMessage,
        ErrorMessage,
        FileSyncedMessage,
        ForceSyncResultMessage,
        QuotaStatusMessage,
        CredentialLoginResultMessage,
        CredentialExhaustedMessage,
        AuthMessage,
    ],
    Field(discriminator="type"),
]


# ---------------------------------------------------------------------------
# Parsing helper
# ---------------------------------------------------------------------------


from pydantic import TypeAdapter

_any_adapter = TypeAdapter(AnyMessage)


def parse_message(data: str | bytes | dict) -> Message:
    """Parse raw JSON into a typed Message instance."""
    if isinstance(data, (str, bytes)):
        return _any_adapter.validate_json(data)
    return _any_adapter.validate_python(data)
