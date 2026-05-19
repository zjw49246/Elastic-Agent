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


class QuotaStatusMessage(Message):
    type: Literal["QUOTA_STATUS"] = "QUOTA_STATUS"
    task_id: str
    account_id: str
    usage_percent: float
    remaining_tokens: int | None = None
    window_resets_at: datetime | None = None


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
        QuotaStatusMessage,
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
        QuotaStatusMessage,
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
