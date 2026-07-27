"""Manager <-> Worker communication protocol message types."""

from __future__ import annotations

import enum
import uuid
from datetime import datetime, timezone
from typing import Annotated, Literal, Union

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter


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
    # Batch / Mode-B fields. `job_id` ties this run to a BatchJob; when
    # `watch_exhaustion` is set the worker scans the opaque command's output for
    # rate-limit/auth-failure banners and, on a hit, interrupts the process and
    # emits a RunExhaustedMessage so the orchestrator can rotate the account and
    # restart with --resume (rotation strategy "a").
    job_id: str | None = None
    watch_exhaustion: bool = False


class StopMessage(Message):
    type: Literal["STOP"] = "STOP"
    task_id: str
    signal: str = "SIGTERM"


class EventAckMessage(Message):
    """Manager -> Worker acknowledgement for a durable terminal event.

    Workers retain critical events until this acknowledgement arrives.  The
    event id also lets the Manager make reconnect replays idempotent.
    """

    type: Literal["EVENT_ACK"] = "EVENT_ACK"
    event_id: str


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


class AccountLoginMessage(Message):
    """Manager -> Worker: log this account in ON the worker (worker-autonomous).

    Unlike CREDENTIAL_LOGIN (which pushes already-obtained tokens down), this
    carries only the account identity + login inputs; the worker runs the
    provider-specific login flow locally and the resulting OAuth credentials
    are written on the worker and never sent back up. ``provider`` continues to
    select the mailbox backend; ``agent_type`` selects Claude versus Codex.
    """

    type: Literal["ACCOUNT_LOGIN"] = "ACCOUNT_LOGIN"
    login_request_id: str = ""
    account_id: str
    email: str
    agent_type: Literal["claude", "codex"] = "claude"
    email_token: str = Field(default="", repr=False)
    password: str = Field(default="", repr=False)
    config_dir: str
    provider: str | None = None
    slot_index: int = 0
    # Worker-side browser state-machine budget.  This is deliberately below
    # the Manager's end-to-end wait so post-browser validation and correlated
    # cancellation still have time to finish.
    login_timeout_seconds: int = Field(default=900, ge=60, le=1200)


class AccountLoginOtpMessage(Message):
    """Manager -> Worker: answer the OTP challenge for one login request."""

    type: Literal["ACCOUNT_LOGIN_OTP"] = "ACCOUNT_LOGIN_OTP"
    login_request_id: str = ""
    account_id: str
    challenge_id: str = ""
    code: str = Field(repr=False)


class AccountLoginCancelMessage(Message):
    """Manager -> Worker: cancel one correlated worker-local login."""

    type: Literal["ACCOUNT_LOGIN_CANCEL"] = "ACCOUNT_LOGIN_CANCEL"
    login_request_id: str
    account_id: str
    reason: Literal["manager_timeout", "manager_cancelled"]


class AgentApiConfigureMessage(Message):
    """Manager -> Worker: install one managed Agent API credential.

    The API key is transported only for this correlated setup request.  It is
    excluded from model representations, and validation errors hide their
    inputs so malformed payloads cannot echo the credential into logs.
    """

    model_config = ConfigDict(hide_input_in_errors=True)

    type: Literal["AGENT_API_CONFIGURE"] = "AGENT_API_CONFIGURE"
    request_id: str
    account_id: str
    provider: Literal["cloudrouter"] = "cloudrouter"
    agent_type: Literal["claude", "codex"]
    config_dir: str
    api_key: str = Field(repr=False, min_length=1)
    models: dict[str, list[str]] = Field(default_factory=dict)


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
    event_id: str = Field(default_factory=lambda: uuid.uuid4().hex)


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
    # A process can disappear from ``active_processes`` just before its durable
    # PROCESS_EXIT is delivered/ACKed.  Advertising those pending task ids keeps
    # reconnect reconciliation from falsely failing a run in that narrow gap.
    pending_process_exits: list[str] = Field(default_factory=list)
    runtime_ready: bool = True
    runtime_error: str | None = None
    agent_type: Literal["claude", "codex"] = "claude"
    claude_cli_ok: bool = True
    claude_version: str | None = None
    claude_path: str | None = None
    codex_cli_ok: bool | None = None
    codex_version: str | None = None
    codex_path: str | None = None


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


class AccountLoginResultMessage(Message):
    type: Literal["ACCOUNT_LOGIN_RESULT"] = "ACCOUNT_LOGIN_RESULT"
    login_request_id: str = ""
    account_id: str
    slot_index: int
    success: bool
    error: str | None = None


class AccountLoginOtpRequiredMessage(Message):
    """Worker -> Manager: interactive login is waiting for an OTP code."""

    type: Literal["ACCOUNT_LOGIN_OTP_REQUIRED"] = "ACCOUNT_LOGIN_OTP_REQUIRED"
    login_request_id: str = ""
    account_id: str
    challenge_id: str = ""
    expires_at: int = 0


class AccountLoginCancelledMessage(Message):
    """Worker -> Manager: correlated login cleanup has reached a safe point."""

    type: Literal["ACCOUNT_LOGIN_CANCELLED"] = "ACCOUNT_LOGIN_CANCELLED"
    login_request_id: str
    account_id: str
    cleanup_complete: bool = True


class AgentApiConfigureResultMessage(Message):
    """Worker -> Manager: correlated Agent API setup result."""

    type: Literal["AGENT_API_CONFIGURE_RESULT"] = "AGENT_API_CONFIGURE_RESULT"
    request_id: str
    account_id: str
    provider: Literal["cloudrouter"] = "cloudrouter"
    agent_type: Literal["claude", "codex"]
    success: bool
    error: str | None = None
    config_dir: str


class CredentialExhaustedMessage(Message):
    type: Literal["CREDENTIAL_EXHAUSTED"] = "CREDENTIAL_EXHAUSTED"
    worker_id: str
    slot_index: int
    account_id: str
    reason: str


class RunExhaustedMessage(Message):
    """Worker -> Manager: a Mode-B run command tripped the rate-limit detectors.

    The worker has already interrupted the process; the BatchOrchestrator routes
    this to on_worker_exhausted() to swap the account and restart with --resume.
    """

    type: Literal["RUN_EXHAUSTED"] = "RUN_EXHAUSTED"
    task_id: str
    job_id: str
    worker_id: str
    reason: str = "rate_limit"
    event_id: str = Field(default_factory=lambda: uuid.uuid4().hex)


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
        EventAckMessage,
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
        AccountLoginMessage,
        AccountLoginOtpMessage,
        AccountLoginCancelMessage,
        AgentApiConfigureMessage,
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
        AccountLoginResultMessage,
        AccountLoginOtpRequiredMessage,
        AccountLoginCancelledMessage,
        AgentApiConfigureResultMessage,
        CredentialExhaustedMessage,
        RunExhaustedMessage,
        AuthMessage,
    ],
    Field(discriminator="type"),
]

AnyMessage = Annotated[
    Union[
        ExecuteMessage,
        StopMessage,
        EventAckMessage,
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
        AccountLoginMessage,
        AccountLoginOtpMessage,
        AccountLoginCancelMessage,
        AgentApiConfigureMessage,
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
        AccountLoginResultMessage,
        AccountLoginOtpRequiredMessage,
        AccountLoginCancelledMessage,
        AgentApiConfigureResultMessage,
        CredentialExhaustedMessage,
        RunExhaustedMessage,
        AuthMessage,
    ],
    Field(discriminator="type"),
]


# ---------------------------------------------------------------------------
# Parsing helper
# ---------------------------------------------------------------------------


_any_adapter = TypeAdapter(
    AnyMessage,
    config=ConfigDict(hide_input_in_errors=True),
)


def parse_message(data: str | bytes | dict) -> Message:
    """Parse raw JSON into a typed Message instance."""
    if isinstance(data, (str, bytes)):
        return _any_adapter.validate_json(data)
    return _any_adapter.validate_python(data)
