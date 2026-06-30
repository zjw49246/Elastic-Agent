"""PTY-hosted agent execution for the Worker Runtime (claude-pty integration).

Instead of spawning `claude -p ...` per task, the worker hosts Claude Code in
a persistent PTY session (claude_pty.BasePTYBackend). Input goes through
channel injection, output comes from the session JSONL, normalized to
PTYEvents. This module maps those events back onto the existing
Manager<->Worker protocol (LogMessage / ProcessExitMessage), so the Manager
side needs no changes.

claude-pty is an optional dependency: PTY_AVAILABLE gates usage, and the
runtime falls back to subprocess execution when it is missing.

Key shape decisions:
- Events carrying `raw_json` (the original session-JSONL line) are forwarded
  verbatim as stdout lines — the Manager's NDJSON parsers see native Claude
  Code types (system/assistant/user) untouched.
- Interactive-mode JSONL has no `result` line (the turn ends with a
  system/turn_duration sentinel), but the Manager's LogEventParser extracts
  session_id from `result` events — so we synthesize one at turn end.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

try:
    from claude_pty.adapters.base import BasePTYBackend
    from claude_pty.bridge import BridgeHub

    PTY_AVAILABLE = True
except ImportError:  # pragma: no cover - exercised via runtime fallback tests
    BasePTYBackend = object  # type: ignore[assignment,misc]
    BridgeHub = None  # type: ignore[assignment]
    PTY_AVAILABLE = False


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def event_to_log_line(event_dict: dict[str, Any]) -> str | None:
    """Convert a PTYEvent dict to an NDJSON stdout line for the Manager.

    Prefers the raw session-JSONL line (native Claude Code shape). Events
    without raw_json (PTY-internal: session lifecycle, timeouts, errors) are
    wrapped as system events; pure-noise ones return None.
    """
    raw = event_dict.get("raw_json")
    if raw:
        return raw

    event_type = event_dict.get("event_type")
    content = event_dict.get("content")
    is_error = bool(event_dict.get("is_error"))
    # Internal bookkeeping events with neither payload nor error are noise
    if not content and not is_error:
        return None
    return json.dumps({
        "type": "system",
        "subtype": f"pty_{event_type}",
        "content": content,
        "is_error": is_error,
        "session_id": event_dict.get("session_id"),
    }, ensure_ascii=False)


def synthesize_result_line(
    session_id: str | None,
    is_error: bool,
    error_message: str | None = None,
    error_type: str | None = None,
) -> str:
    """Build the `result` event interactive-mode JSONL never emits.

    The Manager's LogEventParser extracts session_id (and marks the turn
    outcome) from `result` events; PTY mode must synthesize it at turn end.
    """
    obj: dict[str, Any] = {
        "type": "result",
        "subtype": "error" if is_error else "success",
        "is_error": is_error,
        "session_id": session_id,
        "synthesized_by": "pty_backend",
    }
    if error_message:
        obj["error"] = error_message
    if error_type:
        obj["error_type"] = error_type
    return json.dumps(obj, ensure_ascii=False)


def classify_turn_error(content: Any | None) -> tuple[str, str]:
    if isinstance(content, str):
        message = content
    elif content is None:
        message = "PTY turn error"
    else:
        try:
            message = json.dumps(content, ensure_ascii=False)
        except TypeError:
            message = str(content)
    lower = message.lower()
    if "response timed out" in lower or "response timeout" in lower:
        return (
            "runtime_timeout",
            f"Worker runtime timed out and interrupted the Claude process ({message})",
        )
    if (
        "rate_limit_event" in lower
        or "usage limit reached" in lower
        or "hit your session limit" in lower
        or '"api_error_status": 429' in lower
        or '"api_error_status":429' in lower
    ):
        return (
            "claude_rate_limited",
            "Claude account usage limit was reached. The worker will pause "
            "until the quota reset time; this task can be continued from the "
            f"latest Claude session after quota recovers. Original error: {message}",
        )
    if "prompt is too long" in lower or "message too long" in lower:
        return (
            "prompt_too_long",
            f"Claude rejected the request because the prompt is too long: {message}",
        )
    return "pty_turn_error", message


if PTY_AVAILABLE:

    class ElasticPTYBackend(BasePTYBackend):
        """Adapter wiring claude-pty sessions into WorkerRuntime's protocol.

        Keyed by process task_id (the `<task>:<hex>` ids the Manager
        generates). One live PTY session per key; resume reuses the warm
        session when the pool still has it.
        """

        def __init__(self, runtime, max_sessions: int = 4, log_dir: str | Path = "logs"):
            # Own BridgeHub so prompts go via channel injection (stdin is
            # only the fallback path inside claude-pty).
            self._bridge = BridgeHub()
            self._bridge.start()
            super().__init__(max_sessions=max_sessions, bridge=self._bridge)
            self._runtime = runtime
            self._log_dir = Path(log_dir)
            self._task_session_ids: dict[str, str] = {}
            self._turn_errors: dict[str, str | None] = {}
            self._turn_error_types: dict[str, str] = {}
            self._saw_result: set[str] = set()
            self._saw_claude_output: set[str] = set()

        def build_config(self, **kwargs):
            """Extend the base config with a per-task response timeout.

            PTYConfig.response_timeout may be shorter than long production
            turns; audiobook builds need the task's own timeout,
            otherwise claude-pty aborts the turn long before the task limit.
            """
            config = super().build_config(**kwargs)
            response_timeout = kwargs.get("response_timeout")
            if response_timeout:
                config.response_timeout = float(response_timeout)
            return config

        def has_task(self, task_id: str) -> bool:
            return task_id in self._sessions

        def mark_task_error(
            self,
            task_id: str,
            error_type: str,
            error_message: str,
        ) -> None:
            """Attach a fatal manager/worker-side error to the PTY turn."""
            self._turn_errors[task_id] = error_message
            self._turn_error_types[task_id] = error_type

        @property
        def active_tasks(self) -> list[str]:
            return list(self._sessions.keys())

        async def _emit_log(self, task_id: str, line: str) -> None:
            from elastic_agent.core.protocols.messages import LogMessage
            from elastic_agent.worker.runtime import WorkerRuntime

            parsed = WorkerRuntime._try_parse_ndjson(line)
            log_path = self._log_dir / f"{task_id}.ndjson"
            try:
                log_path.parent.mkdir(parents=True, exist_ok=True)
                with open(log_path, "a", encoding="utf-8") as f:
                    f.write(json.dumps({
                        "task_id": task_id,
                        "stream": "stdout",
                        "data": line,
                        "timestamp": _utcnow().isoformat(),
                        "parsed": parsed,
                    }, ensure_ascii=False) + "\n")
            except Exception:
                logger.exception("Failed to write PTY log for task %s", task_id)

            await self._runtime._send_event(LogMessage(
                task_id=task_id,
                stream="stdout",
                data=line,
                parsed=parsed,
            ))

        async def on_event(self, key: Any, event_dict: dict, **context) -> None:
            task_id = key
            sid = event_dict.get("session_id")
            if sid:
                self._task_session_ids[task_id] = sid
            # Only session-level errors are turn-fatal: API errors, rate
            # limits, response timeouts (claude-pty emits those as
            # system_event/message/result with is_error). A failed
            # tool_result is normal agent life — the model recovers and the
            # turn can still deliver; it must not poison the synthesized
            # result (a real book once finished 100% but was reported
            # failed because of one mid-run tool error).
            if event_dict.get("is_error") and event_dict.get("event_type") in (
                "system_event", "message", "result",
            ):
                error_type, error_message = classify_turn_error(event_dict.get("content"))
                current_type = self._turn_error_types.get(task_id)
                if current_type is None or (
                    current_type == "pty_turn_error" and error_type == "runtime_timeout"
                ):
                    self._turn_errors[task_id] = error_message
                    self._turn_error_types[task_id] = error_type
            if event_dict.get("event_type") == "result":
                self._saw_result.add(task_id)
            if event_dict.get("raw_json"):
                self._saw_claude_output.add(task_id)

            line = event_to_log_line(event_dict)
            if line is None:
                return
            await self._emit_log(task_id, line)

        async def on_exit(self, key: Any, exit_code: int | None, **context) -> None:
            task_id = key
            error = self._turn_errors.pop(task_id, None)
            error_type = self._turn_error_types.pop(task_id, None)
            ec = exit_code if exit_code is not None else 0
            if error and ec == 0:
                # Errors that end the turn without killing the process
                # (API error sentinel, response timeout) must still surface
                # as a failed run on the Manager side.
                ec = 1
            if ec == 0 and task_id not in self._saw_claude_output:
                # A PTY session can exit cleanly after prompt injection never
                # reaches Claude (for example channel injection fails and the
                # stdin fallback is ignored). Do not report that empty turn as
                # a successful production run.
                error = (
                    "Claude PTY session produced no Claude output; "
                    "prompt injection may have failed"
                )
                error_type = error_type or "no_claude_output"
                ec = 1

            session_id = self._task_session_ids.get(task_id)
            if task_id not in self._saw_result:
                await self._emit_log(task_id, synthesize_result_line(
                    session_id=session_id,
                    is_error=ec != 0,
                    error_message=error,
                    error_type=error_type,
                ))
            self._saw_result.discard(task_id)
            self._saw_claude_output.discard(task_id)
            self._task_session_ids.pop(task_id, None)
            self._sessions.pop(task_id, None)
            self._consumers.pop(task_id, None)

            await self._runtime._on_pty_exit(
                task_id,
                ec,
                session_id=session_id,
                error_type=error_type,
                error_message=error,
            )

        async def recycle_config_dir(self, config_dir: str | None) -> int:
            """Stop every session bound to config_dir. Returns the count.

            Credential rotation swaps the account's credentials *in place*
            (new tokens written into the same config_dir). A warm PTY session
            keeps running under the OLD account it authenticated with at
            spawn — follow-ups injected into it would keep burning the
            exhausted account. Recycle them; the next EXECUTE cold-resumes
            and picks up the new credentials.
            """
            recycled = 0
            # Sessions with an active task (keyed): full stop — interrupt,
            # teardown, pool removal; the consumer's on_exit notifies the
            # Manager via PROCESS_EXIT.
            for key, session in list(self._sessions.items()):
                if session.config.config_dir == config_dir:
                    try:
                        await self.stop(key)
                        recycled += 1
                    except Exception:
                        logger.exception(
                            "Failed to recycle PTY session for task %s", key
                        )
            # Warm sessions whose task already finished live only in the
            # pool — exactly the dangerous ones (idle, old account, waiting
            # to be hot-reused).
            for sid, session in list(self._pool._sessions.items()):
                if session.config.config_dir == config_dir:
                    try:
                        await self._pool.remove(sid)
                        recycled += 1
                    except Exception:
                        logger.exception(
                            "Failed to recycle warm PTY session %s", sid
                        )
            if recycled:
                logger.info(
                    "Recycled %d PTY session(s) for config_dir %s after "
                    "credential swap", recycled, config_dir,
                )
            return recycled

        async def shutdown(self) -> None:
            await super().shutdown()
            self._bridge.stop()
