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

import asyncio
import json
import logging
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from elastic_agent.core.rate_limit import (
    is_cloudrouter_auth_failure,
    is_cloudrouter_hard_limit,
    is_cloudrouter_transient,
    is_rate_limited,
    is_transient_overload,
    transient_retry_delay,
)

logger = logging.getLogger(__name__)

_TURN_ERROR_PRIORITY = {
    "pty_turn_error": 0,
    "transient_overload": 1,
    "pty_stopped": 2,
    "prompt_too_long": 2,
    "claude_rate_limited": 3,
    "runtime_timeout": 3,
    "agent_api_rate_limited": 3,
    "agent_api_auth_failure": 4,
}

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


def classify_turn_error(
    content: Any | None,
    *,
    cloudrouter: bool = False,
) -> tuple[str, str]:
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
    if cloudrouter and is_cloudrouter_auth_failure(message):
        return (
            "agent_api_auth_failure",
            "CloudRouter rejected the delegated API key",
        )
    if cloudrouter and is_cloudrouter_hard_limit(message):
        return (
            "agent_api_rate_limited",
            "CloudRouter key quota or rate limit was reached; "
            "the current PTY task cannot continue.",
        )
    if cloudrouter and is_cloudrouter_transient(message):
        return (
            "transient_overload",
            "CloudRouter is temporarily rate limiting gateway requests. "
            "The worker will back off and retry the same account.",
        )
    # Server-side transient 429/overload (Anthropic infra, NOT an account usage
    # limit) → retry the SAME account after backoff. Checked before the
    # usage-limit branch; is_transient_overload already excludes usage-limit /
    # auth banners so those still route to rotation.
    if is_transient_overload(message):
        return (
            "transient_overload",
            "Anthropic API is temporarily overloaded / limiting requests "
            "(infrastructure-side, not an account usage limit). The worker will "
            f"back off and retry the same account. Original error: {message}",
        )
    if (
        is_rate_limited(message)
        or "rate_limit_event" in lower
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

        def __init__(
            self,
            runtime,
            max_sessions: int = 4,
            log_dir: str | Path = "logs",
            transient_retry_max: int = 5,
            transient_retry_base: float = 10.0,
            transient_retry_cap: float = 120.0,
        ):
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
            # Transient-overload same-account retry (P2): remember each task's
            # launch kwargs so a 429/overload turn can be re-run on the same
            # session after backoff instead of failing the task.
            self._launch_kwargs: dict[str, dict] = {}
            self._transient_retries: dict[str, int] = {}
            self._transient_retry_tasks: dict[str, asyncio.Task] = {}
            self._transient_retry_phases: dict[str, str] = {}
            self._stopping_tasks: set[str] = set()
            self._stop_tasks: dict[str, asyncio.Task] = {}
            # Terminal callbacks run in independent tasks.  BasePTYBackend.stop
            # may cancel a slow consumer after 15s; that cancellation must not
            # cancel final sync or durable PROCESS_EXIT persistence.
            self._finalizer_tasks: dict[str, asyncio.Task] = {}
            self._finalized_tasks: set[str] = set()
            self._transient_retry_max = transient_retry_max
            self._transient_retry_base = transient_retry_base
            self._transient_retry_cap = transient_retry_cap

        async def launch(self, **kwargs):
            """Capture launch kwargs (for transient retry) then launch."""
            key = kwargs.get("key")
            if key is not None:
                retry_task = self._transient_retry_tasks.get(key)
                retrying = (
                    retry_task is asyncio.current_task()
                    and self._transient_retry_phases.get(key) == "launching"
                )
                if not retrying and not self.has_task(key):
                    # A task id should normally be unique, but reset completed
                    # synchronization objects if a caller intentionally reuses
                    # one after its terminal hand-off.
                    self._finalizer_tasks.pop(key, None)
                    self._finalized_tasks.discard(key)
                    self._stop_tasks.pop(key, None)
                    self._stopping_tasks.discard(key)
                # Keep only what a re-launch needs; resume_session_id is
                # refreshed from the live session at retry time.
                self._launch_kwargs[key] = dict(kwargs)
            return await super().launch(**kwargs)

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
            config_dir = kwargs.get("config_dir")
            if config_dir:
                from elastic_agent.worker.agent_api import (
                    CLOUDROUTER_CLAUDE_BINARY_ENV,
                    agent_api_marker_for_home,
                    apply_agent_api_runtime_env,
                    claude_wrapper_for_home,
                )

                projection = agent_api_marker_for_home(config_dir)
                if projection is not None:
                    if projection.agent_type != "claude":
                        raise RuntimeError(
                            "PTY requires a Claude Agent API projection"
                        )
                    env = dict(config.env_overrides or {})
                    original_binary = config.claude_binary
                    if not Path(original_binary).is_absolute():
                        original_binary = shutil.which(
                            original_binary,
                            path=env.get("PATH"),
                        )
                    if not original_binary:
                        raise RuntimeError("Claude CLI is unavailable")
                    apply_agent_api_runtime_env(env, projection)
                    env[CLOUDROUTER_CLAUDE_BINARY_ENV] = original_binary
                    config.env_overrides = env
                    # This final exec boundary unsets inherited official auth
                    # again immediately before starting Claude.
                    config.claude_binary = claude_wrapper_for_home(config_dir)
            return config

        def has_task(self, task_id: str) -> bool:
            retry_task = self._transient_retry_tasks.get(task_id)
            finalizer = self._finalizer_tasks.get(task_id)
            return (
                task_id in self._sessions
                or bool(retry_task is not None and not retry_task.done())
                or bool(finalizer is not None and not finalizer.done())
            )

        def mark_task_error(
            self,
            task_id: str,
            error_type: str,
            error_message: str,
        ) -> None:
            """Attach a fatal manager/worker-side error to the PTY turn."""
            self._record_turn_error(task_id, error_type, error_message)

        def _record_turn_error(
            self,
            task_id: str,
            error_type: str,
            error_message: str,
        ) -> None:
            """Keep the most actionable error across a multi-frame turn."""

            current_type = self._turn_error_types.get(task_id)
            if (
                current_type is not None
                and _TURN_ERROR_PRIORITY.get(error_type, 0)
                <= _TURN_ERROR_PRIORITY.get(current_type, 0)
            ):
                return
            self._turn_errors[task_id] = error_message
            self._turn_error_types[task_id] = error_type

        @property
        def active_tasks(self) -> list[str]:
            task_ids = list(self._sessions.keys())
            task_ids.extend(
                task_id
                for task_id, retry_task in self._transient_retry_tasks.items()
                if not retry_task.done() and task_id not in self._sessions
            )
            task_ids.extend(
                task_id
                for task_id, finalizer in self._finalizer_tasks.items()
                if (
                    not finalizer.done()
                    and task_id not in self._sessions
                    and task_id not in task_ids
                )
            )
            return task_ids

        def _clear_task_state(self, task_id: str) -> None:
            """Forget all non-retry-task state after a terminal hand-off."""

            self._turn_errors.pop(task_id, None)
            self._turn_error_types.pop(task_id, None)
            self._saw_result.discard(task_id)
            self._saw_claude_output.discard(task_id)
            self._task_session_ids.pop(task_id, None)
            self._sessions.pop(task_id, None)
            self._consumers.pop(task_id, None)
            self._transient_retries.pop(task_id, None)
            self._transient_retry_phases.pop(task_id, None)
            self._launch_kwargs.pop(task_id, None)

        async def stop(self, key: Any) -> None:
            """Stop an active PTY turn or a task waiting to retry.

            A transient-overload backoff is still an active task from the
            runtime's perspective.  Cancel it explicitly and publish exactly
            one terminal hand-off so STOP, timeouts, and shutdown cannot leave
            a delayed retry capable of resurrecting the task.
            """

            existing = self._stop_tasks.get(key)
            if existing is None or existing.done():
                # Mark synchronously before the implementation's first await so
                # an on_exit racing this STOP cannot schedule another retry.
                self._stopping_tasks.add(key)
                if key not in self._turn_errors:
                    self._turn_errors[key] = (
                        "PTY task stopped during transient retry backoff"
                    )
                    self._turn_error_types[key] = "pty_stopped"
                existing = asyncio.create_task(self._stop_once(key))
                self._stop_tasks[key] = existing
            await asyncio.shield(existing)

        async def _stop_once(self, key: Any) -> None:
            """Serialize one STOP without cancelling an in-flight launch."""

            try:
                retry_task = self._transient_retry_tasks.get(key)
                phase = self._transient_retry_phases.get(key)
                if retry_task is not None and not retry_task.done():
                    if phase == "backoff":
                        # Cancellation is safe before BasePTYBackend.launch has
                        # begun its Session.start transaction.
                        retry_task.cancel()
                    # Once phase=launching, cancellation can orphan the process
                    # between spawn and pool registration. Let launch settle,
                    # then tear down the now-trackable session.
                    await asyncio.gather(retry_task, return_exceptions=True)

                if key in self._sessions:
                    try:
                        await super().stop(key)
                    except Exception:
                        # Teardown is best-effort, but terminal delivery is
                        # mandatory.  Interrupt/session/pool cleanup can fail
                        # after the process has already exited; letting that
                        # exception skip the finalizer leaves the Manager
                        # waiting forever and the task registered as active.
                        # _finalize_task clears the backend registration after
                        # first marking the task exiting and durably handing
                        # off PROCESS_EXIT.
                        logger.exception(
                            "PTY teardown failed for task %s; "
                            "continuing terminal hand-off",
                            key,
                        )

                error = self._turn_errors.get(key)
                error_type = self._turn_error_types.get(key)
                if not error:
                    error = "PTY task stopped"
                    error_type = "pty_stopped"
                session_id = self._task_session_ids.get(key)
                finalizer = self._start_finalizer(
                    key,
                    130,
                    session_id=session_id,
                    error_type=error_type,
                    error_message=error,
                )
                await asyncio.shield(finalizer)
            finally:
                self._stopping_tasks.discard(key)

        def _start_finalizer(
            self,
            task_id: str,
            exit_code: int,
            *,
            session_id: str | None,
            error_type: str | None,
            error_message: str | None,
        ) -> asyncio.Task:
            """Return the one independent terminal hand-off for ``task_id``."""

            existing = self._finalizer_tasks.get(task_id)
            if existing is not None:
                return existing
            finalizer = asyncio.create_task(self._finalize_task(
                task_id,
                exit_code,
                session_id=session_id,
                error_type=error_type,
                error_message=error_message,
            ))
            self._finalizer_tasks[task_id] = finalizer
            return finalizer

        async def _finalize_task(
            self,
            task_id: str,
            exit_code: int,
            *,
            session_id: str | None,
            error_type: str | None,
            error_message: str | None,
        ) -> None:
            """Synthesize output and durably publish exactly one terminal."""

            if task_id in self._finalized_tasks:
                return
            try:
                if task_id not in self._saw_result:
                    await self._emit_log(task_id, synthesize_result_line(
                        session_id=session_id,
                        is_error=exit_code != 0,
                        error_message=error_message,
                        error_type=error_type,
                    ))
                # Publish the terminal hand-off before removing active state.
                await self._runtime._mark_task_exiting(task_id)
                self._clear_task_state(task_id)
                await self._runtime._on_pty_exit(
                    task_id,
                    exit_code,
                    session_id=session_id,
                    error_type=error_type,
                    error_message=error_message,
                )
            finally:
                self._finalized_tasks.add(task_id)

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
            # Orphan events are the PREVIOUS turn's session JSONL replayed on
            # cold-resume; autonomous events belong to a background sub-agent
            # turn, not the foreground one. Neither may drive the foreground
            # turn's session-id capture, error marking, or result/output
            # accounting — otherwise a resumed session re-marks a stale
            # api_error as turn-fatal and a turn that just succeeded is
            # reported failed (recover-then-failed), and an autonomous
            # sub-agent's session_id would clobber the task's own.
            turn_scoped = not event_dict.get("orphan") and not event_dict.get("autonomous")

            sid = event_dict.get("session_id")
            if sid and turn_scoped:
                self._task_session_ids[task_id] = sid
            # Only session-level errors are turn-fatal: API errors, rate
            # limits, response timeouts (claude-pty emits those as
            # system_event/message/result with is_error). A failed
            # tool_result is normal agent life — the model recovers and the
            # turn can still deliver; it must not poison the synthesized
            # result (a real book once finished 100% but was reported
            # failed because of one mid-run tool error).
            if (
                turn_scoped
                and event_dict.get("is_error")
                and event_dict.get("event_type") in ("system_event", "message", "result")
            ):
                projection = self._runtime._agent_api_tasks.get(task_id)
                cloudrouter = bool(
                    projection is not None
                    and projection.provider == "cloudrouter"
                )
                # Claude's real result frame carries api_error_status only in
                # raw_json; claude-pty's normalized content may contain just a
                # short result string. Classify both and let priority retain
                # auth/transient semantics over a generic turn error.
                values = [
                    event_dict.get("raw_json"),
                    event_dict.get("content"),
                ]
                for value in values:
                    if value is None:
                        continue
                    error_type, error_message = classify_turn_error(
                        value,
                        cloudrouter=cloudrouter,
                    )
                    self._record_turn_error(
                        task_id,
                        error_type,
                        error_message,
                    )
            if turn_scoped and event_dict.get("event_type") == "result":
                self._saw_result.add(task_id)
            if turn_scoped and event_dict.get("raw_json"):
                self._saw_claude_output.add(task_id)

            # Orphan events are stale replays already forwarded in a prior
            # turn — dropping them keeps the Manager from double-logging and
            # from parsing an old `result` line as this turn's outcome.
            # Autonomous (sub-agent) output is real and still forwarded.
            if event_dict.get("orphan"):
                return

            line = event_to_log_line(event_dict)
            if line is None:
                return
            await self._emit_log(task_id, line)

        async def on_exit(self, key: Any, exit_code: int | None, **context) -> None:
            task_id = key
            existing_finalizer = self._finalizer_tasks.get(task_id)
            if existing_finalizer is not None:
                await asyncio.shield(existing_finalizer)
                return
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

            # P2: a server-side transient 429/overload turn is retried on the
            # SAME session after backoff instead of failing the task. Don't
            # synthesize a failed result or notify the Manager — the retried
            # turn's own on_exit reports the eventual outcome.
            if (
                task_id not in self._stopping_tasks
                and error_type == "transient_overload"
                and self._schedule_transient_retry(task_id, session_id)
            ):
                return

            finalizer = self._start_finalizer(
                task_id,
                ec,
                session_id=session_id,
                error_type=error_type,
                error_message=error,
            )
            # Shield the durable hand-off from BasePTYBackend.stop's 15-second
            # consumer timeout/cancellation. The independent finalizer remains
            # live even if this consumer is cancelled.
            await asyncio.shield(finalizer)

        def _schedule_transient_retry(self, task_id: str, session_id: str | None) -> bool:
            """Schedule a same-session retry for a transient-overload turn.

            Returns True if a retry was scheduled (the caller must return
            without reporting the turn), False to fall through to failure.
            """
            if task_id not in self._launch_kwargs:
                return False
            if (
                task_id in self._stopping_tasks
                or task_id in self._finalizer_tasks
            ):
                return False
            pending = self._transient_retry_tasks.get(task_id)
            if pending is not None and not pending.done():
                return True
            attempt = self._transient_retries.get(task_id, 0) + 1
            if attempt > self._transient_retry_max:
                logger.warning(
                    "PTY task %s exhausted %d transient-overload retries; failing",
                    task_id, self._transient_retry_max,
                )
                return False
            self._transient_retries[task_id] = attempt
            # Tear down the just-exited turn's per-turn state; keep the retry
            # budget and stored launch kwargs for the re-launch.
            self._turn_errors.pop(task_id, None)
            self._turn_error_types.pop(task_id, None)
            self._saw_result.discard(task_id)
            self._saw_claude_output.discard(task_id)
            self._sessions.pop(task_id, None)
            self._consumers.pop(task_id, None)
            delay = transient_retry_delay(
                attempt, self._transient_retry_base, self._transient_retry_cap
            )
            logger.warning(
                "PTY task %s transient overload — retry %d/%d on same account in %.1fs",
                task_id, attempt, self._transient_retry_max, delay,
            )
            self._transient_retry_tasks[task_id] = asyncio.create_task(
                self._run_transient_retry(task_id, session_id, delay)
            )
            self._transient_retry_phases[task_id] = "backoff"
            return True

        async def _run_transient_retry(
            self, task_id: str, session_id: str | None, delay: float
        ) -> None:
            try:
                await asyncio.sleep(delay)
                if task_id in self._stopping_tasks:
                    return
                kwargs = dict(self._launch_kwargs.get(task_id) or {})
                if not kwargs:
                    return
                # Resume the same session so the retry continues the same
                # context and account.
                kwargs["resume_session_id"] = (
                    session_id or kwargs.get("resume_session_id")
                )
                # From here through launch return the transaction is
                # cancellation-unsafe: Session.start may have spawned Claude
                # before BasePTYBackend registers it in the pool/session maps.
                self._transient_retry_phases[task_id] = "launching"
                await self.launch(**kwargs)
            except asyncio.CancelledError:
                raise
            except Exception:
                if task_id in self._stopping_tasks:
                    return
                logger.exception(
                    "PTY task %s transient retry launch failed; reporting failure",
                    task_id,
                )
                msg = "transient-overload retry failed to relaunch the session"
                finalizer = self._start_finalizer(
                    task_id,
                    1,
                    session_id=session_id,
                    error_type="transient_overload",
                    error_message=msg,
                )
                await asyncio.shield(finalizer)
            finally:
                current = asyncio.current_task()
                if (
                    self._transient_retry_tasks.get(task_id) is current
                ):
                    self._transient_retry_tasks.pop(task_id, None)
                    self._transient_retry_phases.pop(task_id, None)

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
            recycled_keys: set[str] = set()
            # Sessions with an active task (keyed): full stop — interrupt,
            # teardown, pool removal; the consumer's on_exit notifies the
            # Manager via PROCESS_EXIT.
            for key, session in list(self._sessions.items()):
                if session.config.config_dir == config_dir:
                    try:
                        await self.stop(key)
                        recycled += 1
                        recycled_keys.add(key)
                    except Exception:
                        logger.exception(
                            "Failed to recycle PTY session for task %s", key
                        )
            # A task in transient-overload backoff has no live session but is
            # still bound to the old credential directory and must not wake
            # after the account is replaced.
            for key, retry_task in list(self._transient_retry_tasks.items()):
                launch_config_dir = (self._launch_kwargs.get(key) or {}).get(
                    "config_dir"
                )
                if (
                    key not in recycled_keys
                    and not retry_task.done()
                    and launch_config_dir == config_dir
                ):
                    try:
                        await self.stop(key)
                        recycled += 1
                    except Exception:
                        logger.exception(
                            "Failed to recycle pending PTY retry for task %s", key
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
            # Route all active/backoff/launching tasks through the serialized
            # STOP path; direct cancellation can orphan a just-spawned Claude.
            task_ids = list(dict.fromkeys(self.active_tasks))
            if task_ids:
                await asyncio.gather(
                    *(self.stop(task_id) for task_id in task_ids),
                    return_exceptions=True,
                )
            await super().shutdown()
            self._bridge.stop()
