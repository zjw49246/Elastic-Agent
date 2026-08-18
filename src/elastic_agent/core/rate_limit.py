"""Rate-limit / auth-failure / transient-overload detection (ported from CCM).

A failed Claude turn falls into classes that demand *opposite* responses, so
they must be told apart precisely:

- **account usage limit / auth failure** → rotate to another account (handled
  Manager-side by QuotaMonitor + CredentialRotator).
- **server-side transient 429 / overload** → retry the SAME account after
  backoff; switching accounts won't help — it's Anthropic infrastructure, not
  your quota. Usage-limit/auth patterns take precedence so a usage-limit banner
  never triggers a same-account retry loop.

The CLI also emits a ``rate_limit_event`` on almost every turn as a routine
quota-status ping; only some are actionable — see
``rate_limit_event_is_actionable`` (used to avoid benching healthy accounts on
benign pings, the CCM #734/#740 pool-starvation bug).

These detectors are deliberately narrow to avoid false positives.
"""

from __future__ import annotations

import json
import random
import re

# "hit your limit" / "hit your session limit" / "hit your weekly limit"…,
# "usage limit reached", "resets 5pm (…)" (any tz, optional minutes),
# org/account disabled, and the Chinese CLI banner.
_RATE_LIMIT_RE = re.compile(
    r"hit your (?:\w+ )?limit"
    r"|usage limit reached"
    r"|session limit reached"
    r"|resets \d{1,2}(?::\d{2})?\s*[ap]m"
    r"|organization has been disabled"
    r"|organization has disabled"
    r"|account has been disabled"
    r"|当前限速",
    re.IGNORECASE,
)

_AUTH_FAIL_RE = re.compile(
    r"not logged in"
    r"|please run /login"
    r"|not authenticated"
    r"|please log in"
    r"|failed to authenticate",
    re.IGNORECASE,
)

# Anthropic's human-readable text for HTTP 429 (rate_limit) / 529 (overloaded)
# that is infrastructure-side, NOT an account usage limit.
_TRANSIENT_OVERLOAD_RE = re.compile(
    r"temporarily limiting requests"
    r"|not your usage limit"
    r"|overloaded_error"
    r"|api overloaded",
    re.IGNORECASE,
)

# Third-party gateway errors are intentionally separate from the generic
# Anthropic detectors. Callers must apply these only after proving that the
# task uses a managed CloudRouter projection.
_CLOUDROUTER_TRANSIENT_RE = re.compile(
    r"^\s*(?:CloudRouter(?:\s+(?:API|gateway))?|API\s+Error|HTTP"
    r"(?:\s+Error)?|status(?:\s+code)?|upstream(?:\s+error)?)"
    r"\s*[:=-]?[^\n]{0,120}(?:\b50[02]\b|upstream[ _-]?error|"
    r"internal[ _-]?server[ _-]?error|overloaded[ _-]?error)\b",
    re.IGNORECASE,
)
_CLOUDROUTER_STRUCTURED_TRANSIENT_RE = re.compile(
    r"\b(?:unexpected\s+status\s+)?(?:500|502)\b"
    r"|upstream[ _-]?error"
    r"|internal[ _-]?server[ _-]?error"
    r"|overloaded[ _-]?error",
    re.IGNORECASE,
)
_CLOUDROUTER_AUTH_RE = re.compile(
    r"^\s*(?:CloudRouter(?:\s+(?:API|gateway))?|API\s+Error|HTTP"
    r"(?:\s+Error)?|status(?:\s+code)?)\s*[:=-]?[^\n]{0,40}"
    r"(?:\b401\b[^\n]{0,100}(?:unauthori[sz]ed|invalid|API[ _-]?key))"
    r"|^\s*(?:error\s*:\s*)?(?:invalid[ _-]?api[ _-]?key"
    r"|API[ _-]?key[^\n]{0,40}invalid)\s*[.!]?\s*$",
    re.IGNORECASE,
)
_CLOUDROUTER_STRUCTURED_AUTH_RE = re.compile(
    r"\b(?:unexpected\s+status\s+)?401\b[^\n]{0,160}"
    r"(?:unauthori[sz]ed|invalid[ _-]?api[ _-]?key)"
    r"|\binvalid[ _-]?api[ _-]?key\b",
    re.IGNORECASE,
)
_CLOUDROUTER_HARD_LIMIT_RE = re.compile(
    r"\b(?:unexpected\s+status\s+|last\s+status\s*:\s*)?(?:403|429)\b"
    r"|API_KEY_RATE_(?:5H|1D|7D)_EXCEEDED"
    r"|quota[ _-]?(?:exhausted|exceeded)"
    r"|too many requests"
    r"|rate[ _-]?limit(?:ed|_error)?",
    re.IGNORECASE,
)
_CLOUDROUTER_HARD_LIMIT_FALLBACK_RE = re.compile(
    r"^\s*(?:CloudRouter(?:\s+(?:API|gateway))?|API\s+Error|HTTP"
    r"(?:\s+Error)?|status(?:\s+code)?|error)\s*[:=-]?"
    r"[^\n]{0,160}(?:\b403\b|\b429\b|too many requests|"
    r"quota[ _-]?(?:exhausted|exceeded)|rate[ _-]?limit)"
    r"|^\s*API_KEY_RATE_(?:5H|1D|7D)_EXCEEDED\b",
    re.IGNORECASE,
)
_CLOUDROUTER_AUTH_ERROR_TYPES = frozenset(
    {
        "authentication_error",
        "authentication_failed",
        "authentication_failure",
        "invalid_api_key",
        "unauthorized",
    }
)
_CLOUDROUTER_TRANSIENT_ERROR_TYPES = frozenset(
    {
        "internal_server_error",
        "overloaded_error",
        "server_error",
        "upstream_error",
    }
)
_CLOUDROUTER_HARD_LIMIT_ERROR_TYPES = frozenset(
    {
        "api_key_rate_5h_exceeded",
        "api_key_rate_1d_exceeded",
        "api_key_rate_7d_exceeded",
        "forbidden",
        "permission_error",
        "quota_exhausted",
        "quota_exceeded",
        "rate_limit_error",
        "rate_limited",
        "too_many_requests",
    }
)

# ApexRouter's 429 is a group-shared concurrency/request condition, so changing
# keys cannot repair it.  Only an explicit per-key quota/credit message is a
# hard limit.  Its 401 and 403 responses both mean the delegated key was
# rejected and must take auth precedence over quota/transient handling.
_APEXROUTER_AUTH_ERROR_TYPES = frozenset(
    {
        "authentication_error",
        "authentication_failed",
        "authentication_failure",
        "forbidden",
        "invalid_api_key",
        "permission_error",
        "unauthorized",
    }
)
_APEXROUTER_TRANSIENT_ERROR_TYPES = frozenset(
    {
        "internal_server_error",
        "overloaded_error",
        "rate_limit_error",
        "rate_limited",
        "server_error",
        "too_many_requests",
        "upstream_error",
    }
)
_APEXROUTER_HARD_LIMIT_ERROR_TYPES = frozenset(
    {
        "billing_hard_limit_reached",
        "credits_exhausted",
        "insufficient_credits",
        "insufficient_quota",
        "out_of_credits",
        "quota_exhausted",
        "quota_exceeded",
        "spend_limit_reached",
    }
)
_APEXROUTER_AUTH_MESSAGE_RE = re.compile(
    r"\b(?:401|403)\b"
    r"|unauthori[sz]ed"
    r"|forbidden"
    r"|invalid[ _-]?api[ _-]?key",
    re.IGNORECASE,
)
_APEXROUTER_TRANSIENT_MESSAGE_RE = re.compile(
    r"\b(?:429|500|502)\b"
    r"|too many requests"
    r"|rate[ _-]?limit(?:ed|_error)?"
    r"|internal[ _-]?server[ _-]?error"
    r"|overloaded[ _-]?error"
    r"|upstream[ _-]?error",
    re.IGNORECASE,
)
_APEXROUTER_HARD_LIMIT_MESSAGE_RE = re.compile(
    r"\binsufficient[ _-]?(?:quota|credits?)\b"
    r"|\bquota[ _-]?(?:exhausted|exceeded)\b"
    r"|\bout of credits?\b"
    r"|\bcredits?[ _-]?exhausted\b"
    r"|\b(?:billing[ _-]?hard|monthly[ _-]?spend|spend)[ _-]?limit"
    r"(?:[ _-]?(?:reached|exceeded))?\b",
    re.IGNORECASE,
)
_APEXROUTER_FALLBACK_PREFIX = (
    r"^\s*[^\n]{0,80}"
    r"(?:ApexRouter(?:\s+(?:API|gateway))?"
    r"|Apex\s+(?:API|gateway)"
    # Keep the retired hostname here so logs emitted by older Workers still
    # receive the same safe classification after a rolling deployment.
    r"|api\.apexin\.ai"
    r"|35-75-22-186\.sslip\.io)"
    r"[^\n]{0,240}"
)
_APEXROUTER_AUTH_FALLBACK_RE = re.compile(
    _APEXROUTER_FALLBACK_PREFIX
    + r"(?:\b401\b|\b403\b|unauthori[sz]ed|forbidden|"
    r"invalid[ _-]?api[ _-]?key)",
    re.IGNORECASE,
)
_APEXROUTER_TRANSIENT_FALLBACK_RE = re.compile(
    _APEXROUTER_FALLBACK_PREFIX
    + r"(?:\b429\b|\b500\b|\b502\b|too many requests|"
    r"rate[ _-]?limit|internal[ _-]?server[ _-]?error|"
    r"overloaded[ _-]?error|upstream[ _-]?error)",
    re.IGNORECASE,
)
_APEXROUTER_HARD_LIMIT_FALLBACK_RE = re.compile(
    _APEXROUTER_FALLBACK_PREFIX
    + r"(?:insufficient[ _-]?(?:quota|credits?)|"
    r"quota[ _-]?(?:exhausted|exceeded)|out of credits?|"
    r"credits?[ _-]?exhausted|"
    r"(?:billing[ _-]?hard|monthly[ _-]?spend|spend)[ _-]?limit)",
    re.IGNORECASE,
)


def _normalise_error_type(value: object) -> str:
    if isinstance(value, bool) or value is None:
        return ""
    return re.sub(r"[\s-]+", "_", str(value).strip().lower())


def _cloudrouter_structured_error(
    text: str,
) -> tuple[frozenset[str], str] | None:
    """Return bounded type/message fields from a known provider-error frame.

    Claude stream-json nests API-error text below ``message.content`` while
    Codex normally puts it below ``error`` or ``result``. Only traverse a
    small allowlist after the outer frame itself has proved error semantics;
    otherwise normal assistant/task output containing error documentation
    could bench a healthy account.
    """

    try:
        event = json.loads(text)
    except (TypeError, ValueError, RecursionError):
        return None
    if not isinstance(event, dict):
        return None
    event_type = str(event.get("type") or "")
    error = event.get("error")
    recognized = (
        event_type in {"error", "turn.failed", "session_crashed"}
        or (
            event_type in {"assistant", "message", "result"}
            and bool(event.get("isApiErrorMessage"))
        )
        or (
            event_type == "result"
            and (
                bool(event.get("is_error"))
                or str(event.get("subtype") or "").lower()
                in {"error", "api_error"}
            )
        )
    )
    if not recognized:
        return None

    error_types: set[str] = set()
    text_parts: list[str] = []
    text_chars = 0

    def add_type(value: object) -> None:
        normalized = _normalise_error_type(value)
        if normalized:
            error_types.add(normalized)

    def add_text(value: object, *, depth: int = 0) -> None:
        nonlocal text_chars
        if depth > 4 or text_chars >= 16_384:
            return
        if isinstance(value, str):
            selected = value[: 16_384 - text_chars]
            if selected:
                text_parts.append(selected)
                text_chars += len(selected)
            return
        if isinstance(value, list):
            for item in value[:32]:
                add_text(item, depth=depth + 1)
            return
        if not isinstance(value, dict):
            return
        for key in (
            "message",
            "detail",
            "text",
            "content",
            "result",
            "error",
            "reason",
        ):
            if key in value:
                add_text(value[key], depth=depth + 1)

    for key in (
        "error_type",
        "code",
        "status",
        "status_code",
        "api_error_status",
    ):
        add_type(event.get(key))
    if isinstance(error, dict):
        for key in (
            "type",
            "code",
            "error_type",
            "status",
            "status_code",
            "api_error_status",
        ):
            add_type(error.get(key))
    elif isinstance(error, (str, int)):
        add_type(error)

    for key in ("error", "message", "content", "result", "detail", "reason"):
        if key in event:
            add_text(event[key])
    return frozenset(error_types), " ".join(text_parts)


def is_cloudrouter_transient(text: str | None) -> bool:
    """Retryable CloudRouter platform/upstream failure on the same key."""

    if not text:
        return False
    structured = _cloudrouter_structured_error(text)
    if structured is not None:
        error_types, message = structured
        return (
            bool(error_types & _CLOUDROUTER_TRANSIENT_ERROR_TYPES)
            or bool(error_types & {"500", "502"})
            or bool(_CLOUDROUTER_STRUCTURED_TRANSIENT_RE.search(message))
        )
    return bool(_CLOUDROUTER_TRANSIENT_RE.search(text))


def is_cloudrouter_auth_failure(text: str | None) -> bool:
    """Gateway key rejection for a proven CloudRouter task."""

    if not text:
        return False
    structured = _cloudrouter_structured_error(text)
    if structured is not None:
        error_types, message = structured
        return (
            bool(error_types & _CLOUDROUTER_AUTH_ERROR_TYPES)
            or "401" in error_types
            or bool(_CLOUDROUTER_STRUCTURED_AUTH_RE.search(message))
        )
    return bool(_CLOUDROUTER_AUTH_RE.search(text))


def is_cloudrouter_hard_limit(text: str | None) -> bool:
    """CloudRouter key/quota/rate condition that needs another account.

    CloudRouter documents 403 as a balance/quota/key-expiry class and 429 as a
    key-window/quota/concurrency class.  When a CLI drops the response body and
    retains only the status, treating these as hard is safer than repeatedly
    hammering the same key.
    """

    if not text:
        return False
    structured = _cloudrouter_structured_error(text)
    if structured is not None:
        error_types, message = structured
        return (
            bool(error_types & _CLOUDROUTER_HARD_LIMIT_ERROR_TYPES)
            or bool(error_types & {"403", "429"})
            or bool(_CLOUDROUTER_HARD_LIMIT_RE.search(message))
        )
    return bool(_CLOUDROUTER_HARD_LIMIT_FALLBACK_RE.search(text))


def is_apexrouter_auth_failure(text: str | None) -> bool:
    """ApexRouter key rejection (both HTTP 401 and 403)."""

    if not text:
        return False
    structured = _cloudrouter_structured_error(text)
    if structured is not None:
        error_types, message = structured
        return (
            bool(error_types & _APEXROUTER_AUTH_ERROR_TYPES)
            or bool(error_types & {"401", "403"})
            or bool(_APEXROUTER_AUTH_MESSAGE_RE.search(message))
        )
    # Some OpenAI-compatible clients retain only the HTTP status and a short
    # reason, dropping the provider prefix and the exact "invalid api key"
    # wording.  Apex treats both 401 and 403 as key rejection, so recognize
    # those status lines directly while keeping arbitrary prose out.
    if re.search(r"^\s*(?:HTTP\s*)?(?:401|403)\b", text, re.IGNORECASE):
        return True
    return bool(_APEXROUTER_AUTH_FALLBACK_RE.search(text))


def is_apexrouter_hard_limit(text: str | None) -> bool:
    """Explicit per-key ApexRouter quota/credit exhaustion.

    A bare 429 is deliberately excluded because ApexRouter reports shared
    group request/concurrency pressure with that status; rotating keys would
    only amplify load against the same group.
    """

    if not text or is_apexrouter_auth_failure(text):
        return False
    structured = _cloudrouter_structured_error(text)
    if structured is not None:
        error_types, message = structured
        return (
            bool(error_types & _APEXROUTER_HARD_LIMIT_ERROR_TYPES)
            or bool(_APEXROUTER_HARD_LIMIT_MESSAGE_RE.search(message))
        )
    return bool(
        _APEXROUTER_HARD_LIMIT_MESSAGE_RE.search(text)
        or _APEXROUTER_HARD_LIMIT_FALLBACK_RE.search(text)
    )


def is_apexrouter_transient(text: str | None) -> bool:
    """Shared ApexRouter 429 or retryable 500/502; keep the same key."""

    if (
        not text
        or is_apexrouter_auth_failure(text)
        or is_apexrouter_hard_limit(text)
    ):
        return False
    structured = _cloudrouter_structured_error(text)
    if structured is not None:
        error_types, message = structured
        return (
            bool(error_types & _APEXROUTER_TRANSIENT_ERROR_TYPES)
            or bool(error_types & {"429", "500", "502"})
            or bool(_APEXROUTER_TRANSIENT_MESSAGE_RE.search(message))
        )
    return bool(
        re.search(r"^\s*(?:HTTP\s*)?(?:429|500|502)\b", text, re.IGNORECASE)
        or _APEXROUTER_TRANSIENT_MESSAGE_RE.search(text)
        or _APEXROUTER_TRANSIENT_FALLBACK_RE.search(text)
    )


def is_rate_limited(text: str | None) -> bool:
    """Account usage-limit / disabled banner → rotate accounts."""
    if not text:
        return False
    return bool(_RATE_LIMIT_RE.search(text))


def is_auth_failure(text: str | None) -> bool:
    """Not-logged-in / auth failure → re-login / rotate."""
    if not text:
        return False
    return bool(_AUTH_FAIL_RE.search(text))


def is_transient_overload(text: str | None) -> bool:
    """Server-side transient 429/overload — wait-and-retry the SAME account.

    Mutually exclusive with usage-limit/auth-failure (which rotate accounts):
    those take precedence, so a usage-limit banner never enters a same-account
    retry loop.
    """
    if not text:
        return False
    if is_rate_limited(text) or is_auth_failure(text):
        return False
    return bool(_TRANSIENT_OVERLOAD_RE.search(text))


def rate_limit_event_is_actionable(
    rate_limit_info: dict | None, *, warn_threshold: float = 0.9
) -> bool:
    """Whether a CLI ``rate_limit_event`` warrants rotating off the account.

    The Claude CLI emits ``rate_limit_event`` on nearly every turn as a routine
    quota-status ping; its ``status`` field is the real signal:

    - ``allowed``         → healthy. **Never** actionable; cooling it down here
                            benches a usable account every turn and starves the
                            pool (CCM #734/#740: a 37%-of-7-day *warning* was
                            benching accounts for 5 min).
    - ``allowed_warning`` → approaching a threshold. Only actionable for the
                            **short** (``five_hour``) window AND when utilization
                            is genuinely high (``>= warn_threshold``). A
                            ``seven_day`` warning is never actionable — a 5-min
                            cooldown can't change a 7-day window.
    - anything else (``rejected``/``blocked``/…) → actionable.
    """
    if not isinstance(rate_limit_info, dict):
        return False
    status = str(rate_limit_info.get("status") or "").lower()
    if status == "allowed":
        return False
    if status == "allowed_warning":
        if rate_limit_info.get("rateLimitType") != "five_hour":
            return False
        util = rate_limit_info.get("utilization")
        if util is None:
            util = rate_limit_info.get("surpassedThreshold")
        try:
            return float(util) >= warn_threshold
        except (TypeError, ValueError):
            return False
    # rejected / blocked / unknown non-"allowed" status → be safe, rotate.
    return True


def transient_retry_delay(attempt: int, base: float, cap: float) -> float:
    """Exponential backoff (with jitter) for transient-overload retries.

    ``attempt`` is 1-based: ``delay = min(base * 2**(attempt-1), cap)``, then
    ±20% jitter so concurrent tasks don't retry in lockstep against the same
    overloaded backend. Always >= 1s.
    """
    attempt = max(1, attempt)
    raw = base * (2 ** (attempt - 1))
    delay = min(raw, cap)
    jitter = delay * 0.2
    return max(1.0, delay + random.uniform(-jitter, jitter))
