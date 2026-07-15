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
