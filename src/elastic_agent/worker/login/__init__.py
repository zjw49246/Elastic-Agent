"""Worker-side Claude account auto-login (vendored from CCM).

CCM's `scripts/auto_login.py` + `scripts/cdp_login.py` evolved a robust,
worker-local login flow: Chrome CDP drives the OAuth authorize call directly
(no Playwright/mitmproxy), with multi-backend magic-link 接码 (171mail API for
most domains, a mail relay / mail.com web-login for mail.com-family accounts,
auto-selected by email domain). It runs entirely on the machine that needs the
credentials, so the worker logs itself in — the Manager only distributes the
account (email + 接码 token).

These modules are vendored near-verbatim so improvements track CCM's proven
version; only deployment-specific hardcodes were parameterised via env vars
(`CLAUDE_MAILCATCHER_URL`, `CLAUDE_171MAIL_URL`, `CLAUDE_SETTINGS_EXTRA_DIRS`).

Entry points:
- `perform_login(email=, token_171=, config_dir=, provider=None)` — in-process.
- `python -m elastic_agent.worker.login.auto_login --email .. --token .. --config-dir ..`
  — the CLI the Manager runs on the worker over SSH (Xvfb + this command).
"""

from __future__ import annotations

from elastic_agent.worker.login.auto_login import (
    is_mailcom_domain,
    perform_login,
)

__all__ = ["perform_login", "is_mailcom_domain"]
