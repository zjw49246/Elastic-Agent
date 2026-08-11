"""UI v2 — static app shell serving and the lightweight UI summary API.

The v2 frontend lives in ``src/elastic_agent/api/ui_v2/`` as plain ES-module
assets (no build step required for development).  In production canary the same
directory is published to Cloudflare Workers Static Assets and this router is
bypassed entirely; serving it from the Manager keeps local development, tests
and small deployments working without an edge dependency.

Security posture:

* JavaScript/CSS assets are public and contain no data. The SPA shell requires
  an authenticated administrator session and preserves safe deep links through
  the native login/password-change flow.
* ``GET /api/ui/summary`` returns aggregate counts only — no account IDs, no
  emails, no job names, no secrets — and never scans S3.
* Static responses set ``X-Content-Type-Options: nosniff``; ``index.html`` is
  ``no-cache`` so a rollback is picked up immediately.
* Path traversal is prevented by resolving against the asset root and refusing
  anything that escapes it, plus an allowlist of file suffixes.
"""

from __future__ import annotations

import time
from pathlib import Path
from urllib.parse import urlencode

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import FileResponse, RedirectResponse

from elastic_agent.api.auth import get_session_principal, require_api_key

router = APIRouter(tags=["ui-v2"])

UI_V2_ROOT = Path(__file__).resolve().parent.parent / "ui_v2"
UI_V2_ASSET_REVISION = "admin-session-v3"
_REVISIONED_ASSET_PREFIX = f"rev/{UI_V2_ASSET_REVISION}/"

# Only these static types exist in the bundle; anything else 404s.
_ALLOWED_SUFFIXES = {
    ".html": "text/html; charset=utf-8",
    ".css": "text/css; charset=utf-8",
    ".js": "text/javascript; charset=utf-8",
    ".mjs": "text/javascript; charset=utf-8",
    ".svg": "image/svg+xml",
    ".json": "application/json",
    ".map": "application/json",
    ".ico": "image/x-icon",
    ".png": "image/png",
}

_INDEX_HEADERS = {
    "Cache-Control": "no-cache, must-revalidate",
    "X-Content-Type-Options": "nosniff",
    "Referrer-Policy": "no-referrer",
    "Content-Security-Policy": (
        "default-src 'self'; script-src 'self'; style-src 'self'; "
        "connect-src 'self' wss:; img-src 'self' data:; object-src 'none'; "
        "base-uri 'none'; frame-ancestors 'none'; form-action 'self'"
    ),
}

_ASSET_HEADERS = {
    # Dev-served assets are un-hashed, so they must not be cached immutably;
    # the CDN build (scripts/build_ui_v2.py) rewrites to hashed names with
    # long-lived caching instead.
    "Cache-Control": "no-cache",
    "X-Content-Type-Options": "nosniff",
}

_REVISIONED_ASSET_HEADERS = {
    # The revision is changed whenever deployed UI source changes, so every
    # module in the relative-import graph may be cached under this namespace.
    "Cache-Control": "public, max-age=31536000, immutable",
    "X-Content-Type-Options": "nosniff",
}


def _resolve_asset(rel_path: str) -> Path:
    """Resolve a request path inside the UI v2 root, fail closed on escapes."""
    candidate = (UI_V2_ROOT / rel_path).resolve()
    try:
        candidate.relative_to(UI_V2_ROOT.resolve())
    except ValueError:
        raise HTTPException(404, "Not found")
    if not candidate.is_file():
        raise HTTPException(404, "Not found")
    if candidate.suffix not in _ALLOWED_SUFFIXES:
        raise HTTPException(404, "Not found")
    return candidate


async def _serve_index(request: Request) -> FileResponse | RedirectResponse:
    principal = await get_session_principal(request)
    if principal is None:
        return RedirectResponse(
            "/login?" + urlencode({"next": request.url.path}),
            status_code=303,
            headers={"Cache-Control": "no-store"},
        )
    if principal.must_change_password:
        return RedirectResponse(
            "/change-password?" + urlencode({"next": request.url.path}),
            status_code=303,
            headers={"Cache-Control": "no-store"},
        )
    index = UI_V2_ROOT / "index.html"
    if not index.is_file():
        raise HTTPException(404, "UI v2 assets are not installed")
    return FileResponse(index, media_type="text/html", headers=_INDEX_HEADERS)


@router.get("/ui-v2", include_in_schema=False)
@router.get("/ui-v2/", include_in_schema=False)
async def ui_v2_root(request: Request):
    return await _serve_index(request)


@router.get("/ui-v2/{rest:path}", include_in_schema=False)
async def ui_v2_assets(rest: str, request: Request):
    """Serve static assets; unknown paths fall back to the SPA shell.

    The fallback only exists under ``/ui-v2/*`` — ``/api/*`` and ``/ws/*`` are
    mounted before this router and are never claimed by the SPA.
    """
    # A revisioned namespace changes the URL of the entry point and every
    # relative ES-module import together.  This bypasses stale browser/CDN
    # copies even when an edge policy overrides the origin's no-cache header.
    if rest.startswith("rev/"):
        if not rest.startswith(_REVISIONED_ASSET_PREFIX):
            raise HTTPException(404, "Unknown UI asset revision")
        revisioned_path = rest.removeprefix(_REVISIONED_ASSET_PREFIX)
        if "." not in revisioned_path.rsplit("/", 1)[-1]:
            raise HTTPException(404, "Not found")
        asset = _resolve_asset(revisioned_path)
        media_type = _ALLOWED_SUFFIXES[asset.suffix]
        return FileResponse(
            asset,
            media_type=media_type,
            headers=_REVISIONED_ASSET_HEADERS,
        )

    # Explicit unversioned asset paths remain available for local tooling and
    # old open tabs, but the current shell no longer references them.
    if "." in rest.rsplit("/", 1)[-1]:
        asset = _resolve_asset(rest)
        media_type = _ALLOWED_SUFFIXES[asset.suffix]
        return FileResponse(asset, media_type=media_type, headers=_ASSET_HEADERS)
    # Everything else is a client-side route (deep link) — serve the shell.
    return await _serve_index(request)


# --------------------------------------------------------------------------
# Lightweight aggregate summary (Bearer-protected).

_SUMMARY_CACHE: dict = {"at": 0.0, "payload": None}
_SUMMARY_TTL_SECONDS = 5.0


def _mgr():
    from elastic_agent.api.app import get_manager

    return get_manager()


@router.get(
    "/api/ui/summary",
    dependencies=[Depends(require_api_key)],
)
async def ui_summary() -> dict:
    """Aggregate counters for the app shell and overview page.

    Deliberately contains no identifiers (no account IDs/emails, no job
    names), performs no S3 access, and caches for up to 5 seconds so 100
    polling browser tabs cannot amplify into Manager load.
    """
    now = time.monotonic()
    cached = _SUMMARY_CACHE["payload"]
    if cached is not None and now - _SUMMARY_CACHE["at"] < _SUMMARY_TTL_SECONDS:
        return cached

    mgr = _mgr()

    # Jobs: in-memory list only (bounded); historical journals are not
    # rescanned for a badge.
    jobs_active = 0
    jobs_by_state: dict[str, int] = {}
    cleanup_pending = 0
    try:
        for job in mgr.batch.list_jobs():
            summary = job.summary()
            state = str(summary.get("state") or "unknown")
            jobs_by_state[state] = jobs_by_state.get(state, 0) + 1
            if not summary.get("done"):
                jobs_active += 1
            cleanup_pending += int(summary.get("cleanup_pending") or 0)
    except Exception:  # noqa: BLE001 — summary must not 500 on batch quirks
        pass

    # Workers: registry counts + live WS connections.
    nodes = await mgr.registry.list_all()
    workers_by_status: dict[str, int] = {}
    for node in nodes:
        key = getattr(node.status, "value", str(node.status))
        workers_by_status[key] = workers_by_status.get(key, 0) + 1
    connected = len(mgr.connection_manager.connected_workers)

    # Accounts: counts only.
    accounts_total = 0
    accounts_enabled = 0
    try:
        oauth_accounts = await mgr.account_store.list()
        accounts_total += len(oauth_accounts)
        accounts_enabled += sum(1 for a in oauth_accounts if getattr(a, "enabled", True))
    except Exception:  # noqa: BLE001
        pass
    try:
        api_accounts = await mgr.agent_api_store.list()
        accounts_total += len(api_accounts)
        accounts_enabled += sum(1 for a in api_accounts if getattr(a, "enabled", True))
    except Exception:  # noqa: BLE001
        pass

    allocated = 0
    try:
        leases = await mgr.account_binding_store.list_leases(active_only=True)
        allocated = len({lease.account_id for lease in leases})
    except Exception:  # noqa: BLE001
        pass

    otp_pending = 0
    try:
        coordinator = mgr.account_login_coordinator
        if coordinator is not None:
            otp_pending = len(coordinator.list_otp_challenges())
    except Exception:  # noqa: BLE001
        pass

    provider = mgr.config.provider
    region = ""
    if provider.type == "aws":
        region = provider.aws.region
    elif provider.type == "aliyun":
        region = provider.aliyun.region_id

    payload = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "manager": {
            "status": "healthy",
            "provider": provider.type,
            "region": region,
            "uptime_seconds": None,
        },
        "jobs": {
            "active": jobs_active,
            "by_state": jobs_by_state,
            "terminal_total": max(
                sum(jobs_by_state.values()) - jobs_active, 0
            ),
            "cleanup_pending": cleanup_pending,
        },
        "workers": {
            "total": len(nodes),
            "connected": connected,
            "by_status": workers_by_status,
        },
        "accounts": {
            "total": accounts_total,
            "enabled": accounts_enabled,
            "allocated": allocated,
        },
        "otp": {"pending": otp_pending},
    }
    _SUMMARY_CACHE["payload"] = payload
    _SUMMARY_CACHE["at"] = now
    return payload


def reset_summary_cache() -> None:
    """Test hook."""
    _SUMMARY_CACHE["payload"] = None
    _SUMMARY_CACHE["at"] = 0.0
