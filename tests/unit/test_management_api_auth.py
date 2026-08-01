"""End-to-end management-account authentication and browser-session tests."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from argon2 import PasswordHasher, Type
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient
from httpx import ASGITransport, AsyncClient

from elastic_agent.api import auth
from elastic_agent.api.app import create_app
from elastic_agent.api.routes.management_auth import reset_login_limiter
from elastic_agent.core.management_auth import ManagementUserStore
from elastic_agent.core.protocols.messages import AuthMessage
from elastic_agent.core.registry import NodeRecord
from elastic_agent.testing import create_test_manager

ORIGIN = "https://testserver"
ADMIN_EMAIL = "owner@example.test"
ADMIN_PASSWORD = "temporary-test-passphrase"
TEMP_EMAIL = "temporary-owner@example.test"
TEMP_PASSWORD = "another-temporary-passphrase"
SERVICE_KEY = "test-service-key"


@pytest.fixture
def fast_hasher() -> PasswordHasher:
    return PasswordHasher(
        time_cost=1,
        memory_cost=32,
        parallelism=1,
        hash_len=16,
        salt_len=8,
        type=Type.ID,
    )


@pytest.fixture
async def browser_auth(tmp_path, monkeypatch, fast_hasher):
    monkeypatch.setenv("ELASTIC_AGENT_EXTERNAL_API_KEYS", SERVICE_KEY)
    monkeypatch.setenv("ELASTIC_AGENT_PUBLIC_ORIGIN", ORIGIN)
    auth.reset_api_keys()
    auth.reset_management_auth()
    reset_login_limiter()

    store = ManagementUserStore(
        tmp_path / "private" / "management-users.json",
        password_hasher=fast_hasher,
    )
    store.upsert_user(ADMIN_EMAIL, ADMIN_PASSWORD)
    store.upsert_user(
        TEMP_EMAIL,
        TEMP_PASSWORD,
        must_change_password=True,
    )
    monkeypatch.setattr(auth, "_management_user_store", store)
    monkeypatch.setattr(auth, "_management_sessions", auth.ManagementSessionManager())

    result = create_test_manager()
    app = create_app(result.manager)
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url=ORIGIN,
    ) as client:
        yield SimpleNamespace(client=client, app=app, store=store)

    reset_login_limiter()
    auth.reset_api_keys()
    auth.reset_management_auth()


async def _login(client: AsyncClient, email=ADMIN_EMAIL, password=ADMIN_PASSWORD):
    return await client.post(
        "/api/auth/login",
        headers={"Origin": ORIGIN},
        json={"email": email, "password": password},
    )


@pytest.mark.asyncio
async def test_login_sets_opaque_host_cookie_without_echoing_password(browser_auth):
    response = await _login(browser_auth.client)

    assert response.status_code == 200
    payload = response.json()
    assert payload["email"] == ADMIN_EMAIL
    assert payload["role"] == "admin"
    assert payload["must_change_password"] is False
    assert len(payload["csrf_token"]) >= 32
    assert ADMIN_PASSWORD not in response.text

    cookie = response.headers["set-cookie"]
    lowered = cookie.lower()
    assert cookie.startswith(f"{auth.SESSION_COOKIE_NAME}=")
    assert "httponly" in lowered
    assert "secure" in lowered
    assert "samesite=strict" in lowered
    assert "path=/" in lowered
    token = browser_auth.client.cookies.get(auth.SESSION_COOKIE_NAME)
    assert token
    assert ADMIN_EMAIL not in token
    assert ADMIN_PASSWORD not in token


@pytest.mark.asyncio
async def test_unknown_user_and_wrong_password_are_indistinguishable(browser_auth):
    unknown = await _login(
        browser_auth.client,
        email="missing@example.test",
        password="wrong-test-passphrase",
    )
    wrong = await _login(
        browser_auth.client,
        password="wrong-test-passphrase",
    )

    assert unknown.status_code == wrong.status_code == 401
    assert unknown.json() == wrong.json() == {"detail": "Invalid email or password"}
    assert auth.SESSION_COOKIE_NAME not in unknown.headers.get("set-cookie", "")
    assert auth.SESSION_COOKIE_NAME not in wrong.headers.get("set-cookie", "")


@pytest.mark.asyncio
async def test_invalid_login_payload_never_echoes_password(browser_auth):
    oversized = "sensitive-test-fragment" * 200
    response = await browser_auth.client.post(
        "/api/auth/login",
        headers={"Origin": ORIGIN},
        json={"email": ADMIN_EMAIL, "password": oversized},
    )

    assert response.status_code == 422
    assert oversized not in response.text
    assert "sensitive-test-fragment" not in response.text


@pytest.mark.asyncio
async def test_password_change_rejects_non_printable_value_without_echo(browser_auth):
    login = await _login(browser_auth.client)
    invalid = "hidden-test-fragment\nlong-enough"
    response = await browser_auth.client.post(
        "/api/auth/password",
        headers={
            "Origin": ORIGIN,
            "X-CSRF-Token": login.json()["csrf_token"],
        },
        json={"current_password": ADMIN_PASSWORD, "new_password": invalid},
    )

    assert response.status_code == 422
    assert invalid not in response.text
    assert "hidden-test-fragment" not in response.text


@pytest.mark.asyncio
async def test_wrong_current_password_is_bounded_and_revokes_session(browser_auth):
    login = await _login(browser_auth.client)
    headers = {
        "Origin": ORIGIN,
        "X-CSRF-Token": login.json()["csrf_token"],
    }
    responses = [
        await browser_auth.client.post(
            "/api/auth/password",
            headers=headers,
            json={
                "current_password": "wrong-current-passphrase",
                "new_password": "unused-new-test-passphrase",
            },
        )
        for _ in range(5)
    ]

    assert [response.status_code for response in responses[:4]] == [400] * 4
    assert responses[4].status_code == 401
    assert (await browser_auth.client.get("/api/auth/me")).status_code == 401


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "origin",
    [None, "https://attacker.example", "https://testserver/", "null"],
)
async def test_login_requires_exact_same_origin(browser_auth, origin):
    headers = {} if origin is None else {"Origin": origin}
    response = await browser_auth.client.post(
        "/api/auth/login",
        headers=headers,
        json={"email": ADMIN_EMAIL, "password": ADMIN_PASSWORD},
    )

    assert response.status_code == 403
    assert auth.SESSION_COOKIE_NAME not in response.headers.get("set-cookie", "")


@pytest.mark.asyncio
async def test_session_cookie_authenticates_rest_and_bad_bearer_never_falls_back(
    browser_auth,
):
    assert (await _login(browser_auth.client)).status_code == 200
    assert (await browser_auth.client.get("/api/nodes")).status_code == 200

    wrong = await browser_auth.client.get(
        "/api/nodes",
        headers={"Authorization": "Bearer invalid-service-key"},
    )
    malformed = await browser_auth.client.get(
        "/api/nodes",
        headers={"Authorization": "Basic anything"},
    )
    service = await browser_auth.client.get(
        "/api/nodes",
        headers={"Authorization": f"Bearer {SERVICE_KEY}"},
    )

    assert wrong.status_code == 401
    assert malformed.status_code == 401
    assert service.status_code == 200


@pytest.mark.asyncio
async def test_service_bearer_mutation_does_not_require_browser_csrf(browser_auth):
    response = await browser_auth.client.post(
        "/api/nodes/not-found/drain",
        headers={"Authorization": f"Bearer {SERVICE_KEY}"},
    )

    assert response.status_code != 403


@pytest.mark.asyncio
async def test_cookie_authenticated_mutations_require_origin_and_csrf(browser_auth):
    login = await _login(browser_auth.client)
    csrf = login.json()["csrf_token"]

    missing = await browser_auth.client.post("/api/nodes/not-found/drain")
    bad_origin = await browser_auth.client.post(
        "/api/nodes/not-found/drain",
        headers={"Origin": "https://attacker.example", "X-CSRF-Token": csrf},
    )
    bad_token = await browser_auth.client.post(
        "/api/nodes/not-found/drain",
        headers={"Origin": ORIGIN, "X-CSRF-Token": "wrong"},
    )
    accepted = await browser_auth.client.post(
        "/api/nodes/not-found/drain",
        headers={"Origin": ORIGIN, "X-CSRF-Token": csrf},
    )

    assert missing.status_code == 403
    assert bad_origin.status_code == 403
    assert bad_token.status_code == 403
    assert accepted.status_code != 403


@pytest.mark.asyncio
async def test_logout_revokes_cookie_and_replay_fails(browser_auth):
    login = await _login(browser_auth.client)
    token = browser_auth.client.cookies.get(auth.SESSION_COOKIE_NAME)
    logout = await browser_auth.client.post(
        "/api/auth/logout",
        headers={
            "Origin": ORIGIN,
            "X-CSRF-Token": login.json()["csrf_token"],
        },
    )

    assert logout.status_code == 204
    assert "max-age=0" in logout.headers["set-cookie"].lower()
    async with AsyncClient(
        transport=ASGITransport(app=browser_auth.app),
        base_url=ORIGIN,
        cookies={auth.SESSION_COOKIE_NAME: token},
    ) as replay:
        assert (await replay.get("/api/auth/me")).status_code == 401


@pytest.mark.asyncio
async def test_temporary_password_must_be_changed_and_rotates_all_sessions(browser_auth):
    login = await _login(browser_auth.client, TEMP_EMAIL, TEMP_PASSWORD)
    assert login.status_code == 200
    assert login.json()["must_change_password"] is True
    old_token = browser_auth.client.cookies.get(auth.SESSION_COOKIE_NAME)
    assert (await browser_auth.client.get("/api/nodes")).status_code == 403

    new_password = "new-long-test-passphrase"
    changed = await browser_auth.client.post(
        "/api/auth/password",
        headers={
            "Origin": ORIGIN,
            "X-CSRF-Token": login.json()["csrf_token"],
        },
        json={
            "current_password": TEMP_PASSWORD,
            "new_password": new_password,
        },
    )

    assert changed.status_code == 200
    assert changed.json()["must_change_password"] is False
    assert browser_auth.client.cookies.get(auth.SESSION_COOKIE_NAME) != old_token
    assert (await browser_auth.client.get("/api/nodes")).status_code == 200

    async with AsyncClient(
        transport=ASGITransport(app=browser_auth.app),
        base_url=ORIGIN,
        cookies={auth.SESSION_COOKIE_NAME: old_token},
    ) as replay:
        assert (await replay.get("/api/auth/me")).status_code == 401
        assert (await _login(replay, TEMP_EMAIL, TEMP_PASSWORD)).status_code == 401
        assert (await _login(replay, TEMP_EMAIL, new_password)).status_code == 200


@pytest.mark.asyncio
async def test_concurrent_external_reset_cannot_be_overwritten_by_old_session(
    browser_auth, monkeypatch
):
    login = await _login(browser_auth.client, TEMP_EMAIL, TEMP_PASSWORD)
    original_verify = browser_auth.store.verify_credentials
    external_password = "external-reset-test-passphrase"

    def verify_then_reset(email, password):
        verified = original_verify(email, password)
        if verified is not None:
            browser_auth.store.set_password(email, external_password)
        return verified

    monkeypatch.setattr(browser_auth.store, "verify_credentials", verify_then_reset)
    rejected = await browser_auth.client.post(
        "/api/auth/password",
        headers={
            "Origin": ORIGIN,
            "X-CSRF-Token": login.json()["csrf_token"],
        },
        json={
            "current_password": TEMP_PASSWORD,
            "new_password": "stale-session-test-passphrase",
        },
    )

    assert rejected.status_code == 409
    assert (
        original_verify(TEMP_EMAIL, "stale-session-test-passphrase") is None
    )
    assert original_verify(TEMP_EMAIL, external_password) is not None
    assert (await browser_auth.client.get("/api/auth/me")).status_code == 401


@pytest.mark.asyncio
async def test_login_replaces_caller_supplied_session_cookie(browser_auth):
    browser_auth.client.cookies.set(
        auth.SESSION_COOKIE_NAME,
        "a" * 43,
        domain="testserver.local",
        path="/",
    )
    response = await _login(browser_auth.client)

    assert response.status_code == 200
    assert browser_auth.client.cookies.get(auth.SESSION_COOKIE_NAME) != "a" * 43


@pytest.mark.asyncio
async def test_login_attempts_are_rate_limited_per_identity(browser_auth):
    responses = [
        await _login(browser_auth.client, password="wrong-test-passphrase")
        for _ in range(6)
    ]

    assert [response.status_code for response in responses[:5]] == [401] * 5
    assert responses[5].status_code == 429
    assert int(responses[5].headers["retry-after"]) > 0


@pytest.mark.asyncio
async def test_identity_limit_crosses_sources_but_correct_password_can_recover(
    browser_auth,
):
    for _ in range(5):
        assert (
            await _login(browser_auth.client, password="wrong-test-passphrase")
        ).status_code == 401

    async with AsyncClient(
        transport=ASGITransport(
            app=browser_auth.app,
            client=("198.51.100.19", 43210),
        ),
        base_url=ORIGIN,
    ) as other_source:
        limited = await _login(other_source, password="wrong-test-passphrase")
        recovered = await _login(other_source)

    assert limited.status_code == 429
    assert recovered.status_code == 200


def test_session_manager_enforces_idle_and_absolute_expiry(fast_hasher, tmp_path):
    store = ManagementUserStore(
        tmp_path / "private" / "users.json",
        password_hasher=fast_hasher,
    )
    user = store.upsert_user(ADMIN_EMAIL, ADMIN_PASSWORD)
    now = [1000.0]
    sessions = auth.ManagementSessionManager(
        ttl_seconds=120,
        idle_seconds=60,
        clock=lambda: now[0],
    )
    token, _principal = sessions.create(user)

    assert sessions.authenticate(token, store) is not None
    now[0] += 61
    assert sessions.authenticate(token, store) is None


@pytest.mark.asyncio
async def test_every_management_api_route_has_an_explicit_auth_boundary(browser_auth):
    self_authenticated = {
        "/api/health",
        "/api/auth/login",
        "/api/auth/me",
        "/api/auth/logout",
        "/api/auth/password",
    }
    missing = []
    for route in browser_auth.app.routes:
        if not isinstance(route, APIRoute) or not route.path.startswith("/api/"):
            continue
        if route.path in self_authenticated:
            continue
        if not any(
            dependency.call is auth.require_api_key
            for dependency in route.dependant.dependencies
        ):
            missing.append(route.path)

    assert missing == []


@pytest.mark.asyncio
async def test_worker_websocket_uses_only_its_node_token(tmp_path):
    result = create_test_manager(tmp_dir=tmp_path / "worker-boundary")
    await result.manager.registry.add(
        NodeRecord(
            node_id="worker-auth-boundary",
            instance_id="dryrun:i-auth-boundary",
            platform="dryrun",
            auth_token="worker-only-test-token",
        )
    )
    app = create_app(result.manager)

    with TestClient(app) as client:
        with client.websocket_connect(
            "/ws/runtime",
            headers={"Authorization": f"Bearer {SERVICE_KEY}"},
        ) as websocket:
            websocket.send_text(
                AuthMessage(
                    worker_id="worker-auth-boundary",
                    token="not-the-worker-token",
                ).model_dump_json()
            )
            assert websocket.receive_json()["success"] is False

        with client.websocket_connect(
            "/ws/runtime",
            headers={
                "Authorization": "Bearer invalid-management-token",
                "Cookie": f"{auth.SESSION_COOKIE_NAME}={'x' * 43}",
            },
        ) as websocket:
            websocket.send_text(
                AuthMessage(
                    worker_id="worker-auth-boundary",
                    token="worker-only-test-token",
                ).model_dump_json()
            )
            assert websocket.receive_json()["success"] is True
