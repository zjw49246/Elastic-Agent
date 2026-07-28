"""Regression tests for the API-wide bounded request-body reader."""

from __future__ import annotations

import asyncio

import pytest
from fastapi import FastAPI, Request
from httpx import ASGITransport, AsyncClient

from elastic_agent.api.body_limit import (
    JOB_SUBMIT_MAX_BODY_BYTES,
    REQUEST_BODY_MEMORY_MULTIPLIER,
    RequestBodyLimitMiddleware,
    configured_max_aggregate_request_body_bytes,
    configured_max_concurrent_request_bodies,
    configured_request_body_read_timeout_seconds,
)


def _app(*, max_bytes: int, **middleware_options) -> FastAPI:
    app = FastAPI()
    app.add_middleware(
        RequestBodyLimitMiddleware,
        max_bytes=max_bytes,
        **middleware_options,
    )

    @app.post("/size")
    async def size(request: Request) -> dict[str, int]:
        return {"size": len(await request.body())}

    return app


def _status(sent: list[dict]) -> int:
    return next(message["status"] for message in sent if message["type"] == "http.response.start")


def _collector(target: list[dict]):
    async def send(message: dict) -> None:
        target.append(message)

    return send


async def _one_body(body: bytes, *, more_body: bool = False) -> dict:
    return {
        "type": "http.request",
        "body": body,
        "more_body": more_body,
    }


@pytest.mark.asyncio
async def test_rejects_oversized_content_length_before_route() -> None:
    async with AsyncClient(
        transport=ASGITransport(app=_app(max_bytes=8)),
        base_url="http://test",
    ) as client:
        response = await client.post("/size", content=b"123456789")

    assert response.status_code == 413
    assert response.headers["cache-control"] == "no-store"
    assert "9" not in response.text


@pytest.mark.asyncio
async def test_rejects_oversized_chunked_body_without_content_length() -> None:
    async def chunks():
        yield b"12345"
        yield b"67890"

    async with AsyncClient(
        transport=ASGITransport(app=_app(max_bytes=8)),
        base_url="http://test",
    ) as client:
        response = await client.post("/size", content=chunks())

    assert response.status_code == 413


@pytest.mark.asyncio
async def test_jobs_endpoint_keeps_16mib_limit_when_global_limit_is_64mib() -> None:
    app = FastAPI()
    app.add_middleware(
        RequestBodyLimitMiddleware,
        max_bytes=64 * 1024 * 1024,
    )

    @app.post("/api/jobs")
    async def jobs(request: Request) -> dict[str, int]:
        return {"size": len(await request.body())}

    read_past_limit = False

    async def chunks():
        nonlocal read_past_limit
        yield b"x" * (8 * 1024 * 1024)
        yield b"x" * (8 * 1024 * 1024)
        yield b"x"
        read_past_limit = True
        yield b"must-not-be-read"

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        response = await client.post("/api/jobs", content=chunks())

    assert response.status_code == 413
    assert str(JOB_SUBMIT_MAX_BODY_BYTES) in response.text
    assert read_past_limit is False


@pytest.mark.asyncio
async def test_accepts_body_at_exact_limit() -> None:
    async with AsyncClient(
        transport=ASGITransport(app=_app(max_bytes=8)),
        base_url="http://test",
    ) as client:
        response = await client.post("/size", content=b"12345678")

    assert response.status_code == 200
    assert response.json() == {"size": 8}


@pytest.mark.asyncio
async def test_many_tiny_chunks_are_replayed_as_one_bounded_message() -> None:
    received: list[dict] = []

    async def downstream(scope, receive, send) -> None:
        received.append(await receive())
        await send(
            {
                "type": "http.response.start",
                "status": 204,
                "headers": [],
            }
        )
        await send({"type": "http.response.body", "body": b""})

    messages = [
        {
            "type": "http.request",
            "body": b"x",
            "more_body": index < 4_095,
        }
        for index in range(4_096)
    ]

    async def receive() -> dict:
        return messages.pop(0)

    sent: list[dict] = []

    async def send(message: dict) -> None:
        sent.append(message)

    middleware = RequestBodyLimitMiddleware(downstream, max_bytes=4_096)
    await middleware(
        {"type": "http", "headers": []},
        receive,
        send,
    )

    assert received == [
        {
            "type": "http.request",
            "body": b"x" * 4_096,
            "more_body": False,
        }
    ]
    assert sent[0]["status"] == 204


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["GET", "HEAD", "OPTIONS", "TRACE"])
async def test_bodyless_methods_bypass_pre_read_and_admission(
    method: str,
) -> None:
    receive_calls = 0
    downstream_calls = 0

    async def receive() -> dict:
        nonlocal receive_calls
        receive_calls += 1
        raise AssertionError("bodyless request was eagerly read")

    async def downstream(scope, downstream_receive, send) -> None:
        nonlocal downstream_calls
        downstream_calls += 1
        assert downstream_receive is receive
        await send(
            {
                "type": "http.response.start",
                "status": 204,
                "headers": [],
            }
        )
        await send({"type": "http.response.body", "body": b""})

    sent: list[dict] = []

    async def send(message: dict) -> None:
        sent.append(message)

    middleware = RequestBodyLimitMiddleware(
        downstream,
        max_bytes=8,
        max_concurrent_bodies=1,
        max_aggregate_body_bytes=24,
    )
    await middleware(
        {
            "type": "http",
            "method": method,
            "headers": [],
        },
        receive,
        send,
    )

    assert _status(sent) == 204
    assert downstream_calls == 1
    assert receive_calls == 0
    assert middleware._budget.active_requests == 0
    assert middleware._budget.reserved_bytes == 0


@pytest.mark.asyncio
async def test_slow_chunks_share_one_strict_total_deadline() -> None:
    calls = 0
    downstream_called = False

    async def receive() -> dict:
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.03)
        return {
            "type": "http.request",
            "body": b"x",
            "more_body": True,
        }

    async def downstream(_scope, _receive, _send) -> None:
        nonlocal downstream_called
        downstream_called = True

    sent: list[dict] = []

    async def send(message: dict) -> None:
        sent.append(message)

    middleware = RequestBodyLimitMiddleware(
        downstream,
        max_bytes=8,
        read_timeout_seconds=0.05,
        max_concurrent_bodies=1,
        max_aggregate_body_bytes=24,
    )
    await middleware(
        {"type": "http", "method": "POST", "headers": []},
        receive,
        send,
    )

    assert _status(sent) == 408
    assert calls == 2
    assert downstream_called is False
    assert middleware._budget.active_requests == 0
    assert middleware._budget.reserved_bytes == 0


@pytest.mark.asyncio
async def test_admission_is_fail_fast_and_held_through_downstream() -> None:
    downstream_entered = asyncio.Event()
    release_downstream = asyncio.Event()

    async def downstream(scope, receive, send) -> None:
        if scope["method"] == "GET":
            await send(
                {
                    "type": "http.response.start",
                    "status": 204,
                    "headers": [],
                }
            )
            await send({"type": "http.response.body", "body": b""})
            return
        message = await receive()
        assert message["body"] == b"1234"
        downstream_entered.set()
        await release_downstream.wait()
        await send(
            {
                "type": "http.response.start",
                "status": 204,
                "headers": [],
            }
        )
        await send({"type": "http.response.body", "body": b""})

    middleware = RequestBodyLimitMiddleware(
        downstream,
        max_bytes=8,
        max_concurrent_bodies=1,
        max_aggregate_body_bytes=24,
    )
    first_sent: list[dict] = []

    async def first_send(message: dict) -> None:
        first_sent.append(message)

    first = asyncio.create_task(
        middleware(
            {
                "type": "http",
                "method": "POST",
                "headers": [(b"content-length", b"4")],
            },
            lambda: _one_body(b"1234"),
            first_send,
        )
    )
    await downstream_entered.wait()

    saturated_receive_called = False

    async def saturated_receive() -> dict:
        nonlocal saturated_receive_called
        saturated_receive_called = True
        return await _one_body(b"x")

    saturated_sent: list[dict] = []
    await asyncio.wait_for(
        middleware(
            {
                "type": "http",
                "method": "POST",
                "headers": [(b"content-length", b"1")],
            },
            saturated_receive,
            _collector(saturated_sent),
        ),
        timeout=0.1,
    )
    assert _status(saturated_sent) == 503
    assert saturated_receive_called is False

    # Read-only traffic bypasses admission even while the body request remains
    # inside the downstream JSON/application lifecycle.
    bodyless_sent: list[dict] = []
    await middleware(
        {"type": "http", "method": "GET", "headers": []},
        saturated_receive,
        _collector(bodyless_sent),
    )
    assert _status(bodyless_sent) == 204
    assert saturated_receive_called is False

    release_downstream.set()
    await first
    assert _status(first_sent) == 204
    assert middleware._budget.active_requests == 0
    assert middleware._budget.reserved_bytes == 0


@pytest.mark.asyncio
async def test_aggregate_budget_rejects_before_read_and_releases() -> None:
    entered = asyncio.Event()
    release = asyncio.Event()

    async def downstream(_scope, receive, send) -> None:
        assert (await receive())["body"] == b"12345678"
        # The conservative three-copy reservation remains live after replay.
        assert middleware._budget.reserved_bytes == (REQUEST_BODY_MEMORY_MULTIPLIER * 8)
        entered.set()
        await release.wait()
        await send(
            {
                "type": "http.response.start",
                "status": 204,
                "headers": [],
            }
        )
        await send({"type": "http.response.body", "body": b""})

    middleware = RequestBodyLimitMiddleware(
        downstream,
        max_bytes=8,
        max_concurrent_bodies=2,
        max_aggregate_body_bytes=24,
    )
    first_sent: list[dict] = []
    first = asyncio.create_task(
        middleware(
            {
                "type": "http",
                "method": "POST",
                "headers": [(b"content-length", b"8")],
            },
            lambda: _one_body(b"12345678"),
            _collector(first_sent),
        )
    )
    await entered.wait()

    second_receive_called = False

    async def second_receive() -> dict:
        nonlocal second_receive_called
        second_receive_called = True
        return await _one_body(b"abcdefgh")

    second_sent: list[dict] = []
    await middleware(
        {
            "type": "http",
            "method": "POST",
            "headers": [(b"content-length", b"8")],
        },
        second_receive,
        _collector(second_sent),
    )

    assert _status(second_sent) == 503
    assert second_receive_called is False
    release.set()
    await first
    assert middleware._budget.active_requests == 0
    assert middleware._budget.reserved_bytes == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["disconnect", "oversized", "timeout"])
async def test_prebuffer_failure_paths_release_every_budget(
    failure: str,
) -> None:
    downstream_calls = 0

    async def downstream(_scope, receive, send) -> None:
        nonlocal downstream_calls
        downstream_calls += 1
        await receive()
        await send(
            {
                "type": "http.response.start",
                "status": 204,
                "headers": [],
            }
        )
        await send({"type": "http.response.body", "body": b""})

    middleware = RequestBodyLimitMiddleware(
        downstream,
        max_bytes=8,
        read_timeout_seconds=0.02,
        max_concurrent_bodies=1,
        max_aggregate_body_bytes=24,
    )
    sent: list[dict] = []

    if failure == "disconnect":

        async def failing_receive() -> dict:
            return {"type": "http.disconnect"}
    elif failure == "oversized":

        async def failing_receive() -> dict:
            return await _one_body(b"123456789")
    else:

        async def failing_receive() -> dict:
            await asyncio.Future()

    await middleware(
        {"type": "http", "method": "POST", "headers": []},
        failing_receive,
        _collector(sent),
    )

    assert middleware._budget.active_requests == 0
    assert middleware._budget.reserved_bytes == 0
    if failure == "disconnect":
        assert sent == []
    elif failure == "oversized":
        assert _status(sent) == 413
    else:
        assert _status(sent) == 408

    # The sole permit and all bytes are immediately reusable.
    retry_sent: list[dict] = []
    await middleware(
        {
            "type": "http",
            "method": "POST",
            "headers": [(b"content-length", b"1")],
        },
        lambda: _one_body(b"x"),
        _collector(retry_sent),
    )
    assert _status(retry_sent) == 204
    assert downstream_calls == 1


@pytest.mark.asyncio
async def test_downstream_exception_and_cancellation_release_permit() -> None:
    mode = "error"
    downstream_entered = asyncio.Event()

    async def downstream(_scope, receive, send) -> None:
        await receive()
        if mode == "error":
            raise RuntimeError("route failed")
        if mode == "block":
            downstream_entered.set()
            await asyncio.Future()
        await send(
            {
                "type": "http.response.start",
                "status": 204,
                "headers": [],
            }
        )
        await send({"type": "http.response.body", "body": b""})

    middleware = RequestBodyLimitMiddleware(
        downstream,
        max_bytes=8,
        max_concurrent_bodies=1,
        max_aggregate_body_bytes=24,
    )
    scope = {
        "type": "http",
        "method": "POST",
        "headers": [(b"content-length", b"1")],
    }

    with pytest.raises(RuntimeError, match="route failed"):
        await middleware(scope, lambda: _one_body(b"x"), _collector([]))
    assert middleware._budget.active_requests == 0
    assert middleware._budget.reserved_bytes == 0

    mode = "block"
    blocked = asyncio.create_task(
        middleware(
            scope,
            lambda: _one_body(b"x"),
            _collector([]),
        )
    )
    await downstream_entered.wait()
    blocked.cancel()
    with pytest.raises(asyncio.CancelledError):
        await blocked
    assert middleware._budget.active_requests == 0
    assert middleware._budget.reserved_bytes == 0

    mode = "success"
    sent: list[dict] = []
    await middleware(scope, lambda: _one_body(b"x"), _collector(sent))
    assert _status(sent) == 204


@pytest.mark.parametrize(
    ("name", "value", "loader"),
    [
        (
            "ELASTIC_AGENT_REQUEST_BODY_READ_TIMEOUT_SECONDS",
            "0",
            configured_request_body_read_timeout_seconds,
        ),
        (
            "ELASTIC_AGENT_REQUEST_BODY_READ_TIMEOUT_SECONDS",
            "nan",
            configured_request_body_read_timeout_seconds,
        ),
        (
            "ELASTIC_AGENT_MAX_CONCURRENT_REQUEST_BODIES",
            "0",
            configured_max_concurrent_request_bodies,
        ),
    ],
)
def test_new_body_limit_configuration_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
    name: str,
    value: str,
    loader,
) -> None:
    monkeypatch.setenv(name, value)

    with pytest.raises(ValueError, match=name):
        loader()


def test_aggregate_configuration_must_cover_three_body_copies(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        "ELASTIC_AGENT_MAX_AGGREGATE_REQUEST_BODY_BYTES",
        str(1024 * 1024),
    )

    with pytest.raises(ValueError, match="at least 3 times"):
        configured_max_aggregate_request_body_bytes(
            max_request_body_bytes=1024 * 1024,
        )
