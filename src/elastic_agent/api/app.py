"""FastAPI application factory for Elastic-Agent Manager.

T-016: Manager FastAPI service skeleton — mounts REST routes, WebSocket endpoint,
and manages the lifecycle of the ElasticAgentManager.
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.websockets import WebSocket

from elastic_agent.api.body_limit import (
    RequestBodyLimitMiddleware,
    configured_max_aggregate_request_body_bytes,
    configured_max_concurrent_request_bodies,
    configured_max_request_body_bytes,
    configured_request_body_read_timeout_seconds,
)
from elastic_agent.manager.manager import ElasticAgentManager

logger = logging.getLogger(__name__)

_manager_instance: ElasticAgentManager | None = None


def get_manager() -> ElasticAgentManager:
    """Return the singleton Manager instance. Raises if not started."""
    if _manager_instance is None:
        raise RuntimeError("ElasticAgentManager not initialised — call create_app() first")
    return _manager_instance


def create_app(manager: ElasticAgentManager) -> FastAPI:
    """Build the FastAPI application with all routes and the WS endpoint."""
    global _manager_instance
    _manager_instance = manager

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        await manager.start()
        logger.info("Elastic-Agent API server started")
        yield
        await manager.stop()
        logger.info("Elastic-Agent API server stopped")

    app = FastAPI(
        title="Elastic-Agent Manager",
        version="0.1.0",
        lifespan=lifespan,
    )
    max_request_body_bytes = configured_max_request_body_bytes()
    app.add_middleware(
        RequestBodyLimitMiddleware,
        max_bytes=max_request_body_bytes,
        read_timeout_seconds=(configured_request_body_read_timeout_seconds()),
        max_concurrent_bodies=(configured_max_concurrent_request_bodies()),
        max_aggregate_body_bytes=(
            configured_max_aggregate_request_body_bytes(
                max_request_body_bytes=max_request_body_bytes,
            )
        ),
    )

    @app.exception_handler(RequestValidationError)
    async def safe_request_validation_error(
        _request: Request,
        exc: RequestValidationError,
    ) -> JSONResponse:
        """Return useful validation locations without reflecting request data.

        FastAPI's default 422 body includes Pydantic's ``input`` member.  That
        can echo malformed write-only passwords, mailbox tokens, OTPs, or Job
        environment secrets.  Type/location/message are sufficient for API
        clients and deliberately omit both the rejected input and validator
        context.
        """

        errors = [
            {key: value for key, value in error.items() if key in {"type", "loc", "msg", "url"}}
            for error in exc.errors()
        ]
        return JSONResponse(
            status_code=422,
            content=jsonable_encoder({"detail": errors}),
        )

    from elastic_agent.api.routes.account_login import router as account_login_router
    from elastic_agent.api.routes.accounts import router as accounts_router
    from elastic_agent.api.routes.agent_api_accounts import (
        router as agent_api_accounts_router,
    )
    from elastic_agent.api.routes.files import router as files_router
    from elastic_agent.api.routes.health import router as health_router
    from elastic_agent.api.routes.job_batches import router as job_batches_router
    from elastic_agent.api.routes.jobs import router as jobs_router
    from elastic_agent.api.routes.management_auth import (
        router as management_auth_router,
    )
    from elastic_agent.api.routes.nodes import router as nodes_router
    from elastic_agent.api.routes.ui import router as ui_router
    from elastic_agent.api.routes.ui_v2 import router as ui_v2_router

    app.include_router(health_router)
    app.include_router(management_auth_router, prefix="/api")
    app.include_router(nodes_router, prefix="/api")
    app.include_router(files_router, prefix="/api")
    app.include_router(accounts_router, prefix="/api")
    app.include_router(agent_api_accounts_router, prefix="/api")
    app.include_router(account_login_router, prefix="/api")
    app.include_router(jobs_router, prefix="/api")
    app.include_router(job_batches_router, prefix="/api")
    # ui_v2 mounts /api/ui/summary plus the /ui-v2/* static shell; API routes
    # are registered above so the SPA fallback can never shadow /api or /ws.
    app.include_router(ui_v2_router)
    app.include_router(ui_router)

    @app.websocket("/ws/runtime")
    async def ws_runtime(websocket: WebSocket) -> None:
        await manager.connection_manager.handle_connection(websocket)

    return app
