"""FastAPI application factory for Elastic-Agent Manager.

T-016: Manager FastAPI service skeleton — mounts REST routes, WebSocket endpoint,
and manages the lifecycle of the ElasticAgentManager.
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI
from starlette.websockets import WebSocket

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

    from elastic_agent.api.routes.account_login import router as account_login_router
    from elastic_agent.api.routes.accounts import router as accounts_router
    from elastic_agent.api.routes.files import router as files_router
    from elastic_agent.api.routes.health import router as health_router
    from elastic_agent.api.routes.jobs import router as jobs_router
    from elastic_agent.api.routes.nodes import router as nodes_router
    from elastic_agent.api.routes.ui import router as ui_router

    app.include_router(health_router)
    app.include_router(nodes_router, prefix="/api")
    app.include_router(files_router, prefix="/api")
    app.include_router(accounts_router, prefix="/api")
    app.include_router(account_login_router, prefix="/api")
    app.include_router(jobs_router, prefix="/api")
    app.include_router(ui_router)

    @app.websocket("/ws/runtime")
    async def ws_runtime(websocket: WebSocket) -> None:
        await manager.connection_manager.handle_connection(websocket)

    return app
