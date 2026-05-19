"""Platform FastAPI app.

Single-process control plane: ``/health`` (substrate + DuckLake + workspace
Postgres probes), ``/mcp/`` (streamable-HTTP MCP transport behind a bearer
gate), and the DuckLake anchor opened at startup.

This is the only entrypoint inside the container — there is no stdio MCP
and no standalone Starlette MCP runner. ``uvicorn dataraum.server.app:app``
serves everything.

Layout:
    FastAPI app
      ├── BearerAuthMiddleware (bypasses /health)
      ├── @app.get("/health") — substrate probe
      └── mount("/mcp", _mcp_subapp) — streamable-HTTP MCP transport
              └── lifespan chained into ours via lifespan_context

The MCP transport is a Starlette sub-app because the official ``mcp`` SDK's
streamable-HTTP transport is exposed as an ASGI app, not as FastAPI route
handlers. Mounting it is the same shape FastMCP's ``mcp.http_app()`` uses.
"""

from __future__ import annotations

import hmac
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Response
from fastapi.responses import JSONResponse
from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
from sqlalchemy import create_engine, text
from starlette.applications import Starlette
from starlette.routing import Route

from dataraum.core.logging import get_logger
from dataraum.mcp.server import create_server
from dataraum.server.storage import bootstrap_lake, health_probe, teardown_lake

_TOKEN_ENV_VAR = "DATARAUM_MCP_TOKEN"
logger = get_logger(__name__)


class _StreamableHTTPASGIApp:
    """ASGI3 callable that forwards to the lifespan-managed session manager.

    Must be a class (not an async function) because ``starlette.routing.Route``
    auto-wraps coroutine-function endpoints as request/response handlers; a
    callable class is treated as a raw ASGI app, which is what the streamable-
    HTTP transport requires (it controls the response stream itself — SSE for
    GET, JSON-RPC over POST).

    The session manager is built in the sub-app's lifespan (not at module
    load) so ``create_server``'s heavy init does not run on import. Until the
    lifespan has executed, requests receive 503 — in practice this only
    happens if a probe races with shutdown.
    """

    def __init__(self) -> None:
        self.session_manager: StreamableHTTPSessionManager | None = None

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        sm = self.session_manager
        if sm is None:
            response = JSONResponse({"error": "mcp_not_ready"}, status_code=503)
            await response(scope, receive, send)
            return
        await sm.handle_request(scope, receive, send)


def _build_mcp_subapp() -> Starlette:
    """Build the streamable-HTTP MCP transport as a mountable sub-app."""
    asgi_app = _StreamableHTTPASGIApp()

    @asynccontextmanager
    async def lifespan(_app: Starlette) -> AsyncIterator[None]:
        server = create_server()
        sm = StreamableHTTPSessionManager(app=server)
        asgi_app.session_manager = sm
        async with sm.run():
            try:
                yield
            finally:
                asgi_app.session_manager = None

    return Starlette(routes=[Route("/", endpoint=asgi_app)], lifespan=lifespan)


_mcp_subapp = _build_mcp_subapp()


class BearerAuthMiddleware:
    """Pure-ASGI middleware enforcing ``Authorization: Bearer <token>``.

    ``/health`` bypasses auth so liveness probes don't need the secret. Token
    comparison uses ``hmac.compare_digest``. Pure ASGI (not FastAPI's
    ``BaseHTTPMiddleware``) to keep SSE streams from ``/mcp/`` free of
    backpressure pitfalls.

    The token is read from ``DATARAUM_MCP_TOKEN`` at request time so tests can
    flip the env without re-importing the app. The lifespan refuses to start
    when the env is unset, so a live server never sees a missing token here —
    the empty-token branch only matters for misconfigured setups that somehow
    bypass the lifespan check.
    """

    def __init__(self, app: Any) -> None:
        self._app = app

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        if scope["type"] != "http" or scope.get("path") == "/health":
            await self._app(scope, receive, send)
            return

        token = os.environ.get(_TOKEN_ENV_VAR, "")
        auth_header = ""
        for raw_name, raw_value in scope.get("headers", []):
            if raw_name == b"authorization":
                auth_header = raw_value.decode("latin-1")
                break
        scheme, _, presented = auth_header.partition(" ")
        if (
            not token
            or scheme.lower() != "bearer"
            or not presented
            or not hmac.compare_digest(presented, token)
        ):
            response = JSONResponse({"error": "unauthorized"}, status_code=401)
            await response(scope, receive, send)
            return

        await self._app(scope, receive, send)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Open the substrate at startup; chain the MCP sub-app's lifespan into ours.

    Refuses to start if any of ``DATARAUM_MCP_TOKEN``, ``DUCKLAKE_CATALOG_URL``,
    ``DUCKLAKE_DATA_PATH`` is unset — the container substrate (L1+L4) provides
    all three.
    """
    token = os.environ.get(_TOKEN_ENV_VAR)
    if not token:
        raise RuntimeError(
            f"{_TOKEN_ENV_VAR} is unset. The MCP transport refuses to start "
            f"without a bearer token. Set a strong random secret, e.g. "
            f"`export {_TOKEN_ENV_VAR}=$(uuidgen)`."
        )

    catalog_url = os.environ.get("DUCKLAKE_CATALOG_URL")
    data_path = os.environ.get("DUCKLAKE_DATA_PATH")
    if not catalog_url:
        raise RuntimeError(
            "DUCKLAKE_CATALOG_URL is not set. The container substrate (L1) "
            "provides this; for local dev outside the container, export "
            "DUCKLAKE_CATALOG_URL=postgresql://<user>:<pass>@<host>:<port>/<db>."
        )
    if not data_path:
        raise RuntimeError(
            "DUCKLAKE_DATA_PATH is not set. The container substrate (L1) "
            "mounts the dataraum_lake named volume at /var/lib/dataraum/lake/."
        )

    bootstrap_lake(catalog_url, data_path)

    # Chain the sub-app's lifespan inside ours so create_server + session
    # manager init/teardown happen in lockstep with the parent app's lifecycle.
    async with _mcp_subapp.router.lifespan_context(app):
        try:
            yield
        finally:
            teardown_lake()


app = FastAPI(title="DataRaum Control Plane", version="0.2.2", lifespan=lifespan)
app.add_middleware(BearerAuthMiddleware)
app.mount("/mcp", _mcp_subapp)


def _postgres_probe() -> dict[str, str]:
    """Return a /health-shaped dict for the workspace Postgres engine.

    Uses a short-lived engine (no pool reuse) so the probe never wedges on a
    pool-exhausted main connection manager. ``DATABASE_URL`` is the same env
    var the rest of the engine reads at runtime.
    """
    url = os.environ.get("DATABASE_URL")
    if not url:
        return {"status": "not_configured"}
    try:
        engine = create_engine(url, pool_pre_ping=True)
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
        finally:
            engine.dispose()
    except Exception as e:
        # Log the cause but don't expose it in the response body (CodeQL info-
        # exposure pattern). Misconfigured URL schemes (e.g. `postgresql://`
        # instead of `postgresql+psycopg://`) show up here as ModuleNotFoundError.
        logger.warning("postgres_health_probe_failed", error=str(e))
        return {"status": "unreachable"}
    return {"status": "ok"}


@app.get("/health")
def health() -> Response:
    """Substrate + DuckLake catalog + workspace Postgres health.

    Returns 200 with ``status: ok`` when both substrate components are
    reachable; 503 with ``status: degraded`` otherwise so k8s/ECS readiness
    probes that only inspect the status code route traffic away from the
    container instead of seeing a healthy 200 with a degraded body.
    """
    ducklake = health_probe()
    postgres = _postgres_probe()
    healthy = ducklake.get("status") == "ok" and postgres.get("status") == "ok"
    overall = "ok" if healthy else "degraded"
    return JSONResponse(
        {"status": overall, "ducklake": ducklake, "postgres": postgres},
        status_code=200 if healthy else 503,
    )
