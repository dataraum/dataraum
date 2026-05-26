"""Platform Starlette control plane shell.

Hosts ``/health`` (DuckLake catalog + workspace Postgres probes) and three
not-yet-implemented kernel verbs (``/measure``, ``/query``, ``/probe``).
Engine logic for those verbs migrates here phase-by-phase per the DAT-339
pivot.

Lifespan opens the DuckLake substrate, bootstraps the workspace config
overlay, then eagerly initializes the workspace SQLAlchemy substrate so
the ``ws_<id>`` schema + tables materialize before the first request.
Pre-pivot the schema was created lazily on first DB hit, which made
``/health`` race the substrate and surfaced ``DATABASE_URL``
misconfigurations only at runtime.

Run via ``uvicorn dataraum.server.app:app`` or ``docker compose up``.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy import create_engine, text
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from dataraum.core.connections import ConnectionConfig, ConnectionManager
from dataraum.core.logging import get_logger
from dataraum.core.settings import get_settings
from dataraum.server.storage import bootstrap_lake, health_probe, teardown_lake
from dataraum.server.workspace import bootstrap_workspace

logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: Starlette) -> AsyncIterator[None]:
    """Open DuckLake + workspace + SQLAlchemy substrate at startup.

    Calls ``get_settings()`` first, which validates the full env contract
    (DB / DuckLake / workspace / LLM) in one place and raises a
    ``pydantic.ValidationError`` naming any missing var — the container
    substrate provides all of them. This is the single boot-time gate; the
    future Temporal worker entrypoint imports the same ``get_settings()``.

    After this lifespan runs, the ``ws_<workspace_id>`` Postgres schema
    exists with all SQLAlchemy-registered tables, and a process-wide
    :class:`ConnectionManager` lives on ``app.state.workspace_manager``
    for downstream routes.
    """
    settings = get_settings()
    bootstrap_lake(settings.ducklake_catalog_url, str(settings.ducklake_data_path))
    try:
        bootstrap_workspace()

        # Eager substrate init: create the ws_<id> schema + all SQLAlchemy
        # tables before any request can land. Pre-DAT-339-0c this was lazy
        # on first DB hit, which raced /health and hid DATABASE_URL
        # misconfigurations until the first /api/* call.
        workspace_manager = ConnectionManager(ConnectionConfig.for_workspace())
        try:
            workspace_manager.initialize()
            app.state.workspace_manager = workspace_manager
            yield
        finally:
            # close() is documented as safe on partial init.
            workspace_manager.close()
    finally:
        # Always teardown the DuckLake catalog, even if workspace bootstrap
        # or substrate init raised — otherwise a partial-init container
        # leaks the open Postgres connection backing the catalog.
        teardown_lake()


def _postgres_probe() -> dict[str, str]:
    """Return a /health-shaped dict for the workspace Postgres engine.

    Uses a short-lived engine (no pool reuse) so the probe never wedges on a
    pool-exhausted main connection manager. Reads the same typed
    ``database_url`` the rest of the engine uses; boot validation guarantees
    it is present by the time any request lands.
    """
    url = get_settings().database_url
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


async def health(_request: Request) -> JSONResponse:
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


async def _not_implemented(verb: str) -> JSONResponse:
    return JSONResponse(
        {"detail": f"{verb} is not implemented yet (DAT-339 pivot Phase 0c stub)."},
        status_code=501,
    )


async def measure(_request: Request) -> JSONResponse:
    """Measure SSE verb — stub until Phase 2 lands the pipeline-runner SSE."""
    return await _not_implemented("measure")


async def query(_request: Request) -> JSONResponse:
    """Query Arrow verb — stub until Phase 1 lands the read surface."""
    return await _not_implemented("query")


async def probe(_request: Request) -> JSONResponse:
    """Probe read-only SQL verb — stub until Phase 2 lands add_source."""
    return await _not_implemented("probe")


# CORS for the cockpit dev server. Origins are localhost-only by design —
# v1 is single-user and the cockpit runs on http://localhost:3000 (TanStack
# Start default). 5173 covers Vite's default in case the cockpit is run
# with a stock Vite config.
_cors = Middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:5173"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app = Starlette(
    debug=False,
    routes=[
        Route("/health", health, methods=["GET"]),
        Route("/measure", measure, methods=["POST"]),
        Route("/query", query, methods=["POST"]),
        Route("/probe", probe, methods=["POST"]),
    ],
    middleware=[_cors],
    lifespan=lifespan,
)
