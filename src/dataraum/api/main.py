"""FastAPI application factory."""

import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from dataraum.api import routers
from dataraum.api.routers.pipeline import mark_interrupted_runs
from dataraum.core.connections import (
    ConnectionManager,
    close_default_manager,
    get_connection_manager,
)
from dataraum.core.logging import get_logger

logger = get_logger(__name__)

# Module-level manager reference for lifespan cleanup
_app_manager: ConnectionManager | None = None


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Application lifespan handler.

    Initializes database connections on startup using the shared ConnectionManager.
    Marks any interrupted pipeline runs from previous process.
    FastAPI requires async lifespan, but our connections are sync.
    """
    global _app_manager

    output_dir = getattr(app.state, "output_dir", None)
    if output_dir:
        # Initialize the shared connection manager
        _app_manager = get_connection_manager(output_dir=output_dir)

        # Mark any stale "running" pipelines as interrupted
        interrupted_count = mark_interrupted_runs()
        if interrupted_count > 0:
            logger.warning(
                f"Marked {interrupted_count} pipeline run(s) as interrupted from previous process"
            )

    yield

    # Cleanup
    close_default_manager()
    _app_manager = None


def create_app(
    output_dir: Path | None = None,
    title: str = "DataRaum Context API",
    version: str = "1.0.0",
    cors_origins: list[str] | None = None,
) -> FastAPI:
    """Create and configure the FastAPI application.

    Args:
        output_dir: Directory containing metadata.db and data.duckdb.
                    If None, reads from DATARAUM_OUTPUT_DIR env var.
        title: API title
        version: API version
        cors_origins: Allowed CORS origins (default: allow all)

    Returns:
        Configured FastAPI application
    """
    # Get output_dir from environment if not provided
    if output_dir is None:
        env_dir = os.environ.get("DATARAUM_OUTPUT_DIR")
        if env_dir:
            output_dir = Path(env_dir)

    app = FastAPI(
        title=title,
        version=version,
        description="Rich metadata context engine for AI-driven data analytics",
        lifespan=lifespan,
    )

    # Store output_dir for lifespan handler
    if output_dir:
        app.state.output_dir = output_dir

    # Configure CORS
    if cors_origins is None:
        cors_origins = ["*"]

    app.add_middleware(
        CORSMiddleware,
        allow_origins=cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Include routers
    app.include_router(routers.sources.router, prefix="/api/v1", tags=["sources"])
    app.include_router(routers.pipeline.router, prefix="/api/v1", tags=["pipeline"])
    app.include_router(routers.tables.router, prefix="/api/v1", tags=["tables"])
    app.include_router(routers.context.router, prefix="/api/v1", tags=["context"])
    app.include_router(routers.contracts.router, prefix="/api/v1", tags=["contracts"])
    app.include_router(routers.entropy.router, prefix="/api/v1", tags=["entropy"])
    app.include_router(routers.graphs.router, prefix="/api/v1", tags=["graphs"])
    app.include_router(routers.query.router, prefix="/api/v1", tags=["query"])

    @app.get("/health")
    async def health_check() -> dict[str, str]:
        """Health check endpoint."""
        return {"status": "healthy"}

    return app
