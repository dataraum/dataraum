"""Platform FastAPI app.

L1 placeholder + L4 DuckLake bootstrap: exposes ``/health`` (substrate +
DuckLake catalog status + workspace Postgres status) and opens the
process-wide DuckLake anchor at startup. Real subsystems (sessions,
sources, REST API) are wired in by later lanes.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from sqlalchemy import create_engine, text

from dataraum.server.storage import bootstrap_lake, health_probe, teardown_lake


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    """Open the DuckLake anchor at startup; close it at shutdown.

    Refuses to start if ``DUCKLAKE_CATALOG_URL`` or ``DUCKLAKE_DATA_PATH``
    is unset — the container substrate (L1) is responsible for both.
    """
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
    try:
        yield
    finally:
        teardown_lake()


app = FastAPI(title="DataRaum Control Plane", version="0.2.2", lifespan=lifespan)


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
    except Exception:
        return {"status": "unreachable"}
    return {"status": "ok"}


@app.get("/health")
def health() -> dict[str, object]:
    """Substrate + DuckLake catalog + workspace Postgres health.

    Returns ``status: ok`` overall only when both Postgres and DuckLake are
    reachable. Container orchestrators use this for readiness.
    """
    ducklake = health_probe()
    postgres = _postgres_probe()
    overall = (
        "ok" if ducklake.get("status") == "ok" and postgres.get("status") == "ok" else "degraded"
    )
    return {"status": overall, "ducklake": ducklake, "postgres": postgres}
