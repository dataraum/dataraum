"""Smoke test for the session-scoped Postgres fixture (DAT-321 phase 2).

The fixture must boot a Postgres 17 container, expose a working psycopg URL,
and let TRUNCATE-based per-test cleanup run without error. Removed by phase 5
once the lane smoke covers the same ground.
"""

from __future__ import annotations

from sqlalchemy import create_engine, text


def test_pg_url_resolves_to_live_postgres(pg_url: str) -> None:
    """pg_url is a working psycopg URL against a live Postgres 17 container."""
    assert pg_url.startswith("postgresql+psycopg://")
    engine = create_engine(pg_url, future=True)
    try:
        with engine.connect() as conn:
            version = conn.execute(text("SHOW server_version_num")).scalar_one()
        assert int(version) >= 170000, f"Expected Postgres ≥ 17.0, got {version}"
    finally:
        engine.dispose()


def test_pg_url_clean_runs_without_error(pg_url_clean: str) -> None:
    """pg_url_clean truncates an empty schema as a no-op without error."""
    assert pg_url_clean == pg_url_clean  # idempotent — just exercises the fixture
