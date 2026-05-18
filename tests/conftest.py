"""Shared pytest fixtures for all tests."""

from __future__ import annotations

from collections.abc import Generator

import duckdb
import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker
from testcontainers.postgres import PostgresContainer

from dataraum.storage import init_database


@pytest.fixture(scope="function")
def engine() -> Engine:
    """Create an in-memory SQLite engine for testing.

    Creates a fresh database for each test function.
    """
    test_engine = create_engine(
        "sqlite:///:memory:",
        echo=False,
        future=True,
    )

    # Enable foreign keys for SQLite
    @event.listens_for(test_engine, "connect")
    def set_sqlite_pragma(dbapi_conn, connection_record):
        cursor = dbapi_conn.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    init_database(test_engine)
    yield test_engine
    test_engine.dispose()


@pytest.fixture
def session(engine: Engine) -> Session:
    """Create a test database session.

    Creates a session tied to the test's engine.
    """
    factory = sessionmaker(
        bind=engine,
        expire_on_commit=False,
    )
    with factory() as sess:
        yield sess


@pytest.fixture
def duckdb_conn():
    """Create an in-memory DuckDB connection for testing."""
    conn = duckdb.connect(":memory:")
    yield conn
    conn.close()


# ---------------------------------------------------------------------------
# Postgres workspace fixtures (DAT-321)
#
# The full SQLAlchemy spine runs against Postgres post-L2. A single container
# is reused for the entire pytest invocation (boot ~3 s, amortized across all
# tests); per-test isolation is handled by TRUNCATE CASCADE over every table
# registered on Base.metadata.
#
# Fixture overview:
#   pg_container   — session-scoped, lifecycle of the Postgres 17 container
#   pg_url         — session-scoped, the psycopg URL ("postgresql+psycopg://…")
#   pg_url_clean   — function-scoped, same URL but TRUNCATE'd before each test
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def pg_container() -> Generator[PostgresContainer]:
    """Boot one Postgres 17 container for the whole pytest invocation.

    Pinned to the same image L1's docker-compose runs so any dialect quirk
    surfaces here too. Cleaned up at session end.
    """
    with PostgresContainer("postgres:17") as pg:
        yield pg


@pytest.fixture(scope="session")
def pg_url(pg_container: PostgresContainer) -> str:
    """SQLAlchemy URL for the session-scoped Postgres container.

    Forces the psycopg driver to match production (`DATABASE_URL` in the
    container substrate uses `postgresql+psycopg://`).
    """
    return pg_container.get_connection_url(driver="psycopg")


@pytest.fixture
def pg_url_clean(pg_url: str) -> str:
    """Postgres URL with all Base-registered tables truncated before the test.

    Uses ``TRUNCATE ... RESTART IDENTITY CASCADE`` over ``Base.metadata.tables``
    so per-test isolation does not depend on FK declaration order. Tables that
    haven't been created yet (e.g. before ``metadata.create_all``) are simply
    skipped — TRUNCATE on a missing table is an error, so we filter by what
    actually exists.
    """
    from sqlalchemy import inspect

    from dataraum.core.connections import ConnectionManager as _CM  # noqa: F401
    from dataraum.storage import Base

    # Importing ConnectionManager triggers _import_all_models registration
    # so Base.metadata has every per-session table by the time we truncate.
    engine = create_engine(pg_url, future=True)
    try:
        existing = set(inspect(engine).get_table_names())
        if existing:
            targets = [t for t in Base.metadata.tables.values() if t.name in existing]
            if targets:
                names = ", ".join(f'"{t.name}"' for t in targets)
                with engine.begin() as conn:
                    conn.execute(text(f"TRUNCATE {names} RESTART IDENTITY CASCADE"))
    finally:
        engine.dispose()
    return pg_url
