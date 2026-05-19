"""Shared pytest fixtures for all tests."""

from __future__ import annotations

from collections.abc import Generator

import duckdb
import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool
from testcontainers.postgres import PostgresContainer

from dataraum.storage import init_database

_TEST_SESSION_ID = "00000000-0000-0000-0000-000000000001"
_TEST_SOURCE_ID = "00000000-0000-0000-0000-000000000002"


@pytest.fixture(autouse=True)
def _close_mcp_servers_after_test(monkeypatch: pytest.MonkeyPatch):
    """Auto-close any MCP server created during a test.

    Tests construct servers inline via ``create_server(output_dir=...)``
    which caches workspace + session ``ConnectionManager`` instances. The
    cached psycopg pools never get disposed by the test itself, so they
    leak until GC catches them — Python 3.12+ raises
    ``ResourceWarning: <psycopg.Connection> was deleted while still open``.

    This fixture monkeypatches ``create_server`` to track every returned
    ``Server`` and calls ``server.close()`` on teardown. Tests do not need
    to opt in.
    """
    import warnings

    from dataraum.mcp import server as server_mod

    created: list = []
    original = server_mod.create_server

    def tracked(*args, **kwargs):
        s = original(*args, **kwargs)
        created.append(s)
        return s

    monkeypatch.setattr(server_mod, "create_server", tracked)
    yield
    for s in created:
        try:
            s.close()  # type: ignore[attr-defined]
        except Exception as exc:
            warnings.warn(
                f"Failed to close MCP server during test teardown: {s!r} ({exc!r})",
                stacklevel=2,
            )


@event.listens_for(Session, "before_flush")
def _autofill_session_id_globally(sess, _flush_ctx, _instances):
    """Auto-fill per-session FK on any pending row that left it None.

    Pure test convenience — production code always sets ``session_id`` via the
    hybrid plumbing introduced in DAT-321. This hook keeps test fixtures that
    construct DB rows directly from having to know about the new FK.

    Excludes ``InvestigationSession`` itself — its ``session_id`` is the PK,
    not a FK, so autofilling would collide with the baseline row.
    """
    from dataraum.investigation.db_models import InvestigationSession

    for obj in sess.new:
        if isinstance(obj, InvestigationSession):
            continue
        if hasattr(obj, "session_id") and getattr(obj, "session_id", None) is None:
            obj.session_id = _TEST_SESSION_ID


@pytest.fixture(scope="function")
def engine() -> Engine:
    """Create an in-memory SQLite engine for testing.

    Uses ``StaticPool`` so the engine owns exactly one SQLite connection
    that ``dispose()`` closes deterministically — Python 3.12+ raises
    ``ResourceWarning`` if a ``sqlite3.Connection`` is GC'd while still
    open, and ``QueuePool`` for ``:memory:`` SQLite tends to leave raw
    connections around for the GC to find.
    """
    test_engine = create_engine(
        "sqlite:///:memory:",
        echo=False,
        future=True,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
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

    Seeds a baseline ``Source`` + ``InvestigationSession`` row so tests that
    construct per-session DB models have a valid FK target. The
    ``session_id`` is auto-populated by the global ``before_flush`` hook
    above; tests that need the value explicitly call ``baseline_session_id()``.
    """
    from datetime import UTC, datetime

    from dataraum.investigation.db_models import InvestigationSession
    from dataraum.storage import Source

    factory = sessionmaker(
        bind=engine,
        expire_on_commit=False,
    )
    with factory() as sess:
        sess.add(Source(source_id=_TEST_SOURCE_ID, name="test_baseline", source_type="csv"))
        sess.flush()
        sess.add(
            InvestigationSession(
                session_id=_TEST_SESSION_ID,
                source_id=_TEST_SOURCE_ID,
                intent="conftest baseline",
                status="active",
                started_at=datetime.now(UTC),
            )
        )
        sess.flush()
        yield sess


def baseline_session_id() -> str:
    """Return the baseline InvestigationSession id seeded by the ``session`` fixture.

    Per-session DB models post-DAT-321 carry a NOT NULL FK to
    ``investigation_sessions.session_id``; tests that ``session.add(...)``
    one of those rows should set ``session_id=baseline_session_id()``.
    """
    return _TEST_SESSION_ID


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


@pytest.fixture(scope="session")
def lake_catalog_url(pg_container: PostgresContainer) -> str:
    """Create + return a Postgres URL for the DuckLake catalog database.

    Sibling DB on the same testcontainer used for the workspace; mirrors the
    L1 docker-compose shape (one Postgres, two logical DBs).
    """
    import psycopg
    from psycopg.conninfo import make_conninfo

    catalog_db = "dataraum_lake_catalog_test"
    base_url = pg_container.get_connection_url(driver=None)

    # urlparse-friendly: testcontainers returns postgresql:// for driver=None
    from urllib.parse import urlparse

    p = urlparse(base_url)
    conninfo = make_conninfo(
        host=p.hostname or "localhost",
        port=p.port or 5432,
        user=p.username or "",
        password=p.password or "",
        dbname="postgres",
    )
    with psycopg.connect(conninfo, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(f"DROP DATABASE IF EXISTS {catalog_db}")
            cur.execute(f"CREATE DATABASE {catalog_db}")

    return f"postgresql://{p.username}:{p.password}@{p.hostname}:{p.port}/{catalog_db}"


@pytest.fixture(scope="session")
def lake_data_path(tmp_path_factory: pytest.TempPathFactory) -> str:
    """Canonical DATA_PATH for the DuckLake catalog used across this pytest run.

    DuckLake persists the DATA_PATH inside its catalog, so every ATTACH to the
    same catalog must point at the *same* directory. Tests that tear the
    anchor down and re-ATTACH need this canonical path to restore.
    """
    return str(tmp_path_factory.mktemp("ducklake_data"))


@pytest.fixture(scope="session")
def lake_anchor(lake_catalog_url: str, lake_data_path: str):
    """Bootstrap the DuckLake anchor once for the whole pytest invocation.

    Pairs the session-scoped lake catalog DB with a session-scoped tmp DATA_PATH.
    Per-session schemas are cleaned by the function-scoped ``lake_clean`` fixture.
    """
    from dataraum.server.storage import bootstrap_lake, teardown_lake

    bootstrap_lake(lake_catalog_url, lake_data_path)
    yield
    teardown_lake()


@pytest.fixture
def no_anchor(lake_anchor, lake_catalog_url: str, lake_data_path: str):
    """Tear down the session anchor for one test; restore it after.

    Use this for "before bootstrap" tests so other session-scoped consumers of
    ``lake_anchor`` keep working. The restore reuses the canonical
    ``lake_data_path`` because DuckLake's catalog rejects a new DATA_PATH.
    """
    from dataraum.server.storage import bootstrap_lake, teardown_lake

    teardown_lake()
    yield
    bootstrap_lake(lake_catalog_url, lake_data_path)


@pytest.fixture
def lake_clean(lake_anchor):
    """Drop per-session and archived schemas from the lake before each test.

    Pairs with ``pg_url_clean`` so test isolation is symmetric across the
    Postgres workspace and the DuckLake catalog.
    """
    from dataraum.server.storage import LAKE_CATALOG_ALIAS, get_anchor

    anchor = get_anchor()
    schemas = anchor.execute(
        "SELECT schema_name FROM duckdb_schemas() "
        f"WHERE database_name = '{LAKE_CATALOG_ALIAS}' "
        "AND (schema_name LIKE 'session_%' OR schema_name LIKE 'archive_%')"
    ).fetchall()
    for (name,) in schemas:
        anchor.execute(f'DROP SCHEMA IF EXISTS {LAKE_CATALOG_ALIAS}."{name}" CASCADE')
    yield


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

    from dataraum.storage import Base

    # Filter by Postgres-side existence: TRUNCATE on a missing table is an
    # error, so phases before any ConnectionManager.initialize() runs stay
    # no-ops. Once a test triggers initialize() the side-effect of
    # _import_all_models registers every model on Base.metadata for the
    # rest of the pytest invocation.
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
