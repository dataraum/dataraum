"""Shared pytest fixtures for all tests."""

from __future__ import annotations

import importlib
import os
from collections.abc import Generator

# Set the test workspace_id BEFORE any dataraum import. The connection layer
# (post-DAT-339 Commit B) reads DATARAUM_WORKSPACE_ID at engine-create time
# to derive the per-workspace Postgres schema; tests that touch Postgres
# need a stable value. Unit tests using SQLite ignore it (the listener is
# dialect-gated). Set unconditionally so a stray pytest invocation from a
# shell where DATARAUM_WORKSPACE_ID is already exported doesn't pollute
# the test schema.
os.environ["DATARAUM_WORKSPACE_ID"] = "test"

# DAT-363: the engine reads all substrate config through one validated
# Settings object (dataraum.core.settings). get_settings() validates the full
# env on first call, so ANY test that triggers it — booting the server,
# building a DB connection, creating the LLM provider via the factory — needs
# every required var present. Set deterministic placeholders here (mirroring
# DATARAUM_WORKSPACE_ID above) so the singleton constructs; tests needing real
# values (e.g. a live Postgres URL) override via monkeypatch + the autouse
# reset_settings fixture below, and tests asserting a specific var is required
# delenv just that one.
os.environ["DATABASE_URL"] = "postgresql+psycopg://test:test@localhost:5432/test"
os.environ["DUCKLAKE_CATALOG_URL"] = "postgresql://test:test@localhost:5432/lake"
os.environ["DUCKLAKE_DATA_PATH"] = "/tmp/dataraum-test-lake"
os.environ["ANTHROPIC_API_KEY"] = "sk-ant-test-placeholder"
os.environ["TEMPORAL_HOST"] = "localhost:7233"
os.environ["TEMPORAL_NAMESPACE"] = "default"
# Per-workspace task queue (DAT-505): the worker polls ``engine-<workspace_id>``,
# and ``bootstrap_workspace`` asserts the two agree at boot. Keep it consistent
# with DATARAUM_WORKSPACE_ID="test" above so any boot path in the suite passes.
os.environ["TEMPORAL_TASK_QUEUE"] = "engine-test"
# DAT-388: object-store creds are required Settings now (the lake lives on the
# object store in production). Tests bootstrap DuckLake against a local tmp
# DATA_PATH and stub apply_s3_secret (see _stub_s3_secret), so these never reach
# a real store — just placeholders to satisfy the boot-time contract.
os.environ["S3_ENDPOINT"] = "test-s3:8333"
os.environ["S3_ACCESS_KEY_ID"] = "test-access-key"
os.environ["S3_SECRET_ACCESS_KEY"] = "test-secret-key"
# The lake bucket the source-URI validator (DAT-389) allows. Production sets it
# from S3_BUCKET (compose); tests use a stable name so ``s3://dataraum-lake/...``
# source URIs pass validation while the lake itself bootstraps off a local tmp
# DATA_PATH (apply_s3_secret is stubbed — see _stub_s3_secret).
os.environ["S3_BUCKET"] = "dataraum-lake"

import duckdb  # noqa: E402
import pytest  # noqa: E402
from sqlalchemy import create_engine, event, text  # noqa: E402
from sqlalchemy.engine import Engine  # noqa: E402
from sqlalchemy.orm import Session, sessionmaker  # noqa: E402
from sqlalchemy.pool import StaticPool  # noqa: E402
from testcontainers.postgres import PostgresContainer  # noqa: E402

# Mirror what ``bootstrap_workspace()`` would do at worker startup:
# stamp the module-level active-workspace pointer so any
# ConnectionManager.initialize() in a unit test resolves the workspace
# schema. Tests for ``bootstrap_workspace`` itself reset and rebootstrap
# via the autouse ``_isolate_active_workspace`` fixture in
# ``tests/unit/server/test_workspace.py``.
_ws_mod = importlib.import_module("dataraum.server.workspace")  # noqa: E402
from dataraum.storage import init_database  # noqa: E402

_ws_mod._active_workspace_id = os.environ["DATARAUM_WORKSPACE_ID"]

_TEST_RUN_ID = "00000000-0000-0000-0000-000000000001"
_TEST_SOURCE_ID = "00000000-0000-0000-0000-000000000002"


@pytest.fixture(autouse=True)
def _reset_settings_cache() -> Generator[None]:
    """Clear the settings singleton around every test (DAT-363).

    ``get_settings()`` caches process-wide; without this, a ``monkeypatch.setenv``
    in one test would not take effect (stale cache) and settings would leak
    across tests. Reset before and after so each test reads current env.
    """
    from dataraum.core.settings import reset_settings

    reset_settings()
    yield
    reset_settings()


@event.listens_for(Session, "before_flush")
def _autofill_run_id_globally(sess, _flush_ctx, _instances):
    """Auto-fill the ``run_id`` version axis on any pending row that left it None.

    Pure test convenience (DAT-506) — production code always stamps ``run_id``
    (the workflow mints it). Run-versioned models now carry a NOT NULL ``run_id``;
    this hook keeps test fixtures that construct those rows directly from having
    to set it. ``RunTable`` carries ``run_id`` as a PK (not a fillable default),
    so it is left alone — tests anchor it explicitly via ``link_run_tables``.
    """
    from dataraum.investigation.db_models import RunTable

    for obj in sess.new:
        if isinstance(obj, RunTable):
            continue
        if hasattr(obj, "run_id") and getattr(obj, "run_id", None) is None:
            obj.run_id = _TEST_RUN_ID


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

    Seeds a baseline ``Source`` row so tests that construct source-scoped DB
    models have a valid FK target. Sessions live in cockpit_db now (DAT-506) —
    the engine has no ``InvestigationSession`` row; run-versioned models scope by
    ``run_id`` (``baseline_run_id()``).
    """
    from dataraum.storage import Source

    factory = sessionmaker(
        bind=engine,
        expire_on_commit=False,
    )
    with factory() as sess:
        sess.add(Source(source_id=_TEST_SOURCE_ID, name="test_baseline", source_type="csv"))
        sess.flush()
        yield sess


def baseline_run_id() -> str:
    """Return the baseline run_id for run-versioned test rows (DAT-506).

    Run-versioned DB models carry a NOT NULL ``run_id``; tests that
    ``session.add(...)`` one of those rows should set ``run_id=baseline_run_id()``
    (and anchor tables via ``link_run_tables(sess, baseline_run_id(), [...])``).
    """
    return _TEST_RUN_ID


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
    """Boot one Postgres 19beta1 container for the whole pytest invocation.

    Pinned to the same image docker-compose runs (DAT-726) so any dialect quirk
    surfaces here too — and so SQL/PGQ property-graph tests (ADR-0021) run on the
    core-Postgres-19 feature they target. Cleaned up at session end.
    """
    with PostgresContainer("postgres:19beta1") as pg:
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


@pytest.fixture(scope="session", autouse=True)
def _stub_s3_secret() -> Generator[None]:
    """Stub the object-store secret/``httpfs`` registration for the whole suite.

    Production lake DATA_PATH is an ``s3://`` URI and :func:`bootstrap_lake`
    unconditionally registers the S3 secret before ATTACH. Tests bootstrap
    DuckLake against a local tmp DATA_PATH — we do not stand up an object store
    (DuckLake-over-S3 is DuckLake's concern, not ours to test) — so the real
    secret + ``INSTALL httpfs`` step is replaced with a no-op. This is the one
    place the suite acknowledges it has no object store.
    """
    from unittest.mock import patch

    with patch("dataraum.server.storage.apply_s3_secret"):
        yield


@pytest.fixture(scope="session")
def lake_metadata_schema() -> str:
    """The per-workspace catalog schema every test ATTACH uses (DAT-815).

    Same ``ws_<id>`` derivation as production (the worker bootstrap derives it
    from the boot workspace id), applied to the suite's fixed test workspace —
    so the whole suite exercises the METADATA_SCHEMA path, never the implicit
    ``public`` layout.
    """
    from dataraum.server.workspace import schema_name_for

    return schema_name_for(os.environ["DATARAUM_WORKSPACE_ID"])


@pytest.fixture(scope="session")
def lake_anchor(lake_catalog_url: str, lake_data_path: str, lake_metadata_schema: str):
    """Bootstrap the DuckLake anchor once for the whole pytest invocation.

    Pairs the session-scoped lake catalog DB with a session-scoped tmp DATA_PATH.
    Per-session schemas are cleaned by the function-scoped ``lake_clean`` fixture.
    """
    from dataraum.server.storage import bootstrap_lake, teardown_lake

    bootstrap_lake(lake_catalog_url, lake_data_path, metadata_schema=lake_metadata_schema)
    yield
    teardown_lake()


@pytest.fixture
def no_anchor(lake_anchor, lake_catalog_url: str, lake_data_path: str, lake_metadata_schema: str):
    """Tear down the session anchor for one test; restore it after.

    Use this for "before bootstrap" tests so other session-scoped consumers of
    ``lake_anchor`` keep working. The restore reuses the canonical
    ``lake_data_path`` because DuckLake's catalog rejects a new DATA_PATH.
    """
    from dataraum.server.storage import bootstrap_lake, teardown_lake

    teardown_lake()
    yield
    bootstrap_lake(lake_catalog_url, lake_data_path, metadata_schema=lake_metadata_schema)


def clean_lake_layers() -> None:
    """Drop per-test residue from the DuckLake layer schemas.

    Post-DAT-341 the workspace-stable layer schemas (``raw`` / ``typed`` /
    ``quarantine``) survive across tests, so isolation needs to drop the
    tables INSIDE those schemas rather than dropping the schemas themselves.
    Reserved ``session_*`` / ``archive_*`` schemas (slice 2) are dropped
    wholesale. Plain function so both the function-scoped ``lake_clean``
    fixture and module-scoped read-only fixtures can reuse it.
    """
    from dataraum.server.storage import LAKE_CATALOG_ALIAS, LAKE_LAYER_SCHEMAS, get_anchor

    anchor = get_anchor()

    # Drop per-test tables in workspace-stable layer schemas.
    for layer_schema in LAKE_LAYER_SCHEMAS:
        tables = anchor.execute(
            "SELECT table_name FROM duckdb_tables() "
            f"WHERE database_name = '{LAKE_CATALOG_ALIAS}' "
            f"AND schema_name = '{layer_schema}'"
        ).fetchall()
        for (name,) in tables:
            anchor.execute(f'DROP TABLE IF EXISTS {LAKE_CATALOG_ALIAS}."{layer_schema}"."{name}"')

    # Reserved session_* / archive_* namespaces (slice 2): drop wholesale.
    schemas = anchor.execute(
        "SELECT schema_name FROM duckdb_schemas() "
        f"WHERE database_name = '{LAKE_CATALOG_ALIAS}' "
        "AND (schema_name LIKE 'session_%' OR schema_name LIKE 'archive_%')"
    ).fetchall()
    for (name,) in schemas:
        anchor.execute(f'DROP SCHEMA IF EXISTS {LAKE_CATALOG_ALIAS}."{name}" CASCADE')


@pytest.fixture
def lake_clean(lake_anchor):
    """Drop per-test residue from the lake before each test."""
    clean_lake_layers()
    yield


def truncate_workspace_tables(pg_url: str) -> None:
    """TRUNCATE every Base-registered table in the workspace schema.

    Uses ``TRUNCATE ... RESTART IDENTITY CASCADE`` over ``Base.metadata.tables``
    so isolation does not depend on FK declaration order. Tables that haven't
    been created yet (e.g. before ``metadata.create_all``) are simply skipped —
    TRUNCATE on a missing table is an error, so we filter by what actually
    exists. Plain function so both the function-scoped ``pg_url_clean`` fixture
    and module-scoped read-only fixtures can reuse it.
    """
    from sqlalchemy import inspect

    from dataraum.server.workspace import schema_name_for
    from dataraum.storage import Base

    # Filter by Postgres-side existence: TRUNCATE on a missing table is an
    # error, so phases before any ConnectionManager.initialize() runs stay
    # no-ops. Once a test triggers initialize() the side-effect of
    # _import_all_models registers every model on Base.metadata for the
    # rest of the pytest invocation.
    #
    # Post-DAT-339 Commit B: tables live in the workspace schema, not
    # ``public``. ``inspect(...).get_table_names`` needs the schema arg,
    # and TRUNCATE must qualify by schema. First-test no-op survives:
    # ``get_table_names`` on a missing schema returns an empty list.
    schema_name = schema_name_for(os.environ["DATARAUM_WORKSPACE_ID"])
    engine = create_engine(pg_url, future=True)
    try:
        existing = set(inspect(engine).get_table_names(schema=schema_name))
        if existing:
            targets = [t for t in Base.metadata.tables.values() if t.name in existing]
            if targets:
                names = ", ".join(f'"{schema_name}"."{t.name}"' for t in targets)
                with engine.begin() as conn:
                    conn.execute(text(f"TRUNCATE {names} RESTART IDENTITY CASCADE"))
    finally:
        engine.dispose()


@pytest.fixture
def pg_url_clean(pg_url: str) -> str:
    """Postgres URL with all Base-registered tables truncated before the test."""
    truncate_workspace_tables(pg_url)
    return pg_url
