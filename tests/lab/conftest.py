"""Fixtures for the entropy lab — read-only access to real e2e pipeline output."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

import duckdb
import pytest
from sqlalchemy import create_engine, event, select, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

E2E_DATA_DIR = Path(__file__).resolve().parents[2] / ".e2e" / "medium" / "pipeline_full"


def _ensure_models_imported() -> None:
    """Import all SQLAlchemy models so mapper relationships resolve."""
    from dataraum.pipeline.registry import import_all_phase_models

    import_all_phase_models()


@pytest.fixture(scope="session")
def lab_engine() -> Engine:
    """SQLAlchemy engine pointing at the e2e metadata.db (read-only)."""
    db_path = E2E_DATA_DIR / "metadata.db"
    if not db_path.exists():
        pytest.skip(f"E2E data not found at {db_path}")

    # Open read-only via SQLite URI mode
    def _connect() -> sqlite3.Connection:
        return sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)

    _ensure_models_imported()

    engine = create_engine("sqlite://", creator=_connect, echo=False)

    @event.listens_for(engine, "connect")
    def _pragmas(dbapi_conn: Any, _rec: Any) -> None:
        cur = dbapi_conn.cursor()
        cur.execute("PRAGMA foreign_keys=ON")
        cur.execute("PRAGMA query_only=ON")
        cur.close()

    yield engine
    engine.dispose()


@pytest.fixture(scope="session")
def lab_session_factory(lab_engine: Engine) -> sessionmaker:
    """Session factory bound to the e2e metadata.db."""
    return sessionmaker(bind=lab_engine, expire_on_commit=False)


@pytest.fixture
def lab_session(lab_session_factory: sessionmaker) -> Session:
    """Fresh session for each test — no writes committed."""
    with lab_session_factory() as session:
        yield session


@pytest.fixture(scope="session")
def lab_duckdb() -> duckdb.DuckDBPyConnection | None:
    """DuckDB connection to the e2e data.duckdb (read-only).

    Returns None if the file is locked (e.g. pipeline still running).
    Detectors currently don't use DuckDB — it's reserved for future use.
    """
    db_path = E2E_DATA_DIR / "data.duckdb"
    if not db_path.exists():
        return None
    try:
        conn = duckdb.connect(str(db_path), read_only=True)
        yield conn
        conn.close()
    except duckdb.IOException:
        yield None


@pytest.fixture(scope="session")
def source_id(lab_engine: Engine) -> str:
    """The single source_id in the e2e data."""
    with lab_engine.connect() as conn:
        row = conn.execute(text("SELECT source_id FROM sources LIMIT 1")).fetchone()
        assert row is not None, "No sources in e2e metadata.db"
        return row[0]


@pytest.fixture(scope="session")
def typed_columns(lab_engine: Engine) -> list[dict[str, str]]:
    """All typed columns: list of {table_name, column_name, column_id, table_id}."""
    with lab_engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT t.table_name, c.column_name, c.column_id, c.table_id "
                "FROM columns c JOIN tables t ON c.table_id = t.table_id "
                "WHERE t.layer = 'typed' ORDER BY t.table_name, c.column_name"
            )
        ).fetchall()
    return [
        {
            "table_name": r[0],
            "column_name": r[1],
            "column_id": r[2],
            "table_id": r[3],
        }
        for r in rows
    ]
