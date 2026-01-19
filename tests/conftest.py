"""Shared pytest fixtures for all tests."""

import duckdb
import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from dataraum_context.storage import init_database


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
