"""Shared pytest fixtures for all tests."""

import duckdb
import pytest
from sqlalchemy import event
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from dataraum_context.storage.schema import init_database


@pytest.fixture(scope="function")
async def engine() -> AsyncEngine:
    """Create an in-memory SQLite engine for testing.

    Creates a fresh database for each test function.
    """
    test_engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        echo=False,
        future=True,
    )

    # Enable foreign keys for SQLite
    @event.listens_for(test_engine.sync_engine, "connect")
    def set_sqlite_pragma(dbapi_conn, connection_record):
        cursor = dbapi_conn.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    await init_database(test_engine)
    yield test_engine
    await test_engine.dispose()


@pytest.fixture
async def async_session(engine: AsyncEngine) -> AsyncSession:
    """Create a test database session.

    Creates a session tied to the test's engine.
    """
    factory = async_sessionmaker(
        engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )
    async with factory() as session:
        yield session


@pytest.fixture
def duckdb_conn():
    """Create an in-memory DuckDB connection for testing."""
    conn = duckdb.connect(":memory:")
    yield conn
    conn.close()
