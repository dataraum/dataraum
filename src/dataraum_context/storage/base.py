"""SQLAlchemy base configuration and session management."""

from sqlalchemy import event
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    """Base class for all SQLAlchemy models."""

    pass


# Global engine and session factory (initialized by app)
_engine: AsyncEngine | None = None
_session_factory: async_sessionmaker[AsyncSession] | None = None


def get_engine(database_url: str | None = None) -> AsyncEngine:
    """
    Get or create the database engine.

    Args:
        database_url: Database connection string. If None, uses existing engine.
                     Supports:
                     - sqlite+aiosqlite:///path/to/db.sqlite
                     - postgresql+asyncpg://user:pass@host/db

    Returns:
        Async SQLAlchemy engine
    """
    global _engine

    if database_url is not None:
        _engine = create_async_engine(
            database_url,
            echo=False,
            future=True,
        )

        # Enable foreign keys for SQLite
        if database_url.startswith("sqlite"):

            @event.listens_for(_engine.sync_engine, "connect")
            def set_sqlite_pragma(dbapi_conn, connection_record):
                cursor = dbapi_conn.cursor()
                cursor.execute("PRAGMA foreign_keys=ON")
                cursor.close()

    if _engine is None:
        raise RuntimeError("Database engine not initialized. Call get_engine(database_url) first.")

    return _engine
