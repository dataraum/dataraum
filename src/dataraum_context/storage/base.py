"""SQLAlchemy base configuration, engine management, and schema initialization."""

from typing import Any

from sqlalchemy import MetaData, event
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase

# Naming convention for constraints
# This ensures consistent constraint names across PostgreSQL and SQLite
convention = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}

metadata_obj = MetaData(naming_convention=convention)


class Base(DeclarativeBase):
    """Base class for all SQLAlchemy models."""

    metadata = metadata_obj


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
            def set_sqlite_pragma(dbapi_conn: Any, connection_record: Any) -> None:
                cursor = dbapi_conn.cursor()
                cursor.execute("PRAGMA foreign_keys=ON")
                cursor.close()

    if _engine is None:
        raise RuntimeError("Database engine not initialized. Call get_engine(database_url) first.")

    return _engine


async def init_database(engine: AsyncEngine) -> None:
    """
    Initialize database schema.

    Creates all tables defined in SQLAlchemy models.
    Safe to call multiple times - only creates missing tables.

    Args:
        engine: Async SQLAlchemy engine
    """
    # Import all model modules to register them with SQLAlchemy Base metadata
    # These imports ensure tables are created when init_database() is called
    from dataraum_context.analysis.correlation import db_models as _correlation_models  # noqa: F401
    from dataraum_context.analysis.cycles import db_models as _cycles_models  # noqa: F401
    from dataraum_context.analysis.quality_summary import (
        db_models as _quality_summary_models,  # noqa: F401
    )
    from dataraum_context.analysis.relationships import (
        db_models as _relationships_models,  # noqa: F401
    )
    from dataraum_context.analysis.semantic import db_models as _semantic_models  # noqa: F401
    from dataraum_context.analysis.slicing import db_models as _slicing_models  # noqa: F401
    from dataraum_context.analysis.statistics import db_models as _statistics_models  # noqa: F401
    from dataraum_context.analysis.temporal import db_models as _temporal_models  # noqa: F401
    from dataraum_context.analysis.temporal_slicing import (
        db_models as _temporal_slicing_models,  # noqa: F401
    )
    from dataraum_context.analysis.topology import db_models as _topology_models  # noqa: F401
    from dataraum_context.analysis.typing import db_models as _typing_models  # noqa: F401
    from dataraum_context.analysis.validation import db_models as _validation_models  # noqa: F401
    from dataraum_context.graphs import db_models as _graphs_models  # noqa: F401
    from dataraum_context.llm import db_models as _llm_models  # noqa: F401
    from dataraum_context.quality import db_models as _quality_models  # noqa: F401
    from dataraum_context.storage import models as _storage_models  # noqa: F401

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def reset_database(engine: AsyncEngine) -> None:
    """
    Drop and recreate all tables.

    WARNING: This destroys all data. Use only in development/testing.

    Args:
        engine: Async SQLAlchemy engine
    """
    # Import all model modules to register them with SQLAlchemy Base metadata
    from dataraum_context.analysis.correlation import db_models as _correlation_models  # noqa: F401
    from dataraum_context.analysis.cycles import db_models as _cycles_models  # noqa: F401
    from dataraum_context.analysis.quality_summary import (
        db_models as _quality_summary_models,  # noqa: F401
    )
    from dataraum_context.analysis.relationships import (
        db_models as _relationships_models,  # noqa: F401
    )
    from dataraum_context.analysis.semantic import db_models as _semantic_models  # noqa: F401
    from dataraum_context.analysis.slicing import db_models as _slicing_models  # noqa: F401
    from dataraum_context.analysis.statistics import db_models as _statistics_models  # noqa: F401
    from dataraum_context.analysis.temporal import db_models as _temporal_models  # noqa: F401
    from dataraum_context.analysis.temporal_slicing import (
        db_models as _temporal_slicing_models,  # noqa: F401
    )
    from dataraum_context.analysis.topology import db_models as _topology_models  # noqa: F401
    from dataraum_context.analysis.typing import db_models as _typing_models  # noqa: F401
    from dataraum_context.analysis.validation import db_models as _validation_models  # noqa: F401
    from dataraum_context.graphs import db_models as _graphs_models  # noqa: F401
    from dataraum_context.llm import db_models as _llm_models  # noqa: F401
    from dataraum_context.quality import db_models as _quality_models  # noqa: F401
    from dataraum_context.storage import models as _storage_models  # noqa: F401

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
