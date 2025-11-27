"""Database schema initialization and management."""

from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from dataraum_context.storage.base import Base


async def init_database(engine: AsyncEngine) -> None:
    """
    Initialize database schema.

    Creates all tables defined in SQLAlchemy models.
    Safe to call multiple times - only creates missing tables.

    Args:
        engine: Async SQLAlchemy engine
    """
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # Record schema version
    await _set_schema_version(engine, "0.1.0")


async def reset_database(engine: AsyncEngine) -> None:
    """
    Drop and recreate all tables.

    WARNING: This destroys all data. Use only in development/testing.

    Args:
        engine: Async SQLAlchemy engine
    """
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)

    await _set_schema_version(engine, "0.1.0")


async def _set_schema_version(engine: AsyncEngine, version: str) -> None:
    """Record the schema version in the database."""
    from dataraum_context.storage.models import DBSchemaVersion

    async with AsyncSession(engine) as session:
        # Check if version already exists
        result = await session.execute(
            select(DBSchemaVersion).where(DBSchemaVersion.version == version)
        )
        existing = result.scalar_one_or_none()

        if not existing:
            schema_version = DBSchemaVersion(
                version=version,
                applied_at=datetime.now(UTC),
            )
            session.add(schema_version)
            await session.commit()


async def get_schema_version(session: AsyncSession) -> str | None:
    """Get the current schema version."""
    from dataraum_context.storage.models import DBSchemaVersion

    result = await session.execute(
        select(DBSchemaVersion).order_by(DBSchemaVersion.applied_at.desc()).limit(1)
    )
    version = result.scalar_one_or_none()
    return version.version if version else None
