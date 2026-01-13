"""Tests for database schema initialization."""

from sqlalchemy import inspect, select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from dataraum_context.storage import Source, init_database, reset_database


class TestSchemaInitialization:
    """Test database schema initialization."""

    async def test_init_database_creates_tables(self, engine: AsyncEngine):
        """Test that init_database creates all tables."""
        async with engine.connect() as conn:
            # Get list of tables using inspector
            tables = await conn.run_sync(lambda sync_conn: inspect(sync_conn).get_table_names())

        # Check that key tables exist
        expected_tables = [
            "sources",
            "tables",
            "columns",
            "statistical_profiles",
            "type_candidates",
            "type_decisions",
            "semantic_annotations",
            "table_entities",
            "relationships",
            "temporal_column_profiles",
            "llm_cache",
        ]

        for table in expected_tables:
            assert table in tables, f"Table {table} not created"

    async def test_reset_database_clears_data(self, engine: AsyncEngine):
        """Test that reset_database clears all data."""
        from sqlalchemy.ext.asyncio import async_sessionmaker

        factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

        # Add some data
        async with factory() as session:
            source = Source(name="test", source_type="csv")
            session.add(source)
            await session.commit()

        # Verify data exists
        async with factory() as session:
            result = await session.execute(select(Source))
            sources = result.scalars().all()
            assert len(sources) == 1

        # Reset database
        await reset_database(engine)

        # Verify data is cleared
        async with factory() as session:
            result = await session.execute(select(Source))
            sources = result.scalars().all()
            assert len(sources) == 0

    async def test_init_database_idempotent(self, engine: AsyncEngine):
        """Test that init_database can be called multiple times safely."""
        # First initialization happens in fixture
        # Call again
        await init_database(engine)
        await init_database(engine)

        # Should still work
        from sqlalchemy.ext.asyncio import async_sessionmaker

        factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

        async with factory() as session:
            source = Source(name="test", source_type="csv")
            session.add(source)
            await session.commit()

            result = await session.execute(select(Source))
            sources = result.scalars().all()
            assert len(sources) == 1
