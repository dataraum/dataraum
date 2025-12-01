"""Tests for database schema initialization."""

from sqlalchemy import inspect, select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from dataraum_context.storage.models_v2 import Source
from dataraum_context.storage.schema import get_schema_version, init_database, reset_database


class TestSchemaInitialization:
    """Test database schema initialization."""

    async def test_init_database_creates_tables(self, engine: AsyncEngine):
        """Test that init_database creates all tables."""
        async with engine.connect() as conn:
            # Get list of tables using inspector
            tables = await conn.run_sync(lambda sync_conn: inspect(sync_conn).get_table_names())

        # Check that key tables exist
        expected_tables = [
            "schema_version",
            "sources",
            "tables",
            "columns",
            "column_profiles",
            "type_candidates",
            "type_decisions",
            "semantic_annotations",
            "table_entities",
            "relationships",
            "join_paths",
            "temporal_profiles",
            "quality_rules",
            "quality_results",
            "quality_scores",
            "ontologies",
            "ontology_applications",
            "checkpoints",
            "review_queue",
            "llm_cache",
        ]

        for table in expected_tables:
            assert table in tables, f"Table {table} not created"

    async def test_init_database_sets_version(self, engine: AsyncEngine):
        """Test that init_database sets the schema version."""
        from sqlalchemy.ext.asyncio import async_sessionmaker

        factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        async with factory() as session:
            version = await get_schema_version(session)
            assert version == "0.1.0"

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

        # Verify data is cleared (except schema version)
        async with factory() as session:
            result = await session.execute(select(Source))
            sources = result.scalars().all()
            assert len(sources) == 0

            # But schema version should still be set
            version = await get_schema_version(session)
            assert version == "0.1.0"

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
