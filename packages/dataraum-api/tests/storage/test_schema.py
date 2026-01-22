"""Tests for database schema initialization."""

from sqlalchemy import inspect, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

from dataraum.storage import Source, init_database, reset_database


class TestSchemaInitialization:
    """Test database schema initialization."""

    def test_init_database_creates_tables(self, engine: Engine):
        """Test that init_database creates all tables."""
        with engine.connect() as conn:
            # Get list of tables using inspector
            tables = inspect(conn).get_table_names()

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

    def test_reset_database_clears_data(self, engine: Engine):
        """Test that reset_database clears all data."""
        factory = sessionmaker(bind=engine, expire_on_commit=False)

        # Add some data
        with factory() as session:
            source = Source(name="test", source_type="csv")
            session.add(source)
            session.commit()

        # Verify data exists
        with factory() as session:
            result = session.execute(select(Source))
            sources = result.scalars().all()
            assert len(sources) == 1

        # Reset database
        reset_database(engine)

        # Verify data is cleared
        with factory() as session:
            result = session.execute(select(Source))
            sources = result.scalars().all()
            assert len(sources) == 0

    def test_init_database_idempotent(self, engine: Engine):
        """Test that init_database can be called multiple times safely."""
        # First initialization happens in fixture
        # Call again
        init_database(engine)
        init_database(engine)

        # Should still work
        factory = sessionmaker(bind=engine, expire_on_commit=False)

        with factory() as session:
            source = Source(name="test", source_type="csv")
            session.add(source)
            session.commit()

            result = session.execute(select(Source))
            sources = result.scalars().all()
            assert len(sources) == 1
