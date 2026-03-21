"""Integration tests for enriched views phase."""

import duckdb
import pytest

from dataraum.analysis.views.builder import DimensionJoin, build_enriched_view_sql
from dataraum.pipeline.phases.enriched_views_phase import EnrichedViewsPhase


class TestEnrichedViewsIntegration:
    """Integration tests for enriched views with DuckDB."""

    @pytest.fixture
    def duckdb_conn(self):
        conn = duckdb.connect(":memory:")
        yield conn
        conn.close()

    def test_view_creation_preserves_grain(self, duckdb_conn):
        """Test that enriched view preserves fact table row count."""
        # Create fact table
        duckdb_conn.execute("""
            CREATE TABLE typed_orders (
                order_id INTEGER,
                customer_id INTEGER,
                amount DOUBLE,
                order_date DATE
            )
        """)
        duckdb_conn.execute("""
            INSERT INTO typed_orders VALUES
                (1, 10, 100.0, '2024-01-01'),
                (2, 20, 200.0, '2024-01-02'),
                (3, 10, 150.0, '2024-01-03')
        """)

        # Create dimension table
        duckdb_conn.execute("""
            CREATE TABLE typed_customers (
                id INTEGER,
                name VARCHAR,
                country VARCHAR
            )
        """)
        duckdb_conn.execute("""
            INSERT INTO typed_customers VALUES
                (10, 'Alice', 'US'),
                (20, 'Bob', 'UK')
        """)

        # Build and execute view
        joins = [
            DimensionJoin(
                dim_table_name="customers",
                dim_duckdb_path="typed_customers",
                fact_fk_column="customer_id",
                dim_pk_column="id",
                include_columns=["name", "country"],
                relationship_id="rel-1",
            )
        ]

        view_name, sql, dim_cols = build_enriched_view_sql(
            fact_table_name="orders",
            fact_duckdb_path="typed_orders",
            dimension_joins=joins,
        )

        duckdb_conn.execute(sql)

        # Verify grain preserved (3 fact rows)
        result = duckdb_conn.execute(f'SELECT COUNT(*) FROM "{view_name}"').fetchone()
        assert result[0] == 3

        # Verify dimension columns present
        result = duckdb_conn.execute(f'SELECT * FROM "{view_name}" ORDER BY order_id').fetchall()
        assert result[0][4] == "Alice"  # customers__name
        assert result[0][5] == "US"  # customers__country
        assert result[1][4] == "Bob"  # customers__name

    def test_view_with_no_match_uses_null(self, duckdb_conn):
        """Test that LEFT JOIN produces NULLs for unmatched rows."""
        duckdb_conn.execute("""
            CREATE TABLE typed_orders (
                order_id INTEGER,
                customer_id INTEGER,
                amount DOUBLE
            )
        """)
        duckdb_conn.execute("""
            INSERT INTO typed_orders VALUES
                (1, 10, 100.0),
                (2, 99, 200.0)
        """)

        duckdb_conn.execute("""
            CREATE TABLE typed_customers (
                id INTEGER,
                name VARCHAR
            )
        """)
        duckdb_conn.execute("INSERT INTO typed_customers VALUES (10, 'Alice')")

        joins = [
            DimensionJoin(
                dim_table_name="customers",
                dim_duckdb_path="typed_customers",
                fact_fk_column="customer_id",
                dim_pk_column="id",
                include_columns=["name"],
            )
        ]

        _, sql, _ = build_enriched_view_sql(
            fact_table_name="orders",
            fact_duckdb_path="typed_orders",
            dimension_joins=joins,
        )

        duckdb_conn.execute(sql)

        # Grain preserved (2 rows)
        assert duckdb_conn.execute('SELECT COUNT(*) FROM "enriched_orders"').fetchone()[0] == 2

        # Unmatched customer_id=99 gets NULL
        # Column is named {fact_fk_column}__{dim_col} = customer_id__name
        result = duckdb_conn.execute(
            'SELECT "customer_id__name" FROM "enriched_orders" WHERE customer_id = 99'
        ).fetchone()
        assert result[0] is None


class TestEnrichedViewsPhaseProperties:
    """Tests for EnrichedViewsPhase static properties."""

    def test_skip_when_no_typed_tables(self, session):
        """Test skipping when no typed tables exist."""
        from dataraum.pipeline.base import PhaseContext
        from dataraum.storage import Source

        source = Source(name="test", source_type="csv")
        session.add(source)
        session.flush()

        ctx = PhaseContext(
            session=session,
            duckdb_conn=None,
            source_id=source.source_id,
        )

        phase = EnrichedViewsPhase()
        reason = phase.should_skip(ctx)
        assert reason == "No typed tables found"
