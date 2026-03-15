"""Tests for enriched derived column detection (cross-table formulas)."""

from uuid import uuid4

import duckdb
import pytest
from sqlalchemy.orm import Session

from dataraum.analysis.correlation.within_table.derived_columns import (
    detect_enriched_derived_columns,
)
from dataraum.analysis.statistics.db_models import StatisticalProfile
from dataraum.analysis.views.db_models import EnrichedView
from dataraum.storage import Column, Table
from dataraum.storage.models import Source


@pytest.fixture
def enriched_duckdb():
    """DuckDB connection with a fact+dimension enriched view."""
    conn = duckdb.connect(":memory:")

    # Fact table: orders
    conn.execute("""
        CREATE TABLE typed_orders AS
        SELECT
            i AS order_id,
            (i % 5 + 1) AS quantity,
            ((i % 5 + 1) * (10.0 + (i % 3)))::DOUBLE AS total
        FROM generate_series(1, 200) AS t(i)
    """)

    # Dimension table: products (unit_price)
    conn.execute("""
        CREATE TABLE typed_products AS
        SELECT
            i AS product_id,
            (10.0 + (i % 3))::DOUBLE AS unit_price
        FROM generate_series(1, 200) AS t(i)
    """)

    # Enriched view: orders + products__unit_price
    # total = quantity * products__unit_price
    conn.execute("""
        CREATE VIEW enriched_orders AS
        SELECT
            o.order_id,
            o.quantity,
            o.total,
            p.unit_price AS products__unit_price
        FROM typed_orders o
        JOIN typed_products p ON o.order_id = p.product_id
    """)

    yield conn
    conn.close()


def _make_source(session: Session, source_id: str = "src1") -> None:
    """Create a Source record."""
    session.add(Source(source_id=source_id, name=source_id, source_type="csv"))
    session.flush()


def _make_table(session: Session, name: str, source_id: str = "src1") -> Table:
    t = Table(
        table_id=str(uuid4()),
        source_id=source_id,
        table_name=name,
        layer="typed",
        duckdb_path=f"typed_{name}",
        row_count=200,
    )
    session.add(t)
    session.flush()
    return t


def _make_column(
    session: Session, table: Table, name: str, resolved_type: str = "DOUBLE"
) -> Column:
    c = Column(
        column_id=str(uuid4()),
        table_id=table.table_id,
        column_name=name,
        column_position=0,
        raw_type="VARCHAR",
        resolved_type=resolved_type,
    )
    session.add(c)
    session.flush()
    return c


def _make_stat_profile(session: Session, column: Column, distinct_count: int = 10) -> None:
    sp = StatisticalProfile(
        profile_id=str(uuid4()),
        column_id=column.column_id,
        distinct_count=distinct_count,
        null_count=0,
        total_count=200,
        profile_data={},
    )
    session.add(sp)
    session.flush()


def _make_view_table(session: Session, name: str, source_id: str = "src1") -> Table:
    """Create an enriched-layer Table record for dimension columns."""
    t = Table(
        table_id=str(uuid4()),
        source_id=source_id,
        table_name=name,
        layer="enriched",
        duckdb_path=name,
        row_count=200,
    )
    session.add(t)
    session.flush()
    return t


def _make_enriched_view(
    session: Session,
    fact_table: Table,
    view_name: str,
    dimension_columns: list[str] | None,
    view_table: Table | None = None,
) -> EnrichedView:
    ev = EnrichedView(
        view_id=str(uuid4()),
        fact_table_id=fact_table.table_id,
        view_name=view_name,
        view_sql=f"CREATE VIEW {view_name} AS ...",
        dimension_columns=dimension_columns,
        view_table_id=view_table.table_id if view_table else None,
    )
    session.add(ev)
    session.flush()
    return ev


@pytest.fixture(autouse=True)
def _create_source(session):
    """Ensure a Source record exists for all tests."""
    _make_source(session)


class TestDetectsEnrichedDerivedColumns:
    """Tests for detect_enriched_derived_columns()."""

    def test_detects_cross_table_derivation(self, enriched_duckdb, session):
        """total = quantity * products__unit_price should be found."""
        table = _make_table(session, "orders")
        col_qty = _make_column(session, table, "quantity", "INTEGER")
        col_total = _make_column(session, table, "total", "DOUBLE")
        _make_stat_profile(session, col_qty)
        _make_stat_profile(session, col_total)

        # Register dimension column via view_table
        view_table = _make_view_table(session, "enriched_orders")
        dim_col = _make_column(session, view_table, "products__unit_price", "DOUBLE")
        _make_stat_profile(session, dim_col)

        ev = _make_enriched_view(
            session,
            table,
            "enriched_orders",
            ["products__unit_price"],
            view_table=view_table,
        )

        result = detect_enriched_derived_columns(ev, table, enriched_duckdb, session)
        assert result.success
        derived = result.unwrap()
        assert len(derived) >= 1

        # Find the cross-table derivation (source includes dim column's real ID)
        fact_col_ids = {col_qty.column_id, col_total.column_id}
        cross = [d for d in derived if any(sid not in fact_col_ids for sid in d.source_column_ids)]
        assert len(cross) >= 1
        assert cross[0].derived_column_name == "total"
        assert cross[0].derivation_type == "product"
        assert cross[0].match_rate >= 0.80

    def test_detects_same_table_derivation_via_view(self, session):
        """Same-table derivation still found when running on enriched view."""
        conn = duckdb.connect(":memory:")
        # a + b = c
        conn.execute("""
            CREATE VIEW enriched_test AS
            SELECT
                i AS id,
                i::DOUBLE AS col_a,
                (i * 2)::DOUBLE AS col_b,
                (i + i * 2)::DOUBLE AS col_c
            FROM generate_series(1, 200) AS t(i)
        """)

        table = _make_table(session, "test")
        col_a = _make_column(session, table, "col_a", "DOUBLE")
        col_b = _make_column(session, table, "col_b", "DOUBLE")
        col_c = _make_column(session, table, "col_c", "DOUBLE")
        _make_stat_profile(session, col_a)
        _make_stat_profile(session, col_b)
        _make_stat_profile(session, col_c)

        ev = _make_enriched_view(session, table, "enriched_test", [])

        result = detect_enriched_derived_columns(ev, table, conn, session)
        assert result.success
        derived = result.unwrap()
        assert len(derived) >= 1
        # col_c = col_a + col_b
        sums = [d for d in derived if d.derivation_type == "sum"]
        assert len(sums) >= 1
        conn.close()

    def test_no_dimension_columns_returns_empty_for_constants(self, session):
        """With no dimension columns and constant values, returns empty."""
        conn = duckdb.connect(":memory:")
        conn.execute("""
            CREATE VIEW enriched_nodim AS
            SELECT 1::DOUBLE AS col_a, 2::DOUBLE AS col_b
            FROM generate_series(1, 50) AS t(i)
        """)

        table = _make_table(session, "nodim")
        col_a = _make_column(session, table, "col_a", "DOUBLE")
        col_b = _make_column(session, table, "col_b", "DOUBLE")
        _make_stat_profile(session, col_a, distinct_count=1)
        _make_stat_profile(session, col_b, distinct_count=1)

        ev = _make_enriched_view(session, table, "enriched_nodim", None)

        result = detect_enriched_derived_columns(ev, table, conn, session)
        assert result.success
        assert result.unwrap() == []
        conn.close()

    def test_non_numeric_dim_columns_excluded(self, session):
        """VARCHAR dimension columns should not participate."""
        conn = duckdb.connect(":memory:")
        conn.execute("""
            CREATE VIEW enriched_varchar AS
            SELECT
                i::DOUBLE AS amount,
                'text' AS dim__name
            FROM generate_series(1, 50) AS t(i)
        """)

        table = _make_table(session, "varchar_test")
        col_amount = _make_column(session, table, "amount", "DOUBLE")
        _make_stat_profile(session, col_amount)

        # Register VARCHAR dimension column via view_table
        view_table = _make_view_table(session, "enriched_varchar")
        _make_column(session, view_table, "dim__name", "VARCHAR")

        ev = _make_enriched_view(
            session,
            table,
            "enriched_varchar",
            ["dim__name"],
            view_table=view_table,
        )

        result = detect_enriched_derived_columns(ev, table, conn, session)
        assert result.success
        # Only 1 numeric column + 0 numeric dim cols → not enough for triples
        assert result.unwrap() == []
        conn.close()

    def test_derived_column_id_is_fact_column(self, enriched_duckdb, session):
        """derived_column_id must always be a fact table Column ID."""
        table = _make_table(session, "orders")
        col_qty = _make_column(session, table, "quantity", "INTEGER")
        col_total = _make_column(session, table, "total", "DOUBLE")
        _make_stat_profile(session, col_qty)
        _make_stat_profile(session, col_total)

        view_table = _make_view_table(session, "enriched_orders")
        dim_col = _make_column(session, view_table, "products__unit_price", "DOUBLE")
        _make_stat_profile(session, dim_col)

        ev = _make_enriched_view(
            session,
            table,
            "enriched_orders",
            ["products__unit_price"],
            view_table=view_table,
        )

        result = detect_enriched_derived_columns(ev, table, enriched_duckdb, session)
        assert result.success
        fact_col_ids = {col_qty.column_id, col_total.column_id}
        for dc in result.unwrap():
            assert dc.derived_column_id in fact_col_ids

    def test_source_column_ids_include_dim_column(self, enriched_duckdb, session):
        """Cross-table sources should include the dimension column's real ID."""
        table = _make_table(session, "orders")
        col_qty = _make_column(session, table, "quantity", "INTEGER")
        col_total = _make_column(session, table, "total", "DOUBLE")
        _make_stat_profile(session, col_qty)
        _make_stat_profile(session, col_total)

        view_table = _make_view_table(session, "enriched_orders")
        dim_col = _make_column(session, view_table, "products__unit_price", "DOUBLE")
        _make_stat_profile(session, dim_col)

        ev = _make_enriched_view(
            session,
            table,
            "enriched_orders",
            ["products__unit_price"],
            view_table=view_table,
        )

        result = detect_enriched_derived_columns(ev, table, enriched_duckdb, session)
        assert result.success
        derived = result.unwrap()
        fact_col_ids = {col_qty.column_id, col_total.column_id}
        cross = [d for d in derived if any(sid not in fact_col_ids for sid in d.source_column_ids)]
        assert len(cross) >= 1
        # The dimension column's real column_id should appear in source_column_ids
        dim_ids = [sid for d in cross for sid in d.source_column_ids if sid not in fact_col_ids]
        assert dim_col.column_id in dim_ids

    def test_deduplication(self, session):
        """Algebraic equivalences should be deduplicated."""
        conn = duckdb.connect(":memory:")
        # z = x * y → x = z / y, y = z / x are the same relationship
        conn.execute("""
            CREATE VIEW enriched_dedup AS
            SELECT
                i::DOUBLE AS x,
                (i * 2)::DOUBLE AS y,
                (i * i * 2)::DOUBLE AS z
            FROM generate_series(1, 200) AS t(i)
        """)

        table = _make_table(session, "dedup")
        col_x = _make_column(session, table, "x", "DOUBLE")
        col_y = _make_column(session, table, "y", "DOUBLE")
        col_z = _make_column(session, table, "z", "DOUBLE")
        _make_stat_profile(session, col_x)
        _make_stat_profile(session, col_y)
        _make_stat_profile(session, col_z)

        ev = _make_enriched_view(session, table, "enriched_dedup", [])

        result = detect_enriched_derived_columns(ev, table, conn, session)
        assert result.success
        derived = result.unwrap()
        # Dedup should ensure only one entry per column triple
        col_sets = [frozenset([d.derived_column_id] + d.source_column_ids) for d in derived]
        assert len(col_sets) == len(set(col_sets))
        conn.close()
