"""Tests for within-table correlation analysis."""

from datetime import UTC, datetime
from uuid import uuid4

import duckdb
import pytest
from sqlalchemy.orm import Session

from dataraum.analysis.correlation.within_table import (
    compute_numeric_correlations,
    detect_derived_columns,
)
from dataraum.storage import Column, Source, Table


@pytest.fixture
def test_duckdb(tmp_path):
    """Create file-based DuckDB with test data.

    Returns a connection that can coexist with parallel read-only connections.
    Uses tmp_path so parallel workers can connect to the same database file.
    """
    db_path = str(tmp_path / "test_correlation.duckdb")

    # Create and populate the database
    conn = duckdb.connect(db_path)

    # Create test table with correlated numeric columns
    conn.execute("""
        CREATE TABLE test_numeric AS
        SELECT
            i AS id,
            (RANDOM() * 100)::DOUBLE AS col_a,
            (RANDOM() * 100)::DOUBLE AS col_b,
            0.0::DOUBLE AS col_c,
            0.0::DOUBLE AS col_d
        FROM generate_series(1, 100) AS t(i)
    """)
    # Make col_c perfectly correlated with col_a
    conn.execute("UPDATE test_numeric SET col_c = col_a * 2")
    # Make col_d = col_a + col_b (derived)
    conn.execute("UPDATE test_numeric SET col_d = col_a + col_b")

    # Close setup connection so parallel workers can open read-only connections
    conn.close()

    # Return a fresh read-only connection (compatible with parallel workers)
    conn = duckdb.connect(db_path, read_only=True)
    yield conn
    conn.close()


@pytest.fixture
def test_source(session: Session):
    """Create a test source for foreign key requirements."""
    source = Source(
        source_id=str(uuid4()),
        name="test_source",
        source_type="csv",
        connection_config={},
    )
    session.add(source)
    session.commit()
    return source


@pytest.fixture
def table_numeric(session: Session, test_source: Source):
    """Create Table and Column records for numeric test."""
    table = Table(
        table_id=str(uuid4()),
        source_id=test_source.source_id,
        table_name="test_numeric",
        duckdb_path="test_numeric",
        layer="typed",
        row_count=100,
        created_at=datetime.now(UTC),
    )
    session.add(table)

    columns = []
    for name, dtype in [
        ("id", "INTEGER"),
        ("col_a", "DOUBLE"),
        ("col_b", "DOUBLE"),
        ("col_c", "DOUBLE"),
        ("col_d", "DOUBLE"),
    ]:
        col = Column(
            column_id=str(uuid4()),
            table_id=table.table_id,
            column_name=name,
            column_position=len(columns),
            raw_type="VARCHAR",
            resolved_type=dtype,
        )
        columns.append(col)
        session.add(col)

    session.commit()
    return table


def test_compute_numeric_correlations(session, test_duckdb, table_numeric):
    """Test numeric correlation detection."""
    result = compute_numeric_correlations(table_numeric, test_duckdb, session, min_correlation=0.3)

    assert result.success
    correlations = result.unwrap()

    # Should find correlation between col_a and col_c (r ≈ 1.0)
    col_a_c = next(
        (c for c in correlations if {c.column1_name, c.column2_name} == {"col_a", "col_c"}),
        None,
    )
    assert col_a_c is not None
    assert abs(col_a_c.pearson_r - 1.0) < 0.01  # Perfect correlation


def test_detect_derived_columns(session, test_duckdb, table_numeric):
    """Test derived column detection."""
    result = detect_derived_columns(table_numeric, test_duckdb, session, min_match_rate=0.95)

    assert result.success
    derived = result.unwrap()

    # col_d = col_a + col_b should be detected
    col_d_sum = next(
        (
            d
            for d in derived
            if d.derived_column_name == "col_d"
            and d.derivation_type == "sum"
            and set(d.source_column_names) == {"col_a", "col_b"}
        ),
        None,
    )
    assert col_d_sum is not None
    assert col_d_sum.match_rate > 0.99  # Near perfect match


def test_no_correlations_with_insufficient_data(session, test_source):
    """Test that analysis handles tables with insufficient data."""
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE TABLE small_table (id INTEGER, val DOUBLE)")
    conn.execute("INSERT INTO small_table VALUES (1, 1.0), (2, 2.0)")

    table = Table(
        table_id=str(uuid4()),
        source_id=test_source.source_id,
        table_name="small_table",
        duckdb_path="small_table",
        layer="typed",
        row_count=2,
        created_at=datetime.now(UTC),
    )
    session.add(table)

    for i, (name, dtype) in enumerate([("id", "INTEGER"), ("val", "DOUBLE")]):
        col = Column(
            column_id=str(uuid4()),
            table_id=table.table_id,
            column_name=name,
            column_position=i,
            raw_type="VARCHAR",
            resolved_type=dtype,
        )
        session.add(col)

    session.commit()

    result = compute_numeric_correlations(table, conn, session)
    assert result.success
    # Should return empty list (not enough data points)
    assert len(result.unwrap()) == 0

    conn.close()


def test_zero_target_not_false_positive(session, test_source):
    """Zero-target rows must not inflate match rate via absolute tolerance.

    Regression test for GH issue: discount_amount=0 was falsely detected as
    derived from quantity/amount because ABS(0 - quantity/amount) < 0.01
    for any large denominator.
    """
    conn = duckdb.connect(":memory:")
    # 30 rows: discount_amount mostly 0, amount & quantity always non-zero
    conn.execute("""
        CREATE TABLE typed_invoices AS
        SELECT
            i AS id,
            (10000 + i * 1000)::BIGINT AS amount,
            (i % 5 + 1)::BIGINT AS quantity,
            (CASE WHEN i <= 4 THEN i * 8000 ELSE 0 END)::BIGINT AS discount_amount
        FROM generate_series(1, 30) AS t(i)
    """)

    table = Table(
        table_id=str(uuid4()),
        source_id=test_source.source_id,
        table_name="typed_invoices",
        duckdb_path="typed_invoices",
        layer="typed",
        row_count=30,
        created_at=datetime.now(UTC),
    )
    session.add(table)

    for i, (name, dtype) in enumerate(
        [
            ("id", "INTEGER"),
            ("amount", "BIGINT"),
            ("quantity", "BIGINT"),
            ("discount_amount", "BIGINT"),
        ]
    ):
        col = Column(
            column_id=str(uuid4()),
            table_id=table.table_id,
            column_name=name,
            column_position=i,
            raw_type="VARCHAR",
            resolved_type=dtype,
        )
        session.add(col)

    session.commit()

    result = detect_derived_columns(table, conn, session, min_match_rate=0.80)
    assert result.success
    derived = result.unwrap()

    # discount_amount must NOT be detected as derived from quantity / amount
    false_positive = next(
        (d for d in derived if d.derived_column_name == "discount_amount"),
        None,
    )
    assert false_positive is None, (
        f"discount_amount falsely detected as derived: "
        f"formula={false_positive.formula}, match_rate={false_positive.match_rate}"
    )
    conn.close()


def test_correlation_strength_classification(session, test_duckdb, table_numeric):
    """Test that correlation strength is correctly classified."""
    result = compute_numeric_correlations(table_numeric, test_duckdb, session, min_correlation=0.3)

    assert result.success
    correlations = result.unwrap()

    # Find the perfect correlation (col_a <-> col_c)
    perfect = next(
        (c for c in correlations if abs(c.pearson_r or 0) > 0.99),
        None,
    )
    assert perfect is not None
    assert perfect.correlation_strength == "very_strong"
