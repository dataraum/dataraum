"""Tests for within-table correlation analysis."""

from datetime import UTC, datetime
from uuid import uuid4

import duckdb
import pytest
from sqlalchemy.orm import Session

from dataraum.analysis.correlation.within_table import (
    compute_categorical_associations,
    compute_numeric_correlations,
    detect_derived_columns,
    detect_functional_dependencies,
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

    # Create test table with categorical columns
    # cat1 and cat2 are strongly associated but not perfectly 1:1
    # We need at least a 2x2 contingency table (4+ cells with data)
    conn.execute("""
        CREATE TABLE test_categorical AS
        SELECT
            i AS id,
            CASE WHEN i % 3 = 0 THEN 'A' WHEN i % 3 = 1 THEN 'B' ELSE 'C' END AS cat1,
            CASE
                WHEN i % 3 = 0 AND i % 2 = 0 THEN 'X'
                WHEN i % 3 = 0 AND i % 2 = 1 THEN 'Y'
                WHEN i % 3 = 1 AND i % 2 = 0 THEN 'X'
                WHEN i % 3 = 1 AND i % 2 = 1 THEN 'Y'
                ELSE 'Z'
            END AS cat2,
            CASE WHEN i % 2 = 0 THEN 'even' ELSE 'odd' END AS cat3
        FROM generate_series(1, 100) AS t(i)
    """)

    # Create test table with functional dependencies
    conn.execute("""
        CREATE TABLE test_fd AS
        SELECT
            i AS id,
            'code_' || (i % 10) AS code,
            'name_' || (i % 10) AS name,
            (RANDOM() * 100)::DOUBLE AS value
        FROM generate_series(1, 100) AS t(i)
    """)
    # code -> name is a functional dependency (same code always has same name)

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


@pytest.fixture
def table_categorical(session: Session, test_source: Source):
    """Create Table and Column records for categorical test."""
    table = Table(
        table_id=str(uuid4()),
        source_id=test_source.source_id,
        table_name="test_categorical",
        duckdb_path="test_categorical",
        layer="typed",
        row_count=100,
        created_at=datetime.now(UTC),
    )
    session.add(table)

    columns = []
    for name, dtype in [
        ("id", "INTEGER"),
        ("cat1", "VARCHAR"),
        ("cat2", "VARCHAR"),
        ("cat3", "VARCHAR"),
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


@pytest.fixture
def table_fd(session: Session, test_source: Source):
    """Create Table and Column records for functional dependency test."""
    table = Table(
        table_id=str(uuid4()),
        source_id=test_source.source_id,
        table_name="test_fd",
        duckdb_path="test_fd",
        layer="typed",
        row_count=100,
        created_at=datetime.now(UTC),
    )
    session.add(table)

    columns = []
    for name, dtype in [
        ("id", "INTEGER"),
        ("code", "VARCHAR"),
        ("name", "VARCHAR"),
        ("value", "DOUBLE"),
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


def test_compute_categorical_associations(session, test_duckdb, table_categorical):
    """Test categorical association detection."""
    result = compute_categorical_associations(
        table_categorical, test_duckdb, session, min_cramers_v=0.1
    )

    assert result.success
    associations = result.unwrap()

    # cat1 and cat2 have strong association (A,B→X/Y, C→Z pattern)
    cat1_cat2 = next(
        (a for a in associations if {a.column1_name, a.column2_name} == {"cat1", "cat2"}),
        None,
    )
    assert cat1_cat2 is not None
    assert cat1_cat2.cramers_v > 0.3  # Moderate to strong association


def test_detect_functional_dependencies(session, test_duckdb, table_fd):
    """Test functional dependency detection."""
    result = detect_functional_dependencies(table_fd, test_duckdb, session, min_confidence=0.95)

    assert result.success
    dependencies = result.unwrap()

    # code -> name should be detected (each code has one name)
    code_name = next(
        (
            fd
            for fd in dependencies
            if fd.determinant_column_names == ["code"] and fd.dependent_column_name == "name"
        ),
        None,
    )
    assert code_name is not None
    assert code_name.confidence == 1.0  # Exact FD


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
