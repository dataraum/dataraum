"""Tests for cross-table quality analysis."""

import duckdb
import pytest

from dataraum.analysis.correlation.cross_table import analyze_relationship_quality
from dataraum.analysis.correlation.models import (
    CrossTableQualityResult,
    EnrichedRelationship,
)
from dataraum.core.models.base import RelationshipType


@pytest.fixture
def duckdb_conn():
    """Create in-memory DuckDB connection with test tables."""
    conn = duckdb.connect(":memory:")

    # Table A: orders with derived column (tax = amount * 0.1)
    conn.execute("""
        CREATE TABLE orders AS
        SELECT
            i AS order_id,
            (i % 10) + 1 AS customer_id,
            ROUND(RANDOM() * 1000, 2)::DOUBLE AS amount,
            ROUND(RANDOM() * 1000 * 0.1, 2)::DOUBLE AS tax
        FROM generate_series(1, 1000) AS t(i)
    """)
    # Make tax a derived column: tax = amount * 0.1
    conn.execute("UPDATE orders SET tax = ROUND(amount * 0.1, 2)")

    # Table B: customers with redundant column
    conn.execute("""
        CREATE TABLE customers AS
        SELECT
            i AS customer_id,
            'Customer ' || i AS name,
            ROUND(RANDOM() * 10000, 2)::DOUBLE AS credit_limit,
            ROUND(RANDOM() * 10000, 2)::DOUBLE AS credit_limit_copy
        FROM generate_series(1, 10) AS t(i)
    """)
    # Make credit_limit_copy redundant
    conn.execute("UPDATE customers SET credit_limit_copy = credit_limit")

    yield conn
    conn.close()


@pytest.fixture
def relationship():
    """Create test relationship."""
    return EnrichedRelationship(
        relationship_id="rel_1",
        from_table="orders",
        from_column="customer_id",
        from_column_id="col_1",
        from_table_id="tbl_1",
        to_table="customers",
        to_column="customer_id",
        to_column_id="col_2",
        to_table_id="tbl_2",
        relationship_type=RelationshipType.FOREIGN_KEY,
        confidence=0.95,
        detection_method="llm",
    )


def test_analyze_relationship_quality_basic(duckdb_conn, relationship):
    """Test basic cross-table quality analysis."""
    result = analyze_relationship_quality(
        relationship=relationship,
        duckdb_conn=duckdb_conn,
        from_table_path="orders",
        to_table_path="customers",
    )

    assert result is not None
    assert isinstance(result, CrossTableQualityResult)
    assert result.relationship_id == "rel_1"
    assert result.from_table == "orders"
    assert result.to_table == "customers"
    assert result.joined_row_count == 1000


def test_detects_derived_columns(duckdb_conn, relationship):
    """Test detection of derived columns (amount -> tax)."""
    result = analyze_relationship_quality(
        relationship=relationship,
        duckdb_conn=duckdb_conn,
        from_table_path="orders",
        to_table_path="customers",
    )

    assert result is not None

    # Should detect amount <-> tax as redundant/derived
    orders_redundant = [r for r in result.redundant_columns if r.table == "orders"]
    assert len(orders_redundant) >= 1

    amount_tax = next(
        (r for r in orders_redundant if {"amount", "tax"} == {r.column1, r.column2}), None
    )
    assert amount_tax is not None
    assert abs(amount_tax.correlation - 1.0) < 0.01  # Should be ~1.0


def test_detects_redundant_columns(duckdb_conn, relationship):
    """Test detection of redundant columns (credit_limit <-> credit_limit_copy)."""
    result = analyze_relationship_quality(
        relationship=relationship,
        duckdb_conn=duckdb_conn,
        from_table_path="orders",
        to_table_path="customers",
    )

    assert result is not None

    # Should detect credit_limit <-> credit_limit_copy
    customers_redundant = [r for r in result.redundant_columns if r.table == "customers"]
    assert len(customers_redundant) >= 1

    credit_pair = next(
        (
            r
            for r in customers_redundant
            if {"credit_limit", "credit_limit_copy"} == {r.column1, r.column2}
        ),
        None,
    )
    assert credit_pair is not None
    assert abs(credit_pair.correlation - 1.0) < 0.001  # Should be exactly 1.0


def test_detects_cross_table_join_correlation(duckdb_conn, relationship):
    """Test detection of join column correlation."""
    result = analyze_relationship_quality(
        relationship=relationship,
        duckdb_conn=duckdb_conn,
        from_table_path="orders",
        to_table_path="customers",
    )

    assert result is not None

    # Find the join column correlation
    join_corr = next((c for c in result.cross_table_correlations if c.is_join_column), None)
    assert join_corr is not None
    assert join_corr.from_column == "customer_id" or join_corr.to_column == "customer_id"
    assert abs(join_corr.pearson_r - 1.0) < 0.01  # Should be ~1.0 for FK join


def test_detects_multicollinearity(duckdb_conn, relationship):
    """Test multicollinearity detection."""
    result = analyze_relationship_quality(
        relationship=relationship,
        duckdb_conn=duckdb_conn,
        from_table_path="orders",
        to_table_path="customers",
    )

    assert result is not None
    assert result.overall_condition_index > 1.0  # Should detect some multicollinearity

    # Should have dependency groups
    assert len(result.dependency_groups) > 0


def test_quality_issues_generated(duckdb_conn, relationship):
    """Test that quality issues are generated."""
    result = analyze_relationship_quality(
        relationship=relationship,
        duckdb_conn=duckdb_conn,
        from_table_path="orders",
        to_table_path="customers",
    )

    assert result is not None
    assert len(result.issues) > 0

    # Should have redundant column warnings
    redundant_issues = [i for i in result.issues if i.issue_type == "redundant_column"]
    assert len(redundant_issues) >= 2  # amount/tax and credit_limit/credit_limit_copy


def test_returns_none_for_empty_tables(duckdb_conn):
    """Test that analysis returns None for empty tables."""
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE TABLE empty_a (id INTEGER, val DOUBLE)")
    conn.execute("CREATE TABLE empty_b (id INTEGER, other DOUBLE)")

    relationship = EnrichedRelationship(
        relationship_id="rel_empty",
        from_table="empty_a",
        from_column="id",
        from_column_id="col_1",
        from_table_id="tbl_1",
        to_table="empty_b",
        to_column="id",
        to_column_id="col_2",
        to_table_id="tbl_2",
        relationship_type=RelationshipType.FOREIGN_KEY,
        confidence=0.8,
        detection_method="test",
    )

    result = analyze_relationship_quality(
        relationship=relationship,
        duckdb_conn=conn,
        from_table_path="empty_a",
        to_table_path="empty_b",
    )

    # Should return None (not enough data)
    assert result is None
    conn.close()


def test_cross_table_dependency_groups(duckdb_conn, relationship):
    """Test that cross-table dependency groups are identified."""
    result = analyze_relationship_quality(
        relationship=relationship,
        duckdb_conn=duckdb_conn,
        from_table_path="orders",
        to_table_path="customers",
    )

    assert result is not None

    # The join column pair should create a cross-table dependency group
    cross_table_groups = result.cross_table_dependency_groups
    assert len(cross_table_groups) > 0

    for group in cross_table_groups:
        assert group.is_cross_table
        tables_in_group = {t for t, _ in group.columns}
        assert len(tables_in_group) > 1  # Multiple tables
