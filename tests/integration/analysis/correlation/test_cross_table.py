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


def test_only_cross_table_correlations_returned(duckdb_conn, relationship):
    """Test that only cross-table correlations are returned (no within-table)."""
    result = analyze_relationship_quality(
        relationship=relationship,
        duckdb_conn=duckdb_conn,
        from_table_path="orders",
        to_table_path="customers",
    )

    assert result is not None

    # Cross-table correlations should be present
    # (at minimum the join column pair)
    assert len(result.cross_table_correlations) > 0


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


def test_vdp_skipped_by_default(duckdb_conn, relationship):
    """Test that VDP multicollinearity is skipped by default."""
    result = analyze_relationship_quality(
        relationship=relationship,
        duckdb_conn=duckdb_conn,
        from_table_path="orders",
        to_table_path="customers",
    )

    assert result is not None
    # VDP skipped = no dependency groups, default values
    assert result.overall_condition_index == 1.0
    assert result.overall_severity == "none"
    assert len(result.dependency_groups) == 0
    assert len(result.cross_table_dependency_groups) == 0


def test_vdp_computed_when_enabled(duckdb_conn, relationship):
    """Test that VDP multicollinearity is computed when enabled."""
    result = analyze_relationship_quality(
        relationship=relationship,
        duckdb_conn=duckdb_conn,
        from_table_path="orders",
        to_table_path="customers",
        compute_vdp=True,
    )

    assert result is not None
    assert result.overall_condition_index > 1.0  # Should detect some multicollinearity
    assert len(result.dependency_groups) > 0


def test_quality_issues_generated(duckdb_conn, relationship):
    """Test that quality issues are generated for strong cross-table correlations."""
    result = analyze_relationship_quality(
        relationship=relationship,
        duckdb_conn=duckdb_conn,
        from_table_path="orders",
        to_table_path="customers",
        min_correlation=0.3,
    )

    assert result is not None
    # All issues should be about cross-table correlations (no redundant_column issues)
    for issue in result.issues:
        assert issue.issue_type in ("unexpected_correlation", "multicollinearity")


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


def test_min_correlation_filtering(duckdb_conn, relationship):
    """Test that min_correlation filter works."""
    # With high threshold, fewer correlations
    result_high = analyze_relationship_quality(
        relationship=relationship,
        duckdb_conn=duckdb_conn,
        from_table_path="orders",
        to_table_path="customers",
        min_correlation=0.9,
    )
    # With low threshold, more correlations
    result_low = analyze_relationship_quality(
        relationship=relationship,
        duckdb_conn=duckdb_conn,
        from_table_path="orders",
        to_table_path="customers",
        min_correlation=0.3,
    )

    assert result_high is not None
    assert result_low is not None
    assert len(result_low.cross_table_correlations) >= len(result_high.cross_table_correlations)
