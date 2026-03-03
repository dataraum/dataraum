"""Tests for cross-table quality analysis (streamlined)."""

import duckdb
import pytest

from dataraum.analysis.correlation.cross_table import analyze_relationship_quality
from dataraum.analysis.correlation.models import (
    EnrichedRelationship,
)
from dataraum.core.models.base import RelationshipType


@pytest.fixture
def duckdb_conn():
    """Create in-memory DuckDB connection with cross-table correlation data."""
    conn = duckdb.connect(":memory:")

    # Table A: invoices
    conn.execute("""
        CREATE TABLE invoices AS
        SELECT
            i AS invoice_id,
            ROUND(100 + RANDOM() * 900, 2)::DOUBLE AS amount,
            ROUND(RANDOM() * 50, 2)::DOUBLE AS unrelated_metric
        FROM generate_series(1, 500) AS t(i)
    """)

    # Table B: payments — amount tracks invoices.amount closely
    conn.execute("""
        CREATE TABLE payments AS
        SELECT
            i AS payment_id,
            i AS invoice_id,
            0.0::DOUBLE AS amount,
            ROUND(RANDOM() * 100, 2)::DOUBLE AS fee
        FROM generate_series(1, 500) AS t(i)
    """)
    # Make payments.amount ≈ invoices.amount (near-perfect cross-table correlation)
    conn.execute("""
        UPDATE payments SET amount = (
            SELECT invoices.amount FROM invoices WHERE invoices.invoice_id = payments.invoice_id
        )
    """)

    yield conn
    conn.close()


@pytest.fixture
def relationship():
    """Create test relationship."""
    return EnrichedRelationship(
        relationship_id="rel_1",
        from_table="invoices",
        from_column="invoice_id",
        from_column_id="col_1",
        from_table_id="tbl_1",
        to_table="payments",
        to_column="invoice_id",
        to_column_id="col_2",
        to_table_id="tbl_2",
        relationship_type=RelationshipType.FOREIGN_KEY,
        confidence=0.95,
        detection_method="llm",
    )


class TestCrossTableOnly:
    """Test that only cross-table correlations are returned."""

    def test_result_has_no_within_table_fields(self, duckdb_conn, relationship):
        """CrossTableQualityResult should not have within-table fields."""
        result = analyze_relationship_quality(
            relationship=relationship,
            duckdb_conn=duckdb_conn,
            from_table_path="invoices",
            to_table_path="payments",
            min_correlation=0.3,
        )
        assert result is not None
        assert not hasattr(result, "redundant_columns")
        assert not hasattr(result, "derived_columns")


class TestCrossTableCorrelationDetection:
    """Test cross-table correlation detection."""

    def test_detects_strong_cross_table_correlation(self, duckdb_conn, relationship):
        """Should detect invoices.amount ≈ payments.amount as strong cross-table correlation."""
        result = analyze_relationship_quality(
            relationship=relationship,
            duckdb_conn=duckdb_conn,
            from_table_path="invoices",
            to_table_path="payments",
            min_correlation=0.5,
        )
        assert result is not None

        # Find the amount ↔ amount correlation
        amount_corrs = [
            c
            for c in result.cross_table_correlations
            if c.from_column == "amount" and c.to_column == "amount"
        ]
        assert len(amount_corrs) == 1
        assert abs(amount_corrs[0].pearson_r) > 0.99  # Near-perfect
        assert amount_corrs[0].strength == "very_strong"
        assert not amount_corrs[0].is_join_column

    def test_join_column_flagged(self, duckdb_conn, relationship):
        """Join columns should be flagged as is_join_column."""
        result = analyze_relationship_quality(
            relationship=relationship,
            duckdb_conn=duckdb_conn,
            from_table_path="invoices",
            to_table_path="payments",
            min_correlation=0.3,
        )
        assert result is not None

        join_corrs = [c for c in result.cross_table_correlations if c.is_join_column]
        assert len(join_corrs) > 0


class TestVDPOptional:
    """Test VDP multicollinearity is optional."""

    def test_vdp_skipped_when_disabled(self, duckdb_conn, relationship):
        """VDP should not run when compute_vdp=False (default)."""
        result = analyze_relationship_quality(
            relationship=relationship,
            duckdb_conn=duckdb_conn,
            from_table_path="invoices",
            to_table_path="payments",
            compute_vdp=False,
        )
        assert result is not None
        assert result.overall_condition_index == 1.0
        assert result.overall_severity == "none"
        assert len(result.dependency_groups) == 0

    def test_vdp_runs_when_enabled(self, duckdb_conn, relationship):
        """VDP should produce dependency groups when compute_vdp=True."""
        result = analyze_relationship_quality(
            relationship=relationship,
            duckdb_conn=duckdb_conn,
            from_table_path="invoices",
            to_table_path="payments",
            compute_vdp=True,
        )
        assert result is not None
        # With a near-perfect correlation, VDP should detect multicollinearity
        assert result.overall_condition_index > 1.0
        assert len(result.dependency_groups) > 0
