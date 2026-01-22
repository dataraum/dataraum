"""Tests for parallel statistical quality assessment."""

from datetime import UTC, datetime
from uuid import uuid4

import duckdb
import pytest
from sqlalchemy.orm import Session

from dataraum.analysis.statistics import assess_statistical_quality
from dataraum.storage import Column, Source, Table


@pytest.fixture
def test_duckdb(tmp_path):
    """Create file-based DuckDB with test data for statistical quality.

    Returns a connection that can coexist with parallel read-only connections.
    """
    db_path = str(tmp_path / "test_quality.duckdb")

    # Create and populate the database
    conn = duckdb.connect(db_path)

    # Create test table with numeric columns for quality assessment
    # Include Benford-like data (Fibonacci-ish) and uniform data
    conn.execute("""
        CREATE TABLE test_numeric AS
        SELECT
            i AS id,
            -- Benford-compliant: amounts that span orders of magnitude
            POWER(1.5, i % 30) * (1 + RANDOM()) AS amount,
            -- Uniform: won't follow Benford's Law
            (RANDOM() * 900 + 100)::INTEGER AS uniform_value,
            -- With outliers
            CASE
                WHEN i % 50 = 0 THEN (RANDOM() * 10000)::DOUBLE
                ELSE (RANDOM() * 100 + 50)::DOUBLE
            END AS with_outliers,
            -- Small variance (few outliers)
            (RANDOM() * 10 + 95)::DOUBLE AS stable_value
        FROM generate_series(1, 500) AS t(i)
    """)

    # Close setup connection so parallel workers can open read-only connections
    conn.close()

    # Return a fresh read-only connection
    conn = duckdb.connect(db_path, read_only=True)
    yield conn
    conn.close()


@pytest.fixture
def test_source(session: Session):
    """Create a test source."""
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
def test_table(session: Session, test_source: Source):
    """Create Table and Column records for statistical quality test."""
    table = Table(
        table_id=str(uuid4()),
        source_id=test_source.source_id,
        table_name="test_numeric",
        duckdb_path="test_numeric",
        layer="typed",
        row_count=500,
        created_at=datetime.now(UTC),
    )
    session.add(table)

    columns = []
    for name, dtype in [
        ("id", "INTEGER"),
        ("amount", "DOUBLE"),
        ("uniform_value", "INTEGER"),
        ("with_outliers", "DOUBLE"),
        ("stable_value", "DOUBLE"),
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


class TestAssessStatisticalQualityParallel:
    """Tests for parallel statistical quality assessment."""

    def test_assesses_all_numeric_columns(self, session, test_duckdb, test_table):
        """Test that all numeric columns are assessed."""
        result = assess_statistical_quality(
            table_id=test_table.table_id,
            duckdb_conn=test_duckdb,
            session=session,
            max_workers=4,
        )

        assert result.success
        quality_results = result.unwrap()

        # Should assess all 5 numeric columns (id, amount, uniform_value, with_outliers, stable_value)
        assert len(quality_results) == 5

        # Check that each result has the expected structure
        for qr in quality_results:
            assert qr.column_id is not None
            assert qr.column_ref is not None
            # Either benford_analysis or outlier_detection should be present
            # (benford requires 100+ rows, outlier detection always runs)

    def test_detects_outliers(self, session, test_duckdb, test_table):
        """Test that outliers are detected in the with_outliers column."""
        result = assess_statistical_quality(
            table_id=test_table.table_id,
            duckdb_conn=test_duckdb,
            session=session,
            max_workers=4,
        )

        assert result.success
        quality_results = result.unwrap()

        # Find the with_outliers column result
        outlier_result = next(
            (qr for qr in quality_results if qr.column_ref.column_name == "with_outliers"),
            None,
        )
        assert outlier_result is not None
        assert outlier_result.outlier_detection is not None
        # Should detect some outliers (we injected every 50th row as outlier)
        assert outlier_result.outlier_detection.iqr_outlier_count > 0

    def test_parallel_execution_uses_file_db(self, session, test_duckdb, test_table):
        """Test that parallel execution is used for file-based DBs."""
        # Verify we have a file-based DB
        db_info = test_duckdb.execute("PRAGMA database_list").fetchall()
        db_path = db_info[0][2] if db_info else ""
        assert db_path, "Expected file-based DuckDB"

        # Run assessment - should use parallel processing
        result = assess_statistical_quality(
            table_id=test_table.table_id,
            duckdb_conn=test_duckdb,
            session=session,
            max_workers=4,
        )

        assert result.success
        # Should return results for all numeric columns
        assert len(result.unwrap()) == 5

    def test_benford_analysis_runs(self, session, test_duckdb, test_table):
        """Test that Benford's Law analysis runs on applicable columns."""
        result = assess_statistical_quality(
            table_id=test_table.table_id,
            duckdb_conn=test_duckdb,
            session=session,
            max_workers=4,
        )

        assert result.success
        quality_results = result.unwrap()

        # Find a result with Benford analysis
        results_with_benford = [qr for qr in quality_results if qr.benford_analysis is not None]

        # At least some columns should have Benford analysis (500 rows > 100 threshold)
        assert len(results_with_benford) > 0

        # Check structure
        for qr in results_with_benford:
            assert qr.benford_analysis.chi_square is not None
            assert qr.benford_analysis.p_value is not None
            assert qr.benford_analysis.is_compliant is not None
