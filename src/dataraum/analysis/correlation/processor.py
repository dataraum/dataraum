"""Correlation analysis processor.

Main orchestrator that runs all correlation analyses:

Within-table analysis (analyze_correlations):
- Derived column detection (sum, product, ratio, etc.)
- Numeric correlations are computed on-demand only (not in pipeline)

Cross-table quality analysis (analyze_cross_table_quality):
- Cross-table correlations between joined data
- Redundant/derived column detection
- Multicollinearity (VDP) analysis
- Requires confirmed relationships from semantic agent
"""

import math
import time
from datetime import UTC, datetime

import duckdb
from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.analysis.correlation.cross_table import analyze_relationship_quality
from dataraum.analysis.correlation.db_models import (
    CrossTableCorrelationDB,
)
from dataraum.analysis.correlation.models import (
    CorrelationAnalysisResult,
    CrossTableQualityResult,
    EnrichedRelationship,
)
from dataraum.analysis.correlation.within_table import (
    detect_derived_columns,
)
from dataraum.analysis.relationships.db_models import Relationship
from dataraum.core.logging import get_logger
from dataraum.core.models.base import Cardinality, RelationshipType, Result
from dataraum.storage import Column, Table

logger = get_logger(__name__)


def analyze_correlations(
    table_id: str,
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: Session,
) -> Result[CorrelationAnalysisResult]:
    """Run correlation analysis on a table.

    Currently runs derived column detection only. Numeric correlations
    (Pearson, Spearman) are available via compute_numeric_correlations()
    for on-demand use but are not part of the pipeline — no downstream
    consumer acts on them.

    Args:
        table_id: Table ID to analyze
        duckdb_conn: DuckDB connection
        session: SQLAlchemy session

    Returns:
        Result containing CorrelationAnalysisResult
    """
    start_time = time.time()

    try:
        # Get table
        table = session.get(Table, str(table_id))
        if not table:
            return Result.fail(f"Table not found: {table_id}")

        derived_result = detect_derived_columns(table, duckdb_conn, session)
        derived_columns = derived_result.unwrap() if derived_result.success else []

        # Summary stats
        stmt = select(Column).where(Column.table_id == table_id)
        result = session.execute(stmt)
        total_columns = len(result.scalars().all())
        total_pairs = (total_columns * (total_columns - 1)) // 2

        duration = time.time() - start_time
        computed_at = datetime.now(UTC)

        analysis_result = CorrelationAnalysisResult(
            table_id=table_id,
            table_name=table.table_name,
            derived_columns=derived_columns,
            total_column_pairs=total_pairs,
            significant_correlations=0,
            strong_correlations=0,
            duration_seconds=duration,
            computed_at=computed_at,
        )

        return Result.ok(analysis_result)

    except Exception as e:
        return Result.fail(f"Correlation analysis failed: {e}")


def analyze_cross_table_quality(
    relationship: Relationship,
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: Session,
    min_correlation: float = 0.3,
    redundancy_threshold: float = 0.99,
) -> Result[CrossTableQualityResult]:
    """Analyze quality of a confirmed cross-table relationship.

    This runs after relationships are confirmed by semantic analysis.
    It joins the tables and analyzes:
    - Cross-table correlations (unexpected relationships)
    - Redundant columns (r ≈ 1.0 within same table)
    - Derived columns (one column computed from another)
    - Multicollinearity (VDP-based dependency groups)

    Args:
        relationship: Confirmed Relationship DB model
        duckdb_conn: DuckDB connection
        session: SQLAlchemy session
        min_correlation: Minimum |r| to report (default: 0.3)
        redundancy_threshold: Threshold for redundancy detection (default: 0.99)

    Returns:
        Result containing CrossTableQualityResult
    """
    try:
        # Load table metadata to get DuckDB paths
        from_table = session.get(Table, relationship.from_table_id)
        to_table = session.get(Table, relationship.to_table_id)
        from_column = session.get(Column, relationship.from_column_id)
        to_column = session.get(Column, relationship.to_column_id)

        if not all([from_table, to_table, from_column, to_column]):
            return Result.fail("Could not load relationship metadata")

        # Type narrowing - all objects are non-None after the check above
        assert from_table is not None
        assert to_table is not None
        assert from_column is not None
        assert to_column is not None

        # Verify DuckDB paths exist
        if not from_table.duckdb_path or not to_table.duckdb_path:
            return Result.fail("Tables missing DuckDB paths")

        # Convert cardinality string to enum (if present)
        cardinality = None
        if relationship.cardinality:
            try:
                cardinality = Cardinality(relationship.cardinality)
            except ValueError:
                pass  # Keep None if value doesn't match enum

        # Build EnrichedRelationship for the cross_table analyzer
        enriched = EnrichedRelationship(
            relationship_id=relationship.relationship_id,
            from_table=from_table.table_name,
            from_column=from_column.column_name,
            from_column_id=relationship.from_column_id,
            from_table_id=relationship.from_table_id,
            to_table=to_table.table_name,
            to_column=to_column.column_name,
            to_column_id=relationship.to_column_id,
            to_table_id=relationship.to_table_id,
            relationship_type=RelationshipType(relationship.relationship_type),
            cardinality=cardinality,
            confidence=relationship.confidence,
            detection_method=relationship.detection_method or "unknown",
            evidence=relationship.evidence or {},
        )

        # Run cross-table quality analysis
        start_time = time.time()
        quality_result = analyze_relationship_quality(
            relationship=enriched,
            duckdb_conn=duckdb_conn,
            from_table_path=from_table.duckdb_path,
            to_table_path=to_table.duckdb_path,
            min_correlation=min_correlation,
            redundancy_threshold=redundancy_threshold,
        )

        if quality_result is None:
            return Result.fail("Cross-table analysis returned no results (insufficient data)")

        duration = time.time() - start_time

        # Store results to DB
        _store_cross_table_results(
            session=session,
            relationship=relationship,
            quality_result=quality_result,
            from_table_name=from_table.table_name,
            to_table_name=to_table.table_name,
            from_column_name=from_column.column_name,
            to_column_name=to_column.column_name,
            duration=duration,
        )

        return Result.ok(quality_result)

    except Exception as e:
        return Result.fail(f"Cross-table quality analysis failed: {e}")


def _store_cross_table_results(
    session: Session,
    relationship: Relationship,
    quality_result: CrossTableQualityResult,
    from_table_name: str,
    to_table_name: str,
    from_column_name: str,
    to_column_name: str,
    duration: float,
) -> None:
    """Store cross-table quality analysis results to database."""
    now = datetime.now(UTC)

    # Store cross-table correlations (skip NaN values from constant columns)
    for corr in quality_result.cross_table_correlations:
        # Skip correlations with NaN (happens when column is constant)
        if math.isnan(corr.pearson_r) or math.isnan(corr.spearman_rho):
            continue

        db_corr = CrossTableCorrelationDB(
            relationship_id=relationship.relationship_id,
            from_table=corr.from_table,
            from_column=corr.from_column,
            to_table=corr.to_table,
            to_column=corr.to_column,
            pearson_r=corr.pearson_r,
            spearman_rho=corr.spearman_rho,
            strength=corr.strength,
            is_join_column=corr.is_join_column,
            computed_at=now,
        )
        session.add(db_corr)
