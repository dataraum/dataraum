"""Correlation analysis processor.

Main orchestrator that runs all correlation analyses:

Within-table analysis (analyze_correlations):
- Derived column detection (sum, product, ratio, etc.)

Cross-table quality analysis (analyze_cross_table_quality):
- Cross-table correlations between columns in different tables
- Requires confirmed relationships from semantic agent
"""

import time
from datetime import UTC, datetime

import duckdb
from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.analysis.correlation.cross_table import analyze_relationship_quality
from dataraum.analysis.correlation.db_models import CrossTableCorrelationRecord
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

    Runs derived column detection.

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
    min_correlation: float = 0.5,
    redundancy_threshold: float = 0.99,
    compute_vdp: bool = False,
) -> Result[CrossTableQualityResult]:
    """Analyze quality of a confirmed cross-table relationship.

    This runs after relationships are confirmed by semantic analysis.
    It joins the tables and analyzes cross-table correlations.

    Results are persisted as CrossTableCorrelationRecord entries.

    Args:
        relationship: Confirmed Relationship DB model
        duckdb_conn: DuckDB connection
        session: SQLAlchemy session
        min_correlation: Minimum |r| to report (default: 0.5)
        redundancy_threshold: Threshold for redundancy detection (default: 0.99)
        compute_vdp: Whether to compute VDP multicollinearity (default: False)

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
        quality_result = analyze_relationship_quality(
            relationship=enriched,
            duckdb_conn=duckdb_conn,
            from_table_path=from_table.duckdb_path,
            to_table_path=to_table.duckdb_path,
            min_correlation=min_correlation,
            redundancy_threshold=redundancy_threshold,
            compute_vdp=compute_vdp,
        )

        if quality_result is None:
            return Result.fail("Cross-table analysis returned no results (insufficient data)")

        # Persist cross-table correlations
        # Build column name -> column_id lookup for both tables
        col_name_to_id: dict[tuple[str, str], str] = {}  # (table_name, col_name) -> col_id
        for tbl in [from_table, to_table]:
            cols_stmt = select(Column).where(Column.table_id == tbl.table_id)
            for col in session.execute(cols_stmt).scalars().all():
                col_name_to_id[(tbl.table_name, col.column_name)] = col.column_id

        # Table name -> table_id
        table_name_to_id = {
            from_table.table_name: from_table.table_id,
            to_table.table_name: to_table.table_id,
        }

        records_created = 0
        for ctc in quality_result.cross_table_correlations:
            from_col_id = col_name_to_id.get((ctc.from_table, ctc.from_column))
            to_col_id = col_name_to_id.get((ctc.to_table, ctc.to_column))
            from_tbl_id = table_name_to_id.get(ctc.from_table)
            to_tbl_id = table_name_to_id.get(ctc.to_table)

            if not all([from_col_id, to_col_id, from_tbl_id, to_tbl_id]):
                logger.warning(
                    "cross_table_correlation_missing_ids",
                    from_table=ctc.from_table,
                    from_column=ctc.from_column,
                    to_table=ctc.to_table,
                    to_column=ctc.to_column,
                )
                continue

            record = CrossTableCorrelationRecord(
                relationship_id=relationship.relationship_id,
                from_table_id=from_tbl_id,
                from_column_id=from_col_id,
                to_table_id=to_tbl_id,
                to_column_id=to_col_id,
                pearson_r=ctc.pearson_r,
                spearman_rho=ctc.spearman_rho,
                strength=ctc.strength,
                is_join_column=ctc.is_join_column,
            )
            session.add(record)
            records_created += 1

        logger.info(
            "cross_table_correlations_persisted",
            relationship_id=relationship.relationship_id,
            correlations_found=len(quality_result.cross_table_correlations),
            records_created=records_created,
        )

        return Result.ok(quality_result)

    except Exception as e:
        return Result.fail(f"Cross-table quality analysis failed: {e}")
