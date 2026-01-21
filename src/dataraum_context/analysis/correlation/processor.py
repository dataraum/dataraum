"""Correlation analysis processor.

Main orchestrator that runs all correlation analyses:

Within-table analysis (analyze_correlations):
- Numeric correlations (Pearson, Spearman)
- Categorical associations (Cramér's V)
- Functional dependencies (A → B)
- Derived columns

Cross-table quality analysis (analyze_cross_table_quality):
- Cross-table correlations between joined data
- Redundant/derived column detection
- Multicollinearity (VDP) analysis
- Requires confirmed relationships from semantic agent
"""

import math
import time
from datetime import UTC, datetime
from uuid import uuid4

import duckdb
from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum_context.analysis.correlation.cross_table import analyze_relationship_quality
from dataraum_context.analysis.correlation.db_models import (
    CorrelationAnalysisRun,
    CrossTableCorrelationDB,
    MulticollinearityGroup,
    QualityIssueDB,
)
from dataraum_context.analysis.correlation.models import (
    CorrelationAnalysisResult,
    CrossTableQualityResult,
    EnrichedRelationship,
)
from dataraum_context.analysis.correlation.within_table import (
    compute_categorical_associations,
    compute_numeric_correlations,
    detect_derived_columns,
    detect_functional_dependencies,
)
from dataraum_context.analysis.relationships.db_models import Relationship
from dataraum_context.core.models.base import Cardinality, RelationshipType, Result
from dataraum_context.storage import Column, Table


def analyze_correlations(
    table_id: str,
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: Session,
) -> Result[CorrelationAnalysisResult]:
    """Run complete correlation analysis on a table.

    This orchestrates all correlation analyses:
    - Numeric correlations
    - Categorical associations
    - Functional dependencies
    - Derived columns

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

        # Run all analyses
        numeric_corr_result = compute_numeric_correlations(table, duckdb_conn, session)
        numeric_correlations = numeric_corr_result.unwrap() if numeric_corr_result.success else []

        categorical_assoc_result = compute_categorical_associations(table, duckdb_conn, session)
        categorical_associations = (
            categorical_assoc_result.unwrap() if categorical_assoc_result.success else []
        )

        fd_result = detect_functional_dependencies(table, duckdb_conn, session)
        functional_dependencies = fd_result.unwrap() if fd_result.success else []

        derived_result = detect_derived_columns(table, duckdb_conn, session)
        derived_columns = derived_result.unwrap() if derived_result.success else []

        # Summary stats
        stmt = select(Column).where(Column.table_id == table_id)
        result = session.execute(stmt)
        total_columns = len(result.scalars().all())
        total_pairs = (total_columns * (total_columns - 1)) // 2

        significant_correlations = sum(1 for c in numeric_correlations if c.is_significant)
        strong_correlations = sum(
            1
            for c in numeric_correlations
            if max(abs(c.pearson_r or 0), abs(c.spearman_rho or 0)) > 0.7
        )

        duration = time.time() - start_time
        computed_at = datetime.now(UTC)

        analysis_result = CorrelationAnalysisResult(
            table_id=table_id,
            table_name=table.table_name,
            numeric_correlations=numeric_correlations,
            categorical_associations=categorical_associations,
            functional_dependencies=functional_dependencies,
            derived_columns=derived_columns,
            total_column_pairs=total_pairs,
            significant_correlations=significant_correlations,
            strong_correlations=strong_correlations,
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

    # Generate UUID explicitly (SQLAlchemy defaults may not work reliably in all contexts)
    run_id = str(uuid4())

    # Create analysis run record
    run_record = CorrelationAnalysisRun(
        run_id=run_id,
        target_id=relationship.relationship_id,
        target_type="relationship",
        from_table=from_table_name,
        to_table=to_table_name,
        join_column_from=from_column_name,
        join_column_to=to_column_name,
        rows_analyzed=quality_result.joined_row_count,
        columns_analyzed=quality_result.numeric_columns_analyzed,
        overall_condition_index=quality_result.overall_condition_index,
        overall_severity=quality_result.overall_severity,
        started_at=quality_result.analyzed_at,
        completed_at=now,
        duration_seconds=duration,
    )
    session.add(run_record)

    # Store cross-table correlations (skip NaN values from constant columns)
    for corr in quality_result.cross_table_correlations:
        # Skip correlations with NaN (happens when column is constant)
        if math.isnan(corr.pearson_r) or math.isnan(corr.spearman_rho):
            continue

        db_corr = CrossTableCorrelationDB(
            run_id=run_id,
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

    # Store multicollinearity groups (both within-table and cross-table)
    all_groups = quality_result.dependency_groups + quality_result.cross_table_dependency_groups
    for group in all_groups:
        columns_data = [
            {"table": t, "column": c, "vdp": group.variance_proportions.get((t, c), 0.0)}
            for t, c in group.columns
        ]
        db_group = MulticollinearityGroup(
            run_id=run_id,
            columns_involved=columns_data,
            condition_index=group.condition_index,
            severity=group.severity,
            is_cross_table=group.is_cross_table,
            computed_at=now,
        )
        session.add(db_group)

    # Store quality issues
    for issue in quality_result.issues:
        affected_cols = [{"table": t, "column": c} for t, c in issue.affected_columns]
        db_issue = QualityIssueDB(
            run_id=run_id,
            issue_type=issue.issue_type,
            severity=issue.severity,
            message=issue.message,
            affected_columns=affected_cols,
            detected_at=now,
        )
        session.add(db_issue)
