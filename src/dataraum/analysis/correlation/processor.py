"""Correlation analysis processor.

Runs all correlation analyses:

Within-table analysis (analyze_correlations):
- Derived column detection (sum, product, ratio, etc.)

Enriched view analysis (analyze_enriched_correlations):
- Same-table + cross-table derived columns via enriched views
"""

from __future__ import annotations

import time
from datetime import UTC, datetime

import duckdb
from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.analysis.correlation.models import (
    CorrelationAnalysisResult,
)
from dataraum.analysis.correlation.within_table import (
    detect_derived_columns,
    detect_enriched_derived_columns,
)
from dataraum.core.logging import get_logger
from dataraum.core.models.base import Result
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


def analyze_enriched_correlations(
    enriched_view: object,
    fact_table: Table,
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: Session,
) -> Result[CorrelationAnalysisResult]:
    """Run correlation analysis on an enriched view (fact + dimension columns).

    The enriched view is a superset of the typed table, so this finds both
    same-table and cross-table derived columns in a single pass.

    Args:
        enriched_view: EnrichedView DB model.
        fact_table: The fact Table this view is based on.
        duckdb_conn: DuckDB connection.
        session: SQLAlchemy session.

    Returns:
        Result containing CorrelationAnalysisResult.
    """
    start_time = time.time()

    try:
        derived_result = detect_enriched_derived_columns(
            enriched_view, fact_table, duckdb_conn, session
        )
        derived_columns = derived_result.unwrap() if derived_result.success else []

        # Summary stats
        stmt = select(Column).where(Column.table_id == fact_table.table_id)
        total_columns = len(session.execute(stmt).scalars().all())
        total_pairs = (total_columns * (total_columns - 1)) // 2

        duration = time.time() - start_time
        computed_at = datetime.now(UTC)

        analysis_result = CorrelationAnalysisResult(
            table_id=fact_table.table_id,
            table_name=fact_table.table_name,
            derived_columns=derived_columns,
            total_column_pairs=total_pairs,
            significant_correlations=0,
            strong_correlations=0,
            duration_seconds=duration,
            computed_at=computed_at,
        )

        return Result.ok(analysis_result)

    except Exception as e:
        return Result.fail(f"Enriched correlation analysis failed: {e}")
