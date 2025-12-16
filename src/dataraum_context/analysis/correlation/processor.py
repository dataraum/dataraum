"""Correlation analysis processor.

Main orchestrator that runs all correlation analyses:
- Numeric correlations (Pearson, Spearman)
- Categorical associations (Cramér's V)
- Functional dependencies (A → B)
- Derived columns
- Multicollinearity (VIF, Tolerance, Condition Index)
"""

import time
from datetime import UTC, datetime
from uuid import uuid4

import duckdb
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dataraum_context.analysis.correlation.categorical import compute_categorical_associations
from dataraum_context.analysis.correlation.db_models import (
    MulticollinearityMetrics as DBMulticollinearityMetrics,
)
from dataraum_context.analysis.correlation.derived_columns import detect_derived_columns
from dataraum_context.analysis.correlation.functional_dependency import (
    detect_functional_dependencies,
)
from dataraum_context.analysis.correlation.models import CorrelationAnalysisResult
from dataraum_context.analysis.correlation.multicollinearity import (
    compute_multicollinearity_for_table,
)
from dataraum_context.analysis.correlation.numeric import compute_numeric_correlations
from dataraum_context.core.models.base import Result
from dataraum_context.storage import Column, Table


async def analyze_correlations(
    table_id: str,
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: AsyncSession,
    include_multicollinearity: bool = False,
) -> Result[CorrelationAnalysisResult]:
    """Run complete correlation analysis on a table.

    This orchestrates all correlation analyses:
    - Numeric correlations
    - Categorical associations
    - Functional dependencies
    - Derived columns
    - Multicollinearity (VIF, Tolerance, Condition Index) - optional, off by default

    Args:
        table_id: Table ID to analyze
        duckdb_conn: DuckDB connection
        session: SQLAlchemy session
        include_multicollinearity: Whether to run multicollinearity analysis.
            Disabled by default - should only be enabled after data cleaning
            (outlier handling, null imputation). See multicollinearity.py for details.

    Returns:
        Result containing CorrelationAnalysisResult
    """
    start_time = time.time()

    try:
        # Get table
        table = await session.get(Table, str(table_id))
        if not table:
            return Result.fail(f"Table not found: {table_id}")

        # Run all analyses
        numeric_corr_result = await compute_numeric_correlations(table, duckdb_conn, session)
        numeric_correlations = numeric_corr_result.unwrap() if numeric_corr_result.success else []

        categorical_assoc_result = await compute_categorical_associations(
            table, duckdb_conn, session
        )
        categorical_associations = (
            categorical_assoc_result.unwrap() if categorical_assoc_result.success else []
        )

        fd_result = await detect_functional_dependencies(table, duckdb_conn, session)
        functional_dependencies = fd_result.unwrap() if fd_result.success else []

        derived_result = await detect_derived_columns(table, duckdb_conn, session)
        derived_columns = derived_result.unwrap() if derived_result.success else []

        # Compute multicollinearity (VIF, Tolerance, Condition Index) - optional
        multicollinearity_analysis = None
        if include_multicollinearity:
            multicollinearity_result = await compute_multicollinearity_for_table(
                table, duckdb_conn, session
            )
            multicollinearity_analysis = (
                multicollinearity_result.value if multicollinearity_result.success else None
            )

        # Persist multicollinearity results (only if computed)
        if multicollinearity_analysis:
            db_multicollinearity = DBMulticollinearityMetrics(
                metric_id=str(uuid4()),
                table_id=table.table_id,
                computed_at=multicollinearity_analysis.computed_at,
                has_severe_multicollinearity=multicollinearity_analysis.has_severe_multicollinearity,
                num_problematic_columns=multicollinearity_analysis.num_problematic_columns,
                condition_index=(
                    multicollinearity_analysis.condition_index.condition_index
                    if multicollinearity_analysis.condition_index
                    else None
                ),
                max_vif=(
                    max(vif.vif for vif in multicollinearity_analysis.column_vifs)
                    if multicollinearity_analysis.column_vifs
                    else None
                ),
                analysis_data=multicollinearity_analysis.model_dump(mode="json"),
            )
            session.add(db_multicollinearity)
            await session.commit()

        # Summary stats
        stmt = select(Column).where(Column.table_id == table_id)
        result = await session.execute(stmt)
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
