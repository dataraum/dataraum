"""Numeric correlation analysis (Pearson & Spearman).

Orchestrates numeric correlation computation:
1. Loads numeric columns from database
2. Fetches data from DuckDB (with sampling for large tables)
3. Calls pure algorithm from algorithms/numeric.py
4. Stores results in database

Uses RESERVOIR sampling for tables with >100K rows to maintain performance.
For correlation analysis, 100K samples provide very accurate estimates
(standard error ~0.003 for r near 0).
"""

import logging
from datetime import UTC, datetime
from uuid import uuid4

import duckdb
import numpy as np
from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum_context.analysis.correlation.algorithms import (
    compute_pairwise_correlations,
)
from dataraum_context.analysis.correlation.db_models import (
    ColumnCorrelation as DBColumnCorrelation,
)
from dataraum_context.analysis.correlation.models import NumericCorrelation
from dataraum_context.core.models.base import Result
from dataraum_context.storage import Column, Table

logger = logging.getLogger(__name__)

# Sampling thresholds for numeric correlation
# Standard error for Pearson r is ~1/sqrt(n), so n=100K gives SE~0.003
LARGE_TABLE_THRESHOLD = 100_000
DEFAULT_SAMPLE_SIZE = 100_000


def compute_numeric_correlations(
    table: Table,
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: Session,
    min_correlation: float = 0.3,
    sample_size: int = DEFAULT_SAMPLE_SIZE,
) -> Result[list[NumericCorrelation]]:
    """Compute Pearson and Spearman correlations for all numeric column pairs.

    Uses RESERVOIR sampling for large tables (>100K rows) to maintain
    performance while preserving statistical validity.

    Args:
        table: Table to analyze
        duckdb_conn: DuckDB connection
        session: SQLAlchemy session
        min_correlation: Minimum |r| to store (reduces noise)
        sample_size: Maximum rows to sample for large tables (default 100K)

    Returns:
        Result containing list of NumericCorrelation objects
    """
    try:
        # Get numeric columns
        stmt = select(Column).where(
            Column.table_id == table.table_id,
            Column.resolved_type.in_(["INTEGER", "BIGINT", "DOUBLE", "DECIMAL"]),
        )
        result = session.execute(stmt)
        numeric_columns = list(result.scalars().all())

        if len(numeric_columns) < 2:
            return Result.ok([])  # Need at least 2 columns

        table_path = table.duckdb_path

        # Get row count for sampling decision
        row_count = table.row_count or 0
        if row_count == 0:
            count_result = duckdb_conn.execute(f"SELECT COUNT(*) FROM {table_path}").fetchone()
            row_count = count_result[0] if count_result else 0

        # Build column list for query
        col_names = [col.column_name for col in numeric_columns]
        col_select = ", ".join(f'TRY_CAST("{name}" AS DOUBLE) AS "{name}"' for name in col_names)

        # Use sampling for large tables
        use_sampling = row_count > LARGE_TABLE_THRESHOLD
        if use_sampling:
            actual_sample_size = min(sample_size, row_count)
            logger.info(
                f"Using RESERVOIR sampling ({actual_sample_size:,} rows) for "
                f"numeric correlations on {table.table_name} ({row_count:,} rows)"
            )
            query = f"""
                SELECT {col_select}
                FROM {table_path}
                USING SAMPLE reservoir({actual_sample_size} ROWS)
            """
        else:
            query = f"SELECT {col_select} FROM {table_path}"

        data_dict = duckdb_conn.execute(query).fetchnumpy()

        # Convert to 2D array (columns as variables)
        data_arrays = [data_dict[name] for name in col_names]
        data = np.column_stack(data_arrays)

        # Call pure algorithm
        algo_results = compute_pairwise_correlations(
            data, min_correlation=min_correlation, min_samples=10
        )

        # Convert algorithm results to domain models and store
        correlations = []
        computed_at = datetime.now(UTC)

        for algo_result in algo_results:
            col1 = numeric_columns[algo_result.col1_idx]
            col2 = numeric_columns[algo_result.col2_idx]

            correlation = NumericCorrelation(
                correlation_id=str(uuid4()),
                table_id=table.table_id,
                column1_id=col1.column_id,
                column2_id=col2.column_id,
                column1_name=col1.column_name,
                column2_name=col2.column_name,
                pearson_r=algo_result.pearson_r,
                pearson_p_value=algo_result.pearson_p,
                spearman_rho=algo_result.spearman_rho,
                spearman_p_value=algo_result.spearman_p,
                sample_size=algo_result.sample_size,
                computed_at=computed_at,
                correlation_strength=algo_result.strength,
                is_significant=algo_result.is_significant,
            )
            correlations.append(correlation)

            # Store in database
            db_corr = DBColumnCorrelation(
                correlation_id=correlation.correlation_id,
                table_id=correlation.table_id,
                column1_id=correlation.column1_id,
                column2_id=correlation.column2_id,
                pearson_r=correlation.pearson_r,
                pearson_p_value=correlation.pearson_p_value,
                spearman_rho=correlation.spearman_rho,
                spearman_p_value=correlation.spearman_p_value,
                sample_size=correlation.sample_size,
                computed_at=correlation.computed_at,
                correlation_strength=correlation.correlation_strength,
                is_significant=correlation.is_significant,
            )
            session.add(db_corr)

        return Result.ok(correlations)

    except Exception as e:
        return Result.fail(f"Numeric correlation computation failed: {e}")
