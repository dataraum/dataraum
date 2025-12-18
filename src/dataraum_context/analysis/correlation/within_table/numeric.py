"""Numeric correlation analysis (Pearson & Spearman).

Orchestrates numeric correlation computation:
1. Loads numeric columns from database
2. Fetches data from DuckDB
3. Calls pure algorithm from algorithms/numeric.py
4. Stores results in database
"""

from datetime import UTC, datetime
from uuid import uuid4

import duckdb
import numpy as np
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dataraum_context.analysis.correlation.algorithms import (
    compute_pairwise_correlations,
)
from dataraum_context.analysis.correlation.db_models import (
    ColumnCorrelation as DBColumnCorrelation,
)
from dataraum_context.analysis.correlation.models import NumericCorrelation
from dataraum_context.core.models.base import Result
from dataraum_context.storage import Column, Table


async def compute_numeric_correlations(
    table: Table,
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: AsyncSession,
    min_correlation: float = 0.3,
) -> Result[list[NumericCorrelation]]:
    """Compute Pearson and Spearman correlations for all numeric column pairs.

    Args:
        table: Table to analyze
        duckdb_conn: DuckDB connection
        session: SQLAlchemy session
        min_correlation: Minimum |r| to store (reduces noise)

    Returns:
        Result containing list of NumericCorrelation objects
    """
    try:
        # Get numeric columns
        stmt = select(Column).where(
            Column.table_id == table.table_id,
            Column.resolved_type.in_(["INTEGER", "BIGINT", "DOUBLE", "DECIMAL"]),
        )
        result = await session.execute(stmt)
        numeric_columns = list(result.scalars().all())

        if len(numeric_columns) < 2:
            return Result.ok([])  # Need at least 2 columns

        table_path = table.duckdb_path

        # Build column list for query
        col_names = [col.column_name for col in numeric_columns]
        col_select = ", ".join(f'TRY_CAST("{name}" AS DOUBLE) AS "{name}"' for name in col_names)

        # Fetch all numeric data at once
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

        await session.commit()

        return Result.ok(correlations)

    except Exception as e:
        return Result.fail(f"Numeric correlation computation failed: {e}")
