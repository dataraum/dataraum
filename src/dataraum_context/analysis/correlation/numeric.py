"""Numeric correlation analysis (Pearson & Spearman).

Computes linear (Pearson) and monotonic (Spearman) correlations
between all pairs of numeric columns in a table.
"""

from datetime import UTC, datetime
from uuid import uuid4

import duckdb
import numpy as np
from scipy import stats
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

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
        numeric_columns = result.scalars().all()

        if len(numeric_columns) < 2:
            return Result.ok([])  # Need at least 2 columns

        correlations = []
        table_name = table.duckdb_path

        # Compute correlations for all pairs
        for i, col1 in enumerate(numeric_columns):
            for col2 in numeric_columns[i + 1 :]:  # Only upper triangle
                # Get values from DuckDB
                query = f"""
                    SELECT
                        TRY_CAST("{col1.column_name}" AS DOUBLE) as val1,
                        TRY_CAST("{col2.column_name}" AS DOUBLE) as val2
                    FROM {table_name}
                    WHERE
                        TRY_CAST("{col1.column_name}" AS DOUBLE) IS NOT NULL
                        AND TRY_CAST("{col2.column_name}" AS DOUBLE) IS NOT NULL
                """

                data = duckdb_conn.execute(query).fetchnumpy()
                val1 = data["val1"]
                val2 = data["val2"]

                if len(val1) < 10:
                    continue  # Not enough data

                # Pearson correlation
                pearson_r, pearson_p = stats.pearsonr(val1, val2)
                pearson_r_float = float(np.asarray(pearson_r).item())
                pearson_p_float = float(np.asarray(pearson_p).item())

                # Spearman correlation
                spearman_rho, spearman_p = stats.spearmanr(val1, val2)
                spearman_rho_float = float(np.asarray(spearman_rho).item())
                spearman_p_float = float(np.asarray(spearman_p).item())

                # Only store if above threshold
                if (
                    abs(pearson_r_float) < min_correlation
                    and abs(spearman_rho_float) < min_correlation
                ):
                    continue

                # Determine strength
                max_corr = max(abs(pearson_r_float), abs(spearman_rho_float))
                if max_corr >= 0.9:
                    strength = "very_strong"
                elif max_corr >= 0.7:
                    strength = "strong"
                elif max_corr >= 0.5:
                    strength = "moderate"
                elif max_corr >= 0.3:
                    strength = "weak"
                else:
                    strength = "none"

                is_significant = bool(min(pearson_p_float, spearman_p_float) < 0.05)

                computed_at = datetime.now(UTC)

                correlation = NumericCorrelation(
                    correlation_id=str(uuid4()),
                    table_id=table.table_id,
                    column1_id=col1.column_id,
                    column2_id=col2.column_id,
                    column1_name=col1.column_name,
                    column2_name=col2.column_name,
                    pearson_r=pearson_r_float,
                    pearson_p_value=pearson_p_float,
                    spearman_rho=spearman_rho_float,
                    spearman_p_value=spearman_p_float,
                    sample_size=len(val1),
                    computed_at=computed_at,
                    correlation_strength=strength,
                    is_significant=is_significant,
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
