"""Categorical association analysis (Cramér's V).

Computes Cramér's V for categorical column pairs, based on the
chi-square test. Ranges from 0 (no association) to 1 (perfect association).
"""

from datetime import UTC, datetime
from uuid import uuid4

import duckdb
import numpy as np
from scipy import stats
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dataraum_context.analysis.correlation.db_models import (
    CategoricalAssociation as DBCategoricalAssociation,
)
from dataraum_context.analysis.correlation.models import CategoricalAssociation
from dataraum_context.core.models.base import Result
from dataraum_context.storage import Column, Table


async def compute_categorical_associations(
    table: Table,
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: AsyncSession,
    max_distinct_values: int = 100,
    min_cramers_v: float = 0.1,
) -> Result[list[CategoricalAssociation]]:
    """Compute Cramér's V for categorical column pairs.

    Cramér's V is based on chi-square test and ranges from 0 (no association)
    to 1 (perfect association).

    Args:
        table: Table to analyze
        duckdb_conn: DuckDB connection
        session: SQLAlchemy session
        max_distinct_values: Skip columns with too many values
        min_cramers_v: Minimum V to store

    Returns:
        Result containing list of CategoricalAssociation objects
    """
    try:
        # Get columns that could be categorical (low cardinality or VARCHAR)
        stmt = select(Column).where(Column.table_id == table.table_id)
        result = await session.execute(stmt)
        all_columns = result.scalars().all()

        # Filter to categorical candidates
        categorical_columns = []
        table_name = table.duckdb_path

        for col in all_columns:
            # Get distinct count
            query = f'SELECT COUNT(DISTINCT "{col.column_name}") FROM {table_name}'
            distinct_count_rows = duckdb_conn.execute(query).fetchone()

            if distinct_count_rows and 2 <= distinct_count_rows[0] <= max_distinct_values:
                categorical_columns.append(col)

        if len(categorical_columns) < 2:
            return Result.ok([])

        associations = []

        # Compute Cramér's V for all pairs
        for i, col1 in enumerate(categorical_columns):
            for col2 in categorical_columns[i + 1 :]:
                # Build contingency table using DuckDB
                query = f"""
                    SELECT
                        "{col1.column_name}" as val1,
                        "{col2.column_name}" as val2,
                        COUNT(*) as count
                    FROM {table_name}
                    WHERE
                        "{col1.column_name}" IS NOT NULL
                        AND "{col2.column_name}" IS NOT NULL
                    GROUP BY val1, val2
                """

                contingency_data = duckdb_conn.execute(query).fetchall()

                if len(contingency_data) < 4:
                    continue  # Need at least 2x2 table

                # Convert to contingency table
                val1_list = [row[0] for row in contingency_data]
                val2_list = [row[1] for row in contingency_data]
                count_list = [row[2] for row in contingency_data]

                # Create contingency table
                unique_val1 = sorted(set(val1_list))
                unique_val2 = sorted(set(val2_list))

                contingency = np.zeros((len(unique_val1), len(unique_val2)))
                val1_idx = {v: i for i, v in enumerate(unique_val1)}
                val2_idx = {v: i for i, v in enumerate(unique_val2)}

                for v1, v2, count in zip(val1_list, val2_list, count_list, strict=False):
                    contingency[val1_idx[v1], val2_idx[v2]] = count

                # Chi-square test
                chi2, p_value, dof, _expected = stats.chi2_contingency(contingency)

                # Cramér's V
                n = contingency.sum()
                min_dim = min(len(unique_val1), len(unique_val2)) - 1

                # Skip if min_dim is 0 (no variation to correlate)
                if min_dim == 0:
                    continue

                cramers_v = np.sqrt(chi2 / (n * min_dim))

                if cramers_v < min_cramers_v:
                    continue

                # Determine strength
                if cramers_v >= 0.5:
                    strength = "strong"
                elif cramers_v >= 0.3:
                    strength = "moderate"
                elif cramers_v >= 0.1:
                    strength = "weak"
                else:
                    strength = "none"

                computed_at = datetime.now(UTC)

                association = CategoricalAssociation(
                    association_id=str(uuid4()),
                    table_id=table.table_id,
                    column1_id=col1.column_id,
                    column2_id=col2.column_id,
                    column1_name=col1.column_name,
                    column2_name=col2.column_name,
                    cramers_v=float(cramers_v),
                    chi_square=float(chi2),
                    p_value=float(p_value),
                    degrees_of_freedom=int(dof),
                    sample_size=int(n),
                    computed_at=computed_at,
                    association_strength=strength,
                    is_significant=bool(p_value < 0.05),
                )

                associations.append(association)

                # Store in database
                db_assoc = DBCategoricalAssociation(
                    association_id=association.association_id,
                    table_id=association.table_id,
                    column1_id=association.column1_id,
                    column2_id=association.column2_id,
                    cramers_v=association.cramers_v,
                    chi_square=association.chi_square,
                    p_value=association.p_value,
                    degrees_of_freedom=association.degrees_of_freedom,
                    sample_size=association.sample_size,
                    computed_at=association.computed_at,
                    association_strength=association.association_strength,
                    is_significant=association.is_significant,
                )
                session.add(db_assoc)

        await session.commit()

        return Result.ok(associations)

    except Exception as e:
        return Result.fail(f"Categorical association computation failed: {e}")
