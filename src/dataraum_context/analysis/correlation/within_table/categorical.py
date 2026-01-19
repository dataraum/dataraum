"""Categorical association analysis (Cramér's V).

Orchestrates categorical association computation:
1. Loads categorical columns from database
2. Fetches data from DuckDB
3. Calls pure algorithm from algorithms/categorical.py
4. Stores results in database
"""

from datetime import UTC, datetime
from uuid import uuid4

import duckdb
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dataraum_context.analysis.correlation.algorithms import (
    compute_cramers_v,
)
from dataraum_context.analysis.correlation.algorithms.categorical import (
    build_contingency_table,
)
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
        max_distinct_values: Skip columns with too many distinct values
        min_cramers_v: Minimum V to store

    Returns:
        Result containing list of CategoricalAssociation objects
    """
    try:
        # Get columns that could be categorical (VARCHAR or BOOLEAN)
        stmt = select(Column).where(Column.table_id == table.table_id)
        result = await session.execute(stmt)
        all_columns = result.scalars().all()

        # Filter to categorical candidates
        categorical_columns = []
        table_path = table.duckdb_path

        for col in all_columns:
            # Only include categorical types
            if col.resolved_type not in ("VARCHAR", "BOOLEAN"):
                continue

            # Get distinct count
            query = f'SELECT COUNT(DISTINCT "{col.column_name}") FROM {table_path}'
            distinct_count_result = duckdb_conn.execute(query).fetchone()

            if distinct_count_result and 2 <= distinct_count_result[0] <= max_distinct_values:
                categorical_columns.append(col)

        if len(categorical_columns) < 2:
            return Result.ok([])

        associations = []
        computed_at = datetime.now(UTC)

        # Compute Cramér's V for all pairs
        for i, col1 in enumerate(categorical_columns):
            for j, col2 in enumerate(categorical_columns[i + 1 :], start=i + 1):
                # Fetch paired values from DuckDB
                query = f"""
                    SELECT "{col1.column_name}", "{col2.column_name}"
                    FROM {table_path}
                    WHERE "{col1.column_name}" IS NOT NULL
                      AND "{col2.column_name}" IS NOT NULL
                """
                rows = duckdb_conn.execute(query).fetchall()

                if len(rows) < 10:
                    continue

                col1_values = [row[0] for row in rows]
                col2_values = [row[1] for row in rows]

                # Build contingency table and compute Cramér's V
                contingency = build_contingency_table(col1_values, col2_values)
                algo_result = compute_cramers_v(contingency, col1_idx=i, col2_idx=j)

                if algo_result is None:
                    continue

                if algo_result.cramers_v < min_cramers_v:
                    continue

                association = CategoricalAssociation(
                    association_id=str(uuid4()),
                    table_id=table.table_id,
                    column1_id=col1.column_id,
                    column2_id=col2.column_id,
                    column1_name=col1.column_name,
                    column2_name=col2.column_name,
                    cramers_v=algo_result.cramers_v,
                    chi_square=algo_result.chi_square,
                    p_value=algo_result.p_value,
                    degrees_of_freedom=algo_result.degrees_of_freedom,
                    sample_size=algo_result.sample_size,
                    computed_at=computed_at,
                    association_strength=algo_result.strength,
                    is_significant=algo_result.is_significant,
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

        return Result.ok(associations)

    except Exception as e:
        return Result.fail(f"Categorical association computation failed: {e}")
