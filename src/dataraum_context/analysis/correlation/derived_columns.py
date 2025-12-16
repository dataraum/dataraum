"""Derived column detection.

Detects columns that are derived from other columns:
- Arithmetic: col3 = col1 + col2, col1 - col2, col1 * col2, col1 / col2
- String transforms: col2 = UPPER(col1), LOWER(col1)
- Concatenation: col3 = col1 || col2
"""

from datetime import UTC, datetime
from uuid import uuid4

import duckdb
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dataraum_context.analysis.correlation.db_models import (
    DerivedColumn as DBDerivedColumn,
)
from dataraum_context.analysis.correlation.models import DerivedColumn
from dataraum_context.core.models.base import Result
from dataraum_context.storage import Column, Table


async def detect_derived_columns(
    table: Table,
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: AsyncSession,
    min_match_rate: float = 0.95,
) -> Result[list[DerivedColumn]]:
    """Detect columns that are derived from other columns.

    Checks for:
    - Arithmetic: col3 = col1 + col2, col1 - col2, col1 * col2, col1 / col2
    - String transforms: col2 = UPPER(col1), LOWER(col1)
    - Concatenation: col3 = col1 || col2

    Args:
        table: Table to analyze
        duckdb_conn: DuckDB connection
        session: AsyncSession
        min_match_rate: Minimum match rate to consider derived

    Returns:
        Result containing list of DerivedColumn objects
    """
    try:
        # Get columns
        stmt = select(Column).where(Column.table_id == table.table_id)
        result = await session.execute(stmt)
        columns = result.scalars().all()

        if len(columns) < 2:
            return Result.ok([])

        derived_columns = []
        table_name = table.duckdb_path

        # Check arithmetic derivations (numeric columns only)
        numeric_cols = [
            c for c in columns if c.resolved_type in ["INTEGER", "BIGINT", "DOUBLE", "DECIMAL"]
        ]

        for target in numeric_cols:
            for col1 in numeric_cols:
                for col2 in numeric_cols:
                    if target.column_id in [col1.column_id, col2.column_id]:
                        continue

                    # Check col_target = col1 op col2
                    for op, op_name, is_commutative in [
                        ("+", "sum", True),
                        ("-", "difference", False),
                        ("*", "product", True),
                        ("/", "ratio", False),
                    ]:
                        # For commutative ops, use canonical ordering to avoid duplicates
                        # e.g., only check A + B, not B + A
                        if is_commutative and col1.column_id > col2.column_id:
                            continue
                        query = f"""
                            WITH derivation_check AS (
                                SELECT
                                    TRY_CAST("{target.column_name}" AS DOUBLE) as target_val,
                                    TRY_CAST("{col1.column_name}" AS DOUBLE) as col1_val,
                                    TRY_CAST("{col2.column_name}" AS DOUBLE) as col2_val,
                                    ABS(
                                        TRY_CAST("{target.column_name}" AS DOUBLE) -
                                        (TRY_CAST("{col1.column_name}" AS DOUBLE) {op} TRY_CAST("{col2.column_name}" AS DOUBLE))
                                    ) as diff
                                FROM {table_name}
                                WHERE
                                    "{target.column_name}" IS NOT NULL
                                    AND "{col1.column_name}" IS NOT NULL
                                    AND "{col2.column_name}" IS NOT NULL
                            )
                            SELECT
                                COUNT(CASE WHEN diff < 0.01 THEN 1 END) as matches,
                                COUNT(*) as total
                            FROM derivation_check
                        """

                        deriv_result = duckdb_conn.execute(query).fetchone()
                        if not deriv_result:
                            continue
                        matches, total = deriv_result

                        if total == 0:
                            continue

                        match_rate = matches / total

                        if match_rate >= min_match_rate:
                            computed_at = datetime.now(UTC)

                            derived = DerivedColumn(
                                derived_id=str(uuid4()),
                                table_id=table.table_id,
                                derived_column_id=target.column_id,
                                derived_column_name=target.column_name,
                                source_column_ids=[col1.column_id, col2.column_id],
                                source_column_names=[col1.column_name, col2.column_name],
                                derivation_type=op_name,
                                formula=f"{col1.column_name} {op} {col2.column_name}",
                                match_rate=float(match_rate),
                                total_rows=int(total),
                                matching_rows=int(matches),
                                mismatch_examples=None,  # Could add samples
                                computed_at=computed_at,
                            )

                            derived_columns.append(derived)

                            # Store in database
                            db_derived = DBDerivedColumn(
                                derived_id=derived.derived_id,
                                table_id=derived.table_id,
                                derived_column_id=derived.derived_column_id,
                                source_column_ids=[col1.column_id, col2.column_id],
                                derivation_type=derived.derivation_type,
                                formula=derived.formula,
                                match_rate=derived.match_rate,
                                total_rows=derived.total_rows,
                                matching_rows=derived.matching_rows,
                                mismatch_examples=derived.mismatch_examples,
                                computed_at=derived.computed_at,
                            )
                            session.add(db_derived)

        await session.commit()

        return Result.ok(derived_columns)

    except Exception as e:
        return Result.fail(f"Derived column detection failed: {e}")
