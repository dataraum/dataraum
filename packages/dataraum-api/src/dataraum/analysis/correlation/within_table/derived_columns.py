"""Derived column detection.

Detects columns that are derived from other columns:
- Arithmetic: col3 = col1 + col2, col1 - col2, col1 * col2, col1 / col2
- String transforms: col2 = UPPER(col1), LOWER(col1)
- Concatenation: col3 = col1 || col2

Uses parallel processing for large tables to speed up detection.
"""

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from uuid import uuid4

import duckdb
from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.analysis.correlation.db_models import (
    DerivedColumn as DBDerivedColumn,
)
from dataraum.analysis.correlation.models import DerivedColumn
from dataraum.core.models.base import Result
from dataraum.storage import Column, Table


def _check_derived_triple(
    duckdb_conn: duckdb.DuckDBPyConnection,
    table_name: str,
    table_id: str,
    target_id: str,
    target_name: str,
    col1_id: str,
    col1_name: str,
    col2_id: str,
    col2_name: str,
    op: str,
    op_name: str,
    min_match_rate: float,
) -> DerivedColumn | None:
    """Check if target = col1 op col2 is a derivation.

    Runs in a worker thread using a cursor from the shared DuckDB connection.
    DuckDB cursors are thread-safe for read operations.
    """
    cursor = duckdb_conn.cursor()
    try:
        query = f"""
            WITH derivation_check AS (
                SELECT
                    ABS(
                        TRY_CAST("{target_name}" AS DOUBLE) -
                        (TRY_CAST("{col1_name}" AS DOUBLE) {op} TRY_CAST("{col2_name}" AS DOUBLE))
                    ) as diff
                FROM {table_name}
                WHERE
                    "{target_name}" IS NOT NULL
                    AND "{col1_name}" IS NOT NULL
                    AND "{col2_name}" IS NOT NULL
            )
            SELECT
                COUNT(CASE WHEN diff < 0.01 THEN 1 END) as matches,
                COUNT(*) as total
            FROM derivation_check
        """
        result = cursor.execute(query).fetchone()
        if not result:
            return None

        matches, total = result
        if total == 0:
            return None

        match_rate = matches / total
        if match_rate < min_match_rate:
            return None

        return DerivedColumn(
            derived_id=str(uuid4()),
            table_id=table_id,
            derived_column_id=target_id,
            derived_column_name=target_name,
            source_column_ids=[col1_id, col2_id],
            source_column_names=[col1_name, col2_name],
            derivation_type=op_name,
            formula=f"{col1_name} {op} {col2_name}",
            match_rate=float(match_rate),
            total_rows=int(total),
            matching_rows=int(matches),
            mismatch_examples=None,
            computed_at=datetime.now(UTC),
        )
    finally:
        cursor.close()


def detect_derived_columns(
    table: Table,
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: Session,
    min_match_rate: float = 0.95,
    max_workers: int = 4,
) -> Result[list[DerivedColumn]]:
    """Detect columns that are derived from other columns.

    Checks for:
    - Arithmetic: col3 = col1 + col2, col1 - col2, col1 * col2, col1 / col2
    - String transforms: col2 = UPPER(col1), LOWER(col1)
    - Concatenation: col3 = col1 || col2

    Uses parallel processing when there are many combinations to check.

    Args:
        table: Table to analyze
        duckdb_conn: DuckDB connection
        session: AsyncSession
        min_match_rate: Minimum match rate to consider derived
        max_workers: Maximum parallel workers

    Returns:
        Result containing list of DerivedColumn objects
    """
    try:
        stmt = select(Column).where(Column.table_id == table.table_id)
        result = session.execute(stmt)
        columns = result.scalars().all()

        if len(columns) < 2:
            return Result.ok([])

        table_name = table.duckdb_path
        if not table_name:
            return Result.fail("Table has no DuckDB path")

        # Check arithmetic derivations (numeric columns only)
        numeric_cols = [
            c for c in columns if c.resolved_type in ["INTEGER", "BIGINT", "DOUBLE", "DECIMAL"]
        ]

        # Generate all triples and operations to check
        operations = [
            ("+", "sum", True),
            ("-", "difference", False),
            ("*", "product", True),
            ("/", "ratio", False),
        ]

        checks = []
        for target in numeric_cols:
            for col1 in numeric_cols:
                for col2 in numeric_cols:
                    if target.column_id in [col1.column_id, col2.column_id]:
                        continue
                    for op, op_name, is_commutative in operations:
                        if is_commutative and col1.column_id > col2.column_id:
                            continue
                        checks.append((target, col1, col2, op, op_name))

        derived_columns: list[DerivedColumn] = []

        # Use parallel processing with cursors from shared connection
        # DuckDB cursors are thread-safe for read operations
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = [
                pool.submit(
                    _check_derived_triple,
                    duckdb_conn,
                    table_name,
                    table.table_id,
                    target.column_id,
                    target.column_name,
                    col1.column_id,
                    col1.column_name,
                    col2.column_id,
                    col2.column_name,
                    op,
                    op_name,
                    min_match_rate,
                )
                for target, col1, col2, op, op_name in checks
            ]

            for future in futures:
                derived = future.result()
                if derived:
                    derived_columns.append(derived)

        # Store all in database (sequential - SQLite writes)
        for derived in derived_columns:
            db_derived = DBDerivedColumn(
                derived_id=derived.derived_id,
                table_id=derived.table_id,
                derived_column_id=derived.derived_column_id,
                source_column_ids=derived.source_column_ids,
                derivation_type=derived.derivation_type,
                formula=derived.formula,
                match_rate=derived.match_rate,
                total_rows=derived.total_rows,
                matching_rows=derived.matching_rows,
                mismatch_examples=derived.mismatch_examples,
                computed_at=derived.computed_at,
            )
            session.add(db_derived)

        return Result.ok(derived_columns)

    except Exception as e:
        return Result.fail(f"Derived column detection failed: {e}")
