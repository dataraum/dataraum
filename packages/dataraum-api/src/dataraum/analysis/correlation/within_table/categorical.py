"""Categorical association analysis (Cramér's V).

Orchestrates categorical association computation:
1. Loads categorical columns from database
2. Fetches data from DuckDB (with sampling for large tables)
3. Calls pure algorithm from algorithms/categorical.py
4. Stores results in database

Uses parallel processing for large tables to speed up detection.
Uses RESERVOIR sampling for tables with >100K rows to maintain performance
while preserving statistical validity for chi-square tests.
"""

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from uuid import uuid4

import duckdb
from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.analysis.correlation.algorithms import (
    compute_cramers_v,
)
from dataraum.analysis.correlation.algorithms.categorical import (
    build_contingency_table,
)
from dataraum.analysis.correlation.db_models import (
    CategoricalAssociation as DBCategoricalAssociation,
)
from dataraum.analysis.correlation.models import CategoricalAssociation
from dataraum.core.logging import get_logger
from dataraum.core.models.base import Result
from dataraum.storage import Column, Table

logger = get_logger(__name__)

# Sampling thresholds for categorical correlation
# Chi-square requires minimum expected cell counts of 5
# With 100K samples, even 100x100 contingency tables have ~10 expected per cell
LARGE_TABLE_THRESHOLD = 100_000  # Use sampling above this row count
DEFAULT_SAMPLE_SIZE = 100_000  # Target sample size for large tables
MIN_SAMPLE_SIZE = 10_000  # Minimum samples for statistical validity


def _compute_association_pair(
    duckdb_conn: duckdb.DuckDBPyConnection,
    table_path: str,
    col1_id: str,
    col1_name: str,
    col2_id: str,
    col2_name: str,
    col1_idx: int,
    col2_idx: int,
    table_id: str,
    min_cramers_v: float,
    row_count: int,
    sample_size: int = DEFAULT_SAMPLE_SIZE,
) -> CategoricalAssociation | None:
    """Compute Cramér's V for a single column pair.

    Runs in a worker thread using a cursor from the shared DuckDB connection.
    DuckDB cursors are thread-safe for read operations.

    Uses RESERVOIR sampling for large tables (>100K rows) to maintain
    performance while preserving statistical validity for chi-square tests.
    """
    cursor = duckdb_conn.cursor()
    try:
        # Use sampling for large tables
        use_sampling = row_count > LARGE_TABLE_THRESHOLD
        actual_sample_size = min(sample_size, row_count)

        if use_sampling:
            # RESERVOIR sampling provides uniform random sample
            query = f"""
                SELECT "{col1_name}", "{col2_name}"
                FROM {table_path}
                WHERE "{col1_name}" IS NOT NULL
                  AND "{col2_name}" IS NOT NULL
                USING SAMPLE reservoir({actual_sample_size} ROWS)
            """
        else:
            query = f"""
                SELECT "{col1_name}", "{col2_name}"
                FROM {table_path}
                WHERE "{col1_name}" IS NOT NULL
                  AND "{col2_name}" IS NOT NULL
            """
        rows = cursor.execute(query).fetchall()

        if len(rows) < 10:
            return None

        col1_values = [row[0] for row in rows]
        col2_values = [row[1] for row in rows]

        # Build contingency table and compute Cramér's V
        contingency = build_contingency_table(col1_values, col2_values)
        algo_result = compute_cramers_v(contingency, col1_idx=col1_idx, col2_idx=col2_idx)

        if algo_result is None or algo_result.cramers_v < min_cramers_v:
            return None

        return CategoricalAssociation(
            association_id=str(uuid4()),
            table_id=table_id,
            column1_id=col1_id,
            column2_id=col2_id,
            column1_name=col1_name,
            column2_name=col2_name,
            cramers_v=algo_result.cramers_v,
            chi_square=algo_result.chi_square,
            p_value=algo_result.p_value,
            degrees_of_freedom=algo_result.degrees_of_freedom,
            sample_size=algo_result.sample_size,
            computed_at=datetime.now(UTC),
            association_strength=algo_result.strength,
            is_significant=algo_result.is_significant,
        )
    finally:
        cursor.close()


def compute_categorical_associations(
    table: Table,
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: Session,
    max_distinct_values: int = 100,
    min_cramers_v: float = 0.1,
    max_workers: int = 4,
    sample_size: int = DEFAULT_SAMPLE_SIZE,
) -> Result[list[CategoricalAssociation]]:
    """Compute Cramér's V for categorical column pairs.

    Cramér's V is based on chi-square test and ranges from 0 (no association)
    to 1 (perfect association).

    Uses parallel processing when there are many column pairs.
    Uses RESERVOIR sampling for large tables (>100K rows) to maintain
    performance while preserving statistical validity.

    Args:
        table: Table to analyze
        duckdb_conn: DuckDB connection
        session: SQLAlchemy session
        max_distinct_values: Skip columns with too many distinct values
        min_cramers_v: Minimum V to store
        max_workers: Maximum parallel workers
        sample_size: Maximum rows to sample for large tables (default 100K)

    Returns:
        Result containing list of CategoricalAssociation objects
    """
    try:
        # Get columns that could be categorical (VARCHAR or BOOLEAN)
        stmt = select(Column).where(Column.table_id == table.table_id)
        result = session.execute(stmt)
        all_columns = result.scalars().all()

        table_path = table.duckdb_path
        if not table_path:
            return Result.fail("Table has no DuckDB path")

        # Filter to categorical candidates
        categorical_columns = []
        for col in all_columns:
            if col.resolved_type not in ("VARCHAR", "BOOLEAN"):
                continue

            query = f'SELECT COUNT(DISTINCT "{col.column_name}") FROM {table_path}'
            distinct_count_result = duckdb_conn.execute(query).fetchone()

            if distinct_count_result and 2 <= distinct_count_result[0] <= max_distinct_values:
                categorical_columns.append(col)

        if len(categorical_columns) < 2:
            return Result.ok([])

        # Get row count for sampling decision
        row_count = table.row_count or 0
        if row_count == 0:
            count_result = duckdb_conn.execute(f"SELECT COUNT(*) FROM {table_path}").fetchone()
            row_count = count_result[0] if count_result else 0

        if row_count > LARGE_TABLE_THRESHOLD:
            logger.info(
                f"Using RESERVOIR sampling ({sample_size:,} rows) for "
                f"categorical correlations on {table.table_name} ({row_count:,} rows)"
            )

        # Generate all pairs
        pairs = [
            (i, col1, j, col2)
            for i, col1 in enumerate(categorical_columns)
            for j, col2 in enumerate(categorical_columns[i + 1 :], start=i + 1)
        ]

        associations: list[CategoricalAssociation] = []

        # Use parallel processing with cursors from shared connection
        # DuckDB cursors are thread-safe for read operations
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = [
                pool.submit(
                    _compute_association_pair,
                    duckdb_conn,
                    table_path,
                    col1.column_id,
                    col1.column_name,
                    col2.column_id,
                    col2.column_name,
                    i,
                    j,
                    table.table_id,
                    min_cramers_v,
                    row_count,
                    sample_size,
                )
                for i, col1, j, col2 in pairs
            ]

            for future in futures:
                assoc = future.result()
                if assoc:
                    associations.append(assoc)

        # Store all associations in database (sequential - SQLite writes)
        for association in associations:
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
