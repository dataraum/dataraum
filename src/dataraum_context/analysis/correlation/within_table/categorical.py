"""Categorical association analysis (Cramér's V).

Orchestrates categorical association computation:
1. Loads categorical columns from database
2. Fetches data from DuckDB
3. Calls pure algorithm from algorithms/categorical.py
4. Stores results in database

Uses parallel processing for large tables to speed up detection.
"""

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from uuid import uuid4

import duckdb
from sqlalchemy import select
from sqlalchemy.orm import Session

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


def _compute_association_pair(
    db_path: str,
    table_path: str,
    col1_id: str,
    col1_name: str,
    col2_id: str,
    col2_name: str,
    col1_idx: int,
    col2_idx: int,
    table_id: str,
    min_cramers_v: float,
) -> CategoricalAssociation | None:
    """Compute Cramér's V for a single column pair.

    Runs in a worker thread with its own DuckDB connection.
    """
    conn = duckdb.connect(db_path, read_only=True)
    try:
        query = f"""
            SELECT "{col1_name}", "{col2_name}"
            FROM {table_path}
            WHERE "{col1_name}" IS NOT NULL
              AND "{col2_name}" IS NOT NULL
        """
        rows = conn.execute(query).fetchall()

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
        conn.close()


def compute_categorical_associations(
    table: Table,
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: Session,
    max_distinct_values: int = 100,
    min_cramers_v: float = 0.1,
    max_workers: int = 4,
) -> Result[list[CategoricalAssociation]]:
    """Compute Cramér's V for categorical column pairs.

    Cramér's V is based on chi-square test and ranges from 0 (no association)
    to 1 (perfect association).

    Uses parallel processing when there are many column pairs.

    Args:
        table: Table to analyze
        duckdb_conn: DuckDB connection
        session: SQLAlchemy session
        max_distinct_values: Skip columns with too many distinct values
        min_cramers_v: Minimum V to store
        max_workers: Maximum parallel workers

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

        # Get DuckDB file path
        db_info = duckdb_conn.execute("PRAGMA database_list").fetchall()
        db_path = db_info[0][2] if db_info and db_info[0][2] else ""

        # Generate all pairs
        pairs = [
            (i, col1, j, col2)
            for i, col1 in enumerate(categorical_columns)
            for j, col2 in enumerate(categorical_columns[i + 1 :], start=i + 1)
        ]

        associations: list[CategoricalAssociation] = []

        # Use parallel processing for file-based DBs (workers need to connect to same DB)
        if db_path:
            with ThreadPoolExecutor(max_workers=max_workers) as pool:
                futures = [
                    pool.submit(
                        _compute_association_pair,
                        db_path,
                        table_path,
                        col1.column_id,
                        col1.column_name,
                        col2.column_id,
                        col2.column_name,
                        i,
                        j,
                        table.table_id,
                        min_cramers_v,
                    )
                    for i, col1, j, col2 in pairs
                ]

                for future in futures:
                    assoc = future.result()
                    if assoc:
                        associations.append(assoc)
        else:
            # Sequential processing
            computed_at = datetime.now(UTC)
            for i, col1, j, col2 in pairs:
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

                contingency = build_contingency_table(col1_values, col2_values)
                algo_result = compute_cramers_v(contingency, col1_idx=i, col2_idx=j)

                if algo_result is None or algo_result.cramers_v < min_cramers_v:
                    continue

                associations.append(
                    CategoricalAssociation(
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
                )

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
