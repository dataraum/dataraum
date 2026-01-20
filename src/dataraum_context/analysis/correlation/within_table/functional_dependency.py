"""Functional dependency detection.

Detects functional dependencies: A → B or (A, B) → C.
A functional dependency means that for each value (or combination of values)
in the determinant, there is exactly one value in the dependent column.

Uses parallel processing for large tables to speed up detection.
"""

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from uuid import uuid4

import duckdb
from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum_context.analysis.correlation.db_models import (
    FunctionalDependency as DBFunctionalDependency,
)
from dataraum_context.analysis.correlation.models import FunctionalDependency
from dataraum_context.core.models.base import Result
from dataraum_context.storage import Column, Table


def _check_fd_pair(
    db_path: str,
    table_name: str,
    col_a_name: str,
    col_a_id: str,
    col_b_name: str,
    col_b_id: str,
    min_confidence: float,
) -> FunctionalDependency | None:
    """Check if col_a -> col_b is a functional dependency.

    Runs in a worker thread with its own DuckDB connection.
    """
    conn = duckdb.connect(db_path, read_only=True)
    try:
        query = f"""
            WITH mappings AS (
                SELECT
                    "{col_a_name}",
                    COUNT(DISTINCT "{col_b_name}") as distinct_b_values
                FROM {table_name}
                WHERE
                    "{col_a_name}" IS NOT NULL
                    AND "{col_b_name}" IS NOT NULL
                GROUP BY "{col_a_name}"
            )
            SELECT
                COUNT(CASE WHEN distinct_b_values = 1 THEN 1 END) as valid_mappings,
                COUNT(CASE WHEN distinct_b_values > 1 THEN 1 END) as violations,
                COUNT(*) as total_unique_a
            FROM mappings
        """
        fd_result = conn.execute(query).fetchone()
        if not fd_result:
            return None

        valid_mappings, violations, total_unique_a = fd_result
        if total_unique_a == 0:
            return None

        confidence = valid_mappings / total_unique_a
        if confidence < min_confidence:
            return None

        # Get example
        example_query = f"""
            SELECT "{col_a_name}", "{col_b_name}"
            FROM {table_name}
            WHERE "{col_a_name}" IS NOT NULL
            LIMIT 1
        """
        example_row = conn.execute(example_query).fetchone()
        example = (
            {
                "determinant_values": [str(example_row[0])],
                "dependent_value": str(example_row[1]),
            }
            if example_row
            else None
        )

        return FunctionalDependency(
            dependency_id=str(uuid4()),
            table_id="",  # Will be set by caller
            determinant_column_ids=[col_a_id],
            determinant_column_names=[col_a_name],
            dependent_column_id=col_b_id,
            dependent_column_name=col_b_name,
            confidence=float(confidence),
            unique_determinant_values=int(total_unique_a),
            violation_count=int(violations),
            example=example,
            computed_at=datetime.now(UTC),
        )
    finally:
        conn.close()


def detect_functional_dependencies(
    table: Table,
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: Session,
    min_confidence: float = 0.95,
    max_determinant_columns: int = 3,
    max_workers: int = 4,
) -> Result[list[FunctionalDependency]]:
    """Detect functional dependencies: A → B or (A, B) → C.

    A functional dependency means that for each value (or combination of values)
    in the determinant, there is exactly one value in the dependent column.

    Uses parallel processing when there are many column pairs to check.

    Args:
        table: Table to analyze
        duckdb_conn: DuckDB connection (used for db path only)
        session: Session
        min_confidence: Minimum confidence (1.0 = exact FD)
        max_determinant_columns: Maximum columns in determinant
        max_workers: Maximum parallel workers for FD checking

    Returns:
        Result containing list of FunctionalDependency objects
    """
    try:
        # Get all columns
        stmt = select(Column).where(Column.table_id == table.table_id)
        result = session.execute(stmt)
        columns = result.scalars().all()

        if len(columns) < 2:
            return Result.ok([])

        table_name = table.duckdb_path
        if not table_name:
            return Result.fail("Table has no DuckDB path")

        # Get DuckDB file path from the connection
        # The duckdb_path is the table name, we need the database file path
        db_info = duckdb_conn.execute("PRAGMA database_list").fetchall()
        db_path = db_info[0][2] if db_info and db_info[0][2] else ""

        # Generate all column pairs to check
        pairs = [
            (col_a, col_b)
            for col_a in columns
            for col_b in columns
            if col_a.column_id != col_b.column_id
        ]

        dependencies: list[FunctionalDependency] = []

        # Use parallel processing for file-based DBs (workers need to connect to same DB)
        if db_path:
            # Parallel: each worker gets its own read-only DuckDB connection
            with ThreadPoolExecutor(max_workers=max_workers) as pool:
                futures = [
                    pool.submit(
                        _check_fd_pair,
                        db_path,
                        table_name,
                        col_a.column_name,
                        col_a.column_id,
                        col_b.column_name,
                        col_b.column_id,
                        min_confidence,
                    )
                    for col_a, col_b in pairs
                ]

                for future in futures:
                    fd = future.result()
                    if fd:
                        # Set table_id (wasn't available in worker)
                        fd.table_id = table.table_id
                        dependencies.append(fd)
        else:
            # Sequential: use the provided connection
            for col_a, col_b in pairs:
                query = f"""
                    WITH mappings AS (
                        SELECT
                            "{col_a.column_name}",
                            COUNT(DISTINCT "{col_b.column_name}") as distinct_b_values
                        FROM {table_name}
                        WHERE
                            "{col_a.column_name}" IS NOT NULL
                            AND "{col_b.column_name}" IS NOT NULL
                        GROUP BY "{col_a.column_name}"
                    )
                    SELECT
                        COUNT(CASE WHEN distinct_b_values = 1 THEN 1 END) as valid_mappings,
                        COUNT(CASE WHEN distinct_b_values > 1 THEN 1 END) as violations,
                        COUNT(*) as total_unique_a
                    FROM mappings
                """
                fd_result = duckdb_conn.execute(query).fetchone()
                if not fd_result:
                    continue

                valid_mappings, violations, total_unique_a = fd_result
                if total_unique_a == 0:
                    continue

                confidence = valid_mappings / total_unique_a
                if confidence < min_confidence:
                    continue

                # Get example
                example_query = f"""
                    SELECT "{col_a.column_name}", "{col_b.column_name}"
                    FROM {table_name}
                    WHERE "{col_a.column_name}" IS NOT NULL
                    LIMIT 1
                """
                example_row = duckdb_conn.execute(example_query).fetchone()
                example = (
                    {
                        "determinant_values": [str(example_row[0])],
                        "dependent_value": str(example_row[1]),
                    }
                    if example_row
                    else None
                )

                dependencies.append(
                    FunctionalDependency(
                        dependency_id=str(uuid4()),
                        table_id=table.table_id,
                        determinant_column_ids=[col_a.column_id],
                        determinant_column_names=[col_a.column_name],
                        dependent_column_id=col_b.column_id,
                        dependent_column_name=col_b.column_name,
                        confidence=float(confidence),
                        unique_determinant_values=int(total_unique_a),
                        violation_count=int(violations),
                        example=example,
                        computed_at=datetime.now(UTC),
                    )
                )

        # Store all dependencies in database (sequential - SQLite writes)
        for dependency in dependencies:
            db_fd = DBFunctionalDependency(
                dependency_id=dependency.dependency_id,
                table_id=dependency.table_id,
                determinant_column_ids=dependency.determinant_column_ids,
                dependent_column_id=dependency.dependent_column_id,
                confidence=dependency.confidence,
                unique_determinant_values=dependency.unique_determinant_values,
                violation_count=dependency.violation_count,
                example=dependency.example,
                computed_at=dependency.computed_at,
            )
            session.add(db_fd)

        return Result.ok(dependencies)

    except Exception as e:
        return Result.fail(f"Functional dependency detection failed: {e}")
