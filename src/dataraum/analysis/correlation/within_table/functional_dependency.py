"""Functional dependency detection.

Detects single-column functional dependencies: A → B.
A functional dependency means that for each value in the determinant,
there is exactly one value in the dependent column.

Uses parallel processing for large tables to speed up detection.
"""

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from uuid import uuid4

import duckdb
from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.analysis.correlation.db_models import (
    FunctionalDependency as DBFunctionalDependency,
)
from dataraum.analysis.correlation.models import FunctionalDependency
from dataraum.analysis.statistics.db_models import StatisticalProfile
from dataraum.core.logging import get_logger
from dataraum.core.models.base import Result
from dataraum.storage import Column, Table

logger = get_logger(__name__)


def _check_fd_pair(
    duckdb_conn: duckdb.DuckDBPyConnection,
    table_name: str,
    col_a_name: str,
    col_a_id: str,
    col_b_name: str,
    col_b_id: str,
    min_confidence: float,
) -> FunctionalDependency | None:
    """Check if col_a -> col_b is a functional dependency.

    Runs in a worker thread using a cursor from the shared DuckDB connection.
    DuckDB cursors are thread-safe for read operations.
    """
    cursor = duckdb_conn.cursor()
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
        fd_result = cursor.execute(query).fetchone()
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
        example_row = cursor.execute(example_query).fetchone()
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
        cursor.close()


def detect_functional_dependencies(
    table: Table,
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: Session,
    min_confidence: float = 0.95,
    max_workers: int = 4,
) -> Result[list[FunctionalDependency]]:
    """Detect single-column functional dependencies: A → B.

    A functional dependency means that for each value in the determinant,
    there is exactly one value in the dependent column.

    Uses parallel processing when there are many column pairs to check.

    Args:
        table: Table to analyze
        duckdb_conn: DuckDB connection
        session: SQLAlchemy session
        min_confidence: Minimum confidence (1.0 = exact FD)
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

        # Load statistical profiles to filter trivial dependents
        column_ids = [c.column_id for c in columns]
        profile_stmt = select(StatisticalProfile).where(
            StatisticalProfile.column_id.in_(column_ids)
        )
        profiles_by_col: dict[str, StatisticalProfile] = {
            p.column_id: p for p in session.execute(profile_stmt).scalars().all()
        }

        # Build set of trivial column IDs — columns that should NOT be dependents:
        # - Constants (distinct_count <= 1): any column trivially determines them
        # - Near-empty (null_ratio > 0.95, distinct_count <= 2): too few rows for reliable FDs
        trivial_dependent_ids: set[str] = set()
        for col_id, profile in profiles_by_col.items():
            if profile.distinct_count is not None and profile.distinct_count <= 1:
                trivial_dependent_ids.add(col_id)
            elif (
                profile.null_ratio is not None
                and profile.null_ratio > 0.95
                and profile.distinct_count is not None
                and profile.distinct_count <= 2
            ):
                trivial_dependent_ids.add(col_id)

        # Generate all column pairs to check, skipping trivial dependents
        pairs = [
            (col_a, col_b)
            for col_a in columns
            for col_b in columns
            if col_a.column_id != col_b.column_id and col_b.column_id not in trivial_dependent_ids
        ]

        dependencies: list[FunctionalDependency] = []

        # Use parallel processing with cursors from shared connection
        # DuckDB cursors are thread-safe for read operations
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = [
                pool.submit(
                    _check_fd_pair,
                    duckdb_conn,
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
