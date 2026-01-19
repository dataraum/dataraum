"""Functional dependency detection.

Detects functional dependencies: A → B or (A, B) → C.
A functional dependency means that for each value (or combination of values)
in the determinant, there is exactly one value in the dependent column.
"""

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


def detect_functional_dependencies(
    table: Table,
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: Session,
    min_confidence: float = 0.95,
    max_determinant_columns: int = 3,
) -> Result[list[FunctionalDependency]]:
    """Detect functional dependencies: A → B or (A, B) → C.

    A functional dependency means that for each value (or combination of values)
    in the determinant, there is exactly one value in the dependent column.

    Args:
        table: Table to analyze
        duckdb_conn: DuckDB connection
        session: Session
        min_confidence: Minimum confidence (1.0 = exact FD)
        max_determinant_columns: Maximum columns in determinant

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

        dependencies = []
        table_name = table.duckdb_path

        # Check single-column FDs: A → B
        for col_a in columns:
            for col_b in columns:
                if col_a.column_id == col_b.column_id:
                    continue

                # Check if col_a → col_b
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

                if confidence >= min_confidence:
                    computed_at = datetime.now(UTC)

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

                    dependency = FunctionalDependency(
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
                        computed_at=computed_at,
                    )

                    dependencies.append(dependency)

                    # Store in database
                    db_fd = DBFunctionalDependency(
                        dependency_id=dependency.dependency_id,
                        table_id=dependency.table_id,
                        determinant_column_ids=[col_a.column_id],
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
