"""Type resolution engine - DuckDB SQL generation.

Generates SQL to create typed tables from raw VARCHAR tables
using TypeCandidates computed during profiling.
"""

from __future__ import annotations

from dataclasses import dataclass
from uuid import uuid4

import duckdb
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from dataraum_context.core.models.base import ColumnRef, DataType, Result
from dataraum_context.profiling.models import ColumnCastResult, TypeResolutionResult
from dataraum_context.profiling.patterns import Pattern, load_pattern_config
from dataraum_context.storage.models_v2.core import Column, Table


@dataclass
class ColumnTypeSpec:
    """Type specification for a column during resolution."""

    column_id: str
    column_name: str
    data_type: DataType
    pattern: Pattern | None = None


def _select_best_candidates(
    columns: list[Column],
    min_confidence: float,
) -> list[ColumnTypeSpec]:
    """Select best type candidate per column.

    Priority:
    1. TypeDecision (human override) if exists
    2. Highest confidence TypeCandidate >= threshold
    3. Fallback to VARCHAR
    """
    pattern_config = load_pattern_config()
    patterns_by_name = {p.name: p for p in pattern_config.get_value_patterns()}
    specs = []

    for col in sorted(columns, key=lambda c: c.column_position):
        # Check for human override
        if col.type_decision:
            specs.append(
                ColumnTypeSpec(
                    column_id=col.column_id,
                    column_name=col.column_name,
                    data_type=DataType[col.type_decision.decided_type],
                )
            )
            continue

        # Find best candidate
        candidates = sorted(col.type_candidates, key=lambda c: c.confidence, reverse=True)
        if candidates and candidates[0].confidence >= min_confidence:
            best = candidates[0]
            pattern = patterns_by_name.get(best.detected_pattern) if best.detected_pattern else None
            specs.append(
                ColumnTypeSpec(
                    column_id=col.column_id,
                    column_name=col.column_name,
                    data_type=DataType[best.data_type],
                    pattern=pattern,
                )
            )
        else:
            # Fallback to VARCHAR
            specs.append(
                ColumnTypeSpec(
                    column_id=col.column_id,
                    column_name=col.column_name,
                    data_type=DataType.VARCHAR,
                )
            )

    return specs


def _generate_typed_table_sql(
    raw_table: str,
    typed_table: str,
    specs: list[ColumnTypeSpec],
) -> str:
    """Generate CREATE TABLE with TRY_CAST per column."""
    selects = []
    for spec in specs:
        col = f'"{spec.column_name}"'
        target = spec.data_type.value

        if spec.pattern and spec.pattern.standardization_expr:
            # Apply standardization before cast
            expr = spec.pattern.standardization_expr.format(col=spec.column_name)
            selects.append(f"TRY_CAST({expr} AS {target}) AS {col}")
        else:
            selects.append(f"TRY_CAST({col} AS {target}) AS {col}")

    return (
        f'CREATE OR REPLACE TABLE "{typed_table}" AS SELECT {", ".join(selects)} FROM "{raw_table}"'
    )


def _generate_quarantine_sql(
    raw_table: str,
    quarantine_table: str,
    specs: list[ColumnTypeSpec],
) -> str:
    """Generate quarantine table for rows where any cast fails."""
    checks = []
    for spec in specs:
        col = f'"{spec.column_name}"'
        target = spec.data_type.value

        if spec.pattern and spec.pattern.standardization_expr:
            expr = spec.pattern.standardization_expr.format(col=spec.column_name)
            checks.append(f"(TRY_CAST({expr} AS {target}) IS NULL AND {col} IS NOT NULL)")
        else:
            checks.append(f"(TRY_CAST({col} AS {target}) IS NULL AND {col} IS NOT NULL)")

    where = " OR ".join(checks) if checks else "FALSE"
    return f'CREATE OR REPLACE TABLE "{quarantine_table}" AS SELECT *, CURRENT_TIMESTAMP AS _quarantined_at FROM "{raw_table}" WHERE {where}'


async def resolve_types(
    table_id: str,
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: AsyncSession,
    min_confidence: float = 0.85,
) -> Result[TypeResolutionResult]:
    """Resolve types for a raw table using DuckDB SQL.

    1. Load TypeCandidates (from profiling)
    2. Select best candidate per column
    3. Generate and execute typed table SQL
    4. Generate and execute quarantine table SQL
    5. Return stats
    """
    # Load table with columns and type candidates
    stmt = (
        select(Table)
        .where(Table.table_id == table_id)
        .options(
            selectinload(Table.columns).selectinload(Column.type_candidates),
            selectinload(Table.columns).selectinload(Column.type_decision),
        )
    )
    result = await session.execute(stmt)
    table = result.scalar_one_or_none()

    if not table:
        return Result.fail(f"Table not found: {table_id}")
    if table.layer != "raw":
        return Result.fail(f"Table is not a raw table: {table.layer}")
    if not table.duckdb_path:
        return Result.fail(f"Table has no DuckDB path: {table_id}")

    raw_table = table.duckdb_path
    base_name = raw_table.replace("raw_", "")
    typed_table = f"typed_{base_name}"
    quarantine_table = f"quarantine_{base_name}"

    # Select best candidates
    specs = _select_best_candidates(table.columns, min_confidence)

    # Generate and execute SQL
    try:
        typed_sql = _generate_typed_table_sql(raw_table, typed_table, specs)
        duckdb_conn.execute(typed_sql)

        quarantine_sql = _generate_quarantine_sql(raw_table, quarantine_table, specs)
        duckdb_conn.execute(quarantine_sql)
    except Exception as e:
        return Result.fail(f"SQL execution failed: {e}")

    # Get row counts
    total_result = duckdb_conn.execute(f'SELECT COUNT(*) FROM "{raw_table}"').fetchone()
    total_rows = total_result[0] if total_result else 0
    typed_result = duckdb_conn.execute(f'SELECT COUNT(*) FROM "{typed_table}"').fetchone()
    typed_rows = typed_result[0] if typed_result else 0
    quarantine_result = duckdb_conn.execute(f'SELECT COUNT(*) FROM "{quarantine_table}"').fetchone()
    quarantine_rows = quarantine_result[0] if quarantine_result else 0

    # Create metadata records for typed and quarantine tables
    typed_table_record = Table(
        table_id=str(uuid4()),
        source_id=table.source_id,
        table_name=table.table_name,
        layer="typed",
        duckdb_path=typed_table,
        row_count=typed_rows,
    )
    session.add(typed_table_record)

    quarantine_table_record = Table(
        table_id=str(uuid4()),
        source_id=table.source_id,
        table_name=table.table_name,
        layer="quarantine",
        duckdb_path=quarantine_table,
        row_count=quarantine_rows,
    )
    session.add(quarantine_table_record)

    # Create column records for typed table
    for i, spec in enumerate(specs):
        typed_col = Column(
            column_id=str(uuid4()),
            table_id=typed_table_record.table_id,
            column_name=spec.column_name,
            column_position=i,
            raw_type="VARCHAR",
            resolved_type=spec.data_type.value,
        )
        session.add(typed_col)

    # Create column records for quarantine table (all columns + _quarantined_at)
    for i, spec in enumerate(specs):
        quarantine_col = Column(
            column_id=str(uuid4()),
            table_id=quarantine_table_record.table_id,
            column_name=spec.column_name,
            column_position=i,
            raw_type="VARCHAR",
            resolved_type="VARCHAR",  # Quarantine keeps original VARCHAR
        )
        session.add(quarantine_col)

    # Add _quarantined_at column
    quarantine_meta_col = Column(
        column_id=str(uuid4()),
        table_id=quarantine_table_record.table_id,
        column_name="_quarantined_at",
        column_position=len(specs),
        raw_type="TIMESTAMP",
        resolved_type="TIMESTAMP",
    )
    session.add(quarantine_meta_col)

    await session.commit()

    # Build column results
    column_results = []
    for spec in specs:
        col = f'"{spec.column_name}"'
        target = spec.data_type.value

        if spec.pattern and spec.pattern.standardization_expr:
            expr = spec.pattern.standardization_expr.format(col=spec.column_name)
            cast_expr = f"TRY_CAST({expr} AS {target})"
        else:
            cast_expr = f"TRY_CAST({col} AS {target})"

        success_result = duckdb_conn.execute(
            f'SELECT COUNT(*) FROM "{raw_table}" WHERE {cast_expr} IS NOT NULL OR {col} IS NULL'
        ).fetchone()
        success = success_result[0] if success_result else 0
        failures = total_rows - success

        column_results.append(
            ColumnCastResult(
                column_id=spec.column_id,
                column_ref=ColumnRef(table_name=table.table_name, column_name=spec.column_name),
                source_type="VARCHAR",
                target_type=spec.data_type,
                success_count=success,
                failure_count=failures,
                success_rate=success / total_rows if total_rows > 0 else 1.0,
            )
        )

    return Result.ok(
        TypeResolutionResult(
            typed_table_name=typed_table,
            quarantine_table_name=quarantine_table,
            total_rows=total_rows,
            typed_rows=typed_rows,
            quarantined_rows=quarantine_rows,
            column_results=column_results,
        )
    )
