"""Staging pipeline entry point.

Combines CSV loading, schema profiling, type resolution, and statistics profiling
into a single operation.

Flow:
1. Load CSV → raw_table (VARCHAR)
2. profile_schema() → patterns, TypeCandidates
3. resolve_types() → typed_table + quarantine_table
4. profile_statistics() → numeric stats, correlations, multicollinearity
"""

from __future__ import annotations

from dataclasses import dataclass

import duckdb
from sqlalchemy.ext.asyncio import AsyncSession

from dataraum_context.core.models import SourceConfig
from dataraum_context.core.models.base import Result
from dataraum_context.profiling.models import (
    SchemaProfileResult,
    StatisticsProfileResult,
    TypeResolutionResult,
)
from dataraum_context.profiling.profiler import profile_schema, profile_statistics
from dataraum_context.profiling.type_resolution import resolve_types
from dataraum_context.staging.loaders.csv import CSVLoader
from dataraum_context.staging.models import StagingResult


@dataclass
class StagingPipelineResult:
    """Result of the complete staging pipeline."""

    staging_result: StagingResult
    schema_profile_result: SchemaProfileResult | None
    type_resolution_result: TypeResolutionResult | None
    statistics_profile_result: StatisticsProfileResult | None

    @property
    def raw_table_name(self) -> str:
        """Get the raw table name."""
        if self.staging_result.tables:
            return self.staging_result.tables[0].raw_table_name
        return ""

    @property
    def typed_table_name(self) -> str:
        """Get the typed table name."""
        if self.type_resolution_result:
            return self.type_resolution_result.typed_table_name
        return ""

    @property
    def total_rows(self) -> int:
        """Get total rows loaded."""
        return self.staging_result.total_rows


async def stage_csv(
    file_path: str,
    table_name: str,
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: AsyncSession,
    auto_resolve_types: bool = True,
    auto_profile_statistics: bool = True,
    min_confidence: float = 0.85,
) -> Result[StagingPipelineResult]:
    """Complete CSV staging pipeline.

    1. Load CSV → raw_table (VARCHAR)
    2. profile_schema() → patterns, TypeCandidates (sample-based, stable)
    3. resolve_types() → typed_table + quarantine_table
    4. profile_statistics() → all row-based stats on clean data

    Args:
        file_path: Path to the CSV file
        table_name: Name for the table (without prefix)
        duckdb_conn: DuckDB connection
        session: SQLAlchemy session
        auto_resolve_types: Whether to resolve types automatically
        auto_profile_statistics: Whether to profile statistics after type resolution
        min_confidence: Minimum confidence for type resolution

    Returns:
        Result containing StagingPipelineResult
    """
    # Step 1: Load CSV
    source_config = SourceConfig(name=table_name, source_type="csv", path=file_path)
    loader = CSVLoader()
    load_result = await loader.load(
        source_config=source_config,
        duckdb_conn=duckdb_conn,
        session=session,
    )

    if not load_result.success:
        return Result.fail(f"CSV loading failed: {load_result.error}")

    staging_result = load_result.unwrap()
    if not staging_result.tables:
        return Result.fail("No tables created during staging")

    table_id = staging_result.tables[0].table_id

    # Step 2: Schema profiling (raw table - type discovery)
    schema_result = await profile_schema(
        table_id=table_id,
        duckdb_conn=duckdb_conn,
        session=session,
    )

    schema_profile = None
    if schema_result.success:
        schema_profile = schema_result.unwrap()

    # Step 3: Type resolution (if enabled)
    resolution_result = None
    typed_table_id = None
    if auto_resolve_types:
        type_result = await resolve_types(
            table_id=table_id,
            duckdb_conn=duckdb_conn,
            session=session,
            min_confidence=min_confidence,
        )
        if type_result.success:
            resolution_result = type_result.unwrap()
            # Get the typed table ID for statistics profiling
            # For now, we need to look up the typed table
            typed_table_id = await _get_typed_table_id(resolution_result.typed_table_name, session)

    # Step 4: Statistics profiling (typed table - all row-based stats)
    statistics_profile = None
    if auto_resolve_types and auto_profile_statistics and typed_table_id:
        stats_result = await profile_statistics(
            table_id=typed_table_id,
            duckdb_conn=duckdb_conn,
            session=session,
            include_correlations=True,
        )
        if stats_result.success:
            statistics_profile = stats_result.unwrap()

    return Result.ok(
        StagingPipelineResult(
            staging_result=staging_result,
            schema_profile_result=schema_profile,
            type_resolution_result=resolution_result,
            statistics_profile_result=statistics_profile,
        )
    )


async def _get_typed_table_id(
    typed_table_name: str,
    session: AsyncSession,
) -> str | None:
    """Get the table ID for a typed table by name.

    Args:
        typed_table_name: Name of the typed table
        session: SQLAlchemy session

    Returns:
        Table ID or None if not found
    """
    from sqlalchemy import select

    from dataraum_context.storage.models_v2.core import Table

    stmt = select(Table).where(Table.duckdb_path == typed_table_name)
    result = await session.execute(stmt)
    table = result.scalar_one_or_none()
    return table.table_id if table else None
