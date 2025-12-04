"""Staging pipeline entry point.

Combines CSV loading, profiling, and type resolution into a single operation.
"""

from __future__ import annotations

from dataclasses import dataclass

import duckdb
from sqlalchemy.ext.asyncio import AsyncSession

from dataraum_context.core.models import SourceConfig
from dataraum_context.core.models.base import Result
from dataraum_context.profiling.models import TypeResolutionResult
from dataraum_context.profiling.profiler import profile_table
from dataraum_context.profiling.type_resolution import resolve_types
from dataraum_context.staging.loaders.csv import CSVLoader
from dataraum_context.staging.models import StagingResult


@dataclass
class StagingPipelineResult:
    """Result of the complete staging pipeline."""

    staging_result: StagingResult
    type_resolution_result: TypeResolutionResult | None

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
    min_confidence: float = 0.85,
) -> Result[StagingPipelineResult]:
    """Complete CSV staging pipeline.

    1. Load CSV → raw_{table} (VARCHAR)
    2. Profile table (statistics + type candidates)
    3. Optionally resolve types → typed_{table} + quarantine_{table}

    Args:
        file_path: Path to the CSV file
        table_name: Name for the table (without prefix)
        duckdb_conn: DuckDB connection
        session: SQLAlchemy session
        auto_resolve_types: Whether to resolve types automatically
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

    # Step 2: Profile table
    profile_result = await profile_table(
        table_id=table_id,
        duckdb_conn=duckdb_conn,
        session=session,
    )

    if not profile_result.success:
        return Result.fail(f"Profiling failed: {profile_result.error}")

    # Step 3: Optionally resolve types
    resolution_result = None
    if auto_resolve_types:
        type_result = await resolve_types(
            table_id=table_id,
            duckdb_conn=duckdb_conn,
            session=session,
            min_confidence=min_confidence,
        )
        if type_result.success:
            resolution_result = type_result.unwrap()

    return Result.ok(
        StagingPipelineResult(
            staging_result=staging_result,
            type_resolution_result=resolution_result,
        )
    )
