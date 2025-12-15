#!/usr/bin/env python3
"""Run staging, profiling, and enrichment on a CSV file.

This script runs the first 3 phases of the pipeline:
1. Staging: Load CSV files as VARCHAR (preserve raw values)
2. Profiling:
   - 2.1: Schema profiling (pattern detection, type candidates)
   - 2.2: Type resolution (create typed tables, quarantine failed casts)
   - 2.3: Statistics profiling (column stats, correlations)
3. Enrichment:
   - 3.1: Semantic enrichment
   - 3.2: Topology enrichment
   - 3.3: Temporal enrichment
   - 3.4: Cross-table multicollinearity

Usage:
    python scripts/run_staging_profiling.py <csv_file> [table_name]

Example:
    python scripts/run_staging_profiling.py examples/finance_csv_example/data.csv my_data
"""
from __future__ import annotations

import asyncio
import sys
from dataclasses import dataclass
from pathlib import Path

import duckdb
from sqlalchemy import event, func, select
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from dataraum_context.core.models import SourceConfig
from dataraum_context.core.models.base import Result
from dataraum_context.enrichment.cross_table_multicollinearity import (
    compute_cross_table_multicollinearity,
)
from dataraum_context.enrichment.semantic import enrich_semantic
from dataraum_context.enrichment.temporal import enrich_temporal
from dataraum_context.enrichment.topology import enrich_topology
from dataraum_context.llm import LLMService, load_llm_config
from dataraum_context.profiling.profiler import profile_schema, profile_statistics
from dataraum_context.profiling.type_resolution import resolve_types
from dataraum_context.staging.loaders.csv import CSVLoader
from dataraum_context.storage.schema import init_database
from dataraum_context.storage.models_v2.core import Column, Table
from dataraum_context.storage.models_v2.statistical_context import StatisticalProfile
from dataraum_context.storage.models_v2.relationship import Relationship
from dataraum_context.storage.models_v2.semantic_context import SemanticAnnotation


@dataclass
class TableHealth:
    """Track health of table through pipeline stages."""

    table_id: str
    table_name: str
    raw_table_name: str
    row_count: int
    column_count: int
    typed_table_name: str | None = None
    quarantine_table_name: str | None = None
    schema_profiling_completed: bool = False
    type_resolution_completed: bool = False
    statistics_profiling_completed: bool = False
    # Phase 3: Enrichment tracking
    semantic_enrichment_completed: bool = False
    topology_enrichment_completed: bool = False
    temporal_enrichment_completed: bool = False
    semantic_annotation_count: int = 0
    relationship_count: int = 0
    error: str | None = None


async def _get_typed_table_id(typed_table_name: str, session: AsyncSession) -> str | None:
    """Get the table ID for a typed table by DuckDB path."""
    stmt = select(Table).where(Table.duckdb_path == typed_table_name)
    result = await session.execute(stmt)
    table = result.scalar_one_or_none()
    return table.table_id if table else None


async def _count_annotations(session: AsyncSession, table_id: str) -> int:
    """Count semantic annotations for a table."""
    stmt = (
        select(func.count(SemanticAnnotation.annotation_id))
        .join(Column, SemanticAnnotation.column_id == Column.column_id)
        .where(Column.table_id == table_id)
    )
    result = await session.execute(stmt)
    return result.scalar() or 0


async def _count_relationships(session: AsyncSession, table_id: str) -> int:
    """Count relationships for a table."""
    stmt = select(func.count(Relationship.relationship_id)).where(
        (Relationship.from_table_id == table_id) | (Relationship.to_table_id == table_id)
    )
    result = await session.execute(stmt)
    return result.scalar() or 0


async def run_staging_and_profiling(
    csv_path: str, table_name: str = "my_table", min_confidence: float = 0.85, llm_service: LLMService | None = None, ontology: str | None = None
):
    """Run staging and profiling on a CSV file.

    Args:
        csv_path: Path to the CSV file
        table_name: Name for the table (default: "my_table")
        min_confidence: Minimum confidence for type resolution (default: 0.85)
    """
    # Validate file exists
    if not Path(csv_path).exists():
        print(f"❌ Error: File not found: {csv_path}")
        return

    warnings: list[str] = []
    table_health_records: list[TableHealth] = []

    # Create output directory for database files
    output_dir = Path("output")
    output_dir.mkdir(exist_ok=True)

    # Database file paths
    duckdb_path = output_dir / f"{table_name}_data.duckdb"
    sqlite_path = output_dir / f"{table_name}_metadata.sqlite"

    # Remove existing files for clean run
    if duckdb_path.exists():
        duckdb_path.unlink()
    if sqlite_path.exists():
        sqlite_path.unlink()

    print(f"📁 Output files:")
    print(f"   DuckDB (data):     {duckdb_path.absolute()}")
    print(f"   SQLite (metadata): {sqlite_path.absolute()}")
    print()

    # 1. Setup DuckDB connection (file-based)
    duckdb_conn = duckdb.connect(str(duckdb_path))

    # 2. Setup SQLAlchemy async engine (file-based)
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{sqlite_path}",
        echo=False,
        future=True,
    )

    @event.listens_for(engine.sync_engine, "connect")
    def set_sqlite_pragma(dbapi_conn, connection_record):
        cursor = dbapi_conn.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    # 3. Initialize database schema
    await init_database(engine)

    # 4. Create session factory
    session_factory = async_sessionmaker(
        engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )

    async with session_factory() as session:
        # =====================================================================
        # PHASE 1: STAGING
        # =====================================================================
        print("=" * 60)
        print("PHASE 1: STAGING")
        print("=" * 60)

        loader = CSVLoader()
        config = SourceConfig(
            name=table_name,
            source_type="csv",
            path=csv_path,
        )

        staging_result = await loader.load(config, duckdb_conn, session)

        if not staging_result.success:
            print(f"❌ Staging failed: {staging_result.error}")
            return

        staging = staging_result.value
        print(f"✅ Staging completed in {staging.duration_seconds:.2f}s")
        print(f"   Source ID: {staging.source_id}")
        print(f"   Total rows: {staging.total_rows}")

        for table in staging.tables:
            print(f"\n   Table: {table.table_name}")
            print(f"   - Table ID: {table.table_id}")
            print(f"   - Raw table: {table.raw_table_name}")
            print(f"   - Rows: {table.row_count}")
            print(f"   - Columns: {table.column_count}")

        # Initialize health records
        for staged_table in staging.tables:
            health = TableHealth(
                table_id=staged_table.table_id,
                table_name=staged_table.table_name,
                raw_table_name=staged_table.raw_table_name,
                row_count=staged_table.row_count,
                column_count=staged_table.column_count,
            )
            table_health_records.append(health)

        # Track typed table IDs for statistics profiling
        typed_table_ids: dict[str, str] = {}

        
        # =====================================================================
        # PHASE 2: PROFILING
        # =====================================================================
        print("\n" + "=" * 60)
        print("PHASE 2: PROFILING")
        print("=" * 60)

        # -----------------------------------------------------------------
        # Stage 2.1: Schema profiling (type discovery)
        # -----------------------------------------------------------------
        print("\n--- Stage 2.1: Schema Profiling ---")
        for health in table_health_records:
            if health.error:
                continue

            schema_result = await profile_schema(health.table_id, duckdb_conn, session)

            if schema_result.success:
                health.schema_profiling_completed = True
                profile = schema_result.value
                print(f"✅ Schema profiling completed for {health.table_name}")
                print(f"   Type candidates detected: {len(profile.type_candidates)}")

                print("\n   Column Type Candidates:")
                print("   " + "-" * 56)
                for candidate in profile.type_candidates:
                    col_name = candidate.column_ref.column_name
                    data_type = candidate.data_type.value
                    confidence = candidate.confidence
                    print(f"   {col_name:<25} → {data_type:<15} ({confidence:.1%})")
            else:
                health.error = f"Schema profiling failed: {schema_result.error}"
                warnings.append(
                    f"Schema profiling failed for {health.table_name}: {schema_result.error}"
                )
                print(f"❌ {health.error}")

        # -----------------------------------------------------------------
        # Stage 2.2: Type resolution
        # -----------------------------------------------------------------
        print("\n--- Stage 2.2: Type Resolution ---")
        for health in table_health_records:
            if health.error:
                continue

            type_result = await resolve_types(
                health.table_id, duckdb_conn, session, min_confidence
            )

            if type_result.success:
                health.type_resolution_completed = True
                resolution = type_result.unwrap()
                health.typed_table_name = resolution.typed_table_name
                health.quarantine_table_name = resolution.quarantine_table_name

                # Get typed table ID for statistics profiling
                typed_id = await _get_typed_table_id(resolution.typed_table_name, session)
                if typed_id:
                    typed_table_ids[health.table_id] = typed_id

                print(f"✅ Type resolution completed for {health.table_name}")
                print(f"   Typed table: {resolution.typed_table_name}")
                if resolution.quarantine_table_name:
                    print(f"   Quarantine table: {resolution.quarantine_table_name}")
            else:
                health.error = f"Type resolution failed: {type_result.error}"
                warnings.append(
                    f"Type resolution failed for {health.table_name}: {type_result.error}"
                )
                print(f"❌ {health.error}")

        # -----------------------------------------------------------------
        # Stage 2.3: Statistics profiling (on typed tables)
        # -----------------------------------------------------------------
        print("\n--- Stage 2.3: Statistics Profiling ---")
        for raw_id, typed_id in typed_table_ids.items():
            # Find health record by raw table ID
            health = next(h for h in table_health_records if h.table_id == raw_id)
            if health.error:
                continue

            stats_result = await profile_statistics(
                typed_id, duckdb_conn, session, include_correlations=True
            )

            if stats_result.success:
                health.statistics_profiling_completed = True
                stats = stats_result.value
                print(f"✅ Statistics profiling completed for {health.table_name}")
                print(f"   Columns profiled: {len(stats.column_profiles)}")
                if stats.correlation_result:
                    corr = stats.correlation_result
                    print(f"   Numeric correlations: {len(corr.numeric_correlations)}")
                    print(f"   Strong correlations: {corr.strong_correlations}")
            else:
                # Non-critical - warn but don't fail table
                warnings.append(
                    f"Statistics profiling failed for {health.table_name}: {stats_result.error}"
                )
                print(f"⚠️ Statistics profiling failed for {health.table_name}: {stats_result.error}")

        # =====================================================================
        # DISPLAY DATABASE RESULTS: Stage 2.3 Statistical Profiles
        # =====================================================================
        print("\n" + "=" * 60)
        print("DATABASE OUTPUT: Statistical Profiles (Stage 2.3)")
        print("=" * 60)

        for health in table_health_records:
            if not health.statistics_profiling_completed:
                continue

            # Get the typed table ID
            typed_id = typed_table_ids.get(health.table_id)
            if not typed_id:
                continue

            # Query statistical profiles from database
            stmt = (
                select(StatisticalProfile, Column)
                .join(Column, StatisticalProfile.column_id == Column.column_id)
                .where(Column.table_id == typed_id)
                .where(StatisticalProfile.layer == "typed")
                .order_by(Column.column_position)
            )
            result = await session.execute(stmt)
            profiles = result.all()

            print(f"\nTable: {health.table_name} ({len(profiles)} columns)")
            print("-" * 80)

            for profile, column in profiles:
                col_type = column.resolved_type or column.raw_type or "VARCHAR"
                print(f"\n  📊 Column: {column.column_name} ({col_type})")
                print(f"     Total: {profile.total_count:,} | Nulls: {profile.null_count:,} ({profile.null_ratio:.1%})" if profile.null_ratio else f"     Total: {profile.total_count:,} | Nulls: {profile.null_count:,}")
                print(f"     Distinct: {profile.distinct_count:,} | Cardinality: {profile.cardinality_ratio:.2%}" if profile.cardinality_ratio else f"     Distinct: {profile.distinct_count:,}")
                print(f"     Unique: {'Yes' if profile.is_unique else 'No'} | Numeric: {'Yes' if profile.is_numeric else 'No'}")

                # Show detailed stats from profile_data JSON
                data = profile.profile_data
                if data:
                    # Numeric stats
                    if "numeric_stats" in data and data["numeric_stats"]:
                        ns = data["numeric_stats"]
                        print(f"     Numeric Stats:")
                        print(f"       Min: {ns.get('min', 'N/A')} | Max: {ns.get('max', 'N/A')} | Mean: {ns.get('mean', 'N/A'):.2f}" if ns.get('mean') else f"       Min: {ns.get('min', 'N/A')} | Max: {ns.get('max', 'N/A')}")
                        if ns.get('std'):
                            print(f"       Std: {ns['std']:.2f} | Median: {ns.get('median', 'N/A')}")
                        if ns.get('percentiles'):
                            p = ns['percentiles']
                            print(f"       P25: {p.get('p25', 'N/A')} | P50: {p.get('p50', 'N/A')} | P75: {p.get('p75', 'N/A')}")

                    # String stats
                    if "string_stats" in data and data["string_stats"]:
                        ss = data["string_stats"]
                        print(f"     String Stats:")
                        print(f"       Min len: {ss.get('min_length', 'N/A')} | Max len: {ss.get('max_length', 'N/A')} | Avg len: {ss.get('avg_length', 'N/A'):.1f}" if ss.get('avg_length') else f"       Min len: {ss.get('min_length', 'N/A')} | Max len: {ss.get('max_length', 'N/A')}")

                    # Top values
                    if "top_values" in data and data["top_values"]:
                        print(f"     Top Values:")
                        for tv in data["top_values"][:5]:
                            print(f"       '{tv.get('value', 'N/A')}': {tv.get('count', 0):,} ({tv.get('percentage', 0):.1%})")

        # =====================================================================
        # PHASE 3: ENRICHMENT
        # =====================================================================
        print("\n" + "=" * 60)
        print("PHASE 3: ENRICHMENT")
        print("=" * 60)

        # Only enrich tables that completed profiling successfully
        successful_table_ids = [h.table_id for h in table_health_records if h.error is None]

        if not successful_table_ids:
            print("❌ No tables completed profiling successfully - skipping enrichment")

        if successful_table_ids:
            # Stage 3.1: Semantic enrichment (CRITICAL - requires LLM)
            print("\n--- Stage 3.1: Semantic Enrichment ---")
            if llm_service is None:
                print("   Skipped (LLM service not available)")
            else:
                try:
                    semantic_result = await enrich_semantic(
                        session=session,
                        llm_service=llm_service,
                        table_ids=successful_table_ids,
                        ontology=ontology,
                    )
                    # ✅ SemanticAnnotation, TableEntity, Relationship stored by enrich_semantic()

                    if not semantic_result.success:
                        warnings.append(f"Semantic enrichment failed: {semantic_result.error}")
                        print(f"❌ Semantic enrichment failed: {semantic_result.error}")
                    else:
                        if hasattr(semantic_result, 'warnings') and semantic_result.warnings:
                            warnings.extend(semantic_result.warnings)

                        # Update health records with semantic counts
                        for health in table_health_records:
                            if health.table_id in successful_table_ids:
                                health.semantic_enrichment_completed = True
                                # Query counts from database
                                health.semantic_annotation_count = await _count_annotations(
                                    session, health.table_id
                                )
                                health.relationship_count = await _count_relationships(session, health.table_id)
                        print(f"✅ Semantic enrichment completed")

                except Exception as e:
                    warnings.append(f"Semantic enrichment exception: {e}")
                    print(f"❌ Semantic enrichment exception: {e}")

            # Stage 3.2: Topology enrichment (NON-CRITICAL)
            print("\n--- Stage 3.2: Topology Enrichment ---")
            try:
                topology_result = await enrich_topology(
                    session=session,
                    duckdb_conn=duckdb_conn,
                    table_ids=successful_table_ids,
                )
                # ✅ Relationship, TopologyMetrics stored by enrich_topology()

                if topology_result.success:
                    for health in table_health_records:
                        if health.table_id in successful_table_ids:
                            health.topology_enrichment_completed = True
                    if hasattr(topology_result, 'warnings') and topology_result.warnings:
                        warnings.extend(topology_result.warnings)
                    print(f"✅ Topology enrichment completed")
                else:
                    warnings.append(f"Topology enrichment failed: {topology_result.error}")
                    print(f"⚠️ Topology enrichment failed: {topology_result.error}")

            except Exception as e:
                warnings.append(f"Topology enrichment exception: {e}")
                print(f"⚠️ Topology enrichment exception: {e}")

            # Stage 3.3: Temporal enrichment (NON-CRITICAL)
            print("\n--- Stage 3.3: Temporal Enrichment ---")
            try:
                temporal_result = await enrich_temporal(
                    session=session,
                    duckdb_conn=duckdb_conn,
                    table_ids=successful_table_ids,
                )
                # ✅ TemporalQualityMetrics stored by enrich_temporal()

                if temporal_result.success:
                    for health in table_health_records:
                        if health.table_id in successful_table_ids:
                            health.temporal_enrichment_completed = True
                    if hasattr(temporal_result, 'warnings') and temporal_result.warnings:
                        warnings.extend(temporal_result.warnings)
                    print(f"✅ Temporal enrichment completed")
                else:
                    warnings.append(f"Temporal enrichment failed: {temporal_result.error}")
                    print(f"⚠️ Temporal enrichment failed: {temporal_result.error}")

            except Exception as e:
                warnings.append(f"Temporal enrichment exception: {e}")
                print(f"⚠️ Temporal enrichment exception: {e}")

            # Stage 3.4: Cross-table multicollinearity (CONDITIONAL, NON-CRITICAL)
            print("\n--- Stage 3.4: Cross-table Multicollinearity ---")
            cross_table_completed = False
            cross_table_column_count = 0
            cross_table_relationship_count = 0

            if len(successful_table_ids) > 1:
                try:
                    cross_table_result = await compute_cross_table_multicollinearity(
                        table_ids=successful_table_ids,
                        duckdb_conn=duckdb_conn,
                        session=session,
                    )
                    # ✅ CrossTableAnalysis stored by compute_cross_table_multicollinearity()

                    if cross_table_result.success and cross_table_result.value:
                        cross_table_completed = True
                        analysis = cross_table_result.value
                        cross_table_column_count = analysis.total_columns_analyzed
                        cross_table_relationship_count = analysis.total_relationships_used
                        if hasattr(cross_table_result, 'warnings') and cross_table_result.warnings:
                            warnings.extend(cross_table_result.warnings)
                        print(f"✅ Cross-table multicollinearity completed")
                        print(f"   Columns analyzed: {cross_table_column_count}")
                        print(f"   Relationships used: {cross_table_relationship_count}")
                    else:
                        warnings.append(f"Cross-table multicollinearity failed: {cross_table_result.error}")
                        print(f"⚠️ Cross-table multicollinearity failed: {cross_table_result.error}")

                except Exception as e:
                    warnings.append(f"Cross-table multicollinearity exception: {e}")
                    print(f"⚠️ Cross-table multicollinearity exception: {e}")
            else:
                print("   Skipped (requires multiple tables)")

        # Show raw data preview
        staged_table = staging.tables[0]
        print("\n   Raw Data Preview (first 5 rows):")
        print("   " + "-" * 56)
        preview = duckdb_conn.execute(
            f"SELECT * FROM {staged_table.raw_table_name} LIMIT 5"
        ).fetchdf()
        print(preview.to_string(index=False))

        # Show typed data preview if available
        if table_health_records[0].typed_table_name:
            print("\n   Typed Data Preview (first 5 rows):")
            print("   " + "-" * 56)
            typed_preview = duckdb_conn.execute(
                f"SELECT * FROM {table_health_records[0].typed_table_name} LIMIT 5"
            ).fetchdf()
            print(typed_preview.to_string(index=False))

    # Cleanup
    duckdb_conn.close()
    await engine.dispose()

    # Print database file locations
    print("\n" + "=" * 60)
    print("📁 DATABASE FILES SAVED")
    print("=" * 60)
    print(f"   DuckDB (data tables):    {duckdb_path.absolute()}")
    print(f"   SQLite (metadata):       {sqlite_path.absolute()}")
    print("\nTo browse:")
    print(f"   duckdb {duckdb_path}")
    print(f"   sqlite3 {sqlite_path}")

    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    for health in table_health_records:
        print(f"\nTable: {health.table_name}")
        print(f"  Schema Profiling:     {'✅' if health.schema_profiling_completed else '❌'}")
        print(f"  Type Resolution:      {'✅' if health.type_resolution_completed else '❌'}")
        print(f"  Statistics Profiling: {'✅' if health.statistics_profiling_completed else '❌'}")
        print(f"  Semantic Enrichment:  {'✅' if health.semantic_enrichment_completed else '❌'}")
        print(f"  Topology Enrichment:  {'✅' if health.topology_enrichment_completed else '❌'}")
        print(f"  Temporal Enrichment:  {'✅' if health.temporal_enrichment_completed else '❌'}")
        if health.error:
            print(f"  Error: {health.error}")

    if warnings:
        print(f"\n⚠️ Warnings ({len(warnings)}):")
        for w in warnings:
            print(f"  - {w}")

    print("\n" + "=" * 60)
    print("✅ Pipeline phases 1-3 completed!")
    print("=" * 60)


def main():
    """Entry point for the script."""
    if len(sys.argv) < 2:
        print("Usage: python scripts/run_staging_profiling.py <csv_file> [table_name]")
        print("\nExample:")
        print("  python scripts/run_staging_profiling.py data/sales.csv sales_data")
        sys.exit(1)

    csv_file = sys.argv[1]
    name = sys.argv[2] if len(sys.argv) > 2 else Path(csv_file).stem

    # Initialize LLM service (optional - requires anthropic package and API key)
    llm_service = None
    try:
        llm_config = load_llm_config()
        llm_service = LLMService(llm_config)
        print("✅ LLM service initialized")
    except Exception as e:
        print(f"⚠️ LLM service not available (semantic enrichment will be skipped): {e}")

    asyncio.run(run_staging_and_profiling(csv_file, name, llm_service=llm_service))


if __name__ == "__main__":
    main()
