#!/usr/bin/env python3
"""Phase 8: Slice Analysis - Run analysis phases on slice tables.

This script runs Phases 3, 3b, and 5 on all slice tables created by Phase 7:
- Registers slice tables in metadata database (layer='slice')
- Runs statistical profiling (Phase 3)
- Runs statistical quality assessment (Phase 3b)
- Runs semantic analysis (Phase 5)

Prerequisites:
    - Phase 7 must be completed with --execute flag (run_phase7_slicing.py --execute)
    - Slice tables must exist in DuckDB

Usage:
    uv run python scripts/run_phase8_slice_analysis.py [--skip-semantic]

Options:
    --skip-semantic    Skip semantic analysis (faster, no LLM calls)
"""

import asyncio
import os
import sys
from typing import Any

from dotenv import load_dotenv
from infra import (
    cleanup_connections,
    get_duckdb_conn,
    get_test_session,
    print_database_summary,
    print_phase_status,
)
from sqlalchemy import func, select

# Load environment variables from .env
load_dotenv()


async def main(skip_semantic: bool = False) -> int:
    """Run Phase 8: Slice Analysis.

    Args:
        skip_semantic: Whether to skip semantic analysis

    Returns:
        Exit code (0 = success, 1 = error)
    """
    print("=" * 70)
    print("Phase 8: Slice Analysis (Persistent Storage)")
    print("=" * 70)

    # Check for API key if running semantic
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not skip_semantic and not api_key:
        print("\nWARNING: ANTHROPIC_API_KEY not found!")
        print("Semantic analysis will be skipped.")
        skip_semantic = True

    if not skip_semantic:
        print(f"\nUsing Anthropic API key: {api_key[:8]}...{api_key[-4:]}")

    # Get connections
    session_factory = await get_test_session()
    duckdb_conn = get_duckdb_conn()

    async with session_factory() as session:
        from dataraum_context.analysis.slicing import (
            SliceTableInfo,
            register_slice_tables,
            run_analysis_on_slices,
        )
        from dataraum_context.analysis.slicing.db_models import SliceDefinition
        from dataraum_context.analysis.statistics.db_models import StatisticalProfile
        from dataraum_context.storage import Table

        # Check prerequisites
        print("\n1. Checking prerequisites...")
        print("-" * 50)

        # Check for slice definitions
        slice_def_count = (
            await session.execute(select(func.count(SliceDefinition.slice_id)))
        ).scalar() or 0

        if slice_def_count == 0:
            print("   ERROR: No slice definitions found!")
            print("   Please run run_phase7_slicing.py first.")
            await cleanup_connections()
            return 1

        print(f"   Found {slice_def_count} slice definitions")

        # Check for slice tables in DuckDB
        tables_result = duckdb_conn.execute("SHOW TABLES").fetchall()
        slice_tables = [t[0] for t in tables_result if t[0].startswith("slice_")]

        if not slice_tables:
            print("   ERROR: No slice tables found in DuckDB!")
            print("   Please run run_phase7_slicing.py --execute first.")
            await cleanup_connections()
            return 1

        print(f"   Found {len(slice_tables)} slice tables in DuckDB")

        # Register slice tables in metadata
        print("\n2. Registering slice tables...")
        print("-" * 50)

        reg_result = await register_slice_tables(session, duckdb_conn)
        if not reg_result.success:
            print(f"   ERROR: {reg_result.error}")
            await cleanup_connections()
            return 1

        slice_infos: list[SliceTableInfo] = reg_result.unwrap()
        print(f"   Registered {len(slice_infos)} slice tables")

        # Show registration summary
        for info in slice_infos[:5]:
            print(f"      - {info.slice_table_name} ({info.row_count:,} rows)")
        if len(slice_infos) > 5:
            print(f"      ... and {len(slice_infos) - 5} more")

        # Check what's already analyzed
        print("\n   Slice analysis status:")
        slice_table_ids = [s.slice_table_id for s in slice_infos]

        # Check for existing profiles
        profiled_stmt = (
            select(StatisticalProfile.column_id)
            .join(Table, StatisticalProfile.column_id.isnot(None))
            .where(Table.layer == "slice")
            .distinct()
        )
        # Simplified check - just count slice tables with profiles
        slice_tables_stmt = select(Table).where(Table.layer == "slice")
        slice_tables_db = (await session.execute(slice_tables_stmt)).scalars().all()

        analyzed_count = 0
        for st in slice_tables_db:
            profile_count = (
                await session.execute(
                    select(func.count(StatisticalProfile.profile_id))
                    .join(
                        Table,
                        StatisticalProfile.column_id.isnot(None),  # Placeholder join
                    )
                    .where(StatisticalProfile.layer == "slice")
                )
            ).scalar() or 0
            if profile_count > 0:
                analyzed_count += 1

        print_phase_status(
            "slice_analysis",
            analyzed_count == len(slice_infos) and analyzed_count > 0,
        )

        # Setup semantic agent if needed
        semantic_agent = None
        if not skip_semantic:
            print("\n3. Setting up LLM for semantic analysis...")
            print("-" * 50)

            from dataraum_context.analysis.semantic.agent import SemanticAgent
            from dataraum_context.llm.cache import LLMCache
            from dataraum_context.llm.config import load_llm_config
            from dataraum_context.llm.prompts import PromptRenderer
            from dataraum_context.llm.providers import create_provider

            try:
                llm_config = load_llm_config()
                provider_config = llm_config.providers["anthropic"]
                provider = create_provider(
                    "anthropic",
                    {
                        "api_key_env": provider_config.api_key_env,
                        "default_model": provider_config.default_model,
                        "models": provider_config.models,
                        "base_url_env": provider_config.base_url_env,
                    },
                )
                renderer = PromptRenderer()
                cache = LLMCache()

                semantic_agent = SemanticAgent(
                    config=llm_config,
                    provider=provider,
                    prompt_renderer=renderer,
                    cache=cache,
                )
                print("   LLM configured successfully")
            except Exception as e:
                print(f"   WARNING: Failed to setup LLM: {e}")
                print("   Semantic analysis will be skipped.")
                skip_semantic = True

        # Run analysis on slices
        step_num = 4 if not skip_semantic else 3
        print(f"\n{step_num}. Running analysis on slice tables...")
        print("-" * 50)

        try:
            analysis_result = await run_analysis_on_slices(
                session=session,
                duckdb_conn=duckdb_conn,
                slice_infos=slice_infos,
                semantic_agent=semantic_agent,
                run_statistics=True,
                run_quality=True,
                run_semantic=not skip_semantic,
            )

            print(f"\n   Analysis complete!")
            print(f"   Slices analyzed: {analysis_result.slices_analyzed}")
            print(f"   Statistics computed: {analysis_result.statistics_computed}")
            print(f"   Quality assessed: {analysis_result.quality_assessed}")
            print(f"   Semantic enriched: {analysis_result.semantic_enriched}")

            if analysis_result.errors:
                print(f"\n   Errors ({len(analysis_result.errors)}):")
                for err in analysis_result.errors[:5]:
                    print(f"      - {err}")
                if len(analysis_result.errors) > 5:
                    print(f"      ... and {len(analysis_result.errors) - 5} more")

        except Exception as e:
            print(f"   ERROR: {e}")
            import traceback

            traceback.print_exc()
            await cleanup_connections()
            return 1

        # Summary
        await _print_slice_analysis_summary(session, duckdb_conn)
        await print_database_summary(session, duckdb_conn)

    await cleanup_connections()

    print("\n" + "=" * 70)
    print("Phase 8 COMPLETE")
    print("=" * 70)
    print("\nSlice analysis complete!")
    print("Next: Run run_phase9_quality_summary.py to generate quality summaries")
    return 0


async def _print_slice_analysis_summary(session: Any, duckdb_conn: Any) -> None:
    """Print summary of slice analysis results."""
    from dataraum_context.analysis.statistics.db_models import (
        StatisticalProfile,
        StatisticalQualityMetrics,
    )
    from dataraum_context.storage import Column, Table

    # Count slice tables
    slice_tables_stmt = select(Table).where(Table.layer == "slice")
    slice_tables = (await session.execute(slice_tables_stmt)).scalars().all()

    # Count profiles for slice tables
    if slice_tables:
        slice_table_ids = [t.table_id for t in slice_tables]
        columns_stmt = select(Column.column_id).where(Column.table_id.in_(slice_table_ids))
        column_ids = (await session.execute(columns_stmt)).scalars().all()

        profile_count = 0
        quality_count = 0
        if column_ids:
            profile_count = (
                await session.execute(
                    select(func.count(StatisticalProfile.profile_id)).where(
                        StatisticalProfile.column_id.in_(column_ids)
                    )
                )
            ).scalar() or 0

            quality_count = (
                await session.execute(
                    select(func.count(StatisticalQualityMetrics.metric_id)).where(
                        StatisticalQualityMetrics.column_id.in_(column_ids)
                    )
                )
            ).scalar() or 0

        print("\nSlice Analysis Summary:")
        print(f"  Slice tables: {len(slice_tables)}")
        print(f"  Statistical profiles: {profile_count}")
        print(f"  Quality metrics: {quality_count}")

        # Show row distribution across slices
        if slice_tables:
            total_rows = sum(t.row_count or 0 for t in slice_tables)
            print(f"  Total rows across slices: {total_rows:,}")


if __name__ == "__main__":
    # Check for flags
    skip_sem = "--skip-semantic" in sys.argv

    exit_code = asyncio.run(main(skip_semantic=skip_sem))
    sys.exit(exit_code)
