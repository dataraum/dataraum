#!/usr/bin/env python3
"""Phase 7: Slicing Analysis with persistent storage.

This script runs LLM-powered slicing analysis to identify optimal
categorical dimensions for creating data subsets:
- Analyzes statistical profiles for cardinality
- Uses semantic annotations for business context
- Considers correlations to avoid redundant slices
- Generates DuckDB SQL for creating slice tables

Prerequisites:
    - 
    - Phase 3 must be completed (run_phase3_statistics.py)
    - Phase 5 recommended (run_phase5_semantic.py)
    - ANTHROPIC_API_KEY must be set in environment or .env file

Usage:
    uv run python scripts/run_phase7_slicing.py [--execute]

Options:
    --execute    Execute SQL to create slice tables in DuckDB
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


async def main(execute_slices: bool = False) -> int:
    """Run Phase 7: Slicing Analysis.

    Args:
        execute_slices: Whether to execute SQL and create slice tables

    Returns:
        Exit code (0 = success, 1 = error)
    """
    print("=" * 70)
    print("Phase 7: Slicing Analysis (Persistent Storage)")
    print("=" * 70)

    # Check for API key
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        print("\nERROR: ANTHROPIC_API_KEY not found!")
        print("Please set ANTHROPIC_API_KEY in your environment or .env file.")
        return 1

    print(f"\nUsing Anthropic API key: {api_key[:8]}...{api_key[-4:]}")

    # Get connections
    session_factory = await get_test_session()
    duckdb_conn = get_duckdb_conn()

    async with session_factory() as session:
        from dataraum_context.analysis.slicing.db_models import (
            SliceDefinition,
        )
        from dataraum_context.analysis.statistics.db_models import StatisticalProfile
        from dataraum_context.storage import Table

        # Check prerequisites - need typed tables with statistics
        print("\n1. Checking prerequisites...")
        print("-" * 50)

        typed_tables_stmt = select(Table).where(Table.layer == "typed")
        typed_tables = (await session.execute(typed_tables_stmt)).scalars().all()

        if not typed_tables:
            print("   ERROR: No typed tables found!")
            print("   Please run run_phase2_typing.py first.")
            await cleanup_connections()
            return 1

        # Check for statistical profiles
        profile_count = (
            await session.execute(
                select(func.count(StatisticalProfile.profile_id)).where(
                    StatisticalProfile.layer == "typed"
                )
            )
        ).scalar()

        if profile_count == 0:
            print("   ERROR: No statistical profiles found!")
            print("   Please run run_phase3_statistics.py first.")
            await cleanup_connections()
            return 1

        print(f"   Found {len(typed_tables)} typed tables")
        print(f"   Found {profile_count} statistical profiles")

        # Check if slicing analysis already done
        slice_count = (
            await session.execute(select(func.count(SliceDefinition.slice_id)))
        ).scalar() or 0

        # if slice_count > 0:
        #     print("\n   Slicing analysis already performed!")
        #     print(f"   Slice definitions: {slice_count}")
        #     print_phase_status("slicing", True)
        #     await _print_slicing_summary(session)
        #     await print_database_summary(session, duckdb_conn)
        #     await cleanup_connections()
        #     return 0

        print_phase_status("slicing", False)

        # Setup LLM and create slicing agent
        print("\n2. Running slicing analysis...")
        print("-" * 50)

        from dataraum_context.analysis.slicing import SlicingAgent, analyze_slices
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

            agent = SlicingAgent(
                config=llm_config,
                provider=provider,
                prompt_renderer=renderer,
                cache=cache,
            )

            table_ids = [t.table_id for t in typed_tables]

            result = await analyze_slices(
                session=session,
                agent=agent,
                table_ids=table_ids,
                duckdb_conn=duckdb_conn if execute_slices else None,
                execute_slices=execute_slices,
            )

            if not result.success:
                print(f"   ERROR: {result.error}")
                await cleanup_connections()
                return 1

            slicing_result = result.unwrap()

            print("\n   Analysis complete!")
            print(f"   Tables analyzed: {slicing_result.tables_analyzed}")
            print(f"   Columns considered: {slicing_result.columns_considered}")
            print(f"   Recommendations: {len(slicing_result.recommendations)}")
            print(f"   Slice queries generated: {len(slicing_result.slice_queries)}")

            # Print recommendations
            if slicing_result.recommendations:
                print("\n3. Slice Recommendations:")
                print("-" * 50)
                for rec in slicing_result.recommendations:
                    print(f"\n   Priority {rec.slice_priority}: {rec.table_name}.{rec.column_name}")
                    print(f"   Values: {rec.value_count} distinct values")
                    print(f"   Confidence: {rec.confidence:.0%}")
                    print(f"   Reasoning: {rec.reasoning}")
                    if rec.business_context:
                        print(f"   Business context: {rec.business_context}")

                    # Show sample values
                    if rec.distinct_values:
                        sample = rec.distinct_values[:5]
                        if len(rec.distinct_values) > 5:
                            print(
                                f"   Sample values: {sample} ... (+{len(rec.distinct_values) - 5} more)"
                            )
                        else:
                            print(f"   Values: {sample}")

            # Print SQL preview
            if slicing_result.slice_queries and not execute_slices:
                print("\n4. Generated SQL (preview):")
                print("-" * 50)
                for sq in slicing_result.slice_queries[:3]:
                    print(f"\n   -- {sq.slice_name}")
                    print(f"   {sq.sql_query}")

                if len(slicing_result.slice_queries) > 3:
                    print(f"\n   ... and {len(slicing_result.slice_queries) - 3} more queries")

                print("\n   To execute these queries and create slice tables:")
                print("   uv run python scripts/run_phase7_slicing.py --execute")

            if execute_slices:
                print("\n4. Slice tables created in DuckDB")
                print("-" * 50)
                tables = duckdb_conn.execute("SHOW TABLES").fetchall()
                slice_tables = [t[0] for t in tables if t[0].startswith("slice_")]
                print(f"   Created {len(slice_tables)} slice tables")
                for st in slice_tables[:10]:
                    count = duckdb_conn.execute(f"SELECT COUNT(*) FROM {st}").fetchone()[0]
                    print(f"   - {st}: {count:,} rows")
                if len(slice_tables) > 10:
                    print(f"   ... and {len(slice_tables) - 10} more")

        except Exception as e:
            print(f"   ERROR: {e}")
            import traceback

            traceback.print_exc()
            await cleanup_connections()
            return 1

        # Summary
        await _print_slicing_summary(session)
        await print_database_summary(session, duckdb_conn)

    await cleanup_connections()

    print("\n" + "=" * 70)
    print("Phase 7 COMPLETE")
    print("=" * 70)
    print("\nSlicing analysis complete! Use the generated SQL to create data subsets.")
    return 0


async def _print_slicing_summary(session: Any) -> None:
    """Print summary of slicing analysis results."""
    from dataraum_context.analysis.slicing.db_models import (
        SliceDefinition,
        SlicingAnalysisRun,
    )

    # Count definitions
    slice_count = (
        await session.execute(select(func.count(SliceDefinition.slice_id)))
    ).scalar() or 0

    # Get latest run
    run_stmt = select(SlicingAnalysisRun).order_by(SlicingAnalysisRun.started_at.desc()).limit(1)
    run_result = await session.execute(run_stmt)
    latest_run = run_result.scalar_one_or_none()

    print("\nSlicing Analysis Results:")
    print(f"  Slice definitions: {slice_count}")

    if latest_run:
        print(f"  Last run: {latest_run.started_at}")
        print(f"  Status: {latest_run.status}")
        print(f"  Tables analyzed: {latest_run.tables_analyzed}")
        print(f"  Recommendations: {latest_run.recommendations_count}")
        if latest_run.duration_seconds:
            print(f"  Duration: {latest_run.duration_seconds:.2f}s")


if __name__ == "__main__":
    # Check for --execute flag
    execute = "--execute" in sys.argv

    exit_code = asyncio.run(main(execute_slices=execute))
    sys.exit(exit_code)
