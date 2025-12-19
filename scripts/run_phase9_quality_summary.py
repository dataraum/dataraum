#!/usr/bin/env python3
"""Phase 9: Quality Summary - Generate LLM-powered quality reports.

This script generates quality summaries per column, aggregating findings
from all slices created by Phase 7 and analyzed by Phase 8:
- Aggregates statistics, quality metrics, and semantic info across slices
- Uses LLM to generate human-readable quality summaries
- Stores reports with investigation SQL views for UI drill-down

Prerequisites:
    - Phase 7 must be completed (run_phase7_slicing.py --execute)
    - Phase 8 must be completed (run_phase8_slice_analysis.py)

Usage:
    uv run python scripts/run_phase9_quality_summary.py
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


async def main() -> int:
    """Run Phase 9: Quality Summary.

    Returns:
        Exit code (0 = success, 1 = error)
    """
    print("=" * 70)
    print("Phase 9: Quality Summary (LLM-Powered Reports)")
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
        from dataraum_context.analysis.quality_summary import (
            ColumnQualityReport,
            QualitySummaryAgent,
            QualitySummaryRun,
            summarize_quality,
        )
        from dataraum_context.analysis.slicing.db_models import SliceDefinition
        from dataraum_context.analysis.statistics.db_models import StatisticalProfile
        from dataraum_context.storage import Column, Table

        # Check prerequisites
        print("\n1. Checking prerequisites...")
        print("-" * 50)

        # Check for slice definitions
        slice_def_stmt = select(SliceDefinition).order_by(SliceDefinition.slice_priority)
        slice_defs = (await session.execute(slice_def_stmt)).scalars().all()

        if not slice_defs:
            print("   ERROR: No slice definitions found!")
            print("   Please run run_phase7_slicing.py first.")
            await cleanup_connections()
            return 1

        print(f"   Found {len(slice_defs)} slice definitions")

        # Check for slice tables
        slice_tables_stmt = select(Table).where(Table.layer == "slice")
        slice_tables = (await session.execute(slice_tables_stmt)).scalars().all()

        if not slice_tables:
            print("   ERROR: No slice tables found!")
            print("   Please run run_phase8_slice_analysis.py first.")
            await cleanup_connections()
            return 1

        print(f"   Found {len(slice_tables)} slice tables")

        # Check for statistical profiles on slice tables
        slice_table_ids = [t.table_id for t in slice_tables]
        slice_columns_stmt = select(Column.column_id).where(
            Column.table_id.in_(slice_table_ids)
        )
        slice_column_ids = (await session.execute(slice_columns_stmt)).scalars().all()

        profile_count = 0
        if slice_column_ids:
            profile_count = (
                await session.execute(
                    select(func.count(StatisticalProfile.profile_id)).where(
                        StatisticalProfile.column_id.in_(slice_column_ids)
                    )
                )
            ).scalar() or 0

        if profile_count == 0:
            print("   ERROR: No statistical profiles found for slice tables!")
            print("   Please run run_phase8_slice_analysis.py first.")
            await cleanup_connections()
            return 1

        print(f"   Found {profile_count} statistical profiles on slice tables")

        # Check existing reports
        report_count = (
            await session.execute(select(func.count(ColumnQualityReport.report_id)))
        ).scalar() or 0

        print_phase_status("quality_summary", report_count > 0)

        # Setup LLM
        print("\n2. Setting up LLM...")
        print("-" * 50)

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

            agent = QualitySummaryAgent(
                config=llm_config,
                provider=provider,
                prompt_renderer=renderer,
                cache=cache,
            )
            print("   LLM configured successfully")
        except Exception as e:
            print(f"   ERROR: Failed to setup LLM: {e}")
            await cleanup_connections()
            return 1

        # Generate quality summaries for each slice definition
        print("\n3. Generating quality summaries...")
        print("-" * 50)

        total_reports = 0
        total_columns = 0

        for slice_def in slice_defs:
            # Get info for display
            source_table = await session.get(Table, slice_def.table_id)
            slice_column = await session.get(Column, slice_def.column_id)

            if not source_table or not slice_column:
                continue

            print(f"\n   Processing: {source_table.table_name}")
            print(f"   Slice column: {slice_column.column_name}")
            print(f"   Slice values: {len(slice_def.distinct_values or [])} values")

            try:
                result = await summarize_quality(
                    session=session,
                    agent=agent,
                    slice_definition=slice_def,
                )

                if not result.success:
                    print(f"      ERROR: {result.error}")
                    continue

                summary_result = result.unwrap()
                total_columns += len(summary_result.column_summaries)
                total_reports += len(summary_result.column_summaries)

                print(f"      Generated {len(summary_result.column_summaries)} column reports")
                if summary_result.duration_seconds:
                    print(f"      Duration: {summary_result.duration_seconds:.2f}s")

                # Show sample of findings
                for col_summary in summary_result.column_summaries[:3]:
                    print(
                        f"      - {col_summary.column_name}: "
                        f"Grade {col_summary.quality_grade} "
                        f"({col_summary.overall_quality_score:.0%})"
                    )
                if len(summary_result.column_summaries) > 3:
                    print(f"      ... and {len(summary_result.column_summaries) - 3} more")

            except Exception as e:
                print(f"      ERROR: {e}")
                import traceback

                traceback.print_exc()
                continue

        print(f"\n   Total reports generated: {total_reports}")
        print(f"   Total columns analyzed: {total_columns}")

        # Print summary
        await _print_quality_summary(session)
        await print_database_summary(session, duckdb_conn)

    await cleanup_connections()

    print("\n" + "=" * 70)
    print("Phase 9 COMPLETE")
    print("=" * 70)
    print("\nQuality summaries generated!")
    print("Reports are stored in column_quality_reports table.")
    print("Use the investigation_views SQL for UI drill-down.")
    return 0


async def _print_quality_summary(session: Any) -> None:
    """Print summary of quality reports."""
    from dataraum_context.analysis.quality_summary.db_models import (
        ColumnQualityReport,
        QualitySummaryRun,
    )

    # Count reports
    report_count = (
        await session.execute(select(func.count(ColumnQualityReport.report_id)))
    ).scalar() or 0

    # Get grade distribution
    grade_counts: dict[str, int] = {}
    if report_count > 0:
        reports_stmt = select(ColumnQualityReport)
        reports = (await session.execute(reports_stmt)).scalars().all()
        for report in reports:
            grade = report.quality_grade
            grade_counts[grade] = grade_counts.get(grade, 0) + 1

    # Get latest run
    run_stmt = select(QualitySummaryRun).order_by(QualitySummaryRun.started_at.desc()).limit(1)
    run_result = await session.execute(run_stmt)
    latest_run = run_result.scalar_one_or_none()

    print("\nQuality Summary Results:")
    print(f"  Total reports: {report_count}")

    if grade_counts:
        print("  Grade distribution:")
        for grade in ["A", "B", "C", "D", "F"]:
            count = grade_counts.get(grade, 0)
            if count > 0:
                print(f"    {grade}: {count}")

    if latest_run:
        print(f"\n  Last run: {latest_run.started_at}")
        print(f"  Status: {latest_run.status}")
        print(f"  Columns analyzed: {latest_run.columns_analyzed}")
        if latest_run.duration_seconds:
            print(f"  Duration: {latest_run.duration_seconds:.2f}s")


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
