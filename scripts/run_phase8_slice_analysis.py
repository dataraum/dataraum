#!/usr/bin/env python3
"""Phase 8: Slice Analysis - Run analysis phases on slice tables.

This script runs Phases 3, 3b, and 5 on all slice tables created by Phase 7:
- Registers slice tables in metadata database (layer='slice')
- Runs statistical profiling (Phase 3)
- Runs statistical quality assessment (Phase 3b)
- Runs semantic analysis (Phase 5)
- Optionally runs temporal analysis (4 levels)

Prerequisites:
    - Phase 7 must be completed with --execute flag (run_phase7_slicing.py --execute)
    - Slice tables must exist in DuckDB

Usage:
    uv run python scripts/run_phase8_slice_analysis.py [--skip-semantic]
    uv run python scripts/run_phase8_slice_analysis.py --temporal --time-column Buchungsdatum

Options:
    --skip-semantic    Skip semantic analysis (faster, no LLM calls)
    --temporal         Enable temporal analysis
    --time-column      Name of the temporal column (required with --temporal)
    --period-start     Start date YYYY-MM-DD (default: auto-detect)
    --period-end       End date YYYY-MM-DD (default: auto-detect)
    --time-grain       Time granularity: daily, weekly, monthly (default: monthly)
"""

import argparse
import asyncio
import os
import sys
from datetime import date, timedelta
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


async def main(
    skip_semantic: bool = False,
    run_temporal: bool = False,
    run_topology: bool = False,
    time_column: str | None = None,
    period_start: date | None = None,
    period_end: date | None = None,
    time_grain: str = "monthly",
) -> int:
    """Run Phase 8: Slice Analysis.

    Args:
        skip_semantic: Whether to skip semantic analysis
        run_topology: Whether to run topological analysis on slices
        run_temporal: Whether to run temporal analysis
        time_column: Name of temporal column for temporal analysis
        period_start: Start date for temporal analysis
        period_end: End date for temporal analysis
        time_grain: Time granularity (daily, weekly, monthly)

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
        _slice_table_ids = [s.slice_table_id for s in slice_infos]

        # Check for existing profiles
        _profiled_stmt = (
            select(StatisticalProfile.column_id)
            .join(Table, StatisticalProfile.column_id.isnot(None))
            .where(Table.layer == "slice")
            .distinct()
        )
        # Simplified check - just count slice tables with profiles
        slice_tables_stmt = select(Table).where(Table.layer == "slice")
        slice_tables_db = (await session.execute(slice_tables_stmt)).scalars().all()

        analyzed_count = 0
        for _st in slice_tables_db:
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

            print("\n   Analysis complete!")
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

        # Run temporal analysis if enabled
        if run_temporal and time_column:
            step_num += 1
            print(f"\n{step_num}. Running temporal analysis on slice tables...")
            print("-" * 50)

            from dataraum_context.analysis.slicing.slice_runner import (
                run_temporal_analysis_on_slices,
            )

            # Auto-detect date range if not specified
            actual_period_start = period_start
            actual_period_end = period_end

            if not actual_period_start or not actual_period_end:
                # Try to detect date range from a slice table
                if slice_infos:
                    sample_table = slice_infos[0].slice_table_name
                    try:
                        range_sql = f"""
                            SELECT
                                MIN(CAST("{time_column}" AS DATE)),
                                MAX(CAST("{time_column}" AS DATE))
                            FROM "{sample_table}"
                        """
                        range_result = duckdb_conn.execute(range_sql).fetchone()
                        if range_result:
                            if not actual_period_start:
                                actual_period_start = range_result[0]
                            if not actual_period_end:
                                actual_period_end = range_result[1] + timedelta(days=1)
                    except Exception as e:
                        print(f"   WARNING: Could not auto-detect date range: {e}")

            if not actual_period_start or not actual_period_end:
                print("   ERROR: Could not determine date range for temporal analysis")
                print("   Please specify --period-start and --period-end")
            else:
                print(f"   Time column: {time_column}")
                print(f"   Period: {actual_period_start} to {actual_period_end}")
                print(f"   Granularity: {time_grain}")

                try:
                    temporal_result = await run_temporal_analysis_on_slices(
                        session=session,
                        duckdb_conn=duckdb_conn,
                        slice_infos=slice_infos,
                        time_column=time_column,
                        period_start=actual_period_start,
                        period_end=actual_period_end,
                        time_grain=time_grain,
                    )

                    print("\n   Temporal analysis complete!")
                    print(f"   Slices analyzed: {temporal_result.slices_analyzed}")
                    print(f"   Periods analyzed: {temporal_result.periods_analyzed}")
                    print(f"   Incomplete periods: {temporal_result.incomplete_periods}")
                    print(f"   Volume anomalies: {temporal_result.anomalies_detected}")
                    print(f"   Drift detected in: {temporal_result.drift_detected_count} slices")

                    if temporal_result.errors:
                        print(f"\n   Temporal errors ({len(temporal_result.errors)}):")
                        for err in temporal_result.errors[:3]:
                            print(f"      - {err}")
                        if len(temporal_result.errors) > 3:
                            print(f"      ... and {len(temporal_result.errors) - 3} more")

                    # Run temporal topology analysis if topology is also enabled
                    if run_topology:
                        try:
                            from dataraum_context.analysis.temporal_slicing import (
                                analyze_temporal_topology,
                            )
                            from dataraum_context.storage import Column

                            print("\n   Running temporal topology analysis...")

                            temporal_topo_results: list[dict[str, Any]] = []
                            grain_map = {
                                "daily": "day",
                                "weekly": "week",
                                "monthly": "month",
                            }
                            period_grain = grain_map.get(time_grain, "month")

                            for slice_info in slice_infos[:3]:  # Limit to first 3 for demo
                                # Get numeric columns for this slice
                                cols_stmt = select(Column).where(
                                    Column.table_id == slice_info.slice_table_id
                                )
                                cols_result = await session.execute(cols_stmt)
                                cols = list(cols_result.scalars().all())

                                numeric_types = {
                                    "INTEGER", "FLOAT", "DECIMAL", "NUMERIC",
                                    "DOUBLE", "BIGINT", "INT", "REAL",
                                }
                                numeric_cols = [
                                    c.column_name for c in cols
                                    if (c.resolved_type or c.raw_type or "").upper() in numeric_types
                                ]

                                if len(numeric_cols) >= 3:
                                    topo_result = analyze_temporal_topology(
                                        duck_conn=duckdb_conn,
                                        table_name=slice_info.slice_table_name,
                                        time_column=time_column,
                                        numeric_columns=numeric_cols,
                                        period=period_grain,
                                    )

                                    temporal_topo_results.append({
                                        "slice": slice_info.slice_table_name,
                                        "periods": topo_result.periods_analyzed,
                                        "trend": topo_result.trend_direction or "unknown",
                                        "drifts": len(topo_result.topology_drifts),
                                        "anomalies": len(topo_result.structural_anomaly_periods),
                                    })

                            if temporal_topo_results:
                                print(f"   Temporal topology analyzed: {len(temporal_topo_results)} slices")
                                for r in temporal_topo_results[:3]:
                                    print(
                                        f"      - {r['slice']}: {r['periods']} periods, "
                                        f"trend={r['trend']}, {r['drifts']} drifts, "
                                        f"{r['anomalies']} anomalies"
                                    )

                        except Exception as topo_err:
                            print(f"   WARNING: Temporal topology failed: {topo_err}")

                except Exception as e:
                    print(f"   ERROR in temporal analysis: {e}")
                    import traceback

                    traceback.print_exc()

        # Run topology analysis if enabled
        if run_topology:
            step_num += 1
            print(f"\n{step_num}. Running topology analysis on slice tables...")
            print("-" * 50)

            try:
                from dataraum_context.analysis.slicing.slice_runner import (
                    run_topology_on_slices,
                )

                topo_result = await run_topology_on_slices(
                    session=session,
                    duckdb_conn=duckdb_conn,
                    slice_infos=slice_infos,
                )

                print(f"   Slices analyzed: {topo_result.slices_analyzed}")
                print(f"   Slices with anomalies: {topo_result.slices_with_anomalies}")

                if topo_result.average_topology:
                    avg = topo_result.average_topology
                    print(f"\n   Average topology across slices:")
                    print(f"      Betti-0 (components): {avg['betti_0']:.1f}")
                    print(f"      Betti-1 (cycles): {avg['betti_1']:.1f}")
                    print(f"      Complexity: {avg['complexity']:.1f}")

                if topo_result.structural_drift:
                    print(
                        f"\n   Structural drift detected "
                        f"({len(topo_result.structural_drift)} deviations):"
                    )
                    for drift in topo_result.structural_drift[:5]:
                        print(
                            f"      - {drift['slice_value']}: {drift['metric']} = "
                            f"{drift['value']:.1f} (avg: {drift['average']:.1f}, "
                            f"{drift['deviation_pct']:.0f}% deviation)"
                        )
                    if len(topo_result.structural_drift) > 5:
                        print(
                            f"      ... and {len(topo_result.structural_drift) - 5} more"
                        )

                if topo_result.errors:
                    print(f"\n   Topology warnings ({len(topo_result.errors)}):")
                    for err in topo_result.errors[:3]:
                        print(f"      - {err}")
                    if len(topo_result.errors) > 3:
                        print(f"      ... and {len(topo_result.errors) - 3} more")

            except ImportError as e:
                print(f"   Skipped: {e}")
            except Exception as e:
                print(f"   ERROR in topology analysis: {e}")
                import traceback

                traceback.print_exc()

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
    parser = argparse.ArgumentParser(description="Phase 8: Run analysis on slice tables")
    parser.add_argument(
        "--skip-semantic",
        action="store_true",
        help="Skip semantic analysis (faster, no LLM calls)",
    )
    parser.add_argument(
        "--temporal",
        action="store_true",
        help="Enable temporal analysis",
    )
    parser.add_argument(
        "--topology",
        action="store_true",
        help="Enable topological analysis on slice tables",
    )
    parser.add_argument(
        "--time-column",
        type=str,
        help="Name of the temporal column (required with --temporal)",
    )
    parser.add_argument(
        "--period-start",
        type=str,
        help="Start date YYYY-MM-DD (auto-detect if not specified)",
    )
    parser.add_argument(
        "--period-end",
        type=str,
        help="End date YYYY-MM-DD (auto-detect if not specified)",
    )
    parser.add_argument(
        "--time-grain",
        type=str,
        choices=["daily", "weekly", "monthly"],
        default="monthly",
        help="Time granularity (default: monthly)",
    )

    args = parser.parse_args()

    # Validate temporal args
    if args.temporal and not args.time_column:
        print("ERROR: --time-column is required when using --temporal")
        sys.exit(1)

    # Parse dates
    period_start = None
    period_end = None
    if args.period_start:
        period_start = date.fromisoformat(args.period_start)
    if args.period_end:
        period_end = date.fromisoformat(args.period_end)

    exit_code = asyncio.run(
        main(
            skip_semantic=args.skip_semantic,
            run_temporal=args.temporal,
            run_topology=args.topology,
            time_column=args.time_column,
            period_start=period_start,
            period_end=period_end,
            time_grain=args.time_grain,
        )
    )
    sys.exit(exit_code)
