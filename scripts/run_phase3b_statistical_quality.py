#!/usr/bin/env python3
"""Phase 3B: Statistical Quality Assessment.

This script runs statistical quality checks (Benford's Law, outlier detection)
on typed tables that have been profiled in Phase 3.

Prerequisites:
    - Phase 3 must be completed (run_phase3_statistics.py)

Usage:
    uv run python scripts/run_phase3b_statistical_quality.py
"""

import asyncio
import sys

from infra import (
    cleanup_connections,
    get_duckdb_conn,
    get_test_session,
    print_database_summary,
    print_phase_status,
)
from sqlalchemy import func, select


async def main() -> int:
    """Run Phase 3B: Statistical Quality Assessment."""
    print("=" * 70)
    print("Phase 3B: Statistical Quality Assessment")
    print("=" * 70)

    # Get connections
    session_factory = await get_test_session()
    duckdb_conn = get_duckdb_conn()

    async with session_factory() as session:
        from dataraum_context.analysis.statistics.db_models import (
            StatisticalProfile,
            StatisticalQualityMetrics,
        )
        from dataraum_context.storage import Column, Table

        # Check prerequisites - need typed tables with statistical profiles
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

        if not profile_count:
            print("   ERROR: No statistical profiles found!")
            print("   Please run run_phase3_statistics.py first.")
            await cleanup_connections()
            return 1

        print(f"   Found {len(typed_tables)} typed tables")
        print(f"   Found {profile_count} statistical profiles")

        # Check what already has quality metrics
        typed_table_ids = [t.table_id for t in typed_tables]
        columns_stmt = select(Column).where(Column.table_id.in_(typed_table_ids))
        all_columns = (await session.execute(columns_stmt)).scalars().all()

        assessed_stmt = select(StatisticalQualityMetrics.column_id).distinct()
        assessed_column_ids = set((await session.execute(assessed_stmt)).scalars().all())

        unassessed_tables = []
        for tt in typed_tables:
            table_columns = [c for c in all_columns if c.table_id == tt.table_id]
            # Only check numeric columns (quality assessment is for numeric data)
            numeric_columns = [
                c
                for c in table_columns
                if c.resolved_type in ["INTEGER", "BIGINT", "DOUBLE", "DECIMAL"]
            ]
            if numeric_columns:
                numeric_column_ids = {c.column_id for c in numeric_columns}
                if numeric_column_ids - assessed_column_ids:
                    unassessed_tables.append(tt)

        if not unassessed_tables:
            print("\n   All tables already assessed for quality!")
            for tt in typed_tables:
                print_phase_status(f"quality_{tt.table_name}", True)
            await print_database_summary(session, duckdb_conn)
            await cleanup_connections()
            return 0

        # Show what needs assessment
        print("\n   Statistical quality status:")
        for tt in typed_tables:
            table_columns = [c for c in all_columns if c.table_id == tt.table_id]
            numeric_columns = [
                c
                for c in table_columns
                if c.resolved_type in ["INTEGER", "BIGINT", "DOUBLE", "DECIMAL"]
            ]
            if numeric_columns:
                numeric_column_ids = {c.column_id for c in numeric_columns}
                is_assessed = not (numeric_column_ids - assessed_column_ids)
                print_phase_status(f"quality_{tt.table_name}", is_assessed)
            else:
                print_phase_status(f"quality_{tt.table_name}", True, "(no numeric columns)")

        # Run statistical quality assessment
        print("\n2. Running statistical quality assessment...")
        print("-" * 50)

        from dataraum_context.analysis.statistics import assess_statistical_quality

        assessed_count = 0
        total_columns = 0
        benford_violations = 0
        outlier_columns = 0

        for typed_table in unassessed_tables:
            print(f"\n   Processing: {typed_table.table_name}...")

            result = await assess_statistical_quality(typed_table.table_id, duckdb_conn, session)
            if not result.success:
                print(f"      ERROR: {result.error}")
                continue

            quality_results = result.unwrap()
            assessed_count += 1
            total_columns += len(quality_results)

            # Summarize findings
            for qr in quality_results:
                if qr.benford_analysis and not qr.benford_analysis.is_compliant:
                    benford_violations += 1
                    print(
                        f"      - {qr.column_ref.column_name}: "
                        f"Benford violation (p={qr.benford_analysis.p_value:.4f})"
                    )

                if qr.outlier_detection:
                    if qr.outlier_detection.iqr_outlier_ratio > 0.05:
                        outlier_columns += 1
                        print(
                            f"      - {qr.column_ref.column_name}: "
                            f"{qr.outlier_detection.iqr_outlier_ratio * 100:.1f}% outliers (IQR)"
                        )
                    if qr.outlier_detection.isolation_forest_anomaly_ratio > 0.05:
                        print(
                            f"      - {qr.column_ref.column_name}: "
                            f"{qr.outlier_detection.isolation_forest_anomaly_ratio * 100:.1f}% "
                            "anomalies (Isolation Forest)"
                        )

            if not quality_results:
                print("      (no numeric columns to assess)")
            else:
                print(f"      Assessed {len(quality_results)} numeric columns")

        # Summary
        print("\n" + "-" * 50)
        print("3. Quality Assessment Summary")
        print("-" * 50)
        print(f"   Tables assessed: {assessed_count}")
        print(f"   Columns assessed: {total_columns}")
        print(f"   Benford violations: {benford_violations}")
        print(f"   Columns with high outlier ratio: {outlier_columns}")

        await print_database_summary(session, duckdb_conn)

        # Show quality metrics count
        quality_count = (
            await session.execute(select(func.count(StatisticalQualityMetrics.metric_id)))
        ).scalar()
        print(f"\nStatistical quality metrics: {quality_count}")

    await cleanup_connections()

    print("\n" + "=" * 70)
    print("Phase 3B COMPLETE")
    print("=" * 70)
    print("\nNext: Run run_phase4_relationships.py to detect relationships")
    return 0


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
