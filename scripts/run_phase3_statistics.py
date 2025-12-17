#!/usr/bin/env python3
"""Phase 3: Statistical Profiling with persistent storage.

This script computes statistical profiles for typed tables created in Phase 2.

Prerequisites:
    - Phase 2 must be completed (run_phase2_typing.py)

Usage:
    uv run python scripts/run_phase3_statistics.py
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
    """Run Phase 3: Statistical Profiling."""
    print("=" * 70)
    print("Phase 3: Statistical Profiling (Persistent Storage)")
    print("=" * 70)

    # Get connections
    session_factory = await get_test_session()
    duckdb_conn = get_duckdb_conn()

    async with session_factory() as session:
        from dataraum_context.analysis.statistics.db_models import StatisticalProfile
        from dataraum_context.storage import Column, Table

        # Check prerequisites - need typed tables from Phase 2
        print("\n1. Checking prerequisites...")
        print("-" * 50)

        typed_tables_stmt = select(Table).where(Table.layer == "typed")
        typed_tables = (await session.execute(typed_tables_stmt)).scalars().all()

        if not typed_tables:
            print("   ERROR: No typed tables found!")
            print("   Please run run_phase2_typing.py first.")
            await cleanup_connections()
            return 1

        print(f"   Found {len(typed_tables)} typed tables")
        for tt in typed_tables:
            print(f"      - {tt.table_name}")

        # Check what's already profiled
        # Get columns for typed tables
        typed_table_ids = [t.table_id for t in typed_tables]
        columns_stmt = select(Column).where(Column.table_id.in_(typed_table_ids))
        all_columns = (await session.execute(columns_stmt)).scalars().all()

        # Get columns that already have profiles (layer='typed')
        profiled_stmt = (
            select(StatisticalProfile.column_id)
            .where(StatisticalProfile.layer == "typed")
            .distinct()
        )
        profiled_column_ids = set((await session.execute(profiled_stmt)).scalars().all())

        unprofiled_tables = []
        for tt in typed_tables:
            table_columns = [c for c in all_columns if c.table_id == tt.table_id]
            table_column_ids = {c.column_id for c in table_columns}
            if table_column_ids - profiled_column_ids:
                unprofiled_tables.append(tt)

        if not unprofiled_tables:
            print("\n   All tables already profiled!")
            for tt in typed_tables:
                print_phase_status(f"stats_{tt.table_name}", True)
            await print_database_summary(session, duckdb_conn)
            await cleanup_connections()
            return 0

        # Show what needs profiling
        print("\n   Statistical profiles status:")
        for tt in typed_tables:
            table_columns = [c for c in all_columns if c.table_id == tt.table_id]
            table_column_ids = {c.column_id for c in table_columns}
            is_profiled = not (table_column_ids - profiled_column_ids)
            print_phase_status(f"stats_{tt.table_name}", is_profiled)

        # Run statistical profiling
        print("\n2. Running statistical profiling...")
        print("-" * 50)

        from dataraum_context.analysis.statistics import profile_statistics

        profiled_count = 0

        for typed_table in unprofiled_tables:
            print(f"      Processing: {typed_table.table_name}...")

            result = await profile_statistics(typed_table.table_id, duckdb_conn, session)
            if not result.success:
                print(f"         ERROR: {result.error}")
                continue

            profile_result = result.unwrap()
            print(f"         Profiled {len(profile_result.column_profiles)} columns")
            print(f"         Duration: {profile_result.duration_seconds:.2f}s")
            profiled_count += 1

        print(f"\n   Profiled {profiled_count} tables")

        # Summary
        await print_database_summary(session, duckdb_conn)

        # Show profile counts
        profile_count = (
            await session.execute(
                select(func.count(StatisticalProfile.profile_id)).where(
                    StatisticalProfile.layer == "typed"
                )
            )
        ).scalar()
        print(f"\nStatistical profiles (typed): {profile_count}")

    await cleanup_connections()

    print("\n" + "=" * 70)
    print("Phase 3 COMPLETE")
    print("=" * 70)
    print("\nNext: Run run_phase4_relationships.py to detect relationships")
    return 0


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
