#!/usr/bin/env python3
"""Phase 2: Type Resolution with persistent storage.

This script resolves column types for raw tables loaded in Phase 1.
Creates typed tables with proper data types.

Prerequisites:
    - Phase 1 must be completed (run_phase1_import.py)

Usage:
    uv run python scripts/run_phase2_typing.py
"""

import asyncio
import sys

from sqlalchemy import select
from test_infra import (
    cleanup_connections,
    get_duckdb_conn,
    get_test_session,
    print_database_summary,
    print_phase_status,
)


async def main() -> int:
    """Run Phase 2: Type Resolution."""
    print("=" * 70)
    print("Phase 2: Type Resolution (Persistent Storage)")
    print("=" * 70)

    # Get connections
    session_factory = await get_test_session()
    duckdb_conn = get_duckdb_conn()

    async with session_factory() as session:
        from dataraum_context.storage import Table

        # Check prerequisites - need raw tables from Phase 1
        print("\n1. Checking prerequisites...")
        print("-" * 50)

        raw_tables_stmt = select(Table).where(Table.layer == "raw")
        raw_tables = (await session.execute(raw_tables_stmt)).scalars().all()

        if not raw_tables:
            print("   ERROR: No raw tables found!")
            print("   Please run run_phase1_import.py first.")
            await cleanup_connections()
            return 1

        print(f"   Found {len(raw_tables)} raw tables")
        for rt in raw_tables:
            print(f"      - {rt.table_name}")

        # Check what's already typed
        # Note: table_name is the base name (e.g., "customer_table"),
        # layer distinguishes raw vs typed, duckdb_path has the prefix
        typed_tables_stmt = select(Table).where(Table.layer == "typed")
        typed_tables = (await session.execute(typed_tables_stmt)).scalars().all()

        typed_base_names = {t.table_name for t in typed_tables}
        # Raw tables may have "raw_" in table_name or not - get base names
        expected_base_names = {rt.table_name.replace("raw_", "") for rt in raw_tables}

        all_typed = expected_base_names <= typed_base_names
        if all_typed:
            print("\n   All tables already typed!")
            for name in sorted(typed_base_names):
                print_phase_status(f"typed_{name}", True)
            await print_database_summary(session, duckdb_conn)
            await cleanup_connections()
            return 0

        # Show what needs typing
        print("\n   Typed tables status:")
        for name in sorted(expected_base_names):
            print_phase_status(f"typed_{name}", name in typed_base_names)

        # Run type inference and resolution
        print("\n2. Running type inference and resolution...")
        print("-" * 50)

        from dataraum_context.analysis.typing import infer_type_candidates, resolve_types

        typed_ids = []

        for raw_table in raw_tables:
            # Check if already typed (compare base names)
            base_name = raw_table.table_name.replace("raw_", "")
            if base_name in typed_base_names:
                print(f"      Skipping (exists): {raw_table.table_name}")
                continue

            print(f"      Processing: {raw_table.table_name}...")

            # Type inference
            infer_result = await infer_type_candidates(raw_table, duckdb_conn, session)
            if not infer_result.success:
                print(f"         ERROR in inference: {infer_result.error}")
                continue

            candidates = infer_result.unwrap()
            print(f"         Generated {len(candidates)} type candidates")

            # Type resolution
            resolve_result = await resolve_types(raw_table.table_id, duckdb_conn, session)
            if not resolve_result.success:
                print(f"         ERROR in resolution: {resolve_result.error}")
                continue

            resolution = resolve_result.unwrap()
            print(f"         Created: {resolution.typed_table_name}")
            print(
                f"         Rows: {resolution.total_rows}, Quarantined: {resolution.quarantined_rows}"
            )

            # Get the typed table ID
            typed_stmt = select(Table).where(Table.duckdb_path == resolution.typed_table_name)
            typed_table = (await session.execute(typed_stmt)).scalar_one_or_none()
            if typed_table:
                typed_ids.append(typed_table.table_id)

        print(f"\n   Created {len(typed_ids)} typed tables")

        # Summary
        await print_database_summary(session, duckdb_conn)

    await cleanup_connections()

    print("\n" + "=" * 70)
    print("Phase 2 COMPLETE")
    print("=" * 70)
    print("\nNext: Run run_phase3_statistics.py to compute statistical profiles")
    return 0


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
