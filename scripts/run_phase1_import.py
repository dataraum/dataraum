#!/usr/bin/env python3
"""Phase 1: CSV Import with persistent storage.

This script loads CSV files and stores them in persistent DuckDB + SQLite databases.
Later phases can then use this data without re-importing.

Usage:
    uv run python scripts/run_phase1_import.py [--reset]

Options:
    --reset: Delete existing databases and start fresh
"""

import asyncio
import sys
from pathlib import Path
from typing import Any

from infra import (
    check_tables_exist,
    cleanup_connections,
    get_duckdb_conn,
    get_test_session,
    print_database_summary,
    print_phase_status,
)

# CSV files to load
CSV_DIR = Path(__file__).parent.parent / "examples" #/ "finance_csv_example"
CSV_FILES = [
    "kontobuchungen.csv"
]

# Junk columns to drop (artifacts from CSV exports)
JUNK_COLUMNS = ["column0", "column00", "Unnamed: 0.2", "Unnamed: 0.1", "Unnamed: 0"]


async def load_csv_file(
    csv_path: Path,
    session: Any,
    duckdb_conn: Any,
    loader: Any,
) -> str | None:
    """Load a single CSV file and return the table ID."""
    from sqlalchemy import select

    from dataraum_context.core.models import SourceConfig
    from dataraum_context.storage import Column

    config = SourceConfig(
        name=csv_path.stem.lower(),
        source_type="csv",
        path=str(csv_path),
    )

    result = await loader.load(config, duckdb_conn, session)
    if not result.success:
        print(f"      ERROR: {result.error}")
        return None

    staged = result.unwrap().tables[0]

    # Drop junk columns
    from dataraum_context.storage import Table

    raw_table = await session.get(Table, staged.table_id)
    if raw_table:
        for junk in JUNK_COLUMNS:
            try:
                duckdb_conn.execute(f'ALTER TABLE {raw_table.duckdb_path} DROP COLUMN "{junk}"')
                stmt = select(Column).where(
                    Column.table_id == raw_table.table_id, Column.column_name == junk
                )
                col = (await session.execute(stmt)).scalar_one_or_none()
                if col:
                    await session.delete(col)
            except Exception:
                pass
        await session.commit()

    print(f"      {csv_path.name}: {staged.row_count} rows, {staged.column_count} cols")
    return str(staged.table_id)


async def main(reset: bool = False) -> int:
    """Run Phase 1: CSV Import."""
    print("=" * 70)
    print("Phase 1: CSV Import (Persistent Storage)")
    print("=" * 70)

    # Check CSV directory exists
    if not CSV_DIR.exists():
        print(f"ERROR: CSV directory not found: {CSV_DIR}")
        return 1

    # Get connections
    session_factory = await get_test_session(reset=reset)
    duckdb_conn = get_duckdb_conn(reset=reset)

    async with session_factory() as session:
        # Check what already exists
        print("\n1. Checking existing data...")
        print("-" * 50)

        expected_tables = [Path(f).stem.lower() for f in CSV_FILES]
        existing = await check_tables_exist(session, expected_tables)

        all_exist = all(existing.values())
        if all_exist and not reset:
            print("   All CSV files already loaded!")
            for name, exists in existing.items():
                print_phase_status(f"raw_{name}", exists)
            await print_database_summary(session, duckdb_conn)
            await cleanup_connections()
            return 0

        # Show what needs to be loaded
        for name, exists in existing.items():
            print_phase_status(f"raw_{name}", exists)

        # Load missing CSVs
        print("\n2. Loading CSV files...")
        print("-" * 50)

        from dataraum_context.sources.csv import CSVLoader

        loader = CSVLoader()
        loaded_ids = []

        for csv_file in CSV_FILES:
            csv_path = CSV_DIR / csv_file
            if not csv_path.exists():
                print(f"      Missing: {csv_file}")
                continue

            # Skip if already loaded
            table_name = csv_path.stem.lower()
            if existing.get(table_name, False):
                print(f"      Skipping (exists): {csv_file}")
                continue

            table_id = await load_csv_file(csv_path, session, duckdb_conn, loader)
            if table_id:
                loaded_ids.append(table_id)

        print(f"\n   Loaded {len(loaded_ids)} new tables")

        # Summary
        await print_database_summary(session, duckdb_conn)

    await cleanup_connections()

    print("\n" + "=" * 70)
    print("Phase 1 COMPLETE")
    print("=" * 70)
    print("\nNext: Run run_phase2_typing.py to resolve column types")
    return 0


if __name__ == "__main__":
    reset = "--reset" in sys.argv
    if reset:
        print("Reset mode: will delete existing databases\n")
    exit_code = asyncio.run(main(reset=reset))
    sys.exit(exit_code)
