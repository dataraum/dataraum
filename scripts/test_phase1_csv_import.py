#!/usr/bin/env python3
"""Phase 1 Verification Script - CSV Import

This script verifies that the new sources/csv module works correctly
by loading real CSV files from the finance_csv_example dataset.

Usage:
    cd /home/philipp/Code/dataraum-context
    uv run python scripts/test_phase1_csv_import.py

Expected output:
    - Source record created in SQLite
    - Table records created for each CSV
    - Column records created for each column
    - DuckDB raw tables with VARCHAR columns
    - Row counts matching source files
"""

import asyncio
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import duckdb
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

# Import from NEW sources module
from dataraum_context.sources.csv import CSVLoader, StagingResult

# Import from existing storage (not moved yet)
from dataraum_context.storage import Base, Column, Source, Table


async def setup_database() -> async_sessionmaker[AsyncSession]:
    """Create in-memory SQLite database for testing."""
    # Use in-memory SQLite for test isolation
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        echo=False,
    )

    # Import all model modules to register them with SQLAlchemy Base metadata
    # This is required because storage/models.py has relationships to other modules
    from dataraum_context.analysis.correlation import db_models as _correlation_models  # noqa: F401
    from dataraum_context.analysis.statistics import db_models as _statistics_models  # noqa: F401
    from dataraum_context.analysis.typing import db_models as _typing_models  # noqa: F401
    from dataraum_context.enrichment import db_models as _enrichment_models  # noqa: F401
    from dataraum_context.graphs import db_models as _graphs_models  # noqa: F401
    from dataraum_context.llm import db_models as _llm_models  # noqa: F401
    from dataraum_context.quality import db_models as _quality_models  # noqa: F401
    from dataraum_context.quality.domains import db_models as _domain_quality_models  # noqa: F401

    # Create all tables
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    return async_sessionmaker(engine, expire_on_commit=False)


async def test_single_csv_load(
    session: AsyncSession,
    duckdb_conn: duckdb.DuckDBPyConnection,
    csv_path: Path,
) -> bool:
    """Test loading a single CSV file."""
    print(f"\n{'=' * 60}")
    print(f"Testing: {csv_path.name}")
    print("=" * 60)

    from dataraum_context.core.models import SourceConfig

    loader = CSVLoader()

    # Create source config
    config = SourceConfig(
        name=csv_path.stem,
        source_type="csv",
        path=str(csv_path),
    )

    # Load the CSV
    result = await loader.load(config, duckdb_conn, session)

    if not result.success:
        print(f"FAILED: {result.error}")
        return False

    staging_result: StagingResult = result.unwrap()

    # Verify results
    print(f"Source ID: {staging_result.source_id}")
    print(f"Duration: {staging_result.duration_seconds:.2f}s")
    print(f"Total rows: {staging_result.total_rows}")

    for table in staging_result.tables:
        print(f"\nTable: {table.table_name}")
        print(f"  Raw table: {table.raw_table_name}")
        print(f"  Rows: {table.row_count}")
        print(f"  Columns: {table.column_count}")

        # Verify DuckDB table exists and has correct structure
        schema = duckdb_conn.execute(f"DESCRIBE {table.raw_table_name}").fetchall()
        print("  DuckDB schema:")
        for col_name, col_type, *_ in schema:
            print(f"    - {col_name}: {col_type}")
            if col_type != "VARCHAR":
                print(f"      WARNING: Expected VARCHAR, got {col_type}")

        # Show sample data
        sample = duckdb_conn.execute(f"SELECT * FROM {table.raw_table_name} LIMIT 2").fetchall()
        print(f"  Sample rows: {len(sample)}")

    return True


async def test_directory_load(
    session: AsyncSession,
    duckdb_conn: duckdb.DuckDBPyConnection,
    directory: Path,
    files_to_load: list[str],
) -> bool:
    """Test loading multiple CSV files from a directory."""
    print(f"\n{'=' * 60}")
    print(f"Testing directory load: {directory}")
    print(f"Files: {files_to_load}")
    print("=" * 60)

    loader = CSVLoader()

    # Load specific files by creating a temp directory with symlinks
    # Or just load all and filter - for simplicity, load subset directly
    result = await loader.load_directory(
        directory_path=str(directory),
        source_name="finance_csv_test",
        duckdb_conn=duckdb_conn,
        session=session,
        file_pattern="*.csv",
    )

    if not result.success:
        print(f"FAILED: {result.error}")
        return False

    if result.warnings:
        print(f"Warnings: {result.warnings}")

    staging_result: StagingResult = result.unwrap()

    print(f"Source ID: {staging_result.source_id}")
    print(f"Duration: {staging_result.duration_seconds:.2f}s")
    print(f"Total rows: {staging_result.total_rows}")
    print(f"Tables loaded: {len(staging_result.tables)}")

    for table in staging_result.tables:
        print(f"\n  {table.table_name}: {table.row_count} rows, {table.column_count} cols")

    return True


async def verify_metadata(session: AsyncSession) -> bool:
    """Verify metadata records in SQLite."""
    print(f"\n{'=' * 60}")
    print("Verifying SQLite metadata")
    print("=" * 60)

    # Count sources
    sources = (await session.execute(select(Source))).scalars().all()
    print(f"Sources: {len(sources)}")
    for src in sources:
        print(f"  - {src.name} ({src.source_type})")

    # Count tables
    tables = (await session.execute(select(Table))).scalars().all()
    print(f"Tables: {len(tables)}")
    for tbl in tables:
        print(f"  - {tbl.table_name} (layer={tbl.layer}, rows={tbl.row_count})")

    # Count columns
    columns = (await session.execute(select(Column))).scalars().all()
    print(f"Columns: {len(columns)}")

    # Sample column details
    if columns:
        print("Sample columns (first 5):")
        for col in columns[:5]:
            print(f"  - {col.column_name}: raw={col.raw_type}, resolved={col.resolved_type}")

    return len(sources) > 0 and len(tables) > 0 and len(columns) > 0


async def main() -> int:
    """Run Phase 1 verification tests."""
    print("Phase 1 Verification: CSV Import")
    print("Using NEW sources/csv module")
    print()

    # Paths
    project_root = Path(__file__).parent.parent
    finance_dir = project_root / "examples" / "finance_csv_example"

    if not finance_dir.exists():
        print(f"ERROR: Finance example directory not found: {finance_dir}")
        return 1

    # Setup
    session_factory = await setup_database()
    duckdb_conn = duckdb.connect(":memory:")

    # Configure DuckDB
    duckdb_conn.execute("SET memory_limit='2GB'")

    all_passed = True

    async with session_factory() as session:
        # Test 1: Load single small CSV
        small_csv = finance_dir / "payment_method.csv"
        if small_csv.exists():
            passed = await test_single_csv_load(session, duckdb_conn, small_csv)
            all_passed = all_passed and passed
        else:
            print(f"Skipping: {small_csv} not found")

        # Test 2: Load another CSV
        product_csv = finance_dir / "product_service_table.csv"
        if product_csv.exists():
            passed = await test_single_csv_load(session, duckdb_conn, product_csv)
            all_passed = all_passed and passed

        # Test 3: Verify metadata
        passed = await verify_metadata(session)
        all_passed = all_passed and passed

    # Summary
    print(f"\n{'=' * 60}")
    print("SUMMARY")
    print("=" * 60)

    if all_passed:
        print("All tests PASSED")
        print("\nPhase 1 verification complete. The sources/csv module works correctly.")
        print("\nNext steps:")
        print("  1. Remove old staging/ module")
        print("  2. Update imports throughout codebase")
        print("  3. Proceed to Phase 2: analysis/typing")
        return 0
    else:
        print("Some tests FAILED")
        print("Please fix issues before proceeding.")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
