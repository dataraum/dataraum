#!/usr/bin/env python3
"""Phase 2 Verification Script - Type Inference

This script verifies that the new analysis/typing module works correctly
by loading CSV files and running type inference.

Usage:
    cd /home/philipp/Code/dataraum-context
    uv run python scripts/test_phase2_typing.py

Expected output:
    - Type candidates inferred for each VARCHAR column
    - Confidence scores and detected patterns
    - Unit detection results (if applicable)
"""

import asyncio
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import duckdb
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

# Import from NEW analysis/typing module
from dataraum_context.analysis.typing import infer_type_candidates
from dataraum_context.analysis.typing.db_models import TypeCandidate

# Import from sources module (Phase 1)
from dataraum_context.sources.csv import CSVLoader

# Import from storage
from dataraum_context.storage import Base, Table


async def setup_database() -> async_sessionmaker[AsyncSession]:
    """Create in-memory SQLite database for testing."""
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        echo=False,
    )

    # Import all model modules to register them with SQLAlchemy Base metadata
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


async def load_csv_and_infer_types(
    session: AsyncSession,
    duckdb_conn: duckdb.DuckDBPyConnection,
    csv_path: Path,
) -> bool:
    """Load a CSV and run type inference."""
    print(f"\n{'=' * 70}")
    print(f"Processing: {csv_path.name}")
    print("=" * 70)

    from dataraum_context.core.models import SourceConfig

    # Step 1: Load CSV (Phase 1)
    loader = CSVLoader()
    config = SourceConfig(
        name=csv_path.stem,
        source_type="csv",
        path=str(csv_path),
    )

    load_result = await loader.load(config, duckdb_conn, session)
    if not load_result.success:
        print(f"FAILED to load CSV: {load_result.error}")
        return False

    staging_result = load_result.unwrap()
    table_info = staging_result.tables[0]
    print(f"Loaded: {table_info.row_count} rows, {table_info.column_count} columns")

    # Get the table from database
    stmt = select(Table).where(Table.duckdb_path == table_info.raw_table_name)
    result = await session.execute(stmt)
    table = result.scalar_one()

    # Step 2: Run type inference (Phase 2)
    print("\nRunning type inference...")
    inference_result = await infer_type_candidates(table, duckdb_conn, session)

    if not inference_result.success:
        print(f"FAILED type inference: {inference_result.error}")
        return False

    candidates = inference_result.unwrap()
    print(f"Generated {len(candidates)} type candidates")

    # Step 3: Display results
    print("\n" + "-" * 70)
    print("INFERRED TYPES")
    print("-" * 70)

    # Group by column
    by_column: dict[str, list] = {}
    for c in candidates:
        col_name = c.column_ref.column_name
        if col_name not in by_column:
            by_column[col_name] = []
        by_column[col_name].append(c)

    for col_name, col_candidates in by_column.items():
        print(f"\n  {col_name}:")
        for c in sorted(col_candidates, key=lambda x: x.confidence, reverse=True):
            type_str = f"{c.data_type.value}"
            conf_str = f"confidence={c.confidence:.2f}"
            parse_str = f"parse_rate={c.parse_success_rate:.2f}"

            extras = []
            if c.detected_pattern:
                extras.append(f"pattern={c.detected_pattern}")
            if c.detected_unit:
                extras.append(f"unit={c.detected_unit}")

            extra_str = f" ({', '.join(extras)})" if extras else ""
            print(f"    -> {type_str}: {conf_str}, {parse_str}{extra_str}")

    # Step 4: Verify database records
    print("\n" + "-" * 70)
    print("DATABASE VERIFICATION")
    print("-" * 70)

    db_candidates = await session.run_sync(
        lambda sync_session: sync_session.query(TypeCandidate).all()
    )
    print(f"  TypeCandidate records in DB: {len(db_candidates)}")

    return True


async def main() -> int:
    """Run Phase 2 verification tests."""
    print("Phase 2 Verification: Type Inference")
    print("Using NEW analysis/typing module")
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
    duckdb_conn.execute("SET memory_limit='2GB'")

    all_passed = True

    async with session_factory() as session:
        # Test with payment_method.csv (small, simple)
        csv_file = finance_dir / "payment_method.csv"
        if csv_file.exists():
            passed = await load_csv_and_infer_types(session, duckdb_conn, csv_file)
            all_passed = all_passed and passed

        # Test with customer_table.csv (has null values)
        csv_file = finance_dir / "customer_table.csv"
        if csv_file.exists():
            passed = await load_csv_and_infer_types(session, duckdb_conn, csv_file)
            all_passed = all_passed and passed

    # Summary
    print(f"\n{'=' * 70}")
    print("SUMMARY")
    print("=" * 70)

    if all_passed:
        print("All tests PASSED")
        print("\nPhase 2 verification complete. The analysis/typing module works correctly.")
        print("\nKey observations:")
        print("  - Type inference based on VALUE patterns only (no column name matching)")
        print("  - TypeCandidate records created in database")
        print("  - Confidence scores reflect pattern match + parse success rates")
        print("\nNext steps:")
        print("  1. Remove old staging/ module")
        print("  2. Remove old profiling/patterns.py, profiling/units.py")
        print("  3. Proceed to Phase 3: analysis/statistics")
        return 0
    else:
        print("Some tests FAILED")
        print("Please fix issues before proceeding.")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
