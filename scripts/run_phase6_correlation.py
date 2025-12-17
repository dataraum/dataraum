#!/usr/bin/env python3
"""Phase 6: Correlation Analysis with persistent storage.

This script analyzes correlations within typed tables:
- Numeric correlations (Pearson, Spearman)
- Categorical associations (Cramér's V)
- Functional dependencies
- Derived columns

Prerequisites:
    - Phase 5 must be completed (run_phase5_semantic.py)
    - Or at minimum Phase 3 (run_phase3_statistics.py)

Usage:
    uv run python scripts/run_phase6_correlation.py
"""

import asyncio
import sys
from typing import Any

from sqlalchemy import func, select
from test_infra import (
    cleanup_connections,
    get_duckdb_conn,
    get_test_session,
    print_database_summary,
    print_phase_status,
)


async def main() -> int:
    """Run Phase 6: Correlation Analysis."""
    print("=" * 70)
    print("Phase 6: Correlation Analysis (Persistent Storage)")
    print("=" * 70)

    # Get connections
    session_factory = await get_test_session()
    duckdb_conn = get_duckdb_conn()

    async with session_factory() as session:
        from dataraum_context.analysis.correlation.db_models import (
            CorrelationAnalysisRun,
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
            print("   WARNING: No statistical profiles found!")
            print("   Recommend running run_phase3_statistics.py first.")

        print(f"   Found {len(typed_tables)} typed tables")
        print(f"   Found {profile_count} statistical profiles")

        # Check what's already analyzed (use CorrelationAnalysisRun with target_type='table')
        analyzed_stmt = select(CorrelationAnalysisRun.target_id).where(
            CorrelationAnalysisRun.target_type == "table"
        )
        analyzed_table_ids = set((await session.execute(analyzed_stmt)).scalars().all())

        unanalyzed_tables = [t for t in typed_tables if t.table_id not in analyzed_table_ids]

        if not unanalyzed_tables:
            print("\n   All tables already analyzed!")
            for tt in typed_tables:
                print_phase_status(f"corr_{tt.table_name}", True)
            await _print_correlation_summary(session)
            await print_database_summary(session, duckdb_conn)
            await cleanup_connections()
            return 0

        # Show what needs analysis
        print("\n   Correlation analysis status:")
        for tt in typed_tables:
            is_analyzed = tt.table_id in analyzed_table_ids
            print_phase_status(f"corr_{tt.table_name}", is_analyzed)

        # Run correlation analysis
        print("\n2. Running correlation analysis...")
        print("-" * 50)

        from datetime import UTC, datetime

        from dataraum_context.analysis.correlation import analyze_correlations
        from dataraum_context.storage import Column

        analyzed_count = 0

        for typed_table in unanalyzed_tables:
            print(f"      Processing: {typed_table.table_name}...")

            result = await analyze_correlations(typed_table.table_id, duckdb_conn, session)
            if not result.success:
                print(f"         ERROR: {result.error}")
                continue

            corr_result = result.unwrap()
            print(f"         Numeric correlations: {len(corr_result.numeric_correlations)}")
            print(f"         Categorical associations: {len(corr_result.categorical_associations)}")
            print(f"         Functional dependencies: {len(corr_result.functional_dependencies)}")
            print(f"         Derived columns: {len(corr_result.derived_columns)}")
            print(f"         Duration: {corr_result.duration_seconds:.2f}s")

            # Create CorrelationAnalysisRun record to track completion
            columns_stmt = select(Column).where(Column.table_id == typed_table.table_id)
            columns = (await session.execute(columns_stmt)).scalars().all()

            run_record = CorrelationAnalysisRun(
                target_id=typed_table.table_id,
                target_type="table",
                rows_analyzed=0,  # Within-table analysis doesn't have row counts
                columns_analyzed=len(columns),
                started_at=corr_result.computed_at,
                completed_at=datetime.now(UTC),
                duration_seconds=corr_result.duration_seconds,
            )
            session.add(run_record)

            analyzed_count += 1

        # Commit the analysis run records
        await session.commit()

        print(f"\n   Analyzed {analyzed_count} tables")

        # Summary
        await _print_correlation_summary(session)
        await print_database_summary(session, duckdb_conn)

    await cleanup_connections()

    print("\n" + "=" * 70)
    print("Phase 6 COMPLETE")
    print("=" * 70)
    print("\nAll phases complete! Data is ready for downstream analysis.")
    return 0


async def _print_correlation_summary(session: Any) -> None:
    """Print summary of correlation analysis results."""
    from dataraum_context.analysis.correlation.db_models import (
        CategoricalAssociation,
        ColumnCorrelation,
        DerivedColumn,
        FunctionalDependency,
    )

    numeric_count = (
        await session.execute(select(func.count(ColumnCorrelation.correlation_id)))
    ).scalar()
    cat_count = (
        await session.execute(select(func.count(CategoricalAssociation.association_id)))
    ).scalar()
    fd_count = (
        await session.execute(select(func.count(FunctionalDependency.dependency_id)))
    ).scalar()
    derived_count = (await session.execute(select(func.count(DerivedColumn.derived_id)))).scalar()

    print("\nCorrelation Analysis Results:")
    print(f"  Numeric correlations: {numeric_count}")
    print(f"  Categorical associations: {cat_count}")
    print(f"  Functional dependencies: {fd_count}")
    print(f"  Derived columns: {derived_count}")


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
