#!/usr/bin/env python3
"""Phase 4 verification script: analysis/correlation module.

This script verifies that the correlation algorithms actually work
by testing with real data from finance_csv_example.

Tests:
1. Numeric correlations (Pearson/Spearman) - Credit, Debit, Amount should correlate
2. Derived columns - Credit = Quantity * Rate (verified in raw data)
3. Functional dependencies - Product_Service determines behavior
4. Categorical associations - Transaction type vs payment method
"""

import asyncio
from pathlib import Path

import duckdb
from sqlalchemy import event, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from dataraum_context.analysis.correlation import (
    analyze_correlations,
    compute_categorical_associations,
    compute_numeric_correlations,
    detect_derived_columns,
    detect_functional_dependencies,
)
from dataraum_context.analysis.typing import infer_type_candidates, resolve_types
from dataraum_context.core.models import SourceConfig
from dataraum_context.sources.csv import CSVLoader
from dataraum_context.storage import Table, init_database


async def _get_typed_table(typed_table_name: str, session) -> Table | None:
    """Get the Table object for a typed table by DuckDB path."""
    stmt = select(Table).where(Table.duckdb_path == typed_table_name)
    result = await session.execute(stmt)
    return result.scalar_one_or_none()


async def main():
    """Run Phase 4 verification."""
    print("=" * 70)
    print("Phase 4 Verification: analysis/correlation module")
    print("=" * 70)

    # Setup in-memory databases
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)

    @event.listens_for(engine.sync_engine, "connect")
    def set_sqlite_pragma(dbapi_conn, connection_record):
        cursor = dbapi_conn.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    await init_database(engine)
    async_session = async_sessionmaker(engine, expire_on_commit=False)

    duckdb_conn = duckdb.connect(":memory:")

    # Find CSV file - use smaller file first for quick test
    csv_path = Path("examples/finance_csv_example/Master_txn_table.csv")
    if not csv_path.exists():
        print(f"ERROR: CSV file not found: {csv_path}")
        return

    print(f"\n1. Loading CSV: {csv_path}")

    async with async_session() as session:
        # Step 1: Load CSV
        loader = CSVLoader()
        config = SourceConfig(
            name="master_txn",
            source_type="csv",
            path=str(csv_path),
        )

        load_result = await loader.load(config, duckdb_conn, session)
        if not load_result.success:
            print(f"   ERROR: {load_result.error}")
            return

        staged_table = load_result.unwrap().tables[0]
        print(f"   Staged table ID: {staged_table.table_id}")

        # Get SQLAlchemy Table object
        raw_table = await session.get(Table, staged_table.table_id)
        if not raw_table:
            print("   ERROR: Could not find raw table")
            return
        print(f"   Raw table: {raw_table.duckdb_path}")

        # Drop junk columns (index columns from CSV export)
        print("\n   Dropping junk columns...")
        junk_columns = ["column00", "Unnamed: 0.2", "Unnamed: 0.1", "Unnamed: 0"]
        from dataraum_context.storage import Column

        for junk_col in junk_columns:
            try:
                # Drop from DuckDB
                duckdb_conn.execute(f'ALTER TABLE {raw_table.duckdb_path} DROP COLUMN "{junk_col}"')
                # Delete from SQLAlchemy metadata
                stmt = select(Column).where(
                    Column.table_id == raw_table.table_id,
                    Column.column_name == junk_col,
                )
                result = await session.execute(stmt)
                col = result.scalar_one_or_none()
                if col:
                    await session.delete(col)
                print(f"      Dropped: {junk_col}")
            except Exception:
                pass  # Column may not exist
        await session.commit()

        # Step 2: Type inference
        print("\n2. Running type inference...")
        infer_result = await infer_type_candidates(raw_table, duckdb_conn, session)
        if not infer_result.success:
            print(f"   ERROR: {infer_result.error}")
            return
        print(f"   Generated {len(infer_result.unwrap())} type candidates")

        # Step 3: Type resolution
        print("\n3. Resolving types...")
        resolve_result = await resolve_types(staged_table.table_id, duckdb_conn, session)
        if not resolve_result.success:
            print(f"   ERROR: {resolve_result.error}")
            return

        resolution = resolve_result.unwrap()
        print(f"   Typed table: {resolution.typed_table_name}")
        print(f"   Total rows: {resolution.total_rows}")

        # Get typed table
        typed_table = await _get_typed_table(resolution.typed_table_name, session)
        if not typed_table:
            print("   ERROR: Could not find typed table")
            return

        # Step 4: Test individual correlation algorithms
        print("\n" + "=" * 70)
        print("4. Testing Individual Correlation Algorithms")
        print("=" * 70)

        # 4a. Numeric Correlations
        print("\n4a. Numeric Correlations (Pearson & Spearman):")
        print("-" * 50)
        numeric_result = await compute_numeric_correlations(
            typed_table, duckdb_conn, session, min_correlation=0.3
        )
        if numeric_result.success:
            correlations = numeric_result.unwrap()
            print(f"   Found {len(correlations)} significant correlations (|r| >= 0.3)")
            for corr in correlations[:5]:
                print(f"   • {corr.column1_name} ↔ {corr.column2_name}:")
                print(f"     Pearson r={corr.pearson_r:.3f}, Spearman ρ={corr.spearman_rho:.3f}")
                print(
                    f"     Strength: {corr.correlation_strength}, Significant: {corr.is_significant}"
                )
        else:
            print(f"   ERROR: {numeric_result.error}")

        # 4b. Derived Columns
        print("\n4b. Derived Column Detection:")
        print("-" * 50)
        derived_result = await detect_derived_columns(
            typed_table, duckdb_conn, session, min_match_rate=0.90
        )
        if derived_result.success:
            derived = derived_result.unwrap()
            print(f"   Found {len(derived)} derived columns")
            for d in derived:
                print(f"   • {d.derived_column_name} = {d.formula}")
                print(f"     Match rate: {d.match_rate:.1%} ({d.matching_rows}/{d.total_rows})")
                print(f"     Type: {d.derivation_type}")
            if not derived:
                print("   WARNING: No derived columns detected!")
                print("   Expected: Credit = Quantity * Rate")
        else:
            print(f"   ERROR: {derived_result.error}")

        # 4c. Functional Dependencies
        print("\n4c. Functional Dependency Detection:")
        print("-" * 50)
        fd_result = await detect_functional_dependencies(
            typed_table, duckdb_conn, session, min_confidence=0.95
        )
        if fd_result.success:
            fds = fd_result.unwrap()
            print(f"   Found {len(fds)} functional dependencies (confidence >= 95%)")
            for fd in fds[:10]:
                det_cols = ", ".join(fd.determinant_column_names)
                print(f"   • {det_cols} → {fd.dependent_column_name}")
                print(f"     Confidence: {fd.confidence:.1%}, Violations: {fd.violation_count}")
        else:
            print(f"   ERROR: {fd_result.error}")

        # 4d. Categorical Associations
        print("\n4d. Categorical Associations (Cramér's V):")
        print("-" * 50)
        cat_result = await compute_categorical_associations(
            typed_table, duckdb_conn, session, max_distinct_values=50, min_cramers_v=0.1
        )
        if cat_result.success:
            associations = cat_result.unwrap()
            print(f"   Found {len(associations)} categorical associations (V >= 0.1)")
            for assoc in associations[:5]:
                print(f"   • {assoc.column1_name} ↔ {assoc.column2_name}:")
                print(f"     Cramér's V={assoc.cramers_v:.3f}, χ²={assoc.chi_square:.1f}")
                print(f"     Strength: {assoc.association_strength}, p={assoc.p_value:.4f}")
        else:
            print(f"   ERROR: {cat_result.error}")

        # 4e. Multicollinearity (VIF, Condition Index)
        print("\n4e. Multicollinearity (VIF, Condition Index):")
        print("-" * 50)
        from dataraum_context.analysis.correlation.multicollinearity import (
            compute_multicollinearity_for_table,
        )

        mc_result = await compute_multicollinearity_for_table(typed_table, duckdb_conn, session)
        if mc_result.success:
            mc = mc_result.unwrap()
            print(f"   Overall severity: {mc.overall_severity}")
            print(f"   Columns with VIF > 10: {mc.num_problematic_columns}")
            if mc.condition_index:
                ci = mc.condition_index
                print(f"   Condition Index: {ci.condition_index:.1f} ({ci.severity})")
            if mc.column_vifs:
                print("   Top VIF columns:")
                sorted_vifs = sorted(mc.column_vifs, key=lambda v: v.vif, reverse=True)
                for vif in sorted_vifs[:5]:
                    print(f"   • {vif.column_ref.column_name}: VIF={vif.vif:.1f}")
        else:
            print(f"   ERROR: {mc_result.error}")

        # Step 5: Test the main orchestrator
        print("\n" + "=" * 70)
        print("5. Testing Main Orchestrator (analyze_correlations)")
        print("=" * 70)

        full_result = await analyze_correlations(typed_table.table_id, duckdb_conn, session)
        if full_result.success:
            analysis = full_result.unwrap()
            print(f"   Numeric correlations: {len(analysis.numeric_correlations)}")
            print(f"   Categorical associations: {len(analysis.categorical_associations)}")
            print(f"   Functional dependencies: {len(analysis.functional_dependencies)}")
            print(f"   Derived columns: {len(analysis.derived_columns)}")
            print(f"   Significant correlations: {analysis.significant_correlations}")
            print(f"   Total column pairs: {analysis.total_column_pairs}")
        else:
            print(f"   ERROR: {full_result.error}")

    duckdb_conn.close()
    await engine.dispose()

    print("\n" + "=" * 70)
    print("Phase 4 verification COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(main())
