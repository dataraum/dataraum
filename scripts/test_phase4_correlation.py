#!/usr/bin/env python3
"""Phase 4 verification script: analysis/correlation module.

This script verifies that the correlation algorithms actually work
by testing with real data from finance_csv_example.

Tests:
1. Per-table: Numeric correlations, derived columns, functional dependencies, categorical associations
2. Cross-table: Correlation analysis across multiple tables using relationships

The cross-table analysis helps debug the new correlation integration.
"""

import asyncio
from pathlib import Path

import duckdb
import numpy as np
from sqlalchemy import event, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from dataraum_context.analysis.correlation import (
    analyze_correlations,
    analyze_cross_table_correlations,
    compute_categorical_associations,
    compute_numeric_correlations,
    detect_derived_columns,
    detect_functional_dependencies,
)
from dataraum_context.analysis.correlation.algorithms.multicollinearity import (
    compute_multicollinearity,
)
from dataraum_context.analysis.correlation.models import EnrichedRelationship
from dataraum_context.analysis.relationships import detect_relationships
from dataraum_context.analysis.typing import infer_type_candidates, resolve_types
from dataraum_context.core.models import SourceConfig
from dataraum_context.core.models.base import Cardinality, RelationshipType
from dataraum_context.sources.csv import CSVLoader
from dataraum_context.storage import Column, Table, init_database


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
        junk_columns = ["column0", "column00", "Unnamed: 0.2", "Unnamed: 0.1", "Unnamed: 0"]
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

        # Step 6: Cross-table correlation analysis (NEW)
        print("\n" + "=" * 70)
        print("6. Cross-Table Correlation Analysis (Multiple Tables)")
        print("=" * 70)

        # Load additional tables for cross-table analysis
        csv_dir = Path("examples/finance_csv_example")
        additional_csvs = [
            csv_dir / "customer_table.csv",
            csv_dir / "vendor_table.csv",
            csv_dir / "payment_method.csv",
        ]

        all_table_ids = [typed_table.table_id]
        table_name_map = {typed_table.table_name: typed_table}

        print("\n6a. Loading additional tables for cross-table analysis:")
        print("-" * 50)

        for csv_path in additional_csvs:
            if not csv_path.exists():
                print(f"   Missing: {csv_path}")
                continue

            config = SourceConfig(name=csv_path.stem.lower(), source_type="csv", path=str(csv_path))
            load_result = await loader.load(config, duckdb_conn, session)
            if not load_result.success:
                print(f"   Failed: {csv_path.name} - {load_result.error}")
                continue

            staged = load_result.unwrap().tables[0]
            raw_table = await session.get(Table, staged.table_id)
            if not raw_table:
                continue

            # Drop junk columns
            for junk in ["column0", "column00", "Unnamed: 0.2", "Unnamed: 0.1", "Unnamed: 0"]:
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

            # Type resolution
            await infer_type_candidates(raw_table, duckdb_conn, session)
            resolve_result = await resolve_types(staged.table_id, duckdb_conn, session)
            if resolve_result.success:
                resolution = resolve_result.unwrap()
                typed = await _get_typed_table(resolution.typed_table_name, session)
                if typed:
                    all_table_ids.append(typed.table_id)
                    table_name_map[typed.table_name] = typed
                    print(f"   {csv_path.name} -> {resolution.typed_table_name}")

        print(f"\n   Total tables for cross-table analysis: {len(all_table_ids)}")

        # 6b. Detect relationships
        print("\n6b. Detecting relationships between tables:")
        print("-" * 50)
        rel_result = await detect_relationships(all_table_ids, duckdb_conn, session)
        relationship_candidates = []
        if rel_result.success:
            r = rel_result.unwrap()
            print(f"   Found {r.total_candidates} relationship candidates")
            print(f"   High confidence: {r.high_confidence_count}")

            for candidate in r.candidates:
                relationship_candidates.append(candidate)
                print(f"\n   {candidate.table1} <-> {candidate.table2}")
                print(
                    f"   confidence={candidate.confidence:.2f}, topo_sim={candidate.topology_similarity:.2f}"
                )
                for jc in candidate.join_candidates[:3]:
                    print(f"      {jc.column1} <-> {jc.column2}: {jc.confidence:.2f}")
        else:
            print(f"   ERROR: {rel_result.error}")

        # 6c. Build EnrichedRelationship objects
        print("\n6c. Building enriched relationships:")
        print("-" * 50)
        import time

        start_6c = time.time()
        enriched_relationships = []
        query_count = 0

        for rc in relationship_candidates:
            table1 = table_name_map.get(rc.table1)
            table2 = table_name_map.get(rc.table2)
            if not table1 or not table2:
                continue

            # Limit to top 3 join candidates per relationship to avoid slowness
            for jc in rc.join_candidates:
                col1_stmt = select(Column).where(
                    Column.table_id == table1.table_id, Column.column_name == jc.column1
                )
                col2_stmt = select(Column).where(
                    Column.table_id == table2.table_id, Column.column_name == jc.column2
                )
                col1 = (await session.execute(col1_stmt)).scalar_one_or_none()
                col2 = (await session.execute(col2_stmt)).scalar_one_or_none()
                query_count += 2

                if not col1 or not col2:
                    continue

                cardinality = None
                if jc.cardinality == "one_to_one":
                    cardinality = Cardinality.ONE_TO_ONE
                elif jc.cardinality in ["one_to_many", "many_to_one"]:
                    cardinality = Cardinality.ONE_TO_MANY

                enriched_relationships.append(
                    EnrichedRelationship(
                        relationship_id=f"{col1.column_id}_{col2.column_id}",
                        from_table=table1.table_name,
                        from_column=col1.column_name,
                        from_column_id=col1.column_id,
                        from_table_id=table1.table_id,
                        to_table=table2.table_name,
                        to_column=col2.column_name,
                        to_column_id=col2.column_id,
                        to_table_id=table2.table_id,
                        relationship_type=RelationshipType.FOREIGN_KEY,
                        cardinality=cardinality,
                        confidence=jc.confidence,
                        detection_method="candidate",
                    )
                )

        print(f"   Built {len(enriched_relationships)} enriched relationships")
        print(f"   Queries executed: {query_count}")
        print(f"   Time: {time.time() - start_6c:.2f}s")

        # 6d. Run cross-table correlation analysis
        print("\n6d. Running cross-table correlation analysis:")
        print("-" * 50)

        if len(all_table_ids) >= 2 and enriched_relationships:
            corr_result = await analyze_cross_table_correlations(
                table_ids=all_table_ids,
                relationships=enriched_relationships,
                duckdb_conn=duckdb_conn,
                session=session,
            )

            if corr_result.success and corr_result.value:
                analysis = corr_result.value
                print(f"   Tables analyzed: {', '.join(analysis.table_names)}")
                print(f"   Numeric columns analyzed: {analysis.total_columns_analyzed}")
                print(f"   Relationships used: {analysis.total_relationships_used}")
                print(f"   Overall condition index: {analysis.overall_condition_index:.1f}")
                print(f"   Overall severity: {analysis.overall_severity}")
                print(f"   Dependency groups: {len(analysis.dependency_groups)}")
                print(f"   Cross-table groups: {len(analysis.cross_table_groups)}")

                if analysis.dependency_groups:
                    print("\n   Dependency groups (all):")
                    for i, group in enumerate(analysis.dependency_groups):
                        cols = [f"{t}.{c}" for t, c in group.involved_columns]
                        tables = list({t for t, _ in group.involved_columns})
                        print(
                            f"   Group {i + 1}: CI={group.condition_index:.1f} ({group.severity})"
                        )
                        print(f"      Tables: {tables}")
                        print(f"      Columns: {cols}")
                        print(f"      VDPs: {[f'{v:.3f}' for v in group.variance_proportions]}")

                if analysis.cross_table_groups:
                    print("\n   Cross-table dependency groups:")
                    for group in analysis.cross_table_groups:
                        cols = [f"{t}.{c}" for t, c in group.involved_columns]
                        print(f"      CI={group.condition_index:.1f}: {cols}")
                else:
                    print("\n   No cross-table dependency groups found.")
                    print("   (Dependencies are within single tables, not spanning tables)")

                # Show numeric correlations
                if analysis.numeric_correlations:
                    print(f"\n   Numeric correlations: {len(analysis.numeric_correlations)}")
                    print(f"   Cross-table correlations: {len(analysis.cross_table_correlations)}")
                    for corr in analysis.cross_table_correlations[:5]:
                        print(
                            f"      {corr.table1}.{corr.column1} <-> {corr.table2}.{corr.column2}: "
                            f"r={corr.pearson_r:.3f}, rho={corr.spearman_rho:.3f} ({corr.strength})"
                        )

                # Show categorical associations
                if analysis.categorical_associations:
                    print(
                        f"\n   Categorical associations: {len(analysis.categorical_associations)}"
                    )
                    print(f"   Cross-table associations: {len(analysis.cross_table_associations)}")
                    for assoc in analysis.cross_table_associations[:5]:
                        print(
                            f"      {assoc.table1}.{assoc.column1} <-> {assoc.table2}.{assoc.column2}: "
                            f"V={assoc.cramers_v:.3f} ({assoc.strength})"
                        )
            else:
                print(f"   ERROR: {corr_result.error}")
        else:
            print("   Skipped (need >= 2 tables with relationships)")

        # 6e. Debug: Show numeric columns per table
        print("\n6e. Debug - Numeric columns per table:")
        print("-" * 50)
        for table_id in all_table_ids:
            table = await session.get(Table, table_id)
            if not table:
                continue

            cols_stmt = select(Column).where(Column.table_id == table_id)
            cols = (await session.execute(cols_stmt)).scalars().all()

            numeric_cols = [
                c
                for c in cols
                if c.resolved_type in ["INTEGER", "BIGINT", "DOUBLE", "DECIMAL", "FLOAT"]
            ]
            print(f"\n   {table.table_name}:")
            print(f"      Total columns: {len(cols)}")
            print(f"      Numeric columns: {len(numeric_cols)}")
            for nc in numeric_cols:
                print(f"         - {nc.column_name} ({nc.resolved_type})")

        # 6f. Debug: Direct correlation matrix computation
        print("\n6f. Debug - Direct correlation matrix:")
        print("-" * 50)

        # Get all numeric columns
        all_numeric_cols = []
        for table_id in all_table_ids:
            table = await session.get(Table, table_id)
            if not table:
                continue
            cols_stmt = select(Column).where(
                Column.table_id == table_id,
                Column.resolved_type.in_(["INTEGER", "BIGINT", "DOUBLE", "DECIMAL", "FLOAT"]),
            )
            cols = (await session.execute(cols_stmt)).scalars().all()
            for col in cols:
                all_numeric_cols.append((table, col))

        if len(all_numeric_cols) >= 2:
            print(f"   Found {len(all_numeric_cols)} numeric columns total")

            # Build a simple SELECT query for the first table
            first_table = all_numeric_cols[0][0]
            first_cols = [(t, c) for t, c in all_numeric_cols if t.table_id == first_table.table_id]

            if len(first_cols) >= 2:
                col_names = [f'"{c.column_name}"' for _, c in first_cols]
                query = f"SELECT {', '.join(col_names)} FROM {first_table.duckdb_path} LIMIT 1000"
                print(f"\n   Query: {query[:100]}...")

                try:
                    data = duckdb_conn.execute(query).fetchnumpy()
                    X = np.column_stack([data[c.column_name] for _, c in first_cols])
                    X = X[~np.isnan(X).any(axis=1)]
                    print(f"   Rows after removing NULLs: {len(X)}")

                    if len(X) >= 10:
                        # Filter out zero-variance columns
                        col_stds = np.std(X, axis=0)
                        valid_mask = col_stds > 1e-10
                        valid_cols = [c for c, v in zip(first_cols, valid_mask, strict=False) if v]
                        X_valid = X[:, valid_mask]

                        if len(valid_cols) < 2:
                            print("   Not enough non-zero-variance columns")
                        else:
                            if sum(~valid_mask) > 0:
                                skipped = [
                                    col.column_name
                                    for (_, col), v in zip(first_cols, valid_mask, strict=False)
                                    if not v
                                ]
                                print(f"   Skipped zero-variance columns: {skipped}")

                            corr_matrix = np.corrcoef(X_valid, rowvar=False)
                            print(f"\n   Correlation matrix ({first_table.table_name}):")
                            col_labels = [c.column_name[:15] for _, c in valid_cols]
                            print("   " + " ".join(f"{lbl:>15}" for lbl in col_labels))
                            for i, (_, c) in enumerate(valid_cols):
                                row = " ".join(
                                    f"{corr_matrix[i, j]:>15.3f}" for j in range(len(valid_cols))
                                )
                                print(f"   {c.column_name[:15]:>15} {row}")

                            # Run multicollinearity on this
                            mc_result = compute_multicollinearity(corr_matrix)
                            print("\n   Multicollinearity analysis:")
                            print(f"      Condition index: {mc_result.overall_condition_index:.1f}")
                            print(f"      Severity: {mc_result.overall_severity}")
                            print(f"      Dependency groups: {len(mc_result.dependency_groups)}")

                            for group in mc_result.dependency_groups:
                                involved = [
                                    valid_cols[i][1].column_name for i in group.involved_col_indices
                                ]
                                print(f"      Group: {involved}")
                                print(
                                    f"         CI={group.condition_index:.1f}, VDPs={group.variance_proportions}"
                                )
                except Exception as e:
                    print(f"   ERROR: {e}")
        else:
            print("   Not enough numeric columns for correlation analysis")

    duckdb_conn.close()
    await engine.dispose()

    print("\n" + "=" * 70)
    print("Phase 4 verification COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(main())
