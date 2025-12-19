#!/usr/bin/env python3
"""Phase 10b: Within-Table Topological Analysis.

This script runs TDA (Topological Data Analysis) on individual tables:
- Betti numbers (connected components, cycles, voids)
- Persistence diagrams
- Persistent entropy (structural complexity)
- Cycle detection with persistence
- Stability assessment

This complements the multi-table financial analysis by providing
within-table structural insights.

Prerequisites:
    - Phase 2 must be completed (typed tables exist)

Usage:
    uv run python scripts/run_phase10_topology.py
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
    """Run Phase 10b: Within-Table Topological Analysis."""
    print("=" * 70)
    print("Phase 10b: Within-Table Topological Analysis (TDA)")
    print("=" * 70)

    # Get connections
    session_factory = await get_test_session()
    duckdb_conn = get_duckdb_conn()

    async with session_factory() as session:
        from dataraum_context.analysis.topology import (
            TopologicalQualityMetrics,
            analyze_topological_quality,
        )
        from dataraum_context.storage import Table

        # Check prerequisites
        print("\n1. Checking prerequisites...")
        print("-" * 50)

        typed_tables_stmt = select(Table).where(Table.layer == "typed")
        typed_tables = (await session.execute(typed_tables_stmt)).scalars().all()

        if not typed_tables:
            print("   ERROR: No typed tables found!")
            print("   Please run run_phase2_typing.py first.")
            await cleanup_connections()
            return 1

        print(f"   Found {len(typed_tables)} typed tables:")
        for t in typed_tables:
            print(f"      - {t.table_name}")

        # Check what's already analyzed
        analyzed_stmt = select(TopologicalQualityMetrics.table_id).distinct()
        analyzed_ids = set((await session.execute(analyzed_stmt)).scalars().all())

        unanalyzed = [t for t in typed_tables if t.table_id not in analyzed_ids]

        if not unanalyzed:
            print("\n   All tables already have topological analysis!")
            for t in typed_tables:
                print_phase_status(f"topology_{t.table_name}", True)
        else:
            print(f"\n   Tables needing analysis: {len(unanalyzed)}")
            for t in unanalyzed:
                print_phase_status(f"topology_{t.table_name}", False)

        # Run topological analysis
        print("\n2. Running topological analysis...")
        print("-" * 50)

        results = {}
        for typed_table in typed_tables:
            print(f"\n   Analyzing: {typed_table.table_name}...")

            result = await analyze_topological_quality(
                table_id=typed_table.table_id,
                duckdb_conn=duckdb_conn,
                session=session,
                min_persistence=0.1,
                stability_threshold=0.1,
            )

            if not result.success:
                print(f"      ERROR: {result.error}")
                continue

            tq = result.unwrap()
            results[typed_table.table_name] = tq

            # Print results
            print(
                f"      Betti numbers: b0={tq.betti_numbers.betti_0}, "
                f"b1={tq.betti_numbers.betti_1}, b2={tq.betti_numbers.betti_2}"
            )
            print(f"      Total complexity: {tq.betti_numbers.total_complexity}")
            print(f"      Persistent entropy: {tq.persistent_entropy:.4f}")
            print(f"      Structural complexity: {tq.structural_complexity:.4f}")

            if tq.persistent_cycles:
                print(f"      Persistent cycles: {len(tq.persistent_cycles)}")
                for cycle in tq.persistent_cycles[:3]:
                    print(
                        f"         - dim={cycle.dimension}, "
                        f"persistence={cycle.persistence:.3f}, "
                        f"birth={cycle.birth:.3f}, death={cycle.death:.3f}"
                    )
                if len(tq.persistent_cycles) > 3:
                    print(f"         ... and {len(tq.persistent_cycles) - 3} more")

            if tq.stability:
                print(
                    f"      Stability: "
                    f"is_stable={tq.stability.is_stable}, "
                    f"level={tq.stability.stability_level}"
                )

            if tq.orphaned_components > 0:
                print(f"      WARNING: {tq.orphaned_components} orphaned components")

            if tq.has_anomalies:
                print(f"      ANOMALIES DETECTED: {len(tq.anomalies)}")
                for anomaly in tq.anomalies[:3]:
                    print(f"         - {anomaly.anomaly_type}: {anomaly.description}")

        # Summary
        print("\n" + "-" * 50)
        print("3. Topological Analysis Summary")
        print("-" * 50)

        if results:
            print("\n   BETTI NUMBERS (Structural Characteristics):")
            print("   " + "-" * 45)
            print(f"   {'Table':<25} {'b0':>5} {'b1':>5} {'b2':>5} {'Total':>8}")
            print("   " + "-" * 45)
            for name, tq in results.items():
                b = tq.betti_numbers
                print(
                    f"   {name:<25} {b.betti_0:>5} {b.betti_1:>5} {b.betti_2:>5} "
                    f"{b.total_complexity:>8}"
                )

            print("\n   INTERPRETATION:")
            print("   " + "-" * 45)
            print("   b0 = Connected components (should be 1 for integrated data)")
            print("   b1 = Cycles (redundant relationships or loops)")
            print("   b2 = Voids (3D holes, usually 0 for tabular data)")

            # Find interesting patterns
            high_b1_tables = [
                (name, tq) for name, tq in results.items() if tq.betti_numbers.betti_1 > 0
            ]
            if high_b1_tables:
                print("\n   Tables with cycles (b1 > 0):")
                for name, tq in high_b1_tables:
                    print(f"      - {name}: {tq.betti_numbers.betti_1} cycles")

            fragmented_tables = [
                (name, tq) for name, tq in results.items() if tq.betti_numbers.betti_0 > 1
            ]
            if fragmented_tables:
                print("\n   Tables with multiple components (b0 > 1):")
                for name, tq in fragmented_tables:
                    print(f"      - {name}: {tq.betti_numbers.betti_0} components")

            high_entropy_tables = [
                (name, tq)
                for name, tq in results.items()
                if tq.persistent_entropy is not None and tq.persistent_entropy > 2.0
            ]
            if high_entropy_tables:
                print("\n   Tables with high structural complexity (entropy > 2.0):")
                for name, tq in high_entropy_tables:
                    print(f"      - {name}: entropy={tq.persistent_entropy:.4f}")

            anomaly_tables = [(name, tq) for name, tq in results.items() if tq.has_anomalies]
            if anomaly_tables:
                print("\n   Tables with anomalies:")
                for name, tq in anomaly_tables:
                    print(f"      - {name}: {len(tq.anomalies)} anomalies")
                    for a in tq.anomalies:
                        print(f"         [{a.severity}] {a.anomaly_type}")

        await print_database_summary(session, duckdb_conn)

        # Show metrics count
        metrics_count = (
            await session.execute(select(func.count(TopologicalQualityMetrics.metric_id)))
        ).scalar()
        print(f"\nTopological quality metrics records: {metrics_count}")

    await cleanup_connections()

    print("\n" + "=" * 70)
    print("Phase 10b COMPLETE")
    print("=" * 70)
    print("\nWithin-table topological analysis complete.")
    print("Use run_phase10_multi_table_financial.py for cross-table business cycle analysis.")
    return 0


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
