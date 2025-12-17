#!/usr/bin/env python3
"""Phase 4: Relationship Detection with persistent storage.

This script detects relationships between typed tables using:
- TDA (Topological Data Analysis) for structural similarity
- Join column detection via value overlap
- Cardinality analysis

Prerequisites:
    - Phase 3 must be completed (run_phase3_statistics.py)

Usage:
    uv run python scripts/run_phase4_relationships.py
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
    """Run Phase 4: Relationship Detection."""
    print("=" * 70)
    print("Phase 4: Relationship Detection (Persistent Storage)")
    print("=" * 70)

    # Get connections
    session_factory = await get_test_session()
    duckdb_conn = get_duckdb_conn()

    async with session_factory() as session:
        from dataraum_context.analysis.relationships.db_models import Relationship
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

        # Check if relationships already detected (detection_method='candidate')
        existing_rel_count = (
            await session.execute(
                select(func.count(Relationship.relationship_id)).where(
                    Relationship.detection_method == "candidate"
                )
            )
        ).scalar() or 0

        if existing_rel_count > 0:
            print(f"\n   Already detected {existing_rel_count} relationship candidates!")
            print_phase_status("relationships", True)
            await _print_relationship_summary(session)
            await print_database_summary(session, duckdb_conn)
            await cleanup_connections()
            return 0

        print_phase_status("relationships", False)

        # Run relationship detection
        print("\n2. Running relationship detection...")
        print("-" * 50)

        from dataraum_context.analysis.relationships import detect_relationships

        table_ids = [t.table_id for t in typed_tables]
        print(f"   Analyzing relationships between {len(table_ids)} tables...")

        result = await detect_relationships(
            table_ids,
            duckdb_conn,
            session,
            min_confidence=0.3,
            sample_percent=10.0,
            evaluate=True,
        )

        if not result.success:
            print(f"   ERROR: {result.error}")
            await cleanup_connections()
            return 1

        detection_result = result.unwrap()
        print(f"\n   Found {len(detection_result.candidates)} relationship candidates")
        print(f"   Duration: {detection_result.duration_seconds:.2f}s")

        # Show top candidates
        if detection_result.candidates:
            print("\n   Top relationship candidates:")
            for candidate in sorted(
                detection_result.candidates, key=lambda c: c.confidence, reverse=True
            )[:10]:
                conf = f"{candidate.confidence:.2f}"
                rel_type = candidate.relationship_type or "unknown"
                print(f"      {candidate.table1} <-> {candidate.table2}: {conf} ({rel_type})")
                if candidate.join_candidates:
                    best_join = max(candidate.join_candidates, key=lambda j: j.confidence)
                    print(f"         Best join: {best_join.column1} <-> {best_join.column2}")

        # Summary
        await _print_relationship_summary(session)
        await print_database_summary(session, duckdb_conn)

    await cleanup_connections()

    print("\n" + "=" * 70)
    print("Phase 4 COMPLETE")
    print("=" * 70)
    print("\nNext: Run run_phase5_semantic.py for semantic analysis")
    return 0


async def _print_relationship_summary(session: Any) -> None:
    """Print summary of relationship detection results."""
    from dataraum_context.analysis.relationships.db_models import Relationship

    # Count by detection method
    method_counts = {}
    for method in ["candidate", "llm", "manual"]:
        count = (
            await session.execute(
                select(func.count(Relationship.relationship_id)).where(
                    Relationship.detection_method == method
                )
            )
        ).scalar()
        method_counts[method] = count

    # Count by cardinality
    cardinality_counts = {}
    for card in ["1:1", "1:N", "N:1", "N:M"]:
        count = (
            await session.execute(
                select(func.count(Relationship.relationship_id)).where(
                    Relationship.cardinality == card
                )
            )
        ).scalar()
        cardinality_counts[card] = count

    confirmed_count = (
        await session.execute(
            select(func.count(Relationship.relationship_id)).where(
                Relationship.is_confirmed == True  # noqa: E712
            )
        )
    ).scalar()

    print("\nRelationship Detection Results:")
    print("  By detection method:")
    for method, count in method_counts.items():
        print(f"    {method}: {count}")
    print("  By cardinality:")
    for card, count in cardinality_counts.items():
        if count > 0:
            print(f"    {card}: {count}")
    print(f"  Confirmed: {confirmed_count}")


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
