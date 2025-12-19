#!/usr/bin/env python3
"""Phase 10: Multi-Table Financial Quality Analysis.

This script runs cross-table financial analysis on the dataset:
- Gathers LLM-detected relationships between tables
- Analyzes relationship structure (pattern, hubs, cycles)
- Classifies business cycles (AR, AP, Revenue, Expense) with LLM
- Generates holistic dataset interpretation

Prerequisites:
    - Phase 2 must be completed (typed tables exist)
    - Phase 5 must be completed (LLM semantic enrichment with relationships)
    - ANTHROPIC_API_KEY environment variable must be set

Usage:
    ANTHROPIC_API_KEY=sk-... uv run python scripts/run_phase10_multi_table_financial.py
"""

from __future__ import annotations

import asyncio
import sys
from typing import TYPE_CHECKING

from dotenv import load_dotenv
from infra import (
    cleanup_connections,
    get_duckdb_conn,
    get_test_session,
    print_database_summary,
    print_phase_status,
)
from sqlalchemy import func, select

if TYPE_CHECKING:
    from dataraum_context.llm.providers.base import LLMProvider

# Load environment variables from .env
load_dotenv()


def get_llm_provider() -> LLMProvider:
    """Get LLM provider. Uses config from config/llm.yaml and .env file."""
    from dataraum_context.llm.config import load_llm_config
    from dataraum_context.llm.providers import create_provider

    llm_config = load_llm_config()
    provider_config = llm_config.providers["anthropic"]

    provider = create_provider(
        "anthropic",
        {
            "api_key_env": provider_config.api_key_env,
            "default_model": provider_config.default_model,
            "models": provider_config.models,
            "base_url_env": provider_config.base_url_env,
        },
    )
    return provider


async def main() -> int:
    """Run Phase 10: Multi-Table Financial Quality Analysis."""
    print("=" * 70)
    print("Phase 10: Multi-Table Financial Quality Analysis")
    print("=" * 70)

    # Get connections
    session_factory = await get_test_session()
    duckdb_conn = get_duckdb_conn()

    async with session_factory() as session:
        from dataraum_context.analysis.relationships.db_models import Relationship
        from dataraum_context.analysis.topology.db_models import (
            BusinessCycleClassification,
            MultiTableTopologyMetrics,
        )
        from dataraum_context.domains.financial import (
            analyze_complete_financial_dataset_quality,
        )
        from dataraum_context.storage import Table

        # Check prerequisites
        print("\n1. Checking prerequisites...")
        print("-" * 50)

        # Check typed tables
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

        # Check for LLM-detected relationships
        llm_rel_count = (
            await session.execute(
                select(func.count(Relationship.relationship_id)).where(
                    Relationship.detection_method == "llm"
                )
            )
        ).scalar() or 0

        if llm_rel_count == 0:
            print("\n   WARNING: No LLM-detected relationships found!")
            print("   Run run_phase5_semantic.py first for best results.")
            print("   Continuing with candidate relationships...")

        print(f"   Found {llm_rel_count} LLM-confirmed relationships")
        print_phase_status("multi_table_financial", llm_rel_count > 0)

        # Check if already analyzed
        existing_analysis = (
            await session.execute(select(func.count(MultiTableTopologyMetrics.analysis_id)))
        ).scalar() or 0

        if existing_analysis > 0:
            print(f"\n   Found {existing_analysis} existing multi-table analyses")

        # Initialize LLM provider (required)
        print("\n2. Initializing LLM provider...")
        print("-" * 50)

        try:
            llm_provider = get_llm_provider()
            print("   LLM provider: Anthropic (Claude)")
            print("   Cycle classification: ENABLED")
        except ValueError as e:
            print(f"   ERROR: {e}")
            await cleanup_connections()
            return 1

        # Run multi-table financial analysis
        print("\n3. Running multi-table financial analysis...")
        print("-" * 50)

        table_ids = [t.table_id for t in typed_tables]

        result = await analyze_complete_financial_dataset_quality(
            table_ids=table_ids,
            duckdb_conn=duckdb_conn,
            session=session,
            llm_provider=llm_provider,
        )

        if not result.success:
            print(f"   ERROR: {result.error}")
            await cleanup_connections()
            return 1

        analysis = result.unwrap()

        # Print results
        print("\n4. Analysis Results")
        print("-" * 50)

        # Relationship structure
        print("\n   RELATIONSHIP STRUCTURE:")
        structure = analysis.get("relationship_structure", {})
        print(f"      Pattern: {structure.get('pattern', 'N/A')}")
        print(f"      Description: {structure.get('pattern_description', 'N/A')}")
        print(f"      Total tables: {structure.get('total_tables', 0)}")
        print(f"      Total relationships: {structure.get('total_relationships', 0)}")
        print(f"      Connected components: {structure.get('connected_components', 0)}")

        if structure.get("hub_tables"):
            print(f"      Hub tables: {', '.join(structure['hub_tables'])}")
        if structure.get("leaf_tables"):
            print(f"      Leaf tables: {', '.join(structure['leaf_tables'])}")
        if structure.get("isolated_tables"):
            print(f"      Isolated tables: {', '.join(structure['isolated_tables'])}")

        # Table roles
        print("\n   TABLE ROLES:")
        for table_role in structure.get("tables", []):
            role = table_role.get("role", "unknown")
            name = table_role.get("table_name", "unknown")
            connections = table_role.get("connection_count", 0)
            print(f"      {name}: {role} ({connections} connections)")

        # Cross-table cycles
        cycles = analysis.get("cross_table_cycles", [])
        print(f"\n   CROSS-TABLE CYCLES: {len(cycles)}")
        for i, cycle in enumerate(cycles, 1):
            print(f"      Cycle {i}: {' -> '.join(cycle)} -> {cycle[0]}")

        # Classified cycles (if LLM available)
        classified = analysis.get("classified_cycles", [])
        if classified:
            print(f"\n   CLASSIFIED BUSINESS CYCLES: {len(classified)}")
            for i, c in enumerate(classified, 1):
                print(f"\n      Cycle {i}:")
                print(f"         Tables: {' -> '.join(c.get('cycle_tables', []))}")
                print(f"         Type: {c.get('primary_type', 'UNKNOWN')}")
                print(f"         Business value: {c.get('business_value', 'unknown')}")
                print(f"         Completeness: {c.get('completeness', 'unknown')}")
                if c.get("explanation"):
                    explanation = c["explanation"][:100]
                    print(f"         Explanation: {explanation}...")
                if c.get("cycle_types"):
                    print("         All types:")
                    for ct in c["cycle_types"]:
                        print(f"            - {ct.get('type')}: {ct.get('confidence', 0):.0%}")

        # Missing expected cycles
        missing = analysis.get("missing_expected_cycles", [])
        if missing:
            print(f"\n   MISSING EXPECTED CYCLES: {', '.join(missing)}")

        # Interpretation (if LLM available)
        interpretation = analysis.get("interpretation")
        if interpretation:
            print("\n   LLM INTERPRETATION:")
            print(f"      Summary: {interpretation.get('summary', 'N/A')}")
            if interpretation.get("business_processes"):
                print(
                    f"      Business processes: {', '.join(interpretation['business_processes'])}"
                )
            if interpretation.get("missing_processes"):
                print(f"      Missing processes: {', '.join(interpretation['missing_processes'])}")
            if interpretation.get("data_model_observations"):
                print("      Data model observations:")
                for obs in interpretation["data_model_observations"][:3]:
                    print(f"         - {obs}")
            if interpretation.get("recommendations"):
                print("      Recommendations:")
                for rec in interpretation["recommendations"][:3]:
                    print(f"         - {rec}")

        # Per-table metrics summary
        per_table = analysis.get("per_table_metrics", {})
        if per_table:
            print(f"\n   PER-TABLE FINANCIAL HEALTH: {len(per_table)} tables")
            for table_id, metrics in per_table.items():
                table_name = next(
                    (t.table_name for t in typed_tables if t.table_id == table_id),
                    table_id[:8],
                )
                balanced = metrics.get("double_entry_balanced", "N/A")
                equation = metrics.get("accounting_equation_holds", "N/A")
                print(f"      {table_name}:")
                print(f"         Double-entry balanced: {balanced}")
                print(f"         Accounting equation: {equation}")

        # Persistence info
        if analysis.get("analysis_id"):
            print(f"\n   Persisted as analysis_id: {analysis['analysis_id']}")
        if analysis.get("persisted_cycle_ids"):
            print(f"   Persisted {len(analysis['persisted_cycle_ids'])} cycle classifications")

        # Summary
        print("\n" + "-" * 50)
        print("5. Summary")
        print("-" * 50)
        print(f"   LLM available: {analysis.get('llm_available', False)}")
        print(f"   Tables analyzed: {len(table_ids)}")
        print(f"   Relationships found: {structure.get('total_relationships', 0)}")
        print(f"   Cycles detected: {len(cycles)}")
        print(f"   Cycles classified: {len(classified)}")

        await print_database_summary(session, duckdb_conn)

        # Show topology metrics count
        metrics_count = (
            await session.execute(select(func.count(MultiTableTopologyMetrics.analysis_id)))
        ).scalar()
        cycle_count = (
            await session.execute(select(func.count(BusinessCycleClassification.cycle_id)))
        ).scalar()
        print(f"\nMulti-table topology records: {metrics_count}")
        print(f"Business cycle classifications: {cycle_count}")

    await cleanup_connections()

    print("\n" + "=" * 70)
    print("Phase 10 COMPLETE")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
