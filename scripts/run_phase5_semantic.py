#!/usr/bin/env python3
"""Phase 5: Semantic Analysis with persistent storage.

This script runs LLM-powered semantic analysis on typed tables:
- Semantic roles (measure, dimension, key, etc.)
- Entity types (customer, product, transaction, etc.)
- Business names and descriptions
- Relationship confirmation/refinement

Prerequisites:
    - Phase 4 must be completed (run_phase4_relationships.py)
    - Phase 4b is recommended (run_phase4b_correlations.py) for enriched context
    - ANTHROPIC_API_KEY must be set in environment or .env file

Usage:
    uv run python scripts/run_phase5_semantic.py
"""

import asyncio
import os
import sys
from typing import Any

from dotenv import load_dotenv
from infra import (
    cleanup_connections,
    get_duckdb_conn,
    get_test_session,
    print_database_summary,
    print_phase_status,
)
from sqlalchemy import func, select

# Load environment variables from .env
load_dotenv()


async def main() -> int:
    """Run Phase 5: Semantic Analysis."""
    print("=" * 70)
    print("Phase 5: Semantic Analysis (Persistent Storage)")
    print("=" * 70)

    # Check for API key
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        print("\nERROR: ANTHROPIC_API_KEY not found!")
        print("Please set ANTHROPIC_API_KEY in your environment or .env file.")
        print("\nTo skip semantic analysis, you can manually mark relationships as confirmed.")
        return 1

    print(f"\nUsing Anthropic API key: {api_key[:8]}...{api_key[-4:]}")

    # Get connections
    session_factory = await get_test_session()
    duckdb_conn = get_duckdb_conn()

    async with session_factory() as session:
        from dataraum_context.analysis.relationships.db_models import Relationship
        from dataraum_context.analysis.semantic.db_models import SemanticAnnotation, TableEntity
        from dataraum_context.storage import Table

        # Check prerequisites - need typed tables and relationship candidates
        print("\n1. Checking prerequisites...")
        print("-" * 50)

        typed_tables_stmt = select(Table).where(Table.layer == "typed")
        typed_tables = (await session.execute(typed_tables_stmt)).scalars().all()

        if not typed_tables:
            print("   ERROR: No typed tables found!")
            print("   Please run run_phase2_typing.py first.")
            await cleanup_connections()
            return 1

        # Check for relationship candidates
        rel_count = (
            await session.execute(
                select(func.count(Relationship.relationship_id)).where(
                    Relationship.detection_method == "candidate"
                )
            )
        ).scalar()

        if rel_count == 0:
            print("   WARNING: No relationship candidates found!")
            print("   Recommend running run_phase4_relationships.py first.")

        print(f"   Found {len(typed_tables)} typed tables")
        print(f"   Found {rel_count} relationship candidates")

        # Check if semantic analysis already done
        annotation_count = (
            await session.execute(select(func.count(SemanticAnnotation.annotation_id)))
        ).scalar() or 0
        entity_count = (
            await session.execute(select(func.count(TableEntity.entity_id)))
        ).scalar() or 0

        if annotation_count > 0 or entity_count > 0:
            print("\n   Semantic analysis already performed!")
            print(f"   Annotations: {annotation_count}")
            print(f"   Entities: {entity_count}")
            print_phase_status("semantic", True)
            await _print_semantic_summary(session)
            await print_database_summary(session, duckdb_conn)
            await cleanup_connections()
            return 0

        print_phase_status("semantic", False)

        # Load relationship candidates to pass to semantic agent
        print("\n2. Loading relationship candidates...")
        print("-" * 50)

        from dataraum_context.analysis.relationships import (
            load_relationship_candidates_for_semantic,
        )

        table_ids = [t.table_id for t in typed_tables]
        relationship_candidates = await load_relationship_candidates_for_semantic(
            session, table_ids=table_ids, detection_method="candidate"
        )

        # Count total join columns across all candidates
        total_joins = sum(len(c.get("join_columns", [])) for c in relationship_candidates)
        print(f"   Loaded {len(relationship_candidates)} relationship candidates")
        print(f"   Total join column pairs: {total_joins}")

        # Setup LLM and create semantic agent
        print("\n3. Running semantic analysis...")
        print("-" * 50)

        from dataraum_context.analysis.semantic import SemanticAgent, enrich_semantic
        from dataraum_context.llm.cache import LLMCache
        from dataraum_context.llm.config import load_llm_config
        from dataraum_context.llm.prompts import PromptRenderer
        from dataraum_context.llm.providers import create_provider

        try:
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
            renderer = PromptRenderer()
            cache = LLMCache()

            agent = SemanticAgent(
                config=llm_config,
                provider=provider,
                prompt_renderer=renderer,
                cache=cache,
            )
        except Exception as e:
            print(f"   ERROR creating LLM agent: {e}")
            await cleanup_connections()
            return 1

        # Run semantic enrichment
        table_ids = [t.table_id for t in typed_tables]
        print(f"   Analyzing {len(table_ids)} tables...")

        result = await enrich_semantic(
            session=session,
            agent=agent,
            table_ids=table_ids,
            ontology="financial_reporting",
            relationship_candidates=relationship_candidates,
        )

        if not result.success:
            print(f"   ERROR: {result.error}")
            await cleanup_connections()
            return 1

        enrichment = result.unwrap()
        print("\n   Semantic analysis complete!")
        print(f"   Annotations: {len(enrichment.annotations)}")
        print(f"   Entities: {len(enrichment.entity_detections)}")
        print(f"   Relationships: {len(enrichment.relationships)}")

        # Commit the session to save changes
        await session.commit()

        # Summary
        await _print_semantic_summary(session)
        await print_database_summary(session, duckdb_conn)

    await cleanup_connections()

    print("\n" + "=" * 70)
    print("Phase 5 COMPLETE")
    print("=" * 70)
    print("\nNext: Run run_phase6_correlation.py for correlation analysis")
    return 0


async def _print_semantic_summary(session: Any) -> None:
    """Print summary of semantic analysis results."""
    from dataraum_context.analysis.relationships.db_models import Relationship
    from dataraum_context.analysis.semantic.db_models import SemanticAnnotation, TableEntity

    # Count annotations by source
    source_counts = {}
    for source in ["llm", "manual", "config_override"]:
        count = (
            await session.execute(
                select(func.count(SemanticAnnotation.annotation_id)).where(
                    SemanticAnnotation.annotation_source == source
                )
            )
        ).scalar()
        source_counts[source] = count

    # Count entities
    entity_count = (await session.execute(select(func.count(TableEntity.entity_id)))).scalar()

    # Count LLM-confirmed relationships
    llm_rel_count = (
        await session.execute(
            select(func.count(Relationship.relationship_id)).where(
                Relationship.detection_method == "llm"
            )
        )
    ).scalar()

    print("\nSemantic Analysis Results:")
    print("  Annotations by source:")
    for source, count in source_counts.items():
        print(f"    {source}: {count}")
    print(f"  Entity detections: {entity_count}")
    print(f"  LLM-confirmed relationships: {llm_rel_count}")


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
