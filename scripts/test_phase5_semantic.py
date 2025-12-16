#!/usr/bin/env python3
"""Phase 5 verification: analysis/semantic module with relationship candidates + correlation context.

This script demonstrates the full semantic analysis flow:
1. Load CSV files
2. Run type inference and resolution
3. Run statistical profiling
4. Detect relationship candidates (Phase 6) - stored in DB
5. Run cross-table correlation analysis (NEW)
6. Run semantic analysis with relationship candidates AND correlation context
7. Print the LLM prompt for inspection
"""

import asyncio
import os
from pathlib import Path

import duckdb
from dotenv import load_dotenv
from sqlalchemy import event, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from dataraum_context.analysis.correlation import analyze_cross_table_correlations
from dataraum_context.analysis.relationships import detect_relationships
from dataraum_context.analysis.relationships.models import EnrichedRelationship
from dataraum_context.analysis.semantic import SemanticAgent, enrich_semantic
from dataraum_context.analysis.statistics import profile_statistics
from dataraum_context.analysis.typing import infer_type_candidates, resolve_types
from dataraum_context.core.models import SourceConfig
from dataraum_context.core.models.base import Cardinality, RelationshipType
from dataraum_context.llm.cache import LLMCache
from dataraum_context.llm.config import load_llm_config
from dataraum_context.llm.prompts import PromptRenderer
from dataraum_context.llm.providers import create_provider
from dataraum_context.sources.csv import CSVLoader
from dataraum_context.storage import Column, Table, init_database

# Load environment variables from .env
load_dotenv()


async def main():
    print("=" * 70)
    print("Phase 5: analysis/semantic with relationship candidates")
    print("=" * 70)

    # Check for API key
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        print("\nWARNING: ANTHROPIC_API_KEY not found in .env file")
        print("LLM analysis will be skipped.")
    else:
        print(f"\nUsing Anthropic API key: {api_key[:8]}...{api_key[-4:]}")

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

    # Load CSV files
    csv_dir = Path("examples/finance_csv_example")
    csv_files = [
        csv_dir / "customer_table.csv",
        csv_dir / "vendor_table.csv",
        csv_dir / "payment_method.csv",
    ]

    print(f"\n1. Loading {len(csv_files)} tables")
    print("-" * 50)

    table_ids = []
    async with async_session() as session:
        loader = CSVLoader()

        for csv_path in csv_files:
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
            for junk in ["column00", "Unnamed: 0.2", "Unnamed: 0.1", "Unnamed: 0"]:
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
                stmt = select(Table).where(Table.duckdb_path == resolution.typed_table_name)
                typed = (await session.execute(stmt)).scalar_one_or_none()
                if typed:
                    table_ids.append(typed.table_id)
                    print(f"   {csv_path.name} -> {resolution.typed_table_name}")

        # Statistical profiling
        print("\n2. Statistical profiling")
        print("-" * 50)
        for table_id in table_ids:
            table = await session.get(Table, table_id)
            if table:
                result = await profile_statistics(table, duckdb_conn, session)
                if result.success:
                    stats = result.unwrap()
                    print(f"   {table.table_name}: {len(stats.column_profiles)} columns profiled")

        # Relationship detection (Phase 6) - now stores candidates in DB
        print("\n3. Relationship detection (Phase 6)")
        print("-" * 50)
        rel_result = await detect_relationships(table_ids, duckdb_conn, session)
        relationship_candidates = []
        if rel_result.success:
            r = rel_result.unwrap()
            print(f"   Found {r.total_candidates} relationship candidates (stored in DB)")
            print(f"   High confidence: {r.high_confidence_count}")
            print(f"   Duration: {r.duration_seconds:.2f}s")

            # Convert to dict format for semantic analysis
            for candidate in r.candidates:
                relationship_candidates.append(
                    {
                        "table1": candidate.table1,
                        "table2": candidate.table2,
                        "confidence": candidate.confidence,
                        "topology_similarity": candidate.topology_similarity,
                        "relationship_type": candidate.relationship_type,
                        "join_columns": [
                            {
                                "column1": jc.column1,
                                "column2": jc.column2,
                                "confidence": jc.confidence,
                                "cardinality": jc.cardinality,
                            }
                            for jc in candidate.join_candidates
                        ],
                    }
                )

            # Print candidates
            for rc in relationship_candidates:
                print(f"\n   {rc['table1']} <-> {rc['table2']}")
                print(f"   conf={rc['confidence']:.2f}, topo_sim={rc['topology_similarity']:.2f}")
                for jc in rc["join_columns"][:3]:  # Show first 3
                    print(f"      {jc['column1']} <-> {jc['column2']}: {jc['confidence']:.2f}")

        # Cross-table correlation analysis (NEW)
        print("\n4. Cross-table correlation analysis")
        print("-" * 50)
        correlation_context = None

        if len(table_ids) > 1 and relationship_candidates:
            # Convert relationship candidates to EnrichedRelationship objects
            enriched_relationships = []

            # Build table name -> table_id mapping for typed tables only
            table_name_to_id = {}
            for tid in table_ids:
                t = await session.get(Table, tid)
                if t:
                    table_name_to_id[t.table_name] = t

            for rc in relationship_candidates:
                # Get tables from our mapping
                table1 = table_name_to_id.get(rc["table1"])
                table2 = table_name_to_id.get(rc["table2"])

                if not table1 or not table2:
                    continue

                for jc in rc["join_columns"]:
                    # Get column IDs
                    col1_stmt = select(Column).where(
                        Column.table_id == table1.table_id,
                        Column.column_name == jc["column1"],
                    )
                    col2_stmt = select(Column).where(
                        Column.table_id == table2.table_id,
                        Column.column_name == jc["column2"],
                    )
                    col1 = (await session.execute(col1_stmt)).scalar_one_or_none()
                    col2 = (await session.execute(col2_stmt)).scalar_one_or_none()

                    if not col1 or not col2:
                        continue

                    # Map cardinality string to enum
                    cardinality = None
                    card_str = jc.get("cardinality", "")
                    if card_str == "one_to_one":
                        cardinality = Cardinality.ONE_TO_ONE
                    elif card_str in ["one_to_many", "many_to_one"]:
                        cardinality = Cardinality.ONE_TO_MANY
                    elif card_str == "many_to_many":
                        cardinality = Cardinality.MANY_TO_MANY

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
                            confidence=jc["confidence"],
                            detection_method="candidate",
                        )
                    )

            if enriched_relationships:
                print(f"   Built {len(enriched_relationships)} enriched relationships")

                # Run cross-table correlation analysis
                corr_result = await analyze_cross_table_correlations(
                    table_ids=table_ids,
                    relationships=enriched_relationships,
                    duckdb_conn=duckdb_conn,
                    session=session,
                )

                if corr_result.success and corr_result.value:
                    correlation_context = corr_result.value
                    print(
                        f"   Overall condition index: {correlation_context.overall_condition_index:.1f}"
                    )
                    print(f"   Overall severity: {correlation_context.overall_severity}")
                    print(f"   Columns analyzed: {correlation_context.total_columns_analyzed}")
                    print(f"   Dependency groups: {len(correlation_context.dependency_groups)}")
                    print(f"   Cross-table groups: {len(correlation_context.cross_table_groups)}")

                    if correlation_context.cross_table_groups:
                        print("\n   Cross-table dependency groups:")
                        for group in correlation_context.cross_table_groups:
                            cols = [f"{t}.{c}" for t, c in group.involved_columns]
                            print(
                                f"      CI={group.condition_index:.1f} ({group.severity}): {', '.join(cols)}"
                            )
                else:
                    print(f"   Correlation analysis failed: {corr_result.error}")
            else:
                print("   No enriched relationships could be built")
        else:
            print("   Skipped (requires multiple tables with relationships)")

        # Semantic analysis (Phase 5)
        print("\n5. Semantic analysis with relationship candidates + correlation context")
        print("-" * 50)

        if not api_key:
            print("   Skipping - no API key configured")
            duckdb_conn.close()
            await engine.dispose()
            return

        # Load LLM config and create agent
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

            # Print the prompt that will be sent to the LLM
            print("\n" + "=" * 70)
            print("LLM PROMPT (for inspection)")
            print("=" * 70)

            # Build context and render prompt manually to show it
            import json

            from dataraum_context.analysis.statistics.models import ColumnProfile
            from dataraum_context.core.models.base import ColumnRef
            from dataraum_context.llm.privacy import DataSampler

            # Load profiles for context
            profiles = []
            for table_id in table_ids:
                table = await session.get(Table, table_id)
                if table:
                    cols_stmt = select(Column).where(Column.table_id == table_id)
                    cols = (await session.execute(cols_stmt)).scalars().all()
                    for col in cols:
                        profiles.append(
                            ColumnProfile(
                                column_id=col.column_id,
                                column_ref=ColumnRef(
                                    table_name=table.table_name, column_name=col.column_name
                                ),
                                profiled_at=table.created_at,
                                total_count=table.row_count or 0,
                                null_count=0,
                                distinct_count=0,
                                null_ratio=0.0,
                                cardinality_ratio=0.0,
                                top_values=[],
                            )
                        )

            # Build tables JSON
            sampler = DataSampler(llm_config.privacy)
            samples = sampler.prepare_samples(profiles)
            tables_json = agent._build_tables_json(profiles, samples)

            # Format relationship candidates
            rel_candidates_str = agent._format_relationship_candidates(relationship_candidates)

            # Load ontology
            ontology_def = agent._ontology_loader.load("general")
            ontology_concepts = agent._ontology_loader.format_concepts_for_prompt(ontology_def)

            # Format correlation context
            corr_context_str = agent._format_correlation_context(correlation_context)

            context = {
                "tables_json": json.dumps(tables_json, indent=2),
                "ontology_name": "general",
                "ontology_concepts": ontology_concepts,
                "relationship_candidates": rel_candidates_str,
                "correlation_context": corr_context_str,
            }

            prompt, temperature = renderer.render("semantic_analysis", context)
            print(f"\nTemperature: {temperature}")
            print(f"Prompt length: {len(prompt)} characters")

            # Find and show the correlation context section
            if "Cross-Table Correlation Analysis" in prompt:
                idx = prompt.find("Cross-Table Correlation Analysis")
                end_idx = (
                    prompt.find("##", idx + 10) if prompt.find("##", idx + 10) > 0 else idx + 1500
                )
                print("\n--- CORRELATION CONTEXT SECTION ---")
                print(prompt[idx:end_idx])
                print("--- END CORRELATION SECTION ---\n")

            print("\n--- PROMPT START (first 3000 chars) ---")
            print(prompt[:3000])
            if len(prompt) > 3000:
                print(f"\n... [{len(prompt) - 3000} more characters] ...")
            print("--- PROMPT END ---\n")

            # Run semantic enrichment with relationship candidates AND correlation context
            print("\n6. Running LLM analysis...")
            print("-" * 50)

            sem_result = await enrich_semantic(
                session=session,
                agent=agent,
                table_ids=table_ids,
                ontology="general",
                relationship_candidates=relationship_candidates,
                correlation_context=correlation_context,
            )

            if sem_result.success:
                enrichment = sem_result.unwrap()
                print(f"   Annotations: {len(enrichment.annotations)}")
                print(f"   Entity detections: {len(enrichment.entity_detections)}")
                print(f"   Relationships: {len(enrichment.relationships)}")

                # Show entity detections
                print("\n   Entity detections:")
                for entity in enrichment.entity_detections:
                    print(f"      {entity.table_name}: {entity.entity_type}")
                    if entity.description:
                        desc = (
                            entity.description[:80] + "..."
                            if len(entity.description) > 80
                            else entity.description
                        )
                        print(f"         {desc}")

                # Show relationships
                print("\n   LLM-confirmed relationships:")
                for rel in enrichment.relationships:
                    print(
                        f"      {rel.from_table}.{rel.from_column} -> "
                        f"{rel.to_table}.{rel.to_column}"
                    )
                    print(f"         type={rel.relationship_type}, conf={rel.confidence:.2f}")
                    if rel.evidence and "reasoning" in rel.evidence:
                        reasoning = rel.evidence["reasoning"]
                        if len(reasoning) > 80:
                            reasoning = reasoning[:80] + "..."
                        print(f"         reasoning: {reasoning}")
            else:
                print(f"   Semantic analysis failed: {sem_result.error}")

        except Exception as e:
            import traceback

            print(f"   Error: {e}")
            traceback.print_exc()

    duckdb_conn.close()
    await engine.dispose()
    print("\n" + "=" * 70)
    print("Phase 5 COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(main())
