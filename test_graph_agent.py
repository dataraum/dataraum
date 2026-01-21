#!/usr/bin/env python3
"""Test script for GraphAgent with existing E2E data."""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

import duckdb
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from dataraum_context.analysis.correlation import db_models as corr_models  # noqa: F401
from dataraum_context.analysis.cycles import db_models as cycle_models  # noqa: F401
from dataraum_context.analysis.quality_summary import db_models as quality_models  # noqa: F401
from dataraum_context.analysis.relationships import db_models as rel_models  # noqa: F401
from dataraum_context.analysis.semantic import db_models as semantic_models  # noqa: F401
from dataraum_context.analysis.slicing import db_models as slice_models  # noqa: F401
from dataraum_context.analysis.statistics import db_models as stats_models  # noqa: F401
from dataraum_context.analysis.typing import db_models as typing_models  # noqa: F401
from dataraum_context.entropy import db_models as entropy_models  # noqa: F401
from dataraum_context.graphs import db_models as graph_models  # noqa: F401
from dataraum_context.graphs.agent import ExecutionContext, GraphAgent
from dataraum_context.graphs.context import build_execution_context, format_context_for_prompt
from dataraum_context.graphs.field_mapping import can_execute_metric, load_semantic_mappings
from dataraum_context.graphs.loader import GraphLoader
from dataraum_context.llm import create_provider, load_llm_config
from dataraum_context.llm.cache import LLMCache
from dataraum_context.llm.prompts import PromptRenderer

# Import all db_models to register them
from dataraum_context.storage import Table


def main():
    output_dir = Path("/tmp/e2e-test")
    metadata_db = output_dir / "metadata.db"
    data_db = output_dir / "data.duckdb"

    if not metadata_db.exists():
        print(f"Error: {metadata_db} not found. Run the E2E test first.")
        return

    # Connect to databases
    engine = create_engine(f"sqlite:///{metadata_db}")
    duckdb_conn = duckdb.connect(str(data_db), read_only=True)

    with Session(engine) as session:
        # Get typed tables
        stmt = select(Table).where(Table.layer == "typed")
        tables = session.execute(stmt).scalars().all()
        table_ids = [t.table_id for t in tables]

        print("=" * 60)
        print("GRAPH AGENT TEST")
        print("=" * 60)

        print(f"\nFound {len(tables)} typed tables:")
        for t in tables:
            print(f"  - {t.table_name}")

        # Load field mappings
        print("\n--- Field Mappings ---")
        field_mappings = load_semantic_mappings(session, table_ids)
        print(f"Available business concepts: {field_mappings.available_concepts}")
        for concept in field_mappings.available_concepts:
            col = field_mappings.get_column(concept)
            if col:
                print(f"  {concept} -> {col.table_name}.{col.column_name}")

        # Load graphs
        print("\n--- Loading Graphs ---")
        loader = GraphLoader()
        graphs = loader.load_all()
        print(f"Loaded {len(graphs)} graphs")

        metric_graphs = loader.get_metric_graphs()
        print(f"Metric graphs: {[g.graph_id for g in metric_graphs]}")

        # Check which metrics can be executed
        print("\n--- Metric Execution Feasibility ---")
        for graph in metric_graphs:
            # Get required standard fields
            required_fields = []
            for step in graph.steps.values():
                if step.source and step.source.standard_field:
                    required_fields.append(step.source.standard_field)

            can_exec, missing = can_execute_metric(field_mappings, required_fields)
            status = "YES" if can_exec else f"NO (missing: {missing})"
            print(f"  {graph.graph_id}: requires {required_fields} -> {status}")

        # Build rich execution context
        print("\n--- Building Execution Context ---")
        rich_context = build_execution_context(
            session=session,
            table_ids=table_ids,
            duckdb_conn=duckdb_conn,
        )
        print(f"  Tables: {rich_context.total_tables}")
        print(f"  Columns: {rich_context.total_columns}")
        print(f"  Relationships: {rich_context.total_relationships}")
        print(f"  Business cycles: {len(rich_context.business_cycles)}")
        if rich_context.field_mappings:
            print(f"  Field mappings: {rich_context.field_mappings.available_concepts}")

        # Show a preview of the formatted context
        print("\n--- Context Preview (first 2000 chars) ---")
        formatted = format_context_for_prompt(rich_context)
        print(formatted[:2000])
        if len(formatted) > 2000:
            print(f"\n... ({len(formatted)} total chars)")

        # Try to execute DSO metric (which needs inference for revenue)
        print("\n" + "=" * 60)
        print("ATTEMPTING DSO METRIC EXECUTION")
        print("=" * 60)

        dso_graph = loader.get_graph("dso")
        if not dso_graph:
            print("DSO graph not found")
            return

        # Initialize LLM
        try:
            config = load_llm_config()
            provider_config = config.providers.get(config.active_provider)
            provider = create_provider(config.active_provider, provider_config.model_dump())
            renderer = PromptRenderer()
            cache = LLMCache()

            agent = GraphAgent(
                config=config,
                provider=provider,
                prompt_renderer=renderer,
                cache=cache,
            )

            # Create execution context with rich metadata
            # Find primary table (master_txn_table for this dataset)
            primary_table = next(
                (
                    t
                    for t in tables
                    if "master" in t.table_name.lower() or "txn" in t.table_name.lower()
                ),
                tables[0] if tables else None,
            )

            if not primary_table:
                print("No suitable table found")
                return

            print(f"\nUsing primary table: typed_{primary_table.table_name}")

            exec_context = ExecutionContext.with_rich_context(
                session=session,
                duckdb_conn=duckdb_conn,
                table_name=f"typed_{primary_table.table_name}",
                table_ids=table_ids,
                entropy_behavior_mode="balanced",
            )

            # Execute the graph
            print("\nExecuting DSO graph...")

            # First just generate SQL to see what it produces
            print("\n--- Generated SQL Debug ---")
            gen_result = agent._generate_sql(
                session, dso_graph, exec_context, {"days_in_period": 30}
            )
            if gen_result.success:
                generated = gen_result.value
                print(f"Column mappings: {generated.column_mappings}")
                print(f"Steps ({len(generated.steps)}):")
                for step in generated.steps:
                    print(f"  {step.get('step_id')}: {step.get('sql')[:200]}...")
                print(f"\nFinal SQL:\n{generated.final_sql}")
            else:
                print(f"Generation failed: {gen_result.error}")

            result = agent.execute(
                session=session,
                graph=dso_graph,
                context=exec_context,
                force_regenerate=True,  # Force fresh generation to test
            )

            if result.success:
                execution = result.value
                print("\nSUCCESS!")
                print(f"  Output value: {execution.output_value}")
                print(f"  Interpretation: {execution.output_interpretation}")
                print(f"  Execution hash: {execution.execution_hash}")
                print(f"  Steps executed: {len(execution.step_results)}")
                for step in execution.step_results:
                    print(
                        f"    - {step.step_id}: {step.value_scalar or step.value_string or step.value_boolean}"
                    )
            else:
                print(f"\nFAILED: {result.error}")

        except Exception as e:
            print(f"\nError: {e}")
            import traceback

            traceback.print_exc()


if __name__ == "__main__":
    main()
