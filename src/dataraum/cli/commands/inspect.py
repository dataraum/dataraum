"""Inspect command - show graph definitions and execution context."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from rich.table import Table as RichTable

from dataraum.cli.common import OutputDirArg, console, get_manager


def inspect(
    output_dir: OutputDirArg = Path("./pipeline_output"),
) -> None:
    """Inspect graph definitions and execution context.

    Shows loaded graphs, applicable filters, and execution context.
    """
    from sqlalchemy import select

    from dataraum.analysis.semantic.db_models import SemanticAnnotation
    from dataraum.analysis.typing.db_models import TypeDecision
    from dataraum.graphs import GraphLoader
    from dataraum.storage import Column, Source, Table

    manager = get_manager(output_dir)

    try:
        with manager.session_scope() as session:
            # Check for sources
            sources_result = session.execute(select(Source))
            sources = sources_result.scalars().all()

            if not sources:
                console.print("[yellow]No sources found. Run the pipeline first.[/yellow]")
                return

            # Check for tables
            tables_result = session.execute(select(Table))
            tables = tables_result.scalars().all()

            if not tables:
                console.print("[yellow]No tables found. Run import phase first.[/yellow]")
                return

            console.print("\n[bold]Graph Loader Status[/bold]\n")

            # Load graphs
            loader = GraphLoader()
            graphs = loader.load_all()

            console.print(f"Loaded {len(graphs)} graphs")

            # Separate by type
            filters = loader.get_filter_graphs()
            metrics = loader.get_metric_graphs()

            # Filter graphs table
            if filters:
                console.print(f"\n[cyan]Filter Graphs ({len(filters)}):[/cyan]")
                filter_table = RichTable(show_header=True, header_style="bold")
                filter_table.add_column("ID")
                filter_table.add_column("Name")
                filter_table.add_column("Applies To")

                for g in filters:
                    applies_to = ""
                    if g.metadata.applies_to:
                        if g.metadata.applies_to.semantic_role:
                            applies_to = f"role: {g.metadata.applies_to.semantic_role}"
                        elif g.metadata.applies_to.data_type:
                            applies_to = f"type: {g.metadata.applies_to.data_type}"
                        elif g.metadata.applies_to.column_pattern:
                            pattern = g.metadata.applies_to.column_pattern
                            applies_to = f"pattern: {pattern[:30]}..."
                    filter_table.add_row(g.graph_id, g.metadata.name, applies_to)

                console.print(filter_table)

            # Metric graphs table
            if metrics:
                console.print(f"\n[cyan]Metric Graphs ({len(metrics)}):[/cyan]")
                metric_table = RichTable(show_header=True, header_style="bold")
                metric_table.add_column("ID")
                metric_table.add_column("Name")
                metric_table.add_column("Scope")

                for g in metrics:
                    scope = getattr(g, "scope", None)
                    scope_str = scope.value if scope else ""
                    metric_table.add_row(g.graph_id, g.metadata.name, scope_str)

                console.print(metric_table)

            # Load errors
            errors = loader.get_load_errors()
            if errors:
                console.print(f"\n[red]Load Errors ({len(errors)}):[/red]")
                for err in errors:
                    console.print(f"  - {err}")

            # Build column metadata for filter matching
            console.print("\n[bold]Dataset Filter Coverage[/bold]\n")

            columns_metadata = []
            for table in tables:
                cols_result = session.execute(
                    select(Column).where(Column.table_id == table.table_id)
                )
                columns = cols_result.scalars().all()

                for col in columns:
                    # Get type decision
                    type_result = session.execute(
                        select(TypeDecision).where(TypeDecision.column_id == col.column_id)
                    )
                    type_dec = type_result.scalar_one_or_none()

                    # Get semantic annotation
                    sem_result = session.execute(
                        select(SemanticAnnotation).where(
                            SemanticAnnotation.column_id == col.column_id
                        )
                    )
                    sem_ann = sem_result.scalar_one_or_none()

                    columns_metadata.append(
                        {
                            "column_name": col.column_name,
                            "table_name": table.table_name,
                            "data_type": type_dec.decided_type if type_dec else None,
                            "semantic_role": sem_ann.semantic_role if sem_ann else None,
                            "has_profile": True,
                        }
                    )

            # Get filter summary
            summary = loader.get_quality_filter_summary(columns_metadata)

            console.print(f"Total unique filters: {summary['total_filters']}")
            console.print(f"Filter coverage: {summary['filter_coverage']:.1%}")
            if summary["filter_ids"]:
                console.print(f"Filters applied: {', '.join(summary['filter_ids'])}")

            # Show columns with filters
            filters_by_col = loader.get_filters_for_dataset(columns_metadata)
            columns_with_filters: list[tuple[str, str, list[Any]]] = [
                (
                    str(c["table_name"]),
                    str(c["column_name"]),
                    filters_by_col.get(str(c["column_name"]), []),
                )
                for c in columns_metadata
                if filters_by_col.get(str(c["column_name"]))
            ]

            if columns_with_filters:
                console.print("\n[cyan]Columns with filters:[/cyan]")
                col_table = RichTable(show_header=True, header_style="bold")
                col_table.add_column("Table")
                col_table.add_column("Column")
                col_table.add_column("Filters")

                for tbl_name, col_name, col_filters in columns_with_filters[:15]:
                    filter_names = [f.graph_id for f in col_filters]
                    col_table.add_row(tbl_name, col_name, ", ".join(filter_names))

                console.print(col_table)

                if len(columns_with_filters) > 15:
                    console.print(f"  ... and {len(columns_with_filters) - 15} more")

            # Execution context sample
            console.print("\n[bold]Execution Context (Sample)[/bold]\n")

            if tables:
                from dataraum.graphs.context import (
                    build_execution_context,
                    format_context_for_prompt,
                )

                table_ids = [t.table_id for t in tables[:3]]
                with manager.duckdb_cursor() as cursor:
                    context = build_execution_context(
                        session=session,
                        table_ids=table_ids,
                        duckdb_conn=cursor,
                    )

                console.print(f"Tables in context: {context.total_tables}")
                console.print(f"Columns in context: {context.total_columns}")
                console.print(f"Relationships: {context.total_relationships}")
                console.print(f"Graph pattern: {context.graph_pattern}")

                formatted = format_context_for_prompt(context)
                lines = formatted.split("\n")
                console.print("\n[dim]Formatted context preview:[/dim]")
                for line in lines[:15]:
                    console.print(f"  {line}")
                if len(lines) > 15:
                    console.print(f"  [dim]... ({len(lines) - 15} more lines)[/dim]")

            console.print()
    finally:
        manager.close()
