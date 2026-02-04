"""Status command - show pipeline status."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from rich.table import Table as RichTable

from dataraum.cli.common import JsonFlag, OutputDirArg, TuiFlag, console, get_manager


def status(
    output_dir: OutputDirArg = Path("./pipeline_output"),
    tui: TuiFlag = False,
    json_output: JsonFlag = False,
) -> None:
    """Show status of a pipeline run.

    Displays information about completed phases, sources, and tables.

    Examples:

        dataraum status ./pipeline_output

        dataraum status ./output --tui

        dataraum status ./output --json
    """
    if tui:
        _status_tui(output_dir)
    elif json_output:
        _status_json(output_dir)
    else:
        _status_rich(output_dir)


def _status_tui(output_dir: Path) -> None:
    """Launch TUI for status."""
    from dataraum.cli.tui import run_app

    run_app(output_dir, initial_screen="home")


def _status_json(output_dir: Path) -> None:
    """Output status as JSON."""
    import json

    from sqlalchemy import func, select

    from dataraum.pipeline.db_models import PhaseCheckpoint
    from dataraum.storage import Column, Source, Table

    manager = get_manager(output_dir)

    try:
        with manager.session_scope() as session:
            sources_result = session.execute(select(Source))
            sources = sources_result.scalars().all()

            output_data: dict[str, list[dict[str, Any]]] = {"sources": []}

            for source in sources:
                tables_result = session.execute(
                    select(Table).where(Table.source_id == source.source_id)
                )
                tables = tables_result.scalars().all()

                by_layer: dict[str, list[dict[str, Any]]] = {}
                for t in tables:
                    layer = t.layer or "unknown"
                    by_layer.setdefault(layer, []).append(
                        {
                            "name": t.table_name,
                            "rows": t.row_count,
                        }
                    )

                columns_result = session.execute(
                    select(func.count())
                    .select_from(Column)
                    .join(Table)
                    .where(Table.source_id == source.source_id)
                )
                columns_count = columns_result.scalar() or 0

                phases_result = session.execute(
                    select(PhaseCheckpoint)
                    .where(PhaseCheckpoint.source_id == source.source_id)
                    .order_by(PhaseCheckpoint.started_at)
                )
                phases = phases_result.scalars().all()

                output_data["sources"].append(
                    {
                        "name": source.name,
                        "source_id": source.source_id,
                        "tables_by_layer": {
                            layer: {
                                "count": len(tables),
                                "rows": sum(t["rows"] or 0 for t in tables),
                            }
                            for layer, tables in by_layer.items()
                        },
                        "total_columns": columns_count,
                        "phases": [
                            {
                                "name": p.phase_name,
                                "status": p.status,
                                "duration_seconds": p.duration_seconds,
                                "llm_calls": p.llm_calls,
                            }
                            for p in phases
                        ],
                    }
                )

            console.print(json.dumps(output_data, indent=2))
    finally:
        manager.close()


def _status_rich(output_dir: Path) -> None:
    """Print status with Rich tables."""
    from sqlalchemy import func, select

    from dataraum.pipeline.db_models import PhaseCheckpoint
    from dataraum.storage import Column, Source, Table

    manager = get_manager(output_dir)

    try:
        with manager.session_scope() as session:
            # Get sources
            sources_result = session.execute(select(Source))
            sources = sources_result.scalars().all()

            if not sources:
                console.print("[yellow]No sources found in database[/yellow]")
                return

            console.print(f"\n[bold]Pipeline Status[/bold] - {output_dir}\n")

            for source in sources:
                # Source info
                console.print(f"[cyan]Source:[/cyan] {source.name} ({source.source_id[:8]}...)")

                # Tables
                tables_result = session.execute(
                    select(Table).where(Table.source_id == source.source_id)
                )
                tables = tables_result.scalars().all()

                # Group by layer
                by_layer: dict[str, list[Table]] = {}
                for t in tables:
                    layer = t.layer or "unknown"
                    by_layer.setdefault(layer, []).append(t)

                table = RichTable(show_header=True, header_style="bold")
                table.add_column("Layer")
                table.add_column("Tables", justify="right")
                table.add_column("Total Rows", justify="right")

                for layer in ["raw", "typed", "quarantine"]:
                    if layer in by_layer:
                        layer_tables = by_layer[layer]
                        total_rows = sum(t.row_count or 0 for t in layer_tables)
                        table.add_row(layer, str(len(layer_tables)), f"{total_rows:,}")

                console.print(table)

                # Columns count
                columns_result = session.execute(
                    select(func.count())
                    .select_from(Column)
                    .join(Table)
                    .where(Table.source_id == source.source_id)
                )
                columns_count = columns_result.scalar() or 0
                console.print(f"Total columns: {columns_count}")

                # Phase executions
                phases_result = session.execute(
                    select(PhaseCheckpoint)
                    .where(PhaseCheckpoint.source_id == source.source_id)
                    .order_by(PhaseCheckpoint.started_at)
                )
                phases = phases_result.scalars().all()

                if phases:
                    console.print("\n[bold]Phase History:[/bold]")
                    phase_table = RichTable(show_header=True, header_style="bold")
                    phase_table.add_column("Phase")
                    phase_table.add_column("Status")
                    phase_table.add_column("Duration")
                    phase_table.add_column("Started")

                    for p in phases:
                        duration = ""
                        if p.started_at and p.completed_at:
                            delta = p.completed_at - p.started_at
                            duration = f"{delta.total_seconds():.1f}s"

                        status_color = {
                            "completed": "green",
                            "failed": "red",
                            "skipped": "yellow",
                            "running": "blue",
                        }.get(p.status, "white")

                        started = p.started_at.strftime("%H:%M:%S") if p.started_at else ""

                        phase_table.add_row(
                            p.phase_name,
                            f"[{status_color}]{p.status}[/{status_color}]",
                            duration,
                            started,
                        )

                    console.print(phase_table)

                    # Timing summary
                    completed = [
                        p for p in phases if p.status == "completed" and p.duration_seconds
                    ]
                    if completed:
                        total = sum(p.duration_seconds for p in completed)
                        sorted_phases = sorted(
                            completed, key=lambda p: p.duration_seconds, reverse=True
                        )

                        console.print("\n[bold]Phase Timing (slowest first):[/bold]")
                        timing_table = RichTable(show_header=True, header_style="bold")
                        timing_table.add_column("Phase")
                        timing_table.add_column("Duration", justify="right")
                        timing_table.add_column("% of Total", justify="right")
                        timing_table.add_column("LLM Calls", justify="right")
                        timing_table.add_column("LLM Tokens", justify="right")

                        for p in sorted_phases[:10]:
                            pct = (p.duration_seconds / total * 100) if total > 0 else 0
                            llm_calls = str(p.llm_calls) if p.llm_calls > 0 else "-"
                            llm_tokens = (
                                f"{p.llm_input_tokens + p.llm_output_tokens:,}"
                                if (p.llm_input_tokens + p.llm_output_tokens) > 0
                                else "-"
                            )
                            timing_table.add_row(
                                p.phase_name,
                                f"{p.duration_seconds:.2f}s",
                                f"{pct:.1f}%",
                                llm_calls,
                                llm_tokens,
                            )

                        console.print(timing_table)
                        console.print(f"\n[cyan]Total:[/cyan] {total:.2f}s")

                        # Show aggregate LLM usage
                        total_llm_calls = sum(p.llm_calls for p in completed)
                        total_llm_tokens = sum(
                            p.llm_input_tokens + p.llm_output_tokens for p in completed
                        )
                        if total_llm_calls > 0:
                            console.print(
                                f"[cyan]Total LLM:[/cyan] {total_llm_calls} calls, "
                                f"{total_llm_tokens:,} tokens"
                            )

                console.print()
    finally:
        manager.close()
