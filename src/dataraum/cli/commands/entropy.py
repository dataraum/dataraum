"""Entropy command - explore entropy metrics and data quality issues."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Annotated, Any

import typer
from rich.table import Table as RichTable

from dataraum.cli.common import JsonFlag, OutputDirArg, TuiFlag, VerboseOption, console, get_manager


def entropy(
    output_dir: OutputDirArg = Path("./pipeline_output"),
    table: Annotated[
        str | None,
        typer.Option(
            "--table",
            "-t",
            help="Filter to a specific table",
        ),
    ] = None,
    status: Annotated[
        str | None,
        typer.Option(
            "--status",
            "-s",
            help="Filter by readiness status (ready, investigate, blocked)",
        ),
    ] = None,
    issues: Annotated[
        bool,
        typer.Option(
            "--issues",
            "-i",
            help="Show all issues in linter-style format",
        ),
    ] = False,
    tui: TuiFlag = False,
    json_output: JsonFlag = False,
    verbose: VerboseOption = 0,
) -> None:
    """Explore entropy metrics and data quality issues.

    Shows a summary of data uncertainty across dimensions, helping developers
    understand what assumptions the system makes and what to fix.

    Examples:

        dataraum entropy ./pipeline_output

        dataraum entropy ./output -v

        dataraum entropy ./output --table master_txn_table

        dataraum entropy ./output --status investigate

        dataraum entropy ./output --issues

        dataraum entropy ./output --tui

        dataraum entropy ./output --json
    """
    if tui:
        from dataraum.cli.tui import run_app

        run_app(output_dir, initial_screen="entropy", table_filter=table)
    elif json_output:
        _entropy_json(output_dir, table, status)
    else:
        _entropy_rich(output_dir, table, status, issues, verbose)


def _entropy_json(output_dir: Path, table_filter: str | None, status_filter: str | None) -> None:
    """Output entropy as JSON."""
    import json

    from sqlalchemy import select

    from dataraum.entropy.db_models import (
        EntropyInterpretationRecord,
        EntropySnapshotRecord,
    )
    from dataraum.storage import Source

    manager = get_manager(output_dir)

    try:
        with manager.session_scope() as session:
            sources_result = session.execute(select(Source))
            sources = sources_result.scalars().all()

            if not sources:
                console.print(json.dumps({"error": "No sources found"}))
                return

            source = sources[0]

            snapshot_result = session.execute(
                select(EntropySnapshotRecord)
                .where(EntropySnapshotRecord.source_id == source.source_id)
                .order_by(EntropySnapshotRecord.snapshot_at.desc())
                .limit(1)
            )
            snapshot = snapshot_result.scalar_one_or_none()

            if not snapshot:
                console.print(json.dumps({"error": "No entropy data found"}))
                return

            interp_query = select(EntropyInterpretationRecord).where(
                EntropyInterpretationRecord.source_id == source.source_id
            )

            if table_filter:
                interp_query = interp_query.where(
                    EntropyInterpretationRecord.table_name == table_filter
                )

            if status_filter:
                interp_query = interp_query.where(
                    EntropyInterpretationRecord.readiness == status_filter
                )

            interp_query = interp_query.order_by(EntropyInterpretationRecord.composite_score.desc())
            interp_result = session.execute(interp_query)
            interpretations = interp_result.scalars().all()

            output_data = {
                "source": source.name,
                "overall_readiness": snapshot.overall_readiness,
                "avg_composite_score": snapshot.avg_composite_score,
                "dimensions": {
                    "structural": snapshot.avg_structural_entropy,
                    "semantic": snapshot.avg_semantic_entropy,
                    "value": snapshot.avg_value_entropy,
                    "computational": snapshot.avg_computational_entropy,
                },
                "columns": [
                    {
                        "table": i.table_name,
                        "column": i.column_name,
                        "composite_score": i.composite_score,
                        "readiness": i.readiness,
                        "explanation": i.explanation,
                    }
                    for i in interpretations
                ],
            }

            console.print(json.dumps(output_data, indent=2))
    finally:
        manager.close()


def _entropy_rich(
    output_dir: Path,
    table_filter: str | None,
    status_filter: str | None,
    show_issues: bool,
    verbose: int,
) -> None:
    """Print entropy with Rich tables."""
    from sqlalchemy import select

    from dataraum.entropy.db_models import (
        EntropyInterpretationRecord,
        EntropyObjectRecord,
        EntropySnapshotRecord,
    )
    from dataraum.storage import Source

    manager = get_manager(output_dir)

    try:
        with manager.session_scope() as session:
            # Get source
            sources_result = session.execute(select(Source))
            sources = sources_result.scalars().all()

            if not sources:
                console.print("[yellow]No sources found in database[/yellow]")
                return

            source = sources[0]

            # Get snapshot for summary
            snapshot_result = session.execute(
                select(EntropySnapshotRecord)
                .where(EntropySnapshotRecord.source_id == source.source_id)
                .order_by(EntropySnapshotRecord.snapshot_at.desc())
                .limit(1)
            )
            snapshot = snapshot_result.scalar_one_or_none()

            if not snapshot:
                console.print("[yellow]No entropy data found. Run entropy phase first.[/yellow]")
                return

            # Build interpretation query with filters
            interp_query = select(EntropyInterpretationRecord).where(
                EntropyInterpretationRecord.source_id == source.source_id
            )

            if table_filter:
                interp_query = interp_query.where(
                    EntropyInterpretationRecord.table_name == table_filter
                )

            if status_filter:
                interp_query = interp_query.where(
                    EntropyInterpretationRecord.readiness == status_filter
                )

            interp_query = interp_query.order_by(EntropyInterpretationRecord.composite_score.desc())

            interp_result = session.execute(interp_query)
            interpretations = interp_result.scalars().all()

            # Get entropy objects for issue counts
            obj_query = select(EntropyObjectRecord).where(
                EntropyObjectRecord.source_id == source.source_id
            )
            obj_result = session.execute(obj_query)
            entropy_objects = obj_result.scalars().all()

            if show_issues:
                _print_issues_view(interpretations, entropy_objects, verbose)
            else:
                _print_summary_view(
                    source.name, snapshot, interpretations, entropy_objects, table_filter, verbose
                )

            console.print()
    finally:
        manager.close()


def _print_summary_view(
    source_name: str,
    snapshot: Any,
    interpretations: Sequence[Any],
    entropy_objects: Sequence[Any],
    table_filter: str | None,
    verbose: int,
) -> None:
    """Print summary view of entropy state."""
    # Header
    title = f"Entropy Summary - {source_name}"
    if table_filter:
        title += f" (table: {table_filter})"
    console.print(f"\n[bold]{title}[/bold]\n")

    # Overall readiness with color
    readiness_colors = {
        "ready": "green",
        "investigate": "yellow",
        "blocked": "red",
    }
    readiness_icons = {
        "ready": "🟢",
        "investigate": "🟡",
        "blocked": "🔴",
    }
    color = readiness_colors.get(snapshot.overall_readiness, "white")
    icon = readiness_icons.get(snapshot.overall_readiness, "⚪")

    console.print(
        f"[{color}]{icon} Overall Readiness: {snapshot.overall_readiness.upper()}[/{color}]"
    )
    console.print(f"Composite Score: {snapshot.avg_composite_score:.3f}")
    console.print()

    # Dimension breakdown
    console.print("[bold]Entropy by Dimension:[/bold]")
    dim_table = RichTable(show_header=True, header_style="bold", box=None)
    dim_table.add_column("Dimension")
    dim_table.add_column("Score", justify="right")
    dim_table.add_column("Bar")

    dimensions = [
        ("Structural", snapshot.avg_structural_entropy),
        ("Semantic", snapshot.avg_semantic_entropy),
        ("Value", snapshot.avg_value_entropy),
        ("Computational", snapshot.avg_computational_entropy),
    ]

    for name, score in dimensions:
        bar_len = int(score * 20)
        bar = "█" * bar_len + "░" * (20 - bar_len)
        if score > 0.3:
            bar = f"[red]{bar}[/red]"
        elif score > 0.15:
            bar = f"[yellow]{bar}[/yellow]"
        else:
            bar = f"[green]{bar}[/green]"
        dim_table.add_row(name, f"{score:.3f}", bar)

    console.print(dim_table)
    console.print()

    # Issue counts
    high_entropy = [i for i in interpretations if i.composite_score > 0.2]
    investigate = [i for i in interpretations if i.readiness == "investigate"]
    blocked = [i for i in interpretations if i.readiness == "blocked"]

    console.print("[bold]Issue Summary:[/bold]")
    console.print(f"  Total columns analyzed: {len(interpretations)}")
    console.print(f"  High entropy (>0.2): {len(high_entropy)}")
    if investigate:
        console.print(f"  [yellow]Needs investigation: {len(investigate)}[/yellow]")
    if blocked:
        console.print(f"  [red]Blocked: {len(blocked)}[/red]")
    console.print()

    # Top high-entropy columns
    if interpretations:
        show_count = 10 if verbose else 5
        top_columns = interpretations[:show_count]

        console.print(f"[bold]Top {len(top_columns)} High-Entropy Columns:[/bold]")
        col_table = RichTable(show_header=True, header_style="bold")
        col_table.add_column("Table")
        col_table.add_column("Column")
        col_table.add_column("Score", justify="right")
        col_table.add_column("Status")

        if verbose >= 1:
            col_table.add_column("Top Issue")

        for interp in top_columns:
            status_color = readiness_colors.get(interp.readiness, "white")
            status_icon = readiness_icons.get(interp.readiness, "⚪")

            row = [
                interp.table_name,
                interp.column_name,
                f"{interp.composite_score:.3f}",
                f"[{status_color}]{status_icon}[/{status_color}]",
            ]

            if verbose >= 1:
                # Show first line of explanation as top issue
                explanation = interp.explanation or ""
                first_line = explanation.split(".")[0] if explanation else "-"
                if len(first_line) > 50:
                    first_line = first_line[:47] + "..."
                row.append(f"[dim]{first_line}[/dim]")

            col_table.add_row(*row)

        console.print(col_table)

        if len(interpretations) > show_count:
            console.print(f"[dim]  ... and {len(interpretations) - show_count} more columns[/dim]")

        # Verbose: show explanations
        if verbose >= 2 and top_columns:
            console.print()
            console.print("[bold]Details:[/bold]")
            for interp in top_columns:
                console.print(f"\n[cyan]{interp.table_name}.{interp.column_name}[/cyan]")
                console.print(f"  {interp.explanation}")

                if interp.assumptions_json:
                    assumptions = interp.assumptions_json
                    console.print(f"  [bold]Assumptions ({len(assumptions)}):[/bold]")
                    for a in assumptions:
                        console.print(
                            f"    - [{a.get('dimension', '')}] {a.get('assumption_text', '')}"
                        )
                        console.print(
                            f"      Confidence: {a.get('confidence', '')} | "
                            f"Impact: {a.get('impact', '')}"
                        )

                if interp.resolution_actions_json:
                    actions = interp.resolution_actions_json
                    console.print(f"  [bold]Actions ({len(actions)}):[/bold]")
                    for a in actions:
                        priority = a.get("priority", "medium")
                        action_name = a.get("action", "")
                        description = a.get("description", "")
                        effort = a.get("effort", "")
                        console.print(f"    - ({priority}) {action_name}: {description}")
                        console.print(f"      Effort: {effort}")


def _print_issues_view(
    interpretations: Sequence[Any],
    entropy_objects: Sequence[Any],
    verbose: int,
) -> None:
    """Print linter-style issues view."""
    console.print("\n[bold]Entropy Issues[/bold]\n")

    if not interpretations:
        console.print("[green]No issues found.[/green]")
        return

    # Group by severity
    blocked = [i for i in interpretations if i.readiness == "blocked"]
    investigate = [i for i in interpretations if i.readiness == "investigate"]
    high_entropy = [
        i for i in interpretations if i.readiness == "ready" and i.composite_score > 0.2
    ]

    def print_issue_group(title: str, items: list[Any], color: str, icon: str) -> None:
        if not items:
            return
        console.print(f"[{color}][bold]{icon} {title} ({len(items)})[/bold][/{color}]")
        for item in items:
            loc = f"{item.table_name}.{item.column_name}"
            score = f"{item.composite_score:.3f}"

            # Get top dimension issue
            top_issue = ""
            if item.assumptions_json and len(item.assumptions_json) > 0:
                first_assumption = item.assumptions_json[0]
                dim = first_assumption.get("dimension", "")
                top_issue = f" [{dim}]" if dim else ""

            console.print(f"  [{color}]{icon}[/{color}] {loc}: {score}{top_issue}")

            if verbose == 1:
                # Show first assumption (compact)
                if item.assumptions_json:
                    assumption_text = item.assumptions_json[0].get("assumption_text", "")
                    if len(assumption_text) > 70:
                        assumption_text = assumption_text[:67] + "..."
                    console.print(f"      [dim]→ {assumption_text}[/dim]")

            if verbose >= 2:
                # Show all assumptions
                if item.assumptions_json:
                    for a in item.assumptions_json:
                        dim = a.get("dimension", "")
                        text = a.get("assumption_text", "")
                        console.print(f"      [dim]→ [{dim}] {text}[/dim]")
                # Show all recommended actions
                if item.resolution_actions_json:
                    for action in item.resolution_actions_json:
                        action_desc = action.get("description", "")
                        priority = action.get("priority", "medium")
                        effort = action.get("effort", "")
                        console.print(f"      [dim]Fix ({priority}, {effort}): {action_desc}[/dim]")

        console.print()

    print_issue_group("BLOCKED", blocked, "red", "✗")
    print_issue_group("INVESTIGATE", investigate, "yellow", "⚠")
    print_issue_group("HIGH ENTROPY", high_entropy, "cyan", "●")

    # Summary
    total = len(blocked) + len(investigate) + len(high_entropy)
    console.print(f"[bold]Total: {total} issues[/bold]")
    if blocked:
        console.print(f"  [red]✗ {len(blocked)} blocked[/red]")
    if investigate:
        console.print(f"  [yellow]⚠ {len(investigate)} need investigation[/yellow]")
    if high_entropy:
        console.print(f"  [cyan]● {len(high_entropy)} high entropy[/cyan]")
