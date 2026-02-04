"""Contracts command - evaluate data quality contracts."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Any

import typer
from rich.table import Table as RichTable

from dataraum.cli.common import (
    JsonFlag,
    OutputDirArg,
    TuiFlag,
    VerboseOption,
    console,
    get_manager,
    setup_logging,
)

if TYPE_CHECKING:
    from dataraum.entropy.contracts import ContractEvaluation, ContractProfile


def contracts(
    output_dir: OutputDirArg = Path("./pipeline_output"),
    contract: Annotated[
        str | None,
        typer.Option(
            "--contract",
            "-c",
            help="Evaluate a specific contract (default: all)",
        ),
    ] = None,
    tui: TuiFlag = False,
    json_output: JsonFlag = False,
    verbose: VerboseOption = 0,
) -> None:
    """Evaluate data quality contracts.

    Shows which contracts the data meets and which it doesn't.
    Use --contract to evaluate a specific contract and see details.

    Examples:

        dataraum contracts ./pipeline_output

        dataraum contracts ./pipeline_output --contract executive_dashboard

        dataraum contracts ./output --tui

        dataraum contracts ./output --json
    """
    setup_logging(verbosity=verbose)

    if tui:
        from dataraum.cli.tui import run_app

        run_app(output_dir, initial_screen="contracts")
    elif json_output:
        _contracts_json(output_dir, contract)
    else:
        _contracts_rich(output_dir, contract)


def _contracts_json(output_dir: Path, contract_name: str | None) -> None:
    """Output contracts as JSON."""
    import json

    from sqlalchemy import select

    from dataraum.entropy.analysis.aggregator import ColumnSummary, EntropyAggregator
    from dataraum.entropy.contracts import (
        evaluate_all_contracts,
        evaluate_contract,
        get_contract,
        list_contracts,
    )
    from dataraum.entropy.core.storage import EntropyRepository
    from dataraum.storage import Source, Table

    manager = get_manager(output_dir)

    try:
        with manager.session_scope() as session:
            sources_result = session.execute(select(Source))
            sources = sources_result.scalars().all()

            if not sources:
                console.print(json.dumps({"error": "No sources found"}))
                return

            source = sources[0]

            tables_result = session.execute(
                select(Table).where(Table.source_id == source.source_id)
            )
            tables = tables_result.scalars().all()

            if not tables:
                console.print(json.dumps({"error": "No tables found"}))
                return

            table_ids = [t.table_id for t in tables]

            # Build column summaries
            repo = EntropyRepository(session)
            aggregator = EntropyAggregator()

            typed_table_ids = repo.get_typed_table_ids(table_ids)
            column_summaries: dict[str, ColumnSummary] = {}
            compound_risks: list[Any] = []

            if typed_table_ids:
                table_map, column_map = repo.get_table_column_mapping(typed_table_ids)
                entropy_objects = repo.load_for_tables(typed_table_ids, enforce_typed=True)

                if entropy_objects:
                    column_summaries, _ = aggregator.summarize_columns_by_table(
                        entropy_objects=entropy_objects,
                        table_map=table_map,
                        column_map=column_map,
                    )
                    for summary in column_summaries.values():
                        compound_risks.extend(summary.compound_risks)

            if contract_name:
                profile = get_contract(contract_name)
                if profile is None:
                    console.print(
                        json.dumps(
                            {
                                "error": f"Contract not found: {contract_name}",
                                "available": [c["name"] for c in list_contracts()],
                            }
                        )
                    )
                    return

                evaluation = evaluate_contract(column_summaries, contract_name, compound_risks)
                console.print(
                    json.dumps(
                        {
                            "contract": contract_name,
                            "is_compliant": evaluation.is_compliant,
                            "confidence_level": evaluation.confidence_level.value,
                            "overall_score": evaluation.overall_score,
                            "dimension_scores": evaluation.dimension_scores,
                            "violations": [
                                {
                                    "dimension": v.dimension,
                                    "actual": v.actual,
                                    "max_allowed": v.max_allowed,
                                }
                                for v in evaluation.violations
                            ],
                        },
                        indent=2,
                    )
                )
            else:
                evaluations = evaluate_all_contracts(column_summaries, compound_risks)
                console.print(
                    json.dumps(
                        {
                            "source": source.name,
                            "contracts": {
                                name: {
                                    "is_compliant": e.is_compliant,
                                    "confidence_level": e.confidence_level.value,
                                    "overall_score": e.overall_score,
                                    "violations_count": len(e.violations),
                                }
                                for name, e in evaluations.items()
                            },
                        },
                        indent=2,
                    )
                )
    finally:
        manager.close()


def _contracts_rich(output_dir: Path, contract_name: str | None) -> None:
    """Print contracts with Rich tables."""
    from sqlalchemy import select

    from dataraum.entropy.analysis.aggregator import ColumnSummary, EntropyAggregator
    from dataraum.entropy.contracts import (
        ConfidenceLevel,
        evaluate_all_contracts,
        evaluate_contract,
        get_contract,
        list_contracts,
    )
    from dataraum.entropy.core.storage import EntropyRepository
    from dataraum.storage import Source, Table

    manager = get_manager(output_dir)

    try:
        with manager.session_scope() as session:
            # Get sources
            sources_result = session.execute(select(Source))
            sources = sources_result.scalars().all()

            if not sources:
                console.print("[yellow]No sources found in database[/yellow]")
                return

            # Get first source (or could iterate)
            source = sources[0]

            # Get tables for this source
            tables_result = session.execute(
                select(Table).where(Table.source_id == source.source_id)
            )
            tables = tables_result.scalars().all()

            if not tables:
                console.print("[yellow]No tables found. Run pipeline first.[/yellow]")
                return

            table_ids = [t.table_id for t in tables]

            console.print(f"\n[bold]Contract Evaluation[/bold] - {source.name}\n")

            # Build column summaries for contract evaluation
            repo = EntropyRepository(session)
            aggregator = EntropyAggregator()

            typed_table_ids = repo.get_typed_table_ids(table_ids)
            column_summaries: dict[str, ColumnSummary] = {}
            compound_risks: list[Any] = []

            if typed_table_ids:
                table_map, column_map = repo.get_table_column_mapping(typed_table_ids)
                entropy_objects = repo.load_for_tables(typed_table_ids, enforce_typed=True)

                if entropy_objects:
                    column_summaries, _ = aggregator.summarize_columns_by_table(
                        entropy_objects=entropy_objects,
                        table_map=table_map,
                        column_map=column_map,
                    )
                    for summary in column_summaries.values():
                        compound_risks.extend(summary.compound_risks)

            if contract_name:
                # Evaluate specific contract
                profile = get_contract(contract_name)
                if profile is None:
                    console.print(f"[red]Contract not found: {contract_name}[/red]")
                    console.print("\nAvailable contracts:")
                    for c in list_contracts():
                        console.print(f"  - {c['name']}: {c['description']}")
                    raise typer.Exit(1)

                evaluation = evaluate_contract(column_summaries, contract_name, compound_risks)
                _print_contract_detail(evaluation, profile)

            else:
                # Evaluate all contracts
                evaluations = evaluate_all_contracts(column_summaries, compound_risks)

                def _get_threshold(name: str) -> float:
                    """Get threshold for sorting, defaulting to 1.0 if not found."""
                    c = get_contract(name)
                    return c.overall_threshold if c else 1.0

                # Sort by strictness (stricter contracts have lower thresholds)
                sorted_evals = sorted(
                    evaluations.items(),
                    key=lambda x: _get_threshold(x[0]),
                    reverse=True,  # Most lenient first
                )

                console.print("[bold]Contract Compliance:[/bold]\n")

                table = RichTable(show_header=True, header_style="bold")
                table.add_column("Status")
                table.add_column("Contract")
                table.add_column("Description")
                table.add_column("Issues", justify="right")

                for name, evaluation in sorted_evals:
                    emoji = evaluation.confidence_level.emoji
                    label = evaluation.confidence_level.label

                    # Color based on status
                    if evaluation.confidence_level == ConfidenceLevel.GREEN:
                        status = f"[green]{emoji} {label}[/green]"
                    elif evaluation.confidence_level == ConfidenceLevel.YELLOW:
                        status = f"[yellow]{emoji} {label}[/yellow]"
                    elif evaluation.confidence_level == ConfidenceLevel.ORANGE:
                        status = f"[yellow]{emoji} {label}[/yellow]"
                    else:
                        status = f"[red]{emoji} {label}[/red]"

                    profile = get_contract(name)
                    if profile:
                        desc = (
                            profile.description[:40] + "..."
                            if len(profile.description) > 40
                            else profile.description
                        )
                    else:
                        desc = ""

                    issues = len(evaluation.violations) + len(evaluation.warnings)
                    issue_str = str(issues) if issues > 0 else "-"

                    table.add_row(status, name, desc, issue_str)

                console.print(table)

                # Summary
                passing = [e for e in evaluations.values() if e.is_compliant]
                console.print(
                    f"\n[cyan]Passing:[/cyan] {len(passing)}/{len(evaluations)} contracts"
                )

                if passing:
                    # Find strictest passing
                    strictest = min(
                        passing,
                        key=lambda e: _get_threshold(e.contract_name),
                    )
                    console.print(f"[cyan]Strictest passing:[/cyan] {strictest.contract_name}")

                console.print(
                    "\nUse [cyan]--contract NAME[/cyan] to see details for a specific contract."
                )

            console.print()
    finally:
        manager.close()


def _print_contract_detail(
    evaluation: ContractEvaluation,
    profile: ContractProfile,
) -> None:
    """Print detailed evaluation for a single contract."""
    from dataraum.entropy.contracts import ConfidenceLevel

    # Header with status
    emoji = evaluation.confidence_level.emoji
    label = evaluation.confidence_level.label

    if evaluation.confidence_level == ConfidenceLevel.GREEN:
        status_color = "green"
    elif evaluation.confidence_level in (ConfidenceLevel.YELLOW, ConfidenceLevel.ORANGE):
        status_color = "yellow"
    else:
        status_color = "red"

    console.print(f"[bold]Contract:[/bold] {profile.display_name}")
    console.print(f"[bold]Status:[/bold] [{status_color}]{emoji} {label}[/{status_color}]")
    console.print(f"[dim]{profile.description}[/dim]\n")

    # Overall score
    overall_status = "✓" if evaluation.overall_score <= profile.overall_threshold else "✗"
    console.print(
        f"[bold]Overall Score:[/bold] {evaluation.overall_score:.2f} "
        f"(threshold: {profile.overall_threshold}) {overall_status}"
    )

    # Dimension breakdown
    console.print("\n[bold]Dimension Scores:[/bold]")

    dim_table = RichTable(show_header=True, header_style="bold")
    dim_table.add_column("Dimension")
    dim_table.add_column("Score", justify="right")
    dim_table.add_column("Threshold", justify="right")
    dim_table.add_column("Status")

    for dim, threshold in profile.dimension_thresholds.items():
        score = evaluation.dimension_scores.get(dim, 0.0)

        if score <= threshold:
            status = "[green]✓ PASS[/green]"
        elif score <= threshold * 1.2:
            status = "[yellow]⚠ WARN[/yellow]"
        else:
            status = "[red]✗ FAIL[/red]"

        dim_table.add_row(
            dim,
            f"{score:.2f}",
            f"{threshold:.2f}",
            status,
        )

    console.print(dim_table)

    # Violations
    if evaluation.violations:
        console.print("\n[bold red]Violations:[/bold red]")
        for v in evaluation.violations:
            if v.dimension:
                console.print(f"  [red]✗[/red] {v.dimension}: {v.actual:.2f} > {v.max_allowed:.2f}")
                if v.affected_columns:
                    cols = ", ".join(v.affected_columns[:5])
                    if len(v.affected_columns) > 5:
                        cols += f" (+{len(v.affected_columns) - 5} more)"
                    console.print(f"    Affected: {cols}")
            elif v.condition:
                console.print(f"  [red]✗[/red] {v.details}")
            else:
                console.print(f"  [red]✗[/red] {v.details}")

    # Warnings
    if evaluation.warnings:
        console.print("\n[bold yellow]Warnings:[/bold yellow]")
        for w in evaluation.warnings:
            console.print(f"  [yellow]⚠[/yellow] {w.details}")

    # Path to compliance
    if not evaluation.is_compliant and evaluation.worst_dimension:
        console.print("\n[bold]Path to Compliance:[/bold]")
        console.print(
            f"  Focus on: [cyan]{evaluation.worst_dimension}[/cyan] "
            f"(score: {evaluation.worst_dimension_score:.2f})"
        )
        console.print(f"  Estimated effort: {evaluation.estimated_effort_to_comply}")
