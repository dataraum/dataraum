"""Query command - ask questions about data using natural language."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer
from rich.table import Table as RichTable

from dataraum.cli.common import (
    OutputDirOption,
    TuiFlag,
    VerboseOption,
    console,
    get_manager,
    setup_logging,
)


def query(
    question: Annotated[
        str,
        typer.Argument(
            help="Natural language question to answer",
        ),
    ],
    output_dir: OutputDirOption = Path("./pipeline_output"),
    contract: Annotated[
        str | None,
        typer.Option(
            "--contract",
            "-c",
            help="Contract to evaluate against (e.g., 'executive_dashboard')",
        ),
    ] = None,
    auto_contract: Annotated[
        bool,
        typer.Option(
            "--auto-contract",
            help="Automatically select the strictest passing contract",
        ),
    ] = False,
    show_sql: Annotated[
        bool,
        typer.Option(
            "--show-sql",
            help="Show the generated SQL",
        ),
    ] = False,
    ephemeral: Annotated[
        bool,
        typer.Option(
            "--ephemeral",
            help="Don't save this query to the library (default: saves successful queries)",
        ),
    ] = False,
    tui: TuiFlag = False,
    verbose: VerboseOption = 0,
) -> None:
    """Ask a question about the data using natural language.

    The Query Agent converts your question into SQL and returns results
    with a confidence level based on data quality.

    By default, successful queries are saved to the query library for future
    reuse. Use --ephemeral to skip saving.

    Examples:

        dataraum query "What was total revenue?" -o ./pipeline_output

        dataraum query "Show sales by region" -o ./output --contract executive_dashboard

        dataraum query "Monthly trend" -o ./output --auto-contract --show-sql

        dataraum query "Quick test" -o ./output --ephemeral

        dataraum query "Revenue by month" -o ./output --tui
    """
    setup_logging(verbosity=verbose)

    if tui:
        from dataraum.cli.tui import run_app

        run_app(output_dir, initial_screen="query", query=question)
    else:
        _query_rich(question, output_dir, contract, auto_contract, show_sql, ephemeral)


def _query_rich(
    question: str,
    output_dir: Path,
    contract_name: str | None,
    auto_contract: bool,
    show_sql: bool,
    ephemeral: bool,
) -> None:
    """Execute query and print results with Rich."""
    from sqlalchemy import select

    from dataraum.entropy.contracts import ConfidenceLevel
    from dataraum.query import answer_question
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

            source = sources[0]

            # Get tables
            tables_result = session.execute(
                select(Table).where(Table.source_id == source.source_id)
            )
            tables = tables_result.scalars().all()

            if not tables:
                console.print("[yellow]No tables found. Run pipeline first.[/yellow]")
                return

            # Get DuckDB cursor (properly managed context)
            with manager.duckdb_cursor() as cursor:
                # Call the query agent
                result = answer_question(
                    question=question,
                    session=session,
                    duckdb_conn=cursor,
                    source_id=source.source_id,
                    contract=contract_name,
                    auto_contract=auto_contract,
                    manager=manager,
                    ephemeral=ephemeral,
                )

            if not result.success or not result.value:
                console.print(f"[red]Error: {result.error}[/red]")
                raise typer.Exit(1)

            query_result = result.value

            # Display confidence header
            emoji = query_result.confidence_level.emoji
            label = query_result.confidence_level.label
            contract_display = query_result.contract or "default"

            if query_result.confidence_level == ConfidenceLevel.GREEN:
                status_color = "green"
            elif query_result.confidence_level == ConfidenceLevel.YELLOW:
                status_color = "yellow"
            elif query_result.confidence_level == ConfidenceLevel.ORANGE:
                status_color = "yellow"
            else:
                status_color = "red"

            console.print(
                f"\n[{status_color}]{emoji} Data Quality: {label}[/{status_color}] "
                f"for [cyan]{contract_display}[/cyan]\n"
            )

            # Display answer
            console.print(query_result.answer)

            # Display data table if available
            if query_result.data and query_result.columns and len(query_result.data) <= 50:
                console.print()
                data_table = RichTable(show_header=True, header_style="bold")
                for col in query_result.columns:
                    data_table.add_column(col)

                for row in query_result.data[:50]:
                    data_table.add_row(*[str(row.get(c, "")) for c in query_result.columns])

                console.print(data_table)

                if len(query_result.data) > 50:
                    console.print(f"[dim]... showing 50 of {len(query_result.data)} rows[/dim]")

            # Display SQL if requested
            if show_sql and query_result.sql:
                console.print("\n[bold]Generated SQL:[/bold]")
                console.print(f"[dim]{query_result.sql}[/dim]")

            # Display assumptions
            if query_result.assumptions:
                console.print("\n[bold]Assumptions:[/bold]")
                for a in query_result.assumptions:
                    console.print(f"  - {a.assumption} ([dim]{a.basis.value}[/dim])")

            # Display warnings for non-green confidence
            if query_result.confidence_level in (
                ConfidenceLevel.ORANGE,
                ConfidenceLevel.RED,
            ):
                if query_result.contract_evaluation:
                    eval_ = query_result.contract_evaluation
                    if eval_.violations:
                        console.print("\n[bold yellow]Quality Issues:[/bold yellow]")
                        for v in eval_.violations[:5]:
                            if v.dimension:
                                console.print(
                                    f"  [yellow]⚠[/yellow] {v.dimension}: "
                                    f"{v.actual:.2f} (threshold: {v.max_allowed:.2f})"
                                )
                            elif v.details:
                                console.print(f"  [yellow]⚠[/yellow] {v.details}")

            # Show path to compliance for blocked queries
            if query_result.confidence_level == ConfidenceLevel.RED:
                console.print("\n[bold]To improve data quality:[/bold]")
                console.print(
                    f"  Run: [cyan]dataraum contracts {output_dir} --contract "
                    f"{query_result.contract}[/cyan]"
                )

            console.print()
    finally:
        manager.close()
