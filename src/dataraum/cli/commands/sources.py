"""Sources subcommand - manage data sources."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer
from rich.table import Table as RichTable

from dataraum.cli.common import OutputDirOption, console

app = typer.Typer(
    name="sources",
    help="Manage data sources (discover, register, list, remove).",
    no_args_is_help=True,
)


@app.command("list")
def list_sources(
    output_dir: OutputDirOption = Path("./pipeline_output"),
) -> None:
    """List registered data sources."""
    from dataraum.cli.common import get_manager
    from dataraum.core.credentials import CredentialChain
    from dataraum.sources.manager import SourceManager

    manager = get_manager(output_dir)
    try:
        with manager.session_scope() as session:
            sm = SourceManager(session=session, credential_chain=CredentialChain())
            sources = sm.list_sources()

            if not sources:
                console.print("[yellow]No sources registered.[/yellow]")
                console.print("Use [bold]dataraum sources discover[/bold] to find data files.")
                return

            table = RichTable(title="Registered Sources", show_header=True, header_style="bold")
            table.add_column("Name")
            table.add_column("Type")
            table.add_column("Status")
            table.add_column("Path / Backend")
            table.add_column("Columns")

            for src in sources:
                location = src.path or src.backend or "-"
                col_count = str(len(src.columns)) if src.columns else "-"
                table.add_row(src.name, src.source_type, src.status or "-", location, col_count)

            console.print(table)
    finally:
        manager.close()


@app.command("add")
def add_source(
    name: Annotated[
        str,
        typer.Argument(help="Source name (lowercase, a-z/0-9/_, starts with letter)"),
    ],
    path: Annotated[
        Path | None,
        typer.Argument(help="Path to data file (CSV, Parquet, JSON, XLSX)"),
    ] = None,
    backend: Annotated[
        str | None,
        typer.Option("--backend", "-b", help="Database backend: postgres, mysql, sqlite"),
    ] = None,
    tables: Annotated[
        list[str] | None,
        typer.Option("--tables", "-t", help="Table filter for database sources"),
    ] = None,
    credential_ref: Annotated[
        str | None,
        typer.Option("--credential-ref", help="Credential lookup key (defaults to source name)"),
    ] = None,
    output_dir: OutputDirOption = Path("./pipeline_output"),
) -> None:
    """Register a data source for analysis.

    Provide either a file path or a database backend (not both).

    Examples:

        dataraum sources add sales /path/to/sales.csv

        dataraum sources add prod_db --backend postgres
    """
    if path is None and backend is None:
        console.print("[red]Provide either a file path or --backend (not neither)[/red]")
        raise typer.Exit(1)

    if path is not None and backend is not None:
        console.print("[red]Provide either a file path or --backend (not both)[/red]")
        raise typer.Exit(1)

    from dataraum.cli.common import get_manager
    from dataraum.core.credentials import CredentialChain
    from dataraum.sources.manager import SourceManager

    manager = get_manager(output_dir)
    try:
        with manager.session_scope() as session:
            cred_chain = CredentialChain()
            duckdb_conn = None
            if backend:
                duckdb_conn = manager.duckdb_cursor().__enter__()

            sm = SourceManager(
                session=session,
                credential_chain=cred_chain,
                duckdb_conn=duckdb_conn,
            )

            if path is not None:
                result = sm.add_file_source(name, str(path.resolve()))
            else:
                assert backend is not None  # validated above
                result = sm.add_database_source(
                    name,
                    backend=backend,
                    tables=tables,
                    credential_ref=credential_ref,
                )

            if duckdb_conn:
                duckdb_conn.__exit__(None, None, None)

            if not result.success:
                console.print(f"[red]{result.error}[/red]")
                raise typer.Exit(1)

            info = result.value
            assert info is not None  # guaranteed by success=True
            console.print(f"\n[green]Source '{info.name}' registered[/green]")
            console.print(f"  Type: {info.source_type}")
            console.print(f"  Status: {info.status}")
            if info.columns:
                console.print(f"  Columns: {len(info.columns)}")
            if info.row_count_estimate:
                console.print(f"  Rows (est): {info.row_count_estimate:,}")
            if info.credential_instructions:
                console.print("\n[yellow]Credentials needed:[/yellow]")
                for key, val in info.credential_instructions.items():
                    console.print(f"  {key}: {val}")
    finally:
        manager.close()


@app.command("discover")
def discover(
    path: Annotated[
        Path,
        typer.Argument(help="Directory to scan for data files"),
    ] = Path("."),
    recursive: Annotated[
        bool,
        typer.Option("--recursive/--no-recursive", help="Scan subdirectories"),
    ] = True,
    output_dir: Annotated[
        Path,
        typer.Option(
            "--output",
            "-o",
            help="Output directory containing pipeline databases",
        ),
    ] = Path("./pipeline_output"),
) -> None:
    """Scan a directory for data files (CSV, Parquet, JSON, XLSX).

    Examples:

        dataraum sources discover /path/to/data

        dataraum sources discover . --no-recursive
    """
    from dataraum.sources.discovery import discover_sources

    # Get existing sources if output dir exists
    existing: list[str] = []
    if (output_dir / "metadata.db").exists():
        from dataraum.cli.common import get_manager
        from dataraum.core.credentials import CredentialChain
        from dataraum.sources.manager import SourceManager

        manager = get_manager(output_dir)
        try:
            with manager.session_scope() as session:
                sm = SourceManager(session=session, credential_chain=CredentialChain())
                existing = [s.name for s in sm.list_sources()]
        finally:
            manager.close()

    result = discover_sources(
        root=path.resolve(),
        recursive=recursive,
        existing_sources=existing,
    )

    if not result.files:
        console.print(f"[yellow]No data files found in {path}[/yellow]")
        return

    table = RichTable(title=f"Data Files in {result.scan_root}", show_header=True, header_style="bold")
    table.add_column("Path")
    table.add_column("Format")
    table.add_column("Size")
    table.add_column("Columns")
    table.add_column("Rows (est)")

    for f in result.files:
        size = _format_size(f.size_bytes)
        cols = str(len(f.columns)) if f.columns else "-"
        rows = f"{f.row_count_estimate:,}" if f.row_count_estimate else "-"
        table.add_row(f.path, f.format, size, cols, rows)

    console.print(table)

    if existing:
        console.print(f"\n[dim]Already registered: {', '.join(existing)}[/dim]")


@app.command("remove")
def remove_source(
    name: Annotated[
        str,
        typer.Argument(help="Source name to remove"),
    ],
    purge: Annotated[
        bool,
        typer.Option("--purge", help="Also delete analysis results (hard delete)"),
    ] = False,
    output_dir: OutputDirOption = Path("./pipeline_output"),
) -> None:
    """Remove (archive) a registered source.

    By default, analysis results are preserved. Use --purge to delete everything.

    Examples:

        dataraum sources remove old_data

        dataraum sources remove old_data --purge
    """
    from dataraum.cli.common import get_manager
    from dataraum.core.credentials import CredentialChain
    from dataraum.sources.manager import SourceManager

    manager = get_manager(output_dir)
    try:
        with manager.session_scope() as session:
            sm = SourceManager(session=session, credential_chain=CredentialChain())
            result = sm.remove_source(name, purge=purge)

            if not result.success:
                console.print(f"[red]{result.error}[/red]")
                raise typer.Exit(1)

            console.print(f"[green]{result.value}[/green]")
    finally:
        manager.close()


def _format_size(size_bytes: int) -> str:
    """Format bytes as human-readable size."""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    else:
        return f"{size_bytes / (1024 * 1024):.1f} MB"
