"""Reset command - delete pipeline databases."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from dataraum.cli.common import console


def reset(
    output_dir: Annotated[
        Path,
        typer.Argument(
            help="Output directory to reset",
        ),
    ] = Path("./pipeline_output"),
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            "-f",
            help="Skip confirmation prompt",
        ),
    ] = False,
) -> None:
    """Reset pipeline output by deleting databases.

    Removes metadata.db and data.duckdb from the output directory.
    """
    metadata_db = output_dir / "metadata.db"
    duckdb_file = output_dir / "data.duckdb"

    # Check what exists
    files_to_delete = []
    if metadata_db.exists():
        files_to_delete.append(metadata_db)
    if duckdb_file.exists():
        files_to_delete.append(duckdb_file)
    # Also check for WAL files
    for wal_file in output_dir.glob("*.db-wal"):
        files_to_delete.append(wal_file)
    for shm_file in output_dir.glob("*.db-shm"):
        files_to_delete.append(shm_file)

    if not files_to_delete:
        console.print(f"[yellow]No database files found in {output_dir}[/yellow]")
        return

    console.print("\n[bold]Files to delete:[/bold]")
    for f in files_to_delete:
        size_kb = f.stat().st_size / 1024
        console.print(f"  {f.name} ({size_kb:.1f} KB)")

    if not force:
        confirm = typer.confirm("\nDelete these files?")
        if not confirm:
            console.print("[yellow]Cancelled[/yellow]")
            raise typer.Exit(0)

    # Delete files
    for f in files_to_delete:
        f.unlink()
        console.print(f"[green]Deleted {f.name}[/green]")

    console.print("\n[green]Reset complete. Run 'dataraum run' to start fresh.[/green]")
