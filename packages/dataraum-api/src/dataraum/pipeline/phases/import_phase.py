"""Import phase - loads data from sources into raw tables.

This is the first phase in the pipeline. It:
1. Creates or retrieves the Source record
2. Loads data into DuckDB as raw VARCHAR tables
3. Creates Table and Column records in SQLAlchemy
"""

from __future__ import annotations

import time
from pathlib import Path

from sqlalchemy import select

from dataraum.pipeline.base import PhaseContext, PhaseResult
from dataraum.pipeline.phases.base import BasePhase
from dataraum.sources.csv import CSVLoader
from dataraum.sources.csv.null_values import load_null_value_config
from dataraum.storage import Source, Table


class ImportPhase(BasePhase):
    """Import phase - loads raw data from sources.

    Configuration (in ctx.config):
        source_path: Path to CSV file or directory
        source_name: Name for the source (optional, defaults to path stem)
        file_pattern: Glob pattern for directory loading (default: "*.csv")
        junk_columns: List of column names to drop after loading

    Outputs:
        raw_tables: List of table_ids for the loaded raw tables
    """

    @property
    def name(self) -> str:
        return "import"

    @property
    def description(self) -> str:
        return "Load CSV files into raw tables"

    @property
    def dependencies(self) -> list[str]:
        return []

    @property
    def outputs(self) -> list[str]:
        return ["raw_tables"]

    def should_skip(self, ctx: PhaseContext) -> str | None:
        """Skip if raw tables already exist for this source."""
        # Check if source exists and has tables
        stmt = (
            select(Table)
            .join(Source)
            .where(Source.source_id == ctx.source_id, Table.layer == "raw")
        )
        result = ctx.session.execute(stmt)
        existing_tables = result.scalars().all()

        if existing_tables:
            # Check if force reimport is requested
            if ctx.config.get("force_reimport", False):
                return None
            return f"Source already has {len(existing_tables)} raw tables"

        return None

    def _run(self, ctx: PhaseContext) -> PhaseResult:
        """Load data from source.

        Args:
            ctx: Phase context with config containing source_path

        Returns:
            PhaseResult with raw_tables output
        """
        source_path = ctx.config.get("source_path")
        if not source_path:
            return PhaseResult.failed("source_path not provided in config")

        path = Path(source_path)
        if not path.exists():
            return PhaseResult.failed(f"Source path not found: {path}")

        # Determine source name
        source_name = ctx.config.get("source_name", path.stem.lower())

        # Get or create source
        source = self._get_or_create_source(ctx, source_name, path)

        # Load data based on path type
        loader = CSVLoader()
        junk_columns = ctx.config.get("junk_columns", [])

        if path.is_dir():
            result = self._load_directory(
                ctx=ctx,
                loader=loader,
                source=source,
                directory=path,
                file_pattern=ctx.config.get("file_pattern", "*.csv"),
                junk_columns=junk_columns,
            )
        else:
            result = self._load_single_file(
                ctx=ctx,
                loader=loader,
                source=source,
                file_path=path,
                junk_columns=junk_columns,
            )

        return result

    def _get_or_create_source(self, ctx: PhaseContext, source_name: str, path: Path) -> Source:
        """Get existing source or create a new one."""
        # Check for existing source with this ID
        source = ctx.session.get(Source, ctx.source_id)

        if source is None:
            # Create new source
            source = Source(
                source_id=ctx.source_id,
                name=source_name,
                source_type="csv" if path.is_file() else "csv_directory",
                connection_config={"path": str(path)},
            )
            ctx.session.add(source)

        return source

    def _load_directory(
        self,
        ctx: PhaseContext,
        loader: CSVLoader,
        source: Source,
        directory: Path,
        file_pattern: str,
        junk_columns: list[str],
    ) -> PhaseResult:
        """Load all CSV files from a directory."""
        start_time = time.time()
        null_config = load_null_value_config()
        warnings: list[str] = []

        # Find all CSV files
        csv_files = sorted(directory.glob(file_pattern))
        if not csv_files:
            return PhaseResult.failed(
                f"No CSV files found matching '{file_pattern}' in {directory}"
            )

        # Load each file
        table_ids: list[str] = []
        total_rows = 0

        for csv_file in csv_files:
            result = loader._load_single_file(
                file_path=csv_file,
                source_id=source.source_id,
                duckdb_conn=ctx.duckdb_conn,
                session=ctx.session,
                null_config=null_config,
                junk_columns=junk_columns,
            )

            if not result.success:
                warnings.append(f"Failed to load {csv_file.name}: {result.error}")
                continue

            staged_table = result.unwrap()
            table_ids.append(str(staged_table.table_id))
            total_rows += staged_table.row_count

        if not table_ids:
            return PhaseResult.failed("No CSV files were successfully loaded")

        # Note: commit handled by session_scope() in orchestrator
        duration = time.time() - start_time

        return PhaseResult.success(
            outputs={"raw_tables": table_ids},
            records_processed=total_rows,
            records_created=len(table_ids),
            duration=duration,
            warnings=warnings,
        )

    def _load_single_file(
        self,
        ctx: PhaseContext,
        loader: CSVLoader,
        source: Source,
        file_path: Path,
        junk_columns: list[str],
    ) -> PhaseResult:
        """Load a single CSV file."""
        start_time = time.time()
        null_config = load_null_value_config()

        # Use the loader's internal method with our source_id
        result = loader._load_single_file(
            file_path=file_path,
            source_id=source.source_id,
            duckdb_conn=ctx.duckdb_conn,
            session=ctx.session,
            null_config=null_config,
            junk_columns=junk_columns,
        )

        if not result.success:
            return PhaseResult.failed(result.error or "Failed to load file")

        staged_table = result.unwrap()
        duration = time.time() - start_time

        # Note: commit handled by session_scope() in orchestrator

        return PhaseResult.success(
            outputs={"raw_tables": [str(staged_table.table_id)]},
            records_processed=staged_table.row_count,
            records_created=1,
            duration=duration,
            warnings=result.warnings,
        )
