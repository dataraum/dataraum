"""Import phase - loads data from sources into raw tables.

This is the first phase in the pipeline. It:
1. Creates or retrieves the Source record
2. Loads data into DuckDB as raw tables (VARCHAR for CSV, native types for Parquet)
3. Creates Table and Column records in SQLAlchemy
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from sqlalchemy import func, select

from dataraum.core.config import load_pipeline_config
from dataraum.core.logging import get_logger
from dataraum.pipeline.base import PhaseContext, PhaseResult, PhaseStatus
from dataraum.pipeline.phases.base import BasePhase
from dataraum.pipeline.registry import analysis_phase
from dataraum.sources.csv import CSVLoader
from dataraum.sources.csv.null_values import load_null_value_config
from dataraum.sources.parquet import ParquetLoader
from dataraum.storage import Column, Source, Table

logger = get_logger(__name__)

_CSV_EXTENSIONS = {".csv", ".tsv"}
_PARQUET_EXTENSIONS = {".parquet", ".pq"}


@analysis_phase
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
        return "Load data files into raw tables"

    @property
    def dependencies(self) -> list[str]:
        return []

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
        registered_sources = ctx.config.get("registered_sources")

        if not source_path and not registered_sources:
            return PhaseResult.failed("No source_path or registered_sources provided in config")

        if registered_sources and not source_path:
            result = self._load_registered_sources(ctx, registered_sources)
        else:
            assert isinstance(source_path, str)
            result = self._load_from_path(ctx, source_path)

        if result.status != PhaseStatus.COMPLETED:
            return result

        # Enforce column limit
        limit_error = self._check_column_limit(ctx)
        if limit_error:
            return PhaseResult.failed(limit_error)

        return result

    def _load_from_path(self, ctx: PhaseContext, source_path: str) -> PhaseResult:
        """Load data from a file path (legacy single-source mode)."""
        path = Path(source_path)
        if not path.exists():
            return PhaseResult.failed(f"Source path not found: {path}")

        # Determine source name
        source_name = ctx.config.get("source_name", path.stem.lower())

        # Get or create source
        source = self._get_or_create_source(ctx, source_name, path)

        # Detect source type from path
        source_type = self._detect_source_type(path, ctx.config)
        junk_columns = ctx.config.get("junk_columns", [])

        if source_type == "parquet":
            return self._load_parquet(ctx, source, path)
        else:
            # Default to CSV
            loader = CSVLoader()
            if path.is_dir():
                return self._load_directory(
                    ctx=ctx,
                    loader=loader,
                    source=source,
                    directory=path,
                    file_pattern=ctx.config.get("file_pattern", "*.csv"),
                    junk_columns=junk_columns,
                )
            else:
                return self._load_single_file(
                    ctx=ctx,
                    loader=loader,
                    source=source,
                    file_path=path,
                    junk_columns=junk_columns,
                )

    def _detect_source_type(self, path: Path, config: dict[str, Any]) -> str:
        """Detect source type from file extension or directory contents.

        Args:
            path: Path to file or directory
            config: Phase configuration

        Returns:
            Source type string: "csv" or "parquet"
        """
        if path.is_file():
            if path.suffix.lower() in _PARQUET_EXTENSIONS:
                return "parquet"
            return "csv"

        # Directory: check file_pattern config, then scan for files
        file_pattern = config.get("file_pattern", "")
        if "parquet" in file_pattern or ".pq" in file_pattern:
            return "parquet"

        # Check what files exist in the directory
        parquet_files = list(path.glob("*.parquet")) + list(path.glob("*.pq"))
        csv_files = list(path.glob("*.csv"))

        if parquet_files and not csv_files:
            return "parquet"

        return "csv"

    def _get_or_create_source(self, ctx: PhaseContext, source_name: str, path: Path) -> Source:
        """Get existing source or create a new one."""
        # Check for existing source with this ID
        source = ctx.session.get(Source, ctx.source_id)

        if source is None:
            source_type = self._detect_source_type(path, ctx.config)
            if path.is_dir():
                source_type = f"{source_type}_directory"

            source = Source(
                source_id=ctx.source_id,
                name=source_name,
                source_type=source_type,
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

        # Note: commit handled by session_scope() in scheduler
        return PhaseResult.success(
            outputs={"raw_tables": table_ids},
            records_processed=total_rows,
            records_created=len(table_ids),
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

        # Note: commit handled by session_scope() in scheduler
        return PhaseResult.success(
            outputs={"raw_tables": [str(staged_table.table_id)]},
            records_processed=staged_table.row_count,
            records_created=1,
            warnings=result.warnings,
        )

    def _load_parquet(
        self,
        ctx: PhaseContext,
        source: Source,
        path: Path,
    ) -> PhaseResult:
        """Load Parquet file(s) using ParquetLoader."""
        loader = ParquetLoader()

        if path.is_dir():
            # Find parquet files
            file_pattern = ctx.config.get("file_pattern", "*.parquet")
            parquet_files = sorted(path.glob(file_pattern))
            # Also check .pq if default pattern
            if file_pattern == "*.parquet":
                parquet_files.extend(sorted(path.glob("*.pq")))

            if not parquet_files:
                return PhaseResult.failed(f"No Parquet files found in {path}")

            warnings: list[str] = []
            table_ids: list[str] = []
            total_rows = 0

            for pq_file in parquet_files:
                result = loader._load_single_file(
                    file_path=pq_file,
                    source_id=source.source_id,
                    duckdb_conn=ctx.duckdb_conn,
                    session=ctx.session,
                )

                if not result.success:
                    warnings.append(f"Failed to load {pq_file.name}: {result.error}")
                    continue

                staged_table = result.unwrap()
                table_ids.append(str(staged_table.table_id))
                total_rows += staged_table.row_count

            if not table_ids:
                return PhaseResult.failed("No Parquet files were successfully loaded")

            return PhaseResult.success(
                outputs={"raw_tables": table_ids},
                records_processed=total_rows,
                records_created=len(table_ids),
                warnings=warnings,
            )
        else:
            result = loader._load_single_file(
                file_path=path,
                source_id=source.source_id,
                duckdb_conn=ctx.duckdb_conn,
                session=ctx.session,
            )

            if not result.success:
                return PhaseResult.failed(result.error or "Failed to load Parquet file")

            staged_table = result.unwrap()

            return PhaseResult.success(
                outputs={"raw_tables": [str(staged_table.table_id)]},
                records_processed=staged_table.row_count,
                records_created=1,
                warnings=result.warnings,
            )

    def _check_column_limit(self, ctx: PhaseContext) -> str | None:
        """Check if total column count exceeds the configured limit.

        Returns:
            Error message if limit exceeded, None otherwise.
        """
        pipeline_config = load_pipeline_config()
        max_columns = pipeline_config.get("limits", {}).get("max_columns", 500)

        count = ctx.session.execute(
            select(func.count(Column.column_id))
            .join(Table)
            .where(Table.source_id == ctx.source_id, Table.layer == "raw")
        ).scalar_one()

        if count > max_columns:
            return (
                f"Column limit exceeded: {count} > {max_columns}. "
                f"Reduce tables or increase limits.max_columns in pipeline.yaml."
            )
        return None

    def _load_registered_sources(
        self,
        ctx: PhaseContext,
        registered_sources: list[dict[str, Any]],
    ) -> PhaseResult:
        """Load tables from all registered sources.

        Each source's tables are prefixed with the source name to avoid collisions:
        {source_name}__{table_name}.

        Args:
            ctx: Phase context
            registered_sources: List of source dicts with name, source_type, path, backend

        Returns:
            PhaseResult with all loaded table IDs
        """
        warnings: list[str] = []
        table_ids: list[str] = []
        total_rows = 0

        # Get or create the pipeline source record
        source = ctx.session.get(Source, ctx.source_id)
        if source is None:
            source = Source(
                source_id=ctx.source_id,
                name="multi_source",
                source_type="multi_source",
                connection_config={"sources": [s["name"] for s in registered_sources]},
            )
            ctx.session.add(source)

        for src in registered_sources:
            src_name = src["name"]
            src_type = src["source_type"]
            src_path = src.get("path")

            if src_type in ("csv", "parquet", "file") and src_path:
                result = self._load_file_source(ctx, source, src_name, Path(src_path), src_type)
            elif src.get("backend"):
                result = self._load_database_source(ctx, source, src_name, src)
            else:
                warnings.append(f"Skipping source '{src_name}': unsupported type '{src_type}'")
                continue

            if result.status != PhaseStatus.COMPLETED:
                warnings.append(f"Failed to load source '{src_name}': {result.error}")
                continue

            if result.outputs:
                table_ids.extend(result.outputs.get("raw_tables", []))
            total_rows += result.records_processed

        if not table_ids:
            return PhaseResult.failed("No tables were loaded from any registered source")

        return PhaseResult.success(
            outputs={"raw_tables": table_ids},
            records_processed=total_rows,
            records_created=len(table_ids),
            warnings=warnings,
        )

    def _load_file_source(
        self,
        ctx: PhaseContext,
        source: Source,
        source_name: str,
        path: Path,
        source_type: str,
    ) -> PhaseResult:
        """Load a file source with table name prefixing."""
        if not path.exists():
            return PhaseResult.failed(f"Source path not found: {path}")

        null_config = load_null_value_config()
        junk_columns = ctx.config.get("junk_columns", [])
        table_ids: list[str] = []
        total_rows = 0
        warnings: list[str] = []

        if path.is_dir():
            # Determine file type pattern
            if source_type == "parquet":
                patterns = ["*.parquet", "*.pq"]
            else:
                patterns = ["*.csv"]

            files: list[Path] = []
            for pat in patterns:
                files.extend(sorted(path.glob(pat)))

            if not files:
                return PhaseResult.failed(f"No data files found in {path}")

            for file_path in files:
                result = self._load_single_file_with_prefix(
                    ctx, source, source_name, file_path, null_config, junk_columns
                )
                if result.status == PhaseStatus.COMPLETED and result.outputs:
                    table_ids.extend(result.outputs.get("raw_tables", []))
                    total_rows += result.records_processed
                elif result.status != PhaseStatus.COMPLETED:
                    warnings.append(f"Failed to load {file_path.name}: {result.error}")
        else:
            result = self._load_single_file_with_prefix(
                ctx, source, source_name, path, null_config, junk_columns
            )
            if result.status == PhaseStatus.COMPLETED and result.outputs:
                table_ids.extend(result.outputs.get("raw_tables", []))
                total_rows += result.records_processed
            elif result.status != PhaseStatus.COMPLETED:
                return result

        if not table_ids:
            return PhaseResult.failed(f"No files loaded from source '{source_name}'")

        return PhaseResult.success(
            outputs={"raw_tables": table_ids},
            records_processed=total_rows,
            records_created=len(table_ids),
            warnings=warnings,
        )

    def _load_single_file_with_prefix(
        self,
        ctx: PhaseContext,
        source: Source,
        source_name: str,
        file_path: Path,
        null_config: Any,
        junk_columns: list[str],
    ) -> PhaseResult:
        """Load a single file, prefixing the table name with source_name__."""
        suffix = file_path.suffix.lower()

        if suffix in _PARQUET_EXTENSIONS:
            pq_loader = ParquetLoader()
            result = pq_loader._load_single_file(
                file_path=file_path,
                source_id=source.source_id,
                duckdb_conn=ctx.duckdb_conn,
                session=ctx.session,
            )
        else:
            csv_loader = CSVLoader()
            result = csv_loader._load_single_file(
                file_path=file_path,
                source_id=source.source_id,
                duckdb_conn=ctx.duckdb_conn,
                session=ctx.session,
                null_config=null_config,
                junk_columns=junk_columns,
            )

        if not result.success:
            return PhaseResult.failed(result.error or f"Failed to load {file_path}")

        staged_table = result.unwrap()

        # Rename the table in DuckDB and update SQLAlchemy Table record.
        # The loader creates the DuckDB table as raw_table_name (e.g. "raw_orders")
        # and the SQLAlchemy Table record as table_name (e.g. "orders").
        # We rename both to the prefixed form.
        prefixed_name = f"{source_name}__{staged_table.table_name}"
        duckdb_name = staged_table.raw_table_name

        # Check for table name collision (e.g., Orders.csv and orders.csv both → same name)
        existing_table = ctx.session.execute(
            select(Table).where(
                Table.source_id == source.source_id,
                Table.table_name == prefixed_name,
            )
        ).scalar_one_or_none()
        if existing_table:
            # Drop the just-created DuckDB table to avoid orphans
            try:
                ctx.duckdb_conn.execute(f'DROP TABLE IF EXISTS "{duckdb_name}"')
            except Exception:
                pass
            return PhaseResult.failed(
                f"Table name collision: '{file_path.name}' produces table name "
                f"'{prefixed_name}' which already exists from a previous file"
            )

        try:
            ctx.duckdb_conn.execute(f'ALTER TABLE "{duckdb_name}" RENAME TO "{prefixed_name}"')
        except Exception as e:
            logger.warning(
                "duckdb_rename_failed", table=duckdb_name, target=prefixed_name, error=str(e)
            )

        # Update the SQLAlchemy Table record
        table_record = ctx.session.execute(
            select(Table).where(Table.table_id == staged_table.table_id)
        ).scalar_one_or_none()
        if table_record:
            table_record.table_name = prefixed_name
            table_record.duckdb_path = prefixed_name

        return PhaseResult.success(
            outputs={"raw_tables": [str(staged_table.table_id)]},
            records_processed=staged_table.row_count,
            records_created=1,
            warnings=result.warnings,
        )

    def _load_database_source(
        self,
        ctx: PhaseContext,
        source: Source,
        source_name: str,
        src: dict[str, Any],
    ) -> PhaseResult:
        """Load tables from a database source via DuckDB ATTACH."""
        from dataraum.core.credentials import CredentialChain

        backend = src["backend"]
        credential_ref = src.get("credential_ref", source_name)

        chain = CredentialChain()
        credential = chain.resolve(credential_ref)
        if credential is None:
            return PhaseResult.failed(
                f"No credentials found for database source '{source_name}' "
                f"(credential_ref: {credential_ref})"
            )

        tables_filter = src.get("tables", [])
        table_ids: list[str] = []
        total_rows = 0
        warnings: list[str] = []

        try:
            # Attach the database
            attach_alias = f"_src_{source_name}"
            ctx.duckdb_conn.execute(
                f"ATTACH '{credential.url}' AS \"{attach_alias}\" (TYPE {backend}, READ_ONLY)"
            )

            # Discover tables if not specified
            if not tables_filter:
                rows = ctx.duckdb_conn.execute(
                    f"SELECT table_name FROM information_schema.tables "
                    f"WHERE table_schema = 'main' AND table_catalog = '{attach_alias}'"
                ).fetchall()
                tables_filter = [r[0] for r in rows]

            for table_name in tables_filter:
                prefixed = f"{source_name}__{table_name}"
                try:
                    ctx.duckdb_conn.execute(
                        f'CREATE TABLE "{prefixed}" AS '
                        f'SELECT * FROM "{attach_alias}"."{table_name}"'
                    )

                    # Get row count and column info
                    row_count_result = ctx.duckdb_conn.execute(
                        f'SELECT count(*) FROM "{prefixed}"'
                    ).fetchone()
                    row_count = row_count_result[0] if row_count_result else 0

                    col_info = ctx.duckdb_conn.execute(
                        f"SELECT column_name, data_type FROM information_schema.columns "
                        f"WHERE table_name = '{prefixed}'"
                    ).fetchall()

                    # Create Table + Column records
                    from uuid import uuid4

                    table_id = str(uuid4())
                    table_record = Table(
                        table_id=table_id,
                        source_id=source.source_id,
                        table_name=prefixed,
                        layer="raw",
                        duckdb_path=prefixed,
                        row_count=row_count,
                    )
                    ctx.session.add(table_record)

                    for pos, (col_name, col_type) in enumerate(col_info):
                        col_record = Column(
                            table_id=table_id,
                            column_name=col_name,
                            column_position=pos,
                            raw_type=col_type,
                        )
                        ctx.session.add(col_record)

                    table_ids.append(table_id)
                    total_rows += row_count

                except Exception as e:
                    warnings.append(f"Failed to import {table_name}: {e}")

            # Detach after import
            ctx.duckdb_conn.execute(f'DETACH "{attach_alias}"')

        except Exception as e:
            return PhaseResult.failed(f"Failed to connect to database source '{source_name}': {e}")

        if not table_ids:
            return PhaseResult.failed(f"No tables loaded from database source '{source_name}'")

        return PhaseResult.success(
            outputs={"raw_tables": table_ids},
            records_processed=total_rows,
            records_created=len(table_ids),
            warnings=warnings,
        )
