"""Import phase - loads data for the session's bound source into raw tables.

This is the first phase in the pipeline. It:

1. Resolves the Source row already written by ``begin_session`` (MCP) or
   ``setup_pipeline._resolve_source_spec`` (CLI).
2. Dispatches by ``source_type``: db_recipe → extract_backend; otherwise →
   file loader (CSV/Parquet/JSON, or a directory of those).
3. Creates raw Table + Column records, table names prefixed with
   ``{source_name}__`` to keep them recognizable in DuckDB.

Per DAT-290 there is exactly one source per pipeline run — no multi-source
fan-out, no synthetic ``multi_source`` row.
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
from dataraum.sources.json import JsonLoader
from dataraum.sources.parquet import ParquetLoader
from dataraum.storage import Column, Source, Table

logger = get_logger(__name__)

_PARQUET_EXTENSIONS = {".parquet", ".pq"}
_JSON_EXTENSIONS = {".json", ".jsonl"}


@analysis_phase
class ImportPhase(BasePhase):
    """Import phase — loads raw data for the bound source.

    Configuration (in ctx.config, populated by ``setup_pipeline``):
        source_name: Registered source name.
        source_type: csv, parquet, json, file, or db_recipe.
        source_connection_config: dict — file path, or recipe queries+backend.
        source_backend: For db_recipe sources only (mssql today).
        source_path: Optional CLI hint (the path the user typed).
        junk_columns: List of column names to drop after loading.

    Outputs:
        raw_tables: List of table_ids for the loaded raw tables.
    """

    @property
    def name(self) -> str:
        return "import"

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
        """Load data for the single source bound to this pipeline run.

        ``setup_pipeline`` populates ``ctx.config`` with the registered
        source's identity and connection config. The Source row already
        exists in the session DB (written by ``begin_session`` in MCP mode,
        or by ``setup_pipeline._resolve_source_spec`` in CLI mode). This
        phase just materializes raw tables and Column records.

        Per DAT-290, there is exactly one source — no fan-out, no synthetic
        multi_source row, no swallowing of per-source failures.
        """
        config = ctx.config
        source_name = config.get("source_name")
        source_type = config.get("source_type")
        source_connection_config = config.get("source_connection_config") or {}
        source_backend = config.get("source_backend")
        explicit_path = config.get("source_path")  # set in CLI mode only

        if not source_name or not source_type:
            return PhaseResult.failed(
                "Pipeline config is missing source_name or source_type. "
                "setup_pipeline must populate them from the registered Source row."
            )

        source = ctx.session.get(Source, ctx.source_id)
        if source is None:
            return PhaseResult.failed(
                f"Source row {ctx.source_id} ('{source_name}') not found in the "
                "session DB. begin_session or setup_pipeline was expected to "
                "create it before import runs."
            )

        # Dispatch by source_type.
        if source_type == "db_recipe":
            if not source_backend:
                return PhaseResult.failed(
                    f"db_recipe source '{source_name}' is missing a backend declaration."
                )
            result = self._load_database_source(
                ctx, source, source_name, source_connection_config, source_backend
            )
        else:
            path_str = explicit_path or source_connection_config.get("path")
            if not path_str:
                return PhaseResult.failed(
                    f"Source '{source_name}' (type={source_type}) has no path "
                    "in its connection_config."
                )
            path = Path(path_str)
            if not path.exists():
                return PhaseResult.failed(f"Source path not found: {path}")
            result = self._load_file_source(ctx, source, source_name, path, source_type)

        if result.status != PhaseStatus.COMPLETED:
            return result

        # Enforce column limit
        limit_error = self._check_column_limit(ctx)
        if limit_error:
            return PhaseResult.failed(limit_error)

        return result

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
            # Load all supported file types (not just dominant format)
            all_patterns = ["*.csv", "*.tsv", "*.parquet", "*.pq", "*.json", "*.jsonl"]

            files: list[Path] = []
            for pat in all_patterns:
                files.extend(sorted(path.glob(pat)))

            if not files:
                return PhaseResult.failed(f"No data files found in {path}")

            from dataraum.sources.manager import MAX_FILES_PER_SOURCE

            if len(files) > MAX_FILES_PER_SOURCE:
                return PhaseResult.failed(
                    f"Directory contains {len(files)} data files (max {MAX_FILES_PER_SOURCE}). "
                    f"Split into multiple sources or reduce the number of files."
                )

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
            summary=f"{len(table_ids)} tables, {total_rows:,} rows",
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
        elif suffix in _JSON_EXTENSIONS:
            json_loader = JsonLoader()
            result = json_loader._load_single_file(
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

        # Find the Table record from session.new (unflushed pending objects).
        # We avoid flush() here because SQLite doesn't handle concurrent flushes
        # well in the free-threaded setup.
        table_record: Table | None = None
        for obj in ctx.session.new:
            if isinstance(obj, Table) and obj.table_id == staged_table.table_id:
                table_record = obj
                break

        # Check for table name collision (e.g., Orders.csv and orders.csv both → same name)
        # Check both flushed records (SELECT) and pending objects (session.new)
        existing_table = ctx.session.execute(
            select(Table).where(
                Table.source_id == source.source_id,
                Table.table_name == prefixed_name,
            )
        ).scalar_one_or_none()
        if existing_table is None:
            for obj in ctx.session.new:
                if (
                    isinstance(obj, Table)
                    and obj.source_id == source.source_id
                    and obj.table_name == prefixed_name
                ):
                    existing_table = obj
                    break
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

        # Update the Table record in-memory (committed when session scope exits)
        if table_record:
            table_record.table_name = prefixed_name
            table_record.duckdb_path = prefixed_name

        return PhaseResult.success(
            outputs={"raw_tables": [str(staged_table.table_id)]},
            records_processed=staged_table.row_count,
            records_created=1,
            warnings=result.warnings,
            summary=f"1 table, {staged_table.row_count:,} rows",
        )

    def _load_database_source(
        self,
        ctx: PhaseContext,
        source: Source,
        source_name: str,
        connection_config: dict[str, Any],
        backend: str,
    ) -> PhaseResult:
        """Materialize a recipe-driven database source.

        Resolves credentials via ``CredentialChain`` keyed by source name
        (``DATARAUM_{NAME}_URL``), then delegates to ``extract_backend`` to
        ATTACH READ_ONLY and run each named SELECT into ``raw_{name}``.
        Per DAT-274: any failure surfaces as ``PhaseResult.failed`` with
        the offending step quoted.
        """
        from uuid import uuid4

        from dataraum.core.credentials import CredentialChain
        from dataraum.sources.backends import extract_backend
        from dataraum.sources.db_recipe import RecipeTable

        raw_queries = connection_config.get("tables") or []
        if not raw_queries:
            return PhaseResult.failed(
                f"Database source '{source_name}' has no recipe queries to materialize."
            )

        queries: list[RecipeTable] = []
        for q in raw_queries:
            if (
                not isinstance(q, dict)
                or "name" not in q
                or "sql" not in q
                or not isinstance(q["name"], str)
                or not isinstance(q["sql"], str)
            ):
                return PhaseResult.failed(
                    f"Database source '{source_name}' has a malformed recipe entry: {q!r}"
                )
            queries.append(RecipeTable(name=q["name"], sql=q["sql"]))

        chain = CredentialChain()
        credential = chain.resolve(source_name)
        if credential is None:
            return PhaseResult.failed(
                f"No credentials found for database source '{source_name}'. "
                f"Set DATARAUM_{source_name.upper()}_URL in .env or add an entry to "
                f"{chain.credentials_file}."
            )

        prefix = f"{source_name}__"
        result = extract_backend(
            backend=backend,
            url=credential.url,
            queries=queries,
            duckdb_conn=ctx.duckdb_conn,
            raw_prefix=prefix,
        )
        if not result.success or result.value is None:
            return PhaseResult.failed(
                f"Database source '{source_name}' extraction failed: {result.error}"
            )
        payload = result.value

        table_ids: list[str] = []
        total_rows = 0
        for extracted in payload.tables:
            table_id = str(uuid4())
            ctx.session.add(
                Table(
                    table_id=table_id,
                    source_id=source.source_id,
                    table_name=extracted.duckdb_table,
                    layer="raw",
                    duckdb_path=extracted.duckdb_table,
                    row_count=extracted.row_count,
                )
            )
            for pos, (col_name, col_type) in enumerate(extracted.columns):
                ctx.session.add(
                    Column(
                        table_id=table_id,
                        column_name=col_name,
                        column_position=pos,
                        raw_type=col_type,
                    )
                )
            table_ids.append(table_id)
            total_rows += extracted.row_count

        if not table_ids:
            return PhaseResult.failed(
                f"No tables materialized from database source '{source_name}'."
            )

        return PhaseResult.success(
            outputs={"raw_tables": table_ids},
            records_processed=total_rows,
            records_created=len(table_ids),
            warnings=payload.warnings,
            summary=f"{len(table_ids)} tables, {total_rows:,} rows",
        )
