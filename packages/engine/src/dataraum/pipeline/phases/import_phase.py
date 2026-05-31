"""Import phase - loads data for the session's bound source into raw tables.

This is the first phase in the pipeline. It:

1. Resolves the Source row the workflow caller (the cockpit) wrote before
   triggering ``addSourceWorkflow``.
2. Dispatches by ``source_type``: db_recipe → extract_backend; otherwise →
   file loader (CSV/Parquet/JSON) selected by the source URI's suffix.
3. Creates raw Table + Column records, table names prefixed with
   ``{source_name}__`` to keep them recognizable in DuckDB.

Per DAT-290 there is exactly one source per pipeline run — no multi-source
fan-out, no synthetic ``multi_source`` row.

Per DAT-389 the source path is an ``s3://<lake-bucket>/<key>`` URI handed
verbatim to DuckDB's ``read_*_auto`` over httpfs — never to ``pathlib``. Because
that URI is a read primitive, the import phase gates it through
``validate_source_uri`` before loader dispatch: anything that is not the lake
bucket (a local path, ``file://``, another bucket, a cred-in-URL form) is a
loud failure, never a silent read. Dispatch is on the URI suffix alone; the
filesystem is never stat'd.

Per DAT-378 a file source is a LIST of explicit ``s3://`` URIs under the
``connection_config['file_uris']`` key — the cockpit's ``select`` stage
enumerated the prefix (ListObjectsV2) into that immutable list BEFORE triggering
the workflow (ADR-0007 frozen-artifact: the persisted list is authoritative for
the run). The engine NEVER globs: each element is gated through
``validate_source_uri`` (which forbids glob metacharacters and requires exactly
one object), then loaded in turn, so ONE import activity yields N raw tables
named ``<source_name>__<file_stem>``. The distinct ``file_uris`` key cannot
collide with the db_recipe ``tables`` key (a list of ``{name, sql}`` query
dicts). ``AddSourceWorkflow`` already fans out one ``ProcessTableWorkflow`` per
raw table, so N>1 falls out with no Temporal-contract change.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import func, select

from dataraum.core.config import load_pipeline_config
from dataraum.core.logging import get_logger
from dataraum.core.uri import uri_stem, uri_suffix, validate_source_uri
from dataraum.pipeline.base import PhaseContext, PhaseResult, PhaseStatus
from dataraum.pipeline.phases.base import BasePhase
from dataraum.pipeline.registry import analysis_phase
from dataraum.sources.csv import CSVLoader
from dataraum.sources.csv.null_values import load_null_value_config
from dataraum.sources.json import JsonLoader
from dataraum.sources.parquet import ParquetLoader
from dataraum.storage import Column, Source, Table

logger = get_logger(__name__)

# Suffix → loader. Mirrors the cockpit's connect/upload contract (connect.ts
# FILE_READERS + upload/policy.ts ALLOWED_EXTENSIONS) so what the cockpit lets a
# practitioner select is exactly what the engine can load: csv/tsv/txt → CSV,
# parquet/pq → Parquet, json/jsonl/ndjson → JSON. ``.ndjson`` is newline-delimited
# JSON — it MUST route to the JSON loader, never fall through to the CSV default
# (DAT-378).
_PARQUET_EXTENSIONS = {".parquet", ".pq"}
_JSON_EXTENSIONS = {".json", ".jsonl", ".ndjson"}


@analysis_phase
class ImportPhase(BasePhase):
    """Import phase — loads raw data for the bound source.

    Configuration (in ctx.config, populated by ``setup_pipeline``):
        source_name: Registered source name.
        source_type: csv, parquet, json, file, or db_recipe.
        source_connection_config: dict — a ``file_uris`` list for file sources,
            or recipe queries + backend for db_recipe sources.
        source_backend: For db_recipe sources only (mssql today).
        junk_columns: List of column names to drop after loading.

    Outputs:
        raw_tables: List of table_ids for the loaded raw tables.
    """

    @property
    def name(self) -> str:
        return "import"

    def replay_cleanup(self, ctx: PhaseContext, table_ids: list[str]) -> None:
        """Drop the source's loaded state so a replay from ``import`` starts fresh.

        Triggered when a teach (e.g. ``null_value``) changes how data should
        load: re-importing means everything downstream of import is stale.

        Drops, in order:
            1. All DuckDB tables (raw/typed/quarantine) for the source's
               existing ``Table`` rows, for a clean slate on re-import.
            2. All ``Table`` rows for this source (any layer). Cascades to
               ``Column`` + every per-column row via the model relationships.

        ``table_ids`` is ignored — ``import`` is source-wide; a per-table
        re-import is out of scope (see the DAT-343 refine on the connector
        future where per-table import gets first-class support).
        """
        del table_ids
        from dataraum.core.duckdb_naming import schema_for_layer
        from dataraum.server.storage import LAKE_CATALOG_ALIAS

        source = ctx.session.get(Source, ctx.source_id)
        if source is None:
            return

        rows = list(
            ctx.session.execute(select(Table).where(Table.source_id == ctx.source_id)).scalars()
        )
        # Two loops on purpose: DROP the DuckDB tables FIRST while the rows
        # still know their ``duckdb_path`` (and layer), then delete the
        # Postgres rows. Folding both into one loop would lose the
        # DuckDB-before-Postgres ordering — a partial failure mid-loop
        # would leave Postgres saying "no raw tables for this source"
        # while DuckDB still holds them. (Raw loads are CREATE OR REPLACE since
        # DAT-378, so a leftover raw table no longer collides on re-import; the
        # DROP-first ordering still gives a clean teardown of all layers.)
        for table in rows:
            if not table.duckdb_path:
                continue
            schema = schema_for_layer(table.layer)
            fqn = f'{LAKE_CATALOG_ALIAS}.{schema}."{table.duckdb_path}"'
            ctx.duckdb_conn.execute(f"DROP TABLE IF EXISTS {fqn}")

        for table in rows:
            ctx.session.delete(table)
        ctx.session.flush()

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

        The worker's ``_build_phase_config`` populates ``ctx.config`` with the
        registered source's identity and connection config. The Source row
        already exists in the workspace DB (the workflow caller — the cockpit —
        wrote it before triggering ``addSourceWorkflow``). This phase just
        materializes raw tables and Column records.

        Per DAT-290, there is exactly one source — no fan-out, no synthetic
        multi_source row, no swallowing of per-source failures.
        """
        config = ctx.config
        source_name = config.get("source_name")
        source_type = config.get("source_type")
        source_connection_config = config.get("source_connection_config") or {}
        source_backend = config.get("source_backend")

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
            source_uris = self._resolve_file_uris(source_connection_config)
            if not source_uris:
                return PhaseResult.failed(
                    f"Source '{source_name}' (type={source_type}) has no file URIs "
                    "in its connection_config (expected a non-empty 'file_uris' list)."
                )
            # Each URI is handed verbatim to DuckDB's ``read_*_auto``, so it is a
            # read primitive: gate EVERY element through ``validate_source_uri``
            # before it reaches a loader. Only ``s3://<lake-bucket>/<key>`` (a
            # single object, no glob) passes; a local path, ``file://``, a
            # foreign bucket, or a cred-in-URL form is a loud failure here, not a
            # silent arbitrary-file read (DAT-389). The engine never globs — the
            # cockpit's ``select`` stage already enumerated the prefix into this
            # explicit, immutable list (DAT-378 / ADR-0007). No filesystem stat:
            # a missing/unreadable but well-formed source still surfaces as the
            # DuckDB read error through ``Result.fail``. The loader is selected by
            # the URI suffix, so ``source_type`` is not consulted here.
            for uri in source_uris:
                try:
                    validate_source_uri(uri)
                except ValueError as e:
                    return PhaseResult.failed(str(e))
            # Each file becomes a raw table ``<source>__<file_stem>``. Two selected
            # files with the same basename (``a/data.csv`` + ``b/data.csv``, or
            # ``data.csv`` + ``data.parquet``) map to ONE raw table — the second
            # CREATE OR REPLACE would clobber the first's data and the second
            # Table row would hit the (source_id, table_name, layer) unique
            # constraint. The engine can't silently merge them, so fail loud
            # BEFORE loading anything (atomic); disambiguation is the cockpit
            # select stage's job (DAT-398).
            duplicates = self._duplicate_table_names(source_name, source_uris)
            if duplicates:
                return PhaseResult.failed(
                    f"Source '{source_name}' selects multiple files that map to the "
                    f"same raw table(s): {', '.join(duplicates)}. Each file must have a "
                    "distinct basename — rename or drop the duplicates."
                )
            result = self._load_file_source(ctx, source, source_name, source_uris)

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

    @staticmethod
    def _resolve_file_uris(connection_config: dict[str, Any]) -> list[str]:
        """Resolve the ordered list of ``s3://`` URIs a file source loads (DAT-378).

        A file source carries its objects as an explicit ``file_uris`` list under
        a key DISTINCT from the db_recipe ``tables`` key. A single-file source
        (``add_file_source`` or one uploaded object) stores a one-element list; a
        multi-file source (the cockpit ``select`` stage enumerating a prefix into
        ``file_uris`` via ListObjectsV2) stores many. There is no scalar ``path``
        form and no CLI ``source_path`` fallback — the worker path carries neither
        (the CLI is gone), so ``file_uris`` is the single source of truth.
        """
        uris = connection_config.get("file_uris")
        if not uris:
            return []
        return [str(u) for u in uris]

    @staticmethod
    def _duplicate_table_names(source_name: str, source_uris: list[str]) -> list[str]:
        """Raw table names that more than one URI in the list maps to (DAT-378).

        Each file loads into ``<source>__<file_stem>`` via
        ``table_name_for_source(source_name, uri_stem(uri))``; two URIs sharing a
        basename (across folders, or differing only by extension) collide on one
        raw table. Returns the colliding names, sorted (empty when all distinct),
        so ``_run`` can fail loud before any load rather than let the second
        ``CREATE OR REPLACE`` clobber the first.
        """
        from dataraum.core.duckdb_naming import table_name_for_source

        counts: dict[str, int] = {}
        for uri in source_uris:
            bare = table_name_for_source(source_name, uri_stem(uri))
            counts[bare] = counts.get(bare, 0) + 1
        return sorted(name for name, count in counts.items() if count > 1)

    def _load_file_source(
        self,
        ctx: PhaseContext,
        source: Source,
        source_name: str,
        source_uris: list[str],
    ) -> PhaseResult:
        """Load a file source's URIs in turn, one raw table per object (DAT-378).

        Each element of ``source_uris`` is an ``s3://<lake-bucket>/<key>`` URI
        (already validated by ``_run``) handed verbatim to the loader, which
        passes it to DuckDB's ``read_*_auto`` — the loader is selected per
        element by its own suffix. The loader names each raw table
        ``<source_name>__<file_stem>``, so files with distinct basenames yield
        distinct tables (``_run`` fails loud on duplicate basenames, which would
        otherwise collide on one raw table). This is the per-URI loop the cockpit's ``select``
        enumeration feeds: ONE import activity yields N raw tables, and
        ``AddSourceWorkflow`` fans out one ``ProcessTableWorkflow`` per raw table
        with no Temporal-contract change. A per-element failure fails the whole
        import (no silent swallow).
        """
        null_config = load_null_value_config()
        junk_columns = ctx.config.get("junk_columns", [])

        table_ids: list[str] = []
        # Raw DuckDB tables this run created, so a per-URI failure can DROP them
        # and roll the import back to nothing (see _rollback_partial_load).
        created_duckdb_paths: list[str] = []
        records_processed = 0
        warnings_acc: list[str] = []

        for source_uri in source_uris:
            result = self._load_single_file_with_prefix(
                ctx, source, source_name, source_uri, null_config, junk_columns
            )
            if result.status != PhaseStatus.COMPLETED:
                # Multi-URI import is all-or-nothing. ``PhaseResult.failed`` is a
                # RETURN, not a raise, so ``run_phase``'s ``session_scope`` would
                # COMMIT the URIs loaded before this one on its clean exit — and
                # the next run's ``should_skip`` (which fires when ANY raw table
                # exists for the source) would then SKIP import and silently drop
                # the URIs after the failure. Undo the partial work so the failed
                # import commits nothing and a clean re-run re-imports the whole
                # list (DAT-378).
                self._rollback_partial_load(ctx, created_duckdb_paths)
                return result
            if result.outputs:
                table_ids.extend(result.outputs.get("raw_tables", []))
                created_duckdb_paths.extend(result.outputs.get("duckdb_paths", []))
            records_processed += result.records_processed
            warnings_acc.extend(result.warnings or [])

        if not table_ids:
            return PhaseResult.failed(f"No files loaded from source '{source_name}'")

        return PhaseResult.success(
            outputs={"raw_tables": table_ids},
            records_processed=records_processed,
            records_created=len(table_ids),
            warnings=warnings_acc,
            summary=f"{len(table_ids)} tables, {records_processed:,} rows",
        )

    def _rollback_partial_load(self, ctx: PhaseContext, duckdb_paths: list[str]) -> None:
        """Undo a partially-applied multi-URI load so a failed import commits nothing.

        Drops the raw DuckDB tables created so far this run, then rolls back the
        in-session ``Table``/``Column`` rows. Combined with the loaders' raw
        ``CREATE OR REPLACE TABLE`` (idempotent), this makes a mid-list failure
        atomic: ``should_skip`` sees no raw tables afterward, so a clean re-run
        re-imports the whole list instead of skipping the URIs past the failure
        (DAT-378).
        """
        from dataraum.core.duckdb_naming import schema_for_layer
        from dataraum.server.storage import LAKE_CATALOG_ALIAS

        raw_schema = schema_for_layer("raw")
        try:
            for path in duckdb_paths:
                fqn = f'{LAKE_CATALOG_ALIAS}.{raw_schema}."{path}"'
                try:
                    ctx.duckdb_conn.execute(f"DROP TABLE IF EXISTS {fqn}")
                except Exception:
                    # A failed DROP must not skip the remaining drops OR the
                    # rollback below: a leftover raw table is harmless (the
                    # re-run's CREATE OR REPLACE overwrites it), but a skipped
                    # session.rollback() would COMMIT the partial Table/Column
                    # rows and re-introduce the should_skip wedge.
                    logger.warning("import.rollback_drop_failed", path=path, exc_info=True)
        finally:
            ctx.session.rollback()

    def _load_single_file_with_prefix(
        self,
        ctx: PhaseContext,
        source: Source,
        source_name: str,
        source_uri: str,
        null_config: Any,
        junk_columns: list[str],
    ) -> PhaseResult:
        """Load a single file. Loaders write directly into ``lake.raw.<source>__<table>``.

        Post-DAT-341 the loader composes the source-prefixed identifier and
        writes the DuckDB table into ``lake.raw.*`` via fully-qualified
        ``CREATE OR REPLACE TABLE`` (DAT-378 — idempotent across retries). There
        is no rename / cross-schema move step here.

        ``source_uri`` is an ``s3://<lake-bucket>/<key>`` URI; dispatch is on its
        suffix alone.
        """
        suffix = uri_suffix(source_uri)

        if suffix in _PARQUET_EXTENSIONS:
            pq_loader = ParquetLoader()
            result = pq_loader._load_single_file(
                source_uri=source_uri,
                source_id=source.source_id,
                source_name=source_name,
                duckdb_conn=ctx.duckdb_conn,
                session=ctx.session,
            )
        elif suffix in _JSON_EXTENSIONS:
            json_loader = JsonLoader()
            result = json_loader._load_single_file(
                source_uri=source_uri,
                source_id=source.source_id,
                source_name=source_name,
                duckdb_conn=ctx.duckdb_conn,
                session=ctx.session,
            )
        else:
            csv_loader = CSVLoader()
            result = csv_loader._load_single_file(
                source_uri=source_uri,
                source_id=source.source_id,
                source_name=source_name,
                duckdb_conn=ctx.duckdb_conn,
                session=ctx.session,
                null_config=null_config,
                junk_columns=junk_columns,
            )

        if not result.success:
            return PhaseResult.failed(result.error or f"Failed to load {source_uri}")

        staged_table = result.unwrap()

        return PhaseResult.success(
            outputs={
                "raw_tables": [str(staged_table.table_id)],
                # The raw DuckDB table name (== duckdb_path) so a mid-list failure
                # can DROP exactly what this run created (DAT-378 atomic import).
                "duckdb_paths": [staged_table.table_name],
            },
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
                f"Set DATARAUM_{source_name.upper()}_URL in the environment "
                "(via .env or the docker-compose environment)."
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
