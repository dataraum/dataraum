"""Import phase — loads ONE source's data into raw tables.

``import`` is the per-source activity of an add_source run (DAT-422): a run
ingests a SET of sources, ``AddSourceWorkflow`` executes this phase once per
``source_id`` in its input set, and everything past import is source-free
(session-scoped). Each execution:

1. Resolves the Source row the workflow caller (the cockpit ``select`` tool)
   wrote before triggering ``addSourceWorkflow``.
2. Dispatches by ``source_type``: db_recipe → extract_backend; otherwise →
   file loader (CSV/Parquet/JSON) selected by the source URI's suffix.
3. Creates raw Table + Column records with NARROW, workspace-unique table names
   (DAT-639 — no source prefix; the source is an atomic wrapper, not a namespace).

Source shapes — both written by the cockpit ``select`` tool, the only producer:

- An upload source is CONTENT-keyed (``src_<digest>``) and carries exactly one
  staged object as a one-element ``connection_config['file_uris']`` list
  (DAT-422: one file = one source). Changed bytes mint a new digest → a new
  source → a fresh import, so a presence check is a correct skip for uploads.
  The per-URI loader loop below stays list-generic (it is the load mechanism),
  but nothing produces a multi-element list today. A mid-list failure is
  atomic via the phase runner: ``run_phase`` rolls the session back on a
  FAILED result (DAT-502), so partial Table/Column rows never commit.
- A db source is NAME-keyed and carries the synthesized recipe under
  ``connection_config['tables']`` (a list of ``{name, sql}`` query dicts) plus
  ``recipe_hash`` — sha256 over the canonical ``{backend, tables}`` JSON,
  stamped by ``select``. Name-keying means re-selecting the same source name with a
  different table pick re-points the recipe under raw tables materialized from
  the OLD one, so presence alone cannot justify a skip (DAT-430). At import
  success this phase copies the hash to ``imported_recipe_hash`` — the
  materialization witness ``select`` preserves across re-selects.
  ``should_skip`` skips only while the two match (idempotent re-select / teach
  re-run); a changed recipe falls through to an in-place
  re-import-with-replace (DAT-596) — the source's existing tables across every
  layer (DuckDB tables, ``Table``/``Column`` rows, all run-versioned metadata
  children, the per-table snapshot heads) are torn down, then the new recipe is
  rematerialized — instead of silently serving the stale raw tables. The engine
  never recomputes the hash — both values are opaque tokens minted by one writer
  (``select``), so no cross-language canonicalization contract exists.

Per DAT-389 the source path is an ``s3://<lake-bucket>/<key>`` URI handed
verbatim to DuckDB's ``read_*_auto`` over httpfs — never to ``pathlib``. Because
that URI is a read primitive, the import phase gates it through
``validate_source_uri`` before loader dispatch: anything that is not the lake
bucket (a local path, ``file://``, another bucket, a cred-in-URL form) is a
loud failure, never a silent read. Dispatch is on the URI suffix alone; the
filesystem is never stat'd.

The run-total column limit is NOT enforced here: a per-source check cannot
bound a run that composes many small sources (or re-composes already-imported
ones, where import skips entirely), so ``AddSourceWorkflow`` gates the run's
union via the ``check_column_limit`` activity after the import loop (DAT-430).
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import select

from dataraum.core.uri import uri_suffix, validate_source_uri
from dataraum.pipeline.base import PhaseContext, PhaseResult, PhaseStatus
from dataraum.pipeline.phases.base import BasePhase
from dataraum.pipeline.registry import analysis_phase
from dataraum.sources.base import raw_table_name_for_uri
from dataraum.sources.csv import CSVLoader
from dataraum.sources.csv.null_values import load_null_value_config
from dataraum.sources.json import JsonLoader
from dataraum.sources.parquet import ParquetLoader
from dataraum.storage import Column, Source, Table

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
    """Import phase — loads raw data for the one source this activity is scoped to.

    Configuration (in ctx.config, populated by the worker's ``_build_phase_config``):
        source_name: Registered source name.
        source_type: csv, parquet, json, file, or db_recipe.
        source_connection_config: dict — a one-element ``file_uris`` list for an
            upload source; recipe queries (``tables``) + ``recipe_hash`` for a
            db_recipe source.
        source_backend: For db_recipe sources only (mssql today).
        junk_columns: List of column names to drop after loading.

    Outputs:
        raw_tables: List of table_ids for the loaded raw tables.
    """

    @property
    def name(self) -> str:
        return "import"

    def should_skip(self, ctx: PhaseContext) -> str | None:
        """Skip a source whose existing raw tables still match its config (DAT-430).

        An upload source is content-keyed (changed bytes = a new source), so the
        presence of raw tables IS the content check. A db source is name-keyed —
        ``select`` re-points ``connection_config.tables`` under the same name —
        so its raw tables only justify a skip while the current ``recipe_hash``
        equals the ``imported_recipe_hash`` witness stamped at import. On a
        mismatch (or a missing hash) this returns ``None`` and ``_run``'s db
        path tears down the old tables and re-imports the re-pointed recipe in
        place (DAT-596) — never a silent skip over stale raw tables.
        """
        source_id = ctx.config.get("source_id")
        stmt = select(Table).where(Table.source_id == source_id, Table.layer == "raw")
        existing_tables = ctx.session.execute(stmt).scalars().all()
        if not existing_tables:
            return None

        source = ctx.session.get(Source, source_id)
        if source is not None and source.source_type == "db_recipe":
            config = source.connection_config or {}
            stored = config.get("recipe_hash")
            imported = config.get("imported_recipe_hash")
            if not (stored and imported and stored == imported):
                return None  # → _run tears down + re-imports (recipe changed / unhashed, DAT-596)
            return (
                f"Source already has {len(existing_tables)} raw tables "
                "(recipe unchanged since import)"
            )

        return f"Source already has {len(existing_tables)} raw tables"

    def _run(self, ctx: PhaseContext) -> PhaseResult:
        """Load data for the one source this import activity is scoped to.

        The worker's ``_build_phase_config`` populates ``ctx.config`` with the
        registered source's identity and connection config. The Source row
        already exists in the workspace DB (the workflow caller — the cockpit —
        wrote it before triggering ``addSourceWorkflow``). This phase just
        materializes raw tables and Column records; the run's OTHER sources are
        each handled by their own import activity (DAT-422), so a failure here
        fails exactly this source's import.
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

        source_id = config.get("source_id")
        source = ctx.session.get(Source, source_id)
        if source is None:
            return PhaseResult.failed(
                f"Source row {source_id} ('{source_name}') not found in the "
                "workspace DB. The workflow caller (cockpit) must create it "
                "before import runs."
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
            # explicit, immutable list (DAT-378 / docs/architecture/pipeline.md). No filesystem stat:
            # a missing/unreadable but well-formed source still surfaces as the
            # DuckDB read error through ``Result.fail``. The loader is selected by
            # the URI suffix, so ``source_type`` is not consulted here.
            for uri in source_uris:
                try:
                    validate_source_uri(uri)
                except ValueError as e:
                    return PhaseResult.failed(str(e))
            collision = self._first_name_collision(
                ctx, source_id, [raw_table_name_for_uri(uri) for uri in source_uris]
            )
            if collision is not None:
                return PhaseResult.failed(collision)
            result = self._load_file_source(ctx, source, source_name, source_uris)

        return result

    @staticmethod
    def _first_name_collision(
        ctx: PhaseContext, source_id: str | None, target_names: list[str]
    ) -> str | None:
        """Fail loud if a target raw-table name is already owned by ANOTHER source.

        Workspace-unique table identity (DAT-639): the per-workspace DuckLake
        catalog holds exactly one raw ``orders``. This import is about to write
        ``CREATE OR REPLACE TABLE lake.raw."<name>"`` for each ``target_names``
        entry — a physical write OUTSIDE the SQLAlchemy transaction the phase
        runner rolls back (DAT-502). So a name already materialized by a DIFFERENT
        source must be caught BEFORE the loaders run, or the CREATE OR REPLACE
        silently overwrites that source's data. The same-source case is NOT a
        collision: an upload replays via ``should_skip`` (content-keyed id), a db
        recipe replaces in place via the recipe-hash teardown.

        Returns an actionable failure message (retire the owning source first) on
        the first cross-source collision, else ``None``. The DB ``uq_table_name_
        layer`` constraint is the backstop if a name slips past this pre-check.

        Concurrency caveat (best-effort, NOT serialization): this guard + the DB
        constraint protect against the common case, but two import activities for
        DIFFERENT sources landing the SAME narrow name concurrently can still race
        — A's check passes, B's ``CREATE OR REPLACE`` overwrites A's DuckDB table,
        then B's ``Table`` insert trips ``uq_table_name_layer`` and rolls back the
        SQLAlchemy session, but the DuckDB write is OUTSIDE that rollback (DAT-502).
        Closing that window needs per-workspace import serialization at the
        Temporal layer (deferred); narrow naming makes the window exist where
        source-prefixed names made it structurally impossible.
        """
        if not target_names:
            return None
        rows = ctx.session.execute(
            select(Table.table_name, Table.source_id, Source.name)
            .join(Source, Source.source_id == Table.source_id)
            .where(Table.layer == "raw", Table.table_name.in_(target_names))
        ).all()
        for table_name, owner_id, owner_name in rows:
            if owner_id != source_id:
                return (
                    f"Workspace already has a raw table '{table_name}' from source "
                    f"'{owner_name}'. A workspace holds exactly one '{table_name}' — "
                    "retire that source first to replace it (re-importing identical "
                    "content replays automatically; different content must be retired)."
                )
        return None

    @staticmethod
    def _resolve_file_uris(connection_config: dict[str, Any]) -> list[str]:
        """Resolve the list of ``s3://`` URIs a file source loads.

        A file source carries its objects as an explicit ``file_uris`` list under
        a key DISTINCT from the db_recipe ``tables`` key. The cockpit ``select``
        tool — the only producer — content-keys one source per uploaded file
        (DAT-422), so the persisted list is one-element today; the loader loop
        stays list-generic. There is no scalar ``path`` form and no CLI
        ``source_path`` fallback — ``file_uris`` is the single source of truth.
        """
        uris = connection_config.get("file_uris")
        if not uris:
            return []
        return [str(u) for u in uris]

    def _load_file_source(
        self,
        ctx: PhaseContext,
        source: Source,
        source_name: str,
        source_uris: list[str],
    ) -> PhaseResult:
        """Load a file source's URIs in turn, one raw table per object.

        Each element of ``source_uris`` is an ``s3://<lake-bucket>/<key>`` URI
        (already validated by ``_run``) handed verbatim to the loader, which
        passes it to DuckDB's ``read_*_auto`` — the loader is selected per
        element by its own suffix and names each raw table by its NARROW,
        workspace-unique stem (``<file_stem>``, no source prefix — DAT-639).
        The cockpit ``select`` tool persists one-element lists (one
        content-keyed source per file, DAT-422), so the loop runs once today;
        it stays list-generic because it is the load mechanism. A per-element
        failure fails the whole import (no silent swallow); the phase runner's
        rollback-on-FAILED keeps it all-or-nothing (DAT-502).
        """
        null_config = load_null_value_config()
        junk_columns = ctx.config.get("junk_columns", [])

        table_ids: list[str] = []
        records_processed = 0
        warnings_acc: list[str] = []

        for source_uri in source_uris:
            result = self._load_single_uri(ctx, source, source_uri, null_config, junk_columns)
            if result.status != PhaseStatus.COMPLETED:
                # Multi-URI import is all-or-nothing — and that atomicity is owned
                # by the phase runner, not this loop (DAT-502): ``run_phase`` /
                # ``run_session_phase`` roll the session back on a FAILED result,
                # so the Table/Column rows of the URIs loaded before this one
                # never commit and the next run's ``should_skip`` sees no raw
                # tables. Leftover raw DuckDB tables from the partial load are
                # harmless: the loaders' ``CREATE OR REPLACE TABLE`` overwrites
                # them on the clean re-run.
                return result
            if result.outputs:
                table_ids.extend(result.outputs.get("raw_tables", []))
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

    def _load_single_uri(
        self,
        ctx: PhaseContext,
        source: Source,
        source_uri: str,
        null_config: Any,
        junk_columns: list[str],
    ) -> PhaseResult:
        """Load a single file. Loaders write directly into ``lake.raw.<table>``.

        Post-DAT-341 the loader writes the DuckDB table into ``lake.raw.*`` via
        a fully-qualified ``CREATE OR REPLACE TABLE`` (DAT-378 — idempotent
        across retries) under its NARROW, workspace-unique name (no source
        prefix — DAT-639). There is no rename / cross-schema move step here.

        ``source_uri`` is an ``s3://<lake-bucket>/<key>`` URI; dispatch is on its
        suffix alone.
        """
        suffix = uri_suffix(source_uri)

        if suffix in _PARQUET_EXTENSIONS:
            pq_loader = ParquetLoader()
            result = pq_loader._load_single_file(
                source_uri=source_uri,
                source_id=source.source_id,
                duckdb_conn=ctx.duckdb_conn,
                session=ctx.session,
            )
        elif suffix in _JSON_EXTENSIONS:
            json_loader = JsonLoader()
            result = json_loader._load_single_file(
                source_uri=source_uri,
                source_id=source.source_id,
                duckdb_conn=ctx.duckdb_conn,
                session=ctx.session,
            )
        else:
            csv_loader = CSVLoader()
            result = csv_loader._load_single_file(
                source_uri=source_uri,
                source_id=source.source_id,
                duckdb_conn=ctx.duckdb_conn,
                session=ctx.session,
                null_config=null_config,
                junk_columns=junk_columns,
            )

        if not result.success:
            return PhaseResult.failed(result.error or f"Failed to load {source_uri}")

        staged_table = result.unwrap()

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

        Re-import-with-replace (DAT-596): a db source that already has raw tables
        only reaches here when ``should_skip`` found the current ``recipe_hash``
        differing from the ``imported_recipe_hash`` witness (or either missing) —
        i.e. the recipe was re-pointed under the SAME source name. Rather than
        fail loud (the old deferred-GC stance), tear the source's existing tables
        down across ALL layers (raw/typed/quarantine/enriched) — DuckDB tables,
        ``Table``/``Column`` rows, every run-versioned metadata child, and the
        per-table snapshot heads — then rematerialize the new recipe in place.
        Name-keyed teardown is unambiguous: table names are unique per workspace.
        A fresh import requires the ``select``-stamped ``recipe_hash`` and copies
        it to ``imported_recipe_hash`` on success, completing the witness pair.

        Then resolves credentials via ``CredentialChain`` keyed by source name
        (``DATARAUM_{NAME}_URL``) and delegates to ``extract_backend`` to
        ATTACH READ_ONLY and run each named SELECT into ``raw_{name}``.
        Per DAT-274: any failure surfaces as ``PhaseResult.failed`` with
        the offending step quoted.
        """
        from uuid import uuid4

        from dataraum.core.credentials import CredentialChain
        from dataraum.pipeline.phases._source_teardown import teardown_source_tables
        from dataraum.sources.backends import extract_backend
        from dataraum.sources.db_recipe import RecipeTable

        # Workspace-unique guard (DAT-639), BEFORE teardown: if this recipe targets
        # a raw-table name ANOTHER source already owns, fail loud now — never tear
        # down or overwrite across sources. A name owned by THIS source is the
        # in-place replace handled just below. The recipe query names ARE the
        # narrow physical table names (no prefix; ``raw_prefix=""``).
        recipe_tables = connection_config.get("tables") or []
        collision = self._first_name_collision(
            ctx,
            source.source_id,
            [
                q["name"]
                for q in recipe_tables
                if isinstance(q, dict) and isinstance(q.get("name"), str)
            ],
        )
        if collision is not None:
            return PhaseResult.failed(collision)

        # The recipe was re-pointed under the same source name (``should_skip``
        # declined on a hash mismatch / missing witness, DAT-430). Replace in
        # place: drop the old tables + every metadata child before extracting the
        # new recipe, so no orphaned rows or dangling snapshot heads survive and
        # the re-extract can't collide with the prior run's overlapping names.
        # The phase runner rolls the session back on a FAILED result (DAT-502),
        # so a mid-replace failure leaves the prior state intact. (Files never
        # reach this branch — content-keyed sources mint a new id on change.)
        teardown_source_tables(ctx, source.source_id)

        recipe_hash = connection_config.get("recipe_hash")
        if not isinstance(recipe_hash, str) or not recipe_hash:
            return PhaseResult.failed(
                f"Database source '{source_name}' has no recipe_hash in its "
                "connection_config. The cockpit select tool stamps it when "
                "synthesizing the recipe (DAT-430) — re-create the source via "
                "select rather than seeding the row by hand."
            )

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

        # The CONNECTION a db_recipe source reads from is decoupled from the
        # source's own NAME (DAT-592). A probed query imported as a new source
        # (`wwi_recent_orders`) still reads through the configured connection it was
        # probed against (`wwi`): `connection_config.credential_source` names that
        # connection. Absent it, a source IS its own credential — the table-pick
        # model, where the source name equals the `DATARAUM_{NAME}_URL` key. Only
        # the CREDENTIAL lookup uses this; the raw-table prefix below stays the
        # source's own name, so two query-sources off one DB never collide.
        cred_ref = connection_config.get("credential_source")
        credential_source = cred_ref if isinstance(cred_ref, str) and cred_ref else source_name
        chain = CredentialChain()
        credential = chain.resolve(credential_source)
        if credential is None:
            named = (
                f"'{source_name}'"
                if credential_source == source_name
                else f"'{source_name}' (connection '{credential_source}')"
            )
            return PhaseResult.failed(
                f"No credentials found for database source {named}. "
                f"Set DATARAUM_{credential_source.upper()}_URL in the environment "
                "(via .env or the docker-compose environment)."
            )

        # Narrow, workspace-unique table names (DAT-639) — no source prefix; the
        # recipe's query name IS the table name in the workspace.
        result = extract_backend(
            backend=backend,
            url=credential.url,
            queries=queries,
            duckdb_conn=ctx.duckdb_conn,
            raw_prefix="",
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

        # Stamp the materialization witness (DAT-430): record WHICH recipe these
        # raw tables came from, so a later run can tell an idempotent re-select
        # (hashes match → skip) from a re-pointed recipe (mismatch → teardown +
        # re-import in place, DAT-596). Merge into the ROW's current config, not
        # the phase-start ``connection_config`` snapshot from ctx.config: if a
        # re-select commits mid-import and the engine commits last, stamping the
        # snapshot would silently REVERT the user's new recipe — merging the row
        # value keeps it (the witness is still THIS import's ``recipe_hash``, so
        # the next run's compare mismatches the re-pointed recipe → replace). The
        # other wedge arm — select commits AFTER this stamp — replaces the JSON
        # without the witness (select read the row pre-stamp), so the next run
        # sees no witness and also replaces; correct now (it re-imports the
        # current recipe), full select/import serialization is still deferred.
        # A fresh dict, not in-place
        # mutation — SQLAlchemy's plain JSON column only change-tracks on
        # reassignment. ``select`` carries this key forward when it re-points
        # the config (its upsert replaces the JSON).
        source.connection_config = {
            **(source.connection_config or {}),
            "imported_recipe_hash": recipe_hash,
        }

        return PhaseResult.success(
            outputs={"raw_tables": table_ids},
            records_processed=total_rows,
            records_created=len(table_ids),
            warnings=payload.warnings,
            summary=f"{len(table_ids)} tables, {total_rows:,} rows",
        )
