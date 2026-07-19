"""Promoted-read surface — head-joined views + grants (ADR-0008, DAT-453).

Head resolution (DAT-413/408) used to be a per-reader convention: every
consumer of run-stamped metadata had to remember the ``metadata_snapshot_head``
join, and three independent misses corrupted measurements in one week
(DAT-405). This module makes promoted-only reads a property of the database:

- One ``current_<table>`` view per run-stamped table, in a per-workspace READ
  schema (``<ws>_read``). The hard join is written exactly once, here.
- Un-versioned tables get same-named pass-through views, so the read schema is
  a complete surface — the cockpit's Drizzle mirror introspects ONLY it.
- The un-versioned entity anchors additionally get analyzed-representative
  views (``current_tables``/``current_columns``, DAT-655) — the typed-layer
  pick behind a promoted generation head, written once instead of per-consumer.
- Enforcement by grant: the per-workspace reader role gets SELECT on the read
  schema and nothing else. A non-head read is unwritable, not discouraged.

The DDL is GENERATED (``schema_read.sql`` via ``dump_ddl``, policed by the
``schema-drift`` CI job) and tokenized: ``__WS__`` = the raw workspace schema,
``__READ__`` = the read schema. Appliers substitute both (engine bootstrap
here; ``pull-metadata.sh`` via sed) — no ``search_path`` tricks, every
reference explicit.

Two read modes (the git frame, ADR-0008): these views are the tracking-branch
mode for CURRENT-STATE readers. In-run readers (detectors, loaders) never use
them — promote is the terminal step, so mid-run the head still names the PRIOR
run; they read this-run rows + the pinned base-run map (DAT-448).

Rows with ``run_id IS NULL`` (legacy / non-workflow writers) never match a
head and are invisible here by design: nothing promoted them.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import UniqueConstraint, text

from dataraum.storage.base import Base, load_all_models
from dataraum.storage.snapshot_head import GENERATION_STAGE

if TYPE_CHECKING:
    from collections.abc import Iterable

    from sqlalchemy import Connection
    from sqlalchemy import Table as SATable

WS_TOKEN = "__WS__"
READ_TOKEN = "__READ__"

# Run-stamped tables sealed under the per-table generation head where the row
# reaches its table THROUGH the columns table (the row carries column_id only).
# add_source seals a table's whole run under ONE generation head (DAT-506), so
# every column-grain table resolves the same head — the per-stage axis is gone.
_COLUMN_GRAIN: tuple[str, ...] = (
    "type_decisions",
    "type_candidates",
    "statistical_profiles",
    "statistical_quality_metrics",
    "temporal_column_profiles",
    "semantic_annotations",
)

# Run-stamped tables sealed under the per-table generation head, direct table_id.
_TABLE_GRAIN: tuple[str, ...] = (
    "column_eligibility",
    "materialization_recipes",
)

# Run-stamped tables sealed at WORKSPACE-CATALOG grain, mapped to their promoting
# stage: begin_session promotes one (catalog, "catalog") head for its atomic run
# (DAT-506); operating_model promotes (catalog, "operating_model") for the
# lifecycle families (validation + cycles, DAT-438/455). Same target, distinct
# stages — the two stages' runs coexist on one workspace catalog head.
_CATALOG_GRAIN: dict[str, str] = {
    "relationships": "catalog",
    # begin_session composite-key confirmations awaiting their surrogate mint
    # (DAT-277) — same atomic catalogue run as the relationships they gate.
    "surrogate_key_intents": "catalog",
    "table_entities": "catalog",
    # Catalogue-grain per-column semantics authored by the table agent (DAT-637):
    # carries column_id, but sealed under the begin_session (catalog, "catalog")
    # head like the other begin_session catalogue tables — NOT the per-table
    # generation head the object-grain semantic_annotations resolve through.
    "column_concepts": "catalog",
    "enriched_views": "catalog",
    "slice_definitions": "catalog",
    "dimension_hierarchies": "catalog",  # begin_session dimension_hierarchies (DAT-537)
    "bus_matrix": "catalog",  # begin_session dimension_hierarchies phase, Part 2 (DAT-762)
    "derived_columns": "catalog",
    "measure_aggregation_lineage": "catalog",  # begin_session aggregation_lineage (DAT-491)
    "driver_rankings": "catalog",  # begin_session driver_rankings (DAT-546)
    "lifecycle_artifacts": "operating_model",
    "validation_results": "operating_model",
    "detected_business_cycles": "operating_model",
    "metric_additivity": "operating_model",  # operating_model metrics phase (DAT-716)
}

# Written by THREE detect paths: add_source seals per (table:{id}, GENERATION),
# begin_session per the workspace (catalog, "catalog") head, and operating_model's
# terminal detect per (catalog, "operating_model") — a row is current when its run
# is promoted under ANY of those heads (DAT-432/L7: without the third,
# cross_table_consistency's OM-run rows were written but invisible to every
# head-resolved reader).
_DUAL_GRAIN: dict[str, str] = {
    "entropy_objects": "catalog",
    "entropy_readiness": "catalog",
    "claim_witnesses": "catalog",
}

# The head pointer + the run-table anchor are exposed read-only and carry
# ``run_id`` as PART OF THEIR KEY, not as a version axis over rows of some other
# grain — so they pass through whole (DAT-506). ``run_tables`` is keyed
# ``(run_id, table_id)`` (its PK is the grain); ``metadata_snapshot_head`` IS the
# promoted state.
_ALWAYS_PASSTHROUGH: tuple[str, ...] = ("metadata_snapshot_head", "run_tables")

# Run-stamped tables SANCTIONED to lack a ``(key, run_id)`` UNIQUE — the
# failure contract's exempt list (DAT-502 / ADR-0010). The contract: Postgres
# owns within-attempt atomicity (run_phase rolls back FAILED phases); writer
# idempotency owns success-redelivery. Exactly two sanctioned writer forms:
#
#   (a) ``(key, run_id)`` UNIQUE + ON CONFLICT upsert — the DEFAULT, enforced
#       structurally by :func:`enforce_run_grain` below;
#   (b) run-scoped delete-then-insert, ONLY for producers whose row-set can
#       legitimately SHRINK on redelivery — each listed here with its reason.
#
# An unlisted run-stamped table without a run-including UNIQUE fails loud at
# generation time (boot + the ``schema-drift`` CI job via ``dump_ddl``).
_RUN_GRAIN_EXEMPT: dict[str, str] = {
    "entropy_readiness": (
        "form-(b): shrink-to-empty rollup — entropy/readiness.py clears this "
        "run's rows then re-derives; an empty rollup must mean empty rows"
    ),
    "entropy_objects": (
        "form-(b): presence-keyed detector row-set — rows exist only where a "
        "detector fired, and adjudication reads the un-run-versioned "
        "config_overlay live, so a redelivered set can shrink "
        "(entropy/engine.py run-scoped clear)"
    ),
    "enriched_views": (
        "latest-only, name-keyed (DAT-506): a materialized view is name-unique in "
        "the workspace (UNIQUE(fact_table_id)), its content per-run; DuckLake native "
        "snapshots version the artifact, not a Postgres run_id row axis — so no "
        "(key, run_id) UNIQUE on the metadata row"
    ),
    "derived_columns": (
        "skip-guarded: CorrelationsPhase.should_skip is the run-scoped "
        "redelivery guard (all-or-nothing single commit per run)"
    ),
}


def enforce_run_grain(tables: Iterable[SATable]) -> None:
    """Every run-stamped table carries a ``(key, run_id)`` UNIQUE or a sanctioned exemption.

    The structural half of the failure contract (DAT-502): a run-stamped
    writer without a DB-enforced grain silently duplicates under Temporal's
    at-least-once redelivery. Fails loud — at boot and in the ``schema-drift``
    CI gate — for: an unlisted table without a run-including UNIQUE, a listed
    table that has GAINED one (prune the listing), and a listing that names a
    table that is no longer run-stamped.

    Args:
        tables: SQLAlchemy ``Table`` objects to check (the live metadata).

    Raises:
        RuntimeError: any violation, all of them named at once.
    """
    problems: list[str] = []
    versioned: set[str] = set()
    for table in tables:
        if table.name in _ALWAYS_PASSTHROUGH:
            continue  # the head pointer: run_id is its payload, not a version axis
        if "run_id" not in {c.name for c in table.columns}:
            continue
        versioned.add(table.name)
        has_run_unique = any(
            isinstance(c, UniqueConstraint) and "run_id" in {col.name for col in c.columns}
            for c in table.constraints
        )
        if has_run_unique and table.name in _RUN_GRAIN_EXEMPT:
            problems.append(
                f"'{table.name}' carries a run-including UNIQUE but is still on "
                f"_RUN_GRAIN_EXEMPT — prune the stale listing (storage/read_views.py)."
            )
        elif not has_run_unique and table.name not in _RUN_GRAIN_EXEMPT:
            problems.append(
                f"run-stamped table '{table.name}' has no (key, run_id) UNIQUE and no "
                f"sanctioned exemption — give the writer a DB-enforced grain + upsert "
                f"(form a) or list it with a reason in _RUN_GRAIN_EXEMPT (form b, "
                f"storage/read_views.py; failure contract DAT-502/ADR-0010)."
            )
    stale = set(_RUN_GRAIN_EXEMPT) - versioned
    if stale:
        problems.append(
            f"_RUN_GRAIN_EXEMPT names tables without run_id (or dropped tables): "
            f"{sorted(stale)} — prune storage/read_views.py."
        )
    if problems:
        raise RuntimeError("\n".join(problems))


def _current_view_sql(table: str) -> str:
    """The head-joined ``current_<table>`` body for one run-stamped table.

    Head resolution (DAT-506): per-table rows resolve the single generation head
    ``(table:{id}, GENERATION_STAGE)``; workspace-catalog rows resolve the single
    ``(catalog, <stage>)`` head — the workspace IS the schema, so the catalog
    target is the constant ``'catalog'`` and no row carries a ``session_id``.
    """
    if table in _COLUMN_GRAIN:
        return (
            f"CREATE VIEW {READ_TOKEN}.current_{table} AS\n"
            f"SELECT r.* FROM {WS_TOKEN}.{table} r\n"
            f"WHERE EXISTS (\n"
            f"  SELECT 1 FROM {WS_TOKEN}.columns c\n"
            f"  JOIN {WS_TOKEN}.metadata_snapshot_head h\n"
            f"    ON h.target = 'table:' || c.table_id\n"
            f"  WHERE c.column_id = r.column_id\n"
            f"    AND h.stage = '{GENERATION_STAGE}'\n"
            f"    AND h.run_id = r.run_id\n"
            f");"
        )
    if table in _TABLE_GRAIN:
        return (
            f"CREATE VIEW {READ_TOKEN}.current_{table} AS\n"
            f"SELECT r.* FROM {WS_TOKEN}.{table} r\n"
            f"WHERE EXISTS (\n"
            f"  SELECT 1 FROM {WS_TOKEN}.metadata_snapshot_head h\n"
            f"  WHERE h.target = 'table:' || r.table_id\n"
            f"    AND h.stage = '{GENERATION_STAGE}'\n"
            f"    AND h.run_id = r.run_id\n"
            f");"
        )
    if table in _DUAL_GRAIN:
        stage = _DUAL_GRAIN[table]
        # entropy_readiness is the ONE-TRUTH-PER-TARGET rollup: between the two
        # CATALOG-grain heads (catalog vs operating_model) the latest-promoted run
        # wins — without this, an OM run + a begin_session run both being promoted
        # returned TWO conflicting 'current' bands per target and an unpinned reader
        # picked one nondeterministically (review wave-1 blocker). Table-grain rows
        # keep the original dual-grain union (the via_table_head pinning contract).
        # Objects/claim_witnesses stay union: per-detector rows, resolved by the
        # run-aware loaders.
        catalog_grain_precedence = ""
        if table == "entropy_readiness":
            catalog_heads = f"('{stage}', 'operating_model')"
            catalog_grain_precedence = (
                f"  AND (\n"
                f"    NOT EXISTS (\n"
                f"      SELECT 1 FROM {WS_TOKEN}.metadata_snapshot_head h3\n"
                f"      WHERE h3.run_id = r.run_id\n"
                f"        AND h3.target = 'catalog'\n"
                f"        AND h3.stage IN {catalog_heads}\n"
                f"    )\n"
                f"    OR NOT EXISTS (\n"
                f"      SELECT 1 FROM {WS_TOKEN}.{table} r2\n"
                f"      JOIN {WS_TOKEN}.metadata_snapshot_head h2\n"
                f"        ON h2.run_id = r2.run_id\n"
                f"       AND h2.target = 'catalog'\n"
                f"       AND h2.stage IN {catalog_heads}\n"
                f"      WHERE r2.target = r.target\n"
                f"        AND r2.run_id <> r.run_id\n"
                f"        AND h2.promoted_at > (\n"
                f"          SELECT MAX(h3.promoted_at)\n"
                f"          FROM {WS_TOKEN}.metadata_snapshot_head h3\n"
                f"          WHERE h3.run_id = r.run_id\n"
                f"            AND h3.target = 'catalog'\n"
                f"            AND h3.stage IN {catalog_heads}\n"
                f"        )\n"
                f"    )\n"
                f"  )\n"
            )
        # Written by BOTH detect paths, so after add_source + begin_session a
        # column legitimately has TWO current rows (one per sealed run, computed
        # from different detector subsets). The ``via_*`` discriminators let a
        # consumer pin one grain — e.g. the cockpit's per-column reads pin
        # ``via_table_head`` to keep the add_source state, exactly what the
        # hand-rolled head join used to do.
        return (
            f"CREATE VIEW {READ_TOKEN}.current_{table} AS\n"
            f"SELECT r.*,\n"
            f"  EXISTS (\n"
            f"    SELECT 1 FROM {WS_TOKEN}.metadata_snapshot_head h\n"
            f"    WHERE h.stage = '{GENERATION_STAGE}' AND h.run_id = r.run_id\n"
            f"      AND h.target = 'table:' || r.table_id\n"
            f"  ) AS via_table_head,\n"
            f"  EXISTS (\n"
            f"    SELECT 1 FROM {WS_TOKEN}.metadata_snapshot_head h\n"
            f"    WHERE h.stage = '{stage}' AND h.run_id = r.run_id\n"
            f"      AND h.target = 'catalog'\n"
            f"  ) AS via_catalog_head,\n"
            f"  EXISTS (\n"
            f"    SELECT 1 FROM {WS_TOKEN}.metadata_snapshot_head h\n"
            f"    WHERE h.stage = 'operating_model' AND h.run_id = r.run_id\n"
            f"      AND h.target = 'catalog'\n"
            f"  ) AS via_operating_model_head\n"
            f"FROM {WS_TOKEN}.{table} r\n"
            f"WHERE EXISTS (\n"
            f"  SELECT 1 FROM {WS_TOKEN}.metadata_snapshot_head h\n"
            f"  WHERE h.run_id = r.run_id\n"
            f"    AND ((h.stage = '{GENERATION_STAGE}'\n"
            f"      AND h.target = 'table:' || r.table_id)\n"
            f"     OR (h.stage = '{stage}' AND h.target = 'catalog')\n"
            f"     OR (h.stage = 'operating_model' AND h.target = 'catalog'))\n"
            f")\n"
            f"{catalog_grain_precedence}"
            f";"
        )
    if table in _CATALOG_GRAIN:
        stage = _CATALOG_GRAIN[table]
        return (
            f"CREATE VIEW {READ_TOKEN}.current_{table} AS\n"
            f"SELECT r.* FROM {WS_TOKEN}.{table} r\n"
            f"WHERE EXISTS (\n"
            f"  SELECT 1 FROM {WS_TOKEN}.metadata_snapshot_head h\n"
            f"  WHERE h.target = 'catalog'\n"
            f"    AND h.stage = '{stage}'\n"
            f"    AND h.run_id = r.run_id\n"
            f");"
        )
    raise AssertionError(f"unreachable: {table} not classified")


def _current_entity_view_statements() -> list[tuple[str, str]]:
    """Analyzed-representative views for the un-versioned entity anchors (DAT-655).

    ``tables``/``columns`` carry no ``run_id`` — their identity is
    ``(table_name, layer)`` — so the run-stamped machinery above cannot scope
    them, and every logical table surfaces once per physical layer
    (raw/typed/quarantine/…). Consumers kept re-deriving "the analyzed
    representative" (typed-layer collapse) independently, a silent footgun.
    These views write that pick once: the ``typed``-layer row whose table has a
    PROMOTED generation run (``promote_run`` upserts the ``(table:{id},
    generation)`` head as add_source's terminal step; source teardown deletes
    it, retiring the table from this surface). The plain ``tables``/``columns``
    pass-throughs remain for staging/quarantine surfaces — this is an
    additional surface, not a replacement.
    """
    return [
        (
            "current_tables",
            (
                f"CREATE VIEW {READ_TOKEN}.current_tables AS\n"
                f"SELECT t.* FROM {WS_TOKEN}.tables t\n"
                f"WHERE t.layer = 'typed'\n"
                f"  AND EXISTS (\n"
                f"    SELECT 1 FROM {WS_TOKEN}.metadata_snapshot_head h\n"
                f"    WHERE h.target = 'table:' || t.table_id\n"
                f"      AND h.stage = '{GENERATION_STAGE}'\n"
                f");"
            ),
        ),
        (
            "current_columns",
            (
                f"CREATE VIEW {READ_TOKEN}.current_columns AS\n"
                f"SELECT c.* FROM {WS_TOKEN}.columns c\n"
                f"WHERE EXISTS (\n"
                f"  SELECT 1 FROM {WS_TOKEN}.tables t\n"
                f"  JOIN {WS_TOKEN}.metadata_snapshot_head h\n"
                f"    ON h.target = 'table:' || t.table_id\n"
                f"   AND h.stage = '{GENERATION_STAGE}'\n"
                f"  WHERE t.table_id = c.table_id\n"
                f"    AND t.layer = 'typed'\n"
                f");"
            ),
        ),
        # DAT-811 — the served columns of the CURRENT enriched views. Enriched columns are
        # latest-only substrate (no generations, unlike typed): the ``enriched`` Table is
        # reconciled in place each run, so the current set is every column whose table is a
        # current enriched view. Scoped by the SAME (catalog, 'catalog') head that
        # ``current_enriched_views`` uses (``enriched_views`` is _CATALOG_GRAIN), so a
        # column surfaces iff its view's ``og_tables`` vertex does — but read off the BASE
        # ``enriched_views`` (NOT the ``current_enriched_views`` read view), because the
        # read schema is re-materialized DROP-then-CREATE in list order: a view depending on
        # another read view would fail to drop. ``current_columns`` hard-filters
        # ``layer='typed'`` and excludes these; this is the parallel surface ``og_columns``
        # unions in so the catalog describes an enriched view completely.
        # ``source_column_id`` rides along as the typed identity semantics resolve through.
        (
            "current_enriched_columns",
            (
                f"CREATE VIEW {READ_TOKEN}.current_enriched_columns AS\n"
                f"SELECT c.* FROM {WS_TOKEN}.columns c\n"
                f"WHERE EXISTS (\n"
                f"  SELECT 1 FROM {WS_TOKEN}.enriched_views ev\n"
                f"  JOIN {WS_TOKEN}.metadata_snapshot_head h\n"
                f"    ON h.target = 'catalog'\n"
                f"   AND h.stage = 'catalog'\n"
                f"   AND h.run_id = ev.run_id\n"
                f"  WHERE ev.view_table_id = c.table_id\n"
                f");"
            ),
        ),
    ]


def read_view_statements() -> list[tuple[str, str]]:
    """Deterministic ``(view_name, tokenized DDL)`` list for the read schema.

    Built from the live model metadata: every run-stamped table MUST be
    classified into exactly one grain map — an unclassified one fails loud
    here (and thereby in the ``schema-drift`` CI job), so a new versioned
    table cannot silently skip the read surface.
    """
    load_all_models()
    # Failure-contract gate first (DAT-502): writer grain before read surface.
    enforce_run_grain(Base.metadata.tables.values())
    classified = set(_COLUMN_GRAIN) | set(_TABLE_GRAIN) | set(_CATALOG_GRAIN) | set(_DUAL_GRAIN)

    statements: list[tuple[str, str]] = []
    versioned: set[str] = set()
    for table in sorted(Base.metadata.tables.values(), key=lambda t: t.name):
        name = table.name
        if name in _ALWAYS_PASSTHROUGH:
            pass  # pointer table: pass-through below
        elif "run_id" in {c.name for c in table.columns}:
            versioned.add(name)
            if name not in classified:
                raise RuntimeError(
                    f"run-stamped table '{name}' has no read-view grain "
                    f"classification (storage/read_views.py) — every versioned "
                    f"table must appear on the promoted-read surface (ADR-0008)."
                )
            statements.append((f"current_{name}", _current_view_sql(name)))
            continue
        statements.append(
            (
                name,
                f"CREATE VIEW {READ_TOKEN}.{name} AS\nSELECT * FROM {WS_TOKEN}.{name};",
            )
        )

    stale = classified - versioned
    if stale:
        raise RuntimeError(
            f"read-view grain classification names tables without run_id (or "
            f"dropped tables): {sorted(stale)} — prune storage/read_views.py."
        )
    statements.extend(_current_entity_view_statements())
    return statements


def dump_read_ddl() -> str:
    """The full read-schema DDL as one deterministic, tokenized script."""
    header = (
        "-- GENERATED by `uv run python -m dataraum.storage.dump_ddl` — do not edit.\n"
        "-- Promoted-read surface (ADR-0008): current_* head-joined views for\n"
        "-- run-stamped tables + same-named pass-throughs for the rest, plus the\n"
        "-- analyzed-representative entity views current_tables/current_columns\n"
        "-- (DAT-655: typed layer behind a promoted generation head).\n"
        f"-- Tokenized: {WS_TOKEN} = raw workspace schema, {READ_TOKEN} = read schema.\n"
        "-- Appliers substitute both (engine bootstrap; pull-metadata.sh via sed).\n"
    )
    return (
        header
        + "\n"
        + "\n\n".join(
            f"DROP VIEW IF EXISTS {READ_TOKEN}.{name};\n{sql}"
            for name, sql in read_view_statements()
        )
        + "\n"
    )


def read_schema_name_for(workspace_schema: str) -> str:
    """The read schema paired with one ``ws_<id>`` workspace schema."""
    return f"{workspace_schema}_read"


def materialize_read_schema(connection: Connection, workspace_schema: str) -> int:
    """Create/refresh the read schema's views for one workspace (idempotent).

    Runs after ``create_all`` on every boot — DROP + CREATE keeps the views in
    lockstep with the models without migrations (``CREATE OR REPLACE`` cannot
    drop or rename view columns, so the first removed model column would fail
    every boot). Nothing depends on view identity. Postgres-only; callers guard
    on dialect (the SQLite test substrate has no read surface).

    Returns:
        Number of views created/refreshed.
    """
    read_schema = read_schema_name_for(workspace_schema)
    connection.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{read_schema}"'))
    statements = read_view_statements()
    for name, sql in statements:
        connection.execute(text(f'DROP VIEW IF EXISTS "{read_schema}".{name}'))
        connection.execute(
            text(
                sql.replace(READ_TOKEN, f'"{read_schema}"').replace(
                    WS_TOKEN, f'"{workspace_schema}"'
                )
            )
        )
    return len(statements)


# The cockpit's CONTROL-PLANE write surface (DAT-453): the only raw tables the
# writer role can touch, with the minimum verbs. Registering a source, opening
# a session, teaching (config_overlay), and saving a learned query snippet
# (sql_snippets, DAT-486) are deliberate cockpit writes — the teach vocabulary
# IS overlay rows, and save-on-clean grows the snippet library from real
# questions. Everything else stays read-only via the read schema. SELECT is
# included because INSERT … RETURNING needs it (and sql_snippets reads its own
# key for the IS-NULL-aware dedup lookup before INSERT-if-absent).
#
# ``sources`` carries a COLUMN-level UPDATE: the cockpit's select upsert is
# ``INSERT … ON CONFLICT DO UPDATE`` and Postgres checks the UPDATE privilege
# statically at executor startup — even when the conflict arm never runs. The
# column list is exactly the upsert's SET list (cockpit select.ts); identity
# columns (source_id, name, created_at) stay unwritable.
#
# NOTE: grants are append-only on a live cluster — shrinking this dict does not
# REVOKE anything already granted; revoke manually when narrowing.
_CONTROL_WRITE_GRANTS: dict[str, str] = {
    "sources": (
        "SELECT, INSERT, UPDATE (source_type, connection_config, stage, backend, updated_at)"
    ),
    "config_overlay": "SELECT, INSERT, UPDATE",
    # concepts (DAT-728, config→DB): the typed concept vocabulary. `frame`
    # declares/edits concepts as an edit = supersede active (UPDATE superseded_at)
    # + INSERT a new active row; the readiness count SELECTs active rows. Identity
    # (concept_id) and the seed's rows are written engine-side; the cockpit's writes
    # ride the same narrow surface as config_overlay did before the cut.
    "concepts": "SELECT, INSERT, UPDATE",
    # save-on-clean (DAT-486): the cockpit query tool saves learned `query:`
    # snippets. SELECT for the IS-NULL-aware key lookup (the unique key has
    # nullable columns and Postgres is NULLS DISTINCT, so dedup is app-level,
    # not ON CONFLICT) + INSERT-if-absent (first-writer-wins). No UPDATE:
    # failure-replacement / usage telemetry is P2b (DAT-488).
    "sql_snippets": "SELECT, INSERT",
}


def reader_role_for(workspace_schema: str) -> str:
    """The per-workspace reader role paired with one ``ws_<id>`` schema.

    The role IS the workspace resolution (DAT-816): ``ALTER ROLE … SET
    search_path`` pins it to the read schema, so the cockpit's Drizzle mirror
    emits unqualified names and carries zero workspace literals. Cluster-global
    namespace, so the schema name is embedded.
    """
    return _role_name(workspace_schema, "reader")


def writer_role_for(workspace_schema: str) -> str:
    """The per-workspace control-plane writer role for one ``ws_<id>`` schema."""
    return _role_name(workspace_schema, "writer")


def _role_name(workspace_schema: str, suffix: str) -> str:
    candidate = f"{workspace_schema}_{suffix}"
    if len(candidate) > 63:  # Postgres identifier length limit.
        raise ValueError(
            f"role name '{candidate}' exceeds Postgres's 63-char identifier "
            f"limit — shorten the workspace id."
        )
    return candidate


def _ensure_role(connection: Connection, role: str, password: str, search_path: str) -> None:
    """Create-if-absent a LOGIN role, re-assert its password + search_path.

    Idempotent per boot. The password is re-asserted every boot so rotating the
    env secret actually takes effect; the search_path is a ROLE property (applied
    at login), which is the whole schema-resolution mechanism — the client never
    names a schema.
    """
    # Literal-quoting: doubling embedded single quotes is the only escape a
    # standard single-quoted literal needs; the DO body uses a tagged dollar
    # quote so a password containing ``$$`` cannot terminate it.
    pw = password.replace("'", "''")
    connection.execute(
        text(
            "DO $dataraum_role$ BEGIN "
            f"IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{role}') THEN "
            f"CREATE ROLE {role} LOGIN PASSWORD '{pw}'; "
            "END IF; END $dataraum_role$;"
        )
    )
    connection.execute(text(f"ALTER ROLE {role} PASSWORD '{pw}'"))
    connection.execute(text(f'ALTER ROLE {role} SET search_path = "{search_path}"'))


def ensure_workspace_roles(
    connection: Connection,
    workspace_schema: str,
    reader_password: str,
    writer_password: str,
) -> None:
    """Mint the two per-workspace roles that RESOLVE the metadata schema (DAT-816).

    The role, not the client, decides which workspace a connection sees:

    - ``<ws>_reader`` — ``search_path = <ws>_read``, SELECT on that schema and
      nothing else. The cockpit's metadata mirror emits unqualified names, so
      this role is both the schema resolution and the ADR-0008 enforcement:
      raw run-stamped tables (and every other workspace) are unreachable.
    - ``<ws>_writer`` — ``search_path = <ws>``, exactly the control-table verbs
      of ``_CONTROL_WRITE_GRANTS`` (the ADR-0008 deviation). USAGE on the raw
      schema exposes nothing by itself; only the granted tables are reachable.

    The two stay separate clients by design: the read schema's pass-through
    views share names with the raw tables, so one merged search_path would make
    every unqualified name ambiguous about which surface it hits.

    Idempotent, runs every boot (the read views are DROP+CREATEd each boot, so
    the SELECT grants must be re-applied after). Requires CREATEROLE on the
    bootstrap connection (true for the compose superuser; managed-Postgres
    deployments pre-provision the roles instead). Replaces the cluster-global
    ``cockpit_reader`` role — a pre-DAT-816 volume may retain that role; it is
    simply no longer granted to or used.
    """
    read_schema = read_schema_name_for(workspace_schema)
    reader = reader_role_for(workspace_schema)
    writer = writer_role_for(workspace_schema)

    _ensure_role(connection, reader, reader_password, search_path=read_schema)
    connection.execute(text(f'GRANT USAGE ON SCHEMA "{read_schema}" TO {reader}'))
    connection.execute(text(f'GRANT SELECT ON ALL TABLES IN SCHEMA "{read_schema}" TO {reader}'))
    connection.execute(
        text(
            f'ALTER DEFAULT PRIVILEGES IN SCHEMA "{read_schema}" GRANT SELECT ON TABLES TO {reader}'
        )
    )

    _ensure_role(connection, writer, writer_password, search_path=workspace_schema)
    connection.execute(text(f'GRANT USAGE ON SCHEMA "{workspace_schema}" TO {writer}'))
    for table, verbs in _CONTROL_WRITE_GRANTS.items():
        connection.execute(text(f'GRANT {verbs} ON "{workspace_schema}".{table} TO {writer}'))
