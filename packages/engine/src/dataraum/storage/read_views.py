"""Promoted-read surface — head-joined views + grants (ADR-0008, DAT-453).

Head resolution (DAT-413/408) used to be a per-reader convention: every
consumer of run-stamped metadata had to remember the ``metadata_snapshot_head``
join, and three independent misses corrupted measurements in one week
(DAT-405). This module makes promoted-only reads a property of the database:

- One ``current_<table>`` view per run-stamped table, in a per-workspace READ
  schema (``<ws>_read``). The hard join is written exactly once, here.
- Un-versioned tables get same-named pass-through views, so the read schema is
  a complete surface — the cockpit's Drizzle mirror introspects ONLY it.
- Enforcement by grant: the ``cockpit_reader`` role gets SELECT on the read
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

from sqlalchemy import text

from dataraum.storage.base import Base, load_all_models

if TYPE_CHECKING:
    from sqlalchemy import Connection

WS_TOKEN = "__WS__"
READ_TOKEN = "__READ__"
READER_ROLE = "cockpit_reader"

# Run-stamped tables sealed per (table:{id}, stage) where the row reaches its
# table THROUGH the columns table (the row carries column_id only).
_COLUMN_GRAIN: dict[str, str] = {
    "type_decisions": "typing",
    "type_candidates": "typing",
    "statistical_profiles": "statistics",
    "statistical_quality_metrics": "statistical_quality",
    "temporal_column_profiles": "temporal",
    "semantic_annotations": "semantic_per_column",
}

# Run-stamped tables sealed per (table:{id}, stage) with a direct table_id.
_TABLE_GRAIN: dict[str, str] = {
    "column_eligibility": "column_eligibility",
    "materialization_recipes": "typing",
}

# Run-stamped tables sealed at SESSION grain, mapped to their promoting stage:
# begin_session promotes one (session:{id}, "detect") head for its atomic run
# (DAT-408/448); operating_model promotes (session:{id}, "operating_model")
# for the lifecycle families (validation + cycles, DAT-438/455). Same target,
# distinct stages — the two stages' runs coexist on one session.
_SESSION_GRAIN: dict[str, str] = {
    "relationships": "detect",
    "table_entities": "detect",
    "enriched_views": "detect",
    "slicing_views": "detect",
    "slice_definitions": "detect",
    "column_drift_summaries": "detect",
    "temporal_slice_analyses": "detect",
    "derived_columns": "detect",
    "lifecycle_artifacts": "operating_model",
    "validation_results": "operating_model",
    "detected_business_cycles": "operating_model",
}

# Written by BOTH detect paths: add_source seals per (table:{id}, "detect"),
# begin_session per (session:{id}, "detect") — a row is current when its run
# is promoted under EITHER head.
_DUAL_GRAIN: dict[str, str] = {
    "entropy_objects": "detect",
    "entropy_readiness": "detect",
    "claim_witnesses": "detect",
}

# The head pointer itself is exposed read-only (it IS the promoted state).
_ALWAYS_PASSTHROUGH: tuple[str, ...] = ("metadata_snapshot_head",)


def _current_view_sql(table: str) -> str:
    """The head-joined ``current_<table>`` body for one run-stamped table."""
    if table in _COLUMN_GRAIN:
        stage = _COLUMN_GRAIN[table]
        return (
            f"CREATE VIEW {READ_TOKEN}.current_{table} AS\n"
            f"SELECT r.* FROM {WS_TOKEN}.{table} r\n"
            f"WHERE EXISTS (\n"
            f"  SELECT 1 FROM {WS_TOKEN}.columns c\n"
            f"  JOIN {WS_TOKEN}.metadata_snapshot_head h\n"
            f"    ON h.target = 'table:' || c.table_id\n"
            f"  WHERE c.column_id = r.column_id\n"
            f"    AND h.stage = '{stage}'\n"
            f"    AND h.run_id = r.run_id\n"
            f");"
        )
    if table in _TABLE_GRAIN:
        stage = _TABLE_GRAIN[table]
        return (
            f"CREATE VIEW {READ_TOKEN}.current_{table} AS\n"
            f"SELECT r.* FROM {WS_TOKEN}.{table} r\n"
            f"WHERE EXISTS (\n"
            f"  SELECT 1 FROM {WS_TOKEN}.metadata_snapshot_head h\n"
            f"  WHERE h.target = 'table:' || r.table_id\n"
            f"    AND h.stage = '{stage}'\n"
            f"    AND h.run_id = r.run_id\n"
            f");"
        )
    if table in _DUAL_GRAIN:
        stage = _DUAL_GRAIN[table]
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
            f"    WHERE h.stage = '{stage}' AND h.run_id = r.run_id\n"
            f"      AND h.target = 'table:' || r.table_id\n"
            f"  ) AS via_table_head,\n"
            f"  EXISTS (\n"
            f"    SELECT 1 FROM {WS_TOKEN}.metadata_snapshot_head h\n"
            f"    WHERE h.stage = '{stage}' AND h.run_id = r.run_id\n"
            f"      AND h.target = 'session:' || r.session_id\n"
            f"  ) AS via_session_head\n"
            f"FROM {WS_TOKEN}.{table} r\n"
            f"WHERE EXISTS (\n"
            f"  SELECT 1 FROM {WS_TOKEN}.metadata_snapshot_head h\n"
            f"  WHERE h.stage = '{stage}'\n"
            f"    AND h.run_id = r.run_id\n"
            f"    AND (h.target = 'table:' || r.table_id\n"
            f"      OR h.target = 'session:' || r.session_id)\n"
            f");"
        )
    if table in _SESSION_GRAIN:
        stage = _SESSION_GRAIN[table]
        return (
            f"CREATE VIEW {READ_TOKEN}.current_{table} AS\n"
            f"SELECT r.* FROM {WS_TOKEN}.{table} r\n"
            f"WHERE EXISTS (\n"
            f"  SELECT 1 FROM {WS_TOKEN}.metadata_snapshot_head h\n"
            f"  WHERE h.target = 'session:' || r.session_id\n"
            f"    AND h.stage = '{stage}'\n"
            f"    AND h.run_id = r.run_id\n"
            f");"
        )
    raise AssertionError(f"unreachable: {table} not classified")


def read_view_statements() -> list[tuple[str, str]]:
    """Deterministic ``(view_name, tokenized DDL)`` list for the read schema.

    Built from the live model metadata: every run-stamped table MUST be
    classified into exactly one grain map — an unclassified one fails loud
    here (and thereby in the ``schema-drift`` CI job), so a new versioned
    table cannot silently skip the read surface.
    """
    load_all_models()
    classified = set(_COLUMN_GRAIN) | set(_TABLE_GRAIN) | set(_SESSION_GRAIN) | set(_DUAL_GRAIN)

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
    return statements


def dump_read_ddl() -> str:
    """The full read-schema DDL as one deterministic, tokenized script."""
    header = (
        "-- GENERATED by `uv run python -m dataraum.storage.dump_ddl` — do not edit.\n"
        "-- Promoted-read surface (ADR-0008): current_* head-joined views for\n"
        "-- run-stamped tables + same-named pass-throughs for the rest.\n"
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
# reader role can touch, with the minimum verbs. Registering a source, opening
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
        "SELECT, INSERT, "
        "UPDATE (source_type, connection_config, status, stage, backend, updated_at)"
    ),
    "investigation_sessions": "SELECT, INSERT",
    "config_overlay": "SELECT, INSERT, UPDATE",
    # save-on-clean (DAT-486): the cockpit query tool saves learned `query:`
    # snippets. SELECT for the IS-NULL-aware key lookup (the unique key has
    # nullable columns and Postgres is NULLS DISTINCT, so dedup is app-level,
    # not ON CONFLICT) + INSERT-if-absent (first-writer-wins). No UPDATE:
    # failure-replacement / usage telemetry is P2b (DAT-488).
    "sql_snippets": "SELECT, INSERT",
}


def ensure_reader_role(connection: Connection, workspace_schema: str, password: str) -> None:
    """Create the ``cockpit_reader`` role and grant it the read surface.

    The grant is the ADR-0008 enforcement: the cockpit's metadata connection
    uses this role, so raw run-stamped tables are not even visible to its
    introspection — the wrong query is unwritable, not discouraged. The one
    carve-out is the control-plane write surface (``_CONTROL_WRITE_GRANTS``):
    three un-versioned control tables the cockpit legitimately writes. The
    role is cluster-global and idempotent; grants are per schema. Requires the
    bootstrap connection to hold CREATEROLE (true for the compose superuser;
    managed-Postgres deployments pre-provision the role instead).
    """
    read_schema = read_schema_name_for(workspace_schema)
    # Literal-quoting: doubling embedded single quotes is the only escape a
    # standard single-quoted literal needs; the DO body uses a tagged dollar
    # quote so a password containing ``$$`` cannot terminate it.
    pw = password.replace("'", "''")
    connection.execute(
        text(
            "DO $dataraum_role$ BEGIN "
            f"IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{READER_ROLE}') THEN "
            f"CREATE ROLE {READER_ROLE} LOGIN PASSWORD '{pw}'; "
            "END IF; END $dataraum_role$;"
        )
    )
    # Rotation: the CREATE above fires once per cluster; re-assert the password
    # every boot so changing METADATA_READER_PASSWORD actually takes effect.
    connection.execute(text(f"ALTER ROLE {READER_ROLE} PASSWORD '{pw}'"))
    connection.execute(text(f'GRANT USAGE ON SCHEMA "{read_schema}" TO {READER_ROLE}'))
    connection.execute(
        text(f'GRANT SELECT ON ALL TABLES IN SCHEMA "{read_schema}" TO {READER_ROLE}')
    )
    connection.execute(
        text(
            f'ALTER DEFAULT PRIVILEGES IN SCHEMA "{read_schema}" '
            f"GRANT SELECT ON TABLES TO {READER_ROLE}"
        )
    )
    # Control-plane write surface: USAGE on the raw schema exposes nothing by
    # itself; only the explicitly granted tables become reachable.
    connection.execute(text(f'GRANT USAGE ON SCHEMA "{workspace_schema}" TO {READER_ROLE}'))
    for table, verbs in _CONTROL_WRITE_GRANTS.items():
        connection.execute(text(f'GRANT {verbs} ON "{workspace_schema}".{table} TO {READER_ROLE}'))
