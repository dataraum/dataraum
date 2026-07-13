"""Operating-model property graph — SQL/PGQ over the promoted-read views (ADR-0021).

The metadata the pipeline emits is one typed graph. Postgres 19's SQL/PGQ makes
that literal: ``CREATE PROPERTY GRAPH operating_model`` binds vertex/edge element
tables to the ``current_*`` read views (ADR-0008) with explicit ``KEY`` clauses —
no primary key on a view, zero migration, run-versioned through the views. Agents
and validation query it with ``GRAPH_TABLE (... MATCH ...)`` instead of hand-rolled
joins.

**Two query mechanisms, one edge set (ADR-0021).** PG19 SQL/PGQ is *fixed-depth*
only — ``MATCH`` expresses a fixed number of hops; a path quantifier is not
supported. So every read splits:

- 1..N *fixed* hops → PGQ ``MATCH`` (every edge here is 1-hop and native);
- *transitive closure* (reference chains, and later part_of ancestry / calendar
  roll-up) → a bounded recursive CTE over the SAME edge view, capped at a max
  traversal depth (≈4) with a cycle guard.

**Scope (P1 / DAT-726).** Vertices are the typed rows that exist today — ``Column``
and ``Table``; the vocabulary ``Concept`` vertices are P3. Edges/props carried:

    column_node  (KEY column_id)  props: semantic_role (has_role),
                                          materialization (materializes_as)
    table_node   (KEY table_id)   props: is_fact_table / is_dimension_table
    refs          table → table   [relationships]      FK topology + cardinality
    has_dimension table → column  [slice_definitions]  a fact's slice columns
    derived_from  table → table   [enriched_views]     view → fact + dim bases

``rolls_up_to`` (dimension_hierarchies' JSON members) lands in P5 where its
consumer does; concept edges (part_of / disjoint_with / reconciles_with) in P4.

**Bootstrap ordering is load-bearing.** A property graph *depends on* its element
views, and an element view depends on the ``current_*`` views. Postgres refuses to
``DROP VIEW`` while a dependent exists. ``materialize_read_schema`` drops+recreates
every ``current_*`` view on each boot, so the graph + its element views MUST be torn
down first: :func:`drop_property_graph` runs *before* the read-view refresh, and
:func:`materialize_property_graph` rebuilds *after* it.

The DDL is GENERATED (``schema_graph.sql`` via ``dump_ddl``, policed by the
``schema-drift`` CI job) and tokenized exactly like the read surface: ``__WS__`` =
raw workspace schema, ``__READ__`` = read schema. Postgres-only — SQL/PGQ has no
SQLite equivalent, so callers guard on dialect.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import text

from dataraum.storage.read_views import READ_TOKEN, WS_TOKEN, read_schema_name_for

if TYPE_CHECKING:
    from sqlalchemy import Connection

PROPERTY_GRAPH_NAME = "operating_model"

# Element views the graph binds to. Names are deterministic and prefixed ``og_``
# (operating graph) so the graph's contract is decoupled from the read-view names
# it reads. Each is a thin shaping view over the ``current_*`` surface: two carry a
# LEFT JOIN that folds enrichment onto the vertex; the edge views project the
# (source_key, dest_key) pair the physical rows already hold. Order matters only for
# the deterministic dump; at apply time the graph is created after all of them.
_ELEMENT_VIEWS: tuple[str, ...] = (
    "og_tables",
    "og_columns",
    "og_references",
    "og_has_dimension",
    "og_derived_from",
)


def _element_view_sql(name: str) -> str:
    """The tokenized body for one ``og_*`` element view over the read surface.

    Every KEY / SOURCE KEY / DESTINATION KEY column is cast ``::text``. The id
    columns are unbounded ``varchar``, and PG19 SQL/PGQ finds *no equality operator*
    for a ``varchar`` key comparison between an edge endpoint and a vertex KEY — a
    view cannot carry a primary key to satisfy it otherwise. ``text`` resolves the
    comparison; the cast is free (the values are already textual ids).
    """
    if name == "og_tables":
        # Table vertex: the analyzed-representative table + its entity subtype
        # (FactTable / DimensionTable). current_table_entities is (table_id, run)
        # unique post-head, so the LEFT JOIN stays 1:1 and table_id is a valid KEY.
        return (
            f"CREATE VIEW {READ_TOKEN}.og_tables AS\n"
            f"SELECT t.table_id::text AS table_id, t.table_name, t.layer,\n"
            f"       te.is_fact_table, te.is_dimension_table, te.detected_entity_type\n"
            f"FROM {READ_TOKEN}.current_tables t\n"
            f"LEFT JOIN {READ_TOKEN}.current_table_entities te ON te.table_id = t.table_id;"
        )
    if name == "og_columns":
        # Column vertex: the physical column with its semantic role (has_role) and
        # materialization (materializes_as = the witness pattern, else the concept's
        # temporal_behavior claim). Each LEFT-joined table is (column_id, run) unique
        # after head resolution, so the join is 1:1 and column_id is a valid KEY.
        return (
            f"CREATE VIEW {READ_TOKEN}.og_columns AS\n"
            f"SELECT c.column_id::text AS column_id, c.table_id::text AS table_id, c.column_name,\n"
            f"       sa.semantic_role,\n"
            f"       COALESCE(mal.pattern, cc.temporal_behavior) AS materialization\n"
            f"FROM {READ_TOKEN}.current_columns c\n"
            f"LEFT JOIN {READ_TOKEN}.current_semantic_annotations sa ON sa.column_id = c.column_id\n"
            f"LEFT JOIN {READ_TOKEN}.current_column_concepts cc ON cc.column_id = c.column_id\n"
            f"LEFT JOIN {READ_TOKEN}.current_measure_aggregation_lineage mal\n"
            f"       ON mal.measure_column_id = c.column_id;"
        )
    if name == "og_references":
        # refs edge (table → table): the detected FK topology. relationship_id is
        # per-run and unique within one head-resolved view — a fine LOCAL edge key
        # inside one promoted state (never keyed on across runs). Self-referential
        # and multi-hop chains here are what the recursive-CTE closure walks.
        return (
            f"CREATE VIEW {READ_TOKEN}.og_references AS\n"
            f"SELECT relationship_id::text AS relationship_id,\n"
            f"       from_table_id::text AS from_table_id, to_table_id::text AS to_table_id,\n"
            f"       from_column_id, to_column_id, cardinality, relationship_type,\n"
            f"       confidence, is_confirmed\n"
            f"FROM {READ_TOKEN}.current_relationships;"
        )
    if name == "og_has_dimension":
        # has_dimension edge (table → column): a fact table's slice (dimension)
        # columns. slice_id is the per-run local edge key.
        return (
            f"CREATE VIEW {READ_TOKEN}.og_has_dimension AS\n"
            f"SELECT slice_id::text AS slice_id, table_id::text AS table_id,\n"
            f"       column_id::text AS column_id, column_name, slice_type, slice_priority\n"
            f"FROM {READ_TOKEN}.current_slice_definitions;"
        )
    if name == "og_derived_from":
        # derived_from edge (view table → base table): an enriched view derives from
        # its fact plus each exposed dimension table (dimension_table_ids JSON,
        # unnested). edge_key = view_id + role[+dim] keeps it unique across the union
        # — '_'-delimited, NOT ':' (a ':' before a letter is a bind param to text()).
        # view_table_id is nullable (a non-materialized view has no vertex) → skip.
        return (
            f"CREATE VIEW {READ_TOKEN}.og_derived_from AS\n"
            f"SELECT ev.view_id || '_fact' AS edge_key,\n"
            f"       ev.view_table_id::text AS view_table_id,\n"
            f"       ev.fact_table_id::text AS base_table_id, 'fact' AS base_role\n"
            f"FROM {READ_TOKEN}.current_enriched_views ev\n"
            f"WHERE ev.view_table_id IS NOT NULL\n"
            f"UNION ALL\n"
            f"SELECT ev.view_id || '_dim_' || dt.value AS edge_key,\n"
            f"       ev.view_table_id::text AS view_table_id,\n"
            f"       dt.value AS base_table_id, 'dimension' AS base_role\n"
            f"FROM {READ_TOKEN}.current_enriched_views ev\n"
            f"CROSS JOIN LATERAL json_array_elements_text(\n"
            f"     COALESCE(ev.dimension_table_ids, '[]'::json)) AS dt(value)\n"
            f"WHERE ev.view_table_id IS NOT NULL;"
        )
    raise AssertionError(f"unreachable: {name} not an element view")


def _property_graph_sql() -> str:
    """The tokenized ``CREATE PROPERTY GRAPH`` binding the element views.

    Vertex ``KEY`` clauses are explicit (a view has no primary key); edge
    ``SOURCE/DESTINATION KEY ... REFERENCES`` name the vertex element table (no
    schema qualifier — the reference resolves within the graph definition).
    """
    return (
        f"CREATE PROPERTY GRAPH {READ_TOKEN}.{PROPERTY_GRAPH_NAME}\n"
        f"  VERTEX TABLES (\n"
        f"    {READ_TOKEN}.og_tables KEY (table_id) LABEL table_node\n"
        f"      PROPERTIES (table_id, table_name, layer, is_fact_table,\n"
        f"                  is_dimension_table, detected_entity_type),\n"
        f"    {READ_TOKEN}.og_columns KEY (column_id) LABEL column_node\n"
        f"      PROPERTIES (column_id, table_id, column_name, semantic_role, materialization)\n"
        f"  )\n"
        f"  EDGE TABLES (\n"
        f"    {READ_TOKEN}.og_references KEY (relationship_id)\n"
        f"      SOURCE KEY (from_table_id) REFERENCES og_tables (table_id)\n"
        f"      DESTINATION KEY (to_table_id) REFERENCES og_tables (table_id)\n"
        f"      LABEL refs\n"
        f"      PROPERTIES (cardinality, relationship_type, confidence, is_confirmed,\n"
        f"                  from_column_id, to_column_id),\n"
        f"    {READ_TOKEN}.og_has_dimension KEY (slice_id)\n"
        f"      SOURCE KEY (table_id) REFERENCES og_tables (table_id)\n"
        f"      DESTINATION KEY (column_id) REFERENCES og_columns (column_id)\n"
        f"      LABEL has_dimension\n"
        f"      PROPERTIES (column_name, slice_type, slice_priority),\n"
        f"    {READ_TOKEN}.og_derived_from KEY (edge_key)\n"
        f"      SOURCE KEY (view_table_id) REFERENCES og_tables (table_id)\n"
        f"      DESTINATION KEY (base_table_id) REFERENCES og_tables (table_id)\n"
        f"      LABEL derived_from\n"
        f"      PROPERTIES (base_role)\n"
        f"  );"
    )


def graph_statements() -> list[tuple[str, str]]:
    """Deterministic ``(name, tokenized DDL)`` list: element views, then the graph."""
    statements: list[tuple[str, str]] = [(name, _element_view_sql(name)) for name in _ELEMENT_VIEWS]
    statements.append((PROPERTY_GRAPH_NAME, _property_graph_sql()))
    return statements


def dump_graph_ddl() -> str:
    """The full property-graph DDL as one deterministic, tokenized script.

    Mirrors ``read_views.dump_read_ddl`` — each statement is preceded by its drop
    guard so the script is idempotent on apply. The graph is dropped before its
    element views (it depends on them); the views drop after (nothing here depends
    on them once the graph is gone).
    """
    header = (
        "-- GENERATED by `uv run python -m dataraum.storage.dump_ddl` — do not edit.\n"
        "-- Operating-model property graph (ADR-0021): CREATE PROPERTY GRAPH over the\n"
        "-- promoted-read views (SQL/PGQ, Postgres 19). Element views shape the\n"
        "-- current_* surface into (source_key, dest_key) relations; the graph binds\n"
        "-- them with explicit KEY clauses.\n"
        f"-- Tokenized: {WS_TOKEN} = raw workspace schema, {READ_TOKEN} = read schema.\n"
    )
    drops = f"DROP PROPERTY GRAPH IF EXISTS {READ_TOKEN}.{PROPERTY_GRAPH_NAME};\n" + "\n".join(
        f"DROP VIEW IF EXISTS {READ_TOKEN}.{name};" for name in _ELEMENT_VIEWS
    )
    bodies = "\n\n".join(sql for _, sql in graph_statements())
    return header + "\n" + drops + "\n\n" + bodies + "\n"


def drop_property_graph(connection: Connection, workspace_schema: str) -> None:
    """Tear down the graph + its element views (idempotent), in dependency order.

    Runs BEFORE ``materialize_read_schema`` on every boot: that refresh drops+
    recreates the ``current_*`` views the element views depend on, and Postgres
    refuses to drop a view while a dependent (element view / graph) exists. The
    graph goes first (it depends on the element views), then the element views.
    Postgres-only.
    """
    read_schema = read_schema_name_for(workspace_schema)
    connection.execute(text(f'DROP PROPERTY GRAPH IF EXISTS "{read_schema}".{PROPERTY_GRAPH_NAME}'))
    for name in _ELEMENT_VIEWS:
        connection.execute(text(f'DROP VIEW IF EXISTS "{read_schema}".{name}'))


def materialize_property_graph(connection: Connection, workspace_schema: str) -> int:
    """Create the element views + the property graph for one workspace (idempotent).

    Runs AFTER ``materialize_read_schema`` (the graph binds the freshly-created
    ``current_*`` views through the element views). Self-contained: it re-drops the
    graph + views first so a direct call (tests) needs no preceding
    :func:`drop_property_graph`. Postgres-only; callers guard on dialect.

    Returns:
        Number of statements applied (element views + the graph).
    """
    drop_property_graph(connection, workspace_schema)
    read_schema = read_schema_name_for(workspace_schema)
    statements = graph_statements()
    for _, sql in statements:
        connection.execute(
            text(
                sql.replace(READ_TOKEN, f'"{read_schema}"').replace(
                    WS_TOKEN, f'"{workspace_schema}"'
                )
            )
        )
    return len(statements)
