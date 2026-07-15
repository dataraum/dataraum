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
    table_node   (KEY table_id)   props: table_role (fact/periodic_snapshot/dimension)
    refs               table → table     [relationships]      FK topology (conformed dims excluded)
    has_dimension      table → column    [slice_definitions]  a fact's slice cols + dim identity
    derived_from       table → table     [enriched_views]     view → fact + dim bases
    concept_edge       concept → concept [concept_edges]      part_of/disjoint/reconciles (P4)
    conformed_dimension table → table    [slice_definitions]  two facts sharing a dimension (DAT-756)
    temporal_coverage  table → column    [temporal_column_profiles] per (relation × time col) window/grain/last-period (P5)
    measure_time_axis  column → column   [semantic_annotations ⋈ time_columns] a measure's time axes + anchor (P5)
    rolls_up_to        column → column   [dimension_hierarchies]    dimension level roll-up, finest→coarsest (P5)

The temporal edges (DAT-730 / P5) type the calendar half of the graph. ``rolls_up_to``
unnests ``dimension_hierarchies.members`` (a drill-down chain, finest→coarsest) into
consecutive level→level edges; its transitive closure (the full drill path) is walked
by the bounded recursive CTE, like ``part_of``. ``temporal_coverage`` is per (relation
× time column) — ``table_entities.time_columns`` is plural, so role-playing dates
(order/ship/due/paid) each surface as a distinct edge carrying that column's data
window, period grain and last-period-complete flag. ``measure_time_axis`` gives every
measure its time axes with one designated ANCHOR (``is_anchor`` — the default trend
axis, the fact's primary/first ``time_columns`` entry); the non-anchor rows are the
enumerable alternates with their roles. Two auxiliary read views ride the same graph
lifecycle but are NOT PGQ-bound (they need no vertex identity, and the period ladder is
walked by the recursive CTE, not PGQ): ``og_calendar`` — the workspace calendar
(observed window + base grain DERIVED from ``detected_granularity``, not a config
constant; fiscal-year start) — and ``og_period_grain`` — the day→month→quarter→year
roll-up ladder the recursive CTE walks to derive "last complete quarter" from "last
complete month".

The ``concept_edge`` edge (DAT-729) carries the vocabulary relations
``part_of`` / ``disjoint_with`` / ``reconciles_with`` as a ``predicate`` property;
its transitive closure (``part_of`` ancestry) is walked by the bounded recursive CTE.
``conformed_dimension`` (DAT-756) types two facts sharing a dimension AXIS — the same
resolved ``(dimension_table_id, attribute)`` identity, NOT a column name — as a
drill-across path (an alignable GROUP BY the SQL agents can author over). It is
ATTRIBUTE grain (the actionable unit for SQL is a shared axis, not a shared table),
deliberately DECOUPLED from the table-grain ``refs`` fan-trap exclusion below — the two
serve different consumers, so a cross-level fan trap is excluded from ``refs`` yet
correctly has no conformed edge (see the edge's own note).

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

from dataraum.storage.read_views import (
    READ_TOKEN,
    READER_ROLE,
    WS_TOKEN,
    read_schema_name_for,
)

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
    "og_concepts",
    "og_references",
    "og_has_dimension",
    "og_derived_from",
    "og_concept_edges",
    "og_conformed_dimension",
    "og_temporal_coverage",
    "og_measure_time_axis",
    "og_rolls_up_to",
)

# Auxiliary read views (DAT-730 / P5) created + dropped on the graph lifecycle but
# NOT bound into CREATE PROPERTY GRAPH: the workspace calendar and the period-grain
# roll-up ladder. Neither needs a graph vertex identity — the calendar is a single
# derived row queried directly, and the ladder is walked by the recursive-CTE closure
# (AC5: period-grain roll-up is "via the recursive CTE", not a PGQ path). They depend
# on the ``current_*`` views (like the element views), so they share the drop-before-
# refresh / create-after ordering.
_AUX_VIEWS: tuple[str, ...] = (
    "og_calendar",
    "og_period_grain",
)

# Everything the graph lifecycle drops/creates around the property graph, in a
# deterministic order (element views first, then the aux views).
_ALL_VIEWS: tuple[str, ...] = _ELEMENT_VIEWS + _AUX_VIEWS


def _element_view_sql(name: str) -> str:
    """The tokenized body for one ``og_*`` element view over the read surface.

    Every KEY / SOURCE KEY / DESTINATION KEY column is cast ``::text``. The id
    columns are unbounded ``varchar``, and PG19 SQL/PGQ finds *no equality operator*
    for a ``varchar`` key comparison between an edge endpoint and a vertex KEY — a
    view cannot carry a primary key to satisfy it otherwise. ``text`` resolves the
    comparison; the cast is free (the values are already textual ids).
    """
    if name == "og_tables":
        # Table vertex: the analyzed-representative table + its role
        # (table_role: fact / periodic_snapshot / dimension). current_table_entities
        # is (table_id, run) unique post-head, so the LEFT JOIN stays 1:1 and
        # table_id is a valid KEY.
        return (
            f"CREATE VIEW {READ_TOKEN}.og_tables AS\n"
            f"SELECT t.table_id::text AS table_id, t.table_name, t.layer,\n"
            f"       te.table_role, te.detected_entity_type\n"
            f"FROM {READ_TOKEN}.current_tables t\n"
            f"LEFT JOIN {READ_TOKEN}.current_table_entities te ON te.table_id = t.table_id;"
        )
    if name == "og_columns":
        # Column vertex: the physical column with its semantic role (has_role) and
        # materialization (materializes_as → flow | stock). The two source columns
        # carry DIFFERENT raw vocabularies and must be normalized, NOT COALESCEd raw:
        #   measure_aggregation_lineage.pattern ∈ {per_period, cumulative}  (DAT-491
        #     witness posterior — per_period ⇒ flow, cumulative ⇒ stock),
        #   column_concepts.temporal_behavior ∈ {additive, point_in_time}   (ontology
        #     prior — the canonical drivers map: additive ⇒ flow, point_in_time ⇒ stock).
        # Prefer the data-reconciled witness posterior over the prior claim; NULL when
        # neither is present. Each LEFT-joined table is (column_id, run) unique after
        # head resolution, so the join is 1:1 and column_id is a valid KEY.
        return (
            f"CREATE VIEW {READ_TOKEN}.og_columns AS\n"
            f"SELECT c.column_id::text AS column_id, c.table_id::text AS table_id, c.column_name,\n"
            f"       sa.semantic_role,\n"
            f"       COALESCE(\n"
            f"         CASE mal.pattern WHEN 'per_period' THEN 'flow' WHEN 'cumulative' THEN 'stock' END,\n"
            f"         CASE cc.temporal_behavior WHEN 'additive' THEN 'flow'\n"
            f"                                   WHEN 'point_in_time' THEN 'stock' END\n"
            f"       ) AS materialization\n"
            f"FROM {READ_TOKEN}.current_columns c\n"
            f"LEFT JOIN {READ_TOKEN}.current_semantic_annotations sa ON sa.column_id = c.column_id\n"
            f"LEFT JOIN {READ_TOKEN}.current_column_concepts cc ON cc.column_id = c.column_id\n"
            f"LEFT JOIN {READ_TOKEN}.current_measure_aggregation_lineage mal\n"
            f"       ON mal.measure_column_id = c.column_id;"
        )
    if name == "og_concepts":
        # Concept vertex: the workspace's typed vocabulary (DAT-728). Active rows
        # only (superseded_at IS NULL) — the partial-unique index makes concept_id
        # unique among them, so it is a valid KEY. Its vocabulary edges are the
        # `concept_edge` edge below (DAT-729); `grounded_by` arrives with P2.
        #
        # dimension_order (DAT-730): a DimensionConcept's window/comparison kind —
        # 'ordered' (points on a line → lag/lead/Δ/CAGR) vs 'nominal' (a set →
        # share/rank/pairwise). The 'ordered' value is authored in the vertical seed
        # (config→DB — ``fiscal_period`` is 'ordered'); any DimensionConcept left unset
        # (a seed default, or a frame-declared one — cockpit authoring of the value is
        # not wired yet) defaults to 'nominal' HERE, so every one carries an order
        # (AC7). NULL for non-dimension kinds (measure/entity/unit have no window axis).
        return (
            f"CREATE VIEW {READ_TOKEN}.og_concepts AS\n"
            f"SELECT concept_id::text AS concept_id, vertical, name, kind,\n"
            f"       CASE WHEN kind = 'dimension' THEN COALESCE(dimension_order, 'nominal')\n"
            f"            END AS dimension_order\n"
            f"FROM {READ_TOKEN}.concepts\n"
            f"WHERE superseded_at IS NULL;"
        )
    if name == "og_concept_edges":
        # concept_edge (concept → concept): the vocabulary relations part_of /
        # disjoint_with / reconciles_with (DAT-729), predicate carried as a property.
        # concept_edges stores endpoints by the stable (vertical, name) key — the P3
        # identity contract — so each endpoint JOINs to its ACTIVE concept to resolve
        # the concept_id the PGQ vertex KEY needs. The INNER JOINs drop an edge whose
        # endpoint concept is superseded/absent (the graph never dangles). edge_id is
        # the per-edge local key (unique among active rows via the partial index).
        return (
            f"CREATE VIEW {READ_TOKEN}.og_concept_edges AS\n"
            f"SELECT e.edge_id::text AS edge_id,\n"
            f"       cf.concept_id::text AS from_concept_id,\n"
            f"       ct.concept_id::text AS to_concept_id,\n"
            f"       e.predicate, e.tolerance\n"
            f"FROM {READ_TOKEN}.concept_edges e\n"
            f"JOIN {READ_TOKEN}.concepts cf\n"
            f"  ON cf.vertical = e.vertical AND cf.name = e.from_concept\n"
            f" AND cf.superseded_at IS NULL\n"
            f"JOIN {READ_TOKEN}.concepts ct\n"
            f"  ON ct.vertical = e.vertical AND ct.name = e.to_concept\n"
            f" AND ct.superseded_at IS NULL\n"
            f"WHERE e.superseded_at IS NULL;"
        )
    if name == "og_references":
        # refs edge (table → table): the detected FK topology. relationship_id is
        # per-run and unique within one head-resolved view — a fine LOCAL edge key
        # inside one promoted state (never keyed on across runs). Self-referential
        # and multi-hop chains here are what the recursive-CTE closure walks.
        #
        # Conformed-dimension exclusion (DAT-756, rebuilding DAT-729 on identity): a
        # relationship whose BOTH endpoints are fact SLICE columns resolving the SAME
        # dimension_table_id is not an FK — it is two facts sharing a dimension (the
        # DAT-723 fan trap), typed as the og_conformed_dimension edge below and dropped
        # here. Keyed on the resolved dimension IDENTITY, never column names (the wrong
        # signal the revert removed). A genuine fact→dim FK survives: a dimension's key
        # is never a fact slice column, so at most one endpoint matches a slice row and
        # the NOT EXISTS cannot fire.
        return (
            f"CREATE VIEW {READ_TOKEN}.og_references AS\n"
            f"SELECT relationship_id::text AS relationship_id,\n"
            f"       from_table_id::text AS from_table_id, to_table_id::text AS to_table_id,\n"
            f"       from_column_id, to_column_id, cardinality, relationship_type,\n"
            f"       confidence, is_confirmed\n"
            f"FROM {READ_TOKEN}.current_relationships r\n"
            f"WHERE NOT EXISTS (\n"
            f"  SELECT 1 FROM {READ_TOKEN}.current_slice_definitions s1\n"
            f"  JOIN {READ_TOKEN}.current_slice_definitions s2\n"
            f"    ON s1.dimension_table_id = s2.dimension_table_id\n"
            f"  WHERE s1.column_id = r.from_column_id AND s2.column_id = r.to_column_id\n"
            f"    AND s1.table_id <> s2.table_id AND s1.dimension_table_id IS NOT NULL\n"
            f");"
        )
    if name == "og_has_dimension":
        # has_dimension edge (table → column): a fact table's slice (dimension)
        # columns, CARRYING the resolved referenced-dimension identity (DAT-756):
        # dimension_table_id (the FK-target dim table — NULL for a folded slice),
        # dimension_attribute (the level), fk_role (the FK column). This is where the
        # edge "binds to identity": two facts whose has_dimension edges share
        # (dimension_table_id, dimension_attribute) reference one conformed dimension,
        # which the og_conformed_dimension edge derives. slice_id is the per-run local
        # edge key.
        return (
            f"CREATE VIEW {READ_TOKEN}.og_has_dimension AS\n"
            f"SELECT slice_id::text AS slice_id, table_id::text AS table_id,\n"
            f"       column_id::text AS column_id, column_name, slice_type, slice_priority,\n"
            f"       dimension_table_id::text AS dimension_table_id,\n"
            f"       dimension_attribute, fk_role\n"
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
            f"SELECT (ev.view_id || '_fact')::text AS edge_key,\n"
            f"       ev.view_table_id::text AS view_table_id,\n"
            f"       ev.fact_table_id::text AS base_table_id, 'fact' AS base_role\n"
            f"FROM {READ_TOKEN}.current_enriched_views ev\n"
            f"WHERE ev.view_table_id IS NOT NULL\n"
            f"UNION ALL\n"
            f"SELECT (ev.view_id || '_dim_' || dt.value)::text AS edge_key,\n"
            f"       ev.view_table_id::text AS view_table_id,\n"
            f"       dt.value AS base_table_id, 'dimension' AS base_role\n"
            f"FROM {READ_TOKEN}.current_enriched_views ev\n"
            f"CROSS JOIN LATERAL json_array_elements_text(\n"
            f"     COALESCE(ev.dimension_table_ids, '[]'::json)) AS dt(value)\n"
            f"WHERE ev.view_table_id IS NOT NULL;"
        )
    if name == "og_conformed_dimension":
        # conformed_dimension edge (table → table): two facts sharing a dimension AXIS
        # (DAT-756, rebuilding the reverted DAT-729 edge on referenced identity).
        # Derived by self-joining slice (has_dimension) rows on the SAME resolved
        # identity — (dimension_table_id, dimension_attribute), NEVER column names —
        # of DIFFERENT tables.
        #
        # ATTRIBUTE grain, and deliberately DECOUPLED from the og_references fan-trap
        # exclusion (which is TABLE grain) — they serve different consumers:
        #   - This edge exists to hand a SQL author an ALIGNABLE drill-across axis. Both
        #     agents (engine GraphAgent, cockpit answer agent) author SQL over COLUMNS,
        #     GROUP BY-ing slice columns — you cannot GROUP BY a table. The actionable
        #     unit is therefore "fact A's column and fact B's column are the SAME axis"
        #     = a shared (dim table, attribute); that is exactly the within-fact `alias`
        #     hierarchy ("region ≡ region_code") lifted across facts. So two facts
        #     conform iff they expose the same axis at the same level.
        #   - The refs exclusion hides a fan trap, defined by two fact FKs sharing a dim
        #     TABLE regardless of how each is sliced — a table-grain fact.
        # A cross-level fan trap (fact-A-by-type ↔ fact-B-by-region) is thus excluded
        # from refs (correct — not an FK) AND has no conformed edge (correct — no common
        # axis to drill across); the facts' shared dimension is still visible via their
        # genuine fact→dim FKs, and this edge only asserts the stronger, actionable
        # "drill these across THIS axis." COALESCE pairs the slice-by-FK-key case (NULL
        # attribute) with itself. Folded slices (NULL dimension_table_id) have no dim
        # table to conform over and are excluded. Symmetric — both directions emitted
        # (edge_key = the ordered slice-id pair, '_'-joined, NOT ':' a bind-param sigil
        # to text()). A fact with multiple role-playing FKs at one axis yields one edge
        # per slice-row pair, so a table pair can carry several conformed edges.
        return (
            f"CREATE VIEW {READ_TOKEN}.og_conformed_dimension AS\n"
            f"SELECT (s1.slice_id || '_' || s2.slice_id)::text AS edge_key,\n"
            f"       s1.table_id::text AS from_table_id, s2.table_id::text AS to_table_id,\n"
            f"       s1.dimension_table_id::text AS dimension_table_id,\n"
            f"       s1.dimension_attribute AS dimension_attribute\n"
            f"FROM {READ_TOKEN}.current_slice_definitions s1\n"
            f"JOIN {READ_TOKEN}.current_slice_definitions s2\n"
            f"  ON s1.dimension_table_id = s2.dimension_table_id\n"
            f" AND COALESCE(s1.dimension_attribute, '') = COALESCE(s2.dimension_attribute, '')\n"
            f" AND s1.table_id <> s2.table_id\n"
            f"WHERE s1.dimension_table_id IS NOT NULL;"
        )
    if name == "og_temporal_coverage":
        # temporal_coverage edge (table → time-column, DAT-730): one edge per
        # (relation × time column) carrying that column's observed data window,
        # detected period grain, gap-completeness and the last-period-complete flag
        # (a trailing PARTIAL period → False; the AR-NULL-at-MAX surprise made
        # queryable). Because coverage is per-column, role-playing dates
        # (invoices.date vs invoices.due_date) each get their own edge — the metric
        # basis (booking vs cash) is choosable. profile_id is the per-run local edge
        # key. The JOIN to current_columns resolves the edge's source table_id (the
        # profile row carries only column_id) and drops a profile whose column is not
        # on the analyzed-representative surface (never dangle).
        return (
            f"CREATE VIEW {READ_TOKEN}.og_temporal_coverage AS\n"
            f"SELECT tcp.profile_id::text AS profile_id,\n"
            f"       c.table_id::text AS table_id, tcp.column_id::text AS column_id,\n"
            f"       c.column_name, tcp.min_timestamp, tcp.max_timestamp,\n"
            f"       tcp.detected_granularity, tcp.completeness_ratio, tcp.last_period_complete\n"
            f"FROM {READ_TOKEN}.current_temporal_column_profiles tcp\n"
            f"JOIN {READ_TOKEN}.current_columns c ON c.column_id = tcp.column_id;"
        )
    if name == "og_measure_time_axis":
        # measure_time_axis edge (measure-column → time-column, DAT-730): every
        # measure's time axes, with exactly one designated ANCHOR. A measure's axes
        # are its OWN fact's ``time_columns`` (the run-scoped, LLM-named event dates,
        # DAT-565) — a measure and a time axis conform iff they sit on the same table
        # (mc.table_id = tc.table_id). ``is_anchor`` marks the fact's PRIMARY axis
        # (the first ``time_columns`` entry, ordinality 1) — the default trend/window
        # axis; the non-anchor rows are the enumerable alternates, each carrying its
        # ``role`` (the aspect: order/ship/due/paid). The time-column name is resolved
        # to its column_id through current_columns so the edge binds column→column;
        # an unresolved axis name simply drops (never dangles). edge_key = the ordered
        # (measure, time) column-id pair, '_'-joined (NOT ':' — a bind-param sigil).
        return (
            f"CREATE VIEW {READ_TOKEN}.og_measure_time_axis AS\n"
            f"SELECT (mc.column_id || '_' || tc.column_id)::text AS edge_key,\n"
            f"       mc.column_id::text AS measure_column_id,\n"
            f"       tc.column_id::text AS time_column_id,\n"
            f"       tc.role, (tc.ord = 1) AS is_anchor\n"
            f"FROM (\n"
            f"  SELECT c.column_id, c.table_id\n"
            f"  FROM {READ_TOKEN}.current_columns c\n"
            f"  JOIN {READ_TOKEN}.current_semantic_annotations sa ON sa.column_id = c.column_id\n"
            f"  WHERE sa.semantic_role = 'measure'\n"
            f") mc\n"
            f"JOIN (\n"
            f"  SELECT DISTINCT ON (tcol.column_id)\n"
            f"         te.table_id, tcol.column_id, ax.value->>'aspect' AS role, ax.ord AS ord\n"
            f"  FROM {READ_TOKEN}.current_table_entities te\n"
            f"  CROSS JOIN LATERAL json_array_elements(\n"
            f"       COALESCE(te.time_columns, '[]'::json)) WITH ORDINALITY AS ax(value, ord)\n"
            f"  JOIN {READ_TOKEN}.current_columns tcol\n"
            f"    ON tcol.table_id = te.table_id AND tcol.column_name = ax.value->>'column'\n"
            # A malformed time_columns array could name one physical column twice
            # (two aspects); keep only its lowest-ordinality entry so each (measure,
            # time-column) pair yields exactly ONE edge — a non-unique edge KEY would
            # make the PGQ binding ambiguous.
            f"  ORDER BY tcol.column_id, ax.ord\n"
            f") tc ON tc.table_id = mc.table_id;"
        )
    if name == "og_rolls_up_to":
        # rolls_up_to edge (column → column, DAT-730 — moved here from P1): a
        # dimension-level roll-up. dimension_hierarchies stores a drill-down chain's
        # levels in ``members`` finest→coarsest (JSON); this unnests them into
        # consecutive level→level edges (member i → member i+1), so a MATCH walks one
        # direct level link and the recursive-CTE closure walks the whole drill path
        # (the bounded-depth + cycle-guard mechanism, like part_of). Only
        # ``kind='drilldown'`` rolls up — ``alias`` (equivalent axes) and ``role``
        # (role-playing pairs) are not level ladders. Each member's ``column_id`` is
        # resolved through current_columns so the edge binds column→column and an
        # enriched-only member (no base column vertex) drops rather than dangles.
        # edge_key = hierarchy_id + the finer level's ordinality, '_'-joined.
        return (
            f"CREATE VIEW {READ_TOKEN}.og_rolls_up_to AS\n"
            f"SELECT (dh.hierarchy_id || '_' || lo.ord::text)::text AS edge_key,\n"
            f"       cf.column_id::text AS from_column_id, ct.column_id::text AS to_column_id,\n"
            f"       lo.value->>'column_name' AS from_level,\n"
            f"       hi.value->>'column_name' AS to_level, dh.canonical_label\n"
            f"FROM {READ_TOKEN}.current_dimension_hierarchies dh\n"
            f"CROSS JOIN LATERAL json_array_elements(dh.members) WITH ORDINALITY AS lo(value, ord)\n"
            f"CROSS JOIN LATERAL json_array_elements(dh.members) WITH ORDINALITY AS hi(value, ord)\n"
            f"JOIN {READ_TOKEN}.current_columns cf ON cf.column_id = lo.value->>'column_id'\n"
            f"JOIN {READ_TOKEN}.current_columns ct ON ct.column_id = hi.value->>'column_id'\n"
            f"WHERE dh.kind = 'drilldown' AND hi.ord = lo.ord + 1;"
        )
    raise AssertionError(f"unreachable: {name} not an element view")


def _aux_view_sql(name: str) -> str:
    """The tokenized body for one auxiliary (non-PGQ-bound) calendar view (DAT-730).

    These ride the graph lifecycle (dropped before / created after the ``current_*``
    refresh) but are NOT bound into the property graph — they carry no vertex identity
    a MATCH needs. ``og_calendar`` is the workspace calendar; ``og_period_grain`` is
    the roll-up ladder the recursive-CTE closure walks (AC5).
    """
    if name == "og_calendar":
        # og_calendar: the workspace calendar as one derived row. The observed window
        # is the min/max across every profiled time column; ``base_grain`` is the
        # FINEST detected grain (DERIVED from detected_granularity, not the config
        # constant the lineage witness used — DD/49938435 Phase C). fiscal_year_start
        # is the month AFTER the most-common detected fiscal-year end (per-column
        # fiscal detection), defaulting to 1 (calendar year) when none was detected.
        # profile_data is json (not jsonb) — ``->/->>'`` operate on it directly.
        def grain_rank(col: str) -> str:
            return (
                f"CASE {col} WHEN 'second' THEN 1 WHEN 'minute' THEN 2 WHEN 'hour' THEN 3 "
                f"WHEN 'day' THEN 4 WHEN 'week' THEN 5 WHEN 'month' THEN 6 "
                f"WHEN 'quarter' THEN 7 WHEN 'year' THEN 8 ELSE 99 END"
            )

        return (
            f"CREATE VIEW {READ_TOKEN}.og_calendar AS\n"
            f"SELECT 'workspace'::text AS calendar_id,\n"
            f"       MIN(tcp.min_timestamp) AS window_start,\n"
            f"       MAX(tcp.max_timestamp) AS window_end,\n"
            f"       (SELECT g.detected_granularity\n"
            f"        FROM {READ_TOKEN}.current_temporal_column_profiles g\n"
            f"        ORDER BY {grain_rank('g.detected_granularity')}\n"
            f"        LIMIT 1) AS base_grain,\n"
            f"       COALESCE(\n"
            f"         (SELECT (CAST(f.profile_data->'fiscal_calendar'->>'fiscal_year_end_month' AS INT)\n"
            f"                  % 12) + 1\n"
            f"          FROM {READ_TOKEN}.current_temporal_column_profiles f\n"
            f"          WHERE (f.profile_data->'fiscal_calendar'->>'fiscal_alignment_detected') = 'true'\n"
            f"            AND f.profile_data->'fiscal_calendar'->>'fiscal_year_end_month' IS NOT NULL\n"
            f"          GROUP BY f.profile_data->'fiscal_calendar'->>'fiscal_year_end_month'\n"
            f"          ORDER BY COUNT(*) DESC\n"
            f"          LIMIT 1), 1) AS fiscal_year_start_month\n"
            f"FROM {READ_TOKEN}.current_temporal_column_profiles tcp;"
        )
    if name == "og_period_grain":
        # og_period_grain: the fixed period-grain roll-up ladder day→month→quarter→
        # year (plus the sub-day exact-nesting steps). Each row is one direct parent
        # link; the recursive-CTE closure walks the chain to derive "last complete
        # quarter" from "last complete month". Week is deliberately absent — weeks
        # cross month boundaries, so a week does not nest cleanly in a month.
        return (
            f"CREATE VIEW {READ_TOKEN}.og_period_grain AS\n"
            f"SELECT from_grain, to_grain FROM (VALUES\n"
            f"  ('second', 'minute'), ('minute', 'hour'), ('hour', 'day'),\n"
            f"  ('day', 'month'), ('month', 'quarter'), ('quarter', 'year')\n"
            f") AS ladder(from_grain, to_grain);"
        )
    raise AssertionError(f"unreachable: {name} not an aux view")


def _property_graph_sql() -> str:
    """The tokenized ``CREATE PROPERTY GRAPH`` binding the element views.

    Vertex ``KEY`` clauses are explicit (a view has no primary key); edge
    ``SOURCE/DESTINATION KEY ... REFERENCES`` name the vertex element table (no
    schema qualifier — the reference resolves within the graph definition). The
    references edge is labelled ``refs``, not ``references`` — ``REFERENCES`` is a
    reserved keyword in the PGQ edge grammar (``SOURCE KEY ... REFERENCES ...``),
    so ``LABEL references`` is a syntax error.
    """
    return (
        f"CREATE PROPERTY GRAPH {READ_TOKEN}.{PROPERTY_GRAPH_NAME}\n"
        f"  VERTEX TABLES (\n"
        f"    {READ_TOKEN}.og_tables KEY (table_id) LABEL table_node\n"
        f"      PROPERTIES (table_id, table_name, layer, table_role, detected_entity_type),\n"
        f"    {READ_TOKEN}.og_columns KEY (column_id) LABEL column_node\n"
        f"      PROPERTIES (column_id, table_id, column_name, semantic_role, materialization),\n"
        f"    {READ_TOKEN}.og_concepts KEY (concept_id) LABEL concept_node\n"
        f"      PROPERTIES (concept_id, vertical, name, kind, dimension_order)\n"
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
        f"      PROPERTIES (column_name, slice_type, slice_priority,\n"
        f"                  dimension_table_id, dimension_attribute, fk_role),\n"
        f"    {READ_TOKEN}.og_derived_from KEY (edge_key)\n"
        f"      SOURCE KEY (view_table_id) REFERENCES og_tables (table_id)\n"
        f"      DESTINATION KEY (base_table_id) REFERENCES og_tables (table_id)\n"
        f"      LABEL derived_from\n"
        f"      PROPERTIES (base_role),\n"
        f"    {READ_TOKEN}.og_concept_edges KEY (edge_id)\n"
        f"      SOURCE KEY (from_concept_id) REFERENCES og_concepts (concept_id)\n"
        f"      DESTINATION KEY (to_concept_id) REFERENCES og_concepts (concept_id)\n"
        f"      LABEL concept_edge\n"
        f"      PROPERTIES (predicate, tolerance),\n"
        f"    {READ_TOKEN}.og_conformed_dimension KEY (edge_key)\n"
        f"      SOURCE KEY (from_table_id) REFERENCES og_tables (table_id)\n"
        f"      DESTINATION KEY (to_table_id) REFERENCES og_tables (table_id)\n"
        f"      LABEL conformed_dimension\n"
        f"      PROPERTIES (dimension_table_id, dimension_attribute),\n"
        f"    {READ_TOKEN}.og_temporal_coverage KEY (profile_id)\n"
        f"      SOURCE KEY (table_id) REFERENCES og_tables (table_id)\n"
        f"      DESTINATION KEY (column_id) REFERENCES og_columns (column_id)\n"
        f"      LABEL temporal_coverage\n"
        f"      PROPERTIES (column_name, min_timestamp, max_timestamp,\n"
        f"                  detected_granularity, completeness_ratio, last_period_complete),\n"
        f"    {READ_TOKEN}.og_measure_time_axis KEY (edge_key)\n"
        f"      SOURCE KEY (measure_column_id) REFERENCES og_columns (column_id)\n"
        f"      DESTINATION KEY (time_column_id) REFERENCES og_columns (column_id)\n"
        f"      LABEL measure_time_axis\n"
        f"      PROPERTIES (role, is_anchor),\n"
        f"    {READ_TOKEN}.og_rolls_up_to KEY (edge_key)\n"
        f"      SOURCE KEY (from_column_id) REFERENCES og_columns (column_id)\n"
        f"      DESTINATION KEY (to_column_id) REFERENCES og_columns (column_id)\n"
        f"      LABEL rolls_up_to\n"
        f"      PROPERTIES (from_level, to_level, canonical_label)\n"
        f"  );"
    )


def graph_statements() -> list[tuple[str, str]]:
    """Deterministic ``(name, tokenized DDL)`` list: element views, aux views, graph.

    The bound element views come first, then the non-bound auxiliary calendar views
    (DAT-730), then ``CREATE PROPERTY GRAPH`` last (it depends on the element views;
    the aux views depend only on the ``current_*`` surface, so their position among
    the views is free — they sit before the graph for a stable dump).
    """
    statements: list[tuple[str, str]] = [(name, _element_view_sql(name)) for name in _ELEMENT_VIEWS]
    statements.extend((name, _aux_view_sql(name)) for name in _AUX_VIEWS)
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
        f"DROP VIEW IF EXISTS {READ_TOKEN}.{name};" for name in _ALL_VIEWS
    )
    bodies = "\n\n".join(sql for _, sql in graph_statements())
    return header + "\n" + drops + "\n\n" + bodies + "\n"


def drop_property_graph(connection: Connection, workspace_schema: str) -> None:
    """Tear down the graph + its element views (idempotent), in dependency order.

    Runs BEFORE ``materialize_read_schema`` on every boot: that refresh drops+
    recreates the ``current_*`` views the element views depend on, and Postgres
    refuses to drop a view while a dependent (element view / graph) exists. The
    graph goes first (it depends on the element views), then all element + aux
    views (the aux calendar views depend only on ``current_*``). Postgres-only.
    """
    read_schema = read_schema_name_for(workspace_schema)
    connection.execute(text(f'DROP PROPERTY GRAPH IF EXISTS "{read_schema}".{PROPERTY_GRAPH_NAME}'))
    for name in _ALL_VIEWS:
        connection.execute(text(f'DROP VIEW IF EXISTS "{read_schema}".{name}'))


def materialize_property_graph(connection: Connection, workspace_schema: str) -> int:
    """Create the element views + the property graph for one workspace (idempotent).

    Runs AFTER ``materialize_read_schema`` (the graph binds the freshly-created
    ``current_*`` views through the element views). Self-contained: it re-drops the
    graph + views first so a direct call (tests) needs no preceding
    :func:`drop_property_graph`. Postgres-only; callers guard on dialect.

    Returns:
        Number of statements applied (element views + aux views + the graph).
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


def grant_reader_on_graph(connection: Connection, workspace_schema: str) -> None:
    """Grant the ``cockpit_reader`` role SELECT on the property graph (ADR-0008).

    A property graph is a distinct privilege-checked object: ``GRANT SELECT ON ALL
    TABLES`` (what ``ensure_reader_role`` does for the ``og_*`` views) does NOT cover
    ``GRAPH_TABLE`` access — without this the reader can plain-SELECT the element
    views but the one query form the graph exists for is ``permission denied``. The
    graph is dropped+recreated every boot, so the grant is re-applied here every
    boot too. Runs AFTER ``ensure_reader_role`` (which creates the role) and after
    the graph is (re)created. Postgres-only.
    """
    read_schema = read_schema_name_for(workspace_schema)
    connection.execute(
        text(
            f'GRANT SELECT ON PROPERTY GRAPH "{read_schema}".{PROPERTY_GRAPH_NAME} TO {READER_ROLE}'
        )
    )
