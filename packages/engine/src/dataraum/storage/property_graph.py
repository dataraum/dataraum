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

**Scope (DAT-726 topology, the concept vocabulary, and the DAT-727 grounding
reification).** Vertices/edges:

    column_node    (KEY column_id)   props: semantic_role (has_role),
                                            materialization (materializes_as),
                                            anchor_time_axis (witness axis ▸ declared anchor)
    table_node     (KEY table_id)    props: layer (typed | enriched), table_role
                                            (fact/periodic_snapshot/dimension)
    concept_node   (KEY concept_id)  the typed vocabulary (DAT-728); ``ordering``
                                            (ordered | nominal | NULL) types a
                                            dimension concept's axis (DAT-730)
    grounding_node (KEY snippet_id)  the reified grounding commitment (DAT-727):
                                            concept / relation / select_expr /
                                            where_predicates un-nested from the
                                            extract snippet's clause parts
                                            (DAT-671) via current_groundings;
                                            ``failed`` discriminates a retained
                                            DAT-543 failure from knowledge
    period_grain   (KEY grain)       the constant period ladder node (day/month/
                                            quarter/year) carrying the workspace's
                                            declared fiscal boundary (DAT-730)
    additivity_verdict (KEY additivity_id) the 2-axis drill additivity verdict
                                            (DAT-731) projected from
                                            current_metric_additivity: categorical /
                                            time additive + reason, per drill target
                                            (a ``metric`` graph_id or a ``measure``
                                            standard_field)
    refs               table → table     [relationships]      FK topology (conformed dims excluded)
    has_dimension      table → column    [slice_definitions]  a fact's slice cols + dim identity
    derived_from       table → table     [enriched_views]     view → fact + dim bases
    concept_edge       concept → concept [concept_edges]      part_of/disjoint/reconciles
    conformed_dimension table → table    [slice_definitions]  two facts sharing a dimension (DAT-756)
    grounded_by        concept → grounding [current_groundings] a concept's groundings; >1 healthy = multi-grounding
    uses               grounding → column  [provenance contract v2] the columns a grounding touches
    temporal_coverage  table → column    [temporal_column_profiles ▸ time_columns] observed window/grain/completeness per (relation × time col) (DAT-730)
    rolls_up_to        column → column   [dimension_hierarchies] ordered drill level→level, finer→coarser (DAT-730)
    period_rolls_up_to grain → grain     [constant + workspace_calendar] the calendar ladder day→month→quarter→year (DAT-730)
    has_additivity     concept → additivity_verdict [current_metric_additivity] a MEASURE concept's 2-axis verdict (DAT-731)
    measured_in        column → column   [column_concepts.unit_source_column] a measure column → the column that defines its unit (DAT-731)

One vertex label spans both layers (DAT-774): typed source tables AND enriched-view
tables are ``table_node``, discriminated by the ``layer`` property (the DD types "Table
… incl. enriched views" as one label). Before DAT-774 ``og_tables`` was typed-only, so
every ``derived_from`` edge — whose source is always an enriched-view table — dangled at
its source endpoint and none ever instantiated in a MATCH. See ``og_tables`` below.

``rolls_up_to`` (DAT-730) unnests ``dimension_hierarchies.members`` into ordered
level→level column edges (``HierarchyMember.level`` is the SOLE direction carrier —
level 0 coarsest, DAT-779 — never array position); the parallel ``period_rolls_up_to``
constant ladder (day→month→quarter→year, fiscal boundaries from the declared
``workspace_calendar``) is walkable by the SAME bounded recursive CTE the ``part_of``
closure uses, so last-complete-quarter is derivable from last-complete-month.
``temporal_coverage`` (DAT-730) exposes the persisted ``temporal_column_profiles``
coverage per DECLARED time column (``table_entities.time_columns`` is the role home —
event/attribute + single anchor); observed grain is ``detected_granularity`` (NEVER the
``period_grain`` config echo), and an unmeasurable window stays NULL, never fabricated.
The ``concept_edge`` edge (DAT-729) carries the vocabulary relations
``part_of`` / ``disjoint_with`` / ``reconciles_with`` as a ``predicate`` property;
its transitive closure (``part_of`` ancestry) is walked by the bounded recursive CTE.
The ``uses`` edge un-nests the TYPED ``column_mappings_basis`` (provenance contract
v2, DAT-727) — enforced at authoring, never parsed out of SQL; ``filtered_by →
DimMember`` is deferred to DAT-787, with ``where[]`` carried losslessly on the
grounding vertex meanwhile.
``conformed_dimension`` (DAT-756) types two facts sharing a dimension AXIS — the same
resolved ``(dimension_table_id, attribute)`` identity, NOT a column name — as a
drill-across path (an alignable GROUP BY the SQL agents can author over). It is
ATTRIBUTE grain (the actionable unit for SQL is a shared axis, not a shared table),
deliberately DECOUPLED from the table-grain ``refs`` fan-trap exclusion below — the two
serve different consumers, so a cross-level fan trap is excluded from ``refs`` yet
correctly has no conformed edge (see the edge's own note).

**Additivity + units (DAT-731).** ``additivity_verdict`` is a small VERTEX projecting
``current_metric_additivity`` — the deterministic 2-axis drill verdict (categorical /
time additive + reason) the metrics phase persists per drill target (a ``metric``
graph_id or a ``measure`` standard_field). A vertex (not a property on an existing
node) because the two target kinds have NO common home: a ``measure`` target_key is a
concept name, but a ``metric`` target_key is a formula ``graph_id`` with no vertex at
all (metrics are not in the graph). One uniform vertex covers both and stays MATCH-able
by property for the metric case; ``has_additivity`` (concept → verdict) makes the
MEASURE case a traversal — ``MATCH (c:concept_node)-[:has_additivity]->(a)`` answers
"can I sum concept X over time?". ``measured_in`` (column → column) projects the
semantic-layer authored ``ColumnConcept.unit_source_column`` — a measure column → the
column that defines its unit (a sibling ``currency`` dimension, or a ``table.column``
across a confirmed FK, or the measure's OWN column for a self-denominated value). The
authored pointer is the ONE home for a measure's unit: the typing-layer Pint
``detected_unit`` (analysis/typing/units.py) is EVIDENCE the catalogue author already
sees, never a second edge source — the two are not fused. ``dimensionless`` / NULL /
'' carry no edge (no unit column to point at). Together they make a cross-unit
aggregation REPRESENTABLE (a measure ``measured_in`` a multi-valued unit column cannot
be summed raw without a conversion) — the conversion GROUNDING itself (an fx-rate
table) is not a clean structural edge today and is left to the consumer's flag.

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
    WS_TOKEN,
    read_schema_name_for,
    reader_role_for,
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
    "og_grounding",
    "og_period_grain",
    "og_references",
    "og_has_dimension",
    "og_derived_from",
    "og_concept_edges",
    "og_conformed_dimension",
    "og_grounded_by",
    "og_uses",
    "og_temporal_coverage",
    "og_rolls_up_to",
    "og_period_rolls_up_to",
    "og_additivity",
    "og_has_additivity",
    "og_measured_in",
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
        # Table vertex: BOTH typed source tables AND enriched-view tables (DAT-774).
        # The DD's type system declares "Table (physical relation, incl. enriched
        # views)" as ONE vertex label, so both layers bind to table_node with ``layer``
        # ('typed' | 'enriched') the discriminating property — a consumer meaning
        # "source tables only" filters ``WHERE layer = 'typed'``; a SECOND vertex label
        # would contradict the declared typing (the DAT-774 tiebreaker).
        #
        # Typed branch: the analyzed-representative typed table + its role (table_role:
        # fact / periodic_snapshot / dimension). current_table_entities is (table_id,
        # run) unique post-head, so the LEFT JOIN stays 1:1 and table_id is a valid KEY.
        #
        # Enriched branch (the DAT-774 fix): the enriched-view tables. Sourced from
        # current_enriched_views — NOT current_tables, which is hard-filtered
        # ``layer='typed'`` (read_views.py, DAT-655), so enriched tables never surfaced
        # and EVERY og_derived_from edge dangled at its source endpoint: no derived_from
        # edge had ever instantiated. Head-resolution stays intact under the two-head
        # model — a typed vertex is current under its (table:{id}, generation) head; an
        # enriched vertex is current under the begin_session (catalog) enriched_views
        # head, the SAME head og_derived_from reads — so every derived_from SOURCE
        # endpoint now resolves BY CONSTRUCTION (both views read current_enriched_views).
        # ``view_name`` IS the enriched Table's table_name (enriched_views_phase sets
        # both to ``enriched_{fact}``); layer is the constant 'enriched'; table_role /
        # detected_entity_type are NULL (an enriched view is a derived relation, never a
        # detected entity). ``WHERE view_table_id IS NOT NULL`` drops an un-materialized
        # view (no vertex), mirroring the edge's own guard. table_id is uuid4-unique
        # across both branches, so the union KEY stays valid.
        return (
            f"CREATE VIEW {READ_TOKEN}.og_tables AS\n"
            f"SELECT t.table_id::text AS table_id, t.table_name, t.layer,\n"
            f"       te.table_role, te.detected_entity_type\n"
            f"FROM {READ_TOKEN}.current_tables t\n"
            f"LEFT JOIN {READ_TOKEN}.current_table_entities te ON te.table_id = t.table_id\n"
            f"UNION ALL\n"
            f"SELECT ev.view_table_id::text AS table_id, ev.view_name AS table_name,\n"
            f"       'enriched' AS layer, NULL AS table_role, NULL AS detected_entity_type\n"
            f"FROM {READ_TOKEN}.current_enriched_views ev\n"
            f"WHERE ev.view_table_id IS NOT NULL;"
        )
    if name == "og_columns":
        # Column vertex: the physical column with its semantic role (has_role),
        # materialization (materializes_as → flow | stock), and the anchor event-time
        # axis a measure trends by (anchor_time_axis). The two materialization source
        # columns carry DIFFERENT raw vocabularies and must be normalized, NOT
        # COALESCEd raw:
        #   measure_aggregation_lineage.pattern ∈ {per_period, cumulative}  (DAT-491
        #     witness posterior — per_period ⇒ flow, cumulative ⇒ stock),
        #   column_concepts.temporal_behavior ∈ {additive, point_in_time}   (ontology
        #     prior — the canonical drivers map: additive ⇒ flow, point_in_time ⇒ stock).
        # Prefer the data-reconciled witness posterior over the prior claim; NULL when
        # neither is present. Each LEFT-joined table is (column_id, run) unique after
        # head resolution, so the join is 1:1 and column_id is a valid KEY.
        #
        # anchor_time_axis — THE anchor event-time axis for this column, the ONE
        # documented home of the DAT-780 witness-precedence rule (replaces the parked
        # #486's positional `tc.ord = 1` pick; nothing reads array position). Computed
        # for EVERY column vertex but only meaningful for a MEASURE (its trend axis) —
        # a non-measure resolves to its table's declared anchor, harmlessly unread:
        #   1. the DAT-778 lineage-witness event-side axis (mal.event_time_axis_column)
        #      where a witness reconciled this measure — the data-proven rollup axis;
        #   2. else the table's DECLARED anchor — the one time_columns entry the LLM
        #      committed with role='event' AND is_anchor=true (the typed field, NOT
        #      list position). Read from current_table_entities' JSON interior via a
        #      lateral; the save-time contract guarantees at most one such row.
        # NULL when neither exists. The COALESCE order IS the precedence, exactly like
        # materialization prefers the witness posterior over the concept prior.
        #
        # DAT-811 — the vertex set is the UNION of two branches:
        #   TYPED    (current_columns): a column resolves its own semantics by its own
        #     column_id, and the declared anchor comes from its own table's entity.
        #   ENRICHED (current_enriched_columns): a served column of an enriched view. It
        #     keeps its OWN column_id (the KEY must stay unique — a typed id must never
        #     appear on two vertices), but resolves EVERY semantic (role, materialization,
        #     anchor) THROUGH ``source_column_id`` — its typed source. So the enriched view
        #     is self-describing: a MATCH over its table_id returns its full column set with
        #     semantics attached, no walk back to origin tables. ``te`` joins the SOURCE
        #     column's table so an f.* measure inherits the fact's declared anchor.
        return (
            f"CREATE VIEW {READ_TOKEN}.og_columns AS\n"
            f"SELECT c.column_id::text AS column_id, c.table_id::text AS table_id, c.column_name,\n"
            f"       sa.semantic_role,\n"
            f"       COALESCE(\n"
            f"         CASE mal.pattern WHEN 'per_period' THEN 'flow' WHEN 'cumulative' THEN 'stock' END,\n"
            f"         CASE cc.temporal_behavior WHEN 'additive' THEN 'flow'\n"
            f"                                   WHEN 'point_in_time' THEN 'stock' END\n"
            f"       ) AS materialization,\n"
            f"       COALESCE(mal.event_time_axis_column, declared_anchor.column_name) AS anchor_time_axis\n"
            f"FROM {READ_TOKEN}.current_columns c\n"
            f"LEFT JOIN {READ_TOKEN}.current_semantic_annotations sa ON sa.column_id = c.column_id\n"
            f"LEFT JOIN {READ_TOKEN}.current_column_concepts cc ON cc.column_id = c.column_id\n"
            f"LEFT JOIN {READ_TOKEN}.current_measure_aggregation_lineage mal\n"
            f"       ON mal.measure_column_id = c.column_id\n"
            f"LEFT JOIN {READ_TOKEN}.current_table_entities te ON te.table_id = c.table_id\n"
            f"LEFT JOIN LATERAL (\n"
            f"    SELECT elem->>'column' AS column_name\n"
            f"    FROM json_array_elements(COALESCE(te.time_columns, '[]'::json)) AS elem\n"
            f"    WHERE elem->>'role' = 'event' AND (elem->>'is_anchor')::boolean IS TRUE\n"
            f"    LIMIT 1\n"
            f"  ) declared_anchor ON TRUE\n"
            f"UNION ALL\n"
            f"SELECT ec.column_id::text AS column_id, ec.table_id::text AS table_id,"
            f" ec.column_name,\n"
            f"       sa.semantic_role,\n"
            f"       COALESCE(\n"
            f"         CASE mal.pattern WHEN 'per_period' THEN 'flow' WHEN 'cumulative' THEN 'stock' END,\n"
            f"         CASE cc.temporal_behavior WHEN 'additive' THEN 'flow'\n"
            f"                                   WHEN 'point_in_time' THEN 'stock' END\n"
            f"       ) AS materialization,\n"
            f"       COALESCE(mal.event_time_axis_column, declared_anchor.column_name) AS anchor_time_axis\n"
            f"FROM {READ_TOKEN}.current_enriched_columns ec\n"
            f"LEFT JOIN {READ_TOKEN}.current_semantic_annotations sa ON sa.column_id = ec.source_column_id\n"
            f"LEFT JOIN {READ_TOKEN}.current_column_concepts cc ON cc.column_id = ec.source_column_id\n"
            f"LEFT JOIN {READ_TOKEN}.current_measure_aggregation_lineage mal\n"
            f"       ON mal.measure_column_id = ec.source_column_id\n"
            f"LEFT JOIN {READ_TOKEN}.current_columns src ON src.column_id = ec.source_column_id\n"
            f"LEFT JOIN {READ_TOKEN}.current_table_entities te ON te.table_id = src.table_id\n"
            f"LEFT JOIN LATERAL (\n"
            f"    SELECT elem->>'column' AS column_name\n"
            f"    FROM json_array_elements(COALESCE(te.time_columns, '[]'::json)) AS elem\n"
            f"    WHERE elem->>'role' = 'event' AND (elem->>'is_anchor')::boolean IS TRUE\n"
            f"    LIMIT 1\n"
            f"  ) declared_anchor ON TRUE;"
        )
    if name == "og_concepts":
        # Concept vertex: the workspace's typed vocabulary (DAT-728). Active rows
        # only (superseded_at IS NULL) — the partial-unique index makes concept_id
        # unique among them, so it is a valid KEY. Its vocabulary edges are the
        # `concept_edge` edge below (DAT-729) and `grounded_by` (DAT-727).
        return (
            f"CREATE VIEW {READ_TOKEN}.og_concepts AS\n"
            f"SELECT concept_id::text AS concept_id, vertical, name, kind, ordering\n"
            f"FROM {READ_TOKEN}.concepts\n"
            f"WHERE superseded_at IS NULL;"
        )
    if name == "og_grounding":
        # Grounding vertex (DAT-727): one node per graph-authored extract snippet —
        # the reified N-ary grounding commitment (concept + relation + filter +
        # select_expr; the DD's "Grounding is a NODE, not an edge"). A thin ::text
        # projection of `current_groundings` (read_views.py), which owns membership
        # (snippet_type='extract' AND source LIKE 'graph:%' — the cockpit's query:%
        # rows share the table and must never become groundings) and the parts
        # un-nest. Retained DAT-543 failures ARE vertices, discriminated by the
        # `failed` property — "why is this concept ungrounded?" is a graph question
        # — but only healthy nodes get `uses` edges (a failed row's provenance
        # carries failure keys, no basis). where_predicates is the where[] JSON
        # carried LOSSLESSLY on the node; its DimMember decomposition (filtered_by)
        # is DAT-787. sql_snippets is workspace-persistent, so snippet_id is a
        # valid KEY as-is. Parity: relation/select_expr/where re-render through
        # compose_extract_sql to exactly the snippet's persisted sql (tested).
        return (
            f"CREATE VIEW {READ_TOKEN}.og_grounding AS\n"
            f"SELECT g.snippet_id::text AS snippet_id,\n"
            f"       g.concept, g.statement, g.aggregation, g.description,\n"
            f"       g.relation, g.select_expr, g.where_predicates, g.failed\n"
            f"FROM {READ_TOKEN}.current_groundings g;"
        )
    if name == "og_concept_edges":
        # concept_edge (concept → concept): the vocabulary relations part_of /
        # disjoint_with / reconciles_with (DAT-729), predicate carried as a property.
        # concept_edges stores endpoints by the stable (vertical, name) key — the
        # concept identity contract — so each endpoint JOINs to its ACTIVE concept to resolve
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
        # refs edge (table → table): the detected REFERENCE topology. relationship_id
        # is per-run and unique within one head-resolved view — a fine LOCAL edge key
        # inside one promoted state (never keyed on across runs). Self-referential
        # and multi-hop chains here are what the recursive-CTE closure walks.
        #
        # Membership consumes the edge-kind owner (DAT-850): the row's
        # relationship_type, resolved at the write site (oriented_row) and enforced
        # by ck_relationships_reference_not_many_to_many, so a conformed-dimension
        # meeting (two facts at a shared axis, the DAT-723 fan trap) is typed on the
        # row and simply not a reference kind here. This replaces the DAT-756
        # slice-identity NOT-EXISTS — the same fact re-derived from a second basis;
        # the shared-axis reading of a pair stays first-class as the
        # og_conformed_dimension edge below. Defined catalog only: the same
        # detection_method != 'candidate' contract every downstream stage reads
        # (structural candidates and judge-DECLINED rows share the catalog head's
        # run_id and used to leak into this view as FK edges). NULL-method rows
        # drop here too, deliberately (SQL three-valued !=) — same semantics as
        # load_defined_relationships; no writer produces NULL.
        return (
            f"CREATE VIEW {READ_TOKEN}.og_references AS\n"
            f"SELECT relationship_id::text AS relationship_id,\n"
            f"       from_table_id::text AS from_table_id, to_table_id::text AS to_table_id,\n"
            f"       from_column_id, to_column_id, cardinality, relationship_type,\n"
            f"       confidence, confirmation_source\n"
            f"FROM {READ_TOKEN}.current_relationships r\n"
            f"WHERE r.relationship_type IN ('foreign_key', 'hierarchy')\n"
            f"  AND r.detection_method != 'candidate';"
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
        # The dimension branch is SELECT DISTINCT (DAT-774): dimension_table_ids is a
        # JSON array, and a DUPLICATE id in it would unnest to two rows sharing one
        # edge_key (view_id || '_dim_' || id) — a non-unique PGQ edge KEY. DISTINCT over
        # the projected columns dedups on (view_id, id), so the key is genuinely unique
        # regardless of the JSON's contents. (The writer already set-dedups the ids, but
        # the KEY invariant must hold on the view, not on a producer's good behaviour.)
        return (
            f"CREATE VIEW {READ_TOKEN}.og_derived_from AS\n"
            f"SELECT (ev.view_id || '_fact')::text AS edge_key,\n"
            f"       ev.view_table_id::text AS view_table_id,\n"
            f"       ev.fact_table_id::text AS base_table_id, 'fact' AS base_role\n"
            f"FROM {READ_TOKEN}.current_enriched_views ev\n"
            f"WHERE ev.view_table_id IS NOT NULL\n"
            f"UNION ALL\n"
            f"SELECT DISTINCT (ev.view_id || '_dim_' || dt.value)::text AS edge_key,\n"
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
        # ATTRIBUTE grain, and deliberately a DIFFERENT question from the edge-kind
        # owner (DAT-850: a relationship row's relationship_type, resolved at the
        # write site) — this edge derives from the dimension-identity home
        # (SliceDefinition.dimension_table_id/attribute), not from relationship rows:
        #   - This edge exists to hand a SQL author an ALIGNABLE drill-across axis. Both
        #     agents (engine GraphAgent, cockpit answer agent) author SQL over COLUMNS,
        #     GROUP BY-ing slice columns — you cannot GROUP BY a table. The actionable
        #     unit is therefore "fact A's column and fact B's column are the SAME axis"
        #     = a shared (dim table, attribute); that is exactly the within-fact `alias`
        #     hierarchy ("region ≡ region_code") lifted across facts. So two facts
        #     conform iff they expose the same axis at the same level.
        #   - A relationship row typed 'conformed_dimension' (a measured m2m meeting)
        #     is dropped from og_references by its KIND; it gets an edge HERE only
        #     when slicing resolved the shared axis — an unresolved meeting has no
        #     axis to assert, and this edge only asserts the stronger, actionable
        #     "drill these across THIS axis."
        # A cross-level fan trap (fact-A-by-type ↔ fact-B-by-region) is thus typed out
        # of refs (correct — not a reference) AND has no conformed edge (correct — no
        # common axis to drill across); the facts' shared dimension is still visible
        # via their genuine fact→dim FKs. COALESCE pairs the slice-by-FK-key case (NULL
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
    if name == "og_grounded_by":
        # grounded_by edge (concept → grounding, DAT-727): a grounding's `concept`
        # IS a concept name (the snippet's standard_field), so the edge resolves it
        # to the ACTIVE concept row — the same (name, superseded_at IS NULL)
        # resolution og_concept_edges uses, and the same INNER-JOIN discipline
        # (the graph never dangles: a grounding whose concept names no active
        # Concept simply has no edge). The join is name-grain across verticals — a
        # workspace runs one vertical; if two actives ever shared a name, each
        # would carry the edge, which is the honest reading. FAILED groundings
        # keep their edge (the node's `failed` property discriminates), so a
        # concept's failed attempt is reachable from the concept. Multi-grounding
        # enumeration is first-class: a concept with >1 healthy grounding
        # (account_balance across trial_balance/balance_sheet) has >1 grounded_by
        # edge to a non-failed node.
        return (
            f"CREATE VIEW {READ_TOKEN}.og_grounded_by AS\n"
            f"SELECT (c.concept_id || '_' || g.snippet_id)::text AS edge_key,\n"
            f"       c.concept_id::text AS concept_id,\n"
            f"       g.snippet_id::text AS snippet_id,\n"
            f"       g.concept\n"
            f"FROM {READ_TOKEN}.current_groundings g\n"
            f"JOIN {READ_TOKEN}.concepts c\n"
            f"  ON c.name = g.concept AND c.superseded_at IS NULL;"
        )
    if name == "og_uses":
        # uses edge (grounding → column, DAT-727): un-nests the TYPED provenance
        # contract v2 — provenance.column_mappings_basis {concept:
        # {measure_columns[], filter_columns[], filter}}, ENFORCED against the
        # served relation schema at authoring (validate_grounding_basis + repair
        # turn), never recovered by parsing SQL. Pre-v2 rows lack the arrays and
        # yield no edges (clean cut, no backfill). Healthy groundings only — a
        # failed row's provenance carries failure keys, no basis (the WHERE is
        # belt-and-braces over that contract).
        #
        # Name → column_id resolution targets the SERVED relation's own columns
        # (DAT-811 — this recuts #487's base-table name-scatter, which double-
        # resolved and could mis-resolve a dim-attribute name collision):
        #   enriched relation — current_enriched_views(view_name) → the view's
        #     own served columns (current_enriched_columns), which ARE og_columns
        #     vertices with semantics resolved through source_column_id;
        #   typed relation (no enriched view of that name) — current_tables
        #     matched on table_name OR duckdb_path (the _build_schema_info
        #     fallback serves duckdb names) → current_columns.
        # The enumeration was validated against exactly this relation's served
        # schema at save, so the JOIN can only miss on a pre-v2/renamed row —
        # honest under-coverage, never a wrong edge.
        #
        # DISTINCT ON dedupes a column enumerated by several concepts/roles into
        # ONE edge per (snippet, column) — PGQ needs a unique edge KEY — keeping
        # the measure reading when roles collide ('measure' sorts before
        # 'filter'). role rides as the edge property. Consumer caveat: a
        # genuinely dual-role column (read AND filtered on, enumerated under
        # both lists) therefore surfaces as role='measure' only — a consumer
        # filtering strictly role='filter' can under-report it.
        return (
            f"CREATE VIEW {READ_TOKEN}.og_uses AS\n"
            f"SELECT DISTINCT ON (g.snippet_id, col.column_id)\n"
            f"       (g.snippet_id || '_' || col.column_id)::text AS edge_key,\n"
            f"       g.snippet_id::text AS snippet_id,\n"
            f"       col.column_id::text AS column_id,\n"
            f"       u.role\n"
            f"FROM {READ_TOKEN}.current_groundings g\n"
            f"CROSS JOIN LATERAL json_each(\n"
            f"     COALESCE(g.provenance->'column_mappings_basis', '{{}}'::json)\n"
            f"     ) AS b(concept, entry)\n"
            f"CROSS JOIN LATERAL (\n"
            f"  SELECT m.value AS column_name, 'measure' AS role\n"
            f"  FROM json_array_elements_text(\n"
            f"       COALESCE(b.entry->'measure_columns', '[]'::json)) m(value)\n"
            f"  UNION ALL\n"
            f"  SELECT f.value, 'filter'\n"
            f"  FROM json_array_elements_text(\n"
            f"       COALESCE(b.entry->'filter_columns', '[]'::json)) f(value)\n"
            f") u\n"
            f"CROSS JOIN LATERAL (\n"
            f"  SELECT ec.column_id\n"
            f"  FROM {READ_TOKEN}.current_enriched_views ev\n"
            f"  JOIN {READ_TOKEN}.current_enriched_columns ec\n"
            f"    ON ec.table_id = ev.view_table_id AND ec.column_name = u.column_name\n"
            f"  WHERE ev.view_name = g.relation\n"
            f"  UNION ALL\n"
            f"  SELECT tc.column_id\n"
            f"  FROM {READ_TOKEN}.current_tables t\n"
            f"  JOIN {READ_TOKEN}.current_columns tc\n"
            f"    ON tc.table_id = t.table_id AND tc.column_name = u.column_name\n"
            f"  WHERE (t.table_name = g.relation OR t.duckdb_path = g.relation)\n"
            f"    AND NOT EXISTS (\n"
            f"      SELECT 1 FROM {READ_TOKEN}.current_enriched_views ev2\n"
            f"      WHERE ev2.view_name = g.relation)\n"
            f") col\n"
            f"WHERE NOT g.failed\n"
            f"ORDER BY g.snippet_id, col.column_id,\n"
            f"         CASE u.role WHEN 'measure' THEN 0 ELSE 1 END;"
        )
    if name == "og_period_grain":
        # period_grain vertex (DAT-730): the CONSTANT period-roll-up ladder nodes
        # day/month/quarter/year, carrying the workspace's DECLARED fiscal-year start
        # month so a consumer can place fiscal-quarter/year boundaries. The nodes are
        # workspace-invariant; only ``fiscal_year_start_month`` varies, read from the
        # singleton ``workspace_calendar`` (the ``pin`` CHECK keeps it one row, so the
        # LEFT JOIN ON TRUE stays 1:1). Unset workspace ⇒ NO row ⇒ COALESCE to 1
        # (January = calendar year) AND ``calendar_source='default'`` — the STAMPED,
        # visible default (never a silent fallback), the structural analogue of
        # ``period_resolver``'s flagged fallback. ``ordinal`` orders the ladder
        # (day=0 finest .. year=3 coarsest) for a consumer that reads the nodes flat.
        return (
            f"CREATE VIEW {READ_TOKEN}.og_period_grain AS\n"
            f"SELECT g.grain::text AS grain, g.ordinal,\n"
            f"       COALESCE(wc.fiscal_year_start_month, 1) AS fiscal_year_start_month,\n"
            f"       CASE WHEN wc.fiscal_year_start_month IS NULL THEN 'default'\n"
            f"            ELSE 'declared' END AS calendar_source\n"
            f"FROM (VALUES ('day', 0), ('month', 1), ('quarter', 2), ('year', 3))\n"
            f"       AS g(grain, ordinal)\n"
            f"LEFT JOIN {READ_TOKEN}.workspace_calendar wc ON TRUE;"
        )
    if name == "og_temporal_coverage":
        # temporal_coverage edge (table → column, DAT-730): one edge per (typed
        # relation × DECLARED time column) exposing the PERSISTED
        # ``temporal_column_profiles`` coverage as PGQ-queryable structure — parallel
        # to og_has_dimension (table → column). The ROLE home is
        # ``table_entities.time_columns`` (the event/attribute + single-anchor JSON,
        # enforced at the LLM seam): unnested to name each declared time column with
        # its ``role`` / ``aspect`` / ``declared_anchor``. The name resolves to its
        # ``column_id`` in current_columns (INNER — a graph edge needs a real column
        # vertex; an unresolvable name is honest under-coverage, no dangling edge, the
        # og_concept_edges discipline). The observed facts LEFT-join
        # current_temporal_column_profiles: a declared time column that was never
        # temporally profiled (or an irregular/unknown grain) keeps its edge with NULL
        # observed_* — absence falls loud, NEVER a fabricated window (DAT-853
        # abstention discipline; completeness_ratio/last_period_complete already NULL
        # at the source). The OBSERVED grain is ``detected_granularity`` — NEVER the
        # ``measure_aggregation_lineage.period_grain`` config echo.
        #
        # ``declared_anchor`` is the time_columns ROLE flag — whether the LLM DECLARED
        # this column the table's anchor — NOT the operating-model anchor. The ONE home
        # of a measure's resolved anchor axis is ``og_columns.anchor_time_axis`` (witness
        # ▸ declared precedence, DAT-780); a witness can override the declaration there,
        # so declared_anchor here and anchor_time_axis there legitimately DIVERGE. Named
        # ``declared_anchor`` (not ``is_anchor``) so the two are impossible to conflate.
        #
        # DISTINCT ON (table, column) guarantees the unique PGQ KEY even if a column were
        # ever listed twice in the JSON; the tiebreak is TOTAL (declared_anchor DESC then
        # role/aspect/name) so the retained row is deterministic — the anchor row wins.
        return (
            f"CREATE VIEW {READ_TOKEN}.og_temporal_coverage AS\n"
            f"SELECT DISTINCT ON (te.table_id, col.column_id)\n"
            f"       (te.table_id || '_' || col.column_id)::text AS coverage_key,\n"
            f"       te.table_id::text AS table_id, col.column_id::text AS column_id,\n"
            f"       col.column_name, tc.role, tc.aspect, tc.declared_anchor,\n"
            f"       tp.min_timestamp AS observed_min, tp.max_timestamp AS observed_max,\n"
            f"       tp.span_days, tp.detected_granularity AS observed_grain,\n"
            f"       tp.completeness_ratio, tp.expected_periods, tp.actual_periods,\n"
            f"       tp.gap_count, tp.is_stale, tp.last_period_complete\n"
            f"FROM {READ_TOKEN}.current_table_entities te\n"
            f"CROSS JOIN LATERAL (\n"
            f"    SELECT elem->>'column' AS column_name, elem->>'role' AS role,\n"
            f"           elem->>'aspect' AS aspect,\n"
            f"           (elem->>'is_anchor')::boolean AS declared_anchor\n"
            f"    FROM json_array_elements(COALESCE(te.time_columns, '[]'::json)) AS elem\n"
            f"  ) tc\n"
            f"JOIN {READ_TOKEN}.current_columns col\n"
            f"  ON col.table_id = te.table_id AND col.column_name = tc.column_name\n"
            f"LEFT JOIN {READ_TOKEN}.current_temporal_column_profiles tp\n"
            f"  ON tp.column_id = col.column_id\n"
            f"ORDER BY te.table_id, col.column_id, tc.declared_anchor DESC NULLS LAST,\n"
            f"         tc.role, tc.aspect, tc.column_name;"
        )
    if name == "og_rolls_up_to":
        # rolls_up_to edge (column → column, DAT-730): the dimension drill-down
        # hierarchy unnested into ordered level→level edges. ``HierarchyMember.level``
        # is the SOLE direction carrier (level 0 coarsest, increasing = finer, DAT-779
        # — array position is incidental and MUST NOT be read). "Rolls up to" is
        # finer→coarser, so an edge goes from the level-N member (source) to the
        # level-(N-1) member (destination): consecutive levels only (one drill step).
        # kind='drilldown' ONLY — alias/role structures are peer sets with no
        # coarse/fine axis (their ``level`` is a bare ordinal). members[].column_id is
        # the catalog SliceDefinition's underlying column (og_columns vertex identity);
        # a member with no catalog column ('' per HierarchyMember) is skipped — no
        # dangling endpoint.
        #
        # edge_key = hierarchy_id + the two LEVELS ('_'-joined, NOT ':' a bind-param
        # sigil). Keyed on LEVELS, not column ids: ``processor._validated_members``
        # (DAT-779) asserts each drilldown's levels are a contiguous, UNIQUE ``0..n-1``
        # at every write, so the (from_level, to_level) consecutive pair is unique within
        # a hierarchy → the PGQ KEY is unique BY CONSTRUCTION. Column-uniqueness across
        # levels is NOT enforced there, so a column id repeated at two consecutive levels
        # would collide a column-keyed edge — keying on the validated levels sidesteps it
        # without relying on an unenforced convention (the og_derived_from KEY-on-the-view
        # discipline). Reads current_dimension_hierarchies (the promoted catalog head,
        # DAT-537).
        return (
            f"CREATE VIEW {READ_TOKEN}.og_rolls_up_to AS\n"
            f"SELECT (h.hierarchy_id || '_' || fine.lvl || '_' || coarse.lvl)::text\n"
            f"         AS edge_key,\n"
            f"       fine.col_id::text AS from_column_id,\n"
            f"       coarse.col_id::text AS to_column_id,\n"
            f"       h.hierarchy_id::text AS hierarchy_id,\n"
            f"       fine.lvl AS from_level, coarse.lvl AS to_level\n"
            f"FROM {READ_TOKEN}.current_dimension_hierarchies h\n"
            f"CROSS JOIN LATERAL (\n"
            f"    SELECT m->>'column_id' AS col_id, (m->>'level')::int AS lvl\n"
            f"    FROM json_array_elements(h.members) AS m\n"
            f"  ) fine\n"
            f"CROSS JOIN LATERAL (\n"
            f"    SELECT m->>'column_id' AS col_id, (m->>'level')::int AS lvl\n"
            f"    FROM json_array_elements(h.members) AS m\n"
            f"  ) coarse\n"
            f"WHERE h.kind = 'drilldown'\n"
            f"  AND coarse.lvl = fine.lvl - 1\n"
            f"  AND fine.col_id <> '' AND coarse.col_id <> '';"
        )
    if name == "og_period_rolls_up_to":
        # period_rolls_up_to edge (period_grain → period_grain, DAT-730): the CONSTANT
        # calendar ladder day→month→quarter→year. Workspace-invariant structure (the
        # fiscal boundary rides the period_grain VERTEX, read from workspace_calendar);
        # here the three roll-up steps are literal. Walkable by the SAME bounded
        # recursive CTE the part_of closure uses, so last-complete-quarter follows from
        # last-complete-month. ``::text`` keys for the PGQ varchar-equality rule; '_'
        # delimiter (not ':' — a bind-param sigil to text()).
        return (
            f"CREATE VIEW {READ_TOKEN}.og_period_rolls_up_to AS\n"
            f"SELECT (l.from_grain || '_' || l.to_grain)::text AS edge_key,\n"
            f"       l.from_grain::text AS from_grain, l.to_grain::text AS to_grain\n"
            f"FROM (VALUES ('day', 'month'), ('month', 'quarter'), ('quarter', 'year'))\n"
            f"       AS l(from_grain, to_grain);"
        )
    if name == "og_additivity":
        # additivity_verdict vertex (DAT-731): the 2-axis drill additivity verdict, a
        # ::text projection of current_metric_additivity (read_views.py, the
        # operating_model metrics-phase head). One row per drill TARGET — a `metric`
        # (target_key = the formula graph_id / lifecycle artifact_key) or a `measure`
        # (target_key = the concept standard_field). categorical_/time_additive say
        # whether a breakdown by that axis class reconciles to the unsliced total;
        # the *_reason names the cause when it does not (stock / average /
        # distinct_count / snapshot_count / min_max / ratio / unknown_*), NULL when it
        # reconciles. additivity_id is a per-run uuid4 but UNIQUE within one promoted
        # state (the current view resolves one run), so it is a valid LOCAL vertex KEY
        # — the og_references relationship_id discipline ("a fine local edge key inside
        # one promoted state"). target_kind discriminates the two kinds for a consumer
        # that MATCHes the metric case by property (no metric vertex to traverse from).
        return (
            f"CREATE VIEW {READ_TOKEN}.og_additivity AS\n"
            f"SELECT additivity_id::text AS additivity_id, target_kind, target_key,\n"
            f"       categorical_additive, time_additive, categorical_reason, time_reason\n"
            f"FROM {READ_TOKEN}.current_metric_additivity;"
        )
    if name == "og_has_additivity":
        # has_additivity edge (concept → additivity_verdict, DAT-731): a MEASURE
        # verdict's target_key IS a concept name (the standard_field), so the edge
        # resolves it to the ACTIVE concept row — the same (name, superseded_at IS
        # NULL) resolution og_grounded_by uses, and the same INNER-JOIN discipline
        # (the graph never dangles: a verdict whose target_key names no active concept
        # simply has no edge). METRIC-kind verdicts are excluded here — a formula
        # graph_id is not a concept, so a metric verdict is reachable only by property
        # on the vertex, never by this traversal (honest under-coverage, documented).
        # concepts is the vertical-scoped read view (read_views.py _VERTICAL_SCOPED),
        # so cross-vertical names never bind. edge_key = (concept_id, additivity_id) —
        # unique because a measure verdict resolves to at most one active concept per
        # name under the vertical scope; '_'-joined, NOT ':' (a bind-param sigil).
        return (
            f"CREATE VIEW {READ_TOKEN}.og_has_additivity AS\n"
            f"SELECT (c.concept_id || '_' || a.additivity_id)::text AS edge_key,\n"
            f"       c.concept_id::text AS concept_id,\n"
            f"       a.additivity_id::text AS additivity_id,\n"
            f"       a.target_key\n"
            f"FROM {READ_TOKEN}.current_metric_additivity a\n"
            f"JOIN {READ_TOKEN}.concepts c\n"
            f"  ON c.name = a.target_key AND c.superseded_at IS NULL\n"
            f"WHERE a.target_kind = 'measure';"
        )
    if name == "og_measured_in":
        # measured_in edge (column → column, DAT-731): a measure column → the column
        # that DEFINES its unit, projecting the semantic-layer authored
        # ColumnConcept.unit_source_column (catalogue_semantics writes it; this is its
        # ONE home — the typing-layer Pint detected_unit is evidence the author saw,
        # never a second edge source). The pointer's value shapes (catalogue_semantics
        # prompt): a bare sibling column name in the measure's OWN table (e.g.
        # `currency`), a 'table.column' reachable via a confirmed FK, or the measure's
        # own column name (a self-denominated value → a self-loop, honest). NULL / ''
        # / 'dimensionless' carry NO edge (nothing to point at). Name → column_id
        # resolution is INNER (the og_uses / og_temporal_coverage discipline: an
        # unresolvable name is honest under-coverage, never a dangling edge). A bare
        # name resolves in the measure column's own table; a qualified name splits on
        # '.' and resolves the table by name (typed tables — current_tables is
        # layer='typed'). edge_key = (measure_column_id, unit_column_id), unique per
        # pair; '_'-joined, NOT ':'. self_denominated flags the measure==unit self-loop
        # so a consumer can tell "carries its own unit" from "points at a sibling".
        return (
            f"CREATE VIEW {READ_TOKEN}.og_measured_in AS\n"
            f"SELECT (cc.column_id || '_' || unit_col.column_id)::text AS edge_key,\n"
            f"       cc.column_id::text AS measure_column_id,\n"
            f"       unit_col.column_id::text AS unit_column_id,\n"
            f"       cc.unit_source_column AS unit_source,\n"
            f"       (cc.column_id = unit_col.column_id) AS self_denominated\n"
            f"FROM {READ_TOKEN}.current_column_concepts cc\n"
            f"JOIN {READ_TOKEN}.current_columns mc ON mc.column_id = cc.column_id\n"
            f"JOIN {READ_TOKEN}.current_tables mt ON mt.table_id = mc.table_id\n"
            f"CROSS JOIN LATERAL (\n"
            f"    SELECT\n"
            f"      CASE WHEN position('.' in cc.unit_source_column) > 0\n"
            f"           THEN split_part(cc.unit_source_column, '.', 1)\n"
            f"           ELSE mt.table_name END AS tbl,\n"
            f"      CASE WHEN position('.' in cc.unit_source_column) > 0\n"
            f"           THEN split_part(cc.unit_source_column, '.', 2)\n"
            f"           ELSE cc.unit_source_column END AS col\n"
            f"  ) ref\n"
            f"JOIN {READ_TOKEN}.current_tables ut ON ut.table_name = ref.tbl\n"
            f"JOIN {READ_TOKEN}.current_columns unit_col\n"
            f"  ON unit_col.table_id = ut.table_id AND unit_col.column_name = ref.col\n"
            f"WHERE cc.unit_source_column IS NOT NULL\n"
            f"  AND cc.unit_source_column <> ''\n"
            f"  AND cc.unit_source_column <> 'dimensionless';"
        )
    raise AssertionError(f"unreachable: {name} not an element view")


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
        f"      PROPERTIES (column_id, table_id, column_name, semantic_role, materialization,\n"
        f"                  anchor_time_axis),\n"
        f"    {READ_TOKEN}.og_concepts KEY (concept_id) LABEL concept_node\n"
        f"      PROPERTIES (concept_id, vertical, name, kind, ordering),\n"
        f"    {READ_TOKEN}.og_grounding KEY (snippet_id) LABEL grounding_node\n"
        f"      PROPERTIES (snippet_id, concept, statement, aggregation,\n"
        f"                  relation, select_expr, where_predicates, description, failed),\n"
        f"    {READ_TOKEN}.og_period_grain KEY (grain) LABEL period_grain\n"
        f"      PROPERTIES (grain, ordinal, fiscal_year_start_month, calendar_source),\n"
        f"    {READ_TOKEN}.og_additivity KEY (additivity_id) LABEL additivity_verdict\n"
        f"      PROPERTIES (additivity_id, target_kind, target_key, categorical_additive,\n"
        f"                  time_additive, categorical_reason, time_reason)\n"
        f"  )\n"
        f"  EDGE TABLES (\n"
        f"    {READ_TOKEN}.og_references KEY (relationship_id)\n"
        f"      SOURCE KEY (from_table_id) REFERENCES og_tables (table_id)\n"
        f"      DESTINATION KEY (to_table_id) REFERENCES og_tables (table_id)\n"
        f"      LABEL refs\n"
        f"      PROPERTIES (cardinality, relationship_type, confidence, confirmation_source,\n"
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
        f"    {READ_TOKEN}.og_grounded_by KEY (edge_key)\n"
        f"      SOURCE KEY (concept_id) REFERENCES og_concepts (concept_id)\n"
        f"      DESTINATION KEY (snippet_id) REFERENCES og_grounding (snippet_id)\n"
        f"      LABEL grounded_by\n"
        f"      PROPERTIES (concept),\n"
        f"    {READ_TOKEN}.og_uses KEY (edge_key)\n"
        f"      SOURCE KEY (snippet_id) REFERENCES og_grounding (snippet_id)\n"
        f"      DESTINATION KEY (column_id) REFERENCES og_columns (column_id)\n"
        f"      LABEL uses\n"
        f"      PROPERTIES (role),\n"
        f"    {READ_TOKEN}.og_temporal_coverage KEY (coverage_key)\n"
        f"      SOURCE KEY (table_id) REFERENCES og_tables (table_id)\n"
        f"      DESTINATION KEY (column_id) REFERENCES og_columns (column_id)\n"
        f"      LABEL temporal_coverage\n"
        f"      PROPERTIES (column_name, role, aspect, declared_anchor, observed_min,\n"
        f"                  observed_max, span_days, observed_grain, completeness_ratio,\n"
        f"                  expected_periods, actual_periods, gap_count, is_stale,\n"
        f"                  last_period_complete),\n"
        f"    {READ_TOKEN}.og_rolls_up_to KEY (edge_key)\n"
        f"      SOURCE KEY (from_column_id) REFERENCES og_columns (column_id)\n"
        f"      DESTINATION KEY (to_column_id) REFERENCES og_columns (column_id)\n"
        f"      LABEL rolls_up_to\n"
        f"      PROPERTIES (hierarchy_id, from_level, to_level),\n"
        f"    {READ_TOKEN}.og_period_rolls_up_to KEY (edge_key)\n"
        f"      SOURCE KEY (from_grain) REFERENCES og_period_grain (grain)\n"
        f"      DESTINATION KEY (to_grain) REFERENCES og_period_grain (grain)\n"
        f"      LABEL period_rolls_up_to\n"
        f"      PROPERTIES (from_grain, to_grain),\n"
        f"    {READ_TOKEN}.og_has_additivity KEY (edge_key)\n"
        f"      SOURCE KEY (concept_id) REFERENCES og_concepts (concept_id)\n"
        f"      DESTINATION KEY (additivity_id) REFERENCES og_additivity (additivity_id)\n"
        f"      LABEL has_additivity\n"
        f"      PROPERTIES (target_key),\n"
        f"    {READ_TOKEN}.og_measured_in KEY (edge_key)\n"
        f"      SOURCE KEY (measure_column_id) REFERENCES og_columns (column_id)\n"
        f"      DESTINATION KEY (unit_column_id) REFERENCES og_columns (column_id)\n"
        f"      LABEL measured_in\n"
        f"      PROPERTIES (unit_source, self_denominated)\n"
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


def grant_reader_on_graph(connection: Connection, workspace_schema: str) -> None:
    """Grant the workspace's reader role SELECT on the property graph (ADR-0008).

    A property graph is a distinct privilege-checked object: ``GRANT SELECT ON ALL
    TABLES`` (what ``ensure_workspace_roles`` does for the ``og_*`` views) does NOT
    cover ``GRAPH_TABLE`` access — without this the reader can plain-SELECT the
    element views but the one query form the graph exists for is ``permission
    denied``. The graph is dropped+recreated every boot, so the grant is re-applied
    here every boot too. Runs AFTER ``ensure_workspace_roles`` (which creates the
    role) and after the graph is (re)created. Postgres-only.
    """
    read_schema = read_schema_name_for(workspace_schema)
    reader = reader_role_for(workspace_schema)
    connection.execute(
        text(f'GRANT SELECT ON PROPERTY GRAPH "{read_schema}".{PROPERTY_GRAPH_NAME} TO {reader}')
    )
