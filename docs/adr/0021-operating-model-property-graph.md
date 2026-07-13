# ADR-0021 — The operating model is a SQL/PGQ property graph over the read views

- **Status:** Accepted
- **Date:** 2026-07-13
- **Ticket:** DAT-726 (Phase 1 of the operating-model knowledge-graph epic DAT-725)
- **Design doc:** Confluence DD/49152002

## Context

The pipeline already emits the operating model as metadata — column roles, measure
materialization, FK topology, dimensions, enriched-view derivation — but it is scattered
across the `current_*` promoted-read rows (ADR-0008) with ad-hoc, per-consumer
vocabularies. Both SQL consumers (the engine grounding agent, the cockpit answer agent)
re-derive the same structure from a flat context, and per-vertical validations are
hand-authored. The epic's thesis is that grounding *and* validation should be generated
from one typed structure. Postgres 19 ships SQL/PGQ (SQL:2023 property graphs) in core,
which makes "query the metadata as a graph" available without a new store — the open
question was whether it composes with our substrate.

## Decision

Define one property graph, `operating_model`, with `CREATE PROPERTY GRAPH` over the
`current_*` read views, materialized per workspace in the `ws_<id>_read` schema. Thin
`og_*` element views shape the read surface into vertex/edge relations; the graph binds
them with explicit `KEY` clauses. **PGQ is committed with no fallback read layer** — if
it cannot carry the model the ontology approach itself does not hold, so a hedge would
only add dead code (the epic's locked call).

The choice was verified empirically on `postgres:19beta1` before adoption, which also
fixed two encoded facts: the graph binds directly to **views** with explicit `KEY`
clauses (a view has no primary key, and none is needed), and key columns must be `text`
(PG19 finds no equality operator for an unbounded-`varchar` SOURCE/DESTINATION key
comparison).

**Two query mechanisms over one edge set.** PG19 SQL/PGQ is *fixed-depth* only — `MATCH`
expresses a fixed number of hops; a path quantifier raises `element pattern quantifier
is not supported`. So a graph read is one of:

- **1..N fixed hops → PGQ `MATCH`.** Every edge is 1-hop and native.
- **Transitive closure → a bounded recursive CTE** over the same edge view, capped at a
  max traversal depth (≈4, the observed CoA/dimension/calendar depth) with a cycle
  guard. This carries reference chains now, and part_of ancestry / calendar roll-up /
  metric DAGs as those land.

Rejected: **RDF/triplestore** — a grounding and a comparison are N-ary facts that reify
to blank nodes in RDF but are single nodes in a property graph; a triplestore is a second
store to keep in sync. **A dedicated graph database** (Neo4j, or the AGE extension) — a
second store and query language over the same rows the pipeline already writes to
Postgres; SQL/PGQ keeps one store, one dialect, native run-versioning through the views.
**Keeping the flat context** — it cannot present edges or semantic structure and is
retired at the cutover (P9), so running it alongside is A/B dead weight.

## Consequences

- **Grounding and validation generate from edges.** A concept's groundings, a fact's
  dimensions, an enriched view's bases, a measure's additivity are edges an agent
  `MATCH`es rather than prose it re-derives; each shipped domain validation is an
  instance of an edge, so the validation set is generated, not hand-authored (P10).
- **Bootstrap ordering is load-bearing.** The graph depends on its element views, which
  depend on the `current_*` views, which `materialize_read_schema` drops+recreates each
  boot; Postgres refuses to drop a view with a dependent. So the graph + element views
  are torn down *before* the read-view refresh and rebuilt *after* it. The DDL is
  generated (`schema_graph.sql`) and policed by the same `schema-drift` CI gate as the
  schema and read views.
- **No variable-length PGQ.** Transitive traversal is a recursive CTE, not a `MATCH`
  quantifier — bounded and cycle-guarded by contract. This is the honest limit of the
  PG19 feature, not a design choice, and it is proven survivable in P1 rather than
  discovered in a later phase.
- **Follow-ups:** vocabulary `Concept` vertices and concept edges are P3/P4;
  `rolls_up_to` (dimension-hierarchy JSON members) is P5 with its consumer; the cockpit
  read side is P12; the `postgres:19beta1` pin becomes `postgres:19` at GA (a version
  bump). The flat grounding context is retired at P9 — no A/B.
