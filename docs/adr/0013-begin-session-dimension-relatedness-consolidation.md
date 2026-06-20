# ADR-0013 — Consolidate begin_session's dimension & relatedness output (DAT-514)

- **Status:** Accepted; **amended 2026-06-17** (one-view model) and **2026-06-20** (enriched-view shape stability — see Amendments)
- **Date:** 2026-06-16
- **Ticket:** DAT-514 (epic "Stabilise relationships and slices discovery"); spike DAT-535; enriched-view stability DAT-516
- **Design doc:** Confluence DD/36798466

## Amendment (2026-06-17): one-view model — the aggregation-view substrate is dropped

Reviewing DAT-536 (P1) under the light of DAT-543 (driver discovery) collapsed the original
decision's justification for *building* an aggregation-view substrate. The "Keep the witness;
replace slice materialization with aggregation VIEWS" bullet below is **superseded in part**:
we still kill the slice sprawl and still re-point the witness off the enriched view behind the
same equivalence gate — but we do **not** build named, recipe-versioned aggregation views.

Why: the substrate was justified by **three** consumers. Under DAT-543 only one is live, and it
runs once:
- **Driver tree (DAT-543/545)** — reads `enriched_view` at **row grain**, never a pre-summed
  view, because its (A) dim-present / (B) missingness-concentration gates need the raw NULL rows
  a `GROUP BY` has already discarded. It also searches dimension *combinations* (recursive splits,
  per measure, on demand) — combinatorial, so a fixed one-view-per-dimension set is the wrong
  *shape* for it regardless.
- **Catalog** — its readers consume `SliceDefinition` *metadata*, not a physical view; "each
  cataloged dimension *is* an aggregation view" was a conceptual flourish, not a dependency.
- **Metrics page** — north-star, and per this ADR itself Haiku-composed-SQL on demand, not
  pre-built views.

That leaves the `structural_reconciliation` witness as the sole live consumer, and it runs **once
per session over single dimensions** — a lazy view caches nothing, so a named/recipe-versioned
view for one once-per-run reader is speculative generality.

**Revised decision — separate the two concerns:**
- begin_session **evaluates** the dimensional/aggregation *quality vectors* — grain-safety,
  cardinality, FD/hierarchy edges (DAT-537), and later driver rankings (DAT-545) — and promotes
  that **catalog metadata** downstream. This is the durable value of the slice/dice phase.
- **Materializing** slices/aggregations *for reuse* is deferred to the downstream consumers that
  actually need a concrete artifact — the SQL / graph / answer agents and a future
  metrics-exploration agent — composed on demand as SQL, and promoted to a materialized table
  only on a measured hot path. Reusability is their concern, not begin_session's.
- The witness computes its per-(dim-value, period) sums **inline** over the enriched view (a
  CTE/subquery keyed by the catalog's `distinct_values`, `WHERE time IS NOT NULL`, same numeric
  column set) — no named view, no aggregation `MaterializationRecipe`, no versioning discipline.
  Same equivalence gate (verdicts unchanged on `detection-stockflow-events-v1`), simpler mechanism.

Net change to the model below: **one-view** (`enriched_view`) + inline witness aggregation,
**not** a two-view (`enriched_view` + `aggregation_view`) model. Everything else in this ADR —
the catalog promotion, `grain_safe`-is-free, the g3 FD/hierarchy pass, the `dimensional_entropy`
cut, the dead-correlation cut, the slice-sprawl removal list — stands unchanged.

## Amendment (2026-06-20): the enriched-view shape is sticky, not re-judged (DAT-516)

The enriched view's **shape** — which `<fk>__<attr>` dimension columns a fact exposes — is the
output of two LLM judgments: (A) `relationship_discovery` confirms the FK is genuine, and (B) the
`enriched_views` phase runs a *second* LLM (`_get_llm_recommendations` → `EnrichmentAgent`) that, of
the already-confirmed relationships, judges which "add analytical value" as dimension joins. **Layer
B re-judges from scratch every run.** Combined with the latest-only lake grain (DAT-415 — the enriched
`Table`/`Column`s are reconciled in place, prior columns deleted), a re-run of the *same session* can
silently change or erase the columns downstream SQL depends on. DAT-516's observation: `journal_lines`
exposed `account_id__account_type` one run and produced a `passthrough_enriched_view` (0 dimension
columns) the next. The phase's own `sql_equivalent` no-op (it skips a redundant recipe version when
"temp-0 LLM → same joins") rests on a **false premise** — LLMs are not deterministic even at
temperature 0.

The asymmetry is the bug: **Layer A (the relationship catalog) has silent-accept durability**
(DAT-409 — an `llm` relationship the prior promoted run found, that this run didn't reproduce and the
user didn't reject, is lifted to a `keep`→`keeper` overlay; existing relationships are not removed
unless the user rejects them). **Layer B has no such memory** — it re-litigates which durable
relationships count, throwing away the durability Layer A established.

We **do not** make Layer B deterministic — column selection is genuine judgement (a rules-based
builder was tried and is hard), and the broader call already stands that determinism is not the goal
for this part of the pipeline. Instead we extend Layer A's silent-accept pattern **one layer up**, to
the enriched-view shape:

**Revised decision — the enriched-view shape is decided once and inherited:**
- **Keep Layer B's intelligence, run it incrementally.** On a re-run the enrichment LLM is fed
  **only the undecided relationships** (candidates confirmed since the shape was last decided), and is
  **skipped entirely** when none are. The first run is unchanged (everything is undecided).
- **Persist the verdict, inherit on re-run.** `EnrichedView` already stores the *exposed* joins
  (`relationship_ids`); add `considered_relationship_ids` (the candidates already judged, exposed or
  not) so an already-rejected-by-LLM relationship is not re-asked. The shape is read from the stored
  spec, not re-derived.
- **The shape is monotonic.** Columns are *added* when a new confirmed relationship is judged in, and
  *removed only* on an explicit teach/reject (or a relationship the user rejected) — **never** flipped
  by a fresh re-judgment. This is precisely what keeps a named view's column set stable across runs so
  saved/downstream SQL stays valid. (Within a run, the atomic `CHECKPOINT` already prevents torn
  mid-materialize reads; this closes the cross-run half — the silent column-vanish.)
- **Materialization is unchanged and already deterministic.** The DuckDB enriched view is a
  `CREATE OR REPLACE VIEW` rebuilt from the durable spec each run; the recipe substrate
  (DAT-414/415) stores and replays that DDL. Replaying a stored join list is deterministic by
  construction — only the *decision* was ever the non-deterministic part, and it is now made once.
- **Reconcile, don't replace, at the lake grain.** `_register_and_profile_dim_columns` adds new
  dimension columns and keeps existing ones rather than delete-all-then-reinsert, so an unchanged
  shape does not churn the columns (or their `StatisticalProfile`s) consumers read.

Caveat (consistent with the silent-accept contract): an already-exposed column is **not**
auto-re-evaluated if its underlying dimension table later changes — re-deciding is an explicit signal
(a teach/reject), exactly as a relationship is. "Decide once, keep unless told otherwise" is the
contract, mirroring Layer A.

Net change: Layer B is **kept but made incremental + sticky**; one new persisted field
(`considered_relationship_ids`); the column write reconciles instead of replacing. `build_enriched_view_sql`,
`_verify_grain`, the recipe substrate, and the atomic `CHECKPOINT` — the stable core — are untouched.
Plan: DAT-516.

## Context

DAT-514 began as "evaluate the value of slices" + feed aggregation dimensions to the SQL
agents, framed as a *brutal delete* of a dead slice→temporal→lineage tail. Investigation
falsified that premise: the substrate is **not** dead — it feeds the
`structural_reconciliation` witness of the `temporal_behavior` (stock/flow) measurement,
the only data-grounded vote in that adjudication, whose resolved label already drives the
SQL agent's `end_of_period`-vs-`SUM` choice. Two kill-gate evals settled the contested
pieces; the original "grain + additivity for a deterministic composer" framing dissolved
once the composer approach was dropped. The work is a **consolidation**, not a deletion.

Settled facts:
- **Additivity is already homed.** Stock/flow lives on `SemanticAnnotation.temporal_behavior`
  / `temporal_behavior_claim` / `temporal_behavior_contested`, adjudicated by the
  `temporal_behavior` measurement and already read by `graph_sql_generation.yaml`. There is
  nothing to "rescue" and no new per-metric additivity attribute to invent.
- **Per-metric base-grain is genuinely absent**, but it was only a prerequisite for a
  deterministic GROUP-BY composer we are **not** building.
- **The answer agent is CTE + `final_sql` + decompose-on-the-way-out** (`tools/query.ts`),
  not scalar steps — GROUP BY is LLM-authored, so "deterministically wrap a scalar" never
  applied to it.

Eval results (kill-gate, recorded as evidence):
- **structural_reconciliation witness — KEEP.** Non-redundant: rescues 2/5 backed×ambiguous
  columns the name-pair gets wrong (+16pp on backed columns), zero reverse flips, correct
  non-override on broken backing. Decisive exactly in the ambiguous regime.
- **`dimensional_entropy` (NMI) — DEMOTE.** Its band→risk mapping is *anti-predictive*:
  highest NMI on clean intrinsic structure (mutex/alias/FD, no wrong answer) and *relaxes*
  as corruption grows (20%-violated FD → lower NMI → readier band than clean). Blind to the
  violation rate that causes the wrong answer (owned by `derived_value` /
  `relationship_entropy`). A loss signal highest on safe data is misleading on the loss path.

## Decision

Reframe DAT-514 to **consolidate the dimension declaration and keep the proven machinery** —
almost nothing is deleted:

- **Keep the witness; replace slice materialization with aggregation VIEWS.** *(Superseded in
  part by the 2026-06-17 Amendment: the witness is kept and re-pointed off the enriched view, but
  the aggregation views are NOT built — the witness aggregates inline; see Amendment.)* The
  `structural_reconciliation` witness, `reconcile`, and `MeasureAggregationLineage` are
  eval-justified and stay. Their input — per-(dimension-value, period) numeric sums — is
  **path-independent**: `temporal_slice` is `FROM {slice_table} … GROUP BY period` per
  physical table, byte-identically `GROUP BY dim, period` on the enriched view. So replace
  the per-value `slice_*` materialization with a named (lazy) **aggregation view** per
  (fact × dimension): `SELECT dim, period, SUM(<numeric cols>) FROM enriched_view GROUP BY
  dim, period`. One view per dimension (not one table per value), lazy so near-free,
  idiomatic with the existing `enriched_views`. It serves **three** consumers at once: the
  witness (its reconciliation input), the dimension catalog (each cataloged dimension *is* an
  aggregation view), and the future metrics page (measure-by-dimension). The slicing agent's
  job stays "pick the dimension" (→ catalog); the view SQL is deterministic from (dimension,
  period, numeric cols), so no LLM authoring is needed and `sql_template` dies. Default lazy
  views; promote a specific one to a materialized table only on a measured hot path (the
  epic's "pre-computed aggregation" question, answered on evidence). The reconciliation reads
  the aggregation views directly, so the `TemporalSliceAnalysis` table goes too. **Gate:**
  prove the aggregation view reproduces the witness verdicts on `detection-stockflow-events-v1`
  before flipping — equivalence on period bucketing, the numeric-column set, and the value
  set + NULL handling. Net: a **two-view model** — `enriched_view` (flexible, ad-hoc
  GROUP BY) + `aggregation_view` (the reusable roll-up) — replaces the slice sprawl, and the
  `slice_table_name` prefix-collision guard that only existed because of per-value tables
  dies with them.
- **Consolidate the dimension *declaration*, not the materialization.** `SliceDefinition`
  already is a per-`(table, column)` dimension declaration spanning fact-own and enriched
  `<fk>__<attr>` dims, with `distinct_values`, `priority`, `business_context`, `sql_template`.
  Promote it to the **dimension catalog**: same row + `grain_safe` (= the enriched view's
  `is_grain_verified`, free) + FD/hierarchy edges. **One declaration, two consumers:** the
  witness path still materializes from `sql_template`; the catalog / answer agent / future
  metrics page read it for grain-safe aggregation.
- **New: deterministic g3 functional-dependency / hierarchy pass over the enriched view.**
  `g3(A→B) = 1 − COUNT(DISTINCT A)/COUNT(DISTINCT (A,B))`, one SQL pass, no LLM, no NMI.
  Directional ⇒ drill-down hierarchies (`zip → city → state`, crossing the star schema for
  free because the view is denormalized) + 1:1 alias collapse (redundant-axis dedup).
  Net-new (it does not exist today). Guard FD false positives with min-support thresholds +
  teach-confirm.
- **`grain_safe` is free.** Every column on a `is_grain_verified` enriched view is grain-safe
  to GROUP BY by construction (a fan-out dim would have failed verification). Cross-checked
  against `Relationship.cardinality` for the audit trail.
- **`dimensional_entropy` → CUT** (detector + `loss.yaml` row + `expected_dependency` teach).
  The NMI formula is a correct association measure; the *framing* is broken: association
  **strength ≠ risk**. NMI is highest on clean intrinsic structure and *lowest* on the
  violation that causes the wrong answer (eval: anti-predictive — a 20%-violated FD lands a
  *readier* band than clean). Risk lives in violation of *expected* structure, already owned
  by `derived_value` (formula), `relationship_entropy`/orphan (join). Reframed to discovery,
  directional `g3` subsumes it; its only non-redundant residue is symmetric mutex/co-presence,
  which has **no consumer** (sweep: zero cockpit readers of `cross_column_patterns`; off the
  loss path). It is not the hierarchy engine either (NMI is symmetric + table-grained). The
  bar is *proven to work*, not origin — it failed its gate, so it goes.
- **`derived_value` — KEEP** (pooled formula-identity measurement, real aggregation-safety
  failure mode); expand it on enriched views. **Cut the dead Pearson/Spearman correlation**
  (never computed, no consumer).
- **Additivity** consumers read `SemanticAnnotation.temporal_behavior`; no new attribute.
  **Per-metric base-grain is deferred** to the metrics-page north-star.

## Consequences

Removal list is from a consumer sweep (grep every begin_session artifact for a live reader —
the method that caught the `loaders.py:757` orphan and falsified the original "dead" calls).

- **Kept:** the `structural_reconciliation` witness + `reconcile` + `MeasureAggregationLineage`;
  `derived_value` / `DerivedColumn`; cross-table `relationships`; `enriched_views`. `SliceDefinition`
  is **kept and repurposed** as the dimension catalog — the sweep shows it load-bearing well beyond
  slicing (`cycles/context`, `validation/resolver`, `graphs/context` = the GraphAgent's dimension
  context, `lineage/processor`).
- **Removed (with the aggregation-view switch, behind the equivalence gate):** the per-value
  `slice_*` materialization (`slice_analysis` phase); `SlicingView` + the `slicing_view` phase;
  the `TemporalSliceAnalysis` table (reconciliation reads the aggregation view instead);
  `sql_template`; the `slice_table_name` collision guard; and the defined-but-never-queried cockpit
  views `currentSlicingViews` / `currentTemporalSliceAnalyses` (+ their `read_views.py` catalog
  registrations). `schema.sql` regenerated; `schema-drift` gate green.
- **New:** the dimension catalog (declaration consolidation) + the g3 FD/hierarchy pass (its own
  phase — additive, not core to the catalog v1). *(Per the Amendment: no aggregation-view
  substrate — the witness aggregates inline over the enriched view.)*
- **Cut:** `dimensional_entropy` (detector + loss row + `expected_dependency` teach — failed its
  eval gate) and the dead Pearson/Spearman numeric correlation.
- **Dropped from the original plan:** the brutal delete, the stock/flow "rescue," and the
  deterministic GROUP-BY composer.
- **Teach:** the new **hierarchy/alias teach** is net-new (not an absorption — the
  `expected_dependency` teach is cut with `dimensional_entropy`). It asserts "`A→B` is a
  hierarchy level / alias," consumed by the catalog.
- **North-star (out of scope):** a validated-metrics web page with clickable dimensions as
  aggregation opportunities, composed by a Haiku agent + validated with polyglot (not a
  deterministic composer). Per-metric base-grain lands when that does.
- **Follow-ups / risks:** g3 FD false positives on thin domains (mitigate with min-support +
  teach-confirm); the FD pass is genuinely new work; the witness re-point (inline aggregation
  over the enriched view) is gated on the equivalence proof (witness verdicts unchanged). Eval
  caveat: both kill-gate results are small-n directional reads — widen the corpora before
  treating reliabilities as final.
