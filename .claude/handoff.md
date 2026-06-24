# Calibration Handoff

Changes in dataraum that need attention in other repos.

Updated by `/implement` in this repo. Read by `/accept` in dataraum-eval.

## 2026-06-24: DAT-629 — topo-warm the shared metric node-set (within-run reuse)

`metrics_phase` authored metrics in parallel, so a sub-node shared by several metrics (e.g.
the `cost_of_goods_sold` extract) was cold in the snippet cache for all of them at once —
each independently LLM-authored it, they diverged, and some ground to an empty filter
(born-loud `composed but not executed: extract '<x>' has no support`). The fix adds a
**warming pre-pass** (`graphs/node_warming.py` + `metrics_phase._warm_shared_nodes`): each
UNIQUE cache-keyed node (extract/constant/formula, keyed exactly as the snippet cache) is
authored once in dependency order before the fan-out, so the per-metric execute assembles
from the warm cache — no LLM, no within-run race, consistent grounding.

### dataraum-eval
- **Outcome shift, not a shape change.** Same artifacts/snippets; what changes is that
  metrics sharing an extract no longer randomly fall to `grounded — no support` on the
  long-format fixture (the `ebitda`/`net_income`/`operating_margin` class of failures from
  the DAT-616 smoke). Expect **more `executed`, fewer flaky `grounded — no support`**, and
  far less run-to-run variance in which metrics execute.
- A genuine empty-filter extract still stays `grounded` with the reason (warming inherits
  the same verifier gate) — that regression assertion holds.
- Orthogonal to the JOIN fan-out (**DAT-277**): warming makes grounding *consistent*, it does
  not fix a wrong value from a many-to-many join. gross_margin's magnitude bug is still DAT-277.
- **Status**: pending.

## 2026-06-24: DAT-621 — context model "don't cut, don't guess" (caps gone, complete value-sets, look_values)

Follow-up to DAT-616. The principle: never cut grounding context blindly, never pre-guess
relevance. Changes the SQL-gen context surface again — calibrations that snapshot
`format_metadata_document` or the cockpit context blocks will see it.

- **Complete categorical value-sets (engine):** `build_execution_context` now fetches the FULL
  freq-ordered value-set live (`_fetch_complete_value_set`, a bounded `DISTINCT`) for a
  categorical-role column whose `distinct_count` is within the reasonable-top (`_VALUE_SET_COMPLETE_MAX`
  = 200) but exceeds the profiler's stored top-K (=20). So a genuine dimension's Value-set is now
  COMPLETE, not a top-20 PARTIAL. (Spike: dim distinct median 27 → top-20 was incomplete for the
  median dimension.)
- **No silent suppression / caps:** the engine `_VALUE_SET_RENDER_MAX=50` suppression is GONE —
  every categorical-role column renders (low-card = complete set; high-card = freq-ordered SAMPLE
  with its size, marked `SAMPLE — N of M`). `cycle.evidence[:3]`, cockpit `MAX_SECONDARY_PER_MEASURE`,
  `MAX_IDENTITIES_PER_TABLE` dropped (render all). Tail-bearing caps kept.
- **`look_values([cols])` (cockpit):** new analyse/inspect drill tool — complete `{value,count}`
  per column live from the lake, batched; `complete:false` flags >1000 distinct. Chip-only (no
  canvas widget); the `<dimensions>` block points the agent to it when listed values are truncated.
- **Cache-friendly prompt layout:** `graph_sql_generation` user prompt reordered — stable grounding
  prefix, per-metric suffix (for DAT-599 caching). Pure layout; the metric `graph_specification` no
  longer leads.

### dataraum-eval
- **Prompt-surface / metadata-document change only** (no detector / DB-schema / response-shape
  change). Snapshots of the SQL-gen context will differ: Value-set lines now COMPLETE for genuine
  dimensions (no top-20 truncation), high-card columns show `SAMPLE — N of M`, no suppressed
  columns, no `[:3]`/secondary/identity caps; block ORDER changed (cache layout).
- **The shape-varied eval (the DAT-616 open ask) still applies** — now run against complete
  value-sets. Opaque-code fixtures still fall loud.
- **Status**: pending.

## 2026-06-24: DAT-616 — feed BOTH SQL agents the full grounding + compose-CTE + feedback loops

The full feed (the design in `plans/metric-grounding/dat-616.md`), not just the value-set
slice. The engine GraphAgent is the grounding PRODUCER (one-shot, writes the snippet
library — full feed); the cockpit answer agent is the CONSUMER (searches the library,
token-constrained — lean touches).

**Engine GraphAgent context (`graphs/context.py::format_metadata_document`, the `<dataset_context>`):**
- **Value sets** per table — COMPLETE value enumeration of each low-card categorical
  (`value (count)`, freq-ordered) from `StatisticalProfile.top_values` + `distinct_count`
  (the assembler dropped both); `complete` vs `PARTIAL — not exhaustive`; measures/keys/
  timestamps + `distinct_count > 50` suppressed.
- **Business Concepts** — the vertical ontology vocabulary (name + description + `indicators`
  + `exclude_patterns`).
- **## Drivers** per measure — `target_type` (grounds the aggregation) + `ranked_dimensions`
  + `interesting_slices` (dimension=value, signed effect, support) from `DriverRankingArtifact`.
  The engine loaded ZERO drivers before (the cockpit/engine asymmetry); a HINT, not the set.
- **Cycle concept bindings** — `CycleStage.indicator_column`+`indicator_values` /
  `status_column`=`completion_value` rendered as explicit IN-list `concept → (column,
  value-set)` lines (a detection-confirmed binding, lifecycle/status concepts).
- **Fan-trap caution** — `relationships.evidence.introduces_duplicates` → "SUM across this
  join double-counts (pre-aggregate)" (the second silent-wrong vector).
- **Signed-measure range** — `numeric_stats` min/max on measures; a negative min flags a
  signed measure (SUM nets). `unit_source_column` rendered as a mixed-unit caveat.
- **`_describe_table`'s `SELECT DISTINCT … LIMIT 5` self-fetch is GONE** — name+type only;
  Value sets are the authoritative enumeration.

**Engine execution (`query/execution.py`):** steps + final_sql are folded into ONE CTE
(`compose_standalone`, the Python mirror of the cockpit `composeStandalone`) and executed
as a single statement — no temp-view state; `GraphExecution.composed_sql` carries the
executable artifact alongside the per-step snippet list. Per-step scalars are still fetched
(standalone steps) so the `verifier.py` floor is unchanged.

**Engine prompt (`graph_sql_generation.yaml` v3.1 → v4.0):** grounding contract (IN-list over
the served values, no ILIKE), compose-CTE `<step_execution_model>`, `<channel_precedence>`
(concepts=meaning, Value sets=the set, drivers=hint; human/teach>driver>frequency),
`<never_force_fit>` (abstain on opaque codes / PARTIAL), `<blueprint_library>` (simple
aggregate / end-of-period / window / multi-column / ratio shapes), `<prior_context>` slot.
Generic placeholders (de-finance v3.1 spirit).

**Feedback loops:** the prior-run honest-fail `LifecycleArtifact.state_reason` and prior
`provenance.column_mappings_basis` (the value→concept filter) — both written-never-read — are
now fed back into the GraphAgent's `<prior_context>` (abstain-or-address steer; reuse prior
grounding). Pairs with the kept `verifier.py` value-bound + NULL sanity floor (#369).

**Cockpit answer agent (CONSUMER — lean):** `<dimensions>` now carries slice-catalog
`distinct_values` inline (capped + overflow tail — the naturally-bounded grounding set);
`snippet_search` results surface `column_mappings_basis`; the query prompt gains a
`<grounding>` IN-list + never-force-fit rule. No raw value-set pre-inject.

### dataraum-eval
- **Prompt-surface + metadata-document change only** (no detector / DB-schema / response-shape
  change). Calibrations snapshotting `format_metadata_document` or the cockpit context blocks
  will see the new `## Drivers`, `**Value sets**`, `## Business Concepts`, cycle-binding,
  fan-trap, and `<dimensions>` value lines; empty/absent when no vertical / no low-card dims.
- **The long-format NAMES regression should PASS** (feed-only → gross_profit rel.err 0, the
  lane-1 finding). Opaque-code fixtures stay inconclusive-by-design (fall-loud + teach case).
- **NEW eval ask — shape-varied fixtures (gate the blueprint library):** the lane-1 gate only
  proved semantic NAMES. Add long-format fixtures exercising the non-trivial shapes —
  end-of-period (stock/latest-period), window/period-comparison (growth/YoY), multi-column
  conjunction, ratio — with ground-truth metric values, to verify the `<blueprint_library>`
  produces correct SQL beyond the simple aggregate. **Real-LLM; runs on request, not yet executed.**
- **Status**: pending (engine + cockpit landed; shape eval is the open cross-repo task).

## 2026-06-23: DAT-616 reworked + DAT-620 — metric grounding on long-format finance (REFRAMED)

The silently-wrong metric bug is **context-engine starvation**, not a missing checker. Full
reworked design: `docs/dat543-construct-dont-improvise.md` (this PR). The graph agent
improvises the row filter because it is served `SELECT DISTINCT … LIMIT 5` (no counts) and no
drivers. Fix = **FEED** it `top_values` + drivers + a teach-confirmed `concept→value-set`
binding (new ticket **DAT-620**), and have it author SQL from a blueprint. The `verifier.py`
in this PR stays as a cheap value-bound + NULL **sanity floor** — NOT the fix.

### dataraum-eval
- The real oracle is now **DAT-620's proposer**, not the metric value alone. Needs a
  long-format fixture with **ground-truth `concept → value-set` labels** (which
  `account_type` values ARE revenue / cost_of_goods_sold / opex / …) so the proposer's
  **precision/recall** can be scored — that p/r is the acceptance gate on DAT-620.
- Keep the prior regressions too: assert metric VALUES (not just `executed`) on the
  long-format fixture; an empty-filter extract stays `grounded` with a reason (the sanity
  floor).
- **Status**: GATE RAN (2026-06-24, eval `scripts/probes/dat620/`) — verdict shrinks
  DAT-620. Feed-only (≈ DAT-616: `top_values` + ontology) grounds **semantic names
  exactly** (gross_profit rel.err = 0.000, incl. synonyms + the exclude-trap); a dedicated
  value-level labeler adds nothing there. Opaque **GL codes break it dangerously** — the
  LLM never abstains, it *confidently mislabels* (≈57–81% gross-margin error), and a richer
  prompt doesn't fix it. So: **BUILD** DAT-616's feed; **CUT** the standalone
  lexicon-proposer (no signal on codes, redundant on names); **KEEP** teach as the
  *code/unmappable* path (binding table = the teach target, not a guess-the-code proposer);
  **MANDATORY** grounding-confidence fall-loud floor (the LLM won't self-report failure).
  Codes = a teach case by design (a missing chart-of-accounts mapping is human error).

### dataraum-testdata
- A **BookSQL-style long/transactional finance fixture** (one `Amount` column + an
  `account_type` discriminator, no per-concept columns) with TWO ground truths: (1) the
  **`concept → value-set` labels** (the NEW requirement — for the DAT-620 proposer), and
  (2) ground-truth metric values for gross_margin / gross_profit (the regression).

## 2026-06-22: fix — deterministic `top_values` ordering (profiling reproducibility)

`StatisticalProfile.top_values` is now ordered `count DESC, value` (was `count DESC` only),
so equal-frequency values no longer come back in arbitrary order across runs. The sampled
values that feed the LLM semantic prompts (`DataSampler.prepare_samples` reads `top_values`)
are therefore reproducible run-to-run for the same data.

### dataraum-eval
- **Reproducibility, not a detector change** — recall/precision unaffected; profiles +
  LLM prompts are just stable across re-runs now. No schema change.
- **Status**: pending

## 2026-06-22: fix — driver_rankings no longer crashes on a VARCHAR measure (TRY_CAST)

The driver-discovery load (`analysis/drivers/processor.py`) projected measure columns
with a hard `"{col}"::DOUBLE`, which throws `ConversionException` → `PhaseFailed` (non-
retryable) → the whole `beginSessionWorkflow` fails when a measure column the typing left
VARCHAR carries a non-numeric value. Now `TRY_CAST({col} AS DOUBLE)`: unparseable values
load as NULL→NaN, which the numpy core already treats as missing (`_floats` nulls→NaN;
ICC/targets mask `~isnan`). Behaviour-equivalent on clean numeric data (golden equivalence
+ grain suites green); only the crash path changes.

### dataraum-eval
- **Fixes a begin_session crash, NOT a detector change.** Surfaced by the DAT-540 queue:
  `detection-null-v1` (null_tokens family injects `~~~~~` sentinels into `debit`/`amount`
  at severity high → typing leaves the column VARCHAR) crashed at `driver_rankings`
  (`PhaseFailed: Could not convert string '~~~~~' to DOUBLE ... "debit"::DOUBLE`). With the
  fix the run completes; the dirty-measure rows just read as NaN in driver discovery.
- Recall/precision unaffected; no schema change. Regression pinned by
  `tests/unit/analysis/drivers/test_processor.py::TestDiscoverDrivers::test_dirty_varchar_measure_does_not_crash`.
- **Status**: pending

## 2026-06-22: DAT-540 — slice_conditional_null DEMOTED off the loss path (informative DirectSignal)

`slice_conditional_null` (bias-corrected Cramér's V of is-null × slice) was removed from
`loss.yaml` (it had `query 0.4 / aggregation 0.7 / reporting 0.6`). It now falls through
`assemble_readiness_context` to a **DirectSignal** — the benford/dimensional_entropy lane:
the Cramér's V score + `expected_dependency`/`documented_dependency` teach still compute as
context, but no longer drive intent readiness bands. The detector still runs (`statistics`
phase, add_source detect) and still emits its column-scoped `value.nulls.slice_conditional_null`
EntropyObject; only its loss row is gone.

**Why** (eval band-impact ablation, DAT-540 P5 / ADR-0013, `dataraum-eval`
`scripts/probes/dat540` — the {score}-detector analogue of the structural ablation):
(1) its only OBSERVABLE band move is a **false positive on benign structural conditionality** —
`bank_transactions.payment_id` (an optional FK, null-by-design when a transaction is not a
payment, which the column's OWN `business_meaning` documents) scored V=0.97 on slice
`counterparty` → blocked aggregation; optional FKs are ubiquitous, so the untaught default is
to block them. (2) On its INJECTED columns (`credit`/`debit`) the aggregation band is already
set by `cross_table_consistency` (0.80), so ablating slice_conditional_null moved NO band — its
marginal loss value is unproven on the existing slice corpus (confounded). A loss signal whose
only visible band move is a false block on a benign-by-default pattern is anti-predictive — the
benford/DEMOTE signature. Recorded: eval `entropy_eval_architecture.md`.

### dataraum-eval
- **Changed (engine)**: `loss.yaml` (row removed + rationale comment),
  `tests/unit/entropy/views/test_readiness_context.py` (added
  `test_slice_conditional_null_is_a_direct_signal_not_a_band_driver`). No detector/registry/phase
  change — the detector still exists and runs.
- **Affects**: any column×intent band driven *only* by slice_conditional_null now drops one band
  (it contributed agg 0.7·V / reporting 0.6·V / query 0.4·V). The `bank_transactions.payment_id`
  false `blocked` aggregation clears. A column whose only object is slice_conditional_null is no
  longer in `readiness.columns` — it's a `direct_signal`.
- **Calibrate (eval-side, NOT in this engine branch)**: `detector_coverage.yaml` disposition flips
  to `informative` (mirrors benford/dimensional_entropy); `intent_readiness.yaml` /
  `test_intent_readiness.py` clean-readiness expectations for payment_id drop the slice block;
  recall/teach for slice_conditional_null move to the DirectSignal grammar. The Cramér's V
  statistic is unchanged, so its SCORE (and the precision/recall score-separation) is unchanged.
- **Status**: pending

## 2026-06-22: DAT-566 — `identity_columns` now in the answer-agent metadata document

`semantic_per_table` has produced+persisted `TableEntity.identity_columns` since DAT-565
(recurring real-world identities / would-be FKs, distinct from `grain`), but it was
write-only on the answer-agent surface. The metadata document (`graphs/context.py`,
`format_metadata_document`) now renders an **`**Identity columns**: <col> (<note>), …`**
clause on a table's meta line, right after the time-column clause, sourced from
`TableContext.identity_columns` (new dataclass field, populated from `TableEntity`).

### dataraum-eval
- **Prompt-surface change only** (no detector, no threshold, no response-schema change): the
  SQL-gen context the GraphAgent/answer agent sees now lists each table's recurring identities,
  so it can ground "per &lt;entity&gt;" groupings (e.g. "per customer") on the real cluster key.
  Calibrations that snapshot/diff the metadata document will see the added clause; tables with
  no identities render nothing (unchanged output).
- Affected: `graphs/context.py` (`TableContext.identity_columns` + render). Cockpit-side
  surfacing (`look_table`, table-readiness widget) is out of eval scope.


## 2026-06-22: DAT-516 — enriched-view shape is now sticky (deterministic across re-runs)

The enriched-view shape (which `fk__attr` dimension columns a fact exposes) no longer
drifts across begin_session re-runs. Previously `enriched_views_phase` re-judged the shape
with a per-run LLM (`_get_llm_recommendations`/`EnrichmentAgent`), so the same session could
expose `account_id__account_type` one run and a `passthrough_enriched_view` (0 columns) the
next. Now the shape is decided once and inherited (silent-accept, mirroring the Layer-A
relationship catalog DAT-409).

### dataraum-eval
- **Behavior change (stabilizing, not a detector change):** re-running begin_session over an
  unchanged confirmed-relationship set yields the **identical** enriched-view shape and makes
  **no enrichment LLM call**. The shape changes only on: a newly-confirmed relationship (a
  column is added) or a user reject (a column is removed) — monotonic. A fresh contradictory
  LLM verdict is **ignored**. Any calibration that depended on the shape being re-derived each
  run should expect it stable now.
- **`column_id` stability:** enriched-dimension `column_id`s are now **preserved** across
  re-runs (reconcile-don't-replace), and a kept column keeps its `StatisticalProfile` (same
  run_id as when first computed). Eval assertions that expected fresh column_ids/profiles per
  run must drop that expectation.
- **New persisted fields** on `enriched_views`: `considered_relationship_pairs` (judged FK
  column-pairs) + `exposed_dimension_joins` (serialized exposed joins). `dimension_coverage`
  and slicing read the enriched columns and benefit from the now-stable shape; no read-shape
  change for them.
- Affected: `pipeline/phases/enriched_views_phase.py`, `analysis/views/db_models.py`,
  `graphs/context.py` consumers of enriched columns (unchanged behavior, stabler input).

## 2026-06-22: DAT-596 — in-place re-import-with-replace for db_recipe sources

Re-importing a `db_recipe` source under the SAME user-chosen name with a CHANGED recipe
(re-pointed SQL) no longer **fails loud**. The import phase
(`pipeline/phases/import_phase.py::_load_database_source`) now tears the source's existing
tables down across all layers (DuckDB tables, `Table`/`Column` rows, every run-versioned
metadata child, the per-table `metadata_snapshot_head` rows) and rematerializes the new
recipe in place, re-stamping `imported_recipe_hash`. New helper:
`pipeline/phases/_source_teardown.py::teardown_source_tables`.

### dataraum-eval
- **Lifecycle change, NOT a detector / response-shape change** — no DB schema change, no
  detector retuning expected; calibration recall should be unaffected. Flagged so eval is
  aware re-import is no longer an error.
- **Behavior delta to any harness that asserted the old guard:** the message
  `"…re-import is not yet supported. Re-select … under a NEW source name…"` is GONE. A
  re-pointed db_recipe import now SUCCEEDS (replaces the old tables) instead of returning a
  FAILED `PhaseResult`. Scope is db_recipe ONLY (files are content-keyed → a new source on
  change, never this path). Same-recipe retry still skips via `should_skip` (unchanged).

- **Status**: pending

## 2026-06-22: DAT-524 — temporal value-analysis cut (seasonality/trend/change-points/stability removed)

The degenerate value-analysis half of the temporal phase is gone (it ran on a constant
`Series(1)` and produced foregone-conclusion output). `statsmodels` + `ruptures` dropped;
`scipy` stays.

### dataraum-eval
- **Dropped fields — any fixture/assertion that seeds OR reads these will break:**
  `temporal_column_profiles.has_seasonality` and `.has_trend` (columns gone);
  the `SeasonalityAnalysis` / `TrendAnalysis` / `ChangePointResult` /
  `DistributionStabilityAnalysis` models and the `TemporalAnalysisResult` fields
  `seasonality` / `trend` / `change_points` / `distribution_stability`; the
  `TemporalTableSummary` scalars `columns_with_seasonality` / `columns_with_trends` /
  `columns_with_change_points`; the `many_change_points` + `unstable_distribution`
  quality issues; the `temporal_phase` outputs `with_seasonality` / `with_trend`.
- **Kept (real, index-derived):** `detected_granularity`, `update_frequency`/`is_stale`,
  `fiscal_calendar`, `completeness`.
- **LLM-facing:** `graphs/context` no longer emits the per-column **"Trending over time."**
  note (the DAT-284 `has_trend` surface). The metadata document loses that line — any
  golden/snapshot asserting it must update. No other context-doc field changed.
- **Config:** `config/phases/temporal.yaml` lost the `seasonality` / `trend` /
  `change_points` / `distribution_stability` blocks + two `quality_issues` keys.
- **Status**: pending

## 2026-06-21: DAT-580 — driver engine ported pandas → DuckDB arrow→polars + int codes

The driver-discovery engine (`analysis/drivers/`) no longer uses pandas. The enriched
view is loaded via DuckDB `to_arrow_table()` → `pl.from_arrow` (zero-copy); dimensions
are factorized to physical int codes + label lists (no resident Python `str` objects);
the measure is cast to `DOUBLE` and read as a float view; all entity-grain aggregation
(`_collapse_to_entity`, `_within_entity_residual`, ICC factorize,
`_partition_by_entity_constancy`) is numpy `bincount` over physical entity codes. The
`criterion.build_codes`/`tree` contract now takes int codes instead of object arrays.
`DEFAULT_MAX_ROWS` raised 800k → 2.4M (the arrow load cut peak RSS ~67% at 1M×15, so
the DAT-571 bottom-k-by-hash subsampling is now a rare fallback). `targets.py`,
`models.py`, `persistence.py`, `db_models.py` are unchanged.

### dataraum-eval
- **Output is behavior-equivalent — not a detector change.** A committed golden
  (`tests/unit/analysis/drivers/test_golden_equivalence.py`, 6 scenarios) pins
  `DriverRanking` across the port: structural fields exact, gains/effects within
  `atol=1e-7`, p-values within one permutation quantum. **Driver calibration recall/FDR
  should be unaffected** — run the driver-ranking calibration to confirm, but no
  re-pinning of golden gain values is expected beyond float-ε.
- **Watch for ε-level shifts only at decision boundaries**: polars/bincount summation
  can differ from pandas at ~1e-15, which *could* flip a single near-`icc_threshold`
  (0.10) routing or near-α significance call. Per polars#5325 polars summation is often
  the more accurate one; treat any such flip as a possible correction, not a regression.
- If `dataraum-eval` carries its own committed driver golden built on the pandas output,
  it may need regenerating for float-ε — same structural result expected.
- **New engine deps**: `polars`, `pyarrow` (pyarrow already transitive via pandas 3.0
  in some envs, now explicit). pandas remains a dep (relationships/temporal still use it).
- **Status**: pending

## 2026-06-19: DAT-538 — `slice_definitions.grain_safe` removed

The `grain_safe` boolean column on `slice_definitions` (the dimension catalog,
DAT-536) is **gone**. It was hardcoded `True` for every row and carried no
information: the slicing phase pre-filters fan-out columns (`distinct_count > 50` /
`cardinality_ratio > 0.5`) before the LLM, so a cataloged dimension is grain-safe by
construction. The two always-true `grain_safe.is_(True)` filters (driver
`_candidate_dims`, hierarchies processor) were removed (behavior-preserving), and
`schema.sql` + the cockpit Drizzle `current_slice_definitions` mirror were regenerated.

### dataraum-eval
- **Any fixture or assertion that seeds OR reads `slice_definitions.grain_safe` will
  break** — drop it. There is no replacement column; grain-safety is no longer a
  persisted flag.
- No detector/score output shape changed; calibration recall is unaffected. The
  answer-agent's grain handling moved to a cockpit-side, cardinality-derived
  *caveat* (inform-don't-block) — not an engine artifact, nothing to calibrate.
- Supersedes the `SliceDefinition.grain_safe (DAT-536)` mention in the DAT-546 entry
  below.

## 2026-06-18: DAT-571 — driver-discovery in-memory load is bounded by sampling

`discover_drivers` now caps the row-grain frame it materializes. A `max_rows` gate
(`DEFAULT_MAX_ROWS = 800_000`): at/below the cap the view loads in full (validated path,
byte-for-byte unchanged — the COUNT(*) is the only added work); above it, the view is
deterministically sub-sampled to `max_rows` rows via a bottom-k-by-hash sketch
(`ORDER BY hash(<cols>) LIMIT max_rows`) and a `driver_rankings_view_sampled` log fires.
Engine core (`tree.py`/`targets.py`/`criterion.py`) untouched.

### dataraum-eval
- **Two regimes for the DAT-546 "n_rows stable across reruns" assertion.** On views ≤ 800k
  rows nothing changes. On views **> 800k rows**, `DriverRanking.n_rows` now reports the
  **sample size (800k)**, not the full view size — and the bottom-k sketch is a total order
  (deterministic regardless of DuckDB thread count), so the rerun-stability assertion still
  holds in BOTH regimes (same corpus → same sample → same ranking). If a large-view
  calibration fixture exists, expect `n_rows == 800_000` and a `driver_rankings_view_sampled`
  log, not the raw row count.
- **Sampling is fail-safe, not power-neutral.** The permutation null is recomputed on the
  sample, so FDR/precision hold; the entity-grain family loses some power on *weak* drivers
  under sampling (degrades to a miss, never a fabricated driver). A large-view recall fixture
  should treat a missed *weak* entity-grain driver as expected, not a regression; a missed
  *strong* driver (or any false positive) is a real bug. DAT-580 (arrow-backed load) will
  raise the ceiling so sampling becomes a rare fallback.

## 2026-06-18: DAT-546 — driver_rankings begin_session artifact

Driver discovery is now PERSISTED, not just an in-memory engine. A new begin_session
value-layer phase `driver_rankings` (runs last in `_SESSION_VALUE_PHASE_ORDER`, after
`correlations`, before `session_detect`) enumerates each session fact's
`semantic_role='measure'` columns, runs the unchanged `discover_drivers` over each (it
self-resolves cluster keys from `identity_columns`), and writes one run-versioned
`DriverRankingArtifact` per `(measure_column_id, run_id)`. The grain-labeled output is
stored GRANULARLY (primary `grain`/`entity` + the `secondary_dimensions` list, each item
keeping its own `grain`/`entity`) — never merged into one cross-grain ranking.

- New table `driver_rankings` + read view `current_driver_rankings` (catalog grain, in
  `schema.sql`/`schema_read.sql`). The proven engine core is untouched.
- Run-scoping: measure role + temporal_behavior read unscoped-by-run (the `slicing_phase`
  convention; role is generation-stable); `discover_drivers`' substrate reads stay on the
  begin_session run.
- v1 = measure-role columns only (flow/stock). Declared-metric ratios + genuinely-ad-hoc
  question ratios are deferred (the on-demand `discover_drivers` tool, same result contract).

### dataraum-eval
- **Ranking-stability across reruns:** the artifact must be rerun-stable (DAT-563's
  home-grain routing is what makes it deterministic). Run begin_session twice on the same
  corpus and assert each measure's persisted `ranked_dimensions` + `secondary_dimensions`
  (dimension, grain, entity) are identical run-to-run; n_rows stable. A drift here is a
  determinism regression in the engine, not a calibration knob.
- The phase persists EVERY measure-role column, including empty rankings (`n_rows` records
  the power) — assert "no significant driver" is a stored row, not an absent one.

## 2026-06-18: DAT-563 — N-entity home-grain driver routing + ICC-verification resolver

Makes the cluster-aware driver path actually fire end to end, generalized to N recurring
identities. `discover_drivers` now:
- takes `cluster_keys: list[str] | None` (was `cluster_key: str | None`);
- when `cluster_keys` is None, **resolves** them from the fact's persisted
  `TableEntity.identity_columns` (DAT-565) and **ICC-verifies** each — keeps only identities
  the measure clusters within (ICC > 0.10), drops the rest (no heuristic). An explicit list
  is used verbatim;
- routes each candidate dim to ONE home grain (the entity it's constant within; finest-entity
  tiebreak) and ranks per entity via the unchanged `_entity_grain_ranking`, row-level dims via
  the unchanged `_row_wise_ranking` (de-meaned against the highest-ICC entity);
- result shape: `DriverRanking.entity` (which identity the primary entity grain belongs to) +
  `SecondaryDriver.entity` (per non-primary family) added; N=1 is bit-for-bit DAT-561.

The proven core (`_entity_grain_ranking` / `_row_wise_ranking` / `tree.py` / de-mean) is
untouched.

### dataraum-eval
- **Graduate the dat-544 probes from a HARDCODED `entity_col` to the resolver path:** call
  `discover_drivers` WITHOUT `cluster_keys` so it reads persisted `identity_columns` and
  ICC-verifies. The probe must first run `semantic_per_table` (DAT-565) so the identities are
  persisted; then assert the resolver picks the right cluster keys (not a hand-fed string).
- **Add a multi-entity fixture** (≥2 recurring identities, each with attributes + a
  within-entity row-level dim) and assert **FDR controlled PER GRAIN** (each entity's nulls +
  the row null stay ≤ ~2α at their own grain — never pooled across families) and that the
  primary is the highest-ICC entity. The engine guard is `make_two_entity_corpus` +
  `TestIdentityResolver.test_fdr_controlled_per_grain_multi_entity`.
- **Flat-denormalized fixture:** a named identity on a clustered flat table must resolve +
  cluster (grain `entity`), NOT fall through to the broken row-wise null. And a **mis-named
  low-ICC "identity"** must be dropped by verification (no heuristic) → plain row-wise.
- New result fields to consume: `DriverRanking.entity` and `SecondaryDriver.entity` (the
  identity each grain belongs to). DAT-546's artifact carries `grain`/entity per ranked dim.

### dataraum-testdata
- A **multi-entity denormalized** fixture: several recurring identities (e.g. customer,
  product, vendor) that each functionally determine some attributes, plus a genuinely
  row-level dimension — matches `make_two_entity_corpus`. Asymmetric clustering strength
  (one identity should cluster the measure harder than the others) exercises the
  primary-vs-secondary entity selection.

## 2026-06-18: DAT-565 — multi-role semantic_per_table (all time axes + identity columns)

`semantic_per_table` now emits **every** event-time axis and the table's recurring
**identity** columns, replacing the singular `TableEntity.time_column`:
- `TableEntity.time_column VARCHAR` is **GONE**, replaced by two run-versioned JSON
  columns: `time_columns` (`[{column, aspect, note}, …]`) and `identity_columns`
  (`[{column, note}, …]`). Schema change in `packages/engine/schema.sql`.
- Formatters emit ALL axes: `graphs/context.py` renders each axis (granularity/range +
  note) into the answer-agent SQL context; `slicing_phase` passes all axes to the slice
  agent and matches `is_dimension_time_column` by set-membership.
- **Lineage** (`analysis/lineage/processor.py`): the stock/flow reconciliation now
  competes EVERY event-time axis per measure and keeps the best-reconciling verdict.
  **Grain unchanged** — still one row per `(measure_column, run_id)` in
  `measure_aggregation_lineage`; the `structural_reconciliation` witness is untouched.
- `identity_columns` is **additive** — sole consumer is DAT-563 (not yet built); nothing
  reads it today.

### dataraum-eval
- **BREAKING fixture change:** any eval fixture that seeds `TableEntity(time_column=…)`
  will fail (the column no longer exists). Switch to
  `time_columns=[{"column": …, "aspect": …, "note": …}]`. Existing `ws_*` schemas need a
  fresh `down -v` (or migration) — `time_column` → `time_columns` + `identity_columns`.
- **Lineage is behavior-preserving for single-axis tables** (one axis ⇒ identical verdict
  to before). Add a **denormalized fixture with ≥2 event-time columns** on a measure fact
  and verify the best-reconciling axis still produces the correct flow/stock verdict and a
  bad/degenerate axis does not dislodge it (engine guard: `test_competes_time_axes_and_keeps_best`).
- The answer-agent SQL context now lists **all** time axes per table (each with range +
  note) — calibration that pins the metadata-document/SQL-gen context should expect the
  multi-line "Time column" block, not a single line.

### dataraum-testdata
- Add a **denormalized multi-temporal** fixture: a fact with several genuine event-time
  columns (e.g. `order_date` / `ship_date` / `delivery_date`), each a distinct lens, plus
  **≥1 recurring identity column that is a NON-grain FK** (high-cardinality, recurs across
  rows, not part of the row grain) — the shape DAT-565 produces and DAT-563 will consume.

## 2026-06-17: DAT-561 — candidate-grain routing (fixes the low-ICC entity-level FP)

Closes the DAT-552 eval-gate residual: at **ICC ≈ 0.03** a high-cardinality
entity-LEVEL random dim still false-positived under the row-wise null. Root cause:
the row-wise null is structurally invalid for an **entity-constant** candidate at
ANY ICC > 0 (pseudoreplication — its groups are whole entities). The 0.10 threshold
only masked it for high-ICC measures. `discover_drivers` now routes **per-candidate
by within-entity constancy**, not by the measure's global ICC:
- **Entity-constant** candidates (one value per entity) → entity-grain null ALWAYS.
- **Row-level** candidates (vary within entity) → row-wise null (valid at any ICC).
- The two families merge into ONE `DriverRanking`: the ICC-preferred family is the
  primary tree; the other family's significant dims surface in the NEW
  `DriverRanking.secondary_dimensions` field (a flat list of `SecondaryDriver(dimension,
  gain, grain)` — grains are not cross-comparable, never folded into the primary).
- **Power add-on:** under high ICC the row-level (secondary) family gates on the
  within-entity **de-meaned residual** — valid and powered for within-entity drivers.
  Flow/stock de-mean the measure (`measure − entity_mean`); ratio de-means the per-row
  ratio by its entity's volume-weighted mean (pooled `Σnum/Σden`), weighted-VR on the
  residual with the `den` weight.

`discover_drivers`' public signature is unchanged; `DriverRanking` gains one additive
field. Still a pure engine (no schema/persistence).

### dataraum-eval
- **The arr_delay/tailnum (low-ICC, high-K entity-level) fixture is now the regression guard.** Verify the entity-level dim never enters the row-wise primary (`ranked_dimensions`) and is gated at the entity grain (≤ 2α). Reverting to global-ICC routing puts it back into the row-wise primary → the guard fails.
- **New result field to consume: `DriverRanking.secondary_dimensions`** — the non-primary grain family's significant dims, each carrying its own `grain` (`"entity"`/`"row"`). The harness must read drivers from BOTH `ranked_dimensions` (primary) and `secondary_dimensions` (secondary), and must NOT compare gains across the two (different exchangeable grains).
- **Add a clustered fixture carrying BOTH an entity-level and a within-entity row-level driver** — for FLOW/stock (`make_clustered_two_driver_corpus`, additive, ICC ≈ 0.86) AND for RATIO (`make_clustered_ratio_two_driver_corpus`, ICC ≈ 0.85). Verify: the entity-level driver leads the entity-grain primary; the within-entity driver surfaces in the de-meaned row-wise secondary; the row-level null FDR ≤ 2α on the residual; no grain-mixing. The ratio residual is the per-row ratio minus its entity's volume-weighted mean.
- The DAT-552 entry's "open follow-up: row-level drivers skipped at entity grain" is now CLOSED by this routing — calibration CAN expect within-entity drivers from the de-meaned row-level family (flow/stock).

### dataraum-testdata
- The clustered family (DAT-552) should gain a **within-entity row-level driver** variant: a row-level column that shifts the measure within entity (independent of the entity level), alongside the existing entity-level driver — so the real fixture exercises BOTH grains in one dataset (matches `make_clustered_two_driver_corpus`).

## 2026-06-17: DAT-552 — grain-aware permutation null for driver discovery

Fixes the DAT-545 engine's row-exchangeability flaw (eval residual probe E1: the
row-wise null inflates FDR to ~100% on clustered / per-entity-level measures —
which dominate ERP/finance). `discover_drivers` gains an optional `cluster_key`:
it measures the measure's **ICC within that entity** (`intraclass_correlation` = η²
of the measure by the entity) and, above `icc_threshold` (0.10), switches to an
**entity-grain** null — collapse to one row per entity (mean measure, observed-row
weight), permute ENTITIES not rows. Below the threshold / no `cluster_key` → the
row-wise null (DAT-545) is unchanged. Still a pure engine (no schema/persistence).

### dataraum-eval
- **The calibration harness must now condition on `DriverRanking.grain`.** When `grain == "entity"`, the effective sample size is the **entity count**, reported in `DriverRanking.n_rows` (NOT the row count) — power scales with entities, so recall bars at entity grain must be entity-count-aware, not row-count-aware.
- **The real-fixture transfer check (DAT-545 handoff) MUST include repeated-entity / high-ICC fixtures** — that is exactly the case this fixes; an i.i.d.-only fixture would never exercise it. Verify: (a) row-wise null on a high-ICC fixture inflates FDR (the bug), (b) the `cluster_key` path holds FDR ≤ 2α, (c) the ICC switch fires at ~0.10. The eval probes `scripts/probes/dat-544/{exchangeability_and_measure_types,real_fixture}.py` are the validated reference; graduate them into the rig.
- **Cluster-aware applies to ratio too:** a clustered ratio uses the entity grain on the same ICC condition (entity statistic = Σnum/Σden, weight = Σden) — so the real-fixture check should include a **clustered-ratio** fixture, not just clustered levels.
- **Open follow-ups (documented gaps, not bugs):** ~~row-level (within-entity) drivers under high ICC are skipped at entity grain~~ — **CLOSED by DAT-561** (see the entry above: row-level dims now route to a de-meaned row-wise family, entity-constant dims always to the entity grain). Entity grain remains single-level (`max_depth=1`).

### dataraum-testdata
- Add a **clustered / repeated-entity** generative family (per-entity random effect on the measure → high ICC; entity-level driver + entity-level nulls + a row-level null) — the conftest `make_clustered_corpus` (200 entities × 100 rows) is the synthetic reference; a real analogue (e.g. customer/account recurring across transactions) is the target.

## 2026-06-17: DAT-545 — driver-discovery engine (analysis/drivers/)

New **pure, on-demand** engine `packages/engine/src/dataraum/analysis/drivers/`:
ranks the catalog's grain-safe dimensions by how much they explain a numeric
measure's variation (variance-reduction tree), gated by a **within-dataset
permutation null** — no global threshold, vertical-agnostic. Productionizes the
DAT-544 kill-gate spike. **No schema change, no pipeline phase, no persistence** —
it returns an in-memory `DriverRanking`; it is not yet wired to any caller (DAT-546
adds the artifact + cockpit read surface; an agent caller is later). So it does not
run in add_source/begin_session and changes no existing measurement.

Engine: candidate dims = `SliceDefinition.grain_safe` (DAT-536) with
`DimensionHierarchy` 1:1 aliases collapsed (DAT-537); substrate = the fact's
grain-verified enriched view read row-grain via DuckDB; target-type from
`SemanticAnnotation.temporal_behavior` (additive→flow, point_in_time→stock); ratio =
support-weighted Σnum/Σden.

### dataraum-eval
- **The real-fixture transfer check is eval's task** (agreed handoff — it needs a real enriched view with planted drivers, not a unit test). Build the FDR/recall calibration rig over the DAT-544 adversarial corpus + harness and verify separation holds on REAL data across ≥1 non-synthetic fixture (vertical-agnostic — not finance-specific).
- **Acceptance bars the spike established** (match these): strong driver (±60%) recall ≥ 0.9; independent-null FDR ≤ 2α (α=0.05), including a participating high-card dim; marginal-driver (±25%) power ≥ ~0.6 (the documented ≈±20–25% floor — NOT 90%; weak effects miss safely). ratio + stock target types separate too.
- **No threshold to calibrate across datasets** — both ranking (ordinal) and the noise gate (within-dataset permutation null) are self-calibrating. This is the structural difference from the cut `slice_variance`/`temporal_drift` detectors; the eval rig should confirm there is no global constant being tuned.

### dataraum-testdata
- The DAT-544 corpus (`make_corpus`: planted drivers at known effect sizes + independent nulls + a confounded proxy + measure-conditional missingness) seeds a generative family. Add **ratio** (numerator/denominator whose ratio depends on a driver, denominator varying independently) and **stock** fixtures, and a **confounded-dim** fixture (a proxy that is an 80% copy of the strongest driver) for the de-confounding check.

## 2026-06-17: DAT-537 — new `dimension_hierarchies` begin_session phase (g3 FD / drill-down / alias)

A new **deterministic** value-layer phase, `dimension_hierarchies`, runs in the
begin_session chain between `slicing` and `aggregation_lineage`
(`slicing → dimension_hierarchies → aggregation_lineage → correlations`). It
computes the g3 approximate-functional-dependency measure
(`g3(A→B) = 1 − COUNT(DISTINCT A)/COUNT(DISTINCT(A,B))`) over each fact's
grain-verified enriched view across the catalog's grain-safe `SliceDefinition`
dimensions (DAT-536), and writes drill-down hierarchies (`zip → city → state`) +
1:1 alias groups. **No LLM, no detectors** — it declares none in `pipeline.yaml`,
so it does not feed `session_detect` and changes no entropy/readiness measurement.

New run-versioned table **`dimension_hierarchies`** (form-a: `(signature, run_id)`
UNIQUE + upsert), sealed under the begin_session `(catalog,"catalog")` head; read
view `current_dimension_hierarchies` added (on `read_views._CATALOG_GRAIN`).
Net-new **`hierarchy` teach** type (`config_overlay` type='hierarchy', actions
add/reject/alias) — deterministic, so NO keeper-lift-up / witness pool (unlike
relationship teaches). Exposed on `GraphExecutionContext.dimension_hierarchies`;
the GraphAgent prompt does NOT yet consume it (that is **DAT-538**).

### dataraum-eval
- **Affects engine phases/tables**: new `dimension_hierarchies` phase + activity; new `dimension_hierarchies` table + `current_dimension_hierarchies` read view. Engine `schema.sql` + `schema_read.sql` regenerated; cockpit drizzle mirror regenerated. A begin_session run now persists hierarchy/alias rows for any fact whose grain-safe catalog has ≥2 related dimensions.
- **No measurement/threshold change**: the phase declares no detectors and touches no entropy/readiness/witness path, so existing calibration verdicts are unchanged. New surface to calibrate is **hierarchy/alias correctness** (geo `zip→city→state`, product→category chains; bidirectional-g3 aliases; the guards: constant dropped, ≤2-distinct/near-key rejected as determinant, low-support flagged `needs_confirmation`).
- **New teach surface**: `teach({type:"hierarchy", payload:{action, table_id, members}})` (cockpit `AGENT_TEACH_TYPES`).

### dataraum-testdata
- A fixture carrying a clean FD chain (`zip→city→state` or `product→subcategory→category`) plus a 1:1 alias pair (e.g. `state` ↔ `state_name`) and a violated/near-FD would exercise the discovery + guards directly.

## 2026-06-17: DAT-536 — slice materialization removed; witness substrate re-pointed inline; dimensional_entropy no longer runs

The slice sprawl is gone (ADR-0013 one-view model). The
`structural_reconciliation` witness of `temporal_behavior` (stock/flow) now gets
its per-(dimension-value, period) sums by **inline aggregation** — one
`GROUP BY dim, period` over each fact's enriched view in the `aggregation_lineage`
phase — instead of the materialized `slice_*` tables → `TemporalSliceAnalysis`
substrate. **Verdict-equivalent** (proven byte-identical per cell on the current
code before the cut; the witness verdicts are unchanged by construction).

Removed: the `slicing_view`, `slice_analysis`, `temporal_slice_analysis` phases +
their worker activities; the `temporal_slicing` module; `TemporalSliceAnalysis`
and `slicing_views` tables; per-value `slice_*` materialization; the slicing
agent's slice-SQL generation. `SliceDefinition` is now the **dimension catalog**:
`sql_template` dropped, `grain_safe` (Boolean) added.

**`dimensional_entropy` no longer runs.** It was already DEMOTED off the loss path
(2026-06-16 entry below — informative DirectSignal). Its only run site was the
`temporal_slice_analysis` phase, now removed, so it produces **no** EntropyObject
at all this run. Its formal removal (detector module + `expected_dependency` teach
+ any residual config) stays **DAT-539**.

### dataraum-eval
- **Affects engine phases/tables**: `slicing_view` / `slice_analysis` / `temporal_slice_analysis` phases removed; `slice_definitions` schema changed (`-sql_template`, `+grain_safe`); `slicing_views` + `temporal_slice_analyses` tables (and the `current_slicing_views` / `current_temporal_slice_analyses` read views) dropped. Engine `schema.sql` + `schema_read.sql` regenerated; cockpit drizzle mirror regenerated.
- **Re-point**: any eval driver/probe that asserts slice or temporal-slice tables, or reads a `dimensional_entropy` EntropyObject, must drop those assertions. The stock/flow witness behaviour on `detection-stockflow-events-v1` should be **unchanged** — re-pin it as the equivalence check (engine-side it's `tests/unit/analysis/lineage/test_processor.py`, DuckDB-fixture verdicts).
- **Calibrate**: no detector recall/precision change is intended (the reconciliation arithmetic is untouched; only its substrate path changed). `dimensional_entropy` disposition: now **not produced** in begin_session (was already `informative`); the DAT-539 cut finalizes it.
- **Status**: pending

## 2026-06-16: refactor — dimensional_entropy DEMOTED off the loss path (informative DirectSignal)

`dimensional_entropy` (cross-column NMI) was removed from `loss.yaml` (it had
`query_intent: {score: 0.3}`, `aggregation_intent: {score: 0.4}`). It now falls
through `_build_column_result` to a **DirectSignal** — the benford lane: the NMI
score + `expected_dependency` teach still compute as context, but no longer drive
intent readiness bands. The detector still runs (`temporal_slice_analysis` phase,
in slice) and still emits its `table:` EntropyObject; only its loss row is gone.

**Why** (eval Tier-1 falsification, `dataraum-eval` scripts/probes/dimensional-entropy):
on the loss path the NMI band is *anti-predictive* of wrong answers — highest on
CLEAN intrinsic structure (mutex/alias/FD, no wrong answer) and BLIND to the
dependency violation that causes a bad join/rollup (a 5%-broken FD barely moves
NMI, 0.862→0.799; the violation rate is owned by `derived_value` /
`relationship_entropy`). Recorded: eval `entropy_eval_architecture.md`.

### dataraum-eval
- **Changed**: `loss.yaml` (row removed + rationale comment), `tests/unit/entropy/views/test_readiness_context.py` (`test_table_target_rolls_up` now uses `dimension_coverage`; added `test_dimensional_entropy_is_a_direct_signal_not_a_band_driver`).
- **Affects**: any column×intent band that was driven *only* by dimensional_entropy now drops one band (it contributed query 0.3·NMI, agg 0.4·NMI). A `table:` whose only object is dimensional_entropy is no longer in `readiness.columns` — it's a `direct_signal`. Cockpit/readers that surface DirectSignals are unaffected; readers that assumed it banded need none, by design.
- **Calibrate**: eval-side `detector_coverage.yaml` disposition flips `scalar → informative` (mirrors benford); no recall/precision change (the NMI statistic is unchanged).
- **Status**: pending

## 2026-06-16: fix — operating_model_detect scored nothing (DAT-506 re-grain miss; surfaced by DAT-508)

`operating_model_detect` was the lone OM phase wired with a bare `RunRef` instead
of `OperatingModelScopedInput` (validation/cycles/metrics all take the scoped
input). So it called `run_detectors(run_id=om_run)` → `tables_for_run(om_run)` →
**empty** (the OM run never anchors `run_tables`; begin_session owns them) →
`detect_no_run_tables` warning → `cross_table_consistency` wrote **zero** entropy
objects. The DAT-508 eval run caught it: 4 `cross_table_consistency` recall
failures ("produced no score"), zero rows in `ws_<id>.entropy_objects`.

Fix (`worker/`): `run_detectors` takes an optional explicit `table_ids`;
`operating_model_detect` now takes `OperatingModelScopedInput` and passes
`payload.scope.table_ids` (the set PINNED at `operating_model_resolve`, ADR-0008,
exactly what validation/cycles/metrics read); the workflow passes `scoped`, not
`run`. add_source / begin_session detect are unchanged (`table_ids=None` →
`tables_for_run` as before). Eval impact: `cross_table_consistency` recall scores
again once the engine pointer includes this fix.

## 2026-06-15: DAT-506 — sessions leave the engine; manifest entry shape; head re-grain (BREAKS the eval driver → DAT-508)

The engine no longer models investigation sessions. The eval driver must be re-pointed (tracked as **DAT-508**). What changed for eval:

- **Entry shape is a flat MANIFEST (no identity envelope, no `session_id`/`source_id` on the wire):**
  - `AddSourceInput { workspace_id, sources[], verticals[] }` → `AddSourceResult { run_id, raw_table_ids[], tables[] }`
  - `BeginSessionInput { workspace_id, tables[], verticals[] }` → `BeginSessionResult { run_id, table_ids[] }`
  - `OperatingModelInput { workspace_id, verticals[] }` → `OperatingModelResult { run_id, validation_summary }`
  - `verticals[]` is by name (resolved engine-side via `VerticalLoader`); the engine born-loud guards `len > 1` (multi-vertical grounding not built); empty → `_adhoc`. The driver must pass the workspace's framed vertical (e.g. `["finance"]`) — `_adhoc` fails loud ("run frame first").
  - The engine MINTS `run_id` inside the workflow and RETURNS it; the run is identified by `run_id`. `import` is the only source-bearing activity (explicit arg from `sources[]`).
- **`investigation_sessions` / `investigation_steps` / `session_tables` tables are DELETED.** New `run_tables(run_id, table_id)`. Any eval seeding/asserting against those tables breaks — drop it. The driver no longer seeds an investigation_sessions row.
- **Version axis = per-table generation head + ONE workspace catalog head** (`metadata_snapshot_head`: `target="table:{id}"` stage `"generation"`; `target="catalog"` stages `"catalog"`/`"operating_model"`). All `session:{id}` head targets are gone. `current_*` read views carry NO `session_id` column — one row per entity at the workspace catalog head. The dual-grain discriminator renamed `via_session_head` → `via_catalog_head`. `current_entropy_readiness` precedence is catalog-vs-operating_model (in the view).
- **`PhaseContext.source_id`/`.session_id` deleted** (DAT-426 folded in). All `session_id` metadata columns dropped; `sql_snippets`/`snippet_usage` re-keyed `session_id` → `workspace_id`. `run_id` is NOT NULL on run-stamped tables; `ondelete=CASCADE` dropped from `columns`/`tables` run-stamped children (GC is DAT-507).
- **Eval coverage ask (see DAT-508 comment):** include a dataset that produces a competing **operating_model** `entropy_readiness` band so the catalog-vs-OM precedence clause gets real-data coverage — the DAT-506 live smoke (finance/2-table) produced zero OM-band readiness rows, so that path is only unit-tested. Possibly related to DAT-515 (entropy objects under-promoted to the read view).
- **No detector behavior changed** — this is a contract/persistence re-grain, not a measurement change; recall/precision baselines should hold once the driver speaks the manifest. testdata unaffected.

## 2026-06-15: slicing agent grounds recommendations — no empty-FK crash (fix)

The slicing agent built a `SliceRecommendation` with `column_id=''` whenever the
LLM's recommended column could not be resolved in this run's context (a
hallucination, or a cross-run enriched-view shape change — a fact's dimension
join drops to a passthrough view on a re-run, so its `fk__dim` columns vanish).
That empty id is a guaranteed FK violation on `slice_definitions` →
`PhaseFailed` → the whole begin_session crashes. Surfaced on a DAT-473 teach
re-run: `account_id__account_type` recommended for a now-passthrough
journal_lines view. Fix (`analysis/slicing/agent.py::_convert_output_to_result`):
drop — with a `slice_recommendation_ungrounded` warning — any recommendation
that does not ground to a real `table_id` + `column_id`, mirroring the existing
time-axis validation (the propagation path already guarded empty FK ids). Eval
impact: begin_session re-runs (every teach-and-rerun closure) no longer crash on
an ungroundable slice recommendation. NOTE (separate, not fixed here): the
*reason* journal_lines lost its account dimension join across runs is
enriched-view / relationship-discovery nondeterminism — a deeper determinism
question; this fix makes slicing robust to it rather than papering it over.

## 2026-06-15: slice_conditional_null detector — nulls concentrated in a slice (DAT-473)

New value-layer, column-scoped detector `slice_conditional_null` (declared in
`pipeline.yaml` under the `statistics` phase, so it runs at the terminal
add_source detect with the typed table in scope). The dataset-level
`null_ratio` is a single fraction; this reads whether those nulls *concentrate*
in particular slices of a sibling categorical (a 60%-null cost center hiding
behind a 5% overall rate, silently biasing that slice's aggregates).

- **Statistic:** `stats.cramers_v` — bias-corrected Cramér's V (Bergsma) on the
  2×K `(value IS NULL) × slice` contingency under the **Cochran validity rule**
  (any expected cell < 5 → abstain, returns `None`). Grounded in the DAT-473
  kill gate; the pure function is pinned both here (`test_stats.py::TestCramersV`)
  and in eval (`test_slice_null_gate.py`) — one statistic, two guards.
- **Slice dimensions:** each sibling low-cardinality categorical (identifiers
  excluded by name / near-unique cardinality; the *actual scanned* distinct
  count on slice-labelled rows is the authoritative 2..50 gate, so a missing
  profile never silently drops a column). Score = MAX V over valid slices; 0.0
  when the column has no nulls, missingness is MCAR, or no slice yields a valid
  table. Per-column VALUE/NULLS → rolls into the column's band beside null_ratio.
- **Teach (closure):** reuses the EXISTING `expected_dependency` overlay (the
  `document_business_rule` archetype `dimensional_entropy` already reads via
  `load_documented_dependencies`). Documenting `{target_column, slice_column}`
  marks the conditional missingness expected → that pair is excluded → the
  score drops. Closure pinned by
  `test_slice_conditional_null_detector::test_document_business_rule_teach_closes_the_score`.
- **Loss:** `loss.yaml` row `slice_conditional_null` (query 0.4 / aggregation
  0.7 / reporting 0.6 — PLACEHOLDER, calibrated:false; recall is separation
  from clean, not a tuned point). Replaces the DAT-473 deferral note left on the
  cut `slice_variance` block.

Eval implications: a new strategy injection family (slice-conditional nulls)
drives recall (injected > clean + margin), and the teach closes it via an
`expected_dependency` overlay on the (column, slice) pair. BUILTIN_DETECTORS is
now 16 (value layer 4); the no-orphan / registry guards are updated.


---

Older entries (2026-06-11 and earlier — the DAT-442 / value-layer / DAT-506 saga) are in
[`handoff-archive.md`](handoff-archive.md).
