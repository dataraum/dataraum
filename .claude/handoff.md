# Engine → eval/testdata handoff

Bridge for `dataraum-eval` (calibration) and `dataraum-testdata`. One entry per
change that affects a detector, pipeline phase, or a response shape eval consumes.

---

## DAT-695 — join coverage as judge evidence + driver routing fixes

**Branch:** `feat/dat-695-coverage-and-routing`. Root cause of "driver rankings
empty over surrogate-joined dims" on the bookkeeping smoke corpus.

### What changed

- **New measured signal `coverage`** (`relationships/evaluator.py::compute_join_coverage`
  — share of fact rows with a non-NULL key that find a dim match). Multiplicity
  proves a key's SHAPE; coverage proves it is USED — the corpus's customer/vendor
  dims verified many-to-one at **0.27% / ~0% coverage** (independently generated
  lookalike tables). Coverage now rides: the composite-rescue hint + the
  `semantic_per_table` prompt (decline lookalikes), the minted relationship's
  evidence (`evidence.coverage`, low-coverage warning log), and the enrichment
  feed (`[matches 0.3% of fact rows]` next to the grain marker). **Evidence for
  the LLM judges — no numeric gate**; the slicing null-filter stays the floor.
- **Driver routing** (`drivers/processor.py`): (1) a dim SATURATED against its
  home entity (distinct == entity count, constant within) is a 1:1 alias of the
  key and is dropped — it fabricated a guaranteed-empty headline family
  (`business_id ↔ created_user`); (2) the headline family must carry content —
  first non-empty family in precedence order wins, but never past the row
  family (the DAT-561 low-ICC demotion is preserved, pinned by the existing
  `test_entity_constant_dim_never_enters_row_wise_primary`).

### Calibration to run

- Driver-discovery recall/precision on the calibration corpora — the routing
  changes alter WHICH family headlines (alias dims no longer home; empty
  high-ICC families no longer block the row family). The DAT-561/563 grain
  contracts are pinned by the existing suites, but ranked-dimension sets can
  legitimately shift on corpora with tenant-alias structures.
- Relationship confirmation on composite corpora — the LLM now sees coverage in
  the hint; expect DECLINES of lookalike composites (a recall drop there is the
  intended behavior, not a regression).

### testdata hints

The driver-over-surrogate acceptance (epic DAT-652) needs a corpus with REAL
referential coverage: a fact whose composite `(name, tenant)` FK actually
resolves against its dimensions (coverage ≥ ~0.9), dim attributes that drive the
measure, plus a LOOKALIKE negative (name pools overlap, coverage ≪ 1) that the
judge must now decline. The bookkeeping smoke corpus structurally cannot serve
this: only its payment_method dim is genuinely referenced. Pairs with the
DAT-679 fixture work (greedy-search miss-rate corpus).

---

## DAT-672 — drill keystone; `column_mappings` removed end-to-end (PR #438, merged 2026-07-06)

**For dataraum-eval:**
- **`sql_snippets.column_mappings` no longer exists** (column dropped; LLM output field, `GeneratedCode`, reuse-merge and persist paths all removed). Any eval strategy reading it gets `UndefinedColumn` — switch to `provenance.column_mappings_basis` (`{concept: {column, filter, resolution}}`), which is the prompted, populated per-concept grounding record. The flat field had been silently empty since DAT-636 dropped its prompt teaching (`default_factory=dict` masked it).
- No other engine response/pipeline shape changed; the rest of the PR is cockpit-side (drill tiers A/B/C over the promoted surface — read-only).

**Testdata note:** BookSQL's COA has **zero COGS-type and zero inventory accounts**, so gross-profit-family metrics can never execute there (honest NULL extracts) — don't read that as a grounding regression; realistic executed ceiling on BookSQL ≈ dso + current_ratio.

---

## DAT-603 — graph agent: single-extract output schema + adaptive thinking (PRs #434, merged 2026-07-03)

**Re-baseline `graph_sql_generation` before trusting comparisons against the DAT-602 eval baseline.** Three changes eval must know about:

1. **Output schema replaced.** `GraphSQLGenerationOutput` (summary/steps[]/final_sql) is gone; the tool now takes `ExtractGroundingOutput`: `grounding` (evidence commitment, FIRST field), `sql`, `description`, `assumptions`, `provenance` (no more `llm_reasoning`; `column_mappings` was removed by DAT-672 — see its entry). The agent binds the SQL to the graph's own leaf id — snippet `step_id`s always equal catalogue step ids now (the DAT-664 paraphrase class is structurally gone). `validation_sql`'s schema also lost its unread `explanation` field.
2. **Adaptive thinking ON for this label** (`thinking: true` in `llm/config.yaml`), with `tool_choice: auto` + `disable_parallel_tool_use`. Latency/token profile shifted: measured 763 → ~3,726 mean output tokens/call (thinking billed as output), ~10s → ~35s/call, absorbed by the 10-wide fan-out. Any eval latency/cost assertions on this label need new baselines.
3. **Prompt v6.1** (floors-not-scripts rewrite) — grounding QUALITY improved on the finance fresh-wipe smoke: revenue grounds via the complete `account_type` classification and matches `ground_truth.yaml` exactly (51,766,199.72); cogs+opex+depreciation match `total_expenses` to the cent; 22/34 executed (prior fresh runs: 21 and 19). Value-level GT comparison is now part of the grounding smoke — distribution parity alone masked a 48% revenue error in pre-rework sampling.

**Testdata note:** `trial_balance.csv` carries trailing periods (2026-01/02) with only partial accounts (no AR rows) — an extract grounding AR at `period = MAX(period)` honestly NULLs. Consider whether the generator should emit complete trailing periods.

---

## DAT-277 — composite keys cured by surrogate-key mint (supersedes the parked rescue)

**Branch:** `worktree-dat-277-surrogate-keys`. Replaces the parked
`refactor/dat-277-composite-key-rescue` design (multi-column ON) — the catalog
now only ever sees single-column FKs.

### What changed (pipeline shape + relationship catalog)

- **New begin_session phase `surrogate_mint`** (between
  `session_materialize_overlays` and `enriched_views`; deterministic, no LLM).
  The session chain is now 14 phases.
- **Detection**: the greedy composite rescue (`relationships/composite.py`,
  ported from the parked branch, same 9-case math matrix) probes each
  fan-out candidate; a hit attaches a `composite_key` hint to the
  `semantic_per_table` candidate feed. **The LLM stays the sole judge** and
  confirms via the new `RelationshipOutput.key_columns`.
- **A confirmed composite NEVER persists as a plain llm relationship.** It
  becomes one `surrogate_key_intents` row (new table, catalogue-grain,
  `(run_id, intent_digest)` upsert); the mint phase then re-materializes BOTH
  typed tables with a deterministic hash column
  (`_sk__<components>`, `md5(a::VARCHAR || '|' || b …)` — **NULL-propagating**,
  deliberately NOT dbt's coalesce placeholder: any NULL component → NULL
  surrogate → LEFT JOIN misses, FK semantics) and persists ONE ordinary
  single-column `llm` relationship on the surrogate pair (empirical
  cardinality + `introduces_duplicates` + RI in evidence, plus
  `evidence.surrogate.natural_pairs` provenance).
- **Typed tables can now carry engine-minted `_sk__*` columns** — profiled
  (`StatisticalProfile`, layer `typed`), stable `column_id` across runs
  (upsert by `(table_id, column_name)`), reconciled by the mint (dropped when
  no longer confirmed nor keeper-kept). Downstream consumers see them as
  ordinary VARCHAR columns.
- Enriched views / drivers / grounding / cycles / validation: **no code
  change** — they consume the surrogate relationship through the existing
  single-column machinery.

### Calibration to run

- **Relationship detection / FK confirmation** on composite-key datasets: the
  fan-trap edge (a transactions↔chart-of-accounts pair scoped by tenant) should
  now surface as ONE stable many-to-one surrogate relationship instead of a
  flaky/degenerate many-to-many + 20 tenant-key candidates. Watch single-key
  datasets for regressions — with no composite hint the pipeline is
  byte-identical (worst case = no mint; abstain at detection, judge, and mint).
- **Enriched-view grain + metric grounding behind the composite**: the
  previously fan-trapped dimension becomes joinable; metrics should ground on
  the real discriminator (`account_type`), not single-table proxies
  (`transaction_type` at 0.35). DAT-652's acceptance case (non-empty driver
  rankings on the bookkeeping smoke corpus) is the headline check.
- **The bookkeeping smoke corpus becomes a legitimate grounding oracle once this lands** — the
  standing "don't use it as the acceptance oracle" caveat retires.

### testdata hints

The canonical injection is unchanged from the parked branch's handoff: a fact
whose FK recurs across a tenant/scope partition, dimension keyed on the
composite; single-column join fans out, composite holds grain. The negative — a
genuine bridge/junction m2m — must ABSTAIN (no intent, no mint, flagged
fan-trap). New assertable surface: the `_sk__*` columns themselves (both
tables), the `surrogate_key_intents` row, and the surrogate relationship's
`evidence.surrogate.natural_pairs`.

### Validated live on the bookkeeping smoke corpus (2026-07-03, full 7-table set, real LLM)

Four composites minted (`(name, business_id)` for customer/vendor/
payment_method/product_service), all persisted fact→dim many-to-one,
`introduces_duplicates=false`; `enriched_master_txn_table` grain-verified over
the 810k-row fact with 11 dim columns joined via the surrogates; the flaky
20-candidate `business_id` degeneracy is gone. revenue grounds
on `account_type='Income'` (the real classification, not transaction_type).
Two smoke-corpus DATA truths the platform now states instead of absorbing:
`chart_of_account_OB`'s `(account, business)` collisions are 82 exact duplicate
rows PLUS 135 dual-role accounts (same name+full name, DIFFERENT account type —
Installation as both Income and Expenses in one business), so no name-based
composite is a key there and **dedup cannot fix it** (the true key would need
`account_type`, which the fact doesn't carry) — the confirmed composite was
REFUSED (non-collapsing gate), the anchor persists m2m + fan-trap-flagged, and
the semi-join grounding pattern (`account IN (SELECT … WHERE account_type=…)`)
is the correct end-state consumption. And the corpus has NO COGS account type, so
`cost_of_goods_sold` is honestly inconclusive ("filter matched no rows"), never
a transaction_type proxy. Eval should treat both as expected corpus baseline,
not regressions.

---

## DAT-654 — SQL canonicalization on DuckDB `json_serialize_sql` (retire sqlglot)

**Branch:** `feat/dat-654-engine-json-serialize`. **No calibration action required.**

Pure refactor of the two engine SQL consumers off `sqlglot` onto DuckDB's own
`json_serialize_sql` parser (matching the cockpit, PR #416): `core/sql_normalize.py`
(the enriched-view recipe-version gate) and `entropy/measurements/derived_value.py::parse_formula`
(the `derived_value` detector's formula → `CanonicalFormula` witness). Output is
proven **byte-identical** to the old sqlglot logic by the pre-existing, **unchanged**
`test_measurement_derived_value.py` suite (every `identity`/`operation`/`operands`
case still passes) plus the enriched-view integration gate. No detector inputs,
scores, thresholds, or response shapes change → **recall/precision unaffected; do
not recalibrate.** The only watch item is nil: `parse_formula` returns the same
`CanonicalFormula` on every supported/unsupported shape.

---

## DAT-631 — metric grounding: teach the agent to down-rank blocked columns

**Branch:** `feat/dat-631-grounding-quality`. **Re-verify metric grounding confidence; no schema/field change.**

Prompt-only (`graph_sql_generation` → v5.1). A new `<column_reliability>` block: when the agent grounds a concept on a `⛔ blocked` column (readiness flagged it unreliable), it now MUST record an inferred assumption + set confidence LOW (≤0.4) — mirroring the existing `<data_trust>` pattern. Previously the agent saw the `⛔` marker but had no instruction, so it summed blocked columns at ~0.5 and the metric read confidently green. The LOW confidence feeds the existing DAT-631 gate (`metrics_phase._low_confidence_reason`, floor 0.5) → the metric flags low-confidence-executed instead.

**Why eval cares (RE-VERIFY):** grounding confidence shifts — metrics resting on blocked columns now flag low-confidence rather than plain executed. Expect MORE low-confidence flags (intended honesty, not a regression). No detector surface, no new fields; it's grounding quality. It's informative/interpretable (a blocked column may still be the right measure), so the agent may still ground on it — just at lower confidence.

_(The double-count half of DAT-631 Problem 2 — a concept-boundary prompt + a deterministic overlap flag — was explored and DROPPED: value-set overlap is a symptom needing interpretation, not a deterministic per-metric signal; the real fix is interpretive-at-compose or a global vocabulary invariant, deferred under DAT-652.)_

---

## DAT-647 — unit detection split into two grain-strict detectors

**Branch:** `fix/dat-647-split-unit-detectors`. **Re-calibrate unit recall + confirm the currency-measure false-block is gone.**

### What changed (detector split — the DAT-637 unit migration, finished)
`unit_entropy` conflated two unit questions at the add_source grain, so every
currency measure (unit defined by a sibling `currency` column, catalogue grain)
read `missing → 1.0 → 0.8 agg → blocked`, capping metric-grounding confidence.
Now split by grain:
- **`unit_entropy`** (unchanged phase: `semantic_per_column`, add_source) — scores the
  **value-carried** unit only (`typing.detected_unit`): `1 − unit_confidence` when a
  unit token is in the VALUES, **abstain `0.0` (`no_value_unit`)** when there is none.
  It no longer reads `unit_source_column` and no longer emits `missing=1.0`.
- **`unit_source`** (NEW, `semantic_per_table`, **session detect only**) — reads
  `ColumnConcept.unit_source_column`: `0.0` when resolved (`resolved_from_dimension` /
  `dimensionless`), `1.0` (`unresolved`) when a MEASURE has no determinable unit
  source. This is the aggregation-safety block. `loss.yaml` row: agg 0.8 / reporting
  0.6 (inherits the old block); `unit_entropy` keeps agg 0.8 / reporting 0.6 for
  value-carried ambiguity. Readiness MAX-combines the two per column.
- **Context feed:** `semantic_per_table` now gets `detected_unit` (rendered
  `value_unit=<u>`) + `unit_from_concept` (was dropped by `format_concepts_for_prompt`).
  Prompt `semantic_per_table` → **v2.0.0**: author `unit_source_column` for every
  measure (self when value-carried, sibling `currency` via `unit_from_concept`, else
  `dimensionless`).

### Why eval cares (calibration to run)
- **Currency-measure false-block is fixed** — VERIFIED on a fresh clean run
  (2026-07-01): `journal_lines.debit/credit/net_amount` go `blocked(0.8) → ready`,
  both unit detectors score `0.0`, and no metric carries the unit/⛔ low-confidence
  reason. Re-confirm on the finance-clean corpus.
- **`unit_source` recall:** a measure with a genuinely undeterminable unit (no value
  token, no currency/dimension source, not dimensionless) should still band
  `blocked` for aggregation. A measure with a `currency` sibling should band `ready`.
- **Teach-closure:** the value-carried unit teach (`unit` → `detected_unit`) still
  closes `unit_entropy`; the concept-level teach (`unit_from_concept` / `rebind`)
  steers `unit_source` via the re-wired context. The eval harness reads
  `entropy_objects` directly, so recall calibration sees `unit_source` regardless of
  the readiness-view grain.

### New fields / thresholds
- New detector id `unit_source`; new `SubDimension.UNIT_SOURCE`
  (`semantic.units.unit_source`). `unit_entropy` evidence `unit_status` values are now
  `declared` / `low_confidence` / `no_value_unit` (dropped `missing` /
  `inferred_from_dimension` / `dimensionless`). No score-threshold change.

### Known follow-ups (NOT in this branch)
- **Cockpit read-grain (deferred lane):** `unit_source` writes at the catalogue grain,
  but the automated grounding-teach loop reads add_source-grain readiness
  (`grounding-readiness.ts` `viaTableHead`). Until moved to include the catalogue
  head, the loop won't see `unit_source` (teach-closure via the loop; inspect tools
  already show it via catalogue-supersedes). Product-surface only.
- **Value-carried-unit determinism gap:** a measure carrying its OWN unit token with
  NO currency column relies on the LLM setting `unit_source_column=self` (prompt
  v2.0.0). Deterministic on the finance corpus (currency-sourced); the LLM-gated edge
  is flagged for a follow-up decision (make `unit_source` read `detected_unit`
  deterministically vs. keep the strict grain split).
- Separate, pre-existing: a `fiscal_period_integrity` validation WARNING caps journal
  metric confidence at 0.25 on clean — unrelated to units.

---

## DAT-617 — validation verdict on demand from a contracted SQL output (ADR-0017)

**Branch:** `feat/dat-617-validation-on-demand-p1`. **Re-verify `cross_table_consistency`; do NOT recalibrate blind.**

### What changed
The validation pass/fail VERDICT is no longer stored — it is recomputed on demand
by re-running the run-versioned `sql_used` (a stored verdict goes stale on
re-import, the SQL doesn't). Two coupled moves:

1. **Contracted SQL output (prompt `validation_sql` → v2.0.0).** Every check now
   returns ONE row with a non-negative `deviation` (0 = clean) + `magnitude`. The
   judgement collapses to the uniform `deviation <= tolerance`
   (`analysis/validation/evaluate.py::_judge`). The old per-`check_type` result
   shapes (`difference`/`equation_holds`/`orphan_rate`/`total_rows`/… + the
   column-name string-matching) are **deleted**.
2. **`validation_results` is a pure SQL store.** Dropped: the data-derived verdict
   (`status`, `passed`, `message`, `details`) AND the declared params (`severity`,
   `tolerance`). Kept: `sql_used` + `columns_used` + ids. The detector reads the
   run's vertical from a validation `lifecycle_artifacts` `teaches` row (its shared
   session) and loads the spec for `severity`/`tolerance`.

### Why eval cares (RE-VERIFY, not recalibrate)
- **The `cross_table_consistency` detector changed inputs.** It now re-runs
  `sql_used` and reads `verdict.details` `deviation`/`magnitude` (uniform), not the
  old per-check_type `details` keys. The SCORE math is unchanged — non-critical =
  `min(1, deviation/magnitude)`, critical-failed = categorical 1.0, passed/inconclusive
  = 0.0. If the LLM's contracted SQL computes the same numbers the old free-form SQL
  did, the detector's recall/precision is **unchanged** → confirm with the
  `cross_table_consistency` calibration; the thing to watch is whether the new
  `deviation` differs numerically from the old `difference`/rate.
- **The `validation_sql` prompt changed** → any eval fixture asserting a fixed
  validation SQL string/shape will diverge; update to the `deviation`/`magnitude`
  contract. A check whose authored SQL doesn't return `deviation` now reads
  **inconclusive** (ERROR), never FAILED.
- **Migration edge — re-run before calibrating:** `sql_used` rows authored under
  the **v1.0.0** prompt return the OLD per-check_type shape (no `deviation`
  column), so on-demand re-evaluation scores them **inconclusive (0.0)** until the
  validation phase re-runs under v2.0.0. A calibration harness seeded from a
  pre-change DB must re-run the operating_model validation phase first, or it will
  see every validation score 0.0.
- **`graphs/context` severity now comes from the spec** (not the dropped column),
  and a validation whose spec was removed from config since the run is silently
  omitted from the metric-agent context (it is no longer a current validation).
- **Schema:** `validation_results` dropped 4 columns + added `tolerance` →
  `schema.sql` re-dumped; the cockpit drizzle mirror re-pull + the `look-validation`
  rewire is the remaining cross-package step (docker-gated, not in this branch yet).

### Thresholds / new fields
No score threshold changed. `validation_results` is now a pure SQL store:
`-status,-passed,-message,-details,-severity`. The verdict + declared params are
never stored — recomputed / read from config on demand.

---

## DAT-651 — validation phase parallelized (latency only)

**Branch:** `feat/dat-651-parallel-validation`. **No calibration action required.**

The validation phase's per-spec loop (bind LLM + EXPLAIN, then execute) now fans across a bounded `ThreadPoolExecutor` (per-worker `manager.duckdb_cursor()`; session mutations applied serially on the main thread after the pool joins). **Pure latency refactor — identical observable output**: same lifecycle states, same `validation_results`, same order. No new fields, no threshold/format change. Eval should see no diff in validation outcomes; if it does, that's a regression to flag.

---

## DAT-630 — ground the business_cycles agent (context + prompts, no deterministic path)

**Branch:** `feat/dat-630-cycle-grounding`.

### What changed (business_cycles detection — better context + prompt + a guardrail; the LLM still authors)
The cycle agent missed cycles that complete on a NUMERIC condition (a ledger that balances) because it was served status columns only. Four moves, no deterministic detector:
- **Context feed** (`analysis/cycles/context.py`): the cycle agent now gets (a) arithmetic `DerivedColumn` relationships (`sum`/`difference`/`product`/`ratio`, run-scoped to `relationship_run_id`, fail-closed) as numeric-completion signals, and (b) semantic field mappings via the **same** `graphs/field_mapping.load_semantic_mappings` the metric agent uses. Slice value-counts are now read run-scoped to the table's generation head (`base_runs.semantic_runs`), fail-closed.
- **Prompt** (`dataraum-config/llm/prompts/business_cycles.yaml` → v2.0.0): a first-class numeric-completion path alongside status-completion + a grounding-discipline block (cite only served references, ground via mappings, abstain rather than force-fit, honest confidence).
- **Membership floor** (`analysis/cycles/verify.py`, new): drops any detected cycle citing a column/value not in the served context — a guardrail on the agent, not a re-detector (never re-derives a rate).
- **Confidence gate** (`pipeline/phases/business_cycles_phase.py`): a measured cycle below 0.5 confidence still reaches `executed` but is flagged in `state_reason` (mirrors `metrics_phase._low_confidence_reason`); new `low_confidence` output tally.

Validation surface deliberately deferred to a follow-up.

### Calibration to run
- **Cycle detection on the cycle-relevant scenarios** (`month_end_close`, `multi_system_recon`, `erp_migration` in dataraum-testdata): confirm `journal_entry_cycle`/`period_close` now detect when a numeric completion signal (a balancing derived relationship) is present, and still honestly abstain when none is — the key acceptance check. No regression on cycles that already detected via status columns.
- Confirm the membership floor produces no false rejects on real detections (a dropped cycle reads as "not detected").

### Thresholds / new fields
- New low-confidence floor `_LOW_CONFIDENCE_FLOOR = 0.5` in the cycles phase (mirrors metrics). No DB schema change (`confidence`/`state_reason` already exist). New phase output key `low_confidence`.

### testdata hints
- The numeric-completion path needs a scenario where a GL/ledger balances (debit/credit net, or a reconciliation ties out) so the correlations phase emits a `difference`/`ratio` `DerivedColumn` for the cycle agent to ground on — a GL **without** a lifecycle status column is exactly the gap this closes.

---

## DAT-646 — formula SQL is composed + persisted PER-METRIC (kills cross-metric aliasing)

**Branch:** `refactor/dat-646-formula-identity`.

### What changed (metric SQL composition + snippet persistence — NOT a new response shape)
The metrics phase warms only leaf EXTRACTs now; a metric's FORMULA/CONSTANT SQL is
composed **per-metric** from the DAG (`graphs/agent.py` `_compose_metric_from_dag`),
never warmed or shared by expression shape. The bug this fixes: formula snippets were
deduped by `normalize_expression`, so same-shape metrics collided — `ebitda/revenue`,
`net_income/revenue`, `operating_income/revenue` all normalize to `{A}/{B}` and aliased
to ONE snippet, attributed to whichever metric authored first. The losers either reused
the wrong numerator's SQL or were left un-composable.
- Composition is now deterministic per-metric: each step is a CTE in topo order
  (`compose_formula_sql`/`compose_constant_sql`), so `net_margin` references `net_income`
  and `ebitda_margin` references `ebitda` — provably distinct.
- Persistence: formula/constant snippets are saved per-metric in `assemble`
  (`_save_composed_snippets`), sourced to `graph:{graph_id}` and keyed per-source, not by
  shape. `find_by_expression` (the shape lookup) is deleted.

### Why eval cares (calibration to run)
- **The margin family should now EXECUTE with CORRECT, DISTINCT values.** Before, same-
  shape margins aliased → a margin could compute another margin's numerator over revenue,
  or fail. Re-run finance metric grounding/execution calibration; focus on
  `gross_margin` / `ebitda_margin` / `net_margin` / `operating_margin` — expect each to
  reach `executed` with its OWN value, and aliasing-induced wrong/crashing margins to
  disappear. Net: more correct margins, fewer ungroundable/wrong ones.
- **No threshold or response-field change.** The metric output shape (value, assumptions,
  state/reason) is unchanged; only the composed `final_sql` and the snippet KB rows
  differ. A metric's numeric value may CHANGE where it was previously aliased to the wrong
  SQL — that is the fix, not a regression; update any fixed-SQL/value snapshots for the
  margin metrics.

### Snippet KB shape (if eval inspects `sql_snippets`)
Formula snippets are now **one row per metric** (sourced `graph:{graph_id}`, sql = the
whole standalone composition), not one shape-shared row. Extract/constant snippets are
unchanged (concept- / param-keyed, shared). No schema/column change.

---

## DAT-645 — vertical sign conventions wired into grounding + validation

**Branch:** `feat/dat-645-vertical-conventions`.

### What changed (grounding INPUT, not a new response shape)
The finance ontology now declares a `conventions.sign_natural_balance` block
(`verticals/finance/ontology.yaml`) stating that measures are expressed in their
natural-balance direction (credit-normal = credit−debit, debit-normal = debit−credit)
so they read positive. The engine pipes this verbatim into BOTH SQL-authoring agents:
- **extraction** (`graphs/context.py` → `graphs/agent.py` `_generate_sql` → the
  `graph_sql_generation` prompt's new `{vertical_conventions}` slot), and
- **validation** (`validation_phase.py` → `validation/agent.py` → the `validation_sql`
  prompt's new `{conventions}` slot).
The engine stays domain-agnostic — it routes an opaque string; only the vertical YAML
holds credit/debit vocabulary.

### Why eval cares (calibration to run)
- **Profitability tree should now GROUND and EXECUTE.** Before, `revenue` grounded with
  a non-deterministic sign (often `SUM(debit)−SUM(credit)` = negative) and failed its
  declared `value > 0`, cascading 8 dependent metrics to ungroundable. With the sign
  convention fed in, `revenue` (and other credit-normal measures) should ground positive
  and the gross_profit/margin/ebitda/net_income tree should reach `executed`. Re-run the
  finance grounding calibration; expect MORE metrics executed, not fewer.
- **`sign_conventions` validation SQL changed framing.** It no longer declares its own
  `credit_normal_types`/`debit_normal_types` lists or expects `revenue ≤ 0` (net-debit).
  It now consumes the shared convention and checks **natural balance ≥ 0**. If eval holds
  a fixed ground-truth SQL/snapshot for `sign_conventions`, it will diverge — update it.
  The pass/fail outcome on clean data is unchanged (still ~0 violations).

### Thresholds / new fields
None. No score thresholds, no new stored response fields — this changes LLM prompt
INPUT (an extra `<domain_conventions>` block), not engine output shape.

### testdata hints
Any finance fixture exercising the profitability tree is the regression: revenue should
ground positive and the margin metrics should execute. A vertical without a `conventions`
block is unaffected (the block renders empty).

---

## DAT-643 — formula/constant authoring is fully deterministic (shadow + LLM fallback retired)

**Branch:** `refactor/dat-643-retire-shadow`.

### What changed (run behavior, NOT a detector/response shape)
Metric grounding's formula/constant path no longer touches the LLM at all. DAT-636
had already made `formula_composer` the primary author but kept the LLM running as a
comparison **shadow** and left a whole-graph **fallback** for a formula whose deps
weren't cached. Both are deleted (`graphs/agent.py`): `execute()` now branches on the
authored node's type — FORMULA/CONSTANT compose deterministically via
`_compose_grounding_free` (born-loud `Result.fail` on a missing dep / unresolved
constant / malformed expression), EXTRACT is the sole LLM authoring surface.
`_generate_sql` is extract-only and the `graph_formula_composition` prompt is removed.

### Calibration to run
**None — calibration-neutral.** No detector, threshold, phase output, or response
field changed. Persisted snippets on the happy path are byte-identical (the
deterministic composer was already the source of truth). The only behavioral delta is
on the FAILURE path: a key mismatch between warm-mint and per-metric lookup now
honest-fails the formula instead of an LLM re-deriving a shared extract — so grounding
becomes *more* deterministic, never less. Recall/precision cannot regress.

### testdata hints
None. The natural regression is the finance-clean profitability tree: `revenue`
authored exactly once (one `graph_sql_generation` dump), `gross_profit`/`gross_margin`/
`ebitda` composed deterministically over it — no per-formula re-authoring.

---

## DAT-641 — concurrent-typing DuckLake commit conflict is now Temporal-retryable

**Branch:** `worktree-dat-641`.

### What changed (run behavior, NOT a detector/response shape)
The typing phase's "all tables failed" failure message now **folds in the per-table
error detail** (`typing_phase.py`, mirroring statistics/correlations_phase) instead
of the bare `"No tables were successfully typed"`. That surfaces a DuckLake
optimistic-commit conflict signature into `PhaseRun.error`, where the worker's
`_is_transient_commit_conflict` classifier (already present, DAT-641 part 1 +
`ducklake_max_retry_count` bump) turns it into a **retryable** `TransientPhaseFailure`
rather than a fatal `PhaseFailed`. Net effect: a wide concurrent replay (≥~20 tables
fanned out) that lost a commit race used to fail the whole run; it now retries the
losing table activity and completes.

### Calibration to run
**None — calibration-neutral.** No detector logic, threshold, or output shape
changed; this only affects the FAILURE path (a previously-fatal transient race now
retries to success). Recall/precision cannot move. If anything it removes spurious
run failures from wide-replay eval scenarios.

### testdata hints
A wide multi-table replay (≥~20 tables typed concurrently) is the natural regression
that used to trip the commit race — it should now complete without a fatal
`PhaseFailed: No tables were successfully typed`.

---

## DAT-639 — narrow, workspace-unique table names (no `src_<digest>__` prefix)

**Branch:** `fix/dat-639-narrow-table-identity`.

### What changed (response shape)
Physical raw/typed/quarantine table names are now **NARROW and workspace-unique**
— the file stem / recipe name, sanitized, with **no `src_<digest>__` (or `raw_`)
source prefix**. `Table.table_name` and `Table.duckdb_path` both store the bare
narrow name (e.g. `orders`, not `src_abc…__orders`). The per-workspace DuckLake
catalog is the namespace; `Table` uniqueness is now `(table_name, layer)`
(`uq_table_name_layer`), not source-scoped. (Completes DAT-506 into physical
naming.)

### Engine routes / phases affected
- `pipeline/phases/import_phase.py` — loaders compose the narrow name via the new
  `sources.base.raw_table_name_for_uri`; db recipe extract uses `raw_prefix=""`.
  New **pre-flight collision guard** (`_first_name_collision`): importing a source
  whose narrow table name is already owned by a **different** source now **FAILS
  LOUD** ("retire that source first") instead of silently materializing a parallel
  table. Same-source re-import still replays (upload: `should_skip`; db recipe:
  recipe-hash teardown).
- `pipeline/phases/typing_phase.py` — unit-override teaches key on the bare
  `<table>.<column>` only (the dual qualified/de-prefixed lookup is gone).
- `entropy/detectors/computational/cross_table_consistency.py` — **detector
  change**: `_own_columns_used` now matches a validation check's `columns_used`
  `"table.column"` refs by **exact narrow table name**. The `src_<digest>__`
  prefix-strip fallback is deleted.

### What eval must do
- **Any ground truth / fixture that references a physical table name must use the
  NARROW form** (`orders`, not `src_<digest>__orders` and not `<source>__orders`).
  This includes: unit-teach keys (`overrides.units` → `"<table>.<column>"`), the
  validation phase's `columns_used` refs, and any assertion on `Table.table_name`
  / `duckdb_path` / enriched view names (`enriched_<table>`).
- **Re-seeding the same content under a new source name now FAILS** (collision
  fail-loud + `uq_table_name_layer`). Calibration/smoke harnesses that re-add the
  same files each run must either reuse a stable content-keyed source id (so it
  replays) or use distinct table names — a fresh random `source_<uuid>` re-import
  of the same files will be rejected, not duplicated. (This is the intended fix
  for the DAT-639 duplication bug; harness hygiene is the follow-up.)

### Calibration to run
- `cross_table_consistency` recall/precision — confirm column-fan-out still bands
  the right columns when `columns_used` uses narrow names (the detector's match is
  now exact; a fixture still carrying a `src_<digest>__` prefix would silently stop
  matching → false "clean").
- Unit-teach (DAT-428) calibration — confirm a `<table>.<column>` unit teach still
  lands now that there's no de-prefix fallback.
- A full fresh-workspace re-seed (the migration for this change is fresh re-seed,
  no in-place migration) before any calibration that reads table names.

### Thresholds / new fields
None. No score thresholds changed; no new response fields. Names changed shape
only (prefix dropped).

### testdata hints
The collision fail-loud is testable: two sources whose names/files resolve to the
same narrow table name should now produce one materialized table + one loud
failure, not two parallel tables. An injection that re-imports identical content
under a second name is the natural regression for the original duplication bug.

---

## DAT-637 — catalogue-grain column semantics move to ColumnConcept

**What changed.** Single-ownership move: the per-column semantic attributes that need the *composed catalogue* (not one table) were physically removed from `SemanticAnnotation` (object-grain, add_source) and re-homed on a NEW `ColumnConcept` model, authored ONLY by the table agent (`semantic_per_table`, begin_session) and sealed under the workspace **catalogue head**. Moved: `business_concept`, `temporal_behavior` (+`contested`), `unit_source_column`, `derived_formula_hypothesis` (+conf). Also `foreign_key` removed from `SemanticRole`/the column-agent schema (FK-ness is the `Relationship` catalogue's job).

**Engine routes/phases affected.**
- `semantic_per_table` now emits `TableSynthesisOutput.column_concepts` (new authoring surface) + applies a **near-constant refusal**: never binds a concept to a column whose top value ≥90% (flagged `near_constant` in the feed), and leaves `business_concept` null when no genuine discriminator column exists (→ value-set grounding). Prompt: `dataraum-config/llm/prompts/semantic_per_table.yaml`.
- The **metric-grounding feed** (`graphs/field_mapping.load_semantic_mappings` + `graphs/context.build_execution_context`) now reads `business_concept`/`temporal_behavior`/`unit_source` from `ColumnConcept` pinned to the **catalogue run** (`base_runs.relationship_run_id`), threaded through `metrics_phase` → `ExecutionContext.with_rich_context`.
- The `derived_value` / `temporal_behavior` detector inputs (`entropy/detectors/loaders.load_semantic`) now grain-split: object-grain fields from `SemanticAnnotation`, catalogue-grain from `ColumnConcept` at the run — so catalogue fields are present at `session_detect`, ABSENT at add_source `detect` (the intended grain boundary). `entropy/resolve.resolve_temporal_behavior` now writes `ColumnConcept`.

### Calibration to run
- **Bookkeeping-corpus cold re-seed grounding** — the headline acceptance check: `revenue`/`accounts_payable`/`accounts_receivable` must NO LONGER trap-bind to near-constant flags (`sale`/`ap_paid`/`ar_paid`) — they bind to a genuine discriminator or stay null (value-set grounded). The 11 already-grounding metrics must NOT regress.
- Driver-discovery `target_type` (reads `ColumnConcept.temporal_behavior` now) — confirm stock/flow target selection is unchanged on the calibration corpora.

### Thresholds / new fields
No score thresholds changed. New table `column_concepts` (catalogue-grain, `(column_id, run_id)`). `near_constant` is a new boolean hint in the per-table LLM feed only (not a stored field).

### Cross-package
- **Cockpit drizzle mirror is STALE** until `bun run db:pull:metadata` runs against a migrated DB — `schema.sql` gained `column_concepts` and dropped 5 columns from `semantic_annotations`. The `schema-drift` CI gate will fail until the cockpit mirror is re-pulled.
