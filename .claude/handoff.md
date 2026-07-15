# Engine → eval/testdata handoff

Bridge for `dataraum-eval` (calibration) and `dataraum-testdata`. One entry per
change that affects a detector, pipeline phase, or a response shape eval consumes.

---

## DAT-794 — Layer-A relationship detection is now deterministic

**Branch:** `fix/dat-794-layer-a-determinism`. Both unseeded sampling sites in
Layer-A candidate detection are gone — repeated pipeline runs over the same
data now produce identical relationship candidates and identical LLM evidence.

### What changed

- `joins.py`: the reservoir-sampled middle band (10K–1M distinct) is DELETED —
  exact Jaccard/containment below 1M distinct, MinHash (deterministic,
  hash-based) above. The probe showed the sampled band was slower than exact
  at every scale it covered (59ms vs 17ms at 1M distinct) and dropped subset
  FKs (true Jaccard below min_score, rescued only by containment≥0.95) in
  ~30% of runs — on the calibration corpus that was
  `invoices.entry_id → journal_entries.entry_id` at 35/50 detection.
- Containment is now FRACTIONAL, exact, ≥0.95 to rescue (uniform across all
  sizes; still gated at >10 distinct). Previously the exact path required
  100% containment — a dirty subset FK (a few orphans, e.g. an orphan
  injection) whose Jaccard sits below the gate would have been dropped
  deterministically; now it yields a candidate scored at its true containment
  (e.g. 0.98) so the RI evaluator can quantify the orphans. Expect candidates
  for dirty FKs that previously vanished, with honest fractional scores
  instead of a snapped 1.0.
- `finder.py` `_uniqueness_ratio`: the 10% Bernoulli row sample is DELETED —
  exact `COUNT(DISTINCT)/COUNT(*)`. The sampled ratio was a *biased* estimator
  (sample-distinct/sample-rows), overstating uniqueness of FK-like columns at
  any rate (measured 0.93–0.95 for a true 0.47); the value feeds the semantic
  LLM prompt as `[uniq: L= R=]` key-vs-measure evidence, so it was both
  nondeterministic prompt churn AND misinformation.
- `sample_percent` is gone end-to-end: `detect_relationships` /
  `find_relationships` signatures, `relationships_phase`, and
  `phases/relationships.yaml` (key deleted).

### What eval should expect

- Layer-A candidates stable across runs — candidate-set diffs between reps of
  the same strategy now indicate a real bug, not sampling noise.
- Uniqueness ratios in candidates/prompts are exact; expect shifted values
  (e.g. journal_lines.entry_id 0.47, not ~0.94).
- Two clean-corpus FKs remain undetectable at Layer A by design
  (`bank_transactions.account_id` and `balance_sheet.account_id` → chart, 2
  and 7 distinct values): statistically invisible to any overlap measure —
  LLM-lane territory (DAT-762), documented on DAT-794.
  
## DAT-786 — column_concepts.temporal_behavior_contested removed (verdict is authoritative)

**Branch:** `fix/dat-786-remove-contested-flag`. Lead ruling (DAT-772 Gate 3):
the reconciled `temporal_behavior` verdict IS the adjudication outcome — a
parallel "contested" doubt-flag downstream second-guessed a deterministic,
correct resolution.

### What changed

- **Schema:** `column_concepts.temporal_behavior_contested` (BOOLEAN) is GONE —
  model column, resolve-pass write, `schema.sql`, and the cockpit Drizzle mirror.
  Any eval/testdata fixture or assertion reading that column must drop it; test
  DBs recreate (no migration, per the no-backfill rule).
- **Resolve pass** (`entropy/resolve.py`): still writes the adjudicated
  `temporal_behavior`; a witness disagreement now emits a
  `temporal_behavior_contested` **log line** (column_id, run_id, resolved) —
  diagnostic only, the resolved value wins unchanged.
- **Detector unchanged:** the `temporal_behavior` EntropyObject evidence still
  carries its `contested` key (pooled-conflict observability); only the
  ColumnConcept persistence + downstream serving were cut.
- **Cockpit drill flow-gate reversal (DAT-673):** a contested `additive` was
  treated as stock (time-grain slice withheld); it is now trusted as additive —
  the drill's axis menu offers the time grain wherever the reconciled verdict
  says flow.

### What eval should see

- No detector/calibration change: same adjudication, same resolved labels.
- Downstream shape change only: `column_concepts` has one fewer column; drill
  axis menus may now offer time-grain on measures the old gate withheld.

## DAT-775 — grain_columns persists as a bare list; cycle prompt renders real grain

**Branch:** `fix/dat-775-grain-columns-bare-list`. `table_entities.grain_columns`
was written as `{"columns": [...]}` — an unenforced wrapper convention. The
cycle-detection context joined the raw value into its prompt, and joining a dict
iterates its KEYS, so every table's grain rendered as the literal string
`grain: columns`. Live prompt corruption.

### What changed

- The writer persists a bare JSON list of column names; the SQLAlchemy column is
  typed `Mapped[list[str] | None]` (no DDL change — JSON stays JSON).
- The defensive dict-or-list unwrap in `graphs/context.py` is deleted; the
  cockpit's `look_table`/`query-context` grain parser is a bare `string[]` only.
- No backfill: existing workspaces re-run `add_source` (test DBs recreate).

### What eval should see

- The cycle-detection prompt's TABLE CLASSIFICATIONS section now carries each
  table's actual grain columns (`grain: account_id, period`) instead of
  `grain: columns` for every table — cycle-detection quality may shift;
  re-baseline any cycle evals that snapshot prompts or scores.

---

## DAT-769 — business_concept retired: meaning-as-context semantic layer

**Branch:** `feat/dat-769-meaning-as-context`. The single categorical
column→ontology binding is GONE — decided 2026-07-15: no precise word-mapping
onto ontologies; the system maps MEANING and transports it as context,
accepting business-reality ambiguity. **Re-point every eval read of
`ColumnConcept.business_concept`.**

### What changed

- `ColumnConcept.business_concept` → **`meaning`** (free-text business-model
  characterization, catalogue-grain, authored per column — EVERY column). ONE
  field: the initially-planned `ontology_hints` list was CUT before merge
  (consumers must never need exact token matches — resolution is by meaning in
  context; the ontology grounds as SERVED CONTEXT, never as tokens attached to
  columns). Schema clean-cut, `schema.sql` regenerated.
- `semantic_per_table` prompt authors characterization, not classification;
  the object-grain `business_concept` transient field and BOTH its prompt asks
  (system `<business_concept_mapping>` + the user-prompt "EXACT concept name"
  instruction) are deleted (they never persisted anywhere). The downstream
  prompts re-pointed to the meanings feed: `graph_sql_generation` (grounding
  recipe), `business_cycles` (completion-concept grounding),
  `metric_induction`, `column_annotation`.
- The grounding feed (`graphs/field_mapping.py`) is rewritten:
  `load_column_meanings`/`format_meanings_for_prompt` render each column's
  meaning + measured facts (aggregation-lineage pattern/convention/match from
  DAT-759, unit source, temporal behavior, role). Consumed unchanged
  by the metric graph agent and cycles context; validation resolver, cockpit
  context lines, and the `business_meaning` detector evidence carry
  meaning instead of a concept string.
- The DAT-768 fall-loud gate widens: zero resolved `column_concepts` rows for
  a non-empty schema ALWAYS fails begin_session (every column carries meaning
  by contract; the old gate was measure-conditional).

### What eval must do (this bumps with the submodule)

- `test_business_concept_grounding`'s measure asserts are RETIRED — the
  ill-posed oracle (grade consumers instead: reconciliation coverage, cycle
  recall, /deliver accuracy). Dimension-identity truth is graded via the
  DAT-762 bus-matrix cells when that lands.
- `calibration/metadata_truth.py` readers of `business_concept` and the
  testdata `metadata_truth.yaml` `column_concepts` export re-point to the new
  shape (or retire).
- Worker-log greps: `column_concepts_persisted` unchanged; meanings visible in
  the `## COLUMN MEANINGS` prompt block.

Cockpit: drizzle mirror regenerated; look tools / query-context / widgets
swept to `meaning`; vitest unit suite green (1672).

---

## DAT-768 — empty column_concepts falls loud (salvaged from PR #483)

**Branch:** `fix/dat-768-empty-concepts-fall-loud`. The `column_concepts` surface
(metric grounding, cycles field mappings) could come out EMPTY while the phase
reported success — observed 2/3 runs on 2026-07-14, one of them fully green.
Mechanical honesty fixes only; the DAT-769-directed prompt-binding commits from
PR #483 are dropped (that question is being retired — see DAT-769 redesign).

### What changed

- `TableSynthesisOutput.column_concepts` and `.relationships` are **REQUIRED
  tool-schema fields** (no `default_factory`): wholesale omission is now a
  validation error the DAT-710 repair turn catches, instead of schema-legal
  silence.
- `persist_column_concepts` returns an emitted/resolved/dropped_unresolved
  breakdown and logs `column_concepts_persisted` (+ a debug list of the exact
  unresolved names) — a name-resolution wipeout is diagnosable, not
  indistinguishable from an empty emission.
- `synthesize_and_store_tables` **fails begin_session loud** when zero concepts
  resolve while the batch has measure-role columns — an emptied load-bearing
  surface, never a plausible judgment. Gates on emptiness only, never on any
  specific binding (ADR-0009).
- `semantic_analysis.effort: high` pinned explicitly in `llm/config.yaml`
  (no-op vs the current API default; removes the hidden dependency).

### What eval should see

- A DAT-768 recurrence now fails the `semantic_per_table` phase with
  `column_concepts empty despite measure-role columns...` instead of going
  green with 0 rows; worker.log carries `column_concepts_persisted
  (emitted=…, resolved=…, dropped_unresolved=…)` per batch.
- No behavior change when concepts are produced (the common case).

---

## DAT-759 — aggregation-lineage convention selection is support-first (Wilson LCB)

**Branch:** `fix/dat-759-convention-selection`. `discover_aggregation_lineage`
no longer selects the reconciliation convention by minimum median residual —
that criterion is monotone under the ordered-difference search family, so
collinear artifacts (`debit − net_amount ≡ credit`) out-raced true singles on
half-entity subsets and persisted **value-wrong `convention_sql`** into the
property-graph grounding (the 0.50/0.75 match rates eval surfaced).

### What changed

- **Selection order:** Wilson score LCB (95%) of the vote rate over the
  pairing's **common entity denominator** → on LCB ties, lower arity unless the
  difference wins by ΔBIC > 10 (Kass–Raftery) → median residual. Grounded in
  the eval probe `scripts/probes/dat759-convention-selection` (truth 3/3,
  LCB margins 0.345–0.620; min-residual was value-wrong on 2/3 real measures).
- **No schema change.** Persisted `MeasureAggregationLineage` fields are
  unchanged; only which candidate wins changed. `lineage_reconciled` log lines
  now also carry `support_lcb` + `n_entities_fired`.
- `reconcile.py` gains `wilson_lcb`, `classify_series`, `dispose_classified`
  (pure refactor of `dispose`; `FIRE_RESIDUAL_MAX` vote gate unchanged — the
  min-over-family permutation-null replacement for it is a follow-up ticket).

### What eval should see

- `trial_balance.debit_balance` / `credit_balance` reconcile with conventions
  `"debit"` / `"credit"` at match_rate 1.0 (was `debit − net_amount` at 0.50 /
  `credit` at 0.75) → `test_reconciliation_covers_expected_rollup_measures`
  goes 3/3. `balance_sheet.ending_balance` may report `"net_amount"` instead of
  the value-identical `"debit" - "credit"` (arity preference).

## DAT-766 — addSource re-run: typing no longer deletes minted `_sk__*` (FK crash fixed)

**Branch:** `fix/dat-766-typing-preserve-surrogate-columns`. Typing-phase behavior
change on **re-runs only** (a fresh run is unaffected).

### What changed

- `reconcile_typed_columns` (`analysis/typing/resolution.py`) now **never deletes a
  minted surrogate** (`_sk__*`, DAT-277). `resolve_types` builds its `desired` set
  from the RAW source's columns only, so a surrogate minted onto the typed table by
  a prior run looked "dropped" and was DELETEd — violating the FK from the surrogate
  relationship that still referenced it (`ForeignKeyViolation` → `PhaseFailed: No
  tables were successfully typed` → the whole sibling-table cascade cancelled). The
  surrogate mint owns the `_sk__*` lifecycle; typing leaves those columns alone.

### For eval

- **The DAT-766 repro is fixed:** `python -m calibration.run -s clean` twice without
  `--reset` (any corpus that mints a surrogate) now proceeds **past typing** on the
  re-run instead of dying at the FK violation. Re-running addSource over an
  already-completed (minted) workspace is now safe.
- **Sibling DAT-767 is still open:** the `detection-v1` re-run can still fail typing
  with a DuckDB binder error (`"…" cannot be referenced before it is defined`) on a
  mixed-case noise column. That mechanism is NOT closed — the engine loaders all
  normalize physical raw columns to lowercase, so the reported "physical column is
  mixed-case" doesn't hold against the loader code; closing it needs a trace of the
  exact failing SQL + the raw table's true physical column case from a failing
  workspace. Until then, expect `detection-v1` re-run (no `--reset`) to still break.

---

## DAT-761 — stack v4 dimension identity: DAT-757 gate stack replaces distinct-ratio g3

**Branch:** `feat/dat-761-stack-v4-dimension-identity`. **The `dimension_hierarchies`
phase's decision layer is replaced end-to-end and its candidate universe widened —
re-run any calibration that consumes hierarchies/aliases (driver de-confounding
included).** Folds the DAT-757 research verdicts (Jira 16265/66/67, 16300/02/03)
into the engine.

### What changed

- **The distinct-count-ratio "g3" is GONE.** Edges are now decided by classic
  row-g3 ≤ 0.01 (Kivinen–Mannila) + Goodman–Kruskal λ ≥ 0.5 (kills the
  vacuous-skew class: ≥98%-dominant dependents) + a seeded permutation p with
  Benjamini–Hochberg q ≤ 0.05 over each view's screened family. Aliases keep
  pair-count g3 (both directions ≤ 0.01) + both-direction perm-p under the same
  BH family. New pure-stats module `analysis/hierarchies/stats.py`.
- **New structure kind `role`** (`dimension_hierarchies.kind`): a value-equality
  near-copy (disagreement in (0, 5%]) is classified via disagreement-set
  permutation tests (T1 membership vs contexts, T2 value concentration,
  Bonferroni). ROLE pairs (bill-to vs sold-to) persist as `kind='role'`, are
  never merged, and never stack as drill-down levels; undecidable near-copies
  surface as `needs_confirmation` aliases instead of silently merging.
- **Candidate universe = the enriched view's columns**, no longer the grain-safe
  slice catalog. Measures are excluded by `semantic_role='measure'` (the
  additivity lane); everything else is guarded data-grounded (null-aware
  constant drop, near-key, λ), each exclusion logged. The upstream pipeline
  `max_columns` limit is the only width cap. Null-coded columns (`{1, NULL}`)
  are now eligible axes (null-as-category — the rel-hm FN/Active lesson).
- **Scan grain:** guards + pair counts from a full-view scan (chunked O(k²)
  aggregates); row statistics on an aligned in-memory sample (≤ 40M cells,
  seeded bottom-k-by-hash sketch, the DAT-571 drivers idiom) — a sampled fold key can never trip the near-key guard.
- Phase precondition changed: requires a grain-verified enriched view (was:
  slice definitions this run).

### For eval (calibration to run)

- **Hierarchy/alias recall on the calibration corpora** — structures can
  legitimately CHANGE: spurious chains from the distinct-ratio vanish; role
  near-copies stop merging; null-coded columns join alias groups. The DAT-757
  matrix (32/32) + RelBench fold harness re-run against this implementation is
  the acceptance gate (tracked on DAT-761).
- **Driver rankings shift (correctly):** `drivers/_candidate_dims` collapses
  alias groups — role pairs now stay separate competing axes instead of being
  wrongly collapsed to one canonical.
- New oracle surface: `kind='role'` rows (role-playing folded dims) — testdata's
  role-FK fixtures (SALT-style bill-to/ship-to) can be graded directly.

### testdata hints

The 2b generator features already scoped on DAT-757 (partial-inline, false-friend
collision, disjoint regions, folded-numeric) are exactly the fixtures this stack
is calibrated against; a role-playing folded pair with channel-driven divergence
exercises the `role` kind end-to-end.

---

## DAT-763 — deterministic self-referential FK candidate detection

**Branch:** `fix/dat-763-self-fk-candidate`. **Re-run relationship recall — a
self-referential FK is now a DETERMINISTIC Layer-A candidate (was LLM-draw-dependent).**
The finder (`relationships/finder.py`) iterated `table_names[i+1:]` — distinct
cross-table pairs only — since the v1 restructure, so a self-FK
(`chart_of_accounts.parent_id -> account_id`, both endpoints in ONE table) was never a
structural candidate; it reached the judge only when the LLM spontaneously proposed it
(the DAT-761 Tier-3 run caught it missing — recall 8/9).

### What changed

- **`relationships/finder.py`** — the finder now probes each table against ITSELF
  (`table_names[i:]`, the diagonal), not just distinct pairs.
- **`relationships/joins.py`** — `find_join_columns` gained `same_table: bool`
  (default False, cross-table unchanged): the self-probe is restricted to the upper
  triangle (i < j) so a column is never matched to itself (trivial identity) and each
  unordered pair is tried once. Direction is normalized downstream at persist (DAT-758).
- **`relationships/graph_topology.py`** — the undirected structure graph now SKIPS
  self-loops: a NetworkX self-loop double-counts degree (+2) and lists a table as its
  own neighbor, which would misclassify a dimension carrying one external FK + a self-FK
  as a `hub` and corrupt the ContextDocument. (Cycle detection already skipped self-loops
  via `len >= 2`.) A single-table workspace can now detect its own self-FK
  (`detector.py` no longer early-returns below 2 tables). The evaluator needs no change
  (self-joins already alias `t1`/`t2` on one path); the persist keys on the column pair,
  so a `(table, table)` candidate stores correctly.

### For eval (calibration to run)

- **Relationship recall now includes self-FKs every run.** `chart_of_accounts.parent_id
  -> account_id` (the DAT-763 miss) should be a confirmed `llm` relationship on the
  finance corpus deterministically. Verified at Layer-A grain: the finder emits it at
  join_confidence 1.0 (containment), so it always reaches the judge — the remaining
  variance is only the judge's confirm/decline, not candidate existence.
- **Expect MORE intra-table candidates reaching the judge** on wide tables (any two
  same-table columns whose values overlap). The DEFINED catalog is unaffected — a
  spurious self-pair the judge declines persists as `candidate` (below REL_CONFIRM_MIN),
  never `llm`. Precision on the defined catalog is unchanged; the judge is the filter.

### testdata hints

A dimension table with a genuine hierarchy column (`parent_id`, `manager_id`,
`reports_to`) referencing its own PK is the fixture; the finance `chart_of_accounts`
already carries `parent_id`. A negative — two same-table columns that overlap in values
but are NOT a FK (two user-id audit columns) — exercises the judge-declines-to-candidate
path.

## DAT-764 — structural reconciliation is authoritative for stock/flow

**Branch:** `fix/dat-764-structural-authoritative`. **Re-run Tier-3 stock/flow — this
fixes the three `trial_balance` reds WITHOUT a band resweep (the harness was detecting a
real mislabel, exactly as the ticket said).** The DAT-728 handoff assumed the surviving
2-witness pool (`llm_claim` + `structural_reconciliation`) would resolve `debit_balance`
via structural+LLM **agreement**. The eval run showed the LLM intermittently name-reads
the periodic "balance" columns as **stock**, and the symmetric pool let that confident
name-read tip a data-grounded `per_period` (flow) verdict — `debit_balance` reconciled at
match_rate 0.75 → resolved `point_in_time`, while `credit_balance` at 1.0 survived.

### What changed

- **`entropy/measurements/temporal_behavior.py`** — when the `structural_reconciliation`
  witness fired a gated verdict this run AND the `llm_claim` disagrees, the LLM claim is
  **pooled OUT** (not pooled against it). Stock/flow is data-determined (DAT-657/491): the
  reconciliation is the only witness whose input is the data, not the name, so its verdict
  sets the label regardless of match magnitude. An AGREEING claim is kept (corroborates,
  lower ignorance); when structural abstained (every add_source detect) the LLM stands
  alone (unchanged). Verified on the eval match_rates: `debit_balance`@0.75 and
  `credit_balance`@1.0 both now resolve `additive` with pooled **conflict 0.000**; a
  genuine cumulative stock (`balance_sheet.ending_balance`) still resolves `point_in_time`.
- `resolved_behaviour(adj)` now takes the adjudication (was the bare `PoolResult`) so it
  reads the two witness positions; `contested` records whether the reads disagreed (pure
  observability — the readiness/loss lane keys on the conflict SCORE, `loss.py`, not this
  flag). One caller updated (the detector).

### For eval (calibration to run)

- **Re-run Tier-3 stock/flow.** `trial_balance.debit_balance` AND `credit_balance` →
  `additive`. The `temporal_behavior` disagreement SCORE for these drops to ~0 (the pool no
  longer manufactures conflict when the data decided), so `test_clean_scores_within_measured_bands`
  and `test_clean_readiness_no_regression` (query_intent) should recover **without**
  touching bands or `intent_readiness.yaml`. Keep the witness-liveness guard.
- **Watch:** a genuinely ambiguous flow the structural witness gets WRONG would now be
  authoritative with no LLM temper. This is the deliberate DAT-491 stance (the witness is
  heavily gated: ≥2 voting entities, ≥0.8 agreement, wrong-anchor residual guard). Confirm
  no CLEAR stock (`balance_sheet`/`ending_balance` family) regresses to flow.
- **Cross-detector safety invariant to pin (eval-owned test):** making structural
  authoritative removes the pooled CONFLICT signal, so a WEAK verdict (low match_rate —
  few entities reconciled) that overrules a confident LLM no longer contributes conflict
  risk. Safety then rests on IGNORANCE, which scales with match_rate (measured: ~0.998 at
  match 0.05 → ~0.53 at 1.0) and is unit-tested here. But whether that keeps
  `aggregation_intent` OUT of "ready" is a CROSS-detector property of `loss.yaml`'s
  ignorance weight × readiness bands — not enforced in engine code (deliberately: no
  invented match_rate floor). Add an eval regression: a low-match structural override
  disagreeing with a confident LLM must NOT band `aggregation_intent` "ready". A future
  `loss.yaml`/bands edit could otherwise silently reopen the SUM-a-stock failure class.

### testdata hints

None. The finance corpus's `trial_balance` (per-period flows named "balance") + genuine
`balance_sheet` stocks already exercise both sides. The moderate-match (0.75) verdict is
the discriminating case — a fact that reconciles on only some entities is what separates
"data authoritative even at partial match" from the old symmetric tip.

---

## DAT-756 — referenced-dimension identity + `shared_dims` fix + conformed-dimension

**Branch:** `feat/dat-756-dimension-identity`. **A detector-grouping-key fix (closes a
live stock/flow false-negative AND false-positive) + a new graph-edge oracle surface +
one changed graph surface.** Foundation tier of the operating-model graph (DAT-725): a
dimension now has an IDENTITY (the FK-target dim table), so every consumer keys off it
instead of the column name. Restores the DAT-729 conformed-dimension capability that was
reverted for name-matching, rebuilt on the identity.

### What changed

- **Referenced-dimension identity persisted on `slice_definitions` (`slicing` phase).**
  Three new columns — `dimension_table_id` (FK-target dim table, NULL for a folded
  slice), `dimension_attribute` (the enriched `fk__attr` level), `fk_role` (the FK
  column) — resolved at slice-write from the enriched view's grain-safe relationship
  provenance, never from the column name.
- **`shared_dims` / stock-flow witness (`aggregation_lineage` phase) regrouped by
  identity.** `analysis/lineage/processor.py` now pairs two facts iff they reference the
  SAME dim table at the SAME attribute (was: same `column_name`). **Closes a live bug in
  both directions:** the same dimension reached via differently-named FK columns was
  never paired (the witness silently never ran); two unrelated same-named FOLDED columns
  were paired. Role-playing FKs to one dim on a single fact are all kept (list per
  fact), not collapsed. The persisted `measure_aggregation_lineage.slice_dimension` is
  now a `<dim table>.<attribute>` label, not a physical column name.
- **`og_conformed_dimension` edge (NEW) + `og_has_dimension` identity props + a changed
  `og_references`.** `og_has_dimension` carries `dimension_table_id`/`dimension_attribute`/
  `fk_role`. `og_conformed_dimension` (table→table, ATTRIBUTE grain — same
  `(dimension_table_id, dimension_attribute)`) types two facts sharing a dimension
  AXIS (the alignable drill-across GROUP BY the SQL agents author over). `og_references`
  now EXCLUDES the DAT-723 fan trap (a relationship whose both endpoints are slice
  columns resolving one dim TABLE) — TABLE grain, deliberately DECOUPLED from the
  attribute-grain edge (a cross-level fan trap is excluded from refs yet correctly has
  no conformed edge).

### For eval (oracle surfaces)

- **`og_conformed_dimension` — new graph-edge truth section.** Assert the finance
  conformed pairs (facts sharing a dimension AXIS — e.g. `trial_balance` ↔ `balance_sheet`
  both sliced on the accounts dim at the SAME attribute) are typed via the shared
  `(dimension_table_id, dimension_attribute)`, both directions. Graded absolutely
  (generator-known pairs). Two caveats: (a) it is ATTRIBUTE grain — two facts sharing the
  dim TABLE but sliced at DIFFERENT attributes do NOT conform (no alignable axis); (b) a
  single fact-table PAIR can emit MULTIPLE edges (role-playing FKs at one axis) — assert
  on the table pair + `(dim, attribute)`, not a single-edge count.
- **`og_references` — CHANGED surface.** Fan-trap fact↔fact edges between shared-dimension
  slice columns no longer appear as `refs`. Any truth assertion enumerating references
  must expect these excluded (a genuine fact→dim FK still appears — a dim key is never a
  slice column, so the exclusion cannot fire on it).
- **Stock/flow witness may FLIP on existing fixtures.** The `shared_dims` fix can now
  fire the witness on previously-silent differently-named-FK pairs, and now correctly
  abstains on previously-firing same-named-folded pairs. Re-run the stock/flow oracle on
  the finance corpus and check whether any currently passing/failing case flips.

### testdata hints

- A fixture with **two facts joining ONE dim table via differently-named FK columns**
  (e.g. `gl_account` in one, `account_no` in another, both → `chart_of_accounts`)
  exercises the false-negative that finance's consistent naming currently hides.
- A **role-playing** fact (two FKs to one dim — `kontonummer` + `kontonummer_des_gegenkontos`
  → accounts) exercises the multi-slice-per-identity path.

---

## DAT-729 — concept edges (`disjoint_with` / `part_of`)

**Branch:** `feat/dat-729-concept-edges`. **New graph-edge oracle surfaces — no
detector-score change.** Phase 4 of the operating-model graph (DAT-725): the concept
vocabulary gains typed edges. Both are **seed structure**, not a detector
recalibration — the eval work is **new `metadata_truth.yaml` sections + oracle
assertions**, graded absolutely. (Conformed-dimension typing was pulled from this phase
— it needs a real dimension-identity design, tracked separately; see the DAT-725 thread.)

### What changed

- **`disjoint_with` concept edges (seed).** A new `concept_edges` table (workspace-
  persistent, supersede-on-edit — same identity contract as `concepts`) seeded from
  convention `concept_groups`: concepts in DIFFERENT groups of one convention are
  disjoint (an account is an asset xor a liability). Finance's `sign_natural_balance`
  (credit-normal 4 × debit-normal 8) yields **32 unordered = 64 directed** edges,
  including the DD's named examples `accounts_payable ⊥ accounts_receivable` and
  `current_assets ⊥ current_liabilities`. Bound into the property graph as the
  `concept_edge` edge (predicate property).
- **`part_of` concept edges (seed).** A new `compositions` ontology block (`whole ←
  parts`, lint-validated) seeds directed `part → whole` edges: finance authors
  `current_assets ← {cash, accounts_receivable, inventory}` and `current_liabilities ←
  {accounts_payable}` (4 edges). Concept-grain composition ONLY — the account-instance
  chart-of-accounts tree stays the physical `references` topology (P1) / `rolls_up_to`
  (P5). Transitive ancestry is a bounded recursive-CTE (max-depth 4 + cycle guard).
- **`reconciles_with` DEFERRED to P2 (DAT-727).** Its producers are all Grounding-node-
  dependent (the aggregation-lineage witness reconciles a measure against its event
  aggregation = two groundings of ONE concept; the "4 generator pairs" are dataset-level,
  not Concept↔Concept). The `ConceptEdge` model carries the `reconciles_with` predicate
  + `tolerance` for P2 to populate — see the DAT-727 note.

### For eval (new oracle surfaces to add)

- **`disjoint_with` truth section.** Assert the finance disjoint set against the sign
  partition RULE (any credit-normal concept ⊥ any debit-normal concept), not a hand-
  picked list — the engine emits the full cross-product, both directions. Named
  anchors: `accounts_payable ↔ accounts_receivable`, `current_assets ↔ current_liabilities`.
- **`part_of` truth section.** Assert the composition edges (`cash`/`accounts_receivable`/
  `inventory` part_of `current_assets`; `accounts_payable` part_of `current_liabilities`),
  directed (whole is NOT part_of its part), and that the recursive-CTE ancestor closure
  is transitive + cycle-guarded (the graph query, not a stored transitive edge).
- **No stock/flow, additivity, or grounding recalibration** — no detector inputs, scores,
  or thresholds changed. `reconciles_with` is NOT in this truth set (lands with P2).

### testdata hints

None. The finance corpus already carries the disjoint/compose partitions in the shipped
ontology, so both edge types are exercised without new fixtures.

### Cross-package / schema

`schema.sql` gained the `concept_edges` table; `schema_graph.sql` gained the
`og_concept_edges` graph element; the cockpit drizzle mirror gained the `conceptEdges`
view (`schema-drift` CI enforces). No read-view (`current_*`) shape change beyond the new
`concept_edges` passthrough.

---

## DAT-728 — typed concept vocabulary (config→DB) + `ontology_prior` witness drop + 4-way table role

**Branch:** `feat/dat-728-typed-concept-vocabulary`. Three eval-facing changes: a
`temporal_behavior` witness removal (**re-calibrate stock/flow**), a persisted
table-role taxonomy (**new `table_roles` oracle surface**), and a concept `kind`
field (**new `metadata_truth` report surface**). The config→DB move itself is
structure, not a detector-score change.

### What changed

- **`ontology_prior` witness DROPPED from `temporal_behavior` pooling (DAT-657).**
  Stock/flow is data-determined — a concept cannot declare a format — so the
  concept-seeded prior is gone. The pool is now **`llm_claim` + `structural_reconciliation`**
  only (`entropy/measurements/temporal_behavior.py`; `reliabilities.yaml` lost the
  `ontology_prior: 0.762` entry). `OntologyConcept.temporal_behavior` and all 18
  finance-measure `temporal_behavior:` lines are removed. The `debit_balance` case
  resolves to **flow** via LLM+structural agreement, with **no manufactured conflict**
  (the prior used to fight the data here). The `temporal_behavior` detector now emits
  **NO teach suggestion** (the `rebind` teach was redundant with the grounding path —
  and was the only `rebind` emitter).
- **4-way table role, PERSISTED.** `TableEntity.is_fact_table`/`is_dimension_table`
  booleans → one `table_role` column, `TableRole` ∈ {`fact`, `periodic_snapshot`,
  `dimension`} (`analysis/semantic/db_models.py::derive_table_role`, computed at
  classification from is-fact ∧ grain∩time, persisted by `processor.py`). The
  additivity COUNT rule (`graphs/additivity_resolver.py::_fact_is_snapshot`) reads the
  **persisted** subtype now, not a re-derivation. (Bridge is DEFERRED to DAT-747 — not
  in this enum.)
- **Concept vocabulary is a typed `concepts` table (config→DB).** The shipped
  `ontology.yaml` is the SEED (normalized to rows at connect via
  `ensure_concepts_seeded`); runtime reads the typed table (`load_workspace_concepts`),
  not YAML⊕overlay. The `config_overlay(type='concept'/'concept_property'/'rebind')`
  family is retired READ+WRITE. New required concept field **`kind`** (`ConceptKind` ∈
  {measure, entity, dimension, unit}) — declared in `finance/ontology.yaml` (22
  concepts) and produced by the cockpit frame induction.

### For eval (calibration to run)

- **Re-calibrate stock/flow — a witness was removed, so this is a real calibration
  change, not a re-baseline.** Recall must be re-validated on the generative
  stock/flow corpus (`detection-stockflow-*`): the two surviving witnesses
  (`llm_claim` 0.838, `structural_reconciliation` 0.889) now carry the full pool.
  `trial_balance.debit_balance/credit_balance` must STILL resolve **`additive` (flow)**
  — but now via structural+LLM agreement, not a prior override. Keep the
  **witness-liveness guard** (structural fired on ≥1 column); a 0/N is the regression
  signature. Confirm no stock/flow label REGRESSES from dropping the prior (the prior
  was a name-anchored vote; its removal should only remove name-anchoring errors).
- **New `table_roles` oracle.** `table_role` is now a first-class persisted verdict
  (`fact` / `periodic_snapshot` / `dimension`). Add/point the oracle at it:
  `trial_balance` → `periodic_snapshot`, `journal_lines` → `fact`, the dimension
  tables → `dimension`. The additivity matrix's semi-additive (stock-over-snapshot)
  verdicts now trace to this persisted subtype.
- **New concept `kind` surface for `metadata_truth`.** Each grounded concept carries a
  `kind`; a `metadata_truth.yaml` concept-kind report can assert the seeded finance
  kinds (revenue → measure, account → entity, fiscal_period → dimension, currency →
  unit). This is the config→DB seed's ground truth.

### testdata hints

No new fixtures required — the finance corpus already exercises all three (the
`trial_balance` periodic snapshot, the stock/flow measures, the seeded finance
concepts). Directional: a periodic-snapshot fact with a clear grain∩time is what
distinguishes `periodic_snapshot` from `fact`; a concept-only framed vertical (no
validations/cycles/metrics) is the fixture that would exercise the new typed-concept
framed-vertical detection (`core/vertical.py`), though that path is finance-agnostic.

### Cross-package / schema

`schema.sql` gained the `concepts` table + `og_concepts` graph element and changed
`table_entities` (dropped the two booleans, added `table_role`); the cockpit drizzle
mirror is regenerated (`schema-drift` CI enforces). The cockpit `frame` stage writes
`concepts` rows directly (a granted control-write surface).

---

## DAT-699 follow-up — judge-declined relationships cut at the source

**Branch:** `fix/dat-699-cut-declined-rels-at-source`. The systemic version of the
DAT-721 lineage gate. The semantic judge encodes its verdict in `confidence`
(no explicit field); on the finance corpus it lands bimodally — declines ≤ 0.40,
accepts ≥ 0.85, wide dead zone. Persistence wrote **every** returned rel as
`detection_method='llm'` (defined), so ~6/13 judge-DECLINED relationships (date/
amount value-coincidences the LLM itself rejected) polluted the "defined" catalog
that every consumer reads (lineage, cycles, enriched_views, validation, graphs).
DAT-699 had removed the read-path floor, exposing them.

### What changed
- `semantic/processor.py` — a relationship is persisted as `llm` (confirmed) only
  at `confidence >= REL_CONFIRM_MIN` (0.7, the judge's own decision boundary in
  its dead zone); below that it's persisted as `candidate` with the judge's
  evidence/reasoning kept (auditable), so `load_defined_relationships`
  (`!= 'candidate'`) is now truthfully "judge-confirmed". Both write paths are
  gated: single-column, AND the composite/surrogate-intent path (a declined
  composite falls through to the gated single-column persist instead of minting a
  confirmed `llm` row). No consumer re-weighs confidence; the source is the single
  contract.
- `lineage/processor.py` — the DAT-721 per-consumer confidence gate
  (`KEY_CONFIDENCE_MIN`) is **removed**. With declines cut at the source it was
  redundant, and a second threshold that must track `REL_CONFIRM_MIN` is a drift
  trap (lower the source and lineage would silently strip confirmed FKs). One
  threshold, at the source; the lineage key-exclusion trusts the catalog.

### For eval (calibration to run)
- "Defined" relationship counts DROP to confirmed-only (declines are now
  candidates). Relationship-recall assertions that expected a declined value-
  coincidence to be "defined" should now (correctly) not see it.

---

## DAT-710 — `semantic_per_table` schema-repair turn (begin_session survives a shape flake)

**Branch:** `fix/dat-710-semantic-repair-turn`. **No calibration action required — a
robustness fix; recall/precision unchanged.**

One malformed `analyze_tables` relationship entry (a missing `to_column`, a literal
`"placeholder"` reasoning) used to fail `begin_session` WHOLE: strict Pydantic
validation → non-retryable `PhaseFailed`, whole-cascade blast radius (a manual re-run
passed clean — a pure LLM shape flake). `semantic_per_table` now gets the same one-turn
schema repair `generate_sql` got in DAT-699 — on a `TableSynthesisOutput` validation
failure the model fixes its own tool output under a forced tool choice, and only a
SECOND failure fails loud.

- The repair turn is now a shared helper (`llm/tool_repair.py::repair_tool_output`,
  generic over the output model); both `graphs/agent.py` (grounding) and
  `analysis/semantic/agent.py` (synthesis) call it. `GraphAgent._repair_tool_output`
  was inlined + deleted — behavior byte-identical (`test_tool_repair.py` unchanged, green).
- **Not `strict`:** `analyze_tables` is a large batched extraction, exactly the shape
  where `ToolDefinition.strict` makes the model legally under-produce (the
  column_annotation 1-of-8-tables collapse); the repair turn, not strict, is the
  recall-safe lever.

### For eval
No detector or response-shape change. The only observable delta is on the FAILURE path:
a semantic shape flake that used to kill a calibration run's `begin_session` now
self-repairs, so wide / real-LLM eval runs see one fewer spurious failure. Nothing to
recalibrate.

## DAT-720 — structural stock/flow witness restored (enriched time-axis backfill)

**Branch:** `fix/lineage-enriched-time-axis`. **Re-run stock/flow calibration — the
data-grounded witness now fires; some labels change (correctly).**

DAT-536's inline-aggregation re-point silently disabled the DAT-491 structural
reconciliation witness on the finance corpus: a fact whose event date is a JOINED
column (`journal_lines.entry_id__date`, the header date) had empty
`TableEntity.time_columns`, so the inline lineage path dropped it → 0
`measure_aggregation_lineage` rows → the witness abstained on **every** column →
stock/flow was decided by the two name-based witnesses only.
`trial_balance.debit_balance/credit_balance` (per-period FLOWS) were mislabeled
`point_in_time` (stock) by the "balance" name. Found by the DAT-685 eval oracle.

### What changed
- **Slicing agent** — `slicing_analysis` `effort: low → medium`, prompt/schema
  framing tightened (dropped the "fallback"/"omit … or genuinely has none" escape).
  At `effort: low` Sonnet 5 scoped to the literal ask and dropped the secondary
  enriched time-axis backfill (it's the SLICING agent — not semantic_per_table —
  that names the enriched `is_dimension_time_column` axis for facts with no own date).
- **Deterministic backstop** (`slicing_phase.py`) — `TableEntity.time_columns` is
  now backfilled straight from the deterministic `is_dimension_time_column` flag for
  any analyzed fact the agent (and semantic) left empty. The witness can no longer
  go inert on an LLM miss. Fixes every consumer at the source: lineage, drivers,
  and the drill's time grain.

### For eval (calibration to run)
- **Stock/flow recall CHANGES, correctly:** the structural witness now fires;
  `trial_balance.debit_balance/credit_balance` should resolve **`additive` (flow)**,
  not `point_in_time`. Re-baseline the DAT-685 stock/flow oracle — the trial_balance
  known-miss should FLIP to correct. Add a **witness-liveness guard** (structural
  witness fired on ≥1 column); a 0/N is the regression signature.
- Additivity verdicts on trial_balance measures change accordingly (flow → not
  time-stripped). No score-threshold change — this restores an inert data witness.

---

## DAT-721 — lineage key-exclusion weighs relationship confidence

**Branch:** `fix/dat-721-lineage-confidence-gate`. The SECOND regression behind the
same witness (independent of DAT-720). Even with the time axis restored,
`trial_balance.debit_balance` still would not reconcile: the lineage key-exclusion
(`discover_aggregation_lineage`) treats every endpoint of a *defined* relationship
as a key (never SUMmed, dropped as a convention term). DAT-699 deliberately removed
the confidence floor from `load_defined_relationships`, so a judge-DECLINED
`journal_lines.debit → payments.amount` at **confidence 0.05** (the LLM's own
"coincidental numeric overlap; decline" verdict) now reaches this consumer and
stripped `debit`'s only reconciliation convention → `debit_balance` silently
dropped (only `credit_balance` reconciled → 1/20 witness firing).

### What changed
- **Consumer-local confidence gate** (`processor.py`, `KEY_CONFIDENCE_MIN = 0.7`):
  a MEASURED (`llm`/`keeper`) relationship endpoint is a key only at `>= 0.7`
  (the relationships phase's high-confidence band); `manual` (user-asserted)
  bypasses the number. Does NOT re-add a global gate to `load_defined_relationships`
  (that contradicts DAT-699 — confidence is evidence for consumers to weigh).

### For eval (calibration to run)
- **Both** `trial_balance` measures should now reconcile (debit AND credit), not
  just credit. Extend the DAT-685 structural check to assert coverage of both, not
  only the label-correctness of whatever fired.
- Open follow-up (not this fix): a judge-DECLINED relationship is still persisted
  as `detection_method='llm'` (defined). Consumers that don't weigh confidence stay
  exposed — a broader DAT-699 question (honor the verdict vs. every consumer weighs).

---

## DAT-718 — activity metrics + `count_distinct` grounding vocabulary

**Branch:** `feat/dat-718-matrix-metrics`. Extends the finance metric catalogue +
the grounding vocabulary so the DAT-716 additivity matrix fires on real metrics.

### What changed

- **Three new finance metrics** (`packages/dataraum-config/verticals/finance/metrics/activity/`):
  `transaction_count` (`COUNT` → additive flow), `average_transaction_value`
  (`AVG` → non-additive), `active_accounts` (`COUNT(DISTINCT)` → non-additive).
  Single-extract graphs; grounded over the `journal_lines` event fact.
- **New aggregation `count_distinct`** in the grounding vocabulary
  (`graph_sql_generation.yaml` `<aggregation_types>` → emit `COUNT(DISTINCT "<col>")`;
  `GraphStep.aggregation` doc). The DAT-716 classifier already handles the DISTINCT
  shape; this lets a metric *declare* it.

### For eval (the validation this needs — run in eval / `/smoke`)

- **Grounding recall on the new metrics**: confirm the agent grounds
  `transaction_count`/`average_transaction_value` over `journal_lines`, and
  `active_accounts` as `COUNT(DISTINCT account_id)` (the new vocabulary entry).
  `count`/`avg` are validated by unit test at the config level; the actual SQL the
  LLM emits is the e2e question.
- **The additivity oracle** (the DAT-718 core): once grounded, assert the
  `metric_additivity` per-target verdicts — `transaction_count` → additive,
  `average_transaction_value`/`active_accounts` → non-additive — against ground truth.

### For testdata

The current corpus already carries `journal_lines` (an event fact) with per-line
amounts + account FKs, so these should ground without new fixtures — confirm at
e2e; add fixtures only if the agent can't ground `active_accounts`.

---

## DAT-716 — metric additivity verdict (new `metric_additivity` read-view)

**Branch:** `feat/dat-716-additivity-verdict`. Engine-internal, **no detector or
calibration change** — the metric grounding numbers are untouched.

### What changed

- **New artifact `metric_additivity`** (read-view `current_metric_additivity`,
  operating_model stage): one row per **drill target**, keyed
  `(target_kind, target_key, run_id)` — `'metric'` (graph_id) for a formula node,
  `'measure'` (standard_field) for a grounded-extract node (both are drillable).
  Payload `{categorical_additive, time_additive, categorical_reason, time_reason}`.
  The operating_model `metrics` phase computes it (deterministically, no LLM)
  after metrics execute: each extract is classified (function symmetry × stock/flow
  × periodic-snapshot grain), rolled up through the DAG for the metric verdict and
  mapped by standard_field for the measure verdicts. An unresolved target gets no
  row.
- **No response-shape change to existing artifacts.** The drill (cockpit) consumes
  this in DAT-717 (reading by `target_kind`); the eval-relevant work is **DAT-718**
  — extending the finance vertical + `dataraum-testdata` with `AVG` / `COUNT(*)` /
  `COUNT(DISTINCT)` metrics and a ground-truth oracle so the full additivity matrix
  is exercised on real metrics. That is where testdata + a new calibration/eval
  check land.

### Confirmed for DAT-717 (drill axes)

A fact's own bare date/period column DOES reach the drill axis set today: the
enriched view selects `f.*` for fact columns (`analysis/views/builder.py`),
unfiltered by role/type — so `trial_balance.period` survives into the view as a
usable categorical axis. No engine fix was needed for the DAT-716 AC7 check;
surfacing it as a *time grain* (vs a categorical slice) is DAT-717's call.

### For testdata (directional, lands in DAT-718)

The current finance corpus already exercises `SUM(flow)` (→ additive), `SUM(stock)`
(→ semi-additive — fires live now via the **measure** verdicts, e.g. `current_assets`),
and ratios (→ non-additive). To cover the rest of the matrix, DAT-718 needs induced
metrics using `COUNT(*)` (event fact → time-additive), `COUNT(DISTINCT)`
(→ non-additive), and `AVG` (→ non-additive), plus the ground-truth oracle.

---

## DAT-699 — flag-and-surface over fabricated determinism (metric grounding + enrichment)

**Branch:** `feat/dat-699-flag-and-surface`. Seven changes from the the bookkeeping smoke corpus
clean-stack root-cause pass (0/13 metrics executed vs. a measured ceiling of 2)
plus the determinism audit. Response shapes eval reads have changed:

### What changed

- **Metric artifact `state_reason` format** (biggest eval-facing change): a
  metric with ungroundable dependencies now reads
  `dependency 'cogs' is ungroundable — revenue = 5,925,920,163.00 ✓ · cogs ✗ <reason> · gross_profit blocked (needs cogs)`
  — ALL holes named (not just the first), per-step measured values for the
  groundable subgraph, which EXECUTES. Assertions matching the old
  `dependency 'X' is ungroundable: <reason>` prefix need updating.
- **Verifier no-support reason**: `has no support: it aggregated to NULL —
  either its filter matched no rows, or an aggregated operand is entirely
  NULL over the rows it did match` — the old `its filter matched no rows`
  ASSERTED an unmeasured cause (misclassified a one-sided A/R ledger whose
  join matched 167k rows).
- **Grounding agent is no longer one-shot**: high-cardinality columns
  (> 200 distinct) are served as size+sample+`search_values` hint instead of
  nothing, and the agent may spend up to 4 bounded catalog searches before
  emitting `generate_sql`. Expect grounding recall UP on datasets whose
  discriminators exceed the enumeration bound (the bookkeeping smoke's depreciation/tax
  class) and 1–5 extra small LLM turns per affected extract. A tool-output
  schema validation failure gets ONE model repair turn before failing.
- **Enrichment**: the 0.7 confidence floor is gone (the judge sees all
  defined relationships); keeper rows carry their last-measured
  confidence/cardinality/evidence stamped `not_remeasured` (never
  `confidence=1.0, cardinality=NULL`); the sticky shape re-offers a pair
  when its evidence fingerprint changed.
- **Prompt** (`graph_sql_generation.yaml`): one-sided-ledger netting shape
  (`CASE WHEN COUNT(*) = 0 THEN NULL ELSE COALESCE(SUM(a),0) - COALESCE(SUM(b),0) END`)
  — absence still surfaces as NULL, never masked as 0.

- **Declared metric validations flag, never gate** (approved follow-up on this
  branch): a violated catalogue `validation:` condition no longer blocks
  execution ("composed but not executed: declared validation failed …" is
  gone) — the metric EXECUTES and the violation rides `state_reason` as
  "declared expectation not met for 'X': … (value=…, severity=…)", combined
  with the DAT-631 low-confidence flag. Config-side, all extract-level sign
  bounds (revenue > 0, COGS >= 0, …) were removed from the finance metric
  YAMLs — the sign rule's homes are the `sign_natural_balance` convention
  (authoring) and the `sign_conventions` validation (dataset-level); only
  formula-level plausibility ranges remain (dso/dpo/dio 0–365 at warning,
  current_ratio sign) and they flag.

### Calibration to run

- Metric grounding recall on ledger-shaped corpora: dso-class metrics
  (one-sided debit/credit ledgers) and depreciation/tax-class extracts
  (values beyond the enumeration bound) should now ground; COGS-class
  honest-NULLs must STAY declined (a confident number for an absent concept
  is the regression to watch).
- Any eval parser reading metric `state_reason` needs the new format.

### testdata hints

A one-sided-ledger fixture (AR-style: 100%-NULL credit leg over matched
rows) + a lookalike-negative distinguishes the netting fix from COALESCE
masking: correct = dso executes with the real balance; regression = a
metric executes as 0 when the filter matches nothing.

---

## DAT-697 — composite verdicts gate the silent-accept keeper machinery

**Branch:** `feat/dat-697-keeper-adjudication`. Fixes the resurrection loop
found live on the bookkeeping smoke: a judge-DECLINED composite (DAT-695
measured-usage decline) was silently lifted back as a `keeper` by DAT-409's
silent-accept, and the mint's grace window then kept its hollow `_sk__`
columns alive run after run.

### What changed

- **`surrogate_key_intents` gained `status`** (`'confirmed' | 'declined'`,
  schema.sql + drizzle mirror re-dumped): `semantic_per_table` now records a
  verdict row for EVERY offered rescue hint — declined = offered − confirmed.
  The mint loads only confirmed intents (unchanged behavior).
- **`intent_digest` is direction-neutral** (`surrogate.py::composite_intent_digest`
  — sha1 over unordered id pairs; the canonical NAME order key is likewise
  direction-neutral now). Neither the judge's anchor nor its from/to
  orientation is run-stable; matching recomputes digests from stored natural
  column ids, never compares stored strings.
- **Keeper machinery honors adjudication** (`materialize.py`): a prior llm
  pair the current run RULED on is not lifted; a stale `keep` overlay on an
  adjudicated pair is superseded (never deleted — audit trail) and no longer
  materializes a keeper row. Pairs with NO verdict this run keep full DAT-409
  flake protection. Polluted workspaces self-heal within ~2 runs (grace ages
  out of the promoted head) instead of requiring a wipe.

### Calibration to run

- Composite/relationship confirmation suites: run-over-run STABILITY is the
  new observable — a declined lookalike must STAY declined across consecutive
  begin_session runs (pre-fix it flip-flopped back via keeper). The DAT-695
  decline-recall expectations are unchanged.
- Surrogate column lifecycle: after a decline, `_sk__*` columns for that
  composite must be GONE by the second following run (grace window is one
  promoted head, not forever).

### testdata hints

The lookalike-negative corpus (DAT-695 entry below) doubles as the
resurrection fixture: run begin_session twice on it — first with the hint
confirmed (or seeded keeper state), then with the decline — and assert the
keeper does not resurrect the declined composite.

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

**Testdata note:** the bookkeeping smoke corpus's COA has **zero COGS-type and zero inventory accounts**, so gross-profit-family metrics can never execute there (honest NULL extracts) — don't read that as a grounding regression; realistic executed ceiling on the bookkeeping smoke corpus ≈ dso + current_ratio.

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
