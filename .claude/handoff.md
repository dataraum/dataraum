# Engine → eval/testdata handoff

Bridge for `dataraum-eval` (calibration) and `dataraum-testdata`. One entry per
change that affects a detector, pipeline phase, or a response shape eval consumes.

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
- **BookSQL cold re-seed grounding** — the headline acceptance check: `revenue`/`accounts_payable`/`accounts_receivable` must NO LONGER trap-bind to near-constant flags (`sale`/`ap_paid`/`ar_paid`) — they bind to a genuine discriminator or stay null (value-set grounded). The 11 already-grounding metrics must NOT regress.
- Driver-discovery `target_type` (reads `ColumnConcept.temporal_behavior` now) — confirm stock/flow target selection is unchanged on the calibration corpora.

### Thresholds / new fields
No score thresholds changed. New table `column_concepts` (catalogue-grain, `(column_id, run_id)`). `near_constant` is a new boolean hint in the per-table LLM feed only (not a stored field).

### Cross-package
- **Cockpit drizzle mirror is STALE** until `bun run db:pull:metadata` runs against a migrated DB — `schema.sql` gained `column_concepts` and dropped 5 columns from `semantic_annotations`. The `schema-drift` CI gate will fail until the cockpit mirror is re-pulled.
