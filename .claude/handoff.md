# Engine → eval/testdata handoff

Bridge for `dataraum-eval` (calibration) and `dataraum-testdata`. One entry per
change that affects a detector, pipeline phase, or a response shape eval consumes.

---

## DAT-277 — composite-key rescue of many-to-many fan-out edges (LLM-confirmed)

**Branch:** `refactor/dat-277-composite-key-rescue` — **PARKED, not for merge.** Composite
foreign keys to a dimension are a messy-natural-key/multi-tenant artifact, not a clean
star (which uses single-column surrogate FKs); the proper cure is a conform/star-prep
recipe at import (generate surrogate keys), making this downstream rescue unnecessary.
Held pending synthetic ground-truth testdata + `dataraum-eval` calibration to decide if
the pattern is prevalent enough to justify the complexity. Reviewer fixes applied (B1
zero-match ranking, B2 abstain-on-shared-scope-collision, B3 deterministic group id).

**Known limitations to resolve before any un-parking (no calibration yet):**
- **NULL-bias in the greedy ranking** — `_join_multiplication`'s `IS NOT NULL` filter can
  make a mostly-NULL scope column look like a good disambiguator; grain still holds but
  the enrichment is sparse. Needs a coverage-aware ranking + a calibrated fixture.
- **S1: composite extras aren't gated** — the enrichment phase validates only the primary
  join pair against the confirmed relationship + suppression overlay; the LLM's
  `additional_join_columns` are assembled verbatim. A suppressed/unconfirmed scope pair
  would still be ANDed in (grain verification is the only backstop, and it's skipped when
  `row_count is None`).

### What changed (detector + enriched-view shape)
A fan-trap edge (best single-column join is **many-to-many** → over-counts in
enriched views) can now be **rescued** by a composite key — the real FK plus a
shared scoping column (e.g. a tenant `business_id` present on both tables). The
rescue is a **greedy statistical pre-pass** (parallel to Jaccard) that SURFACES a
composite candidate; **the LLM (semantic_per_table agent) is the sole judge** and
confirms it via the new `RelationshipOutput.key_columns`. Nothing is auto-created
from statistics.

- **New relationship shape:** a confirmed composite is persisted as a **group** of
  N `relationships` rows sharing `relationship_group_id`, the anchor at
  `key_position` 0, all carrying the COMPOSITE cardinality (the collapsed value).
  Two new columns: `relationships.relationship_group_id`, `key_position`. A plain
  single-column relationship leaves both NULL.
- **Consumer gate:** `load_defined_relationships` EXCLUDES grouped rows by default
  (`include_composite_groups=False`) — single-column consumers never fan out on a
  half-key. Only `enriched_views` opts in and assembles the multi-column join.
- **Enriched views:** `DimensionJoin.key_pairs` + a multi-column ON clause; a
  composite join scopes on the full key and stays grain-preserving. Grain
  verification remains the backstop (a bad join is still dropped).
- **Fix folded in:** the candidate-dict overlap score is keyed `confidence` (not
  `join_confidence`); the per-table agent's candidate block previously rendered
  `overlap=0.00` for every pair — now shows real scores. This changes the LLM
  input to `semantic_per_table` (relationship confirmation may shift slightly).

### Engine routes / phases affected
- `analysis/relationships/{evaluator,composite,models,db_models,utils}.py`
- `analysis/semantic/{processor,agent,models}.py` + `semantic_per_table` prompt
- `analysis/views/builder.py`, `pipeline/phases/enriched_views_phase.py`

### Calibration to run
- **Relationship detection / FK confirmation** — verify recall didn't regress now
  that the agent sees real overlap scores (was 0.00) and a composite hint. The
  fan-trap cases (DAT-642: drivers empty because real discriminators sit behind an
  m2m/composite fan-out) are the target win.
- **Enriched-view grain + downstream metrics** — a previously-fan-trapped fact↔dim
  pair (e.g. txn↔chart-of-accounts on `account` alone) should now enrich
  grain-preserving via the composite key; metric grounding behind that dimension
  should improve. Watch for any view newly built (and grain-verified) where none
  existed before.

### testdata hints
The canonical shape: a fact table whose FK recurs across a tenant/scope partition
(same `account` name under several `business_id`s) joined to a dimension keyed on
`(account, business_id)`. Single-column join fans out; the composite holds grain.
A genuine many-to-many (bridge/junction table) is the negative — it must abstain
(no rescue, flagged fan-trap), never be forced into a composite.

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
