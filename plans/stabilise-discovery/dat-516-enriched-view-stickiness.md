# DAT-516 — Enriched-view shape stability (silent-accept for the dimension shape)

- **Epic:** DAT-514 (Stabilise relationships and slices discovery)
- **Design anchor:** ADR-0013, Amendment 2026-06-20
- **Size:** M — one phase file + one persisted field + tests
- **Status:** plan (pre-`/refine`)

## Problem

The enriched view's shape (which `<fk>__<attr>` dimension columns a fact exposes) is re-judged by a
per-run LLM (`enriched_views_phase._get_llm_recommendations` → `EnrichmentAgent`), and the lake-grain
columns are latest-only (DAT-415 — deleted + reinserted each run). So re-running the *same session* can
silently erase or change columns downstream SQL depends on (observed: `account_id__account_type` present
one run, `passthrough_enriched_view`/0 columns the next). The relationship catalog (Layer A) already has
silent-accept durability (DAT-409); the enriched-view shape (Layer B) does not — it re-litigates from
scratch.

## Approach (from the ADR amendment)

Keep the enrichment LLM, but make its verdict **sticky**: decide each relationship's exposure once,
persist it, inherit on re-run, and feed the LLM only the *undecided* (newly-confirmed) relationships. The
shape becomes monotonic — grows on a newly-confirmed relationship, shrinks only on an explicit
teach/reject — which is what keeps named-view columns stable for downstream SQL. We do **not** attempt a
deterministic builder (tried; column selection is genuine judgement) and we do **not** touch Layer A.

## Key files

- `packages/engine/src/dataraum/pipeline/phases/enriched_views_phase.py` — `_run` (~150-373), the
  `_get_llm_recommendations` call-site (~173), `_register_and_profile_dim_columns` (~375-460, the
  `delete(Column)` at ~435-442).
- `packages/engine/src/dataraum/analysis/views/db_models.py` — `EnrichedView` (add the field).
- `packages/engine/schema.sql` — regenerated DDL dump; `packages/cockpit/src/db/metadata/*` — drizzle mirror.

## Phases (each leaves the tree green)

### P1 — Persist the considered set
> Implemented with the column-PAIR key (not `relationship_id`, which is a per-run uuid4) and a
> SECOND field, since `include_columns` is itself LLM-judged and the pair can't rebuild the shape.
- Add `considered_relationship_pairs: list[list[str]] | None` (JSON `[[from_column_id, to_column_id]]`)
  — the candidate FK column-pairs already judged this fact (exposed **or** rejected-by-LLM) — AND
  `exposed_dimension_joins: list[dict] | None` (the full exposed-join specs incl. `include_columns`),
  distinct from `relationship_ids` (the per-run-id exposed subset).
- `uv run python -m dataraum.storage.dump_ddl` → regenerate `schema.sql`; `bun run db:pull:metadata` in
  the cockpit; confirm the `schema-drift` CI gate is green.
- **AC:** the field exists, defaults null (first run / legacy rows read as "nothing decided yet").

### P2 — Inherit-on-re-run + incremental LLM
- In `_run`, before the enrichment call, load each fact's prior `EnrichedView`. Per fact compute:
  - `candidates` = the fact's defined relationships ≥ `_MIN_CONFIDENCE` (already loaded as
    `all_relationships`).
  - `undecided` = `candidates − considered_relationship_pairs`.
  - `rejected` = relationships dropped from the catalog or suppressed via the relationship reject overlay
    (`load_suppressed_relationship_pairs`).
- Call `_get_llm_recommendations` **only for `undecided`** (and skip the call entirely when every fact's
  `undecided` is empty). First run: `considered` is null → all candidates undecided → unchanged behaviour.
- Assemble `dimension_joins = inherited_exposed_joins + new_LLM_joins − rejected`, where
  `inherited_exposed_joins` is rebuilt from the prior `relationship_ids`.
- Persist on the `EnrichedView` upsert: `relationship_ids` = exposed set; `considered_relationship_pairs` =
  `prior_considered ∪ undecided`.
- **AC:** re-running a session with an unchanged catalog makes **no** enrichment LLM call and yields the
  identical join set; a newly-confirmed relationship is the only thing that triggers the LLM, and only for
  itself; a rejected relationship drops its join.

### P3 — Reconcile the lake-grain column write (monotonic)
- `_register_and_profile_dim_columns`: when the enriched `Table` already exists, **diff** the dimension
  column set — add new columns (+ profile them), keep existing columns and their `StatisticalProfile`s,
  and remove only columns whose relationship left the exposed set. Do not delete-all-then-reinsert when the
  set is unchanged.
- Keep the recipe no-op (`sql_equivalent`, ~303) — an unchanged shape stamps no new recipe version.
- **AC:** an unchanged shape leaves the enriched `Table`'s `Column` rows and profiles untouched (no churn);
  a grown shape adds columns without disturbing the existing ones.

### P4 — Determinism test + integration
- Unit/integration: run the enriched_views phase twice over the same session (stub the enrichment LLM to a
  *different* verdict on the second call) and assert the shape is **inherited**, not re-judged — the
  second verdict is ignored for already-considered relationships.
- Assert the monotonic contract: new confirmed relationship → column added; reject → column removed;
  otherwise stable.
- Run `tests/integration/worker` (the begin_session phase-stub list is hardcoded — a phase-shape change
  must not break it) before declaring done.

## Out of scope / non-goals

- Making the enrichment LLM deterministic, or a rules-based view builder (explicitly rejected — see ADR).
- Touching Layer A (relationship_discovery / silent-accept / keeper materialization).
- Re-deciding an exposed column when its dimension table later changes — that is an explicit teach/reject,
  not automatic (silent-accept contract).
- The `aggregation_view` substrate (already dropped by the 2026-06-17 amendment).

## Risks / watch

- **First-run / legacy rows:** null `considered_relationship_pairs` must read as "decide everything," so the
  first run after deploy re-establishes the shape exactly as today (no surprise wipe).
- **Reject path:** confirm the relationship reject/teach signal reaches this phase as a removal, so a
  user-rejected dimension actually drops its column (monotonic-down on explicit signal).
- **Cross-consumer:** `dimension_coverage` and slicing read the enriched columns — the determinism test
  should cover that a stable shape keeps their reads stable across re-runs.
