# DAT-413 — Snapshot substrate + non-destructive replay (add_source)

Epic DAT-412 · Design: Confluence DD/30113794 · Slice A, Phase 1.

## Scope fence

**DO change** — engine `worker/{contracts,workflows,activities,activity,main}.py`; `pipeline/base.py` (PhaseContext) + `pipeline/phases/*` (stamp run_id, delete replay_cleanup overrides); add_source metadata models (`typing/`, `statistics/`, `eligibility/`, `semantic/`, `temporal/`, `entropy/` db_models) — add `run_id`, widen `uq_column_type_decision` + `uq_column_semantic_annotation`; new `metadata_snapshot_head` model + resolver; `entropy/detectors/loaders.py` + `entropy/readiness.py` + `column_eligibility` + external readers (look/why/contracts, graphs/context if live) → head-resolved; cockpit `db/metadata/*` (regen), `tools/replay.ts`, `temporal/types.ts`, `temporal/drive-add-source.ts`; affected tests.

**DO NOT change** — begin_session substrate (only delete its replay_cleanup + session-replay gating); `Relationship` model + `uq_relationship_columns_method` (DAT-408); DuckDB materialization / DDL versioning (DAT-414); detector scoring logic; config-overlay/teach write path; `Table`/`Column` stay un-versioned (identity anchors).

## Internal phases (green + commit after each)

1. **Substrate schema + run_id plumbing** (behavior-preserving): head model + create_all; `run_id` col on add_source metadata; mint in AddSourceWorkflow → contracts → PhaseContext; stamp on write; regen cockpit mirror. Keep delete-then-insert + cleanup → one run, identical behavior.
2. **promote_to_latest + head-resolved reads + baseline** (still single-run, identical): promote as AddSourceWorkflow final activity; switch engine + cockpit readers to head; capture baseline at workflow start, thread into detect loaders.
3. **Non-destructive cut**: widen the two constraints → `(column_id, run_id)`; remove `replay_cleanup` entirely. ≥2 runs coexist, reads-via-head pick latest. Zero-delete + cascade-not-load-bearing tests.
4. **Retire ReplayScope + simplify trigger**: strip `from_phase` gating from all workflows; delete replay-only `lookup_typed_table_id`/`lookup_raw_table_ids`; simplify cockpit `replay.ts` → full re-run; drop ReplayScope from cockpit types + drive-add-source; delete dead replay-scope tests.

## Test strategy
Engine unit (head model/resolver, run_id stamping, widened constraints allow 2 runs, promote flips head, zero-delete on re-run) + integration (add_source → teach → full re-run → readers return latest via head; detect reads run baseline; begin_session runs once via head). Cockpit: mirror compiles, replay triggers full re-run, look/why via head. Reviewer gate (senior + spec) before done; eval handoff for replay-path recalibration.

## Risks
Cross-package atomicity (mirror regen + read-switch land with constraint-widening, phases 2→3); begin_session becomes run-once (verify cockpit doesn't re-invoke expecting replay); baseline-threading shape for `loaders.py` (resolve in phase 2 before committing the interface).
