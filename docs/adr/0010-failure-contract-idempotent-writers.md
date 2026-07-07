# ADR-0010 — Failure contract: (key, run_id) idempotent writers; skips and deletes are exceptions, not the mechanism

- **Status:** Accepted (the slice-substrate entries below — `slicing_views`, `temporal_slice_analysis_phase`/`processed_slice_tables`, the `temporal_slice_analyses` grain — were retired with the slice materialization by [ADR-0013](./0013-begin-session-dimension-relatedness-consolidation.md); the writer contract itself is unchanged, see `storage/read_views.py::_RUN_GRAIN_EXEMPT` for the live exempt list)
- **Date:** 2026-06-11
- **Ticket:** DAT-502 (Phase 1 of epic DAT-501)
- **Design doc:** Confluence DD/34045953 §4

## Context

Temporal activities are **at-least-once**: a worker can commit its rows, then crash
before acking, and the activity re-runs under the SAME `run_id` (success-redelivery —
the one window Temporal + Postgres cannot close). The codebase had grown a byzantine
mix of defenses against this: phase-local rollback helpers, in-`_run` "already done"
re-checks, cross-run stale skips, and run-scoped delete-then-insert clears in front of
writers — each one hand-rolled, several of them wrong (the DAT-448 stale-skip bug
class; the slice/drift duplicate-key class).

`9d262fde` settled the *failure* half: `run_phase` / `run_session_phase` roll the
session back on a FAILED phase result, so **Postgres owns within-attempt atomicity**
— a failed attempt persists nothing and the retry starts clean.

## Decision

**Writer idempotency owns success-redelivery.** Exactly two sanctioned writer forms:

- **Form (a) — the default:** a `(key, run_id)` UNIQUE constraint + ON CONFLICT
  upsert (`storage/upsert.py`), with in-batch dedup on the same key. A redelivered
  activity converges in place; a NEW run's rows coexist with prior runs'
  (the DAT-413 version axis).
- **Form (b) — the exception:** run-scoped delete-then-insert, ONLY for producers
  whose row-set can legitimately **shrink** on redelivery. Sanctioned instances:
  - `entropy_objects` — presence-keyed detector row-set; adjudication reads the
    un-run-versioned `config_overlay` live between attempts (`entropy/engine.py`).
  - `claim_witnesses` — witness sets shrink when an adjudication resolves
    differently; its UNIQUE stays as the grain guard (`entropy/engine.py`).
  - `entropy_readiness` — shrink-to-empty rollup (`entropy/readiness.py`).

**Constraints-first ordering:** never remove a delete/skip before its writer has a
DB-enforced grain.

**Structural enforcement:** `storage/read_views.py::enforce_run_grain` — every
run-stamped table carries a run-including UNIQUE or appears on the explicit
`_RUN_GRAIN_EXEMPT` list with its reason. Runs at boot and in the `schema-drift`
CI gate (via `dump_ddl`). Exempt today: the three form-(b) tables above (those
without UNIQUEs), `enriched_views` + `slicing_views` (mutate-in-place writers —
re-grain owed to DAT-501 Phase 5 / DAT-506), and `derived_columns` (skip-guarded).

**Sanctioned exceptions to one-commit-per-phase:**

- `metrics_phase` commits once **per metric** (`_execute_isolated`). Allowed because
  every per-metric write converges: snippet state is first-writer-wins (DAT-485) and
  `snippet_usage`/`execution_count` are the documented **telemetry exception** —
  not run-stamped, may inflate on redelivery, nothing gates on them.
- `lifecycle_artifacts` advance state in place within a run; `declare_artifact` is
  **declare-or-reuse**: a redelivered declare RESETS the same `(session, type, key,
  run)` row to `declared` (state_reason/grounded_against cleared) because
  `transition()` requires exact from-states — the redelivered run re-flows the
  lifecycle on the same row.

## Keep-list (deliberate, NOT redelivery hedges — do not "clean up")

- **TableEntity replace-delete** and the **relationship overlay materialization
  clear** (`relationships/materialize.py`) — LLM-nondeterministic producers whose
  row-set shrinks; form (b) by design.
- **Readiness shrink-to-empty** (`entropy/readiness.py:56-73`) — form (b).
- **`AggregationLineagePhase.should_skip`** — run-scoped structural preconditions,
  not a stale-skip.
- **`entropy/resolve.py` UPDATEs** (`resolve_null_tokens` / `resolve_temporal_behavior`)
  — same-run idempotent UPDATE writers inside the terminal detect transaction.
- **Import content-key skip** (`ImportPhase.should_skip`) — an upload source is
  content-keyed (`src_<digest>`), so raw-table presence IS the content check; the
  db_recipe arm compares recipe-hash witnesses.
- **Run-scoped redelivery skip arms** (`slicing_phase.should_skip` + its in-`_run`
  guard, `correlations_phase.should_skip`) — scoped to THIS run's rows; they
  short-circuit the LLM on redelivery, with the UNIQUEs as the DB-grain backstop.
- **Business deletes** (user-driven drops/teach) and **lake `CREATE OR REPLACE` /
  `DROP`** — the physical lake is latest-only by design (DAT-413: only metadata is
  versioned).
- **PR #280's `processed_slice_tables` consumer-side dedup**
  (`temporal_slice_analysis_phase`) — sanitized slice prefixes are not prefix-free
  (`account` vs `account_type`), so two DISTINCT definitions can route one physical
  slice table twice even with the `SliceDefinition` grain.

## Consequences

- Deleted: import `_rollback_partial_load`, correlations' in-`_run` re-check,
  slice_analysis' cross-run "All slices already analyzed" arm, the dead
  `MetadataSnapshotHead.version` counter, and the run-scoped clears in front of the
  relationship-candidate / TypeCandidate / TemporalSliceAnalysis / drift /
  measure_aggregation_lineage writers.
- New grains: `relationships` (existing UNIQUE now load-bearing for the writer),
  `type_candidates (column_id, data_type, detected_pattern, run_id)` with
  `detected_pattern NOT NULL DEFAULT ''`,
  `slice_definitions (table_id, column_name, run_id)`,
  `temporal_slice_analyses (slice_table_name, period_label, run_id)`.
- Every conversion is gated on an at-least-once test: re-execute the committed
  writer body twice under the same `run_id` with a commit between; assert
  convergence and prior runs untouched.
- `run_id` stays NULLABLE; the new UNIQUEs are NULLS-DISTINCT, so only run-stamped
  rows converge — the workflow path always stamps, tests must stamp.
- The session axis (`session_id` columns/predicates) is untouched — DAT-506 owns it.
- Existing Postgres workspaces require `docker compose down -v` (a fresh schema):
  `create_all` is additive, so it does NOT apply the three new UNIQUEs, the
  `detected_pattern NOT NULL DEFAULT ''`, or the `metadata_snapshot_head.version`
  column drop to an existing volume. There is no migration tooling — consistent
  with the disposable-workspace / clean-cut design.
