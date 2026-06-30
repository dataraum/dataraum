# ADR-0017 — Validation verdicts: computed on demand from a contracted SQL output

- **Status:** Accepted
- **Date:** 2026-06-30
- **Ticket:** DAT-617 (epic DAT-543)
- **Design doc:** Confluence DD space (DAT-543 epic)

## Context

A validation's pass/fail is a first-class cockpit surface — practitioners read "do the
books balance / does AR tie out" directly. It must be **fresh**: a stored verdict goes
stale the moment data is re-imported, the run-versioned SQL does not. This is the same
shape the metric path already settled (ADR-0016): there is **no stored metric value** —
`sql_snippets` holds the SQL, the cockpit re-runs it on the lake on demand. `validation_results`
storing `status/passed/message/details` is the inconsistency — a pushed value that rots.

Two further problems surfaced while grounding the cut:

- **The judgement guesses its own input by string-matching column names.** `evaluate_result`
  doesn't know which result column carries the answer, so it matches against a hardcoded
  vocabulary (`difference|diff`, `equation_holds|is_valid`, `orphan_rate|violation_rate|…`).
  An off-vocabulary column name silently degrades the check to "inconclusive." That is the
  brittle heuristic-matching the codebase forbids ("if the LLM can decide, don't build a
  heuristic pre-pass").
- **Declared params have one home — the config.** `tolerance`/`check_type`/`severity` live in
  the vertical config (base ⊕ DB overlay, read via `VerticalLoader` / the cockpit's reader).
  Storing them on the result row would duplicate config into the DB.

## Decision

**The validation verdict is computed on demand, never stored — and the judgement reads a
contracted output column instead of guessing it.**

1. **`validation_results` drops the data-derived verdict** (`status, passed, message, details`)
   and becomes a run-versioned SQL store: `validation_id, run_id, table_ids, columns_used,
   sql_used, executed_at`. It **keeps the declared judgement params the in-run engine detector
   needs** — `severity` and `tolerance` (both non-stale config scalars). This is a deliberate
   denormalization: the entropy/detect layer carries **no vertical**, so the
   `cross_table_consistency` detector cannot read config the way the cockpit can, and threading
   the vertical down through `take_snapshot` → the detector is a disproportionate,
   calibration-blind cross-cutting change. The cockpit reads its params from the spec reader
   (config base ⊕ overlay) it already has; lifecycle state + reason stay in `lifecycle_artifacts`.
   (The "pure SQL store, all params from config" form remains the north star — a follow-up if
   the vertical is ever threaded into the detect layer.)

2. **The validation SQL output is contracted, killing the string-match.** The generated SQL
   aliases its verdict-bearing measure to a fixed name — `deviation` (and `magnitude` where a
   relative scale is needed); `constraint` checks keep "zero rows = pass." The contract is
   asserted **where the SQL actually runs** (execute / on-demand): a non-conforming output (a
   summary check missing `deviation`) is a loud grounded-with-reason failure carrying that
   reason, never a silent inconclusive. `evaluate_result`'s column-name vocabulary is deleted.

3. **The judgement is then a trivial threshold, applied at every consumer.** `passed =
   deviation <= tolerance` (constraint: `violations == 0`). The engine in-run consumers
   (cycle health, graph context, the `cross_table_consistency` detector) apply it in Python
   (`validation/evaluate.py`); the cockpit applies the **same two-line rule in TS** after
   re-running `sql_used` on the lake. The two copies are pinned by **one shared fixture set**
   (`rows + check_type + tolerance → verdict`) run in both pytest and vitest — drift is a test
   failure, not a production surprise. (A two-line threshold is not the duplication risk; the
   deleted 150-line shape-sniffing was.)

## Consequences

- **Engine:** `ValidationResultRecord` slimmed; `evaluate_result` collapses to the threshold;
  `evaluate_validation(conn, sql_used, spec)` is the on-demand entry the in-run consumers pull.
  `cross_table_consistency` reads the contracted `deviation`/`magnitude` — same numbers, new
  column names → a **re-verify**, not a recalibration.
- **Cockpit:** `look-validation` + widgets mirror `look-metric` — lifecycle state + reason —
  and compute the fresh verdict by running `sql_used`; a small TS verdict mirror + the shared
  fixtures.
- **Config / schema:** `validation_sql` prompt tightened to the output contract; `schema.sql`
  re-dump + cockpit drizzle re-pull (the `schema-drift` gate spans both, so this lands as one
  cross-package cut).
- **Calibration:** the reading change (verdict on demand) is calibration-neutral; the only
  touchpoint is the detector's contracted input columns → re-verify recall/precision unchanged.

## Alternatives rejected

- **Mirror the column-name string-matching into TS.** Copies the rot into a second language;
  the heuristic should be deleted, not propagated.
- **Store the verdict but refresh it.** Any stored verdict is stale between refreshes — the
  problem this ADR exists to remove.
- **Route the cockpit's verdict through a Temporal activity.** Too heavy for a display read;
  the cockpit already executes SQL on the lake directly (the metric precedent).
- **Self-evaluating SQL** (the SQL returns the boolean verdict). Rejected: the SQL *measures*
  (`deviation`), the consumer *judges* with the declared `tolerance` it owns — keeping the
  threshold out of the generated SQL and uniform across consumers.
