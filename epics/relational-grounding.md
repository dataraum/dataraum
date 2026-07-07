# Epic: Relational grounding — metrics ground through real structure, stably

> **DRAFT — pilot epic for docs/architecture/development-process.md. Not approved. Remaining before approval:
> (1) baselines captured on main for all three KPIs; (2) a BookSQL-specific
> oracle mapping (ground-truth file with `metric_aliases` for the dso /
> current_ratio ceiling metrics) so `booksql_signature` gets a runnable
> measure — its `measure` is still null; (3) ground-truth coverage extension
> in dataraum-testdata — the current `ground_truth.yaml` verifies at most 3 of
> the declared finance metrics, so the `clean_executed_correct` target is
> provisional until the GT export covers the metric tree and the target is
> re-set at baseline capture. The grounding and stability measures are
> runnable (see the scorecard block).**

Metrics ground through the confirmed relational structure of the workspace —
classification joins over judged relationships (including minted surrogates) —
never single-table value proxies, and the grounding verdicts are **stable
run-over-run**: a declined lookalike stays declined, a grounded metric stays
grounded on the same lineage, and what cannot ground says so honestly. A
practitioner re-running analysis on unchanged data must see the same operating
model twice. (Absorbs the residual thread previously tracked as DAT-652:
catalog-membership variance, formula-level double-count, driver-over-surrogate
acceptance.)

## Out of scope

- Cockpit surfaces (drill-down UI, canvas rendering) — read-only consumers here.
- Loss-weight refitting — separate epic if pursued. (Evidence on record, 2026-07:
  `relationship_entropy`'s current loss weights read a corpus with 20% injected
  orphans as "ready" — measured in the DAT-602 gate sweep, not hypothesized.)
- New vertical content; this epic runs on the existing finance oracle instances,
  but everything it builds goes through the oracle registry
  (`scorecard/scorecard.yaml`), nothing finance-hardwired.

## Oracle extension (fail-to-pass)

Two measure scripts implement this epic's oracle, and both **fail on main**
(the required fail-to-pass state):

1. `packages/cockpit/scripts/measure-grounding.ts` — emits
   `{value: executed_and_correct, executed, total, mismatches, unverified, …}`
   by diffing metric values against the oracle's ground truth (value-level,
   the discipline that caught the 48% revenue error). Default mode measures
   the current promoted surface (no pipeline, no LLM); `--run` drives
   add_source → begin_session → operating_model first (real LLM spend).
   **It fails on main by construction:** the promoted surface exposes no
   executed metric values (`GraphExecution.output_value` is ephemeral —
   engine `graphs/models.py`), so every executed metric classifies as
   `unverified` and scores 0. The epic's engine work must persist/expose
   executed values through the promoted read surface (not a cockpit_reader
   grant hack) to flip it; the seam is `extractMetricValue`
   (`packages/cockpit/src/lib/measure/compare-values.ts`).
2. `packages/cockpit/scripts/measure-stability.ts` — two consecutive
   `begin_session` → `operating_model` passes on unchanged data, diffing
   grounding verdicts (surrogate-intent status/membership by `intent_digest`,
   metric artifact state/lineage/reason by `artifact_key` — content identity,
   never per-run uuids) and emitting `{value: flip_count, flips}`. `--run`
   only — it inherently runs the pipeline twice.

## Honorable exit

If catalog-membership variance proves irreducible at the LLM tier (verdict flips
survive prompt/context fixes across ≥3 controlled A/Bs), file the evidence and
the measured flip rate — that converts this epic into a contract-change decision
(pooled fallback vs catalog exclusion), which is Philipp's call, not more code.

```yaml scorecard
slug: relational-grounding
areas: [engine, eval]
kpis:
  - id: clean_executed_correct
    statement: Ground-truth metrics executed with values matching the clean-corpus oracle (±0.5%)
    target: ">= 26"       # provisional — re-set at approval once the GT export covers the metric tree (banner item 3)
    baseline: null        # last observed fresh run: 22/34 executed — recapture on main at approval
    tier: promotion
    # Measures the current promoted surface; the promotion protocol runs the
    # pipeline (its live leg) first, or append --run to drive it here.
    measure: "GROUND_TRUTH_PATH=../../../dataraum-testdata/output/clean/ground_truth.yaml bun run --cwd packages/cockpit scripts/measure-grounding.ts"
  - id: booksql_signature
    statement: BookSQL ceiling metrics (dso, current_ratio) execute with correct values via account-classification joins, no payment-instrument proxies
    target: "== 1"
    baseline: null
    tier: promotion
    measure: null          # measure-grounding.ts on a BookSQL oracle file — needs the BookSQL metric_aliases mapping (see DRAFT banner; data caveats in the registry)
  - id: rerun_stability_flips
    statement: Grounding-verdict flips across two consecutive runs on unchanged data
    target: "== 0"
    baseline: null
    tier: promotion
    # Inherently live: two full begin_session → operating_model passes.
    measure: "bun run --cwd packages/cockpit scripts/measure-stability.ts --run"
promotion:
  strategies: [clean, detection-relationship-cal-v1]
  seed_policy: fresh
  live_budget: "measure-grounding --run + measure-stability --run at promotion, plus one lean calibration pass (~15% calibrate-all)"
  live_at_promotion: true  # blast radius is LLM-semantic — PR-time live leg authorized
rubric: none
```
