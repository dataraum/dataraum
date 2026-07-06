# Epic: Relational grounding — metrics ground through real structure, stably

> **DRAFT — pilot epic for ADR-0019. Not approved. Baselines and two measure
> scripts are pending; the definition PR is approvable only when every KPI has a
> runnable measure and a baseline captured on main.**

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
- Loss-weight refitting (the DAT-668 evidence thread) — separate epic if pursued.
- New vertical content; this epic runs on the existing finance oracle instances,
  but everything it builds goes through the oracle registry
  (`scorecard/scorecard.yaml`), nothing finance-hardwired.

## Oracle extension (fail-to-pass)

Two measure scripts are part of this epic's definition work and must **fail on
main** before approval:

1. `grounding-report` — runs the pipeline on an oracle dataset and emits
   `{executed_correct, proxy_grounded, total}` by diffing metric values against
   the oracle's ground truth (value-level, the discipline that caught the 48%
   revenue error).
2. `stability-report` — two consecutive `begin_session` runs on unchanged data,
   diffing grounding verdicts (declined composites, metric lineage) and emitting
   the count of flips.

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
    target: ">= 26"
    baseline: null        # last observed fresh run: 22/34 — recapture on main at approval
    tier: promotion
    measure: null          # grounding-report on oracle `clean-corpus` — oracle extension #1
  - id: booksql_signature
    statement: BookSQL ceiling metrics (dso, current_ratio) execute with correct values via account-classification joins, no payment-instrument proxies
    target: "== 1"
    baseline: null
    tier: promotion
    measure: null          # grounding-report on oracle `booksql` (see its data caveats in the registry)
  - id: rerun_stability_flips
    statement: Grounding-verdict flips across two consecutive runs on unchanged data
    target: "== 0"
    baseline: null
    tier: promotion
    measure: null          # stability-report — oracle extension #2
promotion:
  strategies: [clean, detection-relationship-cal-v1]
  seed_policy: fresh
  live_budget: "grounding-report + stability-report runs at promotion, plus one lean calibration pass (~15% calibrate-all)"
  live_at_promotion: true  # blast radius is LLM-semantic — PR-time live leg authorized
rubric: none
```
