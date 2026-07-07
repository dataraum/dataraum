# Epic: Eval grades the agent layer — metadata ground truth at decision tolerance

> **DRAFT — distilled from DAT-680 P0+P1 (plus DAT-669) for the ADR-0019 migration.
> Not approved. Targets are provisional until baselines are captured on main.
> DAT-680's P2 (vertical-agnostic testdata protocol) and P3 (wild-data gate) are
> deliberately excluded — they become their own epic files when actually next.
> The oracle extension is cross-repo: the `metadata_truth.yaml` export lands in
> dataraum-testdata; the assertion layer lands in dataraum-eval. Fill
> `promotion.strategies` from `scorecard/scorecard.yaml` at approval.**

Every agent-layer artifact the pipeline persists as truth — relationships, table
and column labels, cycles, validation SQL, the metric graph's own SQL, driver
rankings — is graded against generator-exported ground truth, closing the gap
where detectors are well graded but everything in the `current_*` views is
asserted by nothing. Grading is at **decision tolerance per KPI** (pipeline error
+ model error ≤ decision tolerance — ADR-0022), never reporting-grade exactness.
No new framework: assertions reuse the existing grammar (ordering + margin,
measured clean bands, `xfail(strict=False)`, pooled C/U) plus named set statistics
(precision/recall/F1/Jaccard) for set-valued outputs; the tool surface
(look/measure/sql) stays fixed. (Absorbs DAT-681–688 and DAT-669: the measurement
contract, the `metadata_truth.yaml` export, relationship-catalog F1 across seeds —
the missing DAT-667-class evidence — label/cycle assertions, the product answer
path, the driver ordering probe, and the measure-tool grain / `dimension_coverage`
outlier root-cause.)

## Out of scope

- Breaking the generator's finance hardcoding / vertical protocol (DAT-680 P2) and
  any vertical build — vertical selection, multi-site panels, lever ontologies are
  explicitly undecided (ADR-0022).
- The wild-data external-corpus lane (DAT-680 P3: WWI, CTU Financial, RelBench,
  Raha/Baran) and its curation checklist.
- Model-swap batteries (MMTU) and cassette record/replay infrastructure.
- Engine behaviour changes: this epic measures; fixing what the measurements
  expose is follow-on work (e.g. the relational-grounding epic).

## Oracle extension (fail-to-pass)

dataraum-testdata exports `metadata_truth.yaml` — the agent-layer ground truth the
generator already knows (true FK topology, table roles, semantic roles, stock/flow,
units, formulas, cycles). A new assertion layer in dataraum-eval reads the
`current_*` views and compares against it. Fails on main by construction: no
agent-metadata assertion exists today. The `/ground` kill gate applies to every
new measurement — CUT remains the default outcome for anything that cannot be
graded honestly.

## Honorable exit

If an artifact class proves inherently run-variant beyond what the pooled /
ordering-with-margin grammar absorbs across ≥ 3 controlled seeds (the DAT-667
pattern), file the per-class measured variance and mark the class
ungradeable-as-persisted. That is a contract decision about what the pipeline may
persist as truth — Philipp's call — not a mandate for more assertion code.

```yaml scorecard
slug: eval-agent-ground-truth
areas: [engine, eval]
kpis:
  - id: relationship_catalog_f1
    statement: Relationship-catalog membership F1 vs metadata_truth, pooled across >= 3 seeds
    target: ">= 0.9"       # provisional — recalibrate against the first measured run
    baseline: null
    tier: promotion
    measure: null          # /ground F1 probe over metadata_truth.yaml (the DAT-683 evidence)
  - id: artifact_class_assertion_coverage
    statement: Agent artifact classes (relationships, labels, cycles, validation SQL, metric-graph SQL, drivers) wired with at least one calibrated assertion
    target: "== 6"
    baseline: null         # 0 on main
    tier: promotion
    measure: null          # count from the eval scoreboard registry
  - id: wrong_delivered_clean
    statement: wrong_delivered outcomes (outcomes.py map — the cardinal metric) on the clean corpus at declared per-KPI tolerances
    target: "== 0"
    baseline: null
    tier: promotion
    measure: null          # outcomes report over the clean-corpus run
promotion:
  strategies: []           # fill from scorecard/scorecard.yaml at approval
  seed_policy: fresh
  live_budget: "relationship-F1 + outcomes legs across 3 fresh seeds at promotion (lean)"
  live_at_promotion: true  # blast radius is LLM-semantic — the graded subject IS agent output
rubric: none
```
