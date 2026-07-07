# Epic: Result drill-down — slice any result on the shared grid, no agent round-trip

> **DRAFT — distilled from DAT-671 + DD/43417601 for the ADR-0019 migration. Not
> approved. All three KPI measures are pending (targets provisional until baselines
> are captured on main); the two oracle-extension harnesses below must exist and
> fail on main before the definition PR is approvable. The P1 keystone (Model-canvas
> metric overlay, tier A/B machinery) already merged (PRs #438/#443) — this epic
> carries the remainder.**

A practitioner slices and dices any query or stored-metric result — grouping,
filtering, hierarchy descent, time bucketing — directly on the shared result grid,
without a round-trip through the answer agent. The capability, proven on the Model
canvas metric overlay, is inherited by the answer widget, `run_sql` grids, and
report detail; axes come from engine metadata that previously had no UI consumer
(`slice_definitions`, `driver_rankings`, `dimension_hierarchies`). Drill composes a
new effective base SQL upstream of the grid (GROUP BY happens once, in SQL; charts
encode the grouped rows); drill state is grid-local and persists as **steps**, not
SQL. (Absorbs the open DAT-671 children: ad-hoc FROM-parse resolver + surface
inheritance, drill persistence, drilled-result → child report, driver-ranked
guidance + flow-gated time, chart click-to-pin + stock-over-time spike, and the
on-demand `discover_drivers` tool for ad-hoc measures.)

## Out of scope

- Engine changes beyond the already-shipped metadata read surface — this epic
  consumes `slice_definitions` / `driver_rankings` / `dimension_hierarchies` as-is;
  no new engine phases, no driver-algorithm work.
- Chat write-back: the chat canvas stays ephemeral; persistence is drill steps on
  report detail or minting a report — never mutating conversation state.
- Charting-library changes (Vega-Lite per ADR-0015) and any client-side
  aggregation of drilled results.
- Dashboards / report composition; staleness machinery.

## Oracle extension (fail-to-pass)

The cockpit has a thin oracle surface (ADR-0019), so this epic builds its two
computable oracles as part of definition work; both must **fail on main**:

1. `drill-axis-report` — for each grain-safe `slice_definitions` axis of an oracle
   metric, compose the drill SQL (canvas path and ad-hoc path) and diff the grouped
   values against the testdata generator's ground truth; emits
   `{axes_correct, axes_total}` per surface. Fails on main: the ad-hoc surfaces
   cannot drill at all yet.
2. `drill-persistence-report` — apply a scripted drill-step sequence, serialize to
   search params, rehydrate, and assert the recomposed effective SQL is identical;
   emits the flip count. Fails on main: no persistence exists.

The UX half of the epic is gated by the human rubric below, never by a numeric
proxy.

## Honorable exit

If FROM-parse resolution of ad-hoc answer/`run_sql` SQL onto catalog axes proves
unreliable on real agent-authored SQL (measured hit-rate on a collected corpus, not
anecdotes), file the evidence and cut scope to the deterministic metric path
(canvas + stored metrics + reports minted from them). That converts the ad-hoc
inheritance into a contract decision on the resolver approach — Philipp's call,
not more parser code.

```yaml scorecard
slug: result-drill-down
areas: [cockpit, engine]
kpis:
  - id: axis_grouped_values_correct
    statement: Share of grain-safe slice_definitions axes for oracle metrics whose drill SQL returns ground-truth-correct grouped values, across canvas and ad-hoc surfaces
    target: ">= 1.0"
    baseline: null        # capture on main at approval (canvas path partial, ad-hoc 0)
    tier: promotion
    measure: null          # drill-axis-report — oracle extension #1
  - id: drill_steps_roundtrip_flips
    statement: Persisted drill steps rehydrate to the identical effective SQL (flip count across the scripted sequences)
    target: "== 0"
    baseline: null
    tier: promotion
    measure: null          # drill-persistence-report — oracle extension #2
  - id: drill_latency_p95_s
    statement: p95 wall-clock seconds for a drill action's composed SQL on the oracle workspace
    target: "<= 2"
    baseline: null
    tier: checkpoint
    measure: null          # timed leg of drill-axis-report
promotion:
  strategies: []           # cockpit-surface epic; engine blast radius is read-only views
  seed_policy: fresh
  live_budget: none        # tier C Haiku fallback is the only LLM touch; covered by the rubric session
  live_at_promotion: false
rubric: "human /smoke session, scored: drill affordance discoverable from the canvas node and grid toolbar; driver-ranked chips plausibly explain variance; hierarchy descent and flow-gated time bucketing behave coherently; no dead-end drill (every applied step renders grid + chart); minting a drilled result yields a child report with parent lineage"
```
