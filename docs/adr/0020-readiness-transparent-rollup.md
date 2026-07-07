# ADR-0020 — Readiness is a transparent per-intent expected-loss rollup; the Bayesian network is retired

- **Status:** Accepted (records shipped reality)
- **Date:** 2026-07-07
- **Ticket:** DAT-394 (BBN retirement), DAT-442 (loss layer)
- **Design doc:** Confluence DD/28803073, DD/29163521 (historical — superseded in part by the second wave recorded here)

## Context

Per-column readiness (can this column be queried / aggregated / reported on?) gates
entropy contracts and feeds the cockpit's `why`/`look` tools. It was produced by a
hand-authored Bayesian network (`pgmpy`): CPDs and edge strengths were uncalibrated
guesses, its distinctive do-calculus output had zero live consumers, and the cockpit
(TypeScript, per ADR-0004) could never reach a live Python BBN anyway. For a project
whose bar is "calibration outranks unit tests", the signal that gated contracts was
itself uncalibratable. An interim replacement (deterministic noisy-OR over the old
edge strengths) preserved the same problem in lighter form: hand-set graph weights
with no provenance.

## Decision

Readiness is **expected loss per intent, computed from measured disagreement** — no
probabilistic network, no graph of hand-set edges.

- Per measurement and intent: `risk(intent) = clamp01(Σ_signal weight · value)`,
  where the signals are the measurement's disagreement outputs (conflict `C`,
  ignorance `U` — ADR-0009), never the point belief. A column's per-intent risk is
  its **worst measurement** (max); the worst intent yields the collapsed band the
  contract gate reads.
- The weights live in one config table (`dataraum-config` `entropy/loss.yaml`),
  per measurement × intent (`query` / `aggregation` / `reporting`). Severity moves
  out of detector scores and into this table: the same conflict is catastrophic
  for aggregation but mild for an exploratory query. Weights are placeholder
  priors to be calibrated from generative families — never tuned to pass a metric.
- Banding is preserved from the network era: risk ≤ 0.3 ready, ≤ 0.6 investigate,
  else blocked — the v1 target was behavioural parity, since the BBN was never
  itself calibrated against readiness labels (there was no oracle to beat).
- The rollup runs in the terminal `detect` step and is persisted per column
  (run-scoped, delete-before-insert, self-refreshing on replay); the cockpit reads
  it via Drizzle with no engine round-trip.

**Rejected:** keeping the BBN and persisting its output (ships an uncalibrated prior
plus a heavy dependency and dead do-calculus); a live engine query per `why` call
(breaks the ADR-0004 boundary, adds worker latency to chat); reimplementing the
network in TypeScript (duplicates engine analysis in the agent tier).

This completes ADR-0009 §3: its transitional "noisy-OR rollup mechanics stay" no
longer holds — the network and its config are deleted; the loss tables are the
whole rollup.

## Consequences

- Adding a measurement to readiness is one loss-table entry with provenance, not a
  graph edit; driver evidence for `why` is each measurement's transparent per-intent
  contribution, replacing do-calculus.
- Calibration has a single surface: the loss weights (and the measured
  reliabilities behind `C`/`U`), all config-with-provenance.
- Forbidden: reintroducing graph/edge-weight rollups or severity baked into
  detector scores; readiness signals with no loss row silently gating anything
  (no-loss detectors are informative context only).
- Open: the placeholder weights are uncalibrated until the generative-family
  calibration lands; the 0.3/0.6 bands are inherited, not derived.
