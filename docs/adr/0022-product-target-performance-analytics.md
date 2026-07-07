# ADR-0022 — Product target: performance analytics; deliverables graded at decision tolerance

- **Status:** Proposed (drafted from the 2026-07-06 decision text; ratified by the lead at PR review)
- **Date:** 2026-07-07
- **Ticket:** DAT-680 (decision recorded in its description, 2026-07-06)
- **Design doc:** —

## Context

The platform's deliverables and its eval were implicitly aimed at financial
reporting, grading outputs against reporting-grade exactness. A pipeline plus a
model each contribute error; reporting exactness is the wrong bar for that chain
and does not match where the product's value is: operational performance
decisions. The 2026-07-06 product review settled the target domain.

## Decision

- **The product is performance analytics — strictly and only.** Financial
  reporting is no longer a target.
- Finance survives **forward-looking only**: cash-flow forecast, DSO risk, margin
  decomposition. Reporting artifacts (trial balance, balance sheet) become
  generator-internal consistency checks, not graded deliverables.
- **Deliverables are graded at decision tolerance per KPI**, never
  reporting-grade exactness: `pipeline error + model error ≤ decision tolerance`.

**Explicitly not decided — do not treat as settled:** which verticals beyond
forward-looking finance; multi-site comparison; wedge industries. The
performance-colleague ideation material is direction, not decisions.

## Consequences

- Eval grades at per-KPI tolerance; delivering a wrong number inside a claimed
  tolerance (`wrong_delivered`) is the cardinal failure metric.
- Reporting-exactness assertions are demoted to internal generator checks;
  committed vertical builds beyond forward-looking finance were cancelled
  (supply-chain, hospitality, multi-site panels) pending the vertical decision.
- Open: per-KPI decision tolerances must be declared where KPIs are defined —
  a tolerance nobody set is exactness by default, which this decision forbids.
