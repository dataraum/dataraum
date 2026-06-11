# ADR-0009 — Entropy measures disagreement between witnesses; no deterministic semantic overrides

- **Status:** Accepted
- **Date:** 2026-06-08
- **Ticket:** DAT-442 (epic)
- **Design doc:** Confluence DD — "Entropy as Disagreement" (page 32145409)

## Context

Calibration kept "fixing" wrong LLM judgments with deterministic patches: an
exact-indicator override, a naming-quality floor, network edge weights moved
until bands flipped, per-detector boost curves fitted until scores crossed
0.3. Each patch made a metric pass without making the system understand more —
Goodhart's law eating the calibration loop. Root defect: a single LLM
judgment treated as truth, overruled by code when wrong. Rejected outright
(Philipp, 2026-06-07): "it is 2026; LLMs are here; this is a multi-agent
system."

## Decision

**Entropy is measured disagreement between witnesses over canonical claims.**

1. **Claims are canonical.** Semantic verdicts live in closed claim spaces
   (enums, ontology IDs, a unit lattice), forced by structured output at
   write time. Comparison is identity or graph distance — never string or
   embedding matching. Free text is explanation; it never gates behavior.
2. **Claims are witnessed, never overridden.** Each modeled measurement pools
   witnesses (semantic claims, statistic signatures, ontology priors
   conditional on grounding, human teaches) with **measured** reliabilities,
   and emits two numbers: **conflict** `C` (witnesses contradict) and
   **ignorance** `U` (evidence is thin). No code path replaces a claim;
   resolution is a new witness entering — a teach is an overlay row that
   becomes the dominant witness. Even a teach leaves residual *documented
   conflict* if the data still contradicts it.
3. **Severity lives in per-intent loss tables**, not in scores:
   `risk(intent) = E_q[loss_intent]`. Boost curves are dead for new work. The
   noisy-OR rollup mechanics stay (deterministic plumbing) but consume
   `risk(intent)`; hand-set `network.yaml` edge weights retire into loss
   tables.
4. **Reliabilities are calibrated artifacts with provenance** — shipped from
   the eval corpus, Beta-updated by teach adjudications, cross-checked by
   independent-witness agreement, ceiling-bounded by cross-run flicker. Never
   inline constants; never updated by a witness's agreement with a posterior
   it dominates.
5. **The resolved layer delivers semantics**: `SemanticAnnotation` becomes the
   resolved view (value + posterior + conflict flag + provenance), promoted
   and read via the ADR-0008 surface; the Column Semantic Profile is the
   per-column contract (teach vocabulary = write schema, entropy =
   adjudication state, ContextDocument = read view).
6. **The eval is Goodhart-resistant**: orderings, monotonicity, teach-deltas,
   and calibration on **generative** fixture families — never point
   thresholds on fixed fixtures. Terminal metric: bands vs ground-truth task
   outcomes, and teach closure.

## Consequences

- **Forbidden:** deterministic semantic overrides/floors/gates; new boost
  curves or severity-bent scores; point-threshold eval assertions for new
  measurements; string/embedding comparison of verdicts in production;
  reliability constants in code.
- Retires as pattern: `network.yaml` edge tuning, per-detector severity
  formulas, fixed memorizable fixtures. Existing instances are tolerated debt
  until migrated, not precedent.
- DAT-445's override framing and DAT-446's deterministic floor are dead; the
  design doc's disagreement matrix is the build list (proving slice: null
  semantics; then unit; then temporal_behavior after the stock/flow spike).
- Follow-ups: pooling-engine substrate (one generic implementation; all
  per-measurement specifics are config-with-provenance), loss-table
  authorship in verticals, Dawid–Skene independence mapping.
