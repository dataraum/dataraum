# ADR-0011 — Entropy measurements ship as the 7-piece measurement pack; all numbers live in config

- **Status:** Accepted
- **Date:** 2026-06-10
- **Ticket:** DAT-442 (epic); pattern proven by DAT-457 (null_semantics) + DAT-445/491 (temporal_behavior)
- **Design doc:** Confluence DD/32145409 ("Entropy as Disagreement", v6)

## Context

The framework decision (already cited across the codebase as ADR-0009, recorded here):
entropy measures **disagreement between witnesses over a canonical claim**, split into
conflict (witnesses contradict) and ignorance (nobody qualified weighed in); severity
lives in per-intent loss tables, never in the score; teaches enter as witnesses, never
as overrides. Two measurements shipped this way end-to-end (null_semantics,
temporal_behavior) and the backlog now scales to many more, implemented in parallel by
independent lanes and **calibrated from data afterwards**. That only works if every
measurement has the same anatomy and zero tuned numbers in code.

## Decision

Every entropy measurement ships as the same seven pieces — the shape extracted from the
two shipped measurements, with `null_semantics` as the reference implementation:

1. **Claim space** — a small explicit enum in the measurement module
   (`measurements/null_semantics.py:40`, `measurements/temporal_behavior.py:55`).
2. **Witness extractors** — pure functions, one per witness; each reads ONE declared
   input (via `detectors/loaders.py`), emits a distribution over the claim space, and
   **abstains to uniform** when it has no opinion. At least one witness must read the
   data, not the column name (the v6 entry criterion).
3. **Detector shell** — an `EntropyDetector` subclass that loads inputs, calls the
   measurement, emits one `EntropyObject` (+ `WitnessClaim` list) per target, and is
   registered in `detectors/base.py`. The shell contains no math.
4. **Config rows** — a `loss.yaml` entry (per-intent signal weights) and a
   `reliabilities.yaml` entry (per-witness trust), both shipped as **placeholders with
   `calibrated: false` provenance**. Code-level `DEFAULT_RELIABILITIES` exist only as
   cold-start fallbacks when the artifact has no entry.
5. **Resolve write-back** — a `resolve.py` function that lands the decided value on the
   consumer-facing row (`SemanticAnnotation.*`) inside the terminal detect transaction.
6. **Teach applier** — the overlay merger (`core/overlay.py`) for the teach type that
   closes the measurement; the teach re-enters the next run as a witness input.
7. **Eval row** — a `calibration/detector_coverage.yaml` entry (disposition, witnesses,
   coverage cells) plus recall/precision fixtures and a reliability-rig block in the
   eval repo. The no-orphan test makes this mandatory.

**The no-constants rule:** detector and measurement code may contain claim-space
definitions and structural logic, **never tunable numbers**. Thresholds, signal
strengths, reliabilities, and loss weights live in `dataraum-config` with provenance,
so implementation (lanes, parallel) and calibration (batch runs, data-driven) are
separate activities on separate artifacts. Implementation ships with placeholders;
calibration flips them with measured values and provenance; nobody edits a constant to
make a test pass.

Shared engine code is off-limits to lanes: `pooling/` (the generic C/U engine),
`detectors/base.py`, `engine.py`, `resolve.py`'s frame, `core/overlay.py`'s frame,
`models.py`. A lane that needs a shared-code change routes it through the integrator.

## Consequences

- New measurements become mechanical: ~350–400 lines (measurement + shell) against this
  checklist, implementable in parallel worktrees with no cross-lane coupling except the
  two config files and the registry (integrator-merged).
- Calibration is uniform: one batch run feeds every measurement's rig, and the
  outcomes scoreboard (eval `calibration/outcomes.py`) is the target loss weights are
  fit against — bands must predict wrong answers, not injections.
- Forbidden: numeric constants in detector/measurement code (known debt to migrate:
  `temporal_behavior.py` `CONTESTED_MIN_CONFLICT`/`_DEFAULT_CONFIDENCE`,
  `null_semantics.py` `RESOLVED_IS_NULL_THRESHOLD`/vocabulary signal strengths);
  deterministic semantic overrides (the firewall stands); pooling without a
  data-grounded witness (name-correlated witnesses fail together — measured).
- The eval's coverage lock extends to this pack: a registered detector without all
  seven pieces is an incomplete lane, visible as unfilled coverage cells.
