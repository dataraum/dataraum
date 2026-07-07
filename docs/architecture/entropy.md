# Entropy

What the quality layer measures and how the measurement stays honest. Engine
code lives under `packages/engine/src/dataraum/entropy/`; every tunable number
lives in `packages/dataraum-config/entropy/`.

## Entropy is measured disagreement between witnesses

- Semantic verdicts live in closed, canonical claim spaces (enums, ontology
  ids), forced by structured output at write time. Comparison is identity or
  graph distance — never string or embedding matching in production. Free text
  is explanation; it never gates behavior.
- Each modeled measurement pools witnesses (`entropy/pooling/pool.py`) into a
  log-linear posterior plus two numbers: **conflict** `C` (a generalized
  Jensen–Shannon divergence, normalized so it reads as the fraction of maximum
  disagreement — weight-robust: full disagreement reads `C = 1` whatever the
  reliabilities) and **ignorance** `U` (evidence thinness `κ / (κ + m)`, where
  the informative mass discounts witnesses by reliability and by how sharp
  their distribution is). `C` says witnesses contradict; `U` says nobody
  qualified weighed in — distinct signals, and they, never the point posterior,
  drive severity.
- Witnesses **abstain to uniform** when they have no opinion, and every
  measurement carries at least one witness that reads the data, not the column
  name (name-correlated witnesses fail together).
- **No code path overrides a claim.** Resolution is a new witness entering: a
  teach is an overlay row (`core/overlay.py`) that re-enters the next run as
  the dominant witness, and residual conflict stays documented if the data
  still contradicts it. Deterministic semantic overrides, floors, and gates are
  forbidden.
- Reliability is the *resolution* weight only — because `C` is weight-robust,
  an uncalibrated reliability can mis-resolve a posterior but never hide a
  disagreement. That is what makes cold-start on placeholder priors safe.

## A measurement ships as seven pieces

1. **Claim space** — a small explicit enum in the measurement module
   (`entropy/measurements/`).
2. **Witness extractors** — pure functions, one per witness; each reads one
   declared input (`entropy/detectors/loaders.py`) and emits a distribution
   over the claim space.
3. **Detector shell** — an `EntropyDetector` subclass registered in
   `entropy/detectors/base.py`; it loads inputs, calls the measurement, and
   emits objects. The shell contains no math.
4. **Config rows** — a `loss.yaml` entry (per-intent weights) and a
   `reliabilities.yaml` entry (per-witness trust).
5. **Resolve write-back** — `entropy/resolve.py` lands the decided value on the
   consumer-facing row inside the terminal detect transaction.
6. **Teach applier** — the overlay merger for the teach type that closes the
   measurement; the teach re-enters the next run as a witness input.
7. **Eval row** — a coverage entry plus recall/precision fixtures and a
   reliability-rig block in `dataraum-eval`; a registered detector without all
   seven pieces is visibly incomplete there.

Shared substrate — the pooling engine, detector base, resolve and overlay
frames — changes only as a coordinated edit, never inside one measurement's
implementation.

## Zero tunable numbers in detector code

- Measurement and detector code may contain claim spaces and structural logic,
  never tunable numbers. Thresholds, signal strengths, reliabilities, and loss
  weights live in `dataraum-config/entropy/*.yaml` with provenance recording,
  per measurement, whether a value is rig-measured (`calibrated`) or a
  placeholder prior. Code-level defaults exist only as cold-start fallbacks
  when the artifact has no entry.
- Implementation and calibration are separate activities on separate artifacts:
  a measurement ships with placeholder priors; the eval rig replaces them with
  measured values. Nobody edits a constant to make a test pass.

## Readiness is a transparent per-intent loss rollup

- Per measurement and intent: `risk(intent) = clamp01(Σ_signal weight · value)`
  (`entropy/loss.py`), with the weights in `dataraum-config/entropy/loss.yaml`
  per measurement × intent (`query_intent` / `aggregation_intent` /
  `reporting_intent`). A weight named `score`/`conflict`/`surprise` scores the
  measurement's primary value; any other name (e.g. `ignorance`) scores the
  worst matching evidence signal.
- A column's per-intent risk is its **worst measurement** (max); the bands are
  `risk ≤ 0.3` ready, `≤ 0.6` investigate, else blocked (`readiness_bands` in
  `loss.yaml`).
- **No Bayesian network exists, and no graph of hand-set edge weights** — the
  loss table is the whole rollup, and severity is never baked into a
  detector's score.
- Severity lives in the loss table, not the score: the same conflict is
  catastrophic for aggregation and mild for an exploratory query that can hedge
  on a caveat.
- A detector with no loss row is informative context only — it never gates
  anything.
- The rollup runs in the terminal detect step and persists per column,
  run-scoped (`entropy/readiness.py`); the cockpit reads it from the workspace
  schema with no engine round-trip.

## Ground truth is the oracle

- Detector correctness is **recall** (finds the known injections in generated
  corpora with injection maps, `dataraum-testdata`) and **precision** (quiet on
  clean data), proven by calibration in `dataraum-eval` — never by unit tests.
  A detector that misses a known injection has a bug.
- Eval assertions are orderings, monotonicity, teach-deltas, and pooled rates
  on generative fixture families — never point thresholds on fixed,
  memorizable fixtures.
