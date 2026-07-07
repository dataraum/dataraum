# Entropy as disagreement

Data quality is not a property of data in isolation — it is **disagreement
between witnesses**. Typing, statistics, semantic annotation, and relationship
evidence each testify about the same column; where they disagree, the
disagreement is measured, pooled, and priced. There is no global "bad data"
threshold anywhere in the system.

- **Severity is contextual.** The same conflict costs differently per
  analytical intent: a unit ambiguity can block aggregation and barely matter
  for reporting. Readiness is a per-intent price of acting on the data, never
  a universal grade.
- **Conflict and ignorance are different states.** Witnesses that disagree and
  witnesses that are silent produce different signals; collapsing them would
  hide exactly what a practitioner needs to know.
- **Every score is explainable end-to-end** — it traces to named witnesses and
  their claims. Free text explains; it never gates.
- **Correctness is proven against ground truth, not asserted by tests.** The
  oracle is generated data with known injections: a detector is correct when
  it finds what was planted (recall) and stays quiet on clean data
  (precision), under calibration. A detector that misses a known injection is
  wrong — this requirement admits no reinterpretation.
- **The system learns by being taught, not tuned.** A human resolution closes
  a disagreement as durable evidence. Hand-adjusting thresholds to make a
  metric pass is not a move that exists.
