# Grounding

A number delivered to a user is **grounded in evidence, authored by a model,
judged by the harness** — three roles that never merge.

- **Deterministic evidence in, semantic judgment out.** The system feeds the
  authoring model everything it can establish mechanically — value
  distributions, confirmed concept-to-value bindings, domain conventions,
  reliability markers — and the model contributes exactly the part that is
  irreducibly semantic. The quality gate is the evidence feed, not a check
  bolted on after.
- **Honest abstention beats a plausible wrong number.** A metric that cannot
  be grounded says so, with its reason. Low confidence flags a delivered
  result visibly; nothing in the path fabricates certainty, and nothing
  silently blocks.
- **Durable knowledge is the query, never the value.** Values are recomputed
  on demand from stored SQL. A stored number goes stale silently; a stored
  query cannot.
- **Post-execution verification is a floor, not the gate.** It catches
  unsupported results; it is structurally blind to plausible-but-wrong ones —
  which is why the investment goes into the evidence, not the checker.
- **A judgment rule exists once.** Where the same verdict must be computed in
  two languages, one shared truth table pins both implementations; divergence
  is a test failure, not a production surprise.
