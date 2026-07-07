# Prepared context

AI tools reason over **prepared context**: the pipeline pre-computes rich,
ontology-interpreted metadata once, so that at question time nothing has to be
discovered. The alternative — an agent exploring raw tables per question — is
what this system exists to remove.

- **Data is never rejected at the door.** Everything loads untyped; typing is
  inference; a value that fails its inferred type is quarantined, row by row.
  Ingestion failure is not a quality verdict — quality is measured later, as
  evidence.
- **The unit of analysis is the session over a set of tables**, not the file or
  connection that delivered them. Provenance ends at import; everything
  downstream reasons about tables and their relationships.
- **Structure is judged, not assumed.** Relationships, keys, and hierarchies
  enter the model as evidence-backed judgments — deterministic probing supplies
  the evidence, a semantic judge rules, and a human can teach against any
  verdict. A taught verdict is durable: it survives re-runs and is superseded
  only by new evidence or a new teach, never by a fresh coin-flip.
- **Domain knowledge is data.** Ontologies, conventions, and vocabulary are
  configuration a vertical supplies; the machinery is domain-blind. Swapping
  the analytical domain changes configuration, not code.
