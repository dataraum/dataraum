# ADR-0024 — One resolution home per drill question; served facts ship with consumers

Status: **Proposed** (2026-07-29, from the five-audit sweep on `epic/dat-671-phase3`)

## Context

A five-agent audit of the drill/axis/verdict machinery (cockpit overlap trace, engine
overlap trace, artifact-consumption census, string-heuristic census, finance-coupling
sweep) measured the state of the serving substrate:

- The drill layer answers five canonical questions — *source, identity, axes,
  permission, total* — at ~40 cockpit sites and ~35 engine sites. The snippet semantic
  key alone is hand-restated 7×; stock/flow materialization is decided at 5 sites with
  two opposing conflict policies; "two facts share an axis" exists at three strictness
  bars plus an unread SQL edge.
- The cockpit's three compose paths (node / parts / tier-A) implement the questions
  independently: the same data gets different capabilities per path (time bucketing
  offered / hardcoded-withheld / silently absent; a footer on one path of three), and
  the parts path refuses verdicts one function call away from the resolver that reads
  them.
- Of 42 persisted analysis tables, 40% are fully consumed; 26% have a dead cockpit
  mirror; 7% have zero readers anywhere; 31 individual output columns are write-only.
  15 of 26 property-graph element views have no reader; the graph is MATCHed by exactly
  3 queries touching 3 of 16 edge labels. The disclosure layer (every "why / evidence /
  withheld" channel, including `PhaseResult.warnings`, which the Temporal contract
  drops by construction) is written and unread while the verdict column beside it is
  consumed.
- Name-keyed joins (~43) are the dominant fragility: `axis_key` is a served column
  NAME mirrored as a literal across packages (an alias silently drops a
  non-additivity verdict), and the entropy target key composes NAMES rebuilt
  byte-identically at three sites across two packages.

## Decision

1. **Each of the five questions has exactly one resolution home**, keyed on stable
   identity (ids; the snippet semantic key as a shared dataclass; concept =
   `standard_field`), never on bare names. Names are display, not keys. The chain
   `snippet → standard_field → verdict target` is the identity spine; it already
   exists in the schema and becomes the only way any path resolves a target.
2. **Compose paths are thin callers.** A capability difference between node / parts /
   tier-A must trace to a *data* difference (no verdict exists), never to a *path*
   difference (this route doesn't look). The parts and tier-A paths consult the same
   verdict resolution the node path uses; tier-A resolves aliased projections to their
   source columns through the existing AST reader before intersecting with the catalog.
3. **A served fact ships with its first consumer, or it does not ship.** New tables,
   views, and output columns land in the same change as a reader (engine prompt,
   cockpit surface, or eval oracle — named in the PR). Every existing zero-reader
   surface on the census gets an explicit wire-or-delete decision; "keep, someone
   might read it later" is not an outcome.
4. **Acceptance is the journey suite**: practitioner journeys over the reference
   corpus, run as integration tests against the real routes in the cockpit IT harness,
   asserting both numbers (from the corpus answer key) and affordances (which axes,
   which grains, what the total row shows). A resolver change is done when the
   journeys pass, not when its unit tests do.

## Consequences

- The parts-path verdict hardcode, the tier-A alias loss, and the grain-control
  path-gating are removed by construction, not patched.
- `metric_unit_grain` (this epic's B1 substrate) gets its first consumer or is
  deleted — decided, not deferred.
- The wire-or-delete triage list (15 og views, 16 dead mirrors, 31 dead columns, the
  disclosure-channel architecture, the entropy name-key) is worked as explicit
  decisions in follow-on slices; this ADR makes the *rule*, not the full cleanup.
- Cross-package duplicated literals (curation order, `'*'` sentinel, grain ladder)
  become mirrored-with-test or served, per the existing hand-mirrored-contract
  discipline.
