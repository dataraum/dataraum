# ADR-0007 — Frame's output is the frozen grounding input: concept overlay rows

- **Status:** Accepted
- **Date:** 2026-05-31
- **Ticket:** DAT-382 (frame frontend + engine induction retirement), DAT-377 (engine grounding-only)
- **Design doc:** Confluence DD/27688962, DD/26968066

## Context

ADR-0004 moved concept induction from the engine to the cockpit's frame stage. That
removal opened a hole: a cold-start workspace used to bootstrap its own ontology inside
the engine's semantic phase. With induction gone, the engine needs a defined, frozen
input to ground against — and a defined behavior when that input is absent.

## Decision

The frame stage's output — and the engine's grounding input — is **`concept` rows in the
workspace's config overlay**, written by the cockpit before `add_source` runs.

- The cockpit's frame tool induces concepts from the staged schemas and writes one
  overlay row per concept, workspace-scoped, through the same seam teach uses.
- The payload mirrors the engine's ontology-concept model (name, indicators, exclusion
  patterns, temporal behaviour, role, unit fields). Optional fields the model omits are
  dropped.
- The engine materializes the rows onto the workspace ontology (upsert by name,
  last-write-wins) and grounds columns against the result — grounding only, no induction.
- **Fail-loud:** a workspace that reaches semantic grounding with zero concept rows fails
  with an error naming the missing frame step, rather than silently grounding against an
  empty concept set.

## Consequences

- The grounding path has one explicit input and one defined failure mode — no hidden
  cold-start induction.
- The concept payload is a cross-package contract: changing the engine's ontology-concept
  model or the cockpit's frame output is a coordinated edit on both sides.
- The induction prompt lives only in the cockpit; the engine's copy was deleted with the
  responsibility.
