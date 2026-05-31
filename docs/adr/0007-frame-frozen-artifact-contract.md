# ADR-0007 — Frame frozen-artifact contract: concept overlay rows as the engine↔cockpit grounding input

- **Status:** Accepted
- **Date:** 2026-05-31
- **Ticket:** DAT-382 (frame frontend + engine induction retirement), folds in DAT-377 (engine grounding-only)
- **Design doc:** Confluence DD/27688962 (agent-tier boundary), DD/26968066 (relocating ontology induction upstream)

## Context

ADR-0004 drew the tier boundary: induction leaves the engine for the cockpit agent
tier. DAT-382 lands that cut — the cockpit `frame` stage induces the ontology and the
engine's `induce_adhoc_concepts` is deleted. That removal opens a hole: a cold-start
`_adhoc` workspace used to bootstrap its own ontology inside the `semantic_per_column`
phase. With induction gone, the engine needs a defined, frozen input to ground against,
and a defined behavior when that input is absent.

## Decision

The **frozen frame artifact** the engine grounding consumes is **`concept` `config_overlay`
rows**, written by the cockpit `frame` stage before `add_source` runs.

- **Producer (cockpit):** the `frame` tool (`packages/cockpit/src/tools/frame.ts`) induces
  concepts from the DAT-381 `ConnectSchema` and writes one `config_overlay` row per
  concept — `type="concept"`, `payload={"vertical": "_adhoc", ...OntologyConcept fields}`,
  workspace-scoped (`session_id=null`) — through the same Drizzle seam `teach` uses.
- **Payload contract:** the payload field set mirrors the engine's `OntologyConcept`
  (`analysis/semantic/ontology.py`): `name` (required) + optional `description`,
  `indicators`, `exclude_patterns`, `temporal_behavior`, `typical_role`, `typical_values`,
  `unit_from_concept`, `is_unit_dimension`. Optional fields the model omits are dropped
  (mirrors the engine's old `model_dump(exclude_none=True)`).
- **Consumer (engine):** `core/overlay._apply_concept` materializes those rows onto the
  `_adhoc` ontology (upsert-replace by `name`, last-write-wins by `created_at`); the
  `semantic_per_column` phase then grounds columns against the resolved concepts via
  `ground_columns` — **grounding only, no induction**.
- **Fail-loud (this PR):** when a cold-start `_adhoc` workspace reaches
  `semantic_per_column` with **zero** concept rows, the phase **fails loud** with a clear
  error pointing at the missing frame step, rather than silently grounding against an
  empty concept set. This turns the induction-removal run-time hole into a loud failure.

## Scope / deferred

- **Selection manifest is deferred to DAT-378.** The frozen artifact today is the concept
  rows only; the "which tables/units to import" manifest (the `select` stage output) lands
  with DAT-378 and extends this contract then.
- **Journey-layer gating is NOT built here.** Stopping the user *before* they reach
  `add_source` on an unframed workspace is DAT-378/DAT-356. This PR's fail-loud is the
  engine-side backstop, not the UX guard.

## Consequences

- The engine grounding path has a single, explicit input (concept overlay rows) and a
  defined failure mode — no hidden cold-start induction, no silent empty-ontology grounding.
- The concept payload shape is a cross-package contract: changing `OntologyConcept`
  (engine) or the frame `ProposedConcept` / `concept` teach payload (cockpit) is a
  coordinated edit on both sides.
- The induction prompt now lives only in the cockpit (`src/prompts/frame.ts`), re-homed
  from the deleted `dataraum-config/llm/prompts/ontology_induction.yaml` — not duplicated.
