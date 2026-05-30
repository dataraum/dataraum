# ADR-0004 — Agent-tier boundary: agentic LLM in the cockpit, durable pipeline in the engine

- **Status:** Accepted
- **Date:** 2026-05-29
- **Ticket:** DAT-353 (agentic loop), DAT-367 (cockpit DuckDB)
- **Design doc:** Confluence DD space

## Context

The product needs a conversational, streaming agent (connect / frame / select) *and* a
durable, reproducible analysis pipeline. Putting streaming LLM loops inside Temporal
activities fights durable execution (non-determinism, long-held activities); putting the
durable pipeline in the agent tier loses reproducibility.

## Decision

Draw the tier boundary: **agentic / streaming LLM lives in the cockpit (the TS agent
tier)**; the **engine is durable pipeline + grounding only**. Concept *induction* relocates
to TS (the engine's `induce_adhoc_concepts` retires); agent prompts are TS-owned. The agent
tier is built on the **TanStack AI SDK** and the DuckDB **neo driver** (`@duckdb/node-api`,
not the deprecated `duckdb`).

## Consequences

- Engine activities stay deterministic and replayable; the chatty/uncertain LLM work lives where streaming and tool-calling are natural.
- Induction moving to TS is a clean cut, not a shim — the engine loses that responsibility entirely.
- DuckDB neo driver constraints apply in the cockpit: no `arrowIPCStream` (use materialized row JSON); READ_ONLY ATTACH sees only the last CHECKPOINTed snapshot; native bindings must be externalized from the Nitro build.
