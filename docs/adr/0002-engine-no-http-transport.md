# ADR-0002 — Engine is a pure Temporal activity worker (no HTTP / MCP transport)

- **Status:** Accepted
- **Date:** 2026-05-25
- **Ticket:** DAT-339
- **Design doc:** Confluence DD space

## Context

The engine has worn several transport skins: an MCP server (stdio, then a spike at
streamable-HTTP + bearer auth), and a short-lived 3-verb Starlette HTTP kernel (DAT-339
pivot). Maintaining a request/response transport on the engine duplicated what Temporal
already provides for long-running work, and kept reference code alive that no longer
reflects how the cockpit talks to the engine.

## Decision

The engine is a **pure Temporal activity worker — no HTTP server, no MCP transport**. There
is no OpenAPI and no client codegen. The cockpit reads engine metadata **directly from the
`ws_<id>` Postgres schema via Drizzle** (`bun run db:pull:metadata`) and drives work as
Temporal workflows. The Starlette kernel is deleted; `src/dataraum/mcp/` is **reference-only**
(kept for reading, not wired into any runtime path).

## Consequences

- One integration surface engine→cockpit: Postgres (metadata) + Temporal (work). No HTTP seam to secure, version, or codegen.
- "MCP is dead" for transport purposes — do not add MCP-over-HTTP or MCP-in-TS. (Supersedes the DAT-291 HTTP-transport spike and the earlier `transport retired` phrasing scattered across notes.)
- `src/dataraum/mcp/` is a deletion candidate once nothing references its patterns; treat it as docs, not code.
