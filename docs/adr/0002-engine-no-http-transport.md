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
Postgres schema via Drizzle** (`bun run db:pull:metadata`) and drives work as Temporal
workflows — [ADR-0008](./0008-promoted-read-views.md) §3 narrows that mirror from the raw
`ws_<id>` tables to the `ws_<id>_read` view schema. The Starlette kernel and the MCP
surface are both deleted.

## Consequences

- One integration surface engine→cockpit: Postgres (metadata) + Temporal (work). No HTTP seam to secure, version, or codegen.
- "MCP is dead" for transport purposes — do not add MCP-over-HTTP or MCP-in-TS. (Supersedes the DAT-291 HTTP-transport spike and the earlier `transport retired` phrasing scattered across notes.)
- The MCP source tree was deleted in DAT-487; recover it from git history only as a reading reference, never as something to rebuild on.
