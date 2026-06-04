# Cockpit tools

The agent-tier tools the cockpit chat agent calls (DAT-353). Each file is one
tool defined with the TanStack AI SDK's `toolDefinition(...)`, registered
explicitly in `registry.ts` (the array passed to `chat({ tools })` on
`../routes/api/chat.ts`).

Slice-1 toolset:

- `list-sources.ts` — read the workspace's registered sources.
- `list-tables.ts` — read the workspace's tables (optionally scoped to a source).
- `teach.ts` — record a correction/declaration as a `config_overlay` row.
- `replay.ts` — start an `addSourceWorkflow` to re-apply pending teaches.

`look_table` / `why_column` are **deferred** to DAT-367 (cockpit DuckDB).

## Architecture

```
chat.ts ──registry──→ tools/<name>.ts ──┬──→ src/db/metadata/  (Drizzle, engine ws_<id> read; teach writes config_overlay)
                                         ├──→ src/db/cockpit/   (Drizzle, chat history / ui_state)
                                         ├──→ @temporalio/client (replay → addSourceWorkflow)
                                         └──→ src/duckdb/        (cockpit-owned DuckDB read verbs)
```

The TanStack AI SDK owns the agentic loop: `chat()` runs the model, executes
the `.server(...)` handler of each tool the model calls, pauses for user
confirmation on `needsApproval` tools, feeds results back, and iterates.
React components never reach across to the engine directly — tools are the only
layer that touches engine state.

The interactive DuckDB read verbs are **cockpit-owned** (DAT-367) — there is no
HTTP round-trip to the engine for them. `run_sql` / `probe` are thin LLM-facing
wrappers in `tools/`; the connection lifecycle + query logic live in
`src/duckdb/` (neo driver `@duckdb/node-api`):

- `src/duckdb/lake.ts` — lazily-opened, process-wide DuckDB connection that
  ATTACHes the engine's DuckLake catalog **READ_ONLY**. `getLakeConnection()`
  is reusable by any read verb and by the future `connect` schema-sniff (DAT-381).
- `src/duckdb/run-sql.ts` — `runSql` over the lake (`lake.typed.*`, etc.).
- `src/duckdb/probe.ts` — `probe` against an external DB source via a throwaway
  READ_ONLY ATTACH; credentials resolved by source name in
  `src/duckdb/credentials.ts` (`DATARAUM_<NAME>_URL`, re-homed from the engine).
- `src/duckdb/query-result.ts` — shared JSON-safe `{columns, rows, rowCount}`
  result shape (neo `getRowObjectsJson()`; not Arrow IPC — see that file).

## Defining a tool

```ts
import { toolDefinition } from "@tanstack/ai";
import { z } from "zod";

export const fooTool = toolDefinition({
	name: "foo",
	description: "What the model sees.",
	inputSchema: z.object({ ... }),
	outputSchema: z.array(FooRow),
	// needsApproval: true,  // write/compute tools only — pauses for user OK
}).server((input) => foo(input));
```

- **Reads** (`list_*`) run unattended — no `needsApproval`.
- **Writes / compute** (`teach`, `replay`) set `needsApproval: true`; the SDK
  pauses and the cockpit answers via `addToolApprovalResponse` before `.server`
  runs.
- The DB-bound logic lives in a plain exported function (`listSources()`,
  `teach()`, …) so it can be tested directly; `.server(...)` just adapts it to
  the SDK.

## The N:M policy

> One tool wraps N engine operations. N tools share M backends.

- A single tool can compose multiple drizzle queries and a `run_sql` read
  over the lake before returning to the agent. The tool is the unit the LLM
  reasons about; the boundary is intent (`look_table`, `add_source`), not
  protocol (`run_drizzle_query`, `run_sql`).
- The same drizzle helper or DuckDB read verb is fair game for many tools —
  shared helpers live in `src/db/metadata/` (for Drizzle-backed reads) and
  `src/duckdb/` (for the cockpit-owned DuckDB read verbs). Don't reach across
  to another tool's internal helpers.
- **No openapi-fetch.** Pre-pivot the cockpit consumed a generated REST
  client; that surface (and the `codegen` script) retired in DAT-339 Phase 0c.
  The DuckDB read verbs (`run_sql`, `probe`) are cockpit-owned (DAT-367,
  `src/duckdb/`); metadata reads go directly via the Drizzle introspected
  schema. No HTTP to the engine for reads.

## Why an explicit registry, not autodiscovery?

Cheap to think about. `registry.ts` is a single explicit array, so refactors
are mechanical and lint-checkable. Adding a tool = one file + one import +
one array entry. Revisit if the directory grows past ~20 tools.

## Testing

- **Unit** (`bun run test`): the registry/route wiring tests mock `#/config` +
  `#/db/metadata/client` (the tools import a live `postgres()` client at module
  load). They set NO `process.env`, so nothing leaks into the integration gate.
- **Integration** (`bun run test:integration`, compose stack up): `teach`'s
  write/undo round-trip runs against a real Postgres (`*.integration.test.ts`),
  self-skipping when `METADATA_DATABASE_URL` is unset. The forward replay path
  is covered by the compose smoke (`scripts/smoke-add-source.ts`).
