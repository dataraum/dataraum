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
                                         └──→ @temporalio/client (replay → addSourceWorkflow)
```

The TanStack AI SDK owns the agentic loop: `chat()` runs the model, executes
the `.server(...)` handler of each tool the model calls, pauses for user
confirmation on `needsApproval` tools, feeds results back, and iterates.
React components never reach across to the engine directly — tools are the only
layer that touches engine state.

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
  is covered by the compose smoke (`reference/.../drive-add-source.ts`).
