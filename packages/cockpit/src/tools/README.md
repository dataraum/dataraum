# Cockpit tools

Hand-written TypeScript tools that the chat agent calls. Each file in this
directory is one tool: a plain TypeScript function that conforms to the
Anthropic `Tool` schema (`name`, `description`, `input_schema`, plus a
handler), and gets registered explicitly in the tool registry imported by
`../routes/api/chat.ts`.

Empty for slice 1 — the first batch of read-surface tools
(`list_sources`, `list_tables`, `look_table`, `search_snippets`) lands in
Phase 1 of the DAT-339 pivot (DAT-353).

## Architecture

```
chat.ts  ──registry──→  tools/<name>.ts  ──┬──→  src/db/metadata/  (Drizzle, engine substrate read)
                                            ├──→  src/db/cockpit/   (Drizzle, chat history / ui_state)
                                            └──→  fetch("/measure" | "/query" | "/probe")  (engine kernel)
```

Tools are the **only** layer that touches engine state. The chat handler
streams Anthropic responses and runs tools when the model emits `tool_use`;
React components never reach across to the engine directly.

## The N:M policy

> One tool wraps N engine operations. N tools share M backends.

- A single tool can compose multiple drizzle queries, a kernel `/query`
  call, and a `/measure` SSE follow-up before returning to the agent. The
  tool is the unit the LLM reasons about; the boundary is intent
  (`look_table`, `add_source`), not protocol (`run_drizzle_query`,
  `call_kernel`).
- The same drizzle helper or kernel verb is fair game for many tools —
  shared helpers live in `src/db/metadata/` (for Drizzle-backed reads)
  and a future `src/kernel/` (for kernel-verb fetch wrappers when slice 2
  needs them). Don't reach across to another tool's internal helpers.
- **No openapi-fetch.** Pre-pivot the cockpit consumed a generated REST
  client; that surface (and `pnpm codegen`) retired in DAT-339 Phase 0c.
  Tools call the kernel verbs (`/measure`, `/query`, `/probe`) via a
  hand-written `fetch` wrapper, and read metadata directly via the
  Drizzle introspected schema.

## File layout convention

```
tools/
├── README.md         ← this file
├── registry.ts       ← landing in Phase 1: re-exports every tool + handler map
├── list_sources.ts   ← Phase 1
├── list_tables.ts    ← Phase 1
├── look_table.ts     ← Phase 1
├── search_snippets.ts ← Phase 1
└── add_source.ts     ← Phase 2 (uses the mounted dataraum_lake volume)
```

One tool per file. Each file exports:

```ts
import type { Tool } from '@anthropic-ai/sdk/resources/messages'

export const definition: Tool = { name, description, input_schema }
export async function handler(input: ParsedInput): Promise<ToolResult> { ... }
```

`registry.ts` assembles the `Tool[]` array + a `Record<string, handler>` for
the chat dispatch loop. Adding a tool = adding a file + one import line.

## Why not autodiscover?

Cheap to think about. The chat handler reads a single explicit registry
file, so refactors are mechanical and lint-checkable. We can revisit if
the directory grows past ~20 tools.
