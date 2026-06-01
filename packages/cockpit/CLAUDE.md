# DataRaum Cockpit

TanStack Start web UI for the DataRaum engine — one of three packages in the [dataraum](https://github.com/dataraum/dataraum) monorepo (with `engine` + `infra`).

## Layout

```
src/
├── db/
│   ├── cockpit/    # hand-written cockpit_db schema + client (TS-side persistence)
│   └── metadata/   # GENERATED (bun run db:pull:metadata) — read-only Drizzle into the engine's ws_<id> schema
├── routes/         # file-based TanStack Router routes
├── router.tsx      # Router + QueryClient wiring (setupRouterSsrQueryIntegration)
└── config.ts       # typed Zod env, parsed + validated once at boot — server-only
```

`src/db/metadata/{schema,relations}.ts` is generated — never edit by hand; re-run `bun run db:pull:metadata` after the engine changes SQLAlchemy models.

## Dev loop

Run the dev server **outside docker** for hot reload (the cockpit container is for prod-like smoke only):

```bash
docker compose -f packages/infra/docker-compose.yml up -d --wait control-plane postgres  # once, from root
bun install && bun run dev          # → http://localhost:3000, proxies /api/* to the engine on :8000
bun run check                       # biome lint + format
bun run test                        # vitest
```

## Stack

TanStack **Start** (React 19) · **Router** / **Query** · **Mantine v9** + **Tailwind v4** · **Drizzle** + `postgres` · **Temporal** (`@temporalio/client` — triggers Python workflows) · **Biome** · **Vitest** · strict TypeScript. The wider ecosystem (TanStack AI, xyflow, ECharts, CodeMirror, Arrow JS) lands as widgets need it — see the [Tech Stack](https://real-dataraum.atlassian.net/wiki/spaces/DD/pages/18153474) page.

## How it fits together

- **Engine contract:** the engine is a **Temporal activity worker** (no HTTP surface). Metadata reads go **direct via the Drizzle metadata client** (the `ws_<id>` schema); long-running operations run as **Temporal workflows** the cockpit starts. Chat and other BFF logic use TanStack Start **server functions** (`/api/chat`).
- **Temporal (DAT-344):** workflows **and** activities are **Python, bundled on the engine worker**. The cockpit is the **Client** — a server function calls `client.workflow.start("addSourceWorkflow", …)` via `@temporalio/client` (pure-JS, no native bridge) and renders progress; it does **not** author or run workflows. See the `feedback-durable-execution-lean` memory.
- **Config data** (vertical YAMLs) is bind-mounted read-only at `DATARAUM_CONFIG_PATH`; read via Node `fs` (no consumers yet).

## Skills & conventions

- **External skills** (install once, auto-activate): TanStack — `npx skills add DeckardGer/tanstack-agent-skills`; Temporal — `npx skills add temporalio/skill-temporal-developer`.
- **Temporal is Client-only on the cockpit side** — use `@temporalio/client` from server functions to start/signal/query workflows; the workflows themselves are **Python** (`packages/engine/src/dataraum/worker/workflows.py`). No `@temporalio/worker` or `@temporalio/workflow` in the cockpit, hence no native core-bridge (stays alpine). **Bun ≥ 1.3.14** still pinned.
- Env only through `config.ts` (never `process.env`). One Mantine component per widget; Tailwind for layout. Routes that need data use `loader: queryClient.ensureQueryData(...)` so SSR dehydrates.

## UI quality bar — build for practitioner data, not demo data

A widget that "renders" is not done. The user's real query returns big, messy data, and the bar is **usable at that scale**, not "it displayed something in the 5-row demo." Before you call a data surface finished, design for these up front — they are requirements, not polish:

- **Never render an unbounded result set into the DOM.** Any list/grid/table that can hold a large result set **virtualizes** (the result grid uses `@tanstack/react-virtual`) or paginates/caps. Dumping 50k rows into the page is a bug — it freezes the tab and burns memory. "It rendered all 50k" is the failure, not the success.
- **Loading, empty, and error states are part of the widget**, not afterthoughts. A surface with no empty state or no error state is incomplete.
- **Perceived performance:** stream/skeleton long operations; don't block the UI on a slow query. Big payloads cross the wire in a bounded/columnar shape (see the run_sql streaming-grid design), not one giant JSON blob.

If you notice yourself thinking "this works" about a surface you only tried with toy data, that's the cue to push it to realistic scale — which is exactly what `/smoke` step 4 checks.

## Driving the UI from a session

Playwright MCP is registered per-project in `~/.claude.json` (stdio, `npx @playwright/mcp@latest`) — browser tools are available automatically when the dev server is up on `:3000`. Bring up engine + `bun run dev`, then point the agent at a page; edits land hot via Vite.

## Sibling packages

`../engine` (Python engine — Temporal activity worker + workflows) · `../infra` (docker-compose for postgres + Temporal + engine worker + cockpit).
