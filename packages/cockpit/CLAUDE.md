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

TanStack **Start** (React 19) · **Router** / **Query** · **Mantine v9** + **Tailwind v4** · **Drizzle** + `postgres` · **Temporal** (TS SDK) · **Biome** · **Vitest** · strict TypeScript. The wider ecosystem (TanStack AI, xyflow, ECharts, CodeMirror, Arrow JS) lands as widgets need it — see the [Tech Stack](https://real-dataraum.atlassian.net/wiki/spaces/DD/pages/18153474) page.

## How it fits together

- **Engine contract:** the engine is a Starlette kernel — `/measure`, `/query`, `/probe`, `/health`. Metadata reads go **direct via the Drizzle metadata client**; the kernel verbs handle long-running operations. Chat and other BFF logic use TanStack Start **server functions** (`/api/chat`).
- **Temporal:** the cockpit **authors the workflows and is the orchestrator**; the Python engine runs the activities. See the `feedback-durable-execution-lean` memory.
- **Config data** (vertical YAMLs) is bind-mounted read-only at `DATARAUM_CONFIG_PATH`; read via Node `fs` (no consumers yet).

## Skills & conventions

- **External skills** (install once, auto-activate): TanStack — `npx skills add DeckardGer/tanstack-agent-skills`; Temporal — `npx skills add temporalio/skill-temporal-developer`.
- **Temporal workflow code is locked-deterministic** — workflow modules import only from `@temporalio/workflow`; call activities via `proxyActivities` with `import type`-only imports; all IO lives in (Python) activities. **Bun ≥ 1.3.14** required. The rest is in the skill.
- Env only through `config.ts` (never `process.env`). One Mantine component per widget; Tailwind for layout. Routes that need data use `loader: queryClient.ensureQueryData(...)` so SSR dehydrates.

## Driving the UI from a session

Playwright MCP is registered per-project in `~/.claude.json` (stdio, `npx @playwright/mcp@latest`) — browser tools are available automatically when the dev server is up on `:3000`. Bring up engine + `bun run dev`, then point the agent at a page; edits land hot via Vite.

## Sibling packages

`../engine` (Python engine + kernel) · `../infra` (docker-compose for engine + cockpit + postgres).
