# cockpit

The DataRaum cockpit — TanStack Start app that hosts the chat surface and renders the agentic UI. One of four packages in the [dataraum](https://github.com/dataraum/dataraum) monorepo (`engine`, `api`, `cockpit`, `infra`).

## Status

Sources list lands (v1 plan step 4); `/api/chat` lands in step 6. See [Cockpit + Engine REST: v1 plan](https://real-dataraum.atlassian.net/wiki/spaces/DD/pages/22872066).

## Stack

- **TanStack Start** (React 19) — file-based routing + server functions
- **TanStack Router / Query** — type-safe routing + server state
- **Mantine v9** — component library
- **Tailwind CSS v4** — utility + layout alongside Mantine
- **Lucide React** — icons
- **Drizzle ORM** + `postgres` — `cockpit_db` (own database in the shared Postgres instance; holds chat history + UI state)
- **Biome** — lint + format (no ESLint, no Prettier)
- **Vitest** — tests
- **TypeScript** only, strict

The full ecosystem pick (TanStack AI, xyflow, ECharts, CodeMirror, sql-formatter, marked, Arrow JS, etc.) lands as the corresponding widgets get built. See [Web UI: Tech Stack](https://real-dataraum.atlassian.net/wiki/spaces/DD/pages/18153474).

## Architecture

```
Browser  ──── direct fetch / EventSource ────→  Python FastAPI (engine REST)
   │
   └─── /api/chat (SSE, AG-UI)  ────→  TanStack Start (this app)
                                          ├── Anthropic streaming
                                          ├── Tool registry (TS fns wrapping engine REST)
                                          ├── AG-UI emitter (TanStack AI native)
                                          └── cockpit_db (Drizzle → shared Postgres)
```

The browser direct-fetches the Python engine REST for everything that isn't chat (sources, sessions, snippets, pipeline SSE). Only `/api/chat` goes through TanStack Start's server functions.

## Sibling packages

- `../engine` — Python engine + FastAPI shell at `src/dataraum/api/`
- `../api` — OpenAPI contract (`openapi.yaml`, regenerated from engine)
- `../infra` — docker-compose orchestrating engine + cockpit + postgres

## Develop

```bash
cp .env.example .env
# fill in COCKPIT_DATABASE_URL pointing at cockpit_db
pnpm install
pnpm codegen
pnpm dev
```

Dev server runs on http://localhost:3000.

## Scripts

```bash
pnpm dev       # vite dev server
pnpm build     # production build
pnpm preview   # serve the production build locally
pnpm test      # vitest
pnpm check     # biome check (lint + format)
pnpm lint      # biome lint
pnpm format    # biome format
```

## Drizzle (cockpit_db)

```bash
# Generate migrations from schema changes
pnpm exec drizzle-kit generate

# Push migrations to cockpit_db
pnpm exec drizzle-kit push
```

`src/db/schema.ts` is the source of truth. Tables land here as they're needed — conversations + conversation_messages land in step 6.
