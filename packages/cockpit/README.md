# cockpit

The DataRaum cockpit — TanStack Start app that hosts the chat surface and renders the agentic UI. One of three packages in the [dataraum](https://github.com/dataraum/dataraum) monorepo (`engine`, `cockpit`, `infra`).

## Status

Read surfaces (sources, tables, snippets) land in Phase 1 of the [DAT-339 pivot](https://real-dataraum.atlassian.net/wiki/spaces/DD/pages/23363586) — wired via Drizzle direct against the engine's metadata schema. The chat surface (`/api/chat`) streams via the Anthropic SDK; tool wiring lands alongside the read surfaces in Phase 1+.

## Stack

- **TanStack Start** (React 19) — file-based routing + server functions
- **TanStack Router / Query** — type-safe routing + server state
- **Mantine v9** — component library
- **Tailwind CSS v4** — utility + layout alongside Mantine
- **Lucide React** — icons
- **Drizzle ORM** + `postgres` — two clients: `cockpit_db` (own database, holds chat history + UI state) and the engine's `ws_<workspace_id>` metadata schema (introspected via `bun run db:pull:metadata`)
- **Biome** — lint + format (no ESLint, no Prettier)
- **Vitest** — tests
- **TypeScript** only, strict

The full ecosystem pick (TanStack AI, xyflow, ECharts, CodeMirror, sql-formatter, marked, Arrow JS, etc.) lands as the corresponding widgets get built. See [Web UI: Tech Stack](https://real-dataraum.atlassian.net/wiki/spaces/DD/pages/18153474).

## Architecture

```
Browser  ──── /api/chat (SSE) ────→  TanStack Start (this app)
                                       ├── Anthropic streaming
                                       ├── Tool registry (TS fns calling kernel verbs + metadata Drizzle)
                                       └── cockpit_db (Drizzle → shared Postgres)

TanStack Start ──── metadata reads ───────→  engine's ws_<id> schema  (Drizzle, src/db/metadata/)
               ──── /measure SSE, /query Arrow, /probe SQL ───→  Starlette kernel (engine REST)
```

The engine exposes three verbs (`/measure`, `/query`, `/probe`) plus `/health` over a Starlette shell. Metadata is consumed directly via Drizzle introspection — no OpenAPI, no codegen anymore (retired in the DAT-339 pivot).

## Sibling packages

- `../engine` — Python engine + Starlette kernel shell at `src/dataraum/server/`
- `../infra` — docker-compose orchestrating engine + cockpit + postgres

## Develop

The dev server runs **outside Docker** (for hot reload) against the compose backend (Postgres + SeaweedFS on `localhost`). Bring the backend up first, then start the host dev server:

```bash
docker compose -f ../infra/docker-compose.yml up -d --wait postgres seaweedfs   # backend deps
cp .env.example .env        # host-dev defaults (localhost URLs); fill ANTHROPIC_API_KEY
bun install
bun --bun run dev           # → http://localhost:3000  (the --bun flag is required)
```

`.env.example` carries **every** var `src/config.ts` validates at boot (DB URLs, S3 creds, lake path, workspace id, …) with host-dev `localhost` defaults — only `ANTHROPIC_API_KEY` needs filling. `.env` is gitignored. A missing/empty required var fails loud at boot, naming the field.

If the engine adds or changes SQLAlchemy models, refresh the metadata client:

```bash
DATARAUM_WORKSPACE_ID=<id> METADATA_DATABASE_URL=<url> bun run db:pull:metadata
```

## Scripts

```bash
bun --bun run dev         # vite dev server (host dev; --bun flag required)
bun run build             # production build
bun run preview           # serve the production build locally
bun run test              # vitest
bun run check             # biome check (lint + format)
bun run lint              # biome lint
bun run format            # biome format
bun run db:pull:metadata  # introspect engine's ws_<id> schema → src/db/metadata/
bun run db:generate:cockpit  # cockpit_db migration SQL from src/db/cockpit/schema.ts
bun run db:push:cockpit   # push schema directly to cockpit_db
```

## Drizzle layout

Two clients in one package:

- `src/db/cockpit/{schema,client}.ts` — hand-written cockpit_db (push/generate target)
- `src/db/metadata/{schema,relations,client}.ts` — generated from the engine substrate by `bun run db:pull:metadata`; the cockpit reads, never pushes

Cockpit_db tables land as they're needed (conversations + conversation_messages are the next addition).
