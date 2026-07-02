# cockpit

The DataRaum cockpit — the TanStack Start web app you actually use. One of four packages in the [dataraum](https://github.com/dataraum/dataraum) monorepo (`engine`, `cockpit`, `dataraum-config`, `infra`).

## What it is

The cockpit is the product surface: typed chats (**Connect** / **Stage** / **Analyse**) with an agent canvas, plus the standing views — the operating-**Model** graph, **Governance**, **Runs**, and minted **Reports**. It hosts the chat agent (streaming via the Anthropic SDK), reads engine metadata straight from Postgres, and drives the engine's durable analysis by starting Temporal workflows. It also runs its own co-located TS **orchestration worker** for control-plane workflows (grounding loop, session cascade) — analysis itself stays in the Python engine.

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

Also in the stack: **TanStack AI** (the agent loop + tool streaming), **xyflow / React Flow** (the operating-model canvas), **Vega-Lite** (agent-authored charts, [ADR-0015](../../docs/adr/0015-charting-library-vega-lite.md)), **CodeMirror** (SQL editing), **marked** (chat markdown).

## Architecture

```
Browser ── /api/chat + /api/chat-stream (SSE) ──→ TanStack Start (this app)
                                                    ├── Anthropic streaming (TanStack AI agent loop)
                                                    ├── Tool registry (TS fns over Drizzle metadata + Temporal)
                                                    ├── cockpit_db (Drizzle → chat, reports, control plane)
                                                    └── co-located TS orchestration worker (@temporalio/worker)

TanStack Start ── metadata reads ──→ engine's ws_<id> schema (Drizzle, src/db/metadata/)
               ── workflow starts ──→ Temporal ──→ engine worker (Python, the analysis)
```

The engine has **no HTTP surface** ([ADR-0002](../../docs/adr/0002-engine-no-http-transport.md)): the seam is Postgres + Temporal, nothing else. Metadata is consumed via a generated Drizzle mirror — no OpenAPI, no codegen.

## Sibling packages

- `../engine` — Python analysis engine, a Temporal activity worker (no HTTP)
- `../dataraum-config` — YAML data (entropy config, LLM prompts, verticals); bind-mounted, never imported
- `../infra` — docker-compose orchestrating postgres + object store + Temporal + engine + cockpit

## Develop

The dev server runs **outside Docker** (for hot reload) against the compose backend (Postgres + SeaweedFS on `localhost`). Bring the backend up first, then start the host dev server:

```bash
docker compose -f ../infra/docker-compose.yml up -d --wait postgres seaweedfs   # backend deps
cp .env.example .env        # host-dev defaults (localhost URLs); fill ANTHROPIC_API_KEY
bun install
bun run db:migrate:cockpit  # apply cockpit_db migrations (control plane: workspaces/sessions/…)
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
bun run db:generate:cockpit  # generate a cockpit_db migration from src/db/cockpit/schema.ts
bun run db:migrate:cockpit   # apply committed cockpit_db migrations (drizzle/cockpit/)
bun run db:push:cockpit   # push schema directly (quick local iteration; no migration file)
```

## Drizzle layout

Two clients in one package:

- `src/db/cockpit/{schema,client,registry,runs}.ts` — hand-written cockpit_db (generate/migrate target); committed migrations in `drizzle/cockpit/`
- `src/db/metadata/{schema,relations,client}.ts` — generated from the engine substrate by `bun run db:pull:metadata`; the cockpit reads, never pushes

cockpit_db is the cockpit's own persistence: the control plane (`workspaces` / `sessions` / `session_runs` / `actors`) plus the chat transcript (`conversations` / `conversation_messages`) and reports — additive to the engine's `investigation_sessions` run anchor. Migrations apply on the compose stack via the `cockpit-migrate` one-shot service; registry/actor rows seed lazily on first resolve.
