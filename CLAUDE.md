# DataRaum — workspace CLAUDE.md

This is a monorepo. Three packages, two languages, one repo.

```
packages/
├── engine/     # Python — pipeline, detectors, Starlette kernel shell. See packages/engine/CLAUDE.md.
├── cockpit/    # TypeScript — TanStack Start web UI. See packages/cockpit/CLAUDE.md.
└── infra/      # docker-compose orchestration for the stack.
```

## Where to look first

- **Engine work (Python, pipeline, detectors, kernel verbs):** `packages/engine/CLAUDE.md` is the canonical guidance — everything about correctness-over-speed, calibration, detector standards, skills, branching, testing. Read it before touching anything under `packages/engine/`.
- **Cockpit work (UI, TanStack Start, Mantine, Drizzle):** `packages/cockpit/CLAUDE.md` — stack overview, dev loop, two-config Drizzle setup.
- **Engine ↔ cockpit contract:** the engine exposes a 3-verb Starlette kernel (`/measure` SSE, `/query` Arrow, `/probe` read-only SQL) plus `/health`. There is no OpenAPI spec and no codegen anymore (retired in the DAT-339 pivot). Cockpit data access is split: tools/widgets read the engine's metadata schema directly via Drizzle (`packages/cockpit/src/db/metadata/`); long-running operations call the kernel verbs.
- **Infra (docker-compose, postgres):** `packages/infra/docker-compose.yml`. Build contexts point at sibling packages (`../engine`, `../cockpit`).

## Cross-package conventions

- **No OpenAPI, no codegen.** Engine kernel = 3 verbs + /health. Cockpit consumes the engine's metadata via Drizzle introspection (`pnpm db:pull:metadata`), not generated REST types.
- **No backwards-compat shims.** v0.2.x MCP transport is gone; v1 is kernel verbs + chat-via-cockpit. Don't reintroduce migration code or compatibility paths.
- **Persistence is transparent.** Engine owns SQLAlchemy models in the workspace's `ws_<uuid>` schema; cockpit owns its own `cockpit_db` via Drizzle. They share the same Postgres instance but never share schemas.
- **Skills live at workspace root** (`.claude/skills/`). They drive engine work today; cockpit/infra workflows are still emerging.

## Workspace dev loop

```bash
# Bring up the full stack
docker compose -f packages/infra/docker-compose.yml up -d --wait

# Health probe
curl -fsS http://localhost:8000/health

# Cockpit
open http://localhost:3000
```

For UI iteration, run cockpit dev outside docker for hot reload — see `packages/cockpit/CLAUDE.md`.

## When in doubt

- The engine package's CLAUDE.md is the long version of how this project thinks about correctness, skills, calibration, and testing. If your task touches Python/pipeline/detectors, that file overrides anything brief said here.
- Confluence is canonical for design docs; Jira (DAT-*) tracks active work.
