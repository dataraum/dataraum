# DataRaum — workspace CLAUDE.md

This is a monorepo. Four packages, two languages, one repo.

```
packages/
├── engine/     # Python — pipeline, detectors, FastAPI REST shell. See packages/engine/CLAUDE.md.
├── api/        # OpenAPI contract — openapi.yaml regenerated from engine.
├── cockpit/    # TypeScript — TanStack Start web UI. See packages/cockpit/CLAUDE.md.
└── infra/      # docker-compose orchestration for the stack.
```

## Where to look first

- **Engine work (Python, pipeline, detectors, REST routes):** `packages/engine/CLAUDE.md` is the canonical guidance — everything about correctness-over-speed, calibration, detector standards, skills, branching, testing. Read it before touching anything under `packages/engine/`.
- **Cockpit work (UI, TanStack Start, Mantine, Drizzle):** `packages/cockpit/CLAUDE.md` — stack overview, dev loop, codegen flow.
- **Contract changes (OpenAPI):** `packages/api/openapi.yaml` is **generated** from the engine — never hand-edit it. Run `(cd packages/engine && uv run python scripts/export_openapi.py) > packages/api/openapi.yaml`, then `(cd packages/cockpit && pnpm codegen)` to refresh the TS types.
- **Infra (docker-compose, postgres):** `packages/infra/docker-compose.yml`. Build contexts point at sibling packages (`../engine`, `../cockpit`).

## Cross-package conventions

- **Spec is source of truth, code generates from spec.** Engine REST → `packages/api/openapi.yaml` → cockpit `src/api/types.ts`. Never edit `types.ts` or `openapi.yaml` by hand.
- **No backwards-compat shims.** v0.2.x MCP transport is gone; v1 is REST + chat-via-cockpit. Don't reintroduce migration code or compatibility paths.
- **Persistence is transparent.** Engine owns SQLAlchemy + DuckDB models; cockpit owns its own `cockpit_db` via Drizzle. They share the same Postgres instance but never share schemas.
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
