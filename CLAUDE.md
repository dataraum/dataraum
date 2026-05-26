# DataRaum — workspace map

Monorepo. Four packages, two languages.

```
packages/
├── engine/          # Python — pipeline, detectors, Starlette kernel.  → packages/engine/CLAUDE.md
├── cockpit/         # TypeScript — TanStack Start web UI.              → packages/cockpit/CLAUDE.md
├── dataraum-config/ # YAML data (entropy contracts, LLM prompts, verticals). No code; bind-mounted, never imported.
└── infra/           # docker-compose orchestration.                   → packages/infra/docker-compose.yml
```

Read the package's own CLAUDE.md before touching it. This file is just the map between them.

## How the packages connect

- **Engine ↔ cockpit:** the engine is a 3-verb Starlette kernel (`/measure`, `/query`, `/probe`) + `/health`. No OpenAPI, no codegen. The cockpit reads engine metadata directly from the `ws_<id>` Postgres schema via Drizzle (`bun run db:pull:metadata`) and calls the kernel verbs for long-running work.
- **Orchestration:** Temporal. The cockpit (TS) authors workflows and orchestrates; the engine (Python) runs activities. See `feedback-durable-execution-lean` memory.
- **Persistence:** one Postgres instance, separate schemas — engine owns `ws_<id>` (SQLAlchemy), cockpit owns `cockpit_db` (Drizzle). Never shared.
- **Config:** `packages/dataraum-config/` is **data, not code** — bind-mounted at `/opt/dataraum/config`, resolved through `dataraum.core.config` (engine) / `fs` (cockpit), never imported or path-navigated.
- **No backwards-compat shims.** Clean cuts, no migration/compatibility paths.

## Skills

Workflow skills live at the workspace root (`.claude/skills/`): `/ideate` `/refine` `/implement` `/decompose` `/smoke` `/take` `/release-prep`.

External, stack-specific skills (install once, auto-activate): `npx skills add temporalio/skill-temporal-developer`, `npx skills add DeckardGer/tanstack-agent-skills`.

## Dev environment

Dev runs in a sandboxed (SBX) container — the sandbox handles permissions, so agents run **without per-command gating**. Don't add `permissionMode: bypassPermissions`, cd-must-be-relative rules, or `permissions.allow` allowlists to dodge prompts.

## Dev loop

```bash
docker compose -f packages/infra/docker-compose.yml up -d --wait   # full stack
curl -fsS http://localhost:8000/health
open http://localhost:3000                                          # cockpit (run dev outside docker for hot reload)
```

Design docs → Confluence (space DD). Active work → Jira (DAT-*).
