# DataRaum — workspace map

Monorepo. Four packages, two languages.

```
packages/
├── engine/          # Python — pipeline, detectors, Temporal activity worker. → packages/engine/CLAUDE.md
├── cockpit/         # TypeScript — TanStack Start web UI.              → packages/cockpit/CLAUDE.md
├── dataraum-config/ # YAML data (entropy contracts, LLM prompts, verticals). No code; bind-mounted, never imported.
└── infra/           # docker-compose orchestration.                   → packages/infra/docker-compose.yml
```

Read the package's own CLAUDE.md before touching it. This file is just the map between them.

## Default to the clean cut

This codebase is mid-pivot (Python library → web-app cockpit). Retiring recently-shipped work is normal — it's the cost of doing the pivot right, not a loss. Implement the agreed design to its clean conclusion:

- **Existing code, scaffolding, and especially tests *follow* the design — they never constrain it.** When the design implies removing a field/abstraction/module, remove it everywhere, including its tests, in one cut. Don't minimize-the-touch to keep tests green; adapting or deleting a test to match the new design is expected, not a concession.
- **Don't quote prior notes as constraints — even ones written this session, including recalled memory.** They're context, not law; they reflect what was true when written. The code on disk and the agreed design win. If a note and the code disagree, verify the code and act on it.
- **Investigate to decide, then act.** Grep who actually uses something, then make the cut — don't re-confirm a decision already made or ask permission for routine cleanup.
- **Philipp owns design direction and is the senior engineer.** Build what's specified at high quality without relitigating settled calls. Reserve check-ins for genuine forks or design risk, not micro-steps.
- **No backwards-compat shims.** Clean cuts, no migration/compatibility paths.

## How the packages connect

- **Engine ↔ cockpit:** the engine is a **Temporal activity worker** (no HTTP). No OpenAPI, no codegen. The cockpit reads engine metadata directly from the `ws_<id>` Postgres schema via Drizzle (`bun run db:pull:metadata`) and drives long-running work as Temporal workflows.
- **Orchestration:** Temporal (DAT-344). Workflows **and** activities are **Python, bundled on one engine worker** (one task queue); the cockpit is a **Client** that triggers workflows by name (`@temporalio/client`) and renders progress. See `feedback-durable-execution-lean` memory. (This reverses DAT-360's "workflows in TS".)
- **Persistence:** one Postgres instance, separate schemas — engine owns `ws_<id>` (SQLAlchemy), cockpit owns `cockpit_db` (Drizzle). Never shared.
- **Config:** `packages/dataraum-config/` is **data, not code** — bind-mounted at `/opt/dataraum/config`, resolved through `dataraum.core.config` (engine) / `fs` (cockpit), never imported or path-navigated.

## Skills

Workflow skills live at the workspace root (`.claude/skills/`): `/ideate` `/refine` `/implement` `/decompose` `/smoke` `/take` `/release-prep`.

External, stack-specific skills (install once, auto-activate): `npx skills add temporalio/skill-temporal-developer`, `npx skills add DeckardGer/tanstack-agent-skills`.

## Dev environment

Dev runs in a sandboxed (SBX) container — the sandbox handles permissions, so agents run **without per-command gating**. Don't add `permissionMode: bypassPermissions`, cd-must-be-relative rules, or `permissions.allow` allowlists to dodge prompts.

## Dev loop

```bash
docker compose -f packages/infra/docker-compose.yml up -d --wait   # full stack
# engine health = Temporal worker heartbeat (no HTTP endpoint):
docker compose -f packages/infra/docker-compose.yml run --rm --no-deps \
  --entrypoint temporal temporal-admin-tools \
  worker list --namespace default --address temporal:7233          # → Status: Running
open http://localhost:3000                                          # cockpit (run dev outside docker for hot reload)
```

Design docs → Confluence (space DD). Active work → Jira (DAT-*).
