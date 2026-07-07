# DataRaum — workspace map

Monorepo. Four packages, two languages.

```
packages/
├── engine/          # Python — pipeline, detectors, Temporal worker. → packages/engine/CLAUDE.md
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

## Information has one home — by lifecycle

The architecture is fixed; the work now is fixing bugs and inconsistencies. Don't let information accrete in parallel journals. Each fact has exactly one home, chosen by how long it lives:

- **How it works *now*** (invariants, behaviour, the why behind a line) → **code: precise comments + tests.** A finished task's knowledge lives in the diff it produced. This is the default — reach for it before any doc.
- **Why the architecture/approach is this way** → **ADRs (`docs/adr/`) + Confluence (space DD).** Long-lived, superseded only by a newer ADR.
- **In-flight work, plans, design exploration, refinement notes, status** → **the Jira epic/task** (description or an attached `.md`). Closed issue = delete the artifact; the code is the truth, do *not* migrate a closed issue's docs anywhere.
- **Non-derivable, currently-true, cross-cutting facts with no other home** → agent memory (gotchas, seams, preferences). Not a status board.

**The rule that enforces it: a ticket ID in a filename (`dat-NNN-*.md`) means the file is ephemeral → it belongs in Jira, never committed to the repo and never in memory.** `docs/adr/` is the one ticket-free zone. When a task lands, delete its plan/handoff/status file — don't archive it. When a Jira issue closes, delete its `project_` memory and prune its MEMORY.md line.

## How the packages connect

- **Engine ↔ cockpit:** the engine is a **Temporal worker** (no HTTP). No OpenAPI, no codegen. The cockpit reads engine metadata directly from the `ws_<id>` Postgres schema via Drizzle (`bun run db:pull:metadata`) and drives long-running work as Temporal workflows.
- **Orchestration:** Temporal (DAT-344). ALL workflows — analysis and orchestration — are **Python on the engine worker** (ADR-0020 / DAT-708); the cockpit is a **Client** that triggers workflows by name (`@temporalio/client`) and renders progress, plus a co-located **activity-only** worker (queue `cockpit-orchestration`) hosting the cockpit_db run writers + the grounding-teach agent the orchestration workflows call back into. See `feedback-durable-execution-lean` memory. (This reverses DAT-360's "workflows in TS"; DAT-529/609's TS orchestration workflows were retired by DAT-708.)
- **Persistence:** one Postgres instance, separate schemas — engine owns `ws_<id>` (SQLAlchemy), cockpit owns `cockpit_db` (Drizzle). Never shared.
- **Config:** `packages/dataraum-config/` is **data, not code** — bind-mounted at `/opt/dataraum/config`, resolved through `dataraum.core.config` (engine) / `fs` (cockpit), never imported or path-navigated.

## Skills

Workflow skills live at the workspace root (`.claude/skills/`): `/ideate` `/refine` `/implement` `/decompose` `/smoke` `/take` `/release-prep`.

External, stack-specific skills:
- **Temporal** — `npx skills add temporalio/skill-temporal-developer` (install once, auto-activates).
- **TanStack** — official **[TanStack Intent](https://tanstack.com/intent/latest)** skills, version-pinned to the cockpit's installed packages and discovered via CLI (`bunx @tanstack/intent@latest list` / `load <pkg>#<skill>` from `packages/cockpit`). Wired through the `intent-skills` block in `packages/cockpit/CLAUDE.md` — not vendored into `.claude/skills/`. (Replaced the `DeckardGer/tanstack-agent-skills` workaround, which lacked `@tanstack/ai`.)

## Dev environment

Dev runs in a sandboxed (SBX) container — the sandbox handles permissions, so agents run **without per-command gating**. Don't add `permissionMode: bypassPermissions`, cd-must-be-relative rules, or `permissions.allow` allowlists to dodge prompts.

**Git hygiene for concurrent agents (this repo runs several at once):**
- **ALWAYS WORK IN A WORKTREE.** Never edit on a shared checkout's branch — another agent can switch or commit under you, and your work ends up stranded on someone else's branch (or their commits land in your PR). Spawn with `isolation: "worktree"`, or `git worktree add .claude/worktrees/<task> -b <branch>`; commit there.
- **ALWAYS REBASE/MERGE onto `origin/main` BEFORE PUSHING.** Fetch + rebase first — main moves under long runs. Pushing a stale-base branch forces a non-fast-forward fixup later (or clobbers a teammate's force-push). Rebase clean, re-verify, then push.

## Dev loop

```bash
docker compose -f packages/infra/docker-compose.yml up -d --wait   # full stack
# engine health = Temporal worker heartbeat (no HTTP endpoint):
docker compose -f packages/infra/docker-compose.yml run --rm --no-deps \
  --entrypoint temporal temporal-admin-tools \
  worker list --namespace default --address temporal:7233          # → Status: Running
open http://localhost:3000                                          # cockpit (run dev outside docker for hot reload)
```

Settled architecture decisions → `docs/adr/` (short, git-tracked). Long-form design docs → Confluence (space DD). Active work → Jira (DAT-*).
