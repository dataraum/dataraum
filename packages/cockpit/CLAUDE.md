# DataRaum Cockpit

TanStack Start web UI for the DataRaum engine — one of three packages in the [dataraum](https://github.com/dataraum/dataraum) monorepo (with `engine` + `infra`).

<!-- intent-skills:start -->
## Skill Loading

Before substantial work:
- Skill check: run `bunx @tanstack/intent@latest list`, or use skills already listed in context.
- Skill guidance: if one local skill clearly matches the task, run `bunx @tanstack/intent@latest load <package>#<skill>` and follow the returned `SKILL.md`.
- Monorepos: when working across packages, run the skill check from the workspace root and prefer the local skill for the package being changed.
- Multiple matches: prefer the most specific local skill for the package or concern you are changing; load additional skills only when the task spans multiple packages or concerns.
<!-- intent-skills:end -->

> TanStack guidance is official **[TanStack Intent](https://tanstack.com/intent/latest)** skills, version-pinned to our installed packages (`@tanstack/ai`, `react-start`, `router-core`, …) and discovered via the CLI above — not vendored into `.claude/skills/`. Run `bunx @tanstack/intent@latest list` from this package dir.

Knowledge sources beyond Intent:
- **AG-UI (the streaming protocol under TanStack AI):** the `@tanstack/ai#ai-core/ag-ui-protocol` sub-skill covers the event layer (`RUN_*`, `TOOL_CALL_*`, `STATE_SNAPSHOT`/`STATE_DELTA`, `CUSTOM`). Load it for chat-transport, tool-state, or model-only-context work; upstream protocol reference: <https://docs.ag-ui.com>. Where a skill doc and the installed dist disagree, the installed types win.
- **React 19 has NO Intent skill** — the authority is <https://react.dev> (fetch it; never write React idioms from training-data memory). Project-distilled rules: the "UI quality bar" below, plus the cockpit-idiom conventions as the React-idiom audit lands.
- **Dependency convention:** `@tanstack/*` deps are declared `latest` and **nothing freezes** — bun.lock owns resolution; never add version pins. Contract tests + tsc guard deliberate updates.

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

`src/db/metadata/{schema,relations}.ts` is generated — never edit by hand; re-run `bun run db:pull:metadata` after the engine changes SQLAlchemy models. The command is **self-contained** (needs docker + uv + bun, ~15s): it dumps the engine models offline to `packages/engine/schema.sql`, materializes a scratch Postgres, and pulls — **no running stack, no engine boot**. CI (`schema-drift` in ci.yml) fails on any drift between the models and the two checked-in artifacts.

To grasp the engine DB schema, read `packages/engine/schema.sql` — the full DDL, always current (CI-enforced).

## Dev loop

Run the dev server **outside docker** for hot reload (the cockpit container is for prod-like smoke only):

```bash
docker compose -f packages/infra/docker-compose.yml up -d --wait postgres seaweedfs  # backend deps, from root
cp .env.example .env                 # host-dev defaults; fill ANTHROPIC_API_KEY (gitignored)
bun install && bun --bun run dev     # → http://localhost:3000  (the --bun flag is required)
bun run check                        # biome lint + format
bun run test                         # vitest
```

> **Worker code is NOT hot-reloaded.** The orchestration worker is a `globalThis`-pinned singleton bundled once at `Worker.create` (`src/worker/worker.ts`); HMR re-imports the module but reuses the running worker. So edits to **`src/worker/`** (`JourneyWorkflow`, contracts, activities) do **not** take effect under `bun --bun run dev` — **restart the dev server** to load them. (Dev face of DAT-567; the prod/container face is the deploy step under "Temporal — two workers" below.)

`/api/chat` is a TanStack Start **server route** that streams from the Anthropic SDK directly — NOT a proxy to the engine (the engine is a Temporal worker with no HTTP). The cockpit reads engine metadata straight from Postgres via Drizzle.

## Stack

TanStack **Start** (React 19) · **Router** / **Query** · **Mantine v9** + **Tailwind v4** · **Drizzle** + `postgres` · **Temporal** (`@temporalio/client` triggers the Python engine workflows; `@temporalio/worker` runs the co-located TS **orchestration** worker — DAT-529) · **Biome** · **Vitest** · strict TypeScript. The wider ecosystem (TanStack AI, xyflow, ECharts, CodeMirror, Arrow JS) lands as widgets need it — see the [Tech Stack](https://real-dataraum.atlassian.net/wiki/spaces/DD/pages/18153474) page.

## How it fits together

- **Engine contract:** the engine is a **Temporal activity worker** (no HTTP surface). Metadata reads go **direct via the Drizzle metadata client** (the `ws_<id>` schema); long-running operations run as **Temporal workflows** the cockpit starts. Chat and other BFF logic use TanStack Start **server functions** (`/api/chat`).
- **Temporal (DAT-344):** the **analysis** workflows + activities are **Python, bundled on the engine worker** — a server function calls `client.workflow.start("addSourceWorkflow", …)` via `@temporalio/client` and renders progress. **DAT-529 amends this, scoped:** the cockpit now ALSO authors + runs **orchestration / control-plane** workflows (`JourneyWorkflow`) on a **co-located TS worker** — a module-level singleton started at server boot (Nitro plugin, `src/server/plugins/orchestration-worker.ts`), polling the `cockpit-orchestration` queue. Analysis stays Python; only orchestration is TS. See the `feedback-durable-execution-lean` memory.
- **Config data** (vertical YAMLs) is bind-mounted read-only at `DATARAUM_CONFIG_PATH`; read via Node `fs` (no consumers yet).

## Skills & conventions

- **External skills** (install once, auto-activate): Temporal — `npx skills add temporalio/skill-temporal-developer`. (TanStack guidance is the official **Intent** skills — see the Skill Loading block at the top of this file, not a vendored skill.)
- **Temporal — two workers, scoped (DAT-529 amends DAT-344):** **analysis** workflows are **Python** on the engine worker; the cockpit `@temporalio/client` starts/signals/queries them (`packages/engine/src/dataraum/worker/workflows.py`). **Orchestration** workflows are **TypeScript** on the cockpit's own co-located worker (`@temporalio/worker` + `@temporalio/workflow`, `src/worker/`): `JourneyWorkflow`, started as a boot-time singleton, polling `cockpit-orchestration`. The worker pulls a native Rust **core-bridge** — so the runner is **no longer pure-alpine**: `@temporalio/*` is externalized from the bundle and the workflow code is pre-bundled at build time (`scripts/build-workflow-bundle.ts` → `workflowBundle`, the SDK's prod path — never `workflowsPath` in prod), with the worker's runtime deps shipped via a prod-deps Docker stage. The worker runs under **Bun ≥ 1.3.14** (Bun-worker support landed in SDK 1.15; one benign caveat — `promiseHooks` is unavailable, so the built-in `__stack_trace` query is disabled). Sandbox rule: `src/worker/workflows/` is deterministic, imports only `@temporalio/workflow` + the pure `contracts.ts`; all IO lives in `src/worker/activities.ts`. **Deploying a journey-code change (DAT-567):** the bundle is baked into the image, so recreate the container — `docker compose -f packages/infra/docker-compose.yml up -d --build --force-recreate cockpit`. A running per-workspace journey then adopts the new code on its **next signal** (no continue-as-new / no restart needed; `patched()` keeps the replay deterministic and already-executed stages keep their old behavior — by design). The real trap is the **cached-image / no-recreate** path: `up` without rebuild+`--force-recreate` leaves the old worker on the old bundle and the change silently never lands. Full model + the "never insert a `patched()` gate before an existing marker" rule → [ADR-0014](../../docs/adr/0014-cockpit-orchestration-worker.md#deploying-journey-code-changes-dat-567).
- Env only through `config.ts` (never `process.env`). One Mantine component per widget; Tailwind for layout. Routes that need data use `loader: queryClient.ensureQueryData(...)` so SSR dehydrates.

## UI quality bar — build for practitioner data, not demo data

A widget that "renders" is not done. The user's real query returns big, messy data, and the bar is **usable at that scale**, not "it displayed something in the 5-row demo." Before you call a data surface finished, design for these up front — they are requirements, not polish:

- **Never render an unbounded result set into the DOM.** Any list/grid/table that can hold a large result set **virtualizes** (the result grid uses `@tanstack/react-virtual`) or paginates/caps. Dumping 50k rows into the page is a bug — it freezes the tab and burns memory. "It rendered all 50k" is the failure, not the success.
- **Loading, empty, and error states are part of the widget**, not afterthoughts. A surface with no empty state or no error state is incomplete.
- **Perceived performance:** stream/skeleton long operations; don't block the UI on a slow query. Big payloads cross the wire in a bounded/columnar shape (see the run_sql streaming-grid design), not one giant JSON blob.

If you notice yourself thinking "this works" about a surface you only tried with toy data, that's the cue to push it to realistic scale — which is exactly what `/smoke` step 4 checks.

## React idiom (React 19, no Compiler — manual memoization is load-bearing)

Derived from the 2026-06-05 React-idiom audit: these rules state what the codebase already does — hold the line. React itself has no Intent skill; <https://react.dev> is the authority (fetch it, don't recall it).

1. **Derive during render; never mirror into state via effects.** Anything computable from props/state/messages is computed inline (the canvas, chip status, inventory grouping are the precedents). [react.dev/learn/you-might-not-need-an-effect]
2. **Effects are for external systems only** — DOM sync, stream subscriptions with abort/cleanup. Two exist (chat scroll-pin, NDJSON fold); a third needs the same justification in a comment. [react.dev/learn/synchronizing-with-effects]
3. **Server data goes through TanStack Query** — polling = `refetchInterval` callback returning `false` when done (measure-progress is the template). No `setInterval`, no hand-rolled `isLoading` for queries.
4. **Mutations fired by user events live in event handlers** (optionally `useMutation`), never in effects.
5. **Reset child state with a remount `key`, not a reset effect** (ResultGridWidget → StreamingGrid is the template).
6. **Memoize with a stated reason** — streaming makes the provider re-render per token, so `memo`/`useMemo` on that path is load-bearing (markdown, focus-canvas, context values); anywhere else it must earn its line. There is no React Compiler — don't assume auto-memoization, and don't blanket-memoize either.
7. **Context splits by volatility:** reactive state and stable actions are separate contexts; action-only widgets read `useCockpitActions()` and never re-render while a turn streams. New cross-cutting state joins this split — no prop-drilling, no third merged context.
8. **No refs read/written during render** (init excepted) — value-stabilize with `useMemo` over a serialized key instead. [react.dev/reference/react/useRef pitfall]
9. **No legacy APIs:** no `forwardRef` (ref is a prop in 19), `defaultProps`, class components, or `UNSAFE_` lifecycles.
10. **Extract pure logic to `.ts` modules with their own tests** (tool-chip-state, inventory-grouping); components stay render + dispatch.
11. **Tool/LLM output is `unknown` at the boundary** and narrowed explicitly — never `any`, never trusted shapes.
12. **Widgets are pure renders of engine-persisted values** — they color/format, never recompute analysis; new canvas kinds land via one `register()` in canvas-registry.ts.
13. **Shared visual vocabulary is shared code** — band/intent badges and the like live in one widget module (evidence-detail is the precedent), not per-widget copies.
14. **Chat/streaming idiom comes from the `@tanstack/ai` Intent skills** (`ai-core/chat-experience`, `ai-core/tool-calling`) — `useChat` owns optimistic append and approval flows; don't wrap it in `useActionState`/`useOptimistic`.
15. **Bound every data surface:** virtualize (result-grid) or cap with an overflow tail (inventory, evidence arrays) — never render an unbounded set into the DOM.

## Driving the UI from a session

Playwright MCP is registered per-project in `~/.claude.json` (stdio, `npx @playwright/mcp@latest`) — browser tools are available automatically when the dev server is up on `:3000`. Bring up the backend deps + `bun --bun run dev`, then point the agent at a page; edits land hot via Vite.

## Sibling packages

`../engine` (Python engine — Temporal activity worker + workflows) · `../infra` (docker-compose for postgres + Temporal + engine worker + cockpit).
