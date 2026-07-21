---
name: smoke
description: Quick UX smoke test — drive the cockpit you just built and see how it feels as a practitioner
allowed-tools:
  - mcp__playwright__browser_navigate
  - mcp__playwright__browser_click
  - mcp__playwright__browser_type
  - mcp__playwright__browser_fill_form
  - mcp__playwright__browser_press_key
  - mcp__playwright__browser_snapshot
  - mcp__playwright__browser_take_screenshot
  - mcp__playwright__browser_wait_for
  - mcp__playwright__browser_console_messages
  - mcp__playwright__browser_network_requests
  - Read
  - Bash
  - AskUserQuestion
---

# Smoke: $ARGUMENTS

You just changed a cockpit surface, an engine workflow/activity, or both. Now USE it. Not to
verify correctness (that's eval's job) — to feel what the UX is like.

The engine has **no HTTP surface** (ADR-0002): it is a Temporal worker. So a smoke is always
*the cockpit in a browser*, optionally with a Temporal workflow running behind it. There is
nothing to `curl` on the engine.

## Input

$ARGUMENTS is one of:
- A section to focus on (e.g., `/library`, `/operating-model`, `/cockpit`)
- A scenario to play through (e.g., "open a chat, ask for revenue by month, check the chart")
- Empty — exercise whatever just changed

## What this is

A quick, informal test drive. Like kicking the tires after a change. You're not checking ground
truth or running calibration. You're checking:

- Does the surface respond at all?
- Does the output make sense to a human?
- Is the layout/copy useful or confusing?
- Are there obvious gaps (missing fields, unhelpful messages, errors, empty states with no guidance)?
- Would you, as a practitioner, know what to do next based on what's on screen?

## Where to run it — one rule

**Host dev server by default; the composed container when the change is one the dev server
cannot show you.**

- **Host dev (`bun --bun run dev` from `packages/cockpit`, → `http://localhost:3000`)** is the
  fast loop for pages, components, routes, copy, layout. Hot reload, no image rebuild. This is
  what `packages/cockpit/CLAUDE.md` ("Dev loop") and `.env.example` describe as the normal way
  to run the cockpit. DuckDB is fine here: the native binding is only a problem for the
  *bundler* — `vite.config.ts` externalizes `@duckdb/node-bindings-*` (plus `@temporalio/*`,
  `@swc/*`, `@opentelemetry/*`) inside the **`nitro()` build** config, and `src/lib/sql-canonical.ts`
  imports `@duckdb/node-api` lazily. Dev SSR doesn't bundle those, so it boots.
- **Container** (`docker compose … up -d --build cockpit`) is required when the thing you need
  to see only exists in the production shape:
  - changes under **`src/worker/`** — the activity-only worker is a `globalThis`-pinned
    singleton created once at server boot; HMR re-imports the module but reuses the running
    worker, so worker edits do not take effect under `dev` (restart at minimum, container for
    a true check),
  - **bundle-shape** problems — anything touching `vite.config.ts`, the Nitro plugins, or an
    externalized dependency; those failures exist only in `vite build`/`.output`,
  - **SSR-runtime crashes** (window-dependent module code) and anything you want prod-parity on,
  - the **portal + Caddy ingress**: subdomain routing, workspace switching, auth-cookie
    derivation, `/create`. Bare host dev has no Caddy and no subdomains.
- Middle ground for a prod-accurate check without docker: `bun run build && bun run start`.

Two setup notes that bite:
- After `down -v`, host dev needs `bun run db:migrate:cockpit` (in the stack that's the
  `cockpit-migrate` service).
- `docker compose up` **reuses an existing image**. After a code change, `--build` the service
  you changed, or you are smoking stale code.

If you changed **engine Python**, rebuild its container before the smoke — the service is
`engine-worker` (there is no `control-plane`):

```bash
docker compose -f packages/infra/docker-compose.yml up -d --build engine-worker
```

If the engine added or changed SQLAlchemy models, refresh the cockpit's Drizzle metadata client
(`bun run db:pull:metadata`) against a fresh DB before smoking.

## How to do it

### 1. Bring up what you need

```bash
# Full stack. `env -u ANTHROPIC_API_KEY` so a stale shell key doesn't shadow the
# .env one (compose var precedence has bitten us).
env -u ANTHROPIC_API_KEY docker compose -f packages/infra/docker-compose.yml up -d --wait

# Engine health = the Temporal worker heartbeat (no HTTP endpoint):
docker compose -f packages/infra/docker-compose.yml run --rm --no-deps \
  --entrypoint temporal temporal-admin-tools \
  worker list --namespace default --address temporal:7233          # → Status: Running
```

For host-dev UI work you only need the backend deps plus the migration:

```bash
docker compose -f packages/infra/docker-compose.yml up -d --wait postgres seaweedfs
cd packages/cockpit && bun install && bun run db:migrate:cockpit && bun --bun run dev
```

(One shell invocation — a Bash call's cwd does not persist to the next. `dev` blocks, so run it
in the background and poll `http://localhost:3000/api/health`.)

**Smoking a branch's container:** the compose `cockpit` / `engine-worker` `build.context`
(`../cockpit`, `../engine`) resolves relative to the compose *file*, i.e. the main checkout — a
plain rebuild ships `main`, not your worktree. Use a one-line override with an absolute context
path, or build the image directly from your checkout:

```bash
docker build -t infra-cockpit <checkout>/packages/cockpit
env -u ANTHROPIC_API_KEY docker compose -f packages/infra/docker-compose.yml \
  up -d --no-build --force-recreate cockpit
```

A chat smoke makes a **real LLM call** (the agent needs a valid `ANTHROPIC_API_KEY`) — ask
before running one unprompted.

### 2. Get in — every surface is auth-gated

There is no anonymous page. Signed-out HTML bounces to the portal; `/api/*` and server fns 401.

| Running | Entry URL | Sign in with |
|---|---|---|
| Composed stack (Caddy) | `http://dataraum.localhost` → portal → pick the workspace (`http://ws1.dataraum.localhost`) | `dev@dataraum.dev` / `dataraum-dev` |
| Host dev | `http://localhost:3000` (workspace mode; `/` redirects to `/cockpit`) | same |

`:80` must be free for the Caddy default; otherwise set `CADDY_HTTP_PORT` **and** a matching
`DATARAUM_PORTAL_ORIGIN` (e.g. `http://dataraum.localhost:8000`), and use that port everywhere.
Chromium and Firefox resolve `*.localhost` natively; curl does not (`--resolve`). The compose
`cockpit` container also publishes host `:3000` as a **debug port** — signed-out HTML there
still bounces to the portal.

### 3. Drive the cockpit via Playwright MCP

The workspace is the host; routes are flat under it. The real sections (source:
`packages/cockpit/src/ui/sections.ts`) are:

| Route | Rail label | What it is |
|---|---|---|
| `/cockpit` | Cockpit | the chat surface — index mints a conversation, `/cockpit/$conversationId` is the turn view |
| `/reports` | Reports | minted reports; `/reports/$reportId` opens one |
| `/library` | Sources | the data-sources browser (path is historical — it was `/sources`) |
| `/workflows` | Runs | cockpit_db-backed run monitor (links out to the Temporal UI) |
| `/metadata` | Metadata | the engine's `ws_<id>` metadata read views |
| `/operating-model` | Model | the concept-spine / metric canvas |
| `/governance` | Governance | |
| `/settings` | Settings | |

Portal-only routes: `/` (login + workspace list) and `/create` (create-workspace flow).

```
browser_navigate to http://ws1.dataraum.localhost/cockpit   (or http://localhost:3000/cockpit)
browser_snapshot          # accessibility tree of the page
browser_take_screenshot   # keep one per surface — a UI change needs a judged screenshot
```

For a chat turn:
```
browser_navigate to <workspace>/cockpit
browser_type into the composer: "<your test intent>"      # or click a type chip
browser_click Send  →  lands on /cockpit/<conversationId>
browser_wait_for text in the assistant turn (or a tool-result canvas)
browser_network_requests  # /api/chat + /api/chat-stream returned; tool routes not 4xx/5xx
browser_console_messages  # client-side errors
```

For each interaction note:
- **Response**: did it work? What rendered?
- **Clarity**: would a practitioner understand this without reading source?
- **Usefulness**: does this help decide what to do next?
- **Surprises**: anything unexpected, missing, or confusing? Errors in the console? Failed
  network requests?

### 4. Try a mini workflow

String 2-3 interactions together as a practitioner would — the point is the *flow* between
surfaces, not one page. For example:

- `/library` → see what sources exist → `/cockpit` → ask about one of them → confirm the
  answer's canvas matches what `/metadata` says about that table.
- Trigger something long-running from chat → `/workflows` → does the run show up, progress, and
  end in a state you can act on?

### 5. Try to break it (gently)

- Click a button before its data has loaded — does the empty state make sense?
- Ask something the workspace can't answer — does the agent fail gracefully?
- Reload mid-stream — does the page recover?
- Open the network panel: is anything 404-ing? Any unhandled 500s?
- **Push it to realistic scale.** Run something that returns a *large* result set — thousands to
  tens of thousands of rows, not the 5-row demo. Does the surface **virtualize / paginate / cap**,
  or does it try to render every row into the DOM? Watch for a frozen tab, multi-second render,
  runaway memory, or a scrollbar implying tens of thousands of live DOM nodes. **Dumping an
  unbounded result set into the page is a bug, not "fine"** — a practitioner's real query returns
  big data, and "it rendered" ≠ "it's usable." If you catch yourself thinking *"50k rows
  displayed, works,"* that *is* the finding to report, not a pass.

### 6. Share impressions

Tell the user what you found. Not a formal report — just honest impressions:

- "`/library` renders, but the empty state still says the sources list 'is rewired in Phase 1' — stale copy"
- "Chat streams text fine, but tool-call chips show the raw JSON result — needs a friendlier shape"
- "`/api/chat-stream` 500s when the engine worker hasn't finished bootstrapping the ws schema"
- "Console has a React hydration mismatch on the run timestamps"

Be specific. Quote actual text on screen. Reference actual route paths and network requests.
Attach the screenshots. This is feedback, not a verdict.

## Next step

After smoke testing:
1. Fix any obvious issues found (UI copy rot, missing error states, broken empty states).
   Rebuild the container if you touched engine Python or cockpit worker code.
2. Commit the implementation.

If smoke testing reveals deeper problems (not just UX polish but fundamental issues like wrong
widget shape, contract mismatch, missing route): go back to `/implement` or even `/refine`.
Don't patch over structural problems.

## Rules

- This is NOT acceptance testing — don't assert against ground truth
- This is NOT a unit test — don't test internal behavior
- This IS a UX check — would a human find this useful?
- If the cockpit errors: capture the console message + a screenshot, then move on
- If something isn't responding, check `docker compose -f packages/infra/docker-compose.yml ps`
  and then the logs of the actual service — `engine-worker`, `cockpit`, `portal`, `caddy`,
  `temporal` (there is no `control-plane` service). Don't fight it for more than 2 minutes.
- Spend 5-10 minutes, not 30. Quick impressions are the point.
- Be honest. "This feels clunky" is useful feedback. "Looks great!" is not.
