---
name: smoke
description: Quick UX smoke test — drive the cockpit (or a REST route) you just built and see how it feels as a practitioner
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

You just implemented or changed a cockpit surface, an engine kernel verb, or both. Now USE it. Not to verify correctness (that's eval's job) — to feel what the UX is like.

**IMPORTANT:**
- If you changed **engine Python code**, the engine container loaded the old code — rebuild + restart it before smoke: `docker compose -f packages/infra/docker-compose.yml up -d --build control-plane`.
- If you only changed **cockpit code** and `pnpm dev` is running, Vite hot-reloads — no restart needed.
- If the **engine added or changed SQLAlchemy models**, refresh the cockpit's Drizzle metadata client: `(cd packages/cockpit && DATARAUM_WORKSPACE_ID=<id> METADATA_DATABASE_URL=<url> pnpm db:pull:metadata)`.

## Input

$ARGUMENTS is one of:
- A route or page to focus on (e.g., `/sources`, `/chat`)
- A scenario to play through (e.g., "ask the agent to list sources, check the streaming feels right")
- A kernel verb to exercise headless (e.g., `POST /measure`)
- Empty — exercise whatever just changed

## What this is

A quick, informal test drive. Like kicking the tires after a change. You're not checking ground truth or running calibration. You're checking:

- Does the surface respond at all?
- Does the output make sense to a human?
- Is the layout/copy useful or confusing?
- Are there obvious gaps (missing fields, unhelpful messages, errors, empty states with no guidance)?
- Would you, as a practitioner, know what to do next based on what's on screen?

## How to do it

### 1. Bring up the stack

```bash
docker compose -f packages/infra/docker-compose.yml up -d --wait
curl -fsS http://localhost:8000/health
```

For UI iteration: also run `pnpm dev` in `packages/cockpit/` (hot-reload). For pure REST smoke: skip the cockpit, hit the engine directly.

### 2. Drive the cockpit via Playwright MCP

```
browser_navigate to http://localhost:3000/<route>
browser_snapshot      # accessibility tree of the page
browser_take_screenshot save: docs of what landed (optional)
```

For the chat surface:
```
browser_navigate to http://localhost:3000/chat
browser_type into the textarea: "<your test intent>"
browser_click the Send button
browser_wait_for text that should appear in the assistant turn (or a tool-result chip)
browser_network_requests  # confirm /api/chat returned SSE, tool calls hit the right routes
browser_console_messages  # check for client-side errors
```

For each interaction note:
- **Response**: did it work? What rendered?
- **Clarity**: would a practitioner understand this without reading source?
- **Usefulness**: does this help decide what to do next?
- **Surprises**: anything unexpected, missing, or confusing? Errors in the console? Failed network requests?

### 3. Try a mini workflow

String 2-3 interactions together as a practitioner would. For the current cockpit slice:
- Navigate to `/sources` → check sources render → switch to `/chat` → ask "list my sources" → confirm the agent calls `list_sources` and renders the result

This tests the *flow* between page + chat + tools, not just one surface.

### 4. Try to break it (gently)

- Click a button before its data has loaded — does the empty state make sense?
- Type a question the engine can't answer — does the agent fail gracefully?
- Reload mid-stream — does the page recover?
- Open the network panel: is anything 404-ing? Any unhandled 500s?

### 5. Share impressions

Tell the user what you found. Not a formal report — just honest impressions:

- "The sources table renders, but the empty state says 'lands in a later step' which is stale copy"
- "Chat streams text fine, but tool-call chips show the raw JSON result — needs a friendlier shape"
- "Network panel shows `/api/sources` returning a 503 when the engine container hasn't finished warmup"
- "Console has a React strict-mode warning about double-stream state updates"

Be specific. Quote actual text on screen. Reference actual route paths and network requests. This is feedback, not a verdict.

## Next step

After smoke testing:
1. Fix any obvious issues found (UI copy rot, missing error states, broken empty states). Rebuild engine container if you touched Python.
2. Commit the implementation.
3. For engine surface or detector changes: update `.claude/handoff.md` so eval picks up what changed.
4. For cockpit-only or REST-only work: no handoff needed — eval doesn't consume the cockpit.

If smoke testing reveals deeper problems (not just UX polish but fundamental issues like wrong widget shape, contract mismatch, missing route): go back to `/implement` or even `/refine`. Don't patch over structural problems.

## Rules

- This is NOT acceptance testing — don't assert against ground truth
- This is NOT a unit test — don't test internal behavior
- This IS a UX check — would a human find this useful?
- If the cockpit errors: capture the console message + a screenshot, then move on
- If the engine isn't responding: check `docker compose ps` + `docker compose logs control-plane`; don't fight it for more than 2 minutes
- Spend 5-10 minutes, not 30. Quick impressions are the point.
- Be honest. "This feels clunky" is useful feedback. "Looks great!" is not.
