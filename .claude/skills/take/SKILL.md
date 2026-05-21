---
name: take
description: Take a single DAT-294 task end-to-end as one parallel lane — worktree, refine, implement, lane-smoke, PR. Invoke once per task. Launch N concurrently via the Agent tool with isolation:worktree.
allowed-tools:
  - Read
  - Write
  - Edit
  - Bash
  - Glob
  - Grep
  - Agent
  - AskUserQuestion
  - Skill
  - EnterWorktree
  - ExitWorktree
  - mcp__jira__getJiraIssue
  - mcp__jira__editJiraIssue
  - mcp__jira__getConfluencePage
---

# Take: $ARGUMENTS

You are taking a single DAT-294 task end-to-end as one parallel lane. **One task = one worktree = one branch = one PR = one focused session.**

This skill exists so the user can launch N lanes in parallel via the Agent tool with `isolation: "worktree"` — each agent inherits this skill and runs independently. Without a skill there is no parallelism unit; with it, multiple lanes ship from one orchestrator message.

The full enforcement rules live in `CLAUDE.md → "Parallel platform work runbook"`. This skill executes that runbook. If you find this skill and the runbook diverging, the runbook wins — open a PR to align them.

**When to use `/take` today:** reserved for genuinely parallel platform lanes. The v1 plan (post-spine cockpit + engine REST) ships PR-per-step on a shared branch — single stream, no contract-lock gate, just use `/refine` + `/implement` directly. Reach for `/take` when a future wave decomposes into 2+ independently-mergeable tasks that touch separate code areas.

## Input

$ARGUMENTS is a Jira task identifier under DAT-294 (a direct child or a child of a sub-epic). Refuse to start if the ticket is not under DAT-294.

## Step 1: Pre-check (lane can open)

Before touching any code:

1. **Fetch the task ticket.** Confirm: parent is a DAT-294 phase, status is To Do or In Progress, every `is blocked by` dependency is Done. Surface the blocker list and STOP if any is not Done.
2. **Check for existing worktree** at `.worktrees/{task-id}/`. If branch matches `feat/{task-id}-{slug}` → resume it. If branch is something else → STOP and ask. **Note:** worktrees live INSIDE the main repo at `.worktrees/{task-id}/` (gitignored). This is load-bearing for the review gate — subagents spawned by `/implement` inherit the orchestrator's `$CLAUDE_PROJECT_DIR` and can only `Read` paths underneath it, so a sibling worktree at `../dataraum-context.worktrees/...` is invisible to them and the review gate fails silently.
3. **Check for existing PR** via `gh pr list --search "{task-id} in:title"`. If open → the lane is already in flight. STOP and ask.
4. **Check the status board** `.claude/platform-status.md`. STOP if another active lane claims this task or a contract this task touches.
5. **Verify the contract is locked, if the task names one.** If the task ticket references a contract artifact (path + sha — e.g., a row on the Platform Contracts page, an `openapi.yaml` sha, a proto file sha), the artifact must be merged on `main`. **If named but not locked, STOP** — open a contract-lock issue instead. Do not start implementation against an unlocked contract; that is the single biggest cause of parallel-merge chaos.

   Note: most of the original Platform Contracts inventory (Mcp-Session-Id, CP↔executor gRPC, TanStack AI wire format, OpenAPI for `/api/*`) was superseded or deferred by the v1 plan. Contract #5 (config storage shape) stays locked for future multi-user work. If the task names no contract, this step is a no-op.

If anything is wrong, STOP and report. Don't "make it work."

## Step 2: Open the worktree and switch the session into it

```bash
git fetch origin main
git worktree add .worktrees/{task-id} -b feat/{task-id}-{slug} origin/main
```

Then call `EnterWorktree` with the absolute path:

```
EnterWorktree(path="$CLAUDE_PROJECT_DIR/.worktrees/{task-id}")
```

This switches the session's working directory AND `$CLAUDE_PROJECT_DIR` to the worktree. Every subsequent `Bash` / `Read` / `Edit` / spawned `Agent` call resolves paths relative to the worktree — no `cd` prefix on every command, no per-command permission prompt for paths outside the orchestrator's view.

Worktrees live INSIDE the main repo at `.worktrees/{task-id}/` (gitignored), not as a sibling. Two reasons:

- **Subagent file access.** Subagents spawned by `/implement` (senior-code-reviewer, spec-compliance-reviewer) inherit the orchestrator session's `$CLAUDE_PROJECT_DIR`, not the EnterWorktree-shifted one. A sibling at `../dataraum-context.worktrees/...` falls outside that root and the harness blocks `Read` against it — the review gate fails silently. Inside-project placement keeps the lane visible.
- **End-of-turn hook scope.** The hook (`.claude/hooks/end-of-turn-check.sh`) `cd`s into `packages/engine/` before running ruff/mypy/pytest. `.worktrees/` is a sibling of `packages/`, so the hook never recurses into it — no cross-lane pollution. (Historical note: the prior skill version placed worktrees SIBLING to the main repo to dodge a hook that recursed from the project root; the current hook doesn't, so the dodge is no longer needed and now actively breaks the review gate.)

Make sure `.worktrees/` is in `.gitignore`. If not, add it before the worktree opens.

Do NOT use `cd .worktrees/...` as a substitute for `EnterWorktree` — `Bash` resets cwd between commands and the prefix has to be re-applied every call, which triggers a permission prompt each time.

All subsequent steps run inside the worktree.

## Step 3: Run /refine

Invoke `/refine {task-id}`. Reality-check the spec against the codebase. If `/refine` finds the **contract** is wrong (not the implementation), STOP the lane — re-locking the contract is higher-priority than this task.

## Step 4: Run /implement

After the user approves the refined approach: invoke `/implement {task-id}`.

The DO NOT change scope **must include**:

- Every contract file (consumed, never edited from a lane)
- Any directory owned by another phase
- Any cross-cutting infrastructure not owned by this task

`/implement` runs the senior-code-reviewer and spec-compliance-reviewer at the end. Both must approve before this skill proceeds.

## Step 5: Lane smoke

Run smoke scoped to **this task's contract surface only** — backend task: its API/RPC against a minimal stand (Postgres + this process); frontend task: its UI against a contract-mock (Vite dev + mock). Lives at `tests/platform/smoke_{task-id}.py` (or equivalent path the task defines).

Three smoke tiers; only the first is this lane's responsibility:

| Tier | Scope | When | Owner |
|---|---|---|---|
| **Lane smoke** | This task's contract surface | Every commit on the lane branch | This skill |
| **Integration smoke** | Full spine end-to-end on docker-compose | After each PR merges to `main` | CI on `main` |
| **Journey smoke** | Full Playwright user journey | At first-wave milestones (post-P5, post-P9, post-P11) | `/release-prep` |

If integration smoke on `main` is red because another lane is mid-flight, that is **expected** per DAT-294 ("`main` is allowed to be temporarily user-broken between phases"). Don't block this lane on it.

## Step 6: Open the PR

```bash
gh pr create --title "{task-id}: {title}" --body "$(cat <<'EOF'
## Task

{task-id} — {ticket link}

## Contract consumed

{contract path} @ {sha}

## Acceptance criteria

{copied from ticket, checked off}

## Lane smoke

\`\`\`
{command + result summary}
\`\`\`

## Other lanes in flight

{output of: gh pr list --search "DAT-294 in:body"}

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

The "Other lanes in flight" block is non-negotiable — reviewers need the parallel context to judge merge order.

## Step 7: Update the status board

Edit `.claude/platform-status.md` (create if missing) with one row per active lane:

```markdown
| Task | Worktree | Branch | PR | Contract | Status |
|---|---|---|---|---|---|
| {task-id} | .worktrees/{task-id} | feat/{task-id}-{slug} | #NN | {contract}@{sha} | smoke green, awaiting review |
```

Remove the row when the PR merges. This board is the at-a-glance "all lanes in flight" view for the user.

## Step 8: Exit the worktree and hand back

```
ExitWorktree(action="keep")
```

Returns the session to the orchestrator's main-repo cwd so the next lane / status-board update / `gh pr list` poll operates from the right place. `keep` preserves the worktree on disk (PR is still open against it).

Report concisely:

- Lane closed: PR #NN opened, lane smoke green
- Contract consumed: file + version
- Lanes unblocked when this merges: list of task tickets that become ready
- Lanes potentially conflicting at merge: list (cross-reference `platform-status.md`)

**Do NOT merge the PR yourself.** Merge order across parallel lanes is the user's call.

## Parallel launch (orchestrator pattern)

To run multiple lanes concurrently from one orchestrator session: launch each as an Agent tool call with `isolation: "worktree"`, in a single message with multiple tool blocks. Each agent inherits this skill, opens its own worktree, works independently, and closes when its PR is open.

The orchestrator session does NOT touch code — it only:
- Decomposes the phase into tasks (via `/decompose`)
- Locks contracts (via dedicated contract-lock PRs, one per contract)
- Spawns parallel `/take` agents
- Watches `gh pr list` and `.claude/platform-status.md`
- Unblocks downstream lanes as PRs merge
- Triggers journey smoke at milestones

## When NOT to use /take

- Non-platform work (single-stream library work): use `/refine` + `/implement` directly
- Spikes (DAT-295/296/297/298): throwaway exploration — no worktree isolation needed
- Contract-lock PRs: separate flow — small, focused, reviewer-heavy, not phased
- S-size bug fixes inside an already-merged task: normal feature branch on `main`

## Rules

- One task per worktree, one worktree per task
- Contract locked BEFORE the lane opens — no exceptions
- Lane smoke is mandatory; integration smoke is informational
- Do not edit contracts from inside a lane
- Do not reach into other lanes' code
- Three-strikes rule applies — stop and report, don't power through
- The lane closes when the PR opens, not when it merges
- Update `.claude/platform-status.md` so the user sees all lanes at once
