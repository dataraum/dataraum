# ADR-0006 — Team-lead operating model: parallel lanes, gates at the intent layer

- **Status:** Accepted
- **Date:** 2026-05-31
- **Ticket:** —
- **Design doc:** supersedes the per-lane checkpoint model implied by `/take`

## Context

The step-wise skills (`/ideate` → `/refine` → `/decompose` → `/implement` → `/smoke` →
review → `/release-prep`) read as an **assembly line**: each stage stops for human
approval, so work serialises on *the human's attention* even when the tasks are
independent. `/take` was the first move toward parallelism — it packaged a lane body
(worktree → refine → implement → smoke → PR) so N lanes could run at once — but it kept a
**per-lane** approval stop (Step 4: "after the user approves the refined approach"), which
re-serialises attention. Running M lanes still means M approval interrupts.

The correct mental model is an **org chart, not an assembly line**: a team lead exercises
judgment, then several engineers execute independently. The lead does not re-approve each
engineer's diff before it lands. Two facts shape where the lead's judgment must sit:

1. **Escalation alone is insufficient.** A confidently-wrong assumption does not escalate —
   the engineer doesn't know it's wrong. So the autonomous path cannot be trusted with
   *approach*; some human gate on approach is structurally required.
2. **Reviewing approaches is cheap; reviewing diffs is not.** A wrong assumption is visible
   in a one-paragraph approach summary (intent), not only in a 400-line diff. Gating the
   *approach* catches wrongness at the cheapest point and keeps the human off the code path.

## Decision

Adopt a **team-lead operating model** with exactly **two human judgment gates, both at the
intent layer**, plus one boundary the sandbox imposes:

| Gate | Stage | Why human | Cost |
|---|---|---|---|
| **Intake** | `/ideate` | Is this worth building (strategic, "don't build this") | low |
| **The cut** | `/decompose` | Slice into **independent, contract-locked** tasks whose approach is judgeable in a paragraph. **The load-bearing call** — a bad cut causes merge chaos. | medium |
| **The approach queue** | post-`/refine` | Catch confidently-wrong assumptions in prose, per-lane, **rolling** (not a barrier) | low, parallel |
| *(boundary)* PR-open | post-push | `gh` has no token in the sandbox → PRs open from the MacBook/CI | n/a |

Everything else is autonomous. In particular:

- **Push is autonomous and early; review runs on the pushed branch; the gate is on the
  *merge*, not the push.** This matches the user's rule ("I don't have to sign off the
  review before a git push") and how real teams work: push to a branch, machine review +
  CI run, merge is gated on green. No human sign-off sits between implement and push.
- **`refine`'s "what do you think?" stop converts** from a blocking gate into a
  **rolling approach queue**: each lane finishes refine and *parks* a structured approach
  summary (recommendation + assumptions + contract-deps + risk + size); the lead drains the
  queue on their own schedule; each approved approach releases its lane into implement. A
  lane that discovers "spec is wrong / money pit" parks with `status: escalate` instead of
  an approach — the queue carries both.
- **`implement`'s stop-early stays**, but "stop" means *park a blocker on the board / open a
  draft*, not *halt and wait*. The lane yields; the lead drains async.

### `/take` is superseded by two workflows, but its rules are inherited, not discarded

`/take` proved the lane body and encodes hard-won parallel-safety rules. Those rules move
into the workflows; the per-lane *human stop* does not. Specifically inherited:

- **Five pre-flight STOP conditions** (`/take` Step 1): parent is a real epic phase; every
  `blocked by` is Done; worktree branch matches `feat/{id}-{slug}`; no PR already open; no
  status-board claim on this task or a contract it touches; **contract locked on `main`** if
  named. These become the workflow's parallel-safety phase — *called, not reimplemented*.
- **Worktrees live INSIDE the repo** at `.worktrees/{id}/` (gitignored). Load-bearing:
  reviewer subagents inherit the orchestrator's `$CLAUDE_PROJECT_DIR` and cannot `Read` a
  sibling path — a sibling worktree makes the review gate **pass silently without reading
  the code**. Do NOT rely on the Workflow tool's `isolation:'worktree'` (placement is not
  guaranteed inside the project); use the proven manual `git worktree add .worktrees/{id}`.
- **Lane subagents must run the full CI gates locally before pushing** (they don't fire the
  end-of-turn hook): `ruff format` + `biome --write` + the CI gate set. Else CI format-check
  goes red. (`feedback_workflow_lanes_run_full_ci_gates`.)
- **The lane closes at branch-push** (not PR, not merge). Merge order across lanes is the
  user's call.

### The two workflows, split at the approach-queue gate

A single background workflow cannot pause mid-run for the rolling human gate, so the
orchestration splits at the gate; the queue lives in the conversation between the two:

1. **`team-refine`** (`.claude/workflows/team-refine.js`) — input: the approved cut (a list
   of task IDs). Fans out one refine agent per task (read-only; no worktree mutation).
   Each returns a **structured approach summary or an escalation**. Output: the queue the
   lead drains.
2. *(human, in conversation)* — lead reviews the parked approaches, approves / redirects /
   defers each. Approved approaches (with any redirection notes) become the input to:
3. **`team-build`** (`.claude/workflows/team-build.js`) — input: approved approaches. Each
   lane: pre-flight STOP checks → `git worktree add .worktrees/{id}` → implement → local CI
   gates → **in-lane review (3 agents)** → gate → **push branch** → update status board.
   Output: per-lane {branch, ci_gates, reviews, push_gate, lanes-unblocked}. A blocked review
   means the lane does NOT push. The lead then opens PRs from the MacBook and chooses
   merge order.

## Consequences

- Two human gates, both reading **intent** (the cut + the approaches), zero on the code
  path. Attention parallelises; compute parallelises; they stop fighting each other.
- **`/decompose`'s output gets richer**: each task must carry explicit assumptions +
  contract-dependencies, because that is what the approach gate scans. "Slice into tasks"
  becomes "slice into tasks whose approach can be judged in a paragraph."
- `/take` is retired as a launch path but **not deleted** — its runbook is the source of the
  inherited rules; `team-build` is the runbook executed as a fan-out. Keep the skill file as
  the canonical statement of the lane rules until `team-build` fully absorbs them.
- **Open technical questions to verify on first real run** (flagged in the scripts):
  (a) **Confirm a workflow lane agent can itself spawn the three reviewer subagents.** The
  design REQUIRES this (reviewers run in-lane, gating the push) and it mirrors how `/take`'s
  `/implement` already spawns reviewers, so it should hold — but a workflow `agent()` spawning
  further `Agent` calls is the one mechanic to smoke-test before trusting at scale. If it
  turns out a workflow subagent cannot nest `Agent`, the fallback is to make each lane a
  `/take` invocation via the Skill/Agent path rather than a bare workflow `agent()`.
  (b) confirm `git worktree add` from concurrent lane agents under the workflow concurrency
  cap doesn't race on the index — each worktree is an independent dir, but the initial
  `git fetch origin main` is pre-run once before fan-out (the Preflight phase) to avoid the
  race.
- Under **ultracode**, this is the default executor: the cut is drafted by AI + ratified by
  the lead, refine fan-out goes wide, build lanes run to clean conclusion. The two intent
  gates remain — they are correctness firewalls, not budget concessions.
