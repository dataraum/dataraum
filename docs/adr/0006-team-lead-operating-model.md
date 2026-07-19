# ADR-0006 — Team-lead operating model: parallel lanes, gates at the intent layer

- **Status:** Superseded by ADR-0022
- **Date:** 2026-05-31
- **Ticket:** —
- **Design doc:** supersedes the per-lane checkpoint model implied by `/take`

> **Internal process record** — how this repository is developed, not product
> architecture. Not part of the documented product decision set.

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
- **A lane that hits an ambiguous design fork ASKS rather than guesses or dies** (see the
  mid-run ask section below). Distinct from stop-early: the lane keeps its context and resumes
  with the lead's answer, instead of throwing the work away.

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
   lane is a **pipeline**: pre-flight STOP checks → `git worktree add .worktrees/{id}` →
   implement → local CI gates + lane smoke → **commit (no push)** → **runtime-spawned
   review (3 reviewers)** → **deterministic JS gate** → **push branch** → update status board.
   Output: per-lane {branch, ci_gates, reviews, asks, push_gate, lanes-unblocked}. A blocked
   review means the lane does NOT push. The lead then opens PRs from the MacBook and chooses
   merge order. **Launch requirement:** because lanes may ASK mid-run (see below), the
   session that launches team-build must drain the mailbox while it runs — start a `Monitor`
   on `.claude/.mailbox` before/just after launching, answer each `*.q`, write the `*.a`.

### Reviewers run as a runtime-spawned pipeline stage, NOT nested in the lane (revised 2026-05-31)

The original design had each lane agent spawn its three reviewers in-lane (mirroring how
`/implement` spawns reviewers inside `/take`). **A probe (`wf_acbcecdc`, 2026-05-31) proved
this is structurally impossible.** A workflow `agent()`'s entire toolset is `Bash, Edit,
Read, Skill, ToolSearch, Write, StructuredOutput` — there is **no Agent tool**, and an
`agentType` override does **not** add one. A lane agent therefore cannot spawn reviewer
subagents; the in-lane review was a silent no-op. (This was the load-bearing correction's
unverified assumption — open question (a) below — and it failed.)

The fix keeps reviewers per-lane and concurrent **without nesting**: the workflow *runtime*
spawns the three reviewers (`spec-compliance-reviewer`, `senior-code-reviewer`,
`strict-reviewer`, each via `agentType`) as their own **pipeline stage**. Because
`pipeline()` has no barrier between stages, lane X's review fires the instant X's implement
finishes — concurrent with lane Y still building. So review stays per-lane (no
one-big-review bottleneck) while respecting the recursion limit. Two consequences:

- The lane **commits** on its branch (so reviewers can `git diff origin/main...HEAD`) but
  does **not** push. Push happens only in the Gate stage.
- **The push gate becomes a deterministic JS decision** on the structured verdicts
  (`reviews.every(r => r.verdict === 'pass')`), not a prompt instruction the lane can skip.
  This is *stronger* than the original prompt-gated push (the PR #161 format-check miss) and
  stronger than agent-teams' `TaskCompleted` hook — the gate is in code, un-bypassable.

**Why not Claude Code's built-in agent teams here?** Evaluated 2026-05-31. Agent teams would
make reviewers *peer teammates* (sidestepping the no-Agent-tool limit natively) and adds live
human steering + per-teammate plan approval. But it is **experimental** (no resume,
early-shutdown, status lag), **foreground**, **non-deterministic**, **one-team-at-a-time**,
and offers **no enforced worktree isolation** — it trades away exactly the robustness
(deterministic gate, background, resume, isolation) that the build lane needs. The
relocation above fixes the bug *inside* the robust workflow, so the build executor stays a
workflow. Agent teams remains the better fit for the **interactive refine / steer** phase
(its plan-approval loop is the native form of the approach queue) and may be piloted there.

### Lanes ask mid-run instead of guessing (added 2026-05-31)

The remaining weakness of a background workflow is the one the intent gates *cannot* cover:
a lane that hits an **unanticipated design fork** *inside* implementation. Escalate-and-die
throws away the lane's accumulated context; guessing sends hours of work down the wrong branch
that may take the user just as long to unwind. Both lose "the intended complexity."

A probe (`wf_c18f4719`, 2026-05-31) proved a running workflow `agent()` **can pause
mid-execution, ask, block, and resume with full context** — it even acted on an *extra*
instruction the answer carried (something a yes/no escalate would have dropped). So lanes get a
**mid-run ask primitive** (`ASK_PROTOCOL` in `team-build.js`):

- On an ambiguous fork the approved approach doesn't settle, the lane writes its question to a
  **file mailbox** (`<repo>/.claude/.mailbox/{laneId}.q`, gitignored), **blocks** polling for
  `{laneId}.a` (up to ~30 min), then **resumes** with the answer (which overrides its default
  and may carry extra instruction). Timeout → stop-early with `blocker: "unanswered ask: …"` —
  it still **never guesses**.
- **The orchestrator (the session that launched the workflow) is the human-proxy.** While
  team-build runs in the background, the launcher watches the mailbox (a `Monitor` on
  `.claude/.mailbox` emitting on new `*.q` files), and for each question either **answers from
  project context** (CLAUDE.md / memory / ADRs) or **`PushNotification`s the user** for a
  genuine fork, then writes `{laneId}.a`. This is the org-chart model in full: the lead fields
  the engineers' questions and shields the user from the routine ones, escalating only the real
  decisions.

This is **why we don't need agent teams to get interactivity.** Agent teams makes *the user*
field every teammate question in the foreground; the mailbox makes *the orchestrator* field
them and bother the user selectively — while keeping the workflow's determinism, background
execution, resume, and worktree isolation. Constraints: a blocked lane holds a concurrency slot
while it waits (fine at our lane counts); ask quality depends on the lane choosing to ask
rather than barrelling ahead (same discipline stop-early already needs); and team-build stays
**one-run-at-a-time** (it mutates worktrees + the status board), so per-lane mailbox filenames
don't collide. Reserve *ask* for "I hit a fork and won't guess"; keep *escalate/stop-early* for
"I am fundamentally blocked".

## Consequences

- Two human gates, both reading **intent** (the cut + the approaches), zero on the code
  path. Attention parallelises; compute parallelises; they stop fighting each other.
- **`/decompose`'s output gets richer**: each task must carry explicit assumptions +
  contract-dependencies, because that is what the approach gate scans. "Slice into tasks"
  becomes "slice into tasks whose approach can be judged in a paragraph."
- `/take` is retired as a launch path but **not deleted** — its runbook is the source of the
  inherited rules; `team-build` is the runbook executed as a fan-out. Keep the skill file as
  the canonical statement of the lane rules until `team-build` fully absorbs them.
- **Open technical questions:**
  (a) ~~Confirm a workflow lane agent can itself spawn the three reviewer subagents.~~
  **RESOLVED 2026-05-31 — NO (probe `wf_acbcecdc`).** Workflow agents have no Agent tool;
  reviewers were relocated to a runtime-spawned pipeline stage (see "Reviewers run as a
  runtime-spawned pipeline stage" above). The fallback considered here (each lane a `/take`
  invocation via Skill/Agent) was *not* taken — `/take` would itself try to nest reviewers
  and hit the same wall; the pipeline-stage relocation is the actual fix.
  (b) **Still to verify on first real run:** `git worktree add` from concurrent lane agents
  under the workflow concurrency cap may race on the index — each worktree is an independent
  dir, but `git worktree add` briefly locks the main repo. Mitigations in place: the initial
  `git fetch origin main` is pre-run once before fan-out (Preflight), and the implement stage
  retries `git worktree add` up to 3× on a lock. If contention persists, serialize the
  worktree-creation step.
- Under **ultracode**, this is the default executor: the cut is drafted by AI + ratified by
  the lead, refine fan-out goes wide, build lanes run to clean conclusion. The two intent
  gates remain — they are correctness firewalls, not budget concessions.
