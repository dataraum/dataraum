# ADR-0023 — Owner/Eval operating model: one owner agent per epic slice, eval as the gate

- **Status:** Accepted
- **Date:** 2026-07-19
- **Ticket:** —
- **Supersedes:** ADR-0006

> **Internal process record** — how this repository is developed, not product
> architecture. Not part of the documented product decision set.

## Context

ADR-0006 organized work as ticket-sized parallel lanes behind two human intent gates. It
delivered ticket throughput — and its failure mode appeared at epic scale. DAT-725, first
week: 3 of 18 planned phase issues done (one reverted), while 22 of the 25 issues actually
completed were reactive substrate-bug tickets created after the epic started; every
value-bearing phase (graph-context cutover, generated validations, cockpit read side, frame
induction) untouched. The mechanism:

- **The unit of ownership was the ticket.** Lane context dies at the PR. Nothing held the
  epic objective across lanes; the human was the only integrator, reassembling epic state
  between sessions.
- **Discovery could not be absorbed.** A lane hitting a substrate bug cannot fix it in-lane
  while holding a goal it doesn't know about, so every discovery became a new ticket — the
  epic turned into a bug tracker of its own excavation, and ticket-scoped minimal refactors
  missed adjacent work or cut foundations (the DAT-729 revert; DAT-760's write-only fields).
- **The eval was sequenced as a late phase** instead of gating the work, so the epic's
  measurable objective never steered anything.

Excavating substrate bugs is healthy — a stable substrate is built that way. What was
missing is the thing that holds the objective while the substrate churns. Requirement:
complex epics must be delivered against a measurable objective, with discovered work
absorbed rather than fragmenting the epic.

## Decision

One spine — **Spec → Owner → Eval** — replacing the two-gate lane model. (Spec-first
agentic engineering: the human architects and reviews intent, one owning agent drives,
verification is continuous.)

**Spec (human-led).** One conversation with the lead — absorbing what `/ideate` and
`/decompose` did — produces: the objective (one sentence, outcome-shaped), non-goals, a
**scorecard** (eval bands + invariants that define done), and a per-slice **e2e budget**
for real-LLM eval runs. The scorecard exists before cutting begins; if the eval cannot yet
measure the objective, oracle work is the first milestone — never a late phase. Jira keeps
the epic and checkpoint mirrors; the working plan lives with the owner.

**Owner.** One long-lived agent session per epic slice, spec → eval-green (`/own`). It
grounds first (fan-out read of the substrate the slice touches — engine AND cockpit), then
cuts a living plan, spawns implementation lanes, and **integrates them itself** on an epic
branch (`epic/dat-NNN-<slug>`). Its state lives in a ledger, `.claude/epics/dat-NNN.md`,
on the epic branch only — the final integration PR deletes it (the one sanctioned
exception to "ticket-ID filenames are never committed"); checkpoints mirror summaries to
the Jira epic. The **discovery rule** replaces satellite tickets: work that blocks the
objective is fixed now (in-lane or by the owner); work that doesn't is parked in the
ledger with a reason; extraction into tickets is a lead decision at a checkpoint. The
owner reports at checkpoints (milestone, replan, budget threshold, blocked, uncleared
regression), not per PR.

**Eval as the gate.** After every lane integration the owner runs the eval
(`dataraum-eval`; `.claude/handoff.md` updated in the same motion) and records the
scorecard in the ledger. A regression is stop-the-line — fixed before the next lane, never
a new ticket. Real-LLM runs draw down the granted budget; without one, the owner asks per
run.

Rejected alternatives: a stronger status board / more coordination atop the lane model
(still no agent holds the objective — coordination was not the missing piece, ownership
was); agent teams (same robustness trade rejected in ADR-0006, and it parallelizes
*attention*, which was never the bottleneck — integration and objective-holding were).

### Inherited from ADR-0006 (the rules survive; the machinery retires)

- **Worktrees live inside the repo** (`.claude/worktrees/...`): reviewer subagents inherit
  the session's `$CLAUDE_PROJECT_DIR` and cannot read sibling paths — outside placement
  makes the review gate pass silently. Lanes use `git worktree add`, not
  `isolation:"worktree"` (placement inside the project is not guaranteed). Worktrees are
  siblings registered with the shared repo, so creating one from inside a worktree works —
  owner-in-a-worktree spawning lane worktrees is supported topology.
- **Lanes run the full CI gates locally** before handing back (`ruff format`,
  `biome --write`, the CI set) — lane agents don't fire the end-of-turn hook.
- **Lane review gate:** `/implement`'s senior-code-reviewer + spec-compliance-reviewer;
  the ledger's lane brief is the spec the compliance reviewer reads.
- **Ask-don't-guess:** a lane at a design fork its brief doesn't settle asks the owner
  instead of guessing; the owner answers from spec/ledger context or escalates a genuine
  fork to the lead. ADR-0006's insight stands — confidently-wrong assumptions don't
  escalate — the owner's brief + diff review and the eval gate are the structural answer,
  with the lead's checkpoint as backstop.
- **Workflow `agent()` has no Agent tool** (probe `wf_acbcecdc`): implementation lanes are
  Agent-tool subagents (which can spawn reviewers); `Workflow` remains useful for
  read-only fan-outs such as the grounding pass.

### Retired

`/ideate`, `/decompose`, `/take` (skills); `team-refine.js` / `team-build.js`
(workflows); `.claude/platform-status.md` (the ledger replaces the status board).
`/refine` and `/implement` survive as lane discipline; `/smoke` and `/release-prep` are
unchanged.

## Consequences

- **Overfitting is this model's failure mode.** An owner optimizing a scorecard will
  overfit to what it measures, and the corpus is n=1 finance. Corpus widening in
  `dataraum-eval` is the control that keeps owners honest — no longer a follow-up.
  "The corpus is not the argument" applies doubly.
- Epics without an eval-measurable objective (cockpit/UX) still get an owner; their spec
  must define done differently (smoke-based) and say so explicitly.
- "Never run e2e without asking" becomes per-slice: the granted budget in the spec is the
  standing authorization; no budget → ask per run.
- Jira becomes a mirror (epic, checkpoint comments, post-hoc ticket extraction), not the
  working plan. Expect fewer, larger tickets; satellite-bug swarms become ledger entries.
- First application: DAT-725 objective (a) — graph context live, flat deleted, eval ≥
  baseline — with the epic's open satellite tickets triaged into the first owner's ledger.

## Trial result (DAT-725 slice, 2026-07-19/20 — recorded before merge)

The model held. One owner ran the slice in ~11.5 hours across 86 commits: substrate map,
eight lanes briefed/integrated with reviewers, six budgeted eval runs, three Jira
checkpoints, two stop-the-line regressions cleared, the P9 cutover (flat assembly deleted)
landed, ledger deleted at close, four residual defects extracted as tickets (DAT-823–826)
*at closeout* rather than mid-flight. The discovery rule is what changed the outcome —
substrate repairs that would have become satellite tickets under ADR-0006 were absorbed
into the slice.

Two weaknesses surfaced, both now addressed in `/own`:

1. **Ungradeable forks are the model's soft spot.** A fork the scorecard cannot decide
   (1:1 orientation on symmetric pairs) consumed roughly a quarter of the slice's wall
   clock in decide/reverse/re-decide churn and pulled the lead back in. The skill now
   requires pre-registering the decision criterion *before* the run that settles it, caps a
   fork at **one reversal** before it escalates as a spec question, and states that a fork
   no eval fixture distinguishes is by definition not the owner's to settle by measurement.
2. **The ledger outgrew its job** (1,082 lines). It is the resume artifact; at that size an
   owner resuming after compaction cannot cheaply reload it. It is now split: a
   rewritten-in-place HEAD (next action, state, open forks, parked) over append-only
   history.

Continuity, not model tier, is the load-bearing variable: the role degrades when context is
*inherited* rather than built. `/own` now forbids switching the owner model mid-slice and
requires a fresh session with re-grounding when it changes. Owner runs on Fable; lanes are
model-agnostic.
