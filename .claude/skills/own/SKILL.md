---
name: own
description: Own one epic slice end-to-end as the single long-lived owner agent — spec in, eval-green out. Grounding pass, epic integration branch, lane fan-out, discovery absorption, eval gate after every integration, checkpoint reports to the lead. (ADR-0022)
---

# Own: $ARGUMENTS

You are the **owner** of one epic slice. You hold the objective from spec to eval-green:
you ground, plan, spawn lanes, integrate, run the eval, absorb discoveries, and report at
checkpoints. You are the only writer of epic state. The lead (Philipp) owns the spec and
the merge to `main`; everything between is yours.

**One owner = one slice = one integration branch = one ledger.**

## Step 0 — Spec contract (refuse to start without it)

$ARGUMENTS names the epic (DAT-NNN) and optionally the slice. Before touching code, the
spec conversation with the lead must have produced — and the ledger header must record:

1. **Objective** — one sentence, outcome-shaped ("graph context live, flat deleted, eval ≥
   baseline"), not a phase list.
2. **Scorecard** — the measurable definition of done: eval bands / oracle checks + named
   invariants. If the eval cannot measure the objective yet, building that oracle is your
   FIRST milestone, before any cutting. For slices the eval can't grade (cockpit/UX), the
   spec must define done explicitly (smoke-based) instead.
3. **Non-goals** — what this slice explicitly does not do.
4. **e2e budget** — how many real-LLM eval runs (or a token cap) are pre-authorized.
   No budget granted → ask before every real-LLM run. Never assume one.

Anything missing → hold the spec conversation now, once, at the start. Do NOT
reverse-engineer a spec from Jira phase tickets — tickets are input material for your
plan (and the epic's open satellite tickets are triage material for your ledger), not
the spec.

## Step 1 — Open the slice

```bash
git fetch origin
git worktree add .claude/worktrees/epic-dat-NNN -b epic/dat-NNN-<slug> origin/main
```

Then `EnterWorktree` with the absolute path so every subsequent call resolves inside the
epic worktree.

Init the ledger at `.claude/epics/dat-NNN.md` (template below) and commit it. The ledger
exists ONLY on this branch — the final integration PR deletes it (ADR-0022; the one
exception to the ticket-ID-filename rule). It is your memory: re-read it first on every
wake-up and after every context compaction.

Worktree mechanics: worktrees are siblings registered with the shared repo — creating one
from inside a worktree works. Keep lane worktrees INSIDE your project dir
(`.claude/worktrees/...` relative to your session root): reviewer subagents inherit your
`$CLAUDE_PROJECT_DIR` and cannot read paths outside it — a lane placed elsewhere makes the
review gate pass silently without reading the code. For lanes, use `git worktree add`,
not `isolation:"worktree"` (placement inside the project is not guaranteed).

## Step 2 — Ground before cutting

Fan out read-only agents (Explore agents, or a read-only Workflow) over everything the
slice touches: models/tables, pipeline phases, consumers — **engine AND cockpit** ("no
writers" needs a cockpit grep too) — config, tests, and the eval's oracle surface. Write
the **substrate map** into the ledger: what exists, who consumes it, where the seams are,
which open satellite tickets fall inside this slice.

Then cut the plan: lanes that are independently mergeable INTO THE EPIC BRANCH, each with
an approach judgeable in a paragraph. Record it in the ledger. The plan is living — you
resequence and re-cut it yourself; replanning within the objective needs no permission.
Replanning that changes the objective's shape is a checkpoint.

## Step 3 — Drive loop

Repeat until the scorecard is green:

1. **Brief the lane** in the ledger: goal, the surface it may touch, approach, what it
   must NOT touch (other lanes' surface, contracts).
2. **Spawn the lane agent** (Agent tool). The lane: opens its worktree
   (`git worktree add .claude/worktrees/<lane-id> -b feat/dat-NNN-<lane-slug>
   epic/dat-NNN-<slug>`) → `/refine` discipline against its brief → `/implement`
   discipline (its two reviewers at the end; the ledger brief is the spec the
   spec-compliance reviewer reads) → runs the FULL CI gates locally (`ruff format`,
   `biome --write`, the CI gate set — lanes don't fire the end-of-turn hook) → commits.
   Lanes never push and never touch `main`.
3. **Ask-don't-guess:** a lane that hits a design fork its brief doesn't settle ends its
   turn with the question instead of guessing. Answer from spec/ledger context
   (SendMessage to resume it), or escalate a genuine fork to the lead. Never let a lane
   barrel through an ambiguity.
4. **You integrate.** Review the lane diff yourself, merge the lane branch into the epic
   branch, resolve conflicts, remove the lane worktree.
5. **Eval gate.** Run the eval after EVERY integration (`dataraum-eval`, sibling repo —
   see its Makefile; update `.claude/handoff.md` in the same motion as any
   detector/pipeline/response-shape change). Record the scorecard row in the ledger.
   **Regression = stop-the-line:** fix it before the next lane. A regression is never a
   new ticket. Real-LLM runs draw down the budget; log every run.
6. **Rebase** the epic branch onto `origin/main` regularly (main moves under you); re-run
   the eval after any rebase that pulled in substrate changes.

**Discovery rule** — the heart of the model. When a lane or the eval surfaces unplanned
work (substrate bug, missing foundation, dead surface):

- It **blocks the objective** → fix it now, in-lane or as your own commit on the epic
  branch. It is your work, not a ticket.
- It **doesn't block** → park it in the ledger (one line: what · where · why deferred).
  NO satellite Jira tickets mid-flight; extraction into tickets is the lead's call at a
  checkpoint.

## Checkpoints

Report to the lead at: a milestone (a scorecard band newly green), a replan that changes
the objective's shape, budget half-spent or exhausted, blocked for more than a session, or
a regression you cannot clear. NOT per lane, NOT per PR.

A checkpoint = scorecard delta + plan delta + parked list + budget spent. Mirror a summary
comment to the Jira epic. Between checkpoints, pace autonomous work with scheduled
wakeups; the ledger is the resume point.

## Stop conditions

- A locked call in the spec proves infeasible → **epic-level replan trigger**: stop, write
  the findings to the ledger, escalate to the lead. Never silently defer around it.
- A lane fails three attempts → salvage its branch state into the ledger, stop the lane,
  re-cut the work differently or escalate.
- Scorecard green → Step 4.

## Step 4 — Close

1. Final rebase onto `origin/main`; full CI gates; eval green one last time.
2. Delete the ledger in the closing commit; anything still parked moves into the closeout
   report for the lead to triage into tickets.
3. Open the integration PR `epic/dat-NNN-<slug>` → `main`. Body: objective, scorecard
   result vs bands, lanes merged, parked list. The lead reviews and merges — never merge
   to `main` yourself.
4. Closeout comment on the Jira epic. The epic's phase tickets are resolved by the lead
   against reality — you do not auto-close them.

## Ledger template

```markdown
# DAT-NNN <slice> — owner ledger
Objective: …
Scorecard: … (bands + invariants; source: dataraum-eval …)
Non-goals: …
e2e budget: N runs granted YYYY-MM-DD | none (ask per run) · spent: …

## Substrate map      (Step 2 output: what exists, consumers, seams, in-scope satellites)
## Plan               (living: lanes, sequence, state)
## Lane briefs        (per lane: goal · surface · approach · do-not-touch)
## Parked             (one line each: what · where · why deferred)
## Eval history       (date · trigger · scorecard row · verdict)
## Decisions          (fork → call → why)
## Checkpoint log     (date · what was reported)
```

## Rules

- One owner per slice; the ledger has exactly one writer: you.
- The scorecard exists before the first lane opens — oracle-first is not optional.
- **Corpus discipline:** the finance corpus is ONE instance for verification — never tune
  to its numbers or bake its names into code. Overfitting the scorecard is this model's
  failure mode; raise any suspicion of it at a checkpoint.
- Lanes always get `/refine` + `/implement` discipline and the reviewers — no express lanes.
- Real-LLM runs only within the granted budget; log every run in the ledger.
- Green plumbing ≠ semantic correctness: the scorecard, not test-green, is "done".
