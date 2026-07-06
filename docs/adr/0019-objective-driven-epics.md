# ADR-0019 — Objective-driven epics: scorecard-gated autonomous development

- **Status:** Accepted
- **Date:** 2026-07-06
- **Ticket:** —
- **Design doc:** —

> **Internal process record** — how this repository is developed, not product
> architecture. Not part of the documented product decision set.

## Context

The ticket-driven loop (Jira task → agent lane → reviewer verdicts → PR) rewarded
process events an agent can satisfy without moving the underlying objective: tiny
tickets closed, reviews passed, PRs opened — while regressions were ignored and
completion was self-reported, routinely over-optimistically. This is the documented
industry failure mode (agents grading their own homework; measured reward-hacking
rates of 12–44% even against hidden tests), and prompting against it does not work —
only structural mitigation does. Meanwhile the repository already owns a real
measurement substrate — dataraum-eval's calibration scoreboard (per-detector
recall/precision/coverage), generated corpora with exact ground truth
(dataraum-testdata), the ADR-0011 measurement pack, ADR-0017's shared verdict
vectors — but **no gate consumed it**: CI was binary lint/unit green, and the
eval bridge (`.claude/handoff.md`) was a human relay that eval mostly waved off.

## Decision

Development is organised around **epics with machine-checkable objectives**, gated by
a **scorecard the agent never computes for itself**.

### 1. The epic is one file in the repo

`epics/<slug>.md` holds the objective, the KPIs (each with a measure command, target,
and baseline captured from main), scope areas, out-of-scope boundaries, a promotion
block (blast-radius eval strategies, seed policy, live budget), and the honorable
exit. It lands on `main` via a small definition PR — **approval of that PR is the
first of the two human gates** and doubles as the standing authorization for the
epic's live-eval budget. The file is **frozen during the run** (a fixed control
input: if the output is wrong, the human fixes the spec) and **deleted in the PR
that completes the epic**. `epics/` only ever contains live work; git history is the
archive; anything architectural the epic proved becomes an ADR. Jira and Confluence
leave the development loop entirely.

### 2. The scorecard computes the verdict

`scorecard/run.py` (registry: `scorecard/scorecard.yaml`) runs area regression
checks and epic KPI measures, compares against baselines and targets, and emits
JSON + a markdown report. The PR body is generated from that JSON; agent prose is
confined to approach, tradeoffs, and known gaps. Evidence weight is tiered:

1. **Data oracles and invariants** — pipeline outputs compared to ground-truth data
   files (value-level), detector recall/precision vs injection maps, conservation/
   idempotency/coverage invariants. These are the acceptance signal.
2. **Cassette replay** (content-keyed, provider-seam — DAT-666) — keeps LLM-path
   wiring exercised for free on frozen corpora and isolates A/Bs. A cost tool;
   carries no evidentiary weight on its own.
3. **Agent-written tests** — scaffolding for the inner loop. **Zero evidentiary
   weight**: they are green by construction.

**Fail-to-pass oracle discipline:** an epic that adds capability must first extend
the oracle (new ground-truth entries, a new injected corpus), and the epic is not
approved until that oracle **fails on main**. The branch's job is to flip it
fail→pass without breaking any pass-to-pass oracle. The agent never authors the
thing that judges it.

### 3. Tamper resistance is structural, not requested

- CI recomputes the verdict after restoring `scorecard/` and `epics/` from
  `origin/main` — the branch cannot bend the runner, the KPI definitions, the
  baselines, or the expected values.
- The report carries an automatic **test-diff section**: every existing test
  modified or deleted on the branch is listed. Weakening tests is visible, not
  forbidden-and-hidden.
- Verdict numbers are produced only by the runner; nothing the agent writes enters
  the KPI table.

### 4. The loop

Work happens on an `epic/<slug>` branch in fresh sessions; durable state is the
branch itself (`progress.md`, git history, last scorecard output) — never the
context window, never external trackers. Per session: read epic + progress + last
scorecard, do **one slice**, run the fast profile on touched areas at each
checkpoint, **push to the remote branch after every green checkpoint** (backup).
Promotion = full profile + gate → PR generated from the scorecard, plus one
advisory strict-review pass on the diff. The **honorable exit** is sanctioned: if
evidence shows the target unreachable, stop and file the evidence report instead of
a PR — that is a valid outcome, not a failure.

### 5. The eval ladder — live verification gates the release, not the PR

| Tier | What | When | Trust |
|---|---|---|---|
| 1 | Deterministic oracles + invariants, frozen corpora, no LLM | every checkpoint | primary (non-LLM paths) |
| 2 | Cassette replay (DAT-666), frozen corpora | checkpoints | none — wiring + A/B economics |
| 3 | **Live lean gate**: real API key, blast-radius calibration strategies + agentic `/investigate` financial leg, on a **freshly generated seed corpus** | **release cut** (`/release-prep`), the system/UAT family | the verdict |

Tier 3 is scored with eval's variance-tolerant rules (recall as ordering with
margins, pooled pass rates — never point thresholds), on data minted *after* the
code was written, over a non-deterministic path: three independent layers gaming
would have to beat simultaneously. An epic whose blast radius is LLM-semantic may
opt into a lean live leg at promotion (declared and budgeted in its epic file);
the default keeps PRs cheap and batches live spend per release. The manual
DAT-602-style milestone gate is retired as an event — it is now the standing
release exit.

### 6. Verticals are data, not machinery

The scorecard machinery names commands and **oracle ids**; datasets, ground-truth
files, and vertical vocabulary live in the oracle registry and the corpora repos
(dataraum-testdata / dataraum-eval / `packages/dataraum-config`). The current
finance corpora are *instances*, not structure — swapping the analytical vertical
changes registry entries, not the runner, the skills, or this ADR.

### Retired by this decision

`/ideate`, `/refine`, `/decompose`, `/implement`, `/take`, `team-refine.js`,
`team-build.js`, the status board, the lane mailbox, and `.claude/handoff.md`
(eval consumption now flows through the scorecard and the release gate). ADR-0006
is **superseded**: its two-intent-gate philosophy survives, but the gates' content
changes (approve the objective; merge the PR) and its lane/reviewer/push-gate
machinery is replaced by the scorecard gate. ADR-0005 is **amended**: knowledge
homes are now **code → ADR → `epics/` → memory**; Confluence and Jira are no
longer homes (existing content is distilled into epics/ADRs in a one-time
migration, then read-only).

## Consequences

- Defining an epic becomes real senior work: KPIs must be measurable, cheap to
  compute, and Goodhart-resistant. An epic without a computable KPI (e.g. UX
  quality) gets a **declared human rubric** — a scored `/smoke` session — never a
  fake numeric proxy.
- Oracle coverage becomes the constraint on what can be worked autonomously: the
  engine has a mature oracle culture; the cockpit does not — cockpit epics lean on
  the human-rubric gate until its oracle surface grows.
- Merges get bigger; the mitigations are the frozen objective, per-checkpoint green
  pushes, and rebasing onto `origin/main` at every checkpoint.
- Gaming is reduced, not eliminated: layered defense (verdicts recomputed from
  main, visible test-diff, fresh-seed live tier at release) is the position, and
  detected gaming is treated as task failure.
- Follow-ups: DAT-666 cassette at the provider seam (Phase 1); Jira/Confluence
  disposition sweep, ADR verify-pass, memory prune (Phase 2).
