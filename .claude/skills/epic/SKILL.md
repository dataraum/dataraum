---
name: epic
description: Objective-driven epic (docs/architecture/development-process.md) — define an epic with machine-checkable KPIs, or run the scorecard-gated loop on one until the harness says the objective is met
---

# Epic: $ARGUMENTS

`$ARGUMENTS` is `define <idea>` or `run <slug>`. The model is docs/architecture/development-process.md: the agent
never grades its own homework — `scorecard/run.py` computes every verdict, CI
recomputes it with the judge restored from `origin/main`, and the PR body is
generated from the scorecard, not written as prose.

## define <idea>

Produce `epics/<slug>.md` (copy `epics/TEMPLATE.md`) and get it approved onto main.
This is the human gate — the KPI definition is the load-bearing senior work.

1. **Objective**: one paragraph, outcome-for-a-user, checkable by a stranger.
2. **KPIs**: each with a `measure` command (prints a number or `{"value": N}` as
   its last stdout line), a comparator `target`, and a `baseline` measured on
   main. A KPI must be cheap enough to run at promotion. No computable KPI
   possible (UX quality)? Declare a **human rubric** (`rubric:` — a scored
   `/smoke` session) — never invent a numeric proxy.
3. **Oracle extension (fail-to-pass)**: a capability epic first extends the
   oracle (ground-truth entries, injected corpus, invariant — registered in
   `scorecard/scorecard.yaml` oracles) and you must show it **fails on main**.
   The branch's job is fail→pass without breaking pass-to-pass oracles.
4. **Promotion block**: blast-radius eval strategies, seed policy, live budget.
   Approval of this file **is** the authorization for that live spend.
5. **Honorable exit**: what evidence would prove the target unreachable.
6. Capture baselines on main: `uv run scorecard/run.py --epic <slug> --profile
   full --areas <deterministic areas only>` and paste the values. **The epic is
   not approved yet, so nothing live is authorized**: exclude live areas (e.g.
   `eval`) and live measure commands unless the user explicitly okays that spend
   now. Then open the **definition PR** containing only the epic file +
   oracle-extension data. Philipp's approval merges it; only then may an
   `epic/<slug>` branch gate, and only then does the live budget exist.

## run <slug>

Preconditions: `epics/<slug>.md` is on `origin/main`; work on branch
`epic/<slug>` in a worktree.

**Session protocol** (safe under fresh context — the branch is the memory):

1. Read `epics/<slug>.md`, `progress.md` (branch root), the last
   `scorecard/out/scorecard.md`, and `git log --oneline -20`. Pick **one slice**.
2. Inner loop: implement with targeted tests (`pytest --testmon`, vitest). Tests
   you write are scaffolding — they carry **zero evidentiary weight**.
3. **Checkpoint**: `uv run scorecard/run.py --profile fast --epic <slug>`.
   Green → commit, rebase onto `origin/main`, **push** (the remote branch is the
   backup), append 3–5 lines to `progress.md` (done / next / open questions).
   Red → fix before new work.
4. **Promotion** (KPIs believed met): `uv run scorecard/run.py --profile full
   --epic <slug> --gate`. Gate green →
   - generate the PR body: `... --pr-body` (KPI table, area regressions,
     test-diff). Your own prose is limited to **approach, tradeoffs, known
     gaps** — no success claims, no numbers;
   - run one advisory `strict-reviewer` pass on the diff (input to the merge
     decision, not a gate);
   - the PR **also deletes `epics/<slug>.md`** — completing the epic removes it
     from `epics/` (git history is the archive).

**Rules**

- **Never touch the judge**: `scorecard/`, `epics/`, and baselines are
  out-of-scope on an epic branch — CI restores them from main, and a local touch
  fails the gate. Wrong spec? Stop and tell Philipp; the fix is a new definition
  PR, not an edit mid-run.
- Modifying or deleting **existing** tests is visible in the report's test-diff
  section — do it only when the design requires it, and say why in the PR prose.
- Live-LLM runs only within the epic's approved `live_budget`
  (`live_at_promotion: true`); otherwise the live tier belongs to the release
  gate (`/release-prep`). Never beyond budget without asking.
- **Honorable exit**: if evidence shows a KPI unreachable, stop and file the
  evidence (what was tried, what the scorecard shows, why the target can't hold)
  instead of a PR. That is a sanctioned outcome; a padded near-miss is not.
