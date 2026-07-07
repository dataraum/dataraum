# Development process

How work happens in this repository.

## Work is organized as objective-driven epics

- An epic is one file, `epics/<slug>.md`: the objective in one paragraph, KPIs
  (each with a measure command, comparator target, and baseline from main),
  scope areas, out-of-scope boundaries, a promotion contract (blast-radius eval
  strategies, seed policy, live budget), and an honorable exit.
- The epic file lands on `main` via a small definition PR; approving it is the
  first of exactly **two human gates** (the second is merging the result) and
  authorizes the epic's live-eval budget.
- The file is frozen while the epic is worked — a wrong spec is fixed by a new
  definition PR, never edited mid-run — and is **deleted by the PR that
  completes the epic**. `epics/` contains live work only.
- Jira and Confluence are not used. Knowledge homes: how the system works now →
  code comments + tests; cross-cutting requirements and invariants → this
  directory; live objectives → `epics/`; non-derivable currently-true gotchas →
  agent memory. Every home holds present-tense facts; none is an archive.

## The scorecard computes every verdict

- `scorecard/run.py` (registry `scorecard/scorecard.yaml`) runs area regression
  checks and the epic's KPI measures, compares them to baselines and targets,
  and emits the report. **The agent never grades its own work**: PR bodies are
  generated from the scorecard output; agent prose is limited to approach,
  tradeoffs, and known gaps.
- Evidence weight, in order: **data oracles and invariants** (pipeline outputs
  vs ground-truth data, detector recall/precision vs injection maps,
  conservation/idempotency/coverage properties) — the acceptance signal;
  **cassette replay** (content-keyed, provider-seam) — a cost tool with no
  evidentiary weight; **agent-written tests** — inner-loop scaffolding with
  zero evidentiary weight.
- A capability epic first extends the oracle, and the extension must **fail on
  main** before the epic is approvable; the branch's job is to flip it
  fail→pass without breaking any passing oracle.
- A KPI with no computable oracle (UX quality) gets a declared human rubric —
  a scored `/smoke` session — never an invented numeric proxy.

## The verdict is tamper-resistant by construction

- CI (`.github/workflows/scorecard.yml`, required check on `epic/*` PRs)
  recomputes the verdict after restoring `scorecard/`, `epics/`, and baselines
  from `origin/main` — an epic branch cannot bend its own judge. `.github/` is
  itself a judge path.
- Every existing test the branch modifies or deletes is listed in the report's
  test-diff section. The one sanctioned judge edit is the completing PR
  deleting exactly its own epic file.
- Verdict numbers come only from the runner; nothing the agent writes enters
  the KPI table.

## The loop

Epic work runs on an `epic/<slug>` branch in fresh sessions; the branch is the
memory (`progress.md`, git history, the last scorecard output). Each session:
read the epic + progress + last scorecard, do one slice, run the fast profile
on touched areas at each checkpoint, and **push to the remote branch after
every green checkpoint**. Promotion is the full profile with `--gate`, plus one
advisory strict-review pass on the diff. The honorable exit — evidence that the
target is unreachable, filed instead of a PR — is a sanctioned outcome.

## Live verification gates the release, not the PR

| Tier | What | When |
|---|---|---|
| 1 | Deterministic oracles + invariants, frozen corpora, no LLM | every checkpoint |
| 2 | Cassette replay, frozen corpora | checkpoints; wiring + A/B economics only |
| 3 | Live lean gate: real API key, blast-radius calibration strategies + an agentic financial leg, on a **freshly generated seed corpus** | release cut (`/release-prep`) |

Tier 3 is scored with variance-tolerant rules (recall as ordering with margins,
pooled pass rates — never point thresholds) on data minted after the code was
written. An epic whose blast radius is LLM-semantic may run a lean live leg at
promotion, declared and budgeted in its epic file; live spend never exceeds the
approved budget without asking.

## Verticals are data, not machinery

The scorecard machinery names commands and **oracle ids**; datasets,
ground-truth files, and vertical vocabulary live in the oracle registry and the
corpora repos (dataraum-testdata, dataraum-eval) and `packages/dataraum-config`.
Swapping the analytical vertical changes registry entries — never the runner,
the skills, or this document.
