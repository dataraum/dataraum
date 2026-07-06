# Epic: <title>

<Objective in one paragraph: the outcome for a user of the platform, not a list of
changes. A reader must be able to tell from this paragraph alone whether the epic
succeeded.>

## Out of scope

<Hard boundaries. Files/areas this epic must not touch; adjacent problems it
deliberately leaves open.>

## Oracle extension (fail-to-pass)

<For capability epics: which oracle was extended (ground-truth entries, injected
corpus, invariant) and confirmation that it FAILS on main. An epic without a
failing oracle on main is not approvable. Pure-fix epics may reference existing
failing oracles instead.>

## Honorable exit

<What evidence would prove the target unreachable. Filing that evidence instead of
a PR is a sanctioned outcome.>

```yaml scorecard
slug: <slug>                 # must match the filename and the epic/<slug> branch
areas: [engine]              # scorecard areas in blast radius (see scorecard/scorecard.yaml)
kpis:
  - id: <kpi_id>
    statement: <one sentence, human-readable>
    target: ">= 0.9"         # comparator + number: >=, <=, >, <, ==
    baseline: null           # measured on main at definition time; null = capture pending
    tier: promotion          # checkpoint | promotion (release-tier KPIs live in the release gate)
    measure: <command>       # prints a bare number or {"value": N} as its last stdout line
promotion:
  strategies: []             # dataraum-eval strategies in blast radius (release gate scope)
  seed_policy: fresh         # fresh = mint a new testdata seed for live runs
  live_budget: none          # e.g. "one lean gate (~15% calibrate-all)"; approving this file authorizes it
  live_at_promotion: false   # true only when the blast radius is LLM-semantic
rubric: none                 # for KPIs no oracle can compute: "human /smoke session, scored: <criteria>"
```
