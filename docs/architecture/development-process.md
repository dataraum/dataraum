# Development process

Work is organized around **measurable objectives, judged by a harness the
worker cannot influence**.

- **An epic is an objective with a machine-checkable definition of done** —
  KPIs with measures, targets, and baselines — plus explicit scope boundaries
  and an honorable exit. It lives as one file in `epics/`, is frozen while
  worked (a wrong spec is fixed by a new definition, never edited mid-run),
  and is deleted by the PR that completes it: the directory holds live intent
  only.
- **Exactly two human gates**: approving the objective, and merging the
  result. Approval also authorizes the epic's live-model budget. Everything
  between the gates is autonomous.
- **The worker never grades its own work.** KPI results, area regressions,
  and the diff of existing tests are computed by the scorecard harness and
  recomputed by CI with the judge taken from main; the PR report is generated
  from that output, and the worker's prose is limited to approach, tradeoffs,
  and known gaps.
- **Data oracles and invariants are the acceptance signal.** Tests written by
  the worker carry no evidentiary weight. A capability epic first extends the
  oracle, and the extension must fail on main before the epic is approvable.
  A KPI no oracle can compute gets a declared human rubric — never an
  invented numeric proxy.
- **State lives in the branch.** Work proceeds in fresh sessions against
  durable on-disk state — the epic file, progress notes, the last scorecard —
  and every green checkpoint is pushed. Long-horizon work is many short
  sessions, not one long one.
- **Honest failure is a sanctioned outcome.** Evidence that a target is
  unreachable, filed instead of a PR, is a valid completion; a padded
  near-miss is not.
- **Live verification gates the release, not the PR.** Checkpoints run
  deterministic oracles; real-model verification runs at release cut, on
  freshly generated data with known truth, scored by variance-tolerant rules
  (ordering and margins, never point thresholds). An epic whose blast radius
  is model-semantic may run a budgeted live leg at promotion.
- **Knowledge homes**: how the system works now → code and tests; ideas and
  requirements → this directory; live objectives → `epics/`; non-derivable
  gotchas → agent memory. Jira and Confluence are not used. Every home holds
  present-tense truth; none is an archive.
