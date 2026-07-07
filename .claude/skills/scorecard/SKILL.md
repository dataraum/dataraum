---
name: scorecard
description: Measure the current branch against the area regression suites and (optionally) an epic's KPIs; print the delta report. The harness computes verdicts, never the agent.
allowed-tools:
  - Bash
  - Read
---

# Scorecard: $ARGUMENTS

`$ARGUMENTS` is an epic slug, or empty for area regressions only. Runner:
`scorecard/run.py`; registry: `scorecard/scorecard.yaml`; model: docs/architecture/development-process.md.

```bash
uv run scorecard/run.py --list --epic <slug>                  # resolved plan, runs nothing
uv run scorecard/run.py --profile fast --epic <slug>          # checkpoint (touched areas: --areas engine)
uv run scorecard/run.py --profile full --epic <slug> --gate   # promotion verdict (exit != 0 on failure)
uv run scorecard/run.py --profile full --epic <slug> --pr-body  # markdown report to stdout
```

- Output lands in `scorecard/out/scorecard.{json,md}` (gitignored). Read the md;
  report its content faithfully — including SKIPPED checks (a skip is visible
  coverage loss, not a pass).
- **Baseline capture** (during `/epic define`, on main): run `--profile full
  --epic <slug>`, paste the measured KPI values into the epic file's `baseline:`
  fields before the definition PR.
- A KPI `error` or `pending` is a finding to surface, never something to
  paraphrase into a pass.
