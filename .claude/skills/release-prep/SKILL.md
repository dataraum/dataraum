---
name: release-prep
description: Pre-release editorial sweep — make sure README, docs, CHANGELOG, and version metadata reflect what actually shipped before tagging.
allowed-tools:
  - Read
  - Edit
  - Bash
  - Grep
  - Glob
  - AskUserQuestion
---

# Release prep: $ARGUMENTS

A version bump is about to happen. The CI preflight enforces the *mechanical*
facts — version match across `pyproject.toml` / `server.json`, `server.json`
schema, and numeric counts of phases / MCP tools / detectors via
`scripts/check_doc_counts.py`. CI cannot tell you whether the **prose** still
matches reality. That's this skill's job.

**Run this BEFORE creating the release commit and tag.**

## Input

`$ARGUMENTS` is the target version, e.g. `0.2.2`. If empty, read the version
from `pyproject.toml` and ask the user whether that's the target.

## Why this exists

Past releases shipped with stale facts (e.g. README claiming "17-phase
pipeline" after we added phase 18, missing tool rows in the tool table). The
CI count-check now catches numeric drift, but tool rows, descriptions, examples,
and links rot quietly. A 5-minute editorial pass before tagging is cheaper than
a docs-only patch release.

## Procedure

### 1. Establish the baseline

Find the previous release tag and the diff since:

```bash
PREV=$(git tag --sort=-creatordate | head -1)
echo "Previous release: $PREV"
git log --oneline "$PREV"..HEAD
git diff --stat "$PREV"..HEAD
```

If `$ARGUMENTS` was empty, also confirm the target version with the user before
proceeding.

### 2. Run the mechanical check first

```bash
uv run python scripts/check_doc_counts.py
```

If it reports drift, fix the doc files it points to before going further. This
check covers:

- numbered claims like "18 phases", "10 MCP tools", "16 detectors" across the
  user-facing docs in `DOC_FILES`
- README tool table completeness vs. tools registered in
  `src/dataraum/mcp/server.py`

### 3. Editorial sweep

For each file below, skim the **diff since `$PREV`** for behavior changes that
affect prose, then read the file and update anything that's now wrong.

Files to review (in order):

1. `README.md` — quick start commands, tool table descriptions, workflow
   example, doc links, badges, install instructions
2. `CHANGELOG.md` — must have a section for `$ARGUMENTS` summarizing user-facing
   changes (added / changed / removed / fixed). Don't list internal refactors.
3. `docs/index.md` — landing page claims and entry points
4. `docs/pipeline.md` — phase list and per-phase descriptions if phases were
   added/removed/renamed
5. `docs/entropy.md` — detector list, scores, thresholds
6. `docs/architecture.md` — module diagram, tool count, data flow
8. `docs/configuration.md` — env vars, paths, contracts
9. `docs/data-model.md` — tables, schema fields
10. `docs/contributing.md` — module tree, dev commands

For each: ask "what would surprise a new user who reads this against today's
behavior?" — that's what to fix.

### 4. Verify documented commands actually work

If the docs claim a CLI command exists, run `--help` on it. If they show a
shell example, sanity-check that it parses (`bash -n` on snippets, or just
read carefully). Tool descriptions in the README table should match the
descriptions registered in `src/dataraum/mcp/server.py` at the spirit level
(don't mechanically copy the multi-paragraph server description into the
table — keep the README terse).

### 5. Version metadata

Confirm the version bump touches all three places (CI will enforce this on
tag, but catching it now is cheaper than a failed release):

```bash
grep '^version = ' pyproject.toml
python -c 'import json; print(json.load(open("server.json"))["version"])'
python -c 'import json; print(json.load(open("server.json"))["packages"][0]["version"])'
```

All three must equal `$ARGUMENTS`.

### 6. Final mechanical check + summary

```bash
uv run python scripts/check_doc_counts.py
uv run pytest --testmon tests -q
```

Report to the user:

- the previous tag and number of commits since
- which doc files you edited and a one-line summary per file
- the CHANGELOG entry you wrote (paste it)
- anything you noticed but didn't change (because it wasn't clearly stale or
  needed product judgment)

Then stop. **Do NOT create the release commit, tag, or PR yourself** — the user
does that. The skill ends with "ready for tagging".

## Rules

- This is editorial, not architectural. Don't refactor. Don't rename anything.
- Don't invent new docs pages. If something is undocumented, say so in the
  summary so the user can decide whether to defer.
- Don't update `plans/` (gitignored), Confluence, or Jira from here.
- Don't push, don't tag, don't run the release workflow.
- If the diff reveals a behavior change that has no doc anywhere, flag it
  loudly in the summary — that's the most important thing this skill catches.
- If `scripts/check_doc_counts.py` exits non-zero after your edits, you are
  not done.
