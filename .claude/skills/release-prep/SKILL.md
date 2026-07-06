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

A release is about to be cut. Releasing = the user publishes a GitHub Release with tag
`v<version>`; `release.yml` then pushes the three container images to GHCR
(`dataraum`, `dataraum-cockpit`, `dataraum-cockpit-migrate`), each tagged `:{version}` +
`:latest`. **The git tag is the single source of version truth** — nothing is published
to PyPI or npm. CI's preflight runs `packages/engine/scripts/check_doc_counts.py`
(numbered phase/detector claims across the doc files it lists). CI cannot tell whether
the **prose** still matches reality; that's this skill's job.

**Run this BEFORE the release commit and tag.** Scope is the whole monorepo.

## Input

`$ARGUMENTS` is the target version, e.g. `0.3.0`. If empty, read
`packages/engine/pyproject.toml` and ask the user for the target.

## Procedure

### 1. Baseline

```bash
PREV=$(git tag --sort=-creatordate | head -1)
git log --oneline "$PREV"..HEAD | wc -l
git diff --stat "$PREV"..HEAD | tail -5
```

### 2. Mechanical check

```bash
(cd packages/engine && uv run python scripts/check_doc_counts.py)
```

If it reports drift, fix the docs it names. Note: the script **silently skips missing
files** — if the docs tree moved, fix its `DOC_FILES` first or it checks nothing.

### 3. Editorial sweep

Skim the diff since `$PREV` for behavior changes, then review these against today's
behavior — the question per file is "what would surprise a new user?":

1. `README.md` (root) — status claims, quick start, release-overlay instructions
2. `packages/engine/README.md` + `packages/cockpit/README.md` — package maps, dev loops
3. `docs/` (workspace root, the published site) — `index.md`, `getting-started/`,
   `concepts/` (pipeline table, relationships), `platform/architecture.md`,
   `operations/deployment.md`
4. `CHANGELOG.md` (root) — add the `$ARGUMENTS` section. **Keep it brief**: the net
   user-facing change since `$PREV`, grouped Added / Changed / Removed, thematic bullets
   with ADR links — never a per-commit or per-PR list. Intermediate states that shipped
   and were retired within the cycle get no entry (Keep-a-Changelog documents releases,
   not the path between them).

### 4. Verify documented commands

Run the quick-start and dev-loop commands the docs claim (or at minimum parse-check
them). The compose quick start + the release overlay
(`docker-compose.release.yml` + `DATARAUM_VERSION`) are the two a new user hits first.

### 5. Version metadata

```bash
grep '^version = ' packages/engine/pyproject.toml   # must equal $ARGUMENTS
(cd packages/engine && uv lock --offline)            # keep uv.lock's own version in sync
```

The cockpit's `package.json` carries no version — the image tag is its version. If that
changes, add it here.

### 6. Release gate — the live eval family (ADR-0019 tier 3)

The system/UAT tier runs at release cut, not per-PR. **Confirm with the user
before any live spend**, then, in `../dataraum-eval`:

1. Mint a **fresh seed corpus** (dataraum-testdata) — data the merged code has
   never seen, with exact ground truth.
2. Run the lean gate: `clean` + the union of the released epics' promotion
   `strategies`, plus the agentic `/investigate` financial leg against the
   corpus ground truth.
3. Score by eval's own rules (recall as ordering with margins, pooled pass
   rates — never point thresholds). Record GO/NO-GO in eval's `docs/history.md`.

NO-GO blocks the tag — findings become epics, not footnotes.

### 7. Wrap up

Re-run the mechanical check, then report: previous tag + commit count, files edited
(one line each), the CHANGELOG entry (paste it), and anything noticed but deliberately
not changed. Then stop — **the user creates the release commit, tag, and GitHub
Release**. Docs deploy independently of the release (`docs.yml`, on any main push
touching `docs/**`).

## Rules

- Editorial, not architectural — don't refactor or rename.
- Don't invent docs pages; flag gaps in the summary instead.
- A behavior change with no doc anywhere is the loudest thing to flag.
- Don't push, don't tag, don't publish the release.
