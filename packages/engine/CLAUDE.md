# DataRaum Context Engine

A Python library for extracting rich metadata context from data sources to power AI-driven data analytics. Instead of giving AI tools to discover metadata at runtime, we pre-compute comprehensive metadata and serve it as structured context documents interpreted through domain ontologies.

## Core Philosophy

This project prioritizes **correctness over speed**. We would rather have working code slowly than broken code quickly.

### Analytical Correctness Is the Product

The entropy detectors, pipeline phases, and fix system are the core value proposition. "Working" means: **the system finds real data quality issues and helps users fix them.** It does NOT mean: the code compiles, tests pass, scores are produced.

A detector that scores 0.00 on a column with 3% garbage values is **broken**, not "measuring something different." A fix system that writes YAML but the detector never fired is **untested**, not "ready for review." An e2e test asserting `coverage >= 0.5` is **accepting 50% failure**, not "providing safety."

### Ground Truth, Not Current Behavior

The `dataraum-testdata` project generates data with **known injections** and an `entropy_map.yaml` listing exactly what was injected. This is the oracle. Detector correctness is measured by:
- **Recall**: did the detector find the known injection? (score > threshold for the affected column)
- **Precision**: on clean data, are scores low? (no false alarms)

When a detector misses a known injection, the detector has a bug. Not a "design gap." Not a "future improvement." A bug that needs fixing.

### No Sugar-Coating

Do not:
- Label broken detectors as "design gaps" or "out of scope"
- Write tests that assert against current (broken) behavior
- Set weak thresholds (coverage >= 0.5) to make tests pass
- Create Jira issues to defer bugs instead of fixing them
- Claim the system "works" when it finds 3 of 15 known problems

When something doesn't work, say so plainly, then fix it or propose how to fix it.

## Critical Rules

### Investigate Before Acting

This is the most important rule. The codebase evolves fast — modules get deleted, APIs get renamed, entire subsystems get redesigned. **Your training data is stale. Your assumptions are wrong.** The only source of truth is the code on disk right now.

Before proposing or implementing anything:
1. **Read the actual code** involved — not just the file you plan to edit, but its callers and dependencies. Skim, don't assume.
2. **Grep for the thing you're about to use** — does that function, class, config key, or CLI command actually exist? If you can't find it, it was probably deleted or renamed.
3. **Check imports and call sites** — if you're changing a function signature, find every caller first. If you're adding an import, verify the module exists.
4. **When in doubt, read more code, not less.** A 30-second grep saves a 30-minute wrong-direction detour.

Do NOT:
- Assume a module, function, or pattern exists because it "should" or because it "makes sense for this kind of project"
- Propose using framework features without verifying they're actually in this codebase
- Generate code that imports from modules you haven't confirmed exist
- Suggest architectural patterns based on what's common in similar projects — check what THIS project actually does

When you catch yourself thinking "this project probably has X" — stop and grep for X. If it's not there, it's not there.

### Never Claim "Done" Until:
1. ALL tests pass (not just the file you changed)
2. You have verified actual output matches expected behavior
3. Type checking passes
4. Linting passes

If any of these fail, the task is NOT complete. Fix the issues before declaring success.

### The "Three Strikes" Rule
If you've attempted the same fix 3 times without success:
1. STOP making changes
2. Explain what you've tried and what you observed
3. Form a hypothesis about the ROOT CAUSE (not just symptoms)
4. Ask for guidance or propose a fundamentally different approach

Do not continue making random changes hoping something works.

### Calibration Is the Definition of Done

Entropy detectors are validated by **calibration against ground truth** in `dataraum-eval` (separate repo). The calibration harness:
1. Generates clean + medium data via `dataraum-testdata`
2. Runs the pipeline on both
3. For each injection in `entropy_map.yaml`, asserts the detector found it
4. On clean data, asserts no false alarms

A detector is "done" when its calibration tests pass. Unit tests in this repo verify internal logic; calibration tests verify the detector does its job.

**When a calibration test fails:**
1. The detector has a bug — fix the detection logic
2. Do NOT weaken the calibration threshold
3. Do NOT label it a "design gap" and defer — it's a bug
4. If the detector fundamentally cannot find this injection type, redesign it

## Problem-Solving Standards

### When Something Doesn't Work
1. **Read the actual error message** — quote it in your response
2. **Form a hypothesis** about WHY this error occurred
3. **Verify your hypothesis** before attempting a fix
4. **Make ONE targeted change** to test your hypothesis
5. **Observe the result** — did it confirm or refute your hypothesis?

Do NOT:
- Make multiple simultaneous changes
- Modify tests to make them pass (unless the test itself is wrong)
- Assume simple explanations for persistent problems
- Skip the hypothesis step

### Test Failures Are Information
- The test is probably right and your code is wrong
- Understand WHAT the test expects and WHY
- Only modify the test if you can articulate why the test's expectation is incorrect

## Branching & Pull Requests

All work happens on feature branches, never directly on `main`.

### Branch naming
Use `type/description` where type is one of: `feat`, `fix`, `refactor`, `docs`, `chore`, `test`.
Examples: `feat/bayesian-network`, `fix/cpt-ordering`, `refactor/streamline`.

### Workflow
1. **Before starting work**: if on `main`, ask the user before creating a feature branch.
2. **During work**: commit after each verified phase (tests green).
3. **When done**: create a PR to `main` via `gh pr create` (only when the user asks).
4. **Never push directly to `main`.**

## Development Workflow

This project uses skills to structure development work. Skills encode the checkpoints, reviews, and handoffs that are otherwise easy to skip or shortcut.

### Intent routing

| User says | You do | Why |
|-----------|--------|-----|
| "What if we...", "I have an idea", "We need..." | `/ideate` | Explore before committing to a direction |
| "Let's break this down", "Create the epic" | `/decompose` | Turn designs into Jira work items |
| "Implement X", "Build X", "Work on DAT-nnn" | `/refine` first | Understand before committing — the model's "obvious" is often wrong |
| "Is X feasible?", "How should we approach X?" | `/refine` | Stop after alignment, don't start coding |
| Approved approach after `/refine` | `/implement` | Phased execution with checkpoints and review gate |
| MCP tool changes completed | Remind: restart session, then `/smoke` | Server runs old code until restarted |
| Implementation verified and reviewed | Update `.claude/handoff.md` | Bridge to dataraum-eval and dataraum-testdata |
| "Cut a release", "prep v0.x.y", before tagging | `/release-prep` | Editorial sweep on README/docs/CHANGELOG; CI enforces counts |
| Quick fix, <3 files, obvious change | Direct implementation | S-size tasks don't need ceremony |

### Skills (`.claude/skills/`)

**Product thinking:**
- **`/ideate <topic>`** — Explore an idea: check existing work, read the codebase, think through feasibility, produce a design document. No issues, no code.
- **`/decompose <doc or issue>`** — Turn a design into Jira artifacts: epic, phase issues, acceptance criteria, dependency relations. Follows the DAT-173 pattern.

**Execution:**
- **`/refine <issue>`** — Pre-implementation: explore feasibility, surface spec vs. reality conflicts, align on approach. No code.
- **`/implement <issue>`** — Phased execution with mandatory self-audit checkpoints. Invokes senior-code-reviewer + spec-compliance-reviewer at the end. Updates `handoff.md`.
- **`/smoke [tool]`** — Quick MCP UX check: call the tools you just built, feel how they work. Requires session restart if server code changed.
- **`/release-prep [version]`** — Pre-release editorial sweep across README, `docs/`, `CHANGELOG.md`, and version metadata. Runs `scripts/check_doc_counts.py`. Stops before tagging.

### Cross-repo handoff

`.claude/handoff.md` records what needs attention in other repos:
- **dataraum-eval**: which tools/detectors changed, what to calibrate (consumed by `/accept` skill in eval)
- **dataraum-testdata**: directional hints for new injections or ground truth (not specs — testdata has its own design concerns)

### When to skip

- **S-size tasks** (1-3 files, <50 lines, obvious): implement directly
- **Bug fixes with failing tests**: fix → verify → done
- **Documentation-only changes**: no workflow needed
- **Verified-dead deletions**: if grep proves zero consumers, just delete — don't refine, don't audit-doc, don't invoke reviewers

### Bundle small work

Per-ticket nano-branches are friction without payoff. Default to bundling:

- **Combine multiple small tickets in one branch/PR**, even if topically unrelated. A "v0.2.2 cleanup" branch with three small commits (one per ticket) is cheaper to ship than three back-to-back branches each paying the rebase + CI + PR overhead.
- **Piggyback small fixes onto an open larger branch** when discovered mid-flight. If `/smoke` surfaces a one-file bug while testing a feature branch, stack the fix as a commit on that branch rather than spinning up a new branch — the bundled PR is easier to review and rolls back together.
- **One ticket = one branch is wrong as a default.** One ticket = one commit (or one commit group), shipped in whatever branch is convenient.

The line: bundle anything S-size and Light (see above). Keep Heavy items (behavior changes, prompt engineering, response-shape changes, detector changes) on their own branches so reviewers can isolate them.

### When NOT to skip

- **M+ tasks**: always `/refine` first
- **MCP tool changes**: always `/smoke` after — UX can't be judged from source code
- **Detector changes**: always update `handoff.md` — calibration is the definition of done

### Contracts-first for DAT-294 platform work

The DAT-294 platform splits the codebase into parallel work streams (control plane, executors, frontend, observability, chat BFF). Parallel work without locked contracts produces merge mush. The rule:

- **No implementation work starts on a platform phase until its contract is locked** on the [Platform Contracts](https://real-dataraum.atlassian.net/wiki/spaces/DD/pages/18972674/Platform+Contracts) page.
- **Contracts are the sync point.** Frontend codes against types generated from the contract; backend codes against the same contract; they meet at smoke. Don't synchronize on running code — synchronize on the spec.
- **Spec is source of truth, code generates from spec.** Protos live in `src/dataraum/proto/`, OpenAPI in `src/dataraum/control_plane/openapi.yaml`, etc. Never the reverse.
- **The seven contracts** are inventoried on the page: Mcp-Session-Id semantics, CP↔executor gRPC service, sessions/principals SQLAlchemy schema, coordination events schema, config storage shape, OpenAPI for `/api/*`, TanStack AI wire format. Each gates a specific phase (P1, P2, P3, P8, P9).
- **Per the no-backwards-compat rule**, "migration" means porting SQLAlchemy code to Postgres dialect, not Alembic scripts. Schemas drop-and-reinit at cutover.

This rule applies only to DAT-294 phases (P1+). It does NOT apply to v0.2.x maintenance work on `main`.

### Parallel platform work runbook

For DAT-294 phase tasks (decomposed children of P1–P11), multiple tasks within a phase can run as parallel lanes. Each lane = one worktree = one branch = one PR. This runbook replaces the library-style handoff in `/implement` for platform work.

**Invoke `/take {task-id}` to run this end-to-end as one lane.** The skill executes the steps below. To run N lanes concurrently from one orchestrator session, call `/take` via the Agent tool with `isolation: "worktree"` in a single message with multiple tool blocks — that's where the parallelism comes from.

**Per-lane workflow** (what `/take` does):

1. **Verify prerequisites.** The task's contract is locked (see Platform Contracts page). All `is blocked by` dependencies are merged. No other lane already claims this task ID — check `.claude/platform-status.md` and `gh pr list --search "{task-id} in:title"`.
2. **Open the worktree.**
   ```bash
   git fetch origin main
   git worktree add .worktrees/{task-id} -b feat/{task-id}-{slug} origin/main
   cd .worktrees/{task-id}
   ```
3. **Run `/refine` then `/implement` {task-id}** inside the worktree. The discipline (scope, checkpoints, psych safety, review gate) belongs to `/implement`; its handoff section auto-skips for platform-shell work.
4. **Lane smoke** scoped to this task's contract surface only (`tests/platform/smoke_{task-id}.py`).
5. **Open the PR.**
   ```bash
   gh pr create --title "{task-id}: {title}" --body "..."
   ```
   PR body must reference the contract version (file + sha) consumed and list other open lanes via `gh pr list --search "DAT-294 in:body"` so reviewers see the parallel context.
6. **Update `.claude/platform-status.md`** with one row per active lane (Task | Worktree | Branch | PR | Contract | Status). Remove the row when the PR merges.

**Parallel etiquette (mandatory):**

- **Never edit a contract from inside a lane.** Contracts change only via dedicated contract-lock PRs. If you find a contract is wrong, STOP the lane and surface it — don't fix it in passing.
- **Never reach into another lane's code.** Drive-by fixes break parallel discipline. File a follow-up ticket; don't cross lanes.
- **Rebase once, at the end.** Continuous rebasing during parallel work is merge soup. If `main` moved, rebase before opening the PR — not during.
- **Lane smoke is yours; integration smoke is `main`'s.** Don't try to keep integration green from inside a lane — `main` is allowed to flicker red between phases per DAT-294.
- **If a lane needs more than ~2 days, decompose further.** Multi-day lanes usually have hidden coupling.

**Parallel launch (orchestrator pattern):** to run N lanes concurrently from one orchestrator session, launch each as `/take {task-id}` via the Agent tool with `isolation: "worktree"`, in a single message with multiple tool blocks. The orchestrator does NOT touch code — it decomposes the phase into tasks (`/decompose`), spawns the lanes, watches `gh pr list` and `.claude/platform-status.md`, surfaces conflicts, unblocks downstream lanes as PRs merge, and triggers journey smoke at milestones.

## Work Decomposition Protocol

### Task Sizing
Before starting any work item, classify it:

| Size | Definition | Approach |
|------|-----------|----------|
| **S** | 1-3 files, <50 lines changed | Feature branch, direct implementation, no plan needed |
| **M** | 3-8 files, <200 lines changed | Feature branch, Plan Mode, single session |
| **L** | 8+ files or 200+ lines | Feature branch, Confluence document linked to issue, phased execution |
| **XL** | Spans multiple modules or repos | Plan approval required, integration branch, phased PRs |

### For M/L/XL tasks: mandatory plan structure
Plans live as Confluence documents (linked to the relevant Jira issue) and must include:
1. **Scope**: What changes. What explicitly does NOT change.
2. **Files affected**: List every file. Mark read-only/do-not-touch files.
3. **Dependency order**: Which steps block which (e.g., A1a → A1b).
4. **Per-step acceptance criteria**: Verifiable outcomes, not vague descriptions.
5. **Test plan**: Which test files cover this step, what new tests are needed.
6. **Rollback**: How to safely abort mid-phase (revert strategy, branch state).

### Phasing Rules
- Each phase MUST leave ALL tests green — no half-done states
- Commit after each verified phase
- Never start phase N+1 until phase N passes all checks
- For L/XL: use a long-lived feature branch, PR per phase into it

### Boundaries — Scope Creep Prevention
Every plan must declare:
```
DO change: [list of files]
DO NOT change: [list of files that must remain untouched]
```
If you find yourself wanting to edit a file not in the "DO change" list, STOP and discuss first. Unplanned edits to adjacent code are the #1 source of refactoring errors.

## After Writing Code — Verification Sequence

1. **Read back what you wrote** — does it match the requirement?
2. **Run the specific test file** for the module you changed
3. **Check imports** — no unused imports, no missing imports
4. **Check callers** — does the function signature match all call sites? (grep for usage)
5. **For refactors: verify the old code path is dead** — grep for references to removed/renamed functions
6. **For rewrites: verify nothing imports from `_legacy/`** — legacy code is reference only

## Testing

### Test Quality
- Each test should test ONE thing
- Test names should describe the expected behavior
- Tests should be independent — order should not matter
- Prefer many small, focused tests over few large tests

### Test-Driven Debugging
When fixing a bug:
1. First write a failing test that reproduces the bug
2. Then fix the bug
3. Verify the test now passes

### When Tests Become Bloated
If you find yourself iterating heavily on tests:
1. STOP
2. Step back and understand what behavior you're actually testing
3. Delete the bloated test
4. Write a fresh, minimal test for that single behavior

### Running Tests

This project uses `pytest-testmon` to only re-run tests affected by code changes.

**During development (after each code change):**
```bash
uv run pytest tests/unit/path/to/test_file.py -v
```

**After finishing a feature or fix:**
```bash
uv run pytest --testmon tests/unit -q
```

**Before declaring a task done:**
```bash
uv run pytest --testmon tests -q
```

**Rules:**
- **NEVER run `pytest tests/ -v` (full suite without testmon)** — takes 2+ minutes
- **Only run specific integration test files** when you changed integration-level code
- Unit tests: `tests/unit/` (~760 tests, ~13s). Integration: `tests/integration/` (~300 tests, ~2min)
- The end-of-turn hook runs testmon automatically — don't duplicate its work

### Calibration Tests (separate repo: dataraum-eval)
Calibration tests run the full pipeline against testdata with known injections. They are the ultimate measure of detector correctness. See DAT-133 / DAT-135 in Jira.

```bash
# Run calibration (from dataraum-eval repo)
uv run pytest calibration/ -v
```

**When a calibration test fails:**
1. Identify which detector missed which injection
2. Read the detector source code and the injection parameters
3. Understand WHY the detector doesn't find the problem (not just THAT it doesn't)
4. Fix the detector — redesign if needed
5. Re-run calibration to verify

## Code Quality

### Changes Should Be Minimal
- Prefer small, targeted changes over broad rewrites
- Each commit should do ONE thing
- If you're changing many files, question whether you're taking the right approach

### Avoid Premature Abstraction
- Write concrete code first
- Only abstract when you see actual duplication (rule of three)
- Simple, readable code beats clever, abstract code

## Definition of Done

- [ ] All existing tests still pass
- [ ] New functionality has tests
- [ ] Type checking passes
- [ ] Linting passes
- [ ] Output verified (not just "it compiles")
- [ ] No debug statements left in code
- [ ] Error handling is in place
- [ ] Edge cases are handled
- [ ] Docs updated (if user-facing behavior changed)
- [ ] **For detector changes**: calibration recall did not regress (run `dataraum-eval`)
- [ ] **For fix system changes**: fix loop test passes (apply fix → score drops)

## Cross-Project Work

For work spanning multiple repos:
1. Plan lives in the *coordinating* repo
2. Each repo gets its own sub-plan with repo-local acceptance criteria
3. Integration testing happens in a dedicated step AFTER per-repo work
4. Never assume another repo's state — verify with tests/imports
5. Pin dependencies between repos during cross-project work

## Quality Gates (Automated via Hooks)

These are enforced mechanically — you don't need to remember them, but know they exist:
- **Post-edit**: `ruff format` runs after every file edit
- **End-of-turn**: `ruff check` + `mypy` + `pytest --testmon` blocks if any fail

## Documentation

### Docs Site

User-facing documentation lives in `docs/` and is published via [Zensical](https://zensical.org/) to GitHub Pages.

| Location | Purpose | Published |
|----------|---------|-----------|
| `docs/*.md` | User-facing guides (pipeline, entropy, CLI, MCP setup, etc.) | Yes — in site nav |

**Local preview:** `uv run zensical serve`
**Build:** `uv run zensical build --clean`

### When to Update Docs

- **New user-facing feature or behavior change** → update the relevant `docs/*.md` page
- **Internal implementation detail** → use docstrings in source code
- **Design decision or project plan** → create a Confluence document

### Docstring Convention

Google-style docstrings, required on **new** public functions, classes, and methods. No backfill obligation.

```python
def infer_types(cursor: DuckDBPyConnection, table: str) -> Result[TypeProfile]:
    """Infer column types from raw VARCHAR data.

    Args:
        cursor: DuckDB connection with the raw table loaded.
        table: Name of the raw staging table.

    Returns:
        TypeProfile with inferred types and cast failures.

    Raises:
        StorageError: If the table does not exist.
    """
```

Rules:
- One-line summary in imperative mood ("Infer types", not "Infers types")
- `Args`, `Returns`, `Raises` sections when applicable
- Not required on: private functions (`_helper`), trivial one-liners, test functions
- Enforced by ruff `D` rules (Google convention, relaxed for existing code)

## Current Work

Check [Jira](https://real-dataraum.atlassian.net/jira/software/projects/DAT/boards/35) for active issues, plans, and project documents. Jira MCP is available.

## Architecture

### Key Design Decisions

- **VARCHAR-first staging** — All data loaded as VARCHAR to preserve raw values. Type inference happens in profiling, not during load. Prevents silent data loss.
- **Quarantine pattern** — Failed type casts go to quarantine tables for review, not pipeline failure.
- **Pre-computed context** — AI receives a pre-assembled `ContextDocument` with all metadata already computed and interpreted through the selected ontology. No runtime discovery.
- **Ontologies as configuration** — Domain ontologies (financial_reporting, marketing, etc.) are YAML configs that map column patterns to business terms, define computable metrics, and guide semantic interpretation.
- **Pipeline measures, doesn't interpret** — Pipeline runs detectors as post-steps. Interpretation (why, hypothesize) happens interactively via MCP tools. No gate phases.
- **BBN readiness replaces LLM quality grades** — Per-column readiness (ready/investigate/blocked) via Bayesian network. `column_quality` detector retired (was circular with BBN).
- **MCP code retained, transport retired** — `src/dataraum/mcp/server.py` still holds the 12-tool engine logic; the HTTP MCP mount is gone (v1 plan pivot). Engine logic migrates into FastAPI handlers at `src/dataraum/api/` as the v1 plan progresses. The cockpit (sibling package `../cockpit`) consumes these routes via the generated OpenAPI client.
- **Free-threading** — Python 3.14t with GIL disabled for true CPU parallelism in pipeline phases.

### Module Structure

```
src/dataraum/
├── analysis/       # Data analysis (typing, statistics, correlations, relationships,
│                   #   semantic, temporal, slicing, cycles, validation)
├── entropy/        # Uncertainty quantification (detectors, measurement, BBN)
├── graphs/         # Calculation graphs, context assembly
├── investigation/  # Session trace models for MCP audit trail
├── pipeline/       # Pipeline orchestrator (17 phases), fixes
├── sources/        # Data source loaders (CSV, Parquet)
├── storage/        # SQLAlchemy models, migrations
├── llm/            # LLM providers and prompts
├── core/           # Config, connections, utilities
├── server/         # FastAPI control plane shell (/health, DuckLake bootstrap)
├── api/            # Engine REST surface (lands in v1 plan step 3b)
└── mcp/            # Legacy MCP tool implementations — kept for extraction; no transport mounted
```

SQLAlchemy DB models are co-located with business logic in `db_models.py` files within each module.

### Data Flow

```
Source (CSV/Parquet/JSON) → [staging] VARCHAR → raw_{table}
  → [profiling] Type inference → typed_{table}, quarantine_{table}
  → [enrichment] LLM semantic analysis → roles, entities, relationships
  → [enrichment] Temporal + topology → additional metadata
  → [quality] LLM rule generation + entropy → scores, actions
  → [context] Assembly + LLM summary → ContextDocument (for AI)
```

### Quick Reference Commands

Run from the workspace root unless noted otherwise.

```bash
# Bring up the full stack (Postgres + control-plane + cockpit container)
docker compose -f packages/infra/docker-compose.yml up -d --wait

# Or run the control plane directly from this package (DUCKLAKE_* env required)
uv run uvicorn dataraum.server.app:app --host 0.0.0.0 --port 8000

# Health probe (substrate + DuckLake + Postgres)
curl -fsS http://localhost:8000/health

# Cockpit (TanStack Start) — see packages/cockpit
open http://localhost:3000

# Regenerate the OpenAPI contract consumed by the cockpit
uv run python scripts/export_openapi.py > ../api/openapi.yaml
```

### Code Patterns
```python
# Error handling — use Result type, not exceptions
from dataraum.core.models import Result

async def some_operation() -> Result[SomeOutput]:
    try:
        return Result.ok(output, warnings=["minor issue"])
    except SomeExpectedError as e:
        return Result.fail(str(e))

# Database connections — always use context managers
with manager.session_scope() as session:
    with manager.duckdb_cursor() as cursor:
        result = some_operation(cursor, session)
```

### Code Style
- Type hints on all functions
- Pydantic models for data classes
- No classes where functions suffice
- Max function length: ~50 lines
