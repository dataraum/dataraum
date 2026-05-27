# DataRaum Context Engine

Python library that pre-computes rich metadata from data sources and serves it as structured **context documents** — interpreted through domain ontologies — so AI tools reason over prepared context instead of discovering metadata at runtime.

## What "working" means here

Correctness over speed. The product is **analytical correctness** — the system finding real data-quality issues — not "code compiles, tests pass, scores produced."

- **Ground truth is the oracle.** `dataraum-testdata` generates data with known injections plus an `entropy_map.yaml`. A detector is correct when it has **recall** (finds the injection) and **precision** (no false alarms on clean data), proven by **calibration** in the `dataraum-eval` repo — not by unit tests.
- A detector that misses a known injection has a **bug** — not a "design gap," not "out of scope." Say so plainly and fix it; don't weaken the threshold or file a ticket to defer it.

## Working in this codebase

**Investigate before acting.** The codebase moves fast — modules get deleted and renamed. Your training assumptions are stale; the code on disk is the only truth. Grep for a function/class/config key before using it, check call sites before changing a signature, read more code when unsure.

**Then act — don't cling.** Investigate to *decide*, not to keep re-confirming. Existing code and tests follow the design; they never constrain it. Make the clean cut in one pass and don't quote prior notes (even your own, even recalled memory) as constraints. Full rule: "Default to the clean cut" in the workspace CLAUDE.md.

**Branching.** Feature branches only (`type/description`: feat / fix / refactor / docs / chore / test); never push to `main`. Commit after each verified (green) phase. Open PRs with `gh pr create` only when asked.

**Skills drive the work** (`.claude/skills/`):

| Intent | Skill |
|--------|-------|
| "I have an idea", "what if…" | `/ideate` → design doc |
| "break this down", "create the epic" | `/decompose` → Jira epic |
| "implement X", "is X feasible?" | `/refine` first (understand, surface conflicts) |
| approved approach | `/implement` (phased; invokes the two reviewers at the end) |
| UI or tool just built | `/smoke` (drive it, feel the UX) |
| cutting a release | `/release-prep` |
| quick fix (<3 files, obvious) | just do it |

`/implement` updates `.claude/handoff.md`, the bridge telling `dataraum-eval` (calibration) and `dataraum-testdata` what changed. Detector changes always update it.

**Sizing.** S (1–3 files): direct. M (3–8 files): plan, single session. L/XL (8+ files or cross-module): Confluence plan linked to the Jira issue, phased, each phase green before the next. Declare DO-change / DO-NOT-change file lists to fence *unrelated* scope. Cleanup the design implies — deleting dead code, removing a retired field, adapting its tests — is in-scope, not an adjacent-edit violation. What to avoid is unplanned *unrelated* edits, never design-implied deletion (see "Default to the clean cut" in the workspace CLAUDE.md).

## Testing

`pytest-testmon` re-runs only affected tests.

```bash
uv run pytest tests/unit/path/to/test_file.py -v   # during dev
uv run pytest --testmon tests/unit -q              # after a change
uv run pytest --testmon tests -q                   # before declaring done
```

- Never run the full suite without `--testmon` (2+ min). Run specific integration files only when you touched integration code.
- **Calibration tests live in `dataraum-eval`** and are the ultimate measure of detector correctness.
- **e2e tests make real LLM calls — never run them without asking.**
- The end-of-turn hook runs ruff + mypy + vulture + testmon automatically; don't duplicate it.

## Definition of done

Tests pass · type-check passes · lint passes · output verified (not just "it compiles") · new behavior has tests · no debug leftovers. For detector changes: calibration recall did not regress.

## Architecture

The engine is a **Temporal activity worker** (`src/dataraum/worker/`, entrypoint `python -m dataraum.worker.main`) — no HTTP surface. It bootstraps the substrate once (DuckLake anchor + one workspace `ConnectionManager`, reusing `src/dataraum/server/{storage,workspace}.py`), then serves the **bundled** `AddSourceWorkflow` + the phase activities on one task queue. The cockpit (`../cockpit`) reads engine metadata directly from the `ws_<id>` Postgres schema via Drizzle and triggers workflows via the Temporal Client. No OpenAPI, no codegen. The legacy MCP surface has been moved out of the package to `reference/mcp/` (DAT-369) — **dead code**, no transport, no in-tree consumer, kept only as a copy-reference during the cockpit takeover and slated for deletion in slice 2. Do not extend it, build on it, import from it, or treat its presence as a reason to preserve anything related. It is the one tolerated exception, not a pattern to emulate.

**Key design decisions:**
- **VARCHAR-first staging** — everything loads as VARCHAR; type inference happens in profiling, not load. Failed casts go to quarantine tables, never pipeline failure.
- **Pre-computed context** — AI receives a fully-assembled `ContextDocument`; no runtime discovery.
- **Ontologies are config** — domain ontologies (financial_reporting, marketing, …) are YAML mapping column patterns → business terms, defining metrics, guiding interpretation. They live in `packages/dataraum-config/` (bind-mounted at `/opt/dataraum/config`); load them only through `dataraum.core.config`, never `Path(__file__)` navigation.
- **Pipeline measures, doesn't interpret** — detectors run as pipeline post-steps; interpretation happens interactively through the cockpit (Temporal workflows + chat).
- **BBN readiness** — per-column ready / investigate / blocked via a Bayesian network.
- **Concurrency** — standard **GIL-on** CPython 3.14 (container `python:3.14-slim`; free-threading was evaluated and dropped as a target). The Temporal activity worker still runs phases concurrently on a `ThreadPoolExecutor`, so shared worker state — notably the one `ConnectionManager` — is touched by multiple activity threads; guard it as concurrent.

**Temporal (durable orchestration).** Skill: `npx skills add temporalio/skill-temporal-developer`. The engine runs a **bundled Python worker** (`worker/`): `AddSourceWorkflow` (`worker/workflows.py`, sandbox-deterministic, imports only `temporalio` + the engine-free `worker/contracts.py`) **and** the phase activities (`worker/activities.py`, `@activity.defn(name="<phase>")` over `run_phase_activity`) on one task queue. Activities are **sync**, run on a `ThreadPoolExecutor` (NOT `asyncio.to_thread`). The cockpit triggers workflows via the Temporal Client. Workflow names are called by string; no shared catalogue. Locked decision + the DAT-360→DAT-344 reversal (workflows are Python, not TS) live in the `feedback-durable-execution-lean` memory.

**Module layout:**
```
src/dataraum/
├── analysis/    # typing, stats, correlations, relationships, semantic, temporal, slicing, cycles, validation
├── entropy/     # detectors, measurement, BBN
├── graphs/      # calculation graphs, context assembly
├── pipeline/    # orchestrator + phases, fixes
├── sources/     # loaders — CSV, Parquet, JSON, DB-via-recipe
├── storage/     # SQLAlchemy models (co-located in db_models.py per module)
├── llm/         # providers + prompts
├── core/        # config, connections, settings
├── worker/      # Temporal activity worker — workflows, activities, bootstrap, contracts
└── server/      # substrate bootstrap (DuckLake anchor + workspace overlay); no HTTP
# (legacy mcp/ moved out of the package to reference/mcp/ — dead, slated for deletion)
```

**Data flow:** Source → VARCHAR staging → type inference (typed + quarantine tables) → LLM semantic / temporal / topology enrichment → quality (LLM rules + entropy) → `ContextDocument`.

## Conventions

```python
# Errors: Result type, not exceptions, for expected failures
async def op() -> Result[Out]:
    return Result.ok(out)        # or Result.fail(reason)

# Resources: always context managers
with manager.session_scope() as s, manager.duckdb_cursor() as c:
    ...

# Env config: typed Settings, never os.environ directly (DAT-363)
settings = get_settings()        # validates once, fails loud at boot
```

- Type hints everywhere; Pydantic for data classes; functions over classes; ~50 lines max.
- Google-style docstrings on **new** public functions (no backfill obligation); enforced by ruff `D`.
- Quality gates run as hooks — `ruff format` after each edit; ruff + mypy + vulture + testmon at end-of-turn.

## Run it

```bash
docker compose -f packages/infra/docker-compose.yml up -d --wait   # full stack
# engine health = Temporal worker heartbeat (no HTTP endpoint):
docker compose -f packages/infra/docker-compose.yml run --rm --no-deps \
  --entrypoint temporal temporal-admin-tools \
  worker list --namespace default --address temporal:7233          # → Status: Running
# or run the worker directly (DATABASE_URL / DUCKLAKE_* / TEMPORAL_* env required):
uv run python -m dataraum.worker.main
```

## Docs & tracking

- User docs: `docs/*.md`, published via Zensical (`uv run zensical serve`). Update when user-facing behavior changes; internal detail goes in docstrings.
- Design docs → **Confluence** (space DD). Active work → **Jira** (DAT-*; MCP available). Not local files.
