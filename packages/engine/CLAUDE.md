# DataRaum Context Engine

Python library that pre-computes rich metadata from data sources and serves it as structured **context documents** — interpreted through domain ontologies — so AI tools reason over prepared context instead of discovering metadata at runtime.

## What "working" means here

Correctness over speed. The product is **analytical correctness** — the system finding real data-quality issues — not "code compiles, tests pass, scores produced."

- **Ground truth is the oracle.** `dataraum-testdata` generates data with known injections plus an `entropy_map.yaml`. A detector is correct when it has **recall** (finds the injection) and **precision** (no false alarms on clean data), proven by **calibration** in the `dataraum-eval` repo — not by unit tests.
- A detector that misses a known injection has a **bug** — not a "design gap," not "out of scope." Say so plainly and fix it; don't weaken the threshold or file a ticket to defer it.

## Working in this codebase

**Investigate before acting.** The codebase moves fast — modules get deleted and renamed. Your training assumptions are stale; the code on disk is the only truth. Grep for a function/class/config key before using it, check call sites before changing a signature, read more code when unsure.

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

**Sizing.** S (1–3 files): direct. M (3–8 files): plan, single session. L/XL (8+ files or cross-module): Confluence plan linked to the Jira issue, phased, each phase green before the next. Declare DO-change / DO-NOT-change file lists — unplanned edits to adjacent code are the #1 source of bugs.

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

The engine is a **Starlette kernel** (`src/dataraum/server/`) exposing `/measure` (SSE), `/query` (Arrow), `/probe` (read-only SQL), and `/health`. No OpenAPI, no codegen. The cockpit (`../cockpit`) reads engine metadata directly from the `ws_<id>` Postgres schema via Drizzle and calls these verbs for long-running work. A reference-only `src/dataraum/mcp/` module survives for the cockpit takeover — no transport, no in-tree consumer.

**Key design decisions:**
- **VARCHAR-first staging** — everything loads as VARCHAR; type inference happens in profiling, not load. Failed casts go to quarantine tables, never pipeline failure.
- **Pre-computed context** — AI receives a fully-assembled `ContextDocument`; no runtime discovery.
- **Ontologies are config** — domain ontologies (financial_reporting, marketing, …) are YAML mapping column patterns → business terms, defining metrics, guiding interpretation. They live in `packages/dataraum-config/` (bind-mounted at `/opt/dataraum/config`); load them only through `dataraum.core.config`, never `Path(__file__)` navigation.
- **Pipeline measures, doesn't interpret** — detectors run as pipeline post-steps; interpretation happens interactively through the kernel.
- **BBN readiness** — per-column ready / investigate / blocked via a Bayesian network.
- **Free-threading** — Python 3.14t with the GIL off; treat all shared mutable state as unsafe.

**Temporal (durable orchestration).** Skill: `npx skills add temporalio/skill-temporal-developer`. The engine runs a **Python activity worker** — each phase is an `@activity.defn(name="<phase>")` wrapper; workflows are authored in TypeScript (the cockpit orchestrates). Bridge sync SQLAlchemy / DuckDB through `asyncio.to_thread`. Determinism + retry rules live in the skill; the locked decision and constraints live in the `feedback-durable-execution-lean` memory.

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
├── server/      # Starlette kernel
└── mcp/         # reference-only (no transport)
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
curl -fsS http://localhost:8000/health
# or run the kernel directly (DUCKLAKE_* env required):
uv run uvicorn dataraum.server.app:app --port 8000
```

## Docs & tracking

- User docs: `docs/*.md`, published via Zensical (`uv run zensical serve`). Update when user-facing behavior changes; internal detail goes in docstrings.
- Design docs → **Confluence** (space DD). Active work → **Jira** (DAT-*; MCP available). Not local files.
