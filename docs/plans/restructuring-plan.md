# DataRaum Context Engine - Restructuring Plan

## Context

The project has grown organically through 4 priority phases to ~50 SQLAlchemy models, 21 pipeline phases, 692 tests, and 4 interfaces (CLI, TUI, API, MCP). The codebase needs consolidation: dead code removal, model streamlining, test improvement, configuration separation, and specification extraction — done module-by-module with a mini-plan agreed per module.

**End goal:** A clean, well-documented codebase ready for vertical-specific test data (`dataraum-testdata`) and final UX/agent tuning.

---

## Part 1: UX & Information Architecture

> Output: `docs/plans/information-architecture.md` — no code changes.

### 1.1 User Journeys

| Persona | Journey | Key Touchpoints | Data Needs |
|---------|---------|-----------------|------------|
| **Data Engineer** | Load data → inspect quality → fix typing → re-run phases → verify entropy → deploy | CLI `run`, `status`, `phases`, `reset`; TUI drill-down | Phase status, type decisions, quarantine counts, entropy scores, compound risks |
| **Data Analyst** | Browse tables → check quality → ask questions → save queries → build reports | TUI home/table/query; Web UI (future) | Schema, semantic annotations, quality grades, query results, slice profiles |
| **AI Agent** | Get context → evaluate readiness → run queries → track assumptions | MCP `get_context`, `evaluate_contract`, `query` | GraphExecutionContext, entropy summary, contract evaluation, query results |
| **Business User** | View dashboard → check KPIs → monitor quality trends → share reports | Web UI (future, via API) | Quality grades, metric values, entropy trends, contract traffic lights |
| **Jupyter User** | `Context("./output")` → explore → query → DataFrames | Python API (`Context` class) | Same as Data Analyst as Python objects/DataFrames |

### 1.2 Information Architecture Per Touchpoint

**CLI** — automation, summary output, CI/CD:
- Pipeline control: run, status, phases, reset
- Data summaries: tables, entropy, contracts, query
- `--json` for scripts, `--no-tui` for raw output
- Data: PhaseCheckpoint, PipelineRun, EntropySnapshotRecord

**TUI** — exploration, drill-down:
- HomeScreen → EntropyScreen → ContractsScreen → QueryScreen → TableScreen
- Data: All SQLAlchemy models via ConnectionManager, DuckDB for samples

**Web UI** (future, evolves from current API) — large dataset browsing, report sharing:
- HTMX/HATEOAS architecture (see `docs/ui/01-architecture-overview.md`)
- Better than TUI for: large data tables, visualizations, business user sharing
- Data: Same as TUI, served via FastAPI + Jinja2

**MCP** — LLM integration, high-level:
- 4 tools: get_context, get_entropy, evaluate_contract, query
- Data: Pre-assembled context document, entropy summary, contract evaluation

**Python API** — Jupyter, scripts:
- `Context` class: `.tables`, `.entropy`, `.contracts`, `.query()`
- Data: Same models as DataFrames/dataclasses

### 1.3 Data Lineage

```
CSV Input
  ↓ [import]       → DuckDB: raw_{table}; SQLAlchemy: Source, Table, Column
  ↓ [typing]       → DuckDB: typed_{table}, quarantine_{table}; SA: TypeCandidate, TypeDecision
  ↓ [statistics]   → SA: StatisticalProfile
  ↓ [eligibility]  → SA: ColumnEligibilityRecord (gate)
  ↓ [stat_quality, relationships, correlations] (parallel)
  │                 → SA: StatisticalQualityMetrics, Relationship, Correlation models
  ↓ [semantic]     → SA: SemanticAnnotation, TableEntity (LLM)
  ↓ [validation, slicing, cycles, cross_table_quality] (parallel)
  │                 → SA: ValidationResult, SliceDefinition, DetectedBusinessCycle
  │                 → DuckDB: slice_{table}_{value}
  ↓ [slice_analysis, temporal, temporal_slice_analysis]
  │                 → SA: SliceProfile, TemporalProfile, DriftAnalysis
  ↓ [quality_summary] → SA: ColumnQualityReport (LLM)
  ↓ [entropy]      → SA: EntropyObjectRecord, CompoundRiskRecord
  ↓ [entropy_interpretation] → SA: EntropyInterpretationRecord (LLM)
  ↓ [graph_execution] → SA: GraphExecutionRecord; DuckDB: metric tables (LLM)
  ↓
  GraphExecutionContext → MCP/API/CLI/TUI
```

### 1.4 Gaps & Decisions

| Gap | Decision |
|-----|----------|
| Context assembly runs ad-hoc, not as pipeline phase | Clean up existing ContextPhase. Evaluate if pipeline phase adds value vs on-demand assembly. |
| Query library not seeded from graph definitions | Streamline graph/query agents — **do last**, complex topic to discuss. |
| EntropySnapshotRecord has no trending phase | Remove trending for now. Re-imports + drift analysis when test data exists. |
| Vectors DB only created by query agent | Should be initialized by graph agent. Needs specific cleanup task when we get to graphs/query. |
| API exists despite stale backlog saying "removed" | **Keep.** API is the foundation for future Web UI. Move out to a branch if it gets in the way during cleanup, re-introduce after pipeline is clean. |

---

## Part 2: Infrastructure Setup

### 2.1 Branch Strategy

```
main (current, preserved as reference)
  └─ refactor/streamline (all work here)
       ├─ Module-by-module commits
       ├─ Verify per-module with `dataraum run --phase <name>`
       └─ Final: fix overarching concerns
```

### 2.2 Test Separation

**Structure:**
```
tests/
├── unit/                    # Fast, no DB/filesystem
│   ├── analysis/typing/
│   ├── analysis/statistics/
│   ├── entropy/
│   └── ...
├── integration/             # Needs DB, DuckDB, filesystem
│   ├── conftest.py          # PipelineTestHarness, small_finance
│   ├── phases/              # Per-phase integration tests
│   └── ...
└── conftest.py              # Shared base fixtures
```

**Migration rule:** Tests using `ConnectionManager`, `session_scope`, `duckdb_cursor`, `tmp_path` for pipeline output, or `PipelineTestHarness` → `integration/`. Everything else → `unit/`.

**Usage:**
```bash
uv run pytest tests/unit/ -v              # Fast (~30s), run often
uv run pytest tests/integration/ -v       # Slow (~5min), end of dev cycle
```

### 2.3 Configuration Separation

**Target:**
```
config/
├── system/                          # Engine behavior (same across verticals)
│   ├── null_values.yaml
│   ├── typing.yaml
│   ├── llm.yaml
│   ├── pipeline.yaml
│   ├── column_eligibility.yaml
│   ├── entropy/thresholds.yaml
│   ├── entropy/contracts.yaml
│   └── prompts/*.yaml
│
└── verticals/                       # Business-specific (per domain)
    └── finance/
        ├── ontology.yaml
        ├── validations/*.yaml
        ├── metrics/*.yaml
        ├── filters/*.yaml
        └── cycles.yaml
```

**Config loading:** `core/config.py` takes a config path argument, used everywhere. No fallbacks — fail fast and loudly if config missing.

---

## Part 3: Module-by-Module Cleanup

### Approach

**For each module, we will:**
1. Do a mini-plan together (discuss specifics, agree on scope)
2. Execute the agreed checklist
3. Verify with `dataraum run --phase <name>`
4. It's OK if downstream modules break — we'll fix them when we get there

### Per-Module Checklist (Execute in Order)

For each module, work through these steps sequentially:

| # | Step | Details |
|---|------|---------|
| 1 | **Analyse data model** | Understand SQLAlchemy models, relationships, fields. Identify unused models/fields. |
| 2 | **Remove dead code** | Unused functions, unreachable branches, stale comments, unused models/fields. Reduce surface area before anything else. |
| 3 | **Streamline configuration** | Migrate to central config loader (`get_config_file()` / `load_yaml_config()`). Extract hardcoded configs to YAML. Merge overlapping configs. Remove unused config keys. |
| 4 | **Remove fallbacks and defaults** | No silent fallbacks. Fail fast and loudly if config/data is missing. Remove defensive `or default` patterns — if something is required, require it. |
| 5 | **Streamline dependencies** | Remove unused imports, simplify internal coupling between submodules. (External package audit is Part 4.) |
| 6 | **Streamline logging** | Use `core/logging.py` consistently. No `print()`, no direct `logging.getLogger()`. Ensure log messages are useful, not noisy. Review log levels: debug for internals, info for operations, warning for recoverable issues, error for failures. |
| 7 | **Streamline tests** | Remove tests that don't add value. Prefer realistic data (small_finance fixtures). Move misplaced tests between unit/integration. Each test should test ONE behavior. |
| 8 | **Write spec documentation** | Create `docs/specs/<module>.md` following the template below. |

### Spec Template (for `docs/specs/<module>.md`)

Each spec follows this structure:
1. **Reasoning & Summary** — why this module exists, what problem it solves
2. **Data Model** — SQLAlchemy models, key fields, relationships
3. **Metrics** (if applicable) — what is measured, formulas, thresholds
4. **Configuration** (if applicable) — YAML structure, required fields
5. **Roadmap / Planned Features** — what's deferred, what's next

### Processing Order: Bottom-Up (source → output)

Each module is stable before its consumers are cleaned.

| # | Module | Pipeline Phase | Key Files |
|---|--------|---------------|-----------|
| 1 | `core/` + `storage/` | (infrastructure) | `core/config.py`, `core/connections.py`, `storage/models.py` |
| 2 | `sources/` | import | `sources/csv/` |
| 3 | `analysis/typing/` | typing | `typing/analyzer.py`, `typing/db_models.py` |
| 4 | `analysis/statistics/` | statistics | `statistics/profiler.py`, `statistics/db_models.py` |
| 5 | `analysis/eligibility/` | column_eligibility | `eligibility/evaluator.py` |
| 6 | `analysis/relationships/` | relationships | `relationships/detector.py` |
| 7 | `analysis/correlation/` | correlations | Multiple files, 7 models — streamline candidate |
| 8 | `analysis/semantic/` | semantic | `semantic/analyzer.py` |
| 9 | `analysis/temporal/` | temporal | `temporal/analyzer.py` |
| 10 | `analysis/validation/` | validation | `validation/agent.py` |
| 11 | `analysis/slicing/` | slicing | `slicing/analyzer.py` |
| 12 | `analysis/cycles/` | business_cycles | `cycles/agent.py` |
| 13 | `analysis/quality_summary/` | quality_summary | `quality_summary/analyzer.py` |
| 14 | `analysis/temporal_slicing/` | temporal_slice_analysis | `temporal_slicing/analyzer.py` (44K lines — red flag) |
| 15 | `analysis/topology/` | (consumed by slicing phases) | TDA integration |
| 16 | `entropy/` | entropy, entropy_interpretation | `detectors/`, `contracts.py`, `interpretation.py` |
| 17 | `llm/` | (cross-cutting) | `providers/`, `features/`, `service.py` |
| 18 | `pipeline/` | (orchestrator) | `runner.py` (30 prints), `base.py`, `phases/` |
| — | `graphs/` | ~~graph_execution~~ | **OUT OF SCOPE** — de-configure from pipeline, re-introduce later |
| — | `query/` | ~~(consumption)~~ | **OUT OF SCOPE** — re-introduce after modules + TUI are solid |

**Modules 18-19 (graphs/query):** **Moved out of scope.** De-configure graph_execution from the pipeline DAG for now. The graph and query agents are complex multi-agent topics that depend on the cleaned pipeline being stable. We'll re-introduce them after the modules + TUI are solid, with their own dedicated plan.

**Module 20 (pipeline):** Clean up the orchestrator itself (runner.py prints, phase registration) but skip graph_execution phase.

---

## Part 4: Overarching Cleanup (After All Modules)

### 4.1 Pipeline Architecture: Config-Driven Phases

> **Goal:** Phases become thin wrappers (or disappear entirely). Analysis modules own their execution logic. Pipeline is assembled from configuration.

**Current state:** Each phase is a Python class in `pipeline/phases/` that hardcodes dependencies, outputs, skip conditions, and config defaults in `base.py`. Adding a new phase means writing a class + updating the DAG in `base.py`.

**Target state:**
- Each phase has a config file: `config/phases/<phase_name>.yaml`
- Phase config declares: dependencies, outputs, skip conditions, concurrency group, analysis module entry point
- Pipeline DAG is assembled from `config/system/pipeline.yaml` (which phases to run, in what order)
- `pipeline/base.py` has no per-phase defaults or hardcoded dependencies
- Analysis modules expose an `execute(ctx) -> PhaseResult` entry point
- New phases can be added by: (1) writing an analysis module, (2) adding a config file

**Config structure (proposed):**
```
config/phases/
├── typing.yaml              # Dependencies, outputs, skip conditions
├── statistics.yaml
├── semantic.yaml
├── ...
config/system/
└── pipeline.yaml            # Which phases to run, order, concurrency settings
```

**Concurrency review:**
- Current: `max_parallel` in pipeline.yaml, phases declare dependencies but concurrency is implicit
- Review: How phases declare thread-safety, which phases can actually run in parallel
- Streamline: Make concurrency explicit per phase in config (e.g., `concurrency: parallel` vs `concurrency: serial`)

**Phase config location:** Configurable per phase — default convention `config/phases/<name>.yaml`, overridable in pipeline.yaml.

**Spec location:** `docs/specs/phases/<name>.md` — one spec per phase (currently mixed into module specs).

**Steps:**
1. Audit current phase classes — catalog what each hardcodes vs what comes from config
2. Design phase config schema (YAML structure)
3. Migrate phase-by-phase: extract hardcoded values to config files
4. Remove defaults from `base.py` and phase classes
5. Implement config-driven DAG assembly
6. Verify concurrency still works correctly

### 4.2 Config Audit

- Review all files in `config/system/` and `config/verticals/` for obsolete or illogical entries
- Check for config files that are loaded but never used
- Check for config keys that no code reads
- Verify config file naming conventions are consistent
- Remove stale config entries left over from deleted features

### 4.3 Test Cleanup

- Review all integration and unit tests for:
  - Spikes / experiments that shouldn't be permanent tests
  - `@pytest.mark.skip` tests — either fix or delete
  - Tests that duplicate other tests
  - Tests that test framework behavior rather than our code
  - Tests with no meaningful assertions
- Goal: every test justifies its existence and tests ONE behavior

### 4.4 Feature Extraction from Docs

- Scan existing documentation (`docs/`, `BACKLOG.md`, `PROGRESS.md`, old plans) for planned-but-not-implemented features
- Review each with user: keep / discard / defer
- Add kept items to the Roadmap section of the relevant `docs/specs/<module>.md`
- Remove feature references from stale docs to avoid confusion

### 4.5 CLI/TUI

- Verify all TUI screens work with cleaned models
- Remove references to deleted models/fields
- Test with `dataraum tui ./output`

### 4.6 API (Deferred Decision)

- Leave as-is during module cleanup
- After cleanup: evaluate state, clean up routers to match streamlined models
- This becomes the foundation for Web UI (HTMX/HATEOAS per `docs/ui/`)
- If it gets in the way during cleanup, move to a branch and re-introduce later

### 4.7 MCP

- Verify 4 tools work with cleaned context assembly
- Update formatters if model fields changed

### 4.8 Dependency Audit

| Dependency | Action |
|------------|--------|
| `pandas` vs `pyarrow` | Evaluate overlap, prefer PyArrow where possible |
| `pydantic-settings` | **Removed** (Settings class deleted, env var override kept via `os.environ`) |
| `ripser` + `persim` | Keep — topology is actively used |
| `ruptures` | Check if actually used in temporal analysis |
| `networkx` | Check usage scope — may be replaceable |
| `structlog` | **Done** — replaced with `core/logging.get_logger()` everywhere |

### 4.9 Documentation Retirement

- Retire stale docs (old backlog entries, completed plans)
- `docs/specs/` becomes the authoritative developer documentation
- Each spec has a Roadmap section for planned features (fed by 4.4)

---

## Part 5: Verification

### Per-Module
```bash
uv run pytest tests/unit/<module>/ -v                           # Unit tests pass
uv run dataraum run ./examples/data/ --phase <phase> --output /tmp/test   # Phase works
python -c "from dataraum.<module> import *"                     # No import errors
```

### End-to-End (After All Modules)
```bash
uv run pytest tests/unit/ -v              # All unit tests (<60s)
uv run pytest tests/integration/ -v       # All integration tests
uv run dataraum run ./examples/data/ --output /tmp/full_test    # Full pipeline
uv run dataraum status /tmp/full_test     # CLI works
uv run dataraum entropy /tmp/full_test    # TUI works
uv run dataraum-mcp                       # MCP tools register
```

### Success Criteria
- [ ] All unit tests pass (target: <60s)
- [ ] All integration tests pass
- [ ] Full pipeline completes on small_finance data
- [ ] TUI screens render correctly
- [ ] MCP tools return valid responses
- [ ] `docs/specs/` has one spec per module
- [ ] `config/system/` and `config/verticals/finance/` separated
- [ ] No print statements in source code
- [ ] No config fallbacks — fail fast everywhere
- [ ] Logger used consistently across all modules
- [ ] Pipeline phases driven by config, not hardcoded in base.py
- [ ] No skipped or spike tests remaining

---

## Execution Order Summary

```
1. Save this plan to docs/plans/restructuring-plan.md (persistent reference)
2. Write docs/plans/information-architecture.md (Part 1 output)
3. Create refactor/streamline branch
4. Infrastructure: test separation (directories, conftest, markers)
5. Infrastructure: config separation (system/ vs verticals/, config path argument)
6. Modules 1-18 (bottom-up, mini-plan per module agreed with user) ✅ DONE
   - De-configure graph_execution from pipeline DAG
   - Skip graphs/ and query/ modules
7. Overarching cleanup:
   a. Config audit (4.2) — understand current state
   b. Pipeline architecture (4.1) — config-driven phases, concurrency review
   c. Test cleanup (4.3) — remove spikes, skipped tests
   d. Feature extraction (4.4) — planned features → module roadmaps
   e. CLI/TUI (4.5) — verify with cleaned models
   f. MCP (4.7) — verify tools work
   g. Dependency audit (4.8)
   h. Documentation retirement (4.9)
8. End-to-end verification
9. LATER: Re-introduce graphs/ + query/ with dedicated plan
10. LATER: API cleanup + Web UI evolution
```
