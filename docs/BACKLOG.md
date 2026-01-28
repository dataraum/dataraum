# Backlog

Prioritized backlog for the dataraum-context project. Items are organized by priority and dependency.

**Related Documentation:**
- [plans/interface-strategy.md](./plans/interface-strategy.md) - Current plan: Agent testing, HTMX UI, MCP, Jupyter
- [plans/query-agent-architecture.md](./plans/query-agent-architecture.md) - RAG-based query reuse design
- [ENTROPY_IMPLEMENTATION_PLAN.md](./ENTROPY_IMPLEMENTATION_PLAN.md) - Entropy system architecture
- [ENTROPY_MODELS.md](./ENTROPY_MODELS.md) - Data model specifications
- [ENTROPY_CONTRACTS.md](./ENTROPY_CONTRACTS.md) - Data readiness thresholds
- [ENTROPY_QUERY_BEHAVIOR.md](./ENTROPY_QUERY_BEHAVIOR.md) - Agent response policies
- [PROGRESS.md](./PROGRESS.md) - Completed work log

---

## Priority 1: Phase 1 Foundation (Current Focus)

### Step 1.1: File Migrations (Do First) ✅ COMPLETED 2025-01-13
- [x] Move `quality/formatting/base.py` → `core/formatting/base.py`
- [x] Move `quality/formatting/config.py` → `core/formatting/config.py`
- [x] Create `core/formatting/__init__.py` with exports
- [x] Update all imports referencing moved files
- [x] Run tests to verify no breakage (144 tests passed)

### Step 1.2: Core Models and Storage ✅ COMPLETED 2025-01-13
- [x] Create `entropy/__init__.py` with public API
- [x] Create `entropy/models.py` per [ENTROPY_MODELS.md](./ENTROPY_MODELS.md)
  - [x] EntropyObject dataclass
  - [x] ResolutionOption dataclass
  - [x] LLMContext and HumanContext dataclasses
  - [x] ColumnEntropyProfile dataclass
  - [x] TableEntropyProfile dataclass
  - [x] CompoundRisk dataclass
  - [x] EntropyContext dataclass
  - [x] RelationshipEntropyProfile dataclass
  - [x] CompoundRiskDefinition dataclass
  - [x] ResolutionCascade dataclass
- [x] Create `entropy/db_models.py` with SQLAlchemy models
  - [x] EntropyObjectRecord
  - [x] CompoundRiskRecord
  - [x] EntropySnapshotRecord
- [ ] Create database migration for entropy tables (deferred - tables auto-create in dev)
- [x] Create `entropy/compound_risk.py` for dangerous combination detection
- [x] Create `entropy/resolution.py` for cascade tracking

### Step 1.3: Detector Infrastructure ✅ COMPLETED 2025-01-13
- [x] Create `entropy/detectors/__init__.py`
- [x] Create `entropy/detectors/base.py` with EntropyDetector ABC
- [x] Create `DetectorRegistry` for detector discovery
- [x] Create `entropy/processor.py` for running all detectors
- [x] Create test fixtures in `tests/entropy/conftest.py`
  - [x] sample_detector_context with typing/statistics/semantic results
  - [x] high_entropy_context with known high-entropy characteristics
  - [x] low_entropy_context with clean data
  - [x] sample_entropy_object, sample_column_profile, high_entropy_column_profile
- [x] Create comprehensive test suite (43 entropy tests total)

### Step 1.4: High-Priority Detectors ✅ COMPLETED 2025-01-13
- [x] `TypeFidelityDetector` (structural/types.py)
  - Source: `typing/TypeCandidate.parse_success_rate`
  - Formula: `entropy = 1.0 - parse_success_rate`
- [x] `NullRatioDetector` (value/null_semantics.py)
  - Source: `statistics/ColumnProfile.null_ratio`
  - Formula: `entropy = min(1.0, null_ratio * 2)`
- [x] `OutlierRateDetector` (value/outliers.py)
  - Source: `statistics/quality.iqr_outlier_ratio`
  - Formula: `entropy = min(1.0, outlier_ratio * 10)`
- [x] `BusinessMeaningDetector` (semantic/business_meaning.py)
  - Source: `semantic/SemanticAnnotation.business_description`
  - Formula: `entropy = 1.0 if empty, 0.7 if brief, 0.2 if substantial`
- [x] `DerivedValueDetector` (computational/derived_values.py)
  - Source: `correlation/DerivedColumn.formula, match_rate`
  - Formula: `entropy = 1.0 - match_rate` (or 1.0 if no formula)
- [x] `JoinPathDeterminismDetector` (structural/relations.py)
  - Source: `relationships` + graph_topology analysis
  - Formula: `entropy = 0.7 if multiple paths, 0.9 if no path, 0.1 if single path`
- [x] `register_builtin_detectors()` function for auto-registration
- [x] Comprehensive test coverage (97 entropy tests total)

### Step 1.5: Medium-Priority Detectors ⏸️ DEFERRED

> **Deferred**: Proceeding with end-to-end integration first. These detectors are valuable but we want to learn from initial integration before expanding detector coverage. Will revisit once we have clarity on which dimensions matter most in practice.

- [ ] `PatternConsistencyDetector` (value/patterns.py)
- [ ] `UnitDeclaredDetector` (semantic/units.py)
- [ ] `TemporalClarityDetector` (semantic/temporal.py)
- [ ] `RangeBoundsDetector` (value/ranges.py)
- [ ] `FreshnessDetector` (semantic/temporal.py) - uses existing `is_stale`

### Step 1.6: Compound Risk Detection ⏸️ DEFERRED (MVP in place)

> **Deferred**: Basic compound risk detection already exists in `compound_risk.py` with hardcoded defaults. YAML configuration and additional risk patterns deferred until we learn which combinations actually matter from real usage. Current MVP is sufficient for initial integration.

- [ ] Create `config/entropy/compound_risks.yaml` with risk definitions
- [ ] Implement detection for critical: Units + Aggregations
- [ ] Implement detection for high: Relations + Filters
- [ ] Implement detection for high: Nulls + Aggregations
- [ ] Implement detection for medium: Temporal + Ranges
- [ ] Create compound risk scoring with multipliers

### Step 1.7: Aggregation and Scoring ⏸️ DEFERRED (MVP in place)

> **Deferred**: Core aggregation and scoring already implemented in `processor.py` (layer averaging, weighted composite, readiness classification). Separate files and additional sophistication deferred until we understand real-world scoring needs. Current MVP is sufficient for initial integration.

- [ ] Create `entropy/aggregation.py`
  - [ ] Dimension-level aggregation
  - [ ] Column-level composite scoring (weighted average)
  - [ ] Table-level rollup (avg and max)
- [ ] Create `entropy/scoring.py`
  - [ ] Implement formula from plan (0.25 structural, 0.30 semantic, 0.30 value, 0.15 computational)
  - [ ] Apply compound risk multipliers
  - [ ] Generate readiness classification (ready/investigate/blocked)

---

## Priority 2: Phase 2 Context Integration

### Step 2.1: Entropy Context Builder ✅ COMPLETED 2025-01-13
- [x] Create `entropy/context.py` with `build_entropy_context()`
- [x] Add `entropy_scores` field to `graphs/context.py:ColumnContext`
- [x] Add `resolution_hints` field to `ColumnContext`
- [x] Add `table_entropy` field to `TableContext`
- [x] Add `readiness_for_use` field to `TableContext`
- [x] Add `relationship_entropy` field to `RelationshipContext`
- [x] Add `entropy_summary` field to `GraphExecutionContext`
- [x] Modify `build_execution_context()` to call entropy builder
- [x] Create tests for entropy context integration (102 entropy tests total)

### Step 2.2: Prompt Formatting ✅ COMPLETED 2025-01-13
- [x] Create `format_entropy_for_prompt()` function
- [x] Create entropy summary section template (DATA READINESS header)
- [x] Create dangerous combination warning template (DANGEROUS COMBINATIONS section)
- [x] Create high-entropy column listing (HIGH UNCERTAINTY COLUMNS section)
- [x] Create inline entropy indicators (⚠ and ⛔ symbols)
- [x] Integrate into `format_context_for_prompt()`
- [x] Create tests for prompt formatting (9 new tests)

### Step 2.3: Contract Evaluation ✅ COMPLETED 2025-01-23

- [x] Create `entropy/contracts.py` with full contract evaluation system
- [x] Create `config/entropy/contracts.yaml` with 5 standard profiles
- [x] Implement `evaluate_contract()` function with dimension thresholds
- [x] Implement `_calculate_confidence_level()` for traffic light model (GREEN/YELLOW/ORANGE/RED)
- [x] Add `dataraum contracts` CLI command
- [x] API endpoints for contracts (list, detail, evaluate, evaluate all)
- [x] 26 tests covering all contract functionality
- [x] Fail-fast pattern: no default contracts, fails if YAML missing

### Step 2.4: Graph Agent Enhancement ✅ COMPLETED 2025-01-13
- [x] Update `graphs/agent.py` to read entropy context
- [x] Add entropy warnings to SQL comments (`format_entropy_sql_comments()`)
- [x] Implement query-time behavior per [ENTROPY_QUERY_BEHAVIOR.md](./ENTROPY_QUERY_BEHAVIOR.md)
  - [x] `EntropyBehaviorConfig` with strict/balanced/lenient modes
  - [x] `determine_action()` based on entropy level and compound risks
  - [x] Dimension-specific thresholds for currency/relations
- [x] Track assumptions in execution metadata
  - [x] `GraphExecution.assumptions`, `max_entropy_score`, `entropy_warnings`
- [x] Create `QueryAssumption` model for assumption logging
  - [x] `AssumptionBasis` enum (system_default, inferred, user_specified)
  - [x] Factory method and promotion tracking
- [x] Create tests (34 new tests, 550 total)

### Step 2.5: LLM-Assisted Entropy Interpretation (NEW)

> **Rationale**: Phase 2.4 review revealed 44 hardcoded heuristics (arbitrary multipliers, thresholds, assumption text). Formula-based confidence is valuable for determinism, but semantic interpretation (assumptions, resolutions, explanations) should be LLM-generated.

**Architecture: Formula + LLM Hybrid**
- Layer 1: Deterministic metrics & confidence (formula-based, configurable)
- Layer 2: LLM interpretation (assumptions, resolutions, explanations)
- Layer 3: Structured output for Graph Agent and Dashboards

**Step 2.5.1: Configuration Extraction** ⏸️ DEFERRED
- [ ] Create `config/entropy/thresholds.yaml` with all scoring parameters
- [ ] Extract detector multipliers (null_ratio * 2, outlier_ratio * 10, etc.)
- [ ] Extract composite score weights (structural, semantic, value, computational)
- [ ] Extract readiness thresholds (0.3, 0.6, 0.8)
- [ ] Update detectors to read from config

> Deferred: Current hardcoded values work. Will extract to YAML when we need configurability.

**Step 2.5.2: Code Cleanup** ✅ PARTIALLY COMPLETED 2025-01-14
- [x] Removed dead `core/formatting/` module (unused after quality/ removal)
- [x] Removed `config/formatter_thresholds/` (unused configs)
- [x] Removed `sf-hamilton` dependency (never imported)
- [ ] Remaining: Extract hardcoded entropy thresholds (deferred with 2.5.1)

**Step 2.5.3: LLM Entropy Interpretation Feature** ✅ COMPLETED 2025-01-14
- [x] Created `entropy/interpretation.py` with `EntropyInterpreter` class
- [x] Created prompt templates (entropy_interpretation, entropy_query_interpretation)
- [x] Batch interpretation support (single LLM call for multiple columns)
- [x] `create_fallback_interpretation()` for when LLM is unavailable

**Step 2.5.4: Analysis-Time Baseline** ✅ COMPLETED 2025-01-14
- [x] Integrated interpretation into `build_entropy_context()`
- [x] `InterpretationInput.from_profile()` factory method
- [x] Interpretation stored in `ColumnEntropyProfile.interpretation`

**Step 2.5.5: Query-Time Refinement** ✅ COMPLETED 2025-01-14
- [x] `interpret_batch()` accepts query context for query-specific interpretation
- [x] `_create_assumptions_from_entropy()` in GraphAgent uses interpretations
- [x] Assumptions flow into `GraphExecution.assumptions`

**Step 2.5.6: Dashboard Models** ✅ COMPLETED 2025-01-14
- [x] `EntropyInterpretation.to_dashboard_dict()` method
- [x] `EntropyContext.to_dashboard_dict()` method
- [x] Dashboard-friendly JSON with column_key, explanation, actions

---

## Priority 3: Phase 3 Cleanup, API, and Pipeline

### Step 3.1: Quality Module Cleanup ✅ COMPLETED 2025-01-14
- [x] Removed `quality/db_models.py` (unused QualityRule/QualityResult)
- [x] Removed entire `quality/` module (deprecated, replaced by graphs/)
- [x] Removed `tests/quality/` (166 tests of deprecated code)
- [x] Updated imports (scripts/infra.py, test_topological_quality.py)
- [x] Kept `core/formatting/` (reusable threshold/config infrastructure)

### Step 3.2: Topology Module Simplification ✅ COMPLETED 2025-01-15
- [x] Simplified topology API to focus on slice-based analysis
- [x] Kept: β₁ (Betti-1), stability metrics, bottleneck distance for temporal comparison
- [x] Topology now integrated into SliceAnalysisPhase and TemporalSliceAnalysisPhase
- [x] Removed standalone run_phase10_topology.py (TDA only useful in comparison context)
- [x] Full persistence diagrams retained for temporal stability analysis

**Decision:** Standalone TDA on single tables has limited value. TDA becomes meaningful when:
1. Comparing topology across slices (detecting structural changes)
2. Temporal stability analysis (bottleneck distance between time periods)

The topology module is now consumed by slice analysis phases, not as a standalone pipeline phase.

### Step 3.3: API Endpoints
- [ ] Create `api/routes/entropy.py`
- [ ] Implement `GET /entropy/table/{table_id}`
- [ ] Implement `GET /entropy/column/{table_id}/{column_name}`
- [ ] Implement `GET /entropy/contracts`
- [ ] Implement `GET /entropy/contracts/{contract_name}/evaluate`
- [ ] Implement `GET /entropy/resolution-hints`

### Step 3.4: MCP Server
- [ ] Create MCP tool: `get_entropy_context`
- [ ] Create MCP tool: `get_resolution_hints`
- [ ] Create MCP tool: `accept_assumption`
- [ ] Create MCP resource: `entropy://table/{table_name}`
- [ ] Create MCP resource: `entropy://column/{table}.{column}`
- [ ] Create MCP resource: `entropy://contract/{use_case}`

### Step 3.5: Pipeline Orchestrator ✅ COMPLETED 2025-01-16

> **Rationale**: Replace 17 ad-hoc scripts with a testable, parallel DAG orchestrator.
> See [ORCHESTRATOR_PLAN.md](./ORCHESTRATOR_PLAN.md) for full design.

**Step 3.5.1: Core Infrastructure** ✅ COMPLETED
- [x] Create `pipeline/` module structure
- [x] Define `BasePhase` ABC and `PhaseContext` dataclass
- [x] Create `PipelineCheckpoint` SQLAlchemy model
- [x] Create `PipelineRun` model for tracking executions
- [x] Create `PhaseResult` and `PhaseStatus` for phase outputs

**Step 3.5.2: Orchestrator** ✅ COMPLETED
- [x] Build `PipelineRunner` class with dependency resolution
- [x] Implement phase execution with skip detection
- [x] Implement checkpoint-based resume via `should_skip()`
- [x] Create `PIPELINE_DAG` with phase definitions

**Step 3.5.3: Phase Migration** ✅ COMPLETED
- [x] Migrate all 15 phase scripts to pipeline phases (18 total phases)
- [x] Created comprehensive test suite (47 new tests, 546 total)
- [ ] Delete old scripts after verification (pending - see below)

**Migrated Phases:**
| Script | Phase | Tests |
|--------|-------|-------|
| `run_phase1_import.py` | `ImportPhase` | ✅ |
| `run_phase2_typing.py` | `TypingPhase` | ✅ |
| `run_phase3_statistics.py` | `StatisticsPhase` | ✅ |
| `run_phase3b_statistical_quality.py` | `StatisticalQualityPhase` | ✅ |
| `run_phase4_relationships.py` | `RelationshipsPhase` | ✅ |
| `run_phase4b_correlations.py` | `CorrelationsPhase` | ✅ |
| `run_phase5_semantic.py` | `SemanticPhase` | ✅ |
| `run_phase6_correlation.py` | `CrossTableQualityPhase` | ✅ |
| `run_phase7_slicing.py` | `SlicingPhase` | ✅ |
| `run_phase7_temporal.py` | `TemporalPhase` | ✅ |
| `run_phase8_slice_analysis.py` | `SliceAnalysisPhase` | ✅ |
| `run_phase9_quality_summary.py` | `QualitySummaryPhase` | ✅ |
| `run_phase11_business_cycles.py` | `BusinessCyclesPhase` | ✅ |
| `run_phase12_validation.py` | `ValidationPhase` | ✅ |
| (new) | `EntropyPhase` | ✅ |
| (new) | `EntropyInterpretationPhase` | ✅ |
| (new) | `TemporalSliceAnalysisPhase` | ✅ |
| (new) | `ContextPhase` | ✅ |

**NOT Migrated (requires decision):**
- `run_phase10_topology.py` - TDA analysis (see Step 3.2)
- `run_graph_agent.py` - Interactive agent (not a pipeline phase)
- `run_subsets_pipeline.py` - Old utility (likely obsolete)
- `infra.py` - Test infrastructure utilities

**Step 3.5.4: CLI** ✅ COMPLETED 2025-01-16
- [x] Created `dataraum` CLI command via Typer
- [x] `dataraum run` - Run pipeline with phase selection, skip-llm, output dir
- [x] `dataraum status` - Show pipeline status, tables, phase history
- [x] `dataraum inspect` - Show graphs, filter coverage, execution context
- [x] `dataraum phases` - List all phases with dependencies
- [x] Created `docs/CLI.md` documentation

**Step 3.5.5: Pipeline API** ✅ COMPLETED 2026-01-22
- [x] Created `api/routes/pipeline.py` endpoints
- [x] SSE progress streaming
- [x] 31 API tests

---

## Priority 4: Agent Validation & Interfaces (Current Focus)

> **Plan:** [plans/interface-strategy.md](./plans/interface-strategy.md)
> **Previous plan archived:** [archive/ui-api-consolidation.md](./archive/ui-api-consolidation.md)

### Phase 0: Agent Validation with BookSQL

**Step 0.1: Graph Agent Integration Tests**
- [ ] Verify `build_execution_context()` populates entropy from real BookSQL data
- [ ] Generate SQL via graph agent, verify valid DuckDB syntax
- [ ] Test entropy behavior modes (strict/balanced/lenient) against real entropy
- [ ] Verify assumption tracking in execution metadata
- [ ] Verify entropy SQL comments

**Step 0.2: Query Agent Integration Tests**
- [ ] End-to-end: `answer_question()` against BookSQL, verify plausible results
- [ ] Entropy-aware query: verify assumptions for ambiguous columns
- [ ] Contract evaluation with different contracts (GREEN vs RED)
- [ ] Auto-contract selection
- [ ] Query library save/search/reuse cycle

**Step 0.3: Embedding and Library Tests**
- [ ] Embedding generation from QueryDocument
- [ ] Vector search: save queries, search, verify ranking
- [ ] Library persistence and retrieval
- [ ] Usage tracking

**Step 0.4: Contract Evaluation Against Real Data**
- [ ] Evaluate all 5 contract profiles against BookSQL entropy
- [ ] Verify traffic light classifications are sensible
- [ ] Verify resolution hints are actionable

**Step 0.5: Codify Integration Test Suite**
- [ ] `tests/integration/conftest.py` with BookSQL fixtures
- [ ] Skip gracefully when data absent
- [ ] Mock LLM provider for deterministic tests

### Phase 1: HTMX UI Foundation
- [ ] Add Jinja2 + static files to FastAPI app
- [ ] Content negotiation on existing routes (JSON or HTML)
- [ ] Base template with daisyUI layout
- [ ] Conversation interface with SSE streaming
- [ ] `<arrow-table>` web component (regular-table + Arrow JS)
- [ ] Entropy dashboard (Vega-Lite radar + issues table)
- [ ] Query agent UI (input, streaming response, results)

### Phase 2a: MCP Server
- [ ] Create `mcp/server.py` with tool registration
- [ ] Implement `get_context` tool (wraps `build_execution_context`)
- [ ] Implement `query` tool (wraps `answer_question`)
- [ ] Implement `get_metrics` tool (graph listing + entropy)
- [ ] Implement `annotate` tool (semantic annotation update)
- [ ] Create `mcp/formatters.py` for LLM-optimized output
- [ ] Claude Desktop configuration docs

### Phase 2b: Jupyter / Python API
- [ ] Create `notebook/` module with `Context` class
- [ ] Entropy accessor: `ctx.entropy.summary()`, `.table()`, `.column()`
- [ ] Contracts accessor: `ctx.contracts.evaluate()`, `.evaluate_all()`
- [ ] Query accessor: `ctx.query("...")` returning pandas DataFrame
- [ ] Library accessor: `ctx.library.search()`, `.list()`
- [ ] IPython `_repr_html_` methods for rich display

### Phase 3: UI Completion
- [ ] Schema explorer (Cytoscape.js)
- [ ] Pipeline monitor (SSE)
- [ ] Context panel (sidebar)
- [ ] SQL editor (CodeMirror 6)
- [ ] Mutations + undo system

### Graph Agent Enhancements (Parallel)

**Step 4.1: Field Mapping with Entropy**
- [ ] Modify field mapping to prefer lower-entropy columns
- [ ] Generate warnings for high-entropy field resolutions
- [ ] Add entropy score to ColumnCandidate

**Step 4.2: Multi-Table Graph Execution**
- [ ] Validate join entropy before execution
- [ ] Flag non-deterministic joins in generated SQL
- [ ] Track join assumptions in execution metadata

**Step 4.3: Graph Validation**
- [ ] Add entropy validation to generated SQL
- [ ] Flag high-uncertainty calculations
- [ ] Include entropy context in GraphExecution results

---

## Priority 5: Semantic Agent Extension

Extend semantic agent to enrich entropy-related fields during analysis.

- [ ] Create `config/prompts/entropy_enrichment.yaml`
- [ ] Add `naming_clarity_score` field to semantic output
- [ ] Add `scale_indicator` field (raw, thousands, millions, percent)
- [ ] Add `accumulation_type` field (period, ytd, all_time)
- [ ] Add `suggested_aggregation` field (sum, avg, count, min, max, none)
- [ ] Add `null_interpretation` field (not_applicable, unknown, not_yet_set)
- [ ] Modify semantic agent to use enrichment prompt

---

## Blocked/Waiting

Items that depend on external work or decisions.

- [ ] Source.Physical entropy (waiting for encoding detection infrastructure)
- [ ] Source.Provenance entropy (waiting for lineage tracking infrastructure)
- [ ] Source.AccessRights entropy (waiting for RBAC infrastructure)
- [ ] Query entropy layer (waiting for query agent implementation)
- [ ] Schema stability tracking (waiting for schema versioning)
- [ ] Entropy history/trending (waiting for snapshot infrastructure)

---

## Evaluation Notes

Items to evaluate during implementation:

- [ ] **Cycles as Context**: Evaluate if BusinessCycleAgent output (detected cycles, entity flows,
  business processes) should feed into context assembly. Could help LLM understand data semantics
  better when generating queries or interpreting results. Cycles depend on semantic phase.

---

## Infrastructure: Concurrency Design ✅ ADDRESSED 2025-01-16

> **Solution Implemented**: Created `core/connections.py` with thread-safe ConnectionManager.

### Implementation

**ConnectionManager** (`src/dataraum_context/core/connections.py`):
- Thread-safe connection management for SQLAlchemy + DuckDB
- Exported from `core/__init__.py`

**SQLAlchemy Sessions:**
- `session_scope()` - async context manager for pooled sessions
- `get_session()` - manual session management
- Connection pooling with configurable pool_size, max_overflow
- SQLite WAL mode for concurrent reads with writes
- Busy timeout for lock contention

**DuckDB Concurrency:**
- `duckdb_cursor()` - thread-safe read access via cursor()
- `duckdb_write()` - serialized writes via mutex (threading.Lock)
- Single connection, cursor() for reads is concurrent-safe

**Design Decision:** Session-per-phase with mutex for DuckDB writes (Option A + C hybrid)
- Simple, predictable behavior
- No external dependencies
- Sufficient for current data sizes
- Can add parallelism later if benchmarks show benefit

---

## Infrastructure: CLI Improvements ✅ PARTIALLY COMPLETED 2025-01-16

> **Goal**: Better developer experience and operational visibility.

### Completed

**CLI Module** (`src/dataraum_context/cli.py`):
- [x] `dataraum run` with `--phase`, `--skip-llm`, `--output`, `--quiet` options
- [x] `dataraum status` shows tables, columns, phase history with durations
- [x] `dataraum inspect` shows graphs, filter coverage, execution context
- [x] `dataraum phases` lists all phases with dependencies and LLM requirements
- [x] Rich tables for formatted output
- [x] Documentation at `docs/CLI.md`

**Phase Selection:**
- [x] `--phase typing` to run up to a specific phase
- [x] `--skip-llm` to exclude LLM phases

**Output Verbosity:**
- [x] `--quiet` for minimal output
- [x] Default: progress summary per phase

**Status Command:**
- [x] `dataraum status` shows current state
- [x] Lists completed phases with durations
- [x] Shows table counts by layer

### Remaining
- [ ] Real-time progress with elapsed time (live updates)
- [ ] `--from-phase` to start from a specific phase
- [ ] `--force-restart` flag to ignore checkpoints
- [ ] Remaining work estimation

---

## Technical Debt

Items that should be addressed when time permits.

- [ ] Review and potentially migrate remaining `quality/formatting/*.py` files
- [ ] Add property-based tests for entropy score normalization (always 0-1)
- [ ] Add integration tests for entropy + graph agent
- [ ] Create benchmark for entropy computation performance
- [ ] Document detector development guide

---

## Notes

- Items with `[ ]` are not started
- Items with `[~]` are in progress
- Items with `[x]` are completed (move to PROGRESS.md)
- Dependencies are indicated by item ordering within each priority
- Phase 1 must complete before Phase 2 can begin
- Steps within a phase can often be parallelized

---

## Quick Reference

| Phase | Goal | Key Deliverables |
|-------|------|------------------|
| 1 | Foundation | Models, detectors, compound risks, scoring |
| 2 | Integration | Context builder, prompt formatting, contracts |
| 3 | API | Cleanup, endpoints, MCP server |
| 4 | Completion | Field mapping, multi-table, UI |
