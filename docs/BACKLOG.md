# Backlog

Prioritized backlog for the dataraum-context project. Items are organized by priority and dependency.

**Related Documentation:**
- [ENTROPY_IMPLEMENTATION_PLAN.md](./ENTROPY_IMPLEMENTATION_PLAN.md) - Implementation roadmap
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

### Step 2.3: Contract Evaluation ⏸️ DEFERRED

> **Deferred**: Contract evaluation is about policy enforcement (what entropy levels are acceptable for different use cases). Deferring until we have real usage patterns to inform contract thresholds. The spec in [ENTROPY_CONTRACTS.md](./ENTROPY_CONTRACTS.md) remains valid but implementation will benefit from learnings.

- [ ] Create `entropy/contracts.py` per [ENTROPY_CONTRACTS.md](./ENTROPY_CONTRACTS.md)
- [ ] Create `config/entropy/contracts.yaml` with 5 standard profiles
- [ ] Implement `evaluate_contract()` function
- [ ] Implement `get_path_to_compliance()` function
- [ ] Add contract compliance to EntropyContext

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

**Step 2.5.1: Configuration Extraction**
- [ ] Create `config/entropy/thresholds.yaml` with all scoring parameters
- [ ] Extract detector multipliers (null_ratio * 2, outlier_ratio * 10, etc.)
- [ ] Extract composite score weights (structural, semantic, value, computational)
- [ ] Extract readiness thresholds (0.3, 0.6, 0.8)
- [ ] Update detectors to read from config

**Step 2.5.2: Code Cleanup**
- [ ] Remove `_get_default_assumption_for_dimension()` from agent.py
- [ ] Remove `_create_assumptions_from_entropy()` hardcoded logic
- [ ] Simplify `BusinessMeaningDetector` to collect raw data (remove char counting scores)
- [ ] Remove hardcoded impact descriptions from compound_risk.py
- [ ] Add `raw_metrics` field to detector output (separate from computed score)

**Step 2.5.3: LLM Entropy Interpretation Feature**
- [ ] Create `entropy/interpretation.py` with LLM feature
- [ ] Create prompt template `config/prompts/entropy_interpretation.yaml`
- [ ] Input: raw metrics, computed confidence, column/table context
- [ ] Output: assumptions, resolution_actions, explanation (structured JSON)
- [ ] Cache interpretations with column profile

**Step 2.5.4: Analysis-Time Baseline**
- [ ] Integrate LLM interpretation into `build_entropy_context()`
- [ ] Store cached interpretations in database
- [ ] Add `EntropyInterpretation` model with assumptions, actions, explanation

**Step 2.5.5: Query-Time Refinement**
- [ ] Create `refine_entropy_interpretation()` for query context
- [ ] Input: baseline interpretation + query + columns used + aggregation type
- [ ] Output: query-specific assumptions, caveats, SQL comments
- [ ] Integrate into GraphAgent execution flow

**Step 2.5.6: Dashboard Models**
- [ ] Add `explanation: str` field to entropy context models
- [ ] Add `resolution_actions: list[ResolutionAction]` with LLM-generated descriptions
- [ ] Create dashboard-friendly serialization format

---

## Priority 3: Phase 3 Cleanup and API

### Step 3.1: Quality Module Cleanup
- [ ] Verify no remaining usage of `quality/synthesis.py`
- [ ] Remove `quality/synthesis.py`
- [ ] Remove `quality/db_models.py`
- [ ] Remove `quality/models.py`
- [ ] Remove `quality/__init__.py`
- [ ] Update all imports throughout codebase
- [ ] Run full test suite

### Step 3.2: Topology Module Simplification
- [ ] Document which outputs feed entropy detection
- [ ] Keep: β₁ (Betti-1), stability metrics, connectivity
- [ ] Remove: Full persistence diagrams, β₂, complex homology
- [ ] Update tests for simplified interface

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

---

## Priority 4: Phase 4 Graph Agent Completion + UI

### Step 4.1: Field Mapping with Entropy
- [ ] Modify field mapping to prefer lower-entropy columns
- [ ] Generate warnings for high-entropy field resolutions
- [ ] Add entropy score to ColumnCandidate

### Step 4.2: Multi-Table Graph Execution
- [ ] Validate join entropy before execution
- [ ] Flag non-deterministic joins in generated SQL
- [ ] Track join assumptions in execution metadata

### Step 4.3: Graph Validation
- [ ] Add entropy validation to generated SQL
- [ ] Flag high-uncertainty calculations
- [ ] Include entropy context in GraphExecution results

### Step 4.4: UI Foundation
- [ ] Create `ui/` directory at project root
- [ ] Migrate web_visualizer from `prototypes/calculation-graphs/`
- [ ] Convert from npm to bun
- [ ] Create entropy dashboard component
- [ ] Create resolution workflow component
- [ ] Integrate with API endpoints

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
