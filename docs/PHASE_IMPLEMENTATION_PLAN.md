# Pipeline Phase Implementation Plan

## Overview

This document provides a detailed implementation plan for the 8 remaining pipeline phases plus 1 identified gap (temporal slice analysis).

## Current State

**Implemented Phases (10):**
1. `import` - Load CSV files into raw tables
2. `typing` - Type inference and resolution
3. `statistics` - Statistical profiling
4. `statistical_quality` - Benford's Law and outlier detection
5. `correlations` - Within-table correlation analysis
6. `temporal` - Temporal column analysis (on typed tables)
7. `relationships` - Cross-table relationship detection
8. `semantic` - LLM semantic enrichment
9. `slicing` - LLM-powered data slicing
10. `slice_analysis` - Analysis on slice tables

**Not Implemented (9):**
1. `topology` - TDA topological analysis (non-LLM)
2. `cross_table_quality` - Cross-table correlation analysis (LLM)
3. `quality_summary` - LLM quality report generation
4. `business_cycles` - Expert LLM cycle detection
5. `validation` - LLM-powered validation checks
6. `entropy` - Entropy detection across all dimensions (non-LLM)
7. `entropy_interpretation` - LLM interpretation of entropy
8. `context` - Build execution context for graph agent (non-LLM)
9. `temporal_slice_analysis` - Temporal + topology analysis on slices (non-LLM)

---

## Phase Implementation Details

### 1. TopologyPhase (Non-LLM)

**File:** `src/dataraum_context/pipeline/phases/topology_phase.py`

**Dependencies:** `["typing"]`
**Parallel Group:** `post_typing`

**Infrastructure:**
- Module: `src/dataraum_context/analysis/topology/`
- Main function: `analyze_topological_quality_multi_table()`
- DB Models: `TopologicalQualityMetrics`, `MultiTableTopologyMetrics`

**API Signature:**
```python
async def analyze_topological_quality_multi_table(
    table_ids: list[str],
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: AsyncSession,
    min_persistence: float = 0.1,
    stability_threshold: float = 0.1,
) -> Result[dict[str, Any]]
```

**Implementation Steps:**
1. Get all table_ids for the source
2. Call `analyze_topological_quality_multi_table()`
3. Results are auto-persisted to `multi_table_topology_metrics` table
4. Return success with analysis summary

**Outputs:** Betti numbers, persistence diagrams, cycles, stability analysis, anomalies

**Architecture Note:** The topology module is the single source of truth for TDA computation. Both slice and temporal analyses delegate to it:
- `run_topology_on_slices()` calls `analyze_topological_quality()` per slice
- `analyze_temporal_topology()` calls the same TDA per time period
- Full persistence diagrams and entropy everywhere (not correlation-based approximations)

---

### 2. EntropyPhase (Non-LLM)

**File:** `src/dataraum_context/pipeline/phases/entropy_phase.py`

**Dependencies:** `["statistics", "semantic", "relationships", "correlations"]`

**Infrastructure:**
- Module: `src/dataraum_context/entropy/`
- Main class: `EntropyProcessor`
- DB Models: `EntropyObjectRecord`, `CompoundRiskRecord`, `EntropySnapshotRecord`

**API Signatures:**
```python
# EntropyProcessor constructor
def __init__(
    self,
    registry: DetectorRegistry | None = None,
    config: ProcessorConfig | None = None,
) -> None

# Main methods
async def process_column(
    self,
    table_name: str,
    column_name: str,
    analysis_results: dict[str, Any],
    source_id: str | None = None,
    table_id: str | None = None,
    column_id: str | None = None,
) -> ColumnEntropyProfile

async def process_table(
    self,
    table_name: str,
    columns: list[dict[str, Any]],
    source_id: str | None = None,
    table_id: str | None = None,
) -> TableEntropyProfile

async def build_entropy_context(
    self,
    tables: list[TableEntropyProfile],
) -> EntropyContext
```

**Implementation Steps:**
1. Instantiate `EntropyProcessor()` with default config
2. For each table, build `columns` list with:
   - `"name"`: Column name
   - `"column_id"`: Column ID
   - `"analysis_results"`: Dict with keys like "typing", "statistics", "semantic"
3. Call `processor.process_table()` for each table
4. Call `processor.build_entropy_context()` with all table profiles
5. Persist `EntropyObjectRecord` and `CompoundRiskRecord` entries
6. Create `EntropySnapshotRecord` for the run

**Key:** The `analysis_results` dict must be populated from prior phase outputs:
- `"typing"`: TypeCandidate from typing phase
- `"statistics"`: ColumnProfile from statistics phase
- `"semantic"`: SemanticAnnotation from semantic phase
- `"correlations"`: Correlation data from correlations phase
- `"relationships"`: Relationship data from relationships phase

---

### 3. EntropyInterpretationPhase (LLM)

**File:** `src/dataraum_context/pipeline/phases/entropy_interpretation_phase.py`

**Dependencies:** `["entropy"]`
**Requires LLM:** Yes

**Infrastructure:**
- Module: `src/dataraum_context/entropy/interpretation.py`
- Main class: `EntropyInterpreter`
- Input: `InterpretationInput.from_profile()`

**API Signatures:**
```python
# Constructor
def __init__(
    self,
    config: LLMConfig,
    provider: LLMProvider,
    prompt_renderer: PromptRenderer,
    cache: LLMCache,
) -> None

# Main method
async def interpret_batch(
    self,
    session: AsyncSession,
    inputs: list[InterpretationInput],
    query: str | None = None,
) -> Result[dict[str, EntropyInterpretation]]

# Input creation
@classmethod
def from_profile(
    cls,
    profile: ColumnEntropyProfile,
    detected_type: str = "unknown",
    business_description: str | None = None,
    raw_metrics: dict[str, Any] | None = None,
) -> InterpretationInput
```

**Implementation Steps:**
1. Load LLM config, provider, renderer, cache from context
2. Instantiate `EntropyInterpreter(config, provider, renderer, cache)`
3. Query `ColumnEntropyProfile` records from entropy phase
4. For each profile, create `InterpretationInput.from_profile()`
5. Call `interpreter.interpret_batch(session, inputs)`
6. Store interpretations (assumptions, resolution actions)

**Fallback:** `create_fallback_interpretation()` when LLM unavailable

---

### 4. QualitySummaryPhase (LLM)

**File:** `src/dataraum_context/pipeline/phases/quality_summary_phase.py`

**Dependencies:** `["slice_analysis"]`
**Requires LLM:** Yes

**Infrastructure:**
- Module: `src/dataraum_context/analysis/quality_summary/`
- Agent: `QualitySummaryAgent`
- Processor: `summarize_quality()`
- DB Models: `ColumnQualityReport`, `QualitySummaryRun`

**API Signatures:**
```python
# Agent constructor
def __init__(
    self,
    config: LLMConfig,
    provider: LLMProvider,
    prompt_renderer: PromptRenderer,
    cache: LLMCache,
) -> None

# Main processor function
async def summarize_quality(
    session: AsyncSession,
    agent: QualitySummaryAgent,
    slice_definition: SliceDefinition,
    skip_existing: bool = True,
) -> Result[QualitySummaryResult]
```

**Implementation Steps:**
1. Load LLM infrastructure from context
2. Instantiate `QualitySummaryAgent(config, provider, renderer, cache)`
3. Query all `SliceDefinition` records for the source
4. For each slice definition, call `summarize_quality(session, agent, slice_def)`
5. Results auto-persist to `ColumnQualityReport` and `QualitySummaryRun`

---

### 5. ValidationPhase (LLM)

**File:** `src/dataraum_context/pipeline/phases/validation_phase.py`

**Dependencies:** `["semantic"]`
**Requires LLM:** Yes

**Infrastructure:**
- Module: `src/dataraum_context/analysis/validation/`
- Agent: `ValidationAgent`
- Config: `load_all_validation_specs()`
- DB Models: `ValidationRunRecord`, `ValidationResultRecord`

**API Signatures:**
```python
# Agent constructor
def __init__(
    self,
    config: LLMConfig,
    provider: LLMProvider,
    prompt_renderer: PromptRenderer,
    cache: LLMCache,
) -> None

# Main method
async def run_validations(
    self,
    session: AsyncSession,
    duckdb_conn: duckdb.DuckDBPyConnection,
    table_ids: list[str],
    validation_ids: list[str] | None = None,  # NOT validation_specs!
    category: str | None = None,
    persist: bool = True,
) -> Result[ValidationRunResult]

# Config loader
def load_all_validation_specs() -> dict[str, ValidationSpec]
```

**Implementation Steps:**
1. Load LLM infrastructure from context
2. Instantiate `ValidationAgent(config, provider, renderer, cache)`
3. Get all table_ids for the source
4. Call `agent.run_validations(session, duckdb_conn, table_ids, persist=True)`
5. Results auto-persist to database

**Note:** `validation_ids` parameter, not `validation_specs`

---

### 6. BusinessCyclesPhase (LLM)

**File:** `src/dataraum_context/pipeline/phases/business_cycles_phase.py`

**Dependencies:** `["semantic", "temporal"]`
**Requires LLM:** Yes

**Infrastructure:**
- Module: `src/dataraum_context/analysis/cycles/`
- Agent: `BusinessCycleAgent`
- DB Models: `BusinessCycleAnalysisRun`, `DetectedBusinessCycle`

**API Signatures:**
```python
# Constructor - ONLY takes provider!
def __init__(
    self,
    provider: LLMProvider,
    model: str | None = None,
) -> None

# Main method
async def analyze(
    self,
    session: AsyncSession,
    duckdb_conn: duckdb.DuckDBPyConnection,
    table_ids: list[str],
    max_tool_calls: int = 10,
    *,
    domain: str | None = None,  # keyword-only
) -> Result[BusinessCycleAnalysis]
```

**Implementation Steps:**
1. Get LLM provider from context
2. Instantiate `BusinessCycleAgent(provider=provider)`
3. Get all table_ids for the source
4. Call `agent.analyze(session, duckdb_conn, table_ids)`
5. Results auto-persist to database

**Note:** Constructor only takes `provider`, NOT `config`, `cache`, etc.

---

### 7. CrossTableQualityPhase (LLM)

**File:** `src/dataraum_context/pipeline/phases/cross_table_quality_phase.py`

**Dependencies:** `["semantic"]`
**Requires LLM:** Yes

**Infrastructure:**
- Module: `src/dataraum_context/analysis/correlation/cross_table/quality.py`
- Async wrapper: `analyze_cross_table_quality()` in `processor.py`
- DB Models: `cross_table_correlations`, `multicollinearity_groups`

**API Signatures:**
```python
# Sync function (low-level)
def analyze_relationship_quality(
    relationship: EnrichedRelationship,
    duckdb_conn: duckdb.DuckDBPyConnection,
    from_table_path: str,
    to_table_path: str,
    min_correlation: float = 0.3,
    redundancy_threshold: float = 0.99,
    max_sample_size: int = 50000,
) -> CrossTableQualityResult | None

# Async wrapper (use this)
async def analyze_cross_table_quality(
    relationship: Relationship,  # SQLAlchemy model
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: AsyncSession,
    min_correlation: float = 0.3,
    redundancy_threshold: float = 0.99,
) -> Result[CrossTableQualityResult]
```

**Implementation Steps:**
1. Query all `Relationship` records from semantic phase
2. For each relationship with `detection_method != 'candidate'`:
   - Call `analyze_cross_table_quality(relationship, duckdb_conn, session)`
3. Results auto-persist to database

**Note:** Relationship model uses `from_column_id`/`to_column_id`, NOT `source_column_id`/`target_column_id`

---

### 8. ContextPhase (Non-LLM)

**File:** `src/dataraum_context/pipeline/phases/context_phase.py`

**Dependencies:** `["entropy_interpretation", "quality_summary"]`

**Infrastructure:**
- Module: `src/dataraum_context/graphs/context.py`
- Main function: `build_execution_context()`
- Output: `GraphExecutionContext`

**API Signature:**
```python
async def build_execution_context(
    session: AsyncSession,
    table_ids: list[str],
    duckdb_conn: duckdb.DuckDBPyConnection | None = None,
    *,
    slice_column: str | None = None,
    slice_value: str | None = None,
) -> GraphExecutionContext
```

**Implementation Steps:**
1. Get all table_ids for the source
2. Call `build_execution_context(session, table_ids, duckdb_conn)`
3. Store context or make available for downstream use
4. Optionally call `format_context_for_prompt()` for debugging

**Note:** `GraphExecutionContext` has attributes:
- `tables`, `relationships`, `business_cycles`
- `field_mappings`, `entropy_summary`
- `available_slices`, `quality_issues_by_severity`
- NOT: `slices`, `has_entropy_data`, `high_entropy_columns`, `blocked_columns`

---

### 9. TemporalSliceAnalysisPhase (Non-LLM)

**File:** `src/dataraum_context/pipeline/phases/temporal_slice_analysis_phase.py`

**Dependencies:** `["slice_analysis", "temporal"]`

**Infrastructure:**
- Module: `src/dataraum_context/analysis/temporal_slicing/`
- Module: `src/dataraum_context/analysis/slicing/slice_runner.py`
- Analyzer: `TemporalSliceAnalyzer`
- DB Models: `TemporalSliceRun`, `TemporalSliceAnalysis`, `TemporalDriftAnalysis`, `SliceTimeMatrixEntry`, `TemporalTopologyAnalysis`

**API Signatures:**
```python
# Temporal analysis on slices
async def run_temporal_analysis_on_slices(
    session: AsyncSession,
    duckdb_conn: duckdb.DuckDBPyConnection,
    slice_definition: SliceDefinition,
    time_column: str,
    time_grain: TimeGrain = TimeGrain.DAILY,
    start_date: date | None = None,
    end_date: date | None = None,
) -> Result[list[TemporalAnalysisResult]]

# Topology on dimensional slices
async def run_topology_on_slices(
    session: AsyncSession,
    duckdb_conn: duckdb.DuckDBPyConnection,
    slice_definition: SliceDefinition,
    correlation_threshold: float = 0.5,
) -> TopologySlicesResult

# Temporal topology drift analysis
async def analyze_temporal_topology(
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: AsyncSession,
    table_name: str,
    time_column: str,
    time_grain: TimeGrain = TimeGrain.MONTHLY,
    correlation_threshold: float = 0.5,
) -> Result[TemporalTopologyResult]

# Configuration
class TemporalSliceConfig(BaseModel):
    time_column: str
    start_date: date | None = None
    end_date: date | None = None
    time_grain: TimeGrain = TimeGrain.DAILY
    completeness_threshold: float = 0.8
    drift_threshold: float = 0.1
    volume_z_threshold: float = 2.0
    analyze_columns: list[str] | None = None  # None = all
```

**Implementation Steps:**
1. Query `SliceDefinition` records for the source
2. Get temporal column from `run_config` or detect from `TemporalProfile` records
3. For each slice definition:
   a. Call `run_temporal_analysis_on_slices()` - temporal metrics per slice
   b. Call `run_topology_on_slices()` - topology comparison across dimensional slices
   c. For slices with sufficient data, call `analyze_temporal_topology()` - topology drift over time
4. Persist results to temporal analysis tables
5. Return summary of analyses performed

**Conditional Execution:**
- Skip if no temporal columns detected in source data
- Skip if no slice definitions exist
- Time column can be specified in `run_config["time_column"]` or auto-detected

**Outputs:**
- Period completeness metrics
- Distribution drift detection
- Cross-slice temporal comparison (slice × time matrix)
- Volume anomaly detection
- Per-slice topology (full TDA: Betti numbers, persistence diagrams, entropy)
- Cross-slice topology drift (via persistent entropy comparison)
- Temporal topology trends (increasing/decreasing/stable/volatile)
- Homological stability assessment per slice

**Architecture Note:** Uses the topology module as single source of truth - `run_topology_on_slices()` delegates to `analyze_topological_quality()` for each slice, providing full TDA capabilities rather than simplified correlation-based metrics.

---

## Implementation Order

Based on dependencies, implement in this order:

### Batch 1: Parallel with post-typing (no dependencies on new phases)
1. **TopologyPhase** - Non-LLM, simple wrapper

### Batch 2: Depends on existing phases only
2. **EntropyPhase** - Non-LLM, core infrastructure
3. **ValidationPhase** - LLM, depends on semantic
4. **BusinessCyclesPhase** - LLM, depends on semantic + temporal
5. **CrossTableQualityPhase** - LLM, depends on semantic

### Batch 3: Depends on slicing
6. **QualitySummaryPhase** - LLM, depends on slice_analysis
7. **TemporalSliceAnalysisPhase** - Non-LLM, depends on slice_analysis + temporal

### Batch 4: Depends on entropy
8. **EntropyInterpretationPhase** - LLM, depends on entropy

### Batch 5: Final aggregation
9. **ContextPhase** - Non-LLM, depends on entropy_interpretation + quality_summary

---

## File Structure

After implementation, the phases directory should contain:

```
src/dataraum_context/pipeline/phases/
├── __init__.py                    # Update exports
├── base.py                        # BasePhase class
├── import_phase.py                # ✓ Exists
├── typing_phase.py                # ✓ Exists
├── statistics_phase.py            # ✓ Exists
├── statistical_quality_phase.py   # ✓ Exists
├── correlations_phase.py          # ✓ Exists
├── temporal_phase.py              # ✓ Exists
├── relationships_phase.py         # ✓ Exists
├── semantic_phase.py              # ✓ Exists
├── slicing_phase.py               # ✓ Exists
├── slice_analysis_phase.py        # ✓ Exists
├── topology_phase.py              # NEW
├── entropy_phase.py               # NEW
├── entropy_interpretation_phase.py # NEW
├── quality_summary_phase.py       # NEW
├── validation_phase.py            # NEW
├── business_cycles_phase.py       # NEW
├── cross_table_quality_phase.py   # NEW
├── context_phase.py               # NEW
└── temporal_slice_analysis_phase.py # NEW
```

---

## Registration in Runner

After implementing phases, update `runner.py`:

```python
from dataraum_context.pipeline.phases import (
    # ... existing imports ...
    TopologyPhase,
    EntropyPhase,
    EntropyInterpretationPhase,
    QualitySummaryPhase,
    ValidationPhase,
    BusinessCyclesPhase,
    CrossTableQualityPhase,
    ContextPhase,
    TemporalSliceAnalysisPhase,
)

def create_pipeline(config: RunConfig) -> Pipeline:
    # ... existing setup ...

    # Register new phases
    pipeline.register(TopologyPhase())
    pipeline.register(EntropyPhase())
    pipeline.register(EntropyInterpretationPhase())
    pipeline.register(QualitySummaryPhase())
    pipeline.register(ValidationPhase())
    pipeline.register(BusinessCyclesPhase())
    pipeline.register(CrossTableQualityPhase())
    pipeline.register(ContextPhase())
    pipeline.register(TemporalSliceAnalysisPhase())

    return pipeline
```

---

## DAG Update Required

Add to `PIPELINE_DAG` in `src/dataraum_context/pipeline/base.py`:

```python
PhaseDefinition(
    name="temporal_slice_analysis",
    description="Temporal + topology analysis on slices",
    dependencies=["slice_analysis", "temporal"],
    outputs=["temporal_slice_profiles", "slice_topology"],
),
```

---

## Common Pattern for All Phases

Each phase follows this structure:

```python
class XxxPhase(BasePhase):
    @property
    def name(self) -> str:
        return "xxx"  # Must match PIPELINE_DAG name

    @property
    def description(self) -> str:
        return "Description from DAG"

    @property
    def dependencies(self) -> list[str]:
        return ["dep1", "dep2"]  # Must match DAG

    @property
    def outputs(self) -> list[str]:
        return ["output1"]  # Must match DAG

    @property
    def is_llm_phase(self) -> bool:
        return True  # or False

    async def _run(
        self,
        session: AsyncSession,
        duckdb_conn: duckdb.DuckDBPyConnection,
        source_id: str,
        run_config: dict[str, Any],
        context: PhaseContext,
    ) -> Result[dict[str, Any]]:
        # Implementation
        pass
```

For LLM phases, get infrastructure from context:
```python
llm_config = context.get("llm_config")
llm_provider = context.get("llm_provider")
prompt_renderer = context.get("prompt_renderer")
llm_cache = context.get("llm_cache")
```
