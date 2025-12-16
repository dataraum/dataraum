# Codebase Restructuring Plan

## Overview

This document captures the plan to restructure the dataraum-context codebase into smaller, self-contained modules with clear functional scope. The goal is to untangle complex logic while preserving analytical depth needed for LLM context generation.

**Key Principles**:
- Each module is self-contained with db_models, models, processor, formatter, tests
- Configuration-driven dataflows
- Based on current code only - remove unnecessary, do NOT add new functionality
- Verify each phase with actual data through working pipeline

---

## Target Structure

```
src/dataraum_context/

core/
├── __init__.py
├── models.py                    # Result, DataType, refs, enums
├── config.py                    # Settings
└── storage/
    ├── __init__.py
    ├── base.py                  # SQLAlchemy engine
    └── models.py                # Source, Table, Column

sources/
├── __init__.py
├── base.py                      # LoaderBase, TypeSystemStrength, ColumnInfo
├── csv/
│   ├── __init__.py
│   ├── loader.py
│   ├── null_values.py
│   └── tests/
└── parquet/                     # (future)
└── sqlite/                      # (future)

analysis/
├── typing/
│   ├── __init__.py
│   ├── db_models.py             # TypeCandidate, TypeDecision
│   ├── models.py
│   ├── patterns.py              # Value pattern config (from YAML)
│   ├── units.py                 # Pint unit detection
│   ├── inference.py             # Pattern + TRY_CAST on VALUES only
│   ├── resolution.py            # Create typed table, quarantine
│   └── tests/
│
├── statistics/
│   ├── __init__.py
│   ├── db_models.py             # StatisticalProfile
│   ├── models.py
│   ├── processor.py             # All stats computation
│   ├── formatter.py             # For LLM context
│   └── tests/
│
├── correlation/
│   ├── __init__.py
│   ├── db_models.py             # ColumnCorrelation, FunctionalDependency, etc.
│   ├── models.py
│   ├── numeric.py               # Pearson + Spearman
│   ├── categorical.py           # Cramér's V
│   ├── functional_dependency.py # A → B detection
│   ├── derived_columns.py       # Arithmetic detection
│   ├── multicollinearity.py     # VIF, condition index
│   ├── processor.py             # Orchestrates all correlation types
│   ├── formatter.py
│   └── tests/
│
├── semantic/
│   ├── __init__.py
│   ├── db_models.py             # SemanticAnnotation, TableEntity
│   ├── models.py
│   ├── agent.py                 # LLM analysis
│   ├── ontology.py              # YAML loading
│   ├── formatter.py
│   └── tests/
│
├── relationships/
│   ├── __init__.py
│   ├── db_models.py             # Relationship
│   ├── models.py
│   ├── fk_detection.py          # FK candidates
│   ├── cardinality.py           # 1:1, 1:n, n:m
│   ├── processor.py
│   ├── formatter.py
│   └── tests/
│
├── temporal/
│   ├── __init__.py
│   ├── db_models.py             # TemporalQualityMetrics (consolidated)
│   ├── models.py
│   ├── detection.py             # Find time columns, granularity
│   ├── completeness.py          # Gaps, coverage
│   ├── patterns.py              # Seasonality, trends
│   ├── freshness.py             # Staleness
│   ├── processor.py
│   ├── formatter.py
│   └── tests/
│
└── topological/
    ├── __init__.py
    ├── db_models.py
    ├── models.py
    ├── tda.py                   # Betti numbers, persistence
    ├── cycles.py                # Cycle detection
    ├── processor.py
    ├── formatter.py
    └── tests/

quality/
├── __init__.py
├── statistical/
│   ├── __init__.py
│   ├── db_models.py
│   ├── models.py
│   ├── benford.py
│   ├── outliers.py
│   ├── processor.py
│   ├── formatter.py
│   └── tests/
│
├── domains/
│   └── financial/
│       ├── __init__.py
│       ├── db_models.py
│       ├── models.py
│       ├── processor.py
│       ├── formatter.py
│       ├── llm.py
│       └── tests/
│
└── synthesis/
    ├── __init__.py
    ├── aggregator.py
    ├── models.py
    └── tests/

context/
├── __init__.py
├── assembly.py                  # Combine all formatters → rich context
├── models.py                    # DatasetQualityContext, etc.
└── tests/

llm/                             # Keep as-is (already clean)

graphs/                          # Keep as-is (already clean)

pipeline/
├── __init__.py
├── config.py                    # PipelineConfig, StepConfig
├── registry.py                  # Step registration + discovery
├── runner.py                    # Execute pipeline from YAML config
└── tests/
```

---

## Migration Phases

### Dependency Order

```
Phase 1: core/ + sources/csv
    ↓
Phase 2: analysis/typing
    ↓
Phase 3: analysis/statistics
    ↓
Phase 4: analysis/correlation
    ↓
Phase 5: analysis/semantic
    ↓
Phase 6: analysis/relationships
    ↓
Phase 7: analysis/temporal (consolidate enrichment + quality)
    ↓
Phase 8: analysis/topological
    ↓
Phase 9: quality/*
    ↓
Phase 10: context/ + pipeline/
```

### Planning Stage Template

Before migrating each module:

1. **Source files** - which current files feed into this module
2. **What stays** - code that serves the goal (LLM context for SQL generation)
3. **What goes** - unnecessary code to remove
4. **Module structure** - exact files and their responsibilities
5. **Interface** - what the module exports (public functions)
6. **Dependencies** - what this module needs from others
7. **Test data** - what CSV/data to verify with
8. **Verification** - how we know the migration worked

---

## Phase 1: core/ + sources/csv

### Goal
Get CSV → raw table working in the new structure. This is the foundation.

### Source Files (current → new)

| Current Location | New Location | Notes |
|-----------------|--------------|-------|
| `core/config.py` | `core/config.py` | Keep as-is |
| `core/models/base.py` | `core/models.py` | Flatten |
| `core/models/__init__.py` | Remove | No longer needed |
| `storage/base.py` | `core/storage/base.py` | Move under core |
| `storage/models.py` | `core/storage/models.py` | Move under core |
| `staging/base.py` | `sources/base.py` | Rename staging→sources |
| `staging/loaders/csv.py` | `sources/csv/loader.py` | |
| `staging/null_values.py` | `sources/csv/null_values.py` | |

### What Stays
- `Result[T]` monad
- `DataType` enum
- `ColumnRef`, `TableRef`
- `SourceConfig`
- All enums (SemanticRole, RelationshipType, Cardinality, etc.)
- SQLAlchemy engine setup
- Source, Table, Column models
- `LoaderBase` ABC
- `TypeSystemStrength` enum
- `CSVLoader` implementation
- Null value handling

### What Goes
- Nothing in Phase 1 - this is pure reorganization

### New Structure
```
core/
├── __init__.py
├── models.py           # Result, DataType, enums, refs (from core/models/base.py)
├── config.py           # Settings (unchanged)
└── storage/
    ├── __init__.py
    ├── base.py         # SQLAlchemy engine (from storage/base.py)
    └── models.py       # Source, Table, Column (from storage/models.py)

sources/
├── __init__.py
├── base.py             # LoaderBase, TypeSystemStrength, ColumnInfo (from staging/base.py)
└── csv/
    ├── __init__.py
    ├── loader.py       # CSVLoader (from staging/loaders/csv.py)
    ├── null_values.py  # (from staging/null_values.py)
    └── tests/
        └── test_csv_loader.py
```

### Interface

**core/__init__.py**:
```python
from core.models import Result, DataType, ColumnRef, TableRef, SourceConfig
from core.config import Settings, get_settings
```

**core/storage/__init__.py**:
```python
from core.storage.base import init_database, get_session, get_engine
from core.storage.models import Source, Table, Column
```

**sources/__init__.py**:
```python
from sources.base import LoaderBase, TypeSystemStrength, ColumnInfo
```

**sources/csv/__init__.py**:
```python
from sources.csv.loader import CSVLoader
```

### Dependencies
- `core/` depends on: nothing (foundation)
- `sources/` depends on: `core/`

### Test Data
- Use existing test CSVs in `tests/` or `examples/data/`
- Simple CSV with various column types

### Verification
1. Import works: `from core import Result, DataType`
2. Import works: `from sources.csv import CSVLoader`
3. Load CSV creates Source, Table, Column records in DB
4. DuckDB raw table exists with VARCHAR columns
5. Existing tests pass (relocated to `sources/csv/tests/`)

### Migration Steps
1. Create new directory structure
2. Copy files to new locations
3. Update imports in copied files
4. Create `__init__.py` files with public exports
5. Relocate relevant tests
6. Verify with actual CSV load
7. Remove old files once verified

---

## Phase 2: analysis/typing

### Goal
Type inference and resolution: raw VARCHAR → typed table + quarantine

### Source Files (current → new)

| Current Location | New Location | Notes |
|-----------------|--------------|-------|
| `profiling/type_inference.py` | `analysis/typing/inference.py` | Remove column name matching |
| `profiling/type_resolution.py` | `analysis/typing/resolution.py` | Keep as-is |
| `profiling/patterns.py` | `analysis/typing/patterns.py` | Keep as-is |
| `profiling/units.py` | `analysis/typing/units.py` | Keep as-is |
| `profiling/db_models.py` (partial) | `analysis/typing/db_models.py` | Only TypeCandidate, TypeDecision |
| `profiling/models.py` (partial) | `analysis/typing/models.py` | Only type-related models |

### What Stays
- Pattern matching on VALUES (regex patterns from YAML)
- TRY_CAST testing to determine parse success rate
- Pint unit detection on values
- Type candidate generation with confidence
- Type resolution with configurable threshold
- Quarantine pattern for failed casts
- TypeCandidate, TypeDecision db_models

### What Goes
- **Column name pattern matching** (type_inference.py lines 157-268):
  - `column_name_hints = pattern_config.match_column_name(col_name)`
  - Strategy 2 fallback to column name hints
  - Boosting pattern matches based on column name
- `match_column_name()` method in patterns.py
- `ColumnNamePattern` model if exists

### New Structure
```
analysis/typing/
├── __init__.py
├── db_models.py        # TypeCandidate, TypeDecision
├── models.py           # TypeCandidateModel, ParseResult, etc.
├── patterns.py         # PatternConfig, load_pattern_config (VALUE patterns only)
├── units.py            # detect_unit (Pint integration)
├── inference.py        # infer_type_candidates (no column name matching)
├── resolution.py       # resolve_types, create typed/quarantine tables
└── tests/
    ├── test_inference.py
    ├── test_resolution.py
    ├── test_patterns.py
    └── test_units.py
```

### Interface

**analysis/typing/__init__.py**:
```python
from analysis.typing.inference import infer_type_candidates
from analysis.typing.resolution import resolve_types
from analysis.typing.patterns import load_pattern_config
from analysis.typing.units import detect_unit
from analysis.typing.db_models import TypeCandidate, TypeDecision
from analysis.typing.models import TypeCandidateModel
```

### Dependencies
- Depends on: `core/` (Result, DataType, models)
- Depends on: `core/storage/` (Table, Column)
- Reads: `config/patterns/default.yaml`

### Test Data
- CSV with mixed types (integers, floats, dates, strings, emails, UUIDs)
- CSV with values that fail type casting (to test quarantine)

### Verification
1. Import works: `from analysis.typing import infer_type_candidates, resolve_types`
2. Given raw table, generates TypeCandidate records
3. Type candidates based ONLY on value patterns, not column names
4. Creates typed table with correct DuckDB types
5. Creates quarantine table with failed rows
6. TypeDecision records created with audit trail
7. Existing type inference tests pass

### Migration Steps
1. Create `analysis/typing/` directory
2. Copy db_models (extract TypeCandidate, TypeDecision)
3. Copy models (extract type-related models)
4. Copy patterns.py, remove `match_column_name` and related code
5. Copy units.py as-is
6. Copy type_inference.py → inference.py, remove column name matching code
7. Copy type_resolution.py → resolution.py
8. Update all imports
9. Create `__init__.py`
10. Relocate/create tests
11. Verify with actual data: raw → typed + quarantine
12. Remove old files once verified

### Code to Remove (type_inference.py)

Lines to delete in inference.py:
```python
# DELETE: Line 157-166
# Check column name hints
column_name_hints = pattern_config.match_column_name(col_name)

# Boost pattern matches that align with column name hints
for hint in column_name_hints:
    if hint.likely_type:
        # Find patterns that infer the same type as the column name suggests
        for pattern_name, pattern in pattern_by_name.items():
            if pattern.inferred_type == hint.likely_type:
                # Boost match count by 20% when column name confirms the pattern
                pattern_matches[pattern_name] = int(pattern_matches[pattern_name] * 1.2)

# DELETE: Lines 241-268 (Strategy 2: Column name hints fallback)
# Strategy 2: Column name hints when no value patterns matched
# This handles cases where the column name strongly suggests a type
# but our value patterns didn't detect it (e.g., unconventional formatting)
if not candidates and column_name_hints:
    for hint in column_name_hints:
        ...entire block...
```

---

## Phase 3: analysis/statistics

### Goal
Column-level statistical profiling on typed data: counts, distributions, histograms, top values.

### Source Files (current → new)

| Current Location | New Location | Notes |
|-----------------|--------------|-------|
| `profiling/statistics_profiler.py` | `analysis/statistics/processor.py` | Remove correlation call |
| `profiling/db_models.py` (partial) | `analysis/statistics/db_models.py` | Only StatisticalProfile, StatisticalQualityMetrics |
| `profiling/models.py` (partial) | `analysis/statistics/models.py` | Only stats-related models |

### What Stays
- `profile_statistics()` - main entry point
- `_profile_column_stats()` - per-column stats computation
- Basic counts (total, null, distinct, cardinality)
- String stats (min/max/avg length)
- Top values (frequency analysis)
- Numeric stats (min, max, mean, stddev, skewness, kurtosis, cv)
- Percentiles (p01, p25, p50, p75, p99)
- Histograms
- `StatisticalProfile` db_model
- `StatisticalQualityMetrics` db_model
- `ColumnProfile`, `NumericStats`, `StringStats`, `HistogramBucket`, `ValueCount` models

### What Goes
- Correlation integration removed from `profile_statistics()` - correlation becomes separate step
- `include_correlations` parameter - removed (Phase 4 handles correlation)

### What Changes
- `profile_statistics()` no longer calls `analyze_correlations()`
- Returns `StatisticsProfileResult` without correlation_result
- Pipeline orchestrates statistics → correlation as separate steps

### New Structure
```
analysis/statistics/
├── __init__.py
├── db_models.py        # StatisticalProfile, StatisticalQualityMetrics
├── models.py           # ColumnProfile, NumericStats, StringStats, etc.
├── processor.py        # profile_statistics, _profile_column_stats
├── formatter.py        # Format for LLM context (future)
└── tests/
    ├── test_statistics.py
    └── test_models.py
```

### Interface

**analysis/statistics/__init__.py**:
```python
from analysis.statistics.processor import profile_statistics
from analysis.statistics.db_models import StatisticalProfile, StatisticalQualityMetrics
from analysis.statistics.models import (
    ColumnProfile,
    NumericStats,
    StringStats,
    HistogramBucket,
    ValueCount,
    StatisticsProfileResult,
)
```

### Dependencies
- Depends on: `core/` (Result, ColumnRef)
- Depends on: `core/storage/` (Table, Column)
- Depends on: `analysis/typing/` (typed tables must exist)

### Test Data
- Typed table with numeric, string, date columns
- Mix of values to test all stats paths

### Verification
1. Import works: `from analysis.statistics import profile_statistics`
2. Given typed table, generates StatisticalProfile records
3. Numeric columns have full numeric_stats
4. String columns have string_stats
5. Histogram generated for numeric columns
6. Top values captured
7. No correlation analysis (separate phase)

### Migration Steps
1. Create `analysis/statistics/` directory
2. Extract StatisticalProfile, StatisticalQualityMetrics → `db_models.py`
3. Extract stats models (ColumnProfile, NumericStats, etc.) → `models.py`
4. Copy statistics_profiler.py → `processor.py`, remove correlation code
5. Update imports
6. Create `__init__.py`
7. Update profiling/__init__.py to re-export from new location
8. Relocate/create tests
9. Verify with actual data
10. Update pipeline to call statistics separately from correlation

---

## Phase 4: analysis/correlation

### Goal
Inter-column correlation analysis: numeric, categorical, functional dependencies, derived columns, multicollinearity.

### Source Files (current → new)

| Current Location | New Location | Notes |
|-----------------|--------------|-------|
| `profiling/correlation.py` (lines 1-200) | `analysis/correlation/numeric.py` | Pearson + Spearman |
| `profiling/correlation.py` (lines 200-360) | `analysis/correlation/categorical.py` | Cramér's V |
| `profiling/correlation.py` (lines 360-490) | `analysis/correlation/functional_dependency.py` | A → B detection |
| `profiling/correlation.py` (lines 490-620) | `analysis/correlation/derived_columns.py` | Arithmetic detection |
| `profiling/correlation.py` (lines 620-985) | `analysis/correlation/multicollinearity.py` | VIF, Condition Index |
| `profiling/correlation.py` (lines 985-1103) | `analysis/correlation/processor.py` | analyze_correlations |
| `profiling/db_models.py` (partial) | `analysis/correlation/db_models.py` | Correlation-related tables |
| `profiling/models.py` (partial) | `analysis/correlation/models.py` | Correlation models |

### What Stays
- All correlation functions (numeric, categorical, FD, derived, multicollinearity)
- All db_models (ColumnCorrelation, CategoricalAssociation, FunctionalDependency, DerivedColumn, MulticollinearityMetrics, CrossTableMulticollinearityMetrics)
- All Pydantic models

### What Goes
- Nothing removed - this is reorganization

### New Structure
```
analysis/correlation/
├── __init__.py
├── db_models.py             # ColumnCorrelation, CategoricalAssociation, etc.
├── models.py                # NumericCorrelation, CategoricalAssociation (Pydantic), etc.
├── numeric.py               # Pearson + Spearman
├── categorical.py           # Cramér's V
├── functional_dependency.py # A → B detection
├── derived_columns.py       # Arithmetic detection
├── multicollinearity.py     # VIF, Condition Index, VDP
├── processor.py             # analyze_correlations (orchestrates all)
├── formatter.py             # Format for LLM context (future)
└── tests/
    ├── test_numeric.py
    ├── test_categorical.py
    ├── test_functional_dependency.py
    ├── test_derived_columns.py
    ├── test_multicollinearity.py
    └── test_processor.py
```

### Interface

**analysis/correlation/__init__.py**:
```python
from analysis.correlation.processor import analyze_correlations
from analysis.correlation.numeric import compute_numeric_correlations
from analysis.correlation.categorical import compute_categorical_associations
from analysis.correlation.functional_dependency import detect_functional_dependencies
from analysis.correlation.derived_columns import detect_derived_columns
from analysis.correlation.multicollinearity import compute_multicollinearity_for_table
from analysis.correlation.db_models import (
    ColumnCorrelation,
    CategoricalAssociation,
    FunctionalDependency,
    DerivedColumn,
    MulticollinearityMetrics,
    CrossTableMulticollinearityMetrics,
)
from analysis.correlation.models import (
    NumericCorrelation,
    CorrelationAnalysisResult,
    MulticollinearityAnalysis,
    # ... other models
)
```

### Dependencies
- Depends on: `core/` (Result, ColumnRef)
- Depends on: `core/storage/` (Table, Column)
- Depends on: `analysis/typing/` (typed tables)
- Optional: scipy, sklearn, numpy

### Test Data
- Typed table with correlated numeric columns
- Categorical columns with associations
- Columns with functional dependencies
- Derived columns (e.g., total = price * quantity)

### Verification
1. Import works: `from analysis.correlation import analyze_correlations`
2. Numeric correlations computed correctly
3. Categorical associations (Cramér's V) computed
4. Functional dependencies detected
5. Derived columns detected
6. Multicollinearity metrics (VIF, Condition Index) computed
7. Existing correlation tests pass

---

## Phase 5: analysis/semantic

### Goal
LLM-powered semantic analysis with enriched context from prior analysis phases. The LLM receives analysis results (types, correlations, derived columns, functional dependencies) and optionally TDA-detected relationship candidates to confirm/enhance.

### Source Files (current → new)

| Current Location | New Location | Notes |
|-----------------|--------------|-------|
| `enrichment/agent.py` | `analysis/semantic/agent.py` | Enhanced with analysis context |
| `enrichment/semantic.py` | `analysis/semantic/processor.py` | Orchestration |
| `enrichment/ontology.py` | `analysis/semantic/ontology.py` | Keep as-is |
| `enrichment/models.py` (partial) | `analysis/semantic/models.py` | SemanticAnnotation, EntityDetection |
| `enrichment/db_models.py` (partial) | `analysis/semantic/db_models.py` | SemanticAnnotation, TableEntity |

### What Stays
- SemanticAgent class extending LLMFeature
- OntologyLoader with concept/metric/rule loading
- SemanticAnnotation, EntityDetection models
- Database persistence for annotations and entities

### What Changes

1. **Enriched Context for LLM**:
   - Add `resolved_type` to each column (from analysis/typing)
   - Add correlation summaries (high correlations from analysis/correlation)
   - Add derived column info ("Credit = Quantity × Rate")
   - Add functional dependencies ("Transaction ID → all columns")
   - Optionally pass TDA relationship candidates for confirmation

2. **Split Prompts** (if beneficial):
   - Column analysis prompt (roles, types, business terms)
   - Table analysis prompt (entity type, fact/dimension, grain)
   - Relationship confirmation prompt (confirm/enhance TDA candidates + discover new)

3. **Relationship Flow**:
   - TDA detects structural candidates (Phase 6)
   - LLM confirms/enhances with semantic understanding (Phase 5)
   - No heuristic combination - LLM is the final arbiter

### New Structure
```
analysis/semantic/
├── __init__.py
├── db_models.py        # SemanticAnnotation, TableEntity
├── models.py           # SemanticAnnotation, EntityDetection, SemanticEnrichmentResult
├── ontology.py         # OntologyLoader, OntologyDefinition
├── context.py          # NEW: Build enriched context from analysis results
├── agent.py            # SemanticAgent with enriched context
├── processor.py        # enrich_semantic orchestration
├── formatter.py        # Format for downstream LLM context
└── tests/
    ├── test_agent.py
    ├── test_ontology.py
    └── test_context.py
```

### Interface

**analysis/semantic/__init__.py**:
```python
from analysis.semantic.processor import analyze_semantic
from analysis.semantic.agent import SemanticAgent
from analysis.semantic.ontology import OntologyLoader, OntologyDefinition
from analysis.semantic.context import build_semantic_context
from analysis.semantic.db_models import SemanticAnnotation, TableEntity
from analysis.semantic.models import (
    SemanticAnnotation as SemanticAnnotationModel,
    EntityDetection,
    SemanticEnrichmentResult,
)
```

### Dependencies
- Depends on: `core/` (Result, enums, refs)
- Depends on: `core/storage/` (Table, Column)
- Depends on: `analysis/typing/` (resolved types)
- Depends on: `analysis/statistics/` (column profiles)
- Depends on: `analysis/correlation/` (correlations, FDs, derived columns)
- Optionally: `analysis/relationships/` (TDA candidates to confirm)
- Uses: `llm/` (LLMFeature, providers, cache)

### Context Building (NEW)

**analysis/semantic/context.py**:
```python
async def build_semantic_context(
    session: AsyncSession,
    duckdb_conn: duckdb.DuckDBPyConnection,
    table_ids: list[str],
    include_tda_candidates: bool = False,
) -> SemanticContext:
    """Build enriched context for LLM semantic analysis.

    Gathers:
    - Column profiles (statistics)
    - Resolved types (typing)
    - Correlations (correlation)
    - Derived columns (correlation)
    - Functional dependencies (correlation)
    - TDA relationship candidates (optional, from relationships)
    """
```

### Test Data
- Use finance_csv_example with multiple tables
- Verify LLM receives enriched context
- Mock LLM responses for deterministic testing

### Verification
1. Import works: `from analysis.semantic import analyze_semantic`
2. Context includes resolved types, correlations, derived columns, FDs
3. LLM receives enriched context JSON
4. Annotations stored correctly
5. Relationships include reasoning from LLM

---

## Phases 6-10: To Be Planned

### Phase 6: analysis/relationships
Source: `enrichment/topology.py`, `enrichment/relationships/*`, `enrichment/tda/*`, parts of `enrichment/db_models.py`

**Key Decision**: TDA detects structural candidates. LLM (Phase 5) confirms/enhances. No heuristic combination.

### Phase 7: analysis/temporal
Source: `enrichment/temporal.py` + `quality/temporal.py` (consolidate), `quality/formatting/temporal.py`

### Phase 8: analysis/topological
Source: `quality/topological.py`, `quality/formatting/topological.py`

### Phase 9: quality/*
Source: `quality/statistical.py`, `quality/domains/*`, `quality/synthesis.py`, `quality/formatting/*`

### Phase 10: context/ + pipeline/
Source: `quality/context.py`, `quality/models.py`, `dataflows/pipeline.py`

---

## Old Files to Remove (after all phases complete)

After successful migration and verification:

```
staging/                         # Replaced by sources/
profiling/                       # Split into analysis/typing, statistics, correlation
enrichment/                      # Split into analysis/semantic, relationships, temporal, topological
quality/                         # Reorganized into quality/ with submodules
dataflows/pipeline.py            # Replaced by pipeline/
```

---

## Verification Scripts

| Phase | Script | Status |
|-------|--------|--------|
| Phase 1 | `scripts/test_phase1_csv_import.py` | PASSED |
| Phase 2 | `scripts/test_phase2_typing.py` | PASSED |
| Phase 3 | `scripts/test_phase3_statistics.py` | PASSED |
| Phase 4 | `scripts/test_phase4_correlation.py` | PASSED |
| Phase 5 | `scripts/test_phase5_semantic.py` | PENDING |

Run verification:
```bash
# Phase 1
uv run python scripts/test_phase1_csv_import.py

# Phase 2
uv run python -m pytest tests/analysis/typing/ -v

# Phase 3
uv run python scripts/test_phase3_statistics.py

# Phase 4
uv run python scripts/test_phase4_correlation.py
```

---

## Revision History

| Date | Version | Changes |
|------|---------|---------|
| 2024-12-16 | 0.1.0 | Initial restructuring plan |
| 2024-12-16 | 0.1.1 | Phase 1 complete - sources/csv module verified |
| 2024-12-16 | 0.2.0 | Phase 2 complete - analysis/typing module with simplified inference |
| 2024-12-16 | 0.2.1 | Phase 3 and Phase 4 planned in detail |
| 2024-12-16 | 0.3.0 | Phase 3 complete - analysis/statistics module with 5 new tests |
| 2024-12-16 | 0.3.1 | Phase 3 fixes - moved quality models to quality/, removed detected_patterns from ColumnProfile |
| 2024-12-16 | 0.4.0 | Phase 4 complete - analysis/correlation module, deleted profiling/ |
| 2024-12-16 | 0.4.1 | Phase 4 fixes - deduplicate commutative derived columns, multicollinearity optional |
| 2024-12-16 | 0.5.0 | Phase 5 planned - analysis/semantic with enriched context from prior phases |

## Phase 2 Completion Notes

Key changes in Phase 2:
1. Created `analysis/typing/` module with simplified type inference
2. **Removed column name pattern matching** - type inference now based ONLY on value patterns
3. Migrated TypeCandidate and TypeDecision from profiling/db_models.py
4. Updated storage/models.py imports to use new location
5. Re-exported models in profiling/models.py for backwards compatibility
6. 21 tests passing for patterns and units modules
7. **Removed old staging/ module** - replaced by sources/csv/
8. Updated all imports in dataflows/pipeline.py and test files
9. 452 total tests passing

## Phase 3 Completion Notes

Key changes in Phase 3:
1. Created `analysis/statistics/` module for column-level statistics
2. Migrated StatisticalProfile and StatisticalQualityMetrics from profiling/db_models.py
3. Created Pydantic models: ColumnProfile, NumericStats, StringStats, HistogramBucket, ValueCount
4. Updated storage/models.py imports to use new location
5. Re-exported models in profiling/db_models.py and profiling/models.py for backwards compatibility
6. 5 new tests in tests/analysis/statistics/test_statistics.py
7. Verification script `scripts/test_phase3_statistics.py` successfully profiles 32 columns
8. 457 total tests passing

### Phase 3 Post-Completion Fixes

Architectural cleanup after Phase 3:
1. **Moved statistical quality models to quality/** - BenfordAnalysis, OutlierDetection, StatisticalQualityResult → `quality/models.py` and StatisticalQualityMetrics → `quality/db_models.py`. These represent quality assessment, not statistics computation.
2. **Removed detected_patterns from ColumnProfile** - This field is only meaningful during schema profiling (raw stage pattern detection). Statistics profiling always sets it to `[]`. Patterns are stored in SchemaProfileResult.detected_patterns instead.
3. **Backwards compatibility maintained in profiling/** - Re-exports in `profiling/models.py` and `profiling/db_models.py` for code that imports from the old location.

---

## Future Tasks (Final Phase)

These tasks should be done as the last phase of restructuring:

1. **Move storage/ under core/**: The storage module (SQLAlchemy models for Source, Table, Column) is tightly coupled with core and should be under `core/storage/`.

2. **Flatten core/models/ to core/models.py**: Currently `core/models/base.py` could be simplified to `core/models.py` since there's only one file.

These are low-priority cleanup items that don't affect functionality.
