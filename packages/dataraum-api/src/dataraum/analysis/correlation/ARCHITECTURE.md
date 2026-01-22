# Correlation Module Architecture

## Overview

The correlation module analyzes relationships between columns:
- **Within-table**: Numeric correlations, categorical associations, functional dependencies, derived columns
- **Cross-table**: Quality analysis after relationships are confirmed (VDP, redundant columns)

## Model Layers

### 1. Pydantic Models (`models.py`) - API/Domain Layer

These models are returned from analysis functions and used in API responses.

**Within-Table Analysis:**
- `NumericCorrelation` - Pearson/Spearman correlations
- `CategoricalAssociation` - Cramér's V associations
- `FunctionalDependency` - A → B dependencies
- `DerivedColumn` - Derived/computed columns

**Cross-Table Quality:**
- `CrossTableCorrelation` - Correlation between columns in different tables
- `RedundantColumnPair` - Perfectly correlated columns (same table)
- `DependencyGroup` - VDP multicollinearity group
- `QualityIssue` - Generic quality issue

**Result Containers:**
- `CorrelationAnalysisResult` - Per-table analysis result
- `CrossTableQualityResult` - Cross-table quality result

**Utilities:**
- `EnrichedRelationship` - Relationship with metadata for building joins

### 2. DB Models (`db_models.py`) - Persistence Layer

SQLAlchemy models for storing analysis results in SQLite.

**Within-Table:**
- `ColumnCorrelation` - Numeric correlations
- `CategoricalAssociation` - Cramér's V
- `FunctionalDependency` - Dependencies
- `DerivedColumn` - Derived columns

**Cross-Table:**
- `CrossTableCorrelation` - Cross-table correlations
- `MulticollinearityGroup` - VDP groups
- `QualityIssue` - Quality issues

**Metadata:**
- `CorrelationAnalysisRun` - Tracks when analysis was run

### 3. Algorithms (`algorithms.py`) - Pure Computation (Internal)

Pure functions operating on numpy arrays. These are internal and not exported from the module:
- `compute_pairwise_correlations()` - Pearson/Spearman
- `compute_cramers_v()` - Chi-square based association
- `compute_multicollinearity()` - VDP analysis

### 4. Processor (`processor.py`) - Entry Points

Main functions that orchestrate analysis:
- `analyze_correlations()` - Per-table analysis
- `analyze_cross_table_quality()` - Cross-table quality after semantic confirmation

## Public API

Only processors and models are exported from the module:

```python
from dataraum_context.analysis.correlation import (
    # Processors (main entry points)
    analyze_correlations,
    analyze_cross_table_quality,
    # Pydantic Models
    CorrelationAnalysisResult,
    CrossTableQualityResult,
    # etc.
)
```

## Usage Pattern

```python
# Per-table analysis
result = await analyze_correlations(table_id, duckdb_conn, session)

# Cross-table quality (after semantic agent confirms relationship)
quality = await analyze_cross_table_quality(relationship, duckdb_conn, session)
```

## Data Flow

```
CSV Files
    ↓
[sources/csv] Load → raw_{table}
    ↓
[analysis/typing] Type resolution → typed_{table}
    ↓
[analysis/correlation] Per-table analysis → CorrelationAnalysisResult
    ↓                                       (stored in DB)
[analysis/relationships] Detect candidates
    ↓
[enrichment/semantic] LLM confirms relationships
    ↓
[analysis/correlation] Cross-table quality → CrossTableQualityResult
                                             (stored in DB)
```
