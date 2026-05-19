# Correlation Module Architecture

## Overview

The correlation module detects derived columns — columns that are arithmetic
derivations of other columns (sum, product, ratio, difference).

Detection runs on enriched views when available (finding both same-table and
cross-table derivations) and falls back to typed tables otherwise.

## Model Layers

### 1. Pydantic Models (`models.py`) - API/Domain Layer

- `NumericCorrelation` - Pearson/Spearman correlations
- `DerivedColumn` - Derived/computed columns
- `CorrelationAnalysisResult` - Per-table analysis result

### 2. DB Models (`db_models.py`) - Persistence Layer

- `DerivedColumn` - Persisted derived column detection results

### 3. Algorithms (`algorithms/`) - Pure Computation (Internal)

Pure functions operating on numpy arrays:
- `compute_pairwise_correlations()` - Pearson/Spearman
- `compute_cramers_v()` - Chi-square based association
- `compute_multicollinearity()` - VDP analysis

### 4. Processor (`processor.py`) - Entry Points

- `analyze_correlations()` - Per-table analysis (typed table)
- `analyze_enriched_correlations()` - Enriched view analysis (same + cross-table)

### 5. Within-Table (`within_table/`) - Detection Functions

- `detect_derived_columns()` - Typed table derived column detection
- `detect_enriched_derived_columns()` - Enriched view detection (cross-table formulas)

## Data Flow

```
typed_{table}
    ↓
[enriched_views] Build fact+dimension views
    ↓
[correlations] Detect derived columns on enriched view (or typed table fallback)
    ↓
DerivedColumn records (stored in DB)
```
