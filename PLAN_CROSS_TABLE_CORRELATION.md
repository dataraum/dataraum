# Plan: Cross-Table Correlation & Multicollinearity

## Goal
Add cross-table correlation analysis to the correlation module, including multicollinearity detection.
The analysis runs 2x:
1. Before semantic agent: on staging data + relationship candidates → context for semantic agent
2. After semantic agent: on confirmed relationships → context for downstream quality agents

## Architecture Decision
**All cross-table correlation/multicollinearity lives in `analysis/correlation/`**

Rationale:
- It's computing correlations (unified correlation matrix across tables)
- Multicollinearity is derived from that correlation matrix
- Pure algorithms are reusable for both per-table and cross-table analysis
- Cross-table runner uses existing relationships infrastructure for joins (doesn't rebuild it)

This means `enrichment/cross_table_multicollinearity.py` gets replaced by `correlation/cross_table.py`.

## Current State
- [x] `analysis/correlation/algorithms/` - pure computation functions (numeric, categorical, multicollinearity)
- [x] `analysis/correlation/cross_table.py` - cross-table correlation runner
- `analysis/correlation/` - per-table correlation analysis
- `analysis/relationships/` - relationship detection (topology, finder, joins)
- `enrichment/cross_table_multicollinearity.py` - to be replaced

## Tasks

### Task 0: Create pure algorithms in correlation/algorithms/ ✅
- [x] `algorithms/numeric.py` - pure Pearson/Spearman on numpy arrays (`compute_pairwise_correlations`)
- [x] `algorithms/categorical.py` - pure Cramér's V on contingency tables (`compute_cramers_v`, `build_contingency_table`)
- [x] `algorithms/multicollinearity.py` - pure VDP/condition index (`compute_multicollinearity`)

### Task 1: Create cross-table correlation runner ✅
- [x] Created `analysis/correlation/cross_table.py`
- [x] Uses existing relationships infrastructure (`analysis/relationships/`) for joins
- [x] Accepts explicit relationships (for flexibility)
- [x] Computes multicollinearity analysis (using algorithms/multicollinearity.py)
- [x] Returns rich context: dependency groups, join paths, quality issues

### Task 2: Update models for cross-table support ✅
- [x] Reused existing models from `analysis/relationships/models.py`
- [x] No duplication needed - CrossTableMulticollinearityAnalysis already exists

### Task 3: Integrate with semantic agent ✅
- [x] Updated prompt template with `correlation_context` variable
- [x] Added `_format_correlation_context()` method to SemanticAgent
- [x] Updated `analyze()` and `enrich_semantic()` to accept correlation context
- [x] Added cross-table numeric correlations (strong/very_strong, limited to top 10)
- [x] Added cross-table categorical associations (moderate/strong, limited to top 10)

### Task 4: Integrate with downstream quality ✅
- [x] Added `store_cross_table_analysis()` function
- [x] Added `compute_cross_table_multicollinearity()` convenience wrapper
- [x] Quality context already fetches from CrossTableMulticollinearityMetrics DB

### Task 5: Remove old implementation ✅
- [x] Updated imports in `dataflows/pipeline.py` and `scripts/run_staging_profiling.py`
- [x] Updated import in `tests/quality/test_multi_table_business_cycles.py`
- [ ] Old file `enrichment/cross_table_multicollinearity.py` can be deleted when ready
- [ ] Old test files in `tests/enrichment/` can be updated or removed

### Task 5b: Move cross-table models to correlation/models.py ✅
**Problem:** Cross-table correlation models were in `relationships/models.py` but are primarily used by correlation module.

**Models moved to `correlation/models.py`:**
- [x] `CrossTableNumericCorrelation`
- [x] `CrossTableCategoricalAssociation`
- [x] `CrossTableDependencyGroup`
- [x] `CrossTableMulticollinearityAnalysis`
- [x] `SingleRelationshipJoin` (used for join path context)
- [x] `EnrichedRelationship` (used for building joins)

**Kept in relationships/models.py:**
- `JoinCandidate`, `RelationshipCandidate`, `RelationshipDetectionResult` (relationship detection)
- `DependencyGroup` (within-table multicollinearity)

### Task 6: Use pure algorithms in cross-table analysis ✅
**Problem:** The pure algorithms created in Task 0 were not being used in cross-table analysis.

**Solution:** Updated `cross_table.py` to use all pure algorithms:
- [x] Use `compute_pairwise_correlations` instead of raw `np.corrcoef`
- [x] Add cross-table categorical associations using `compute_cramers_v`
- [x] Handle zero-variance columns (skip from correlation matrix)
- [x] Handle NaN values and masked arrays in correlation matrix
- [x] Use DuckDB `USING SAMPLE` for random data sampling
- [x] Update result model with `numeric_correlations` and `categorical_associations`
- [x] Update `_format_correlation_context()` to include new results

**Results from test data:**
- 30 numeric correlations (17 cross-table)
- 25 categorical associations (11 cross-table)
- Cross-table dependencies detected when data supports it

### Task 7: Test data validation ✅
**Status:** Existing finance CSV example works well with updated implementation.

**What works:**
- [x] `USING SAMPLE` for random sampling now implemented
- [x] Master_txn_table has good numeric columns (Amount, Quantity, Rate, Credit, Debit)
- [x] Cross-table correlations detected (17 cross-table out of 30 total)
- [x] Cross-table categorical associations detected (11 cross-table out of 25 total)
- [x] Multicollinearity groups detected (within-table Business ID dependencies)

**Future improvements (optional):**
- [ ] Create synthetic test dataset with explicit cross-table multicollinearity
- [ ] Add more diverse numeric columns to customer/vendor tables

### Task 8: Fix VDP group splitting for perfectly correlated columns ✅
**Problem:** When 4+ columns are perfectly correlated (r=1.0), the Belsley VDP algorithm spreads
variance across multiple near-zero eigenvalue dimensions. This caused:
1. Columns split into separate groups (e.g., 2 in one group, 2 in another)
2. Some perfectly correlated columns missed entirely (VDP < threshold)

**Example:** 4 Business Id columns from 4 tables (master_txn, customer, vendor, payment_method)
were all perfectly correlated (r=1.0), but only 2 were found in a dependency group.

**Solution:** Added two post-processing functions:

1. **`_merge_correlated_groups()`** - Expands VDP groups:
   - Expands each group to include ALL columns with r >= 0.95 correlation with any member
   - Merges groups that share columns or have highly correlated members

2. **`_find_correlation_clusters()`** - Finds additional groups:
   - Builds a correlation graph where edges connect columns with |r| >= threshold
   - Finds connected components (clusters) of highly correlated columns
   - Creates dependency groups for clusters not already covered by VDP

**Result:** All 4 Business Id columns now correctly appear in a single dependency group spanning
all 4 tables, plus Transaction ID (r=0.999 with Business Id). Additional correlation clusters
are detected even when VDP misses them due to eigenvalue/VDP thresholds.

**Debug mode:** Set `DEBUG_MULTICOLLINEARITY=1` environment variable to see raw VDP results
and merging output.

## Output Context (what downstream agents receive)

```python
CrossTableCorrelationResult:
  - table_ids: list[str]
  - table_names: list[str]
  - relationships_used: list[RelationshipInfo]

  # Numeric Correlations (NEW - from compute_pairwise_correlations)
  - numeric_correlations: list[CrossTableNumericCorrelation]
    - table1, column1: source column
    - table2, column2: target column
    - pearson_r, spearman_rho: correlation coefficients
    - strength: "weak" | "moderate" | "strong" | "very_strong"
    - is_cross_table: bool (whether columns are from different tables)

  # Categorical Associations (NEW - from compute_cramers_v)
  - categorical_associations: list[CrossTableCategoricalAssociation]
    - table1, column1: source column
    - table2, column2: target column
    - cramers_v: association strength
    - strength: "weak" | "moderate" | "strong"
    - is_cross_table: bool

  # Multicollinearity (existing)
  - overall_condition_index: float
  - overall_severity: "none" | "moderate" | "severe"
  - dependency_groups: list[DependencyGroup]
    - involved_columns: list[(table, column)]
    - join_paths: how columns are connected
    - variance_proportions: VDP values

  # Quality context
  - quality_issues: list[QualityIssue]
```

## Principles
- Keep it lean and focused
- Use existing infrastructure (relationships module for joins)
- Use pure algorithms consistently (algorithms/ folder)
- Exact function copies when possible, no unnecessary reinterpretation
- Rich context output for both semantic and quality agents
