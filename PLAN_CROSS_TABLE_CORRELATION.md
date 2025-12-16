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
- Pure algorithms are reusable for both calls
- Cross-table runner uses existing relationships infrastructure for joins (doesn't rebuild it)

This means `enrichment/cross_table_multicollinearity.py` gets replaced by `correlation/cross_table.py`.

## Current State
- [x] `analysis/correlation/algorithms/` - pure computation functions (numeric, categorical, multicollinearity)
- `analysis/correlation/` - per-table correlation analysis
- `analysis/relationships/` - relationship detection (topology, finder, joins)
- `enrichment/cross_table_multicollinearity.py` - to be replaced

## Tasks

### Task 0: Create pure algorithms in correlation/algorithms/ ✅
- [x] `algorithms/numeric.py` - pure Pearson/Spearman on numpy arrays
- [x] `algorithms/categorical.py` - pure Cramér's V on contingency tables
- [x] `algorithms/multicollinearity.py` - pure VDP/condition index (exact copy from enrichment)

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

### Task 4: Integrate with downstream quality ✅
- [x] Added `store_cross_table_analysis()` function
- [x] Added `compute_cross_table_multicollinearity()` convenience wrapper
- [x] Quality context already fetches from CrossTableMulticollinearityMetrics DB

### Task 5: Remove old implementation ✅
- [x] Updated imports in `dataflows/pipeline.py` and `scripts/run_staging_profiling.py`
- [x] Updated import in `tests/quality/test_multi_table_business_cycles.py`
- [ ] Old file `enrichment/cross_table_multicollinearity.py` can be deleted when ready
- [ ] Old test files in `tests/enrichment/` can be updated or removed

## Output Context (what downstream agents receive)

```python
CrossTableCorrelationResult:
  - table_ids: list[str]
  - table_names: list[str]
  - relationships_used: list[RelationshipInfo]

  # Correlations
  - numeric_correlations: list[CrossTableCorrelation]
  - categorical_associations: list[CrossTableAssociation]  # optional

  # Multicollinearity
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
- Exact function copies when possible, no unnecessary reinterpretation
- Rich context output for both semantic and quality agents
