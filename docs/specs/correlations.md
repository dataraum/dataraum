# Correlations Module Spec

## Reasoning & Summary

The correlation module detects statistical relationships between columns — both within a single table and across tables joined by confirmed relationships. This serves two purposes:

1. **Data quality**: Identifies redundant columns, derived columns, and multicollinearity that inflate entropy and reduce data reliability.
2. **Context enrichment**: Provides the semantic agent and entropy system with quantitative evidence about column interdependencies.

The module is split into three layers: pure algorithms (no DB), within-table orchestrators (parallel DuckDB + DB persistence), and cross-table quality analysis (post-relationship confirmation).

## Architecture

```
analysis/correlation/
├── algorithms/           # Pure functions, no DB access
│   ├── numeric.py        # Pearson/Spearman via scipy
│   ├── categorical.py    # Cramér's V via chi-square
│   └── multicollinearity.py  # Belsley VDP methodology
├── within_table/         # Single-table analysis with DB persistence
│   ├── numeric.py        # Orchestrates numeric correlations with sampling
│   ├── categorical.py    # Orchestrates Cramér's V with parallel processing
│   ├── functional_dependency.py  # A → B detection
│   └── derived_columns.py       # col3 = col1 op col2 detection
├── cross_table/          # Post-relationship quality analysis
│   └── quality.py        # VDP, cross-table correlations, redundancy
├── processor.py          # Main entry points: analyze_correlations, analyze_cross_table_quality
├── models.py             # 12 Pydantic models
└── db_models.py          # 8 SQLAlchemy models
```

## Data Model

### Pydantic Models (models.py)

| Model | Purpose |
|-------|---------|
| `CorrelationResult` | Single numeric correlation (Pearson r, Spearman rho, p-value) |
| `AssociationResult` | Single categorical association (Cramér's V, chi-square) |
| `MulticollinearityResult` | VDP analysis result (condition index, eigenvalues) |
| `DependencyGroupResult` | Group of collinear variables from VDP |
| `FunctionalDependency` | A → B with confidence, violation count |
| `DerivedColumn` | col3 = col1 op col2 with match rate |
| `CorrelationAnalysisResult` | Aggregated within-table results |
| `CrossTableCorrelation` | Correlation between columns in different tables |
| `RedundantColumnPair` | r ≈ 1.0 within same table |
| `DerivedColumnCandidate` | Cross-table derivation candidate |
| `DependencyGroup` | Cross-table multicollinearity group |
| `CrossTableQualityResult` | Aggregated cross-table results |
| `EnrichedRelationship` | Relationship with resolved table/column metadata |

### SQLAlchemy Models (db_models.py)

| Model | Table | Key Fields |
|-------|-------|------------|
| `CorrelationAnalysisRun` | `correlation_analysis_runs` | table_id, status, started_at, completed_at |
| `NumericCorrelationDB` | `numeric_correlations` | table_id, col1_idx, col2_idx, pearson_r, spearman_rho, p_value |
| `CategoricalAssociationDB` | `categorical_associations` | table_id, col1_idx, col2_idx, cramers_v, chi2_stat, p_value |
| `FunctionalDependency` | `functional_dependencies` | table_id, determinant_column_ids, dependent_column_id, confidence |
| `DerivedColumn` | `derived_columns` | table_id, derived_column_id, source_column_ids, formula, match_rate |
| `CrossTableCorrelationDB` | `cross_table_correlations` | relationship_id, from_col, to_col, pearson_r |
| `MulticollinearityGroup` | `multicollinearity_groups` | relationship_id, involved_columns, condition_index, severity |
| `QualityIssueDB` | `quality_issues` | relationship_id, issue_type, severity, description |

All tables have appropriate indexes on table_id, relationship_id, and composite keys.

## Algorithms

### Numeric Correlation (Pearson + Spearman)
- scipy `pearsonr()` and `spearmanr()` with p-value
- NaN-aware: drops rows where either column is NaN
- Minimum sample size check (default: 30)
- Strength classification: none (<0.3), weak (0.3-0.5), moderate (0.5-0.7), strong (0.7-0.9), very_strong (≥0.9)

### Categorical Association (Cramér's V)
- scipy `chi2_contingency()` with Yates' correction for 2x2 tables
- Bias-corrected V using Bergsma & Wicher (2013) formula
- Minimum 5 observations required
- Strength: none (<0.1), weak (0.1-0.3), moderate (0.3-0.5), strong (≥0.5)

### Multicollinearity (Belsley VDP)
- Condition indices from eigenvalue decomposition of correlation matrix
- Variance Decomposition Proportions per eigenvalue
- Severity: none (CI < 10), moderate (10-30), severe (> 30)
- Dependency groups: columns with high VDP (> 0.5) on same weak eigenvalue

### Functional Dependencies
- A → B: for each distinct A value, check if B has exactly one value
- Confidence = valid_mappings / total_unique_A
- Parallel via ThreadPoolExecutor with DuckDB cursors

### Derived Columns
- Checks arithmetic: col3 = col1 ± col2, col1 × col2, col1 / col2
- Excludes all-zero rows (inflate match rates)
- Deduplicates algebraic equivalences (z = x*y ↔ x = z/y)
- Parallel via ThreadPoolExecutor with DuckDB cursors

## Configuration

`config/system/correlations.yaml`:
```yaml
min_correlation: 0.3         # Minimum |r| to report cross-table correlations
redundancy_threshold: 0.99   # Correlation threshold for redundant column detection
```

Within-table thresholds are hardcoded in algorithm functions (classify_strength boundaries). These are statistical constants unlikely to need domain customization.

## Pipeline Phases

| Phase | Input | Output |
|-------|-------|--------|
| `correlations` | typed tables, statistical profiles | numeric_correlations, categorical_associations, functional_dependencies, derived_columns |
| `cross_table_quality` | confirmed relationships (from semantic phase) | cross_table_correlations, redundant_pairs, multicollinearity_groups |

Both phases use `load_yaml_config("system/correlations.yaml")` for thresholds.

## Parallelism

Within-table analysis uses `ThreadPoolExecutor` for:
- Functional dependency checking (one pair per thread)
- Derived column checking (one triple+operation per thread)

DuckDB cursors from a shared connection are thread-safe for read operations (Python 3.14 free-threaded build).

## De-configured: Cross-Table Quality Phase

The `cross_table_quality` phase has been **removed from the pipeline**. It produces 3 DB models (`CrossTableCorrelationDB`, `MulticollinearityGroup`, `QualityIssueDB`) that are currently write-only — no downstream module reads them. The code is preserved for potential future use.

**Evaluation needed before re-introduction:**
- How much of cross-table correlations overlaps with what relationships + semantic already provide?
- Can multicollinearity (VDP) feed the entropy scoring system meaningfully?
- Are cross-table quality issues distinct from what quality_summary already captures?

The within-table correlation phase (`correlations`) remains active — its outputs are consumed by slicing, semantic, entropy, and graphs.

## Roadmap

- **String transforms**: Detect UPPER/LOWER/TRIM derivations between VARCHAR columns
- **Concatenation**: Detect col3 = col1 || col2 for string columns
- **Composite FDs**: (A, B) → C — multi-column determinants
- **Cross-source correlations**: When multi-source support lands, correlate columns across different data sources joined by confirmed relationships
