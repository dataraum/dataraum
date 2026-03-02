# Statistical Profiling & Quality

## Reasoning & Summary

`analysis/statistics/` computes row-based statistics on typed (clean) data and assesses statistical quality of numeric columns. It runs after type resolution and provides the foundation for downstream modules (eligibility, correlations, entropy).

Two main functions:
- **`profile_statistics()`** — Computes per-column statistics: counts, nulls, cardinality, numeric stats (min/max/mean/stddev/skewness/kurtosis/CV/percentiles), string stats (lengths), histograms, top-k values. Parallel via `ThreadPoolExecutor`.
- **`assess_statistical_quality()`** — Runs Benford's Law compliance and outlier detection (IQR + Modified Z-Score) on numeric columns. Parallel via `ThreadPoolExecutor`.

## Data Model

### SQLAlchemy Models (Hybrid Storage)

Both models use hybrid storage: structured fields for fast queries + JSONB for full Pydantic model flexibility.

```
StatisticalProfile (statistical_profiles)
├── profile_id: String (PK, auto-generated)
├── column_id: FK → columns (CASCADE)
├── profiled_at: DateTime
├── layer: String ('typed')
├── total_count: Integer
├── null_count: Integer
├── distinct_count: Integer
├── null_ratio: Float
├── cardinality_ratio: Float
├── is_unique: Boolean (all values unique — potential PK)
├── is_numeric: Boolean (has numeric stats)
└── profile_data: JSON (full ColumnProfile Pydantic model)

StatisticalQualityMetrics (statistical_quality_metrics)
├── metric_id: String (PK, auto-generated)
├── column_id: FK → columns (CASCADE)
├── computed_at: DateTime
├── benford_compliant: Boolean
├── has_outliers: Boolean (IQR ratio > 5%)
├── iqr_outlier_ratio: Float
├── zscore_outlier_ratio: Float
└── quality_data: JSON (full StatisticalQualityResult Pydantic model)
```

### Pydantic Models (Computation)

- **ColumnProfile** — Per-column: counts, ratios, NumericStats, StringStats, histogram, top values
- **NumericStats** — min, max, mean, stddev, skewness, kurtosis, CV, MAD, robust_cv, percentiles (p01/p25/p50/p75/p99)
- **StringStats** — min/max/avg length
- **BenfordAnalysis** — chi_square, p_value, is_compliant, digit_distribution, interpretation
- **OutlierDetection** — IQR fences + counts + Modified Z-Score counts + sample outliers
- **StatisticalQualityResult** — Per-column: benford + outliers + quality issues list

## Metrics

### Benford's Law
- Chi-square goodness-of-fit test against log10(1 + 1/d) distribution
- Requires ≥100 non-zero values
- Compliant if p_value > 0.05; weak deviation if p_value > 0.01; strong deviation otherwise

### Outlier Detection (IQR)
- Q1, Q3 from PERCENTILE_CONT
- Fences: Q1 - 1.5×IQR, Q3 + 1.5×IQR
- Outlier ratio > 5% → has_outliers flag

### Outlier Detection (Modified Z-Score)
- Uses MAD (Median Absolute Deviation) instead of standard deviation
- Formula: modified_z = 0.6745 × |x - median| / MAD
- Threshold: 3.5 (values above are flagged as outliers)
- Robust to the outliers it detects (unlike stddev-based methods)
- Pure DuckDB SQL — no external dependencies

## Configuration

### Statistics Config (`system/statistics.yaml`)

```yaml
# Number of top frequent values to collect per column
top_k_values: 20
```

## Roadmap / Planned Features

- **Distribution type detection** — Classify columns as normal, log-normal, exponential, etc.
- **Shannon entropy** — Per-column information content metric: `-Σ(p_i * log2(p_i))`. Histogram data already computed; add entropy formula on top. Enables feature selection and predictability analysis. Also add normalized entropy (scaled to [0,1]) for cross-column comparison.
- **Primary key violation count** — Explicit `pk_violation_count` metric for duplicate PK detection, building on existing `is_unique` flag in `StatisticalProfile`.
- **Ordering properties** — Detection of `is_sorted`, `is_monotonic_increasing`, `is_monotonic_decreasing` for index and time-series identification.
