# Temporal Analysis Module

## Reasoning & Summary

The temporal module answers: **"What are the temporal characteristics of time columns in this data?"**

Most data has a time dimension. Understanding its structure — granularity, completeness, seasonality, trends, change points, staleness — is essential for data quality assessment and for downstream consumers (entropy, slicing, LLM agents) to make informed decisions.

The module runs as pipeline phase `temporal` (after `column_eligibility`) and profiles every column with resolved type `DATE`, `TIMESTAMP`, or `TIMESTAMPTZ`.

## Architecture

```
temporal/
├── __init__.py        # Public API: profile_temporal()
├── models.py          # Pydantic result models (15 models)
├── db_models.py       # SQLAlchemy persistence (2 models)
├── detection.py       # Basic: granularity, gaps, completeness
├── patterns.py        # Advanced: seasonality, trend, change points, fiscal, distribution
└── processor.py       # Orchestrator: parallel profiling, quality issues, persistence
```

**~2100 LOC** across 6 files.

### Data Flow

```
profile_temporal(table_id, duckdb_conn, session)
  │
  ├── For each temporal column (parallel via ThreadPoolExecutor):
  │     ├── _load_time_series()          → pd.Series (Bernoulli sampled)
  │     ├── infer_granularity()          → (name, confidence)
  │     ├── analyze_seasonality()        → SeasonalityAnalysis
  │     ├── analyze_trend()              → TrendAnalysis
  │     ├── detect_change_points()       → list[ChangePointResult]
  │     ├── analyze_update_frequency()   → UpdateFrequencyAnalysis
  │     ├── detect_fiscal_calendar()     → FiscalCalendarAnalysis
  │     ├── analyze_distribution_stability() → DistributionStabilityAnalysis
  │     ├── analyze_basic_temporal()     → completeness (via DuckDB SQL)
  │     └── _detect_quality_issues()     → list[TemporalQualityIssue]
  │
  ├── _compute_table_summary()           → TemporalTableSummary
  ├── Persist TemporalColumnProfile rows (SQLAlchemy)
  └── Persist TemporalTableSummary row (SQLAlchemy, upsert)
```

### External Dependencies

| Library | Used For |
|---------|----------|
| `statsmodels` | `seasonal_decompose()` for seasonality detection |
| `ruptures` | PELT algorithm for change point detection |
| `scipy.stats` | `linregress()` for trend, `ks_2samp()` for distribution stability |
| `numpy` | Array operations throughout |
| `pandas` | Time series representation, frequency inference |

## Data Model

### Pydantic Models (models.py)

| Model | Purpose |
|-------|---------|
| `TemporalGapInfo` | Single gap: start, end, length, severity |
| `TemporalCompletenessAnalysis` | Completeness ratio, expected/actual periods, gaps |
| `SeasonalDecompositionResult` | Decomposition components, strength metrics |
| `SeasonalityAnalysis` | Has seasonality, strength, period, peaks |
| `TrendAnalysis` | Has trend, strength, direction, slope, autocorrelation |
| `ChangePointResult` | Single change point: type, magnitude, confidence, before/after stats |
| `UpdateFrequencyAnalysis` | Regularity score, median interval, staleness |
| `FiscalCalendarAnalysis` | Fiscal year end detection, period-end effects |
| `DistributionShiftResult` | Single distribution shift between periods (KS test) |
| `DistributionStabilityAnalysis` | Overall stability score, shift count |
| `TemporalQualityIssue` | Quality issue: type, severity, description, evidence |
| `TemporalAnalysisResult` | Per-column result (composes all above) |
| `TemporalTableSummary` | Per-table aggregate (column counts, pattern flags) |
| `TemporalProfileResult` | Top-level return: column_profiles + table_summary |

### SQLAlchemy Models (db_models.py)

**TemporalColumnProfile** (`temporal_column_profiles`):
- Hybrid storage: structured queryable fields + full JSON blob
- FK to `columns.column_id`
- Queryable: `min_timestamp`, `max_timestamp`, `detected_granularity`, `completeness_ratio`, `has_seasonality`, `has_trend`, `is_stale`
- `profile_data`: Full `TemporalAnalysisResult.model_dump(mode="json")`

**TemporalTableSummary** (`temporal_table_summaries`):
- PK: `table_id` (FK to `tables`)
- Queryable: pattern counts, `stalest_column_days`, `has_stale_columns`
- `summary_data`: Full summary as JSON
- Currently write-only; intended for future dashboards

## Algorithms

### Granularity Detection (detection.py)

Compares median gap between consecutive timestamps against known granularity definitions:
- second (1s ± 0.5s), minute (60s ± 5s), hour (3600s ± 300s), day (86400s ± 3600s), week (604800s ± 86400s), month (~30d ± 3d), quarter (~90d ± 9d), year (~365d ± 36.5d)
- Confidence based on variation: `1 - min((max_gap - min_gap) / median_gap / divisor, 0.5)`
- Falls back to "irregular" with low confidence if no match

### Completeness (detection.py)

- Calculates expected periods from time span and detected granularity
- Completeness ratio = distinct_count / expected_periods
- Detects significant gaps (> 2× median gap) via DuckDB SQL
- Gap severity: severe (> 10× median), moderate (> 5× median), minor

### Seasonality (patterns.py)

- Uses `statsmodels.seasonal_decompose` (additive, falls back to multiplicative)
- Auto-detects period from pandas frequency inference or config map
- Strength: `1 - Var(residual) / Var(detrended)`
- Detected when strength > 0.3 (configurable)

### Trend (patterns.py)

- Linear regression via `scipy.stats.linregress`
- Direction: increasing/decreasing/stable
- Significance: slope must exceed stderr × 2.0 (configurable)
- Strength: R² (detected when > 0.3)

### Change Points (patterns.py)

- PELT algorithm via `ruptures` with L2 cost model
- Samples to 1000 points for large series (stride sampling)
- Classifies as level_shift or variance_change based on before/after statistics
- Confidence based on segment sizes

### Update Frequency (patterns.py)

- Coefficient of variation of inter-timestamp intervals
- Regularity score: `1 - min(CV, 1)`
- Staleness: data age > median_interval × 2.0 (configurable)

### Fiscal Calendar (patterns.py)

- Detects fiscal year end by finding month with anomalous activity (> 1.5× mean)
- Period-end effects: checks if days 28-31 have disproportionate data (> 1.5× expected 13% ratio)

### Distribution Stability (patterns.py)

- Splits time series into 4 periods, compares adjacent pairs with KS test
- Stability score: `1 - mean(KS statistics)`
- Classifies shifts as increase/decrease/mixed based on configurable thresholds

## Configuration

All thresholds live in `config/system/temporal.yaml`. See that file for the full structure. Key sections:

| Section | Controls |
|---------|----------|
| `granularity` | Definitions, confidence calculations |
| `gaps` | Significant gap multiplier, severity thresholds |
| `seasonality` | Min data points, strength threshold, period map |
| `trend` | Min data points, significance multiplier, R² threshold |
| `change_points` | PELT parameters (min_segment, jump, penalty), sampling |
| `staleness` | Stale multiplier |
| `fiscal_calendar` | Activity spike multiplier, period-end thresholds |
| `distribution_stability` | Number of periods, KS significance level, shift thresholds |
| `quality_issues` | Completeness/gap/change point/stability thresholds |
| `processing` | max_workers, sample_percent, min_sample_rows |

Config is auto-loaded in `profile_temporal()` if not explicitly provided.

## Consumers

| Consumer | What It Uses |
|----------|--------------|
| `temporal_slice_analysis` | `TemporalAnalysisResult` per column for drift analysis |
| `entropy` | Temporal quality issues feed into entropy scoring |
| `context assembly` | Temporal metadata included in GraphExecutionContext |
| TUI (future) | `TemporalTableSummary` for dashboard display |

## Cleanup History (This Refactor)

| Change | Rationale |
|--------|-----------|
| Added `get_logger` to detection.py, patterns.py, processor.py | Was missing logging entirely |
| Fixed bare except in processor.py worker thread | Silent failures now logged with context |
| Moved `import time` to module level | Inline imports are a code smell |
| Removed deprecated `TemporalEnrichmentResult` | Dead model, no consumers |
| Extracted 15+ magic numbers to `config/system/temporal.yaml` | Calibration readiness |
| Added `config` kwarg to all public functions | Config-driven, no hardcoded defaults |
| Moved inline `load_yaml_config` import to module level in processor.py | Consistency with other modules |
| Verified all functions are used (no dead code) | `calculate_expected_periods` is used internally by `analyze_basic_temporal` |

## Roadmap

- **Sampling strategy**: Current Bernoulli sampling creates artificial gaps; consider stratified sampling for better temporal coverage
- **Calibration**: Tune thresholds in `temporal.yaml` with real test data from `dataraum-testdata`
- **TUI integration**: Surface `TemporalTableSummary` in dashboard
- **Streaming**: Support incremental temporal analysis for append-only data
