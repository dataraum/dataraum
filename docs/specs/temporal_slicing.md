# Temporal Slicing Module

## Reasoning & Summary

The temporal_slicing module answers: **"How does data quality change over time within each slice?"**

Given slice tables with a time column, it analyzes 5 levels of temporal quality:
1. **Period Completeness** — Is each time period fully populated? Early cutoffs?
2. **Distribution Drift** — Do categorical column distributions change between periods? (JS divergence, chi-square)
3. **Cross-Slice Temporal Comparison** — Slice x time matrix exposing hidden trends where individual slices grow/decline while totals remain stable
4. **Volume Anomaly Detection** — Z-score-based spike/drop/gap detection with rolling statistics
5. **Temporal Topology** — TDA-based structural drift detection using bottleneck distance between persistence diagrams of consecutive periods

The module runs as pipeline phase `temporal_slice_analysis` (after `slice_analysis`).

## Architecture

```
temporal_slicing/
├── __init__.py     # Public API (12 exports)
├── analyzer.py     # TemporalSliceAnalyzer (Levels 1-4) + analyze_temporal_topology (Level 5)
├── db_models.py    # SQLAlchemy persistence (5 models)
└── models.py       # Pydantic + dataclass models (12 models)
```

**~1,600 LOC** across 4 files.

### Data Flow

```
analyze_temporal_slices(duckdb_conn, session, config, slice_table_name, slice_column_name)
  │
  ├── Level 1: _analyze_completeness()          → list[CompletenessResult]
  │     └── Period coverage ratio, early cutoff detection, last-day volume dropoff
  │
  ├── Level 2: _analyze_drift()                 → list[DistributionDriftResult]
  │     └── JS divergence + chi-square per categorical column per period
  │
  ├── Level 3: _build_slice_time_matrix()       → SliceTimeMatrix
  │     └── Slice x period row counts, period-over-period changes, hidden trend detection
  │
  ├── Level 4: _detect_volume_anomalies()       → list[VolumeAnomalyResult]
  │     └── Z-score anomalies, rolling avg/std, spike/drop/gap classification
  │
  └── Persist: TemporalSliceRun + TemporalSliceAnalysis + TemporalDriftAnalysis + SliceTimeMatrixEntry

analyze_temporal_topology(duck_conn, table_name, time_column, period)
  │
  ├── Per-period: TDA extraction via TableTopologyExtractor
  │     └── Persistence diagrams → Betti numbers + persistent entropy
  │
  ├── Drift: Bottleneck distance between consecutive period diagrams
  │     └── Entropy drift, complexity drift detection
  │
  ├── Trend: Increasing/decreasing/volatile/stable via half-split comparison
  │
  └── Anomalies: >2σ from mean complexity → structural anomaly periods
```

## Data Model

### SQLAlchemy Models (db_models.py)

**TemporalSliceRun** (`temporal_slice_runs`):
- PK: `run_id`
- Fields: `slice_table_name`, `time_column`, `period_start/end`, `time_grain`
- Summary: `total_periods`, `incomplete_periods`, `anomaly_count`, `drift_detected`
- Relationships: analyses, drift_analyses, matrix_entries (cascade delete)

**TemporalSliceAnalysis** (`temporal_slice_analyses`):
- PK: `id`, FK: `run_id`
- Level 1: `row_count`, `expected_days`, `observed_days`, `coverage_ratio`, `is_complete`, `has_early_cutoff`, `days_missing_at_end`, `last_day_ratio`
- Level 4: `z_score`, `rolling_avg`, `rolling_std`, `is_volume_anomaly`, `anomaly_type`, `period_over_period_change`
- `issues_json` (JSON)

**TemporalDriftAnalysis** (`temporal_drift_analyses`):
- PK: `id`, FK: `run_id`
- Fields: `column_name`, `period_label`, `js_divergence`, `chi_square_statistic`, `chi_square_p_value`
- Flags: `has_significant_drift`, `has_category_changes`
- JSON: `new_categories_json`, `missing_categories_json`

**SliceTimeMatrixEntry** (`slice_time_matrix_entries`):
- PK: `id`, FK: `run_id`
- Fields: `slice_value`, `period_label`, `row_count`, `period_over_period_change`

**TemporalTopologyAnalysis** (`temporal_topology_analyses`):
- PK: `id`, FK: `run_id` (nullable)
- Summary: `periods_analyzed`, `avg_complexity`, `complexity_variance`, `trend_direction`, `num_drifts_detected`, `num_anomaly_periods`
- JSON: `period_topologies_json`, `topology_drifts_json`, `anomaly_periods_json`

### Pydantic/Dataclass Models (models.py)

| Model | Purpose |
|-------|---------|
| `TemporalSliceConfig` | Per-analysis configuration (time_column, period range, thresholds) |
| `TimeGrain` | Enum: DAILY, WEEKLY, MONTHLY |
| `PeriodMetrics` | Per-period row count, coverage, rolling stats |
| `CompletenessResult` | Level 1 output per period |
| `DistributionDriftResult` | Level 2 output per column per period |
| `SliceTimeCell` | Single cell in slice x time matrix |
| `SliceTimeMatrix` | Level 3 output: full matrix with trends |
| `VolumeAnomalyResult` | Level 4 output per period |
| `TemporalAnalysisResult` | Combined result of Levels 1-4 |
| `PeriodTopology` | Level 5: per-period Betti numbers + entropy |
| `TopologyDrift` | Level 5: drift between consecutive periods |
| `TemporalTopologyResult` | Level 5: complete topology analysis |

## Configuration

### `config/system/temporal_slicing.yaml`

```yaml
defaults:
  completeness_threshold: 0.9        # coverage_ratio threshold
  drift_threshold: 0.1               # JS divergence threshold
  volume_zscore_threshold: 2.5       # z-score for volume anomaly
  last_day_ratio_threshold: 0.3      # for cutoff detection

hidden_trends:
  global_change_threshold: 0.1       # <10% global change = "stable"
  slice_trend_threshold: 0.2         # >20% slice change = significant

topology:
  max_rows_per_period: 5000          # max rows fed to TDA per period
  bottleneck_threshold: 0.5          # bottleneck distance for significant drift
  min_samples: 10                    # min rows per period for valid analysis
  entropy_change_threshold: 0.3      # >30% entropy change between periods
  entropy_significance: 0.5          # >50% = significant
  complexity_change_threshold: 0.2   # >20% complexity change between periods
  complexity_significance: 0.5       # >50% = significant
  trend_increase_factor: 1.2         # second-half avg > 1.2x first-half = increasing
  trend_decrease_factor: 0.8         # second-half avg < 0.8x first-half = decreasing
  volatility_threshold: 0.5          # variance > 50% of mean = volatile
  anomaly_stddev_factor: 2.0         # >2 sigma from mean = anomalous period
```

## Consumers

| Consumer | What It Uses |
|----------|--------------|
| `temporal_slice_analysis_phase` | `analyze_temporal_slices()`, `analyze_temporal_topology()` |
| `quality_summary/processor.py` | `TemporalSliceAnalysis`, `TemporalDriftAnalysis` (loads temporal context for LLM) |
| `entropy_phase` | `TemporalSliceAnalysis` (temporal quality for entropy scoring) |

## Cleanup History (This Refactor)

| Change | Rationale |
|--------|-----------|
| Removed 7 unused exports from `__init__.py` | Internal models not used externally |
| Moved inline imports to module level | numpy, topology modules |
| Added structured logging (7 log points) | Silent operations now observable |
| Created `config/system/temporal_slicing.yaml` | Extracted 11 Python constants to YAML |
| Removed `bottleneck_threshold`/`min_samples` function parameters | Now loaded from YAML config |
| Updated `temporal_slice_analysis_phase.py` | Removed `bottleneck_threshold` passthrough |

## Roadmap

- **TemporalSliceConfig defaults from YAML**: Config model defaults could load from YAML instead of hardcoded
- **Level 5 persistence**: `TemporalTopologyAnalysis` DB model exists and is populated; could add UI/API exposure
