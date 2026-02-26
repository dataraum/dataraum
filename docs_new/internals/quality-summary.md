# Quality Summary Module

## Reasoning & Summary

The quality summary module answers: **"What is the quality of each column across its slices?"**

It aggregates statistical profiles, quality metrics, and semantic annotations from all slice tables for each source column, then uses an LLM to generate structured quality assessments. The key insight is the **variance filter**: if a metric (null ratio, distinct count, outlier ratio, Benford compliance) is the same across all slices, it tells nothing about slice-specific behavior. Only columns with INTERESTING variance patterns are sent to the LLM, reducing noise and cost.

The module runs as pipeline phase `quality_summary` (after `slice_analysis` and `temporal_slice_analysis`).

## Architecture

```
quality_summary/
├── __init__.py     # Public API (5 exports)
├── agent.py        # QualitySummaryAgent: LLM tool-use for structured output
├── db_models.py    # SQLAlchemy persistence (3 models)
├── models.py       # Pydantic models (14 models: 8 internal + 6 LLM output)
├── processor.py    # Orchestrator: aggregation, batching, parallel processing
└── variance.py     # Slice variance analysis and column filtering
```

**~1,900 LOC** across 6 files.

### Data Flow

```
summarize_quality(session, agent, slice_definition)
  │
  ├── aggregate_slice_results()            → list[AggregatedColumnData]
  │     ├── Source columns from Table/Column
  │     ├── SemanticAnnotation (role, business name)
  │     ├── StatisticalProfile per slice column
  │     ├── StatisticalQualityMetrics per slice column
  │     └── TemporalSliceAnalysis + TemporalDriftAnalysis → temporal_context
  │
  ├── filter_interesting_columns()         → INTERESTING columns only
  │     └── compute_slice_variance()       → ColumnClassification per column
  │
  ├── Batch processing (configurable batch_size, parallel workers):
  │     └── QualitySummaryAgent.summarize_columns_batch()
  │           ├── Render prompt (quality_summary_batch.yaml)
  │           ├── LLM tool-use call → QualitySummaryBatchOutput
  │           └── Convert to list[ColumnQualitySummary]
  │
  ├── Persist ColumnQualityReport per column
  └── Persist ColumnSliceProfile per column×slice
```

### LLM Integration

| Aspect | Detail |
|--------|--------|
| Agent type | Single-shot tool-use (extends LLMFeature) |
| Prompts | `config/system/prompts/quality_summary.yaml` (single), `quality_summary_batch.yaml` (batch) |
| Tools | `summarize_quality` / `summarize_quality_batch` with Pydantic JSON schema |
| Output | Structured: score, grade, findings, issues, comparisons, recommendations, SQL |
| Batch mode | Up to `batch_size` columns per LLM call (default: 10) |
| Parallel | `max_batch_workers` concurrent batches (default: 4) |

## Data Model

### SQLAlchemy Models (db_models.py)

**ColumnQualityReport** (`column_quality_reports`):
- PK: `report_id`
- FKs: `source_column_id`, `slice_column_id` → Column
- Assessment: `overall_quality_score` (0-1), `quality_grade` (A-F), `summary`
- JSON: `report_data` (findings, issues, comparisons, recommendations), `investigation_views`

**ColumnSliceProfile** (`column_slice_profiles`):
- PK: `profile_id`
- FKs: `source_column_id`, `slice_column_id` → Column
- Metrics: `row_count`, `null_ratio`, `distinct_count`, `quality_score`, `has_issues`, `issue_count`
- Classification: `variance_classification` (empty, constant, stable, interesting)
- JSON: `profile_data` (extended numeric/quality metrics)

**QualitySummaryRun** (`quality_summary_runs`):
- PK: `run_id`
- FKs: `source_table_id` → Table, `slice_column_id` → Column
- Tracking: `columns_analyzed`, `reports_generated`, `status`, timing

## Metrics

### Variance Filter (variance.py)

Classifies columns by comparing metrics across slices:

| Classification | Condition |
|---------------|-----------|
| EMPTY | All slices have null_ratio > `empty_null_threshold` |
| CONSTANT | All slices have distinct_count = 1 |
| INTERESTING | Any threshold exceeded (see below) |
| STABLE | No thresholds exceeded |

| Threshold | Default | Signal |
|-----------|---------|--------|
| `null_spread_threshold` | 0.10 | Field conditionally populated |
| `distinct_ratio_threshold` | 2.0 | Cardinality depends on context |
| `outlier_spread_threshold` | 0.05 | Slice-specific quality issue |
| `benford_spread_threshold` | 0.30 | Potential data manipulation |
| `row_ratio_threshold` | 10.0 | Very uneven slice coverage |

### Quality Scoring (processor.py)

Per-slice quality scores computed from configurable penalties:

| Condition | Default Penalty |
|-----------|----------------|
| null_ratio > `high_null_ratio` (0.5) | -0.3 |
| null_ratio > `moderate_null_ratio` (0.2) | -0.1 |
| has_outliers | -0.2 |
| benford non-compliant | -0.1 |

## Configuration

### `config/system/quality_summary.yaml`

```yaml
variance_filter:
  enabled: true
  null_spread_threshold: 0.10
  distinct_ratio_threshold: 2.0
  outlier_spread_threshold: 0.05
  benford_spread_threshold: 0.30
  row_ratio_threshold: 10.0
  empty_null_threshold: 0.99

quality_scoring:
  high_null_ratio: 0.5
  high_null_penalty: 0.3
  moderate_null_ratio: 0.2
  moderate_null_penalty: 0.1
  outlier_penalty: 0.2
  benford_violation_penalty: 0.1

batch_size: 10
max_batch_workers: 4
```

### Prompt Templates

- `config/system/prompts/quality_summary.yaml` — single-column mode (temperature 0.1)
- `config/system/prompts/quality_summary_batch.yaml` — batch mode

## Consumers

| Consumer | What It Uses |
|----------|--------------|
| `quality_summary_phase` | `summarize_quality()`, `QualitySummaryAgent` |
| `entropy_phase` | `ColumnQualityReport` (quality grades), `ColumnSliceProfile` |
| `graphs/context.py` | `ColumnQualityReport` (quality grades for context assembly) |

## Cleanup History (This Refactor)

| Change | Rationale |
|--------|-----------|
| Removed 12 dead exports from `__init__.py` | Only 5 symbols used externally |
| Created `config/system/quality_summary.yaml` | Extracted hardcoded thresholds to YAML |
| Removed `get_filter_config()` silent fallback | Fail fast via `load_yaml_config()` |
| Removed `MAX_BATCH_SIZE` from agent.py | Unused constant |
| Removed `max_batch_workers` parameter from `summarize_quality()` | Now loaded from YAML config |

## Roadmap

- **Vertical-specific scoring**: Quality scoring penalties could vary by domain (finance vs marketing)
- **Trend tracking**: Compare quality grades across pipeline runs to detect quality regression
- **Duplicate row detection** — Identify exact duplicate rows across all columns per slice. Fundamental data quality signal. Consider near-duplicate detection (fuzzy string similarity) for catching data entry errors.
- **Format validation metrics** — `email_format_valid_ratio`, `phone_format_valid_ratio`, `date_format_consistency`. Reuse pattern definitions from `config/system/typing.yaml`.
- **Statistical quality formatter** — LLM-interpreted formatter for Benford's Law, outlier rates, and distribution shape. Raw metrics already exist in `statistics/quality.py`; missing interpretation layer with business-context recommendations.
- **Temporal quality formatter** — LLM-interpreted formatter for seasonality, trends, gaps, staleness. Raw metrics exist in `temporal/` and `cycles/`; missing interpretation layer.
