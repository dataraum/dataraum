# Entropy

Entropy quantifies **uncertainty** in your data. Instead of binary pass/fail quality checks, DataRaum measures how much you can trust each column for a specific use case — from exploratory analysis to regulatory reporting.

Scores range from **0.0** (deterministic, fully certain) to **1.0** (maximum uncertainty).

## The Four Layers

Entropy is measured across four layers, each capturing a different kind of uncertainty.

### Structural (Schema and Relationships)

How well-defined is the data's structure?

| Detector | Dimension | What It Measures |
|----------|-----------|-----------------|
| TypeFidelityDetector | `types > type_fidelity` | Type consistency — what fraction of values fail to parse as their declared type |
| JoinPathDeterminismDetector | `relations > join_path_determinism` | Relationship ambiguity — are join paths between tables deterministic or ambiguous? |
| RelationshipEntropyDetector | `relations > relationship_quality` | Referential integrity, cardinality verification, and semantic clarity of relationships |

### Semantic (Business Meaning)

How well-documented is what the data means?

| Detector | Dimension | What It Measures |
|----------|-----------|-----------------|
| BusinessMeaningDetector | `business_meaning > naming_clarity` | Whether columns have clear descriptions and business context |
| UnitEntropyDetector | `units > unit_declaration` | Whether numeric measures have declared units (USD, kg, etc.) |
| TemporalEntropyDetector | `temporal > time_role` | Whether temporal columns are properly identified and typed |
| DimensionalEntropyDetector | `dimensional > cross_column_patterns` | Undocumented business rules between columns (mutual exclusivity, conditional dependencies) |
| DimensionCoverageDetector | `coverage > dimension_coverage` | Whether enriched views adequately cover the available dimensions |
| BusinessCycleHealthDetector | `cycles > business_cycle_health` | Whether detected business cycles have complete, healthy data |

### Value (Data Quality)

How clean and reliable are the actual values?

| Detector | Dimension | What It Measures |
|----------|-----------|-----------------|
| NullRatioDetector | `nulls > null_ratio` | Proportion of missing values |
| OutlierRateDetector | `outliers > outlier_rate` | Proportion of statistical outliers, attenuated for high-variance columns |
| TemporalDriftDetector | `temporal > temporal_drift` | Changes in data distribution over time |
| BenfordDetector | `distribution > benford_compliance` | Whether first-digit distribution follows Benford's Law (applicable to financial/count data) |
| SliceVarianceDetector | `variance > slice_stability` | Whether data quality is consistent across slices (segments) of the data |

### Computational (Aggregation Safety)

Can you safely compute on this data?

| Detector | Dimension | What It Measures |
|----------|-----------|-----------------|
| DerivedValueDetector | `derived_values > formula_match` | Whether calculated columns match their source formula |
| CrossTableConsistencyDetector | `reconciliation > cross_table_consistency` | Whether values that should agree across tables actually do |

## Score Interpretation

| Score Range | State | Meaning |
|-------------|-------|---------|
| 0.0 – 0.3 | Low | Data is reliable for this dimension |
| 0.3 – 0.6 | Medium | Investigate before using in production |
| 0.6 – 1.0 | High | Significant uncertainty — action needed |

## Contracts

Contracts define acceptable entropy thresholds for specific use cases. Different use cases tolerate different levels of uncertainty — exploratory analysis is lenient, regulatory reporting is strict.

### Built-in Contracts

| Contract | Threshold | Use Case |
|----------|-----------|----------|
| `exploratory_analysis` | 0.5 | Data exploration, hypothesis testing |
| `data_science` | 0.35 | Feature engineering, ML training |
| `operational_analytics` | 0.35 | Team dashboards, process monitoring |
| `aggregation_safe` | 0.35 | SUM/AVG/COUNT queries |
| `executive_dashboard` | 0.25 | C-level reporting, KPI tracking |
| `regulatory_reporting` | 0.1 | Financial statements, compliance, audit |

Each contract specifies per-dimension thresholds. For example, `aggregation_safe` is particularly strict on `value.nulls` and `semantic.units` because missing values and undeclared units make aggregations unreliable.

### Confidence Levels

Contract evaluation produces a traffic-light confidence level:

| Level | Meaning |
|-------|---------|
| **GREEN** | Compliant — all dimensions within thresholds |
| **YELLOW** | Compliant — but approaching thresholds (warnings) |
| **ORANGE** | Non-compliant — 1–2 blocking violations |
| **RED** | Critical — 3+ violations or blocked columns |

### Evaluating Contracts

Via MCP:
```
> Is my data aggregation safe?
> Evaluate the executive_dashboard contract
```

Via Python:
```python
from dataraum import Context

with Context("./pipeline_output") as ctx:
    result = ctx.contracts.evaluate("aggregation_safe")
    print(result["confidence_level"])  # "green", "yellow", "orange", "red"
    print(result["violations"])        # List of threshold breaches
```

Via CLI (contract evaluated during pipeline run):
```bash
dataraum run /path/to/data --contract aggregation_safe
```

## Readiness

Per-column readiness is derived from the Bayesian entropy network:

| Level | Meaning |
|-------|---------|
| **ready** | Column is reliable for the selected contract |
| **investigate** | Some dimensions have elevated uncertainty |
| **blocked** | Critical uncertainty — do not use without remediation |

Use `measure` (MCP) or `ctx.entropy.summary()` (Python) to see readiness at column, table, and dataset levels.

## Viewing Entropy

Via MCP:
```
> Show me the entropy scores
> What's the readiness for the orders table?
```

Via Python:
```python
from dataraum import Context

with Context("./pipeline_output") as ctx:
    # Overall summary
    summary = ctx.entropy.summary()

    # Per-table detail
    table_entropy = ctx.entropy.table("orders")

    # Per-column detail with evidence
    detail = ctx.entropy.details("orders", "amount")
```
