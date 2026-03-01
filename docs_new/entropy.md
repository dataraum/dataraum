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

### Value (Data Quality)

How clean and reliable are the actual values?

| Detector | Dimension | What It Measures |
|----------|-----------|-----------------|
| NullRatioDetector | `nulls > null_ratio` | Proportion of missing values |
| OutlierRateDetector | `outliers > outlier_rate` | Proportion of statistical outliers, attenuated for high-variance columns |
| TemporalDriftDetector | `temporal > temporal_drift` | Changes in data distribution over time |
| BenfordDetector | `distribution > benford_compliance` | Whether first-digit distribution follows Benford's Law (applicable to financial/count data) |

### Computational (Aggregation Safety)

Can you safely compute on this data?

| Detector | Dimension | What It Measures |
|----------|-----------|-----------------|
| DerivedValueDetector | `derived_values > formula_match` | Whether calculated columns match their source formula |

## Detector Trust

Each detector has a **trust level** that determines whether it can gate the pipeline:

| Trust | Detectors | Meaning |
|-------|-----------|---------|
| **HARD** | type_fidelity, join_path_determinism, relationship_entropy, null_ratio, outlier_rate, benford, temporal_drift, derived_value | Machine-verifiable. Can block pipeline phases via gates. |
| **SOFT** | business_meaning, unit_entropy, temporal_entropy | LLM/heuristic-derived. Inform actions but don't block phases. |

Hard detectors produce scores that are objectively measurable — you can re-run them and get the same result. Soft detectors depend on LLM interpretation and are inherently less deterministic.

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

```bash
# CLI
dataraum contracts ./pipeline_output
```

```python
# Python
from dataraum import Context

with Context("./pipeline_output") as ctx:
    result = ctx.contracts.evaluate("aggregation_safe")
    print(result["confidence_level"])  # "green", "yellow", "orange", "red"
    print(result["violations"])        # List of threshold breaches
```

Via MCP:
```
> Is my data aggregation safe?
> Evaluate the executive_dashboard contract
```

## Actions

When entropy is too high, DataRaum generates prioritized **actions** — concrete steps to improve data quality. Actions are ranked by impact-to-effort ratio and traced to their source (LLM interpretation or Bayesian network analysis).

```bash
# CLI — not yet available as standalone command
# Use MCP or Python API
```

```python
from dataraum import Context

with Context("./pipeline_output") as ctx:
    actions = ctx.actions(contract="aggregation_safe")
    for action in actions:
        print(f"[{action['priority']}] {action['description']}")
        print(f"  Affects: {action['affected_columns']}")
        print(f"  Effort: {action['effort']}")
```

Via MCP:
```
> What should I fix first?
> Show high priority actions for the orders table
```

### Action Priority

Actions are scored based on:
- **Impact**: How many columns benefit, plus causal network impact
- **Effort**: Low, medium, or high (estimated remediation work)
- **Priority score**: `impact / effort_factor` — higher means better ROI

### Fix Execution

When an action is applied (at a gate or via MCP), the `FixExecutor` runs it through a verified flow:

1. Execute the action (e.g., override a column type, declare a unit)
2. Create an immutable `Decision` record with evidence
3. Persist a `DecisionRecord` to the database for audit
4. Return a `FixResult` with `success`, `improved`, and score deltas

Available seed actions:

| Action | Category | What It Does | Hard-Verifiable? |
|--------|----------|-------------|-----------------|
| `override_type` | Transform | Change column type in TypeDecision | Yes |
| `declare_unit` | Annotate | Write unit to TypeCandidate | No |
| `add_business_name` | Annotate | Update SemanticAnnotation | No |
| `declare_null_meaning` | Annotate | Document null semantics in business_description | No |
| `confirm_relationship` | Annotate | Confirm FK relationship | Partial |
| `create_filtered_view` | Transform | Create DuckDB view excluding problematic rows | Yes |

Via MCP:
```
> Apply the override_type fix to orders.amount with target_type DECIMAL(10,2)
```

Every fix is recorded in the **decision ledger** — an append-only audit trail of who (user, auto_fix, mcp_agent) did what, when, and with what evidence.

## Viewing Entropy

```bash
# CLI — interactive TUI explorer
dataraum entropy ./pipeline_output

# Filter to a specific table
dataraum entropy ./pipeline_output --table orders
```

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

Via MCP:
```
> Show me the entropy for the orders table
> What are the entropy details for orders.amount?
```
