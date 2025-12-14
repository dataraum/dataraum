# Metrics Backlog

This document tracks metrics and features planned for future implementation. Items are organized by category and priority.

---

## Metrics to Implement

These metrics are documented or desired but not yet computed.

### Statistical Metrics

| Metric | Description | Priority | Rationale |
|--------|-------------|----------|-----------|
| `shannon_entropy` | Information content of column values | Medium | Measures predictability, useful for feature selection |
| `normalized_entropy` | Entropy normalized to [0,1] range | Medium | Enables cross-column comparison |
| `is_sorted` | Whether values are in sorted order | Low | Detects ordered sequences, index columns |
| `is_monotonic_increasing` | Strictly increasing values | Low | Time series property |
| `is_monotonic_decreasing` | Strictly decreasing values | Low | Counter patterns |
| `inversions_ratio` | Ratio of out-of-order pairs | Low | Ordering quality measure |

### Uniqueness / Integrity Metrics

| Metric | Description | Priority | Rationale |
|--------|-------------|----------|-----------|
| `duplicate_row_detection` | Identify exact duplicate rows | High | Data quality fundamental |
| `near_duplicate_detection` | Fuzzy duplicate detection | Medium | Catches data entry errors |
| `referential_integrity_check` | FK values exist in PK table | High | Orphan record detection |
| `pk_violation_count` | Duplicate primary key values | High | Constraint validation |

### Pattern / Format Validation

| Metric | Description | Priority | Rationale |
|--------|-------------|----------|-----------|
| `email_format_valid_ratio` | Ratio of valid email formats | Medium | Common data quality issue |
| `phone_format_valid_ratio` | Ratio of valid phone formats | Medium | Varies by locale |
| `date_format_consistency` | Consistency of date formats | Medium | Parsing reliability |
| `uuid_format_valid_ratio` | Ratio of valid UUIDs | Low | Technical identifier validation |

---

## Formatters to Create

LLM-optimized formatters for existing metrics that lack interpretation.

### Statistical Quality Formatter

**Target metrics:** Outliers (IQR, Isolation Forest), Benford's Law, distribution shape

**Output structure:**
```python
{
    "statistical_assessment": {
        "outlier_summary": {...},      # Interpretation of outlier detection
        "benford_summary": {...},       # Fraud/anomaly interpretation
        "distribution_summary": {...},  # Shape interpretation (normal, skewed, bimodal)
        "recommendations": [...]
    }
}
```

### Temporal Quality Formatter

**Target metrics:** Seasonality, trends, gaps, staleness, change points

**Output structure:**
```python
{
    "temporal_assessment": {
        "freshness_summary": {...},     # How current is the data
        "pattern_summary": {...},       # Seasonality, trends detected
        "completeness_summary": {...},  # Gaps in time series
        "stability_summary": {...},     # Distribution drift, change points
        "recommendations": [...]
    }
}
```

### Topological Quality Formatter

**Target metrics:** Betti numbers, persistence, cycles, fragmentation

**Output structure:**
```python
{
    "topological_assessment": {
        "connectivity_summary": {...},  # Is data graph connected
        "cycle_summary": {...},         # Circular relationships
        "complexity_summary": {...},    # Overall structural complexity
        "recommendations": [...]
    }
}
```

### Domain Quality Formatter

**Target metrics:** Rule violations, accounting checks, business logic

**Output structure:**
```python
{
    "domain_assessment": {
        "rule_compliance_summary": {...},
        "violation_details": [...],
        "severity_breakdown": {...},
        "recommendations": [...]
    }
}
```

---

## Integration Work

### API Endpoints

| Endpoint | Description | Priority |
|----------|-------------|----------|
| `GET /quality/context/{dataset_id}` | Full quality context for dataset | High |
| `GET /quality/context/{table_id}` | Quality context for single table | High |
| `GET /quality/issues` | Query issues with filters | Medium |
| `GET /quality/recommendations` | LLM-generated filter recommendations | Medium |

### MCP Tools

| Tool | Description | Priority |
|------|-------------|----------|
| `get_quality_context` | Retrieve formatted quality context | High |
| `get_quality_issues` | Query quality issues | Medium |
| `get_filter_recommendations` | Get LLM filter SQL recommendations | Medium |
| `explain_quality_metric` | Natural language explanation of a metric | Low |

### LLM Prompt Integration

| Feature | Description | Priority |
|---------|-------------|----------|
| Quality context in semantic analysis | Include quality flags when analyzing semantics | Medium |
| Quality-aware query generation | Consider data quality in suggested queries | Medium |
| Quality summary in context document | Include quality overview in ContextDocument | High |

---

## Technical Debt

Items that need cleanup or improvement.

| Item | Description | Priority |
|------|-------------|----------|
| Consolidate threshold definitions | Move hardcoded thresholds to config | Medium |
| Standardize issue severity mapping | Consistent severity across all pillars | Medium |
| Add metric lineage tracking | Track which metrics were used for which issues | Low |
| Improve evidence serialization | Standardize evidence dict structure | Low |

---

## Notes

- This backlog will be updated as new requirements emerge
- Priority levels: High (next sprint), Medium (upcoming), Low (future)
- User can add additional items during review
