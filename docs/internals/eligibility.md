# Column Eligibility

## Reasoning & Summary

`analysis/eligibility/` is a gate phase that evaluates columns against configurable quality thresholds after statistical profiling. Columns that fail (e.g., 100% null, single value) are marked INELIGIBLE and dropped from typed tables — their data is preserved in quarantine tables. This prevents downstream modules from wasting compute on unusable columns.

Three statuses: **ELIGIBLE** (proceed), **WARN** (proceed with flag), **INELIGIBLE** (drop from typed table).

## Data Model

### SQLAlchemy Model

```
ColumnEligibilityRecord (column_eligibility)
├── eligibility_id: String (PK, auto-generated)
├── column_id: String (preserved for audit, no FK — survives column deletion)
├── table_id: FK → tables (CASCADE)
├── source_id: FK → sources (CASCADE)
├── column_name: String (denormalized)
├── table_name: String (denormalized)
├── resolved_type: String
├── status: String ('ELIGIBLE', 'WARN', 'INELIGIBLE')
├── triggered_rule: String (rule ID that matched)
├── reason: Text (human-readable)
├── metrics_snapshot: JSON (null_ratio, distinct_count, etc. at decision time)
├── config_version: String (for reproducibility)
└── evaluated_at: DateTime
```

Key design: record is **denormalized** (column_name, table_name stored directly) so it survives column deletion when ineligible columns are dropped.

## Rules Engine

Rules are evaluated in order from `config/system/column_eligibility.yaml`. First match wins.

Available metrics in conditions: `null_ratio`, `distinct_count`, `cardinality_ratio`, `total_count`
Available thresholds: `max_null_ratio`, `eliminate_single_value`, `warn_null_ratio`

Conditions are Python expressions evaluated via safe `eval()` with no builtins.

### Key Column Protection

Columns matching `key_patterns` (regex: `_id$`, `^id$`, `_key$`) that evaluate as INELIGIBLE cause the pipeline to **fail** rather than silently dropping a likely primary key.

## Configuration

### `config/system/column_eligibility.yaml`

```yaml
version: "1.0"

thresholds:
  max_null_ratio: 1.0           # INELIGIBLE if null_ratio >= this
  eliminate_single_value: true   # INELIGIBLE if distinct_count == 1
  warn_null_ratio: 0.5          # WARN if null_ratio > this

rules:
  - id: "all_null"
    condition: "null_ratio >= max_null_ratio"
    status: "INELIGIBLE"
    reason: "Column has {null_ratio:.0%} null values - no usable data"
  # ... more rules ...

default_status: "ELIGIBLE"

key_patterns:
  - "_id$"
  - "^id$"
  - "_key$"
```

## Roadmap / Planned Features

- **Vertical-specific rules** — Domain-specific eligibility (e.g., financial columns with different null tolerance)
