# Column Eligibility Phase

## Problem Statement

The current pipeline processes all columns regardless of data quality. Columns with no usable data (e.g., 100% null) waste compute in downstream phases and can produce misleading results.

## Solution

A new `column_eligibility` phase that:
1. Evaluates each column against configurable quality thresholds
2. **Drops ineligible columns from typed tables** (moves to quarantine)
3. Records eligibility decisions for monitoring and audit
4. Fails fast if critical columns (keys) are ineligible

### Key Design Decision

**Physical removal** instead of soft filtering:
- Ineligible columns are dropped from `typed_*` tables
- Column data preserved in `quarantine_columns_*` table for review
- Downstream phases automatically work with clean data (no filtering needed)

---

## Position in Pipeline

```
staging
   ↓
profiling
   ↓
typing
   ↓
statistics
   ↓
column_eligibility  ← NEW (drops ineligible columns from typed_*)
   ↓
correlation (already clean)
   ↓
relationships (already clean)
   ↓
semantic (already clean - saves LLM $$)
   ...
```

**Why here?**
- After `statistics`: Has null_ratio, cardinality, distinct_count
- Before `correlation`: No wasted compute on dead columns
- Before `semantic`: No wasted LLM tokens on dead columns

---

## Eligibility Status

| Status | Meaning | Action |
|--------|---------|--------|
| `ELIGIBLE` | Column has sufficient data quality | Keep in typed table |
| `WARN` | Column has quality concerns | Keep but flag for review |
| `INELIGIBLE` | Column is unusable | Drop from typed, move to quarantine |

---

## Configuration

```yaml
# config/column_eligibility.yaml

version: "1.0"

# Thresholds for eligibility evaluation
thresholds:
  # Column is INELIGIBLE if null_ratio exceeds this
  max_null_ratio: 1.0  # Start conservative: only 100% null

  # Column is INELIGIBLE if it has single value (no variance)
  # Set to false to keep constant columns
  eliminate_single_value: true

  # Column gets WARN status if null_ratio exceeds this
  warn_null_ratio: 0.5

# Metrics used for evaluation (extensible)
metrics:
  - name: "null_ratio"
    source: "statistical_profile"
    field: "null_ratio"

  - name: "distinct_count"
    source: "statistical_profile"
    field: "distinct_count"

  - name: "cardinality_ratio"
    source: "statistical_profile"
    field: "cardinality_ratio"

# Rules evaluated in order, first match wins
rules:
  # Elimination rules
  - id: "all_null"
    condition: "null_ratio >= ${thresholds.max_null_ratio}"
    status: "INELIGIBLE"
    reason: "Column has {null_ratio:.0%} null values - no usable data"

  - id: "single_value"
    condition: "distinct_count == 1 AND ${thresholds.eliminate_single_value}"
    status: "INELIGIBLE"
    reason: "Column has single value '{sample_value}' - no variance"

  # Warning rules
  - id: "high_null"
    condition: "null_ratio > ${thresholds.warn_null_ratio}"
    status: "WARN"
    reason: "High null ratio ({null_ratio:.0%}) may affect analysis"

  - id: "near_constant"
    condition: "cardinality_ratio < 0.01 AND distinct_count <= 3"
    status: "WARN"
    reason: "Near-constant column with only {distinct_count} distinct values"

# Default status if no rules match
default_status: "ELIGIBLE"
```

---

## Critical Column Handling

If a column with semantic role `key`, `primary_key`, or `foreign_key` is marked `INELIGIBLE`:

- **Pipeline fails immediately** with clear error
- Rationale: A dataset without usable keys cannot produce reliable joins or metrics
- User must fix source data before proceeding

**Note**: Semantic roles are assigned in the `semantic` phase which runs *after* eligibility. For the initial implementation, we check column names matching common key patterns (`*_id`, `id`, `*_key`). Full semantic role checking can be added once we have a pre-semantic role detection or config override.

---

## Database Model

```python
class ColumnEligibilityRecord(Base):
    """Column eligibility decision with audit trail."""

    __tablename__ = "column_eligibility"

    eligibility_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    column_id: Mapped[str] = mapped_column(String(36), ForeignKey("columns.column_id"))
    table_id: Mapped[str] = mapped_column(String(36), ForeignKey("tables.table_id"))
    source_id: Mapped[str] = mapped_column(String(36), ForeignKey("sources.source_id"))

    # Decision
    status: Mapped[str] = mapped_column(String(20))  # ELIGIBLE, WARN, INELIGIBLE
    triggered_rule: Mapped[str | None] = mapped_column(String(50))  # Rule ID that matched
    reason: Mapped[str | None] = mapped_column(Text)  # Human-readable reason

    # Snapshot of metrics at decision time (for audit/debugging)
    metrics_snapshot: Mapped[dict] = mapped_column(JSON)  # {null_ratio, distinct_count, ...}

    # Config version used (for reproducibility)
    config_version: Mapped[str] = mapped_column(String(20))

    evaluated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
```

---

## Quarantine Schema

Ineligible columns are moved to a separate quarantine table per source table:

```sql
-- Created by column_eligibility phase
CREATE TABLE quarantine_columns_{table_name} (
    _row_id INTEGER,              -- Original row position
    _column_name VARCHAR,         -- Which column this data came from
    _value VARCHAR,               -- The original value
    _quarantine_reason VARCHAR,   -- Why it was quarantined
    _quarantined_at TIMESTAMP
);
```

**Example**: If `typed_orders` has column `legacy_code` that is 100% null:
1. Drop `legacy_code` from `typed_orders`
2. Insert into `quarantine_columns_orders`: all rows with `_column_name='legacy_code'`

This preserves the data for potential recovery while keeping typed tables clean.

---

## Phase Implementation

```python
class ColumnEligibilityPhase(BasePhase):
    """Evaluate column eligibility and drop ineligible columns."""

    @property
    def name(self) -> str:
        return "column_eligibility"

    @property
    def description(self) -> str:
        return "Column eligibility evaluation"

    @property
    def dependencies(self) -> list[str]:
        return ["statistics"]

    @property
    def outputs(self) -> list[str]:
        return ["eligible", "warned", "dropped"]

    def _run(self, ctx: PhaseContext) -> PhaseResult:
        # 1. Load config
        config = load_eligibility_config()

        # 2. Get typed tables and columns with statistics
        typed_tables = get_typed_tables(ctx)

        for table in typed_tables:
            columns_to_drop = []

            for column in table.columns:
                # 3. Load metrics for column
                metrics = get_column_metrics(ctx, column)

                # 4. Evaluate against rules
                status, rule_id, reason = evaluate_rules(config, metrics)

                # 5. Check if critical column (key pattern)
                if status == "INELIGIBLE" and is_likely_key(column.column_name):
                    return PhaseResult.failed(
                        f"Critical column '{column.column_name}' is ineligible: {reason}"
                    )

                # 6. Record decision
                record = ColumnEligibilityRecord(
                    eligibility_id=str(uuid4()),
                    column_id=column.column_id,
                    table_id=table.table_id,
                    source_id=ctx.source_id,
                    status=status,
                    triggered_rule=rule_id,
                    reason=reason,
                    metrics_snapshot=metrics,
                    config_version=config.version,
                    evaluated_at=datetime.now(UTC),
                )
                ctx.session.add(record)

                if status == "INELIGIBLE":
                    columns_to_drop.append(column)

            # 7. Drop ineligible columns from DuckDB
            if columns_to_drop:
                quarantine_and_drop_columns(
                    ctx.duckdb_conn,
                    table.duckdb_path,
                    columns_to_drop,
                )

                # 8. Update column metadata (mark as dropped)
                for col in columns_to_drop:
                    col.is_dropped = True  # New field on Column model

        return PhaseResult.success(
            outputs={
                "eligible": count_by_status["ELIGIBLE"],
                "warned": count_by_status["WARN"],
                "dropped": count_by_status["INELIGIBLE"],
            },
            records_processed=total_columns,
            records_created=total_columns,  # One eligibility record per column
        )
```

### Helper: Quarantine and Drop

```python
def quarantine_and_drop_columns(
    conn: duckdb.DuckDBPyConnection,
    typed_table: str,
    columns: list[Column],
) -> None:
    """Move column data to quarantine and drop from typed table."""

    base_name = typed_table.replace("typed_", "")
    quarantine_table = f"quarantine_columns_{base_name}"

    # Create quarantine table if not exists
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS "{quarantine_table}" (
            _row_id INTEGER,
            _column_name VARCHAR,
            _value VARCHAR,
            _quarantine_reason VARCHAR,
            _quarantined_at TIMESTAMP
        )
    """)

    # For each column, insert data then drop
    for col in columns:
        # Insert column data into quarantine
        conn.execute(f"""
            INSERT INTO "{quarantine_table}"
            SELECT
                ROW_NUMBER() OVER () as _row_id,
                '{col.column_name}' as _column_name,
                CAST("{col.column_name}" AS VARCHAR) as _value,
                '{col.eligibility_reason}' as _quarantine_reason,
                CURRENT_TIMESTAMP as _quarantined_at
            FROM "{typed_table}"
        """)

        # Drop column from typed table
        conn.execute(f"""
            ALTER TABLE "{typed_table}" DROP COLUMN "{col.column_name}"
        """)
```

---

## Implementation Steps

### Step 1: Database Model
1. Create `ColumnEligibilityRecord` in new `analysis/eligibility/db_models.py`
2. Add `is_dropped: bool` field to `Column` model
3. Add Alembic migration

### Step 2: Configuration
1. Create `config/column_eligibility.yaml`
2. Create config loader in `analysis/eligibility/config.py`

### Step 3: Phase Implementation
1. Create `pipeline/phases/column_eligibility_phase.py`
2. Implement rule evaluation logic
3. Implement quarantine and drop logic
4. Register in phase registry

### Step 4: Testing
1. Unit tests for rule evaluation
2. Integration test: column actually dropped from DuckDB
3. Integration test: pipeline fails on key column
4. Test with various threshold configurations

---

## Success Criteria

1. **100% null columns dropped** from typed tables
2. **Quarantine preserves data** for potential recovery
3. **Pipeline fails** when likely key column is ineligible
4. **Configurable thresholds** work as expected
5. **Eligibility records** created for all columns (audit trail)
6. **Downstream phases** see clean typed tables (no filtering needed)

---

## Monitoring

The `column_eligibility` table provides monitoring data:

```sql
-- Columns dropped per source
SELECT source_id, COUNT(*) as dropped_count
FROM column_eligibility
WHERE status = 'INELIGIBLE'
GROUP BY source_id;

-- Most common drop reasons
SELECT triggered_rule, reason, COUNT(*)
FROM column_eligibility
WHERE status = 'INELIGIBLE'
GROUP BY triggered_rule, reason
ORDER BY COUNT(*) DESC;

-- Warned columns that may need attention
SELECT c.column_name, t.table_name, ce.reason
FROM column_eligibility ce
JOIN columns c ON ce.column_id = c.column_id
JOIN tables t ON ce.table_id = t.table_id
WHERE ce.status = 'WARN';
```

---

## Future Considerations (Backlog)

- **Quality aggregation phase**: Pre-compute quality metrics (see roadmap)
- **Config restructure**: Separate quality vs business graphs
- **Business aggregation**: Pre-compute metrics
- **Threshold tuning**: Learn optimal thresholds from data patterns
- **Column resurrection**: API to restore quarantined columns

---

## Related Documents

- [Aggregation Framework Roadmap](./aggregation-framework-roadmap.md) - Overall direction (phases 2-7 in backlog)
