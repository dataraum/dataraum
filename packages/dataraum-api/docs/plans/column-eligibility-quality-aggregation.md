# Column Eligibility & Quality Aggregation

## Problem Statement

The current pipeline processes all columns through all phases regardless of data quality. This leads to:

1. **Wasted compute**: Correlation, relationship detection, and entropy analysis on columns with no usable data (e.g., 100% null)
2. **Wasted LLM tokens**: Semantic analysis and entropy interpretation spend tokens on columns that will never be useful
3. **No pre-computed quality metrics**: Quality information is scattered across tables, requiring re-computation for each query
4. **Late failure detection**: Fundamental data problems (e.g., key column is empty) discovered late in pipeline

## Solution Overview

Two new pipeline phases:

| Phase | Position | Purpose |
|-------|----------|---------|
| **column_eligibility** | After statistics (4.5) | Early elimination of unusable columns |
| **quality_aggregation** | After quality_summary (12.5) | Pre-compute quality metrics for fast access |

### Benefits

- **Earlier failures**: Key column problems halt pipeline immediately
- **Reduced compute**: Downstream phases skip eliminated columns
- **Reduced LLM cost**: Semantic/entropy interpretation skip eliminated columns
- **Fast quality access**: Pre-computed aggregates for API and dashboards
- **Holistic quality view**: Single source of truth for data quality metrics

---

## Phase 1: Column Eligibility

### Position in Pipeline

```
staging
   ↓
profiling
   ↓
typing
   ↓
statistics
   ↓
column_eligibility  ← NEW
   ↓
correlation (filters ELIMINATE columns)
   ↓
relationships (filters ELIMINATE columns)
   ↓
semantic (filters ELIMINATE columns)
   ...
```

### Eligibility Status

| Status | Meaning | Downstream Behavior |
|--------|---------|---------------------|
| `ELIGIBLE` | Column has sufficient data quality | Process normally |
| `WARN` | Column has quality concerns | Process with flags in context |
| `ELIMINATE` | Column is unusable | Skip in downstream phases |

### Critical Column Handling

If a column with semantic role `key`, `primary_key`, or `foreign_key` is marked `ELIMINATE`:

- **Pipeline fails immediately** with clear error
- Rationale: A dataset without usable keys cannot produce reliable joins or metrics
- User must fix data or adjust expectations before proceeding

### Configuration

```yaml
# config/column_eligibility.yaml

# Version for cache invalidation
version: "1.0"

# Elimination rules - column excluded from downstream analysis
elimination_rules:
  - id: "all_null"
    condition: "null_ratio = 1.0"
    action: "ELIMINATE"
    reason: "100% null values - no usable data"

  - id: "single_value_no_variance"
    condition: "distinct_count = 1 AND null_ratio < 1.0"
    action: "ELIMINATE"
    reason: "Single non-null value - no variance for analysis"
    configurable: true  # Some users may want constants preserved

# Warning rules - column processed but flagged
warning_rules:
  - id: "high_null"
    condition: "null_ratio > 0.5"
    action: "WARN"
    reason: "High null ratio (>50%) may affect aggregation reliability"

  - id: "very_high_null"
    condition: "null_ratio > 0.8"
    action: "WARN"
    severity: "high"
    reason: "Very high null ratio (>80%) - consider if column is useful"

  - id: "low_cardinality_numeric"
    condition: "data_type IN ('INTEGER', 'BIGINT', 'DOUBLE') AND distinct_count <= 3"
    action: "WARN"
    reason: "Numeric column with ≤3 distinct values - verify if categorical"

  - id: "high_cardinality_text"
    condition: "data_type = 'VARCHAR' AND cardinality_ratio > 0.95"
    action: "WARN"
    reason: "Near-unique text (>95% unique) - likely identifier, not for aggregation"

# Columns matching these patterns are never eliminated (only warned)
protected_patterns:
  - pattern: "^_"           # System columns
  - pattern: "_at$"         # Timestamp columns
  - pattern: "_id$"         # ID columns (but can still warn)
```

### Database Model

```python
class ColumnEligibilityRecord(Base):
    """Column eligibility decision with audit trail."""

    __tablename__ = "column_eligibility"

    eligibility_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    column_id: Mapped[str] = mapped_column(String(36), ForeignKey("columns.column_id"))
    table_id: Mapped[str] = mapped_column(String(36), ForeignKey("tables.table_id"))
    source_id: Mapped[str] = mapped_column(String(36), ForeignKey("sources.source_id"))

    # Decision
    status: Mapped[str] = mapped_column(String(20))  # ELIGIBLE, WARN, ELIMINATE
    triggered_rules: Mapped[dict] = mapped_column(JSON)  # List of rule IDs
    reasons: Mapped[dict] = mapped_column(JSON)  # Human-readable reasons
    severity: Mapped[str | None] = mapped_column(String(20))  # For WARN: low, medium, high

    # Snapshot of signals at decision time (for audit)
    null_ratio: Mapped[float | None] = mapped_column(Float)
    cardinality_ratio: Mapped[float | None] = mapped_column(Float)
    distinct_count: Mapped[int | None] = mapped_column(Integer)
    data_type: Mapped[str | None] = mapped_column(String(50))

    # Config version used (for cache invalidation)
    config_version: Mapped[str] = mapped_column(String(20))

    evaluated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))

    # Relationships
    column: Mapped["Column"] = relationship(back_populates="eligibility")
```

### Phase Implementation

```python
class ColumnEligibilityPhase(BasePhase):
    """Evaluate column eligibility based on data quality signals."""

    @property
    def name(self) -> str:
        return "column_eligibility"

    @property
    def description(self) -> str:
        return "Column eligibility evaluation"

    @property
    def dependencies(self) -> list[str]:
        return ["statistics"]  # Needs null_ratio, cardinality

    @property
    def outputs(self) -> list[str]:
        return ["eligible_columns", "warned_columns", "eliminated_columns"]

    def _run(self, ctx: PhaseContext) -> PhaseResult:
        # 1. Load config
        # 2. Get typed tables and columns
        # 3. Load statistical profiles
        # 4. Evaluate each column against rules
        # 5. Check for critical column elimination (key columns)
        # 6. Persist ColumnEligibilityRecord
        # 7. Return counts

        # Critical check: fail if key column eliminated
        for col_id, status in evaluations.items():
            if status == "ELIMINATE":
                col = column_map[col_id]
                semantic = semantic_annotations.get(col_id)
                if semantic and semantic.semantic_role in ("key", "primary_key", "foreign_key"):
                    return PhaseResult.failed(
                        f"Critical column '{col.column_name}' (role: {semantic.semantic_role}) "
                        f"is ineligible: {reasons[col_id]}. "
                        f"Cannot proceed with unusable key column."
                    )
```

### Downstream Integration

Phases that should filter eliminated columns:

```python
# In correlation phase
def _run(self, ctx: PhaseContext) -> PhaseResult:
    # Get eligible columns only
    eligible_stmt = select(Column).join(ColumnEligibilityRecord).where(
        ColumnEligibilityRecord.status != "ELIMINATE",
        Column.table_id.in_(table_ids)
    )
    columns = ctx.session.execute(eligible_stmt).scalars().all()
```

---

## Phase 2: Quality Aggregation

### Position in Pipeline

```
...
validation
   ↓
quality_summary
   ↓
quality_aggregation  ← NEW
   ↓
entropy (uses aggregates)
   ↓
entropy_interpretation
...
```

### Aggregation Levels

| Level | Scope | Use Case |
|-------|-------|----------|
| `column` | Per column | Detailed drill-down |
| `table` | Per table | Table-level dashboards |
| `source` | Entire source | Overview metrics |

### Computed Aggregates

#### Table-Level

| Metric | Formula | Description |
|--------|---------|-------------|
| `total_cells` | `row_count × column_count` | Total data points |
| `null_cells` | `SUM(null_count)` | Missing data points |
| `completeness_ratio` | `1 - (null_cells / total_cells)` | Data completeness |
| `total_columns` | `COUNT(columns)` | Column count |
| `eligible_columns` | `COUNT(status = 'ELIGIBLE')` | Usable columns |
| `warned_columns` | `COUNT(status = 'WARN')` | Flagged columns |
| `eliminated_columns` | `COUNT(status = 'ELIMINATE')` | Unusable columns |
| `health_ratio` | `eligible_columns / total_columns` | Column usability |
| `typed_rows` | Row count from `typed_*` | Successfully typed |
| `quarantined_rows` | Row count from `quarantine_*` | Failed typing |
| `fidelity_ratio` | `typed / (typed + quarantined)` | Type success rate |
| `columns_with_outliers` | `COUNT(outlier_ratio > 0)` | Outlier presence |
| `avg_outlier_ratio` | `AVG(outlier_ratio)` | Average outlier rate |
| `max_outlier_ratio` | `MAX(outlier_ratio)` | Worst outlier rate |

#### Source-Level

| Metric | Formula | Description |
|--------|---------|-------------|
| `table_count` | `COUNT(tables)` | Number of tables |
| `total_rows` | `SUM(row_count)` | Total rows across tables |
| `total_columns` | `SUM(column_count)` | Total columns |
| `overall_completeness` | Weighted avg by row count | Source completeness |
| `overall_health` | Avg across tables | Source column health |
| `overall_fidelity` | Weighted avg by row count | Source type fidelity |

### Database Model

```python
class QualityAggregateRecord(Base):
    """Pre-computed quality aggregates at table or source level."""

    __tablename__ = "quality_aggregates"

    aggregate_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    source_id: Mapped[str] = mapped_column(String(36), ForeignKey("sources.source_id"))
    table_id: Mapped[str | None] = mapped_column(String(36), ForeignKey("tables.table_id"))

    level: Mapped[str] = mapped_column(String(20))  # 'table' or 'source'

    # Completeness metrics
    total_cells: Mapped[int] = mapped_column(BigInteger)
    null_cells: Mapped[int] = mapped_column(BigInteger)
    completeness_ratio: Mapped[float] = mapped_column(Float)

    # Column health metrics
    total_columns: Mapped[int] = mapped_column(Integer)
    eligible_columns: Mapped[int] = mapped_column(Integer)
    warned_columns: Mapped[int] = mapped_column(Integer)
    eliminated_columns: Mapped[int] = mapped_column(Integer)
    health_ratio: Mapped[float] = mapped_column(Float)

    # Type fidelity metrics
    typed_rows: Mapped[int] = mapped_column(BigInteger)
    quarantined_rows: Mapped[int] = mapped_column(BigInteger)
    fidelity_ratio: Mapped[float] = mapped_column(Float)

    # Outlier metrics
    columns_with_outliers: Mapped[int] = mapped_column(Integer)
    avg_outlier_ratio: Mapped[float | None] = mapped_column(Float)
    max_outlier_ratio: Mapped[float | None] = mapped_column(Float)

    # Quality grade (from quality_summary phase)
    quality_grade: Mapped[str | None] = mapped_column(String(5))  # A, B, C, D, F
    quality_score: Mapped[float | None] = mapped_column(Float)  # 0-100

    # Metadata
    computed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))

    # For source-level only
    table_count: Mapped[int | None] = mapped_column(Integer)
    total_rows: Mapped[int | None] = mapped_column(BigInteger)
```

### DuckDB Views (Dual Storage)

In addition to metadata records, create DuckDB views for SQL queryability:

```
┌─────────────────────────────────────────────────────────────┐
│                    Quality Aggregation                       │
├─────────────────────────────┬───────────────────────────────┤
│     Metadata Records        │       DuckDB Views            │
│     (SQLite/PostgreSQL)     │       (per source DB)         │
├─────────────────────────────┼───────────────────────────────┤
│ • QualityAggregateRecord    │ • agg_quality_{table_name}    │
│ • Fast API access (<50ms)   │ • SQL queryable               │
│ • Pre-computed at phase end │ • Computed on SELECT          │
│ • Snapshot in time          │ • Always current              │
│ • Used by: REST API         │ • Used by: GraphAgent, SQL    │
└─────────────────────────────┴───────────────────────────────┘
```

**Why both?**
- **API clients** want instant responses → use metadata records
- **SQL queries** want to JOIN quality data → use DuckDB views
- **Views** reference typed tables, always fresh (no staleness)

#### View Definition (per table)

```sql
-- Created by quality_aggregation phase
CREATE OR REPLACE VIEW agg_quality_{table_name} AS
SELECT
    '{table_id}' AS table_id,
    '{table_name}' AS table_name,
    (SELECT COUNT(*) FROM typed_{table_name}) AS row_count,
    {column_count} AS column_count,
    {eligible_columns} AS eligible_columns,
    {warned_columns} AS warned_columns,
    {eliminated_columns} AS eliminated_columns,
    ROUND({eligible_columns}::FLOAT / {column_count}, 4) AS health_ratio,
    -- Completeness: computed from typed table
    (SELECT COUNT(*) * {column_count} FROM typed_{table_name}) AS total_cells,
    (SELECT
        {sum_of_null_counts_expression}
     FROM typed_{table_name}
    ) AS null_cells,
    -- Quality grade from metadata
    '{quality_grade}' AS quality_grade
;
```

#### View Definition (source-level)

```sql
-- Aggregates across all table views
CREATE OR REPLACE VIEW agg_quality_source AS
SELECT
    '{source_id}' AS source_id,
    COUNT(*) AS table_count,
    SUM(row_count) AS total_rows,
    SUM(column_count) AS total_columns,
    SUM(eligible_columns) AS eligible_columns,
    SUM(eliminated_columns) AS eliminated_columns,
    ROUND(SUM(eligible_columns)::FLOAT / SUM(column_count), 4) AS overall_health,
    ROUND(1.0 - (SUM(null_cells)::FLOAT / SUM(total_cells)), 4) AS overall_completeness
FROM (
    SELECT * FROM agg_quality_table1
    UNION ALL
    SELECT * FROM agg_quality_table2
    -- ... dynamically generated
);
```

### Phase Implementation

```python
class QualityAggregationPhase(BasePhase):
    """Pre-compute quality aggregates for fast access."""

    @property
    def name(self) -> str:
        return "quality_aggregation"

    @property
    def description(self) -> str:
        return "Quality metrics aggregation"

    @property
    def dependencies(self) -> list[str]:
        return ["quality_summary", "column_eligibility"]

    @property
    def outputs(self) -> list[str]:
        return ["table_aggregates", "source_aggregate"]

    def _run(self, ctx: PhaseContext) -> PhaseResult:
        # 1. Load typed tables
        # 2. For each table:
        #    a. Count cells, nulls from statistical profiles
        #    b. Count eligibility statuses
        #    c. Get typed/quarantine row counts
        #    d. Compute outlier stats
        #    e. Get quality grade from quality_summary
        #    f. Persist table-level QualityAggregateRecord (metadata)
        #    g. Create agg_quality_{table} DuckDB view
        # 3. Compute source-level weighted averages
        # 4. Persist source-level QualityAggregateRecord (metadata)
        # 5. Create agg_quality_source DuckDB view
```

### API Integration

```python
# GET /sources/{source_id}/quality
@router.get("/sources/{source_id}/quality", response_model=QualityAggregateResponse)
def get_source_quality(source_id: str, session: SessionDep):
    """Get pre-computed quality aggregates for a source."""
    stmt = select(QualityAggregateRecord).where(
        QualityAggregateRecord.source_id == source_id,
        QualityAggregateRecord.level == "source"
    )
    aggregate = session.execute(stmt).scalar_one_or_none()
    if not aggregate:
        raise HTTPException(404, "Quality aggregates not computed")
    return QualityAggregateResponse.from_record(aggregate)

# GET /tables/{table_id}/quality
@router.get("/tables/{table_id}/quality", response_model=QualityAggregateResponse)
def get_table_quality(table_id: str, session: SessionDep):
    """Get pre-computed quality aggregates for a table."""
    # Similar implementation
```

---

## Updated Pipeline Order

```
 1. staging
 2. profiling
 3. typing
 4. statistics
 5. column_eligibility      ← NEW
 6. correlation             (filters eliminated columns)
 7. relationships           (filters eliminated columns)
 8. semantic                (filters eliminated columns - saves LLM $$)
 9. temporal
10. slicing
11. cycles
12. validation
13. quality_summary
14. quality_aggregation     ← NEW
15. entropy                 (uses quality aggregates)
16. entropy_interpretation  (filters eliminated columns - saves LLM $$)
17. context                 (includes aggregates)
18. graphs
19. query_library
20. summary
```

---

## Implementation Plan

### Step 1: Database Models

1. Create `ColumnEligibilityRecord` in `analysis/eligibility/db_models.py`
2. Create `QualityAggregateRecord` in `analysis/quality_summary/db_models.py`
3. Add Alembic migration

### Step 2: Configuration

1. Create `config/column_eligibility.yaml`
2. Add loader in `core/config.py`

### Step 3: Column Eligibility Phase

1. Create `pipeline/phases/column_eligibility_phase.py`
2. Register in phase registry
3. Add tests

### Step 4: Update Downstream Phases

1. Update `correlation_phase.py` to filter eliminated columns
2. Update `relationships_phase.py` to filter eliminated columns
3. Update `semantic_phase.py` to filter eliminated columns
4. Update `entropy_interpretation_phase.py` to filter eliminated columns

### Step 5: Quality Aggregation Phase

1. Create `pipeline/phases/quality_aggregation_phase.py`
2. Implement metadata record persistence
3. Implement DuckDB view creation (`agg_quality_*`)
4. Register in phase registry
5. Add tests

### Step 6: API Endpoints

1. Add `/sources/{id}/quality` endpoint
2. Add `/tables/{id}/quality` endpoint
3. Add `/columns/{id}/eligibility` endpoint

### Step 7: Context Integration

1. Update `graphs/context.py` to include eligibility status
2. Update `graphs/context.py` to include quality aggregates
3. Update entropy phase to use aggregates

---

## Success Criteria

1. **Pipeline fails fast** when key column is 100% null
2. **LLM token savings** measurable (compare before/after on test dataset)
3. **Quality API** returns pre-computed metrics in <50ms
4. **Eliminated columns** do not appear in semantic annotations or entropy records
5. **Quality grade** available at table and source level via single query
6. **DuckDB views** queryable via SQL (e.g., `SELECT * FROM agg_quality_source`)
7. **GraphAgent** can JOIN quality views with business data

---

## Future Considerations

- **Configurable elimination thresholds**: Per-ontology or per-source settings
- **Column resurrection**: Process to manually mark eliminated column as eligible
- **Quality trends**: Track quality aggregates over time for monitoring
- **Slice-level aggregates**: Quality metrics per slice dimension (Phase 2+)

---

## Related Documents

- [Aggregation Framework Roadmap](./aggregation-framework-roadmap.md) - Overall 7-phase roadmap
- [Entropy Implementation Plan](../ENTROPY_IMPLEMENTATION_PLAN.md) - Entropy system architecture
- [Backlog](../BACKLOG.md) - Current task stack
