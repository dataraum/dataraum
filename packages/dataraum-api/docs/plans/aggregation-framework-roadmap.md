# Aggregation Framework Roadmap

## Vision

A hybrid aggregation architecture that balances **pre-computation** for common metrics with **on-demand flexibility** for custom analysis.

```
                     ┌─────────────────────────┐
                     │   Aggregation Layer     │
                     │                         │
  Quality Aggs ──────┤  agg_quality_* views    │
  (completeness,     │  (pre-computed records  │
   outliers, etc.)   │   + DuckDB views)       │
                     ├─────────────────────────┤
  Business Aggs ─────┤  agg_metric_* views     │
  (DSO, ratios)      │  (pre-computed global)  │
                     ├─────────────────────────┤
  Slice Aggs ────────┤  agg_slice_* views      │
  (per dimension)    │  (pre-computed common)  │
                     └──────────┬──────────────┘
                                │
                     ┌──────────▼──────────────┐
                     │   Query Layer           │
                     │  (on-demand for custom  │
                     │   slices, drill-downs)  │
                     └─────────────────────────┘
```

---

## Implementation Phases

| Phase | Focus | Output | Status |
|-------|-------|--------|--------|
| **1** | Column eligibility | `column_eligibility` table, phase | Planned |
| **2** | Quality aggregation | `quality_aggregates` records + views, API | Planned |
| **3** | Config restructure | Reorganized `config/graphs/` folder | Planned |
| **4** | Business aggregation | `agg_metric_*` views, API | Future |
| **5** | Slice framework | Formalized slice definitions | Future (external) |
| **6** | Query layer optimization | Use pre-computed where possible | Future |
| **7** | Execution persistence | `graph_executions`, `query_executions` | Future |

**Note**: Execution persistence (phase 7) is last because the architecture may evolve. No point persisting executions until the aggregation model stabilizes.

---

## Phase 1: Column Eligibility

**Goal**: Early elimination of unusable columns to save downstream compute and LLM tokens.

**Detailed spec**: [column-eligibility-quality-aggregation.md](./column-eligibility-quality-aggregation.md)

**Deliverables**:
- `ColumnEligibilityRecord` model
- `column_eligibility_phase.py`
- Config: `config/column_eligibility.yaml`
- Downstream phase updates (filter eliminated columns)

---

## Phase 2: Quality Aggregation

**Goal**: Pre-compute quality metrics for fast API access and SQL queryability.

**Detailed spec**: [column-eligibility-quality-aggregation.md](./column-eligibility-quality-aggregation.md)

**Deliverables**:
- `QualityAggregateRecord` model (metadata DB)
- `agg_quality_table` DuckDB view per table
- `agg_quality_source` DuckDB view per source
- `quality_aggregation_phase.py`
- API endpoints: `/sources/{id}/quality`, `/tables/{id}/quality`

### Dual Storage Strategy

```
┌─────────────────────────────────────────────────────────────┐
│                    Quality Aggregation                       │
├─────────────────────────────┬───────────────────────────────┤
│     Metadata Records        │       DuckDB Views            │
│     (SQLite/PostgreSQL)     │       (per source DB)         │
├─────────────────────────────┼───────────────────────────────┤
│ • QualityAggregateRecord    │ • agg_quality_{table}         │
│ • Fast API access (<50ms)   │ • SQL queryable               │
│ • Pre-computed at phase end │ • Computed on SELECT          │
│ • Snapshot in time          │ • Always current              │
│ • Used by: REST API         │ • Used by: GraphAgent, SQL    │
└─────────────────────────────┴───────────────────────────────┘
```

**Why both?**
- API clients want instant responses → records
- SQL queries want to JOIN quality data → views
- Views reference typed tables, always fresh

---

## Phase 3: Config Restructure

**Goal**: Clear separation of quality vs business graphs.

**Current structure**:
```
config/graphs/
├── filters/
│   └── rules/
│       ├── type_based.yaml
│       ├── role_based.yaml
│       ├── pattern_based.yaml
│       └── consistency.yaml
└── metrics/
    ├── dso.yaml
    ├── dpo.yaml
    ├── current_ratio.yaml
    ├── data_completeness.yaml    # Quality metric mixed in
    ├── anomaly_rate.yaml         # Quality metric mixed in
    └── data_freshness.yaml       # Quality metric mixed in
```

**Proposed structure**:
```
config/graphs/
├── quality/                      # Quality-focused graphs
│   ├── completeness/
│   │   └── null_analysis.yaml
│   ├── validity/
│   │   ├── type_compliance.yaml
│   │   └── parse_success.yaml
│   ├── consistency/
│   │   └── cross_column.yaml
│   └── freshness/
│       └── staleness.yaml
├── business/                     # Business metrics
│   ├── financial/
│   │   ├── dso.yaml
│   │   ├── dpo.yaml
│   │   └── cash_conversion_cycle.yaml
│   ├── liquidity/
│   │   └── current_ratio.yaml
│   └── operational/
│       └── (future)
├── slices/                       # Dimensional definitions
│   ├── temporal/
│   │   └── (future)
│   └── categorical/
│       └── (future)
└── filters/                      # Row-level classification
    └── rules/
        ├── type_based.yaml
        ├── role_based.yaml
        ├── pattern_based.yaml
        └── consistency.yaml
```

**Migration**:
1. Create new folder structure
2. Move existing files
3. Update `GraphLoader` to handle new paths
4. Update any hardcoded references

---

## Phase 4: Business Aggregation

**Goal**: Pre-compute common business metrics at global level.

**Deliverables**:
- `BusinessAggregateRecord` model
- `agg_metric_{metric_id}` DuckDB views
- `business_aggregation_phase.py`
- API endpoints: `/sources/{id}/metrics`, `/metrics/{metric_id}`

**Scope**:
- Global metrics only (not per-slice)
- Metrics defined in `config/graphs/business/`
- Requires filter results (clean rows only)

**Dependency**: Requires phase 3 (config restructure) to cleanly identify business graphs.

---

## Phase 5: Slice Framework

**Goal**: Formalize slice definitions and compute per-slice aggregates.

**Note**: Currently being developed externally. Integration planned after phases 1-4.

**Expected deliverables**:
- Slice definition YAML schema
- `SliceDefinitionRecord` model
- `agg_slice_{dimension}_{metric}` views
- Integration with business aggregation

---

## Phase 6: Query Layer Optimization

**Goal**: Query layer automatically uses pre-computed aggregates when available.

**Behavior**:
```python
def answer_question(question: str, ...):
    # 1. Analyze question intent
    # 2. Check if pre-computed aggregate exists
    if matches_precomputed(question, available_aggregates):
        # Use pre-computed, faster
        return fetch_aggregate(...)
    else:
        # Generate SQL on-demand
        return generate_and_execute(...)
```

**Deliverables**:
- Aggregate registry (what's pre-computed)
- Question-to-aggregate matcher
- Fallback to on-demand generation

---

## Phase 7: Execution Persistence

**Goal**: Wire up `graph_executions`, `query_executions`, `step_results` tables.

**Why last?**
- Architecture may change in phases 1-6
- No point persisting executions that will be obsolete
- Current phase-level persistence is sufficient for now

**Deliverables**:
- `GraphAgent._execute_sql()` persists `GraphExecutionRecord`
- `QueryLibrary.record_execution()` called after queries
- `StepResultRecord` persisted per step
- Execution history API endpoints

---

## Success Metrics

| Phase | Metric | Target |
|-------|--------|--------|
| 1 | Pipeline fails on bad key column | 100% |
| 1 | LLM token reduction (eliminated columns) | >10% |
| 2 | Quality API response time | <50ms |
| 2 | Quality data SQL queryable | Yes |
| 3 | Config structure matches spec | 100% |
| 4 | Business metrics pre-computed | Top 5 metrics |
| 6 | Queries using pre-computed | >50% common queries |

---

## Dependencies

```
Phase 1 ──────┐
              ├──→ Phase 2 ──→ Phase 3 ──→ Phase 4 ──→ Phase 6 ──→ Phase 7
              │                              ↑
              │                              │
              └──────────────────────────────┘
                                             │
                              Phase 5 ───────┘
                              (external)
```

- Phase 2 depends on Phase 1 (uses eligibility data)
- Phase 4 depends on Phase 3 (needs restructured config)
- Phase 5 can proceed in parallel (external)
- Phase 6 depends on Phase 4 (needs aggregates to use)
- Phase 7 waits for architecture to stabilize

---

## Open Questions

1. **View refresh strategy**: DuckDB views are always fresh, but expensive. Add materialized view option?
2. **Historical tracking**: Should quality aggregates be versioned over time for trend analysis?
3. **Custom aggregates**: Allow users to define custom aggregates via YAML?
4. **Slice granularity**: Which slice dimensions to pre-compute vs on-demand?

---

## Related Documents

- [Column Eligibility & Quality Aggregation](./column-eligibility-quality-aggregation.md) - Detailed spec for phases 1-2
- [Entropy Implementation Plan](../ENTROPY_IMPLEMENTATION_PLAN.md) - Entropy system architecture
- [Backlog](../BACKLOG.md) - Current task stack
