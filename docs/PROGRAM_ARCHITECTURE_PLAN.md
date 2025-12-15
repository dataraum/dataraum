# Program Architecture Plan

**Created:** 2025-12-15
**Status:** Draft for Review
**Goal:** Clean data for downstream business analysis with auditable transparency on data quality

---

## Executive Summary

The DataRaum Context Engine has made significant progress in computing rich metadata (profiling, enrichment, quality assessment). However, the **integration layer** that makes this data actionable for downstream consumers (LLM agents, APIs, MCP) is incomplete and needs architectural clarity.

### Core Problem Statement

We want to:
1. **Clean data** for downstream business analysis (filtering, quality gates)
2. **Auditable transparency** on data quality (what was filtered, why, by whom)
3. **Expose context** to business analysis agents (API/MCP)

### Current State

| Layer | Status | Notes |
|-------|--------|-------|
| Staging | ✅ Complete | VARCHAR-first loading |
| Profiling | ✅ Complete | Pattern detection, type inference, statistics |
| Enrichment | ✅ Complete | Semantic, topology, temporal |
| Quality Assessment | ✅ Complete | Statistical, topological, temporal, domain |
| **Formatters** | ✅ Complete | Transform metrics → LLM-ready context |
| **Business Cycles** | ⚠️ Computed, not persisted | Detected but lost after orchestrator returns |
| **Calculation Graphs** | ⚠️ Loaded, not executed | Schema matcher exists, no execution engine |
| **Filtering** | ⚠️ Incomplete | Models exist, pipeline not integrated |
| **API/MCP** | ❌ Not started | No routes, no MCP server |
| **Integration** | ❌ Not started | No end-to-end flow |

---

## Architecture Overview

### Target Data Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ LAYER 1: DATA INGESTION & PROFILING                                         │
│                                                                              │
│   CSV/DB → Staging → Profiling → Type Resolution                            │
│                                    ↓                                         │
│                              [metadata DB]                                   │
└─────────────────────────────────────────────────────────────────────────────┘
                                     ↓
┌─────────────────────────────────────────────────────────────────────────────┐
│ LAYER 2: ENRICHMENT & QUALITY                                               │
│                                                                              │
│   Semantic → Topology → Temporal → Quality Assessment                       │
│       ↓          ↓          ↓              ↓                                │
│   [metadata DB: annotations, relationships, metrics, issues]                │
│                                                                              │
│   Business Cycle Detection → Classification (LLM) → [persist cycles]       │
└─────────────────────────────────────────────────────────────────────────────┘
                                     ↓
┌─────────────────────────────────────────────────────────────────────────────┐
│ LAYER 3: CONTEXT FORMATTING (for LLM consumption)                           │
│                                                                              │
│   quality/formatting/* → Structured output with severity, interpretation    │
│                                                                              │
│   Formatters:                                                               │
│   - statistical.py    → StatisticalQualityOutput                            │
│   - temporal.py       → TemporalQualityOutput                               │
│   - topological.py    → TopologicalQualityOutput                            │
│   - domain.py         → DomainQualityOutput                                 │
│   - multicollinearity → MulticollinearityOutput                             │
│   - business_cycles   → BusinessCyclesOutput                                │
│                                                                              │
│   quality/context.py → ColumnQualityContext, TableQualityContext,           │
│                        DatasetQualityContext                                │
└─────────────────────────────────────────────────────────────────────────────┘
                                     ↓
┌─────────────────────────────────────────────────────────────────────────────┐
│ LAYER 4: CALCULATION GRAPHS & SCHEMA MAPPING                                │
│                                                                              │
│   Calculation Graphs (YAML)        Schema Mapping (LLM)                     │
│   ─────────────────────           ──────────────────────                    │
│   DSO: revenue, AR                 revenue → transactions.amount            │
│   Cash Runway: cash, expenses      AR → ledger.ar_balance                   │
│   OCF: net_income, adjustments                                              │
│                                                                              │
│   Graph defines WHAT is needed + aggregation rules                          │
│   Mapping provides WHERE to find it                                         │
└─────────────────────────────────────────────────────────────────────────────┘
                                     ↓
┌─────────────────────────────────────────────────────────────────────────────┐
│ LAYER 5: FILTER GENERATION & EXECUTION                                      │
│                                                                              │
│   Input Context:                                                            │
│   - Quality metrics (formatted)                                             │
│   - Business cycles (which process?)                                        │
│   - Calculation mapping (downstream impact)                                 │
│                                                                              │
│   LLM generates:                                                            │
│   - Scope filters (row selection for calculations)                          │
│   - Quality filters (data cleaning)                                         │
│   - Flags (issues that can't be filtered)                                   │
│                                                                              │
│   User rules (config/filtering/*.yaml) merged with OVERRIDE/EXTEND/SUGGEST  │
│                                                                              │
│   Executor creates:                                                         │
│   - typed_{table}_clean view                                                │
│   - quality_quarantine_{table} table                                        │
│                                                                              │
│   Persist: FilteringRecommendations + FilteringResult                       │
└─────────────────────────────────────────────────────────────────────────────┘
                                     ↓
┌─────────────────────────────────────────────────────────────────────────────┐
│ LAYER 6: CALCULATION EXECUTION                                              │
│                                                                              │
│   Clean views + Schema mapping + Graph aggregation rules                    │
│   → Execute calculation SQL                                                 │
│   → DSO = 34.5 days, Cash Runway = 8.2 months, etc.                        │
│                                                                              │
│   Persist: CalculationResult with lineage                                   │
└─────────────────────────────────────────────────────────────────────────────┘
                                     ↓
┌─────────────────────────────────────────────────────────────────────────────┐
│ LAYER 7: EXPOSURE (API + MCP)                                               │
│                                                                              │
│   FastAPI routes:                      MCP Tools:                           │
│   - GET /datasets/{id}/quality         - get_quality_context                │
│   - GET /tables/{id}/context           - get_table_context                  │
│   - GET /columns/{id}/issues           - query_data                         │
│   - GET /calculations/{id}/result      - get_calculation_result             │
│   - POST /filters/generate             - generate_filters                   │
│   - POST /filters/execute              - execute_filters                    │
│                                                                              │
│   Output: JSON context documents for LLM prompt injection                   │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Identified Gaps & Sub-Projects

### GAP 1: Business Cycles Not Persisted

**Problem:** `analyze_multi_table_financial_quality()` computes `classified_cycles` but doesn't persist them.

**Solution:**
1. Add `BusinessCycleClassification` DB model
2. Persist after LLM classification in orchestrator
3. Query in filter agent like other quality data

**Files:**
- `storage/models_v2/domain_quality.py` - Add model
- `quality/domains/financial_orchestrator.py` - Persist results

**Effort:** Small

---

### GAP 2: Calculation Graphs Need Execution Engine

**Problem:** Calculation graphs define formulas but there's no engine to execute them.

**Current state:**
- `calculations/graphs.py` - Loads YAML, extracts fields
- `calculations/matcher.py` - LLM maps columns to fields
- `calculations/mapping.py` - Stores mappings

**Missing:**
- Execution engine that reads clean views + applies aggregations
- Result persistence with lineage

**Design Questions:**
1. Should calculation graphs also define FILTER rules? (replace `config/filtering/`)
2. How to handle multi-step aggregations (Level 1 → Level 2 → Level 3)?
3. Should each calculation produce a materialized view?

**Proposed Architecture:**
```
CalculationEngine:
  - load_graph(graph_id) → CalculationGraph
  - resolve_dependencies(graph, schema_mapping) → ExecutionPlan
  - execute(plan, clean_views) → CalculationResult

ExecutionPlan:
  - ordered_steps: List[ExecutionStep]
  - each step: SQL query + expected output

CalculationResult:
  - calculation_id: str
  - value: float
  - executed_at: datetime
  - lineage: {step → SQL → rows_processed}
```

**Files to create:**
- `calculations/engine.py` - Execution engine
- `calculations/executor.py` - SQL generation from graph
- `storage/models_v2/calculation_result.py` - Result persistence

**Effort:** Medium-Large (needs design)

---

### GAP 3: Filtering Architecture Needs Redesign

**Problem:** Current filtering is confused about scope vs quality, doesn't integrate with calculations.

**Current state:**
- `filtering/models.py` - Extended with scope/quality/flags
- `filtering/llm_filter_agent.py` - Accepts context, generates filters
- `filtering/executor.py` - Creates views, uses only `clean_view_filters`
- `filtering/rules_merger.py` - Loses new structured fields
- `config/filtering/default.yaml` - User rules

**Issues:**
1. Executor doesn't distinguish scope vs quality filters
2. Rules merger doesn't preserve new fields
3. No persistence of recommendations
4. Not integrated with calculation graphs

**Proposed Architecture:**
```
FilteringPipeline:
  Step 1: Generate quality filters (LLM + quality context)
          → Clean data, remove invalid rows
          → Persist: QualityFilterResult

  Step 2: For each calculation graph:
          - Generate scope filters (LLM + graph context)
          - Apply to clean data
          - Persist: ScopeFilterResult

  Step 3: Merge with user rules (OVERRIDE/EXTEND/SUGGEST)
          → Final filter set per calculation

  Step 4: Execute
          → typed_{table}_quality_clean (quality filters only)
          → typed_{table}_{calc}_scope (quality + scope for specific calc)
```

**Alternative:** Define scope filters IN calculation graphs (like validations):
```yaml
# In dso_calculation_graph.yaml
dependencies:
  revenue:
    scope_filters:
      - condition: "transaction_type IN ('sale', 'revenue')"
        reason: "DSO only considers revenue transactions"
```

**Files to modify:**
- `filtering/executor.py` - Create separate scope views
- `filtering/rules_merger.py` - Preserve structured fields
- `storage/models_v2/filtering.py` - NEW: persistence models

**Effort:** Medium

---

### GAP 4: No API/MCP Layer

**Problem:** No way to expose context to downstream systems.

**Current state:** Nothing exists.

**Proposed Architecture:**
```
api/
  routes/
    quality.py      - GET /quality/tables/{id}, /quality/columns/{id}
    context.py      - GET /context/dataset/{id}, /context/table/{id}
    filtering.py    - POST /filtering/generate, /filtering/execute
    calculations.py - POST /calculations/execute, GET /calculations/{id}
  models/
    responses.py    - Pydantic response models
  main.py           - FastAPI app

mcp/
  server.py         - MCP server setup
  tools/
    quality.py      - get_quality_context tool
    query.py        - query_data tool
    filtering.py    - generate_filters, execute_filters tools
```

**Effort:** Medium

---

### GAP 5: Technical Debt / Cleanup

**Problem:** Iteration has left unused code, duplicate configs, unclear responsibilities.

**Areas to audit:**
1. **config/** - Multiple prompt files, possible duplicates
2. **storage/models_v2/** - Unused models? Missing indexes?
3. **quality/** - Duplicate formatters in enrichment/ vs quality/formatting/?
4. **calculations/** - `AggregationDefinition` in mapping vs in graphs

**Cleanup tasks:**
- [ ] Audit config/prompts/ - consolidate duplicates
- [ ] Audit storage/models_v2/ - verify all models have usage
- [ ] Move remaining formatters from enrichment/ to quality/formatting/
- [ ] Clarify aggregation responsibility (graph only)
- [ ] Remove `config/prompts/filter_recommendations.yaml` if unused

**Effort:** Small-Medium

---

### GAP 6: End-to-End Integration Test

**Problem:** No way to verify the full pipeline works.

**Solution:**
1. Create `examples/run_pipeline.py` - Full pipeline script
2. Use `examples/finance_csv_example/` data
3. Persist results to `examples/output/` (gitignored)
4. Verify: profiles → enrichment → quality → filters → clean views

**Files:**
- `examples/run_pipeline.py` - Main script
- `examples/output/` - Generated DBs (gitignored)

**Effort:** Medium

---

## Recommended Execution Order

### Phase A: Stabilization (fix what exists)

| # | Task | Effort | Dependency |
|---|------|--------|------------|
| A1 | Persist business cycles in DB | Small | None |
| A2 | Fix rules_merger to preserve new fields | Small | None |
| A3 | Add filtering persistence models | Small | None |
| A4 | Create end-to-end test script | Medium | A1-A3 |

### Phase B: Calculation Engine Design

| # | Task | Effort | Dependency |
|---|------|--------|------------|
| B1 | Deep-dive calculation graphs structure | Small | None |
| B2 | Design decision: scope filters in graphs? | Small | B1 |
| B3 | Design CalculationEngine architecture | Medium | B2 |
| B4 | Implement engine + executor | Large | B3 |

### Phase C: Filtering Redesign

| # | Task | Effort | Dependency |
|---|------|--------|------------|
| C1 | Decide: separate quality/scope views? | Small | B2 |
| C2 | Update executor for scope/quality split | Medium | C1 |
| C3 | Integrate with calculation graphs | Medium | B4, C2 |

### Phase D: Exposure Layer

| # | Task | Effort | Dependency |
|---|------|--------|------------|
| D1 | Design API routes | Small | None |
| D2 | Implement FastAPI routes | Medium | A1-A3 |
| D3 | Design MCP tools | Small | D1 |
| D4 | Implement MCP server | Medium | D2 |

### Phase E: Cleanup

| # | Task | Effort | Dependency |
|---|------|--------|------------|
| E1 | Audit and clean config/ | Small | D4 |
| E2 | Audit and clean storage/ | Small | D4 |
| E3 | Consolidate formatters | Small | D4 |
| E4 | Documentation update | Medium | E1-E3 |

---

## Open Design Questions

### Q1: Should scope filters be defined in calculation graphs?

**Option A:** Yes, in graphs
```yaml
# cash_runway_graph.yaml
dependencies:
  revenue:
    scope_filters:
      - condition: "type = 'sale'"
```
- Pro: Single source of truth per calculation
- Con: Mixing concerns (calculation logic + data selection)

**Option B:** No, separate filter config
- Pro: Separation of concerns
- Con: Two places to look, can drift

**Recommendation:** Option A - scope filters are calculation-specific, belong with calculation definition.

### Q2: Should we have separate views per calculation?

**Option A:** Yes
- `typed_sales_quality_clean` (quality filters)
- `typed_sales_dso_scope` (quality + DSO scope)
- `typed_sales_cash_runway_scope` (quality + cash runway scope)

**Option B:** No, dynamic filtering
- Apply scope filters at query time
- No materialized scope views

**Recommendation:** Start with Option B (dynamic), materialize only if performance requires.

### Q3: How to handle multi-step calculation aggregations?

Cash runway example:
1. Level 1: Get `historical_revenue` (monthly_values, last 3 months)
2. Level 2: Compute `average_monthly_revenue` = AVG(historical_revenue)
3. Level 3: Compute `burn_rate` = expenses - revenue
4. Level 4: Compute `runway` = cash / burn_rate

**Options:**
- A: Generate single complex SQL
- B: Create CTEs for each level
- C: Create temp tables/views for each level

**Recommendation:** Option B (CTEs) - readable, debuggable, single transaction.

---

## Success Metrics

1. **End-to-end test passes** - Pipeline runs from CSV to clean views
2. **Business cycles persisted** - Query from DB works
3. **Filters persisted** - Full audit trail
4. **At least 2 calculations execute** - DSO + Cash Runway
5. **API serves context** - GET /quality/tables/{id} returns formatted context
6. **MCP tools work** - get_quality_context callable from Claude Desktop

---

## File Organization Summary

```
src/dataraum_context/
├── calculations/
│   ├── graphs.py          # Graph loading (EXISTS)
│   ├── mapping.py         # Schema mapping models (EXISTS)
│   ├── matcher.py         # LLM schema matcher (EXISTS)
│   ├── engine.py          # NEW: Execution engine
│   └── executor.py        # NEW: SQL generation
├── quality/
│   ├── formatting/        # EXISTS: All formatters
│   ├── filtering/         # EXISTS: Filter models + agent
│   │   ├── models.py      # MODIFIED: Persistence models
│   │   ├── executor.py    # MODIFIED: Scope/quality split
│   │   └── rules_merger.py # MODIFIED: Preserve fields
│   └── context.py         # EXISTS: Context assembly
├── storage/
│   └── models_v2/
│       ├── domain_quality.py  # MODIFIED: BusinessCycleClassification
│       ├── filtering.py       # NEW: FilteringResult persistence
│       └── calculation.py     # NEW: CalculationResult persistence
├── api/                   # NEW: FastAPI routes
│   ├── routes/
│   └── main.py
└── mcp/                   # NEW: MCP server
    ├── server.py
    └── tools/

config/
├── calculation_graphs/    # EXISTS: DSO, Cash Runway, OCF
├── filtering/             # EXISTS: User rules (may deprecate)
├── prompts/               # AUDIT: Consolidate duplicates
└── formatter_thresholds/  # EXISTS: Keep

examples/
├── finance_csv_example/   # EXISTS: Test data
├── run_pipeline.py        # NEW: End-to-end script
└── output/                # NEW: Generated DBs (gitignored)
```

---

## Next Steps

1. Review this plan - identify disagreements or missing pieces
2. Decide on open design questions (Q1, Q2, Q3)
3. Start with Phase A (Stabilization) - small wins, test coverage
4. Design Phase B in detail before implementation
