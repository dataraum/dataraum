# Agent Refactoring Plan

> Master plan for streamlining the agent architecture around vertical configuration.
> Each phase leaves the codebase green and shippable. No half-done states.

**Created:** 2026-02-24
**Updated:** 2026-02-25 (revised Phase D: shared infra extraction, not merge; added temporal_behavior gap; added Phase 0.6 E2E tests)
**Branch:** `refactor/streamline`
**Related:** [BACKLOG.md](../BACKLOG.md), [PROGRESS.md](../PROGRESS.md)

---

## Problem Statement

The four LLM agents (business cycles, validation, graph, query) grew independently.
They share infrastructure but diverged in how they load context, reference vertical
configuration, and surface results. Key issues:

1. **Vertical is hardcoded** — Only semantic agent abstracts the vertical name. GraphLoader,
   cycle config, validation config all hardwire `verticals/finance/`.
2. **Context loading is inconsistent** — Each agent builds its own context differently.
   Some load from DB objects, some from raw relationships, some skip data entirely.
3. **Agents don't use the metadata they have** — The pipeline now produces slice definitions,
   statistical profiles, temporal profiles, enriched views, quality grades, and entropy
   interpretations. Neither the cycle nor validation agent uses any of these.
4. **Cycle agent rediscovers what's already known** — Its exploration tools re-query
   data that's already in statistical profiles and slice definitions. The tool loop
   (3-15 LLM round trips) is unnecessary when context is rich enough.
5. **Validation results are a dead end** — Written to DB, never consumed downstream.
6. **Business cycle richness is lost** — Stages, entity flows, health observations
   discarded when loaded into GraphExecutionContext.
7. **Quality metrics conflated with business metrics** — `metrics/quality/` serves the
   entropy layer but lives alongside DSO/current_ratio and is executed by the graph agent.
8. **Graph + Query agents are near-duplicates** — Same SQL generation, execution, repair,
   assumption tracking, library storage. Different input format.

## Key Insight: Tools vs Context

The cycle agent's exploration tools exist because the original context was thin.
Now the pipeline produces rich pre-computed metadata that makes the tools redundant:

| What LLM needs | Old approach (tool calls) | New approach (in context) |
|---|---|---|
| Status column values | `get_column_value_distribution` → SQL | Slice definitions: already identified with LLM confidence |
| Completion rates | `get_cycle_completion_metrics` → 4 SQL queries | Computable from slice counts in context |
| Entity flows | `get_entity_transaction_flow` → SQL | Enriched views: pre-joined tables |
| Value distributions | `get_column_value_distribution` → SQL | Statistical profiles: `top_values` pre-computed |
| Date ranges | *(not available)* | Temporal profiles: min/max, granularity, gaps |
| Data quality | *(not available)* | Quality grades: A/B/C per column with findings |

**Decision: Eliminate exploration tools. The cycle agent becomes a single LLM call
with structured output (like the validation agent already is).**

## Sequencing Principles

- **Rewrite cycle agent, fix validation agent in-place.** Cycle changes are too deep
  for in-place editing (~73% of code replaced/deleted). Validation changes are surgical.
- **Each step = one commit.** If interrupted, codebase is green. Plan file says where to resume.
- **Tests are the checkpoint.** `pytest --testmon tests -q` must pass after every step.
- **Deep dive before coding.** Phase A starts with audit, not changes.
- **_legacy/ as reference.** Old cycle agent files moved to `_legacy/` during rewrite,
  deleted once tests pass.

---

## Phase A: Fix + Modernize Agent Context

**Goal:** Both agents load the right context from the right sources. Cycle agent
rewritten as single-call synthesis. Validation agent fixed in-place.

### A0. Audit & Document Current State ✅

> **Completed 2026-02-24.** See A0 Findings below.
> Deep evaluation of agent approaches vs available metadata also completed.

### A1. Rewrite Business Cycle Agent (context → synthesis)

**Strategy:** Move `context.py`, `agent.py`, `tools.py` to `analysis/cycles/_legacy/`.
Rewrite from scratch using pre-computed pipeline metadata. Keep `config.py`,
`models.py`, `db_models.py` unchanged. Delete `_legacy/` when tests pass.

#### A1a. New context builder (`context.py`)

Rewrite `build_cycle_detection_context()` to load from all available metadata:

| Data Source | DB Model / Table | What it provides | Example from test data |
|---|---|---|---|
| Slice definitions | `SliceDefinition` | Pre-identified status columns with values + counts | `invoices.status`: paid, open, cancelled, overdue, partial (conf 0.95) |
| Statistical profiles | `StatisticalProfile` | Top values, null rates, distinct counts, distributions | `bank_transactions.counterparty`: 28 distinct, top = "GlobalTech" (12.5%) |
| Temporal profiles | `TemporalColumnProfile` | Date ranges, granularity, gaps, staleness | `invoices.date`: 2025-01-01 to 2025-12-28, irregular, 1.0 completeness |
| Quality reports | `ColumnQualityReport` | Quality grades (A/B/C), key findings, anomalies | `journal_lines.credit`: grade C, Benford violation in CC400 |
| Enriched views | `EnrichedView` + DuckDB `enriched_*` | Pre-joined table schemas | `enriched_invoices`: invoices + payments columns |
| Semantic annotations | `SemanticAnnotation` | business_concept, semantic_role, entity_type | `invoices.amount` → concept=accounts_payable, role=measure |
| Entity classifications | `TableEntity` | Fact vs dimension, grain columns | `invoices`: fact table, grain=invoice_id |
| Relationships | `Relationship` (detection_method="llm" only) | Confirmed FK/hierarchy relationships | `payments.invoice_id → invoices.invoice_id` (FK, conf 0.95) |
| Graph topology | computed from relationships | Hub tables, star/snowflake patterns | chart_of_accounts is hub (3 incoming FKs) |
| Domain vocabulary | `config/verticals/{vertical}/cycles.yaml` | Cycle type definitions, completion indicators | order_to_cash, procure_to_pay definitions |

New `format_context_for_prompt()` organizes this as:
1. Domain vocabulary (reference framework)
2. Dataset summary (tables, facts/dimensions, row counts)
3. Pre-identified cycle indicators (slice definitions = status columns)
4. Enriched views (pre-joined tables the LLM can reason about)
5. Relationships (LLM-confirmed only)
6. Temporal patterns (date ranges, granularity per temporal column)
7. Quality signals (grades, anomalies)
8. Column semantics by table (with business_concept)

#### A1b. New agent (`agent.py`)

Rewrite `BusinessCycleAgent.analyze()` as single-call structured output:

- **No tool loop.** Single `ConversationRequest` → structured output via `submit_analysis` tool.
- **No `_execute_tool`.** Deleted (no exploration tools).
- **No `_parse_response` fallback.** Single parsing path via `_parse_tool_output`.
- **New system prompt** focused on synthesis: "Here are the pre-identified status columns,
  confirmed relationships, enriched views, and temporal patterns. Synthesize this into
  business cycle analysis."
- **`tools.py` deleted entirely.** Only the `submit_analysis` Pydantic schema stays
  (moved into agent.py or models.py as tool definition).

Files affected:
- `context.py` → rewrite (new data sources)
- `agent.py` → rewrite (single-call, no tool loop)
- `tools.py` → delete (tools eliminated)
- `config.py` → keep (vocabulary loading)
- `models.py` → keep (output models unchanged — `BusinessCycleAnalysisOutput` still used as tool schema)
- `db_models.py` → keep (persistence unchanged)

### A2. Fix Validation Agent (in-place)

**Strategy:** Surgical edits to `resolver.py` and `agent.py`. Structure stays.

#### A2a. Enrich resolver context (`resolver.py`)

1. **Add detection_method filter** — `Relationship.detection_method == "llm"` on line 78-83
2. **Add business_concept + business_description** — to `_format_table_schema` column output (line 126-134)
3. **Add row counts per table** — DuckDB `SELECT COUNT(*)` per table, added to schema dict
4. **Add enriched view schemas** — Query `EnrichedView` table, include available pre-joined views
5. **Update prompt formatter** — `format_multi_table_schema_for_prompt` includes new fields

Tangible change to schema XML:
```xml
<!-- Before -->
<column name="amount" type="DECIMAL" role="measure" entity="transaction_amount" business_name="Amount" />

<!-- After -->
<column name="amount" type="DECIMAL" role="measure" entity="transaction_amount"
        business_name="Amount" business_concept="accounts_payable"
        description="Total invoice amount in local currency" />
```

#### A2b. Fix agent execution (`agent.py`)

1. **Replace pandas `.df()`** — Use `duckdb_conn.execute(...).fetchall()` + `result.description` (line 297-298)
2. **Add EXPLAIN validation** — Before SQL execution, run `EXPLAIN {sql}` to catch syntax errors (line 296)
3. **Fix balance evaluation** — Require specific output columns (`total_debits`, `total_credits`, `difference`);
   fail explicitly when columns missing instead of defaulting to pass (line 492-514)
4. **Update prompt for column naming** — Add instruction: "Your output query MUST include columns named
   `total_debits`, `total_credits`, and `difference` for balance checks"

---

## Phase B: Clean the Model

**Goal:** Vertical config becomes a proper abstraction. Quality metrics move to where they belong.

### B1. Create VerticalConfig Abstraction

- New class `VerticalConfig` that bundles: ontology, cycles, filters, metrics, validations
- Single entry point: `VerticalConfig.load("finance")` loads everything
- Replace hardcoded paths in:
  - `GraphLoader.__init__()` (currently `get_config_dir("verticals/finance")`)
  - `analysis/cycles/config.py` (currently `get_config_dir("verticals/finance/cycles.yaml")`)
  - `analysis/validation/config.py` (currently `verticals/finance/validations/`)
- Vertical name flows from pipeline config → phase config → agent
- Semantic agent already works this way (OntologyLoader) — extend the pattern

### B2. Extract Quality Metrics from metrics/ to Entropy System

- Move `config/verticals/finance/metrics/quality/*.yaml` to `config/entropy/quality_metrics/`
- These become inputs to entropy dimension scoring, not graph-executed metrics
- GraphLoader stops loading them as metric graphs
- Entropy processor consumes them directly (or they inform detector thresholds)
- Adjust any tests that reference quality metrics as graph metrics

---

## Phase C: Complete the Context

**Goal:** All agent outputs flow into GraphExecutionContext for downstream use.

### C1. Surface Validation Results in GraphExecutionContext

- Add `ValidationContext` dataclass to `graphs/context.py`
- Load from `ValidationResultRecord` in `build_execution_context()`
- Include: check name, status, severity, message (not full SQL)
- Query agent and graph agent now know about accounting rule failures
- Entropy layer can factor in validation failures as a signal

### C2. Forward Full Cycle Data to Context

- Replace flat `BusinessCycleContext` (4 fields) with richer structure
- Include: stages with completion rates, entity flows, health observations
- Query agent can now reason about business process state
- Format for prompt: concise business process summary, not raw data

### C3. Align Ontology Concepts with Graph standard_field Vocabulary

- Audit: compare ontology concept names vs graph `standard_field` references
- Options: (a) unify naming, (b) add mapping layer, (c) validate at load time
- Decision during implementation based on divergence found
- Goal: no silent mapping failures

---

## Phase D: Extract Shared Infrastructure *(revised 2026-02-25)*

**Goal:** Extract shared SQL execution infrastructure from graph + query agents.
Keep agents separate — they have fundamentally different runtime characteristics.

> **Why not merge?** The original D2 ("Create unified DataAgent") was dropped because:
> - Graph agent is deterministic — can skip LLM entirely via cached snippets
> - Query agent is exploratory — always needs LLM
> - Different snippet discovery patterns (exact key vs term-based search)
> - Different output types (scalar metric vs table rows)
> - Merging creates a kitchen-sink class with if/else branching everywhere
>
> The right approach: extract shared infrastructure, keep agents as thin wrappers.

### D1. Extract Shared Base

Extract into a shared module (e.g., `agents/sql_base.py` or `agents/shared/`):
- Context building (`build_execution_context` — already shared)
- SQL execution (`execute_sql_steps` — already in `query/execution.py`)
- SQL repair logic (both agents have `_repair_sql`)
- Assumption tracking (both produce `QueryAssumption`)
- Snippet/library storage (both save to `QueryLibrary`)

### D2. Refactor Both Agents to Use Shared Base

- Graph agent delegates SQL execution and repair to shared base
- Query agent delegates SQL execution and repair to shared base
- Each agent retains its own: prompt construction, snippet discovery, output formatting

### D3. Cleanup Dead Code

- Remove duplicated execution/repair logic from individual agents
- Update imports across pipeline phases and MCP server
- Verify all tests pass with new structure

---

## Known Gap: temporal_behavior Not Reaching SQL Generation *(added 2026-02-25)*

### Problem

The ontology defines `temporal_behavior: point_in_time` vs `additive` per business concept.
This is the right semantic distinction for SQL aggregation (snapshot vs sum):
- **additive** (e.g., revenue): `SUM(amount)` across periods is valid
- **point_in_time** (e.g., accounts_receivable): only the latest period's value is valid;
  `SUM(amount)` across periods is meaningless

Currently this information **never flows** to the SQL generation prompt. The DSO
`accounts_receivable` step (3/4 failure in testdata calibration) is caused by this gap:
the LLM uses `ORDER BY period DESC LIMIT 1` instead of a subquery pattern that filters
to the latest reporting period. The repair LLM makes the same mistake because it also
lacks this context.

### Planned Fix: Store on SemanticAnnotation During Semantic Phase

`temporal_behavior` is semantic information — it belongs in the semantic phase output.

1. The semantic agent already reads the ontology to assign `business_concept`
2. After assignment, look up `temporal_behavior` from the matched concept
   (no new LLM call — just a metadata join in `_parse_tool_output()`)
3. Add `temporal_behavior: str | None` field to `SemanticAnnotation` DB model
4. Information then flows automatically:
   `SemanticAnnotation` → `ColumnContext` → `format_context_for_prompt()` →
   all agents see `(point_in_time)` or `(additive)` per column

This is domain-agnostic: any vertical's ontology can define temporal behavior.
Implementation deferred until after E2E tests validate the feedback loop.

---

## Phase 0.6: E2E Validation Tests *(added 2026-02-25)*

**Goal:** Validate the pipeline end-to-end against synthetic data with known correct answers.
Priority work before continuing to Phase C/D.

### Motivation

- The mocked integration tests (Session 12 / Phase 0.5) validate code paths, not actual behavior
- `dataraum-testdata` generates synthetic finance data with known properties (8 tables,
  deterministic with seed, manifest with row counts)
- Nothing currently connects testdata to the pipeline — this phase bridges that gap

### Approach

- Add `dataraum-testdata` as uv path dev dependency (editable, `../dataraum-testdata`)
- `@pytest.mark.e2e` marker — excluded from testmon and end-of-turn hook
- Session-scoped fixtures: generate once, run pipeline once, many test functions query results
- `clean` strategy = no entropy injections = predictable data for arithmetic verification
- Non-LLM phases only in base fixture (import → typing → statistics → ... → entropy)

### Test Coverage

| Test File | What It Validates |
|-----------|-------------------|
| `test_pipeline_phases.py` | Import row counts, type detection, statistics, relationships, temporal, entropy |
| `test_context_building.py` | `build_execution_context()` completeness, semantic annotations, field mappings, relationships |

### Running

```bash
uv sync --group e2e                       # Install testdata dependency
uv run pytest tests/e2e -v -m e2e         # Run E2E tests only
uv run pytest --testmon tests/unit -q     # Normal dev (excludes E2E)
```

---

## A0 Findings

> Audit completed 2026-02-24. Each section documents what the agent loads,
> what's wrong, and what to fix.

### Business Cycle Agent

**Files:** `analysis/cycles/context.py`, `agent.py`, `tools.py`, `config.py`

#### What it loads (context.py `build_cycle_detection_context`)

1. **Tables + columns** — from `Table`, `Column` DB objects. ✅ Correct.
2. **Semantic annotations** — from `SemanticAnnotation` DB objects, joined via Column/Table. ✅ Correct.
3. **Status column detection** — scans `entity_type` for keywords ("status", "state", "paid", "cleared", "flag"), then queries DuckDB for distinct values. ⚠ Fragile (see issues).
4. **Entity classifications** — from `TableEntity` DB objects. ✅ Correct.
5. **Relationships** — from `Relationship` DB objects, but with N+1 query problem. ⚠ See issues.
6. **Graph topology** — calls `analyze_graph_topology()` with the relationship dicts. ✅ Correct pattern.
7. **Domain vocabulary** — from `config/verticals/finance/cycles.yaml`. ✅ Correct (but hardcoded vertical).

#### Issues Found

**Issue 1: Relationship loading has N+1 query pattern (context.py:177-211)**
The code loads relationships by `from_table_id`, then for each relationship does **two separate queries** to get `to_table_name` and `to_column_name`:
```python
to_table_stmt = select(Table.table_name).where(Table.table_id == rel.to_table_id)
to_col_stmt = select(Column.column_name).where(Column.column_id == rel.to_column_id)
```
With 20 relationships, that's 40 extra queries. Compare with `graphs/context.py:301-339` which loads all relationships in one query and resolves names from the already-loaded column/table lists.

**Fix:** Load relationships with both `from_table_id` and `to_table_id` in the table_ids set (like `graphs/context.py` does), resolve names from already-loaded Table/Column objects. Remove the inner-loop queries.

**Issue 2: Relationships not filtered by detection_method (context.py:199)**
The code uses a confidence threshold (`rel.confidence > 0.7`) OR `detection_method == "llm"` as filter. Compare with `graphs/context.py:305` which filters strictly to `detection_method == "llm"`. The cycle agent may include low-confidence `candidate` relationships that haven't been LLM-confirmed, polluting the context with noise.

**Fix:** Filter to `detection_method == "llm"` only, matching `graphs/context.py` behavior.

**Issue 3: Status column detection is purely heuristic (context.py:123-126)**
Status columns are detected by checking if `entity_type` contains keywords like "status", "state", "paid". This misses columns where the semantic agent classified `entity_type` differently (e.g., "transaction_status" might match, but "payment_indicator" won't). The semantic agent already has `semantic_role` — a column with role "status" or entity_type containing status-like terms should both qualify.

**Fix:** Also check `semantic_role` for status-like values, and check `business_description` for status keywords. Or better: have the semantic agent explicitly tag status columns during its analysis (deferred to Phase B when vertical config is abstracted).

**Issue 4: Column `resolved_type` may be None (context.py:88)**
The context builder uses `c.resolved_type` which can be None for columns that haven't been through type resolution. The prompt receives `null` type info for these columns.

**Fix:** Fall back to `c.raw_type` when `resolved_type` is None (like the validation resolver does at `resolver.py:124`).

**Issue 5: Duplicate code in agent.py `_parse_tool_output` vs `_parse_response` (agent.py:370-604)**
Both methods contain ~100 lines of nearly identical cycle parsing code (EntityFlow construction, stage parsing, vocabulary mapping, analysis assembly). The `_parse_response` is a JSON fallback that's rarely used.

**Fix (streamlining):** Extract shared cycle parsing into a helper. Consider removing the JSON fallback entirely — if the LLM doesn't call `submit_analysis`, that's a prompt/model issue, not something to silently handle with fragile JSON extraction.

**Issue 6: Config hardcoded to finance (config.py:33)**
`get_config_file("verticals/finance/cycles.yaml")` — will break for any non-finance vertical.

**Fix:** Deferred to Phase B1 (VerticalConfig abstraction).

#### What the Prompt Expects vs What It Gets

The system prompt tells the LLM to look for:
- Entity flows (dimension → fact relationships) ← **Gets this** from entity classifications + relationships
- Status columns with distinct values ← **Gets this** but detection is fragile (Issue 3)
- Transaction type columns ← **Does NOT get these explicitly**; relies on LLM inferring from semantic annotations
- Relationships to follow ← **Gets this** but may include noisy candidates (Issue 2)

#### Summary: A1 Fix List

| # | Fix | Scope | Risk |
|---|-----|-------|------|
| 1 | Remove N+1 relationship queries, use pre-loaded data | context.py:177-211 | Low |
| 2 | Filter relationships to `detection_method == "llm"` | context.py:199 | Low |
| 3 | Improve status column detection (add semantic_role check) | context.py:122-126 | Low |
| 4 | Fall back to `raw_type` when `resolved_type` is None | context.py:88 | Low |
| 5 | Extract shared cycle parsing, remove JSON fallback | agent.py | Medium |
| 6 | Hardcoded finance config | Deferred to B1 | — |

---

### Validation Agent

**Files:** `analysis/validation/resolver.py`, `agent.py`, `config.py`

#### What it loads (resolver.py `get_multi_table_schema_for_llm`)

1. **Tables with columns** — Eager-loads via `selectinload(Table.columns).selectinload(Column.semantic_annotation)`. ✅ Clean, single query with joins.
2. **Semantic annotations** — Loaded alongside columns via eager loading. ✅ Correct.
3. **Relationships** — from `Relationship` DB objects filtered by `from_table_id` and `to_table_id` both in table_ids. ✅ Correct pattern.
4. **Column name/type info** — Uses `resolved_type or raw_type` fallback. ✅ Correct.

#### Issues Found

**Issue 1: No detection_method filter on relationships (resolver.py:78-83)**
Unlike `graphs/context.py` which filters to `detection_method == "llm"`, the validation resolver loads ALL relationships between the tables. This includes raw `candidate` relationships from join detection that haven't been LLM-confirmed. For validation SQL generation (e.g., trial balance requiring joins), noisy join candidates could lead the LLM to generate incorrect JOINs.

**Fix:** Add `Relationship.detection_method == "llm"` filter to match the standard.

**Issue 2: No business_concept in schema (resolver.py:126-134)**
The resolver passes `semantic_role`, `entity_type`, and `business_name` to the LLM. It does NOT pass `business_concept` (e.g., "revenue", "accounts_receivable") or `business_description`. For financial validations like trial_balance (Assets = Liabilities + Equity), the LLM needs to know which columns represent assets vs liabilities. Without `business_concept`, it relies on name matching.

**Fix:** Add `business_concept` and `business_description` to the column schema passed to the prompt.

**Issue 3: No statistical context (agent.py)**
The validation agent receives only schema information (column names, types, semantic roles). It gets no statistical context: null ratios, row counts, value distributions. For validations like `fiscal_period_integrity` (checking for gaps), knowing the date range or row count per period would help the LLM generate better SQL.

**Fix (lightweight):** Add row counts per table to the schema dict. Full statistical context would be over-engineering for the validation use case.

**Issue 4: Validation results use pandas `.df()` (agent.py:297-298)**
```python
result_df = duckdb_conn.execute(generated.sql_query).df()
result_rows = result_df.to_dict(orient="records")
```
This pulls results through pandas, which is unnecessary. DuckDB can return tuples directly. This creates an unnecessary pandas dependency for the validation path.

**Fix:** Use `duckdb_conn.execute(...).fetchall()` with `result.description` for column names, matching the pattern in `query/execution.py`.

**Issue 5: No SQL validation before execution (agent.py:296)**
The graph agent validates generated SQL with `EXPLAIN` before execution (`graphs/agent.py` `_validate_sql`). The validation agent executes LLM-generated SQL directly without syntax checking. A malformed query hits DuckDB cold.

**Fix:** Add `EXPLAIN` validation before execution, or wrap in try/except with a repair attempt (like the graph agent does). For now, the try/except at line 324 catches failures but doesn't attempt repair.

**Issue 6: Config hardcoded to finance (config.py)**
`load_all_validation_specs()` loads from `config/verticals/finance/validations/`. Same issue as cycles.

**Fix:** Deferred to Phase B1 (VerticalConfig abstraction).

**Issue 7: `_evaluate_result` is brittle for balance checks (agent.py:492-512)**
The balance evaluation searches for columns containing "total" or "sum" in their name. If the LLM names columns differently (e.g., "debit_balance", "credit_balance"), the check silently falls through to "Balance check passed" (line 514) — a false positive.

**Fix:** The prompt should instruct the LLM to use specific column names (`total_debits`, `total_credits`, `difference`), and the evaluation should fail explicitly when it can't find expected columns rather than defaulting to pass.

#### What the Prompt Expects vs What It Gets

The prompt template (`config/llm/prompts/validation_sql.yaml`) receives:
- `spec_name`, `spec_description`, `check_type` ← From YAML spec. ✅
- `parameters` (tolerance, account types, etc.) ← From YAML spec. ✅
- `sql_hints` ← From YAML spec. ✅ Good — gives the LLM concrete guidance
- `schema` (XML formatted tables + columns + relationships) ← From resolver. ⚠ Missing `business_concept`
- `expected_outcome` ← From YAML spec. ✅

The schema XML shows column names, types, semantic roles, entity types, business names, and relationships. For financial validations, the missing piece is `business_concept` — the ontology-level classification that maps columns to accounting categories.

#### Summary: A2 Fix List

| # | Fix | Scope | Risk |
|---|-----|-------|------|
| 1 | Filter relationships to `detection_method == "llm"` | resolver.py:78-83 | Low |
| 2 | Add `business_concept` + `business_description` to schema | resolver.py:126-134 | Low |
| 3 | Add row counts per table to schema | resolver.py | Low |
| 4 | Replace pandas `.df()` with DuckDB `.fetchall()` | agent.py:297-298 | Low |
| 5 | Add EXPLAIN validation before SQL execution | agent.py:296 | Low |
| 6 | Hardcoded finance config | Deferred to B1 | — |
| 7 | Make balance evaluation fail explicitly on missing columns | agent.py:492-514 | Medium |

---

## Decision Log

| Date | Decision | Rationale |
|------|----------|-----------|
| 2026-02-24 | Fix context loading before architecture | Broken context makes all later changes unreliable |
| 2026-02-24 | Quality metrics → entropy, not graph metrics | They measure data health, not business KPIs |
| 2026-02-24 | ~~Merge graph + query into DataAgent~~ | ~~80% shared code, same execution pattern~~ |
| 2026-02-25 | **Revised:** Extract shared infrastructure, keep agents separate | Graph is deterministic (skippable LLM), query is exploratory (always LLM). Different snippet discovery, different output types. Merge creates kitchen-sink class |
| 2026-02-25 | temporal_behavior gap identified | Ontology defines additive vs point_in_time but it never reaches SQL generation prompt. Root cause of DSO failures. Fix: store on SemanticAnnotation during semantic phase |
| 2026-02-25 | E2E tests before Phase C/D | Pipeline + agents never validated against known-correct data. dataraum-testdata provides it. Must close feedback loop before further architecture changes |
| 2026-02-24 | Phase A starts with audit (A0) | Deep dive prevents wasted effort on wrong fixes |
| 2026-02-24 | Eliminate cycle agent tools, use context only | Pipeline now produces slice defs, statistical profiles, temporal profiles, enriched views — all the data the tools re-queried. Single LLM call with rich context replaces 3-15 tool call loop |
| 2026-02-24 | Rewrite cycle agent (move to _legacy/) | ~73% of code replaced/deleted (context.py, agent.py, tools.py). In-place editing would leave dead code. Move to _legacy/, rewrite from scratch, delete when tests pass |
| 2026-02-24 | Fix validation agent in-place | Changes are surgical (~30% of code touched). Structure is sound, just needs enriched context and execution fixes |

---

## How to Resume

Each session:
1. Read this file — know where you are in the sequence
2. Read BACKLOG.md — current focus section points here
3. Check status in the tracker table below
4. Run `pytest --testmon tests -q` — verify green before touching code
5. Work on the current step. Update this file with findings/completions.
6. Update PROGRESS.md with session log entry

## Tracker

| Phase | Step | Description | Strategy | Status |
|-------|------|-------------|----------|--------|
| A | A0 | Audit agent context loading + deep metadata evaluation | Research only | ✅ Done |
| A | A1a | New cycle context builder (load from all pipeline metadata) | Rewrite (_legacy/) | ✅ Done |
| A | A1b | New cycle agent (single-call synthesis, no tools) | Rewrite (_legacy/) | ✅ Done |
| A | A2a | Enrich validation resolver (business_concept, enriched views, row counts) | In-place edit | ✅ Done |
| A | A2b | Fix validation execution (EXPLAIN, no pandas, fix evaluation) | In-place edit | ✅ Done |
| B | B1 | Create VerticalConfig abstraction | New code | ✅ Done |
| B | B2 | Extract quality metrics to entropy system | Move + rewire | ✅ Done |
| 0.6 | 0.6 | E2E validation tests (testdata → pipeline → verify) | New tests | **Current** |
| — | — | temporal_behavior on SemanticAnnotation | Semantic phase fix | Pending |
| C | C1 | Surface validation results in GraphExecutionContext | Additive | Pending |
| C | C2 | Forward full cycle data to context | Additive | Pending |
| C | C3 | Align ontology concepts ↔ standard_field vocabulary | Audit + fix | Pending |
| D | D1 | Extract shared SQL execution base | Refactor | Pending |
| D | D2 | Refactor both agents to use shared base | Refactor | Pending |
| D | D3 | Cleanup dead code, update imports | Delete | Pending |

Dependencies: A1a → A1b (context before agent). A2a → A2b (resolver before agent).
A1 and A2 are independent of each other. B depends on A.
**Revised sequencing:** 0.6 → temporal_behavior fix → C → D(revised).
C depends on B. D depends on C.
