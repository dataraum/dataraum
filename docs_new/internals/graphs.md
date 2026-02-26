# Graph Agent

## Reasoning & Summary

The graph agent answers: **"What is the value of this business metric for this dataset?"**

It takes a declarative graph definition (YAML) describing a financial metric (e.g., Days Sales Outstanding, Gross Margin), generates SQL via LLM, executes it step-by-step, and returns a typed result with interpretation. The agent handles entropy-aware execution: it tracks assumptions, respects compound risks, and adjusts response style based on data confidence levels.

The module runs as pipeline phase `graph_execution` (after `entropy_interpretation`).

## Architecture

```
graphs/
├── __init__.py          # Public API (re-exports)
├── agent.py             # GraphAgent: LLM SQL generation + execution
├── models.py            # Dataclass/Pydantic models (graph defs, execution, mapping)
├── db_models.py         # SQLAlchemy: GeneratedCodeRecord, GraphExecutionRecord, StepResultRecord
├── context.py           # GraphExecutionContext builder (semantic, statistical, entropy)
├── persistence.py       # GraphExecutionRepository (save/load executions)
├── loader.py            # GraphLoader: YAML graph definitions (filters + metrics)
├── entropy_behavior.py  # EntropyBehaviorConfig (strict/balanced/lenient modes)
├── field_mapping.py     # FieldMappings: business concept → column resolution
└── export.py            # React Flow JSON visualization export
```

**~4,500 LOC** across 10 files.

### Data Flow

```
GraphAgent.execute(session, graph, context, parameters)
  │
  ├── Merge user params with graph defaults
  │
  ├── Cache lookup (in-memory → DB: GeneratedCodeRecord)
  │     └── Key: {graph_id}:{version}:{schema_mapping_id}
  │
  ├── SQL generation (if not cached):
  │     ├── Extract table schema (columns, types, samples)
  │     ├── Serialize graph to YAML for context
  │     ├── Render "graph_sql_generation" prompt (system + user)
  │     ├── LLM tool call → GraphSQLGenerationOutput
  │     ├── Validate SQL via EXPLAIN
  │     └── Persist GeneratedCodeRecord
  │
  ├── SQL execution:
  │     ├── Create GraphExecution record
  │     ├── Extract entropy info (assumptions, warnings)
  │     ├── execute_sql_steps() (shared with query agent)
  │     │     ├── CREATE TEMP VIEW per step
  │     │     ├── Execute final_sql
  │     │     └── Repair via LLM on failure (max 2 attempts)
  │     ├── Extract output_value + interpretation
  │     └── Save to QueryLibrary (for cross-agent reuse)
  │
  └── Return: Result[GraphExecution]
```

### LLM Integration

| Aspect | Detail |
|--------|--------|
| Prompt | `config/system/prompts/graph_sql_generation.yaml` |
| Temperature | 0.0 (deterministic) |
| Tool | `generate_sql` → `GraphSQLGenerationOutput` (Pydantic schema) |
| Repair | `sql_repair` prompt, max 2 attempts, "fast" model tier |

## Data Model

### Graph Definitions (YAML → dataclass)

| Model | Purpose |
|-------|---------|
| `TransformationGraph` | Main graph spec: graph_id, type (FILTER/METRIC), version, steps, output, parameters |
| `GraphStep` | Single step: step_id, level, step_type (EXTRACT/CONSTANT/PREDICATE/FORMULA/COMPOSITE) |
| `OutputDef` | Output spec: output_type (SCALAR/SERIES/TABLE/CLASSIFICATION), metric_id, unit |
| `Interpretation` | Value ranges with labels (e.g., DSO < 30 = "Good") |

### Execution Models (dataclass)

| Model | Purpose |
|-------|---------|
| `GraphExecution` | Full result: execution_id, step_results, output_value, output_interpretation, assumptions, entropy_warnings |
| `StepResult` | Per-step: value, classification, rows_passed/failed, source_query |
| `ExecutionContext` | Input: duckdb_conn, table_name, schema_mapping, entropy_behavior, rich_context |
| `QueryAssumption` | From entropy: dimension, target, assumption text, basis, confidence |

### Context Models (dataclass)

| Model | Purpose |
|-------|---------|
| `GraphExecutionContext` | Complete LLM context: tables, relationships, entropy_summary, business_cycles, field_mappings |
| `ColumnContext` | Column metadata: data_type, semantic_role, entity_type, null_ratio, entropy_scores |
| `TableContext` | Table metadata: row_count, is_fact_table, table_entropy, readiness_for_use |
| `RelationshipContext` | Join: from/to table+column, cardinality, confidence, relationship_entropy |
| `FieldMappings` | Business concept → column resolution (e.g., `accounts_receivable` → `typed_orders.ar_amount`) |

### Behavior Models (dataclass)

| Model | Purpose |
|-------|---------|
| `EntropyBehaviorConfig` | Mode (strict/balanced/lenient), thresholds, auto_assume, disclosure |
| `EntropyAction` | ANSWER_CONFIDENTLY / ANSWER_WITH_ASSUMPTIONS / ASK_OR_CAVEAT / REFUSE |

### SQLAlchemy Models (db_models.py)

**GeneratedCodeRecord** (`generated_code`):
- PK: `code_id` (UUID)
- `graph_id`, `graph_version`, `schema_mapping_id` (indexed)
- `summary`, `steps_json` (JSON), `final_sql`, `column_mappings`
- `llm_model`, `prompt_hash` (indexed), `generated_at`
- `is_validated`, `validation_errors`

**GraphExecutionRecord** (`graph_executions`):
- PK: `execution_id` (UUID)
- `graph_id`, `graph_type`, `graph_version` (indexed)
- `source`, `parameters`, `period` (indexed)
- `output_value`, `output_interpretation`, `execution_hash`
- FK: `generated_code_id`, `library_entry_id` (bidirectional to query_library)

**StepResultRecord** (`step_results`):
- PK: `result_id` (UUID)
- FK: `execution_id` (indexed)
- `step_id`, `level`, `step_type`
- `value_scalar`, `value_boolean`, `value_string`, `value_json`

## Configuration

- **Graph definitions**: `config/verticals/finance/filters/` and `config/verticals/finance/metrics/` (YAML)
- **LLM prompt**: `config/system/prompts/graph_sql_generation.yaml`
- **LLM config**: `config/llm.yaml` (provider, model, limits)
- **SQL repair**: `features.sql_repair.enabled`, `max_repair_attempts` (default 2)
- **Entropy behavior**: Programmatic (EntropyBehaviorConfig), not yet in YAML

## Consumers

| Consumer | What It Uses |
|----------|--------------|
| `pipeline/phases/graph_execution_phase.py` | `GraphAgent.execute()`, `GraphLoader.load_graphs()` |
| `query/library.py` | `QueryDocument.from_graph_output()` for cross-agent reuse |
| `query/execution.py` | `execute_sql_steps()` (shared execution engine) |

## Roadmap

- **Filter graph execution**: Filter graphs load but aren't integrated into the pipeline execution flow. Metric graphs should run after their required filters.
- **Graph dependency resolution**: Multi-level graph execution — metrics that depend on other metrics (via `depends_on_executions`). Currently tracked but not enforced.
- **Schema mapping inference**: `DatasetSchemaMapping` models exist but aren't integrated into the LLM flow. Could auto-infer mappings from semantic annotations.
- **Slice-based metric computation**: `ExecutionContext` captures `slice_column`/`slice_value` but SQL generation doesn't use them for GROUP BY.
- **Cache invalidation via hashing**: `GeneratedCodeRecord` has `prompt_hash` but no `graph_hash` or `schema_hash`. Schema changes don't invalidate cached SQL.
- **Context integration depth**: `GraphExecutionContext` gathers rich metadata but formatters (statistical, temporal, topological, domain, multicollinearity, business_cycles) aren't wired into the context builder. Quality formatting from `quality_summary` is not used.
- **Async execution model**: GraphAgent.execute() is synchronous; QueryAgent.analyze() is async. Inconsistency to resolve.
- **Entropy behavior YAML config**: `EntropyBehaviorConfig` with strict/balanced/lenient modes exists in code but isn't loadable from YAML config. Should externalize to `config/system/entropy/query_behavior.yaml`.
- **MCP integration**: Tools for `get_metric_explanation()`, `list_metric_executions()` — expose graph results to AI consumers.
- **Parameterized graphs**: Parameter validation, injection, safe substitution for dynamic metric computation.
