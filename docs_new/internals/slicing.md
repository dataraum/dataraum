# Slicing Module

## Reasoning & Summary

The slicing module answers: **"What are the best categorical dimensions for splitting data into comparable subsets?"**

Data slices enable per-segment analysis: comparing quality, distributions, and temporal behavior across meaningful subgroups (e.g., "by region", "by product category"). The LLM recommends slicing dimensions based on semantic annotations, statistical profiles, and correlations. Once identified, slice tables are created in DuckDB and registered in metadata for downstream analysis.

The module spans two pipeline phases:
- `slicing` (LLM-powered): Identifies optimal dimensions and stores `SliceDefinition` records
- `slice_analysis` (compute): Creates slice tables, registers metadata, runs statistics + quality on each

## Architecture

```
slicing/
├── __init__.py        # Public API exports
├── models.py          # Pydantic models (5 models)
├── db_models.py       # SQLAlchemy persistence (2 models)
├── agent.py           # LLM-powered dimension recommendation
├── processor.py       # Orchestrator: context loading → LLM → persistence
├── utils.py           # Context loading from prior phases
└── slice_runner.py    # Slice table registration + analysis runners
```

**~1,400 LOC** across 7 files.

### Data Flow

```
processor.analyze_slices(session, agent, table_ids, duckdb_conn)
  │
  ├── utils.load_slicing_context()          → tables, statistics, semantic, correlations, quality
  ├── agent.analyze()                       → LLM call with tool use
  │     ├── Render prompt (slicing_analysis)
  │     ├── Call LLM with SlicingAnalysisOutput tool schema
  │     └── _convert_output_to_result()     → SlicingAnalysisResult
  ├── Store SliceDefinition records
  └── Optionally execute SQL to create slice tables

slice_analysis_phase._run(ctx)
  │
  ├── Execute SQL templates from SliceDefinitions → DuckDB slice tables
  ├── register_slice_tables()               → Table + Column metadata entries
  └── run_analysis_on_slices()
        ├── run_statistics_on_slice()       → StatisticalProfile per slice
        └── run_quality_on_slice()          → StatisticalQualityMetrics per slice
```

### LLM Integration

The agent extends `LLMFeature` and uses tool-based structured output:

| Aspect | Detail |
|--------|--------|
| Prompt template | `config/system/prompts/slicing_analysis.yaml` |
| Tool schema | `SlicingAnalysisOutput.model_json_schema()` |
| Model tier | From `config.features.slicing_analysis.model_tier` |
| Fallback | If LLM doesn't use tool, tries JSON parse of text response |

The LLM receives context from prior phases: table metadata, column statistics, semantic annotations, correlations, and quality metrics — all serialized as JSON.

## Data Model

### Pydantic Models (models.py)

| Model | Purpose |
|-------|---------|
| `SliceRecommendation` | Internal result: table/column IDs, priority, distinct_values, reasoning, confidence, sql_template |
| `SliceSQL` | Single slice SQL: slice_name, slice_value, table_name, sql_query |
| `SlicingAnalysisResult` | Run result: recommendations, slice_queries, source, counts |
| `SliceRecommendationOutput` | LLM tool output: table_name, column_name, priority, distinct_values, reasoning, confidence |
| `SlicingAnalysisOutput` | LLM tool envelope: list of SliceRecommendationOutput |

### SQLAlchemy Models (db_models.py)

**SliceDefinition** (`slice_definitions`):
- PK: `slice_id`
- FK: `table_id` -> tables, `column_id` -> columns
- Fields: `slice_priority`, `slice_type` (always "categorical"), `distinct_values` (JSON), `value_count`, `reasoning`, `business_context`, `confidence`, `sql_template`, `detection_source`, `created_at`
- Relationships: `table`, `column`

**SlicingAnalysisRun** (`slicing_analysis_runs`):
- PK: `run_id`
- Fields: `table_ids` (JSON), `tables_analyzed`, `columns_considered`, `recommendations_count`, `slices_generated`, timing fields, `status`, `error_message`

### Dataclasses (slice_runner.py)

| Dataclass | Purpose |
|-----------|---------|
| `SliceTableInfo` | Registered slice table info (IDs, names, row_count) |
| `SliceAnalysisResult` | Analysis run result (counts per sub-phase + errors) |
| `TemporalSlicesResult` | Temporal analysis results across slices |
| `TopologySlicesResult` | Topology analysis results across slices |

## Slice Table Naming

All slice tables follow the convention: `slice_{column}_{value}` where both parts are sanitized (alphanumeric + underscore, lowercased). The same sanitization logic exists in `agent.py` (`_sanitize_for_table_name`) and `slice_runner.py` (`_sanitize_name`).

## Consumers

| Consumer | What It Uses |
|----------|--------------|
| `slice_analysis_phase` | `register_slice_tables()`, `run_analysis_on_slices()` |
| `temporal_slice_analysis_phase` | `run_temporal_analysis_on_slices()`, `run_topology_on_slices()` |
| `slicing_phase` | `analyze_slices()` via processor |
| Downstream entropy | Slice profiles feed into entropy scoring |

## Cleanup History (This Refactor)

| Change | Rationale |
|--------|-----------|
| Removed `run_semantic_on_slices()` | Never called; `run_analysis_on_slices` always invoked with `run_semantic=False` |
| Removed `semantic_agent`/`run_semantic` params from `run_analysis_on_slices` | Dead parameters |
| Removed `semantic_enriched` field from `SliceAnalysisResult` | Always 0 |
| Removed `execute_slices_from_definitions()` | Never called from any pipeline phase or test |
| Cleaned `_get_slice_table_name()` unused `source_table_name` param | Was assigned to `_` immediately |
| Cleaned `run_topology_on_slices()` unused `correlation_threshold` param | No caller passes it |
| Moved inline imports to module level | `profile_statistics`, `assess_statistical_quality`, `temporal_slicing` imports, `analyze_topological_quality`, `ConversationRequest`/`Message`/`ToolDefinition` |
| Updated `__init__.py` exports | Removed dead function exports |

## Cross-Table Slicing via Enriched Views

### Problem

The slicing agent currently only considers columns from the original typed table. When a fact table has confirmed dimension relationships (e.g., orders → customers), valuable slicing dimensions from dimension tables (e.g., `country`, `region`, `product_category`) are invisible to the LLM.

### Solution: Consume Enriched Views

The `enriched_views` phase (upstream of slicing) creates grain-preserving DuckDB views that LEFT JOIN fact tables with their confirmed dimensions. These views contain dimension columns like `customers__country`. The slicing agent should consider these as additional candidate dimensions.

### What Needs to Change

**1. Agent context (`agent.py` — `analyze()` method)**

Pass enriched columns to the prompt:

```python
context = {
    ...existing keys...,
    "enriched_columns_json": json.dumps(context_data.get("enriched_columns", []), indent=2),
}
```

**2. Prompt template (`config/system/prompts/slicing_analysis.yaml`)**

Add to `<selection_criteria>`:
```
6. Cross-Table Dimensions: Dimension columns from enriched views (prefixed with
   {dim_table}__) are available and often make excellent slicing dimensions
   (e.g., customers__country, products__category)
```

Add to `<guidelines>`:
```
- Consider enriched dimension columns (prefixed with {table}__) as candidates.
  These come from confirmed dimension relationships and are already joined.
- When recommending an enriched column, use the enriched view name as the
  table_name (e.g., "enriched_orders"), not the original fact table.
```

Add to user prompt:
```yaml
<enriched_views>
Dimension columns available from enriched views (pre-joined from related tables):
{enriched_columns_json}
</enriched_views>
```

Add to `inputs:`:
```yaml
enriched_columns_json:
  description: JSON array of enriched view dimension columns from confirmed relationships
  required: false
  default: "[]"
```

**3. SQL generation (`agent.py` — `_convert_output_to_result()`)**

When the LLM recommends a column from an enriched view:
- Detect enriched columns by `table_name` starting with `enriched_` or column containing `__`
- Use the enriched view name as the DuckDB source table (not `typed_*`)
- The enriched view is already a DuckDB view, so `SELECT * FROM enriched_orders WHERE ...` works

Lookup logic:
```python
# Build enriched view lookup from context
enriched_view_map = {}  # table_name -> view_name
for ec in context_data.get("enriched_columns", []):
    fact_table_id = ec["fact_table_id"]
    fact_table = next((t for t in context_data["tables"] if t["table_id"] == fact_table_id), None)
    if fact_table:
        enriched_view_map[fact_table["table_name"]] = ec["view_name"]

# In _convert_output_to_result, when building SQL:
if "__" in column_name:
    # Cross-table column — use enriched view as source
    duckdb_table = enriched_view_map.get(table_name, table_info.get("duckdb_path", ...))
```

**4. Context data loading (`slicing_phase.py` — `_build_context_data()`)**

Already loads enriched columns into `context_data["enriched_columns"]` — no change needed. But currently drops them before they reach the prompt. The fix is in point 1 above.

**5. `SliceRecommendationOutput` model (optional enhancement)**

Add optional field so the LLM can explicitly indicate cross-table source:
```python
source_view: str | None = Field(
    default=None,
    description="If slicing on an enriched dimension column, the view name to query (e.g., 'enriched_orders')"
)
```

### Data Flow After Changes

```
enriched_views_phase
  │
  └── Creates DuckDB views: enriched_orders = typed_orders LEFT JOIN typed_customers ...
      Stores EnrichedView records with dimension_columns

slicing_phase._build_context_data()
  │
  └── Loads EnrichedView.dimension_columns → enriched_columns in context

slicing agent.analyze()
  │
  ├── Passes enriched_columns_json to prompt
  ├── LLM sees cross-table candidates: customers__country, products__category
  └── LLM may recommend: table="enriched_orders", column="customers__country"

agent._convert_output_to_result()
  │
  └── Detects enriched column → uses enriched view for SQL:
      CREATE TABLE slice_customers__country_US AS
      SELECT * FROM enriched_orders WHERE "customers__country" = 'US';

slice_analysis_phase
  │
  └── Executes SQL (works — enriched_orders is a real DuckDB view)
```

### Single-Table Case

When there's only one table and no enriched view exists, `enriched_columns` is empty and the prompt section is blank. The LLM falls back to within-table slicing only. No behavioral change.

### What Does NOT Need to Change

| Component | Why |
|-----------|-----|
| `slice_analysis_phase` | Executes SQL from `SliceDefinition.sql_template` — works with any valid DuckDB table/view |
| `temporal_slice_analysis_phase` | Operates on slice tables created by slice_analysis — unaware of source |
| `cross_table_quality_phase` | Uses Relationship records directly, not enriched views |
| `quality_summary_phase` | Aggregates slice results, source-agnostic |
| `entropy_phase` | Column-level analysis, uses `unit_source_column` from semantic (already wired) |

## Roadmap

- **Consolidate sanitization**: Three copies of name sanitization logic could be extracted to a shared utility
- **Numeric slicing**: Currently categorical only; range-based slicing for numeric columns
- **Slice quality comparison**: Cross-slice quality metrics comparison (beyond topology)
- **Cross-table slicing**: Implement the enriched views consumption described above
