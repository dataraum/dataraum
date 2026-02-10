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

## Roadmap

- **Consolidate sanitization**: Three copies of name sanitization logic could be extracted to a shared utility
- **Numeric slicing**: Currently categorical only; range-based slicing for numeric columns
- **Slice quality comparison**: Cross-slice quality metrics comparison (beyond topology)
