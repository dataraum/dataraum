# Query Agent

## Reasoning & Summary

The query agent answers: **"What does this data say about [user's question]?"**

It takes a natural language question, generates SQL via LLM, executes it against the typed tables, and returns a formatted answer with confidence assessment. The agent integrates entropy context for assumption tracking, contract evaluation for data readiness, and a semantic library for query reuse.

The query agent is the primary interactive interface for users asking ad-hoc questions about their data.

## Architecture

```
query/
├── __init__.py       # Public API exports
├── core.py           # Entry point: answer_question()
├── agent.py          # QueryAgent: LLM interaction, execution flow
├── models.py         # Pydantic models: QueryAnalysisOutput, QueryResult
├── db_models.py      # SQLAlchemy: QueryLibraryEntry, QueryExecutionRecord
├── document.py       # QueryDocument: shared semantic representation (used by graph agent too)
├── execution.py      # execute_sql_steps(): shared execution engine (used by graph agent)
├── library.py        # QueryLibrary: save/search/reuse queries via embeddings
└── embeddings.py     # QueryEmbeddings: sentence-transformers similarity search
```

**~2,500 LOC** across 8 files.

### Data Flow

```
answer_question(question, session, duckdb_conn, source_id, contract)
  │
  ├── Filter to typed tables only (layer == "typed")
  │
  ├── Build execution context (GraphExecutionContext)
  │     └── Tables, columns, relationships, semantic annotations
  │
  ├── Build entropy context (EntropyForQuery)
  │     ├── Contract evaluation (if specified)
  │     ├── Confidence level (GREEN/YELLOW/ORANGE/RED)
  │     └── Overall entropy score
  │
  ├── Check if blocked (RED confidence → refuse)
  │
  ├── Library search (semantic similarity):
  │     ├── Exact match (>= 0.85) → reuse SQL directly
  │     └── Inspiration match (>= 0.5) → pass to LLM as context
  │
  ├── LLM generation (if no exact match):
  │     ├── Render "query_analysis" prompt
  │     ├── LLM tool call → QueryAnalysisOutput
  │     └── Validate Pydantic output
  │
  ├── SQL execution:
  │     ├── execute_sql_steps() (shared with graph agent)
  │     ├── Repair via LLM on failure (max 2 attempts)
  │     └── Extract columns + data
  │
  ├── Save to library (if succeeded, not ephemeral)
  │     └── Generate embedding for semantic search
  │
  └── Return: QueryResult
```

### LLM Integration

| Aspect | Detail |
|--------|--------|
| Prompt | `config/system/prompts/query_analysis.yaml` |
| Temperature | 0.0 (deterministic) |
| Tool | `analyze_query` → `QueryAnalysisOutput` (Pydantic schema) |
| Repair | `sql_repair` prompt, max 2 attempts |
| Base class | Extends `LLMFeature` for consistent config handling |

## Data Model

### Pydantic Models (models.py)

| Model | Purpose |
|-------|---------|
| `QueryAnalysisOutput` | LLM output: summary, interpreted_question, metric_type, steps, final_sql, column_mappings, assumptions, validation_notes |
| `SQLStepOutput` | Single step: step_id, sql, description |
| `QueryAssumptionOutput` | LLM assumption: dimension, target, assumption text, basis, confidence |
| `QueryResult` | Full result: answer, sql, data, columns, confidence_level, entropy_score, assumptions, contract_evaluation, library_entry_id |

### Shared Models (document.py)

| Model | Purpose |
|-------|---------|
| `QueryDocument` | Unified representation for both graph and query agent. Has `from_query_analysis()` and `from_graph_output()` constructors. Stored in library for reuse. |
| `QueryAssumptionData` | Simple dict-like assumption data for library storage |

### SQLAlchemy Models (db_models.py)

**QueryLibraryEntry** (`query_library`):
- PK: `query_id` (UUID)
- FK: `source_id` → sources (indexed)
- `original_question`, `graph_id` (indexed), `name`, `description`
- `summary`, `steps_json` (JSON), `final_sql`, `column_mappings`, `assumptions`
- `contract_name`, `confidence_level`
- `embedding_text`, `embedding_model` (default: "all-MiniLM-L6-v2")
- `usage_count`, `last_used_at`, `is_validated`

**QueryExecutionRecord** (`query_executions`):
- PK: `execution_id` (UUID)
- FK: `library_entry_id` → query_library, `source_id` → sources
- `question`, `sql_executed`, `executed_at`
- `success`, `row_count`, `error_message`
- `confidence_level`, `contract_name`, `entropy_action`
- `similarity_score` (if reused from library)

### Shared Execution (execution.py)

| Model | Purpose |
|-------|---------|
| `SQLStep` | Step for execution: step_id, sql, description |
| `ExecutionResult` | Execution output: step_results, columns, rows, final_value |
| `StepExecutionResult` | Per-step: success, error, rows_affected |

## Semantic Library

The query library enables RAG-style query reuse:

1. **Embedding model**: `all-MiniLM-L6-v2` (384-dim, via sentence-transformers)
2. **Embedding text**: Truncated summary + step descriptions + assumption texts (max 1000 chars)
3. **Storage**: DuckDB with `vss` extension for vector similarity search
4. **Search**: Cosine distance, configurable threshold (default 0.85 for reuse, 0.5 for inspiration)
5. **Cross-agent**: Graph agent saves to library via `QueryDocument.from_graph_output()`, query agent can reuse

## Consumers

| Consumer | What It Uses |
|----------|--------------|
| `cli/commands/query.py` | `answer_question()` |
| `api/routers/query.py` | `answer_question()`, `QueryLibrary` CRUD |
| `mcp/server.py` | `answer_question()` (MCP tool) |
| `cli/tui/screens/query.py` | Interactive query screen |

## Configuration

- **LLM prompt**: `config/system/prompts/query_analysis.yaml`
- **LLM config**: `config/llm.yaml` (provider, model, limits)
- **Entropy integration**: `build_for_query()` from `entropy/views/query_context.py`
- **Contract evaluation**: Optional `--contract` or `--auto-contract` flags
- **Library**: Requires `ConnectionManager` with vectors database configured

## Roadmap

- **Entropy-aware response policies**: The agent determines entropy action (answer_confidently / answer_with_assumptions / ask_or_caveat / refuse) but response formatting doesn't fully differentiate. Implement response templates per entropy level: confident answers (no caveats), assumption-annotated answers, clarification requests, and refusal with explanation. See archived `docs/archive/ENTROPY_QUERY_BEHAVIOR.md` for full design.
- **Configurable behavior modes**: Strict (ask for any entropy > 0.3), balanced (default, ask > 0.6), lenient (only refuse > 0.8). Config structure exists in `entropy_behavior.py` but no YAML file. Externalize to `config/system/entropy/query_behavior.yaml` with dimension-specific overrides.
- **Assumption promotion**: Track assumption success rates, promote validated assumptions to permanent rules, detect recurring corrections. Requires `QueryAssumption` persistence and feedback loop integration.
- **User feedback loop**: Collect accept/reject/correct/clarify signals per execution. Track correction patterns to improve defaults. `QueryFeedback` model designed but not implemented.
- **Multi-table join suggestions**: Query agent generates SQL against single tables or pre-existing relationships. Should suggest joins based on relationship candidates when the question spans multiple entities.
- **Query result caching**: Beyond library reuse, cache recent results for short-term deduplication. Entropy context is rebuilt per query — could be cached.
- **SQL comments with entropy context**: Generated SQL should include comments noting assumptions and entropy levels for auditability. Design exists in archived `ENTROPY_QUERY_BEHAVIOR.md`.
