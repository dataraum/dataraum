# Plan: Unify Query and Graph Agent SQL Execution

## Problem Statement

Currently, Graph Agent and Query Agent have divergent implementations:
- **Graph Agent**: Executes steps as temp views, has SQL repair, but doesn't use QueryLibrary/embeddings
- **Query Agent**: Uses QueryLibrary with embeddings, but executes only final_sql (no step execution, no repair)

This prevents:
1. Cross-agent SQL reuse (query agent can't find graph-generated SQL)
2. Step-level visualization in query agent
3. Partial step reuse across queries
4. Consistent debugging experience

## Solution Overview

Unify both agents around:
1. **Same document model**: `QueryDocument` for storage
2. **Same storage**: `QueryLibrary` with vector embeddings
3. **Same execution model**: Steps as temp views, then final_sql
4. **Same prompt pattern**: Standalone steps (no CTEs in steps)
5. **RAG inspiration**: Both agents receive similar QueryDocuments as context

## Files to Modify

### Phase 1: Align Query Agent Prompts & Execution

#### 1.1 Update `config/prompts/query_analysis.yaml`

Add step execution model (same pattern as graph_sql_generation.yaml):

```yaml
# After line 27 (after </sql_guidelines>)

  <reserved_words>
  NEVER use SQL reserved words as column aliases:
  - CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP, DATE, TIME, YEAR, MONTH, DAY
  - Use descriptive alternatives: current_date_value, calculation_date, period_days
  </reserved_words>

  <step_execution_model>
  CRITICAL - How steps are executed:
  1. Each step's SQL becomes a temp view: CREATE TEMP VIEW <step_id> AS <sql>
  2. Steps execute sequentially, each creating a named view
  3. Final SQL references these views by step_id

  Therefore:
  - Step SQL must be STANDALONE (executable independently)
  - Step SQL must NOT contain WITH clauses or reference other steps
  - Step SQL should return a result set that final_sql will use
  - Final SQL combines step results, not redefine them
  </step_execution_model>
```

Update the instructions section to match:

```yaml
# Replace lines 71-87 with:

  <instructions>
  1. Write a brief summary (one sentence) describing what this query answers
  2. Restate the question to show your understanding
  3. Identify the metric type (scalar, table, time_series, comparison)
  4. Map question concepts to actual columns using semantic annotations
  5. Break the query into logical steps (each becomes a temp view)
  6. Generate final SQL that combines step results
  7. Document any assumptions made due to data uncertainty

  STEP FORMAT REQUIREMENTS:
  - Each step SQL must be standalone (no WITH clauses, no step references)
  - Each step returns a result accessible via: SELECT * FROM <step_id>
  - Step IDs must be valid SQL identifiers (no spaces, no reserved words)

  FINAL SQL REQUIREMENTS:
  - Reference step results via: SELECT ... FROM step_1 JOIN step_2 ...
  - OR use subqueries: SELECT (SELECT value FROM step_1) / (SELECT value FROM step_2)
  - Do NOT redefine step logic with CTEs

  EXAMPLE:
  steps:
    - step_id: "monthly_revenue"
      sql: "SELECT DATE_TRUNC('month', \"Date\") as month, SUM(\"Amount\") as revenue FROM typed_sales GROUP BY 1"
    - step_id: "monthly_costs"
      sql: "SELECT DATE_TRUNC('month', \"Date\") as month, SUM(\"Cost\") as cost FROM typed_expenses GROUP BY 1"

  final_sql: "SELECT r.month, r.revenue, c.cost, r.revenue - c.cost as profit FROM monthly_revenue r JOIN monthly_costs c ON r.month = c.month ORDER BY r.month"

  IMPORTANT:
  - Use column names exactly as they appear in the schema
  - If a concept isn't directly available, check business_concept annotations
  - If currency/units are uncertain, document the assumption
  </instructions>
```

#### 1.2 Update `src/dataraum/query/models.py`

Update field descriptions to match graph agent:

```python
# Line 20-25: Update SQLStepOutput
class SQLStepOutput(BaseModel):
    """A single step in SQL generation."""

    step_id: str = Field(description="Unique identifier for this step (e.g., 'filter_active')")
    sql: str = Field(
        description="Standalone DuckDB SQL query for this step. "
        "Must be executable as: CREATE TEMP VIEW {step_id} AS {this_sql}. "
        "Must NOT contain WITH clauses or reference other steps."
    )
    description: str = Field(description="Human-readable description of what this step does")


# Line 69: Update final_sql description
    final_sql: str = Field(
        description="SQL that combines step results to produce the final output. "
        "Reference steps via: SELECT ... FROM step_1 JOIN step_2 ... "
        "Do NOT use CTEs to redefine step logic."
    )
```

#### 1.3 Update `src/dataraum/query/agent.py`

Add step execution and SQL repair:

```python
# Add imports at top
from dataraum.graphs.context import format_entropy_for_prompt

# Add new method after _execute_query (around line 456):

def _execute_with_steps(
    self,
    analysis_output: QueryAnalysisOutput,
    duckdb_conn: duckdb.DuckDBPyConnection,
    execution_context: GraphExecutionContext,
) -> Result[tuple[list[str], list[dict[str, Any]]]]:
    """Execute query with step-by-step temp view creation.

    Similar to GraphAgent execution model:
    1. Create temp view for each step
    2. Execute final_sql that references the views
    3. Repair SQL on failure (up to 2 attempts)
    """
    created_views: list[str] = []
    max_repair_attempts = self.config.get("sql_repair", {}).get("max_attempts", 2)

    try:
        # Execute each step as a temp view
        for step in analysis_output.steps:
            step_id = step.step_id
            current_sql = step.sql

            for attempt in range(max_repair_attempts + 1):
                try:
                    view_sql = f"CREATE OR REPLACE TEMP VIEW {step_id} AS {current_sql}"
                    duckdb_conn.execute(view_sql)
                    created_views.append(step_id)
                    break
                except Exception as e:
                    if attempt < max_repair_attempts:
                        repair_result = self._repair_sql(
                            failed_sql=current_sql,
                            error_message=str(e),
                            step_description=step.description,
                            execution_context=execution_context,
                        )
                        if repair_result.success and repair_result.value:
                            current_sql = repair_result.value
                        else:
                            return Result.fail(f"Step '{step_id}' failed: {e}")
                    else:
                        return Result.fail(f"Step '{step_id}' failed after {max_repair_attempts} repairs: {e}")

        # Execute final SQL
        final_sql = analysis_output.final_sql
        for attempt in range(max_repair_attempts + 1):
            try:
                result = duckdb_conn.execute(final_sql)
                columns = [desc[0] for desc in result.description]
                rows = result.fetchall()
                data = [dict(zip(columns, row, strict=True)) for row in rows]
                return Result.ok((columns, data))
            except Exception as e:
                if attempt < max_repair_attempts:
                    repair_result = self._repair_sql(
                        failed_sql=final_sql,
                        error_message=str(e),
                        step_description="Combine step results into final answer",
                        execution_context=execution_context,
                    )
                    if repair_result.success and repair_result.value:
                        final_sql = repair_result.value
                    else:
                        return Result.fail(f"Final SQL failed: {e}")
                else:
                    return Result.fail(f"Final SQL failed after {max_repair_attempts} repairs: {e}")

    finally:
        # Clean up temp views
        for view_name in created_views:
            try:
                duckdb_conn.execute(f"DROP VIEW IF EXISTS {view_name}")
            except Exception:
                pass

    return Result.fail("Unexpected execution path")


def _repair_sql(
    self,
    failed_sql: str,
    error_message: str,
    step_description: str,
    execution_context: GraphExecutionContext,
) -> Result[str]:
    """Attempt to repair failed SQL using LLM."""
    schema_info = self._build_schema_info(execution_context)

    try:
        system_prompt, user_prompt, temperature = self.renderer.render_split(
            "sql_repair",
            {
                "error_message": error_message,
                "failed_sql": failed_sql,
                "table_schema": json.dumps(schema_info, indent=2),
                "step_description": step_description,
            },
        )
    except Exception as e:
        return Result.fail(f"Failed to render repair prompt: {e}")

    request = ConversationRequest(
        messages=[Message(role="user", content=user_prompt)],
        system=system_prompt,
        max_tokens=self.config.limits.max_output_tokens_per_request,
        temperature=temperature,
    )

    result = self.provider.converse(request)
    if not result.success or not result.value:
        return Result.fail(result.error or "Repair LLM call failed")

    repaired_sql = result.value.content.strip()
    # Strip markdown code blocks if present
    if repaired_sql.startswith("```"):
        lines = repaired_sql.split("\n")
        repaired_sql = "\n".join(lines[1:-1] if lines[-1] == "```" else lines[1:])

    return Result.ok(repaired_sql)
```

Update `analyze()` method to use step execution:

```python
# Replace lines 250-262 with:

        # Execute with step-by-step model (if steps exist) or simple execution
        if analysis_output.steps:
            exec_result = self._execute_with_steps(
                analysis_output=analysis_output,
                duckdb_conn=duckdb_conn,
                execution_context=execution_context,
            )
        else:
            # Fallback for queries without steps
            exec_result = self._execute_query(
                sql=analysis_output.final_sql,
                duckdb_conn=duckdb_conn,
            )
```

### Phase 2: Add RAG Inspiration to Query Agent

#### 2.1 Update `_generate_query()` to include similar documents

```python
# In _generate_query(), after building prompt_context (around line 375):

        # Search for similar queries as inspiration
        similar_docs: list[dict[str, Any]] = []
        if manager and source_id:
            library_matches = self._search_library_for_inspiration(
                session=session,
                manager=manager,
                question=question,
                source_id=source_id,
                limit=3,
                min_similarity=0.5,  # Lower threshold for inspiration
            )
            similar_docs = [m.to_context() for m in library_matches]

        # Add to prompt context
        prompt_context["similar_queries"] = json.dumps(similar_docs, indent=2) if similar_docs else ""
```

#### 2.2 Update `query_analysis.yaml` to use similar queries

```yaml
# Add to user_prompt section (after </data_quality>):

  <similar_queries>
  {similar_queries}
  </similar_queries>

# Add to instructions:

  If similar queries are provided, use them as inspiration:
  - Reuse step patterns that worked for similar questions
  - Adapt column mappings from similar queries
  - Note: Don't copy blindly - adapt to the current question
```

### Phase 3: Graph Agent Saves to QueryLibrary

#### 3.1 Update `src/dataraum/graphs/agent.py`

Add QueryLibrary integration:

```python
# Add imports
from dataraum.query.document import QueryDocument
from dataraum.query.library import QueryLibrary

# In execute() method, after successful execution (around line 234):

        # Save to QueryLibrary for cross-agent reuse
        if manager and source_id:
            self._save_to_library(
                session=session,
                manager=manager,
                source_id=source_id,
                graph=graph,
                generated_code=generated_code,
                execution=execution,
            )

# Add new method:

def _save_to_library(
    self,
    session: Session,
    manager: ConnectionManager,
    source_id: str,
    graph: TransformationGraph,
    generated_code: GeneratedCode,
    execution: GraphExecution,
) -> None:
    """Save graph execution to QueryLibrary for cross-agent reuse."""
    from dataraum.graphs.models import GraphSQLGenerationOutput, SQLStepOutput

    # Convert to GraphSQLGenerationOutput format
    output = GraphSQLGenerationOutput(
        summary=generated_code.summary or f"Calculates {graph.metadata.name}",
        steps=[
            SQLStepOutput(
                step_id=s["step_id"],
                sql=s["sql"],
                description=s["description"],
            )
            for s in generated_code.steps
        ],
        final_sql=generated_code.final_sql,
        column_mappings=generated_code.column_mappings,
    )

    # Build assumptions from execution
    assumptions = [
        {
            "dimension": a.dimension,
            "target": a.target,
            "assumption": a.assumption,
            "basis": a.basis.value,
            "confidence": a.confidence,
        }
        for a in execution.assumptions
    ]

    document = QueryDocument.from_graph_output(output, assumptions)

    try:
        library = QueryLibrary(session, manager)
        library.save(
            source_id=source_id,
            document=document,
            graph_id=graph.graph_id,
            name=graph.metadata.name,
            description=graph.metadata.description,
            confidence_level="GREEN",  # Graphs are pre-validated
        )
    except Exception as e:
        logger.warning(f"Failed to save graph to library: {e}")
```

#### 3.2 Update GraphAgent.execute() signature

```python
def execute(
    self,
    session: Session,
    graph: TransformationGraph,
    context: ExecutionContext,
    parameters: dict[str, Any] | None = None,
    force_regenerate: bool = False,
    *,
    manager: ConnectionManager | None = None,  # NEW
    source_id: str | None = None,              # NEW
) -> Result[GraphExecution]:
```

### Phase 4: Update Pipeline to Pass Manager/SourceID

Update `src/dataraum/pipeline/phases/graph_execution.py` to pass the new parameters to GraphAgent.execute().

## Verification Steps

1. **Clear caches and run graph_execution**:
   ```bash
   uv run python -c "
   from pathlib import Path
   from dataraum.core.connections import get_connection_manager
   from dataraum.graphs.db_models import GeneratedCodeRecord
   from dataraum.query.db_models import QueryLibraryEntry
   from dataraum.pipeline.db_models import PhaseCheckpoint

   cm = get_connection_manager(Path('data'))
   cm.initialize()
   with cm.session_scope() as session:
       session.query(GeneratedCodeRecord).delete()
       session.query(QueryLibraryEntry).delete()
       session.query(PhaseCheckpoint).filter(PhaseCheckpoint.phase_name == 'graph_execution').delete()
       session.commit()
   "

   uv run dataraum run /path/to/data --phase graph_execution --output data
   ```

2. **Verify graph SQL saved to library**:
   ```bash
   uv run python -c "
   from pathlib import Path
   from dataraum.core.connections import get_connection_manager
   from dataraum.query.db_models import QueryLibraryEntry

   cm = get_connection_manager(Path('data'))
   cm.initialize()
   with cm.session_scope() as session:
       for e in session.query(QueryLibraryEntry).all():
           print(f'{e.query_id}: graph_id={e.graph_id}, summary={e.summary[:50]}...')
   "
   ```

3. **Test query agent finds graph SQL**:
   ```bash
   uv run python -c "
   from pathlib import Path
   from dataraum.core.connections import get_connection_manager
   from dataraum.query.library import QueryLibrary

   cm = get_connection_manager(Path('data'))
   cm.initialize()
   with cm.session_scope() as session:
       library = QueryLibrary(session, cm)
       matches = library.find_similar_all('What is the DSO?', source_id='...', min_similarity=0.3)
       for m in matches:
           print(f'{m.similarity:.3f}: {m.entry.summary}')
   "
   ```

4. **Test query agent with steps**:
   ```bash
   uv run dataraum query "What was total revenue last month?" --source-id ... --output data
   ```

## Summary

| Change | Files | Purpose |
|--------|-------|---------|
| Prompt alignment | query_analysis.yaml | Standalone steps, reserved words |
| Model alignment | query/models.py | Field descriptions match graph |
| Step execution | query/agent.py | Execute steps as temp views |
| SQL repair | query/agent.py | Retry failed SQL |
| RAG inspiration | query/agent.py, query_analysis.yaml | Similar docs in context |
| Library save | graphs/agent.py | Graph SQL → QueryLibrary |
| Pipeline update | phases/graph_execution.py | Pass manager/source_id |

## Scope

- **In scope**: Storage unification, execution alignment, RAG inspiration
- **Out of scope**: Step-level matching (match individual steps, not whole queries)
- **Future**: Step-level reuse, visualization UI
