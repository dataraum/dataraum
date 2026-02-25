"""Query Agent for natural language to SQL conversion.

This agent converts natural language questions into executable SQL
with entropy awareness, assumption tracking, and snippet knowledge base.

The agent uses the SQL Knowledge Base:
1. Discover validated SQL snippets from previous graph/query executions
2. Inject snippets into LLM prompt as validated building blocks
3. Generate SQL using LLM (with snippet hints)
4. Track snippet usage for stabilization metrics
5. Save novel query patterns as snippets for future reuse

Usage:
    agent = QueryAgent(config, provider, renderer, cache)
    result = agent.analyze(
        session=session,
        duckdb_conn=conn,
        question="What was total revenue last month?",
        table_ids=["t1", "t2"],
    )
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from sqlalchemy import select

from dataraum.core.logging import get_logger
from dataraum.core.models.base import Result
from dataraum.entropy.contracts import (
    ConfidenceLevel,
    ContractEvaluation,
)
from dataraum.graphs.context import build_execution_context, format_context_for_prompt
from dataraum.llm.features._base import LLMFeature
from dataraum.llm.providers.base import ConversationRequest, Message, ToolDefinition, ToolResult
from dataraum.storage import Table

from .models import (
    QueryAnalysisOutput,
    QueryResult,
    SQLStepOutput,
    assumption_output_to_query_assumption,
)
from .snippet_utils import determine_usage_type

if TYPE_CHECKING:
    import duckdb
    from sqlalchemy.orm import Session

    from dataraum.graphs.context import GraphExecutionContext
    from dataraum.graphs.models import QueryAssumption

logger = get_logger(__name__)

# Maximum individual snippets for full injection mode.
# Above this, switch to tool-based search where the LLM picks terms.
_MAX_FULL_INJECT = 200


@dataclass
class DiscoveryResult:
    """Result of snippet discovery — determines LLM mode."""

    snippets: list[dict[str, Any]]  # Pre-fetched snippets (Mode 1) or empty (Mode 2)
    vocabulary: dict[str, list[str]] | None = None  # Set when Mode 2 (tool search)
    mode: str = "full"  # "full" or "tool_search"


@dataclass
class QueryContext:
    """Context for query execution."""

    session: Session
    duckdb_conn: duckdb.DuckDBPyConnection
    table_ids: list[str]
    source_id: str | None = None

    # Rich metadata context
    execution_context: GraphExecutionContext | None = None

    # Contract settings
    contract_name: str | None = None
    auto_contract: bool = False


class QueryAgent(LLMFeature):
    """Agent for converting natural language questions to SQL.

    Extends LLMFeature to provide:
    - Natural language understanding
    - Schema-aware SQL generation
    - Entropy-aware assumption tracking
    - Contract-based confidence levels
    """

    def analyze(
        self,
        session: Session,
        duckdb_conn: duckdb.DuckDBPyConnection,
        question: str,
        table_ids: list[str],
        *,
        contract: str | None = None,
        auto_contract: bool = False,
        source_id: str | None = None,
        ephemeral: bool = False,
    ) -> Result[QueryResult]:
        """Analyze a natural language question and generate SQL.

        Uses snippet knowledge base for validated SQL patterns.

        Args:
            session: SQLAlchemy session for metadata access
            duckdb_conn: DuckDB connection for schema introspection
            question: Natural language question
            table_ids: List of table IDs to query against
            contract: Explicit contract name for evaluation
            auto_contract: If True, find the strictest passing contract
            source_id: Optional source ID for context
            ephemeral: If True, don't save novel snippets

        Returns:
            Result containing QueryResult with SQL, data, and confidence level
        """
        execution_id = str(uuid4())

        # Filter to only typed tables (these are the tables the LLM should query)
        typed_table_ids = [
            t.table_id
            for t in session.execute(
                select(Table).where(
                    Table.table_id.in_(table_ids),
                    Table.layer == "typed",
                )
            )
            .scalars()
            .all()
        ]

        if not typed_table_ids:
            return Result.fail("No typed tables found. Run the typing phase first.")

        # Build rich context (using only typed tables)
        try:
            execution_context = build_execution_context(
                session=session,
                table_ids=typed_table_ids,
                duckdb_conn=duckdb_conn,
            )
        except Exception as e:
            logger.error("context_build_failed", error=str(e))
            return Result.fail(f"Failed to build context: {e}")

        # Evaluate contract using column_summaries from execution_context
        contract_evaluation: ContractEvaluation | None = None
        confidence_level = ConfidenceLevel.YELLOW  # Default when unknown

        if execution_context.column_summaries:
            from dataraum.entropy.contracts import (
                evaluate_contract,
                find_best_contract,
                get_contract,
            )

            if contract and not auto_contract:
                # Explicit contract — validate it exists
                if get_contract(contract) is None:
                    return Result.fail(f"Contract not found: {contract}")
                contract_evaluation = evaluate_contract(
                    execution_context.column_summaries, contract
                )
                confidence_level = contract_evaluation.confidence_level
            elif auto_contract:
                best_name, best_eval = find_best_contract(
                    execution_context.column_summaries
                )
                if best_name and best_eval:
                    contract = best_name
                    contract_evaluation = best_eval
                    confidence_level = best_eval.confidence_level
                else:
                    contract = "exploratory_analysis"
                    confidence_level = ConfidenceLevel.RED
            else:
                # Default: evaluate exploratory_analysis
                contract = "exploratory_analysis"
                try:
                    contract_evaluation = evaluate_contract(
                        execution_context.column_summaries, "exploratory_analysis"
                    )
                    confidence_level = contract_evaluation.confidence_level
                except ValueError:
                    readiness = (execution_context.entropy_summary or {}).get(
                        "overall_readiness", "investigate"
                    )
                    if readiness == "blocked":
                        confidence_level = ConfidenceLevel.RED
                    elif readiness == "investigate":
                        confidence_level = ConfidenceLevel.YELLOW
                    else:
                        confidence_level = ConfidenceLevel.GREEN
        elif execution_context.entropy_summary:
            # No column summaries but have entropy summary — use heuristic
            readiness = execution_context.entropy_summary.get("overall_readiness", "investigate")
            if readiness == "blocked":
                confidence_level = ConfidenceLevel.RED
            elif readiness == "investigate":
                confidence_level = ConfidenceLevel.YELLOW
            else:
                confidence_level = ConfidenceLevel.GREEN

        # Check if query is blocked
        if confidence_level == ConfidenceLevel.RED and contract_evaluation:
            return Result.ok(
                QueryResult(
                    execution_id=execution_id,
                    question=question,
                    success=False,
                    confidence_level=ConfidenceLevel.RED,
                    contract=contract,
                    contract_evaluation=contract_evaluation,
                    answer=self._format_blocked_response(contract_evaluation),
                    error="Query blocked due to data quality issues",
                )
            )

        # Discover validated SQL snippets from the knowledge base
        discovery = self._discover_snippets(
            session=session,
            question=question,
            schema_mapping_id=source_id or "default",
        )

        # Index provided snippets by snippet_id for post-LLM resolution and tracking
        snippet_id_index: dict[str, dict[str, Any]] = {}
        if discovery.snippets:
            for s in discovery.snippets:
                sid = s.get("snippet_id")
                if sid:
                    snippet_id_index[sid] = s

        gen_result = self._generate_query(
            question=question,
            execution_id=execution_id,
            execution_context=execution_context,
            discovered_snippets=discovery.snippets if discovery.snippets else None,
            search_vocabulary=discovery.vocabulary,
            schema_mapping_id=source_id or "default",
            session=session,
        )

        if not gen_result.success or not gen_result.value:
            return Result.ok(
                QueryResult(
                    execution_id=execution_id,
                    question=question,
                    success=False,
                    confidence_level=confidence_level,
                    contract=contract,
                    contract_evaluation=contract_evaluation,
                    error=gen_result.error or "Query generation failed",
                )
            )

        analysis_output = gen_result.value

        # Resolve snippet_id references: validate, fetch authoritative SQL for reuse
        if snippet_id_index:
            analysis_output = self._resolve_snippet_references(
                analysis_output, snippet_id_index, session,
            )

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

        data: list[dict[str, Any]] | None = None
        columns: list[str] | None = None
        exec_error: str | None = None

        if exec_result.success and exec_result.value:
            columns, data = exec_result.value
        else:
            exec_error = exec_result.error

        # Convert assumptions
        assumptions: list[QueryAssumption] = [
            assumption_output_to_query_assumption(a, execution_id)
            for a in analysis_output.assumptions
        ]

        # Overall entropy score from execution context
        entropy_score = execution_context.overall_entropy_score

        # Format answer
        answer = self._format_answer(
            question=question,
            data=data,
            columns=columns,
            metric_type=analysis_output.metric_type,
            assumptions=assumptions,
            confidence_level=confidence_level,
        )

        # Track snippet usage (compare generated steps to provided snippets)
        if exec_error is None:
            self._track_snippet_usage(
                session=session,
                execution_id=execution_id,
                analysis_output=analysis_output,
                snippet_id_index=snippet_id_index,
            )

        # Save novel snippets for future reuse (skip if ephemeral or failed)
        if exec_error is None and not ephemeral and source_id:
            self._save_novel_snippets(
                session=session,
                execution_id=execution_id,
                analysis_output=analysis_output,
                schema_mapping_id=source_id,
            )

        # Record execution
        if source_id:
            self._record_execution(
                session=session,
                source_id=source_id,
                question=question,
                sql=analysis_output.final_sql,
                success=exec_error is None,
                row_count=len(data) if data else None,
                error_message=exec_error,
                confidence_level=confidence_level,
                contract=contract,
            )

        return Result.ok(
            QueryResult(
                execution_id=execution_id,
                question=question,
                executed_at=datetime.now(UTC),
                answer=answer,
                sql=analysis_output.final_sql,
                data=data,
                columns=columns,
                confidence_level=confidence_level,
                entropy_score=entropy_score,
                assumptions=assumptions,
                contract=contract,
                contract_evaluation=contract_evaluation,
                interpreted_question=analysis_output.interpreted_question,
                metric_type=analysis_output.metric_type,
                column_mappings=analysis_output.column_mappings,
                validation_notes=analysis_output.validation_notes,
                success=exec_error is None,
                error=exec_error,
            )
        )

    def _discover_snippets(
        self,
        session: Session,
        question: str,
        schema_mapping_id: str,
    ) -> DiscoveryResult:
        """Discover relevant SQL snippet graphs from the Knowledge Base.

        Two modes based on corpus size:
        1. Full injection (≤ _MAX_FULL_INJECT snippets): all snippet graphs
           pre-fetched and injected into prompt. Single LLM call.
        2. Tool-based search (> _MAX_FULL_INJECT): vocabulary goes into prompt,
           LLM uses search_snippets tool to find relevant graphs. Multi-turn.

        Args:
            session: SQLAlchemy session
            question: Natural language question
            schema_mapping_id: Schema mapping identifier

        Returns:
            DiscoveryResult with mode, snippets, and optional vocabulary
        """
        from dataraum.query.snippet_library import SnippetLibrary

        library = SnippetLibrary(session)

        all_snippets = library.find_all_for_schema(schema_mapping_id)
        total_count = len(all_snippets)

        if total_count == 0:
            return DiscoveryResult(snippets=[], mode="full")

        if total_count <= _MAX_FULL_INJECT:
            graphs = library.find_all_graphs(schema_mapping_id)
            snippets = self._flatten_graphs(graphs)
            logger.info("full_injection", snippet_count=len(snippets), graph_count=len(graphs))
            return DiscoveryResult(snippets=snippets, mode="full")
        else:
            vocabulary = library.get_search_vocabulary(schema_mapping_id)
            logger.info(
                f"Tool search mode: {total_count} snippets, "
                f"vocabulary has {sum(len(v) for v in vocabulary.values())} terms"
            )
            return DiscoveryResult(
                snippets=[], vocabulary=vocabulary, mode="tool_search"
            )

    @staticmethod
    def _flatten_graphs(graphs: list[Any]) -> list[dict[str, Any]]:
        """Flatten SnippetGraph list into snippet dicts for prompt injection."""
        results = []
        for graph in graphs:
            for snippet in graph.snippets:
                step_id = snippet.standard_field or snippet.snippet_id[:8]
                results.append({
                    "step_id": step_id,
                    "sql": snippet.sql,
                    "description": snippet.description,
                    "snippet_id": snippet.snippet_id,
                    "snippet_type": snippet.snippet_type,
                    "source": snippet.source,
                    "graph_id": graph.graph_id,
                })
        return results

    @staticmethod
    def _build_snippet_context(
        discovered_snippets: list[dict[str, Any]] | None,
        search_vocabulary: dict[str, list[str]] | None,
    ) -> str:
        """Build the snippet context section for the prompt.

        Produces one of three outputs depending on mode:
        - Full injection: validated snippets with reuse instructions
        - Tool search: search vocabulary with tool instructions
        - Neither: empty string (no knowledge base available)
        """
        if discovered_snippets:
            snippets_json = json.dumps(
                [
                    {
                        "step_id": s["step_id"],
                        "snippet_id": s.get("snippet_id", ""),
                        "sql": s["sql"],
                        "description": s["description"],
                        "type": s.get("snippet_type", "unknown"),
                        "source": s.get("source", "unknown"),
                        "graph_id": s.get("graph_id", ""),
                    }
                    for s in discovered_snippets
                ],
                indent=2,
            )
            return (
                "<validated_snippets>\n"
                f"{snippets_json}\n"
                "</validated_snippets>\n\n"
                "SQL KNOWLEDGE BASE:\n"
                "The <validated_snippets> above contains SQL building blocks from the "
                "knowledge base, grouped by source graph. Each has: step_id, snippet_id, "
                "sql, description, type, source, graph_id.\n\n"
                "- `snippet_id`: unique identifier — reference this in your output step "
                "to declare reuse\n"
                '- `source`: provenance — "graph:..." = verified calculation graph, '
                '"query:..." = previous ad-hoc query\n'
                "- `graph_id`: which calculation graph this snippet belongs to\n"
                "- Snippets sharing a graph_id form a complete calculation chain"
            )

        if search_vocabulary:
            vocab_json = json.dumps(search_vocabulary, indent=2)
            return (
                "<available_search_terms>\n"
                f"{vocab_json}\n"
                "</available_search_terms>\n\n"
                "SQL KNOWLEDGE BASE (search mode):\n"
                "The knowledge base contains validated SQL building blocks but is too large "
                "to inject directly.\n"
                "Use the search_snippets tool FIRST to find relevant SQL building blocks.\n"
                "Select terms from the <available_search_terms> vocabulary that relate to "
                "your question.\n"
                "The tool returns complete calculation graphs — all connected SQL steps.\n"
                "After receiving search results, proceed with the analyze_query tool."
            )

        return ""

    def _track_snippet_usage(
        self,
        session: Session,
        execution_id: str,
        analysis_output: QueryAnalysisOutput,
        snippet_id_index: dict[str, dict[str, Any]],
    ) -> None:
        """Record snippet usage deterministically based on snippet_id.

        Classification:
        - step.snippet_id set + normalized SQL matches provided → exact_reuse
        - step.snippet_id set + SQL differs → adapted
        - step.snippet_id is None → newly_generated
        - Provided snippets not referenced by any step → provided_not_used
        """
        from dataraum.query.snippet_library import SnippetLibrary

        library = SnippetLibrary(session)
        referenced_snippet_ids: set[str] = set()

        for step in analysis_output.steps:
            if step.snippet_id is None:
                library.record_usage(
                    execution_id=execution_id,
                    execution_type="query",
                    usage_type="newly_generated",
                    step_id=step.step_id,
                )
            else:
                referenced_snippet_ids.add(step.snippet_id)

                provided = snippet_id_index.get(step.snippet_id)
                authoritative_sql = provided["sql"] if provided else ""
                usage_type = determine_usage_type(step.sql, authoritative_sql)
                is_exact = usage_type == "exact_reuse"

                library.record_usage(
                    execution_id=execution_id,
                    execution_type="query",
                    usage_type=usage_type,
                    snippet_id=step.snippet_id,
                    match_confidence=1.0,
                    sql_match_ratio=1.0 if is_exact else 0.0,
                    step_id=step.step_id,
                )

        # Record provided_not_used for snippets not referenced by any step
        for snippet_id, provided in snippet_id_index.items():
            if snippet_id not in referenced_snippet_ids:
                library.record_usage(
                    execution_id=execution_id,
                    execution_type="query",
                    usage_type="provided_not_used",
                    snippet_id=snippet_id,
                    step_id=provided.get("step_id"),
                )

    def _resolve_snippet_references(
        self,
        analysis_output: QueryAnalysisOutput,
        snippet_id_index: dict[str, dict[str, Any]],
        session: Session,
    ) -> QueryAnalysisOutput:
        """Validate and resolve snippet_id references from LLM output.

        For each step with a snippet_id:
        - Valid ID + normalized SQL matches DB → exact reuse: replace with DB SQL
        - Valid ID + SQL differs → adaptation: keep LLM SQL, snippet_id tracks provenance
        - Unknown ID → hallucination: clear to None, log warning

        Args:
            analysis_output: LLM-generated query analysis
            snippet_id_index: Map of snippet_id -> snippet dict from discovery
            session: SQLAlchemy session for DB lookups

        Returns:
            Updated QueryAnalysisOutput with resolved references
        """
        from dataraum.query.snippet_library import SnippetLibrary

        library = SnippetLibrary(session)
        resolved_steps: list[SQLStepOutput] = []

        for step in analysis_output.steps:
            if step.snippet_id is None:
                resolved_steps.append(step)
                continue

            # Check if snippet_id is in the provided index
            if step.snippet_id not in snippet_id_index:
                # Not in provided set — check DB as fallback
                db_record = library.find_by_id(step.snippet_id)
                if db_record is None:
                    # Hallucinated snippet_id
                    logger.warning(
                        f"Step '{step.step_id}' references unknown snippet_id "
                        f"'{step.snippet_id}' — treating as fresh SQL"
                    )
                    resolved_steps.append(step.model_copy(update={"snippet_id": None}))
                    continue
                authoritative_sql = db_record.sql
            else:
                authoritative_sql = snippet_id_index[step.snippet_id]["sql"]

            # Compare normalized SQL
            if determine_usage_type(step.sql, authoritative_sql) == "exact_reuse":
                # Exact reuse — replace with authoritative SQL
                resolved_steps.append(step.model_copy(update={"sql": authoritative_sql}))
                logger.debug(
                    f"Step '{step.step_id}': exact reuse of snippet '{step.snippet_id}'"
                )
            else:
                # Adaptation — keep LLM SQL, snippet_id tracks provenance
                resolved_steps.append(step)
                logger.debug(
                    f"Step '{step.step_id}': adapted from snippet '{step.snippet_id}'"
                )

        return analysis_output.model_copy(update={"steps": resolved_steps})

    def _save_novel_snippets(
        self,
        session: Session,
        execution_id: str,
        analysis_output: QueryAnalysisOutput,
        schema_mapping_id: str,
    ) -> None:
        """Save novel query steps as snippets for future reuse.

        After a successful query execution, saves any freshly generated steps
        (not reused from existing snippets) as "query" type snippets.
        Steps with snippet_id are skipped — they reference existing snippets.

        Args:
            session: SQLAlchemy session
            execution_id: Query execution ID
            analysis_output: Generated query analysis with steps
            schema_mapping_id: Schema mapping identifier (source_id)
        """
        from dataraum.query.snippet_library import SnippetLibrary

        library = SnippetLibrary(session)

        saved_count = 0
        for step in analysis_output.steps:
            # Skip steps that reference existing snippets
            if step.snippet_id is not None:
                continue

            # Save as a query-type snippet
            library.save_snippet(
                snippet_type="query",
                sql=step.sql,
                description=step.description or f"Query step: {step.step_id}",
                schema_mapping_id=schema_mapping_id,
                source=f"query:{execution_id}",
                standard_field=step.step_id,
                column_mappings=analysis_output.column_mappings,
            )
            saved_count += 1

        if saved_count:
            logger.info("saved_novel_snippets", count=saved_count)

    def _handle_snippet_search(
        self,
        search_terms: list[str],
        vocabulary: dict[str, list[str]],
        schema_mapping_id: str,
        session: Session,
    ) -> str:
        """Handle search_snippets tool call from LLM.

        Validates >= 85% of terms match curated vocabulary.
        Returns matching snippet graphs as JSON.
        """
        from dataraum.query.snippet_library import SnippetLibrary

        # Flatten all vocabulary terms for validation
        all_vocab_terms: set[str] = set()
        for terms in vocabulary.values():
            all_vocab_terms.update(t.lower() for t in terms)

        if not search_terms:
            return json.dumps({"error": "No search terms provided"})

        matched = sum(1 for t in search_terms if t.lower() in all_vocab_terms)
        match_ratio = matched / len(search_terms)

        if match_ratio < 0.85:
            return json.dumps({
                "error": f"Only {match_ratio:.0%} of terms match vocabulary (need >= 85%)",
                "hint": "Use terms from the <available_search_terms> list",
            })

        library = SnippetLibrary(session)
        graphs = library.find_graphs_by_terms(
            question=" ".join(search_terms),
            schema_mapping_id=schema_mapping_id,
            vocabulary=vocabulary,
            limit=50,
        )

        result = []
        for graph in graphs:
            result.append({
                "graph_id": graph.graph_id,
                "source": graph.source,
                "source_type": graph.source_type,
                "snippets": [
                    {
                        "step_id": s.standard_field or s.snippet_id[:8],
                        "snippet_id": s.snippet_id,
                        "sql": s.sql,
                        "description": s.description,
                        "snippet_type": s.snippet_type,
                    }
                    for s in graph.snippets
                ],
            })

        return json.dumps(result, indent=2)

    def _generate_query(
        self,
        question: str,
        execution_id: str,
        execution_context: GraphExecutionContext,
        discovered_snippets: list[dict[str, Any]] | None = None,
        *,
        search_vocabulary: dict[str, list[str]] | None = None,
        schema_mapping_id: str = "default",
        session: Session | None = None,
    ) -> Result[QueryAnalysisOutput]:
        """Use LLM to analyze question and generate SQL.

        Supports two modes:
        - Full injection: discovered_snippets provided, single LLM call
        - Tool search: search_vocabulary provided, multi-turn with search tool
        """
        # Build schema information
        schema_info = self._build_schema_info(execution_context)

        # Format context for prompt
        context_str = format_context_for_prompt(execution_context)

        # Format entropy warnings from execution_context
        entropy_warnings = ""
        if execution_context.entropy_summary:
            from dataraum.graphs.context import format_entropy_for_prompt

            entropy_warnings = format_entropy_for_prompt(execution_context)

        # Build snippet context (mode-dependent: full injection OR search vocabulary)
        snippet_context = self._build_snippet_context(
            discovered_snippets=discovered_snippets,
            search_vocabulary=search_vocabulary,
        )

        # Build field mappings string
        field_mappings_str = ""
        if execution_context.field_mappings:
            from dataraum.graphs.field_mapping import format_mappings_for_prompt

            field_mappings_str = format_mappings_for_prompt(execution_context.field_mappings)

        # Build prompt context
        prompt_context = {
            "question": question,
            "schema_info": json.dumps(schema_info, indent=2),
            "dataset_context": context_str,
            "field_mappings": field_mappings_str,
            "entropy_warnings": entropy_warnings
            or "Data quality assessment not available - proceed with caution.",
            "snippet_context": snippet_context,
        }

        # Render prompt
        try:
            system_prompt, user_prompt, temperature = self.renderer.render_split(
                "query_analysis", prompt_context
            )
        except Exception as e:
            return Result.fail(f"Failed to render prompt: {e}")

        # Compute prompt hash
        prompt_hash = hashlib.sha256(user_prompt.encode()).hexdigest()[:16]
        logger.debug("query_prompt_rendered", prompt_hash=prompt_hash)

        # Define tools
        analyze_tool = ToolDefinition(
            name="analyze_query",
            description="Analyze the question and provide SQL to answer it",
            input_schema=QueryAnalysisOutput.model_json_schema(),
        )

        tools = [analyze_tool]
        use_search_tool = bool(search_vocabulary and not discovered_snippets)

        if use_search_tool:
            search_tool = ToolDefinition(
                name="search_snippets",
                description=(
                    "Search the SQL Knowledge Base for validated snippet graphs. "
                    "Select search terms from the <available_search_terms> list. "
                    "Returns complete calculation graphs matching your terms."
                ),
                input_schema={
                    "type": "object",
                    "properties": {
                        "search_terms": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": (
                                "Terms from the available_search_terms vocabulary. "
                                "At least 85% must match the curated list."
                            ),
                        },
                    },
                    "required": ["search_terms"],
                },
            )
            tools.append(search_tool)

        # Build conversation
        messages = [Message(role="user", content=user_prompt)]
        tool_choice = (
            None if use_search_tool
            else {"type": "tool", "name": "analyze_query"}
        )
        model = self.provider.get_model_for_tier("balanced")
        max_turns = 3

        for _ in range(max_turns):
            request = ConversationRequest(
                messages=messages,
                system=system_prompt,
                tools=tools,
                tool_choice=tool_choice,
                max_tokens=self.config.limits.max_output_tokens_per_request,
                temperature=temperature,
                model=model,
            )

            result = self.provider.converse(request)
            if not result.success or not result.value:
                return Result.fail(result.error or "LLM call failed")

            response = result.value

            if not response.tool_calls:
                if response.content:
                    try:
                        output = QueryAnalysisOutput.model_validate(
                            json.loads(response.content)
                        )
                        return Result.ok(output)
                    except Exception:
                        pass
                return Result.fail("LLM did not use any tool")

            tool_call = response.tool_calls[0]

            if tool_call.name == "search_snippets":
                assert session is not None and search_vocabulary is not None
                search_result = self._handle_snippet_search(
                    search_terms=tool_call.input.get("search_terms", []),
                    vocabulary=search_vocabulary,
                    schema_mapping_id=schema_mapping_id,
                    session=session,
                )

                messages.append(Message(
                    role="assistant", content="", tool_calls=[tool_call],
                ))
                messages.append(Message(
                    role="tool_result",
                    content=[ToolResult(
                        tool_use_id=tool_call.id, content=search_result,
                    )],
                ))

                # After search, force analyze_query on next turn
                tool_choice = {"type": "tool", "name": "analyze_query"}
                continue

            elif tool_call.name == "analyze_query":
                try:
                    output = QueryAnalysisOutput.model_validate(tool_call.input)
                    return Result.ok(output)
                except Exception as e:
                    return Result.fail(f"Failed to validate tool response: {e}")

            else:
                return Result.fail(f"Unexpected tool call: {tool_call.name}")

        return Result.fail("Max tool turns exceeded")

    @staticmethod
    def _is_read_only_sql(sql: str) -> str | None:
        """Check if SQL is read-only. Returns error message if not, None if safe."""
        import re

        sql_upper = sql.upper().strip()
        # Check for DML/DDL keywords as standalone words (not inside strings)
        dangerous = ["INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "TRUNCATE"]
        for keyword in dangerous:
            if re.search(rf"\b{keyword}\b", sql_upper):
                return f"Query contains dangerous keyword: {keyword}"
        # Allow CREATE only for TEMP VIEW (used by step execution)
        if re.search(r"\bCREATE\b", sql_upper) and not re.search(
            r"\bCREATE\s+(OR\s+REPLACE\s+)?TEMP\s+VIEW\b", sql_upper
        ):
            return "Query contains CREATE (only TEMP VIEW allowed)"
        return None

    def _execute_query(
        self,
        sql: str,
        duckdb_conn: duckdb.DuckDBPyConnection,
    ) -> Result[tuple[list[str], list[dict[str, Any]]]]:
        """Execute generated SQL and return results."""
        try:
            # Validate SQL is read-only
            safety_error = self._is_read_only_sql(sql)
            if safety_error:
                return Result.fail(safety_error)

            result = duckdb_conn.execute(sql)
            columns = [desc[0] for desc in result.description]
            rows = result.fetchall()

            data = [dict(zip(columns, row, strict=True)) for row in rows]

            return Result.ok((columns, data))
        except Exception as e:
            return Result.fail(f"SQL execution failed: {e}")

    def _build_schema_info(
        self,
        execution_context: GraphExecutionContext,
    ) -> dict[str, Any]:
        """Build schema information for prompt.

        Note: Table names are prefixed with 'typed_' to match the actual
        DuckDB table names. The context stores base names but DuckDB has
        layer-prefixed tables (typed_*, raw_*, quarantine_*).
        """
        tables = []
        for table_ctx in execution_context.tables:
            columns = []
            for col_ctx in table_ctx.columns:
                col_info: dict[str, Any] = {
                    "name": col_ctx.column_name,
                    "data_type": col_ctx.data_type,
                }
                if col_ctx.semantic_role:
                    col_info["semantic_role"] = col_ctx.semantic_role
                if col_ctx.business_concept:
                    col_info["business_concept"] = col_ctx.business_concept
                columns.append(col_info)

            # Use the actual DuckDB table name from context
            duckdb_table_name = table_ctx.duckdb_name or table_ctx.table_name

            tables.append(
                {
                    "name": duckdb_table_name,
                    "row_count": table_ctx.row_count,
                    "columns": columns,
                }
            )

        return {"tables": tables}

    def _format_answer(
        self,
        question: str,
        data: list[dict[str, Any]] | None,
        columns: list[str] | None,
        metric_type: str,
        assumptions: list[QueryAssumption],
        confidence_level: ConfidenceLevel,
    ) -> str:
        """Format the answer for display."""
        if data is None:
            return "Unable to retrieve data."

        if not data:
            return "No data found matching your query."

        # Simple formatting based on metric type
        if metric_type == "scalar" and len(data) == 1 and columns:
            # Single value answer
            row = data[0]
            col = columns[0]
            value = row.get(col, "N/A")
            return f"The answer is: {value}"

        # Default: describe the data
        row_count = len(data)
        col_count = len(columns) if columns else 0

        return f"Found {row_count} result(s) with {col_count} column(s)."

    def _format_blocked_response(self, evaluation: ContractEvaluation) -> str:
        """Format response when query is blocked."""
        lines = [
            f"Cannot provide answer for {evaluation.contract_display_name} because:",
            "",
        ]

        for v in evaluation.violations:
            if v.dimension:
                lines.append(f"❌ {v.dimension}: {v.actual:.2f} (max allowed: {v.max_allowed:.2f})")
            elif v.condition:
                lines.append(f"❌ {v.condition}: {v.details}")

        lines.append("")
        lines.append("Options:")
        lines.append("1. Use a less strict contract (e.g., exploratory_analysis)")
        lines.append("2. Resolve the data quality issues first")

        return "\n".join(lines)


    def _record_execution(
        self,
        session: Session,
        source_id: str,
        question: str,
        sql: str,
        success: bool,
        row_count: int | None,
        error_message: str | None,
        confidence_level: ConfidenceLevel,
        contract: str | None,
        entropy_action: str | None = None,
    ) -> None:
        """Record a query execution for audit trail.

        Args:
            session: SQLAlchemy session
            source_id: Source ID
            question: Question asked
            sql: SQL executed
            success: Whether execution succeeded
            row_count: Number of rows returned
            error_message: Error if failed
            confidence_level: Confidence level
            contract: Contract name
            entropy_action: Entropy action determined at query time
        """
        from dataraum.query.db_models import QueryExecutionRecord

        record = QueryExecutionRecord(
            execution_id=str(uuid4()),
            source_id=source_id,
            question=question,
            sql_executed=sql,
            executed_at=datetime.now(UTC),
            success=success,
            row_count=row_count,
            error_message=error_message,
            confidence_level=confidence_level.value,
            contract_name=contract,
            entropy_action=entropy_action,
        )
        session.add(record)

    def _execute_with_steps(
        self,
        analysis_output: QueryAnalysisOutput,
        duckdb_conn: duckdb.DuckDBPyConnection,
        execution_context: GraphExecutionContext,
    ) -> Result[tuple[list[str], list[dict[str, Any]]]]:
        """Execute query with step-by-step temp view creation.

        Delegates to shared execute_sql_steps() for the common pattern of
        creating temp views per step, executing final SQL, and cleanup.

        Args:
            analysis_output: Generated query analysis with steps
            duckdb_conn: DuckDB connection
            execution_context: Context for schema info (used in repair)

        Returns:
            Result with (columns, data) on success
        """
        from dataraum.query.execution import SQLStep, execute_sql_steps

        # Convert analysis steps to shared format
        steps = [
            SQLStep(step_id=s.step_id, sql=s.sql, description=s.description)
            for s in analysis_output.steps
        ]

        # Create repair function that captures execution_context
        def repair_fn(failed_sql: str, error_msg: str, description: str) -> Result[str]:
            return self._repair_sql(
                failed_sql=failed_sql,
                error_message=error_msg,
                step_description=description,
                execution_context=execution_context,
            )

        result = execute_sql_steps(
            steps=steps,
            final_sql=analysis_output.final_sql,
            duckdb_conn=duckdb_conn,
            max_repair_attempts=2,
            repair_fn=repair_fn,
            return_table=True,
        )

        if not result.success or not result.value:
            return Result.fail(result.error or "Execution failed")

        exec_result = result.value
        columns = exec_result.columns or []
        rows = exec_result.rows or []
        data = [dict(zip(columns, row, strict=True)) for row in rows]
        return Result.ok((columns, data))

    def _repair_sql(
        self,
        failed_sql: str,
        error_message: str,
        step_description: str,
        execution_context: GraphExecutionContext,
    ) -> Result[str]:
        """Attempt to repair failed SQL using LLM.

        Args:
            failed_sql: The SQL that failed
            error_message: Error message from DuckDB
            step_description: What the SQL should do
            execution_context: Context for schema info

        Returns:
            Result with repaired SQL on success
        """
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
            if lines[-1].strip() == "```":
                repaired_sql = "\n".join(lines[1:-1])
            else:
                repaired_sql = "\n".join(lines[1:])

        return Result.ok(repaired_sql)
