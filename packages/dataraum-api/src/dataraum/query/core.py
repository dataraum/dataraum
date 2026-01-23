"""Core library function for Query Agent.

This module provides the main entry point for answering questions.
It is designed to be called by CLI, API, and MCP interfaces.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import select

from dataraum.core.logging import get_logger
from dataraum.core.models.base import Result
from dataraum.llm import LLMCache, PromptRenderer, create_provider, load_llm_config
from dataraum.storage import Source, Table

from .agent import QueryAgent
from .models import QueryResult

if TYPE_CHECKING:
    import duckdb
    from sqlalchemy.orm import Session

logger = get_logger(__name__)


def answer_question(
    question: str,
    session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
    source_id: str,
    *,
    contract: str | None = None,
    auto_contract: bool = False,
    table_ids: list[str] | None = None,
) -> Result[QueryResult]:
    """Answer a natural language question about the data.

    This is the main library function that CLI, API, and MCP all call.
    It handles all the complexity of LLM interaction, entropy evaluation,
    and contract-based confidence levels.

    Args:
        question: Natural language question to answer
        session: SQLAlchemy session for metadata access
        duckdb_conn: DuckDB connection for data queries
        source_id: Source ID to query against
        contract: Explicit contract name (e.g., "executive_dashboard")
        auto_contract: If True, find the strictest passing contract
        table_ids: Optional list of specific table IDs (defaults to all tables in source)

    Returns:
        Result containing QueryResult with:
        - answer: Natural language response
        - sql: Generated SQL
        - data: Query results
        - confidence_level: GREEN/YELLOW/ORANGE/RED
        - assumptions: List of assumptions made
        - contract_evaluation: Full contract evaluation if contract specified

    Example:
        >>> from dataraum.query import answer_question
        >>> result = answer_question(
        ...     question="What was total revenue last month?",
        ...     session=session,
        ...     duckdb_conn=conn,
        ...     source_id="src_abc123",
        ...     contract="executive_dashboard",
        ... )
        >>> if result.success:
        ...     print(result.value.confidence_level.emoji)  # 🟢
        ...     print(result.value.answer)
    """
    # Verify source exists
    stmt = select(Source).where(Source.source_id == source_id)
    source = session.execute(stmt).scalar_one_or_none()

    if source is None:
        return Result.fail(f"Source not found: {source_id}")

    # Get table IDs if not provided
    if table_ids is None:
        tables_stmt = select(Table).where(Table.source_id == source_id)
        tables = list(session.execute(tables_stmt).scalars().all())

        if not tables:
            return Result.fail("Source has no tables")

        table_ids = [t.table_id for t in tables]

    # Initialize LLM components
    try:
        config = load_llm_config()

        # Create provider
        provider_config = config.providers.get(config.active_provider)
        if not provider_config:
            return Result.fail(f"Provider '{config.active_provider}' not configured")

        provider = create_provider(config.active_provider, provider_config.model_dump())
        renderer = PromptRenderer()
        cache = LLMCache()
    except Exception as e:
        logger.error(f"Failed to initialize LLM: {e}")
        return Result.fail(f"Failed to initialize LLM: {e}")

    # Create agent and analyze
    agent = QueryAgent(
        config=config,
        provider=provider,
        prompt_renderer=renderer,
        cache=cache,
    )

    return agent.analyze(
        session=session,
        duckdb_conn=duckdb_conn,
        question=question,
        table_ids=table_ids,
        contract=contract,
        auto_contract=auto_contract,
        source_id=source_id,
    )
