"""Query Agent module for natural language to SQL conversion.

This module provides the core query agent functionality that converts
natural language questions into executable SQL with entropy awareness
and assumption tracking.

Usage:
    from dataraum.query import answer_question, QueryResult

    result = answer_question(
        question="What was total revenue last month?",
        session=session,
        duckdb_conn=conn,
        source_id="src_123",
        contract="exploratory_analysis",
    )

    if result.success:
        print(result.value.answer)
        print(result.value.confidence_level)  # GREEN, YELLOW, ORANGE, RED
"""

from dataraum.query.agent import QueryAgent
from dataraum.query.core import answer_question
from dataraum.query.models import (
    QueryAnalysisOutput,
    QueryResult,
)

__all__ = [
    "QueryAgent",
    "QueryAnalysisOutput",
    "QueryResult",
    "answer_question",
]
