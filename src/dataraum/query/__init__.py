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
from dataraum.query.db_models import QueryExecutionRecord
from dataraum.query.embeddings import QueryEmbeddings, SimilarQuery
from dataraum.query.models import (
    QueryAnalysisOutput,
    QueryResult,
)
from dataraum.query.snippet_library import SnippetLibrary, SnippetMatch
from dataraum.query.snippet_models import SnippetUsageRecord, SQLSnippetRecord
from dataraum.query.snippet_utils import (
    determine_usage_type,
    normalize_expression,
    normalize_sql,
    sql_similarity,
)

__all__ = [
    "QueryAgent",
    "QueryAnalysisOutput",
    "QueryEmbeddings",
    "QueryExecutionRecord",
    "QueryResult",
    "SQLSnippetRecord",
    "SimilarQuery",
    "SnippetLibrary",
    "SnippetMatch",
    "SnippetUsageRecord",
    "answer_question",
    "determine_usage_type",
    "normalize_expression",
    "normalize_sql",
    "sql_similarity",
]
