"""Query-time entropy refinement.

This module provides functionality to refine entropy interpretations
based on how columns are used in a specific query. The query is passed
as context to the LLM interpreter, which determines the relevant
assumptions and risks in a single batch call.

Usage:
    from dataraum.entropy.query_refinement import (
        refine_interpretations_for_query,
    )

    # Refine interpretations for columns used in a query
    result = refine_interpretations_for_query(
        session=session,
        entropy_context=entropy_ctx,
        query=sql_query,
        interpreter=interpreter,  # Optional
    )
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from sqlalchemy.orm import Session

from dataraum.entropy.interpretation import (
    EntropyInterpretation,
    InterpretationInput,
)
from dataraum.entropy.models import EntropyContext

if TYPE_CHECKING:
    from dataraum.entropy.interpretation import EntropyInterpreter


@dataclass
class QueryRefinementResult:
    """Result of query-time refinement."""

    query: str

    # Refined interpretations for columns found in query
    column_interpretations: dict[str, EntropyInterpretation] = field(default_factory=dict)
    # Key: "{table_name}.{column_name}"

    # Columns from entropy context that matched the query
    matched_columns: list[str] = field(default_factory=list)


def find_columns_in_query(
    query: str,
    entropy_context: EntropyContext,
) -> list[str]:
    """Find which columns from the entropy context appear in the query.

    Uses simple string matching to identify column names.
    The LLM will determine actual usage context.

    Args:
        query: SQL or natural language query
        entropy_context: Entropy context with column profiles

    Returns:
        List of matched column keys (table.column format)
    """
    matched = []
    query_lower = query.lower()

    for key, profile in entropy_context.column_profiles.items():
        column_name = profile.column_name.lower()

        # Check if column name appears in query
        # Use word boundary matching to avoid partial matches
        pattern = rf"\b{re.escape(column_name)}\b"
        if re.search(pattern, query_lower):
            matched.append(key)

    return matched


def refine_interpretations_for_query(
    session: Session,
    entropy_context: EntropyContext,
    query: str,
    *,
    interpreter: EntropyInterpreter | None = None,
    use_fallback: bool = True,
    detected_types: dict[str, str] | None = None,
    business_descriptions: dict[str, str] | None = None,
) -> QueryRefinementResult:
    """Refine entropy interpretations for columns used in a query.

    Identifies columns from the entropy context that appear in the query,
    then generates refined interpretations with the query as context.
    Makes a single batch LLM call for all columns.

    Args:
        session: Database session
        entropy_context: Pre-computed entropy context
        query: SQL or natural language query
        interpreter: Optional EntropyInterpreter for LLM interpretation
        use_fallback: Whether to use fallback interpretation if LLM fails
        detected_types: Optional map of column keys to detected types
        business_descriptions: Optional map of column keys to descriptions

    Returns:
        QueryRefinementResult with refined interpretations
    """
    result = QueryRefinementResult(query=query)

    detected_types = detected_types or {}
    business_descriptions = business_descriptions or {}

    # Find columns that appear in the query
    matched_keys = find_columns_in_query(query, entropy_context)
    result.matched_columns = matched_keys

    if not matched_keys:
        return result

    # Build interpretation inputs for all matched columns
    inputs: list[InterpretationInput] = []
    for key in matched_keys:
        profile = entropy_context.column_profiles[key]
        input_data = InterpretationInput.from_profile(
            profile=profile,
            detected_type=detected_types.get(key, "unknown"),
            business_description=business_descriptions.get(key),
        )
        inputs.append(input_data)

    # Try batch LLM interpretation with query context
    if interpreter is not None:
        batch_result = interpreter.interpret_batch(
            session=session,
            inputs=inputs,
            query=query,
        )
        if batch_result.success and batch_result.value:
            result.column_interpretations = batch_result.value

    # No fallback - return empty interpretations if LLM fails
    return result


__all__ = [
    "QueryRefinementResult",
    "find_columns_in_query",
    "refine_interpretations_for_query",
]
