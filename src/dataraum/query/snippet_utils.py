"""Utility functions for the SQL Knowledge Base.

Provides:
- SQL similarity comparison (normalized whitespace + Levenshtein ratio)
- Expression normalization (commutative sorting, placeholder substitution)
- Usage type determination (exact_reuse, adapted, etc.)
"""

from __future__ import annotations

import re


def normalize_sql(sql: str) -> str:
    """Normalize SQL for comparison: lowercase, collapse whitespace.

    Args:
        sql: Raw SQL string

    Returns:
        Normalized SQL string
    """
    s = sql.strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s


def sql_similarity(sql_a: str, sql_b: str) -> float:
    """Compute similarity between two SQL strings.

    Uses normalized whitespace + lowercase then Levenshtein ratio.
    This is intentionally simple — used for tracking, not cache keying.

    Args:
        sql_a: First SQL string
        sql_b: Second SQL string

    Returns:
        Similarity ratio 0.0 to 1.0
    """
    a = normalize_sql(sql_a)
    b = normalize_sql(sql_b)

    if a == b:
        return 1.0
    if not a or not b:
        return 0.0

    # Levenshtein ratio: 1 - (edit_distance / max_length)
    distance = _levenshtein_distance(a, b)
    max_len = max(len(a), len(b))
    return 1.0 - (distance / max_len)


def _levenshtein_distance(s1: str, s2: str) -> int:
    """Compute Levenshtein edit distance between two strings.

    Uses the iterative matrix approach with O(min(m,n)) space.

    Args:
        s1: First string
        s2: Second string

    Returns:
        Edit distance (integer)
    """
    if len(s1) < len(s2):
        return _levenshtein_distance(s2, s1)

    if not s2:
        return len(s1)

    previous_row = list(range(len(s2) + 1))
    for i, c1 in enumerate(s1):
        current_row = [i + 1]
        for j, c2 in enumerate(s2):
            # Insertions, deletions, substitutions
            insertions = previous_row[j + 1] + 1
            deletions = current_row[j] + 1
            substitutions = previous_row[j] + (c1 != c2)
            current_row.append(min(insertions, deletions, substitutions))
        previous_row = current_row

    return previous_row[-1]


# --- Expression Normalization ---

# Regex to match a binary expression: operand OPERATOR operand
# Supports nested parenthesized expressions
_OPERATOR_PATTERN = re.compile(r"([+\-*/])")


def normalize_expression(expression: str) -> tuple[str, list[str], dict[str, str]]:
    """Normalize a mathematical expression for template matching.

    Canonicalizes commutative operations by sorting operands alphabetically.
    Replaces concrete field names with placeholders ({A}, {B}, ...).

    Rules:
    - a * b -> sort operands (a < b alphabetically)
    - a + b -> sort operands
    - a / b -> preserve order (NOT commutative)
    - a - b -> preserve order (NOT commutative)
    - Parentheses preserved for precedence

    Args:
        expression: A formula expression like "(accounts_receivable / revenue) * days_in_period"

    Returns:
        Tuple of:
        - normalized_expression: Expression with sorted commutative ops and placeholders
        - sorted_input_fields: Alphabetically sorted list of unique field names
        - input_bindings: Mapping from placeholder ({A}, {B}, ...) to field name
    """
    # Extract unique field names (identifiers that aren't numbers or operators)
    fields = _extract_fields(expression)
    sorted_fields = sorted(set(fields))

    # Create placeholder mapping
    bindings: dict[str, str] = {}
    field_to_placeholder: dict[str, str] = {}
    for i, field_name in enumerate(sorted_fields):
        placeholder = "{" + chr(65 + i) + "}"  # {A}, {B}, {C}, ...
        bindings[placeholder] = field_name
        field_to_placeholder[field_name] = placeholder

    # Replace field names with placeholders (longest first to avoid partial matches)
    normalized = expression.strip()
    for field_name in sorted(field_to_placeholder.keys(), key=len, reverse=True):
        normalized = normalized.replace(field_name, field_to_placeholder[field_name])

    # Sort commutative operations
    normalized = _sort_commutative_ops(normalized)

    return normalized, sorted_fields, bindings


def _extract_fields(expression: str) -> list[str]:
    """Extract field name identifiers from an expression.

    Matches sequences of word characters (letters, digits, underscore)
    that are not pure numbers.

    Args:
        expression: Math expression string

    Returns:
        List of field name strings (may contain duplicates)
    """
    # Match word sequences that aren't pure numbers
    tokens = re.findall(r"\b([a-zA-Z_][a-zA-Z0-9_]*)\b", expression)
    return tokens


def _sort_commutative_ops(expr: str) -> str:
    """Sort operands of commutative operations (+ and *).

    Handles top-level commutative operations. For nested expressions,
    works on the string level after placeholder substitution.

    This is simple text manipulation, not full symbolic algebra.
    Handles common patterns: ratio * constant, sum of components.

    Args:
        expr: Expression with placeholders

    Returns:
        Expression with sorted commutative operands
    """
    # Handle multiplication: split on *, sort parts, rejoin
    expr = _sort_binary_commutative(expr, "*")
    # Handle addition: split on +, sort parts, rejoin
    expr = _sort_binary_commutative(expr, "+")
    return expr


def _sort_binary_commutative(expr: str, operator: str) -> str:
    """Sort operands around a commutative operator.

    Only sorts at the top level (respects parentheses).

    Args:
        expr: Expression string
        operator: The operator to sort around ("*" or "+")

    Returns:
        Expression with sorted operands for this operator
    """
    parts = _split_at_top_level(expr, operator)
    if len(parts) <= 1:
        return expr

    # Sort the parts (strip whitespace for comparison, preserve structure)
    stripped = [p.strip() for p in parts]
    sorted_parts = sorted(stripped)

    op_with_space = f" {operator} "
    return op_with_space.join(sorted_parts)


def _split_at_top_level(expr: str, operator: str) -> list[str]:
    """Split expression by operator only at top level (outside parentheses).

    Args:
        expr: Expression string
        operator: Single character operator

    Returns:
        List of parts
    """
    parts: list[str] = []
    current: list[str] = []
    depth = 0

    i = 0
    while i < len(expr):
        ch = expr[i]
        if ch == "(":
            depth += 1
            current.append(ch)
        elif ch == ")":
            depth -= 1
            current.append(ch)
        elif ch == operator and depth == 0:
            # Check it's not part of ** (power operator)
            if operator == "*" and i + 1 < len(expr) and expr[i + 1] == "*":
                current.append(ch)
            else:
                parts.append("".join(current))
                current = []
        else:
            current.append(ch)
        i += 1

    parts.append("".join(current))
    return parts


# --- Usage Type Determination ---

# Threshold for considering SQL as "adapted" vs "provided_not_used"
_ADAPTED_THRESHOLD = 0.8


def determine_usage_type(
    generated_sql: str,
    provided_snippet_sql: str | None,
) -> str:
    """Determine how a generated SQL step relates to a provided snippet.

    Args:
        generated_sql: The SQL actually generated/used
        provided_snippet_sql: The snippet SQL that was provided (None if no snippet)

    Returns:
        One of: "exact_reuse", "adapted", "provided_not_used", "newly_generated"
    """
    if provided_snippet_sql is None:
        return "newly_generated"

    similarity = sql_similarity(generated_sql, provided_snippet_sql)

    if similarity >= 0.99:
        return "exact_reuse"
    elif similarity >= _ADAPTED_THRESHOLD:
        return "adapted"
    else:
        return "provided_not_used"


__all__ = [
    "normalize_sql",
    "sql_similarity",
    "normalize_expression",
    "determine_usage_type",
]
