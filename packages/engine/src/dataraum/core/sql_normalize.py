"""Canonical SQL comparison for recipe-equality checks (DAT-415).

A view's ``CREATE VIEW`` DDL is re-derived on each begin_session run — the LLM
selects which dimension joins to include. To decide whether a re-run produced a
*genuinely* different view (rather than the same view with cosmetic syntax
differences), we compare the two DDL strings in sqlglot's canonical form
(parse -> re-render). Whitespace, keyword casing, and optional-token noise
collapse, so a noise-only re-run does not mint a spurious new recipe version;
identifiers (table and column names) are preserved, so a real join change still
registers as different.

sqlglot cannot canonicalize every dialect quirk; on a parse/tokenize failure the
helper falls back to the stripped raw string, so the check degrades to
byte-equality rather than raising. (Same duckdb-dialect idiom the retired MCP
``cte_parser`` used — reimplemented here, not imported.)
"""

from __future__ import annotations

import sqlglot
from sqlglot.errors import SqlglotError

_DIALECT = "duckdb"


def canonical_sql(sql: str) -> str:
    """Return ``sql`` in sqlglot's canonical duckdb rendering.

    Falls back to the stripped input when sqlglot cannot parse it, so callers
    can treat the result as a stable comparison key without guarding for
    malformed SQL.
    """
    try:
        parsed = sqlglot.parse_one(sql, dialect=_DIALECT)
    except SqlglotError:
        return sql.strip()
    if parsed is None:
        return sql.strip()
    return parsed.sql(dialect=_DIALECT)


def sql_equivalent(left: str, right: str) -> bool:
    """True if two SQL statements are equal modulo syntax noise.

    Compares the canonical form (:func:`canonical_sql`) of each side, so casing,
    whitespace, and formatting differences are ignored while identifier and
    structural changes are not.
    """
    return canonical_sql(left) == canonical_sql(right)
