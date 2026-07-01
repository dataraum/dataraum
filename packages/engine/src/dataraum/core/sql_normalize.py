"""Canonical SQL comparison for recipe-equality checks (DAT-415, DAT-654).

A view's ``CREATE VIEW`` DDL is re-derived on each begin_session run — the LLM
selects which dimension joins to include. To decide whether a re-run produced a
*genuinely* different view (rather than the same view with cosmetic syntax
differences), we compare the two DDLs by their **parsed structure** rather than
their text: DuckDB's ``json_serialize_sql`` renders the statement's parse tree as
JSON, we drop the per-node ``query_location`` byte offsets (the only thing that
varies under whitespace/formatting noise), and compare the normalized trees.
Whitespace, keyword casing, and optional-token noise collapse; identifiers
(table and column names) and clause **order** are preserved, so a real join
change — or a re-ordered SELECT list — still registers as different.

``json_serialize_sql`` serializes ``SELECT`` statements only, so the
machine-generated ``CREATE [OR REPLACE] VIEW … AS`` wrapper (added by
``analysis.views.builder``) is stripped to recover the inner SELECT before
serialization. On any parse/serialize failure the helper falls back to the
stripped raw string, so the check degrades to byte-equality rather than raising.

This retired sqlglot (DAT-654): one parser — DuckDB's own — now backs every SQL
canonicalization in the workspace (the cockpit's snippet reuse does the same via
``json_serialize_sql``), so engine and cockpit agree byte-for-byte on what "the
same SQL" means.
"""

from __future__ import annotations

import json
import re
import threading
from typing import TYPE_CHECKING, Any

import duckdb

if TYPE_CHECKING:
    from duckdb import DuckDBPyConnection

# The machine-generated view envelope (``analysis.views.builder``): strip a
# leading ``CREATE [OR REPLACE] [TEMP] VIEW <fqn> AS`` to recover the inner
# SELECT. Non-greedy up to the first standalone ``AS`` keyword — the view FQN is
# a quoted ``catalog.schema."name"`` with no spaced ``AS`` inside, and a bare
# SELECT (no wrapper) simply doesn't match, so it passes through untouched. The
# "no spaced ``AS``" guarantee rests on ``core.duckdb_naming.sanitize_identifier``
# collapsing every non-``[a-z0-9_]`` run (spaces included) to ``_``, so an
# embedded "as" is always ``_as_`` (no word boundary) — if that ever weakens, an
# early match just degrades to byte-equality, the safe direction for the gate.
_VIEW_WRAPPER = re.compile(
    r"^\s*CREATE\s+(?:OR\s+REPLACE\s+)?(?:TEMP(?:ORARY)?\s+)?VIEW\b.*?\bAS\b\s*",
    re.IGNORECASE | re.DOTALL,
)

# One in-memory DuckDB serves as the parser — no lake, no catalog, no
# persistence; ``json_serialize_sql`` only tokenizes + parses. Lazily created
# (import is cheap) and shared process-wide; each call takes its own cursor so
# the engine's concurrent activity threads never share parser state.
_parser_lock = threading.Lock()
_parser: DuckDBPyConnection | None = None


def _connection() -> DuckDBPyConnection:
    global _parser
    if _parser is None:
        with _parser_lock:
            if _parser is None:
                _parser = duckdb.connect(":memory:")
    return _parser


def serialize_sql(sql: str) -> dict[str, Any] | None:
    """Return DuckDB's ``json_serialize_sql`` parse tree for ``sql``, or ``None``.

    ``None`` on anything that is not a cleanly-serializable ``SELECT`` — a
    non-SELECT statement (DuckDB sets ``error``), a syntax error, or an
    infrastructure failure — so callers get a single "could not parse" signal.
    Shared by the view-equality gate here and the derived-formula parser
    (:mod:`dataraum.entropy.measurements.derived_value`).
    """
    cursor = None
    try:
        cursor = _connection().cursor()
        row = cursor.execute("SELECT json_serialize_sql(?::VARCHAR)", [sql]).fetchone()
    except Exception:
        return None
    finally:
        if cursor is not None:
            cursor.close()
    if not row or row[0] is None:
        return None
    try:
        tree = json.loads(row[0])
    except ValueError, TypeError:
        return None
    if not isinstance(tree, dict) or tree.get("error"):
        return None
    return tree


def _strip_query_location(value: Any) -> Any:
    """Recursively drop ``query_location`` byte offsets — the only formatting noise.

    Every parse-tree node carries a ``query_location`` (its byte offset in the
    source text), which differs for two whitespace-variant renderings of the
    same statement. Nothing else in the tree varies with formatting, so removing
    it makes the normalized tree a stable comparison key.
    """
    if isinstance(value, dict):
        return {k: _strip_query_location(v) for k, v in value.items() if k != "query_location"}
    if isinstance(value, list):
        return [_strip_query_location(v) for v in value]
    return value


def canonical_sql(sql: str) -> str:
    """Return a stable comparison key for ``sql`` (opaque; compare, don't render).

    Strips the ``CREATE … VIEW … AS`` wrapper, serializes the inner SELECT, and
    returns the normalized parse tree as canonical JSON. Falls back to the
    stripped input when the SQL cannot be serialized, so callers can treat the
    result as a stable key without guarding for malformed SQL. The return value
    is an internal key — never valid SQL; do not attempt to execute it.
    """
    inner = _VIEW_WRAPPER.sub("", sql, count=1)
    tree = serialize_sql(inner)
    if tree is None:
        return inner.strip()
    return json.dumps(_strip_query_location(tree), sort_keys=True, separators=(",", ":"))


def sql_equivalent(left: str, right: str) -> bool:
    """True if two SQL statements are equal modulo syntax noise.

    Compares the canonical form (:func:`canonical_sql`) of each side, so casing,
    whitespace, and formatting differences are ignored while identifier and
    structural changes — including a re-ordered SELECT list — are not.
    """
    return canonical_sql(left) == canonical_sql(right)
