"""Save-time enforcement of the grounding provenance contract v2 (DAT-727).

The operating-model graph's ``uses`` edge un-nests
``provenance.column_mappings_basis`` — so that enumeration must be TRUE at
authoring time, not best-effort. Governing rule: information the graph needs
exists as strictly typed, enforced data in the model output; parsing rendered
SQL as a *source* of typed data is forbidden. This module is the enforcement:

1. **Membership** — every enumerated column is a member of the served
   relation's schema (typed set-membership; the relation itself must be a
   served relation).
2. **Completeness, PER ROLE** — every column the ``select_expr`` reads appears
   under ``measure_columns``, and every column the ``where`` predicates filter
   on appears under ``filter_columns`` (a dual-role column must appear under
   both). Role-checked because ``role`` is a real ``og_uses`` edge property —
   a mislabeled enumeration would mislabel the graph. The reference sets are
   derived from the parts as a *validator only*: DuckDB's catalog-free parse
   (``json_serialize_sql``, the DAT-713 seam) recovers the true column
   references — distinguishing identifiers from string literals and skipping
   subquery-internal names — and falls back to coarse lexical tokens matched
   against the relation's known column vocabulary when a fragment does not
   parse (or no connection is available). Neither path ever *writes* a column
   name anywhere; a violation is fed back to the model for a repair turn
   (DAT-710 pattern) and the model fixes its own enumeration.
3. **No phantoms, PER ROLE** — an enumerated column its role's fragments never
   touch is a violation too, but only when every fragment parsed (the lexical
   fallback over-collects — a string literal containing a column name would
   make an honest enumeration look phantom, so the check stays parse-gated).

The fall-loud grounding shape (``relation: ""`` / ``select_expr: "NULL"``)
carries no columns and is exempt.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

from dataraum.core.logging import get_logger

if TYPE_CHECKING:
    import duckdb

    from dataraum.graphs.models import ExtractGroundingOutput

logger = get_logger(__name__)

# Bare or double-quoted identifier-shaped tokens; the vocabulary intersection
# discards keywords, literals' words, and other relations' column names.
_TOKEN_RE = re.compile(r'"([^"]+)"|[A-Za-z_][A-Za-z0-9_]*')


def validate_grounding_basis(
    output: ExtractGroundingOutput,
    schema_tables: dict[str, set[str]],
    duckdb_conn: duckdb.DuckDBPyConnection | None,
) -> list[str]:
    """Contract-v2 violations of one grounding output, empty when clean.

    Args:
        output: The schema-validated grounding output.
        schema_tables: The SERVED relations — ``{relation_name: {column, …}}``
            exactly as the prompt's schema block described them.
        duckdb_conn: Connection for the catalog-free parse (validator only);
            ``None`` degrades to the lexical fallback for every fragment.

    Returns:
        Human-readable violation lines for the repair turn; ``[]`` when the
        output honors the contract (including the exempt fall-loud shape).
    """
    if not output.relation:
        return []  # fall-loud: no relation, no columns, nothing to enforce

    relation_columns = schema_tables.get(output.relation)
    if relation_columns is None:
        return [
            f"relation '{output.relation}' is not among the served relations "
            f"({sorted(schema_tables)}) — use a served relation name verbatim"
        ]

    measure_enumerated: set[str] = set()
    filter_enumerated: set[str] = set()
    violations: list[str] = []
    for entry in output.provenance.column_mappings_basis:
        measure_enumerated.update(entry.basis.measure_columns)
        filter_enumerated.update(entry.basis.filter_columns)
        for col in [*entry.basis.measure_columns, *entry.basis.filter_columns]:
            if col not in relation_columns:
                violations.append(
                    f"column_mappings_basis['{entry.concept}'] names '{col}', which is not a "
                    f"column of '{output.relation}' — enumerate served column names "
                    "verbatim, without table qualifiers"
                )

    select_used, where_used, parsed = _used_columns(output, relation_columns, duckdb_conn)
    for col in sorted(select_used - measure_enumerated):
        violations.append(
            f"select_expr reads '{output.relation}' column '{col}' but "
            "column_mappings_basis does not enumerate it under measure_columns — "
            "every column the select_expr reads must appear there, under its concept"
        )
    for col in sorted(where_used - filter_enumerated):
        violations.append(
            f"the where predicates filter on '{output.relation}' column '{col}' but "
            "column_mappings_basis does not enumerate it under filter_columns — "
            "every column the where predicates touch must appear there, under its concept"
        )
    if parsed:
        # Phantom check only among membership-valid names (invalid ones are
        # already flagged above) and only under a successful parse — the
        # lexical fallback over-collects, which would misread honest entries.
        for col in sorted((measure_enumerated & relation_columns) - select_used):
            violations.append(
                f"column_mappings_basis enumerates '{col}' under measure_columns but "
                "select_expr never reads it — enumerate exactly the columns each "
                "role's SQL touches"
            )
        for col in sorted((filter_enumerated & relation_columns) - where_used):
            violations.append(
                f"column_mappings_basis enumerates '{col}' under filter_columns but "
                "the where predicates never touch it — enumerate exactly the columns "
                "each role's SQL touches"
            )
    return violations


def where_filter_columns(
    output: ExtractGroundingOutput,
    relation_columns: set[str],
    duckdb_conn: duckdb.DuckDBPyConnection | None,
) -> set[str]:
    """The relation columns the grounding's WHERE predicates already constrain.

    Reuses the same catalog-free parse as contract validation, so a status column
    the LLM deliberately filtered on is detected precisely — never by a lexical
    coincidence inside a string literal. DAT-733's default-scope opt-out reads this
    to defer the canonical validity scope to the grounding's own constraint.
    """
    _, where_used, _ = _used_columns(output, relation_columns, duckdb_conn)
    return where_used


def _used_columns(
    output: ExtractGroundingOutput,
    relation_columns: set[str],
    duckdb_conn: duckdb.DuckDBPyConnection | None,
) -> tuple[set[str], set[str], bool]:
    """The relation columns each ROLE's fragments reference, and whether all parsed.

    Returns ``(select_used, where_used, all_parsed)`` — the ``select_expr``'s
    references and the ``where`` predicates' references separately, because the
    contract (and the ``og_uses`` edge's ``role`` property) is per-role. Per
    fragment: DuckDB's catalog-free parse when it succeeds (identifier-precise),
    lexical tokens otherwise. Both are intersected with the relation's known
    vocabulary, so a subquery's *other-relation* names and SQL keywords can
    never enter; the parse additionally excludes string literals and
    subquery-internal names that happen to collide with relation columns.
    """

    all_parsed = True

    def refs_of(fragment: str) -> set[str]:
        nonlocal all_parsed
        try:
            if duckdb_conn is None:
                raise ValueError("no DuckDB connection for the parse validator")
            refs = _parsed_column_refs(fragment, duckdb_conn)
        except ValueError:
            all_parsed = False
            refs = {m.group(1) or m.group(0) for m in _TOKEN_RE.finditer(fragment)}
        return refs & relation_columns

    select_used: set[str] = set()
    if output.select_expr and output.select_expr.strip():
        select_used = refs_of(output.select_expr)
    where_used: set[str] = set()
    for predicate in output.where:
        if predicate and predicate.strip():
            where_used |= refs_of(predicate)
    return select_used, where_used, all_parsed


def _parsed_column_refs(sql_expr: str, con: duckdb.DuckDBPyConnection) -> set[str]:
    """``COLUMN_REF`` base names in one expression, via DuckDB's JSON AST.

    Parses ``SELECT <expr>`` catalog-free (``json_serialize_sql``) — works for
    a select_expr and for a standalone predicate alike. The last name of a
    ``COLUMN_REF`` strips any ``table.`` qualifier. A ``SUBQUERY`` node's inner
    query is deliberately NOT walked (``x IN (SELECT id FROM ref_table)``
    references ``id`` over *ref_table*); only its outer operand (``child``)
    counts.

    Raises:
        ValueError: the fragment does not parse (caller falls back to lexical).
    """
    import json as _json

    import duckdb as _duckdb

    try:
        raw = con.execute("SELECT json_serialize_sql(?)", [f"SELECT {sql_expr}"]).fetchone()
    except _duckdb.Error as exc:
        raise ValueError(f"unparseable fragment {sql_expr!r}: {exc}") from exc
    if raw is None:  # pragma: no cover - json_serialize_sql always returns a row
        raise ValueError(f"fragment {sql_expr!r} did not serialize")
    doc: dict[str, Any] = _json.loads(raw[0])
    if doc.get("error"):
        raise ValueError(f"unparseable fragment {sql_expr!r}: {doc.get('error_message')}")

    cols: set[str] = set()

    def rec(node: Any) -> None:
        if isinstance(node, dict):
            if node.get("class") == "COLUMN_REF":
                names = node.get("column_names") or []
                if names:
                    cols.add(names[-1])
                return
            if node.get("class") == "SUBQUERY":
                rec(node.get("child"))
                return
            for value in node.values():
                rec(value)
        elif isinstance(node, list):
            for item in node:
                rec(item)

    rec(doc["statements"][0]["node"]["select_list"])
    return cols


def schema_tables_from_info(schema_info: dict[str, Any]) -> dict[str, set[str]]:
    """``{relation: {column, …}}`` from the agent's served schema block.

    The SAME artifact the prompt's ``table_schema`` slot renders
    (``GraphAgent._build_schema_info``) — validation and prompt read one
    source, so "served" means the same thing in both places.
    """
    return {
        t["table_name"]: {c["name"] for c in t.get("columns", [])}
        for t in schema_info.get("tables", [])
        if t.get("table_name")
    }
