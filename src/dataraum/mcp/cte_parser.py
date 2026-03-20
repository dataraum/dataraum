"""CTE auto-decomposition via sqlglot.

Parses SQL containing CTEs and decomposes each CTE into a separate step,
enabling per-CTE snippet caching in the run_sql MCP tool.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Any

import sqlglot
from sqlglot import exp

_log = logging.getLogger(__name__)

# CTE aliases must be safe identifiers for CREATE TEMP VIEW in execute_sql_steps.
_SAFE_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass
class CteDecomposition:
    """Result of decomposing a CTE query into individual steps."""

    steps: list[dict[str, Any]]
    final_sql: str


def decompose_ctes(
    sql: str, column_mappings: dict[str, str] | None = None
) -> CteDecomposition | None:
    """Decompose a CTE query into individual steps.

    Args:
        sql: SQL string potentially containing CTEs.
        column_mappings: Optional mapping of output column names to source columns.
            Distributed to CTEs based on which columns each CTE references.
            Supports both plain column names and qualified 'table.column' format.

    Returns:
        CteDecomposition with per-CTE steps and a final SELECT, or None if
        the SQL has no CTEs, uses recursive CTEs, or fails to parse.
    """
    try:
        tree = sqlglot.parse_one(sql, dialect="duckdb")
    except sqlglot.errors.ParseError:
        _log.debug("sqlglot parse failed, falling back to monolithic mode", exc_info=True)
        return None

    with_node = tree.find(exp.With)
    if with_node is None:
        return None

    # Recursive CTEs are too complex to decompose safely
    if with_node.args.get("recursive"):
        return None

    if not hasattr(tree, "ctes"):
        return None
    ctes = list(tree.ctes)  # type: ignore[attr-defined]
    if not ctes:
        return None

    # Validate all CTE aliases are safe identifiers for CREATE TEMP VIEW
    for cte in ctes:
        if not _SAFE_IDENTIFIER.match(cte.alias):
            _log.debug("CTE alias %r is not a safe identifier, falling back", cte.alias)
            return None

    steps: list[dict[str, Any]] = []
    for cte in ctes:
        step_id = cte.alias
        cte_sql = cte.this.sql(dialect="duckdb")

        step: dict[str, Any] = {
            "step_id": step_id,
            "sql": cte_sql,
            "description": f"CTE: {step_id}",
            "_snippet_key": step_id,
        }

        # Distribute column_mappings to this CTE based on referenced columns.
        # Strips table qualifier ('orders.amount' -> 'amount') for matching.
        # Note: aliases (e.g. COUNT(*) AS n) are not Column nodes and won't match.
        if column_mappings:
            cte_columns = {col.name for col in cte.this.find_all(exp.Column)}
            step_mappings = {
                out_col: src_col
                for out_col, src_col in column_mappings.items()
                if src_col.split(".", 1)[-1] in cte_columns
            }
            if step_mappings:
                step["column_mappings"] = step_mappings

        steps.append(step)

    # Strip the WITH clause to get the final SELECT
    with_node.pop()

    # Only decompose SELECT statements — INSERT/UPDATE/DELETE with CTEs
    # would fail in execute_sql_steps which expects tabular results.
    if not isinstance(tree, exp.Select):
        _log.debug("CTE wraps a non-SELECT statement, falling back to monolithic")
        return None

    final_sql = tree.sql(dialect="duckdb")

    return CteDecomposition(steps=steps, final_sql=final_sql)
