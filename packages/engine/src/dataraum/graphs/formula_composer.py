"""Deterministic composition of a FORMULA node's SQL (DAT-636).

A metric formula is pure arithmetic over already-decided building blocks — e.g.
``(accounts_receivable / revenue) * days_in_period`` or
``revenue - cost_of_goods_sold``. Each operand names a dependency *step*, whose
SQL is folded in upstream as a CTE returning a single column ``value``. Composing
the final SQL is therefore mechanical: substitute each operand with
``(SELECT value FROM <step_id>)``, guard division denominators with ``NULLIF`` so
a zero divisor yields NULL (never a runtime error), and alias the result ``value``.

This is the SOLE formula/constant authoring path (DAT-643 retired the LLM
``graph_formula_composition`` prompt and its comparison shadow). It cannot fabricate a
missing dependency (an unknown operand fails loud), cannot leak prompt placeholders, and
is byte-for-byte reproducible — so it dissolves the fragilities of the old LLM formula
path (round-trip, fabrication, cross-run drift) by construction. The expression grammar
is closed: identifiers
(dependency step_ids), numeric literals, ``+ - * /``, unary minus, and
parentheses. Anything else (a call, an attribute, an unknown name) is a malformed
catalogue formula and is raised, not guessed.
"""

from __future__ import annotations

import ast
from typing import Any

_BINOP_SQL: dict[type[ast.operator], str] = {
    ast.Add: "+",
    ast.Sub: "-",
    ast.Mult: "*",
    ast.Div: "/",
}


def compose_where_predicate(where: list[str]) -> str | None:
    """AND-compose an EXTRACT's persisted WHERE predicates into one clause body.

    Returns the predicate string to place after ``WHERE`` (never the keyword
    itself), or ``None`` when there is nothing to filter on. Multiple predicates
    AND-compose, each parenthesized so an OR inside one leaf can never bleed
    across leaves.

    This is the SINGLE source of the filter the executed flow SUM applies
    (:func:`compose_extract_sql` renders it verbatim). The period resolver
    (DAT-785) filters the same relation with the SAME clause so its window query
    provably scans the exact rows the SUM aggregates — never the whole column.
    """
    preds = [p.strip() for p in where if p and p.strip()]
    if not preds:
        return None
    return preds[0] if len(preds) == 1 else " AND ".join(f"({p})" for p in preds)


def compose_extract_sql(select_expr: str, relation: str | None, where: list[str]) -> str:
    """Render an EXTRACT's clause parts to its scalar SQL (DAT-671, parts-at-source).

    The parts are the persisted artifact; this render is the ONE place they
    become a string on the engine side (the cockpit drill builder composes its
    own variants — sliced, pinned — from the same parts, never parsing SQL).
    A null relation is the fall-loud shape (``SELECT NULL AS value``, no FROM).
    """
    sql = f"SELECT {select_expr} AS value"
    if relation:
        sql += f"\nFROM {relation}"
    clause = compose_where_predicate(where)
    if clause:
        sql += f"\nWHERE {clause}"
    return sql


def extract_parts_dict(select_expr: str, relation: str | None, where: list[str]) -> dict[str, Any]:
    """The persisted clause-parts shape (DAT-671).

    This is the GENERAL schema every structured SQL author shares (the answer
    agent adopts it later), even though the graph agent only ever fills the
    single-relation single-item case:
    ``{select: [{expr, alias}], from: [relation], where: [pred, …]}``.
    """
    return {
        "select": [{"expr": select_expr, "alias": "value"}],
        "from": [relation] if relation else [],
        "where": [p.strip() for p in where if p and p.strip()],
    }


def compose_constant_sql(value: Any) -> str:
    """SQL for a CONSTANT node — emit the resolved parameter value as a scalar.

    A constant carries no judgment: its value is already resolved deterministically
    from the graph's parameter defaults, so the LLM adds nothing. An integer value
    stays integer (``days_in_period=30`` → ``SELECT 30 AS value``, matching the
    snippet the LLM path used) — a constant is never a division denominator, so
    integer typing is safe.

    Raises:
        ValueError: The value is not numeric (metric constants are numeric periods).
    """
    try:
        numeric = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"constant value {value!r} is not numeric") from exc
    literal = repr(int(numeric)) if numeric.is_integer() else repr(numeric)
    return f"SELECT {literal} AS value"


def compose_formula_sql(expression: str, dep_step_ids: set[str]) -> str:
    """Compose a formula's final SQL from its dependency step CTEs.

    Args:
        expression: The metric's arithmetic expression over dependency step ids,
            e.g. ``"(accounts_receivable / revenue) * days_in_period"``.
        dep_step_ids: The formula step's declared dependencies — every identifier
            in the expression must be one of these (it names a step CTE that
            returns a scalar ``value``).

    Returns:
        A single ``SELECT <expr> AS value`` statement that references each
        dependency as ``(SELECT value FROM <step_id>)`` and guards every division
        denominator with ``NULLIF(<denom>, 0)``.

    Raises:
        ValueError: The expression is unparseable, references an operand that is
            not a declared dependency, or uses a construct outside the closed
            arithmetic grammar — surfaced born-loud rather than mis-composed.
    """
    try:
        tree = ast.parse(expression, mode="eval")
    except SyntaxError as exc:
        raise ValueError(f"unparseable formula expression {expression!r}: {exc}") from exc
    rendered = _render(tree.body, dep_step_ids, expression)
    return f"SELECT {rendered} AS value"


def _render(node: ast.expr, dep_step_ids: set[str], expression: str) -> str:
    """Render one expression node to SQL, recursively."""
    if isinstance(node, ast.Name):
        if node.id not in dep_step_ids:
            raise ValueError(
                f"formula {expression!r} references '{node.id}', which is not a declared "
                f"dependency ({sorted(dep_step_ids)}) — refusing to compose a fabricated operand"
            )
        return f"(SELECT value FROM {node.id})"

    if isinstance(node, ast.Constant) and isinstance(node.value, (int, float)):
        if isinstance(node.value, bool):  # bool is an int subclass — reject explicitly
            raise ValueError(f"formula {expression!r} uses a boolean literal")
        # Emit as a float literal (e.g. 100 → 100.0) so a literal can never make a
        # surrounding division integer-typed (and silently truncate) in DuckDB.
        return repr(float(node.value))

    if isinstance(node, ast.BinOp):
        op = _BINOP_SQL.get(type(node.op))
        if op is None:
            raise ValueError(
                f"formula {expression!r} uses unsupported operator {type(node.op).__name__}"
            )
        left = _render(node.left, dep_step_ids, expression)
        right = _render(node.right, dep_step_ids, expression)
        # A zero divisor must propagate as NULL, not raise — NULLIF the denominator.
        if isinstance(node.op, ast.Div):
            right = f"NULLIF({right}, 0)"
        return f"({left} {op} {right})"

    if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.USub):
        return f"-{_render(node.operand, dep_step_ids, expression)}"

    raise ValueError(
        f"formula {expression!r} contains an unsupported expression node: {type(node).__name__}"
    )
