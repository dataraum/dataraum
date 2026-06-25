"""Deterministic composition of a FORMULA node's SQL (DAT-636).

A metric formula is pure arithmetic over already-decided building blocks — e.g.
``(accounts_receivable / revenue) * days_in_period`` or
``revenue - cost_of_goods_sold``. Each operand names a dependency *step*, whose
SQL is folded in upstream as a CTE returning a single column ``value``. Composing
the final SQL is therefore mechanical: substitute each operand with
``(SELECT value FROM <step_id>)``, guard division denominators with ``NULLIF`` so
a zero divisor yields NULL (never a runtime error), and alias the result ``value``.

This is the deterministic counterpart to the LLM ``graph_formula_composition``
prompt. It cannot fabricate a missing dependency (an unknown operand fails loud),
cannot leak prompt placeholders, and is byte-for-byte reproducible — so it
dissolves the fragilities of the LLM formula path (round-trip, fabrication,
cross-run drift) by construction. The expression grammar is closed: identifiers
(dependency step_ids), numeric literals, ``+ - * /``, unary minus, and
parentheses. Anything else (a call, an attribute, an unknown name) is a malformed
catalogue formula and is raised, not guessed.
"""

from __future__ import annotations

import ast

_BINOP_SQL: dict[type[ast.operator], str] = {
    ast.Add: "+",
    ast.Sub: "-",
    ast.Mult: "*",
    ast.Div: "/",
}


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
        return repr(node.value)

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
