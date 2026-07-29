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
from collections.abc import Sequence
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


def quote_key(column: str) -> str:
    """One group key as a quoted identifier — the single rendering of a grain key."""
    escaped = column.replace('"', '""')
    return f'"{escaped}"'


def same_name_keys(*columns: str) -> tuple[tuple[str, str], ...]:
    """Grain keys for a SINGLE-relation grouping — each column keeps its own name.

    The degenerate case of :func:`compose_extract_sql`'s ``group_by``: when only
    one relation is being grouped there is no second spelling to reconcile, so
    the axis identity and the local column coincide.
    """
    return tuple((c, c) for c in columns)


def compose_extract_sql(
    select_expr: str,
    relation: str | None,
    where: list[str],
    group_by: Sequence[tuple[str, str]] = (),
) -> str:
    """Render an EXTRACT's clause parts to SQL (DAT-671, parts-at-source).

    The parts are the persisted artifact; this render is the ONE place they
    become a string on the engine side (the cockpit drill builder composes its
    own variants — sliced, pinned — from the same parts, never parsing SQL).
    A null relation is the fall-loud shape (``SELECT NULL AS value``, no FROM).

    ``group_by`` renders the extract at UNIT GRAIN (DAT-671 B1): one row per
    distinct key instead of one workspace scalar. It is a property of the
    COMPOSITION, not of the grounding — the same persisted parts render either
    way, which is why it is an argument here and not a stored part. The grain
    changes nothing else: same relation, same predicates (validity scope,
    declared restriction, and any bound reporting instant alike), so the rows it
    groups are exactly the rows the scalar aggregates and the parts therefore
    sum back to it whenever the served verdict says the measure is additive on
    that axis.

    Each key is ``(source_column, output_alias)``. They differ only for CROSS-FACT
    drill-across (DAT-809): two facts realize ONE conformed dimension with their
    own local columns (``account_id`` here, ``acct`` there), so each side groups
    by its own column and projects it under the shared axis identity, which is
    what :func:`compose_formula_sql` then merges on. Grouping stays on the SOURCE
    column — aliasing a projection never changes which rows collapse together —
    so the aliased render aggregates exactly the rows the same-named one would.
    Use :func:`same_name_keys` for the single-relation case, where they coincide.

    An entity with no row under those predicates is ABSENT from the result. That
    is the honest reading — the data records no level for it — and it must never
    be filled with a zero, which would assert a measurement nobody made.
    """
    keys = [(c, a) for c, a in group_by if c and c.strip() and a and a.strip()]
    projection = ", ".join(f"{quote_key(c)} AS {quote_key(a)}" for c, a in keys)
    sql = f"SELECT {projection + ', ' if projection else ''}{select_expr} AS value"
    if relation:
        sql += f"\nFROM {relation}"
    clause = compose_where_predicate(where)
    if clause:
        sql += f"\nWHERE {clause}"
    if keys:
        sql += "\nGROUP BY " + ", ".join(quote_key(c) for c, _ in keys)
    return sql


def extract_parts_dict(
    select_expr: str,
    relation: str | None,
    where: list[str],
    period_binding: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """The persisted clause-parts shape (DAT-671).

    This is the GENERAL schema every structured SQL author shares (the answer
    agent adopts it later), even though the graph agent only ever fills the
    single-relation single-item case:
    ``{select: [{expr, alias}], from: [relation], where: [pred, …]}``.

    ``period_binding`` (DAT-887) is the resolved reporting instant a POINT-IN-TIME
    extract was bound to — the ticket's required observable, so a consumer can tell
    "the balance at the fiscal-year close" from "the latest balance in the table"
    instead of having to infer it from the value. The key is present ONLY when an
    instant was actually bound: a flow never carries one, and an unresolved binding
    discloses itself as a typed assumption instead of a half-filled record here.
    """
    parts: dict[str, Any] = {
        "select": [{"expr": select_expr, "alias": "value"}],
        "from": [relation] if relation else [],
        "where": [p.strip() for p in where if p and p.strip()],
    }
    if period_binding is not None:
        parts["period_binding"] = period_binding
    return parts


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


def compose_formula_sql(
    expression: str,
    dep_step_ids: set[str],
    *,
    group_by: Sequence[str] = (),
    grouped_steps: frozenset[str] = frozenset(),
) -> str:
    """Compose a formula's final SQL from its dependency step CTEs.

    Args:
        expression: The metric's arithmetic expression over dependency step ids,
            e.g. ``"(accounts_receivable / revenue) * days_in_period"``.
        dep_step_ids: The formula step's declared dependencies — every identifier
            in the expression must be one of these (it names a step CTE that
            returns a scalar ``value``).
        group_by: The unit-grain keys (DAT-671 B1). Empty renders the workspace
            scalar exactly as before.
        grouped_steps: Which dependencies were themselves composed at unit grain
            (their CTE carries the keys plus ``value``). Dependencies NOT listed
            are entity-independent single-row CTEs — a CONSTANT is the case that
            actually occurs — and stay scalar subqueries, which is what makes
            ``ap / cogs * days_in_period`` compose per entity without the constant
            needing a key it has no meaning for.

    Returns:
        A single statement aliasing the arithmetic ``AS value`` and guarding every
        division denominator with ``NULLIF(<denom>, 0)``. At unit grain it also
        projects the keys and FULL OUTER JOINs the grouped dependencies on them.

    Why FULL OUTER: an entity present in one carrier and missing from another is
    the normal case (a vendor with a payable but no purchases in the window). An
    inner join would silently DROP that entity from the breakdown; the full join
    keeps it, and its missing operand stays NULL so the arithmetic yields NULL —
    "not computable for this entity" — instead of a fabricated zero. Reconciling
    to the workspace scalar is the served verdict's business, not the join's.

    Raises:
        ValueError: The expression is unparseable, references an operand that is
            not a declared dependency, or uses a construct outside the closed
            arithmetic grammar — surfaced born-loud rather than mis-composed.
    """
    try:
        tree = ast.parse(expression, mode="eval")
    except SyntaxError as exc:
        raise ValueError(f"unparseable formula expression {expression!r}: {exc}") from exc

    keys = [k for k in group_by if k and k.strip()]
    if not keys:
        rendered = _render(tree.body, dep_step_ids, expression, frozenset())
        return f"SELECT {rendered} AS value"

    joined = [s for s in sorted(dep_step_ids) if s in grouped_steps]
    if not joined:
        raise ValueError(
            f"formula {expression!r} was asked for unit grain on {keys} but none of its "
            f"dependencies ({sorted(dep_step_ids)}) were composed at that grain — a metric "
            "whose every carrier is entity-independent has no per-entity value"
        )

    rendered = _render(tree.body, dep_step_ids, expression, frozenset(joined))
    # Project each key by COALESCE over every joined side: under a FULL OUTER
    # join the key is NULL on whichever side lacks the entity, so reading it from
    # one fixed side would blank the very rows the full join exists to keep.
    projection = ", ".join(
        f"COALESCE({', '.join(f'{s}.{quote_key(k)}' for s in joined)}) AS {quote_key(k)}"
        if len(joined) > 1
        else f"{joined[0]}.{quote_key(k)} AS {quote_key(k)}"
        for k in keys
    )
    sql = f"SELECT {projection}, {rendered} AS value\nFROM {joined[0]}"
    for i, step in enumerate(joined[1:], start=1):
        prior = joined[:i]
        on = " AND ".join(
            f"COALESCE({', '.join(f'{p}.{quote_key(k)}' for p in prior)}) = {step}.{quote_key(k)}"
            if len(prior) > 1
            else f"{prior[0]}.{quote_key(k)} = {step}.{quote_key(k)}"
            for k in keys
        )
        sql += f"\nFULL OUTER JOIN {step} ON {on}"
    return sql


def _render(
    node: ast.expr, dep_step_ids: set[str], expression: str, grouped: frozenset[str]
) -> str:
    """Render one expression node to SQL, recursively."""
    if isinstance(node, ast.Name):
        if node.id not in dep_step_ids:
            raise ValueError(
                f"formula {expression!r} references '{node.id}', which is not a declared "
                f"dependency ({sorted(dep_step_ids)}) — refusing to compose a fabricated operand"
            )
        # A grouped dependency is already joined on the keys, so its value is a
        # COLUMN of this row; an entity-independent one stays a scalar subquery.
        return f"{node.id}.value" if node.id in grouped else f"(SELECT value FROM {node.id})"

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
        left = _render(node.left, dep_step_ids, expression, grouped)
        right = _render(node.right, dep_step_ids, expression, grouped)
        # A zero divisor must propagate as NULL, not raise — NULLIF the denominator.
        if isinstance(node.op, ast.Div):
            right = f"NULLIF({right}, 0)"
        return f"({left} {op} {right})"

    if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.USub):
        return f"-{_render(node.operand, dep_step_ids, expression, grouped)}"

    raise ValueError(
        f"formula {expression!r} contains an unsupported expression node: {type(node).__name__}"
    )
