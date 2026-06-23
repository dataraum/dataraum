"""Post-execution verifier for metric graphs (DAT-616).

Execution-pass is *not* validation. A metric whose SQL ran cleanly can still be
silently wrong: when an extract's filter matched no rows the aggregate is NULL
("no support"), and when the prompt masks that NULL with ``COALESCE(col, 0)`` it
reads as a real ``0`` — so a long-format finance metric (no ``revenue`` column,
"revenue" is a row filter the agent improvises) reaches ``executed``/green with a
fabricated value (the canonical case: ``gross_margin = 100%`` because cogs
matched nothing).

This verifier converts that into an honest *inconclusive*: a metric with an
unsupported extract, a degenerate (NULL) composed value, or a violated
catalogue-declared condition stays ``grounded`` with a stated reason — it is
never reported as ``executed``. It mirrors the ``ValidationAgent``'s
``row_count == 0 -> ERROR`` gate (``analysis/validation/agent.py``), the part the
metric path lacked, and is the seed of the DAT-619 snippet-library verifier
(nothing enters the reuse cache until it passed here — the caller gates
``_save_snippets`` on this verdict).

The signal is **support, not magnitude**: a genuine ``0`` (a filter that matched
rows summing to zero) passes; only a NULL (nothing aggregated) fails.
"""

from __future__ import annotations

import ast
import operator
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from dataraum.core.models.base import Result

if TYPE_CHECKING:
    from dataraum.graphs.models import GraphExecution, TransformationGraph

_COMPARATORS: dict[type[ast.cmpop], Callable[[Any, Any], bool]] = {
    ast.Gt: operator.gt,
    ast.GtE: operator.ge,
    ast.Lt: operator.lt,
    ast.LtE: operator.le,
    ast.Eq: operator.eq,
    ast.NotEq: operator.ne,
}


def verify_execution(graph: TransformationGraph, execution: GraphExecution) -> Result[None]:
    """Judge a clean execution for support, non-degeneracy, and declared conditions.

    Args:
        graph: The metric graph (carries each step's declared ``validations``).
        execution: The completed execution (per-step values + composed value).

    Returns:
        ``Result.ok(None)`` if the metric is trustworthy; ``Result.fail(reason)``
        with a human-readable reason if it is inconclusive or violates a declared
        condition — the caller keeps the artifact ``grounded`` with that reason
        and does not cache the SQL.
    """
    by_step = {sr.step_id: sr for sr in execution.step_results}

    # 1. Support: an extract whose aggregate is NULL matched no rows. With the
    #    COALESCE mask removed from the prompt, an empty filter surfaces as NULL
    #    here — no support, so the metric is inconclusive (not a real value).
    for sr in execution.step_results:
        if sr.value is None:
            return Result.fail(
                f"extract '{sr.step_id}' has no support: its filter matched no rows "
                f"(aggregated to NULL) — metric inconclusive, not a real value"
            )

    # 2. Non-degeneracy of the composed value. A NULL output means a contributing
    #    extract had no support; a genuine 0 is a real answer and passes.
    if execution.output_value is None:
        return Result.fail(
            "composed metric value is NULL — a contributing extract had no support; inconclusive"
        )

    # 3. Enforce the catalogue's declared per-extract conditions (e.g. revenue
    #    `value > 0`, cogs `value >= 0`), bound to the executed step by step_id
    #    (the dependency key). An unbindable condition is skipped — the support
    #    gate above already guards the real risk; DAT-619 hardens the binding.
    for step_id, step in graph.steps.items():
        bound = by_step.get(step_id)
        if bound is None or bound.value is None:
            continue
        for check in step.validations:
            if not _condition_holds(check.condition, bound.value):
                reason = check.message or check.condition
                return Result.fail(
                    f"declared validation failed for '{step_id}': {reason} (value={bound.value})"
                )

    return Result.ok(None)


def _condition_holds(condition: str, value: Any) -> bool:
    """Evaluate a declared extract condition (e.g. ``value > 0``) against a value.

    Only a comparison over the name ``value`` and numeric literals is permitted,
    parsed via :mod:`ast` — never ``eval``. A malformed or over-broad condition
    raises, so a bad catalogue condition fails loud rather than silently passing.
    """
    node = ast.parse(condition, mode="eval").body
    return _truth(node, value)


def _truth(node: ast.expr, value: Any) -> bool:
    if isinstance(node, ast.Compare):
        left = _term(node.left, value)
        for op, comparator in zip(node.ops, node.comparators, strict=True):
            comparator_fn = _COMPARATORS.get(type(op))
            if comparator_fn is None:
                raise ValueError(f"unsupported comparison operator in condition: {type(op).__name__}")
            right = _term(comparator, value)
            if not comparator_fn(left, right):
                return False
            left = right  # chained comparison: 0 < value < 100
        return True
    if isinstance(node, ast.BoolOp):
        results = [_truth(v, value) for v in node.values]
        return all(results) if isinstance(node.op, ast.And) else any(results)
    raise ValueError(f"unsupported condition expression: {ast.dump(node)}")


def _term(node: ast.expr, value: Any) -> Any:
    if isinstance(node, ast.Name) and node.id == "value":
        return value
    if isinstance(node, ast.Constant) and isinstance(node.value, (int, float)) and not isinstance(
        node.value, bool
    ):
        return node.value
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.USub):
        return -_term(node.operand, value)
    raise ValueError(f"unsupported term in condition (only `value` and numbers): {ast.dump(node)}")
