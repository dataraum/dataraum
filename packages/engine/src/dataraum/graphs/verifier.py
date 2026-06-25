"""Post-execution SANITY floor for metric graphs (DAT-616).

This is a *cheap value-space sanity check*, NOT the grounding fix. It keeps a
metric ``grounded``/inconclusive (never silently ``executed``/green) when:
- an extract aggregated to NULL — its filter matched no rows ("no support");
- the composed value is NULL (a contributing extract had no support);
- a catalogue-declared per-extract ``validation:`` bound is violated (e.g. revenue
  ``value > 0``) — a one-number comparison on the step's executed scalar.

**It is structurally blind to the real bug** (DAT-616): a *wrong-but-non-empty*
filter (the agent improvises ``WHERE account_type ILIKE '%cost%'`` and matches the
wrong rows) returns a well-typed, in-range value that passes every check here. The
grounding fix lives elsewhere — feed the agent the real value distribution + a
teach-confirmed concept→value-set binding (DAT-620) so it stops improvising the
predicate (design: ``docs/dat543-construct-dont-improvise.md``). Keep this as the
cheap floor; do not mistake it for the fix.

The signal is **support, not magnitude**: a genuine ``0`` (a filter that matched
rows summing to zero) passes; only a NULL (nothing aggregated) fails.
"""

from __future__ import annotations

import ast
import operator
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from dataraum.core.models.base import Result
from dataraum.graphs.models import StepType

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
    #    here — no support, so the metric is inconclusive (not a real value). A
    #    non-extract step (formula/constant) that is NULL is degenerate, not
    #    "unfiltered" — word the reason for what the step actually is.
    for sr in execution.step_results:
        if sr.value is None:
            step = graph.steps.get(sr.step_id)
            if step is None or step.step_type == StepType.EXTRACT:
                return Result.fail(
                    f"extract '{sr.step_id}' has no support: its filter matched no rows "
                    f"(aggregated to NULL) — metric inconclusive, not a real value"
                )
            return Result.fail(
                f"step '{sr.step_id}' computed to NULL (degenerate) — metric inconclusive"
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
            try:
                holds = _condition_holds(check.condition, bound.value)
            except (ValueError, SyntaxError) as exc:
                # A malformed catalogue condition fails loud HERE as a clean
                # Result.fail (routed through the caller's snippet-failure path),
                # not escaping to the blanket worker handler. ValueError = parseable
                # but unsupported (e.g. a bad operator); SyntaxError = unparseable
                # (e.g. SQL `AND` where Python wants `and` or a chained comparison).
                return Result.fail(
                    f"catalogue validation condition for '{step_id}' is malformed "
                    f"({check.condition!r}): {exc}"
                )
            if not holds:
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
                raise ValueError(
                    f"unsupported comparison operator in condition: {type(op).__name__}"
                )
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
    if (
        isinstance(node, ast.Constant)
        and isinstance(node.value, (int, float))
        and not isinstance(node.value, bool)
    ):
        return node.value
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.USub):
        return -_term(node.operand, value)
    raise ValueError(f"unsupported term in condition (only `value` and numbers): {ast.dump(node)}")
