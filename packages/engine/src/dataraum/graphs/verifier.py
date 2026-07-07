"""Post-execution SANITY floor for metric graphs (DAT-616).

This is a *cheap value-space sanity check*, NOT the grounding fix. It keeps a
metric ``grounded``/inconclusive (never silently ``executed``/green) when there
is NO VALUE to stand behind:
- an extract aggregated to NULL ("no support") — reported as the MEASUREMENT,
  never a cause: a NULL aggregate can mean the filter matched no rows OR that
  an aggregated operand was entirely NULL over the matched rows (a one-sided
  two-operand extract). The old text asserted the zero-row cause as fact and
  misdirected diagnosis of exactly the second case — DAT-699;
- the composed value is NULL (a contributing extract had no support).

A catalogue-declared ``validation:`` bound (e.g. ``0 <= value <= 365``) is an
EXPECTATION, not a gate (DAT-699): a violation FLAGS the executed metric
(execute-and-flag, the DAT-631 amber pattern) — it never refuses the number.
A declared "shouldn't" stated as "can't" kept blocking real values (negative
COGS is unusual, not impossible), and refusing the number hides exactly the
signal the user needs; the measured value disagreeing with the declared
expectation IS the signal (ADR-0009).

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


def verify_execution(graph: TransformationGraph, execution: GraphExecution) -> Result[list[str]]:
    """Judge a clean execution for support and non-degeneracy; flag declared expectations.

    Args:
        graph: The metric graph (carries each step's declared ``validations``).
        execution: The completed execution (per-step values + composed value).

    Returns:
        ``Result.ok(flags)`` — the metric EXECUTES; ``flags`` is the (possibly
        empty) list of declared-expectation violations to surface on the
        artifact as visible state_reason flags. ``Result.fail(reason)`` is
        reserved for NO-VALUE outcomes (no support / degenerate NULL) — there
        is nothing to execute-and-flag, the caller keeps the artifact
        ``grounded`` with the reason and does not cache the SQL.
    """
    by_step = {sr.step_id: sr for sr in execution.step_results}

    # 1. Support: an extract whose aggregate is NULL has no measured support.
    #    With the COALESCE mask removed from the prompt, that surfaces as NULL
    #    here — inconclusive (not a real value). Report ONLY the measurement:
    #    this check cannot see whether the filter matched zero rows or whether
    #    an aggregated operand was all-NULL over matched rows, and asserting
    #    either would fabricate a cause (DAT-699). The reason enumerates the
    #    possibility space so the re-author loop (retained failed snippet →
    #    prior context) can resolve it instead of trusting a wrong diagnosis.
    #    A non-extract step (formula/constant) that is NULL is degenerate —
    #    word the reason for what the step actually is.
    for sr in execution.step_results:
        if sr.value is None:
            step = graph.steps.get(sr.step_id)
            if step is None or step.step_type == StepType.EXTRACT:
                return Result.fail(
                    f"extract '{sr.step_id}' has no support: it aggregated to NULL — "
                    "either its filter matched no rows, or an aggregated operand is "
                    "entirely NULL over the rows it did match; metric inconclusive, "
                    "not a real value"
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

    # 3. The catalogue's declared conditions (e.g. `0 <= value <= 365`), bound
    #    to the executed step by step_id (the dependency key). Violations FLAG,
    #    never gate (DAT-699) — the number executed; the flag says the declared
    #    expectation disagrees with it, and severity rides along as the flag's
    #    weight. An unbindable condition is skipped — the support gate above
    #    already guards the real risk; DAT-619 hardens the binding. A malformed
    #    condition is a config bug: flagged on the artifact (visible where the
    #    cockpit shows it) and never a reason to refuse a good number.
    flags: list[str] = []
    for step_id, step in graph.steps.items():
        bound = by_step.get(step_id)
        if bound is None or bound.value is None:
            continue
        for check in step.validations:
            try:
                holds = _condition_holds(check.condition, bound.value)
            except (ValueError, SyntaxError) as exc:
                # ValueError = parseable but unsupported (e.g. a bad operator);
                # SyntaxError = unparseable (e.g. SQL `AND` where Python wants
                # `and`). Neither escapes to the blanket worker handler.
                flags.append(
                    f"declared expectation for '{step_id}' is malformed "
                    f"({check.condition!r}): {exc}"
                )
                continue
            if not holds:
                reason = check.message or check.condition
                flags.append(
                    f"declared expectation not met for '{step_id}': {reason} "
                    f"(value={bound.value}, severity={check.severity})"
                )

    return Result.ok(flags)


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
