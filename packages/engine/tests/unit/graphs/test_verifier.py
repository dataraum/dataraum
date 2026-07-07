"""Unit tests for the metric post-execution verifier (DAT-616).

The verifier is the honest-fail gate the metric path lacked: execution-pass is
not validation. It converts a silently-wrong metric (an extract whose filter
matched no rows, masked into a fake 0) into an inconclusive — grounded with a
reason — and enforces the catalogue's per-extract conditions. The signal is
*support*, not magnitude: a genuine 0 passes; only a NULL fails.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from dataraum.graphs.models import (
    GraphExecution,
    GraphMetadata,
    GraphSource,
    GraphStep,
    OutputDef,
    OutputType,
    StepResult,
    StepType,
    StepValidation,
    TransformationGraph,
)
from dataraum.graphs.verifier import _condition_holds, verify_execution


def _graph(steps: dict[str, GraphStep]) -> TransformationGraph:
    return TransformationGraph(
        graph_id="gross_margin",
        version="1.0",
        metadata=GraphMetadata(
            name="Gross Margin", description="", category="profitability", source=GraphSource.SYSTEM
        ),
        output=OutputDef(output_type=OutputType.SCALAR),
        steps=steps,
    )


def _extract(step_id: str, *, validations: list[StepValidation] | None = None) -> GraphStep:
    return GraphStep(
        step_id=step_id,
        step_type=StepType.EXTRACT,
        aggregation="sum",
        validations=validations or [],
    )


def _step_result(step_id: str, value: float | None) -> StepResult:
    sr = StepResult(step_id=step_id)
    if value is not None:
        sr.value_scalar = float(value)
    return sr


def _execution(step_values: dict[str, float | None], output_value: object) -> GraphExecution:
    ex = GraphExecution(execution_id="e1", graph_id="gross_margin", source=GraphSource.SYSTEM)
    ex.step_results = [_step_result(sid, v) for sid, v in step_values.items()]
    ex.output_value = output_value
    return ex


class TestSupportGate:
    def test_null_extract_is_inconclusive(self) -> None:
        """An extract that aggregated to NULL fails loud."""
        graph = _graph({"revenue": _extract("revenue"), "cogs": _extract("cogs")})
        execution = _execution({"revenue": 1000.0, "cogs": None}, output_value=100.0)

        result = verify_execution(graph, execution)

        assert not result.success
        assert "cogs" in result.error
        assert "no support" in result.error

    def test_null_extract_reason_reports_measurement_never_a_cause(self) -> None:
        """The reason states what was MEASURED (aggregated to NULL) and the
        possibility space — it must never assert 'filter matched no rows' as
        fact (DAT-699: that fabricated diagnosis misclassified a one-sided
        ledger whose join matched 167k rows, and the re-author loop feeds this
        exact text back to the agent)."""
        graph = _graph({"revenue": _extract("revenue"), "cogs": _extract("cogs")})
        execution = _execution({"revenue": 1000.0, "cogs": None}, output_value=100.0)

        result = verify_execution(graph, execution)

        assert not result.success
        assert "aggregated to NULL" in result.error
        # Both possibilities enumerated, neither asserted:
        assert "either its filter matched no rows" in result.error
        assert "entirely NULL over the rows it did match" in result.error

    def test_all_supported_passes(self) -> None:
        graph = _graph({"revenue": _extract("revenue"), "cogs": _extract("cogs")})
        execution = _execution({"revenue": 1000.0, "cogs": 600.0}, output_value=40.0)

        assert verify_execution(graph, execution).success

    def test_genuine_zero_extract_passes(self) -> None:
        """A real 0 (rows matched, summed to 0) has support — it is not NULL."""
        graph = _graph({"revenue": _extract("revenue"), "cogs": _extract("cogs")})
        execution = _execution({"revenue": 1000.0, "cogs": 0.0}, output_value=100.0)

        assert verify_execution(graph, execution).success

    def test_null_formula_step_reads_as_degenerate_not_unfiltered(self) -> None:
        """A NULL non-extract step is 'degenerate', not 'filter matched no rows'."""
        graph = _graph(
            {
                "revenue": _extract("revenue"),
                "ratio": GraphStep(step_id="ratio", step_type=StepType.FORMULA, expression="x"),
            }
        )
        execution = _execution({"revenue": 1000.0, "ratio": None}, output_value=None)

        result = verify_execution(graph, execution)
        assert not result.success
        assert "ratio" in result.error
        assert "degenerate" in result.error
        assert "filter matched no rows" not in result.error

    def test_null_output_value_is_inconclusive(self) -> None:
        """Even with supported steps, a NULL composed value is degenerate."""
        graph = _graph({"revenue": _extract("revenue")})
        execution = _execution({"revenue": 1000.0}, output_value=None)

        result = verify_execution(graph, execution)
        assert not result.success
        assert "NULL" in result.error

    def test_genuine_zero_output_value_passes(self) -> None:
        """A composed value of exactly 0 is a real answer, not degenerate."""
        graph = _graph({"revenue": _extract("revenue")})
        execution = _execution({"revenue": 1000.0}, output_value=0.0)

        assert verify_execution(graph, execution).success


class TestDeclaredConditions:
    """Declared conditions are EXPECTATIONS, not gates (DAT-699): violations flag
    the executed metric — the number is never refused. Result.fail is reserved
    for no-value outcomes (the support gate)."""

    def test_violated_condition_flags_with_message_never_gates(self) -> None:
        """A declared `value > 0` on a 0-valued extract executes WITH the flag —
        a "shouldn't" stated as "can't" used to block real numbers (negative
        COGS is unusual, not impossible)."""
        graph = _graph(
            {
                "revenue": _extract(
                    "revenue",
                    validations=[
                        StepValidation(condition="value > 0", message="Revenue must be positive")
                    ],
                )
            }
        )
        execution = _execution({"revenue": 0.0}, output_value=0.0)

        result = verify_execution(graph, execution)
        assert result.success
        flags = result.unwrap()
        assert len(flags) == 1
        assert "declared expectation not met" in flags[0]
        assert "Revenue must be positive" in flags[0]
        assert "value=0.0" in flags[0]
        assert "severity=error" in flags[0]  # severity rides as the flag's weight

    def test_satisfied_condition_passes_with_no_flags(self) -> None:
        graph = _graph(
            {
                "revenue": _extract("revenue", validations=[StepValidation(condition="value > 0")]),
                "cogs": _extract("cogs", validations=[StepValidation(condition="value >= 0")]),
            }
        )
        execution = _execution({"revenue": 1000.0, "cogs": 0.0}, output_value=100.0)

        result = verify_execution(graph, execution)
        assert result.success
        assert result.unwrap() == []

    def test_unbindable_condition_is_skipped(self) -> None:
        """A condition whose step_id has no executed step is skipped, not flagged —
        the support gate already guards the real risk; DAT-619 hardens binding."""
        graph = _graph(
            {"revenue": _extract("revenue", validations=[StepValidation(condition="value > 0")])}
        )
        # The executed step is named differently (LLM renamed it) — no binding.
        execution = _execution({"total_revenue": 1000.0}, output_value=1000.0)

        result = verify_execution(graph, execution)
        assert result.success
        assert result.unwrap() == []

    def test_malformed_condition_flags_never_raises_or_gates(self) -> None:
        """A malformed catalogue condition is a config bug: flagged visibly on the
        executed artifact, never a raise and never a reason to refuse a good number."""
        graph = _graph(
            {
                "revenue": _extract(
                    "revenue", validations=[StepValidation(condition="value.__class__")]
                )
            }
        )
        execution = _execution({"revenue": 1000.0}, output_value=1000.0)

        result = verify_execution(graph, execution)
        assert result.success
        assert any("malformed" in f for f in result.unwrap())

    def test_unparseable_condition_flags_not_crashes_worker(self) -> None:
        """A condition that isn't valid Python (e.g. SQL `AND` instead of a chained
        comparison) raises SyntaxError in ast.parse — caught HERE as a flag, not
        escaping to the blanket worker handler as an opaque
        "Unexpected error ... invalid syntax" (the dso/dpo regression)."""
        graph = _graph(
            {
                "revenue": _extract(
                    "revenue", validations=[StepValidation(condition="value >= 0 AND value <= 365")]
                )
            }
        )
        execution = _execution({"revenue": 30.0}, output_value=30.0)

        result = verify_execution(graph, execution)
        assert result.success
        assert any("malformed" in f for f in result.unwrap())

    def test_all_violations_flag_not_just_the_first(self) -> None:
        """Every violated expectation surfaces — the old gate stopped at one."""
        graph = _graph(
            {
                "revenue": _extract(
                    "revenue", validations=[StepValidation(condition="value > 0", message="R")]
                ),
                "cogs": _extract(
                    "cogs", validations=[StepValidation(condition="value >= 0", message="C")]
                ),
            }
        )
        execution = _execution({"revenue": 0.0, "cogs": -5.0}, output_value=5.0)

        result = verify_execution(graph, execution)
        assert result.success
        assert len(result.unwrap()) == 2

    def test_decimal_value_is_comparable(self) -> None:
        """Currency sums arrive as Decimal in production — conditions still hold."""
        graph = _graph(
            {"revenue": _extract("revenue", validations=[StepValidation(condition="value > 0")])}
        )
        ex = GraphExecution(execution_id="e", graph_id="gross_margin", source=GraphSource.SYSTEM)
        sr = StepResult(step_id="revenue")
        sr.value_scalar = float(Decimal("1234.56"))
        ex.step_results = [sr]
        ex.output_value = Decimal("1234.56")

        result = verify_execution(graph, ex)
        assert result.success
        assert result.unwrap() == []


class TestConditionEvaluator:
    @pytest.mark.parametrize(
        ("condition", "value", "expected"),
        [
            ("value > 0", 5, True),
            ("value > 0", 0, False),
            ("value > 0", -1, False),
            ("value >= 0", 0, True),
            ("value >= 0", -0.01, False),
            ("value < 100", 50, True),
            ("value != 0", 0, False),
            ("value == 42", 42, True),
            ("0 < value < 100", 50, True),
            ("0 < value < 100", 150, False),
            # chained bound — the dso/dpo range condition shape (was SQL `AND`)
            ("0 <= value <= 365", 30, True),
            ("0 <= value <= 365", 400, False),
            ("0 <= value <= 365", -1, False),
            ("value > -10", -5, True),
        ],
    )
    def test_evaluates(self, condition: str, value: float, expected: bool) -> None:
        assert _condition_holds(condition, value) is expected

    def test_malformed_condition_raises(self) -> None:
        """An over-broad/unsupported condition fails loud, never silently passes."""
        with pytest.raises(ValueError):
            _condition_holds("value.__class__", 1)
        with pytest.raises(ValueError):
            _condition_holds("other > 0", 1)
