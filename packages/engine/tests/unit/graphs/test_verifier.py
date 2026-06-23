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
        """An extract that aggregated to NULL (no rows matched) fails loud."""
        graph = _graph({"revenue": _extract("revenue"), "cogs": _extract("cogs")})
        execution = _execution({"revenue": 1000.0, "cogs": None}, output_value=100.0)

        result = verify_execution(graph, execution)

        assert not result.success
        assert "cogs" in result.error
        assert "no support" in result.error

    def test_all_supported_passes(self) -> None:
        graph = _graph({"revenue": _extract("revenue"), "cogs": _extract("cogs")})
        execution = _execution({"revenue": 1000.0, "cogs": 600.0}, output_value=40.0)

        assert verify_execution(graph, execution).success

    def test_genuine_zero_extract_passes(self) -> None:
        """A real 0 (rows matched, summed to 0) has support — it is not NULL."""
        graph = _graph({"revenue": _extract("revenue"), "cogs": _extract("cogs")})
        execution = _execution({"revenue": 1000.0, "cogs": 0.0}, output_value=100.0)

        assert verify_execution(graph, execution).success

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
    def test_violated_condition_fails_with_message(self) -> None:
        """A declared `value > 0` on a 0-valued extract fails with the message."""
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
        assert not result.success
        assert "Revenue must be positive" in result.error

    def test_satisfied_condition_passes(self) -> None:
        graph = _graph(
            {
                "revenue": _extract(
                    "revenue", validations=[StepValidation(condition="value > 0")]
                ),
                "cogs": _extract("cogs", validations=[StepValidation(condition="value >= 0")]),
            }
        )
        execution = _execution({"revenue": 1000.0, "cogs": 0.0}, output_value=100.0)

        assert verify_execution(graph, execution).success

    def test_unbindable_condition_is_skipped(self) -> None:
        """A condition whose step_id has no executed step is skipped, not failed —
        the support gate already guards the real risk; DAT-619 hardens binding."""
        graph = _graph(
            {
                "revenue": _extract(
                    "revenue", validations=[StepValidation(condition="value > 0")]
                )
            }
        )
        # The executed step is named differently (LLM renamed it) — no binding.
        execution = _execution({"total_revenue": 1000.0}, output_value=1000.0)

        assert verify_execution(graph, execution).success

    def test_decimal_value_is_comparable(self) -> None:
        """Currency sums arrive as Decimal in production — conditions still hold."""
        graph = _graph(
            {
                "revenue": _extract(
                    "revenue", validations=[StepValidation(condition="value > 0")]
                )
            }
        )
        ex = GraphExecution(execution_id="e", graph_id="gross_margin", source=GraphSource.SYSTEM)
        sr = StepResult(step_id="revenue")
        sr.value_scalar = float(Decimal("1234.56"))
        ex.step_results = [sr]
        ex.output_value = Decimal("1234.56")

        assert verify_execution(graph, ex).success


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
