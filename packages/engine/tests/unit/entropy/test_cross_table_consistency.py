"""Tests for cross_table_consistency entropy detector."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from dataraum.entropy.detectors.base import DetectorContext
from dataraum.entropy.detectors.computational.cross_table_consistency import (
    CrossTableConsistencyDetector,
    _score_validation_result,
)


@pytest.fixture
def detector() -> CrossTableConsistencyDetector:
    return CrossTableConsistencyDetector()


def _make_context(
    validations: list | None = None,
    table_id: str = "t1",
    table_name: str = "orders",
) -> DetectorContext:
    ctx = DetectorContext(
        table_id=table_id,
        table_name=table_name,
    )
    if validations is not None:
        ctx.analysis_results["validation"] = validations
    return ctx


def _make_result(
    *,
    passed: bool = False,
    status: str = "failed",
    severity: str = "critical",
    details: dict | None = None,
    validation_id: str = "v1",
    message: str | None = None,
    columns_used: list[str] | None = None,
) -> MagicMock:
    r = MagicMock()
    r.passed = passed
    r.status = status
    r.severity = severity
    r.details = details or {}
    r.validation_id = validation_id
    r.message = message
    r.columns_used = columns_used or []
    return r


class TestScoreValidationResult:
    def test_passed_returns_zero(self):
        r = _make_result(passed=True)
        assert _score_validation_result(r) == 0.0

    def test_error_returns_zero(self):
        """Execution errors AND inconclusive evaluations (DAT-439) are
        ignorance, never a risk measurement (L7): the old 0.5 banded clean
        tables whenever LLM SQL generation was nondeterministically broken."""
        r = _make_result(status="error")
        assert _score_validation_result(r) == 0.0

    def test_failed_critical_is_categorical(self):
        """A CRITICAL identity failing beyond its own tolerance scores 1.0
        regardless of the relative magnitude (L7): the injected 10% TB↔GL
        break scored 0.001-0.10 as a rate — invisible below the band — while
        every GL-derived deliverable number was provably wrong."""
        r = _make_result(
            severity="critical",
            details={"check_type": "balance", "difference": 50_000, "magnitude": 50_000_000},
        )
        assert _score_validation_result(r) == 1.0

    def test_skipped_returns_zero(self):
        """A bind-time skip (LLM declared the validation inapplicable) is not
        a data measurement — without the explicit branch it would fall into
        check-type scoring and a comparison skip would score 1.0 (DAT-439)."""
        r = _make_result(status="skipped", details={"check_type": "comparison"})
        assert _score_validation_result(r) == 0.0

    def test_comparison_failure_returns_one(self):
        r = _make_result(details={"check_type": "comparison"})
        assert _score_validation_result(r) == 1.0

    def test_balance_small_difference(self):
        """Non-critical: $50k on $50M → honest relative discrepancy 0.001 (no boost)."""
        r = _make_result(
            severity="high",
            details={
                "check_type": "balance",
                "difference": 50_000,
                "magnitude": 50_000_000,
            },
        )
        score = _score_validation_result(r)
        assert score == pytest.approx(0.001, abs=1e-4)

    def test_balance_zero_magnitude(self):
        r = _make_result(details={"check_type": "balance", "difference": 100, "magnitude": 0})
        assert _score_validation_result(r) == 1.0

    def test_aggregate_orphan_rate(self):
        """Non-critical: 5% orphans → honest rate 0.05 (no boost, DAT-442)."""
        r = _make_result(severity="high", details={"check_type": "aggregate", "orphan_rate": 0.05})
        score = _score_validation_result(r)
        assert score == pytest.approx(0.05, abs=1e-6)

    def test_constraint_violations(self):
        """Non-critical: 10 violations in 1000 rows → honest rate 0.01 (no boost)."""
        r = _make_result(
            severity="high",
            details={"check_type": "constraint", "violation_count": 10, "total_rows": 1000},
        )
        score = _score_validation_result(r)
        assert score == pytest.approx(0.01, abs=1e-6)

    def test_unknown_check_type_uses_severity(self):
        r = _make_result(severity="medium", details={"check_type": "exotic"})
        assert _score_validation_result(r) == 0.4


class TestDetectNoResults:
    def test_no_validations_returns_zero(self, detector: CrossTableConsistencyDetector):
        ctx = _make_context(validations=[])
        objects = detector.detect(ctx)
        assert len(objects) == 1
        assert objects[0].score == 0.0

    def test_no_data_loaded(self, detector: CrossTableConsistencyDetector):
        ctx = _make_context()
        objects = detector.detect(ctx)
        assert len(objects) == 1
        assert objects[0].score == 0.0


class TestDetectFailures:
    def test_single_critical_failure(self, detector: CrossTableConsistencyDetector):
        ctx = _make_context(validations=[_make_result(details={"check_type": "comparison"})])
        objects = detector.detect(ctx)
        assert len(objects) == 1
        assert objects[0].score == 1.0

    def test_max_aggregation(self, detector: CrossTableConsistencyDetector):
        """Worst failure drives the score (honest orphan rate, no boost)."""
        ctx = _make_context(
            validations=[
                _make_result(passed=True, status="passed", validation_id="v1"),
                _make_result(
                    severity="high",
                    details={"check_type": "aggregate", "orphan_rate": 0.05},
                    validation_id="v2",
                ),
            ]
        )
        objects = detector.detect(ctx)
        assert objects[0].score == pytest.approx(0.05, abs=1e-6)

    def test_evidence_per_check(self, detector: CrossTableConsistencyDetector):
        ctx = _make_context(
            validations=[
                _make_result(validation_id="v1", passed=True, status="passed"),
                _make_result(validation_id="v2", severity="high"),
            ]
        )
        objects = detector.detect(ctx)
        evidence = objects[0].evidence
        assert len(evidence) == 2
        assert evidence[0]["validation_id"] == "v1"
        assert evidence[1]["validation_id"] == "v2"


class TestColumnFanOut:
    """Failed checks band the columns their SQL touched (L7).

    The band must reach the columns deliverable metrics flow through — a
    ``table:`` row joins to nothing downstream (scoreboard baseline:
    0 prevented / 8 wrong-delivered with the GL unbanded).
    """

    @staticmethod
    def _seed(session) -> tuple[str, dict[str, str]]:  # noqa: ANN001 — conftest fixture
        from dataraum.storage import Column, Source, Table

        session.add(Source(source_id="src_v", name="src_v", source_type="csv"))
        table = Table(
            table_id="jl",
            source_id="src_v",
            table_name="src_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa__journal_lines",
            layer="typed",
        )
        session.add(table)
        session.flush()
        ids: dict[str, str] = {}
        for pos, name in enumerate(("credit", "debit", "account_id")):
            col = Column(
                table_id="jl", column_name=name, column_position=pos, resolved_type="DOUBLE"
            )
            session.add(col)
            session.flush()
            ids[name] = col.column_id
        return "jl", ids

    def _context(self, session, validations: list) -> DetectorContext:  # noqa: ANN001
        ctx = DetectorContext(
            table_id="jl",
            table_name="src_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa__journal_lines",
            session=session,
        )
        ctx.analysis_results["validation"] = validations
        return ctx

    def test_failed_check_bands_its_columns(
        self, detector: CrossTableConsistencyDetector, session
    ) -> None:  # noqa: ANN001
        """Logical and physical table prefixes both match; foreign tables ignored;
        hallucinated columns dropped; column_id rides in evidence."""
        _, ids = self._seed(session)
        ctx = self._context(
            session,
            [
                _make_result(
                    severity="critical",
                    details={"check_type": "aggregate", "violation_rate": 0.1},
                    columns_used=[
                        "journal_lines.credit",  # logical table name
                        "src_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa__journal_lines.debit",  # physical name
                        "trial_balance.debit_balance",  # other table — not ours
                        "journal_lines.ghost",  # hallucinated column
                    ],
                )
            ],
        )
        objects = detector.detect(ctx)

        by_target = {o.target: o for o in objects}
        assert (
            "column:src_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa__journal_lines.credit" in by_target
        )
        assert (
            "column:src_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa__journal_lines.debit" in by_target
        )
        assert len(objects) == 3  # table object + 2 columns (ghost + foreign dropped)
        credit = by_target[
            "column:src_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa__journal_lines.credit"
        ]
        assert credit.score == 1.0  # critical → categorical
        assert credit.evidence[0]["column_id"] == ids["credit"]

    def test_passed_checks_band_nothing(
        self, detector: CrossTableConsistencyDetector, session
    ) -> None:  # noqa: ANN001
        self._seed(session)
        ctx = self._context(
            session,
            [_make_result(passed=True, status="passed", columns_used=["journal_lines.credit"])],
        )
        objects = detector.detect(ctx)
        assert len(objects) == 1  # the table object only
        assert objects[0].score == 0.0

    def test_worst_score_wins_per_column(
        self, detector: CrossTableConsistencyDetector, session
    ) -> None:  # noqa: ANN001
        """Two failing checks touching the same column → one object, worst score,
        both checks in evidence."""
        _, ids = self._seed(session)
        ctx = self._context(
            session,
            [
                _make_result(
                    validation_id="v_rate",
                    severity="high",
                    details={"check_type": "aggregate", "violation_rate": 0.05},
                    columns_used=["journal_lines.credit"],
                ),
                _make_result(
                    validation_id="v_critical",
                    severity="critical",
                    details={"check_type": "comparison"},
                    columns_used=["journal_lines.credit"],
                ),
            ],
        )
        objects = detector.detect(ctx)
        column_objs = [o for o in objects if o.target.startswith("column:")]
        assert len(column_objs) == 1
        assert column_objs[0].score == 1.0
        assert {e["validation_id"] for e in column_objs[0].evidence} == {"v_rate", "v_critical"}


class TestDetectorProperties:
    def test_detector_id(self, detector: CrossTableConsistencyDetector):
        assert detector.detector_id == "cross_table_consistency"

    def test_scope(self, detector: CrossTableConsistencyDetector):
        assert detector.scope == "table"

    def test_layer(self, detector: CrossTableConsistencyDetector):
        assert str(detector.layer) == "computational"

    def test_required_analyses(self, detector: CrossTableConsistencyDetector):
        assert str(detector.required_analyses[0]) == "validation"
