"""Tests for cross_table_consistency entropy detector (docs/architecture/grounding.md).

The verdict is recomputed on demand: ``detect`` re-runs each record's
``sql_used`` via ``verdict_from_sql``, and reads the declared ``tolerance`` +
``severity`` from the spec (loaded via ``_load_run_specs``, not the record). The
unit tests patch BOTH — the verdict (keyed by ``sql_used``) and the spec map
(keyed by ``validation_id``) — so the scoring + fan-out logic is tested in
isolation; ``_score`` is tested directly against verdicts.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

import dataraum.entropy.detectors.computational.cross_table_consistency as ctc
from dataraum.analysis.validation.evaluate import ValidationVerdict
from dataraum.analysis.validation.models import ValidationStatus
from dataraum.entropy.detectors.base import DetectorContext


@pytest.fixture
def detector() -> ctc.CrossTableConsistencyDetector:
    return ctc.CrossTableConsistencyDetector()


def _verdict(
    status: ValidationStatus, *, deviation: float = 0.0, magnitude: float = 1.0
) -> ValidationVerdict:
    return ValidationVerdict(
        status=status,
        passed=status == ValidationStatus.PASSED,
        message="",
        details={"deviation": deviation, "magnitude": magnitude},
    )


def _spec(severity: str = "critical", tolerance: float = 0.01) -> MagicMock:
    spec = MagicMock()
    spec.severity = MagicMock()
    spec.severity.value = severity
    spec.parameters = {"tolerance": tolerance}
    return spec


def _make_result(
    *,
    sql_used: str = "SELECT 0 AS deviation, 1 AS magnitude",
    validation_id: str = "v1",
    columns_used: list[str] | None = None,
) -> MagicMock:
    r = MagicMock()
    r.sql_used = sql_used
    r.validation_id = validation_id
    r.columns_used = columns_used or []
    return r


def _install_verdicts(monkeypatch, mapping: dict[str, ValidationVerdict]) -> None:
    """Patch the on-demand re-run: sql_used → chosen verdict."""
    monkeypatch.setattr(ctc, "verdict_from_sql", lambda _conn, sql, **_kw: mapping[sql])


def _install_specs(monkeypatch, mapping: dict[str, MagicMock]) -> None:
    """Patch the spec load: validation_id → spec (severity + tolerance)."""
    monkeypatch.setattr(ctc, "_load_run_specs", lambda _ctx: mapping)


def _make_context(
    validations: list | None = None,
    table_id: str = "t1",
    table_name: str = "orders",
) -> DetectorContext:
    ctx = DetectorContext(table_id=table_id, table_name=table_name, duckdb_conn=MagicMock())
    if validations is not None:
        ctx.analysis_results["validation"] = validations
    return ctx


class TestScore:
    """The uniform deviation/magnitude scoring (docs/architecture/grounding.md)."""

    def test_passed_returns_zero(self) -> None:
        assert ctc._score(_verdict(ValidationStatus.PASSED), "critical") == 0.0

    def test_inconclusive_returns_zero(self) -> None:
        # Execution error / unbound / non-conforming output = ignorance, never a
        # risk measurement (the old 0.5 banded clean tables, DAT-439).
        assert ctc._score(_verdict(ValidationStatus.ERROR), "critical") == 0.0

    def test_failed_critical_is_categorical(self) -> None:
        # A CRITICAL identity failing beyond tolerance scores 1.0 regardless of
        # the relative magnitude (L7): a 10% TB↔GL break scored as a rate was
        # invisible below the band while every GL deliverable was provably wrong.
        v = _verdict(ValidationStatus.FAILED, deviation=50_000, magnitude=50_000_000)
        assert ctc._score(v, "critical") == 1.0

    def test_failed_noncritical_is_relative_discrepancy(self) -> None:
        # $50k on $50M → honest 0.001 (no boost, DAT-442).
        v = _verdict(ValidationStatus.FAILED, deviation=50_000, magnitude=50_000_000)
        assert ctc._score(v, "high") == pytest.approx(0.001, abs=1e-4)

    def test_rate_scores_as_itself(self) -> None:
        # aggregate: deviation = rate, magnitude = 1 → 0.05.
        v = _verdict(ValidationStatus.FAILED, deviation=0.05, magnitude=1.0)
        assert ctc._score(v, "high") == pytest.approx(0.05, abs=1e-6)

    def test_zero_magnitude_falls_back(self) -> None:
        v = _verdict(ValidationStatus.FAILED, deviation=100, magnitude=0)
        assert ctc._score(v, "high") == 1.0


class TestDetectNoResults:
    def test_no_validations_returns_zero(self, detector: ctc.CrossTableConsistencyDetector) -> None:
        objects = detector.detect(_make_context(validations=[]))
        assert len(objects) == 1
        assert objects[0].score == 0.0

    def test_no_data_loaded(self, detector: ctc.CrossTableConsistencyDetector) -> None:
        objects = detector.detect(_make_context())
        assert len(objects) == 1
        assert objects[0].score == 0.0


class TestDetectFailures:
    def test_single_critical_failure(
        self, detector: ctc.CrossTableConsistencyDetector, monkeypatch
    ) -> None:
        _install_specs(monkeypatch, {"v1": _spec("critical")})
        _install_verdicts(monkeypatch, {"s1": _verdict(ValidationStatus.FAILED, deviation=1)})
        ctx = _make_context(validations=[_make_result(sql_used="s1", validation_id="v1")])
        objects = detector.detect(ctx)
        assert len(objects) == 1
        assert objects[0].score == 1.0

    def test_max_aggregation(
        self, detector: ctc.CrossTableConsistencyDetector, monkeypatch
    ) -> None:
        """Worst failure drives the score (honest rate, no boost)."""
        _install_specs(monkeypatch, {"v1": _spec("critical"), "v2": _spec("high")})
        _install_verdicts(
            monkeypatch,
            {
                "pass": _verdict(ValidationStatus.PASSED),
                "rate": _verdict(ValidationStatus.FAILED, deviation=0.05, magnitude=1.0),
            },
        )
        ctx = _make_context(
            validations=[
                _make_result(sql_used="pass", validation_id="v1"),
                _make_result(sql_used="rate", validation_id="v2"),
            ]
        )
        objects = detector.detect(ctx)
        assert objects[0].score == pytest.approx(0.05, abs=1e-6)

    def test_evidence_per_check(
        self, detector: ctc.CrossTableConsistencyDetector, monkeypatch
    ) -> None:
        _install_specs(monkeypatch, {"v1": _spec("critical"), "v2": _spec("high")})
        _install_verdicts(
            monkeypatch,
            {
                "pass": _verdict(ValidationStatus.PASSED),
                "fail": _verdict(ValidationStatus.FAILED, deviation=1),
            },
        )
        ctx = _make_context(
            validations=[
                _make_result(sql_used="pass", validation_id="v1"),
                _make_result(sql_used="fail", validation_id="v2"),
            ]
        )
        evidence = detector.detect(ctx)[0].evidence
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
            table_name="journal_lines",  # narrow, workspace-unique (DAT-639)
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
            table_name="journal_lines",
            session=session,
            duckdb_conn=MagicMock(),
        )
        ctx.analysis_results["validation"] = validations
        return ctx

    def test_failed_check_bands_its_columns(
        self, detector: ctc.CrossTableConsistencyDetector, session, monkeypatch
    ) -> None:  # noqa: ANN001
        """Own-table columns matched by exact narrow name; foreign tables ignored;
        hallucinated columns dropped; column_id rides in evidence."""
        _, ids = self._seed(session)
        _install_specs(monkeypatch, {"v1": _spec("critical")})
        _install_verdicts(monkeypatch, {"s1": _verdict(ValidationStatus.FAILED, deviation=0.1)})
        ctx = self._context(
            session,
            [
                _make_result(
                    validation_id="v1",
                    sql_used="s1",
                    columns_used=[
                        "journal_lines.credit",  # ours
                        "journal_lines.debit",  # ours
                        "trial_balance.debit_balance",  # other table — not ours
                        "journal_lines.ghost",  # hallucinated column
                    ],
                )
            ],
        )
        objects = detector.detect(ctx)

        by_target = {o.target: o for o in objects}
        assert "column:journal_lines.credit" in by_target
        assert "column:journal_lines.debit" in by_target
        assert len(objects) == 3  # table object + 2 columns (ghost + foreign dropped)
        credit = by_target["column:journal_lines.credit"]
        assert credit.score == 1.0  # critical → categorical
        assert credit.evidence[0]["column_id"] == ids["credit"]

    def test_passed_checks_band_nothing(
        self, detector: ctc.CrossTableConsistencyDetector, session, monkeypatch
    ) -> None:  # noqa: ANN001
        self._seed(session)
        _install_specs(monkeypatch, {"v1": _spec("critical")})
        _install_verdicts(monkeypatch, {"sp": _verdict(ValidationStatus.PASSED)})
        ctx = self._context(
            session,
            [
                _make_result(
                    validation_id="v1", sql_used="sp", columns_used=["journal_lines.credit"]
                )
            ],
        )
        objects = detector.detect(ctx)
        assert len(objects) == 1  # the table object only
        assert objects[0].score == 0.0

    def test_worst_score_wins_per_column(
        self, detector: ctc.CrossTableConsistencyDetector, session, monkeypatch
    ) -> None:  # noqa: ANN001
        """Two failing checks touching the same column → one object, worst score,
        both checks in evidence."""
        _, ids = self._seed(session)
        _install_specs(monkeypatch, {"v_rate": _spec("high"), "v_critical": _spec("critical")})
        _install_verdicts(
            monkeypatch,
            {
                "rate": _verdict(ValidationStatus.FAILED, deviation=0.05, magnitude=1.0),
                "crit": _verdict(ValidationStatus.FAILED, deviation=1.0),
            },
        )
        ctx = self._context(
            session,
            [
                _make_result(
                    validation_id="v_rate",
                    sql_used="rate",
                    columns_used=["journal_lines.credit"],
                ),
                _make_result(
                    validation_id="v_critical",
                    sql_used="crit",
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
    def test_detector_id(self, detector: ctc.CrossTableConsistencyDetector) -> None:
        assert detector.detector_id == "cross_table_consistency"

    def test_scope(self, detector: ctc.CrossTableConsistencyDetector) -> None:
        assert detector.scope == "table"

    def test_layer(self, detector: ctc.CrossTableConsistencyDetector) -> None:
        assert str(detector.layer) == "computational"

    def test_required_analyses(self, detector: ctc.CrossTableConsistencyDetector) -> None:
        assert str(detector.required_analyses[0]) == "validation"
