"""Entropy lab Phase 3 — Priority B detector fixes.

Tests and fixes for:
- type_fidelity: VARCHAR fallback blind spot (score=0.0 when typing fails)
- naming_clarity: detector_id/sub_dimension name mismatch (cosmetic)
- unit_declaration: score differentiation (verify current behavior)
"""

from __future__ import annotations

from typing import Any

import duckdb
import pytest
from sqlalchemy.orm import Session

from dataraum.entropy.detectors.base import DetectorContext
from dataraum.entropy.detectors.structural.types import TypeFidelityDetector
from dataraum.entropy.dimensions import SubDimension
from dataraum.entropy.snapshot import Snapshot, take_snapshot


def _snap_column(
    session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection | None,
    table_name: str,
    column_name: str,
    dimensions: list[SubDimension] | None = None,
) -> Snapshot:
    return take_snapshot(
        target=f"column:{table_name}.{column_name}",
        session=session,
        duckdb_conn=duckdb_conn,
        dimensions=dimensions,
    )


def _make_context(
    typing_data: dict[str, Any] | None = None,
    semantic_data: dict[str, Any] | None = None,
) -> DetectorContext:
    """Build a DetectorContext with injected analysis results."""
    analysis = {}
    if typing_data is not None:
        analysis["typing"] = typing_data
    if semantic_data is not None:
        analysis["semantic"] = semantic_data
    return DetectorContext(
        table_name="test_table",
        column_name="test_column",
        analysis_results=analysis,
    )


# ---------------------------------------------------------------------------
# type_fidelity — VARCHAR fallback blind spot
# ---------------------------------------------------------------------------


class TestTypeFidelityBlindSpot:
    """The VARCHAR fallback blind spot: typing falls back to VARCHAR when no
    candidate passes min_confidence. VARCHAR always parses at 100%, so
    score = 1.0 - 1.0 = 0.0, hiding the failure.

    decision_source="fallback" is the signal. The detector should penalize it.
    """

    def test_varchar_fallback_produces_nonzero_score(self) -> None:
        """Fallback VARCHAR should produce a non-zero score (blind spot fixed)."""
        detector = TypeFidelityDetector()
        context = _make_context(
            typing_data={
                "resolved_type": "VARCHAR",
                "detected_type": "VARCHAR",
                "decision_source": "fallback",
                "decision_reason": "No candidate above min_confidence",
                "parse_success_rate": 1.0,
                "confidence": 0.3,
                "failed_examples": [],
            }
        )
        objects = detector.detect(context)
        assert len(objects) == 1
        score = objects[0].score
        print(f"\n  Fallback VARCHAR score (fixed): {score}")
        # Default score_fallback is 0.5
        assert score == 0.5, f"Expected fallback score 0.5, got {score}"
        # Evidence should mark this as a fallback
        ev = objects[0].evidence[0]
        assert ev["is_fallback"] is True
        assert ev["decision_source"] == "fallback"

    def test_automatic_varchar_should_score_zero(self) -> None:
        """Genuine VARCHAR columns (decision_source=automatic) should score 0.0."""
        detector = TypeFidelityDetector()
        context = _make_context(
            typing_data={
                "resolved_type": "VARCHAR",
                "detected_type": "VARCHAR",
                "decision_source": "automatic",
                "decision_reason": "Best candidate with confidence 1.00",
                "parse_success_rate": 1.0,
                "confidence": 1.0,
                "failed_examples": [],
            }
        )
        objects = detector.detect(context)
        assert objects[0].score == 0.0, "Genuine VARCHAR should remain at 0.0"

    def test_low_parse_rate_scores_correctly(self) -> None:
        """Columns with actual parse failures should produce non-zero scores."""
        detector = TypeFidelityDetector()
        context = _make_context(
            typing_data={
                "resolved_type": "DOUBLE",
                "detected_type": "DOUBLE",
                "decision_source": "automatic",
                "parse_success_rate": 0.95,
                "confidence": 0.99,
                "failed_examples": ["N/A", "null", "TBD"],
            }
        )
        objects = detector.detect(context)
        score = objects[0].score
        assert abs(score - 0.05) < 0.001, f"Expected ~0.05, got {score}"

    def test_evidence_includes_decision_source(self) -> None:
        """Evidence should include decision_source for transparency."""
        detector = TypeFidelityDetector()
        context = _make_context(
            typing_data={
                "resolved_type": "VARCHAR",
                "detected_type": "VARCHAR",
                "decision_source": "fallback",
                "parse_success_rate": 1.0,
                "confidence": 0.3,
                "failed_examples": [],
            }
        )
        objects = detector.detect(context)
        ev = objects[0].evidence[0]
        assert "decision_source" in ev, "Evidence should include decision_source"
        assert "is_fallback" in ev, "Evidence should include is_fallback flag"
        assert ev["decision_source"] == "fallback"
        assert ev["is_fallback"] is True
        print(f"\n  Evidence keys: {list(ev.keys())}")

    def test_real_data_no_fallback_columns(
        self,
        lab_session: Session,
        lab_duckdb: duckdb.DuckDBPyConnection | None,
    ) -> None:
        """Verify the e2e dataset has no fallback columns (all automatic)."""
        from sqlalchemy import text

        rows = lab_session.execute(
            text(
                "SELECT td.decision_source, COUNT(*) "
                "FROM type_decisions td "
                "JOIN columns c ON td.column_id = c.column_id "
                "JOIN tables t ON c.table_id = t.table_id "
                "WHERE t.layer = 'typed' "
                "GROUP BY td.decision_source"
            )
        ).fetchall()
        sources = {r[0]: r[1] for r in rows}
        print(f"\n  Decision sources in e2e data: {sources}")
        assert "fallback" not in sources, "Expected no fallback columns in e2e data"

    def test_journal_lines_debit_has_slight_entropy(
        self,
        lab_session: Session,
        lab_duckdb: duckdb.DuckDBPyConnection | None,
    ) -> None:
        """journal_lines.debit has confidence=0.99 → slight type fidelity entropy."""
        snap = _snap_column(
            lab_session, lab_duckdb,
            "journal_lines", "debit",
            dimensions=[SubDimension.TYPE_FIDELITY],
        )
        score = snap.scores.get(str(SubDimension.TYPE_FIDELITY))
        assert score is not None and score > 0, f"Expected slight entropy, got {score}"
        print(f"\n  journal_lines.debit type_fidelity: {score:.4f}")


# ---------------------------------------------------------------------------
# naming_clarity — verify current behavior, document name mismatch
# ---------------------------------------------------------------------------


class TestNamingClarity:
    """The detector_id is 'business_meaning' but sub_dimension is NAMING_CLARITY.
    Functionally correct — measures documentation completeness of business meaning.
    """

    def test_fully_documented_column_scores_near_zero(
        self,
        lab_session: Session,
        lab_duckdb: duckdb.DuckDBPyConnection | None,
    ) -> None:
        """Fully documented columns should score near 0.0."""
        snap = _snap_column(
            lab_session, lab_duckdb,
            "bank_transactions", "amount",
            dimensions=[SubDimension.NAMING_CLARITY],
        )
        score = snap.scores.get(str(SubDimension.NAMING_CLARITY))
        assert score is not None
        assert score < 0.05, f"Fully documented column should be near 0, got {score}"

    def test_evidence_shows_assessment(
        self,
        lab_session: Session,
        lab_duckdb: duckdb.DuckDBPyConnection | None,
    ) -> None:
        """Evidence should contain assessment and score components."""
        snap = _snap_column(
            lab_session, lab_duckdb,
            "bank_transactions", "amount",
            dimensions=[SubDimension.NAMING_CLARITY],
        )
        for obj in snap.objects:
            if obj.sub_dimension == str(SubDimension.NAMING_CLARITY):
                ev = obj.evidence[0]
                assert "assessment" in ev
                assert "score_components" in ev
                assert "raw_metrics" in ev
                print(f"\n  Assessment: {ev['assessment']}")
                print(f"  Score components: {ev['score_components']}")

    def test_columns_without_business_name_score_higher(
        self,
        lab_session: Session,
        lab_duckdb: duckdb.DuckDBPyConnection | None,
        typed_columns: list[dict[str, str]],
    ) -> None:
        """Columns missing business_name or entity_type should score higher."""
        scores_by_assessment: dict[str, list[float]] = {}
        for col in typed_columns[:10]:  # Sample 10 for speed
            snap = _snap_column(
                lab_session, lab_duckdb,
                col["table_name"], col["column_name"],
                dimensions=[SubDimension.NAMING_CLARITY],
            )
            for obj in snap.objects:
                if obj.sub_dimension == str(SubDimension.NAMING_CLARITY):
                    assessment = obj.evidence[0]["assessment"]
                    scores_by_assessment.setdefault(assessment, []).append(obj.score)

        print(f"\n  Scores by assessment:")
        for assessment, scores in sorted(scores_by_assessment.items()):
            avg = sum(scores) / len(scores)
            print(f"    {assessment}: n={len(scores)}, avg={avg:.3f}")


# ---------------------------------------------------------------------------
# unit_declaration — verify differentiation works
# ---------------------------------------------------------------------------


class TestUnitDeclaration:
    """Verify unit_declaration differentiates between inferred and dimensionless."""

    def test_dimensionless_scores_lower(
        self,
        lab_session: Session,
        lab_duckdb: duckdb.DuckDBPyConnection | None,
    ) -> None:
        """fx_rates.rate (dimensionless) should score lower than inferred columns."""
        snap_rate = _snap_column(
            lab_session, lab_duckdb,
            "fx_rates", "rate",
            dimensions=[SubDimension.UNIT_DECLARATION],
        )
        snap_amount = _snap_column(
            lab_session, lab_duckdb,
            "bank_transactions", "amount",
            dimensions=[SubDimension.UNIT_DECLARATION],
        )
        rate_score = snap_rate.scores.get(str(SubDimension.UNIT_DECLARATION))
        amount_score = snap_amount.scores.get(str(SubDimension.UNIT_DECLARATION))
        print(f"\n  fx_rates.rate (dimensionless): {rate_score}")
        print(f"  bank_transactions.amount (inferred from currency): {amount_score}")
        assert rate_score is not None and amount_score is not None
        assert rate_score < amount_score, "Dimensionless should score lower than inferred"

    def test_unit_evidence_populated(
        self,
        lab_session: Session,
        lab_duckdb: duckdb.DuckDBPyConnection | None,
    ) -> None:
        """Unit evidence should show unit_status and unit_source_column."""
        snap = _snap_column(
            lab_session, lab_duckdb,
            "bank_transactions", "amount",
            dimensions=[SubDimension.UNIT_DECLARATION],
        )
        for obj in snap.objects:
            if obj.sub_dimension == str(SubDimension.UNIT_DECLARATION):
                ev = obj.evidence[0]
                assert "unit_status" in ev
                assert "unit_source_column" in ev
                print(f"\n  unit_status: {ev['unit_status']}")
                print(f"  unit_source_column: {ev.get('unit_source_column')}")

    def test_non_measure_columns_skipped(
        self,
        lab_session: Session,
        lab_duckdb: duckdb.DuckDBPyConnection | None,
    ) -> None:
        """Non-measure columns should not produce unit scores."""
        snap = _snap_column(
            lab_session, lab_duckdb,
            "bank_transactions", "txn_id",
            dimensions=[SubDimension.UNIT_DECLARATION],
        )
        assert str(SubDimension.UNIT_DECLARATION) not in snap.scores

    def test_all_measure_columns_scored(
        self,
        lab_session: Session,
        lab_duckdb: duckdb.DuckDBPyConnection | None,
    ) -> None:
        """All measure columns should have unit scores with evidence."""
        from sqlalchemy import text

        measures = lab_session.execute(
            text(
                "SELECT c.column_name, t.table_name "
                "FROM semantic_annotations sa "
                "JOIN columns c ON sa.column_id = c.column_id "
                "JOIN tables t ON c.table_id = t.table_id "
                "WHERE sa.semantic_role = 'measure' AND t.layer = 'typed' "
                "ORDER BY t.table_name, c.column_name"
            )
        ).fetchall()

        print(f"\n  Measure columns ({len(measures)}):")
        for row in measures:
            table_name, col_name = row[1], row[0]
            snap = _snap_column(
                lab_session, lab_duckdb,
                table_name, col_name,
                dimensions=[SubDimension.UNIT_DECLARATION],
            )
            score = snap.scores.get(str(SubDimension.UNIT_DECLARATION))
            status = "—"
            for obj in snap.objects:
                if obj.sub_dimension == str(SubDimension.UNIT_DECLARATION):
                    status = obj.evidence[0].get("unit_status", "?")
            print(f"    {table_name}.{col_name}: {score:.3f} ({status})")
            assert score is not None, f"Measure column {table_name}.{col_name} has no unit score"
