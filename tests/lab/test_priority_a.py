"""Entropy lab Phase 4 — Priority A detector fixes.

Tests and fixes for:
- benford_compliance: Cramér's V normalization (raw chi-square inflated by sample size)
- null_ratio: assess current behavior, document as acceptable
"""

from __future__ import annotations

import math
from typing import Any

import duckdb
import pytest
from sqlalchemy.orm import Session

from dataraum.entropy.detectors.base import DetectorContext
from dataraum.entropy.detectors.value.benford import BenfordDetector
from dataraum.entropy.detectors.value.null_semantics import NullRatioDetector
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
    stats: dict[str, Any] | None = None,
    semantic: dict[str, Any] | None = None,
) -> DetectorContext:
    analysis = {}
    if stats is not None:
        analysis["statistics"] = stats
    if semantic is not None:
        analysis["semantic"] = semantic
    return DetectorContext(
        table_name="test_table",
        column_name="test_column",
        analysis_results=analysis,
    )


# ---------------------------------------------------------------------------
# benford_compliance — Cramér's V normalization
# ---------------------------------------------------------------------------


class TestBenfordCramersV:
    """Benford scores should reflect practical significance (Cramér's V),
    not just statistical significance (raw chi-square).

    Chi-square scales linearly with sample size. A chi_sq of 1917 with
    n=8000 is Cramér's V=0.17 (small effect), not catastrophic.
    """

    def test_large_sample_small_effect_scores_moderate(self) -> None:
        """Large chi-square with large n → small V → moderate score."""
        detector = BenfordDetector()
        context = _make_context(
            stats={
                "total_count": 8000,
                "quality": {
                    "benford_compliant": False,
                    "benford_analysis": {
                        "is_compliant": False,
                        "chi_square": 1917.0,
                        "p_value": 0.0,
                    },
                },
            },
            semantic={"semantic_role": "measure"},
        )
        objects = detector.detect(context)
        assert len(objects) == 1
        score = objects[0].score
        ev = objects[0].evidence[0]
        v = ev["cramers_v"]
        print(f"\n  chi_sq=1917, n=8000: V={v}, effect={ev['effect_size']}, score={score:.3f}")
        # V ≈ 0.173 → small effect, score should be moderate, not 1.0
        assert v < 0.2, f"Cramér's V should be small, got {v}"
        assert score < 0.85, f"Score should be moderate with small V, got {score}"

    def test_small_sample_same_chi_sq_scores_higher(self) -> None:
        """Same chi-square with fewer samples → larger V → higher score."""
        detector = BenfordDetector()
        context = _make_context(
            stats={
                "total_count": 500,
                "quality": {
                    "benford_compliant": False,
                    "benford_analysis": {
                        "is_compliant": False,
                        "chi_square": 1917.0,
                        "p_value": 0.0,
                    },
                },
            },
            semantic={"semantic_role": "measure"},
        )
        objects = detector.detect(context)
        score = objects[0].score
        ev = objects[0].evidence[0]
        v = ev["cramers_v"]
        print(f"\n  chi_sq=1917, n=500: V={v}, effect={ev['effect_size']}, score={score:.3f}")
        # V ≈ 0.693 → large effect, score should be high
        assert v > 0.5, f"Cramér's V should be large, got {v}"
        assert score > 0.9, f"Score should be high with large V, got {score}"

    def test_negligible_effect_scores_at_floor(self) -> None:
        """Negligible Cramér's V → score at score_non_compliant floor."""
        detector = BenfordDetector()
        context = _make_context(
            stats={
                "total_count": 3000,
                "quality": {
                    "benford_compliant": False,
                    "benford_analysis": {
                        "is_compliant": False,
                        "chi_square": 69.0,
                        "p_value": 0.0,
                    },
                },
            },
            semantic={"semantic_role": "measure"},
        )
        objects = detector.detect(context)
        score = objects[0].score
        ev = objects[0].evidence[0]
        print(f"\n  chi_sq=69, n=3000: V={ev['cramers_v']}, effect={ev['effect_size']}, score={score:.3f}")
        assert ev["effect_size"] == "negligible"
        # Negligible effect → score stays near score_non_compliant (0.7)
        assert 0.7 <= score <= 0.75, f"Expected score near 0.7, got {score}"

    def test_min_sample_size_skips_small_datasets(self) -> None:
        """Datasets below min_sample_size should be skipped."""
        detector = BenfordDetector()
        context = _make_context(
            stats={
                "total_count": 50,  # Below default min_sample_size=100
                "quality": {
                    "benford_compliant": False,
                    "benford_analysis": {
                        "is_compliant": False,
                        "chi_square": 100.0,
                        "p_value": 0.0,
                    },
                },
            },
            semantic={"semantic_role": "measure"},
        )
        objects = detector.detect(context)
        assert len(objects) == 0, "Should skip datasets below min_sample_size"

    def test_evidence_includes_sample_size(self) -> None:
        """Evidence should include n_values and cramers_v."""
        detector = BenfordDetector()
        context = _make_context(
            stats={
                "total_count": 1000,
                "quality": {
                    "benford_compliant": False,
                    "benford_analysis": {
                        "is_compliant": False,
                        "chi_square": 50.0,
                        "p_value": 0.001,
                    },
                },
            },
            semantic={"semantic_role": "measure"},
        )
        objects = detector.detect(context)
        ev = objects[0].evidence[0]
        assert "n_values" in ev
        assert "cramers_v" in ev
        assert "effect_size" in ev
        assert ev["n_values"] == 1000

    def test_compliant_column_unchanged(self) -> None:
        """Compliant columns should still score score_compliant."""
        detector = BenfordDetector()
        context = _make_context(
            stats={
                "total_count": 5000,
                "quality": {
                    "benford_compliant": True,
                    "benford_analysis": {
                        "is_compliant": True,
                        "chi_square": 5.0,
                        "p_value": 0.8,
                    },
                },
            },
            semantic={"semantic_role": "measure"},
        )
        objects = detector.detect(context)
        score = objects[0].score
        # p_value=0.8 > threshold: gradient maps near score_compliant
        assert score < 0.3, f"Compliant column should score low, got {score}"


class TestBenfordRealData:
    """Verify Benford scores on real e2e data after Cramér's V fix."""

    def test_scores_differentiate_by_effect_size(
        self,
        lab_session: Session,
        lab_duckdb: duckdb.DuckDBPyConnection | None,
    ) -> None:
        """bank_transactions.amount (V=0.17) should score higher than
        invoices.amount (V=0.05) because it has a larger effect size."""
        snap_bank = _snap_column(
            lab_session, lab_duckdb,
            "bank_transactions", "amount",
            dimensions=[SubDimension.BENFORD_COMPLIANCE],
        )
        snap_inv = _snap_column(
            lab_session, lab_duckdb,
            "invoices", "amount",
            dimensions=[SubDimension.BENFORD_COMPLIANCE],
        )
        bank_score = snap_bank.scores[str(SubDimension.BENFORD_COMPLIANCE)]
        inv_score = snap_inv.scores[str(SubDimension.BENFORD_COMPLIANCE)]
        print(f"\n  bank_transactions.amount: {bank_score:.3f}")
        print(f"  invoices.amount: {inv_score:.3f}")
        assert bank_score > inv_score, "Larger effect should score higher"

    def test_all_measures_scored_with_evidence(
        self,
        lab_session: Session,
        lab_duckdb: duckdb.DuckDBPyConnection | None,
    ) -> None:
        """All Benford-assessed measures should have Cramér's V in evidence."""
        measures = [
            ("bank_transactions", "amount"),
            ("invoices", "amount"),
            ("payments", "amount"),
            ("trial_balance", "credit_balance"),
            ("trial_balance", "debit_balance"),
        ]
        print()
        for table, col in measures:
            snap = _snap_column(
                lab_session, lab_duckdb,
                table, col,
                dimensions=[SubDimension.BENFORD_COMPLIANCE],
            )
            score = snap.scores.get(str(SubDimension.BENFORD_COMPLIANCE))
            assert score is not None, f"{table}.{col} should have Benford score"
            obj = [o for o in snap.objects if o.sub_dimension == str(SubDimension.BENFORD_COMPLIANCE)][0]
            ev = obj.evidence[0]
            assert "cramers_v" in ev, f"{table}.{col} evidence missing cramers_v"
            assert "effect_size" in ev
            assert "n_values" in ev
            print(
                f"  {table}.{col}: score={score:.3f}, "
                f"V={ev['cramers_v']}, effect={ev['effect_size']}, n={ev['n_values']}"
            )

    def test_no_score_above_one(
        self,
        lab_session: Session,
        lab_duckdb: duckdb.DuckDBPyConnection | None,
    ) -> None:
        """Scores should be clamped to [0, 1]."""
        snap = _snap_column(
            lab_session, lab_duckdb,
            "bank_transactions", "amount",
            dimensions=[SubDimension.BENFORD_COMPLIANCE],
        )
        score = snap.scores[str(SubDimension.BENFORD_COMPLIANCE)]
        assert 0.0 <= score <= 1.0


# ---------------------------------------------------------------------------
# null_ratio — assessment of current behavior
# ---------------------------------------------------------------------------


class TestNullRatio:
    """Null ratio detector assessment.

    Current behavior: score = null_ratio directly.
    This is correct for this dataset — nulls are structurally expected:
    - cost_center (28.1%): optional dimension — not all journal entries have cost centers
    - parent_id (8.3%): tree FK — root accounts have no parent
    - debit (3%): paired with credit — one side is always null
    """

    def test_score_equals_null_ratio(self) -> None:
        """Score should equal null_ratio directly."""
        detector = NullRatioDetector()
        context = _make_context(
            stats={"null_ratio": 0.25, "null_count": 250, "total_count": 1000}
        )
        objects = detector.detect(context)
        assert objects[0].score == 0.25

    def test_zero_nulls_score_zero(self) -> None:
        """Columns with no nulls should score 0.0."""
        detector = NullRatioDetector()
        context = _make_context(
            stats={"null_ratio": 0.0, "null_count": 0, "total_count": 1000}
        )
        objects = detector.detect(context)
        assert objects[0].score == 0.0

    def test_resolution_options_threshold(self) -> None:
        """document_null_semantics at >10%, filter_nulls at >40%."""
        detector = NullRatioDetector()

        # 5% — no options
        ctx_low = _make_context(
            stats={"null_ratio": 0.05, "null_count": 50, "total_count": 1000}
        )
        assert len(detector.detect(ctx_low)[0].resolution_options) == 0

        # 15% — document_null_semantics only
        ctx_mid = _make_context(
            stats={"null_ratio": 0.15, "null_count": 150, "total_count": 1000}
        )
        mid_opts = detector.detect(ctx_mid)[0].resolution_options
        assert len(mid_opts) == 1
        assert mid_opts[0].action == "document_null_semantics"

        # 50% — document + filter + impute
        ctx_high = _make_context(
            stats={"null_ratio": 0.50, "null_count": 500, "total_count": 1000}
        )
        high_opts = detector.detect(ctx_high)[0].resolution_options
        assert len(high_opts) == 3
        actions = {o.action for o in high_opts}
        assert "document_null_semantics" in actions
        assert "transform_filter_nulls" in actions
        assert "transform_impute_values" in actions

    def test_real_data_cost_center_nulls(
        self,
        lab_session: Session,
        lab_duckdb: duckdb.DuckDBPyConnection | None,
    ) -> None:
        """journal_lines.cost_center: 28.1% nulls (optional dimension)."""
        snap = _snap_column(
            lab_session, lab_duckdb,
            "journal_lines", "cost_center",
            dimensions=[SubDimension.NULL_RATIO],
        )
        score = snap.scores[str(SubDimension.NULL_RATIO)]
        print(f"\n  journal_lines.cost_center null_ratio: {score:.3f}")
        assert 0.25 < score < 0.30, f"Expected ~0.281, got {score}"
        # Should suggest document_null_semantics
        obj = snap.objects[0]
        actions = {o.action for o in obj.resolution_options}
        assert "document_null_semantics" in actions

    def test_real_data_most_columns_zero_nulls(
        self,
        lab_session: Session,
        lab_duckdb: duckdb.DuckDBPyConnection | None,
        typed_columns: list[dict[str, str]],
    ) -> None:
        """Most columns should have zero nulls → score 0.0."""
        zero_count = 0
        nonzero = []
        for col in typed_columns:
            snap = _snap_column(
                lab_session, lab_duckdb,
                col["table_name"], col["column_name"],
                dimensions=[SubDimension.NULL_RATIO],
            )
            score = snap.scores.get(str(SubDimension.NULL_RATIO), 0)
            if score == 0:
                zero_count += 1
            else:
                nonzero.append((f"{col['table_name']}.{col['column_name']}", score))

        print(f"\n  Zero-null columns: {zero_count}/{len(typed_columns)}")
        print(f"  Non-zero null columns:")
        for name, score in sorted(nonzero, key=lambda x: x[1], reverse=True):
            print(f"    {score:.3f}  {name}")
        assert zero_count > 40, f"Expected most columns to be null-free, got {zero_count}"
