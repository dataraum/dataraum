"""Tests for UnitEntropyDetector — the value-carried unit half (DAT-647 split).

The catalogue-grain unit-source half lives in test_unit_source_entropy.py. This
detector scores ONLY the value-carried unit (typing.detected_unit); it no longer
reads unit_source_column and no longer emits "missing" for a plain measure.
"""

import pytest

from dataraum.entropy.detectors import DetectorContext
from dataraum.entropy.detectors.semantic.unit_entropy import UnitEntropyDetector


class TestUnitEntropy:
    @pytest.fixture
    def detector(self) -> UnitEntropyDetector:
        return UnitEntropyDetector()

    def test_declared_value_unit(self, detector: UnitEntropyDetector):
        """A confidently-detected value-carried unit → low entropy (1 - confidence)."""
        context = DetectorContext(
            table_name="orders",
            column_name="total_usd",
            analysis_results={
                "typing": {"detected_unit": "USD", "unit_confidence": 0.9},
                "semantic": {"semantic_role": "measure"},
            },
        )

        results = detector.detect(context)

        assert len(results) == 1
        assert results[0].score == pytest.approx(0.1, abs=0.01)
        assert results[0].evidence[0]["unit_status"] == "declared"

    def test_low_confidence_value_unit(self, detector: UnitEntropyDetector):
        """A low-confidence value-carried unit is ambiguous → high entropy."""
        context = DetectorContext(
            table_name="orders",
            column_name="amount",
            analysis_results={
                "typing": {"detected_unit": "USD", "unit_confidence": 0.3},
                "semantic": {"semantic_role": "measure"},
            },
        )

        results = detector.detect(context)

        assert len(results) == 1
        assert results[0].score == pytest.approx(0.7, abs=0.01)
        assert results[0].evidence[0]["unit_status"] == "low_confidence"

    def test_no_value_unit_abstains(self, detector: UnitEntropyDetector):
        """No value-carried unit token → abstain (0.0), NOT "missing".

        A currency measure like journal_lines.debit carries no value-token — its
        unit comes from the currency column, which is unit_source's concern. This
        detector must not block it (the DAT-647 false-block).
        """
        context = DetectorContext(
            table_name="journal_lines",
            column_name="debit",
            analysis_results={
                "typing": {"detected_unit": None, "unit_confidence": 0.0},
                # unit_source_column present in context is deliberately IGNORED here.
                "semantic": {
                    "semantic_role": "measure",
                    "unit_source_column": "journal_lines.currency",
                },
            },
        )

        results = detector.detect(context)

        assert len(results) == 1
        assert results[0].score == 0.0
        assert results[0].evidence[0]["unit_status"] == "no_value_unit"
        assert "unit_source_column" not in results[0].evidence[0]

    def test_non_measure_skipped(self, detector: UnitEntropyDetector):
        """Non-measure columns don't need a unit — abstain entirely."""
        context = DetectorContext(
            table_name="customers",
            column_name="name",
            analysis_results={
                "typing": {},
                "semantic": {"semantic_role": "attribute"},
            },
        )

        assert detector.detect(context) == []
