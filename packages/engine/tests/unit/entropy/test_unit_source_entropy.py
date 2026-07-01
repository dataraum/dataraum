"""Tests for UnitSourceEntropyDetector — the semantic-grain unit-source half (DAT-647).

The value-carried half lives in test_unit_entropy.py. This detector reads
ONLY ColumnConcept.unit_source_column (catalogue grain); it does not read typing.
"""

import pytest

from dataraum.entropy.detectors import DetectorContext
from dataraum.entropy.detectors.semantic.unit_source_entropy import UnitSourceEntropyDetector


class TestUnitSourceEntropy:
    @pytest.fixture
    def detector(self) -> UnitSourceEntropyDetector:
        return UnitSourceEntropyDetector()

    def test_resolved_from_dimension(self, detector: UnitSourceEntropyDetector):
        """A measure whose unit is defined by a dimension column is resolved → 0.0.

        This is the DAT-647 case: journal_lines.debit → currency column.
        """
        context = DetectorContext(
            table_name="journal_lines",
            column_name="debit",
            analysis_results={
                "semantic": {
                    "semantic_role": "measure",
                    "unit_source_column": "journal_lines.currency",
                },
            },
        )

        results = detector.detect(context)

        assert len(results) == 1
        assert results[0].score == 0.0
        assert results[0].evidence[0]["unit_status"] == "resolved_from_dimension"
        assert results[0].evidence[0]["unit_source_column"] == "journal_lines.currency"

    def test_dimensionless(self, detector: UnitSourceEntropyDetector):
        """A dimensionless measure has no unit-source entropy → 0.0."""
        context = DetectorContext(
            table_name="orders",
            column_name="conversion_rate",
            analysis_results={
                "semantic": {
                    "semantic_role": "measure",
                    "unit_source_column": "dimensionless",
                },
            },
        )

        results = detector.detect(context)

        assert len(results) == 1
        assert results[0].score == 0.0
        assert results[0].evidence[0]["unit_status"] == "dimensionless"

    def test_unresolved_measure_blocks(self, detector: UnitSourceEntropyDetector):
        """A measure with no determinable unit source is unsafe to aggregate → 1.0."""
        context = DetectorContext(
            table_name="orders",
            column_name="total",
            analysis_results={
                "semantic": {
                    "semantic_role": "measure",
                },
            },
        )

        results = detector.detect(context)

        assert len(results) == 1
        assert results[0].score == pytest.approx(1.0)
        assert results[0].evidence[0]["unit_status"] == "unresolved"
        assert "unit_source_column" not in results[0].evidence[0]

    def test_non_measure_skipped(self, detector: UnitSourceEntropyDetector):
        """Non-measure columns don't need a unit — abstain entirely."""
        context = DetectorContext(
            table_name="customers",
            column_name="name",
            analysis_results={
                "semantic": {
                    "semantic_role": "attribute",
                },
            },
        )

        assert detector.detect(context) == []
