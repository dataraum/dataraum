"""Tests for semantic layer entropy detectors (DAT-442 two-table).

Measurements are pure functions in entropy/stats.py: temporal_entropy →
time_role_mismatch (binary), business_meaning → confidence_entropy (1 - confidence,
no deterministic metadata override per ADR-0009).
"""

import pytest

from dataraum.entropy.detectors import (
    BusinessMeaningDetector,
    DetectorContext,
)
from dataraum.entropy.detectors.semantic.temporal_entropy import TemporalEntropyDetector


class TestTemporalEntropyDetector:
    """Binary time-role mismatch via stats.time_role_mismatch (DAT-442 two-table)."""

    @pytest.fixture
    def detector(self) -> TemporalEntropyDetector:
        return TemporalEntropyDetector()

    def _context(self, data_type: str, semantic_role: str | None) -> DetectorContext:
        return DetectorContext(
            table_name="payments",
            column_name="date",
            analysis_results={
                "typing": {"data_type": data_type},
                "semantic": {"semantic_role": semantic_role},
            },
        )

    def test_timestamp_role_on_varchar_is_mismatch(self, detector: TemporalEntropyDetector) -> None:
        """A timestamp role on a non-temporal type (corrupt dates → VARCHAR) → 1.0."""
        results = detector.detect(self._context("VARCHAR", "timestamp"))
        assert len(results) == 1
        assert results[0].score == 1.0
        assert results[0].evidence[0]["temporal_status"] == "mismatch"

    def test_aligned_temporal_column_is_zero(self, detector: TemporalEntropyDetector) -> None:
        """A DATE-typed column marked as a timestamp role is aligned → 0.0."""
        results = detector.detect(self._context("DATE", "timestamp"))
        assert len(results) == 1
        assert results[0].score == 0.0
        assert results[0].evidence[0]["temporal_status"] == "aligned"

    def test_unmarked_date_is_no_longer_a_misfire(self, detector: TemporalEntropyDetector) -> None:
        """A DATE column merely not marked as the time axis → 0.0 (was a 0.6 misfire)."""
        results = detector.detect(self._context("DATE", "dimension"))
        assert len(results) == 1
        assert results[0].score == 0.0
        assert results[0].evidence[0]["temporal_status"] == "unmarked"

    def test_non_temporal_column_is_skipped(self, detector: TemporalEntropyDetector) -> None:
        """Neither a temporal type nor a timestamp role → nothing to measure."""
        assert detector.detect(self._context("BIGINT", "measure")) == []


class TestBusinessMeaningDetector:
    """The score is the LLM's naming confidence alone: score = 1 - confidence.

    No deterministic metadata override (ADR-0009 / DAT-442 two-table): documentation
    and ontology presence are evidence context, never the score.
    """

    @pytest.fixture
    def detector(self) -> BusinessMeaningDetector:
        """Create detector instance."""
        return BusinessMeaningDetector()

    def _context(self, *, confidence: float | None = None, **semantic: object) -> DetectorContext:
        sem: dict[str, object] = dict(semantic)
        if confidence is not None:
            sem["confidence"] = confidence
        return DetectorContext(
            table_name="orders", column_name="amount", analysis_results={"semantic": sem}
        )

    def test_score_is_one_minus_confidence(self, detector: BusinessMeaningDetector) -> None:
        """The measurement is the LLM's naming confidence alone: score == 1 - confidence."""
        results = detector.detect(self._context(confidence=0.4))
        assert len(results) == 1
        assert results[0].score == pytest.approx(0.6)
        components = results[0].evidence[0]["score_components"]
        assert components["naming_confidence"] == pytest.approx(0.4)

    def test_high_confidence_is_low_entropy(self, detector: BusinessMeaningDetector) -> None:
        """A confidently-understood column is low entropy."""
        results = detector.detect(self._context(confidence=0.95))
        assert results[0].score == pytest.approx(0.05, abs=0.001)

    def test_low_confidence_is_high_entropy(self, detector: BusinessMeaningDetector) -> None:
        """A garbage name the LLM is unsure about is high entropy (teach: name it)."""
        results = detector.detect(self._context(confidence=0.1))
        assert results[0].score == pytest.approx(0.9, abs=0.001)

    def test_documentation_presence_does_not_change_the_score(
        self, detector: BusinessMeaningDetector
    ) -> None:
        """ADR-0009 hard rule: metadata presence is CONTEXT, never score.

        At the same confidence, a fully-documented column and a bare one score
        identically — the old base_score / concept_bonus deterministic override is gone.
        """
        bare = detector.detect(self._context(confidence=0.4, business_description=None))
        documented = detector.detect(
            self._context(
                confidence=0.4,
                business_description="Total order amount in USD",
                business_name="Order Amount",
                entity_type="monetary_amount",
                meaning="Total order amount",
            )
        )
        assert bare[0].score == pytest.approx(documented[0].score)
        assert bare[0].score == pytest.approx(0.6)

    def test_missing_confidence_defaults_to_certain(
        self, detector: BusinessMeaningDetector
    ) -> None:
        """No confidence on the annotation → assume certain (1.0) → zero entropy."""
        results = detector.detect(self._context(business_description="Order amount"))
        assert results[0].score == pytest.approx(0.0)

    def test_raw_metrics_collected_as_context(self, detector: BusinessMeaningDetector) -> None:
        """Documentation facts are still collected in evidence as context (not score)."""
        results = detector.detect(
            self._context(
                confidence=0.95,
                business_description="Order amount",
                business_name="Order Amount",
                entity_type="monetary_amount",
                semantic_role="measure",
            )
        )
        raw = results[0].evidence[0]["raw_metrics"]
        assert raw["description"] == "Order amount"
        assert raw["has_business_name"] is True
        assert raw["has_entity_type"] is True
        assert raw["semantic_role"] == "measure"
        assert raw["semantic_confidence"] == 0.95

    def test_detector_properties(self, detector: BusinessMeaningDetector):
        """Test detector has correct properties."""
        assert detector.detector_id == "business_meaning"
        assert detector.layer == "semantic"
        assert detector.dimension == "business_meaning"
        assert detector.required_analyses == ["semantic"]
