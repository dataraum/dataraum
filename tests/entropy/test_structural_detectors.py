"""Tests for structural layer entropy detectors."""

import pytest

from dataraum_context.entropy.detectors import (
    DetectorContext,
    JoinPathDeterminismDetector,
    TypeFidelityDetector,
)


class TestTypeFidelityDetector:
    """Tests for TypeFidelityDetector."""

    @pytest.fixture
    def detector(self) -> TypeFidelityDetector:
        """Create detector instance."""
        return TypeFidelityDetector()

    @pytest.mark.asyncio
    async def test_perfect_parse_rate(self, detector: TypeFidelityDetector):
        """Test entropy is 0 for perfect parse rate."""
        context = DetectorContext(
            table_name="orders",
            column_name="amount",
            analysis_results={
                "typing": {
                    "parse_success_rate": 1.0,
                    "detected_type": "DECIMAL",
                    "failed_examples": [],
                }
            },
        )

        results = await detector.detect(context)

        assert len(results) == 1
        assert results[0].score == pytest.approx(0.0, abs=0.01)
        assert results[0].layer == "structural"
        assert results[0].dimension == "types"

    @pytest.mark.asyncio
    async def test_low_parse_rate(self, detector: TypeFidelityDetector):
        """Test high entropy for low parse rate."""
        context = DetectorContext(
            table_name="orders",
            column_name="amount",
            analysis_results={
                "typing": {
                    "parse_success_rate": 0.6,
                    "detected_type": "INTEGER",
                    "failed_examples": ["abc", "n/a", "unknown"],
                }
            },
        )

        results = await detector.detect(context)

        assert len(results) == 1
        assert results[0].score == pytest.approx(0.4, abs=0.01)
        # Should have resolution options for significant failure rate
        assert len(results[0].resolution_options) >= 1

    @pytest.mark.asyncio
    async def test_resolution_options_at_high_entropy(self, detector: TypeFidelityDetector):
        """Test resolution options are provided for high entropy."""
        context = DetectorContext(
            table_name="orders",
            column_name="value",
            analysis_results={
                "typing": {
                    "parse_success_rate": 0.5,
                    "failed_examples": ["bad1", "bad2"],
                }
            },
        )

        results = await detector.detect(context)

        # Should have override_type and quarantine_values options
        actions = [opt.action for opt in results[0].resolution_options]
        assert "override_type" in actions
        assert "quarantine_values" in actions

    @pytest.mark.asyncio
    async def test_evidence_includes_failure_samples(self, detector: TypeFidelityDetector):
        """Test evidence includes failure samples."""
        context = DetectorContext(
            table_name="test",
            column_name="col",
            analysis_results={
                "typing": {
                    "parse_success_rate": 0.9,
                    "failed_examples": ["sample1", "sample2"],
                }
            },
        )

        results = await detector.detect(context)

        evidence = results[0].evidence[0]
        assert "failed_examples" in evidence
        assert len(evidence["failed_examples"]) == 2

    def test_detector_properties(self, detector: TypeFidelityDetector):
        """Test detector has correct properties."""
        assert detector.detector_id == "type_fidelity"
        assert detector.layer == "structural"
        assert detector.dimension == "types"
        assert detector.required_analyses == ["typing"]


class TestJoinPathDeterminismDetector:
    """Tests for JoinPathDeterminismDetector."""

    @pytest.fixture
    def detector(self) -> JoinPathDeterminismDetector:
        """Create detector instance."""
        return JoinPathDeterminismDetector()

    @pytest.mark.asyncio
    async def test_single_path(self, detector: JoinPathDeterminismDetector):
        """Test low entropy for single join path."""
        context = DetectorContext(
            table_name="orders",
            column_name="customer_id",
            analysis_results={
                "relationships": {
                    "relationships": [
                        {"from_table": "orders", "to_table": "customers"},
                    ],
                    "outgoing_count": 1,
                    "incoming_count": 0,
                }
            },
        )

        results = await detector.detect(context)

        assert len(results) == 1
        assert results[0].score == pytest.approx(0.1, abs=0.01)
        assert results[0].evidence[0]["path_status"] == "single"

    @pytest.mark.asyncio
    async def test_no_paths_orphan(self, detector: JoinPathDeterminismDetector):
        """Test high entropy for orphan table with no paths."""
        context = DetectorContext(
            table_name="isolated",
            column_name="id",
            analysis_results={
                "relationships": {
                    "relationships": [],
                    "outgoing_count": 0,
                    "incoming_count": 0,
                }
            },
        )

        results = await detector.detect(context)

        assert len(results) == 1
        assert results[0].score == pytest.approx(0.9, abs=0.01)
        assert results[0].evidence[0]["path_status"] == "orphan"
        # Should suggest declaring relationship
        actions = [opt.action for opt in results[0].resolution_options]
        assert "declare_relationship" in actions

    @pytest.mark.asyncio
    async def test_multiple_paths(self, detector: JoinPathDeterminismDetector):
        """Test high entropy for multiple join paths."""
        context = DetectorContext(
            table_name="transactions",
            column_name="id",
            analysis_results={
                "relationships": {
                    "relationships": [
                        {"from_table": "transactions", "to_table": "orders"},
                        {"from_table": "transactions", "to_table": "customers"},
                        {"from_table": "transactions", "to_table": "products"},
                        {"from_table": "payments", "to_table": "transactions"},
                    ],
                    "outgoing_count": 3,
                    "incoming_count": 1,
                }
            },
        )

        results = await detector.detect(context)

        assert len(results) == 1
        assert results[0].score == pytest.approx(0.7, abs=0.01)
        assert results[0].evidence[0]["path_status"] == "multiple"
        # Should suggest preferred path
        actions = [opt.action for opt in results[0].resolution_options]
        assert "declare_preferred_path" in actions

    @pytest.mark.asyncio
    async def test_few_paths(self, detector: JoinPathDeterminismDetector):
        """Test medium entropy for few paths."""
        context = DetectorContext(
            table_name="orders",
            column_name="id",
            analysis_results={
                "relationships": [
                    {"from_table": "orders", "to_table": "customers"},
                    {"from_table": "line_items", "to_table": "orders"},
                ]
            },
        )

        results = await detector.detect(context)

        assert len(results) == 1
        assert results[0].score == pytest.approx(0.4, abs=0.01)
        assert results[0].evidence[0]["path_status"] == "few"

    def test_detector_properties(self, detector: JoinPathDeterminismDetector):
        """Test detector has correct properties."""
        assert detector.detector_id == "join_path_determinism"
        assert detector.layer == "structural"
        assert detector.dimension == "relations"
        assert detector.required_analyses == ["relationships"]
