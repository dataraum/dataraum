"""Tests for fixes API module."""

from __future__ import annotations

from unittest.mock import patch

from dataraum.pipeline.fixes.api import _detector_ids_for_gate


class TestDetectorIdsForGate:
    """Test gate → detector IDs collection from YAML declarations."""

    def test_quality_review_collects_zone1_detectors(self) -> None:
        ids = _detector_ids_for_gate("quality_review")
        # Zone 1 detectors from typing, statistics, semantic phases
        assert "type_fidelity" in ids
        assert "null_ratio" in ids
        assert "business_meaning" in ids
        # Zone 2 detector should not be included
        assert "temporal_drift" not in ids

    def test_analysis_review_collects_zone2_detectors(self) -> None:
        ids = _detector_ids_for_gate("analysis_review")
        # Includes zone 1 detectors
        assert "type_fidelity" in ids
        assert "null_ratio" in ids
        # Includes zone 2 detectors
        assert "dimension_coverage" in ids
        assert "derived_value" in ids
        assert "column_quality" in ids
        # Zone 3 detectors not included
        assert "cross_table_consistency" not in ids

    def test_computation_review_collects_zone3_detectors(self) -> None:
        ids = _detector_ids_for_gate("computation_review")
        # Includes zone 1+2 detectors
        assert "type_fidelity" in ids
        assert "derived_value" in ids
        # Includes zone 3 detectors
        assert "cross_table_consistency" in ids
        assert "business_cycle_health" in ids

    def test_returns_list(self) -> None:
        ids = _detector_ids_for_gate("quality_review")
        assert isinstance(ids, list)
        # No duplicates
        assert len(ids) == len(set(ids))
