"""Tests for fixes API module."""

from __future__ import annotations

from dataraum.pipeline.fixes.api import _detector_ids_for_gate


class TestDetectorIdsForGate:
    """Test phase → detector IDs collection from YAML declarations."""

    def test_typing_phase_detectors(self) -> None:
        ids = _detector_ids_for_gate("typing")
        assert "type_fidelity" in ids
        # Only typing's detector, not downstream
        assert "null_ratio" not in ids

    def test_semantic_collects_upstream_detectors(self) -> None:
        ids = _detector_ids_for_gate("semantic")
        # Includes typing + statistics + semantic detectors
        assert "type_fidelity" in ids
        assert "null_ratio" in ids
        assert "business_meaning" in ids

    def test_validation_collects_all_upstream(self) -> None:
        ids = _detector_ids_for_gate("validation")
        # Includes zone 1 detectors
        assert "type_fidelity" in ids
        assert "null_ratio" in ids
        # Includes semantic detectors
        assert "business_meaning" in ids
        # Includes zone 2 detectors
        assert "dimension_coverage" in ids

    def test_returns_list_no_duplicates(self) -> None:
        ids = _detector_ids_for_gate("semantic")
        assert isinstance(ids, list)
        assert len(ids) == len(set(ids))
