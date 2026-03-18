"""Tests for fixes API module."""

from __future__ import annotations

from dataraum.entropy.dimensions import AnalysisKey
from dataraum.pipeline.fixes.api import _analyses_for_gate


class TestAnalysesForGate:
    """Test gate → analysis key mapping."""

    def test_quality_review_returns_zone1(self) -> None:
        keys = _analyses_for_gate("quality_review")
        assert AnalysisKey.TYPING in keys
        assert AnalysisKey.STATISTICS in keys
        assert AnalysisKey.SEMANTIC in keys
        assert AnalysisKey.CORRELATION not in keys

    def test_analysis_review_returns_zone2(self) -> None:
        keys = _analyses_for_gate("analysis_review")
        assert AnalysisKey.TYPING in keys
        assert AnalysisKey.CORRELATION in keys
        assert AnalysisKey.DRIFT_SUMMARIES in keys
        assert AnalysisKey.VALIDATION not in keys

    def test_computation_review_returns_zone3(self) -> None:
        keys = _analyses_for_gate("computation_review")
        assert AnalysisKey.TYPING in keys
        assert AnalysisKey.CORRELATION in keys
        assert AnalysisKey.VALIDATION in keys
        assert AnalysisKey.BUSINESS_CYCLES in keys

    def test_unknown_gate_returns_zone1(self) -> None:
        keys = _analyses_for_gate("unknown")
        assert keys == _analyses_for_gate("quality_review")
