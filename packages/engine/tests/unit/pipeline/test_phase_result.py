"""Tests for PhaseResult summary field."""

from __future__ import annotations

from dataraum.pipeline.base import PhaseResult, PhaseStatus


class TestPhaseResultSummary:
    def test_success_with_summary(self):
        """summary= kwarg is stored on the result."""
        result = PhaseResult.success(summary="3 tables typed")
        assert result.summary == "3 tables typed"
        assert result.status == PhaseStatus.COMPLETED

    def test_success_default_summary(self):
        """Default summary is empty string."""
        result = PhaseResult.success()
        assert result.summary == ""

    def test_failed_has_no_summary(self):
        """Failed results don't have a summary."""
        result = PhaseResult.failed("boom")
        assert result.summary == ""

    def test_skipped_has_no_summary(self):
        """Skipped results don't have a summary."""
        result = PhaseResult.skipped("not needed")
        assert result.summary == ""
