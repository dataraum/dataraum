"""Tests for PhaseResult.detail() and summary field."""

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


class TestPhaseResultDetail:
    def test_empty_outputs(self):
        """detail() returns empty string for no outputs."""
        result = PhaseResult.success()
        assert result.detail() == ""

    def test_scalar_values(self):
        """detail() renders scalar values as key: value."""
        result = PhaseResult.success(outputs={"count": 42, "name": "test"})
        detail = result.detail()
        assert "  count: 42" in detail
        assert "  name: test" in detail

    def test_list_values_short(self):
        """detail() renders short lists with items and preview."""
        result = PhaseResult.success(outputs={"tables": ["a", "b", "c"]})
        detail = result.detail()
        assert "  tables: 3 items" in detail
        assert "a, b, c" in detail

    def test_list_values_truncated(self):
        """detail() truncates lists longer than 5 items."""
        items = ["a", "b", "c", "d", "e", "f", "g"]
        result = PhaseResult.success(outputs={"items": items})
        detail = result.detail()
        assert "  items: 7 items" in detail
        assert "a, b, c, d, e, ..." in detail
        assert "f" not in detail.split("...")[0].split("e,")[1]

    def test_dict_values(self):
        """detail() renders dicts inline."""
        result = PhaseResult.success(outputs={"config": {"a": 1, "b": 2}})
        detail = result.detail()
        assert "  config: {'a': 1, 'b': 2}" in detail

    def test_empty_list(self):
        """detail() renders empty lists."""
        result = PhaseResult.success(outputs={"items": []})
        detail = result.detail()
        assert "  items: 0 items" in detail

    def test_multiline_output(self):
        """detail() returns newline-separated lines."""
        result = PhaseResult.success(
            outputs={"count": 10, "tables": ["orders", "products"]}
        )
        lines = result.detail().split("\n")
        assert len(lines) == 2
