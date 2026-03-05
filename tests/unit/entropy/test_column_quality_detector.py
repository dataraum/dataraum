"""Tests for ColumnQualityDetector."""

from unittest.mock import MagicMock

import pytest

from dataraum.entropy.detectors.base import DetectorContext
from dataraum.entropy.detectors.semantic.column_quality import ColumnQualityDetector


class TestColumnQualityDetector:
    @pytest.fixture
    def detector(self) -> ColumnQualityDetector:
        return ColumnQualityDetector()

    def test_detector_properties(self, detector: ColumnQualityDetector):
        assert detector.detector_id == "column_quality"
        assert detector.layer == "semantic"
        assert detector.dimension == "dimensional"
        assert detector.sub_dimension == "column_quality"
        assert detector.scope == "table"
        assert detector.required_analyses == ["column_quality_reports"]

    def test_returns_empty_when_no_data(self, detector: ColumnQualityDetector):
        context = DetectorContext(
            table_name="orders",
            analysis_results={},
        )
        assert detector.detect(context) == []

    def test_returns_empty_when_empty_reports(self, detector: ColumnQualityDetector):
        context = DetectorContext(
            table_name="orders",
            analysis_results={"column_quality_reports": {}},
        )
        assert detector.detect(context) == []

    def test_single_column_high_quality(self, detector: ColumnQualityDetector):
        context = DetectorContext(
            table_name="orders",
            analysis_results={
                "column_quality_reports": {
                    "amount": {
                        "column_id": "col1",
                        "table_id": "tbl1",
                        "table_name": "orders",
                        "avg_quality_score": 0.9,
                        "grades": ["A"],
                        "slices_analyzed": 1,
                        "key_findings": [],
                        "quality_issues": [],
                        "quality_issues_count": 0,
                        "recommendations": [],
                        "recommendations_count": 0,
                    }
                }
            },
        )

        results = detector.detect(context)

        assert len(results) == 1
        assert results[0].score == pytest.approx(0.1)
        assert results[0].target == "column:orders.amount"
        assert results[0].detector_id == "column_quality"
        assert results[0].layer == "semantic"
        assert results[0].dimension == "dimensional"
        assert results[0].sub_dimension == "column_quality"

    def test_single_column_low_quality(self, detector: ColumnQualityDetector):
        context = DetectorContext(
            table_name="orders",
            analysis_results={
                "column_quality_reports": {
                    "status": {
                        "column_id": "col2",
                        "table_id": "tbl1",
                        "table_name": "orders",
                        "avg_quality_score": 0.3,
                        "grades": ["D"],
                        "slices_analyzed": 2,
                        "key_findings": ["High null ratio"],
                        "quality_issues": [{"type": "nulls"}],
                        "quality_issues_count": 1,
                        "recommendations": ["Investigate nulls"],
                        "recommendations_count": 1,
                    }
                }
            },
        )

        results = detector.detect(context)

        assert len(results) == 1
        assert results[0].score == pytest.approx(0.7)

    def test_multiple_columns(self, detector: ColumnQualityDetector):
        context = DetectorContext(
            table_name="orders",
            analysis_results={
                "column_quality_reports": {
                    "amount": {
                        "column_id": "col1",
                        "table_id": "tbl1",
                        "table_name": "orders",
                        "avg_quality_score": 0.95,
                        "grades": ["A"],
                        "slices_analyzed": 1,
                        "key_findings": [],
                        "quality_issues": [],
                        "quality_issues_count": 0,
                        "recommendations": [],
                        "recommendations_count": 0,
                    },
                    "status": {
                        "column_id": "col2",
                        "table_id": "tbl1",
                        "table_name": "orders",
                        "avg_quality_score": 0.5,
                        "grades": ["C"],
                        "slices_analyzed": 3,
                        "key_findings": ["Inconsistent values"],
                        "quality_issues": [{"type": "consistency"}],
                        "quality_issues_count": 1,
                        "recommendations": ["Standardize values"],
                        "recommendations_count": 1,
                    },
                }
            },
        )

        results = detector.detect(context)

        assert len(results) == 2
        targets = {r.target: r.score for r in results}
        assert targets["column:orders.amount"] == pytest.approx(0.05)
        assert targets["column:orders.status"] == pytest.approx(0.5)

    def test_evidence_structure(self, detector: ColumnQualityDetector):
        context = DetectorContext(
            table_name="orders",
            analysis_results={
                "column_quality_reports": {
                    "amount": {
                        "column_id": "col1",
                        "table_id": "tbl1",
                        "table_name": "orders",
                        "avg_quality_score": 0.8,
                        "grades": ["B", "A"],
                        "slices_analyzed": 2,
                        "key_findings": ["Finding 1"],
                        "quality_issues": [{"type": "outliers"}],
                        "quality_issues_count": 1,
                        "recommendations": ["Fix outliers"],
                        "recommendations_count": 1,
                    }
                }
            },
        )

        results = detector.detect(context)
        ev = results[0].evidence[0]

        assert ev["source"] == "column_quality_report"
        assert ev["column_id"] == "col1"
        assert ev["table_id"] == "tbl1"
        assert ev["slices_analyzed"] == 2
        assert ev["avg_quality_score"] == 0.8
        assert ev["grades"] == ["B", "A"]
        assert ev["key_findings"] == ["Finding 1"]
        assert ev["quality_issues_count"] == 1
        assert ev["recommendations_count"] == 1

    def test_resolution_options(self, detector: ColumnQualityDetector):
        context = DetectorContext(
            table_name="orders",
            analysis_results={
                "column_quality_reports": {
                    "amount": {
                        "column_id": "col1",
                        "table_id": "tbl1",
                        "table_name": "orders",
                        "avg_quality_score": 0.6,
                        "grades": ["C"],
                        "slices_analyzed": 1,
                        "key_findings": ["High nulls"],
                        "quality_issues": [{"type": "nulls"}, {"type": "outliers"}],
                        "quality_issues_count": 2,
                        "recommendations": ["Fix nulls", "Fix outliers"],
                        "recommendations_count": 2,
                    }
                }
            },
        )

        results = detector.detect(context)
        opts = results[0].resolution_options

        assert len(opts) == 1
        assert opts[0].action == "investigate_quality_issues"
        assert opts[0].effort == "medium"
        assert opts[0].parameters["column_name"] == "amount"
        assert len(opts[0].parameters["quality_issues"]) == 2
        assert "2 quality issues" in opts[0].description

    def test_slicing_view_target(self, detector: ColumnQualityDetector):
        """Column quality from slicing_view uses effective table name in target."""
        context = DetectorContext(
            table_name="orders",
            analysis_results={
                "column_quality_reports": {
                    "amount": {
                        "column_id": "sv_col1",
                        "table_id": "sv_tbl1",
                        "table_name": "slicing_orders",
                        "avg_quality_score": 0.7,
                        "grades": ["B"],
                        "slices_analyzed": 1,
                        "key_findings": [],
                        "quality_issues": [],
                        "quality_issues_count": 0,
                        "recommendations": [],
                        "recommendations_count": 0,
                    }
                }
            },
        )

        results = detector.detect(context)

        assert len(results) == 1
        assert results[0].target == "column:slicing_orders.amount"

    def test_can_run_requires_reports(self, detector: ColumnQualityDetector):
        context_without = DetectorContext(table_name="orders", analysis_results={})
        context_with = DetectorContext(
            table_name="orders",
            analysis_results={"column_quality_reports": {"a": {}}},
        )

        assert not detector.can_run(context_without)
        assert detector.can_run(context_with)


class TestLoadColumnQualityReports:
    """Tests for ColumnQualityDetector._load_column_quality_reports."""

    def test_returns_none_when_no_reports(self):
        session = MagicMock()

        cols_result = MagicMock()
        cols_result.scalars.return_value.all.return_value = []

        sv_result = MagicMock()
        sv_result.scalar_one_or_none.return_value = None

        reports_result = MagicMock()
        reports_result.scalars.return_value.all.return_value = []

        session.execute.side_effect = [cols_result, sv_result, reports_result]

        result = ColumnQualityDetector._load_column_quality_reports(session, "tbl1", "orders")
        assert result is None

    def test_returns_grouped_reports(self):
        session = MagicMock()

        col = MagicMock()
        col.column_id = "col1"
        col.column_name = "amount"

        cols_result = MagicMock()
        cols_result.scalars.return_value.all.return_value = [col]

        sv_result = MagicMock()
        sv_result.scalar_one_or_none.return_value = None

        report = MagicMock()
        report.column_name = "amount"
        report.source_column_id = "col1"
        report.overall_quality_score = 0.85
        report.quality_grade = "B"
        report.report_data = {
            "key_findings": ["Finding 1"],
            "quality_issues": [{"type": "nulls"}],
            "recommendations": ["Fix nulls"],
        }

        reports_result = MagicMock()
        reports_result.scalars.return_value.all.return_value = [report]

        session.execute.side_effect = [cols_result, sv_result, reports_result]

        result = ColumnQualityDetector._load_column_quality_reports(session, "tbl1", "orders")
        assert result is not None
        assert "amount" in result
        assert result["amount"]["column_id"] == "col1"
        assert result["amount"]["table_id"] == "tbl1"
        assert result["amount"]["table_name"] == "orders"
        assert result["amount"]["avg_quality_score"] == 0.85
        assert result["amount"]["grades"] == ["B"]
        assert result["amount"]["slices_analyzed"] == 1
        assert result["amount"]["key_findings"] == ["Finding 1"]
        assert result["amount"]["quality_issues_count"] == 1
        assert result["amount"]["recommendations_count"] == 1

    def test_skips_columns_not_in_typed_table(self):
        session = MagicMock()

        cols_result = MagicMock()
        cols_result.scalars.return_value.all.return_value = []

        sv_result = MagicMock()
        sv_result.scalar_one_or_none.return_value = None

        report = MagicMock()
        report.column_name = "unknown_col"
        report.source_column_id = "col_x"
        report.overall_quality_score = 0.5
        report.quality_grade = "C"
        report.report_data = {}

        reports_result = MagicMock()
        reports_result.scalars.return_value.all.return_value = [report]

        session.execute.side_effect = [cols_result, sv_result, reports_result]

        result = ColumnQualityDetector._load_column_quality_reports(session, "tbl1", "orders")
        assert result is None
