"""Tests for entropy detector loader helpers."""

from unittest.mock import MagicMock

from dataraum.entropy.detectors.loaders import (
    load_correlation,
    load_drift_summaries,
    load_relationships,
    load_semantic,
    load_slice_variance,
    load_statistics,
    load_typing,
)


class TestLoadTyping:
    def test_returns_none_when_no_data(self):
        session = MagicMock()
        session.execute.return_value.scalar_one_or_none.return_value = None
        assert load_typing(session, "col1") is None

    def test_returns_decision_with_candidate(self):
        session = MagicMock()
        td = MagicMock()
        td.decided_type = "DECIMAL"
        td.decision_source = "inference"
        td.decision_reason = "high confidence"

        tc = MagicMock()
        tc.confidence = 0.95
        tc.parse_success_rate = 0.99
        tc.failed_examples = ["N/A"]
        tc.detected_pattern = r"\d+\.\d+"
        tc.pattern_match_rate = 0.98
        tc.detected_unit = "USD"
        tc.unit_confidence = 0.8

        session.execute.return_value.scalar_one_or_none.side_effect = [td, tc]

        result = load_typing(session, "col1")
        assert result is not None
        assert result["resolved_type"] == "DECIMAL"
        assert result["confidence"] == 0.95
        assert result["detected_unit"] == "USD"

    def test_returns_candidate_only_when_no_decision(self):
        session = MagicMock()
        tc = MagicMock()
        tc.data_type = "INTEGER"
        tc.confidence = 0.8
        tc.parse_success_rate = 1.0
        tc.failed_examples = []
        tc.detected_pattern = None
        tc.pattern_match_rate = None
        tc.detected_unit = None
        tc.unit_confidence = None

        session.execute.return_value.scalar_one_or_none.side_effect = [None, tc]

        result = load_typing(session, "col1")
        assert result is not None
        assert result["data_type"] == "INTEGER"
        assert result["confidence"] == 0.8


class TestLoadStatistics:
    def test_returns_none_when_no_profile(self):
        session = MagicMock()
        session.execute.return_value.scalar_one_or_none.return_value = None
        assert load_statistics(session, "col1") is None

    def test_returns_stats_with_quality(self):
        session = MagicMock()
        sp = MagicMock()
        sp.null_count = 5
        sp.total_count = 100
        sp.distinct_count = 80
        sp.cardinality_ratio = 0.8
        sp.profile_data = {"numeric_stats": {}}

        qm = MagicMock()
        qm.iqr_outlier_ratio = 0.02
        qm.zscore_outlier_ratio = 0.01
        qm.has_outliers = True
        qm.benford_compliant = True
        qm.quality_data = {"outlier_detection": {"iqr_outlier_count": 2}}

        session.execute.return_value.scalar_one_or_none.side_effect = [sp, qm]

        result = load_statistics(session, "col1")
        assert result is not None
        assert result["null_ratio"] == 0.05
        assert result["quality"]["benford_compliant"] is True


class TestLoadSemantic:
    def test_returns_none_when_no_annotation(self):
        session = MagicMock()
        session.execute.return_value.scalar_one_or_none.return_value = None
        assert load_semantic(session, "col1") is None

    def test_returns_semantic_dict(self):
        session = MagicMock()
        sa = MagicMock()
        sa.semantic_role = "measure"
        sa.entity_type = "monetary"
        sa.business_name = "Revenue"
        sa.business_description = "Total revenue"
        sa.confidence = 0.9
        sa.business_concept = "revenue"
        sa.unit_source_column = None

        session.execute.return_value.scalar_one_or_none.return_value = sa

        result = load_semantic(session, "col1")
        assert result is not None
        assert result["semantic_role"] == "measure"
        assert "unit_source_column" not in result

    def test_includes_unit_source_column(self):
        session = MagicMock()
        sa = MagicMock()
        sa.semantic_role = "measure"
        sa.entity_type = "monetary"
        sa.business_name = "Amount"
        sa.business_description = ""
        sa.confidence = 0.7
        sa.business_concept = None
        sa.unit_source_column = "currency"

        session.execute.return_value.scalar_one_or_none.return_value = sa

        result = load_semantic(session, "col1")
        assert result is not None
        assert result["unit_source_column"] == "currency"


class TestLoadRelationships:
    def test_returns_none_when_no_column(self):
        session = MagicMock()
        session.execute.return_value.scalar_one_or_none.return_value = None
        assert load_relationships(session, "col1", "tbl1") is None

    def test_returns_none_when_no_rels(self):
        session = MagicMock()
        col = MagicMock()
        # First call: Column lookup; Second call: Relationship query
        session.execute.return_value.scalar_one_or_none.return_value = col
        session.execute.return_value.scalars.return_value.all.return_value = []

        # Need to differentiate calls — use side_effect
        col_result = MagicMock()
        col_result.scalar_one_or_none.return_value = col

        rel_result = MagicMock()
        rel_result.scalars.return_value.all.return_value = []

        session.execute.side_effect = [col_result, rel_result]

        assert load_relationships(session, "col1", "tbl1") is None

    def test_returns_relationship_dicts(self):
        session = MagicMock()
        col = MagicMock()

        rel = MagicMock()
        rel.relationship_type = "foreign_key"
        rel.confidence = 0.95
        rel.detection_method = "llm"
        rel.from_table_id = "tbl1"
        rel.to_table_id = "tbl2"
        rel.cardinality = "many_to_one"
        rel.is_confirmed = True
        rel.evidence = {"method": "llm"}

        tbl1 = MagicMock()
        tbl1.table_id = "tbl1"
        tbl1.table_name = "orders"
        tbl2 = MagicMock()
        tbl2.table_id = "tbl2"
        tbl2.table_name = "customers"

        col_result = MagicMock()
        col_result.scalar_one_or_none.return_value = col

        rel_result = MagicMock()
        rel_result.scalars.return_value.all.return_value = [rel]

        table_result = MagicMock()
        table_result.scalars.return_value.all.return_value = [tbl1, tbl2]

        session.execute.side_effect = [col_result, rel_result, table_result]

        result = load_relationships(session, "col1", "tbl1")
        assert result is not None
        assert len(result) == 1
        assert result[0]["from_table"] == "orders"
        assert result[0]["to_table"] == "customers"


class TestLoadCorrelation:
    def test_returns_none_when_no_derived_columns(self):
        session = MagicMock()
        session.execute.return_value.scalars.return_value.all.return_value = []
        assert load_correlation(session, "col1", "amount") is None

    def test_returns_derived_column_info(self):
        session = MagicMock()
        dc = MagicMock()
        dc.formula = "price * quantity"
        dc.match_rate = 0.98
        dc.derivation_type = "multiplication"
        dc.source_column_ids = ["col2", "col3"]

        session.execute.return_value.scalars.return_value.all.return_value = [dc]

        result = load_correlation(session, "col1", "total")
        assert result is not None
        assert len(result["derived_columns"]) == 1
        assert result["derived_columns"][0]["derived_column_name"] == "total"
        assert result["derived_columns"][0]["match_rate"] == 0.98


class TestLoadDriftSummaries:
    def test_returns_none_when_no_column(self):
        session = MagicMock()
        session.execute.return_value.scalar_one_or_none.return_value = None
        assert load_drift_summaries(session, "col1", "tbl1", "orders") is None

    def test_returns_none_when_no_slice_tables(self):
        session = MagicMock()
        col = MagicMock()
        col.column_name = "amount"

        col_result = MagicMock()
        col_result.scalar_one_or_none.return_value = col

        slice_result = MagicMock()
        slice_result.scalars.return_value.all.return_value = []

        cols_result = MagicMock()
        cols_result.scalars.return_value.all.return_value = []

        session.execute.side_effect = [col_result, slice_result, cols_result]

        assert load_drift_summaries(session, "col1", "tbl1", "orders") is None


class TestLoadSliceVariance:
    def test_returns_none_when_no_profiles(self):
        session = MagicMock()

        # table_columns query
        cols_result = MagicMock()
        cols_result.scalars.return_value.all.return_value = []

        # slicing_view query
        sv_result = MagicMock()
        sv_result.scalar_one_or_none.return_value = None

        # profiles query
        profiles_result = MagicMock()
        profiles_result.scalars.return_value.all.return_value = []

        session.execute.side_effect = [cols_result, sv_result, profiles_result]

        assert load_slice_variance(session, "tbl1", "orders") is None

    def test_returns_slice_variance_data(self):
        session = MagicMock()

        col = MagicMock()
        col.column_id = "col1"
        col.column_name = "amount"

        # table_columns query
        cols_result = MagicMock()
        cols_result.scalars.return_value.all.return_value = [col]

        # slicing_view query
        sv_result = MagicMock()
        sv_result.scalar_one_or_none.return_value = None

        # profiles query
        profile = MagicMock()
        profile.slice_value = "2024-Q1"
        profile.column_name = "amount"
        profile.null_ratio = 0.05
        profile.distinct_count = 50
        profile.row_count = 1000
        profile.quality_score = 0.9
        profile.has_issues = False
        profile.variance_classification = "stable"
        profile.source_column_id = "col1"

        profiles_result = MagicMock()
        profiles_result.scalars.return_value.all.return_value = [profile]

        # slice_defs query
        slice_defs_result = MagicMock()
        slice_defs_result.scalars.return_value.all.return_value = []

        session.execute.side_effect = [
            cols_result,
            sv_result,
            profiles_result,
            slice_defs_result,
        ]

        result = load_slice_variance(session, "tbl1", "orders")
        assert result is not None
        assert "slice_variance" in result
        assert "amount" in result["slice_variance"]["columns"]
