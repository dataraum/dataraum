"""Tests for structural layer entropy detectors."""

import pytest

from dataraum.entropy.detectors import (
    DetectorContext,
    JoinPathDeterminismDetector,
    RelationshipEntropyDetector,
    TypeFidelityDetector,
)


class TestTypeFidelityDetector:
    """Tests for TypeFidelityDetector."""

    @pytest.fixture
    def detector(self) -> TypeFidelityDetector:
        """Create detector instance."""
        return TypeFidelityDetector()

    def test_perfect_parse_rate(self, detector: TypeFidelityDetector):
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

        results = detector.detect(context)

        assert len(results) == 1
        assert results[0].score == pytest.approx(0.0, abs=0.01)
        assert results[0].layer == "structural"
        assert results[0].dimension == "types"

    def test_low_parse_rate(self, detector: TypeFidelityDetector):
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

        results = detector.detect(context)

        assert len(results) == 1
        assert results[0].score == pytest.approx(0.4, abs=0.01)

    def test_evidence_includes_failure_samples(self, detector: TypeFidelityDetector):
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

        results = detector.detect(context)

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
    """Tests for JoinPathDeterminismDetector — relationship-scoped (DAT-408)."""

    @pytest.fixture
    def detector(self) -> JoinPathDeterminismDetector:
        """Create detector instance."""
        return JoinPathDeterminismDetector()

    def test_single_path_abstains(self, detector: JoinPathDeterminismDetector):
        """≤1 distinct path: the ambiguity question is unanswerable -> ABSTAIN.

        DAT-851: the loader excludes candidates and the LLM confirms ~one
        relationship per pair, so a single path is the structural norm — the old
        constant 0.1 here claimed "measured deterministic" for every
        relationship while the ambiguous branch was unreachable.
        """
        context = DetectorContext(
            from_table_name="orders",
            to_table_name="customers",
            from_column_id="c_fk",
            to_column_id="c_pk",
            analysis_results={
                "relationships": [
                    {
                        "from_table": "orders",
                        "to_table": "customers",
                        "from_column_id": "c_fk",
                        "to_column_id": "c_pk",
                    },
                ]
            },
        )

        results = detector.detect(context)

        assert len(results) == 1
        assert results[0].status == "abstained"
        assert results[0].abstain_reason == "not_applicable"
        assert results[0].score is None
        assert results[0].evidence[0]["path_status"] == "single_path"
        assert results[0].evidence[0]["distinct_join_paths"] == 1

    def test_two_paths_same_tables_ambiguous(self, detector: JoinPathDeterminismDetector):
        """Two distinct column-pairs between the SAME two tables -> ambiguous."""
        context = DetectorContext(
            from_table_name="orders",
            to_table_name="customers",
            from_column_id="c_fk1",
            to_column_id="c_pk",
            analysis_results={
                "relationships": [
                    {
                        "from_table": "orders",
                        "to_table": "customers",
                        "from_column_id": "c_fk1",
                        "to_column_id": "c_pk",
                    },
                    {
                        "from_table": "orders",
                        "to_table": "customers",
                        "from_column_id": "c_fk2",
                        "to_column_id": "c_pk",
                    },
                ]
            },
        )

        results = detector.detect(context)

        assert len(results) == 1
        assert results[0].status == "measured"
        assert results[0].score == pytest.approx(0.7, abs=0.01)
        assert results[0].evidence[0]["path_status"] == "ambiguous"
        assert results[0].evidence[0]["distinct_join_paths"] == 2

    def test_two_paths_resolved_by_teach_is_deterministic(
        self, detector: JoinPathDeterminismDetector, monkeypatch: pytest.MonkeyPatch
    ):
        """Genuine ambiguity resolved by a preferred-join teach -> measured 0.1."""
        from dataraum.analysis.relationships import utils as rel_utils

        monkeypatch.setattr(
            rel_utils,
            "load_confirmed_relationship_pairs",
            lambda session: {frozenset({"c_fk1", "c_pk"})},
        )
        context = DetectorContext(
            from_table_name="orders",
            to_table_name="customers",
            from_column_id="c_fk1",
            to_column_id="c_pk",
            # A session sentinel so the overlay lookup runs (monkeypatched).
            session=object(),  # type: ignore[arg-type]
            analysis_results={
                "relationships": [
                    {
                        "from_table": "orders",
                        "to_table": "customers",
                        "from_column_id": "c_fk1",
                        "to_column_id": "c_pk",
                    },
                    {
                        "from_table": "orders",
                        "to_table": "customers",
                        "from_column_id": "c_fk2",
                        "to_column_id": "c_pk",
                    },
                ]
            },
        )

        results = detector.detect(context)

        assert len(results) == 1
        assert results[0].status == "measured"
        assert results[0].score == pytest.approx(0.1, abs=0.01)
        assert results[0].evidence[0]["path_status"] == "resolved"
        assert results[0].evidence[0]["resolved_by_overlay"] is True

    def test_paths_to_other_tables_dont_add_ambiguity(self, detector: JoinPathDeterminismDetector):
        """A relationship to a DIFFERENT table is not ambiguity for this pair (star
        schema) — the focal pair still has one path, so the detector abstains."""
        context = DetectorContext(
            from_table_name="orders",
            to_table_name="customers",
            from_column_id="c_fk",
            to_column_id="c_pk",
            analysis_results={
                "relationships": [
                    {
                        "from_table": "orders",
                        "to_table": "customers",
                        "from_column_id": "c_fk",
                        "to_column_id": "c_pk",
                    },
                    {
                        "from_table": "orders",
                        "to_table": "products",
                        "from_column_id": "c_pfk",
                        "to_column_id": "c_ppk",
                    },
                ]
            },
        )

        results = detector.detect(context)

        assert results[0].status == "abstained"
        assert results[0].abstain_reason == "not_applicable"
        assert results[0].evidence[0]["distinct_join_paths"] == 1

    def test_missing_endpoints_abstains(self, detector: JoinPathDeterminismDetector):
        """No focal endpoints -> an upstream gap, traced as missing_inputs (DAT-853)."""
        context = DetectorContext(analysis_results={"relationships": []})
        results = detector.detect(context)
        assert len(results) == 1
        assert results[0].status == "abstained"
        assert results[0].abstain_reason == "missing_inputs"

    def test_detector_properties(self, detector: JoinPathDeterminismDetector):
        """Test detector has correct properties."""
        assert detector.detector_id == "join_path_determinism"
        assert detector.layer == "structural"
        assert detector.dimension == "relations"
        assert detector.scope == "relationship"
        assert detector.required_analyses == ["relationships"]


class TestRelationshipEntropyDetector:
    """Tests for RelationshipEntropyDetector — relationship-scoped (DAT-408)."""

    @pytest.fixture
    def detector(self) -> RelationshipEntropyDetector:
        """Create detector instance."""
        return RelationshipEntropyDetector()

    def _context(self, evidence: dict) -> DetectorContext:
        """A relationship-scoped context for the focal pair (no session -> overlay
        confirmation is False)."""
        return DetectorContext(
            from_table_name="orders",
            to_table_name="customers",
            from_column_id="c_fk",
            to_column_id="c_pk",
            analysis_results={
                # Key MUST match the detector's required_analyses (AnalysisKey.RELATIONSHIPS):
                # can_run() gates on it and detect() reads it. The old singular "relationship"
                # key left can_run() False in production -> the detector was silently skipped
                # (zero recall), which these detect()-only tests couldn't see (DAT-405).
                "relationships": {
                    "from_table": "orders",
                    "to_table": "customers",
                    "relationship_type": "foreign_key",
                    "confidence": 0.9,
                    "cardinality": "many-to-one",
                    "evidence": evidence,
                }
            },
        )

    def test_ri_from_left_referential_integrity(self, detector: RelationshipEntropyDetector):
        """RI entropy computed from left_referential_integrity percentage."""
        context = self._context(
            {
                "left_referential_integrity": 95.0,
                "left_orphan_count": 50,
                "left_total_count": 1000,
                "cardinality_verified": True,
            }
        )
        results = detector.detect(context)
        assert len(results) == 1
        # honest orphan rate: 1.0 - 95/100 = 0.05 (no sqrt boost, DAT-442)
        assert results[0].evidence[0]["ri_entropy"] == pytest.approx(0.05, abs=0.01)

    def test_orphan_with_total_uses_ratio(self, detector: RelationshipEntropyDetector):
        """Orphan count with total_count uses the ratio formula."""
        context = self._context(
            {
                "left_orphan_count": 50,
                "left_total_count": 1000,
                "cardinality_verified": True,
            }
        )
        results = detector.detect(context)
        assert len(results) == 1
        # honest orphan rate: 50/1000 = 0.05 (no sqrt boost, DAT-442)
        assert results[0].evidence[0]["ri_entropy"] == pytest.approx(0.05, abs=0.01)

    def test_orphan_count_without_total_is_not_scored(self, detector: RelationshipEntropyDetector):
        """Orphan count with no denominator → no rate → ignorance, not a fabricated score.

        The old `0.3 + orphan/1000` count fallback was theater (DAT-442 two-table); with
        no total_count and no left_referential_integrity there is nothing to measure.
        """
        context = self._context({"left_orphan_count": 50, "cardinality_verified": True})
        assert detector.detect(context) == []

    def test_no_relationship_empty(self, detector: RelationshipEntropyDetector):
        """No focal relationship in context -> empty result."""
        context = DetectorContext(
            from_table_name="orders",
            to_table_name="customers",
            from_column_id="c_fk",
            to_column_id="c_pk",
            analysis_results={},
        )
        assert detector.detect(context) == []

    def test_detector_properties(self, detector: RelationshipEntropyDetector):
        """Test detector has correct properties."""
        assert detector.detector_id == "relationship_entropy"
        assert detector.layer == "structural"
        assert detector.dimension == "relations"
        assert detector.scope == "relationship"
        assert detector.required_analyses == ["relationships"]
