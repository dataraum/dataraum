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
        # Should have resolution options for significant failure rate
        assert len(results[0].resolution_options) >= 1

    def test_resolution_options_at_high_entropy(self, detector: TypeFidelityDetector):
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

        results = detector.detect(context)

        # Should have override_type and quarantine_values options
        actions = [opt.action for opt in results[0].resolution_options]
        assert "document_type_override" in actions
        assert "transform_quarantine_values" in actions

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
    """Tests for JoinPathDeterminismDetector."""

    @pytest.fixture
    def detector(self) -> JoinPathDeterminismDetector:
        """Create detector instance."""
        return JoinPathDeterminismDetector()

    def test_single_path(self, detector: JoinPathDeterminismDetector):
        """Test low entropy for single join path."""
        context = DetectorContext(
            table_name="orders",
            column_name="customer_id",
            analysis_results={
                "relationships": [
                    {"from_table": "orders", "to_table": "customers"},
                ]
            },
        )

        results = detector.detect(context)

        assert len(results) == 1
        assert results[0].score == pytest.approx(0.1, abs=0.01)
        assert results[0].evidence[0]["path_status"] == "deterministic"

    def test_no_paths_orphan(self, detector: JoinPathDeterminismDetector):
        """Test high entropy for orphan table with no paths."""
        context = DetectorContext(
            table_name="isolated",
            column_name="id",
            analysis_results={"relationships": []},
        )

        results = detector.detect(context)

        assert len(results) == 1
        assert results[0].score == pytest.approx(0.9, abs=0.01)
        assert results[0].evidence[0]["path_status"] == "orphan"
        # Should suggest declaring relationship
        actions = [opt.action for opt in results[0].resolution_options]
        assert "confirm_relationship" in actions

    def test_star_schema_deterministic(self, detector: JoinPathDeterminismDetector):
        """Test LOW entropy for star schema (multiple paths to DIFFERENT tables)."""
        # Fact table connecting to multiple dimension tables = deterministic, not ambiguous
        context = DetectorContext(
            table_name="transactions",
            column_name="id",
            analysis_results={
                "relationships": [
                    {"from_table": "transactions", "to_table": "orders"},
                    {"from_table": "transactions", "to_table": "customers"},
                    {"from_table": "transactions", "to_table": "products"},
                    {"from_table": "payments", "to_table": "transactions"},
                ]
            },
        )

        results = detector.detect(context)

        assert len(results) == 1
        # Star schema = low entropy (each target table has one path)
        assert results[0].score == pytest.approx(0.1, abs=0.01)
        assert results[0].evidence[0]["path_status"] == "deterministic"
        assert results[0].evidence[0]["connected_tables"] == 4

    def test_ambiguous_multiple_paths_same_table(self, detector: JoinPathDeterminismDetector):
        """Test HIGH entropy for multiple paths to SAME table (ambiguous)."""
        # Two different ways to join orders -> customers = ambiguous
        context = DetectorContext(
            table_name="orders",
            column_name="id",
            analysis_results={
                "relationships": [
                    {"from_table": "orders", "to_table": "customers"},  # via customer_id
                    {"from_table": "orders", "to_table": "customers"},  # via billing_customer_id
                ]
            },
        )

        results = detector.detect(context)

        assert len(results) == 1
        assert results[0].score == pytest.approx(0.7, abs=0.01)
        assert results[0].evidence[0]["path_status"] == "ambiguous"
        assert "customers" in results[0].evidence[0]["ambiguous_tables"]
        # Should suggest preferred path
        actions = [opt.action for opt in results[0].resolution_options]
        assert "resolve_join_ambiguity" in actions

    def test_mixed_deterministic_and_ambiguous(self, detector: JoinPathDeterminismDetector):
        """Test proportional entropy when some tables have multiple paths."""
        context = DetectorContext(
            table_name="orders",
            column_name="id",
            analysis_results={
                "relationships": [
                    {"from_table": "orders", "to_table": "customers"},
                    {"from_table": "orders", "to_table": "customers"},  # Ambiguous!
                    {"from_table": "orders", "to_table": "products"},  # Single path OK
                ]
            },
        )

        results = detector.detect(context)

        assert len(results) == 1
        # 1 ambiguous out of 2 tables → 0.1 + 0.6 * 0.5 = 0.4
        assert results[0].score == pytest.approx(0.4, abs=0.01)
        assert results[0].evidence[0]["path_status"] == "ambiguous"

    def test_proportional_ambiguity_scoring(self, detector: JoinPathDeterminismDetector):
        """Test proportional scoring based on ambiguity ratio."""
        # 1 ambiguous out of 5 tables → 0.1 + 0.6 * 0.2 = 0.22
        context = DetectorContext(
            table_name="fact",
            column_name="id",
            analysis_results={
                "relationships": [
                    {"from_table": "fact", "to_table": "dim_a"},
                    {"from_table": "fact", "to_table": "dim_a"},  # Ambiguous
                    {"from_table": "fact", "to_table": "dim_b"},
                    {"from_table": "fact", "to_table": "dim_c"},
                    {"from_table": "fact", "to_table": "dim_d"},
                    {"from_table": "fact", "to_table": "dim_e"},
                ]
            },
        )
        results = detector.detect(context)
        assert results[0].score == pytest.approx(0.22, abs=0.01)

    def test_proportional_high_ambiguity(self, detector: JoinPathDeterminismDetector):
        """Test proportional scoring with high ambiguity."""
        # 3 ambiguous out of 5 tables → 0.1 + 0.6 * 0.6 = 0.46
        context = DetectorContext(
            table_name="fact",
            column_name="id",
            analysis_results={
                "relationships": [
                    {"from_table": "fact", "to_table": "dim_a"},
                    {"from_table": "fact", "to_table": "dim_a"},  # Ambiguous
                    {"from_table": "fact", "to_table": "dim_b"},
                    {"from_table": "fact", "to_table": "dim_b"},  # Ambiguous
                    {"from_table": "fact", "to_table": "dim_c"},
                    {"from_table": "fact", "to_table": "dim_c"},  # Ambiguous
                    {"from_table": "fact", "to_table": "dim_d"},
                    {"from_table": "fact", "to_table": "dim_e"},
                ]
            },
        )
        results = detector.detect(context)
        assert results[0].score == pytest.approx(0.46, abs=0.01)

    def test_full_ambiguity_equals_max(self, detector: JoinPathDeterminismDetector):
        """Test all tables ambiguous produces maximum ambiguity score."""
        # All tables ambiguous → 0.1 + 0.6 * 1.0 = 0.7
        context = DetectorContext(
            table_name="fact",
            column_name="id",
            analysis_results={
                "relationships": [
                    {"from_table": "fact", "to_table": "dim_a"},
                    {"from_table": "fact", "to_table": "dim_a"},
                    {"from_table": "fact", "to_table": "dim_b"},
                    {"from_table": "fact", "to_table": "dim_b"},
                ]
            },
        )
        results = detector.detect(context)
        assert results[0].score == pytest.approx(0.7, abs=0.01)

    def test_detector_properties(self, detector: JoinPathDeterminismDetector):
        """Test detector has correct properties."""
        assert detector.detector_id == "join_path_determinism"
        assert detector.layer == "structural"
        assert detector.dimension == "relations"
        assert detector.required_analyses == ["relationships"]


class TestRelationshipEntropyDetector:
    """Tests for RelationshipEntropyDetector orphan fallback formula."""

    @pytest.fixture
    def detector(self) -> RelationshipEntropyDetector:
        """Create detector instance."""
        return RelationshipEntropyDetector()

    def test_ri_from_left_referential_integrity(self, detector: RelationshipEntropyDetector):
        """Test RI entropy computed from left_referential_integrity percentage."""
        context = DetectorContext(
            table_name="orders",
            column_name="customer_id",
            analysis_results={
                "relationships": [
                    {
                        "from_table": "orders",
                        "to_table": "customers",
                        "relationship_type": "foreign_key",
                        "is_confirmed": True,
                        "confidence": 0.9,
                        "cardinality": "many-to-one",
                        "evidence": {
                            "left_referential_integrity": 95.0,
                            "orphan_count": 50,
                            "left_total_count": 1000,
                            "cardinality_verified": True,
                        },
                    }
                ]
            },
        )

        results = detector.detect(context)

        assert len(results) == 1
        # RI entropy from left_referential_integrity: 1.0 - 95/100 = 0.05
        ri_entropy = results[0].evidence[0]["ri_entropy"]
        assert ri_entropy == pytest.approx(0.05, abs=0.01)

    def test_orphan_with_total_uses_ratio(self, detector: RelationshipEntropyDetector):
        """Test orphan count with total_count uses ratio-based formula."""
        context = DetectorContext(
            table_name="orders",
            column_name="customer_id",
            analysis_results={
                "relationships": [
                    {
                        "from_table": "orders",
                        "to_table": "customers",
                        "relationship_type": "foreign_key",
                        "is_confirmed": True,
                        "confidence": 0.9,
                        "cardinality": "many-to-one",
                        "evidence": {
                            # No left_referential_integrity — triggers fallback
                            "orphan_count": 50,
                            "left_total_count": 1000,
                            "cardinality_verified": True,
                        },
                    }
                ]
            },
        )

        results = detector.detect(context)

        assert len(results) == 1
        # Ratio-based: 50/1000 = 0.05
        ri_entropy = results[0].evidence[0]["ri_entropy"]
        assert ri_entropy == pytest.approx(0.05, abs=0.01)

    def test_orphan_without_total_uses_count_formula(self, detector: RelationshipEntropyDetector):
        """Test orphan count without total falls back to count-based formula."""
        context = DetectorContext(
            table_name="orders",
            column_name="customer_id",
            analysis_results={
                "relationships": [
                    {
                        "from_table": "orders",
                        "to_table": "customers",
                        "relationship_type": "foreign_key",
                        "is_confirmed": True,
                        "confidence": 0.9,
                        "cardinality": "many-to-one",
                        "evidence": {
                            # No left_referential_integrity and no total_count
                            "orphan_count": 50,
                            "cardinality_verified": True,
                        },
                    }
                ]
            },
        )

        results = detector.detect(context)

        assert len(results) == 1
        # Count-based fallback: 0.3 + 50/1000 = 0.35
        ri_entropy = results[0].evidence[0]["ri_entropy"]
        assert ri_entropy == pytest.approx(0.35, abs=0.01)

    def test_no_relationships_empty(self, detector: RelationshipEntropyDetector):
        """Test empty result when no relationships exist."""
        context = DetectorContext(
            table_name="orders",
            column_name="customer_id",
            analysis_results={"relationships": []},
        )

        results = detector.detect(context)
        assert results == []

    def test_detector_properties(self, detector: RelationshipEntropyDetector):
        """Test detector has correct properties."""
        assert detector.detector_id == "relationship_entropy"
        assert detector.layer == "structural"
        assert detector.dimension == "relations"
        assert detector.required_analyses == ["relationships"]
