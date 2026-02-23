"""Tests for bridge module (EntropyObject <-> network evidence)."""


from dataraum.entropy.models import EntropyObject
from dataraum.entropy.network.bridge import (
    build_dimension_path_to_node_map,
    discretize_score,
    entropy_objects_to_evidence,
)
from dataraum.entropy.network.model import EntropyNetwork


class TestDiscretizeScore:
    """Test continuous-to-discrete score conversion."""

    def test_zero_is_low(self):
        assert discretize_score(0.0) == "low"

    def test_at_low_boundary_is_low(self):
        assert discretize_score(0.3) == "low"

    def test_just_above_low_is_medium(self):
        assert discretize_score(0.31) == "medium"

    def test_at_medium_boundary_is_medium(self):
        assert discretize_score(0.6) == "medium"

    def test_just_above_medium_is_high(self):
        assert discretize_score(0.61) == "high"

    def test_max_is_high(self):
        assert discretize_score(1.0) == "high"

    def test_custom_thresholds(self):
        assert discretize_score(0.2, low_upper=0.25, medium_upper=0.5) == "low"
        assert discretize_score(0.3, low_upper=0.25, medium_upper=0.5) == "medium"
        assert discretize_score(0.6, low_upper=0.25, medium_upper=0.5) == "high"


class TestBuildDimensionPathToNodeMap:
    """Test dimension path mapping."""

    def test_all_nodes_have_paths(self, full_network: EntropyNetwork):
        path_map = build_dimension_path_to_node_map(full_network)
        assert len(path_map) == 15

    def test_correct_mapping(self, full_network: EntropyNetwork):
        path_map = build_dimension_path_to_node_map(full_network)
        assert path_map["structural.types.type_fidelity"] == "type_fidelity"
        assert path_map["value.nulls.null_ratio"] == "null_ratio"
        assert path_map["intent.query.readiness"] == "query_intent"

    def test_all_values_are_valid_node_names(self, full_network: EntropyNetwork):
        path_map = build_dimension_path_to_node_map(full_network)
        node_names = set(full_network.node_names)
        for path, name in path_map.items():
            assert name in node_names, f"Path {path} maps to unknown node {name}"


class TestEntropyObjectsToEvidence:
    """Test converting EntropyObjects to network evidence."""

    def test_maps_known_objects(self, full_network: EntropyNetwork):
        objects = [
            EntropyObject(
                layer="structural", dimension="types",
                sub_dimension="type_fidelity", score=0.8,
            ),
            EntropyObject(
                layer="value", dimension="nulls",
                sub_dimension="null_ratio", score=0.1,
            ),
        ]
        evidence = entropy_objects_to_evidence(objects, full_network)
        assert evidence["type_fidelity"] == "high"
        assert evidence["null_ratio"] == "low"

    def test_unmapped_objects_skipped(self, full_network: EntropyNetwork):
        objects = [
            EntropyObject(
                layer="unknown", dimension="unknown",
                sub_dimension="unknown", score=0.5,
            ),
        ]
        evidence = entropy_objects_to_evidence(objects, full_network)
        assert evidence == {}

    def test_empty_input(self, full_network: EntropyNetwork):
        evidence = entropy_objects_to_evidence([], full_network)
        assert evidence == {}

    def test_uses_config_discretization_thresholds(self, full_network: EntropyNetwork):
        """Score right at the boundary should use config thresholds."""
        objects = [
            EntropyObject(
                layer="structural", dimension="types",
                sub_dimension="type_fidelity", score=0.3,  # At low_upper boundary
            ),
        ]
        evidence = entropy_objects_to_evidence(objects, full_network)
        assert evidence["type_fidelity"] == "low"

    def test_medium_score(self, full_network: EntropyNetwork):
        objects = [
            EntropyObject(
                layer="value", dimension="outliers",
                sub_dimension="outlier_rate", score=0.45,
            ),
        ]
        evidence = entropy_objects_to_evidence(objects, full_network)
        assert evidence["outlier_rate"] == "medium"
