"""Tests for YAML-driven pipeline configuration."""

from __future__ import annotations

import pytest

from dataraum.entropy.dimensions import AnalysisKey
from dataraum.pipeline.pipeline_config import (
    PhaseDeclaration,
    get_all_dependencies_from_declarations,
    get_downstream_phases_from_declarations,
    load_phase_declarations,
)


class TestLoadPhaseDeclarations:
    """Tests for loading and parsing pipeline.yaml."""

    def test_loads_all_phases(self):
        declarations = load_phase_declarations()
        # Should have all active phases
        assert "import" in declarations
        assert "typing" in declarations
        assert "quality_review" in declarations
        assert "graph_execution" in declarations

    def test_import_has_no_dependencies(self):
        declarations = load_phase_declarations()
        assert declarations["import"].dependencies == []

    def test_typing_produces_typing(self):
        declarations = load_phase_declarations()
        assert declarations["typing"].produces == {AnalysisKey.TYPING}

    def test_quality_review_is_gate(self):
        declarations = load_phase_declarations()
        assert declarations["quality_review"].gate is True

    def test_non_gate_phase_is_not_gate(self):
        declarations = load_phase_declarations()
        assert declarations["typing"].gate is False

    def test_detectors_are_listed(self):
        declarations = load_phase_declarations()
        assert "type_fidelity" in declarations["typing"].detectors
        assert "null_ratio" in declarations["statistics"].detectors

    def test_phases_preserve_insertion_order(self):
        declarations = load_phase_declarations()
        names = list(declarations)
        assert names[0] == "import"
        assert names[1] == "typing"


class TestValidation:
    """Tests for YAML validation."""

    def test_rejects_flat_list_format(self):
        with pytest.raises(ValueError, match="must be a dict"):
            load_phase_declarations({"phases": ["import", "typing"]})

    def test_rejects_unknown_analysis_key(self):
        config = {
            "phases": {
                "test_phase": {
                    "description": "test",
                    "dependencies": [],
                    "produces": ["nonexistent_key"],
                }
            }
        }
        with pytest.raises(ValueError, match="unknown produces key"):
            load_phase_declarations(config)

    def test_rejects_unknown_dependency(self):
        config = {
            "phases": {
                "test_phase": {
                    "description": "test",
                    "dependencies": ["nonexistent_phase"],
                }
            }
        }
        with pytest.raises(ValueError, match="unknown dependencies"):
            load_phase_declarations(config)

    def test_rejects_dependency_cycle(self):
        config = {
            "phases": {
                "a": {"description": "a", "dependencies": ["b"]},
                "b": {"description": "b", "dependencies": ["a"]},
            }
        }
        with pytest.raises(ValueError, match="cycle"):
            load_phase_declarations(config)


class TestDependencyHelpers:
    """Tests for transitive dependency resolution from declarations."""

    def test_transitive_deps(self):
        declarations = {
            "a": PhaseDeclaration(name="a", description="", dependencies=[]),
            "b": PhaseDeclaration(name="b", description="", dependencies=["a"]),
            "c": PhaseDeclaration(name="c", description="", dependencies=["b"]),
        }
        assert get_all_dependencies_from_declarations("c", declarations) == {"a", "b"}

    def test_downstream(self):
        declarations = {
            "a": PhaseDeclaration(name="a", description="", dependencies=[]),
            "b": PhaseDeclaration(name="b", description="", dependencies=["a"]),
            "c": PhaseDeclaration(name="c", description="", dependencies=["b"]),
        }
        assert get_downstream_phases_from_declarations("a", declarations) == {"b", "c"}

    def test_unknown_phase_returns_empty(self):
        declarations = {
            "a": PhaseDeclaration(name="a", description="", dependencies=[]),
        }
        assert get_all_dependencies_from_declarations("z", declarations) == set()


class TestYAMLMatchesRegistry:
    """Every declared phase has a registered class, and vice versa."""

    def test_all_declared_phases_have_classes(self):
        from dataraum.pipeline.registry import get_registry

        declarations = load_phase_declarations()
        registry = get_registry()
        for name in declarations:
            assert name in registry, f"Phase {name!r} declared in YAML but not registered"

    def test_all_registered_phases_are_declared(self):
        from dataraum.pipeline.registry import get_registry

        declarations = load_phase_declarations()
        registry = get_registry()
        for name in registry:
            assert name in declarations, f"Phase {name!r} registered but not in YAML"
