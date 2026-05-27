"""Tests for YAML-driven pipeline configuration."""

from __future__ import annotations

import pytest

from dataraum.pipeline.pipeline_config import load_phase_declarations


class TestLoadPhaseDeclarations:
    """Tests for loading and parsing pipeline.yaml."""

    def test_loads_all_phases(self):
        declarations = load_phase_declarations()
        # Should have all active phases
        assert "import" in declarations
        assert "typing" in declarations
        assert "semantic_per_column" in declarations
        assert "semantic_per_table" in declarations

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
