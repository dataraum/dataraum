"""Tests for entropy models."""

from dataraum.entropy.models import (
    EntropyObject,
    ResolutionOption,
)


class TestEntropyObject:
    """Tests for EntropyObject."""

    def test_dimension_path(self, sample_entropy_object: EntropyObject):
        """Test dimension path property."""
        assert sample_entropy_object.dimension_path == "structural.types.type_fidelity"


class TestResolutionOption:
    """Tests for ResolutionOption."""

    def test_basic_construction(self):
        """Test ResolutionOption can be constructed with required fields."""
        opt = ResolutionOption(
            action="declare_type",
            parameters={"column": "amount", "type": "DECIMAL"},
            effort="low",
            description="Declare explicit type",
        )
        assert opt.action == "declare_type"
        assert opt.parameters == {"column": "amount", "type": "DECIMAL"}
        assert opt.effort == "low"
        assert opt.description == "Declare explicit type"

    def test_default_description(self):
        """Test ResolutionOption defaults description to empty string."""
        opt = ResolutionOption(
            action="test",
            parameters={},
            effort="medium",
        )
        assert opt.description == ""
