"""Tests for entropy models."""

from dataraum.entropy.models import EntropyObject


class TestEntropyObject:
    """Tests for EntropyObject."""

    def test_dimension_path(self, sample_entropy_object: EntropyObject):
        """Test dimension path property."""
        assert sample_entropy_object.dimension_path == "structural.types.type_fidelity"
