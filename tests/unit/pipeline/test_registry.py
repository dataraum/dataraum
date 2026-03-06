"""Tests for pipeline phase registry."""

from __future__ import annotations

from dataraum.pipeline.registry import get_all_dependencies, get_downstream_phases


class TestGetDownstreamPhases:
    def test_semantic_has_downstream(self) -> None:
        """semantic phase has multiple downstream dependents."""
        downstream = get_downstream_phases("semantic")
        # These phases all transitively depend on semantic
        assert "enriched_views" in downstream
        assert "entropy" in downstream
        assert "entropy_interpretation" in downstream

    def test_leaf_phase_has_no_downstream(self) -> None:
        """A leaf phase (nothing depends on it) returns empty set."""
        downstream = get_downstream_phases("entropy_interpretation")
        assert downstream == set()

    def test_unknown_phase_returns_empty(self) -> None:
        """Unknown phase name returns empty set."""
        downstream = get_downstream_phases("nonexistent_phase")
        assert downstream == set()

    def test_downstream_does_not_include_self(self) -> None:
        """The phase itself is not in its downstream set."""
        downstream = get_downstream_phases("semantic")
        assert "semantic" not in downstream

    def test_downstream_consistent_with_dependencies(self) -> None:
        """If B is downstream of A, then A must be in B's transitive dependencies."""
        downstream = get_downstream_phases("semantic")
        for phase in downstream:
            deps = get_all_dependencies(phase)
            assert "semantic" in deps, f"{phase} is downstream but semantic not in its deps"
