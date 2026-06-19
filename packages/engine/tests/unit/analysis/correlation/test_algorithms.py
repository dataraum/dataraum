"""Tests for pure correlation algorithms."""

import numpy as np
import pytest

from dataraum.analysis.correlation.algorithms.categorical import (
    _classify_strength as classify_categorical_strength,
)
from dataraum.analysis.correlation.algorithms.categorical import (
    compute_cramers_v,
)
from dataraum.analysis.correlation.algorithms.multicollinearity import (
    compute_multicollinearity,
)


class TestClassifyCategoricalStrength:
    """Tests for categorical _classify_strength."""

    def test_strong(self):
        assert classify_categorical_strength(0.6) == "strong"

    def test_moderate(self):
        assert classify_categorical_strength(0.35) == "moderate"

    def test_weak(self):
        assert classify_categorical_strength(0.15) == "weak"

    def test_none(self):
        assert classify_categorical_strength(0.05) == "none"

    def test_boundary_values(self):
        assert classify_categorical_strength(0.5) == "strong"
        assert classify_categorical_strength(0.3) == "moderate"
        assert classify_categorical_strength(0.1) == "weak"
        assert classify_categorical_strength(0.09) == "none"


class TestComputeCramersV:
    """Tests for compute_cramers_v."""

    def test_perfect_association(self):
        # Perfect association: each row category maps to exactly one column
        table = np.array([[50, 0], [0, 50]])
        result = compute_cramers_v(table)

        assert result is not None
        assert result.cramers_v == pytest.approx(1.0, abs=0.05)
        assert result.is_significant

    def test_no_association(self):
        # No association: uniform distribution
        table = np.array([[25, 25], [25, 25]])
        result = compute_cramers_v(table)

        assert result is not None
        assert result.cramers_v < 0.1

    def test_insufficient_data(self):
        table = np.array([[1, 1], [1, 1]])
        result = compute_cramers_v(table)

        # Only 4 observations - should return None
        assert result is None

    def test_single_row_returns_none(self):
        table = np.array([[10, 20]])
        result = compute_cramers_v(table)

        assert result is None

    def test_preserves_column_indices(self):
        table = np.array([[30, 10], [10, 30]])
        result = compute_cramers_v(table, col1_idx=3, col2_idx=7)

        assert result is not None
        assert result.col1_idx == 3
        assert result.col2_idx == 7


class TestComputeMulticollinearity:
    """Tests for compute_multicollinearity."""

    def test_no_multicollinearity(self):
        # Identity matrix = no correlation between variables
        corr_matrix = np.eye(3)
        result = compute_multicollinearity(corr_matrix)

        assert result.overall_severity == "none"
        assert result.overall_condition_index < 10
        assert len(result.dependency_groups) == 0

    def test_perfect_multicollinearity(self):
        # Near-singular matrix = severe multicollinearity
        corr_matrix = np.array(
            [
                [1.0, 0.999, 0.0],
                [0.999, 1.0, 0.0],
                [0.0, 0.0, 1.0],
            ]
        )
        result = compute_multicollinearity(corr_matrix)

        assert result.overall_severity in ("moderate", "severe")
        assert result.overall_condition_index > 10

    def test_returns_eigenvalues(self):
        corr_matrix = np.eye(4)
        result = compute_multicollinearity(corr_matrix)

        assert len(result.eigenvalues) == 4
        # Identity matrix has all eigenvalues = 1
        for ev in result.eigenvalues:
            assert abs(ev - 1.0) < 0.01

    def test_dependency_group_has_at_least_two_variables(self):
        # Create matrix with two correlated variables
        corr_matrix = np.array(
            [
                [1.0, 0.999, 0.1],
                [0.999, 1.0, 0.1],
                [0.1, 0.1, 1.0],
            ]
        )
        result = compute_multicollinearity(corr_matrix)

        for group in result.dependency_groups:
            assert len(group.involved_col_indices) >= 2
