"""Tests for pure correlation algorithms."""

import numpy as np
import pytest

from dataraum.analysis.correlation.algorithms.categorical import (
    _classify_strength as classify_categorical_strength,
)
from dataraum.analysis.correlation.algorithms.categorical import (
    build_contingency_table,
    compute_cramers_v,
)
from dataraum.analysis.correlation.algorithms.multicollinearity import (
    compute_multicollinearity,
)
from dataraum.analysis.correlation.algorithms.numeric import (
    _classify_strength as classify_numeric_strength,
)
from dataraum.analysis.correlation.algorithms.numeric import (
    compute_pairwise_correlations,
)


class TestClassifyNumericStrength:
    """Tests for numeric _classify_strength."""

    def test_very_strong(self):
        assert classify_numeric_strength(0.95) == "very_strong"
        assert classify_numeric_strength(-0.95) == "very_strong"

    def test_strong(self):
        assert classify_numeric_strength(0.75) == "strong"
        assert classify_numeric_strength(-0.75) == "strong"

    def test_moderate(self):
        assert classify_numeric_strength(0.55) == "moderate"

    def test_weak(self):
        assert classify_numeric_strength(0.35) == "weak"

    def test_none(self):
        assert classify_numeric_strength(0.1) == "none"
        assert classify_numeric_strength(0.0) == "none"

    def test_boundary_values(self):
        assert classify_numeric_strength(0.9) == "very_strong"
        assert classify_numeric_strength(0.7) == "strong"
        assert classify_numeric_strength(0.5) == "moderate"
        assert classify_numeric_strength(0.3) == "weak"
        assert classify_numeric_strength(0.29) == "none"


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


class TestComputePairwiseCorrelations:
    """Tests for compute_pairwise_correlations."""

    def test_perfect_positive_correlation(self):
        rng = np.random.default_rng(42)
        x = rng.normal(0, 1, 100)
        data = np.column_stack([x, x * 2 + 1])  # y = 2x + 1

        results = compute_pairwise_correlations(data, min_correlation=0.3)

        assert len(results) == 1
        assert abs(results[0].pearson_r - 1.0) < 0.01
        assert results[0].is_significant

    def test_no_correlation(self):
        rng = np.random.default_rng(42)
        data = rng.normal(0, 1, (100, 2))

        results = compute_pairwise_correlations(data, min_correlation=0.3)

        # Random data should have near-zero correlation, filtered by threshold
        assert len(results) == 0

    def test_filters_by_min_correlation(self):
        rng = np.random.default_rng(42)
        x = rng.normal(0, 1, 100)
        y = x + rng.normal(0, 2, 100)  # Weak correlation
        data = np.column_stack([x, y])

        # With high threshold, weak correlation is filtered
        results_high = compute_pairwise_correlations(data, min_correlation=0.9)
        # With low threshold, it's included
        results_low = compute_pairwise_correlations(data, min_correlation=0.1)

        assert len(results_high) == 0
        assert len(results_low) >= 1

    def test_skips_constant_columns(self):
        data = np.column_stack(
            [
                np.ones(100),  # constant
                np.arange(100, dtype=float),
            ]
        )

        results = compute_pairwise_correlations(data, min_correlation=0.0)
        assert len(results) == 0

    def test_skips_insufficient_samples(self):
        data = np.column_stack(
            [
                np.array([1.0, 2.0, 3.0]),
                np.array([4.0, 5.0, 6.0]),
            ]
        )

        results = compute_pairwise_correlations(data, min_correlation=0.0, min_samples=10)
        assert len(results) == 0

    def test_handles_nan_values(self):
        x = np.arange(50, dtype=float)
        y = x * 2.0
        # Add NaN values
        x = np.concatenate([x, [np.nan] * 10])
        y = np.concatenate([y, [np.nan] * 10])
        data = np.column_stack([x, y])

        results = compute_pairwise_correlations(data, min_correlation=0.3)

        assert len(results) == 1
        assert results[0].sample_size == 50

    def test_multiple_columns(self):
        rng = np.random.default_rng(42)
        x = rng.normal(0, 1, 100)
        data = np.column_stack(
            [
                x,
                x * 2 + 1,  # perfect correlation with x
                rng.normal(0, 1, 100),  # uncorrelated
            ]
        )

        results = compute_pairwise_correlations(data, min_correlation=0.5)

        # Should find correlation between col0 and col1
        assert any(r.col1_idx == 0 and r.col2_idx == 1 for r in results)


class TestBuildContingencyTable:
    """Tests for build_contingency_table."""

    def test_simple_table(self):
        col1 = ["A", "A", "B", "B"]
        col2 = ["X", "Y", "X", "Y"]

        table = build_contingency_table(col1, col2)

        assert table.shape == (2, 2)
        assert table.sum() == 4

    def test_uneven_categories(self):
        col1 = ["A", "A", "A", "B"]
        col2 = ["X", "X", "Y", "X"]

        table = build_contingency_table(col1, col2)

        assert table.shape == (2, 2)
        assert table[0, 0] == 2  # A, X
        assert table[0, 1] == 1  # A, Y
        assert table[1, 0] == 1  # B, X
        assert table[1, 1] == 0  # B, Y


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
