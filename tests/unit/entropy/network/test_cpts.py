"""Tests for CPT generation."""

import math

import numpy as np

from dataraum.entropy.network.config import CptGenerationConfig, NetworkConfig
from dataraum.entropy.network.cpts import (
    generate_all_cpds,
    make_multi_parent_cpd,
    make_root_cpd,
    make_single_parent_cpd,
)

STATES = ["low", "medium", "high"]


class TestRootCPD:
    """Test root node CPD generation."""

    def test_shape_matches_prior(self):
        cpd = make_root_cpd("test_node", [0.6, 0.3, 0.1], STATES)
        assert cpd.get_values().shape == (3, 1)

    def test_values_match_prior(self):
        prior = [0.5, 0.3, 0.2]
        cpd = make_root_cpd("test_node", prior, STATES)
        values = cpd.get_values().flatten()
        for i, p in enumerate(prior):
            assert math.isclose(values[i], p, rel_tol=1e-6)

    def test_columns_sum_to_one(self):
        cpd = make_root_cpd("test_node", [0.7, 0.2, 0.1], STATES)
        col_sum = cpd.get_values().sum(axis=0)
        assert math.isclose(col_sum[0], 1.0, rel_tol=1e-6)


class TestSingleParentCPD:
    """Test single-parent CPD generation."""

    def test_columns_sum_to_one(self):
        config = CptGenerationConfig()
        cpd = make_single_parent_cpd("child", "parent", 0.8, STATES, config)
        col_sums = cpd.get_values().sum(axis=0)
        for s in col_sums:
            assert math.isclose(s, 1.0, rel_tol=1e-6)

    def test_high_strength_diagonal_dominance(self):
        """High edge strength should give diagonal dominance in the CPT."""
        config = CptGenerationConfig(influence_blend=0.7, min_probability=0.01)
        cpd = make_single_parent_cpd("child", "parent", 0.95, STATES, config)
        values = cpd.get_values()
        # Diagonal elements should be the largest in each column
        for j in range(3):
            assert values[j, j] == max(values[:, j])

    def test_low_strength_closer_to_uniform(self):
        """Low edge strength should produce near-uniform distribution."""
        config = CptGenerationConfig(influence_blend=0.7, min_probability=0.01)
        cpd = make_single_parent_cpd("child", "parent", 0.1, STATES, config)
        values = cpd.get_values()
        # All values should be relatively close to 1/3 (within 0.2 of uniform)
        for val in values.flatten():
            assert 0.15 < val < 0.55

    def test_no_zero_probabilities(self):
        config = CptGenerationConfig(min_probability=0.01)
        cpd = make_single_parent_cpd("child", "parent", 0.99, STATES, config)
        assert np.all(cpd.get_values() >= 0.01)


class TestMultiParentCPD:
    """Test multi-parent CPD generation."""

    def test_all_columns_sum_to_one(self):
        config = CptGenerationConfig()
        cpd = make_multi_parent_cpd(
            "child",
            [("parent_a", 0.8), ("parent_b", 0.6)],
            STATES,
            config,
        )
        col_sums = cpd.get_values().sum(axis=0)
        for s in col_sums:
            assert math.isclose(s, 1.0, rel_tol=1e-6)

    def test_no_zero_probabilities(self):
        config = CptGenerationConfig(min_probability=0.01)
        cpd = make_multi_parent_cpd(
            "child",
            [("parent_a", 0.9), ("parent_b", 0.9)],
            STATES,
            config,
        )
        assert np.all(cpd.get_values() >= 0.009)  # Allow small float error

    def test_correct_shape_for_two_parents(self):
        """Two parents with 3 states each = 9 columns."""
        config = CptGenerationConfig()
        cpd = make_multi_parent_cpd(
            "child",
            [("parent_a", 0.8), ("parent_b", 0.6)],
            STATES,
            config,
        )
        values = cpd.get_values()
        assert values.shape == (3, 9)

    def test_pessimistic_shift_increases_high_probability(self):
        """Pessimistic shift should increase P(high) compared to no shift."""
        config_no_shift = CptGenerationConfig(pessimistic_shift=0.0, min_probability=0.01)
        config_with_shift = CptGenerationConfig(pessimistic_shift=0.2, min_probability=0.01)

        cpd_no_shift = make_multi_parent_cpd(
            "child",
            [("p1", 0.5), ("p2", 0.5)],
            STATES,
            config_no_shift,
        )
        cpd_with_shift = make_multi_parent_cpd(
            "child",
            [("p1", 0.5), ("p2", 0.5)],
            STATES,
            config_with_shift,
        )

        # P(high) should be larger with pessimistic shift for most columns
        high_no_shift = cpd_no_shift.get_values()[2, :]  # "high" is index 2
        high_with_shift = cpd_with_shift.get_values()[2, :]
        # At least some columns should have higher P(high)
        assert np.any(high_with_shift > high_no_shift)

    def test_three_parents(self):
        """Three parents with 3 states each = 27 columns."""
        config = CptGenerationConfig()
        cpd = make_multi_parent_cpd(
            "child",
            [("p1", 0.7), ("p2", 0.5), ("p3", 0.3)],
            STATES,
            config,
        )
        values = cpd.get_values()
        assert values.shape == (3, 27)
        col_sums = values.sum(axis=0)
        for s in col_sums:
            assert math.isclose(s, 1.0, rel_tol=1e-6)


class TestGenerateAllCPDs:
    """Test generating all CPDs from config."""

    def test_one_cpd_per_node(self, small_config: NetworkConfig):
        cpds = generate_all_cpds(small_config)
        assert len(cpds) == len(small_config.nodes)

    def test_full_config_cpds(self, full_config: NetworkConfig):
        cpds = generate_all_cpds(full_config)
        assert len(cpds) == 15

    def test_all_cpds_valid(self, full_config: NetworkConfig):
        """Every CPD should have columns summing to 1."""
        cpds = generate_all_cpds(full_config)
        for cpd in cpds:
            col_sums = cpd.get_values().sum(axis=0)
            for s in col_sums:
                assert math.isclose(s, 1.0, rel_tol=1e-5), (
                    f"CPD for {cpd.variable} has column not summing to 1"
                )
