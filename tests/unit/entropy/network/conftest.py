"""Pytest fixtures for Bayesian Entropy Network tests."""

import pytest

from dataraum.entropy.network.config import (
    CptGenerationConfig,
    DiscretizationConfig,
    EdgeConfig,
    NetworkConfig,
    NodeConfig,
    get_network_config,
)
from dataraum.entropy.network.model import EntropyNetwork


@pytest.fixture
def small_config() -> NetworkConfig:
    """Minimal 4-node network for fast tests.

    Structure:
        root_a ──0.8──> child_x ──0.7──> leaf_z
        root_b ──0.6──> child_x
                        child_x ──0.5──> leaf_z (via root_b path)
    """
    return NetworkConfig(
        states=["low", "medium", "high"],
        discretization=DiscretizationConfig(low_upper=0.3, medium_upper=0.6),
        nodes={
            "root_a": NodeConfig(
                name="root_a",
                layer="structural",
                dimension="types",
                sub_dimension="root_a",
                prior=[0.6, 0.3, 0.1],
            ),
            "root_b": NodeConfig(
                name="root_b",
                layer="value",
                dimension="nulls",
                sub_dimension="root_b",
                prior=[0.5, 0.3, 0.2],
            ),
            "child_x": NodeConfig(
                name="child_x",
                layer="computational",
                dimension="derived_values",
                sub_dimension="child_x",
            ),
            "leaf_z": NodeConfig(
                name="leaf_z",
                layer="intent",
                dimension="query",
                sub_dimension="readiness",
            ),
        },
        edges=[
            EdgeConfig(parent="root_a", child="child_x", strength=0.8),
            EdgeConfig(parent="root_b", child="child_x", strength=0.6),
            EdgeConfig(parent="child_x", child="leaf_z", strength=0.7),
        ],
        cpt_generation=CptGenerationConfig(
            influence_blend=0.7,
            pessimistic_shift=0.1,
            min_probability=0.01,
        ),
    )


@pytest.fixture
def small_network(small_config: NetworkConfig) -> EntropyNetwork:
    """Built network from small_config."""
    return EntropyNetwork(config=small_config)


@pytest.fixture
def full_config() -> NetworkConfig:
    """Full 15-node network loaded from config/entropy/network.yaml."""
    return get_network_config()


@pytest.fixture
def full_network(full_config: NetworkConfig) -> EntropyNetwork:
    """Built network from full config."""
    return EntropyNetwork(config=full_config)
