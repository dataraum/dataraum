"""Pytest fixtures for entropy views tests.

Reuses small_network and full_network from network conftest,
plus EntropyObject builders for test data.
"""

import pytest

from dataraum.entropy.models import EntropyObject, ResolutionOption
from dataraum.entropy.network.config import (
    CptGenerationConfig,
    DiscretizationConfig,
    EdgeConfig,
    NetworkConfig,
    NodeConfig,
    get_network_config,
    reset_config_cache,
)
from dataraum.entropy.network.model import EntropyNetwork


@pytest.fixture(autouse=True)
def _clear_config_cache():
    """Reset config cache before each test."""
    reset_config_cache()
    yield
    reset_config_cache()


@pytest.fixture
def small_config() -> NetworkConfig:
    """Minimal 4-node network for fast tests.

    Structure:
        root_a ──0.8──> child_x ──0.7──> leaf_z
        root_b ──0.6──> child_x
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


def make_entropy_object(
    *,
    layer: str = "structural",
    dimension: str = "types",
    sub_dimension: str = "type_fidelity",
    target: str = "column:test_table.col1",
    score: float = 0.5,
    confidence: float = 0.9,
    evidence: list | None = None,
    resolution_options: list | None = None,
    detector_id: str = "test_detector",
) -> EntropyObject:
    """Create an EntropyObject for testing."""
    return EntropyObject(
        layer=layer,
        dimension=dimension,
        sub_dimension=sub_dimension,
        target=target,
        score=score,
        confidence=confidence,
        evidence=evidence or [{"metric": "test", "value": score}],
        resolution_options=resolution_options or [],
        detector_id=detector_id,
    )


def make_resolution_option(
    *,
    action: str = "fix_something",
    expected_entropy_reduction: float = 0.3,
    effort: str = "low",
    description: str = "Fix the thing",
) -> ResolutionOption:
    """Create a ResolutionOption for testing."""
    return ResolutionOption(
        action=action,
        parameters={"key": "value"},
        expected_entropy_reduction=expected_entropy_reduction,
        effort=effort,
        description=description,
        cascade_dimensions=["dim1"],
    )
