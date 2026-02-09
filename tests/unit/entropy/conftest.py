"""Pytest fixtures for entropy layer tests."""

import pytest

from dataraum.entropy.analysis.aggregator import ColumnSummary
from dataraum.entropy.config import get_entropy_config
from dataraum.entropy.detectors.base import DetectorContext, DetectorRegistry
from dataraum.entropy.models import (
    EntropyObject,
    ResolutionOption,
)


@pytest.fixture
def empty_registry() -> DetectorRegistry:
    """Empty detector registry for testing."""
    return DetectorRegistry()


@pytest.fixture
def sample_detector_context() -> DetectorContext:
    """Sample detector context with mock analysis results."""
    return DetectorContext(
        source_id="src_001",
        table_id="tbl_001",
        table_name="orders",
        column_id="col_001",
        column_name="amount",
        analysis_results={
            "typing": {
                "detected_type": "DECIMAL",
                "parse_success_rate": 0.95,
                "type_candidates": [
                    {"type": "DECIMAL", "confidence": 0.95},
                    {"type": "VARCHAR", "confidence": 0.05},
                ],
            },
            "statistics": {
                "null_ratio": 0.02,
                "distinct_ratio": 0.87,
                "min": 0.0,
                "max": 10000.0,
                "mean": 245.50,
                "outlier_ratio": 0.03,
            },
            "semantic": {
                "role": "measure",
                "business_description": "Order total amount in USD",
                "unit": "USD",
            },
        },
    )


@pytest.fixture
def high_entropy_context() -> DetectorContext:
    """Context with high-entropy characteristics for testing compound risks."""
    return DetectorContext(
        source_id="src_001",
        table_id="tbl_001",
        table_name="transactions",
        column_id="col_002",
        column_name="value",
        analysis_results={
            "typing": {
                "detected_type": "VARCHAR",  # Should be numeric
                "parse_success_rate": 0.60,  # Low parse rate
                "type_candidates": [
                    {"type": "VARCHAR", "confidence": 0.40},
                    {"type": "DECIMAL", "confidence": 0.35},
                    {"type": "INTEGER", "confidence": 0.25},
                ],
            },
            "statistics": {
                "null_ratio": 0.35,  # High nulls
                "distinct_ratio": 0.12,
                "outlier_ratio": 0.15,  # High outliers
            },
            "semantic": {
                "role": "unknown",
                "business_description": "",  # No description
                "unit": None,  # No unit
            },
        },
    )


@pytest.fixture
def low_entropy_context() -> DetectorContext:
    """Context with low-entropy (clean) characteristics."""
    return DetectorContext(
        source_id="src_001",
        table_id="tbl_001",
        table_name="customers",
        column_id="col_003",
        column_name="customer_id",
        analysis_results={
            "typing": {
                "detected_type": "INTEGER",
                "parse_success_rate": 1.0,
                "type_candidates": [
                    {"type": "INTEGER", "confidence": 1.0},
                ],
            },
            "statistics": {
                "null_ratio": 0.0,
                "distinct_ratio": 1.0,  # Unique values (key)
                "outlier_ratio": 0.0,
            },
            "semantic": {
                "role": "key",
                "business_description": "Unique customer identifier",
                "unit": None,
            },
        },
    )


@pytest.fixture
def sample_entropy_object() -> EntropyObject:
    """Sample entropy object for testing."""
    return EntropyObject(
        layer="structural",
        dimension="types",
        sub_dimension="type_fidelity",
        target="column:orders.amount",
        score=0.35,
        evidence=[
            {"parse_success_rate": 0.65, "failed_values": ["N/A", "TBD", "-"]},
        ],
        resolution_options=[
            ResolutionOption(
                action="declare_type",
                parameters={"column": "amount", "type": "DECIMAL(10,2)"},
                expected_entropy_reduction=0.3,
                effort="low",
                description="Declare explicit type for amount column",
            ),
        ],
        detector_id="type_fidelity",
    )


@pytest.fixture
def sample_column_profile() -> ColumnSummary:
    """Sample column entropy summary for testing."""
    config = get_entropy_config()
    weights = config.composite_weights
    layer_scores = {
        "structural": 0.25,
        "semantic": 0.40,
        "value": 0.15,
        "computational": 0.10,
    }
    composite_score = (
        layer_scores["structural"] * weights["structural"]
        + layer_scores["semantic"] * weights["semantic"]
        + layer_scores["value"] * weights["value"]
        + layer_scores["computational"] * weights["computational"]
    )
    readiness = config.get_readiness(composite_score)
    return ColumnSummary(
        column_id="col_001",
        column_name="amount",
        table_id="tbl_001",
        table_name="orders",
        composite_score=composite_score,
        readiness=readiness,
        layer_scores=layer_scores,
        dimension_scores={
            "structural.types.type_fidelity": 0.25,
            "semantic.units.unit_declared": 0.40,
            "value.nulls.null_ratio": 0.15,
            "computational.aggregations.aggregation_clarity": 0.10,
        },
        high_entropy_dimensions=[],
    )


@pytest.fixture
def high_entropy_column_profile() -> ColumnSummary:
    """High-entropy column summary for compound risk testing."""
    config = get_entropy_config()
    weights = config.composite_weights
    layer_scores = {
        "structural": 0.60,
        "semantic": 0.75,
        "value": 0.55,
        "computational": 0.70,
    }
    composite_score = (
        layer_scores["structural"] * weights["structural"]
        + layer_scores["semantic"] * weights["semantic"]
        + layer_scores["value"] * weights["value"]
        + layer_scores["computational"] * weights["computational"]
    )
    readiness = config.get_readiness(composite_score)
    dimension_scores = {
        "structural.types.type_fidelity": 0.60,
        "semantic.units.unit_declared": 0.85,
        "semantic.business_meaning.naming_clarity": 0.65,
        "value.nulls.null_ratio": 0.55,
        "value.outliers.outlier_rate": 0.45,
        "computational.aggregations.aggregation_clarity": 0.70,
    }
    high_entropy_dims = [
        dim for dim, score in dimension_scores.items() if score >= config.high_entropy_threshold
    ]
    return ColumnSummary(
        column_id="col_002",
        column_name="value",
        table_id="tbl_001",
        table_name="transactions",
        composite_score=composite_score,
        readiness=readiness,
        layer_scores=layer_scores,
        dimension_scores=dimension_scores,
        high_entropy_dimensions=high_entropy_dims,
    )
