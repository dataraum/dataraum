"""Pytest fixtures for entropy layer tests."""

import pytest

from dataraum.entropy.detectors.base import DetectorContext, DetectorRegistry
from dataraum.entropy.models import (
    ColumnEntropyProfile,
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
def sample_column_profile() -> ColumnEntropyProfile:
    """Sample column entropy profile for testing."""
    profile = ColumnEntropyProfile(
        column_id="col_001",
        column_name="amount",
        table_name="orders",
        structural_entropy=0.25,
        semantic_entropy=0.40,
        value_entropy=0.15,
        computational_entropy=0.10,
        dimension_scores={
            "structural.types.type_fidelity": 0.25,
            "semantic.units.unit_declared": 0.40,
            "value.nulls.null_ratio": 0.15,
            "computational.aggregations.aggregation_clarity": 0.10,
        },
    )
    profile.calculate_composite()
    profile.update_readiness()
    return profile


@pytest.fixture
def high_entropy_column_profile() -> ColumnEntropyProfile:
    """High-entropy column profile for compound risk testing."""
    profile = ColumnEntropyProfile(
        column_id="col_002",
        column_name="value",
        table_name="transactions",
        structural_entropy=0.60,
        semantic_entropy=0.75,
        value_entropy=0.55,
        computational_entropy=0.70,
        dimension_scores={
            "structural.types.type_fidelity": 0.60,
            "semantic.units.unit_declared": 0.85,
            "semantic.business_meaning.naming_clarity": 0.65,
            "value.nulls.null_ratio": 0.55,
            "value.outliers.outlier_rate": 0.45,
            "computational.aggregations.aggregation_clarity": 0.70,
        },
    )
    profile.calculate_composite()
    profile.update_readiness()
    return profile
