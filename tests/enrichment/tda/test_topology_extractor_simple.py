"""Simplified tests for TDA topology extractor focusing on core behavior."""

import numpy as np
import pandas as pd
import pytest

from dataraum_context.enrichment.tda.topology_extractor import TableTopologyExtractor


@pytest.fixture
def extractor():
    """Create a topology extractor instance."""
    return TableTopologyExtractor(max_dimension=2)


@pytest.fixture
def simple_df():
    """Create a simple dataframe for testing."""
    return pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "value": [10.0, 20.0, 30.0, 40.0, 50.0],
        }
    )


def test_topology_extractor_basic(extractor, simple_df):
    """Test basic topology extraction functionality."""
    result = extractor.extract_topology(simple_df)

    # Check top-level structure
    assert isinstance(result, dict)
    assert "global_persistence" in result
    assert "column_topology" in result
    assert "row_topology" in result
    assert "metadata" in result


def test_global_persistence_structure(extractor, simple_df):
    """Test that global persistence has required structure."""
    result = extractor.extract_topology(simple_df)
    persistence = result["global_persistence"]

    assert isinstance(persistence, dict)
    assert "diagrams" in persistence or "stats" in persistence


def test_metadata_basic_fields(extractor, simple_df):
    """Test that metadata contains basic information."""
    result = extractor.extract_topology(simple_df)
    metadata = result["metadata"]

    assert isinstance(metadata, dict)
    assert "n_rows" in metadata
    assert metadata["n_rows"] == 5


def test_feature_matrix_generation(extractor, simple_df):
    """Test that feature matrix can be built."""
    features = extractor.build_feature_matrix(simple_df)

    assert isinstance(features, np.ndarray)
    assert features.shape[0] == len(simple_df.columns)
    assert features.shape[1] > 0


def test_column_features_numeric(extractor):
    """Test feature extraction for numeric columns."""
    series = pd.Series([1.0, 2.0, 3.0, 4.0, 5.0])
    features = extractor.extract_column_features(series)

    assert isinstance(features, np.ndarray)
    assert len(features) > 0
    assert not np.isnan(features).all()


def test_column_features_string(extractor):
    """Test feature extraction for string columns."""
    series = pd.Series(["A", "B", "C", "A", "B"])
    features = extractor.extract_column_features(series)

    assert isinstance(features, np.ndarray)
    assert len(features) > 0


def test_persistence_computation(extractor, simple_df):
    """Test that persistence can be computed."""
    features = extractor.build_feature_matrix(simple_df)
    persistence = extractor.compute_persistence(features)

    assert isinstance(persistence, dict)
    # Should have some persistence information
    assert len(persistence) > 0


def test_mixed_types_dataframe(extractor):
    """Test with mixed data types."""
    df = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "value": [100.0, 200.0, 300.0],
        }
    )

    result = extractor.extract_topology(df)
    assert isinstance(result, dict)


def test_single_row_dataframe(extractor):
    """Test with single row."""
    df = pd.DataFrame({"a": [1], "b": [2]})
    result = extractor.extract_topology(df)

    assert isinstance(result, dict)
    assert result["metadata"]["n_rows"] == 1


def test_single_column_dataframe(extractor):
    """Test with single column."""
    df = pd.DataFrame({"value": [1, 2, 3, 4, 5]})
    result = extractor.extract_topology(df)

    assert isinstance(result, dict)


def test_empty_series_features(extractor):
    """Test handling of empty series."""
    series = pd.Series([], dtype=float)
    features = extractor.extract_column_features(series)

    assert isinstance(features, np.ndarray)
    assert len(features) > 0


def test_datetime_column(extractor):
    """Test with datetime column."""
    df = pd.DataFrame(
        {
            "date": pd.date_range("2024-01-01", periods=5, freq="D"),
            "value": [1, 2, 3, 4, 5],
        }
    )

    result = extractor.extract_topology(df)
    assert isinstance(result, dict)


def test_categorical_data(extractor):
    """Test with categorical data."""
    df = pd.DataFrame(
        {
            "category": pd.Categorical(["A", "B", "C", "A", "B"]),
            "value": [1, 2, 3, 4, 5],
        }
    )

    result = extractor.extract_topology(df)
    assert isinstance(result, dict)


def test_max_dimension_parameter():
    """Test max_dimension parameter."""
    extractor1 = TableTopologyExtractor(max_dimension=1)
    extractor2 = TableTopologyExtractor(max_dimension=2)

    assert extractor1.max_dimension == 1
    assert extractor2.max_dimension == 2


def test_feature_consistency(extractor, simple_df):
    """Test that feature extraction is consistent."""
    features1 = extractor.build_feature_matrix(simple_df)
    features2 = extractor.build_feature_matrix(simple_df)

    np.testing.assert_array_almost_equal(features1, features2)
