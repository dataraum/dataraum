# core/topology_extractor.py
from typing import Any

import numpy as np
import pandas as pd
from ripser import ripser
from scipy.spatial.distance import pdist, squareform
from scipy.stats import entropy


class TableTopologyExtractor:
    """
    Extract topological features from tabular data
    """

    def __init__(self, max_dimension=2):
        self.max_dimension = max_dimension
        self.feature_cache = {}

    def extract_topology(self, df: pd.DataFrame) -> dict[str, Any]:
        """
        Main method: Extract full topological signature
        """
        # Convert dataframe to feature space
        features = self.build_feature_matrix(df)

        # Compute persistence
        persistence = self.compute_persistence(features)

        # Extract column-level topology
        column_topology = self.extract_column_topology(df)

        # Extract row-level topology (entity relationships)
        row_topology = self.extract_row_topology(df)

        return {
            "global_persistence": persistence,
            "column_topology": column_topology,
            "row_topology": row_topology,
            "metadata": self.extract_metadata(df),
        }

    def build_feature_matrix(self, df: pd.DataFrame) -> np.ndarray:
        """
        Convert table to numerical feature space for TDA
        """
        features = []

        for col in df.columns:
            col_features = self.extract_column_features(df[col])
            features.append(col_features)

        return np.array(features)

    def extract_column_features(self, series: pd.Series) -> np.ndarray:
        """
        Extract topological features from a single column
        """
        features = []

        # Type-based features
        if pd.api.types.is_numeric_dtype(series):
            # Statistical features
            features.extend(
                [
                    series.mean() if not series.empty else 0,
                    series.std() if len(series) > 1 else 0,
                    series.skew() if len(series) > 2 else 0,
                    series.kurtosis() if len(series) > 3 else 0,
                ]
            )

            # Distribution entropy
            hist, _ = np.histogram(series.dropna(), bins=20)
            features.append(entropy(hist + 1e-10))

            # Autocorrelation (for temporal patterns)
            if len(series) > 1:
                features.append(series.autocorr() if not series.isna().all() else 0)
            else:
                features.append(0)

        elif pd.api.types.is_string_dtype(series):
            # Categorical features
            features.extend(
                [
                    series.nunique() / len(series) if len(series) > 0 else 0,  # Cardinality ratio
                    series.str.len().mean() if not series.empty else 0,  # Avg length
                    entropy(series.value_counts() + 1e-10),  # Category distribution entropy
                    0,
                    0,
                    0,  # Padding to match numeric features
                ]
            )

        elif pd.api.types.is_datetime64_dtype(series):
            # Temporal features
            if not series.empty:
                time_diffs = series.sort_values().diff().dt.total_seconds()
                features.extend(
                    [
                        time_diffs.mean() if len(time_diffs) > 1 else 0,
                        time_diffs.std() if len(time_diffs) > 1 else 0,
                        entropy(
                            pd.cut(series.dt.hour, bins=24).value_counts() + 1e-10
                        ),  # Hour distribution
                        entropy(
                            pd.cut(series.dt.dayofweek, bins=7).value_counts() + 1e-10
                        ),  # Day distribution
                        0,
                        0,
                    ]
                )
            else:
                features.extend([0, 0, 0, 0, 0, 0])
        else:
            # Unknown type - use basic features
            features.extend([0, 0, 0, 0, 0, 0])

        # Nullability feature
        features.append(series.isna().mean())

        return np.array(features)

    def compute_persistence(self, features: np.ndarray) -> dict[str, Any]:
        """
        Compute persistence diagrams
        """
        if len(features) < 2:
            return {"diagrams": [], "betti": []}

        # Compute distance matrix
        distances = pdist(features, metric="euclidean")
        distance_matrix = squareform(distances)

        # Compute persistence
        result = ripser(distance_matrix, maxdim=self.max_dimension, distance_matrix=True)

        # Extract persistence diagrams and statistics
        diagrams = result["dgms"]

        # Compute Betti numbers and persistence statistics
        persistence_stats = []
        for dim, dgm in enumerate(diagrams):
            if len(dgm) > 0:
                births = dgm[:, 0]
                deaths = dgm[:, 1]
                lifetimes = deaths - births

                # Filter out infinite persistence
                finite_mask = deaths < np.inf
                finite_lifetimes = lifetimes[finite_mask]

                stats = {
                    "dimension": dim,
                    "betti_number": len(dgm),
                    "mean_lifetime": np.mean(finite_lifetimes) if len(finite_lifetimes) > 0 else 0,
                    "max_lifetime": np.max(finite_lifetimes) if len(finite_lifetimes) > 0 else 0,
                    "total_persistence": np.sum(finite_lifetimes)
                    if len(finite_lifetimes) > 0
                    else 0,
                }
                persistence_stats.append(stats)

        return {
            "diagrams": diagrams,
            "stats": persistence_stats,
            "distance_matrix": distance_matrix,
        }

    def extract_column_topology(self, df: pd.DataFrame) -> dict[str, Any]:
        """
        Understand relationships between columns
        """
        column_relationships = {}

        for i, col1 in enumerate(df.columns):
            for j, col2 in enumerate(df.columns):
                if i < j:  # Only compute once per pair
                    relationship = self.analyze_column_relationship(df[col1], df[col2])
                    if relationship["strength"] > 0.1:  # Threshold for relevance
                        column_relationships[f"{col1}-{col2}"] = relationship

        return column_relationships

    def analyze_column_relationship(self, col1: pd.Series, col2: pd.Series) -> dict[str, float]:
        """
        Analyze topological relationship between two columns
        """
        relationship = {"strength": 0, "type": "unknown", "confidence": 0}

        # Both numeric - correlation and mutual information
        if pd.api.types.is_numeric_dtype(col1) and pd.api.types.is_numeric_dtype(col2):
            valid_mask = ~(col1.isna() | col2.isna())
            if valid_mask.sum() > 10:
                correlation = col1[valid_mask].corr(col2[valid_mask])
                relationship["strength"] = abs(correlation)
                relationship["type"] = "numeric_correlation"
                relationship["confidence"] = min(valid_mask.sum() / len(col1), 1.0)

        # Categorical - check for foreign key relationship
        elif pd.api.types.is_string_dtype(col1) or pd.api.types.is_string_dtype(col2):
            # Check cardinality ratio
            card1 = col1.nunique()
            card2 = col2.nunique()

            if card1 > 0 and card2 > 0:
                cardinality_ratio = min(card1, card2) / max(card1, card2)

                # Check for subset relationship (potential FK)
                if pd.api.types.is_string_dtype(col1) and pd.api.types.is_string_dtype(col2):
                    set1 = set(col1.dropna())
                    set2 = set(col2.dropna())

                    if set1.issubset(set2) or set2.issubset(set1):
                        relationship["strength"] = 0.8
                        relationship["type"] = "potential_foreign_key"
                        relationship["confidence"] = cardinality_ratio
                    else:
                        overlap = len(set1.intersection(set2)) / len(set1.union(set2))
                        if overlap > 0.1:
                            relationship["strength"] = overlap
                            relationship["type"] = "partial_overlap"
                            relationship["confidence"] = overlap

        return relationship

    def extract_row_topology(self, df: pd.DataFrame) -> dict[str, Any]:
        """
        Understand entity relationships (row-level topology)
        """
        # Sample rows for efficiency
        sample_size = min(1000, len(df))
        if len(df) > sample_size:
            sample_df = df.sample(n=sample_size, random_state=42)
        else:
            sample_df = df

        # Create row feature matrix
        row_features = []
        numeric_cols = df.select_dtypes(include=[np.number]).columns

        if len(numeric_cols) > 0:
            for _, row in sample_df[numeric_cols].iterrows():
                row_features.append(row.values)

            row_features = np.array(row_features)

            # Compute persistence on row space
            if len(row_features) > 2:
                row_persistence = ripser(row_features, maxdim=1)

                return {
                    "row_clusters": self.identify_clusters(row_persistence),
                    "outlier_score": self.compute_outlier_scores(row_features),
                    "row_persistence": row_persistence["dgms"],
                }

        return {"message": "No numeric columns for row topology"}

    def identify_clusters(self, persistence_result):
        """
        Identify clusters from persistence diagram
        """
        H0 = persistence_result["dgms"][0]

        # Find significant connected components
        if len(H0) > 0:
            deaths = H0[:, 1]
            finite_deaths = deaths[deaths < np.inf]

            if len(finite_deaths) > 0:
                # Use elbow method to find number of clusters
                sorted_deaths = np.sort(finite_deaths)[::-1]
                if len(sorted_deaths) > 1:
                    gaps = np.diff(sorted_deaths)
                    if len(gaps) > 0:
                        n_clusters = np.argmax(gaps) + 2
                    else:
                        n_clusters = 1
                else:
                    n_clusters = 1

                return {
                    "n_clusters": n_clusters,
                    "cluster_separation": np.mean(finite_deaths) if len(finite_deaths) > 0 else 0,
                }

        return {"n_clusters": 1, "cluster_separation": 0}

    def compute_outlier_scores(self, features):
        """
        Use topological methods to find outliers
        """
        if len(features) < 10:
            return []

        # Compute distance to k-nearest neighbors
        k = min(5, len(features) - 1)
        distances = squareform(pdist(features))

        outlier_scores = []
        for i in range(len(features)):
            # Sort distances and take k-nearest
            sorted_distances = np.sort(distances[i])[1 : k + 1]  # Exclude self
            outlier_scores.append(np.mean(sorted_distances))

        # Normalize scores
        if np.std(outlier_scores) > 0:
            outlier_scores = (outlier_scores - np.mean(outlier_scores)) / np.std(outlier_scores)

        return outlier_scores

    def extract_metadata(self, df):
        """
        Extract basic metadata
        """
        return {
            "n_rows": len(df),
            "n_columns": len(df.columns),
            "column_types": df.dtypes.value_counts().to_dict(),
            "memory_usage": df.memory_usage(deep=True).sum(),
        }
