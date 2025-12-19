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

    def __init__(self, max_dimension: int = 2) -> None:
        self.max_dimension = max_dimension
        self.feature_cache: dict[str, Any] = {}

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
        Extract topological features from a single column.

        Returns exactly 7 features for all column types:
        [mean, std, skew, kurtosis, entropy, autocorr, null_ratio]
        """
        # Standard feature vector length
        FEATURE_COUNT = 7

        # Handle boolean columns separately
        if pd.api.types.is_bool_dtype(series):
            bool_as_int = series.astype(int)
            bool_non_null = bool_as_int.dropna()
            counts = bool_as_int.value_counts()
            mean_val = float(bool_non_null.mean()) if len(bool_non_null) > 0 else 0.0
            std_val = float(bool_non_null.std()) if len(bool_non_null) > 1 else 0.0
            entropy_val = float(entropy(counts.to_numpy() + 1e-10)) if len(counts) > 0 else 0.0
            features = [
                mean_val,
                std_val,
                0.0,  # skew not meaningful for binary
                0.0,  # kurtosis not meaningful for binary
                entropy_val,
                0.0,  # autocorr not meaningful for binary
                float(series.isna().mean()),
            ]
            return np.array(features, dtype=np.float64)

        # Type-based features for numeric columns
        elif pd.api.types.is_numeric_dtype(series):
            try:
                numeric_series: pd.Series[float] = series.astype(float)
            except (ValueError, TypeError):
                # Can't convert to float, treat as unknown
                return np.zeros(FEATURE_COUNT, dtype=np.float64)

            non_null = numeric_series.dropna()

            # Statistical features with NaN handling
            mean_val = float(non_null.mean()) if len(non_null) > 0 else 0.0
            std_val = float(non_null.std()) if len(non_null) > 1 else 0.0
            skew_result = non_null.skew() if len(non_null) > 2 else 0.0
            kurt_result = non_null.kurtosis() if len(non_null) > 3 else 0.0
            skew_val = float(skew_result) if isinstance(skew_result, (int, float)) else 0.0
            kurt_val = float(kurt_result) if isinstance(kurt_result, (int, float)) else 0.0

            # Handle NaN results from stats
            mean_val = 0.0 if np.isnan(mean_val) else mean_val
            std_val = 0.0 if np.isnan(std_val) else std_val
            skew_val = 0.0 if np.isnan(skew_val) else skew_val
            kurt_val = 0.0 if np.isnan(kurt_val) else kurt_val

            # Distribution entropy
            if len(non_null) > 0:
                hist, _ = np.histogram(non_null.to_numpy(), bins=20)
                entropy_val = float(entropy(hist + 1e-10))
            else:
                entropy_val = 0.0

            # Autocorrelation (for temporal patterns)
            if len(non_null) > 1 and std_val > 0:
                autocorr_result = numeric_series.autocorr()
                autocorr_val = 0.0 if np.isnan(autocorr_result) else float(autocorr_result)
            else:
                autocorr_val = 0.0

            features = [
                mean_val,
                std_val,
                skew_val,
                kurt_val,
                entropy_val,
                autocorr_val,
                float(series.isna().mean()),
            ]
            return np.array(features, dtype=np.float64)

        elif pd.api.types.is_string_dtype(series):
            # Categorical features (7 features total)
            non_null = series.dropna()
            cardinality_ratio = float(series.nunique() / len(series)) if len(series) > 0 else 0.0
            avg_len = float(non_null.str.len().mean()) if len(non_null) > 0 else 0.0
            avg_len = 0.0 if np.isnan(avg_len) else avg_len
            category_entropy = (
                float(entropy(series.value_counts().to_numpy() + 1e-10))
                if series.nunique() > 0
                else 0.0
            )
            features = [
                cardinality_ratio,  # like mean - represents distribution
                0.0,  # std - not applicable
                0.0,  # skew - not applicable
                0.0,  # kurtosis - not applicable
                category_entropy,  # entropy
                avg_len,  # use avg_len instead of autocorr
                float(series.isna().mean()),
            ]
            return np.array(features, dtype=np.float64)

        elif pd.api.types.is_datetime64_dtype(series):
            # Temporal features (7 features total)
            non_null = series.dropna()
            if len(non_null) > 1:
                time_diffs = non_null.sort_values().diff().dt.total_seconds().dropna()
                mean_diff = float(time_diffs.mean()) if len(time_diffs) > 0 else 0.0
                std_diff = float(time_diffs.std()) if len(time_diffs) > 1 else 0.0
                mean_diff = 0.0 if np.isnan(mean_diff) else mean_diff
                std_diff = 0.0 if np.isnan(std_diff) else std_diff

                # Hour and day distribution entropy
                try:
                    hour_counts = non_null.dt.hour.value_counts()
                    hour_entropy = float(entropy(hour_counts.to_numpy() + 1e-10))
                except Exception:
                    hour_entropy = 0.0

                try:
                    day_counts = non_null.dt.dayofweek.value_counts()
                    day_entropy = float(entropy(day_counts.to_numpy() + 1e-10))
                except Exception:
                    day_entropy = 0.0

                features = [
                    mean_diff,  # mean
                    std_diff,  # std
                    0.0,  # skew - not computed
                    0.0,  # kurtosis - not computed
                    hour_entropy,  # entropy of hour distribution
                    day_entropy,  # use day entropy instead of autocorr
                    float(series.isna().mean()),
                ]
            else:
                features = [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, float(series.isna().mean())]
            return np.array(features, dtype=np.float64)

        else:
            # Unknown type - return zeros
            return np.array([0, 0, 0, 0, 0, 0, float(series.isna().mean())], dtype=np.float64)

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

    def analyze_column_relationship(self, col1: pd.Series, col2: pd.Series) -> dict[str, Any]:
        """
        Analyze topological relationship between two columns
        """
        relationship = {"strength": 0, "type": "unknown", "confidence": 0}

        # Both numeric - correlation and mutual information
        if pd.api.types.is_numeric_dtype(col1) and pd.api.types.is_numeric_dtype(col2):
            valid_mask = ~(col1.isna() | col2.isna())
            if valid_mask.sum() > 10:
                c1_valid = col1[valid_mask].astype(float)
                c2_valid = col2[valid_mask].astype(float)
                # Check for constant columns (would cause NaN correlation)
                if c1_valid.std() > 0 and c2_valid.std() > 0:
                    correlation = c1_valid.corr(c2_valid)
                    if not np.isnan(correlation):
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

        # Get only truly numeric columns (exclude object, string, etc.)
        numeric_df = sample_df.select_dtypes(include=[np.number])

        if numeric_df.empty or len(numeric_df.columns) == 0:
            return {"message": "No numeric columns for row topology"}

        # Convert to float64 and handle NaN
        try:
            row_features_array = numeric_df.values.astype(np.float64)
        except (ValueError, TypeError):
            return {"message": "Could not convert to numeric array"}

        # Replace NaN with column means (or 0 if all NaN)
        col_means = np.nanmean(row_features_array, axis=0)
        col_means = np.where(np.isnan(col_means), 0, col_means)
        for col_idx in range(row_features_array.shape[1]):
            nan_mask = np.isnan(row_features_array[:, col_idx])
            row_features_array[nan_mask, col_idx] = col_means[col_idx]

        # Check for valid data
        if len(row_features_array) < 3:
            return {"message": "Not enough rows for topology analysis"}

        if np.all(np.isnan(row_features_array)) or np.all(row_features_array == 0):
            return {"message": "No valid numeric data for topology analysis"}

        # Compute persistence on row space
        try:
            row_distances = squareform(pdist(row_features_array, metric="euclidean"))
            row_persistence = ripser(row_distances, maxdim=1, distance_matrix=True)

            return {
                "row_clusters": self.identify_clusters(row_persistence),
                "outlier_score": self.compute_outlier_scores(row_features_array),
                "row_persistence": row_persistence["dgms"],
            }
        except Exception as e:
            return {"message": f"Row topology computation failed: {e}"}

    def identify_clusters(self, persistence_result: dict[str, Any]) -> dict[str, Any]:
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
                        n_clusters = int(np.argmax(gaps)) + 2
                    else:
                        n_clusters = 1
                else:
                    n_clusters = 1

                return {
                    "n_clusters": n_clusters,
                    "cluster_separation": np.mean(finite_deaths) if len(finite_deaths) > 0 else 0,
                }

        return {"n_clusters": 1, "cluster_separation": 0}

    def compute_outlier_scores(self, features: np.ndarray) -> list[float]:
        """
        Use topological methods to find outliers
        """
        if len(features) < 10:
            return []

        # Compute distance to k-nearest neighbors
        k = min(5, len(features) - 1)
        distances = squareform(pdist(features))

        outlier_scores: list[float] = []
        for i in range(len(features)):
            # Sort distances and take k-nearest
            sorted_distances = np.sort(distances[i])[1 : k + 1]  # Exclude self
            outlier_scores.append(float(np.mean(sorted_distances)))

        # Normalize scores
        if len(outlier_scores) > 0 and np.std(outlier_scores) > 0:
            scores_array = np.array(outlier_scores)
            normalized_scores = (scores_array - np.mean(scores_array)) / np.std(scores_array)
            return [float(x) for x in normalized_scores]

        return outlier_scores

    def extract_metadata(self, df: pd.DataFrame) -> dict[str, Any]:
        """
        Extract basic metadata
        """
        return {
            "n_rows": len(df),
            "n_columns": len(df.columns),
            "column_types": {str(k): int(v) for k, v in df.dtypes.value_counts().to_dict().items()},
            "memory_usage": int(df.memory_usage(deep=True).sum()),
        }
