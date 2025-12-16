"""Topological feature extraction from tables using TDA.

Extracts persistence diagrams from column feature space to measure
structural similarity between tables.
"""

from typing import Any

import numpy as np
import pandas as pd
from ripser import ripser
from scipy.spatial.distance import pdist, squareform
from scipy.stats import entropy


def extract_column_features(series: pd.Series) -> np.ndarray:
    """Extract numerical features from a column for TDA."""
    features = []

    if pd.api.types.is_numeric_dtype(series):
        features = [
            series.mean() if not series.empty else 0,
            series.std() if len(series) > 1 else 0,
            series.skew() if len(series) > 2 else 0,
            series.kurtosis() if len(series) > 3 else 0,
            entropy(np.histogram(series.dropna(), bins=20)[0] + 1e-10),
            series.autocorr() if len(series) > 1 and not series.isna().all() else 0,
        ]
    elif pd.api.types.is_string_dtype(series):
        features = [
            series.nunique() / len(series) if len(series) > 0 else 0,
            series.str.len().mean() if not series.empty else 0,
            entropy(series.value_counts() + 1e-10),
            0,
            0,
            0,
        ]
    elif pd.api.types.is_datetime64_dtype(series):
        if not series.empty:
            time_diffs = series.sort_values().diff().dt.total_seconds()
            features = [
                time_diffs.mean() if len(time_diffs) > 1 else 0,
                time_diffs.std() if len(time_diffs) > 1 else 0,
                entropy(pd.cut(series.dt.hour, bins=24).value_counts() + 1e-10),
                entropy(pd.cut(series.dt.dayofweek, bins=7).value_counts() + 1e-10),
                0,
                0,
            ]
        else:
            features = [0, 0, 0, 0, 0, 0]
    else:
        features = [0, 0, 0, 0, 0, 0]

    features.append(series.isna().mean())
    return np.array(features)


def compute_persistence(df: pd.DataFrame, max_dim: int = 2) -> dict[str, Any]:
    """Compute persistence diagram for a table's column space."""
    features = np.array([extract_column_features(df[col]) for col in df.columns])

    if len(features) < 2:
        return {"diagrams": [], "stats": []}

    distance_matrix = squareform(pdist(features, metric="euclidean"))
    result = ripser(distance_matrix, maxdim=max_dim, distance_matrix=True)

    stats = []
    for dim, dgm in enumerate(result["dgms"]):
        if len(dgm) > 0:
            lifetimes = dgm[:, 1] - dgm[:, 0]
            finite_mask = dgm[:, 1] < np.inf
            finite_lifetimes = lifetimes[finite_mask]

            stats.append(
                {
                    "dimension": dim,
                    "betti_number": len(dgm),
                    "mean_lifetime": float(np.mean(finite_lifetimes))
                    if len(finite_lifetimes) > 0
                    else 0,
                    "total_persistence": float(np.sum(finite_lifetimes))
                    if len(finite_lifetimes) > 0
                    else 0,
                }
            )

    return {"diagrams": result["dgms"], "stats": stats}


def persistence_similarity(persistence1: dict[str, Any], persistence2: dict[str, Any]) -> float:
    """Compare two persistence diagrams using Wasserstein distance on death times.

    In H0 (connected components), births are all 0. Death times indicate when
    components merge, which is the meaningful structural information.
    """
    from scipy.stats import wasserstein_distance

    dgms1 = persistence1.get("diagrams", [])
    dgms2 = persistence2.get("diagrams", [])

    if not dgms1 or not dgms2:
        return 0.0

    similarities = []
    for dim in range(min(len(dgms1), len(dgms2))):
        dgm1, dgm2 = dgms1[dim], dgms2[dim]

        if len(dgm1) > 0 and len(dgm2) > 0:
            # Use death times (column 1), not births (column 0)
            # In H0, births are all 0; deaths carry the structural info
            deaths1 = dgm1[:, 1][dgm1[:, 1] < np.inf]
            deaths2 = dgm2[:, 1][dgm2[:, 1] < np.inf]

            if len(deaths1) > 0 and len(deaths2) > 0:
                max_death = max(deaths1.max(), deaths2.max())
                if max_death > 0:
                    d1 = np.pad(deaths1 / max_death, (0, max(0, len(deaths2) - len(deaths1))))
                    d2 = np.pad(deaths2 / max_death, (0, max(0, len(deaths1) - len(deaths2))))
                    similarity = 1 / (1 + wasserstein_distance(d1, d2))
                    similarities.append(similarity)

    return float(np.mean(similarities)) if similarities else 0.0
