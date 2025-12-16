"""Pure numeric correlation algorithms.

Computes Pearson and Spearman correlations on numpy arrays.
No database, no async - just math.
"""

from dataclasses import dataclass

import numpy as np
from scipy import stats


@dataclass
class CorrelationResult:
    """Result from correlation computation."""

    col1_idx: int
    col2_idx: int
    pearson_r: float
    pearson_p: float
    spearman_rho: float
    spearman_p: float
    sample_size: int
    strength: str  # 'none', 'weak', 'moderate', 'strong', 'very_strong'
    is_significant: bool


def _classify_strength(r: float) -> str:
    """Classify correlation strength by absolute value."""
    abs_r = abs(r)
    if abs_r >= 0.9:
        return "very_strong"
    elif abs_r >= 0.7:
        return "strong"
    elif abs_r >= 0.5:
        return "moderate"
    elif abs_r >= 0.3:
        return "weak"
    return "none"


def compute_pairwise_correlations(
    data: np.ndarray,
    min_correlation: float = 0.3,
    min_samples: int = 10,
) -> list[CorrelationResult]:
    """Compute Pearson and Spearman correlations for all column pairs.

    Args:
        data: 2D array where each column is a variable (rows are observations)
        min_correlation: Minimum |r| to include in results
        min_samples: Minimum observations required

    Returns:
        List of CorrelationResult for pairs above threshold
    """
    n_cols = data.shape[1]
    results = []

    for i in range(n_cols):
        for j in range(i + 1, n_cols):
            col1 = data[:, i]
            col2 = data[:, j]

            # Remove NaN pairs
            mask = ~(np.isnan(col1) | np.isnan(col2))
            col1_clean = col1[mask]
            col2_clean = col2[mask]

            if len(col1_clean) < min_samples:
                continue

            # Pearson
            pearson_r, pearson_p = stats.pearsonr(col1_clean, col2_clean)
            pearson_r = float(np.asarray(pearson_r).item())
            pearson_p = float(np.asarray(pearson_p).item())

            # Spearman
            spearman_rho, spearman_p = stats.spearmanr(col1_clean, col2_clean)
            spearman_rho = float(np.asarray(spearman_rho).item())
            spearman_p = float(np.asarray(spearman_p).item())

            # Filter by threshold
            if abs(pearson_r) < min_correlation and abs(spearman_rho) < min_correlation:
                continue

            max_corr = max(abs(pearson_r), abs(spearman_rho))
            strength = _classify_strength(max_corr)
            is_significant = bool(min(pearson_p, spearman_p) < 0.05)

            results.append(
                CorrelationResult(
                    col1_idx=i,
                    col2_idx=j,
                    pearson_r=pearson_r,
                    pearson_p=pearson_p,
                    spearman_rho=spearman_rho,
                    spearman_p=spearman_p,
                    sample_size=len(col1_clean),
                    strength=strength,
                    is_significant=is_significant,
                )
            )

    return results
