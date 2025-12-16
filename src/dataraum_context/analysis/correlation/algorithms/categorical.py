"""Pure categorical association algorithms (Cramér's V).

Computes Cramér's V from contingency tables.
No database, no async - just math.
"""

from dataclasses import dataclass
from typing import Any

import numpy as np
from scipy import stats


@dataclass
class AssociationResult:
    """Result from Cramér's V computation."""

    col1_idx: int
    col2_idx: int
    cramers_v: float
    chi_square: float
    p_value: float
    degrees_of_freedom: int
    sample_size: int
    strength: str  # 'none', 'weak', 'moderate', 'strong'
    is_significant: bool


def _classify_strength(v: float) -> str:
    """Classify association strength."""
    if v >= 0.5:
        return "strong"
    elif v >= 0.3:
        return "moderate"
    elif v >= 0.1:
        return "weak"
    return "none"


def compute_cramers_v(
    contingency_table: np.ndarray,
    col1_idx: int = 0,
    col2_idx: int = 1,
) -> AssociationResult | None:
    """Compute Cramér's V from contingency table.

    Args:
        contingency_table: 2D array of counts
        col1_idx: Index of first column (for result)
        col2_idx: Index of second column (for result)

    Returns:
        AssociationResult or None if insufficient data
    """
    if contingency_table.shape[0] < 2 or contingency_table.shape[1] < 2:
        return None

    n = contingency_table.sum()
    if n < 10:
        return None

    # Chi-square test
    chi2, p_value, dof, _ = stats.chi2_contingency(contingency_table)

    # Cramér's V
    min_dim = min(contingency_table.shape) - 1
    if min_dim == 0:
        return None

    cramers_v = float(np.sqrt(chi2 / (n * min_dim)))

    return AssociationResult(
        col1_idx=col1_idx,
        col2_idx=col2_idx,
        cramers_v=cramers_v,
        chi_square=float(chi2),
        p_value=float(p_value),
        degrees_of_freedom=int(dof),
        sample_size=int(n),
        strength=_classify_strength(cramers_v),
        is_significant=bool(p_value < 0.05),
    )


def build_contingency_table(
    col1_values: list[Any],
    col2_values: list[Any],
) -> np.ndarray:
    """Build contingency table from two columns of values.

    Args:
        col1_values: Values from first column
        col2_values: Values from second column

    Returns:
        2D numpy array contingency table
    """
    # Get unique values
    unique1 = sorted(set(col1_values))
    unique2 = sorted(set(col2_values))

    # Build index maps
    idx1 = {v: i for i, v in enumerate(unique1)}
    idx2 = {v: i for i, v in enumerate(unique2)}

    # Count occurrences
    table = np.zeros((len(unique1), len(unique2)))
    for v1, v2 in zip(col1_values, col2_values, strict=False):
        table[idx1[v1], idx2[v2]] += 1

    return table
