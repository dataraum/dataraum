"""Pure multicollinearity algorithms (Belsley VDP).

Computes Variance Decomposition Proportions per Belsley, Kuh, Welsch (1980).
No database, no async - just math.
"""

from dataclasses import dataclass
from typing import Literal

import numpy as np

from dataraum.core.logging import get_logger

logger = get_logger(__name__)


@dataclass
class DependencyGroupResult:
    """A group of columns involved in a linear dependency."""

    dimension: int
    eigenvalue: float
    condition_index: float
    severity: Literal["moderate", "severe"]
    involved_col_indices: list[int]
    variance_proportions: list[float]


@dataclass
class MulticollinearityResult:
    """Result from multicollinearity analysis."""

    overall_condition_index: float
    overall_severity: Literal["none", "moderate", "severe"]
    eigenvalues: list[float]
    dependency_groups: list[DependencyGroupResult]


def compute_multicollinearity(
    corr_matrix: np.ndarray,
    vdp_threshold: float = 0.5,
) -> MulticollinearityResult:
    """Compute multicollinearity analysis using Belsley VDP methodology.

    Args:
        corr_matrix: Correlation matrix (n_vars x n_vars)
        vdp_threshold: VDP threshold (Belsley recommends 0.5-0.8)

    Returns:
        MulticollinearityResult with overall metrics and dependency groups
    """
    # Eigenvalue decomposition
    eigenvalues, eigenvectors = np.linalg.eigh(corr_matrix)

    # Sort descending
    idx = eigenvalues.argsort()[::-1]
    eigenvalues = eigenvalues[idx]
    eigenvectors = eigenvectors[:, idx]

    # Compute overall condition index
    max_eigenvalue = eigenvalues[0]
    min_eigenvalue = eigenvalues[-1]
    condition_index = (
        float(np.sqrt(max_eigenvalue / abs(min_eigenvalue)))
        if abs(min_eigenvalue) > 1e-10
        else 999.0
    )

    # Determine overall severity
    if condition_index < 10:
        overall_severity: Literal["none", "moderate", "severe"] = "none"
    elif condition_index < 30:
        overall_severity = "moderate"
    else:
        overall_severity = "severe"

    # Compute VDP and identify dependency groups
    n_vars = len(eigenvalues)
    dependency_groups = _compute_variance_decomposition(
        eigenvalues=eigenvalues,
        eigenvectors=eigenvectors,
        n_vars=n_vars,
        vdp_threshold=vdp_threshold,
    )

    return MulticollinearityResult(
        overall_condition_index=condition_index,
        overall_severity=overall_severity,
        eigenvalues=eigenvalues.tolist(),
        dependency_groups=dependency_groups,
    )


def _compute_variance_decomposition(
    eigenvalues: np.ndarray,
    eigenvectors: np.ndarray,
    n_vars: int,
    vdp_threshold: float = 0.5,
) -> list[DependencyGroupResult]:
    """Compute Variance Decomposition Proportions per Belsley, Kuh, Welsch (1980).

    Correct implementation of Belsley diagnostics for detecting multicollinearity
    involving any number of variables (2, 3, 4+).

    Methodology:
    1. Compute phi_kj = V_kj² / D_j² for all variables k and dimensions j
    2. For each variable k: VDP_kj = phi_kj / Σ_j(phi_kj) [normalize across dimensions]
    3. For dimensions with high CI (>30): variables with VDP > threshold form a group

    This differs from naive eigenvector normalization - VDPs sum to 1 ACROSS dimensions
    for each variable, not across variables for a single dimension.

    Args:
        eigenvalues: Eigenvalues sorted descending (D² from SVD)
        eigenvectors: Corresponding eigenvectors (V from SVD)
        n_vars: Number of variables
        vdp_threshold: VDP threshold (Belsley recommends 0.5-0.8)

    Returns:
        List of DependencyGroupResult objects

    Reference:
        Belsley, D.A., Kuh, E., Welsch, R.E. (1980). Regression Diagnostics:
        Identifying Influential Data and Sources of Collinearity. Wiley.
    """
    n_dims = len(eigenvalues)

    # Step 1: Compute phi matrix (n_vars x n_dims)
    # phi_kj = V_kj² / eigenvalue_j
    # Use abs(eigenvalue) to handle near-zero negative values
    phi_matrix = np.zeros((n_vars, n_dims))
    for j in range(n_dims):
        eigenvalue_abs = abs(eigenvalues[j]) if abs(eigenvalues[j]) > 1e-10 else 1e-10
        for k in range(n_vars):
            phi_matrix[k, j] = eigenvectors[k, j] ** 2 / eigenvalue_abs

    # Step 2: Compute VDP matrix by normalizing phi across dimensions (row-wise)
    # VDP_kj = phi_kj / Σ_j(phi_kj)
    phi_sums = phi_matrix.sum(axis=1, keepdims=True)  # Sum across dimensions for each var
    phi_sums = np.where(phi_sums < 1e-10, 1.0, phi_sums)  # Avoid division by zero
    vdp_matrix = phi_matrix / phi_sums

    # Step 3: For each high-CI dimension, find variables with high VDP
    dependency_groups = []
    max_eigenvalue = eigenvalues[0]

    for j, eigenvalue in enumerate(eigenvalues):
        # Skip if eigenvalue is not near-zero
        if abs(eigenvalue) >= 0.01:
            continue

        # Compute condition index for this dimension
        condition_index = float(np.sqrt(max_eigenvalue / abs(eigenvalue)))

        # Belsley recommends CI > 30 for severe multicollinearity
        if condition_index < 10:
            continue

        # Find variables with VDP > threshold on this dimension
        high_vdp_indices = np.where(vdp_matrix[:, j] > vdp_threshold)[0]

        logger.debug(
            "vdp_analysis",
            dimension=j,
            eigenvalue=round(eigenvalue, 6),
            condition_index=round(condition_index, 1),
            vdps=[round(v, 3) for v in vdp_matrix[:, j]],
            high_vdp_indices=high_vdp_indices.tolist(),
            vdp_threshold=vdp_threshold,
        )

        # Need at least 2 variables for a dependency group
        if len(high_vdp_indices) < 2:
            continue

        # Determine severity
        severity: Literal["moderate", "severe"] = "severe" if condition_index > 30 else "moderate"

        dependency_groups.append(
            DependencyGroupResult(
                dimension=j,
                eigenvalue=float(eigenvalue),
                condition_index=condition_index,
                severity=severity,
                involved_col_indices=high_vdp_indices.tolist(),
                variance_proportions=vdp_matrix[high_vdp_indices, j].tolist(),
            )
        )

    return dependency_groups
