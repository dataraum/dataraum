"""CPT (Conditional Probability Table) generation from edge strengths.

Generates TabularCPDs for the Bayesian Entropy Network using the
influence matrix approach described in docs/bayesian-entropy-network.md.

For root nodes: CPD is the prior distribution directly.
For single-parent nodes: influence matrix blended with uniform.
For multi-parent nodes: combined influence with pessimistic shift.
"""

import numpy as np
from pgmpy.factors.discrete import TabularCPD

from dataraum.entropy.network.config import CptGenerationConfig, NetworkConfig


def make_root_cpd(
    node_name: str,
    prior: list[float],
    states: list[str],
) -> TabularCPD:
    """Create CPD for a root node from its prior distribution.

    Args:
        node_name: Name of the root node.
        prior: Prior probability distribution [P(low), P(medium), P(high)].
        states: State names (e.g., ["low", "medium", "high"]).

    Returns:
        TabularCPD with the prior as a column vector.
    """
    n = len(states)
    values = [[p] for p in prior[:n]]
    return TabularCPD(
        variable=node_name,
        variable_card=n,
        values=values,
        state_names={node_name: states},
    )


def _build_influence_matrix(
    strength: float,
    n_states: int,
    config: CptGenerationConfig,
) -> np.ndarray:
    """Build an influence matrix for a single parent-child edge.

    The influence matrix captures how a parent's state affects the child:
    - High strength: parent state strongly predicts child state (diagonal dominance)
    - Low strength: parent has weak influence (closer to uniform)

    I[i][j] = strength * (1 if i==j else (1-strength)/(n-1))
    Then blended with uniform: blend * I + (1-blend) * uniform

    Args:
        strength: Edge strength in (0, 1].
        n_states: Number of discrete states.
        config: CPT generation parameters.

    Returns:
        n_states x n_states influence matrix, columns sum to 1.
    """
    # Build raw influence: diagonal = strength, off-diagonal = (1-strength)/(n-1)
    off_diag = (1.0 - strength) / (n_states - 1) if n_states > 1 else 0.0
    influence = np.full((n_states, n_states), off_diag)
    np.fill_diagonal(influence, strength)

    # Blend with uniform
    uniform = np.full((n_states, n_states), 1.0 / n_states)
    blended = config.influence_blend * influence + (1.0 - config.influence_blend) * uniform

    # Normalize columns
    col_sums = blended.sum(axis=0)
    blended = blended / col_sums[np.newaxis, :]

    result: np.ndarray = blended
    return result


def make_single_parent_cpd(
    node_name: str,
    parent_name: str,
    strength: float,
    states: list[str],
    config: CptGenerationConfig,
) -> TabularCPD:
    """Create CPD for a node with a single parent.

    Args:
        node_name: Name of the child node.
        parent_name: Name of the parent node.
        strength: Edge strength from parent to child.
        states: State names.
        config: CPT generation parameters.

    Returns:
        TabularCPD conditioned on the parent.
    """
    n = len(states)
    matrix = _build_influence_matrix(strength, n, config)
    _apply_probability_floor(matrix, config.min_probability)

    return TabularCPD(
        variable=node_name,
        variable_card=n,
        values=matrix.tolist(),
        evidence=[parent_name],
        evidence_card=[n],
        state_names={node_name: states, parent_name: states},
    )


def make_multi_parent_cpd(
    node_name: str,
    parents_with_strengths: list[tuple[str, float]],
    states: list[str],
    config: CptGenerationConfig,
) -> TabularCPD:
    """Create CPD for a node with multiple parents.

    Combines per-parent influence matrices via element-wise product,
    then applies pessimistic shift and probability floor.

    Args:
        node_name: Name of the child node.
        parents_with_strengths: List of (parent_name, strength) tuples.
        states: State names.
        config: CPT generation parameters.

    Returns:
        TabularCPD conditioned on all parents.
    """
    n = len(states)
    parent_names = [p[0] for p in parents_with_strengths]
    n_parents = len(parent_names)

    # Total number of parent state combinations
    n_combos = n**n_parents

    # Build the CPD matrix: n_states rows x n_combos columns
    # Each column corresponds to a specific parent state combination
    cpd_matrix = np.zeros((n, n_combos))

    for combo_idx in range(n_combos):
        # Decode combo index to parent states
        parent_states = []
        remainder = combo_idx
        for p_idx in range(n_parents):
            # States ordered: parent_0 changes slowest, parent_last changes fastest
            divisor = n ** (n_parents - 1 - p_idx)
            state_idx = remainder // divisor
            remainder = remainder % divisor
            parent_states.append(state_idx)

        # For each parent, get the column from its influence matrix
        # corresponding to its state, then combine via element-wise product
        combined = np.ones(n)
        for p_idx, (_, strength) in enumerate(parents_with_strengths):
            influence = _build_influence_matrix(strength, n, config)
            combined *= influence[:, parent_states[p_idx]]

        # Normalize
        total = combined.sum()
        if total > 0:
            combined /= total

        # Pessimistic shift: move probability mass toward "high" (last state)
        shift = config.pessimistic_shift
        if shift > 0 and n > 1:
            # Take from all states proportionally, add to last state
            donation = combined[:-1] * shift
            combined[:-1] -= donation
            combined[-1] += donation.sum()

        cpd_matrix[:, combo_idx] = combined

    _apply_probability_floor(cpd_matrix, config.min_probability)

    state_names = {node_name: states}
    for p_name in parent_names:
        state_names[p_name] = states

    return TabularCPD(
        variable=node_name,
        variable_card=n,
        values=cpd_matrix.tolist(),
        evidence=parent_names,
        evidence_card=[n] * n_parents,
        state_names=state_names,
    )


def _apply_probability_floor(matrix: np.ndarray, min_prob: float) -> None:
    """Apply minimum probability floor and renormalize columns in-place."""
    matrix[matrix < min_prob] = min_prob
    col_sums = matrix.sum(axis=0)
    matrix[:] = matrix / col_sums[np.newaxis, :]


def generate_all_cpds(config: NetworkConfig) -> list[TabularCPD]:
    """Generate all CPDs for the network from configuration.

    Args:
        config: Complete network configuration.

    Returns:
        List of TabularCPD objects, one per node.
    """
    states = config.states

    # Build parent map: child -> [(parent, strength)]
    parent_map: dict[str, list[tuple[str, float]]] = {name: [] for name in config.nodes}
    for edge in config.edges:
        parent_map[edge.child].append((edge.parent, edge.strength))

    cpds: list[TabularCPD] = []

    for name, node in config.nodes.items():
        parents = parent_map[name]

        if node.is_root and not parents:
            # Root node: use prior directly
            assert node.prior is not None
            cpds.append(make_root_cpd(name, node.prior, states))
        elif len(parents) == 1:
            parent_name, strength = parents[0]
            cpds.append(make_single_parent_cpd(
                name, parent_name, strength, states, config.cpt_generation,
            ))
        else:
            cpds.append(make_multi_parent_cpd(
                name, parents, states, config.cpt_generation,
            ))

    return cpds
