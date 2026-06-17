"""The greedy driver tree + within-dataset permutation null (DAT-545 P2/P4).

The engine's decision core, ported and generalized from the DAT-544 spike:

- **Gate** — for every candidate dimension, the real gain, plus a p-value from a
  **within-dataset permutation null**: shuffle the target ``n_perm`` times and, each
  shuffle, take the MAX gain over all candidates (free multiple-comparison control —
  a dim's gain must beat the best a SHUFFLED target produces across the whole
  candidate set). Codes are built ONCE on the real target (the (B) gate is part of a
  dimension's encoding); only the target is permuted.
- **Greedy split** — keep the significant dim with the highest gain, recurse into each
  of its slice values that has enough rows, on the remaining dims, up to ``max_depth``.
- **Depth penalty** — the significance bar tightens with depth (``alpha / (depth+1)``)
  and each node rebuilds its null on its own rows, so deeper, smaller splits face a
  stricter, self-calibrated gate (the spike confirmed FDR doesn't compound at depth).

Magnitudes are only ever RANKED and permutation-gated — no global threshold anywhere,
which is what makes this vertical-agnostic (the noise floor is the dataset's own). The
target type (flow/stock vs ratio) is abstracted behind :class:`Target` (``targets.py``).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np

from dataraum.analysis.drivers.criterion import (
    DEFAULT_MIN_SUPPORT,
    DEFAULT_MISSINGNESS_GATE,
    build_codes,
)
from dataraum.analysis.drivers.models import DriverNode, DriverRanking, DriverSlice

if TYPE_CHECKING:
    from collections.abc import Mapping

    from dataraum.analysis.drivers.targets import Target

DEFAULT_N_PERM = 500
DEFAULT_ALPHA = 0.05
DEFAULT_MAX_DEPTH = 2
DEFAULT_TOP_K_SLICES = 5
# A child subset must clear this multiple of min_support to be re-gated — too few
# rows and the per-node null is meaningless (the spike's 4× rule).
_RECURSE_SUPPORT_MULTIPLE = 4

_Coded = tuple[np.ndarray, int]


def _gate(
    values_by_dim: Mapping[str, np.ndarray],
    target: Target,
    *,
    dims: list[str],
    rng: np.random.Generator,
    n_perm: int,
    min_support: int,
    missingness_gate: float,
) -> tuple[dict[str, _Coded], dict[str, float], dict[str, float]]:
    """Return per-dim ``(codes, n_codes)``, real gain, and permutation-null p-value."""
    coded: dict[str, _Coded] = {
        d: build_codes(
            values_by_dim[d], target.observed, handle_nulls=True, missingness_gate=missingness_gate
        )
        for d in dims
    }
    real = {d: target.gain(*coded[d], min_support=min_support) for d in dims}
    perm_max = np.empty(n_perm)
    for i in range(n_perm):
        shuffled = target.permuted(rng)
        perm_max[i] = max(
            (shuffled.gain(*coded[d], min_support=min_support) for d in dims), default=0.0
        )
    p = {d: (1 + int(np.sum(perm_max >= real[d]))) / (1 + n_perm) for d in dims}
    return coded, real, p


def _code_labels(values: np.ndarray, codes: np.ndarray, n_codes: int) -> list[str]:
    """The representative raw label of each group code (one label per code)."""
    labels = [""] * n_codes
    for c in range(n_codes):
        idx = np.flatnonzero(codes == c)
        if idx.size:
            labels[c] = str(values[idx[0]])
    return labels


def _slices(
    dimension: str,
    values: np.ndarray,
    target: Target,
    coded: _Coded,
    *,
    min_support: int,
    top_k: int,
) -> tuple[DriverSlice, ...]:
    """The supported slice values whose target deviates most from the node baseline."""
    codes, n_codes = coded
    labels = _code_labels(values, codes, n_codes)
    out = [
        DriverSlice(dimension, labels[code], effect, support)
        for code, effect, support in target.group_effects(codes, n_codes, min_support=min_support)
    ]
    out.sort(key=lambda s: abs(s.effect), reverse=True)
    return tuple(out[:top_k])


def _build_node(
    values_by_dim: Mapping[str, np.ndarray],
    target: Target,
    *,
    dims: list[str],
    depth: int,
    max_depth: int,
    alpha: float,
    min_support: int,
    missingness_gate: float,
    n_perm: int,
    top_k: int,
    rng: np.random.Generator,
) -> tuple[DriverNode | None, dict[str, float], dict[str, float]]:
    """Build the best split for this subset; recurse into its slice values.

    Returns ``(node, real_gain, p_value)`` — the gain/p maps are this node's gate
    result (the caller uses the root's to rank dimensions).
    """
    coded, real, p = _gate(
        values_by_dim,
        target,
        dims=dims,
        rng=rng,
        n_perm=n_perm,
        min_support=min_support,
        missingness_gate=missingness_gate,
    )
    eff_alpha = alpha / (depth + 1)  # depth penalty: deeper splits face a stricter bar
    significant = [d for d in dims if p[d] < eff_alpha and real[d] > 0.0]
    if not significant:
        return None, real, p

    best = max(significant, key=lambda d: real[d])
    codes, n_codes = coded[best]
    support = int((codes >= 0).sum())
    slices = _slices(
        best, values_by_dim[best], target, coded[best], min_support=min_support, top_k=top_k
    )

    children: list[tuple[str, DriverNode]] = []
    if depth + 1 < max_depth and len(dims) > 1:
        labels = _code_labels(values_by_dim[best], codes, n_codes)
        remaining = [d for d in dims if d != best]
        for c in range(n_codes):
            mask = codes == c
            if int(mask.sum()) < _RECURSE_SUPPORT_MULTIPLE * min_support:
                continue
            sub_values = {d: values_by_dim[d][mask] for d in remaining}
            child, _, _ = _build_node(
                sub_values,
                target.subset(mask),
                dims=remaining,
                depth=depth + 1,
                max_depth=max_depth,
                alpha=alpha,
                min_support=min_support,
                missingness_gate=missingness_gate,
                n_perm=n_perm,
                top_k=top_k,
                rng=rng,
            )
            if child is not None:
                children.append((labels[c], child))

    return DriverNode(best, real[best], p[best], support, slices, tuple(children)), real, p


def _walk_paths(node: DriverNode, prefix: list[str]) -> list[list[str]]:
    """Every root→leaf dimension path through the tree (drill vectors)."""
    path = [*prefix, node.dimension]
    if not node.children:
        return [path]
    paths: list[list[str]] = []
    for _value, child in node.children:
        paths.extend(_walk_paths(child, path))
    return paths


def _all_slices(node: DriverNode) -> list[DriverSlice]:
    out = list(node.slices)
    for _value, child in node.children:
        out.extend(_all_slices(child))
    return out


def discover_tree(
    values_by_dim: Mapping[str, np.ndarray],
    target: Target,
    *,
    measure_label: str,
    dims: list[str],
    rng: np.random.Generator,
    max_depth: int = DEFAULT_MAX_DEPTH,
    alpha: float = DEFAULT_ALPHA,
    min_support: int = DEFAULT_MIN_SUPPORT,
    missingness_gate: float = DEFAULT_MISSINGNESS_GATE,
    n_perm: int = DEFAULT_N_PERM,
    top_k_slices: int = DEFAULT_TOP_K_SLICES,
) -> DriverRanking:
    """Rank ``dims`` by their permutation-gated gain on ``target`` and build the tree.

    Each array in ``values_by_dim`` is row-aligned with the target. Returns a
    :class:`DriverRanking`; ``root`` is ``None`` when no dimension clears the null (a
    clean "no significant driver" answer, not an error).
    """
    root, real, p = _build_node(
        values_by_dim,
        target,
        dims=dims,
        depth=0,
        max_depth=max_depth,
        alpha=alpha,
        min_support=min_support,
        missingness_gate=missingness_gate,
        n_perm=n_perm,
        top_k=top_k_slices,
        rng=rng,
    )
    ranked = sorted(
        ((d, real[d]) for d in dims if p[d] < alpha and real[d] > 0.0),
        key=lambda pair: pair[1],
        reverse=True,
    )
    paths = _walk_paths(root, []) if root is not None else []
    slices = sorted(_all_slices(root), key=lambda s: abs(s.effect), reverse=True) if root else []
    return DriverRanking(
        measure=measure_label,
        target_type=target.target_type,
        n_rows=int(target.observed.size),
        ranked_dimensions=ranked,
        root=root,
        driver_paths=paths,
        interesting_slices=slices,
    )
