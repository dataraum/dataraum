"""Target types for the driver tree (DAT-545 P4) — flow/stock vs ratio.

The tree is target-agnostic: it asks a ``Target`` for the gain of a grouping, a
shuffled copy of itself (the permutation null), and per-group effects (for the
interesting slices). This is where flow/stock and ratio differ:

- :class:`FlowTarget` serves both **flow** (additive) and **stock** (point_in_time):
  row-grain variance reduction of the measure value. Stock is additivity-respecting
  *because* it never sums — it reduces the raw snapshot value's variance.
- :class:`RatioTarget` serves **ratio**: the group statistic is ``Σnum/Σden`` (never
  the mean of per-row ratios), so it permutes the (num, den) PAIRS jointly and uses
  the support-weighted gain.

``observed`` is the array :func:`build_codes` reads for the (B) missingness gate —
finite where the row contributes to the measure, NaN where it does not.
"""

from __future__ import annotations

from abc import ABC, abstractmethod

import numpy as np

from dataraum.analysis.drivers.criterion import variance_reduction, weighted_variance_reduction


class Target(ABC):
    """A numeric target the driver tree explains. Immutable; ``permuted`` returns a copy."""

    target_type: str
    observed: np.ndarray

    @abstractmethod
    def gain(self, codes: np.ndarray, n_codes: int, *, min_support: int) -> float:
        """Fraction of the target's variation explained by the grouping ``codes``."""

    @abstractmethod
    def permuted(self, rng: np.random.Generator) -> Target:
        """A copy with the target shuffled across rows (the permutation null draw)."""

    @abstractmethod
    def subset(self, mask: np.ndarray) -> Target:
        """A copy restricted to ``mask`` rows (a child node's row subset)."""

    @abstractmethod
    def group_effects(
        self, codes: np.ndarray, n_codes: int, *, min_support: int
    ) -> list[tuple[int, float, int]]:
        """``(code, effect, support)`` per supported group; effect = group/baseline − 1."""


class FlowTarget(Target):
    """Flow (additive) or stock (point_in_time): row-grain variance reduction."""

    def __init__(self, measure: np.ndarray, *, target_type: str = "flow") -> None:
        self.target_type = target_type
        self._measure = measure
        self.observed = measure  # finite where measured (NaN = missing)

    def gain(self, codes: np.ndarray, n_codes: int, *, min_support: int) -> float:
        return variance_reduction(codes, n_codes, self._measure, min_support=min_support)

    def permuted(self, rng: np.random.Generator) -> FlowTarget:
        return FlowTarget(rng.permutation(self._measure), target_type=self.target_type)

    def subset(self, mask: np.ndarray) -> FlowTarget:
        return FlowTarget(self._measure[mask], target_type=self.target_type)

    def group_effects(
        self, codes: np.ndarray, n_codes: int, *, min_support: int
    ) -> list[tuple[int, float, int]]:
        observed = ~np.isnan(self._measure)
        keep = (codes >= 0) & observed
        if int(keep.sum()) < min_support or not self._measure[keep].size:
            return []
        baseline = float(self._measure[keep].mean())
        out: list[tuple[int, float, int]] = []
        for c in range(n_codes):
            in_group = (codes == c) & observed
            support = int(in_group.sum())
            if support < min_support:
                continue
            group_mean = float(self._measure[in_group].mean())
            effect = (group_mean / baseline - 1.0) if baseline else 0.0
            out.append((c, effect, support))
        return out


class RatioTarget(Target):
    """Ratio: the group statistic is Σnum/Σden, support-weighted by the denominator."""

    def __init__(self, numerator: np.ndarray, denominator: np.ndarray) -> None:
        self.target_type = "ratio"
        self._num = numerator
        self._den = denominator
        valid = ~np.isnan(numerator) & ~np.isnan(denominator) & (denominator > 0)
        # Per-row ratio (NaN where invalid) doubles as the (B)-gate observed mask;
        # weight is the denominator mass (0 where invalid).
        with np.errstate(divide="ignore", invalid="ignore"):
            self._ratio = np.where(valid, numerator / np.where(valid, denominator, 1.0), np.nan)
        self._weight = np.where(valid, denominator, 0.0)
        self.observed = self._ratio

    def gain(self, codes: np.ndarray, n_codes: int, *, min_support: int) -> float:
        return weighted_variance_reduction(
            codes, n_codes, self._ratio, self._weight, min_support=min_support
        )

    def permuted(self, rng: np.random.Generator) -> RatioTarget:
        idx = rng.permutation(self._num.size)
        return RatioTarget(self._num[idx], self._den[idx])

    def subset(self, mask: np.ndarray) -> RatioTarget:
        return RatioTarget(self._num[mask], self._den[mask])

    def group_effects(
        self, codes: np.ndarray, n_codes: int, *, min_support: int
    ) -> list[tuple[int, float, int]]:
        valid = (codes >= 0) & ~np.isnan(self._ratio) & (self._weight > 0)
        if int(valid.sum()) < min_support:
            return []
        den_total = float(self._den[valid].sum())
        baseline = float(self._num[valid].sum() / den_total) if den_total else 0.0
        out: list[tuple[int, float, int]] = []
        for c in range(n_codes):
            in_group = (codes == c) & valid
            support = int(in_group.sum())
            if support < min_support:
                continue
            den_g = float(self._den[in_group].sum())
            group_ratio = float(self._num[in_group].sum() / den_g) if den_g else 0.0
            effect = (group_ratio / baseline - 1.0) if baseline else 0.0
            out.append((c, effect, support))
        return out


class EntityMeanTarget(Target):
    """The cluster-aware target for high-ICC measures (DAT-552).

    One row per ENTITY — the entity's mean measure, weighted by its row count — so the
    permutation null shuffles ENTITIES, not rows (the exchangeable unit when the
    measure is clustered, DAT-544 E1). Power then scales with entity count, not row
    count. The processor collapses the frame to entity grain and supplies entity-level
    candidate values (constant within entity) before building this. Gain is the
    support-weighted between-entity variance reduction — algebraically the same
    :func:`weighted_variance_reduction` the ratio target uses (entity mean as the
    value, entity size as the weight). ``min_support`` here is an ENTITY count, not a
    row count.
    """

    def __init__(
        self, entity_means: np.ndarray, entity_sizes: np.ndarray, *, target_type: str
    ) -> None:
        self.target_type = target_type
        self._means = entity_means
        self._sizes = entity_sizes
        self.observed = entity_means  # one observed value per entity (collapse drops NaN)

    def gain(self, codes: np.ndarray, n_codes: int, *, min_support: int) -> float:
        return weighted_variance_reduction(
            codes, n_codes, self._means, self._sizes, min_support=min_support
        )

    def permuted(self, rng: np.random.Generator) -> EntityMeanTarget:
        idx = rng.permutation(self._means.size)
        return EntityMeanTarget(self._means[idx], self._sizes[idx], target_type=self.target_type)

    def subset(self, mask: np.ndarray) -> EntityMeanTarget:
        return EntityMeanTarget(self._means[mask], self._sizes[mask], target_type=self.target_type)

    def group_effects(
        self, codes: np.ndarray, n_codes: int, *, min_support: int
    ) -> list[tuple[int, float, int]]:
        keep = codes >= 0
        if int(keep.sum()) < min_support:
            return []
        w_total = float(self._sizes[keep].sum())
        baseline = (
            float((self._means[keep] * self._sizes[keep]).sum() / w_total) if w_total else 0.0
        )
        out: list[tuple[int, float, int]] = []
        for c in range(n_codes):
            in_group = codes == c
            n_entities = int(in_group.sum())
            if n_entities < min_support:
                continue
            w_g = float(self._sizes[in_group].sum())
            group_mean = (
                float((self._means[in_group] * self._sizes[in_group]).sum() / w_g) if w_g else 0.0
            )
            effect = (group_mean / baseline - 1.0) if baseline else 0.0
            out.append((c, effect, n_entities))  # support = ENTITY count
        return out
