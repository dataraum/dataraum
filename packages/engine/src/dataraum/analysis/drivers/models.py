"""Result types for the driver-discovery engine (DAT-545).

In-memory only — DAT-545 is the engine, not the persisted artifact (that is
DAT-546). ``Measure`` is the engine's input value object; the rest is its output.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class Measure:
    """The numeric target a driver search explains.

    ``target_type`` drives the criterion (DAT-545): ``flow`` (additive) and
    ``stock`` (point_in_time) variance-reduce the row-grain value directly; ``ratio``
    aggregates Σnumerator / Σdenominator per group, support-weighted (averaging
    ratios is invalid). The type is read from the catalog's ``temporal_behavior``;
    a ratio is a computed measure carrying both columns.
    """

    target_type: str  # 'flow' | 'stock' | 'ratio'
    column: str | None = None  # flow / stock
    numerator: str | None = None  # ratio
    denominator: str | None = None  # ratio

    def __post_init__(self) -> None:
        if self.target_type in ("flow", "stock"):
            if not self.column:
                raise ValueError(f"{self.target_type} measure needs `column`")
        elif self.target_type == "ratio":
            if not (self.numerator and self.denominator):
                raise ValueError("ratio measure needs `numerator` and `denominator`")
        else:
            raise ValueError(f"unknown target_type {self.target_type!r}")

    @property
    def label(self) -> str:
        """A human label for logs/diagnostics."""
        if self.target_type == "ratio":
            return f"{self.numerator}/{self.denominator}"
        return self.column or "?"


@dataclass(frozen=True)
class DriverSlice:
    """A filter slice where the measure deviates sharply from the node baseline.

    ``effect`` is the signed relative deviation of the slice's value from the node
    baseline (``group / baseline − 1``); the baseline is the node's dim-present mean
    (flow/stock) or pooled ratio (ratio). ``support`` is the slice's row count (it
    cleared ``min_support``).
    """

    dimension: str
    value: str
    effect: float
    support: int


@dataclass(frozen=True)
class SecondaryDriver:
    """A significant dim from the NON-primary grain family (DAT-561).

    The cluster-aware search runs two families — entity-constant candidates at entity
    grain, row-level candidates row-wise — and reports the one selected by ICC as the
    primary tree. The other family's significant dims land here instead of in
    ``ranked_dimensions``: their gains were computed at a different exchangeable grain
    (``grain``), so they are NOT comparable to the primary tree's gains and must not be
    folded into the same ranking. A flat labeled list, strongest first.
    """

    dimension: str
    gain: float
    grain: str  # the exchangeable unit this dim's null used: "entity" or "row"


@dataclass(frozen=True)
class DriverNode:
    """One surviving split: the dimension that best explains the node's measure.

    ``children`` are ``(slice_value, subtree)`` pairs — the deeper split found WITHIN
    that slice value's rows (the drill path). Empty at a leaf / at ``max_depth``.
    """

    dimension: str
    gain: float
    p_value: float
    # Dim-present rows at this node (codes ≥ 0) — INCLUDES rows in sub-min_support
    # groups that the gain itself excluded, so ``support`` ≥ Σ(slice.support).
    support: int
    slices: tuple[DriverSlice, ...]
    children: tuple[tuple[str, DriverNode], ...] = ()


@dataclass(frozen=True)
class DriverRanking:
    """The engine's output for one measure.

    ``ranked_dimensions`` = the significant dims at the root by gain (the best
    aggregation vectors); ``root`` = the greedy driver tree; ``driver_paths`` = the
    surviving dimension drill vectors; ``interesting_slices`` = the sharp-deviation
    slices across the tree, strongest first.

    ``grain`` is the exchangeable unit the PRIMARY family's null used: ``"row"`` (the
    default) or ``"entity"`` when the cluster-aware path made the entity-grain family
    primary (DAT-552/561). ``n_rows`` is the count of those units (rows, or entities at
    entity grain) — i.e. the effective sample size the power scales with, so a "no
    significant driver" result on few entities is honestly attributable.

    ``secondary_dimensions`` carries the OTHER grain family's significant dims when a
    ``cluster_key`` is present (DAT-561 candidate-grain routing): entity-constant
    candidates always run at entity grain, row-level candidates row-wise, and the family
    not chosen as primary (by ICC) surfaces here as a flat grain-labeled list — never
    mixed into ``ranked_dimensions``/``root`` (the grains are not cross-comparable).
    """

    measure: str
    target_type: str
    n_rows: int
    grain: str = "row"
    ranked_dimensions: list[tuple[str, float]] = field(default_factory=list)
    root: DriverNode | None = None
    driver_paths: list[list[str]] = field(default_factory=list)
    interesting_slices: list[DriverSlice] = field(default_factory=list)
    secondary_dimensions: list[SecondaryDriver] = field(default_factory=list)
